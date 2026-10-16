"""Synthetic public data only; no real session, authority or financial I/O."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
import os,pickle,tempfile,unittest,json,sys,threading
from decimal import Decimal,localcontext
from types import SimpleNamespace
from contextlib import ExitStack,nullcontext
from unittest.mock import patch,Mock

_MONOREPO=Path(os.environ.get('LIVE_NATIVE_TEST_MONOREPO_ROOT') or Path(__file__).resolve().parents[3])
_SHARED_TESTS=_MONOREPO/'packages/trading_runtime/tests'
if not _SHARED_TESTS.is_dir():
    raise RuntimeError('Native Live tests require the matching monorepo shared test fixtures')
if str(_SHARED_TESTS) not in sys.path:
    sys.path.insert(0,str(_SHARED_TESTS))
from test_native_rest_initialization import rest_fixture
from trading_runtime.artifact_governance import stable_sha256,seal_strategy_artifact,seal_strategy_instance,seal_portfolio_artifact,assert_verified_portfolio_artifact
from trading_runtime.continuous_runtime import ClosedBar,DurableRuntimeState,ContinuousRuntimeSupervisor
from trading_runtime.market_clock_integrity import StickyMarketClockIntegrity
from trading_runtime.market_source_integrity import StickyMarketSourceIntegrity
from trading_runtime.native_rest_initialization import build_native_rest_initialization,initialization_bars
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator,load_portfolio_runtime_path
from trading_runtime.execution_quantity import seal_quantity_contract
from live_trader.runtime_scope import RuntimeScope
from live_trader.kis_native_initialization import build_live_initialization,validate_live_initialization,LiveKisNativeInitializedEngine
from live_trader.kis_prepared_native_feed import prepare_live_kis_feed
from live_trader.kis_native_public_preparation import PublicKisInitializationTransport,prepare_live_kis_native
import live_trader.kis_native_public_preparation as public_preparation
from live_trader.account_profiles import saved_account_profiles
from live_trader.read_only_account import ReadOnlyBrokerReader


class Clock:
    def __init__(self):self.now=datetime(2026,10,16,5,10,3,tzinfo=timezone.utc);self.tick=1000.0
    def utc(self):return self.now
    def mono(self):self.tick+=.01;return self.tick


class Feed:
    provider_id='kis'
    def __init__(self,data,specs,clock):
        self.data,self.specs,self.clock=data,specs,clock
        self.socket=None;self._clock_generation=0;self.connected=0;self.disconnected=0;self.polls=0
        self.source=StickyMarketSourceIntegrity();self.clock_proof=StickyMarketClockIntegrity()
    def connect(self):self.socket=object();self.connected+=1;self._clock_generation+=1
    def disconnect(self):self.disconnected+=1;self.socket=None
    def market_source_integrity_snapshot(self):return self.source.snapshot()
    def market_clock_integrity_snapshot(self):return self.clock_proof.snapshot()
    def readiness_snapshot(self):return {'ready':self.socket is not None,'connected':self.socket is not None,'transport':'kis-websocket'}
    def poll(self,timeout):
        self.polls+=1
        if self.polls==1:
            bars=initialization_bars(self.data,portfolio_hash=self.data['portfolioHash'],promotion_policy_hash=self.data['promotionPolicyHash'])
            return [replace(rows[-1],evidence_class='',source_sequence='native-wire') for rows in bars.values()]
        self.clock.now=datetime(2026,10,16,5,15,1,tzinfo=timezone.utc)
        return [ClosedBar(spec.instrument_id,spec.symbol,'kis','5m','2026-10-16T05:10:00Z','2026-10-16T05:15:00Z',103000,103010,102990,103000,10,source_provider='kis') for spec in self.specs]


class LiveKisNativeBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.expected=rest_fixture();self.specs=tuple(self.expected.specs)
        self.data=build_native_rest_initialization(expected=self.expected);self.clock=Clock()
        self.scope=RuntimeScope('kis','a'*64,'fixture-deployment','stock','binding-fixture','b'*64,'KRW')
        self.context={'schemaVersion':'kis-scoped-manual-start-context/v1','scopeHash':'c'*64,'profileId':'stock',
            'mode':'SMALL_LIVE','portfolioId':self.data['portfolioId'],'deploymentId':self.scope.deployment_id,
            'strategyId':self.specs[0].strategy_id,'runtimeScopeId':self.scope.runtime_id,'capitalCurrency':'KRW',
            'accountFingerprint':self.scope.account_fingerprint,'credentialConfigurationHash':self.scope.credential_configuration_hash,
            'wireAccountFingerprint':'d'*64,'wireCredentialConfigurationHash':'e'*64,'ownerEpochHash':'f'*64}
        self.binding={'context':self.context,'runtimeScope':self.scope.to_dict(),'deploymentManifestHash':'1'*64}
        self.feed=Feed(self.data,self.specs,self.clock)
    def prepare(self,**kwargs):
        return prepare_live_kis_feed(feeds=(self.feed,),specs=self.specs,context=self.context,runtime_scope=self.scope,
            deployment_manifest_hash=self.binding['deploymentManifestHash'],initialize=lambda _:self.data,
            binding_reader=lambda:deepcopy(self.binding),preparation_guard=lambda:None,
            utc_clock=self.clock.utc,monotonic_clock=self.clock.mono,maximum_seconds=10,**kwargs)
    def receipt(self):
        return build_live_initialization(self.data,context=self.context,runtime_scope=self.scope,
            deployment_manifest_hash=self.binding['deploymentManifestHash'],specs=self.specs)
    def activate(self,prepared):
        self.session={'sessionId':'fixture-session','deploymentId':self.scope.deployment_id,
            'deploymentManifestHash':self.binding['deploymentManifestHash'],'profile':'stock','mode':'SMALL_LIVE',
            'runtimeInstanceId':'fixture-process','lifecycle':'RUNNING','createdAt':'2026-10-16T05:10:04Z',
            'metadata':self.scope.to_dict()}
        prepared.bind_activation(session=self.session,activation_reader=lambda:deepcopy(self.session))
        prepared.confirm_running(session=self.session,activation_reader=lambda:deepcopy(self.session))
    def engine(self,receipt,path=None):
        engine=LiveKisNativeInitializedEngine(self.specs,mode='MONITOR',evaluator=BuiltinBarSignalEvaluator(),
            cycle_handler=lambda _:None,state_store=DurableRuntimeState(path) if path else None)
        engine.seed_live_initialization(receipt,context=self.context,runtime_scope=self.scope,
            deployment_manifest_hash=self.binding['deploymentManifestHash'])
        return engine
    def attestation(self,engine):
        body={'portfolioId':engine.portfolio_id,'portfolioHash':engine.portfolio_hash,
            'strategyIdentityHash':engine.strategy_identity_hash,'liveNativeInitializationHash':engine.live_native_initialization['contentHash']}
        return {**{k:body[k] for k in ('portfolioId','portfolioHash','strategyIdentityHash')},
            'fresh':True,'allSpecsKnown':True,'brokerSnapshotComplete':True,'ageSeconds':0,
            'observedAt':datetime.now(timezone.utc).isoformat(),'pendingOrUnknownOrderCount':0,
            'allFlat':True,'programLedgerFlat':True,'contextSeal':{'body':body,'bodyHash':stable_sha256(body)}}

    def test_separate_live_data_scope_is_not_paper_or_order_authority(self):
        receipt=self.receipt()
        self.assertFalse(receipt['executionAuthorityGranted'] or receipt['paperSessionImported'] or receipt['promotionEvidence'])
        self.assertEqual(self.data['contentHash'],receipt['publicInitializationHash'])
        self.assertEqual(receipt,validate_live_initialization(receipt,context=self.context,runtime_scope=self.scope,
            deployment_manifest_hash=self.binding['deploymentManifestHash'],specs=self.specs))
        with self.assertRaises(ValueError):
            validate_live_initialization(self.data,context=self.context,runtime_scope=self.scope,deployment_manifest_hash='1'*64,specs=self.specs)
    def test_account_credentials_manifest_context_or_source_rebinding_is_denied(self):
        receipt=self.receipt()
        for changes in ({'runtime_scope':replace(self.scope,account_fingerprint='2'*64)},
                {'context':{**self.context,'credentialConfigurationHash':'2'*64}},
                {'deployment_manifest_hash':'2'*64},{'context':{**self.context,'scopeHash':'2'*64}}):
            kwargs={'context':self.context,'runtime_scope':self.scope,'deployment_manifest_hash':'1'*64,'specs':self.specs,**changes}
            with self.assertRaises(ValueError):validate_live_initialization(receipt,**kwargs)
    def test_prepared_public_feed_has_zero_engine_session_or_order_before_native_confirmation(self):
        prepared=self.prepare()
        self.assertEqual(1,self.feed.connected)
        self.assertFalse(hasattr(self,'session'))
        engine=self.engine(prepared.receipt)
        self.assertEqual((0,0),(engine.bar_count,engine.decision_count))
        with self.assertRaises(RuntimeError):prepared.claim_for_supervisor(feeds=prepared.feeds,engine=engine)
        prepared.close()
    def test_real_live_restore_gate_then_same_socket_first_native_forward_decisions(self):
        prepared=self.prepare();self.activate(prepared)
        with tempfile.TemporaryDirectory() as temporary:
            engine=self.engine(prepared.receipt,Path(temporary)/'engine.json')
            self.assertTrue(engine.transition_mode('SMALL_LIVE',restore_context=None))
            self.assertEqual('',engine.transition_mode('SMALL_LIVE',restore_context=self.attestation(engine)))
            supervisor=ContinuousRuntimeSupervisor(engine=engine,feeds=prepared.feeds,prepared_feed_handoff=prepared,
                utc_clock=self.clock.utc,monotonic_clock=self.clock.mono)
            engine.cycle_handler=lambda _:supervisor._stop.set()
            supervisor.run_forever()
            self.assertEqual(3,engine.decision_count)
            self.assertEqual(0,engine.gap_count)
            self.assertEqual((1,1),(self.feed.connected,self.feed.disconnected))
            self.assertEqual('2026-10-16T05:10:04Z',self.session['createdAt'])
    def test_fresh_broker_context_must_bind_same_native_data_and_preserve_flat_pending_gates(self):
        with tempfile.TemporaryDirectory() as temporary:
            engine=self.engine(self.receipt(),Path(temporary)/'engine.json')
            proof=self.attestation(engine);proof['contextSeal']['body'].pop('liveNativeInitializationHash')
            proof['contextSeal']['bodyHash']=stable_sha256(proof['contextSeal']['body'])
            self.assertTrue(engine.transition_mode('SMALL_LIVE',restore_context=proof))
            for changes in ({'pendingOrUnknownOrderCount':1},{'programLedgerFlat':False,'allFlat':False},{'fresh':False}):
                self.assertTrue(engine.transition_mode('SMALL_LIVE',restore_context={**self.attestation(engine),**changes}))
    def test_native_checkpoint_cannot_cold_restore_and_original_bytes_are_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'engine.json';engine=self.engine(self.receipt(),path);before=path.read_bytes()
            restored=LiveKisNativeInitializedEngine(self.specs,mode='MONITOR',evaluator=BuiltinBarSignalEvaluator(),cycle_handler=lambda _:None,state_store=DurableRuntimeState(path))
            self.assertEqual('REJECTED',restored.restore_status);self.assertEqual(before,path.read_bytes())
            with self.assertRaises(ValueError):restored.seed_live_initialization(self.receipt(),context=self.context,runtime_scope=self.scope,deployment_manifest_hash='1'*64)
    def test_cutoff_and_session_scope_or_generation_cannot_change(self):
        for change in ('cutoff','scope','generation'):
            self.feed=Feed(self.data,self.specs,self.clock);prepared=self.prepare()
            if change=='generation':self.feed._clock_generation+=1
            elif change=='scope':self.binding['context']={**self.context,'ownerEpochHash':'2'*64}
            else:
                self.activate(prepared);self.session['createdAt']='2026-10-16T05:15:04Z'
                with self.assertRaises(RuntimeError):prepared.assert_current()
                continue
            with self.assertRaises(RuntimeError):self.activate(prepared)
    def test_source_clock_failure_or_revoke_never_grants_handoff(self):
        for change in ('source','clock','revoke'):
            self.feed=Feed(self.data,self.specs,self.clock);prepared=self.prepare()
            if change=='source':self.feed.source.observe({'sourceIntegrity':None})
            elif change=='clock':self.feed.clock_proof.invalidate()
            else:prepared.revoke()
            with self.assertRaises(RuntimeError):self.activate(prepared)
    def test_public_transport_rejects_account_order_cancel_and_wrong_tr(self):
        transport=PublicKisInitializationTransport()
        for method,path in (('GET','/uapi/domestic-stock/v1/trading/inquire-balance'),
                ('POST','/uapi/domestic-stock/v1/trading/order-cash'),('POST','/uapi/domestic-stock/v1/trading/order-rvsecncl')):
            with self.assertRaises(ValueError):transport.request('kis',method,path)
    def test_no_label_approval_or_synthetic_auction_or_reseed_permission(self):
        receipt=self.receipt();engine=self.engine(receipt)
        with self.assertRaises(ValueError):engine.seed_live_initialization(receipt,context=self.context,runtime_scope=self.scope,deployment_manifest_hash='1'*64)
        spec=self.specs[0]
        for start,end in (('2026-10-16T05:05:00Z','2026-10-16T05:10:00Z'),('2026-10-16T06:15:00Z','2026-10-16T06:20:00Z')):
            with self.assertRaises(ValueError):engine.ingest_closed_bar(ClosedBar(spec.instrument_id,spec.symbol,'kis','5m',start,end,1,1,1,1,1))
        with self.assertRaises(TypeError):pickle.dumps(self.prepare())

    def canonical_live_fixture(self,temporary,*,activate=True):
        root=Path(temporary);(root/'portfolios').mkdir();(root/'strategy-instances').mkdir()
        sources={};instances={};members=[]
        for old in self.expected.portfolio['strategyInstances']:
            symbol=old['qualifiedSymbol'];source_id=old['sourceStrategyId']
            source={k:deepcopy(v) for k,v in self.expected.sources[source_id].items() if k not in {'artifactLock','artifactHash'}}
            contract=seal_quantity_contract({'schemaVersion':'native-equity-quantity-v1','provider':'kis','assetClass':'EQUITY',
                'quantityUnit':'SHARE','executionScope':'LOCAL_MODEL_ONLY','symbol':symbol,'baseCurrency':symbol,'quoteCurrency':'KRW',
                'exchange':'KRX','orderType':'MARKET','stepSize':'1','minQuantity':'1','maxQuantity':'1000000',
                'minNotional':'0','maxNotional':'0','notionalReferenceMinutes':0,'capabilityStatus':'UNKNOWN',
                'provenance':{'references':['synthetic native Source fixture']}})
            source['executionSizing']={'mode':'capital_percentage','fractionalAllowed':False,
                'capitalBasis':'PORTFOLIO_EQUITY_WITH_CASH_CAP','quantityContract':contract}
            source=seal_strategy_artifact(source);sources[source_id]=source
            instance={k:deepcopy(v) for k,v in self.expected.instances[old['templateInstanceId']].items() if k not in {'instanceLock','artifactLock','artifactHash'}}
            instance['sourceArtifactHash']=source['artifactLock']['artifactHash']
            instance['runtimeMarketDataContract']['sourceArtifactHash']=instance['sourceArtifactHash']
            instance=seal_strategy_instance(instance);instances[instance['instanceId']]=instance
            member={k:deepcopy(v) for k,v in instance.items() if k not in {'instanceLock','artifactLock','artifactHash'}}
            member.update(instanceId=old['instanceId'],templateInstanceId=instance['instanceId'],strategyId=source_id,
                sourceInstanceHash=instance['instanceLock']['artifactHash'],allocation={'normalizedWeight':1/3})
            members.append(member)
            (root/(source_id+'.json')).write_text(json.dumps(source),encoding='utf-8')
            (root/'strategy-instances'/(instance['instanceId']+'.json')).write_text(json.dumps(instance),encoding='utf-8')
        portfolio={k:deepcopy(v) for k,v in self.expected.portfolio.items() if k not in {'artifactLock','artifactHash'}}
        portfolio['permissions']={'live_small_eligible':True}
        portfolio['strategyInstances']=members;portfolio=seal_portfolio_artifact(portfolio)
        path=root/'portfolios/portfolio.json';path.write_text(json.dumps(portfolio),encoding='utf-8')
        loaded=load_portfolio_runtime_path(path,artifact_root=root)
        self.specs=tuple(loaded.specs)
        self.expected=replace(self.expected,specs=self.specs,sources=sources,instances=instances,portfolio=portfolio)
        self.data=build_native_rest_initialization(expected=self.expected)
        self.feed=Feed(self.data,self.specs,self.clock)
        prepared=self.prepare(portfolio_path=path,artifact_root=root)
        if activate:self.activate(prepared)
        return root,loaded,prepared

    def test_actual_live_canonical_start_loader_seeds_before_restore_and_defers_thread_until_state_cas(self):
        from live_trader.continuous_live import LiveContinuousController
        from live_trader import state
        with tempfile.TemporaryDirectory() as temporary:
            root,loaded,prepared=self.canonical_live_fixture(temporary)
            controller=LiveContinuousController(root);controller.bind_runtime_scope(self.scope)
            self.assertTrue(controller._native_equity_specs(self.specs))
            with ExitStack() as stack:
                for name,value in (('_select_runtime_portfolio',{'source_path':str(prepared.portfolio_path)}),('_dispatch_status',{'reconciliationRequired':False}),
                        ('_paper_final_binding_blocker',''),('_loaded_portfolio_lineage_blocker',''),('_configure_portfolio_execution',''),
                        ('_portfolio_reconciliation_blocker',''),('_spec_mode_allowed',True),('_runtime_position_quantity',0),
                        ('_ensure_native_runtime',None),('_dispatcher',nullcontext())):
                    stack.enter_context(patch.object(controller,name,return_value=value))
                stack.enter_context(patch.object(controller,'_restore_context_assessment',side_effect=lambda specs,engine:self.attestation(engine)))
                stack.enter_context(patch.object(state,'snapshot',return_value={}))
                stack.enter_context(patch.object(state,'append_audit'))
                stack.enter_context(patch('live_trader.continuous_live.feeds_for_specs',side_effect=AssertionError('must reuse same prepared feed')))
                result=controller.start('stock','SMALL_LIVE',loaded.portfolio_id,deployment_id=self.scope.deployment_id,prepared_native_context=prepared)
                self.assertTrue(result['ok'],result);self.assertTrue(result['preparedRuntimeReady']);self.assertFalse(controller.supervisor.running)
                self.assertEqual(0,controller.supervisor.engine.decision_count)
                self.assertEqual(1,self.feed.connected)
                supervisor=controller.supervisor
                supervisor._utc_clock=self.clock.utc;supervisor._monotonic_clock=self.clock.mono
                controller.supervisor.engine.cycle_handler=lambda _:supervisor._stop.set()
                real_start=supervisor.start
                def unlocked_start():
                    self.assertFalse(controller._lock._is_owned())
                    return real_start()
                with patch.object(supervisor,'start',side_effect=unlocked_start):
                    activated=controller.activate_prepared_native_runtime(prepared)
                self.assertTrue(activated['runtimeStarted'])
                supervisor._thread.join(3)
                self.assertFalse(supervisor.running)
                self.assertEqual(3,supervisor.engine.decision_count)
                self.assertEqual((1,1),(self.feed.connected,self.feed.disconnected))

    def test_unproven_full_label_scope_blocks_before_oauth_public_feed_or_any_account_reader(self):
        labels=deepcopy(self.expected.label_acceptances)
        label=labels[self.specs[0].strategy_instance_id];label['scopeStart']='2026-10-16T05:09:00Z'
        label['contentHash']=stable_sha256({k:v for k,v in label.items() if k!='contentHash'})
        plan={'loaded':SimpleNamespace(specs=self.specs,payload=self.expected.portfolio),
            'context':self.context,'runtimeScope':self.scope.to_dict(),'promotionPolicy':self.expected.promotion_policy,
            'labelAcceptances':labels}
        factory=Mock(side_effect=AssertionError('no public connect until actual label scope exists'))
        with patch.object(ReadOnlyBrokerReader,'_kis_token',side_effect=AssertionError('OAuth forbidden')) as token:
            with self.assertRaisesRegex(ValueError,'LABEL_SCOPE'):
                prepare_live_kis_native(Path('.'),plan=plan,binding_reader=lambda:self.binding,preparation_guard=lambda:None,
                    settings={},utc_clock=self.clock.utc,monotonic_clock=self.clock.mono,feed_factory=factory)
            token.assert_not_called();factory.assert_not_called()

    def test_actual_public_producer_uses_exact_current_day_seven_gets_and_no_private_endpoints(self):
        settings={'KIS_APP_KEY':'SYNTHETIC_PUBLIC_APP','KIS_APP_SECRET':'SYNTHETIC_PUBLIC_SECRET',
            'KIS_ACCOUNT_NO':'12345678','KIS_ACCOUNT_PRODUCT_CODE':'01'}
        account=next(row for row in saved_account_profiles(settings) if row['brokerId']=='kis')
        self.scope=RuntimeScope('kis',account['accountFingerprint'],'fixture-deployment','stock',account['accountBindingId'],account['credentialConfigurationHash'],'KRW')
        self.context.update(runtimeScopeId=self.scope.runtime_id,accountFingerprint=self.scope.account_fingerprint,
            credentialConfigurationHash=self.scope.credential_configuration_hash)
        self.binding.update(context=self.context,runtimeScope=self.scope.to_dict())
        events=[];rows={}
        for segment in self.expected.segments.values():
            for page in segment['pages']:
                rows.setdefault(page['params']['FID_INPUT_ISCD'],{}).update({row['stck_cntg_hour']:row for row in page['publicResponse']['output2']})
        class PublicFixtureTransport:
            def request(inner,provider,method,path,*,query=None,headers=None,body=None):
                self.assertEqual(('kis','GET','/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice'),(provider,method,path))
                self.assertEqual('FHKST03010230',headers['tr_id']);self.assertEqual('20261016',query['FID_INPUT_DATE_1'])
                self.assertNotIn('CANO',query);self.assertIsNone(body)
                events.append(deepcopy(query))
                available=rows[query['FID_INPUT_ISCD']]
                selected=[available[key] for key in sorted(available,reverse=True) if key<=query['FID_INPUT_HOUR_1']][:120]
                return {'rt_cd':'0','output2':selected},{}
        with tempfile.TemporaryDirectory() as temporary:
            plan={'loaded':SimpleNamespace(specs=self.specs,payload=self.expected.portfolio),
                'context':self.context,'runtimeScope':self.scope.to_dict(),'deploymentManifestHash':'1'*64,
                'portfolioPath':str(Path(temporary)/'portfolio.json'),'artifactRoot':temporary,
                'promotionPolicy':self.expected.promotion_policy,'labelAcceptances':self.expected.label_acceptances,
                'calendarObjects':self.expected.calendar_objects}
            synthetic_reviews=tuple({k:deepcopy(label[k]) for k in ('symbol','businessDate','scopeStart','scopeEnd','evidenceHashes')}
                for label in self.expected.label_acceptances.values())
            with patch.object(ReadOnlyBrokerReader,'_kis_token',return_value='synthetic-public-token'),patch.object(ReadOnlyBrokerReader,'_pace_kis'),\
                    patch.object(public_preparation,'_load_code_pinned_label_scopes',return_value=synthetic_reviews):
                prepared=prepare_live_kis_native(Path(temporary),plan=plan,binding_reader=lambda:deepcopy(self.binding),
                    preparation_guard=lambda:None,settings=settings,utc_clock=self.clock.utc,monotonic_clock=self.clock.mono,
                    maximum_seconds=10,feed_factory=lambda *args,**kwargs:(self.feed,),transport=PublicFixtureTransport())
            self.assertEqual(7,len(events))
            self.assertEqual(111,sum(len(member['historyRows']) for member in prepared.receipt['publicInitializationData']['members']))
            self.assertEqual(1,self.feed.connected);self.assertFalse(prepared.receipt['executionAuthorityGranted'])
            prepared.close()

    def test_original_source_in_existing_sealed_one_member_portfolio_remains_single_symbol_capable(self):
        original_sources=deepcopy(self.expected.sources)
        first=self.specs[0];member=deepcopy(self.expected.portfolio['strategyInstances'][0])
        member['allocation']={'normalizedWeight':1}
        portfolio={k:deepcopy(v) for k,v in self.expected.portfolio.items() if k not in {'artifactLock','artifactHash'}}
        portfolio['strategyInstances']=[member]
        portfolio['framework']={'targetPortfolio':[{'strategyInstanceId':member['instanceId'],'targetWeight':1}]}
        portfolio=seal_portfolio_artifact(portfolio)
        folder=tempfile.TemporaryDirectory();self.addCleanup(folder.cleanup);root=Path(folder.name)
        (root/'strategy-instances').mkdir();(root/'portfolios').mkdir()
        (root/(first.strategy_id+'.json')).write_text(json.dumps(self.expected.sources[first.strategy_id]),encoding='utf-8')
        (root/'strategy-instances'/(member['templateInstanceId']+'.json')).write_text(json.dumps(self.expected.instances[member['templateInstanceId']]),encoding='utf-8')
        path=root/'portfolios/one.json';path.write_text(json.dumps(portfolio),encoding='utf-8')
        single=load_portfolio_runtime_path(path,artifact_root=root).specs[0]
        self.expected=replace(self.expected,specs=(single,),portfolio=portfolio,
            sources={single.strategy_id:self.expected.sources[single.strategy_id]},
            instances={member['templateInstanceId']:self.expected.instances[member['templateInstanceId']]},
            segments={single.strategy_instance_id:self.expected.segments[single.strategy_instance_id]},
            label_acceptances={single.strategy_instance_id:self.expected.label_acceptances[single.strategy_instance_id]})
        self.specs=(single,);self.data=build_native_rest_initialization(expected=self.expected)
        self.feed=Feed(self.data,self.specs,self.clock)
        prepared=self.prepare();self.activate(prepared)
        with tempfile.TemporaryDirectory() as temporary:
            engine=self.engine(prepared.receipt,Path(temporary)/'engine.json')
            self.assertEqual('',engine.transition_mode('SMALL_LIVE',restore_context=self.attestation(engine)))
            supervisor=ContinuousRuntimeSupervisor(engine=engine,feeds=prepared.feeds,prepared_feed_handoff=prepared,
                utc_clock=self.clock.utc,monotonic_clock=self.clock.mono)
            engine.cycle_handler=lambda _:supervisor._stop.set()
            supervisor.run_forever()
            self.assertEqual(1,engine.decision_count)
            self.assertEqual(50,len(next(iter(engine.histories.values())))-1)
            self.assertEqual(original_sources[single.strategy_id],self.expected.sources[single.strategy_id])

    def test_portfolio_less_kis_source_is_human_readable_hold_before_capital_or_yahoo_fallback(self):
        from live_trader.continuous_live import LiveContinuousController
        from live_trader import state
        with tempfile.TemporaryDirectory() as temporary:
            controller=LiveContinuousController(Path(temporary))
            with patch.object(controller,'_dispatch_status',return_value={'reconciliationRequired':False}),\
                    patch.object(controller,'_select_standalone_strategy',return_value={'source_path':'synthetic-source.json','permissions':{}}),\
                    patch.object(controller,'_standalone_spec',return_value=self.specs[0]),\
                    patch.object(controller,'_ensure_native_runtime') as account,\
                    patch('live_trader.continuous_live.feeds_for_specs') as public,\
                    patch.object(state,'snapshot',return_value={}):
                result=controller.start('stock','SMALL_LIVE',strategy_id=self.specs[0].strategy_id)
            self.assertFalse(result['ok']);self.assertEqual('KIS_NATIVE_SEALED_PORTFOLIO_REQUIRED',result['reasonCode'])
            self.assertIn('포트폴리오',result['reason']);account.assert_not_called();public.assert_not_called()

    def test_starting_session_does_not_start_supervisor_before_exact_running_cas(self):
        prepared=self.prepare()
        self.session={'sessionId':'fixture-session','deploymentId':self.scope.deployment_id,
            'deploymentManifestHash':'1'*64,'profile':'stock','mode':'SMALL_LIVE','runtimeInstanceId':'fixture-process',
            'lifecycle':'STARTING','createdAt':'2026-10-16T05:10:04Z','metadata':self.scope.to_dict()}
        reader=lambda:deepcopy(self.session)
        prepared.bind_activation(session=self.session,activation_reader=reader)
        with tempfile.TemporaryDirectory() as temporary:
            engine=self.engine(prepared.receipt,Path(temporary)/'engine.json')
            self.assertEqual('',engine.transition_mode('SMALL_LIVE',restore_context=self.attestation(engine)))
            with self.assertRaises(RuntimeError):prepared.claim_for_supervisor(feeds=prepared.feeds,engine=engine)
            self.session['lifecycle']='RUNNING'
            wrong={**self.session,'createdAt':'2026-10-16T05:10:05Z'}
            with self.assertRaises(RuntimeError):prepared.confirm_running(session=wrong,activation_reader=lambda:wrong)
            prepared.confirm_running(session=self.session,activation_reader=reader)
            self.assertEqual({'kis':True},prepared.claim_for_supervisor(feeds=prepared.feeds,engine=engine))
        prepared.close()

    def test_running_confirmation_cannot_renew_original_fifteen_second_activation_lease(self):
        prepared=self.prepare()
        self.session={'sessionId':'fixture-session','deploymentId':self.scope.deployment_id,
            'deploymentManifestHash':'1'*64,'profile':'stock','mode':'SMALL_LIVE','runtimeInstanceId':'fixture-process',
            'lifecycle':'STARTING','createdAt':'2026-10-16T05:10:04Z','metadata':self.scope.to_dict()}
        reader=lambda:deepcopy(self.session)
        prepared.bind_activation(session=self.session,activation_reader=reader)
        self.clock.tick+=15.1;self.session['lifecycle']='RUNNING'
        with self.assertRaisesRegex(RuntimeError,'CLAIM_EXPIRED'):
            prepared.confirm_running(session=self.session,activation_reader=reader)
        self.assertFalse(prepared._running_confirmed)
        prepared.close()

    def test_actual_activation_crossing_five_minute_boundary_is_not_forward_evidence(self):
        prepared=self.prepare()
        self.session={'sessionId':'fixture-session','deploymentId':self.scope.deployment_id,
            'deploymentManifestHash':'1'*64,'profile':'stock','mode':'SMALL_LIVE','runtimeInstanceId':'fixture-process',
            'lifecycle':'STARTING','createdAt':'2026-10-16T05:10:04Z','metadata':self.scope.to_dict()}
        reader=lambda:deepcopy(self.session)
        prepared.bind_activation(session=self.session,activation_reader=reader)
        self.clock.now=datetime(2026,10,16,5,15,0,tzinfo=timezone.utc);self.session['lifecycle']='RUNNING'
        with self.assertRaisesRegex(RuntimeError,'PREDECESSOR_EXPIRED'):
            prepared.confirm_running(session=self.session,activation_reader=reader)
        self.assertFalse(prepared._running_confirmed)
        prepared.close()

    def test_delayed_worker_claim_rechecks_current_cutoff_without_changing_running_guard(self):
        prepared=self.prepare();self.activate(prepared)
        with tempfile.TemporaryDirectory() as temporary:
            engine=self.engine(prepared.receipt,Path(temporary)/'engine.json')
            self.assertEqual('',engine.transition_mode('SMALL_LIVE',restore_context=self.attestation(engine)))
            self.clock.now=datetime(2026,10,16,5,15,0,tzinfo=timezone.utc)
            # An already running handoff is long lived; only the first claim expires.
            prepared.assert_current()
            with self.assertRaisesRegex(RuntimeError,'PREDECESSOR_EXPIRED'):
                prepared.claim_for_supervisor(feeds=prepared.feeds,engine=engine)
        prepared.close()

    def test_manager_activation_releases_both_manager_locks_before_controller_thread_start(self):
        from live_trader.continuous_live import LiveContinuousRuntimeManager
        manager=object.__new__(LiveContinuousRuntimeManager)
        manager._control_lock=threading.RLock();manager._lock=threading.RLock()
        marker=object()
        def activate(value):
            self.assertIs(marker,value)
            self.assertFalse(manager._control_lock._is_owned());self.assertFalse(manager._lock._is_owned())
            return {'ok':True,'runtimeStarted':True}
        manager.runtimes={'fixture':SimpleNamespace(activate_prepared_native_runtime=activate)}
        manager.runtime_scopes={'fixture':self.scope};manager._writer_leases={'fixture':SimpleNamespace(held=True)}
        self.assertTrue(manager.activate_prepared_native_runtime('fixture',marker)['runtimeStarted'])

    def test_stop_revocation_between_controller_reservation_and_unlocked_start_prevents_poll_thread(self):
        from live_trader.continuous_live import LiveContinuousController
        prepared=self.prepare();self.activate(prepared)
        with tempfile.TemporaryDirectory() as temporary:
            controller=LiveContinuousController(Path(temporary));start=Mock()
            controller._prepared_native_context=prepared
            controller.supervisor=SimpleNamespace(phase='PREPARED',running=False,start=start)
            original=prepared.assert_activation_claimable;calls=[]
            def racing_stop():
                calls.append(None)
                if len(calls)==2:
                    self.assertFalse(controller._lock._is_owned())
                    prepared.revoke('concurrent-stop')
                original()
            with patch.object(prepared,'assert_activation_claimable',side_effect=racing_stop):
                with self.assertRaisesRegex(RuntimeError,'REVOKED'):
                    controller.activate_prepared_native_runtime(prepared)
            start.assert_not_called()
        prepared.close()

    def test_explicit_noncallable_source_reader_is_not_legacy_absence(self):
        self.feed.market_source_integrity_snapshot=None
        with self.assertRaisesRegex(RuntimeError,'SOURCE_READER_INVALID'):
            self.prepare()
        self.assertEqual(1,self.feed.disconnected)
        self.feed=Feed(self.data,self.specs,self.clock);prepared=self.prepare()
        self.feed.market_source_integrity_snapshot=False
        with self.assertRaisesRegex(RuntimeError,'SOURCE_READER_INVALID'):
            self.activate(prepared)
        prepared.close()

    def test_self_resealed_label_file_cannot_expand_real_independent_minute_scope(self):
        label=deepcopy(next(iter(self.expected.label_acceptances.values())))
        actual=public_preparation._CODE_PINNED_LABEL_REVIEWS[0]
        label.update(symbol=actual['symbol'],businessDate=actual['businessDate'],scopeStart=actual['scopeStart'],
            scopeEnd=actual['scopeEnd'],evidenceHashes=list(actual['evidenceHashes']))
        label['contentHash']=stable_sha256({k:v for k,v in label.items() if k!='contentHash'})
        public_preparation._assert_labels_within_independent_review({'fixture':label},(actual,))
        for field,value in (('scopeStart','2026-10-16T01:00:00Z'),('scopeEnd','2026-10-16T05:10:00Z'),
                ('symbol','000660'),('businessDate','20261019'),('evidenceHashes',['f'*64])):
            changed={**label,field:value};changed['contentHash']=stable_sha256({k:v for k,v in changed.items() if k!='contentHash'})
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'REVIEW_SCOPE_UNPROVEN'):
                public_preparation._assert_labels_within_independent_review({'fixture':changed},(actual,))

    def test_missing_or_changed_independent_public_artifact_never_uses_recorded_c_absolute_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError,'PROOF_MISSING'):
                public_preparation._load_code_pinned_label_scopes(temporary)
            folder=Path(temporary)/'logs/native_initialization/label_proofs';folder.mkdir(parents=True)
            (folder/'tick-label-actual-independent-recalculation-v2.json').write_text('{}',encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'PROOF_CHANGED'):
                public_preparation._load_code_pinned_label_scopes(temporary)

    def test_full_lookback_self_reviewed_labels_cannot_reach_oauth_or_feed_without_pinned_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            plan={'loaded':SimpleNamespace(specs=self.specs,payload=self.expected.portfolio),'context':self.context,
                'runtimeScope':self.scope.to_dict(),'promotionPolicy':self.expected.promotion_policy,
                'labelAcceptances':self.expected.label_acceptances}
            with patch.object(ReadOnlyBrokerReader,'_kis_token') as token,\
                    patch.object(public_preparation,'feeds_for_specs') as feed:
                with self.assertRaisesRegex(ValueError,'INDEPENDENT_LABEL_PROOF_MISSING'):
                    prepare_live_kis_native(temporary,plan=plan,binding_reader=lambda:self.binding,preparation_guard=lambda:None,
                        settings={},utc_clock=self.clock.utc,monotonic_clock=self.clock.mono)
                token.assert_not_called();feed.assert_not_called()

    def test_native_confirmation_window_begins_once_after_bounded_account_read_without_restamping_public_data(self):
        prepared=self.prepare();original=deepcopy(prepared.receipt)
        self.clock.tick+=14
        prepared.begin_native_confirmation_window()
        self.clock.tick+=40
        prepared.assert_prepared();self.assertEqual(original,prepared.receipt)
        with self.assertRaisesRegex(RuntimeError,'ALREADY_STARTED'):prepared.begin_native_confirmation_window()
        self.clock.tick+=6
        with self.assertRaisesRegex(RuntimeError,'CONFIRMATION_EXPIRED'):prepared.assert_prepared()
        prepared.close()
        self.feed=Feed(self.data,self.specs,self.clock);prepared=self.prepare();self.clock.tick+=15.1
        with self.assertRaisesRegex(RuntimeError,'PREFLIGHT_EXPIRED'):prepared.begin_native_confirmation_window()
        prepared.close()

    def test_original_rsi_source_produces_real_buy_on_first_synthetic_forward_drop(self):
        spec=next(spec for spec in self.specs if spec.symbol=='000660')
        original=deepcopy(spec.artifact)
        engine=self.engine(self.receipt())
        history=tuple(engine.histories[spec.strategy_instance_id])
        bar=ClosedBar(spec.instrument_id,spec.symbol,'kis','5m','2026-10-16T05:10:00Z','2026-10-16T05:15:00Z',
            100550,100550,95000,95000,10,source_provider='kis',source_sequence='synthetic-forward-only')
        decision=BuiltinBarSignalEvaluator()(spec,bar,(*history,bar))
        self.assertEqual('BUY',decision.signal);self.assertLessEqual(decision.metadata['rsi'],30)
        self.assertEqual(original,spec.artifact)

    def test_blocked_private_reads_allow_status_crypto_control_and_exact_stop_without_late_publication(self):
        from live_trader.continuous_live import LiveContinuousRuntimeManager
        from live_trader import state
        for point in ('ensure','restore','portfolio'):
            with self.subTest(point=point),tempfile.TemporaryDirectory() as temporary:
                self.setUp()
                root,loaded,prepared=self.canonical_live_fixture(temporary)
                manager=LiveContinuousRuntimeManager(root,account_writer_lease_root=root/'writer-leases',
                    scope_resolver=lambda **kw:self.scope if kw['profile_id']=='stock' else None)
                controller=manager._register_scope(self.scope)
                entered,released=threading.Event(),threading.Event();results=[]
                def read(which,value):
                    self.assertFalse(controller._lock._is_owned())
                    self.assertFalse(manager._lock._is_owned());self.assertFalse(manager._control_lock._is_owned())
                    self.assertFalse(state.RUNTIME_MODE_LOCK.owned_by_current_thread())
                    if point==which:
                        entered.set();self.assertTrue(released.wait(4),'private fixture release timed out')
                    return value
                with ExitStack() as stack:
                    for name,value in (('_select_runtime_portfolio',{'source_path':str(prepared.portfolio_path)}),
                            ('_dispatch_status',{'reconciliationRequired':False}),('_paper_final_binding_blocker',''),
                            ('_loaded_portfolio_lineage_blocker',''),('_configure_portfolio_execution',''),
                            ('_spec_mode_allowed',True),('_runtime_position_quantity',0),('_dispatcher',nullcontext())):
                        stack.enter_context(patch.object(controller,name,return_value=value))
                    stack.enter_context(patch.object(controller,'_ensure_native_runtime',side_effect=lambda *a,**kw:read('ensure',None)))
                    stack.enter_context(patch.object(controller,'_restore_context_assessment',side_effect=lambda specs,engine:read('restore',self.attestation(engine))))
                    stack.enter_context(patch.object(controller,'_portfolio_reconciliation_blocker',side_effect=lambda *a,**kw:read('portfolio','')))
                    stack.enter_context(patch.object(state,'LIVE_CONTINUOUS_CONTROLLER',manager))
                    stack.enter_context(patch.object(state,'snapshot',return_value={}))
                    stack.enter_context(patch.object(state,'append_audit'))
                    stack.enter_context(patch.object(manager.controllers['stock'],'_dispatch_status',return_value={'reconciliationRequired':False}))
                    crypto=stack.enter_context(patch.object(manager.controllers['crypto'],'start',return_value={'ok':True}))
                    def start():
                        try:results.append(manager.start('stock','SMALL_LIVE',loaded.portfolio_id,
                            deployment_id=self.scope.deployment_id,prepared_native_context=prepared))
                        except Exception as error:results.append(error)
                    worker=threading.Thread(target=start,daemon=True);worker.start()
                    try:
                        self.assertTrue(entered.wait(2),'fixture did not reach the actual controller read seam')
                        current_lease=manager._writer_leases[self.scope.runtime_id]
                        observed=threading.Event()
                        observer=threading.Thread(target=lambda:(manager.snapshot(),observed.set()),daemon=True);observer.start()
                        self.assertTrue(observed.wait(1),'status blocked behind private broker read')
                        controlled=threading.Event();crypto_results=[]
                        control=threading.Thread(target=lambda:(crypto_results.append(manager.start('crypto','MONITOR')),controlled.set()),daemon=True)
                        control.start();self.assertTrue(controlled.wait(1),'other account control blocked behind private broker read')
                        self.assertTrue(crypto_results[0]['ok']);crypto.assert_called_once()
                        stop_results=[];stopped=threading.Event()
                        stop=threading.Thread(target=lambda:(stop_results.append(manager.stop(runtime_id=self.scope.runtime_id,
                            deployment_id=self.scope.deployment_id)),stopped.set()),daemon=True)
                        stop.start();self.assertTrue(stopped.wait(1),'STOP blocked behind private broker read')
                        self.assertTrue(stop_results[0]['ok']);self.assertTrue(stop_results[0]['writerLeaseRetained'])
                        self.assertTrue(current_lease.held);self.assertTrue(prepared._revoked.is_set())
                    finally:
                        released.set();worker.join(4);prepared.close()
                    self.assertFalse(worker.is_alive());self.assertIsInstance(results[0],dict)
                    self.assertFalse(results[0]['ok']);self.assertIsNone(controller.supervisor)
                    self.assertFalse(current_lease.held);self.assertNotIn(self.scope.runtime_id,manager._native_start_inflight)

    def test_stop_linearized_after_local_commit_does_not_publish_cancelled_native_reader_or_engine(self):
        from live_trader.continuous_live import LiveContinuousController
        from live_trader import state
        with tempfile.TemporaryDirectory() as temporary:
            root,loaded,prepared=self.canonical_live_fixture(temporary)
            controller=LiveContinuousController(root);controller.bind_runtime_scope(self.scope)
            committed=[]
            def commit(fact,*,preparation_guard):
                preparation_guard();committed.append(fact)
                prepared.revoke('STOP_AFTER_LOCAL_TRANSACTION_LINEARIZED')
            staged=SimpleNamespace(commit_manual_start_initialization=commit,context=lambda *a:None)
            with ExitStack() as stack:
                for name,value in (('_select_runtime_portfolio',{'source_path':str(prepared.portfolio_path)}),
                        ('_dispatch_status',{'reconciliationRequired':False}),('_paper_final_binding_blocker',''),
                        ('_loaded_portfolio_lineage_blocker',''),('_configure_portfolio_execution',''),
                        ('_spec_mode_allowed',True),('_ensure_native_runtime',(staged,{'localTransaction':'committed'}))):
                    stack.enter_context(patch.object(controller,name,return_value=value))
                stack.enter_context(patch.object(state,'snapshot',return_value={}))
                result=controller.start('stock','SMALL_LIVE',loaded.portfolio_id,
                    deployment_id=self.scope.deployment_id,prepared_native_context=prepared)
            self.assertFalse(result['ok']);self.assertEqual(1,len(committed))
            self.assertIsNone(controller._native_runtime);self.assertIsNone(controller.supervisor)
            prepared.close()

    def test_three_equal_source_weights_reach_bounded_decimal_consumer_without_overallocation(self):
        from live_trader.continuous_live import LiveContinuousController
        from live_trader.native_spot_quantity import decimal_value
        with tempfile.TemporaryDirectory() as temporary:
            root,loaded,prepared=self.canonical_live_fixture(temporary)
            controller=LiveContinuousController(root);controller.deployment_id=self.scope.deployment_id
            controller.native_context_provider=lambda *args:{}
            original=[deepcopy(spec.artifact) for spec in loaded.specs];allocations=[]
            def consumer(spec,*,allocation_weight,**kwargs):
                allocation=decimal_value(allocation_weight,'allocation_weight',positive=True)
                self.assertLessEqual(len(allocation.as_tuple().digits),128)
                allocations.append(allocation)
                return {'status':'READY','quantityForSubmission':1}
            with patch('live_trader.kis_equity_execution.live_equity_quantity',side_effect=consumer):
                for spec in loaded.specs:
                    result=controller._native_quantity_result(spec,'BUY',95000,0,loaded.specs)
                    self.assertEqual('READY',result['status'],result)
            self.assertEqual(3,len(allocations));self.assertEqual(1,len(set(allocations)))
            with localcontext() as precision:
                precision.prec=2048
                self.assertLessEqual(sum(allocations),Decimal(1))
                for spec,allocation in zip(loaded.specs,allocations):
                    exact=Decimal(str(spec.target_weight))/sum(Decimal(str(row.target_weight)) for row in loaded.specs)
                    self.assertLessEqual(allocation,exact)
            self.assertEqual(original,[spec.artifact for spec in loaded.specs]);prepared.close()


if __name__=='__main__':unittest.main()
