"""Connected native KR graph -> state start -> actual runtime/OMS -> offline ACK.

Only external public/account/review/OS-dialog producers use explicit synthetic
fixtures.  Source evaluation, native sizing/ticket, runtime market guards,
scoped consent/15s one-use wire claim and local journals remain real.
"""
from contextlib import ExitStack,closing
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
import os,sqlite3,threading,time,unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch

from live_trader import state,contracts,kis_order_authority as authority
from live_trader.continuous_live import LiveContinuousRuntimeManager
from live_trader.runtime_scope import RuntimeScope
from live_trader.operational_governance import OperationalGovernanceStore
from live_trader.kis_scoped_operational_authority import KisScopedOperationalAuthority
from live_trader.kis_native_start_registry import KisNativeStartRegistry
from live_trader.kis_native_start_preparation_jobs import KisNativePreparationJobs
from live_trader.kis_operational_native_confirmation import KisNativeOperationalStartConfirmations
from live_trader.live_adapters import build_kis_live_order_request,kis_prepared_payload_hash
from live_trader.native_order_ticket import validate_native_ticket
from trading_runtime.continuous_runtime import ClosedBar,OpenBoundary
from trading_runtime.artifact_governance import artifact_reference,verify_strategy_artifact,stable_sha256
import test_live_kis_native_bootstrap as bootstrap
import test_order_dispatch_safety as safety
import test_native_fixture_support as native_support
from test_kis_native_start_registry import Timer,QueuedThread


class NativeFullFlowTests(unittest.TestCase):
    def test_canonical_async_native_start_real_buy_queue_exact_wire_ack_then_stop(self):
        external=safety.OrderDispatchSafetyTest();external.setUp()
        self.addCleanup(external.doCleanups);self.addCleanup(external.tearDown)
        external.integrity_fixture.stop()
        stack=ExitStack();self.addCleanup(stack.close)
        stack.enter_context(patch.dict(os.environ,native_support.SETTINGS))
        fixture=bootstrap.LiveKisNativeBootstrapTests();fixture.setUp()
        account=native_support.native_fixture_account()
        fixture.scope=RuntimeScope('kis',account['accountFingerprint'],'fixture-deployment','stock',account['accountBindingId'],account['credentialConfigurationHash'],'KRW')
        fixture.context.update(runtimeScopeId=fixture.scope.runtime_id,accountFingerprint=fixture.scope.account_fingerprint,
            credentialConfigurationHash=fixture.scope.credential_configuration_hash)
        wire_account,wire_credentials=state._kis_environment_order_authority_binding()
        fixture.context.update(wireAccountFingerprint=wire_account,wireCredentialConfigurationHash=wire_credentials)
        fixture.binding.update(context=fixture.context,runtimeScope=fixture.scope.to_dict())
        root=Path(external.temporary.name)/'native';root.mkdir()
        real_seal=bootstrap.seal_strategy_artifact
        def offline_next_open_source(source):
            # This test creates its Source before sealing. Runtime never edits
            # a Source, and the complete native receipt binds its resulting hash.
            return real_seal({**source,'executionPolicy':{'executionTiming':'NEXT_BAR_OPEN','feePercent':0.1,'slippagePercent':0.2}})
        real_portfolio_seal=bootstrap.seal_portfolio_artifact
        def offline_portfolio(source):
            return real_portfolio_seal({**source,'executionPolicy':{**source.get('executionPolicy',{}),
                'executionTiming':'NEXT_BAR_OPEN','feePercent':0.1,'slippagePercent':0.2}})
        with patch.object(bootstrap,'seal_strategy_artifact',side_effect=offline_next_open_source),patch.object(bootstrap,'seal_portfolio_artifact',side_effect=offline_portfolio):
            root,loaded,obsolete_prepared=fixture.canonical_live_fixture(root,activate=False)
        # Construct the scientific public receipt against the actual immutable
        # governance manifest, rather than editing a sealed receipt in place.
        obsolete_prepared.close()
        governance=OperationalGovernanceStore(root/'governance.sqlite3',clock=fixture.clock.utc)
        manifest=governance.create_deployment_manifest(deployment_id=fixture.scope.deployment_id,
            strategy_artifact_hash=stable_sha256([s.artifact_hash for s in loaded.specs]),portfolio_artifact_hash=loaded.portfolio_hash,
            account_fingerprint=account['accountFingerprint'],broker_route='kis',runtime_version='3.14.0',build_hash='a'*64,
            execution_adapter='kis-rest',execution_adapter_version='offline',risk_policy_revision=1,risk_policy_hash='a'*64,
            config_revision=1,config_hash='b'*64)
        fixture.binding['deploymentManifestHash']=manifest.manifest_hash
        fixture.feed=bootstrap.Feed(fixture.data,fixture.specs,fixture.clock)
        prepared=fixture.prepare(portfolio_path=root/'portfolios/portfolio.json',artifact_root=root)
        registry=KisNativeStartRegistry(monotonic_clock=fixture.clock.mono,timer_factory=Timer,close_dispatch=lambda obj:obj.close())
        QueuedThread.pending=[]
        jobs=KisNativePreparationJobs(registry=registry,clock=fixture.clock.mono,thread_factory=QueuedThread,timer_factory=Timer)
        native=KisNativeOperationalStartConfirmations(prompt=lambda value:value.expected_phrase,
            clock=lambda:fixture.clock.utc().timestamp(),monotonic_clock=fixture.clock.mono)
        scoped=KisScopedOperationalAuthority(root/'scoped.sqlite3',clock=lambda:fixture.clock.utc().timestamp())
        grants=[];wire_claims=[]
        real_authorize=scoped.authorize;real_consume=scoped.consume_wire
        def authorize(*args,**kwargs):
            result=real_authorize(*args,**kwargs);grants.append((deepcopy(kwargs['fresh_evidence']),result));return result
        def consume(*args,**kwargs):
            real_consume(*args,**kwargs);wire_claims.append((args,deepcopy(kwargs)))
        stack.enter_context(patch.object(scoped,'authorize',side_effect=authorize))
        stack.enter_context(patch.object(scoped,'consume_wire',side_effect=consume))
        class FrozenDateTime(datetime):
            @classmethod
            def now(cls,tz=None):
                value=fixture.clock.utc()
                return value.astimezone(tz) if tz else value.replace(tzinfo=None)
        stack.enter_context(native_support.native_fixture_clock(fixture.clock.utc))
        stack.enter_context(patch.object(state,'datetime',FrozenDateTime))
        for module in ('trading_runtime.continuous_runtime','live_trader.native_open_boundary'):
            stack.enter_context(patch(module+'.datetime',FrozenDateTime))
        real_native=native_support.build_native_runtime(root,specs=loaded.specs,runtime_scope=fixture.scope.to_dict(),
            clock=fixture.clock.utc,dispatch_reader=lambda:external.ledger.order_dispatch_rows(1000))
        stack.enter_context(patch('live_trader.kis_equity_runtime.NativeEquityRuntime',return_value=real_native.runtime))
        stack.enter_context(patch('live_trader.kis_native_start_registry.KIS_NATIVE_START_REGISTRY',registry))
        stack.enter_context(patch('live_trader.kis_native_start_preparation_jobs.KIS_NATIVE_PREPARATION_JOBS',jobs))
        stack.enter_context(patch('live_trader.kis_operational_native_confirmation.KIS_NATIVE_OPERATIONAL_START_CONFIRMATIONS',native))
        stack.enter_context(patch('live_trader.kis_native_public_preparation.prepare_live_kis_native',return_value=prepared))
        stack.enter_context(patch.object(state,'APP_DATA_ROOT',root))
        stack.enter_context(patch.object(state,'OPERATIONAL_GOVERNANCE',governance))
        stack.enter_context(patch.object(state,'_kis_scoped_store',return_value=scoped))
        stack.enter_context(patch.object(state,'snapshot',return_value={}))
        stack.enter_context(patch.object(state,'append_audit'))
        stack.enter_context(patch.object(state,'runtime_settings',return_value=dict(native_support.SETTINGS)))
        stack.enter_context(patch.object(state,'_upbit_functional_durable_authority_open',return_value=False))
        stack.enter_context(patch.object(state,'_kis_application_owner_epoch',return_value=('fixture-owner','f'*64,True)))
        stack.enter_context(patch('live_trader.process_safety.hold_kis_dispatch_lease',return_value={'acquired':True}))
        stack.enter_context(patch('live_trader.process_safety.held_process_lease_status',return_value={'acquired':True}))
        stack.enter_context(patch('live_trader.kis_order_authority.kis_specialized_operational_fence_status',return_value={'installed':True,'routeHeld':True,'mutationInFlight':False}))
        # External reviewed Paper/config preconditions are explicit fixtures;
        # no such fixture grants native Source/market/order authority itself.
        rows=[]
        for spec in loaded.specs:
            source=fixture.expected.sources[spec.strategy_id];verified=verify_strategy_artifact(source)
            rows.append({'symbol':spec.symbol,'artifact_reference':artifact_reference(source),
                'artifact_integrity':{'valid':verified.valid,'declaredHash':verified.declared_hash,'computedHash':verified.computed_hash},
                'paper_live_qualification':{'required':True,'ready':True,'portfolioRequired':True,
                    'strategyInstanceId':spec.strategy_instance_id,'portfolioArtifactId':loaded.portfolio_id,
                    'portfolioArtifactHash':loaded.portfolio_hash,'promotionPolicy':fixture.expected.promotion_policy,
                    'promotionPolicyHash':fixture.expected.promotion_policy['policyHash']}})
        portfolio_row={'id':loaded.portfolio_id,'source_path':str(root/'portfolios/portfolio.json'),'broker_id':'kis'}
        stack.enter_context(patch.object(state,'portfolio_rows',return_value=[portfolio_row]))
        stack.enter_context(patch.object(contracts,'strategy_artifact_dirs',return_value=[root]))
        stack.enter_context(patch.object(state,'load_strategy_artifacts',return_value=rows))
        stack.enter_context(patch.object(state,'resolve_continuous_runtime_scope',return_value=fixture.scope.to_dict()))
        stack.enter_context(patch.object(state,'_operational_strategy',return_value=portfolio_row))
        stack.enter_context(patch.object(state,'strategy_rows',return_value=[portfolio_row]))
        stack.enter_context(patch.object(state,'strategy_broker_id',return_value='kis'))
        fresh={'poll':{'ok':True,'coalesced':False},'summary':{'fresh':True,'three_way_verified':True,'status':'pass','evidence_ages_seconds':{'account':0}},'errors':[]}
        stack.enter_context(patch.object(state,'refresh_preflight_reconciliation',return_value=fresh))
        stack.enter_context(patch.object(state,'broker_readiness',return_value=[]))
        stack.enter_context(patch.object(state,'final_preflight_checks',return_value=[{'label':'synthetic external reviewed truth','status':'PASS','detail':'synthetic'}]))
        def material(*args,require_fresh_preflight=True,**kwargs):
            preflight=governance.latest_preflight_for_deployment(fixture.scope.deployment_id)
            if require_fresh_preflight and (preflight is None or governance.preflight_validity(preflight.snapshot_id)['valid'] is not True):
                raise ValueError('synthetic external preflight not current')
            return deepcopy(fixture.context),preflight
        stack.enter_context(patch.object(state,'_kis_scoped_start_material',side_effect=material))
        def runtime_session(*args):
            preflight=governance.latest_preflight_for_deployment(fixture.scope.deployment_id)
            session=governance.create_runtime_session(deployment_id=fixture.scope.deployment_id,
                deployment_manifest_hash=manifest.manifest_hash,profile='stock',mode='SMALL_LIVE',runtime_instance_id='synthetic-process',
                preflight_snapshot_id=preflight.snapshot_id,metadata={**fixture.scope.to_dict(),'portfolioId':loaded.portfolio_id,'strategyId':fixture.context['strategyId']})
            session=governance.transition_runtime_session(session.session_id,'STARTING',actor='offline fixture',event_type='RUNTIME_START_REQUESTED')
            state.STATE.setdefault('active_runtime_session_ids',{})[fixture.scope.runtime_id]=session.session_id
            return session,'synthetic external approved binding'
        stack.enter_context(patch.object(state,'_prepare_operational_runtime_session',side_effect=runtime_session))
        manager=LiveContinuousRuntimeManager(root,account_writer_lease_root=root/'locks',scope_resolver=lambda **kwargs:fixture.scope)
        stack.enter_context(patch.object(state,'LIVE_CONTINUOUS_CONTROLLER',manager))
        controller=manager._register_scope(fixture.scope)
        for name,value in (('_select_runtime_portfolio',portfolio_row),('_paper_final_binding_blocker',''),
                ('_loaded_portfolio_lineage_blocker',''),('_configure_portfolio_execution',''),('_portfolio_reconciliation_blocker',''),('_spec_mode_allowed',True)):
            stack.enter_context(patch.object(controller,name,return_value=value))
        stack.enter_context(patch.object(state,'real_orders_enabled',return_value=True))
        stack.enter_context(patch.object(state,'durable_control_halt_active',return_value=False))
        stack.enter_context(patch.object(state,'operational_runtime_dispatch_allowed',return_value=(True,'external reviewed runtime',{})))
        stack.enter_context(patch.object(state,'current_live_canary_scope',side_effect=lambda strategy_id,**kwargs:external.eligible_canary_scope(SimpleNamespace(strategy_id=strategy_id))))
        stack.enter_context(patch.object(state,'portfolio_gate_for_intent',return_value={'active':True,'allowed':True,'portfolioId':loaded.portfolio_id}))
        stack.enter_context(patch.object(state,'evaluate_order_gate_with_report',return_value=(True,'approved','ready','passed',external.passing_report())))
        stack.enter_context(patch.dict(state.STATE,{'active_runtime_session_ids':{},'orders':[],'kill_switch':False,
            'new_entries_blocked':False,'mode':'SMALL_LIVE','dry_run':False,'operator_confirmed':True}))
        def authority_snapshot():
            return {'durableAuthorityReadable':True,'functionalAuthorityOpen':True,'functionalPhase':'ARMED_WAIT_PUBLIC',
                'functionalRevision':1,'stateRevision':state._KIS_ROUTE_STATE_REVISION,'ownerEpochId':'fixture-owner','ownerEpochHash':'f'*64,
                'functionalSessionId':'','functionalAccountFingerprint':wire_account,'credentialConfigurationHash':wire_credentials,
                'functionalMutationIntent':{},'killOrdinaryCancelAllowed':False,'killOrdinaryCancelRevision':0,'killOrdinaryCancelIntent':{},
                'applicationInstanceLeaseHeld':True,'ordinaryRoutesClosed':True,'controlReservation':deepcopy(state._KIS_CONTROL_RESERVATION)}
        stack.enter_context(patch.object(state,'_kis_order_authority_snapshot',side_effect=authority_snapshot))
        stack.enter_context(patch.object(authority,'_AUTHORITY_READER',authority_snapshot))
        # Other boundary tests deliberately reset import-time registrations.
        # Reinstall the actual state resolver for this fixture; never a fake
        # allow result, and restore the prior registry on teardown.
        stack.enter_context(patch.object(authority,'_SCOPED_OPERATIONAL_READER',state._kis_scoped_operational_dispatch_authority))
        stack.enter_context(patch.object(authority,'_SPECIALIZED_EXCLUSION_READER',state._kis_scoped_specialized_exclusion_snapshot))
        router=Mock();router.list_positions.return_value=[]
        posted=threading.Event();allow_poll=threading.Event();wire_calls=[];intents=[];gate_checks=[]
        original_market_guard=state.runtime_market_integrity_dispatch_allowed
        def market_guard(intent):
            outcome=original_market_guard(intent);gate_checks.append(outcome)
            return outcome
        stack.enter_context(patch.object(state,'runtime_market_integrity_dispatch_allowed',side_effect=market_guard))
        original_submit=state.submit_order_intent
        def submit(snapshot,intent,**kwargs):
            intents.append(intent)
            return original_submit(snapshot,intent,**kwargs)
        stack.enter_context(patch.object(state,'submit_order_intent',side_effect=submit))
        def broker(payload):
            with authority.scoped_kis_authenticated_mutation_preflight(payload):
                request=build_kis_live_order_request(payload,access_token='synthetic-memory-token')
                wire={'operation':'PLACE_ORDER','claimId':'offline-claim','ownedOrderKey':{'orderDate':'','organizationNo':'','orderNo':''},
                    'accountFingerprint':wire_account,'credentialConfigurationHash':wire_credentials,
                    'endpoint':request.endpoint,'payloadHash':kis_prepared_payload_hash(request)}
                with authority.scoped_kis_final_mutation_boundary(operation='PLACE_ORDER',intent=wire,prepared_request=request):
                    grant=authority.consume_inherited_kis_transport_authority(endpoint=wire['endpoint'],payload_hash=wire['payloadHash'],
                        account_fingerprint=wire_account,credential_configuration_hash=wire_credentials)
                    wire_calls.append((payload,dict(grant)))
            posted.set()
            return {'ok':True,'statusCode':200,'kisOrderAccountBinding':external.kis_wire_account_binding(),
                'json':{'rt_cd':'0','output':{'ODNO':'123456789','ORD_DT':'20261016','KRX_FWDG_ORD_ORGNO':'001'}}}
        router.place_order.side_effect=broker
        stack.enter_context(patch.object(state,'LiveBrokerRouter',return_value=router))
        def feed_poll(timeout):
            if not allow_poll.wait(.01):return ()
            if getattr(fixture.feed,'forward_sent',False):return ()
            fixture.feed.forward_sent=True
            fixture.clock.now=datetime(2026,10,16,5,15,0,1000,tzinfo=timezone.utc)
            events=[]
            for spec in loaded.specs:
                buy=spec.symbol=='000660'
                events.append(ClosedBar(spec.instrument_id,spec.symbol,'kis','5m','2026-10-16T05:10:00Z','2026-10-16T05:15:00Z',
                    100550 if buy else 103000,100550 if buy else 103010,95000 if buy else 102990,95000 if buy else 103000,10,
                    source_provider='kis',received_time=fixture.clock.utc().isoformat(),source_sequence='synthetic-next-native-close'))
            spec=next(s for s in loaded.specs if s.symbol=='000660')
            events.append(OpenBoundary(spec.instrument_id,spec.symbol,'kis','5m','2026-10-16T05:15:00Z',95000,
                fixture.clock.utc().isoformat(),'synthetic-next-native-open','9'*64,'KIS_WEBSOCKET',True,source_provider='kis'))
            return events
        fixture.feed.poll=feed_poll
        cycle_results=[]
        real_cycle=controller._handle_cycle_locked
        def cycle(*args,**kwargs):
            result=real_cycle(*args,**kwargs);cycle_results.append(result);return result
        stack.enter_context(patch.object(controller,'_handle_cycle_locked',side_effect=cycle))
        real_start=manager.start
        def start(*args,**kwargs):
            result=real_start(*args,**kwargs)
            if controller.supervisor is not None:
                controller.supervisor._utc_clock=fixture.clock.utc;controller.supervisor._monotonic_clock=fixture.clock.mono
            return result
        stack.enter_context(patch.object(manager,'start',side_effect=start))
        try:
            begin=state.prepare_kis_scoped_manual_start('stock','SMALL_LIVE',loaded.portfolio_id,fixture.scope.deployment_id,fixture.context['strategyId'])
            self.assertTrue(begin['ok'],begin);self.assertFalse(controller.supervisor)
            QueuedThread.pending.pop()()
            ready=state.kis_native_preparation_status(begin['preparationId']);self.assertEqual(ready['status'],'READY',ready)
            proof=native.request(ready['nativeNonce']);self.assertTrue(proof['ok'],proof)
            started=state.start_continuous_runtime('stock','SMALL_LIVE',loaded.portfolio_id,fixture.scope.deployment_id,
                fixture.context['strategyId'],safety_confirmation={'nativeAttestation':proof['attestation']})
            self.assertTrue(started['ok'],started)
            self.assertTrue(controller.supervisor.running)
            self.assertEqual(controller.supervisor.engine.decision_count,0)
            self.assertEqual(1,fixture.feed.connected)
            allow_poll.set()
            self.assertTrue(posted.wait(5),{'lastError':controller.supervisor.last_error,'cycle':cycle_results,'dispatch':controller._dispatcher().last_results})
            deadline=time.monotonic()+3
            while time.monotonic()<deadline:
                rows=external.ledger.order_dispatch_rows()
                queued=controller._dispatcher().journal.rows()
                if rows and rows[0]['state']=='acknowledged' and queued and queued[0]['state']=='COMPLETED':break
                time.sleep(.01)
            self.assertEqual(len(wire_calls),1)
            self.assertEqual(len(wire_claims),1)
            self.assertTrue(grants)
            self.assertTrue(all(0<grant['expiresAt']-fixture.clock.utc().timestamp()<=15 for _,grant in grants))
            self.assertEqual(len({grant['permitId'] for _,grant in grants}),1)
            self.assertEqual(intents[0].symbol,'000660');self.assertEqual(intents[0].side,'BUY')
            self.assertTrue(all(value[0] for value in gate_checks),gate_checks)
            self.assertEqual(external.ledger.order_dispatch_rows()[0]['state'],'acknowledged')
            self.assertEqual(controller._dispatcher().journal.rows()[0]['state'],'COMPLETED')
            self.assertEqual(intents[0].metadata['source_artifact_hash'],next(s.artifact_hash for s in loaded.specs if s.symbol=='000660'))
            validate_native_ticket(intents[0])
            with self.assertRaises((ValueError,TypeError,KeyError)):
                validate_native_ticket(intents[0],now=fixture.clock.utc()+timedelta(seconds=16))
            sid=started['runtime_session_id'];scoped.authorize_consent(sid,context=fixture.context)
            with self.assertRaisesRegex(Exception,'already consumed'):
                real_consume(*wire_claims[0][0],**wire_claims[0][1])
            with closing(sqlite3.connect(scoped.path)) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM kis_operational_wire_claims').fetchone()[0],1)
            self.assertFalse(controller.supervisor.engine.live_native_initialization['paperSessionImported'])
            self.assertFalse(controller.supervisor.engine.live_native_initialization['executionAuthorityGranted'])
            self.assertEqual(controller.supervisor.engine.live_native_initialization['deploymentManifestHash'],manifest.manifest_hash)
            stopped=state.stop_continuous_runtime('stock',runtime_id=fixture.scope.runtime_id,deployment_id=fixture.scope.deployment_id)
            self.assertFalse(controller.supervisor.running,stopped)
            self.assertFalse(state.runtime_market_integrity_dispatch_allowed(intents[0])[0])
            with self.assertRaises(Exception):scoped.authorize_consent(sid,context=fixture.context)
            self.assertEqual(len(wire_calls),1);router.cancel_working_orders.assert_not_called()
        finally:
            allow_poll.set()
            manager.stop(runtime_id=fixture.scope.runtime_id,deployment_id=fixture.scope.deployment_id)
            for lease in manager._writer_leases.values():lease.release()

