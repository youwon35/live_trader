import ast
from contextlib import nullcontext
import copy
from datetime import datetime, timedelta, timezone
from functools import wraps
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
from typing import Any, Iterator
import unittest
from unittest.mock import Mock, patch

from live_trader.kis_scoped_operational_authority import KisScopedAuthorityError, KisScopedOperationalAuthority
from test_kis_scoped_operational_authority import context
from live_trader.order_management import OrderIntent


def selected_functions(names):
    source=(Path(__file__).resolve().parents[1]/'live_trader/state.py').read_text(encoding='utf-8')
    nodes=[]
    for node in ast.parse(source).body:
        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in names:
            if node.name=='start_continuous_runtime':
                node.decorator_list=[decorator for decorator in node.decorator_list
                    if isinstance(decorator,ast.Call) and getattr(decorator.func,'id','')=='_two_phase_kis_route_control']
            nodes.append(node)
    if {node.name for node in nodes}!=set(names):raise AssertionError('selected state functions missing')
    return ast.Module(body=nodes,type_ignores=[])


class ScopedStateStartTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store=KisScopedOperationalAuthority(Path(self.directory.name)/'authority.sqlite3')
        self.context=context()
        self.preflight=SimpleNamespace(expires_at=(datetime.now(timezone.utc)+timedelta(seconds=240)).isoformat())
        self.session=SimpleNamespace(session_id='session-a',deployment_id='deployment-alpha',lifecycle='STARTING',
            metadata={'runtimeScopeId':'runtime-alpha','portfolioId':'portfolio-alpha','strategyId':'strategy-alpha'})
        self.state={'mode':'SMALL_LIVE','active_runtime_session_ids':{'runtime-alpha':'session-a'},'new_entries_blocked':False,'kill_switch':False}
        self.controller=Mock()
        self.controller._writer_leases={'runtime-alpha':SimpleNamespace(held=True)}
        self.controller.snapshot.return_value={'runtimes':{'runtime-alpha':{'running':True,'mode':'SMALL_LIVE','profileId':'stock'}}}
        self.controller.start.side_effect=self.manager_started
        governance=Mock()
        governance.get_runtime_session.return_value=self.session
        def transition(session_id,lifecycle,**kwargs):
            self.assertEqual(session_id,self.session.session_id);self.session.lifecycle=lifecycle;return self.session
        governance.transition_runtime_session.side_effect=transition
        self.native=Mock();self.native.consume.return_value={'ok':True}
        patcher=patch('live_trader.kis_operational_native_confirmation.KIS_NATIVE_OPERATIONAL_START_CONFIRMATIONS',self.native)
        patcher.start();self.addCleanup(patcher.stop)
        patcher=patch('live_trader.process_safety.hold_kis_dispatch_lease',return_value={'acquired':True})
        patcher.start();self.addCleanup(patcher.stop)
        patcher=patch('live_trader.kis_order_authority.kis_specialized_operational_fence_status',return_value={'installed':True,'routeHeld':True,'mutationInFlight':False})
        patcher.start();self.addCleanup(patcher.stop)
        self.namespace={'__package__':'live_trader','Any':Any,'Iterator':Iterator,'Mode':str,'wraps':wraps,
            'sqlite3':sqlite3,'time':time,'datetime':datetime,'json':json,'hashlib':hashlib,'secrets':secrets,
            'STATE':self.state,'_KIS_CONTROL_RESERVATION':{},'_KIS_ROUTE_STATE_REVISION':1,
            '_KIS_CONTROL_RESERVATION_SCOPE':threading.local(),'_KIS_CONTROL_SUPERSEDED_IDS':set(),
            '_KIS_SCOPED_PENDING_STARTS':{},'kis_route_authority_serialization':lambda:nullcontext(),
            'RUNTIME_CONTROL_LOCK':threading.RLock(),'RUNTIME_MODE_LOCK':threading.RLock(),
            'OPERATIONAL_GOVERNANCE':governance,'LIVE_CONTINUOUS_CONTROLLER':self.controller,
            '_kis_scoped_store':lambda:self.store,'_kis_scoped_start_material':lambda *a:(copy.deepcopy(self.context),self.preflight),
            '_kis_scoped_authority_status':lambda:self.store.status(),
            '_kis_scoped_operational_dispatch_authority':lambda *a:None,'_kis_scoped_specialized_exclusion_snapshot':lambda:{'status':'UNKNOWN'},
            '_kis_order_authority_snapshot':lambda:{'functionalSessionId':'','functionalMutationIntent':{}},
            'PROGRAM_LEDGER':SimpleNamespace(order_dispatch_rows=lambda limit:[]),
            'functional_test_account_fingerprint':lambda value:'0'*64,'kis_functional_test_account_id':lambda:'fixture-account',
            'normalize_runtime_mode':lambda mode:mode,'_upbit_functional_durable_authority_open':lambda:False,
            'FUNCTIONAL_TEST_EXECUTION_PURPOSE':'FUNCTIONAL_TEST','_FUNCTIONAL_TEST_START_CAPABILITY':object(),
            '_prepare_operational_runtime_session':lambda *a:(self.session,'fixture'),
            '_restore_previous_runtime_session_binding':Mock(),'append_audit':Mock(),'snapshot':lambda:{'mode':self.state['mode']},
            'sync_runtime_profile_mode':Mock(),'RUNTIME_MODE_RANK':{'MONITOR':0,'SMALL_LIVE':1,'FULL_LIVE':2},
            '_retire_superseded_runtime_session':lambda *a:True,'ArtifactMetadataStore':Mock()}
        names=['_advance_kis_route_state_revision','_kis_control_reservation_hash','_two_phase_kis_route_control',
            '_revoke_kis_scoped_operational_authority','_reserve_kis_scoped_manual_start',
            '_finalize_kis_scoped_manual_start','_finish_operational_runtime_start','start_continuous_runtime']
        exec(compile(selected_functions(names),'<isolated state functions>','exec'),self.namespace)
        self.pending_seen=False

    def manager_started(self,*args):
        self.pending_seen=True
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)
        self.assertTrue(self.namespace['_KIS_CONTROL_RESERVATION'])
        self.assertEqual(self.session.lifecycle,'STARTING')
        return {'ok':True,'runtimeStarted':True}

    def start(self,confirmation=None):
        return self.namespace['start_continuous_runtime']('stock','SMALL_LIVE','portfolio-alpha','deployment-alpha','strategy-alpha',
            safety_confirmation={'nativeAttestation':{'opaque':'test-proof'}} if confirmation is None else confirmation)

    def test_success_has_no_authority_until_exact_running_and_finalized_reservation(self):
        result=self.start()
        self.assertTrue(result['ok']);self.assertTrue(self.pending_seen)
        self.assertEqual(result['kisOperationalAuthority']['status'],'ACTIVE')
        self.assertFalse(result['kisOperationalAuthority']['orderAuthorityGranted'])
        self.assertEqual(self.session.lifecycle,'RUNNING')
        self.assertFalse(self.namespace['_KIS_CONTROL_RESERVATION'])
        self.assertTrue(self.store.authorize_consent('session-a',context=self.context)['consentId'])
        self.native.consume.assert_called_once_with({'opaque':'test-proof'},self.context)
        self.native.revoke_all.assert_called_once()

    def test_direct_start_without_native_proof_or_renderer_phrase_never_calls_manager(self):
        for confirmation in ({},{'typedPhrase':'anything'},{'nativeAttestation':{},'typedPhrase':'anything'}):
            with self.subTest(confirmation=confirmation):
                result=self.start(confirmation);self.assertFalse(result['ok'])
        self.controller.start.assert_not_called();self.assertFalse(self.store.path.exists())

    def test_native_rejection_and_scope_change_never_calls_manager(self):
        self.native.consume.return_value={'ok':False}
        self.assertFalse(self.start()['ok']);self.controller.start.assert_not_called()
        self.native.consume.assert_called_once()

    def test_manager_failure_or_start_stop_race_never_activates(self):
        self.controller.start.return_value={'ok':False,'reason':'feed failed'}
        self.controller.start.side_effect=None
        self.assertFalse(self.start()['ok'])
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)
        self.session.session_id='session-b';self.session.lifecycle='STARTING'
        self.state['active_runtime_session_ids']['runtime-alpha']='session-b'
        def stopped(*args):
            self.namespace['_revoke_kis_scoped_operational_authority']('stop-before-join')
            return {'ok':True}
        self.controller.start.side_effect=stopped
        result=self.start()
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-b',context=self.context)
        self.assertFalse(self.namespace['_KIS_SCOPED_PENDING_STARTS'])

    def test_revision_change_before_reservation_finalizer_revokes(self):
        def changed(*args):
            self.context['scopeHash']='0'*64
            return {'ok':True}
        self.controller.start.side_effect=changed
        result=self.start()
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)
        self.assertTrue(self.state['new_entries_blocked'])
        self.assertFalse(result['ok'])
        self.assertEqual(result['kisOperationalAuthority']['status'],'REVOKED')

    def test_lease_loss_or_missing_exact_running_blocks_finalization(self):
        self.controller._writer_leases['runtime-alpha'].held=False
        result=self.start()
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)
        self.assertTrue(self.state['new_entries_blocked'])
        self.assertFalse(result['ok'])
        self.assertEqual(self.session.lifecycle,'DEGRADED')


class ScopedStateWireTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.context=context();self.store=KisScopedOperationalAuthority(Path(self.directory.name)/'authority.sqlite3')
        self.store.establish_exclusion(self.context,proof=dict(routeQuiescent=True,specializedClosureInstalled=True,sameAccountLeaseHeld=True,reconciliationClear=True))
        self.store.reserve(self.context,session_id='session-a',reservation_id='reservation-a',expires_at=time.time()+100)
        self.store.activate('session-a',context=self.context,reservation_id='reservation-a',reservation_finalized=True,session_running=True)
        now=datetime.now(timezone.utc).isoformat()
        self.intent=OrderIntent('strategy-alpha','한국주식','005930','BUY',1,50000,'SMALL_LIVE','fixture',
            {'broker_id':'kis','runtime_scope_id':'runtime-alpha','native_quantity_model':{'sourceArtifactHash':'a'*64,'evaluationKey':'eval-1','boundaryHash':'b'*64,'asOf':now},'native_open_boundary':{'startTime':now}})
        self.session=SimpleNamespace(session_id='session-a',mode='SMALL_LIVE',lifecycle='RUNNING',deployment_id='deployment-alpha',metadata={'portfolioId':'portfolio-alpha','strategyId':'strategy-alpha','runtimeScopeId':'runtime-alpha'})
        self.payload={'broker_id':'kis','quantity':1,'price':50000,'order_type':'00','symbol':'005930','identifier':'dispatch-1'}
        self.prepared=SimpleNamespace(headers={'authorization':'Bearer fake-token'},body=copy.deepcopy(self.payload))
        self.hash=lambda req:hashlib.sha256(json.dumps(req.body,sort_keys=True).encode()).hexdigest()
        self.wire={'operation':'PLACE_ORDER','accountFingerprint':'d'*64,'credentialConfigurationHash':'e'*64,'endpoint':'/uapi/domestic-stock/v1/trading/order-cash','payloadHash':self.hash(self.prepared)}
        self.preflight=SimpleNamespace(snapshot_hash='1'*64,expires_at=(datetime.now(timezone.utc)+timedelta(seconds=100)).isoformat())
        self.namespace={'__package__':'live_trader','Any':Any,'OrderIntent':OrderIntent,'datetime':datetime,'STATE':{'mode':'SMALL_LIVE','kill_switch':False,'new_entries_blocked':False},
            'live_broker_payload':lambda intent,**kw:{**self.payload,'quantity':intent.quantity,'price':intent.reference_price},
            'live_broker_dispatch_allowed':lambda *a,**kw:(True,''),'operational_runtime_dispatch_allowed':lambda *a:(True,'',{}),
            '_active_session_for_intent':lambda intent:('runtime-alpha',self.session),'_kis_scoped_start_material':lambda *a,**kw:(copy.deepcopy(self.context),self.preflight),
            'runtime_settings':lambda:{},'LIVE_CONTINUOUS_CONTROLLER':SimpleNamespace(_writer_leases={'runtime-alpha':SimpleNamespace(held=True)}),
            'functional_test_account_fingerprint':lambda v:'0'*64,'kis_functional_test_account_id':lambda:'fixture-account',
            '_kis_scoped_store':lambda:self.store,'governance_sha256':lambda v:hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()}
        self.namespace['_require_kis_scoped_runtime_clock_integrity']=lambda runtime_id:None
        self.namespace['sqlite3']=sqlite3
        self.namespace['OPERATIONAL_GOVERNANCE']=Mock()
        exec(compile(selected_functions(['_kis_scoped_operational_dispatch_assessment','_kis_scoped_operational_dispatch_authority']),'<isolated final state resolver>','exec'),self.namespace)
        self.patches=[patch('live_trader.kis_equity_execution.validate_ticket',return_value={'currency':'KRW'}),
            patch('live_trader.process_safety.held_process_lease_status',return_value={'acquired':True}),
            patch('live_trader.live_adapters.build_kis_live_order_request',side_effect=lambda payload,**kw:SimpleNamespace(body=payload)),
            patch('live_trader.live_adapters.kis_prepared_payload_hash',side_effect=self.hash)]
        for item in self.patches:item.start();self.addCleanup(item.stop)

    def read(self,**kwargs):
        return self.namespace['_kis_scoped_operational_dispatch_authority'](self.intent,broker_payload=self.payload,authority_intent=self.wire,prepared_request=self.prepared,**kwargs)

    def test_valid_exact_intent_can_consume_only_once(self):
        self.assertTrue(self.read(consume=True)['allowed'])
        with self.assertRaises(KisScopedAuthorityError):self.read(consume=True)

    def test_prepared_quantity_tr_id_or_endpoint_cannot_borrow_valid_intent(self):
        self.prepared.body['quantity']=2;self.wire['payloadHash']=self.hash(self.prepared)
        with self.assertRaisesRegex(ValueError,'wire-hash-mismatch'):self.read()
        self.prepared.body['quantity']=1;self.wire['payloadHash']=self.hash(self.prepared)
        self.wire['endpoint']='/uapi/overseas-stock/v1/trading/order'
        with self.assertRaisesRegex(ValueError,'currency-wire-endpoint'):self.read()

    def test_payload_cannot_borrow_state_intent_or_serialized_permit(self):
        self.payload['quantity']=2
        with self.assertRaisesRegex(ValueError,'payload-differs'):self.read()

    def test_kill_monitor_block_or_lease_loss_revokes_final_edge(self):
        for key,value in [('mode','MONITOR'),('kill_switch',True),('new_entries_blocked',True)]:
            previous=self.namespace['STATE'][key];self.namespace['STATE'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.read()
            self.namespace['STATE'][key]=previous
        self.namespace['LIVE_CONTINUOUS_CONTROLLER']._writer_leases['runtime-alpha'].held=False
        with self.assertRaisesRegex(ValueError,'lease-lost'):self.read()

    def test_valid_queued_intent_is_blocked_if_sticky_kis_clock_failure_arrives_later(self):
        from trading_runtime.market_clock_integrity import StickyMarketClockIntegrity
        integrity=StickyMarketClockIntegrity()
        controller=SimpleNamespace(supervisor=SimpleNamespace(market_clock_integrity_snapshot=integrity.snapshot))
        self.namespace['LIVE_CONTINUOUS_CONTROLLER'].runtimes={'runtime-alpha':controller}
        exec(compile(selected_functions(['_require_kis_scoped_runtime_clock_integrity']),'<clock final gate>','exec'),self.namespace)
        self.assertTrue(self.read()['allowed'])
        integrity.observe('KIS_BOUNDARY_CLOCK_SKEW_RECEIVED_BEFORE_BUCKET',provider='kis',generation=1,
            instrumentId='kis:005930',symbol='005930',timeframe='5m',providerEventUtc='2026-10-16T00:05:00+00:00',
            providerTimeMeaning='KIS_NATIVE_TRADE_DATE_AND_TIME',socketRecvReturnedUtc='2026-10-16T00:04:59+00:00',
            socketRecvReturnedMonotonic=10.0,bucketStartUtc='2026-10-16T00:05:00+00:00',rawHash='a'*64,sourceSequence='fixture')
        with self.assertRaisesRegex(ValueError,'CLOCK_SKEW_RECEIVED_BEFORE_BUCKET'):self.read()
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)


class ScopedStatePreflightRefreshTests(unittest.TestCase):
    read=ScopedStateWireTests.read
    def setUp(self):
        ScopedStateWireTests.setUp(self)
        from live_trader.kis_scoped_operational_authority import canonical_hash
        self.session.preflight_snapshot_id='preflight-old';self.session.deployment_manifest_hash='a'*64
        self.session.config_revision=1;self.session.config_hash='b'*64
        self.namespace['STATE']['active_runtime_session_ids']={'runtime-alpha':'session-a'}
        self.valid=False;self.collected=0
        gov=self.namespace['OPERATIONAL_GOVERNANCE']
        gov.preflight_validity.side_effect=lambda pf:{'valid':self.valid and pf=='preflight-new'}
        gov.get_runtime_session.return_value=self.session
        manifest=SimpleNamespace(deployment_id='deployment-alpha',manifest_hash='a'*64)
        gov.get_deployment_manifest_by_hash.return_value=manifest
        def create(**kwargs):
            self.assertTrue(all(check['status']=='PASS' for check in kwargs['checks']))
            self.preflight.snapshot_hash='c'*64
            return SimpleNamespace(status='PASS',snapshot_id='preflight-new',snapshot_hash='c'*64,expires_at=self.preflight.expires_at)
        gov.create_preflight_snapshot.side_effect=create
        def rebind(session_id,preflight_id,**kwargs):
            self.session.preflight_snapshot_id=preflight_id;self.valid=True;return self.session
        gov.rebind_runtime_preflight.side_effect=rebind
        def collect(strategy):
            self.collected+=1
            return {'summary':{'status':'pass','fresh':True,'three_way_verified':True,
                'evidence_ages_seconds':{'broker_snapshot':1,'execution_event':1,'local_ledger':1}},'positions':[],'accounts':[],'errors':[]}
        strategy={'strategy_id':'strategy-alpha','deployment_id':'deployment-alpha','broker_id':'kis'}
        controller=SimpleNamespace(_lock=threading.RLock())
        manager=self.namespace['LIVE_CONTINUOUS_CONTROLLER'];manager.runtimes={'runtime-alpha':controller}
        manager._lock=threading.RLock();manager._control_lock=threading.RLock()
        self.namespace.update({'timezone':timezone,'SAFETY_CONFIRMATION_MUTATION_LOCK':threading.RLock(),
            'RUNTIME_MODE_LOCK':threading.RLock(),'RUNTIME_CONTROL_LOCK':threading.RLock(),
            'kis_route_authority_serialization':lambda:nullcontext(),'strategy_rows':lambda portfolios:[strategy],
            'portfolio_rows':lambda:[],'_operational_strategy':lambda *a,**kw:strategy,'strategy_broker_id':lambda s:'kis',
            'refresh_preflight_reconciliation':collect,'broker_readiness':lambda:[SimpleNamespace(to_dict=lambda:{'broker_id':'kis'})],
            '_operational_manifest_matches_inputs':lambda *a:True,'_operational_manifest_inputs':lambda s:{'current':True},
            'final_preflight_checks':lambda *a,**kw:[{'label':'fresh account/market/source','status':'pass','detail':'fixture'}],
            'PREFLIGHT_RECONCILIATION_MAX_AGE_SECONDS':60,'governance_sha256':canonical_hash,
            'functional_test_execution_requested':lambda i:False})
        names=['_capture_kis_scoped_preflight_assessment','capture_kis_scoped_preflight_expected','collect_kis_scoped_preflight_truth','commit_kis_scoped_preflight_rebind',
            'revoke_kis_scoped_preflight_failure','_assert_kis_scoped_preflight_outside_locks','_revoke_kis_scoped_unacknowledged_intent_failure','refresh_kis_scoped_runtime_preflight']
        exec(compile(selected_functions(names),'<state preflight callbacks>','exec'),self.namespace)

    def test_expired_launch_preflight_produces_new_truth_and_rebinds_same_consent(self):
        result=self.namespace['refresh_kis_scoped_runtime_preflight'](self.intent)
        self.assertTrue(result['ok'],result);self.assertEqual(result['action'],'REFRESHED')
        self.assertEqual(self.session.preflight_snapshot_id,'preflight-new');self.assertEqual(self.collected,1)
        self.assertTrue(self.read()['allowed'])
        current=self.namespace['refresh_kis_scoped_runtime_preflight'](self.intent)
        self.assertEqual(current['action'],'CURRENT');self.assertEqual(self.collected,1)

    def test_scope_change_during_network_read_revokes_without_rebinding(self):
        collect=self.namespace['refresh_preflight_reconciliation']
        def changed(strategy):
            result=collect(strategy);self.context['scopeHash']='0'*64;return result
        self.namespace['refresh_preflight_reconciliation']=changed
        result=self.namespace['refresh_kis_scoped_runtime_preflight'](self.intent)
        self.assertFalse(result['ok'])
        self.namespace['OPERATIONAL_GOVERNANCE'].create_preflight_snapshot.assert_not_called()
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=context())

    def test_network_under_state_or_controller_lock_never_collects(self):
        for lock in [self.namespace['SAFETY_CONFIRMATION_MUTATION_LOCK'],self.namespace['LIVE_CONTINUOUS_CONTROLLER'].runtimes['runtime-alpha']._lock]:
            with self.subTest(lock=lock),lock:
                result=self.namespace['refresh_kis_scoped_runtime_preflight'](self.intent)
                self.assertFalse(result['ok']);self.assertEqual(self.collected,0)
                self.assertTrue(result['revocationAcknowledged'])
                with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)

    def test_initial_capture_control_or_lease_failure_really_revokes_exact_consent(self):
        self.namespace['STATE']['kill_switch']=True
        result=self.namespace['refresh_kis_scoped_runtime_preflight'](self.intent)
        self.assertFalse(result['ok']);self.assertEqual(self.collected,0)
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)


class ScopedStateMaterialTests(unittest.TestCase):
    def setUp(self):
        from live_trader.kis_scoped_operational_authority import canonical_hash
        fixed=context()
        self.scope={key:fixed[key] for key in ('accountFingerprint','credentialConfigurationHash','capitalCurrency','runtimeScopeId')}
        self.scope['brokerId']='kis'
        self.wire_profiles=[{'brokerId':'kis','accountFingerprint':fixed['accountFingerprint'],'credentialConfigurationHash':fixed['credentialConfigurationHash']}]
        self.strategy={'strategy_id':'strategy-alpha','deployment_id':'deployment-alpha','broker_id':'kis'}
        self.artifacts=[{'symbol':'005930','hash':'1'*64,'contract':{'schemaVersion':'native-equity-quantity-v1','quoteCurrency':'KRW','exchange':'KRX'}},
            {'symbol':'069500','hash':'2'*64,'contract':{'schemaVersion':'native-equity-quantity-v1','quoteCurrency':'KRW','exchange':'KRX'}}]
        self.inputs={'riskPolicyHash':'3'*64,'metadata':{'portfolioId':'portfolio-alpha','deploymentBinding':{'sealed':True},
            'paperFinalBindings':[{'ready':True}],
            'strategyMembers':[{'artifactHash':artifact['hash'],'symbol':artifact['symbol']} for artifact in self.artifacts]}}
        self.preflight=SimpleNamespace(snapshot_id='preflight-a',snapshot_hash='4'*64,reconciliation_hash='5'*64)
        gov=Mock();gov.get_deployment_manifest.return_value=object();gov.latest_preflight_for_deployment.side_effect=lambda dep:self.preflight
        gov.preflight_validity.return_value={'valid':True}
        self.namespace={'__package__':'live_trader','Any':Any,'Path':Path,'OPERATIONAL_GOVERNANCE':gov,
            '_operational_strategy':lambda *a,**kw:self.strategy,'strategy_rows':lambda portfolios:[], 'portfolio_rows':lambda:[],
            'strategy_broker_id':lambda row:row['broker_id'],'resolve_continuous_runtime_scope':lambda *a:self.scope,
            '_operational_manifest_inputs':lambda row:self.inputs,'_live_deployment_binding_is_executable':lambda *a,**kw:True,
            '_operational_manifest_matches_inputs':lambda *a:True,'_reconciliation_summary_for_broker_snapshot':lambda *a:{'status':'pass'},
            'reconciliation_snapshot':lambda:{},'_kis_environment_order_authority_binding':lambda:('d'*64,'e'*64),
            '_kis_application_owner_epoch':lambda:('epoch','f'*64,True),'load_strategy_artifacts':lambda:self.artifacts,
            '_operational_artifact_hash':lambda row:row['hash'],'governance_sha256':canonical_hash,'env_value':lambda key:'SYNTHETIC_FIXTURE'}
        self.namespace['_kis_cleanup_store'] = lambda: SimpleNamespace(pending_for_account=lambda account: [])
        exec(compile(selected_functions(['_kis_scoped_start_material']),'<current exact consent material>','exec'),self.namespace)
        patcher=patch('live_trader.native_spot_quantity.sizing_from_artifact',side_effect=lambda artifact:{'quantityContract':artifact['contract']})
        patcher.start();self.addCleanup(patcher.stop)
        patcher=patch('live_trader.account_profiles.saved_account_profiles',side_effect=lambda settings:self.wire_profiles)
        patcher.start();self.addCleanup(patcher.stop)

    def material(self,**kwargs):return self.namespace['_kis_scoped_start_material']('stock','SMALL_LIVE','portfolio-alpha','deployment-alpha','strategy-alpha',**kwargs)[0]

    def test_usd_native_start_and_renewal_cannot_borrow_generic_krw_reconciliation(self):
        self.scope['capitalCurrency']='USD'
        for operation in ('START','RENEWAL'):
            with self.subTest(operation=operation),self.assertRaisesRegex(ValueError,'KIS_USD_NATIVE_RECONCILIATION_REQUIRED'):
                self.material(usd_operation=operation)

    def test_all_members_venues_bound_and_new_preflight_does_not_change_human_consent(self):
        original=self.material()
        self.preflight.snapshot_hash='6'*64;self.preflight.snapshot_id='preflight-new'
        self.assertEqual(self.material(),original)
        self.inputs['riskPolicyHash']='7'*64
        self.assertNotEqual(self.material()['scopeHash'],original['scopeHash'])

    def test_missing_member_wrong_currency_or_foreign_venue_blocks(self):
        original=copy.deepcopy(self.artifacts)
        self.artifacts.pop()
        with self.assertRaisesRegex(ValueError,'source-route-ambiguous'):self.material()
        self.artifacts[:]=copy.deepcopy(original);self.artifacts[-1]['contract']['quoteCurrency']='USD'
        with self.assertRaisesRegex(ValueError,'currency-contract'):self.material()
        self.artifacts[:]=copy.deepcopy(original);self.artifacts[-1]['contract']['exchange']='NASD'
        with self.assertRaisesRegex(ValueError,'exchange-mismatch'):self.material()

    def test_expired_preflight_is_not_initial_start_but_can_capture_plan_for_fresh_production(self):
        self.namespace['OPERATIONAL_GOVERNANCE'].preflight_validity.return_value={'valid':False}
        with self.assertRaisesRegex(ValueError,'fresh-preflight-required'):self.material()
        self.assertEqual(self.material(require_fresh_preflight=False)['capitalCurrency'],'KRW')

    def test_separately_valid_saved_and_wire_accounts_or_credentials_cannot_be_mixed(self):
        for key in ('accountFingerprint','credentialConfigurationHash'):
            original=self.wire_profiles[0][key];self.wire_profiles[0][key]='0'*64
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,'actual-wire-account'):self.material()
            self.wire_profiles[0][key]=original


if __name__=='__main__':unittest.main()
