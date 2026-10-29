"""Exact STOP and deferred cleanup use synthetic callbacks, no state import."""
import ast
from contextlib import nullcontext, closing
from copy import deepcopy
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json
import secrets
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from typing import Any, Mapping

from live_trader.kis_scoped_cleanup_authority import KisScopedCleanupStore, CLEANUP_POLICY, dispatch_ownership_hash
from live_trader.kis_scoped_operational_authority import KisScopedOperationalAuthority, canonical_hash
from test_kis_scoped_operational_authority import context


def functions(names):
    source=(Path(__file__).resolve().parents[1]/'live_trader/state.py').read_text(encoding='utf-8')
    nodes=[node for node in ast.parse(source).body if isinstance(node,ast.FunctionDef) and node.name in names]
    if {node.name for node in nodes} != set(names):raise AssertionError('missing cleanup state functions')
    return ast.Module(body=nodes,type_ignores=[])


class Session(SimpleNamespace):
    def to_dict(self):return {'sessionId':self.session_id,'deploymentId':self.deployment_id,'profile':self.profile,
        'mode':self.mode,'lifecycle':self.lifecycle,'metadata':dict(self.metadata)}


class ScopedStopSelectionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.ops=KisScopedOperationalAuthority(Path(self.temp.name)/'authority.sqlite3')
        self.context=context();self.context['runtimeScopeId']='runtime-kis';self.context['deploymentId']='deployment-kis'
        self.ops.establish_exclusion(self.context,proof={key:True for key in ('routeQuiescent','specializedClosureInstalled','sameAccountLeaseHeld','reconciliationClear')})
        import time
        self.ops.reserve(self.context,session_id='session-kis',reservation_id='native-start',expires_at=time.time()+200)
        self.ops.record_native_cleanup_policy('session-kis',context=self.context,policy=CLEANUP_POLICY)
        self.ops.activate('session-kis',context=self.context,reservation_id='native-start',reservation_finalized=True,session_running=True)
        self.kis=Session(session_id='session-kis',deployment_id='deployment-kis',profile='stock',mode='SMALL_LIVE',lifecycle='RUNNING',metadata={
            'runtimeScopeId':'runtime-kis','brokerId':'kis'})
        self.crypto=Session(session_id='session-crypto',deployment_id='deployment-crypto',profile='crypto',mode='FULL_LIVE',lifecycle='RUNNING',metadata={
            'runtimeScopeId':'runtime-crypto','brokerId':'upbit'})
        self.sessions={s.session_id:s for s in (self.kis,self.crypto)}
        self.rows={'runtime-kis':{'profileId':'stock','brokerId':'kis','mode':'SMALL_LIVE','deploymentId':'deployment-kis','running':True},
            'runtime-crypto':{'profileId':'crypto','brokerId':'upbit','mode':'FULL_LIVE','deploymentId':'deployment-crypto','running':True}}
        self.manager=Mock();self.manager.runtimes={'runtime-kis':SimpleNamespace(_native_runtime=object()),'runtime-crypto':SimpleNamespace(_native_runtime=None)}
        self.manager.snapshot.side_effect=lambda:{'runtimes':deepcopy(self.rows)}
        self.manager.stop.side_effect=self.stop_manager
        self.gov=Mock();self.gov.get_runtime_session.side_effect=lambda sid:self.sessions.get(sid)
        self.gov.transition_runtime_session.side_effect=self.transition
        self.cleanup_store=KisScopedCleanupStore(self.ops)
        self.events=[]
        self.native=Mock();self.native_patch=patch('live_trader.kis_operational_native_confirmation.KIS_NATIVE_OPERATIONAL_START_CONFIRMATIONS',self.native)
        self.native_patch.start();self.addCleanup(self.native_patch.stop)
        self.state={'active_runtime_session_ids':{'runtime-kis':'session-kis','runtime-crypto':'session-crypto'},'mode':'FULL_LIVE'}
        self.ns={'__package__':'live_trader','Any':Any,'wraps':wraps,'threading':threading,'sqlite3':sqlite3,
            'hashlib':hashlib,'json':json,'secrets':secrets,'STATE':self.state,'RUNTIME_CONTROL_LOCK':threading.RLock(),
            'RUNTIME_MODE_LOCK':threading.RLock(),'SAFETY_CONFIRMATION_MUTATION_LOCK':threading.RLock(),
            'LIVE_CONTINUOUS_CONTROLLER':self.manager,'OPERATIONAL_GOVERNANCE':self.gov,
            '_CONTINUOUS_STOP_SCOPE':threading.local(),'_KIS_CONTROL_RESERVATION':{},'_KIS_ROUTE_STATE_REVISION':1,
            '_KIS_CONTROL_RESERVATION_SCOPE':threading.local(),'_KIS_CONTROL_SUPERSEDED_IDS':set(),
            '_KIS_SCOPED_PENDING_STARTS':{},'kis_route_authority_serialization':lambda:nullcontext(),
            '_kis_scoped_store':lambda:self.ops,'_kis_cleanup_store':lambda:self.cleanup_store,
            '_prepare_kis_owned_stop_cleanup':self.prepare_cleanup,'_finish_kis_owned_stop_cleanup':self.finish_cleanup,
            '_kis_cleanup_status':self.cleanup_store.public_status,'snapshot':lambda:{'runtimes':deepcopy(self.rows)},
            'begin_continuous_control':lambda:self.events.append('control-begin') or 'token',
            'end_continuous_control':lambda token:self.events.append('control-end'),
            'sync_runtime_profile_mode':Mock(),'normalize_runtime_mode':lambda mode:mode,
            'RUNTIME_MODE_RANK':{'MONITOR':0,'SMALL_LIVE':1,'FULL_LIVE':2},'stop_functional_test_runtime_guard':Mock(return_value={}),
            '_revoke_runtime_order_authority':Mock()}
        names=['_advance_kis_route_state_revision','_kis_control_reservation_hash','_two_phase_kis_route_control',
            '_continuous_exact_stop_plan','_continuous_stop_requires_kis_route','_continuous_stop_control','stop_continuous_runtime']
        exec(compile(functions(names),'<exact STOP source>','exec'),self.ns)
        self.cleanup_result={'ok':True,'status':'CLEAN'}

    def transition(self,sid,lifecycle,**kwargs):self.sessions[sid].lifecycle=lifecycle;return self.sessions[sid]
    def prepare_cleanup(self,key,session):
        self.events.append('cleanup-reserve')
        self.assertEqual(key,'runtime-kis')
        row=self.cleanup_store.reserve(session.session_id,self.context,[])
        return {'cleanupId':row['cleanup_id'],'sessionId':session.session_id,'runtimeScopeId':key}
    def finish_cleanup(self,plan):
        self.events.append('cleanup-collect')
        self.assertFalse(self.ns['RUNTIME_CONTROL_LOCK']._is_owned())
        self.assertFalse(self.ns['SAFETY_CONFIRMATION_MUTATION_LOCK']._is_owned())
        self.assertFalse(self.ns['_KIS_CONTROL_RESERVATION'])
        self.assertFalse(self.rows['runtime-kis']['running'])
        if self.cleanup_result['ok']:
            self.cleanup_store.finish(plan['cleanupId'],terminal_truth={'orders':[]},owned_capital_settled=True,reconciliation_passed=True)
            self.kis.lifecycle='STOPPED';self.state['active_runtime_session_ids'].pop('runtime-kis',None)
        return {**self.cleanup_result,'runtimeScopeId':plan['runtimeScopeId']}
    def stop_manager(self,profile='',*,runtime_id='',deployment_id=''):
        self.events.append('worker-drain')
        for key,row in self.rows.items():
            if (not runtime_id or key==runtime_id) and (not deployment_id or row['deploymentId']==deployment_id) and (not profile or row['profileId']==profile):
                row['running']=False;row['mode']='MONITOR'
        return {'ok':True,'runtime':{'phase':'STOPPED','running':False}}

    def test_unknown_exact_id_never_revokes_or_supersedes_other_start(self):
        self.ns['_KIS_CONTROL_RESERVATION']={'reservationId':'someone-start','reservationKind':'START'}
        before=deepcopy(self.ns['_KIS_CONTROL_RESERVATION'])
        result=self.ns['stop_continuous_runtime']('crypto',runtime_id='no-such-runtime',deployment_id='deployment-crypto')
        self.assertFalse(result['ok']);self.assertFalse(result['runtimeStopped'])
        self.assertEqual(before,self.ns['_KIS_CONTROL_RESERVATION']);self.manager.stop.assert_not_called()
        self.ops.authorize_consent('session-kis',context=self.context);self.native.revoke_scope.assert_not_called()

    def test_exact_crypto_stop_preserves_kis_consent_and_start_reservation(self):
        self.ns['_KIS_CONTROL_RESERVATION']={'reservationId':'kis-start-in-progress','reservationKind':'START'}
        before=deepcopy(self.ns['_KIS_CONTROL_RESERVATION'])
        result=self.ns['stop_continuous_runtime']('crypto',runtime_id='runtime-crypto',deployment_id='deployment-crypto')
        self.assertTrue(result['ok']);self.assertTrue(result['runtimeStopped']);self.assertTrue(self.rows['runtime-kis']['running'])
        self.assertEqual(before,self.ns['_KIS_CONTROL_RESERVATION'])
        self.ops.authorize_consent('session-kis',context=self.context)
        self.native.revoke_scope.assert_not_called();self.manager.retain_cleanup_writer.assert_not_called()

    def test_exact_kis_stop_revokes_first_drains_then_collects_outside_all_locks(self):
        result=self.ns['stop_continuous_runtime']('stock',runtime_id='runtime-kis',deployment_id='deployment-kis',cleanup_requested=True)
        self.assertTrue(result['ok']);self.assertTrue(result['runtimeStopped']);self.assertEqual(result['cleanupStatus'],'CLEAN')
        self.assertEqual(self.events,['cleanup-reserve','control-begin','worker-drain','control-end','cleanup-collect'])
        self.assertTrue(self.rows['runtime-crypto']['running']);self.assertEqual(self.kis.lifecycle,'STOPPED')
        self.native.revoke_scope.assert_called_with('runtime-kis')

    def test_cancel_ack_or_unknown_terminal_never_returns_clean_or_removes_session(self):
        self.cleanup_result={'ok':False,'status':'STOPPED_PENDING_RECONCILIATION','reason':'terminal-not-confirmed'}
        result=self.ns['stop_continuous_runtime']('stock',runtime_id='runtime-kis',deployment_id='deployment-kis',cleanup_requested=True)
        self.assertFalse(result['ok']);self.assertTrue(result['runtimeStopped'])
        self.assertEqual(result['cleanupStatus'],'STOPPED_PENDING_RECONCILIATION');self.assertEqual(self.kis.lifecycle,'STOPPING')
        self.assertEqual(self.state['active_runtime_session_ids']['runtime-kis'],'session-kis')
        self.assertEqual(len(result['cleanup']['scopes']),1)

    def test_ambiguous_crypto_profile_is_observational_and_never_stops_all(self):
        self.rows['runtime-binance']={**self.rows['runtime-crypto'],'deploymentId':'deployment-binance'}
        result=self.ns['stop_continuous_runtime']('crypto')
        self.assertFalse(result['ok']);self.manager.stop.assert_not_called()
        self.ops.authorize_consent('session-kis',context=self.context)

    def test_default_engine_only_stop_never_collects_or_cancels_and_retains_recovery(self):
        result=self.ns['stop_continuous_runtime']('stock',runtime_id='runtime-kis',deployment_id='deployment-kis')
        self.assertFalse(result['ok']);self.assertTrue(result['runtimeStopped'])
        self.assertEqual(result['cleanupStatus'],'STOPPED_PENDING_RECONCILIATION')
        self.assertNotIn('cleanup-collect',self.events);self.assertEqual(self.kis.lifecycle,'STOPPING')
        self.assertEqual(self.state['active_runtime_session_ids']['runtime-kis'],'session-kis')

    def test_unreadable_owned_receipt_still_drains_and_durably_exposes_recovery(self):
        self.ns['_prepare_kis_owned_stop_cleanup']=Mock(side_effect=ValueError('owned-ledger-unavailable'))
        result=self.ns['stop_continuous_runtime']('stock',runtime_id='runtime-kis',deployment_id='deployment-kis',cleanup_requested=True)
        self.assertFalse(result['ok']);self.assertTrue(result['runtimeStopped'])
        self.assertFalse(self.rows['runtime-kis']['running'])
        self.assertEqual(result['cleanup']['scopes'][0]['status'],'RECONCILIATION_REQUIRED')
        self.assertTrue(self.cleanup_store.pending_for_account(self.context['accountFingerprint']))


class CleanupManagerLeaseTests(unittest.TestCase):
    def test_owned_cleanup_pin_keeps_account_writer_and_blocks_restart_until_positive_completion(self):
        from live_trader.continuous_live import LiveContinuousRuntimeManager
        from test_runtime_scope import FakeController, scope
        with tempfile.TemporaryDirectory() as folder:
            selected=scope('kis','a','kis-deployment')
            manager=LiveContinuousRuntimeManager(Path(folder),scope_resolver=lambda **kw:selected,
                controller_factory=FakeController,account_writer_lease_root=Path(folder)/'leases')
            try:
                result=manager.start('stock','SMALL_LIVE',deployment_id='kis-deployment')
                self.assertTrue(result['ok']);key=result['runtimeId']
                lease=manager._writer_leases[key]
                manager.retain_cleanup_writer(key)
                result=manager.stop(runtime_id=key,deployment_id='kis-deployment')
                self.assertTrue(result['writerLeaseRetained']);self.assertTrue(lease.held)
                self.assertFalse(manager.start('stock','SMALL_LIVE',deployment_id='kis-deployment')['ok'])
                self.assertFalse(manager.start('stock','MONITOR',deployment_id='kis-deployment')['ok'])
                manager.complete_cleanup_writer(key);self.assertFalse(lease.held)
                self.assertTrue(manager.start('stock','SMALL_LIVE',deployment_id='kis-deployment')['ok'])
            finally:
                for lease in manager._writer_leases.values():lease.release()


class CleanupActualWireStateTests(unittest.TestCase):
    def setUp(self):
        from test_kis_scoped_cleanup_truth import fixture, Observer, NOW, SETTINGS
        from live_trader.kis_scoped_cleanup_truth import collect_cleanup_truth, validate_cleanup_truth
        self.now=NOW;self.scope,self.owned,self.working,self.history=fixture()
        self.observer=Observer(self.working,self.history)
        self.native=SimpleNamespace(scope=lambda:deepcopy(self.scope),observer=self.observer)
        self.truth=collect_cleanup_truth(self.native,[self.owned],now=NOW,monotonic_clock=lambda:1000.)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.ops=KisScopedOperationalAuthority(Path(self.temp.name)/'authority.sqlite3',clock=lambda:NOW.timestamp(),monotonic=lambda:1000.)
        self.ctx=context();self.ctx.update(runtimeScopeId='runtime-1',deploymentId='deployment',capitalCurrency='USD',
            accountFingerprint=self.owned['accountFingerprint'],credentialConfigurationHash=self.owned['credentialConfigurationHash'])
        self.ops.establish_exclusion(self.ctx,proof={key:True for key in ('routeQuiescent','specializedClosureInstalled','sameAccountLeaseHeld','reconciliationClear')})
        self.ops.reserve(self.ctx,session_id='session-1',reservation_id='start-1',expires_at=NOW.timestamp()+200)
        self.ops.record_native_cleanup_policy('session-1',context=self.ctx,policy=CLEANUP_POLICY)
        self.ops.activate('session-1',context=self.ctx,reservation_id='start-1',reservation_finalized=True,session_running=True)
        self.store=KisScopedCleanupStore(self.ops)
        row=self.store.reserve('session-1',self.ctx,[{'dispatchKey':self.owned['dispatchKey'],'ownershipHash':dispatch_ownership_hash(self.owned['dispatch'])}])
        self.key=row['cleanup_id'];self.store.bind_truth(self.key,self.truth,expires_at=NOW.timestamp()+15,ownership=[{'dispatchKey':self.owned['dispatchKey'],'ownershipHash':dispatch_ownership_hash(self.owned['dispatch'])}])
        self.current=deepcopy(self.owned['dispatch'])
        self.owner_allowed=True
        def owner(*args,**kwargs):
            if not self.owner_allowed:raise ValueError('current owner/credentials/lease revoked')
            return object(),object()
        self.ns={'__package__':'live_trader','json':json,'_kis_cleanup_store':lambda:self.store,
            '_kis_cleanup_current_owner':owner,'_kis_cleanup_owned_orders':lambda *a:[deepcopy(self.owned)],
            'PROGRAM_LEDGER':SimpleNamespace(order_dispatch_for_idempotency_key=lambda key:deepcopy(self.current)),
            'governance_sha256':canonical_hash}
        exec(compile(functions(['_kis_scoped_cleanup_dispatch_authority']),'<actual cleanup wire state>','exec'),self.ns)
        self.validation_patch=patch('live_trader.kis_scoped_cleanup_truth.validate_cleanup_truth',side_effect=lambda truth,owned,**kw:validate_cleanup_truth(truth,owned,now=NOW))
        self.validation_patch.start();self.addCleanup(self.validation_patch.stop)
        import os
        self.env=patch.dict(os.environ,{**SETTINGS,'KIS_ENV':'real','KIS_BASE_URL':'https://openapi.koreainvestment.com:9443'})
        self.env.start();self.addCleanup(self.env.stop)
        self.selector={'cleanupId':self.key,'dispatchKey':self.owned['dispatchKey']}

    def wire(self,proof=None):
        from live_trader.kis_scoped_cleanup_adapter import build_kis_scoped_cleanup_request
        from live_trader.live_adapters import kis_prepared_payload_hash
        row=proof or self.truth['orders'][0]
        prepared=build_kis_scoped_cleanup_request(row,access_token='synthetic-token')
        intent={'operation':'SCOPED_CANCEL_US','claimId':'wire-1','ownedOrderKey':{'orderDate':row['orderDate'],'organizationNo':row['organizationNo'],'orderNo':row['orderNo']},
            'accountFingerprint':self.ctx['wireAccountFingerprint'],'credentialConfigurationHash':self.ctx['wireCredentialConfigurationHash'],
            'endpoint':prepared.endpoint,'payloadHash':kis_prepared_payload_hash(prepared)}
        return prepared,intent

    def test_exact_actual_two_share_remainder_not_original_three_is_consumed_once(self):
        prepared,intent=self.wire()
        self.assertEqual(prepared.body['ORD_QTY'],'2')
        result=self.ns['_kis_scoped_cleanup_dispatch_authority'](self.selector,authority_intent=intent,prepared_request=prepared,consume=True)
        self.assertTrue(result['allowed']);self.assertTrue(result['cleanupOnly'])
        with self.assertRaises(RuntimeError):self.ns['_kis_scoped_cleanup_dispatch_authority'](self.selector,authority_intent={**intent,'claimId':'new-random-claim'},prepared_request=prepared,consume=True)

    def test_changed_actual_quantity_venue_symbol_or_original_order_is_rejected_before_claim(self):
        for field,value in [('remainingQuantity','3'),('venue','NYSE'),('symbol','IBM'),('orderNo','9999')]:
            with self.subTest(field=field):
                changed={**self.truth['orders'][0],field:value}
                if field=='remainingQuantity':changed['filledQuantity']='0'
                prepared,intent=self.wire(changed)
                with self.assertRaisesRegex(ValueError,'actual-wire-differs'):
                    self.ns['_kis_scoped_cleanup_dispatch_authority'](self.selector,authority_intent=intent,prepared_request=prepared,consume=True)
        self.store.authorize(self.key,context=self.ctx,dispatch_key=self.owned['dispatchKey'])

    def test_ledger_current_owner_or_unknown_selector_changes_cannot_borrow_proof(self):
        prepared,intent=self.wire()
        self.current['response']['changed']=True
        with self.assertRaisesRegex(ValueError,'ledger-receipt-changed'):
            self.ns['_kis_scoped_cleanup_dispatch_authority'](self.selector,authority_intent=intent,prepared_request=prepared,consume=True)
        self.current=deepcopy(self.owned['dispatch']);self.owner_allowed=False
        with self.assertRaisesRegex(ValueError,'owner'):
            self.ns['_kis_scoped_cleanup_dispatch_authority'](self.selector,authority_intent=intent,prepared_request=prepared,consume=True)
        self.owner_allowed=True
        with self.assertRaises(ValueError):self.ns['_kis_scoped_cleanup_dispatch_authority']({**self.selector,'quantity':'3'})


class CleanupTerminalOrchestrationTests(unittest.TestCase):
    def setUp(self):
        from test_kis_scoped_cleanup_truth import fixture, Observer, NOW
        from live_trader.kis_scoped_cleanup_truth import collect_cleanup_truth, validate_cleanup_truth
        self.now=NOW;self.scope,self.owned,working,history=fixture('KRX')
        self.observer=Observer(working,history)
        self.native=SimpleNamespace(scope=lambda:deepcopy(self.scope),observer=self.observer,sync_owned_fills=Mock(return_value=1))
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        # Real read-only original-principal/source adapter is exercised by the
        # actual AST cleanup function; this is a synthetic C-only store.
        from live_trader.kis_owned_capital import KisOwnedCapitalStore
        from test_kis_equity_operational import fixture as source_fixture
        source_spec, _contract, _source_context = source_fixture('KRX')
        original_store = KisOwnedCapitalStore(Path(self.temp.name) / 'original-principal.sqlite3')
        original_store.initialize(self.scope, {'currency':'KRW','profileId':self.scope['accountProfileId'],
            'accountFingerprint':self.scope['accountFingerprint'],'credentialConfigurationHash':self.scope['credentialConfigurationHash'],
            'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE','asOf':NOW.isoformat(),'availableCash':'100000'}, now=NOW)
        self.native.store = original_store
        self.native.specs = [source_spec]
        ops=KisScopedOperationalAuthority(Path(self.temp.name)/'authority.sqlite3',clock=lambda:NOW.timestamp(),monotonic=lambda:1000.)
        ctx=context();ctx.update(runtimeScopeId='runtime-1',deploymentId='deployment',capitalCurrency='KRW',
            accountFingerprint=self.owned['accountFingerprint'],credentialConfigurationHash=self.owned['credentialConfigurationHash'])
        ops.establish_exclusion(ctx,proof={key:True for key in ('routeQuiescent','specializedClosureInstalled','sameAccountLeaseHeld','reconciliationClear')})
        ops.reserve(ctx,session_id='session-1',reservation_id='start-1',expires_at=NOW.timestamp()+200)
        ops.record_native_cleanup_policy('session-1',context=ctx,policy=CLEANUP_POLICY)
        ops.activate('session-1',context=ctx,reservation_id='start-1',reservation_finalized=True,session_running=True)
        self.store=KisScopedCleanupStore(ops)
        reserved=[{'dispatchKey':self.owned['dispatchKey'],'ownershipHash':dispatch_ownership_hash(self.owned['dispatch'])}]
        row=self.store.reserve('session-1',ctx,reserved)
        self.plan={'cleanupId':row['cleanup_id'],'runtimeScopeId':'runtime-1','sessionId':'session-1','context':ctx,'reserved':reserved}
        self.manager=SimpleNamespace(complete_cleanup_writer=Mock())
        self.controller=SimpleNamespace(_native_runtime=self.native)
        self.result_mode='terminal';self.cancel_calls=[]
        def cancel(selector,proof):
            self.cancel_calls.append((selector,proof))
            self.assertEqual(proof['remainingQuantity'],'2')
            if self.result_mode=='transport-error':raise OSError('ambiguous broker transport')
            if self.result_mode=='terminal':
                self.observer.working=[]
                self.observer.history[0].update(rmn_qty='0',cnc_cfrm_qty='2')
            return {'rt_cd':'0','output':{'ODNO':'cancel-ACK-is-not-terminal'}}
        self.router=SimpleNamespace(cancel_kis_scoped_cleanup=cancel)
        self.ns={'__package__':'live_trader','datetime':datetime,'STATE':{'active_runtime_session_ids':{'runtime-1':'session-1'}},
            'SAFETY_CONFIRMATION_MUTATION_LOCK':threading.RLock(),'kis_route_authority_serialization':lambda:nullcontext(),
            '_assert_kis_preflight_collection_outside_authority_locks_for_runtime':lambda key:True,
            '_kis_cleanup_current_owner':lambda *a,**kw:(object(),self.controller),
            '_recover_kis_cleanup_owned_identities':lambda *a:[deepcopy(self.owned)],'_kis_cleanup_store':lambda:self.store,
            'LiveBrokerRouter':lambda:self.router,'LIVE_CONTINUOUS_CONTROLLER':self.manager,
            'OPERATIONAL_GOVERNANCE':SimpleNamespace(transition_runtime_session=Mock()), '_revoke_runtime_order_authority':Mock(),
            'refresh_preflight_reconciliation':Mock(return_value={'poll':{'ok':True,'coalesced':False,'error_count':0},
                'summary':{'status':'pass','fresh':True,'three_way_verified':True,'error_count':0,'blocking_count':0},'errors':[]})}
        exec(compile(functions(['_finish_kis_owned_stop_cleanup']),'<terminal STOP cleanup source>','exec'),self.ns)
        p=patch('live_trader.kis_scoped_cleanup_truth.collect_cleanup_truth',side_effect=lambda runtime,owned:collect_cleanup_truth(runtime,owned,now=NOW,monotonic_clock=lambda:1000.))
        p.start();self.addCleanup(p.stop)
        p=patch('live_trader.kis_scoped_cleanup_truth.validate_cleanup_truth',side_effect=lambda truth,owned,**kw:validate_cleanup_truth(truth,owned,now=NOW))
        p.start();self.addCleanup(p.stop)
        p=patch('live_trader.kis_scoped_cleanup_scope.cleanup_read_runtime',side_effect=lambda native,ctx:native)
        p.start();self.addCleanup(p.stop)

    def finish(self):return self.ns['_finish_kis_owned_stop_cleanup'](self.plan)

    def test_kr_partial_cancel_requires_new_terminal_get_owned_book_and_three_way_before_release(self):
        result=self.finish();self.assertTrue(result['ok'],result)
        self.assertEqual(len(self.cancel_calls),1);self.assertEqual(len(self.observer.calls),4)
        self.native.sync_owned_fills.assert_called_once();self.manager.complete_cleanup_writer.assert_called_once_with('runtime-1')
        passed_source = self.ns['refresh_preflight_reconciliation'].call_args.args[0]
        self.assertEqual('KRX', passed_source['exchange'])
        self.assertEqual(self.scope['scopeHash'], passed_source['cleanup_source_scope_hash'])
        self.assertEqual(self.store.public_status()['status'],'CLEAN')
        self.assertNotIn('runtime-1',self.ns['STATE']['active_runtime_session_ids'])

    def test_ack_without_terminal_proof_retains_lease_and_never_marks_stopped_session_complete(self):
        self.result_mode='ACK-only';result=self.finish()
        self.assertFalse(result['ok']);self.assertTrue(result['writerLeaseRetained'])
        self.manager.complete_cleanup_writer.assert_not_called();self.native.sync_owned_fills.assert_not_called()
        self.ns['OPERATIONAL_GOVERNANCE'].transition_runtime_session.assert_not_called()
        self.assertEqual(self.store.public_status()['status'],'RECONCILIATION_REQUIRED')

    def test_transport_error_never_clears_or_invents_terminal_receipt(self):
        self.result_mode='transport-error';result=self.finish()
        self.assertFalse(result['ok']);self.assertIn('ambiguous',result['reason'])
        self.manager.complete_cleanup_writer.assert_not_called();self.assertEqual(len(self.cancel_calls),1)

    def test_failed_get_or_three_way_never_clears_writer(self):
        self.observer.history=[]
        self.assertFalse(self.finish()['ok']);self.assertEqual(self.cancel_calls,[])
        self.manager.complete_cleanup_writer.assert_not_called()
        from test_kis_scoped_cleanup_truth import fixture
        _scope,_owned,working,history=fixture('KRX');self.observer.working=[working];self.observer.history=[history]
        self.ns['refresh_preflight_reconciliation'].return_value['summary']['three_way_verified']=False
        self.assertFalse(self.finish()['ok']);self.manager.complete_cleanup_writer.assert_not_called()

    def test_no_owned_orders_still_requires_fresh_reconciliation_and_no_cancel_or_history_get(self):
        # A separate native-approved run with a complete, empty local receipt set.
        self.store.operational.revoke(session_id='session-1')
        row=self.store.read(self.plan['cleanupId'])
        with closing(self.store._connect()) as connection, connection:
            connection.execute("UPDATE kis_operational_cleanups SET orders_json='[]',orders_hash=? WHERE cleanup_id=?",(canonical_hash([]),row['cleanup_id']))
        self.plan['reserved']=[];self.ns['_recover_kis_cleanup_owned_identities']=lambda *a:[]
        result=self.finish();self.assertTrue(result['ok'],result)
        self.assertEqual(self.cancel_calls,[]);self.assertEqual(self.observer.calls,[])
        self.ns['refresh_preflight_reconciliation'].assert_called_once()


class CleanupHistoricalCacheSelectionTests(unittest.TestCase):
    def setUp(self):
        from test_kis_scoped_cleanup_truth import fixture, NOW
        from live_trader.kis_owned_capital import KisOwnedCapitalStore, terminal_receipt
        from live_trader.verified_terminal_cache import VerifiedTerminalCache, terminal_identity, terminal_proof
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        scope,owned,_working,official=fixture('KRX');self.scope=scope
        store=KisOwnedCapitalStore(Path(self.temp.name)/'principal.sqlite3')
        store.initialize(scope,{'currency':'KRW','profileId':scope['accountProfileId'],'accountFingerprint':scope['accountFingerprint'],
            'credentialConfigurationHash':scope['credentialConfigurationHash'],'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE',
            'asOf':NOW.isoformat(),'availableCash':'10000000'},now=NOW)
        member=scope['members'][0]
        self.spec=SimpleNamespace(strategy_instance_id=member['strategyInstanceId'],artifact_hash=member['sourceArtifactHash'],portfolio_hash=member['portfolioHash'],
            symbol=member['symbol'],broker_id='kis',artifact={})
        self.cache=VerifiedTerminalCache(Path(self.temp.name)/'terminal.sqlite3')
        self.sizing=patch('live_trader.native_spot_quantity.sizing_from_artifact',return_value={'quantityContract':{'exchange':'KRX'}})
        self.sizing.start();self.addCleanup(self.sizing.stop)
        self.rows=[]
        for number in range(1,71):
            row=deepcopy(owned['dispatch']);row.update(idempotency_key='dispatch-'+str(number),broker_order_id=str(number),
                broker_order_key=row['order_date']+':001:'+str(number),runtime_authorization={'session':{'sessionId':'session-1'}})
            current={**official,'odno':str(number),'tot_ccld_qty':'3','rmn_qty':'0','cnc_cfrm_qty':'0','tot_ccld_amt':'300'}
            receipt=terminal_receipt(scope,self.spec,row,current);identity=terminal_identity(scope,self.spec,row)
            proof=terminal_proof(identity,[receipt],broker_response=current,executed_quantity='3')
            self.cache.put(identity,[receipt],proof);self.rows.append(row)
        self.ctx={'deploymentId':scope['deploymentId'],'accountFingerprint':scope['accountFingerprint'],
            'credentialConfigurationHash':scope['credentialConfigurationHash'],'capitalCurrency':'KRW'}
        self.reservation=None
        cleanup=SimpleNamespace(for_session=lambda sid:self.reservation,consent_context=lambda sid:self.ctx)
        self.controller=SimpleNamespace(_native_runtime=SimpleNamespace(specs=[self.spec],store=store,terminal_cache=self.cache))
        self.session=SimpleNamespace(session_id='session-1',deployment_id='deployment',metadata={'runtimeScopeId':'runtime-1',
            'accountFingerprint':scope['accountFingerprint'],'credentialConfigurationHash':scope['credentialConfigurationHash']})
        self.ns={'__package__':'live_trader','json':json,'_kis_cleanup_store':lambda:cleanup,
            'PROGRAM_LEDGER':SimpleNamespace(order_dispatch_rows=lambda limit:deepcopy(self.rows)),
            'governance_sha256':canonical_hash}
        exec(compile(functions(['_kis_cleanup_owned_orders']),'<verified terminal selection>','exec'),self.ns)

    def capture(self):return self.ns['_kis_cleanup_owned_orders'](self.session,self.controller)

    def test_more_than_64_proven_terminal_orders_do_not_block_bounded_pending_selection(self):
        self.assertEqual(self.capture(),[])
        pending=deepcopy(self.rows[-1]);pending.update(idempotency_key='pending-71',broker_order_id='71',broker_order_key=pending['order_date']+':001:71')
        self.rows.append(pending)
        selected=self.capture();self.assertEqual([item['dispatchKey'] for item in selected],['pending-71'])

    def test_already_reserved_receipt_stays_in_recheck_after_terminal_cache_is_populated(self):
        self.reservation={'status':'RECONCILIATION_REQUIRED','orders_json':json.dumps([{'dispatchKey':'dispatch-1','ownershipHash':'a'*64}])}
        self.assertEqual([item['dispatchKey'] for item in self.capture()],['dispatch-1'])

    def test_changed_date_source_account_credential_or_venue_cannot_borrow_history(self):
        original=deepcopy(self.rows)
        for mutation in ('date','source','account','credential','venue'):
            self.rows=deepcopy(original)
            row=self.rows[0]
            if mutation=='date':row['order_date']='20261014';row['broker_order_key']='20261014:001:1'
            if mutation=='source':row['metadata']['source_artifact_hash']='0'*64
            if mutation=='account':row['metadata']['native_account_fingerprint']='0'*64
            if mutation=='credential':
                from live_trader.kis_equity_execution import seal
                model=row['metadata']['native_quantity_model'];model['credentialConfigurationHash']='0'*64;model.pop('contentHash',None)
                row['metadata']['native_quantity_model']=seal(model)
            if mutation=='venue':
                from live_trader.kis_equity_execution import seal
                model=row['metadata']['native_quantity_model'];model['exchange']='NASD';model.pop('contentHash',None)
                row['metadata']['native_quantity_model']=seal(model)
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):self.capture()
