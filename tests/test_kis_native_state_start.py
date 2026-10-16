"""Real state orchestration functions, local stores and synthetic transports."""
import ast
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_trader.kis_native_start_registry import KisNativeStartRegistry
from live_trader.kis_operational_native_confirmation import KisNativeOperationalStartConfirmations
from live_trader.kis_scoped_operational_authority import KisScopedAuthorityError
import test_kis_scoped_operational_state as existing_state
from test_kis_native_start_registry import Timer, QueuedThread
from live_trader.kis_native_start_preparation_jobs import KisNativePreparationJobs


class Prepared:
    def __init__(self): self.revoked=False;self.closed=False;self.confirmed=False
    def revoke(self, reason=''): self.revoked=True
    def close(self): self.closed=True
    def assert_prepared(self):
        if self.revoked: raise RuntimeError('fake prepared revoked')
    def begin_native_confirmation_window(self):
        if getattr(self, 'window_started', False): raise RuntimeError('fake native window already started')
        self.window_started=True
    def bind_activation(self, *, session, activation_reader):
        if self.revoked or session['lifecycle']!='STARTING' or activation_reader()!=session:
            raise ValueError('fake prepared activation changed')
        self.session=copy.deepcopy(session)
    def confirm_running(self, *, session, activation_reader):
        if self.revoked or session!={**self.session,'lifecycle':'RUNNING'} or activation_reader()!=session:
            raise ValueError('fake actual running session changed')
        self.confirmed=True


class NativeStateStartTests(unittest.TestCase):
    def setUp(self):
        # Reuse the original local durable authority fixture, not its tests.
        existing_state.ScopedStateStartTests.setUp(self)
        self.closed=[]
        self.registry=KisNativeStartRegistry(close_dispatch=self.closed.append,timer_factory=Timer)
        self.native=KisNativeOperationalStartConfirmations(prompt=lambda value:value.expected_phrase)
        self.native.register_invalidation_listener(self.registry.invalidate_nonce)
        for path,value in (('live_trader.kis_native_start_registry.KIS_NATIVE_START_REGISTRY',self.registry),
                           ('live_trader.kis_operational_native_confirmation.KIS_NATIVE_OPERATIONAL_START_CONFIRMATIONS',self.native)):
            active=patch(path,value);active.start();self.addCleanup(active.stop)
        self.namespace['SAFETY_CONFIRMATION_MUTATION_LOCK']=threading.RLock()
        self.namespace['timezone']=timezone
        self.namespace['_assert_kis_native_start_outside_locks']=self.assert_outside
        self.session.to_dict=lambda:{'sessionId':self.session.session_id,'deploymentId':self.session.deployment_id,
            'lifecycle':self.session.lifecycle,'metadata':copy.deepcopy(self.session.metadata)}
        self.plan={'context':self.context,'runtimeScope':{'runtimeScopeId':'runtime-alpha'},'deploymentManifestHash':'1'*64}
        functions=['_kis_native_plan_binding','_recapture_kis_native_plan','_prepare_kis_native_runtime_start',
                   '_start_kis_prepared_native_runtime','_native_prepared_kis_start',
                   '_collect_kis_native_start_preflight_truth','_commit_kis_native_start_preflight']
        exec(compile(existing_state.selected_functions(functions),'<actual native state functions>','exec'),self.namespace)
        self.namespace['_capture_kis_native_start_plan']=lambda *args:copy.deepcopy(self.plan)
        self.namespace['_assert_kis_native_start_outside_locks']=self.assert_outside
        self.controller.start.side_effect=self.prepare_controller
        self.controller.activate_prepared_native_runtime.side_effect=self.activate_controller
        self.prepared=Prepared();self.entry=self.registry.reserve(self.plan)
        self.registry.attach(self.entry,self.prepared)
        armed=self.native.arm(self.context);self.registry.arm(self.entry,armed['nativeNonce'])
        self.proof=self.native.request(armed['nativeNonce'])['attestation']
        self.prepared_called=False;self.activated_called=False

    def manager_started(self, *args):
        # Original fixture callback is replaced before any invocation.
        raise AssertionError('old manager route is not the native state test')

    def assert_outside(self):
        self.assertFalse(self.namespace['SAFETY_CONFIRMATION_MUTATION_LOCK']._is_owned())
        return True

    def prepare_controller(self,*args,**kwargs):
        self.assert_outside();self.assertIs(kwargs['prepared_native_context'],self.prepared)
        self.assertEqual(self.session.lifecycle,'STARTING');self.assertTrue(self.namespace['_KIS_CONTROL_RESERVATION'])
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)
        self.prepared_called=True
        return {'ok':True,'preparedRuntimeReady':True,'runtimeStarted':False}

    def activate_controller(self,runtime_id,prepared):
        self.assert_outside();self.assertIs(prepared,self.prepared);self.assertTrue(prepared.confirmed)
        self.assertEqual(runtime_id,'runtime-alpha');self.assertFalse(self.namespace['_KIS_CONTROL_RESERVATION'])
        self.assertEqual(self.session.lifecycle,'RUNNING')
        self.store.authorize_consent('session-a',context=self.context)
        self.activated_called=True
        return {'ok':True,'runtimeStarted':True}

    def start(self,confirmation=None):
        return self.namespace['_start_kis_prepared_native_runtime']('stock','SMALL_LIVE','portfolio-alpha',
            'deployment-alpha','strategy-alpha',safety_confirmation=confirmation or {'nativeAttestation':self.proof})

    def test_positive_actual_state_native_reserve_prepare_running_finalize_then_unlocked_activate(self):
        result=self.start();self.assertTrue(result['ok'],result)
        self.assertTrue(self.prepared_called);self.assertTrue(self.activated_called);self.assertTrue(result['runtimeStarted'])
        self.assertEqual(result['kisOperationalAuthority']['status'],'ACTIVE');self.assertNotIn('__native_entry',result)
        self.assertFalse(result['kisOperationalAuthority']['orderAuthorityGranted'])

    def test_no_native_proof_or_private_object_never_creates_runtime(self):
        result=self.start({'nativeAttestation':{}});self.assertFalse(result['ok'])
        self.controller.start.assert_not_called();self.controller.activate_prepared_native_runtime.assert_not_called()

    def test_native_valid_but_process_private_preparation_missing_denies_before_runtime(self):
        self.registry.invalidate(self.entry)
        result=self.start();self.assertFalse(result['ok'])
        self.controller.start.assert_not_called();self.controller.activate_prepared_native_runtime.assert_not_called()

    def test_state_stop_during_controller_preparation_closes_without_thread_or_consent(self):
        original=self.controller.start.side_effect
        def stopped(*args,**kwargs):
            result=original(*args,**kwargs)
            self.registry.invalidate_stop_selector('stock',runtime_id='runtime-alpha',deployment_id='deployment-alpha')
            return result
        self.controller.start.side_effect=stopped
        result=self.start();self.assertFalse(result['ok']);self.controller.activate_prepared_native_runtime.assert_not_called()
        self.assertTrue(self.entry.revoked.is_set())
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)

    def test_source_or_config_changes_after_controller_prepare_never_start_thread(self):
        def changed(*args,**kwargs):
            result=self.prepare_controller(*args,**kwargs)
            self.plan['context']={**self.context,'scopeHash':'0'*64}
            return result
        self.controller.start.side_effect=changed
        self.assertFalse(self.start()['ok']);self.controller.activate_prepared_native_runtime.assert_not_called()

    def test_thread_activation_failure_revokes_and_exact_stops_only_its_scope(self):
        self.controller.activate_prepared_native_runtime.return_value={'ok':False}
        self.controller.activate_prepared_native_runtime.side_effect=None
        self.assertFalse(self.start()['ok']);self.assertTrue(self.entry.revoked.is_set())
        self.controller.stop.assert_called_once_with(runtime_id='runtime-alpha',deployment_id='deployment-alpha')
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize_consent('session-a',context=self.context)

    def test_outer_dispatch_preserves_crypto_monitor_and_specialized_functional_path(self):
        legacy=Mock(return_value={'ok':True,'legacy':True})
        wrapped=self.namespace['_native_prepared_kis_start'](legacy)
        for args in [('crypto','SMALL_LIVE'),('stock','MONITOR'),('stock','SMALL_LIVE','','','','FUNCTIONAL_TEST')]:
            self.assertTrue(wrapped(*args)['legacy'])
        self.assertEqual(legacy.call_count,3)

    def setup_truth(self):
        self.namespace['PREFLIGHT_RECONCILIATION_MAX_AGE_SECONDS']=15
        self.namespace['governance_sha256']=__import__('live_trader.operational_governance',fromlist=['stable_sha256']).stable_sha256
        self.namespace['_operational_strategy']=lambda *a,**kw:{'broker_id':'kis'}
        self.namespace['strategy_rows']=lambda *a:[];self.namespace['portfolio_rows']=lambda:[]
        self.namespace['strategy_broker_id']=lambda row:'kis'
        self.namespace['final_preflight_checks']=lambda *a,**kw:[{'label':'fresh','status':'PASS','detail':'fresh exact fixture'}]
        manifest=type('Manifest',(),{'deployment_id':'deployment-alpha','manifest_hash':'1'*64})()
        self.namespace['OPERATIONAL_GOVERNANCE'].get_deployment_manifest.return_value=manifest
        self.namespace['OPERATIONAL_GOVERNANCE'].preflight_validity.return_value={'valid':True}
        self.namespace['OPERATIONAL_GOVERNANCE'].create_preflight_snapshot.return_value=type('Preflight',(),{
            'status':'PASS','snapshot_id':'new-actual-snapshot','expires_at':(datetime.now(timezone.utc)+timedelta(seconds=300)).isoformat()})()
        self.reconciliation={'poll':{'ok':True,'coalesced':False},'summary':{'fresh':True,'three_way_verified':True,
            'status':'pass','evidence_ages_seconds':{'account':0}},'errors':[]}
        self.namespace['refresh_preflight_reconciliation']=Mock(return_value=self.reconciliation)
        self.namespace['broker_readiness']=lambda:[]

    def test_start_preflight_is_new_force_read_not_timestamp_extension_or_running_session_rebind(self):
        self.setup_truth()
        truth=self.namespace['_collect_kis_native_start_preflight_truth'](self.plan,self.entry)
        self.namespace['refresh_preflight_reconciliation'].assert_called_once()
        self.assertEqual(truth['schemaVersion'],'kis-native-start-preflight-truth/v1')
        preflight=self.namespace['_commit_kis_native_start_preflight'](self.plan,truth,self.entry)
        self.assertEqual(preflight.snapshot_id,'new-actual-snapshot')
        self.namespace['OPERATIONAL_GOVERNANCE'].rebind_runtime_preflight.assert_not_called()

    def test_start_preflight_old_coalesced_or_failed_truth_cannot_create_snapshot(self):
        self.setup_truth()
        truth=self.namespace['_collect_kis_native_start_preflight_truth'](self.plan,self.entry)
        bad=copy.deepcopy(truth);bad['observedAt']=(datetime.now(timezone.utc)-timedelta(seconds=16)).isoformat()
        with self.assertRaises(ValueError):self.namespace['_commit_kis_native_start_preflight'](self.plan,bad,self.entry)
        bad=copy.deepcopy(truth);bad['reconciliation']['poll']['coalesced']=True
        with self.assertRaises(ValueError):self.namespace['_commit_kis_native_start_preflight'](self.plan,bad,self.entry)
        self.namespace['OPERATIONAL_GOVERNANCE'].create_preflight_snapshot.assert_not_called()

    def test_start_preflight_scope_change_during_read_denies_new_snapshot(self):
        self.setup_truth();truth=self.namespace['_collect_kis_native_start_preflight_truth'](self.plan,self.entry)
        expected=copy.deepcopy(self.plan);self.plan['context']={**self.context,'scopeHash':'0'*64}
        with self.assertRaises(ValueError):self.namespace['_commit_kis_native_start_preflight'](expected,truth,self.entry)
        self.namespace['OPERATIONAL_GOVERNANCE'].create_preflight_snapshot.assert_not_called()

    def test_start_network_seam_fails_before_any_provider_when_authority_lock_held(self):
        self.setup_truth()
        with self.namespace['SAFETY_CONFIRMATION_MUTATION_LOCK']:
            with self.assertRaises(AssertionError):self.namespace['_collect_kis_native_start_preflight_truth'](self.plan,self.entry)
        self.namespace['refresh_preflight_reconciliation'].assert_not_called()

    def setup_async(self):
        self.setup_truth()
        names=['_prepare_kis_native_manual_challenge','prepare_kis_scoped_manual_start',
               'kis_native_preparation_status','cancel_kis_native_preparation']
        exec(compile(existing_state.selected_functions(names),'<actual async native state>','exec'),self.namespace)
        self.namespace['APP_DATA_ROOT']=Path(self.directory.name)
        self.namespace['runtime_settings']=lambda:{}
        self.namespace['OPERATIONAL_GOVERNANCE'].create_preflight_snapshot.return_value=SimpleNamespace(
            status='PASS',snapshot_id='fresh-start-preflight',expires_at=(datetime.now(timezone.utc)+timedelta(seconds=300)).isoformat())
        QueuedThread.pending=[]
        self.jobs=KisNativePreparationJobs(registry=self.registry,thread_factory=QueuedThread,timer_factory=Timer)
        active=patch('live_trader.kis_native_start_preparation_jobs.KIS_NATIVE_PREPARATION_JOBS',self.jobs)
        active.start();self.addCleanup(active.stop)
        self.prepared=Prepared()
        self.public=Mock(return_value=self.prepared)
        active=patch('live_trader.kis_native_public_preparation.prepare_live_kis_native',self.public)
        active.start();self.addCleanup(active.stop)

    def test_actual_async_begin_public_read_fresh_preflight_native_confirmation_then_start_chain(self):
        self.setup_async()
        result=self.namespace['prepare_kis_scoped_manual_start']('stock','SMALL_LIVE','portfolio-alpha','deployment-alpha','strategy-alpha')
        self.assertTrue(result['ok']);self.assertNotIn('nativeNonce',result)
        self.public.assert_not_called();self.controller.start.assert_not_called()
        QueuedThread.pending.pop()()
        ready=self.namespace['kis_native_preparation_status'](result['preparationId'])
        self.assertEqual(ready['status'],'READY',ready)
        self.namespace['refresh_preflight_reconciliation'].assert_called_once()
        self.assertEqual(self.public.call_args.kwargs['maximum_seconds'],645)
        self.entry=self.jobs._jobs[result['preparationId']]['entry']
        self.proof=self.native.request(ready['nativeNonce'])['attestation']
        started=self.start();self.assertTrue(started['ok'],started);self.assertTrue(self.activated_called)
        self.assertEqual(self.jobs.status(result['preparationId'])['status'],'CONSUMED')
        self.assertFalse(self.namespace['cancel_kis_native_preparation'](result['preparationId'])['ok'])

    def test_actual_async_cancel_before_worker_has_zero_public_or_account_reads_and_no_native_proof(self):
        self.setup_async()
        result=self.namespace['prepare_kis_scoped_manual_start']('stock','SMALL_LIVE','portfolio-alpha','deployment-alpha','strategy-alpha')
        self.assertTrue(self.namespace['cancel_kis_native_preparation'](result['preparationId'])['ok'])
        QueuedThread.pending.pop()()
        self.public.assert_not_called();self.namespace['refresh_preflight_reconciliation'].assert_not_called()
        self.assertEqual(self.namespace['kis_native_preparation_status'](result['preparationId'])['status'],'CANCELLED')

    def test_actual_async_setting_change_during_public_collection_closes_before_fresh_account_read(self):
        self.setup_async()
        def changed(*args,**kwargs):
            self.assert_outside();self.registry.invalidate_all()
            return self.prepared
        self.public.side_effect=changed
        result=self.namespace['prepare_kis_scoped_manual_start']('stock','SMALL_LIVE','portfolio-alpha','deployment-alpha','strategy-alpha')
        QueuedThread.pending.pop()()
        self.assertNotEqual(self.namespace['kis_native_preparation_status'](result['preparationId'])['status'],'READY')
        self.assertTrue(self.prepared.revoked);self.assertTrue(self.prepared.closed)
        self.namespace['refresh_preflight_reconciliation'].assert_not_called()

    def test_actual_prepare_label_failure_keeps_fixed_user_reason_after_registry_revoke(self):
        self.setup_async()
        self.public.side_effect=ValueError('LIVE_NATIVE_INDEPENDENT_LABEL_REVIEW_SCOPE_UNPROVEN')
        result=self.namespace['prepare_kis_scoped_manual_start']('stock','SMALL_LIVE','portfolio-alpha','deployment-alpha','strategy-alpha')
        QueuedThread.pending.pop()()
        status=self.namespace['kis_native_preparation_status'](result['preparationId'])
        self.assertEqual(status['status'],'FAILED',status)
        self.assertEqual(status['reasonCode'],'KIS_LABEL_SCOPE_UNPROVEN')
        self.assertEqual(status['userMessage'],'국내 시세의 분봉 시각 검증이 완료되지 않아 시작을 보류했습니다.')
        self.namespace['refresh_preflight_reconciliation'].assert_not_called()
        self.controller.start.assert_not_called()
