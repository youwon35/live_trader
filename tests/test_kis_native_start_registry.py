import copy
import threading
import unittest
from unittest.mock import Mock

from live_trader.kis_native_start_registry import KisNativeStartRegistry
from live_trader.kis_native_start_preparation_jobs import KisNativePreparationJobs
from live_trader.kis_operational_native_confirmation import KisNativeOperationalStartConfirmations
from test_kis_scoped_operational_authority import context


class Timer:
    def __init__(self, seconds, callback): self.callback=callback;self.cancelled=False
    def start(self): pass
    def cancel(self): self.cancelled=True


class QueuedThread:
    pending=[]
    def __init__(self, target, **kwargs): self.target=target
    def start(self): self.pending.append(self.target)


class NativeRegistryTests(unittest.TestCase):
    def setUp(self):
        self.now=1000.0;self.closed=[]
        self.registry=KisNativeStartRegistry(monotonic_clock=lambda:self.now,
            close_dispatch=self.closed.append,timer_factory=Timer)
        self.binding={'context':context(),'runtimeScope':{'runtimeScopeId':'runtime-alpha'},'deploymentManifestHash':'1'*64}
        self.prepared=Mock()
        self.entry=self.registry.reserve(self.binding)
        self.registry.attach(self.entry,self.prepared)

    def arm(self): self.registry.arm(self.entry,'n'*43)

    def test_exact_nonce_private_object_one_use(self):
        self.arm();self.assertIs(self.registry.consume('n'*43,self.binding),self.entry)
        with self.assertRaises(RuntimeError):self.registry.consume('n'*43,self.binding)
        self.assertTrue(self.entry.revoked.is_set());self.assertEqual(self.closed,[self.prepared])

    def test_scope_change_burns_without_object_reuse(self):
        self.arm();changed=copy.deepcopy(self.binding);changed['context']['scopeHash']='0'*64
        with self.assertRaises(RuntimeError):self.registry.consume('n'*43,changed)
        self.assertTrue(self.entry.revoked.is_set())

    def test_expiry_and_rollback_never_keep_prepared_socket(self):
        for offset in (45,-1):
            with self.subTest(offset=offset):
                entry=self.registry.reserve(self.binding);self.registry.attach(entry,self.prepared);self.registry.arm(entry,'a'*43)
                self.now+=offset
                with self.assertRaises(RuntimeError):self.registry.binding(entry)
                self.assertTrue(entry.revoked.is_set())
                self.now=1000

    def test_rearm_invalidates_old_scope_and_preserves_other_account(self):
        other=copy.deepcopy(self.binding);other['context']['runtimeScopeId']='runtime-other'
        second=self.registry.reserve(other)
        third=self.registry.reserve(self.binding)
        self.assertTrue(self.entry.revoked.is_set());self.registry.binding(second);self.registry.binding(third)

    def test_exact_stop_global_stop_and_foreign_selector(self):
        for selector in ({'profile_id':'crypto','runtime_id':'runtime-alpha','deployment_id':'deployment-alpha'},
                         {'profile_id':'stock','runtime_id':'unknown','deployment_id':'deployment-alpha'},
                         {'profile_id':'stock','runtime_id':'runtime-alpha','deployment_id':''},
                         {'profile_id':'stock','runtime_id':42,'deployment_id':'deployment-alpha'}):
            self.assertFalse(self.registry.invalidate_stop_selector(**selector));self.registry.binding(self.entry)
        self.assertTrue(self.registry.invalidate_stop_selector('stock',runtime_id='runtime-alpha',deployment_id='deployment-alpha'))
        self.assertTrue(self.entry.revoked.is_set())
        other=self.registry.reserve(self.binding)
        self.assertTrue(self.registry.invalidate_stop_selector());self.assertTrue(other.revoked.is_set())

    def test_keep_is_only_consumed_server_entry(self):
        self.arm()
        with self.assertRaises(RuntimeError):self.registry.invalidate_all(keep=self.entry)
        self.registry.consume('n'*43,self.binding)
        self.registry.invalidate_all(keep=self.entry);self.registry.binding(self.entry)
        with self.assertRaises(RuntimeError):self.registry.invalidate_all(keep=copy.copy(self.entry))

    def test_native_cancel_and_same_scope_rearm_revoke_data_immediately(self):
        native=KisNativeOperationalStartConfirmations(prompt=lambda value:None,clock=lambda:1800000000,monotonic_clock=lambda:self.now)
        native.register_invalidation_listener(self.registry.invalidate_nonce)
        armed=native.arm(self.binding['context']);self.registry.arm(self.entry,armed['nativeNonce'])
        self.assertFalse(native.request(armed['nativeNonce'])['ok'])
        self.assertTrue(self.entry.revoked.is_set());self.prepared.revoke.assert_called_once()

    def test_private_consume_returns_nonce_public_consume_contract_unchanged(self):
        native=KisNativeOperationalStartConfirmations(prompt=lambda value:value.expected_phrase,
            clock=lambda:1800000000,monotonic_clock=lambda:self.now)
        armed=native.arm(self.binding['context']);proof=native.request(armed['nativeNonce'])['attestation']
        consumed=native.consume_prepared_start(proof,self.binding['context'])
        self.assertEqual(consumed['nativeNonce'],armed['nativeNonce'])
        armed=native.arm(self.binding['context']);proof=native.request(armed['nativeNonce'])['attestation']
        self.assertEqual(set(native.consume(proof,self.binding['context'])),{'ok','context'})

    def test_starting_to_running_cache_is_exact(self):
        self.arm();self.registry.consume('n'*43,self.binding)
        initial={'sessionId':'a','lifecycle':'STARTING','configHash':'1'*64}
        self.registry.activated(self.entry,initial)
        with self.assertRaises(RuntimeError):self.registry.activated_running(self.entry,{**initial,'lifecycle':'RUNNING','configHash':'2'*64})
        self.registry.activated_running(self.entry,{**initial,'lifecycle':'RUNNING'})
        self.assertEqual(self.registry.activation(self.entry)['lifecycle'],'RUNNING')

    def test_consumed_native_proof_replay_cannot_close_a_running_private_object(self):
        native=KisNativeOperationalStartConfirmations(prompt=lambda value:value.expected_phrase,
            clock=lambda:1800000000,monotonic_clock=lambda:self.now)
        native.register_invalidation_listener(self.registry.invalidate_nonce)
        armed=native.arm(self.binding['context']);self.registry.arm(self.entry,armed['nativeNonce'])
        proof=native.request(armed['nativeNonce'])['attestation']
        value=native.consume_prepared_start(proof,self.binding['context'])
        self.registry.consume(value['nativeNonce'],self.binding)
        self.registry.activated(self.entry,{'lifecycle':'STARTING'})
        self.registry.activated_running(self.entry,{'lifecycle':'RUNNING'})
        self.assertFalse(native.consume(proof,{})['ok'])
        self.assertFalse(native.request(armed['nativeNonce'])['ok'])
        self.registry.binding(self.entry);self.assertFalse(self.entry.revoked.is_set())


class NativePreparationJobTests(unittest.TestCase):
    def setUp(self):
        self.now=1000.0;self.closed=[];QueuedThread.pending=[]
        self.registry=KisNativeStartRegistry(monotonic_clock=lambda:self.now,close_dispatch=self.closed.append,timer_factory=Timer)
        self.jobs=KisNativePreparationJobs(registry=self.registry,clock=lambda:self.now,thread_factory=QueuedThread,timer_factory=Timer)
        self.binding={'context':context(),'runtimeScope':{},'deploymentManifestHash':'1'*64}
        self.entry=self.registry.reserve(self.binding)
        self.obj=Mock();self.registry.attach(self.entry,self.obj)
    def ready(self,publish):
        publish('CHECKING_ACCOUNT');self.registry.arm(self.entry,'a'*43)
        return {'ok':True,'nativeNonce':'a'*43,'context':self.binding['context']}
    def test_async_response_has_no_nonce_no_runtime_and_status_ready_after_work(self):
        result=self.jobs.begin(entry=self.entry,run=self.ready)
        self.assertNotIn('nativeNonce',result);self.assertFalse(result['runtimeStarted'])
        self.assertEqual(self.jobs.status(result['preparationId'])['status'],'PREPARING_PUBLIC')
        QueuedThread.pending.pop()()
        status=self.jobs.status(result['preparationId']);self.assertEqual(status['status'],'READY');self.assertEqual(status['nativeNonce'],'a'*43)
    def test_cancel_before_worker_means_no_producer_call(self):
        producer=Mock();result=self.jobs.begin(entry=self.entry,run=producer)
        self.assertEqual(self.jobs.cancel(result['preparationId'])['status'],'CANCELLED')
        QueuedThread.pending.pop()();producer.assert_not_called();self.assertTrue(self.entry.revoked.is_set())
    def test_setting_change_during_read_can_never_publish_ready(self):
        def changed(publish):
            publish('CHECKING_ACCOUNT');self.registry.invalidate_all()
            return {'ok':True,'nativeNonce':'a'*43}
        result=self.jobs.begin(entry=self.entry,run=changed);QueuedThread.pending.pop()()
        self.assertNotEqual(self.jobs.status(result['preparationId'])['status'],'READY')
    def test_unknown_job_restart_and_expired_ready_are_not_authority(self):
        result=self.jobs.begin(entry=self.entry,run=self.ready);QueuedThread.pending.pop()()
        self.assertFalse(KisNativePreparationJobs(registry=self.registry).status(result['preparationId'])['ok'])
        self.now+=45
        self.assertEqual(self.jobs.status(result['preparationId'])['status'],'EXPIRED')
        self.assertTrue(self.entry.revoked.is_set())
    def test_native_consumed_job_cancel_cannot_stop_running_runtime(self):
        result=self.jobs.begin(entry=self.entry,run=self.ready);QueuedThread.pending.pop()()
        self.jobs.consume('a'*43)
        self.assertFalse(self.jobs.cancel(result['preparationId'])['ok']);self.registry.binding(self.entry)
    def test_deadline_expiry_before_worker_has_zero_producer_calls(self):
        producer=Mock();result=self.jobs.begin(entry=self.entry,run=producer)
        self.jobs._jobs[result['preparationId']]['timer'].callback()
        QueuedThread.pending.pop()();producer.assert_not_called()
        self.assertEqual(self.jobs.status(result['preparationId'])['status'],'EXPIRED')
    def test_failed_reason_is_fixed_code_and_no_remote_values(self):
        result=self.jobs.begin(entry=self.entry,run=lambda publish:{'ok':False,'reason':'remote payload sensitive value'})
        QueuedThread.pending.pop()()
        status=self.jobs.status(result['preparationId'])
        self.assertEqual(status['reasonCode'],'KIS_NATIVE_PREPARATION_FAILED');self.assertNotIn('sensitive',str(status))

    def test_unproved_label_file_or_scope_has_fixed_korean_hold_message(self):
        result=self.jobs.begin(entry=self.entry,run=lambda publish:{'ok':False,'reason':'LIVE_NATIVE_INDEPENDENT_LABEL_REVIEW_SCOPE_UNPROVEN'})
        QueuedThread.pending.pop()()
        status=self.jobs.status(result['preparationId'])
        self.assertEqual(status['reasonCode'],'KIS_LABEL_SCOPE_UNPROVEN')
        self.assertEqual(status['userMessage'],'국내 시세의 분봉 시각 검증이 완료되지 않아 시작을 보류했습니다.')

    def test_expired_unpolled_ready_jobs_do_not_permanently_fill_capacity(self):
        keys=[]
        for index in range(17):
            self.entry=self.registry.reserve(self.binding)
            self.registry.attach(self.entry,Mock())
            result=self.jobs.begin(entry=self.entry,run=self.ready)
            QueuedThread.pending.pop()()
            keys.append(result['preparationId'])
            self.now+=46
        self.assertEqual(len(self.jobs._jobs),16)
        self.assertNotIn(keys[0],self.jobs._jobs)
        self.assertEqual(self.jobs.status(keys[-1])['status'],'EXPIRED')
