"""Actual forced restore IO is outside global restore/state/manager locks."""
from contextlib import ExitStack
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousRuntimeManager
from live_trader.kis_native_start_registry import KisNativeStartRegistry
from live_trader.runtime_scope import RuntimeScope
import test_kis_native_plan_and_http as plan_fixtures
from test_kis_native_start_registry import Timer


class EmptyCryptoFeed:
    provider_id='binance'
    def connect(self): pass
    def disconnect(self): pass
    def warmup(self, subscription, count): return ()
    def poll(self, timeout): threading.Event().wait(.01);return ()


class NativeRestoreConcurrencyTests(unittest.TestCase):
    def setUp(self):
        plan_fixtures.NativePlanTests.setUp(self)
        self.registry=KisNativeStartRegistry(close_dispatch=lambda prepared:prepared.close(),timer_factory=Timer)
        self.stack.enter_context(patch('live_trader.kis_native_start_registry.KIS_NATIVE_START_REGISTRY',self.registry))
        self.plan=state._capture_kis_native_start_plan('stock','SMALL_LIVE',self.loaded.portfolio_id,'deployment-alpha','strategy-alpha')
        self.entry=self.registry.reserve(state._kis_native_plan_binding(self.plan))
        self.hash='9'*64
        self.prepared=Mock();self.prepared.assert_current=Mock()
        self.prepared.receipt={'contentHash':self.hash,'portfolioId':self.loaded.portfolio_id,'portfolioHash':self.loaded.portfolio_hash}
        self.registry.attach(self.entry,self.prepared);self.registry.arm(self.entry,'a'*43)
        self.registry.consume('a'*43,state._kis_native_plan_binding(self.plan))
        self.session_dict={'sessionId':'native-start','lifecycle':'STARTING','metadata':{'runtimeScopeId':'runtime-alpha'}}
        self.session=SimpleNamespace(lifecycle='STARTING',to_dict=lambda:dict(self.session_dict))
        self.governance.get_runtime_session.return_value=self.session
        self.registry.activated(self.entry,self.session_dict)
        self.stack.enter_context(patch.dict(state.STATE,{'active_runtime_session_ids':{'runtime-alpha':'native-start'},'orders':[],'kill_switch':False}))
        self.restore_lock=threading.RLock();self.stack.enter_context(patch.object(state,'RESTORE_CONTEXT_LOCK',self.restore_lock))
        self.program=Mock();self.program.position_rows.return_value=[];self.program.execution_event_rows.return_value=[]
        self.stack.enter_context(patch.object(state,'PROGRAM_LEDGER',self.program))
        self.publish=self.stack.enter_context(patch.object(state,'_publish_forced_restore_positions'))
        self.scope=RuntimeScope('binance','b'*64,'crypto-native-concurrency','crypto')
        self.manager=LiveContinuousRuntimeManager(self.root,account_writer_lease_root=self.root/'locks',scope_resolver=lambda **kw:self.scope)
        self.stack.enter_context(patch.object(state,'LIVE_CONTINUOUS_CONTROLLER',self.manager))
        self.stack.enter_context(patch.object(state,'snapshot',return_value={}))
        self.stack.enter_context(patch.object(state,'append_audit'))

    def assess(self, native_hash=None):
        return state.forced_restore_context_assessment(tuple(self.loaded.specs),portfolio_id=self.loaded.portfolio_id,
            portfolio_hash=self.loaded.portfolio_hash,strategy_identity_hash='8'*64,
            checkpoint_seal=None,evaluator_state=None,live_native_initialization_hash=native_hash)

    def test_actual_blocked_kis_restore_does_not_block_crypto_manager_start_snapshot_or_stop(self):
        entered,release=threading.Event(),threading.Event();results={};calls=[]
        def positions(broker):
            calls.append(broker)
            if broker=='kis':
                self.assertFalse(self.restore_lock._is_owned())
                self.assertFalse(state.SAFETY_CONFIRMATION_MUTATION_LOCK._is_owned())
                entered.set()
                if not release.wait(8):raise RuntimeError('fixture release timeout')
            return []
        router=Mock();router.list_positions.side_effect=positions
        self.stack.enter_context(patch.object(state,'LiveBrokerRouter',return_value=router))
        def run_native():
            try:results['native']=self.assess(self.hash)
            except Exception as exc:results['nativeError']=type(exc).__name__
        native=threading.Thread(target=run_native);native.start()
        try:
            self.assertTrue(entered.wait(2))
            self.assertTrue(self.restore_lock.acquire(timeout=.2));self.restore_lock.release()
            crypto=self.manager._register_scope(self.scope)
            strategy={'strategy_id':'independent-crypto','instance_id':'independent-crypto','symbol':'BTCUSDT',
                'provider':'binance','broker_id':'binance','timeframe':'1m','plugin':'ma-cross',
                'parameters':{},'permissions':{'live_small_eligible':True}}
            self.stack.enter_context(patch.object(crypto,'_select_standalone_strategy',return_value=strategy))
            self.stack.enter_context(patch.object(crypto,'_paper_final_binding_blocker',return_value=''))
            self.stack.enter_context(patch.object(crypto,'_spec_mode_allowed',return_value=True))
            self.stack.enter_context(patch('live_trader.continuous_live.feeds_for_specs',return_value=(EmptyCryptoFeed(),)))
            completed=threading.Event()
            def run_crypto():
                try:
                    results['cryptoStart']=self.manager.start('crypto','SMALL_LIVE',strategy_id='independent-crypto',deployment_id=self.scope.deployment_id)
                    results['snapshot']=self.manager.snapshot()
                    results['cryptoStop']=self.manager.stop(runtime_id=self.scope.runtime_id,deployment_id=self.scope.deployment_id)
                except Exception as exc:results['cryptoError']=str(exc)
                finally:completed.set()
            crypto_thread=threading.Thread(target=run_crypto);crypto_thread.start()
            self.assertTrue(completed.wait(2),results)
            crypto_thread.join(1)
            self.assertTrue(results['cryptoStart']['ok'],results)
            self.assertTrue(results['cryptoStop']['ok'],results)
            self.assertIn('binance',calls)  # real legacy forced restore ran too
            self.assertTrue(self.registry.invalidate_stop_selector('stock',runtime_id='runtime-alpha',deployment_id='deployment-alpha'))
            self.assertTrue(self.entry.revoked.is_set())
        finally:
            release.set();native.join(3)
            if 'crypto' in locals(): self.manager.stop(runtime_id=self.scope.runtime_id)
        self.assertFalse(native.is_alive())
        self.assertIn('nativeError',results)
        self.assertFalse(any('kis' in call.args[0] for call in self.publish.call_args_list))

    def test_unregistered_hash_or_stopped_scope_never_queries_or_publishes_account(self):
        router=Mock();self.stack.enter_context(patch.object(state,'LiveBrokerRouter',return_value=router))
        with self.assertRaises(RuntimeError):self.assess('0'*64)
        self.registry.invalidate(self.entry)
        with self.assertRaises(RuntimeError):self.assess(self.hash)
        router.list_positions.assert_not_called();self.publish.assert_not_called()

    def test_native_positive_seal_binds_hash_without_holding_collection_lock(self):
        router=Mock()
        def positions(broker):
            self.assertFalse(self.restore_lock._is_owned());return []
        router.list_positions.side_effect=positions
        self.stack.enter_context(patch.object(state,'LiveBrokerRouter',return_value=router))
        result=self.assess(self.hash)
        self.assertTrue(result['brokerSnapshotComplete'],result)
        self.assertEqual(result['contextSeal']['body']['liveNativeInitializationHash'],self.hash)
        self.publish.assert_called_once()

    def test_existing_legacy_restore_does_not_make_native_publish_hold_global_control_locks(self):
        router=Mock();router.list_positions.return_value=[]
        self.stack.enter_context(patch.object(state,'LiveBrokerRouter',return_value=router))
        held,release=threading.Event(),threading.Event()
        def legacy_owner():
            with self.restore_lock:
                held.set();release.wait(4)
        owner=threading.Thread(target=legacy_owner);owner.start()
        self.assertTrue(held.wait(1))
        result={};done=threading.Event()
        def native():
            try:self.assess(self.hash)
            except Exception as exc:result['error']=str(exc)
            finally:done.set()
        worker=threading.Thread(target=native);worker.start()
        try:
            self.assertTrue(done.wait(1),result)
            self.assertEqual(result.get('error'),'KIS_NATIVE_RESTORE_PUBLICATION_BUSY_RETRY_PREPARATION')
            self.assertTrue(state.SAFETY_CONFIRMATION_MUTATION_LOCK.acquire(timeout=.2))
            state.SAFETY_CONFIRMATION_MUTATION_LOCK.release()
            self.assertTrue(self.registry.invalidate_stop_selector('stock',runtime_id='runtime-alpha',deployment_id='deployment-alpha'))
            self.publish.assert_not_called()
        finally:
            release.set();owner.join(2);worker.join(2)
        self.assertFalse(worker.is_alive() or owner.is_alive())
