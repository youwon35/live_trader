from __future__ import annotations
import importlib
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP.parents[1] / 'packages/trading_runtime'))
PACKAGE = '_live_latency_optin_isolated'
package = types.ModuleType(PACKAGE)
package.__path__ = [str(APP / 'live_trader')]
sys.modules[PACKAGE] = package
options = importlib.import_module(PACKAGE + '.runtime_latency_optin')
controller_module = importlib.import_module(PACKAGE + '.continuous_live')


class FakeEngine:
    def __init__(self, specs, **kwargs):
        self.specs = specs
        self.kwargs = kwargs
        self.mode = 'MONITOR'

    def transition_mode(self, mode, **kwargs):
        self.mode = mode
        return None

    def seed_history(self, instance, bars):
        return 0

    def snapshot(self):
        return {'mode': self.mode, 'decisions': 0}


class FakeSupervisor:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.engine = kwargs['engine']
        self.running = False
        self.start_count = 0
        self.instances.append(self)

    def start(self):
        # Only a fake marker: no worker, socket or native runtime starts.
        self.running = True
        self.start_count += 1

    def snapshot(self):
        return {'schemaVersion': 'continuous-runtime-supervisor-v1',
                'phase': 'RUNNING', 'running': self.running,
                'engine': self.engine.snapshot()}


class RuntimeLatencyOptinTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        FakeSupervisor.instances.clear()

    def test_default_off_has_no_observer_import_or_files(self):
        import builtins
        original_import = builtins.__import__
        def guarded_import(name, *args, **kwargs):
            if name == 'trading_runtime.runtime_latency_diagnostics':
                raise AssertionError('disabled must not import/create an observer')
            return original_import(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=guarded_import):
            for flag in ('', 'true', '0', 'yes'):
                os.environ[options.ENVIRONMENT_FLAG] = flag
                self.assertEqual((None, None), options.runtime_latency_options(self.root, 'crypto', 'a'*64))
        self.assertEqual([], list(self.root.iterdir()))

    def test_explicit_option_and_per_start_unique_dedicated_paths(self):
        first, path = options.runtime_latency_options(self.root, 'crypto', 'a'*64, enabled=True)
        second, next_path = options.runtime_latency_options(self.root, 'crypto', 'a'*64, enabled=True)
        self.assertIsNotNone(first)
        self.assertNotEqual(first.process_key, second.process_key)
        self.assertNotEqual(path, next_path)
        self.assertEqual(self.root / 'logs' / 'diagnostics', path.parent)
        self.assertIn(first.process_key, path.name)
        self.assertLess(len(path.stem.removesuffix('_latency')), 129)
        os.environ[options.ENVIRONMENT_FLAG] = '1'
        self.assertEqual((None, None), options.runtime_latency_options(self.root, 'crypto', 'a'*64, enabled=False))
        self.assertEqual([], list(self.root.iterdir()))
        self.assertEqual([], first.snapshot()['records'])

    def test_diagnostic_configuration_and_constructor_failure_are_isolated(self):
        from trading_runtime import runtime_latency_diagnostics as shared
        for profile, digest in (('other','a'*64), ('crypto','../escape'), ('stock','x'*129), ('crypto',None)):
            self.assertEqual((None,None), options.runtime_latency_options(self.root,profile,digest,enabled=True))
        with patch.object(shared, 'RuntimeLatencyDiagnostics', side_effect=OSError('diagnostics unavailable')):
            self.assertEqual((None,None), options.runtime_latency_options(self.root,'crypto','a'*64,enabled=True))
        self.assertEqual([], list(self.root.iterdir()))

    def fake_start(self, enabled):
        state = types.ModuleType(PACKAGE + '.state')
        state.FUNCTIONAL_TEST_EXECUTION_PURPOSE = 'FUNCTIONAL_TEST'
        state.RUNTIME_MODE_LOCK = threading.RLock()
        state.snapshot = Mock(return_value={'liveArmed':False, 'orderSubmitAllowed':False})
        state.append_audit = Mock()
        state.STATE = {}
        spec = types.SimpleNamespace(portfolio_id='standalone-scope', portfolio_hash='a'*64,
            strategy_id='source', strategy_instance_id='instance', provider='binance',
            symbol='ETHUSDT', instrument_id='binance:spot:ETHUSDT', timeframe='5m')
        feed = types.SimpleNamespace(provider_id='binance', connect=Mock(),disconnect=Mock(),warmup=Mock(return_value=[]))
        feeds = Mock(return_value=[feed])
        stores = []
        def store(path):
            stores.append(path)
            return types.SimpleNamespace(path=path)
        controller = controller_module.LiveContinuousController(self.root, latency_diagnostics_enabled=enabled)
        controller._dispatch_status = Mock(return_value={'reconciliationRequired':False})
        controller._select_standalone_strategy = Mock(return_value={'source_path':'sealed-fixture.json','permissions':{'live_allowed':False}})
        controller._standalone_spec = Mock(return_value=spec)
        controller._single_broker_context_blocker = Mock(return_value='')
        controller._loaded_portfolio_lineage_blocker = Mock(return_value='')
        controller._configure_portfolio_execution = Mock(return_value='')
        controller._dispatcher = Mock(return_value=types.SimpleNamespace())
        with patch.dict(sys.modules,{PACKAGE+'.state':state}), patch.object(package,'state',state,create=True), \
             patch.object(controller_module,'PortfolioRuntimeEngine',FakeEngine), \
             patch.object(controller_module,'ContinuousRuntimeSupervisor',FakeSupervisor), \
             patch.object(controller_module,'DurableRuntimeState',side_effect=store), \
             patch.object(controller_module,'BuiltinBarSignalEvaluator',return_value=object()), \
             patch.object(controller_module,'required_warmup_bars',return_value=0), \
             patch.object(controller_module,'feeds_for_specs',feeds), \
             patch('socket.socket.connect',side_effect=AssertionError('network forbidden')):
            result = controller.start('crypto','MONITOR',strategy_id='source')
            self.assertTrue(result['ok'],result)
        self.assertEqual([], list(self.root.iterdir()))
        self.assertFalse(result['snapshot']['liveArmed'])
        self.assertFalse(result['snapshot']['orderSubmitAllowed'])
        return controller, feeds, stores, result, state

    def test_off_preserves_adapter_kwargs_and_snapshot(self):
        controller, feeds, stores, result, _ = self.fake_start(False)
        self.assertNotIn('latency_diagnostics',controller.supervisor.engine.kwargs)
        self.assertNotIn('latency_diagnostics',feeds.call_args.kwargs)
        self.assertNotIn('latency_diagnostics',controller.supervisor.kwargs)
        self.assertNotIn('latency_diagnostics_path',controller.supervisor.kwargs)
        self.assertEqual([self.root/'logs'/('continuous_crypto_'+'a'*16+'_engine.json'),
                          self.root/'logs'/'continuous_crypto_status.json'], stores)
        self.assertNotIn('latency',str(result['runtime']).lower())

    def test_on_forwards_one_observer_without_changing_status_or_hash_paths(self):
        off_controller, _, off_stores, off_result, _ = self.fake_start(False)
        controller, feeds, stores, result, _ = self.fake_start(True)
        observer = controller.supervisor.kwargs['latency_diagnostics']
        self.assertIs(observer, controller.supervisor.engine.kwargs['latency_diagnostics'])
        self.assertIs(observer, feeds.call_args.kwargs['latency_diagnostics'])
        path = controller.supervisor.kwargs['latency_diagnostics_path']
        self.assertEqual(self.root/'logs'/'diagnostics',path.parent)
        self.assertIn(observer.process_key,path.name)
        self.assertEqual(off_stores,stores)
        self.assertEqual(off_result,result)
        self.assertEqual('MONITOR',controller.supervisor.engine.mode)
        self.assertEqual([],observer.snapshot()['records'])

    def test_running_reuse_does_not_toggle_observer_or_restart(self):
        controller, _, _, _, state = self.fake_start(False)
        original = controller.supervisor
        controller._latency_diagnostics_enabled = True
        os.environ[options.ENVIRONMENT_FLAG] = '1'
        with patch.dict(sys.modules,{PACKAGE+'.state':state}),patch.object(package,'state',state,create=True), \
             patch.object(options,'runtime_latency_options',side_effect=AssertionError('running reuse must not create/toggle diagnostics')), \
             patch.object(controller_module,'feeds_for_specs',side_effect=AssertionError('must not reconnect')):
            result=controller.start('crypto','MONITOR',strategy_id='source')
        self.assertTrue(result['ok'],result)
        self.assertIs(original,controller.supervisor)
        self.assertEqual(1,original.start_count)
        self.assertNotIn('latency_diagnostics',original.kwargs)

    def test_factory_failure_does_not_change_monitor_start_or_permissions(self):
        from trading_runtime import runtime_latency_diagnostics as shared
        with patch.object(shared,'RuntimeLatencyDiagnostics',side_effect=ValueError('diagnostic only')):
            controller,feeds,_,result,_ = self.fake_start(True)
        self.assertEqual('MONITOR',controller.mode)
        self.assertNotIn('latency_diagnostics',feeds.call_args.kwargs)
        self.assertNotIn('latency_diagnostics',controller.supervisor.kwargs)
        self.assertFalse(result['snapshot']['orderSubmitAllowed'])


if __name__ == '__main__':
    unittest.main()
