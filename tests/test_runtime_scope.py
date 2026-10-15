from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import json
import os
import subprocess
import sys
import threading
import unittest
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousController, LiveContinuousRuntimeManager
from live_trader.continuous_dispatch import IntentJournal
from live_trader.order_management import OrderIntent
from live_trader.runtime_scope import AccountWriterLease, RuntimeScope, runtime_scope_id, scope_directory, scope_from_mapping
from trading_runtime import ClosedBar


def scope(broker="binance", account="a", deployment="binance-deployment"):
    return RuntimeScope(broker, account * 64, deployment, "stock" if broker == "kis" else "crypto", "saved-" + broker, "c" * 64)


class FakeController:
    def __init__(self, root):
        self.root = Path(root)
        self.profile_id = ""
        self.mode = "MONITOR"
        self.deployment_id = ""
        self.portfolio_id = ""
        self.requested_strategy_id = ""
        self.execution_purpose = ""
        self.functional_test_context = None
        self.runtime_scope = None
        self.supervisor = None
        self._native_runtime = None
        self.portfolio_execution_scope_id = ""
        self.portfolio_ledger = None
        self.stop_calls = 0
        self.fail_stop = False
        self.fail_monitor = False

    def bind_runtime_scope(self, selected):
        self.runtime_scope = selected
        self.profile_id = selected.profile_id
        self.deployment_id = selected.deployment_id

    def _dispatch_status(self):
        if self.runtime_scope is None:
            return {"reconciliationRequired": False}
        path = scope_directory(self.root, self.runtime_scope) / "continuous_crypto_intents.sqlite3"
        return {"reconciliationRequired": IntentJournal(path).reconciliation_required()}

    def start(self, profile, mode, portfolio="", strategy="", deployment="", purpose="", context=None):
        if mode == "MONITOR" and self.fail_monitor:
            return {"ok": False, "reason": "fixture-monitor-failure"}
        self.profile_id = profile
        self.mode = mode
        self.deployment_id = deployment
        self.portfolio_id = portfolio
        self.requested_strategy_id = strategy
        self.execution_purpose = purpose
        self.functional_test_context = context
        self.supervisor = SimpleNamespace(running=True)
        return {"ok": True, "runtime": self.snapshot()}

    def stop(self):
        self.stop_calls += 1
        if self.fail_stop:
            return {"ok": False, "reason": "fixture-stop-timeout", "runtime": {"phase": "FAILED", "running": True}}
        if self.supervisor is not None:
            self.supervisor.running = False
        self.mode = "MONITOR"
        return {"ok": True, "runtime": self.snapshot()}

    def snapshot(self):
        running = bool(self.supervisor and self.supervisor.running)
        return {"running": running, "phase": "RUNNING" if running else "STOPPED", "mode": self.mode, "deploymentId": self.deployment_id, "profileId": self.profile_id, "dispatch": self._dispatch_status()}

    def reconcile_deferred_dispatches(self, rows):
        return self._dispatch_status()


class ScopedRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.lease_root = self.root / "isolated-shared-account-writers"
        self.scopes = {s.deployment_id: s for s in (scope(), scope("upbit", "b", "upbit-deployment"))}

    def manager(self, real=False, **kwargs):
        manager = LiveContinuousRuntimeManager(self.root, account_writer_lease_root=self.lease_root, scope_resolver=lambda **kw: self.scopes[kw["deployment_id"]], controller_factory=None if real else FakeController, **kwargs)
        self.addCleanup(lambda: [lease.release() for lease in manager._writer_leases.values()])
        return manager

    def start(self, manager, deployment="binance-deployment", mode="SMALL_LIVE"):
        return manager.start("crypto", mode, strategy_id=deployment + "-strategy", deployment_id=deployment)

    def test_scope_identity_is_stable_exact_and_account_sensitive(self):
        selected = scope()
        self.assertEqual(selected, scope_from_mapping(selected.to_dict()))
        self.assertEqual(selected.runtime_id, runtime_scope_id("binance", "a" * 64, "binance-deployment"))
        self.assertNotEqual(selected.runtime_id, scope(account="b").runtime_id)
        with self.assertRaises(ValueError):
            scope_from_mapping({**selected.to_dict(), "runtimeId": "wrong"})
        with self.assertRaises(ValueError):
            RuntimeScope("binance", "a" * 64, "deployment", "stock")

    def test_two_exchanges_run_concurrently_and_exact_stop_preserves_other(self):
        manager = self.manager()
        first = self.start(manager)
        second = self.start(manager, "upbit-deployment")
        self.assertTrue(first["ok"], first)
        self.assertTrue(second["ok"], second)
        snapshot = manager.snapshot()
        self.assertEqual(2, len(snapshot["runningRuntimeIds"]))
        self.assertTrue(snapshot["profiles"]["crypto"]["ambiguous"])
        self.assertNotIn("deploymentId", snapshot["profiles"]["crypto"])
        self.assertFalse(manager.stop("crypto")["ok"])
        result = manager.stop(runtime_id=first["runtimeId"], deployment_id="binance-deployment")
        self.assertTrue(result["ok"])
        self.assertTrue(manager.runtime_for(second["runtimeId"]).supervisor.running)
        self.assertEqual([second["runtimeId"]], manager.snapshot()["runningRuntimeIds"])

    def test_stop_releases_snapshot_lock_before_joining_a_worker(self):
        manager = self.manager()
        started = self.start(manager, mode="MONITOR")
        controller = manager.runtime_for(started["runtimeId"])
        original_stop = controller.stop
        snapshotted = threading.Event()
        workers = []
        def stop_after_snapshot():
            worker = threading.Thread(target=lambda: (manager.snapshot(), snapshotted.set()))
            workers.append(worker)
            worker.start()
            worker.join(0.5)
            if worker.is_alive():
                return {"ok": False, "reason": "snapshot-deadlocked-stop", "runtime": {"phase": "FAILED", "running": True}}
            return original_stop()
        with patch.object(controller, "stop", side_effect=stop_after_snapshot):
            stopped = manager.stop(runtime_id=started["runtimeId"])
        for worker in workers:
            worker.join(1)
        self.assertTrue(stopped["ok"], stopped)
        self.assertTrue(snapshotted.is_set())

    def test_unknown_stop_selector_never_stops_all(self):
        manager = self.manager()
        result = self.start(manager)
        for selectors in ({"runtime_id": "missing"}, {"deployment_id": "missing"}, {"profile_id": "cryptoo"}, {"runtime_id": result["runtimeId"], "deployment_id": "wrong"}):
            self.assertFalse(manager.stop(**selectors)["ok"])
        self.assertEqual(0, manager.runtime_for(result["runtimeId"]).stop_calls)

    def test_same_account_cannot_run_a_second_portfolio(self):
        manager = self.manager()
        self.scopes["other"] = scope(deployment="other")
        self.assertTrue(self.start(manager)["ok"])
        blocked = self.start(manager, "other")
        self.assertFalse(blocked["ok"])
        self.assertIn("already-running", blocked["reason"])

    def test_writer_is_exclusive_across_managers_and_survives_failed_stop(self):
        first = self.manager()
        result = self.start(first)
        self.scopes["other"] = scope(deployment="other")
        second = self.manager()
        blocked = self.start(second, "other")
        self.assertFalse(blocked["ok"])
        self.assertIn("writer-already-owned", blocked["reason"])
        original = first.runtime_for(result["runtimeId"])
        original.fail_stop = True
        self.assertFalse(first.stop(runtime_id=result["runtimeId"])["ok"])
        self.assertFalse(self.start(second, "other")["ok"])
        original.fail_stop = False
        self.assertTrue(first.stop(runtime_id=result["runtimeId"])["ok"])
        self.assertTrue(self.start(second, "other")["ok"])

    def test_monitor_does_not_acquire_live_account_writer(self):
        manager = self.manager()
        result = self.start(manager, mode="MONITOR")
        self.assertTrue(result["ok"])
        self.assertEqual({}, manager._writer_leases)
        lease = AccountWriterLease(self.root, self.scopes["binance-deployment"], lease_root=self.lease_root)
        lease.acquire()
        lease.release()

    def test_restart_loads_same_scopes_without_restarting_or_writer_authority(self):
        manager = self.manager()
        result = self.start(manager, mode="MONITOR")
        self.assertTrue(manager.stop(runtime_id=result["runtimeId"])["ok"])
        restarted = self.manager()
        self.assertIn(result["runtimeId"], restarted.runtimes)
        self.assertFalse(restarted.snapshot()["running"])
        self.assertEqual({}, restarted._writer_leases)
        self.assertTrue(self.start(restarted, mode="MONITOR")["ok"])

    def test_prior_unknown_intent_blocks_other_deployment_on_same_account(self):
        manager = self.manager()
        first = self.start(manager, mode="MONITOR")
        manager.stop(runtime_id=first["runtimeId"])
        selected = self.scopes["binance-deployment"]
        journal = IntentJournal(scope_directory(self.root, selected) / "continuous_crypto_intents.sqlite3")
        intent = OrderIntent("strategy", "CRYPTO", "ETHUSDT", "BUY", 1, 100, "SMALL_LIVE", "fixture", {"runtime_evaluation_key": "fixture", "deployment_id": selected.deployment_id, "broker_id": "binance"})
        row = journal.enqueue(intent)
        journal.claim(row["command_id"])
        journal.finish(row["command_id"], "UNKNOWN")
        self.scopes["other"] = scope(deployment="other")
        restarted = self.manager()
        blocked = self.start(restarted, "other")
        self.assertFalse(blocked["ok"])
        self.assertIn("prior-dispatch", blocked["reason"])

    def test_global_monitor_attempts_every_runtime_without_live_rollback(self):
        manager = self.manager()
        first = self.start(manager)
        second = self.start(manager, "upbit-deployment")
        manager.runtime_for(first["runtimeId"]).fail_monitor = True
        result = manager.transition_running("MONITOR")
        self.assertFalse(result["ok"])
        self.assertEqual("MONITOR", manager.runtime_for(second["runtimeId"]).mode)
        self.assertFalse(manager.runtime_for(first["runtimeId"]).supervisor.running)

    def test_kis_currency_books_share_one_account_writer_until_last_clean_stop(self):
        kr = RuntimeScope("kis", "e" * 64, "kr-portfolio", "stock", capital_currency="KRW")
        us = RuntimeScope("kis", "e" * 64, "us-portfolio", "stock", capital_currency="USD")
        self.scopes.update({kr.deployment_id: kr, us.deployment_id: us})
        coordinator = SimpleNamespace(can_share=lambda first, second: True)
        manager = self.manager(account_feed_coordinator=coordinator)
        one = manager.start("stock", "SMALL_LIVE", deployment_id=kr.deployment_id)
        two = manager.start("stock", "SMALL_LIVE", deployment_id=us.deployment_id)
        self.assertTrue(one["ok"], one)
        self.assertTrue(two["ok"], two)
        self.assertEqual(kr.account_key, us.account_key)
        self.assertIs(manager._writer_leases[one["runtimeId"]], manager._writer_leases[two["runtimeId"]])
        lease = AccountWriterLease(self.root, kr, lease_root=self.lease_root)
        self.assertTrue(manager.stop(runtime_id=one["runtimeId"])["ok"])
        with self.assertRaisesRegex(ValueError, "already-owned"):
            lease.acquire()
        self.assertTrue(manager.stop(runtime_id=two["runtimeId"])["ok"])
        lease.acquire()
        lease.release()

    def test_kis_different_currency_without_shared_feed_capability_fails_honestly(self):
        kr = RuntimeScope("kis", "e" * 64, "kr-portfolio", "stock", capital_currency="KRW")
        us = RuntimeScope("kis", "e" * 64, "us-portfolio", "stock", capital_currency="USD")
        self.scopes.update({kr.deployment_id: kr, us.deployment_id: us})
        manager = self.manager()
        one = manager.start("stock", "MONITOR", deployment_id=kr.deployment_id)
        blocked = manager.start("stock", "MONITOR", deployment_id=us.deployment_id)
        self.assertFalse(blocked["ok"])
        self.assertIn("shared-market-feed", blocked["reason"])
        manager.stop(runtime_id=one["runtimeId"])
        self.assertTrue(manager.start("stock", "MONITOR", deployment_id=us.deployment_id)["ok"])

    def test_unknown_journal_keeps_writer_lease_after_thread_stops(self):
        manager = self.manager()
        result = self.start(manager)
        selected = self.scopes["binance-deployment"]
        journal = IntentJournal(scope_directory(self.root, selected) / "continuous_crypto_intents.sqlite3")
        intent = OrderIntent("strategy", "CRYPTO", "ETHUSDT", "BUY", 1, 100, "SMALL_LIVE", "fixture", {"runtime_evaluation_key": "fixture", "deployment_id": selected.deployment_id, "broker_id": "binance"})
        row = journal.enqueue(intent)
        journal.claim(row["command_id"])
        journal.finish(row["command_id"], "UNKNOWN")
        stopped = manager.stop(runtime_id=result["runtimeId"])
        self.assertTrue(stopped["ok"])
        self.assertTrue(stopped["writerLeaseRetained"])
        with self.assertRaisesRegex(ValueError, "already-owned"):
            AccountWriterLease(self.root, selected, lease_root=self.lease_root).acquire()

    def test_os_writer_lock_excludes_an_independent_python_process(self):
        selected = self.scopes["binance-deployment"]
        lease = AccountWriterLease(self.root, selected, lease_root=self.lease_root)
        lease.acquire()
        self.addCleanup(lease.release)
        code = "from pathlib import Path; import json,sys; from live_trader.runtime_scope import AccountWriterLease,scope_from_mapping; lease=AccountWriterLease(Path(sys.argv[1]),scope_from_mapping(json.loads(sys.argv[2])),lease_root=Path(sys.argv[3])); lease.acquire(); lease.release()"
        environment = dict(os.environ)
        app = Path(__file__).resolve().parents[1]
        environment["PYTHONPATH"] = str(app)
        args = [sys.executable, "-B", "-c", code, str(self.root / "different-frozen-exe-data"), json.dumps(selected.to_dict()), str(self.lease_root)]
        blocked = subprocess.run(args, capture_output=True, text=True, env=environment, timeout=15)
        self.assertNotEqual(0, blocked.returncode)
        self.assertIn("runtime-account-live-writer-already-owned", blocked.stderr)
        lease.release()
        accepted = subprocess.run(args, capture_output=True, text=True, env=environment, timeout=15)
        self.assertEqual(0, accepted.returncode, accepted.stderr)

    def test_default_lease_path_is_shared_across_data_roots_without_touching_global_path(self):
        with patch.dict(os.environ, {"LOCALAPPDATA": str(self.root / "isolated-user-local-appdata")}):
            first = AccountWriterLease(self.root / "source-data", scope())
            second = AccountWriterLease(self.root / "frozen-data", scope())
            self.assertEqual(first.path, second.path)
            self.assertTrue(first.path.is_relative_to(self.root))
            first.acquire()
            self.addCleanup(first.release)
            with self.assertRaisesRegex(ValueError, "already-owned"):
                second.acquire()
            first.release()
            second.acquire()
            second.release()

    def test_explicit_test_lease_root_never_uses_user_global_ownership(self):
        with patch("live_trader.runtime_scope.default_account_writer_lease_root", side_effect=AssertionError("global ownership forbidden in test")):
            lease = AccountWriterLease(self.root, scope(), lease_root=self.lease_root)
            lease.acquire()
            lease.release()

    def test_controller_data_paths_are_per_scope_but_capital_root_is_shared(self):
        one = LiveContinuousController(self.root)
        two = LiveContinuousController(self.root)
        one.bind_runtime_scope(scope())
        two.bind_runtime_scope(scope("upbit", "b", "upbit-deployment"))
        self.assertEqual(one.root, two.root)
        self.assertNotEqual(one.runtime_data_root, two.runtime_data_root)
        self.assertNotEqual(one._dispatcher().journal.path, two._dispatcher().journal.path)
        self.assertNotEqual(one._boundary_queue().path, two._boundary_queue().path)
        self.assertNotEqual(one._portfolio_scope_id("p", "h"), two._portfolio_scope_id("p", "h"))

    def test_real_monitor_engines_for_binance_upbit_run_and_stop_independently(self):
        manager = self.manager(real=True)
        connected = {broker: threading.Event() for broker in ("binance", "upbit")}
        sources = {
            "binance-deployment-strategy": {"strategy_id": "binance-deployment-strategy", "symbol": "ETHUSDT", "provider": "binance", "broker_id": "binance", "plugin": "ma-cross", "timeframe": "1m", "parameters": {"shortMa": 2, "longMa": 3}},
            "upbit-deployment-strategy": {"strategy_id": "upbit-deployment-strategy", "symbol": "KRW-ETH", "provider": "upbit", "broker_id": "upbit", "plugin": "ma-cross", "timeframe": "1m", "parameters": {"shortMa": 2, "longMa": 3}},
        }
        class Feed:
            def __init__(self, spec): self.provider_id = spec.provider
            def connect(self): connected[self.provider_id].set()
            def disconnect(self): pass
            def warmup(self, subscription, count): return ()
            def poll(self, timeout): return ()
        with patch.object(LiveContinuousController, "_select_standalone_strategy", side_effect=lambda profile, mode, strategy, deployment: sources[strategy]), patch("live_trader.continuous_live.feeds_for_specs", side_effect=lambda specs, **kw: [Feed(specs[0])]), patch.object(state, "snapshot", return_value={}), patch.object(state, "append_audit"), patch.object(state, "submit_order_intent", side_effect=AssertionError("financial submit forbidden")):
            one = self.start(manager, mode="MONITOR")
            two = self.start(manager, "upbit-deployment", mode="MONITOR")
            try:
                self.assertTrue(one["ok"], one)
                self.assertTrue(two["ok"], two)
                self.assertTrue(all(event.wait(1) for event in connected.values()))
                self.assertEqual(2, len(manager.snapshot()["runningRuntimeIds"]))
                self.assertTrue(manager.stop(runtime_id=one["runtimeId"])["ok"])
                self.assertTrue(manager.runtime_for(two["runtimeId"]).supervisor.running)
            finally:
                manager.stop()


if __name__ == "__main__":
    unittest.main()
