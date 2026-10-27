from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import threading
import unittest

# Load the pure module without live_trader.__init__/real .env/secret store.
import importlib.util
import sys
_module_spec = importlib.util.spec_from_file_location("_runtime_batch_tests", Path(__file__).resolve().parents[1] / "live_trader/runtime_batch.py")
_module = importlib.util.module_from_spec(_module_spec)
sys.modules[_module_spec.name] = _module
_module_spec.loader.exec_module(_module)
RuntimeBatchService, RuntimeBatchError = _module.RuntimeBatchService, _module.RuntimeBatchError


class RuntimeBatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.now = datetime(2026, 10, 27, tzinfo=timezone.utc)
        self.calls = []
        self.runtime = {}
        self.revision = "original"
        self.failure = ""
        self.entered, self.release = threading.Event(), threading.Event()
        self.service = self.make_service()

    def tearDown(self):
        self.tmp.cleanup()

    def selection(self, broker="binance"):
        return {"profile_id": "stock" if broker == "kis" else "crypto",
                "mode": "SMALL_LIVE", "deployment_id": "deployment-" + broker,
                "strategy_id": "strategy-" + broker, "portfolio_id": "portfolio-" + broker}

    def resolve(self, selection):
        broker = selection["deployment_id"].removeprefix("deployment-")
        if broker == "bad":
            raise ValueError("PRIVATE_API_KEY")
        return {"scope": {"runtimeScopeId": "runtime-" + broker, "brokerId": broker,
                         "deploymentId": selection["deployment_id"], "profileId": selection["profile_id"],
                         "capitalCurrency": "KRW" if broker in {"kis", "upbit"} else "USDT",
                         "accountFingerprint": "private-account-fingerprint"},
                "manifestInputsHash": self.revision}

    def start(self, selection, options):
        self.calls.append((deepcopy(selection), deepcopy(options)))
        if self.failure == "wait":
            self.entered.set()
            self.release.wait(2)
        if self.failure == "unknown":
            raise TimeoutError("PRIVATE_ACCOUNT_OR_KEY")
        if selection["deployment_id"] == self.failure:
            return {"ok": False, "reason": "EXISTING_GUARD_BLOCKED"}
        binding = self.resolve(selection)["scope"]
        self.runtime[binding["runtimeScopeId"]] = {**binding, "running": True, "phase": "RUNNING", "mode": "SMALL_LIVE"}
        return {"ok": True, "runtimeId": binding["runtimeScopeId"], "privateReply": "PRIVATE"}

    def make_service(self):
        return RuntimeBatchService(self.root, resolve=self.resolve, start=self.start,
                                   observe=lambda: {"runtimes": deepcopy(self.runtime)}, clock=lambda: self.now)

    def prepare(self, *brokers):
        return self.service.prepare({"selections": [self.selection(b) for b in brokers]})["batch"]

    def payload(self, batch, index=0, **kwargs):
        return {"batchId": batch["batchId"], "scopeHash": batch["scopeHash"],
                "memberId": batch["members"][index]["memberId"], "confirmed": True, **kwargs}

    def test_prepare_exact_separate_accounts_and_no_start(self):
        batch = self.prepare("binance", "kis", "upbit")
        self.assertEqual([r["brokerId"] for r in batch["members"]], ["binance", "kis", "upbit"])
        self.assertEqual([r["capitalCurrency"] for r in batch["members"]], ["USDT", "KRW", "KRW"])
        self.assertFalse(batch["fundsMerged"])
        self.assertEqual(self.calls, [])
        self.assertNotIn("private-account", str(batch))

    def test_independent_starts_route_correctly_and_preserve_kis_native(self):
        batch = self.prepare("binance", "kis")
        one = self.service.start_member(self.payload(batch))
        self.assertEqual(one["batch"]["status"], "PARTIAL")
        native = {"nativeAttestation": {"opaque": "EXACT_NATIVE"}}
        two = self.service.start_member(self.payload(batch, 1, safety_confirmation=native, marketGapPreparationId="gap-1"))
        self.assertEqual(two["batch"]["status"], "RUNNING")
        self.assertEqual(self.calls[1], (self.selection("kis"), {"safety_confirmation": native, "marketGapPreparationId": "gap-1"}))
        persisted = "".join(path.read_text() for path in self.root.glob("*.json"))
        self.assertNotIn("EXACT_NATIVE", persisted)
        self.assertNotIn("privateReply", persisted)

    def test_failure_does_not_claim_healthy_peer_stopped_or_all_running(self):
        batch = self.prepare("binance", "kis")
        self.service.start_member(self.payload(batch))
        self.failure = "deployment-kis"
        result = self.service.start_member(self.payload(batch, 1))
        self.assertFalse(result["ok"])
        self.assertEqual(result["batch"]["status"], "PARTIAL")
        self.assertEqual([r["status"] for r in result["batch"]["members"]], ["RUNNING", "BLOCKED"])
        self.assertEqual(len(self.calls), 2)

    def test_closed_or_invalid_selection_does_not_hide_healthy_crypto(self):
        batch = self.prepare("binance", "bad")
        self.assertEqual(batch["status"], "PARTIAL")
        self.assertEqual(batch["members"][1]["status"], "BLOCKED")
        self.assertNotIn("PRIVATE", str(batch))
        result = self.service.start_member(self.payload(batch))
        self.assertEqual(result["batch"]["status"], "PARTIAL")
        self.assertEqual(len(self.calls), 1)

    def test_duplicate_and_tampered_selection_rejected(self):
        with self.assertRaisesRegex(RuntimeBatchError, "DUPLICATE"):
            self.prepare("binance", "binance")
        for extra in ({"accountFingerprint": "spoof"}, {"execution_purpose": "FUNCTIONAL_TEST"}):
            with self.assertRaises(RuntimeBatchError):
                self.service.prepare({"selections": [{**self.selection(), **extra}]})
        self.assertFalse(self.calls)

    def test_expiry_binding_hash_and_confirmation_reject_before_call(self):
        batch = self.prepare("binance")
        for updates in ({"confirmed": False}, {"scopeHash": "wrong"}, {"memberId": "other"}):
            with self.assertRaises(RuntimeBatchError):
                self.service.start_member({**self.payload(batch), **updates})
        self.now += timedelta(seconds=181)
        with self.assertRaisesRegex(RuntimeBatchError, "EXPIRED"):
            self.service.start_member(self.payload(batch))
        self.assertFalse(self.calls)

    def test_binding_changed_after_prepare_cannot_inherit_authority(self):
        batch = self.prepare("binance")
        self.revision = "new-source-or-credentials"
        result = self.service.start_member(self.payload(batch))
        self.assertEqual(result["reason"], "BATCH_MEMBER_BINDING_CHANGED")
        self.assertFalse(self.calls)

    def test_duplicate_lost_reply_does_not_call_again(self):
        batch = self.prepare("binance")
        self.failure = "unknown"
        first = self.service.start_member(self.payload(batch))
        second = self.service.start_member(self.payload(batch))
        self.assertEqual(first["batch"]["status"], "UNKNOWN")
        self.assertTrue(second["replayed"])
        self.assertEqual(len(self.calls), 1)
        self.assertNotIn("PRIVATE", str(first))

    def test_concurrent_same_member_only_one_call(self):
        batch = self.prepare("binance")
        self.failure = "wait"
        thread = threading.Thread(target=lambda: self.service.start_member(self.payload(batch)))
        thread.start()
        self.assertTrue(self.entered.wait(2))
        try:
            result = self.service.start_member(self.payload(batch))
            self.assertTrue(result["replayed"])
            self.assertEqual(result["batch"]["status"], "STARTING")
        finally:
            self.release.set()
            thread.join(3)
        self.assertEqual(len(self.calls), 1)

    def test_restarted_owner_can_read_but_never_resume(self):
        batch = self.prepare("binance")
        self.service = self.make_service()
        result = self.service.start_member(self.payload(batch))
        self.assertTrue(result["replayed"])
        self.assertTrue(result["batch"]["restoredReadOnly"])
        self.assertEqual(result["batch"]["status"], "UNKNOWN")
        self.assertFalse(self.calls)

    def test_status_poll_never_starts_or_persists(self):
        batch = self.prepare("binance")
        files = list(self.root.glob("*"))
        for _ in range(5):
            self.service.status(batch["batchId"])
        self.assertEqual(files, list(self.root.glob("*")))
        self.assertFalse(self.calls)

    def test_actual_runtime_status_and_partial_termination(self):
        batch = self.prepare("binance", "upbit")
        self.service.start_member(self.payload(batch))
        self.service.start_member(self.payload(batch, 1))
        self.runtime["runtime-upbit"]["running"] = False
        self.runtime["runtime-upbit"]["phase"] = "STOPPED"
        value = self.service.status(batch["batchId"])["batch"]
        self.assertEqual(value["status"], "PARTIAL")
        self.assertEqual([r["status"] for r in value["members"]], ["RUNNING", "STOPPED"])

    def test_success_reply_without_exact_running_observation_remains_unknown(self):
        batch = self.prepare("binance")
        self.service.start_runtime = lambda *_: {"ok": True}
        result = self.service.start_member(self.payload(batch))
        self.assertEqual(result["batch"]["status"], "UNKNOWN")
        self.assertFalse(result["ok"])

    def test_expiry_is_checked_again_after_slow_binding_read(self):
        batch = self.prepare("binance")
        original = self.service.resolve
        def slow_read(selection):
            self.now += timedelta(seconds=181)
            return original(selection)
        self.service.resolve = slow_read
        with self.assertRaisesRegex(RuntimeBatchError, "EXPIRED"):
            self.service.start_member(self.payload(batch))
        self.assertFalse(self.calls)

    def test_claim_persistence_failure_precedes_start(self):
        batch = self.prepare("binance")
        original = self.service._write
        def fail_claim(name, value):
            if name.startswith("claim-"):
                raise OSError("disk-failure")
            original(name, value)
        self.service._write = fail_claim
        with self.assertRaises(OSError):
            self.service.start_member(self.payload(batch))
        self.assertFalse(self.calls)

    def test_observed_existing_runtime_does_not_turn_rejected_start_into_success(self):
        batch = self.prepare("binance")
        self.runtime["runtime-binance"] = {**self.resolve(self.selection())["scope"], "running": True}
        self.failure = "deployment-binance"
        result = self.service.start_member(self.payload(batch))
        self.assertEqual(result["batch"]["members"][0]["status"], "BLOCKED")

if __name__ == "__main__":
    unittest.main()
