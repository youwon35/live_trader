from __future__ import annotations
import unittest
from types import SimpleNamespace
from live_trader.account_runtime_governance import select_session_bindings, unique_session_binding, runtime_rows

def session(key, broker, deployment, profile="crypto", mode="SMALL_LIVE"):
    value = {"profile": profile, "deploymentId": deployment, "mode": mode,
             "metadata": {"brokerId": broker, "runtimeScopeId": key}}
    return SimpleNamespace(to_dict=lambda: value, mode=mode, session_id="session:"+key)

class ExactAccountSessionTest(unittest.TestCase):
    def setUp(self):
        self.active = {"runtime-binance": "b", "runtime-upbit": "u"}
        self.sessions = {"b": session("runtime-binance", "binance", "dep-b"), "u": session("runtime-upbit", "upbit", "dep-u", mode="MONITOR")}
    def lookup(self, **filters):
        return unique_session_binding(self.active, self.sessions.get, **filters)
    def test_selects_binance_and_upbit_separately_in_same_crypto_profile(self):
        self.assertEqual("runtime-binance", self.lookup(broker_id="binance", deployment_id="dep-b")[0])
        self.assertEqual("MONITOR", self.lookup(runtime_id="runtime-upbit")[1].mode)
    def test_missing_exact_identity_never_falls_back(self):
        self.assertEqual(("", None), self.lookup(runtime_id="runtime-missing", broker_id="binance"))
        self.assertEqual(("", None), self.lookup(runtime_id="runtime-binance", deployment_id="dep-u"))
        self.assertEqual(("", None), self.lookup(deployment_id="missing"))
        self.assertEqual(("", None), self.lookup(profile_id="crypto"))
    def test_changed_scope_and_unreadable_session_block_instead_of_disappear(self):
        self.sessions["b"] = session("runtime-upbit", "binance", "dep-b")
        with self.assertRaisesRegex(ValueError, "scope-mismatch"): self.lookup(runtime_id="runtime-binance")
        self.sessions["b"] = None
        with self.assertRaisesRegex(ValueError, "unreadable"): self.lookup(runtime_id="runtime-binance")
    def test_all_kill_targets_and_exact_account_stop_preserve_other_bindings(self):
        rows = select_session_bindings(self.active, self.sessions.get)
        self.assertEqual(2, len(rows))
        rows = select_session_bindings(self.active, self.sessions.get, deployment_id="dep-b")
        self.assertEqual(["runtime-binance"], [row[0] for row in rows])
        self.assertEqual(2, len(self.active))
    def test_real_runtimes_take_precedence_over_ambiguous_compatibility_projection(self):
        self.assertEqual({"b": {"running": True}}, runtime_rows({"runtimes": {"b": {"running": True}}, "profiles": {"crypto": {"ambiguous": True}}}))
        self.assertEqual({}, runtime_rows({"profiles": {"crypto": {"ambiguous": True}}}))

if __name__ == "__main__": unittest.main()
