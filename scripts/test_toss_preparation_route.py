"""Offline route and read-only regression; never constructs production state."""
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock

from live_trader.read_only_account import ReadOnlyBrokerReader
from live_trader.read_only_preparation import _instrument
from test_toss_adapter import FakeToss


class TossPreparationRouteTests(unittest.TestCase):
    def test_account_read_keeps_buying_power_separate_from_cash_and_equity(self):
        api = FakeToss()
        reader = ReadOnlyBrokerReader({"TOSS_CLIENT_ID": "fixture", "TOSS_CLIENT_SECRET": "fixture",
            "TOSS_ACCOUNT_SEQ": "1"}, toss_client=api)
        report = reader.observe_preparation("toss-kr", "005930")
        self.assertEqual(report["account"], "AVAILABLE")
        self.assertEqual(report["status"], "PARTIAL")
        self.assertIsNone(report["availableCash"])
        self.assertEqual(report["buyingPower"], 200000)
        self.assertEqual(report["fundsCheck"], "UNKNOWN")
        self.assertEqual(report["currency"], "KRW")

    def test_public_read_does_not_leak_private_amounts(self):
        report = ReadOnlyBrokerReader({"TOSS_ACCOUNT_SEQ": "1"}, toss_client=FakeToss()).read("toss-kr", "005930")
        self.assertNotIn("buyingPower", report)
        self.assertNotIn("maskedAccount", report)
        self.assertEqual(report["account"], "AVAILABLE")

    def test_wrong_snapshot_account_does_not_fallback_to_environment(self):
        report = ReadOnlyBrokerReader({"TOSS_ACCOUNT_SEQ": "99"}, toss_client=FakeToss()).observe_preparation("toss-kr", "005930")
        self.assertEqual(report["account"], "UNKNOWN")
        self.assertIsNone(report["availableCash"])

    def test_toss_cannot_be_classified_as_24_hour_crypto(self):
        self.assertEqual(_instrument("i", "0101N0", "toss", "toss")["broker"], "toss-kr")
        self.assertEqual(_instrument("i", "AAPL", "toss", "toss")["broker"], "toss-us")

    def test_server_saved_exact_selection_controls_kis_route_guard(self):
        source = (Path(__file__).parents[1] / "live_trader/state.py").read_text(encoding="utf-8")
        names = {"_selected_continuous_start_broker", "_continuous_start_requires_kis_route"}
        functions = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in names]
        selected = {"broker_id": "toss", "deployment_id": "d", "strategy_id": "s", "portfolio_gate": {"portfolioId": "p"}}
        rows = [selected, {"broker_id": "kis", "deployment_id": "kis-d", "strategy_id": "kis-s", "portfolio_gate": {"portfolioId": "kis-p"}}]
        scope = {"strategy_rows": lambda p: p, "portfolio_rows": lambda: rows,
                 "strategy_broker_id": lambda s: s["broker_id"]}
        exec(compile(ast.Module(body=functions, type_ignores=[]), "route-boundary-fixture", "exec"), scope)
        guard = scope["_continuous_start_requires_kis_route"]
        self.assertFalse(guard("stock", "SMALL_LIVE", "p", "d", "s"))
        with self.assertRaisesRegex(ValueError, "current-deployment-missing"):
            guard("stock", "SMALL_LIVE", "p", "unknown", "s")
        with self.assertRaisesRegex(ValueError, "current-deployment-missing"):
            guard("stock", "SMALL_LIVE", "p", "kis-d", "s")
        selected["broker_id"] = "kis"
        self.assertTrue(guard("stock", "SMALL_LIVE", "p", "d", "s"))
        self.assertTrue(guard("stock", "SMALL_LIVE"))
        with self.assertRaisesRegex(ValueError, "current-deployment-missing"):
            guard("stock", "SMALL_LIVE", "p", "missing", "s")


if __name__ == "__main__":
    unittest.main()
