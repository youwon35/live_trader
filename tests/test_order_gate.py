import copy
import unittest
from unittest.mock import patch

from live_trader import state


class OrderGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))

    def test_order_and_risk_classes_come_from_shared_runtime(self) -> None:
        self.assertEqual(state.OrderIntent.__module__, "trading_runtime.order_management")
        self.assertEqual(state.PreTradeRiskGate.__module__, "trading_runtime.risk_engine")
        self.assertEqual(state.PreTradeContext.__module__, "trading_runtime.risk_engine")
        self.assertEqual(state.StrategyExecutionRunner.__module__, "trading_runtime.strategy_runner")

    def test_order_gate_blocks_buy_when_new_entries_are_blocked(self) -> None:
        state.STATE["new_entries_blocked"] = True

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("신규 진입 차단", reason)

    def test_order_gate_blocks_when_readiness_has_blockers(self) -> None:
        state.STATE["new_entries_blocked"] = False

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 2}},
            "SELL",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("readiness blocker 2개", reason)

    def test_order_gate_records_dry_run_without_broker_transmission(self) -> None:
        state.STATE["new_entries_blocked"] = False

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertTrue(ok)
        self.assertEqual(order_state, "dry_run")
        self.assertEqual(queue_state, "simulated")
        self.assertIn("브로커 전송 없이", reason)

    def test_order_gate_holds_non_dry_run_until_adapter_is_verified(self) -> None:
        state.STATE["new_entries_blocked"] = False

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 0}},
            "SELL",
            dry_run=False,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "adapter_blocked")
        self.assertEqual(queue_state, "held")
        self.assertIn("실제 주문 어댑터 안전 검증 전", reason)

    def test_order_gate_uses_pre_trade_risk_engine_for_daily_loss(self) -> None:
        state.STATE["new_entries_blocked"] = False
        state.STATE["risk_settings"]["daily_loss_limit_pct"] = -2.0

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertTrue(ok)
        self.assertEqual(order_state, "dry_run")
        self.assertTrue(report.can_submit)

        state.STATE["risk_settings"]["daily_loss_limit_pct"] = 0.5
        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("일일 손실 한도", reason)
        self.assertFalse(report.can_submit)

    def test_kill_switch_forces_monitor_mode_and_blocks_new_entries(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False

        result = state.set_flag("kill_switch", True)

        self.assertTrue(result["ok"])
        self.assertEqual(state.STATE["mode"], "MONITOR")
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertTrue(state.STATE["kill_switch"])

    def test_full_live_mode_is_blocked_when_warnings_remain(self) -> None:
        state.STATE["mode"] = "MONITOR"
        snapshot = {"summary": {"blocker_count": 0, "warning_count": 1}}

        with patch("live_trader.state.snapshot", return_value=snapshot):
            result = state.set_mode("FULL_LIVE")

        self.assertFalse(result["ok"])
        self.assertEqual(state.STATE["mode"], "MONITOR")
        self.assertIn("경고 0개", result["reason"])

    def test_automation_provider_validation_keeps_stock_route_on_kis(self) -> None:
        result = state.set_automation_profile("stock", True, provider="binance", mode="MONITOR")

        self.assertFalse(result["ok"])
        self.assertEqual(state.STATE["automation"]["stock"]["provider"], "kis")
        self.assertIn("kis만 허용", result["reason"])

    def test_submit_test_intent_creates_dry_run_order_when_gate_passes(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-OK", "symbol": "BTCUSDT", "live_allowed": True}],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.submit_test_intent()

        self.assertTrue(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertEqual(order["order_id"], "LIVE-DRY-0001")
        self.assertEqual(order["state"], "dry_run")
        self.assertEqual(order["queue_state"], "simulated")
        self.assertTrue(order["dry_run"])
        self.assertEqual(order["strategy_id"], "LIVE-OK")
        self.assertIn("risk_report", order)
        self.assertTrue(order["risk_report"]["can_submit"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn("LIVE-DRY-0001", audit_detail)
        self.assertIn("BTCUSDT BUY dry_run/simulated", audit_detail)
        self.assertIn("risk pass", audit_detail)
        self.assertIn("제출 허용", audit_detail)

    def test_submit_test_intent_audit_records_blocking_risk_reason(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = True
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-BLOCKED", "symbol": "BTCUSDT", "live_allowed": True}],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.submit_test_intent()

        self.assertFalse(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertEqual(order["state"], "risk_blocked")
        self.assertFalse(order["risk_report"]["can_submit"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn("BTCUSDT BUY risk_blocked/blocked", audit_detail)
        self.assertIn("risk pass", audit_detail)
        self.assertIn("fail", audit_detail)
        self.assertIn("제출 차단", audit_detail)
        self.assertIn("신규 진입 차단", audit_detail)

    def test_submit_test_intent_holds_non_dry_run_order_even_without_blockers(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-OK", "symbol": "069500.KS", "live_allowed": True}],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.submit_test_intent()

        self.assertFalse(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertEqual(order["order_id"], "LIVE-BLOCK-0001")
        self.assertEqual(order["state"], "adapter_blocked")
        self.assertEqual(order["queue_state"], "held")
        self.assertFalse(order["dry_run"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn("LIVE-BLOCK-0001", audit_detail)
        self.assertIn("adapter_blocked/held", audit_detail)
        self.assertIn("risk pass", audit_detail)
        self.assertIn("제출 차단", audit_detail)

    def test_strategy_cycle_without_signal_records_no_order(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [
                {
                    "strategy_id": "LIVE-NO-SIGNAL",
                    "name": "No Signal",
                    "symbol": "BTCUSDT",
                    "asset": "코인",
                    "live_allowed": True,
                }
            ],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.run_strategy_cycle("crypto")

        self.assertTrue(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 0)
        self.assertEqual(state.STATE["strategy_runner"]["last_strategy"], "LIVE-NO-SIGNAL")
        self.assertEqual(state.STATE["strategy_runner"]["last_signal"], "-")
        self.assertIn("전략 신호 없음", state.STATE["audit"][-1]["detail"])

    def test_strategy_cycle_signal_routes_through_pre_trade_gate(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [
                {
                    "strategy_id": "LIVE-RUNNER-BUY",
                    "name": "Runner Buy",
                    "symbol": "BTCUSDT",
                    "asset": "코인",
                    "plugin": "breakout",
                    "live_allowed": True,
                    "signal": "BUY",
                    "reference_price": 65000,
                    "quantity": 0.01,
                }
            ],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.run_strategy_cycle("crypto")

        self.assertTrue(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertEqual(order["strategy_id"], "LIVE-RUNNER-BUY")
        self.assertEqual(order["state"], "dry_run")
        self.assertEqual(order["runner_report"]["signal"], "BUY")
        self.assertTrue(order["risk_report"]["can_submit"])
        self.assertEqual(result["runner_report"]["intent"]["metadata"]["runner"], "StrategyExecutionRunner")
        self.assertEqual(state.STATE["audit"][-1]["event"], "전략 Runner")
        self.assertIn("risk pass", state.STATE["audit"][-1]["detail"])


if __name__ == "__main__":
    unittest.main()
