import copy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from live_trader import state
from live_trader.audit_store import SQLiteAuditEventStore


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

    def test_submit_test_intent_persists_common_audit_event(self) -> None:
        original_store = state.AUDIT_STORE
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-AUDIT", "symbol": "BTCUSDT", "live_allowed": True}],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLiteAuditEventStore(Path(temp_dir) / "live_trader_audit.sqlite3")
            state.AUDIT_STORE = store
            try:
                with patch("live_trader.state.snapshot", return_value=fake_snapshot):
                    result = state.submit_test_intent()
            finally:
                state.AUDIT_STORE = original_store

            self.assertTrue(result["ok"])
            rows = store.list_events(newest_first=False)

        self.assertEqual(1, len(rows))
        event = rows[0]
        self.assertEqual("live_trader", event["app"])
        self.assertEqual("ORDER", event["category"])
        self.assertEqual("주문 게이트", event["source"])
        self.assertEqual("allow", event["decision"])
        self.assertEqual("dry_run", event["state"])
        self.assertEqual("LIVE-DRY-0001", event["order_id"])
        self.assertEqual("LIVE-AUDIT", event["strategy_id"])
        self.assertEqual("BTCUSDT", event["symbol"])
        self.assertTrue(event["payload"]["risk_report"]["can_submit"])
        self.assertEqual("simulated", event["payload"]["queue_state"])

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

    def test_order_gate_adds_watchdog_blocker_to_risk_report(self) -> None:
        state.STATE["new_entries_blocked"] = False
        checks = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "watchdog": {"critical_count": 1, "next_actions": ["과도 주문 감시"]},
        }

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(
            checks,
            "BUY",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("Watchdog", reason)
        self.assertTrue(any(check.label == "Watchdog" and check.status == "fail" for check in report.blockers))

    def test_watchdog_fail_closed_blocks_entries_and_monitor_mode(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        state.STATE["automation"]["crypto"]["enabled"] = True
        state.STATE["automation"]["crypto"]["mode"] = "SMALL_LIVE"
        state.STATE["watchdog"]["settings"]["max_recent_orders_per_min"] = 1.0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.STATE["orders"] = [
            {
                "order_id": f"LIVE-DRY-{index:04d}",
                "created_at": now,
                "time": now,
                "state": "dry_run",
                "queue_state": "simulated",
                "attempts": 1,
            }
            for index in range(1, 4)
        ]
        state.STATE["audit"] = []

        result = state.run_watchdog(include_snapshot=False)

        self.assertFalse(result["ok"])
        self.assertEqual(state.STATE["mode"], "MONITOR")
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertFalse(state.STATE["automation"]["crypto"]["enabled"])
        self.assertEqual(state.STATE["automation"]["crypto"]["mode"], "MONITOR")
        self.assertEqual(state.STATE["watchdog"]["trip_count"], 1)
        self.assertEqual(state.STATE["audit"][-1]["event"], "Watchdog Fail Closed")

    def test_automation_start_is_blocked_by_watchdog_critical(self) -> None:
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "watchdog": {"critical_count": 1},
            "automation_profiles": [
                {"id": "crypto", "title": "코인 자동화", "live_strategy_count": 1}
            ],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.set_automation_profile("crypto", True, provider="binance", mode="SMALL_LIVE")

        self.assertFalse(result["ok"])
        self.assertIn("Watchdog critical", result["reason"])
        self.assertFalse(state.STATE["automation"]["crypto"]["enabled"])

    def test_reconciliation_refreshes_read_only_broker_snapshots(self) -> None:
        class FakeRouter:
            def get_account_snapshot(self, broker_id):
                rows = {
                    "kis": [
                        {
                            "broker_id": "kis",
                            "broker_name": "한국투자증권 Open API",
                            "account": "KIS 실계좌",
                            "currency": "KRW",
                            "broker_cash": 100000.0,
                            "detail": "fake kis account",
                        }
                    ],
                    "binance": [
                        {
                            "broker_id": "binance",
                            "broker_name": "Binance API",
                            "account": "Binance Spot",
                            "currency": "USDT",
                            "broker_cash": 25.0,
                            "detail": "fake binance account",
                        }
                    ],
                    "upbit": [
                        {
                            "broker_id": "upbit",
                            "broker_name": "Upbit API",
                            "account": "Upbit KRW",
                            "currency": "KRW",
                            "broker_cash": 50000.0,
                            "detail": "fake upbit account",
                        }
                    ],
                }
                return {"broker_id": broker_id, "accounts": rows[broker_id]}

            def list_positions(self, broker_id):
                rows = {
                    "kis": [
                        {
                            "symbol": "069500.KS",
                            "asset": "한국주식",
                            "broker_id": "kis",
                            "broker_name": "한국투자증권 Open API",
                            "currency": "KRW",
                            "broker_qty": 0.0,
                            "broker_value": 0.0,
                            "detail": "fake kis position",
                        }
                    ],
                    "binance": [
                        {
                            "symbol": "BTC",
                            "asset": "코인",
                            "broker_id": "binance",
                            "broker_name": "Binance API",
                            "currency": "BTC",
                            "broker_qty": 0.1,
                            "broker_value": 0.0,
                            "detail": "fake binance position",
                        }
                    ],
                    "upbit": [],
                }
                return rows[broker_id]

        with patch("live_trader.state.LiveBrokerRouter", return_value=FakeRouter()):
            result = state.run_reconciliation()

        self.assertTrue(result["ok"])
        broker_data = state.STATE["broker_reconciliation"]
        self.assertEqual(len(broker_data["accounts"]), 3)
        self.assertEqual(len(broker_data["positions"]), 2)
        self.assertEqual(len(broker_data["errors"]), 0)
        self.assertEqual(result["reconciliation"]["summary"]["error_count"], 0)
        self.assertGreaterEqual(result["reconciliation"]["summary"]["mismatch_count"], 1)
        self.assertTrue(
            any(
                row["broker_id"] == "kis"
                and row["currency"] == "KRW"
                and row["broker_cash"].startswith("100,000")
                and row["status"] == "api_required"
                for row in result["reconciliation"]["accounts"]
            )
        )
        self.assertTrue(
            any(
                row["broker_id"] == "binance" and row["symbol"] == "BTC" and row["status"] == "mismatch"
                for row in result["reconciliation"]["positions"]
            )
        )


if __name__ == "__main__":
    unittest.main()
