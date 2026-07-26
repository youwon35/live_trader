from __future__ import annotations

import copy
import os
import unittest
from unittest.mock import patch

from live_trader import state


class UpbitSmokeOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        self.env = patch.dict(
            os.environ,
            {
                "UPBIT_ACCESS_KEY": "test-access",
                "UPBIT_SECRET_KEY": "test-secret",
                "UPBIT_BASE_URL": "https://upbit.example.test",
                "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
            },
        )
        self.env.start()
        self.strategy_rows = patch.object(state, "strategy_rows", return_value=[self.strategy()])
        self.strategy_rows.start()
        self.snapshot = patch.object(state, "snapshot", return_value={})
        self.snapshot.start()

    def tearDown(self) -> None:
        self.snapshot.stop()
        self.strategy_rows.stop()
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        self.env.stop()

    @staticmethod
    def account_snapshot(balance: float = 50_001.0) -> dict[str, object]:
        return {
            "broker_id": "upbit",
            "accounts": [{"broker_id": "upbit", "broker_cash": balance}],
        }

    @staticmethod
    def order_chance(balance: float = 50_001.0) -> dict[str, object]:
        return {
            "bid_fee": "0.0005",
            "market": {"bid": {"min_total": "5000", "max_total": "1000000000"}},
            "bid_account": {"balance": str(balance), "currency": "KRW"},
        }

    @staticmethod
    def strategy() -> dict[str, object]:
        return {
            "strategy_id": "upbit-btc-qualified",
            "symbol": "KRW-BTC",
            "asset": "CRYPTO",
            "dataset_provider": "upbit",
            "broker_id": "upbit",
            "lifecycle_status": "before-live-small",
            "live_small_eligible": True,
        }

    def preview(self) -> dict[str, object]:
        with patch.object(state.LiveBrokerRouter, "get_account_snapshot", return_value=self.account_snapshot()), patch.object(
            state.LiveBrokerRouter,
            "get_upbit_order_chance",
            return_value=self.order_chance(),
        ):
            return state.preview_upbit_smoke_order("upbit-btc-qualified", 5000)

    def test_preview_is_read_only_and_builds_exact_market_buy(self) -> None:
        result = self.preview()

        self.assertTrue(result["ok"])
        preview = result["preview"]
        self.assertEqual(preview["market"], "KRW-BTC")
        self.assertEqual(preview["strategy_id"], "upbit-btc-qualified")
        self.assertEqual(preview["notional_krw"], 5000)
        self.assertEqual(preview["status"], "ready")
        self.assertTrue(preview["confirmation_token"])
        self.assertFalse(preview["used"])
        request = preview["request_preview"]
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["body"]["side"], "bid")
        self.assertEqual(request["body"]["ord_type"], "price")
        self.assertEqual(request["body"]["price"], "5000")
        self.assertNotIn("volume", request["body"])
        self.assertTrue(request["body"]["identifier"].startswith("lt-smoke-"))

    def test_preview_hard_blocks_more_than_user_cap(self) -> None:
        result = state.preview_upbit_smoke_order("upbit-btc-qualified", 10_001)

        self.assertFalse(result["ok"])
        self.assertIn("10,000", result["reason"])
        self.assertFalse(state.STATE["upbit_smoke_order"]["confirmation_token"])

    def test_preview_requires_approved_upbit_krw_btc_strategy(self) -> None:
        with patch.object(state, "strategy_rows", return_value=[]), patch.object(state, "snapshot", return_value={}):
            result = state.preview_upbit_smoke_order("missing", 5000)

        self.assertFalse(result["ok"])
        self.assertIn("before-live-small", result["reason"])

    def test_submit_requires_action_time_confirmation_and_all_live_gates(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]

        without_confirmation = state.submit_upbit_smoke_order(token, confirmed=False)
        wrong_token = state.submit_upbit_smoke_order("wrong", confirmed=True)

        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = True
        entry_blocked = state.submit_upbit_smoke_order(token, confirmed=True)

        state.STATE["new_entries_blocked"] = False
        state.STATE["dry_run"] = True
        dry_run_blocked = state.submit_upbit_smoke_order(token, confirmed=True)

        state.STATE["dry_run"] = False
        state.STATE["mode"] = "MONITOR"
        mode_blocked = state.submit_upbit_smoke_order(token, confirmed=True)

        state.STATE["mode"] = "SMALL_LIVE"
        flag_off = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertFalse(without_confirmation["ok"])
        self.assertFalse(wrong_token["ok"])
        self.assertFalse(entry_blocked["ok"])
        self.assertIn("신규 진입", entry_blocked["reason"])
        self.assertFalse(dry_run_blocked["ok"])
        self.assertIn("Dry Run", dry_run_blocked["reason"])
        self.assertFalse(mode_blocked["ok"])
        self.assertIn("SMALL_LIVE", mode_blocked["reason"])
        self.assertFalse(flag_off["ok"])
        self.assertIn("LIVE_TRADER_ENABLE_REAL_ORDERS", flag_off["reason"])
        self.assertFalse(state.STATE["upbit_smoke_order"]["used"])

    def test_submit_revalidates_strategy_approval(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False

        with patch.dict(os.environ, {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}), patch.object(
            state,
            "strategy_rows",
            return_value=[],
        ), patch.object(
            state.LiveBrokerRouter,
            "place_order",
        ) as place_order, patch.object(
            state,
            "snapshot",
            return_value={},
        ):
            result = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertFalse(result["ok"])
        self.assertIn("승인이 더 이상 유효하지 않습니다", result["reason"])
        place_order.assert_not_called()
        self.assertFalse(state.STATE["upbit_smoke_order"]["used"])

    def test_successful_submit_consumes_token_once_and_records_fill(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False
        order_payload = {
            "uuid": "upbit-order-1",
            "state": "done",
            "paid_fee": "2.5",
            "trades": [{"volume": "0.00004", "funds": "5000"}],
        }
        with patch.dict(os.environ, {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}), patch.object(
            state.LiveBrokerRouter,
            "get_account_snapshot",
            return_value=self.account_snapshot(),
        ), patch.object(
            state.LiveBrokerRouter,
            "place_order",
            return_value={"ok": True, "statusCode": 201, "json": {"uuid": "upbit-order-1", "state": "wait"}},
        ) as place_order, patch.object(
            state.LiveBrokerRouter,
            "get_upbit_order",
            return_value=order_payload,
        ), patch.object(state, "poll_execution_events", return_value={"ok": True}), patch.object(
            state,
            "run_reconciliation",
            return_value={"ok": True},
        ):
            result = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["order"]["status"], "filled")
        self.assertEqual(result["order"]["broker_order_id"], "upbit-order-1")
        self.assertEqual(result["order"]["executed_funds"], 5000.0)
        self.assertTrue(result["order"]["used"])
        self.assertEqual(result["order"]["confirmation_token"], "")
        sent = place_order.call_args.args[0]
        self.assertEqual(sent["market"], "KRW-BTC")
        self.assertEqual(sent["strategy_id"], "upbit-btc-qualified")
        self.assertEqual(sent["order_type"], "price")
        self.assertEqual(sent["notional"], 5000)

        duplicate = state.submit_upbit_smoke_order(token, confirmed=True)
        self.assertFalse(duplicate["ok"])

    def test_market_buy_cancel_with_trades_is_reported_as_filled_remainder_cancelled(self) -> None:
        state.STATE["upbit_smoke_order"] = {
            "status": "acknowledged",
            "broker_order_id": "upbit-order-cancelled-remainder",
        }
        order_payload = {
            "uuid": "upbit-order-cancelled-remainder",
            "state": "cancel",
            "paid_fee": "2.5",
            "trades": [{"volume": "0.00004", "funds": "4999.104"}],
        }
        with patch.object(state.LiveBrokerRouter, "get_upbit_order", return_value=order_payload), patch.object(
            state,
            "poll_execution_events",
            return_value={"ok": True},
        ), patch.object(state, "run_reconciliation", return_value={"ok": True}):
            result = state.refresh_upbit_smoke_order()

        self.assertTrue(result["ok"])
        self.assertEqual(result["order"]["status"], "filled")
        self.assertEqual(result["order"]["status_label"], "체결·잔여취소")
        self.assertEqual(result["order"]["executed_funds"], 4999.104)


if __name__ == "__main__":
    unittest.main()
