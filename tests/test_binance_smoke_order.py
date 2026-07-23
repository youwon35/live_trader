from __future__ import annotations

import copy
import os
import unittest
from unittest.mock import patch

from live_trader import state


class BinanceSmokeOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        self.env = patch.dict(
            os.environ,
            {
                "BINANCE_API_KEY": "test-key",
                "BINANCE_API_SECRET": "test-secret",
                "BINANCE_BASE_URL": "https://binance.example.test",
                "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
            },
        )
        self.env.start()

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        self.env.stop()

    @staticmethod
    def strategy() -> dict[str, object]:
        return {
            "strategy_id": "btc-qualified",
            "symbol": "BTCUSDT",
            "provider": "binance",
            "live_small_eligible": True,
        }

    @staticmethod
    def account() -> dict[str, object]:
        return {
            "accounts": [
                {
                    "broker_id": "binance",
                    "currency": "USDT",
                    "broker_cash": 30.0,
                }
            ]
        }

    def preview(self) -> dict[str, object]:
        with patch.object(state, "strategy_rows", return_value=[self.strategy()]), patch.object(
            state.LiveBrokerRouter,
            "get_account_snapshot",
            return_value=self.account(),
        ), patch.object(state, "_binance_ticker_price", return_value=65_000.0), patch.object(
            state,
            "snapshot",
            return_value={},
        ):
            return state.preview_binance_smoke_order("btc-qualified")

    def test_preview_is_read_only_and_hard_capped(self) -> None:
        result = self.preview()

        self.assertTrue(result["ok"])
        preview = result["preview"]
        self.assertEqual("BTCUSDT", preview["symbol"])
        self.assertEqual(0.0001, preview["quantity"])
        self.assertEqual(6.5, preview["notional_usdt"])
        self.assertTrue(preview["confirmation_token"])
        self.assertFalse(preview["used"])
        self.assertEqual("MARKET", preview["order_type"])

    def test_submit_requires_real_small_live_and_consumes_token_once(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]

        state.STATE["new_entries_blocked"] = False
        blocked = state.submit_binance_smoke_order(token, confirmed=True)
        self.assertFalse(blocked["ok"])
        self.assertIn("SMALL_LIVE", blocked["reason"])

        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False
        acknowledged = {
            "ok": True,
            "reason": "broker-acknowledged",
            "order": {
                "order_id": "LIVE-ORDER-0001",
                "state": "acknowledged",
                "broker_order_id": "12345",
            },
            "snapshot": {},
        }
        with patch.dict(os.environ, {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}), patch.object(
            state,
            "_binance_ticker_price",
            return_value=65_100.0,
        ), patch.object(
            state.LiveBrokerRouter,
            "get_account_snapshot",
            return_value=self.account(),
        ), patch.object(
            state,
            "submit_order_intent",
            return_value=acknowledged,
        ) as submit:
            result = state.submit_binance_smoke_order(token, confirmed=True)

        self.assertTrue(result["ok"])
        intent = submit.call_args.args[1]
        self.assertEqual("btc-qualified", intent.strategy_id)
        self.assertEqual(0.0001, intent.quantity)
        self.assertEqual("BROKER_SMOKE", intent.metadata["order_purpose"])
        self.assertIn("전략 신호 아님", intent.reason)
        self.assertTrue(state.STATE["binance_smoke_order"]["used"])
        self.assertEqual("", state.STATE["binance_smoke_order"]["confirmation_token"])

        duplicate = state.submit_binance_smoke_order(token, confirmed=True)
        self.assertFalse(duplicate["ok"])


if __name__ == "__main__":
    unittest.main()
