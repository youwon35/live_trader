from __future__ import annotations

import copy
import threading
import time
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader.brokers import BrokerNotReadyError


class FakeFuturesSettingsRouter:
    def __init__(self) -> None:
        self.margin_type = "CROSSED"
        self.leverage = 20
        self.position_count = 0
        self.open_order_count = 0
        self.configure_calls: list[dict[str, object]] = []
        self.fail_configure = False

    def get_binance_futures_canary_observation(
        self,
        symbol: str,
    ) -> dict[str, object]:
        return {
            "account": {
                "can_trade": True,
                "available_usdt": 10.0,
                "available_usdt_known": True,
            },
            "position_mode": {"dual_side_position": True},
            "symbol_config": {
                "symbol": symbol,
                "margin_type": self.margin_type,
                "leverage": self.leverage,
            },
            "position_count": self.position_count,
            "open_order_count": self.open_order_count,
        }

    def configure_binance_futures_symbol(
        self,
        symbol: str,
        *,
        margin_type: str,
        leverage: int,
        change_margin_type: bool,
        change_leverage: bool,
    ) -> dict[str, object]:
        self.configure_calls.append(
            {
                "symbol": symbol,
                "margin_type": margin_type,
                "leverage": leverage,
                "change_margin_type": change_margin_type,
                "change_leverage": change_leverage,
            }
        )
        if self.fail_configure:
            raise BrokerNotReadyError("ambiguous timeout")
        applied: list[str] = []
        if change_margin_type:
            self.margin_type = margin_type
            applied.append("margin_type")
        if change_leverage:
            self.leverage = leverage
            applied.append("leverage")
        return {"applied": applied}


class FuturesSettingsStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_factory = (
            state.BINANCE_FUTURES_SETTINGS_ROUTER_FACTORY
        )
        self.original_internal = copy.deepcopy(
            state.BINANCE_FUTURES_SETTINGS_INTERNAL
        )
        self.router = FakeFuturesSettingsRouter()
        state.BINANCE_FUTURES_SETTINGS_ROUTER_FACTORY = lambda: self.router
        state.BINANCE_FUTURES_SETTINGS_INTERNAL.update(
            {
                "status": "IDLE",
                "confirmation_token_hash": "",
                "confirmation_expires_epoch": 0.0,
                "confirmation_used": False,
                "preview": {},
                "result": {},
            }
        )

    def tearDown(self) -> None:
        state.BINANCE_FUTURES_SETTINGS_ROUTER_FACTORY = (
            self.original_factory
        )
        state.BINANCE_FUTURES_SETTINGS_INTERNAL.clear()
        state.BINANCE_FUTURES_SETTINGS_INTERNAL.update(
            self.original_internal
        )

    def test_preview_issues_one_time_token_without_public_exposure(
        self,
    ) -> None:
        response = state.preview_binance_futures_settings(
            "ETHUSDT",
            "ISOLATED",
            1,
        )

        self.assertTrue(response["ok"])
        token = response["authorization"]["confirmation_token"]
        self.assertGreater(len(token), 20)
        public = state.binance_futures_settings_status()
        self.assertEqual("READY", public["status"])
        self.assertNotIn(token, repr(public))
        self.assertNotIn("confirmation_token_hash", public)

    def test_apply_rechecks_and_verifies_target_settings(self) -> None:
        preview = state.preview_binance_futures_settings(
            "ETHUSDT",
            "ISOLATED",
            1,
        )

        result = state.apply_binance_futures_settings(
            preview["authorization"]["confirmation_token"],
            confirmed=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(1, len(self.router.configure_calls))
        self.assertEqual("ISOLATED", self.router.margin_type)
        self.assertEqual(1, self.router.leverage)
        self.assertEqual("APPLIED", result["settings"]["status"])
        self.assertTrue(result["settings"]["result"]["verified"])

    def test_new_position_after_preview_blocks_mutation(self) -> None:
        preview = state.preview_binance_futures_settings(
            "ETHUSDT",
            "ISOLATED",
            1,
        )
        self.router.position_count = 1

        result = state.apply_binance_futures_settings(
            preview["authorization"]["confirmation_token"],
            confirmed=True,
        )

        self.assertFalse(result["ok"])
        self.assertEqual([], self.router.configure_calls)
        self.assertEqual("FAILED", result["settings"]["status"])

    def test_unsafe_leverage_is_blocked_before_broker_query(self) -> None:
        result = state.preview_binance_futures_settings(
            "ETHUSDT",
            "ISOLATED",
            20,
        )

        self.assertFalse(result["ok"])
        self.assertEqual("BLOCKED", result["settings"]["status"])
        self.assertIn(
            "leverage-outside-safe-presets",
            result["settings"]["preview"]["blockers"],
        )

    def test_failed_mutation_is_not_automatically_retried(self) -> None:
        preview = state.preview_binance_futures_settings(
            "ETHUSDT",
            "ISOLATED",
            1,
        )
        self.router.fail_configure = True

        result = state.apply_binance_futures_settings(
            preview["authorization"]["confirmation_token"],
            confirmed=True,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(1, len(self.router.configure_calls))
        self.assertIn("자동 재시도하지 않았습니다", result["reason"])

    def test_functional_authority_blocks_preview_without_state_change(self) -> None:
        before = copy.deepcopy(state.BINANCE_FUTURES_SETTINGS_INTERNAL)
        with patch.object(
            state,
            "_binance_functional_durable_authority_open",
            return_value=True,
        ):
            result = state.preview_binance_futures_settings(
                "ETHUSDT", "ISOLATED", 1
            )
        self.assertFalse(result["ok"])
        self.assertEqual(before, state.BINANCE_FUTURES_SETTINGS_INTERNAL)
        self.assertEqual([], self.router.configure_calls)

    def test_functional_start_waits_for_inflight_settings_sender(self) -> None:
        preview = state.preview_binance_futures_settings(
            "ETHUSDT", "ISOLATED", 1
        )
        entered = threading.Event()
        release = threading.Event()
        start_finished = threading.Event()
        original_configure = self.router.configure_binance_futures_symbol

        def paused_configure(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(2))
            return original_configure(*args, **kwargs)

        self.router.configure_binance_futures_symbol = paused_configure

        def apply() -> None:
            state.apply_binance_futures_settings(
                preview["authorization"]["confirmation_token"],
                confirmed=True,
            )

        def functional_start_boundary() -> None:
            with state.binance_route_authority_serialization():
                start_finished.set()

        apply_thread = threading.Thread(target=apply)
        start_thread = threading.Thread(target=functional_start_boundary)
        apply_thread.start()
        self.assertTrue(entered.wait(1))
        start_thread.start()
        time.sleep(0.05)
        self.assertFalse(start_finished.is_set())
        release.set()
        apply_thread.join(2)
        start_thread.join(2)
        self.assertTrue(start_finished.is_set())
        self.assertEqual(1, len(self.router.configure_calls))


if __name__ == "__main__":
    unittest.main()
