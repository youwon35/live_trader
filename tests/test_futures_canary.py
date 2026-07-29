from __future__ import annotations

import copy
import json
import os
import unittest
from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import patch

from live_trader import state
from live_trader.futures_canary import (
    build_futures_canary_test_intents,
    derive_canary_quantity,
    evaluate_futures_canary_preflight,
)
from live_trader.live_adapters import build_binance_futures_order_request


class FuturesCanaryTests(unittest.TestCase):
    class FakeRouter:
        def __init__(
            self,
            *,
            observations: list[dict[str, object]] | None = None,
            responses: list[dict[str, object]] | None = None,
        ) -> None:
            self.observations = observations or [
                FuturesCanaryTests.ready_observation()
            ]
            self.responses = list(
                responses
                or [
                    {
                        "ok": True,
                        "statusCode": 200,
                        "json": {},
                        "text": "",
                    },
                    {
                        "ok": True,
                        "statusCode": 200,
                        "json": {},
                        "text": "",
                    },
                ]
            )
            self.observation_calls = 0
            self.test_calls: list[dict[str, object]] = []

        def get_binance_futures_canary_observation(
            self,
            _symbol: str,
        ) -> dict[str, object]:
            index = min(
                self.observation_calls,
                len(self.observations) - 1,
            )
            self.observation_calls += 1
            return copy.deepcopy(self.observations[index])

        def test_binance_futures_order(
            self,
            intent: dict[str, object],
        ) -> dict[str, object]:
            self.test_calls.append(copy.deepcopy(intent))
            return self.responses.pop(0)

    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))

    @staticmethod
    def quantity(target: object = 6) -> dict[str, object]:
        return derive_canary_quantity(
            target_notional_usdt=target,
            price=60_000,
            min_qty="0.0001",
            max_qty="100",
            step_size="0.0001",
            exchange_min_notional=5,
        )

    @staticmethod
    def strategy_gate() -> dict[str, object]:
        return {
            "found": True,
            "broker_id": "binance-futures",
            "symbol": "BTCUSDT",
            "symbol_matches": True,
            "market_type": "futures",
            "position_direction": "short",
            "short_authorized": True,
            "lifecycle_status": "before-live-small",
            "live_small_eligible": True,
            "deployment_provenance_ok": True,
            "canary_scope_ok": True,
        }

    @staticmethod
    def ready_strategy() -> dict[str, object]:
        return {
            "id": "btc-short",
            "strategy_id": "btc-short",
            "symbol": "BTCUSDT",
            "broker_id": "binance-futures",
            "marketType": "futures",
            "status": "before-live-small",
            "live_small_eligible": True,
            "parameters": {
                "customStrategyDefinition": {
                    "positionDirection": "short",
                }
            },
        }

    @staticmethod
    def ready_scope() -> dict[str, object]:
        return {
            "eligible": True,
            "deploymentId": "dep-1",
            "strategyArtifactHash": "artifact-hash",
            "strategyId": "btc-short",
            "scopeId": "scope-1",
        }

    @staticmethod
    def ready_observation() -> dict[str, object]:
        return {
            "account": {
                "can_trade": True,
                "available_usdt": 50.0,
                "available_usdt_known": True,
            },
            "position_mode": {"dual_side_position": True},
            "symbol_config": {
                "symbol": "BTCUSDT",
                "margin_type": "ISOLATED",
                "leverage": 1,
            },
            "position_count": 0,
            "open_order_count": 0,
        }

    @contextmanager
    def ready_environment(
        self,
        router: "FuturesCanaryTests.FakeRouter",
    ):
        with (
            patch.object(
                state,
                "strategy_rows",
                return_value=[self.ready_strategy()],
            ),
            patch.object(
                state,
                "current_live_canary_scope",
                return_value=self.ready_scope(),
            ),
            patch.object(state, "LiveBrokerRouter", return_value=router),
            patch.object(
                state,
                "_binance_futures_ticker_price",
                return_value=60_000,
            ),
            patch.object(
                state,
                "binance_symbol_rules",
                return_value={
                    "minQty": Decimal("0.0001"),
                    "maxQty": Decimal("100"),
                    "stepSize": Decimal("0.0001"),
                    "minNotional": Decimal("5"),
                },
            ),
            patch.object(state, "real_orders_enabled", return_value=False),
            patch.object(state, "append_audit"),
        ):
            yield

    def test_quantity_rounds_up_and_hard_caps_notional(self) -> None:
        valid = self.quantity(5)
        below = self.quantity("4.999")
        above = self.quantity("10.001")
        step_over_cap = derive_canary_quantity(
            target_notional_usdt=10,
            price=60_001,
            min_qty="0.0001",
            max_qty="100",
            step_size="0.0001",
            exchange_min_notional=5,
        )

        self.assertTrue(valid["ok"])
        self.assertEqual("0.0001", valid["quantity"])
        self.assertIn("notional-below-hard-min", below["blockers"])
        self.assertIn("notional-above-hard-max", above["blockers"])
        self.assertIn(
            "estimated-notional-above-hard-max",
            step_over_cap["blockers"],
        )

    def test_current_account_observation_fails_closed_but_hedge_passes(self) -> None:
        result = evaluate_futures_canary_preflight(
            strategy_gate=self.strategy_gate(),
            account={
                "can_trade": False,
                "available_usdt": "0",
                "available_usdt_known": True,
            },
            position_mode={"dual_side_position": True},
            symbol_config={
                "symbol": "BTCUSDT",
                "margin_type": "CROSSED",
                "leverage": 20,
            },
            position_count=0,
            open_order_count=0,
            requested_notional_usdt=6,
            quantity_result=self.quantity(),
            real_orders_enabled=False,
        )

        self.assertFalse(result["ready_for_test"])
        self.assertIn("account-can-trade-false", result["test_blockers"])
        self.assertIn("available-usdt-insufficient", result["test_blockers"])
        self.assertIn("margin-type-not-isolated", result["test_blockers"])
        self.assertIn("leverage-not-1x", result["test_blockers"])
        self.assertIn("real-orders-disabled", result["start_blockers"])
        hedge = next(
            item
            for item in result["checks"]
            if item["id"] == "position-mode-hedge"
        )
        self.assertEqual("pass", hedge["status"])

    def test_missing_observations_are_unknown_failures(self) -> None:
        result = evaluate_futures_canary_preflight(
            strategy_gate=self.strategy_gate(),
            account={},
            position_mode={},
            symbol_config={},
            position_count=None,
            open_order_count=None,
            requested_notional_usdt=6,
            quantity_result=self.quantity(),
            real_orders_enabled=True,
        )

        for reason in (
            "account-can-trade-unknown",
            "available-usdt-unknown",
            "position-mode-unknown",
            "symbol-config-missing",
            "positions-observation-failed",
            "open-orders-observation-failed",
        ):
            self.assertIn(reason, result["test_blockers"])

    def test_entry_and_cover_use_short_hedge_payload_without_reduce_only_query(
        self,
    ) -> None:
        entry, cover = build_futures_canary_test_intents(
            strategy_id="short-btc",
            symbol="BTCUSDT.PERP",
            quantity="0.0001",
            token_fingerprint="ABCDEF1234567890",
        )
        with patch.dict(
            os.environ,
            {
                "BINANCE_API_KEY": "test-key",
                "BINANCE_API_SECRET": "test-secret",
            },
        ):
            entry_request = build_binance_futures_order_request(
                entry,
                hedge_mode=True,
                test=True,
            )
            cover_request = build_binance_futures_order_request(
                cover,
                hedge_mode=True,
                test=True,
            )

        self.assertEqual("SELL", entry_request.query["side"])
        self.assertEqual("BUY", cover_request.query["side"])
        self.assertEqual("SHORT", entry_request.query["positionSide"])
        self.assertEqual("SHORT", cover_request.query["positionSide"])
        self.assertEqual(
            entry_request.query["quantity"],
            cover_request.query["quantity"],
        )
        self.assertNotIn("reduceOnly", cover_request.query)
        self.assertEqual(
            "***",
            cover_request.preview()["query"]["signature"],
        )

    def test_state_preview_exposes_current_blockers_without_secrets(self) -> None:
        strategy = {
            "id": "btc-short",
            "strategy_id": "btc-short",
            "symbol": "BTCUSDT",
            "broker_id": "binance-futures",
            "marketType": "futures",
            "status": "before-live-small",
            "live_small_eligible": True,
            "parameters": {
                "customStrategyDefinition": {
                    "positionDirection": "short",
                }
            },
        }

        class FakeRouter:
            def get_binance_futures_canary_observation(self, _symbol):
                return {
                    "account": {
                        "can_trade": False,
                        "available_usdt": 0.0,
                        "available_usdt_known": True,
                    },
                    "position_mode": {"dual_side_position": True},
                    "symbol_config": {
                        "symbol": "BTCUSDT",
                        "margin_type": "CROSSED",
                        "leverage": 20,
                    },
                    "position_count": 0,
                    "open_order_count": 0,
                }

        with (
            patch.object(state, "strategy_rows", return_value=[strategy]),
            patch.object(
                state,
                "current_live_canary_scope",
                return_value={
                    "eligible": True,
                    "deploymentId": "dep-1",
                    "artifactHash": "artifact-hash",
                    "strategyId": "btc-short",
                },
            ),
            patch.object(state, "LiveBrokerRouter", return_value=FakeRouter()),
            patch.object(
                state,
                "_binance_futures_ticker_price",
                return_value=60_000,
            ),
            patch.object(
                state,
                "binance_symbol_rules",
                return_value={
                    "minQty": Decimal("0.0001"),
                    "maxQty": Decimal("100"),
                    "stepSize": Decimal("0.0001"),
                    "minNotional": Decimal("5"),
                },
            ),
            patch.object(state, "real_orders_enabled", return_value=False),
            patch.object(state, "append_audit"),
        ):
            result = state.preview_binance_futures_canary(
                "btc-short",
                6,
            )

        self.assertFalse(result["ok"])
        blockers = result["canary"]["test_blockers"]
        self.assertIn("account-can-trade-false", blockers)
        self.assertIn("available-usdt-insufficient", blockers)
        self.assertIn("margin-type-not-isolated", blockers)
        self.assertIn("leverage-not-1x", blockers)
        self.assertIn(
            "real-orders-disabled",
            result["canary"]["start_blockers"],
        )
        serialized = str(result).lower()
        self.assertNotIn("signature", serialized)
        self.assertNotIn("https://", serialized)

    def test_ready_preview_issues_one_time_token_only_in_direct_response(
        self,
    ) -> None:
        router = self.FakeRouter()
        with self.ready_environment(router):
            result = state.preview_binance_futures_canary(
                "btc-short",
                6,
            )
            status = state.binance_futures_canary_status()

        self.assertTrue(result["ok"])
        authorization = result["test_authorization"]
        self.assertTrue(authorization["confirmation_token"])
        self.assertTrue(authorization["expires_at"])
        self.assertEqual("test_ready", status["status"])
        self.assertNotIn("confirmation_token", status)
        self.assertNotIn("test_context", status)
        self.assertFalse(status["used"])
        self.assertLessEqual(
            state.STATE["binance_futures_canary"]["expires_epoch"],
            state.datetime.now(state.timezone.utc).timestamp() + 301,
        )

    def test_test_order_requires_confirmation_and_rejects_expired_token(
        self,
    ) -> None:
        router = self.FakeRouter()
        with self.ready_environment(router):
            preview = state.preview_binance_futures_canary(
                "btc-short",
                6,
            )
            token = preview["test_authorization"][
                "confirmation_token"
            ]
            missing_confirmation = (
                state.test_binance_futures_canary_order(
                    token,
                    confirmed=False,
                )
            )
            wrong_token = state.test_binance_futures_canary_order(
                "wrong",
                confirmed=True,
            )
            state.STATE["binance_futures_canary"][
                "expires_epoch"
            ] = 0
            expired = state.test_binance_futures_canary_order(
                token,
                confirmed=True,
            )

        self.assertFalse(missing_confirmation["ok"])
        self.assertFalse(wrong_token["ok"])
        self.assertFalse(expired["ok"])
        self.assertEqual([], router.test_calls)
        self.assertTrue(
            state.STATE["binance_futures_canary"]["used"]
        )
        self.assertEqual(
            "",
            state.STATE["binance_futures_canary"][
                "confirmation_token"
            ],
        )

    def test_action_time_preflight_change_consumes_token_without_test_post(
        self,
    ) -> None:
        changed = self.ready_observation()
        changed["position_count"] = 1
        router = self.FakeRouter(
            observations=[self.ready_observation(), changed]
        )
        with self.ready_environment(router):
            preview = state.preview_binance_futures_canary(
                "btc-short",
                6,
            )
            result = state.test_binance_futures_canary_order(
                preview["test_authorization"]["confirmation_token"],
                confirmed=True,
            )

        self.assertFalse(result["ok"])
        self.assertEqual([], router.test_calls)
        self.assertEqual(
            "action-time-preflight-blocked",
            result["test"]["reason_id"],
        )
        self.assertNotIn(
            "confirmation_token",
            result["canary"],
        )

    def test_test_order_validates_sell_then_buy_without_live_side_effects(
        self,
    ) -> None:
        router = self.FakeRouter(
            responses=[
                {
                    "ok": True,
                    "statusCode": 200,
                    "json": {
                        "ignored": "signed-material-must-not-return"
                    },
                    "text": (
                        "https://futures.example.test?"
                        "signature=must-not-return"
                    ),
                },
                {
                    "ok": True,
                    "statusCode": 200,
                    "json": {},
                    "text": "",
                },
            ]
        )
        orders_before = copy.deepcopy(state.STATE["orders"])
        with (
            self.ready_environment(router),
            patch.object(state, "submit_order_intent") as submit,
            patch.object(state.LIVE_OMS, "create") as oms_create,
            patch.object(
                state.PROGRAM_LEDGER,
                "record_execution_events",
            ) as ledger_write,
        ):
            preview = state.preview_binance_futures_canary(
                "btc-short",
                6,
            )
            result = state.test_binance_futures_canary_order(
                preview["test_authorization"]["confirmation_token"],
                confirmed=True,
            )
            duplicate = state.test_binance_futures_canary_order(
                preview["test_authorization"]["confirmation_token"],
                confirmed=True,
            )

        self.assertTrue(result["ok"])
        self.assertFalse(duplicate["ok"])
        self.assertEqual(["SELL", "BUY"], [
            item["side"] for item in router.test_calls
        ])
        self.assertEqual(
            ["short", "short"],
            [
                item["position_direction"]
                for item in router.test_calls
            ],
        )
        self.assertEqual(
            router.test_calls[0]["quantity"],
            router.test_calls[1]["quantity"],
        )
        self.assertFalse(router.test_calls[0]["risk_reducing"])
        self.assertTrue(router.test_calls[1]["risk_reducing"])
        self.assertEqual(orders_before, state.STATE["orders"])
        submit.assert_not_called()
        oms_create.assert_not_called()
        ledger_write.assert_not_called()
        self.assertFalse(
            result["test"]["submitted_to_matching_engine"]
        )
        self.assertEqual(
            "/fapi/v1/order/test",
            result["test"]["endpoint"],
        )
        serialized = json.dumps(result, ensure_ascii=False).lower()
        for fragment in (
            "must-not-return",
            "signature",
            "https://",
            "confirmation_token",
        ):
            self.assertNotIn(fragment, serialized)

    def test_first_test_leg_failure_stops_before_cover(self) -> None:
        router = self.FakeRouter(
            responses=[
                {
                    "ok": False,
                    "statusCode": 400,
                    "json": {
                        "code": -1102,
                        "msg": "Mandatory parameter missing.",
                    },
                    "text": "raw response is not returned",
                }
            ]
        )
        with self.ready_environment(router):
            preview = state.preview_binance_futures_canary(
                "btc-short",
                6,
            )
            result = state.test_binance_futures_canary_order(
                preview["test_authorization"]["confirmation_token"],
                confirmed=True,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(1, len(router.test_calls))
        self.assertEqual("SELL", router.test_calls[0]["side"])
        self.assertEqual(1, len(result["test"]["legs"]))
        self.assertEqual(
            "-1102",
            result["test"]["legs"][0]["exchange_code"],
        )

    def test_cover_test_leg_failure_is_reported_without_live_side_effects(
        self,
    ) -> None:
        router = self.FakeRouter(
            responses=[
                {
                    "ok": True,
                    "statusCode": 200,
                    "json": {},
                    "text": "",
                },
                {
                    "ok": False,
                    "statusCode": 400,
                    "json": {
                        "code": -2022,
                        "msg": "ReduceOnly Order is rejected.",
                    },
                    "text": "raw response is not returned",
                },
            ]
        )
        orders_before = copy.deepcopy(state.STATE["orders"])
        with (
            self.ready_environment(router),
            patch.object(state, "submit_order_intent") as submit,
            patch.object(state.LIVE_OMS, "create") as oms_create,
            patch.object(
                state.PROGRAM_LEDGER,
                "record_execution_events",
            ) as ledger_write,
        ):
            preview = state.preview_binance_futures_canary(
                "btc-short",
                6,
            )
            result = state.test_binance_futures_canary_order(
                preview["test_authorization"]["confirmation_token"],
                confirmed=True,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(["SELL", "BUY"], [
            item["side"] for item in router.test_calls
        ])
        self.assertEqual(2, len(result["test"]["legs"]))
        self.assertTrue(result["test"]["legs"][0]["ok"])
        self.assertFalse(result["test"]["legs"][1]["ok"])
        self.assertEqual(
            "-2022",
            result["test"]["legs"][1]["exchange_code"],
        )
        self.assertEqual(orders_before, state.STATE["orders"])
        submit.assert_not_called()
        oms_create.assert_not_called()
        ledger_write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
