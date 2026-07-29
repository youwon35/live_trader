from __future__ import annotations

import os
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from live_trader.brokers import (
    BrokerNotReadyError,
    LiveBrokerRouter,
    normalize_binance_futures_symbol_config,
    parse_binance_futures_accounts,
    parse_binance_futures_positions,
    validate_binance_futures_execution_policy,
)
from live_trader.execution_streams import (
    parse_binance_futures_order_update,
)
from live_trader.live_adapters import (
    BINANCE_FUTURES_ACCOUNT_CONFIG_ENDPOINT,
    BINANCE_FUTURES_OPEN_ORDERS_ENDPOINT,
    BINANCE_FUTURES_ORDER_ENDPOINT,
    BINANCE_FUTURES_POSITION_MODE_ENDPOINT,
    BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT,
    BINANCE_FUTURES_TEST_ORDER_ENDPOINT,
    build_binance_futures_account_config_request,
    build_binance_futures_open_orders_request,
    build_binance_futures_order_request,
    build_binance_futures_order_status_request,
    normalize_binance_futures_intent,
)


class BinanceFuturesAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env = patch.dict(
            os.environ,
            {
                "BINANCE_API_KEY": "futures-key",
                "BINANCE_API_SECRET": "futures-secret",
                "BINANCE_FUTURES_BASE_URL": "https://futures.example.test",
            },
            clear=False,
        )
        self.env.start()

    def tearDown(self) -> None:
        self.env.stop()

    @staticmethod
    def rules() -> dict[str, Decimal]:
        return {
            "minQty": Decimal("0.001"),
            "maxQty": Decimal("1000"),
            "stepSize": Decimal("0.001"),
            "minNotional": Decimal("5"),
        }

    def test_short_entry_and_cover_follow_one_way_reduce_only_rules(
        self,
    ) -> None:
        with patch(
            "live_trader.live_adapters.binance_symbol_rules",
            return_value=self.rules(),
        ):
            entry = normalize_binance_futures_intent(
                {
                    "symbol": "BTCUSDT.PERP",
                    "side": "SELL",
                    "quantity": 0.0019,
                    "price": 65_000,
                    "position_direction": "short",
                }
            )
            cover = normalize_binance_futures_intent(
                {
                    **entry,
                    "side": "BUY",
                    "risk_reducing": True,
                }
            )

        entry_request = build_binance_futures_order_request(
            entry,
            hedge_mode=False,
        )
        cover_request = build_binance_futures_order_request(
            cover,
            hedge_mode=False,
        )

        self.assertEqual("0.001", entry["quantity"])
        self.assertEqual(BINANCE_FUTURES_ORDER_ENDPOINT, entry_request.endpoint)
        self.assertEqual("BOTH", entry_request.query["positionSide"])
        self.assertNotIn("reduceOnly", entry_request.query)
        self.assertEqual("true", cover_request.query["reduceOnly"])

    def test_account_config_request_uses_current_can_trade_endpoint(
        self,
    ) -> None:
        request = build_binance_futures_account_config_request()

        self.assertEqual("GET", request.method)
        self.assertEqual(
            BINANCE_FUTURES_ACCOUNT_CONFIG_ENDPOINT,
            request.endpoint,
        )
        self.assertEqual([], request.blocked_reasons)
        self.assertIn("timestamp", request.query)
        self.assertIn("signature", request.query)

    def test_hedge_mode_uses_short_position_side_without_reduce_only(
        self,
    ) -> None:
        request = build_binance_futures_order_request(
            {
                "symbol": "ETHUSDT",
                "side": "BUY",
                "quantity": "0.01",
                "position_direction": "short",
                "risk_reducing": True,
            },
            hedge_mode=True,
            test=True,
        )

        self.assertEqual(
            BINANCE_FUTURES_TEST_ORDER_ENDPOINT,
            request.endpoint,
        )
        self.assertEqual("SHORT", request.query["positionSide"])
        self.assertNotIn("reduceOnly", request.query)

    def test_signed_short_position_keeps_negative_quantity(self) -> None:
        positions = parse_binance_futures_positions(
            [
                {
                    "symbol": "BTCUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "0.02",
                    "entryPrice": "65000",
                    "markPrice": "64000",
                    "notional": "-1280",
                }
            ]
        )

        self.assertEqual(-0.02, positions[0]["broker_qty"])
        self.assertEqual(1280.0, positions[0]["broker_value"])
        self.assertEqual("binance-futures", positions[0]["broker_id"])
        self.assertEqual("SHORT", positions[0]["position_side"])
        self.assertEqual("SHORT", positions[0]["positionSide"])

    def test_hedge_mode_long_and_short_legs_keep_distinct_sides(self) -> None:
        positions = parse_binance_futures_positions(
            [
                {
                    "symbol": "BTCUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0.03",
                    "entryPrice": "64000",
                    "markPrice": "65000",
                },
                {
                    "symbol": "BTCUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "0.02",
                    "entryPrice": "66000",
                    "markPrice": "65000",
                },
            ]
        )

        self.assertEqual(2, len(positions))
        self.assertEqual(["LONG", "SHORT"], [row["position_side"] for row in positions])
        self.assertEqual([0.03, -0.02], [row["broker_qty"] for row in positions])

    def test_account_and_execution_update_are_normalized(self) -> None:
        accounts = parse_binance_futures_accounts(
            {
                "availableBalance": "91.5",
                "totalWalletBalance": "100",
                "assets": [
                    {
                        "asset": "USDT",
                        "availableBalance": "91.5",
                        "walletBalance": "100",
                        "marginBalance": "99",
                    }
                ],
            }
        )
        event = parse_binance_futures_order_update(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1_700_000_000_000,
                "o": {
                    "s": "BTCUSDT",
                    "c": "client-1",
                    "S": "SELL",
                    "ps": "SHORT",
                    "x": "TRADE",
                    "X": "FILLED",
                    "i": 123,
                    "l": "0.001",
                    "L": "65000",
                    "t": 99,
                },
            }
        )

        self.assertEqual(91.5, accounts[0]["broker_cash"])
        self.assertIsNotNone(event)
        self.assertEqual("filled", event["state"])
        self.assertEqual("SHORT", event["position_side"])

    def test_new_entry_blocks_when_account_policy_is_riskier(self) -> None:
        config = normalize_binance_futures_symbol_config(
            [
                {
                    "symbol": "BTCUSDT",
                    "marginType": "CROSSED",
                    "leverage": 20,
                }
            ],
            "BTCUSDT",
        )
        with self.assertRaises(BrokerNotReadyError):
            validate_binance_futures_execution_policy(
                {
                    "max_leverage": 1,
                    "required_margin_type": "ISOLATED",
                },
                config,
            )

        validate_binance_futures_execution_policy(
            {
                "risk_reducing": True,
                "max_leverage": 1,
                "required_margin_type": "ISOLATED",
            },
            config,
        )

    def test_open_orders_and_order_status_are_signed_get_requests(self) -> None:
        open_orders = build_binance_futures_open_orders_request(
            "BTCUSDT.PERP"
        )
        status = build_binance_futures_order_status_request(
            "BTCUSDT.PERP",
            "12345",
        )

        self.assertEqual("GET", open_orders.method)
        self.assertEqual(
            BINANCE_FUTURES_OPEN_ORDERS_ENDPOINT,
            open_orders.endpoint,
        )
        self.assertEqual("BTCUSDT", open_orders.query["symbol"])
        self.assertEqual("GET", status.method)
        self.assertEqual(
            BINANCE_FUTURES_ORDER_ENDPOINT,
            status.endpoint,
        )
        self.assertEqual("12345", status.query["orderId"])
        self.assertEqual("***", open_orders.preview()["query"]["signature"])
        self.assertEqual("***", status.preview()["query"]["signature"])

    def test_router_exposes_futures_open_orders_and_order_status(self) -> None:
        responses = [
            {
                "ok": True,
                "status": 200,
                "json": [
                    {
                        "symbol": "BTCUSDT",
                        "orderId": 12345,
                        "status": "NEW",
                    }
                ],
                "text": "",
            },
            {
                "ok": True,
                "status": 200,
                "json": {
                    "symbol": "BTCUSDT",
                    "orderId": 12345,
                    "status": "FILLED",
                },
                "text": "",
            },
        ]
        with patch(
            "live_trader.brokers.send_binance_signed_request",
            side_effect=responses,
        ):
            router = LiveBrokerRouter()
            open_orders = router.list_open_orders(
                "binance-futures",
                symbol="BTCUSDT.PERP",
            )
            status = router.get_order_status(
                "binance-futures",
                symbol="BTCUSDT.PERP",
                broker_order_id="12345",
            )

        self.assertEqual(12345, open_orders[0]["orderId"])
        self.assertEqual("FILLED", status["status"])

    def test_canary_observation_returns_only_sanitized_fresh_account_facts(
        self,
    ) -> None:
        responses = [
            {
                "ok": True,
                "status": 200,
                "json": {
                    "assets": [
                        {
                            "asset": "USDT",
                            "availableBalance": "0",
                            "walletBalance": "23",
                        }
                    ],
                },
                "text": "",
            },
            {
                "ok": True,
                "status": 200,
                "json": {
                    "canTrade": False,
                    "dualSidePosition": True,
                },
                "text": "",
            },
            {
                "ok": True,
                "status": 200,
                "json": {"dualSidePosition": True},
                "text": "",
            },
            {
                "ok": True,
                "status": 200,
                "json": [
                    {
                        "symbol": "BTCUSDT",
                        "positionAmt": "0",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "positionAmt": "0",
                    },
                ],
                "text": "",
            },
            {
                "ok": True,
                "status": 200,
                "json": [
                    {
                        "symbol": "BTCUSDT",
                        "marginType": "cross",
                        "leverage": "20",
                    }
                ],
                "text": "",
            },
            {
                "ok": True,
                "status": 200,
                "json": [],
                "text": "",
            },
        ]
        with patch(
            "live_trader.brokers.send_binance_signed_request",
            side_effect=responses,
        ):
            observation = (
                LiveBrokerRouter()
                .get_binance_futures_canary_observation("BTCUSDT.PERP")
            )

        self.assertEqual(
            {
                "can_trade": False,
                "available_usdt": 0.0,
                "available_usdt_known": True,
            },
            observation["account"],
        )
        self.assertEqual(
            {"dual_side_position": True},
            observation["position_mode"],
        )
        self.assertEqual(0, observation["position_count"])
        self.assertEqual(0, observation["open_order_count"])
        self.assertEqual(
            {
                "symbol": "BTCUSDT",
                "margin_type": "CROSS",
                "leverage": 20.0,
                "max_notional": 0.0,
            },
            observation["symbol_config"],
        )
        serialized = str(observation).lower()
        for secret_fragment in (
            "walletbalance",
            "signature",
            "api-key",
            "https://",
        ):
            self.assertNotIn(secret_fragment, serialized)

    def test_router_test_order_is_pinned_to_non_matching_endpoint(
        self,
    ) -> None:
        requests = []

        def send(builder, *, futures=False):
            self.assertTrue(futures)
            prepared = builder()
            requests.append(prepared)
            if prepared.endpoint == BINANCE_FUTURES_POSITION_MODE_ENDPOINT:
                payload = {"dualSidePosition": True}
            elif (
                prepared.endpoint
                == BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT
            ):
                payload = [
                    {
                        "symbol": "BTCUSDT",
                        "marginType": "ISOLATED",
                        "leverage": 1,
                    }
                ]
            else:
                payload = {}
            return {
                "ok": True,
                "statusCode": 200,
                "json": payload,
                "text": "",
            }

        normalized = {
            "symbol": "BTCUSDT",
            "side": "SELL",
            "order_type": "MARKET",
            "quantity": "0.001",
            "qty": "0.001",
            "position_direction": "short",
            "risk_reducing": False,
            "max_leverage": 1,
            "required_margin_type": "ISOLATED",
        }
        with (
            patch(
                "live_trader.brokers.normalize_binance_futures_intent",
                return_value=normalized,
            ),
            patch(
                "live_trader.brokers.send_binance_signed_request",
                side_effect=send,
            ),
        ):
            response = LiveBrokerRouter().test_binance_futures_order(
                normalized
            )

        self.assertTrue(response["ok"])
        self.assertEqual(
            [
                BINANCE_FUTURES_POSITION_MODE_ENDPOINT,
                BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT,
                BINANCE_FUTURES_TEST_ORDER_ENDPOINT,
            ],
            [item.endpoint for item in requests],
        )
        self.assertEqual("POST", requests[-1].method)
        self.assertNotIn(
            BINANCE_FUTURES_ORDER_ENDPOINT,
            [item.endpoint for item in requests],
        )

    def test_router_test_order_fails_closed_if_builder_drifts_to_live_endpoint(
        self,
    ) -> None:
        normalized = {
            "symbol": "BTCUSDT",
            "side": "SELL",
            "order_type": "MARKET",
            "quantity": "0.001",
            "qty": "0.001",
            "position_direction": "short",
            "risk_reducing": False,
            "max_leverage": 1,
            "required_margin_type": "ISOLATED",
        }

        def send(builder, *, futures=False):
            self.assertTrue(futures)
            prepared = builder()
            if prepared.endpoint == BINANCE_FUTURES_POSITION_MODE_ENDPOINT:
                payload = {"dualSidePosition": True}
            elif (
                prepared.endpoint
                == BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT
            ):
                payload = [
                    {
                        "symbol": "BTCUSDT",
                        "marginType": "ISOLATED",
                        "leverage": 1,
                    }
                ]
            else:
                payload = {}
            return {
                "ok": True,
                "statusCode": 200,
                "json": payload,
                "text": "",
            }

        unsafe = SimpleNamespace(
            method="POST",
            endpoint=BINANCE_FUTURES_ORDER_ENDPOINT,
        )
        with (
            patch(
                "live_trader.brokers.normalize_binance_futures_intent",
                return_value=normalized,
            ),
            patch(
                "live_trader.brokers.send_binance_signed_request",
                side_effect=send,
            ),
            patch(
                "live_trader.brokers.build_binance_futures_order_request",
                return_value=unsafe,
            ),
            self.assertRaises(BrokerNotReadyError),
        ):
            LiveBrokerRouter().test_binance_futures_order(normalized)


if __name__ == "__main__":
    unittest.main()
