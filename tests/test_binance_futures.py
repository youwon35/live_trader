from __future__ import annotations

import os
import unittest
from decimal import Decimal
from unittest.mock import patch

from live_trader.brokers import (
    BrokerNotReadyError,
    normalize_binance_futures_symbol_config,
    parse_binance_futures_accounts,
    parse_binance_futures_positions,
    validate_binance_futures_execution_policy,
)
from live_trader.execution_streams import (
    parse_binance_futures_order_update,
)
from live_trader.live_adapters import (
    BINANCE_FUTURES_ORDER_ENDPOINT,
    BINANCE_FUTURES_TEST_ORDER_ENDPOINT,
    build_binance_futures_order_request,
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


if __name__ == "__main__":
    unittest.main()
