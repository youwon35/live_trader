from __future__ import annotations

import json
import math
import unittest

from live_trader.futures_risk import simulate_futures_order_risk


class FuturesRiskSimulatorTest(unittest.TestCase):
    def test_isolated_short_estimate_includes_fee_funding_and_stop_risk(self) -> None:
        result = simulate_futures_order_risk(
            {
                "symbol": "ETHUSDT",
                "direction": "SHORT",
                "margin_type": "ISOLATED",
                "account_equity_usdt": 100,
                "available_usdt": 100,
                "entry_price": 2000,
                "notional_usdt": 20,
                "leverage": 2,
                "maintenance_margin_rate": 0.005,
                "taker_fee_rate": 0.0005,
                "funding_rate": 0.0001,
                "funding_intervals": 2,
                "stop_price": 2020,
            },
            policy={
                "allowed_margin_type": "ISOLATED",
                "max_leverage": 3,
                "per_trade_risk_pct": 2,
                "max_notional_pct": 25,
            },
        )

        self.assertEqual("READY", result["status"])
        self.assertAlmostEqual(0.01, result["inputs"]["quantity"])
        self.assertAlmostEqual(10.0, result["estimate"]["initial_margin_usdt"])
        self.assertGreater(result["estimate"]["liquidation_price"], 2000)
        self.assertAlmostEqual(0.02, result["estimate"]["round_trip_fee_usdt"])
        self.assertGreater(result["estimate"]["estimated_loss_at_stop_usdt"], 0.2)

    def test_policy_drift_and_unprotected_order_fail_closed(self) -> None:
        result = simulate_futures_order_risk(
            {
                "symbol": "BTCUSDT",
                "direction": "LONG",
                "margin_type": "CROSSED",
                "account_equity_usdt": 10,
                "available_usdt": 10,
                "entry_price": 100_000,
                "notional_usdt": 50,
                "leverage": 20,
            },
            policy={
                "allowed_margin_type": "ISOLATED",
                "max_leverage": 2,
                "per_trade_risk_pct": 1,
                "max_notional_pct": 10,
            },
        )

        self.assertEqual("BLOCKED", result["status"])
        self.assertIn("margin-type-policy-drift", result["blockers"])
        self.assertIn("leverage-policy-drift", result["blockers"])
        self.assertIn("max-notional-policy-exceeded", result["blockers"])
        self.assertIn("protective-stop-missing", result["blockers"])
        self.assertIsNone(result["estimate"]["liquidation_price"])

    def test_available_margin_is_checked_independently_from_notional(self) -> None:
        result = simulate_futures_order_risk(
            {
                "symbol": "SOLUSDT",
                "direction": "LONG",
                "margin_type": "ISOLATED",
                "account_equity_usdt": 100,
                "available_usdt": 2,
                "entry_price": 100,
                "notional_usdt": 10,
                "leverage": 2,
                "stop_price": 99,
            },
            policy={
                "allowed_margin_type": "ISOLATED",
                "max_leverage": 2,
                "per_trade_risk_pct": 2,
                "max_notional_pct": 20,
            },
        )
        self.assertIn("required-margin-exceeds-available", result["blockers"])

    def test_non_finite_inputs_and_policy_are_fail_closed(self) -> None:
        result = simulate_futures_order_risk(
            {
                "symbol": "ETHUSDT",
                "direction": "SHORT",
                "margin_type": "ISOLATED",
                "account_equity_usdt": 100,
                "available_usdt": 100,
                "entry_price": float("nan"),
                "notional_usdt": float("inf"),
                "leverage": 2,
                "stop_price": 101,
            },
            policy={
                "allowed_margin_type": "ISOLATED",
                "max_leverage": float("-inf"),
                "per_trade_risk_pct": 1,
                "max_notional_pct": 10,
            },
        )

        self.assertEqual("BLOCKED", result["status"])
        self.assertIn(
            "numeric-input-non-finite:entry_price",
            result["blockers"],
        )
        self.assertIn(
            "numeric-input-non-finite:notional_usdt",
            result["blockers"],
        )
        self.assertIn(
            "numeric-policy-non-finite:max_leverage",
            result["blockers"],
        )
        numeric_values = [
            value
            for section in ("inputs", "policy", "estimate")
            for value in result[section].values()
            if isinstance(value, float)
        ]
        self.assertTrue(all(math.isfinite(value) for value in numeric_values))

    def test_zero_policy_limits_are_preserved_and_block_exposure(self) -> None:
        result = simulate_futures_order_risk(
            {
                "symbol": "ETHUSDT",
                "direction": "SHORT",
                "margin_type": "ISOLATED",
                "account_equity_usdt": 100,
                "available_usdt": 100,
                "entry_price": 2000,
                "notional_usdt": 10,
                "leverage": 1,
                "taker_fee_rate": 0,
                "funding_rate": 0,
                "funding_intervals": 0,
                "stop_price": 2010,
            },
            policy={
                "allowed_margin_type": "ISOLATED",
                "max_leverage": 1,
                "per_trade_risk_pct": 0,
                "max_notional_pct": 0,
            },
        )

        self.assertEqual(0.0, result["policy"]["per_trade_risk_pct"])
        self.assertEqual(0.0, result["policy"]["max_notional_pct"])
        self.assertEqual(0.0, result["inputs"]["taker_fee_rate"])
        self.assertEqual(0.0, result["inputs"]["funding_rate"])
        self.assertIn("max-notional-policy-exceeded", result["blockers"])
        self.assertIn("per-trade-risk-policy-exceeded", result["blockers"])

    def test_invalid_direction_and_margin_type_fail_closed(self) -> None:
        result = simulate_futures_order_risk(
            {
                "symbol": "ETHUSDT",
                "direction": "SIDEWAYS",
                "margin_type": "UNKNOWN",
                "account_equity_usdt": 100,
                "available_usdt": 100,
                "entry_price": 2000,
                "notional_usdt": 10,
                "leverage": 1,
                "stop_price": 1990,
            },
            policy={
                "allowed_margin_type": "ISOLATED",
                "max_leverage": 1,
                "per_trade_risk_pct": 2,
                "max_notional_pct": 20,
            },
        )

        self.assertEqual("BLOCKED", result["status"])
        self.assertIn("direction-invalid", result["blockers"])
        self.assertIn("margin-type-invalid", result["blockers"])

    def test_explicit_zero_and_invalid_policy_validity_fail_closed(self) -> None:
        payload = {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "margin_type": "ISOLATED",
            "account_equity_usdt": 100,
            "available_usdt": 100,
            "entry_price": 100,
            "notional_usdt": 10,
            "leverage": 1,
            "maintenance_margin_rate": 0.005,
            "taker_fee_rate": 0.0005,
            "funding_rate": 0,
            "funding_intervals": 1,
            "stop_price": 99,
        }
        policy = {
            "allowed_margin_type": "ISOLATED",
            "max_leverage": 2,
            "per_trade_risk_pct": 5,
            "max_notional_pct": 100,
        }
        cases = (
            ("direction-zero", {"direction": 0}, {}, "direction-invalid"),
            (
                "margin-zero",
                {"margin_type": 0},
                {},
                "margin-type-invalid",
            ),
            ("leverage-zero", {"leverage": 0}, {}, "leverage-invalid"),
            (
                "policy-leverage-zero",
                {},
                {"max_leverage": 0},
                "max-leverage-policy-invalid",
            ),
            (
                "policy-valid-string",
                {},
                {"valid": "false"},
                "futures-policy-invalid",
            ),
            (
                "policy-valid-zero",
                {},
                {"valid": 0},
                "futures-policy-invalid",
            ),
        )

        for name, payload_update, policy_update, blocker in cases:
            with self.subTest(name=name):
                result = simulate_futures_order_risk(
                    {**payload, **payload_update},
                    policy={**policy, **policy_update},
                )

                self.assertEqual("BLOCKED", result["status"])
                self.assertIn(blocker, result["blockers"])

    def test_explicit_non_numeric_fields_fail_closed(self) -> None:
        payload = {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "margin_type": "ISOLATED",
            "account_equity_usdt": 100,
            "available_usdt": 100,
            "entry_price": 100,
            "notional_usdt": 5,
            "leverage": 1,
            "maintenance_margin_rate": 0.005,
            "taker_fee_rate": 0.0005,
            "funding_rate": 0,
            "funding_intervals": 1,
            "stop_price": 99,
        }
        policy = {
            "allowed_margin_type": "ISOLATED",
            "max_leverage": 2,
            "per_trade_risk_pct": 5,
            "max_notional_pct": 100,
        }
        input_fields = (
            "account_equity_usdt",
            "available_usdt",
            "entry_price",
            "notional_usdt",
            "leverage",
            "maintenance_margin_rate",
            "taker_fee_rate",
            "funding_rate",
            "funding_intervals",
            "stop_price",
        )
        for field in input_fields:
            with self.subTest(kind="input", field=field):
                result = simulate_futures_order_risk(
                    {**payload, field: "not-a-number"},
                    policy=policy,
                )

                self.assertEqual("BLOCKED", result["status"])
                self.assertIn(
                    f"numeric-input-invalid:{field}",
                    result["blockers"],
                )

        policy_fields = (
            "max_leverage",
            "per_trade_risk_pct",
            "max_notional_pct",
        )
        for field in policy_fields:
            with self.subTest(kind="policy", field=field):
                result = simulate_futures_order_risk(
                    payload,
                    policy={**policy, field: "not-a-number"},
                )

                self.assertEqual("BLOCKED", result["status"])
                self.assertIn(
                    f"numeric-policy-invalid:{field}",
                    result["blockers"],
                )

    def test_stop_at_or_beyond_estimated_liquidation_is_blocked(self) -> None:
        policy = {
            "allowed_margin_type": "ISOLATED",
            "max_leverage": 10,
            "per_trade_risk_pct": 25,
            "max_notional_pct": 100,
        }
        cases = (
            ("LONG", 80),
            ("SHORT", 120),
        )
        for direction, stop_price in cases:
            with self.subTest(direction=direction):
                result = simulate_futures_order_risk(
                    {
                        "symbol": "BTCUSDT",
                        "direction": direction,
                        "margin_type": "ISOLATED",
                        "account_equity_usdt": 100,
                        "available_usdt": 100,
                        "entry_price": 100,
                        "notional_usdt": 100,
                        "leverage": 10,
                        "maintenance_margin_rate": 0.005,
                        "taker_fee_rate": 0.0005,
                        "funding_rate": 0,
                        "funding_intervals": 1,
                        "stop_price": stop_price,
                    },
                    policy=policy,
                )

                self.assertEqual("BLOCKED", result["status"])
                self.assertIn(
                    "protective-stop-at-or-beyond-liquidation",
                    result["blockers"],
                )
                self.assertFalse(
                    result["estimate"][
                        "protective_stop_precedes_liquidation"
                    ]
                )

    def test_finite_input_overflow_is_blocked_and_serializes_finitely(
        self,
    ) -> None:
        result = simulate_futures_order_risk(
            {
                "symbol": "BTCUSDT",
                "direction": "SHORT",
                "margin_type": "ISOLATED",
                "account_equity_usdt": 1e308,
                "available_usdt": 1e308,
                "entry_price": 1,
                "notional_usdt": 1e308,
                "leverage": 125,
                "maintenance_margin_rate": 0,
                "taker_fee_rate": 0,
                "funding_rate": 0.1,
                "funding_intervals": 90,
                "stop_price": 1.0001,
            },
            policy={
                "allowed_margin_type": "ISOLATED",
                "max_leverage": 125,
                "per_trade_risk_pct": 100,
                "max_notional_pct": 100,
            },
        )

        self.assertEqual("BLOCKED", result["status"])
        self.assertTrue(
            any(
                blocker.startswith("risk-arithmetic-non-finite:")
                for blocker in result["blockers"]
            )
        )
        numeric_values = self._float_values(result)
        self.assertTrue(all(math.isfinite(value) for value in numeric_values))
        json.dumps(result, allow_nan=False)

    def _float_values(self, value: object) -> list[float]:
        if isinstance(value, dict):
            return [
                item
                for child in value.values()
                for item in self._float_values(child)
            ]
        if isinstance(value, (list, tuple)):
            return [
                item
                for child in value
                for item in self._float_values(child)
            ]
        return [value] if isinstance(value, float) else []


if __name__ == "__main__":
    unittest.main()
