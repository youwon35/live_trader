from datetime import datetime, timedelta, timezone
from unittest import TestCase
from unittest.mock import patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousController
from live_trader.order_management import OrderIntent


class ContinuousPortfolioWeightsTests(TestCase):
    @staticmethod
    def intent(side, quantity, *, asset="kr-stock", broker="kis", purpose="SIGNAL", alpha=15):
        return OrderIntent(
            "confirmed-bar", asset, "AAPL" if asset == "us-stock" else "005930", side,
            quantity, 100.0, "SMALL_LIVE", "unit",
            {"broker_id": broker, "order_purpose": purpose, "expected_alpha_bps": alpha,
             "expected_cost_bps": 8.0},
        )

    @staticmethod
    def evaluate(intent):
        gate = {
            "active": True, "allowed": True, "targetWeight": 0.5, "maxSymbolWeightPct": 50,
            "rebalancePolicy": {"deadbandWeight": 0.001, "minimumNotional": 10},
        }
        with patch.object(state, "portfolio_gate_for_strategy", return_value=gate):
            return state.portfolio_gate_for_intent({}, intent)

    def test_entry_partial_and_full_exit_measure_observed_weight_and_order_effect(self):
        for side, held, ordered, post_trade_weight, reason in (
            ("BUY", 0, 3, 0.3, "economic-rebalance"),
            ("SELL", 5, 2, 0.3, "economic-rebalance"),
            ("SELL", 5, 5, 0.0, "risk-reducing-order-priority"),
            ("BUY", -5, 5, 0.0, "risk-reducing-order-priority"),
        ):
            with self.subTest(side=side, held=held, ordered=ordered):
                purpose = "PORTFOLIO_NET" if side == "SELL" and ordered == 2 else "SIGNAL"
                intent = LiveContinuousController._with_observed_position(
                    self.intent(side, ordered, purpose=purpose), held, native_equity=1000,
                )
                gate = self.evaluate(intent)
                decision = gate["rebalanceDecision"]
                self.assertTrue(gate["allowed"])
                self.assertEqual(gate["targetWeight"], 0.5)
                self.assertAlmostEqual(decision["currentWeight"], held * 0.1)
                self.assertAlmostEqual(decision["targetWeight"], post_trade_weight)
                self.assertAlmostEqual(decision["expectedNotional"], ordered * 100)
                self.assertEqual(decision["reason"], reason)

    def test_full_close_remains_valid_but_partial_reduction_considers_cost(self):
        for ordered, expected_reason in (
            (2, "expected-alpha-does-not-cover-cost"),
            (5, "risk-reducing-order-priority"),
            (6, "observed-order-exposure-invalid"),
        ):
            with self.subTest(ordered=ordered):
                intent = LiveContinuousController._with_observed_position(
                    self.intent("SELL", ordered, alpha=0), 5, native_equity=1000,
                )
                self.assertEqual(self.evaluate(intent)["rebalanceDecision"]["reason"], expected_reason)

    def test_legacy_live_weight_uses_only_fresh_same_currency_equity(self):
        now = datetime.now(timezone.utc)
        for age, expected in ((1, 0.2), (300, None)):
            with self.subTest(age=age), patch.dict(state.STATE, {
                "dry_run": False,
                "account_risk": {"budgets": {
                    "kr": {"broker_id": "kis", "currency": "KRW", "current_equity": 3_000_000,
                           "observed_epoch": now.timestamp()},
                    "us": {"broker_id": "kis", "currency": "USD", "current_equity": 1000,
                           "observed_epoch": (now - timedelta(seconds=age)).timestamp()},
                }},
            }):
                intent = LiveContinuousController._with_observed_position(
                    self.intent("BUY", 1, asset="us-stock"), 2,
                )
                self.assertEqual(intent.metadata["quote_currency"], "USD")
                self.assertEqual(intent.metadata["current_weight"], expected)
                self.assertEqual(intent.metadata["portfolio_equity"], 1000 if expected is not None else None)
                if expected is None:
                    self.assertEqual(self.evaluate(intent)["rebalanceDecision"]["reason"], "rebalance-inputs-missing")
