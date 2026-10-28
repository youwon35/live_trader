"""Offline 2x/10x sizing and protection: no application startup or real API."""
from copy import deepcopy
from decimal import Decimal
import socket
import unittest
from unittest.mock import patch

import test_short_live_brokers as base
import test_short_live_futures_integration as integration


class FuturesLeverageLevelsTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket.socket, "connect", side_effect=AssertionError("NETWORK_FORBIDDEN"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def fixture(self, leverage=10, *, entry=False, direction="long"):
        return integration.FuturesIntegrationTests().fixture(leverage, entry=entry, direction=direction)

    def test_one_two_five_ten_use_collateral_plus_cost_reserve_without_multiplying_cash(self):
        expected_quantities = {1: Decimal(99), 2: Decimal(199), 5: Decimal(494), 10: Decimal(978)}
        for leverage, expected in expected_quantities.items():
            with self.subTest(leverage=leverage):
                f = self.fixture(leverage)
                original = deepcopy(f.truth)
                sized = f.adapter.size_order(f.proposal("1000000"))
                self.assertTrue(sized["ready"], sized)
                quantity = base.mod.number(sized["quantity"])
                notional = quantity * Decimal(1000)
                required = notional * (Decimal(1) / leverage + Decimal("0.0022"))
                self.assertEqual(quantity, expected)
                self.assertEqual(base.mod.number(sized["leverage"]), leverage)
                self.assertEqual(base.mod.number(sized["estimatedNotionalKrw"]), notional)
                self.assertEqual(base.mod.number(sized["requiredCapitalKrw"]), required)
                self.assertLessEqual(required, Decimal(100000))
                self.assertGreater((quantity + 1) * 1000 * (Decimal(1)/leverage + Decimal("0.0022")), 100000)
                self.assertEqual(f.truth, original)
                self.assertEqual([], f.calls)
                self.assertEqual([], f.protection_calls)

    def test_two_and_ten_restrict_small_cash_to_real_collateral(self):
        for leverage, expected in ((2, "1"), (10, "9")):
            with self.subTest(leverage=leverage):
                f = self.fixture(leverage)
                f.truth["availableNative"] = "1000"
                sized = f.adapter.size_order(f.proposal("1000000"))
                self.assertTrue(sized["ready"], sized)
                self.assertEqual(sized["quantity"], expected)
                self.assertLessEqual(base.mod.number(sized["requiredCapitalKrw"]), Decimal(1000))
                self.assertEqual(f.truth["availableNative"], "1000")
                self.assertEqual([], f.calls)

    def test_two_and_ten_keep_long_short_direction_and_protection_before_entry(self):
        for leverage in (2, 10):
            for direction in ("long", "short"):
                with self.subTest(leverage=leverage, direction=direction):
                    f = self.fixture(leverage, entry=True, direction=direction)
                    self.assertEqual(len(f.calls), 1)
                    self.assertEqual(f.calls[0]["side"], "BUY" if direction == "long" else "SELL")
                    self.assertEqual(len(f.protections), 1)
                    claim = next(iter(f.protections.values()))
                    self.assertEqual(claim["workingType"], "MARK_PRICE")
                    self.assertTrue(claim["closePosition"])
                    self.assertEqual(claim["side"], "SELL" if direction == "long" else "BUY")
                    self.assertEqual(claim["status"], "NEW")
                    state = f.adapter.reconcile(f.manifest)
                    self.assertTrue(state["known"], state)
                    self.assertEqual(base.mod.number(state["notionalExposureKrw"]), Decimal(10000))
                    self.assertEqual(base.mod.number(state["committedCapitalKrw"]), Decimal(10000)/leverage)

    def test_mismatch_between_selected_ten_and_actual_two_blocks_new_entry(self):
        f = self.fixture(10)
        f.quote["leverage"] = "2"
        ready = f.adapter.readiness(f.manifest)
        self.assertFalse(ready["ready"], ready)
        self.assertIn("FUTURES_SETTINGS_DIFFER_FROM_PREPARED_POLICY", ready["blockers"])
        self.assertEqual([], f.calls)
        self.assertEqual([], f.protection_calls)
        self.assertEqual(f.manifest["futuresPolicy"]["leverage"], 10)
        self.assertEqual(f.quote["leverage"], "2")

    def test_previous_fifty_percent_stop_is_rejected_without_silent_clamping(self):
        for leverage in (2, 10):
            with self.subTest(leverage=leverage):
                f = base.Fixture("BINANCE_USDM_FUTURES")
                f.manifest["futuresPolicy"].update(leverage=leverage, stopLossPercent=50)
                f.quote["leverage"] = str(leverage)
                ready = f.adapter.readiness(f.manifest)
                self.assertFalse(ready["ready"], ready)
                self.assertEqual(f.manifest["futuresPolicy"]["stopLossPercent"], 50)
                self.assertEqual([], f.calls)
                self.assertEqual([], f.protection_calls)

    def test_ten_requires_actual_bracket_allowance_even_when_contract_range_accepts_it(self):
        f = base.Fixture("BINANCE_USDM_FUTURES")
        f.manifest["futuresPolicy"]["leverage"] = 10
        f.quote["leverage"] = "10"
        f.quote["leverageBrackets"][0]["initialLeverage"] = "5"
        ready = f.adapter.readiness(f.manifest)
        self.assertFalse(ready["ready"], ready)
        self.assertIn("FUTURES_LEVERAGE_BRACKET_EXCEEDED", ready["blockers"])
        self.assertEqual([], f.calls)
        self.assertEqual([], f.protection_calls)

    def test_ten_stop_inside_simple_ten_percent_bound_still_respects_maintenance_and_cost(self):
        f = base.Fixture("BINANCE_USDM_FUTURES")
        f.manifest["futuresPolicy"].update(leverage=10, stopLossPercent=9.9)
        f.quote["leverage"] = "10"
        ready = f.adapter.readiness(f.manifest)
        self.assertFalse(ready["ready"], ready)
        self.assertIn("FUTURES_STOP_AT_OR_BEYOND_MAINTENANCE_BUFFER", ready["blockers"])
        self.assertEqual([], f.calls)
        self.assertEqual([], f.protection_calls)


if __name__ == "__main__":
    unittest.main()
