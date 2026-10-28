"""Offline 2x/10x sizing and protection: no application startup or real API."""
from copy import deepcopy
from decimal import Decimal
import socket
import unittest
from unittest.mock import patch

import test_short_live_brokers as base
import test_short_live_futures_integration as integration
import test_short_live_futures_protection as protection


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


    def path_fixture(self, *, direction="short", stop="9.35", notional="99"):
        f = protection.ProtectionFixture(direction)
        f.order.update(quantity=str(Decimal(notional)/100), projectedGrossExposureKrw=notional,
                       estimatedNotionalKrw=notional)
        f.policy.update(leverage=10, stopLossPercent=stop)
        f.market.update(feeRate=".0004", fundingRate=".0001", leverageBrackets=[
            {"notionalFloor":"0", "notionalCap":"100", "initialLeverage":"20", "maintMarginRatio":".005", "cum":"0"},
            {"notionalFloor":"100", "notionalCap":"1000", "initialLeverage":"20", "maintMarginRatio":".02", "cum":"1.5"}])
        return f

    def test_short_crossing_higher_maintenance_tier_blocks_stop_beyond_piecewise_liquidation(self):
        f = self.path_fixture()
        # Both tiers are continuous at 100. Even ignoring commissions/funding,
        # the actual short equity reaches maintenance at 109.32858, before the
        # old entry-tier-only guard's accepted stop at 109.35.
        liquidation = (Decimal("9.9") + 99 + Decimal("1.5")) / (Decimal(".99") * Decimal("1.02"))
        self.assertLess(liquidation, Decimal("109.35"))
        self.assertGreater(liquidation * Decimal(".99"), 100)
        with self.assertRaisesRegex(protection.mod.FuturesProtectionError, "MAINTENANCE_BUFFER"):
            f.ensure()
        self.assertEqual(f.calls, [])

    def test_short_crossing_higher_tier_with_nearby_stop_remains_valid(self):
        f = self.path_fixture(stop="2")
        self.assertEqual(protection.mod.stop_price(f.order, f.market, f.policy), "102.00")
        self.assertEqual(f.calls, [])

    def test_long_stops_keep_current_tier_bound_even_when_notional_falls_to_lower_tier(self):
        for notional, stop, expected in (("99", "9.35", "90.65"), ("101", "2", "98.00")):
            with self.subTest(notional=notional):
                f = self.path_fixture(direction="long", stop=stop, notional=notional)
                self.assertEqual(protection.mod.stop_price(f.order, f.market, f.policy), expected)
                self.assertEqual(f.calls, [])

    def test_stop_path_rejects_gaps_overlaps_and_uncovered_adverse_notional_before_post(self):
        for field, value in (("notionalFloor", "100.5"), ("notionalFloor", "99.5"), ("notionalCap", "105")):
            with self.subTest(field=field, value=value):
                f = self.path_fixture()
                f.market["leverageBrackets"][1][field] = value
                with self.assertRaisesRegex(protection.mod.FuturesProtectionError, "NOTIONAL_PATH_UNKNOWN"):
                    f.ensure()
                self.assertEqual(f.calls, [])

    def test_stop_at_exact_notional_cap_requires_coverage_of_endpoint_tier(self):
        f = self.path_fixture(stop="2")
        endpoint = Decimal("99") * Decimal("1.02")
        f.market["leverageBrackets"] = [{"notionalFloor":"0", "notionalCap":str(endpoint),
            "initialLeverage":"20", "maintMarginRatio":".005", "cum":"0"}]
        with self.assertRaisesRegex(protection.mod.FuturesProtectionError, "NOTIONAL_PATH_UNKNOWN"):
            f.ensure()
        self.assertEqual(f.calls, [])
        f.market["leverageBrackets"].append({"notionalFloor":str(endpoint), "notionalCap":"1000",
            "initialLeverage":"20", "maintMarginRatio":".005", "cum":"0"})
        self.assertEqual(protection.mod.stop_price(f.order, f.market, f.policy), "102.00")


if __name__ == "__main__":
    unittest.main()
