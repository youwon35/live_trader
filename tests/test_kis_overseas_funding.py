"""Meaningful offline policy/diagnostic controls; no application initialization."""
from pathlib import Path
from decimal import Decimal
import copy
import hashlib
import importlib.util
import json
import unittest

SOURCE = Path(__file__).resolve().parents[1] / "live_trader/kis_overseas_funding.py"
spec = importlib.util.spec_from_file_location("_pure_kis_overseas_funding_test", SOURCE)
funding = importlib.util.module_from_spec(spec)
spec.loader.exec_module(funding)


class FundingPolicyTests(unittest.TestCase):
    def test_public_mode_aliases_align_with_default(self):
        self.assertEqual(funding.MODE_KEY, funding.KEY)
        self.assertEqual(funding.DEFAULT_MODE, funding.INTEGRATED_MARGIN)
        self.assertEqual(funding.resolve_funding_mode({}), funding.DEFAULT_MODE)

    def test_missing_and_blank_choose_requested_default(self):
        for settings in ({}, {funding.KEY: ""}, {funding.KEY: "  \t"}):
            self.assertEqual(funding.resolve_funding_mode(settings), "INTEGRATED_MARGIN")

    def test_explicit_foreign_cash_is_preserved(self):
        settings = {funding.KEY: " FOREIGN_CASH "}
        before = copy.deepcopy(settings)
        policy = funding.funding_policy(settings)
        self.assertEqual(policy["fundingMode"], "FOREIGN_CASH")
        self.assertFalse(policy["autoFxRequested"])
        self.assertFalse(policy["brokerServiceChanged"])
        self.assertEqual(settings, before)

    def test_invalid_mode_does_not_silently_fallback(self):
        for value in (None, True, 1, [], {}, "AUTO", "integrated_margin"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                funding.resolve_funding_mode({funding.KEY: value})
        with self.assertRaises(ValueError):
            funding.resolve_funding_mode(None)

    def test_policy_hash_binds_mode_and_capability(self):
        a = funding.funding_policy({}, exchange="NASD", currency="USD")
        b = funding.funding_policy({funding.KEY: "FOREIGN_CASH"}, exchange="NASD", currency="USD")
        c = funding.funding_policy({}, exchange="TKSE", currency="JPY")
        payload = dict(a); expected = payload.pop("policyHash")
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                         separators=(",", ":"), allow_nan=False).encode()
        self.assertEqual(expected, hashlib.sha256(raw).hexdigest())
        self.assertNotEqual(a["policyHash"], b["policyHash"])
        self.assertNotEqual(a["policyHash"], c["policyHash"])

    def test_all_policies_keep_financial_authority_false(self):
        for mode in ("INTEGRATED_MARGIN", "FOREIGN_CASH"):
            for exchange, currency in ((None, None), ("NASD", "USD"), ("TKSE", "JPY"), ("SEHK", "USD"), ("LSE", "GBP")):
                policy = funding.funding_policy({funding.KEY: mode}, exchange=exchange, currency=currency)
                for key in ("brokerEnrollmentVerified", "settlementVerified", "sharedCashReservationVerified", "productionReady", "orderAuthorityGranted"):
                    self.assertIs(policy[key], False)
                for absent in ("availableCash", "ready", "READY", "maximumBuyQuantity"):
                    self.assertNotIn(absent, policy)

    def test_documented_service_does_not_enable_new_app_markets(self):
        for exchange, currency in (("SEHK", "HKD"), ("SHAA", "CNY"), ("SZAA", "CNY"), ("TKSE", "JPY")):
            p = funding.funding_policy({}, exchange=exchange, currency=currency)
            self.assertEqual(p["capability"]["serviceSupport"], "DOCUMENTED_MARKET_CURRENCY_ONLY_ACCOUNT_UNVERIFIED")
            self.assertEqual(p["capability"]["currentAppSupport"], "UNKNOWN_OR_UNIMPLEMENTED_IN_CURRENT_APP")
            self.assertFalse(p["productionReady"])

    def test_unknown_and_unlisted_are_not_supported_by_guess(self):
        for exchange, currency in (("LSE", "USD"), ("FAKE", "USD"), (None, None)):
            p = funding.funding_policy({}, exchange=exchange, currency=currency)
            self.assertEqual(p["capability"]["serviceSupport"], "UNKNOWN")
        p = funding.funding_policy({}, exchange="VNSE", currency="VND")
        self.assertEqual(p["capability"]["serviceSupport"], "NOT_LISTED_IN_OFFICIAL_SERVICE_COUNTRIES")

    def test_official_currency_and_aftermarket_restrictions(self):
        for exchange, currency in (("LSE", "GBP"), ("LSE", "GBX"), ("SEHK", "USD")):
            p = funding.funding_policy({}, exchange=exchange, currency=currency)
            self.assertEqual(p["capability"]["serviceSupport"], "DENIED_OFFICIAL_CURRENCY_RESTRICTION")
            self.assertTrue(p["capability"]["restrictionCodes"])
        p = funding.funding_policy({}, exchange="NASD", currency="USD")
        self.assertFalse(p["capability"]["extendedAftermarketSupportedForIntegratedAccount"])
        self.assertIn("EXTENDED_AFTERMARKET_ENROLLMENT", p["capability"]["restrictedServices"])


class ReportedCapacityTests(unittest.TestCase):
    def setUp(self):
        self.caps = {"tr_crcy_cd": "USD", "echm_af_ord_psbl_amt": "100.2500",
                     "echm_af_ord_psbl_qty": "2", "ord_psbl_frcr_amt": "0"}

    def parse(self, caps=None, settings=None, **scope):
        return funding.parse_orderable_funding(settings or {}, self.caps if caps is None else caps,
            exchange=scope.get("exchange", "NASD"), currency=scope.get("currency", "USD"))

    def test_after_fx_is_reported_without_cash_promotion_or_arithmetic(self):
        original = copy.deepcopy(self.caps)
        value = self.parse()
        self.assertEqual(value["reportedAfterFxBuyingPower"], "100.2500")
        self.assertEqual(value["reportedAfterFxMaximumBuyQuantity"], "2")
        self.assertEqual(value["existingForeignCash"], "0")
        self.assertTrue(value["privateValuesLocalOnly"])
        for key in ("cashAndCostArithmeticPerformed", "amountsCombined", "capacityUsedForOrderSizing", "orderAuthorityGranted", "productionReady", "brokerEnrollmentVerified", "settlementVerified", "actualFeeVerified", "fxEventVerified"):
            self.assertIs(value[key], False)
        self.assertNotIn("availableCash", value)
        self.assertNotIn("maximumBuyQuantity", value)
        self.assertEqual(self.caps, original)

    def test_foreign_cash_preference_does_not_activate_after_fx(self):
        value = self.parse(settings={funding.KEY: "FOREIGN_CASH"})
        self.assertFalse(value["autoFxRequested"])
        self.assertFalse(value["capacityUsedForOrderSizing"])
        self.assertFalse(value["brokerServiceChanged"])

    def test_absent_pair_is_unknown_not_zero(self):
        for extras in ({}, {"echm_af_ord_psbl_amt": "", "echm_af_ord_psbl_qty": " "},
                       {"echm_af_ord_psbl_amt": None, "echm_af_ord_psbl_qty": None}):
            value = self.parse({"tr_crcy_cd": "USD", **extras})
            self.assertEqual(value["capacityStatus"], "UNKNOWN_NOT_ZERO")
            self.assertIsNone(value["reportedAfterFxBuyingPower"])
            self.assertIsNone(value["reportedAfterFxMaximumBuyQuantity"])
            self.assertIsNone(value["existingForeignCash"])

    def test_partial_after_fx_pair_is_rejected(self):
        for extras in ({"echm_af_ord_psbl_amt": "1"}, {"echm_af_ord_psbl_qty": "1"},
                       {"echm_af_ord_psbl_amt": ""}, {"echm_af_ord_psbl_qty": None},
                       {"echm_af_ord_psbl_amt": "", "echm_af_ord_psbl_qty": "1"},
                       {"echm_af_ord_psbl_amt": "1", "echm_af_ord_psbl_qty": None}):
            with self.subTest(extras=extras), self.assertRaises(ValueError):
                self.parse({"tr_crcy_cd": "USD", **extras})

    def test_invalid_number_or_ambiguous_field_types_rejected(self):
        invalid = (True, False, 0, 1.2, Decimal("1"), [], {}, ["1"], "-1", "-0", "+1", "NaN", "Infinity", "1e3", "1,000", ".5", "1.", "١", "0" * 129)
        for field in ("echm_af_ord_psbl_amt", "echm_af_ord_psbl_qty", "ord_psbl_frcr_amt"):
            for value in invalid:
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.parse({**self.caps, field: value})

    def test_zero_is_observed_but_never_enrollment_or_authority(self):
        value = self.parse({"tr_crcy_cd": "USD", "echm_af_ord_psbl_amt": "0.00", "echm_af_ord_psbl_qty": "0"})
        self.assertEqual(value["capacityStatus"], "REPORTED_AFTER_FX_CAPACITY_NOT_FINANCIAL_AUTHORITY")
        self.assertEqual(value["reportedAfterFxBuyingPower"], "0.00")
        self.assertFalse(value["brokerEnrollmentVerified"])
        self.assertFalse(value["productionReady"])

    def test_caps_must_be_one_unambiguous_object(self):
        for caps in ([], [self.caps], None, "USD", True):
            with self.subTest(caps=caps), self.assertRaises(ValueError):
                funding.parse_orderable_funding({}, caps, exchange="NASD", currency="USD")

    def test_currency_binding_required(self):
        for value in (None, "", "HKD", True, ["USD"]):
            caps = {**self.caps, "tr_crcy_cd": value}
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.parse(caps)
        for exchange, currency in ((None, "USD"), ("NASD", None), ("../NASD", "USD")):
            with self.assertRaises(ValueError):
                self.parse(exchange=exchange, currency=currency)

    def test_denied_service_capacity_does_not_bypass_restriction(self):
        result = self.parse(exchange="SEHK", currency="USD")
        self.assertEqual(result["capability"]["serviceSupport"], "DENIED_OFFICIAL_CURRENCY_RESTRICTION")
        self.assertFalse(result["orderAuthorityGranted"])
        self.assertEqual(result["reportedAfterFxBuyingPower"], "100.2500")


if __name__ == "__main__":
    unittest.main()
