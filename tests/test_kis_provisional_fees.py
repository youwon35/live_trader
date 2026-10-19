"""Unconfirmed native commission estimates, independent of accounts and orders."""
from copy import deepcopy
from decimal import Decimal, localcontext
import importlib.util
import json
from pathlib import Path
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'live_trader/kis_provisional_fees.py'
spec = importlib.util.spec_from_file_location('_pure_kis_provisional_fees_test', SOURCE)
fees = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fees)


class ProvisionalFeeTests(unittest.TestCase):
    def assert_unconfirmed(self, value):
        self.assertEqual(value['status'], 'PROVISIONAL_CONFIRMATION_REQUIRED')
        self.assertIs(value['confirmationRequired'], True)
        self.assertIs(value['estimateOnly'], True)
        for key in fees._FALSE:
            self.assertIs(value[key], False, key)

    def test_public_default_rates_are_distinct_by_market_and_product(self):
        profile = fees.build_provisional_fee_profile()
        self.assert_unconfirmed(profile)
        self.assertEqual({(row['market'], row['product']): row['ratePercent'] for row in profile['rates']},
            {('KRX', 'STOCK'): '0.0140527', ('KRX', 'ETF'): '0.0146527',
             ('NXT', 'STOCK'): '0.0130527', ('US', 'STOCK'): '0.25', ('US', 'ETF'): '0.25'})
        self.assertEqual(fees.validate_provisional_fee_profile(profile), profile)
        self.assertEqual(profile['feeProfileHash'], fees.build_provisional_fee_profile({})['feeProfileHash'])

    def test_percent_and_fraction_are_not_confused_and_native_estimate_is_unrounded(self):
        profile = fees.build_provisional_fee_profile()
        krw = fees.estimate_fee(profile, 'KRX', 'STOCK', '1000000')
        self.assertEqual((krw['ratePercent'], krw['rateFraction'], krw['nativeEstimate'], krw['currency']),
            ('0.0140527', '0.000140527', '140.527', 'KRW'))
        usd = fees.estimate_fee(profile, 'US', 'ETF', '123.45')
        self.assertEqual((usd['rateFraction'], usd['nativeEstimate'], usd['currency']),
            ('0.0025', '0.308625', 'USD'))
        self.assertFalse(usd['roundingApplied'])
        self.assertIsNone(usd['settlementRoundingRule'])
        self.assert_unconfirmed(usd)

    def test_whole_supported_draft_product_precision_is_exact_even_under_small_context(self):
        profile = fees.build_provisional_fee_profile()
        with localcontext() as context:
            context.prec = 128
            operand = Decimal('999999999999999999.123456789012345678')
            notional = format(operand * operand, 'f')
            expected = fees._canonical(Decimal(notional) * Decimal('0.000140527'))
        with localcontext() as context:
            context.prec = 4
            result = fees.estimate_fee(profile, 'KRX', 'STOCK', notional)
        self.assertEqual(result['nativeEstimate'], expected)

    def test_unentered_notional_and_unknown_costs_remain_none_never_zero(self):
        profile = fees.build_provisional_fee_profile()
        result = fees.estimate_fee(profile, 'US', 'STOCK', None)
        self.assertIsNone(result['notional'])
        self.assertIsNone(result['nativeEstimate'])
        self.assertTrue(all(value is None for value in profile['unknownCosts'].values()))
        self.assertTrue(all(value is None for value in result['unknownCosts'].values()))
        for key in ('totalCost', 'fxRate', 'orderQuantity', 'availableCash'):
            self.assertNotIn(key, result)
        self.assert_unconfirmed(result)

    def test_custom_or_zero_estimates_never_become_personal_confirmed_fees(self):
        key = 'KIS_ESTIMATED_US_FEE_PERCENT'
        for percent in ('0', '0.1', '0.2500'):
            profile = fees.build_provisional_fee_profile({key: percent})
            self.assert_unconfirmed(profile)
            self.assert_unconfirmed(fees.estimate_fee(profile, 'US', 'STOCK', '100'))
            self.assertEqual(profile, fees.validate_provisional_fee_profile(profile))
        zero = fees.estimate_fee(fees.build_provisional_fee_profile({key: '0'}), 'US', 'ETF', '100')
        self.assertEqual(zero['nativeEstimate'], '0')
        self.assertIsNone(zero['unknownCosts']['minimumCommission'])

    def test_bad_percent_values_fail_instead_of_silently_becoming_defaults_or_zero(self):
        for value in (True, False, 1, Decimal('0.25'), None, '-1', '101', '1e-2', 'NaN',
                      'Infinity', ' 0.25', '0.25 ', '０.２５', '0.1234567890123'):
            with self.subTest(value=value), self.assertRaises(fees.KisProvisionalFeeError):
                fees.build_provisional_fee_profile({'KIS_ESTIMATED_US_FEE_PERCENT': value})
        self.assertEqual(fees.build_provisional_fee_profile({'KIS_ESTIMATED_US_FEE_PERCENT': ''}),
                         fees.build_provisional_fee_profile())

    def test_bad_notional_and_unsupported_markets_never_guess_or_convert(self):
        profile = fees.build_provisional_fee_profile()
        for value in (True, 100, Decimal('100'), '-1', 'NaN', '1e3', '1,000', '１', '1\n',
                      '9' * 37, '0.' + '1' * 37):
            with self.subTest(value=value), self.assertRaises(fees.KisProvisionalFeeError):
                fees.estimate_fee(profile, 'US', 'STOCK', value)
        for market, product in (('NXT', 'ETF'), ('JP', 'STOCK'), ('US', 'OPTION'),
                                ('krx', 'STOCK'), ('US', None)):
            with self.assertRaises(fees.KisProvisionalFeeError):
                fees.estimate_fee(profile, market, product, '100')

    def test_profile_validation_rebuild_rejects_private_fields_false_integer_aliases_and_new_rates(self):
        original = fees.build_provisional_fee_profile()
        for key, value in (('accountNumber', 'private-value'), ('actualFeeObserved', True),
                           ('confirmationRequired', 1), ('orderAuthorityGranted', 0)):
            altered = {**deepcopy(original), key: value}
            with self.subTest(key=key), self.assertRaises(fees.KisProvisionalFeeError):
                fees.validate_provisional_fee_profile(altered)
        altered = deepcopy(original)
        altered['rates'][0]['privateNote'] = 'private-value'
        with self.assertRaises(fees.KisProvisionalFeeError):
            fees.validate_provisional_fee_profile(altered)
        altered = deepcopy(original)
        altered['rates'][-1]['ratePercent'] = '0.1'
        with self.assertRaises(fees.KisProvisionalFeeError):
            fees.validate_provisional_fee_profile(altered)

    def test_profile_hash_binds_settings_without_copying_unrelated_account_values(self):
        original = fees.build_provisional_fee_profile({'KIS_APP_SECRET': 'private-secret'})
        changed = fees.build_provisional_fee_profile({'KIS_ESTIMATED_KRX_ETF_FEE_PERCENT': '0.02'})
        self.assertNotEqual(original['feeProfileHash'], changed['feeProfileHash'])
        self.assertNotIn('private-secret', json.dumps(original))


if __name__ == '__main__':
    unittest.main()
