from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock, patch

from live_trader import read_only_preparation as preparation
from live_trader.kis_provisional_preparation import build_provisional_fee_preview
from live_trader.kis_provisional_fees import SETTING_DEFAULTS


class ProvisionalPreparationTests(TestCase):
    def source(self, lane='kis-kr'):
        return {'kind': 'NON_PROMOTION', 'evidenceClass': 'FUNCTIONAL_TEST_NON_PROMOTION',
                'qualification': 'NON_PROMOTION', 'portfolioHash': 'a' * 64,
                'instruments': [{'instanceId': 'example', 'symbol': '005930' if lane == 'kis-kr' else 'SPY',
                                 'broker': lane, 'provider': 'kis',
                                 'executionPolicy': {'feePercent': '0.777'}}]}

    def request(self, read=False, complete=True):
        return {'source': {'kind': 'NON_PROMOTION'}, 'readAccount': read,
                'draft': {'instanceId': 'example', **({'quantity': '10', 'limitPrice': '10000',
                                                      'side': 'BUY'} if complete else {})}}

    def run_local(self, *, lane='kis-kr', read=False, complete=True, reader=None):
        source = self.source(lane)
        old = deepcopy(source)
        with patch.object(preparation, 'source_snapshot', return_value=source):
            result = preparation.prepare_read_only(self.request(read, complete), roots=[], reader=reader)
        self.assertEqual(old, source, 'Source scientific costs must not change')
        return result

    def test_offline_selected_kis_has_two_alternatives_and_no_actual_fee_or_permission(self):
        reader = Mock(settings={})
        result = self.run_local(reader=reader)
        reader.read.assert_not_called()
        preview = result['observation']['provisionalFeeEstimate']
        self.assertEqual('USER_ENTERED_NOTIONAL_ONLY', preview['calculationBasis'])
        self.assertEqual('UNKNOWN_SHOW_SEPARATE_STOCK_ETF_ALTERNATIVES', preview['productClassification'])
        self.assertEqual(['STOCK', 'ETF'], [row['product'] for row in preview['estimates']])
        self.assertEqual(['14.0527', '14.6527'], [row['nativeEstimate'] for row in preview['estimates']])
        self.assertIsNone(result['observation'].get('feeRate'))
        self.assertFalse(preview['actualFeeObserved'])
        self.assertFalse(preview['orderAuthorityGranted'])
        self.assertFalse(result['executable'])
        self.assertEqual(0, result['ordersSubmitted'])
        self.assertEqual('UNKNOWN', next(c for c in result['checks'] if c['code'] == 'KIS_PROVISIONAL_FEES')['status'])

    def test_us_estimate_uses_usd_without_converting_or_assuming_dollar_cash(self):
        result = self.run_local(lane='kis-us', reader=Mock(settings={}))
        preview = result['observation']['provisionalFeeEstimate']
        for row in preview['estimates']:
            self.assertEqual('USD', row['currency'])
            self.assertEqual('250', row['nativeEstimate'])
            self.assertNotIn('totalCost', row)
            self.assertNotIn('quantity', row)
        self.assertTrue(all(value is None for value in preview['profile']['unknownCosts'].values()))
        self.assertNotIn('availableCash', result['observation'])

    def test_incomplete_notional_remains_null_and_never_invents_zero_cost(self):
        result = self.run_local(complete=False, reader=Mock(settings={}))
        preview = result['observation']['provisionalFeeEstimate']
        self.assertIsNone(preview['estimatedNotional'])
        self.assertTrue(all(row['nativeEstimate'] is None for row in preview['estimates']))
        self.assertTrue(all(row['notional'] is None for row in preview['estimates']))

    def test_saved_estimate_changes_calculation_and_hash_but_not_confirmation(self):
        setting = {'KIS_ESTIMATED_KRX_STOCK_FEE_PERCENT': '0.01'}
        default = self.run_local(reader=Mock(settings={}))['observation']['provisionalFeeEstimate']
        updated = self.run_local(reader=Mock(settings=setting))['observation']['provisionalFeeEstimate']
        self.assertEqual('10', updated['estimates'][0]['nativeEstimate'])
        self.assertNotEqual(default['profile']['feeProfileHash'], updated['profile']['feeProfileHash'])
        self.assertEqual('PROVISIONAL_CONFIRMATION_REQUIRED', updated['status'])
        self.assertFalse(updated['actualFeeObserved'])

    def test_bad_estimate_rejected_before_any_requested_account_read(self):
        reader = Mock(settings={'KIS_ESTIMATED_US_FEE_PERCENT': 'NaN'})
        with self.assertRaises(ValueError):
            self.run_local(lane='kis-us', read=True, reader=reader)
        reader.read.assert_not_called()
        reader.observe_preparation.assert_not_called()

    def test_change_during_account_read_discards_result(self):
        reader = Mock(settings={})
        def changed(*args, **kwargs):
            reader.settings['KIS_ESTIMATED_KRX_STOCK_FEE_PERCENT'] = '0.09'
            return {'status': 'UNKNOWN', 'account': 'UNKNOWN', 'openOrders': 'UNKNOWN',
                    'orderability': 'UNKNOWN', 'fundsCheck': 'UNKNOWN'}
        reader.read.side_effect = changed
        with self.assertRaises(ValueError):
            self.run_local(read=True, reader=reader)
        self.assertEqual(1, reader.read.call_count)

    def test_non_kis_path_ignores_unrelated_provisional_fees(self):
        result = self.run_local(lane='upbit', reader=Mock(settings={'KIS_ESTIMATED_US_FEE_PERCENT': 'bad'}))
        self.assertNotIn('provisionalFeeEstimate', result['observation'])

    def test_followup_steps_cannot_claim_full_account_readiness(self):
        value = build_provisional_fee_preview('kis-kr', None, {})
        self.assertEqual(['PERSONAL_FEE_TERMS', 'ACCOUNT_SHARED_KRW_RECONCILIATION',
                          'ACTUAL_FX_AND_SETTLEMENT', 'PAPER_SOURCE_REVALIDATION',
                          'MANUAL_LIVE_START'], [row['code'] for row in value['followupSteps']])
        self.assertEqual('BLOCKED', value['followupSteps'][-1]['status'])
        self.assertFalse(value['orderAuthorityGranted'])

    def test_runtime_settings_includes_estimates_but_no_secret_is_in_fee_preview(self):
        from live_trader.read_only_account import runtime_settings
        with patch.dict('os.environ', {'KIS_ESTIMATED_US_FEE_PERCENT': '0.08', 'KIS_APP_KEY': 'never-expose'}):
            settings = runtime_settings()
        self.assertTrue(set(SETTING_DEFAULTS).issubset(settings))
        value = build_provisional_fee_preview('kis-us', '100', settings)
        self.assertEqual('0.08', value['estimates'][0]['nativeEstimate'])
        self.assertNotIn('never-expose', repr(value))

    def test_unsupported_market_does_not_fall_back_to_us_commission(self):
        for lane in ('kis-hk', 'kis-jp', 'upbit', ''):
            with self.assertRaises(ValueError):
                build_provisional_fee_preview(lane, '100', {})
