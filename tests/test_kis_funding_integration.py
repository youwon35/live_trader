"""Funding preference integration, using synthetic accounts and no broker IO."""
import importlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import Mock, patch

import test_read_only_preparation as prep
import test_kis_equity_operational as operational

account = prep.account
profiles = importlib.import_module(prep.base.PACKAGE + '.account_profiles')
preview = importlib.import_module(prep.base.PACKAGE + '.cash_preparation_preview')
policy = importlib.import_module(prep.base.PACKAGE + '.kis_overseas_funding')
from live_trader import env_settings


SETTINGS = {'KIS_APP_KEY': 'SYNTHETIC_KEY', 'KIS_APP_SECRET': 'SYNTHETIC_SECRET',
            'KIS_ACCOUNT_NO': '12345678', 'KIS_ACCOUNT_PRODUCT_CODE': '01'}


class FundingSettingsTests(unittest.TestCase):
    def test_default_and_roundtrip_choice_preserve_secret_and_real_order_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / '.env'
            with patch.object(env_settings, 'ENV_PATH', target), patch.dict(os.environ, {
                'LIVE_TRADER_SECRET_STORE_PATH': str(Path(tmp) / 'secrets.json'),
                'KIS_OVERSEAS_FUNDING_MODE': '', 'LIVE_TRADER_ENABLE_REAL_ORDERS': 'false'}, clear=False):
                fields = {f['key']: f for f in env_settings.env_settings_snapshot()['fields']}
                field = fields[policy.MODE_KEY]
                self.assertEqual('select', field['kind'])
                self.assertEqual('INTEGRATED_MARGIN', field['value'])
                self.assertEqual({'INTEGRATED_MARGIN', 'FOREIGN_CASH'}, {v['value'] for v in field['options']})
                for mode in ('FOREIGN_CASH', 'INTEGRATED_MARGIN'):
                    snapshot = env_settings.save_env_settings({policy.MODE_KEY: mode})
                    fields = {f['key']: f for f in snapshot['fields']}
                    self.assertEqual(mode, fields[policy.MODE_KEY]['value'])
                    self.assertIn('KIS_OVERSEAS_FUNDING_MODE=' + mode, target.read_text())
                    self.assertEqual('false', os.environ['LIVE_TRADER_ENABLE_REAL_ORDERS'])

    def test_invalid_choice_fails_before_secret_migration_or_file_write(self):
        with patch.object(env_settings, 'read_env_file') as read, patch.object(env_settings, 'live_secret_store') as store, patch.object(env_settings, 'write_env_file') as write:
            for bad in ('AUTO', False, 1, None, 'integrated_margin'):
                with self.assertRaises(ValueError):
                    env_settings.save_env_settings({policy.MODE_KEY: bad})
            read.assert_not_called(); store.assert_not_called(); write.assert_not_called()

    def test_mode_change_invalidates_kis_configuration_but_keeps_identity_and_crypto(self):
        settings = {**SETTINGS, 'BINANCE_API_KEY': 'SYNTHETIC_BINANCE', 'BINANCE_API_SECRET': 'SYNTHETIC_SECRET'}
        first = {p['brokerId']: p for p in profiles.saved_account_profiles(settings)}
        second = {p['brokerId']: p for p in profiles.saved_account_profiles({**settings, policy.MODE_KEY: 'FOREIGN_CASH'})}
        self.assertEqual(first['kis']['accountFingerprint'], second['kis']['accountFingerprint'])
        self.assertNotEqual(first['kis']['profileId'], second['kis']['profileId'])
        self.assertNotEqual(first['kis']['credentialConfigurationHash'], second['kis']['credentialConfigurationHash'])
        self.assertEqual(first['binance'], second['binance'])
        self.assertFalse(first['kis']['overseasFunding']['productionReady'])


class FundingReadTests(unittest.TestCase):
    def reader(self, mode=None):
        settings = dict(SETTINGS)
        if mode is not None: settings[policy.MODE_KEY] = mode
        reader = account.ReadOnlyBrokerReader(settings, transport=Mock())
        reader._kis_token = Mock(return_value='SYNTHETIC_TOKEN')
        reader._pace_kis = Mock()
        reader._kis = Mock(return_value={'account': 'AVAILABLE', 'openOrders': 'SYMBOL_NONE_OBSERVED',
            'orderability': 'UNKNOWN', 'currency': 'USD', '_quote': Decimal('2'), '_base': Decimal('3')})
        reader._get = Mock(return_value=({'rt_cd': '0', 'output': {'tr_crcy_cd': 'USD',
            'ord_psbl_frcr_amt': '2', 'ovrs_ord_psbl_amt': '1', 'ord_psbl_qty': '1',
            'echm_af_ord_psbl_amt': '1000', 'echm_af_ord_psbl_qty': '100'}}, {}))
        return reader

    def test_after_fx_capacity_is_private_diagnostic_and_never_owned_cash_or_order_size(self):
        reader = self.reader()
        observed = reader.observe_preparation('kis-us', 'AAPL', side='BUY', quantity=Decimal('1'), limit_price=Decimal('10'))
        self.assertEqual('OBSERVED', observed['status'])
        self.assertIsNone(observed['availableCash'])
        self.assertIsNone(observed['constraints']['maximumBuyQuantity'])
        self.assertIsNone(observed['feeRate'])
        self.assertEqual('UNKNOWN', observed['fundsCheck'])
        detail = observed['fundingDiagnostics']
        self.assertEqual('1000', detail['reportedAfterFxBuyingPower'])
        self.assertEqual('2', detail['existingForeignCash'])
        self.assertFalse(detail['capacityUsedForOrderSizing'])
        self.assertFalse(detail['orderAuthorityGranted'])
        calculated = preview.cash_preview({}, observed, {'side': 'BUY'})
        self.assertEqual('BLOCKED', calculated['status'])
        self.assertEqual('INTEGRATED_MARGIN_AWAITING_SHARED_CASH_AND_SETTLEMENT', calculated['calculationScope'])
        self.assertEqual(1, reader._get.call_count)
        self.assertTrue(reader._get.call_args.args[1].endswith('/inquire-psamount'))
        reader.transport.request.assert_not_called()

    def test_public_status_does_not_expose_private_reported_amounts(self):
        reader = self.reader()
        observed = reader.read('kis-us', 'AAPL', side='BUY', quantity=Decimal('1'), limit_price=Decimal('1'))
        self.assertEqual('UNKNOWN', observed['fundsCheck'])
        self.assertNotIn('reportedAfterFxBuyingPower', json.dumps(observed))
        self.assertNotIn('SYNTHETIC', json.dumps(observed))
        reader._get.assert_not_called()

    def test_explicit_foreign_cash_retains_existing_cash_preparation(self):
        observed = self.reader('FOREIGN_CASH').observe_preparation('kis-us', 'AAPL', side='BUY', quantity=Decimal('1'), limit_price=Decimal('1'))
        self.assertEqual('1', observed['availableCash'])
        self.assertEqual('1', observed['constraints']['maximumBuyQuantity'])
        self.assertEqual('KIS_NASD_USD_NO_AUTO_FX', observed['cashScope'])
        self.assertEqual('WITHIN_OBSERVED_BALANCE', observed['fundsCheck'])

    def test_integrated_execution_stays_closed_before_broker_io_and_krx_stays_independent(self):
        _, contract, _ = operational.fixture('NASD')
        fake = operational.FakeReader()
        fake.settings.pop(policy.MODE_KEY)
        with self.assertRaisesRegex(operational.reader_module.ReadFailure, 'KIS_INTEGRATED_MARGIN_EXECUTION_NOT_VERIFIED'):
            operational.reader_module.KisEquityReader(fake).observe(contract, side='BUY', raw_open=100, protective_bps=50)
        self.assertEqual([], fake.events)
        _, kr_contract, _ = operational.fixture('KRX')
        kr = operational.FakeReader('KRX'); kr.settings.pop(policy.MODE_KEY)
        result = operational.reader_module.KisEquityReader(kr, clock=lambda: operational.NOW).observe(kr_contract, side='BUY', raw_open=100, protective_bps=50)
        self.assertEqual('KRX', result['exchange'])
        self.assertTrue(kr.events)


if __name__ == '__main__':
    unittest.main()
