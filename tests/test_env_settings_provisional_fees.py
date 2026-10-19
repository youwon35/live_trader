"""Fee settings stay provisional and validate before any secret mutation."""
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from live_trader import env_settings
from live_trader.kis_provisional_fees import (
    FEE_SETTING_DEFAULTS, KisProvisionalFeeError, build_provisional_fee_profile,
)


class MemoryStore:
    def __init__(self):
        self.values = {}
        self.mutations = []
    def get(self, key):
        return self.values.get(key, '')
    def set(self, key, value):
        self.values[key] = value
        self.mutations.append(('set', key))
    def migrate_plaintext(self, key, value):
        self.set(key, value)


class ProvisionalFeeSettingsTests(unittest.TestCase):
    def test_all_fee_text_snapshots_populate_real_defaults_and_unconfirmed_metadata(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(env_settings, 'read_env_file', return_value={}), \
             patch.object(env_settings, 'live_secret_store', return_value=MemoryStore()):
            fields = {field['key']: field for field in env_settings.env_settings_snapshot()['fields']}
        for key, default in FEE_SETTING_DEFAULTS.items():
            self.assertEqual((fields[key]['kind'], fields[key]['value'], fields[key]['default']), ('text', default, default))
            self.assertTrue(fields[key]['configured'])
            self.assertIs(fields[key]['confirmationRequired'], True)
            self.assertIs(fields[key]['estimateOnly'], True)
            self.assertEqual(fields[key]['status'], 'PROVISIONAL_CONFIRMATION_REQUIRED')
            self.assertFalse(fields[key]['orderAuthorityGranted'])
            self.assertFalse(fields[key]['actualFeeObserved'])
            self.assertTrue(fields[key]['sourceUrl'].startswith('https://www.truefriend.com/'))
        self.assertNotIn('estimateOnly', fields['BINANCE_BASE_URL'])

    def test_one_invalid_fee_rejects_before_store_migration_or_file_write(self):
        with patch.object(env_settings, 'read_env_file', return_value={'KIS_APP_KEY': 'synthetic-legacy'}), \
             patch.object(env_settings, 'live_secret_store') as store, \
             patch.object(env_settings, 'write_env_file') as write:
            with self.assertRaises(KisProvisionalFeeError):
                env_settings.save_env_settings({'KIS_APP_KEY': 'synthetic-new',
                    'KIS_ESTIMATED_KRX_STOCK_FEE_PERCENT': '0.01',
                    'KIS_ESTIMATED_US_FEE_PERCENT': 'NaN'})
            store.assert_not_called()
            write.assert_not_called()

    def test_invalid_existing_fee_is_not_rewritten_when_saving_another_field(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(env_settings, 'read_env_file',
                return_value={'KIS_ESTIMATED_US_FEE_PERCENT': '-1'}), \
             patch.object(env_settings, 'live_secret_store') as store, \
             patch.object(env_settings, 'write_env_file') as write:
            with self.assertRaises(KisProvisionalFeeError):
                env_settings.save_env_settings({'BINANCE_BASE_URL': 'https://api.binance.com'})
            store.assert_not_called()
            write.assert_not_called()

    def test_save_reload_keeps_exact_rate_precision_and_never_confirms_it(self):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / 'synthetic-settings.env'
            store = MemoryStore()
            with patch.dict(os.environ, {}, clear=True), patch.object(env_settings, 'ENV_PATH', path), \
                 patch.object(env_settings, 'live_secret_store', return_value=store):
                snapshot = env_settings.save_env_settings({'KIS_ESTIMATED_US_FEE_PERCENT': '0.123456789012'})
                values = env_settings.read_env_file()
                self.assertEqual(values['KIS_ESTIMATED_US_FEE_PERCENT'], '0.123456789012')
                fields = {field['key']: field for field in snapshot['fields']}
                self.assertEqual(fields['KIS_ESTIMATED_US_FEE_PERCENT']['value'], '0.123456789012')
                self.assertTrue(fields['KIS_ESTIMATED_US_FEE_PERCENT']['confirmationRequired'])
                self.assertFalse(fields['KIS_ESTIMATED_US_FEE_PERCENT']['actualFeeObserved'])
                self.assertEqual(store.mutations, [])
                profile = build_provisional_fee_profile(values)
                self.assertFalse(profile['accountFeeRateApproved'])
                self.assertFalse(profile['orderAuthorityGranted'])
                snapshot = env_settings.save_env_settings({'KIS_ESTIMATED_US_FEE_PERCENT': ''})
                fields = {field['key']: field for field in snapshot['fields']}
                self.assertEqual(fields['KIS_ESTIMATED_US_FEE_PERCENT']['value'], '0.25')
                self.assertTrue(fields['KIS_ESTIMATED_US_FEE_PERCENT']['confirmationRequired'])


if __name__ == '__main__':
    unittest.main()
