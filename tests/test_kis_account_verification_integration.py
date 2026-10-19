"""Normal preparation wiring with synthetic GETs; no private settings or network."""
from copy import deepcopy
import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import test_paper_candidate_inbox as base
import test_kis_usd_preparation as sources
import test_kis_account_inventory_observation as inventory_fixture
import test_kis_settlement_history_preparation as history_fixture

account = importlib.import_module(base.PACKAGE + '.read_only_account')
service = importlib.import_module(base.PACKAGE + '.read_only_preparation')
envelopes = importlib.import_module(base.PACKAGE + '.kis_account_verification')


class AccountVerificationIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.transport = Mock()
        self.reader = account.ReadOnlyBrokerReader(dict(inventory_fixture.SETTINGS), transport=self.transport)
        self.reader._kis_token = Mock(return_value='SYNTHETIC_TOKEN')
        self.reader._pace_kis = Mock()
        self.reader.observe_preparation = Mock(return_value={
            'status': 'OBSERVED', 'account': 'AVAILABLE', 'openOrders': 'UNKNOWN',
            'orderability': 'UNKNOWN', 'fundsCheck': 'UNKNOWN'})
        self.reader.read = self.reader.observe_preparation
        # Other diagnostics are covered independently. This suite counts only
        # the new inventory reads and checks their full normal service wiring.
        self.reader.observe_kis_integrated_financial_preparation = Mock(return_value={})
        self.reader.observe_kis_usd_financial_preparation = Mock(return_value={})
        def request(provider, method, path, **kwargs):
            self.assertEqual((provider, method), ('kis', 'GET'))
            spec = next(row for row in inventory_fixture.inventory.SCOPE_SPECS if row['path'] == path)
            return inventory_fixture.response(spec['scopeId'])
        self.transport.request.side_effect = request
        self.select('AAPL')

    def select(self, symbol):
        sources.make_source(self.root, symbol=symbol)
        self.request = {'source': service.list_preparation_sources(roots=[self.root])[0]['request'],
            'draft': {'instanceId': 'usd-instance', 'side': 'BUY', 'quantity': '1', 'limitPrice': '10'},
            'readAccount': True}

    def preview(self):
        return service.prepare_read_only(self.request, roots=[self.root], reader=self.reader)

    def test_both_kis_lanes_collect_four_fixed_scopes_once_without_writes_or_authority(self):
        for symbol in ('AAPL', '005930'):
            with self.subTest(symbol=symbol):
                self.select(symbol)
                self.transport.reset_mock()
                before = base.file_bytes(self.root)
                result = self.preview()
                self.assertEqual(before, base.file_bytes(self.root))
                self.assertEqual(self.transport.request.call_count, 4)
                diagnostic = result['observation']['accountInventoryDiagnostics']
                self.assertEqual(diagnostic['status'], 'OBSERVED_LIMITED_SCOPES')
                self.assertEqual(len(diagnostic['inventory']['scopes']), 4)
                self.assertEqual(len(diagnostic['inventory']['uncoveredScopes']), 7)
                self.assertFalse(diagnostic['fullAccountReady'])
                self.assertFalse(result['executable'])
                self.assertEqual(result['ordersSubmitted'], 0)
                check = next(row for row in result['checks'] if row['code'] == 'KIS_ACCOUNT_INVENTORY_COVERAGE')
                self.assertEqual(check['status'], 'UNKNOWN')
                public = json.dumps(result)
                for value in ('SYNTHETIC_KEY', 'SYNTHETIC_SECRET', 'SYNTHETIC_TOKEN',
                              'PRIVATE_REMOTE_MESSAGE', '987654.3210', '12345678', 'PRIVATE_KR_NAME'):
                    self.assertNotIn(value, public)

    def test_read_account_false_never_calls_inventory_or_any_other_account_reader(self):
        self.request['readAccount'] = False
        result = self.preview()
        self.transport.request.assert_not_called()
        self.reader.observe_preparation.assert_not_called()
        self.assertNotIn('accountInventoryDiagnostics', result['observation'])

    def test_wrong_instance_fails_before_get(self):
        self.request['draft']['instanceId'] = 'wrong'
        with self.assertRaises(ValueError):
            self.preview()
        self.transport.request.assert_not_called()

    def test_custom_reader_nested_private_or_positive_flag_is_discarded(self):
        good = self.reader.observe_kis_account_inventory_preparation()
        for change in ('private', 'authority', 'count'):
            bad = deepcopy(good)
            if change == 'private': bad['inventory']['scopes'][0]['raw'] = 'PRIVATE_LEAK'
            elif change == 'authority': bad['fullAccountReady'] = True
            else: bad['inventory']['scopes'][0]['rowCount'] += 1
            self.reader.observe_kis_account_inventory_preparation = Mock(return_value=bad)
            result = self.preview()
            self.assertEqual(result['observation']['accountInventoryDiagnostics'], envelopes.inventory_preparation())
            self.assertNotIn('PRIVATE_LEAK', json.dumps(result))

    def test_inventory_error_is_fixed_unavailable_and_does_not_retry(self):
        self.transport.request.side_effect = RuntimeError('PRIVATE_REMOTE_FAILURE')
        result = self.preview()
        self.transport.request.assert_called_once()
        self.assertEqual(result['observation']['accountInventoryDiagnostics'], envelopes.inventory_preparation())
        self.assertNotIn('PRIVATE_REMOTE_FAILURE', json.dumps(result))
        check = next(row for row in result['checks'] if row['code'] == 'KIS_ACCOUNT_INVENTORY_COVERAGE')
        self.assertIn('기록을 확인하지 못했습니다', check['detail'])

    def test_settings_change_during_inventory_discards_outer_report(self):
        original = self.transport.request.side_effect
        def mutate(*args, **kwargs):
            result = original(*args, **kwargs)
            self.reader.settings['KIS_APP_KEY'] = 'CHANGED'
            return result
        self.transport.request.side_effect = mutate
        with self.assertRaises(ValueError):
            self.preview()
        self.transport.request.assert_called_once()

    def test_source_change_during_inventory_discards_outer_report(self):
        original = self.transport.request.side_effect
        def mutate(*args, **kwargs):
            result = original(*args, **kwargs)
            source = json.loads((self.root / 'strategy.json').read_text())
            source['sample_prices'][0] += 1
            base.write(self.root / 'strategy.json', source)
            return result
        self.transport.request.side_effect = mutate
        with self.assertRaises(ValueError):
            self.preview()

    def test_history_invalid_budget_rejected_before_auth_or_get(self):
        for kwargs in ({'start_date': '20260101', 'end_date': '20261016'},
                       {'start_date': '20260721', 'end_date': '20261016', 'max_pages': 3},
                       {'start_date': '20260721', 'end_date': '20261016', 'max_pages': True}):
            with self.assertRaises(ValueError):
                self.reader.observe_kis_settlement_history_preparation(**kwargs)
        self.reader._kis_token.assert_not_called()
        self.transport.request.assert_not_called()

    def test_history_valid_read_is_bounded_report_without_finality_or_rate_approval(self):
        self.transport.request.side_effect = None
        self.transport.request.return_value = history_fixture.page()
        result = self.reader.observe_kis_settlement_history_preparation(
            start_date=history_fixture.START, end_date=history_fixture.END)
        self.transport.request.assert_called_once()
        self.assertEqual(self.transport.request.call_args.args, ('kis', 'GET', history_fixture.history.PATH))
        self.assertEqual(result['settlementFinality'], 'UNKNOWN')
        self.assertFalse(result['accountFeeRateApproved'])
        for value in ('PRIVATE_TICKER', '90127345.6789123', '1386.4321', 'PRIVATE_REMOTE_MESSAGE'):
            self.assertNotIn(value, json.dumps(result))


if __name__ == '__main__':
    unittest.main()
