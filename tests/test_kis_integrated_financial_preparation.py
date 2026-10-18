"""Normal preparation wiring with synthetic GETs and read-only book inventory."""
from copy import deepcopy
import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import test_kis_integrated_margin_observation as fixture
import test_kis_usd_preparation as preparation

ro = fixture.ro
local = importlib.import_module(fixture.PACKAGE + '.kis_shared_krw_local')


def inventory():
    return {'status': 'UNKNOWN', 'ownedBooks': None, 'reasons': ['KIS_LOCAL_OWNED_DATABASE_MISSING_NOT_ZERO_FUNDS'],
        'localGeneration': None, 'dependencyHash': None,
        'sourceMeaning': 'EXISTING_LOCAL_OWNED_BOOKS_NOT_ACCOUNT_WIDE_CASH'}


class IntegratedFinancialPreparationTests(unittest.TestCase):
    def reader(self):
        reader = ro.ReadOnlyBrokerReader(dict(fixture.SETTINGS), transport=Mock())
        reader._kis_token = Mock(return_value='SYNTHETIC_TOKEN')
        reader._pace_kis = Mock()
        reader._get = Mock(return_value=(fixture.reported_body(), {'tr_id': fixture.integrated.TR_ID, 'tr_cont': ''}))
        return reader

    def test_real_collector_wiring_preserves_reference_roles_and_strips_private_values(self):
        reader = self.reader()
        with patch.object(local, 'read_existing_owned_books', return_value=inventory()) as books:
            result = reader.observe_kis_integrated_financial_preparation()
        self.assertEqual('REPORTED_REFERENCE_AND_LOCAL_DEPENDENCIES_ONLY', result['status'])
        self.assertEqual(34, result['integratedObservation']['reportedNumericFieldCount'])
        self.assertEqual('UNKNOWN', result['integratedObservation']['paginationStatus'])
        self.assertEqual('BLOCKED', result['sharedKrwReadiness']['status'])
        self.assertIsNone(result['sharedKrwReadiness']['sharedKrwAvailableAmount'])
        self.assertFalse(result['productionReady']); self.assertFalse(result['orderAuthorityGranted'])
        for private in ('123456.7800', 'SYNTHETIC_REMOTE', 'SYNTHETIC_KEY', '12345678', 'accountFingerprint'):
            self.assertNotIn(private, json.dumps(result))
        self.assertEqual(1, books.call_count)
        reader.transport.request.assert_not_called()
        self.assertEqual(1, reader._get.call_count)
        self.assertEqual(('logs', 'kis_owned_capital_v2.sqlite3'), Path(books.call_args.args[0]).parts[-2:])

    def test_foreign_cash_is_zero_reads_and_mode_mutation_discards_private_capture(self):
        reader = self.reader(); reader.settings['KIS_OVERSEAS_FUNDING_MODE'] = 'FOREIGN_CASH'
        self.assertEqual('NOT_REQUESTED_FOR_FOREIGN_CASH', reader.observe_kis_integrated_financial_preparation()['status'])
        reader._get.assert_not_called()
        reader.settings['KIS_OVERSEAS_FUNDING_MODE'] = 'INTEGRATED_MARGIN'
        def mutate(*args, **kwargs):
            reader.settings['KIS_OVERSEAS_FUNDING_MODE'] = 'FOREIGN_CASH'
            return fixture.reported_body(), {'tr_id': fixture.integrated.TR_ID}
        reader._get.side_effect = mutate
        with patch.object(local, 'read_existing_owned_books') as books:
            report = reader.observe_kis_integrated_financial_preparation()
        self.assertEqual('UNAVAILABLE', report['status']); self.assertIsNone(report['integratedObservation'])
        books.assert_not_called()

    def test_source_and_saved_setting_races_reject_normal_preparation_report(self):
        for race in ('SOURCE', 'SETTINGS'):
            with self.subTest(race=race), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); preparation.make_source(root)
                reader = self.reader()
                reader._kis = Mock(return_value={'account': 'AVAILABLE', 'openOrders': 'UNKNOWN', 'orderability': 'UNKNOWN'})
                reader.observe_kis_usd_financial_preparation = Mock(return_value=preparation.usd.build_usd_preparation_diagnostic())
                original = reader.observe_kis_integrated_financial_preparation
                def change():
                    result = original()
                    if race == 'SOURCE':
                        path = root / 'strategy.json'; raw = json.loads(path.read_text()); raw['sample_prices'][0] += 1
                        preparation.base.write(path, raw)
                    else: reader.settings['KIS_OVERSEAS_FUNDING_MODE'] = 'FOREIGN_CASH'
                    return result
                reader.observe_kis_integrated_financial_preparation = change
                request = {'source': preparation.service.list_preparation_sources(roots=[root])[0]['request'],
                    'draft': {'instanceId': 'usd-instance', 'side': 'BUY', 'quantity': '1', 'limitPrice': '10'}, 'readAccount': True}
                with patch.object(local, 'read_existing_owned_books', return_value=inventory()):
                    with self.assertRaises(ValueError): preparation.service.prepare_read_only(request, roots=[root], reader=reader)

    def test_normal_kr_and_us_preparation_add_blocked_shared_check_without_readiness(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for symbol in ('AAPL', '005930'):
                preparation.make_source(root, symbol=symbol)
                reader = self.reader()
                reader._kis = Mock(return_value={'account': 'AVAILABLE', 'openOrders': 'UNKNOWN', 'orderability': 'UNKNOWN'})
                reader.observe_kis_usd_financial_preparation = Mock(return_value=preparation.usd.build_usd_preparation_diagnostic())
                request = {'source': preparation.service.list_preparation_sources(roots=[root])[0]['request'],
                    'draft': {'instanceId': 'usd-instance', 'side': 'BUY', 'quantity': '1', 'limitPrice': '10'}, 'readAccount': True}
                with patch.object(local, 'read_existing_owned_books', return_value=inventory()):
                    result = preparation.service.prepare_read_only(request, roots=[root], reader=reader)
                checks = {row['code']: row for row in result['checks']}
                self.assertEqual('BLOCKED', checks['KIS_SHARED_KRW_RESERVATION']['status'])
                self.assertEqual('UNKNOWN', checks['KIS_INTEGRATED_MARGIN_REFERENCE']['status'])
                self.assertFalse(result['executable']); self.assertEqual(0, result['ordersSubmitted'])

    def test_malformed_nested_custom_reader_is_discarded_at_normal_report_boundary(self):
        import test_kis_integrated_financial_public_diagnostic as public_fixture
        for nested in ('integratedObservation', 'sharedKrwReadiness', 'localOwnedInventory', 'EXCEPTION'):
            with self.subTest(nested=nested), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); preparation.make_source(root)
                reader = self.reader()
                reader._kis = Mock(return_value={'account': 'AVAILABLE', 'openOrders': 'UNKNOWN', 'orderability': 'UNKNOWN'})
                reader.observe_kis_usd_financial_preparation = Mock(return_value=preparation.usd.build_usd_preparation_diagnostic())
                malformed = public_fixture.reported()
                if nested == 'EXCEPTION':
                    reader.observe_kis_integrated_financial_preparation = Mock(side_effect=RuntimeError('SYNTHETIC_PRIVATE_LEAK'))
                else:
                    malformed[nested]['privateBalance'] = 'SYNTHETIC_PRIVATE_LEAK'
                    reader.observe_kis_integrated_financial_preparation = Mock(return_value=malformed)
                request = {'source': preparation.service.list_preparation_sources(roots=[root])[0]['request'],
                    'draft': {'instanceId': 'usd-instance', 'side': 'BUY', 'quantity': '1', 'limitPrice': '10'}, 'readAccount': True}
                result = preparation.service.prepare_read_only(request, roots=[root], reader=reader)
                self.assertEqual('UNAVAILABLE', result['observation']['integratedFinancialDiagnostics']['status'])
                self.assertNotIn('SYNTHETIC_PRIVATE_LEAK', json.dumps(result))
                self.assertFalse(result['executable']); self.assertEqual(0, result['ordersSubmitted'])


if __name__ == '__main__': unittest.main()
