"""Opt-in route tests using synthetic sources, settings and broker responses."""
from copy import deepcopy
from datetime import datetime, timezone
import importlib
import unittest
from unittest.mock import Mock, patch
import test_read_only_preparation as base
import test_kis_reserved_order_observation as facts

service, account = base.service, base.account
envelope = importlib.import_module(base.base.PACKAGE + '.kis_reserved_order_preparation')


class ReservedPreparationTests(unittest.TestCase):
    def setUp(self):
        self.reader = Mock()
        self.reader.settings = dict(facts.SETTINGS)
        self.reader.read.return_value = {'status': 'OBSERVED', 'account': 'AVAILABLE',
            'openOrders': 'UNKNOWN', 'orderability': 'UNKNOWN', 'fundsCheck': 'UNKNOWN'}
        self.source = {'kind': 'NON_PROMOTION', 'instruments': [
            {'instanceId': 'selected', 'broker': 'kis-kr', 'symbol': '005930'}]}
        self.request = {'source': {}, 'draft': {'instanceId': 'selected'}, 'readAccount': True}
        self.summary = facts.summary(facts.capture())
        self.reader.observe_kis_reserved_order_preparation.return_value = envelope.reserved_preparation(self.summary)
        self.source_patch = patch.object(service, 'source_snapshot', side_effect=lambda *a: deepcopy(self.source))
        self.source_patch.start()
        self.addCleanup(self.source_patch.stop)

    def run_preview(self):
        return service.prepare_read_only(self.request, roots=[], reader=self.reader)

    def test_default_does_not_request_reserved_order_reads(self):
        for explicit in (None, False):
            if explicit is not None:
                self.request['readReservedOrders'] = explicit
            result = self.run_preview()
            self.assertNotIn('reservedOrderDiagnostics', result['observation'])
        self.reader.observe_kis_reserved_order_preparation.assert_not_called()

    def test_explicit_observation_remains_unknown_and_never_grants_authority(self):
        self.request['readReservedOrders'] = True
        result = self.run_preview()
        self.reader.observe_kis_reserved_order_preparation.assert_called_once_with()
        self.assertEqual(self.summary, result['observation']['reservedOrderDiagnostics']['reservations'])
        check = next(v for v in result['checks'] if v['code'] == 'KIS_RESERVED_CONDITIONAL_ORDER_COVERAGE')
        self.assertEqual('UNKNOWN', check['status'])
        for key in ('executable', 'authorityGranted', 'promotionEligible'):
            self.assertIs(result[key], False)
        self.assertEqual(0, result['ordersSubmitted'])

    def test_opt_in_wrong_lane_no_account_or_invalid_boolean_rejected_before_get(self):
        for key, value in [('readReservedOrders', 'true'), ('readReservedOrders', 1), ('readAccount', False)]:
            request = {**self.request, 'readReservedOrders': True, key: value}
            with self.assertRaises(ValueError):
                service.prepare_read_only(request, roots=[], reader=self.reader)
        self.request['readReservedOrders'] = True
        self.source['instruments'][0]['broker'] = 'upbit'
        with self.assertRaises(ValueError):
            self.run_preview()
        self.reader.read.assert_not_called()
        self.reader.observe_kis_reserved_order_preparation.assert_not_called()

    def test_malformed_or_private_diagnostic_becomes_unavailable(self):
        self.request['readReservedOrders'] = True
        for mutate in (lambda d: d.update(privateAccount='PRIVATE'),
                lambda d: d.update(orderAuthorityGranted=True),
                lambda d: d['reservations']['scopes'][0].update(orderId='PRIVATE'),
                lambda d: d['reservations'].update(completeConditionalOrdersVerified=True)):
            value = envelope.reserved_preparation(self.summary)
            mutate(value)
            self.reader.observe_kis_reserved_order_preparation.return_value = value
            result = self.run_preview()
            self.assertEqual(envelope.reserved_preparation(), result['observation']['reservedOrderDiagnostics'])
            self.assertNotIn('PRIVATE', str(result))

    def test_source_or_settings_change_discards_entire_preparation(self):
        self.request['readReservedOrders'] = True
        original = envelope.reserved_preparation(self.summary)
        def mutate_settings():
            self.reader.settings['KIS_ACCOUNT_NO'] = '87654321-01'
            return original
        self.reader.observe_kis_reserved_order_preparation.side_effect = mutate_settings
        with self.assertRaises(ValueError):
            self.run_preview()
        self.reader.settings = dict(facts.SETTINGS)
        def mutate_source():
            self.source['changed'] = True
            return original
        self.reader.observe_kis_reserved_order_preparation.side_effect = mutate_source
        with self.assertRaises(ValueError):
            self.run_preview()

    def test_date_window_uses_kst_calendar_and_includes_exact_thirty_days(self):
        self.assertEqual({'start_date': '20260920', 'end_date': '20261019'},
            envelope.recent_reserved_window(datetime(2026, 10, 18, 15, 0, tzinfo=timezone.utc)))
        with self.assertRaises(ValueError):
            envelope.recent_reserved_window(datetime(2026, 10, 19))


class ReservedTransportTests(unittest.TestCase):
    def test_only_exact_get_routes_and_transaction_ids_are_permitted(self):
        transport = account.ReadOnlyTransport()
        transport.opener = Mock()
        for path in ('/uapi/domestic-stock/v1/trading/order-resv-ccnl',
                     '/uapi/overseas-stock/v1/trading/order-resv-list'):
            for method in ('POST', 'PUT', 'DELETE', 'PATCH'):
                with self.assertRaises(account.ReadBoundaryError):
                    transport.request('kis', method, path, headers={'tr_id': account.KIS_TR_IDS[path]})
            with self.assertRaises(account.ReadBoundaryError):
                transport.request('kis', 'GET', path, headers={'tr_id': 'TTTC0012U'})
            with self.assertRaises(account.ReadBoundaryError):
                transport.request('kis', 'GET', path + '/', headers={'tr_id': account.KIS_TR_IDS[path]})
        transport.opener.open.assert_not_called()


if __name__ == '__main__':
    unittest.main()
