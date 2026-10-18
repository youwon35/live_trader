"""Pure accounting-role regressions. Synthetic records never qualify money."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib
from pathlib import Path
import socket
import sys
from types import ModuleType
import unittest
from unittest.mock import patch

NAME = '_pure_shared_krw_readiness_test'
package = ModuleType(NAME)
package.__path__ = [str(Path(__file__).resolve().parents[1] / 'live_trader')]
sys.modules[NAME] = package
module = importlib.import_module(NAME + '.kis_shared_krw_readiness')
observations = importlib.import_module(NAME + '.kis_integrated_margin_observation')
NOW = datetime(2026, 10, 19, 1, 0, tzinfo=timezone.utc)


class FakeReader:
    def __init__(self, *, continuation='D'):
        self.settings = {'KIS_APP_KEY': 'synthetic-key', 'KIS_APP_SECRET': 'synthetic-secret',
            'KIS_ACCOUNT_NO': '12345678', 'KIS_ACCOUNT_PRODUCT_CODE': '01',
            'KIS_BASE_URL': 'https://openapi.koreainvestment.com:9443',
            'KIS_ENV': 'real', 'KIS_OVERSEAS_FUNDING_MODE': 'INTEGRATED_MARGIN'}
        self.continuation = continuation
        self.calls = 0
    def _credentials(self, broker):
        assert broker == 'kis'
    def _kis_token(self):
        return 'synthetic-memory-token'
    def _pace_kis(self):
        return None
    def _get(self, provider, path, *, query, headers):
        assert provider == 'kis' and path == observations.PATH
        assert all(query[k] == v for k, v in observations.PUBLIC_QUERY.items())
        self.calls += 1
        output = {field: '0' for field in observations.ROLE_FIELDS}
        output['stck_itgr_cash100_ord_psbl_amt'] = '1000000'
        output['usd_itgr_ord_psbl_amt'] = '1000000'
        return {'rt_cd': '0', 'output': output}, {
            'tr_id': observations.TR_ID, 'tr_cont': self.continuation}


def capture(*, continuation='D'):
    reader = FakeReader(continuation=continuation)
    times = iter([(NOW - timedelta(seconds=2)).isoformat(),
                  (NOW - timedelta(seconds=1)).isoformat()])
    ticks = iter([1.0, 2.0])
    result = observations.KisIntegratedMarginObservationReader(reader,
        utc_now=lambda: next(times), monotonic=lambda: next(ticks)).observe()
    assert reader.calls == 1
    return result


def owned_book(observation, *, currency='KRW', deployment='domestic', cash='1000'):
    binding = observation['accountBinding']
    scope = {'version': 'kis-owned-capital-v2', 'deploymentId': deployment,
        'accountFingerprint': binding['accountFingerprint'],
        'credentialConfigurationHash': binding['credentialConfigurationHash'],
        'accountProfileId': binding['profileId'], 'reviewBindingHash': 'a' * 64,
        'capitalPercent': '5', 'quoteCurrency': currency,
        'members': [{'strategyInstanceId': deployment + '-member', 'sourceArtifactHash': 'b' * 64,
            'portfolioHash': 'c' * 64, 'symbol': '005930' if currency == 'KRW' else 'AAPL',
            'quoteCurrency': currency}]}
    scope['scopeHash'] = module._hash(scope)
    projection = {'equityScope': 'OWNED_DEPLOYMENT_ONLY',
        'capitalAccounting': 'OWNED_CONSERVATIVE_COST_RESERVE',
        'deploymentEquity': cash, 'ownedCash': cash, 'grossCash': cash,
        'costReserveOutstanding': '0', 'ownedPositions': {deployment + '-member': '0'},
        'capitalScopeHash': scope['scopeHash'], 'fillCount': 0,
        'actualFeesReconciled': False,
        'netPerformanceStatus': 'ESTIMATED_UNTIL_ACTUAL_COST_RECONCILIATION',
        'orderAuthorityGranted': False}
    return {'scope': scope, 'projection': projection,
        'feePolicy': {'reserveRate': '0.01', 'actualPersonalFeeObserved': False}}


class SharedKrwReadinessTests(unittest.TestCase):
    def setUp(self):
        self.no_network = patch.object(socket.socket, 'connect',
            side_effect=AssertionError('offline test forbids sockets'))
        self.no_network.start()
        self.addCleanup(self.no_network.stop)
        self.observation = capture()

    def inspect(self, **values):
        args = {'integrated_observation': self.observation, 'owned_books': [],
            'open_orders': [], 'now': NOW}
        args.update(values)
        result = module.inspect_shared_krw_readiness(**args)
        self.assertEqual(result['status'], 'BLOCKED')
        for key in module._FALSE:
            self.assertIs(result[key], False, key)
        self.assertIsNone(result['sharedKrwAvailableAmount'])
        self.assertFalse(result['currencyConversionPerformed'])
        return result

    def test_same_reported_capacity_and_separate_currency_books_are_never_added(self):
        krw = owned_book(self.observation, cash='1000000')
        usd = owned_book(self.observation, currency='USD', deployment='overseas', cash='750')
        result = self.inspect(owned_books=[krw, usd])
        self.assertTrue(result['checks']['allOwnedBookBindingsMatch'])
        self.assertEqual(result['currencyBooks']['KRW']['ownedCash'], '1000000')
        self.assertEqual(result['currencyBooks']['USD']['ownedCash'], '750')
        self.assertFalse(result['brokerCapacityAndOwnedCashCombined'])
        self.assertIn('KIS_SHARED_KRW_INDEPENDENT_CURRENCY_ALLOCATIONS_NOT_SHARED_POOL', result['reasons'])
        self.assertIn('KIS_SHARED_KRW_VERIFIED_BASELINE_PRODUCER_UNAVAILABLE', result['reasons'])

    def test_duplicate_deployment_cannot_double_count_cash_or_hide_behind_currency(self):
        krw = owned_book(self.observation)
        duplicate = owned_book(self.observation, currency='USD')
        result = self.inspect(owned_books=[krw, duplicate])
        self.assertEqual(result['currencyBooks'], {})
        self.assertFalse(result['checks']['localOwnedBooksStructurallyConsistent'])
        self.assertIn('KIS_SHARED_KRW_DUPLICATE_DEPLOYMENT_ALLOCATION', result['reasons'])

    def test_unavailable_or_unknown_pending_is_never_empty_account_scope(self):
        missing = self.inspect(open_orders=None)
        self.assertIsNone(missing['localOrderRowCount'])
        self.assertFalse(missing['checks']['localOrderDependenciesProvided'])
        unknown = self.inspect(open_orders=[{'idempotency_key': 'one'}])
        self.assertIn('KIS_SHARED_KRW_PENDING_STATE_UNKNOWN', unknown['reasons'])
        empty = self.inspect(open_orders=[])
        self.assertEqual(empty['localOrderRowCount'], 0)
        for result in (missing, unknown, empty):
            self.assertFalse(result['checks']['completeAccountWorkingOrdersVerified'])

    def test_partial_future_stale_and_slow_get_remain_explicitly_unqualified(self):
        partial = self.inspect(integrated_observation=capture(continuation='M'))
        self.assertIn('KIS_SHARED_KRW_INTEGRATED_RESPONSE_PARTIAL_OR_UNKNOWN', partial['reasons'])
        for clock in (NOW - timedelta(seconds=3), NOW + timedelta(seconds=16), NOW.replace(tzinfo=None)):
            result = self.inspect(now=clock)
            self.assertFalse(result['checks']['localRetrievalWithinExisting15SecondBoundary'])
        slow = deepcopy(self.observation)
        slow['startedAt'] = (NOW - timedelta(seconds=20)).isoformat()
        slow['contentHash'] = module._hash({k: v for k, v in slow.items() if k != 'contentHash'})
        self.assertFalse(self.inspect(integrated_observation=slow)['checks']['localRetrievalWithinExisting15SecondBoundary'])

    def test_current_profile_and_book_state_and_fee_changes_bind_dependencies(self):
        book = owned_book(self.observation)
        orders = [{'broker_id': 'kis', 'idempotency_key': 'request-one', 'state': 'accepted'}]
        baseline = self.inspect(owned_books=[book], open_orders=orders)
        changed_fee = deepcopy(book)
        changed_fee['feePolicy']['reserveRate'] = '0.02'
        fee_result = self.inspect(owned_books=[changed_fee], open_orders=orders)
        self.assertNotEqual(baseline['dependencyHash'], fee_result['dependencyHash'])
        changed_state = deepcopy(orders)
        changed_state[0]['state'] = 'filled'
        state_result = self.inspect(owned_books=[book], open_orders=changed_state)
        self.assertNotEqual(baseline['dependencyHash'], state_result['dependencyHash'])
        mismatch = deepcopy(book)
        mismatch['scope']['credentialConfigurationHash'] = 'f' * 64
        mismatch['scope']['scopeHash'] = module._hash({k: v for k, v in mismatch['scope'].items() if k != 'scopeHash'})
        mismatch['projection']['capitalScopeHash'] = mismatch['scope']['scopeHash']
        result = self.inspect(owned_books=[mismatch])
        self.assertFalse(result['checks']['allOwnedBookBindingsMatch'])
        self.assertEqual(result['currencyBooks'], {})
        self.assertIn('KIS_SHARED_KRW_CURRENT_ACCOUNT_OR_CONFIGURATION_BINDING_UNVERIFIED', result['reasons'])

    def test_estimated_fee_holdback_is_once_per_native_book_and_not_actual_fee(self):
        book = owned_book(self.observation)
        book['projection'].update(grossCash='1010', costReserveOutstanding='10', fillCount=1)
        result = self.inspect(owned_books=[book])
        self.assertEqual(result['currencyBooks']['KRW']['ownedCash'], '1000')
        self.assertEqual(result['currencyBooks']['KRW']['estimatedCostHoldback'], '10')
        self.assertFalse(result['estimatedFeesAreActualFees'])
        invalid = deepcopy(book)
        invalid['projection']['ownedCash'] = '990'
        self.assertIn('KIS_SHARED_KRW_ESTIMATED_HOLDBACK_IDENTITY_INVALID',
            self.inspect(owned_books=[invalid])['reasons'])

    def test_public_summary_strips_private_amounts_and_rejects_authority_or_remote_text(self):
        result = self.inspect(owned_books=[owned_book(self.observation, cash='918273')])
        summary = module.safe_shared_krw_readiness_summary(result)
        self.assertNotIn('918273', repr(summary))
        for key in ('currencyBooks', 'dependencyHashes', 'accountBinding', 'ownedCash', 'feePolicy'):
            self.assertNotIn(key, summary)
        for key in module._FALSE:
            tampered = {**result, key: True}
            tampered['contentHash'] = module._hash({k: v for k, v in tampered.items() if k != 'contentHash'})
            with self.assertRaises(ValueError):
                module.safe_shared_krw_readiness_summary(tampered)
        tampered = {**result, 'reasons': ['private broker text 918273']}
        tampered['contentHash'] = module._hash({k: v for k, v in tampered.items() if k != 'contentHash'})
        with self.assertRaises(ValueError):
            module.safe_shared_krw_readiness_summary(tampered)


if __name__ == '__main__':
    unittest.main()
