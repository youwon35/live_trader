"""Synthetic reader tests. Load actual modules without running app __init__."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib
import json
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
# These namespace packages point to actual source; no implementation functions
# are substituted. App __init__ would otherwise load the real user's env store.
PACKAGE = '_kis_inventory_actual_source_test'
namespace = types.ModuleType(PACKAGE)
namespace.__path__ = [str(ROOT / 'live_trader')]
sys.modules[PACKAGE] = namespace
if 'trading_runtime' not in sys.modules:
    runtime = types.ModuleType('trading_runtime')
    runtime.__path__ = [str(ROOT.parents[1] / 'packages/trading_runtime/trading_runtime')]
    sys.modules['trading_runtime'] = runtime
inventory = importlib.import_module(PACKAGE + '.kis_account_inventory_observation')

NOW = datetime(2026, 10, 19, 1, 0, tzinfo=timezone.utc)
SETTINGS = {'KIS_APP_KEY': 'SYNTHETIC_KEY', 'KIS_APP_SECRET': 'SYNTHETIC_SECRET',
    'KIS_ACCOUNT_NO': '12345678-01', 'KIS_ACCOUNT_PRODUCT_CODE': '01',
    'KIS_BASE_URL': 'https://openapi.koreainvestment.com:9443', 'KIS_ENV': 'real',
    'KIS_OVERSEAS_FUNDING_MODE': 'INTEGRATED_MARGIN'}


def response(scope_id, *, rows=None, continuation='D', cursor=('NEXT_FK', 'NEXT_NK')):
    spec = next(row for row in inventory.SCOPE_SPECS if row['scopeId'] == scope_id)
    if rows is None:
        rows = {
            'KR_BASE_POSITIONS': [{'pdno': '005930', 'hldg_qty': '7', 'loan_dt': '', 'prdt_name': 'PRIVATE_KR_NAME'}],
            'KR_REVOCABLE_ORDERS': [{'ord_gno_brno': '06010', 'odno': '000001', 'pdno': '000660', 'ord_qty': '3'}],
            'US_POSITIONS': [{'ovrs_pdno': 'F', 'ovrs_excg_cd': 'NYSE', 'tr_crcy_cd': 'USD', 'ovrs_cblc_qty': '2'},
                {'ovrs_pdno': 'SPY', 'ovrs_excg_cd': 'AMEX', 'tr_crcy_cd': 'USD', 'ovrs_cblc_qty': '4'}],
            'US_REGULAR_ORDERS': [{'ord_dt': '20261016', 'ord_gno_brno': '02111', 'odno': '000002',
                'pdno': 'MSFT', 'nccs_qty': '1', 'ovrs_excg_cd': 'NASD', 'tr_crcy_cd': 'USD'}],
        }[scope_id]
    body = {'rt_cd': '0', 'msg1': 'PRIVATE_REMOTE_MESSAGE', spec['rowsKey']: deepcopy(rows)}
    if spec['rowsKey'] == 'output1':
        body['output2'] = [{'PRIVATE_AMOUNT': '987654.3210'}]
    if continuation in ('F', 'M'):
        body['ctx_area_fk' + spec['cursorLength']], body['ctx_area_nk' + spec['cursorLength']] = cursor
    return body, {'tr_id': spec['trId'], 'tr_cont': continuation}


class Reader:
    def __init__(self):
        self.settings = dict(SETTINGS)
        self.responses = {row['path']: [response(row['scopeId'])] for row in inventory.SCOPE_SPECS}
        self.requests = []
        self.credentials = self.tokens = self.paces = 0
        self.on_token = self.on_pace = self.on_get = None
    def _credentials(self, provider):
        assert provider == 'kis'
        self.credentials += 1
    def _kis_token(self):
        self.tokens += 1
        if self.on_token:
            self.on_token()
        return 'SYNTHETIC_TOKEN'
    def _pace_kis(self):
        self.paces += 1
        if self.on_pace:
            self.on_pace()
    def _get(self, provider, path, **kwargs):
        self.requests.append((provider, path, deepcopy(kwargs)))
        if self.on_get:
            self.on_get()
        queue = self.responses[path]
        if not queue:
            raise AssertionError('UNEXPECTED_ADDITIONAL_GET')
        return deepcopy(queue.pop(0))


class Clock:
    def __init__(self, *, step=1):
        self.tick = 0
        self.step = step
        self.last = 0
    def utc(self):
        self.last = self.tick
        self.tick += self.step
        return (NOW + timedelta(seconds=self.last)).isoformat()
    def mono(self):
        return float(self.last)


def capture(reader=None, *, step=1, **kwargs):
    clock = Clock(step=step)
    return inventory.KisAccountInventoryReader(reader or Reader(), utc_now=clock.utc, monotonic=clock.mono).observe(**kwargs)


def summary(value):
    end = datetime.fromisoformat(value['completedAt'])
    return inventory.safe_inventory_summary(value, now=end)


def rehash(value):
    result = deepcopy(value)
    result.pop('contentHash', None)
    result['contentHash'] = inventory._hash(result)
    return result


class InventoryTests(unittest.TestCase):
    def test_constructor_has_no_io_and_four_queries_have_no_selected_symbol(self):
        reader = Reader()
        inventory.KisAccountInventoryReader(reader)
        self.assertEqual((reader.credentials, reader.tokens, reader.paces, reader.requests), (0, 0, 0, []))
        value = capture(reader)
        self.assertEqual(len(reader.requests), 4)
        self.assertEqual((reader.credentials, reader.tokens, reader.paces), (1, 1, 4))
        for request, spec in zip(reader.requests, inventory.SCOPE_SPECS):
            self.assertEqual(request[:2], ('kis', spec['path']))
            self.assertEqual(request[2]['headers']['tr_id'], spec['trId'])
            self.assertEqual(request[2]['query'], {'CANO': '12345678', 'ACNT_PRDT_CD': '01',
                **spec['baseQuery'], 'CTX_AREA_FK' + spec['cursorLength']: '', 'CTX_AREA_NK' + spec['cursorLength']: ''})
            self.assertNotIn('PDNO', request[2]['query'])
        facts = inventory.validate_inventory_capture(value)
        self.assertEqual(facts[2]['rowCount'], 2)
        self.assertEqual(value['scopes'][2]['pages'][0]['body']['output1'][1]['ovrs_pdno'], 'SPY')

    def test_all_pages_exact_both_cursors_one_cursor_valid_and_continuation_header(self):
        for cursor in (('FK2', 'NK2'), ('FK2', ''), ('', 'NK2')):
            reader = Reader()
            path = inventory.SCOPE_SPECS[0]['path']
            reader.responses[path] = [response('KR_BASE_POSITIONS', continuation='M', cursor=cursor),
                response('KR_BASE_POSITIONS', rows=[{'pdno': '000660', 'hldg_qty': '9'}])]
            value = capture(reader)
            second = reader.requests[1][2]
            self.assertEqual(second['headers']['tr_cont'], 'N')
            self.assertEqual((second['query']['CTX_AREA_FK100'], second['query']['CTX_AREA_NK100']), cursor)
            result = summary(value)
            self.assertEqual((result['scopes'][0]['rowCount'], result['scopes'][0]['pageCount']), (2, 2))
            self.assertEqual(result['scopes'][0]['paginationStatus'], 'COMPLETE')

    def test_default_two_page_cap_is_partial_and_unknown_header_is_unknown_not_empty(self):
        reader = Reader()
        path = inventory.SCOPE_SPECS[0]['path']
        reader.responses[path] = [response('KR_BASE_POSITIONS', rows=[], continuation='F', cursor=('A', '')),
            response('KR_BASE_POSITIONS', rows=[], continuation='M', cursor=('B', ''))]
        reader.responses[inventory.SCOPE_SPECS[1]['path']] = [response('KR_REVOCABLE_ORDERS', rows=[], continuation='')]
        result = summary(capture(reader))
        self.assertEqual(result['scopes'][0]['paginationStatus'], 'PARTIAL')
        self.assertEqual(result['scopes'][1]['paginationStatus'], 'UNKNOWN')
        self.assertFalse(result['accountWideAbsenceAuthoritative'])
        self.assertEqual(len(reader.requests), 5)

    def test_repeated_missing_cursor_bad_header_and_bad_trid_fail_before_next_get(self):
        for bad in ('missing', 'repeated', 'header', 'trid'):
            reader = Reader()
            path = inventory.SCOPE_SPECS[0]['path']
            first = response('KR_BASE_POSITIONS', rows=[], continuation='M')
            if bad == 'missing':
                first[0].pop('ctx_area_nk100')
            elif bad == 'header':
                first[1]['tr_cont'] = 'X'
            elif bad == 'trid':
                first[1]['tr_id'] = 'WRONG'
            reader.responses[path] = [first, response('KR_BASE_POSITIONS', rows=[], continuation='F')]
            with self.assertRaises(inventory.KisAccountInventoryError):
                capture(reader)
            self.assertEqual(len(reader.requests), 2 if bad == 'repeated' else 1)

    def test_conflicting_page_identity_rejected_identical_overlap_explicit(self):
        for changed in (True, False):
            reader = Reader()
            path = inventory.SCOPE_SPECS[0]['path']
            first = response('KR_BASE_POSITIONS', continuation='M')
            second = response('KR_BASE_POSITIONS')
            if changed:
                second[0]['output1'][0]['hldg_qty'] = '99'
            reader.responses[path] = [first, second]
            if changed:
                with self.assertRaisesRegex(inventory.KisAccountInventoryError, 'IDENTITY_CONFLICT'):
                    capture(reader)
            else:
                row = summary(capture(reader))['scopes'][0]
                self.assertEqual((row['rowCount'], row['distinctIdentityCount'], row['identicalDuplicateCount']), (2, 1, 1))

    def test_account_mode_secret_and_fee_changes_discard_all_results_at_each_boundary(self):
        for stage in ('on_token', 'on_pace', 'on_get'):
            for key, replacement in (('KIS_ACCOUNT_NO', '87654321-01'), ('KIS_APP_SECRET', 'CHANGED'),
                ('KIS_OVERSEAS_FUNDING_MODE', 'FOREIGN_CASH'), ('KIS_ESTIMATED_US_FEE_PERCENT', '0.1')):
                reader = Reader()
                setattr(reader, stage, lambda r=reader, k=key, v=replacement: r.settings.__setitem__(k, v))
                with self.assertRaisesRegex(inventory.KisAccountInventoryError, 'SCOPE_CHANGED'):
                    capture(reader)
                self.assertLessEqual(len(reader.requests), 1)

    def test_invalid_scope_or_page_limit_rejected_before_token(self):
        for maximum in (0, 11, True, '2', 2.0):
            reader = Reader()
            with self.assertRaises(inventory.KisAccountInventoryError):
                capture(reader, max_pages=maximum)
            self.assertEqual(reader.tokens, 0)
        reader = Reader()
        reader.settings['KIS_BASE_URL'] = 'https://evil.invalid'
        with self.assertRaises(inventory.KisAccountInventoryError):
            capture(reader)
        self.assertEqual(reader.tokens, 0)

    def test_missing_rows_malformed_rows_and_broker_rejection_never_mean_zero(self):
        for mutation in ('missing', 'mapping', 'scalar_row', 'reject', 'numeric_success'):
            reader = Reader()
            body, headers = response('KR_BASE_POSITIONS')
            if mutation == 'missing':
                body.pop('output1')
            elif mutation == 'mapping':
                body['output1'] = {}
            elif mutation == 'scalar_row':
                body['output1'] = ['PRIVATE_BAD_VALUE']
            else:
                body['rt_cd'] = '1' if mutation == 'reject' else 0
            reader.responses[inventory.SCOPE_SPECS[0]['path']] = [(body, headers)]
            with self.assertRaises(inventory.KisAccountInventoryError) as caught:
                capture(reader)
            self.assertRegex(str(caught.exception), r'^KIS_INVENTORY_[A-Z0-9_]+$')
            self.assertNotIn('PRIVATE', str(caught.exception))

    def test_auxiliary_cash_shape_missing_and_empty_remain_unreported(self):
        for state in ('ABSENT', 'EMPTY_UNREPORTED'):
            reader = Reader()
            body, headers = response('KR_BASE_POSITIONS')
            if state == 'ABSENT':
                body.pop('output2')
            else:
                body['output2'] = []
            reader.responses[inventory.SCOPE_SPECS[0]['path']] = [(body, headers)]
            result = summary(capture(reader))
            self.assertEqual(result['scopes'][0]['auxiliaryOutputState'], state)
            self.assertFalse(result['cashAmountsDerived'])
            self.assertFalse(result['accountCashReconciled'])

    def test_fifteen_second_boundary_and_long_collection_remain_stale(self):
        value = capture()
        at_boundary = inventory.safe_inventory_summary(value, now=NOW + timedelta(seconds=15))
        past_boundary = inventory.safe_inventory_summary(value, now=NOW + timedelta(seconds=15, microseconds=1))
        self.assertEqual(at_boundary['freshnessStatus'], 'WITHIN_LOCAL_15_SECOND_BOUNDARY')
        self.assertEqual(past_boundary['freshnessStatus'], 'STALE_OR_CLOCK_INVALID')
        long_read = summary(capture(step=3))
        self.assertEqual(long_read['freshnessBoundarySeconds'], 15)
        self.assertEqual(long_read['freshnessStatus'], 'STALE_OR_CLOCK_INVALID')
        self.assertEqual(inventory.validate_inventory_summary(long_read), long_read)
        # A slow/frozen wall clock cannot hide a monotonic collection over 15s.
        altered = deepcopy(value)
        for index, page in enumerate(p for scope in altered['scopes'] for p in scope['pages']):
            page['startedMonotonic'] = index * 10.0
            page['completedMonotonic'] = index * 10.0 + 1
        altered['startedMonotonic'] = 0.0
        altered['completedMonotonic'] = 31.0
        monotonic_stale = summary(rehash(altered))
        self.assertEqual(monotonic_stale['freshnessStatus'], 'STALE_OR_CLOCK_INVALID')
        self.assertEqual(monotonic_stale['collectionMonotonicDurationSeconds'], 31.0)

    def test_every_scope_empty_does_not_qualify_full_account_or_working_order_absence(self):
        reader = Reader()
        for spec in inventory.SCOPE_SPECS:
            reader.responses[spec['path']] = [response(spec['scopeId'], rows=[])]
        result = summary(capture(reader))
        self.assertTrue(all(row['rowCount'] == 0 and row['paginationStatus'] == 'COMPLETE' for row in result['scopes']))
        self.assertEqual(result['uncoveredScopes'], list(inventory.UNCOVERED_SCOPES))
        for key in inventory.FALSE_CLAIMS:
            self.assertIs(result[key], False, key)

    def test_private_capture_and_safe_summary_strict_nested_privacy_and_false_flags(self):
        value = capture()
        result = summary(value)
        self.assertEqual(inventory.validate_inventory_summary(result), result)
        text = json.dumps(result)
        for private in ('12345678', '987654.3210', 'PRIVATE_', 'SYNTHETIC_KEY', 'SYNTHETIC_SECRET',
            'SYNTHETIC_TOKEN', value['accountQueryHash'], value['readerSettingsHash'],
            value['accountBinding']['accountFingerprint']):
            self.assertNotIn(private, text)
        for mutate in (lambda s: s.__setitem__('private', 'PRIVATE'),
            lambda s: s['scopes'][0].__setitem__('rows', [{'balance': 'PRIVATE'}]),
            lambda s: s['scopes'][0]['queryScope'].__setitem__('CANO', '12345678'),
            lambda s: s['scopes'][0].__setitem__('rowCount', True),
            lambda s: s.__setitem__('orderAuthorityGranted', 0),
            lambda s: s.__setitem__('fullAccountReady', True),
            lambda s: s.__setitem__('reasons', ['PRIVATE_REMOTE_REASON']),
            lambda s: s.__setitem__('freshnessBoundarySeconds', 90)):
            altered = deepcopy(result); mutate(altered)
            with self.assertRaises(inventory.KisAccountInventoryError):
                inventory.validate_inventory_summary(rehash(altered))

    def test_capture_rehash_cannot_change_query_profile_shape_flags_or_page_chain(self):
        value = capture()
        for mutate in (lambda s: s['scopes'][0]['pages'][0]['queryWithoutAccount'].__setitem__('PDNO', '005930'),
            lambda s: s.__setitem__('authenticatedProducerAttested', True),
            lambda s: s.__setitem__('accountBinding', {'profileId': 'a' * 64}),
            lambda s: s.__setitem__('startedAt', '2026-10-19T00:59:00Z'),
            lambda s: s['scopes'][0]['pages'][0].__setitem__('responseTrId', 'WRONG')):
            altered = deepcopy(value); mutate(altered)
            with self.assertRaises(inventory.KisAccountInventoryError):
                inventory.validate_inventory_capture(rehash(altered))

    def test_strict_wire_decoder_rejects_duplicate_nonfinite_and_invalid_json(self):
        for raw in (b'{"rt_cd":"0","rt_cd":"1"}', b'{"output1":[{"q":"1","q":"2"}]}',
            b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}', b'\xff', b'{'):
            with self.assertRaises(inventory.KisAccountInventoryError):
                inventory.decode_inventory_response(raw)
        self.assertEqual(inventory.decode_inventory_response(b'{"rt_cd":"0","output":[]}'), {'rt_cd': '0', 'output': []})

    def test_reader_failures_are_fixed_codes_without_private_exception_text(self):
        for stage in ('on_token', 'on_pace', 'on_get'):
            reader = Reader()
            def failed():
                raise RuntimeError('PRIVATE_ACCOUNT_AND_REMOTE_MESSAGE')
            setattr(reader, stage, failed)
            with self.assertRaisesRegex(inventory.KisAccountInventoryError, '^KIS_INVENTORY_READER_UNAVAILABLE$'):
                capture(reader)

    def test_missing_row_identity_is_explicit_and_never_defaulted_to_zero(self):
        reader = Reader()
        reader.responses[inventory.SCOPE_SPECS[2]['path']] = [response('US_POSITIONS', rows=[{'ovrs_cblc_qty': ''}])]
        result = summary(capture(reader))
        row = result['scopes'][2]
        self.assertEqual((row['rowCount'], row['distinctIdentityCount'], row['missingIdentityCount']), (1, 0, 1))
        self.assertIn('KIS_INVENTORY_US_POSITIONS_ROW_IDENTITIES_UNVERIFIED', result['reasons'])
        self.assertFalse(result['fullAccountReady'])

    def test_us_row_scope_contradictions_keep_rows_but_not_recognized_identity(self):
        for scope_id, changes in (('US_POSITIONS', {'tr_crcy_cd': 'HKD'}),
            ('US_POSITIONS', {'ovrs_excg_cd': 'SEHK'}),
            ('US_REGULAR_ORDERS', {'tr_crcy_cd': 'HKD'}),
            ('US_REGULAR_ORDERS', {'ovrs_excg_cd': 'SEHK'}),
            ('US_REGULAR_ORDERS', {'ord_dt': '20260230'}),
            ('US_REGULAR_ORDERS', {'odno': 'UNPROVEN'})):
            reader = Reader()
            spec = next(row for row in inventory.SCOPE_SPECS if row['scopeId'] == scope_id)
            body, headers = response(scope_id)
            body[spec['rowsKey']] = [body[spec['rowsKey']][0]]
            body[spec['rowsKey']][0].update(changes)
            reader.responses[spec['path']] = [(body, headers)]
            result = summary(capture(reader))
            row = next(item for item in result['scopes'] if item['scopeId'] == scope_id)
            self.assertEqual((row['rowCount'], row['distinctIdentityCount'], row['missingIdentityCount']), (1, 0, 1))
            self.assertFalse(result['fullAccountReady'])


if __name__ == '__main__':
    unittest.main()
