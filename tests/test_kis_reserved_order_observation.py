"""Synthetic bounded read tests; actual source, no app/env/credential loading."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib
import json
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = '_kis_reserved_actual_source_test'
namespace = types.ModuleType(PACKAGE)
namespace.__path__ = [str(ROOT / 'live_trader')]
sys.modules[PACKAGE] = namespace
if 'trading_runtime' not in sys.modules:
    runtime = types.ModuleType('trading_runtime')
    runtime.__path__ = [str(ROOT.parents[1] / 'packages/trading_runtime/trading_runtime')]
    sys.modules['trading_runtime'] = runtime
reserved = importlib.import_module(PACKAGE + '.kis_reserved_order_observation')
NOW = datetime(2026, 10, 19, 1, 0, tzinfo=timezone.utc)
SETTINGS = {'KIS_APP_KEY': 'SYNTHETIC_KEY', 'KIS_APP_SECRET': 'SYNTHETIC_SECRET',
    'KIS_ACCOUNT_NO': '12345678-01', 'KIS_ACCOUNT_PRODUCT_CODE': '01',
    'KIS_BASE_URL': 'https://openapi.koreainvestment.com:9443', 'KIS_ENV': 'real',
    'KIS_OVERSEAS_FUNDING_MODE': 'INTEGRATED_MARGIN'}
START, END = '20260920', '20261019'


def response(spec, *, rows=None, continuation='D', cursor=('PRIVATE_FK', 'PRIVATE_NK')):
    if rows is None:
        if spec['scopeId'].startswith('KR_RESERVED'):
            rows = [{'rsvn_ord_ord_dt': END, 'rsvn_ord_seq': 'PRIVATE_RESERVATION_ID',
                'prcs_rslt': 'PRIVATE_REMOTE_STATE', 'pdno': 'PRIVATE_KR_SYMBOL', 'ord_rsvn_unpr': '12345.67'}]
        elif spec['scopeId'].startswith('US_RESERVED'):
            rows = [{'rsvn_ord_rcit_dt': END, 'ovrs_rsvn_odno': 'PRIVATE_US_RESERVATION_ID',
                'ovrs_excg_cd': spec['baseQuery']['OVRS_EXCG_CD'], 'ovrs_rsvn_ord_stat_cd': 'PRIVATE_REMOTE_STATE',
                'pdno': 'PRIVATE_US_SYMBOL', 'ft_ord_unpr3': '12345.67'}]
        else:
            rows = [{'ord_gno_brno': 'PRIVATE_BRANCH', 'odno': 'PRIVATE_ORDER', 'stpm_cndt_pric': '12345.67',
                'stpm_efct_occr_yn': 'Y'}]
    body = {'rt_cd': '0', 'msg1': 'PRIVATE_REMOTE_MESSAGE', 'output': deepcopy(rows)}
    if continuation in ('F', 'M'):
        body['ctx_area_fk' + spec['cursorLength']], body['ctx_area_nk' + spec['cursorLength']] = cursor
    return body, {'tr_id': spec['trId'], 'tr_cont': continuation}


class Reader:
    def __init__(self):
        self.settings = dict(SETTINGS)
        self.responses = {s['scopeId']: [response(s)] for s in reserved.SCOPE_SPECS}
        self.credentials = self.tokens = self.paces = 0
        self.requests = []
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
        spec = next(s for s in reserved.SCOPE_SPECS if s['path'] == path and
            (path != reserved._OVERSEAS or s['baseQuery']['OVRS_EXCG_CD'] == kwargs['query']['OVRS_EXCG_CD']))
        queue = self.responses[spec['scopeId']]
        if not queue:
            raise AssertionError('UNEXPECTED_ADDITIONAL_GET')
        return deepcopy(queue.pop(0))


class Clock:
    def __init__(self, step=1):
        self.tick = self.last = 0
        self.step = step
    def utc(self):
        self.last = self.tick
        self.tick += self.step
        return (NOW + timedelta(seconds=self.last)).isoformat()
    def mono(self):
        return float(self.last)


def capture(reader=None, *, step=1, **kwargs):
    clock = Clock(step)
    return reserved.KisReservedOrderReader(reader or Reader(), utc_now=clock.utc, monotonic=clock.mono).observe(
        start_date=START, end_date=END, **kwargs)


def summary(value):
    return reserved.safe_reserved_order_summary(value, now=datetime.fromisoformat(value['completedAt']))


def rehash(value):
    result = deepcopy(value)
    result.pop('contentHash', None)
    result['contentHash'] = reserved._hash(result)
    return result


class ReservedTests(unittest.TestCase):
    def test_constructor_no_io_five_exact_scopes_no_selected_symbol(self):
        reader = Reader()
        reserved.KisReservedOrderReader(reader)
        self.assertEqual((reader.credentials, reader.tokens, reader.paces, reader.requests), (0, 0, 0, []))
        result = summary(capture(reader))
        self.assertEqual((reader.credentials, reader.tokens, reader.paces, len(reader.requests)), (1, 1, 5, 5))
        self.assertEqual(result['requestWindow']['calendarDayCount'], 30)
        for request, spec in zip(reader.requests, reserved.SCOPE_SPECS):
            self.assertEqual(request[:2], ('kis', spec['path']))
            self.assertEqual(request[2]['headers']['tr_id'], spec['trId'])
            expected = {**reserved._query(spec, result['requestWindow']), 'CANO': '12345678', 'ACNT_PRDT_CD': '01',
                'CTX_AREA_FK' + spec['cursorLength']: '', 'CTX_AREA_NK' + spec['cursorLength']: ''}
            self.assertEqual(request[2]['query'], expected)
            self.assertEqual(request[2]['headers']['tr_cont'], '')
        self.assertEqual(result['scopes'][0]['queryScope']['CNCL_YN'], 'Y')
        self.assertNotIn('INQR_STRT_DT', result['scopes'][-1]['queryScope'])
        self.assertEqual([s['baseQuery']['OVRS_EXCG_CD'] for s in reserved.SCOPE_SPECS[1:4]], ['NASD', 'NYSE', 'AMEX'])

    def test_invalid_dates_future_and_local_cap_rejected_before_credentials(self):
        for start, end, maximum in [('20260919', END, 3), ('20260918', END, 2),
            ('20260230', END, 2), (END, '20261020', 2), (END, START, 2), (START, END, True)]:
            reader = Reader()
            with self.assertRaises(reserved.KisReservedOrderError):
                reserved.KisReservedOrderReader(reader, utc_now=lambda: NOW.isoformat()).observe(
                    start_date=start, end_date=end, max_pages=maximum)
            self.assertEqual((reader.credentials, reader.tokens, reader.requests), (0, 0, []))
        self.assertEqual(reserved.validate_reserved_request(start_date='20260919', end_date=END, as_of=NOW)['calendarDayCount'], 31)

    def test_ten_get_cap_exact_200_and_100_cursors_partial_not_absence(self):
        reader = Reader()
        for spec in reserved.SCOPE_SPECS:
            reader.responses[spec['scopeId']] = [response(spec, continuation='M', cursor=('FIRST', '')),
                response(spec, continuation='F', cursor=('', 'SECOND'))]
        result = summary(capture(reader))
        self.assertEqual(len(reader.requests), 10)
        self.assertTrue(all(s['paginationStatus'] == 'PARTIAL' for s in result['scopes']))
        for request, spec in zip(reader.requests[1::2], reserved.SCOPE_SPECS):
            self.assertEqual(request[2]['headers']['tr_cont'], 'N')
            self.assertEqual(request[2]['query']['CTX_AREA_FK' + spec['cursorLength']], 'FIRST')
            self.assertEqual(request[2]['query']['CTX_AREA_NK' + spec['cursorLength']], '')
        self.assertFalse(result['accountWideAbsenceAuthoritative'])

    def test_unknown_header_zero_rows_never_means_no_reservations(self):
        reader = Reader()
        for spec in reserved.SCOPE_SPECS:
            reader.responses[spec['scopeId']] = [response(spec, rows=[], continuation='')]
        result = summary(capture(reader))
        self.assertTrue(all(s['rowCount'] == 0 and s['paginationStatus'] == 'UNKNOWN' for s in result['scopes']))
        self.assertTrue(all(result[k] is False for k in reserved.FALSE_CLAIMS))
        self.assertIn('DOMESTIC_CNCL_Y_PRCS_0_ALL_ACTIVE_FILTER_MEANING_UNVERIFIED', result['uncoveredScopes'])

    def test_cursor_trid_json_shape_errors_fail_before_next_get(self):
        for defect in ('missing', 'repeated', 'header', 'trid', 'rows'):
            reader = Reader()
            spec = reserved.SCOPE_SPECS[0]
            first, second = response(spec, continuation='M'), response(spec, continuation='M')
            if defect == 'missing':
                first[0].pop('ctx_area_nk200')
            elif defect == 'header':
                first[1]['tr_cont'] = 'PRIVATE_BAD_HEADER'
            elif defect == 'trid':
                first[1]['tr_id'] = 'PRIVATE_BAD_TRID'
            elif defect == 'rows':
                first[0]['output'] = {}
            reader.responses[spec['scopeId']] = [first, second]
            with self.assertRaises(reserved.KisReservedOrderError) as failure:
                capture(reader)
            self.assertNotIn('PRIVATE', str(failure.exception))
            self.assertEqual(len(reader.requests), 2 if defect == 'repeated' else 1)

    def test_page_conflict_fails_identical_overlap_counted_and_venues_not_merged(self):
        for conflict in (True, False):
            reader = Reader()
            spec = reserved.SCOPE_SPECS[0]
            first, second = response(spec, continuation='M'), response(spec)
            if conflict:
                second[0]['output'][0]['ord_rsvn_unpr'] = '999'
            reader.responses[spec['scopeId']] = [first, second]
            if conflict:
                with self.assertRaisesRegex(reserved.KisReservedOrderError, 'IDENTITY_CONFLICT'):
                    capture(reader)
            else:
                result = summary(capture(reader))
                self.assertEqual((result['scopes'][0]['rowCount'], result['scopes'][0]['identicalDuplicateCount']), (2, 1))
                self.assertEqual([s['distinctIdentityCount'] for s in result['scopes'][1:4]], [1, 1, 1])
                self.assertFalse(result['crossVenueRowsMerged'])

    def test_outside_window_or_wrong_venue_rows_remain_but_identity_unverified(self):
        reader = Reader()
        spec = reserved.SCOPE_SPECS[1]
        reply = response(spec)
        reply[0]['output'][0]['ovrs_excg_cd'] = 'SEHK'
        reader.responses[spec['scopeId']] = [reply]
        reply = response(reserved.SCOPE_SPECS[0])
        reply[0]['output'][0]['rsvn_ord_ord_dt'] = '20260919'
        reader.responses[reserved.SCOPE_SPECS[0]['scopeId']] = [reply]
        value = capture(reader)
        result = summary(value)
        self.assertEqual([(s['rowCount'], s['missingIdentityCount']) for s in result['scopes'][:2]], [(1, 1), (1, 1)])
        self.assertEqual(value['scopes'][1]['pages'][0]['body']['output'][0]['ovrs_excg_cd'], 'SEHK')

    def test_account_secret_mode_fee_changes_discard_results_at_io_boundaries(self):
        for stage in ('on_token', 'on_pace', 'on_get'):
            for key, value in [('KIS_ACCOUNT_NO', '87654321-01'), ('KIS_APP_SECRET', 'CHANGED'),
                ('KIS_OVERSEAS_FUNDING_MODE', 'FOREIGN_CASH'), ('KIS_ESTIMATED_US_FEE_PERCENT', '0.1')]:
                reader = Reader()
                setattr(reader, stage, lambda r=reader, k=key, v=value: r.settings.__setitem__(k, v))
                with self.assertRaises(reserved.KisReservedOrderError):
                    capture(reader)

    def test_public_summary_omits_private_values_and_lifecycle_inference(self):
        result = summary(capture())
        text = json.dumps(result)
        for private in ('PRIVATE_', 'SYNTHETIC_', '12345678', '12345.67', 'Bearer ', 'accountFingerprint', 'accountQueryHash', 'readerSettingsHash'):
            self.assertNotIn(private, text)
        self.assertTrue(all(s['stateInterpretation'] == 'UNKNOWN_UNREVIEWED_CODES' for s in result['scopes']))
        self.assertEqual(reserved.validate_reserved_order_summary(result), result)

    def test_strict_public_validator_rejects_resealed_nested_private_flags_counts_and_filters(self):
        result = summary(capture())
        mutations = [lambda r: r.__setitem__('orderAuthorityGranted', True),
            lambda r: r['scopes'][0]['queryScope'].__setitem__('CNCL_YN', 'N'),
            lambda r: r['scopes'][-1]['conditionFieldStates'][0].__setitem__('amount', 'SECRET'),
            lambda r: r['scopes'][-1]['conditionFieldStates'][0].__setitem__('reportedTextCount', True),
            lambda r: r['scopes'][0].__setitem__('rowCount', 5),
            lambda r: r.__setitem__('uncoveredScopes', []),
            lambda r: r['scopes'][1].__setitem__('scopeId', 'US_RESERVED_ALL')]
        for mutation in mutations:
            changed = deepcopy(result)
            mutation(changed)
            with self.assertRaises(reserved.KisReservedOrderError):
                reserved.validate_reserved_order_summary(rehash(changed))

    def test_local_and_monotonic_fifteen_second_bounds_do_not_change_policy(self):
        result = summary(capture(step=2))
        self.assertEqual(result['freshnessStatus'], 'STALE_OR_CLOCK_INVALID')
        value = capture()
        result = reserved.safe_reserved_order_summary(value, now=NOW + timedelta(seconds=30))
        self.assertEqual(result['freshnessStatus'], 'STALE_OR_CLOCK_INVALID')
        for scope in value['scopes']:
            for page in scope['pages']:
                page['startedMonotonic'] *= 2
                page['completedMonotonic'] *= 2
        value['startedMonotonic'] *= 2
        value['completedMonotonic'] *= 2
        self.assertEqual(summary(rehash(value))['freshnessStatus'], 'STALE_OR_CLOCK_INVALID')

    def test_condition_fields_reported_zero_negative_empty_absent_bad_type_no_event_interpretation(self):
        reader = Reader()
        spec = reserved.SCOPE_SPECS[-1]
        rows = [{'ord_gno_brno': 'BR', 'odno': str(i), **fields} for i, fields in enumerate([
            {'stpm_cndt_pric': '0', 'stpm_efct_occr_yn': 'Y'},
            {'stpm_cndt_pric': '-1.25', 'stpm_efct_occr_yn': ''},
            {'stpm_cndt_pric': None}, {}])]
        reader.responses[spec['scopeId']] = [response(spec, rows=rows)]
        result = summary(capture(reader))
        fields = result['scopes'][-1]['conditionFieldStates']
        self.assertEqual(fields[0], {'field': 'stpm_cndt_pric', 'absentCount': 1, 'emptyCount': 0,
            'reportedTextCount': 2, 'invalidTypeCount': 1, 'reportedDecimalTextCount': 2})
        self.assertEqual(fields[1]['emptyCount'], 1)
        self.assertFalse(result['completeConditionalOrdersVerified'])
        self.assertEqual(reserved.validate_reserved_order_summary(result), result)

    def test_capture_tampering_hash_and_exact_query_binding_rejected(self):
        value = capture()
        value['scopes'][0]['pages'][0]['body']['output'][0]['prcs_rslt'] = 'CHANGED'
        with self.assertRaises(reserved.KisReservedOrderError):
            reserved.validate_reserved_order_capture(value)
        with self.assertRaises(reserved.KisReservedOrderError):
            reserved.validate_reserved_order_capture(rehash(value))
        value = capture()
        value['scopes'][0]['pages'][0]['queryWithoutAccount']['EXTRA'] = 'x'
        with self.assertRaises(reserved.KisReservedOrderError):
            reserved.validate_reserved_order_capture(rehash(value))

    def test_duplicate_nonfinite_wire_json_rejected(self):
        for raw in ('{"output":[],"output":[]}', '{"x":NaN}', '{"x":Infinity}', '[1e999]'):
            with self.assertRaises(reserved.KisReservedOrderError):
                reserved.decode_reserved_order_response(raw)
        self.assertEqual(reserved.decode_reserved_order_response('{"rt_cd":"0","output":[]}'), {'rt_cd': '0', 'output': []})

    def test_standalone_condition_facts_from_existing_inventory_no_additional_reads(self):
        inventory = reserved.inventory
        class InventoryReader(Reader):
            def _get(self, provider, path, **kwargs):
                self.requests.append((provider, path, deepcopy(kwargs)))
                spec = next(s for s in inventory.SCOPE_SPECS if s['path'] == path)
                rows = ([{'ord_gno_brno': 'PRIVATE_BRANCH', 'odno': 'PRIVATE_ORDER',
                    'stpm_cndt_pric': '12345.67', 'stpm_efct_occr_yn': 'Y'}]
                    if spec['scopeId'] == 'KR_REVOCABLE_ORDERS' else [])
                return {'rt_cd': '0', spec['rowsKey']: rows}, {'tr_id': spec['trId'], 'tr_cont': 'D'}
        reader = InventoryReader()
        clock = Clock()
        private = inventory.KisAccountInventoryReader(reader, utc_now=clock.utc, monotonic=clock.mono).observe()
        before = len(reader.requests)
        result = reserved.safe_regular_condition_facts(private)
        self.assertEqual(len(reader.requests), before)
        self.assertEqual(result, reserved.validate_regular_condition_facts(result))
        self.assertEqual(result['fields'][0]['reportedDecimalTextCount'], 1)
        self.assertNotIn('PRIVATE', json.dumps(result))
        changed = deepcopy(result)
        changed['fields'][0]['value'] = '12345.67'
        with self.assertRaises(reserved.KisReservedOrderError):
            reserved.validate_regular_condition_facts(rehash(changed))


if __name__ == '__main__':
    unittest.main()
