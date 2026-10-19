"""Bounded reservation query facts, never complete working-order authority.

Only the injected normal read-only reader performs IO. Account values, remote
messages, order identities, amounts and continuation keys stay in the capture.
The domestic CNCL_YN=Y/PRCS_DVSN_CD=0 example has not been established as an
all-active-orders filter. US venue responses are retained separately, not summed
or assumed disjoint. No lifecycle, funding, cash, fee or settlement is inferred.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import math
import re
import time

from . import kis_account_inventory_observation as inventory

SCHEMA = 'kis-reserved-order-observation/v1'
SUMMARY_SCHEMA = 'kis-reserved-order-summary/v1'
STATUS = 'REPORTED_FILTERED_RESERVATIONS_NOT_ALL_WORKING_ORDERS'
HASH_MEANING = inventory.HASH_MEANING
TIME_MEANING = inventory.TIME_MEANING
FRESHNESS_SECONDS = 15
MAX_ROWS = 10000
MAX_WINDOW_DAYS = 31  # Explicit local request bound, not a claimed broker rule.
MAX_PAGES = 2
_KST = timezone(timedelta(hours=9))
_HASH = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)
_DECIMAL = re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\Z', re.ASCII)
_DOMESTIC = '/uapi/domestic-stock/v1/trading/order-resv-ccnl'
_OVERSEAS = '/uapi/overseas-stock/v1/trading/order-resv-list'
SCOPE_SPECS = (
    {'scopeId': 'KR_RESERVED_CNCL_Y_PRCS_0', 'path': _DOMESTIC, 'trId': 'CTSC0004R',
     'method': 'GET', 'cursorLength': '200', 'rowsKey': 'output',
     'baseQuery': {'TMNL_MDIA_KIND_CD': '00', 'PRCS_DVSN_CD': '0', 'CNCL_YN': 'Y',
                   'RSVN_ORD_SEQ': '', 'PDNO': '', 'SLL_BUY_DVSN_CD': ''},
     'scopeMeaning': 'KR_EXAMPLE_FILTER_PROCESSING_HISTORY_NOT_ALL_ACTIVE_RESERVATIONS'},
    *({'scopeId': 'US_RESERVED_' + venue, 'path': _OVERSEAS, 'trId': 'TTTT3039R',
       'method': 'GET', 'cursorLength': '200', 'rowsKey': 'output',
       'baseQuery': {'INQR_DVSN_CD': '00', 'OVRS_EXCG_CD': venue, 'PRDT_TYPE_CD': ''},
       'scopeMeaning': 'US_EXPLICIT_VENUE_RESERVATION_QUERY_NOT_DISJOINT_OR_ALL_CONDITIONS'}
      for venue in ('NASD', 'NYSE', 'AMEX')),
    {'scopeId': 'KR_REGULAR_CONDITION_FIELDS',
     'path': '/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl', 'trId': 'TTTC0084R',
     'method': 'GET', 'cursorLength': '100', 'rowsKey': 'output',
     'baseQuery': {'INQR_DVSN_1': '0', 'INQR_DVSN_2': '0'},
     'scopeMeaning': 'KR_REVOCABLE_ORDER_REPORTED_FIELDS_NOT_ALL_CONDITIONAL_INSTRUCTIONS'},
)
SCOPE_DEFINITIONS = tuple((s['scopeId'], s['path'], s['trId'], s['cursorLength'],
    s['rowsKey'], dict(s['baseQuery']), s['scopeMeaning']) for s in SCOPE_SPECS)
UNCOVERED_SCOPES = (
    'DOMESTIC_CNCL_Y_PRCS_0_ALL_ACTIVE_FILTER_MEANING_UNVERIFIED',
    'ORDER_LIFECYCLE_STATE_CODES_UNREVIEWED', 'DATES_BEFORE_WINDOW_AND_AFTER_WINDOW',
    'UNTRIGGERED_HTS_MTS_OR_OTHER_CONDITIONAL_INSTRUCTIONS',
    'OVERSEAS_NON_US_MARKETS_AND_OTHER_SERVICES', 'CROSS_VENUE_OVERLAP_UNRECONCILED',
    'CROSS_REQUEST_ATOMIC_ACCOUNT_SNAPSHOT', 'ACTUAL_CASH_FEES_FX_AND_SETTLEMENT',
)
FALSE_CLAIMS = {**inventory.FALSE_CLAIMS, 'reservedOrdersReconciled': False,
    'completeConditionalOrdersVerified': False, 'orderLifecycleInterpreted': False,
    'crossVenueRowsMerged': False}


class KisReservedOrderError(ValueError):
    """Fixed safe codes only; never remote text or an input value."""


def _bad(code='KIS_RESERVED_CAPTURE_INVALID'):
    raise KisReservedOrderError(code)


def _hash(value):
    try:
        return inventory._hash(value)
    except (ValueError, TypeError, ArithmeticError, RecursionError):
        _bad('KIS_RESERVED_JSON_INVALID')


def decode_reserved_order_response(raw):
    try:
        return inventory.decode_inventory_response(raw)
    except (ValueError, TypeError, RecursionError):
        _bad('KIS_RESERVED_JSON_INVALID')


def _utc(value):
    try:
        return inventory._utc(value)
    except (ValueError, TypeError, ArithmeticError):
        _bad('KIS_RESERVED_CLOCK_INVALID')


def _mono(value):
    if type(value) not in (int, float):
        _bad('KIS_RESERVED_CLOCK_INVALID')
    try:
        if not math.isfinite(value) or value < 0:
            _bad('KIS_RESERVED_CLOCK_INVALID')
    except OverflowError:
        _bad('KIS_RESERVED_CLOCK_INVALID')
    return value


def _date(value):
    if type(value) is not str or not re.fullmatch(r'[0-9]{8}', value, re.ASCII):
        _bad('KIS_RESERVED_DATE_INVALID')
    try:
        return datetime.strptime(value, '%Y%m%d').date()
    except ValueError:
        _bad('KIS_RESERVED_DATE_INVALID')


def validate_reserved_request(*, start_date, end_date, max_pages=2, as_of=None):
    """Validate local bounds; these are not claims about API maximum periods."""
    if type(max_pages) is not int or not 1 <= max_pages <= MAX_PAGES:
        _bad('KIS_RESERVED_PAGE_LIMIT_INVALID')
    reference = as_of if as_of is not None else datetime.now(timezone.utc)
    if not isinstance(reference, datetime) or reference.tzinfo is None:
        _bad('KIS_RESERVED_CLOCK_INVALID')
    start, end = _date(start_date), _date(end_date)
    days = (end - start).days + 1
    if not 1 <= days <= MAX_WINDOW_DAYS or end > reference.astimezone(_KST).date():
        _bad('KIS_RESERVED_WINDOW_INVALID')
    return {'startDate': start_date, 'endDate': end_date, 'calendarDayCount': days,
        'localMaximumCalendarDays': MAX_WINDOW_DAYS,
        'dateMeaning': 'API_NAMED_DATE_FILTERS_NOT_ALL_LIFETIME_OR_FUTURE_ORDERS'}


def _binding(settings):
    try:
        return inventory._binding(settings)
    except (ValueError, TypeError, KeyError, ArithmeticError):
        _bad('KIS_RESERVED_ACCOUNT_BINDING_INVALID')


def _query(spec, window):
    if spec['scopeId'] == 'KR_REGULAR_CONDITION_FIELDS':
        return dict(spec['baseQuery'])
    dates = ({'RSVN_ORD_ORD_DT': window['startDate'], 'RSVN_ORD_END_DT': window['endDate']}
        if spec['path'] == _DOMESTIC else
        {'INQR_STRT_DT': window['startDate'], 'INQR_END_DT': window['endDate']})
    return {**spec['baseQuery'], **dates}


def _cursor(body, context):
    pair = tuple(body.get('ctx_area_' + part + context) for part in ('fk', 'nk'))
    if any(type(v) is not str or len(v) > 4096 for v in pair) or not any(v.strip() for v in pair):
        _bad('KIS_RESERVED_CONTINUATION_CURSOR_MISSING')
    return pair


def _identity(row, spec, window):
    if spec['scopeId'] == 'KR_REGULAR_CONDITION_FIELDS':
        values = tuple(row.get(k) for k in ('ord_gno_brno', 'odno'))
        return values if all(type(v) is str and v.strip() and len(v) <= 256 for v in values) else None
    fields = (('rsvn_ord_ord_dt', 'rsvn_ord_seq') if spec['path'] == _DOMESTIC
        else ('rsvn_ord_rcit_dt', 'ovrs_rsvn_odno', 'ovrs_excg_cd'))
    values = tuple(row.get(k) for k in fields)
    if any(type(v) is not str or not v.strip() or len(v) > 256 for v in values):
        return None
    try:
        day = _date(values[0])
    except KisReservedOrderError:
        return None
    if not _date(window['startDate']) <= day <= _date(window['endDate']):
        return None
    if spec['path'] == _OVERSEAS and values[-1] != spec['baseQuery']['OVRS_EXCG_CD']:
        return None
    return values


def _condition_states(rows):
    fields = []
    for field in ('stpm_cndt_pric', 'stpm_efct_occr_yn'):
        states = {'absentCount': 0, 'emptyCount': 0, 'reportedTextCount': 0, 'invalidTypeCount': 0, 'reportedDecimalTextCount': 0}
        for row in rows:
            if field not in row:
                states['absentCount'] += 1
            elif type(row[field]) is not str:
                states['invalidTypeCount'] += 1
            elif not row[field].strip():
                states['emptyCount'] += 1
            else:
                states['reportedTextCount'] += 1
                if len(row[field]) <= 256 and _DECIMAL.fullmatch(row[field]):
                    states['reportedDecimalTextCount'] += 1
        fields.append({'field': field, **states})
    return fields


def _validate_condition_states(fields, row_count):
    if type(fields) is not list or len(fields) != 2:
        _bad('KIS_RESERVED_CONDITION_FACTS_INVALID')
    for row, field in zip(fields, ('stpm_cndt_pric', 'stpm_efct_occr_yn')):
        keys = {'field', 'absentCount', 'emptyCount', 'reportedTextCount', 'invalidTypeCount', 'reportedDecimalTextCount'}
        if (type(row) is not dict or set(row) != keys or row['field'] != field
            or any(type(row[k]) is not int or not 0 <= row[k] <= MAX_ROWS for k in keys - {'field'})
            or row_count != sum(row[k] for k in ('absentCount', 'emptyCount', 'reportedTextCount', 'invalidTypeCount'))
            or row['reportedDecimalTextCount'] > row['reportedTextCount']):
            _bad('KIS_RESERVED_CONDITION_FACTS_INVALID')
    return deepcopy(fields)


def _scope_facts(scope, spec, window, maximum):
    if type(scope) is not dict or set(scope) != {'scopeId', 'pages'} or scope['scopeId'] != spec['scopeId']:
        _bad('KIS_RESERVED_SCOPE_INVALID')
    pages = scope['pages']
    if type(pages) is not list or not 1 <= len(pages) <= maximum:
        _bad('KIS_RESERVED_SCOPE_INVALID')
    fk = nk = ''
    cursors, identities = set(), {}
    count = duplicates = missing = reported_state = 0
    all_rows = []
    context = spec['cursorLength']
    previous_end = previous_mono = None
    for index, page in enumerate(pages):
        if type(page) is not dict or set(page) != {'queryWithoutAccount', 'responseTrId', 'responseContinuation',
            'startedAt', 'completedAt', 'startedMonotonic', 'completedMonotonic', 'body', 'canonicalParsedBodyHash'}:
            _bad('KIS_RESERVED_PAGE_INVALID')
        expected = {**_query(spec, window), 'CTX_AREA_FK' + context: fk, 'CTX_AREA_NK' + context: nk}
        if page['queryWithoutAccount'] != expected or page['responseTrId'] != spec['trId']:
            _bad('KIS_RESERVED_PAGE_BINDING_INVALID')
        start, end = _utc(page['startedAt']), _utc(page['completedAt'])
        sm, em = _mono(page['startedMonotonic']), _mono(page['completedMonotonic'])
        if end < start or em < sm or previous_end is not None and (start < previous_end or sm < previous_mono):
            _bad('KIS_RESERVED_CLOCK_INVALID')
        previous_end, previous_mono = end, em
        body = page['body']
        if type(body) is not dict or body.get('rt_cd') != '0' or page['canonicalParsedBodyHash'] != _hash(body):
            _bad('KIS_RESERVED_RESPONSE_REJECTED')
        rows = body.get('output')
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            _bad('KIS_RESERVED_ROWS_INVALID')
        count += len(rows)
        all_rows.extend(rows)
        if count > MAX_ROWS:
            _bad('KIS_RESERVED_ROW_LIMIT')
        state_key = ('prcs_rslt' if spec['path'] == _DOMESTIC else 'ovrs_rsvn_ord_stat_cd'
            if spec['path'] == _OVERSEAS else None)
        for row in rows:
            if type(row.get(state_key)) is str and row[state_key].strip():
                reported_state += 1
            identity = _identity(row, spec, window)
            if identity is None:
                missing += 1
                continue
            seal = _hash(row)
            if identity in identities:
                if identities[identity] != seal:
                    _bad('KIS_RESERVED_ROW_IDENTITY_CONFLICT')
                duplicates += 1
            identities[identity] = seal
        continuation = page['responseContinuation']
        if continuation not in (None, '', 'D', 'E', 'F', 'M'):
            _bad('KIS_RESERVED_CONTINUATION_INVALID')
        if continuation in ('F', 'M'):
            fk, nk = _cursor(body, context)
            if (fk, nk) in cursors:
                _bad('KIS_RESERVED_CONTINUATION_REPEATED')
            cursors.add((fk, nk))
        elif index != len(pages) - 1:
            _bad('KIS_RESERVED_PAGE_AFTER_TERMINAL_OR_UNKNOWN')
    last = pages[-1]['responseContinuation']
    if last in ('F', 'M') and len(pages) != maximum:
        _bad('KIS_RESERVED_UNEXPLAINED_PARTIAL_CAPTURE')
    return {'scopeId': spec['scopeId'], 'path': spec['path'], 'trId': spec['trId'],
        'method': 'GET', 'queryScope': _query(spec, window), 'scopeMeaning': spec['scopeMeaning'],
        'pageCount': len(pages), 'rowCount': count, 'distinctIdentityCount': len(identities),
        'identicalDuplicateCount': duplicates, 'missingIdentityCount': missing,
        'reportedStateFieldRows': reported_state, 'missingStateFieldRows': count - reported_state,
        'conditionFieldStates': _condition_states(all_rows) if spec['scopeId'] == 'KR_REGULAR_CONDITION_FIELDS' else [],
        'stateInterpretation': 'UNKNOWN_UNREVIEWED_CODES',
        'paginationStatus': 'COMPLETE' if last in ('D', 'E') else 'PARTIAL' if last in ('F', 'M') else 'UNKNOWN',
        'startedAt': pages[0]['startedAt'], 'completedAt': pages[-1]['completedAt']}


def validate_reserved_order_capture(capture):
    keys = {'schemaVersion', 'status', 'provider', 'accountBinding', 'readerSettingsHash', 'accountQueryHash',
        'requestWindow', 'maxPages', 'scopes', 'startedAt', 'completedAt', 'startedMonotonic',
        'completedMonotonic', 'hashMeaning', 'retrievalTimeMeaning', 'privateValuesLocalOnly', 'contentHash'} | set(FALSE_CLAIMS)
    if type(capture) is not dict or set(capture) != keys:
        _bad()
    if capture['contentHash'] != _hash({k: v for k, v in capture.items() if k != 'contentHash'}):
        _bad('KIS_RESERVED_CAPTURE_HASH_MISMATCH')
    if (capture['schemaVersion'] != SCHEMA or capture['status'] != STATUS or capture['provider'] != 'kis'
        or capture['hashMeaning'] != HASH_MEANING or capture['retrievalTimeMeaning'] != TIME_MEANING
        or capture['privateValuesLocalOnly'] is not True or any(capture[k] is not False for k in FALSE_CLAIMS)
        or any(type(capture[k]) is not str or not _HASH.fullmatch(capture[k]) for k in ('readerSettingsHash', 'accountQueryHash'))):
        _bad('KIS_RESERVED_CAPTURE_BINDING_INVALID')
    try:
        inventory._profile(capture['accountBinding'])
    except (ValueError, TypeError):
        _bad('KIS_RESERVED_ACCOUNT_BINDING_INVALID')
    window = capture['requestWindow']
    if type(window) is not dict:
        _bad('KIS_RESERVED_WINDOW_INVALID')
    expected = validate_reserved_request(start_date=window.get('startDate'), end_date=window.get('endDate'),
        max_pages=capture['maxPages'], as_of=_utc(capture['startedAt']))
    if window != expected or set(window) != set(expected):
        _bad('KIS_RESERVED_WINDOW_INVALID')
    if type(capture['scopes']) is not list or len(capture['scopes']) != len(SCOPE_SPECS):
        _bad('KIS_RESERVED_SCOPE_INVALID')
    facts = [_scope_facts(scope, spec, window, capture['maxPages']) for scope, spec in zip(capture['scopes'], SCOPE_SPECS)]
    pages = [p for scope in capture['scopes'] for p in scope['pages']]
    if any(_utc(cur['startedAt']) < _utc(prev['completedAt']) or _mono(cur['startedMonotonic']) < _mono(prev['completedMonotonic'])
        for cur, prev in zip(pages[1:], pages)):
        _bad('KIS_RESERVED_CLOCK_INVALID')
    if any(capture[k] != pages[0][k] for k in ('startedAt', 'startedMonotonic')) or any(
        capture[k] != pages[-1][k] for k in ('completedAt', 'completedMonotonic')):
        _bad('KIS_RESERVED_CLOCK_INVALID')
    return facts


def _summary_body(*, scopes, window, started_at, completed_at, assessed_at, source_hash, elapsed):
    start, end, assessed = _utc(started_at), _utc(completed_at), _utc(assessed_at)
    elapsed = _mono(elapsed)
    if end < start:
        _bad('KIS_RESERVED_CLOCK_INVALID')
    fresh = start <= end <= assessed and 0 <= (assessed - start).total_seconds() <= 15 and elapsed <= 15
    reasons = ['KIS_RESERVED_FILTERED_SCOPES_NOT_FULL_ACCOUNT', 'KIS_RESERVED_FILTER_AND_LIFECYCLE_MEANING_UNVERIFIED']
    if not fresh:
        reasons.append('KIS_RESERVED_LOCAL_RETRIEVAL_STALE_OR_CLOCK_INVALID')
    for scope in scopes:
        for key, suffix in (('missingIdentityCount', 'ROW_IDENTITIES_UNVERIFIED'), ('identicalDuplicateCount', 'IDENTICAL_PAGE_OVERLAP')):
            if scope[key]:
                reasons.append('KIS_RESERVED_' + scope['scopeId'] + '_' + suffix)
        if scope['paginationStatus'] != 'COMPLETE':
            reasons.append('KIS_RESERVED_' + scope['scopeId'] + '_PAGINATION_' + scope['paginationStatus'])
    return {'schemaVersion': SUMMARY_SCHEMA, 'status': STATUS, 'provider': 'kis', 'requestWindow': deepcopy(window),
        'scopes': deepcopy(scopes), 'startedAt': started_at, 'completedAt': completed_at, 'assessedAt': assessed_at,
        'freshnessBoundarySeconds': 15, 'collectionMonotonicDurationSeconds': elapsed,
        'freshnessExpiresAt': (start + timedelta(seconds=15)).isoformat(),
        'freshnessStatus': 'WITHIN_LOCAL_15_SECOND_BOUNDARY' if fresh else 'STALE_OR_CLOCK_INVALID',
        'uncoveredScopes': list(UNCOVERED_SCOPES), 'reasons': reasons, 'sourceCaptureHash': source_hash,
        'hashMeaning': HASH_MEANING, 'retrievalTimeMeaning': TIME_MEANING, **FALSE_CLAIMS}


def safe_reserved_order_summary(capture, *, now=None):
    scopes = validate_reserved_order_capture(capture)
    reference = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(reference, datetime) or reference.tzinfo is None:
        _bad('KIS_RESERVED_CLOCK_INVALID')
    body = _summary_body(scopes=scopes, window=capture['requestWindow'], started_at=capture['startedAt'],
        completed_at=capture['completedAt'], assessed_at=reference.astimezone(timezone.utc).isoformat(),
        source_hash=capture['contentHash'], elapsed=capture['completedMonotonic'] - capture['startedMonotonic'])
    return {**body, 'contentHash': _hash(body)}


def validate_reserved_order_summary(summary):
    if type(summary) is not dict or type(summary.get('scopes')) is not list or len(summary['scopes']) != len(SCOPE_SPECS):
        _bad('KIS_RESERVED_SUMMARY_INVALID')
    window = summary.get('requestWindow')
    if type(window) is not dict:
        _bad('KIS_RESERVED_SUMMARY_INVALID')
    expected_window = validate_reserved_request(start_date=window.get('startDate'), end_date=window.get('endDate'),
        as_of=_utc(summary.get('startedAt')))
    if window != expected_window or set(window) != set(expected_window):
        _bad('KIS_RESERVED_SUMMARY_INVALID')
    scopes = []
    counts = ('pageCount', 'rowCount', 'distinctIdentityCount', 'identicalDuplicateCount', 'missingIdentityCount',
        'reportedStateFieldRows', 'missingStateFieldRows')
    for row, spec in zip(summary['scopes'], SCOPE_SPECS):
        if type(row) is not dict or any(type(row.get(k)) is not int or not 0 <= row[k] <= MAX_ROWS for k in counts):
            _bad('KIS_RESERVED_SUMMARY_INVALID')
        if (not 1 <= row['pageCount'] <= 2 or row['rowCount'] != sum(row[k] for k in counts[2:5])
            or row['rowCount'] != row['reportedStateFieldRows'] + row['missingStateFieldRows']
            or row.get('paginationStatus') not in ('COMPLETE', 'PARTIAL', 'UNKNOWN')):
            _bad('KIS_RESERVED_SUMMARY_INVALID')
        start, end = _utc(row.get('startedAt')), _utc(row.get('completedAt'))
        conditions = (_validate_condition_states(row.get('conditionFieldStates'), row['rowCount'])
            if spec['scopeId'] == 'KR_REGULAR_CONDITION_FIELDS' else [])
        expected = {'scopeId': spec['scopeId'], 'path': spec['path'], 'trId': spec['trId'], 'method': 'GET',
            'queryScope': _query(spec, window), 'scopeMeaning': spec['scopeMeaning'], **{k: row[k] for k in counts},
            'stateInterpretation': 'UNKNOWN_UNREVIEWED_CODES', 'paginationStatus': row['paginationStatus'],
            'conditionFieldStates': conditions,
            'startedAt': row['startedAt'], 'completedAt': row['completedAt']}
        if row != expected or set(row) != set(expected) or end < start or scopes and start < _utc(scopes[-1]['completedAt']):
            _bad('KIS_RESERVED_SUMMARY_INVALID')
        scopes.append(expected)
    if (summary.get('startedAt') != scopes[0]['startedAt'] or summary.get('completedAt') != scopes[-1]['completedAt']
        or type(summary.get('sourceCaptureHash')) is not str or not _HASH.fullmatch(summary['sourceCaptureHash'])):
        _bad('KIS_RESERVED_SUMMARY_INVALID')
    body = _summary_body(scopes=scopes, window=window, started_at=summary['startedAt'], completed_at=summary['completedAt'],
        assessed_at=summary.get('assessedAt'), source_hash=summary['sourceCaptureHash'],
        elapsed=summary.get('collectionMonotonicDurationSeconds'))
    expected = {**body, 'contentHash': _hash(body)}
    if set(summary) != set(expected) or _hash(summary) != _hash(expected):
        _bad('KIS_RESERVED_SUMMARY_INVALID')
    return expected


class KisReservedOrderReader:
    def __init__(self, reader, *, utc_now=None, monotonic=None):
        self.reader = reader
        self.utc_now = utc_now or (lambda: datetime.now(timezone.utc).isoformat())
        self.monotonic = monotonic or time.monotonic

    def observe(self, *, start_date, end_date, max_pages=2):
        window = validate_reserved_request(start_date=start_date, end_date=end_date, max_pages=max_pages,
            as_of=_utc(self.utc_now()))
        original = _binding(self.reader.settings)
        snapshot, account, profile = original
        def unchanged():
            if _binding(self.reader.settings) != original:
                _bad('KIS_RESERVED_ACCOUNT_SCOPE_CHANGED')
        def call(function, *args, **kwargs):
            try:
                return function(*args, **kwargs)
            except KisReservedOrderError:
                raise
            except (ValueError, OSError, RuntimeError, TypeError, KeyError, ArithmeticError):
                _bad('KIS_RESERVED_READER_UNAVAILABLE')
        call(self.reader._credentials, 'kis')
        unchanged()
        token = call(self.reader._kis_token)
        if type(token) is not str or not token or len(token) > 16384:
            _bad('KIS_RESERVED_AUTHENTICATION_INVALID')
        unchanged()
        scopes = []
        for spec in SCOPE_SPECS:
            pages = []
            fk = nk = ''
            for index in range(max_pages):
                unchanged()
                call(self.reader._pace_kis)
                unchanged()
                context = spec['cursorLength']
                query = {**_query(spec, window), 'CTX_AREA_FK' + context: fk, 'CTX_AREA_NK' + context: nk}
                headers = {'authorization': 'Bearer ' + token, 'appkey': self.reader.settings['KIS_APP_KEY'],
                    'appsecret': self.reader.settings['KIS_APP_SECRET'], 'tr_id': spec['trId'],
                    'tr_cont': 'N' if index else '', 'custtype': 'P'}
                start, sm = self.utc_now(), self.monotonic()
                pair = call(self.reader._get, 'kis', spec['path'], query={**account, **query}, headers=headers)
                end, em = self.utc_now(), self.monotonic()
                unchanged()
                if type(pair) not in (tuple, list) or len(pair) != 2 or type(pair[1]) is not dict:
                    _bad('KIS_RESERVED_RESPONSE_PAIR_INVALID')
                body, response = pair
                pages.append({'queryWithoutAccount': query, 'responseTrId': response.get('tr_id'),
                    'responseContinuation': response.get('tr_cont'), 'startedAt': start, 'completedAt': end,
                    'startedMonotonic': sm, 'completedMonotonic': em, 'body': deepcopy(body), 'canonicalParsedBodyHash': _hash(body)})
                _scope_facts({'scopeId': spec['scopeId'], 'pages': pages}, spec, window, len(pages))
                if response.get('tr_cont') not in ('F', 'M'):
                    break
                fk, nk = _cursor(body, context)
            scopes.append({'scopeId': spec['scopeId'], 'pages': pages})
        unchanged()
        first, last = scopes[0]['pages'][0], scopes[-1]['pages'][-1]
        body = {'schemaVersion': SCHEMA, 'status': STATUS, 'provider': 'kis', 'accountBinding': profile,
            'readerSettingsHash': snapshot, 'accountQueryHash': _hash(account), 'requestWindow': window,
            'maxPages': max_pages, 'scopes': scopes, 'startedAt': first['startedAt'], 'completedAt': last['completedAt'],
            'startedMonotonic': first['startedMonotonic'], 'completedMonotonic': last['completedMonotonic'],
            'hashMeaning': HASH_MEANING, 'retrievalTimeMeaning': TIME_MEANING, 'privateValuesLocalOnly': True, **FALSE_CLAIMS}
        result = {**body, 'contentHash': _hash(body)}
        validate_reserved_order_capture(result)
        return result


def safe_regular_condition_facts(inventory_capture):
    """Counts of reported fields in an existing capture, not new account reads."""
    try:
        inventory.validate_inventory_capture(inventory_capture)
    except (ValueError, TypeError, ArithmeticError):
        _bad('KIS_RESERVED_REGULAR_CAPTURE_INVALID')
    rows = [r for s in inventory_capture['scopes'] if s['scopeId'] == 'KR_REVOCABLE_ORDERS'
        for p in s['pages'] for r in p['body']['output']]
    fields = _condition_states(rows)
    body = {'schemaVersion': 'kis-regular-condition-field-facts/v1', 'status': 'REPORTED_FIELDS_NOT_ALL_CONDITIONAL_ORDERS',
        'sourceCaptureHash': inventory_capture['contentHash'], 'rowCount': len(rows), 'fields': fields,
        'completeConditionalOrdersVerified': False, 'orderLifecycleInterpreted': False,
        'accountWideAbsenceAuthoritative': False, 'orderAuthorityGranted': False,
        'fieldValuesDisclosed': False, 'additionalAccountReadsPerformed': False}
    return {**body, 'contentHash': _hash(body)}


def validate_regular_condition_facts(summary):
    """Strict standalone public field facts; no private captures are accepted."""
    if (type(summary) is not dict or type(summary.get('rowCount')) is not int
        or not 0 <= summary['rowCount'] <= MAX_ROWS or type(summary.get('sourceCaptureHash')) is not str
        or not _HASH.fullmatch(summary['sourceCaptureHash'])):
        _bad('KIS_RESERVED_CONDITION_FACTS_INVALID')
    fields = _validate_condition_states(summary.get('fields'), summary['rowCount'])
    body = {'schemaVersion': 'kis-regular-condition-field-facts/v1', 'status': 'REPORTED_FIELDS_NOT_ALL_CONDITIONAL_ORDERS',
        'sourceCaptureHash': summary['sourceCaptureHash'], 'rowCount': summary['rowCount'], 'fields': fields,
        'completeConditionalOrdersVerified': False, 'orderLifecycleInterpreted': False,
        'accountWideAbsenceAuthoritative': False, 'orderAuthorityGranted': False,
        'fieldValuesDisclosed': False, 'additionalAccountReadsPerformed': False}
    expected = {**body, 'contentHash': _hash(body)}
    if set(summary) != set(expected) or _hash(summary) != _hash(expected):
        _bad('KIS_RESERVED_CONDITION_FACTS_INVALID')
    return expected
