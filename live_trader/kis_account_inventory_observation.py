"""Bounded, symbol-independent KIS inventory reads; no financial authority.

Only the existing injected read-only reader performs IO. Raw account responses,
cursor values and account bindings stay in the private capture. Public summaries
contain fixed scope descriptions and counts, never holdings, amounts or orders.
These four scopes cannot establish an atomic, complete account snapshot, shared
KRW cash, actual fees, an FX execution, or settlement finality.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import math
import json
import re
import time

from .account_profiles import saved_account_profiles
from .kis_overseas_funding import funding_policy, resolve_funding_mode
from .kis_provisional_fees import build_provisional_fee_profile
from .kis_usd_observations import _scope, canonical_hash

SCHEMA = 'kis-account-inventory-observation/v1'
SUMMARY_SCHEMA = 'kis-account-inventory-summary/v1'
STATUS = 'REPORTED_SCOPES_ONLY_NOT_ACCOUNT_RECONCILIATION'
HASH_MEANING = 'CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_OR_AUTHENTICATION'
TIME_MEANING = 'LOCAL_GET_TIMES_NOT_ATOMIC_ACCOUNT_OR_SETTLEMENT_TIME'
FRESHNESS_SECONDS = 15
MAX_ROWS = 10000
_HASH = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)
_PREFIX = '/uapi/'
SCOPE_DEFINITIONS = (
    ('KR_BASE_POSITIONS', _PREFIX + 'domestic-stock/v1/trading/inquire-balance', 'TTTC8434R', '100', 'output1',
     {'AFHR_FLPR_YN': 'N', 'OFL_YN': '', 'INQR_DVSN': '02', 'UNPR_DVSN': '01',
      'FUND_STTL_ICLD_YN': 'N', 'FNCG_AMT_AUTO_RDPT_YN': 'N', 'PRCS_DVSN': '00'},
     'KR_DEFAULT_BALANCE_SCOPE_NOT_ALL_VENUE_PROOF'),
    ('KR_REVOCABLE_ORDERS', _PREFIX + 'domestic-stock/v1/trading/inquire-psbl-rvsecncl', 'TTTC0084R', '100', 'output',
     {'INQR_DVSN_1': '0', 'INQR_DVSN_2': '0'}, 'KR_REVISION_CANCEL_ELIGIBLE_ORDERS_ONLY'),
    ('US_POSITIONS', _PREFIX + 'overseas-stock/v1/trading/inquire-balance', 'TTTS3012R', '200', 'output1',
     {'OVRS_EXCG_CD': 'NASD', 'TR_CRCY_CD': 'USD'}, 'REAL_NASD_QUERY_US_MARKETS_USD_POSITIONS'),
    ('US_REGULAR_ORDERS', _PREFIX + 'overseas-stock/v1/trading/inquire-nccs', 'TTTS3018R', '200', 'output',
     {'OVRS_EXCG_CD': 'NASD', 'SORT_SQN': 'DS'}, 'NASD_QUERY_US_MARKETS_REGULAR_UNFILLED_ORDERS'),
)
SCOPE_SPECS = tuple({'scopeId': row[0], 'path': row[1], 'trId': row[2],
    'cursorLength': row[3], 'rowsKey': row[4], 'baseQuery': dict(row[5]),
    'scopeMeaning': row[6]} for row in SCOPE_DEFINITIONS)
UNCOVERED_SCOPES = (
    'KR_RESERVED_ORDERS', 'KR_NON_REVOCABLE_OR_CONDITIONAL_ORDERS',
    'KR_OTHER_AFHR_OR_VENUE_BALANCE_SCOPES', 'US_RESERVED_AND_CONDITIONAL_ORDERS',
    'OVERSEAS_NON_US_MARKETS', 'CROSS_REQUEST_ATOMIC_SNAPSHOT',
    'ACTUAL_CASH_FEES_FX_AND_SETTLEMENT',
)
FALSE_CLAIMS = {'fullAccountReady': False, 'accountWideAbsenceAuthoritative': False,
    'completeAccountWorkingOrdersVerified': False, 'accountCashReconciled': False,
    'sharedCashBaselineEstablished': False, 'sharedCashReservationVerified': False,
    'actualFeeObserved': False, 'actualFxExecutionObserved': False,
    'settlementFinalityVerified': False, 'financialReady': False,
    'productionReady': False, 'orderAuthorityGranted': False,
    'ledgerInitialized': False, 'authenticatedProducerAttested': False}


class KisAccountInventoryError(ValueError):
    """Fixed safe code only, without private input or remote messages."""


def _bad(code='KIS_INVENTORY_CAPTURE_INVALID'):
    raise KisAccountInventoryError(code)


def _hash(value):
    def visit(item, depth=0):
        if depth > 24:
            _bad('KIS_INVENTORY_JSON_LIMIT')
        if type(item) is dict:
            if len(item) > 1024 or any(type(k) is not str or len(k) > 256 for k in item):
                _bad('KIS_INVENTORY_JSON_INVALID')
            for child in item.values():
                visit(child, depth + 1)
        elif type(item) is list:
            if len(item) > MAX_ROWS:
                _bad('KIS_INVENTORY_ROW_LIMIT')
            for child in item:
                visit(child, depth + 1)
        elif type(item) is float:
            if not math.isfinite(item):
                _bad('KIS_INVENTORY_JSON_INVALID')
        elif type(item) is str:
            if len(item) > 65536:
                _bad('KIS_INVENTORY_JSON_LIMIT')
        elif type(item) not in (int, bool, type(None)):
            _bad('KIS_INVENTORY_JSON_INVALID')
    visit(value)
    try:
        return canonical_hash(value)
    except (ValueError, TypeError, OverflowError):
        _bad('KIS_INVENTORY_JSON_INVALID')


def decode_inventory_response(raw):
    """Reject duplicate keys/nonfinite JSON before the transport loses them."""
    if type(raw) not in (bytes, str) or len(raw) > 4 * 1024 * 1024:
        _bad('KIS_INVENTORY_JSON_LIMIT')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                _bad('KIS_INVENTORY_DUPLICATE_JSON_KEY')
            result[key] = value
        return result
    def constant(_):
        _bad('KIS_INVENTORY_JSON_INVALID')
    try:
        result = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
        _hash(result)
        return result
    except KisAccountInventoryError:
        raise
    except (ValueError, TypeError, UnicodeError, RecursionError):
        _bad('KIS_INVENTORY_JSON_INVALID')


def _utc(value):
    try:
        if type(value) is not str:
            _bad('KIS_INVENTORY_CLOCK_INVALID')
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            _bad('KIS_INVENTORY_CLOCK_INVALID')
        return parsed
    except (ValueError, TypeError, OverflowError):
        _bad('KIS_INVENTORY_CLOCK_INVALID')


def _mono(value):
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        valid = False
    if not valid:
        _bad('KIS_INVENTORY_CLOCK_INVALID')
    return value


def _limit(value):
    if type(value) is not int or not 1 <= value <= 10:
        _bad('KIS_INVENTORY_PAGE_LIMIT_INVALID')
    return value


def _binding(settings):
    if type(settings) is not dict or any(type(k) is not str for k in settings):
        _bad('KIS_INVENTORY_SETTINGS_INVALID')
    kis_settings = {k: v for k, v in settings.items() if k.startswith('KIS_')}
    if any(type(v) is not str for v in kis_settings.values()):
        _bad('KIS_INVENTORY_SETTINGS_INVALID')
    try:
        credential_hash, account = _scope(settings)
        mode = resolve_funding_mode(settings)
        policy = funding_policy(settings)
        fee_profile = build_provisional_fee_profile(settings)
        profiles = [p for p in saved_account_profiles(settings) if p['brokerId'] == 'kis']
        if len(profiles) != 1:
            _bad('KIS_INVENTORY_ACCOUNT_BINDING_INVALID')
        profile = {k: profiles[0][k] for k in
            ('profileId', 'accountFingerprint', 'credentialConfigurationHash')}
        _profile(profile)
        # Include every KIS setting verbatim: a changed fee, mode or future KIS
        # configuration cannot reuse a previously collected dependency hash.
        snapshot = _hash({'credentials': credential_hash, 'settings': kis_settings,
            'fundingMode': mode, 'fundingPolicyHash': policy['policyHash'],
            'provisionalFeeProfileHash': fee_profile['feeProfileHash']})
        return snapshot, account, profile
    except KisAccountInventoryError:
        raise
    except (ValueError, TypeError, KeyError, ArithmeticError):
        _bad('KIS_INVENTORY_ACCOUNT_BINDING_INVALID')


def _profile(value):
    if (type(value) is not dict or set(value) != {'profileId', 'accountFingerprint', 'credentialConfigurationHash'}
        or any(type(v) is not str or not _HASH.fullmatch(v) for v in value.values())):
        _bad('KIS_INVENTORY_ACCOUNT_BINDING_INVALID')


def _cursor(body, context):
    values = [body.get('ctx_area_' + part + context) for part in ('fk', 'nk')]
    if any(type(v) is not str or len(v) > 4096 for v in values) or not any(v.strip() for v in values):
        _bad('KIS_INVENTORY_CONTINUATION_CURSOR_MISSING')
    return tuple(values)


def _identity(scope_id, row):
    """Detect page overlap/conflict only; never infer quantity or order state."""
    if scope_id == 'KR_BASE_POSITIONS':
        fields = ('pdno', 'loan_dt')
    elif scope_id == 'US_POSITIONS':
        fields = ('ovrs_pdno', 'ovrs_excg_cd', 'tr_crcy_cd', 'loan_dt', 'loan_type_cd')
    elif scope_id == 'KR_REVOCABLE_ORDERS':
        # This endpoint does not supply ord_dt. No date is invented for ODNO.
        fields = ('ord_gno_brno', 'odno')
    else:
        fields = ('ord_dt', 'ord_gno_brno', 'odno')
    optional = {'loan_dt', 'loan_type_cd'}
    values = tuple(row.get(k, '') for k in fields)
    if any(type(v) is not str or len(v) > 256 or (not v and k not in optional)
           for k, v in zip(fields, values)):
        return None
    us_venues = {'NASD', 'NAS', 'NYSE', 'AMEX'}
    if scope_id == 'US_POSITIONS' and (row['ovrs_excg_cd'] not in us_venues or row['tr_crcy_cd'] != 'USD'):
        return None
    if scope_id == 'US_REGULAR_ORDERS':
        if (not re.fullmatch(r'[0-9]{8}', row['ord_dt'], re.ASCII)
            or not re.fullmatch(r'[0-9]{1,8}', row['ord_gno_brno'], re.ASCII)
            or not re.fullmatch(r'[0-9]{1,10}', row['odno'], re.ASCII)):
            return None
        try:
            datetime.strptime(row['ord_dt'], '%Y%m%d')
        except ValueError:
            return None
        # Do not invent missing optional venue/currency fields. If reported,
        # an incompatible value cannot become a recognized US row identity.
        if ('ovrs_excg_cd' in row and (type(row['ovrs_excg_cd']) is not str or row['ovrs_excg_cd'] not in us_venues)
            or 'tr_crcy_cd' in row and row['tr_crcy_cd'] != 'USD'):
            return None
    return values


def _scope_facts(scope, definition, max_pages):
    scope_id, _, tr_id, context, output, query, _ = definition
    if type(scope) is not dict or set(scope) != {'scopeId', 'pages'} or scope['scopeId'] != scope_id:
        _bad('KIS_INVENTORY_SCOPE_INVALID')
    pages = scope['pages']
    if type(pages) is not list or not 1 <= len(pages) <= max_pages:
        _bad('KIS_INVENTORY_SCOPE_INVALID')
    fk = nk = ''
    cursors, identities = set(), {}
    duplicate_count = missing_identity_count = row_count = 0
    auxiliary_state = 'NOT_APPLICABLE'
    previous_end = previous_mono = None
    for index, page in enumerate(pages):
        if type(page) is not dict or set(page) != {'queryWithoutAccount', 'responseTrId', 'responseContinuation',
            'startedAt', 'completedAt', 'startedMonotonic', 'completedMonotonic', 'body', 'canonicalParsedBodyHash'}:
            _bad('KIS_INVENTORY_PAGE_INVALID')
        expected = {**query, 'CTX_AREA_FK' + context: fk, 'CTX_AREA_NK' + context: nk}
        if page['queryWithoutAccount'] != expected or page['responseTrId'] != tr_id:
            _bad('KIS_INVENTORY_PAGE_BINDING_INVALID')
        start, end = _utc(page['startedAt']), _utc(page['completedAt'])
        sm, em = _mono(page['startedMonotonic']), _mono(page['completedMonotonic'])
        if end < start or em < sm or previous_end is not None and (start < previous_end or sm < previous_mono):
            _bad('KIS_INVENTORY_CLOCK_INVALID')
        previous_end, previous_mono = end, em
        body = page['body']
        if type(body) is not dict or body.get('rt_cd') != '0' or page['canonicalParsedBodyHash'] != _hash(body):
            _bad('KIS_INVENTORY_RESPONSE_REJECTED')
        rows = body.get(output)
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            _bad('KIS_INVENTORY_ROWS_INVALID')
        row_count += len(rows)
        if row_count > MAX_ROWS:
            _bad('KIS_INVENTORY_ROW_LIMIT')
        if output == 'output1':
            auxiliary = body.get('output2')
            if auxiliary is None:
                state = 'ABSENT'
            elif type(auxiliary) in (dict, list):
                if type(auxiliary) is list and any(type(row) is not dict for row in auxiliary):
                    _bad('KIS_INVENTORY_AUXILIARY_INVALID')
                state = 'REPORTED' if auxiliary else 'EMPTY_UNREPORTED'
            else:
                _bad('KIS_INVENTORY_AUXILIARY_INVALID')
            # The states concern reported shape, never cash-role or totals.
            auxiliary_state = state if index == 0 else (auxiliary_state if auxiliary_state == state else 'MIXED_PAGE_SHAPES')
        for row in rows:
            identity = _identity(scope_id, row)
            if identity is None:
                missing_identity_count += 1
                continue
            value_hash = _hash(row)
            if identity in identities:
                if identities[identity] != value_hash:
                    _bad('KIS_INVENTORY_ROW_IDENTITY_CONFLICT')
                duplicate_count += 1
            identities[identity] = value_hash
        continuation = page['responseContinuation']
        if continuation not in (None, '', 'D', 'E', 'F', 'M'):
            _bad('KIS_INVENTORY_CONTINUATION_INVALID')
        if continuation in ('F', 'M'):
            fk, nk = _cursor(body, context)
            if (fk, nk) in cursors:
                _bad('KIS_INVENTORY_CONTINUATION_REPEATED')
            cursors.add((fk, nk))
        elif index != len(pages) - 1:
            _bad('KIS_INVENTORY_PAGE_AFTER_TERMINAL_OR_UNKNOWN')
    last = pages[-1]['responseContinuation']
    if last in ('F', 'M') and len(pages) != max_pages:
        _bad('KIS_INVENTORY_UNEXPLAINED_PARTIAL_CAPTURE')
    pagination = 'COMPLETE' if last in ('D', 'E') else 'PARTIAL' if last in ('F', 'M') else 'UNKNOWN'
    return {'scopeId': scope_id, 'path': definition[1], 'trId': tr_id,
        'queryScope': dict(query), 'scopeMeaning': definition[6], 'pageCount': len(pages),
        'rowCount': row_count, 'distinctIdentityCount': len(identities),
        'identicalDuplicateCount': duplicate_count, 'missingIdentityCount': missing_identity_count,
        'auxiliaryOutputState': auxiliary_state, 'paginationStatus': pagination,
        'startedAt': pages[0]['startedAt'], 'completedAt': pages[-1]['completedAt']}


def validate_inventory_capture(capture):
    """Recompute capture structure and page chain; not producer authentication."""
    keys = {'schemaVersion', 'status', 'provider', 'accountBinding', 'readerSettingsHash', 'accountQueryHash',
        'maxPages', 'scopes', 'startedAt', 'completedAt', 'startedMonotonic', 'completedMonotonic',
        'hashMeaning', 'retrievalTimeMeaning', 'privateValuesLocalOnly', 'contentHash'} | set(FALSE_CLAIMS)
    if type(capture) is not dict or set(capture) != keys:
        _bad()
    if capture['contentHash'] != _hash({k: v for k, v in capture.items() if k != 'contentHash'}):
        _bad('KIS_INVENTORY_CAPTURE_HASH_MISMATCH')
    if (capture['schemaVersion'] != SCHEMA or capture['status'] != STATUS or capture['provider'] != 'kis'
        or capture['hashMeaning'] != HASH_MEANING or capture['retrievalTimeMeaning'] != TIME_MEANING
        or capture['privateValuesLocalOnly'] is not True or any(capture[k] is not False for k in FALSE_CLAIMS)
        or any(type(capture[k]) is not str or not _HASH.fullmatch(capture[k]) for k in ('readerSettingsHash', 'accountQueryHash'))):
        _bad('KIS_INVENTORY_CAPTURE_BINDING_INVALID')
    _profile(capture['accountBinding'])
    maximum = _limit(capture['maxPages'])
    if type(capture['scopes']) is not list or len(capture['scopes']) != len(SCOPE_DEFINITIONS):
        _bad('KIS_INVENTORY_SCOPE_INVALID')
    facts = [_scope_facts(scope, definition, maximum) for scope, definition in zip(capture['scopes'], SCOPE_DEFINITIONS)]
    pages = [page for scope in capture['scopes'] for page in scope['pages']]
    for current, previous in zip(pages[1:], pages):
        if (_utc(current['startedAt']) < _utc(previous['completedAt'])
            or _mono(current['startedMonotonic']) < _mono(previous['completedMonotonic'])):
            _bad('KIS_INVENTORY_CLOCK_INVALID')
    for key in ('startedAt', 'startedMonotonic'):
        if capture[key] != pages[0][key]:
            _bad('KIS_INVENTORY_CLOCK_INVALID')
    for key in ('completedAt', 'completedMonotonic'):
        if capture[key] != pages[-1][key]:
            _bad('KIS_INVENTORY_CLOCK_INVALID')
    return facts


def _summary_body(*, scopes, started_at, completed_at, assessed_at, source_hash, monotonic_elapsed):
    start, end, assessed = _utc(started_at), _utc(completed_at), _utc(assessed_at)
    if end < start:
        _bad('KIS_INVENTORY_CLOCK_INVALID')
    elapsed = _mono(monotonic_elapsed)
    fresh = (0 <= (assessed - start).total_seconds() <= FRESHNESS_SECONDS
        and start <= end <= assessed and elapsed <= FRESHNESS_SECONDS)
    reasons = ['KIS_INVENTORY_SCOPES_DO_NOT_PROVE_FULL_ACCOUNT', 'KIS_INVENTORY_ACCOUNT_BASELINE_AND_ACTUAL_COSTS_UNVERIFIED']
    if not fresh:
        reasons.append('KIS_INVENTORY_LOCAL_RETRIEVAL_STALE_OR_CLOCK_INVALID')
    for row in scopes:
        if row['paginationStatus'] != 'COMPLETE':
            reasons.append('KIS_INVENTORY_' + row['scopeId'] + '_PAGINATION_' + row['paginationStatus'])
        if row['missingIdentityCount']:
            reasons.append('KIS_INVENTORY_' + row['scopeId'] + '_ROW_IDENTITIES_UNVERIFIED')
        if row['identicalDuplicateCount']:
            reasons.append('KIS_INVENTORY_' + row['scopeId'] + '_IDENTICAL_PAGE_OVERLAP')
    return {'schemaVersion': SUMMARY_SCHEMA, 'status': STATUS, 'provider': 'kis',
        'scopes': deepcopy(scopes), 'startedAt': started_at, 'completedAt': completed_at,
        'assessedAt': assessed_at, 'freshnessBoundarySeconds': FRESHNESS_SECONDS,
        'collectionMonotonicDurationSeconds': elapsed,
        'freshnessExpiresAt': (start + timedelta(seconds=FRESHNESS_SECONDS)).isoformat(),
        'freshnessStatus': 'WITHIN_LOCAL_15_SECOND_BOUNDARY' if fresh else 'STALE_OR_CLOCK_INVALID',
        'uncoveredScopes': list(UNCOVERED_SCOPES), 'reasons': reasons,
        'cashAmountsDerived': False, 'currencyConversionPerformed': False,
        'hashMeaning': HASH_MEANING, 'retrievalTimeMeaning': TIME_MEANING,
        'sourceCaptureHash': source_hash, **FALSE_CLAIMS}


def safe_inventory_summary(capture, *, now=None):
    facts = validate_inventory_capture(capture)
    assessed = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(assessed, datetime) or assessed.tzinfo is None:
        _bad('KIS_INVENTORY_CLOCK_INVALID')
    body = _summary_body(scopes=facts, started_at=capture['startedAt'], completed_at=capture['completedAt'],
        assessed_at=assessed.astimezone(timezone.utc).isoformat(), source_hash=capture['contentHash'],
        monotonic_elapsed=capture['completedMonotonic'] - capture['startedMonotonic'])
    return {**body, 'contentHash': _hash(body)}


def validate_inventory_summary(summary):
    """Strict public shape/derivations only; a JSON hash is never attestation."""
    if type(summary) is not dict or type(summary.get('scopes')) is not list or len(summary['scopes']) != 4:
        _bad('KIS_INVENTORY_SUMMARY_INVALID')
    scopes = []
    for row, definition in zip(summary['scopes'], SCOPE_DEFINITIONS):
        if type(row) is not dict:
            _bad('KIS_INVENTORY_SUMMARY_INVALID')
        counts = ('pageCount', 'rowCount', 'distinctIdentityCount', 'identicalDuplicateCount', 'missingIdentityCount')
        if any(type(row.get(k)) is not int or not 0 <= row[k] <= MAX_ROWS for k in counts):
            _bad('KIS_INVENTORY_SUMMARY_INVALID')
        if not 1 <= row['pageCount'] <= 10 or row['rowCount'] != sum(row[k] for k in counts[2:]):
            _bad('KIS_INVENTORY_SUMMARY_INVALID')
        if type(row.get('paginationStatus')) is not str or row['paginationStatus'] not in {'COMPLETE', 'PARTIAL', 'UNKNOWN'}:
            _bad('KIS_INVENTORY_SUMMARY_INVALID')
        if type(row.get('auxiliaryOutputState')) is not str or row['auxiliaryOutputState'] not in ({'ABSENT', 'EMPTY_UNREPORTED', 'REPORTED', 'MIXED_PAGE_SHAPES'} if definition[4] == 'output1' else {'NOT_APPLICABLE'}):
            _bad('KIS_INVENTORY_SUMMARY_INVALID')
        _utc(row.get('startedAt')); _utc(row.get('completedAt'))
        expected = {'scopeId': definition[0], 'path': definition[1], 'trId': definition[2],
            'queryScope': dict(definition[5]), 'scopeMeaning': definition[6],
            **{k: row[k] for k in counts}, 'auxiliaryOutputState': row['auxiliaryOutputState'],
            'paginationStatus': row['paginationStatus'], 'startedAt': row['startedAt'], 'completedAt': row['completedAt']}
        if row != expected or set(row) != set(expected) or _utc(row['completedAt']) < _utc(row['startedAt']):
            _bad('KIS_INVENTORY_SUMMARY_INVALID')
        if scopes and _utc(row['startedAt']) < _utc(scopes[-1]['completedAt']):
            _bad('KIS_INVENTORY_SUMMARY_INVALID')
        scopes.append(expected)
    if (summary.get('startedAt') != scopes[0]['startedAt'] or summary.get('completedAt') != scopes[-1]['completedAt']
        or type(summary.get('sourceCaptureHash')) is not str or not _HASH.fullmatch(summary['sourceCaptureHash'])):
        _bad('KIS_INVENTORY_SUMMARY_INVALID')
    expected = _summary_body(scopes=scopes, started_at=summary['startedAt'], completed_at=summary['completedAt'],
        assessed_at=summary.get('assessedAt'), source_hash=summary['sourceCaptureHash'],
        monotonic_elapsed=summary.get('collectionMonotonicDurationSeconds'))
    expected['contentHash'] = _hash(expected)
    if set(summary) != set(expected) or _hash(summary) != _hash(expected):
        _bad('KIS_INVENTORY_SUMMARY_INVALID')
    return expected


class KisAccountInventoryReader:
    def __init__(self, reader, *, utc_now=None, monotonic=None):
        self.reader = reader
        self.utc_now = utc_now or (lambda: datetime.now(timezone.utc).isoformat())
        self.monotonic = monotonic or time.monotonic

    def observe(self, *, max_pages=2):
        maximum = _limit(max_pages)
        original = _binding(self.reader.settings)
        snapshot, account, profile = original
        def unchanged():
            if _binding(self.reader.settings) != original:
                _bad('KIS_INVENTORY_ACCOUNT_SCOPE_CHANGED')
        def call(function, *args, **kwargs):
            try:
                return function(*args, **kwargs)
            except KisAccountInventoryError:
                raise
            except (ValueError, OSError, RuntimeError, TypeError, KeyError, ArithmeticError):
                _bad('KIS_INVENTORY_READER_UNAVAILABLE')
        call(self.reader._credentials, 'kis')
        unchanged()
        token = call(self.reader._kis_token)
        if type(token) is not str or not token or len(token) > 16384:
            _bad('KIS_INVENTORY_AUTHENTICATION_INVALID')
        unchanged()
        scopes = []
        for definition in SCOPE_DEFINITIONS:
            scope_id, path, tr_id, context, _, base_query, _ = definition
            pages = []
            fk = nk = ''
            for index in range(maximum):
                unchanged()
                call(self.reader._pace_kis)
                unchanged()
                query = {**base_query, 'CTX_AREA_FK' + context: fk, 'CTX_AREA_NK' + context: nk}
                headers = {'authorization': 'Bearer ' + token, 'appkey': self.reader.settings['KIS_APP_KEY'],
                    'appsecret': self.reader.settings['KIS_APP_SECRET'], 'tr_id': tr_id,
                    'tr_cont': 'N' if index else '', 'custtype': 'P'}
                start, start_mono = self.utc_now(), self.monotonic()
                response_pair = call(self.reader._get, 'kis', path, query={**account, **query}, headers=headers)
                if type(response_pair) not in (tuple, list) or len(response_pair) != 2:
                    _bad('KIS_INVENTORY_RESPONSE_PAIR_INVALID')
                body, response = response_pair
                end, end_mono = self.utc_now(), self.monotonic()
                unchanged()
                if type(response) is not dict:
                    _bad('KIS_INVENTORY_HEADERS_INVALID')
                pages.append({'queryWithoutAccount': query, 'responseTrId': response.get('tr_id'),
                    'responseContinuation': response.get('tr_cont'), 'startedAt': start, 'completedAt': end,
                    'startedMonotonic': start_mono, 'completedMonotonic': end_mono,
                    'body': deepcopy(body), 'canonicalParsedBodyHash': _hash(body)})
                partial = {'scopeId': scope_id, 'pages': pages}
                # The current prefix is a valid bounded capture with this page
                # as its temporary cap. This rejects errors before another GET.
                _scope_facts(partial, definition, len(pages))
                if response.get('tr_cont') not in ('F', 'M'):
                    break
                fk, nk = _cursor(body, context)
            scopes.append({'scopeId': scope_id, 'pages': pages})
        unchanged()
        first, last = scopes[0]['pages'][0], scopes[-1]['pages'][-1]
        body = {'schemaVersion': SCHEMA, 'status': STATUS, 'provider': 'kis',
            'accountBinding': profile, 'readerSettingsHash': snapshot, 'accountQueryHash': _hash(account),
            'maxPages': maximum, 'scopes': scopes, 'startedAt': first['startedAt'], 'completedAt': last['completedAt'],
            'startedMonotonic': first['startedMonotonic'], 'completedMonotonic': last['completedMonotonic'],
            'hashMeaning': HASH_MEANING, 'retrievalTimeMeaning': TIME_MEANING, 'privateValuesLocalOnly': True,
            **FALSE_CLAIMS}
        result = {**body, 'contentHash': _hash(body)}
        validate_inventory_capture(result)
        return result
