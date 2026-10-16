"""Local reported KIS USD facts, never a ledger or financial authority producer.

This module has no broker, settings, runtime, order, or filesystem imports. The
collector receives the existing normal ReadOnlyBrokerReader. Its private result
contains account responses and must stay in protected local memory; only
safe_observation_summary is intended for logs/UI. Hashes cover canonical parsed
JSON, not original HTTP bytes. No undocumented fee/net-cash equation is applied.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import time

SCHEMA = 'kis-usd-reported-observation-v1'
PATHS = {
    'SETTLEMENT_BASIS_CASH': ('/uapi/overseas-stock/v1/trading/inquire-paymt-stdr-balance', 'CTRP6010R'),
    'REPORTED_PERIOD_TRANSACTIONS': ('/uapi/overseas-stock/v1/trading/inquire-period-trans', 'CTOS4001R'),
    'PRESENT_BALANCE': ('/uapi/overseas-stock/v1/trading/inquire-present-balance', 'CTRP6504R'),
}
FALSE_CLAIMS = {
    'actualFeeObserved': False, 'finalPostingAuthority': False,
    'accountBaselineEstablished': False, 'accountFeeRateApproved': False,
    'usdCashReconciled': False, 'settlementFinalityVerified': False,
    'ledgerInitialized': False, 'orderAuthorityGranted': False,
    'fxFreshnessVerified': False, 'productionReady': False,
}
_SCOPE_KEYS = ('KIS_APP_KEY', 'KIS_APP_SECRET', 'KIS_ACCOUNT_NO',
               'KIS_ACCOUNT_PRODUCT_CODE', 'KIS_BASE_URL', 'KIS_ENV')
_ORIGIN = 'https://openapi.koreainvestment.com:9443'
_NUMBER = re.compile(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)\Z', re.ASCII)
_SHA = re.compile(r'[0-9a-f]{64}\Z', re.ASCII)


class KisUsdObservationError(ValueError):
    """Fixed safe code only. Never include remote body/account/token."""


def _bad(code='KIS_USD_OBSERVATION_INVALID'):
    raise KisUsdObservationError(code)


def canonical_hash(body):
    try:
        raw = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, OverflowError):
        _bad()
    return hashlib.sha256(raw).hexdigest()


def decode_usd_response(raw):
    """Strict JSON only for the three new observation routes; legacy unchanged."""
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                _bad('KIS_USD_DUPLICATE_JSON_KEY')
            result[key] = value
        return result
    def constant(_):
        _bad('KIS_USD_JSON_CONSTANT_INVALID')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def _date(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{8}', value):
        _bad('KIS_USD_EXPLICIT_DATE_REQUIRED')
    try:
        datetime.strptime(value, '%Y%m%d')
    except ValueError:
        _bad('KIS_USD_EXPLICIT_DATE_REQUIRED')
    return value


def _number(value):
    # Keep the reported text/sign, including explicit zero. No float conversion,
    # fee default, percentage derivation, or precision-losing arithmetic.
    if not isinstance(value, str) or len(value) > 128 or not _NUMBER.fullmatch(value):
        _bad('KIS_USD_REPORTED_NUMBER_INVALID')
    try:
        if not Decimal(value).is_finite():
            _bad('KIS_USD_REPORTED_NUMBER_INVALID')
    except InvalidOperation:
        _bad('KIS_USD_REPORTED_NUMBER_INVALID')
    return value


def _rows(body, key):
    rows = body.get(key)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        _bad('KIS_USD_RESPONSE_SHAPE_INVALID')
    return rows


def _clock(value):
    if not isinstance(value, str):
        _bad()
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            _bad()
    except (ValueError, TypeError, OverflowError):
        _bad()
    return parsed


def _mono(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        _bad()
    return value


def _scope(settings):
    if settings.get('KIS_ENV', '').lower() not in ('', 'real', 'live'):
        _bad('KIS_USD_REAL_ENVIRONMENT_REQUIRED')
    if settings.get('KIS_BASE_URL', '').rstrip('/') not in ('', _ORIGIN):
        _bad('KIS_USD_OFFICIAL_ORIGIN_REQUIRED')
    snapshot = {key: settings.get(key, '') for key in _SCOPE_KEYS}
    if any(not isinstance(value, str) for value in snapshot.values()):
        _bad('KIS_USD_ACCOUNT_BINDING_INVALID')
    if not snapshot['KIS_APP_KEY'] or not snapshot['KIS_APP_SECRET']:
        _bad('KIS_USD_ACCOUNT_BINDING_INVALID')
    account = snapshot['KIS_ACCOUNT_NO'].strip().replace(' ', '')
    product = snapshot['KIS_ACCOUNT_PRODUCT_CODE'].strip()
    if not re.fullmatch(r'[0-9]{2}', product):
        _bad('KIS_USD_ACCOUNT_BINDING_INVALID')
    if re.fullmatch(r'[0-9]{8}', account):
        cano = account
    elif re.fullmatch(r'[0-9]{8}-?[0-9]{2}', account):
        cano, suffix = account[:8], account[-2:]
        if suffix != product:
            _bad('KIS_USD_ACCOUNT_BINDING_INVALID')
    else:
        _bad('KIS_USD_ACCOUNT_BINDING_INVALID')
    return canonical_hash(snapshot), {'CANO': cano, 'ACNT_PRDT_CD': product}


def _public_query(kind, dates):
    if kind == 'SETTLEMENT_BASIS_CASH':
        if set(dates) != {'basis_date'}:
            _bad()
        return {'BASS_DT': _date(dates['basis_date']), 'WCRC_FRCR_DVSN_CD': '02', 'INQR_DVSN_CD': '01'}
    if kind == 'REPORTED_PERIOD_TRANSACTIONS':
        if set(dates) != {'start_date', 'end_date'}:
            _bad()
        start, end = _date(dates['start_date']), _date(dates['end_date'])
        if start > end:
            _bad('KIS_USD_DATE_RANGE_INVALID')
        return {'ERLM_STRT_DT': start, 'ERLM_END_DT': end, 'OVRS_EXCG_CD': '', 'PDNO': '',
                'SLL_BUY_DVSN_CD': '00', 'LOAN_DVSN_CD': '', 'CTX_AREA_FK100': '', 'CTX_AREA_NK100': ''}
    if kind == 'PRESENT_BALANCE' and not dates:
        return {'WCRC_FRCR_DVSN_CD': '02', 'NATN_CD': '840', 'TR_MKET_CD': '00', 'INQR_DVSN_CD': '01'}
    _bad('KIS_USD_OBSERVATION_SCOPE_INVALID')


def _header(headers, name):
    value = headers.get(name)
    if value is not None and (not isinstance(value, str) or len(value) > 256):
        _bad('KIS_USD_RESPONSE_HEADERS_INVALID')
    return value


def _continuation(headers):
    value = _header(headers, 'tr_cont')
    if value in (None, ''):
        return 'UNKNOWN'
    if value in ('D', 'E'):
        return 'COMPLETE'
    if value in ('M', 'F'):
        return 'MORE'
    _bad('KIS_USD_CONTINUATION_INVALID')


class KisUsdObservationReader:
    """Bounded GET observations through an injected normal read-only reader.

    Construction does no IO. No cache/disk/ledger APIs exist here. Callers must
    supply explicit broker query dates; dates are never converted between zones.
    """
    def __init__(self, reader, *, utc_now=None, monotonic=None):
        self.reader = reader
        self._utc_now = utc_now or (lambda: datetime.now(timezone.utc).isoformat())
        self._monotonic = monotonic or time.monotonic

    def observe(self, kind, *, max_pages=3, **dates):
        if kind not in PATHS or isinstance(max_pages, bool) or not isinstance(max_pages, int) or not 1 <= max_pages <= 10:
            _bad('KIS_USD_OBSERVATION_SCOPE_INVALID')
        public = _public_query(kind, dates)  # All scope validation precedes OAuth.
        scope_hash, account = _scope(self.reader.settings)
        self.reader._credentials('kis')
        token = self.reader._kis_token()  # Existing process-memory token cache only.
        path, tr_id = PATHS[kind]
        pages, seen_cursors = [], set()
        query, continuation = {**account, **public}, ''
        completion, reason = 'PARTIAL', 'KIS_USD_PAGE_LIMIT'
        total_rows, total_bytes = 0, 0
        for index in range(max_pages):
            if _scope(self.reader.settings)[0] != scope_hash:
                _bad('KIS_USD_ACCOUNT_SCOPE_CHANGED')
            self.reader._pace_kis()  # Preserve normal shared 2.1s pacing.
            started_utc, started_mono = self._utc_now(), self._monotonic()
            body, headers = self.reader._get('kis', path, query=dict(query), headers={
                'authorization': 'Bearer ' + token, 'appkey': self.reader.settings['KIS_APP_KEY'],
                'appsecret': self.reader.settings['KIS_APP_SECRET'], 'tr_id': tr_id,
                'tr_cont': continuation, 'custtype': 'P'})
            completed_utc, completed_mono = self._utc_now(), self._monotonic()
            if _scope(self.reader.settings)[0] != scope_hash:
                _bad('KIS_USD_ACCOUNT_SCOPE_CHANGED')
            if not isinstance(body, dict) or body.get('rt_cd') != '0' or not isinstance(headers, dict) or headers.get('tr_id') != tr_id:
                _bad('KIS_USD_BROKER_RESPONSE_REJECTED')
            if completed_mono < started_mono or _clock(completed_utc) < _clock(started_utc):
                _bad('KIS_USD_RECEIVE_CLOCK_INVALID')
            body_hash = canonical_hash(body)
            canonical_bytes = len(json.dumps(body, ensure_ascii=False, allow_nan=False).encode('utf-8'))
            total_bytes += canonical_bytes
            total_rows += sum(len(value) for value in body.values() if isinstance(value, list))
            if total_rows > 10000 or total_bytes > 16 * 1024 * 1024:
                _bad('KIS_USD_COLLECTION_LIMIT')
            state = _continuation(headers)
            page = {'pageIndex': index, 'requestContinuation': continuation,
                    'queryWithoutAccount': {key: value for key, value in query.items() if key not in account},
                    'body': body, 'canonicalParsedBodyHash': body_hash,
                    'responseTrId': headers['tr_id'], 'responseContinuation': _header(headers, 'tr_cont'),
                    'httpDateRaw': _header(headers, 'date'), 'httpAgeRaw': _header(headers, 'age'),
                    'startedAt': started_utc, 'completedAt': completed_utc,
                    'startedMonotonic': started_mono, 'completedMonotonic': completed_mono}
            pages.append(page)
            if state == 'COMPLETE':
                completion, reason = 'COMPLETE', ''
                break
            if state == 'UNKNOWN':
                completion, reason = 'UNKNOWN', 'KIS_USD_TERMINAL_HEADER_ABSENT'
                break
            if kind == 'REPORTED_PERIOD_TRANSACTIONS':
                cursor = (body.get('ctx_area_fk100'), body.get('ctx_area_nk100'))
                if any(not isinstance(value, str) or len(value) > 100 for value in cursor) or not any(cursor) or cursor in seen_cursors:
                    _bad('KIS_USD_CURSOR_INVALID_OR_REPEATED')
                seen_cursors.add(cursor)
                query.update(CTX_AREA_FK100=cursor[0], CTX_AREA_NK100=cursor[1])
            elif len(pages) > 1 and pages[-2]['canonicalParsedBodyHash'] == body_hash:
                _bad('KIS_USD_REPEATED_CONTINUATION_PAGE')
            continuation = 'N'
        result = {'schemaVersion': SCHEMA, 'status': 'OBSERVED', 'kind': kind,
                  'path': path, 'trId': tr_id, 'credentialConfigurationHash': scope_hash,
                  'accountQueryHash': canonical_hash(account), 'queryWithoutAccount': public,
                  'paginationStatus': completion, 'paginationReason': reason, 'pages': pages,
                  'hashMeaning': 'CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_BYTES',
                  'retrievalTimeMeaning': 'LOCAL_GET_START_AND_COMPLETION_NOT_BROKER_POSTING_TIME',
                  'authenticatedProducerAttested': False, 'privateValuesLocalOnly': True, **FALSE_CLAIMS}
        result['contentHash'] = canonical_hash(result)
        parse_reported_observation(result)  # Validate without changing capture.
        return result


def parse_reported_observation(observation):
    """Recheck capture structure/hash, expose reported literals without authority."""
    if not isinstance(observation, dict) or observation.get('schemaVersion') != SCHEMA:
        _bad()
    expected_keys = {'schemaVersion', 'status', 'kind', 'path', 'trId', 'credentialConfigurationHash',
                     'accountQueryHash', 'queryWithoutAccount', 'paginationStatus', 'paginationReason',
                     'pages', 'hashMeaning', 'retrievalTimeMeaning', 'authenticatedProducerAttested',
                     'privateValuesLocalOnly', 'contentHash'} | set(FALSE_CLAIMS)
    if set(observation) != expected_keys:
        _bad('KIS_USD_OBSERVATION_SHAPE_INVALID')
    body = dict(observation)
    expected = body.pop('contentHash', None)
    if not isinstance(expected, str) or not _SHA.fullmatch(expected) or canonical_hash(body) != expected:
        _bad('KIS_USD_OBSERVATION_HASH_MISMATCH')
    kind = body.get('kind')
    if kind not in PATHS or (body.get('path'), body.get('trId')) != PATHS[kind] or body.get('status') != 'OBSERVED':
        _bad()
    if any(body.get(key) is not False for key in FALSE_CLAIMS) or body.get('authenticatedProducerAttested') is not False or body.get('privateValuesLocalOnly') is not True:
        _bad('KIS_USD_OBSERVATION_AUTHORITY_FORBIDDEN')
    if body.get('hashMeaning') != 'CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_BYTES' or body.get('retrievalTimeMeaning') != 'LOCAL_GET_START_AND_COMPLETION_NOT_BROKER_POSTING_TIME':
        _bad()
    reasons = {'COMPLETE': '', 'PARTIAL': 'KIS_USD_PAGE_LIMIT', 'UNKNOWN': 'KIS_USD_TERMINAL_HEADER_ABSENT'}
    if body.get('paginationStatus') not in reasons or body.get('paginationReason') != reasons[body['paginationStatus']]:
        _bad('KIS_USD_PAGINATION_INVALID')
    if any(not isinstance(body.get(key), str) or not _SHA.fullmatch(body[key]) for key in ('credentialConfigurationHash', 'accountQueryHash')):
        _bad()
    pages = body.get('pages')
    if not isinstance(pages, list) or not 1 <= len(pages) <= 10:
        _bad()
    public = body.get('queryWithoutAccount')
    if not isinstance(public, dict):
        _bad()
    dates = ({'basis_date': public.get('BASS_DT')} if kind == 'SETTLEMENT_BASIS_CASH' else
             {'start_date': public.get('ERLM_STRT_DT'), 'end_date': public.get('ERLM_END_DT')} if kind == 'REPORTED_PERIOD_TRANSACTIONS' else {})
    if _public_query(kind, dates) != public:
        _bad('KIS_USD_QUERY_BINDING_INVALID')
    values, cumulative_rows, seen_cursors = [], 0, set()
    expected_query = dict(public)
    for i, page in enumerate(pages):
        page_keys = {'pageIndex', 'requestContinuation', 'queryWithoutAccount', 'body',
                     'canonicalParsedBodyHash', 'responseTrId', 'responseContinuation',
                     'httpDateRaw', 'httpAgeRaw', 'startedAt', 'completedAt',
                     'startedMonotonic', 'completedMonotonic'}
        if not isinstance(page, dict) or set(page) != page_keys:
            _bad('KIS_USD_OBSERVATION_SHAPE_INVALID')
        if not isinstance(page, dict) or page.get('pageIndex') != i or page.get('requestContinuation') != ('' if i == 0 else 'N') or page.get('responseTrId') != PATHS[kind][1]:
            _bad()
        payload = page.get('body')
        if not isinstance(payload, dict) or payload.get('rt_cd') != '0' or canonical_hash(payload) != page.get('canonicalParsedBodyHash'):
            _bad('KIS_USD_PAGE_HASH_MISMATCH')
        if page.get('queryWithoutAccount') != expected_query:
            _bad('KIS_USD_QUERY_BINDING_INVALID')
        if _clock(page.get('completedAt')) < _clock(page.get('startedAt')) or _mono(page.get('completedMonotonic')) < _mono(page.get('startedMonotonic')):
            _bad('KIS_USD_RECEIVE_CLOCK_INVALID')
        continuation = page.get('responseContinuation')
        state = _continuation({'tr_cont': continuation})
        if i < len(pages) - 1 and state != 'MORE':
            _bad('KIS_USD_PAGINATION_INVALID')
        if state == 'MORE':
            if kind == 'REPORTED_PERIOD_TRANSACTIONS':
                cursor = (payload.get('ctx_area_fk100'), payload.get('ctx_area_nk100'))
                if any(not isinstance(value, str) or len(value) > 100 for value in cursor) or not any(cursor) or cursor in seen_cursors:
                    _bad('KIS_USD_CURSOR_INVALID_OR_REPEATED')
                seen_cursors.add(cursor)
                expected_query.update(CTX_AREA_FK100=cursor[0], CTX_AREA_NK100=cursor[1])
            elif i > 0 and pages[i-1]['canonicalParsedBodyHash'] == page['canonicalParsedBodyHash']:
                _bad('KIS_USD_REPEATED_CONTINUATION_PAGE')
        if i == len(pages)-1 and body.get('paginationStatus') != ('COMPLETE' if state == 'COMPLETE' else 'UNKNOWN' if state == 'UNKNOWN' else 'PARTIAL'):
            _bad('KIS_USD_PAGINATION_INVALID')
        _header(page, 'httpDateRaw')
        _header(page, 'httpAgeRaw')
        rows = _rows(payload, 'output1' if kind == 'REPORTED_PERIOD_TRANSACTIONS' else 'output2')
        cumulative_rows += len(rows)
        if cumulative_rows > 10000:
            _bad('KIS_USD_COLLECTION_LIMIT')
        for j, row in enumerate(rows):
            currency = row.get('crcy_cd')
            if not isinstance(currency, str) or not re.fullmatch(r'[A-Z]{3}', currency):
                _bad('KIS_USD_CURRENCY_UNPROVEN')
            # Keep all currencies in protected original pages. Public/projection
            # USD facts never include currencyless output2 totals as USD.
            if currency != 'USD':
                continue
            item = {'pageIndex': i, 'rowIndex': j, 'currency': currency, 'reportedRowHash': canonical_hash(row)}
            if kind == 'REPORTED_PERIOD_TRANSACTIONS':
                for key in ('trad_dt', 'sttl_dt'):
                    item[key] = _date(row.get(key))
                if row.get('sll_buy_dvsn_cd') not in ('01', '02') or not isinstance(row.get('pdno'), str) or not row['pdno']:
                    _bad('KIS_USD_TRANSACTION_IDENTITY_UNPROVEN')
                item.update({key: row[key] for key in ('pdno', 'sll_buy_dvsn_cd')})
                for key in ('ccld_qty', 'tr_frcr_amt2', 'frcr_excc_amt_1', 'dmst_frcr_fee1', 'frcr_fee1'):
                    item[key] = _number(row.get(key))
                # Signed adjustments are preserved; no BUY/SELL net equation,
                # order association, sum, deduplication, or implied tax formula.
                item['reportedCostRole'] = 'ROW_FIELDS_ONLY_NO_ORDER_ALLOCATION_OR_FINALITY'
            else:
                item['frcr_dncl_amt_2'] = _number(row.get('frcr_dncl_amt_2'))
                item['reportedCashRole'] = ('SETTLEMENT_BASIS_CURRENCY_DEPOSIT' if kind == 'SETTLEMENT_BASIS_CASH'
                                            else 'PRESENT_FOREIGN_USABLE_AMOUNT_NOT_SETTLEMENT_BASELINE')
            values.append(item)
    result = dict(observation)
    result['reportedUsdRows'] = values
    if kind != 'REPORTED_PERIOD_TRANSACTIONS':
        literals = {Decimal(row['frcr_dncl_amt_2']) for row in values}
        result['usdRowStatus'] = 'ABSENT_NOT_ZERO' if not values else 'CONFLICTING_REPORTED_BALANCES' if len(literals) > 1 else 'REPORTED_VALUE'
        result['reportedUsdCashLiteral'] = values[0]['frcr_dncl_amt_2'] if len(literals) == 1 else None
    # Derived observations get their own hash; preserve original capture hash.
    result['captureContentHash'] = result.pop('contentHash')
    result['contentHash'] = canonical_hash(result)
    return result


def safe_observation_summary(parsed):
    """No account IDs, balances, instruments, fees, raw messages, tokens or keys."""
    if not isinstance(parsed, dict):
        _bad()
    capture = dict(parsed)
    capture.pop('reportedUsdRows', None)
    capture.pop('usdRowStatus', None)
    capture.pop('reportedUsdCashLiteral', None)
    capture.pop('contentHash', None)
    capture['contentHash'] = capture.pop('captureContentHash', None)
    checked = parse_reported_observation(capture)
    if checked != parsed:
        _bad('KIS_USD_DERIVED_FACTS_MISMATCH')
    return {'schemaVersion': SCHEMA, 'status': parsed.get('status'), 'kind': parsed.get('kind'),
            'paginationStatus': parsed.get('paginationStatus'), 'paginationReason': parsed.get('paginationReason'),
            'pageCount': len(parsed.get('pages', [])), 'usdRowCount': len(parsed.get('reportedUsdRows', [])),
            'usdRowStatus': parsed.get('usdRowStatus', 'REPORTED_ROWS_ONLY'),
            'contentHash': parsed.get('contentHash'), **FALSE_CLAIMS}
