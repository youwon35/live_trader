"""Reported currency-margin roles only. No ledger, settings, or order authority."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import re
import time

from .kis_usd_observations import (_scope, canonical_hash, decode_usd_response,
                                   FALSE_CLAIMS, KisUsdObservationError)

SCHEMA = 'kis-margin-currency-reported-observation/v1'
PATH = '/uapi/overseas-stock/v1/trading/foreign-margin'
TR_ID = 'TTTC2101R'
ROLE_FIELDS = {
    'frcr_dncl_amt1': 'REPORTED_FOREIGN_DEPOSIT_NOT_SETTLEMENT_OR_OWNED_BASELINE',
    'ustl_buy_amt': 'REPORTED_UNSETTLED_BUY',
    'ustl_sll_amt': 'REPORTED_UNSETTLED_SELL',
    'frcr_rcvb_amt': 'REPORTED_FOREIGN_RECEIVABLE',
    'frcr_mgn_amt': 'REPORTED_FOREIGN_MARGIN',
    'frcr_gnrl_ord_psbl_amt': 'REPORTED_GENERAL_ORDER_CAPACITY_NOT_CASH',
    'frcr_ord_psbl_amt1': 'REPORTED_CONVERTED_ORDER_CAPACITY_NOT_CASH',
    'itgr_ord_psbl_amt': 'REPORTED_INTEGRATED_CAPACITY_NOT_USD_DEPOSIT',
    'bass_exrt': 'REPORTED_REFERENCE_FX_NOT_FRESH_MARKET_EVENT',
}
_NUMBER = re.compile(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)\Z', re.ASCII)
_HASH = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)

def bad(code='KIS_MARGIN_OBSERVATION_INVALID'):
    raise KisUsdObservationError(code)

def decode_margin_response(raw):
    decoded = decode_usd_response(raw)
    canonical_hash(decoded)  # Reject finite-JSON numeric overflow (e.g. 1e999) here.
    return decoded

def _utc(value):
    try:
        if type(value) is not str:bad()
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:bad()
        return parsed
    except (ValueError, TypeError, OverflowError):bad()

def _mono(value):
    if type(value) not in (float, int) or not math.isfinite(value) or value < 0:bad()
    return value

def _header(value):
    if value is not None and (type(value) is not str or len(value) > 256):bad()
    return value

def _number(value):
    if type(value) is not str or len(value) > 128 or not _NUMBER.fullmatch(value):bad('KIS_MARGIN_REPORTED_NUMBER_INVALID')
    try:
        if not Decimal(value).is_finite():bad('KIS_MARGIN_REPORTED_NUMBER_INVALID')
    except InvalidOperation:bad('KIS_MARGIN_REPORTED_NUMBER_INVALID')
    return value

def validate_observation(capture):
    keys = {'schemaVersion', 'status', 'provider', 'path', 'trId', 'credentialConfigurationHash',
        'accountQueryHash', 'queryWithoutAccount', 'body', 'canonicalParsedBodyHash', 'responseTrId',
        'responseContinuation', 'httpDateRaw', 'httpAgeRaw', 'startedAt', 'completedAt',
        'startedMonotonic', 'completedMonotonic', 'maxPages', 'attemptedPages',
        'authenticatedProducerAttested', 'privateValuesLocalOnly', 'hashMeaning',
        'retrievalTimeMeaning', 'contentHash'} | set(FALSE_CLAIMS)
    if type(capture) is not dict or set(capture) != keys:bad()
    value = {k: v for k, v in capture.items() if k != 'contentHash'}
    if (type(capture['contentHash']) is not str or not _HASH.fullmatch(capture['contentHash'])
        or canonical_hash(value) != capture['contentHash']):bad('KIS_MARGIN_CAPTURE_HASH_MISMATCH')
    if (capture['schemaVersion'] != SCHEMA or capture['status'] != 'OBSERVED_ONLY_NOT_FINANCIALLY_QUALIFIED'
        or capture['provider'] != 'kis' or capture['path'] != PATH or capture['trId'] != TR_ID
        or capture['responseTrId'] != TR_ID or capture['queryWithoutAccount'] != {}
        or type(capture['maxPages']) is not int or capture['maxPages'] != 1
        or type(capture['attemptedPages']) is not int or capture['attemptedPages'] != 1):bad()
    if (capture['authenticatedProducerAttested'] is not False or capture['privateValuesLocalOnly'] is not True
        or any(capture[k] is not False for k in FALSE_CLAIMS)):bad('KIS_MARGIN_AUTHORITY_FORBIDDEN')
    if (capture['hashMeaning'] != 'CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_BYTES'
        or capture['retrievalTimeMeaning'] != 'LOCAL_GET_TIMES_NOT_POSTING_SETTLEMENT_OR_FX_TIME'):bad()
    if any(type(capture[k]) is not str or not _HASH.fullmatch(capture[k]) for k in ('credentialConfigurationHash', 'accountQueryHash')):bad()
    if (_utc(capture['completedAt']) < _utc(capture['startedAt'])
        or _mono(capture['completedMonotonic']) < _mono(capture['startedMonotonic'])):bad('KIS_MARGIN_RECEIVE_CLOCK_INVALID')
    _header(capture['httpDateRaw']);_header(capture['httpAgeRaw'])
    continuation = _header(capture['responseContinuation'])
    if continuation not in (None, '', 'D', 'E', 'M', 'F'):bad('KIS_MARGIN_CONTINUATION_INVALID')
    body = capture['body']
    if (type(body) is not dict or body.get('rt_cd') != '0'
        or canonical_hash(body) != capture['canonicalParsedBodyHash']):bad('KIS_MARGIN_RESPONSE_INVALID')
    rows = body.get('output')
    if type(rows) is not list or len(rows) > 256 or any(type(row) is not dict for row in rows):bad('KIS_MARGIN_RESPONSE_SHAPE_INVALID')
    usd_rows, blank_rows = [], 0
    for row in rows:
        currency = row.get('crcy_cd')
        if type(currency) is not str or currency and not re.fullmatch(r'[A-Z]{3}', currency):bad('KIS_MARGIN_CURRENCY_UNPROVEN')
        if currency == '':
            # Official legacy reader filters blank currency padding. Preserve as
            # unknown only; do not interpret any amounts or infer USD zero.
            blank_rows += 1
            continue
        if type(row.get('natn_name')) is not str:bad('KIS_MARGIN_RESPONSE_SHAPE_INVALID')
        reported = {key: _number(row.get(key)) for key in ROLE_FIELDS}
        if currency == 'USD':usd_rows.append(reported)
    return {'rows': rows, 'usdRows': usd_rows, 'blankCurrencyRows': blank_rows,
        'paginationStatus': 'COMPLETE' if continuation in ('D', 'E') else 'PARTIAL' if continuation in ('M', 'F') else 'UNKNOWN'}

def safe_observation_summary(capture):
    """Only known schema names/counts/USD presence; no amounts or remote text."""
    checked = validate_observation(capture)
    rows, usd = checked['rows'], checked['usdRows']
    known = {'natn_name', 'crcy_cd', *ROLE_FIELDS}
    names = {key for row in rows for key in row if type(key) is str}
    presence = ('EXPLICIT_USD_ROWS_PRESENT_UNMERGED' if usd else
                'UNKNOWN_CURRENCY_ROWS' if checked['blankCurrencyRows'] else 'ABSENT_NOT_ZERO')
    return {'schemaVersion': SCHEMA, 'status': capture['status'], 'trId': TR_ID,
        'pageCount': 1, 'paginationStatus': checked['paginationStatus'],
        'rowCount': len(rows), 'usdRowCount': len(usd), 'usdPresence': presence,
        'blankCurrencyRowCount': checked['blankCurrencyRows'], 'recognizedFieldNames': sorted(names & known),
        'unreportedOtherFieldCount': len(names - known), 'reportedRoleNames': dict(ROLE_FIELDS),
        'cashAndCostArithmeticPerformed': False, 'hashMeaning': capture['hashMeaning'],
        'contentHash': capture['contentHash'], **FALSE_CLAIMS}

class KisMarginObservationReader:
    def __init__(self, reader, *, utc_now=None, monotonic=None):
        self.reader = reader
        self.utc_now = utc_now or (lambda: datetime.now(timezone.utc).isoformat())
        self.monotonic = monotonic or time.monotonic

    def observe(self, *, max_pages=1):
        if type(max_pages) is not int or max_pages != 1:bad('KIS_MARGIN_ONE_PAGE_ONLY')
        scope_hash, account = _scope(self.reader.settings)
        self.reader._credentials('kis')
        token = self.reader._kis_token()
        if _scope(self.reader.settings)[0] != scope_hash:bad('KIS_MARGIN_ACCOUNT_SCOPE_CHANGED')
        self.reader._pace_kis()
        if _scope(self.reader.settings)[0] != scope_hash:bad('KIS_MARGIN_ACCOUNT_SCOPE_CHANGED')
        start_utc, start_mono = self.utc_now(), self.monotonic()
        body, headers = self.reader._get('kis', PATH, query=account, headers={
            'authorization': 'Bearer ' + token, 'appkey': self.reader.settings['KIS_APP_KEY'],
            'appsecret': self.reader.settings['KIS_APP_SECRET'], 'tr_id': TR_ID, 'tr_cont': '', 'custtype': 'P'})
        end_utc, end_mono = self.utc_now(), self.monotonic()
        if _scope(self.reader.settings)[0] != scope_hash:bad('KIS_MARGIN_ACCOUNT_SCOPE_CHANGED')
        if type(headers) is not dict:bad('KIS_MARGIN_RESPONSE_INVALID')
        result = {'schemaVersion': SCHEMA, 'status': 'OBSERVED_ONLY_NOT_FINANCIALLY_QUALIFIED',
            'provider': 'kis', 'path': PATH, 'trId': TR_ID,
            'credentialConfigurationHash': scope_hash, 'accountQueryHash': canonical_hash(account),
            'queryWithoutAccount': {}, 'body': body, 'canonicalParsedBodyHash': canonical_hash(body),
            'responseTrId': headers.get('tr_id'), 'responseContinuation': headers.get('tr_cont'),
            'httpDateRaw': headers.get('date'), 'httpAgeRaw': headers.get('age'),
            'startedAt': start_utc, 'completedAt': end_utc,
            'startedMonotonic': start_mono, 'completedMonotonic': end_mono,
            'maxPages': 1, 'attemptedPages': 1, 'authenticatedProducerAttested': False,
            'privateValuesLocalOnly': True, 'hashMeaning': 'CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_BYTES',
            'retrievalTimeMeaning': 'LOCAL_GET_TIMES_NOT_POSTING_SETTLEMENT_OR_FX_TIME', **FALSE_CLAIMS}
        result['contentHash'] = canonical_hash(result)
        validate_observation(result)
        return result
