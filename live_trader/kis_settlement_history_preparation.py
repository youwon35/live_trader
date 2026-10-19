"""Bounded, public-only counts from an existing private KIS history capture.

No transport, credentials, settings, files, ledger or order APIs are used here.
Reported KRW amounts and registered FX remain private display roles. Neither a
settlement date nor a field named 'final' proves an automatic FX execution or
cash posting. The 90-day/two-page limits are local collection budgets, not an
assertion of the provider's undocumented maximum historical retention.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import re

from .kis_usd_observations import canonical_hash, parse_reported_observation

SCHEMA = 'kis-settlement-history-preparation-v1'
KIND = 'REPORTED_PERIOD_TRANSACTIONS'
PATH = '/uapi/overseas-stock/v1/trading/inquire-period-trans'
TR_ID = 'CTOS4001R'
MAXIMUM_CALENDAR_DAYS = 90
MAXIMUM_PAGES = 2
STATUS = 'REPORTED_HISTORY_ONLY_NOT_FINANCIALLY_QUALIFIED'
FIELD_ROLES = {
    'ccld_qty': 'REPORTED_QUANTITY_NOT_EXACT_ORDER_ALLOCATION',
    'tr_frcr_amt2': 'REPORTED_TRADE_FOREIGN_AMOUNT_NOT_ACTUAL_FX_EVENT',
    'frcr_excc_amt_1': 'REPORTED_FOREIGN_SETTLEMENT_AMOUNT_NOT_POSTING_PROOF',
    'dmst_frcr_fee1': 'REPORTED_DOMESTIC_FOREIGN_FEE_NOT_PERSONAL_RATE_AGREEMENT',
    'frcr_fee1': 'REPORTED_FOREIGN_FEE_NOT_PERSONAL_RATE_AGREEMENT',
    'wcrc_excc_amt': 'REPORTED_KRW_SETTLEMENT_DISPLAY_NOT_AUTOMATIC_FX_PROOF',
    'dmst_wcrc_fee': 'REPORTED_DOMESTIC_KRW_FEE_NOT_PERSONAL_RATE_AGREEMENT',
    'ovrs_wcrc_fee': 'REPORTED_OVERSEAS_KRW_FEE_NOT_PERSONAL_RATE_AGREEMENT',
    'erlm_exrt': 'REPORTED_REGISTERED_FX_NOT_ACTUAL_CONVERSION_EVENT',
}
FIELD_STATES = ('ABSENT', 'EMPTY_TEXT', 'REPORTED_DECIMAL_TEXT', 'INVALID_OR_UNPROVEN')
FLAGS = {
    'actualFeeObserved': False, 'actualPersonalFeeObserved': False,
    'actualFxExecutionObserved': False, 'finalPostingAuthority': False,
    'accountBaselineEstablished': False, 'accountFeeRateApproved': False,
    'usdCashReconciled': False, 'settlementFinalityVerified': False,
    'ledgerInitialized': False, 'orderAuthorityGranted': False,
    'productionReady': False, 'allAccountHistoryVerified': False,
    'orderAttributionVerified': False, 'reportedFieldArithmeticPerformed': False,
}
_FIXED = {
    'schemaVersion': SCHEMA, 'status': STATUS, 'kind': KIND, 'path': PATH, 'trId': TR_ID,
    'coverageScope': 'REQUESTED_REGISTRATION_DATE_WINDOW_AND_RETURNED_PAGES_ONLY',
    'providerMaximumLookback': 'UNCONFIRMED_IN_REVIEWED_PUBLIC_CONTRACT',
    'settlementFinality': 'UNKNOWN',
    'finalityIndicatorContract': 'NO_REVIEWED_EXPLICIT_ROW_FINALITY_FIELD',
    'actualFxEventContract': 'NO_REVIEWED_FX_EVENT_ID_EXECUTION_TIME_AND_POSTING_CONTRACT',
    'settlementDateRole': 'REPORTED_DATE_NOT_CASH_POSTING_CONFIRMATION',
    'retrievalTimeMeaning': 'LOCAL_GET_TIMES_NOT_FX_EXECUTION_OR_SETTLEMENT_POSTING_TIME',
    'hashMeaning': 'LOCAL_CANONICAL_CHECKSUM_NOT_AUTHENTICATED_FINANCIAL_AUTHORITY',
}
_SPAN_MEANING = 'EXPLICIT_REGISTRATION_QUERY_DATES_NOT_ALL_ACCOUNT_HISTORY'
_ROW_STATUSES = ('NO_REPORTED_USD_ROWS_IN_CAPTURE_NOT_ZERO_BALANCE', 'REPORTED_USD_ROWS_ONLY')
_PAGINATION = {'COMPLETE': '', 'PARTIAL': 'KIS_USD_PAGE_LIMIT',
    'UNKNOWN': 'KIS_USD_TERMINAL_HEADER_ABSENT'}
_COUNTS = ('returnedRowCount', 'usdRowCount', 'otherCurrencyRowCount',
    'duplicateUsdRowCount', 'unreviewedRowFieldCount')
_SHA = re.compile(r'[0-9a-f]{64}\Z', re.ASCII)
_NUMBER = re.compile(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)\Z', re.ASCII)
_KNOWN_ROW_FIELDS = frozenset(FIELD_ROLES) | frozenset({
    'trad_dt', 'sttl_dt', 'sll_buy_dvsn_cd', 'sll_buy_dvsn_name', 'pdno',
    'ovrs_item_name', 'amt_unit_ccld_qty', 'ft_ccld_unpr2', 'ovrs_stck_ccld_unpr',
    'tr_amt', 'crcy_cd', 'std_pdno', 'loan_dvsn_cd', 'loan_dvsn_name',
})


class KisSettlementHistoryError(ValueError):
    """A fixed safe code, never a remote message or private input value."""


def _require(condition, code='KIS_HISTORY_PUBLIC_SUMMARY_INVALID'):
    if not condition:
        raise KisSettlementHistoryError(code)


def _date(value):
    _require(type(value) is str and re.fullmatch(r'[0-9]{8}', value, re.ASCII) is not None,
        'KIS_HISTORY_EXPLICIT_DATE_REQUIRED')
    try:
        return datetime.strptime(value, '%Y%m%d').date()
    except ValueError:
        raise KisSettlementHistoryError('KIS_HISTORY_EXPLICIT_DATE_REQUIRED') from None


def _span(start, end):
    days = (_date(end) - _date(start)).days + 1
    _require(1 <= days <= MAXIMUM_CALENDAR_DAYS, 'KIS_HISTORY_LOCAL_DATE_BUDGET_EXCEEDED')
    return {'startDate': start, 'endDate': end, 'calendarDays': days,
        'meaning': _SPAN_MEANING}


def validate_history_request(*, start_date, end_date, max_pages=2):
    """Validate the public local budget before credentials or any account GET."""
    span = _span(start_date, end_date)
    _require(type(max_pages) is int and 1 <= max_pages <= MAXIMUM_PAGES,
        'KIS_HISTORY_LOCAL_PAGE_BUDGET_INVALID')
    return {'startDate': span['startDate'], 'endDate': span['endDate'],
        'calendarDays': span['calendarDays'], 'maxPages': max_pages}


def _time(value):
    _require(type(value) is str)
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        _require(result.tzinfo is not None and result.utcoffset().total_seconds() == 0)
        return result
    except (ValueError, TypeError, OverflowError, AttributeError):
        raise KisSettlementHistoryError('KIS_HISTORY_PUBLIC_SUMMARY_INVALID') from None


def _count(value, maximum=10000):
    return type(value) is int and 0 <= value <= maximum


def _field_state(row, key):
    if key not in row:
        return 'ABSENT'
    value = row[key]
    if type(value) is str and value == '':
        return 'EMPTY_TEXT'
    if type(value) is not str or len(value) > 128 or _NUMBER.fullmatch(value) is None:
        return 'INVALID_OR_UNPROVEN'
    try:
        return 'REPORTED_DECIMAL_TEXT' if Decimal(value).is_finite() else 'INVALID_OR_UNPROVEN'
    except InvalidOperation:
        return 'INVALID_OR_UNPROVEN'


def summarize_settlement_history(capture, *, expected_start_date, expected_end_date, max_pages=2):
    """Return new public count dictionaries; never alter or publish private pages."""
    plan = validate_history_request(start_date=expected_start_date,
        end_date=expected_end_date, max_pages=max_pages)
    span = {key: plan[key] for key in ('startDate', 'endDate', 'calendarDays')}
    span['meaning'] = _SPAN_MEANING
    _require(type(capture) is dict, 'KIS_HISTORY_CAPTURE_UNVERIFIED')
    try:
        parsed = parse_reported_observation(capture)
    except (ValueError, TypeError, KeyError, OverflowError, ArithmeticError):
        raise KisSettlementHistoryError('KIS_HISTORY_CAPTURE_UNVERIFIED') from None
    _require(capture['kind'] == KIND and capture['path'] == PATH and capture['trId'] == TR_ID,
        'KIS_HISTORY_CAPTURE_SCOPE_MISMATCH')
    query = capture['queryWithoutAccount']
    _require(query['ERLM_STRT_DT'] == expected_start_date and query['ERLM_END_DT'] == expected_end_date,
        'KIS_HISTORY_CAPTURE_DATE_BINDING_MISMATCH')
    pages = capture['pages']
    _require(1 <= len(pages) <= max_pages, 'KIS_HISTORY_LOCAL_PAGE_BUDGET_EXCEEDED')
    field_states = {key: {state: 0 for state in FIELD_STATES} for key in FIELD_ROLES}
    total, usd_count, unreviewed = 0, 0, 0
    for page in pages:
        rows = page['body']['output1']
        total += len(rows)
        for row in rows:
            if row['crcy_cd'] != 'USD':
                continue
            usd_count += 1
            unreviewed += len(set(row) - _KNOWN_ROW_FIELDS)
            for key in FIELD_ROLES:
                field_states[key][_field_state(row, key)] += 1
    row_hashes = [item['reportedRowHash'] for item in parsed['reportedUsdRows']]
    body = {**_FIXED, 'querySpan': span, 'pageCount': len(pages), 'maximumPages': max_pages,
        'paginationStatus': capture['paginationStatus'], 'paginationReason': capture['paginationReason'],
        'captureContentHash': capture['contentHash'],
        'retrievalStartedAt': pages[0]['startedAt'], 'retrievalCompletedAt': pages[-1]['completedAt'],
        'rowStatus': _ROW_STATUSES[bool(usd_count)],
        'counts': {'returnedRowCount': total, 'usdRowCount': usd_count,
            'otherCurrencyRowCount': total - usd_count,
            'duplicateUsdRowCount': len(row_hashes) - len(set(row_hashes)),
            'unreviewedRowFieldCount': unreviewed},
        'numericFieldStates': field_states, 'reportedRoleNames': dict(FIELD_ROLES), **FLAGS}
    result = {**body, 'contentHash': canonical_hash(body)}
    return validate_settlement_history_summary(result)


def validate_settlement_history_summary(summary):
    """Validate the exact public shape; checksums never grant financial authority."""
    keys = set(_FIXED) | set(FLAGS) | {'querySpan', 'pageCount', 'maximumPages',
        'paginationStatus', 'paginationReason', 'captureContentHash', 'retrievalStartedAt',
        'retrievalCompletedAt', 'rowStatus', 'counts', 'numericFieldStates',
        'reportedRoleNames', 'contentHash'}
    _require(type(summary) is dict and set(summary) == keys)
    _require(all(summary[key] == value for key, value in _FIXED.items())
        and all(summary[key] is False for key in FLAGS))
    _require(type(summary['contentHash']) is str and _SHA.fullmatch(summary['contentHash']) is not None
        and type(summary['captureContentHash']) is str and _SHA.fullmatch(summary['captureContentHash']) is not None)
    body = {key: value for key, value in summary.items() if key != 'contentHash'}
    _require(canonical_hash(body) == summary['contentHash'])
    span = summary['querySpan']
    _require(type(span) is dict and set(span) == {'startDate', 'endDate', 'calendarDays', 'meaning'})
    _require(type(span['calendarDays']) is int and span == _span(span['startDate'], span['endDate']))
    _require(type(summary['maximumPages']) is int and 1 <= summary['maximumPages'] <= MAXIMUM_PAGES
        and type(summary['pageCount']) is int and 1 <= summary['pageCount'] <= summary['maximumPages'])
    _require(type(summary['paginationStatus']) is str and summary['paginationStatus'] in _PAGINATION
        and summary['paginationReason'] == _PAGINATION[summary['paginationStatus']])
    _require(_time(summary['retrievalCompletedAt']) >= _time(summary['retrievalStartedAt']))
    counts = summary['counts']
    _require(type(counts) is dict and set(counts) == set(_COUNTS)
        and all(_count(counts[key], 1000000 if key == 'unreviewedRowFieldCount' else 10000) for key in _COUNTS))
    _require(counts['usdRowCount'] + counts['otherCurrencyRowCount'] == counts['returnedRowCount']
        and counts['duplicateUsdRowCount'] <= counts['usdRowCount']
        and summary['rowStatus'] == _ROW_STATUSES[bool(counts['usdRowCount'])])
    states = summary['numericFieldStates']
    _require(type(states) is dict and set(states) == set(FIELD_ROLES)
        and type(summary['reportedRoleNames']) is dict and summary['reportedRoleNames'] == FIELD_ROLES)
    for row in states.values():
        _require(type(row) is dict and set(row) == set(FIELD_STATES)
            and all(_count(value) for value in row.values()) and sum(row.values()) == counts['usdRowCount'])
    # Return fresh nested structures; downstream mutation cannot rewrite the
    # caller's summary, its private source capture, or its expected date input.
    return {**body, 'querySpan': dict(span), 'counts': dict(counts),
        'numericFieldStates': {key: dict(value) for key, value in states.items()},
        'reportedRoleNames': dict(FIELD_ROLES), 'contentHash': summary['contentHash']}
