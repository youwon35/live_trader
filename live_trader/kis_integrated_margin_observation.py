"""One reported integrated-margin snapshot, without a shared-cash producer.

Construction and parsing perform no IO. Collection uses only the injected normal
read-only reader. The capture contains private broker values and belongs solely
in protected local memory. Only safe_observation_summary is suitable for logs or
UI. A report is neither an owned-cash baseline nor service/settlement authority.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import re
import time

from .kis_overseas_funding import funding_policy, resolve_funding_mode
from .account_profiles import saved_account_profiles
from .kis_usd_observations import (
    FALSE_CLAIMS as USD_FALSE_CLAIMS, KisUsdObservationError,
    _scope, canonical_hash, decode_usd_response,
)

SCHEMA = 'kis-integrated-margin-reported-observation/v1'
PATH = '/uapi/domestic-stock/v1/trading/intgr-margin'
TR_ID = 'TTTC0869R'
PUBLIC_QUERY = {
    'CMA_EVLU_AMT_ICLD_YN': 'N',
    'WCRC_FRCR_DVSN_CD': '02',
    'FWEX_CTRT_FRCR_DVSN_CD': '02',
}
STATUS = 'OBSERVED_ONLY_NOT_FINANCIALLY_QUALIFIED'
ROLE_FIELDS = {
    'stck_cash100_max_ord_psbl_amt': 'REPORTED_DOMESTIC_CASH100_ORDER_CAPACITY_NOT_OWNED_CASH',
    'stck_itgr_cash100_ord_psbl_amt': 'REPORTED_DOMESTIC_INTEGRATED_CASH100_ORDER_CAPACITY_NOT_OWNED_CASH',
    'stck_cash_ovrs_use_amt': 'REPORTED_DOMESTIC_CASH_USED_OVERSEAS_NOT_LOCAL_RESERVATION_AUTHORITY',
    'stck_cash_objt_amt': 'REPORTED_DOMESTIC_CASH_OBJECT_AMOUNT_NOT_OWNED_CASH_BASELINE',
    'stck_cash_use_amt': 'REPORTED_DOMESTIC_CASH_USED_AMOUNT_NOT_LOCAL_RESERVATION_AUTHORITY',
    'stck_cash_ord_psbl_amt': 'REPORTED_DOMESTIC_CASH_ORDER_CAPACITY_NOT_OWNED_CASH',
}
for _currency in ('usd', 'hkd', 'jpy', 'cny'):
    for _suffix, _role in (
        ('objt_amt', 'OBJECT_AMOUNT_NOT_OWNED_CASH_BASELINE'),
        ('use_amt', 'USED_AMOUNT_NOT_LOCAL_RESERVATION_AUTHORITY'),
        ('ord_psbl_amt', 'ORDER_CAPACITY_NOT_OWNED_CASH'),
        ('gnrl_ord_psbl_amt', 'GENERAL_ORDER_CAPACITY_NOT_OWNED_CASH'),
        ('itgr_ord_psbl_amt', 'INTEGRATED_ORDER_CAPACITY_NOT_OWNED_CASH'),
        ('oth_mket_use_amt', 'OTHER_MARKET_USED_AMOUNT_NOT_LOCAL_RESERVATION_AUTHORITY'),
        ('frst_bltn_exrt', 'FIRST_PUBLISHED_REFERENCE_FX_NOT_ACTUAL_CONVERSION_OR_FRESH_EVENT'),
    ):
        ROLE_FIELDS[_currency + '_' + _suffix] = 'REPORTED_' + _currency.upper() + '_' + _role
del _currency, _suffix, _role

FALSE_CLAIMS = {
    **USD_FALSE_CLAIMS,
    'brokerEnrollmentVerified': False,
    'sharedCashReservationVerified': False,
    'sharedCashBaselineEstablished': False,
    'actualFxExecutionObserved': False,
    'cashAndCostArithmeticPerformed': False,
}
_NUMBER = re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\Z', re.ASCII)
_SHA = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)
_HASH_MEANING = 'CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_BYTES'
_TIME_MEANING = 'LOCAL_GET_TIMES_NOT_POSTING_SETTLEMENT_OR_FX_TIME'
_CURRENCY_MEANING = 'KRW_REFERENCE_REPORTING_NOT_CASH'


def bad(code='KIS_INTEGRATED_MARGIN_OBSERVATION_INVALID'):
    """Fixed safe code only; never propagate remote text, account, or token."""
    raise KisUsdObservationError(code)


def decode_integrated_margin_response(raw):
    """Reject duplicate JSON keys and nonfinite values before broker parsing."""
    decoded = decode_usd_response(raw)
    canonical_hash(decoded)  # Also rejects exponent overflow to float infinity.
    return decoded


def _binding(settings):
    setting_keys = ('KIS_APP_KEY', 'KIS_APP_SECRET', 'KIS_ACCOUNT_NO',
                   'KIS_ACCOUNT_PRODUCT_CODE', 'KIS_BASE_URL', 'KIS_ENV',
                   'KIS_OVERSEAS_FUNDING_MODE')
    if type(settings) is not dict or any(type(settings.get(key, '')) is not str for key in setting_keys):
        bad('KIS_INTEGRATED_MARGIN_ACCOUNT_BINDING_INVALID')
    base_hash, account = _scope(settings)
    try:
        mode = resolve_funding_mode(settings)
        policy_hash = funding_policy(settings)['policyHash']
    except (KeyError, ValueError, TypeError):
        bad('KIS_INTEGRATED_MARGIN_FUNDING_SCOPE_INVALID')
    if type(policy_hash) is not str or not _SHA.fullmatch(policy_hash):
        bad('KIS_INTEGRATED_MARGIN_FUNDING_SCOPE_INVALID')
    profiles = [profile for profile in saved_account_profiles(settings) if profile.get('brokerId') == 'kis']
    if len(profiles) != 1:
        bad('KIS_INTEGRATED_MARGIN_PROFILE_BINDING_UNAVAILABLE')
    profile_binding = {key: profiles[0].get(key) for key in
                       ('profileId', 'accountFingerprint', 'credentialConfigurationHash')}
    _profile_binding(profile_binding)
    # This is a reader setting-snapshot hash, not the different profile hash from
    # saved_account_profiles and not an attested mapping to a native owned book.
    return canonical_hash({'readerCredentialScopeHash': base_hash,
                           'overseasFundingMode': mode,
                           'overseasFundingPolicyHash': policy_hash}), account, profile_binding


def _profile_binding(value):
    keys = {'profileId', 'accountFingerprint', 'credentialConfigurationHash'}
    if type(value) is not dict or set(value) != keys or any(
            type(value[key]) is not str or not _SHA.fullmatch(value[key]) for key in keys):
        bad('KIS_INTEGRATED_MARGIN_PROFILE_BINDING_INVALID')
    return dict(value)


def _utc(value):
    try:
        if type(value) is not str:
            bad()
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            bad()
        return parsed
    except (ValueError, TypeError, OverflowError):
        bad()


def _mono(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        bad('KIS_INTEGRATED_MARGIN_RECEIVE_CLOCK_INVALID')
    return value


def _header(value):
    if value is not None and (type(value) is not str or len(value) > 256):
        bad('KIS_INTEGRATED_MARGIN_RESPONSE_HEADERS_INVALID')
    return value


def _number(value):
    if type(value) is not str or len(value) > 128 or not _NUMBER.fullmatch(value):
        bad('KIS_INTEGRATED_MARGIN_REPORTED_NUMBER_INVALID')
    try:
        if not Decimal(value).is_finite():
            bad('KIS_INTEGRATED_MARGIN_REPORTED_NUMBER_INVALID')
    except InvalidOperation:
        bad('KIS_INTEGRATED_MARGIN_REPORTED_NUMBER_INVALID')
    return value


def _reported_fields(output):
    values, states = {}, {}
    for field in ROLE_FIELDS:
        if field not in output:
            states[field] = 'ABSENT'
        elif output[field] == '' and type(output[field]) is str:
            states[field] = 'EMPTY_UNREPORTED'
        else:
            values[field] = _number(output[field])
            states[field] = 'REPORTED_DECIMAL_TEXT'
    label = output.get('ovrs_stck_itgr_mgna_dvsn_name')
    if label is not None and (type(label) is not str or len(label) > 256):
        bad('KIS_INTEGRATED_MARGIN_ENROLLMENT_LABEL_INVALID')
    return values, states, label is not None and label != ''


def validate_observation(capture):
    """Validate a private capture and expose separate literal reported roles.

    Top-level credentialConfigurationHash is a reader snapshot, distinct from
    accountBinding.credentialConfigurationHash, which uses saved_account_profiles
    exactly. Missing/blank reported fields stay unknown, never zero. A bound read
    does not establish any owned book, financial producer or reservation service.
    """
    keys = {
        'schemaVersion', 'status', 'provider', 'path', 'trId',
        'credentialConfigurationHash', 'credentialConfigurationHashMeaning',
        'accountBinding', 'accountQueryHash', 'queryWithoutAccount', 'body', 'canonicalParsedBodyHash',
        'responseTrId', 'responseContinuation', 'httpDateRaw', 'httpAgeRaw',
        'startedAt', 'completedAt', 'startedMonotonic', 'completedMonotonic',
        'maxPages', 'attemptedPages', 'authenticatedProducerAttested',
        'privateValuesLocalOnly', 'hashMeaning', 'retrievalTimeMeaning',
        'reportedCurrencyBasis', 'contentHash',
    } | set(FALSE_CLAIMS)
    if type(capture) is not dict or set(capture) != keys:
        bad('KIS_INTEGRATED_MARGIN_CAPTURE_SHAPE_INVALID')
    unsigned = {key: value for key, value in capture.items() if key != 'contentHash'}
    if (type(capture['contentHash']) is not str or not _SHA.fullmatch(capture['contentHash'])
            or canonical_hash(unsigned) != capture['contentHash']):
        bad('KIS_INTEGRATED_MARGIN_CAPTURE_HASH_MISMATCH')
    if (capture['schemaVersion'] != SCHEMA or capture['status'] != STATUS
            or capture['provider'] != 'kis' or capture['path'] != PATH
            or capture['trId'] != TR_ID or capture['responseTrId'] != TR_ID
            or capture['queryWithoutAccount'] != PUBLIC_QUERY
            or type(capture['maxPages']) is not int or capture['maxPages'] != 1
            or type(capture['attemptedPages']) is not int or capture['attemptedPages'] != 1):
        bad('KIS_INTEGRATED_MARGIN_CAPTURE_BINDING_INVALID')
    if (capture['authenticatedProducerAttested'] is not False
            or capture['privateValuesLocalOnly'] is not True
            or any(capture[key] is not False for key in FALSE_CLAIMS)):
        bad('KIS_INTEGRATED_MARGIN_AUTHORITY_FORBIDDEN')
    if (capture['hashMeaning'] != _HASH_MEANING
            or capture['retrievalTimeMeaning'] != _TIME_MEANING
            or capture['reportedCurrencyBasis'] != _CURRENCY_MEANING
            or capture['credentialConfigurationHashMeaning'] != 'READER_SETTINGS_AND_FUNDING_POLICY_NOT_SAVED_PROFILE_HASH'):
        bad()
    if any(type(capture[key]) is not str or not _SHA.fullmatch(capture[key])
           for key in ('credentialConfigurationHash', 'accountQueryHash', 'canonicalParsedBodyHash')):
        bad('KIS_INTEGRATED_MARGIN_CAPTURE_BINDING_INVALID')
    profile_binding = _profile_binding(capture['accountBinding'])
    if (_utc(capture['completedAt']) < _utc(capture['startedAt'])
            or _mono(capture['completedMonotonic']) < _mono(capture['startedMonotonic'])):
        bad('KIS_INTEGRATED_MARGIN_RECEIVE_CLOCK_INVALID')
    _header(capture['httpDateRaw'])
    _header(capture['httpAgeRaw'])
    continuation = _header(capture['responseContinuation'])
    if continuation not in (None, '', 'D', 'E', 'M', 'F'):
        bad('KIS_INTEGRATED_MARGIN_CONTINUATION_INVALID')
    body = capture['body']
    if (type(body) is not dict or body.get('rt_cd') != '0'
            or canonical_hash(body) != capture['canonicalParsedBodyHash']):
        bad('KIS_INTEGRATED_MARGIN_BROKER_RESPONSE_REJECTED')
    output = body.get('output')
    if type(output) is not dict or len(output) > 256:
        bad('KIS_INTEGRATED_MARGIN_RESPONSE_SHAPE_INVALID')
    values, states, label_present = _reported_fields(output)
    return {'reportedValues': values, 'numericFieldStates': states, 'accountBinding': profile_binding,
            'enrollmentLabelPresence': label_present,
            'enrollmentClassification': 'UNKNOWN_REQUIRES_EXPLICIT_BROKER_CONFIRMATION',
            'reportedCurrencyBasis': _CURRENCY_MEANING,
            'paginationStatus': ('COMPLETE' if continuation in ('D', 'E') else
                                 'PARTIAL' if continuation in ('M', 'F') else 'UNKNOWN')}


parse_reported_observation = validate_observation


def safe_observation_summary(capture):
    """Return fixed field names, validity and counts; no private amount or label.

    Account/credential hashes, HTTP metadata and broker text are omitted. The
    content hash binds the private capture but does not attest its producer.
    """
    checked = validate_observation(capture)
    states = checked['numericFieldStates']
    output = capture['body']['output']
    return {
        'schemaVersion': SCHEMA, 'status': STATUS, 'provider': 'kis',
        'path': PATH, 'trId': TR_ID, 'responseTrId': TR_ID,
        'queryWithoutAccount': dict(PUBLIC_QUERY),
        'pageCount': 1, 'paginationStatus': checked['paginationStatus'],
        'startedAt': capture['startedAt'], 'completedAt': capture['completedAt'],
        'reportedCurrencyBasis': _CURRENCY_MEANING,
        'numericFieldStates': dict(states),
        'reportedNumericFieldCount': sum(state == 'REPORTED_DECIMAL_TEXT' for state in states.values()),
        'emptyNumericFieldCount': sum(state == 'EMPTY_UNREPORTED' for state in states.values()),
        'absentNumericFieldCount': sum(state == 'ABSENT' for state in states.values()),
        'unreportedOtherFieldCount': len(set(output) - set(ROLE_FIELDS) - {'ovrs_stck_itgr_mgna_dvsn_name'}),
        'reportedRoleNames': dict(ROLE_FIELDS),
        'enrollmentLabelPresence': checked['enrollmentLabelPresence'],
        'enrollmentClassification': checked['enrollmentClassification'],
        'savedProfileBinding': 'EXACT_SAVED_PROFILE_CONFIGURATION_CAPTURED_NOT_LEDGER_AUTHORITY',
        'hashMeaning': _HASH_MEANING, 'contentHash': capture['contentHash'],
        'retrievalTimeMeaning': _TIME_MEANING, **FALSE_CLAIMS,
    }


class KisIntegratedMarginObservationReader:
    """One GET, using the existing memory token and normal shared KIS pacing."""
    def __init__(self, reader, *, utc_now=None, monotonic=None):
        self.reader = reader
        self.utc_now = utc_now or (lambda: datetime.now(timezone.utc).isoformat())
        self.monotonic = monotonic or time.monotonic

    def observe(self, *, max_pages=1):
        if type(max_pages) is not int or max_pages != 1:
            bad('KIS_INTEGRATED_MARGIN_ONE_PAGE_ONLY')
        binding = _binding(self.reader.settings)
        scope_hash, account, profile_binding = binding
        self.reader._credentials('kis')
        token = self.reader._kis_token()
        if type(token) is not str or not token or len(token) > 16384:
            bad('KIS_INTEGRATED_MARGIN_AUTHENTICATION_INVALID')
        if _binding(self.reader.settings) != binding:
            bad('KIS_INTEGRATED_MARGIN_ACCOUNT_SCOPE_CHANGED')
        self.reader._pace_kis()
        if _binding(self.reader.settings) != binding:
            bad('KIS_INTEGRATED_MARGIN_ACCOUNT_SCOPE_CHANGED')
        start_utc, start_mono = self.utc_now(), self.monotonic()
        body, headers = self.reader._get('kis', PATH,
            query={**account, **PUBLIC_QUERY}, headers={
                'authorization': 'Bearer ' + token,
                'appkey': self.reader.settings['KIS_APP_KEY'],
                'appsecret': self.reader.settings['KIS_APP_SECRET'],
                'tr_id': TR_ID, 'tr_cont': '', 'custtype': 'P',
            })
        end_utc, end_mono = self.utc_now(), self.monotonic()
        if _binding(self.reader.settings) != binding:
            bad('KIS_INTEGRATED_MARGIN_ACCOUNT_SCOPE_CHANGED')
        if type(headers) is not dict:
            bad('KIS_INTEGRATED_MARGIN_RESPONSE_HEADERS_INVALID')
        result = {
            'schemaVersion': SCHEMA, 'status': STATUS, 'provider': 'kis',
            'path': PATH, 'trId': TR_ID, 'credentialConfigurationHash': scope_hash,
            'credentialConfigurationHashMeaning': 'READER_SETTINGS_AND_FUNDING_POLICY_NOT_SAVED_PROFILE_HASH',
            'accountBinding': profile_binding,
            'accountQueryHash': canonical_hash(account),
            'queryWithoutAccount': dict(PUBLIC_QUERY), 'body': body,
            'canonicalParsedBodyHash': canonical_hash(body),
            'responseTrId': headers.get('tr_id'), 'responseContinuation': headers.get('tr_cont'),
            'httpDateRaw': headers.get('date'), 'httpAgeRaw': headers.get('age'),
            'startedAt': start_utc, 'completedAt': end_utc,
            'startedMonotonic': start_mono, 'completedMonotonic': end_mono,
            'maxPages': 1, 'attemptedPages': 1,
            'authenticatedProducerAttested': False, 'privateValuesLocalOnly': True,
            'hashMeaning': _HASH_MEANING, 'retrievalTimeMeaning': _TIME_MEANING,
            'reportedCurrencyBasis': _CURRENCY_MEANING, **FALSE_CLAIMS,
        }
        result['contentHash'] = canonical_hash(result)
        validate_observation(result)
        return result
