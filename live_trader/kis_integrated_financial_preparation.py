"""Strict public-only schema for integrated financial preparation diagnostics.

These validators do no IO and do not validate financial authority. They rebuild
known public fields so a custom reader cannot smuggle account values or remote
text through nested diagnostics. Capture hashes are syntax-checked references;
the private capture is intentionally unavailable at this public boundary.
"""
from __future__ import annotations

from datetime import datetime
import re

from .kis_integrated_margin_observation import (
    SCHEMA as OBSERVATION_SCHEMA, STATUS as OBSERVATION_STATUS,
    PATH, TR_ID, PUBLIC_QUERY, ROLE_FIELDS,
    FALSE_CLAIMS as OBSERVATION_FALSE_CLAIMS,
)

SCHEMA = 'kis-integrated-financial-preparation-v1'
FLAGS = {
    'productionReady': False, 'orderAuthorityGranted': False,
    'sharedCashReservationVerified': False, 'actualFeeObserved': False,
    'actualFxExecutionObserved': False, 'settlementFinalityVerified': False,
}
SHARED_FLAGS = {
    'productionReady': False, 'orderAuthorityGranted': False,
    'sharedCashReservationVerified': False, 'accountCashReconciled': False,
    'actualFeesReconciled': False, 'fxSettlementVerified': False,
    'financialQualificationGranted': False, 'reservationWritten': False,
    'ledgerInitialized': False,
}
SHARED_CHECKS = {
    'reportedIntegratedObservationValid',
    'localRetrievalWithinExisting15SecondBoundary', 'localOwnedInventoryProvided',
    'localOwnedBooksStructurallyConsistent', 'allOwnedBookBindingsMatch',
    'localOrderDependenciesProvided', 'completeAccountWorkingOrdersVerified',
}
# Exact product-owned diagnostics, rather than an arbitrary prefix which could
# carry user/account/remote text. An added producer code needs explicit review.
SHARED_REASONS = frozenset('''
KIS_SHARED_KRW_AWARE_CLOCK_REQUIRED
KIS_SHARED_KRW_CURRENT_ACCOUNT_OR_CONFIGURATION_BINDING_UNVERIFIED
KIS_SHARED_KRW_DEPENDENCY_KEYS_INVALID
KIS_SHARED_KRW_DEPENDENCY_NONFINITE
KIS_SHARED_KRW_DEPENDENCY_TOO_DEEP
KIS_SHARED_KRW_DEPENDENCY_TOO_LARGE
KIS_SHARED_KRW_DEPENDENCY_TYPE_INVALID
KIS_SHARED_KRW_DIAGNOSTIC_INVALID
KIS_SHARED_KRW_DUPLICATE_DEPLOYMENT_ALLOCATION
KIS_SHARED_KRW_DUPLICATE_MEMBER
KIS_SHARED_KRW_EMPTY_BOOK_INCONSISTENT
KIS_SHARED_KRW_ESTIMATED_FEE_POLICY_DEPENDENCY_UNAVAILABLE
KIS_SHARED_KRW_ESTIMATED_HOLDBACK_IDENTITY_INVALID
KIS_SHARED_KRW_EXISTING_BOOK_ENVELOPE_REQUIRED
KIS_SHARED_KRW_EXISTING_MEMBERS_INVALID
KIS_SHARED_KRW_EXISTING_PROJECTION_INVALID
KIS_SHARED_KRW_EXISTING_SCOPE_INVALID
KIS_SHARED_KRW_FINAL_FX_AND_SETTLEMENT_PRODUCER_UNAVAILABLE
KIS_SHARED_KRW_GLOBAL_RESERVATION_PRODUCER_UNAVAILABLE
KIS_SHARED_KRW_INDEPENDENT_CURRENCY_ALLOCATIONS_NOT_SHARED_POOL
KIS_SHARED_KRW_INTEGRATED_OBSERVATION_UNAVAILABLE
KIS_SHARED_KRW_INTEGRATED_OBSERVATION_UNVERIFIED
KIS_SHARED_KRW_INTEGRATED_RESPONSE_PARTIAL_OR_UNKNOWN
KIS_SHARED_KRW_LOCAL_BOOKS_DO_NOT_PROVE_EXTERNAL_ACCOUNT_FLOWS
KIS_SHARED_KRW_LOCAL_CLOCK_INVALID
KIS_SHARED_KRW_LOCAL_ORDER_DEPENDENCIES_INVALID
KIS_SHARED_KRW_LOCAL_ORDER_DEPENDENCIES_UNAVAILABLE
KIS_SHARED_KRW_LOCAL_ORDER_DEPENDENCIES_UNVERIFIED
KIS_SHARED_KRW_LOCAL_ORDER_ROWS_NOT_COMPLETE_BROKER_WORKING_SCOPE
KIS_SHARED_KRW_LOCAL_OWNED_INVENTORY_UNAVAILABLE
KIS_SHARED_KRW_LOCAL_OWNED_INVENTORY_UNVERIFIED
KIS_SHARED_KRW_LOCAL_PENDING_ORDERS_REQUIRE_BROKER_RECONCILIATION
KIS_SHARED_KRW_LOCAL_RETRIEVAL_STALE_OR_CLOCK_INVALID
KIS_SHARED_KRW_NATIVE_DECIMAL_TEXT_REQUIRED
KIS_SHARED_KRW_NONNEGATIVE_NATIVE_UNIT_REQUIRED
KIS_SHARED_KRW_OBSERVATION_ACCOUNT_BINDING_UNAVAILABLE
KIS_SHARED_KRW_ORDER_IDENTITY_UNKNOWN_OR_DUPLICATE
KIS_SHARED_KRW_PENDING_STATE_UNKNOWN
KIS_SHARED_KRW_REPORTED_MARGIN_IS_NOT_OWNED_CASH
'''.split())
LOCAL_REASONS = frozenset('''
KIS_LOCAL_OWNED_ACCOUNT_BINDING_INVALID
KIS_LOCAL_OWNED_BOOK_LIMIT_EXCEEDED
KIS_LOCAL_OWNED_CHANGED_DURING_READ
KIS_LOCAL_OWNED_DATABASE_HEADER_INVALID
KIS_LOCAL_OWNED_DATABASE_MISSING_NOT_ZERO_FUNDS
KIS_LOCAL_OWNED_DATABASE_UNVERIFIED
KIS_LOCAL_OWNED_DUPLICATE_JSON_KEY
KIS_LOCAL_OWNED_FIXED_PATH_REQUIRED
KIS_LOCAL_OWNED_INVENTORY_NOT_EXTERNAL_ACCOUNT_PROOF
KIS_LOCAL_OWNED_NONFINITE_JSON
KIS_LOCAL_OWNED_PATH_NOT_REGULAR
KIS_LOCAL_OWNED_PAYLOAD_LIMIT_EXCEEDED
KIS_LOCAL_OWNED_PRINCIPAL_AMOUNT_INVALID
KIS_LOCAL_OWNED_PRINCIPAL_RECORD_INVALID
KIS_LOCAL_OWNED_PRINCIPAL_SCOPE_INVALID
KIS_LOCAL_OWNED_PRINCIPAL_SEAL_INVALID
KIS_LOCAL_OWNED_PRINCIPAL_TIME_INVALID
KIS_LOCAL_OWNED_QUERY_ONLY_REQUIRED
KIS_LOCAL_OWNED_RECEIPT_LIMIT_EXCEEDED
KIS_LOCAL_OWNED_SCHEMA_INVALID
KIS_LOCAL_OWNED_SIDECAR_OR_ACTIVE_JOURNAL_UNSUPPORTED
KIS_LOCAL_OWNED_WAL_MODE_UNSUPPORTED
'''.split())
_SHA = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)
_UNKNOWN_ENROLLMENT = 'UNKNOWN_REQUIRES_EXPLICIT_BROKER_CONFIRMATION'
_PROFILE_MEANING = 'EXACT_SAVED_PROFILE_CONFIGURATION_CAPTURED_NOT_LEDGER_AUTHORITY'
_CURRENCY_MEANING = 'KRW_REFERENCE_REPORTING_NOT_CASH'
_CAPTURE_HASH_MEANING = 'CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_BYTES'
_TIME_MEANING = 'LOCAL_GET_TIMES_NOT_POSTING_SETTLEMENT_OR_FX_TIME'
_SHARED_HASH_MEANING = 'LOCAL_DIAGNOSTIC_DEPENDENCIES_NOT_FINANCIAL_AUTHENTICATION'
_LOCAL_SOURCE_MEANING = 'EXISTING_LOCAL_OWNED_BOOKS_NOT_ACCOUNT_WIDE_CASH'
_LOCAL_SUCCESS = 'KIS_LOCAL_OWNED_INVENTORY_NOT_EXTERNAL_ACCOUNT_PROOF'


def _bad():
    # A safe fixed code never incorporates the rejected input.
    raise ValueError('KIS_INTEGRATED_FINANCIAL_PUBLIC_DIAGNOSTIC_INVALID')


def _exact(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        _bad()


def _fixed(value, expected):
    if type(value) is not type(expected) or value != expected:
        _bad()
    return expected


def _integer(value, *, maximum, nullable=False):
    if value is None and nullable:
        return None
    if type(value) is not int or not 0 <= value <= maximum:
        _bad()
    return value


def _hash(value, *, nullable=False):
    if value is None and nullable:
        return None
    if type(value) is not str or not _SHA.fullmatch(value):
        _bad()
    return value


def _reasons(value, allowed, *, maximum):
    if (type(value) is not list or not 1 <= len(value) <= maximum
            or any(type(code) is not str or code not in allowed for code in value)
            or len(set(value)) != len(value)):
        _bad()
    return list(value)


def _utc(value):
    if type(value) is not str or len(value) > 64:
        _bad()
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            _bad()
        return parsed
    except (ValueError, TypeError, OverflowError):
        _bad()


def validate_integrated_observation_summary(value):
    keys = {
        'schemaVersion', 'status', 'provider', 'path', 'trId', 'responseTrId',
        'queryWithoutAccount', 'pageCount', 'paginationStatus', 'startedAt',
        'completedAt', 'reportedCurrencyBasis', 'numericFieldStates',
        'reportedNumericFieldCount', 'emptyNumericFieldCount', 'absentNumericFieldCount',
        'unreportedOtherFieldCount', 'reportedRoleNames', 'enrollmentLabelPresence',
        'enrollmentClassification', 'savedProfileBinding', 'hashMeaning',
        'contentHash', 'retrievalTimeMeaning',
    } | set(OBSERVATION_FALSE_CLAIMS)
    _exact(value, keys)
    fixed = {
        'schemaVersion': OBSERVATION_SCHEMA, 'status': OBSERVATION_STATUS,
        'provider': 'kis', 'path': PATH, 'trId': TR_ID, 'responseTrId': TR_ID,
        'pageCount': 1, 'reportedCurrencyBasis': _CURRENCY_MEANING,
        'enrollmentClassification': _UNKNOWN_ENROLLMENT,
        'savedProfileBinding': _PROFILE_MEANING,
        'hashMeaning': _CAPTURE_HASH_MEANING, 'retrievalTimeMeaning': _TIME_MEANING,
        **OBSERVATION_FALSE_CLAIMS,
    }
    result = {key: _fixed(value[key], expected) for key, expected in fixed.items()}
    _exact(value['queryWithoutAccount'], PUBLIC_QUERY)
    result['queryWithoutAccount'] = {key: _fixed(value['queryWithoutAccount'][key], expected)
                                   for key, expected in PUBLIC_QUERY.items()}
    _exact(value['reportedRoleNames'], ROLE_FIELDS)
    result['reportedRoleNames'] = {key: _fixed(value['reportedRoleNames'][key], role)
                                   for key, role in ROLE_FIELDS.items()}
    states = value['numericFieldStates']
    _exact(states, ROLE_FIELDS)
    allowed_states = {'ABSENT', 'EMPTY_UNREPORTED', 'REPORTED_DECIMAL_TEXT'}
    if any(type(state) is not str or state not in allowed_states for state in states.values()):
        _bad()
    result['numericFieldStates'] = dict(states)
    for key, state in (('reportedNumericFieldCount', 'REPORTED_DECIMAL_TEXT'),
                       ('emptyNumericFieldCount', 'EMPTY_UNREPORTED'),
                       ('absentNumericFieldCount', 'ABSENT')):
        result[key] = _integer(value[key], maximum=len(ROLE_FIELDS))
        if result[key] != sum(item == state for item in states.values()):
            _bad()
    result['unreportedOtherFieldCount'] = _integer(value['unreportedOtherFieldCount'], maximum=256)
    if type(value['enrollmentLabelPresence']) is not bool:
        _bad()
    result['enrollmentLabelPresence'] = value['enrollmentLabelPresence']
    if type(value['paginationStatus']) is not str or value['paginationStatus'] not in {'COMPLETE', 'PARTIAL', 'UNKNOWN'}:
        _bad()
    result['paginationStatus'] = value['paginationStatus']
    if _utc(value['completedAt']) < _utc(value['startedAt']):
        _bad()
    result.update(startedAt=value['startedAt'], completedAt=value['completedAt'],
                  contentHash=_hash(value['contentHash']))
    return result


def validate_shared_krw_readiness_summary(value):
    keys = {
        'schemaVersion', 'status', 'brokerId', 'reasons', 'checks',
        'dependencyHash', 'contentHash', 'localOwnedBookCount', 'localOrderRowCount',
        'localNonterminalLabelCount', 'observedBookCurrencies',
        'sharedKrwAvailableAmount', 'currencyConversionPerformed', 'hashMeaning',
    } | set(SHARED_FLAGS)
    _exact(value, keys)
    fixed = {'schemaVersion': 'kis-shared-krw-preparation-summary-v1',
             'status': 'BLOCKED', 'brokerId': 'kis', 'sharedKrwAvailableAmount': None,
             'currencyConversionPerformed': False, 'hashMeaning': _SHARED_HASH_MEANING,
             **SHARED_FLAGS}
    result = {key: _fixed(value[key], expected) for key, expected in fixed.items()}
    result['reasons'] = _reasons(value['reasons'], SHARED_REASONS, maximum=len(SHARED_REASONS))
    required = {'KIS_SHARED_KRW_GLOBAL_RESERVATION_PRODUCER_UNAVAILABLE',
                'KIS_SHARED_KRW_FINAL_FX_AND_SETTLEMENT_PRODUCER_UNAVAILABLE'}
    if not required.issubset(result['reasons']):
        _bad()
    _exact(value['checks'], SHARED_CHECKS)
    if any(type(check) is not bool for check in value['checks'].values()):
        _bad()
    if value['checks']['completeAccountWorkingOrdersVerified'] is not False:
        _bad()
    result['checks'] = dict(value['checks'])
    for key in ('dependencyHash', 'contentHash'):
        result[key] = _hash(value[key])
    for key, maximum in (('localOwnedBookCount', 10000), ('localOrderRowCount', 999999),
                         ('localNonterminalLabelCount', 999999)):
        result[key] = _integer(value[key], maximum=maximum, nullable=True)
    if result['localNonterminalLabelCount'] is not None and (
            result['localOrderRowCount'] is None
            or result['localNonterminalLabelCount'] > result['localOrderRowCount']):
        _bad()
    currencies = value['observedBookCurrencies']
    if (type(currencies) is not list or any(type(currency) is not str for currency in currencies)
            or currencies != sorted(set(currencies)) or not set(currencies).issubset({'KRW', 'USD'})):
        _bad()
    result['observedBookCurrencies'] = list(currencies)
    return result


def validate_local_owned_inventory_summary(value):
    _exact(value, {'status', 'reasons', 'localGeneration', 'dependencyHash', 'sourceMeaning'})
    _fixed(value['sourceMeaning'], _LOCAL_SOURCE_MEANING)
    reasons = _reasons(value['reasons'], LOCAL_REASONS, maximum=1)
    status = value['status']
    if type(status) is not str or status not in {'UNKNOWN', 'OBSERVED_LOCAL_INVENTORY_ONLY'}:
        _bad()
    if status == 'UNKNOWN':
        if reasons == [_LOCAL_SUCCESS] or value['localGeneration'] is not None or value['dependencyHash'] is not None:
            _bad()
        generation = digest = None
    else:
        if reasons != [_LOCAL_SUCCESS]:
            _bad()
        generation = _integer(value['localGeneration'], maximum=(2 ** 63 - 1))
        digest = _hash(value['dependencyHash'])
    return {'status': status, 'reasons': reasons, 'localGeneration': generation,
            'dependencyHash': digest, 'sourceMeaning': _LOCAL_SOURCE_MEANING}


def validate_integrated_financial_preparation_diagnostic(value):
    """Rebuild an exact safe public report; malformed readers fail closed."""
    _exact(value, {'schemaVersion', 'status', 'integratedObservation',
                   'sharedKrwReadiness', 'localOwnedInventory'} | set(FLAGS))
    _fixed(value['schemaVersion'], SCHEMA)
    for key in FLAGS:
        _fixed(value[key], False)
    status = value['status']
    if type(status) is not str or status not in {
            'UNAVAILABLE', 'NOT_REQUESTED_FOR_FOREIGN_CASH',
            'REPORTED_REFERENCE_AND_LOCAL_DEPENDENCIES_ONLY'}:
        _bad()
    if status == 'REPORTED_REFERENCE_AND_LOCAL_DEPENDENCIES_ONLY':
        observation = validate_integrated_observation_summary(value['integratedObservation'])
        shared = validate_shared_krw_readiness_summary(value['sharedKrwReadiness'])
        inventory = validate_local_owned_inventory_summary(value['localOwnedInventory'])
        if shared['checks']['reportedIntegratedObservationValid'] is not True:
            _bad()
    else:
        if any(value[key] is not None for key in ('integratedObservation', 'sharedKrwReadiness', 'localOwnedInventory')):
            _bad()
        observation = shared = inventory = None
    return {'schemaVersion': SCHEMA, 'status': status,
            'integratedObservation': observation, 'sharedKrwReadiness': shared,
            'localOwnedInventory': inventory, **FLAGS}
