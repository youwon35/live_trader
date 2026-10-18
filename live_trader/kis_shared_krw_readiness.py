"""Pure shared-KRW preparation diagnosis; no reservation or financial authority.

Existing currency books are allocated capital with estimated cost holdbacks.
An integrated-margin reference is broker-reported buying power. This module
keeps these roles separate; it never converts, sums or subtracts one from the
other. A local dependency hash detects changed inputs, not authenticated funds.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re

from .kis_integrated_margin_observation import validate_observation

SCHEMA = 'kis-shared-krw-preparation-diagnostic-v1'
SUMMARY_SCHEMA = 'kis-shared-krw-preparation-summary-v1'
_HASH = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)
_NUMBER = re.compile(r'[0-9]{1,24}(?:\.[0-9]{1,16})?\Z', re.ASCII)
_BOOK_KEYS = {'scope', 'projection'}
_SCOPE_KEYS = {'version', 'deploymentId', 'accountFingerprint',
    'credentialConfigurationHash', 'accountProfileId', 'reviewBindingHash',
    'capitalPercent', 'quoteCurrency', 'members', 'scopeHash'}
_PROJECTION_KEYS = {'equityScope', 'capitalAccounting', 'deploymentEquity',
    'ownedCash', 'grossCash', 'costReserveOutstanding', 'ownedPositions',
    'capitalScopeHash', 'fillCount', 'actualFeesReconciled',
    'netPerformanceStatus', 'orderAuthorityGranted'}
_MEMBER_KEYS = {'strategyInstanceId', 'sourceArtifactHash', 'portfolioHash',
    'symbol', 'quoteCurrency'}
_FALSE = {'productionReady': False, 'orderAuthorityGranted': False,
    'sharedCashReservationVerified': False, 'accountCashReconciled': False,
    'actualFeesReconciled': False, 'fxSettlementVerified': False,
    'financialQualificationGranted': False, 'reservationWritten': False,
    'ledgerInitialized': False}
_CHECKS = {'reportedIntegratedObservationValid',
    'localRetrievalWithinExisting15SecondBoundary', 'localOwnedInventoryProvided',
    'localOwnedBooksStructurallyConsistent', 'allOwnedBookBindingsMatch',
    'localOrderDependenciesProvided', 'completeAccountWorkingOrdersVerified'}


def _failure_reason(error, fallback):
    # Only this module's fixed codes may enter a UI; remote values never do.
    code = str(error)
    return code if re.fullmatch(r'KIS_SHARED_KRW_[A-Z0-9_]{1,100}', code, re.ASCII) else fallback


def _hash(value):
    def plain(item, depth=0):
        if depth > 32:
            raise ValueError('KIS_SHARED_KRW_DEPENDENCY_TOO_DEEP')
        if type(item) is dict:
            if any(type(k) is not str for k in item):
                raise ValueError('KIS_SHARED_KRW_DEPENDENCY_KEYS_INVALID')
            for child in item.values():
                plain(child, depth + 1)
        elif type(item) is list:
            for child in item:
                plain(child, depth + 1)
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError('KIS_SHARED_KRW_DEPENDENCY_NONFINITE')
        elif type(item) not in (str, int, bool, type(None)):
            raise ValueError('KIS_SHARED_KRW_DEPENDENCY_TYPE_INVALID')
    plain(value)
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':'), allow_nan=False).encode('utf-8')
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError('KIS_SHARED_KRW_DEPENDENCY_TOO_LARGE')
    return hashlib.sha256(raw).hexdigest()


def _amount(value, *, whole=False):
    if type(value) is not str or not _NUMBER.fullmatch(value):
        raise ValueError('KIS_SHARED_KRW_NATIVE_DECIMAL_TEXT_REQUIRED')
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError('KIS_SHARED_KRW_NATIVE_DECIMAL_TEXT_REQUIRED') from None
    if not number.is_finite() or number < 0 or whole and number != number.to_integral_value():
        raise ValueError('KIS_SHARED_KRW_NONNEGATIVE_NATIVE_UNIT_REQUIRED')
    return number


def _time(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('KIS_SHARED_KRW_AWARE_CLOCK_REQUIRED')
    return value.astimezone(timezone.utc)


def _text(number):
    value = format(number, 'f')
    return value.rstrip('0').rstrip('.') if '.' in value else value


def _book(value):
    if type(value) is not dict or set(value) not in (_BOOK_KEYS, _BOOK_KEYS | {'feePolicy'}):
        raise ValueError('KIS_SHARED_KRW_EXISTING_BOOK_ENVELOPE_REQUIRED')
    scope, book = value['scope'], value['projection']
    if (type(scope) is not dict or set(scope) != _SCOPE_KEYS
        or scope['version'] != 'kis-owned-capital-v2'
        or scope['quoteCurrency'] not in {'KRW', 'USD'}
        or any(type(scope[k]) is not str or not _HASH.fullmatch(scope[k]) for k in
               ('accountFingerprint', 'credentialConfigurationHash', 'reviewBindingHash', 'scopeHash'))
        or any(type(scope[k]) is not str or not scope[k] for k in ('deploymentId', 'accountProfileId'))
        or _hash({k: v for k, v in scope.items() if k != 'scopeHash'}) != scope['scopeHash']
        or not 0 < _amount(scope['capitalPercent']) <= 100):
        raise ValueError('KIS_SHARED_KRW_EXISTING_SCOPE_INVALID')
    members = scope['members']
    if type(members) is not list or not 1 <= len(members) <= 32:
        raise ValueError('KIS_SHARED_KRW_EXISTING_MEMBERS_INVALID')
    ids = []
    for member in members:
        if (type(member) is not dict or set(member) != _MEMBER_KEYS
            or member['quoteCurrency'] != scope['quoteCurrency']
            or type(member['strategyInstanceId']) is not str or not member['strategyInstanceId']
            or type(member['symbol']) is not str or not member['symbol']
            or any(type(member[k]) is not str or not _HASH.fullmatch(member[k])
                   for k in ('sourceArtifactHash', 'portfolioHash'))):
            raise ValueError('KIS_SHARED_KRW_EXISTING_MEMBERS_INVALID')
        ids.append(member['strategyInstanceId'])
    if len(set(ids)) != len(ids):
        raise ValueError('KIS_SHARED_KRW_DUPLICATE_MEMBER')
    if (type(book) is not dict or set(book) != _PROJECTION_KEYS
        or book['capitalScopeHash'] != scope['scopeHash']
        or book['equityScope'] != 'OWNED_DEPLOYMENT_ONLY'
        or book['capitalAccounting'] != 'OWNED_CONSERVATIVE_COST_RESERVE'
        or book['actualFeesReconciled'] is not False
        or book['orderAuthorityGranted'] is not False
        or book['netPerformanceStatus'] != 'ESTIMATED_UNTIL_ACTUAL_COST_RECONCILIATION'
        or type(book['fillCount']) is not int or book['fillCount'] < 0
        or type(book['ownedPositions']) is not dict or set(book['ownedPositions']) != set(ids)):
        raise ValueError('KIS_SHARED_KRW_EXISTING_PROJECTION_INVALID')
    cash, gross, reserve = (_amount(book[k]) for k in ('ownedCash', 'grossCash', 'costReserveOutstanding'))
    if book['deploymentEquity'] is not None:
        _amount(book['deploymentEquity'])
    positions = [_amount(qty, whole=True) for qty in book['ownedPositions'].values()]
    with localcontext() as precision:
        precision.prec = 128
        if gross - reserve != cash:
            raise ValueError('KIS_SHARED_KRW_ESTIMATED_HOLDBACK_IDENTITY_INVALID')
    if book['fillCount'] == 0 and (reserve or any(positions)):
        raise ValueError('KIS_SHARED_KRW_EMPTY_BOOK_INCONSISTENT')
    fee_hash = _hash(value['feePolicy']) if 'feePolicy' in value else None
    return {'scope': scope, 'cash': cash, 'gross': gross, 'reserve': reserve,
        'dependencyHash': _hash(value), 'feePolicyHash': fee_hash}


def inspect_shared_krw_readiness(*, integrated_observation, owned_books=None,
        open_orders=None, now=None):
    """Inspect dependencies only. Even complete local inputs cannot grant PASS.

    ``owned_books`` contains existing KisOwnedCapitalStore scopes/projections,
    optionally their current fee policy as a dependency. ``open_orders`` is an
    existing local dispatch-row list, whose journal labels cannot establish
    complete broker-wide working orders. None means unavailable, never empty.
    Returned native-currency totals are private diagnostics, not shared money.
    """
    reasons = ['KIS_SHARED_KRW_GLOBAL_RESERVATION_PRODUCER_UNAVAILABLE',
        'KIS_SHARED_KRW_FINAL_FX_AND_SETTLEMENT_PRODUCER_UNAVAILABLE']
    checks = {'reportedIntegratedObservationValid': False,
        'localRetrievalWithinExisting15SecondBoundary': False,
        'localOwnedInventoryProvided': False, 'localOwnedBooksStructurallyConsistent': False,
        'allOwnedBookBindingsMatch': False, 'localOrderDependenciesProvided': False,
        'completeAccountWorkingOrdersVerified': False}
    dependencies = {'integratedObservationHash': None, 'ownedBooksHash': None,
        'openOrdersHash': None, 'feePolicyHashes': []}
    native_totals = {}
    observation = None
    binding = None
    parsed_books = []
    book_count = None
    order_count = None
    pending_count = None
    try:
        current = _time(now if now is not None else datetime.now(timezone.utc))
    except (ValueError, TypeError, OverflowError):
        current = None
        reasons.append('KIS_SHARED_KRW_LOCAL_CLOCK_INVALID')
    try:
        if integrated_observation is None:
            raise ValueError('KIS_SHARED_KRW_INTEGRATED_OBSERVATION_UNAVAILABLE')
        observation = validate_observation(integrated_observation)
        dependencies['integratedObservationHash'] = integrated_observation['contentHash']
        checks['reportedIntegratedObservationValid'] = True
        binding = observation.get('accountBinding')
        if (type(binding) is not dict
            or set(binding) != {'profileId', 'accountFingerprint', 'credentialConfigurationHash'}
            or any(type(binding[k]) is not str or not _HASH.fullmatch(binding[k]) for k in binding)):
            binding = None
            reasons.append('KIS_SHARED_KRW_OBSERVATION_ACCOUNT_BINDING_UNAVAILABLE')
        start, end = (_time(integrated_observation[k]) for k in ('startedAt', 'completedAt'))
        if current is None or not (0 <= (current - start).total_seconds() <= 15
            and 0 <= (current - end).total_seconds() <= 15
            and 0 <= (end - start).total_seconds() <= 15):
            reasons.append('KIS_SHARED_KRW_LOCAL_RETRIEVAL_STALE_OR_CLOCK_INVALID')
        else:
            checks['localRetrievalWithinExisting15SecondBoundary'] = True
        if observation.get('paginationStatus') != 'COMPLETE':
            reasons.append('KIS_SHARED_KRW_INTEGRATED_RESPONSE_PARTIAL_OR_UNKNOWN')
        reasons.append('KIS_SHARED_KRW_REPORTED_MARGIN_IS_NOT_OWNED_CASH')
    except (ValueError, TypeError, KeyError, ArithmeticError, OverflowError) as error:
        reasons.append(_failure_reason(error, 'KIS_SHARED_KRW_INTEGRATED_OBSERVATION_UNVERIFIED'))
    try:
        if type(owned_books) is not list or len(owned_books) > 10000:
            raise ValueError('KIS_SHARED_KRW_LOCAL_OWNED_INVENTORY_UNAVAILABLE')
        dependencies['ownedBooksHash'] = _hash(owned_books)
        checks['localOwnedInventoryProvided'] = True
        book_count = len(owned_books)
        parsed_books = [_book(book) for book in owned_books]
        ids = [book['scope']['deploymentId'] for book in parsed_books]
        if len(ids) != len(set(ids)):
            raise ValueError('KIS_SHARED_KRW_DUPLICATE_DEPLOYMENT_ALLOCATION')
        checks['localOwnedBooksStructurallyConsistent'] = True
        checks['allOwnedBookBindingsMatch'] = binding is not None and all(
            all(book['scope'][source] == binding[target] for source, target in (
                ('accountProfileId', 'profileId'), ('accountFingerprint', 'accountFingerprint'),
                ('credentialConfigurationHash', 'credentialConfigurationHash')))
            for book in parsed_books)
        if not checks['allOwnedBookBindingsMatch']:
            reasons.append('KIS_SHARED_KRW_CURRENT_ACCOUNT_OR_CONFIGURATION_BINDING_UNVERIFIED')
        if checks['allOwnedBookBindingsMatch']:
            with localcontext() as precision:
                precision.prec = 128
                for book in parsed_books:
                    currency = book['scope']['quoteCurrency']
                    total = native_totals.setdefault(currency,
                        {'deploymentCount': 0, 'ownedCash': Decimal(0),
                         'grossCash': Decimal(0), 'estimatedCostHoldback': Decimal(0)})
                    total['deploymentCount'] += 1
                    for output, source in (('ownedCash', 'cash'), ('grossCash', 'gross'),
                                           ('estimatedCostHoldback', 'reserve')):
                        total[output] += book[source]
        dependencies['feePolicyHashes'] = sorted({book['feePolicyHash'] for book in parsed_books
            if book['feePolicyHash'] is not None})
        if any(book['feePolicyHash'] is None for book in parsed_books):
            reasons.append('KIS_SHARED_KRW_ESTIMATED_FEE_POLICY_DEPENDENCY_UNAVAILABLE')
        if len(native_totals) > 1:
            reasons.append('KIS_SHARED_KRW_INDEPENDENT_CURRENCY_ALLOCATIONS_NOT_SHARED_POOL')
        reasons.append('KIS_SHARED_KRW_LOCAL_BOOKS_DO_NOT_PROVE_EXTERNAL_ACCOUNT_FLOWS')
    except (ValueError, TypeError, KeyError, ArithmeticError, OverflowError) as error:
        native_totals = {}
        reasons.append(_failure_reason(error, 'KIS_SHARED_KRW_LOCAL_OWNED_INVENTORY_UNVERIFIED'))
    try:
        if type(open_orders) is not list or len(open_orders) >= 1000000:
            raise ValueError('KIS_SHARED_KRW_LOCAL_ORDER_DEPENDENCIES_UNAVAILABLE')
        dependencies['openOrdersHash'] = _hash(open_orders)
        if any(type(row) is not dict for row in open_orders):
            raise ValueError('KIS_SHARED_KRW_LOCAL_ORDER_DEPENDENCIES_INVALID')
        checks['localOrderDependenciesProvided'] = True
        order_count = len(open_orders)
        keys = [row.get('idempotency_key') for row in open_orders]
        if (any(type(key) is not str or not key for key in keys)
            or len(keys) != len(set(keys))):
            reasons.append('KIS_SHARED_KRW_ORDER_IDENTITY_UNKNOWN_OR_DUPLICATE')
        terminal_labels = {'risk_blocked', 'adapter_blocked', 'rejected', 'filled', 'cancelled', 'expired'}
        if any(type(row.get('state')) is not str or not row['state'] for row in open_orders):
            reasons.append('KIS_SHARED_KRW_PENDING_STATE_UNKNOWN')
        else:
            pending_count = sum(row['state'] not in terminal_labels for row in open_orders)
            if pending_count:
                reasons.append('KIS_SHARED_KRW_LOCAL_PENDING_ORDERS_REQUIRE_BROKER_RECONCILIATION')
        reasons.append('KIS_SHARED_KRW_LOCAL_ORDER_ROWS_NOT_COMPLETE_BROKER_WORKING_SCOPE')
    except (ValueError, TypeError, KeyError, ArithmeticError, OverflowError) as error:
        reasons.append(_failure_reason(error, 'KIS_SHARED_KRW_LOCAL_ORDER_DEPENDENCIES_UNVERIFIED'))
    totals = {currency: {key: value if key == 'deploymentCount' else _text(value)
        for key, value in row.items()} for currency, row in native_totals.items()}
    result = {'schemaVersion': SCHEMA, 'status': 'BLOCKED', 'brokerId': 'kis',
        'reasons': list(dict.fromkeys(reasons)), 'checks': checks,
        'dependencyHashes': dependencies, 'dependencyHash': _hash(dependencies),
        'localOwnedBookCount': book_count, 'localOrderRowCount': order_count,
        'localNonterminalLabelCount': pending_count, 'currencyBooks': totals,
        'sharedKrwAvailableAmount': None, 'currencyConversionPerformed': False,
        'brokerCapacityAndOwnedCashCombined': False, 'estimatedFeesAreActualFees': False,
        'privateValuesLocalOnly': True,
        'hashMeaning': 'LOCAL_DIAGNOSTIC_DEPENDENCIES_NOT_FINANCIAL_AUTHENTICATION',
        'retrievalTimeMeaning': 'LOCAL_GET_TIME_NOT_FX_OR_SETTLEMENT_FINALITY',
        **_FALSE}
    return {**result, 'contentHash': _hash(result)}


def safe_shared_krw_readiness_summary(value):
    """Strip amounts, accounts and per-book dependencies for the public UI."""
    if (type(value) is not dict or value.get('schemaVersion') != SCHEMA
        or value.get('status') != 'BLOCKED' or any(value.get(k) is not False for k in _FALSE)
        or type(value.get('checks')) is not dict or set(value['checks']) != _CHECKS
        or any(type(v) is not bool for v in value['checks'].values())
        or value['checks']['completeAccountWorkingOrdersVerified'] is not False
        or type(value.get('reasons')) is not list
        or any(type(reason) is not str or not re.fullmatch(r'KIS_SHARED_KRW_[A-Z0-9_]{1,100}', reason, re.ASCII)
               for reason in value['reasons'])
        or type(value.get('currencyBooks')) is not dict
        or not set(value['currencyBooks']).issubset({'KRW', 'USD'})
        or any(value.get(k) is not None and (type(value[k]) is not int or value[k] < 0)
               for k in ('localOwnedBookCount', 'localOrderRowCount', 'localNonterminalLabelCount'))
        or type(value.get('dependencyHash')) is not str or not _HASH.fullmatch(value['dependencyHash'])
        or value.get('privateValuesLocalOnly') is not True
        or value.get('contentHash') != _hash({k: v for k, v in value.items() if k != 'contentHash'})):
        raise ValueError('KIS_SHARED_KRW_DIAGNOSTIC_INVALID')
    return {'schemaVersion': SUMMARY_SCHEMA, 'status': 'BLOCKED', 'brokerId': 'kis',
        'reasons': list(value['reasons']), 'checks': dict(value['checks']),
        'dependencyHash': value['dependencyHash'], 'contentHash': value['contentHash'],
        'localOwnedBookCount': value['localOwnedBookCount'],
        'localOrderRowCount': value['localOrderRowCount'],
        'localNonterminalLabelCount': value['localNonterminalLabelCount'],
        'observedBookCurrencies': sorted(value['currencyBooks']),
        'sharedKrwAvailableAmount': None, 'currencyConversionPerformed': False,
        'hashMeaning': value['hashMeaning'], **_FALSE}
