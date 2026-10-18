"""Bind a local reviewed KRW budget reservation to one owned capital scope.

This bridge performs no conversion and authenticates no broker cash, fees or
settlement. A foreign-currency native principal still needs its separate
verified funding producer. A local KRW budget may be earmarked for that later
step without pretending that an exchange or a native principal occurred.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING
from trading_runtime.artifact_governance import stable_sha256
from .kis_shared_krw_reservations import (
    validate_policy, validate_request, canonical_hash, POLICY_SCHEMA, REQUEST_SCHEMA,
)

INPUT_SCHEMA = 'kis-shared-krw-reviewed-allocation-input-v1'


def validate_reviewed_allocation_input(value, *, account=None):
    """A review input contains no future deployment/scope hash (no hash cycle)."""
    if (type(value) is not dict or set(value) != {'schemaVersion', 'policy', 'allocationKrw', 'inputHash'}
            or value.get('schemaVersion') != INPUT_SCHEMA
            or value.get('inputHash') != canonical_hash({k: v for k, v in value.items() if k != 'inputHash'})):
        raise ValueError('KIS_SHARED_KRW_REVIEWED_ALLOCATION_REQUIRED')
    policy = validate_policy(value['policy'])
    amount = value['allocationKrw']
    import re
    if type(amount) is not str or not re.fullmatch(r'[1-9][0-9]{0,23}', amount, re.ASCII):
        raise ValueError('KIS_SHARED_KRW_REVIEWED_WHOLE_WON_REQUIRED')
    if int(amount) > int(policy['capKrw']):
        raise ValueError('KIS_SHARED_KRW_REVIEWED_ALLOCATION_EXCEEDS_CAP')
    if account is not None and any(policy[k] != account.get(source) for k, source in
            (('accountFingerprint', 'accountFingerprint'), ('accountProfileId', 'profileId'),
             ('credentialConfigurationHash', 'credentialConfigurationHash'))):
        raise ValueError('KIS_SHARED_KRW_REVIEWED_ACCOUNT_CHANGED')
    return {'schemaVersion': INPUT_SCHEMA, 'policy': policy,
        'allocationKrw': amount, 'inputHash': value['inputHash']}


def shared_plan_from_reviewed_input(scope, value):
    review = validate_reviewed_allocation_input(value)
    policy = review['policy']
    basis = canonical_hash({'scopeHash': scope['scopeHash'],
        'liveReviewBindingHash': scope.get('reviewBindingHash'),
        'reviewedAllocationInputHash': review['inputHash']})
    plan = prepare_local_shared_plan(scope, cap_krw=policy['capKrw'],
        allocation_krw=review['allocationKrw'], budget_review_hash=policy['reviewBindingHash'],
        basis_hash=basis)
    if plan['policy'] != policy:
        raise ValueError('KIS_SHARED_KRW_REVIEWED_POLICY_CHANGED')
    return plan


def prepare_local_shared_plan(scope, *, cap_krw, allocation_krw, budget_review_hash, basis_hash):
    """Build an explicit local budget plan for a later normal reviewed commit.

    No amount is inferred from broker capacity, FX or assumed personal fees.
    No policy is installed and no reservation is created by this pure helper.
    """
    policy = {'schemaVersion': POLICY_SCHEMA,
        'accountFingerprint': scope['accountFingerprint'],
        'accountProfileId': scope['accountProfileId'],
        'credentialConfigurationHash': scope['credentialConfigurationHash'],
        'capKrw': cap_krw, 'reviewBindingHash': budget_review_hash}
    policy['policyHash'] = canonical_hash(policy)
    request = {'schemaVersion': REQUEST_SCHEMA,
        'reservationId': 'allocation:' + canonical_hash({'deploymentId': scope['deploymentId'],
            'accountFingerprint': scope['accountFingerprint']}),
        'deploymentId': scope['deploymentId'], 'scopeHash': scope['scopeHash'],
        'accountFingerprint': scope['accountFingerprint'],
        'accountProfileId': scope['accountProfileId'],
        'credentialConfigurationHash': scope['credentialConfigurationHash'],
        'requestedKrw': allocation_krw, 'basisHash': basis_hash,
        'nativeCurrency': scope['quoteCurrency'], 'policyHash': policy['policyHash']}
    request['requestHash'] = canonical_hash(request)
    return validate_shared_allocation(scope, {'policy': policy, 'request': request})


def validate_shared_allocation(scope, bundle, *, principal=None):
    if type(scope) is not dict or scope.get('scopeHash') != stable_sha256(
            {key: value for key, value in scope.items() if key != 'scopeHash'}):
        raise ValueError('KIS_SHARED_KRW_EXACT_OWNED_SCOPE_REQUIRED')
    if type(bundle) is not dict or set(bundle) != {'policy', 'request'}:
        raise ValueError('KIS_SHARED_KRW_REVIEWED_ALLOCATION_REQUIRED')
    policy = validate_policy(bundle['policy'])
    request = validate_request(bundle['request'])
    bindings = {'deploymentId': 'deploymentId', 'scopeHash': 'scopeHash',
        'accountFingerprint': 'accountFingerprint', 'accountProfileId': 'accountProfileId',
        'credentialConfigurationHash': 'credentialConfigurationHash', 'nativeCurrency': 'quoteCurrency'}
    if any(request[key] != scope.get(source) for key, source in bindings.items()):
        raise ValueError('KIS_SHARED_KRW_ALLOCATION_SCOPE_CHANGED')
    if (request['policyHash'] != policy['policyHash']
            or any(request[key] != policy[key] for key in
                ('accountFingerprint', 'accountProfileId', 'credentialConfigurationHash'))):
        raise ValueError('KIS_SHARED_KRW_ALLOCATION_POLICY_CHANGED')
    if principal is not None:
        # The budget request is not a USD principal or an executed FX rate.
        if scope.get('quoteCurrency') != 'KRW':
            raise ValueError('KIS_SHARED_KRW_NATIVE_FUNDING_PRODUCER_UNAVAILABLE')
        if type(principal) is not Decimal or not principal.is_finite() or principal <= 0:
            raise ValueError('KIS_SHARED_KRW_NATIVE_PRINCIPAL_INVALID')
        rounded = principal.to_integral_value(rounding=ROUND_CEILING)
        if Decimal(request['requestedKrw']) < rounded:
            raise ValueError('KIS_SHARED_KRW_RESERVATION_BELOW_NATIVE_PRINCIPAL')
    return {'policy': policy, 'request': request}


def assert_owned_books_covered(db, account_fingerprint, pool):
    """Every existing currency book must already have its exact reservation.

    Existing legacy books never silently become zero or migrate into a pool.
    The caller holds the same write transaction as the upcoming append.
    """
    receipts = pool.get('receipts') if type(pool) is dict else None
    requests = {} if receipts is None else {
        receipt['request']['deploymentId']: receipt['request'] for receipt in receipts}
    count = 0
    for deployment_id, currency_key, raw, digest in db.execute(
            'SELECT deployment_id,account_currency,CASE WHEN length(payload)<=65536 THEN payload ELSE NULL END,content_hash '
            'FROM principals WHERE account_currency LIKE ? LIMIT 10001', (account_fingerprint + ':%',)):
        count += 1
        if count > 10000 or type(raw) is not str:
            raise ValueError('KIS_SHARED_KRW_EXISTING_PRINCIPAL_BOUND_EXCEEDED')
        import json
        def pairs(items):
            parsed = {}
            for key, value in items:
                if key in parsed:raise ValueError('KIS_SHARED_KRW_EXISTING_PRINCIPAL_INVALID_JSON')
                parsed[key] = value
            return parsed
        def constant(value):raise ValueError('KIS_SHARED_KRW_EXISTING_PRINCIPAL_INVALID_JSON')
        body = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
        if (type(body) is not dict or set(body) != {'scope', 'principal', 'occurredAt'}
                or stable_sha256(body) != digest):
            raise ValueError('KIS_SHARED_KRW_EXISTING_PRINCIPAL_SEAL_INVALID')
        scope = body['scope']
        if (type(scope) is not dict or scope.get('deploymentId') != deployment_id
                or scope.get('accountFingerprint') != account_fingerprint
                or currency_key != account_fingerprint + ':' + str(scope.get('quoteCurrency'))):
            raise ValueError('KIS_SHARED_KRW_EXISTING_PRINCIPAL_ACCOUNT_CHANGED')
        request = requests.get(scope.get('deploymentId'))
        if request is None or pool.get('policy') is None:
            raise ValueError('KIS_SHARED_KRW_EXISTING_BOOK_REVIEWED_MIGRATION_REQUIRED')
        validate_shared_allocation(scope, {'policy': pool['policy'], 'request': request},
            principal=Decimal(body['principal']))


def reservation_for_scope(scope, pool):
    if type(pool) is not dict or pool.get('status') != 'LOCAL_BUDGET_STATE_ONLY':
        raise ValueError('KIS_SHARED_KRW_LOCAL_POOL_NOT_INITIALIZED')
    matches = [receipt for receipt in pool['receipts']
        if receipt['request']['deploymentId'] == scope['deploymentId']]
    if len(matches) != 1:
        raise ValueError('KIS_SHARED_KRW_EXACT_LOCAL_RESERVATION_REQUIRED')
    receipt = matches[0]
    validate_shared_allocation(scope, {'policy': pool['policy'], 'request': receipt['request']})
    return receipt
