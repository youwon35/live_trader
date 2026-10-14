"""Reference-only KIS claim validation; never authenticates observation or permits orders.

No production entry imports this module. Callers provide independent expected
scope and UTC ``now``. The 15-second bound validates claim freshness only.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from urllib.parse import urlsplit
from trading_runtime.execution_quantity import validate_quantity_contract

SCHEMA = 'kis-current-equity-reference-observation-v1'
FRESHNESS_SECONDS = 15
TOP_KEYS = frozenset(('schemaVersion', 'purpose', 'provider', 'environment', 'assetClass',
    'market', 'currency', 'symbol', 'exchange', 'side', 'account', 'scope', 'request',
    'response', 'fee', 'requestStartedAt', 'responseReceivedAt', 'expiresAt', 'contentHash'))
SCOPE_KEYS = frozenset(('deploymentId', 'deploymentRevision', 'sourceArtifactHash',
    'sourceQuantityContractHash', 'strategyInstanceId', 'strategyInstanceHash', 'portfolioId', 'portfolioHash'))
ACCOUNT_KEYS = frozenset(('accountAlias', 'accountFingerprint', 'credentialConfigurationHash'))
IDENTITY_KEYS = frozenset(('provider', 'environment', 'market', 'currency', 'symbol', 'exchange', 'side'))
EXPECTED_KEYS = SCOPE_KEYS | ACCOUNT_KEYS | IDENTITY_KEYS | {'queryProjection'}


def _require(condition, reason):
    if not condition:
        raise ValueError('KIS_REFERENCE_' + reason)


def _plain(value, active=None):
    """Reject non-JSON/cycles instead of hashing default=str or custom coercions."""
    active = set() if active is None else active
    if type(value) in (str, int, bool) or value is None:
        return
    _require(type(value) in (dict, list), 'PLAIN_JSON_REQUIRED')
    identity = id(value)
    _require(identity not in active, 'CYCLIC_JSON')
    active.add(identity)
    try:
        values = value.values() if type(value) is dict else value
        if type(value) is dict:
            _require(all(type(key) is str for key in value), 'JSON_STRING_KEYS_REQUIRED')
        for child in values:
            _plain(child, active)
    finally:
        active.remove(identity)


def _encoded(value):
    _plain(value)
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def observation_content_hash(body):
    """Self-consistency digest, explicitly not a producer/transport signature."""
    return hashlib.sha256(_encoded(body)).hexdigest()


def _keys(value, keys, label):
    _require(type(value) is dict and set(value) == set(keys), label + '_EXACT_FIELDS')


def _text(value, label):
    _require(type(value) is str and 0 < len(value) <= 128 and value == value.strip()
             and not any(ord(c) < 32 or ord(c) == 127 for c in value), label + '_TEXT_INVALID')


def _hash(value, label):
    _require(type(value) is str and bool(re.fullmatch('[a-f0-9]{64}', value)), label + '_HASH_INVALID')


def _decimal(value, label, *, positive=False, integer=False):
    _require(type(value) is str and len(value) <= 512 and
             bool(re.fullmatch(r'(?:0|[1-9][0-9]*)(?:\.[0-9]+)?', value)), label + '_DECIMAL_STRING_REQUIRED')
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError('KIS_REFERENCE_' + label + '_DECIMAL_INVALID') from None
    _require(number.is_finite() and len(number.as_tuple().digits) <= 256
             and abs(number.as_tuple().exponent) <= 256, label + '_DECIMAL_BOUND')
    canonical = format(number, 'f')
    if '.' in canonical:
        canonical = canonical.rstrip('0').rstrip('.')
    _require(canonical == value, label + '_DECIMAL_NONCANONICAL')
    _require(number > 0 if positive else number >= 0, label + '_DECIMAL_RANGE')
    if integer:
        _require(number == number.to_integral_value(), label + '_WHOLE_SHARE_REQUIRED')
    return number


def _utc(value):
    _require(type(value) is str and bool(re.fullmatch(
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z', value)), 'CANONICAL_UTC_REQUIRED')
    try:
        return datetime.fromisoformat(value[:-1] + '+00:00')
    except ValueError:
        raise ValueError('KIS_REFERENCE_UTC_INVALID') from None


@dataclass(frozen=True)
class KisReferenceValidation:
    _receipt_json: str = field(repr=False)
    schema_validation: str = field(default='VALID', init=False)
    verification_status: str = field(default='UNVERIFIED', init=False)
    native_eligibility: str = field(default='UNKNOWN', init=False)
    live_allowed: bool = field(default=False, init=False)
    order_authority_granted: bool = field(default=False, init=False)
    actual_fee_observed: bool = field(default=False, init=False)

    @property
    def receipt(self):
        """Detached copy: modifying it cannot change the validation snapshot."""
        return json.loads(self._receipt_json)

    def to_dict(self):
        return {'schema_validation': self.schema_validation,
                'verification_status': self.verification_status,
                'nativeEligibility': self.native_eligibility,
                'live_allowed': self.live_allowed,
                'orderAuthorityGranted': self.order_authority_granted,
                'actualFeeObserved': self.actual_fee_observed,
                'receiptContentHash': self.receipt['contentHash'],
                'scopeMeaning': 'MATCHED_CLAIM_NOT_AUTHENTICATED_OBSERVATION'}


def validate_kis_equity_observation(payload, *, expected_scope, source_quantity_contract, now):
    """Validate exact canonical BUY-cap reference claims without any I/O.

    A rehashed claim can still be invented. Even valid inputs remain
    UNVERIFIED/livefalse. SELL caps, actual personal rates, live capability,
    execution prices, capital ownership and broker rounding are unsupported.
    """
    _plain(payload)
    _keys(payload, TOP_KEYS, 'OBSERVATION')
    _plain(expected_scope)
    _keys(expected_scope, EXPECTED_KEYS, 'EXPECTED_SCOPE')
    _plain(source_quantity_contract)
    source = validate_quantity_contract(source_quantity_contract)
    _require(source['schemaVersion'] == 'native-equity-quantity-v1'
             and source['capabilityStatus'] == 'UNKNOWN'
             and source['executionScope'] == 'LOCAL_MODEL_ONLY', 'FROZEN_EQUITY_MODEL_REQUIRED')
    body = {key: value for key, value in payload.items() if key != 'contentHash'}
    _hash(payload['contentHash'], 'CONTENT')
    _require(observation_content_hash(body) == payload['contentHash'], 'CONTENT_HASH_MISMATCH')
    _require(payload['schemaVersion'] == SCHEMA and payload['purpose'] == 'READ_ONLY_PREPARATION', 'SCHEMA_PURPOSE')
    _require(payload['provider'] == 'kis' and payload['environment'] == 'KIS_LIVE'
             and payload['assetClass'] == 'EQUITY' and payload['side'] == 'BUY', 'PRODUCTION_BUY_REFERENCE_ONLY')
    domestic = source['exchange'] == 'KRX'
    wanted = {'exchange': source['exchange'], 'currency': source['quoteCurrency'],
              'symbol': source['symbol'], 'market': 'KR_STOCK' if domestic else 'US_STOCK'}
    _require(all(payload[key] == value for key, value in wanted.items()), 'SOURCE_MARKET_CHANGED')
    _keys(payload['account'], ACCOUNT_KEYS, 'ACCOUNT')
    _keys(payload['scope'], SCOPE_KEYS, 'SCOPE')
    joined = {**{key: payload[key] for key in IDENTITY_KEYS}, **payload['account'], **payload['scope']}
    _require(all(type(joined[key]) is type(expected_scope[key]) and joined[key] == expected_scope[key] for key in joined), 'EXPECTED_IDENTITY_CHANGED')
    _text(joined['accountAlias'], 'ACCOUNT_ALIAS')
    for key in ('accountFingerprint', 'credentialConfigurationHash', 'sourceArtifactHash',
                'sourceQuantityContractHash', 'strategyInstanceHash'):
        _hash(joined[key], key.upper())
    for key in ('deploymentId', 'strategyInstanceId'):
        _text(joined[key], key.upper())
    _require(type(joined['deploymentRevision']) is int and joined['deploymentRevision'] >= 0, 'REVISION_INVALID')
    _require(joined['sourceQuantityContractHash'] == source['contractHash'], 'SOURCE_CONTRACT_CHANGED')
    _require(type(joined['portfolioId']) is str and type(joined['portfolioHash']) is str, 'PORTFOLIO_PAIR_INVALID')
    if joined['portfolioId'] or joined['portfolioHash']:
        _text(joined['portfolioId'], 'PORTFOLIO_ID')
        _hash(joined['portfolioHash'], 'PORTFOLIO')
    begin, received, expires = (_utc(payload[key]) for key in ('requestStartedAt', 'responseReceivedAt', 'expiresAt'))
    _require(type(now) is datetime and type(now.tzinfo) is timezone and now.utcoffset() == timedelta(0), 'NOW_UTC_REQUIRED')
    _require(begin <= received <= now <= expires, 'TIME_ORDER_FUTURE_OR_EXPIRED')
    _require((now-begin).total_seconds() <= FRESHNESS_SECONDS
             and 0 <= (expires-begin).total_seconds() <= FRESHNESS_SECONDS, 'FRESHNESS_WINDOW')
    request = payload['request']
    _keys(request, {'method', 'endpoint', 'trId', 'bodyAbsent', 'queryProjection', 'requestHash'}, 'REQUEST')
    endpoint = '/uapi/domestic-stock/v1/trading/inquire-psbl-order' if domestic else '/uapi/overseas-stock/v1/trading/inquire-psamount'
    _require(request['method'] == 'GET' and request['endpoint'] == endpoint
             and request['trId'] == ('TTTC8908R' if domestic else 'TTTS3007R')
             and request['bodyAbsent'] is True, 'EXACT_GET_ROUTE_REQUIRED')
    _hash(request['requestHash'], 'REQUEST')
    public_request = {key: value for key, value in request.items() if key != 'requestHash'}
    _require(observation_content_hash(public_request) == request['requestHash'], 'REQUEST_PROJECTION_CHANGED')
    query = request['queryProjection']
    query_keys = {'PDNO', 'ORD_UNPR', 'ORD_DVSN', 'CMA_EVLU_AMT_ICLD_YN', 'OVRS_ICLD_YN'} if domestic else {'OVRS_EXCG_CD', 'OVRS_ORD_UNPR', 'ITEM_CD'}
    _keys(query, query_keys, 'PUBLIC_QUERY')
    _require(query == expected_scope['queryProjection'], 'EXPECTED_QUERY_CHANGED')
    price = _decimal(query['ORD_UNPR' if domestic else 'OVRS_ORD_UNPR'], 'QUERY_PRICE', positive=True)
    _require(query['PDNO' if domestic else 'ITEM_CD'] == source['symbol'], 'QUERY_SYMBOL_CHANGED')
    if domestic:
        _require(query['ORD_DVSN'] == '00' and query['CMA_EVLU_AMT_ICLD_YN'] == 'N'
                 and query['OVRS_ICLD_YN'] == 'N', 'QUERY_DOMESTIC_SCOPE_CHANGED')
    else:
        _require(query['OVRS_EXCG_CD'] == source['exchange'], 'QUERY_EXCHANGE_CHANGED')
    response = payload['response']
    _keys(response, {'currency', 'availableCash', 'maximumBuyQuantity', 'calculationPrice', 'rounding', 'projectionHash'}, 'RESPONSE')
    _require(response['currency'] == source['quoteCurrency']
             and response['rounding'] == 'UNSUPPORTED_BROKER_ROUNDING_REFERENCE_ONLY', 'RESPONSE_CURRENCY_OR_ROUNDING')
    _decimal(response['availableCash'], 'AVAILABLE_CASH')
    _decimal(response['maximumBuyQuantity'], 'MAXIMUM_BUY_QUANTITY', integer=True)
    _require(_decimal(response['calculationPrice'], 'CALCULATION_PRICE', positive=True) == price, 'CALCULATION_PRICE_CHANGED')
    _hash(response['projectionHash'], 'RESPONSE_PROJECTION')
    projection = {key: value for key, value in response.items() if key != 'projectionHash'}
    _require(observation_content_hash(projection) == response['projectionHash'], 'RESPONSE_PROJECTION_CHANGED')
    fee = payload['fee']
    _keys(fee, {'kind', 'rate', 'unit', 'currency', 'side', 'uri'}, 'FEE')
    _require(fee['currency'] == source['quoteCurrency'] and fee['side'] == 'BUY', 'FEE_SCOPE_CHANGED')
    if fee['kind'] == 'UNKNOWN':
        _require(fee['rate'] is None and fee['unit'] is None and fee['uri'] is None, 'UNKNOWN_FEE_MUST_NOT_HAVE_RATE')
    else:
        _require(fee['kind'] == 'PUBLIC_REFERENCE' and fee['unit'] == 'FRACTION', 'ACTUAL_FEE_PRODUCER_NOT_IMPLEMENTED')
        _require(_decimal(fee['rate'], 'PUBLIC_FEE') < 1, 'PUBLIC_FEE_RANGE')
        _require(type(fee['uri']) is str and len(fee['uri']) <= 2048 and not any(ord(c) < 33 for c in fee['uri']), 'PUBLIC_URI_INVALID')
        try:
            uri = urlsplit(fee['uri'])
            _require(uri.scheme == 'https' and bool(uri.hostname) and uri.username is None
                     and uri.password is None and '?' not in fee['uri'] and '#' not in fee['uri']
                     and uri.port in (None, 443), 'PUBLIC_URI_SECRET_OR_SCHEME')
        except ValueError:
            raise ValueError('KIS_REFERENCE_PUBLIC_URI_INVALID') from None
    return KisReferenceValidation(_encoded(payload).decode('utf-8'))
