"""Pure domestic KIS response conversion into unverified reference claims.

No transport, clock, account, runtime or file access. The caller supplies raw
bytes and independent expected scope; neither those claims nor self hashes
prove authentication. The existing reference-only v1 validator is unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

from .native_equity_observation import (
    ACCOUNT_KEYS, IDENTITY_KEYS, SCHEMA, SCOPE_KEYS, KisReferenceValidation,
    _encoded, observation_content_hash, validate_kis_equity_observation,
)

PROVENANCE_SCHEMA = 'kis-kr-reference-conversion-provenance-v1'
PRODUCTION_ORIGIN = 'https://openapi.koreainvestment.com:9443'
DOMESTIC_ENDPOINT = '/uapi/domestic-stock/v1/trading/inquire-psbl-order'
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
PUBLIC_REQUEST_KEYS = frozenset(('method', 'endpoint', 'trId', 'bodyAbsent', 'queryProjection'))


class KisReferenceConversionError(ValueError):
    """Fixed codes only: remote messages, raw bytes and values are never echoed."""


def _require(condition, code):
    if not condition:
        raise KisReferenceConversionError('KIS_CONVERSION_' + code)


def _plain_copy(value):
    return json.loads(_encoded(value))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, 'DUPLICATE_JSON_KEY')
        result[key] = value
    return result


def _reject_constant(_value):
    raise KisReferenceConversionError('KIS_CONVERSION_NONFINITE_JSON_CONSTANT')


def _raw_json(raw_response):
    _require(type(raw_response) is bytes and 0 < len(raw_response) <= MAX_RESPONSE_BYTES,
             'RAW_BYTES_BOUND')
    return json.loads(raw_response.decode('utf-8'), object_pairs_hook=_unique_object,
                      parse_int=Decimal, parse_float=Decimal, parse_constant=_reject_constant)


def _number(value, *, positive=False, whole=False):
    # String lexical cleanup is lossless. No binary floats, quantize or rounding.
    if type(value) is str:
        _require(0 < len(value) <= 512 and bool(re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', value)),
                 'SELECTED_NUMBER_FORMAT')
        number = Decimal(value)
    else:
        _require(type(value) is Decimal, 'SELECTED_NUMBER_TYPE')
        number = value
    _require(number.is_finite() and not number.is_signed()
             and len(number.as_tuple().digits) <= 256
             and abs(number.as_tuple().exponent) <= 256, 'SELECTED_NUMBER_BOUND')
    _require(number > 0 if positive else number >= 0, 'SELECTED_NUMBER_RANGE')
    if whole:
        _require(number == number.to_integral_value(), 'WHOLE_SHARE_REQUIRED')
    return number


def _canonical(number):
    value = format(number, 'f')
    return value.rstrip('0').rstrip('.') if '.' in value else value


@dataclass(frozen=True)
class KisReferenceConversion:
    _validation: KisReferenceValidation = field(repr=False)
    _provenance_json: str = field(repr=False)
    verification_status: str = field(default='UNVERIFIED', init=False)
    native_eligibility: str = field(default='UNKNOWN', init=False)
    live_allowed: bool = field(default=False, init=False)
    order_authority_granted: bool = field(default=False, init=False)
    actual_fee_observed: bool = field(default=False, init=False)
    authenticated_transport: bool = field(default=False, init=False)

    @property
    def reference(self):
        """Detached existing v1 reference claim, including its scoped cap values."""
        return self._validation.receipt

    @property
    def provenance(self):
        """Detached public projection and digests; contains no raw response values."""
        return json.loads(self._provenance_json)

    def to_dict(self):
        """Diagnostic summary: no cash, response operands, private IDs or raw body."""
        provenance = self.provenance
        return {**self._validation.to_dict(),
                'conversionSchema': PROVENANCE_SCHEMA,
                'provenanceContentHash': provenance['contentHash'],
                'rawResponseSha256': provenance['rawResponseSha256'],
                'rawResponseBytes': provenance['rawResponseBytes'],
                'authenticatedTransport': False,
                'cashMeaning': 'QUERY_REFERENCE_NOT_OWNED_CAPITAL',
                'quantityMeaning': 'CLASS00_QUERY_CAP_NOT_NATIVE_MARKET_RULE',
                'affordabilityVerified': False}


def _convert(raw_response, *, request_public, request_origin, response_continuation,
             expected_scope, source_quantity_contract, request_started_at,
             response_received_at, expires_at, now):
    # Detach exact plain JSON first; unsupported custom types never enter hashes.
    request = _plain_copy(request_public)
    expected = _plain_copy(expected_scope)
    source = _plain_copy(source_quantity_contract)
    _require(type(request) is dict and set(request) == PUBLIC_REQUEST_KEYS,
             'PUBLIC_REQUEST_EXACT_FIELDS')
    _require(type(expected) is dict and type(source) is dict, 'SCOPE_AND_SOURCE_REQUIRED')
    # Explicitly unsupported even if an untrusted US body inserts a fake echo.
    _require(expected.get('market') != 'US_STOCK' and source.get('exchange') not in
             ('NASD', 'NYSE', 'AMEX') and request.get('endpoint') !=
             '/uapi/overseas-stock/v1/trading/inquire-psamount',
             'US_BROKER_CALCULATION_PRICE_MISSING_UNSUPPORTED')
    _require(type(request_origin) is str and request_origin == PRODUCTION_ORIGIN,
             'DECLARED_PRODUCTION_ORIGIN_REQUIRED')
    _require(type(response_continuation) is str and response_continuation == '',
             'INCOMPLETE_OR_UNSUPPORTED_CONTINUATION')
    _require(request['method'] == 'GET' and request['endpoint'] == DOMESTIC_ENDPOINT
             and request['trId'] == 'TTTC8908R' and request['bodyAbsent'] is True,
             'DOMESTIC_REFERENCE_GET_REQUIRED')
    raw = _raw_json(raw_response)
    _require(type(raw) is dict and type(raw.get('rt_cd')) is str and raw['rt_cd'] == '0',
             'SUCCESS_RESPONSE_REQUIRED')
    output = raw.get('output')
    shape = 'output.object'
    if type(output) is list:
        _require(len(output) == 1 and type(output[0]) is dict, 'SINGLE_OUTPUT_REQUIRED')
        output = output[0]
        shape = 'output.singleton-list'
    _require(type(output) is dict, 'OUTPUT_OBJECT_REQUIRED')
    required = ('ord_psbl_cash', 'nrcvb_buy_amt', 'nrcvb_buy_qty', 'psbl_qty_calc_unpr')
    _require(all(key in output for key in required), 'SELECTED_RESPONSE_FIELDS_REQUIRED')
    orderable_cash = _number(output['ord_psbl_cash'])
    no_margin_amount = _number(output['nrcvb_buy_amt'])
    direct_quantity = _number(output['nrcvb_buy_qty'], whole=True)
    broker_price = _number(output['psbl_qty_calc_unpr'], positive=True)
    response = {'currency': 'KRW',
                'availableCash': _canonical(min(orderable_cash, no_margin_amount)),
                'maximumBuyQuantity': _canonical(direct_quantity),
                'calculationPrice': _canonical(broker_price),
                'rounding': 'UNSUPPORTED_BROKER_ROUNDING_REFERENCE_ONLY'}
    response['projectionHash'] = observation_content_hash(response)
    request['requestHash'] = observation_content_hash(request)
    # No query price substitution, currency echo invention, fee input or fallback.
    payload = {'schemaVersion': SCHEMA, 'purpose': 'READ_ONLY_PREPARATION',
               'assetClass': 'EQUITY', **{key: expected[key] for key in IDENTITY_KEYS},
               'account': {key: expected[key] for key in ACCOUNT_KEYS},
               'scope': {key: expected[key] for key in SCOPE_KEYS},
               'request': request, 'response': response,
               'fee': {'kind': 'UNKNOWN', 'rate': None, 'unit': None, 'currency': 'KRW',
                       'side': 'BUY', 'uri': None},
               'requestStartedAt': request_started_at,
               'responseReceivedAt': response_received_at, 'expiresAt': expires_at}
    payload['contentHash'] = observation_content_hash(payload)
    validation = validate_kis_equity_observation(payload, expected_scope=expected,
                    source_quantity_contract=source, now=now)
    provenance = {
        'schemaVersion': PROVENANCE_SCHEMA,
        'meaning': 'SELF_CONSISTENT_CLAIM_NOT_AUTHENTICATED_TRANSPORT',
        'declaredRequestOrigin': request_origin, 'publicRequest': request,
        'expectedScopeClaimHash': observation_content_hash(expected),
        'sourceQuantityContractHash': source['contractHash'],
        'referenceClaimContentHash': payload['contentHash'],
        'rawResponseSha256': hashlib.sha256(raw_response).hexdigest(),
        'rawResponseBytes': len(raw_response), 'outputShape': shape,
        'responseContinuation': response_continuation,
        'requestStartedAt': request_started_at, 'responseReceivedAt': response_received_at,
        'expiresAt': expires_at,
        'validatedAt': now.isoformat(timespec='microseconds').replace('+00:00', 'Z'),
        'fieldMappings': {
            'availableCash': {'rawFields': ['ord_psbl_cash', 'nrcvb_buy_amt'],
                'roles': ['REPORTED_ORDERABLE_CASH', 'NO_MARGIN_BUY_AMOUNT'],
                'operation': 'CONSERVATIVE_MIN', 'meaning': 'QUERY_REFERENCE_NOT_OWNED_CAPITAL'},
            'maximumBuyQuantity': {'rawFields': ['nrcvb_buy_qty'],
                'operation': 'DIRECT_INTEGER', 'meaning': 'CLASS00_NO_MARGIN_QUERY_BUY_CAP'},
            'calculationPrice': {'rawFields': ['psbl_qty_calc_unpr'],
                'operation': 'DIRECT_BROKER_PRICE_MATCH_NOT_REQUEST_SUBSTITUTION'},
            'currency': {'origin': 'DOMESTIC_ENDPOINT_AND_FROZEN_SOURCE_NOT_RESPONSE_ECHO'},
            'symbol': {'origin': 'PUBLIC_QUERY_AND_FROZEN_SOURCE_NOT_RESPONSE_ECHO'}},
        'ignoredAmountFields': ['max_buy_amt', 'max_buy_qty', 'ord_psbl_sbst', 'cma_evlu_amt',
                               'ord_psbl_frcr_amt_wcrc', 'ovrs_re_use_amt_wcrc'],
        'actualFeeProvenance': 'NONE_UNKNOWN', 'authenticatedTransport': False,
        'orderAuthorityGranted': False, 'capitalOwnershipVerified': False,
        'affordabilityVerified': False, 'nativeMarketMaximumGuaranteed': False,
    }
    provenance['contentHash'] = observation_content_hash(provenance)
    return KisReferenceConversion(validation, _encoded(provenance).decode('utf-8'))


def convert_kis_buy_reference_response(raw_response: bytes, *, request_public,
        request_origin, response_continuation, expected_scope, source_quantity_contract,
        request_started_at, response_received_at, expires_at, now: datetime):
    """Convert one complete KR reference GET response without any I/O or clocks.

    ``request_public`` and ``expected_scope`` are independent caller claims, not
    authenticated request or account observations. Any nonempty continuation is
    rejected; US has no supported calculation-price echo. Errors are fixed codes
    raised outside the catch block so even exception context cannot retain raw
    JSONDecodeError documents or remote content.
    """
    failure = None
    try:
        return _convert(raw_response, request_public=request_public,
            request_origin=request_origin, response_continuation=response_continuation,
            expected_scope=expected_scope, source_quantity_contract=source_quantity_contract,
            request_started_at=request_started_at, response_received_at=response_received_at,
            expires_at=expires_at, now=now)
    except KisReferenceConversionError as error:
        failure = str(error)
    except (ValueError, TypeError, KeyError, InvalidOperation, RecursionError, OverflowError):
        failure = 'KIS_CONVERSION_INPUT_OR_REFERENCE_VALIDATION_FAILED'
    # Never propagate raw parser exceptions, their .doc, remote messages or values.
    raise KisReferenceConversionError(failure)
