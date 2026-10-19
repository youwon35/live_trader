"""Unconfirmed fee settings and exact commission-only preparation estimates.

Public BanKIS/US online tariffs are defaults, never the user's personal REST
OpenAPI agreement. Saving an estimate cannot confirm it. Missing minimum fees,
taxes, FX costs and settlement adjustments stay None; no total or quantity is
calculated. This pure module has no environment, file, broker or order access.
"""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, localcontext
import hashlib
import json
import re

PROFILE_SCHEMA = 'kis-provisional-fee-profile-v1'
ESTIMATE_SCHEMA = 'kis-provisional-fee-estimate-v1'
STATUS = 'PROVISIONAL_CONFIRMATION_REQUIRED'
SOURCE_CLASSIFICATION = 'PUBLIC_BASELINE_NOT_PERSONAL_AGREEMENT'
DOMESTIC_SOURCE_URL = 'https://www.truefriend.com/main/customer/guide/_static/TF04ae010000.jsp?templet-bypass=true'
US_SOURCE_URL = 'https://www.truefriend.com/main/bond/research/_static/TF03ca050000.jsp'
FEE_SETTING_DEFAULTS = {
    'KIS_ESTIMATED_KRX_STOCK_FEE_PERCENT': '0.0140527',
    'KIS_ESTIMATED_KRX_ETF_FEE_PERCENT': '0.0146527',
    'KIS_ESTIMATED_NXT_STOCK_FEE_PERCENT': '0.0130527',
    'KIS_ESTIMATED_US_FEE_PERCENT': '0.25',
}
FEE_SETTING_KEYS = tuple(FEE_SETTING_DEFAULTS)
SETTING_DEFAULTS = FEE_SETTING_DEFAULTS
FEE_SETTING_SOURCE_URLS = {key: US_SOURCE_URL if key == 'KIS_ESTIMATED_US_FEE_PERCENT'
    else DOMESTIC_SOURCE_URL for key in FEE_SETTING_KEYS}
_ROWS = (
    ('KRX', 'STOCK', FEE_SETTING_KEYS[0], 'KRW'),
    ('KRX', 'ETF', FEE_SETTING_KEYS[1], 'KRW'),
    ('NXT', 'STOCK', FEE_SETTING_KEYS[2], 'KRW'),
    ('US', 'STOCK', FEE_SETTING_KEYS[3], 'USD'),
    ('US', 'ETF', FEE_SETTING_KEYS[3], 'USD'),
)
_RATE = re.compile(r'(?:0|[1-9][0-9]{0,2})(?:\.[0-9]{1,12})?\Z', re.ASCII)
_NOTIONAL = re.compile(r'(?:0|[1-9][0-9]{0,35})(?:\.[0-9]{1,36})?\Z', re.ASCII)
_FALSE = {'accountFeeRateApproved': False, 'actualPersonalFeeObserved': False,
    'actualFeeObserved': False, 'quantityAuthorityGranted': False,
    'orderAuthorityGranted': False, 'productionReady': False, 'financialReady': False}
_UNKNOWN_COSTS = {'minimumCommission': None, 'buyTaxes': None, 'sellTaxes': None,
    'automaticFxRate': None, 'automaticFxSpread': None, 'settlementAdjustment': None}


class KisProvisionalFeeError(ValueError):
    """Fixed safe code only, never the supplied settings or account data."""


def _require(condition, code):
    if not condition:
        raise KisProvisionalFeeError(code)


def _canonical(number):
    value = format(number, 'f')
    return value.rstrip('0').rstrip('.') if '.' in value else value


def _hash(value):
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, OverflowError):
        raise KisProvisionalFeeError('KIS_PROVISIONAL_FEE_CANONICAL_INPUT_INVALID') from None
    return hashlib.sha256(raw).hexdigest()


def validate_fee_setting_value(key, value):
    """Return a canonical percentage; blank explicitly restores its default."""
    _require(type(key) is str and key in FEE_SETTING_DEFAULTS,
        'KIS_PROVISIONAL_FEE_SETTING_KEY_INVALID')
    _require(type(value) is str, 'KIS_PROVISIONAL_FEE_PERCENT_TEXT_REQUIRED')
    if value == '':
        return FEE_SETTING_DEFAULTS[key]
    _require(_RATE.fullmatch(value) is not None,
        'KIS_PROVISIONAL_FEE_PLAIN_PERCENT_REQUIRED')
    rate = Decimal(value)
    _require(rate.is_finite() and 0 <= rate <= 100,
        'KIS_PROVISIONAL_FEE_PERCENT_OUT_OF_RANGE')
    return _canonical(rate)


def fee_setting_metadata(key):
    if key not in FEE_SETTING_DEFAULTS:
        return {}
    return {'confirmationRequired': True, 'estimateOnly': True, 'status': STATUS,
        'sourceUrl': FEE_SETTING_SOURCE_URLS[key], 'rateUnit': 'PERCENT',
        'sourceClassification': SOURCE_CLASSIFICATION, **_FALSE}


def build_provisional_fee_profile(settings=None):
    """Ignore unrelated settings, keeping all unconfirmed fee roles explicit."""
    settings = {} if settings is None else settings
    _require(isinstance(settings, Mapping), 'KIS_PROVISIONAL_FEE_SETTINGS_INVALID')
    values = {key: validate_fee_setting_value(key, settings.get(key, default))
        for key, default in FEE_SETTING_DEFAULTS.items()}
    rows = []
    with localcontext() as context:
        context.prec = 128
        for market, product, key, currency in _ROWS:
            percent = values[key]
            rows.append({'market': market, 'product': product, 'settingKey': key,
                'ratePercent': percent, 'rateFraction': _canonical(Decimal(percent) / Decimal(100)),
                'currency': currency, 'publicDefaultPercent': FEE_SETTING_DEFAULTS[key],
                'sourceUrl': FEE_SETTING_SOURCE_URLS[key],
                'valueOrigin': 'PUBLIC_DEFAULT' if percent == FEE_SETTING_DEFAULTS[key]
                    else 'UNCONFIRMED_SETTING',
                'status': STATUS, 'confirmationRequired': True, 'estimateOnly': True})
    body = {'schemaVersion': PROFILE_SCHEMA, 'status': STATUS,
        'confirmationRequired': True, 'estimateOnly': True,
        'sourceClassification': SOURCE_CLASSIFICATION,
        'rateUnit': 'PERCENT', 'rates': rows, 'unknownCosts': dict(_UNKNOWN_COSTS),
        'costScope': 'UNCONFIRMED_COMMISSION_ONLY_NOT_TOTAL_COST', **_FALSE}
    return {**body, 'feeProfileHash': _hash(body)}


def validate_provisional_fee_profile(profile):
    """Rebuild the entire fixed profile, rejecting extra/private/verified data."""
    _require(type(profile) is dict and type(profile.get('rates')) is list
        and len(profile['rates']) == len(_ROWS), 'KIS_PROVISIONAL_FEE_PROFILE_INVALID')
    settings = {}
    for row in profile['rates']:
        _require(type(row) is dict and type(row.get('settingKey')) is str
            and row['settingKey'] in FEE_SETTING_DEFAULTS,
            'KIS_PROVISIONAL_FEE_PROFILE_INVALID')
        key = row['settingKey']
        value = validate_fee_setting_value(key, row.get('ratePercent'))
        _require(key not in settings or settings[key] == value,
            'KIS_PROVISIONAL_FEE_PROFILE_CONFLICT')
        settings[key] = value
    _require(set(settings) == set(FEE_SETTING_DEFAULTS), 'KIS_PROVISIONAL_FEE_PROFILE_INVALID')
    expected = build_provisional_fee_profile(settings)
    _require(_hash(profile) == _hash(expected),
        'KIS_PROVISIONAL_FEE_PROFILE_CHANGED_OR_AUTHORITY_FORBIDDEN')
    return expected


def estimate_fee(profile, market, product, notional=None):
    """Exact native commission estimate; None input remains None, not zero."""
    profile = validate_provisional_fee_profile(profile)
    _require(type(market) is str and type(product) is str,
        'KIS_PROVISIONAL_FEE_EXACT_MARKET_PRODUCT_REQUIRED')
    rows = [row for row in profile['rates'] if row['market'] == market and row['product'] == product]
    _require(len(rows) == 1, 'KIS_PROVISIONAL_FEE_UNSUPPORTED_MARKET_PRODUCT')
    row = rows[0]
    if notional is not None:
        _require(type(notional) is str and _NOTIONAL.fullmatch(notional) is not None,
            'KIS_PROVISIONAL_FEE_PLAIN_NOTIONAL_TEXT_REQUIRED')
    with localcontext() as context:
        context.prec = 128
        native = None if notional is None else _canonical(Decimal(notional) * Decimal(row['rateFraction']))
    return {'schemaVersion': ESTIMATE_SCHEMA, 'status': STATUS,
        'confirmationRequired': True, 'estimateOnly': True,
        'market': market, 'product': product, 'notional': notional,
        'notionalMeaning': 'USER_ENTERED_NOTIONAL_NOT_EXECUTION',
        'ratePercent': row['ratePercent'], 'rateFraction': row['rateFraction'],
        'nativeEstimate': native, 'currency': row['currency'],
        'feeProfileHash': profile['feeProfileHash'], 'roundingApplied': False,
        'settlementRoundingRule': None, 'unknownCosts': dict(_UNKNOWN_COSTS),
        'costScope': 'UNCONFIRMED_COMMISSION_ONLY_NOT_TOTAL_COST', **_FALSE}
