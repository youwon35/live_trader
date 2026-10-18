"""KIS overseas funding preference and private read-only capacity diagnostics.

This pure module does not apply for a broker service, exchange money, size an
order, interpret reported capacity as cash, or grant trading authority. A user
preference and the broker's enrollment/financial facts remain separate.
"""
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re

KEY = "KIS_OVERSEAS_FUNDING_MODE"
INTEGRATED_MARGIN = "INTEGRATED_MARGIN"
FOREIGN_CASH = "FOREIGN_CASH"
MODE_KEY = KEY
DEFAULT_MODE = INTEGRATED_MARGIN
SCOPE = "ALL_SUPPORTED_KIS_OVERSEAS_MARKETS"
POLICY_SCHEMA = "kis-overseas-funding-preference-v1"
DIAGNOSTIC_SCHEMA = "kis-overseas-orderable-funding-diagnostic-v1"

# These are broker-service countries, not a list of implemented API executions.
# The service description was revised 2025-12-22. The 2026-07-15 overseas
# securities description repeats the UK/Hong Kong currency restrictions.
SERVICE_COUNTRIES = ("US", "UK", "JP", "CN", "HK", "KR")
SERVICE_CURRENCIES = ("USD", "JPY", "CNY", "HKD", "KRW")
SERVICE_DOCUMENT = "https://file.koreainvestment.com/Storage/customer/guide/regards/GDRs_deposit_money.htm"
RESTRICTIONS_DOCUMENT = "https://file.koreainvestment.com/Storage/customer/guide/regards/service_03_1%283%29.htm"
API_DOCUMENT = "https://raw.githubusercontent.com/koreainvestment/open-trading-api/main/examples_llm/overseas_stock/inquire_psamount/chk_inquire_psamount.py"

_QUERY_MARKETS = {
    "NASD": ("US", "USD"), "NYSE": ("US", "USD"), "AMEX": ("US", "USD"),
    "SEHK": ("HK", "HKD"), "SHAA": ("CN", "CNY"), "SZAA": ("CN", "CNY"),
    "TKSE": ("JP", "JPY"), "HASE": ("VN", "VND"), "VNSE": ("VN", "VND"),
}
_CURRENT_US = frozenset(("NASD", "NYSE", "AMEX"))
_PLAIN_NONNEGATIVE = re.compile(r"[0-9]+(?:\.[0-9]+)?\Z", re.ASCII)
_SCOPE_TOKEN = re.compile(r"[A-Z][A-Z0-9]{1,15}\Z", re.ASCII)
_UNVERIFIED = {
    "brokerEnrollmentVerified": False,
    "settlementVerified": False,
    "sharedCashReservationVerified": False,
    "productionReady": False,
    "orderAuthorityGranted": False,
}


def resolve_funding_mode(settings):
    """Missing/blank uses the requested default; invalid values fail closed."""
    if not isinstance(settings, Mapping):
        raise ValueError("KIS_OVERSEAS_FUNDING_SETTINGS_INVALID")
    value = settings.get(KEY, "")
    if type(value) is not str:
        raise ValueError("KIS_OVERSEAS_FUNDING_MODE_INVALID")
    value = value.strip()
    if not value:
        return DEFAULT_MODE
    if value not in (INTEGRATED_MARGIN, FOREIGN_CASH):
        raise ValueError("KIS_OVERSEAS_FUNDING_MODE_INVALID")
    return value


def _token(value):
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError("KIS_OVERSEAS_FUNDING_SCOPE_INVALID")
    value = value.strip().upper()
    if not _SCOPE_TOKEN.fullmatch(value):
        raise ValueError("KIS_OVERSEAS_FUNDING_SCOPE_INVALID")
    return value


def _capability(exchange, currency):
    market = _QUERY_MARKETS.get(exchange)
    country = market[0] if market else None
    reasons = []
    service = "UNKNOWN"
    if currency in ("GBP", "GBX"):
        service = "DENIED_OFFICIAL_CURRENCY_RESTRICTION"
        reasons.append("INTEGRATED_MARGIN_UK_GBP_GBX_NOT_SUPPORTED")
    elif exchange == "SEHK" and currency == "USD":
        service = "DENIED_OFFICIAL_CURRENCY_RESTRICTION"
        reasons.append("INTEGRATED_MARGIN_HONG_KONG_USD_NOT_SUPPORTED")
    elif market and country not in SERVICE_COUNTRIES:
        service = "NOT_LISTED_IN_OFFICIAL_SERVICE_COUNTRIES"
        reasons.append("INTEGRATED_MARGIN_MARKET_NOT_DOCUMENTED")
    elif market and currency == market[1]:
        service = "DOCUMENTED_MARKET_CURRENCY_ONLY_ACCOUNT_UNVERIFIED"
    elif exchange is not None or currency is not None:
        reasons.append("INTEGRATED_MARGIN_MARKET_CURRENCY_UNVERIFIED")
    current_app = ("CURRENT_US_MARKET_PREPARATION_ONLY"
                   if exchange in _CURRENT_US and currency == "USD"
                   else "UNKNOWN_OR_UNIMPLEMENTED_IN_CURRENT_APP")
    return {
        "exchange": exchange, "currency": currency, "country": country,
        "serviceSupport": service,
        "currentAppSupport": current_app,
        "documentedServiceCountries": list(SERVICE_COUNTRIES),
        "documentedServiceCurrencies": list(SERVICE_CURRENCIES),
        "brokerAccountAndMediaEligibilityVerified": False,
        "extendedAftermarketSupportedForIntegratedAccount": False,
        "restrictedServices": ["EXTENDED_AFTERMARKET_ENROLLMENT"],
        "restrictionCodes": reasons,
        "serviceSupportIsExecutionSupport": False,
    }


def funding_policy(settings, *, exchange=None, currency=None):
    """Describe a preference. No settings, Source, broker or account is changed."""
    mode = resolve_funding_mode(settings)
    exchange, currency = _token(exchange), _token(currency)
    messages = (["해외주식은 지원 시장에서 통합증거금 우선 방식으로 준비합니다.",
                 "증권사 서비스 신청 완료와 계좌·거래매체 적용 여부는 별도로 확인해야 합니다."]
                if mode == INTEGRATED_MARGIN else
                ["해외주식은 보유 외화 우선 방식으로 준비합니다.",
                 "이 선호 설정은 증권사의 통합증거금 가입이나 자동환전 서비스를 변경하지 않습니다."])
    messages += ["환전 후 주문가능금액은 예수금이나 확정 정산금액이 아닙니다.",
                 "지원 국가·통화·거래시간 서비스와 현재 프로그램의 실행 지원 범위는 서로 다릅니다.",
                 "통합증거금 계좌는 애프터마켓 연장 신청 대상에서 제외됩니다.",
                 "이 정보는 실제 주문 권한이나 실거래 준비 완료를 뜻하지 않습니다."]
    body = {
        "schemaVersion": POLICY_SCHEMA,
        "fundingMode": mode,
        "fundingModeLabel": "통합증거금 우선" if mode == INTEGRATED_MARGIN else "보유 외화 우선",
        "scope": SCOPE,
        "autoFxRequested": mode == INTEGRATED_MARGIN,
        "autoFxRequestedMeaning": "USER_PREFERENCE_ONLY_NOT_A_BROKER_INSTRUCTION",
        "brokerServiceChanged": False,
        **_UNVERIFIED,
        "capability": _capability(exchange, currency),
        "userMessages": messages,
    }
    raw = json.dumps(body, sort_keys=True, ensure_ascii=False,
                     separators=(",", ":"), allow_nan=False).encode("utf-8")
    return {**body, "policyHash": hashlib.sha256(raw).hexdigest()}


def _reported_number(value):
    # Official reported literals stay strings; no binary floats, signs,
    # exponent notation, commas, empty value or inferred zero is accepted.
    if type(value) is not str or len(value) > 128:
        raise ValueError("KIS_OVERSEAS_REPORTED_CAPACITY_INVALID")
    value = value.strip()
    if not _PLAIN_NONNEGATIVE.fullmatch(value):
        raise ValueError("KIS_OVERSEAS_REPORTED_CAPACITY_INVALID")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError("KIS_OVERSEAS_REPORTED_CAPACITY_INVALID") from None
    if not number.is_finite() or number < 0:
        raise ValueError("KIS_OVERSEAS_REPORTED_CAPACITY_INVALID")
    return value


def _empty(value):
    return value is None or (type(value) is str and not value.strip())


def parse_orderable_funding(settings, caps, *, exchange, currency):
    """Private reported capacity only; never return spendable cash or authority.

    Existing foreign order capacity, after-exchange capacity and actual account
    cash are not additive. No account-enrollment claim is inferred from them.
    The normal caller owns the authenticated account/query/time binding.
    """
    if type(caps) is not dict:
        raise ValueError("KIS_OVERSEAS_ORDERABLE_RESPONSE_INVALID")
    exchange, currency = _token(exchange), _token(currency)
    if exchange is None or currency is None:
        raise ValueError("KIS_OVERSEAS_FUNDING_SCOPE_INVALID")
    if type(caps.get("tr_crcy_cd")) is not str or caps["tr_crcy_cd"].strip() != currency:
        raise ValueError("KIS_OVERSEAS_ORDERABLE_CURRENCY_MISMATCH")
    policy = funding_policy(settings, exchange=exchange, currency=currency)
    if ("echm_af_ord_psbl_amt" in caps) != ("echm_af_ord_psbl_qty" in caps):
        raise ValueError("KIS_OVERSEAS_AFTER_FX_PARTIAL_CAPACITY")
    amount = caps.get("echm_af_ord_psbl_amt")
    quantity = caps.get("echm_af_ord_psbl_qty")
    if _empty(amount) != _empty(quantity):
        raise ValueError("KIS_OVERSEAS_AFTER_FX_PARTIAL_CAPACITY")
    if _empty(amount):
        amount = quantity = None
        status = "UNKNOWN_NOT_ZERO"
    else:
        amount, quantity = _reported_number(amount), _reported_number(quantity)
        status = "REPORTED_AFTER_FX_CAPACITY_NOT_FINANCIAL_AUTHORITY"
    foreign = caps.get("ord_psbl_frcr_amt")
    foreign = None if _empty(foreign) else _reported_number(foreign)
    return {
        **policy,
        "diagnosticSchemaVersion": DIAGNOSTIC_SCHEMA,
        "reportedCurrency": currency,
        "capacityStatus": status,
        "reportedAfterFxBuyingPower": amount,
        "reportedAfterFxMaximumBuyQuantity": quantity,
        "existingForeignCash": foreign,
        "existingForeignCashRole": "REPORTED_FOREIGN_ORDER_CAPACITY_NOT_SETTLED_CASH",
        "afterFxRole": "REPORTED_AFTER_EXCHANGE_CAPACITY_NOT_CASH_OR_ENROLLMENT",
        "privateValuesLocalOnly": True,
        "cashAndCostArithmeticPerformed": False,
        "amountsCombined": False,
        "capacityUsedForOrderSizing": False,
        "actualFeeVerified": False,
        "fxEventVerified": False,
    }
