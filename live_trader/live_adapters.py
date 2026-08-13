from __future__ import annotations

import hashlib
import hmac
import copy
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Literal


for parent in Path(__file__).resolve().parents:
    shared_runtime = parent / "packages" / "trading_runtime"
    if shared_runtime.exists():
        if str(shared_runtime) not in sys.path:
            sys.path.insert(0, str(shared_runtime))
        break

from trading_runtime.kis_rate_limiter import (
    GLOBAL_KIS_REST_LIMITERS,
    KisRestRateLimitError,
)
from .kis_order_authority import (
    KisOrderAuthorityError,
    consume_kis_read_transport_authority,
    consume_inherited_kis_transport_authority,
    require_inherited_kis_transport_authority,
    require_kis_read_transport_authority,
    require_kis_token_authority,
)


OrderSide = Literal["BUY", "SELL"]

KIS_LIVE_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_TOKEN_ENDPOINT = "/oauth2/tokenP"
KIS_DOMESTIC_ORDER_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-cash"
KIS_OVERSEAS_ORDER_ENDPOINT = "/uapi/overseas-stock/v1/trading/order"
KIS_DOMESTIC_CANCEL_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
KIS_OVERSEAS_CANCEL_ENDPOINT = "/uapi/overseas-stock/v1/trading/order-rvsecncl"
KIS_DOMESTIC_BALANCE_ENDPOINT = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_OVERSEAS_BALANCE_ENDPOINT = "/uapi/overseas-stock/v1/trading/inquire-balance"
KIS_OVERSEAS_PRICE_ENDPOINT = "/uapi/overseas-price/v1/quotations/price"
KIS_OVERSEAS_WORKING_ORDERS_ENDPOINT = (
    "/uapi/overseas-stock/v1/trading/inquire-nccs"
)
KIS_DOMESTIC_EXECUTION_ENDPOINT = (
    "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
)
KIS_DOMESTIC_TR_IDS = {"BUY": "TTTC0012U", "SELL": "TTTC0011U"}
KIS_OVERSEAS_TR_IDS = {"BUY": "TTTT1002U", "SELL": "TTTT1006U"}
KIS_DOMESTIC_BALANCE_TR_ID = "TTTC8434R"
KIS_OVERSEAS_BALANCE_TR_ID = "TTTS3012R"
KIS_OVERSEAS_PRICE_TR_ID = "HHDFS00000300"
KIS_OVERSEAS_WORKING_ORDERS_TR_ID = "TTTS3018R"
KIS_DOMESTIC_EXECUTION_TR_IDS = {
    # Official v1_국내주식-005, within the latest three months (inner).
    "real": "TTTC0081R",
    "demo": "VTTC0081R",
}
_KIS_TRADING_ENDPOINTS = frozenset(
    {
        KIS_DOMESTIC_ORDER_ENDPOINT,
        KIS_DOMESTIC_CANCEL_ENDPOINT,
        KIS_OVERSEAS_ORDER_ENDPOINT,
        KIS_OVERSEAS_CANCEL_ENDPOINT,
    }
)
_KIS_OWNED_ENDPOINTS = frozenset(
    {
        *_KIS_TRADING_ENDPOINTS,
        KIS_TOKEN_ENDPOINT,
        KIS_DOMESTIC_BALANCE_ENDPOINT,
        KIS_OVERSEAS_BALANCE_ENDPOINT,
        KIS_OVERSEAS_PRICE_ENDPOINT,
        KIS_OVERSEAS_WORKING_ORDERS_ENDPOINT,
        KIS_DOMESTIC_EXECUTION_ENDPOINT,
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/quotations/chk-holiday",
        "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
        "/uapi/domestic-stock/v1/trading/inquire-period-profit",
        "/uapi/domestic-stock/v1/trading/inquire-period-trade-profit",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
    }
)
_KIS_TRADING_TR_IDS = {
    KIS_DOMESTIC_ORDER_ENDPOINT: frozenset(KIS_DOMESTIC_TR_IDS.values()),
    KIS_OVERSEAS_ORDER_ENDPOINT: frozenset(KIS_OVERSEAS_TR_IDS.values()),
    KIS_DOMESTIC_CANCEL_ENDPOINT: frozenset({"TTTC0013U"}),
    KIS_OVERSEAS_CANCEL_ENDPOINT: frozenset({"TTTT1004U"}),
}
_KIS_READ_TR_IDS = {
    KIS_DOMESTIC_BALANCE_ENDPOINT: frozenset({KIS_DOMESTIC_BALANCE_TR_ID}),
    KIS_OVERSEAS_BALANCE_ENDPOINT: frozenset({KIS_OVERSEAS_BALANCE_TR_ID}),
    KIS_OVERSEAS_PRICE_ENDPOINT: frozenset({KIS_OVERSEAS_PRICE_TR_ID}),
    KIS_OVERSEAS_WORKING_ORDERS_ENDPOINT: frozenset(
        {KIS_OVERSEAS_WORKING_ORDERS_TR_ID}
    ),
    KIS_DOMESTIC_EXECUTION_ENDPOINT: frozenset({"TTTC0081R"}),
    "/uapi/domestic-stock/v1/quotations/inquire-price": frozenset(
        {"FHKST01010100"}
    ),
    "/uapi/domestic-stock/v1/quotations/chk-holiday": frozenset(
        {"CTCA0903R"}
    ),
    "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice": (
        frozenset({"FHKST03010230"})
    ),
    "/uapi/domestic-stock/v1/trading/inquire-period-profit": frozenset(
        {"TTTC8708R"}
    ),
    "/uapi/domestic-stock/v1/trading/inquire-period-trade-profit": (
        frozenset({"TTTC8715R"})
    ),
    "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl": frozenset(
        {"TTTC0084R"}
    ),
}
_KIS_TRADING_HEADER_KEYS = frozenset(
    {
        "authorization",
        "appkey",
        "appsecret",
        "content-type",
        "custtype",
        "tr_id",
    }
)

BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_ORDER_ENDPOINT = "/api/v3/order"
BINANCE_TEST_ORDER_ENDPOINT = "/api/v3/order/test"
BINANCE_ACCOUNT_ENDPOINT = "/api/v3/account"
BINANCE_TIME_ENDPOINT = "/api/v3/time"
BINANCE_EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
BINANCE_FUTURES_ORDER_ENDPOINT = "/fapi/v1/order"
BINANCE_FUTURES_OPEN_ORDERS_ENDPOINT = "/fapi/v1/openOrders"
BINANCE_FUTURES_TEST_ORDER_ENDPOINT = "/fapi/v1/order/test"
BINANCE_FUTURES_ACCOUNT_ENDPOINT = "/fapi/v3/account"
BINANCE_FUTURES_ACCOUNT_CONFIG_ENDPOINT = "/fapi/v1/accountConfig"
BINANCE_FUTURES_POSITION_ENDPOINT = "/fapi/v3/positionRisk"
BINANCE_FUTURES_POSITION_MODE_ENDPOINT = "/fapi/v1/positionSide/dual"
BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT = "/fapi/v1/symbolConfig"
BINANCE_FUTURES_LEVERAGE_ENDPOINT = "/fapi/v1/leverage"
BINANCE_FUTURES_MARGIN_TYPE_ENDPOINT = "/fapi/v1/marginType"
BINANCE_FUTURES_TIME_ENDPOINT = "/fapi/v1/time"
BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"
BINANCE_FUTURES_PREMIUM_INDEX_ENDPOINT = "/fapi/v1/premiumIndex"
BINANCE_FUTURES_LEVERAGE_BRACKET_ENDPOINT = "/fapi/v1/leverageBracket"
BINANCE_FUTURES_COMMISSION_RATE_ENDPOINT = "/fapi/v1/commissionRate"

UPBIT_BASE_URL = "https://api.upbit.com"
UPBIT_ORDER_ENDPOINT = "/v1/orders"
UPBIT_ACCOUNTS_ENDPOINT = "/v1/accounts"
UPBIT_ORDER_CHANCE_ENDPOINT = "/v1/orders/chance"
UPBIT_ORDER_DETAIL_ENDPOINT = "/v1/order"


def _official_upbit_mutation_base_url() -> str:
    """Return the only origin allowed to receive an ordinary Upbit JWT."""

    configured = str(
        env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL
    ).strip()
    if configured != UPBIT_BASE_URL:
        raise RuntimeError(
            "ordinary Upbit mutation origin must be exact official production URL"
        )
    return UPBIT_BASE_URL


def _guard_ordinary_upbit_mutation_edge(prepared: "PreparedRequest") -> None:
    """Recheck provider/method/endpoint/origin before the physical attempt."""

    if prepared.provider.strip().lower() != "upbit":
        return
    method = prepared.method.strip().upper()
    if method not in {"POST", "DELETE"}:
        return
    expected_endpoint = (
        UPBIT_ORDER_ENDPOINT if method == "POST" else UPBIT_ORDER_DETAIL_ENDPOINT
    )
    try:
        parsed = urllib.parse.urlsplit(prepared.url)
    except ValueError as exc:
        raise RuntimeError("ordinary Upbit mutation URL is invalid") from exc
    if (
        prepared.endpoint != expected_endpoint
        or parsed.scheme != "https"
        or parsed.netloc != "api.upbit.com"
        or parsed.hostname != "api.upbit.com"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != expected_endpoint
        or parsed.fragment
        or (method == "POST" and parsed.query)
    ):
        raise RuntimeError(
            "ordinary Upbit mutation URL must match the exact official endpoint"
        )

_KIS_TOKEN_CACHE: dict[str, object] = {"key": "", "token": "", "expires_at": 0.0}
_KIS_TOKEN_LOCK = threading.Lock()
_KIS_REQUEST_LOCK = threading.Lock()
_KIS_HTTP_DISPATCH_LOCK = threading.Lock()
_KIS_HTTP_DISPATCH_LOCAL = threading.local()
_KIS_REQUEST_LAST_MONOTONIC = 0.0
_BINANCE_TIME_CACHE: dict[str, object] = {"base_url": "", "offset_ms": 0, "expires_at": 0.0}
_BINANCE_TIME_LOCK = threading.Lock()
_BINANCE_SYMBOL_RULE_CACHE: dict[str, tuple[float, dict[str, Decimal]]] = {}
_BINANCE_SYMBOL_RULE_LOCK = threading.Lock()


@dataclass(frozen=True)
class PreparedRequest:
    provider: str
    method: str
    url: str
    endpoint: str
    headers: dict[str, str]
    safe_headers: dict[str, object]
    body: dict[str, object] | None
    query: dict[str, object] | None
    blocked_reasons: list[str]

    @property
    def can_send(self) -> bool:
        return not self.blocked_reasons

    def preview(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "method": self.method,
            "url": self.url,
            "endpoint": self.endpoint,
            "headers": self.safe_headers,
            "body": self.body or {},
            "query": self.query or {},
            "blocked_reasons": list(self.blocked_reasons),
            "can_send": self.can_send,
        }


@dataclass
class _KisHttpDispatch:
    """Private, thread-bound, single-call ticket installed after KIS pacing."""

    owner_thread_id: int
    request_hash: str
    kind: str
    nonce: str
    consumed: bool = False


def env_value(name: str) -> str:
    return os.getenv(name, "").strip()


def binance_timestamp_ms() -> int:
    with _BINANCE_TIME_LOCK:
        offset_ms = int(_BINANCE_TIME_CACHE.get("offset_ms") or 0)
    return int(time.time() * 1000) + offset_ms


def refresh_binance_time_offset(
    *,
    timeout_seconds: float = 5.0,
    futures: bool = False,
) -> int:
    base_url = (
        env_value("BINANCE_FUTURES_BASE_URL") or BINANCE_FUTURES_BASE_URL
        if futures
        else env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    )
    time_endpoint = (
        BINANCE_FUTURES_TIME_ENDPOINT
        if futures
        else BINANCE_TIME_ENDPOINT
    )
    now = time.monotonic()
    with _BINANCE_TIME_LOCK:
        if (
            _BINANCE_TIME_CACHE.get("base_url") == base_url
            and float(_BINANCE_TIME_CACHE.get("expires_at") or 0.0) > now
        ):
            return int(_BINANCE_TIME_CACHE.get("offset_ms") or 0)

    started_ms = int(time.time() * 1000)
    response = http_json(
        "GET",
        f"{base_url.rstrip('/')}{time_endpoint}",
        body=None,
        headers={},
        timeout_seconds=timeout_seconds,
    )
    finished_ms = int(time.time() * 1000)
    payload = response.get("json") if isinstance(response.get("json"), dict) else {}
    server_time = int(payload.get("serverTime") or 0)
    if response.get("ok") is not True or server_time <= 0:
        raise RuntimeError(str(response.get("text") or "Binance server time query failed."))
    offset_ms = server_time - ((started_ms + finished_ms) // 2)
    with _BINANCE_TIME_LOCK:
        _BINANCE_TIME_CACHE.update(
            {
                "base_url": base_url,
                "offset_ms": offset_ms,
                "expires_at": time.monotonic() + 300.0,
            }
        )
    return offset_ms


def _clear_binance_time_offset_cache() -> None:
    with _BINANCE_TIME_LOCK:
        _BINANCE_TIME_CACHE.update({"base_url": "", "offset_ms": 0, "expires_at": 0.0})


def missing_env(*names: str) -> list[str]:
    return [name for name in names if not env_value(name)]


def binance_symbol_rules(
    symbol: str,
    *,
    timeout_seconds: float = 5.0,
    futures: bool = False,
) -> dict[str, Decimal]:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    if not normalized_symbol:
        raise RuntimeError("Binance symbol이 비어 있습니다.")
    now = time.monotonic()
    with _BINANCE_SYMBOL_RULE_LOCK:
        cache_key = f"{'futures' if futures else 'spot'}:{normalized_symbol}"
        cached = _BINANCE_SYMBOL_RULE_CACHE.get(cache_key)
        if cached and cached[0] > now:
            return dict(cached[1])
    base_url = (
        env_value("BINANCE_FUTURES_BASE_URL") or BINANCE_FUTURES_BASE_URL
        if futures
        else env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    )
    exchange_info_endpoint = (
        BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT
        if futures
        else BINANCE_EXCHANGE_INFO_ENDPOINT
    )
    url = (
        f"{base_url.rstrip('/')}{exchange_info_endpoint}?"
        + urllib.parse.urlencode({"symbol": normalized_symbol})
    )
    response = http_json("GET", url, body=None, headers={}, timeout_seconds=timeout_seconds)
    payload = response.get("json") if isinstance(response.get("json"), dict) else {}
    symbols = payload.get("symbols") if isinstance(payload.get("symbols"), list) else []
    if response.get("ok") is not True or not symbols or not isinstance(symbols[0], dict):
        raise RuntimeError(str(response.get("text") or f"Binance {normalized_symbol} 거래 규칙 조회 실패"))
    filters = symbols[0].get("filters") if isinstance(symbols[0].get("filters"), list) else []
    by_type = {
        str(item.get("filterType") or ""): item
        for item in filters
        if isinstance(item, dict)
    }
    lot = by_type.get("MARKET_LOT_SIZE") or by_type.get("LOT_SIZE") or {}
    if Decimal(str(lot.get("stepSize") or "0")) <= 0:
        lot = by_type.get("LOT_SIZE") or lot
    notional = by_type.get("NOTIONAL") or by_type.get("MIN_NOTIONAL") or {}
    rules = {
        "minQty": Decimal(str(lot.get("minQty") or "0")),
        "maxQty": Decimal(str(lot.get("maxQty") or "0")),
        "stepSize": Decimal(str(lot.get("stepSize") or "0")),
        "minNotional": Decimal(str(notional.get("minNotional") or "0")),
    }
    with _BINANCE_SYMBOL_RULE_LOCK:
        _BINANCE_SYMBOL_RULE_CACHE[cache_key] = (now + 1800.0, rules)
    return dict(rules)


def normalize_binance_spot_intent(intent: dict[str, object]) -> dict[str, object]:
    normalized = dict(intent)
    symbol = str(normalized.get("symbol") or "").strip().upper()
    side = normalize_side(normalized.get("side"))
    order_type = str(normalized.get("order_type") or "MARKET").strip().upper()
    rules = binance_symbol_rules(symbol)
    notional = _decimal_or_zero(normalized.get("notional"))
    if order_type == "MARKET" and side == "BUY" and notional > 0:
        if rules["minNotional"] > 0 and notional < rules["minNotional"]:
            raise RuntimeError(
                f"Binance {symbol} 최소 주문금액 {rules['minNotional']} USDT 미만입니다."
            )
        normalized["quote_order_qty"] = normalize_decimal_text(notional)
        return normalized
    quantity = _decimal_or_zero(normalized.get("quantity") or normalized.get("qty"))
    step = rules["stepSize"]
    if step > 0:
        quantity = (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
    if quantity <= 0 or (rules["minQty"] > 0 and quantity < rules["minQty"]):
        raise RuntimeError(f"Binance {symbol} 주문 수량이 최소 수량보다 작습니다.")
    if rules["maxQty"] > 0 and quantity > rules["maxQty"]:
        raise RuntimeError(f"Binance {symbol} 주문 수량이 최대 수량을 초과합니다.")
    reference_price = _decimal_or_zero(normalized.get("price"))
    if (
        rules["minNotional"] > 0
        and reference_price > 0
        and quantity * reference_price < rules["minNotional"]
    ):
        raise RuntimeError(
            f"Binance {symbol} 최소 주문금액 {rules['minNotional']} USDT 미만입니다."
        )
    normalized["quantity"] = normalize_decimal_text(quantity)
    normalized["qty"] = normalized["quantity"]
    return normalized


def normalize_binance_futures_intent(
    intent: dict[str, object],
) -> dict[str, object]:
    normalized = dict(intent)
    symbol = (
        str(normalized.get("symbol") or "")
        .strip()
        .upper()
        .removesuffix(".PERP")
        .replace("-", "")
    )
    side = normalize_side(normalized.get("side"))
    order_type = str(
        normalized.get("order_type") or "MARKET"
    ).strip().upper()
    if order_type not in {"MARKET", "LIMIT"}:
        raise RuntimeError(
            "Binance Futures 자동 주문은 MARKET/LIMIT만 허용합니다."
        )
    rules = binance_symbol_rules(symbol, futures=True)
    quantity = _decimal_or_zero(
        normalized.get("quantity") or normalized.get("qty")
    )
    step = rules["stepSize"]
    if step > 0:
        quantity = (
            (quantity / step).to_integral_value(rounding=ROUND_DOWN)
            * step
        )
    if quantity <= 0 or (
        rules["minQty"] > 0 and quantity < rules["minQty"]
    ):
        raise RuntimeError(
            f"Binance Futures {symbol} 주문 수량이 최소 수량보다 작습니다."
        )
    if rules["maxQty"] > 0 and quantity > rules["maxQty"]:
        raise RuntimeError(
            f"Binance Futures {symbol} 주문 수량이 최대 수량을 초과합니다."
        )
    reference_price = _decimal_or_zero(normalized.get("price"))
    if (
        rules["minNotional"] > 0
        and reference_price > 0
        and quantity * reference_price < rules["minNotional"]
    ):
        raise RuntimeError(
            f"Binance Futures {symbol} 최소 주문금액 "
            f"{rules['minNotional']} USDT 미만입니다."
        )
    position_direction = (
        "SHORT"
        if str(
            normalized.get("position_direction")
            or normalized.get("positionDirection")
            or ""
        ).strip().lower() == "short"
        else "LONG"
    )
    normalized.update(
        {
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "quantity": normalize_decimal_text(quantity),
            "qty": normalize_decimal_text(quantity),
            "position_direction": position_direction.lower(),
            "risk_reducing": normalized.get("risk_reducing") is True
            or normalized.get("reduce_only") is True,
        }
    )
    return normalized


def _decimal_or_zero(value: object) -> Decimal:
    try:
        number = Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")
    return number if number.is_finite() else Decimal("0")


def split_kis_account(account_no: str, product_code: str) -> tuple[str, str]:
    text = account_no.strip().replace(" ", "")
    product = product_code.strip() or "01"
    if "-" in text:
        cano, suffix = text.split("-", 1)
        return cano.strip(), suffix.strip() or product
    if len(text) > 8:
        return text[:8], text[8:10] or product
    return text, product


def normalize_kis_market(symbol: str, asset_class: str = "") -> str:
    text = f"{symbol} {asset_class}".upper()
    if ".KS" in text or ".KQ" in text or "KR" in text or "한국" in text:
        return "KR"
    return "US"


def build_kis_live_order_request(intent: dict[str, object], *, access_token: str = "") -> PreparedRequest:
    required = ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE")
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    account_no = env_value("KIS_ACCOUNT_NO")
    product_code = env_value("KIS_ACCOUNT_PRODUCT_CODE")
    base_url = env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    symbol = str(intent.get("symbol") or "").strip().upper()
    side = normalize_side(intent.get("side"))
    qty = normalize_quantity(intent.get("quantity") or intent.get("qty") or 0)
    price = normalize_price(intent.get("price") or 0)
    market = normalize_kis_market(symbol, str(intent.get("asset") or intent.get("asset_class") or ""))
    cano, acnt_prdt_cd = split_kis_account(account_no, product_code)

    if not symbol:
        blocked.append("symbol")
    if qty <= 0:
        blocked.append("quantity")
    if price < 0:
        blocked.append("price")

    if market == "KR":
        endpoint = KIS_DOMESTIC_ORDER_ENDPOINT
        tr_id = KIS_DOMESTIC_TR_IDS[side]
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "PDNO": symbol.removesuffix(".KS").removesuffix(".KQ"),
            "ORD_DVSN": str(intent.get("order_type") or "00"),
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": str(int(round(price))),
        }
    else:
        endpoint = KIS_OVERSEAS_ORDER_ENDPOINT
        tr_id = KIS_OVERSEAS_TR_IDS[side]
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "OVRS_EXCG_CD": str(intent.get("exchange") or infer_us_exchange(symbol)),
            "PDNO": symbol,
            "ORD_QTY": str(int(qty)),
            "OVRS_ORD_UNPR": f"{price:.4f}".rstrip("0").rstrip("."),
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": str(intent.get("order_type") or "00"),
        }

    headers = {
        "content-type": "application/json; charset=utf-8",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
        "custtype": "P",
    }
    if access_token:
        headers["authorization"] = f"Bearer {access_token}"
    return PreparedRequest(
        provider="kis",
        method="POST",
        url=base_url.rstrip("/") + endpoint,
        endpoint=endpoint,
        headers=headers,
        safe_headers={
            "content-type": headers["content-type"],
            "appkey_configured": bool(app_key),
            "appsecret_configured": bool(app_secret),
            "authorization_configured": bool(access_token),
            "tr_id": tr_id,
            "custtype": "P",
        },
        body=body,
        query=None,
        blocked_reasons=blocked,
    )


def build_kis_domestic_balance_request(
    *,
    access_token: str = "",
    context_fk100: str = "",
    context_nk100: str = "",
    continuation: str = "",
) -> PreparedRequest:
    """Build one page of the official KIS domestic balance request."""

    required = ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE")
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    account_no = env_value("KIS_ACCOUNT_NO")
    product_code = env_value("KIS_ACCOUNT_PRODUCT_CODE")
    base_url = env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    cano, acnt_prdt_cd = split_kis_account(account_no, product_code)
    normalized_continuation = str(continuation or "").strip().upper()
    if not access_token:
        blocked.append("access_token")
    if normalized_continuation not in {"", "N"}:
        blocked.append("continuation")
    query: dict[str, object] = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": str(context_fk100 or ""),
        "CTX_AREA_NK100": str(context_nk100 or ""),
    }
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}" if access_token else "",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": KIS_DOMESTIC_BALANCE_TR_ID,
        "custtype": "P",
    }
    if normalized_continuation:
        headers["tr_cont"] = normalized_continuation
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="kis",
        method="GET",
        url=f"{base_url.rstrip('/')}{KIS_DOMESTIC_BALANCE_ENDPOINT}?{encoded}",
        endpoint=KIS_DOMESTIC_BALANCE_ENDPOINT,
        headers=headers,
        safe_headers={
            "content-type": headers["content-type"],
            "authorization_configured": bool(access_token),
            "appkey_configured": bool(app_key),
            "appsecret_configured": bool(app_secret),
            "tr_id": KIS_DOMESTIC_BALANCE_TR_ID,
            "tr_cont": normalized_continuation,
            "custtype": "P",
        },
        body=None,
        query=query,
        blocked_reasons=blocked,
    )


def build_kis_overseas_balance_request(
    *,
    access_token: str = "",
    exchange: str = "NASD",
    currency: str = "USD",
    context_fk200: str = "",
    context_nk200: str = "",
    continuation: str = "",
) -> PreparedRequest:
    """Build the official read-only KIS live overseas balance request.

    ``NASD`` means all US markets for a live account.  Continuation keys are
    explicit so callers can prove that the position snapshot is complete
    before treating an absent symbol as a zero balance.
    """

    required = ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE")
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    account_no = env_value("KIS_ACCOUNT_NO")
    product_code = env_value("KIS_ACCOUNT_PRODUCT_CODE")
    base_url = env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    cano, acnt_prdt_cd = split_kis_account(account_no, product_code)
    normalized_exchange = str(exchange or "").strip().upper()
    normalized_currency = str(currency or "").strip().upper()
    normalized_continuation = str(continuation or "").strip().upper()
    if not access_token:
        blocked.append("access_token")
    if not normalized_exchange:
        blocked.append("exchange")
    if not normalized_currency:
        blocked.append("currency")
    if normalized_continuation not in {"", "N"}:
        blocked.append("continuation")
    query: dict[str, object] = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "OVRS_EXCG_CD": normalized_exchange,
        "TR_CRCY_CD": normalized_currency,
        "CTX_AREA_FK200": str(context_fk200 or ""),
        "CTX_AREA_NK200": str(context_nk200 or ""),
    }
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}" if access_token else "",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": KIS_OVERSEAS_BALANCE_TR_ID,
        "custtype": "P",
    }
    if normalized_continuation:
        headers["tr_cont"] = normalized_continuation
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="kis",
        method="GET",
        url=f"{base_url.rstrip('/')}{KIS_OVERSEAS_BALANCE_ENDPOINT}?{encoded}",
        endpoint=KIS_OVERSEAS_BALANCE_ENDPOINT,
        headers=headers,
        safe_headers={
            "content-type": headers["content-type"],
            "authorization_configured": bool(access_token),
            "appkey_configured": bool(app_key),
            "appsecret_configured": bool(app_secret),
            "tr_id": KIS_OVERSEAS_BALANCE_TR_ID,
            "tr_cont": normalized_continuation,
            "custtype": "P",
        },
        body=None,
        query=query,
        blocked_reasons=blocked,
    )


def build_kis_us_live_quote_request(
    *,
    access_token: str = "",
    symbol: str = "F",
    exchange: str = "NYSE",
) -> PreparedRequest:
    """Build the exact read-only KIS US live-test quote request.

    KIS' official overseas-price API uses a three-letter quote exchange code,
    while orders use the four-letter account exchange code.  This functional
    path deliberately supports only ``F`` on ``NYSE`` and therefore always
    sends ``EXCD=NYS``.  It never builds an order-capable request.
    """

    required = (
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIS_ACCOUNT_NO",
        "KIS_ACCOUNT_PRODUCT_CODE",
    )
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    base_url = env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    environment = env_value("KIS_ENV").strip().lower() or "real"
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_exchange = str(exchange or "").strip().upper()

    if not access_token:
        blocked.append("access_token")
    if environment not in {"real", "live"}:
        blocked.append("kis_live_environment")
    if normalized_symbol != "F":
        blocked.append("exact_symbol_f")
    if normalized_exchange != "NYSE":
        blocked.append("exact_exchange_nyse")

    query: dict[str, object] = {
        "AUTH": "",
        "EXCD": "NYS",
        "SYMB": "F",
    }
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}" if access_token else "",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": KIS_OVERSEAS_PRICE_TR_ID,
        "custtype": "P",
    }
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="kis",
        method="GET",
        url=(
            f"{base_url.rstrip('/')}{KIS_OVERSEAS_PRICE_ENDPOINT}?{encoded}"
        ),
        endpoint=KIS_OVERSEAS_PRICE_ENDPOINT,
        headers=headers,
        safe_headers={
            "content-type": headers["content-type"],
            "authorization_configured": bool(access_token),
            "appkey_configured": bool(app_key),
            "appsecret_configured": bool(app_secret),
            "tr_id": KIS_OVERSEAS_PRICE_TR_ID,
            "custtype": "P",
        },
        body=None,
        query=query,
        blocked_reasons=list(dict.fromkeys(blocked)),
    )


def build_kis_overseas_working_orders_request(
    *,
    access_token: str = "",
    context_fk200: str = "",
    context_nk200: str = "",
    continuation: str = "",
) -> PreparedRequest:
    """Build one official, account-wide US live ``inquire-nccs`` page.

    The KIS contract defines ``NASD`` as all US markets (NASDAQ, NYSE and
    AMEX). Unlike a date-filtered execution-history query, ``TTTS3018R`` has
    no date filter and directly returns the account's currently working orders.
    Pagination is carried by the opaque FK/NK200 pair returned by KIS.
    """

    required = (
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIS_ACCOUNT_NO",
        "KIS_ACCOUNT_PRODUCT_CODE",
    )
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    account_no = env_value("KIS_ACCOUNT_NO")
    product_code = env_value("KIS_ACCOUNT_PRODUCT_CODE")
    base_url = env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    environment = env_value("KIS_ENV").strip().lower() or "real"
    cano, acnt_prdt_cd = split_kis_account(account_no, product_code)
    normalized_continuation = str(continuation or "").strip().upper()

    if not access_token:
        blocked.append("access_token")
    if environment not in {"real", "live"}:
        blocked.append("kis_live_environment")
    if normalized_continuation not in {"", "N"}:
        blocked.append("continuation")

    query: dict[str, object] = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "OVRS_EXCG_CD": "NASD",
        "SORT_SQN": "DS",
        "CTX_AREA_FK200": str(context_fk200 or ""),
        "CTX_AREA_NK200": str(context_nk200 or ""),
    }
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}" if access_token else "",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": KIS_OVERSEAS_WORKING_ORDERS_TR_ID,
        "custtype": "P",
    }
    if normalized_continuation:
        headers["tr_cont"] = normalized_continuation
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="kis",
        method="GET",
        url=(
            f"{base_url.rstrip('/')}{KIS_OVERSEAS_WORKING_ORDERS_ENDPOINT}?"
            f"{encoded}"
        ),
        endpoint=KIS_OVERSEAS_WORKING_ORDERS_ENDPOINT,
        headers=headers,
        safe_headers={
            "content-type": headers["content-type"],
            "authorization_configured": bool(access_token),
            "appkey_configured": bool(app_key),
            "appsecret_configured": bool(app_secret),
            "tr_id": KIS_OVERSEAS_WORKING_ORDERS_TR_ID,
            "tr_cont": normalized_continuation,
            "custtype": "P",
        },
        body=None,
        query=query,
        blocked_reasons=list(dict.fromkeys(blocked)),
    )


def build_kis_domestic_execution_request(
    *,
    access_token: str = "",
    start_date: str,
    end_date: str,
    context_fk100: str = "",
    context_nk100: str = "",
    continuation: str = "",
) -> PreparedRequest:
    """Build one official domestic order/execution-history page.

    This is a read-only truth query.  It deliberately requests both filled
    and unfilled orders and leaves ODNO/PDNO blank so a lost POST response is
    still observable as an *unmatched* official broker order.  Correlation is
    performed later only by the official ODNO; this builder never guesses a
    local order from symbol, quantity, or time proximity.
    """

    required = (
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIS_ACCOUNT_NO",
        "KIS_ACCOUNT_PRODUCT_CODE",
    )
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    account_no = env_value("KIS_ACCOUNT_NO")
    product_code = env_value("KIS_ACCOUNT_PRODUCT_CODE")
    environment_text = (env_value("KIS_ENV") or "real").lower()
    if environment_text in {"real", "live", "prod", "production"}:
        environment = "real"
    elif environment_text in {"demo", "paper", "virtual", "vts"}:
        environment = "demo"
    else:
        environment = ""
        blocked.append("KIS_ENV")
    default_base_url = (
        "https://openapivts.koreainvestment.com:29443"
        if environment == "demo"
        else KIS_LIVE_BASE_URL
    )
    base_url = env_value("KIS_BASE_URL") or default_base_url
    cano, acnt_prdt_cd = split_kis_account(account_no, product_code)
    normalized_start = str(start_date or "").strip()
    normalized_end = str(end_date or "").strip()
    normalized_continuation = str(continuation or "").strip().upper()
    if not access_token:
        blocked.append("access_token")
    if (
        len(normalized_start) != 8
        or not normalized_start.isdigit()
    ):
        blocked.append("start_date")
    else:
        try:
            time.strptime(normalized_start, "%Y%m%d")
        except ValueError:
            blocked.append("start_date")
    if len(normalized_end) != 8 or not normalized_end.isdigit():
        blocked.append("end_date")
    else:
        try:
            time.strptime(normalized_end, "%Y%m%d")
        except ValueError:
            blocked.append("end_date")
    if (
        normalized_start
        and normalized_end
        and normalized_start > normalized_end
    ):
        blocked.append("date_range")
    if normalized_continuation not in {"", "N"}:
        blocked.append("continuation")
    query: dict[str, object] = {
        "CANO": cano,
        "ACNT_PRDT_CD": acnt_prdt_cd,
        "INQR_STRT_DT": normalized_start,
        "INQR_END_DT": normalized_end,
        "SLL_BUY_DVSN_CD": "00",
        "INQR_DVSN": "00",
        "PDNO": "",
        "CCLD_DVSN": "00",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "",
        # Account truth must include KRX/NXT/SOR instead of silently omitting
        # orders that the broker routed away from the legacy KRX venue.
        "EXCG_ID_DVSN_CD": "ALL",
        "CTX_AREA_FK100": str(context_fk100 or ""),
        "CTX_AREA_NK100": str(context_nk100 or ""),
    }
    tr_id = KIS_DOMESTIC_EXECUTION_TR_IDS.get(environment, "")
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}" if access_token else "",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
        "custtype": "P",
    }
    if normalized_continuation:
        headers["tr_cont"] = normalized_continuation
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="kis",
        method="GET",
        url=(
            f"{base_url.rstrip('/')}{KIS_DOMESTIC_EXECUTION_ENDPOINT}"
            f"?{encoded}"
        ),
        endpoint=KIS_DOMESTIC_EXECUTION_ENDPOINT,
        headers=headers,
        safe_headers={
            "content-type": headers["content-type"],
            "authorization_configured": bool(access_token),
            "appkey_configured": bool(app_key),
            "appsecret_configured": bool(app_secret),
            "tr_id": tr_id,
            "tr_cont": normalized_continuation,
            "custtype": "P",
            "environment": environment,
        },
        body=None,
        query=query,
        blocked_reasons=blocked,
    )


def build_kis_cancel_order_request(order: dict[str, object], *, access_token: str = "") -> PreparedRequest:
    """Build an official KIS domestic/overseas full-cancel request.

    KIS cancellation needs original-order metadata in addition to the order
    number. Missing fields remain explicit blocked reasons instead of guessing.
    """
    required = ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE")
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    base_url = env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    cano, acnt_prdt_cd = split_kis_account(env_value("KIS_ACCOUNT_NO"), env_value("KIS_ACCOUNT_PRODUCT_CODE"))
    symbol = str(order.get("symbol") or "").strip().upper()
    broker_order_id = str(order.get("broker_order_id") or order.get("order_id") or "").strip()
    quantity = normalize_quantity(order.get("quantity") or order.get("qty") or 0)
    market = normalize_kis_market(symbol, str(order.get("asset") or order.get("asset_class") or ""))
    env_dv = str(order.get("environment") or env_value("KIS_ENV") or "real").strip().lower()

    if not access_token:
        blocked.append("access_token")
    if not symbol:
        blocked.append("symbol")
    if not broker_order_id:
        blocked.append("broker_order_id")
    if quantity <= 0:
        blocked.append("quantity")
    if env_dv not in {"real", "demo"}:
        blocked.append("environment")

    if market == "KR":
        organization_no = str(order.get("organization_no") or order.get("krx_fwdg_ord_orgno") or "").strip()
        if not organization_no:
            blocked.append("organization_no")
        endpoint = KIS_DOMESTIC_CANCEL_ENDPOINT
        tr_id = "VTTC0013U" if env_dv == "demo" else "TTTC0013U"
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "KRX_FWDG_ORD_ORGNO": organization_no,
            "ORGN_ODNO": broker_order_id,
            "ORD_DVSN": str(order.get("order_type") or "00"),
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": str(int(quantity)),
            "ORD_UNPR": "0",
            "QTY_ALL_ORD_YN": "Y",
            "EXCG_ID_DVSN_CD": str(order.get("exchange") or "KRX"),
        }
    else:
        endpoint = KIS_OVERSEAS_CANCEL_ENDPOINT
        tr_id = "VTTT1004U" if env_dv == "demo" else "TTTT1004U"
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "OVRS_EXCG_CD": str(order.get("exchange") or infer_us_exchange(symbol)),
            "PDNO": symbol,
            "ORGN_ODNO": broker_order_id,
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": str(int(quantity)),
            "OVRS_ORD_UNPR": "0",
            "MGCO_APTM_ODNO": str(order.get("manager_order_id") or ""),
            "ORD_SVR_DVSN_CD": "0",
        }

    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}" if access_token else "",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
        "custtype": "P",
    }
    return PreparedRequest(
        provider="kis",
        method="POST",
        url=base_url.rstrip("/") + endpoint,
        endpoint=endpoint,
        headers=headers,
        safe_headers={
            "content-type": headers["content-type"],
            "authorization_configured": bool(access_token),
            "appkey_configured": bool(app_key),
            "appsecret_configured": bool(app_secret),
            "tr_id": tr_id,
            "custtype": "P",
        },
        body=body,
        query=None,
        blocked_reasons=blocked,
    )


def build_binance_account_request() -> PreparedRequest:
    blocked = missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET")
    api_key = env_value("BINANCE_API_KEY")
    api_secret = env_value("BINANCE_API_SECRET")
    base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    query: dict[str, object] = {"timestamp": binance_timestamp_ms()}
    signed_query = sign_binance_query(query, api_secret) if api_secret else urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="binance",
        method="GET",
        url=f"{base_url.rstrip('/')}{BINANCE_ACCOUNT_ENDPOINT}?{signed_query}",
        endpoint=BINANCE_ACCOUNT_ENDPOINT,
        headers={"X-MBX-APIKEY": api_key},
        safe_headers={"X-MBX-APIKEY_configured": bool(api_key)},
        body=None,
        query={**query, "signature": "***" if api_secret else ""},
        blocked_reasons=blocked,
    )


def _build_binance_futures_signed_request(
    method: str,
    endpoint: str,
    query: dict[str, object] | None = None,
    *,
    provider: str = "binance-futures",
    blocked_reasons: list[str] | None = None,
) -> PreparedRequest:
    blocked = [
        *missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET"),
        *(blocked_reasons or []),
    ]
    api_key = env_value("BINANCE_API_KEY")
    api_secret = env_value("BINANCE_API_SECRET")
    base_url = (
        env_value("BINANCE_FUTURES_BASE_URL")
        or BINANCE_FUTURES_BASE_URL
    )
    request_query = {
        **(query or {}),
        "timestamp": binance_timestamp_ms(),
    }
    signed_query = (
        sign_binance_query(request_query, api_secret)
        if api_secret
        else urllib.parse.urlencode(request_query)
    )
    return PreparedRequest(
        provider=provider,
        method=method,
        url=f"{base_url.rstrip('/')}{endpoint}?{signed_query}",
        endpoint=endpoint,
        headers={"X-MBX-APIKEY": api_key},
        safe_headers={"X-MBX-APIKEY_configured": bool(api_key)},
        body=None,
        query={
            **request_query,
            "signature": "***" if api_secret else "",
        },
        blocked_reasons=list(dict.fromkeys(blocked)),
    )


def build_binance_futures_account_request() -> PreparedRequest:
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_ACCOUNT_ENDPOINT,
    )


def build_binance_futures_positions_request(
    symbol: str = "",
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_POSITION_ENDPOINT,
        {"symbol": normalized_symbol} if normalized_symbol else {},
    )


def build_binance_futures_account_config_request() -> PreparedRequest:
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_ACCOUNT_CONFIG_ENDPOINT,
    )


def build_binance_futures_open_orders_request(
    symbol: str = "",
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_OPEN_ORDERS_ENDPOINT,
        {"symbol": normalized_symbol} if normalized_symbol else {},
    )


def build_binance_futures_order_status_request(
    symbol: str,
    broker_order_id: str,
    *,
    client_order_id: bool = False,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    normalized_order_id = str(broker_order_id or "").strip()
    query: dict[str, object] = {"symbol": normalized_symbol}
    query[
        "origClientOrderId" if client_order_id else "orderId"
    ] = normalized_order_id
    blocked: list[str] = []
    if not normalized_symbol:
        blocked.append("symbol")
    if not normalized_order_id:
        blocked.append("broker_order_id")
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_ORDER_ENDPOINT,
        query,
        blocked_reasons=blocked,
    )


def build_binance_futures_position_mode_request() -> PreparedRequest:
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_POSITION_MODE_ENDPOINT,
    )


def build_binance_futures_symbol_config_request(
    symbol: str,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT,
        {"symbol": normalized_symbol},
        blocked_reasons=[] if normalized_symbol else ["symbol"],
    )


def build_binance_futures_premium_index_request(
    symbol: str,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    base_url = (
        env_value("BINANCE_FUTURES_BASE_URL")
        or BINANCE_FUTURES_BASE_URL
    )
    query = {"symbol": normalized_symbol}
    return PreparedRequest(
        provider="binance-futures",
        method="GET",
        url=(
            f"{base_url.rstrip('/')}{BINANCE_FUTURES_PREMIUM_INDEX_ENDPOINT}?"
            + urllib.parse.urlencode(query)
        ),
        endpoint=BINANCE_FUTURES_PREMIUM_INDEX_ENDPOINT,
        headers={},
        safe_headers={},
        body=None,
        query=query,
        blocked_reasons=[] if normalized_symbol else ["symbol"],
    )


def build_binance_futures_leverage_bracket_request(
    symbol: str,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_LEVERAGE_BRACKET_ENDPOINT,
        {"symbol": normalized_symbol},
        blocked_reasons=[] if normalized_symbol else ["symbol"],
    )


def build_binance_futures_commission_rate_request(
    symbol: str,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    return _build_binance_futures_signed_request(
        "GET",
        BINANCE_FUTURES_COMMISSION_RATE_ENDPOINT,
        {"symbol": normalized_symbol},
        blocked_reasons=[] if normalized_symbol else ["symbol"],
    )


def build_binance_futures_leverage_change_request(
    symbol: str,
    leverage: object,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    try:
        normalized_leverage = int(str(leverage).strip())
    except (TypeError, ValueError):
        normalized_leverage = 0
    blocked: list[str] = []
    if not normalized_symbol:
        blocked.append("symbol")
    if normalized_leverage < 1 or normalized_leverage > 125:
        blocked.append("leverage")
    return _build_binance_futures_signed_request(
        "POST",
        BINANCE_FUTURES_LEVERAGE_ENDPOINT,
        {
            "symbol": normalized_symbol,
            "leverage": normalized_leverage,
        },
        blocked_reasons=blocked,
    )


def build_binance_futures_margin_type_change_request(
    symbol: str,
    margin_type: object,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    normalized_margin_type = str(margin_type or "").strip().upper()
    blocked: list[str] = []
    if not normalized_symbol:
        blocked.append("symbol")
    if normalized_margin_type not in {"ISOLATED", "CROSSED"}:
        blocked.append("margin_type")
    return _build_binance_futures_signed_request(
        "POST",
        BINANCE_FUTURES_MARGIN_TYPE_ENDPOINT,
        {
            "symbol": normalized_symbol,
            "marginType": normalized_margin_type,
        },
        blocked_reasons=blocked,
    )


def build_binance_futures_order_request(
    intent: dict[str, object],
    *,
    hedge_mode: bool,
    test: bool = False,
) -> PreparedRequest:
    symbol = (
        str(intent.get("symbol") or "")
        .strip()
        .upper()
        .removesuffix(".PERP")
        .replace("-", "")
    )
    side = normalize_side(intent.get("side"))
    quantity = normalize_decimal_text(
        intent.get("quantity") or intent.get("qty") or 0
    )
    order_type = str(
        intent.get("order_type") or "MARKET"
    ).strip().upper()
    position_direction = (
        "SHORT"
        if str(
            intent.get("position_direction")
            or intent.get("positionDirection")
            or ""
        ).strip().lower() == "short"
        else "LONG"
    )
    query: dict[str, object] = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": quantity,
        "positionSide": position_direction if hedge_mode else "BOTH",
    }
    if not hedge_mode and (
        intent.get("risk_reducing") is True
        or intent.get("reduce_only") is True
    ):
        query["reduceOnly"] = "true"
    identifier = str(
        intent.get("identifier")
        or intent.get("new_client_order_id")
        or ""
    ).strip()
    if identifier:
        query["newClientOrderId"] = identifier[:36]
    if order_type == "LIMIT":
        query["timeInForce"] = str(
            intent.get("time_in_force") or "GTC"
        ).strip().upper()
        query["price"] = normalize_decimal_text(
            intent.get("price") or 0
        )
    blocked: list[str] = []
    if not symbol:
        blocked.append("symbol")
    if float(quantity or 0) <= 0:
        blocked.append("quantity")
    if order_type not in {"MARKET", "LIMIT"}:
        blocked.append("order_type")
    if order_type == "LIMIT" and float(query.get("price") or 0) <= 0:
        blocked.append("price")
    endpoint = (
        BINANCE_FUTURES_TEST_ORDER_ENDPOINT
        if test
        else BINANCE_FUTURES_ORDER_ENDPOINT
    )
    return _build_binance_futures_signed_request(
        "POST",
        endpoint,
        query,
        blocked_reasons=blocked,
    )


def build_binance_futures_cancel_order_request(
    symbol: str,
    broker_order_id: str,
    *,
    client_order_id: bool = False,
) -> PreparedRequest:
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    normalized_order_id = str(broker_order_id or "").strip()
    query: dict[str, object] = {"symbol": normalized_symbol}
    query[
        "origClientOrderId" if client_order_id else "orderId"
    ] = normalized_order_id
    blocked = []
    if not normalized_symbol:
        blocked.append("symbol")
    if not normalized_order_id:
        blocked.append("broker_order_id")
    return _build_binance_futures_signed_request(
        "DELETE",
        BINANCE_FUTURES_ORDER_ENDPOINT,
        query,
        blocked_reasons=blocked,
    )


def build_upbit_accounts_request() -> PreparedRequest:
    blocked = missing_env("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    base_url = env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL
    authorization = build_upbit_authorization(access_key, secret_key, {}) if access_key and secret_key else ""
    return PreparedRequest(
        provider="upbit",
        method="GET",
        url=base_url.rstrip("/") + UPBIT_ACCOUNTS_ENDPOINT,
        endpoint=UPBIT_ACCOUNTS_ENDPOINT,
        headers={"Authorization": authorization},
        safe_headers={"authorization_configured": bool(authorization)},
        body=None,
        query=None,
        blocked_reasons=blocked,
    )


def build_upbit_order_chance_request(market: str) -> PreparedRequest:
    blocked = missing_env("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    base_url = env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL
    normalized_market = str(market or "").strip().upper()
    query = {"market": normalized_market}
    if not normalized_market:
        blocked.append("market")
    authorization = build_upbit_authorization(access_key, secret_key, query) if access_key and secret_key else ""
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="upbit",
        method="GET",
        url=f"{base_url.rstrip('/')}{UPBIT_ORDER_CHANCE_ENDPOINT}?{encoded}",
        endpoint=UPBIT_ORDER_CHANCE_ENDPOINT,
        headers={"Authorization": authorization},
        safe_headers={"authorization_configured": bool(authorization)},
        body=None,
        query=query,
        blocked_reasons=blocked,
    )


def build_upbit_order_detail_request(order_uuid: str) -> PreparedRequest:
    blocked = missing_env("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    base_url = env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL
    normalized_uuid = str(order_uuid or "").strip()
    query = {"uuid": normalized_uuid}
    if not normalized_uuid:
        blocked.append("uuid")
    authorization = build_upbit_authorization(access_key, secret_key, query) if access_key and secret_key else ""
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="upbit",
        method="GET",
        url=f"{base_url.rstrip('/')}{UPBIT_ORDER_DETAIL_ENDPOINT}?{encoded}",
        endpoint=UPBIT_ORDER_DETAIL_ENDPOINT,
        headers={"Authorization": authorization},
        safe_headers={"authorization_configured": bool(authorization)},
        body=None,
        query=query,
        blocked_reasons=blocked,
    )


def build_binance_spot_order_request(intent: dict[str, object], *, test: bool = False) -> PreparedRequest:
    blocked = missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET")
    api_key = env_value("BINANCE_API_KEY")
    api_secret = env_value("BINANCE_API_SECRET")
    base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    symbol = str(intent.get("symbol") or "").strip().upper()
    side = normalize_side(intent.get("side"))
    quantity = normalize_decimal_text(intent.get("quantity") or intent.get("qty") or 0)
    quote_order_quantity = normalize_decimal_text(
        intent.get("quote_order_qty")
        or intent.get("quoteOrderQty")
        or (
            intent.get("notional")
            if side == "BUY" and str(intent.get("order_type") or "MARKET").strip().upper() == "MARKET"
            else 0
        )
    )
    order_type = str(intent.get("order_type") or "MARKET").strip().upper()
    query: dict[str, object] = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
    }
    uses_quote_order_quantity = (
        order_type == "MARKET"
        and side == "BUY"
        and float(quote_order_quantity or 0) > 0
    )
    if uses_quote_order_quantity:
        query["quoteOrderQty"] = quote_order_quantity
    else:
        query["quantity"] = quantity
    query["timestamp"] = binance_timestamp_ms()
    if order_type == "LIMIT":
        query["timeInForce"] = str(intent.get("time_in_force") or "GTC")
        query["price"] = normalize_decimal_text(intent.get("price") or 0)
    if not symbol:
        blocked.append("symbol")
    if not uses_quote_order_quantity and float(quantity or 0) <= 0:
        blocked.append("quantity")
    signed_query = sign_binance_query(query, api_secret) if api_secret else urllib.parse.urlencode(query)
    endpoint = BINANCE_TEST_ORDER_ENDPOINT if test else BINANCE_ORDER_ENDPOINT
    return PreparedRequest(
        provider="binance",
        method="POST",
        url=f"{base_url.rstrip('/')}{endpoint}?{signed_query}",
        endpoint=endpoint,
        headers={"X-MBX-APIKEY": api_key},
        safe_headers={"X-MBX-APIKEY_configured": bool(api_key)},
        body=None,
        query={**query, "signature": "***" if api_secret else ""},
        blocked_reasons=blocked,
    )


def build_binance_cancel_order_request(symbol: str, broker_order_id: str, *, client_order_id: bool = False) -> PreparedRequest:
    blocked = missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET")
    api_key = env_value("BINANCE_API_KEY")
    api_secret = env_value("BINANCE_API_SECRET")
    base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_order_id = str(broker_order_id or "").strip()
    query: dict[str, object] = {"symbol": normalized_symbol, "timestamp": binance_timestamp_ms()}
    query["origClientOrderId" if client_order_id else "orderId"] = normalized_order_id
    if not normalized_symbol:
        blocked.append("symbol")
    if not normalized_order_id:
        blocked.append("broker_order_id")
    signed_query = sign_binance_query(query, api_secret) if api_secret else urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="binance",
        method="DELETE",
        url=f"{base_url.rstrip('/')}{BINANCE_ORDER_ENDPOINT}?{signed_query}",
        endpoint=BINANCE_ORDER_ENDPOINT,
        headers={"X-MBX-APIKEY": api_key},
        safe_headers={"X-MBX-APIKEY_configured": bool(api_key)},
        body=None,
        query={**query, "signature": "***" if api_secret else ""},
        blocked_reasons=blocked,
    )


def build_upbit_order_request(intent: dict[str, object]) -> PreparedRequest:
    blocked = missing_env("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")
    base_url = _official_upbit_mutation_base_url()
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    market = str(intent.get("market") or intent.get("symbol") or "").strip().upper()
    side = "bid" if normalize_side(intent.get("side")) == "BUY" else "ask"
    ord_type = str(intent.get("order_type") or "limit").strip().lower()
    body: dict[str, object] = {"market": market, "side": side, "ord_type": ord_type}
    identifier = str(intent.get("identifier") or "").strip()
    if identifier:
        body["identifier"] = identifier
    if ord_type == "price":
        body["price"] = normalize_decimal_text(intent.get("price") or intent.get("notional") or 0)
    else:
        body["volume"] = normalize_decimal_text(intent.get("quantity") or intent.get("qty") or 0)
        if ord_type == "limit":
            body["price"] = normalize_decimal_text(intent.get("price") or 0)
    if not market:
        blocked.append("market")
    authorization = build_upbit_authorization(access_key, secret_key, body) if access_key and secret_key else ""
    return PreparedRequest(
        provider="upbit",
        method="POST",
        url=base_url.rstrip("/") + UPBIT_ORDER_ENDPOINT,
        endpoint=UPBIT_ORDER_ENDPOINT,
        headers={"Authorization": authorization, "Content-Type": "application/json"},
        safe_headers={"authorization_configured": bool(authorization), "content-type": "application/json"},
        body=body,
        query=None,
        blocked_reasons=blocked,
    )


def build_upbit_cancel_order_request(broker_order_id: str, *, identifier: bool = False) -> PreparedRequest:
    blocked = missing_env("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")
    base_url = _official_upbit_mutation_base_url()
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    normalized_order_id = str(broker_order_id or "").strip()
    query = {"identifier" if identifier else "uuid": normalized_order_id}
    if not normalized_order_id:
        blocked.append("broker_order_id")
    authorization = build_upbit_authorization(access_key, secret_key, query) if access_key and secret_key else ""
    encoded = urllib.parse.urlencode(query)
    return PreparedRequest(
        provider="upbit",
        method="DELETE",
        url=f"{base_url.rstrip('/')}{UPBIT_ORDER_DETAIL_ENDPOINT}?{encoded}",
        endpoint=UPBIT_ORDER_DETAIL_ENDPOINT,
        headers={"Authorization": authorization},
        safe_headers={"authorization_configured": bool(authorization)},
        body=None,
        query=query,
        blocked_reasons=blocked,
    )


def issue_kis_access_token(*, timeout_seconds: float = 10.0) -> str:
    # Cached bearer material is as sensitive as a new token response and must
    # never escape the same route-held authority required for tokenP itself.
    require_kis_token_authority()
    missing = missing_env("KIS_APP_KEY", "KIS_APP_SECRET")
    if missing:
        raise RuntimeError(f"KIS token settings missing: {', '.join(missing)}")
    base_url = (env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL).rstrip("/")
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    cache_key = hashlib.sha256(f"{base_url}\0{app_key}\0{app_secret}".encode("utf-8")).hexdigest()
    with _KIS_TOKEN_LOCK:
        now = time.monotonic()
        if (
            _KIS_TOKEN_CACHE.get("key") == cache_key
            and str(_KIS_TOKEN_CACHE.get("token") or "")
            and float(_KIS_TOKEN_CACHE.get("expires_at") or 0.0) > now
        ):
            return str(_KIS_TOKEN_CACHE["token"])

        response = _send_kis_http_json(
            "POST",
            base_url + KIS_TOKEN_ENDPOINT,
            body={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret,
            },
            headers={"content-type": "application/json; charset=utf-8"},
            timeout_seconds=timeout_seconds,
        )
        payload = response.get("json") if isinstance(response.get("json"), dict) else {}
        token = str(payload.get("access_token") or "").strip()
        if not token:
            raise RuntimeError(
                str(payload.get("msg1") or response.get("text") or "KIS token response did not include access_token.")
            )
        try:
            expires_in = max(1.0, float(payload.get("expires_in") or 86400.0))
        except (TypeError, ValueError):
            expires_in = 86400.0
        _KIS_TOKEN_CACHE.update(
            {"key": cache_key, "token": token, "expires_at": now + max(1.0, expires_in - 60.0)}
        )
        return token


def _clear_kis_access_token_cache() -> None:
    with _KIS_TOKEN_LOCK:
        _KIS_TOKEN_CACHE.update({"key": "", "token": "", "expires_at": 0.0})


def kis_request_min_interval_seconds() -> float:
    """Return the conservative spacing used for every KIS REST request.

    KIS rejects bursts with EGW00201/EGW00215.  Balance reconciliation needs
    both a domestic and an overseas request, so serializing only each helper is
    insufficient: every KIS REST call in this process shares this one pacer.
    The default intentionally favors a stable live monitor over a one-second
    faster account refresh.  Operators may raise, but not lower, the safety
    floor through the environment.
    """

    try:
        configured = float(
            os.environ.get("KIS_REQUEST_MIN_INTERVAL_SECONDS") or 2.1
        )
    except (TypeError, ValueError):
        configured = 2.1
    return max(2.0, configured)


def _kis_rate_limited(response: dict[str, object]) -> bool:
    payload = (
        response.get("json")
        if isinstance(response.get("json"), dict)
        else {}
    )
    code = str(payload.get("msg_cd") or payload.get("msgCode") or "").upper()
    message = str(
        payload.get("msg1")
        or payload.get("message")
        or response.get("text")
        or ""
    )
    return code in {"EGW00201", "EGW00215"} or (
        "초당" in message and "거래건수" in message
    )


def kis_read_rate_limit_retries() -> int:
    """Owned KIS reads are terminal one-shot attempts under an exact lease."""

    return 0


def _send_kis_http_json(
    method: str,
    url: str,
    *,
    body: dict[str, object] | None,
    headers: dict[str, str],
    timeout_seconds: float,
) -> dict[str, object]:
    """Serialize and pace KIS HTTP traffic, including token issuance."""

    frozen_body = copy.deepcopy(body)
    frozen_headers = copy.deepcopy(headers)
    kind = _guard_kis_owned_http_edge(
        method=method,
        url=url,
        body=frozen_body,
        headers=frozen_headers,
        kis_specific=True,
    )
    global _KIS_REQUEST_LAST_MONOTONIC
    with _KIS_REQUEST_LOCK:
        interval = kis_request_min_interval_seconds()
        elapsed = time.monotonic() - _KIS_REQUEST_LAST_MONOTONIC
        remaining = interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
        # An owned read grant is exact and one-use at the physical edge.
        # Rate-limit responses are terminal observations; callers may open a
        # fresh diagnostic boundary only after publication/reconciliation.
        retry_count = 0
        for attempt in range(retry_count + 1):
            _acquire_shared_kis_rest_slot(url)
            ticket = _install_kis_http_dispatch(
                kind=kind,
                method=method,
                url=url,
                body=frozen_body,
                headers=frozen_headers,
            )
            try:
                response = http_json(
                    method,
                    url,
                    body=frozen_body,
                    headers=frozen_headers,
                    timeout_seconds=timeout_seconds,
                )
            finally:
                if getattr(_KIS_HTTP_DISPATCH_LOCAL, "ticket", None) is ticket:
                    del _KIS_HTTP_DISPATCH_LOCAL.ticket
            _KIS_REQUEST_LAST_MONOTONIC = time.monotonic()
            if kind == "READ" and _kis_rate_limited(response):
                response = {
                    **response,
                    "physicalAttemptCount": 1,
                    "retryAllowed": False,
                    "terminalReadLease": True,
                }
            if not _kis_rate_limited(response) or attempt >= retry_count:
                return response
            # A read-only balance query is safe to repeat.  Orders and token
            # POSTs never enter this retry path because their result can be
            # ambiguous even when the client only sees a transport failure.
            time.sleep(max(2.1, interval * (2 ** attempt)))
        return response


def _acquire_shared_kis_rest_slot(url: str) -> float:
    """Reserve the correct fail-closed KIS slot immediately before I/O."""

    app_key = env_value("KIS_APP_KEY")
    if not app_key:
        raise KisRestRateLimitError(
            "KIS APP key identity is missing; REST request blocked"
        )
    app_key_id = "sha256:" + hashlib.sha256(app_key.encode("utf-8")).hexdigest()
    endpoint = urllib.parse.urlsplit(str(url or "")).path.rstrip("/")
    if endpoint == KIS_TOKEN_ENDPOINT.rstrip("/"):
        # KIS documents tokenP as a separate app-key-scoped 1 request/second
        # budget. It must not borrow capacity from the normal Live REST pool.
        return GLOBAL_KIS_REST_LIMITERS.get_token(app_key_id).acquire()

    account_material = "\0".join(
        (
            env_value("KIS_ACCOUNT_NO"),
            env_value("KIS_ACCOUNT_PRODUCT_CODE"),
        )
    ).strip("\0")
    if not account_material:
        raise KisRestRateLimitError(
            "KIS account identity is missing; REST request blocked"
        )
    account_id = (
        "sha256:"
        + hashlib.sha256(account_material.encode("utf-8")).hexdigest()
    )
    environment_text = (env_value("KIS_ENV") or "real").lower()
    limiter_mode = (
        "VPS"
        if environment_text in {"demo", "paper", "virtual", "vts"}
        else "PROD"
    )
    return GLOBAL_KIS_REST_LIMITERS.get(
        account_id,
        app_key_id,
        limiter_mode,
    ).acquire()


def _reset_kis_request_pacer() -> None:
    """Reset process-local pacing state for isolated tests."""

    global _KIS_REQUEST_LAST_MONOTONIC
    with _KIS_REQUEST_LOCK:
        _KIS_REQUEST_LAST_MONOTONIC = 0.0


def _canonical_kis_origin(value: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(str(value or "").rstrip("/"))
        port = parsed.port
    except ValueError as exc:
        raise KisOrderAuthorityError("KIS URL origin is invalid") from exc
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise KisOrderAuthorityError("KIS URL origin is invalid")
    hostname = parsed.hostname.lower()
    effective_port = 443 if port is None else port
    return f"https://{hostname}:{effective_port}"


def _kis_actual_url(value: str) -> dict[str, object]:
    try:
        parsed = urllib.parse.urlsplit(str(value or ""))
        port = parsed.port
    except ValueError as exc:
        raise KisOrderAuthorityError("KIS final transport URL is invalid") from exc
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or any(ord(character) < 0x20 for character in str(value or ""))
    ):
        raise KisOrderAuthorityError("KIS final transport URL is invalid")
    hostname = parsed.hostname.lower()
    effective_port = 443 if port is None else port
    origin = f"https://{hostname}:{effective_port}"
    official_origin = _canonical_kis_origin(KIS_LIVE_BASE_URL)
    configured_origin = _canonical_kis_origin(
        env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    )
    return {
        "rawUrl": str(value or ""),
        "origin": origin,
        "path": parsed.path,
        "query": parsed.query,
        "official": origin == official_origin,
        "configured": origin == configured_origin,
        "canonicalUrl": origin + parsed.path + (
            "?" + parsed.query if parsed.query else ""
        ),
    }


def _canonical_kis_headers(headers: dict[str, str]) -> dict[str, str]:
    if type(headers) is not dict:
        raise KisOrderAuthorityError("KIS final transport headers are invalid")
    normalized: dict[str, str] = {}
    for raw_key, raw_value in headers.items():
        if type(raw_key) is not str or type(raw_value) is not str:
            raise KisOrderAuthorityError(
                "KIS final transport headers are invalid"
            )
        key = raw_key.strip().lower()
        if (
            not key
            or key in normalized
            or key != raw_key.lower()
            or "\r" in raw_value
            or "\n" in raw_value
        ):
            raise KisOrderAuthorityError(
                "KIS final transport headers are invalid"
            )
        normalized[key] = raw_value
    return normalized


def _kis_trading_wire_projection(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> dict[str, object]:
    actual = _kis_actual_url(url)
    path = str(actual["path"])
    normalized_method = str(method or "").strip().upper()
    normalized_headers = _canonical_kis_headers(headers)
    body_bytes = _canonical_kis_trading_body_bytes(body)
    if (
        path not in _KIS_TRADING_ENDPOINTS
        or normalized_method != "POST"
        or actual["official"] is not True
        or actual["configured"] is not True
        or str(actual["query"])
        or str(actual["canonicalUrl"]) != KIS_LIVE_BASE_URL + path
        or str(actual["rawUrl"]) != KIS_LIVE_BASE_URL + path
        or set(normalized_headers) != _KIS_TRADING_HEADER_KEYS
        or normalized_headers.get("content-type")
        != "application/json; charset=utf-8"
        or normalized_headers.get("custtype") != "P"
        or normalized_headers.get("tr_id")
        not in _KIS_TRADING_TR_IDS[path]
        or not normalized_headers.get("authorization", "").startswith(
            "Bearer "
        )
        or not normalized_headers["authorization"][7:]
        or env_value("KIS_ENV").lower() != "real"
    ):
        raise KisOrderAuthorityError(
            "KIS final trading wire tuple is invalid"
        )
    return {
        "schemaVersion": "kis-final-trading-wire/v1",
        "method": normalized_method,
        "origin": actual["origin"],
        "path": path,
        "query": "",
        "headers": normalized_headers,
        "bodySha256": hashlib.sha256(body_bytes).hexdigest(),
        "bodyLength": len(body_bytes),
    }


def _canonical_kis_trading_body_bytes(
    body: dict[str, object] | None,
) -> bytes:
    if type(body) is not dict:
        raise KisOrderAuthorityError("KIS trading body is not an exact object")
    try:
        return json.dumps(
            body,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KisOrderAuthorityError("KIS trading body is invalid") from exc


def _kis_wire_hash(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> str:
    return hashlib.sha256(
        json.dumps(
            _kis_trading_wire_projection(
                method=method, url=url, body=body, headers=headers
            ),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def kis_prepared_payload_hash(prepared: PreparedRequest) -> str:
    """Hash the exact KIS method, URL, headers and body sent on the wire."""

    if type(prepared) is not PreparedRequest:
        raise KisOrderAuthorityError("KIS trading request is not exact")
    return _kis_wire_hash(
        method=prepared.method,
        url=prepared.url,
        body=prepared.body,
        headers=prepared.headers,
    )


def _kis_final_mutation_binding_raw(
    *, body: dict[str, object] | None, headers: dict[str, str]
) -> tuple[str, str]:
    """Independently bind the actual final request to live env credentials."""

    from .kis_domestic_functional_get_client import (
        _credential_configuration_hash,
        kis_domestic_functional_account_fingerprint,
    )

    body = body if isinstance(body, dict) else {}
    account = kis_domestic_functional_account_fingerprint(
        str(body.get("CANO") or "").strip(),
        str(body.get("ACNT_PRDT_CD") or "").strip(),
    )
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    normalized_headers = _canonical_kis_headers(headers)
    if (
        not app_key
        or not app_secret
        or normalized_headers.get("appkey") != app_key
        or normalized_headers.get("appsecret") != app_secret
        or not normalized_headers.get("authorization", "").startswith(
            "Bearer "
        )
    ):
        raise KisOrderAuthorityError(
            "KIS final transport credential/origin binding is invalid"
        )
    return account, _credential_configuration_hash(
        app_key=app_key,
        app_secret=app_secret,
        account_fingerprint=account,
    )


def _require_kis_trading_wire_authority(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> dict[str, object]:
    actual = _kis_actual_url(url)
    endpoint = str(actual["path"])
    wire_hash = _kis_wire_hash(
        method=method, url=url, body=body, headers=headers
    )
    account, credential = _kis_final_mutation_binding_raw(
        body=body, headers=headers
    )
    authority = require_inherited_kis_transport_authority(
        endpoint=endpoint,
        payload_hash=wire_hash,
        account_fingerprint=account,
        credential_configuration_hash=credential,
    )
    _validate_kis_operation_wire(
        operation=str(authority.get("inheritedOperation") or ""),
        endpoint=endpoint,
        body=body,
        headers=headers,
        owned_order_key=dict(
            authority.get("inheritedOwnedOrderKey") or {}
        ),
    )
    return dict(authority)


def _validate_kis_operation_wire(
    *,
    operation: str,
    endpoint: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
    owned_order_key: dict[str, str],
) -> None:
    normalized_headers = _canonical_kis_headers(headers)
    exact_body = body if type(body) is dict else {}
    tr_id = normalized_headers.get("tr_id", "")
    allowed: dict[str, tuple[str, frozenset[str]]] = {
        "PLACE_ORDER": (
            endpoint,
            (
                frozenset({"TTTC0012U", "TTTC0011U"})
                if endpoint == KIS_DOMESTIC_ORDER_ENDPOINT
                else frozenset({"TTTT1002U", "TTTT1006U"})
            ),
        ),
        "NATURAL_BUY": (
            KIS_DOMESTIC_ORDER_ENDPOINT,
            frozenset({"TTTC0012U"}),
        ),
        "CLEANUP_SELL": (
            KIS_DOMESTIC_ORDER_ENDPOINT,
            frozenset({"TTTC0011U"}),
        ),
        "CANCEL_ORDER": (
            KIS_DOMESTIC_CANCEL_ENDPOINT,
            frozenset({"TTTC0013U"}),
        ),
        "CLEANUP_CANCEL": (
            KIS_DOMESTIC_CANCEL_ENDPOINT,
            frozenset({"TTTC0013U"}),
        ),
        "KILL_ORDINARY_CANCEL": (
            KIS_DOMESTIC_CANCEL_ENDPOINT,
            frozenset({"TTTC0013U"}),
        ),
        "OVERSEAS_CANCEL_ORDER": (
            KIS_OVERSEAS_CANCEL_ENDPOINT,
            frozenset({"TTTT1004U"}),
        ),
    }
    contract = allowed.get(operation)
    if (
        contract is None
        or contract[0] != endpoint
        or tr_id not in contract[1]
    ):
        raise KisOrderAuthorityError(
            "KIS inherited operation/endpoint/TR/side wire binding is invalid"
        )
    if operation in {
        "CANCEL_ORDER",
        "CLEANUP_CANCEL",
        "KILL_ORDINARY_CANCEL",
    } and (
        set(owned_order_key)
        != {"orderDate", "organizationNo", "orderNo"}
        or str(exact_body.get("KRX_FWDG_ORD_ORGNO") or "")
        != str(owned_order_key.get("organizationNo") or "")
        or str(exact_body.get("ORGN_ODNO") or "")
        != str(owned_order_key.get("orderNo") or "")
        or str(exact_body.get("RVSE_CNCL_DVSN_CD") or "") != "02"
        or str(exact_body.get("QTY_ALL_ORD_YN") or "") != "Y"
        or str(exact_body.get("ORD_UNPR") or "") != "0"
    ):
        raise KisOrderAuthorityError(
            "KIS domestic cancel wire differs from exact owned order"
        )
    if operation == "OVERSEAS_CANCEL_ORDER" and (
        str(exact_body.get("RVSE_CNCL_DVSN_CD") or "") != "02"
        or str(exact_body.get("OVRS_ORD_UNPR") or "") != "0"
        or not str(exact_body.get("ORGN_ODNO") or "").strip()
    ):
        raise KisOrderAuthorityError(
            "KIS overseas cancel wire is not exact cancel-only"
        )


def _consume_kis_trading_wire_authority(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> None:
    """Burn the exact inherited lease at the last edge before the opener."""

    actual = _kis_actual_url(url)
    wire_hash = _kis_wire_hash(
        method=method,
        url=url,
        body=body,
        headers=headers,
    )
    account, credential = _kis_final_mutation_binding_raw(
        body=body,
        headers=headers,
    )
    authority = consume_inherited_kis_transport_authority(
        endpoint=str(actual["path"]),
        payload_hash=wire_hash,
        account_fingerprint=account,
        credential_configuration_hash=credential,
    )
    _validate_kis_operation_wire(
        operation=str(authority.get("inheritedOperation") or ""),
        endpoint=str(actual["path"]),
        body=body,
        headers=headers,
        owned_order_key=dict(
            authority.get("inheritedOwnedOrderKey") or {}
        ),
    )


def _kis_http_request_hash(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> str:
    """Hash every byte-producing input for the private paced dispatch."""

    actual = _kis_actual_url(url)
    normalized_headers = _canonical_kis_headers(headers)
    try:
        body_bytes = (
            json.dumps(
                body,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            if body is not None
            else b""
        )
    except (TypeError, ValueError) as exc:
        raise KisOrderAuthorityError("KIS HTTP body is invalid") from exc
    projection = {
        "method": str(method or "").strip().upper(),
        "url": actual["canonicalUrl"],
        "headers": normalized_headers,
        "bodySha256": hashlib.sha256(body_bytes).hexdigest(),
        "bodyLength": len(body_bytes),
    }
    return hashlib.sha256(
        json.dumps(
            projection,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _kis_read_final_binding_raw(
    *,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> tuple[str, str]:
    """Bind one exact credentialed GET to the configured live account."""

    from .kis_domestic_functional_get_client import (
        _credential_configuration_hash,
        kis_domestic_functional_account_fingerprint,
    )

    if body is not None:
        raise KisOrderAuthorityError("KIS read transport body is forbidden")
    actual = _kis_actual_url(url)
    normalized_headers = _canonical_kis_headers(headers)
    allowed_headers = {
        "content-type",
        "authorization",
        "appkey",
        "appsecret",
        "tr_id",
        "custtype",
        "tr_cont",
    }
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    if (
        not app_key
        or not app_secret
        or not set(normalized_headers).issubset(allowed_headers)
        or set(normalized_headers)
        < {
            "content-type",
            "authorization",
            "appkey",
            "appsecret",
            "tr_id",
            "custtype",
        }
        or normalized_headers.get("content-type")
        != "application/json; charset=utf-8"
        or not normalized_headers.get("authorization", "").startswith(
            "Bearer "
        )
        or not normalized_headers["authorization"][7:]
        or normalized_headers.get("appkey") != app_key
        or normalized_headers.get("appsecret") != app_secret
        or not normalized_headers.get("tr_id")
        or normalized_headers.get("tr_id")
        not in _KIS_READ_TR_IDS.get(str(actual["path"]), frozenset())
        or normalized_headers.get("custtype") != "P"
        or normalized_headers.get("tr_cont", "") not in {"", "N"}
    ):
        raise KisOrderAuthorityError(
            "KIS read transport credential/header binding is invalid"
        )
    cano, product = split_kis_account(
        env_value("KIS_ACCOUNT_NO"),
        env_value("KIS_ACCOUNT_PRODUCT_CODE"),
    )
    if not cano or not product:
        raise KisOrderAuthorityError(
            "KIS read transport account binding is missing"
        )
    pairs = urllib.parse.parse_qsl(
        str(actual["query"]),
        keep_blank_values=True,
        strict_parsing=True,
    )
    if len({key for key, _value in pairs}) != len(pairs):
        raise KisOrderAuthorityError(
            "KIS read transport query contains duplicate fields"
        )
    query = dict(pairs)
    carries_account = "CANO" in query or "ACNT_PRDT_CD" in query
    if carries_account and (
        set({"CANO", "ACNT_PRDT_CD"}) - set(query)
        or query.get("CANO") != cano
        or query.get("ACNT_PRDT_CD") != product
    ):
        raise KisOrderAuthorityError(
            "KIS read transport query account changed"
        )
    account = kis_domestic_functional_account_fingerprint(cano, product)
    credential = _credential_configuration_hash(
        app_key=app_key,
        app_secret=app_secret,
        account_fingerprint=account,
    )
    return account, credential


def _require_kis_read_wire_authority(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> None:
    actual = _kis_actual_url(url)
    account, credential = _kis_read_final_binding_raw(
        url=url,
        body=body,
        headers=headers,
    )
    require_kis_read_transport_authority(
        endpoint=str(actual["path"]),
        request_hash=_kis_http_request_hash(
            method=method,
            url=url,
            body=body,
            headers=headers,
        ),
        account_fingerprint=account,
        credential_configuration_hash=credential,
    )


def _consume_kis_read_wire_authority(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> None:
    actual = _kis_actual_url(url)
    account, credential = _kis_read_final_binding_raw(
        url=url,
        body=body,
        headers=headers,
    )
    consume_kis_read_transport_authority(
        endpoint=str(actual["path"]),
        request_hash=_kis_http_request_hash(
            method=method,
            url=url,
            body=body,
            headers=headers,
        ),
        account_fingerprint=account,
        credential_configuration_hash=credential,
    )


def _validate_exact_kis_token_request(
    *,
    actual: dict[str, object],
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> None:
    normalized_headers = _canonical_kis_headers(headers)
    if (
        actual["official"] is not True
        or actual["configured"] is not True
        or str(actual["query"])
        or str(actual["canonicalUrl"])
        != KIS_LIVE_BASE_URL + KIS_TOKEN_ENDPOINT
        or str(actual["rawUrl"])
        != KIS_LIVE_BASE_URL + KIS_TOKEN_ENDPOINT
        or env_value("KIS_ENV").lower() != "real"
        or set(normalized_headers) != {"content-type"}
        or normalized_headers.get("content-type")
        != "application/json; charset=utf-8"
        or type(body) is not dict
        or set(body) != {"grant_type", "appkey", "appsecret"}
        or body.get("grant_type") != "client_credentials"
        or body.get("appkey") != env_value("KIS_APP_KEY")
        or body.get("appsecret") != env_value("KIS_APP_SECRET")
        or not body.get("appkey")
        or not body.get("appsecret")
    ):
        raise KisOrderAuthorityError("KIS token wire tuple is invalid")


def _guard_kis_owned_http_edge(
    *,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
    kis_specific: bool,
) -> str:
    """Classify a KIS URL and fail closed for every unsupported method/path."""

    raw_url = str(url or "")
    raw_header_names = (
        {
            key.strip().lower()
            for key in headers
            if isinstance(key, str) and key.strip()
        }
        if isinstance(headers, dict)
        else set()
    )
    raw_body_names = (
        {
            key.strip().lower()
            for key in body
            if isinstance(key, str) and key.strip()
        }
        if isinstance(body, dict)
        else set()
    )
    signature_shaped = bool(
        "appkey" in raw_header_names
        or "appsecret" in raw_header_names
        or "tr_id" in raw_header_names
        or {"appkey", "appsecret"}.issubset(raw_body_names)
        or {"cano", "acnt_prdt_cd"}.issubset(raw_body_names)
    )
    path_hint_shaped = any(
        endpoint in raw_url for endpoint in _KIS_OWNED_ENDPOINTS
    )
    try:
        parsed = urllib.parse.urlsplit(raw_url)
        port = parsed.port
        candidate_origin = (
            f"{parsed.scheme.lower()}://{parsed.hostname.lower()}:"
            f"{443 if port is None else port}"
            if parsed.hostname and parsed.scheme
            else ""
        )
    except ValueError:
        if kis_specific or signature_shaped or path_hint_shaped:
            raise KisOrderAuthorityError("KIS URL is invalid") from None
        return "NON_KIS"
    path = parsed.path
    normalized_method = str(method or "").strip().upper()
    credential_shaped = bool(
        path in _KIS_OWNED_ENDPOINTS
        or signature_shaped
    )
    official_origin = _canonical_kis_origin(KIS_LIVE_BASE_URL)
    actual_official = candidate_origin == official_origin
    try:
        configured_origin = _canonical_kis_origin(
            env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
        )
    except KisOrderAuthorityError:
        if kis_specific or credential_shaped or actual_official:
            raise KisOrderAuthorityError(
                "KIS configured origin is invalid"
            ) from None
        return "NON_KIS"
    recognized_origin = candidate_origin in {
        official_origin,
        configured_origin,
    }
    trading_candidate = path in _KIS_TRADING_ENDPOINTS
    if not recognized_origin:
        if kis_specific or credential_shaped:
            raise KisOrderAuthorityError(
                "KIS-shaped request origin is not configured"
            )
        return "NON_KIS"
    if type(method) is not str or method != normalized_method:
        raise KisOrderAuthorityError(
            "KIS HTTP method is not the exact uppercase wire token"
        )
    actual = _kis_actual_url(url)
    if trading_candidate:
        _require_kis_trading_wire_authority(
            method=method, url=url, body=body, headers=headers
        )
        return "TRADING"
    if normalized_method == "GET":
        normalized_headers = _canonical_kis_headers(headers)
        credentialed = bool(
            {
                "authorization",
                "appkey",
                "appsecret",
            }.intersection(normalized_headers)
        )
        if credentialed and (
            actual["official"] is not True
            or actual["configured"] is not True
            or env_value("KIS_ENV").lower() != "real"
            or str(actual["rawUrl"]) != str(actual["canonicalUrl"])
        ):
            raise KisOrderAuthorityError(
                "KIS credentialed read wire tuple is invalid"
            )
        _require_kis_read_wire_authority(
            method=method,
            url=url,
            body=body,
            headers=headers,
        )
        return "READ"
    if normalized_method == "POST" and path == KIS_TOKEN_ENDPOINT:
        _validate_exact_kis_token_request(
            actual=actual,
            body=body,
            headers=headers,
        )
        require_kis_token_authority()
        return "TOKEN"
    raise KisOrderAuthorityError("unsupported KIS method/path is forbidden")


def _install_kis_http_dispatch(
    *,
    kind: str,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> _KisHttpDispatch:
    if getattr(_KIS_HTTP_DISPATCH_LOCAL, "ticket", None) is not None:
        raise KisOrderAuthorityError("nested KIS HTTP dispatch is forbidden")
    ticket = _KisHttpDispatch(
        owner_thread_id=threading.get_ident(),
        request_hash=_kis_http_request_hash(
            method=method,
            url=url,
            body=body,
            headers=headers,
        ),
        kind=kind,
        nonce=uuid.uuid4().hex,
    )
    _KIS_HTTP_DISPATCH_LOCAL.ticket = ticket
    return ticket


def _consume_kis_http_dispatch(
    *,
    kind: str,
    method: str,
    url: str,
    body: dict[str, object] | None,
    headers: dict[str, str],
) -> None:
    ticket = getattr(_KIS_HTTP_DISPATCH_LOCAL, "ticket", None)
    request_hash = _kis_http_request_hash(
        method=method,
        url=url,
        body=body,
        headers=headers,
    )
    with _KIS_HTTP_DISPATCH_LOCK:
        if (
            type(ticket) is not _KisHttpDispatch
            or ticket.owner_thread_id != threading.get_ident()
            or ticket.kind != kind
            or ticket.consumed
            or not secrets.compare_digest(ticket.request_hash, request_hash)
        ):
            raise KisOrderAuthorityError(
                "KIS HTTP requires one exact paced dispatch capability"
            )
        ticket.consumed = True


class _KisTradingNoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Turn every redirect into one terminal HTTPError; never follow it."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def send_prepared_request(prepared: PreparedRequest, *, timeout_seconds: float = 10.0) -> dict[str, object]:
    if not prepared.can_send:
        return {"ok": False, "status": "blocked", "preview": prepared.preview()}
    frozen = PreparedRequest(
        provider=prepared.provider,
        method=prepared.method,
        url=prepared.url,
        endpoint=prepared.endpoint,
        headers=copy.deepcopy(prepared.headers),
        safe_headers=copy.deepcopy(prepared.safe_headers),
        body=copy.deepcopy(prepared.body),
        query=copy.deepcopy(prepared.query),
        blocked_reasons=list(prepared.blocked_reasons),
    )
    _guard_ordinary_upbit_mutation_edge(frozen)
    try:
        parsed = urllib.parse.urlsplit(frozen.url)
        port = parsed.port
        actual_origin = (
            f"{parsed.scheme.lower()}://{parsed.hostname.lower()}:"
            f"{443 if port is None else port}"
            if parsed.hostname and parsed.scheme
            else ""
        )
        kis_url = actual_origin in {
            _canonical_kis_origin(KIS_LIVE_BASE_URL),
            _canonical_kis_origin(
                env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
            ),
        }
    except (KisOrderAuthorityError, ValueError):
        kis_url = False
        parsed = urllib.parse.SplitResult("", "", "", "", "")
    kis_provider = frozen.provider.strip().lower() == "kis"
    if kis_url or kis_provider:
        if (
            not kis_url
            or not kis_provider
            or frozen.endpoint != parsed.path
            or frozen.method.strip().upper() not in {"GET", "POST"}
        ):
            raise KisOrderAuthorityError(
                "KIS provider/endpoint/method metadata differs from actual URL"
            )
        _guard_kis_owned_http_edge(
            method=frozen.method,
            url=frozen.url,
            body=frozen.body,
            headers=frozen.headers,
            kis_specific=True,
        )
        return _send_kis_http_json(
            frozen.method,
            frozen.url,
            body=frozen.body,
            headers=frozen.headers,
            timeout_seconds=timeout_seconds,
        )
    return http_json(
        frozen.method,
        frozen.url,
        body=frozen.body,
        headers=frozen.headers,
        timeout_seconds=timeout_seconds,
    )


def http_json(
    method: str,
    url: str,
    *,
    body: dict[str, object] | None,
    headers: dict[str, str],
    timeout_seconds: float,
) -> dict[str, object]:
    frozen_body = copy.deepcopy(body)
    frozen_headers = copy.deepcopy(headers)
    kis_kind = _guard_kis_owned_http_edge(
        method=method,
        url=url,
        body=frozen_body,
        headers=frozen_headers,
        kis_specific=False,
    )
    kis_trading = kis_kind == "TRADING"
    # Every broker request uses one owned no-redirect opener.  urllib's
    # default redirect handler clones all non-content headers to Location,
    # including Upbit Authorization and Binance X-MBX-APIKEY.  A redirect is
    # therefore a terminal single-attempt outcome for KIS and crypto alike.
    kis_request = kis_kind != "NON_KIS"
    data = (
        _canonical_kis_trading_body_bytes(frozen_body)
        if kis_trading
        else (
            json.dumps(frozen_body, ensure_ascii=False).encode("utf-8")
            if frozen_body is not None
            else None
        )
    )
    request = urllib.request.Request(
        url, data=data, headers=frozen_headers, method=method
    )
    opener = urllib.request.build_opener(
        _KisTradingNoRedirectHandler()
    ).open
    if kis_kind != "NON_KIS":
        _consume_kis_http_dispatch(
            kind=kis_kind,
            method=method,
            url=url,
            body=frozen_body,
            headers=frozen_headers,
        )
    if kis_trading:
        _consume_kis_trading_wire_authority(
            method=method,
            url=url,
            body=frozen_body,
            headers=frozen_headers,
        )
    elif kis_kind == "READ":
        _consume_kis_read_wire_authority(
            method=method,
            url=url,
            body=frozen_body,
            headers=frozen_headers,
        )
    try:
        with opener(request, timeout=timeout_seconds) as response:  # noqa: S310 - exact broker URL checked above.
            status_code = int(response.status)
            effective_url = str(response.geturl())
            if effective_url != url or 300 <= status_code <= 399:
                if kis_request:
                    raise KisOrderAuthorityError(
                        "KIS response effective URL changed"
                    )
                return {
                    "ok": False,
                    "statusCode": status_code,
                    "text": "HTTP redirect/effective URL change blocked",
                    "json": {},
                    "redirectBlocked": True,
                    "outcomeAmbiguous": method.strip().upper()
                    in {"POST", "PUT", "PATCH", "DELETE"},
                    "physicalAttemptCount": 1,
                    "retryAllowed": False,
                }
            text = response.read().decode("utf-8", errors="replace")
            return {
                "ok": 200 <= status_code < 300,
                "statusCode": status_code,
                "text": text,
                "json": parse_json(text),
                "trCont": str(response.headers.get("tr_cont") or ""),
            }
    except urllib.error.HTTPError as exc:
        try:
            if 300 <= int(exc.code) <= 399:
                return {
                    "ok": False,
                    "statusCode": int(exc.code),
                    "text": (
                        "KIS redirect blocked before follow"
                        if kis_request
                        else "HTTP redirect blocked before follow"
                    ),
                    "json": {},
                    "trCont": "",
                    "redirectBlocked": True,
                    "outcomeAmbiguous": (
                        True
                        if kis_request
                        else method.strip().upper()
                        in {"POST", "PUT", "PATCH", "DELETE"}
                    ),
                    "physicalAttemptCount": 1,
                    "retryAllowed": False,
                }
            text = exc.read().decode("utf-8", errors="replace")
            result = {
                "ok": False,
                "statusCode": int(exc.code),
                "text": text,
                "json": parse_json(text),
                "trCont": (
                    str(exc.headers.get("tr_cont") or "")
                    if exc.headers
                    else ""
                ),
            }
            if kis_kind in {"TRADING", "READ"}:
                result.update(
                    {
                        "physicalAttemptCount": 1,
                        "retryAllowed": False,
                    }
                )
                if kis_trading:
                    result["outcomeAmbiguous"] = True
            return result
        finally:
            exc.close()
    except urllib.error.URLError as exc:
        result = {
            "ok": False,
            "statusCode": 0,
            "text": str(exc.reason),
            "json": {},
        }
        if kis_kind in {"TRADING", "READ"}:
            result.update(
                {
                    "physicalAttemptCount": 1,
                    "retryAllowed": False,
                }
            )
            if kis_trading:
                result["outcomeAmbiguous"] = True
        return result
    except TimeoutError as exc:
        # urlopen() may succeed and then time out while response.read() waits.
        # That read timeout is a bare TimeoutError, not urllib.error.URLError.
        result = {
            "ok": False,
            "statusCode": 0,
            "text": str(exc),
            "json": {},
        }
        if kis_kind in {"TRADING", "READ"}:
            result.update(
                {
                    "physicalAttemptCount": 1,
                    "retryAllowed": False,
                }
            )
            if kis_trading:
                result["outcomeAmbiguous"] = True
        return result


def parse_json(text: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def normalize_side(value: object) -> OrderSide:
    text = str(value or "BUY").strip().upper()
    return "SELL" if text in {"SELL", "S", "ASK", "매도"} else "BUY"


def normalize_quantity(value: object) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def normalize_price(value: object) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def normalize_decimal_text(value: object) -> str:
    number = normalize_price(value)
    return f"{number:.12f}".rstrip("0").rstrip(".")


def infer_us_exchange(symbol: str) -> str:
    _ = symbol
    return "NASD"


def sign_binance_query(query: dict[str, object], secret: str) -> str:
    encoded = urllib.parse.urlencode(query)
    signature = hmac.new(secret.encode("utf-8"), encoded.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{encoded}&signature={signature}"


def build_upbit_authorization(access_key: str, secret_key: str, body: dict[str, object]) -> str:
    payload = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
    }
    if body:
        query = urllib.parse.urlencode(body).encode("utf-8")
        payload["query_hash"] = hashlib.sha512(query).hexdigest()
        payload["query_hash_alg"] = "SHA512"
    header = _b64url_json({"alg": "HS256", "typ": "JWT"})
    claims = _b64url_json(payload)
    signing_input = f"{header}.{claims}"
    signature = hmac.new(secret_key.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
    return f"Bearer {signing_input}.{_b64url(signature)}"


def _b64url_json(payload: dict[str, object]) -> str:
    return _b64url(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _b64url(raw: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
