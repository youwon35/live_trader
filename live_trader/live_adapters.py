from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Literal


OrderSide = Literal["BUY", "SELL"]

KIS_LIVE_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_TOKEN_ENDPOINT = "/oauth2/tokenP"
KIS_DOMESTIC_ORDER_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-cash"
KIS_OVERSEAS_ORDER_ENDPOINT = "/uapi/overseas-stock/v1/trading/order"
KIS_DOMESTIC_CANCEL_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
KIS_OVERSEAS_CANCEL_ENDPOINT = "/uapi/overseas-stock/v1/trading/order-rvsecncl"
KIS_DOMESTIC_BALANCE_ENDPOINT = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_OVERSEAS_BALANCE_ENDPOINT = "/uapi/overseas-stock/v1/trading/inquire-balance"
KIS_DOMESTIC_TR_IDS = {"BUY": "TTTC0012U", "SELL": "TTTC0011U"}
KIS_OVERSEAS_TR_IDS = {"BUY": "TTTT1002U", "SELL": "TTTT1006U"}
KIS_DOMESTIC_BALANCE_TR_ID = "TTTC8434R"
KIS_OVERSEAS_BALANCE_TR_ID = "TTTS3012R"

BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_ORDER_ENDPOINT = "/api/v3/order"
BINANCE_TEST_ORDER_ENDPOINT = "/api/v3/order/test"
BINANCE_ACCOUNT_ENDPOINT = "/api/v3/account"
BINANCE_TIME_ENDPOINT = "/api/v3/time"
BINANCE_EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
BINANCE_FUTURES_ORDER_ENDPOINT = "/fapi/v1/order"
BINANCE_FUTURES_TEST_ORDER_ENDPOINT = "/fapi/v1/order/test"
BINANCE_FUTURES_ACCOUNT_ENDPOINT = "/fapi/v3/account"
BINANCE_FUTURES_POSITION_ENDPOINT = "/fapi/v3/positionRisk"
BINANCE_FUTURES_POSITION_MODE_ENDPOINT = "/fapi/v1/positionSide/dual"
BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT = "/fapi/v1/symbolConfig"
BINANCE_FUTURES_TIME_ENDPOINT = "/fapi/v1/time"
BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"

UPBIT_BASE_URL = "https://api.upbit.com"
UPBIT_ORDER_ENDPOINT = "/v1/orders"
UPBIT_ACCOUNTS_ENDPOINT = "/v1/accounts"
UPBIT_ORDER_CHANCE_ENDPOINT = "/v1/orders/chance"
UPBIT_ORDER_DETAIL_ENDPOINT = "/v1/order"

_KIS_TOKEN_CACHE: dict[str, object] = {"key": "", "token": "", "expires_at": 0.0}
_KIS_TOKEN_LOCK = threading.Lock()
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


def build_kis_domestic_balance_request(*, access_token: str = "") -> PreparedRequest:
    required = ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE")
    blocked = missing_env(*required)
    app_key = env_value("KIS_APP_KEY")
    app_secret = env_value("KIS_APP_SECRET")
    account_no = env_value("KIS_ACCOUNT_NO")
    product_code = env_value("KIS_ACCOUNT_PRODUCT_CODE")
    base_url = env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL
    cano, acnt_prdt_cd = split_kis_account(account_no, product_code)
    if not access_token:
        blocked.append("access_token")
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
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}" if access_token else "",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": KIS_DOMESTIC_BALANCE_TR_ID,
        "custtype": "P",
    }
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
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    base_url = env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL
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
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    base_url = env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL
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

        response = http_json(
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


def send_prepared_request(prepared: PreparedRequest, *, timeout_seconds: float = 10.0) -> dict[str, object]:
    if not prepared.can_send:
        return {"ok": False, "status": "blocked", "preview": prepared.preview()}
    return http_json(prepared.method, prepared.url, body=prepared.body, headers=prepared.headers, timeout_seconds=timeout_seconds)


def http_json(
    method: str,
    url: str,
    *,
    body: dict[str, object] | None,
    headers: dict[str, str],
    timeout_seconds: float,
) -> dict[str, object]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - official user-selected broker endpoints.
            text = response.read().decode("utf-8", errors="replace")
            return {
                "ok": 200 <= int(response.status) < 400,
                "statusCode": int(response.status),
                "text": text,
                "json": parse_json(text),
                "trCont": str(response.headers.get("tr_cont") or ""),
            }
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "statusCode": int(exc.code),
            "text": text,
            "json": parse_json(text),
            "trCont": str(exc.headers.get("tr_cont") or "") if exc.headers else "",
        }
    except urllib.error.URLError as exc:
        return {"ok": False, "statusCode": 0, "text": str(exc.reason), "json": {}}
    except TimeoutError as exc:
        # urlopen() may succeed and then time out while response.read() waits.
        # That read timeout is a bare TimeoutError, not urllib.error.URLError.
        return {"ok": False, "statusCode": 0, "text": str(exc), "json": {}}


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
