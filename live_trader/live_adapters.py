from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Literal


OrderSide = Literal["BUY", "SELL"]

KIS_LIVE_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_TOKEN_ENDPOINT = "/oauth2/tokenP"
KIS_DOMESTIC_ORDER_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-cash"
KIS_OVERSEAS_ORDER_ENDPOINT = "/uapi/overseas-stock/v1/trading/order"
KIS_DOMESTIC_BALANCE_ENDPOINT = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_DOMESTIC_TR_IDS = {"BUY": "TTTC0012U", "SELL": "TTTC0011U"}
KIS_OVERSEAS_TR_IDS = {"BUY": "TTTT1002U", "SELL": "TTTT1006U"}
KIS_DOMESTIC_BALANCE_TR_ID = "TTTC8434R"

BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_ORDER_ENDPOINT = "/api/v3/order"
BINANCE_TEST_ORDER_ENDPOINT = "/api/v3/order/test"
BINANCE_ACCOUNT_ENDPOINT = "/api/v3/account"

UPBIT_BASE_URL = "https://api.upbit.com"
UPBIT_ORDER_ENDPOINT = "/v1/orders"
UPBIT_ACCOUNTS_ENDPOINT = "/v1/accounts"


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


def missing_env(*names: str) -> list[str]:
    return [name for name in names if not env_value(name)]


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


def build_binance_account_request() -> PreparedRequest:
    blocked = missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET")
    api_key = env_value("BINANCE_API_KEY")
    api_secret = env_value("BINANCE_API_SECRET")
    base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    query: dict[str, object] = {"timestamp": int(time.time() * 1000)}
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


def build_binance_spot_order_request(intent: dict[str, object], *, test: bool = False) -> PreparedRequest:
    blocked = missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET")
    api_key = env_value("BINANCE_API_KEY")
    api_secret = env_value("BINANCE_API_SECRET")
    base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    symbol = str(intent.get("symbol") or "").strip().upper()
    side = normalize_side(intent.get("side"))
    quantity = normalize_decimal_text(intent.get("quantity") or intent.get("qty") or 0)
    order_type = str(intent.get("order_type") or "MARKET").strip().upper()
    query: dict[str, object] = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": quantity,
        "timestamp": int(time.time() * 1000),
    }
    if order_type == "LIMIT":
        query["timeInForce"] = str(intent.get("time_in_force") or "GTC")
        query["price"] = normalize_decimal_text(intent.get("price") or 0)
    if not symbol:
        blocked.append("symbol")
    if float(quantity or 0) <= 0:
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


def build_upbit_order_request(intent: dict[str, object]) -> PreparedRequest:
    blocked = missing_env("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    base_url = env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL
    market = str(intent.get("market") or intent.get("symbol") or "").strip().upper()
    side = "bid" if normalize_side(intent.get("side")) == "BUY" else "ask"
    ord_type = str(intent.get("order_type") or "limit").strip().lower()
    body: dict[str, object] = {"market": market, "side": side, "ord_type": ord_type}
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


def issue_kis_access_token(*, timeout_seconds: float = 10.0) -> str:
    missing = missing_env("KIS_APP_KEY", "KIS_APP_SECRET")
    if missing:
        raise RuntimeError(f"KIS token settings missing: {', '.join(missing)}")
    response = http_json(
        "POST",
        (env_value("KIS_BASE_URL") or KIS_LIVE_BASE_URL).rstrip("/") + KIS_TOKEN_ENDPOINT,
        body={
            "grant_type": "client_credentials",
            "appkey": env_value("KIS_APP_KEY"),
            "appsecret": env_value("KIS_APP_SECRET"),
        },
        headers={"content-type": "application/json; charset=utf-8"},
        timeout_seconds=timeout_seconds,
    )
    payload = response.get("json") if isinstance(response.get("json"), dict) else {}
    token = str(payload.get("access_token") or "").strip()
    if not token:
        raise RuntimeError(str(payload.get("msg1") or response.get("text") or "KIS token response did not include access_token."))
    return token


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
            return {"ok": 200 <= int(response.status) < 400, "statusCode": int(response.status), "text": text, "json": parse_json(text)}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "statusCode": int(exc.code), "text": text, "json": parse_json(text)}
    except urllib.error.URLError as exc:
        return {"ok": False, "statusCode": 0, "text": str(exc.reason), "json": {}}


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
