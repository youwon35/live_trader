"""Credential-scoped account reads. No order transport, global monkeypatch or disk cache.

Only exact official read endpoints are reachable. OAuth tokenP authenticates reads;
it cannot mint a trading permit. Responses and credentials stay in memory.
"""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from .live_adapters import build_upbit_authorization, sign_binance_query, split_kis_account

ORIGINS = {"binance": "https://api.binance.com", "binance-futures": "https://fapi.binance.com",
           "upbit": "https://api.upbit.com", "kis": "https://openapi.koreainvestment.com:9443"}
READ_PATHS = {
 "binance": {"/api/v3/time", "/api/v3/account", "/api/v3/openOrders", "/api/v3/exchangeInfo", "/api/v3/account/commission", "/api/v3/avgPrice", "/api/v3/ticker/price", "/api/v3/myTrades", "/api/v3/order"},
 "binance-futures": {"/fapi/v1/time", "/fapi/v3/account", "/fapi/v1/openOrders", "/fapi/v1/exchangeInfo"},
 "upbit": {"/v1/accounts", "/v1/orders/chance", "/v1/orders/open", "/v1/ticker", "/v1/order"},
 "kis": {"/uapi/domestic-stock/v1/trading/inquire-balance",
         "/uapi/overseas-stock/v1/trading/inquire-balance",
         "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
         "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
         "/uapi/overseas-stock/v1/trading/inquire-psamount",
         "/uapi/domestic-stock/v1/quotations/inquire-price",
         "/uapi/overseas-price/v1/quotations/price-detail",
         "/uapi/overseas-price/v1/quotations/inquire-time-indexchartprice",
         "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
         "/uapi/overseas-stock/v1/trading/inquire-ccnl",
         "/uapi/overseas-price/v1/quotations/price",
         "/uapi/overseas-stock/v1/trading/inquire-nccs"},
}
KIS_TR_IDS = {
 "/uapi/overseas-price/v1/quotations/inquire-time-indexchartprice": "FHKST03030200",
 "/uapi/overseas-price/v1/quotations/price-detail": "HHDFS76200200",
 "/uapi/domestic-stock/v1/trading/inquire-daily-ccld": "TTTC0081R",
 "/uapi/overseas-stock/v1/trading/inquire-ccnl": "TTTS3035R",
 "/uapi/domestic-stock/v1/quotations/inquire-price": "FHKST01010100",
 "/uapi/overseas-price/v1/quotations/price": "HHDFS00000300",
 "/uapi/domestic-stock/v1/trading/inquire-psbl-order": "TTTC8908R",
 "/uapi/overseas-stock/v1/trading/inquire-psamount": "TTTS3007R",
 "/uapi/domestic-stock/v1/trading/inquire-balance": "TTTC8434R",
 "/uapi/overseas-stock/v1/trading/inquire-balance": "TTTS3012R",
 "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl": "TTTC0084R",
 "/uapi/overseas-stock/v1/trading/inquire-nccs": "TTTS3018R",
}
SETTING_KEYS = ("BINANCE_API_KEY", "BINANCE_API_SECRET", "BINANCE_BASE_URL", "BINANCE_FUTURES_BASE_URL",
 "UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY", "UPBIT_BASE_URL", "KIS_APP_KEY", "KIS_APP_SECRET",
 "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE", "KIS_BASE_URL", "KIS_ENV")
_TOKEN_CACHE = {}
_TOKEN_LOCK = threading.Lock()
_KIS_LOCK = threading.Lock()
_KIS_LAST = 0.0


class ReadBoundaryError(ValueError):
    pass


class ReadFailure(RuntimeError):
    """Safe status only. Never include a URL, remote response or credential."""
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ReadFailure("REDIRECT_BLOCKED")


class ReadOnlyTransport:
    def __init__(self, *, timeout=8):
        self.timeout = timeout
        self.events = []
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, provider, method, path, *, query=None, headers=None, body=None):
        oauth = provider == "kis" and method == "POST" and path == "/oauth2/tokenP"
        if not oauth and (method != "GET" or path not in READ_PATHS.get(provider, set()) or body is not None):
            raise ReadBoundaryError("READ_ONLY_ENDPOINT_BLOCKED")
        if provider not in ORIGINS or (oauth and (
            query or set(body or {}) != {"grant_type", "appkey", "appsecret"}
            or body.get("grant_type") != "client_credentials")):
            raise ReadBoundaryError("READ_ONLY_ENDPOINT_BLOCKED")
        if provider == "kis" and not oauth and (headers or {}).get("tr_id") != KIS_TR_IDS[path]:
            raise ReadBoundaryError("READ_ONLY_TR_ID_BLOCKED")
        # Path is an exact constant, never URL input. A query cannot change the path.
        encoded = query if isinstance(query, str) else urllib.parse.urlencode(query or {}, doseq=True)
        url = ORIGINS[provider] + path + ("?" + encoded if encoded else "")
        req = urllib.request.Request(url, data=json.dumps(body).encode() if oauth else None,
                                     headers=headers or {}, method=method)
        event = {"provider": provider, "method": method, "path": path, "status": "UNKNOWN"}
        self.events.append(event)
        try:
            with self.opener.open(req, timeout=self.timeout) as response:
                if response.status < 200 or response.status >= 300:
                    raise ReadFailure("HTTP_ERROR")
                # Defense even for an injected opener which did not use NoRedirect.
                if response.geturl() != url:
                    raise ReadFailure("REDIRECT_BLOCKED")
                raw = response.read(4 * 1024 * 1024 + 1)
                if len(raw) > 4 * 1024 * 1024:
                    raise ReadFailure("RESPONSE_LIMIT")
                payload = json.loads(raw)
                event["status"] = "AVAILABLE"
                return payload, {str(k).lower(): str(v) for k, v in response.headers.items()}
        except ReadFailure:
            event["status"] = "BLOCKED"
            raise
        except (OSError, ValueError, urllib.error.URLError):
            event["status"] = "UNAVAILABLE"
            raise ReadFailure("READ_UNAVAILABLE") from None


def runtime_settings():
    """Snapshot already loaded Live settings; never migrate or write settings."""
    return {key: os.environ.get(key, "").strip() for key in SETTING_KEYS}


def decimal(value):
    try:
        result = Decimal(str(value))
        return result if result.is_finite() and result >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _complete_number(value):
    result = decimal(value)
    if result is None:
        raise ReadFailure("INVALID_ACCOUNT_RESPONSE")
    return result


class ReadOnlyBrokerReader:
    def __init__(self, settings, *, transport=None):
        self.settings = dict(settings)
        self.transport = transport or ReadOnlyTransport()

    def _get(self, provider, path, **kwargs):
        value, headers = self.transport.request(provider, "GET", path, **kwargs)
        if isinstance(value, dict) and (value.get("error") or ("code" in value and int(value.get("code") or 0) < 0)):
            raise ReadFailure("BROKER_READ_REJECTED")
        return value, headers

    def _credentials(self, provider):
        names = {"binance": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
                 "binance-futures": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
                 "upbit": ("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY"),
                 "kis": ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE")}[provider]
        if any(not self.settings.get(key) for key in names):
            raise ReadFailure("CREDENTIALS_MISSING")
        base_key = {"binance": "BINANCE_BASE_URL", "binance-futures": "BINANCE_FUTURES_BASE_URL",
                    "upbit": "UPBIT_BASE_URL", "kis": "KIS_BASE_URL"}[provider]
        if self.settings.get(base_key, "").rstrip("/") not in ("", ORIGINS[provider]):
            raise ReadFailure("CONFIGURED_ORIGIN_BLOCKED")

    def read(self, lane, symbol, *, side="", quantity=None, limit_price=None):
        provider = "kis" if lane in {"kis-kr", "kis-us"} else lane
        if provider not in ORIGINS:
            return {"status": "UNSUPPORTED", "account": "UNKNOWN", "openOrders": "UNKNOWN",
                    "orderability": "UNKNOWN", "fundsCheck": "UNKNOWN"}
        result = {"status": "UNKNOWN", "account": "UNKNOWN", "openOrders": "UNKNOWN",
                  "orderability": "UNKNOWN", "fundsCheck": "UNKNOWN", "credentialSource": "LIVE_RUNTIME_SETTINGS",
                  "accountBinding": "NOT_VERIFIED", "currency": ""}
        try:
            self._credentials(provider)
            facts = self._binance(provider, symbol) if provider.startswith("binance") else (
                self._upbit(symbol) if provider == "upbit" else self._kis(lane, symbol))
            result.update({key: value for key, value in facts.items() if not key.startswith("_")})
            with localcontext() as context:
                context.prec = 80
                amount = quantity * limit_price if quantity is not None and limit_price is not None else None
            available = facts.get("_quote") if side == "BUY" else facts.get("_base") if side == "SELL" else None
            needed = amount if side == "BUY" else quantity
            if available is not None and needed is not None:
                result["fundsCheck"] = "WITHIN_OBSERVED_BALANCE" if needed <= available else "INSUFFICIENT_OBSERVED_BALANCE"
            if provider == "binance-futures":
                result["fundsCheck"] = "UNKNOWN"  # Margin/leverage/position-side risk is a separate live gate.
            result["status"] = "OBSERVED" if result["account"] == "AVAILABLE" else "PARTIAL"
        except ReadFailure as exc:
            result["status"] = str(exc)
        except (ValueError, TypeError, KeyError, ArithmeticError):
            result["status"] = "INVALID_ACCOUNT_RESPONSE"
        result["asOf"] = datetime.now(timezone.utc).isoformat()
        # No account balances, IDs, keys, raw broker responses, or tokens cross this boundary.
        return result

    def observe_preparation(self, lane, symbol, *, side="", quantity=None, limit_price=None):
        """Local explicit preview only. Private balances are never persisted by this reader."""
        from .account_profiles import saved_account_profiles
        provider = "kis" if lane in {"kis-kr", "kis-us"} else lane
        result = {"status": "UNKNOWN", "account": "UNKNOWN", "openOrders": "UNKNOWN",
                  "orderability": "UNKNOWN", "fundsCheck": "UNKNOWN", "accountBinding": "NOT_VERIFIED",
                  "availableCash": None, "availableAssetQuantity": None, "feeRate": None,
                  "constraints": {}, "capitalBasis": "ACCOUNT_AVAILABLE_CASH", "capitalPercent": 100,
                  "currency": "", "privateValuesLocalOnly": True}
        try:
            if provider not in {"binance", "upbit", "kis"}:
                raise ReadFailure("CASH_SPOT_SCOPE_REQUIRED")
            self._credentials(provider)
            profiles = [p for p in saved_account_profiles(self.settings) if p["brokerId"] == provider]
            if len(profiles) != 1:
                raise ReadFailure("SAVED_ACCOUNT_BINDING_UNAVAILABLE")
            facts = self._binance(provider, symbol) if provider == "binance" else self._upbit(symbol) if provider == "upbit" else self._kis(lane, symbol)
            if provider == "kis" and limit_price is not None:
                facts.update(self._kis_cash(lane, symbol, limit_price))
            result.update({key: value for key, value in facts.items() if not key.startswith("_")})
            result.update({"accountAlias": profiles[0]["alias"], "maskedAccount": profiles[0]["maskedAccount"],
                           "profileId": profiles[0]["profileId"], "accountBinding": "SAVED_CREDENTIAL_CONFIGURATION_OBSERVED"})
            for field, key in (("availableCash", "_quote"), ("availableAssetQuantity", "_base")):
                if facts.get(key) is not None:
                    result[field] = format(facts[key], "f")
            if provider == "binance":
                fee = decimal(facts.get("_commission"))
                filters = facts.get("_filters")
                if not isinstance(filters, list):
                    raise ReadFailure("INVALID_MARKET_CONSTRAINTS")
                allowed = {"LOT_SIZE": ("minQty", "maxQty", "stepSize"), "PRICE_FILTER": ("minPrice", "maxPrice", "tickSize"),
                           "MIN_NOTIONAL": ("minNotional",), "NOTIONAL": ("minNotional", "maxNotional")}
                for row in filters:
                    if isinstance(row, dict) and row.get("filterType") in allowed:
                        kind = row["filterType"]
                        if kind in result["constraints"]:
                            raise ReadFailure("AMBIGUOUS_MARKET_CONSTRAINTS")
                        result["constraints"][kind] = {k: format(_complete_number(row.get(k)), "f") for k in allowed[kind]}
            elif provider == "upbit":
                fee = decimal(facts.get("_fee"))
                market = facts.get("_chance", {})
                bid = market.get("bid", {})
                result["constraints"] = {"minNotional": format(_complete_number(bid.get("min_total")), "f"),
                    "maxNotional": format(_complete_number(market.get("max_total")), "f"),
                    "limitBuySupported": "limit" in market.get("bid_types", []),
                    "quantityStep": None, "priceTick": None}
                # Current chance API provides notional bounds, not an authoritative quantity/price grid.
            else:
                fee = None  # Contract fee assumptions remain distinct from actual broker commissions.
                result["constraints"] = {"quantityStep": "1", "minQuantity": "1", "priceTick": None,
                                          "maximumBuyQuantity": str(facts.get("_maximumQty")) if facts.get("_maximumQty") is not None else None}
            result["feeRate"] = format(fee, "f") if fee is not None else None
            available = facts.get("_quote") if side == "BUY" else facts.get("_base") if side == "SELL" else None
            needed = quantity * limit_price if side == "BUY" and quantity is not None and limit_price is not None else quantity if side == "SELL" else None
            if available is not None and needed is not None:
                result["fundsCheck"] = "WITHIN_OBSERVED_BALANCE" if needed <= available else "INSUFFICIENT_OBSERVED_BALANCE"
            result["status"] = "OBSERVED" if result["account"] == "AVAILABLE" else "PARTIAL"
        except ReadFailure as exc:
            result["status"] = str(exc)
        except (ValueError, TypeError, KeyError, ArithmeticError):
            result["status"] = "INVALID_ACCOUNT_RESPONSE"
        result["asOf"] = datetime.now(timezone.utc).isoformat()
        return result

    def kis_reference_price(self, lane, symbol):
        """Official GET quote, no draft selection. NASD scope stays explicit/unverified."""
        self._credentials("kis")
        if self.settings.get("KIS_ENV", "").lower() not in ("", "real", "live"):
            raise ReadFailure("LIVE_ACCOUNT_ENVIRONMENT_REQUIRED")
        token = self._kis_token()
        path = "/uapi/domestic-stock/v1/quotations/inquire-price" if lane == "kis-kr" else "/uapi/overseas-price/v1/quotations/price"
        query = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol} if lane == "kis-kr" else {"AUTH": "", "EXCD": "NAS", "SYMB": symbol}
        self._pace_kis()
        payload, headers = self._get("kis", path, query=query, headers={"authorization": "Bearer " + token,
            "appkey": self.settings["KIS_APP_KEY"], "appsecret": self.settings["KIS_APP_SECRET"], "tr_id": KIS_TR_IDS[path], "custtype": "P"})
        if not isinstance(payload, dict) or str(payload.get("rt_cd")) != "0" or headers.get("tr_cont", "").upper() in {"F", "M"}:
            raise ReadFailure("QUOTE_UNAVAILABLE")
        output = payload.get("output")
        if not isinstance(output, dict):
            raise ReadFailure("QUOTE_UNAVAILABLE")
        value = _complete_number(output.get("stck_prpr" if lane == "kis-kr" else "last"))
        if value <= 0:
            raise ReadFailure("QUOTE_UNAVAILABLE")
        return value

    def _kis_cash(self, lane, symbol, price):
        if price <= 0:
            raise ReadFailure("REFERENCE_PRICE_REQUIRED")
        token = self._kis_token()
        cano, product = split_kis_account(self.settings["KIS_ACCOUNT_NO"], self.settings["KIS_ACCOUNT_PRODUCT_CODE"])
        overseas = lane == "kis-us"
        path = "/uapi/overseas-stock/v1/trading/inquire-psamount" if overseas else "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        query = {"CANO": cano, "ACNT_PRDT_CD": product}
        query.update({"OVRS_EXCG_CD": "NASD", "OVRS_ORD_UNPR": format(price, "f"), "ITEM_CD": symbol} if overseas else {
            "PDNO": symbol, "ORD_UNPR": format(price, "f"), "ORD_DVSN": "00", "CMA_EVLU_AMT_ICLD_YN": "N", "OVRS_ICLD_YN": "N"})
        self._pace_kis()
        payload, response_headers = self._get("kis", path, query=query, headers={"authorization": "Bearer " + token,
            "appkey": self.settings["KIS_APP_KEY"], "appsecret": self.settings["KIS_APP_SECRET"], "tr_id": KIS_TR_IDS[path], "custtype": "P"})
        if not isinstance(payload, dict) or str(payload.get("rt_cd")) != "0" or response_headers.get("tr_cont", "").upper() in {"F", "M"}:
            raise ReadFailure("INCOMPLETE_ACCOUNT_RESPONSE")
        output = payload.get("output")
        if isinstance(output, list) and len(output) == 1:
            output = output[0]
        if not isinstance(output, dict):
            raise ReadFailure("INVALID_ACCOUNT_RESPONSE")
        if overseas:
            if output.get("tr_crcy_cd") != "USD":
                raise ReadFailure("ACCOUNT_CURRENCY_MISMATCH")
            cash = min(_complete_number(output.get("ord_psbl_frcr_amt")), _complete_number(output.get("ovrs_ord_psbl_amt")))
            max_qty = _complete_number(output.get("ord_psbl_qty"))
        else:
            cash = min(_complete_number(output.get("ord_psbl_cash")), _complete_number(output.get("nrcvb_buy_amt")))
            max_qty = _complete_number(output.get("nrcvb_buy_qty"))
        return {"_quote": cash, "_maximumQty": max_qty, "cashScope": "KIS_NASD_USD_NO_AUTO_FX" if overseas else "KIS_NO_MARGIN_NO_CMA_NO_OVERSEAS",
                "cashReferencePrice": format(price, "f"), "cashReferencePriceSource": "USER_LIMIT_PRICE"}

    def _binance(self, provider, symbol):
        prefix = "/api/v3" if provider == "binance" else "/fapi/v1"
        clock, _ = self._get(provider, prefix + "/time")
        timestamp = int(clock["serverTime"])
        signed = lambda params: sign_binance_query({**params, "timestamp": timestamp, "recvWindow": 10000}, self.settings["BINANCE_API_SECRET"])
        headers = {"X-MBX-APIKEY": self.settings["BINANCE_API_KEY"]}
        account, _ = self._get(provider, "/api/v3/account" if provider == "binance" else "/fapi/v3/account",
                              query=signed({}), headers=headers)
        if not isinstance(account, dict) or not isinstance(account.get("balances" if provider == "binance" else "assets"), list):
            raise ReadFailure("INVALID_ACCOUNT_RESPONSE")
        facts = {"account": "AVAILABLE", "openOrders": "UNKNOWN", "orderability": "UNKNOWN", "currency": "", "openOrdersScope": "SELECTED_SYMBOL_ONLY", "conditionalOrders": "UNKNOWN" if provider == "binance-futures" else "INCLUDED_IN_REGULAR_QUERY"}
        try:
            orders, _ = self._get(provider, prefix + "/openOrders", query=signed({"symbol": symbol}), headers=headers)
            if isinstance(orders, list):
                facts["openOrders"] = "PRESENT" if orders else ("REGULAR_SYMBOL_NONE_ALGO_UNKNOWN" if provider == "binance-futures" else "SYMBOL_NONE_OBSERVED")
        except ReadFailure:
            pass
        try:
            info, _ = self._get(provider, prefix + "/exchangeInfo", query={"symbol": symbol} if provider == "binance" else {})
            matches = [item for item in info.get("symbols", []) if item.get("symbol") == symbol]
            if len(matches) != 1:
                return facts
            market = matches[0]
            facts["_filters"] = market.get("filters", [])
            facts["_commission"] = account.get("commissionRates", {}).get("taker") if isinstance(account.get("commissionRates"), dict) else None
            facts["currency"] = str(market.get("quoteAsset") or "")
            facts["orderability"] = ("MARKET_AND_ACCOUNT_ENABLED" if market.get("status") == "TRADING" and account.get("canTrade") is True
                else "BLOCKED" if market.get("status") not in (None, "TRADING") or account.get("canTrade") is False else "UNKNOWN")
            if provider == "binance":
                for field, asset in (("_quote", market.get("quoteAsset")), ("_base", market.get("baseAsset"))):
                    rows = [item for item in account["balances"] if item.get("asset") == asset]
                    if len(rows) == 1:
                        facts[field] = _complete_number(rows[0].get("free"))
        except ReadFailure:
            pass
        return facts

    def _upbit(self, symbol):
        def get(path, params=None):
            query = params or {}
            authorization = build_upbit_authorization(self.settings["UPBIT_ACCESS_KEY"], self.settings["UPBIT_SECRET_KEY"], query)
            return self._get("upbit", path, query=query, headers={"Authorization": authorization})[0]
        account = get("/v1/accounts")
        if not isinstance(account, list):
            raise ReadFailure("INVALID_ACCOUNT_RESPONSE")
        facts = {"account": "AVAILABLE", "openOrders": "UNKNOWN", "orderability": "UNKNOWN", "currency": symbol.split("-")[0]}
        try:
            wait = get("/v1/orders/open", {"market": symbol, "state": "wait", "limit": 100})
            watch = get("/v1/orders/open", {"market": symbol, "state": "watch", "limit": 100})
            orders = wait + watch if isinstance(wait, list) and isinstance(watch, list) else None
            if isinstance(orders, list):
                facts["openOrders"] = "PRESENT" if orders else "SYMBOL_NONE_OBSERVED"
                facts["openOrdersScope"] = "SELECTED_SYMBOL_WAIT_AND_WATCH"
        except ReadFailure:
            pass
        try:
            chance = get("/v1/orders/chance", {"market": symbol})
            if isinstance(chance, dict) and isinstance(chance.get("market"), dict):
                facts["orderability"] = "MARKET_AND_ACCOUNT_ENABLED" if chance["market"].get("state") == "active" else "BLOCKED"
                facts["_quote"] = _complete_number((chance.get("bid_account") or {}).get("balance"))
                facts["_base"] = _complete_number((chance.get("ask_account") or {}).get("balance"))
                facts["_chance"] = chance["market"]
                facts["_fee"] = chance.get("bid_fee")
        except ReadFailure:
            pass
        return facts

    def _kis_token(self):
        import hashlib
        key = hashlib.sha256((self.settings["KIS_APP_KEY"] + "\0" + self.settings["KIS_APP_SECRET"]).encode()).hexdigest()
        with _TOKEN_LOCK:
            cached = _TOKEN_CACHE.get(key)
            if cached and cached[1] > time.monotonic():
                return cached[0]
            self._pace_kis()
            payload, _ = self.transport.request("kis", "POST", "/oauth2/tokenP",
                headers={"content-type": "application/json"}, body={"grant_type": "client_credentials",
                "appkey": self.settings["KIS_APP_KEY"], "appsecret": self.settings["KIS_APP_SECRET"]})
            if not isinstance(payload, dict) or not isinstance(payload.get("access_token"), str) or not payload["access_token"]:
                raise ReadFailure("AUTH_REFRESH_REQUIRED")
            _TOKEN_CACHE.clear()  # Bounded, memory only; never touch operational token caches.
            _TOKEN_CACHE[key] = (payload["access_token"], time.monotonic() + max(1, min(86400, int(payload.get("expires_in") or 60)) - 60))
            return payload["access_token"]

    @staticmethod
    def _pace_kis():
        global _KIS_LAST
        with _KIS_LOCK:
            time.sleep(max(0, 2.1 - (time.monotonic() - _KIS_LAST)))
            _KIS_LAST = time.monotonic()

    def _kis(self, lane, symbol):
        if self.settings.get("KIS_ENV", "").lower() not in ("", "real", "live"):
            raise ReadFailure("LIVE_ACCOUNT_ENVIRONMENT_REQUIRED")
        token = self._kis_token()
        cano, product = split_kis_account(self.settings["KIS_ACCOUNT_NO"], self.settings["KIS_ACCOUNT_PRODUCT_CODE"])
        overseas = lane == "kis-us"
        balance_path = "/uapi/" + ("overseas" if overseas else "domestic") + "-stock/v1/trading/inquire-balance"
        pending_path = "/uapi/overseas-stock/v1/trading/inquire-nccs" if overseas else "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
        context = "200" if overseas else "100"
        def pages(path, extra):
            rows = []; seen = set(); fk = nk = ""
            for index in range(20):
                params = {"CANO": cano, "ACNT_PRDT_CD": product, **extra,
                          "CTX_AREA_FK" + context: fk, "CTX_AREA_NK" + context: nk}
                headers = {"authorization": "Bearer " + token, "appkey": self.settings["KIS_APP_KEY"],
                           "appsecret": self.settings["KIS_APP_SECRET"], "tr_id": KIS_TR_IDS[path], "custtype": "P"}
                if index:
                    headers["tr_cont"] = "N"
                self._pace_kis()
                payload, response_headers = self._get("kis", path, query=params, headers=headers)
                if not isinstance(payload, dict) or str(payload.get("rt_cd")) != "0":
                    raise ReadFailure("BROKER_READ_REJECTED")
                output = payload.get("output1" if path == balance_path else "output")
                if not isinstance(output, list):
                    raise ReadFailure("INVALID_ACCOUNT_RESPONSE")
                rows.extend(output)
                if response_headers.get("tr_cont", "").strip().upper() not in ("F", "M"):
                    return rows
                fk, nk = str(payload.get("ctx_area_fk" + context) or ""), str(payload.get("ctx_area_nk" + context) or "")
                if not (fk or nk) or (fk, nk) in seen:
                    raise ReadFailure("INCOMPLETE_ACCOUNT_RESPONSE")
                seen.add((fk, nk))
            raise ReadFailure("INCOMPLETE_ACCOUNT_RESPONSE")
        balance_params = {"OVRS_EXCG_CD": "NASD", "TR_CRCY_CD": "USD"} if overseas else {
            "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00"}
        rows = pages(balance_path, balance_params)
        facts = {"account": "AVAILABLE", "openOrders": "UNKNOWN", "orderability": "UNKNOWN", "currency": "USD" if overseas else "KRW"}
        try:
            pending = pages(pending_path, {"OVRS_EXCG_CD": "NASD", "SORT_SQN": "DS"} if overseas else {"INQR_DVSN_1": "0", "INQR_DVSN_2": "0"})
            facts["openOrders"] = "PRESENT" if pending else "NONE_OBSERVED"
        except ReadFailure:
            pass
        facts["openOrdersScope"] = "KIS_NASD_QUERY" if overseas else "KIS_DOMESTIC_ACCOUNT"
        facts["exchangeBinding"] = "UNKNOWN" if overseas else "DOMESTIC"
        # Balance total is not orderable cash. KIS order-chance remains explicitly unknown.
        matches = [row for row in rows if str(row.get("ovrs_pdno" if overseas else "pdno") or "") == symbol]
        if len(matches) == 1 and not overseas:
            facts["_base"] = decimal(matches[0].get("ord_psbl_qty" if not overseas else "ord_psbl_qty"))
        return facts
