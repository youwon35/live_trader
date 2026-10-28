"""Official read-only observations for short Live functional sessions.

Settings are supplied by the genuine app, never loaded on import. No order,
margin, leverage, transfer or other account mutation endpoint is reachable.
The sole POST is the existing KIS OAuth authentication boundary.
"""
from __future__ import annotations

from .futures_leverage_limits import MAX_SAFE_LEVERAGE_INTEGER, leverage_integer

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo
from typing import Any

from .short_live_brokers import ROUTES, ShortLiveBrokerBlocked, number, decimal_text
from .short_live_read_diagnostics import DIAGNOSTIC_PATHS, sanitize_read_diagnostics


# Exact codes produced by account_currency_limits.py. Arbitrary ValueError
# messages (including malformed HTTP bodies) never become public blockers.
_FX_VALIDATION_CODES = frozenset({
    "NATIVE_FX_TIME_INVALID", "NATIVE_FX_EXACT_USDT_MARKET_REQUIRED",
    "NATIVE_FX_EXACT_USDT_TRADE_TIME_REQUIRED", "NATIVE_FX_HTTP_CLOCK_PROOF_MISSING",
    "NATIVE_FX_HTTP_CLOCK_HEADER_BINDING_CHANGED", "NATIVE_FX_CACHED_HTTP_RESPONSE_REJECTED",
    "NATIVE_FX_OFFICIAL_HTTP_DATE_MISSING_OR_INVALID", "NATIVE_FX_HTTP_CLOCK_LOCAL_BINDING_CHANGED",
    "NATIVE_FX_HTTP_CLOCK_RTT_INVALID", "NATIVE_FX_HTTP_CLOCK_SKEW_OR_RTT_EXCEEDED",
    "NATIVE_FX_OFFICIAL_CURRENCY_ROUTE_UNAVAILABLE", "NATIVE_FX_OBSERVATION_MISSING",
    "NATIVE_FX_OBSERVATION_SEAL_INVALID", "NATIVE_FX_OBSERVATION_CURRENCY_OR_ROUTE_MISMATCH",
    "NATIVE_FX_OFFICIAL_RESPONSE_BINDING_CHANGED", "NATIVE_FX_OFFICIAL_RESPONSE_SCOPE_CHANGED",
    "NATIVE_FX_RATE_RESPONSE_CHANGED", "NATIVE_FX_TIME_RESPONSE_CHANGED",
    "NATIVE_FX_OBSERVATION_STALE_OR_FUTURE",
})


def _safe_broker_code(payload):
    value = payload.get("code") if isinstance(payload, Mapping) else None
    return value if type(value) is int and -2147483648 <= value < 0 else None


def _safe_read_failure(exc):
    """Inspect types/numeric status only, including sanitized wrapper causes."""
    pending, seen = [exc], set()
    chain = []
    while pending and len(chain) < 8:
        item = pending.pop(0)
        if not isinstance(item, BaseException) or id(item) in seen:
            continue
        seen.add(id(item)); chain.append(item)
        pending.extend((item.__cause__, item.__context__, getattr(item, "reason", None)))
    http = next((item.code for item in chain if isinstance(item, urllib.error.HTTPError)
                 and type(item.code) is int and 100 <= item.code <= 599), None)
    # Boundary exceptions may inherit ValueError. Classify their fixed code
    # before generic JSON/value failures; the message itself never escapes.
    if str(exc) in {"READ_ONLY_ENDPOINT_BLOCKED", "READ_ONLY_TR_ID_BLOCKED",
                    "CONFIGURED_ORIGIN_BLOCKED", "REDIRECT_BLOCKED"}:
        return {"status": "BLOCKED", "failureKind": "READ_BOUNDARY"}
    if str(exc) in {"BROKER_READ_REJECTED", "OFFICIAL_PRIVATE_READ_REJECTED", "OFFICIAL_READ_REJECTED"}:
        return {"status": "REJECTED", "failureKind": "READ_REJECTED"}
    if any(isinstance(item, TimeoutError) for item in chain):
        result = {"status": "UNAVAILABLE", "failureKind": "TIMEOUT"}
    elif http is not None:
        result = {"status": "REJECTED", "failureKind": "HTTP_ERROR", "httpStatus": http}
    elif any(isinstance(item, (urllib.error.URLError, ConnectionError, OSError)) for item in chain):
        result = {"status": "UNAVAILABLE", "failureKind": "CONNECTION_ERROR"}
    elif any(isinstance(item, (json.JSONDecodeError, ValueError, TypeError, KeyError)) for item in chain):
        result = {"status": "BLOCKED", "failureKind": "INVALID_RESPONSE"}
    else:
        # Only compare to a fixed vocabulary. Never copy exception text.
        code = str(exc)
        if code in {"READ_ONLY_ENDPOINT_BLOCKED", "READ_ONLY_TR_ID_BLOCKED",
                    "CONFIGURED_ORIGIN_BLOCKED", "REDIRECT_BLOCKED"}:
            result = {"status": "BLOCKED", "failureKind": "READ_BOUNDARY"}
        elif code in {"BROKER_READ_REJECTED", "OFFICIAL_PRIVATE_READ_REJECTED", "OFFICIAL_READ_REJECTED"}:
            result = {"status": "REJECTED", "failureKind": "READ_REJECTED"}
        elif code in {"READ_UNAVAILABLE", "HTTP_ERROR"}:
            result = {"status": "UNAVAILABLE", "failureKind": "READ_UNAVAILABLE"}
        else:
            result = {"status": "BLOCKED", "failureKind": "UNEXPECTED_ERROR"}
    return result


class ProductionShortLiveReaders:
    def __init__(self, *, settings: Mapping[str, Any], fx_reader: Callable[..., Mapping[str, Any]] | None = None,
                 ownership_reader: Callable[..., Mapping[str, Any]] | None = None,
                 cost_reader: Callable[..., Mapping[str, Any]] | None = None,
                 clock: Callable[[], float] = time.time, reader: Any = None) -> None:
        self.settings = dict(settings)
        self._read_diagnostics: list[dict[str, Any]] = []
        self.ownership_reader, self.cost_reader, self.clock = ownership_reader, cost_reader, clock
        if reader is None:
            from .read_only_account import ReadOnlyBrokerReader, ReadOnlyTransport
            from .read_only_account import ORIGINS, NoRedirect
            class ExtendedReadTransport(ReadOnlyTransport):
                EXTRA = {"/fapi/v3/positionRisk", "/fapi/v1/positionSide/dual", "/fapi/v1/symbolConfig",
                         "/fapi/v1/commissionRate", "/fapi/v1/premiumIndex", "/fapi/v1/accountConfig",
                         "/fapi/v1/order", "/fapi/v1/openAlgoOrders", "/fapi/v1/userTrades",
                         "/fapi/v1/algoOrder", "/fapi/v1/forceOrders", "/fapi/v1/leverageBracket"}
                def request(self, provider, method, path, *, query=None, headers=None, body=None):
                    self.last_broker_code = None
                    extra_allowed = ((provider == "binance-futures" and path in self.EXTRA)
                                     or (provider == "upbit" and path == "/v1/orderbook/instruments"))
                    if not extra_allowed:
                        result = super().request(provider, method, path, query=query, headers=headers, body=body)
                        self.last_broker_code = _safe_broker_code(result[0])
                        return result
                    if method != "GET" or body is not None:
                        raise ShortLiveBrokerBlocked("READ_ONLY_ENDPOINT_BLOCKED")
                    encoded = query if isinstance(query, str) else urllib.parse.urlencode(query or {})
                    url = ORIGINS[provider] + path + ("?" + encoded if encoded else "")
                    req = urllib.request.Request(url, headers=headers or {}, method="GET")
                    with self.opener.open(req, timeout=self.timeout) as response:
                        if response.geturl() != url or not 200 <= response.status < 300:
                            raise ShortLiveBrokerBlocked("OFFICIAL_READ_REJECTED")
                        raw = response.read(4 * 1024 * 1024 + 1)
                        if len(raw) > 4 * 1024 * 1024:
                            raise ShortLiveBrokerBlocked("OFFICIAL_READ_TOO_LARGE")
                        payload = json.loads(raw)
                        self.last_broker_code = _safe_broker_code(payload)
                        return payload, dict(response.headers)
            reader = ReadOnlyBrokerReader(self.settings, transport=ExtendedReadTransport())
        self.reader = reader
        self.fx_reader = fx_reader or self._official_fx
        self.cost_reader = cost_reader or self._configured_kis_costs

    def _now(self) -> str:
        return datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()

    def _record_read(self, provider, path, started, *, exc=None, broker_code=None):
        safe_provider = provider if provider in DIAGNOSTIC_PATHS else "UNKNOWN_PROVIDER"
        safe_path = path if path in DIAGNOSTIC_PATHS.get(provider, ()) else "UNLISTED_ENDPOINT"
        row = {"provider": safe_provider, "path": safe_path,
               "elapsedMs": round(max(0.0, time.monotonic() - started) * 1000, 3),
               "status": "AVAILABLE"}
        if exc is not None:
            row.update(_safe_read_failure(exc))
        if type(broker_code) is int and -2147483648 <= broker_code < 0:
            row.update(status="REJECTED", failureKind="READ_REJECTED", brokerCode=broker_code)
        self._read_diagnostics.append(row)
        del self._read_diagnostics[:-32]

    def _read(self, provider, path, **kwargs):
        started = time.monotonic()
        transport = getattr(self.reader, "transport", None)
        if transport is not None and hasattr(transport, "last_broker_code"):
            transport.last_broker_code = None
        try:
            value, headers = self.reader._get(provider, path, **kwargs)
        except Exception as exc:
            transport = getattr(self.reader, "transport", None)
            self._record_read(provider, path, started, exc=exc,
                              broker_code=getattr(transport, "last_broker_code", None))
            raise
        self._record_read(provider, path, started, broker_code=_safe_broker_code(value))
        return value, headers

    def _fx(self, currency):
        started = time.monotonic()
        try:
            value = dict(self.fx_reader(currency))
            if value.get("known") is not True:
                raise ShortLiveBrokerBlocked("FRESH_FX_REQUIRED")
        except Exception as exc:
            # The production USDT conversion reader has this exact endpoint.
            self._record_read("upbit", "/v1/ticker", started, exc=exc)
            if isinstance(exc, ValueError) and str(exc) in _FX_VALIDATION_CODES:
                raise ShortLiveBrokerBlocked(str(exc)) from None
            raise
        self._record_read("upbit", "/v1/ticker", started)
        return value

    def _failure_reads(self):
        return sanitize_read_diagnostics(self._read_diagnostics)

    def _inventory_price(self, route, symbol):
        """Value existing spot units with one exact public native-price read.

        Account collection does not need order filters, commissions or FX for
        this valuation. Full order readiness still reads them independently.
        Avoid spending the account's 30-second freshness budget on duplicate
        trading-rule requests; never cache or relabel an earlier observation.
        """
        started = self.clock()
        if route == "BINANCE_SPOT":
            quote = self._binance("binance", "/api/v3/ticker/price", {"symbol": symbol}, signed=False)
            if not isinstance(quote, Mapping) or quote.get("symbol") != symbol:
                raise ShortLiveBrokerBlocked("EXACT_MARKET_TRUTH_REQUIRED")
            price = quote.get("price")
        elif route == "UPBIT_KRW_SPOT":
            rows = self._upbit("/v1/ticker", {"markets": symbol}, signed=False)
            if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping) or rows[0].get("market") != symbol:
                raise ShortLiveBrokerBlocked("EXACT_MARKET_TRUTH_REQUIRED")
            quote = rows[0]
            if type(quote.get("trade_timestamp")) is not int or type(quote.get("timestamp")) is not int:
                raise ShortLiveBrokerBlocked("UPBIT_QUOTE_SOURCE_TIME_UNKNOWN")
            age = self.clock() - min(quote["trade_timestamp"], quote["timestamp"]) / 1000
            if age < -2 or age > 10:
                raise ShortLiveBrokerBlocked("SOURCE_QUOTE_OBSERVATION_STALE")
            price = quote.get("trade_price")
        else:
            raise ShortLiveBrokerBlocked("EXACT_SPOT_ROUTE_REQUIRED")
        if not -2 <= self.clock() - started <= 10:
            raise ShortLiveBrokerBlocked("MARKET_OBSERVATION_STALE")
        return number(price, positive=True)

    @staticmethod
    def _positive_id(value: Any, code: str) -> str:
        if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]*", str(value)):
            raise ShortLiveBrokerBlocked(code)
        return str(value)

    @staticmethod
    def _millis(value: Any, code: str) -> int:
        if type(value) is not int or value < 0:
            raise ShortLiveBrokerBlocked(code)
        return value

    @staticmethod
    def _bool(value: Any, code: str) -> bool:
        if type(value) is bool:
            return value
        if value in ("true", "false"):
            return value == "true"
        raise ShortLiveBrokerBlocked(code)

    @staticmethod
    def _symbol(value: Any) -> str:
        if not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9_]{1,35}USDT", value):
            raise ShortLiveBrokerBlocked("FUTURES_SYMBOL_INVALID")
        return value

    @classmethod
    def _algo_order(cls, row: Any) -> dict[str, Any]:
        """Normalize conditional truth without inventing ownership or a fill."""
        if not isinstance(row, Mapping):
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_ORDER_INVALID")
        algo_id = cls._positive_id(row.get("algoId"), "FUTURES_ALGO_ID_INVALID")
        client = row.get("clientAlgoId")
        if not isinstance(client, str) or not client or len(client) > 128:
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_CLIENT_ID_INVALID")
        symbol = cls._symbol(row.get("symbol"))
        side, position_side = row.get("side"), row.get("positionSide")
        if side not in {"BUY", "SELL"} or position_side not in {"BOTH", "LONG", "SHORT"}:
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_SIDE_INVALID")
        kind = row.get("orderType", row.get("type"))
        if kind not in {"STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"}:
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_TYPE_UNKNOWN")
        status = row.get("algoStatus", row.get("status"))
        if status not in {"NEW", "CANCELED", "CANCELLED", "TRIGGERING", "TRIGGERED", "FINISHED", "REJECTED", "EXPIRED"}:
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_STATUS_UNKNOWN")
        close = cls._bool(row.get("closePosition"), "FUTURES_ALGO_CLOSE_POSITION_UNKNOWN")
        reduce = cls._bool(row.get("reduceOnly"), "FUTURES_ALGO_REDUCE_ONLY_UNKNOWN")
        quantity = row.get("quantity")
        if quantity is not None:
            quantity = number(quantity)
            if quantity < 0 or (not close and quantity <= 0):
                raise ShortLiveBrokerBlocked("FUTURES_ALGO_QUANTITY_INVALID")
        elif not close:
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_QUANTITY_UNKNOWN")
        trigger = number(row.get("triggerPrice"))
        if trigger < 0 or (kind != "TRAILING_STOP_MARKET" and trigger <= 0):
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_TRIGGER_INVALID")
        working = row.get("workingType")
        if working not in {"MARK_PRICE", "CONTRACT_PRICE"}:
            raise ShortLiveBrokerBlocked("FUTURES_ALGO_WORKING_TYPE_UNKNOWN")
        actual_id = row.get("actualOrderId")
        if actual_id in (None, "", 0, "0") and not isinstance(actual_id, bool):
            actual_id = ""
        else:
            actual_id = cls._positive_id(actual_id, "FUTURES_ALGO_ACTUAL_ORDER_ID_INVALID")
        result = {"algoId": algo_id, "clientAlgoId": client, "symbol": symbol,
                  "side": side, "positionSide": position_side, "type": kind,
                  "orderType": kind, "status": status, "algoStatus": status,
                  "quantity": None if quantity is None else decimal_text(quantity),
                  "triggerPrice": decimal_text(trigger), "workingType": working,
                  "closePosition": close, "reduceOnly": reduce, "actualOrderId": actual_id}
        for field in ("createTime", "updateTime", "triggerTime"):
            if field in row:
                result[field] = cls._millis(row[field], "FUTURES_ALGO_TIME_INVALID")
        if "priceProtect" in row:
            result["priceProtect"] = cls._bool(row["priceProtect"], "FUTURES_ALGO_PRICE_PROTECT_UNKNOWN")
        return result

    @classmethod
    def _symbol_config(cls, rows: Any, symbol: str) -> dict[str, Any]:
        if not isinstance(rows, list):
            raise ShortLiveBrokerBlocked("FUTURES_CONFIG_UNKNOWN")
        matches = [row for row in rows if isinstance(row, Mapping) and row.get("symbol") == symbol]
        if len(matches) != 1:
            raise ShortLiveBrokerBlocked("FUTURES_CONFIG_UNKNOWN")
        row = matches[0]
        leverage = number(row.get("leverage"), positive=True)
        margin = str(row.get("marginType") or "").upper()
        if leverage != leverage.to_integral_value() or not 1 <= leverage <= MAX_SAFE_LEVERAGE_INTEGER or margin not in {"ISOLATED", "CROSSED"}:
            raise ShortLiveBrokerBlocked("FUTURES_CONFIG_UNKNOWN")
        result = {"leverage": decimal_text(leverage), "marginMode": margin}
        if row.get("maxNotionalValue") is not None:
            result["maxNotionalValue"] = decimal_text(number(row["maxNotionalValue"], positive=True))
        if row.get("isAutoAddMargin") is not None:
            result["isAutoAddMargin"] = cls._bool(row["isAutoAddMargin"], "FUTURES_AUTO_ADD_MARGIN_UNKNOWN")
        return result

    @classmethod
    def _futures_position(cls, row: Any, configs: Any) -> dict[str, Any] | None:
        if not isinstance(row, Mapping):
            raise ShortLiveBrokerBlocked("FUTURES_POSITION_INVALID")
        symbol, position_side = cls._symbol(row.get("symbol")), row.get("positionSide")
        qty = number(row.get("positionAmt"))
        if position_side not in {"BOTH", "LONG", "SHORT"} or (position_side == "LONG" and qty < 0) or (position_side == "SHORT" and qty > 0):
            raise ShortLiveBrokerBlocked("FUTURES_POSITION_SIDE_INVALID")
        if qty == 0:
            return None
        result = {"symbol": symbol, "quantity": decimal_text(qty), "positionSide": position_side,
                  "positionDirection": "short" if qty < 0 else "long", **cls._symbol_config(configs, symbol)}
        for field in ("markPrice", "entryPrice", "liquidationPrice", "isolatedMargin", "isolatedWallet",
                      "initialMargin", "positionInitialMargin", "openOrderInitialMargin", "maintMargin"):
            value = number(row.get(field), positive=field in {"markPrice", "entryPrice"})
            if field != "isolatedMargin" and value < 0:
                raise ShortLiveBrokerBlocked("FUTURES_POSITION_MARGIN_INVALID")
            result[field] = decimal_text(value)
        result["notional"] = decimal_text(number(row.get("notional")))
        result["unrealizedPnlNative"] = decimal_text(number(row.get("unRealizedProfit")))
        result["updateTime"] = cls._millis(row.get("updateTime"), "FUTURES_POSITION_TIME_INVALID")
        if row.get("marginAsset") != "USDT":
            raise ShortLiveBrokerBlocked("FUTURES_POSITION_MARGIN_ASSET_UNKNOWN")
        result["marginAsset"] = "USDT"
        return result

    @staticmethod
    def _leverage_brackets(payload: Any, symbol: str) -> tuple[list[dict[str, str]], str]:
        rows = payload if isinstance(payload, list) else [payload] if isinstance(payload, Mapping) else []
        selected = [row for row in rows if isinstance(row, Mapping) and row.get("symbol") == symbol]
        if len(selected) != 1 or not isinstance(selected[0].get("brackets"), list) or not selected[0]["brackets"]:
            raise ShortLiveBrokerBlocked("FUTURES_LEVERAGE_BRACKETS_UNKNOWN")
        coefficient = number(selected[0].get("notionalCoef", 1), positive=True)
        brackets = []
        for row in selected[0]["brackets"]:
            if not isinstance(row, Mapping):
                raise ShortLiveBrokerBlocked("FUTURES_LEVERAGE_BRACKET_INVALID")
            bracket = {key: number(row.get(key)) for key in
                       ("notionalFloor", "notionalCap", "maintMarginRatio", "cum", "initialLeverage")}
            for key in ("notionalFloor", "notionalCap", "cum"):
                bracket[key] *= coefficient
            if (bracket["notionalFloor"] < 0 or bracket["notionalCap"] <= bracket["notionalFloor"]
                    or not 0 < bracket["maintMarginRatio"] < 1 or bracket["cum"] < 0
                    or not 1 <= bracket["initialLeverage"] <= MAX_SAFE_LEVERAGE_INTEGER
                    or bracket["initialLeverage"] != bracket["initialLeverage"].to_integral_value()):
                raise ShortLiveBrokerBlocked("FUTURES_LEVERAGE_BRACKET_INVALID")
            brackets.append(bracket)
        brackets.sort(key=lambda row: row["notionalFloor"])
        previous_cap, previous_margin = Decimal(0), Decimal(0)
        for bracket in brackets:
            at_floor = bracket["notionalFloor"] * bracket["maintMarginRatio"] - bracket["cum"]
            tolerance = max(Decimal("0.00000001"), abs(previous_margin) * Decimal("0.000000001"))
            if (bracket["notionalFloor"] != previous_cap or at_floor < -tolerance
                    or abs(at_floor - previous_margin) > tolerance):
                raise ShortLiveBrokerBlocked("FUTURES_LEVERAGE_BRACKET_DISCONTINUITY")
            previous_cap = bracket["notionalCap"]
            previous_margin = previous_cap * bracket["maintMarginRatio"] - bracket["cum"]
        return [{key: decimal_text(value) for key, value in row.items()} for row in brackets], decimal_text(coefficient)

    def _official_fx(self, currency: str) -> dict[str, Any]:
        if currency != "USDT":
            return {"known": False}
        from .account_currency_limits import observe_upbit_usdt_krw, validate_conversion_observation
        observation = observe_upbit_usdt_krw(self.reader)
        now = datetime.fromtimestamp(self.clock(), timezone.utc)
        verified = validate_conversion_observation(observation, native_currency="USDT", now=now)
        return {"known": True, "krwPerQuote": verified["krwPerNativeCurrency"],
                "observedAt": verified["rateAsOf"], "source": "OFFICIAL_UPBIT_KRW_USDT",
                "contentHash": verified["contentHash"]}

    def _configured_kis_costs(self, route: str, symbol: str) -> dict[str, Any]:
        """No personal KIS tariff producer exists in the legacy application.

        Its public provisional fee profile explicitly denies quantity/order
        authority. A separately confirmed source with a fresh timestamp is
        therefore required; provisional defaults never silently become facts.
        """
        if self.settings.get("SHORT_LIVE_KIS_COST_CONFIRMED") not in (True, "true"):
            return {"known": False}
        source = str(self.settings.get("SHORT_LIVE_KIS_COST_SOURCE") or "").strip()
        symbols = [text.strip() for text in str(self.settings.get("SHORT_LIVE_KIS_COST_SYMBOLS") or "").split(",")]
        if symbol not in symbols or any(len(text) != 6 or not text.isdigit() for text in symbols):
            return {"known": False}
        from .short_live_brokers import epoch
        age = self.clock() - epoch(self.settings.get("SHORT_LIVE_KIS_COST_OBSERVED_AT"))
        fee = number(self.settings.get("SHORT_LIVE_KIS_FEE_RATE"))
        tax = number(self.settings.get("SHORT_LIVE_KIS_SELL_TAX_RATE"))
        if not source or not 0 <= age <= 86400 or not 0 <= fee <= Decimal("0.1") or not 0 <= tax <= Decimal("0.1"):
            return {"known": False}
        return {"known": True, "feeRate": decimal_text(fee), "sellTaxRate": decimal_text(tax),
                "source": source, "observedAt": self.settings["SHORT_LIVE_KIS_COST_OBSERVED_AT"]}

    def _identity(self, provider: str) -> str:
        from .account_profiles import saved_account_profiles
        key = "binance" if provider == "binance-futures" else provider
        profiles = [p for p in saved_account_profiles(self.settings) if p["brokerId"] == key]
        if len(profiles) != 1:
            raise ShortLiveBrokerBlocked("SAVED_ACCOUNT_IDENTITY_UNKNOWN")
        return str(profiles[0]["accountBindingId"])

    def _binance(self, provider: str, path: str, params=None, *, signed=True):
        from .live_adapters import sign_binance_query
        self.reader._credentials("binance")
        prefix = "/fapi/v1" if provider == "binance-futures" else "/api/v3"
        query = dict(params or {})
        headers = {}
        if signed:
            clock, _ = self._read(provider, prefix + "/time")
            if not isinstance(clock, dict) or isinstance(clock.get("serverTime"), bool):
                raise ShortLiveBrokerBlocked("OFFICIAL_SERVER_TIME_UNKNOWN")
            query.update(timestamp=int(clock["serverTime"]), recvWindow=5000)
            query = sign_binance_query(query, self.settings["BINANCE_API_SECRET"])
            headers = {"X-MBX-APIKEY": self.settings["BINANCE_API_KEY"]}
        payload, _ = self._read(provider, path, query=query, headers=headers)
        if isinstance(payload, dict) and payload.get("code") is not None and number(payload["code"]) < 0:
            raise ShortLiveBrokerBlocked("OFFICIAL_PRIVATE_READ_REJECTED")
        return payload

    def _upbit(self, path: str, params=None, *, signed=True):
        from .live_adapters import build_upbit_authorization
        query = dict(params or {})
        headers = {}
        if signed:
            self.reader._credentials("upbit")
            headers = {"Authorization": build_upbit_authorization(self.settings["UPBIT_ACCESS_KEY"], self.settings["UPBIT_SECRET_KEY"], query)}
        payload, _ = self._read("upbit", path, query=query, headers=headers)
        if isinstance(payload, dict) and "error" in payload:
            raise ShortLiveBrokerBlocked("OFFICIAL_PRIVATE_READ_REJECTED")
        return payload

    @staticmethod
    def _order(row: Mapping[str, Any], *, provider: str) -> dict[str, Any]:
        state = str(row.get("status") or row.get("state") or "UNKNOWN").upper()
        side = str(row.get("side") or "").upper()
        if side in {"BID", "ASK"}:
            side = "BUY" if side == "BID" else "SELL"
        quantity = row.get("origQty") if provider in {"binance", "binance-futures"} else row.get("volume")
        filled = number(row.get("executedQty", row.get("executed_volume", "0")))
        remaining = row.get("remaining_volume")
        if quantity is not None:
            quantity = number(quantity, positive=True)
            if filled < 0 or filled > quantity:
                raise ShortLiveBrokerBlocked("ORDER_TRUTH_QUANTITY_INVALID")
            remaining = quantity - filled if provider in {"binance", "binance-futures"} else remaining
        return {"symbol": str(row.get("symbol") or row.get("market") or ""),
                "brokerOrderId": str(row.get("orderId") or row.get("uuid") or ""),
                "clientOrderId": str(row.get("clientOrderId") or row.get("identifier") or ""),
                "status": state, "side": side,
                **({"positionSide": row.get("positionSide")} if provider == "binance-futures" else {}),
                "quantity": None if quantity is None else decimal_text(quantity),
                "filledQuantity": decimal_text(filled),
                "remainingQuantity": None if remaining is None else decimal_text(number(remaining)),
                "fillPrice": str(row.get("avgPrice") or row.get("price") or "0"),
                "feeNative": row.get("paid_fee"), "provider": provider}

    def _crypto_snapshot(self, route: str, symbols: tuple[str, ...]) -> dict[str, Any]:
        provider, _, currency = ROUTES[route]
        positions: list[dict[str, Any]] = []
        inventory: dict[str, str] = {}
        algo_orders: list[dict[str, Any]] = []

        def spot_balance(asset: Any, free: Any, locked: Any) -> tuple[str, Decimal]:
            # Retain even out-of-scope/delisted dust as quantity-only inventory.
            # It is neither buying power nor a position owned by this session.
            if not isinstance(asset, str) or not asset or asset in inventory:
                raise ShortLiveBrokerBlocked("SPOT_INVENTORY_INVALID")
            free_qty, locked_qty = number(free), number(locked)
            if free_qty < 0 or locked_qty < 0:
                raise ShortLiveBrokerBlocked("SPOT_INVENTORY_INVALID")
            qty = free_qty + locked_qty
            inventory[asset] = decimal_text(qty)
            return (asset + "USDT" if provider == "binance" else "KRW-" + asset), qty
        if provider in {"binance", "binance-futures"}:
            futures = provider == "binance-futures"
            account = self._binance(provider, "/fapi/v3/account" if futures else "/api/v3/account")
            rows = account.get("assets" if futures else "balances") if isinstance(account, dict) else None
            if not isinstance(rows, list):
                raise ShortLiveBrokerBlocked("ACCOUNT_ROWS_INCOMPLETE")
            usd = [r for r in rows if r.get("asset") == "USDT"]
            if len(usd) != 1:
                raise ShortLiveBrokerBlocked("QUOTE_ACCOUNT_UNKNOWN")
            quote = usd[0]
            available = number(quote.get("availableBalance") if futures else quote.get("free"))
            equity = number(quote.get("marginBalance") if futures else quote.get("free"))
            if futures:
                config = self._binance(provider, "/fapi/v1/accountConfig")
                if config.get("canTrade") is not True:
                    raise ShortLiveBrokerBlocked("ACCOUNT_TRADING_DISABLED")
                if config.get("multiAssetsMargin") is not False:
                    raise ShortLiveBrokerBlocked("FUTURES_SINGLE_ASSET_MODE_REQUIRED")
                if any(row.get("asset") != "USDT" and (
                        number(row.get("walletBalance")) != 0 or number(row.get("marginBalance")) != 0) for row in rows):
                    raise ShortLiveBrokerBlocked("UNSCOPED_FUTURES_ASSET_PRESENT")
                raw_positions = self._binance(provider, "/fapi/v3/positionRisk")
                if not isinstance(raw_positions, list):
                    raise ShortLiveBrokerBlocked("POSITIONS_INCOMPLETE")
                configs = (self._binance(provider, "/fapi/v1/symbolConfig")
                           if any(number(row.get("positionAmt")) != 0 for row in raw_positions) else [])
                seen_positions = set()
                for row in raw_positions:
                    position = self._futures_position(row, configs)
                    if position is not None:
                        key = (position["symbol"], position["positionSide"])
                        if key in seen_positions:
                            raise ShortLiveBrokerBlocked("FUTURES_POSITION_IDENTITY_CONFLICT")
                        seen_positions.add(key)
                        positions.append(position)
                # Regular orders and conditional algo orders are distinct.
                raw_orders = self._binance(provider, "/fapi/v1/openOrders")
                algo = self._binance(provider, "/fapi/v1/openAlgoOrders")
                algo_rows = algo.get("orders") if isinstance(algo, dict) else algo
                if not isinstance(algo_rows, list):
                    raise ShortLiveBrokerBlocked("FUTURES_CONDITIONAL_ORDERS_UNKNOWN")
                algo_orders = [self._algo_order(row) for row in algo_rows]
                algo_keys = [(row["symbol"], row["algoId"]) for row in algo_orders]
                if len(set(algo_keys)) != len(algo_keys):
                    raise ShortLiveBrokerBlocked("FUTURES_ALGO_IDENTITY_CONFLICT")
            else:
                if account.get("canTrade") is not True:
                    raise ShortLiveBrokerBlocked("ACCOUNT_TRADING_DISABLED")
                locked_quote = number(quote.get("locked"))
                if available < 0 or locked_quote < 0:
                    raise ShortLiveBrokerBlocked("QUOTE_BALANCE_INVALID")
                equity += locked_quote
                for row in rows:
                    if row.get("asset") == "USDT":
                        continue
                    symbol, qty = spot_balance(row.get("asset"), row.get("free"), row.get("locked"))
                    if qty and symbol in symbols:
                        price = self._inventory_price(route, symbol)
                        equity += qty * price
                        positions.append({"symbol": symbol, "quantity": decimal_text(qty), "markPrice": decimal_text(price)})
                raw_orders = self._binance(provider, "/api/v3/openOrders")
            if not isinstance(raw_orders, list):
                raise ShortLiveBrokerBlocked("OPEN_ORDERS_INCOMPLETE")
            orders = [self._order(r, provider=provider) for r in raw_orders]
        else:
            rows = self._upbit("/v1/accounts")
            if not isinstance(rows, list):
                raise ShortLiveBrokerBlocked("ACCOUNT_ROWS_INCOMPLETE")
            quote = [r for r in rows if r.get("currency") == "KRW"]
            if len(quote) != 1:
                raise ShortLiveBrokerBlocked("QUOTE_ACCOUNT_UNKNOWN")
            available = number(quote[0].get("balance"))
            locked_quote = number(quote[0].get("locked"))
            if available < 0 or locked_quote < 0:
                raise ShortLiveBrokerBlocked("QUOTE_BALANCE_INVALID")
            equity = available + locked_quote
            for row in rows:
                if row.get("currency") == "KRW":
                    continue
                symbol, qty = spot_balance(row.get("currency"), row.get("balance"), row.get("locked"))
                if qty and symbol in symbols:
                    price = self._inventory_price(route, symbol)
                    equity += qty * price
                    positions.append({"symbol": symbol, "quantity": decimal_text(qty), "markPrice": decimal_text(price)})
            orders, seen = [], set()
            for state in ("wait", "watch"):
                for page in range(1, 21):
                    batch = self._upbit("/v1/orders/open", {"state": state, "page": page, "limit": 100})
                    if not isinstance(batch, list):
                        raise ShortLiveBrokerBlocked("OPEN_ORDERS_INCOMPLETE")
                    ids = [str(r.get("uuid") or "") for r in batch]
                    if any(not x or x in seen for x in ids):
                        raise ShortLiveBrokerBlocked("OPEN_ORDER_PAGINATION_CONFLICT")
                    seen.update(ids)
                    orders.extend(self._order(r, provider=provider) for r in batch)
                    if len(batch) < 100:
                        break
                else:
                    raise ShortLiveBrokerBlocked("OPEN_ORDER_PAGINATION_LIMIT")
        spot = ({"spotInventoryComplete": True, "spotInventory": inventory,
                 "equityScope": "QUOTE_AND_SELECTED_SPOT"}
                if provider in {"binance", "upbit"} else {})
        return {"known": True, "accountId": self._identity(provider), "currency": currency, **spot,
                "availableNative": decimal_text(available), "equityNative": decimal_text(equity),
                "positions": positions, "openOrders": orders, "ordersComplete": True,
                **({"openAlgoOrders": algo_orders, "algoOrdersComplete": True}
                   if provider == "binance-futures" else {}),
                "positionsComplete": True, "observedAt": self._now()}

    def _kis_snapshot(self, symbols: tuple[str, ...]) -> dict[str, Any]:
        from .live_adapters import split_kis_account
        from .read_only_account import KIS_TR_IDS
        self.reader._credentials("kis")
        token = self.reader._kis_token()
        cano, product = split_kis_account(self.settings["KIS_ACCOUNT_NO"], self.settings.get("KIS_ACCOUNT_PRODUCT_CODE", "01"))
        def pages(path, params, field):
            fk = nk = ""; seen = set(); rows = []; total = None
            for page in range(20):
                self.reader._pace_kis()
                payload, headers = self._read("kis", path, query={"CANO": cano, "ACNT_PRDT_CD": product,
                    **params, "CTX_AREA_FK100": fk, "CTX_AREA_NK100": nk}, headers={"authorization": "Bearer " + token,
                    "appkey": self.settings["KIS_APP_KEY"], "appsecret": self.settings["KIS_APP_SECRET"],
                    "tr_id": KIS_TR_IDS[path], "custtype": "P", **({"tr_cont": "N"} if page else {})})
                if not isinstance(payload, dict) or str(payload.get("rt_cd")) != "0" or not isinstance(payload.get(field), list):
                    raise ShortLiveBrokerBlocked("KIS_ACCOUNT_TRUTH_INCOMPLETE")
                rows.extend(payload[field])
                if payload.get("output2"):
                    totals = payload["output2"]
                    item = totals[0] if isinstance(totals, list) and len(totals) == 1 else totals
                    if not isinstance(item, dict):
                        raise ShortLiveBrokerBlocked("KIS_BALANCE_TOTAL_UNKNOWN")
                    value = number(item.get("tot_evlu_amt"))
                    if total is not None and total != value:
                        raise ShortLiveBrokerBlocked("KIS_BALANCE_CHANGED_DURING_PAGING")
                    total = value
                if str(headers.get("tr_cont", "")).upper() not in {"F", "M"}:
                    return rows, total
                fk, nk = str(payload.get("ctx_area_fk100") or ""), str(payload.get("ctx_area_nk100") or "")
                if not (fk or nk) or (fk, nk) in seen:
                    raise ShortLiveBrokerBlocked("KIS_PAGINATION_INCOMPLETE")
                seen.add((fk, nk))
            raise ShortLiveBrokerBlocked("KIS_PAGINATION_LIMIT")
        holdings, equity = pages("/uapi/domestic-stock/v1/trading/inquire-balance", {
            "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00"}, "output1")
        pending, _ = pages("/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl", {"INQR_DVSN_1": "0", "INQR_DVSN_2": "0"}, "output")
        if equity is None:
            raise ShortLiveBrokerBlocked("KIS_EQUITY_UNKNOWN")
        orderable = {symbol: self.reader._kis_cash("kis-kr", symbol, self.reader.kis_reference_price("kis-kr", symbol)) for symbol in symbols}
        available = min(number(row["_quote"]) for row in orderable.values())
        maximum_quantities = {symbol: decimal_text(number(row["_maximumQty"])) for symbol, row in orderable.items()}
        positions = [{"symbol": str(r.get("pdno")), "quantity": str(r.get("hldg_qty")), "markPrice": str(r.get("prpr"))}
                     for r in holdings if number(r.get("hldg_qty")) != 0]
        today = datetime.fromtimestamp(self.clock(), ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
        orders = [{"symbol": str(r.get("pdno")), "brokerOrderId": str(r.get("odno") or ""),
                   "clientOrderId": "", "status": "OPEN", "quantity": str(r.get("ord_qty")),
                   "remainingQuantity": str(r.get("psbl_qty")), "organizationNo": str(r.get("krx_fwdg_ord_orgno") or ""),
                   "orderDate": str(r.get("ord_dt") or ""), "side": "BUY" if str(r.get("sll_buy_dvsn_cd")) == "02" else "SELL"}
                  for r in pending]
        return {"known": True, "accountId": self._identity("kis"), "currency": "KRW", "availableNative": decimal_text(available),
                "equityNative": decimal_text(equity), "positions": positions, "openOrders": orders,
                "maximumOrderQuantities": maximum_quantities,
                "ordersComplete": True, "positionsComplete": True, "observedAt": self._now()}

    def snapshot(self, route: str, symbols: tuple[str, ...]) -> dict[str, Any]:
        self._read_diagnostics = []
        try:
            started = self._now()
            if route not in ROUTES or not symbols:
                raise ShortLiveBrokerBlocked("ROUTE_SYMBOL_REQUIRED")
            result = self._kis_snapshot(symbols) if route == "KIS_KR_CASH" else self._crypto_snapshot(route, symbols)
            if self.ownership_reader is not None:
                annotations = dict(self.ownership_reader(route, deepcopy_rows(result)))
                for field in ("positions", "openOrders", "realizedPnlNative", "unrealizedPnlNative"):
                    if field in annotations:
                        result[field] = annotations[field]
            result["observedAt"] = started
            result["readDiagnostics"] = self._failure_reads()
            return result
        except Exception as exc:
            return {"known": False, "ordersComplete": False, "positionsComplete": False,
                    "blockers": [str(exc) if isinstance(exc, ShortLiveBrokerBlocked) else "OFFICIAL_ACCOUNT_READ_UNKNOWN"],
                    "readDiagnostics": self._failure_reads()}

    def get_order(self, route: str, claim: Mapping[str, Any]) -> dict[str, Any]:
        """Read an exact reservation by broker ID or original futures client ID."""
        try:
            observed_started = self._now()
            provider = ROUTES[route][0]
            order_id, symbol = str(claim.get("brokerOrderId") or ""), str(claim.get("symbol") or "")
            if not symbol or (not order_id and provider != "binance-futures"):
                raise ShortLiveBrokerBlocked("EXACT_OWNED_ORDER_REQUIRED")
            if provider in {"binance", "binance-futures"}:
                futures = provider == "binance-futures"
                prefix = "/fapi/v1" if futures else "/api/v3"
                original_client = str(claim.get("clientOrderId") or "")
                if order_id:
                    query = {"symbol": symbol, "orderId": order_id}
                else:
                    if (claim.get("status") != "UNKNOWN"
                            or not re.fullmatch(r"[.A-Z:/a-z0-9_-]{1,36}", original_client)
                            or claim.get("side") not in {"BUY", "SELL"}
                            or claim.get("positionSide") not in {"BOTH", "LONG", "SHORT"}):
                        raise ShortLiveBrokerBlocked("EXACT_UNKNOWN_RESERVATION_REQUIRED")
                    number(claim.get("quantity"), positive=True)
                    query = {"symbol": symbol, "origClientOrderId": original_client}
                raw = self._binance(provider, prefix + "/order", query)
                if (not isinstance(raw, dict) or raw.get("symbol") != symbol
                        or (order_id and str(raw.get("orderId")) != order_id)):
                    raise ShortLiveBrokerBlocked("ORDER_TRUTH_ID_MISMATCH")
                order_id = self._positive_id(raw.get("orderId"), "ORDER_TRUTH_ID_MISMATCH")
                result = self._order(raw, provider=provider)
                for field in ("side", "positionSide"):
                    if claim.get(field) is not None and claim[field] != result.get(field):
                        raise ShortLiveBrokerBlocked("ORDER_TRUTH_SIDE_MISMATCH")
                expected_client = claim.get("actualClientOrderId") if claim.get("externalExecution") is True else claim.get("clientOrderId")
                if expected_client and result.get("clientOrderId") != expected_client:
                    raise ShortLiveBrokerBlocked("ORDER_TRUTH_CLIENT_ID_MISMATCH")
                if futures and claim.get("quantity") is not None and number(claim["quantity"], positive=True) != number(result.get("quantity"), positive=True):
                    raise ShortLiveBrokerBlocked("ORDER_TRUTH_QUANTITY_MISMATCH")
                result.update(baseCommissionQuantity="0", baseCommissionKnown=True,
                              externalCommissionQuantities={}, externalCommissionNative={},
                              externalCommissionKnown=True)
                filled = number(result["filledQuantity"])
                if filled:
                    trades = self._binance(provider, prefix + ("/userTrades" if futures else "/myTrades"),
                                           {"symbol": symbol, "orderId": order_id, "limit": 1000})
                    if not isinstance(trades, list) or len(trades) >= 1000:
                        raise ShortLiveBrokerBlocked("OWNED_TRADES_INCOMPLETE")
                    selected = [r for r in trades if str(r.get("orderId")) == order_id and r.get("symbol") == symbol]
                    trade_ids = [row.get("id") for row in selected]
                    if (any(isinstance(value, bool) or not str(value).isdigit() for value in trade_ids)
                            or len({str(value) for value in trade_ids}) != len(trade_ids)):
                        raise ShortLiveBrokerBlocked("OWNED_TRADE_IDENTITY_CONFLICT")
                    for row in selected:
                        number(row.get("qty"), positive=True)
                        number(row.get("price"), positive=True)
                    if sum((number(r.get("qty")) for r in selected), Decimal(0)) != filled:
                        raise ShortLiveBrokerBlocked("OWNED_TRADE_QUANTITY_MISMATCH")
                    result["fillPrice"] = decimal_text(sum((number(r.get("qty")) * number(r.get("price")) for r in selected), Decimal(0)) / filled)
                    commissions = Decimal(0)
                    base_commissions = Decimal(0)
                    for row in selected:
                        fee = number(row.get("commission"))
                        asset = str(row.get("commissionAsset") or "")
                        if fee < 0 or (fee and not asset):
                            raise ShortLiveBrokerBlocked("OWNED_TRADE_COMMISSION_INVALID")
                        if not futures and asset == symbol.removesuffix("USDT"):
                            base_commissions += fee
                        if asset == "USDT":
                            commissions += fee
                        elif fee == 0:
                            continue
                        else:
                            ticker = self._binance("binance", "/api/v3/ticker/price", {"symbol": asset + "USDT"}, signed=False)
                            if ticker.get("symbol") != asset + "USDT":
                                raise ShortLiveBrokerBlocked("COMMISSION_CONVERSION_SYMBOL_MISMATCH")
                            converted = fee * number(ticker.get("price"), positive=True)
                            commissions += converted
                            if not futures and asset != symbol.removesuffix("USDT"):
                                result["externalCommissionQuantities"][asset] = decimal_text(
                                    number(result["externalCommissionQuantities"].get(asset, 0)) + fee)
                                result["externalCommissionNative"][asset] = decimal_text(
                                    number(result["externalCommissionNative"].get(asset, 0)) + converted)
                    result["feeNative"] = decimal_text(commissions)
                    result["baseCommissionQuantity"] = decimal_text(base_commissions)
            elif provider == "upbit":
                raw = self._upbit("/v1/order", {"uuid": order_id})
                if not isinstance(raw, dict) or str(raw.get("uuid")) != order_id or raw.get("market") != symbol:
                    raise ShortLiveBrokerBlocked("ORDER_TRUTH_ID_MISMATCH")
                result = self._order(raw, provider=provider)
                result.update(baseCommissionQuantity="0", baseCommissionKnown=True)
                trades = raw.get("trades")
                filled = number(result["filledQuantity"])
                if filled:
                    if not isinstance(trades, list) or sum((number(r.get("volume")) for r in trades), Decimal(0)) != filled:
                        raise ShortLiveBrokerBlocked("OWNED_TRADES_INCOMPLETE")
                    result["fillPrice"] = decimal_text(sum((number(r.get("funds")) for r in trades), Decimal(0)) / filled)
                result["status"] = "FILLED" if result["status"] == "DONE" else "CANCELED" if result["status"] == "CANCEL" else result["status"]
            else:
                # The normal KIS reader verifies all continuation pages and
                # rejects conflicting official order-date/branch/ODNO joins.
                from .brokers import fetch_kis_domestic_order_truth, normalize_kis_domestic_order_truth
                token = self.reader._kis_token()
                today = datetime.fromtimestamp(self.clock(), ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
                from .kis_order_authority import kis_read_diagnostic_boundary
                with kis_read_diagnostic_boundary():
                    raw = normalize_kis_domestic_order_truth(fetch_kis_domestic_order_truth(token, start_date=today, end_date=today))
                matches = [r for r in raw["orders"] if str(r.get("broker_order_id")) == order_id
                           and str(r.get("symbol")).removesuffix(".KS").removesuffix(".KQ") == symbol]
                if len(matches) != 1 or order_id in raw.get("ambiguous_broker_order_ids", []):
                    raise ShortLiveBrokerBlocked("EXACT_KIS_ORDER_TRUTH_REQUIRED")
                row = matches[0]
                result = {"symbol": symbol, "brokerOrderId": order_id, "clientOrderId": "",
                          "status": str(row["state"]).upper(), "filledQuantity": str(row["filled_quantity"]),
                          "fillPrice": str(row["average_fill_price"]), "feeNative": None,
                          "orderDate": row["order_date"], "organizationNo": row["organization_no"],
                          "remainingQuantity": str(row["remaining_quantity"]), "side": str(row["side"]).upper()}
                result.update(baseCommissionQuantity="0", baseCommissionKnown=True)
            return {**result, "known": True, "observedAt": observed_started}
        except Exception:
            return {"known": False, "status": "UNKNOWN", "blockers": ["OFFICIAL_OWNED_ORDER_UNKNOWN"]}

    def protection_order(self, route: str, claim: Mapping[str, Any]) -> dict[str, Any]:
        """Read one exact algo and its actual execution; never creates an order."""
        observed = self._now()
        try:
            if route != "BINANCE_USDM_FUTURES" or not isinstance(claim, Mapping):
                raise ShortLiveBrokerBlocked("FUTURES_PROTECTION_ROUTE_INVALID")
            symbol = self._symbol(claim.get("symbol"))
            algo_id, client = claim.get("algoId"), claim.get("clientAlgoId")
            params = {}
            if algo_id is not None and algo_id != "":
                params["algoId"] = self._positive_id(algo_id, "FUTURES_ALGO_ID_INVALID")
            elif isinstance(client, str) and client and len(client) <= 128:
                params["clientAlgoId"] = client
            else:
                raise ShortLiveBrokerBlocked("EXACT_PROTECTION_ORDER_REQUIRED")
            raw = self._binance("binance-futures", "/fapi/v1/algoOrder", params)
            result = self._algo_order(raw)
            if (result["symbol"] != symbol or (params.get("algoId") and result["algoId"] != params["algoId"])
                    or (client is not None and client != "" and result["clientAlgoId"] != client)):
                raise ShortLiveBrokerBlocked("PROTECTION_ORDER_IDENTITY_MISMATCH")
            for key in ("side", "positionSide"):
                if claim.get(key) is not None and claim[key] != result[key]:
                    raise ShortLiveBrokerBlocked("PROTECTION_ORDER_SIDE_MISMATCH")
            actual_id = result["actualOrderId"]
            result.update(triggered=bool(actual_id), executionKnown=False, actualOrder=None)
            if actual_id:
                actual = self.get_order(route, {"brokerOrderId": actual_id, "symbol": symbol})
                if (actual.get("known") is not True or actual.get("symbol") != symbol
                        or str(actual.get("brokerOrderId")) != actual_id
                        or actual.get("side") != result["side"]
                        or actual.get("positionSide") != result["positionSide"]):
                    raise ShortLiveBrokerBlocked("PROTECTION_EXECUTION_UNKNOWN")
                filled = number(actual.get("filledQuantity"))
                if filled < 0 or actual.get("status") not in {
                        "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                    raise ShortLiveBrokerBlocked("PROTECTION_EXECUTION_UNKNOWN")
                # The exchange can create the MARKET child before its first
                # fill. Preserve that known zero-fill state without a fee.
                # Any positive execution still needs actual trade fees.
                if filled > 0:
                    if number(actual.get("feeNative")) < 0:
                        raise ShortLiveBrokerBlocked("PROTECTION_EXECUTION_FEE_UNKNOWN")
                    number(actual.get("fillPrice"), positive=True)
                result.update(actualOrder=actual, executionKnown=True)
            elif result["status"] in {"TRIGGERED", "FINISHED"}:
                raise ShortLiveBrokerBlocked("PROTECTION_EXECUTION_ID_MISSING")
            return {**result, "known": True, "observedAt": observed}
        except Exception as exc:
            return {"known": False, "executionKnown": False, "status": "UNKNOWN", "observedAt": observed,
                    "blockers": [str(exc) if isinstance(exc, ShortLiveBrokerBlocked) else "OFFICIAL_PROTECTION_ORDER_UNKNOWN"]}

    def futures_force_orders(self, symbol: str, since_ms: int) -> dict[str, Any]:
        """Bounded exact-symbol forced executions, queried only on reconciliation need.

        Types are queried independently because the response has no reliable
        autoCloseType field. A saturated page is incomplete, never an empty
        successful result. Unverified partial evidence remains attached.
        """
        observed = self._now()
        events: list[dict[str, Any]] = []
        try:
            symbol = self._symbol(symbol)
            start = self._millis(since_ms, "FORCE_ORDER_WINDOW_INVALID")
            end = int(self.clock() * 1000)
            if start <= 0 or start > end or end - start > 7 * 24 * 3600 * 1000:
                raise ShortLiveBrokerBlocked("FORCE_ORDER_WINDOW_INVALID")
            seen = set()
            for kind in ("LIQUIDATION", "ADL"):
                rows = self._binance("binance-futures", "/fapi/v1/forceOrders", {
                    "symbol": symbol, "autoCloseType": kind, "startTime": start, "endTime": end, "limit": 100})
                if not isinstance(rows, list):
                    raise ShortLiveBrokerBlocked("FORCE_ORDER_HISTORY_UNKNOWN")
                for row in rows:
                    if not isinstance(row, Mapping) or row.get("symbol") != symbol:
                        raise ShortLiveBrokerBlocked("FORCE_ORDER_SCOPE_MISMATCH")
                    order_id = self._positive_id(row.get("orderId"), "FORCE_ORDER_ID_INVALID")
                    if order_id in seen:
                        raise ShortLiveBrokerBlocked("FORCE_ORDER_IDENTITY_CONFLICT")
                    seen.add(order_id)
                    side, position_side = row.get("side"), row.get("positionSide")
                    if (side not in {"BUY", "SELL"} or position_side not in {"BOTH", "LONG", "SHORT"}
                            or (position_side == "LONG" and side != "SELL")
                            or (position_side == "SHORT" and side != "BUY")):
                        raise ShortLiveBrokerBlocked("FORCE_ORDER_SIDE_INVALID")
                    created = self._millis(row.get("time"), "FORCE_ORDER_TIME_INVALID")
                    updated = self._millis(row.get("updateTime"), "FORCE_ORDER_TIME_INVALID")
                    if not start <= created <= updated <= end:
                        raise ShortLiveBrokerBlocked("FORCE_ORDER_TIME_OUTSIDE_WINDOW")
                    quantity, filled = number(row.get("origQty"), positive=True), number(row.get("executedQty"))
                    price = number(row.get("avgPrice"), positive=filled > 0)
                    if filled < 0 or filled > quantity or price < 0:
                        raise ShortLiveBrokerBlocked("FORCE_ORDER_FILL_INVALID")
                    status = row.get("status")
                    if status not in {"NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                        raise ShortLiveBrokerBlocked("FORCE_ORDER_STATUS_UNKNOWN")
                    client = row.get("clientOrderId")
                    if not isinstance(client, str) or not client:
                        raise ShortLiveBrokerBlocked("FORCE_ORDER_CLIENT_ID_UNKNOWN")
                    events.append({"incidentType": kind, "autoCloseType": kind, "brokerOrderId": order_id,
                                   "clientOrderId": client, "symbol": symbol, "side": side, "positionSide": position_side,
                                   "status": status, "quantity": decimal_text(quantity), "filledQuantity": decimal_text(filled),
                                   "fillPrice": decimal_text(price), "time": created, "updateTime": updated})
                if len(rows) >= 100:
                    raise ShortLiveBrokerBlocked("FORCE_ORDER_HISTORY_INCOMPLETE")
            events.sort(key=lambda row: (row["time"], row["brokerOrderId"]))
            return {"known": True, "events": events, "observedAt": observed, "sinceMs": start, "untilMs": end}
        except Exception as exc:
            return {"known": False, "events": events, "observedAt": observed,
                    "blockers": [str(exc) if isinstance(exc, ShortLiveBrokerBlocked) else "OFFICIAL_FORCE_ORDER_UNKNOWN"]}

    def market(self, route: str, symbol: str) -> dict[str, Any]:
        self._read_diagnostics = []
        try:
            provider, _, currency = ROUTES[route]
            extra: dict[str, Any] = {}
            if provider in {"binance", "binance-futures"}:
                futures = provider == "binance-futures"
                prefix = "/fapi/v1" if futures else "/api/v3"
                info = self._binance(provider, prefix + "/exchangeInfo", {"symbol": symbol} if not futures else {}, signed=False)
                matches = [r for r in info.get("symbols", []) if r.get("symbol") == symbol]
                if len(matches) != 1 or matches[0].get("status") != "TRADING" or matches[0].get("quoteAsset") != "USDT":
                    raise ShortLiveBrokerBlocked("EXACT_TRADING_SYMBOL_REQUIRED")
                filters = {r["filterType"]: r for r in matches[0].get("filters", [])}
                extra["priceTick"] = filters.get("PRICE_FILTER", {}).get("tickSize")
                lot = filters.get("LOT_SIZE", {})
                minimum = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))
                min_notional = minimum.get("minNotional", minimum.get("notional"))
                if futures:
                    market_lots = [row for row in matches[0].get("filters", [])
                                   if isinstance(row, Mapping) and row.get("filterType") == "MARKET_LOT_SIZE"]
                    if len(market_lots) != 1:
                        raise ShortLiveBrokerBlocked("FUTURES_MARKET_LOT_SIZE_UNKNOWN")
                    market_lot = market_lots[0]
                    raw_market_step = number(market_lot.get("stepSize"))
                    market_min = number(market_lot.get("minQty"))
                    market_max = number(market_lot.get("maxQty"), positive=True)
                    if raw_market_step < 0 or market_min < 0 or market_max < market_min:
                        raise ShortLiveBrokerBlocked("FUTURES_MARKET_LOT_SIZE_INVALID")
                    market_step = (number(lot.get("stepSize"), positive=True)
                                   if raw_market_step == 0 else raw_market_step)
                    extra.update(marketQuantityStep=decimal_text(market_step),
                                 marketMinQuantity=decimal_text(market_min),
                                 marketMaxQuantity=decimal_text(market_max),
                                 marketQuantityStepRaw=decimal_text(raw_market_step),
                                 marketQuantityStepSource=("LOT_SIZE_FOR_ZERO_MARKET_STEP"
                                     if raw_market_step == 0 else "MARKET_LOT_SIZE"))
                    commission = self._binance(provider, "/fapi/v1/commissionRate", {"symbol": symbol})
                    fee = commission.get("takerCommissionRate")
                    config = self._binance(provider, "/fapi/v1/symbolConfig", {"symbol": symbol})
                    mode = self._binance(provider, "/fapi/v1/positionSide/dual")
                    if not isinstance(mode, Mapping) or type(mode.get("dualSidePosition")) is not bool:
                        raise ShortLiveBrokerBlocked("FUTURES_CONFIG_UNKNOWN")
                    brackets, coefficient = self._leverage_brackets(
                        self._binance(provider, "/fapi/v1/leverageBracket", {"symbol": symbol}), symbol)
                    premium = self._binance(provider, "/fapi/v1/premiumIndex", {"symbol": symbol}, signed=False)
                    quote_received = self._now()
                    price = premium.get("markPrice")
                    if type(premium.get("time")) is not int:
                        raise ShortLiveBrokerBlocked("FUTURES_QUOTE_SOURCE_TIME_UNKNOWN")
                    extra["sourceQuoteObservedAt"] = datetime.fromtimestamp(premium["time"] / 1000, timezone.utc).isoformat()
                    extra.update(**self._symbol_config(config, symbol), markPrice=decimal_text(number(price, positive=True)),
                                 positionMode="HEDGE" if mode["dualSidePosition"] else "ONE_WAY",
                                 fundingRate=decimal_text(number(premium.get("lastFundingRate"))),
                                 leverageBrackets=brackets, notionalCoefficient=coefficient,
                                 leverageBracketsAccountAdjusted=True)
                else:
                    commission = self._binance(provider, "/api/v3/account/commission", {"symbol": symbol})
                    discount = commission.get("discount", {})
                    if isinstance(discount, Mapping):
                        extra["commissionDiscountEnabled"] = (discount.get("enabledForAccount") is True
                                                                and discount.get("enabledForSymbol") is True)
                        extra["commissionDiscountAsset"] = str(discount.get("discountAsset") or "")
                    standard = commission.get("standardCommission", {})
                    special = commission.get("specialCommission", {})
                    tax = commission.get("taxCommission", {})
                    # Conservative account-specific taker + both directional
                    # additions, without assuming a BNB discount is available.
                    fee = sum((number(group.get("taker")) + max(number(group.get("buyer", 0)), number(group.get("seller", 0)))
                               for group in (standard, special, tax)), Decimal(0))
                    ticker = self._binance(provider, "/api/v3/ticker/price", {"symbol": symbol}, signed=False)
                    quote_received = self._now()
                    price = ticker.get("price")
                step, min_qty, max_qty = lot.get("stepSize"), lot.get("minQty"), lot.get("maxQty")
            elif provider == "upbit":
                chance = self._upbit("/v1/orders/chance", {"market": symbol})
                instruments = self._upbit("/v1/orderbook/instruments", {"markets": symbol}, signed=False)
                if not isinstance(instruments, list) or len(instruments) != 1 or instruments[0].get("market") != symbol:
                    raise ShortLiveBrokerBlocked("UPBIT_PRICE_TICK_UNKNOWN")
                extra["priceTick"] = str(number(instruments[0].get("tick_size"), positive=True))
                ticker = self._upbit("/v1/ticker", {"markets": symbol}, signed=False)
                quote_received = self._now()
                market = chance.get("market", {})
                if market.get("state") != "active" or not isinstance(ticker, list) or len(ticker) != 1 or ticker[0].get("market") != symbol:
                    raise ShortLiveBrokerBlocked("UPBIT_MARKET_UNKNOWN")
                price = ticker[0].get("trade_price")
                if type(ticker[0].get("trade_timestamp")) is not int or type(ticker[0].get("timestamp")) is not int:
                    raise ShortLiveBrokerBlocked("UPBIT_QUOTE_SOURCE_TIME_UNKNOWN")
                extra["sourceQuoteObservedAt"] = datetime.fromtimestamp(min(ticker[0]["trade_timestamp"], ticker[0]["timestamp"]) / 1000, timezone.utc).isoformat()
                fee = max(number(chance.get("bid_fee")), number(chance.get("ask_fee")))
                min_notional = max(number(market.get("bid", {}).get("min_total")), number(market.get("ask", {}).get("min_total")))
                step, min_qty, max_qty = "0.00000001", "0.00000001", None
            else:
                price = self.reader.kis_reference_price("kis-kr", symbol)
                quote_received = self._now()
                if self.cost_reader is None:
                    raise ShortLiveBrokerBlocked("KIS_CONFIRMED_COST_POLICY_REQUIRED")
                costs = dict(self.cost_reader(route, symbol))
                if costs.get("known") is not True:
                    raise ShortLiveBrokerBlocked("KIS_CONFIRMED_COST_POLICY_REQUIRED")
                fee = costs.get("feeRate")
                extra["sellTaxRate"] = costs.get("sellTaxRate")
                from trading_runtime.market_calendar import session_bounds_utc
                now = datetime.fromtimestamp(self.clock(), timezone.utc)
                opened, closed = session_bounds_utc("XKRX", now.astimezone(ZoneInfo("Asia/Seoul")).date())
                extra["sessionOpen"] = opened <= now < closed
                extra["sessionRemainingSeconds"] = max(0, (closed - now).total_seconds())
                extra["sessionClosesAt"] = closed.isoformat()
                step, min_qty, max_qty, min_notional = "1", "1", None, "0"
            # Metadata/fee/settings reads can be slow. Observe quote and FX at
            # the end without caching, retrying, or relabeling their timestamps.
            fx = {"known": True, "krwPerQuote": "1", "observedAt": self._now()} if currency == "KRW" else self._fx(currency)
            if fx.get("known") is not True:
                raise ShortLiveBrokerBlocked("FRESH_FX_REQUIRED")
            result = {"known": True, "symbol": symbol, "nativePrice": str(price), "quoteCurrency": currency,
                      "krwPerQuote": str(fx["krwPerQuote"]), "fxObservedAt": fx["observedAt"], "feeRate": str(fee),
                      "lotStep": str(step), "minQuantity": str(min_qty), "minNotional": str(min_notional),
                      "maxQuantity": None if max_qty is None else str(max_qty), "observedAt": quote_received, **extra}
            for key in ("nativePrice", "krwPerQuote", "lotStep", "minQuantity"):
                number(result[key], positive=True)
            number(result["feeRate"]); number(result["minNotional"])
            result["readDiagnostics"] = self._failure_reads()
            return result
        except Exception as exc:
            return {"known": False, "blockers": [str(exc) if isinstance(exc, ShortLiveBrokerBlocked) else "OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"],
                    "readDiagnostics": self._failure_reads()}


def deepcopy_rows(value: Mapping[str, Any]) -> dict[str, Any]:
    """No raw headers, credentials or broker bodies cross the ownership join."""
    return {key: [dict(row) for row in value[key]] for key in ("positions", "openOrders")}


def production_fx_reader(reader, *, clock=time.time):
    """Use the real official KRW/USDT observation; no assumed USD peg."""
    def observe(currency):
        if currency != "USDT":
            return {"known": False}
        from .account_currency_limits import observe_upbit_usdt_krw, validate_conversion_observation
        raw = observe_upbit_usdt_krw(reader)
        now = datetime.fromtimestamp(clock(), timezone.utc)
        verified = validate_conversion_observation(raw, native_currency="USDT", now=now)
        return {"known": True, "krwPerQuote": verified["krwPerNativeCurrency"],
                "observedAt": verified["rateAsOf"], "contentHash": verified["contentHash"],
                "source": "OFFICIAL_UPBIT_KRW_USDT"}
    return observe


def production_cost_reader(settings, *, clock=time.time):
    """Explicit personal KIS cost facts; no provisional profile promotion."""
    # Reuse the pure policy method without constructing readers or loading env.
    policy = object.__new__(ProductionShortLiveReaders)
    policy.settings, policy.clock = dict(settings), clock
    return policy._configured_kis_costs
