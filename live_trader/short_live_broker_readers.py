"""Official read-only observations for short Live functional sessions.

Settings are supplied by the genuine app, never loaded on import. No order,
margin, leverage, transfer or other account mutation endpoint is reachable.
The sole POST is the existing KIS OAuth authentication boundary.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import time
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo
from typing import Any

from .short_live_brokers import ROUTES, ShortLiveBrokerBlocked, number, decimal_text


class ProductionShortLiveReaders:
    def __init__(self, *, settings: Mapping[str, Any], fx_reader: Callable[..., Mapping[str, Any]] | None = None,
                 ownership_reader: Callable[..., Mapping[str, Any]] | None = None,
                 cost_reader: Callable[..., Mapping[str, Any]] | None = None,
                 clock: Callable[[], float] = time.time, reader: Any = None) -> None:
        self.settings = dict(settings)
        self.ownership_reader, self.cost_reader, self.clock = ownership_reader, cost_reader, clock
        if reader is None:
            from .read_only_account import ReadOnlyBrokerReader, ReadOnlyTransport
            from .read_only_account import ORIGINS, NoRedirect
            class ExtendedReadTransport(ReadOnlyTransport):
                EXTRA = {"/fapi/v3/positionRisk", "/fapi/v1/positionSide/dual", "/fapi/v1/symbolConfig",
                         "/fapi/v1/commissionRate", "/fapi/v1/premiumIndex", "/fapi/v1/accountConfig",
                         "/fapi/v1/order", "/fapi/v1/openAlgoOrders", "/fapi/v1/userTrades"}
                def request(self, provider, method, path, *, query=None, headers=None, body=None):
                    extra_allowed = ((provider == "binance-futures" and path in self.EXTRA)
                                     or (provider == "upbit" and path == "/v1/orderbook/instruments"))
                    if not extra_allowed:
                        return super().request(provider, method, path, query=query, headers=headers, body=body)
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
                        return json.loads(raw), dict(response.headers)
            reader = ReadOnlyBrokerReader(self.settings, transport=ExtendedReadTransport())
        self.reader = reader
        self.fx_reader = fx_reader or self._official_fx
        self.cost_reader = cost_reader or self._configured_kis_costs

    def _now(self) -> str:
        return datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()

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
            clock, _ = self.reader._get(provider, prefix + "/time")
            if not isinstance(clock, dict) or isinstance(clock.get("serverTime"), bool):
                raise ShortLiveBrokerBlocked("OFFICIAL_SERVER_TIME_UNKNOWN")
            query.update(timestamp=int(clock["serverTime"]), recvWindow=5000)
            query = sign_binance_query(query, self.settings["BINANCE_API_SECRET"])
            headers = {"X-MBX-APIKEY": self.settings["BINANCE_API_KEY"]}
        payload, _ = self.reader._get(provider, path, query=query, headers=headers)
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
        payload, _ = self.reader._get("upbit", path, query=query, headers=headers)
        if isinstance(payload, dict) and "error" in payload:
            raise ShortLiveBrokerBlocked("OFFICIAL_PRIVATE_READ_REJECTED")
        return payload

    @staticmethod
    def _order(row: Mapping[str, Any], *, provider: str) -> dict[str, Any]:
        state = str(row.get("status") or row.get("state") or "UNKNOWN").upper()
        side = str(row.get("side") or "").upper()
        if side in {"BID", "ASK"}:
            side = "BUY" if side == "BID" else "SELL"
        return {"symbol": str(row.get("symbol") or row.get("market") or ""),
                "brokerOrderId": str(row.get("orderId") or row.get("uuid") or ""),
                "clientOrderId": str(row.get("clientOrderId") or row.get("identifier") or ""),
                "status": state, "side": side,
                "filledQuantity": str(row.get("executedQty", row.get("executed_volume", "0"))),
                "remainingQuantity": str(row.get("remaining_volume", "0")),
                "fillPrice": str(row.get("avgPrice") or row.get("price") or "0"),
                "feeNative": row.get("paid_fee"), "provider": provider}

    def _crypto_snapshot(self, route: str, symbols: tuple[str, ...]) -> dict[str, Any]:
        provider, _, currency = ROUTES[route]
        positions: list[dict[str, Any]] = []
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
                for row in raw_positions:
                    qty = number(row.get("positionAmt"))
                    if row.get("positionSide") == "SHORT": qty = -abs(qty)
                    elif row.get("positionSide") == "LONG": qty = abs(qty)
                    if qty:
                        positions.append({"symbol": str(row.get("symbol")), "quantity": decimal_text(qty),
                                          "markPrice": str(row.get("markPrice")),
                                          "positionDirection": "short" if row.get("positionSide") == "SHORT" or qty < 0 else "long"})
                # Regular orders and conditional algo orders are distinct.
                raw_orders = self._binance(provider, "/fapi/v1/openOrders")
                algo = self._binance(provider, "/fapi/v1/openAlgoOrders")
                algo_rows = algo.get("orders") if isinstance(algo, dict) else algo
                if not isinstance(algo_rows, list) or algo_rows:
                    raise ShortLiveBrokerBlocked("FUTURES_CONDITIONAL_ORDER_PRESENT_OR_UNKNOWN")
            else:
                if account.get("canTrade") is not True:
                    raise ShortLiveBrokerBlocked("ACCOUNT_TRADING_DISABLED")
                equity += number(quote.get("locked"))
                for row in rows:
                    if row.get("asset") == "USDT":
                        continue
                    qty = number(row.get("free")) + number(row.get("locked"))
                    if qty:
                        symbol = str(row.get("asset")) + "USDT"
                        if symbol not in symbols:
                            raise ShortLiveBrokerBlocked("UNSCOPED_SPOT_ASSET_PRESENT")
                        price = number(self.market(route, symbol)["nativePrice"], positive=True)
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
            equity = available + number(quote[0].get("locked"))
            for row in rows:
                if row.get("currency") == "KRW":
                    continue
                qty = number(row.get("balance")) + number(row.get("locked"))
                if qty:
                    symbol = "KRW-" + str(row.get("currency"))
                    if symbol not in symbols:
                        raise ShortLiveBrokerBlocked("UNSCOPED_SPOT_ASSET_PRESENT")
                    price = number(self.market(route, symbol)["nativePrice"], positive=True)
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
        return {"known": True, "accountId": self._identity(provider), "currency": currency,
                "availableNative": decimal_text(available), "equityNative": decimal_text(equity),
                "positions": positions, "openOrders": orders, "ordersComplete": True,
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
                payload, headers = self.reader._get("kis", path, query={"CANO": cano, "ACNT_PRDT_CD": product,
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
            return result
        except Exception as exc:
            return {"known": False, "ordersComplete": False, "positionsComplete": False,
                    "blockers": [str(exc) if isinstance(exc, ShortLiveBrokerBlocked) else "OFFICIAL_ACCOUNT_READ_UNKNOWN"]}

    def get_order(self, route: str, claim: Mapping[str, Any]) -> dict[str, Any]:
        """Read one already reserved order by exact broker ID, never retry POST."""
        try:
            observed_started = self._now()
            provider = ROUTES[route][0]
            order_id, symbol = str(claim.get("brokerOrderId") or ""), str(claim.get("symbol") or "")
            if not order_id or not symbol:
                raise ShortLiveBrokerBlocked("EXACT_OWNED_ORDER_REQUIRED")
            if provider in {"binance", "binance-futures"}:
                futures = provider == "binance-futures"
                prefix = "/fapi/v1" if futures else "/api/v3"
                raw = self._binance(provider, prefix + "/order", {"symbol": symbol, "orderId": order_id})
                if not isinstance(raw, dict) or str(raw.get("orderId")) != order_id or raw.get("symbol") != symbol:
                    raise ShortLiveBrokerBlocked("ORDER_TRUTH_ID_MISMATCH")
                result = self._order(raw, provider=provider)
                result.update(baseCommissionQuantity="0", baseCommissionKnown=True)
                filled = number(result["filledQuantity"])
                if filled:
                    trades = self._binance(provider, prefix + ("/userTrades" if futures else "/myTrades"),
                                           {"symbol": symbol, "orderId": order_id, "limit": 1000})
                    if not isinstance(trades, list) or len(trades) >= 1000:
                        raise ShortLiveBrokerBlocked("OWNED_TRADES_INCOMPLETE")
                    selected = [r for r in trades if str(r.get("orderId")) == order_id and r.get("symbol") == symbol]
                    if sum((number(r.get("qty")) for r in selected), Decimal(0)) != filled:
                        raise ShortLiveBrokerBlocked("OWNED_TRADE_QUANTITY_MISMATCH")
                    result["fillPrice"] = decimal_text(sum((number(r.get("qty")) * number(r.get("price")) for r in selected), Decimal(0)) / filled)
                    commissions = Decimal(0)
                    base_commissions = Decimal(0)
                    for row in selected:
                        fee = number(row.get("commission"))
                        asset = str(row.get("commissionAsset") or "")
                        if not futures and asset == symbol.removesuffix("USDT"):
                            base_commissions += fee
                        if asset == "USDT":
                            commissions += fee
                        elif fee == 0:
                            continue
                        else:
                            ticker = self._binance("binance", "/api/v3/ticker/price", {"symbol": asset + "USDT"}, signed=False)
                            commissions += fee * number(ticker.get("price"), positive=True)
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

    def market(self, route: str, symbol: str) -> dict[str, Any]:
        try:
            provider, _, currency = ROUTES[route]
            fx = {"known": True, "krwPerQuote": "1", "observedAt": self._now()} if currency == "KRW" else dict(self.fx_reader(currency))
            if fx.get("known") is not True:
                raise ShortLiveBrokerBlocked("FRESH_FX_REQUIRED")
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
                    premium = self._binance(provider, "/fapi/v1/premiumIndex", {"symbol": symbol}, signed=False)
                    quote_received = self._now()
                    price = premium.get("markPrice")
                    if type(premium.get("time")) is not int:
                        raise ShortLiveBrokerBlocked("FUTURES_QUOTE_SOURCE_TIME_UNKNOWN")
                    extra["sourceQuoteObservedAt"] = datetime.fromtimestamp(premium["time"] / 1000, timezone.utc).isoformat()
                    commission = self._binance(provider, "/fapi/v1/commissionRate", {"symbol": symbol})
                    fee = commission.get("takerCommissionRate")
                    config = self._binance(provider, "/fapi/v1/symbolConfig", {"symbol": symbol})
                    mode = self._binance(provider, "/fapi/v1/positionSide/dual")
                    if not isinstance(config, list) or len(config) != 1 or config[0].get("symbol") != symbol or type(mode.get("dualSidePosition")) is not bool:
                        raise ShortLiveBrokerBlocked("FUTURES_CONFIG_UNKNOWN")
                    extra.update(marginMode=str(config[0].get("marginType") or "").upper(), leverage=config[0].get("leverage"),
                                 positionMode="HEDGE" if mode["dualSidePosition"] else "ONE_WAY", fundingRate=premium.get("lastFundingRate"))
                else:
                    ticker = self._binance(provider, "/api/v3/ticker/price", {"symbol": symbol}, signed=False)
                    quote_received = self._now()
                    price = ticker.get("price")
                    commission = self._binance(provider, "/api/v3/account/commission", {"symbol": symbol})
                    standard = commission.get("standardCommission", {})
                    special = commission.get("specialCommission", {})
                    tax = commission.get("taxCommission", {})
                    # Conservative account-specific taker + both directional
                    # additions, without assuming a BNB discount is available.
                    fee = sum((number(group.get("taker")) + max(number(group.get("buyer", 0)), number(group.get("seller", 0)))
                               for group in (standard, special, tax)), Decimal(0))
                step, min_qty, max_qty = lot.get("stepSize"), lot.get("minQty"), lot.get("maxQty")
            elif provider == "upbit":
                chance = self._upbit("/v1/orders/chance", {"market": symbol})
                ticker = self._upbit("/v1/ticker", {"markets": symbol}, signed=False)
                quote_received = self._now()
                market = chance.get("market", {})
                if market.get("state") != "active" or not isinstance(ticker, list) or len(ticker) != 1 or ticker[0].get("market") != symbol:
                    raise ShortLiveBrokerBlocked("UPBIT_MARKET_UNKNOWN")
                price = ticker[0].get("trade_price")
                if type(ticker[0].get("trade_timestamp")) is not int or type(ticker[0].get("timestamp")) is not int:
                    raise ShortLiveBrokerBlocked("UPBIT_QUOTE_SOURCE_TIME_UNKNOWN")
                extra["sourceQuoteObservedAt"] = datetime.fromtimestamp(min(ticker[0]["trade_timestamp"], ticker[0]["timestamp"]) / 1000, timezone.utc).isoformat()
                instruments = self._upbit("/v1/orderbook/instruments", {"markets": symbol}, signed=False)
                if not isinstance(instruments, list) or len(instruments) != 1 or instruments[0].get("market") != symbol:
                    raise ShortLiveBrokerBlocked("UPBIT_PRICE_TICK_UNKNOWN")
                extra["priceTick"] = str(number(instruments[0].get("tick_size"), positive=True))
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
            result = {"known": True, "symbol": symbol, "nativePrice": str(price), "quoteCurrency": currency,
                      "krwPerQuote": str(fx["krwPerQuote"]), "fxObservedAt": fx["observedAt"], "feeRate": str(fee),
                      "lotStep": str(step), "minQuantity": str(min_qty), "minNotional": str(min_notional),
                      "maxQuantity": None if max_qty is None else str(max_qty), "observedAt": quote_received, **extra}
            for key in ("nativePrice", "krwPerQuote", "lotStep", "minQuantity"):
                number(result[key], positive=True)
            number(result["feeRate"]); number(result["minNotional"])
            return result
        except Exception as exc:
            return {"known": False, "blockers": [str(exc) if isinstance(exc, ShortLiveBrokerBlocked) else "OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"]}


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
