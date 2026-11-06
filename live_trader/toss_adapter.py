"""Toss cash-equity boundary. Read-only construction is the default.

Order intent must identify its market/currency. There is no KIS fallback and
an accepted order is never represented as a fill. Buying power is not equity.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import hashlib
import os
import re
import time


class TossAdapterError(ValueError):
    pass


def client(*, read_only=True):
    from trading_runtime.toss_securities import TossClient
    return TossClient.from_settings(read_only=read_only)


def number(value, *, positive=False):
    try:
        result = Decimal(str(value))
        if not result.is_finite() or result < 0 or (positive and result <= 0):
            raise ValueError
        return result
    except (InvalidOperation, ValueError, TypeError):
        raise TossAdapterError("TOSS_INVALID_AMOUNT") from None


def market_identity(intent):
    metadata = intent.get("metadata") if isinstance(intent.get("metadata"), dict) else {}
    currency = str(intent.get("currency") or intent.get("quote_currency") or intent.get("capitalCurrency") or metadata.get("currency") or metadata.get("quote_currency") or "").upper()
    market = str(intent.get("market_country") or intent.get("marketCountry") or metadata.get("marketCountry") or "").upper()
    if not market:
        market = {"KRW": "KR", "USD": "US"}.get(currency, "")
    if not currency:
        currency = {"KR": "KRW", "US": "USD"}.get(market, "")
    if (market, currency) not in {("KR", "KRW"), ("US", "USD")}:
        raise TossAdapterError("TOSS_EXPLICIT_MARKET_CURRENCY_REQUIRED")
    return market, currency


def strategy_market_metadata(spec):
    """Use the immutable source quantity contract, never a guessed FX unit."""
    if spec.broker_id != "toss" or spec.provider != "toss":
        raise TossAdapterError("TOSS_EXACT_SOURCE_ROUTE_REQUIRED")
    from .native_spot_quantity import sizing_from_artifact
    sizing = sizing_from_artifact(spec.artifact)
    contract = sizing.get("quantityContract") if isinstance(sizing, dict) else None
    if not isinstance(contract, dict) or contract.get("schemaVersion") != "native-equity-quantity-v1":
        raise TossAdapterError("TOSS_REVIEWED_EQUITY_QUANTITY_CONTRACT_REQUIRED")
    market, currency = market_identity({"currency": contract.get("quoteCurrency")})
    exchange = str(contract.get("exchange") or "")
    allowed = {"KRX", "KOSPI", "KOSDAQ", "KONEX"} if market == "KR" else {"NASDAQ", "NYSE", "AMEX", "NASD", "NYSE", "AMEX"}
    if exchange not in allowed:
        raise TossAdapterError("TOSS_REVIEWED_EXCHANGE_MISMATCH")
    return {"quote_currency": currency, "marketCountry": market, "exchange": exchange, "order_type": "LIMIT"}


def normalized_order(intent):
    market, currency = market_identity(intent)
    symbol = str(intent.get("symbol") or "").strip().upper()
    if not (re.fullmatch(r"[A-Z0-9]{6}", symbol) if market == "KR" else re.fullmatch(r"[A-Z][A-Z0-9.-]{0,19}", symbol)):
        raise TossAdapterError("TOSS_SYMBOL_MARKET_MISMATCH")
    side = str(intent.get("side") or "").upper()
    order_type = str(intent.get("order_type") or intent.get("orderType") or "LIMIT").upper()
    # Normal automated operation uses bounded whole-share limit orders. The
    # native API's amount/fractional/auction orders need separate contracts.
    if side not in {"BUY", "SELL"} or order_type != "LIMIT":
        raise TossAdapterError("TOSS_CASH_LIMIT_ORDER_REQUIRED")
    quantity = number(intent.get("quantity") or intent.get("qty"), positive=True)
    if quantity != quantity.to_integral_value():
        raise TossAdapterError("TOSS_LIMIT_INTEGER_QUANTITY_REQUIRED")
    price = number(intent.get("price"), positive=True)
    identifier = str(intent.get("identifier") or intent.get("client_order_id") or intent.get("clientOrderId") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,36}", identifier):
        raise TossAdapterError("TOSS_EXACT_CLIENT_ORDER_ID_REQUIRED")
    if intent.get("risk_reducing") is True and side != "SELL":
        raise TossAdapterError("TOSS_CASH_REDUCE_REQUIRES_SELL")
    if str(intent.get("position_direction") or "long").lower() == "short":
        raise TossAdapterError("TOSS_SHORT_POSITION_UNSUPPORTED")
    body = {"clientOrderId": identifier, "symbol": symbol, "side": side,
            "orderType": "LIMIT", "quantity": str(int(quantity)), "price": format(price, "f")}
    identity = dict(intent, broker_id="toss", symbol=symbol, side=side, identifier=identifier,
                    quantity=str(quantity), qty=str(quantity), price=str(price),
                    notional=str(price * quantity), currency=currency, market_country=market)
    return identity, body


def verified_account(api, settings=None):
    settings = os.environ if settings is None else settings
    seq = str(settings.get("TOSS_ACCOUNT_SEQ") or "").strip()
    rows = api.accounts()
    if not isinstance(rows, list):
        raise TossAdapterError("TOSS_ACCOUNT_RESPONSE_INVALID")
    selected = [r for r in rows if isinstance(r, dict) and str(r.get("accountSeq")) == seq and r.get("accountType") == "BROKERAGE"]
    if len(selected) != 1 or not selected[0].get("accountNo"):
        raise TossAdapterError("TOSS_CONFIGURED_ACCOUNT_NOT_VERIFIED")
    return {"accountFingerprint": hashlib.sha256(str(selected[0]["accountNo"]).encode()).hexdigest(),
            "maskedAccount": "••••" + str(selected[0]["accountNo"])[-4:]}


def account_choices(api=None, settings=None):
    """Only explicit account discovery; never selects or persists an account."""
    settings = os.environ if settings is None else settings
    api = api or client(read_only=True)
    rows = api.accounts()
    if not isinstance(rows, list):
        raise TossAdapterError("TOSS_ACCOUNT_RESPONSE_INVALID")
    choices = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or row.get("accountType") != "BROKERAGE":
            continue
        seq = str(row.get("accountSeq") or "")
        account = str(row.get("accountNo") or "")
        if not re.fullmatch(r"[1-9][0-9]{0,18}", seq) or not account or seq in seen:
            raise TossAdapterError("TOSS_ACCOUNT_RESPONSE_INVALID")
        seen.add(seq)
        choices.append({"accountSeq": seq, "label": "토스증권 · ••••" + account[-4:],
                        "selected": seq == str(settings.get("TOSS_ACCOUNT_SEQ") or "")})
    return {"accounts": choices, "readOnly": True, "ordersSubmitted": 0,
            "detail": "공식 API 계좌 목록입니다. 사용할 계좌를 직접 선택해 저장하세요. 일반 계좌번호를 입력하지 않습니다."}


def positions(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise TossAdapterError("TOSS_HOLDINGS_RESPONSE_INVALID")
    result = []
    for row in payload["items"]:
        market, currency = market_identity(row)
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            raise TossAdapterError("TOSS_HOLDING_SYMBOL_REQUIRED")
        quantity = number(row.get("quantity"))
        market_value = row.get("marketValue") or {}
        result.append({"broker_id": "toss", "broker_name": "토스증권", "symbol": symbol,
                       "name": str(row.get("name") or symbol), "asset": "한국주식" if market == "KR" else "미국주식",
                       "currency": currency, "market_country": market, "broker_qty": float(quantity),
                       "broker_value": float(number(market_value.get("amount"))),
                       "average_price": float(number(row.get("averagePurchasePrice"))),
                       "current_price": float(number(row.get("lastPrice"))), "position_side": "LONG",
                       "valuation_basis": "market_value", "detail": "토스증권 보유 주식 조회 시점 평가입니다."})
    return result


def account_snapshot(api=None, settings=None):
    api = api or client()
    account = verified_account(api, settings)
    holdings = positions(api.holdings())
    accounts = []
    for currency in ("KRW", "USD"):
        power = api.buying_power(currency=currency)
        if not isinstance(power, dict) or power.get("currency") != currency:
            raise TossAdapterError("TOSS_BUYING_POWER_CURRENCY_MISMATCH")
        available = number(power.get("cashBuyingPower"))
        accounts.append({"broker_id": "toss", "broker_name": "토스증권", "account": "토스증권 " + account["maskedAccount"],
                         **account, "currency": currency, "available_cash": float(available),
                         "buying_power": float(available), "broker_cash": None, "broker_equity": None,
                         "valuation_basis": "buying_power_only", "valuation_complete": False,
                         "detail": "주문 가능 금액입니다. 예수금·총자산으로 합산하지 않습니다."})
    return {"broker_id": "toss", "accounts": accounts, "positions": holdings,
            "observed_at": time.time(), "source": "toss_rest_account_observation"}


def order_status(payload):
    if not isinstance(payload, dict) or not payload.get("orderId"):
        raise TossAdapterError("TOSS_ORDER_RESPONSE_INVALID")
    states = {"PENDING": "new", "PARTIAL_FILLED": "partially_filled", "FILLED": "filled",
              "CANCELED": "canceled", "REJECTED": "rejected", "REPLACED": "replaced",
              "PENDING_CANCEL": "pending_cancel", "PENDING_REPLACE": "pending_replace",
              "CANCEL_REJECTED": "cancel_rejected", "REPLACE_REJECTED": "replace_rejected"}
    state = states.get(str(payload.get("status") or "").upper())
    if state is None:
        raise TossAdapterError("TOSS_ORDER_STATE_UNKNOWN")
    execution = payload.get("execution")
    if not isinstance(execution, dict) or "filledQuantity" not in execution:
        raise TossAdapterError("TOSS_ORDER_EXECUTION_UNCONFIRMED")
    return {**payload, "broker_id": "toss", "broker_order_id": str(payload["orderId"]),
            "state": state, "filled_quantity": str(number(execution["filledQuantity"])),
            "source": "toss_order_detail_rest", "observed_at": time.time()}


def exact_owned_order(payload, proof):
    """Toss has no client-ID detail lookup; require the original durable ACK.

    Missing ACK after an ambiguous POST is intentionally not recoverable by a
    guessed latest order or by creating a new client ID.
    """
    response = proof.get("response") or {}
    order_id = str(response.get("brokerOrderId") or "")
    if not order_id or not response.get("responseHash"):
        raise TossAdapterError("TOSS_DURABLE_ORDER_ACK_REQUIRED")
    row = order_status(payload)
    if (row["broker_order_id"] != order_id or row.get("symbol") != proof.get("symbol")
            or row.get("side") != proof.get("side")
            or number(row.get("quantity"), positive=True) != number(proof.get("quantity"), positive=True)):
        raise TossAdapterError("TOSS_OWNED_ORDER_IDENTITY_MISMATCH")
    if row.get("currency") not in {"KRW", "USD"}:
        raise TossAdapterError("TOSS_ORDER_CURRENCY_UNCONFIRMED")
    if proof.get("currency") and proof["currency"] != row["currency"]:
        raise TossAdapterError("TOSS_OWNED_ORDER_CURRENCY_MISMATCH")
    if row.get("orderType") != "LIMIT" or row.get("timeInForce") != "DAY":
        raise TossAdapterError("TOSS_OWNED_ORDER_STYLE_MISMATCH")
    if proof.get("price") is not None and number(row.get("price"), positive=True) != number(proof["price"], positive=True):
        raise TossAdapterError("TOSS_OWNED_ORDER_PRICE_MISMATCH")
    if number(row["filled_quantity"]) > number(row["quantity"]):
        raise TossAdapterError("TOSS_OWNED_FILL_QUANTITY_INVALID")
    return row


def retirement_snapshot(symbols, api=None):
    api = api or client()
    observed_at = time.time()
    verified_account(api)
    open_result = api.orders(status="OPEN")
    if not isinstance(open_result, dict) or not isinstance(open_result.get("orders"), list) or open_result.get("hasNext") is not False:
        raise TossAdapterError("TOSS_OPEN_ORDER_COVERAGE_INCOMPLETE")
    open_orders = [order_status(row) for row in open_result["orders"]]
    if any(not isinstance(row.get("symbol"), str) or not row["symbol"] for row in open_orders):
        raise TossAdapterError("TOSS_OPEN_ORDER_IDENTITY_UNKNOWN")
    # A paused/armed conditional order can later create an ordinary order.
    # An unsupported or incomplete conditional query cannot mean pending=0.
    if not callable(getattr(api, "conditional_orders", None)):
        raise TossAdapterError("TOSS_CONDITIONAL_ORDER_COVERAGE_UNAVAILABLE")
    conditions = api.conditional_orders()
    if not isinstance(conditions, dict) or not isinstance(conditions.get("conditionalOrders"), list) or conditions.get("hasNext") is not False:
        raise TossAdapterError("TOSS_CONDITIONAL_ORDER_COVERAGE_INCOMPLETE")
    for row in conditions["conditionalOrders"]:
        if not isinstance(row, dict) or not row.get("symbol") or row.get("status") not in {"WATCHING", "PAUSED", "ORDERING", "ORDERED", "COMPLETED", "EXPIRED"}:
            raise TossAdapterError("TOSS_CONDITIONAL_ORDER_STATE_UNKNOWN")
        if row["status"] not in {"COMPLETED", "EXPIRED"}:
            open_orders.append({"symbol": row["symbol"], "conditionalOrderId": row.get("conditionalOrderId"), "state": row["status"]})
    holdings = positions(api.holdings())
    requested = set(symbols)
    return {"known": True, "ordersComplete": True, "positionsComplete": True,
            "conditionalOrdersComplete": True, "observedAt": observed_at,
            "openOrders": [row for row in open_orders if row["symbol"] in requested],
            "positions": [dict(row, quantity=str(row["broker_qty"])) for row in holdings if row["symbol"] in requested]}


def owned_sell_quantity(helper, owner, symbol, api=None):
    """Only original observed fills belong to this runtime, never old holdings."""
    api = api or client()
    scope = helper._owned_scope(owner, "toss", symbol)
    quantity = Decimal(0)
    for intent in helper.coordinator.list_owned(owner_id=owner)["intents"]:
        if intent["scopeKey"] != scope["scopeKey"]:
            continue
        proof = helper.retirement_journal.proof(owner, intent["intentId"])
        if proof.get("symbol") != symbol or proof.get("clientOrderId") != intent["clientOrderId"]:
            raise TossAdapterError("TOSS_OWNED_ORDER_IDENTITY_MISMATCH")
        if intent["state"] == "RESERVED":
            continue
        order_id = str((proof.get("response") or {}).get("brokerOrderId") or "")
        if not order_id:
            raise TossAdapterError("TOSS_OWNED_EXECUTION_UNKNOWN")
        row = exact_owned_order(api.get_order(order_id), proof)
        if row["state"] not in {"filled", "canceled", "rejected"}:
            raise TossAdapterError("TOSS_OWNED_ORDER_STILL_OPEN")
        quantity += number(row["filled_quantity"]) * (1 if row["side"] == "BUY" else -1)
    if quantity < 0:
        raise TossAdapterError("TOSS_OWNED_POSITION_INVALID")
    return quantity


def dispatch_funds(intent, api=None):
    normalized, _ = normalized_order(intent)
    api = api or client()
    verified_account(api)
    currency = normalized["currency"]
    stock_rows = api.stocks([normalized["symbol"]])
    if not isinstance(stock_rows, list) or len(stock_rows) != 1:
        raise TossAdapterError("TOSS_STOCK_METADATA_UNCONFIRMED")
    stock = stock_rows[0]
    if stock.get("symbol") != normalized["symbol"] or stock.get("currency") != currency or stock.get("status") != "ACTIVE":
        raise TossAdapterError("TOSS_STOCK_METADATA_MISMATCH")
    venues = {"KOSPI", "KOSDAQ", "KONEX"} if currency == "KRW" else {"NASDAQ", "NYSE", "AMEX"}
    if stock.get("market") not in venues or stock.get("securityType") not in ({"STOCK", "ETF"} if currency == "KRW" else {"STOCK", "FOREIGN_STOCK", "ETF", "FOREIGN_ETF"}):
        raise TossAdapterError("TOSS_STOCK_MARKET_UNSUPPORTED")
    kr = stock.get("koreanMarketDetail") or {}
    if kr.get("liquidationTrading") or kr.get("krxTradingSuspended"):
        raise TossAdapterError("TOSS_STOCK_TRADING_RESTRICTED")
    if normalized["side"] == "BUY":
        power = api.buying_power(currency=currency)
        if not isinstance(power, dict) or power.get("currency") != currency:
            raise TossAdapterError("TOSS_BUYING_POWER_CURRENCY_MISMATCH")
        available = number(power.get("cashBuyingPower"))
        # Reserve 1% in addition to notional, then rely on the broker's exact
        # commissions/tax/funds rules. No inferred FX or integrated-margin mode.
        required = number(normalized["notional"]) * Decimal("1.01")
    else:
        available = number(api.sellable_quantity(normalized["symbol"]).get("sellableQuantity"))
        required, currency = number(normalized["quantity"]), "TOSS" + normalized["symbol"]
    return {"currency": currency, "available": str(available), "required": str(required),
            "observedAt": time.time(), "unitScale": 8}


def prepare_read_only(*, symbol, market_country, api=None, settings=None):
    """Connection/data check only, never a runtime start or order permission."""
    api = api or client()
    market, currency = market_identity({"market_country": market_country})
    snapshot = account_snapshot(api, settings)
    rows = api.stocks([symbol])
    if not isinstance(rows, list) or len(rows) != 1 or rows[0].get("symbol") != symbol or rows[0].get("currency") != currency:
        raise TossAdapterError("TOSS_STOCK_METADATA_MISMATCH")
    stock = rows[0]
    if stock.get("status") != "ACTIVE":
        raise TossAdapterError("TOSS_STOCK_NOT_ACTIVE")
    orders = api.orders(status="OPEN", symbol=symbol)
    if not isinstance(orders, dict) or not isinstance(orders.get("orders"), list) or orders.get("hasNext") is not False:
        raise TossAdapterError("TOSS_OPEN_ORDER_COVERAGE_INCOMPLETE")
    details = [order_status(row) for row in orders["orders"]]
    if any(row.get("symbol") != symbol or row.get("currency") != currency for row in details):
        raise TossAdapterError("TOSS_ORDER_SYMBOL_MISMATCH")
    return {"ok": True, "readOnly": True, "ordersSubmitted": 0, "runtimeStarted": False,
            "brokerId": "toss", "symbol": symbol, "marketCountry": market, "currency": currency,
            "name": str(stock.get("name") or symbol), "market": str(stock.get("market") or ""),
            "accounts": [row for row in snapshot["accounts"] if row["currency"] == currency],
            "positions": [row for row in snapshot["positions"] if row["symbol"] == symbol],
            "openOrderCount": len(details), "commissions": api.commissions(),
            "observedAt": snapshot["observed_at"], "executionAuthorized": False,
            "detail": "계좌·종목·미체결 조회를 확인했습니다. 실거래 시작 시 전략·한도·계좌 소유권을 별도로 확인합니다."}
