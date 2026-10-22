"""Narrow in-process authority for a manually confirmed short Live session.

Activation and final sends use the existing exchange route locks.  No stored
JSON flag is authority.  An exact thread-local native payload and a fresh
service-owned durable reservation are required at every final boundary.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import secrets
import threading
import urllib.parse

_LOCK = threading.RLock()
_LOCAL = threading.local()
_ACTIVE = {}
_EXCHANGES = {"KIS_KR_CASH": "KIS", "UPBIT_KRW_SPOT": "UPBIT",
              "BINANCE_SPOT": "BINANCE", "BINANCE_USDM_FUTURES": "BINANCE"}
_PROVIDERS = {"KIS_KR_CASH": "kis", "UPBIT_KRW_SPOT": "upbit", "BINANCE_SPOT": "binance", "BINANCE_USDM_FUTURES": "binance-futures"}


class ShortLiveAuthorityError(RuntimeError):
    pass


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _finite_positive(value):
    try:
        if value is None or isinstance(value, bool): raise ValueError
        parsed = Decimal(str(value))
        if not parsed.is_finite() or parsed <= 0: raise ValueError
        return parsed
    except (InvalidOperation, TypeError, ValueError):
        raise ShortLiveAuthorityError("SHORT_LIVE_NATIVE_FINITE_NUMBER_REQUIRED") from None


def _exact_native(order, native):
    if (native.get("broker_id") != _PROVIDERS[order["route"]]
            or native.get("symbol") != order.get("symbol") or not order.get("accountId")):
        raise ShortLiveAuthorityError("SHORT_LIVE_NATIVE_ROUTE_SYMBOL_ACCOUNT_CHANGED")
    if order["route"] == "KIS_KR_CASH" and (not re.fullmatch(r"[0-9]{6}", str(order.get("symbol"))) or native.get("asset") != "KR_STOCK"):
        raise ShortLiveAuthorityError("SHORT_LIVE_DOMESTIC_ASSET_BINDING_REQUIRED")
    if order.get("operation") == "CANCEL":
        if not order.get("brokerOrderId") or str(native.get("broker_order_id")) != str(order["brokerOrderId"]):
            raise ShortLiveAuthorityError("SHORT_LIVE_NATIVE_CANCEL_ID_CHANGED")
        return
    if (native.get("side") != order.get("side")
            or _finite_positive(native.get("quantity")) != _finite_positive(order.get("quantity"))
            or _finite_positive(native.get("price")) != _finite_positive(order.get("referencePrice"))
            or native.get("identifier") != order.get("clientOrderId")
            or native.get("new_client_order_id") != order.get("clientOrderId")
            or native.get("session_id") != order.get("sessionId")
            or native.get("manifest_hash") != order.get("manifestHash")
            or native.get("execution_purpose") != "FUNCTIONAL_TEST_ONLY"
            or native.get("order_type") != ("00" if order["route"] == "KIS_KR_CASH" else "LIMIT")
            or native.get("risk_reducing") is not (order.get("riskReducing") is True)
            or native.get("reduce_only") is not (order.get("riskReducing") is True)
            or native.get("position_direction") != order.get("positionDirection", "long")):
        raise ShortLiveAuthorityError("SHORT_LIVE_NATIVE_RESERVED_ORDER_CHANGED")
    if order["route"] == "KIS_KR_CASH" and any(_finite_positive(native[key]) != _finite_positive(native[key]).to_integral_value() for key in ("quantity", "price")):
        raise ShortLiveAuthorityError("SHORT_LIVE_DOMESTIC_INTEGER_QUANTITY_PRICE_REQUIRED")
    if order["route"] == "BINANCE_USDM_FUTURES" and native.get("position_mode") != order.get("positionMode"):
        raise ShortLiveAuthorityError("SHORT_LIVE_NATIVE_POSITION_MODE_CHANGED")


def short_live_dispatch_present():
    return getattr(_LOCAL, "dispatch", None) is not None


@contextmanager
def _routes(exchanges):
    # Same locks and order as the real mutation boundaries; never acquire a
    # service lock from an ordinary route. Service calls hold its own RLock.
    with ExitStack() as stack:
        for exchange in sorted(set(exchanges)):
            if exchange == "KIS":
                from .kis_order_authority import kis_route_authority_serialization
                stack.enter_context(kis_route_authority_serialization())
            elif exchange == "UPBIT":
                from .upbit_order_authority import upbit_route_authority_serialization
                stack.enter_context(upbit_route_authority_serialization())
            elif exchange == "BINANCE":
                from .binance_order_authority import binance_route_authority_serialization
                stack.enter_context(binance_route_authority_serialization())
        yield


def activate_short_live_routes(manifests):
    rows = [deepcopy(dict(row)) for row in manifests]
    if not rows or any(row.get("route") not in _EXCHANGES or not row.get("sessionId") or not row.get("manifestHash") for row in rows):
        raise ShortLiveAuthorityError("SHORT_LIVE_EXACT_ACTIVATION_REQUIRED")
    with _routes(_EXCHANGES[row["route"]] for row in rows), _LOCK:
        for row in rows:
            previous = _ACTIVE.get(row["route"])
            key = (str(row["sessionId"]), str(row["manifestHash"]))
            if any(_EXCHANGES[route] == _EXCHANGES[row["route"]] and owner != key for route, owner in _ACTIVE.items()):
                raise ShortLiveAuthorityError("SHORT_LIVE_EXCHANGE_ALREADY_OWNED")
            if previous is not None and previous != key:
                raise ShortLiveAuthorityError("SHORT_LIVE_ROUTE_ALREADY_OWNED")
        for row in rows:
            _ACTIVE[row["route"]] = (str(row["sessionId"]), str(row["manifestHash"]))


def release_short_live_routes(session_id, manifest_hash):
    """Only the service calls this after trusted terminal-flat reconciliation."""
    with _routes(_EXCHANGES.values()), _LOCK:
        for route, key in list(_ACTIVE.items()):
            if key == (str(session_id), str(manifest_hash)):
                del _ACTIVE[route]


@contextmanager
def short_live_dispatch(order, native_payload, authority_guard):
    if getattr(_LOCAL, "dispatch", None) is not None or not callable(authority_guard):
        raise ShortLiveAuthorityError("SHORT_LIVE_NESTED_OR_UNOWNED_DISPATCH")
    detached = deepcopy(dict(order))
    route = detached.get("route")
    exchange = _EXCHANGES.get(route)
    if exchange is None:
        raise ShortLiveAuthorityError("SHORT_LIVE_ROUTE_INVALID")
    _exact_native(detached, native_payload)
    context = {"order": detached, "orderHash": _digest(detached), "nativeHash": _digest(native_payload),
               "guard": authority_guard, "exchange": exchange, "route": route,
               "thread": threading.get_ident(), "nonce": secrets.token_hex(32), "routerEntered": False}
    _LOCAL.dispatch = context
    try:
        check_short_live_authority(exchange)
        yield
    finally:
        del _LOCAL.dispatch


def validate_short_live_native_payload(native_payload):
    """Burn the one router entry before building/signing this exact payload."""
    context = getattr(_LOCAL, "dispatch", None)
    if context is None:
        return
    if context["routerEntered"] or _digest(native_payload) != context["nativeHash"]:
        raise ShortLiveAuthorityError("SHORT_LIVE_NATIVE_PAYLOAD_CHANGED_OR_REUSED")
    check_short_live_authority(context["exchange"])
    context["routerEntered"] = True


def validate_short_live_normalized_payload(native_payload):
    """The legacy normalizer may not silently resize a reserved Live order."""
    context = getattr(_LOCAL, "dispatch", None)
    if context is not None:
        _exact_native(context["order"], native_payload)


def validate_short_live_prepared_request(prepared, *, consume=False):
    """Compare the actual native HTTP order against the reserved tuple."""
    context = getattr(_LOCAL, "dispatch", None)
    if context is None or str(prepared.method).upper() == "GET":
        return
    order = context["order"]
    cancel = order.get("operation") == "CANCEL"
    check_short_live_authority(context["exchange"], operation="CANCEL_ORDER" if cancel else "PLACE_ORDER")
    if prepared.provider != _PROVIDERS[order["route"]]:
        raise ShortLiveAuthorityError("SHORT_LIVE_WIRE_PROVIDER_CHANGED")
    parsed = urllib.parse.urlsplit(prepared.url)
    expected_hosts = {"KIS_KR_CASH": "openapi.koreainvestment.com:9443", "BINANCE_SPOT": "api.binance.com",
                      "BINANCE_USDM_FUTURES": "fapi.binance.com", "UPBIT_KRW_SPOT": "api.upbit.com"}
    if parsed.scheme != "https" or parsed.netloc != expected_hosts[order["route"]] or parsed.path != prepared.endpoint or parsed.fragment:
        raise ShortLiveAuthorityError("SHORT_LIVE_OFFICIAL_WIRE_ORIGIN_REQUIRED")
    query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    if len({key for key, _ in query_pairs}) != len(query_pairs):
        raise ShortLiveAuthorityError("SHORT_LIVE_DUPLICATE_WIRE_QUERY")
    query = dict(query_pairs)
    body = prepared.body or {}
    if context["exchange"] == "KIS":
        endpoint = "/uapi/domestic-stock/v1/trading/" + ("order-rvsecncl" if cancel else "order-cash")
        if prepared.method != "POST" or prepared.endpoint != endpoint or body.get("ORD_DVSN") != "00":
            raise ShortLiveAuthorityError("SHORT_LIVE_KIS_DOMESTIC_LIMIT_WIRE_REQUIRED")
        if cancel:
            if (str(body.get("ORGN_ODNO")) != str(order.get("brokerOrderId"))
                    or str(body.get("KRX_FWDG_ORD_ORGNO")) != str(order.get("organizationNo"))
                    or body.get("RVSE_CNCL_DVSN_CD") != "02"
                    or _finite_positive(body.get("ORD_QTY")) != _finite_positive(order.get("remainingQuantity"))):
                raise ShortLiveAuthorityError("SHORT_LIVE_KIS_OWNED_CANCEL_WIRE_CHANGED")
        elif (body.get("PDNO") != order["symbol"]
                or _finite_positive(body.get("ORD_QTY")) != _finite_positive(order["quantity"])
                or _finite_positive(body.get("ORD_UNPR")) != _finite_positive(order["referencePrice"])
                or prepared.headers.get("tr_id") != ("TTTC0012U" if order["side"] == "BUY" else "TTTC0011U")):
            raise ShortLiveAuthorityError("SHORT_LIVE_KIS_RESERVED_WIRE_CHANGED")
    elif context["exchange"] == "BINANCE":
        endpoint = "/fapi/v1/order" if order["route"] == "BINANCE_USDM_FUTURES" else "/api/v3/order"
        if prepared.endpoint != endpoint or prepared.method != ("DELETE" if cancel else "POST") or query.get("symbol") != order["symbol"]:
            raise ShortLiveAuthorityError("SHORT_LIVE_BINANCE_ORDER_WIRE_CHANGED")
        if cancel:
            if query.get("orderId") != str(order["brokerOrderId"]):
                raise ShortLiveAuthorityError("SHORT_LIVE_BINANCE_OWNED_CANCEL_CHANGED")
        elif (query.get("newClientOrderId") != order["clientOrderId"] or query.get("side") != order["side"]
                or query.get("type") != "LIMIT"
                or _finite_positive(query.get("quantity")) != _finite_positive(order["quantity"])
                or _finite_positive(query.get("price")) != _finite_positive(order["referencePrice"])):
            raise ShortLiveAuthorityError("SHORT_LIVE_BINANCE_RESERVED_WIRE_CHANGED")
        if not cancel and order["route"] == "BINANCE_USDM_FUTURES":
            hedge = order.get("positionMode") == "HEDGE"
            expected_side = str(order.get("positionDirection", "long")).upper() if hedge else "BOTH"
            if query.get("positionSide") != expected_side or (hedge and "reduceOnly" in query) or (
                    not hedge and order.get("riskReducing") is True and query.get("reduceOnly") != "true"):
                raise ShortLiveAuthorityError("SHORT_LIVE_FUTURES_OWNED_SIDE_WIRE_CHANGED")
    else:
        if prepared.endpoint != "/v1/order" and not (not cancel and prepared.endpoint == "/v1/orders"):
            raise ShortLiveAuthorityError("SHORT_LIVE_UPBIT_ORDER_WIRE_CHANGED")
        if cancel:
            if prepared.method != "DELETE" or query.get("uuid") != str(order["brokerOrderId"]):
                raise ShortLiveAuthorityError("SHORT_LIVE_UPBIT_OWNED_CANCEL_CHANGED")
        elif (prepared.method != "POST" or body.get("market") != order["symbol"]
                or body.get("identifier") != order["clientOrderId"] or body.get("ord_type") != "limit"
                or body.get("side") != ("bid" if order["side"] == "BUY" else "ask")
                or _finite_positive(body.get("volume")) != _finite_positive(order["quantity"])
                or _finite_positive(body.get("price")) != _finite_positive(order["referencePrice"])):
            raise ShortLiveAuthorityError("SHORT_LIVE_UPBIT_RESERVED_WIRE_CHANGED")
    if consume:
        if context.get("wireMutationAttempted"):
            raise ShortLiveAuthorityError("SHORT_LIVE_MUTATION_ALREADY_ATTEMPTED")
        context["wireMutationAttempted"] = True


def check_short_live_authority(exchange, *, operation=None):
    """False means no short owner; True is the sole exact scoped exception.

    Called while the normal exchange route lock is held at final sends.
    Ordinary/old functional routes are blocked without calling service code.
    """
    context = getattr(_LOCAL, "dispatch", None)
    with _LOCK:
        active = {route: key for route, key in _ACTIVE.items() if _EXCHANGES[route] == exchange}
    if not active:
        if context is not None and context["exchange"] == exchange:
            raise ShortLiveAuthorityError("SHORT_LIVE_ROUTE_NOT_ACTIVE")
        return False
    if context is None or context["exchange"] != exchange or context["thread"] != threading.get_ident():
        raise ShortLiveAuthorityError("SHORT_LIVE_EXCLUSIVE_ROUTE_FENCE")
    order = context["order"]
    if (active.get(context["route"]) != (str(order.get("sessionId")), str(order.get("manifestHash")))
            or _digest(order) != context["orderHash"] or context["guard"](deepcopy(order)) is not True):
        raise ShortLiveAuthorityError("SHORT_LIVE_DURABLE_RESERVATION_CHANGED")
    if operation is not None:
        if not context["routerEntered"]:
            raise ShortLiveAuthorityError("SHORT_LIVE_ROUTER_ENTRY_REQUIRED")
        cancel = order.get("operation") == "CANCEL"
        route_prefix = "SPOT_" if context["route"] == "BINANCE_SPOT" else "FUTURES_" if context["route"] == "BINANCE_USDM_FUTURES" else ""
        allowed = {"CANCEL_ORDER", route_prefix + "CANCEL_ORDER"} if cancel else {"PLACE_ORDER", route_prefix + "PLACE_ORDER"}
        if str(operation) not in allowed:
            raise ShortLiveAuthorityError("SHORT_LIVE_OPERATION_MISMATCH")
    return True


def _reset_for_tests():
    with _LOCK:
        _ACTIVE.clear()
    if hasattr(_LOCAL, "dispatch"):
        del _LOCAL.dispatch
