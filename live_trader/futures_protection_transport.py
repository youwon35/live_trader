"""Exact, one-attempt transport for owned USD-M conditional stop orders.

This module does not arm trading, change leverage, or infer a fill. The caller
must own the complete symbol/position-side exposure and durably reserve every
mutation before providing its final authority callback. An ambiguous POST is
resolved only by a read using the same clientAlgoId, never another POST.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
import threading
import time


class FuturesProtectionError(ValueError):
    """Fixed public error codes; no private response bodies."""


def _positive(value):
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        result = Decimal(str(value))
        if not result.is_finite() or result <= 0:
            raise ValueError
        return result
    except (ValueError, InvalidOperation, TypeError):
        raise FuturesProtectionError("PROTECTION_POSITIVE_NUMBER_REQUIRED") from None


def exact_algo_id(value):
    result = str(value or "")
    if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,19}", result):
        raise FuturesProtectionError("EXACT_PROTECTION_ALGO_ID_REQUIRED")
    return result


def exact_client_algo_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[.A-Za-z0-9_:/-]{1,36}", value):
        raise FuturesProtectionError("EXACT_PROTECTION_CLIENT_ID_REQUIRED")
    return value


def protection_native(order):
    """Map one immutable service reservation to its exact native command."""
    if (not isinstance(order, Mapping) or order.get("route") != "BINANCE_USDM_FUTURES"
            or order.get("operation") not in {"PROTECT", "CANCEL_PROTECTION"}
            or order.get("riskReducing") is not True
            or not all(order.get(key) for key in ("sessionId", "manifestHash", "accountId"))
            or not re.fullmatch(r"[A-Z0-9_]{2,40}", str(order.get("symbol") or ""))):
        raise FuturesProtectionError("EXACT_PROTECTION_RESERVATION_REQUIRED")
    native = {"broker_id": "binance-futures", "symbol": order["symbol"],
              "operation": order["operation"], "session_id": order["sessionId"],
              "manifest_hash": order["manifestHash"], "execution_purpose": "FUNCTIONAL_TEST_ONLY",
              "client_algo_id": exact_client_algo_id(order.get("clientAlgoId")), "risk_reducing": True}
    if order["operation"] == "CANCEL_PROTECTION":
        native["algo_id"] = exact_algo_id(order.get("algoId"))
        return native
    direction, mode = order.get("positionDirection"), order.get("positionMode")
    side = "BUY" if direction == "short" else "SELL"
    if direction not in {"long", "short"} or mode not in {"ONE_WAY", "HEDGE"} or order.get("side") != side:
        raise FuturesProtectionError("PROTECTION_CLOSE_DIRECTION_REQUIRED")
    native.update(side=side, position_direction=direction, position_mode=mode,
                  trigger_price=format(_positive(order.get("triggerPrice")), "f"),
                  order_type="STOP_MARKET", close_position=True, working_type="MARK_PRICE", price_protect=False)
    return native


def normalize_protection_observation(raw, claim, *, observed_at):
    """Accept only the same owned stop; missing/ambiguous fields stay unknown."""
    if not isinstance(raw, Mapping) or not isinstance(claim, Mapping):
        raise FuturesProtectionError("PROTECTION_OBSERVATION_UNKNOWN")
    row = raw.get("json", raw)
    if not isinstance(row, Mapping):
        raise FuturesProtectionError("PROTECTION_OBSERVATION_UNKNOWN")
    algo_id = exact_algo_id(row.get("algoId"))
    client_id = exact_client_algo_id(row.get("clientAlgoId"))
    if (row.get("symbol") != claim.get("symbol") or client_id != claim.get("clientAlgoId")
            or claim.get("algoId") and algo_id != exact_algo_id(claim["algoId"])):
        raise FuturesProtectionError("PROTECTION_IDENTITY_MISMATCH")
    direction = claim.get("positionDirection")
    mode = claim.get("positionMode")
    expected_side = "BUY" if direction == "short" else "SELL"
    expected_position_side = direction.upper() if mode == "HEDGE" and direction in {"long", "short"} else "BOTH"
    if (direction not in {"long", "short"} or mode not in {"ONE_WAY", "HEDGE"}
            or row.get("side") != expected_side or row.get("positionSide") != expected_position_side
            or row.get("orderType") != "STOP_MARKET" or row.get("algoType") != "CONDITIONAL"
            or row.get("closePosition") is not True or row.get("workingType") != "MARK_PRICE"
            or row.get("priceProtect") is not False
            or _positive(row.get("triggerPrice")) != _positive(claim.get("triggerPrice"))):
        raise FuturesProtectionError("PROTECTION_TERMS_MISMATCH")
    status = str(row.get("algoStatus") or "").upper()
    if status not in {"NEW", "TRIGGERING", "TRIGGERED", "FINISHED", "CANCELED", "EXPIRED", "REJECTED"}:
        raise FuturesProtectionError("PROTECTION_STATUS_UNKNOWN")
    actual_id = str(row.get("actualOrderId") or "")
    if actual_id:
        exact_algo_id(actual_id)
    if status == "NEW" and actual_id:
        raise FuturesProtectionError("PROTECTION_STATUS_ID_CONFLICT")
    return {"known": True, "algoId": algo_id, "clientAlgoId": client_id, "symbol": row["symbol"],
            "status": status, "algoStatus": status, "triggerPrice": str(row["triggerPrice"]),
            "side": row["side"], "positionSide": row["positionSide"], "closePosition": True,
            "workingType": "MARK_PRICE", "priceProtect": False, "orderType": "STOP_MARKET",
            "actualOrderId": actual_id, "observedAt": observed_at,
            "createTime": row.get("createTime"), "updateTime": row.get("updateTime"),
            "triggerTime": row.get("triggerTime")}


class FuturesProtectionTransport:
    def __init__(self, router=None, *, clock=time.time):
        if router is None:
            from .brokers import LiveBrokerRouter
            router = LiveBrokerRouter()
        self.router, self.clock = router, clock
        self._attempted = set()
        self._lock = threading.RLock()

    def _mutate(self, order, guard):
        from .short_live_order_authority import short_live_dispatch
        detached = deepcopy(dict(order))
        native = protection_native(detached)
        key = (detached["sessionId"], detached["manifestHash"], detached["operation"], detached["clientAlgoId"])
        with self._lock:
            if key in self._attempted:
                raise FuturesProtectionError("PROTECTION_MUTATION_ALREADY_ATTEMPTED")
            # Keep the attempt consumed even if the network outcome is unknown.
            self._attempted.add(key)
            with short_live_dispatch(detached, native, guard):
                if detached["operation"] == "PROTECT":
                    return self.router.place_futures_protection(native)
                return self.router.cancel_futures_protection(native)

    def protect(self, order, guard):
        if order.get("operation") != "PROTECT":
            raise FuturesProtectionError("PROTECTION_OPERATION_REQUIRED")
        return self._mutate(order, guard)

    def cancel(self, order, guard):
        if order.get("operation") != "CANCEL_PROTECTION":
            raise FuturesProtectionError("PROTECTION_CANCEL_OPERATION_REQUIRED")
        return self._mutate(order, guard)

    def lookup(self, claim):
        started = datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()
        try:
            raw = self.router.get_futures_protection(str(claim["symbol"]),
                algo_id=str(claim.get("algoId") or ""), client_algo_id=exact_client_algo_id(claim.get("clientAlgoId")))
            return normalize_protection_observation(raw, claim, observed_at=started)
        except Exception as exc:
            return {"known": False, "status": "UNKNOWN", "observedAt": started,
                    "blockers": [str(exc) if isinstance(exc, FuturesProtectionError) else "PROTECTION_LOOKUP_UNKNOWN"]}
