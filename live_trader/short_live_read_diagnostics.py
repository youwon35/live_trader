"""Pure allowlist for public short-Live read diagnostics; no runtime imports."""
from collections.abc import Mapping
import math
from .short_live_upbit_quote import sanitize_quote_observation
from .native_fx_observation import sanitize_fx_observation
from .short_live_fx_recovery import sanitize_fx_recovery

# Diagnostic identities are fixed routes only. Never retain a signed URL,
# query, request/response body, credentials or an arbitrary exception message.
DIAGNOSTIC_PATHS = {
    "binance": frozenset({
        "/api/v3/time", "/api/v3/account", "/api/v3/openOrders", "/api/v3/order",
        "/api/v3/myTrades", "/api/v3/exchangeInfo", "/api/v3/ticker/price",
        "/api/v3/account/commission"}),
    "binance-futures": frozenset({
        "/fapi/v1/time", "/fapi/v3/account", "/fapi/v1/accountConfig",
        "/fapi/v3/positionRisk", "/fapi/v1/symbolConfig", "/fapi/v1/openOrders",
        "/fapi/v1/openAlgoOrders", "/fapi/v1/exchangeInfo", "/fapi/v1/premiumIndex",
        "/fapi/v1/commissionRate", "/fapi/v1/positionSide/dual",
        "/fapi/v1/leverageBracket", "/fapi/v1/order", "/fapi/v1/userTrades",
        "/fapi/v1/algoOrder", "/fapi/v1/forceOrders"}),
    "upbit": frozenset({"/v1/accounts", "/v1/orders/open", "/v1/order",
        "/v1/orders/chance", "/v1/ticker", "/v1/orderbook", "/v1/orderbook/instruments"}),
    "kis": frozenset({"/uapi/domestic-stock/v1/trading/inquire-balance",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"}),
}


READ_STATUSES = frozenset({"AVAILABLE", "BLOCKED", "UNAVAILABLE", "REJECTED"})
FAILURE_KINDS = frozenset({"TIMEOUT", "HTTP_ERROR", "CONNECTION_ERROR", "INVALID_RESPONSE",
    "READ_BOUNDARY", "READ_REJECTED", "READ_UNAVAILABLE", "UNEXPECTED_ERROR", "FX_VALIDATION"})


def sanitize_read_diagnostics(value):
    """Return up to 32 safe observations, excluding every raw transport field."""
    if not isinstance(value, list):
        return []
    result = []
    for row in value[-32:]:
        if not isinstance(row, Mapping):
            continue
        provider, path = row.get("provider"), row.get("path")
        elapsed, status = row.get("elapsedMs"), row.get("status")
        if (not isinstance(provider, str) or not isinstance(path, str)
                or path not in DIAGNOSTIC_PATHS.get(provider, ())
                or not isinstance(status, str) or status not in READ_STATUSES
                or type(elapsed) not in (int, float)):
            continue
        try:
            if not math.isfinite(elapsed) or elapsed < 0:
                continue
        except (OverflowError, ValueError):
            continue
        safe = {"provider": provider, "path": path, "elapsedMs": elapsed, "status": status}
        kind, http, code = row.get("failureKind"), row.get("httpStatus"), row.get("brokerCode")
        if isinstance(kind, str) and kind in FAILURE_KINDS:
            safe["failureKind"] = kind
        if type(http) is int and 100 <= http <= 599:
            safe["httpStatus"] = http
        if type(code) is int and -2147483648 <= code < 0:
            safe["brokerCode"] = code
        if provider == "upbit" and path == "/v1/ticker":
            observation = sanitize_quote_observation(row.get("quoteObservation"))
            if observation is not None:
                safe["quoteObservation"] = observation
        if provider == "upbit" and path == "/v1/orderbook":
            evidence = sanitize_fx_observation(row.get("fxObservation"))
            if evidence is not None:
                safe["fxObservation"] = evidence
            recovery = sanitize_fx_recovery(row.get("fxRecovery"))
            if recovery is not None:
                safe["fxRecovery"] = recovery
        result.append(safe)
    return result
