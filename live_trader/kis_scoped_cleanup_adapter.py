"""Exact official cancel wire; no all-remaining flag, venue guess or rounding."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
from dataclasses import replace

from .kis_order_authority import KisOrderAuthorityError
from .live_adapters import build_kis_cancel_order_request, env_value


def build_kis_scoped_cleanup_request(proof, *, access_token=""):
    if not isinstance(proof, dict) or proof.get("state") != "WORKING":
        raise KisOrderAuthorityError("KIS cleanup exact working proof is required")
    if any(type(proof.get(key)) is not str or not re.fullmatch(r"(?:0|[1-9][0-9]{0,23})", proof[key])
            for key in ("remainingQuantity", "originalQuantity", "filledQuantity")):
        raise KisOrderAuthorityError("KIS cleanup canonical whole-share decimal strings are required")
    try:
        remaining = Decimal(str(proof.get("remainingQuantity")))
        original = Decimal(str(proof.get("originalQuantity")))
        filled = Decimal(str(proof.get("filledQuantity")))
    except (ValueError, InvalidOperation) as exc:
        raise KisOrderAuthorityError("KIS cleanup exact whole-share quantities are required") from exc
    if any(not value.is_finite() or value < 0 or value != value.to_integral_value() for value in (remaining, original, filled)) or remaining <= 0 or original <= 0 or filled + remaining != original:
        raise KisOrderAuthorityError("KIS cleanup partial-fill remaining quantity is invalid")
    market, venue = proof.get("market"), proof.get("venue")
    if (market, proof.get("currency")) not in {("KR", "KRW"), ("US", "USD")} or venue not in ({"KRX"} if market == "KR" else {"NASD", "NYSE", "AMEX"}):
        raise KisOrderAuthorityError("KIS cleanup exact venue/currency is required")
    if env_value("KIS_ENV").lower() not in {"", "real", "live"}:
        raise KisOrderAuthorityError("KIS cleanup requires real environment")
    if any(not re.fullmatch(r"[0-9]{1,16}", str(proof.get(key) or "")) for key in ("organizationNo", "orderNo")) or not re.fullmatch(r"[0-9]{8}", str(proof.get("orderDate") or "")):
        raise KisOrderAuthorityError("KIS cleanup exact official date/branch/order number is required")
    symbol = proof.get("symbol")
    if type(symbol) is not str or not re.fullmatch(r"[0-9]{6}" if market == "KR" else r"[A-Z][A-Z.\-]{0,9}", symbol) or proof.get("side") not in {"BUY", "SELL"}:
        raise KisOrderAuthorityError("KIS cleanup exact instrument/side is required")
    prepared = build_kis_cancel_order_request({"symbol": symbol, "asset": "KR" if market == "KR" else "US",
        "environment": "real", "broker_order_id": proof["orderNo"], "organization_no": proof["organizationNo"],
        "quantity": str(remaining), "order_type": "00", "exchange": venue, "manager_order_id": ""}, access_token=access_token)
    if prepared.blocked_reasons:
        raise KisOrderAuthorityError("KIS cleanup authenticated cancel wire is incomplete")
    body = dict(prepared.body)
    if market == "KR":
        body["QTY_ALL_ORD_YN"] = "N"
    # Enforce decimal text independently of the legacy builder's float parser.
    body["ORD_QTY"] = str(int(remaining))
    return replace(prepared, body=body)
