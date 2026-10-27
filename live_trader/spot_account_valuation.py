from __future__ import annotations

"""Public, display-only spot valuation; never supplies order or admission truth."""

import copy
import math
import re
import time
from datetime import datetime, timezone
from typing import Callable

PUBLIC_ORIGINS = {"binance": "https://api.binance.com", "upbit": "https://api.upbit.com"}
PUBLIC_URLS = {
    "binance": "https://api.binance.com/api/v3/ticker/price",
    "upbit": "https://api.upbit.com/v1/ticker/all?quote_currencies=KRW",
}
MAX_QUOTE_AGE_SECONDS = 60.0
MAX_READ_SECONDS = 5.0


def _positive(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def enrich_spot_display_valuation(
    provider: str,
    positions: list[dict[str, object]],
    reader: Callable[[str], dict[str, object]],
    *,
    configured_origin: str,
    wall_clock: Callable[[], float] = time.time,
    monotonic: Callable[[], float] = time.monotonic,
) -> list[dict[str, object]]:
    """Attach one fresh public quote observation without altering financial fields.

    A single public batch avoids invalid/delisted assets failing all other held
    assets. Missing, duplicate, non-finite, stale or wrong-market quotes stay
    unknown. Cash, broker equity, cost basis, futures and order gates are untouched.
    """
    result = copy.deepcopy(positions)
    if provider not in PUBLIC_ORIGINS:
        return result
    currency = "USDT" if provider == "binance" else "KRW"
    for position in result:
        position["display_valuation"] = {
            "status": "UNKNOWN", "current_price": None, "value": None,
            "currency": currency, "observed_at": None, "quote_at": None,
            "source": PUBLIC_URLS[provider], "order_authority_granted": False,
            "reason": "현재 평가 시세를 확인하지 못했습니다.",
        }
    if not result:
        return result
    if configured_origin.rstrip("/") != PUBLIC_ORIGINS[provider]:
        return result
    started = monotonic()
    started_wall = wall_clock()
    try:
        response = reader(PUBLIC_URLS[provider])
    except Exception:
        # Transport exception text may include request information. Never expose it.
        return result
    finished = monotonic()
    received = wall_clock()
    if (
        finished < started or finished - started > MAX_READ_SECONDS
        or not math.isfinite(received) or not math.isfinite(started_wall)
        or abs((received - started_wall) - (finished - started)) > 1.0
        or not isinstance(response, dict) or response.get("ok") is not True
        or not isinstance(response.get("json"), list)
    ):
        return result
    payload = response["json"]
    if len(payload) > 20000:
        return result
    key_field = "symbol" if provider == "binance" else "market"
    price_field = "price" if provider == "binance" else "trade_price"
    by_symbol: dict[str, dict[str, object]] = {}
    duplicate: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        key = str(item.get(key_field) or "")
        if key in by_symbol:
            duplicate.add(key)
        by_symbol[key] = item
    observed_at = datetime.fromtimestamp(received, timezone.utc).isoformat()
    for position in result:
        observation = position["display_valuation"]
        symbol = str(position.get("symbol") or "")
        asset = symbol if provider == "binance" else symbol.rsplit("-", 1)[-1]
        if not re.fullmatch(r"[A-Z0-9]{1,32}", asset):
            continue
        pair = asset + "USDT" if provider == "binance" else "KRW-" + asset
        quote = by_symbol.get(pair)
        if pair in duplicate or not quote:
            continue
        price = _positive(quote.get(price_field))
        quantity = _positive(position.get("broker_qty"))
        if price is None or quantity is None or not math.isfinite(price * quantity):
            continue
        quote_at = None
        if provider == "upbit":
            stamp = _positive(quote.get("timestamp"))
            if stamp is None or not -5.0 <= received - stamp / 1000 <= MAX_QUOTE_AGE_SECONDS:
                continue
            quote_at = datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat()
        observation.update(
            status="OBSERVED", current_price=price, value=price * quantity,
            observed_at=observed_at, quote_at=quote_at, symbol=pair,
            expires_at=datetime.fromtimestamp(received + MAX_QUOTE_AGE_SECONDS, timezone.utc).isoformat(),
            timestamp_basis="exchange_snapshot" if quote_at else "http_receipt",
            reason="공개 현재가 조회 시점의 표시용 평가입니다. 주문 가능 금액이나 체결 보장이 아닙니다.",
        )
    return result
