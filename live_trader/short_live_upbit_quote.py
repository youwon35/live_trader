"""Pure Upbit ticker time semantics. No transport, settings or trading authority.

Official ticker ``timestamp`` records when information was reflected;
``trade_timestamp`` identifies the latest trade, which need not happen every
ten seconds. Neither field may be replaced with a local receive time.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import math
import re

SCHEMA = "upbit-quote-observation-v1"
SOURCE_MAX_AGE_SECONDS = 10
RECEIVE_MAX_AGE_SECONDS = 10
FUTURE_MAX_SECONDS = 2
PURPOSES = frozenset({"INVENTORY_VALUATION", "ORDER_MARKET"})
_SYMBOL = re.compile(r"^[A-Z0-9]{1,16}-[A-Z0-9]{1,48}$")


class UpbitQuoteObservationError(ValueError):
    def __init__(self, code, observation=None):
        super().__init__(code)
        self.observation = observation


def _number(value):
    if type(value) not in (int, float):
        return None
    try:
        return float(value) if math.isfinite(value) else None
    except (OverflowError, ValueError):
        return None


def _stamp(value):
    value = _number(value)
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="microseconds")
    except (ValueError, OverflowError, OSError):
        return None


def _from_stamp(value):
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return None
        result = parsed.timestamp()
        return result if value == _stamp(result) else None
    except (ValueError, OverflowError, OSError):
        return None


def _milliseconds(value):
    if type(value) is not int or value <= 0 or value > 253402300799999:
        return None
    return value if _stamp(value / 1000) is not None else None


def _observation(symbol, purpose, source_ms, trade_ms, started, received, checked):
    # Use the same aware UTC microsecond representation retained publicly.
    # This never substitutes a client timestamp for either provider field.
    started_at, received_at, checked_at = (_stamp(v) for v in (started, received, checked))
    started, received, checked = (_from_stamp(v) for v in (started_at, received_at, checked_at))
    source_ms, trade_ms = _milliseconds(source_ms), _milliseconds(trade_ms)
    source_at = _stamp(source_ms / 1000) if source_ms is not None else None
    trade_at = _stamp(trade_ms / 1000) if trade_ms is not None else None
    age = lambda at: round(checked - at, 6) if checked is not None and at is not None else None
    source_age = age(source_ms / 1000 if source_ms is not None else None)
    trade_age = age(trade_ms / 1000 if trade_ms is not None else None)
    receive_age = age(received)
    elapsed = round(received - started, 6) if received is not None and started is not None else None
    issue = "NONE"
    if None in (started, received, checked):
        issue = "READ_CLOCK_INVALID"
    elif source_ms is None:
        issue = "SOURCE_TIMESTAMP_MISSING_OR_INVALID"
    elif trade_ms is None:
        issue = "LAST_TRADE_TIMESTAMP_MISSING_OR_INVALID"
    elif source_age < -FUTURE_MAX_SECONDS:
        issue = "SOURCE_TIMESTAMP_FUTURE"
    elif source_age > SOURCE_MAX_AGE_SECONDS:
        issue = "SOURCE_TIMESTAMP_STALE"
    elif trade_age < -FUTURE_MAX_SECONDS:
        issue = "LAST_TRADE_TIMESTAMP_FUTURE"
    elif trade_ms > source_ms:
        issue = "LAST_TRADE_AFTER_SOURCE_TIMESTAMP"
    elif elapsed < -FUTURE_MAX_SECONDS or receive_age < -FUTURE_MAX_SECONDS:
        issue = "READ_CLOCK_INVALID"
    elif elapsed > RECEIVE_MAX_AGE_SECONDS:
        issue = "QUOTE_READ_TOO_SLOW"
    elif receive_age > RECEIVE_MAX_AGE_SECONDS:
        issue = "RECEIVE_TIMESTAMP_STALE"
    return {"schemaVersion": SCHEMA, "symbol": symbol, "purpose": purpose,
            "boundary": "QUOTE_READ_VALIDATION", "observationOnly": True,
            "status": "FRESH" if issue == "NONE" else "BLOCKED", "issue": issue,
            "sourceTimestampMs": source_ms, "lastTradeTimestampMs": trade_ms,
            "sourceObservedAt": source_at, "lastTradeObservedAt": trade_at,
            "requestStartedAt": started_at, "receivedAt": received_at, "checkedAt": checked_at,
            "sourceAgeSeconds": source_age, "lastTradeAgeSeconds": trade_age,
            "receiveAgeSeconds": receive_age, "readElapsedSeconds": elapsed,
            "maxSourceAgeSeconds": SOURCE_MAX_AGE_SECONDS,
            "maxReceiveAgeSeconds": RECEIVE_MAX_AGE_SECONDS, "maxFutureSeconds": FUTURE_MAX_SECONDS}


def validate_upbit_quote_observation(row, symbol, *, purpose, request_started, received_at, checked_at):
    """Validate the current ticker snapshot, retaining last-trade time separately."""
    if (not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol)
            or not isinstance(row, Mapping) or row.get("market") != symbol):
        raise UpbitQuoteObservationError("EXACT_MARKET_TRUTH_REQUIRED")
    if not isinstance(purpose, str) or purpose not in PURPOSES:
        raise UpbitQuoteObservationError("EXACT_MARKET_TRUTH_REQUIRED")
    observation = _observation(symbol, purpose, row.get("timestamp"), row.get("trade_timestamp"),
                               request_started, received_at, checked_at)
    issue = observation["issue"]
    if issue != "NONE":
        if issue in {"SOURCE_TIMESTAMP_STALE", "SOURCE_TIMESTAMP_FUTURE", "LAST_TRADE_TIMESTAMP_FUTURE"}:
            code = "SOURCE_QUOTE_OBSERVATION_STALE"
        elif issue in {"QUOTE_READ_TOO_SLOW", "RECEIVE_TIMESTAMP_STALE", "READ_CLOCK_INVALID"}:
            code = "MARKET_OBSERVATION_STALE"
        else:
            code = "UPBIT_QUOTE_SOURCE_TIME_UNKNOWN"
        raise UpbitQuoteObservationError(code, observation)
    return observation


def sanitize_quote_observation(value):
    """Accept only our internally consistent safe metadata, never raw payloads.

    Recompute status, ages and fixed limits rather than trusting a caller's
    FRESH string. This is display evidence, never an order authority check.
    """
    if not isinstance(value, Mapping) or value.get("schemaVersion") != SCHEMA:
        return None
    symbol, purpose = value.get("symbol"), value.get("purpose")
    if (not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol)
            or not isinstance(purpose, str) or purpose not in PURPOSES):
        return None
    for field in ("sourceTimestampMs", "lastTradeTimestampMs"):
        if value.get(field) is not None and type(value.get(field)) is not int:
            return None
    for field in ("sourceAgeSeconds", "lastTradeAgeSeconds", "receiveAgeSeconds", "readElapsedSeconds"):
        if value.get(field) is not None and _number(value.get(field)) is None:
            return None
    safe = _observation(symbol, purpose, value.get("sourceTimestampMs"), value.get("lastTradeTimestampMs"),
                        _from_stamp(value.get("requestStartedAt")), _from_stamp(value.get("receivedAt")),
                        _from_stamp(value.get("checkedAt")))
    if any(value.get(key) != expected for key, expected in safe.items()):
        return None
    return safe
