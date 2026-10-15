"""Bound official Binance time to local request intervals for average-price age.

This clock applies only to the average-price last-trade reference. It does not
extend native observation expiry or the WebSocket next-open boundary deadline.
"""
from datetime import datetime, timezone
import re
from time import monotonic_ns

from trading_runtime.artifact_governance import stable_sha256

VERSION = "binance-market-reference-clock-v1"
ROLE = "MARKET_REFERENCE_ONLY_NO_BOUNDARY_DEADLINE_CHANGE"
MAX_SECONDS = 15


def _timestamp(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("CURRENT_EXCHANGE_CLOCK_TIME_INVALID")
    return result


def observe_clock(reader, *, now=None):
    started = now or datetime.now(timezone.utc)
    tick = monotonic_ns()
    response, _ = reader._get("binance", "/api/v3/time")
    elapsed = monotonic_ns() - tick
    received = now or datetime.now(timezone.utc)
    milliseconds = response.get("serverTime")
    evidence = {"version": VERSION, "role": ROLE, "endpoint": "/api/v3/time",
                "serverTimeMilliseconds": milliseconds, "requestStartedAt": started.isoformat(),
                "responseReceivedAt": received.isoformat(), "roundTripNanoseconds": elapsed,
                "responseHash": stable_sha256(response)}
    validate_clock(evidence, as_of=received, now=received)
    return evidence


def validate_clock(evidence, *, as_of, now):
    if not isinstance(evidence, dict) or evidence.get("version") != VERSION or evidence.get("role") != ROLE or evidence.get("endpoint") != "/api/v3/time":
        raise ValueError("CURRENT_EXCHANGE_CLOCK_EVIDENCE_MISSING")
    milliseconds, nanoseconds = evidence.get("serverTimeMilliseconds"), evidence.get("roundTripNanoseconds")
    if type(milliseconds) is not int or milliseconds <= 0 or type(nanoseconds) is not int or nanoseconds < 0 or not isinstance(evidence.get("responseHash"), str) or not re.fullmatch("[a-f0-9]{64}", evidence["responseHash"]):
        raise ValueError("CURRENT_EXCHANGE_CLOCK_RESPONSE_INVALID")
    started, received = _timestamp(evidence.get("requestStartedAt")), _timestamp(evidence.get("responseReceivedAt"))
    server = datetime.fromtimestamp(milliseconds / 1000, timezone.utc)
    local_elapsed = (received - started).total_seconds()
    round_trip = max(local_elapsed, nanoseconds / 1_000_000_000)
    if not started <= received <= as_of <= now or not 0 <= round_trip <= MAX_SECONDS or max(abs((server - started).total_seconds()), abs((server - received).total_seconds())) > MAX_SECONDS:
        raise ValueError("CURRENT_EXCHANGE_CLOCK_STALE")
    return server, received, round_trip


def validate_average_reference(reference, *, as_of, now):
    if reference.get("closeTimeRole") != "LAST_TRADE_TIME" or reference.get("freshnessClock") != "BINANCE_SERVER_TIME_WITH_BOUNDED_LOCAL_RTT":
        raise ValueError("CURRENT_AVERAGE_CLOCK_BINDING_MISSING")
    clock = reference.get("clockObservation")
    server, received, round_trip = validate_clock(clock, as_of=as_of, now=now)
    average_received = _timestamp(reference.get("responseReceivedAt"))
    if not average_received <= _timestamp(clock["requestStartedAt"]) or not 0 <= (as_of - average_received).total_seconds() <= MAX_SECONDS:
        raise ValueError("CURRENT_AVERAGE_CLOCK_READ_SEQUENCE_INVALID")
    last_trade = _timestamp(reference.get("closeTime"))
    server_age = (server - last_trade).total_seconds()
    # Count the entire clock round trip conservatively; a future-to-server trade
    # is never accepted, even when the local clock is behind the server.
    age_upper_bound = server_age + round_trip + (now - received).total_seconds()
    if not 0 <= server_age or not 0 <= age_upper_bound <= MAX_SECONDS:
        raise ValueError("CURRENT_AVERAGE_PRICE_STALE")
    return {"lastTradeAgeUpperBoundSeconds": age_upper_bound, "referenceClockRole": ROLE}
