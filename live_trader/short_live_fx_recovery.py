"""Bounded public FX re-observation; never retries an account or order call.

A successful HTTP response can contain an old book timestamp. Only that
specific, fully validated age failure may wait briefly for a new public book.
The original timestamp, 15-second gate and HTTP clock proof remain unchanged.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import math
import time

from .native_fx_observation import sanitize_fx_observation

MAX_ATTEMPTS = 3
BUDGET_SECONDS = 3.0
WAIT_SECONDS = 0.5
SCHEMA = "short-live-fx-recovery-v1"


def _aged_source(value):
    evidence = sanitize_fx_observation(value)
    return evidence if (evidence is not None
        and evidence["validationCode"] == "NATIVE_FX_OBSERVATION_STALE_OR_FUTURE"
        and evidence["clockIssue"] == "NATIVE_FX_OBSERVATION_STALE_OR_FUTURE"
        and evidence["sourceAgeSeconds"] is not None and evidence["sourceAgeSeconds"] >= 0
        and evidence["sourceAgeUpperSeconds"] is not None
        and evidence["sourceAgeUpperSeconds"] > evidence["maxAgeSeconds"]
        and evidence["receiveAgeSeconds"] == 0
        and evidence["readElapsedSeconds"] is not None
        and 0 <= evidence["readElapsedSeconds"] <= 1.0) else None


def sanitize_fx_recovery(value):
    if not isinstance(value, Mapping) or value.get("schemaVersion") != SCHEMA:
        return None
    status, count, elapsed = value.get("status"), value.get("attemptCount"), value.get("elapsedMs")
    try:
        finite_elapsed = type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0
    except (ValueError, OverflowError):
        finite_elapsed = False
    if (status not in ("RECOVERED", "EXHAUSTED", "ABORTED")
            or type(count) is not int or not 1 <= count <= MAX_ATTEMPTS
            or not finite_elapsed
            or value.get("orderResubmitted") is not False):
        return None
    first = _aged_source(value.get("firstObservation"))
    last = sanitize_fx_observation(value.get("lastObservation"))
    if first is None or (status == "RECOVERED" and (last is None or last["status"] != "FRESH")):
        return None
    return {"schemaVersion": SCHEMA, "status": status, "attemptCount": count,
        "elapsedMs": elapsed, "maxAttempts": MAX_ATTEMPTS, "budgetSeconds": BUDGET_SECONDS,
        "cause": "SOURCE_TIMESTAMP_AGED", "firstObservation": first,
        "lastObservation": last, "orderResubmitted": False}


def public_reader(remaining_seconds):
    """A separate public-only transport; never mutate a private reader timeout."""
    from .read_only_account import ReadOnlyBrokerReader, ReadOnlyTransport
    base = ReadOnlyBrokerReader({}, transport=ReadOnlyTransport(timeout=min(1.0, remaining_seconds)))

    class ExactPublicBookReader:
        def _get(self, provider, path, *, query):
            if (provider, path, query) != ("upbit", "/v1/orderbook",
                    {"markets": "KRW-USDT", "count": 1, "level": "0"}):
                raise ValueError("READ_ONLY_ENDPOINT_BLOCKED")
            return base._get(provider, path, query=query)

    return ExactPublicBookReader()


def observe_with_recovery(reader_factory, *, clock=time.time, monotonic=time.monotonic,
                          sleeper=time.sleep, can_continue=lambda: True):
    """Return one validated fresh receipt plus optional historical retry evidence.

    The factory receives the remaining whole-call budget and must bound its
    transport timeout. The production factory uses at most one second. No
    background thread, cached fallback, timestamp rewrite or order resubmit is
    created. Future/invalid timestamps and transport failures are not retried.
    """
    from .account_currency_limits import (CurrentFxObservationError, observe_upbit_usdt_krw,
        validate_conversion_observation, conversion_observation_diagnostic)
    began, first, last, failure = monotonic(), None, None, None
    attempts = 0

    def evidence(status):
        return sanitize_fx_recovery({"schemaVersion": SCHEMA, "status": status,
            "attemptCount": attempts, "elapsedMs": round(max(0, monotonic()-began)*1000, 3),
            "firstObservation": first, "lastObservation": last, "orderResubmitted": False})

    def fail(exc, status):
        detail = evidence(status) if first is not None else None
        if detail is not None:
            try:
                exc.fxRecovery = detail
            except (AttributeError, TypeError):
                pass
        raise exc

    while attempts < MAX_ATTEMPTS:
        remaining = BUDGET_SECONDS - (monotonic()-began)
        if not can_continue():
            fail(TimeoutError("FX_PUBLIC_OBSERVATION_CANCELLED"), "ABORTED")
        if remaining <= 0:
            fail(failure or TimeoutError("FX_PUBLIC_OBSERVATION_DEADLINE"), "EXHAUSTED")
        attempts += 1
        try:
            raw = observe_upbit_usdt_krw(reader_factory(remaining))
            now = datetime.fromtimestamp(clock(), timezone.utc)
            verified = validate_conversion_observation(raw, native_currency="USDT", now=now)
            last = conversion_observation_diagnostic(verified, now=now)
            if monotonic()-began > BUDGET_SECONDS or not can_continue():
                fail(TimeoutError("FX_PUBLIC_OBSERVATION_DEADLINE"), "ABORTED")
            return verified, evidence("RECOVERED") if first is not None else None
        except CurrentFxObservationError as exc:
            last = sanitize_fx_observation(exc.fxObservation)
            aged = _aged_source(last)
            if aged is None:
                fail(exc, "ABORTED")
            first = first or aged
            failure = exc
        except Exception as exc:
            fail(exc, "ABORTED")
        remaining = BUDGET_SECONDS - (monotonic()-began)
        if attempts >= MAX_ATTEMPTS or remaining <= WAIT_SECONDS:
            fail(failure, "EXHAUSTED")
        sleeper(WAIT_SECONDS)
    fail(failure, "EXHAUSTED")
