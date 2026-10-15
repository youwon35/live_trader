"""Convert absolute KRW risk ceilings to native quote units, fail closed.

Percentage limits remain outside this module. Public USDT/KRW is a market
conversion observation; USDT is never presumed equal to a US dollar.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, localcontext
import math
from collections.abc import Mapping
from copy import deepcopy
import threading
from email.utils import parsedate_to_datetime, format_datetime
from time import monotonic_ns

from trading_runtime.artifact_governance import stable_sha256
from .native_spot_quantity import decimal_value

SCHEMA = "native-krw-conversion-observation-v1"
MAX_AGE_SECONDS = 15
UPBIT_REFERENCE = "https://docs.upbit.com/kr/reference/list-tickers"
_CACHE = {}
_CACHE_LOCK = threading.RLock()
_PRODUCER_LOCK = threading.Lock()


def _timestamp(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("NATIVE_FX_TIME_INVALID")
    return result


def _seal(body):
    body = {key: value for key, value in body.items() if key != "contentHash"}
    return {**body, "contentHash": stable_sha256(body)}


def observe_upbit_usdt_krw(reader, *, now=None):
    """One official public GET through the existing read-only transport."""
    started = now or datetime.now(timezone.utc)
    tick = monotonic_ns()
    response, headers = reader._get("upbit", "/v1/ticker", query={"markets": "KRW-USDT"})
    round_trip = monotonic_ns() - tick
    received = now or datetime.now(timezone.utc)
    if not isinstance(response, list) or len(response) != 1 or not isinstance(response[0], Mapping):
        raise ValueError("NATIVE_FX_EXACT_USDT_MARKET_REQUIRED")
    row = response[0]
    trade_ms, update_ms = row.get("trade_timestamp"), row.get("timestamp")
    if row.get("market") != "KRW-USDT" or type(trade_ms) is not int or type(update_ms) is not int:
        raise ValueError("NATIVE_FX_EXACT_USDT_TRADE_TIME_REQUIRED")
    price = decimal_value(row.get("trade_price"), "KRW per USDT", positive=True)
    observation = _seal({"schemaVersion": SCHEMA, "provider": "upbit", "nativeCurrency": "USDT",
        "counterCurrency": "KRW", "krwPerNativeCurrency": str(price), "symbol": "KRW-USDT",
        "endpoint": "/v1/ticker", "requestStartedAt": started.isoformat(), "receivedAt": received.isoformat(),
        "rateAsOf": datetime.fromtimestamp(trade_ms / 1000, timezone.utc).isoformat(),
        "sourceUpdateAsOf": datetime.fromtimestamp(update_ms / 1000, timezone.utc).isoformat(),
        "httpClock": {"version":"upbit-http-date-clock-interval-v1", "dateHeader":headers.get("date"),
                      "ageHeader":headers.get("age"), "datePrecisionSeconds":1,
                      "requestStartedAt":started.isoformat(), "receivedAt":received.isoformat(),
                      "roundTripNanoseconds":round_trip,
                      "headersHash":stable_sha256({"date":headers.get("date"),"age":headers.get("age")})},
        "officialResponse": response, "officialResponseHash": stable_sha256(response),
        "reference": UPBIT_REFERENCE, "rateRole": "OBSERVED_MARKET_TRADE_NOT_USD_PEG", "orderAuthorityGranted": False})
    validate_conversion_observation(observation, native_currency="USDT", now=received)
    return observation


def _validate_http_clock(clock, *, started, received, current):
    if not isinstance(clock, Mapping) or clock.get("version") != "upbit-http-date-clock-interval-v1" or type(clock.get("datePrecisionSeconds")) is not int or clock["datePrecisionSeconds"] != 1:
        raise ValueError("NATIVE_FX_HTTP_CLOCK_PROOF_MISSING")
    if clock.get("headersHash") != stable_sha256({"date":clock.get("dateHeader"),"age":clock.get("ageHeader")}):
        raise ValueError("NATIVE_FX_HTTP_CLOCK_HEADER_BINDING_CHANGED")
    age = clock.get("ageHeader")
    if age is not None and str(age).strip() != "0":
        raise ValueError("NATIVE_FX_CACHED_HTTP_RESPONSE_REJECTED")
    raw = clock.get("dateHeader")
    try:
        lower = parsedate_to_datetime(raw)
        if lower.tzinfo is None or lower.utcoffset() != timedelta(0) or format_datetime(lower, usegmt=True) != raw:
            raise ValueError()
    except (ValueError, TypeError, IndexError):
        raise ValueError("NATIVE_FX_OFFICIAL_HTTP_DATE_MISSING_OR_INVALID") from None
    upper = lower + timedelta(seconds=1)
    if _timestamp(clock.get("requestStartedAt")) != started or _timestamp(clock.get("receivedAt")) != received:
        raise ValueError("NATIVE_FX_HTTP_CLOCK_LOCAL_BINDING_CHANGED")
    elapsed = clock.get("roundTripNanoseconds")
    if type(elapsed) is not int or elapsed < 0:
        raise ValueError("NATIVE_FX_HTTP_CLOCK_RTT_INVALID")
    rtt = max((received-started).total_seconds(), elapsed/1_000_000_000)
    if (not started <= received <= current or not 0 <= rtt <= MAX_AGE_SECONDS
        or max(abs((server-local).total_seconds()) for server in (lower,upper) for local in (started,received)) > MAX_AGE_SECONDS):
        raise ValueError("NATIVE_FX_HTTP_CLOCK_SKEW_OR_RTT_EXCEEDED")
    return upper, rtt


def validate_conversion_observation(observation, *, native_currency, now=None):
    """Only an implemented official producer may attest a conversion route.

    No KIS USD rate producer has yet supplied a timestamped authoritative
    schema; USD stays blocked instead of accepting an arbitrary rate field.
    """
    if native_currency != "USDT":
        raise ValueError("NATIVE_FX_OFFICIAL_CURRENCY_ROUTE_UNAVAILABLE")
    if not isinstance(observation, Mapping):
        raise ValueError("NATIVE_FX_OBSERVATION_MISSING")
    body = {key: value for key, value in observation.items() if key != "contentHash"}
    if observation.get("contentHash") != stable_sha256(body) or body.get("schemaVersion") != SCHEMA:
        raise ValueError("NATIVE_FX_OBSERVATION_SEAL_INVALID")
    expected = {"provider": "upbit", "nativeCurrency": "USDT", "counterCurrency": "KRW", "symbol": "KRW-USDT",
                "endpoint": "/v1/ticker", "reference": UPBIT_REFERENCE, "rateRole": "OBSERVED_MARKET_TRADE_NOT_USD_PEG", "orderAuthorityGranted": False}
    if any(body.get(key) != value for key, value in expected.items()):
        raise ValueError("NATIVE_FX_OBSERVATION_CURRENCY_OR_ROUTE_MISMATCH")
    raw = body.get("officialResponse")
    if not isinstance(raw, list) or len(raw) != 1 or not isinstance(raw[0], Mapping) or body.get("officialResponseHash") != stable_sha256(raw):
        raise ValueError("NATIVE_FX_OFFICIAL_RESPONSE_BINDING_CHANGED")
    row = raw[0]
    if row.get("market") != "KRW-USDT" or type(row.get("trade_timestamp")) is not int or type(row.get("timestamp")) is not int:
        raise ValueError("NATIVE_FX_OFFICIAL_RESPONSE_SCOPE_CHANGED")
    price = decimal_value(body.get("krwPerNativeCurrency"), "observed KRW per USDT", positive=True)
    if price != decimal_value(row.get("trade_price"), "official KRW per USDT", positive=True):
        raise ValueError("NATIVE_FX_RATE_RESPONSE_CHANGED")
    rate_at = datetime.fromtimestamp(row["trade_timestamp"] / 1000, timezone.utc)
    update_at = datetime.fromtimestamp(row["timestamp"] / 1000, timezone.utc)
    if _timestamp(body.get("rateAsOf")) != rate_at or _timestamp(body.get("sourceUpdateAsOf")) != update_at:
        raise ValueError("NATIVE_FX_TIME_RESPONSE_CHANGED")
    started, received = _timestamp(body.get("requestStartedAt")), _timestamp(body.get("receivedAt"))
    current = now or datetime.now(timezone.utc)
    server_upper, rtt = _validate_http_clock(body.get("httpClock"), started=started, received=received, current=current)
    if (not rate_at <= update_at or not 0 <= (current-received).total_seconds() <= MAX_AGE_SECONDS
        or any(not 0 <= (server_upper-at).total_seconds()+rtt+(current-received).total_seconds() <= MAX_AGE_SECONDS or at > server_upper for at in (rate_at,update_at))):
        raise ValueError("NATIVE_FX_OBSERVATION_STALE_OR_FUTURE")
    return dict(observation)


def latest_conversion_observation(native_currency, broker_id, *, now=None):
    """Consume only an internally produced, still-fresh public observation."""
    key = (native_currency, broker_id)
    with _CACHE_LOCK:
        value = deepcopy(_CACHE.get(key))
    try:
        return validate_conversion_observation(value, native_currency=native_currency, now=now)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        with _CACHE_LOCK:
            _CACHE.pop(key, None)
        return None


def refresh_conversion_observation(reader, *, native_currency, broker_id, now=None):
    """Read outside the risk-cycle lock; failure removes conversion authority.

    A short five-second reuse period avoids repeated public GETs while sizing
    several portfolio members. Consumer expiry remains fifteen seconds.
    """
    key = (native_currency, broker_id)
    if key != ("USDT", "binance"):
        return None
    # One process-local producer for the shared public market mark. No private
    # account fields, history or credentials are put into the cache.
    if not _PRODUCER_LOCK.acquire(blocking=False):
        return latest_conversion_observation(native_currency, broker_id, now=now)
    try:
        existing = latest_conversion_observation(native_currency, broker_id, now=now)
        current = now or datetime.now(timezone.utc)
        if existing and (current - _timestamp(existing["receivedAt"])).total_seconds() <= 5:
            return existing
        try:
            receipt = observe_upbit_usdt_krw(reader, now=now)
            with _CACHE_LOCK:
                _CACHE[key] = deepcopy(receipt)
            return receipt
        except Exception:
            with _CACHE_LOCK:
                _CACHE.pop(key, None)
            return None
    finally:
        _PRODUCER_LOCK.release()


def native_absolute_limits(krw_limit, *, native_currency, broker_id, conversion_observation=None, fx_buffer_bps=50, now=None):
    """Return native ceilings; a blocked conversion has zero authority/caps."""
    result = {"status": "BLOCKED", "nativeCurrency": native_currency, "limitCurrency": "KRW",
              "maxOrderValue": "0", "strategyCapitalLimit": "0", "nativeLimitExact": "0",
              "reasons": [], "orderAuthorityGranted": False}
    try:
        limit = decimal_value(krw_limit, "absolute KRW limit")
        result["sourceLimitKrw"] = str(limit)
        if (broker_id, native_currency) not in {("upbit", "KRW"), ("kis", "KRW"), ("kis", "USD"), ("binance", "USDT")}:
            raise ValueError("NATIVE_ABSOLUTE_LIMIT_BROKER_CURRENCY_UNSUPPORTED")
        receipt = None
        if native_currency == "KRW":
            rate = Decimal(1)
            buffered_rate = rate
        else:
            receipt = validate_conversion_observation(conversion_observation, native_currency=native_currency, now=now)
            rate = decimal_value(receipt["krwPerNativeCurrency"], "KRW per native", positive=True)
            buffer = decimal_value(fx_buffer_bps, "saved FX risk buffer bps")
            if buffer > 10000:
                raise ValueError("NATIVE_FX_RISK_BUFFER_OUT_OF_RANGE")
            with localcontext() as precision:
                precision.prec = 2048
                buffered_rate = rate * (1 + buffer / 10000)
        with localcontext() as precision:
            precision.prec = 2048
            exact = limit / buffered_rate
            # Explicit conservative native-money transport: never round a KRW
            # ceiling upward when the existing risk engine consumes floats.
            native = exact.quantize(Decimal('0.000000000000000001'), rounding=ROUND_DOWN)
            transported = float(native)
            if not math.isfinite(transported):
                raise ValueError("NATIVE_ABSOLUTE_LIMIT_FLOAT_OVERFLOW")
            if transported > 0 and Decimal.from_float(transported) > native:
                transported = math.nextafter(transported, 0.0)
            text = str(transported)
        result.update(status="READY", maxOrderValue=text, strategyCapitalLimit=text,
            nativeLimitExact=format(native, 'f'), krwPerNativeCurrency=str(rate),
            conservativeKrwPerNativeCurrency=str(buffered_rate), fxBufferBps=str(buffer) if receipt else "0",
            conversionObservationHash=receipt["contentHash"] if receipt else None,
            conversionRole="CURRENT_NATIVE_MONEY_RISK_CEILING_NOT_SOURCE_SIZING", sourcePolicyUnchanged=True)
    except (ValueError, KeyError, TypeError, ArithmeticError, OverflowError) as error:
        result["reasons"].append(str(error))
    return result
