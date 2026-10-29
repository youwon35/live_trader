"""Pure, privacy-safe timing evidence for the current KRW/USDT order book.

This projection is for diagnostics only. A FRESH projection cannot authorize
an order; the producer separately binds and validates the whole public book.
"""
from collections.abc import Mapping
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime, format_datetime

SCHEMA = 'native-fx-clock-observation-v2'
MAX_AGE_SECONDS = 15
MAX_FUTURE_SECONDS = 2
VALIDATION_CODES = frozenset({'NONE', 'NATIVE_FX_ORDERBOOK_SCOPE_INVALID',
    'NATIVE_FX_ORDERBOOK_PRICE_INVALID', 'NATIVE_FX_ORDERBOOK_SIZE_INVALID',
    'NATIVE_FX_ORDERBOOK_TIMESTAMP_INVALID', 'NATIVE_FX_CURRENT_OBSERVATION_REQUIRED',
    'NATIVE_FX_HTTP_CLOCK_PROOF_MISSING', 'NATIVE_FX_HTTP_CLOCK_HEADER_BINDING_CHANGED',
    'NATIVE_FX_CACHED_HTTP_RESPONSE_REJECTED', 'NATIVE_FX_OFFICIAL_HTTP_DATE_MISSING_OR_INVALID',
    'NATIVE_FX_HTTP_CLOCK_LOCAL_BINDING_CHANGED', 'NATIVE_FX_HTTP_CLOCK_RTT_INVALID',
    'NATIVE_FX_HTTP_CLOCK_SKEW_OR_RTT_EXCEEDED', 'NATIVE_FX_OBSERVATION_STALE_OR_FUTURE',
    'NATIVE_FX_OBSERVATION_SEAL_INVALID', 'NATIVE_FX_OBSERVATION_CURRENCY_OR_ROUTE_MISMATCH',
    'NATIVE_FX_OFFICIAL_RESPONSE_BINDING_CHANGED', 'NATIVE_FX_RATE_RESPONSE_CHANGED',
    'NATIVE_FX_TIME_RESPONSE_CHANGED'})


def _time(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo is not None else None
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _iso(value):
    return value.isoformat(timespec='microseconds') if value is not None else None


def _source(value):
    if type(value) is not int or not 0 < value <= 253402300799999:
        return None
    try:
        return datetime.fromtimestamp(value / 1000, timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _delta_ns(a, b):
    if a is None or b is None:
        return None
    delta = a-b
    return (delta.days*86400 + delta.seconds)*1_000_000_000 + delta.microseconds*1000


def fx_clock_observation(*, source_ms, started, received, checked, date_header,
                         age_header, round_trip_ns, validation_code='NONE'):
    source = _source(source_ms)
    started, received, checked = (_time(value) for value in (started, received, checked))
    source_ms = source_ms if source is not None else None
    server = None
    try:
        candidate = parsedate_to_datetime(date_header)
        if candidate.tzinfo is not None and candidate.utcoffset() == timedelta(0) and format_datetime(candidate, usegmt=True) == date_header:
            server = candidate
    except (TypeError, ValueError, IndexError, OverflowError):
        pass
    # These are bounded public Date/Age facts, never arbitrary header strings.
    canonical_date = format_datetime(server, usegmt=True) if server is not None else None
    age = None if age_header is None else '0' if str(age_header).strip() == '0' else 'INVALID'
    ns = round_trip_ns if type(round_trip_ns) is int and 0 <= round_trip_ns <= 86_400_000_000_000 else None
    delta = lambda a, b: round((a-b).total_seconds(), 6) if a is not None and b is not None else None
    local_age, receive_age, wall_rtt = delta(checked, source), delta(checked, received), delta(received, started)
    rtt = max(wall_rtt, ns/1_000_000_000) if wall_rtt is not None and ns is not None else None
    upper = server + timedelta(seconds=1) if server is not None else None
    conservative_ns = (_delta_ns(upper, source) + max(_delta_ns(received, started), ns)
        + _delta_ns(checked, received)) if upper is not None and source is not None and ns is not None and None not in (started, received, checked) else None
    # Compare the exact nanosecond upper bound before any display rounding.
    # The public value rounds upward to microseconds, so a fraction beyond the
    # expiry can never be displayed or consumed as a permitted boundary.
    conservative_age = ((conservative_ns+999)//1000)/1_000_000 if conservative_ns is not None else None
    skew = max(abs((remote-local).total_seconds()) for remote in (server, upper) for local in (started, received)) if server and started and received else None
    issue = 'NONE'
    if None in (started, received, checked): issue = 'NATIVE_FX_HTTP_CLOCK_LOCAL_BINDING_CHANGED'
    elif server is None: issue = 'NATIVE_FX_OFFICIAL_HTTP_DATE_MISSING_OR_INVALID'
    elif age == 'INVALID': issue = 'NATIVE_FX_CACHED_HTTP_RESPONSE_REJECTED'
    elif ns is None: issue = 'NATIVE_FX_HTTP_CLOCK_RTT_INVALID'
    elif not started <= received <= checked or not 0 <= rtt <= MAX_AGE_SECONDS or skew > MAX_AGE_SECONDS:
        issue = 'NATIVE_FX_HTTP_CLOCK_SKEW_OR_RTT_EXCEEDED'
    elif source is None: issue = 'NATIVE_FX_ORDERBOOK_TIMESTAMP_INVALID'
    elif receive_age > MAX_AGE_SECONDS or local_age < -MAX_FUTURE_SECONDS or source > upper or not 0 <= conservative_ns <= MAX_AGE_SECONDS*1_000_000_000:
        issue = 'NATIVE_FX_OBSERVATION_STALE_OR_FUTURE'
    code = validation_code if isinstance(validation_code, str) and validation_code in VALIDATION_CODES else 'NATIVE_FX_CURRENT_OBSERVATION_REQUIRED'
    if code == 'NONE' and issue != 'NONE': code = issue
    return {'schemaVersion': SCHEMA, 'provider': 'upbit', 'symbol': 'KRW-USDT',
        'endpoint': '/v1/orderbook', 'observationOnly': True, 'orderAuthorityGranted': False,
        'timeBasis': 'UPBIT_ORDERBOOK_REQUEST_TIMESTAMP',
        'valueBasis': 'BEST_ASK_CONSERVATIVE_KRW_BUDGET',
        'status': 'FRESH' if code == 'NONE' else 'BLOCKED', 'validationCode': code,
        'clockIssue': issue, 'sourceTimestampMs': source_ms, 'sourceObservedAt': _iso(source),
        'requestStartedAt': _iso(started), 'receivedAt': _iso(received), 'checkedAt': _iso(checked),
        'dateHeader': canonical_date, 'ageHeader': age, 'roundTripNanoseconds': ns,
        'sourceAgeSeconds': local_age, 'sourceAgeUpperSeconds': conservative_age,
        'receiveAgeSeconds': receive_age, 'readElapsedSeconds': round(rtt, 6) if rtt is not None else None,
        'maxAgeSeconds': MAX_AGE_SECONDS, 'maxFutureSeconds': MAX_FUTURE_SECONDS}


def sanitize_fx_observation(value):
    if not isinstance(value, Mapping) or value.get('schemaVersion') != SCHEMA:
        return None
    if not isinstance(value.get('validationCode'), str) or value['validationCode'] not in VALIDATION_CODES:
        return None
    if value.get('observationOnly') is not True or value.get('orderAuthorityGranted') is not False:
        return None
    for key in ('sourceTimestampMs', 'roundTripNanoseconds'):
        if value.get(key) is not None and type(value[key]) is not int:
            return None
    for key in ('sourceAgeSeconds', 'sourceAgeUpperSeconds', 'receiveAgeSeconds', 'readElapsedSeconds'):
        if value.get(key) is not None and type(value[key]) not in (int, float):
            return None
    rebuilt = fx_clock_observation(source_ms=value.get('sourceTimestampMs'),
        started=value.get('requestStartedAt'), received=value.get('receivedAt'),
        checked=value.get('checkedAt'), date_header=value.get('dateHeader'),
        age_header=value.get('ageHeader'), round_trip_ns=value.get('roundTripNanoseconds'),
        validation_code=value['validationCode'])
    if any(value.get(key) != expected for key, expected in rebuilt.items()):
        return None
    return rebuilt


def recheck_fx_diagnostics(rows, *, checked_at):
    """Advance only the actual consumer clock, never the source or receipt.

    The caller passes already sanitized diagnostic rows. This does no I/O and
    cannot turn an earlier BLOCKED observation into FRESH.
    """
    result = []
    for row in rows:
        copied = dict(row)
        evidence = sanitize_fx_observation(row.get('fxObservation'))
        if row.get('provider') == 'upbit' and row.get('path') == '/v1/orderbook' and evidence is not None:
            current = fx_clock_observation(source_ms=evidence['sourceTimestampMs'],
                started=evidence['requestStartedAt'], received=evidence['receivedAt'],
                checked=checked_at, date_header=evidence['dateHeader'], age_header=evidence['ageHeader'],
                round_trip_ns=evidence['roundTripNanoseconds'], validation_code=evidence['validationCode'])
            copied['fxObservation'] = current
            if current['status'] == 'BLOCKED':
                copied.update(status='BLOCKED', failureKind='FX_VALIDATION')
        result.append(copied)
    return result
