"""Native KRX historical seed for the explicitly short functional Live path.

Only two official quotation GETs and the existing OAuth authentication boundary
are reachable. Historical bars never count as natural forward observations.
Official schemas: koreainvestment/open-trading-api examples_llm/domestic_stock/
inquire_time_dailychartprice and inquire_daily_itemchartprice (main).
"""
from __future__ import annotations

from bisect import bisect_right
from datetime import datetime, timedelta, timezone
import hashlib
import json
import time
from types import MethodType, SimpleNamespace
from typing import Any, Callable, Mapping
import urllib.parse
import urllib.request

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.continuous_runtime import ClosedBar
from trading_runtime.market_data import parse_timestamp
from trading_runtime.provider_backfill import _ohlcv, iso
from trading_runtime import provider_backfill_kis_kr as native

DAILY_ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
DAILY_TR_ID = "FHKST03010100"
HISTORY_PATHS = {native.ENDPOINT: native.TR_ID, DAILY_ENDPOINT: DAILY_TR_ID}
SCHEMA = "short-live-kis-native-history-v1"


class NativeKisHistoryBlocked(ValueError):
    """Safe local reason only, never raw credentials or remote response text."""


def _identity(spec: Any) -> tuple[str, str, str]:
    instrument, symbol, provider, timeframe = (str(getattr(spec, key, ""))
                                              for key in ("instrument_id", "symbol", "provider", "timeframe"))
    if (provider != "kis" or timeframe not in {"5m", "1h", "1d"} or not instrument
            or len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit()):
        raise NativeKisHistoryBlocked("KIS_NATIVE_HISTORY_SCOPE_REQUIRED")
    return instrument, symbol, timeframe


def _intervals(calendar: Mapping[str, Any], now: float, timeframe: str, limit: int, *, require_count=True):
    day = datetime.fromtimestamp(now, timezone.utc).astimezone(native.KST).date()
    if not calendar["first"] <= day <= calendar["last"]:
        raise NativeKisHistoryBlocked("KIS_HISTORY_CALENDAR_COVERAGE_MISSING")
    seconds = {"5m": 300, "1h": 3600, "1d": 86400}[timeframe]
    result = []
    for session in reversed(calendar["sessions"][:bisect_right(calendar["sessions"], day)]):
        opening, closing = native.bounds(calendar, session)
        if timeframe == "1d":
            candidates = [(opening, closing)]
        else:
            candidates = []
            start = opening
            while start < closing:
                end = min(start + timedelta(seconds=seconds), closing)
                candidates.append((start, end))
                start = end
        for start, end in reversed(candidates):
            if end.timestamp() < now:
                result.append((start, end))
                if len(result) == limit:
                    return list(reversed(result))
    if result and not require_count:
        return list(reversed(result))
    raise NativeKisHistoryBlocked("KIS_NATIVE_HISTORY_SESSION_COUNT_INSUFFICIENT")


class OfficialHistoryGet:
    """Lazy official GET adapter; constructor performs no authentication/read."""
    def __init__(self, settings: Mapping[str, Any], *, reader: Any = None):
        self.settings = dict(settings)
        if reader is None:
            from .read_only_account import ReadOnlyBrokerReader, ReadOnlyTransport, ORIGINS, ReadFailure

            class HistoryTransport(ReadOnlyTransport):
                def request(self, provider, method, path, *, query=None, headers=None, body=None):
                    if path not in HISTORY_PATHS:
                        return super().request(provider, method, path, query=query, headers=headers, body=body)
                    if (provider != "kis" or method != "GET" or body is not None
                            or (headers or {}).get("tr_id") != HISTORY_PATHS[path]):
                        raise NativeKisHistoryBlocked("KIS_HISTORY_GET_BOUNDARY_BLOCKED")
                    encoded = urllib.parse.urlencode(query or {})
                    url = ORIGINS["kis"] + path + "?" + encoded
                    request = urllib.request.Request(url, headers=headers or {}, method="GET")
                    try:
                        with self.opener.open(request, timeout=self.timeout) as response:
                            if response.geturl() != url or not 200 <= response.status < 300:
                                raise NativeKisHistoryBlocked("KIS_HISTORY_GET_REJECTED")
                            raw = response.read(512 * 1024 + 1)
                            if len(raw) > 512 * 1024:
                                raise NativeKisHistoryBlocked("KIS_HISTORY_RESPONSE_TOO_LARGE")
                            return json.loads(raw), {}
                    except NativeKisHistoryBlocked:
                        raise
                    except Exception:
                        raise ReadFailure("KIS_HISTORY_GET_UNAVAILABLE") from None

            reader = ReadOnlyBrokerReader(self.settings, transport=HistoryTransport())
        self.reader = reader

    def __call__(self, endpoint: str, tr_id: str, params: dict[str, str]):
        if HISTORY_PATHS.get(endpoint) != tr_id:
            raise NativeKisHistoryBlocked("KIS_HISTORY_ENDPOINT_NOT_WHITELISTED")
        self.reader._credentials("kis")
        token = self.reader._kis_token()
        self.reader._pace_kis()
        payload, _ = self.reader._get("kis", endpoint, query=params, headers={
            "authorization": "Bearer " + token, "appkey": self.settings["KIS_APP_KEY"],
            "appsecret": self.settings["KIS_APP_SECRET"], "tr_id": tr_id, "custtype": "P"})
        return payload


def _minute_seed(spec, intervals, fetcher, clock, objects):
    sub = SimpleNamespace(instrument_id=spec.instrument_id, symbol=spec.symbol, timeframe="5m", exchange="KRX")
    count = sum(int((end-start).total_seconds()) // 300 for start, end in intervals)
    if not 1 <= count <= 20000:
        raise NativeKisHistoryBlocked("KIS_MINUTE_HISTORY_REQUIRED_COUNT_EXCEEDS_BOUND")
    through = iso(intervals[-1][1].timestamp())
    cursor = intervals[-1][1] - timedelta(minutes=1)
    calendar = native.calendar_scope(objects)
    pages = []
    for _ in range(native.MAX_PAGES):
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": spec.symbol,
                  "FID_INPUT_DATE_1": cursor.astimezone(native.KST).strftime("%Y%m%d"),
                  "FID_INPUT_HOUR_1": cursor.astimezone(native.KST).strftime("%H%M%S"),
                  "FID_PW_DATA_INCU_YN": "Y", "FID_FAKE_TICK_INCU_YN": ""}
        requested = iso(float(clock()))
        payload = fetcher(native.ENDPOINT, native.TR_ID, params)
        pages.append(native.capture(sub, params, payload, requested, iso(float(clock()))))
        parsed = native.parse(sub, pages, through, objects=objects)
        if len(parsed) >= count:
            bars, receipt = native.seal(sub, pages, through_end=through, after_end=None, limit=count, objects=objects)
            if parse_timestamp(bars[0].start_time) != intervals[0][0]:
                raise NativeKisHistoryBlocked("KIS_NATIVE_MINUTE_ANCHOR_MISSING")
            return bars, receipt
        oldest = min(native.minute_timestamp(row) for row in pages[-1]["publicResponse"]["output2"])
        next_cursor = oldest - timedelta(minutes=1)
        opening, _ = native.bounds(calendar, oldest.astimezone(native.KST).date())
        if next_cursor < opening:
            index = bisect_right(calendar["sessions"], oldest.astimezone(native.KST).date()) - 2
            if index < 0:
                raise NativeKisHistoryBlocked("KIS_HISTORY_PREVIOUS_SESSION_MISSING")
            next_cursor = native.bounds(calendar, calendar["sessions"][index])[1] - timedelta(minutes=1)
        if next_cursor >= cursor:
            raise NativeKisHistoryBlocked("KIS_HISTORY_CURSOR_DID_NOT_MOVE")
        cursor = next_cursor
    raise NativeKisHistoryBlocked("KIS_NATIVE_HISTORY_BOUNDED_PAGINATION_EXHAUSTED")


def _daily_seed(spec, intervals, fetcher, clock, objects):
    expected = {start.astimezone(native.KST).date(): (start, end) for start, end in intervals}
    first, cursor = min(expected), max(expected)
    rows_by_day, pages = {}, []
    for _ in range(native.MAX_PAGES):
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": spec.symbol,
                  "FID_INPUT_DATE_1": first.strftime("%Y%m%d"), "FID_INPUT_DATE_2": cursor.strftime("%Y%m%d"),
                  "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"}
        requested = float(clock())
        payload = fetcher(DAILY_ENDPOINT, DAILY_TR_ID, params)
        observed = float(clock())
        if (not isinstance(payload, dict) or str(payload.get("rt_cd")) != "0"
                or not isinstance(payload.get("output2"), list) or not 1 <= len(payload["output2"]) <= 100
                or observed < requested or intervals[-1][1].timestamp() >= requested):
            raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_RESPONSE_INVALID")
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        if len(raw) > 512 * 1024:
            raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_RESPONSE_TOO_LARGE")
        public = json.loads(raw)
        pages.append({"endpoint": DAILY_ENDPOINT, "trId": DAILY_TR_ID, "params": params,
                      "requestedAt": iso(requested), "observedAt": iso(observed),
                      "publicResponse": public, "publicResponseHash": hashlib.sha256(raw).hexdigest()})
        dates = []
        for row in public["output2"]:
            if not isinstance(row, dict):
                raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_ROW_INVALID")
            text = row.get("stck_bsop_date")
            if not isinstance(text, str) or len(text) != 8 or not text.isascii() or not text.isdigit():
                raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_DATE_INVALID")
            day = datetime.strptime(text, "%Y%m%d").date()
            if not first <= day <= cursor or day not in expected:
                raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_RESPONSE_SCOPE_DIFFERS")
            dates.append(day)
            values = _ohlcv(row, ("stck_oprc", "stck_hgpr", "stck_lwpr", "stck_clpr", "acml_vol"))
            if day in rows_by_day and rows_by_day[day] != values:
                raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_DUPLICATE_CONFLICT")
            rows_by_day[day] = values
        if set(rows_by_day) == set(expected):
            break
        next_cursor = min(dates) - timedelta(days=1)
        if next_cursor >= cursor or next_cursor < first:
            raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_SESSION_ROWS_MISSING")
        cursor = next_cursor
    else:
        raise NativeKisHistoryBlocked("KIS_NATIVE_DAILY_PAGINATION_EXHAUSTED")
    receipt = {"schemaVersion": SCHEMA, "provider": "kis", "exchange": "KRX", "timeframe": "1d",
               "instrumentId": spec.instrument_id, "symbol": spec.symbol, "calendarContentHashes": native.calendar_pins(objects),
               "pages": pages, "historicalOnly": True, "brokerOrders": 0, "naturalForwardCycles": 0}
    receipt["contentHash"] = stable_sha256(receipt)
    result = []
    for day in sorted(expected):
        start, end = expected[day]
        op, hi, lo, close, volume = rows_by_day[day]
        result.append(ClosedBar(spec.instrument_id, spec.symbol, "kis", "1d", iso(start.timestamp()), iso(end.timestamp()),
                                op, hi, lo, close, volume, source_provider="kis", evidence_class="HISTORICAL_WARMUP",
                                historical_source_snapshot_hash=receipt["contentHash"],
                                source_sequence="kis-native-daily:" + receipt["contentHash"] + ":" + str(day)))
    return result, receipt


def _read_intervals(spec, intervals, timeframe, settings, fetcher, clock, objects):
    fetcher = fetcher or OfficialHistoryGet(settings)
    if timeframe == "1d":
        return _daily_seed(spec, intervals, fetcher, clock, objects)
    five, source_receipt = _minute_seed(spec, intervals, fetcher, clock, objects)
    if timeframe == "5m":
        return five, source_receipt
    source_by_start = {parse_timestamp(bar.start_time): bar for bar in five}
    receipt = {"schemaVersion": SCHEMA, "provider": "kis", "exchange": "KRX", "timeframe": "1h",
               "instrumentId": spec.instrument_id, "symbol": spec.symbol, "sourceReceiptHash": source_receipt["contentHash"],
               "sourceReceipt": source_receipt,
               "calendarContentHashes": native.calendar_pins(objects), "historicalOnly": True, "brokerOrders": 0,
               "naturalForwardCycles": 0, "intervals": [[iso(a.timestamp()), iso(b.timestamp())] for a, b in intervals]}
    receipt_hash = stable_sha256(receipt)
    result = []
    for start, end in intervals:
        members = []
        cursor = start
        while cursor < end:
            bar = source_by_start.get(cursor)
            if bar is None or parse_timestamp(bar.end_time) != cursor + timedelta(minutes=5):
                raise NativeKisHistoryBlocked("KIS_NATIVE_HOUR_HAS_MISSING_MINUTES")
            members.append(bar)
            cursor += timedelta(minutes=5)
        result.append(ClosedBar(spec.instrument_id, spec.symbol, "kis", "1h", iso(start.timestamp()), iso(end.timestamp()),
                                members[0].open, max(b.high for b in members), min(b.low for b in members), members[-1].close,
                                sum(b.volume for b in members), source_provider="kis", evidence_class="HISTORICAL_WARMUP",
                                historical_source_snapshot_hash=receipt_hash, source_sequence="kis-native-hour:" + receipt_hash + ":" + iso(end.timestamp())))
    return result, {**receipt, "contentHash": receipt_hash}


def get_warmup(spec: Any, limit: int, settings: Mapping[str, Any], *,
               fetcher: Callable[..., Any] | None = None, clock: Callable[[], float] = time.time,
               calendar_objects: Mapping[str, bytes] | None = None) -> list[ClosedBar]:
    """Exact native closed bars only; no account mutation, persistence or fallback.

    Tests inject public response fixtures and clock. Production reads start only
    when the genuine manual session calls warmup, never at module import.
    """
    _, _, timeframe = _identity(spec)
    if type(limit) is not int or not 1 <= limit <= 4096:
        raise NativeKisHistoryBlocked("KIS_HISTORY_LIMIT_INVALID")
    objects = dict(calendar_objects) if calendar_objects is not None else native.calendar_objects()
    calendar = native.calendar_scope(objects)
    intervals = _intervals(calendar, float(clock()), timeframe, limit)
    bars, _ = _read_intervals(spec, intervals, timeframe, settings, fetcher, clock, objects)
    return bars


def get_segment(spec: Any, *, after_end: str, through_end: str, settings: Mapping[str, Any],
                fetcher: Callable[..., Any] | None = None, clock: Callable[[], float] = time.time,
                calendar_objects: Mapping[str, bytes] | None = None):
    """Append a real completed startup partial as history, never as a signal."""
    _, _, timeframe = _identity(spec)
    objects = dict(calendar_objects) if calendar_objects is not None else native.calendar_objects()
    calendar = native.calendar_scope(objects)
    after, through = parse_timestamp(after_end), parse_timestamp(through_end)
    now = float(clock())
    if not after < through or through.timestamp() >= now or (through-after).days > 7:
        raise NativeKisHistoryBlocked("KIS_STARTUP_BRIDGE_BOUNDARY_INVALID")
    # The target end is bounded independently from the current latest end.
    candidates = _intervals(calendar, through.timestamp() + .000001, timeframe, 64, require_count=False)
    intervals = [(start, end) for start, end in candidates if end > after]
    if not intervals or intervals[-1][1] != through:
        raise NativeKisHistoryBlocked("KIS_STARTUP_BRIDGE_EXACT_END_MISSING")
    expected = native.next_start(calendar, after)
    if intervals[0][0] != expected:
        raise NativeKisHistoryBlocked("KIS_STARTUP_BRIDGE_CALENDAR_ANCHOR_DIFFERS")
    bars, receipt = _read_intervals(spec, intervals, timeframe, settings, fetcher, clock, objects)
    body = {"schemaVersion": SCHEMA, "provider": "kis", "exchange": "KRX",
            "instrumentId": spec.instrument_id, "symbol": spec.symbol, "timeframe": timeframe,
            "afterEnd": iso(after.timestamp()), "throughEnd": iso(through.timestamp()),
            "calendarContentHashes": native.calendar_pins(objects), "sourceReceiptHash": receipt["contentHash"],
            "sourceReceipt": receipt,
            "historicalOnly": True, "brokerOrders": 0, "naturalForwardCycles": 0,
            "normalizedBarsHash": stable_sha256([bar.to_dict() for bar in bars])}
    return bars, {**body, "contentHash": stable_sha256(body)}


def configure_feed_bridge(child: Any, specs, settings: Mapping[str, Any], callback: Callable[..., Any], *,
                          segment_reader: Callable[..., Any] = get_segment,
                          calendar_objects: Mapping[str, bytes] | None = None) -> None:
    """Install a native-only startup join on this short session's feed object.

    The shared generic bridge assumes a contiguous clock grid. KRX legitimately
    jumps from the prior session close to the next calendar opening. This local
    adapter proves that jump with the same pinned calendar and native response;
    it neither changes the shared Paper bridge nor invents overnight bars.
    """
    specs = tuple(specs)
    if getattr(child, "provider_id", None) != "kis" or not specs or not callable(callback):
        raise NativeKisHistoryBlocked("KIS_NATIVE_BRIDGE_OWNER_SCOPE_INVALID")
    if any(_identity(spec)[2] not in {"5m", "1h", "1d"} for spec in specs):
        raise NativeKisHistoryBlocked("KIS_NATIVE_BRIDGE_TIMEFRAME_INVALID")
    child.configure_warmup_bridge(specs, callback)
    exact_specs = {(spec.instrument_id, spec.timeframe): spec for spec in specs}
    objects = dict(calendar_objects) if calendar_objects is not None else native.calendar_objects()
    calendar = native.calendar_scope(objects)

    def authoritative(this, subscription, *, after_end=None, through_end=None, limit=256):
        key = (subscription.instrument_id, subscription.timeframe)
        spec = exact_specs.get(key)
        if spec is None or subscription.symbol != spec.symbol or after_end is None or through_end is None:
            raise NativeKisHistoryBlocked("KIS_NATIVE_BRIDGE_EXACT_SUBSCRIPTION_REQUIRED")
        bars, receipt = segment_reader(spec, after_end=after_end, through_end=through_end, settings=settings)
        if (receipt.get("schemaVersion") != SCHEMA or receipt.get("calendarContentHashes") != native.calendar_pins(objects)
                or receipt.get("contentHash") != stable_sha256({k: v for k, v in receipt.items() if k != "contentHash"})
                or receipt.get("historicalOnly") is not True or receipt.get("naturalForwardCycles") != 0
                or receipt.get("brokerOrders") != 0
                or receipt.get("normalizedBarsHash") != stable_sha256([bar.to_dict() for bar in bars])):
            raise NativeKisHistoryBlocked("KIS_NATIVE_BRIDGE_PUBLIC_RECEIPT_DIFFERS")
        this._startup_seed_anchor[key] = bars[-1].end_time
        return bars, receipt

    def bridge(this, subscription, partial, next_start):
        key = (subscription.instrument_id, subscription.timeframe)
        if key in this._startup_bridge_done:
            return
        scoped = this._startup_bridge_specs.get(key, ())
        spec = exact_specs.get(key)
        if (not scoped or spec is None or subscription.symbol != spec.symbol
                or partial.provider != "kis" or partial.source_provider != "kis"
                or partial.instrument_id != spec.instrument_id or partial.symbol != spec.symbol
                or partial.timeframe != spec.timeframe
                or parse_timestamp(partial.end_time) != parse_timestamp(next_start)):
            raise NativeKisHistoryBlocked("KIS_NATIVE_STARTUP_PARTIAL_SCOPE_DIFFERS")
        anchor = getattr(this, "_startup_seed_anchor", {}).get(key)
        if not anchor:
            raise NativeKisHistoryBlocked("KIS_NATIVE_STARTUP_PRIOR_HISTORY_ANCHOR_REQUIRED")
        if parse_timestamp(anchor) >= parse_timestamp(partial.end_time):
            this._startup_bridge_done.add(key)
            return
        expected = native.next_start(calendar, parse_timestamp(anchor))
        try:
            this._await_startup_partial_close(partial.end_time)
            bars, receipt = this.authoritative_warmup(subscription, after_end=anchor, through_end=partial.end_time)
            if (not bars or parse_timestamp(bars[0].start_time) != expected
                    or parse_timestamp(bars[-1].end_time) != parse_timestamp(partial.end_time)):
                raise NativeKisHistoryBlocked("KIS_NATIVE_STARTUP_EXACT_CALENDAR_JOIN_FAILED")
            callback(scoped, tuple(bars), receipt)
        except Exception:
            this._startup_seed_anchor[key] = anchor
            raise
        this._startup_bridge_done.add(key)

    child.authoritative_warmup = MethodType(authoritative, child)
    child._bridge_startup_partial = MethodType(bridge, child)
