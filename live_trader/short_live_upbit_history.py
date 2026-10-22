"""Official Upbit startup partial history for this short Live session only.

The public response is retained and independently replayed. Missing candles
remain gaps; this historical-only callback never dispatches a live decision.
Official reference: docs.upbit.com/kr/reference/list-candles-minutes and
list-candles-days. Captured response hashes cover canonical parsed JSON, not
unavailable original HTTP wire bytes.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import re
from types import MethodType
from typing import Any, Callable, Mapping
import urllib.parse

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.continuous_runtime import ClosedBar
from trading_runtime.market_data import canonical_timestamp, parse_timestamp
from trading_runtime.provider_backfill import _number, _ohlcv, iso

SCHEMA = "short-live-upbit-native-startup-history-v1"
ENDPOINTS = {"5m": ("https://api.upbit.com/v1/candles/minutes/5", 300),
             "1h": ("https://api.upbit.com/v1/candles/minutes/60", 3600),
             "1d": ("https://api.upbit.com/v1/candles/days", 86400)}


class NativeUpbitHistoryBlocked(ValueError):
    """Fixed local reasons only; remote bodies are never error messages."""


def _identity(spec):
    key = tuple(str(getattr(spec, name, "")) for name in ("instrument_id", "symbol", "provider", "timeframe"))
    if not key[0] or not re.fullmatch(r"KRW-[A-Z0-9]{1,20}", key[1]) or key[2] != "upbit" or key[3] not in ENDPOINTS:
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_EXACT_SCOPE_REQUIRED")
    return key[0], key[1], key[3]


def _public_bytes(payload):
    try:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError):
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_PUBLIC_RESPONSE_INVALID") from None
    if len(raw) > 512 * 1024:
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_RESPONSE_TOO_LARGE")
    return raw


def _material(bar):
    return {key: bar.to_dict().get(key) for key in
            ("provider", "instrumentId", "symbol", "timeframe", "startTime", "endTime", "open", "high", "low", "close", "volume")}


def _parse(spec, pages, through_end):
    instrument, symbol, timeframe = _identity(spec)
    endpoint, seconds = ENDPOINTS[timeframe]
    through = parse_timestamp(through_end)
    rows = {}
    if not isinstance(pages, list) or not 1 <= len(pages) <= 128:
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_PAGE_BOUND_INVALID")
    for page in pages:
        if not isinstance(page, dict):
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_PAGE_INVALID")
        params = page.get("params", {})
        payload = page.get("publicResponse")
        if (page.get("endpoint") != endpoint or set(params) != {"market", "count", "to"}
                or params.get("market") != symbol or type(params.get("count")) is not int
                or not 1 <= params["count"] <= 200 or not isinstance(payload, list)
                or not 1 <= len(payload) <= params["count"]
                or page.get("publicResponseHash") != hashlib.sha256(_public_bytes(payload)).hexdigest()):
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_PAGE_SCOPE_OR_HASH_DIFFERS")
        requested, observed = parse_timestamp(page["requestedAt"]), parse_timestamp(page["observedAt"])
        cursor = parse_timestamp(params["to"])
        if through >= requested or requested > observed or cursor > through:
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_REQUEST_CLOCK_INVALID")
        for row in payload:
            if not isinstance(row, dict) or row.get("market") != symbol:
                raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_MARKET_DIFFERS")
            try:
                start = datetime.fromisoformat(str(row["candle_date_time_utc"]).replace("Z", "+00:00"))
                start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start.astimezone(timezone.utc)
                end_epoch = start.timestamp() + seconds
                last_tick = _number(row.get("timestamp"), positive=True) / 1000
                values = _ohlcv(row, ("opening_price", "high_price", "low_price", "trade_price", "candle_acc_trade_volume"))
            except (KeyError, ValueError, TypeError, OverflowError):
                raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_ROW_INVALID") from None
            if (start.timestamp() % seconds or start >= cursor or end_epoch > through.timestamp()
                    or not start.timestamp() <= last_tick < end_epoch or last_tick > observed.timestamp()):
                raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_CLOSED_GRID_INVALID")
            if timeframe != "1d" and row.get("unit") != seconds // 60:
                raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_MINUTE_UNIT_DIFFERS")
            if start in rows and rows[start] != values:
                raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_DUPLICATE_CONFLICT")
            rows[start] = values
    return [ClosedBar(instrument, symbol, "upbit", timeframe, iso(start.timestamp()), iso(start.timestamp()+seconds),
                      *values, source_provider="upbit") for start, values in sorted(rows.items())]


def _seal(spec, pages, *, after_end, through_end, limit):
    instrument, symbol, timeframe = _identity(spec)
    if type(limit) is not int or not 1 <= limit <= 20000:
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_LIMIT_INVALID")
    parsed = _parse(spec, pages, through_end)
    after = parse_timestamp(after_end) if after_end is not None else None
    selected = [bar for bar in parsed if after is None or parse_timestamp(bar.end_time) > after][-limit:]
    if (not selected or selected[-1].end_time != canonical_timestamp(through_end)
            or (after is None and len(selected) != limit)):
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_EXACT_CLOSED_END_MISSING")
    cursor = after or parse_timestamp(selected[0].start_time)
    for bar in selected:
        if parse_timestamp(bar.start_time) != cursor:
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_MISSING_BUCKET")
        cursor = parse_timestamp(bar.end_time)
    body = {"schemaVersion": SCHEMA, "provider": "upbit", "instrumentId": instrument, "symbol": symbol,
            "timeframe": timeframe, "afterEnd": canonical_timestamp(after_end) if after_end is not None else None,
            "throughEnd": canonical_timestamp(through_end), "limit": limit, "count": len(selected),
            "pages": deepcopy(pages), "normalizedRowsHash": stable_sha256([_material(bar) for bar in selected]),
            "responseHashBasis": "CANONICAL_PARSED_PUBLIC_JSON", "historicalOnly": True,
            "naturalForwardCycles": 0, "brokerOrders": 0}
    receipt = {**body, "contentHash": stable_sha256(body)}
    bars = [replace(bar, evidence_class="HISTORICAL_WARMUP", historical_source_snapshot_hash=receipt["contentHash"],
                    source_sequence="upbit-native-startup:"+receipt["contentHash"]+":"+bar.end_time) for bar in selected]
    return bars, receipt


def verify_segment(spec, receipt):
    if (not isinstance(receipt, Mapping) or receipt.get("schemaVersion") != SCHEMA
            or receipt.get("historicalOnly") is not True or type(receipt.get("naturalForwardCycles")) is not int
            or receipt["naturalForwardCycles"] != 0 or type(receipt.get("brokerOrders")) is not int
            or receipt["brokerOrders"] != 0
            or receipt.get("contentHash") != stable_sha256({k: v for k, v in receipt.items() if k != "contentHash"})):
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_RECEIPT_INVALID")
    bars, rebuilt = _seal(spec, list(receipt.get("pages") or []), after_end=receipt.get("afterEnd"),
                          through_end=receipt.get("throughEnd"), limit=receipt.get("limit"))
    if rebuilt != receipt:
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_REPLAY_DIFFERS")
    return bars


def get_segment(spec, *, after_end, through_end, fetcher: Callable[..., Any], clock: Callable[[], float], limit=256,
                page_pause: Callable[[], None] | None = None):
    _, symbol, timeframe = _identity(spec)
    endpoint, seconds = ENDPOINTS[timeframe]
    if type(limit) is not int or not 1 <= limit <= 20000 or not callable(fetcher) or not callable(clock):
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_READER_REQUIRED")
    through = parse_timestamp(through_end)
    after = parse_timestamp(after_end) if after_end is not None else None
    if (through.timestamp() % seconds or through.timestamp() >= float(clock())
            or (after is not None and (after.timestamp() % seconds or not after < through))):
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_BOUNDARY_INVALID")
    needed = limit if after is None else int((through-after).total_seconds() // seconds)
    if not 1 <= needed <= limit:
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_RANGE_EXCEEDS_LIMIT")
    pages, cursor = [], through
    for _ in range(128):
        if pages and callable(page_pause):
            page_pause()
        params = {"market": symbol, "count": min(200, max(2, needed)), "to": canonical_timestamp(cursor.isoformat())}
        requested = iso(float(clock()))
        payload = fetcher(endpoint, dict(params))
        observed = iso(float(clock()))
        raw = _public_bytes(payload)
        pages.append({"endpoint": endpoint, "params": params, "requestedAt": requested, "observedAt": observed,
                      "publicResponse": json.loads(raw), "publicResponseHash": hashlib.sha256(raw).hexdigest()})
        parsed = _parse(spec, pages, through_end)
        relevant = [bar for bar in parsed if after is None or parse_timestamp(bar.end_time) > after]
        if len(relevant) >= needed:
            return _seal(spec, pages, after_end=after_end, through_end=through_end, limit=needed)
        oldest = min(parse_timestamp(bar.start_time) for bar in parsed)
        if oldest >= cursor:
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_CURSOR_DID_NOT_MOVE")
        cursor = oldest
    raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_HISTORY_PAGINATION_EXHAUSTED")


def record_seed_anchor(child, spec, bars):
    """Bind the actual returned native seed, including composite feed children."""
    if hasattr(child, "feeds"):
        for member in child.feeds:
            record_seed_anchor(member, spec, bars)
        return
    key = (spec.instrument_id, spec.timeframe)
    if bars and key in {(s.instrument_id, s.timeframe) for s in getattr(child, "subscriptions", ())}:
        if any(bar.provider != "upbit" or bar.source_provider not in {"", "upbit"}
               or (bar.instrument_id, bar.symbol, bar.timeframe) != (spec.instrument_id, spec.symbol, spec.timeframe) for bar in bars):
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_SEED_IDENTITY_DIFFERS")
        anchors = getattr(child, "_startup_seed_anchor", {})
        anchors[key] = bars[-1].end_time
        child._startup_seed_anchor = anchors


def configure_feed_bridge(child, specs, callback):
    specs = tuple(specs)
    if getattr(child, "provider_id", None) != "upbit" or not specs or not callable(callback):
        raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_BRIDGE_SCOPE_INVALID")
    for spec in specs:
        _identity(spec)
    child.configure_warmup_bridge(specs, callback)
    exact = {(s.instrument_id, s.timeframe): s for s in specs}

    def authoritative(this, subscription, *, after_end=None, through_end=None, limit=256):
        key = (subscription.instrument_id, subscription.timeframe)
        spec = exact.get(key)
        if spec is None or subscription.symbol != spec.symbol or after_end is None or through_end is None:
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_BRIDGE_SUBSCRIPTION_DIFFERS")
        seconds = ENDPOINTS[spec.timeframe][1]
        needed = int((parse_timestamp(through_end)-parse_timestamp(after_end)).total_seconds() // seconds)
        bars, receipt = get_segment(spec, after_end=after_end, through_end=through_end,
            fetcher=lambda endpoint, params: this.rest.fetch_json(endpoint+"?"+urllib.parse.urlencode(params),
                {"Accept": "application/json", "User-Agent": "trading-system/1.0"}), clock=this.rest.now,
            limit=max(1, needed), page_pause=this.rest._pagination_pause)
        replayed = verify_segment(spec, receipt)
        if [bar.to_dict() for bar in replayed] != [bar.to_dict() for bar in bars]:
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_BRIDGE_REPLAY_DIFFERS")
        this._startup_seed_anchor[key] = bars[-1].end_time
        return bars, receipt

    def bridge(this, subscription, partial, next_start):
        key = (subscription.instrument_id, subscription.timeframe)
        if key in this._startup_bridge_done:
            return
        scoped = this._startup_bridge_specs.get(key, ())
        spec = exact.get(key)
        if (not scoped or spec is None or subscription.symbol != spec.symbol or partial.provider != "upbit"
                or partial.source_provider not in {"", "upbit"}
                or (partial.instrument_id, partial.symbol, partial.timeframe) != (spec.instrument_id, spec.symbol, spec.timeframe)
                or parse_timestamp(partial.end_time) != parse_timestamp(next_start)):
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_STARTUP_PARTIAL_SCOPE_DIFFERS")
        anchor = getattr(this, "_startup_seed_anchor", {}).get(key)
        if not anchor:
            raise NativeUpbitHistoryBlocked("UPBIT_NATIVE_STARTUP_SEED_ANCHOR_REQUIRED")
        if parse_timestamp(anchor) >= parse_timestamp(partial.end_time):
            this._startup_bridge_done.add(key)
            return
        try:
            this._await_startup_partial_close(partial.end_time)
            bars, receipt = this.authoritative_warmup(subscription, after_end=anchor, through_end=partial.end_time)
            callback(scoped, tuple(bars), receipt)
        except Exception:
            this._startup_seed_anchor[key] = anchor
            raise
        this._startup_bridge_done.add(key)

    child.authoritative_warmup = MethodType(authoritative, child)
    child._bridge_startup_partial = MethodType(bridge, child)
