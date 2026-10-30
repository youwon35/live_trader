"""Genuine desktop composition for the separate short functional Live lane.

Importing this module performs no network read, account mutation or startup.
Only an authenticated manual request can construct and start the service.
"""
from __future__ import annotations

from datetime import datetime, timezone
from copy import deepcopy
import hashlib
import json
import os
import threading
import time
from typing import Any, Mapping

from trading_runtime.realtime_feeds import FeedSubscription, feeds_for_specs
from trading_runtime.observed_candle_archive import CandleArchiveWriter
from .env_loader import default_runtime_data_root
from .process_safety import live_trader_instance_lease_status
from .short_live_catalog import ShortLiveCatalogues
from .short_live_session import ShortLiveTestService as _ShortLiveTestService, _spec

_LOCK = threading.RLock()
_SERVICE: ShortLiveTestService | None = None
_NATIVE_WINDOW_VISIBLE = False
_NATIVE_WINDOW_CLOSING = False
_GLOBAL_STOP = threading.Event()
_PUBLIC_STATUS: dict[str, Any] | None = None
KIS_COST_KEYS = ("SHORT_LIVE_KIS_COST_CONFIRMED", "SHORT_LIVE_KIS_COST_SOURCE",
                 "SHORT_LIVE_KIS_COST_OBSERVED_AT", "SHORT_LIVE_KIS_FEE_RATE", "SHORT_LIVE_KIS_SELL_TAX_RATE", "SHORT_LIVE_KIS_COST_SYMBOLS")


class ShortLiveTestService(_ShortLiveTestService):
    def _public_runtime(self):
        # Called while the service already owns its lock. Publish a detached
        # small view; the ordinary dashboard must never wait for this lock,
        # construct a service, or perform broker reads just to show its header.
        runtime = super()._public_runtime()
        if runtime is not None:
            runtime["marketDataArchive"] = {route: feed.archive_status() for route, feed in getattr(self, "_feeds", {}).items()
                                           if callable(getattr(feed, "archive_status", None))}
            runtime["historyObservations"] = {route: feed.history_status() for route, feed in getattr(self, "_feeds", {}).items()
                                               if isinstance(feed, NativeFeed)}
        global _PUBLIC_STATUS
        if runtime is None:
            public = {"known": True, "active": False, "status": "DISABLED", "pendingOrderCount": 0}
        else:
            keys = ("sessionId", "status", "startedAt", "endsAt", "newEntriesBlocked", "terminalVerified",
                    "naturalExitObserved", "ownerAlive", "workerAlive", "orderCount", "fillCount", "unknownOrderCount",
                    "workerRetirementRequired", "workerLaunchState")
            public = {key: deepcopy(runtime.get(key)) for key in keys}
            never_started = (public["workerRetirementRequired"] is False
                             and public["workerLaunchState"] == "NOT_ATTEMPTED"
                             and public["workerAlive"] is False)
            retired = (public["status"] == "TERMINATED" and public["terminalVerified"] is True
                       and (public["naturalExitObserved"] is True or never_started))
            public.update(known=True, active=not retired)
            public["pendingOrderCount"] = sum(row.get("status") in {"SUBMITTING", "UNKNOWN", "ACCEPTED", "PARTIAL"}
                                             for row in (self._session or {}).get("orders", {}).values())
        public.update(schemaVersion="short-live-public-status-v1", purpose="FUNCTIONAL_TEST_ONLY",
                      observedAt=self._now().isoformat(), promotionEligible=False)
        # Atomic reference replacement; published objects are never mutated.
        _PUBLIC_STATUS = public
        return runtime


def peek_status():
    """Read the latest published view without service/state/network locks."""
    public = _PUBLIC_STATUS
    if public is None:
        return None
    return {**deepcopy(public), "globalStopRequested": _GLOBAL_STOP.is_set()}


def _settings():
    from .read_only_account import runtime_settings
    return {**runtime_settings(), **{key: os.environ.get(key, "") for key in KIS_COST_KEYS}}


def _settings_hash(settings):
    return hashlib.sha256(json.dumps(dict(settings), sort_keys=True).encode()).hexdigest()


def desktop_window_shown() -> None:
    """Called only by the real native webview event, never by an HTTP payload."""
    global _NATIVE_WINDOW_VISIBLE
    with _LOCK:
        if not _NATIVE_WINDOW_CLOSING:
            _NATIVE_WINDOW_VISIBLE = True


def desktop_window_closing() -> None:
    global _NATIVE_WINDOW_CLOSING, _NATIVE_WINDOW_VISIBLE
    with _LOCK:
        _NATIVE_WINDOW_CLOSING = True
        _NATIVE_WINDOW_VISIBLE = False
        service = _SERVICE
    if service is not None:
        service.stop({})


def genuine_owner_alive() -> bool:
    with _LOCK:
        visible = _NATIVE_WINDOW_VISIBLE and not _NATIVE_WINDOW_CLOSING
    return bool(visible and live_trader_instance_lease_status().get("acquired") is True)


def request_global_stop() -> None:
    """Publish an explicit operator stop without waiting under state locks."""
    _GLOBAL_STOP.set()


class NativeFeed:
    def __init__(self, manifest: Mapping[str, Any], settings: Mapping[str, Any]):
        self.settings = dict(settings)
        self.route = manifest["route"]
        specs = tuple(_spec(row) for row in manifest["specs"])
        self.specs = specs
        owner = hashlib.sha256((str(manifest["sessionId"]) + str(manifest["manifestHash"])).encode()).hexdigest()
        key_id = hashlib.sha256(str(settings.get("KIS_APP_KEY", "")).encode()).hexdigest()
        self.children = feeds_for_specs(
            specs, prefer_kis=True, kis_demo=False,
            kis_app_key=str(settings.get("KIS_APP_KEY", "")),
            kis_app_secret=str(settings.get("KIS_APP_SECRET", "")),
            kis_account_id=str(settings.get("KIS_ACCOUNT_NO", "")),
            kis_websocket_app_key_id=key_id,
            kis_websocket_owner_id="short-live:" + owner,
        )
        from trading_runtime.market_data_hub import set_history_consumer
        for child in self.children:
            set_history_consumer(child, "live_trader")
        if len(self.children) != 1:
            raise ValueError("EXACT_ROUTE_FEED_REQUIRED")
        self._archive = CandleArchiveWriter() if self.route in {"BINANCE_SPOT", "BINANCE_USDM_FUTURES", "UPBIT_KRW_SPOT"} else None

    def connect(self):
        for child in self.children:
            child.connect()

    def history_status(self):
        # Concrete feed implementations project cached observations only.
        return [child.observation_snapshot() for child in self.children
                if callable(getattr(child, "observation_snapshot", None))]

    def set_history_window(self, through):
        from trading_runtime.market_data_hub import set_history_window
        self.history_through = through
        for child in self.children:
            set_history_window(child, through)

    def warmup(self, spec, limit):
        if self.route == "KIS_KR_CASH":
            from .short_live_kis_history import get_warmup
            bars = get_warmup(spec, limit, self.settings, through=getattr(self, "history_through", None))
            self.children[0].mark_emitted(bars)
            if bars:
                child = self.children[0]
                anchors = getattr(child, "_startup_seed_anchor", {})
                anchors[(spec.instrument_id, spec.timeframe)] = bars[-1].end_time
                child._startup_seed_anchor = anchors
            return bars
        bars = self.children[0].warmup(FeedSubscription(spec.instrument_id, spec.symbol, spec.timeframe), limit)
        if self.route == "UPBIT_KRW_SPOT":
            from .short_live_upbit_history import record_seed_anchor
            record_seed_anchor(self.children[0], spec, bars)
        if getattr(self, "_archive", None) is not None:
            self._archive.record(bars, "HISTORICAL_WARMUP")
        return bars

    def configure_warmup_bridge(self, specs, callback):
        def retain_history(members, bars, receipt):
            callback(members, bars, receipt)
            if getattr(self, "_archive", None) is not None:
                self._archive.record(bars, "HISTORICAL_WARMUP")
        if self.route not in {"KIS_KR_CASH", "UPBIT_KRW_SPOT"}:
            return
        def configure(child):
            if hasattr(child, "feeds"):
                for member in child.feeds:
                    configure(member)
                return
            method = getattr(child, "configure_warmup_bridge", None)
            if not callable(method):
                return
            keys = {(s.instrument_id, s.timeframe) for s in child.subscriptions}
            matching = tuple(s for s in specs if (s.instrument_id, s.timeframe) in keys)
            if not matching:
                return
            if self.route == "KIS_KR_CASH":
                from .short_live_kis_history import configure_feed_bridge
                configure_feed_bridge(child, matching, self.settings, retain_history)
            else:
                from .short_live_upbit_history import configure_feed_bridge
                configure_feed_bridge(child, matching, retain_history)
        for child in self.children:
            configure(child)

    def poll(self, timeout_seconds=0):
        events = []
        for child in self.children:
            events.extend(child.poll(timeout_seconds))
            integrity = getattr(child, "market_clock_integrity_snapshot", None)
            if callable(integrity):
                body = integrity()
                from trading_runtime.market_clock_integrity import validate_market_clock_integrity
                if validate_market_clock_integrity(body):
                    raise ValueError("NATIVE_FEED_CLOCK_INTEGRITY_FAILED")
        if getattr(self, "_archive", None) is not None:
            self._archive.record(events, "FORWARD_OBSERVED")
        return events

    def archive_status(self):
        return [self._archive.snapshot()] if self._archive is not None else []

    def close(self):
        errors = []
        for child in self.children:
            try:
                child.disconnect()
            except Exception as exc:
                errors.append(type(exc).__name__)
        if getattr(self, "_archive", None) is not None:
            self._archive.close()
        if errors:
            raise RuntimeError("NATIVE_FEED_RETIREMENT_UNVERIFIED")


class FreshClock:
    """Read-only exchange time and wall/monotonic continuity; no OS changes."""
    def __init__(self, reader, controls):
        self.reader, self.controls = reader, controls
        self.wall, self.mono = time.time(), time.monotonic()
        self.last_positive = None
        self.failure = None

    def __call__(self, manifest):
        blockers = self.controls()
        wall, mono = time.time(), time.monotonic()
        if abs((wall-self.wall)-(mono-self.mono)) > .25:
            self.failure = "WALL_MONOTONIC_CONTINUITY_FAILED"
        if self.failure:
            blockers.append(self.failure)
        if not blockers and (self.last_positive is None or mono-self.last_positive > 10):
            try:
                started = time.time()
                tick = time.monotonic()
                body, _ = self.reader._get("binance", "/api/v3/time")
                received = time.time()
                elapsed = time.monotonic()-tick
                ms = body.get("serverTime") if isinstance(body, dict) else None
                if type(ms) is not int or elapsed > 2 or abs((received-started)-elapsed) > .25:
                    raise ValueError("CLOCK_RESPONSE_UNBOUNDED")
                if max(abs(ms/1000-started), abs(ms/1000-received)) > 2:
                    raise ValueError("CLOCK_OFFSET_EXCEEDED")
                self.last_positive = time.monotonic()
            except Exception:
                self.failure = "FRESH_EXCHANGE_CLOCK_UNVERIFIED"
                blockers.append(self.failure)
        return {"ready": not blockers, "blockers": list(dict.fromkeys(blockers))}


def _controls() -> list[str]:
    from . import state
    from .emergency_stop import emergency_stop_active
    reasons = []
    # A bounded short session receives exact manual consent in service.start().
    # Ordinary dry-run/operator/aggregate entry flags are not its authority:
    # fresh selected-route readiness and session reconciliation enforce the
    # actual funding, ownership, loss and protection conditions. Explicit
    # operator safety reductions still latch a stop through set_flag/MONITOR.
    if _GLOBAL_STOP.is_set():
        reasons.append("GLOBAL_MONITOR_STOP_REQUESTED")
    if state.STATE.get("kill_switch") or emergency_stop_active() or state.durable_control_snapshot().get("halted"):
        reasons.append("EMERGENCY_OR_RISK_HALT_ACTIVE")
    if any(state.STATE.get("active_runtime_session_ids", {}).values()):
        reasons.append("OTHER_RUNTIME_ACTIVE")
    for runtime in state.LIVE_CONTINUOUS_CONTROLLER.snapshot().get("runtimes", {}).values():
        if isinstance(runtime, dict) and runtime.get("running") is True:
            reasons.append("OTHER_RUNTIME_ACTIVE")
    for name in ("_upbit_order_authority_snapshot", "_binance_order_authority_snapshot"):
        view = getattr(state, name)()
        if view.get("functionalAuthorityOpen") is True:
            reasons.append("OTHER_FUNCTIONAL_AUTHORITY_ACTIVE")
    return reasons


def get_service() -> ShortLiveTestService:
    global _SERVICE
    with _LOCK:
        if _SERVICE is None:
            from .short_live_brokers import SharedExchangeBudget, create_production_short_live_broker
            from .short_live_broker_readers import production_fx_reader, production_cost_reader
            from .short_live_fx_recovery import public_reader
            from .read_only_account import ReadOnlyBrokerReader
            settings = _settings()
            reader = ReadOnlyBrokerReader(settings)
            budget = SharedExchangeBudget()

            def authority(order):
                service = _SERVICE
                reducing = order.get("riskReducing") is True
                controls = _controls()
                if reducing:
                    controls = [r for r in controls if r != "GLOBAL_MONITOR_STOP_REQUESTED"]
                return bool(service and service.dispatch_authorized(order) and not controls
                            and live_trader_instance_lease_status().get("acquired") is True)

            def broker(route):
                # Preparation may be repeated before any order. Each start
                # binds fresh credentials/configuration and a fresh adapter.
                current = _settings()
                digest = _settings_hash(current)
                current_reader = ReadOnlyBrokerReader(current)
                return create_production_short_live_broker(
                    route, settings=current,
                    authority_guard=lambda order: _settings_hash(_settings()) == digest and authority(order),
                    fx_reader=production_fx_reader(current_reader, reader_factory=public_reader,
                        can_continue=lambda: not _GLOBAL_STOP.is_set() and not _NATIVE_WINDOW_CLOSING),
                    cost_reader=production_cost_reader(current),
                    budget=budget,
                    reserve_cleanup_order=lambda order: bool(_SERVICE and _SERVICE.reserve_cleanup_order(order)),
                    reserve_cleanup_cancel=lambda order: bool(_SERVICE and _SERVICE.reserve_cleanup_cancel(order)),
                    observe_cleanup_order=lambda client_id, result: bool(_SERVICE and _SERVICE.observe_cleanup_order(client_id, result)),
                    reserve_protection=lambda order: bool(_SERVICE and _SERVICE.reserve_protection(order)),
                    record_protection=lambda event, detail: _SERVICE.record_protection(event, detail),
                )

            _SERVICE = ShortLiveTestService(
                ShortLiveCatalogues(), broker,
                lambda manifest: NativeFeed(manifest, _settings()),
                default_runtime_data_root()/"short-live-tests", clock_guard=FreshClock(reader, _controls),
                owner_is_alive=genuine_owner_alive,
                history_preparer=_prepare_history,
            )
        return _SERVICE


def _prepare_history(specs, through):
    from trading_runtime.history_preparation import prepare_history_streams
    from trading_runtime.portfolio_runtime import required_warmup_bars
    from .short_live_kis_history import get_warmup
    # This reads public data only. No feed/strategy/order owner is started.
    common = [spec for spec in specs if spec.provider != "kis" or spec.timeframe == "1d"]
    native = [spec for spec in specs if spec.provider == "kis" and spec.timeframe != "1d"]
    result = prepare_history_streams(common, consumer="live_trader", through=through, timeout_seconds=25) if common else {
        "schemaVersion": "history-preparation-v1", "streams": [], "executionEvidence": False, "published": False}
    settings = _settings() if native else {}
    for spec in native:
        required = required_warmup_bars(spec)
        bars = get_warmup(spec, required, settings, through=through)
        result["streams"].append({"provider": spec.provider, "symbol": spec.symbol, "timeframe": spec.timeframe,
            "requiredBars": required, "observedBars": len(bars),
            "status": "COMPLETE" if len(bars) >= required else "UNAVAILABLE",
            "sourceCounts": {"KIS_NATIVE_HISTORY": len(bars)}})
    result["ready"] = bool(result["streams"]) and all(row["status"] == "COMPLETE" for row in result["streams"])
    result["status"] = "COMPLETE" if result["ready"] else "UNAVAILABLE"
    return result


def request(action: str, payload=None):
    restore_prior_stop = False
    try:
        if action not in {"snapshot", "catalog", "prepare", "start", "stop"}:
            raise ValueError("UNKNOWN_SHORT_LIVE_ACTION")
        service = get_service()
        if action == "prepare":
            session = service._session
            if session is None or session.get("status") == "TERMINATED":
                restore_prior_stop = _GLOBAL_STOP.is_set()
                _GLOBAL_STOP.clear()
        if action == "snapshot":
            result = service.snapshot(include_catalog=False)
        elif action == "catalog":
            result = service.catalog()
        else:
            result = getattr(service, action)(payload or {})
        return {"ok": True, **result}
    except Exception as exc:
        if restore_prior_stop:
            _GLOBAL_STOP.set()
        from .short_live_contract import ShortLiveContractError
        public = str(exc) if isinstance(exc, (ShortLiveContractError, ValueError)) else type(exc).__name__
        return {"ok": False, "reasonCode": public[:500], "reason": public[:500],
                "promotionEligible": False, "automaticRestartAllowed": False}
