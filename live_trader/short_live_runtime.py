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
    """Publish a global MONITOR reduction without waiting under state locks."""
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
        if len(self.children) != 1:
            raise ValueError("EXACT_ROUTE_FEED_REQUIRED")

    def connect(self):
        for child in self.children:
            child.connect()

    def warmup(self, spec, limit):
        if self.route == "KIS_KR_CASH":
            from .short_live_kis_history import get_warmup
            bars = get_warmup(spec, limit, self.settings)
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
        return bars

    def configure_warmup_bridge(self, specs, callback):
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
                configure_feed_bridge(child, matching, self.settings, callback)
            else:
                from .short_live_upbit_history import configure_feed_bridge
                configure_feed_bridge(child, matching, callback)
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
        return events

    def close(self):
        errors = []
        for child in self.children:
            try:
                child.disconnect()
            except Exception as exc:
                errors.append(type(exc).__name__)
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
    if not state.real_orders_enabled():
        reasons.append("REAL_ORDERS_DISABLED")
    if state.STATE.get("dry_run") is not False:
        reasons.append("DRY_RUN_ACTIVE")
    # This lane receives its own exact manual consent in service.start().
    # Explicit withdrawal via set_flag still latches a global stop.
    # This separate, explicitly confirmed bounded lane does not require an
    # ordinary promoted Deployment mode. Global MONITOR reductions still stop
    # it through the dedicated latch, while every money/Kill control remains.
    if _GLOBAL_STOP.is_set():
        reasons.append("GLOBAL_MONITOR_STOP_REQUESTED")
    if state.STATE.get("kill_switch") or emergency_stop_active() or state.durable_control_snapshot().get("halted"):
        reasons.append("EMERGENCY_OR_RISK_HALT_ACTIVE")
    if state.STATE.get("manual_new_entries_blocked"):
        reasons.append("MANUAL_NEW_ENTRIES_BLOCKED")
    if state.STATE.get("new_entries_blocked"):
        reasons.append("NEW_ENTRIES_BLOCKED")
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
            from .read_only_account import ReadOnlyBrokerReader
            settings = _settings()
            reader = ReadOnlyBrokerReader(settings)
            budget = SharedExchangeBudget()

            def authority(order):
                service = _SERVICE
                reducing = order.get("riskReducing") is True
                controls = _controls()
                if reducing:
                    controls = [r for r in controls if r not in {
                        "MANUAL_NEW_ENTRIES_BLOCKED", "EXPLICIT_LIVE_MODE_REQUIRED",
                        "OPERATOR_CONFIRMATION_REQUIRED", "DRY_RUN_ACTIVE", "NEW_ENTRIES_BLOCKED", "GLOBAL_MONITOR_STOP_REQUESTED"}]
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
                    fx_reader=production_fx_reader(current_reader), cost_reader=production_cost_reader(current),
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
            )
        return _SERVICE


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
