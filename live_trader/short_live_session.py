"""Operator-started short Live tests with injectable, fail-closed boundaries.

The catalog supplies verified artifacts; official broker adapters own native
account/FX/filter/order checks.  This service never promotes Paper evidence and
never restores a previous live session on startup.  All external operations are
behind explicit injected adapters and a confirmed start.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import threading
import time
from typing import Any, Callable, Mapping
from uuid import uuid4

from trading_runtime.continuous_runtime import (
    ClosedBar, OpenBoundary, PortfolioRuntimeEngine, RuntimeStrategySpec,
    REALTIME_OPEN_BOUNDARY_ATTESTATIONS,
)
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator, required_warmup_bars

from .short_live_contract import (
    CONFIRMATION_TEXT, PURPOSE, ROUTES, ShortLiveContractError, build_contract,
    canonical_hash, finite, leg_manifest, utc, utc_text, verify_contract,
)
from .short_live_order_authority import activate_short_live_routes, release_short_live_routes
from .portfolio_execution import SleeveDelta, _split_internal_and_external_allocations


def _spec(value: Mapping[str, Any]) -> RuntimeStrategySpec:
    return RuntimeStrategySpec(
        portfolio_id=value["portfolioId"], portfolio_hash=value["portfolioHash"],
        strategy_instance_id=value["strategyInstanceId"], strategy_id=value["strategyId"],
        artifact_hash=value["artifactHash"], plugin_id=value["pluginId"],
        instrument_id=value["instrumentId"], symbol=value["symbol"],
        timeframe=value["timeframe"], provider=value["provider"],
        broker_id=value["brokerId"], target_weight=value["targetWeight"],
        parameters=deepcopy(value["parameters"]), artifact=deepcopy(value.get("artifact", {})),
    )


class ShortLiveTestService:
    """One active functional scope per UI owner, with no implicit start authority.

    Catalog: snapshot() -> {candidates, coverage}; resolve(selections) ->
    {targets, specs (RuntimeStrategySpec camelCase + route), sourcePins, accounts}.
    Broker provider(route): readiness(manifest), size_order(proposal),
    submit(order), reconcile(session), cleanup(session). Missing observations
    always block. Feed provider(manifest) returns connect/poll/close and
    warmup(spec, limit) (or warmup_by_instance mapping) using original feeds.
    """

    def __init__(self, catalog_provider: Any, broker_provider: Callable[[str], Any],
                 feed_provider: Callable[[Mapping[str, Any]], Any], store_root: str | Path,
                 *, clock: Callable[[], datetime] | None = None,
                 monotonic: Callable[[], float] = time.monotonic,
                 clock_guard: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
                 owner_is_alive: Callable[[], bool] | None = None,
                 background: bool = True, poll_seconds: float = 0.5,
                 preparation_ttl_seconds: float = 180,
                 account_poll_seconds: float = 5, catalog_display_ttl_seconds: float = 10,
                 order_timeout_seconds: float = 60) -> None:
        self.catalog_provider = catalog_provider
        self.broker_provider = broker_provider
        self.feed_provider = feed_provider
        self.store_root = Path(store_root)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic
        self.clock_guard = clock_guard
        self.owner_is_alive = owner_is_alive
        self.background = bool(background)
        self.poll_seconds = max(0.05, min(float(poll_seconds), 5))
        self.preparation_ttl_seconds = min(300, max(1, float(preparation_ttl_seconds)))
        self.account_poll_seconds = max(1, min(5, float(account_poll_seconds)))
        self.catalog_display_ttl_seconds = max(1, min(30, float(catalog_display_ttl_seconds)))
        self.order_timeout_seconds = max(10, min(600, float(order_timeout_seconds)))
        self._lock = threading.RLock()
        # Display discovery must never hold the live state/worker lock.
        self._catalog_lock = threading.Lock()
        self._prepared: dict[str, Any] | None = None
        self._session: dict[str, Any] | None = None
        self._brokers: dict[str, Any] = {}
        self._feeds: dict[str, Any] = {}
        self._engines: dict[tuple[str, str], PortfolioRuntimeEngine] = {}
        self._specs: dict[str, dict[str, Any]] = {}
        self._thread: threading.Thread | None = None
        self._wake = threading.Event()
        self._journal_sequence = 0
        self._monitor_events: list[dict[str, Any]] = []
        self._catalog_cache = None
        self._catalog_cache_at = float("-inf")
        self._last_reconcile_at = float("-inf")
        self._boot_blockers = self._previous_sessions()

    def _now(self) -> datetime:
        return utc(self.clock())

    def _previous_sessions(self) -> list[str]:
        if not self.store_root.exists():
            return []
        starts = {p.stem.removeprefix("session-start-")
                  for p in self.store_root.glob("session-start-*.json")}
        terminals = {p.stem.removeprefix("session-terminal-")
                     for p in self.store_root.glob("session-terminal-*.json")}
        return (["PREVIOUS_SESSION_REQUIRES_MANUAL_RECONCILIATION"]
                if starts - terminals else [])

    def _write(self, name: str, value: Mapping[str, Any]) -> None:
        self.store_root.mkdir(parents=True, exist_ok=True)
        path = self.store_root / f"{name}.json"
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                             indent=2, allow_nan=False) + "\n"
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(encoded)
            stream.flush()
            import os
            os.fsync(stream.fileno())

    def _event(self, kind: str, **values: Any) -> None:
        self._journal_sequence += 1
        session_id = (self._session or {}).get("sessionId", "preparation")
        observed_at = utc_text(self._now())
        self._write(f"event-{session_id}-{self._journal_sequence:08d}-{uuid4().hex}",
                    {"schemaVersion": "short-live-event-v1", "kind": kind,
                     "observedAt": observed_at, "purpose": PURPOSE,
                     "promotionEligible": False, **values})
        # A bounded, public projection of already journaled events. No private
        # account, broker reply, order identifier or arbitrary nested payload.
        if kind in {"STARTED", "BAR_EVALUATED", "NEXT_OPEN_OBSERVED",
                    "ORDER_DEFERRED_PENDING_FILL", "ORDER_INTENT_BEFORE_SUBMIT",
                    "ORDER_REPLY", "ACTUAL_FILL_INCREMENT", "INTERNAL_ALLOCATION_TRANSFER",
                    "CLEANUP_INTENT_ONCE", "CLEANUP_ORDER_OBSERVED",
                    "WORKER_EXIT_INTENT", "WORKER_DEATH_OBSERVED", "STARTUP_RETIRED_BEFORE_WORKER"}:
            event = {"sequence": self._journal_sequence, "kind": kind,
                     "observedAt": observed_at, "sessionId": session_id}
            order = (self._session or {}).get("orders", {}).get(values.get("clientOrderId"), {}).get("order", {})
            for key in ("route", "symbol", "side"):
                item = values.get(key, order.get(key))
                if isinstance(item, str) and len(item) <= 80:
                    event[key] = item
            if isinstance(values.get("status"), str) and values["status"] in {"SUBMITTING", "UNKNOWN", "ACCEPTED", "PARTIAL", "FILLED", "REJECTED", "CANCELED"}:
                event["status"] = values["status"]
            for key in ("quantity", "cumulativeQuantity", "incrementalQuantity", "cumulativeAveragePrice"):
                item = values.get(key)
                if isinstance(item, (float, int)) and not isinstance(item, bool):
                    event[key] = item
            self._monitor_events.append(event)
            self._monitor_events = self._monitor_events[-60:]

    def _catalog(self) -> dict[str, Any]:
        with self._catalog_lock:
            if self._catalog_cache is not None and self.monotonic() - self._catalog_cache_at < self.catalog_display_ttl_seconds:
                return deepcopy(self._catalog_cache)
            provider = self.catalog_provider
            value = provider.snapshot() if hasattr(provider, "snapshot") else provider()
            if isinstance(value, list):
                value = {"candidates": value, "coverage": []}
            self._catalog_cache = deepcopy(dict(value))
            self._catalog_cache_at = self.monotonic()
            return deepcopy(self._catalog_cache)

    def catalog(self) -> dict[str, Any]:
        value = self._catalog()
        return {"schemaVersion": "short-live-test-catalog-v1",
                "catalog": value.get("candidates", []), "coverage": value.get("coverage", []),
                "readOnly": True, "authorityGranted": False, "promotionEligible": False}

    def _guard(self, manifest: Mapping[str, Any]) -> list[str]:
        if not callable(self.clock_guard):
            return ["FRESH_NATIVE_CLOCK_GUARD_NOT_CONFIGURED"]
        value = self.clock_guard(deepcopy(dict(manifest)))
        if not isinstance(value, Mapping) or value.get("ready") is not True:
            return list(value.get("blockers") or ["FRESH_CLOCK_GUARD_NOT_POSITIVE"]) if isinstance(value, Mapping) else ["FRESH_CLOCK_GUARD_UNKNOWN"]
        return []

    def _owner(self) -> bool:
        return callable(self.owner_is_alive) and self.owner_is_alive() is True

    def snapshot(self, *, include_catalog: bool = True) -> dict[str, Any]:
        # Compatibility callers may ask for discovery, but never under the
        # state lock. HTTP polling and mutation responses omit this work.
        try:
            catalog = self._catalog() if include_catalog else {"candidates": [], "coverage": []}
            catalog_error = []
        except Exception as exc:
            catalog = {"candidates": [], "coverage": []}
            catalog_error = [f"CATALOG_UNAVAILABLE:{type(exc).__name__}"]
        with self._lock:
            # The worker's own finally block cannot prove its death. A later
            # UI/status caller holds the same Thread object and observes it.
            if (self.background and self._session
                    and self._session["status"] == "DRAINED"
                    and self._session.get("workerExitIntent") is True
                    and self._thread is not None and not self._thread.is_alive()):
                self._session["naturalExitObserved"] = True
                self._event("WORKER_DEATH_OBSERVED", sessionId=self._session["sessionId"],
                            threadIdent=self._thread.ident)
                self._finalize_after_worker_exit()
            prepared = deepcopy(self._prepared)
            if prepared:
                manifest = prepared["contract"]
                prepared = {"preparationId": manifest["preparationId"],
                            "scopeHash": manifest["scopeHash"],
                            "targets": deepcopy(manifest["targets"]),
                            "durationHours": manifest["durationHours"],
                            "capitalKrwPerBroker": manifest["capitalKrwPerBroker"],
                            "dailyLossStopKrwPerBroker": manifest["dailyLossStopKrwPerBroker"],
                            "futuresPolicy": deepcopy(manifest.get("futuresPolicy")),
                            "preparedAt": manifest["preparedAt"],
                            "expiresAt": prepared["expiresAt"],
                            "readyToStart": prepared["readyToStart"],
                            "brokerReadiness": deepcopy(prepared.get("publicReadiness", [])),
                            "blockers": prepared["blockers"]}
                if self._now() >= datetime.fromisoformat(prepared["expiresAt"].replace("Z", "+00:00")):
                    prepared["readyToStart"] = False
                    prepared["blockers"] = list(dict.fromkeys([*prepared["blockers"], "PREPARATION_EXPIRED"]))
            runtime = self._public_runtime()
            mode = runtime["status"] if runtime else "PREPARED" if prepared else "DISABLED"
            return {"schemaVersion": "short-live-test-snapshot-v1", "purpose": PURPOSE,
                    "promotionEligible": False, "mode": mode, "status": mode,
                    "catalogIncluded": include_catalog,
                    "catalog": catalog.get("candidates", []),
                    "candidates": catalog.get("candidates", []),
                    "coverage": catalog.get("coverage", []),
                    "prepared": prepared, "runtime": runtime,
                    "observedAt": utc_text(self._now()),
                    "blockers": [*self._boot_blockers, *catalog_error],
                    "confirmationText": CONFIRMATION_TEXT,
                    "automaticRestartAllowed": False}

    def _public_runtime(self) -> dict[str, Any] | None:
        if not self._session:
            return None
        session = self._session
        keys = ("sessionId", "status", "startedAt", "endsAt", "newEntriesBlocked",
                "terminalVerified", "naturalExitObserved", "stopReason", "firstFailure",
                "blockers", "unknownOrderCount", "orderCount", "fillCount", "scopeHash", "internalTransferCount",
                "workerRetirementRequired", "workerLaunchState")
        result = {key: deepcopy(session.get(key)) for key in keys}
        from .short_live_monitor import public_feed_observation
        result["feedObservations"] = [dict(route=route, **public_feed_observation(feed))
                                      for route, feed in self._feeds.items()]
        result["ownerAlive"] = self._owner()
        result["workerAlive"] = bool(self._thread and self._thread.is_alive())
        result["recentActivity"] = [deepcopy(row) for row in self._monitor_events
                                    if row["sessionId"] == session["sessionId"]]
        public_keys = ("route", "ready", "exposureKrw", "realizedLossKrw",
                       "unrealizedLossKrw", "lossBasis", "actualFeeKnown",
                       "feeReconciliationStatus", "committedCapitalKrw", "notionalExposureKrw",
                       "monitorEquity", "accountObservedAt",
                       "protections", "futuresPositions", "futuresIncidents", "observedFuturesSettingsBySymbol")
        result["brokerRows"] = [{key: deepcopy(row.get(key)) for key in public_keys}
                                for row in session["brokerRows"].values()]
        for row in result["brokerRows"]:
            row["effectiveRouteBudgetKrw"] = session["effectiveRouteBudgets"][row["route"]]
            row["brokerBudgetKrw"] = session["contract"]["capitalKrwPerBroker"]
            row["dailyLossStopKrw"] = session["contract"]["dailyLossStopKrwPerBroker"]
        result["strategyRows"] = [
            {"instanceId": key, "pluginId": self._specs[key]["pluginId"],
             "route": self._specs[key]["route"], "symbol": self._specs[key]["symbol"],
             "timeframe": self._specs[key]["timeframe"],
             "allocationWeight": self._specs[key]["targetWeight"],
             "sourcePositionSizePercent": self._specs[key]["sourcePositionSizePercent"],
             "positionSizeFraction": self._specs[key]["positionSizeFraction"],
             "positionQuantity": row["quantity"], "targetWeight": row["targetWeight"],
             "lastSignal": row.get("lastSignal", "NOT_OBSERVED"),
             "decisionCount": row.get("decisionCount", 0),
             "orderAttemptCount": row.get("orderAttemptCount", 0),
             "fillObservationCount": row.get("fillCount", 0),
             "internalAllocationTransferCount": row.get("internalTransferCount", 0)}
            for key, row in session["sleeves"].items()]
        return result

    def prepare(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._session and self._session["status"] != "TERMINATED":
                raise ShortLiveContractError("ACTIVE_SCOPE_CANNOT_BE_REPLACED")
            selections = payload.get("selections")
            if not isinstance(selections, list) or not selections:
                raise ShortLiveContractError("EXACT_SELECTIONS_REQUIRED")
            resolved = self.catalog_provider.resolve(deepcopy(selections))
            contract = build_contract(resolved, payload, now=self._now())
            blockers = list(self._boot_blockers)
            if not self._owner():
                blockers.append("GENUINE_UI_OWNER_UNAVAILABLE")
            blockers += self._guard(contract)
            readiness: dict[str, Any] = {}
            public_readiness = []
            for leg in contract["legs"]:
                manifest = leg_manifest(contract, leg["route"])
                broker = self.broker_provider(leg["route"])
                value = broker.readiness(manifest)
                before_validation = len(blockers)
                readiness[leg["route"]] = self._validate_readiness(value, manifest, blockers)
                public = self._public_preparation_readiness(value, leg["route"])
                public["blockers"] = list(dict.fromkeys([*public["blockers"],
                    *[reason.removeprefix(leg["route"] + ":") for reason in blockers[before_validation:]]]))
                public["ready"] = public["ready"] and not public["blockers"]
                public_readiness.append(public)
            self._prepared = {"contract": contract,
                              "readyToStart": not blockers,
                              "blockers": list(dict.fromkeys(blockers)),
                              "readiness": readiness,
                              "publicReadiness": public_readiness,
                              "expiresAt": utc_text(self._now() + timedelta(seconds=self.preparation_ttl_seconds))}
            self._write(f"preparation-{contract['preparationId']}", self._prepared)
            return self.snapshot(include_catalog=False)

    @staticmethod
    def _public_preparation_readiness(value: Any, route: str) -> dict[str, Any]:
        value = value if isinstance(value, Mapping) else {}
        keys = ("fundingBasis", "quoteCurrency", "availableQuoteNative", "availableKrw",
                "effectiveRouteBudgetKrw", "preservedAssetSymbols")
        result = {key: deepcopy(value[key]) for key in keys if key in value}
        result.update(route=route, ready=value.get("ready") is True,
                      blockers=deepcopy(value.get("blockers") or []))
        for key in ("observedFuturesSettings", "expectedFuturesSettings"):
            if isinstance(value.get(key), Mapping):
                result[key] = {field: deepcopy(value[key].get(field))
                               for field in ("marginMode", "leverage", "positionMode")}
        if isinstance(value.get("observedFuturesSettingsBySymbol"), Mapping):
            result["observedFuturesSettingsBySymbol"] = {
                str(symbol): {field: deepcopy(row.get(field)) for field in ("marginMode", "leverage", "positionMode")}
                for symbol, row in value["observedFuturesSettingsBySymbol"].items() if isinstance(row, Mapping)}
        return result

    @staticmethod
    def _validate_readiness(value: Any, manifest: Mapping[str, Any], blockers: list[str]) -> dict[str, Any]:
        route = manifest["route"]
        if not isinstance(value, Mapping) or value.get("ready") is not True:
            reasons = value.get("blockers") if isinstance(value, Mapping) else None
            blockers.extend(f"{route}:{reason}" for reason in (reasons or ["BROKER_READINESS_UNKNOWN"]))
            return {}
        if value.get("scopeOwnershipVerified") is not True or not value.get("accountId"):
            blockers.append(f"{route}:ACCOUNT_SCOPE_OWNERSHIP_UNVERIFIED")
        if manifest.get("accountId") and value.get("accountId") != manifest["accountId"]:
            blockers.append(f"{route}:EXACT_ACCOUNT_BINDING_MISMATCH")
        try:
            available = finite(value.get("availableKrw"), "AVAILABLE_KRW")
            existing = finite(value.get("existingExposureKrw"), "EXISTING_EXPOSURE_KRW")
            if available <= 0 or existing > 0:
                blockers.append(f"{route}:FUNDING_OR_EXISTING_EXPOSURE_BLOCKED")
        except ShortLiveContractError:
            blockers.append(f"{route}:FUNDING_OBSERVATION_UNKNOWN")
        return deepcopy(dict(value))

    def start(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if payload.get("confirmed") is not True or payload.get("confirmationText") != CONFIRMATION_TEXT:
                raise ShortLiveContractError("EXPLICIT_MANUAL_UI_CONFIRMATION_REQUIRED")
            if not self._prepared:
                raise ShortLiveContractError("PREPARATION_REQUIRED")
            if self._session and self._session["status"] != "TERMINATED":
                raise ShortLiveContractError("DUPLICATE_START_FORBIDDEN")
            contract = self._prepared["contract"]
            if (payload.get("preparationId") != contract["preparationId"]
                    or payload.get("scopeHash") != contract["scopeHash"]):
                raise ShortLiveContractError("PREPARATION_SCOPE_MISMATCH")
            verify_contract(contract)
            if self._now() >= datetime.fromisoformat(self._prepared["expiresAt"].replace("Z", "+00:00")):
                raise ShortLiveContractError("PREPARATION_EXPIRED")
            blockers = list(self._prepared["blockers"])
            if not self._owner():
                blockers.append("GENUINE_UI_OWNER_UNAVAILABLE")
            blockers += self._guard(contract)
            self._brokers = {}
            readiness = {}
            for leg in contract["legs"]:
                route = leg["route"]
                adapter = self.broker_provider(route)
                readiness[route] = self._validate_readiness(adapter.readiness(leg_manifest(contract, route)), leg, blockers)
                prior = self._prepared["readiness"].get(route, {})
                current = readiness[route]
                if route == "BINANCE_USDM_FUTURES" and (
                        not prior.get("observedFuturesSettingsBySymbol") or
                        prior.get("observedFuturesSettingsBySymbol") != current.get("observedFuturesSettingsBySymbol")):
                    blockers.append("FUTURES_SETTINGS_CHANGED_SINCE_PREPARATION")
                if route in {"BINANCE_SPOT", "UPBIT_KRW_SPOT"} and (
                        prior.get("fundingBasis") == "AVAILABLE_QUOTE_ONLY"
                        or current.get("fundingBasis") == "AVAILABLE_QUOTE_ONLY"
                        or prior.get("preservedAssetsBaselineHash")
                        or current.get("preservedAssetsBaselineHash")):
                    before = prior.get("preservedAssetsBaselineHash")
                    after = current.get("preservedAssetsBaselineHash")
                    if (not isinstance(before, str) or len(before) != 64
                            or any(char not in "0123456789abcdef" for char in before)
                            or after != before):
                        blockers.append(f"{route}:PRESERVED_ASSET_BASELINE_CHANGED")
                if (self._prepared["readiness"].get(route, {}).get("accountId")
                        != readiness[route].get("accountId")):
                    blockers.append(f"{route}:PREPARED_ACCOUNT_CHANGED")
                self._brokers[route] = adapter
            binance_accounts = {readiness[leg["route"]].get("accountId") for leg in contract["legs"]
                                if leg["brokerGroup"] == "BINANCE" and readiness[leg["route"]]}
            # The unified budget belongs to one configured Binance owner; an
            # adapter must not silently substitute a different account.
            if len(binance_accounts) > 1:
                blockers.append("BINANCE_SHARED_BUDGET_ACCOUNT_MISMATCH")
            if blockers:
                self._prepared["blockers"] = list(dict.fromkeys(blockers))
                self._prepared["readyToStart"] = False
                raise ShortLiveContractError("START_BLOCKED:" + ",".join(self._prepared["blockers"]))
            # A previous verified terminal session may have used other routes.
            # Its retired feed/Thread objects never belong to this new claim.
            self._feeds = {}
            self._thread = None
            self._last_reconcile_at = float("-inf")
            started = self._now()
            self._session = {
                "sessionId": contract["sessionId"], "scopeHash": contract["scopeHash"],
                "contract": deepcopy(contract), "status": "STARTING",
                "startedAt": utc_text(started),
                "endsAt": utc_text(started + timedelta(seconds=contract["durationSeconds"])),
                "startedMonotonic": self.monotonic(), "readiness": readiness,
                "newEntriesBlocked": True, "terminalVerified": False,
                "naturalExitObserved": False, "firstFailure": None,
                "stopReason": "", "blockers": [], "unknownOrderCount": 0,
                "orderCount": 0, "fillCount": 0, "orders": {},
                "internalTransfers": {}, "internalTransferCount": 0, "startupHistoryReceipts": {},
                "sleeves": {s["strategyInstanceId"]: {"quantity": 0.0, "targetWeight": 0.0,
                             "averagePrice": 0.0} for s in contract["specs"]},
                "brokerRows": {}, "cleanupAttempted": False,
                "feedsClosed": False, "dailyLossByBroker": {},
                "pendingNextOpen": {}, "observedOpenKeys": set(),
                "feesKnown": True,
                "workerExitIntent": False,
                "workerRetirementRequired": self.background,
                "workerLaunchState": "NOT_ATTEMPTED" if self.background else "SYNCHRONOUS",
                "startupAbortedBeforeWorker": False,
                "cancelReservations": {},
                "protectionReservations": {}, "protectionObservations": {},
                "externalExecutions": {},
                "effectiveRouteBudgets": {leg["route"]: min(leg["routeBudgetKrw"],
                    finite(readiness[leg["route"]]["availableKrw"], "ACTUAL_START_AVAILABLE", positive=True))
                    for leg in contract["legs"]},
                "routeAuthorityActive": False,
            }
            self._specs = {s["strategyInstanceId"]: s for s in contract["specs"]}
            # Durable claim precedes connect/submit.  Lost replies cannot mint a
            # fresh UUID or restart this same scope.
            self._write(f"session-start-{contract['sessionId']}", {
                "contract": contract, "startedAt": utc_text(started),
                "effectiveRouteBudgetsKrw": self._session["effectiveRouteBudgets"],
                "purpose": PURPOSE, "promotionEligible": False})
            try:
                activate_short_live_routes([leg_manifest(contract, leg["route"]) for leg in contract["legs"]])
                self._session["routeAuthorityActive"] = True
                self._make_engines(contract)
                for leg in contract["legs"]:
                    route = leg["route"]
                    feed = self.feed_provider(leg_manifest(contract, route))
                    self._feeds[route] = feed
                    feed.connect()
                    self._seed(route, feed)
                self._session["status"] = "RUNNING"
                self._session["newEntriesBlocked"] = False
                self._event("STARTED", sessionId=contract["sessionId"], scopeHash=contract["scopeHash"])
                if self.background:
                    self._wake.clear()
                    self._thread = threading.Thread(target=self._worker,
                        name=f"ShortLiveTest-{contract['sessionId']}", daemon=True)
                    self._session["workerLaunchState"] = "START_ATTEMPTED"
                    self._thread.start()
                    self._session["workerLaunchState"] = "STARTED"
            except Exception as exc:
                if self.background and self._session["workerLaunchState"] == "NOT_ATTEMPTED" and self._thread is None:
                    # This branch precedes any Thread creation/start. Record
                    # that exact fact instead of inventing a natural exit.
                    self._session["startupAbortedBeforeWorker"] = True
                    self._session["workerRetirementRequired"] = False
                elif self.background:
                    self._session["workerLaunchState"] = "START_FAILED_UNVERIFIED"
                self._failure(f"STARTUP_FAILED:{type(exc).__name__}:{exc}")
                self._stop_once("STARTUP_FAILED")
                raise
            return self.snapshot(include_catalog=False)

    def _make_engines(self, contract: Mapping[str, Any]) -> None:
        self._engines = {}
        grouped: dict[tuple[str, str], list[RuntimeStrategySpec]] = {}
        for row in contract["specs"]:
            grouped.setdefault((row["route"], row["portfolioId"]), []).append(_spec(row))
        for key, specs in grouped.items():
            evaluator = BuiltinBarSignalEvaluator(lambda s: self._session["sleeves"][s.strategy_instance_id]["quantity"])
            self._engines[key] = PortfolioRuntimeEngine(
                specs, mode="SMALL_LIVE", evaluator=evaluator,
                cycle_handler=lambda cycle: {"observed": True, "cycleId": cycle.cycle_id},
                nonpromotion_gap_continuity=True)

    def _seed(self, route: str, feed: Any) -> None:
        route_specs = []
        for (engine_route, _), engine in self._engines.items():
            if engine_route != route:
                continue
            for spec in engine.specs:
                limit = required_warmup_bars(spec)
                if hasattr(feed, "warmup_by_instance"):
                    bars = tuple(feed.warmup_by_instance(spec, limit))
                else:
                    bars = tuple(feed.warmup(spec, limit))
                if len(bars) < limit:
                    raise ShortLiveContractError("ORIGINAL_ALGORITHM_WARMUP_INSUFFICIENT")
                if any(not isinstance(bar, ClosedBar) for bar in bars):
                    raise ShortLiveContractError("ORIGINAL_CLOSED_BAR_WARMUP_REQUIRED")
                if any(datetime.fromisoformat(bar.end_time.replace("Z", "+00:00")) > self._now() for bar in bars):
                    raise ShortLiveContractError("WARMUP_CONTAINS_UNCLOSED_BAR")
                engine.seed_history(spec.strategy_instance_id, bars)
                route_specs.append(spec)
        configure = getattr(feed, "configure_warmup_bridge", None)
        if callable(configure):
            configure(tuple(route_specs), self._append_startup_history)

    def _append_startup_history(self, specs, bars, receipt) -> None:
        """A public startup partial is appended as history without dispatch."""
        with self._lock:
            from trading_runtime.artifact_governance import stable_sha256
            from trading_runtime.market_calendar import next_tradable_boundary_start
            from trading_runtime.market_data import parse_timestamp
            if (not isinstance(receipt, Mapping) or receipt.get("historicalOnly") is not True
                    or receipt.get("naturalForwardCycles") != 0 or receipt.get("brokerOrders") != 0
                    or receipt.get("contentHash") != stable_sha256({k: v for k, v in receipt.items() if k != "contentHash"})):
                raise ShortLiveContractError("STARTUP_HISTORY_RECEIPT_INVALID")
            bars = tuple(bars)
            receipt_hash = str(receipt["contentHash"])
            if receipt_hash not in self._session["startupHistoryReceipts"]:
                self._write(f"startup-history-{self._session['sessionId']}-{receipt_hash}", {
                    "schemaVersion": "short-live-startup-history-retained-v1", "purpose": PURPOSE,
                    "promotionEligible": False, "sessionId": self._session["sessionId"],
                    "receipt": deepcopy(dict(receipt)), "bars": [bar.to_dict() for bar in bars],
                    "externalOrders": 0, "naturalForwardCycles": 0})
                self._session["startupHistoryReceipts"][receipt_hash] = True
            for spec in specs:
                matches = [engine for engine in self._engines.values()
                           if any(row.strategy_instance_id == spec.strategy_instance_id for row in engine.specs)]
                if len(matches) != 1 or self._session["sleeves"][spec.strategy_instance_id].get("decisionCount", 0):
                    raise ShortLiveContractError("STARTUP_HISTORY_AFTER_FORWARD_DECISION_FORBIDDEN")
                engine = matches[0]
                history = engine.histories[spec.strategy_instance_id]
                if not history or not bars:
                    raise ShortLiveContractError("STARTUP_HISTORY_EXACT_ANCHOR_REQUIRED")
                expected = next_tradable_boundary_start(provider=spec.provider, instrument_id=spec.instrument_id,
                    symbol=spec.symbol, broker_id=spec.broker_id, closed_bar_end=parse_timestamp(history[-1].end_time))
                if parse_timestamp(bars[0].start_time) != expected:
                    raise ShortLiveContractError("STARTUP_HISTORY_DOES_NOT_JOIN_ORIGINAL_HISTORY")
                previous_end = None
                for bar in bars:
                    if (not isinstance(bar, ClosedBar) or bar.evidence_class != "HISTORICAL_WARMUP"
                            or bar.provider != spec.provider or bar.source_provider != spec.provider
                            or bar.instrument_id != spec.instrument_id or bar.symbol != spec.symbol
                            or bar.timeframe != spec.timeframe or parse_timestamp(bar.end_time) >= self._now()):
                        raise ShortLiveContractError("STARTUP_HISTORY_NATIVE_CLOSED_IDENTITY_REQUIRED")
                    if previous_end is not None and parse_timestamp(bar.start_time) != previous_end:
                        raise ShortLiveContractError("STARTUP_HISTORY_HAS_UNEXPLAINED_GAP")
                    previous_end = parse_timestamp(bar.end_time)
                engine.seed_history(spec.strategy_instance_id, bars)
                self._event("STARTUP_HISTORY_APPENDED", strategyInstanceId=spec.strategy_instance_id,
                            count=len(bars), sourceReceiptHash=receipt["contentHash"], naturalForwardCycles=0)

    def _worker(self) -> None:
        try:
            while True:
                with self._lock:
                    if not self._session or self._session["status"] in {"TERMINATED", "DRAINED"}:
                        break
                self.tick()
                self._wake.wait(self.poll_seconds)
                self._wake.clear()
        except Exception as exc:
            with self._lock:
                self._failure(f"WORKER_FAILED:{type(exc).__name__}:{exc}")
                self._stop_once("WORKER_FAILED")
        finally:
            with self._lock:
                if self._session:
                    self._session["workerExitIntent"] = True
                    self._event("WORKER_EXIT_INTENT", sessionId=self._session["sessionId"],
                                terminalVerified=self._session["terminalVerified"])

    def tick(self) -> dict[str, Any]:
        with self._lock:
            if not self._session:
                return self.snapshot(include_catalog=False)
            if self._session["status"] in {"TERMINATED", "DRAINED"}:
                return self.snapshot(include_catalog=False)
            if self._session["status"] in {"STOPPING", "BLOCKED"}:
                self._reconcile_and_finish()
                return self.snapshot(include_catalog=False)
            if not self._owner():
                self._stop_once("UI_OWNER_EXITED")
                return self.snapshot(include_catalog=False)
            contract = self._session["contract"]
            if (self._now() >= datetime.fromisoformat(self._session["endsAt"].replace("Z", "+00:00"))
                    or self.monotonic() - self._session["startedMonotonic"] >= contract["durationSeconds"]):
                self._stop_once("DURATION_EXPIRED")
                return self.snapshot(include_catalog=False)
            try:
                verify_contract(contract)
                blockers = self._guard(contract)
                if blockers:
                    raise ShortLiveContractError(",".join(blockers))
                if self.monotonic() - self._last_reconcile_at >= self.account_poll_seconds and not self._reconcile():
                    self._stop_once("ACCOUNT_RECONCILIATION_BLOCKED")
                    return self.snapshot(include_catalog=False)
                self._observe_pending_orders()
                if self._session["status"] in {"STOPPING", "BLOCKED", "TERMINATED", "DRAINED"}:
                    return self.snapshot(include_catalog=False)
                events = [(route, event) for route, feed in tuple(self._feeds.items())
                          for event in tuple(feed.poll(0))]
                # All same-boundary closed bars update their independent
                # targets before any next-open proposal is netted.
                events.sort(key=lambda pair: (
                    pair[1].end_time if isinstance(pair[1], ClosedBar) else pair[1].start_time,
                    0 if isinstance(pair[1], ClosedBar) else 1,
                    pair[0], pair[1].instrument_id, pair[1].timeframe))
                for route, event in events:
                    self._step_locked(route, event)
                    if self._session["status"] in {"STOPPING", "BLOCKED", "TERMINATED", "DRAINED"}:
                        break
            except Exception as exc:
                self._failure(f"RUNTIME_BLOCKED:{type(exc).__name__}:{exc}")
                self._stop_once("RUNTIME_BLOCKED")
            return self.snapshot(include_catalog=False)

    def step(self, route: str, bar: ClosedBar | OpenBoundary) -> dict[str, Any]:
        """Deterministic helper; same authority checks as the background loop."""
        with self._lock:
            if not self._session or self._session["status"] not in {"RUNNING", "PENDING_FILL"}:
                raise ShortLiveContractError("RUNNING_EXPLICIT_SCOPE_REQUIRED")
            try:
                if not self._owner():
                    raise ShortLiveContractError("GENUINE_UI_OWNER_UNAVAILABLE")
                verify_contract(self._session["contract"])
                blockers = self._guard(self._session["contract"])
                if blockers:
                    raise ShortLiveContractError(",".join(blockers))
                if (self._now() >= datetime.fromisoformat(self._session["endsAt"].replace("Z", "+00:00"))
                        or self.monotonic() - self._session["startedMonotonic"] >= self._session["contract"]["durationSeconds"]):
                    self._stop_once("DURATION_EXPIRED")
                elif self._reconcile():
                    self._step_locked(route, bar)
                else:
                    self._stop_once("ACCOUNT_RECONCILIATION_BLOCKED")
            except Exception as exc:
                self._failure(f"STEP_BLOCKED:{type(exc).__name__}:{exc}")
                self._stop_once("STEP_BLOCKED")
            return self.snapshot(include_catalog=False)

    def _step_locked(self, route: str, bar: ClosedBar | OpenBoundary) -> None:
        if isinstance(bar, OpenBoundary):
            self._open_locked(route, bar)
            return
        if not isinstance(bar, ClosedBar):
            raise ShortLiveContractError("GENUINE_CLOSED_BAR_DTO_REQUIRED")
        end = datetime.fromisoformat(bar.end_time.replace("Z", "+00:00"))
        if end > self._now():
            raise ShortLiveContractError("OPEN_BAR_NOT_EXECUTABLE")
        if end < datetime.fromisoformat(self._session["startedAt"].replace("Z", "+00:00")):
            raise ShortLiveContractError("OLD_BAR_CANNOT_BE_LIVE_OBSERVATION")
        cycles = []
        for (engine_route, _), engine in self._engines.items():
            if engine_route == route:
                cycle = engine.ingest_closed_bar(bar)
                if cycle:
                    cycles.append(cycle)
        decisions = [decision for cycle in cycles for decision in cycle.decisions]
        if not decisions:
            return
        changed = []
        for decision in decisions:
            row = self._session["sleeves"][decision.strategy_instance_id]
            spec = self._specs[decision.strategy_instance_id]
            row["lastSignal"] = decision.signal
            row["decisionCount"] = row.get("decisionCount", 0) + 1
            if decision.signal == "HOLD":
                continue
            if decision.metadata.get("dataSafetyBlocked"):
                continue
            entry = "SELL" if spec["positionDirection"] == "short" else "BUY"
            row["targetWeight"] = spec["targetWeight"]*spec["positionSizeFraction"] if decision.signal == entry else 0.0
            changed.append(decision.strategy_instance_id)
        self._event("BAR_EVALUATED", route=route, barDtoHash=canonical_hash(bar.to_dict()),
                    decisionKeys=[d.evaluation_key for d in decisions],
                    changedInstances=changed)
        if changed:
            timing = self._specs[changed[0]]["executionTiming"]
            if timing == "signal-close":
                self._dispatch_symbol(route, bar.symbol, bar.close, bar.dedupe_key, changed)
            else:
                key = (route, bar.instrument_id, bar.timeframe)
                if key in self._session["pendingNextOpen"]:
                    raise ShortLiveContractError("EXPECTED_NEXT_OPEN_WAS_NOT_OBSERVED")
                self._session["pendingNextOpen"][key] = {
                    "bar": bar, "changed": changed,
                    "decisionKeys": [d.evaluation_key for d in decisions]}

    def _open_locked(self, route: str, boundary: OpenBoundary) -> None:
        session = self._session
        key = (route, boundary.instrument_id, boundary.timeframe)
        pending = session["pendingNextOpen"].get(key)
        if not pending:
            return
        exact_attestation = {"KIS_KR_CASH": "KIS_WEBSOCKET", "BINANCE_SPOT": "BINANCE_WEBSOCKET",
                             "BINANCE_USDM_FUTURES": "BINANCE_WEBSOCKET", "UPBIT_KRW_SPOT": "UPBIT_WEBSOCKET"}.get(route)
        if (boundary.attestation != exact_attestation
                or boundary.recovery_only
                or boundary.source_provider != boundary.provider
                or boundary.provider != pending["bar"].provider):
            raise ShortLiveContractError("ACTUAL_NATIVE_NEXT_OPEN_ATTESTATION_REQUIRED")
        opened = datetime.fromisoformat(boundary.start_time.replace("Z", "+00:00"))
        observed = datetime.fromisoformat(boundary.observed_time.replace("Z", "+00:00"))
        previous = datetime.fromisoformat(pending["bar"].end_time.replace("Z", "+00:00"))
        if opened != previous:
            raise ShortLiveContractError("NEXT_OPEN_EXACT_PREDECESSOR_REQUIRED")
        if observed > self._now() or (self._now() - observed).total_seconds() > 2:
            raise ShortLiveContractError("NEXT_OPEN_NOT_CURRENT")
        if boundary.symbol != pending["bar"].symbol:
            raise ShortLiveContractError("NEXT_OPEN_SYMBOL_BINDING_MISMATCH")
        identity = (route, boundary.identity_key)
        if identity in session["observedOpenKeys"]:
            return
        session["observedOpenKeys"].add(identity)
        del session["pendingNextOpen"][key]
        self._event("NEXT_OPEN_OBSERVED", route=route,
                    rawHash=boundary.raw_hash, identityKey=boundary.identity_key)
        self._dispatch_symbol(route, boundary.symbol, boundary.open,
                              boundary.identity_key, pending["changed"])

    def _dispatch_symbol(self, route: str, symbol: str, reference_price: float,
                         boundary_key: str, changed: list[str]) -> None:
        session = self._session
        if self._has_pending_orders():
            self._event("ORDER_DEFERRED_PENDING_FILL", route=route, symbol=symbol)
            return
        # A cached UI/account polling snapshot cannot authorize a mutation.
        if not self._reconcile():
            raise ShortLiveContractError("FRESH_PREORDER_RECONCILIATION_REQUIRED")
        contract = session["contract"]
        leg = leg_manifest(contract, route)
        members = [s for s in contract["specs"] if s["route"] == route and s["symbol"] == symbol]
        all_weight = sum(s["targetWeight"] for s in contract["specs"] if s["route"] == route)
        # Current market notional must be converted by the official adapter;
        # only a verified KRW snapshot can determine a cross-currency delta.
        row = session["brokerRows"].get(route, {})
        native = row.get("positionsBySymbol", {}).get(symbol)
        if not isinstance(native, Mapping):
            if row.get("ready") is True and not row.get("ownedPositions") and row.get("exposureKrw") == 0:
                native = {"exposureKrw": 0, "quantity": 0}
            else:
                raise ShortLiveContractError("CURRENT_SYMBOL_ACCOUNT_OBSERVATION_REQUIRED")
        current_krw = finite(native.get("exposureKrw"), "CURRENT_SYMBOL_EXPOSURE")
        native_signed_quantity = float(native.get("quantity"))
        native_quantity = finite(abs(native_signed_quantity), "CURRENT_SYMBOL_QUANTITY")
        ledger_signed_quantity = sum(session["sleeves"][s["strategyInstanceId"]]["quantity"] for s in members)
        if abs(native_signed_quantity-ledger_signed_quantity) > max(1e-10, native_quantity*1e-8):
            raise ShortLiveContractError("EXACT_SYMBOL_SLEEVE_OWNERSHIP_RECONCILIATION_REQUIRED")
        entry_weights, exit_units = {}, {}
        for member in members:
            key = member["strategyInstanceId"]
            if key not in changed:
                continue
            sleeve = session["sleeves"][key]
            held = abs(sleeve["quantity"])
            if sleeve["targetWeight"] == 0 and held > 1e-10:
                exit_units[key] = held
            elif sleeve["targetWeight"] > 0 and held <= 1e-10:
                entry_weights[key] = sleeve["targetWeight"] / all_weight
        if session["newEntriesBlocked"]:
            entry_weights = {}
        if entry_weights and exit_units:
            transferred = self._transfer_internal_units(route, symbol, reference_price, boundary_key, changed,
                entry_weights, exit_units, current_krw/native_quantity, native_signed_quantity)
            if transferred is None:
                return  # Same immutable cross may never produce a second net order.
            entry_weights, exit_units = transferred
        if not entry_weights and not exit_units:
            return
        reducing = bool(exit_units)
        if session["newEntriesBlocked"] and not reducing:
            return
        close_quantity = sum(exit_units.values())
        delta = (current_krw*close_quantity/native_quantity if reducing
                 else sum(entry_weights.values())*session["effectiveRouteBudgets"][route] *
                 float(leg.get("futuresPolicy", {}).get("leverage", 1)))
        if delta <= 0:
            raise ShortLiveContractError("EXACT_CHANGED_SLEEVE_NOTIONAL_REQUIRED")
        direction = members[0]["positionDirection"]
        side = ("SELL" if reducing else "BUY") if direction == "long" else ("BUY" if reducing else "SELL")
        identity = {"sessionId": session["sessionId"], "route": route, "symbol": symbol,
                    "bar": boundary_key, "targets": {s["strategyInstanceId"]: session["sleeves"][s["strategyInstanceId"]]["targetWeight"] for s in members}}
        client_id = "slt-" + canonical_hash(identity)[:28]
        if client_id in session["orders"]:
            return  # UNKNOWN/ACCEPTED intents are never submitted again.
        manifest = self._route_session(route)
        proposal = {"sessionId": session["sessionId"], "manifestHash": contract["manifestHash"],
                    "route": route, "symbol": symbol, "side": side,
                    "deltaKrw": abs(delta), "notionalKrw": abs(delta),
                    "referencePrice": reference_price, "clientOrderId": client_id,
                    "riskReducing": reducing, "positionDirection": direction,
                    "accountId": manifest["accountId"], "ownershipScope": manifest["ownershipScope"],
                    "routeBudgetKrw": leg["routeBudgetKrw"],
                    "brokerBudgetKrw": leg["brokerBudgetKrw"],
                    "dailyLossStopKrw": leg["dailyLossStopKrw"],
                    "originalAllocationWeightSum": all_weight,
                    "effectiveSleeveTargets": {s["strategyInstanceId"]: session["sleeves"][s["strategyInstanceId"]]["targetWeight"]
                                               for s in members}}
        if reducing:
            proposal["closeQuantity"] = close_quantity
        sized = self._brokers[route].size_order(deepcopy(proposal))
        if not isinstance(sized, Mapping) or sized.get("ready") is not True:
            raise ShortLiveContractError("NATIVE_ORDER_SIZE_BLOCKED:" + ",".join(sized.get("blockers", []) if isinstance(sized, Mapping) else []))
        quantity = finite(sized.get("quantity"), "NATIVE_QUANTITY", positive=True)
        if reducing and quantity > close_quantity + max(1e-10, close_quantity*1e-8):
            raise ShortLiveContractError("NATIVE_CLOSE_EXCEEDS_CHANGED_SLEEVE_OWNERSHIP")
        notional = finite(sized.get("estimatedNotionalKrw"), "NATIVE_NOTIONAL", positive=True)
        projected = finite(sized.get("projectedCommittedCapitalKrw", sized.get("projectedGrossExposureKrw")), "PROJECTED_CAPITAL")
        broker_group = ROUTES[route]
        other_exposure = sum(float(row.get("committedCapitalKrw", row.get("exposureKrw", 0))) for r, row in session["brokerRows"].items()
                             if ROUTES[r] == broker_group and r != route)
        if not reducing and (projected + other_exposure > leg["brokerBudgetKrw"] + 1e-8
                             or finite(sized.get("requiredCapitalKrw", notional), "REQUIRED_CAPITAL") > leg["routeBudgetKrw"] + 1e-8):
            raise ShortLiveContractError("SHARED_BROKER_BUDGET_EXCEEDED")
        order = {**proposal, **{key: value for key, value in sized.items()
                              if key in {"quantity", "referencePrice", "feeKrw", "estimatedNotionalKrw", "projectedGrossExposureKrw",
                                         "observedAt", "fxObservedAt", "krwPerQuote", "positionMode", "leverage",
                                         "requiredCapitalKrw", "projectedCommittedCapitalKrw"}}}
        order["quantity"] = quantity
        differences = exit_units if reducing else entry_weights
        denominator = sum(differences.values())
        if denominator <= 0:
            raise ShortLiveContractError("FILL_ATTRIBUTION_UNRESOLVED")
        session["orders"][client_id] = {
            "status": "SUBMITTING", "order": deepcopy(order),
            "appliedBaseCommissionQuantity": 0.0,
            "allocationWeights": {key: value / denominator for key, value in differences.items()},
            "appliedFilledQuantity": 0.0, "appliedFilledNotional": 0.0,
            "submittedMonotonic": self.monotonic(), "brokerOrderId": "",
        }
        session["orderCount"] += 1
        for key, share in session["orders"][client_id]["allocationWeights"].items():
            if share > 0:
                sleeve = session["sleeves"][key]
                sleeve["orderAttemptCount"] = sleeve.get("orderAttemptCount", 0) + 1
        self._event("ORDER_INTENT_BEFORE_SUBMIT", clientOrderId=client_id,
                    route=route, symbol=symbol, side=side, quantity=quantity,
                    riskReducing=reducing, allocationWeights=session["orders"][client_id]["allocationWeights"],
                    instanceIds=[s["strategyInstanceId"] for s in members])
        try:
            reply = self._brokers[route].submit(deepcopy(order))
        except Exception as exc:
            reply = {"status": "UNKNOWN", "reason": type(exc).__name__}
        self._handle_order_observation(client_id, reply)
        if not self._reconcile():
            self._stop_once("POST_ORDER_RECONCILIATION_BLOCKED")

    def _transfer_internal_units(self, route, symbol, reference_price, boundary_key, changed,
                                 entry_weights, exit_units, krw_per_unit, native_signed_quantity):
        """Reuse normal portfolio crossing; transfer owned units, never broker fills."""
        session = self._session
        budget = session["effectiveRouteBudgets"][route] * float(
            leg_manifest(session["contract"], route).get("futuresPolicy", {}).get("leverage", 1))
        source_keys = sorted(set(entry_weights) | set(exit_units))
        identity = {"sessionId": session["sessionId"], "route": route, "symbol": symbol,
                    "boundary": boundary_key, "changed": sorted(changed),
                    "targets": {key: session["sleeves"][key]["targetWeight"] for key in source_keys}}
        transfer_id = canonical_hash(identity)
        if transfer_id in session["internalTransfers"]:
            return None
        deltas = []
        for key in source_keys:
            held = Decimal(str(abs(session["sleeves"][key]["quantity"])))
            delta = (Decimal(str(entry_weights[key]*budget/krw_per_unit)) if key in entry_weights
                     else -Decimal(str(exit_units[key])))
            deltas.append(SleeveDelta(transfer_id+":"+key, key, held, held+delta, delta))
        internal, external = _split_internal_and_external_allocations(tuple(deltas))
        if not internal or sum((item.signed_quantity for item in internal), Decimal(0)) != 0:
            raise ShortLiveContractError("INTERNAL_ALLOCATION_DOES_NOT_CONSERVE_PHYSICAL_UNITS")
        direction = self._specs[source_keys[0]]["positionDirection"]
        sign = -1 if direction == "short" else 1
        before = {key: {"quantity": session["sleeves"][key]["quantity"],
                        "averagePrice": session["sleeves"][key]["averagePrice"]} for key in source_keys}
        after = deepcopy(before)
        for item in internal:
            row = after[item.sleeve_id]
            owned = abs(row["quantity"])
            delta = float(item.signed_quantity)
            result = owned+delta
            if result < -max(1e-10, owned*1e-8):
                raise ShortLiveContractError("INTERNAL_ALLOCATION_EXCEEDS_OWNED_SLEEVE_UNITS")
            row["quantity"] = sign*max(0.0, result)
            if result <= 1e-10:
                row["quantity"], row["averagePrice"] = 0.0, 0.0
            elif delta > 0:
                row["averagePrice"] = (owned*row["averagePrice"]+delta*reference_price)/result
        before_sum, after_sum = (sum(values[key]["quantity"] for key in source_keys) for values in (before, after))
        if abs(before_sum-after_sum) > max(1e-10, abs(native_signed_quantity)*1e-8):
            raise ShortLiveContractError("INTERNAL_ALLOCATION_CHANGED_ACTUAL_BROKER_POSITION")
        body = {"schemaVersion": "short-live-internal-allocation-transfer-v1", "purpose": PURPOSE,
                "promotionEligible": False, "transferId": transfer_id, **identity,
                "positionDirection": direction, "referencePrice": reference_price,
                "krwPerOwnedUnit": krw_per_unit, "actualBrokerSignedQuantity": native_signed_quantity,
                "sleevesBefore": before, "sleevesAfter": after,
                "originalSignals": {key: session["sleeves"][key].get("lastSignal", "FIXTURE_OR_NOT_OBSERVED") for key in source_keys},
                "sourceParametersHashes": {key: canonical_hash(self._specs[key]["parameters"]) for key in source_keys},
                "internalAllocations": [{"instanceId": item.sleeve_id, "signedOwnedQuantity": str(item.signed_quantity)} for item in internal],
                "externalAllocations": [{"instanceId": item.sleeve_id, "signedOwnedQuantity": str(item.signed_quantity)} for item in external],
                "feeKrw": 0, "externalOrders": 0, "externalFills": 0}
        # A lost reply or process crash cannot silently replay an ownership move.
        self._write("internal-allocation-"+transfer_id, body)
        for key, values in after.items():
            session["sleeves"][key].update(values)
            session["sleeves"][key]["internalTransferCount"] = session["sleeves"][key].get("internalTransferCount", 0)+1
        session["internalTransfers"][transfer_id] = body
        session["internalTransferCount"] += 1
        self._event("INTERNAL_ALLOCATION_TRANSFER", transferId=transfer_id, route=route, symbol=symbol,
                    feeKrw=0, externalOrders=0, externalFills=0,
                    signedPhysicalQuantityBefore=before_sum, signedPhysicalQuantityAfter=after_sum)
        residual_entries = {item.sleeve_id: float(item.signed_quantity)*krw_per_unit/budget
                            for item in external if item.signed_quantity > 0}
        residual_exits = {item.sleeve_id: float(-item.signed_quantity)
                          for item in external if item.signed_quantity < 0}
        return residual_entries, residual_exits

    def _has_pending_orders(self) -> bool:
        return bool(self._session and any(
            row["status"] in {"ACCEPTED", "PARTIAL"} and not row.get("cleanup")
            for row in self._session["orders"].values()))

    def _handle_order_observation(self, client_id: str, reply: Any) -> None:
        session = self._session
        record = session["orders"][client_id]
        order = record["order"]
        route = order["route"]
        status = reply.get("status") if isinstance(reply, Mapping) else "UNKNOWN"
        if status not in {"FILLED", "PARTIAL", "REJECTED", "ACCEPTED", "CANCELED"}:
            status = "UNKNOWN"
        if isinstance(reply, Mapping) and status != "UNKNOWN":
            returned_id = str(reply.get("brokerOrderId") or "")
            if (not returned_id or reply.get("clientOrderId", client_id) != client_id
                    or record["brokerOrderId"] and returned_id != record["brokerOrderId"]):
                status = "UNKNOWN"
            else:
                record["brokerOrderId"] = returned_id
        record["status"] = status
        self._event("ORDER_REPLY", clientOrderId=client_id, status=status,
                    brokerOrderId=str(reply.get("brokerOrderId", "")) if isinstance(reply, Mapping) else "")
        if status == "UNKNOWN":
            session["unknownOrderCount"] += 1
            self._failure("ORDER_RESULT_UNKNOWN_NO_RETRY")
            self._stop_once("ORDER_RESULT_UNKNOWN_NO_RETRY")
            return
        if status == "REJECTED":
            self._failure("BROKER_ORDER_REJECTED")
            self._stop_once("BROKER_ORDER_REJECTED")
            return
        filled = finite(reply.get("filledQuantity", 0), "FILLED_QUANTITY")
        if filled < record["appliedFilledQuantity"] - 1e-10 or filled > finite(order["quantity"], "RESERVED_QUANTITY") + 1e-9:
            raise ShortLiveContractError("BROKER_FILL_EXCEEDS_REQUEST")
        if status == "FILLED" and filled <= 0:
            raise ShortLiveContractError("COMPLETE_FILL_QUANTITY_MISSING")
        if status == "FILLED" and abs(filled - finite(order["quantity"], "RESERVED_QUANTITY")) > max(1e-10, filled * 1e-9):
            raise ShortLiveContractError("FILLED_STATUS_QUANTITY_CONFLICT")
        increment = filled - record["appliedFilledQuantity"]
        if increment <= 1e-10 and filled > 0:
            observed_price = finite(reply.get("fillPrice"), "CUMULATIVE_FILL_PRICE", positive=True)
            if abs(observed_price * filled - record["appliedFilledNotional"]) > max(1e-8, record["appliedFilledNotional"] * 1e-9):
                raise ShortLiveContractError("CUMULATIVE_FILL_PRICE_CONFLICT")
        if increment > 1e-10:
            self._apply_fill_increment(client_id, reply, filled, increment)
        if order["route"] == "BINANCE_SPOT" and filled > 0:
            if reply.get("baseCommissionQuantity") is None or reply.get("baseCommissionKnown") is False:
                raise ShortLiveContractError("EXACT_BASE_ASSET_COMMISSION_REQUIRED")
            commission = finite(reply["baseCommissionQuantity"], "BASE_ASSET_COMMISSION")
            previous_commission = record.get("appliedBaseCommissionQuantity", 0.0)
            if commission < previous_commission or commission > filled:
                raise ShortLiveContractError("BASE_ASSET_COMMISSION_CONFLICT")
            increase = commission-previous_commission
            if increase:
                for key, share in record["allocationWeights"].items():
                    sleeve = session["sleeves"][key]
                    sleeve["quantity"] -= increase*share
                    if sleeve["quantity"] < -1e-9:
                        raise ShortLiveContractError("BASE_ASSET_COMMISSION_EXCEEDS_OWNED_SPOT_POSITION")
                record["appliedBaseCommissionQuantity"] = commission
                self._event("ACTUAL_BASE_ASSET_COMMISSION", clientOrderId=client_id,
                            cumulativeQuantity=commission, incrementalQuantity=increase)
        if status in {"ACCEPTED", "PARTIAL"} and session["firstFailure"] is None:
            session["status"] = "PENDING_FILL"
            session["newEntriesBlocked"] = True
        elif not self._has_pending_orders() and session["firstFailure"] is None:
            session["status"] = "RUNNING"
            session["newEntriesBlocked"] = False

    def _apply_fill_increment(self, client_id: str, reply: Mapping[str, Any], filled: float, increment: float) -> None:
        session = self._session
        record = session["orders"][client_id]
        order = record["order"]
        route = order["route"]
        fill_price = finite(reply.get("fillPrice"), "FILL_PRICE", positive=True)
        if reply.get("feeKrw") is None or reply.get("actualFeeKnown") is False:
            readiness = session["readiness"][route]
            if readiness.get("boundedCostAllowanceVerified") is not True or readiness.get("actualEquityLossTracked") is not True:
                raise ShortLiveContractError("ACTUAL_FEE_UNKNOWN_WITHOUT_BOUNDED_COST_AND_EQUITY_GUARD")
            session["feesKnown"] = False
            session["brokerRows"][route]["actualFeeKnown"] = False
            session["brokerRows"][route]["feeReconciliationStatus"] = "UNVERIFIED_ACTUAL_FEE"
        else:
            finite(reply.get("feeKrw"), "ACTUAL_FEE_KRW")
        cumulative_notional = filled * fill_price
        incremental_notional = cumulative_notional - record["appliedFilledNotional"]
        if incremental_notional <= 0:
            raise ShortLiveContractError("CUMULATIVE_FILL_PRICE_CONFLICT")
        incremental_price = incremental_notional / increment
        self._event("ACTUAL_FILL_INCREMENT", clientOrderId=client_id,
                    cumulativeQuantity=filled, incrementalQuantity=increment,
                    cumulativeAveragePrice=fill_price,
                    actualFeeKrw=reply.get("feeKrw"), actualFeeKnown=session["feesKnown"])
        session["fillCount"] += 1
        sign = -1 if order["positionDirection"] == "short" else 1
        reducing = order["riskReducing"]
        for key, share in record["allocationWeights"].items():
            row = session["sleeves"][key]
            amount = increment * share
            old_quantity = abs(row["quantity"])
            if reducing and amount > old_quantity + max(1e-10, old_quantity*1e-8):
                raise ShortLiveContractError("ACTUAL_FILL_EXCEEDS_ATTRIBUTED_SLEEVE_OWNERSHIP")
            row["quantity"] += sign * amount * (-1 if reducing else 1)
            if abs(row["quantity"]) < 1e-10:
                row["quantity"] = 0.0
                row["averagePrice"] = 0.0
            elif not reducing:
                row["averagePrice"] = (old_quantity * row["averagePrice"] + amount * incremental_price) / abs(row["quantity"])
            row["fillCount"] = row.get("fillCount", 0) + int(amount > 0)
        record["appliedFilledQuantity"] = filled
        record["appliedFilledNotional"] = cumulative_notional

    def _observe_pending_orders(self) -> None:
        session = self._session
        for client_id, record in tuple(session["orders"].items()):
            if record["status"] not in {"ACCEPTED", "PARTIAL"} or record.get("cleanup"):
                continue
            if self.monotonic() - record["submittedMonotonic"] >= self.order_timeout_seconds:
                self._failure("EXACT_PENDING_ORDER_TIMEOUT")
                self._stop_once("EXACT_PENDING_ORDER_TIMEOUT")
                return
            adapter = self._brokers[record["order"]["route"]]
            observer = getattr(adapter, "observe_order", None)
            if not callable(observer):
                raise ShortLiveContractError("EXACT_ORDER_OBSERVER_UNAVAILABLE")
            self._handle_order_observation(client_id, observer(client_id))
            if session["status"] in {"STOPPING", "BLOCKED", "TERMINATED", "DRAINED"}:
                return

    def _route_session(self, route: str) -> dict[str, Any]:
        session = self._session
        result = leg_manifest(session["contract"], route)
        result["accountId"] = session["readiness"][route]["accountId"]
        result["clientOrderIds"] = [key for key, value in session["orders"].items() if value["order"]["route"] == route]
        result["orders"] = deepcopy({key: value for key, value in session["orders"].items() if value["order"]["route"] == route})
        result["newEntriesBlocked"] = session["newEntriesBlocked"]
        result["startedAt"] = session["startedAt"]
        return result

    def _reconcile(self, *, enforce_limits: bool = True) -> bool:
        session = self._session
        okay = True
        losses: dict[str, float] = {}
        for route, adapter in self._brokers.items():
            value = adapter.reconcile(self._route_session(route))
            if (not isinstance(value, Mapping) or value.get("ready") is not True
                    or value.get("known") is not True
                    or not isinstance(value.get("ownedOrders"), list)
                    or not isinstance(value.get("ownedPositions"), list)
                    or not isinstance(value.get("positionsBySymbol"), Mapping)):
                session["brokerRows"].pop(route, None)
                codes = value.get("blockers", []) if isinstance(value, Mapping) else []
                # Preserve the first concrete cause instead of masking a
                # protective-stop/force-close incident as a generic failure.
                for code in codes:
                    if isinstance(code, str) and len(code) <= 180 and all(c.isupper() or c.isdigit() or c == "_" for c in code):
                        self._failure(f"{route}:{code}")
                self._failure(f"{route}:RECONCILIATION_UNKNOWN")
                okay = False
                continue
            try:
                exposure = finite(value.get("exposureKrw"), "EXPOSURE_KRW")
                realized = finite(value.get("realizedLossKrw"), "REALIZED_LOSS_KRW")
                unrealized = finite(value.get("unrealizedLossKrw"), "UNREALIZED_LOSS_KRW")
                committed = finite(value.get("committedCapitalKrw", exposure), "COMMITTED_CAPITAL")
            except ShortLiveContractError:
                session["brokerRows"].pop(route, None)
                self._failure(f"{route}:LOSS_OR_EXPOSURE_UNKNOWN")
                okay = False
                continue
            group = ROUTES[route]
            losses[group] = losses.get(group, 0) + realized + unrealized
            session["brokerRows"][route] = {
                "route": route, "ready": True, "exposureKrw": exposure,
                "accountObservedAt": utc_text(self._now()),
                "monitorEquity": {key: deepcopy(value.get("monitorEquity", {}).get(key))
                                  for key in ("basis", "baselineNative", "currentNative", "quoteCurrency",
                                              "krwPerQuote", "preservedSpotExcluded", "cashFlowAdjusted",
                                              "realizedPnlKrw", "unrealizedPnlKrw")}
                                  if isinstance(value.get("monitorEquity"), Mapping) else None,
                "realizedLossKrw": realized, "unrealizedLossKrw": unrealized,
                "ownedOrders": deepcopy(value.get("ownedOrders", [])),
                "ownedPositions": deepcopy(value.get("ownedPositions", [])),
                "ownedProtectionOrders": deepcopy(value.get("ownedProtectionOrders", [])),
                "protectionRetirementVerified": value.get("protectionRetirementVerified") is True,
                "positionsBySymbol": deepcopy(value.get("positionsBySymbol", {})),
                "lossBasis": value.get("lossBasis", "EXACT_SCOPE_PNL"),
                "actualFeeKnown": session["feesKnown"],
                "feeReconciliationStatus": "KNOWN" if session["feesKnown"] else "UNVERIFIED_ACTUAL_FEE",
                "committedCapitalKrw": committed, "notionalExposureKrw": exposure,
                **{key: deepcopy(value.get(key, [] if key != "observedFuturesSettingsBySymbol" else {}))
                   for key in ("protections", "futuresPositions", "futuresIncidents", "observedFuturesSettingsBySymbol")},
            }
            for incident in value.get("futuresIncidents", []):
                code = incident.get("code") or {"LIQUIDATION": "FUTURES_LIQUIDATION_DETECTED",
                    "ADL": "FUTURES_ADL_DETECTED", "PROTECTIVE_STOP": "FUTURES_PROTECTIVE_STOP_TRIGGERED"}.get(incident.get("type"))
                if code:
                    identity = canonical_hash({key: incident.get(key) for key in
                        ("type", "code", "symbol", "positionSide", "brokerOrderId", "algoId")})
                    prior_incident = session["externalExecutions"].get(identity, {})
                    if incident.get("verified") is True and incident.get("type") in {"PROTECTIVE_STOP", "LIQUIDATION", "ADL"}:
                        cumulative = finite(incident.get("quantity"), "EXTERNAL_CUMULATIVE_QUANTITY")
                        increment = cumulative - float(prior_incident.get("quantity", 0))
                        members = [key for key, spec in self._specs.items() if spec["route"] == route and spec["symbol"] == incident["symbol"]]
                        total = sum(abs(session["sleeves"][key]["quantity"]) for key in members)
                        if increment < 0 or increment > total + max(1e-9, total*1e-8):
                            self._failure("FUTURES_EXTERNAL_SLEEVE_ATTRIBUTION_UNKNOWN")
                            okay = False
                        elif increment > 0 and total > 0:
                            allocations = {key: increment*abs(session["sleeves"][key]["quantity"])/total for key in members}
                            self._event("FUTURES_EXTERNAL_FILL_ATTRIBUTED", route=route, incident=deepcopy(incident), allocations=allocations)
                            for key, amount in allocations.items():
                                sleeve = session["sleeves"][key]
                                sign = -1 if sleeve["quantity"] < 0 else 1
                                sleeve["quantity"] -= sign * amount
                                if abs(sleeve["quantity"]) < 1e-9:
                                    sleeve["quantity"], sleeve["averagePrice"] = 0.0, 0.0
                            session["externalExecutions"][identity] = deepcopy(incident)
                    if not prior_incident or prior_incident.get("quantity") != incident.get("quantity"):
                        session["externalExecutions"].setdefault(identity, deepcopy(incident))
                        self._event("FUTURES_RISK_EVENT", route=route, incident=deepcopy(incident))
                    self._failure(f"{route}:{code}")
                    if enforce_limits:
                        okay = False
            if any(leg["route"] == route and committed > leg["routeBudgetKrw"] + 1e-8 for leg in session["contract"]["legs"]):
                self._failure(f"{route}:EXPOSURE_LIMIT_REACHED")
                if enforce_limits:
                    okay = False
        for group, loss in losses.items():
            if loss >= session["contract"]["dailyLossStopKrwPerBroker"]:
                self._failure(f"{group}:DAILY_LOSS_STOP_REACHED")
                if enforce_limits:
                    okay = False
        session["dailyLossByBroker"] = losses
        self._last_reconcile_at = self.monotonic()
        return okay

    def dispatch_authorized(self, order: Mapping[str, Any]) -> bool:
        """Final adapter callback: only this scope's already reserved intent.

        This is not a generic boolean permit from a request payload.  The
        instance owns the durable reservation and compares actual order fields.
        Cleanup uses separately registered, position-verified reducing intents.
        """
        with self._lock:
            session = self._session
            if not session or not isinstance(order, Mapping):
                return False
            if (order.get("sessionId") != session["sessionId"]
                    or order.get("manifestHash") != session["scopeHash"]
                    or order.get("route") not in self._brokers):
                return False
            if order.get("operation") in {"PROTECT", "CANCEL_PROTECTION"}:
                key = (order["operation"], order.get("clientAlgoId"))
                if (session["protectionReservations"].get(key) != canonical_hash(dict(order))
                        or session["status"] not in {"RUNNING", "STOPPING"}):
                    return False
                if order["operation"] == "CANCEL_PROTECTION":
                    return True
                parent = session["orders"].get(order.get("parentClientOrderId"), {})
                entry = parent.get("order", {})
                return bool(session["status"] == "RUNNING" and self._owner() and not session["newEntriesBlocked"]
                    and parent.get("status") == "SUBMITTING" and entry.get("riskReducing") is False
                    and entry.get("side") in {"BUY", "SELL"}
                    and order.get("side") == ("SELL" if entry["side"] == "BUY" else "BUY")
                    and all(entry.get(field) == order.get(field) for field in
                            ("sessionId", "manifestHash", "route", "accountId", "symbol", "positionDirection", "positionMode")))
            if order.get("operation") == "CANCEL":
                reserved = session["cancelReservations"].get((order["route"], str(order.get("brokerOrderId", ""))))
                return bool(session["status"] == "STOPPING" and reserved
                            and reserved == canonical_hash(dict(order)))
            record = session["orders"].get(str(order.get("clientOrderId", "")))
            if not record or record["status"] != "SUBMITTING":
                return False
            expected = record["order"]
            for key in ("sessionId", "manifestHash", "route", "symbol", "side",
                        "clientOrderId", "riskReducing", "positionDirection", "accountId", "operation", "positionMode"):
                if order.get(key) != expected.get(key):
                    return False
            try:
                for key in ("quantity", "referencePrice", "leverage"):
                    if key == "leverage" and order.get(key) is None and expected.get(key) is None:
                        continue
                    if isinstance(order.get(key), bool) or isinstance(expected.get(key), bool):
                        return False
                    actual_number, reserved_number = Decimal(str(order.get(key))), Decimal(str(expected.get(key)))
                    if (not actual_number.is_finite() or actual_number <= 0
                            or not reserved_number.is_finite() or actual_number != reserved_number):
                        return False
            except (ArithmeticError, ValueError, TypeError):
                return False
            if record.get("cleanup") is True:
                return session["status"] == "STOPPING" and order.get("riskReducing") is True
            return self._owner() and session["status"] == "RUNNING" and (
                order.get("riskReducing") is True or session["newEntriesBlocked"] is False)

    def reserve_protection(self, order):
        """Durable single-use stop authority; only a reserved entry may arm it."""
        with self._lock:
            session = self._session
            if (not session or not isinstance(order, Mapping) or order.get("route") != "BINANCE_USDM_FUTURES"
                    or order.get("route") not in self._brokers
                    or order.get("sessionId") != session["sessionId"]
                    or order.get("manifestHash") != session["scopeHash"]
                    or order.get("riskReducing") is not True):
                return False
            operation, client = order.get("operation"), order.get("clientAlgoId")
            key = (operation, client)
            if operation not in {"PROTECT", "CANCEL_PROTECTION"} or not client or key in session["protectionReservations"]:
                return False
            manifest = self._route_session(order["route"])
            if order.get("symbol") not in manifest["symbols"] or order.get("accountId") != manifest["accountId"]:
                return False
            if operation == "PROTECT":
                parent = session["orders"].get(order.get("parentClientOrderId"), {})
                entry = parent.get("order", {})
                if (session["status"] != "RUNNING" or session["newEntriesBlocked"] or not self._owner()
                        or parent.get("status") != "SUBMITTING" or entry.get("riskReducing") is not False
                        or entry.get("side") not in {"BUY", "SELL"}
                        or any(entry.get(field) != order.get(field) for field in
                               ("sessionId", "manifestHash", "route", "accountId", "symbol", "positionDirection", "positionMode"))
                        or order.get("side") != ("SELL" if entry.get("side") == "BUY" else "BUY")):
                    return False
            else:
                observed = session["protectionObservations"].get(client, {})
                original = {**dict(order), "operation": "PROTECT"}
                original.pop("algoId", None)
                if (session["status"] not in {"RUNNING", "STOPPING"}
                        or observed.get("known") is not True
                        or session["protectionReservations"].get(("PROTECT", client)) != canonical_hash(original)
                        or str(observed.get("algoId")) != str(order.get("algoId"))
                        or observed.get("symbol") != order["symbol"]
                        or observed.get("clientAlgoId") != client):
                    return False
            self._event("FUTURES_PROTECTION_RESERVED", operation=operation, order=deepcopy(dict(order)))
            session["protectionReservations"][key] = canonical_hash(dict(order))
            return True

    def record_protection(self, event, detail):
        with self._lock:
            if not self._session:
                raise ShortLiveContractError("PROTECTION_SESSION_UNAVAILABLE")
            self._event(event, detail=deepcopy(detail))
            if event == "FUTURES_PROTECTION_OBSERVED":
                self._session["protectionObservations"][detail["clientAlgoId"]] = deepcopy(detail["observation"])

    def reserve_cleanup_cancel(self, order: Mapping[str, Any]) -> bool:
        with self._lock:
            session = self._session
            if (not session or session["status"] != "STOPPING"
                    or order.get("operation") != "CANCEL" or order.get("riskReducing") is not True
                    or order.get("sessionId") != session["sessionId"]
                    or order.get("manifestHash") != session["scopeHash"]):
                return False
            route = str(order.get("route", ""))
            if route not in self._brokers or not self._reconcile(enforce_limits=False):
                return False
            broker_id = str(order.get("brokerOrderId", ""))
            key = (route, broker_id)
            if not broker_id or key in session["cancelReservations"]:
                return False
            owned = session["brokerRows"][route]["ownedOrders"]
            if not any(str(row.get("brokerOrderId", "")) == broker_id for row in owned):
                return False
            self._event("CLEANUP_CANCEL_RESERVED", route=route, brokerOrderId=broker_id)
            session["cancelReservations"][key] = canonical_hash(dict(order))
            return True

    def reserve_cleanup_order(self, order: Mapping[str, Any]) -> bool:
        """Called only by the normal adapter after exact owned-position read."""
        with self._lock:
            session = self._session
            if (not session or session["status"] != "STOPPING"
                    or not session["cleanupAttempted"] or order.get("riskReducing") is not True
                    or order.get("sessionId") != session["sessionId"]
                    or order.get("manifestHash") != session["scopeHash"]):
                return False
            route = str(order.get("route", ""))
            if route not in self._brokers:
                return False
            manifest = self._route_session(route)
            if order.get("accountId") != manifest["accountId"] or order.get("symbol") not in manifest["symbols"]:
                return False
            if not self._reconcile(enforce_limits=False):
                return False
            position = session["brokerRows"][route]["positionsBySymbol"].get(order["symbol"])
            if not isinstance(position, Mapping):
                return False
            try:
                quantity = finite(order.get("quantity"), "CLEANUP_QUANTITY", positive=True)
                held = abs(float(position["quantity"]))
                if quantity > held + 1e-10:
                    return False
            except (ShortLiveContractError, ValueError, TypeError, KeyError):
                return False
            direction = "short" if float(position["quantity"]) < 0 else "long"
            if order.get("side") != ("BUY" if direction == "short" else "SELL"):
                return False
            client_id = str(order.get("clientOrderId", ""))
            if not client_id or client_id in session["orders"]:
                return False
            allocation_weights = {}
            if route == "BINANCE_USDM_FUTURES":
                members = [key for key, spec in self._specs.items()
                           if spec["route"] == route and spec["symbol"] == order["symbol"]]
                total = sum(abs(session["sleeves"][key]["quantity"]) for key in members)
                if total <= 0 or abs(total-held) > max(1e-9, held*1e-8):
                    return False
                allocation_weights = {key: abs(session["sleeves"][key]["quantity"])/total for key in members}
            self._event("CLEANUP_ORDER_RESERVED", clientOrderId=client_id,
                        route=route, symbol=order["symbol"], quantity=quantity)
            session["orders"][client_id] = {"status": "SUBMITTING", "cleanup": True,
                                            "order": deepcopy(dict(order)), "brokerOrderId": "",
                                            "appliedFilledQuantity": 0.0, "appliedFilledNotional": 0.0,
                                            "allocationWeights": allocation_weights,
                                            "appliedBaseCommissionQuantity": 0.0}
            return True

    def observe_cleanup_order(self, client_order_id: str, result: Mapping[str, Any]) -> bool:
        """Persist an actual cleanup receipt; never manufacture sleeve fills."""
        with self._lock:
            session = self._session
            record = session["orders"].get(client_order_id) if session else None
            if not record or record.get("cleanup") is not True or not isinstance(result, Mapping):
                return False
            status = str(result.get("status") or "UNKNOWN").upper()
            if status not in {"FILLED", "PARTIAL", "ACCEPTED", "CANCELED", "REJECTED", "UNKNOWN"}:
                status = "UNKNOWN"
            broker_id = str(result.get("brokerOrderId") or "")
            if (result.get("clientOrderId", client_order_id) != client_order_id
                    or (status != "UNKNOWN" and not broker_id)
                    or (record["brokerOrderId"] and broker_id != record["brokerOrderId"])):
                status = "UNKNOWN"
            if broker_id:
                record["brokerOrderId"] = broker_id
            try:
                filled = finite(result.get("filledQuantity", 0), "CLEANUP_CUMULATIVE_FILL")
                quantity = finite(record["order"]["quantity"], "CLEANUP_RESERVED_QUANTITY", positive=True)
                if filled < record["appliedFilledQuantity"] or filled > quantity + 1e-9:
                    return False
                if status == "FILLED" and abs(filled-quantity) > max(1e-10, quantity*1e-9):
                    return False
                if filled:
                    notional = filled * finite(result.get("fillPrice"), "CLEANUP_FILL_PRICE", positive=True)
                    if filled == record["appliedFilledQuantity"] and abs(notional-record["appliedFilledNotional"]) > 1e-8:
                        return False
                    if record["order"]["route"] == "BINANCE_USDM_FUTURES":
                        increment = filled - record["appliedFilledQuantity"]
                        if increment > 1e-10:
                            self._apply_fill_increment(client_order_id, result, filled, increment)
                    else:
                        record["appliedFilledQuantity"], record["appliedFilledNotional"] = filled, notional
                if result.get("feeKrw") is None or result.get("actualFeeKnown") is False:
                    session["feesKnown"] = False
                else:
                    finite(result.get("feeKrw"), "CLEANUP_ACTUAL_FEE")
                if record["order"]["route"] == "BINANCE_SPOT" and filled:
                    if result.get("baseCommissionQuantity") is None or result.get("baseCommissionKnown") is False:
                        return False
                    commission = finite(result["baseCommissionQuantity"], "CLEANUP_BASE_COMMISSION")
                    if commission < record["appliedBaseCommissionQuantity"] or commission > filled:
                        return False
                    record["appliedBaseCommissionQuantity"] = commission
            except ShortLiveContractError:
                return False
            record["status"] = status
            self._event("CLEANUP_ORDER_OBSERVED", clientOrderId=client_order_id, brokerOrderId=broker_id,
                        status=status, cumulativeQuantity=filled, actualFeeKnown=session["feesKnown"])
            if status == "UNKNOWN":
                self._failure("CLEANUP_ORDER_RESULT_UNKNOWN_NO_RETRY")
            return status != "UNKNOWN"

    def _failure(self, reason: str) -> None:
        if self._session is None:
            return
        if self._session["firstFailure"] is None:
            self._session["firstFailure"] = reason
        self._session["newEntriesBlocked"] = True
        if reason not in self._session["blockers"]:
            self._session["blockers"].append(reason)

    def stop(self, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            payload = payload or {}
            if not self._session:
                return self.snapshot(include_catalog=False)
            if payload.get("sessionId") not in {None, self._session["sessionId"]}:
                raise ShortLiveContractError("STOP_SCOPE_MISMATCH")
            self._stop_once(str(payload.get("reason") or "OPERATOR_STOP"))
            self._wake.set()
            return self.snapshot(include_catalog=False)

    def _stop_once(self, reason: str) -> None:
        session = self._session
        if not session or session["status"] in {"TERMINATED", "DRAINED"}:
            return
        session["newEntriesBlocked"] = True
        session["status"] = "STOPPING"
        session["stopReason"] = session["stopReason"] or reason
        if not session["cleanupAttempted"]:
            session["cleanupAttempted"] = True
            self._event("CLEANUP_INTENT_ONCE", sessionId=session["sessionId"], reason=reason)
            for route, adapter in self._brokers.items():
                try:
                    result = adapter.cleanup(self._route_session(route))
                    if not isinstance(result, Mapping) or result.get("complete") is not True:
                        self._failure(f"{route}:CLEANUP_NOT_CONFIRMED")
                except Exception as exc:
                    self._failure(f"{route}:CLEANUP_UNKNOWN:{type(exc).__name__}")
            closed = True
            for route, feed in self._feeds.items():
                try:
                    result = feed.close()
                    if result is False:
                        closed = False
                except Exception as exc:
                    closed = False
                    self._failure(f"{route}:FEED_CLOSE_UNKNOWN:{type(exc).__name__}")
            session["feedsClosed"] = closed
        self._reconcile_and_finish()

    def _reconcile_and_finish(self) -> None:
        session = self._session
        if not session:
            return
        if session.get("cleanupAttempted") and session["status"] == "BLOCKED":
            session["status"] = "STOPPING"
        for client_id, record in tuple(session["orders"].items()):
            if record["status"] in {"ACCEPTED", "PARTIAL", "UNKNOWN"}:
                observer = getattr(self._brokers[record["order"]["route"]], "observe_order", None)
                try:
                    result = observer(client_id) if callable(observer) else {}
                    observed = result.get("status") in {"FILLED", "CANCELED", "REJECTED", "ACCEPTED", "PARTIAL"}
                    if observed:
                        if record.get("cleanup"):
                            observed = self.observe_cleanup_order(client_id, result)
                        else:
                            self._handle_order_observation(client_id, result)
                except Exception:
                    observed = False
                if not observed:
                    self._failure("CLEANUP_EXACT_ORDER_OBSERVATION_UNVERIFIED")
        positive_reconciliation = self._reconcile(enforce_limits=False)
        rows = session["brokerRows"]
        if positive_reconciliation:
            # A previously lost reply may now be proven. Continue draining
            # that same session; adapters retain one-write intent identities.
            for route, row in list(rows.items()):
                if row.get("ownedOrders") or row.get("ownedPositions"):
                    drain = getattr(self._brokers[route], "continue_cleanup", None)
                    if callable(drain):
                        try:
                            drain(self._route_session(route))
                        except Exception:
                            self._failure(f"{route}:CLEANUP_CONTINUATION_UNVERIFIED")
            positive_reconciliation = self._reconcile(enforce_limits=False)
            rows = session["brokerRows"]
            for route, row in list(rows.items()):
                if ((row.get("ownedProtectionOrders") or
                        (route == "BINANCE_USDM_FUTURES" and row.get("protectionRetirementVerified") is not True))
                        and not row.get("ownedOrders") and not row.get("ownedPositions")):
                    retire = getattr(self._brokers[route], "retire_flat_protection", None)
                    try:
                        if not callable(retire) or retire(self._route_session(route)) is not True:
                            positive_reconciliation = False
                    except Exception:
                        positive_reconciliation = False
            if positive_reconciliation:
                positive_reconciliation = self._reconcile(enforce_limits=False)
                rows = session["brokerRows"]
        all_known = len(rows) == len(self._brokers) and all(row.get("ready") is True for row in rows.values())
        all_flat = all(not row.get("ownedOrders") and not row.get("ownedPositions") and not row.get("ownedProtectionOrders")
                       and row.get("exposureKrw") == 0
                       and (route != "BINANCE_USDM_FUTURES" or row.get("protectionRetirementVerified") is True)
                       for route, row in rows.items())
        cleanup_terminal = all(record["status"] in {"FILLED", "CANCELED", "REJECTED"}
                               for record in session["orders"].values())
        if not (positive_reconciliation and session["cleanupAttempted"] and session["feedsClosed"] and all_known and all_flat and cleanup_terminal):
            session["status"] = "BLOCKED"
            return
        if (self.background and session.get("workerLaunchState") == "START_FAILED_UNVERIFIED"
                and (self._thread is None or self._thread.ident is None)):
            self._failure("WORKER_START_OUTCOME_UNVERIFIED")
            session["status"] = "BLOCKED"
            return
        session["status"] = "DRAINED"
        if not self.background:
            session["naturalExitObserved"] = True
            self._finalize_after_worker_exit()
        elif (session.get("startupAbortedBeforeWorker") is True
                and session.get("workerRetirementRequired") is False
                and session.get("workerLaunchState") == "NOT_ATTEMPTED"
                and self._thread is None):
            self._event("STARTUP_RETIRED_BEFORE_WORKER", sessionId=session["sessionId"],
                        workerLaunchState="NOT_ATTEMPTED", naturalExitObserved=False)
            self._finalize_after_worker_exit()

    def _finalize_after_worker_exit(self) -> None:
        session = self._session
        if not session or session["status"] != "DRAINED":
            return
        never_started = (session.get("startupAbortedBeforeWorker") is True
                         and session.get("workerRetirementRequired") is False
                         and session.get("workerLaunchState") == "NOT_ATTEMPTED"
                         and self._thread is None)
        if not never_started and not session["naturalExitObserved"]:
            return
        if self.background and not never_started and (self._thread is None or self._thread.is_alive()):
            return
        session["terminalVerified"] = True
        session["status"] = "TERMINATED"
        session["terminatedAt"] = utc_text(self._now())
        if session["routeAuthorityActive"]:
            release_short_live_routes(session["sessionId"], session["scopeHash"])
            session["routeAuthorityActive"] = False
        self._write(f"session-terminal-{session['sessionId']}", {
            "schemaVersion": "short-live-terminal-v1", "purpose": PURPOSE,
            "promotionEligible": False, "sessionId": session["sessionId"],
            "manifestHash": session["scopeHash"], "terminatedAt": session["terminatedAt"],
            "normalCleanupConfirmed": True, "ownedOrdersRemaining": 0,
            "ownedPositionsRemaining": 0, "feedsClosed": True,
            "workerRetirementRequired": session["workerRetirementRequired"],
            "workerLaunchState": session["workerLaunchState"],
            "naturalExitObserved": session["naturalExitObserved"],
            "firstFailure": session["firstFailure"],
            "actualFeeKnown": session["feesKnown"],
            "feeReconciliationStatus": "KNOWN" if session["feesKnown"] else "UNVERIFIED_ACTUAL_FEE",
            "functionalPass": False,
            "note": "Retirement alone does not prove strategy order/fill coverage or profitability."})

    def close(self) -> dict[str, Any]:
        """Called by the genuine UI owner on shutdown; never kills a worker."""
        result = self.stop({"reason": "UI_OWNER_SHUTDOWN"})
        thread = self._thread
        if thread and thread is not threading.current_thread() and thread.ident is not None:
            thread.join(timeout=5)
        return self.snapshot(include_catalog=False)
