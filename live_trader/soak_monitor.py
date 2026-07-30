from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from trading_runtime import (
    ProcessResourceSampler,
    SoakCriteria,
    UnattendedSoakReporter,
    latest_soak_report,
)

TRANSIENT_RECOVERY_WINDOW_SECONDS = 120.0
_FATAL_RUNTIME_PHASES = {"FAILED", "CRASHED", "STALE"}
_REQUIRED_OBSERVATION_READERS = (
    "orders",
    "executionEvents",
    "programPositions",
    "auditEvents",
)
_TRANSIENT_ERROR_TYPES = {
    "connectionabortederror",
    "connectionerror",
    "connectionrefusederror",
    "connectionreseterror",
    "remotedisconnected",
    "sockettimeout",
    "timeouterror",
    "urlerror",
}
_TRANSIENT_ERROR_MARKERS = (
    "connection aborted",
    "connection closed",
    "connection refused",
    "connection reset",
    "connection timed out",
    "name resolution",
    "network is unreachable",
    "read operation timed out",
    "remote end closed",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "http error 502",
    "http error 503",
    "http error 504",
)
_NON_TRANSIENT_ERROR_MARKERS = (
    "api key",
    "authentication",
    "credential",
    "forbidden",
    "insufficient",
    "invalid",
    "malformed",
    "order adapter",
    "permission",
    "rejected",
    "schema",
    "signature",
    "unauthorized",
)


def configured_soak_duration_seconds() -> float:
    for key in (
        "LIVE_TRADER_SOAK_DURATION_SECONDS",
        "TRADING_SOAK_DURATION_SECONDS",
    ):
        try:
            value = float(os.environ.get(key) or 0)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return 18_000.0


def configured_transient_recovery_window_seconds() -> float:
    try:
        configured = float(
            os.environ.get("LIVE_TRADER_SOAK_TRANSIENT_RECOVERY_SECONDS")
            or TRANSIENT_RECOVERY_WINDOW_SECONDS
        )
    except (TypeError, ValueError):
        configured = TRANSIENT_RECOVERY_WINDOW_SECONDS
    return min(300.0, max(15.0, configured))


def soak_auto_stop_enabled() -> bool:
    return str(os.environ.get("LIVE_TRADER_SOAK_AUTO_STOP") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def should_auto_stop_soak(report: Mapping[str, Any]) -> bool:
    if not soak_auto_stop_enabled():
        return False
    try:
        duration = float(report.get("durationSeconds") or 0)
        target = float(report.get("targetDurationSeconds") or 0)
    except (TypeError, ValueError):
        return False
    return target > 0 and duration >= target


def latest_live_soak_report(log_dir: str | Path | None = None) -> dict[str, Any]:
    """Read the latest durable report without opening a broker connection."""

    if log_dir is None:
        from .env_loader import default_runtime_data_root

        logs = default_runtime_data_root() / "logs"
    else:
        logs = Path(log_dir)
    report = latest_soak_report(
        logs / "unattended_soak.sqlite3",
        app="live_trader",
        mode="MONITOR",
    )
    if not report:
        report_dir = logs / "reports" / "soak"
        candidates = sorted(
            report_dir.glob("*.json"),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        ) if report_dir.is_dir() else []
        for path in candidates:
            try:
                payload = __import__("json").loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if (
                isinstance(payload, dict)
                and str(payload.get("app") or "") == "live_trader"
                and str(payload.get("mode") or "").upper() == "MONITOR"
            ):
                report = payload
                break
    if report:
        return report
    return {
        "schemaVersion": "unattended-soak-report-v1",
        "runId": "",
        "app": "live_trader",
        "mode": "MONITOR",
        "status": "IDLE",
        "verdict": "IDLE",
        "startedAt": "",
        "endedAt": "",
        "durationSeconds": 0,
        "targetDurationSeconds": configured_soak_duration_seconds(),
        "progressPct": 0,
        "sampleCount": 0,
        "heartbeat": {
            "lastAt": "",
            "gapCount": 0,
            "maxGapSeconds": 0,
            "limitSeconds": 90,
        },
        "counts": {
            "barCount": 0,
            "decisionCount": 0,
            "orderCount": 0,
            "fillCount": 0,
            "blockCount": 0,
            "errorCount": 0,
            "reconnectCount": 0,
            "realOrderCount": 0,
        },
        "resources": {
            "peakProcessCpuPercent": None,
            "peakProcessMemoryBytes": None,
        },
        "dailyRisk": {
            "dailyPnl": None,
            "dailyPnlPct": None,
            "dailyPnlSource": "unavailable",
            "dailyLossLimitPct": None,
            "lossGateTripped": False,
            "sessionPnl": None,
            "sessionPnlPct": None,
        },
        "finalPositions": [],
        "criteria": [],
        "exportPath": "",
        "viewPath": "",
        "databasePath": str(logs / "unattended_soak.sqlite3"),
        "updatedAt": "",
    }


class LiveDaemonSoakSession:
    """Observe a Live Trader MONITOR daemon without exposing order methods."""

    def __init__(
        self,
        *,
        log_dir: str | Path,
        state_module: Any,
        profiles: tuple[str, ...],
        heartbeat_gap_limit_seconds: float,
        target_duration_seconds: float | None = None,
    ) -> None:
        self.log_dir = Path(log_dir)
        self.state = state_module
        self.profiles = tuple(profiles)
        self.resources = ProcessResourceSampler()
        self._has_sample = False
        self.reporter = _ClassifyingLiveSoakReporter(
            database_path=self.log_dir / "unattended_soak.sqlite3",
            report_dir=self.log_dir / "reports" / "soak",
            app="live_trader",
            mode="MONITOR",
            transient_recovery_window_seconds=(
                configured_transient_recovery_window_seconds()
            ),
            criteria=SoakCriteria(
                target_duration_seconds=(
                    target_duration_seconds
                    if target_duration_seconds is not None
                    else configured_soak_duration_seconds()
                ),
                heartbeat_gap_limit_seconds=max(
                    15.0, float(heartbeat_gap_limit_seconds)
                ),
                max_reconnects=max(
                    0,
                    int(os.environ.get("LIVE_TRADER_SOAK_MAX_RECONNECTS") or 10),
                ),
                max_errors=0,
                max_critical_blocks=0,
                minimum_samples=2,
                require_zero_real_orders=True,
            ),
            metadata={
                "profiles": list(self.profiles),
                "strategyScopes": (
                    self.state.live_soak_strategy_scopes(self.profiles)
                    if callable(
                        getattr(
                            self.state,
                            "live_soak_strategy_scopes",
                            None,
                        )
                    )
                    else []
                ),
                "orderSubmissionEnabled": False,
                "safetyContract": "MONITOR-only; reporter has no broker adapter",
                "verdictPolicy": "live-monitor-soak-v2",
            },
        )

    def sample(self, daemon_status: Mapping[str, Any]) -> dict[str, Any]:
        sample = collect_live_soak_sample(
            daemon_status,
            self.state,
            resources=self.resources.sample(),
        )
        if not self._has_sample:
            self._has_sample = True
            return self.reporter.start(sample)
        return self.reporter.record(sample)

    def finish(
        self,
        daemon_status: Mapping[str, Any],
        *,
        reason: str,
        failed: bool = False,
    ) -> dict[str, Any]:
        return self.reporter.finish(
            collect_live_soak_sample(
                daemon_status,
                self.state,
                resources=self.resources.sample(),
            ),
            reason=reason,
            failed=failed,
        )


def collect_live_soak_sample(
    daemon_status: Mapping[str, Any],
    state_module: Any,
    *,
    resources: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    runtime = (
        daemon_status.get("runtime")
        if isinstance(daemon_status.get("runtime"), Mapping)
        else {}
    )
    runtime_profiles = (
        runtime.get("profiles")
        if isinstance(runtime.get("profiles"), Mapping)
        else {}
    )
    bar_count = 0
    decision_count = 0
    reconnect_count = 0
    feed_error_count = 0
    last_data_at = ""
    last_decision_at = ""
    runtime_error_messages: set[str] = set()
    for profile in runtime_profiles.values():
        if not isinstance(profile, Mapping):
            continue
        engine = (
            profile.get("engine")
            if isinstance(profile.get("engine"), Mapping)
            else {}
        )
        bar_count += _int(engine.get("barCount"))
        decision_count += _int(engine.get("decisionCount"))
        reconnect_count += _int(profile.get("reconnectCount"))
        feed_errors = (
            profile.get("feedErrors")
            if isinstance(profile.get("feedErrors"), Mapping)
            else {}
        )
        feed_error_count += sum(1 for value in feed_errors.values() if str(value))
        if str(profile.get("lastError") or ""):
            runtime_error_messages.add(str(profile.get("lastError")))
        last_data_at = max(last_data_at, str(profile.get("lastDataAt") or ""))
        last_decision_at = max(
            last_decision_at, str(engine.get("lastCycleAt") or "")
        )
    streams = (
        daemon_status.get("executionStreams")
        if isinstance(daemon_status.get("executionStreams"), Mapping)
        else {}
    )
    stream_brokers = (
        streams.get("brokers")
        if isinstance(streams.get("brokers"), Mapping)
        else {}
    )
    for broker in stream_brokers.values():
        if not isinstance(broker, Mapping):
            continue
        reconnect_count += _int(broker.get("reconnectCount"))
        if str(broker.get("lastError") or ""):
            runtime_error_messages.add(str(broker.get("lastError")))

    orders, orders_observation = _read_rows_observation(
        "orders",
        getattr(state_module, "order_rows", None),
    )
    ledger = getattr(state_module, "PROGRAM_LEDGER", None)
    execution_events, execution_events_observation = _read_rows_observation(
        "executionEvents",
        getattr(ledger, "execution_event_rows", None),
        100_000,
    )
    positions, positions_observation = _read_rows_observation(
        "programPositions",
        getattr(ledger, "position_rows", None),
    )
    audit_store = getattr(state_module, "AUDIT_STORE", None)
    audit_events, audit_events_observation = _read_rows_observation(
        "auditEvents",
        getattr(audit_store, "list_events", None),
        limit=100_000,
        newest_first=False,
    )
    observation_availability = {
        "orders": orders_observation,
        "executionEvents": execution_events_observation,
        "programPositions": positions_observation,
        "auditEvents": audit_events_observation,
    }
    required_observations_available = all(
        observation_availability[reader].get("available") is True
        for reader in _REQUIRED_OBSERVATION_READERS
    )
    audit_error_count = sum(
        1
        for event in audit_events
        if str(event.get("level") or "").upper()
        in {"ERROR", "CRITICAL", "DANGER", "FAIL", "FAILED"}
    )
    audit_order_ids = {
        str(event.get("order_id") or "").strip()
        for event in audit_events
        if str(event.get("order_id") or "").strip()
    }
    filled_events = [
        event
        for event in execution_events
        if str(event.get("state") or "").strip().lower()
        in {"filled", "partially_filled", "partial_fill", "trade"}
    ]
    blocked_states = {
        "risk_blocked",
        "adapter_blocked",
        "blocked",
        "rejected",
        "failed",
    }
    blocked_order_count = sum(
        1
        for order in orders
        if str(order.get("state") or "").strip().lower() in blocked_states
    )
    real_order_count = sum(
        1
        for order in orders
        if order.get("dry_run") is not True
        and str(order.get("state") or "").strip().lower()
        in {
            "approved",
            "submitting",
            "submitted",
            "acknowledged",
            "partially_filled",
            "filled",
        }
    )
    broker_positions = (
        getattr(state_module, "STATE", {}).get("broker_reconciliation", {}).get(
            "positions", []
        )
        if isinstance(getattr(state_module, "STATE", {}), dict)
        else []
    )
    unrealized_values = [
        _number(row.get("unrealized_profit"))
        for row in broker_positions
        if isinstance(row, Mapping) and _number(row.get("unrealized_profit")) is not None
    ]
    risk_settings = (
        getattr(state_module, "STATE", {}).get("risk_settings", {})
        if isinstance(getattr(state_module, "STATE", {}), dict)
        else {}
    )
    daily_loss_limit_pct = _number(
        risk_settings.get("daily_loss_limit_pct")
    )
    account_risk_state = (
        getattr(state_module, "STATE", {}).get("account_risk", {})
        if isinstance(getattr(state_module, "STATE", {}), dict)
        else {}
    )
    account_risk = {}
    account_risk_reader = getattr(state_module, "broker_account_risk", None)
    if callable(account_risk_reader):
        try:
            account_risk = account_risk_reader(
                account_risk_state,
                "binance-futures",
                currency="USDT",
            )
        except (TypeError, ValueError):
            account_risk = {}
    account_daily_pnl = _number(account_risk.get("daily_pnl"))
    account_daily_pnl_pct = _number(account_risk.get("daily_pnl_pct"))
    account_equity = _number(account_risk.get("current_equity"))
    account_risk_available = account_risk.get("known") is True
    loss_gate_tripped = (
        account_daily_pnl_pct is not None
        and daily_loss_limit_pct is not None
        and account_daily_pnl_pct <= daily_loss_limit_pct
    )
    last_poll = (
        daemon_status.get("lastExecutionPoll")
        if isinstance(daemon_status.get("lastExecutionPoll"), Mapping)
        else {}
    )
    poll_error = 1 if last_poll.get("ok") is False else 0
    read_only_poll = _read_only_poll_observation(
        last_poll,
        stream_brokers=stream_brokers,
        state_module=state_module,
    )
    resource_values = dict(resources or {})
    return {
        "heartbeatAt": str(
            daemon_status.get("lastHeartbeat")
            or daemon_status.get("stoppedAt")
            or ""
        ),
        "phase": str(daemon_status.get("phase") or "RUNNING"),
        "barCount": bar_count,
        "decisionCount": decision_count,
        "orderCount": max(len(orders), len(audit_order_ids)),
        "fillCount": len(filled_events),
        "blockCount": blocked_order_count,
        "errorCount": (
            audit_error_count
            + feed_error_count
            + len(runtime_error_messages)
            + poll_error
        ),
        "reconnectCount": reconnect_count,
        "realOrderCount": real_order_count,
        "positions": positions,
        "equity": account_equity if account_risk_available else None,
        "dailyPnl": (
            account_daily_pnl
            if account_risk_available
            else sum(value for value in unrealized_values if value is not None)
            if unrealized_values
            else None
        ),
        "dailyPnlPct": (
            account_daily_pnl_pct if account_risk_available else None
        ),
        "dailyPnlSource": (
            "durable-account-risk-budget"
            if account_risk_available
            else "broker-unrealized-only"
            if unrealized_values
            else "unavailable"
        ),
        "dailyLossLimitPct": daily_loss_limit_pct,
        "lossGateTripped": loss_gate_tripped,
        "lastDataAt": last_data_at,
        "lastDecisionAt": last_decision_at,
        "lastError": " | ".join(sorted(runtime_error_messages))[:2000],
        "errorBreakdown": {
            "audit": audit_error_count,
            "feed": feed_error_count,
            "runtime": len(runtime_error_messages),
            "readOnlyPoll": poll_error,
        },
        "monitorIntegrity": {
            "orderCount": max(len(orders), len(audit_order_ids)),
            "fillCount": len(filled_events),
            "realOrderCount": real_order_count,
            "programPositionFingerprint": _position_fingerprint(positions),
            "brokerPositionFingerprint": _position_fingerprint(broker_positions),
            "lossGateTripped": loss_gate_tripped,
        },
        "observationAvailability": observation_availability,
        "requiredObservationsAvailable": required_observations_available,
        "readOnlyPoll": read_only_poll,
        **resource_values,
    }


class _ClassifyingLiveSoakReporter(UnattendedSoakReporter):
    """Preserve strict soak evidence while classifying one narrow warning case.

    Only a broker *read-only* poll connectivity failure may be downgraded.  It
    must explicitly recover inside the configured window and order/fill/
    position fingerprints must remain unchanged.  Every other failure remains
    blocking, including an unresolved incident or any unsafe runtime phase
    unrelated to that poll.
    """

    def __init__(
        self,
        *,
        transient_recovery_window_seconds: float,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.transient_recovery_window_seconds = min(
            300.0,
            max(15.0, float(transient_recovery_window_seconds)),
        )
        self._active_transient: dict[str, Any] | None = None
        self._incidents: list[dict[str, Any]] = []
        self._hard_runtime_phases: set[str] = set()
        self._unclassified_poll_failures: list[dict[str, Any]] = []
        self._required_observation_failures: dict[str, dict[str, Any]] = {}
        self._latest_required_observations: dict[str, dict[str, Any]] = {}
        self._error_baseline: dict[str, int] | None = None
        self._error_high_water: dict[str, int] = {}

    def record(self, sample: Mapping[str, Any]) -> dict[str, Any]:
        self._observe_live_sample(sample)
        return super().record(sample)

    def report(
        self,
        *,
        status: str | None = None,
        ended_at: datetime | None = None,
        stop_reason: str = "",
    ) -> dict[str, Any]:
        report = super().report(
            status=status,
            ended_at=ended_at,
            stop_reason=stop_reason,
        )
        return _classify_live_soak_report(
            report,
            incidents=self._incidents,
            active_incident=self._active_transient,
            hard_runtime_phases=self._hard_runtime_phases,
            unclassified_poll_failures=self._unclassified_poll_failures,
            required_observation_failures=(
                self._required_observation_failures
            ),
            latest_required_observations=(
                self._latest_required_observations
            ),
            hard_error_delta=self._hard_error_delta(),
            transient_recovery_window_seconds=(
                self.transient_recovery_window_seconds
            ),
        )

    def _observe_live_sample(self, sample: Mapping[str, Any]) -> None:
        self._observe_required_observations(sample)
        breakdown = (
            sample.get("errorBreakdown")
            if isinstance(sample.get("errorBreakdown"), Mapping)
            else {}
        )
        normalized_breakdown = {
            key: _int(breakdown.get(key))
            for key in ("audit", "feed", "runtime", "readOnlyPoll")
        }
        if self._error_baseline is None:
            self._error_baseline = dict(normalized_breakdown)
        for key, value in normalized_breakdown.items():
            self._error_high_water[key] = max(
                self._error_high_water.get(key, value),
                value,
            )

        phase = str(sample.get("phase") or "RUNNING").strip().upper()
        poll = (
            sample.get("readOnlyPoll")
            if isinstance(sample.get("readOnlyPoll"), Mapping)
            else {}
        )
        poll_failed = poll.get("observed") is True and poll.get("ok") is False
        transient_poll = poll_failed and poll.get("transientConnectivity") is True
        if phase in _FATAL_RUNTIME_PHASES:
            self._hard_runtime_phases.add(phase)
        elif phase == "DEGRADED" and not transient_poll:
            self._hard_runtime_phases.add(phase)

        if poll_failed:
            if transient_poll:
                self._start_or_extend_transient(sample, poll)
            else:
                self._unclassified_poll_failures.append(
                    {
                        "observedAt": _sample_time_text(sample),
                        "reason": str(poll.get("reason") or "unclassified poll failure")[
                            :500
                        ],
                        "brokers": list(poll.get("brokers") or []),
                    }
                )
        if self._active_transient is not None:
            self._observe_integrity(sample)
            if poll.get("observed") is True and poll.get("ok") is True:
                self._recover_transient(sample, poll)

    def _observe_required_observations(
        self,
        sample: Mapping[str, Any],
    ) -> None:
        observations = (
            sample.get("observationAvailability")
            if isinstance(sample.get("observationAvailability"), Mapping)
            else {}
        )
        observed_at = _sample_time_text(sample)
        for reader in _REQUIRED_OBSERVATION_READERS:
            observation = (
                observations.get(reader)
                if isinstance(observations.get(reader), Mapping)
                else {}
            )
            self._latest_required_observations[reader] = {
                "reader": reader,
                "available": observation.get("available") is True,
                "status": (
                    "AVAILABLE"
                    if observation.get("available") is True
                    else "UNAVAILABLE"
                ),
                "rowCount": (
                    _int(observation.get("rowCount"))
                    if observation.get("available") is True
                    else None
                ),
                "errorCode": str(
                    observation.get("errorCode") or ""
                )[:80],
                "errorType": str(
                    observation.get("errorType") or ""
                )[:80],
            }
            if observation.get("available") is True:
                continue
            current = self._required_observation_failures.get(reader)
            if current is None:
                current = {
                    "reader": reader,
                    "status": "UNAVAILABLE",
                    "errorCode": str(
                        observation.get("errorCode")
                        or "availability-evidence-missing"
                    )[:80],
                    "errorType": str(
                        observation.get("errorType")
                        or "ObservationUnavailable"
                    )[:80],
                    "firstObservedAt": observed_at,
                    "lastObservedAt": observed_at,
                    "occurrences": 0,
                }
                self._required_observation_failures[reader] = current
            current["lastObservedAt"] = observed_at
            current["occurrences"] = _int(current.get("occurrences")) + 1

    def _start_or_extend_transient(
        self,
        sample: Mapping[str, Any],
        poll: Mapping[str, Any],
    ) -> None:
        if self._active_transient is None:
            observed = _sample_datetime(sample)
            self._active_transient = {
                "type": "READ_ONLY_BROKER_CONNECTIVITY",
                "severity": "WARNING",
                "status": "ACTIVE",
                "startedAt": _sample_time_text(sample),
                "_startedEpoch": observed.timestamp(),
                "recoveredAt": "",
                "durationSeconds": None,
                "brokers": sorted(
                    {
                        str(item).strip().lower()
                        for item in poll.get("brokers", [])
                        if str(item).strip()
                    }
                ),
                "details": [
                    str(item)[:500]
                    for item in poll.get("details", [])
                    if str(item).strip()
                ][:10],
                "startIntegrity": _integrity_snapshot(sample),
                "integrityIssues": [],
                "recoveryEvidence": "",
            }
            return
        active = self._active_transient
        active["brokers"] = sorted(
            set(active.get("brokers") or ())
            | {
                str(item).strip().lower()
                for item in poll.get("brokers", [])
                if str(item).strip()
            }
        )
        active["details"] = list(
            dict.fromkeys(
                [
                    *list(active.get("details") or ()),
                    *[
                        str(item)[:500]
                        for item in poll.get("details", [])
                        if str(item).strip()
                    ],
                ]
            )
        )[:10]

    def _observe_integrity(self, sample: Mapping[str, Any]) -> None:
        active = self._active_transient
        if active is None:
            return
        started = active.get("startIntegrity")
        current = _integrity_snapshot(sample)
        if not isinstance(started, Mapping):
            active["integrityIssues"] = ["missing-start-integrity-snapshot"]
            return
        issues = set(active.get("integrityIssues") or ())
        if current["realOrderCount"] > 0:
            issues.add("real-order-observed-in-monitor")
        if current["orderCount"] != _int(started.get("orderCount")):
            issues.add("order-ledger-changed-during-disconnect")
        if current["fillCount"] != _int(started.get("fillCount")):
            issues.add("fill-ledger-changed-during-disconnect")
        if (
            current["programPositionFingerprint"]
            != str(started.get("programPositionFingerprint") or "")
        ):
            issues.add("program-position-changed-during-disconnect")
        if (
            current["brokerPositionFingerprint"]
            != str(started.get("brokerPositionFingerprint") or "")
        ):
            issues.add("broker-position-changed-during-disconnect")
        if current["lossGateTripped"]:
            issues.add("daily-loss-gate-tripped")
        active["integrityIssues"] = sorted(issues)

    def _recover_transient(
        self,
        sample: Mapping[str, Any],
        poll: Mapping[str, Any],
    ) -> None:
        active = self._active_transient
        if active is None:
            return
        broker_connectivity = (
            poll.get("brokerConnectivity")
            if isinstance(poll.get("brokerConnectivity"), Mapping)
            else {}
        )
        brokers = list(active.get("brokers") or ())
        unhealthy = [
            broker
            for broker in brokers
            if str(broker_connectivity.get(broker) or "").lower()
            not in {"", "healthy"}
        ]
        if unhealthy:
            return
        recovered = _sample_datetime(sample)
        duration = max(
            0.0,
            recovered.timestamp() - float(active.get("_startedEpoch") or 0.0),
        )
        active["recoveredAt"] = _sample_time_text(sample)
        active["durationSeconds"] = round(duration, 3)
        active["recoveryEvidence"] = (
            "aggregate-read-only-poll-and-broker-connectivity-healthy"
            if broker_connectivity
            else "aggregate-read-only-poll-healthy"
        )
        issues = list(active.get("integrityIssues") or ())
        if duration > self.transient_recovery_window_seconds:
            issues.append("recovery-window-exceeded")
        active["integrityIssues"] = sorted(set(issues))
        active["status"] = "RECOVERED" if not issues else "RECOVERED_UNSAFE"
        active["severity"] = "WARNING" if not issues else "ERROR"
        active.pop("_startedEpoch", None)
        self._incidents.append(active)
        self._active_transient = None

    def _hard_error_delta(self) -> int:
        baseline = self._error_baseline or {}
        return sum(
            max(0, self._error_high_water.get(key, 0) - baseline.get(key, 0))
            for key in ("audit", "feed", "runtime")
        )


def _classify_live_soak_report(
    report: Mapping[str, Any],
    *,
    incidents: list[dict[str, Any]],
    active_incident: Mapping[str, Any] | None,
    hard_runtime_phases: set[str],
    unclassified_poll_failures: list[dict[str, Any]],
    required_observation_failures: Mapping[str, Mapping[str, Any]],
    latest_required_observations: Mapping[str, Mapping[str, Any]],
    hard_error_delta: int,
    transient_recovery_window_seconds: float,
) -> dict[str, Any]:
    classified = dict(report)
    incident_rows = [_public_incident(row) for row in incidents]
    if active_incident is not None:
        unresolved = _public_incident(active_incident)
        unresolved["status"] = "UNRECOVERED"
        unresolved["severity"] = "ERROR"
        incident_rows.append(unresolved)
    classified["incidents"] = incident_rows

    criteria = [
        dict(row)
        for row in report.get("criteria", [])
        if isinstance(row, Mapping)
    ]
    failed_ids = {
        str(row.get("id") or "")
        for row in criteria
        if row.get("blocking") is True and row.get("passed") is False
    }
    normalized_status = str(report.get("status") or "").upper()
    unsafe_incidents = [
        row for row in incident_rows if str(row.get("status") or "") != "RECOVERED"
    ]
    observation_failures = [
        {
            "reader": str(row.get("reader") or reader),
            "status": "UNAVAILABLE",
            "errorCode": str(
                row.get("errorCode") or "reader-unavailable"
            )[:80],
            "errorType": str(
                row.get("errorType") or "ObservationUnavailable"
            )[:80],
            "firstObservedAt": str(row.get("firstObservedAt") or ""),
            "lastObservedAt": str(row.get("lastObservedAt") or ""),
            "occurrences": _int(row.get("occurrences")),
        }
        for reader, row in sorted(required_observation_failures.items())
    ]
    pending_criteria = (
        {"duration", "samples"} if normalized_status == "RUNNING" else set()
    )
    non_downgradable_failures = (
        failed_ids - {"errors", "runtime-phase"} - pending_criteria
    )
    fatal_reasons: list[str] = []
    if normalized_status in {"FAILED", "ABORTED"}:
        fatal_reasons.append(f"terminal-status:{normalized_status}")
    fatal_reasons.extend(
        f"criterion:{criterion_id}"
        for criterion_id in sorted(non_downgradable_failures)
    )
    fatal_reasons.extend(
        f"runtime-phase:{phase}" for phase in sorted(hard_runtime_phases)
    )
    if hard_error_delta:
        fatal_reasons.append(f"hard-error-count:{hard_error_delta}")
    if unclassified_poll_failures:
        fatal_reasons.append(
            f"unclassified-read-only-poll-failure:{len(unclassified_poll_failures)}"
        )
    if unsafe_incidents:
        fatal_reasons.append(f"unsafe-or-unrecovered-incident:{len(unsafe_incidents)}")
    fatal_reasons.extend(
        f"required-observation-unavailable:{row['reader']}"
        for row in observation_failures
    )

    recovered_warnings = [
        row for row in incident_rows if str(row.get("status") or "") == "RECOVERED"
    ]
    if not recovered_warnings:
        fatal_reasons.extend(
            f"criterion:{criterion_id}"
            for criterion_id in sorted(
                failed_ids & {"errors", "runtime-phase"}
            )
        )
    may_downgrade = bool(recovered_warnings) and not fatal_reasons
    if may_downgrade:
        for row in criteria:
            if str(row.get("id") or "") == "errors":
                row.update(
                    {
                        "label": "런타임 오류 분류",
                        "passed": True,
                        "blocking": True,
                        "observed": {
                            "total": (
                                report.get("counts", {}).get("errorCount", 0)
                                if isinstance(report.get("counts"), Mapping)
                                else 0
                            ),
                            "hard": hard_error_delta,
                            "recoveredConnectivityIncidents": len(
                                recovered_warnings
                            ),
                        },
                        "expected": "0 hard/unclassified errors",
                    }
                )
            elif str(row.get("id") or "") == "runtime-phase":
                row.update(
                    {
                        "label": "런타임 건강 상태·복구",
                        "passed": True,
                        "blocking": True,
                        "observed": (
                            f"DEGRADED {len(recovered_warnings)}회 · "
                            "bounded auto-recovery"
                        ),
                        "expected": (
                            "no unrecovered DEGRADED/FAILED/CRASHED/STALE"
                        ),
                    }
                )
    integrity_issues = sorted(
        {
            str(issue)
            for incident in incident_rows
            for issue in incident.get("integrityIssues", [])
            if str(issue) and str(issue) != "recovery-window-exceeded"
        }
    )
    criteria.append(
        {
            "id": "execution-integrity",
            "label": "MONITOR 주문·포지션 무결성",
            "passed": not integrity_issues,
            "blocking": True,
            "observed": integrity_issues or "UNCHANGED",
            "expected": "no order/fill/position ambiguity and zero real orders",
        }
    )
    criteria.append(
        {
            "id": "required-observations",
            "label": "필수 주문·원장·포지션·감사 관측",
            "passed": not observation_failures,
            "blocking": True,
            "observed": observation_failures or "ALL AVAILABLE",
            "expected": "all required readers available on every sample",
        }
    )
    if recovered_warnings:
        criteria.append(
            {
                "id": "recovered-connectivity",
                "label": "복구된 읽기 전용 연결 장애",
                "passed": True,
                "blocking": False,
                "observed": [
                    {
                        "brokers": row.get("brokers", []),
                        "durationSeconds": row.get("durationSeconds"),
                        "status": row.get("status"),
                    }
                    for row in recovered_warnings
                ],
                "expected": (
                    f"auto-recovered <= {transient_recovery_window_seconds:.0f}s"
                ),
            }
        )
    classified["criteria"] = criteria
    classified["requiredObservations"] = {
        "available": not observation_failures,
        "requiredReaders": list(_REQUIRED_OBSERVATION_READERS),
        "latest": [
            dict(latest_required_observations.get(reader) or {
                "reader": reader,
                "available": False,
                "status": "UNAVAILABLE",
                "rowCount": None,
                "errorCode": "availability-evidence-missing",
                "errorType": "ObservationUnavailable",
            })
            for reader in _REQUIRED_OBSERVATION_READERS
        ],
        "failures": observation_failures,
    }
    classified["classification"] = {
        "policy": "live-monitor-soak-v2",
        "transientRecoveryWindowSeconds": transient_recovery_window_seconds,
        "hardErrorCount": hard_error_delta,
        "unclassifiedPollFailureCount": len(unclassified_poll_failures),
        "hardRuntimePhases": sorted(hard_runtime_phases),
        "unclassifiedPollFailures": [
            dict(row) for row in unclassified_poll_failures
        ],
        "requiredObservationFailureCount": len(observation_failures),
        "fatalReasons": fatal_reasons,
    }

    if normalized_status == "RUNNING":
        classified["verdict"] = "RUNNING"
    elif fatal_reasons:
        classified["verdict"] = "FAIL"
    elif recovered_warnings:
        classified["verdict"] = "PASS_WITH_WARNING"
    return classified


def _read_only_poll_observation(
    last_poll: Mapping[str, Any],
    *,
    stream_brokers: Mapping[str, Any],
    state_module: Any,
) -> dict[str, Any]:
    observed = bool(last_poll)
    ok = last_poll.get("ok") is not False if observed else None
    raw_errors = (
        last_poll.get("errors")
        if isinstance(last_poll.get("errors"), list)
        else []
    )
    errors = [
        {
            "brokerId": str(item.get("brokerId") or item.get("broker_id") or "")[
                :50
            ].strip().lower(),
            "detail": str(item.get("detail") or "")[:500],
            "errorType": str(item.get("errorType") or "")[:100],
        }
        for item in raw_errors[:10]
        if isinstance(item, Mapping)
    ]
    if observed and ok is False and not errors:
        errors = [
            {
                "brokerId": "",
                "detail": str(last_poll.get("reason") or "")[:500],
                "errorType": str(last_poll.get("errorType") or "")[:100],
            }
        ]
    broker_ids = sorted(
        {
            str(item.get("brokerId") or "").strip().lower()
            for item in errors
            if str(item.get("brokerId") or "").strip()
        }
    )
    has_unknown_broker = any(not str(item.get("brokerId") or "") for item in errors)
    transient_errors = bool(errors) and all(
        _is_transient_connectivity_error(
            str(item.get("detail") or ""),
            str(item.get("errorType") or ""),
        )
        for item in errors
    )
    execution_stream_ambiguous = any(
        _stream_has_ambiguity(stream_brokers.get(broker_id))
        for broker_id in broker_ids
    )
    state = getattr(state_module, "STATE", {})
    poll_control = (
        state.get("broker_snapshot_poll", {})
        if isinstance(state, Mapping)
        else {}
    )
    connectivity = (
        poll_control.get("connectivity", {})
        if isinstance(poll_control, Mapping)
        and isinstance(poll_control.get("connectivity"), Mapping)
        else {}
    )
    broker_connectivity = {
        str(item.get("broker_id") or key.split(":", 1)[-1]).strip().lower(): str(
            item.get("status") or ""
        ).strip().lower()
        for key, item in connectivity.items()
        if str(key).startswith("broker_api:") and isinstance(item, Mapping)
    }
    return {
        "observed": observed,
        "ok": ok,
        "reason": str(last_poll.get("reason") or "")[:500],
        "brokers": broker_ids,
        "details": [str(item.get("detail") or "")[:500] for item in errors],
        "brokerConnectivity": broker_connectivity,
        "executionStreamAmbiguous": execution_stream_ambiguous,
        "transientConnectivity": bool(
            observed
            and ok is False
            and transient_errors
            and broker_ids
            and not has_unknown_broker
            and not execution_stream_ambiguous
        ),
    }


def _is_transient_connectivity_error(detail: str, error_type: str = "") -> bool:
    normalized = " ".join(f"{error_type} {detail}".lower().split())
    if any(marker in normalized for marker in _NON_TRANSIENT_ERROR_MARKERS):
        return False
    normalized_type = str(error_type or "").strip().lower()
    return normalized_type in _TRANSIENT_ERROR_TYPES or any(
        marker in normalized for marker in _TRANSIENT_ERROR_MARKERS
    )


def _stream_has_ambiguity(value: object) -> bool:
    if not isinstance(value, Mapping) or value.get("running") is not True:
        return False
    return bool(
        str(value.get("lastError") or "").strip()
        or value.get("connected") is False
    )


def _integrity_snapshot(sample: Mapping[str, Any]) -> dict[str, Any]:
    integrity = (
        sample.get("monitorIntegrity")
        if isinstance(sample.get("monitorIntegrity"), Mapping)
        else {}
    )
    return {
        "orderCount": _int(integrity.get("orderCount", sample.get("orderCount"))),
        "fillCount": _int(integrity.get("fillCount", sample.get("fillCount"))),
        "realOrderCount": _int(
            integrity.get("realOrderCount", sample.get("realOrderCount"))
        ),
        "programPositionFingerprint": str(
            integrity.get("programPositionFingerprint") or ""
        ),
        "brokerPositionFingerprint": str(
            integrity.get("brokerPositionFingerprint") or ""
        ),
        "lossGateTripped": bool(
            integrity.get("lossGateTripped", sample.get("lossGateTripped"))
        ),
    }


def _position_fingerprint(rows: object) -> str:
    normalized = sorted(
        [
            {
                "broker": str(row.get("broker_id") or ""),
                "symbol": str(row.get("symbol") or ""),
                "side": str(row.get("position_side") or ""),
                "quantity": _number(row.get("quantity")),
            }
            for row in rows
            if isinstance(row, Mapping)
        ],
        key=lambda row: (
            row["broker"],
            row["symbol"],
            row["side"],
            str(row["quantity"]),
        ),
    ) if isinstance(rows, (list, tuple)) else []
    return hashlib.sha256(
        json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _sample_datetime(sample: Mapping[str, Any]) -> datetime:
    text = str(
        sample.get("observedAt")
        or sample.get("heartbeatAt")
        or sample.get("stoppedAt")
        or ""
    ).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _sample_time_text(sample: Mapping[str, Any]) -> str:
    return (
        _sample_datetime(sample)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _public_incident(incident: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in incident.items()
        if not str(key).startswith("_") and str(key) != "startIntegrity"
    }


def _read_rows_observation(
    reader: str,
    callback: Any,
    *args: Any,
    **kwargs: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not callable(callback):
        return [], {
            "reader": reader,
            "available": False,
            "status": "UNAVAILABLE",
            "rowCount": None,
            "errorCode": "reader-missing",
            "errorType": "ReaderUnavailable",
        }
    try:
        rows = callback(*args, **kwargs)
        if (
            rows is None
            or isinstance(rows, (str, bytes, bytearray, Mapping))
        ):
            raise TypeError("required reader returned an invalid collection")
        materialized = list(rows)
        if any(not isinstance(row, Mapping) for row in materialized):
            raise TypeError("required reader returned an invalid row")
        normalized = [dict(row) for row in materialized]
    except Exception as exc:
        return [], {
            "reader": reader,
            "available": False,
            "status": "UNAVAILABLE",
            "rowCount": None,
            "errorCode": "reader-error",
            # Exception messages can include URLs, paths, SQL, or credentials.
            # The type alone is enough for diagnostics without leaking them.
            "errorType": type(exc).__name__[:80],
        }
    return normalized, {
        "reader": reader,
        "available": True,
        "status": "AVAILABLE",
        "rowCount": len(normalized),
        "errorCode": "",
        "errorType": "",
    }


def _int(value: object) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _number(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None
