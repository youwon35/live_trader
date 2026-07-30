from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from trading_runtime import (
    ProcessResourceSampler,
    SoakCriteria,
    UnattendedSoakReporter,
    latest_soak_report,
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
        self.reporter = UnattendedSoakReporter(
            database_path=self.log_dir / "unattended_soak.sqlite3",
            report_dir=self.log_dir / "reports" / "soak",
            app="live_trader",
            mode="MONITOR",
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
                "orderSubmissionEnabled": False,
                "safetyContract": "MONITOR-only; reporter has no broker adapter",
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

    orders = _safe_rows(getattr(state_module, "order_rows", None))
    ledger = getattr(state_module, "PROGRAM_LEDGER", None)
    execution_events = (
        _safe_rows(getattr(ledger, "execution_event_rows", None), 100_000)
        if ledger is not None
        else []
    )
    positions = (
        _safe_rows(getattr(ledger, "position_rows", None))
        if ledger is not None
        else []
    )
    audit_store = getattr(state_module, "AUDIT_STORE", None)
    audit_events = (
        _safe_rows(
            getattr(audit_store, "list_events", None),
            limit=100_000,
            newest_first=False,
        )
        if audit_store is not None
        else []
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
        **resource_values,
    }


def _safe_rows(callback: Any, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    if not callable(callback):
        return []
    try:
        rows = callback(*args, **kwargs)
    except Exception:
        return []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


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
