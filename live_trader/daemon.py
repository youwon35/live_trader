from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import signal
import threading
import time
from typing import Callable, Iterable

from .env_loader import default_runtime_data_root


DEFAULT_HEARTBEAT_LEASE_SECONDS = 90.0


def run_daemon(profiles: Iterable[str], mode: str = "MONITOR", poll_seconds: float = 30.0) -> int:
    """Run market and private execution monitors without a desktop window."""
    from . import state

    selected = tuple(dict.fromkeys("stock" if item.strip().lower() == "stock" else "crypto" for item in profiles))
    stop = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)

    status_path = default_runtime_data_root() / "logs" / "daemon_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    heartbeat_lease_seconds = max(
        15.0,
        float(poll_seconds) * 3,
    )
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_status(status_path, {
        "schemaVersion": "live-trader-daemon-v2",
        "phase": "STARTING",
        "running": True,
        "pid": os.getpid(),
        "startedAt": started_at,
        "lastHeartbeat": started_at,
        "heartbeatLeaseSeconds": heartbeat_lease_seconds,
        "mode": mode,
        "profiles": list(selected),
    })
    stream_result = state.start_execution_streams("all")
    runtime_results = {profile: state.start_continuous_runtime(profile, mode) for profile in selected}

    def status_payload(
        *,
        last_execution_poll: dict[str, object] | None = None,
    ) -> dict[str, object]:
        runtime_ok = {
            key: value.get("ok") is True
            for key, value in runtime_results.items()
        }
        all_started = stream_result.get("ok") is True and all(runtime_ok.values())
        return {
            "schemaVersion": "live-trader-daemon-v2",
            "phase": "RUNNING" if all_started else "DEGRADED",
            "running": True,
            "pid": os.getpid(),
            "startedAt": started_at,
            "lastHeartbeat": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "heartbeatLeaseSeconds": heartbeat_lease_seconds,
            "mode": mode,
            "profiles": list(selected),
            "runtime": state.LIVE_CONTINUOUS_CONTROLLER.snapshot(),
            "executionStreams": state.LIVE_EXECUTION_STREAMS.snapshot(),
            "lastExecutionPoll": last_execution_poll or {},
            "startup": {
                "streamsOk": stream_result.get("ok") is True,
                "runtimes": runtime_ok,
            },
        }

    _write_status(status_path, status_payload())
    try:
        while not stop.wait(max(5.0, float(poll_seconds))):
            poll_result = state.poll_execution_events("all")
            _write_status(
                status_path,
                status_payload(
                    last_execution_poll={
                        "ok": poll_result.get("ok"),
                        "reason": poll_result.get("reason"),
                    }
                ),
            )
    finally:
        state.stop_continuous_runtime("")
        state.stop_execution_streams()
        _write_status(status_path, {
            "schemaVersion": "live-trader-daemon-v2",
            "phase": "STOPPED",
            "running": False,
            "pid": os.getpid(),
            "startedAt": started_at,
            "stoppedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "heartbeatLeaseSeconds": heartbeat_lease_seconds,
            "mode": mode,
            "profiles": list(selected),
        })
    return 0


def read_daemon_status(
    path: Path | None = None,
    *,
    now: datetime | None = None,
    process_checker: Callable[[int], bool] | None = None,
    persist: bool = False,
) -> dict[str, object]:
    """Return the effective daemon state, never trusting a stale RUNNING file."""

    target = path or default_runtime_data_root() / "logs" / "daemon_status.json"
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "schemaVersion": "live-trader-daemon-effective-v1",
            "phase": "STOPPED",
            "running": False,
            "exists": target.exists(),
            "statusPath": str(target),
        }
    if not isinstance(payload, dict):
        payload = {}
    recorded_running = payload.get("running") is True
    if not recorded_running:
        return {
            **payload,
            "phase": str(payload.get("phase") or "STOPPED").upper(),
            "running": False,
            "exists": True,
            "statusPath": str(target),
        }

    heartbeat_text = str(payload.get("lastHeartbeat") or "").strip()
    try:
        heartbeat = datetime.fromisoformat(heartbeat_text)
        current = now or datetime.now(heartbeat.tzinfo)
        if heartbeat.tzinfo is None and current.tzinfo is not None:
            current = current.replace(tzinfo=None)
        elif heartbeat.tzinfo is not None and current.tzinfo is None:
            current = current.astimezone(heartbeat.tzinfo)
        heartbeat_age = max(0.0, (current - heartbeat).total_seconds())
    except (TypeError, ValueError):
        current = now or datetime.now()
        heartbeat_age = float("inf")
    try:
        lease_seconds = max(
            15.0,
            float(
                payload.get("heartbeatLeaseSeconds")
                or DEFAULT_HEARTBEAT_LEASE_SECONDS
            ),
        )
    except (TypeError, ValueError):
        lease_seconds = DEFAULT_HEARTBEAT_LEASE_SECONDS
    try:
        pid = int(payload.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    checker = process_checker or process_is_alive
    process_alive = checker(pid) if pid > 0 else False
    stale_reasons: list[str] = []
    if heartbeat_age > lease_seconds:
        stale_reasons.append("heartbeat-timeout")
    if not process_alive:
        stale_reasons.append("process-not-running")
    if not stale_reasons:
        return {
            **payload,
            "phase": str(payload.get("phase") or "RUNNING").upper(),
            "running": True,
            "exists": True,
            "statusPath": str(target),
            "heartbeatAgeSeconds": round(heartbeat_age, 3),
            "processAlive": True,
        }

    effective = {
        **payload,
        "phase": "STALE",
        "running": False,
        "exists": True,
        "statusPath": str(target),
        "recordedPhase": str(payload.get("phase") or ""),
        "recordedRunning": True,
        "processAlive": process_alive,
        "heartbeatAgeSeconds": (
            None if heartbeat_age == float("inf") else round(heartbeat_age, 3)
        ),
        "staleAt": current.isoformat(),
        "staleReasons": stale_reasons,
    }
    if persist:
        _write_status(target, effective)
    return effective


def process_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _write_status(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
