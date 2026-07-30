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
    """Run the daemon without ever leaking an operational exception to PyInstaller."""

    requested_profiles = tuple(profiles)
    try:
        return _run_daemon(requested_profiles, mode, poll_seconds)
    except Exception as exc:  # PyInstaller process boundary; task scheduler handles the non-zero exit.
        try:
            status_path = default_runtime_data_root() / "logs" / "daemon_status.json"
            _write_status(status_path, {
                "schemaVersion": "live-trader-daemon-v2",
                "phase": "FAILED",
                "running": False,
                "pid": os.getpid(),
                "failedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "mode": mode,
                "profiles": list(requested_profiles),
                "fatalError": _exception_detail(exc),
            })
        except Exception:
            pass
        return 1


def _run_daemon(profiles: Iterable[str], mode: str, poll_seconds: float) -> int:
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
    stream_result: dict[str, object] = {
        "ok": False,
        "reason": "execution stream startup not attempted",
    }
    runtime_results: dict[str, dict[str, object]] = {
        profile: {
            "ok": False,
            "reason": "continuous runtime startup not attempted",
        }
        for profile in selected
    }
    startup_attempts = {
        "streams": 0,
        "runtimes": {profile: 0 for profile in selected},
    }
    soak_session = None
    soak_finish_reason = "daemon-stop"
    if str(mode).strip().upper() == "MONITOR":
        try:
            from .soak_monitor import LiveDaemonSoakSession

            soak_session = LiveDaemonSoakSession(
                log_dir=status_path.parent,
                state_module=state,
                profiles=selected,
                heartbeat_gap_limit_seconds=heartbeat_lease_seconds,
            )
        except Exception:
            soak_session = None

    def status_payload(
        *,
        last_execution_poll: dict[str, object] | None = None,
    ) -> dict[str, object]:
        runtime_ok = {
            key: value.get("ok") is True
            for key, value in runtime_results.items()
        }
        all_started = stream_result.get("ok") is True and all(runtime_ok.values())
        poll_ok = not last_execution_poll or last_execution_poll.get("ok") is not False
        return {
            "schemaVersion": "live-trader-daemon-v2",
            "phase": "RUNNING" if all_started and poll_ok else "DEGRADED",
            "running": True,
            "pid": os.getpid(),
            "startedAt": started_at,
            "lastHeartbeat": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "heartbeatLeaseSeconds": heartbeat_lease_seconds,
            "mode": mode,
            "profiles": list(selected),
            "runtime": _safe_dict_call(
                "continuous runtime snapshot",
                state.LIVE_CONTINUOUS_CONTROLLER.snapshot,
            ),
            "executionStreams": _safe_dict_call(
                "execution stream snapshot",
                state.LIVE_EXECUTION_STREAMS.snapshot,
            ),
            "lastExecutionPoll": last_execution_poll or {},
            "startup": {
                "streamsOk": stream_result.get("ok") is True,
                "runtimes": runtime_ok,
                "attempts": startup_attempts,
                "details": {
                    "streams": _result_summary(stream_result),
                    "runtimes": {
                        profile: _result_summary(result)
                        for profile, result in runtime_results.items()
                    },
                },
            },
        }

    try:
        startup_attempts["streams"] = 1
        stream_result = _safe_dict_call(
            "execution stream startup",
            lambda: state.start_execution_streams("all"),
        )
        for profile in selected:
            startup_attempts["runtimes"][profile] = 1
            runtime_results[profile] = _safe_dict_call(
                f"{profile} continuous runtime startup",
                lambda profile=profile: state.start_continuous_runtime(profile, mode),
            )
        last_poll_result: dict[str, object] | None = None
        initial_payload = status_payload()
        if soak_session is not None:
            try:
                initial_payload["soakReport"] = soak_session.sample(initial_payload)
                from .soak_monitor import should_auto_stop_soak

                if should_auto_stop_soak(initial_payload["soakReport"]):
                    soak_finish_reason = "target-duration-complete"
                    stop.set()
            except Exception as exc:
                initial_payload["soakReportError"] = _exception_detail(exc)
        _write_status(status_path, initial_payload)
        while not stop.wait(max(5.0, float(poll_seconds))):
            poll_result = _safe_dict_call(
                "execution event poll",
                lambda: state.poll_execution_events("all"),
            )
            last_poll_result = _result_summary(poll_result)
            if stream_result.get("ok") is not True:
                startup_attempts["streams"] = int(startup_attempts["streams"]) + 1
                stream_result = _safe_dict_call(
                    "execution stream startup retry",
                    lambda: state.start_execution_streams("all"),
                )
            for profile in selected:
                if runtime_results[profile].get("ok") is True:
                    continue
                attempts = startup_attempts["runtimes"]
                attempts[profile] = int(attempts[profile]) + 1
                runtime_results[profile] = _safe_dict_call(
                    f"{profile} continuous runtime startup retry",
                    lambda profile=profile: state.start_continuous_runtime(profile, mode),
                )
            heartbeat_payload = status_payload(
                last_execution_poll=last_poll_result
            )
            if soak_session is not None:
                try:
                    heartbeat_payload["soakReport"] = soak_session.sample(
                        heartbeat_payload
                    )
                    from .soak_monitor import should_auto_stop_soak

                    if should_auto_stop_soak(
                        heartbeat_payload["soakReport"]
                    ):
                        soak_finish_reason = "target-duration-complete"
                        stop.set()
                except Exception as exc:
                    heartbeat_payload["soakReportError"] = _exception_detail(exc)
            _write_status(status_path, heartbeat_payload)
    finally:
        cleanup_results = {
            "runtimes": _result_summary(_safe_dict_call(
                "continuous runtime shutdown",
                lambda: state.stop_continuous_runtime(""),
            )),
            "streams": _result_summary(_safe_dict_call(
                "execution stream shutdown",
                state.stop_execution_streams,
            )),
        }
        stopped_payload = {
            "schemaVersion": "live-trader-daemon-v2",
            "phase": "STOPPED",
            "running": False,
            "pid": os.getpid(),
            "startedAt": started_at,
            "stoppedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "heartbeatLeaseSeconds": heartbeat_lease_seconds,
            "mode": mode,
            "profiles": list(selected),
            "cleanup": cleanup_results,
        }
        if soak_session is not None:
            try:
                stopped_payload["soakReport"] = soak_session.finish(
                    stopped_payload,
                    reason=soak_finish_reason,
                    failed=any(
                        result.get("ok") is False
                        for result in (
                            cleanup_results["streams"],
                            cleanup_results["runtimes"],
                        )
                    ),
                )
            except Exception as exc:
                stopped_payload["soakReportError"] = _exception_detail(exc)
        _write_status(status_path, stopped_payload)
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
    if os.name == "nt":
        # ``os.kill(pid, 0)`` is not a reliable existence probe on Windows:
        # it can report a live venv child process as missing. Query the
        # process handle and exit code without signalling or mutating it.
        try:
            import ctypes
            from ctypes import wintypes

            process_query_limited_information = 0x1000
            still_active = 259
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = (
                wintypes.DWORD,
                wintypes.BOOL,
                wintypes.DWORD,
            )
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.GetExitCodeProcess.argtypes = (
                wintypes.HANDLE,
                ctypes.POINTER(wintypes.DWORD),
            )
            kernel32.GetExitCodeProcess.restype = wintypes.BOOL
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            kernel32.CloseHandle.restype = wintypes.BOOL
            handle = kernel32.OpenProcess(
                process_query_limited_information,
                False,
                pid,
            )
            if not handle:
                return False
            try:
                exit_code = wintypes.DWORD()
                if not kernel32.GetExitCodeProcess(
                    handle,
                    ctypes.byref(exit_code),
                ):
                    return False
                return int(exit_code.value) == still_active
            finally:
                kernel32.CloseHandle(handle)
        except (AttributeError, OSError, TypeError, ValueError):
            return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _safe_dict_call(
    operation: str,
    callback: Callable[[], object],
) -> dict[str, object]:
    try:
        result = callback()
    except Exception as exc:
        return {
            "ok": False,
            "reason": _exception_detail(exc),
            "operation": operation,
            "errorType": type(exc).__name__,
        }
    if isinstance(result, dict):
        return dict(result)
    return {
        "ok": False,
        "reason": f"{operation} returned {type(result).__name__}, expected dict",
        "operation": operation,
        "errorType": "InvalidResult",
    }


def _result_summary(result: dict[str, object]) -> dict[str, object]:
    summary: dict[str, object] = {
        "ok": result.get("ok"),
        "reason": str(result.get("reason") or "")[:500],
        **(
            {"operation": str(result.get("operation") or "")[:100]}
            if result.get("operation")
            else {}
        ),
        **(
            {"errorType": str(result.get("errorType") or "")[:100]}
            if result.get("errorType")
            else {}
        ),
    }
    errors = result.get("errors")
    if isinstance(errors, list):
        summary["errors"] = [
            {
                "brokerId": str(error.get("broker_id") or "")[:50],
                "detail": str(error.get("detail") or "")[:500],
            }
            for error in errors[:10]
            if isinstance(error, dict)
        ]
    return summary


def _exception_detail(exc: Exception) -> str:
    detail = " ".join(str(exc).split())
    return f"{type(exc).__name__}: {detail or 'no detail'}"[:500]


def _write_status(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
