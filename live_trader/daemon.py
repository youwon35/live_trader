from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import signal
import threading
import time
from typing import Iterable

from .env_loader import default_runtime_data_root


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
    stream_result = state.start_execution_streams("all")
    runtime_results = {profile: state.start_continuous_runtime(profile, mode) for profile in selected}
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        while not stop.wait(max(5.0, float(poll_seconds))):
            poll_result = state.poll_execution_events("all")
            _write_status(status_path, {
                "schemaVersion": "live-trader-daemon-v1",
                "running": True,
                "pid": __import__("os").getpid(),
                "startedAt": started_at,
                "lastHeartbeat": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "mode": mode,
                "profiles": list(selected),
                "runtime": state.LIVE_CONTINUOUS_CONTROLLER.snapshot(),
                "executionStreams": state.LIVE_EXECUTION_STREAMS.snapshot(),
                "lastExecutionPoll": {"ok": poll_result.get("ok"), "reason": poll_result.get("reason")},
                "startup": {"streamsOk": stream_result.get("ok"), "runtimes": {key: value.get("ok") for key, value in runtime_results.items()}},
            })
    finally:
        state.stop_continuous_runtime("")
        state.stop_execution_streams()
        _write_status(status_path, {
            "schemaVersion": "live-trader-daemon-v1",
            "running": False,
            "startedAt": started_at,
            "stoppedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": mode,
            "profiles": list(selected),
        })
    return 0


def _write_status(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
