from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from live_trader import daemon, state


class FakeStopEvent:
    def __init__(self, waits: list[bool]) -> None:
        self.waits = list(waits)
        self.was_set = False

    def set(self) -> None:
        self.was_set = True

    def wait(self, _timeout: float) -> bool:
        if self.was_set:
            return True
        return self.waits.pop(0) if self.waits else True


class DaemonResilienceTest(unittest.TestCase):
    def test_poll_timeout_degrades_without_escaping_process_boundary(self) -> None:
        written: list[dict[str, object]] = []
        stop = FakeStopEvent([False, True])
        with tempfile.TemporaryDirectory() as temporary, patch.object(
            daemon.threading,
            "Event",
            return_value=stop,
        ), patch.object(
            daemon.signal,
            "signal",
        ), patch.object(
            daemon,
            "default_runtime_data_root",
            return_value=Path(temporary),
        ), patch.object(
            daemon,
            "_write_status",
            side_effect=lambda _path, payload: written.append(dict(payload)),
        ), patch.object(
            state,
            "start_execution_streams",
            return_value={"ok": True, "reason": "started"},
        ), patch.object(
            state,
            "start_continuous_runtime",
            return_value={"ok": True, "reason": "started"},
        ), patch.object(
            state,
            "poll_execution_events",
            side_effect=TimeoutError("The read operation timed out"),
        ), patch.object(
            state,
            "stop_continuous_runtime",
            return_value={"ok": True, "reason": "stopped"},
        ), patch.object(
            state,
            "stop_execution_streams",
            return_value={"ok": True, "reason": "stopped"},
        ), patch.object(
            state.LIVE_CONTINUOUS_CONTROLLER,
            "snapshot",
            return_value={"running": True},
        ), patch.object(
            state.LIVE_EXECUTION_STREAMS,
            "snapshot",
            return_value={"running": True},
        ):
            result = daemon.run_daemon(("crypto",), "MONITOR", 5)

        self.assertEqual(0, result)
        phases = [str(payload.get("phase")) for payload in written]
        self.assertEqual(["STARTING", "RUNNING", "DEGRADED", "STOPPED"], phases)
        degraded = written[2]
        self.assertFalse(degraded["lastExecutionPoll"]["ok"])
        self.assertIn(
            "TimeoutError: The read operation timed out",
            degraded["lastExecutionPoll"]["reason"],
        )

    def test_failed_startup_is_retried_and_recovers_to_running(self) -> None:
        written: list[dict[str, object]] = []
        stop = FakeStopEvent([False, True])
        with tempfile.TemporaryDirectory() as temporary, patch.object(
            daemon.threading,
            "Event",
            return_value=stop,
        ), patch.object(
            daemon.signal,
            "signal",
        ), patch.object(
            daemon,
            "default_runtime_data_root",
            return_value=Path(temporary),
        ), patch.object(
            daemon,
            "_write_status",
            side_effect=lambda _path, payload: written.append(dict(payload)),
        ), patch.object(
            state,
            "start_execution_streams",
            side_effect=[
                TimeoutError("stream startup timeout"),
                {"ok": True, "reason": "started"},
            ],
        ) as stream_start, patch.object(
            state,
            "start_continuous_runtime",
            side_effect=[
                TimeoutError("runtime startup timeout"),
                {"ok": True, "reason": "started"},
            ],
        ) as runtime_start, patch.object(
            state,
            "poll_execution_events",
            return_value={"ok": True, "reason": "polled"},
        ), patch.object(
            state,
            "stop_continuous_runtime",
            return_value={"ok": True, "reason": "stopped"},
        ), patch.object(
            state,
            "stop_execution_streams",
            return_value={"ok": True, "reason": "stopped"},
        ), patch.object(
            state.LIVE_CONTINUOUS_CONTROLLER,
            "snapshot",
            return_value={"running": True},
        ), patch.object(
            state.LIVE_EXECUTION_STREAMS,
            "snapshot",
            return_value={"running": True},
        ):
            result = daemon.run_daemon(("crypto",), "MONITOR", 5)

        self.assertEqual(0, result)
        phases = [str(payload.get("phase")) for payload in written]
        self.assertEqual(["STARTING", "DEGRADED", "RUNNING", "STOPPED"], phases)
        self.assertEqual(2, stream_start.call_count)
        self.assertEqual(2, runtime_start.call_count)
        recovered = written[2]
        self.assertEqual(2, recovered["startup"]["attempts"]["streams"])
        self.assertEqual(2, recovered["startup"]["attempts"]["runtimes"]["crypto"])

    def test_poll_summary_keeps_only_broker_and_bounded_error_detail(self) -> None:
        summary = daemon._result_summary({
            "ok": False,
            "reason": "one broker failed",
            "errors": [{
                "broker_id": "upbit",
                "detail": "TimeoutError: read timed out" + ("x" * 1000),
                "credentials": "must-not-be-copied",
            }],
            "accounts": [{"secret": "must-not-be-copied"}],
        })

        self.assertEqual([{
            "brokerId": "upbit",
            "detail": ("TimeoutError: read timed out" + ("x" * 1000))[:500],
        }], summary["errors"])
        self.assertNotIn("accounts", summary)
        self.assertNotIn("credentials", str(summary))


if __name__ == "__main__":
    unittest.main()
