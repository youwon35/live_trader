from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

from live_trader.daemon import read_daemon_status


class DaemonStatusTest(unittest.TestCase):
    def test_fresh_running_lease_requires_live_process(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "daemon_status.json"
            now = datetime(2026, 7, 27, 1, 30, 0)
            path.write_text(
                json.dumps(
                    {
                        "schemaVersion": "live-trader-daemon-v2",
                        "phase": "RUNNING",
                        "running": True,
                        "pid": 1234,
                        "lastHeartbeat": (
                            now - timedelta(seconds=10)
                        ).isoformat(sep=" "),
                        "heartbeatLeaseSeconds": 90,
                    }
                ),
                encoding="utf-8",
            )

            status = read_daemon_status(
                path,
                now=now,
                process_checker=lambda pid: pid == 1234,
            )

        self.assertTrue(status["running"])
        self.assertEqual("RUNNING", status["phase"])
        self.assertTrue(status["processAlive"])
        self.assertEqual(10.0, status["heartbeatAgeSeconds"])

    def test_dead_process_materializes_stale_status(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "daemon_status.json"
            now = datetime(2026, 7, 27, 1, 30, 0)
            path.write_text(
                json.dumps(
                    {
                        "schemaVersion": "live-trader-daemon-v2",
                        "phase": "RUNNING",
                        "running": True,
                        "pid": 999999,
                        "lastHeartbeat": (
                            now - timedelta(seconds=10)
                        ).isoformat(sep=" "),
                        "heartbeatLeaseSeconds": 90,
                    }
                ),
                encoding="utf-8",
            )

            status = read_daemon_status(
                path,
                now=now,
                process_checker=lambda _pid: False,
                persist=True,
            )
            persisted = json.loads(path.read_text(encoding="utf-8"))
            second = read_daemon_status(path, now=now)

        self.assertFalse(status["running"])
        self.assertEqual("STALE", status["phase"])
        self.assertEqual(["process-not-running"], status["staleReasons"])
        self.assertFalse(persisted["running"])
        self.assertEqual("STALE", persisted["phase"])
        self.assertTrue(persisted["recordedRunning"])
        self.assertFalse(second["running"])
        self.assertEqual("STALE", second["phase"])

    def test_expired_heartbeat_is_stale_even_when_pid_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "daemon_status.json"
            now = datetime(2026, 7, 27, 1, 30, 0)
            path.write_text(
                json.dumps(
                    {
                        "phase": "DEGRADED",
                        "running": True,
                        "pid": 1234,
                        "lastHeartbeat": (
                            now - timedelta(seconds=91)
                        ).isoformat(sep=" "),
                        "heartbeatLeaseSeconds": 90,
                    }
                ),
                encoding="utf-8",
            )

            status = read_daemon_status(
                path,
                now=now,
                process_checker=lambda _pid: True,
            )

        self.assertFalse(status["running"])
        self.assertEqual("STALE", status["phase"])
        self.assertEqual(["heartbeat-timeout"], status["staleReasons"])
        self.assertEqual("DEGRADED", status["recordedPhase"])

    def test_missing_status_is_stopped(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "missing.json"
            status = read_daemon_status(path)

        self.assertFalse(status["running"])
        self.assertEqual("STOPPED", status["phase"])
        self.assertFalse(status["exists"])


if __name__ == "__main__":
    unittest.main()
