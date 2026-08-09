from __future__ import annotations

import json
import errno
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from live_trader.emergency_stop import (
    DesktopEmergencyStopBridge,
    _reset_emergency_stop_sticky_for_tests,
    clear_emergency_stop,
    emergency_stop_active,
    emergency_stop_status,
    engage_emergency_stop,
)
from live_trader.process_safety import (
    _windows_mutex_name,
    hold_kis_dispatch_lease,
    hold_live_trader_instance_lease,
    release_held_leases_for_tests,
)


class EmergencyStopLatchTests(unittest.TestCase):
    def setUp(self) -> None:
        _reset_emergency_stop_sticky_for_tests()
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "emergency-stop.json"
        self.environment = patch.dict(
            os.environ,
            {"LIVE_TRADER_EMERGENCY_STOP_PATH": str(self.path)},
        )
        self.environment.start()

    def tearDown(self) -> None:
        _reset_emergency_stop_sticky_for_tests()
        self.environment.stop()
        self.temporary.cleanup()

    def test_native_bridge_engages_atomic_durable_latch_without_api(self) -> None:
        bridge = DesktopEmergencyStopBridge()
        result = bridge.engage_emergency_stop("api thread unavailable")

        self.assertTrue(result["ok"])
        self.assertTrue(result["active"])
        self.assertTrue(result["durable"])
        self.assertTrue(emergency_stop_active())
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertTrue(payload["active"])
        self.assertEqual("pywebview-native-bridge", payload["source"])
        self.assertEqual([], list(self.path.parent.glob("*.tmp")))

    def test_each_reengage_has_a_new_public_revision_fingerprint(self) -> None:
        first = engage_emergency_stop("first")
        second = engage_emergency_stop("second")

        self.assertTrue(first["revision"])
        self.assertTrue(second["revision"])
        self.assertNotEqual(first["revision"], second["revision"])
        self.assertNotIn(
            json.loads(self.path.read_text(encoding="utf-8"))["nonce"],
            repr(second),
        )

    def test_release_requires_confirmed_api_boundary(self) -> None:
        self.assertTrue(engage_emergency_stop()["ok"])

        rejected = clear_emergency_stop(confirmed=False)
        self.assertFalse(rejected["ok"])
        self.assertTrue(emergency_stop_active())

        released = clear_emergency_stop(confirmed=True)
        self.assertTrue(released["ok"])
        self.assertFalse(emergency_stop_active())

    def test_malformed_existing_latch_is_fail_closed(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("{not-json", encoding="utf-8")

        status = emergency_stop_status()

        self.assertTrue(status["active"])
        self.assertFalse(status["durable"])
        self.assertEqual("fail-closed", status["status"])

    def test_unreadable_existing_latch_is_fail_closed(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("{}", encoding="utf-8")

        with patch.object(Path, "read_text", side_effect=OSError("denied")):
            status = emergency_stop_status()

        self.assertTrue(status["active"])
        self.assertFalse(status["durable"])
        self.assertEqual("fail-closed", status["status"])
        self.assertIn("OSError", status["reason"])

    def test_latch_presence_io_error_is_fail_closed_not_missing(self) -> None:
        with patch.object(
            Path,
            "lstat",
            side_effect=PermissionError("presence denied"),
        ):
            status = emergency_stop_status()

        self.assertTrue(status["active"])
        self.assertFalse(status["durable"])
        self.assertEqual("fail-closed", status["status"])
        self.assertIn("PermissionError", status["reason"])

    def test_on_write_failures_remain_sticky_until_confirmed_durable_clear(self) -> None:
        failures = (
            PermissionError("permission denied"),
            OSError(errno.ENOSPC, "disk full"),
            OSError("atomic replace failed"),
        )
        for failure in failures:
            with self.subTest(failure=repr(failure)):
                _reset_emergency_stop_sticky_for_tests()
                self.path.unlink(missing_ok=True)
                with patch(
                    "live_trader.emergency_stop._write_atomic",
                    side_effect=failure,
                ):
                    result = engage_emergency_stop("must stop")

                self.assertFalse(result["ok"])
                self.assertFalse(self.path.exists())
                sticky = emergency_stop_status()
                self.assertTrue(sticky["active"])
                self.assertFalse(sticky["durable"])
                self.assertEqual("sticky-fail-closed", sticky["status"])

                released = clear_emergency_stop(confirmed=True)
                self.assertTrue(released["ok"])
                self.assertFalse(emergency_stop_active())

    def test_os_replace_failure_removes_temp_and_stays_fail_closed(self) -> None:
        with patch(
            "live_trader.emergency_stop.os.replace",
            side_effect=OSError("replace failed"),
        ):
            result = engage_emergency_stop("replace failure")

        self.assertFalse(result["ok"])
        self.assertTrue(emergency_stop_active())
        self.assertEqual([], list(self.path.parent.glob("*.tmp")))


class CrossProcessLeaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.lock_root = Path(self.temporary.name) / "locks"
        self.environment = patch.dict(
            os.environ,
            {
                "LIVE_TRADER_PROCESS_LOCK_DIR": str(self.lock_root),
                "LIVE_TRADER_PROCESS_LOCK_NAMESPACE": (
                    f"test-{os.getpid()}-{id(self)}"
                ),
            },
        )
        self.environment.start()
        release_held_leases_for_tests()

    def tearDown(self) -> None:
        release_held_leases_for_tests()
        self.environment.stop()
        self.temporary.cleanup()

    def _holding_child(self, expression: str) -> subprocess.Popen[str]:
        ready = Path(self.temporary.name) / f"ready-{time.time_ns()}.json"
        script = (
            "import json, os, time; from pathlib import Path; "
            f"os.environ['LIVE_TRADER_PROCESS_LOCK_DIR']={str(self.lock_root)!r}; "
            "os.environ['LIVE_TRADER_PROCESS_LOCK_NAMESPACE']='child-bypass-attempt'; "
            "from live_trader.process_safety import "
            "hold_kis_dispatch_lease, hold_live_trader_instance_lease; "
            f"result={expression}; "
            f"Path({str(ready)!r}).write_text(json.dumps(result), encoding='utf-8'); "
            "time.sleep(15)"
        )
        child = subprocess.Popen(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 5
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        if not ready.exists():
            error = child.stderr.read() if child.stderr else ""
            self._stop_child(child)
            self.fail(error or "child lease process did not become ready")
        self.assertTrue(json.loads(ready.read_text(encoding="utf-8"))["acquired"])
        return child

    @staticmethod
    def _stop_child(child: subprocess.Popen[str]) -> None:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
        if child.stderr is not None:
            child.stderr.close()

    def test_second_executable_cannot_acquire_single_instance(self) -> None:
        child = self._holding_child("hold_live_trader_instance_lease()")
        try:
            blocked = hold_live_trader_instance_lease()
            self.assertFalse(blocked["acquired"])
            self.assertEqual(
                "process-lease-owned-by-another-process",
                blocked["reason"],
            )
        finally:
            self._stop_child(child)

        acquired_after_exit = hold_live_trader_instance_lease()
        self.assertTrue(acquired_after_exit["acquired"])

    def test_windows_mutex_name_is_fixed_machine_global(self) -> None:
        with patch.dict(
            os.environ,
            {"LIVE_TRADER_PROCESS_LOCK_NAMESPACE": "bypass-attempt"},
        ):
            production = _windows_mutex_name("scope-hash")

        self.assertEqual(
            "Global\\TradingSystem.LiveTrader.v1.scope-hash",
            production,
        )

    def test_second_server_cannot_own_same_kis_account_or_authority(self) -> None:
        child = self._holding_child(
            "hold_kis_dispatch_lease('account-fingerprint-1', authority_id='permit-1')"
        )
        try:
            blocked = hold_kis_dispatch_lease(
                "account-fingerprint-1",
                authority_id="permit-1",
            )
            self.assertFalse(blocked["acquired"])
            self.assertEqual("kis-account", blocked["kind"])
        finally:
            self._stop_child(child)

        acquired_after_exit = hold_kis_dispatch_lease(
            "account-fingerprint-1",
            authority_id="permit-1",
        )
        self.assertTrue(acquired_after_exit["acquired"])


if __name__ == "__main__":
    unittest.main()
