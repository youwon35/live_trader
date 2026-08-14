from __future__ import annotations

import json
import hashlib
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
    ProcessSafetyAuthorityError,
    _authoritative_current_process_identity,
    _windows_mutex_name,
    crypto_first_live_account_lease_status,
    crypto_first_live_owner_identity,
    hold_crypto_first_live_account_lease,
    hold_kis_dispatch_lease,
    hold_live_trader_instance_lease,
    hold_process_lease,
    release_held_leases_for_tests,
    verify_crypto_first_live_owner_identity,
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
        self.application_scope = (
            f"live-trader:application-instance:test:{os.getpid()}:{id(self)}"
        )
        self.application_scope_patch = patch(
            "live_trader.process_safety._APPLICATION_INSTANCE_SCOPE",
            self.application_scope,
        )
        self.application_scope_patch.start()
        release_held_leases_for_tests()

    def tearDown(self) -> None:
        release_held_leases_for_tests()
        self.application_scope_patch.stop()
        self.environment.stop()
        self.temporary.cleanup()

    def _holding_child_result(
        self, expression: str
    ) -> tuple[subprocess.Popen[str], dict[str, object]]:
        ready = Path(self.temporary.name) / f"ready-{time.time_ns()}.json"
        script = (
            "import json, os, time; from pathlib import Path; "
            f"os.environ['LIVE_TRADER_PROCESS_LOCK_DIR']={str(self.lock_root)!r}; "
            "os.environ['LIVE_TRADER_PROCESS_LOCK_NAMESPACE']='child-bypass-attempt'; "
            "import live_trader.process_safety as process_safety; "
            "process_safety._APPLICATION_INSTANCE_SCOPE="
            f"{self.application_scope!r}; "
            "from live_trader.process_safety import "
            "hold_crypto_first_live_account_lease, hold_kis_dispatch_lease, "
            "hold_live_trader_instance_lease, hold_process_lease; "
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
        result = json.loads(ready.read_text(encoding="utf-8"))
        if result.get("acquired") is not True:
            self._stop_child(child)
            self.fail(f"child lease was not acquired: {result!r}")
        return child, result

    def _holding_child(self, expression: str) -> subprocess.Popen[str]:
        child, _result = self._holding_child_result(expression)
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

    @staticmethod
    def _crypto_fingerprint(label: str = "account") -> str:
        return hashlib.sha256(label.encode("utf-8")).hexdigest()

    def test_crypto_account_requires_official_application_lease(self) -> None:
        fingerprint = self._crypto_fingerprint()

        result = hold_crypto_first_live_account_lease(
            "UPBIT", fingerprint
        )

        self.assertFalse(result["acquired"])
        self.assertEqual(
            "official-application-instance-lease-not-held",
            result["reason"],
        )

    def test_crypto_account_scope_lane_and_hash_are_exact(self) -> None:
        fingerprint = self._crypto_fingerprint()
        invalid = (
            ("upbit", fingerprint, "lane-not-exact"),
            ("FUTURES", fingerprint, "lane-not-exact"),
            ("UPBIT", fingerprint.upper(), "fingerprint-not-exact"),
            ("BINANCE_SPOT", "abc", "fingerprint-not-exact"),
        )

        for lane, account, reason in invalid:
            with self.subTest(lane=lane, account=account):
                result = hold_crypto_first_live_account_lease(lane, account)
                self.assertFalse(result["acquired"])
                self.assertIn(reason, result["reason"])

    def test_crypto_account_status_and_owner_identity_are_exact(self) -> None:
        fingerprint = self._crypto_fingerprint("upbit-primary")
        self.assertTrue(hold_live_trader_instance_lease()["acquired"])

        acquired = hold_crypto_first_live_account_lease(
            "UPBIT", fingerprint
        )
        status = crypto_first_live_account_lease_status(
            "UPBIT", fingerprint
        )
        identity = crypto_first_live_owner_identity(
            "UPBIT", fingerprint
        )

        exact_scope = f"crypto-first-live-account:UPBIT:{fingerprint}"
        self.assertTrue(acquired["acquired"])
        self.assertTrue(status["acquired"])
        self.assertTrue(status["reused"])
        self.assertEqual(exact_scope, status["accountLeaseScope"])
        self.assertEqual(
            {
                "pid",
                "processStartEpoch",
                "bootId",
                "applicationLeaseEpoch",
                "accountLeaseScope",
            },
            set(identity),
        )
        self.assertEqual(os.getpid(), identity["pid"])
        self.assertEqual(exact_scope, identity["accountLeaseScope"])
        self.assertGreater(
            identity["applicationLeaseEpoch"],
            identity["processStartEpoch"],
        )
        release_held_leases_for_tests()
        released = crypto_first_live_account_lease_status(
            "UPBIT", fingerprint
        )
        self.assertFalse(released["acquired"])
        self.assertEqual(
            "official-application-instance-lease-not-held",
            released["reason"],
        )

    def test_owner_identity_verifier_rechecks_current_kernel_identity(self) -> None:
        fingerprint = self._crypto_fingerprint("binance-primary")
        self.assertTrue(hold_live_trader_instance_lease()["acquired"])
        acquired = hold_crypto_first_live_account_lease(
            "BINANCE_SPOT", fingerprint
        )
        identity = dict(acquired["ownerIdentity"])
        canonical = json.dumps(
            identity,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        request = {
            "schemaVersion": "crypto-first-live-owner-identity/v1",
            "purpose": "DISPATCH_ENTRY_ORDER",
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "runId": "crypto-first-live-run-test-0001",
            "lane": "BINANCE_SPOT",
            "accountFingerprint": fingerprint,
            "ownerIdentity": identity,
            "ownerIdentityHash": hashlib.sha256(
                canonical.encode("utf-8")
            ).hexdigest(),
            "ownerEpoch": 1,
            "coordinatorRevision": 2,
        }

        self.assertTrue(verify_crypto_first_live_owner_identity(request))
        request["ownerIdentity"] = {
            **identity,
            "pid": int(identity["pid"]) + 1,
        }
        self.assertFalse(verify_crypto_first_live_owner_identity(request))

    def test_unauthoritative_application_identity_fails_closed(self) -> None:
        fingerprint = self._crypto_fingerprint("no-authority")
        with patch(
            "live_trader.process_safety."
            "_authoritative_current_process_identity",
            side_effect=ProcessSafetyAuthorityError("unavailable"),
        ):
            self.assertTrue(hold_live_trader_instance_lease()["acquired"])
            blocked = hold_crypto_first_live_account_lease(
                "UPBIT", fingerprint
            )

        self.assertFalse(blocked["acquired"])
        self.assertEqual(
            "application-instance-identity-not-authoritative",
            blocked["reason"],
        )

    def test_changed_boot_identity_invalidates_current_application_lease(self) -> None:
        fingerprint = self._crypto_fingerprint("changed-boot")
        self.assertTrue(hold_live_trader_instance_lease()["acquired"])
        current = _authoritative_current_process_identity()
        changed = {**current, "bootId": "windows-boot-changed-00000001"}

        with patch(
            "live_trader.process_safety."
            "_authoritative_current_process_identity",
            return_value=changed,
        ):
            blocked = hold_crypto_first_live_account_lease(
                "UPBIT", fingerprint
            )

        self.assertFalse(blocked["acquired"])
        self.assertEqual(
            "application-instance-owner-identity-changed",
            blocked["reason"],
        )

    def test_same_crypto_account_has_one_cross_process_owner(self) -> None:
        fingerprint = self._crypto_fingerprint("cross-process")
        scope = f"crypto-first-live-account:UPBIT:{fingerprint}"
        self.assertTrue(hold_live_trader_instance_lease()["acquired"])
        child = self._holding_child(f"hold_process_lease({scope!r})")
        try:
            blocked = hold_crypto_first_live_account_lease(
                "UPBIT", fingerprint
            )
            self.assertFalse(blocked["acquired"])
            self.assertEqual(
                "crypto-first-live-account-owned-by-another-process",
                blocked["reason"],
            )
        finally:
            self._stop_child(child)

        self.assertTrue(
            hold_crypto_first_live_account_lease(
                "UPBIT", fingerprint
            )["acquired"]
        )

    def test_process_restart_rotates_authoritative_owner_identity(self) -> None:
        fingerprint = self._crypto_fingerprint("restart")
        expression = (
            "(hold_live_trader_instance_lease(), "
            "hold_crypto_first_live_account_lease("
            f"'BINANCE_SPOT', {fingerprint!r}))[1]"
        )
        child, child_result = self._holding_child_result(expression)
        child_identity = dict(child_result["ownerIdentity"])
        self._stop_child(child)

        self.assertTrue(hold_live_trader_instance_lease()["acquired"])
        current = hold_crypto_first_live_account_lease(
            "BINANCE_SPOT", fingerprint
        )
        current_identity = dict(current["ownerIdentity"])

        self.assertNotEqual(child_identity["pid"], current_identity["pid"])
        self.assertNotEqual(
            child_identity["processStartEpoch"],
            current_identity["processStartEpoch"],
        )
        self.assertEqual(child_identity["bootId"], current_identity["bootId"])

    def test_kernel_identity_is_stable_during_current_process(self) -> None:
        first = _authoritative_current_process_identity()
        second = _authoritative_current_process_identity()

        self.assertTrue(first["authoritative"])
        self.assertEqual(first["pid"], second["pid"])
        self.assertEqual(first["bootId"], second["bootId"])
        self.assertAlmostEqual(
            first["processStartEpoch"],
            second["processStartEpoch"],
            places=6,
        )
        if os.name == "nt":
            self.assertEqual(
                "GetProcessTimes+NtQuerySystemInformation",
                first["source"],
            )


if __name__ == "__main__":
    unittest.main()
