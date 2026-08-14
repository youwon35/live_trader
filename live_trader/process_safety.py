from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import threading
import ctypes
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Mapping


_HELD_LEASES_LOCK = threading.RLock()
_HELD_LEASES: dict[str, "CrossProcessLease"] = {}
_APPLICATION_INSTANCE_SCOPE = "live-trader:application-instance:v1"
_CRYPTO_FIRST_LIVE_LANES = frozenset({"UPBIT", "BINANCE_SPOT"})
_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ProcessSafetyAuthorityError(RuntimeError):
    """Raised when an authoritative process identity cannot be established."""


def _windows_authoritative_process_identity() -> dict[str, object]:
    class FILETIME(ctypes.Structure):
        _fields_ = [
            ("dwLowDateTime", ctypes.c_uint32),
            ("dwHighDateTime", ctypes.c_uint32),
        ]

    class SYSTEM_TIMEOFDAY_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BootTime", ctypes.c_int64),
            ("CurrentTime", ctypes.c_int64),
            ("TimeZoneBias", ctypes.c_int64),
            ("CurrentTimeZoneId", ctypes.c_uint32),
            ("Reserved", ctypes.c_uint32),
            ("BootTimeBias", ctypes.c_uint64),
            ("SleepTimeBias", ctypes.c_uint64),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.argtypes = []
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.GetProcessTimes.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
    ]
    kernel32.GetProcessTimes.restype = ctypes.c_bool
    creation = FILETIME()
    exit_time = FILETIME()
    kernel_time = FILETIME()
    user_time = FILETIME()
    if not kernel32.GetProcessTimes(
        kernel32.GetCurrentProcess(),
        ctypes.byref(creation),
        ctypes.byref(exit_time),
        ctypes.byref(kernel_time),
        ctypes.byref(user_time),
    ):
        raise OSError(ctypes.get_last_error(), "GetProcessTimes failed")
    creation_filetime = (
        int(creation.dwHighDateTime) << 32
    ) | int(creation.dwLowDateTime)

    ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
    ntdll.NtQuerySystemInformation.argtypes = [
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    ntdll.NtQuerySystemInformation.restype = ctypes.c_long
    time_of_day = SYSTEM_TIMEOFDAY_INFORMATION()
    returned = ctypes.c_uint32()
    status = int(
        ntdll.NtQuerySystemInformation(
            3,  # SystemTimeOfDayInformation
            ctypes.byref(time_of_day),
            ctypes.sizeof(time_of_day),
            ctypes.byref(returned),
        )
    )
    if status != 0 or int(time_of_day.BootTime) <= 0:
        raise OSError(status & 0xFFFFFFFF, "NtQuerySystemInformation failed")

    windows_to_unix_100ns = 116_444_736_000_000_000
    process_start = (
        creation_filetime - windows_to_unix_100ns
    ) / 10_000_000.0
    boot_epoch = (
        int(time_of_day.BootTime) - windows_to_unix_100ns
    ) / 10_000_000.0
    now = time.time()
    if (
        not math.isfinite(process_start)
        or not math.isfinite(boot_epoch)
        or boot_epoch <= 0
        or process_start < boot_epoch - 5
        or process_start > now + 5
    ):
        raise ProcessSafetyAuthorityError(
            "windows-process-time-authority-invalid"
        )
    boot_digest = hashlib.sha256(
        f"windows-boot-time-v1:{int(time_of_day.BootTime)}".encode("ascii")
    ).hexdigest()
    return {
        "pid": os.getpid(),
        "processStartEpoch": process_start,
        "bootId": "windows-boot-" + boot_digest,
        "authoritative": True,
        "source": "GetProcessTimes+NtQuerySystemInformation",
    }


def _linux_authoritative_process_identity() -> dict[str, object]:
    boot_uuid = Path("/proc/sys/kernel/random/boot_id").read_text(
        encoding="ascii"
    ).strip().lower()
    if re.fullmatch(r"[0-9a-f-]{36}", boot_uuid) is None:
        raise ProcessSafetyAuthorityError("linux-boot-id-invalid")
    stat_text = Path(f"/proc/{os.getpid()}/stat").read_text(
        encoding="ascii"
    )
    close_paren = stat_text.rfind(")")
    if close_paren < 0:
        raise ProcessSafetyAuthorityError("linux-process-stat-invalid")
    fields = stat_text[close_paren + 2 :].split()
    if len(fields) <= 19:
        raise ProcessSafetyAuthorityError("linux-process-stat-invalid")
    start_ticks = int(fields[19])
    ticks_per_second = int(os.sysconf("SC_CLK_TCK"))
    boot_epoch = 0
    for line in Path("/proc/stat").read_text(encoding="ascii").splitlines():
        if line.startswith("btime "):
            boot_epoch = int(line.split()[1])
            break
    process_start = boot_epoch + (start_ticks / ticks_per_second)
    if boot_epoch <= 0 or process_start <= 0:
        raise ProcessSafetyAuthorityError("linux-process-time-invalid")
    return {
        "pid": os.getpid(),
        "processStartEpoch": process_start,
        "bootId": "linux-boot-" + boot_uuid,
        "authoritative": True,
        "source": "procfs-boot_id+procfs-process-stat",
    }


def _authoritative_current_process_identity() -> dict[str, object]:
    """Read identity from kernel authority, never from caller input."""

    try:
        if os.name == "nt":
            value = _windows_authoritative_process_identity()
        elif os.name == "posix" and Path("/proc").is_dir():
            value = _linux_authoritative_process_identity()
        else:
            raise ProcessSafetyAuthorityError(
                "authoritative-process-identity-platform-unsupported"
            )
    except (OSError, ValueError, ProcessSafetyAuthorityError) as exc:
        raise ProcessSafetyAuthorityError(
            "authoritative-process-identity-unavailable"
        ) from exc
    if (
        value.get("authoritative") is not True
        or int(value.get("pid", 0)) != os.getpid()
        or not math.isfinite(float(value.get("processStartEpoch", 0)))
        or float(value.get("processStartEpoch", 0)) <= 0
        or not str(value.get("bootId") or "").strip()
    ):
        raise ProcessSafetyAuthorityError(
            "authoritative-process-identity-invalid"
        )
    return value


def _utc_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def process_lock_root() -> Path:
    """Return the machine-local directory used for OS process leases.

    Locks intentionally live outside the source tree.  A configured runtime
    data directory can be copied or restored, while these files merely name an
    operating-system lock whose lifetime is the owning process handle.
    """

    configured = str(os.getenv("LIVE_TRADER_PROCESS_LOCK_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    local = str(os.getenv("LOCALAPPDATA") or "").strip()
    base = Path(local).expanduser() if local else Path(tempfile.gettempdir())
    return base / "trading-system" / "live-trader" / "process-locks"


def _safe_lock_name(scope: str) -> str:
    normalized = str(scope or "").strip().lower()
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"{digest}.lock"


def _windows_mutex_name(scope_hash: str) -> str:
    # Global\ is machine-wide across interactive/RDP/service sessions. A
    # Local\ mutex would allow two sessions to dispatch through one account.
    # The namespace is deliberately code-fixed: an environment-controlled
    # namespace would let two executables choose different names and own the
    # same application/account/authority concurrently.
    return (
        "Global\\TradingSystem.LiveTrader.v1."
        f"{str(scope_hash or '').strip().lower()}"
    )


def _lock_file(handle: BinaryIO) -> None:
    handle.seek(0)
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(handle: BinaryIO) -> None:
    handle.seek(0)
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass
class CrossProcessLease:
    scope: str
    path: Path
    handle: BinaryIO
    acquired_at: str
    acquired_epoch: float
    owner_pid: int
    owner_process_start_epoch: float
    owner_boot_id: str
    identity_authoritative: bool
    windows_mutex_handle: int | None = None

    def status(self, *, reused: bool = False) -> dict[str, object]:
        return {
            "acquired": True,
            "scopeHash": self.path.stem,
            "ownerPid": self.owner_pid,
            "acquiredAt": self.acquired_at,
            "reused": reused,
        }

    def release(self) -> None:
        try:
            if self.windows_mutex_handle is not None:
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.ReleaseMutex(
                    ctypes.c_void_p(self.windows_mutex_handle)
                )
                kernel32.CloseHandle(
                    ctypes.c_void_p(self.windows_mutex_handle)
                )
            else:
                _unlock_file(self.handle)
        finally:
            self.handle.close()


def _owner_metadata(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()[1:]
        payload = json.loads(raw.decode("utf-8")) if raw else {}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def acquire_process_lease(scope: str) -> CrossProcessLease | None:
    """Try to acquire an OS-backed, non-blocking lease for *scope*.

    The first byte is locked with ``msvcrt.locking`` on Windows and ``flock``
    elsewhere.  Process termination releases the kernel lock even when the
    executable is killed, so a stale file cannot create a stale authority.
    """

    root = process_lock_root()
    root.mkdir(parents=True, exist_ok=True)
    path = root / _safe_lock_name(scope)
    handle = path.open("a+b")
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
        os.fsync(handle.fileno())
    windows_mutex_handle: int | None = None
    if os.name == "nt":
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.argtypes = [
            ctypes.c_void_p,
            ctypes.c_bool,
            ctypes.c_wchar_p,
        ]
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel32.WaitForSingleObject.restype = ctypes.c_uint32
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_bool
        mutex_name = _windows_mutex_name(path.stem)
        ctypes.set_last_error(0)
        raw_handle = kernel32.CreateMutexW(None, False, mutex_name)
        if not raw_handle:
            handle.close()
            raise OSError(ctypes.get_last_error(), "CreateMutexW failed")
        windows_mutex_handle = int(raw_handle)
        wait_result = int(
            kernel32.WaitForSingleObject(
                ctypes.c_void_p(windows_mutex_handle),
                0,
            )
        )
        if wait_result == 0x00000102:  # WAIT_TIMEOUT
            kernel32.CloseHandle(ctypes.c_void_p(windows_mutex_handle))
            handle.close()
            return None
        if wait_result == 0xFFFFFFFF:  # WAIT_FAILED
            error = ctypes.get_last_error()
            kernel32.CloseHandle(ctypes.c_void_p(windows_mutex_handle))
            handle.close()
            raise OSError(error, "WaitForSingleObject failed")
    else:
        try:
            _lock_file(handle)
        except (OSError, BlockingIOError):
            handle.close()
            return None
    acquired_epoch = time.time()
    acquired_at = _utc_text()
    try:
        process_identity = _authoritative_current_process_identity()
    except ProcessSafetyAuthorityError:
        process_identity = {
            "pid": os.getpid(),
            "processStartEpoch": 0.0,
            "bootId": "",
            "authoritative": False,
        }
    metadata = {
        "schemaVersion": 1,
        "pid": os.getpid(),
        "acquiredAt": acquired_at,
        "scopeHash": path.stem,
    }
    try:
        handle.seek(1)
        handle.truncate()
        handle.write(json.dumps(metadata, separators=(",", ":")).encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())
    except OSError:
        # The kernel lock is the authority. Metadata is diagnostic only.
        pass
    return CrossProcessLease(
        scope=str(scope),
        path=path,
        handle=handle,
        acquired_at=acquired_at,
        acquired_epoch=acquired_epoch,
        owner_pid=int(process_identity["pid"]),
        owner_process_start_epoch=float(
            process_identity["processStartEpoch"]
        ),
        owner_boot_id=str(process_identity["bootId"]),
        identity_authoritative=(
            process_identity.get("authoritative") is True
        ),
        windows_mutex_handle=windows_mutex_handle,
    )


def hold_process_lease(scope: str) -> dict[str, object]:
    """Acquire once and retain until process exit or explicit test cleanup."""

    normalized = str(scope or "").strip().lower()
    if not normalized:
        return {"acquired": False, "reason": "process-lease-scope-missing"}
    with _HELD_LEASES_LOCK:
        existing = _HELD_LEASES.get(normalized)
        if existing is not None and not existing.handle.closed:
            if existing.owner_pid != os.getpid():
                return {
                    "acquired": False,
                    "reason": "process-lease-inherited-from-another-process",
                }
            return existing.status(reused=True)
        try:
            lease = acquire_process_lease(normalized)
        except OSError as exc:
            return {
                "acquired": False,
                "reason": f"process-lease-unavailable:{type(exc).__name__}",
            }
        if lease is None:
            path = process_lock_root() / _safe_lock_name(normalized)
            owner = _owner_metadata(path)
            return {
                "acquired": False,
                "reason": "process-lease-owned-by-another-process",
                "scopeHash": path.stem,
                "ownerPid": owner.get("pid"),
                "acquiredAt": owner.get("acquiredAt"),
            }
        _HELD_LEASES[normalized] = lease
        return lease.status()


def hold_live_trader_instance_lease() -> dict[str, object]:
    return hold_process_lease(_APPLICATION_INSTANCE_SCOPE)


def held_process_lease_status(scope: str) -> dict[str, object]:
    """Return only this process's retained lease state.

    Reading the diagnostic lock file is not authority: another process can
    exit between the read and a mutation.  Production-only composition uses
    this helper to prove that the current process already owns the retained
    kernel lease acquired by the official application entrypoint.
    """

    normalized = str(scope or "").strip().lower()
    if not normalized:
        return {"acquired": False, "reason": "process-lease-scope-missing"}
    with _HELD_LEASES_LOCK:
        lease = _HELD_LEASES.get(normalized)
        if (
            lease is None
            or lease.handle.closed
            or lease.owner_pid != os.getpid()
        ):
            return {
                "acquired": False,
                "reason": "process-lease-not-held-by-current-process",
            }
        return lease.status(reused=True)


def live_trader_instance_lease_status() -> dict[str, object]:
    """Prove current-process ownership of the official application lease."""

    return held_process_lease_status(_APPLICATION_INSTANCE_SCOPE)


def _crypto_first_live_account_scope(
    lane: str,
    account_fingerprint: str,
) -> tuple[str, str, str]:
    exact_lane = str(lane or "").strip()
    if exact_lane not in _CRYPTO_FIRST_LIVE_LANES:
        raise ProcessSafetyAuthorityError(
            "crypto-first-live-lane-not-exact"
        )
    fingerprint = str(account_fingerprint or "").strip()
    if _LOWER_SHA256_RE.fullmatch(fingerprint) is None:
        raise ProcessSafetyAuthorityError(
            "crypto-first-live-account-fingerprint-not-exact"
        )
    return (
        exact_lane,
        fingerprint,
        f"crypto-first-live-account:{exact_lane}:{fingerprint}",
    )


def _same_authoritative_process(
    lease: CrossProcessLease,
    current: Mapping[str, object],
) -> bool:
    try:
        return bool(
            lease.identity_authoritative
            and current.get("authoritative") is True
            and lease.owner_pid == os.getpid() == int(current["pid"])
            and lease.owner_boot_id == str(current["bootId"])
            and math.isclose(
                lease.owner_process_start_epoch,
                float(current["processStartEpoch"]),
                rel_tol=0.0,
                abs_tol=1e-6,
            )
        )
    except (KeyError, TypeError, ValueError):
        return False


def _official_application_authority_locked(
) -> tuple[CrossProcessLease | None, dict[str, object] | None, str]:
    application = _HELD_LEASES.get(_APPLICATION_INSTANCE_SCOPE.lower())
    if (
        application is None
        or application.handle.closed
        or application.owner_pid != os.getpid()
    ):
        return (
            None,
            None,
            "official-application-instance-lease-not-held",
        )
    try:
        current = _authoritative_current_process_identity()
    except ProcessSafetyAuthorityError:
        return (
            None,
            None,
            "application-instance-identity-not-authoritative",
        )
    if not _same_authoritative_process(application, current):
        return (
            None,
            None,
            "application-instance-owner-identity-changed",
        )
    if (
        not math.isfinite(application.acquired_epoch)
        or application.acquired_epoch <= application.owner_process_start_epoch
        or application.acquired_epoch > time.time() + 5
    ):
        return (
            None,
            None,
            "application-instance-lease-epoch-invalid",
        )
    return application, dict(current), ""


def _crypto_account_status_locked(
    *,
    lane: str,
    account_fingerprint: str,
    account_scope: str,
    reused: bool,
) -> dict[str, object]:
    application, current, reason = _official_application_authority_locked()
    if application is None or current is None:
        return {"acquired": False, "reason": reason}
    account = _HELD_LEASES.get(account_scope.lower())
    if (
        account is None
        or account.handle.closed
        or account.owner_pid != os.getpid()
    ):
        return {
            "acquired": False,
            "reason": "crypto-first-live-account-lease-not-held",
            "accountLeaseScope": account_scope,
        }
    if not _same_authoritative_process(account, current):
        return {
            "acquired": False,
            "reason": "crypto-first-live-account-owner-identity-changed",
            "accountLeaseScope": account_scope,
        }
    owner_identity = {
        "pid": int(current["pid"]),
        "processStartEpoch": float(current["processStartEpoch"]),
        "bootId": str(current["bootId"]),
        "applicationLeaseEpoch": float(application.acquired_epoch),
        "accountLeaseScope": account_scope,
    }
    return {
        "schemaVersion": "crypto-first-live-account-lease/v1",
        "acquired": True,
        "retained": True,
        "reused": bool(reused),
        "lane": lane,
        "accountFingerprint": account_fingerprint,
        "accountLeaseScope": account_scope,
        "scopeHash": account.path.stem,
        "ownerPid": int(current["pid"]),
        "applicationInstanceLeaseHeld": True,
        "ownerIdentity": owner_identity,
    }


def hold_crypto_first_live_account_lease(
    lane: str,
    account_fingerprint: str,
) -> dict[str, object]:
    """Retain one Upbit/Binance account for the official app process.

    This cannot be used as a substitute for the global application-instance
    lease.  Both kernel handles must remain retained by the same authoritative
    process identity.
    """

    try:
        exact_lane, fingerprint, scope = _crypto_first_live_account_scope(
            lane, account_fingerprint
        )
    except ProcessSafetyAuthorityError as exc:
        return {"acquired": False, "reason": str(exc)}
    with _HELD_LEASES_LOCK:
        application, _current, reason = (
            _official_application_authority_locked()
        )
        if application is None:
            return {"acquired": False, "reason": reason}
        existing = _HELD_LEASES.get(scope.lower())
        reused = bool(
            existing is not None
            and not existing.handle.closed
            and existing.owner_pid == os.getpid()
        )
        acquired = hold_process_lease(scope)
        if acquired.get("acquired") is not True:
            return {
                **acquired,
                "reason": (
                    "crypto-first-live-account-owned-by-another-process"
                    if acquired.get("reason")
                    == "process-lease-owned-by-another-process"
                    else str(acquired.get("reason") or "account-lease-unavailable")
                ),
                "accountLeaseScope": scope,
            }
        return _crypto_account_status_locked(
            lane=exact_lane,
            account_fingerprint=fingerprint,
            account_scope=scope,
            reused=reused,
        )


def crypto_first_live_account_lease_status(
    lane: str,
    account_fingerprint: str,
) -> dict[str, object]:
    """Return authority only for a retained lease owned by this process."""

    try:
        exact_lane, fingerprint, scope = _crypto_first_live_account_scope(
            lane, account_fingerprint
        )
    except ProcessSafetyAuthorityError as exc:
        return {"acquired": False, "reason": str(exc)}
    with _HELD_LEASES_LOCK:
        return _crypto_account_status_locked(
            lane=exact_lane,
            account_fingerprint=fingerprint,
            account_scope=scope,
            reused=True,
        )


def crypto_first_live_owner_identity(
    lane: str,
    account_fingerprint: str,
) -> dict[str, object]:
    """Return exactly the five coordinator ownerIdentity fields."""

    status = crypto_first_live_account_lease_status(
        lane, account_fingerprint
    )
    if status.get("acquired") is not True:
        raise ProcessSafetyAuthorityError(
            str(status.get("reason") or "crypto-first-live-account-not-held")
        )
    identity = status.get("ownerIdentity")
    if not isinstance(identity, dict) or set(identity) != {
        "pid",
        "processStartEpoch",
        "bootId",
        "applicationLeaseEpoch",
        "accountLeaseScope",
    }:
        raise ProcessSafetyAuthorityError(
            "crypto-first-live-owner-identity-invalid"
        )
    return dict(identity)


def verify_crypto_first_live_owner_identity(
    request: Mapping[str, Any],
) -> bool:
    """Authoritative callback compatible with the common coordinator."""

    try:
        value = dict(request)
        if set(value) != {
            "schemaVersion",
            "purpose",
            "scope",
            "runId",
            "lane",
            "accountFingerprint",
            "ownerIdentity",
            "ownerIdentityHash",
            "ownerEpoch",
            "coordinatorRevision",
        }:
            return False
        if (
            value["schemaVersion"]
            != "crypto-first-live-owner-identity/v1"
            or value["scope"] != "CRYPTO_FIRST_LIVE_GLOBAL"
            or not str(value["purpose"] or "").strip()
            or int(value["ownerEpoch"]) < 0
            or int(value["coordinatorRevision"]) < 0
        ):
            return False
        current = crypto_first_live_owner_identity(
            str(value["lane"]), str(value["accountFingerprint"])
        )
        presented = value["ownerIdentity"]
        if not isinstance(presented, Mapping) or dict(presented) != current:
            return False
        canonical = json.dumps(
            current,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        expected_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return str(value["ownerIdentityHash"] or "") == expected_hash
    except (
        KeyError,
        TypeError,
        ValueError,
        ProcessSafetyAuthorityError,
    ):
        return False


def hold_kis_dispatch_lease(
    account_fingerprint: str,
    *,
    authority_id: str = "",
) -> dict[str, object]:
    """Own a KIS account and optional functional authority for this process.

    Account ownership is deliberately retained for the lifetime of the
    process.  A per-request mutex would only serialize duplicate executables;
    it would still let both submit sequentially.  Retained ownership makes the
    second process fail closed at its final POST boundary.
    """

    fingerprint = str(account_fingerprint or "").strip().lower()
    if not fingerprint:
        fingerprint = "unconfigured"
    account = hold_process_lease(f"live-trader:kis-account:{fingerprint}")
    if account.get("acquired") is not True:
        return {**account, "kind": "kis-account"}
    authority = str(authority_id or "").strip().lower()
    if authority:
        authority_status = hold_process_lease(
            f"live-trader:functional-authority:{authority}"
        )
        if authority_status.get("acquired") is not True:
            return {**authority_status, "kind": "functional-authority"}
    return {
        "acquired": True,
        "kind": "kis-account-and-authority" if authority else "kis-account",
        "accountScopeHash": account.get("scopeHash"),
        "authorityBound": bool(authority),
        "ownerPid": os.getpid(),
    }


def release_held_leases_for_tests() -> None:
    """Release process-retained handles. Only deterministic tests use this."""

    with _HELD_LEASES_LOCK:
        leases = list(_HELD_LEASES.values())
        _HELD_LEASES.clear()
    for lease in reversed(leases):
        try:
            lease.release()
        except OSError:
            pass
