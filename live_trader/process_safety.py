from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import ctypes
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO


_HELD_LEASES_LOCK = threading.RLock()
_HELD_LEASES: dict[str, "CrossProcessLease"] = {}


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
    windows_mutex_handle: int | None = None

    def status(self, *, reused: bool = False) -> dict[str, object]:
        return {
            "acquired": True,
            "scopeHash": self.path.stem,
            "ownerPid": os.getpid(),
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
    acquired_at = _utc_text()
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
    return hold_process_lease("live-trader:application-instance:v1")


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
