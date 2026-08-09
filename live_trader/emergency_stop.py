from __future__ import annotations

import json
import hashlib
import os
import secrets
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EMERGENCY_STOP_SCHEMA_VERSION = 1
_WRITE_LOCK = threading.RLock()
_DISPATCH_BOUNDARY_LOCK = threading.RLock()
_STICKY_FAIL_CLOSED_REASON = ""
_STICKY_ACTIVE_REVISION = ""


def _utc_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def emergency_stop_path() -> Path:
    configured = str(os.getenv("LIVE_TRADER_EMERGENCY_STOP_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    local = str(os.getenv("LOCALAPPDATA") or "").strip()
    base = Path(local).expanduser() if local else Path(tempfile.gettempdir())
    return (
        base
        / "trading-system"
        / "live-trader"
        / "control"
        / "emergency-stop.json"
    )


def _initialized_marker_path() -> Path:
    path = emergency_stop_path()
    return path.with_name(path.name + ".initialized")


def _ensure_initialized_marker() -> None:
    marker = _initialized_marker_path()
    marker.parent.mkdir(parents=True, exist_ok=True)
    if marker.exists():
        return
    try:
        with marker.open("x", encoding="ascii", newline="\n") as handle:
            handle.write("live-trader-emergency-stop-initialized-v1\n")
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        return


def _document(active: bool, *, reason: str, source: str) -> dict[str, Any]:
    return {
        "schemaVersion": EMERGENCY_STOP_SCHEMA_VERSION,
        "active": bool(active),
        "updatedAt": _utc_text(),
        "reason": str(reason or "operator emergency stop")[:500],
        "source": str(source or "unknown")[:120],
        "writerPid": os.getpid(),
        "nonce": secrets.token_hex(16),
    }


def _write_atomic(payload: dict[str, Any]) -> None:
    path = emergency_stop_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_initialized_marker()
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.{secrets.token_hex(6)}.tmp"
    )
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _path_presence(path: Path) -> tuple[bool, OSError | None]:
    """Distinguish an authoritative ENOENT from an unreadable path.

    ``Path.exists()`` intentionally collapses several OS errors to ``False``
    on recent Python versions.  That is unsafe for a Kill latch because a
    permission or storage error must never look like an authoritative OFF.
    ``lstat`` also makes a broken/suspicious symlink proceed to the guarded
    read path instead of being treated as an absent file.
    """

    try:
        path.lstat()
    except FileNotFoundError:
        return False, None
    except OSError as exc:
        return False, exc
    return True, None


def _durable_emergency_stop_status() -> dict[str, Any]:
    path = emergency_stop_path()
    present, presence_error = _path_presence(path)
    if presence_error is not None:
        return {
            "active": True,
            "durable": False,
            "status": "fail-closed",
            "path": str(path),
            "reason": (
                "emergency-stop-state-presence-unavailable:"
                f"{type(presence_error).__name__}"
            ),
            "source": "latch-reader",
            "updatedAt": "",
            "revision": "unavailable",
        }
    if not present:
        marker_present, marker_error = _path_presence(
            _initialized_marker_path()
        )
        if marker_error is not None:
            return {
                "active": True,
                "durable": False,
                "status": "fail-closed",
                "path": str(path),
                "reason": (
                    "emergency-stop-marker-presence-unavailable:"
                    f"{type(marker_error).__name__}"
                ),
                "source": "latch-reader",
                "updatedAt": "",
                "revision": "unavailable",
            }
        initialized = marker_present
        return {
            "active": initialized,
            "durable": not initialized,
            "status": "fail-closed-missing" if initialized else "clear",
            "path": str(path),
            "reason": (
                "emergency-stop-state-missing-after-initialization"
                if initialized
                else ""
            ),
            "source": "latch-reader" if initialized else "",
            "updatedAt": "",
            "revision": "initialized-missing" if initialized else "missing",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("emergency stop document must be an object")
        if payload.get("schemaVersion") != EMERGENCY_STOP_SCHEMA_VERSION:
            raise ValueError("unsupported emergency stop schema")
        if not isinstance(payload.get("active"), bool):
            raise ValueError("emergency stop active flag is invalid")
        nonce = str(payload.get("nonce") or "")
        if not nonce:
            raise ValueError("emergency stop nonce is invalid")
        return {
            "active": payload["active"],
            "durable": True,
            "status": "engaged" if payload["active"] else "clear",
            "path": str(path),
            "reason": str(payload.get("reason") or ""),
            "source": str(payload.get("source") or ""),
            "updatedAt": str(payload.get("updatedAt") or ""),
            "revision": hashlib.sha256(nonce.encode("utf-8")).hexdigest(),
        }
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        return {
            "active": True,
            "durable": False,
            "status": "fail-closed",
            "path": str(path),
            "reason": f"emergency-stop-state-unavailable:{type(exc).__name__}",
            "source": "latch-reader",
            "updatedAt": "",
            "revision": "unavailable",
        }


def _set_sticky_fail_closed(reason: str) -> None:
    global _STICKY_FAIL_CLOSED_REASON
    _STICKY_FAIL_CLOSED_REASON = str(reason or "emergency-stop-write-failed")[:500]


def emergency_stop_status() -> dict[str, Any]:
    """Read the independent durable latch and process-local sticky failure.

    A missing document normally means no local emergency has ever been
    engaged. If an ON write failed, however, missing must never be interpreted
    as OFF by the broker boundary in the same process. That sticky fail-closed
    state survives every read and successful ON rewrite; only a confirmed,
    durably verified OFF write may clear it.
    """

    global _STICKY_ACTIVE_REVISION
    with _WRITE_LOCK:
        durable = _durable_emergency_stop_status()
        if durable.get("active") is True and durable.get("durable") is True:
            _STICKY_ACTIVE_REVISION = str(durable.get("revision") or "")
        if _STICKY_FAIL_CLOSED_REASON:
            return {
                **durable,
                "active": True,
                "durable": False,
                "status": "sticky-fail-closed",
                "reason": _STICKY_FAIL_CLOSED_REASON,
                "source": "latch-writer",
            }
        if _STICKY_ACTIVE_REVISION and durable.get("active") is not True:
            return {
                **durable,
                "active": True,
                "durable": False,
                "status": "sticky-active-state-missing",
                "reason": "emergency-stop-active-document-disappeared",
                "source": "latch-observer",
                "durableRevision": str(durable.get("revision") or ""),
                "revision": _STICKY_ACTIVE_REVISION,
            }
        return durable


def emergency_stop_active() -> bool:
    return emergency_stop_status().get("active") is True


def engage_emergency_stop(
    reason: str = "operator emergency stop",
    *,
    source: str = "live-trader",
) -> dict[str, Any]:
    """Atomically latch Kill ON. This path never offers a release operation."""

    with _WRITE_LOCK:
        try:
            _write_atomic(_document(True, reason=reason, source=source))
        except OSError as exc:
            failure_reason = f"emergency-stop-write-failed:{type(exc).__name__}"
            _set_sticky_fail_closed(failure_reason)
            result = {
                "ok": False,
                "active": True,
                "durable": False,
                "status": "write-failed-local-fail-closed",
                "reason": failure_reason,
                "path": str(emergency_stop_path()),
            }
        else:
            status = emergency_stop_status()
            if (
                status.get("active") is not True
                or status.get("durable") is not True
            ):
                _set_sticky_fail_closed(
                    "emergency-stop-write-verification-failed"
                )
                result = {
                    "ok": False,
                    **status,
                    "reason": "emergency-stop-write-verification-failed",
                }
            else:
                result = {"ok": True, **status}
    # Establish a linearization point with the final broker boundary. The
    # durable ON write happens first; returning waits only for an order that
    # had already entered the irreversible broker call.
    with _DISPATCH_BOUNDARY_LOCK:
        return result


def clear_emergency_stop(
    *,
    confirmed: bool,
    reason: str = "operator confirmed API release",
    source: str = "live-trader-api",
    expected_revision: str = "",
) -> dict[str, Any]:
    """Clear only through the authenticated/confirmed Python API boundary."""

    if confirmed is not True:
        return {
            "ok": False,
            "active": True,
            "durable": True,
            "status": "release-blocked",
            "reason": "emergency-stop-release-confirmation-required",
            "path": str(emergency_stop_path()),
        }
    with _WRITE_LOCK:
        current = emergency_stop_status()
        expected = str(expected_revision or "").strip()
        if expected and not secrets.compare_digest(
            expected,
            str(current.get("revision") or ""),
        ):
            return {
                "ok": False,
                **current,
                "active": True,
                "reason": "emergency-stop-release-revision-changed",
            }
        try:
            _write_atomic(_document(False, reason=reason, source=source))
        except OSError as exc:
            failure_reason = f"emergency-stop-release-failed:{type(exc).__name__}"
            _set_sticky_fail_closed(failure_reason)
            return {
                "ok": False,
                "active": True,
                "durable": False,
                "status": "release-write-failed",
                "reason": failure_reason,
                "path": str(emergency_stop_path()),
            }
        # Verify the document without consulting the sticky state; otherwise a
        # previous write failure could never be safely released.
        status = _durable_emergency_stop_status()
        if status.get("active") is not False or status.get("durable") is not True:
            _set_sticky_fail_closed("emergency-stop-release-verification-failed")
            return {
                "ok": False,
                **status,
                "reason": "emergency-stop-release-verification-failed",
            }
        global _STICKY_FAIL_CLOSED_REASON
        global _STICKY_ACTIVE_REVISION
        _STICKY_FAIL_CLOSED_REASON = ""
        _STICKY_ACTIVE_REVISION = ""
        return {"ok": True, **status}


@contextmanager
def emergency_stop_dispatch_boundary():
    """Serialize the final latch check with the irreversible broker call."""

    with _DISPATCH_BOUNDARY_LOCK:
        yield emergency_stop_status()


def _reset_emergency_stop_sticky_for_tests() -> None:
    """Reset process-only state between isolated tests.

    Production release must go through :func:`clear_emergency_stop`; this
    helper is deliberately private and must never be exposed to the UI bridge.
    """

    global _STICKY_FAIL_CLOSED_REASON
    global _STICKY_ACTIVE_REVISION
    with _WRITE_LOCK:
        _STICKY_FAIL_CLOSED_REASON = ""
        _STICKY_ACTIVE_REVISION = ""


class DesktopEmergencyStopBridge:
    """Tiny pywebview-native bridge kept independent of the HTTP API thread."""

    def engage_emergency_stop(self, reason: str = "desktop emergency stop") -> dict[str, Any]:
        return engage_emergency_stop(reason, source="pywebview-native-bridge")

    def emergency_stop_status(self) -> dict[str, Any]:
        return emergency_stop_status()
