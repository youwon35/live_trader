from __future__ import annotations

"""One final mutation boundary shared by every Binance order-capable route."""

from contextlib import contextmanager
import secrets
import threading
from typing import Any, Callable, Iterator, Mapping

from .emergency_stop import emergency_stop_dispatch_boundary


class BinanceOrderAuthorityError(RuntimeError):
    pass


_ROUTE_LOCK = threading.RLock()
_PROVIDER_LOCK = threading.RLock()
_THREAD_BOUNDARY = threading.local()
_AUTHORITY_READER: Callable[[], Mapping[str, Any]] | None = None


def register_binance_order_authority_reader(
    reader: Callable[[], Mapping[str, Any]],
) -> None:
    """Register the single state-owned durable authority reader."""

    if not callable(reader):
        raise BinanceOrderAuthorityError("Binance authority reader is invalid")
    global _AUTHORITY_READER
    with _PROVIDER_LOCK:
        if _AUTHORITY_READER is not None and _AUTHORITY_READER is not reader:
            raise BinanceOrderAuthorityError(
                "Binance authority reader is already owned by another state graph"
            )
        _AUTHORITY_READER = reader


def _snapshot() -> dict[str, Any]:
    with _PROVIDER_LOCK:
        reader = _AUTHORITY_READER
    if reader is None:
        raise BinanceOrderAuthorityError(
            "state-owned Binance authority reader is not registered"
        )
    try:
        value = dict(reader())
    except Exception as exc:
        raise BinanceOrderAuthorityError(
            "durable Binance authority is unreadable"
        ) from exc
    required = {
        "functionalAuthorityOpen",
        "functionalPhase",
        "functionalRevision",
        "functionalSessionId",
        "functionalAccountFingerprint",
        "applicationInstanceLeaseHeld",
        "ordinaryRoutesClosed",
    }
    if not required.issubset(value):
        raise BinanceOrderAuthorityError(
            "durable Binance authority snapshot is incomplete"
        )
    return value


def binance_functional_authority_open_fail_closed() -> bool:
    try:
        authority_open = _snapshot().get("functionalAuthorityOpen")
        if type(authority_open) is not bool:
            raise BinanceOrderAuthorityError(
                "durable Binance authority state is not boolean"
            )
        return authority_open
    except Exception:
        return True


@contextmanager
def binance_route_authority_serialization() -> Iterator[None]:
    """Serialize start/stop/reverse-route changes with final broker sends."""

    with _ROUTE_LOCK:
        yield


@contextmanager
def ordinary_binance_final_mutation_boundary(
    *, operation: str,
) -> Iterator[Mapping[str, Any]]:
    """Fail ordinary Spot/Futures mutation when functional authority exists."""

    inherited = getattr(_THREAD_BOUNDARY, "ordinary_snapshot", None)
    if isinstance(inherited, Mapping):
        # A state-owned composite operation (notably Futures fill-soak) may
        # already retain route -> emergency through the immediate adapter
        # send.  Reuse that unforgeable thread-local lease so the adapter does
        # not reacquire either boundary in a different order.
        yield inherited
        return

    with _ROUTE_LOCK:
        with emergency_stop_dispatch_boundary() as emergency:
            if emergency.get("active") is True:
                raise BinanceOrderAuthorityError(
                    f"Binance {operation} blocked by durable emergency stop"
                )
            snapshot = _snapshot()
            if snapshot.get("applicationInstanceLeaseHeld") is not True:
                raise BinanceOrderAuthorityError(
                    "Binance mutation requires the official application lease"
                )
            if snapshot.get("functionalAuthorityOpen") is not False:
                raise BinanceOrderAuthorityError(
                    "ordinary Binance mutation blocked by functional authority"
                )
            _THREAD_BOUNDARY.ordinary_snapshot = snapshot
            try:
                yield snapshot
            finally:
                try:
                    del _THREAD_BOUNDARY.ordinary_snapshot
                except AttributeError:
                    pass


@contextmanager
def functional_binance_final_mutation_boundary(
    *,
    session_id: str,
    cleanup_only: bool,
    expected_revision: object | None = None,
) -> Iterator[Callable[[], Mapping[str, Any]]]:
    """Bind functional POST to phase/revision and serialize STOP/Kill races."""

    with _ROUTE_LOCK:
        with emergency_stop_dispatch_boundary() as emergency:
            if emergency.get("active") is True and not cleanup_only:
                raise BinanceOrderAuthorityError(
                    "functional entry blocked by durable emergency stop"
                )

            def read() -> Mapping[str, Any]:
                snapshot = _snapshot()
                phase = str(snapshot.get("functionalPhase") or "").upper()
                expected_phases = (
                    {"CLEANUP"}
                    if cleanup_only
                    else {"ACTIVE"}
                )
                actual_revision = int(snapshot.get("functionalRevision") or 0)
                expected_revision_text = str(expected_revision or "")
                expected_revision_number = (
                    int(expected_revision_text.rsplit("-", 1)[-1])
                    if expected_revision_text
                    else None
                )
                matches = bool(
                    snapshot.get("functionalAuthorityOpen") is True
                    and snapshot.get("applicationInstanceLeaseHeld") is True
                    and secrets.compare_digest(
                        str(snapshot.get("functionalSessionId") or ""),
                        str(session_id or ""),
                    )
                    and phase in expected_phases
                    and (
                        expected_revision_number is None
                        or actual_revision == expected_revision_number
                    )
                    and not (
                        emergency.get("active") is True and not cleanup_only
                    )
                )
                return {
                    **snapshot,
                    "active": matches,
                    "sessionId": str(session_id or ""),
                    "cleanupOnly": bool(cleanup_only),
                    "emergencyRevision": str(emergency.get("revision") or ""),
                    "emergencyActive": emergency.get("active") is True,
                }

            initial = read()
            if initial.get("active") is not True:
                raise BinanceOrderAuthorityError(
                    "functional Binance phase/session/revision authority changed"
                )
            yield read


def _reset_binance_order_authority_reader_for_tests() -> None:
    global _AUTHORITY_READER
    with _PROVIDER_LOCK:
        _AUTHORITY_READER = None


__all__ = [
    "BinanceOrderAuthorityError",
    "binance_functional_authority_open_fail_closed",
    "binance_route_authority_serialization",
    "functional_binance_final_mutation_boundary",
    "ordinary_binance_final_mutation_boundary",
    "register_binance_order_authority_reader",
]
