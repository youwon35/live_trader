from __future__ import annotations

"""One final mutation boundary shared by every ordinary Upbit route."""

from contextlib import contextmanager
import threading
from typing import Any, Callable, Iterator, Mapping

from .emergency_stop import emergency_stop_dispatch_boundary


class UpbitOrderAuthorityError(RuntimeError):
    pass


class OwnedRLock:
    """Project-owned reentrant lock with portable ownership tracking."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._local = threading.local()

    def acquire(
        self,
        blocking: bool = True,
        timeout: float = -1,
    ) -> bool:
        acquired = self._lock.acquire(blocking, timeout)
        if acquired:
            self._local.depth = int(
                getattr(self._local, "depth", 0)
            ) + 1
        return acquired

    def release(self) -> None:
        self._lock.release()
        depth = int(getattr(self._local, "depth", 0)) - 1
        if depth > 0:
            self._local.depth = depth
        elif hasattr(self._local, "depth"):
            del self._local.depth

    def owned_by_current_thread(self) -> bool:
        return int(getattr(self._local, "depth", 0)) > 0

    def __enter__(self) -> "OwnedRLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc_value: Any,
        traceback: Any,
    ) -> bool:
        self.release()
        return False


# This is the only in-process route lock for ordinary and functional Upbit
# mutations.  It is public so the state graph can use the same project-owned
# ownership proof without relying on CPython's private RLock APIs.
UPBIT_ROUTE_AUTHORITY_LOCK = OwnedRLock()

_PROVIDER_LOCK = threading.RLock()
_THREAD_BOUNDARY = threading.local()
_AUTHORITY_READER: Callable[[], Mapping[str, Any]] | None = None


def register_upbit_order_authority_reader(
    reader: Callable[[], Mapping[str, Any]],
) -> None:
    """Register the single state-owned durable authority reader."""

    if not callable(reader):
        raise UpbitOrderAuthorityError("Upbit authority reader is invalid")
    global _AUTHORITY_READER
    with _PROVIDER_LOCK:
        if _AUTHORITY_READER is not None and _AUTHORITY_READER is not reader:
            raise UpbitOrderAuthorityError(
                "Upbit authority reader is already owned by another state graph"
            )
        _AUTHORITY_READER = reader


def _snapshot() -> dict[str, Any]:
    with _PROVIDER_LOCK:
        reader = _AUTHORITY_READER
    if reader is None:
        raise UpbitOrderAuthorityError(
            "state-owned Upbit authority reader is not registered"
        )
    try:
        value = dict(reader())
    except Exception as exc:
        raise UpbitOrderAuthorityError(
            "durable Upbit authority is unreadable"
        ) from exc
    required_booleans = (
        "functionalAuthorityOpen",
        "applicationInstanceLeaseHeld",
        "ordinaryMutationEnabled",
        "ordinaryRoutesClosed",
    )
    if any(type(value.get(name)) is not bool for name in required_booleans):
        raise UpbitOrderAuthorityError(
            "durable Upbit authority snapshot is incomplete"
        )
    return value


def _ordinary_snapshot() -> dict[str, Any]:
    snapshot = _snapshot()
    if snapshot["functionalAuthorityOpen"] is not False:
        raise UpbitOrderAuthorityError(
            "upbit-functional-authority-blocks-ordinary-mutation"
        )
    if snapshot["applicationInstanceLeaseHeld"] is not True:
        raise UpbitOrderAuthorityError(
            "ordinary-upbit-mutation-application-lease-required"
        )
    if snapshot["ordinaryMutationEnabled"] is not True:
        raise UpbitOrderAuthorityError(
            "ordinary-upbit-mutation-real-orders-disabled"
        )
    if snapshot["ordinaryRoutesClosed"] is not False:
        raise UpbitOrderAuthorityError(
            "ordinary-upbit-routes-closed"
        )
    return snapshot


def upbit_functional_authority_open_fail_closed() -> bool:
    try:
        return _snapshot().get("functionalAuthorityOpen") is not False
    except Exception:
        return True


@contextmanager
def upbit_route_authority_serialization() -> Iterator[None]:
    """Serialize activation/STOP/Kill with every final Upbit send."""

    with UPBIT_ROUTE_AUTHORITY_LOCK:
        yield


@contextmanager
def ordinary_upbit_final_mutation_boundary(
    *, operation: str,
) -> Iterator[Mapping[str, Any]]:
    """Retain route -> emergency through one immediate ordinary send."""

    inherited = getattr(_THREAD_BOUNDARY, "ordinary_snapshot", None)
    if isinstance(inherited, Mapping):
        # State owns route -> emergency across its final risk check.  The
        # router reuses that exact same-thread lease, but still rereads the
        # durable state immediately before its physical send.
        final_snapshot = _ordinary_snapshot()
        yield {**dict(inherited), **final_snapshot}
        return

    with UPBIT_ROUTE_AUTHORITY_LOCK:
        # Preserve the historical fail-before-emergency behavior for an
        # already-active functional pointer, then recheck after taking the
        # emergency boundary to close activation and Kill races.
        _ordinary_snapshot()
        with emergency_stop_dispatch_boundary() as emergency:
            if emergency.get("active") is True:
                raise UpbitOrderAuthorityError(
                    f"ordinary Upbit {operation} blocked by durable emergency stop"
                )
            final_snapshot = _ordinary_snapshot()
            composite = {
                **final_snapshot,
                **dict(emergency),
                "boundaryOwner": "UPBIT_SHARED_ROUTE",
            }
            _THREAD_BOUNDARY.ordinary_snapshot = composite
            try:
                yield composite
            finally:
                try:
                    del _THREAD_BOUNDARY.ordinary_snapshot
                except AttributeError:
                    pass


def _reset_upbit_order_authority_reader_for_tests() -> None:
    global _AUTHORITY_READER
    with _PROVIDER_LOCK:
        _AUTHORITY_READER = None
    try:
        del _THREAD_BOUNDARY.ordinary_snapshot
    except AttributeError:
        pass


__all__ = [
    "OwnedRLock",
    "UPBIT_ROUTE_AUTHORITY_LOCK",
    "UpbitOrderAuthorityError",
    "ordinary_upbit_final_mutation_boundary",
    "register_upbit_order_authority_reader",
    "upbit_functional_authority_open_fail_closed",
    "upbit_route_authority_serialization",
]
