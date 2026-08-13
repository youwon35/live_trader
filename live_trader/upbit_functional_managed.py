from __future__ import annotations

"""Single-owner managed lifecycle for the Upbit continuous functional lane.

The application server may expose this controller only through a dedicated
functional-test command.  It deliberately has no generic mode toggle and no
access to the legacy Upbit smoke route.  Global new-entry protection remains
latched throughout; only the service's per-session capability can reach POST.
"""

from datetime import datetime, timezone
import threading
from typing import Any, Callable, Mapping

from .upbit_continuous_functional import (
    UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE,
    UpbitContinuousFunctionalService,
    UpbitFunctionalBlocked,
)


class ManagedUpbitFunctionalController:
    def __init__(
        self,
        *,
        enter_cleanup_latch: Callable[[], None],
        disarm_real_orders: Callable[[], None],
        clock: Callable[[], datetime],
        clear_runtime_capability: Callable[[], None] | None = None,
    ) -> None:
        self._enter_cleanup_latch = enter_cleanup_latch
        self._disarm_real_orders = disarm_real_orders
        self._clear_runtime_capability = clear_runtime_capability or (lambda: None)
        self._clock = clock
        self._lock = threading.RLock()
        self._service: UpbitContinuousFunctionalService | None = None
        self._status = "STOPPED"
        self._reason = ""
        self._failed_session_id = ""
        self._failed_permit_id = ""

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            service = self._service
            session = (
                service.ledger.session(service.session_id)
                if service is not None
                else {}
            )
            return {
                "available": UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE,
                "status": self._status,
                "reason": self._reason,
                "sessionId": (
                    service.session_id if service else self._failed_session_id
                ),
                "permitId": (
                    service.scope.permit_id if service else self._failed_permit_id
                ),
                "endsAt": (
                    service.scope.ends_at.isoformat().replace("+00:00", "Z")
                    if service
                    else ""
                ),
                "cleanupDeadline": (
                    service.scope.cleanup_deadline.isoformat().replace(
                        "+00:00", "Z"
                    )
                    if service
                    else ""
                ),
                "durableState": str(session.get("state") or ""),
                "newEntriesBlocked": True,
                "ordinaryRoutesClosed": True,
                "upbitSmokeRouteClosed": True,
            }

    def start(self, **activation: Any) -> dict[str, Any]:
        """Production start; availability remains false until full E2E passes."""

        with self._lock:
            if self._service is not None or self._status not in {
                "STOPPED",
                "FINALIZED",
                "FAILED_CLOSED",
            }:
                raise UpbitFunctionalBlocked(
                    "upbit-managed-controller-already-owned"
                )
            service = UpbitContinuousFunctionalService.activate(**activation)
            self._service = service
            self._status = "ACTIVE"
            self._reason = ""
            self._failed_session_id = ""
            self._failed_permit_id = ""
            return self.snapshot()

    def fail_closed_after_start(self, *, reason: str) -> dict[str, Any]:
        """Compensate a failure after durable activation but before arming.

        The session is moved to cleanup, the durable mutation capability is
        revoked, and the in-memory service is detached.  Recovery therefore
        requires the explicit cleanup-only reattachment path with a rotated
        capability; a partially armed start can never continue as ACTIVE.
        """

        with self._lock:
            service = self._assert_owner()
            self._status = "CLEANUP"
            self._reason = reason
            self._failed_session_id = service.session_id
            self._failed_permit_id = service.scope.permit_id
            first_error: Exception | None = None
            for reduction in (
                self._enter_cleanup_latch,
                self._disarm_real_orders,
                lambda: service.fail_closed_revoke(reason=reason),
            ):
                try:
                    reduction()
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
            try:
                self._clear_runtime_capability()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
            self._service = None
            self._status = "FAILED_CLOSED"
            if first_error is not None:
                raise first_error
            return self.snapshot()

    def _attach_for_test(
        self,
        service: UpbitContinuousFunctionalService,
    ) -> dict[str, Any]:
        with self._lock:
            if self._service is not None:
                raise UpbitFunctionalBlocked(
                    "upbit-managed-controller-already-owned"
                )
            self._service = service
            self._status = "ACTIVE"
            return self.snapshot()

    def attach_cleanup_recovery(
        self,
        service: UpbitContinuousFunctionalService,
    ) -> dict[str, Any]:
        """Attach a service already reauthenticated as cleanup-only."""

        with self._lock:
            if self._service is not None:
                raise UpbitFunctionalBlocked(
                    "upbit-managed-controller-already-owned"
                )
            durable = service.ledger.session(service.session_id)
            if durable["state"] != "CLEANUP":
                raise UpbitFunctionalBlocked(
                    "upbit-managed-recovery-cleanup-state-required"
                )
            self._service = service
            self._status = "CLEANUP"
            self._reason = "restart-cleanup-recovery"
            return self.snapshot()

    def resume_cleanup(self) -> dict[str, Any]:
        with self._lock:
            self._assert_owner()
            if self._status != "CLEANUP":
                raise UpbitFunctionalBlocked(
                    "upbit-managed-recovery-cleanup-status-required"
                )
            return self._drain_locked(reason=self._reason)

    def _assert_owner(self) -> UpbitContinuousFunctionalService:
        service = self._service
        if service is None:
            raise UpbitFunctionalBlocked("upbit-managed-session-not-running")
        # The controller lock serializes scheduler/server threads.  Broker
        # mutation authority remains the raw per-session capability held only
        # by the attached service; OS thread identity is not a durable owner.
        return service

    def on_finalized_bar(self, bar: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            service = self._assert_owner()
            if self._status != "ACTIVE":
                raise UpbitFunctionalBlocked(
                    "upbit-managed-new-signal-not-active"
                )
            try:
                result = service.on_bar(bar)
            except Exception as exc:
                durable = service.ledger.session(service.session_id)
                if durable["state"] == "CLEANUP":
                    return self._drain_locked(reason=str(exc))
                raise
            if result.get("action") == "CLEANUP":
                return self._drain_locked(reason=str(result.get("reason") or "expiry"))
            return {"ok": True, "result": result, "snapshot": self.snapshot()}

    def monitor_once(self) -> dict[str, Any]:
        with self._lock:
            service = self._assert_owner()
            now = self._clock()
            if now.tzinfo is None or now.utcoffset() is None:
                raise UpbitFunctionalBlocked(
                    "upbit-managed-clock-timezone-missing"
                )
            if self._status == "CLEANUP":
                return self._cleanup_tick_locked()
            if (
                self._status == "ACTIVE"
                and now.astimezone(timezone.utc) >= service.scope.ends_at
            ):
                return self._drain_locked(reason="permit-expired")
            if self._status == "ACTIVE":
                risk = service.assess_risk()
                if risk.get("action") == "CLEANUP":
                    return self._drain_locked(
                        reason=str(risk.get("reason") or "risk-limit-reached")
                    )
            return {"ok": True, "snapshot": self.snapshot()}

    def stop_and_cleanup(self, *, reason: str = "operator-stop") -> dict[str, Any]:
        with self._lock:
            self._assert_owner()
            return self._drain_locked(reason=reason)

    def _drain_locked(self, *, reason: str) -> dict[str, Any]:
        service = self._assert_owner()
        self._status = "CLEANUP"
        self._reason = reason
        service.recover_or_expire(reason=reason)
        self._enter_cleanup_latch()
        return self._cleanup_tick_locked()

    def _cleanup_tick_locked(self) -> dict[str, Any]:
        """Advance bounded cleanup without assuming immediate broker fills.

        A scheduler may call ``monitor_once`` repeatedly until the official
        truth reports no owned working order and no session-owned delta.  Each
        tick performs only the finite actions returned by the durable cleanup
        planner; a working/partial order returns PENDING instead of attempting
        finalization or blindly submitting a replacement.
        """

        service = self._assert_owner()
        now = self._clock()
        if now.astimezone(timezone.utc) >= service.scope.cleanup_deadline:
            self._disarm_real_orders()
            service.fail_closed_revoke(reason="cleanup-deadline-expired")
            self._status = "FAILED_CLOSED"
            self._reason = "cleanup-deadline-expired-manual-intervention-required"
            return {
                "ok": False,
                "pending": False,
                "manualInterventionRequired": True,
                "snapshot": self.snapshot(),
            }
        actions: list[str] = []
        try:
            for _index in range(6):
                plan = service.cleanup_plan()
                pending = plan.get("actions")
                if not isinstance(pending, list) or not pending:
                    break
                if len(pending) != 1:
                    raise UpbitFunctionalBlocked(
                        "upbit-managed-cleanup-action-cardinality-invalid"
                    )
                action = pending[0]
                result = service.dispatch(action)
                actions.append(str(result.get("action") or ""))
            final_plan = service.cleanup_plan()
            if final_plan.get("actions"):
                return {
                    "ok": True,
                    "pending": True,
                    "actions": actions,
                    "plan": final_plan,
                    "snapshot": self.snapshot(),
                }
            if final_plan.get("readyToFinalize") is not True:
                return {
                    "ok": True,
                    "pending": True,
                    "actions": actions,
                    "plan": final_plan,
                    "snapshot": self.snapshot(),
                }
            self._disarm_real_orders()
            final = service.finalize_if_flat()
        except Exception as exc:
            self._disarm_real_orders()
            self._status = "FAILED_CLOSED"
            self._reason = f"{type(exc).__name__}:{exc}"
            raise
        self._status = "FINALIZED"
        return {
            "ok": True,
            "actions": actions,
            "final": final,
            "snapshot": self.snapshot(),
        }


__all__ = ["ManagedUpbitFunctionalController"]
