from __future__ import annotations

"""Backend-owned lifecycle loop for the Binance Spot functional lane.

The loop has no bar or signal argument.  It renews the short owner lease on a
deadline independent from 5-minute evaluation polling.  The lifecycle manager
itself only renews after fresh official/private-stream truth, so a quiet but
healthy socket is acceptable only when its verified inbound liveness proof is
fresh.  Any heartbeat failure stops entry authority before cleanup is driven.
"""

import threading
import time
from typing import Any, Callable, Mapping

from .binance_spot_functional_lifecycle import (
    BinanceSpotFunctionalLifecycleManager,
    BinanceSpotLifecycleError,
    LifecycleHandle,
)


class BinanceSpotFunctionalSchedulerError(RuntimeError):
    pass


class BinanceSpotFunctionalManagedScheduler:
    """One owner process loop with independently scheduled heartbeats/ticks."""

    def __init__(
        self,
        *,
        manager: BinanceSpotFunctionalLifecycleManager,
        clock: Callable[[], float] = time.time,
        wait: Callable[[float], None] = time.sleep,
        heartbeat_interval_seconds: float = 20.0,
        market_poll_interval_seconds: float = 5.0,
    ) -> None:
        heartbeat_interval = float(heartbeat_interval_seconds)
        market_poll_interval = float(market_poll_interval_seconds)
        if heartbeat_interval <= 0 or heartbeat_interval > 20:
            raise BinanceSpotFunctionalSchedulerError(
                "owner heartbeat interval must be within 20 seconds"
            )
        if market_poll_interval <= 0 or market_poll_interval > 30:
            raise BinanceSpotFunctionalSchedulerError(
                "market/risk polling interval must be within 30 seconds"
            )
        self.manager = manager
        self.clock = clock
        self.wait = wait
        self.heartbeat_interval_seconds = heartbeat_interval
        self.market_poll_interval_seconds = market_poll_interval

    def _verified_cleanup_phase(
        self,
        handle: LifecycleHandle,
        *,
        before_revision: int | None = None,
        transition: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Verify the exact durable owner epoch before cleanup can advance."""

        fresh = dict(self.manager.status())
        try:
            revision = int(fresh.get("revision"))
        except (TypeError, ValueError):
            revision = -1
        phase = str(fresh.get("phase") or "").upper()
        session_id = str(
            fresh.get("sessionId") or fresh.get("session_id") or ""
        )
        owner_id = str(
            fresh.get("ownerId") or fresh.get("owner_id") or ""
        )
        if (
            phase != "CLEANUP"
            or session_id != handle.session_id
            or owner_id != handle.owner_id
            or revision < 1
            or (
                before_revision is not None
                and revision <= int(before_revision)
            )
        ):
            raise BinanceSpotLifecycleError(
                "operator stop CLEANUP owner epoch is not durably verified"
            )
        if transition is not None:
            try:
                transition_revision = int(transition.get("revision"))
            except (TypeError, ValueError):
                transition_revision = -1
            if (
                str(transition.get("phase") or "").upper() != phase
                or str(
                    transition.get("sessionId")
                    or transition.get("session_id")
                    or ""
                )
                != session_id
                or str(
                    transition.get("ownerId")
                    or transition.get("owner_id")
                    or ""
                )
                != owner_id
                or transition_revision != revision
            ):
                raise BinanceSpotLifecycleError(
                    "operator stop transition differs from durable owner epoch"
                )
        control = getattr(self.manager, "control", None)
        verifier = getattr(control, "verify_handle", None)
        if callable(verifier):
            verified = dict(verifier(handle))
            if (
                str(verified.get("phase") or "").upper() != phase
                or str(verified.get("session_id") or "") != session_id
                or str(verified.get("owner_id") or "") != owner_id
                or int(verified.get("revision") or -1) != revision
            ):
                raise BinanceSpotLifecycleError(
                    "operator stop owner-token verification changed epoch"
                )
        elif not bool(getattr(self.manager, "allow_mock_lifecycle", False)):
            raise BinanceSpotLifecycleError(
                "production cleanup owner-token verifier is unavailable"
            )
        return fresh

    def run(
        self,
        handle: LifecycleHandle,
        *,
        stop_event: threading.Event | None = None,
        max_iterations: int | None = None,
    ) -> dict[str, Any]:
        """Run until terminal, deadline, owner loss, or an operator stop.

        ``max_iterations`` exists only to bound deterministic tests.  It never
        relaxes a permit, extends a lease, or accepts a caller-created signal.
        """

        if max_iterations is not None and max_iterations <= 0:
            raise BinanceSpotFunctionalSchedulerError(
                "max_iterations must be positive"
            )
        if max_iterations is not None and not bool(
            getattr(self.manager, "allow_mock_lifecycle", False)
        ):
            raise BinanceSpotFunctionalSchedulerError(
                "max_iterations is test-only and unavailable in production"
            )
        next_heartbeat = float(self.clock())
        next_market_poll = float(self.clock())
        iterations = 0
        last_tick: dict[str, Any] | None = None
        operator_cleanup_latched = False
        owner_failure_detail = ""
        cleanup_barrier_reason = ""
        while True:
            iterations += 1
            if max_iterations is not None and iterations > max_iterations:
                before = dict(self.manager.status())
                try:
                    before_revision = int(before.get("revision"))
                except (TypeError, ValueError):
                    before_revision = None
                try:
                    transition = self.manager.begin_cleanup(
                        handle, reason="test iteration bound reached"
                    )
                    self._verified_cleanup_phase(
                        handle,
                        before_revision=before_revision,
                        transition=transition,
                    )
                except Exception as exc:
                    return {
                        "ok": False,
                        "status": (
                            "TEST_ITERATION_BOUND_CLEANUP_RETRY_REQUIRED"
                        ),
                        "detail": f"{type(exc).__name__}:{str(exc)[:200]}",
                        "iterations": iterations - 1,
                        "lastTick": last_tick,
                        "brokerSubmissionPerformed": False,
                    }
                return {
                    "ok": False,
                    "status": "TEST_ITERATION_BOUND_FORCED_CLEANUP",
                    "iterations": iterations - 1,
                    "lastTick": last_tick,
                }
            now = float(self.clock())
            status = self.manager.status()
            phase = str(status.get("phase") or "").upper()
            if phase in {"FINALIZED", "FAILED"}:
                return {
                    "ok": phase == "FINALIZED",
                    "status": phase,
                    "iterations": iterations,
                    "lastTick": last_tick,
                }
            if phase == "FINAL_RESET":
                # A transient failure can occur after mutation authority has
                # already been durably hidden and the final evidence prepared
                # (stream retirement, approval consume, or core/control seal).
                # Never call tick with that stale handle.  Resume the exact
                # immutable final-reset record in-process; repeated failures
                # remain fenced in FINAL_RESET and are safe to retry without
                # another broker mutation.
                resume = getattr(self.manager, "resume_final_reset", None)
                if not callable(resume):
                    return {
                        "ok": False,
                        "status": "FINAL_RESET_RECONCILIATION_REQUIRED",
                        "iterations": iterations,
                        "lastTick": last_tick,
                    }
                try:
                    resumed = dict(resume(session_id=handle.session_id))
                except Exception as exc:
                    last_tick = {
                        "ok": False,
                        "status": "FINAL_RESET_RETRY_PENDING",
                        "detail": f"{type(exc).__name__}:{str(exc)[:200]}",
                    }
                    self.wait(self.market_poll_interval_seconds)
                    continue
                resumed_phase = str(
                    self.manager.status().get("phase") or ""
                ).upper()
                if resumed_phase != "FINALIZED":
                    last_tick = {
                        "ok": False,
                        "status": "FINAL_RESET_RETRY_PENDING",
                        "detail": "final-reset resume did not reach FINALIZED",
                    }
                    self.wait(self.market_poll_interval_seconds)
                    continue
                return {
                    "ok": True,
                    "status": "FINALIZED",
                    "iterations": iterations,
                    "lastTick": last_tick,
                    "final": resumed,
                    "ownerFailure": owner_failure_detail,
                    "resumedFinalReset": True,
                }
            if now >= float(handle.cleanup_deadline_epoch):
                revoked = self.manager.fail_cleanup_deadline(
                    handle,
                    reason=(
                        "managed scheduler cleanup deadline reached; "
                        "manual reconciliation required"
                    ),
                )
                return {
                    "ok": False,
                    "status": "RECONCILIATION_REQUIRED",
                    "iterations": iterations,
                    "lastTick": last_tick,
                    "authority": revoked,
                }
            operator_stop_requested = bool(
                stop_event is not None and stop_event.is_set()
            )
            if operator_stop_requested or cleanup_barrier_reason:
                if not operator_cleanup_latched:
                    cleanup_verified = False
                    if phase == "CLEANUP":
                        try:
                            self._verified_cleanup_phase(handle)
                        except Exception as exc:
                            cleanup_error = exc
                        else:
                            cleanup_verified = True
                    else:
                        try:
                            before_revision = int(status.get("revision"))
                        except (TypeError, ValueError):
                            before_revision = None
                        transition: Mapping[str, Any] | None = None
                        try:
                            transition = self.manager.begin_cleanup(
                                handle,
                                reason=(
                                    "managed scheduler operator stop"
                                    if operator_stop_requested
                                    else cleanup_barrier_reason
                                ),
                            )
                        except Exception as exc:
                            cleanup_error = exc
                        try:
                            self._verified_cleanup_phase(
                                handle,
                                before_revision=before_revision,
                                transition=transition,
                            )
                        except Exception as exc:
                            cleanup_error = exc
                        else:
                            cleanup_verified = True
                    if not cleanup_verified:
                        # STOP is a strict dispatch barrier.  Do not heartbeat,
                        # tick, evaluate, or dispatch while the durable
                        # owner/session CLEANUP transition is unverified.
                        last_tick = {
                            "ok": False,
                            "status": (
                                "STOP_CLEANUP_LATCH_RETRY"
                                if operator_stop_requested
                                else "TICK_CLEANUP_LATCH_RETRY"
                            ),
                            "detail": (
                                f"{type(cleanup_error).__name__}:"
                                f"{str(cleanup_error)[:200]}"
                            ),
                            "entryRetryAttempted": False,
                            "brokerSubmissionPerformed": False,
                        }
                        next_heartbeat = float("inf")
                        self.wait(self.market_poll_interval_seconds)
                        continue
                    operator_cleanup_latched = True
                    cleanup_barrier_reason = ""
                    phase = "CLEANUP"
                    next_heartbeat = float("inf")
                    next_market_poll = min(next_market_poll, now)
            if now >= float(handle.expires_epoch):
                # Entry expiry is a normal lifecycle transition, not an owner
                # heartbeat failure.  Let tick's durable authority snapshot
                # demote to cleanup before any attempted lease renewal.
                next_market_poll = min(next_market_poll, now)
            if (
                phase == "ACTIVE"
                and now < float(handle.expires_epoch)
                and now >= next_heartbeat
            ):
                try:
                    self.manager.heartbeat(handle)
                except Exception as exc:
                    # heartbeat() durably latches CLEANUP before raising.  Keep
                    # this same fenced owner alive through every subsequent
                    # cancel/partial/residual generation and final seal; one
                    # cleanup tick is not a terminal condition.
                    owner_failure_detail = (
                        f"{type(exc).__name__}:{str(exc)[:200]}"
                    )
                    durable_phase = str(
                        self.manager.status().get("phase") or ""
                    ).upper()
                    if durable_phase != "CLEANUP":
                        return {
                            "ok": False,
                            "status": "RECONCILIATION_REQUIRED",
                            "detail": owner_failure_detail,
                            "iterations": iterations,
                            "lastTick": last_tick,
                            "entryRetryAttempted": False,
                        }
                    last_tick = {
                        "ok": False,
                        "status": "OWNER_HEARTBEAT_LOST_CLEANUP_ONLY",
                        "detail": owner_failure_detail,
                    }
                    operator_cleanup_latched = True
                    phase = "CLEANUP"
                    next_market_poll = min(next_market_poll, now)
                    next_heartbeat = float("inf")
                else:
                    next_heartbeat = now + self.heartbeat_interval_seconds
            if now >= next_market_poll:
                if phase == "CLEANUP":
                    selector = getattr(
                        self.manager, "next_due_ambiguous_claim", None
                    )
                    prover = getattr(
                        self.manager, "prove_ambiguous_not_accepted", None
                    )
                    if callable(selector) and callable(prover):
                        try:
                            ambiguous = selector(handle)
                            if ambiguous is not None:
                                proof = dict(
                                    prover(
                                        handle,
                                        claim_id=str(ambiguous["claimId"]),
                                    )
                                )
                                last_tick = {
                                    "ok": True,
                                    "status": proof.get("status"),
                                    "ambiguousProof": proof,
                                    "retryAttempted": False,
                                }
                        except Exception as exc:
                            failer = getattr(
                                self.manager,
                                "fail_ambiguous_reconciliation",
                                None,
                            )
                            authority = (
                                failer(
                                    handle,
                                    reason=f"{type(exc).__name__}:{str(exc)[:200]}",
                                )
                                if callable(failer)
                                else None
                            )
                            return {
                                "ok": False,
                                "status": "RECONCILIATION_REQUIRED",
                                "iterations": iterations,
                                "lastTick": last_tick,
                                "authority": authority,
                                "retryAttempted": False,
                            }
                try:
                    last_tick = dict(self.manager.tick(handle))
                except Exception as exc:
                    if getattr(exc, "transient_market_data", False) is True:
                        last_tick = {
                            "ok": True,
                            "status": "TRANSIENT_MARKET_DATA_RETRY",
                            "detail": f"{type(exc).__name__}:{str(exc)[:200]}",
                        }
                        next_market_poll = (
                            now + self.market_poll_interval_seconds
                        )
                        continue
                    cleanup_barrier_reason = (
                        "managed scheduler tick/evaluator failed:"
                        f"{type(exc).__name__}"
                    )
                    # A generic tick/evaluator fault is a strict barrier.
                    # The next loop must durably prove the same owner/session
                    # CLEANUP epoch before heartbeat, tick, or dispatch can
                    # run again.  A failed CAS remains retry-only here.
                    next_heartbeat = float("inf")
                    last_tick = {
                        "ok": False,
                        "status": "TICK_FAILED_CLEANUP_RETRY_REQUIRED",
                        "detail": f"{type(exc).__name__}:{str(exc)[:200]}",
                        "entryRetryAttempted": False,
                        "brokerSubmissionPerformed": False,
                    }
                    next_market_poll = now + self.market_poll_interval_seconds
                    self.wait(self.market_poll_interval_seconds)
                    continue
                next_market_poll = now + self.market_poll_interval_seconds
                phase = str(self.manager.status().get("phase") or "").upper()
                if phase == "CLEANUP" and last_tick.get("claim") is None:
                    try:
                        finalized = self.manager.finalize(handle)
                    except Exception as exc:
                        # A working/ambiguous action or owned residual remains.
                        # If finalize crossed the durable FINAL_RESET boundary,
                        # the next loop resumes it before any further tick.
                        # Otherwise continue bounded cleanup polling; no blind
                        # submit is performed by finalize.
                        finalized = None
                        if str(
                            self.manager.status().get("phase") or ""
                        ).upper() == "FINAL_RESET":
                            last_tick = {
                                "ok": False,
                                "status": "FINAL_RESET_RETRY_PENDING",
                                "detail": (
                                    f"{type(exc).__name__}:{str(exc)[:200]}"
                                ),
                            }
                    if finalized is not None:
                        return {
                            "ok": True,
                            "status": "FINALIZED",
                            "iterations": iterations,
                            "lastTick": last_tick,
                            "final": finalized,
                            "ownerFailure": owner_failure_detail,
                        }
            wake_at = min(next_heartbeat, next_market_poll)
            if phase != "ACTIVE":
                wake_at = next_market_poll
            delay = max(0.001, min(20.0, wake_at - float(self.clock())))
            self.wait(delay)


__all__ = [
    "BinanceSpotFunctionalManagedScheduler",
    "BinanceSpotFunctionalSchedulerError",
]
