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
from typing import Any, Callable

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
        while True:
            iterations += 1
            if max_iterations is not None and iterations > max_iterations:
                try:
                    self.manager.begin_cleanup(
                        handle, reason="test iteration bound reached"
                    )
                except Exception:
                    pass
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
            if (
                stop_event is not None
                and stop_event.is_set()
                and not operator_cleanup_latched
            ):
                try:
                    self.manager.begin_cleanup(
                        handle, reason="managed scheduler operator stop"
                    )
                except BinanceSpotLifecycleError:
                    # A concurrently expired owner is handled by tick below.
                    pass
                operator_cleanup_latched = True
                phase = "CLEANUP"
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
                    try:
                        self.manager.begin_cleanup(
                            handle,
                            reason=(
                                "managed scheduler tick/evaluator failed:"
                                f"{type(exc).__name__}"
                            ),
                        )
                    except Exception:
                        # A concurrently changed/expired owner is handled by
                        # startup takeover; no new tick or entry is attempted
                        # before the next durable phase observation.
                        pass
                    last_tick = {
                        "ok": False,
                        "status": "TICK_FAILED_CLEANUP_ONLY",
                        "detail": f"{type(exc).__name__}:{str(exc)[:200]}",
                    }
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
