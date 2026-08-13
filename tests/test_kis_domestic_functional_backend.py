from __future__ import annotations

import copy
import time
import unittest
from datetime import datetime, timedelta, timezone

from live_trader.kis_domestic_functional_backend import (
    KIS_DOMESTIC_FUNCTIONAL_BACKEND_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_BACKEND_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_BACKEND_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_BACKEND_RELEASE_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_BACKEND_STATE_SERVER_WIRED,
    KisDomesticFunctionalBackend,
    KisDomesticFunctionalBackendBlocked,
    backend_component_status,
)


COMPONENT_NAMES = (
    "graph",
    "source",
    "rolling",
    "heartbeat",
    "capability",
    "mutation",
    "quote",
    "terminal",
)
NOW = datetime(2026, 8, 14, 3, 0, tzinfo=timezone.utc)


class _Clock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        return self.value


class _Component:
    def __init__(
        self,
        name: str,
        *,
        state: str = "READY",
        ready: bool = True,
        cleanup_terminal: bool = True,
        fail_tick: bool = False,
        fail_latch: bool = False,
        tick_state: str = "",
        block_tick: bool = False,
        hazardous: bool = False,
        owned_working: bool = False,
        owned_position: bool = False,
    ) -> None:
        self.name = name
        self.state = state
        self.ready = ready
        self.cleanup_terminal = cleanup_terminal
        self.fail_tick = fail_tick
        self.fail_latch = fail_latch
        self.tick_state = tick_state
        self.block_tick = block_tick
        self.hazardous = hazardous
        self.owned_working = owned_working
        self.owned_position = owned_position
        self.cancel_observed = False
        self.tick_calls: list[str] = []
        self.latch_calls: list[tuple[str, str]] = []
        self.recover_calls: list[str] = []

    def status(self) -> dict:
        return {
            "schemaVersion": f"mock-{self.name}-status/v1",
            "name": self.name,
            "state": self.state,
            "ready": self.ready,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
            "hazardousAuthorityOpen": self.hazardous,
            "authorityOpen": False,
            "ownedWorkingExposurePresent": self.owned_working,
            "ownedPositionExposurePresent": self.owned_position,
        }

    def tick(
        self,
        *,
        now: str,
        cancel_event,
        deadline_monotonic: float,
    ) -> dict:
        self.tick_calls.append(now)
        if self.block_tick:
            self.cancel_observed = cancel_event.wait(1.0)
        if self.fail_tick:
            raise RuntimeError(f"{self.name}-tick-failed")
        if self.tick_state:
            self.state = self.tick_state
        return self.status()

    def latch_cleanup(
        self,
        *,
        reason: str,
        cleanup_deadline_at: str,
        cancel_event,
        deadline_monotonic: float,
    ) -> dict:
        self.latch_calls.append((reason, cleanup_deadline_at))
        if self.fail_latch:
            raise RuntimeError(f"{self.name}-latch-failed")
        self.state = "CLEANUP"
        return self.status()

    def recover_cleanup(
        self,
        *,
        now: str,
        cancel_event,
        deadline_monotonic: float,
    ) -> dict:
        self.recover_calls.append(now)
        if self.cleanup_terminal:
            self.state = "SAFE_INCOMPLETE"
        return self.status()


class _DormantThread:
    def __init__(self, *, target, args, name, daemon) -> None:
        self.target = target
        self.args = args
        self.name = name
        self.daemon = daemon
        self.started = False
        self.joined = False

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self.started and not self.joined

    def join(self, timeout=None) -> None:
        self.joined = True


class _SynchronousThread(_DormantThread):
    def start(self) -> None:
        self.started = True
        self.target(*self.args)
        self.joined = True


class _FailingThread(_DormantThread):
    def start(self) -> None:
        raise RuntimeError("thread-start-failed")


class _WaitSequence:
    def __init__(self, values: list[bool]) -> None:
        self.values = list(values)
        self.observed: list[float] = []

    def __call__(self, _event, seconds: float) -> bool:
        self.observed.append(seconds)
        return self.values.pop(0) if self.values else True


def _components(**changes) -> dict[str, _Component]:
    values = {name: _Component(name) for name in COMPONENT_NAMES}
    for name, fields in changes.items():
        for key, value in fields.items():
            setattr(values[name], key, value)
    return values


def _backend(
    *,
    owner_key: str = "kis-backend-test-owner",
    components=None,
    clock=None,
    thread_factory=_DormantThread,
    wait_function=None,
    cleanup_seconds: int = 30,
    cadence_seconds: float = 5.0,
    component_call_timeout_seconds: float = 2.0,
) -> KisDomesticFunctionalBackend:
    return KisDomesticFunctionalBackend(
        owner_key=owner_key,
        components=_components() if components is None else components,
        trusted_wall_clock=_Clock() if clock is None else clock,
        cadence_seconds=cadence_seconds,
        cleanup_seconds=cleanup_seconds,
        thread_factory=thread_factory,
        wait_function=wait_function,
        component_call_timeout_seconds=component_call_timeout_seconds,
    )


class KisDomesticFunctionalBackendTest(unittest.TestCase):
    def test_component_and_runtime_status_are_disabled_with_exact_hashes(self) -> None:
        component = backend_component_status()
        for value in (
            KIS_DOMESTIC_FUNCTIONAL_BACKEND_PRODUCTION_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_BACKEND_NETWORK_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_BACKEND_MUTATION_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_BACKEND_RELEASE_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_BACKEND_STATE_SERVER_WIRED,
            component["networkOrderPostAllowed"],
        ):
            self.assertFalse(value)
        backend = _backend()
        try:
            status = backend.status()
            self.assertEqual(set(COMPONENT_NAMES), set(status["componentStatusHashes"]))
            self.assertRegex(status["statusHash"], r"^[0-9a-f]{64}$")
            self.assertFalse(status["productionAvailable"])
            self.assertFalse(status["networkOrderPostAllowed"])
            self.assertEqual(0, status["tradingMutationCount"])
            self.assertIn(
                "G1_REAL_PRODUCER_LEDGER_CAS_NOT_WIRED",
                status["readinessBlockers"],
            )
            self.assertIn(
                "G4_VERIFY_ONLY_UPSTREAM_AUTHORITY_SEPARATION_NOT_WIRED",
                status["readinessBlockers"],
            )
        finally:
            backend.close()

    def test_exact_component_protocol_and_cadence_bounds_fail_closed(self) -> None:
        with self.assertRaisesRegex(
            KisDomesticFunctionalBackendBlocked, "components-not-exact"
        ):
            _backend(components={})
        for cadence in (0.0, 10.0, float("nan"), float("inf")):
            with self.subTest(cadence=cadence):
                with self.assertRaisesRegex(
                    KisDomesticFunctionalBackendBlocked, "cadence-invalid"
                ):
                    _backend(owner_key=f"bad-{cadence}", cadence_seconds=cadence)
        unsafe = _components()
        original_status = unsafe["graph"].status
        unsafe["graph"].status = lambda: {
            **original_status(), "productionAvailable": True
        }
        with self.assertRaisesRegex(
            KisDomesticFunctionalBackendBlocked, "status-contract-invalid"
        ):
            _backend(owner_key="unsafe-component", components=unsafe)

    def test_offline_start_scheduler_ticks_all_at_under_ten_second_cadence(self) -> None:
        components = _components()
        waits = _WaitSequence([False, True])
        backend = _backend(
            components=components,
            thread_factory=_SynchronousThread,
            wait_function=waits,
            cadence_seconds=9.0,
        )
        try:
            status = backend.start_offline_simulation()
            self.assertEqual("ACTIVE", status["state"])
            self.assertTrue(status["schedulerCadenceUnderTenSeconds"])
            self.assertEqual([9.0, 9.0], waits.observed)
            self.assertTrue(all(len(item.tick_calls) == 1 for item in components.values()))
            self.assertFalse(status["networkOrderPostAllowed"])
        finally:
            backend.close()

    def test_stop_is_two_phase_latch_then_owned_cleanup(self) -> None:
        components = _components()
        backend = _backend(components=components)
        try:
            backend.start_offline_simulation()
            latched = backend.request_stop()
            self.assertEqual("CLEANUP", latched["state"])
            self.assertTrue(all(len(item.latch_calls) == 1 for item in components.values()))
            self.assertTrue(all(not item.recover_calls for item in components.values()))
            terminal = backend.run_one_scheduler_cycle_for_test()
            self.assertEqual("SAFE_INCOMPLETE", terminal["state"])
            self.assertTrue(all(len(item.recover_calls) == 1 for item in components.values()))
        finally:
            backend.close()

    def test_kill_uses_same_two_phase_cleanup_and_never_posts(self) -> None:
        components = _components()
        backend = _backend(components=components)
        try:
            backend.start_offline_simulation()
            latched = backend.request_kill()
            self.assertEqual("CLEANUP", latched["state"])
            self.assertTrue(
                all(item.latch_calls[0][0] == "KILL" for item in components.values())
            )
            terminal = backend.run_one_scheduler_cycle_for_test()
            self.assertEqual("SAFE_INCOMPLETE", terminal["state"])
            self.assertFalse(terminal["networkOrderPostAllowed"])
            self.assertEqual(0, terminal["tradingMutationCount"])
        finally:
            backend.close()

    def test_cleanup_deadline_becomes_reconciliation_required(self) -> None:
        clock = _Clock()
        components = _components()
        for item in components.values():
            item.cleanup_terminal = False
        backend = _backend(
            components=components, clock=clock, cleanup_seconds=10
        )
        try:
            backend.start_offline_simulation()
            backend.request_stop()
            clock.value += timedelta(seconds=11)
            status = backend.run_one_scheduler_cycle_for_test()
            self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
            self.assertEqual("cleanup-deadline-expired", status["reason"])
        finally:
            backend.close()

    def test_component_reconciliation_cannot_be_mislabeled_safe_incomplete(self) -> None:
        components = _components()
        components["terminal"].cleanup_terminal = False
        original = components["terminal"].recover_cleanup

        def reconcile(
            *, now: str, cancel_event, deadline_monotonic: float
        ) -> dict:
            original(
                now=now,
                cancel_event=cancel_event,
                deadline_monotonic=deadline_monotonic,
            )
            components["terminal"].state = "RECONCILIATION_REQUIRED"
            return components["terminal"].status()

        components["terminal"].recover_cleanup = reconcile
        backend = _backend(components=components)
        try:
            backend.start_offline_simulation()
            backend.request_stop()
            status = backend.run_one_scheduler_cycle_for_test()
            self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
            self.assertEqual(
                "component-cleanup-reconciliation-required", status["reason"]
            )
        finally:
            backend.close()

    def test_scheduler_component_failure_latches_cleanup_and_retains_owner(self) -> None:
        components = _components(source={"fail_tick": True})
        waits = _WaitSequence([False, True])
        backend = _backend(
            components=components,
            thread_factory=_SynchronousThread,
            wait_function=waits,
        )
        try:
            status = backend.start_offline_simulation()
            self.assertEqual("CLEANUP", status["state"])
            self.assertTrue(all(len(item.latch_calls) == 1 for item in components.values()))
            self.assertFalse(status["networkOrderPostAllowed"])
        finally:
            backend.close()

    def test_thread_start_failure_never_strands_active_or_cleanup(self) -> None:
        components = _components()
        backend = _backend(components=components, thread_factory=_FailingThread)
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalBackendBlocked, "scheduler-thread-start-failed"
            ):
                backend.start_offline_simulation()
            status = backend.status()
            self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
            self.assertFalse(status["schedulerRunning"])
            self.assertTrue(all(len(item.latch_calls) == 1 for item in components.values()))
        finally:
            backend.close()

    def test_restart_owner_loss_latches_cleanup_and_singleton_blocks_second(self) -> None:
        components = _components(graph={"state": "ACTIVE"})
        first = _backend(components=components)
        try:
            status = first.status()
            self.assertEqual("CLEANUP", status["state"])
            self.assertTrue(all(len(item.latch_calls) == 1 for item in components.values()))
            with self.assertRaisesRegex(
                KisDomesticFunctionalBackendBlocked, "singleton-owner-active"
            ):
                _backend(owner_key="kis-backend-test-owner")
        finally:
            first.close()
        replacement = _backend(owner_key="kis-backend-test-owner")
        try:
            self.assertEqual("PREPARED", replacement.status()["state"])
        finally:
            replacement.close()

    def test_component_not_ready_blocks_offline_start_without_scheduler(self) -> None:
        components = _components(quote={"ready": False})
        backend = _backend(components=components)
        try:
            before = copy.deepcopy(backend.status())
            with self.assertRaisesRegex(
                KisDomesticFunctionalBackendBlocked,
                "COMPONENT_NOT_READY:quote",
            ):
                backend.start_offline_simulation()
            after = backend.status()
            self.assertEqual("PREPARED", after["state"])
            self.assertFalse(after["schedulerRunning"])
            self.assertEqual(before["componentStatusHashes"], after["componentStatusHashes"])
        finally:
            backend.close()


if __name__ == "__main__":
    unittest.main()
