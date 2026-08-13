from __future__ import annotations

import hashlib
import inspect
import json
import math
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

from .kis_domestic_functional_contract import PDNO, ROUTE


KIS_DOMESTIC_FUNCTIONAL_BACKEND_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_BACKEND_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_BACKEND_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_BACKEND_RELEASE_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_BACKEND_STATE_SERVER_WIRED = False

STATUS_SCHEMA = "kis-domestic-functional-backend-status/v1"
MIN_CADENCE_SECONDS = 0.05
MAX_CADENCE_SECONDS = 9.0
DEFAULT_CLEANUP_SECONDS = 900
DEFAULT_COMPONENT_CALL_TIMEOUT_SECONDS = 2.0
_TERMINAL_STATES = {"FINALIZED", "SAFE_INCOMPLETE", "RECONCILIATION_REQUIRED"}
_SAFE_CLEANUP_STATES = {"FINALIZED", "SAFE_INCOMPLETE"}
_COMPONENTS = (
    "graph",
    "source",
    "rolling",
    "heartbeat",
    "capability",
    "mutation",
    "quote",
    "terminal",
)


class KisDomesticFunctionalBackendBlocked(RuntimeError):
    pass


@runtime_checkable
class KisDomesticFunctionalBackendComponent(Protocol):
    def status(self) -> Mapping[str, Any]: ...

    def tick(
        self,
        *,
        now: str,
        cancel_event: threading.Event,
        deadline_monotonic: float,
    ) -> Mapping[str, Any]: ...

    def latch_cleanup(
        self,
        *,
        reason: str,
        cleanup_deadline_at: str,
        cancel_event: threading.Event,
        deadline_monotonic: float,
    ) -> Mapping[str, Any]: ...

    def recover_cleanup(
        self,
        *,
        now: str,
        cancel_event: threading.Event,
        deadline_monotonic: float,
    ) -> Mapping[str, Any]: ...


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalBackendBlocked("backend-json-invalid") from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _time(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalBackendBlocked(f"{label}-invalid")
    converted = value.astimezone(timezone.utc)
    if not math.isfinite(converted.timestamp()):
        raise KisDomesticFunctionalBackendBlocked(f"{label}-invalid")
    return converted


def _time_text(value: datetime, label: str) -> str:
    return _time(value, label).isoformat().replace("+00:00", "Z")


_OWNER_LOCK = threading.RLock()
_LIVE_OWNERS: dict[str, str] = {}


class KisDomesticFunctionalBackend:
    """Offline-only lifecycle owner for injected KIS functional components.

    It deliberately has no sender, credential, token, state, or server handle.
    All component calls are orchestration-only and every external authority bit
    remains false even while this in-process state machine is ACTIVE.
    """

    def __init__(
        self,
        *,
        owner_key: str,
        components: Mapping[str, KisDomesticFunctionalBackendComponent],
        trusted_wall_clock: Callable[[], datetime],
        cadence_seconds: float = 5.0,
        cleanup_seconds: int = DEFAULT_CLEANUP_SECONDS,
        thread_factory: Callable[..., Any] = threading.Thread,
        wait_function: Callable[[threading.Event, float], bool] | None = None,
        component_call_timeout_seconds: float = (
            DEFAULT_COMPONENT_CALL_TIMEOUT_SECONDS
        ),
    ) -> None:
        if type(owner_key) is not str or not owner_key.strip():
            raise KisDomesticFunctionalBackendBlocked("backend-owner-key-invalid")
        if set(components) != set(_COMPONENTS):
            raise KisDomesticFunctionalBackendBlocked("backend-components-not-exact")
        for name, component in components.items():
            if not isinstance(component, KisDomesticFunctionalBackendComponent):
                raise KisDomesticFunctionalBackendBlocked(
                    f"backend-component-interface-invalid:{name}"
                )
        if (
            type(cadence_seconds) not in (float, int)
            or not math.isfinite(float(cadence_seconds))
            or not MIN_CADENCE_SECONDS <= float(cadence_seconds) <= MAX_CADENCE_SECONDS
        ):
            raise KisDomesticFunctionalBackendBlocked("backend-cadence-invalid")
        if type(cleanup_seconds) is not int or not 1 <= cleanup_seconds <= 3600:
            raise KisDomesticFunctionalBackendBlocked("backend-cleanup-seconds-invalid")
        if (
            type(component_call_timeout_seconds) not in (float, int)
            or not math.isfinite(float(component_call_timeout_seconds))
            or not 0.01 <= float(component_call_timeout_seconds) <= 10.0
        ):
            raise KisDomesticFunctionalBackendBlocked(
                "backend-component-call-timeout-invalid"
            )
        if not callable(trusted_wall_clock) or not callable(thread_factory):
            raise KisDomesticFunctionalBackendBlocked("backend-callable-invalid")
        self.owner_key = owner_key.strip()
        self.components = dict(components)
        self.clock = trusted_wall_clock
        self.cadence_seconds = float(cadence_seconds)
        self.cleanup_seconds = cleanup_seconds
        self.component_call_timeout_seconds = float(
            component_call_timeout_seconds
        )
        self.thread_factory = thread_factory
        self.wait_function = wait_function or (lambda event, seconds: event.wait(seconds))
        self.owner_token = uuid.uuid4().hex
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Any = None
        self._generation = 0
        self._state = "PREPARED"
        self._reason = ""
        self._cleanup_deadline: datetime | None = None
        self._scheduler_error = ""
        self._closed = False
        self._last_trusted_now: datetime | None = None
        self._component_hashes: dict[str, str] = {}
        self._component_interface_hashes: dict[str, str] = {}
        self._hazardous_components: tuple[str, ...] = ()
        self._timed_out_component_calls: set[str] = set()
        self._detached_component_calls: set[str] = set()
        with _OWNER_LOCK:
            if self.owner_key in _LIVE_OWNERS:
                raise KisDomesticFunctionalBackendBlocked("backend-singleton-owner-active")
            _LIVE_OWNERS[self.owner_key] = self.owner_token
        try:
            self._refresh_components_locked()
            self._audit_restart_locked()
        except BaseException:
            self.close()
            raise

    def _assert_owner(self) -> None:
        if self._closed:
            raise KisDomesticFunctionalBackendBlocked("backend-owner-closed")
        with _OWNER_LOCK:
            if _LIVE_OWNERS.get(self.owner_key) != self.owner_token:
                raise KisDomesticFunctionalBackendBlocked("backend-owner-fence-lost")

    def _now(self) -> datetime:
        value = _time(self.clock(), "backend-trusted-now")
        self._last_trusted_now = value
        return value

    @staticmethod
    def _component_interface(name: str, component: Any) -> dict[str, Any]:
        expected = {
            "status": (),
            "tick": ("now", "cancel_event", "deadline_monotonic"),
            "latch_cleanup": (
                "reason",
                "cleanup_deadline_at",
                "cancel_event",
                "deadline_monotonic",
            ),
            "recover_cleanup": ("now", "cancel_event", "deadline_monotonic"),
        }
        methods: dict[str, Any] = {}
        for method_name, expected_names in expected.items():
            method = getattr(component, method_name, None)
            if not callable(method):
                raise KisDomesticFunctionalBackendBlocked(
                    f"backend-component-interface-invalid:{name}:{method_name}"
                )
            try:
                parameters = tuple(inspect.signature(method).parameters.values())
            except (TypeError, ValueError) as exc:
                raise KisDomesticFunctionalBackendBlocked(
                    f"backend-component-interface-uninspectable:{name}:{method_name}"
                ) from exc
            if tuple(item.name for item in parameters) != expected_names:
                raise KisDomesticFunctionalBackendBlocked(
                    f"backend-component-interface-parameters-invalid:{name}:{method_name}"
                )
            if method_name != "status" and any(
                item.kind is not inspect.Parameter.KEYWORD_ONLY
                for item in parameters
            ):
                raise KisDomesticFunctionalBackendBlocked(
                    f"backend-component-interface-kinds-invalid:{name}:{method_name}"
                )
            methods[method_name] = {
                "parameters": [
                    {"name": item.name, "kind": item.kind.name}
                    for item in parameters
                ]
            }
        return {
            "schemaVersion": "kis-domestic-functional-backend-component-interface/v2",
            "component": name,
            "methods": methods,
            "cooperativeCancellationRequired": True,
        }

    def _component_call(self, name: str, method: str, **kwargs: Any) -> dict[str, Any]:
        component = self.components[name]
        finished = threading.Event()
        cancel_event = threading.Event()
        output: dict[str, Any] = {}
        call_name = f"{name}:{method}"
        if method != "status":
            kwargs = {
                **kwargs,
                "cancel_event": cancel_event,
                "deadline_monotonic": (
                    time.monotonic() + self.component_call_timeout_seconds
                ),
            }

        def invoke() -> None:
            try:
                output["value"] = getattr(component, method)(**kwargs)
            except BaseException as exc:  # captured for the owning caller
                output["error"] = exc
            finally:
                finished.set()

        worker = threading.Thread(
            target=invoke,
            name=f"kis-functional-component-{name}-{method}",
            daemon=True,
        )
        worker.start()
        if not finished.wait(self.component_call_timeout_seconds):
            cancel_event.set()
            self._timed_out_component_calls.add(call_name)
            if not finished.is_set():
                self._detached_component_calls.add(call_name)
            raise KisDomesticFunctionalBackendBlocked(
                f"backend-component-{name}-{method}-deadline-exceeded"
            )
        if "error" in output:
            exc = output["error"]
            raise KisDomesticFunctionalBackendBlocked(
                f"backend-component-{name}-{method}-failed:{type(exc).__name__}"
            ) from exc
        value = output.get("value")
        if not isinstance(value, Mapping):
            raise KisDomesticFunctionalBackendBlocked(
                f"backend-component-{name}-{method}-not-object"
            )
        return dict(value)

    def _refresh_components_locked(self) -> dict[str, dict[str, Any]]:
        snapshots: dict[str, dict[str, Any]] = {}
        hazardous: list[str] = []
        for name in _COMPONENTS:
            interface = self._component_interface(name, self.components[name])
            self._component_interface_hashes[name] = _hash(interface)
            value = self._component_call(name, "status")
            if (
                type(value.get("schemaVersion")) is not str
                or not value.get("schemaVersion")
                or type(value.get("state")) is not str
                or not value.get("state")
                or type(value.get("ready")) is not bool
                or type(value.get("productionAvailable")) is not bool
                or value.get("productionAvailable") is not False
                or type(value.get("networkAvailable")) is not bool
                or value.get("networkAvailable") is not False
                or type(value.get("mutationAvailable")) is not bool
                or value.get("mutationAvailable") is not False
                or type(value.get("releaseAvailable")) is not bool
                or value.get("releaseAvailable") is not False
                or type(value.get("networkOrderPostAllowed")) is not bool
                or value.get("networkOrderPostAllowed") is not False
                or type(value.get("tradingMutationCount")) is not int
                or value.get("tradingMutationCount") != 0
                or type(value.get("hazardousAuthorityOpen")) is not bool
                or type(value.get("authorityOpen")) is not bool
                or type(value.get("ownedWorkingExposurePresent")) is not bool
                or type(value.get("ownedPositionExposurePresent")) is not bool
            ):
                raise KisDomesticFunctionalBackendBlocked(
                    f"backend-component-{name}-status-contract-invalid"
                )
            snapshots[name] = value
            self._component_hashes[name] = _hash(value)
            if (
                self._state_of(value) in {"ACTIVE", "CLEANUP", "CLEANUP_ONLY"}
                or value.get("hazardousAuthorityOpen") is True
                or value.get("authorityOpen") is True
                or value.get("ownedWorkingExposurePresent") is True
                or value.get("ownedPositionExposurePresent") is True
                or any(item.startswith(f"{name}:") for item in self._detached_component_calls)
            ):
                hazardous.append(name)
        self._hazardous_components = tuple(sorted(hazardous))
        return snapshots

    @staticmethod
    def _state_of(value: Mapping[str, Any]) -> str:
        state = value.get("state")
        return state if type(state) is str else "UNKNOWN"

    def _audit_restart_locked(self) -> None:
        self._refresh_components_locked()
        hazardous = list(self._hazardous_components)
        if not hazardous:
            return
        now = self._now()
        self._state = "CLEANUP"
        self._reason = "PROCESS_OR_SINGLETON_OWNER_RESTART"
        self._cleanup_deadline = now + timedelta(seconds=self.cleanup_seconds)
        failures = self._latch_all_cleanup_locked(reason=self._reason, now=now)
        if failures:
            self._set_reconciliation_locked(
                "restart-cleanup-latch-failed:" + ",".join(failures)
            )
            return
        self._start_scheduler_or_reconcile_locked()

    def _latch_all_cleanup_locked(self, *, reason: str, now: datetime) -> list[str]:
        deadline = self._cleanup_deadline or (
            now + timedelta(seconds=self.cleanup_seconds)
        )
        deadline_text = _time_text(deadline, "cleanup-deadline")
        failures: list[str] = []
        for name in _COMPONENTS:
            try:
                self._component_call(
                    name,
                    "latch_cleanup",
                    reason=reason,
                    cleanup_deadline_at=deadline_text,
                )
            except KisDomesticFunctionalBackendBlocked:
                failures.append(name)
        return failures

    def start(self) -> dict[str, Any]:
        self._assert_owner()
        with self._lock:
            snapshots = self._refresh_components_locked()
            blockers = self._readiness_blockers(snapshots)
            raise KisDomesticFunctionalBackendBlocked(
                "backend-production-start-blocked:" + ",".join(blockers)
            )

    def start_offline_simulation(self) -> dict[str, Any]:
        self._assert_owner()
        with self._lock:
            if self._state != "PREPARED":
                raise KisDomesticFunctionalBackendBlocked("backend-not-prepared")
            snapshots = self._refresh_components_locked()
            component_blockers = self._component_readiness_blockers(snapshots)
            if component_blockers:
                raise KisDomesticFunctionalBackendBlocked(
                    "backend-readiness-blocked:" + ",".join(component_blockers)
                )
            self._now()
            self._state = "ACTIVE"
            self._reason = ""
            self._generation += 1
            self._start_scheduler_or_reconcile_locked()
            return self._status_locked(snapshots=self._refresh_components_locked())

    def _readiness_blockers(
        self, snapshots: Mapping[str, Mapping[str, Any]]
    ) -> list[str]:
        blockers = [
            "G1_REAL_PRODUCER_LEDGER_CAS_NOT_WIRED",
            "G2_CROSS_PROCESS_OS_DURABLE_EPOCH_LEASE_NOT_WIRED",
            "G3_INDEPENDENT_GRAPH_VERIFIER_ORPHAN_UNION_NOT_WIRED",
            "G4_VERIFY_ONLY_UPSTREAM_AUTHORITY_SEPARATION_NOT_WIRED",
        ]
        return blockers + self._component_readiness_blockers(snapshots)

    @staticmethod
    def _component_readiness_blockers(
        snapshots: Mapping[str, Mapping[str, Any]]
    ) -> list[str]:
        return [
            f"COMPONENT_NOT_READY:{name}"
            for name, status in snapshots.items()
            if type(status.get("ready")) is not bool or status.get("ready") is not True
        ]

    def _start_scheduler_locked(self) -> None:
        self._stop_event.clear()
        thread = self.thread_factory(
            target=self._scheduler_loop,
            args=(self._generation,),
            name=f"kis-functional-backend-{self._generation}",
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def _start_scheduler_or_reconcile_locked(self) -> None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            try:
                self._start_scheduler_locked()
                return
            except BaseException as exc:
                last_error = exc
                self._thread = None
        try:
            now = self._now()
        except BaseException as exc:
            now = self._last_trusted_now
            if now is None:
                self._set_reconciliation_locked(
                    "scheduler-thread-start-clock-failed:" + type(exc).__name__
                )
                raise KisDomesticFunctionalBackendBlocked(
                    self._reason
                ) from last_error
        if self._state != "CLEANUP":
            self._state = "CLEANUP"
            self._reason = "SCHEDULER_THREAD_START_FAILED"
            self._cleanup_deadline = now + timedelta(seconds=self.cleanup_seconds)
            failures = self._latch_all_cleanup_locked(reason=self._reason, now=now)
        else:
            failures = []
        detail = "scheduler-thread-start-failed"
        if failures:
            detail += ":cleanup-latch-failed:" + ",".join(failures)
        if last_error is not None:
            detail += ":" + type(last_error).__name__
        self._set_reconciliation_locked(detail)
        raise KisDomesticFunctionalBackendBlocked(detail) from last_error

    def _set_reconciliation_locked(self, detail: str) -> None:
        self._state = "RECONCILIATION_REQUIRED"
        self._reason = detail
        self._scheduler_error = detail
        self._stop_event.set()

    def _scheduler_supervisor_failure_locked(self, detail: str) -> None:
        failures: list[str] = []
        if self._state == "ACTIVE":
            now = self._last_trusted_now
            if now is not None:
                self._state = "CLEANUP"
                self._reason = "SCHEDULER_SUPERVISOR_FAILURE"
                self._cleanup_deadline = now + timedelta(
                    seconds=self.cleanup_seconds
                )
                failures = self._latch_all_cleanup_locked(
                    reason=self._reason,
                    now=now,
                )
        if failures:
            detail += ":cleanup-latch-failed:" + ",".join(failures)
        self._set_reconciliation_locked(detail)

    def _scheduler_loop(self, generation: int) -> None:
        try:
            while True:
                try:
                    should_stop = self.wait_function(
                        self._stop_event, self.cadence_seconds
                    )
                except BaseException as exc:
                    with self._lock:
                        self._scheduler_supervisor_failure_locked(
                            "scheduler-wait-failed:" + type(exc).__name__
                        )
                    return
                if type(should_stop) is not bool:
                    with self._lock:
                        self._scheduler_supervisor_failure_locked(
                            "scheduler-wait-result-invalid"
                        )
                    return
                if should_stop:
                    return
                with self._lock:
                    if generation != self._generation or self._state in _TERMINAL_STATES:
                        return
                    try:
                        now = self._now()
                    except BaseException as exc:
                        self._scheduler_supervisor_failure_locked(
                            "scheduler-clock-failed:" + type(exc).__name__
                        )
                        return
                    try:
                        if self._state == "ACTIVE":
                            results = {
                                name: self._component_call(
                                    name, "tick", now=_time_text(now, "scheduler-now")
                                )
                                for name in _COMPONENTS
                            }
                        elif self._state == "CLEANUP":
                            results = {
                                name: self._component_call(
                                    name,
                                    "recover_cleanup",
                                    now=_time_text(now, "cleanup-now"),
                                )
                                for name in _COMPONENTS
                            }
                        else:
                            return
                    except KisDomesticFunctionalBackendBlocked as exc:
                        self._scheduler_failure_locked(exc, now)
                        continue
                    result_states = [self._state_of(value) for value in results.values()]
                    if self._state == "ACTIVE":
                        if any(
                            state in {"RECONCILIATION_REQUIRED", "CLEANUP", "CLEANUP_ONLY"}
                            for state in result_states
                        ):
                            self._state = "CLEANUP"
                            self._reason = "COMPONENT_REQUESTED_CLEANUP"
                            self._cleanup_deadline = now + timedelta(
                                seconds=self.cleanup_seconds
                            )
                            failures = self._latch_all_cleanup_locked(
                                reason=self._reason, now=now
                            )
                            if failures:
                                self._set_reconciliation_locked(
                                    "tick-cleanup-latch-failed:" + ",".join(failures)
                                )
                        elif any(state in _SAFE_CLEANUP_STATES for state in result_states):
                            self._set_reconciliation_locked(
                                "active-component-terminalized-without-cleanup-latch"
                            )
                    if self._state == "CLEANUP":
                        if any(state == "RECONCILIATION_REQUIRED" for state in result_states):
                            self._set_reconciliation_locked(
                                "component-cleanup-reconciliation-required"
                            )
                        elif all(state in _SAFE_CLEANUP_STATES for state in result_states):
                            self._state = "SAFE_INCOMPLETE"
                            self._reason = "OWNED_CLEANUP_COMPLETE_OFFLINE"
                            self._stop_event.set()
                        elif self._cleanup_deadline is not None and now >= self._cleanup_deadline:
                            self._set_reconciliation_locked("cleanup-deadline-expired")
        except BaseException as exc:
            with self._lock:
                self._scheduler_supervisor_failure_locked(
                    "scheduler-outer-escape:" + type(exc).__name__
                )

    def _scheduler_failure_locked(
        self, error: KisDomesticFunctionalBackendBlocked, now: datetime
    ) -> None:
        if self._state != "CLEANUP":
            self._state = "CLEANUP"
            self._reason = "SCHEDULER_COMPONENT_FAILURE"
            self._cleanup_deadline = now + timedelta(seconds=self.cleanup_seconds)
            failures = self._latch_all_cleanup_locked(reason=self._reason, now=now)
            if failures:
                self._set_reconciliation_locked(
                    "scheduler-cleanup-latch-failed:" + ",".join(failures)
                )
                return
        self._scheduler_error = str(error)

    def request_stop(self, *, reason: str = "OPERATOR_STOP") -> dict[str, Any]:
        return self._request_cleanup(reason=reason)

    def request_kill(self) -> dict[str, Any]:
        return self._request_cleanup(reason="KILL")

    def _request_cleanup(self, *, reason: str) -> dict[str, Any]:
        self._assert_owner()
        if type(reason) is not str or not reason:
            raise KisDomesticFunctionalBackendBlocked("cleanup-reason-invalid")
        with self._lock:
            if self._state in _TERMINAL_STATES:
                return self._status_locked(snapshots=self._refresh_components_locked())
            now = self._now()
            self._state = "CLEANUP"
            self._reason = reason
            self._cleanup_deadline = now + timedelta(seconds=self.cleanup_seconds)
            failures = self._latch_all_cleanup_locked(reason=reason, now=now)
            if failures:
                self._set_reconciliation_locked(
                    "cleanup-latch-failed:" + ",".join(failures)
                )
            elif self._thread is None or not bool(self._thread.is_alive()):
                self._generation += 1
                self._start_scheduler_or_reconcile_locked()
            return self._status_locked(snapshots=self._refresh_components_locked())

    def run_one_scheduler_cycle_for_test(self) -> dict[str, Any]:
        self._assert_owner()
        with self._lock:
            now = self._now()
            if self._state == "ACTIVE":
                for name in _COMPONENTS:
                    self._component_call(name, "tick", now=_time_text(now, "test-now"))
            elif self._state == "CLEANUP":
                results = [
                    self._component_call(
                        name, "recover_cleanup", now=_time_text(now, "test-now")
                    )
                    for name in _COMPONENTS
                ]
                result_states = [self._state_of(value) for value in results]
                if any(state == "RECONCILIATION_REQUIRED" for state in result_states):
                    self._set_reconciliation_locked(
                        "component-cleanup-reconciliation-required"
                    )
                elif all(state in _SAFE_CLEANUP_STATES for state in result_states):
                    self._state = "SAFE_INCOMPLETE"
                    self._reason = "OWNED_CLEANUP_COMPLETE_OFFLINE"
                    self._stop_event.set()
                elif self._cleanup_deadline is not None and now >= self._cleanup_deadline:
                    self._set_reconciliation_locked("cleanup-deadline-expired")
            return self._status_locked(snapshots=self._refresh_components_locked())

    def _status_locked(
        self, *, snapshots: Mapping[str, Mapping[str, Any]]
    ) -> dict[str, Any]:
        hashes = {name: _hash(snapshots[name]) for name in _COMPONENTS}
        blockers = self._readiness_blockers(snapshots)
        if self._state == "RECONCILIATION_REQUIRED":
            blockers.append("BACKEND_RECONCILIATION_REQUIRED")
        body = {
            "schemaVersion": STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "ownerKeyHash": hashlib.sha256(self.owner_key.encode("utf-8")).hexdigest(),
            "ownerGeneration": self._generation,
            "state": self._state,
            "reason": self._reason,
            "schedulerRunning": bool(
                self._thread is not None and self._thread.is_alive()
            ),
            "schedulerCadenceSeconds": format(self.cadence_seconds, "g"),
            "schedulerCadenceUnderTenSeconds": self.cadence_seconds < 10,
            "componentCallTimeoutSeconds": format(
                self.component_call_timeout_seconds, "g"
            ),
            "cleanupDeadlineAt": (
                "" if self._cleanup_deadline is None else _time_text(
                    self._cleanup_deadline, "status-cleanup-deadline"
                )
            ),
            "componentStatusHashes": hashes,
            "componentInterfaceHashes": dict(self._component_interface_hashes),
            "componentProtocolHash": _hash(
                {
                    "schemaVersion": "kis-domestic-functional-backend-component-protocol/v2",
                    "components": list(_COMPONENTS),
                    "statusRequired": [
                        "schemaVersion",
                        "state",
                        "ready",
                        "productionAvailable",
                        "networkAvailable",
                        "mutationAvailable",
                        "releaseAvailable",
                        "networkOrderPostAllowed",
                        "tradingMutationCount",
                        "hazardousAuthorityOpen",
                        "authorityOpen",
                        "ownedWorkingExposurePresent",
                        "ownedPositionExposurePresent",
                    ],
                    "cooperativeCancellationRequiredFor": [
                        "tick",
                        "latch_cleanup",
                        "recover_cleanup",
                    ],
                }
            ),
            "hazardousComponentNames": list(self._hazardous_components),
            "hazardousAuthorityOpen": bool(self._hazardous_components),
            "ownedExposurePresent": any(
                snapshots[name].get("ownedWorkingExposurePresent") is True
                or snapshots[name].get("ownedPositionExposurePresent") is True
                for name in _COMPONENTS
            ),
            "timedOutComponentCalls": sorted(self._timed_out_component_calls),
            "detachedComponentCalls": sorted(self._detached_component_calls),
            "readinessBlockers": sorted(set(blockers)),
            "allInjectedComponentStatusesAvailable": True,
            "offlineSchedulerSimulationOnly": True,
            "graphProjectionOnly": True,
            "crossProcessLeaseAvailable": False,
            "independentGraphVerifierAvailable": False,
            "verifyOnlyUpstreamAuthoritySeparated": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
            "stateServerWired": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**body, "statusHash": _hash(body)}

    def status(self) -> dict[str, Any]:
        self._assert_owner()
        with self._lock:
            return self._status_locked(snapshots=self._refresh_components_locked())

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            hazardous = self._state in {"ACTIVE", "CLEANUP"} or bool(
                self._hazardous_components
            )
            audit_error = ""
            try:
                self._refresh_components_locked()
                hazardous = hazardous or bool(self._hazardous_components)
            except BaseException as exc:
                audit_error = "backend-close-audit-failed:" + type(exc).__name__
            if hazardous:
                now = self._last_trusted_now
                if now is None:
                    try:
                        now = self._now()
                    except BaseException as exc:
                        audit_error = (
                            audit_error + ":" if audit_error else ""
                        ) + "backend-close-clock-failed:" + type(exc).__name__
                failures: list[str] = []
                if now is not None:
                    self._state = "CLEANUP"
                    self._reason = "BACKEND_OWNER_CLOSE"
                    self._cleanup_deadline = now + timedelta(
                        seconds=self.cleanup_seconds
                    )
                    failures = self._latch_all_cleanup_locked(
                        reason=self._reason, now=now
                    )
                detail = "backend-owner-closed-with-hazard"
                if audit_error:
                    detail += ":" + audit_error
                if failures:
                    detail += ":latch-failed:" + ",".join(failures)
                self._set_reconciliation_locked(detail)
            elif audit_error:
                self._set_reconciliation_locked(
                    audit_error
                )
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            try:
                thread.join(timeout=max(0.1, self.cadence_seconds * 2))
            except BaseException:
                pass
        with _OWNER_LOCK:
            if _LIVE_OWNERS.get(self.owner_key) == self.owner_token:
                del _LIVE_OWNERS[self.owner_key]
        self._closed = True


def backend_component_status() -> dict[str, Any]:
    body = {
        "schemaVersion": "kis-domestic-functional-backend-component/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "componentNames": list(_COMPONENTS),
        "maxSchedulerCadenceSeconds": format(MAX_CADENCE_SECONDS, "g"),
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "stateServerWired": False,
        "networkOrderPostAllowed": False,
    }
    return {**body, "componentHash": _hash(body)}


__all__ = [
    "DEFAULT_CLEANUP_SECONDS",
    "KIS_DOMESTIC_FUNCTIONAL_BACKEND_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_BACKEND_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_BACKEND_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_BACKEND_RELEASE_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_BACKEND_STATE_SERVER_WIRED",
    "KisDomesticFunctionalBackend",
    "KisDomesticFunctionalBackendBlocked",
    "KisDomesticFunctionalBackendComponent",
    "MAX_CADENCE_SECONDS",
    "STATUS_SCHEMA",
    "backend_component_status",
]
