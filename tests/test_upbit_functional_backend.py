from __future__ import annotations

from datetime import datetime, timedelta
from contextlib import closing, contextmanager
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from live_trader import upbit_continuous_functional as core
from live_trader import upbit_functional_backend as backend
from live_trader import upbit_functional_entrypoint as entrypoint
from live_trader import upbit_functional_mutation as mutation
from live_trader.upbit_continuous_functional import UpbitFunctionalBlocked
from live_trader.upbit_functional_approval import (
    DurableUpbitFunctionalApprovalStore,
    _functional_wiring_evidence_complete,
    _official_rest_raw_matches_terminal,
    _permit_immutable_lineage,
)
from live_trader.upbit_functional_backend import (
    UpbitFunctionalBackendManager,
    upbit_functional_composite_available,
)
from tests.test_upbit_continuous_functional import (
    ACCOUNT,
    FakeBoundaries,
    NOW,
    TEST_EXCLUSIVITY_VERIFIER,
    TEST_EXCLUSIVITY_VERIFIER_PIN,
    permit,
)
from tests.test_upbit_functional_strategy import (
    SealedUpbitMovingAverageEvaluatorTest,
)


class _WaitSequence:
    def __init__(self, values):
        self.values = iter(values)

    def wait(self, _seconds):
        return next(self.values)

    def set(self):
        pass

    def clear(self):
        pass


class _Authority:
    def __init__(self):
        self.cleanup_calls = 0
        self.clear_calls = 0

    def cleanup(self):
        self.cleanup_calls += 1

    def clear(self):
        self.clear_calls += 1

    def orders_enabled(self):
        return False

    def snapshot(self):
        return {
            "functionalCapabilityHash": "",
            "realOrdersEnabled": False,
            "functionalMutationEnabled": False,
            "newEntriesBlocked": True,
        }


class UpbitFunctionalBackendContractTest(unittest.TestCase):
    def test_direct_production_manager_is_blocked_before_database_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "must-not-exist.sqlite3"
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "direct-construction-forbidden"
            ):
                UpbitFunctionalBackendManager(
                    database_path=database,
                    publication_proof_path=Path(temporary) / "proof.json",
                    clock=lambda: NOW,
                    approval_store=None,  # type: ignore[arg-type]
                    sender=lambda *_args, **_kwargs: {},
                    lease_reader_factory=lambda **_kwargs: None,
                    operator_confirmation_verifier=lambda _value: True,
                )
            self.assertFalse(database.exists())

    @staticmethod
    def _preclaim_manager(store):
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._allow_mock_graph = True
        manager._lock = threading.RLock()
        manager._generation = 0
        manager._scheduler = None
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager.approval_store = store
        manager.operator_confirmation_verifier = lambda _value: True
        return manager

    @staticmethod
    def _approved_store(temporary: str):
        functional_permit = permit()
        fake = FakeBoundaries(functional_permit)
        store = DurableUpbitFunctionalApprovalStore(
            Path(temporary) / "preclaim.sqlite3",
            clock=fake.clock,
            operator_verifier=lambda value: value.get("serverSignature")
            == "verified",
            immutable_selection_reader=fake.immutable_selection,
        )
        approval = {
            "approvalId": "upbit-preclaim-approval-0001",
            "operatorId": "operator-you",
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "permitId": functional_permit.permit_id,
            "permitHash": functional_permit.content_hash,
            "accountFingerprint": ACCOUNT,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "approvedAt": fake.now.isoformat().replace("+00:00", "Z"),
            "nonce": "preclaim-approval-nonce-000000000001",
            "serverSignature": "verified",
        }
        store.approve_permit(functional_permit.to_dict(), approval)
        return store, approval

    def test_owner_acquire_exception_retires_exact_unclaimed_approval(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store, approval = self._approved_store(temporary)
            manager = self._preclaim_manager(store)

            @contextmanager
            def failed_owner():
                raise RuntimeError("owner-acquire-failed")
                yield  # pragma: no cover

            manager._owner = failed_owner
            with self.assertRaisesRegex(RuntimeError, "owner-acquire-failed"):
                manager.start(
                    {
                        "approvalId": approval["approvalId"],
                        "operatorConfirmation": {
                            "authenticated": True,
                            "confirmed": True,
                        },
                    }
                )
            self.assertEqual(
                "FAILED", store.permit_status(approval["approvalId"])["state"]
            )
            self.assertEqual("START_FAILED_PRECLAIM", manager._terminal_state)

    def test_exception_after_owner_before_claim_retires_exact_approval(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store, approval = self._approved_store(temporary)
            manager = self._preclaim_manager(store)

            class BrokenScheduler:
                @staticmethod
                def is_alive():
                    raise RuntimeError("preclaim-scheduler-read-failed")

            manager._scheduler = BrokenScheduler()
            with self.assertRaisesRegex(
                RuntimeError, "preclaim-scheduler-read-failed"
            ):
                manager.start(
                    {
                        "approvalId": approval["approvalId"],
                        "operatorConfirmation": {
                            "authenticated": True,
                            "confirmed": True,
                        },
                    }
                )
            self.assertEqual(
                "FAILED", store.permit_status(approval["approvalId"])["state"]
            )

    def test_authority_rechecks_real_ordinary_route_closure_on_every_read(self) -> None:
        closed = {"value": True}
        authority = backend._ManagedFunctionalAuthority(
            ordinary_routes_closed_reader=lambda: closed["value"],
            emergency_stop_reader=lambda: {
                "active": False,
                "revision": "test-emergency-clear",
            },
        )
        authority.bind_scope({"functionalTestSessionId": "session-1"})
        authority.register("a" * 64)
        authority.arm("a" * 64)
        self.assertTrue(authority.orders_enabled())
        self.assertTrue(authority.snapshot()["ordinaryRoutesClosed"])
        closed["value"] = False
        self.assertFalse(authority.orders_enabled())
        snapshot = authority.snapshot()
        self.assertFalse(snapshot["ordinaryRoutesClosed"])
        self.assertFalse(snapshot["upbitSmokeRouteClosed"])
        self.assertFalse(snapshot["newEntriesBlocked"])
        self.assertFalse(snapshot["functionalMutationEnabled"])

    def test_authority_requires_live_durable_owner_lease_on_every_read(self) -> None:
        owner = {"active": False}
        authority = backend._ManagedFunctionalAuthority(
            ordinary_routes_closed_reader=lambda: True,
            emergency_stop_reader=lambda: {
                "active": False,
                "revision": "test-emergency-clear",
            },
            durable_owner_lease_reader=lambda: owner["active"],
            durable_owner_lease_required=True,
        )
        authority.bind_scope({"functionalTestSessionId": "session-1"})
        authority.register("a" * 64)
        authority.arm("a" * 64)
        self.assertFalse(authority.orders_enabled())
        blocked = authority.snapshot()
        self.assertTrue(blocked["durableOwnerLeaseRequired"])
        self.assertFalse(blocked["durableOwnerLeaseActive"])
        self.assertFalse(blocked["functionalMutationEnabled"])
        owner["active"] = True
        self.assertTrue(authority.orders_enabled())
        self.assertTrue(authority.snapshot()["functionalMutationEnabled"])

    def test_process_singleton_wrappers_delegate_server_owned_commands(self) -> None:
        original = backend._BACKEND_SINGLETON
        self.addCleanup(
            lambda: setattr(backend, "_BACKEND_SINGLETON", original)
        )
        backend._BACKEND_SINGLETON = None
        self.assertFalse(backend.upbit_functional_backend_status()["prepared"])
        calls = []

        class Manager:
            def status(self):
                return {"available": False, "terminalState": "IDLE"}

            def start(self, command):
                calls.append(("start", dict(command)))
                return {"ok": True}

            def stop(self, command):
                calls.append(("stop", dict(command)))
                return {"ok": True}

            def recover(self, command):
                calls.append(("recover", dict(command)))
                return {"ok": True}

        backend._BACKEND_SINGLETON = Manager()
        start = {"approvalId": "server-owned", "operatorConfirmation": {}}
        stop = {"operatorConfirmation": {}}
        recover = {"recoveryId": "server-owned", "operatorConfirmation": {}}
        backend.start_upbit_functional_backend(start)
        backend.stop_upbit_functional_backend(stop)
        backend.recover_upbit_functional_backend(recover)
        self.assertEqual(
            [("start", start), ("stop", stop), ("recover", recover)], calls
        )

    def test_composite_gate_is_false_until_every_production_proof_exists(self) -> None:
        self.assertFalse(upbit_functional_composite_available())

    def test_first_live_status_exposes_prepared_but_keeps_network_gate_closed(
        self,
    ) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._lock = threading.RLock()
        manager._account_exclusivity_proof_reader = lambda **_kwargs: {}
        manager._owner_process_identity_hash = "d" * 64
        manager._owner_process_identity_durable = True
        manager._durable_owner_lease_required = True
        manager._approval_id = ""
        manager._session_id = ""
        manager._generation = 0
        manager._scheduler = None
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager._startup_audit = {"graph": {}, "approvals": {}}
        manager.authority = _Authority()

        verifier_status = {
            "ready": True,
            "authorityPinned": True,
            "runtimeIdentityMatched": True,
            "reason": "READY",
            "pinHash": "e" * 64,
        }

        class Store:
            @staticmethod
            def first_live_preparation_status():
                return {
                    "prepared": True,
                    "accountExclusivityVerifier": dict(verifier_status),
                    "ownerLease": {
                        "required": True,
                        "durable": True,
                        "singleOwner": True,
                        "bearerTokenPersisted": False,
                    },
                }

            @staticmethod
            def owner_lease_status(**_kwargs):
                return None

        class Graph:
            @staticmethod
            def status():
                return {
                    "accountExclusivityVerifier": dict(verifier_status),
                    "accountExclusivityProofSourceWired": True,
                    "startupAudit": {},
                }

        manager.approval_store = Store()
        manager.graph = Graph()
        closed_gates = {
            "explicitLiveEnv": True,
            "entrypoint": True,
            "mutation": True,
            "backend": True,
            "stateServer": True,
            "verifierAuthorityPinned": True,
            "productionVerifierWired": True,
            "entrypointComposite": True,
            "bootstrapPreparation": True,
            "bootstrapNetworkRelease": False,
        }
        with (
            patch.object(
                backend,
                "_first_live_static_gate_status",
                return_value=closed_gates,
            ),
            patch.object(
                backend,
                "upbit_functional_composite_available",
                return_value=False,
            ),
        ):
            status = manager.status()
        self.assertTrue(status["firstLiveBootstrapPrepared"])
        self.assertFalse(status["firstLiveBootstrapEligible"])
        self.assertFalse(status["available"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertIn(
            "bootstrapNetworkRelease",
            status["firstLiveBootstrapBlockedReason"],
        )

    def test_explicit_live_env_gate_is_required_even_when_all_code_gates_are_true(
        self,
    ) -> None:
        patches = (
            patch.object(backend, "UPBIT_FUNCTIONAL_BACKEND_AVAILABLE", True),
            patch.object(
                backend, "UPBIT_FUNCTIONAL_STATE_SERVER_WIRING_AVAILABLE", True
            ),
            patch.object(backend, "UPBIT_FUNCTIONAL_REAL_E2E_AVAILABLE", True),
            patch.object(backend, "UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE", True),
            patch.object(backend, "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE", True),
            patch.object(
                backend,
                "UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED",
                True,
            ),
            patch.object(
                backend,
                "UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED",
                True,
            ),
            patch.object(
                backend,
                "production_entrypoint_status",
                return_value={"available": True, "reason": "ready"},
            ),
        )
        with (
            patches[0], patches[1], patches[2], patches[3], patches[4],
            patches[5], patches[6], patches[7]
        ):
            with patch.dict(os.environ, {"UPBIT_FUNCTIONAL_LIVE_ENABLED": "false"}):
                self.assertFalse(upbit_functional_composite_available())
            with patch.dict(os.environ, {"UPBIT_FUNCTIONAL_LIVE_ENABLED": "true"}):
                self.assertTrue(upbit_functional_composite_available())

    def test_composite_and_post_status_stay_false_when_authority_pin_is_false(
        self,
    ) -> None:
        with (
            patch.dict(
                os.environ,
                {"UPBIT_FUNCTIONAL_LIVE_ENABLED": "true"},
            ),
            patch.object(backend, "UPBIT_FUNCTIONAL_BACKEND_AVAILABLE", True),
            patch.object(
                backend,
                "UPBIT_FUNCTIONAL_STATE_SERVER_WIRING_AVAILABLE",
                True,
            ),
            patch.object(backend, "UPBIT_FUNCTIONAL_REAL_E2E_AVAILABLE", True),
            patch.object(backend, "UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE", True),
            patch.object(backend, "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE", True),
            patch.object(
                backend,
                "UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED",
                True,
            ),
            patch.object(
                backend,
                "UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED",
                False,
            ),
            patch.object(
                backend,
                "production_entrypoint_status",
                return_value={"available": True, "reason": "ready"},
            ),
        ):
            self.assertFalse(upbit_functional_composite_available())
            status = backend.upbit_functional_backend_status()
            self.assertFalse(status["available"])
            self.assertFalse(status["networkOrderPostAllowed"])
            self.assertFalse(status["verifierAuthorityPinned"])
            self.assertFalse(status["accountExclusivityPreSendReady"])

    def test_command_surface_rejects_raw_permit_bar_signal_or_capability(self) -> None:
        for field in ("permit", "bar", "signal", "capability", "payload"):
            with self.subTest(field=field), self.assertRaisesRegex(
                UpbitFunctionalBlocked, "fields-not-exact"
            ):
                UpbitFunctionalBackendManager._assert_command_fields(
                    {
                        "approvalId": "server-record",
                        "operatorConfirmation": {},
                        field: {},
                    },
                    {"approvalId", "operatorConfirmation"},
                )

    def test_scheduler_failure_keeps_cleanup_owner_until_finalized(self) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._lock = threading.RLock()
        manager._generation = 7
        manager._scheduler_stop = _WaitSequence([False, False])
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager.authority = _Authority()
        calls = []

        class Graph:
            def pump(self):
                calls.append("pump")
                if calls.count("pump") == 1:
                    raise RuntimeError("truth unavailable")
                return {"snapshot": {"status": "FINALIZED"}}

            def stop(self, *, reason):
                calls.append(f"stop:{reason}")
                return {"pending": True, "snapshot": {"status": "CLEANUP"}}

        manager.graph = Graph()
        finalized = []
        manager._consume_finalized_locked = lambda _result=None: finalized.append(True)
        manager._scheduler_loop(7)
        self.assertEqual(
            ["pump", "stop:scheduler-failure", "pump"], calls
        )
        self.assertEqual(1, manager.authority.cleanup_calls)
        self.assertEqual([True], finalized)

    def test_scheduler_thread_start_failure_immediately_enters_fail_closed_cleanup(
        self,
    ) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager.authority = _Authority()
        manager._scheduler_stop = threading.Event()
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        calls: list[str] = []

        class Graph:
            @staticmethod
            def fail_closed_scheduler_start(*, reason):
                calls.append(f"fail-closed:{reason}")
                return {
                    "ok": False,
                    "pending": True,
                    "snapshot": {"status": "CLEANUP"},
                }

        manager.graph = Graph()
        manager._start_scheduler_locked = lambda _generation: (_ for _ in ()).throw(
            RuntimeError("thread-start-failed")
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "scheduler-start-failed"
        ):
            manager._start_scheduler_or_fail_closed_locked(
                17, reason="activation-scheduler-start-failed"
            )
        self.assertEqual(
            ["fail-closed:activation-scheduler-start-failed"], calls
        )
        self.assertEqual(1, manager.authority.cleanup_calls)
        self.assertEqual(1, manager.authority.clear_calls)
        self.assertEqual("RECONCILIATION_REQUIRED", manager._terminal_state)

    def test_cleanup_scheduler_spawn_retries_with_fresh_thread_before_success(
        self,
    ) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._scheduler = None
        manager.authority = _Authority()
        attempts: list[int] = []

        def spawn(generation):
            attempts.append(generation)
            if len(attempts) < 3:
                raise RuntimeError("transient-thread-start-failure")

        manager._start_scheduler_locked = spawn
        manager._start_scheduler_or_fail_closed_locked(
            23,
            reason="operator-stop-scheduler-start-failed",
        )
        self.assertEqual([23, 23, 23], attempts)
        self.assertEqual(0, manager.authority.cleanup_calls)

    def test_recovery_pending_spawn_failure_revokes_to_sticky_recovery(self) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._lock = threading.RLock()
        manager._generation = 30
        manager._scheduler = None
        manager._scheduler_stop = threading.Event()
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager.authority = _Authority()
        manager.operator_confirmation_verifier = lambda _value: True
        manager._session_id = "upbit-recovery-spawn-failed-session-0001"
        manager._approval_id = "upbit-recovery-spawn-failed-approval-0001"
        calls: list[str] = []

        class Store:
            @staticmethod
            def recovery_authority_pointer():
                return {
                    "state": "APPROVED",
                    "recovery_id": "upbit-recovery-spawn-failed-0001",
                    "recovery_json": json.dumps(
                        {
                            "sessionId": manager._session_id,
                            "previousWriterGeneration": 1,
                            "nextWriterGeneration": 2,
                            "previousOwnerLeaseEvidenceHash": "a" * 64,
                        }
                    ),
                }

            @staticmethod
            def claim_recovery(**_kwargs):
                return {
                    "permitId": "functional-test-permit-recovery-spawn",
                    "permitHash": "b" * 64,
                    "sessionId": manager._session_id,
                    "contentHash": "c" * 64,
                }

            @staticmethod
            def finish_recovery(**kwargs):
                calls.append("finish:" + str(kwargs["state"]))

        class Graph:
            @staticmethod
            def recover_cleanup(**_kwargs):
                return {"ok": True, "pending": True, "snapshot": {"status": "CLEANUP"}}

            @staticmethod
            def fail_closed_scheduler_start(*, reason):
                calls.append("fail-closed:" + reason)
                return {"ok": False}

        manager.approval_store = Store()
        manager.graph = Graph()
        manager._recovery_identity_locked = lambda: {
            "journal": {"writer_generation": 1},
            "leaseEvidenceHash": "a" * 64,
        }
        manager._start_scheduler_locked = lambda _generation: (_ for _ in ()).throw(
            RuntimeError("thread-start-failed")
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "scheduler-start-failed"
        ):
            manager.recover(
                {
                    "recoveryId": "upbit-recovery-spawn-failed-0001",
                    "operatorConfirmation": {
                        "authenticated": True,
                        "confirmed": True,
                    },
                }
            )
        self.assertEqual(
            [
                "finish:CONSUMED",
                "fail-closed:recovery-scheduler-start-failed",
            ],
            calls,
        )
        self.assertEqual("RECONCILIATION_REQUIRED", manager._terminal_state)

    def test_stop_pending_spawn_failure_revokes_to_sticky_recovery(self) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._lock = threading.RLock()
        manager._generation = 40
        manager._scheduler = None
        manager._scheduler_stop = threading.Event()
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager.authority = _Authority()
        manager.operator_confirmation_verifier = lambda _value: True
        calls: list[str] = []

        class Graph:
            @staticmethod
            def stop(*, reason):
                calls.append("stop:" + reason)
                return {
                    "ok": True,
                    "pending": True,
                    "snapshot": {"status": "CLEANUP"},
                }

            @staticmethod
            def fail_closed_scheduler_start(*, reason):
                calls.append("fail-closed:" + reason)
                return {"ok": False}

        manager.graph = Graph()
        manager._start_scheduler_locked = lambda _generation: (_ for _ in ()).throw(
            RuntimeError("thread-start-failed")
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "scheduler-start-failed"
        ):
            manager.stop(
                {
                    "operatorConfirmation": {
                        "authenticated": True,
                        "confirmed": True,
                    }
                }
            )
        self.assertEqual(
            [
                "stop:operator-stop",
                "fail-closed:operator-stop-scheduler-start-failed",
            ],
            calls,
        )
        self.assertEqual("RECONCILIATION_REQUIRED", manager._terminal_state)

    def test_scheduler_failure_that_cannot_attach_cleanup_seals_alarm(self) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._lock = threading.RLock()
        manager._generation = 9
        manager._scheduler_stop = _WaitSequence([False])
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager.authority = _Authority()

        class Graph:
            def pump(self):
                raise RuntimeError("stream lost")

            def stop(self, *, reason):
                raise RuntimeError(f"{reason}:owner detached")

        manager.graph = Graph()
        manager._scheduler_loop(9)
        self.assertEqual("RECONCILIATION_REQUIRED", manager._terminal_state)
        self.assertEqual(1, manager.authority.clear_calls)

    def test_scheduler_never_calls_failed_closed_cleanup_finalized(self) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._lock = threading.RLock()
        manager._generation = 10
        manager._scheduler_stop = _WaitSequence([False])
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager.authority = _Authority()

        class Graph:
            def pump(self):
                raise RuntimeError("truth unavailable")

            def stop(self, *, reason):
                return {
                    "ok": False,
                    "pending": False,
                    "manualInterventionRequired": True,
                    "snapshot": {"status": "FAILED_CLOSED"},
                }

        manager.graph = Graph()
        manager._consume_finalized_locked = lambda _result=None: self.fail(
            "failed-closed cleanup cannot be consumed as finalized"
        )
        manager._scheduler_loop(10)
        self.assertEqual("RECONCILIATION_REQUIRED", manager._terminal_state)
        self.assertEqual(1, manager.authority.clear_calls)

    def test_final_approval_consume_failure_is_visible_and_fail_closed(self) -> None:
        manager = object.__new__(UpbitFunctionalBackendManager)
        manager._approval_id = "upbit-backend-approval-0001"
        manager._session_id = "upbit-functional-session-0001"
        manager._scheduler_stop = threading.Event()
        manager._terminal_state = "IDLE"
        manager._terminal_detail = ""
        manager.authority = _Authority()
        evidence = {
            "functionalTestPassed": False,
            "functionalWiringPassed": False,
            "exclusiveAccountCausalProofComplete": False,
            "exactTwoHourRuntimeComplete": True,
            "activationRelativePermitExact": True,
            "processMonotonicContinuity": True,
            "clockDiscontinuityAbsent": True,
            "actualDurationSeconds": "7200",
            "processMonotonicElapsedSeconds": "7200",
            "functionalCapabilityCleared": True,
            "newEntriesBlocked": True,
            "realOrdersEnabled": False,
        }
        terminal_body = {
            "schemaVersion": "upbit-functional-private-terminal-seal/v1",
            "sessionId": manager._session_id,
            "externalActivityAbsent": True,
            "streamContinuous": True,
        }
        terminal = {**terminal_body, "sealHash": core._stable_hash(terminal_body)}
        evidence["terminalPrivateStreamSeal"] = terminal
        evidence["terminalPrivateStreamSealHash"] = terminal["sealHash"]
        evidence_hash = core._stable_hash(evidence)

        class Ledger:
            def session(self, _session_id):
                return {
                    "state": "FINALIZED",
                    "final_evidence_hash": evidence_hash,
                    "capability_hash": "",
                    "new_entries_blocked": 1,
                    "real_orders_enabled": 0,
                }

            def final_evidence(self, _session_id):
                return dict(evidence)

        class Journal:
            def terminal_seal(self, *, session_id):
                self_session = session_id
                if self_session != manager._session_id:
                    raise AssertionError("wrong session")
                return dict(terminal)

        manager.graph = type(
            "Graph", (), {"ledger": Ledger(), "journal": Journal()}
        )()

        class Store:
            def finish_first_live_bootstrap(self, **_kwargs):
                raise RuntimeError("sqlite unavailable")

        manager.approval_store = Store()
        with self.assertRaisesRegex(RuntimeError, "sqlite unavailable"):
            manager._consume_finalized_locked(
                {
                    "ok": True,
                    "snapshot": {
                        "status": "FINALIZED",
                        "sessionId": manager._session_id,
                    },
                    "final": {
                        "ok": True,
                        "state": "FINALIZED",
                        "testOutcome": "SAFE_INCOMPLETE",
                        "evidence": evidence,
                        "evidenceHash": evidence_hash,
                    },
                }
            )
        self.assertEqual(1, manager.authority.clear_calls)
        self.assertTrue(manager._scheduler_stop.is_set())
        self.assertEqual("RECONCILIATION_REQUIRED", manager._terminal_state)

    def test_server_owned_mock_e2e_buy_sell_finalizes_without_client_signal(self) -> None:
        functional_permit = permit()
        fake = FakeBoundaries(functional_permit)
        fake.session_id = "upbit-functional-" + "1" * 32
        fake.selection_updates.update(
            {
                "strategyPluginId": "moving_average_cross",
                "strategyShortMa": 3,
                "strategyLongMa": 10,
            }
        )

        class Stream:
            def __init__(self):
                self.live = {}
                self.journal = None
                self.writer = None
                self.session_id = ""

            def handshake(self, *, session_id, writer_authority, journal, **_kwargs):
                token_hash = hashlib.sha256(
                    writer_authority["writerToken"].encode("utf-8")
                ).hexdigest()
                self.live = {
                    "sessionId": session_id,
                    "writerGeneration": writer_authority["writerGeneration"],
                    "writerTokenHash": token_hash,
                    "connected": True,
                    "authenticated": True,
                    "myOrderSubscribed": True,
                    "lastFrameAt": fake.now.isoformat().replace(
                        "+00:00", "Z"
                    ),
                }
                self.journal = journal
                self.writer = dict(writer_authority)
                self.session_id = session_id
                return {
                    **self.live,
                    "livenessReader": lambda: dict(self.live),
                    "closePump": lambda: None,
                }

            def ingest_fill(
                self, *, order_uuid, trade_uuid, identifier, side,
                volume, funds, fee,
            ):
                self.journal.ingest(
                    self.session_id,
                    {
                        "type": "myOrder",
                        "code": "KRW-BTC",
                        "uuid": order_uuid,
                        "trade_uuid": trade_uuid,
                        "identifier": identifier,
                        "ask_bid": side,
                        "state": "trade",
                        "timestamp": int(fake.now.timestamp() * 1000),
                        "trade_volume": str(volume),
                        "trade_price": str(
                            Decimal(str(funds)) / Decimal(str(volume))
                        ),
                        "trade_fee": str(fee),
                    },
                    writer_token=self.writer["writerToken"],
                    writer_generation=self.writer["writerGeneration"],
                )

            def terminal_barrier(self, *, session_id):
                self.journal.observe(
                    session_id,
                    writer_token=self.writer["writerToken"],
                    writer_generation=self.writer["writerGeneration"],
                )
                return {"cutoffEstablished": True, "sessionId": session_id}

        class Candles:
            def __init__(self):
                self.value = SealedUpbitMovingAverageEvaluatorTest.window(
                    [10] * 10 + [20]
                )

            def __call__(self):
                return dict(self.value)

        stream = Stream()
        candles = Candles()

        def broker_sender(request):
            if request.method == "POST":
                payload = dict(request.body)
                fake.post_calls += 1
                identifier = str(payload["identifier"])
                order_uuid = f"backend-order-{fake.post_calls:04d}"
                if payload["side"] == "bid":
                    funds = Decimal(str(payload["price"]))
                    volume = (funds / fake.mark).quantize(
                        Decimal("0.00000001")
                    )
                    fee = funds * Decimal("0.0005")
                    fake.quote -= funds + fee
                    fake.base += volume
                    side = "BID"
                else:
                    volume = Decimal(str(payload["volume"]))
                    funds = volume * fake.mark
                    fee = funds * Decimal("0.0005")
                    fake.quote += funds - fee
                    fake.base -= volume
                    side = "ASK"
                fake.fills.append(
                    {
                        "market": "KRW-BTC",
                        "tradeUuid": f"backend-trade-{fake.post_calls:04d}",
                        "orderUuid": order_uuid,
                        "identifier": identifier,
                        "side": side,
                        "volume": str(volume),
                        "funds": str(funds),
                        "fee": str(fee),
                    }
                )
                stream.ingest_fill(
                    order_uuid=order_uuid,
                    trade_uuid=f"backend-trade-{fake.post_calls:04d}",
                    identifier=identifier,
                    side=side,
                    volume=volume,
                    funds=funds,
                    fee=fee,
                )
                fake.closed_orders.append(
                    {
                        "market": "KRW-BTC",
                        "uuid": order_uuid,
                        "identifier": identifier,
                        "side": side,
                        "state": "done",
                    }
                )
                return {
                    "ok": True,
                    "statusCode": 201,
                    "json": {
                        "uuid": order_uuid,
                        "identifier": identifier,
                        "market": "KRW-BTC",
                        "side": str(payload["side"]),
                        "state": "done",
                    },
                }
            raise AssertionError("mock E2E expected order POST only")

        def mock_truth(**truth_kwargs):
            private = stream.journal.snapshot(
                session_id=truth_kwargs["session_id"],
                identifiers=truth_kwargs["identifiers"],
            )
            return {
                **fake.truth(**truth_kwargs),
                "privateStreamEvents": private["events"],
                "privateStreamWriterGeneration": private["writerGeneration"],
                "privateStreamRevision": private["journalRevision"],
                "privateStreamEventCursor": private["eventCursor"],
                "privateStreamLastEventId": private["lastEventId"],
                "privateStreamEventHeadHash": private["eventHeadHash"],
            }

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "backend.sqlite3"
            store = DurableUpbitFunctionalApprovalStore(
                path,
                clock=fake.clock,
                operator_verifier=lambda value: value.get("serverSignature")
                == "verified",
                immutable_selection_reader=fake.immutable_selection,
                account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
                account_exclusivity_verifier_pin=(
                    TEST_EXCLUSIVITY_VERIFIER_PIN
                ),
            )
            approval = {
                "approvalId": "upbit-backend-approval-0001",
                "operatorId": "operator-you",
                "operatorAuthenticated": True,
                "operatorApproved": True,
                "permitId": functional_permit.permit_id,
                "permitHash": functional_permit.content_hash,
                "accountFingerprint": ACCOUNT,
                "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
                "symbol": "KRW-BTC",
                "approvedAt": fake.now.isoformat().replace("+00:00", "Z"),
                "nonce": "backend-approval-nonce-000000000001",
                "serverSignature": "verified",
                "candidateBinding": {
                    "schemaVersion": "upbit-functional-server-candidate-binding/v1",
                    "immutablePermit": _permit_immutable_lineage(
                        functional_permit
                    ),
                    "selection": dict(fake.immutable_selection()),
                },
            }
            environment = {
                "UPBIT_ACCESS_KEY": "test-access",
                "UPBIT_SECRET_KEY": "test-secret",
                "UPBIT_FUNCTIONAL_LIVE_ENABLED": "true",
                "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
            }
            original_activate = (
                core.UpbitContinuousFunctionalService.activate
            )

            def activate_with_exact_test_authority(**kwargs):
                return original_activate(
                    **kwargs,
                    account_exclusivity_verifier=(
                        TEST_EXCLUSIVITY_VERIFIER
                    ),
                    account_exclusivity_verifier_pin=(
                        TEST_EXCLUSIVITY_VERIFIER_PIN
                    ),
                )

            with (
                patch.dict(os.environ, environment, clear=False),
                patch.object(core, "UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE", True),
                patch.object(
                    core,
                    "UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED",
                    True,
                ),
                patch.object(
                    mutation, "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE", True
                ),
                patch.object(
                    entrypoint, "UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE", True
                ),
                patch.object(
                    entrypoint, "UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE", True
                ),
                patch.object(
                    entrypoint,
                    "UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED",
                    True,
                ),
                patch.object(
                    entrypoint,
                    "UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED",
                    True,
                ),
                patch.object(
                    entrypoint, "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE", True
                ),
                patch.object(
                    backend, "upbit_credential_fingerprint", lambda: ACCOUNT
                ),
                patch.object(
                    entrypoint,
                    "upbit_credential_fingerprint",
                    lambda: ACCOUNT,
                ),
                patch.object(
                    entrypoint,
                    "OfficialUpbitFunctionalTruthReader",
                    lambda **_kwargs: mock_truth,
                ),
                patch.object(backend.secrets, "token_hex", lambda _size: "1" * 32),
                patch.object(backend, "_POLL_SECONDS", 3600.0),
                patch.object(core.time, "monotonic", fake.monotonic),
                patch.object(
                    core.UpbitContinuousFunctionalService,
                    "activate",
                    side_effect=activate_with_exact_test_authority,
                ),
                patch.object(
                    backend,
                    "_functional_wiring_evidence_complete",
                    side_effect=lambda evidence: (
                        _functional_wiring_evidence_complete(
                            evidence,
                            account_exclusivity_verifier=(
                                TEST_EXCLUSIVITY_VERIFIER
                            ),
                            account_exclusivity_verifier_pin=(
                                TEST_EXCLUSIVITY_VERIFIER_PIN
                            ),
                        )
                    ),
                ),
            ):
                manager = UpbitFunctionalBackendManager(
                    database_path=path,
                    publication_proof_path=Path(temporary) / "unused.json",
                    clock=fake.clock,
                    approval_store=store,
                    sender=broker_sender,
                    lease_reader_factory=fake.lease,
                    operator_confirmation_verifier=lambda value: value.get(
                        "serverSignature"
                    )
                    == "verified",
                    websocket_source=stream,
                    candle_source=candles,
                    allow_mock_graph=True,
                )
                manager.graph._selection_reader = fake.immutable_selection
                # Production constructs/audits the singleton before issuing
                # a server-owned candidate.  An APPROVED row already present
                # at construction is a crash-left preclaim pointer and is
                # intentionally retired by startup audit.
                store.approve_permit(functional_permit.to_dict(), approval)
                confirmation = {
                    "authenticated": True,
                    "confirmed": True,
                    "serverSignature": "verified",
                }
                manager.start(
                    {
                        "approvalId": approval["approvalId"],
                        "operatorConfirmation": confirmation,
                    }
                )
                fake.lease_updates["permitHash"] = store.permit_status(
                    approval["approvalId"]
                )["permit_hash"]
                with manager._lock:
                    first = manager.graph.pump()
                self.assertEqual("STRATEGY_BUY", first["result"]["action"])
                for _ in range(10):
                    fake.now += timedelta(seconds=30)
                    stream.live["lastFrameAt"] = fake.now.isoformat().replace(
                        "+00:00", "Z"
                    )
                    stream.journal.observe(
                        stream.session_id,
                        writer_token=stream.writer["writerToken"],
                        writer_generation=stream.writer["writerGeneration"],
                    )
                candles.value = SealedUpbitMovingAverageEvaluatorTest.window(
                    [20] * 10 + [10],
                    final_closed_at=fake.now,
                    observed_at=fake.now,
                )
                with manager._lock:
                    second = manager.graph.pump()
                self.assertEqual("STRATEGY_SELL", second["result"]["action"])
                # The two-hour proof also requires an uninterrupted private
                # writer lease.  Model the production socket heartbeat rather
                # than jumping the wall clock across a silent stream gap.
                for _ in range(240):
                    fake.now += timedelta(seconds=30)
                    stream.live["lastFrameAt"] = fake.now.isoformat().replace(
                        "+00:00", "Z"
                    )
                    stream.journal.observe(
                        stream.session_id,
                        writer_token=stream.writer["writerToken"],
                        writer_generation=stream.writer["writerGeneration"],
                    )
                stopped = manager.stop(
                    {"operatorConfirmation": confirmation}
                )
                self.assertEqual(
                    "FINALIZED", stopped["result"]["final"]["state"]
                )
                self.assertTrue(
                    stopped["result"]["final"]["evidence"][
                        "functionalTestPassed"
                    ]
                )
                self.assertEqual(2, fake.post_calls)
                self.assertEqual(
                    "CONSUMED",
                    store.permit_status(approval["approvalId"])["state"],
                )
                self.assertEqual("FINALIZED", manager.status()["terminalState"])
                with closing(sqlite3.connect(path)) as connection:
                    connection.row_factory = sqlite3.Row
                    terminal_row = connection.execute(
                        "SELECT truth_json FROM upbit_functional_terminal_truth"
                    ).fetchone()
                    raw_row = connection.execute(
                        "SELECT raw_json FROM upbit_functional_terminal_raw_truth"
                    ).fetchone()
                    terminal_truth = json.loads(terminal_row["truth_json"])
                    raw_truth = json.loads(raw_row["raw_json"])
                self.assertTrue(
                    _official_rest_raw_matches_terminal(
                        raw_truth,
                        terminal_truth,
                    ),
                    json.dumps(terminal_truth, sort_keys=True),
                )
                raw_tampered = json.loads(json.dumps(terminal_truth))
                raw_rest_tampered = json.loads(json.dumps(raw_truth))
                raw_rest_tampered["detailsByUuid"][0][
                    "payload"
                ]["paid_fee"] = "999"
                self.assertFalse(
                    _official_rest_raw_matches_terminal(
                        raw_rest_tampered,
                        raw_tampered,
                    )
                )
                raw_truncated = json.loads(json.dumps(terminal_truth))
                raw_rest_truncated = json.loads(json.dumps(raw_truth))
                raw_rest_truncated["openOrderPages"][0][
                    "payload"
                ] = [
                    {
                        "uuid": f"hidden-open-{index:04d}",
                        "identifier": f"hidden-open-{index:04d}",
                        "market": "KRW-BTC",
                        "side": "bid",
                        "state": "wait",
                    }
                    for index in range(100)
                ]
                self.assertFalse(
                    _official_rest_raw_matches_terminal(
                        raw_rest_truncated,
                        raw_truncated,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """UPDATE upbit_functional_terminal_raw_truth
                        SET cutoff=? WHERE session_id=?""",
                        (
                            (
                                datetime.fromisoformat(
                                    str(raw_truth["observationCutoff"])
                                    .replace("Z", "+00:00")
                                )
                                - timedelta(seconds=1)
                            ).isoformat().replace("+00:00", "Z"),
                            manager._session_id,
                        ),
                    )
                    connection.commit()
                self.assertFalse(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """UPDATE upbit_functional_terminal_raw_truth
                        SET cutoff=? WHERE session_id=?""",
                        (
                            raw_truth["observationCutoff"],
                            manager._session_id,
                        ),
                    )
                    connection.commit()
                self.assertTrue(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.row_factory = sqlite3.Row
                    natural_buy = connection.execute(
                        """SELECT rowid,* FROM upbit_functional_bars
                        WHERE session_id=? AND signal='BUY'""",
                        (manager._session_id,),
                    ).fetchone()
                    original_evaluation = natural_buy["evaluation_json"]
                    original_evaluation_hash = natural_buy["evaluation_hash"]
                    tampered_evaluation = json.loads(original_evaluation)
                    tampered_evaluation["rawFinalizedWindow"][
                        "officialCandleEvidence"
                    ]["orderedQuery"] = [["market", "KRW-BTC"], ["count", "19"]]
                    tampered_raw = json.dumps(
                        tampered_evaluation,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    connection.execute(
                        """UPDATE upbit_functional_bars
                        SET evaluation_json=?,evaluation_hash=?
                        WHERE rowid=?""",
                        (
                            tampered_raw,
                            hashlib.sha256(tampered_raw.encode("utf-8")).hexdigest(),
                            natural_buy["rowid"],
                        ),
                    )
                    connection.commit()
                self.assertFalse(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """UPDATE upbit_functional_bars
                        SET evaluation_json=?,evaluation_hash=?
                        WHERE rowid=?""",
                        (
                            original_evaluation,
                            original_evaluation_hash,
                            natural_buy["rowid"],
                        ),
                    )
                    connection.commit()
                self.assertTrue(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    buy_claim = connection.execute(
                        """SELECT claim_id,evaluation_id,claimed_at FROM
                        upbit_functional_claims WHERE session_id=?
                        AND slot='STRATEGY_BUY'""",
                        (manager._session_id,),
                    ).fetchone()
                    sell_claim = connection.execute(
                        """SELECT evaluation_id,claimed_at FROM upbit_functional_claims
                        WHERE session_id=? AND slot='STRATEGY_SELL'""",
                        (manager._session_id,),
                    ).fetchone()
                    connection.execute(
                        """UPDATE upbit_functional_claims SET evaluation_id=?
                        WHERE claim_id=?""",
                        (sell_claim[0], buy_claim[0]),
                    )
                    connection.commit()
                self.assertFalse(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    expires_at = connection.execute(
                        """SELECT expires_at FROM upbit_functional_sessions
                        WHERE session_id=?""",
                        (manager._session_id,),
                    ).fetchone()[0]
                    connection.execute(
                        """UPDATE upbit_functional_claims SET evaluation_id=?
                        WHERE claim_id=?""",
                        (buy_claim[1], buy_claim[0]),
                    )
                    connection.commit()
                self.assertTrue(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """UPDATE upbit_functional_claims SET claimed_at=?
                        WHERE session_id=? AND slot='STRATEGY_SELL'""",
                        (
                            expires_at,
                            manager._session_id,
                        ),
                    )
                    connection.commit()
                self.assertFalse(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """UPDATE upbit_functional_claims SET claimed_at=?
                        WHERE session_id=? AND slot='STRATEGY_SELL'""",
                        (sell_claim[1], manager._session_id),
                    )
                    connection.commit()
                final_now = fake.now
                fake.now = final_now - timedelta(seconds=7200)
                self.assertFalse(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                fake.now = final_now
                self.assertTrue(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.row_factory = sqlite3.Row
                    event_row = connection.execute(
                        """SELECT event_id,payload FROM upbit_myorder_events
                        WHERE session_id=? ORDER BY occurred_at,event_id
                        LIMIT 1""",
                        (manager._session_id,),
                    ).fetchone()
                    original_event_payload = event_row["payload"]
                    tampered_event = json.loads(original_event_payload)
                    tampered_event["normalized"]["occurredAt"] = (
                        "2026-08-13T01:09:59.000000Z"
                    )
                    connection.execute(
                        """UPDATE upbit_myorder_events SET payload=?
                        WHERE session_id=? AND event_id=?""",
                        (
                            json.dumps(
                                tampered_event,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                            manager._session_id,
                            event_row["event_id"],
                        ),
                    )
                    connection.commit()
                self.assertFalse(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """UPDATE upbit_myorder_events SET payload=?
                        WHERE session_id=? AND event_id=?""",
                        (
                            original_event_payload,
                            manager._session_id,
                            event_row["event_id"],
                        ),
                    )
                    connection.commit()
                self.assertTrue(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        """UPDATE upbit_functional_claims
                        SET state='ACKNOWLEDGED'
                        WHERE session_id=? AND slot='STRATEGY_BUY'""",
                        (manager._session_id,),
                    )
                    connection.commit()
                self.assertFalse(
                    store.durable_wiring_verified(
                        approval_id=approval["approvalId"],
                        session_id=manager._session_id,
                    )
                )


if __name__ == "__main__":
    unittest.main()
