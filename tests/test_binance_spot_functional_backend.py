from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
from pathlib import Path
import json
import tempfile
import threading
import unittest
from unittest.mock import patch

from live_trader import binance_spot_functional_backend as backend
from live_trader.binance_spot_continuous_functional import ExactBinding
from live_trader.binance_spot_functional_approval import (
    DurableBinanceSpotApprovedPermitStore,
)
from live_trader.binance_spot_functional_backend import (
    BinanceSpotFunctionalBackendError,
    BinanceSpotFunctionalBackendManager,
    issue_binance_spot_functional_permit,
)
from live_trader.binance_spot_functional_bootstrap import (
    DurableBinanceSpotFirstLiveBootstrapStore,
)
from live_trader.binance_spot_functional_lifecycle import LifecycleHandle
from tests.test_binance_spot_continuous_functional import (
    ACCOUNT_FINGERPRINT,
    binding,
)


class Clock:
    def __init__(self) -> None:
        self.value = 1_800_000_000.0

    def __call__(self) -> float:
        return self.value

    def iso(self) -> str:
        return datetime.fromtimestamp(
            self.value, tz=timezone.utc
        ).isoformat().replace("+00:00", "Z")


class FakeManager:
    allow_mock_lifecycle = True

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.phase = "IDLE"
        self.session_id = ""
        self.owner_id = ""
        self.owner_token = ""
        self.revision = 0
        self.cleanup_failures_before_commit = 0
        self.cleanup_raise_after_commit = False
        self.cleanup_calls = 0
        self.terminal_reset_override: bool | None = None
        self.start_pointer: dict[str, str] = {}
        self.recovery: object = {"startupRecovery": "NONE"}

    def audit_incomplete_startup(self):
        if isinstance(self.recovery, LifecycleHandle):
            self.session_id = self.recovery.session_id
            self.owner_id = self.recovery.owner_id
            self.owner_token = self.recovery.owner_token
            self.phase = "CLEANUP"
            self.revision = max(1, self.revision)
        return self.recovery

    def start(self, pointer, *, owner_id: str, owner_token: str):
        self.start_pointer = dict(pointer)
        self.session_id = "bnsft-backend-0000000000000001"
        self.owner_id = owner_id
        self.owner_token = owner_token
        self.phase = "ACTIVE"
        self.revision += 1
        return LifecycleHandle(
            session_id=self.session_id,
            capability="raw-functional-capability-must-never-escape",
            owner_id=owner_id,
            owner_token=owner_token,
            expires_epoch=self.clock() + 7200,
            cleanup_deadline_epoch=self.clock() + 10800,
        )

    def begin_cleanup(self, _handle, *, reason: str):
        self.cleanup_calls += 1
        if self.cleanup_failures_before_commit > 0:
            self.cleanup_failures_before_commit -= 1
            raise RuntimeError("durable cleanup write unavailable")
        self.session_id = _handle.session_id
        self.owner_id = _handle.owner_id
        self.owner_token = _handle.owner_token
        self.phase = "CLEANUP"
        self.revision += 1
        result = {
            "phase": self.phase,
            "sessionId": self.session_id,
            "ownerId": self.owner_id,
            "revision": self.revision,
            "detail": reason,
        }
        if self.cleanup_raise_after_commit:
            self.cleanup_raise_after_commit = False
            raise RuntimeError("response lost after durable cleanup commit")
        return result

    def status(self):
        terminal = (
            self.phase in {"FAILED", "FINALIZED"}
            if self.terminal_reset_override is None
            else self.terminal_reset_override
        )
        return {
            "phase": self.phase,
            "sessionId": self.session_id,
            "ownerId": self.owner_id,
            "revision": self.revision,
            "functionalCapabilityReset": terminal,
            "ownerTokenReset": terminal,
        }


class FakeScheduler:
    def __init__(self, *, manager: FakeManager) -> None:
        self.manager = manager

    def run(self, _handle, *, stop_event: threading.Event):
        stop_event.wait(2)
        self.manager.phase = "FINALIZED"
        return {"ok": True, "status": "FINALIZED"}


class EscapingThenCleanupScheduler:
    calls = 0

    def __init__(self, *, manager: FakeManager) -> None:
        self.manager = manager

    def run(self, _handle, *, stop_event: threading.Event):
        type(self).calls += 1
        if type(self).calls == 1:
            raise OSError("scheduler storage unavailable")
        self.manager.phase = "FINALIZED"
        return {"ok": True, "status": "FINALIZED"}


class ImmediateFinalScheduler:
    def __init__(self, *, manager: FakeManager) -> None:
        self.manager = manager

    def run(self, _handle, *, stop_event: threading.Event):
        del stop_event
        self.manager.phase = "FINALIZED"
        return {"ok": True, "status": "FINALIZED"}


class AlwaysEscapingScheduler:
    def __init__(self, *, manager: FakeManager) -> None:
        self.manager = manager

    def run(self, _handle, *, stop_event: threading.Event):
        del stop_event
        raise OSError("scheduler storage unavailable")


class FalseTerminalScheduler:
    def __init__(self, *, manager: FakeManager) -> None:
        self.manager = manager

    def run(self, _handle, *, stop_event: threading.Event):
        del stop_event
        return {"ok": True, "status": "FINALIZED"}


class RawReconciliationScheduler:
    def __init__(self, *, manager: FakeManager) -> None:
        self.manager = manager

    def run(self, _handle, *, stop_event: threading.Event):
        del stop_event
        return {"ok": False, "status": "RECONCILIATION_REQUIRED"}


class TerminalOnlyBootstrapStore:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.failed: list[tuple[str, str]] = []

    def active_terminal_pointer_for_session(self, session_id: str):
        if session_id != self.session_id:
            return None
        return {"bootstrap_id": "binance-first-live-recovered-terminal-only"}

    def fail(self, *, bootstrap_id: str, detail: str):
        self.failed.append((bootstrap_id, detail))
        return {"state": "FAILED"}


class TerminalEvidenceLedger:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id

    def session(self, session_id: str):
        if session_id != self.session_id:
            raise AssertionError("unexpected terminal session")
        return {
            "permit_id": "binance-terminal-permit-0001",
            "permit_hash": "a" * 64,
        }

    def final_evidence(self, session_id: str):
        if session_id != self.session_id:
            raise AssertionError("unexpected terminal evidence session")
        return {
            "evidence": {
                "sessionId": session_id,
                "outcome": "SAFE_INCOMPLETE_EARLY_TERMINATION",
            },
            "evidenceHash": "b" * 64,
        }


class FailingConsumeBootstrapStore:
    def __init__(self) -> None:
        self.consume_calls: list[dict[str, object]] = []
        self.failed: list[tuple[str, str]] = []

    def consume_terminal(self, **kwargs):
        self.consume_calls.append(dict(kwargs))
        raise RuntimeError("terminal bootstrap store unavailable")

    def fail(self, *, bootstrap_id: str, detail: str):
        self.failed.append((bootstrap_id, detail))
        return {"state": "FAILED"}


def route_lock() -> dict[str, bool]:
    return {
        "globalRealOrdersEnabled": False,
        "ordinaryRuntimeActive": False,
        "binanceSpotOrdinaryRouteClosed": True,
        "binanceSmokeRouteClosed": True,
        "binanceFuturesRouteClosed": True,
        "marginRouteClosed": True,
        "withdrawalRouteClosed": True,
    }


class BinanceSpotFunctionalBackendTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.clock = Clock()
        self.path = Path(self.temporary.name) / "backend.sqlite3"
        self.store = DurableBinanceSpotApprovedPermitStore(
            self.path,
            approval_verifier=lambda _: True,
            clock=self.clock,
        )
        self.manager = FakeManager(self.clock)
        self.backend = BinanceSpotFunctionalBackendManager(
            manager=self.manager,  # type: ignore[arg-type]
            approval_store=self.store,
            binding_reader=lambda: ExactBinding.parse(binding()),
            operator_confirmation_verifier=lambda value: (
                value.get("serverSignature") == "server-only-confirmation"
            ),
            server_record_signer=lambda value: {
                **dict(value),
                "serverSignature": "server-candidate-signature",
            },
            route_lock_reader=route_lock,
            clock=self.clock,
            scheduler_factory=FakeScheduler,
            stream_start=lambda: None,
            stream_ready=lambda: True,
            stream_stop=lambda: None,
            allow_mock_backend=True,
        )
        self.confirmation = {
            "authenticated": True,
            "confirmed": True,
            "source": "SERVER_SAFETY_CONFIRMATION",
            "serverSignature": "server-only-confirmation",
        }

    def tearDown(self) -> None:
        # The backend deliberately owns a daemon scheduler.  Unit-test temp
        # databases must wait for that private owner to release SQLite before
        # TemporaryDirectory removes the file on Windows.
        self.backend._scheduler_stop.set()
        thread = self.backend._scheduler_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3)

    def approve_candidate(self) -> dict[str, object]:
        candidate = self.backend.preissue_candidate()
        durable_candidate = self.store.candidate_status(
            str(candidate["approvalId"])
        )
        self.store.approve_issued_candidate(
            approval_id=str(candidate["approvalId"]),
            approval_attestation={
                "approvalId": candidate["approvalId"],
                "operatorId": "authenticated-operator",
                "operatorAuthenticated": True,
                "operatorApproved": True,
                "permitId": candidate["permitId"],
                "permitHash": candidate["permitHash"],
                "accountFingerprint": ACCOUNT_FINGERPRINT,
                "executionRoute": "BINANCE_SPOT_CONTINUOUS",
                "symbol": "BTCUSDT",
                "approvedAt": self.clock.iso(),
                "activationResealAuthorized": True,
                "activeDurationSeconds": 7200,
                "accountExclusivityProofRequired": True,
                "accountWideCausalClosureProofRequired": True,
                "firstLiveBootstrapAuthorized": True,
                "firstLiveBootstrapRequired": bool(
                    durable_candidate.get("first_live_bootstrap_required")
                ),
                "firstLiveBootstrapId": str(
                    durable_candidate.get("first_live_bootstrap_id") or ""
                ),
                "firstLiveBootstrapHash": str(
                    durable_candidate.get("first_live_bootstrap_hash") or ""
                ),
                "firstLiveSessionNonceHash": str(
                    durable_candidate.get("first_live_session_nonce_hash") or ""
                ),
                "firstLiveCodeHash": str(
                    durable_candidate.get("first_live_code_hash") or ""
                ),
                "nonce": "operator-approval-nonce-00000001",
            },
        )
        return candidate

    def test_all_binance_functional_production_flags_remain_false(self) -> None:
        self.assertFalse(backend.BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE)
        self.assertFalse(backend.BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE)
        self.assertFalse(backend.BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE)
        self.assertFalse(
            backend.BINANCE_SPOT_FUNCTIONAL_FIRST_LIVE_BOOTSTRAP_AVAILABLE
        )
        self.assertFalse(backend.BINANCE_SPOT_FUNCTIONAL_ORDINARY_FENCE_AVAILABLE)
        self.assertFalse(backend.BINANCE_SPOT_FUNCTIONAL_EMERGENCY_FENCE_AVAILABLE)
        self.assertFalse(
            backend.BINANCE_SPOT_FUNCTIONAL_EXCLUSIVE_ACCOUNT_AVAILABLE
        )

    def test_server_issues_only_exact_two_hour_nonpromotion_permit(self) -> None:
        candidate = self.backend.preissue_candidate()
        row = self.store.candidate_status(str(candidate["approvalId"]))
        self.assertEqual("ISSUED", row["state"])
        with closing(self.store._connect()) as connection:  # exact persisted body proof
            permit = json.loads(
                connection.execute(
                    "SELECT permit_json FROM binance_spot_functional_approvals"
                ).fetchone()["permit_json"]
            )
        self.assertEqual("10", permit["maxOrderNotional"])
        self.assertEqual("1", permit["maxOwnerLoss"])
        self.assertEqual(2, permit["sharedPermit"]["duration"]["value"])
        self.assertFalse(permit["promotionEligible"])
        self.assertFalse(permit["futuresAllowed"])
        self.assertFalse(permit["marginAllowed"])
        self.assertFalse(permit["withdrawalAllowed"])

    def test_expired_inert_candidate_is_failed_and_never_returned(self) -> None:
        candidate = self.backend.preissue_candidate()
        self.clock.value += 301
        with self.assertRaisesRegex(
            BinanceSpotFunctionalBackendError, "expired"
        ):
            self.backend.preissue_candidate(str(candidate["approvalId"]))
        self.assertEqual(
            "FAILED",
            self.store.candidate_status(str(candidate["approvalId"]))[
                "state"
            ],
        )
        replacement = self.backend.preissue_candidate()
        self.assertNotEqual(
            candidate["approvalId"], replacement["approvalId"]
        )

    def test_start_uses_only_approved_pointer_and_never_exposes_handle_secrets(self) -> None:
        candidate = self.approve_candidate()
        result = self.backend.start(
            {
                "approvalId": candidate["approvalId"],
                "operatorConfirmation": self.confirmation,
            }
        )
        encoded = json.dumps(result, sort_keys=True)
        self.assertNotIn("raw-functional-capability", encoded)
        self.assertNotIn("owner_token", encoded.lower())
        self.assertEqual(
            {"permitId", "permitHash"}, set(self.manager.start_pointer)
        )
        self.assertFalse(result["status"]["rawCapabilityExposed"])
        self.backend.stop({"operatorConfirmation": self.confirmation})

    def test_first_live_bootstrap_is_in_approved_envelope_and_binds_one_session(self) -> None:
        gates = {
            "allOtherProductionComponentsAvailable": True,
            "ordinaryBinanceRoutesClosed": True,
            "emergencyKillInactive": True,
            "applicationInstanceLeaseHeld": True,
            "operatorApprovalBound": True,
            "accountExclusivityVerifierReady": True,
            "accountExclusivityDurableProviderReady": True,
            "accountExclusivitySigningPrimitiveAbsent": True,
            "accountExclusivityAuthorityPinned": True,
            "accountIdentityPinned": True,
            "globalFirstLiveAuthorityReaderWired": True,
            "realE2EAvailable": False,
            "firstLiveBootstrapFeatureEnabled": True,
        }
        first_live = DurableBinanceSpotFirstLiveBootstrapStore(
            self.path,
            gate_reader=lambda: dict(gates),
            server_record_signer=lambda value: {
                **dict(value),
                "serverSignature": "server-first-live-signature",
            },
            code_hash_reader=lambda: "d" * 64,
            clock=self.clock,
        )
        self.backend = BinanceSpotFunctionalBackendManager(
            manager=self.manager,  # type: ignore[arg-type]
            approval_store=self.store,
            binding_reader=lambda: ExactBinding.parse(binding()),
            operator_confirmation_verifier=lambda value: (
                value.get("serverSignature") == "server-only-confirmation"
            ),
            server_record_signer=lambda value: {
                **dict(value),
                "serverSignature": "server-candidate-signature",
            },
            route_lock_reader=route_lock,
            clock=self.clock,
            scheduler_factory=FakeScheduler,
            stream_start=lambda: None,
            stream_ready=lambda: True,
            stream_stop=lambda: None,
            first_live_bootstrap_store=first_live,
            first_live_gate_reader=lambda: dict(gates),
            allow_mock_backend=True,
        )
        candidate = self.approve_candidate()
        durable = self.store.candidate_status(str(candidate["approvalId"]))
        self.assertEqual(1, durable["first_live_bootstrap_required"])
        self.assertRegex(str(durable["first_live_bootstrap_hash"]), r"^[0-9a-f]{64}$")
        self.assertRegex(
            str(durable["first_live_session_nonce_hash"]), r"^[0-9a-f]{64}$"
        )
        result = self.backend.start(
            {
                "approvalId": candidate["approvalId"],
                "operatorConfirmation": self.confirmation,
            }
        )
        active = first_live.pointer_for_approval(str(candidate["approvalId"]))
        self.assertIsNotNone(active)
        self.assertEqual("ACTIVE", active["state"])
        self.assertEqual(result["sessionId"], active["session_id"])
        self.assertTrue(result["status"]["firstLiveBootstrapActive"])
        self.assertNotIn("session_nonce", json.dumps(result, sort_keys=True).lower())
        self.backend.stop({"operatorConfirmation": self.confirmation})

    def test_scheduler_spawn_failure_happens_before_lifecycle_activation(self) -> None:
        candidate = self.approve_candidate()
        with patch(
            "live_trader.binance_spot_functional_backend.threading.Thread.start",
            side_effect=OSError("thread quota exhausted"),
        ), self.assertRaisesRegex(OSError, "thread quota"):
            self.backend.start(
                {
                    "approvalId": candidate["approvalId"],
                    "operatorConfirmation": self.confirmation,
                }
            )
        self.assertEqual("IDLE", self.manager.phase)
        self.assertEqual({}, self.manager.start_pointer)
        self.assertEqual(
            "FAILED",
            self.store.candidate_status(str(candidate["approvalId"]))[
                "state"
            ],
        )

    def test_scheduler_escape_latches_cleanup_and_keeps_handle_for_retry(self) -> None:
        candidate = self.approve_candidate()
        EscapingThenCleanupScheduler.calls = 0
        self.backend.scheduler_factory = EscapingThenCleanupScheduler
        result = self.backend.start(
            {
                "approvalId": candidate["approvalId"],
                "operatorConfirmation": self.confirmation,
            }
        )
        self.assertTrue(result["ok"])
        deadline = threading.Event()
        for _ in range(100):
            if self.backend.status()["terminalState"] == "FINALIZED":
                break
            deadline.wait(0.01)
        self.assertGreaterEqual(EscapingThenCleanupScheduler.calls, 2)
        self.assertEqual("FINALIZED", self.manager.phase)
        self.assertEqual("FINALIZED", self.backend.status()["terminalState"])

    def test_constructor_recovery_spawn_failure_retains_handle_for_in_process_retry(
        self,
    ) -> None:
        recovered = LifecycleHandle(
            session_id="bnsft-constructor-recovery-000001",
            capability="constructor-cleanup-secret-never-exposed",
            owner_id="constructor-recovery-owner",
            owner_token="constructor-recovery-owner-token",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        manager = FakeManager(self.clock)
        manager.phase = "CLEANUP"
        manager.session_id = recovered.session_id
        manager.recovery = recovered
        with patch(
            "live_trader.binance_spot_functional_backend.threading.Thread.start",
            side_effect=OSError("constructor thread quota exhausted"),
        ):
            recovered_backend = BinanceSpotFunctionalBackendManager(
                manager=manager,  # type: ignore[arg-type]
                approval_store=self.store,
                binding_reader=lambda: ExactBinding.parse(binding()),
                operator_confirmation_verifier=lambda value: (
                    value.get("serverSignature") == "server-only-confirmation"
                ),
                server_record_signer=lambda value: dict(value),
                route_lock_reader=route_lock,
                clock=self.clock,
                scheduler_factory=ImmediateFinalScheduler,
                allow_mock_backend=True,
            )
        failed_status = recovered_backend.status()
        self.assertEqual("CLEANUP_RETRY_REQUIRED", failed_status["terminalState"])
        self.assertTrue(failed_status["cleanupSchedulerRetryRequired"])
        self.assertFalse(failed_status["schedulerRunning"])
        self.assertNotIn(
            "constructor-cleanup-secret",
            json.dumps(failed_status, sort_keys=True),
        )

        retry = recovered_backend.recover(
            {"operatorConfirmation": self.confirmation}
        )
        self.assertTrue(retry["ok"])
        self.assertFalse(retry["pending"])
        for _ in range(100):
            if recovered_backend.status()["terminalState"] == "FINALIZED":
                break
            threading.Event().wait(0.01)
        self.assertEqual("FINALIZED", recovered_backend.status()["terminalState"])

    def test_restarted_active_bootstrap_is_terminal_only_and_never_entry_authority(
        self,
    ) -> None:
        recovered = LifecycleHandle(
            session_id="bnsft-bootstrap-terminal-recovery-0001",
            capability="terminal-cleanup-secret-never-exposed",
            owner_id="terminal-recovery-owner",
            owner_token="terminal-recovery-owner-token",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        manager = FakeManager(self.clock)
        manager.phase = "CLEANUP"
        manager.session_id = recovered.session_id
        manager.recovery = recovered
        bootstrap_store = TerminalOnlyBootstrapStore(recovered.session_id)
        with patch.object(
            backend, "binance_spot_first_live_bootstrap_available", return_value=True
        ):
            recovered_backend = BinanceSpotFunctionalBackendManager(
                manager=manager,  # type: ignore[arg-type]
                approval_store=self.store,
                binding_reader=lambda: ExactBinding.parse(binding()),
                operator_confirmation_verifier=lambda value: (
                    value.get("serverSignature") == "server-only-confirmation"
                ),
                server_record_signer=lambda value: dict(value),
                route_lock_reader=route_lock,
                clock=self.clock,
                scheduler_factory=FakeScheduler,
                first_live_bootstrap_store=bootstrap_store,  # type: ignore[arg-type]
                allow_mock_backend=True,
            )
            status = recovered_backend.status()
            self.assertFalse(status["networkOrderPostAllowed"])
            self.assertFalse(status["firstLiveBootstrapActive"])
            self.assertTrue(status["firstLiveBootstrapTerminalRecoveryPending"])
            recovered_backend._scheduler_stop.set()
            thread = recovered_backend._scheduler_thread
            if thread is not None:
                thread.join(timeout=3)
        self.assertEqual(1, len(bootstrap_store.failed))
        self.assertEqual(
            "binance-first-live-recovered-terminal-only",
            bootstrap_store.failed[0][0],
        )

    def test_finalized_lifecycle_consume_failure_stays_reconciliation(self) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-finalized-consume-fault-0001",
            capability="finalized-consume-capability-private",
            owner_id="finalized-consume-owner",
            owner_token="finalized-consume-token-00000000001",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        bootstrap_id = "binance-first-live-consume-fault-0001"
        bootstrap_store = FailingConsumeBootstrapStore()
        self.manager.phase = "FINALIZED"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 19
        self.manager.ledger = TerminalEvidenceLedger(  # type: ignore[attr-defined]
            handle.session_id
        )
        self.backend.first_live_bootstrap_store = bootstrap_store  # type: ignore[assignment]
        self.backend.scheduler_factory = FalseTerminalScheduler
        self.backend._handle = handle
        self.backend._active_first_live_bootstrap_id = bootstrap_id
        self.backend._generation = 15

        self.backend._run_scheduler(15)

        status = self.backend.status()
        self.assertEqual("FINALIZED", self.manager.phase)
        self.assertEqual("RECONCILIATION_REQUIRED", status["terminalState"])
        self.assertFalse(self.backend._last_result["ok"])
        self.assertFalse(
            self.backend._last_result["firstLiveBootstrapConsumed"]
        )
        self.assertEqual(
            "FINALIZED", self.backend._last_result["durableLifecyclePhase"]
        )
        self.assertEqual(1, len(bootstrap_store.consume_calls))
        self.assertEqual(1, len(bootstrap_store.failed))
        self.assertEqual(bootstrap_id, bootstrap_store.failed[0][0])
        self.assertIsNone(self.backend._handle)

    def test_raw_reconciliation_does_not_burn_durable_cleanup_bootstrap(self) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-raw-reconcile-cleanup-0001",
            capability="raw-reconcile-cleanup-capability-private",
            owner_id="raw-reconcile-cleanup-owner",
            owner_token="raw-reconcile-cleanup-token-00000001",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        bootstrap_store = TerminalOnlyBootstrapStore(handle.session_id)
        self.manager.phase = "CLEANUP"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 23
        self.backend.first_live_bootstrap_store = bootstrap_store  # type: ignore[assignment]
        self.backend.scheduler_factory = RawReconciliationScheduler
        self.backend._handle = handle
        self.backend._active_first_live_bootstrap_id = (
            "binance-first-live-raw-reconcile-0001"
        )
        self.backend._generation = 16

        self.backend._run_scheduler(16)

        self.assertEqual([], bootstrap_store.failed)
        self.assertIs(handle, self.backend._handle)
        self.assertEqual(
            "CLEANUP_RETRY_REQUIRED", self.backend._terminal_state
        )

    def test_explicit_recovery_spawn_failure_is_retryable_without_secret_loss(
        self,
    ) -> None:
        recovered = LifecycleHandle(
            session_id="bnsft-explicit-recovery-00000001",
            capability="explicit-cleanup-secret-never-exposed",
            owner_id="explicit-recovery-owner",
            owner_token="explicit-recovery-owner-token",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        self.manager.phase = "CLEANUP"
        self.manager.session_id = recovered.session_id
        self.manager.recovery = recovered
        self.backend.scheduler_factory = ImmediateFinalScheduler
        with patch(
            "live_trader.binance_spot_functional_backend.threading.Thread.start",
            side_effect=OSError("recovery thread quota exhausted"),
        ):
            first = self.backend.recover(
                {"operatorConfirmation": self.confirmation}
            )
        self.assertFalse(first["ok"])
        self.assertTrue(first["pending"])
        self.assertTrue(first["retryable"])
        self.assertTrue(
            self.backend.status()["cleanupSchedulerRetryRequired"]
        )
        self.assertNotIn(
            "explicit-cleanup-secret",
            json.dumps(first, sort_keys=True),
        )

        second = self.backend.recover(
            {"operatorConfirmation": self.confirmation}
        )
        self.assertTrue(second["ok"])
        for _ in range(100):
            if self.backend.status()["terminalState"] == "FINALIZED":
                break
            threading.Event().wait(0.01)
        self.assertEqual("FINALIZED", self.backend.status()["terminalState"])

    def test_scheduler_escape_reschedule_spawn_failure_remains_recoverable(
        self,
    ) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-escape-recovery-000000001",
            capability="escape-cleanup-secret-never-exposed",
            owner_id="escape-recovery-owner",
            owner_token="escape-recovery-owner-token",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        self.manager.phase = "ACTIVE"
        self.manager.session_id = handle.session_id
        self.backend._handle = handle
        self.backend._generation = 1
        self.backend.scheduler_factory = AlwaysEscapingScheduler
        with patch(
            "live_trader.binance_spot_functional_backend.threading.Thread.start",
            side_effect=OSError("reschedule thread quota exhausted"),
        ):
            self.backend._run_scheduler(1)

        failed_status = self.backend.status()
        self.assertEqual("CLEANUP", self.manager.phase)
        self.assertEqual("CLEANUP_RETRY_REQUIRED", failed_status["terminalState"])
        self.assertTrue(failed_status["cleanupSchedulerRetryRequired"])
        self.assertFalse(failed_status["schedulerRunning"])
        self.assertNotIn(
            "escape-cleanup-secret",
            json.dumps(failed_status, sort_keys=True),
        )

        self.backend.scheduler_factory = ImmediateFinalScheduler
        retry = self.backend.recover(
            {"operatorConfirmation": self.confirmation}
        )
        self.assertTrue(retry["ok"])
        for _ in range(100):
            if self.backend.status()["terminalState"] == "FINALIZED":
                break
            threading.Event().wait(0.01)
        self.assertEqual("FINALIZED", self.backend.status()["terminalState"])

    def test_operator_stop_failure_preserves_handle_until_exact_cleanup_retry(
        self,
    ) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-stop-retry-00000000000001",
            capability="stop-retry-capability-never-exposed",
            owner_id="stop-retry-owner",
            owner_token="stop-retry-owner-token-00000000001",
            expires_epoch=self.clock() + 7200,
            cleanup_deadline_epoch=self.clock() + 10800,
        )
        self.manager.phase = "ACTIVE"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 7
        self.manager.cleanup_failures_before_commit = 1
        self.backend._handle = handle

        failed = self.backend.stop(
            {"operatorConfirmation": self.confirmation}
        )
        self.assertFalse(failed["ok"])
        self.assertTrue(failed["retryable"])
        self.assertEqual("ACTIVE", self.manager.phase)
        self.assertIs(handle, self.backend._handle)
        self.assertEqual(
            "CLEANUP_RETRY_REQUIRED",
            failed["status"]["terminalState"],
        )
        self.assertFalse(failed["brokerSubmissionPerformed"])

        retried = self.backend.stop(
            {"operatorConfirmation": self.confirmation}
        )
        self.assertTrue(retried["ok"])
        self.assertEqual("CLEANUP", self.manager.phase)
        self.assertEqual(8, retried["durableCleanupRevision"])
        self.assertIs(handle, self.backend._handle)

    def test_operator_stop_accepts_exception_only_after_fresh_durable_commit(
        self,
    ) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-stop-after-commit-00000001",
            capability="stop-after-commit-capability-never-exposed",
            owner_id="stop-after-commit-owner",
            owner_token="stop-after-commit-token-000000000001",
            expires_epoch=self.clock() + 7200,
            cleanup_deadline_epoch=self.clock() + 10800,
        )
        self.manager.phase = "ACTIVE"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 11
        self.manager.cleanup_raise_after_commit = True
        self.backend._handle = handle

        stopped = self.backend.stop(
            {"operatorConfirmation": self.confirmation}
        )

        self.assertTrue(stopped["ok"])
        self.assertEqual(12, stopped["durableCleanupRevision"])
        self.assertEqual("CLEANUP", self.manager.phase)
        self.assertIs(handle, self.backend._handle)
        self.assertFalse(stopped["brokerSubmissionPerformed"])

    def test_scheduler_escape_cleanup_and_revocation_fault_preserves_handle(
        self,
    ) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-escape-double-fault-000001",
            capability="escape-double-fault-capability-private",
            owner_id="escape-double-fault-owner",
            owner_token="escape-double-fault-token-000000001",
            expires_epoch=self.clock() + 7200,
            cleanup_deadline_epoch=self.clock() + 10800,
        )
        self.manager.phase = "ACTIVE"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 3
        self.manager.cleanup_failures_before_commit = 1
        self.backend._handle = handle
        self.backend._generation = 9
        self.backend.scheduler_factory = AlwaysEscapingScheduler

        self.backend._run_scheduler(9)

        status = self.backend.status()
        self.assertEqual("ACTIVE", self.manager.phase)
        self.assertEqual("CLEANUP_RETRY_REQUIRED", status["terminalState"])
        self.assertIs(handle, self.backend._handle)
        self.assertNotIn(
            "escape-double-fault-capability", json.dumps(status, sort_keys=True)
        )

    def test_scheduler_terminal_claim_cannot_clear_active_durable_handle(
        self,
    ) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-false-terminal-0000000001",
            capability="false-terminal-capability-private",
            owner_id="false-terminal-owner",
            owner_token="false-terminal-token-000000000001",
            expires_epoch=self.clock() + 7200,
            cleanup_deadline_epoch=self.clock() + 10800,
        )
        self.manager.phase = "ACTIVE"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 5
        self.backend._handle = handle
        self.backend._generation = 12
        self.backend.scheduler_factory = FalseTerminalScheduler

        self.backend._run_scheduler(12)

        status = self.backend.status()
        self.assertEqual("ACTIVE", self.manager.phase)
        self.assertEqual("CLEANUP_RETRY_REQUIRED", status["terminalState"])
        self.assertIs(handle, self.backend._handle)
        self.assertFalse(status["schedulerRunning"])

    def test_terminal_phase_without_both_secret_resets_retains_handle(self) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-unreset-terminal-000000001",
            capability="unreset-terminal-capability-private",
            owner_id="unreset-terminal-owner",
            owner_token="unreset-terminal-token-00000000001",
            expires_epoch=self.clock() + 7200,
            cleanup_deadline_epoch=self.clock() + 10800,
        )
        self.manager.phase = "FAILED"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 6
        self.manager.terminal_reset_override = False
        self.backend._handle = handle
        self.backend._generation = 13
        self.backend.scheduler_factory = FalseTerminalScheduler

        self.backend._run_scheduler(13)

        self.assertIs(handle, self.backend._handle)
        self.assertEqual("CLEANUP_RETRY_REQUIRED", self.backend._terminal_state)

    def test_unreadable_durable_terminal_control_retains_handle(self) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-unknown-terminal-000000001",
            capability="unknown-terminal-capability-private",
            owner_id="unknown-terminal-owner",
            owner_token="unknown-terminal-token-00000000001",
            expires_epoch=self.clock() + 7200,
            cleanup_deadline_epoch=self.clock() + 10800,
        )
        self.backend._handle = handle
        self.backend._generation = 14
        self.backend.scheduler_factory = FalseTerminalScheduler
        self.manager.status = lambda: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("durable control unreadable")
        )

        self.backend._run_scheduler(14)

        self.assertIs(handle, self.backend._handle)
        self.assertEqual("CLEANUP_RETRY_REQUIRED", self.backend._terminal_state)

    def test_final_reset_in_process_recover_resumes_without_cleanup_reentry(
        self,
    ) -> None:
        handle = LifecycleHandle(
            session_id="bnsft-final-reset-recover-000001",
            capability="final-reset-recover-capability-private",
            owner_id="final-reset-recover-owner",
            owner_token="final-reset-recover-token-000000001",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        self.manager.phase = "FINAL_RESET"
        self.manager.session_id = handle.session_id
        self.manager.owner_id = handle.owner_id
        self.manager.owner_token = handle.owner_token
        self.manager.revision = 17
        self.backend._handle = handle
        self.backend.scheduler_factory = ImmediateFinalScheduler

        recovered = self.backend.recover(
            {"operatorConfirmation": self.confirmation}
        )
        self.assertTrue(recovered["ok"])
        for _ in range(100):
            if self.backend.status()["terminalState"] == "FINALIZED":
                break
            threading.Event().wait(0.01)
        self.assertEqual(0, self.manager.cleanup_calls)
        self.assertEqual("FINALIZED", self.backend.status()["terminalState"])

    def test_raw_permit_bar_signal_capability_and_caps_are_rejected(self) -> None:
        candidate = self.approve_candidate()
        base = {
            "approvalId": candidate["approvalId"],
            "operatorConfirmation": self.confirmation,
        }
        for field in (
            "permit",
            "bar",
            "signal",
            "capability",
            "accountFingerprint",
            "caps",
            "payload",
            "extra",
        ):
            with self.subTest(field=field), self.assertRaises(
                BinanceSpotFunctionalBackendError
            ):
                self.backend.start({**base, field: "client-forbidden"})
        self.assertEqual({}, self.manager.start_pointer)

    def test_ordinary_smoke_or_futures_route_open_blocks_before_claim(self) -> None:
        candidate = self.approve_candidate()
        self.backend.route_lock_reader = lambda: {
            **route_lock(),
            "binanceFuturesRouteClosed": False,
        }
        with self.assertRaisesRegex(
            BinanceSpotFunctionalBackendError, "routes are not closed"
        ):
            self.backend.start(
                {
                    "approvalId": candidate["approvalId"],
                    "operatorConfirmation": self.confirmation,
                }
            )
        self.assertEqual("APPROVED", self.store.candidate_status(
            str(candidate["approvalId"])
        )["state"])

    def test_stream_prebaseline_failure_consumes_typed_approval_without_start(self) -> None:
        candidate = self.approve_candidate()
        self.backend.stream_start = lambda: (_ for _ in ()).throw(
            RuntimeError("stream unavailable")
        )
        with self.assertRaisesRegex(RuntimeError, "stream unavailable"):
            self.backend.start(
                {
                    "approvalId": candidate["approvalId"],
                    "operatorConfirmation": self.confirmation,
                }
            )
        self.assertEqual(
            "FAILED",
            self.store.candidate_status(str(candidate["approvalId"]))["state"],
        )
        self.assertEqual({}, self.manager.start_pointer)

    def test_recover_rotated_handle_stays_private(self) -> None:
        self.manager.recovery = LifecycleHandle(
            session_id="bnsft-recovery-00000000000001",
            capability="rotated-cleanup-secret-never-exposed",
            owner_id="backend-recovery-owner",
            owner_token="backend-recovery-owner-token",
            expires_epoch=self.clock(),
            cleanup_deadline_epoch=self.clock() + 3600,
        )
        result = self.backend.recover(
            {"operatorConfirmation": self.confirmation}
        )
        encoded = json.dumps(result, sort_keys=True)
        self.assertNotIn("rotated-cleanup-secret", encoded)
        self.assertTrue(result["cleanupOnly"])
        self.backend.stop({"operatorConfirmation": self.confirmation})

    def test_composite_availability_needs_every_component(self) -> None:
        with (
            patch.object(backend, "BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE", True),
            patch.object(backend, "BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE", True),
            patch.object(backend, "BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE", True),
            patch.object(backend, "composite_production_available", return_value=False),
        ):
            self.assertFalse(backend.binance_spot_functional_composite_available())

    def test_production_builder_rejects_custom_origin_before_credentials_or_network(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "BINANCE_BASE_URL": "https://testnet.binance.vision",
                "BINANCE_API_KEY": "must-not-be-sent",
                "BINANCE_API_SECRET": "must-not-be-sent",
            },
            clear=False,
        ), self.assertRaisesRegex(RuntimeError, "exact https://api.binance.com"):
            backend.build_binance_spot_functional_production_backend(
                database_path=self.path,
                publication_proof_path=Path(self.temporary.name) / "missing.json",
                data_root=self.temporary.name,
                approval_verifier=lambda _: True,
                operator_confirmation_verifier=lambda _: True,
                server_record_signer=lambda value: dict(value),
                route_lock_reader=route_lock,
                dispatch_lease_factory=lambda **_: (_ for _ in ()).throw(
                    AssertionError("invalid origin must block before lease")
                ),
                clock=self.clock,
            )

    def test_production_builder_rejects_direct_use_without_application_lease(self) -> None:
        with (
            patch.dict(
                "os.environ",
                {
                    "BINANCE_BASE_URL": "https://api.binance.com",
                    "BINANCE_API_KEY": "must-not-be-sent",
                    "BINANCE_API_SECRET": "must-not-be-sent",
                },
                clear=False,
            ),
            patch.object(
                backend,
                "live_trader_instance_lease_status",
                return_value={
                    "acquired": False,
                    "reason": "process-lease-not-held-by-current-process",
                },
            ),
            self.assertRaisesRegex(
                BinanceSpotFunctionalBackendError,
                "application-instance lease is not held",
            ),
        ):
            backend.build_binance_spot_functional_production_backend(
                database_path=self.path,
                publication_proof_path=Path(self.temporary.name) / "missing.json",
                data_root=self.temporary.name,
                approval_verifier=lambda _: True,
                operator_confirmation_verifier=lambda _: True,
                server_record_signer=lambda value: dict(value),
                route_lock_reader=route_lock,
                dispatch_lease_factory=lambda **_: (_ for _ in ()).throw(
                    AssertionError("lease must not be entered")
                ),
                clock=self.clock,
            )

    def test_startup_owner_absence_attestation_is_process_one_shot(self) -> None:
        with (
            patch.object(
                backend,
                "_STARTUP_ATTESTATION_MINTED",
                False,
            ),
            patch.object(
                backend,
                "_STARTUP_ATTESTATION_CONSUMED",
                False,
            ),
            patch.object(
                backend,
                "live_trader_instance_lease_status",
                return_value={"acquired": True, "ownerPid": 123},
            ),
        ):
            attestation = backend._mint_startup_owner_absence_attestation()
            self.assertTrue(
                backend._consume_startup_owner_absence_attestation(
                    attestation
                )
            )
            with self.assertRaisesRegex(
                BinanceSpotFunctionalBackendError, "consumed"
            ):
                backend._consume_startup_owner_absence_attestation(
                    attestation
                )

    def test_permit_builder_rejects_no_client_customization_surface(self) -> None:
        payload = issue_binance_spot_functional_permit(
            binding=ExactBinding.parse(binding()), now_epoch=self.clock()
        )
        self.assertEqual("BTCUSDT", payload["binding"]["symbol"])
        self.assertEqual("BINANCE_SPOT_CONTINUOUS", payload["binding"]["executionRoute"])
        self.assertEqual(1, payload["maxBuyOrders"])
        self.assertEqual(1, payload["maxSellOrders"])


if __name__ == "__main__":
    unittest.main()
