from __future__ import annotations

"""Server-owned manager for the isolated Upbit continuous functional lane.

The command surface names only durable server records and an authenticated
operator confirmation.  It never accepts a raw permit, bar, strategy signal,
runtime capability, broker payload, or account fingerprint from a client.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .upbit_continuous_functional import (
    UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED,
    UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED,
    UpbitFunctionalBlocked,
)
from .upbit_functional_approval import (
    DurableUpbitFunctionalApprovalStore,
    _functional_wiring_evidence_complete,
)
from .upbit_functional_entrypoint import (
    UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE,
    _build_upbit_functional_server_graph,
    build_upbit_functional_production_graph,
    production_entrypoint_status,
)
from .upbit_functional_mutation import UPBIT_FUNCTIONAL_MUTATION_AVAILABLE
from .upbit_functional_sources import (
    OfficialUpbitFinalizedFiveMinuteWindowReader,
    OfficialUpbitFunctionalMyOrderPump,
)
from .upbit_functional_transport import upbit_credential_fingerprint
from .upbit_functional_transport import OfficialUpbitFunctionalGetClient
from .upbit_functional_truth import OfficialUpbitFunctionalTruthReader


UPBIT_FUNCTIONAL_BACKEND_AVAILABLE = False
UPBIT_FUNCTIONAL_STATE_SERVER_WIRING_AVAILABLE = False
UPBIT_FUNCTIONAL_REAL_E2E_AVAILABLE = False
_POLL_SECONDS = 5.0
_SCHEDULER_START_ATTEMPTS = 3
_BACKEND_SINGLETON_LOCK = threading.RLock()
_BACKEND_SINGLETON: "UpbitFunctionalBackendManager | None" = None
_BACKEND_CONSTRUCTION_CAPABILITY = object()


def upbit_functional_composite_available() -> bool:
    """Only this all-of gate may authorize construction for live mutation."""

    return all(
        (
            os.environ.get("UPBIT_FUNCTIONAL_LIVE_ENABLED", "").strip().lower()
            == "true",
            UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE,
            UPBIT_FUNCTIONAL_MUTATION_AVAILABLE,
            UPBIT_FUNCTIONAL_BACKEND_AVAILABLE,
            UPBIT_FUNCTIONAL_STATE_SERVER_WIRING_AVAILABLE,
            UPBIT_FUNCTIONAL_REAL_E2E_AVAILABLE,
            UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED,
            UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED,
            production_entrypoint_status()["available"],
        )
    )


class _ManagedFunctionalAuthority:
    """Process-local runtime pointer; raw capability is never exposed."""

    def __init__(
        self,
        *,
        ordinary_routes_closed_reader: Callable[[], bool],
        emergency_stop_reader: Callable[[], Mapping[str, Any]],
    ) -> None:
        self._lock = threading.RLock()
        self._ordinary_routes_closed_reader = ordinary_routes_closed_reader
        self._emergency_stop_reader = emergency_stop_reader
        self._scope: dict[str, Any] = {}
        self._capability_hash = ""
        self._armed = False
        self._cleanup = False

    def bind_scope(self, value: Mapping[str, Any]) -> None:
        with self._lock:
            self._scope = dict(value)
            self._capability_hash = ""
            self._armed = False
            self._cleanup = False

    def register(self, capability_hash: str) -> None:
        with self._lock:
            self._capability_hash = str(capability_hash or "").strip().lower()

    def arm(self, capability_hash: str) -> None:
        normalized = str(capability_hash or "").strip().lower()
        with self._lock:
            if not normalized or not secrets.compare_digest(
                normalized, self._capability_hash
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-capability-arm-mismatch"
                )
            self._armed = True

    def cleanup(self) -> None:
        with self._lock:
            self._cleanup = True

    def disarm(self) -> None:
        """Close POST reachability while preserving core reset authority."""

        with self._lock:
            self._armed = False

    def clear(self) -> None:
        with self._lock:
            self._capability_hash = ""
            self._armed = False
            self._cleanup = True

    def orders_enabled(self) -> bool:
        with self._lock:
            emergency = dict(self._emergency_stop_reader())
            return bool(
                self._armed
                and self._capability_hash
                and self._ordinary_routes_closed_reader() is True
                and emergency.get("active") is not True
            )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            ordinary_routes_closed = (
                self._ordinary_routes_closed_reader() is True
            )
            emergency = dict(self._emergency_stop_reader())
            kill_switch = emergency.get("active") is True
            return {
                **self._scope,
                "killSwitch": kill_switch,
                "killSwitchRevision": str(emergency.get("revision") or ""),
                "cleanupOnly": self._cleanup,
                "dryRun": False,
                "operatorConfirmed": True,
                "newEntriesBlocked": ordinary_routes_closed,
                "realOrdersEnabled": self._armed
                and ordinary_routes_closed
                and not kill_switch,
                "functionalMutationEnabled": self._armed
                and ordinary_routes_closed
                and not kill_switch,
                "functionalOnlyRouting": True,
                "ordinaryRoutesClosed": ordinary_routes_closed,
                "upbitSmokeRouteClosed": ordinary_routes_closed,
                "functionalCapabilityHash": self._capability_hash,
            }


class UpbitFunctionalBackendManager:
    def __init__(
        self,
        *,
        database_path: str | Path,
        publication_proof_path: str | Path,
        clock: Callable[[], datetime],
        approval_store: DurableUpbitFunctionalApprovalStore,
        sender: Callable[..., Mapping[str, Any]],
        lease_reader_factory: Callable[..., Any],
        operator_confirmation_verifier: Callable[[Mapping[str, Any]], bool],
        ordinary_routes_closed_reader: Callable[[], bool] | None = None,
        emergency_stop_reader: Callable[[], Mapping[str, Any]] | None = None,
        approval_record_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
        websocket_source: OfficialUpbitFunctionalMyOrderPump | None = None,
        candle_source: OfficialUpbitFinalizedFiveMinuteWindowReader | None = None,
        allow_mock_graph: bool = False,
        _capability: object | None = None,
    ) -> None:
        if not allow_mock_graph and _capability is not _BACKEND_CONSTRUCTION_CAPABILITY:
            # Check before any SQLite graph/audit construction.  Production
            # ownership is minted only by the application-lease-aware
            # singleton prepare function below.
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-direct-construction-forbidden"
            )
        # Construction is read-only with respect to the broker and is needed
        # at server startup to audit crash-left durable state while the live
        # composite gate is still false.  `start` performs the irreversible
        # availability check immediately before claiming an approval.
        self._allow_mock_graph = bool(allow_mock_graph)
        self.clock = clock
        self.database_path = Path(database_path)
        self.approval_store = approval_store
        self.operator_confirmation_verifier = operator_confirmation_verifier
        if approval_record_signer is None:
            if not allow_mock_graph:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-approval-record-signer-required"
                )
            approval_record_signer = lambda value: dict(value)
        self._approval_record_signer = approval_record_signer
        if ordinary_routes_closed_reader is None:
            if not allow_mock_graph:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-ordinary-route-reader-required"
                )
            ordinary_routes_closed_reader = lambda: True
        if emergency_stop_reader is None:
            if not allow_mock_graph:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-emergency-reader-required"
                )
            emergency_stop_reader = lambda: {
                "active": False,
                "revision": "mock-emergency-clear",
            }
        self.authority = _ManagedFunctionalAuthority(
            ordinary_routes_closed_reader=ordinary_routes_closed_reader,
            emergency_stop_reader=emergency_stop_reader,
        )
        self._lock = threading.RLock()
        self._generation = 0
        self._approval_id = ""
        self._session_id = ""
        self._scheduler_stop = threading.Event()
        self._scheduler: threading.Thread | None = None
        self._terminal_state = "IDLE"
        self._terminal_detail = ""
        account_fingerprint = upbit_credential_fingerprint()
        if not account_fingerprint:
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-account-credential-missing"
            )
        self.websocket_source = websocket_source or (
            OfficialUpbitFunctionalMyOrderPump(
                expected_account_fingerprint=account_fingerprint,
                clock=clock,
            )
        )
        self.candle_source = candle_source or (
            OfficialUpbitFinalizedFiveMinuteWindowReader(clock=clock)
        )

        def permit_reader(permit_id: str, permit_hash: str) -> Mapping[str, Any]:
            record = self.approval_store.permit_reader(
                self._approval_id, session_id=self._session_id
            )
            if (
                record["permitId"] != permit_id
                or record["permitHash"] != permit_hash
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-permit-pointer-mismatch"
                )
            return record

        graph_builder = (
            build_upbit_functional_production_graph
            if allow_mock_graph
            else _build_upbit_functional_server_graph
        )
        self.graph = graph_builder(
            database_path=self.database_path,
            publication_proof_path=publication_proof_path,
            account_fingerprint=account_fingerprint,
            clock=clock,
            runtime_reader=self.authority.snapshot,
            runtime_capability_registrar=self.authority.register,
            enter_cleanup_latch=self.authority.cleanup,
            disarm_functional_orders=self.authority.disarm,
            functional_orders_reader=self.authority.orders_enabled,
            lease_reader_factory=lease_reader_factory,
            sender=sender,
            approved_permit_reader=permit_reader,
            approved_recovery_reader=self.approval_store.recovery_reader,
            websocket_handshake=self.websocket_source.handshake,
            terminal_stream_barrier=self.websocket_source.terminal_barrier,
            finalized_bar_window_reader=self.candle_source,
            arm_functional_orders=self.authority.arm,
            clear_runtime_capability=self.authority.clear,
            **({"allow_mock_graph": True} if allow_mock_graph else {}),
        )
        # Preserve the full immutable identity for the approval-side startup
        # CAS.  A state string alone cannot safely repair the hard-crash
        # window after graph activation but before approval ``bind_permit``.
        durable = {
            str(row["session_id"]): dict(row)
            for row in self.graph.ledger.sessions()
        }
        durable_claims = {
            session_id: self.graph.ledger.claims(session_id)
            for session_id in durable
        }
        active_journals = {
            str(row["session_id"]): dict(row)
            for row in self.graph.journal.active_sessions()
        }
        approval_audit = self.approval_store.audit_startup(
            ledger_sessions=durable,
            ledger_claims=durable_claims,
            journal_sessions=active_journals,
            owner_session_ids=(),
        )
        if approval_audit.get("complete") is not True:
            self.authority.clear()
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-approval-startup-audit-incomplete"
            )
        active = self.approval_store.active_pointer()
        if active is not None:
            self._approval_id = str(active["approval_id"])
            self._session_id = str(active["claimed_session_id"])
            try:
                startup_session = self.graph.ledger.session(self._session_id)
            except Exception:
                startup_session = {}
            if str(startup_session.get("state") or "") in {
                "CLEANUP",
                "FINAL_RESET_PENDING",
            }:
                # Startup never invents a background cleanup owner.  A crash-
                # left session remains an explicit sticky recovery pointer
                # until a new authenticated writer/REST attestation attaches.
                self._terminal_state = "RECONCILIATION_REQUIRED"
                self._terminal_detail = (
                    "startup-cleanup-recovery-owner-required"
                )
        self._startup_audit = {
            "graph": self.graph.status()["startupAudit"],
            "approvals": approval_audit,
        }

    @staticmethod
    def _assert_command_fields(
        command: Mapping[str, Any], exact: set[str]
    ) -> None:
        if set(command) != exact:
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-command-fields-not-exact"
            )
        forbidden = {
            "permit",
            "bar",
            "signal",
            "capability",
            "runtime",
            "payload",
            "accountFingerprint",
        }
        if forbidden & set(command):
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-client-authority-forbidden"
            )

    def _verify_operator(self, command: Mapping[str, Any]) -> None:
        confirmation = command.get("operatorConfirmation")
        if (
            not isinstance(confirmation, Mapping)
            or confirmation.get("authenticated") is not True
            or confirmation.get("confirmed") is not True
            or not self.operator_confirmation_verifier(dict(confirmation))
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-operator-confirmation-invalid"
            )

    @contextmanager
    def _owner(self):
        with self._lock:
            self._generation += 1
            generation = self._generation
            yield generation

    def start(self, command: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_command_fields(
            command, {"approvalId", "operatorConfirmation"}
        )
        self._verify_operator(command)
        if (
            not self._allow_mock_graph
            and not upbit_functional_composite_available()
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-composite-unavailable"
            )
        approval_id = str(command["approvalId"])
        claim_crossed = False
        try:
            with self._owner() as generation:
                if self._scheduler is not None and self._scheduler.is_alive():
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-backend-already-running"
                    )
                session_id = "upbit-functional-" + secrets.token_hex(16)
                record = self.approval_store.claim_permit(
                    approval_id=approval_id, session_id=session_id
                )
                claim_crossed = True
                return self._start_claimed_locked(
                    generation=generation,
                    approval_id=approval_id,
                    session_id=session_id,
                    record=record,
                )
        except Exception as exc:
            if not claim_crossed:
                try:
                    retired = self.approval_store.retire_unclaimed_permit(
                        approval_id=approval_id,
                        detail=(
                            "start owner/preclaim failed:"
                            f"{type(exc).__name__}"
                        ),
                    )
                    if retired:
                        self._terminal_state = "START_FAILED_PRECLAIM"
                        self._terminal_detail = (
                            "unclaimed approval retired:"
                            f"{type(exc).__name__}"
                        )
                except Exception:
                    self._terminal_state = "RECONCILIATION_REQUIRED"
                    self._terminal_detail = (
                        "preclaim-retirement-failed:"
                        f"{type(exc).__name__}"
                    )
            raise

    def _start_claimed_locked(
        self,
        *,
        generation: int,
        approval_id: str,
        session_id: str,
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
            permit = record["permit"]
            binding = permit["binding"]
            self._approval_id = approval_id
            self._session_id = session_id
            self.authority.bind_scope(
                {
                    "executionPurpose": "FUNCTIONAL_TEST",
                    "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
                    "functionalTestSessionId": session_id,
                    "functionalTestPermitId": record["permitId"],
                    "functionalTestPermitHash": record["permitHash"],
                    # Exact hashes are filled by graph/core parsing before the
                    # first mutation. The manager does not accept them from a
                    # client command.
                    "functionalTestAccountFingerprint": binding["accountId"],
                }
            )
            try:
                # Scope-dependent route/session hashes are derived only from
                # the sealed publication and permit inside the graph.  Bind
                # them after its exact parser constructs the immutable scope.
                from .upbit_continuous_functional import UpbitPermitScope, _stable_hash

                scope = UpbitPermitScope.parse(
                    permit,
                    immutable_selection=self.graph._selection_reader(),
                )
                self.authority.bind_scope(
                    {
                        "executionPurpose": "FUNCTIONAL_TEST",
                        "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
                        "functionalTestSessionId": session_id,
                        "functionalTestPermitId": scope.permit_id,
                        "functionalTestPermitHash": scope.permit_hash,
                        "functionalTestRouteScopeHash": scope.route_scope_hash,
                        "functionalTestAccountFingerprint": scope.account_fingerprint,
                        "functionalTestSessionScopeHash": _stable_hash(
                            scope.snapshot()
                        ),
                    }
                )
                result = self.graph.start(
                    permit_id=scope.permit_id,
                    permit_hash=scope.permit_hash,
                    session_id=session_id,
                )
                self.approval_store.bind_permit(
                    approval_id=approval_id, session_id=session_id
                )
            except Exception as exc:
                durable_nonterminal = False
                try:
                    durable = self.graph.ledger.session(session_id)
                    durable_nonterminal = str(durable.get("state") or "") in {
                        "ACTIVE",
                        "CLEANUP",
                        "FINAL_RESET_PENDING",
                    }
                except Exception:
                    durable_nonterminal = False
                try:
                    if durable_nonterminal:
                        # Graph activation crossed the durable boundary.  Keep
                        # the main approval bound cleanup-only; marking it
                        # FAILED would make the sticky-gap session impossible
                        # to recover and permanently block future starts.
                        self.approval_store.bind_permit(
                            approval_id=approval_id,
                            session_id=session_id,
                        )
                        self._terminal_state = "RECONCILIATION_REQUIRED"
                        self._terminal_detail = (
                            "start-crossed-durable-boundary:"
                            f"{type(exc).__name__}"
                        )
                    else:
                        self.approval_store.fail_permit(
                            approval_id=approval_id,
                            session_id=session_id,
                            detail=f"start-failed:{type(exc).__name__}",
                        )
                except Exception:
                    self._terminal_state = "RECONCILIATION_REQUIRED"
                    self._terminal_detail = (
                        "start-failure-compensation-failed:"
                        f"{type(exc).__name__}"
                    )
                self.authority.clear()
                raise
            self._start_scheduler_or_fail_closed_locked(
                generation,
                reason="activation-scheduler-start-failed",
            )
            return {"ok": True, "status": self.status(), "result": result}

    def _start_scheduler_or_fail_closed_locked(
        self,
        generation: int,
        *,
        reason: str,
    ) -> None:
        """Retry scheduler creation, then durably hand off to recovery.

        ``Thread.start`` can fail before any worker owns the cleanup loop.
        Three fresh Thread objects are attempted in-process.  If all fail, the
        graph revokes the functional capability, closes/sticky-gaps the WS
        writer, and leaves an exact cleanup-only durable pointer that startup
        audit plus a newly approved recovery can resume.
        """

        scheduler_exc: Exception | None = None
        for _attempt in range(_SCHEDULER_START_ATTEMPTS):
            try:
                self._start_scheduler_locked(generation)
                return
            except Exception as exc:
                scheduler_exc = exc
                self._scheduler = None
        self.authority.cleanup()
        try:
            self.graph.fail_closed_scheduler_start(reason=reason)
            self._mark_reconciliation_required_locked(
                reason + ":cleanup-recovery-required"
            )
        except Exception as cleanup_exc:
            self.authority.clear()
            self._scheduler_stop.set()
            self._terminal_state = "RECONCILIATION_REQUIRED"
            self._terminal_detail = (
                reason
                + ":compensation-failed:"
                + type(cleanup_exc).__name__
            )
        raise UpbitFunctionalBlocked(
            "upbit-functional-scheduler-start-failed"
        ) from scheduler_exc

    def _start_scheduler_locked(self, generation: int) -> None:
        self._scheduler_stop.clear()
        thread = threading.Thread(
            target=self._scheduler_loop,
            args=(generation,),
            name=f"upbit-functional-scheduler-{generation}",
            daemon=True,
        )
        self._scheduler = thread
        thread.start()

    def _scheduler_loop(self, generation: int) -> None:
        while not self._scheduler_stop.wait(_POLL_SECONDS):
            with self._lock:
                if generation != self._generation:
                    return
                try:
                    result = self.graph.pump()
                    snapshot = result.get("snapshot")
                    if (
                        isinstance(snapshot, Mapping)
                        and snapshot.get("status") == "FINALIZED"
                    ):
                        self._consume_finalized_locked(result)
                        return
                    if (
                        isinstance(snapshot, Mapping)
                        and snapshot.get("status")
                        in {"FAILED_CLOSED", "RECONCILIATION_REQUIRED"}
                    ):
                        self._mark_reconciliation_required_locked(
                            "scheduler-observed-failed-closed"
                        )
                        return
                except Exception:
                    if self._terminal_state == "RECONCILIATION_REQUIRED":
                        return
                    try:
                        cleanup = self.graph.stop(reason="scheduler-failure")
                    except Exception as cleanup_exc:
                        self.authority.clear()
                        self._terminal_state = "RECONCILIATION_REQUIRED"
                        self._terminal_detail = (
                            f"scheduler-cleanup-failed:{type(cleanup_exc).__name__}"
                        )
                        return
                    if cleanup.get("pending") is True:
                        # The same fenced owner remains responsible for every
                        # fresh-truth cancel/flatten generation.  Never strand
                        # a working cleanup order merely because the bar or
                        # truth path failed once.
                        self.authority.cleanup()
                        self._terminal_state = "CLEANUP_PENDING"
                        self._terminal_detail = "scheduler-failure-cleanup-owned"
                        continue
                    cleanup_snapshot = cleanup.get("snapshot")
                    if (
                        isinstance(cleanup_snapshot, Mapping)
                        and cleanup_snapshot.get("status") == "FINALIZED"
                    ):
                        self._consume_finalized_locked(cleanup)
                    else:
                        self._mark_reconciliation_required_locked(
                            "scheduler-cleanup-not-finalized"
                        )
                    return

    def _mark_reconciliation_required_locked(self, detail: str) -> None:
        self.authority.clear()
        self._scheduler_stop.set()
        self._terminal_state = "RECONCILIATION_REQUIRED"
        self._terminal_detail = str(detail or "manual-reconciliation-required")

    def _consume_finalized_locked(
        self, result: Mapping[str, Any] | None = None
    ) -> None:
        payload = dict(result or {})
        snapshot = payload.get("snapshot")
        final = payload.get("final")
        if not isinstance(final, Mapping):
            nested = payload.get("result")
            final = nested.get("final") if isinstance(nested, Mapping) else None
        try:
            if (
                payload.get("ok") is not True
                or not isinstance(snapshot, Mapping)
                or snapshot.get("status") != "FINALIZED"
                or str(snapshot.get("sessionId") or "") != self._session_id
                or not isinstance(final, Mapping)
                or final.get("ok") is not True
                or final.get("state") != "FINALIZED"
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-final-result-not-sealed"
                )
            evidence = final.get("evidence")
            evidence_hash = str(final.get("evidenceHash") or "").lower()
            outcome = str(final.get("testOutcome") or "").strip()
            if (
                not isinstance(evidence, Mapping)
                or len(evidence_hash) != 64
                or any(character not in "0123456789abcdef" for character in evidence_hash)
                or outcome
                not in {
                    "PASS",
                    "SAFE_INCOMPLETE",
                    "SAFE_INCOMPLETE_CAUSAL_UNPROVEN",
                    "INCONCLUSIVE_NO_SIGNAL",
                }
                or (outcome == "PASS")
                != (evidence.get("functionalTestPassed") is True)
                or (
                    outcome == "SAFE_INCOMPLETE_CAUSAL_UNPROVEN"
                    and (
                        evidence.get("functionalWiringPassed") is not True
                        or evidence.get("functionalTestPassed") is True
                        or evidence.get(
                            "exclusiveAccountCausalProofComplete"
                        )
                        is True
                    )
                )
                or evidence.get("functionalCapabilityCleared") is not True
                or evidence.get("newEntriesBlocked") is not True
                or evidence.get("realOrdersEnabled") is not False
                or (
                    evidence.get("functionalWiringPassed") is True
                    and not _functional_wiring_evidence_complete(evidence)
                )
                or (
                    outcome == "PASS"
                    and (
                        evidence.get("exactTwoHourRuntimeComplete") is not True
                        or evidence.get("activationRelativePermitExact") is not True
                        or evidence.get("processMonotonicContinuity") is not True
                        or evidence.get("clockDiscontinuityAbsent") is not True
                        or float(evidence.get("actualDurationSeconds") or 0)
                        < 7200.0
                        or float(
                            evidence.get("processMonotonicElapsedSeconds")
                            or 0
                        )
                        < 7200.0
                    )
                )
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-final-evidence-invalid"
                )
            durable = self.graph.ledger.session(self._session_id)
            durable_evidence = self.graph.ledger.final_evidence(
                self._session_id
            )
            terminal_stream_seal = evidence.get("terminalPrivateStreamSeal")
            if not isinstance(terminal_stream_seal, Mapping):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-terminal-stream-seal-missing"
                )
            terminal_stream_hash = str(
                evidence.get("terminalPrivateStreamSealHash") or ""
            ).lower()
            if (
                terminal_stream_hash
                != str(terminal_stream_seal.get("sealHash") or "").lower()
                or self.graph.journal.terminal_seal(
                    session_id=self._session_id
                )
                != dict(terminal_stream_seal)
                or terminal_stream_seal.get("externalActivityAbsent") is not True
                or (
                    outcome == "PASS"
                    and terminal_stream_seal.get("streamContinuous") is not True
                )
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-terminal-stream-seal-mismatch"
                )
            authority = self.authority.snapshot()
            if (
                durable.get("state") != "FINALIZED"
                or str(durable.get("final_evidence_hash") or "").lower()
                != evidence_hash
                or durable_evidence != dict(evidence)
                or str(durable.get("capability_hash") or "")
                or int(durable.get("new_entries_blocked") or 0) != 1
                or int(durable.get("real_orders_enabled") or 0) != 0
                or str(authority.get("functionalCapabilityHash") or "")
                or authority.get("realOrdersEnabled") is not False
                or authority.get("functionalMutationEnabled") is not False
                or authority.get("newEntriesBlocked") is not True
                or self.authority.orders_enabled() is not False
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-final-authority-not-cleared"
                )
        except Exception as exc:
            self._mark_reconciliation_required_locked(
                f"final-seal-verification-failed:{type(exc).__name__}"
            )
            raise
        if self._approval_id and self._session_id:
            try:
                self.approval_store.finish_first_live_bootstrap(
                    approval_id=self._approval_id,
                    session_id=self._session_id,
                    passed=(outcome == "PASS"),
                    evidence_hash=evidence_hash,
                    detail=f"terminal-evidence:{outcome}",
                )
            except Exception as exc:
                self.authority.clear()
                self._scheduler_stop.set()
                self._terminal_state = "RECONCILIATION_REQUIRED"
                self._terminal_detail = (
                    "approval-consume-failed:"
                    f"{type(exc).__name__}"
                )
                raise
        self.authority.clear()
        self._scheduler_stop.set()
        self._terminal_state = (
            "FINALIZED" if outcome == "PASS" else "SAFE_INCOMPLETE"
        )
        self._terminal_detail = (
            f"durable-final-seal-complete:{outcome}"
            if outcome
            else "durable-final-seal-complete:non-pass-or-unknown"
        )

    def stop(self, command: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_command_fields(command, {"operatorConfirmation"})
        self._verify_operator(command)
        with self._owner() as generation:
            self._scheduler_stop.set()
            result = self.graph.stop(reason="operator-stop")
            if result.get("pending") is True:
                self._start_scheduler_or_fail_closed_locked(
                    generation,
                    reason="operator-stop-scheduler-start-failed",
                )
            elif (
                isinstance(result.get("snapshot"), Mapping)
                and result["snapshot"].get("status") == "FINALIZED"
            ):
                self._consume_finalized_locked(result)
            else:
                self._mark_reconciliation_required_locked(
                    "operator-stop-cleanup-not-finalized"
                )
            return {"ok": True, "result": result, "status": self.status()}

    def recover(self, command: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_command_fields(
            command, {"recoveryId", "operatorConfirmation"}
        )
        self._verify_operator(command)
        recovery_id = str(command["recoveryId"])
        with self._owner() as generation:
            identity = self._recovery_identity_locked()
            pointer = self.approval_store.recovery_authority_pointer()
            if (
                not isinstance(pointer, Mapping)
                or str(pointer.get("state") or "") != "APPROVED"
                or not secrets.compare_digest(
                    str(pointer.get("recovery_id") or ""), recovery_id
                )
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-recovery-approved-pointer-missing"
                )
            approved = json.loads(str(pointer["recovery_json"]))
            exact_recovery = {
                "sessionId": self._session_id,
                "previousWriterGeneration": int(
                    identity["journal"]["writer_generation"]
                ),
                "nextWriterGeneration": int(
                    identity["journal"]["writer_generation"]
                )
                + 1,
                "previousOwnerLeaseEvidenceHash": identity[
                    "leaseEvidenceHash"
                ],
            }
            if any(
                str(approved.get(field)) != str(expected)
                for field, expected in exact_recovery.items()
            ):
                self.approval_store.retire_recovery_authority(
                    recovery_id=recovery_id,
                    expected_states=("APPROVED",),
                    detail="recovery identity changed before claim",
                )
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-recovery-identity-changed"
                )
            recovery = self.approval_store.claim_recovery(
                recovery_id=recovery_id,
                session_id=self._session_id,
            )
            try:
                result = self.graph.recover_cleanup(
                    permit_id=str(recovery["permitId"]),
                    permit_hash=str(recovery["permitHash"]),
                    session_id=str(recovery["sessionId"]),
                    recovery_id=recovery_id,
                    recovery_hash=str(recovery["contentHash"]),
                )
                self.approval_store.finish_recovery(
                    recovery_id=recovery_id,
                    state="CONSUMED",
                    detail="cleanup recovery attached",
                )
            except Exception as exc:
                self.approval_store.finish_recovery(
                    recovery_id=recovery_id,
                    state="FAILED",
                    detail=f"recovery-failed:{type(exc).__name__}",
                )
                raise
            if result.get("pending") is True:
                self._start_scheduler_or_fail_closed_locked(
                    generation,
                    reason="recovery-scheduler-start-failed",
                )
            elif (
                isinstance(result.get("snapshot"), Mapping)
                and result["snapshot"].get("status") == "FINALIZED"
            ):
                self._consume_finalized_locked(result)
            else:
                self._mark_reconciliation_required_locked(
                    "recovery-cleanup-not-finalized"
                )
            return {"ok": True, "result": result, "status": self.status()}

    @staticmethod
    def _hash_payload(value: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(
                dict(value),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
                default=str,
            ).encode("utf-8")
        ).hexdigest()

    def _recovery_identity_locked(self) -> dict[str, Any]:
        if not self._session_id or not self._approval_id:
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-recovery-session-missing"
            )
        if self._scheduler is not None and self._scheduler.is_alive():
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-recovery-owner-still-running"
            )
        durable = self.graph.ledger.session(self._session_id)
        journal_rows = [
            row
            for row in self.graph.journal.active_sessions()
            if str(row.get("session_id") or "") == self._session_id
        ]
        pointer = self.approval_store.active_pointer()
        authority = self.authority.snapshot()
        if (
            len(journal_rows) != 1
            or durable.get("state") != "CLEANUP"
            or str(durable.get("capability_hash") or "")
            or not isinstance(pointer, Mapping)
            or pointer.get("state") != "ACTIVE"
            or str(pointer.get("approval_id") or "") != self._approval_id
            or str(pointer.get("claimed_session_id") or "") != self._session_id
            or str(pointer.get("permit_id") or "")
            != str(durable.get("permit_id") or "")
            or str(pointer.get("permit_hash") or "")
            != str(durable.get("permit_hash") or "")
            or authority.get("newEntriesBlocked") is not True
            or authority.get("ordinaryRoutesClosed") is not True
            or authority.get("upbitSmokeRouteClosed") is not True
            or self.authority.orders_enabled() is not False
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-recovery-state-not-exact"
            )
        journal = journal_rows[0]
        if (
            int(journal.get("gap_detected") or 0) != 1
            or int(journal.get("completed") or 0) != 0
            or int(journal.get("connected") or 0) != 0
            or int(journal.get("authenticated") or 0) != 0
            or str(journal.get("writer_token_hash") or "")
            or str(journal.get("writer_lease_expires_at") or "")
            or int(journal.get("writer_generation") or 0) <= 0
            or str(journal.get("account_fingerprint") or "").lower()
            != str(durable.get("account_fingerprint") or "").lower()
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-backend-recovery-owner-loss-unproven"
            )
        lease_evidence = {
            "sessionId": self._session_id,
            "writerGeneration": int(journal["writer_generation"]),
            "writerTokenRevoked": True,
            "writerLeaseRevoked": True,
            "connected": False,
            "authenticated": False,
            "gapDetected": True,
            "journalDetail": str(journal.get("detail") or ""),
            "startupRecoveryRequired": bool(
                (self._startup_audit.get("graph") or {}).get(
                    "recoveryRequired"
                )
            ),
        }
        return {
            "durable": durable,
            "journal": journal,
            "leaseEvidence": lease_evidence,
            "leaseEvidenceHash": self._hash_payload(lease_evidence),
        }

    def preissue_recovery_candidate(
        self, requested_recovery_id: str = ""
    ) -> dict[str, Any]:
        """Issue an inert cleanup-recovery id; no REST or mutation occurs."""

        with self._owner():
            identity = self._recovery_identity_locked()
            existing = self.approval_store.recovery_authority_pointer()
            requested = str(requested_recovery_id or "").strip()
            now = self.clock().astimezone(timezone.utc)
            if existing is not None:
                raw = json.loads(str(existing["recovery_json"]))
                state = str(existing.get("state") or "").upper()
                timestamp_name = "expiresAt" if state == "ISSUED" else "observedAt"
                expires = datetime.fromisoformat(
                    str(raw.get(timestamp_name) or "").replace("Z", "+00:00")
                )
                fresh = (
                    now < expires
                    if state == "ISSUED"
                    else 0 <= (now - expires).total_seconds() <= 15
                )
                if state == "ISSUED" and fresh:
                    if requested and not secrets.compare_digest(
                        requested, str(existing["recovery_id"])
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-functional-backend-recovery-candidate-id-mismatch"
                        )
                    return {
                        "recoveryId": str(existing["recovery_id"]),
                        "sessionId": self._session_id,
                        "expiresAt": str(raw["expiresAt"]),
                    }
                if state in {"ISSUED", "APPROVED"} and not fresh:
                    self.approval_store.retire_recovery_authority(
                        recovery_id=str(existing["recovery_id"]),
                        expected_states=(state,),
                        detail="server retired stale recovery authority",
                    )
                    existing = None
                elif state == "APPROVED":
                    # A fresh approved record belongs to the already-confirmed
                    # request and cannot be rebound to a new challenge.
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-backend-recovery-already-approved"
                    )
                else:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-backend-recovery-owner-claim-active"
                    )
            if requested:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-recovery-candidate-missing"
                )
            durable = identity["durable"]
            recovery_id = "upbit-recovery-" + secrets.token_hex(16)
            body = {
                "schemaVersion": "upbit-functional-recovery-candidate/v1",
                "recoveryId": recovery_id,
                "mode": "CLEANUP_ONLY",
                "sessionId": self._session_id,
                "permitId": str(durable["permit_id"]),
                "permitHash": str(durable["permit_hash"]),
                "accountFingerprint": str(durable["account_fingerprint"]),
                "candidateState": "ISSUED",
                "serverManaged": True,
                "operatorAuthenticated": False,
                "operatorApproved": False,
                "singleUse": True,
                "previousOwnerLost": True,
                "previousOwnerLeaseExpired": True,
                "previousOwnerLeaseEvidenceHash": identity[
                    "leaseEvidenceHash"
                ],
                "previousWriterGeneration": int(
                    identity["journal"]["writer_generation"]
                ),
                "nextWriterGeneration": int(
                    identity["journal"]["writer_generation"]
                )
                + 1,
                "issuedAt": now.isoformat().replace("+00:00", "Z"),
                "expiresAt": (now + timedelta(minutes=5)).isoformat().replace(
                    "+00:00", "Z"
                ),
            }
            signed = dict(self._approval_record_signer(body))
            signed["candidateHash"] = self._hash_payload(signed)
            self.approval_store.issue_recovery_candidate(signed)
            return {
                "recoveryId": recovery_id,
                "sessionId": self._session_id,
                "expiresAt": body["expiresAt"],
            }

    def approve_recovery_candidate(self, recovery_id: str) -> dict[str, Any]:
        """Freshly reconcile REST truth and approve the exact inert id."""

        with self._owner():
            identity = self._recovery_identity_locked()
            candidate = self.approval_store.issued_recovery_pointer()
            if (
                candidate is None
                or not secrets.compare_digest(
                    str(candidate.get("recovery_id") or ""),
                    str(recovery_id or ""),
                )
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-recovery-candidate-missing"
                )
            candidate_record = json.loads(str(candidate["recovery_json"]))
            now = self.clock().astimezone(timezone.utc)
            expires = datetime.fromisoformat(
                str(candidate_record["expiresAt"]).replace("Z", "+00:00")
            )
            if now >= expires:
                self.approval_store.retire_recovery_authority(
                    recovery_id=str(recovery_id),
                    expected_states=("ISSUED",),
                    detail="recovery candidate expired before approval",
                )
                raise UpbitFunctionalBlocked(
                    "upbit-functional-backend-recovery-candidate-expired"
                )
            durable = identity["durable"]
            claims = self.graph.ledger.claims(self._session_id)
            identifiers = tuple(
                dict.fromkeys(
                    str(value).strip()
                    for claim in claims
                    for value in (
                        claim.get("identifier"),
                        claim.get("target_identifier"),
                    )
                    if str(value or "").strip()
                )
            )
            client = OfficialUpbitFunctionalGetClient(
                expected_account_fingerprint=str(
                    durable["account_fingerprint"]
                ),
                sender=self.graph.sender,
            )
            truth_reader = OfficialUpbitFunctionalTruthReader(
                client=client,
                account_fingerprint=str(durable["account_fingerprint"]),
                session_started_at=datetime.fromisoformat(
                    str(durable["starts_at"]).replace("Z", "+00:00")
                ),
                cleanup_deadline=datetime.fromisoformat(
                    str(durable["cleanup_deadline"]).replace("Z", "+00:00")
                ),
                clock=self.clock,
                private_stream_reader=self.graph.journal.snapshot,
                cleanup_recovery=True,
            )
            truth = truth_reader.read_recovery_approval_attestation(
                session_id=self._session_id,
                identifiers=identifiers,
            )
            observed_at = str(truth["observedAt"])
            record = {
                "schemaVersion": "upbit-functional-recovery-approval/v1",
                "recoveryId": str(recovery_id),
                "mode": "CLEANUP_ONLY",
                "sessionId": self._session_id,
                "permitId": str(durable["permit_id"]),
                "permitHash": str(durable["permit_hash"]),
                "accountFingerprint": str(durable["account_fingerprint"]),
                "approvalState": "ACTIVE",
                "serverManaged": True,
                "operatorAuthenticated": True,
                "operatorApproved": True,
                "singleUse": True,
                "previousOwnerLost": True,
                "previousOwnerLeaseExpired": True,
                "officialRestReconciled": True,
                "officialRestTruthHash": self._hash_payload(truth),
                "previousOwnerLeaseEvidenceHash": identity[
                    "leaseEvidenceHash"
                ],
                "previousWriterGeneration": int(
                    identity["journal"]["writer_generation"]
                ),
                "nextWriterGeneration": int(
                    identity["journal"]["writer_generation"]
                )
                + 1,
                "observedAt": observed_at,
                "externalActivityScope": "UPBIT_ACCOUNT_ALL_MARKETS",
                "officialRestRecoveryOnly": True,
            }
            signed = dict(self._approval_record_signer(record))
            signed["contentHash"] = self._hash_payload(signed)
            pointer = self.approval_store.approve_issued_recovery(
                recovery_id=str(recovery_id), record=signed
            )
            return {
                **pointer,
                "sessionId": self._session_id,
                "observedAt": observed_at,
            }

    def status(self) -> dict[str, Any]:
        with self._lock:
            composite_available = upbit_functional_composite_available()
            return {
                "available": composite_available,
                "networkOrderPostAllowed": False,
                "verifierAuthorityPinned": (
                    UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED
                ),
                "productionExclusivityVerifierWired": (
                    UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED
                ),
                "accountExclusivityPreSendReady": bool(
                    UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED
                    and UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED
                ),
                # The first credentialed canary bypass is intentionally kept
                # unreachable until its route-global burn contract is approved
                # and implemented.  Absence of a field must never be mistaken
                # for authorization by state/server callers.
                "firstLiveBootstrapEligible": False,
                "firstLiveBootstrapBlockedReason": (
                    "route-global-one-shot-bypass-not-authorized"
                ),
                "liveEnableGate": (
                    os.environ.get("UPBIT_FUNCTIONAL_LIVE_ENABLED", "")
                    .strip()
                    .lower()
                    == "true"
                ),
                "generation": self._generation,
                "sessionId": self._session_id,
                "approvalId": self._approval_id,
                "schedulerRunning": bool(
                    self._scheduler is not None and self._scheduler.is_alive()
                ),
                "terminalState": self._terminal_state,
                "terminalDetail": self._terminal_detail,
                "authority": self.authority.snapshot(),
                "graph": self.graph.status(),
                "startupAudit": dict(self._startup_audit),
            }


def prepare_upbit_functional_backend(
    *,
    database_path: str | Path,
    publication_proof_path: str | Path,
    clock: Callable[[], datetime],
    approval_store: DurableUpbitFunctionalApprovalStore,
    sender: Callable[..., Mapping[str, Any]],
    lease_reader_factory: Callable[..., Any],
    operator_confirmation_verifier: Callable[[Mapping[str, Any]], bool],
    ordinary_routes_closed_reader: Callable[[], bool] | None = None,
    emergency_stop_reader: Callable[[], Mapping[str, Any]] | None = None,
    approval_record_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    websocket_source: OfficialUpbitFunctionalMyOrderPump | None = None,
    candle_source: OfficialUpbitFinalizedFiveMinuteWindowReader | None = None,
    allow_mock_graph: bool = False,
) -> dict[str, Any]:
    """Construct the process singleton and run startup audit exactly once."""

    global _BACKEND_SINGLETON
    with _BACKEND_SINGLETON_LOCK:
        if _BACKEND_SINGLETON is None:
            if not allow_mock_graph:
                from .process_safety import live_trader_instance_lease_status

                if live_trader_instance_lease_status().get("acquired") is not True:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-backend-application-lease-required"
                    )
            _BACKEND_SINGLETON = UpbitFunctionalBackendManager(
                database_path=database_path,
                publication_proof_path=publication_proof_path,
                clock=clock,
                approval_store=approval_store,
                sender=sender,
                lease_reader_factory=lease_reader_factory,
                operator_confirmation_verifier=operator_confirmation_verifier,
                ordinary_routes_closed_reader=ordinary_routes_closed_reader,
                emergency_stop_reader=emergency_stop_reader,
                approval_record_signer=approval_record_signer,
                websocket_source=websocket_source,
                candle_source=candle_source,
                allow_mock_graph=allow_mock_graph,
                _capability=(
                    None
                    if allow_mock_graph
                    else _BACKEND_CONSTRUCTION_CAPABILITY
                ),
            )
        return {
            "ok": True,
            "prepared": True,
            "status": _BACKEND_SINGLETON.status(),
        }


def _require_upbit_functional_backend() -> UpbitFunctionalBackendManager:
    with _BACKEND_SINGLETON_LOCK:
        manager = _BACKEND_SINGLETON
    if manager is None:
        raise UpbitFunctionalBlocked(
            "upbit-functional-backend-not-prepared"
        )
    return manager


def upbit_functional_backend_status() -> dict[str, Any]:
    with _BACKEND_SINGLETON_LOCK:
        manager = _BACKEND_SINGLETON
    if manager is None:
        return {
            "available": upbit_functional_composite_available(),
            "prepared": False,
            "networkOrderPostAllowed": False,
            "verifierAuthorityPinned": (
                UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED
            ),
            "productionExclusivityVerifierWired": (
                UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED
            ),
            "accountExclusivityPreSendReady": bool(
                UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED
                and UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED
            ),
            "firstLiveBootstrapEligible": False,
            "firstLiveBootstrapBlockedReason": (
                "route-global-one-shot-bypass-not-authorized"
            ),
            "liveEnableGate": (
                os.environ.get("UPBIT_FUNCTIONAL_LIVE_ENABLED", "")
                .strip()
                .lower()
                == "true"
            ),
            "terminalState": "UNAVAILABLE",
            "terminalDetail": production_entrypoint_status()["reason"],
        }
    return {"prepared": True, **manager.status()}


def start_upbit_functional_backend(
    command: Mapping[str, Any],
) -> dict[str, Any]:
    return _require_upbit_functional_backend().start(command)


def stop_upbit_functional_backend(
    command: Mapping[str, Any],
) -> dict[str, Any]:
    return _require_upbit_functional_backend().stop(command)


def recover_upbit_functional_backend(
    command: Mapping[str, Any],
) -> dict[str, Any]:
    return _require_upbit_functional_backend().recover(command)


def preissue_upbit_functional_recovery_candidate(
    requested_recovery_id: str = "",
) -> dict[str, Any]:
    return _require_upbit_functional_backend().preissue_recovery_candidate(
        requested_recovery_id
    )


def approve_upbit_functional_recovery_candidate(
    recovery_id: str,
) -> dict[str, Any]:
    return _require_upbit_functional_backend().approve_recovery_candidate(
        recovery_id
    )


__all__ = [
    "UPBIT_FUNCTIONAL_BACKEND_AVAILABLE",
    "UPBIT_FUNCTIONAL_REAL_E2E_AVAILABLE",
    "UPBIT_FUNCTIONAL_STATE_SERVER_WIRING_AVAILABLE",
    "UpbitFunctionalBackendManager",
    "approve_upbit_functional_recovery_candidate",
    "prepare_upbit_functional_backend",
    "preissue_upbit_functional_recovery_candidate",
    "recover_upbit_functional_backend",
    "start_upbit_functional_backend",
    "stop_upbit_functional_backend",
    "upbit_functional_composite_available",
    "upbit_functional_backend_status",
]
