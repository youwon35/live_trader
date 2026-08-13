from __future__ import annotations

"""One isolated production composition root for the Upbit functional lane.

This graph is deliberately not imported by ``state.py`` or ``server.py`` yet.
Its status function is safe to expose, but ``start`` remains impossible while
any production E2E requirement is incomplete.  No legacy one-shot or smoke
module is referenced here.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import threading
from pathlib import Path
from typing import Any, Callable, Mapping

from .upbit_continuous_functional import (
    UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED,
    UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE,
    UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED,
    UpbitContinuousFunctionalService,
    UpbitFunctionalBlocked,
    UpbitFunctionalLedger,
    UpbitPermitScope,
    _stable_hash,
)
from .upbit_functional_managed import ManagedUpbitFunctionalController
from .upbit_functional_mutation import (
    UPBIT_FUNCTIONAL_MUTATION_AVAILABLE,
    UpbitFunctionalMutationEdge,
)
from .upbit_functional_publication import load_upbit_functional_selection
from .upbit_functional_strategy import SealedUpbitMovingAverageEvaluator
from .upbit_functional_transport import (
    DurableUpbitMyOrderJournal,
    OfficialUpbitFunctionalGetClient,
    upbit_credential_fingerprint,
)
from .upbit_functional_truth import OfficialUpbitFunctionalTruthReader
from trading_runtime.functional_test import parse_functional_test_permit


UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE = False
UPBIT_FUNCTIONAL_ROUTE_KEY = "UPBIT_KRW_SPOT_CONTINUOUS:KRW-BTC:5m"
_GRAPH_CONSTRUCTION_CAPABILITY = object()
_MOCK_GRAPH_CONSTRUCTION_CAPABILITY = object()


def production_entrypoint_status() -> dict[str, Any]:
    verifier_ready = bool(
        UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED
        and UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED
    )
    return {
        "available": (
            UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE
            and UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE
            and UPBIT_FUNCTIONAL_MUTATION_AVAILABLE
            and verifier_ready
        ),
        "route": UPBIT_FUNCTIONAL_ROUTE_KEY,
        "reason": (
            "state/server ownership wiring and credentialed two-hour real "
            "BUY/SELL/cancel/flatten E2E evidence are incomplete"
            if verifier_ready
            else "PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_NOT_WIRED"
        ),
        "verifierAuthorityPinned": (
            UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED
        ),
        "productionExclusivityVerifierWired": (
            UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED
        ),
        "accountExclusivityPreSendReady": verifier_ready,
        "ordinaryLiveRouteChanged": False,
        "upbitSmokeRouteChanged": False,
        "legacyOneShotImported": False,
        "networkOrderPostAllowed": False,
    }


class UpbitFunctionalProductionGraph:
    def __init__(
        self,
        *,
        database_path: str | Path,
        publication_proof_path: str | Path,
        account_fingerprint: str,
        clock: Callable[[], datetime],
        runtime_reader: Callable[[], Mapping[str, Any]],
        runtime_capability_registrar: Callable[[str], None],
        enter_cleanup_latch: Callable[[], None],
        disarm_functional_orders: Callable[[], None],
        functional_orders_reader: Callable[[], bool],
        lease_reader_factory: Callable[..., Any],
        sender: Callable[..., Mapping[str, Any]],
        approved_permit_reader: Callable[[str, str], Mapping[str, Any]],
        approved_recovery_reader: Callable[[str, str], Mapping[str, Any]],
        websocket_handshake: Callable[..., Mapping[str, Any]],
        terminal_stream_barrier: Callable[..., Mapping[str, Any]] | None = None,
        finalized_bar_window_reader: Callable[[], Mapping[str, Any]],
        arm_functional_orders: Callable[[str], None] | None = None,
        clear_runtime_capability: Callable[[], None] | None = None,
        _capability: object | None = None,
    ) -> None:
        if _capability not in {
            _GRAPH_CONSTRUCTION_CAPABILITY,
            _MOCK_GRAPH_CONSTRUCTION_CAPABILITY,
        }:
            # This check precedes ledger/journal construction so a direct
            # import cannot run startup recovery mutations against a live DB.
            raise UpbitFunctionalBlocked(
                "upbit-functional-production-graph-direct-construction-forbidden"
            )
        self.database_path = Path(database_path)
        self.publication_proof_path = Path(publication_proof_path)
        self.account_fingerprint = str(account_fingerprint).strip().lower()
        self.clock = clock
        self.runtime_reader = runtime_reader
        self.runtime_capability_registrar = runtime_capability_registrar
        self.enter_cleanup_latch = enter_cleanup_latch
        self.disarm_functional_orders = disarm_functional_orders
        self.functional_orders_reader = functional_orders_reader
        self.arm_functional_orders = arm_functional_orders or (
            lambda _capability_hash: None
        )
        self.clear_runtime_capability = clear_runtime_capability or (lambda: None)
        self.lease_reader_factory = lease_reader_factory
        self.sender = sender
        self.approved_permit_reader = approved_permit_reader
        self.approved_recovery_reader = approved_recovery_reader
        self.websocket_handshake = websocket_handshake
        self.terminal_stream_barrier = terminal_stream_barrier
        self.finalized_bar_window_reader = finalized_bar_window_reader
        self.ledger = UpbitFunctionalLedger(
            self.database_path, clock=self.clock
        )
        self.journal = DurableUpbitMyOrderJournal(
            self.database_path, clock=self.clock
        )
        self.controller = ManagedUpbitFunctionalController(
            enter_cleanup_latch=self.enter_cleanup_latch,
            disarm_real_orders=self.disarm_functional_orders,
            clear_runtime_capability=self.clear_runtime_capability,
            clock=self.clock,
        )
        self.__journal_writer: dict[str, Any] | None = None
        self.__websocket_liveness_reader: Callable[[], Mapping[str, Any]] | None = None
        self.__websocket_close: Callable[[], None] | None = None
        self.__strategy_evaluator: SealedUpbitMovingAverageEvaluator | None = None
        self._lock = threading.RLock()
        self._startup_audit = self._run_startup_audit()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                **production_entrypoint_status(),
                "controller": self.controller.snapshot(),
                "startupAudit": dict(self._startup_audit),
            }

    def _clear_all_functional_authority(self) -> None:
        self.disarm_functional_orders()
        self.runtime_capability_registrar("")
        self.clear_runtime_capability()

    def _assert_all_functional_authority_cleared(self) -> None:
        """Re-read the real authority surfaces; callbacks are not evidence."""

        runtime = dict(self.runtime_reader())
        if (
            self.functional_orders_reader() is not False
            or runtime.get("newEntriesBlocked") is not True
            or runtime.get("realOrdersEnabled") is not False
            or runtime.get("functionalMutationEnabled") is not False
            or str(runtime.get("functionalCapabilityHash") or "").strip()
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-startup-authority-reset-not-observed"
            )

    def _run_startup_audit(self) -> dict[str, Any]:
        """Fail closed every durable state left by a prior process death."""

        # A crash can occur after the runtime capability is registered but
        # before the durable session insert.  Therefore the absence of a
        # ledger row is not evidence that runtime authority is absent.
        self._clear_all_functional_authority()
        self._assert_all_functional_authority_cleared()
        durable_rows = {
            str(row["session_id"]): row
            for row in self.ledger.sessions()
        }
        journal_rows = {
            str(row["session_id"]): row
            for row in self.journal.active_sessions()
        }
        actions: list[dict[str, Any]] = []
        for session_id in sorted(set(durable_rows) | set(journal_rows)):
            durable = durable_rows.get(session_id)
            journal = journal_rows.get(session_id)
            if durable is None:
                self.journal.startup_fail_closed(
                    session_id=session_id,
                    account_fingerprint=self.account_fingerprint,
                    detail="startup-orphan-journal-without-trade-session",
                    completed=True,
                )
                actions.append(
                    {"sessionId": session_id, "action": "ORPHAN_JOURNAL_ABORTED"}
                )
                continue
            state = str(durable["state"])
            if state == "FINALIZED":
                evidence = self.ledger.final_evidence(session_id)
                expected = evidence.get("terminalPrivateStreamSeal")
                if not isinstance(expected, Mapping):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-startup-final-journal-seal-missing"
                    )
                identifiers = expected.get("ownedIdentifiers")
                if not isinstance(identifiers, list) or any(
                    not isinstance(identifier, str) or not identifier
                    for identifier in identifiers
                ):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-startup-final-identifiers-invalid"
                    )
                if journal is not None:
                    # New protocol commits the private cursor before ledger
                    # FINALIZED.  An active journal beside FINALIZED is an
                    # impossible/tampered legacy edge and must never preserve
                    # PASS by manufacturing a terminal stream seal at startup.
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-startup-final-journal-incomplete"
                    )
                stored = self.journal.terminal_seal(session_id=session_id)
                if stored != dict(expected):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-startup-final-journal-seal-mismatch"
                    )
                actions.append(
                    {
                        "sessionId": session_id,
                        "action": "FINALIZED_JOURNAL_VERIFIED",
                        "sealHash": str(expected.get("sealHash") or ""),
                    }
                )
                continue
            if state == "FINAL_RESET_PENDING":
                evidence = self.ledger.final_evidence(session_id)
                terminal_seal = evidence.get("terminalPrivateStreamSeal")
                if not isinstance(terminal_seal, Mapping):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-startup-terminal-stream-seal-missing"
                    )
                identifiers = terminal_seal.get("ownedIdentifiers")
                if not isinstance(identifiers, list) or any(
                    not isinstance(identifier, str) or not identifier
                    for identifier in identifiers
                ):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-startup-terminal-identifiers-invalid"
                    )
                self.journal.complete_with_attestation(
                    session_id=session_id,
                    identifiers=tuple(identifiers),
                    expected=terminal_seal,
                    startup_recovery=True,
                )
                self.ledger.complete_final_reset(
                    session_id,
                    evidence_hash=str(durable["final_evidence_hash"]),
                )
                actions.append(
                    {
                        "sessionId": session_id,
                        "action": "FINAL_RESET_COMPLETED",
                        "evidenceHash": str(durable["final_evidence_hash"]),
                        "evidenceVerified": bool(evidence),
                    }
                )
                continue
            if state == "ACTIVE":
                durable = self.ledger.enter_cleanup(
                    session_id,
                    reason="startup-owner-process-lost",
                )
                state = str(durable["state"])
            if state == "CLEANUP":
                self.ledger.revoke_cleanup_capability(
                    session_id,
                    reason="startup-owner-process-lost",
                )
                if journal is not None:
                    self.journal.startup_fail_closed(
                        session_id=session_id,
                        account_fingerprint=self.account_fingerprint,
                        detail="startup-cleanup-recovery-required",
                        completed=False,
                    )
                actions.append(
                    {
                        "sessionId": session_id,
                        "action": "CLEANUP_RECOVERY_REQUIRED",
                        "privateStreamPresent": journal is not None,
                    }
                )
                continue
            raise UpbitFunctionalBlocked(
                "upbit-functional-startup-state-unrecognized"
            )
        return {
            "complete": True,
            "mutationsBlocked": True,
            "actions": actions,
            "recoveryRequired": any(
                row["action"] == "CLEANUP_RECOVERY_REQUIRED"
                for row in actions
            ),
        }

    def start(self, **activation: Any) -> dict[str, Any]:
        with self._lock:
            return self._start_locked(**activation)

    def _start_locked(self, **activation: Any) -> dict[str, Any]:
        if not production_entrypoint_status()["available"]:
            raise UpbitFunctionalBlocked(
                "upbit-functional-production-entrypoint-unavailable"
            )
        if set(activation) != {"permit_id", "permit_hash", "session_id"}:
            raise UpbitFunctionalBlocked(
                "upbit-functional-production-start-input-invalid"
            )
        if self.ledger.nonterminal_sessions():
            raise UpbitFunctionalBlocked(
                "upbit-functional-startup-recovery-required-before-new-session"
            )
        permit_id = str(activation.get("permit_id") or "").strip()
        permit_hash = str(activation.get("permit_hash") or "").strip().lower()
        session_id = str(activation.get("session_id") or "").strip()
        if not permit_id or not permit_hash or not session_id:
            raise UpbitFunctionalBlocked(
                "upbit-functional-production-start-input-invalid"
            )
        permit, parsed_permit = self._load_approved_permit(
            permit_id=permit_id,
            permit_hash=permit_hash,
            session_id=session_id,
        )
        if self.functional_orders_reader():
            raise UpbitFunctionalBlocked(
                "upbit-functional-production-orders-must-start-disarmed"
            )
        if upbit_credential_fingerprint() != self.account_fingerprint:
            raise UpbitFunctionalBlocked(
                "upbit-functional-production-account-fingerprint-mismatch"
            )
        writer = self.journal.begin_authenticated_session(
            session_id=session_id,
            account_fingerprint=self.account_fingerprint,
            started_at=self.clock(),
        )
        self.__journal_writer = dict(writer)
        try:
            if not callable(self.websocket_handshake):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-authenticated-websocket-handshake-required"
                )
            handshake = self.websocket_handshake(
                session_id=session_id,
                journal=self.journal,
                writer_authority=dict(writer),
            )
            self._bind_authenticated_private_stream(
                session_id=session_id,
                writer=writer,
                handshake=handshake,
            )
            client = OfficialUpbitFunctionalGetClient(
                expected_account_fingerprint=self.account_fingerprint,
                sender=self.sender,
            )
            starts_at = parsed_permit.starts_at
            truth_reader = OfficialUpbitFunctionalTruthReader(
                client=client,
                account_fingerprint=self.account_fingerprint,
                session_started_at=starts_at,
                cleanup_deadline=starts_at + timedelta(hours=3),
                clock=self.clock,
                private_stream_reader=self.journal.snapshot,
            )
            scope = UpbitPermitScope.parse(
                permit,
                immutable_selection=self._selection_reader(),
            )
            evaluator = SealedUpbitMovingAverageEvaluator(
                scope=scope,
                immutable_selection_reader=self._selection_reader,
                clock=self.clock,
            )
            edge = UpbitFunctionalMutationEdge(
                session_id=session_id,
                account_fingerprint=self.account_fingerprint,
                permit_id=scope.permit_id,
                permit_hash=scope.permit_hash,
                route_scope_hash=scope.route_scope_hash,
                session_scope_hash=_stable_hash(scope.snapshot()),
                authority_reader=self.runtime_reader,
                claim_reader=lambda claim_id: self.ledger.mutation_authority(
                    session_id, claim_id
                ),
                post_boundary_marker=self.ledger.mark_post_may_have_crossed,
                sender=self.sender,
            )
            result = self.controller.start(
                permit=permit,
                ledger=self.ledger,
                session_id=session_id,
                truth_reader=truth_reader,
                post_order=edge.post,
                cancel_order=edge.cancel,
                lease_factory=self.lease_reader_factory,
                runtime_reader=self.runtime_reader,
                immutable_selection_reader=self._selection_reader,
                runtime_capability_registrar=self.runtime_capability_registrar,
                real_orders_reader=self.functional_orders_reader,
                terminal_stream_prepare=self.journal.prepare_terminal_attestation,
                terminal_stream_commit=lambda **payload: self._commit_terminal_stream(
                    payload
                ),
                terminal_stream_barrier=self._terminal_stream_barrier,
                clock=self.clock,
            )
            durable = self.ledger.session(session_id)
            capability_hash = str(durable["capability_hash"])
            self.arm_functional_orders(capability_hash)
            if not self.functional_orders_reader():
                raise UpbitFunctionalBlocked(
                    "upbit-functional-lane-arm-failed"
                )
            self.__strategy_evaluator = evaluator
        except Exception as exc:
            # Never complete the journal when a durable session may already
            # exist.  A post-activation error is an owner-loss boundary: keep
            # a sticky-gap, token-revoked journal so cleanup recovery remains
            # reachable instead of deadlocking behind a completed stream.
            try:
                self.journal.mark_gap(
                    session_id,
                    detail=f"activation-failed:{type(exc).__name__}",
                    writer_token=str(writer["writerToken"]),
                    writer_generation=int(writer["writerGeneration"]),
                )
            except Exception:
                pass
            try:
                self.disarm_functional_orders()
            except Exception:
                pass
            controller = self.controller.snapshot()
            if (
                controller.get("sessionId") == session_id
                and controller.get("status") in {"ACTIVE", "CLEANUP"}
            ):
                try:
                    self.controller.fail_closed_after_start(
                        reason=f"activation-failed:{type(exc).__name__}"
                    )
                except Exception:
                    # fail_closed_after_start revokes durable authority before
                    # clearing runtime and detaches in a finally block.  Keep
                    # the original activation error as the public failure.
                    pass
            try:
                durable = self.ledger.session(session_id)
            except Exception:
                durable = None
            if isinstance(durable, Mapping) and durable.get("state") in {
                "ACTIVE",
                "CLEANUP",
            }:
                try:
                    self.journal.startup_fail_closed(
                        session_id=session_id,
                        account_fingerprint=self.account_fingerprint,
                        detail="activation-failed-cleanup-recovery-required",
                        completed=False,
                    )
                except Exception:
                    pass
            elif durable is None:
                try:
                    self.journal.startup_fail_closed(
                        session_id=session_id,
                        account_fingerprint=self.account_fingerprint,
                        detail="activation-failed-before-durable-session",
                        completed=True,
                    )
                except Exception:
                    pass
            self.__strategy_evaluator = None
            self.__websocket_liveness_reader = None
            self._close_private_stream()
            raise
        return result

    def _load_approved_permit(
        self,
        *,
        permit_id: str,
        permit_hash: str,
        session_id: str,
    ) -> tuple[Mapping[str, Any], Any]:
        """Load an operator-approved permit from the server-owned store.

        A permit's content hash proves integrity, not authorization.  The
        caller supplies only the id/hash of an already active pointer; the
        actual permit and operator approval record come from this graph's
        backend dependency and are bound to the exact session/account/route.
        """

        record = self.approved_permit_reader(permit_id, permit_hash)
        if not isinstance(record, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-permit-record-missing"
            )
        exact = {
            "schemaVersion": "upbit-functional-approved-permit/v1",
            "permitId": permit_id,
            "permitHash": permit_hash,
            "activeSessionId": session_id,
            "accountFingerprint": self.account_fingerprint,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "approvalState": "ACTIVE",
        }
        for field, expected in exact.items():
            if not hmac.compare_digest(
                str(record.get(field) or "").strip(), expected
            ):
                raise UpbitFunctionalBlocked(
                    f"upbit-functional-approved-permit-{field}-mismatch"
                )
        if any(
            record.get(field) is not True
            for field in (
                "serverManaged",
                "operatorAuthenticated",
                "operatorApproved",
                "singleUse",
            )
        ) or not str(record.get("approvalId") or "").strip():
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-permit-authorization-incomplete"
            )
        permit = record.get("permit")
        if not isinstance(permit, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-permit-payload-missing"
            )
        parsed = parse_functional_test_permit(permit)
        if (
            not hmac.compare_digest(parsed.permit_id, permit_id)
            or not hmac.compare_digest(parsed.content_hash, permit_hash)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-permit-content-mismatch"
            )
        return dict(permit), parsed

    def _selection_reader(self) -> dict[str, Any]:
        return load_upbit_functional_selection(
            self.publication_proof_path,
            account_fingerprint=self.account_fingerprint,
        )

    def ingest_myorder(
        self,
        session_id: str,
        payload: Mapping[str, Any],
        *,
        writer_token: str,
        writer_generation: int,
    ) -> dict[str, Any]:
        with self._lock:
            return self.journal.ingest(
                session_id,
                payload,
                writer_token=writer_token,
                writer_generation=writer_generation,
            )

    @staticmethod
    def _writer_token_hash(writer: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            str(writer.get("writerToken") or "").encode("utf-8")
        ).hexdigest()

    def _bind_authenticated_private_stream(
        self,
        *,
        session_id: str,
        writer: Mapping[str, Any],
        handshake: Mapping[str, Any],
    ) -> None:
        """Bind the ACK and a socket-owned liveness reader to one writer lease."""

        generation = int(writer["writerGeneration"])
        token_hash = self._writer_token_hash(writer)
        liveness_reader = (
            handshake.get("livenessReader")
            if isinstance(handshake, Mapping)
            else None
        )
        close_pump = (
            handshake.get("closePump")
            if isinstance(handshake, Mapping)
            else None
        )
        if (
            not isinstance(handshake, Mapping)
            or handshake.get("connected") is not True
            or handshake.get("authenticated") is not True
            or handshake.get("myOrderSubscribed") is not True
            or str(handshake.get("sessionId") or "") != session_id
            or int(handshake.get("writerGeneration") or -1) != generation
            or not hmac.compare_digest(
                str(handshake.get("writerTokenHash") or ""), token_hash
            )
            or not callable(liveness_reader)
            or not callable(close_pump)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-authenticated-websocket-handshake-failed"
            )
        self.journal.attest_authenticated_connection(
            session_id,
            writer_token=str(writer["writerToken"]),
            writer_generation=generation,
        )
        self.__websocket_liveness_reader = liveness_reader
        self.__websocket_close = close_pump

    def _close_private_stream(self) -> None:
        close = self.__websocket_close
        self.__websocket_close = None
        self.__websocket_liveness_reader = None
        if close is not None:
            try:
                close()
            except Exception:
                pass

    def _fail_private_stream_liveness(self, *, detail: str) -> None:
        writer = self.__journal_writer
        controller = self.controller.snapshot()
        try:
            if writer is not None and controller.get("sessionId"):
                self.journal.mark_gap(
                    str(controller["sessionId"]),
                    detail=detail,
                    writer_token=str(writer["writerToken"]),
                    writer_generation=int(writer["writerGeneration"]),
                )
        finally:
            if controller.get("status") in {"ACTIVE", "CLEANUP"}:
                try:
                    self.controller.fail_closed_after_start(reason=detail)
                except Exception:
                    # Authority reduction must not depend on the in-memory
                    # controller still being attachable after a crash edge.
                    self._clear_all_functional_authority()
                finally:
                    self.__strategy_evaluator = None
                    self._close_private_stream()

    def _refresh_private_stream_liveness(self) -> None:
        """Renew only from a fresh frame/pong owned by the exact WS writer."""

        writer = self.__journal_writer
        reader = self.__websocket_liveness_reader
        controller = self.controller.snapshot()
        session_id = str(controller.get("sessionId") or "")
        if writer is None or reader is None or not session_id:
            self._fail_private_stream_liveness(
                detail="private-stream-liveness-owner-missing"
            )
            raise UpbitFunctionalBlocked(
                "upbit-functional-private-stream-liveness-owner-missing"
            )
        try:
            value = reader()
            generation = int(writer["writerGeneration"])
            token_hash = self._writer_token_hash(writer)
            if not isinstance(value, Mapping):
                raise ValueError("liveness-not-mapping")
            observed = datetime.fromisoformat(
                str(value.get("lastFrameAt") or "").replace("Z", "+00:00")
            )
            now = self.clock()
            if now.tzinfo is None or now.utcoffset() is None:
                raise ValueError("clock-timezone-missing")
            if observed.tzinfo is None or observed.utcoffset() is None:
                raise ValueError("liveness-timezone-missing")
            age = now.astimezone(timezone.utc) - observed.astimezone(timezone.utc)
            if (
                value.get("connected") is not True
                or value.get("authenticated") is not True
                or value.get("myOrderSubscribed") is not True
                or str(value.get("sessionId") or "") != session_id
                or int(value.get("writerGeneration") or -1) != generation
                or not hmac.compare_digest(
                    str(value.get("writerTokenHash") or ""), token_hash
                )
                or age < timedelta(0)
                or age > timedelta(seconds=10)
            ):
                raise ValueError("liveness-attestation-invalid")
            self.journal.observe(
                session_id,
                writer_token=str(writer["writerToken"]),
                writer_generation=generation,
            )
        except Exception as exc:
            detail = f"private-stream-liveness-lost:{type(exc).__name__}"
            self._fail_private_stream_liveness(detail=detail)
            raise UpbitFunctionalBlocked(
                "upbit-functional-private-stream-liveness-lost"
            ) from exc

    def pump(self, bar: Mapping[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            return self._pump_locked(bar=bar)

    def _pump_locked(
        self, bar: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        if bar is not None:
            raise UpbitFunctionalBlocked(
                "upbit-functional-direct-signal-input-forbidden"
            )
        controller = self.controller.snapshot()
        if controller.get("status") in {"ACTIVE", "CLEANUP"}:
            self._refresh_private_stream_liveness()
            monitored = self.controller.monitor_once()
            self._complete_journal_after_finalized(monitored)
            controller = self.controller.snapshot()
            if controller.get("status") != "ACTIVE":
                return monitored
        if controller.get("status") == "ACTIVE":
            evaluator = self.__strategy_evaluator
            if evaluator is None:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-strategy-evaluator-not-owned"
                )
            try:
                raw_window = self.finalized_bar_window_reader()
                if not isinstance(raw_window, Mapping):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-finalized-window-missing"
                    )
                evaluated = evaluator.evaluate(raw_window)
            except Exception as exc:
                result = self.controller.stop_and_cleanup(
                    reason=f"market-data-evaluator-failed:{type(exc).__name__}"
                )
                self._complete_journal_after_finalized(result)
                return result
            durable = self.ledger.session(str(controller["sessionId"]))
            if str(durable.get("last_bar_id") or "") == str(
                evaluated.get("barId") or ""
            ):
                return {
                    "ok": True,
                    "result": {
                        "action": "NO_NEW_BAR",
                        "barId": str(evaluated.get("barId") or ""),
                    },
                    "snapshot": self.controller.snapshot(),
                }
            result = self.controller.on_finalized_bar(evaluated)
            self._complete_journal_after_finalized(result)
            return result
        return self.controller.monitor_once()

    def _complete_journal_after_finalized(
        self, result: Mapping[str, Any]
    ) -> None:
        snapshot = result.get("snapshot")
        if not isinstance(snapshot, Mapping) or snapshot.get("status") != "FINALIZED":
            return
        session_id = str(snapshot.get("sessionId") or "")
        durable = self.ledger.session(session_id)
        if durable["state"] != "FINALIZED":
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-final-state-required"
            )
        evidence = self.ledger.final_evidence(session_id)
        expected = evidence.get("terminalPrivateStreamSeal")
        if not isinstance(expected, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-terminal-evidence-missing"
            )
        stored = self.journal.terminal_seal(session_id=session_id)
        if stored != dict(expected):
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-terminal-evidence-mismatch"
            )
        self.__journal_writer = None
        self._close_private_stream()
        self.__strategy_evaluator = None

    def _commit_terminal_stream(
        self, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        session_id = str(payload.get("session_id") or "")
        identifiers = payload.get("identifiers")
        expected = payload.get("expected")
        if (
            not session_id
            or not isinstance(identifiers, tuple)
            or not isinstance(expected, Mapping)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-terminal-commit-input-invalid"
            )
        writer = self.__journal_writer
        if writer is None:
            # Startup FINAL_RESET_PENDING recovery has no surviving raw writer.
            return self.journal.complete_with_attestation(
                session_id=session_id,
                identifiers=identifiers,
                expected=expected,
                startup_recovery=True,
            )
        return self.journal.complete_with_attestation(
            session_id=session_id,
            identifiers=identifiers,
            expected=expected,
            writer_token=str(writer["writerToken"]),
            writer_generation=int(writer["writerGeneration"]),
        )

    def _terminal_stream_barrier(
        self, *, session_id: str
    ) -> Mapping[str, Any]:
        barrier = self.terminal_stream_barrier
        if not callable(barrier):
            raise UpbitFunctionalBlocked(
                "upbit-functional-terminal-stream-barrier-required"
            )
        result = barrier(session_id=session_id)
        if not isinstance(result, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-functional-terminal-stream-barrier-invalid"
            )
        return dict(result)

    def stop(self, *, reason: str = "operator-stop") -> dict[str, Any]:
        with self._lock:
            result = self.controller.stop_and_cleanup(reason=reason)
            if result.get("pending") is True:
                return result
            self._complete_journal_after_finalized(result)
            return result

    def fail_closed_scheduler_start(self, *, reason: str) -> dict[str, Any]:
        """Revoke a just-activated session when its scheduler cannot start."""

        with self._lock:
            controller = self.controller.snapshot()
            session_id = str(controller.get("sessionId") or "")
            if not session_id or controller.get("status") not in {
                "ACTIVE",
                "CLEANUP",
            }:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-scheduler-failure-owner-missing"
                )
            self._fail_private_stream_liveness(detail=reason)
            # Rotate/revoke the dead writer lease durably.  Recovery must use
            # a new authenticated writer generation and fresh REST proof.
            self.journal.startup_fail_closed(
                session_id=session_id,
                account_fingerprint=self.account_fingerprint,
                detail=reason,
                completed=False,
            )
            self.__journal_writer = None
            self.__strategy_evaluator = None
            durable = self.ledger.session(session_id)
            if (
                durable.get("state") != "CLEANUP"
                or str(durable.get("capability_hash") or "")
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-scheduler-failure-revoke-not-observed"
                )
            return {
                "ok": False,
                "pending": False,
                "manualInterventionRequired": True,
                "snapshot": {
                    "status": "RECONCILIATION_REQUIRED",
                    "sessionId": session_id,
                    "durableState": "CLEANUP",
                },
            }

    def recover_cleanup(
        self,
        *,
        permit_id: str,
        permit_hash: str,
        session_id: str,
        recovery_id: str,
        recovery_hash: str,
    ) -> dict[str, Any]:
        with self._lock:
            return self._recover_cleanup_locked(
                permit_id=permit_id,
                permit_hash=permit_hash,
                session_id=session_id,
                recovery_id=recovery_id,
                recovery_hash=recovery_hash,
            )

    def _recover_cleanup_locked(
        self,
        *,
        permit_id: str,
        permit_hash: str,
        session_id: str,
        recovery_id: str,
        recovery_hash: str,
    ) -> dict[str, Any]:
        """Reauthenticate private stream and rotate the raw cleanup capability."""

        if not production_entrypoint_status()["available"]:
            raise UpbitFunctionalBlocked(
                "upbit-functional-production-entrypoint-unavailable"
            )
        permit, _parsed = self._load_approved_permit(
            permit_id=permit_id,
            permit_hash=permit_hash,
            session_id=session_id,
        )
        owner_recovery_attestation = self._load_approved_recovery(
            recovery_id=recovery_id,
            recovery_hash=recovery_hash,
            permit_id=permit_id,
            permit_hash=permit_hash,
            session_id=session_id,
        )
        writer = self.journal.recover_cleanup_authenticated(
            session_id=session_id,
            account_fingerprint=self.account_fingerprint,
        )
        handshake = self.websocket_handshake(
            session_id=session_id,
            journal=self.journal,
            writer_authority=dict(writer),
            cleanup_only=True,
        )
        self.__journal_writer = dict(writer)
        try:
            self._bind_authenticated_private_stream(
                session_id=session_id,
                writer=writer,
                handshake=handshake,
            )
        except Exception as exc:
            try:
                self.journal.mark_gap(
                    session_id,
                    detail=f"recovery-handshake-failed:{type(exc).__name__}",
                    writer_token=str(writer["writerToken"]),
                    writer_generation=int(writer["writerGeneration"]),
                )
            finally:
                self.__journal_writer = None
                self._close_private_stream()
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-websocket-handshake-failed"
            ) from exc
        client = OfficialUpbitFunctionalGetClient(
            expected_account_fingerprint=self.account_fingerprint,
            sender=self.sender,
        )
        durable = self.ledger.session(session_id)
        truth_reader = OfficialUpbitFunctionalTruthReader(
            client=client,
            account_fingerprint=self.account_fingerprint,
            session_started_at=datetime.fromisoformat(
                str(durable["starts_at"]).replace("Z", "+00:00")
            ),
            cleanup_deadline=datetime.fromisoformat(
                str(durable["cleanup_deadline"]).replace("Z", "+00:00")
            ),
            clock=self.clock,
            private_stream_reader=self.journal.snapshot,
            cleanup_recovery=True,
        )
        edge = UpbitFunctionalMutationEdge(
            session_id=session_id,
            account_fingerprint=self.account_fingerprint,
            permit_id=str(durable["permit_id"]),
            permit_hash=str(durable["permit_hash"]),
            route_scope_hash=str(
                self.runtime_reader().get("functionalTestRouteScopeHash") or ""
            ),
            session_scope_hash=str(durable["scope_hash"]),
            authority_reader=self.runtime_reader,
            claim_reader=lambda claim_id: self.ledger.mutation_authority(
                session_id, claim_id
            ),
            post_boundary_marker=self.ledger.mark_post_may_have_crossed,
            sender=self.sender,
        )
        service: UpbitContinuousFunctionalService | None = None
        attached = False
        try:
            service = UpbitContinuousFunctionalService.reattach_cleanup_after_owner_loss(
                permit=permit,
                ledger=self.ledger,
                session_id=session_id,
                owner_recovery_attestation=owner_recovery_attestation,
                truth_reader=truth_reader,
                post_order=edge.post,
                cancel_order=edge.cancel,
                lease_factory=self.lease_reader_factory,
                runtime_reader=self.runtime_reader,
                immutable_selection_reader=self._selection_reader,
                runtime_capability_registrar=self.runtime_capability_registrar,
                real_orders_reader=self.functional_orders_reader,
                terminal_stream_prepare=self.journal.prepare_terminal_attestation,
                terminal_stream_commit=lambda **payload: self._commit_terminal_stream(
                    payload
                ),
                terminal_stream_barrier=self._terminal_stream_barrier,
                clock=self.clock,
            )
            # Bind the cleanup owner before arming the only mutation lane.
            self.controller.attach_cleanup_recovery(service)
            attached = True
            rotated = self.ledger.session(session_id)
            self.arm_functional_orders(str(rotated["capability_hash"]))
            if not self.functional_orders_reader():
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-lane-arm-failed"
                )
            result = self.controller.resume_cleanup()
        except Exception as exc:
            try:
                if attached:
                    self.controller.fail_closed_after_start(
                        reason=f"cleanup-recovery-failed:{type(exc).__name__}"
                    )
                elif service is not None:
                    service.fail_closed_revoke(
                        reason=f"cleanup-recovery-failed:{type(exc).__name__}"
                    )
            finally:
                self._clear_all_functional_authority()
                try:
                    self.journal.mark_gap(
                        session_id,
                        detail=f"cleanup-recovery-failed:{type(exc).__name__}",
                        writer_token=str(writer["writerToken"]),
                        writer_generation=int(writer["writerGeneration"]),
                    )
                except Exception:
                    pass
                self.__journal_writer = None
                self._close_private_stream()
            raise
        if result.get("pending") is not True:
            final = self.ledger.session(session_id)
            if final["state"] != "FINALIZED":
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-final-state-required"
                )
            self.journal.startup_fail_closed(
                session_id=session_id,
                account_fingerprint=self.account_fingerprint,
                detail="cleanup-recovery-finalized",
                completed=True,
            )
            self.__journal_writer = None
            self._close_private_stream()
        return result

    def _load_approved_recovery(
        self,
        *,
        recovery_id: str,
        recovery_hash: str,
        permit_id: str,
        permit_hash: str,
        session_id: str,
    ) -> dict[str, Any]:
        """Load a server-owned, one-time cleanup recovery approval."""

        record = self.approved_recovery_reader(recovery_id, recovery_hash)
        if not isinstance(record, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-recovery-record-missing"
            )
        journal = next(
            (
                row
                for row in self.journal.active_sessions()
                if str(row["session_id"]) == session_id
            ),
            None,
        )
        if journal is None:
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-recovery-journal-missing"
            )
        exact = {
            "schemaVersion": "upbit-functional-recovery-approval/v1",
            "recoveryId": recovery_id,
            "contentHash": recovery_hash,
            "mode": "CLEANUP_ONLY",
            "sessionId": session_id,
            "permitId": permit_id,
            "permitHash": permit_hash,
            "accountFingerprint": self.account_fingerprint,
            "approvalState": "ACTIVE",
        }
        for field, expected in exact.items():
            if not hmac.compare_digest(
                str(record.get(field) or "").strip(), expected
            ):
                raise UpbitFunctionalBlocked(
                    f"upbit-functional-approved-recovery-{field}-mismatch"
                )
        if (
            int(record.get("previousWriterGeneration") or -1)
            != int(journal["writer_generation"])
            or int(record.get("nextWriterGeneration") or -1)
            != int(journal["writer_generation"]) + 1
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-recovery-writer-generation-mismatch"
            )
        required_true = (
            "serverManaged",
            "operatorAuthenticated",
            "operatorApproved",
            "singleUse",
            "previousOwnerLost",
            "previousOwnerLeaseExpired",
            "officialRestReconciled",
        )
        if any(record.get(field) is not True for field in required_true):
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-recovery-authorization-incomplete"
            )
        expected_hash = _stable_hash(
            {key: value for key, value in record.items() if key != "contentHash"}
        )
        if not hmac.compare_digest(expected_hash, recovery_hash):
            raise UpbitFunctionalBlocked(
                "upbit-functional-approved-recovery-content-hash-mismatch"
            )
        return dict(record)


def _build_upbit_functional_server_graph(
    **kwargs: Any,
) -> UpbitFunctionalProductionGraph:
    """Internal composition used only after the application lease attests."""

    from .process_safety import live_trader_instance_lease_status

    if live_trader_instance_lease_status().get("acquired") is not True:
        raise UpbitFunctionalBlocked(
            "upbit-functional-production-graph-application-lease-required"
        )
    return UpbitFunctionalProductionGraph(
        **kwargs,
        _capability=_GRAPH_CONSTRUCTION_CAPABILITY,
    )


def build_upbit_functional_production_graph(
    *,
    allow_mock_graph: bool = False,
    **kwargs: Any,
) -> UpbitFunctionalProductionGraph:
    """Explicit offline test builder; production uses the private factory."""

    if not allow_mock_graph:
        raise UpbitFunctionalBlocked(
            "upbit-functional-production-graph-direct-construction-forbidden"
        )
    return UpbitFunctionalProductionGraph(
        **kwargs,
        _capability=_MOCK_GRAPH_CONSTRUCTION_CAPABILITY,
    )


__all__ = [
    "UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE",
    "UPBIT_FUNCTIONAL_ROUTE_KEY",
    "UpbitFunctionalProductionGraph",
    "build_upbit_functional_production_graph",
    "production_entrypoint_status",
]
