from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader.upbit_continuous_functional import (
    UpbitFunctionalLedger,
    UpbitFunctionalBlocked,
    _activate_for_test,
    _stable_hash,
)
from live_trader.upbit_functional_approval import (
    DurableUpbitFunctionalApprovalStore,
    _functional_wiring_evidence_complete,
    _natural_claim_lifecycle_complete,
    _permit_immutable_lineage,
)
from tests.test_upbit_continuous_functional import (
    ACCOUNT,
    FakeBoundaries,
    TEST_EXCLUSIVITY_VERIFIER,
    TEST_EXCLUSIVITY_VERIFIER_PIN,
    UpbitContinuousFunctionalTest,
    permit,
)


class DurableUpbitFunctionalApprovalStoreTest(unittest.TestCase):
    def test_natural_fill_causal_order_is_strictly_inside_active_window(self) -> None:
        start = datetime(2026, 8, 13, 1, 0, tzinfo=timezone.utc)
        end = start + timedelta(hours=2)

        def claim(claimed, posted, resolved):
            return {
                "claimed_at": claimed.isoformat(),
                "post_boundary_at": posted.isoformat(),
                "resolved_at": resolved.isoformat(),
            }

        valid = claim(start, start, end - timedelta(microseconds=1))
        self.assertTrue(
            _natural_claim_lifecycle_complete(
                valid,
                starts_at=start,
                ends_at=end,
                fill_occurred_at=end - timedelta(microseconds=1),
            )
        )
        delayed_fill = claim(
            start,
            start + timedelta(seconds=1),
            start + timedelta(seconds=2),
        )
        self.assertTrue(
            _natural_claim_lifecycle_complete(
                delayed_fill,
                starts_at=start,
                ends_at=end,
                fill_occurred_at=start + timedelta(seconds=3),
            )
        )
        for row, fill_time in (
            (valid, end),
            (valid, end + timedelta(microseconds=1)),
            (valid, start - timedelta(microseconds=1)),
            (
                claim(start + timedelta(seconds=2), start, start + timedelta(seconds=3)),
                start + timedelta(seconds=1),
            ),
            (
                claim(start, start + timedelta(seconds=2), start + timedelta(seconds=1)),
                start + timedelta(seconds=2),
            ),
        ):
            with self.subTest(row=row, fill_time=fill_time):
                self.assertFalse(
                    _natural_claim_lifecycle_complete(
                        row,
                        starts_at=start,
                        ends_at=end,
                        fill_occurred_at=fill_time,
                    )
                )

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.functional_permit = permit()
        self.fake = FakeBoundaries(self.functional_permit)
        self.store = DurableUpbitFunctionalApprovalStore(
            Path(self.temp.name) / "approvals.sqlite3",
            clock=self.fake.clock,
            operator_verifier=lambda value: value.get("serverSignature")
            == "verified-by-backend",
            account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
            account_exclusivity_verifier_pin=(
                TEST_EXCLUSIVITY_VERIFIER_PIN
            ),
        )
        self.approval = {
            "approvalId": "upbit-approved-permit-0001",
            "operatorId": "operator-you",
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "permitId": self.functional_permit.permit_id,
            "permitHash": self.functional_permit.content_hash,
            "accountFingerprint": ACCOUNT,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "approvedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
            "nonce": "operator-approval-nonce-000000000001",
            "serverSignature": "verified-by-backend",
        }

    def approve(self) -> None:
        self.store.approve_permit(
            self.functional_permit.to_dict(), self.approval
        )

    def test_durable_owner_lease_fences_claim_and_rotates_only_after_loss(
        self,
    ) -> None:
        path = Path(self.temp.name) / "owner-lease.sqlite3"
        store = DurableUpbitFunctionalApprovalStore(
            path,
            clock=self.fake.clock,
            operator_verifier=lambda value: value.get("serverSignature")
            == "verified-by-backend",
            account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
            account_exclusivity_verifier_pin=(
                TEST_EXCLUSIVITY_VERIFIER_PIN
            ),
            durable_owner_lease_required=True,
        )
        store.approve_permit(
            self.functional_permit.to_dict(), self.approval
        )
        preparation = store.first_live_preparation_status()
        self.assertTrue(preparation["prepared"])
        self.assertTrue(preparation["ownerLease"]["required"])
        session_id = "upbit-owner-session-0001"
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "owner-lease-required"
        ):
            store.claim_permit(
                approval_id=self.approval["approvalId"],
                session_id=session_id,
            )

        owner_id = "upbit-owner-process-0001"
        owner_token = "owner-token-" + "x" * 64
        lease = store.acquire_owner_lease(
            approval_id=self.approval["approvalId"],
            owner_id=owner_id,
            owner_token=owner_token,
            process_identity_hash="a" * 64,
        )
        self.assertEqual("ACQUIRED", lease["state"])
        self.assertTrue(lease["recordHashVerified"])
        self.assertNotIn(owner_token, json.dumps(lease, sort_keys=True))
        with closing(sqlite3.connect(path)) as connection:
            stored_token_hash = connection.execute(
                "SELECT owner_token_hash FROM upbit_functional_owner_lease"
            ).fetchone()[0]
        self.assertNotEqual(owner_token, stored_token_hash)
        self.assertEqual(64, len(stored_token_hash))

        claimed = store.claim_permit(
            approval_id=self.approval["approvalId"],
            session_id=session_id,
            owner_lease_id=lease["leaseId"],
            owner_id=owner_id,
            owner_token=owner_token,
        )
        self.assertEqual(session_id, claimed["activeSessionId"])
        active = store.owner_lease_status(
            approval_id=self.approval["approvalId"]
        )
        self.assertEqual("ACTIVE", active["state"])
        self.assertEqual(session_id, active["sessionId"])
        self.assertTrue(active["recordHashVerified"])
        store.bind_permit(
            approval_id=self.approval["approvalId"],
            session_id=session_id,
            owner_id=owner_id,
            owner_token=owner_token,
        )
        heartbeat = store.heartbeat_owner_lease(
            approval_id=self.approval["approvalId"],
            session_id=session_id,
            owner_id=owner_id,
            owner_token=owner_token,
        )
        self.assertTrue(heartbeat["recordHashVerified"])
        lost = store.finish_owner_lease(
            approval_id=self.approval["approvalId"],
            owner_id=owner_id,
            owner_token=owner_token,
            state="LOST",
            detail="test process absence attested",
        )
        self.assertEqual("LOST", lost["state"])
        self.assertTrue(lost["recordHashVerified"])
        self.assertFalse(
            store.owner_lease_active(
                approval_id=self.approval["approvalId"],
                session_id=session_id,
                owner_id=owner_id,
                owner_token=owner_token,
            )
        )

        cleanup_token = "cleanup-owner-token-" + "y" * 64
        cleanup = store.acquire_owner_lease(
            approval_id=self.approval["approvalId"],
            owner_id="upbit-cleanup-owner-0001",
            owner_token=cleanup_token,
            process_identity_hash="b" * 64,
            cleanup_only=True,
        )
        self.assertEqual("ACTIVE", cleanup["state"])
        self.assertTrue(cleanup["cleanupOnly"])
        self.assertTrue(cleanup["recordHashVerified"])
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "cleanup-owner-rotation-invalid"
        ):
            store.acquire_owner_lease(
                approval_id=self.approval["approvalId"],
                owner_id="upbit-cleanup-owner-0002",
                owner_token="z" * 64,
                process_identity_hash="c" * 64,
                cleanup_only=True,
            )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "process-absence-proof-required"
        ):
            store.attest_owner_process_absent(
                process_absence_attested=False,
                detail="untrusted caller assertion",
            )
        attested = store.attest_owner_process_absent(
            process_absence_attested=True,
            detail="independent application lease proves old process absent",
        )
        self.assertEqual(1, len(attested))
        self.assertEqual("LOST", attested[0]["state"])
        self.assertTrue(attested[0]["recordHashVerified"])
        restarted_cleanup = store.acquire_owner_lease(
            approval_id=self.approval["approvalId"],
            owner_id="upbit-cleanup-owner-0003",
            owner_token="restart-token-" + "r" * 64,
            process_identity_hash="c" * 64,
            cleanup_only=True,
        )
        self.assertEqual("ACTIVE", restarted_cleanup["state"])
        self.assertTrue(restarted_cleanup["recordHashVerified"])
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                "UPDATE upbit_functional_owner_lease SET detail='tampered'"
            )
            connection.commit()
        tampered = store.owner_lease_status(
            approval_id=self.approval["approvalId"]
        )
        self.assertFalse(tampered["recordHashVerified"])
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "record-hash-invalid"
        ):
            store.attest_owner_process_absent(
                process_absence_attested=True,
                detail="must not bless a corrupted owner record",
            )

    def test_owner_finish_serializes_exactly_against_concurrent_heartbeat(
        self,
    ) -> None:
        path = Path(self.temp.name) / "owner-finish-race.sqlite3"

        def make_store() -> DurableUpbitFunctionalApprovalStore:
            return DurableUpbitFunctionalApprovalStore(
                path,
                clock=self.fake.clock,
                operator_verifier=lambda value: value.get(
                    "serverSignature"
                )
                == "verified-by-backend",
                account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
                account_exclusivity_verifier_pin=(
                    TEST_EXCLUSIVITY_VERIFIER_PIN
                ),
                durable_owner_lease_required=True,
            )

        finish_store = make_store()
        finish_store.approve_permit(
            self.functional_permit.to_dict(), self.approval
        )
        owner_id = "upbit-racing-owner-0001"
        owner_token = "racing-owner-token-" + "x" * 64
        lease = finish_store.acquire_owner_lease(
            approval_id=self.approval["approvalId"],
            owner_id=owner_id,
            owner_token=owner_token,
            process_identity_hash="d" * 64,
        )
        session_id = "upbit-racing-session-0001"
        finish_store.claim_permit(
            approval_id=self.approval["approvalId"],
            session_id=session_id,
            owner_lease_id=lease["leaseId"],
            owner_id=owner_id,
            owner_token=owner_token,
        )
        finish_store.bind_permit(
            approval_id=self.approval["approvalId"],
            session_id=session_id,
            owner_id=owner_id,
            owner_token=owner_token,
        )
        heartbeat_store = make_store()

        selected = threading.Event()
        allow_finish = threading.Event()
        heartbeat_started = threading.Event()
        heartbeat_done = threading.Event()
        original_connect = finish_store._connect

        class CursorProxy:
            def __init__(self, cursor):
                self._cursor = cursor

            def fetchone(self):
                row = self._cursor.fetchone()
                selected.set()
                if not allow_finish.wait(5):
                    raise AssertionError("finish SELECT test barrier timed out")
                return row

            def __getattr__(self, name):
                return getattr(self._cursor, name)

        class ConnectionProxy:
            def __init__(self, connection):
                self._connection = connection

            def execute(self, statement, parameters=()):
                cursor = self._connection.execute(statement, parameters)
                normalized = " ".join(str(statement).split())
                if normalized.startswith(
                    "SELECT * FROM upbit_functional_owner_lease"
                ):
                    return CursorProxy(cursor)
                return cursor

            def __getattr__(self, name):
                return getattr(self._connection, name)

        finish_store._connect = lambda: ConnectionProxy(original_connect())

        def finish():
            return finish_store.finish_owner_lease(
                approval_id=self.approval["approvalId"],
                owner_id=owner_id,
                owner_token=owner_token,
                state="RELEASED",
                detail="terminal owner release wins exact CAS",
            )

        def heartbeat():
            heartbeat_started.set()
            try:
                heartbeat_store.heartbeat_owner_lease(
                    approval_id=self.approval["approvalId"],
                    session_id=session_id,
                    owner_id=owner_id,
                    owner_token=owner_token,
                )
                return "COMMITTED"
            except UpbitFunctionalBlocked:
                return "BLOCKED_AFTER_TERMINAL"
            finally:
                heartbeat_done.set()

        with ThreadPoolExecutor(max_workers=2) as executor:
            finish_future = executor.submit(finish)
            self.assertTrue(selected.wait(2))
            heartbeat_future = executor.submit(heartbeat)
            self.assertTrue(heartbeat_started.wait(2))
            try:
                # BEGIN IMMEDIATE must keep the other store from committing a
                # newer revision between terminal SELECT and UPDATE.
                self.assertFalse(heartbeat_done.wait(0.2))
            finally:
                allow_finish.set()
            finished = finish_future.result(timeout=5)
            heartbeat_outcome = heartbeat_future.result(timeout=5)

        self.assertEqual("RELEASED", finished["state"])
        self.assertTrue(finished["recordHashVerified"])
        self.assertEqual("BLOCKED_AFTER_TERMINAL", heartbeat_outcome)
        final = heartbeat_store.owner_lease_status(
            approval_id=self.approval["approvalId"]
        )
        self.assertEqual("RELEASED", final["state"])
        self.assertTrue(final["recordHashVerified"])
        self.assertEqual(3, final["revision"])
        with closing(sqlite3.connect(path)) as connection:
            raw = connection.execute(
                """SELECT session_id,revision,record_hash,owner_token_hash
                FROM upbit_functional_owner_lease WHERE approval_id=?""",
                (self.approval["approvalId"],),
            ).fetchone()
        self.assertEqual(session_id, raw[0])
        self.assertEqual(final["revision"], raw[1])
        self.assertEqual(final["recordHash"], raw[2])
        self.assertEqual("", raw[3])

    def test_wiring_verifier_recomputes_primitives_and_rejects_tamper(self) -> None:
        self.fake.now = self.functional_permit.ends_at
        account_exclusivity_proof = self.fake.account_exclusivity_proof()
        terminal_body = {
            "schemaVersion": "upbit-functional-private-terminal-seal/v1",
            "streamContinuous": True,
            "gapDetected": False,
            "externalActivityAbsent": True,
        }
        evidence = {
            "schemaVersion": "upbit-continuous-functional-v1",
            "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
            "promotionEligible": False,
            "sessionId": self.fake.session_id,
            "accountFingerprint": ACCOUNT,
            "functionalWiringPassed": True,
            "strategyBuyTerminalFilled": True,
            "strategySellTerminalFilled": True,
            "strategyBuyReconciled": True,
            "strategySellReconciled": True,
            "fillAndFeeTruthComplete": True,
            "strategyOrderCountExact": True,
            "noReentryVerified": True,
            "claimCount": 2,
            "cleanupFlattenUsed": False,
            "strategyNotionalCapSatisfied": True,
            "strategyGrossExposureCapSatisfied": True,
            "strategyBuyExecutedNotional": "10000",
            "maxOrderNotionalKRW": "10000",
            "maxObservedOwnerGrossExposure": "10000",
            "maxGrossExposureKRW": "10000",
            "ownerLossLimitSatisfied": True,
            "ownerLoss": "10",
            "maxOwnerLoss": "1000",
            "fees": "10",
            "preexistingBaselinePreserved": True,
            "baselineRestoredWithinExchangePrecision": True,
            "orderableResidual": False,
            "accountOpenOrderCount": 0,
            "ownedWorkingOrderCount": 0,
            "privateStreamContinuous": True,
            "accountExternalActivityAbsent": True,
            "accountExclusivityProof": account_exclusivity_proof,
            "accountExclusivityProofHash": _stable_hash(
                account_exclusivity_proof
            ),
            "accountExclusivityProofVerified": True,
            "accountExclusivityAuthorityPinned": True,
            "accountExclusivityContinuouslyVerified": True,
            "otherApiKeysAbsent": True,
            "manualTradingAbsent": True,
            "otherBotsAbsent": True,
            "terminalPrivateStreamSeal": {
                **terminal_body,
                "sealHash": _stable_hash(terminal_body),
            },
            "activatedAt": "2026-08-13T01:10:00Z",
            "permitEndsAt": "2026-08-13T03:10:00Z",
            "finalObservedAt": "2026-08-13T03:10:00Z",
            "terminalObservationStartedAt": "2026-08-13T03:10:00Z",
            "actualDurationSeconds": "7200",
            "processMonotonicElapsedSeconds": "7200",
            "processMonotonicContinuity": True,
            "clockDiscontinuityAbsent": True,
            "requiredActiveDurationSeconds": "7200",
            "activationRelativePermitExact": True,
            "exactTwoHourRuntimeComplete": True,
            "functionalCapabilityCleared": True,
            "functionalMutationEnabled": False,
            "realOrdersEnabled": False,
            "newEntriesBlocked": True,
            "officialRestRawSnapshotHash": "a" * 64,
        }
        def verified(candidate):
            return _functional_wiring_evidence_complete(
                candidate,
                account_exclusivity_verifier=(
                    TEST_EXCLUSIVITY_VERIFIER
                ),
                account_exclusivity_verifier_pin=(
                    TEST_EXCLUSIVITY_VERIFIER_PIN
                ),
            )

        self.assertTrue(verified(evidence))
        for field in (
            "fillAndFeeTruthComplete",
            "accountOpenOrderCount",
            "exactTwoHourRuntimeComplete",
            "functionalCapabilityCleared",
        ):
            with self.subTest(missing=field):
                tampered = dict(evidence)
                tampered.pop(field)
                self.assertFalse(
                    verified(tampered)
                )
        over_cap = {**evidence, "strategyBuyExecutedNotional": "10000.01"}
        self.assertFalse(verified(over_cap))
        for field in (
            "accountExclusivityProof",
            "otherApiKeysAbsent",
            "manualTradingAbsent",
            "otherBotsAbsent",
        ):
            with self.subTest(exclusivity_missing=field):
                missing = dict(evidence)
                missing.pop(field)
                self.assertFalse(verified(missing))
        tampered_proof = dict(evidence)
        tampered_proof["accountExclusivityProof"] = {
            **account_exclusivity_proof,
            "signature": "tampered-signature",
        }
        tampered_proof["accountExclusivityProofHash"] = _stable_hash(
            tampered_proof["accountExclusivityProof"]
        )
        self.assertFalse(verified(tampered_proof))
        self.assertFalse(_functional_wiring_evidence_complete(evidence))

    def test_missing_exclusivity_proof_cannot_complete_first_live_bootstrap(
        self,
    ) -> None:
        self.approve()
        ledger = UpbitFunctionalLedger(
            self.store.path,
            clock=self.fake.clock,
        )
        service = _activate_for_test(
            permit=self.functional_permit,
            ledger=ledger,
            session_id=self.fake.session_id,
            truth_reader=self.fake.truth,
            post_order=self.fake.post,
            cancel_order=self.fake.cancel,
            lease_factory=self.fake.lease,
            runtime_reader=self.fake.runtime,
            immutable_selection_reader=self.fake.immutable_selection,
            runtime_capability_registrar=self.fake.register_capability,
            real_orders_reader=lambda: self.fake.real_orders,
            clock=self.fake.clock,
            monotonic_clock=self.fake.monotonic,
            account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
            account_exclusivity_verifier_pin=(
                TEST_EXCLUSIVITY_VERIFIER_PIN
            ),
        )
        self.fake.real_orders = True
        self.fake.runtime_updates.update(
            {"newEntriesBlocked": True, "realOrdersEnabled": True}
        )
        service.on_bar(UpbitContinuousFunctionalTest.bar("BUY"))
        service.on_bar(
            UpbitContinuousFunctionalTest.bar(
                "SELL", bar_id="upbit-five-minute-bar-0002"
            )
        )
        self.fake.include_account_exclusivity_proof = False
        service.recover_or_expire(reason="operator-stop")
        self.fake.real_orders = False
        self.fake.runtime_updates.update(
            {"newEntriesBlocked": True, "realOrdersEnabled": False}
        )
        self.fake.now = self.functional_permit.ends_at
        final = service.finalize_if_flat()
        self.assertEqual("SAFE_INCOMPLETE", final["testOutcome"])
        self.assertFalse(final["evidence"]["functionalTestPassed"])
        self.assertFalse(final["evidence"]["promotionEligible"])

        bootstrap_id = "upbit-first-live-bootstrap-missing-proof-0001"
        with closing(sqlite3.connect(self.store.path)) as connection:
            approval = connection.execute(
                """SELECT candidate_hash,candidate_json FROM
                upbit_functional_approvals WHERE approval_id=?""",
                (self.approval["approvalId"],),
            ).fetchone()
            connection.execute(
                """UPDATE upbit_functional_approvals
                SET state='ACTIVE',claimed_session_id=?
                WHERE approval_id=?""",
                (self.fake.session_id, self.approval["approvalId"]),
            )
            connection.execute(
                """INSERT INTO upbit_functional_e2e_bootstrap
                (bootstrap_id,approval_id,candidate_hash,candidate_json,
                 session_nonce,state,claimed_session_id,detail,updated_at)
                VALUES (?,?,?,?,?,'CLAIMED',?,'offline hostile test',?)""",
                (
                    bootstrap_id,
                    self.approval["approvalId"],
                    approval[0],
                    approval[1],
                    "offline-hostile-session-nonce-000000000001",
                    self.fake.session_id,
                    self.fake.now.isoformat(),
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked,
            "first-live-pass-proof-incomplete",
        ):
            self.store.finish_first_live_bootstrap(
                approval_id=self.approval["approvalId"],
                session_id=self.fake.session_id,
                passed=True,
                evidence_hash=final["evidenceHash"],
                detail="must not pass",
            )
        terminal = self.store.finish_first_live_bootstrap(
            approval_id=self.approval["approvalId"],
            session_id=self.fake.session_id,
            passed=False,
            evidence_hash=final["evidenceHash"],
            detail="missing account exclusivity proof",
        )
        self.assertEqual("FAILED", terminal["state"])
        self.assertEqual(0, self.store.real_e2e_status()["validated"])

    def test_first_live_rejects_uppercase_caller_or_durable_evidence_hash(
        self,
    ) -> None:
        self.approve()
        session_id = self.fake.session_id
        UpbitFunctionalLedger(self.store.path, clock=self.fake.clock)
        with closing(sqlite3.connect(self.store.path)) as connection:
            approval = connection.execute(
                """SELECT candidate_hash,candidate_json FROM
                upbit_functional_approvals WHERE approval_id=?""",
                (self.approval["approvalId"],),
            ).fetchone()
            connection.execute(
                """UPDATE upbit_functional_approvals
                SET state='ACTIVE',claimed_session_id=? WHERE approval_id=?""",
                (session_id, self.approval["approvalId"]),
            )
            evidence = {
                "schemaVersion": "uppercase-evidence-hostile/v1",
                "functionalTestPassed": False,
                "promotionEligible": False,
            }
            evidence_json = json.dumps(
                evidence, sort_keys=True, separators=(",", ":")
            )
            evidence_hash = _stable_hash(evidence)
            connection.execute(
                """INSERT INTO upbit_functional_sessions
                (session_id,permit_id,permit_hash,scope_hash,state,starts_at,
                 expires_at,cleanup_deadline,baseline_base,baseline_quote,
                 new_entries_blocked,real_orders_enabled,capability_hash,
                 final_evidence_json,final_evidence_hash)
                VALUES (?,?,?,?,?,?,?,?,?,?,1,0,'',?,?)""",
                (
                    session_id,
                    self.functional_permit.permit_id,
                    self.functional_permit.content_hash,
                    "a" * 64,
                    "FINALIZED",
                    self.fake.now.isoformat(),
                    self.functional_permit.ends_at.isoformat(),
                    (
                        self.functional_permit.ends_at + timedelta(hours=1)
                    ).isoformat(),
                    "0",
                    "0",
                    evidence_json,
                    evidence_hash,
                ),
            )
            connection.execute(
                """INSERT INTO upbit_functional_e2e_bootstrap
                (bootstrap_id,approval_id,candidate_hash,candidate_json,
                 session_nonce,state,claimed_session_id,detail,updated_at)
                VALUES (?,?,?,?,?,'CLAIMED',?,'uppercase hostile',?)""",
                (
                    "upbit-first-live-uppercase-hash-0001",
                    self.approval["approvalId"],
                    approval[0],
                    approval[1],
                    "uppercase-hostile-session-nonce-0000001",
                    session_id,
                    self.fake.now.isoformat(),
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked,
            "evidence-hash-invalid",
        ):
            self.store.finish_first_live_bootstrap(
                approval_id=self.approval["approvalId"],
                session_id=session_id,
                passed=False,
                evidence_hash=evidence_hash.upper(),
                detail="uppercase caller hash",
            )
        with closing(sqlite3.connect(self.store.path)) as connection:
            connection.execute(
                """UPDATE upbit_functional_sessions SET final_evidence_hash=?
                WHERE session_id=?""",
                (evidence_hash.upper(), session_id),
            )
            connection.commit()
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked,
            "durable-final-mismatch",
        ):
            self.store.finish_first_live_bootstrap(
                approval_id=self.approval["approvalId"],
                session_id=session_id,
                passed=False,
                evidence_hash=evidence_hash,
                detail="uppercase durable hash",
            )

    def test_permit_is_claimed_once_bound_and_consumed(self) -> None:
        self.approve()
        session = "upbit-approved-session-0001"
        claimed = self.store.claim_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        activated = claimed["permit"]
        self.assertNotEqual(
            self.functional_permit.permit_id, claimed["permitId"]
        )
        self.assertNotEqual(
            self.functional_permit.content_hash, claimed["permitHash"]
        )
        self.assertEqual(
            self.functional_permit.binding.snapshot(), activated["binding"]
        )
        self.assertEqual(
            self.fake.now.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            activated["startsAt"],
        )
        self.assertEqual(
            (self.fake.now + timedelta(hours=2))
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            activated["endsAt"],
        )
        status = self.store.permit_status(self.approval["approvalId"])
        self.assertEqual(
            self.functional_permit.permit_id,
            status["candidate_permit_id"],
        )
        self.assertEqual(claimed["permitId"], status["permit_id"])
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "not-claimable"
        ):
            self.store.claim_permit(
                approval_id=self.approval["approvalId"],
                session_id="upbit-approved-session-0002",
            )
        self.store.bind_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        self.store.consume_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        # A crash/restart may replay the terminal hand-off after the durable
        # trade session is already final.  The exact same pointer is safe and
        # idempotent; a different session remains forbidden.
        self.store.consume_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        self.assertEqual(
            "CONSUMED",
            self.store.permit_status(self.approval["approvalId"])["state"],
        )

    def test_concurrent_start_claim_has_exactly_one_winner(self) -> None:
        self.approve()

        def claim(index: int) -> str:
            try:
                self.store.claim_permit(
                    approval_id=self.approval["approvalId"],
                    session_id=f"upbit-concurrent-session-{index:04d}",
                )
                return "CLAIMED"
            except UpbitFunctionalBlocked:
                return "BLOCKED"

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(claim, (1, 2)))
        self.assertEqual(1, outcomes.count("CLAIMED"))
        self.assertEqual(1, outcomes.count("BLOCKED"))

    def test_approved_candidate_cannot_be_claimed_after_five_minutes(self) -> None:
        self.approve()
        self.fake.now += timedelta(seconds=301)
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "approval-expired"):
            self.store.claim_permit(
                approval_id=self.approval["approvalId"],
                session_id="upbit-expired-approval-session-0001",
            )
        self.assertEqual(
            "EXPIRED",
            self.store.permit_status(self.approval["approvalId"])["state"],
        )

    def test_server_candidate_is_inert_until_exact_operator_cas(self) -> None:
        self.fake.now = self.functional_permit.starts_at
        candidate = {
            "schemaVersion": "upbit-functional-server-permit-candidate/v1",
            "approvalId": "upbit-server-candidate-0001",
            "permitId": self.functional_permit.permit_id,
            "permitHash": self.functional_permit.content_hash,
            "accountFingerprint": ACCOUNT,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "serverManaged": True,
            "singleUse": True,
            "issuer": "LIVE_TRADER_SERVER",
            "issuedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
            "nonce": "server-candidate-nonce-000000000001",
            "serverSignature": "verified-by-backend",
        }
        issued = self.store.issue_permit_candidate(
            self.functional_permit.to_dict(), candidate
        )
        self.assertEqual("ISSUED", issued["state"])
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "not-claimable"):
            self.store.claim_permit(
                approval_id=candidate["approvalId"],
                session_id="upbit-candidate-session-0001",
            )
        approval = {
            **self.approval,
            "approvalId": candidate["approvalId"],
            "approvedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
        }
        approved = self.store.approve_issued_permit(
            approval_id=candidate["approvalId"], approval=approval
        )
        self.assertEqual("APPROVED", approved["state"])
        claimed = self.store.claim_permit(
            approval_id=candidate["approvalId"],
            session_id="upbit-candidate-session-0001",
        )
        self.assertEqual(candidate["approvalId"], claimed["approvalId"])

    def test_code_manifest_is_path_qualified_and_drift_blocks_approval(self) -> None:
        manifest = state._upbit_functional_code_manifest()
        files = manifest["files"]
        self.assertIn(
            "live_trader/binance_order_authority.py", files
        )
        self.assertIn(
            "live_trader/upbit_functional_publication.py", files
        )
        self.assertIn(
            "live_trader/upbit_order_authority.py", files
        )
        for required in (
            "continuous_live.py",
            "env_loader.py",
            "env_settings.py",
            "safety_confirmation.py",
        ):
            self.assertIn(f"live_trader/{required}", files)
        self.assertIn(
            "packages/trading_runtime/trading_runtime/functional_test.py",
            files,
        )
        self.assertTrue(
            all("/" in label and "\\" not in label for label in files)
        )
        current = {"value": manifest}
        store = DurableUpbitFunctionalApprovalStore(
            Path(self.temp.name) / "manifest.sqlite3",
            clock=self.fake.clock,
            operator_verifier=lambda value: value.get("serverSignature")
            == "verified-by-backend",
            code_manifest_reader=lambda: current["value"],
        )
        binding = {
            "schemaVersion": "upbit-functional-server-candidate-binding/v1",
            "immutablePermit": _permit_immutable_lineage(
                self.functional_permit
            ),
            "codeManifest": manifest,
        }
        candidate = {
            "schemaVersion": "upbit-functional-server-permit-candidate/v2",
            "approvalId": "upbit-manifest-candidate-0001",
            "permitId": self.functional_permit.permit_id,
            "permitHash": self.functional_permit.content_hash,
            "accountFingerprint": ACCOUNT,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "serverManaged": True,
            "singleUse": True,
            "issuer": "LIVE_TRADER_SERVER",
            "issuedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
            "nonce": "manifest-candidate-nonce-000000000001",
            "candidateBinding": binding,
            "candidateHash": _stable_hash(binding),
            "serverSignature": "verified-by-backend",
        }
        store.issue_permit_candidate(
            self.functional_permit.to_dict(), candidate
        )
        changed = {
            **manifest,
            "files": {
                **files,
                "live_trader/upbit_functional_publication.py": "f" * 64,
            },
            "manifestHash": "e" * 64,
        }
        current["value"] = changed
        approval = {
            **self.approval,
            "approvalId": candidate["approvalId"],
            "candidateHash": candidate["candidateHash"],
        }
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "code-manifest-drift"
        ):
            store.approve_issued_permit(
                approval_id=candidate["approvalId"], approval=approval
            )
        current["value"] = manifest
        store.approve_issued_permit(
            approval_id=candidate["approvalId"], approval=approval
        )
        current["value"] = {
            **manifest,
            "files": {
                **files,
                "live_trader/binance_order_authority.py": "d" * 64,
            },
            "manifestHash": "c" * 64,
        }
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "code-manifest-drift"
        ):
            store.claim_permit(
                approval_id=candidate["approvalId"],
                session_id="upbit-manifest-session-0001",
            )

    def test_global_reservation_material_is_post_claim_and_recomputed(self) -> None:
        manifest = state._upbit_functional_code_manifest()
        store = DurableUpbitFunctionalApprovalStore(
            Path(self.temp.name) / "global-material.sqlite3",
            clock=self.fake.clock,
            operator_verifier=lambda value: value.get("serverSignature")
            == "verified-by-backend",
            code_manifest_reader=lambda: manifest,
        )
        binding = {
            "schemaVersion": "upbit-functional-server-candidate-binding/v1",
            "immutablePermit": _permit_immutable_lineage(
                self.functional_permit
            ),
            "codeManifest": manifest,
        }
        candidate = {
            "schemaVersion": "upbit-functional-server-permit-candidate/v2",
            "approvalId": "upbit-global-material-candidate-0001",
            "permitId": self.functional_permit.permit_id,
            "permitHash": self.functional_permit.content_hash,
            "accountFingerprint": ACCOUNT,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "serverManaged": True,
            "singleUse": True,
            "issuer": "LIVE_TRADER_SERVER",
            "issuedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
            "nonce": "global-material-candidate-nonce-000000000001",
            "candidateBinding": binding,
            "candidateHash": _stable_hash(binding),
            "serverSignature": "verified-by-backend",
        }
        store.issue_permit_candidate(
            self.functional_permit.to_dict(), candidate
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "global-reservation-pointer-missing"
        ):
            store.global_reservation_material(
                candidate["approvalId"],
                session_id="upbit-global-material-session-0001",
            )
        approval = {
            **self.approval,
            "approvalId": candidate["approvalId"],
            "candidateHash": candidate["candidateHash"],
        }
        store.approve_issued_permit(
            approval_id=candidate["approvalId"], approval=approval
        )
        claimed = store.claim_permit(
            approval_id=candidate["approvalId"],
            session_id="upbit-global-material-session-0001",
        )
        material = store.global_reservation_material(
            candidate["approvalId"],
            session_id="upbit-global-material-session-0001",
        )
        self.assertEqual(
            "upbit-functional-global-reservation-material/v1",
            material["schemaVersion"],
        )
        self.assertEqual("UPBIT", material["lane"])
        self.assertEqual(claimed["permitId"], material["permitId"])
        self.assertEqual(claimed["permitHash"], material["permitHash"])
        self.assertEqual(manifest["manifestHash"], material["codeHash"])
        self.assertEqual(7200, material["activeDurationSeconds"])
        self.assertFalse(material["promotionEligible"])
        for field in (
            "candidateHash",
            "activationLineageHash",
            "codeHash",
            "operatorApprovalHash",
        ):
            self.assertRegex(material[field], r"^[0-9a-f]{64}$")

    def test_startup_binds_exact_claimed_durable_session_cleanup_only(self) -> None:
        self.approve()
        session = "upbit-crash-after-activate-0001"
        claimed = self.store.claim_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        audit = self.store.audit_startup(
            ledger_sessions={
                session: {
                    "state": "CLEANUP",
                    "permit_id": claimed["permitId"],
                    "permit_hash": claimed["permitHash"],
                    "account_fingerprint": ACCOUNT,
                }
            }
        )
        self.assertTrue(audit["complete"])
        self.assertEqual(
            "CLAIMED_BOUND_CLEANUP_ONLY", audit["actions"][0]["action"]
        )
        self.assertEqual(
            "ACTIVE",
            self.store.permit_status(self.approval["approvalId"])["state"],
        )

    def test_startup_retires_fresh_approved_pointer_with_exact_zero_side_effect_proof(
        self,
    ) -> None:
        self.approve()
        audit = self.store.audit_startup(
            ledger_sessions={},
            ledger_claims={},
            journal_sessions={},
            owner_session_ids=(),
        )
        self.assertTrue(audit["complete"])
        self.assertEqual(
            "APPROVED_FAILED_PRECLAIM", audit["actions"][0]["action"]
        )
        status = self.store.permit_status(self.approval["approvalId"])
        self.assertEqual("FAILED", status["state"])
        self.assertEqual("", status["claimed_session_id"])
        self.assertIsNone(self.store.order_authority_pointer())

    def test_startup_expires_unclaimed_approval_after_permit_window(self) -> None:
        self.approve()
        self.fake.now = self.functional_permit.ends_at + timedelta(seconds=1)
        audit = self.store.audit_startup(
            ledger_sessions={},
            ledger_claims={},
            journal_sessions={},
            owner_session_ids=(),
        )
        self.assertTrue(audit["complete"])
        self.assertEqual(
            "APPROVED_EXPIRED_PRECLAIM", audit["actions"][0]["action"]
        )
        self.assertEqual(
            "EXPIRED",
            self.store.permit_status(self.approval["approvalId"])["state"],
        )

    def test_startup_does_not_retire_approved_pointer_with_durable_side_effect(
        self,
    ) -> None:
        self.approve()
        session = "upbit-approved-unclaimed-residue-0001"
        audit = self.store.audit_startup(
            ledger_sessions={
                session: {
                    "state": "ACTIVE",
                    "permit_id": self.functional_permit.permit_id,
                    "permit_hash": self.functional_permit.content_hash,
                    "account_fingerprint": ACCOUNT,
                }
            },
            ledger_claims={session: [{"claim_state": "POST_MAY_HAVE_CROSSED"}]},
            journal_sessions={},
            owner_session_ids=(),
        )
        self.assertFalse(audit["complete"])
        self.assertEqual(
            "APPROVED_SIDE_EFFECT_PROOF_BLOCKED",
            audit["actions"][0]["action"],
        )
        self.assertEqual(
            "APPROVED",
            self.store.permit_status(self.approval["approvalId"])["state"],
        )

    def test_startup_rejects_claimed_durable_identity_mismatch(self) -> None:
        self.approve()
        session = "upbit-crash-mismatch-session-0001"
        self.store.claim_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        audit = self.store.audit_startup(
            ledger_sessions={
                session: {
                    "state": "CLEANUP",
                    "permit_id": self.functional_permit.permit_id,
                    "permit_hash": "f" * 64,
                    "account_fingerprint": ACCOUNT,
                }
            }
        )
        self.assertFalse(audit["complete"])
        self.assertEqual(
            "CLAIMED_DURABLE_MISMATCH_BLOCKED",
            audit["actions"][0]["action"],
        )

    def test_startup_audit_fails_orphan_claim_and_consumes_final(self) -> None:
        self.approve()
        session = "upbit-orphan-claim-session-0001"
        self.store.claim_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        audit = self.store.audit_startup(ledger_sessions={})
        self.assertEqual("CLAIMED_FAILED_CLOSED", audit["actions"][0]["action"])
        self.assertEqual(
            "FAILED",
            self.store.permit_status(self.approval["approvalId"])["state"],
        )

        other = dict(self.approval)
        other["approvalId"] = "upbit-approved-permit-0002"
        other["nonce"] = "operator-approval-nonce-000000000002"
        # A shared permit id/hash cannot be republished; this is itself part
        # of the durable anti-replay contract.
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "replay"):
            self.store.approve_permit(self.functional_permit.to_dict(), other)

    def test_recovery_is_server_verified_one_time_and_exact_session_bound(self) -> None:
        self.approve()
        session = "upbit-recovery-session-0001"
        claimed_permit = self.store.claim_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        self.store.bind_permit(
            approval_id=self.approval["approvalId"], session_id=session
        )
        recovery = {
            "schemaVersion": "upbit-functional-recovery-approval/v1",
            "recoveryId": "upbit-recovery-approval-0001",
            "mode": "CLEANUP_ONLY",
            "sessionId": session,
            "permitId": claimed_permit["permitId"],
            "permitHash": claimed_permit["permitHash"],
            "accountFingerprint": ACCOUNT,
            "approvalState": "ACTIVE",
            "serverManaged": True,
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "singleUse": True,
            "previousOwnerLost": True,
            "previousOwnerLeaseExpired": True,
            "officialRestReconciled": True,
            "officialRestTruthHash": "1" * 64,
            "previousOwnerLeaseEvidenceHash": "2" * 64,
            "previousWriterGeneration": 1,
            "nextWriterGeneration": 2,
            "observedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
            "serverSignature": "verified-by-backend",
        }
        recovery["contentHash"] = _stable_hash(recovery)
        pointer = self.store.approve_recovery(recovery)
        claimed = self.store.claim_recovery(
            recovery_id=pointer["recoveryId"], session_id=session
        )
        self.assertEqual(recovery, claimed)
        self.assertEqual(
            recovery,
            self.store.recovery_reader(
                pointer["recoveryId"], pointer["recoveryHash"]
            ),
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "not-claimable"
        ):
            self.store.claim_recovery(
                recovery_id=pointer["recoveryId"], session_id=session
            )
        self.store.finish_recovery(
            recovery_id=pointer["recoveryId"],
            state="CONSUMED",
            detail="done",
        )
        stale = {
            **recovery,
            "recoveryId": "upbit-recovery-approval-0002",
            "officialRestTruthHash": "3" * 64,
        }
        stale.pop("contentHash", None)
        stale["contentHash"] = _stable_hash(stale)
        stale_pointer = self.store.approve_recovery(stale)
        self.fake.now += timedelta(seconds=16)
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "approval-stale"):
            self.store.claim_recovery(
                recovery_id=stale_pointer["recoveryId"], session_id=session
            )
        self.assertIsNone(self.store.recovery_authority_pointer())

    def test_actual_state_signer_is_canonical_hmac_and_rejects_tamper(
        self,
    ) -> None:
        body = {
            "schemaVersion": "upbit-functional-recovery-candidate/v1",
            "recoveryId": "upbit-hmac-projection-0001",
            "nonce": "hmac-projection-nonce-000000000001",
            "serverManaged": True,
        }
        signed = state._sign_upbit_functional_approval_record(body)
        raw_secret = state._UPBIT_FUNCTIONAL_APPROVAL_RECORD_SECRET
        self.assertEqual(
            "HMAC-SHA256-PROCESS-LOCAL",
            signed["serverSignatureAlgorithm"],
        )
        self.assertRegex(signed["serverSignature"], r"^[0-9a-f]{64}$")
        self.assertNotEqual(raw_secret, signed["serverSignature"])
        self.assertNotIn(raw_secret, json.dumps(signed, sort_keys=True))
        self.assertTrue(
            state._verify_upbit_functional_approval_record(signed)
        )
        self.assertTrue(
            state._verify_upbit_functional_approval_record(
                dict(reversed(tuple(signed.items())))
            )
        )

        signature = signed["serverSignature"]
        changed_signature = (
            ("0" if signature[0] != "0" else "1") + signature[1:]
        )
        tampered_values = {
            "body": {**signed, "nonce": "tampered"},
            "algorithm": {
                **signed,
                "serverSignatureAlgorithm": "HMAC-SHA256",
            },
            "uppercase": {**signed, "serverSignature": "A" * 64},
            "signature": {
                **signed,
                "serverSignature": changed_signature,
            },
            "unexpected-signature-field": {
                **signed,
                "serverSignatureVersion": "v2",
            },
        }
        for label, tampered in tampered_values.items():
            with self.subTest(label=label):
                self.assertFalse(
                    state._verify_upbit_functional_approval_record(tampered)
                )

        with self.assertRaisesRegex(ValueError, "already signed"):
            state._sign_upbit_functional_approval_record(signed)
        with self.assertRaisesRegex(ValueError, "already signed"):
            state._sign_upbit_functional_approval_record(
                {**body, "serverSignatureVersion": "v2"}
            )
        with patch.object(
            state,
            "_UPBIT_FUNCTIONAL_APPROVAL_RECORD_SECRET",
            "simulated-different-process-secret",
        ):
            self.assertFalse(
                state._verify_upbit_functional_approval_record(signed)
            )

    def test_actual_state_signer_hashes_signed_recovery_envelope(self) -> None:
        path = Path(self.temp.name) / "state-signed-recovery.sqlite3"
        store = DurableUpbitFunctionalApprovalStore(
            path,
            clock=self.fake.clock,
            operator_verifier=state._verify_upbit_functional_approval_record,
        )
        main_body = {
            **self.approval,
            "approvalId": "upbit-state-signed-main-0001",
            "nonce": "state-signed-main-nonce-000000000001",
        }
        main_body.pop("serverSignature")
        main = state._sign_upbit_functional_approval_record(main_body)
        raw_secret = state._UPBIT_FUNCTIONAL_APPROVAL_RECORD_SECRET
        self.assertNotEqual(raw_secret, main["serverSignature"])
        store.approve_permit(self.functional_permit.to_dict(), main)
        session = "upbit-state-signed-recovery-session-0001"
        claimed_permit = store.claim_permit(
            approval_id=main["approvalId"], session_id=session
        )
        store.bind_permit(approval_id=main["approvalId"], session_id=session)
        candidate_body = {
            "schemaVersion": "upbit-functional-recovery-candidate/v1",
            "recoveryId": "upbit-state-signed-recovery-0001",
            "mode": "CLEANUP_ONLY",
            "sessionId": session,
            "permitId": claimed_permit["permitId"],
            "permitHash": claimed_permit["permitHash"],
            "accountFingerprint": ACCOUNT,
            "candidateState": "ISSUED",
            "serverManaged": True,
            "operatorAuthenticated": False,
            "operatorApproved": False,
            "singleUse": True,
            "previousOwnerLost": True,
            "previousOwnerLeaseExpired": True,
            "previousOwnerLeaseEvidenceHash": "2" * 64,
            "previousWriterGeneration": 1,
            "nextWriterGeneration": 2,
            "issuedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
            "expiresAt": (self.fake.now + timedelta(minutes=5)).isoformat().replace(
                "+00:00", "Z"
            ),
        }
        candidate = state._sign_upbit_functional_approval_record(candidate_body)
        candidate["candidateHash"] = _stable_hash(candidate)
        candidate_outer_tamper = {
            **candidate,
            "candidateHash": "0" * 64,
        }
        self.assertTrue(
            state._verify_upbit_functional_approval_record(
                candidate_outer_tamper
            )
        )
        restarted_store = DurableUpbitFunctionalApprovalStore(
            path,
            clock=self.fake.clock,
            operator_verifier=state._verify_upbit_functional_approval_record,
        )
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "candidate-invalid"):
            restarted_store.issue_recovery_candidate(candidate_outer_tamper)
        with patch.object(
            state,
            "_UPBIT_FUNCTIONAL_APPROVAL_RECORD_SECRET",
            "simulated-different-process-secret",
        ):
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "candidate-invalid"
            ):
                restarted_store.issue_recovery_candidate(candidate)
        restarted_store.issue_recovery_candidate(candidate)
        with closing(sqlite3.connect(path)) as connection:
            persisted_candidate = connection.execute(
                """SELECT recovery_json FROM
                upbit_functional_recovery_approvals WHERE recovery_id=?""",
                (candidate["recoveryId"],),
            ).fetchone()[0]
        self.assertNotIn(raw_secret, persisted_candidate)
        self.assertIn(candidate["serverSignature"], persisted_candidate)
        approval_body = {
            "schemaVersion": "upbit-functional-recovery-approval/v1",
            "recoveryId": candidate["recoveryId"],
            "mode": "CLEANUP_ONLY",
            "sessionId": session,
            "permitId": claimed_permit["permitId"],
            "permitHash": claimed_permit["permitHash"],
            "accountFingerprint": ACCOUNT,
            "approvalState": "ACTIVE",
            "serverManaged": True,
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "singleUse": True,
            "previousOwnerLost": True,
            "previousOwnerLeaseExpired": True,
            "officialRestReconciled": True,
            "officialRestTruthHash": "1" * 64,
            "previousOwnerLeaseEvidenceHash": "2" * 64,
            "previousWriterGeneration": 1,
            "nextWriterGeneration": 2,
            "observedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
            "externalActivityScope": "KRW-BTC_TARGET_MARKET_ONLY",
            "officialRestRecoveryOnly": True,
        }
        approval = state._sign_upbit_functional_approval_record(approval_body)
        approval["contentHash"] = _stable_hash(approval)
        approval_outer_tamper = {
            **approval,
            "contentHash": "0" * 64,
        }
        self.assertTrue(
            state._verify_upbit_functional_approval_record(
                approval_outer_tamper
            )
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "candidate-approval-invalid"
        ):
            restarted_store.approve_issued_recovery(
                recovery_id=candidate["recoveryId"],
                record=approval_outer_tamper,
            )
        pointer = restarted_store.approve_issued_recovery(
            recovery_id=candidate["recoveryId"], record=approval
        )
        claimed = restarted_store.claim_recovery(
            recovery_id=candidate["recoveryId"], session_id=session
        )
        self.assertEqual(pointer["recoveryHash"], claimed["contentHash"])
        with closing(sqlite3.connect(path)) as connection:
            persisted_approval = connection.execute(
                """SELECT recovery_json FROM
                upbit_functional_recovery_approvals WHERE recovery_id=?""",
                (candidate["recoveryId"],),
            ).fetchone()[0]
            durable_dump = "\n".join(connection.iterdump())
        self.assertNotIn(raw_secret, persisted_approval)
        self.assertNotIn(raw_secret, durable_dump)
        self.assertIn(approval["serverSignature"], persisted_approval)


if __name__ == "__main__":
    unittest.main()
