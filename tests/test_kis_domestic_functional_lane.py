from __future__ import annotations

import json
import hashlib
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_contract import (
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    LIVE_ORIGIN,
    PDNO,
    ROUTE,
)
from live_trader.kis_domestic_functional_lane import (
    DurableKisDomesticFunctionalLane,
    KisDomesticFunctionalLaneBlocked,
    production_entrypoint_status,
    sign_kis_domestic_lane_capture,
    sign_kis_domestic_lane_grant_receipt,
)
from live_trader.program_ledger import ProgramLedger


UTC = timezone.utc
KEY = b"k" * 32
SHA = {
    "account": "1" * 64,
    "permit": "2" * 64,
    "nonce": "3" * 64,
    "baseline": "4" * 64,
    "contract": "5" * 64,
    "code": "6" * 64,
    "confirmation": "7" * 64,
}


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class KisDomesticFunctionalLaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = Path(self.temp.name) / "program-ledger.sqlite3"
        self.ledger = ProgramLedger(self.db_path)
        self.boundary = datetime(2026, 8, 14, 4, 15, tzinfo=UTC)
        self.clock = MutableClock(self.boundary - timedelta(seconds=2))
        self.lane = DurableKisDomesticFunctionalLane(
            program_ledger=self.ledger,
            server_authority_key=KEY,
            server_authority_key_id="test-kis-lane-key-v1",
            clock=self.clock,
        )

    def _arm(self, *, intent: str | None = None) -> dict:
        return self.lane.arm_public_wait(
            account_fingerprint=SHA["account"],
            permit_id="permit-kis-one-use",
            permit_hash=SHA["permit"],
            session_nonce_hash=SHA["nonce"],
            preactivation_baseline_hash=SHA["baseline"],
            operator_id="operator@example.test",
            operator_confirmation_hash=intent or SHA["confirmation"],
            contract_envelope_hash=SHA["contract"],
            code_manifest_hash=SHA["code"],
        )

    def _issue_and_approve(self) -> tuple[dict, dict]:
        arm = self._arm()
        evaluation, trigger = self._evaluation_and_trigger(arm)
        issued = self.lane.issue_bootstrap(
            public_arm_id=arm["body"]["armId"],
            evaluation_id=evaluation["body"]["evaluationId"],
            trigger_id=trigger["body"]["triggerId"],
            account_fingerprint=SHA["account"],
            permit_id="permit-kis-one-use",
            permit_hash=SHA["permit"],
            session_nonce_hash=SHA["nonce"],
            preactivation_baseline_hash=SHA["baseline"],
            contract_envelope_hash=SHA["contract"],
            code_manifest_hash=SHA["code"],
        )
        approved = self.lane.approve_bootstrap(
            bootstrap_id=issued["body"]["bootstrapId"],
            bootstrap_hash=issued["recordHash"],
            operator_id="operator@example.test",
            operator_confirmation_hash=SHA["confirmation"],
        )
        return issued, approved

    def _window(self, *, breakout: bool = True, generation: str = "8") -> dict:
        bars: list[dict[str, str]] = []
        first_open = self.boundary - timedelta(minutes=55)
        for index in range(11):
            opened = first_open + timedelta(minutes=5 * index)
            closed = opened + timedelta(minutes=5)
            current = index == 10
            bars.append(
                {
                    "openAt": opened.isoformat().replace("+00:00", "Z"),
                    "closeAt": closed.isoformat().replace("+00:00", "Z"),
                    "open": "100",
                    "high": "104" if current and breakout else ("102" if current else "105"),
                    "low": "99" if current else "95",
                    "close": "102" if current else "100",
                    "sourceSequenceStart": str(index * 2 + 1),
                    "sourceSequenceEnd": str(index * 2 + 2),
                    "eventCount": 2,
                    "rawEventChainHash": format(index + 1, "064x"),
                }
            )
        source_proof = {
            "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sourceProvider": "kis",
            "sourceGeneration": "kis-ws-generation-" + generation * 32,
            "firstSourceSequence": "1",
            "lastSourceSequence": "22",
            "sourceEventCount": 22,
            "barRawEventChainHashes": [
                bar["rawEventChainHash"] for bar in bars
            ],
        }
        return {
            "schemaVersion": "kis-domestic-official-5m-window/v1",
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "source": "KIS_WEBSOCKET_H0STCNT0",
            "interval": "5m",
            "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
            "sourceProvider": "kis",
            "sourceGeneration": "kis-ws-generation-" + generation * 32,
            "firstSourceSequence": "1",
            "lastSourceSequence": "22",
            "sourceEventCount": 22,
            "sourceProofHash": self._hash(source_proof),
            "bars": bars,
            "observedAt": self.boundary.isoformat().replace("+00:00", "Z"),
        }

    @staticmethod
    def _hash(value: object) -> str:
        import hashlib

        return hashlib.sha256(
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _evaluation_and_trigger(
        self, arm: dict, *, generation: str = "8"
    ) -> tuple[dict, dict]:
        self.clock.value = self.boundary
        window = self._window(generation=generation)
        evaluation = self.lane.record_breakout_evaluation(
            public_arm_id=arm["body"]["armId"],
            window_body=window,
            server_authority_signature=sign_kis_domestic_lane_capture(
                KEY, "BAR_WINDOW", window
            ),
        )
        trigger_body = {
            "schemaVersion": "kis-domestic-next-open-trigger/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "source": "KIS_WEBSOCKET",
            "eventType": "NEXT_BAR_OPEN",
            "evaluationId": evaluation["body"]["evaluationId"],
            "barOpenAt": self.boundary.isoformat().replace("+00:00", "Z"),
            "observedAt": self.boundary.isoformat().replace("+00:00", "Z"),
            "openPriceKrw": "100",
            "sourceProvider": "kis",
            "sourceGeneration": "kis-ws-generation-" + generation * 32,
            "sourceSequence": "23",
            "rawEventHash": "9" * 64,
        }
        trigger_body["sourceProofHash"] = self._hash(
            {
                "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "sourceProvider": "kis",
                "sourceGeneration": trigger_body["sourceGeneration"],
                "sourceSequence": trigger_body["sourceSequence"],
                "rawEventHash": trigger_body["rawEventHash"],
                "barOpenAt": trigger_body["barOpenAt"],
                "observedAt": trigger_body["observedAt"],
            }
        )
        trigger = self.lane.record_next_open_trigger(
            evaluation_id=evaluation["body"]["evaluationId"],
            trigger_body=trigger_body,
            server_authority_signature=sign_kis_domestic_lane_capture(
                KEY, "NEXT_OPEN", trigger_body
            ),
        )
        return evaluation, trigger

    def _activate(self) -> dict:
        issued, approved = self._issue_and_approve()
        session_id = "kis-session-" + "a" * 32
        return self.lane.activate(
            bootstrap_id=issued["body"]["bootstrapId"],
            approval_id=approved["body"]["approvalId"],
            evaluation_id=issued["body"]["evaluationId"],
            trigger_id=issued["body"]["triggerId"],
            session_id=session_id,
            fresh_quote_hash="a" * 64,
            fresh_quote_observed_at=self.boundary.isoformat().replace("+00:00", "Z"),
            fresh_quote_price_krw="100",
            natural_buy_limit_price_krw="100",
            graph_grant_instant_receipt=self._grant_receipt(
                issued, approved, session_id=session_id
            ),
        )

    def _grant_receipt(
        self,
        issued: dict,
        approved: dict,
        *,
        session_id: str,
        grant_wall: datetime | None = None,
        fresh_quote_hash: str = "a" * 64,
        overrides: dict | None = None,
    ) -> dict:
        with self.ledger.connection() as conn:
            trigger_hash = str(
                conn.execute(
                    "SELECT record_hash FROM kis_functional_next_open WHERE trigger_id=?",
                    (issued["body"]["triggerId"],),
                ).fetchone()[0]
            )
        body = {
            "schemaVersion": "kis-domestic-functional-lane-grant-instant/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "source": "KIS_DOMESTIC_FUNCTIONAL_GRAPH_V2",
            "graphTransactionId": "kis-graph-tx-" + "1" * 32,
            "graphRequestHash": "a" * 64,
            "graphActionInputsHash": "b" * 64,
            "graphIntentStepHash": "c" * 64,
            "expectedStatusRevision": 7,
            "expectedStatusHeadHash": "d" * 64,
            "ownerEpochHash": "e" * 64,
            "registryAcceptedHeadHash": "f" * 64,
            "sessionId": session_id,
            "bootstrapId": issued["body"]["bootstrapId"],
            "approvalId": approved["body"]["approvalId"],
            "evaluationId": issued["body"]["evaluationId"],
            "triggerId": issued["body"]["triggerId"],
            "triggerHash": trigger_hash,
            "accountFingerprint": SHA["account"],
            "preactivationBaselineHash": SHA["baseline"],
            "codeManifestHash": SHA["code"],
            "rollingReceiptHash": "1" * 64,
            "quoteReceiptHash": "2" * 64,
            "freshQuoteHash": fresh_quote_hash,
            "grantWallAt": (grant_wall or self.clock.value).isoformat().replace(
                "+00:00", "Z"
            ),
            "grantMonotonicNs": 9876543210,
            "capturedOnce": True,
            "serverAuthorityKeyIdHash": hashlib.sha256(
                b"test-kis-lane-key-v1"
            ).hexdigest(),
        }
        if overrides:
            body.update(overrides)
        return sign_kis_domestic_lane_grant_receipt(KEY, body)

    def _fill(self, claim: dict, *, side: str, price: str = "100") -> None:
        claim_id = claim["body"]["claimId"]
        step = self.lane.transition_action(
            claim_id=claim_id, expected_revision=1, target_state="SUBMITTING"
        )
        step = self.lane.transition_action(
            claim_id=claim_id,
            expected_revision=step["revision"],
            target_state="POST_MAY_HAVE_CROSSED",
        )
        step = self.lane.transition_action(
            claim_id=claim_id,
            expected_revision=step["revision"],
            target_state="ACKNOWLEDGED",
            broker_order_id="0000012345" if side == "BUY" else "0000012346",
            org_no="00123",
            order_date="20260814",
        )
        self.lane.transition_action(
            claim_id=claim_id,
            expected_revision=step["revision"],
            target_state="FILLED",
            fill_price_krw=price,
            fee_krw="0",
            tax_krw="0",
            loan_interest_krw="0",
        )

    def _natural_buy(self, session_id: str) -> dict:
        with self.ledger.connection() as conn:
            row = conn.execute(
                """SELECT claim_id, record_hash FROM kis_functional_action
                   WHERE session_id=? AND action_kind='NATURAL_BUY'""",
                (session_id,),
            ).fetchone()
        return {
            "body": {"claimId": str(row["claim_id"])},
            "recordHash": str(row["record_hash"]),
        }

    def test_availability_is_false_and_there_is_no_sender_surface(self) -> None:
        status = production_entrypoint_status()
        self.assertFalse(status["available"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["mutationAvailable"])
        self.assertFalse(hasattr(self.lane, "send"))
        self.assertFalse(hasattr(self.lane, "post"))
        self.assertFalse(hasattr(self.lane, "place_order"))
        self.assertFalse(status["atomicTriggerActivationAvailable"])
        self.assertFalse(status["trustedMonotonicHeartbeatAvailable"])
        self.assertTrue(status["nativeLaneGrantInstantAvailable"])
        self.assertFalse(status["graphGrantReceiptProductionAuthorityAvailable"])
        self.assertFalse(status["activationBackdatedToBarOpen"])
        self.assertTrue(status["activationRelative7200ProductionReady"])
        self.assertFalse(status["officialTerminalTruthAvailable"])

    def test_dirty_compatible_bootstrap_table_without_route_primary_key_is_rejected(self) -> None:
        dirty_path = Path(self.temp.name) / "dirty-program-ledger.sqlite3"
        dirty_ledger = ProgramLedger(dirty_path)
        with dirty_ledger.connection() as conn:
            # Same column names as the authoritative table, but deliberately
            # omit every PK/UNIQUE constraint.  This is the historical shape
            # that allowed two route-global bootstrap rows to be issued.
            conn.execute(
                """CREATE TABLE kis_functional_bootstrap (
                    route TEXT NOT NULL,
                    bootstrap_id TEXT NOT NULL,
                    public_arm_id TEXT NOT NULL,
                    evaluation_id TEXT NOT NULL,
                    trigger_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    signature TEXT NOT NULL,
                    preactivation_baseline_hash TEXT NOT NULL,
                    approval_id TEXT NOT NULL DEFAULT '',
                    session_id TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL
                )"""
            )

        for _ in range(2):
            with self.assertRaisesRegex(
                KisDomesticFunctionalLaneBlocked, "schema fingerprint mismatch"
            ):
                DurableKisDomesticFunctionalLane(
                    program_ledger=ProgramLedger(dirty_path),
                    server_authority_key=KEY,
                    server_authority_key_id="test-kis-lane-key-v1",
                    clock=self.clock,
                )

        conn = sqlite3.connect(dirty_path)
        try:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM kis_functional_bootstrap"
                ).fetchone()[0],
            )
            # The failed transaction must not partially install a seemingly
            # usable remainder of the lane schema around the hostile table.
            lane_tables = {
                str(row[0])
                for row in conn.execute(
                    """SELECT name FROM sqlite_master
                       WHERE type='table' AND name LIKE 'kis_functional_%'"""
                ).fetchall()
            }
        finally:
            conn.close()
        self.assertEqual({"kis_functional_bootstrap"}, lane_tables)

    def test_public_hold_or_expiry_never_issues_or_burns_bootstrap(self) -> None:
        arm = self._arm()
        self.clock.value = self.boundary
        hold = self._window(breakout=False)
        evaluation = self.lane.record_breakout_evaluation(
            public_arm_id=arm["body"]["armId"],
            window_body=hold,
            server_authority_signature=sign_kis_domestic_lane_capture(
                KEY, "BAR_WINDOW", hold
            ),
        )
        self.assertEqual("HOLD", evaluation["body"]["signal"])
        with self.ledger.connection() as conn:
            self.assertEqual(
                0, conn.execute("SELECT COUNT(*) FROM kis_functional_bootstrap").fetchone()[0]
            )
        self.clock.value = datetime.fromisoformat(
            arm["body"]["expiresAt"].replace("Z", "+00:00")
        )
        expired = self.lane.expire_public_wait(
            arm_id=arm["body"]["armId"], expected_revision=1
        )
        self.assertFalse(expired["bootstrapEverIssued"])
        self.assertFalse(expired["orderAuthorityEverAvailable"])

    def test_stale_quote_and_late_next_open_dispatch_are_blocked(self) -> None:
        issued, approved = self._issue_and_approve()
        session_id = "kis-session-" + "c" * 32
        self.clock.value = self.boundary + timedelta(seconds=6)
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "fresh quote"):
            self.lane.activate(
                bootstrap_id=issued["body"]["bootstrapId"],
                approval_id=approved["body"]["approvalId"],
                evaluation_id=issued["body"]["evaluationId"],
                trigger_id=issued["body"]["triggerId"],
                session_id=session_id,
                fresh_quote_hash="a" * 64,
                fresh_quote_observed_at=self.boundary.isoformat().replace("+00:00", "Z"),
                fresh_quote_price_krw="100",
                natural_buy_limit_price_krw="100",
                graph_grant_instant_receipt=self._grant_receipt(
                    issued,
                    approved,
                    session_id=session_id,
                    grant_wall=self.clock.value,
                ),
            )
        self.clock.value = self.boundary
        activation = self.lane.activate(
            bootstrap_id=issued["body"]["bootstrapId"],
            approval_id=approved["body"]["approvalId"],
            evaluation_id=issued["body"]["evaluationId"],
            trigger_id=issued["body"]["triggerId"],
            session_id=session_id,
            fresh_quote_hash="a" * 64,
            fresh_quote_observed_at=self.boundary.isoformat().replace("+00:00", "Z"),
            fresh_quote_price_krw="100",
            natural_buy_limit_price_krw="100",
            graph_grant_instant_receipt=self._grant_receipt(
                issued, approved, session_id=session_id
            ),
        )
        buy = self._natural_buy(activation["sessionId"])
        self.clock.value = self.boundary + timedelta(seconds=2, microseconds=1)
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "next-open"):
            self.lane.transition_action(
                claim_id=buy["body"]["claimId"],
                expected_revision=1,
                target_state="SUBMITTING",
            )

    def test_route_global_bootstrap_is_ever_one_use_across_restart(self) -> None:
        issued, _ = self._issue_and_approve()
        restarted = DurableKisDomesticFunctionalLane(
            program_ledger=ProgramLedger(self.db_path),
            server_authority_key=KEY,
            server_authority_key_id="test-kis-lane-key-v1",
            clock=self.clock,
        )
        second_arm = self._arm()
        second_evaluation, second_trigger = self._evaluation_and_trigger(
            second_arm, generation="a"
        )
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "already issued"):
            restarted.issue_bootstrap(
                public_arm_id=second_arm["body"]["armId"],
                evaluation_id=second_evaluation["body"]["evaluationId"],
                trigger_id=second_trigger["body"]["triggerId"],
                account_fingerprint=SHA["account"],
                permit_id="permit-kis-one-use",
                permit_hash=SHA["permit"],
                session_nonce_hash=SHA["nonce"],
                preactivation_baseline_hash=SHA["baseline"],
                contract_envelope_hash=SHA["contract"],
                code_manifest_hash=SHA["code"],
            )
        self.assertEqual(issued["body"]["bootstrapId"], restarted.status()["bootstrap"]["bootstrap_id"])

    def test_raw_breakout_requires_signature_exact_contiguous_bars_and_buy(self) -> None:
        arm = self._arm()
        self.clock.value = self.boundary
        window = self._window()
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "signature"):
            self.lane.record_breakout_evaluation(
                public_arm_id=arm["body"]["armId"],
                window_body=window,
                server_authority_signature="0" * 64,
            )
        gapped = self._window()
        gapped["bars"][1]["openAt"] = (
            self.boundary - timedelta(minutes=49)
        ).isoformat().replace("+00:00", "Z")
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "duration"):
            self.lane.record_breakout_evaluation(
                public_arm_id=arm["body"]["armId"],
                window_body=gapped,
                server_authority_signature=sign_kis_domestic_lane_capture(
                    KEY, "BAR_WINDOW", gapped
                ),
            )
        hold = self._window(breakout=False)
        evaluation = self.lane.record_breakout_evaluation(
            public_arm_id=arm["body"]["armId"],
            window_body=hold,
            server_authority_signature=sign_kis_domestic_lane_capture(
                KEY, "BAR_WINDOW", hold
            ),
        )
        self.assertEqual("HOLD", evaluation["body"]["signal"])
        trigger = {
            "schemaVersion": "kis-domestic-next-open-trigger/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "source": "KIS_WEBSOCKET",
            "eventType": "NEXT_BAR_OPEN",
            "evaluationId": evaluation["body"]["evaluationId"],
            "barOpenAt": self.boundary.isoformat().replace("+00:00", "Z"),
            "observedAt": self.boundary.isoformat().replace("+00:00", "Z"),
            "openPriceKrw": "100",
            "sourceProvider": "kis",
            "sourceGeneration": "kis-ws-generation-" + "8" * 32,
            "sourceSequence": "23",
            "rawEventHash": "9" * 64,
        }
        trigger["sourceProofHash"] = self._hash(
            {
                "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "sourceProvider": "kis",
                "sourceGeneration": trigger["sourceGeneration"],
                "sourceSequence": trigger["sourceSequence"],
                "rawEventHash": trigger["rawEventHash"],
                "barOpenAt": trigger["barOpenAt"],
                "observedAt": trigger["observedAt"],
            }
        )
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "not the evaluation"):
            self.lane.record_next_open_trigger(
                evaluation_id=evaluation["body"]["evaluationId"],
                trigger_body=trigger,
                server_authority_signature=sign_kis_domestic_lane_capture(
                    KEY, "NEXT_OPEN", trigger
                ),
            )

    def test_activation_reseals_exact_7200_and_consumes_inputs(self) -> None:
        activated = self._activate()
        self.assertEqual(7200, activated["activeSeconds"])
        self.assertEqual("2026-08-14T04:15:00Z", activated["activatedAt"])
        self.assertEqual("2026-08-14T06:15:00Z", activated["expiresAt"])
        self.assertFalse(activated["realOrdersEnabled"])
        with self.assertRaisesRegex(
            KisDomesticFunctionalLaneBlocked, "legacy backdated activation"
        ):
            self.lane.activate(
                bootstrap_id=self.lane.status()["bootstrap"]["bootstrap_id"],
                approval_id="missing",
                evaluation_id="missing",
                trigger_id="missing",
                session_id="kis-session-" + "b" * 32,
                fresh_quote_hash="a" * 64,
                fresh_quote_observed_at=self.boundary.isoformat().replace("+00:00", "Z"),
                fresh_quote_price_krw="100",
                natural_buy_limit_price_krw="100",
            )
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT * FROM kis_functional_session").fetchone()
            activation = json.loads(row["activation_record_json"])
        self.assertEqual(SHA["baseline"], activation["preactivationBaselineHash"])
        self.assertEqual(SHA["code"], activation["codeManifestHash"])
        self.assertEqual(1, activation["quantity"])

    def test_grant_plus_1999ms_gets_full_7200_and_expiry_is_exclusive(self) -> None:
        self.boundary = datetime(2026, 8, 14, 4, 10, tzinfo=UTC)
        self.clock.value = self.boundary - timedelta(seconds=2)
        issued, approved = self._issue_and_approve()
        session_id = "kis-session-" + "1" * 32
        grant_wall = self.boundary + timedelta(seconds=1, milliseconds=999)
        self.clock.value = grant_wall
        activated = self.lane.activate(
            bootstrap_id=issued["body"]["bootstrapId"],
            approval_id=approved["body"]["approvalId"],
            evaluation_id=issued["body"]["evaluationId"],
            trigger_id=issued["body"]["triggerId"],
            session_id=session_id,
            fresh_quote_hash="a" * 64,
            fresh_quote_observed_at=self.boundary.isoformat().replace(
                "+00:00", "Z"
            ),
            fresh_quote_price_krw="100",
            natural_buy_limit_price_krw="100",
            graph_grant_instant_receipt=self._grant_receipt(
                issued,
                approved,
                session_id=session_id,
                grant_wall=grant_wall,
            ),
        )
        expected_expiry = grant_wall + timedelta(seconds=7200)
        self.assertEqual(_iso(grant_wall), activated["activatedAt"])
        self.assertEqual(_iso(expected_expiry), activated["expiresAt"])
        self.assertNotEqual(_iso(self.boundary), activated["activatedAt"])
        self.assertEqual(9876543210, activated["grantMonotonicNs"])
        buy = self._natural_buy(session_id)
        self.clock.value = self.boundary + timedelta(seconds=2, microseconds=1)
        with self.assertRaisesRegex(
            KisDomesticFunctionalLaneBlocked, "sealed trigger boundary"
        ):
            self.lane.transition_action(
                claim_id=buy["body"]["claimId"],
                expected_revision=1,
                target_state="SUBMITTING",
            )
        self.lane.transition_action(
            claim_id=buy["body"]["claimId"],
            expected_revision=1,
            target_state="NOT_SENT",
            not_sent_reason="OFFLINE_POST_ZERO",
        )
        cleanup = self.lane.begin_cleanup(
            session_id=session_id,
            expected_revision=1,
            reason="OFFLINE_GRANT_DURATION_TEST",
        )
        self.clock.value = self.boundary + timedelta(seconds=7200)
        with self.assertRaisesRegex(
            KisDomesticFunctionalLaneBlocked, "window has not elapsed"
        ):
            self.lane.finalize(
                session_id=session_id, expected_revision=cleanup["revision"]
            )
        self.clock.value = expected_expiry
        evidence = self.lane.finalize(
            session_id=session_id, expected_revision=cleanup["revision"]
        )
        self.assertEqual(_iso(grant_wall), evidence["activatedAt"])
        self.assertEqual(_iso(expected_expiry), evidence["activeEndsAt"])

    def test_grant_receipt_clock_rollback_signature_and_next_open_fail_closed(self) -> None:
        issued, approved = self._issue_and_approve()
        session_id = "kis-session-" + "2" * 32
        future_grant = self.boundary + timedelta(milliseconds=1)
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "rolled back"):
            self.lane.activate(
                bootstrap_id=issued["body"]["bootstrapId"],
                approval_id=approved["body"]["approvalId"],
                evaluation_id=issued["body"]["evaluationId"],
                trigger_id=issued["body"]["triggerId"],
                session_id=session_id,
                fresh_quote_hash="a" * 64,
                fresh_quote_observed_at=_iso(self.boundary),
                fresh_quote_price_krw="100",
                natural_buy_limit_price_krw="100",
                graph_grant_instant_receipt=self._grant_receipt(
                    issued,
                    approved,
                    session_id=session_id,
                    grant_wall=future_grant,
                ),
            )
        tampered = self._grant_receipt(
            issued, approved, session_id=session_id
        )
        tampered["signature"] = "0" * 64
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "signature"):
            self.lane.activate(
                bootstrap_id=issued["body"]["bootstrapId"],
                approval_id=approved["body"]["approvalId"],
                evaluation_id=issued["body"]["evaluationId"],
                trigger_id=issued["body"]["triggerId"],
                session_id=session_id,
                fresh_quote_hash="a" * 64,
                fresh_quote_observed_at=_iso(self.boundary),
                fresh_quote_price_krw="100",
                natural_buy_limit_price_krw="100",
                graph_grant_instant_receipt=tampered,
            )
        late_grant = self.boundary + timedelta(seconds=2, microseconds=1)
        self.clock.value = late_grant
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "next-open"):
            self.lane.activate(
                bootstrap_id=issued["body"]["bootstrapId"],
                approval_id=approved["body"]["approvalId"],
                evaluation_id=issued["body"]["evaluationId"],
                trigger_id=issued["body"]["triggerId"],
                session_id=session_id,
                fresh_quote_hash="a" * 64,
                fresh_quote_observed_at=_iso(self.boundary),
                fresh_quote_price_krw="100",
                natural_buy_limit_price_krw="100",
                graph_grant_instant_receipt=self._grant_receipt(
                    issued,
                    approved,
                    session_id=session_id,
                    grant_wall=late_grant,
                ),
            )

    def test_v1_manifest_and_legacy_backdated_activate_api_are_migration_hold(self) -> None:
        issued, approved = self._issue_and_approve()
        with self.assertRaisesRegex(
            KisDomesticFunctionalLaneBlocked, "legacy backdated activation"
        ):
            self.lane.activate(
                bootstrap_id=issued["body"]["bootstrapId"],
                approval_id=approved["body"]["approvalId"],
                evaluation_id=issued["body"]["evaluationId"],
                trigger_id=issued["body"]["triggerId"],
                session_id="kis-session-" + "3" * 32,
                fresh_quote_hash="a" * 64,
                fresh_quote_observed_at=_iso(self.boundary),
                fresh_quote_price_krw="100",
                natural_buy_limit_price_krw="100",
            )
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_schema_manifest SET schema_version=?",
                ("kis-domestic-functional-lane-schema/v1",),
            )
        with self.assertRaisesRegex(
            KisDomesticFunctionalLaneBlocked, "schema version/migration manifest"
        ):
            DurableKisDomesticFunctionalLane(
                program_ledger=ProgramLedger(self.db_path),
                server_authority_key=KEY,
                server_authority_key_id="test-kis-lane-key-v1",
                clock=self.clock,
            )

    def test_natural_entry_expiry_is_exclusive_and_activation_schema_is_exact(self) -> None:
        activation = self._activate()
        buy = self._natural_buy(activation["sessionId"])
        self.clock.value = datetime.fromisoformat(
            activation["expiresAt"].replace("Z", "+00:00")
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalLaneBlocked, "outside ACTIVE authority"
        ):
            self.lane.transition_action(
                claim_id=buy["body"]["claimId"],
                expected_revision=1,
                target_state="SUBMITTING",
            )

        with self.ledger.connection() as conn:
            row = conn.execute(
                "SELECT activation_record_json FROM kis_functional_session WHERE session_id=?",
                (activation["sessionId"],),
            ).fetchone()
            record = json.loads(str(row[0]))
            record["legacyBackdatedAt"] = record["triggerBarOpenAt"]
            conn.execute(
                "UPDATE kis_functional_session SET activation_record_json=? WHERE session_id=?",
                (
                    json.dumps(record, sort_keys=True, separators=(",", ":")),
                    activation["sessionId"],
                ),
            )
        with self.assertRaisesRegex(
            KisDomesticFunctionalLaneBlocked, "activation schema is not exact"
        ):
            self.lane.begin_cleanup(
                session_id=activation["sessionId"],
                expected_revision=1,
                reason="OFFLINE_SCHEMA_TAMPER_TEST",
            )

    def test_action_caps_slots_cas_and_phase_are_fail_closed(self) -> None:
        activation = self._activate()
        session_id = activation["sessionId"]
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "cap"):
            self.lane.claim_action(
                session_id=session_id,
                action_kind="NATURAL_BUY",
                limit_price_krw="100001",
            )
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "CLEANUP"):
            self.lane.claim_action(
                session_id=session_id,
                action_kind="CLEANUP_SELL",
                limit_price_krw="100",
            )
        buy = self._natural_buy(session_id)
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "pre-created"):
            self.lane.claim_action(
                session_id=session_id,
                action_kind="NATURAL_BUY",
                limit_price_krw="100",
            )
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "revision"):
            self.lane.transition_action(
                claim_id=buy["body"]["claimId"],
                expected_revision=9,
                target_state="SUBMITTING",
            )

    def test_filled_buy_requires_exact_owned_cleanup_sell(self) -> None:
        activation = self._activate()
        session_id = activation["sessionId"]
        buy = self._natural_buy(session_id)
        self._fill(buy, side="BUY")
        self.clock.value += timedelta(minutes=1)
        cleanup = self.lane.begin_cleanup(
            session_id=session_id, expected_revision=1, reason="OBSERVATION_END"
        )
        self.clock.value = self.boundary + timedelta(seconds=7200)
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "cleanup-sold"):
            self.lane.finalize(
                session_id=session_id, expected_revision=cleanup["revision"]
            )

    def test_full_program_ledger_flow_is_safe_incomplete_and_nonpromoting(self) -> None:
        activation = self._activate()
        session_id = activation["sessionId"]
        buy = self._natural_buy(session_id)
        self._fill(buy, side="BUY")
        self.clock.value += timedelta(minutes=1)
        cleanup = self.lane.begin_cleanup(
            session_id=session_id, expected_revision=1, reason="STOP_OR_EXPIRY"
        )
        sell = self.lane.claim_action(
            session_id=session_id,
            action_kind="CLEANUP_SELL",
            limit_price_krw="99",
        )
        self._fill(sell, side="SELL", price="100")
        self.clock.value = self.boundary + timedelta(seconds=7200)
        evidence = self.lane.finalize(
            session_id=session_id, expected_revision=cleanup["revision"]
        )
        self.assertEqual(
            "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT", evidence["terminalOutcome"]
        )
        self.assertFalse(evidence["promotionEligible"])
        self.assertFalse(evidence["realE2EPromotionReleased"])
        self.assertFalse(evidence["officialTerminalAccountTruthAvailable"])
        self.assertEqual("0", evidence["ownerLossKrw"])
        self.assertEqual(2, len(evidence["programExecutionEventIds"]))
        with self.ledger.connection() as conn:
            self.assertEqual(
                2, conn.execute("SELECT COUNT(*) FROM execution_events").fetchone()[0]
            )
            self.assertEqual(
                10,
                conn.execute(
                    "SELECT COUNT(*) FROM kis_functional_action_transition"
                ).fetchone()[0],
            )
            self.assertEqual(
                "CONSUMED",
                conn.execute("SELECT state FROM kis_functional_bootstrap").fetchone()[0],
            )

    def test_action_transition_chain_tamper_blocks_terminal(self) -> None:
        activation = self._activate()
        session_id = activation["sessionId"]
        buy = self._natural_buy(session_id)
        self.lane.transition_action(
            claim_id=buy["body"]["claimId"],
            expected_revision=1,
            target_state="NOT_SENT",
            not_sent_reason="MOCK_POST_DISABLED",
        )
        self.clock.value += timedelta(minutes=1)
        cleanup = self.lane.begin_cleanup(
            session_id=session_id, expected_revision=1, reason="NO_MUTATION_SURFACE"
        )
        with self.ledger.connection() as conn:
            conn.execute(
                """UPDATE kis_functional_action_transition
                   SET previous_hash=? WHERE claim_id=? AND revision=2""",
                ("f" * 64, buy["body"]["claimId"]),
            )
        self.clock.value = self.boundary + timedelta(seconds=7200)
        with self.assertRaisesRegex(KisDomesticFunctionalLaneBlocked, "hash-chain"):
            self.lane.finalize(
                session_id=session_id, expected_revision=cleanup["revision"]
            )


if __name__ == "__main__":
    unittest.main()
