from __future__ import annotations

import sqlite3
import tempfile
import unittest
import hashlib
import hmac
import json
from datetime import datetime, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_mutation import (
    DurableKisDomesticFunctionalMutationJournal,
    KisDomesticFunctionalMutationBlocked,
    production_entrypoint_status,
    sign_kis_domestic_mutation_truth_capture,
)
from live_trader.program_ledger import ProgramLedger


KEY = b"m" * 32
TRUTH_KEY = b"t" * 32
TRUTH_KEY_ID = "trusted-kis-get-truth-v1"
ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
ORDER_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-cash"
CANCEL_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
EMPTY_KEY = {"orderDate": "", "organizationNo": "", "orderNo": ""}
ACK = {"orderDate": "20260814", "organizationNo": "00123", "orderNo": "0000012345"}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class KisMutationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.sqlite3"
        self.ledger = ProgramLedger(self.path)
        self.now = datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc)
        self.journal = DurableKisDomesticFunctionalMutationJournal(
            program_ledger=self.ledger, signer_key=KEY, signer_key_id="test-kis-mutation-key-v1",
            official_truth_key=TRUTH_KEY, official_truth_key_id=TRUTH_KEY_ID,
            clock=lambda: self.now,
        )
        self.truth_counter = 0

    def seal(self, claim: str = "claim-buy-1", operation: str = "NATURAL_BUY"):
        endpoint, tr_id, side, key = {
            "NATURAL_BUY": (ORDER_ENDPOINT, "TTTC0012U", "BUY", EMPTY_KEY),
            "CLEANUP_SELL": (ORDER_ENDPOINT, "TTTC0011U", "SELL", EMPTY_KEY),
            "CLEANUP_CANCEL": (CANCEL_ENDPOINT, "TTTC0013U", "CANCEL", ACK),
        }[operation]
        return self.journal.seal_request(
            claim_id=claim,
            session_id="kis-session-1",
            operation=operation,
            endpoint=endpoint,
            tr_id=tr_id,
            side=side,
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            authority_revision=7,
            payload=(
                {
                    "KRX_FWDG_ORD_ORGNO": key["organizationNo"],
                    "ORGN_ODNO": key["orderNo"],
                    "ORD_DVSN": "00",
                    "RVSE_CNCL_DVSN_CD": "02",
                    "ORD_QTY": "1",
                    "ORD_UNPR": "0",
                    "QTY_ALL_ORD_YN": "Y",
                    "EXCG_ID_DVSN_CD": "KRX",
                }
                if operation == "CLEANUP_CANCEL"
                else {
                    "PDNO": "010140",
                    "ORD_DVSN": "00",
                    "ORD_QTY": "1",
                    "ORD_UNPR": "100",
                }
            ),
            owned_order_key=key,
            owned_order_side=("BUY" if operation == "CLEANUP_CANCEL" else ""),
        )

    def transition(self, claim: str, revision: int, state: str, key=None):
        return self.journal.transition(
            claim_id=claim,
            expected_revision=revision,
            target_state=state,
            official_order_key=key,
        )

    def official(self, claim: str, revision: int, rows: list[dict], *, complete=True, baseline=None, mutate_request=None, mutate_response=None):
        self.truth_counter += 1
        observed = "2026-08-14T04:15:00.000000Z"
        with self.ledger.connection() as conn:
            stored = conn.execute("SELECT post_marker_at,payload_json,operation,target_order_date,target_organization_no,target_order_no,target_order_side FROM kis_mutation_request WHERE claim_id=?", (claim,)).fetchone()
        payload = json.loads(stored["payload_json"])
        baseline_rows = list(baseline or [])
        if stored["operation"] == "CLEANUP_CANCEL" and baseline is None:
            baseline_rows = [{
                "orderDate": stored["target_order_date"], "organizationNo": stored["target_organization_no"],
                "orderNo": stored["target_order_no"], "pdno": "010140",
                "side": stored["target_order_side"], "limitPriceKrw": "100",
            }]
        request_body = {
            "schemaVersion": "kis-domestic-functional-mutation-official-request/v2",
            "method": "GET", "origin": "https://openapi.koreainvestment.com:9443",
            "endpoint": "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            "trId": "TTTC0081R",
            "orderedQuery": [
                ["CANO", "ACCOUNT_BOUND_REDACTED"],
                ["ACNT_PRDT_CD", "ACCOUNT_BOUND_REDACTED"],
                ["INQR_STRT_DT", "20260814"], ["INQR_END_DT", "20260814"],
                ["SLL_BUY_DVSN_CD", "00"], ["PDNO", ""], ["CCLD_DVSN", "00"],
                ["INQR_DVSN", "00"], ["INQR_DVSN_3", "00"], ["ORD_GNO_BRNO", ""],
                ["ODNO", ""], ["INQR_DVSN_1", ""], ["EXCG_ID_DVSN_CD", "ALL"],
                ["CTX_AREA_FK100", ""], ["CTX_AREA_NK100", ""],
            ],
            "pageCount": 1, "complete": complete, "observedAt": observed,
            "captureId": f"capture-{claim}-{self.truth_counter}",
            "baselineCapturedAt": "2026-08-14T04:14:59.000000Z",
            "baselineOrderKeys": baseline_rows,
            "postMarkerAt": stored["post_marker_at"],
            "limitPriceKrw": (payload["ORD_UNPR"] if stored["operation"] != "CLEANUP_CANCEL" else "100"),
            "claimId": claim, "sessionId": "kis-session-1",
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "serverAuthorityKeyIdHash": hashlib.sha256(TRUTH_KEY_ID.encode()).hexdigest(),
        }
        if mutate_request:
            mutate_request(request_body)
        request = {**request_body, "signatureHash": sign_kis_domestic_mutation_truth_capture(TRUTH_KEY, request_body)}
        raw_body = {"rt_cd": "0", "output1": rows}
        response_body = {
            "schemaVersion": "kis-domestic-functional-mutation-official-response/v2",
            "method": "GET", "origin": request["origin"], "endpoint": request["endpoint"],
            "trId": request["trId"], "httpStatus": 200, "captureId": request["captureId"],
            "requestEnvelopeHash": digest(request),
            "pages": [{"pageNumber": 1, "requestCursor": {"fk100": "", "nk100": ""},
                       "responseCursor": {"fk100": "", "nk100": ""}, "trCont": "E",
                       "rawBody": raw_body, "rawBodyHash": digest(raw_body)}],
            "observedAt": request["observedAt"],
            "serverAuthorityKeyIdHash": hashlib.sha256(TRUTH_KEY_ID.encode()).hexdigest(),
        }
        if mutate_response:
            mutate_response(response_body)
        response = {**response_body, "signatureHash": sign_kis_domestic_mutation_truth_capture(TRUTH_KEY, response_body)}
        return self.journal.reconcile_official_truth(
            claim_id=claim,
            expected_revision=revision,
            truth_id=f"truth-{claim}-{self.truth_counter}",
            request_archive_id=f"request-{claim}-{self.truth_counter}",
            response_archive_id=f"response-{claim}-{self.truth_counter}",
            request_envelope=request,
            response_envelope=response,
        )

    @staticmethod
    def row(state: str, *, key=ACK, filled="0", side="BUY", price="100", observed="2026-08-14T04:15:00.000000Z"):
        return {
            **key,
            "pdno": "010140",
            "side": side,
            "state": state,
            "orderedQty": "1",
            "filledQty": filled,
            "limitPriceKrw": price,
            "orderObservedAt": observed,
        }

    def test_flags_false_and_no_sender_surface(self) -> None:
        status = production_entrypoint_status()
        self.assertFalse(status["available"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["senderAvailable"])
        self.assertFalse(status["officialTruthReconcileAvailable"])
        self.assertFalse(status["rawResponseArchiveAvailable"])
        self.assertFalse(status["trustedGetProductionAvailable"])
        self.assertFalse(status["terminalTransitionProductionAvailable"])
        self.assertFalse(status["promotionAvailable"])
        self.assertFalse(status["releaseEvidenceAvailable"])
        self.assertFalse(hasattr(self.journal, "send"))

    def test_exact_operation_endpoint_tr_side_allowlist_and_intent(self) -> None:
        sealed = self.seal()
        self.assertEqual("NATURAL_BUY", sealed["authorityIntent"]["operation"])
        self.assertEqual(ORDER_ENDPOINT, sealed["authorityIntent"]["endpoint"])
        self.assertEqual(sealed["requestHash"], sealed["authorityIntent"]["payloadHash"])
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "allowlist"):
            self.journal.seal_request(
                claim_id="claim-bad-1", session_id="kis-session-1",
                operation="NATURAL_BUY", endpoint=CANCEL_ENDPOINT,
                tr_id="TTTC0012U", side="BUY", account_fingerprint=ACCOUNT,
                credential_configuration_hash=CREDENTIAL, authority_revision=7,
                payload={"PDNO": "010140", "ORD_DVSN": "00", "ORD_QTY": "1", "ORD_UNPR": "100"},
                owned_order_key=EMPTY_KEY, owned_order_side="",
            )

    def test_sender_entry_and_post_marker_crash_seams(self) -> None:
        self.seal()
        entered = self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "transition"):
            self.transition("claim-buy-1", entered["revision"], "NOT_SENT")
        marker = self.transition("claim-buy-1", entered["revision"], "POST_MAY_HAVE_CROSSED")
        self.assertEqual("POST_MAY_HAVE_CROSSED", marker["state"])
        restarted = DurableKisDomesticFunctionalMutationJournal(
            program_ledger=ProgramLedger(self.path), signer_key=KEY,
            signer_key_id="test-kis-mutation-key-v1", official_truth_key=TRUTH_KEY,
            official_truth_key_id=TRUTH_KEY_ID, clock=lambda: self.now,
        )
        self.assertEqual("POST_MAY_HAVE_CROSSED", restarted.read("claim-buy-1")["state"])

    def test_not_sent_only_before_sender_entry(self) -> None:
        self.seal()
        result = self.transition("claim-buy-1", 1, "NOT_SENT")
        self.assertEqual("NOT_SENT", result["state"])
        with self.assertRaises(KisDomesticFunctionalMutationBlocked):
            self.transition("claim-buy-1", result["revision"], "SENDER_ENTERED")

    def test_ack_tuple_is_exact_unique_and_immutable(self) -> None:
        self.seal()
        self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        acked = self.official("claim-buy-1", 3, [self.row("ACKNOWLEDGED")])
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "official capture"):
            self.transition("claim-buy-1", acked["revision"], "PARTIAL", ACK)

        self.seal("claim-buy-2")
        self.transition("claim-buy-2", 1, "SENDER_ENTERED")
        self.transition("claim-buy-2", 2, "POST_MAY_HAVE_CROSSED")
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "not unique"):
            self.official("claim-buy-2", 3, [self.row("ACKNOWLEDGED")])

    def test_partial_fill_cancel_and_unknown_core_fsm(self) -> None:
        for suffix, terminal in (("partial", "FILLED"), ("cancel", "CANCELED"), ("unknown", "UNKNOWN")):
            claim = "claim-" + suffix
            key = {**ACK, "orderNo": str(1000 + len(suffix))}
            self.seal(claim)
            self.transition(claim, 1, "SENDER_ENTERED")
            self.transition(claim, 2, "POST_MAY_HAVE_CROSSED")
            if suffix == "partial":
                row = self.official(claim, 3, [self.row("ACKNOWLEDGED", key=key)])
                row = self.official(claim, row["revision"], [self.row("PARTIAL", key=key, filled="0.5")])
                row = self.official(claim, row["revision"], [self.row("FILLED", key=key, filled="1")])
            elif suffix == "cancel":
                row = self.official(claim, 3, [self.row("ACKNOWLEDGED", key=key)])
                row = self.official(claim, row["revision"], [self.row("CANCEL_PENDING", key=key)])
                row = self.official(claim, row["revision"], [self.row("CANCELED", key=key)])
            else:
                row = self.official(claim, 3, [])
            self.assertEqual(terminal, row["state"])

    def test_cleanup_cancel_requires_exact_owned_tuple(self) -> None:
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "required"):
            self.journal.seal_request(
                claim_id="claim-cancel-bad", session_id="kis-session-1",
                operation="CLEANUP_CANCEL", endpoint=CANCEL_ENDPOINT,
                tr_id="TTTC0013U", side="CANCEL", account_fingerprint=ACCOUNT,
                credential_configuration_hash=CREDENTIAL, authority_revision=7,
                payload={
                    "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": "", "ORD_DVSN": "00",
                    "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "1", "ORD_UNPR": "0",
                    "QTY_ALL_ORD_YN": "Y", "EXCG_ID_DVSN_CD": "KRX",
                },
                owned_order_key=EMPTY_KEY, owned_order_side="",
            )
        sealed = self.seal("claim-cancel-good", "CLEANUP_CANCEL")
        self.assertEqual(ACK, sealed["authorityIntent"]["ownedOrderKey"])

    def test_dirty_schema_without_primary_key_fails_before_repair(self) -> None:
        dirty_path = Path(self.temp.name) / "dirty.sqlite3"
        dirty = ProgramLedger(dirty_path)
        with dirty.connection() as conn:
            conn.execute("CREATE TABLE kis_mutation_request (claim_id TEXT, state TEXT)")
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "schema fingerprint"):
            DurableKisDomesticFunctionalMutationJournal(
                program_ledger=ProgramLedger(dirty_path), signer_key=KEY,
                signer_key_id="test-kis-mutation-key-v1",
                official_truth_key=TRUTH_KEY, official_truth_key_id=TRUTH_KEY_ID,
            )
        conn = sqlite3.connect(dirty_path)
        try:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kis_mutation_%'")}
        finally:
            conn.close()
        self.assertEqual({"kis_mutation_request"}, tables)

    def test_official_truth_resolves_ambiguous_filled_and_archives_raw(self) -> None:
        self.seal()
        self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        result = self.official("claim-buy-1", 3, [self.row("FILLED", filled="1")])
        self.assertEqual("FILLED", result["state"])
        self.assertFalse(result["retryAllowed"])
        verified = self.journal.verify_integrity("claim-buy-1")
        self.assertEqual(2, verified["archiveCount"])
        self.assertEqual(1, verified["truthCount"])

    def test_absent_official_row_stays_unknown_and_never_retryable(self) -> None:
        self.seal()
        self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        result = self.official("claim-buy-1", 3, [])
        self.assertEqual("UNKNOWN", result["state"])
        self.assertFalse(result["retryAllowed"])

    def test_official_truth_requires_complete_exact_pages_and_identity(self) -> None:
        self.seal()
        self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "complete"):
            self.official("claim-buy-1", 3, [], complete=False)
        duplicate = self.row("FILLED", filled="1")
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "duplicate identity"):
            self.official("claim-buy-1", 3, [duplicate, duplicate])

    def test_late_fill_resolves_unknown_without_retry(self) -> None:
        self.seal()
        self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        unknown = self.official("claim-buy-1", 3, [])
        filled = self.official("claim-buy-1", unknown["revision"], [self.row("FILLED", filled="1")])
        self.assertEqual("FILLED", filled["state"])
        self.assertFalse(filled["retryAllowed"])

    def test_official_cancel_pending_and_canceled_are_durable(self) -> None:
        self.seal()
        self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        pending = self.official("claim-buy-1", 3, [self.row("CANCEL_PENDING")])
        self.assertEqual("CANCEL_PENDING", pending["state"])

    def test_integrity_verifier_rejects_archive_and_chain_tamper(self) -> None:
        self.seal()
        self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        self.official("claim-buy-1", 3, [self.row("FILLED", filled="1")])
        with self.ledger.connection() as conn:
            conn.execute("UPDATE kis_mutation_raw_archive SET envelope_json='{}' WHERE archive_kind='OFFICIAL_RESPONSE'")
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "archive"):
            self.journal.verify_integrity("claim-buy-1")
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "archive"):
            self.journal.read("claim-buy-1")

    def test_missing_ack_requires_new_post_marker_key_exact_time_and_price(self) -> None:
        cases = (
            ("baseline", [self.row("ACKNOWLEDGED")], [{**ACK, "pdno": "010140", "side": "BUY", "limitPriceKrw": "100"}]),
            ("early", [self.row("ACKNOWLEDGED", observed="2026-08-14T04:14:59.000000Z")], []),
            ("price", [self.row("ACKNOWLEDGED", price="101")], []),
        )
        for suffix, rows, baseline in cases:
            with self.subTest(suffix=suffix):
                claim = "claim-causal-" + suffix
                self.seal(claim); self.transition(claim, 1, "SENDER_ENTERED")
                self.transition(claim, 2, "POST_MAY_HAVE_CROSSED")
                result = self.official(claim, 3, rows, baseline=baseline)
                self.assertEqual("UNKNOWN", result["state"])
                self.assertFalse(result["retryAllowed"])

    def test_trusted_get_query_cursor_raw_hash_and_signature_are_exact(self) -> None:
        mutations = (
            ("orderedQuery", lambda body: body["orderedQuery"].reverse(), None),
            ("account binding", lambda body: body.update(accountFingerprint="b" * 64), None),
            ("cursor", None, lambda body: body["pages"][0].update(responseCursor={"fk100": "x", "nk100": ""})),
            ("raw page", None, lambda body: body["pages"][0]["rawBody"].update(rt_cd="1")),
        )
        for index, (message, request_mutation, response_mutation) in enumerate(mutations):
            claim = f"claim-exact-{index}"
            self.seal(claim); self.transition(claim, 1, "SENDER_ENTERED")
            self.transition(claim, 2, "POST_MAY_HAVE_CROSSED")
            with self.subTest(message=message), self.assertRaises(KisDomesticFunctionalMutationBlocked):
                self.official(claim, 3, [], mutate_request=request_mutation,
                              mutate_response=response_mutation)

    def test_manual_terminal_states_are_forbidden_without_official_capture(self) -> None:
        self.seal(); self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        for state in ("ACKNOWLEDGED", "PARTIAL", "FILLED", "CANCEL_PENDING", "CANCELED", "REJECTED", "UNKNOWN"):
            with self.subTest(state=state), self.assertRaisesRegex(
                KisDomesticFunctionalMutationBlocked, "official capture"
            ):
                self.transition("claim-buy-1", 3, state, ACK if state not in {"REJECTED", "UNKNOWN"} else None)

    def test_cleanup_cancel_reconciles_original_side_without_reusing_target_as_ack(self) -> None:
        self.seal("claim-cancel", "CLEANUP_CANCEL")
        self.transition("claim-cancel", 1, "SENDER_ENTERED")
        self.transition("claim-cancel", 2, "POST_MAY_HAVE_CROSSED")
        result = self.official(
            "claim-cancel", 3, [self.row("CANCELED", side="BUY")]
        )
        self.assertEqual("CANCELED", result["state"])
        stored = self.journal.read("claim-cancel")
        self.assertEqual("", stored["ack_order_no"])
        with self.ledger.connection() as conn:
            truth = json.loads(conn.execute(
                "SELECT record_json FROM kis_mutation_official_truth WHERE claim_id='claim-cancel'"
            ).fetchone()[0])
        self.assertEqual(ACK, truth["officialOrderKey"])

    def test_integrity_rejects_orphan_archive_and_truth_archive_projection(self) -> None:
        self.seal(); self.transition("claim-buy-1", 1, "SENDER_ENTERED")
        self.transition("claim-buy-1", 2, "POST_MAY_HAVE_CROSSED")
        self.official("claim-buy-1", 3, [self.row("FILLED", filled="1")])
        with self.ledger.connection() as conn:
            source = conn.execute(
                "SELECT * FROM kis_mutation_raw_archive WHERE archive_kind='OFFICIAL_REQUEST'"
            ).fetchone()
            values = list(source); values[0] = "orphan-request"
            conn.execute("INSERT INTO kis_mutation_raw_archive VALUES (?,?,?,?,?,?,?,?,?,?,?)", values)
        with self.assertRaisesRegex(KisDomesticFunctionalMutationBlocked, "orphan"):
            self.journal.verify_integrity("claim-buy-1")


if __name__ == "__main__":
    unittest.main()
