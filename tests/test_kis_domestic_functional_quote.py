from __future__ import annotations

import copy
import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_quote import (
    DurableKisDomesticFunctionalQuoteStore,
    KisDomesticFunctionalQuoteBlocked,
    LIVE_ORIGIN,
    PDNO,
    QUOTE_ENDPOINT,
    QUOTE_TR_ID,
    ROUTE,
    production_entrypoint_status,
)


KEY = b"quote-server-authority-key-at-least-32-bytes"
KEY_ID = "kis-live-authority-v1"
OWNER = "state-owned-kis-quote-v1"
ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
NOW = datetime(2026, 8, 13, 4, 0, 2, tzinfo=timezone.utc)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def sign_capture(value):
    return hmac.new(KEY, b"kis-domestic-functional-capture/v1\x00" + canonical(value), hashlib.sha256).hexdigest()


def sign_lane(value):
    return hmac.new(KEY, b"kis-domestic-functional-lane-record/v1\x00" + canonical(value), hashlib.sha256).hexdigest()


def sign_next_open(value):
    return hmac.new(KEY, b"kis-domestic-functional-next-open/v1\x00" + canonical(value), hashlib.sha256).hexdigest()


def sign_rolling(value):
    return hmac.new(KEY, b"ROLLING_PREFLIGHT_RECEIPT\n" + canonical(value), hashlib.sha256).hexdigest()


def lane_envelope():
    raw = {
        "schemaVersion": "kis-domestic-next-open-trigger/v1", "route": ROUTE,
        "pdno": PDNO, "source": "KIS_WEBSOCKET", "eventType": "NEXT_BAR_OPEN",
        "evaluationId": "kis-eval-" + "3" * 32,
        "barOpenAt": "2026-08-13T04:00:00Z", "observedAt": "2026-08-13T04:00:01Z",
        "openPriceKrw": "80000", "sourceProvider": "kis",
        "sourceGeneration": "kis-ws-generation-" + "4" * 32,
        "sourceSequence": "23", "rawEventHash": "5" * 64,
    }
    raw["sourceProofHash"] = digest({
        "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1", "route": ROUTE,
        "pdno": PDNO, "sourceProvider": "kis", "sourceGeneration": raw["sourceGeneration"],
        "sourceSequence": raw["sourceSequence"], "rawEventHash": raw["rawEventHash"],
        "barOpenAt": raw["barOpenAt"], "observedAt": raw["observedAt"],
    })
    body = {
        **raw, "triggerId": "kis-trigger-" + "1" * 32, "evaluationHash": "6" * 64,
        "publicArmId": "kis-public-arm-one", "publicArmHash": "7" * 64,
        "publicDataOnly": True, "accountAuthorityAvailable": False,
        "orderAuthorityAvailable": False, "contractEnvelopeHash": "8" * 64,
        "codeManifestHash": "9" * 64, "rawTriggerHash": digest(raw),
        "rawTriggerSignature": sign_next_open(raw), "promotionEligible": False,
    }
    return {"body": body, "recordHash": digest(body), "signature": sign_lane(body)}


def rolling_envelope(lane=None):
    lane = lane or lane_envelope()
    trigger = lane["body"]
    body = {
        "schemaVersion": "kis-domestic-rolling-preflight-consumption/v1",
        "route": ROUTE, "pdno": PDNO, "snapshotId": "kis-rolling-snapshot-one",
        "snapshotHash": "a" * 64, "diagnosticHash": "b" * 64,
        "captureBundleHash": "c" * 64, "accountFingerprint": ACCOUNT,
        "credentialConfigurationHash": CREDENTIAL, "preactivationBaselineHash": "d" * 64,
        "contractEnvelopeHash": trigger["contractEnvelopeHash"],
        "codeManifestHash": trigger["codeManifestHash"], "publicArmId": trigger["publicArmId"],
        "preapprovalHash": "e" * 64, "evaluationId": trigger["evaluationId"],
        "evaluationHash": trigger["evaluationHash"], "triggerId": trigger["triggerId"],
        "triggerHash": lane["recordHash"], "triggerEnvelopeHash": "f" * 64,
        "sourceGeneration": trigger["sourceGeneration"], "barOpenAt": trigger["barOpenAt"],
        "completedAt": "2026-08-13T03:59:55Z", "expiresAt": "2026-08-13T04:00:55Z",
        "consumedAt": "2026-08-13T04:00:01Z", "sessionId": "kis-session-one",
        "sessionNonceHash": "1" * 64, "singleUseConsumed": True,
        "privateAccountAuthorityAvailable": False, "tokenAuthorityAvailable": False,
        "orderAuthorityAvailable": False, "networkOrderPostAllowed": False,
        "tradingMutationCount": 0, "finalQuoteAvailable": False,
        "releaseEvidenceEligible": False,
    }
    return {"body": body, "receiptHash": digest(body), "serverAuthoritySignature": sign_rolling(body)}


def quote(*, broker_time=True, price="80000", ask="80000", bid="79800", ask_qty="10"):
    output = {"stck_prpr": price, "askp1": ask, "bidp1": bid, "askp_rsqn1": ask_qty,
              "stck_mxpr": "104000", "stck_llam": "56000"}
    if broker_time:
        output.update(stck_bsop_date="20260813", stck_cntg_hour="130002")
    response_body = {"rt_cd": "0", "output": output}
    body = {
        "schemaVersion": "kis-domestic-functional-quote-preflight/v1", "method": "GET",
        "origin": LIVE_ORIGIN, "endpoint": QUOTE_ENDPOINT, "trId": QUOTE_TR_ID,
        "query": {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": PDNO},
        "publicRequestHeaders": {"custtype": "P", "tr_id": QUOTE_TR_ID, "tr_cont": ""},
        "accountFingerprint": ACCOUNT, "credentialConfigurationHash": CREDENTIAL,
        "observedAt": "2026-08-13T04:00:02Z", "elapsedSeconds": "1",
        "body": response_body, "bodyHash": digest(response_body), "priceKrw": price,
        "quantity": 1, "notionalKrw": price, "orderCapSatisfied": True,
        "durableCasPersisted": False,
    }
    signed = {**body, "quoteHash": digest(body)}
    return {**signed, "serverAuthoritySignature": sign_capture(signed)}


def query_hmac():
    return sign_capture({
        "endpoint": QUOTE_ENDPOINT, "trId": QUOTE_TR_ID,
        "queryItems": [["FID_COND_MRKT_DIV_CODE", "J"], ["FID_INPUT_ISCD", PDNO]],
        "continuation": "", "accountFingerprint": ACCOUNT,
    })


def audit(*, after=False, dispatch_changes=None, body_changes=None):
    dispatches = []
    if after:
        row = {
            "ordinal": 1, "monotonicStartedAt": 10.0, "endpoint": QUOTE_ENDPOINT,
            "trId": QUOTE_TR_ID, "continuation": "", "accountFingerprint": ACCOUNT,
            "queryHmacSha256": query_hmac(), "method": "GET", "bodyAbsent": True,
            "physicalAttemptCount": 1, "physicalAttemptCountComplete": True,
            "effectiveUrlExact": True, "redirectFollowed": False,
            "transportOutcome": "RESPONSE", "statusCode": 200,
        }
        row.update(dispatch_changes or {})
        dispatches = [row]
    n = int(after)
    body = {
        "schemaVersion": "kis-domestic-functional-get-audit/v1", "origin": LIVE_ORIGIN,
        "accountFingerprint": ACCOUNT, "credentialConfigurationHash": CREDENTIAL,
        "serverAuthorityKeyIdHash": hashlib.sha256(KEY_ID.encode()).hexdigest(),
        "serverAuthorityRestartVerifiable": True, "authenticationTokenReadCount": n,
        "oauthTokenIssuanceMayUsePost": True, "authenticationOauthPostDispatchCount": n,
        "authenticationOauthPostCountComplete": True, "authenticationOauthPostAuthOnly": True,
        "authenticationOauthHiddenRetryCount": 0, "authenticationOauthRedirectFollowCount": 0,
        "officialGetDispatchCount": n, "physicalOfficialGetAttemptCount": n,
        "physicalOfficialGetAttemptCountComplete": True, "hiddenGetRetryCount": 0,
        "redirectFollowCount": 0, "tradingPostDeleteDispatchCount": 0,
        "minimumRequestIntervalSeconds": 2.1, "pacingWaitSeconds": 0.0,
        "dispatches": dispatches,
    }
    body.update(body_changes or {})
    return {**body, "signatureHash": sign_capture(body)}


def capture_binding(q, before, after, **changes):
    body = {
        "schemaVersion": "kis-domestic-functional-quote-capture-binding/v1",
        "captureId": "kis-quote-capture-" + "2" * 32, "quoteHash": q["quoteHash"],
        "dispatchOrdinal": 1, "queryHmacSha256": query_hmac(),
        "auditBeforeHash": digest(before), "auditAfterHash": digest(after),
        "observedAt": q["observedAt"], "endpoint": QUOTE_ENDPOINT, "trId": QUOTE_TR_ID,
    }
    body.update(changes)
    binding_hash = digest(body)
    return {"body": body, "bindingHash": binding_hash,
            "serverAuthoritySignature": sign_capture({**body, "bindingHash": binding_hash})}


class QuoteStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "quote.sqlite3"
        self.clock_value = NOW
        self.store = DurableKisDomesticFunctionalQuoteStore(
            self.path, server_authority_key=KEY, server_authority_key_id=KEY_ID,
            owner_id=OWNER, account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL, clock=lambda: self.clock_value,
        )

    def issue(self, *, lane=None, rolling=None, q=None, before=None, after=None, binding=None):
        lane = lane or lane_envelope(); rolling = rolling or rolling_envelope(lane)
        q = q or quote(); before = before or audit(); after = after or audit(after=True)
        binding = binding or capture_binding(q, before, after)
        return self.store.issue(
            lane_trigger_envelope=lane, rolling_receipt_envelope=rolling,
            signed_quote_capture=q, signed_client_audit_before=before,
            signed_client_audit_after=after, signed_capture_binding=binding,
        )

    def test_authenticated_lane_source_rolling_and_quote_consume_once(self):
        issued = self.issue(); self.assertTrue(issued["body"]["orderAuthorityFresh"])
        self.assertEqual("a" * 64, issued["body"]["trigger"]["rollingSnapshotHash"])
        self.assertEqual("kis-session-one", issued["body"]["trigger"]["rollingSessionId"])
        self.assertEqual("1" * 64, issued["body"]["trigger"]["rollingSessionNonceHash"])
        self.assertEqual("2026-08-13T04:00:01Z", issued["body"]["trigger"]["rollingConsumedAt"])
        self.assertEqual("2026-08-13T04:00:55Z", issued["body"]["trigger"]["rollingExpiresAt"])
        consumed = self.store.consume(receipt_id=issued["body"]["receiptId"],
                                      trigger_id=lane_envelope()["body"]["triggerId"], expected_revision=1)
        self.assertEqual("CONSUMED", consumed["body"]["state"])
        with self.assertRaises(KisDomesticFunctionalQuoteBlocked):
            self.store.consume(receipt_id=issued["body"]["receiptId"],
                               trigger_id=lane_envelope()["body"]["triggerId"], expected_revision=1)

    def test_rolling_receipt_lineage_is_persisted_and_rechecked_at_consume(self):
        rolling = rolling_envelope()
        issued = self.issue(rolling=rolling)
        with closing(sqlite3.connect(self.path)) as conn:
            row = conn.execute(
                """SELECT rolling_session_id,rolling_session_nonce_hash,
                          rolling_consumed_at,rolling_expires_at,rolling_receipt_hash
                   FROM kis_functional_quote_receipt"""
            ).fetchone()
        self.assertEqual(
            (
                rolling["body"]["sessionId"], rolling["body"]["sessionNonceHash"],
                rolling["body"]["consumedAt"], rolling["body"]["expiresAt"],
                rolling["receiptHash"],
            ),
            row,
        )
        self.clock_value = datetime(2026, 8, 13, 4, 0, 56, tzinfo=timezone.utc)
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "rolling snapshot expired"):
            self.store.consume(
                receipt_id=issued["body"]["receiptId"],
                trigger_id=lane_envelope()["body"]["triggerId"],
                expected_revision=1,
            )

    def test_lane_and_source_signatures_not_caller_hex_are_required(self):
        lane = lane_envelope(); lane["signature"] = "0" * 64
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "lane trigger record signature"):
            self.issue(lane=lane, rolling=rolling_envelope(lane_envelope()))
        lane = lane_envelope(); lane["body"]["rawTriggerSignature"] = "0" * 64
        lane["recordHash"] = digest(lane["body"]); lane["signature"] = sign_lane(lane["body"])
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "source NEXT_OPEN"):
            self.issue(lane=lane, rolling=rolling_envelope(lane))

    def test_rolling_evaluation_trigger_snapshot_and_signature_join(self):
        lane = lane_envelope(); rolling = rolling_envelope(lane)
        rolling["body"]["evaluationHash"] = "0" * 64
        rolling["receiptHash"] = digest(rolling["body"])
        rolling["serverAuthoritySignature"] = sign_rolling(rolling["body"])
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "evaluationHash"):
            self.issue(lane=lane, rolling=rolling)
        rolling = rolling_envelope(lane); rolling["serverAuthoritySignature"] = "0" * 64
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "rolling receipt signature"):
            self.issue(lane=lane, rolling=rolling)

    def test_exact_audit_delta_query_hmac_transport_and_capture_id(self):
        for after, message in (
            (audit(after=True, dispatch_changes={"queryHmacSha256": "0" * 64}), "queryHmac"),
            (audit(after=True, dispatch_changes={"effectiveUrlExact": False}), "effectiveUrl"),
            (audit(after=True, body_changes={"hiddenGetRetryCount": 1}), "hiddenGetRetry"),
        ):
            with self.subTest(message=message):
                q = quote(); before = audit(); binding = capture_binding(q, before, after)
                with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, message):
                    self.issue(q=q, before=before, after=after, binding=binding)
        q = quote(); before = audit(); after = audit(after=True)
        bad = capture_binding(q, before, after, captureId="not-a-capture")
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "binding"):
            self.issue(q=q, before=before, after=after, binding=bad)

    def test_exact_2_1_second_physical_pacing_is_recomputed(self):
        before = audit(after=True)

        def after_with_start(start):
            first = copy.deepcopy(before["dispatches"][0])
            second = {**first, "ordinal": 2, "monotonicStartedAt": start}
            return audit(
                after=True,
                body_changes={
                    "authenticationTokenReadCount": 2,
                    "authenticationOauthPostDispatchCount": 2,
                    "officialGetDispatchCount": 2,
                    "physicalOfficialGetAttemptCount": 2,
                    "pacingWaitSeconds": 2.1,
                    "dispatches": [first, second],
                },
            )

        q = quote()
        too_soon = after_with_start(12.099)
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "physical pacing"):
            self.issue(
                q=q, before=before, after=too_soon,
                binding=capture_binding(q, before, too_soon, dispatchOrdinal=2),
            )

        exact = after_with_start(12.1)
        issued = self.issue(
            q=q, before=before, after=exact,
            binding=capture_binding(q, before, exact, dispatchOrdinal=2),
        )
        self.assertTrue(issued["body"]["orderAuthorityFresh"])

        other = lane_envelope()
        other["body"]["triggerId"] = "kis-trigger-" + "8" * 32
        other["recordHash"] = digest(other["body"])
        other["signature"] = sign_lane(other["body"])
        invalid_policy = audit(after=True, body_changes={"minimumRequestIntervalSeconds": 2.0})
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "pacing interval"):
            self.issue(
                lane=other, rolling=rolling_envelope(other), q=q,
                before=audit(), after=invalid_policy,
                binding=capture_binding(q, audit(), invalid_policy),
            )

    def test_broker_timestamp_is_required_but_local_age_is_only_diagnostic(self):
        issued = self.issue(q=quote(broker_time=False))
        self.assertFalse(issued["body"]["orderAuthorityFresh"])
        self.assertTrue(issued["body"]["localObservationDiagnosticFresh"])
        self.assertIn("BROKER_TRADE_TIMESTAMP_ABSENT", issued["body"]["blockedReasons"])
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "no order authority"):
            self.store.consume(receipt_id=issued["body"]["receiptId"],
                               trigger_id=lane_envelope()["body"]["triggerId"], expected_revision=1)

    def test_broker_time_may_follow_trigger_but_not_quote_and_clock_is_strict(self):
        self.assertTrue(self.issue()["body"]["orderAuthorityFresh"])
        self.clock_value = datetime(2026, 8, 13, 13, 0, 2)  # naive
        other = lane_envelope(); other["body"]["triggerId"] = "kis-trigger-" + "9" * 32
        other["recordHash"] = digest(other["body"]); other["signature"] = sign_lane(other["body"])
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "aware"):
            self.issue(lane=other, rolling=rolling_envelope(other))

    def test_spread_liquidity_cap_and_loss_reserves_fail_closed(self):
        wide = self.issue(q=quote(bid="78000"))
        self.assertFalse(wide["body"]["orderAuthorityFresh"])
        self.assertIn("SPREAD_RESERVE_UNBOUNDED", wide["body"]["blockedReasons"])
        other = lane_envelope(); other["body"]["triggerId"] = "kis-trigger-" + "8" * 32
        other["recordHash"] = digest(other["body"]); other["signature"] = sign_lane(other["body"])
        high = self.issue(lane=other, rolling=rolling_envelope(other),
                          q=quote(price="99900", ask="99900", bid="99800"))
        self.assertIn("ORDER_AND_COST_RESERVE_EXCEEDS_CAP", high["body"]["blockedReasons"])

    def test_schema_manifest_rejects_extra_object_and_flags_stay_false(self):
        status = production_entrypoint_status()
        for key in ("available", "networkAvailable", "orderAuthorityAvailable",
                    "mutationAvailable", "promotionAvailable"):
            self.assertFalse(status[key])
        dirty = Path(self.temp.name) / "dirty.sqlite3"
        first = DurableKisDomesticFunctionalQuoteStore(
            dirty, server_authority_key=KEY, server_authority_key_id=KEY_ID,
            owner_id=OWNER, account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
        )
        del first
        with closing(sqlite3.connect(dirty)) as conn:
            conn.execute("CREATE TABLE kis_functional_quote_extra(value TEXT)"); conn.commit()
        with self.assertRaisesRegex(KisDomesticFunctionalQuoteBlocked, "extra"):
            DurableKisDomesticFunctionalQuoteStore(
                dirty, server_authority_key=KEY, server_authority_key_id=KEY_ID,
                owner_id=OWNER, account_fingerprint=ACCOUNT,
                credential_configuration_hash=CREDENTIAL,
            )


if __name__ == "__main__":
    unittest.main()
