from __future__ import annotations

# Primary sources for these fixtures (reviewed 2026-08-14):
# https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_time_dailychartprice/inquire_time_dailychartprice.py
# https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_time_dailychartprice/chk_inquire_time_dailychartprice.py
# https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_time_itemchartprice/inquire_time_itemchartprice.py
# The first source specifies FHKST03010230, the path/query and <=120 rows.  It
# does not specify native 5m rows or continuation.  The third source documents
# newest-minute volume mutability; therefore every successful fixture remains
# diagnostic-only and release/order authority remains false.

import base64
import copy
import hashlib
import hmac
import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from live_trader.kis_domestic_functional_candle_get import (
    ENDPOINT,
    LIVE_ORIGIN,
    PDNO,
    TR_ID,
    KisDomesticFunctionalCandleGetBlocked,
    KisDomesticFunctionalCandleGetVerifier,
    canonical_public_request_bytes,
    production_entrypoint_status,
)


KEY = b"candle-get-test-authority-key-32bytes!!"
KEY_ID = hashlib.sha256(b"candle-get-test-authority-key-id").hexdigest()
ACCOUNT = hashlib.sha256(b"candle-get-test-account").hexdigest()
CREDENTIAL = hashlib.sha256(b"candle-get-test-credential").hexdigest()
AUTH_DOMAIN = b"kis-domestic-functional-authenticated-get/v1\x00"
CAPTURE_DOMAIN = b"kis-domestic-functional-capture/v1\x00"
BUNDLE_DOMAIN = b"kis-domestic-functional-candle-bundle/v1\x00"
KST = ZoneInfo("Asia/Seoul")


def canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def signature(domain: bytes, value: Mapping[str, Any]) -> str:
    return hmac.new(KEY, domain + canonical(value), hashlib.sha256).hexdigest()


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


class Fixture:
    def __init__(self) -> None:
        self.observed = datetime(2026, 8, 14, 9, 55, 1, tzinfo=KST).astimezone(
            timezone.utc
        )
        self.now = self.observed + timedelta(seconds=1)

    @staticmethod
    def query_items() -> list[list[str]]:
        return [
            ["FID_COND_MRKT_DIV_CODE", "J"],
            ["FID_INPUT_ISCD", PDNO],
            ["FID_INPUT_HOUR_1", "095400"],
            ["FID_INPUT_DATE_1", "20260814"],
            ["FID_PW_DATA_INCU_YN", "N"],
            ["FID_FAKE_TICK_INCU_YN", ""],
        ]

    @staticmethod
    def rows() -> list[dict[str, str]]:
        opened = datetime(2026, 8, 14, 9, 0, 0, tzinfo=KST)
        rows: list[dict[str, str]] = []
        for index in range(55):
            moment = opened + timedelta(minutes=index)
            base = 10_000 + index
            rows.append(
                {
                    "stck_bsop_date": moment.strftime("%Y%m%d"),
                    "stck_cntg_hour": moment.strftime("%H%M%S"),
                    "stck_prpr": f"{base + 1:08d}",
                    "stck_oprc": f"{base:08d}",
                    "stck_hgpr": f"{base + 2:08d}",
                    "stck_lwpr": f"{base - 1:08d}",
                    "cntg_vol": f"{index + 1:010d}",
                    "acml_tr_pbmn": f"{(index + 1) * 10000:015d}",
                }
            )
        # Official chart responses are commonly newest-first; normalization
        # must not trust transport order.
        return list(reversed(rows))

    def attestation(self) -> dict[str, Any]:
        body = {
            "schemaVersion": "kis-authenticated-get-attestation/v1",
            "environment": "KIS_LIVE",
            "origin": LIVE_ORIGIN,
            "custtype": "P",
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "authenticated": True,
            "allowedMethods": ["GET"],
        }
        return {**body, "signatureHash": signature(AUTH_DOMAIN, body)}

    @staticmethod
    def query_hmac() -> str:
        return signature(
            CAPTURE_DOMAIN,
            {
                "endpoint": ENDPOINT,
                "trId": TR_ID,
                "queryItems": Fixture.query_items(),
                "continuation": "",
                "accountFingerprint": ACCOUNT,
            },
        )

    def audit(self, *, after: bool) -> dict[str, Any]:
        dispatches: list[dict[str, Any]] = []
        if after:
            dispatches.append(
                {
                    "ordinal": 1,
                    "monotonicStartedAt": 100.0,
                    "endpoint": ENDPOINT,
                    "trId": TR_ID,
                    "continuation": "",
                    "accountFingerprint": ACCOUNT,
                    "queryHmacSha256": self.query_hmac(),
                    "method": "GET",
                    "bodyAbsent": True,
                    "physicalAttemptCount": 1,
                    "physicalAttemptCountComplete": True,
                    "effectiveUrlExact": True,
                    "redirectFollowed": False,
                    "transportOutcome": "RESPONSE",
                    "statusCode": 200,
                }
            )
        count = int(after)
        body = {
            "schemaVersion": "kis-domestic-functional-get-audit/v1",
            "origin": LIVE_ORIGIN,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "serverAuthorityKeyIdHash": KEY_ID,
            "serverAuthorityRestartVerifiable": True,
            "authenticationTokenReadCount": count,
            "oauthTokenIssuanceMayUsePost": True,
            "authenticationOauthPostDispatchCount": 0,
            "authenticationOauthPostCountComplete": True,
            "authenticationOauthPostAuthOnly": True,
            "authenticationOauthHiddenRetryCount": 0,
            "authenticationOauthRedirectFollowCount": 0,
            "officialGetDispatchCount": count,
            "physicalOfficialGetAttemptCount": count,
            "physicalOfficialGetAttemptCountComplete": True,
            "hiddenGetRetryCount": 0,
            "redirectFollowCount": 0,
            "tradingPostDeleteDispatchCount": 0,
            "minimumRequestIntervalSeconds": 2.1,
            "pacingWaitSeconds": 0.0,
            "dispatches": dispatches,
        }
        return {**body, "signatureHash": signature(CAPTURE_DOMAIN, body)}

    def page(self) -> dict[str, Any]:
        body = {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output1": {},
            "output2": self.rows(),
        }
        raw_request = canonical_public_request_bytes(
            trading_date="20260814", through_time="095400"
        )
        raw_response = json.dumps(
            body, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        page = {
            "schemaVersion": "kis-domestic-functional-candle-get-page/v1",
            "captureId": "kis-candle-capture-" + "a" * 32,
            "method": "GET",
            "origin": LIVE_ORIGIN,
            "endpoint": ENDPOINT,
            "trId": TR_ID,
            "queryItems": self.query_items(),
            "publicRequestHeaders": {
                "custtype": "P",
                "tr_id": TR_ID,
                "tr_cont": "",
            },
            "requestContinuation": "",
            "responseContinuation": "",
            "statusCode": 200,
            "observedAt": utc_text(self.observed),
            "officialServerTime": "",
            "rawRequestBytesBase64": base64.b64encode(raw_request).decode("ascii"),
            "rawRequestSha256": hashlib.sha256(raw_request).hexdigest(),
            "rawResponseBytesBase64": base64.b64encode(raw_response).decode("ascii"),
            "rawResponseSha256": hashlib.sha256(raw_response).hexdigest(),
            "body": body,
            "bodyHash": digest(body),
            "dispatchOrdinal": 1,
            "queryHmacSha256": self.query_hmac(),
        }
        return {**page, "serverAuthoritySignature": signature(CAPTURE_DOMAIN, page)}

    def envelope(self) -> dict[str, Any]:
        body = {
            "schemaVersion": "kis-domestic-functional-candle-get-bundle/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": PDNO,
            "origin": LIVE_ORIGIN,
            "endpoint": ENDPOINT,
            "trId": TR_ID,
            "intervalRequested": "5m",
            "requestedFinalizedBarCount": 11,
            "tradingDate": "20260814",
            "requestedThroughTime": "095400",
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "authorityKeyIdHash": KEY_ID,
            "authenticatedGetAttestation": self.attestation(),
            "signedClientAuditBefore": self.audit(after=False),
            "signedClientAuditAfter": self.audit(after=True),
            "pages": [self.page()],
        }
        return {
            "body": body,
            "bundleHash": digest(body),
            "signature": signature(BUNDLE_DOMAIN, body),
        }

    @staticmethod
    def resign_audit(audit: dict[str, Any]) -> None:
        body = dict(audit)
        body.pop("signatureHash", None)
        audit["signatureHash"] = signature(CAPTURE_DOMAIN, body)

    @staticmethod
    def rebuild_and_resign_page(page: dict[str, Any]) -> None:
        body = page["body"]
        raw_response = json.dumps(
            body, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        page["rawResponseBytesBase64"] = base64.b64encode(raw_response).decode("ascii")
        page["rawResponseSha256"] = hashlib.sha256(raw_response).hexdigest()
        page["bodyHash"] = digest(body)
        signed = dict(page)
        signed.pop("serverAuthoritySignature", None)
        page["serverAuthoritySignature"] = signature(CAPTURE_DOMAIN, signed)

    @staticmethod
    def resign_page(page: dict[str, Any]) -> None:
        signed = dict(page)
        signed.pop("serverAuthoritySignature", None)
        page["serverAuthoritySignature"] = signature(CAPTURE_DOMAIN, signed)

    @staticmethod
    def resign_bundle(envelope: dict[str, Any]) -> None:
        envelope["bundleHash"] = digest(envelope["body"])
        envelope["signature"] = signature(BUNDLE_DOMAIN, envelope["body"])


class KisDomesticFunctionalCandleGetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = Fixture()
        self.verifier = KisDomesticFunctionalCandleGetVerifier(
            server_authority_key=KEY,
            server_authority_key_id_hash=KEY_ID,
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            trusted_clock=lambda: self.fixture.now,
        )

    def verify(self, envelope: dict[str, Any]) -> dict[str, Any]:
        return self.verifier.verify(envelope)

    def test_official_minute_capture_normalizes_exactly_eleven_diagnostic_bars(self) -> None:
        result = self.verify(self.fixture.envelope())
        self.assertEqual(55, result["selectedMinuteRowCount"])
        self.assertEqual(11, result["diagnosticBarCount"])
        self.assertEqual("2026-08-14T00:00:00.000000Z", result["diagnosticBars"][0]["openAt"])
        self.assertEqual("2026-08-14T00:55:00.000000Z", result["diagnosticBars"][-1]["closeAt"])
        self.assertTrue(result["authenticatedSignedGetCaptureVerified"])
        self.assertTrue(result["singlePhysicalAttemptVerified"])

    def test_success_remains_non_authoritative_and_all_runtime_flags_false(self) -> None:
        result = self.verify(self.fixture.envelope())
        self.assertTrue(result["diagnosticElevenBarWindowAvailable"])
        for field in (
            "officialHistoricalFiveMinuteBarsGuaranteed",
            "officialNativeFiveMinuteBarsAvailable",
            "explicitFinalizationFlagAvailable",
            "officialServerTimeAvailable",
            "officialContinuationPaginationAvailable",
            "officialPaginationCompletenessProven",
            "productionAuthorityRegistryWired",
            "trustedDualClockLineageAvailable",
            "xkrxOfficialTradingDaySessionProofAvailable",
            "sourceArmGenerationOwnerLineageAvailable",
            "independentAuthenticatedH0stcnt0ArchiveAvailable",
            "h0stcnt0LinkAuthorityAvailable",
            "finalizedElevenBarAuthorityAvailable",
            "productionAvailable",
            "networkAvailable",
            "orderAuthorityAvailable",
            "promotionEligible",
        ):
            self.assertFalse(result[field], field)
        status = production_entrypoint_status()
        self.assertFalse(status["available"])
        self.assertFalse(status["signedGetClientCandleRouteWired"])
        self.assertFalse(status["productionAuthorityRegistryWired"])
        self.assertFalse(status["trustedDualClockLineageAvailable"])

    def test_result_projects_exact_capture_and_identity_join_fields(self) -> None:
        envelope = self.fixture.envelope()
        result = self.verify(envelope)
        page = envelope["body"]["pages"][0]
        self.assertEqual(ACCOUNT, result["accountFingerprint"])
        self.assertEqual(CREDENTIAL, result["credentialConfigurationHash"])
        self.assertEqual(KEY_ID, result["authorityKeyIdHash"])
        self.assertEqual(page["captureId"], result["captureId"])
        self.assertEqual(page["dispatchOrdinal"], result["dispatchOrdinal"])
        self.assertEqual(page["queryHmacSha256"], result["queryHmacSha256"])
        self.assertEqual(
            digest(envelope["body"]["signedClientAuditBefore"]),
            result["signedClientAuditBeforeHash"],
        )
        self.assertEqual(
            digest(envelope["body"]["signedClientAuditAfter"]),
            result["signedClientAuditAfterHash"],
        )
        self.assertTrue(result["singleDocumentedResponseCaptured"])
        self.assertFalse(result["officialPaginationCompletenessProven"])

    def test_exact_route_tr_query_and_live_origin_are_enforced(self) -> None:
        for field, replacement in (
            ("origin", "https://example.invalid"),
            ("trId", "FHKST03010200"),
            ("pdno", "005930"),
        ):
            envelope = self.fixture.envelope()
            envelope["body"][field] = replacement
            self.fixture.resign_bundle(envelope)
            with self.subTest(field=field), self.assertRaises(
                KisDomesticFunctionalCandleGetBlocked
            ):
                self.verify(envelope)
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        page["queryItems"][1][1] = "005930"
        self.fixture.resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaises(KisDomesticFunctionalCandleGetBlocked):
            self.verify(envelope)

    def test_raw_response_bytes_hash_and_body_are_recomputed(self) -> None:
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        raw = base64.b64decode(page["rawResponseBytesBase64"])
        page["rawResponseBytesBase64"] = base64.b64encode(raw + b" ").decode("ascii")
        self.fixture.resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "raw response hash"):
            self.verify(envelope)

    def test_raw_response_duplicate_json_object_key_is_rejected(self) -> None:
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        canonical_body = json.dumps(
            page["body"], ensure_ascii=False, separators=(",", ":")
        )
        duplicate = canonical_body.replace(
            '{"rt_cd":"0",', '{"rt_cd":"0","rt_cd":"0",', 1
        ).encode("utf-8")
        page["rawResponseBytesBase64"] = base64.b64encode(duplicate).decode("ascii")
        page["rawResponseSha256"] = hashlib.sha256(duplicate).hexdigest()
        self.fixture.resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "duplicate"):
            self.verify(envelope)

    def test_bundle_page_audit_and_attestation_signatures_are_independent(self) -> None:
        mutators = (
            lambda envelope: envelope.__setitem__("signature", "0" * 64),
            lambda envelope: envelope["body"]["pages"][0].__setitem__(
                "serverAuthoritySignature", "0" * 64
            ),
            lambda envelope: envelope["body"]["signedClientAuditAfter"].__setitem__(
                "signatureHash", "0" * 64
            ),
            lambda envelope: envelope["body"]["authenticatedGetAttestation"].__setitem__(
                "signatureHash", "0" * 64
            ),
        )
        for mutator in mutators:
            envelope = self.fixture.envelope()
            mutator(envelope)
            if envelope["signature"] != "0" * 64:
                self.fixture.resign_bundle(envelope)
            with self.assertRaises(KisDomesticFunctionalCandleGetBlocked):
                self.verify(envelope)

    def test_hidden_retry_redirect_and_physical_attempt_tamper_are_rejected(self) -> None:
        cases = (
            ("hiddenGetRetryCount", 1),
            ("redirectFollowCount", 1),
            ("physicalOfficialGetAttemptCount", 2),
        )
        for field, value in cases:
            envelope = self.fixture.envelope()
            audit = envelope["body"]["signedClientAuditAfter"]
            audit[field] = value
            self.fixture.resign_audit(audit)
            self.fixture.resign_bundle(envelope)
            with self.subTest(field=field), self.assertRaises(
                KisDomesticFunctionalCandleGetBlocked
            ):
                self.verify(envelope)
        envelope = self.fixture.envelope()
        audit = envelope["body"]["signedClientAuditAfter"]
        audit["dispatches"][0]["redirectFollowed"] = True
        self.fixture.resign_audit(audit)
        self.fixture.resign_bundle(envelope)
        with self.assertRaises(KisDomesticFunctionalCandleGetBlocked):
            self.verify(envelope)

    def test_undocumented_extra_page_or_continuation_is_rejected(self) -> None:
        envelope = self.fixture.envelope()
        envelope["body"]["pages"].append(copy.deepcopy(envelope["body"]["pages"][0]))
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "pagination"):
            self.verify(envelope)
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        page["responseContinuation"] = "M"
        self.fixture.resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaises(KisDomesticFunctionalCandleGetBlocked):
            self.verify(envelope)

    def test_truncated_duplicate_and_noncontiguous_minute_windows_are_rejected(self) -> None:
        variants = []
        truncated = self.fixture.envelope()
        truncated["body"]["pages"][0]["body"]["output2"].pop()
        variants.append(truncated)
        duplicate = self.fixture.envelope()
        rows = duplicate["body"]["pages"][0]["body"]["output2"]
        rows[10]["stck_cntg_hour"] = rows[11]["stck_cntg_hour"]
        variants.append(duplicate)
        gap = self.fixture.envelope()
        rows = gap["body"]["pages"][0]["body"]["output2"]
        rows[20]["stck_cntg_hour"] = "083000"
        variants.append(gap)
        for envelope in variants:
            page = envelope["body"]["pages"][0]
            self.fixture.rebuild_and_resign_page(page)
            self.fixture.resign_bundle(envelope)
            with self.assertRaises(KisDomesticFunctionalCandleGetBlocked):
                self.verify(envelope)

    def test_official_padded_numbers_normalize_but_unicode_negative_and_bad_ohlc_fail(self) -> None:
        result = self.verify(self.fixture.envelope())
        self.assertEqual("10000", result["diagnosticBars"][0]["open"])
        bad_values = ("１２３", "-1", "NaN")
        for bad in bad_values:
            envelope = self.fixture.envelope()
            page = envelope["body"]["pages"][0]
            page["body"]["output2"][0]["stck_prpr"] = bad
            self.fixture.rebuild_and_resign_page(page)
            self.fixture.resign_bundle(envelope)
            with self.subTest(value=bad), self.assertRaises(
                KisDomesticFunctionalCandleGetBlocked
            ):
                self.verify(envelope)
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        page["body"]["output2"][0]["stck_lwpr"] = "99999999"
        self.fixture.rebuild_and_resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "OHLC"):
            self.verify(envelope)

    def test_capture_must_be_same_kst_day_fresh_and_after_window_close(self) -> None:
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        page["observedAt"] = utc_text(self.fixture.observed - timedelta(seconds=10))
        self.fixture.resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "stale"):
            self.verify(envelope)
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        page["observedAt"] = utc_text(
            datetime(2026, 8, 14, 9, 54, 59, tzinfo=KST)
        )
        self.fixture.resign_page(page)
        self.fixture.resign_bundle(envelope)
        verifier = KisDomesticFunctionalCandleGetVerifier(
            server_authority_key=KEY,
            server_authority_key_id_hash=KEY_ID,
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            trusted_clock=lambda: datetime(2026, 8, 14, 9, 55, 0, tzinfo=KST),
        )
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "window close"):
            verifier.verify(envelope)

    def test_official_server_time_cannot_be_asserted_by_caller(self) -> None:
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        page["officialServerTime"] = utc_text(self.fixture.observed)
        self.fixture.resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "officialServerTime"):
            self.verify(envelope)

    def test_h0stcnt0_comparable_hash_is_ohlc_only_and_never_authority(self) -> None:
        first = self.verify(self.fixture.envelope())
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        # Volume is explicitly diagnostic because the official same-day sample
        # documents newest-row cntg_vol carry-forward behavior.
        page["body"]["output2"][-1]["cntg_vol"] = "0000009999"
        self.fixture.rebuild_and_resign_page(page)
        self.fixture.resign_bundle(envelope)
        second = self.verify(envelope)
        self.assertEqual(
            first["diagnosticBars"][0]["h0stcnt0ComparableHash"],
            second["diagnosticBars"][0]["h0stcnt0ComparableHash"],
        )
        self.assertNotEqual(
            first["diagnosticBars"][0]["diagnosticBarHash"],
            second["diagnosticBars"][0]["diagnosticBarHash"],
        )
        self.assertFalse(second["h0stcnt0LinkAuthorityAvailable"])

    def test_missing_required_official_field_and_after_hours_rows_fail_closed(self) -> None:
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        del page["body"]["output2"][0]["stck_hgpr"]
        self.fixture.rebuild_and_resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "required fields"):
            self.verify(envelope)
        envelope = self.fixture.envelope()
        page = envelope["body"]["pages"][0]
        page["body"]["output2"][0]["stck_cntg_hour"] = "153000"
        self.fixture.rebuild_and_resign_page(page)
        self.fixture.resign_bundle(envelope)
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "regular minute grid"):
            self.verify(envelope)

    def test_naive_or_rollback_clock_is_rejected(self) -> None:
        naive = KisDomesticFunctionalCandleGetVerifier(
            server_authority_key=KEY,
            server_authority_key_id_hash=KEY_ID,
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            trusted_clock=lambda: datetime(2026, 8, 14, 9, 55, 2),
        )
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "aware"):
            naive.verify(self.fixture.envelope())
        future = KisDomesticFunctionalCandleGetVerifier(
            server_authority_key=KEY,
            server_authority_key_id_hash=KEY_ID,
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            trusted_clock=lambda: self.fixture.observed - timedelta(seconds=1),
        )
        with self.assertRaisesRegex(KisDomesticFunctionalCandleGetBlocked, "future"):
            future.verify(self.fixture.envelope())


if __name__ == "__main__":
    unittest.main()
