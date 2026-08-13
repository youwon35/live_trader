from __future__ import annotations

import base64
import copy
import hashlib
import json
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from live_trader import kis_domestic_functional_candle_archive as archive_module
from live_trader.kis_domestic_functional_candle_archive import (
    DIAGNOSTIC_CLASSIFICATION,
    KisDomesticFunctionalCandleArchiveBlocked,
    KisDomesticFunctionalCandleArchiveVerifier,
    production_entrypoint_status,
)
from live_trader.kis_domestic_functional_candle_get import (
    KisDomesticFunctionalCandleGetVerifier,
)
from tests.test_kis_domestic_functional_candle_get import (
    ACCOUNT as OLD_ACCOUNT,
    AUTH_DOMAIN,
    BUNDLE_DOMAIN,
    CAPTURE_DOMAIN,
    CREDENTIAL as OLD_CREDENTIAL,
    KEY as CANDLE_KEY,
    KEY_ID as CANDLE_KEY_ID,
    Fixture as CandleFixture,
    digest,
    signature as candle_signature,
    utc_text,
)
from tests.test_kis_domestic_functional_key_registry import (
    _signature as registry_signature,
)
from tests.test_kis_domestic_functional_production_factory import (
    _FactoryFixture,
)


FAKE_NOW = datetime(2026, 8, 14, 0, 56, 0, tzinfo=timezone.utc)
ZERO_SHA = "0" * 64


def canonical(value) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def hash_value(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(
            FAKE_NOW.year,
            FAKE_NOW.month,
            FAKE_NOW.day,
            FAKE_NOW.hour,
            FAKE_NOW.minute,
            FAKE_NOW.second,
            FAKE_NOW.microsecond,
            tzinfo=timezone.utc,
        )
        return value if tz is None else value.astimezone(tz)


def _frozen_clock_pair():
    return FAKE_NOW, time.monotonic_ns()


class KisDomesticFunctionalCandleArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._patchers = (
            patch(
                "tests.test_kis_domestic_functional_production_factory.datetime",
                _FrozenDateTime,
            ),
            patch(
                "live_trader.kis_domestic_functional_production_factory._system_clock_pair",
                _frozen_clock_pair,
            ),
        )
        for patcher in cls._patchers:
            patcher.start()
        try:
            cls.factory_fixture = _FactoryFixture()
            cls.factory = cls.factory_fixture.factory()
            cls.binding = cls.factory.verifier(
                "SIGNED_GET_CAPTURE_VERIFY"
            ).binding_status()
        except BaseException:
            for patcher in reversed(cls._patchers):
                patcher.stop()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        cls.factory_fixture.cleanup()
        for patcher in reversed(cls._patchers):
            patcher.stop()

    def setUp(self) -> None:
        self.candle_fixture = CandleFixture()
        self.account = self.binding["accountFingerprint"]
        self.credential = self.binding["credentialConfigurationHash"]
        self.candle_verifier = KisDomesticFunctionalCandleGetVerifier(
            server_authority_key=CANDLE_KEY,
            server_authority_key_id_hash=CANDLE_KEY_ID,
            account_fingerprint=self.account,
            credential_configuration_hash=self.credential,
            trusted_clock=lambda: self.candle_fixture.now,
        )
        self.verifier = KisDomesticFunctionalCandleArchiveVerifier(
            candle_get_verifier=self.candle_verifier,
            signed_get_verifier=self.factory.verifier("SIGNED_GET_CAPTURE_VERIFY"),
            lane_grant_verifier=self.factory.verifier("LANE_RECORD_VERIFY"),
            source_record_verifier=self.factory.verifier("SOURCE_RECORD_VERIFY"),
            archive_extraction_verifier=self.factory.verifier(
                "ARCHIVE_EXTRACTION_VERIFY"
            ),
            trusted_clock=lambda: FAKE_NOW,
        )

    def _query_hmac(self) -> str:
        return candle_signature(
            CAPTURE_DOMAIN,
            {
                "endpoint": archive_module.ENDPOINT,
                "trId": archive_module.TR_ID,
                "queryItems": CandleFixture.query_items(),
                "continuation": "",
                "accountFingerprint": self.account,
            },
        )

    def _candle_bundle(self) -> dict:
        envelope = self.candle_fixture.envelope()
        body = envelope["body"]
        self.assertEqual(OLD_ACCOUNT, body["accountFingerprint"])
        self.assertEqual(OLD_CREDENTIAL, body["credentialConfigurationHash"])
        body["accountFingerprint"] = self.account
        body["credentialConfigurationHash"] = self.credential
        attestation = body["authenticatedGetAttestation"]
        attestation["accountFingerprint"] = self.account
        attestation["credentialConfigurationHash"] = self.credential
        unsigned_attestation = dict(attestation)
        unsigned_attestation.pop("signatureHash")
        attestation["signatureHash"] = candle_signature(
            AUTH_DOMAIN, unsigned_attestation
        )
        query_hmac = self._query_hmac()
        for audit in (
            body["signedClientAuditBefore"],
            body["signedClientAuditAfter"],
        ):
            audit["accountFingerprint"] = self.account
            audit["credentialConfigurationHash"] = self.credential
            for dispatch in audit["dispatches"]:
                dispatch["accountFingerprint"] = self.account
                dispatch["queryHmacSha256"] = query_hmac
            CandleFixture.resign_audit(audit)
        page = body["pages"][0]
        page["queryHmacSha256"] = query_hmac
        CandleFixture.resign_page(page)
        CandleFixture.resign_bundle(envelope)
        return envelope

    def _registry_envelope(self, purpose: str, domain: str, body: dict) -> dict:
        private, key_id = self.factory_fixture.registry_fixture.keys[purpose]
        return {
            "body": copy.deepcopy(body),
            "recordHash": hash_value(body),
            "signature": registry_signature(
                private, body, prefix=domain.encode("ascii") + b"\0"
            ),
            "keyIdHash": key_id,
        }

    def _resign_registry_envelope(
        self, envelope: dict, purpose: str, domain: str
    ) -> None:
        private, key_id = self.factory_fixture.registry_fixture.keys[purpose]
        envelope["recordHash"] = hash_value(envelope["body"])
        envelope["signature"] = registry_signature(
            private,
            envelope["body"],
            prefix=domain.encode("ascii") + b"\0",
        )
        envelope["keyIdHash"] = key_id

    def _signed_get(self, bundle: dict) -> tuple[dict, dict]:
        result = self.candle_verifier.verify(bundle)
        body = {
            "schemaVersion": "kis-domestic-functional-candle-get-archive/v1",
            "route": archive_module.ROUTE,
            "pdno": archive_module.PDNO,
            "origin": archive_module.LIVE_ORIGIN,
            "endpoint": archive_module.ENDPOINT,
            "trId": archive_module.TR_ID,
            "accountFingerprint": self.account,
            "credentialConfigurationHash": self.credential,
            "registryAcceptedHeadHash": self.binding["registryAcceptedHeadHash"],
            "candleBundleHash": bundle["bundleHash"],
            "candleResultHash": result["resultHash"],
            "captureId": result["captureId"],
            "dispatchOrdinal": result["dispatchOrdinal"],
            "queryHmacSha256": result["queryHmacSha256"],
            "rawRequestSha256": result["rawRequestSha256"],
            "rawResponseSha256": result["rawResponseSha256"],
            "bodyHash": result["bodyHash"],
            "signedClientAuditBeforeHash": result["signedClientAuditBeforeHash"],
            "signedClientAuditAfterHash": result["signedClientAuditAfterHash"],
            "capturedAt": utc_text(
                self.candle_fixture.observed + timedelta(milliseconds=100)
            ),
            "rawBytesIncluded": True,
            "singlePhysicalAttempt": True,
            "hiddenRetryCount": 0,
            "redirectFollowCount": 0,
            "productionAvailable": False,
            "orderAuthorityAvailable": False,
            "releaseAvailable": False,
        }
        return (
            self._registry_envelope(
                "SIGNED_GET_CAPTURE_VERIFY",
                archive_module._GET_ARCHIVE_DOMAIN,
                body,
            ),
            result,
        )

    def _grant(self, result: dict) -> dict:
        body = {
            "schemaVersion": "kis-domestic-functional-candle-grant-projection/v1",
            "route": archive_module.ROUTE,
            "pdno": archive_module.PDNO,
            "accountFingerprint": self.account,
            "registryAcceptedHeadHash": self.binding["registryAcceptedHeadHash"],
            "armId": "kis-arm-" + "a" * 32,
            "sourceGeneration": "kis-source-generation-" + "b" * 32,
            "grantReceiptHash": sha("grant-receipt"),
            "grantWallAt": utc_text(
                datetime.fromisoformat(
                    result["diagnosticBars"][-1]["closeAt"].replace("Z", "+00:00")
                )
                + timedelta(seconds=2)
            ),
            "grantMonotonicNs": 10_000_000_000,
            "capturedOnce": True,
            "productionAvailable": False,
            "orderAuthorityAvailable": False,
            "releaseAvailable": False,
        }
        return self._registry_envelope(
            "LANE_RECORD_VERIFY", archive_module._GRANT_DOMAIN, body
        )

    def _source_archive(self, result: dict, grant: dict) -> dict:
        source_key = self.factory_fixture.registry_fixture.keys[
            "SOURCE_RECORD_VERIFY"
        ]
        grant_body = grant["body"]
        bars = []
        for index, diagnostic in enumerate(result["diagnosticBars"]):
            bars.append(
                {
                    "openAt": diagnostic["openAt"],
                    "closeAt": diagnostic["closeAt"],
                    "open": diagnostic["open"],
                    "high": diagnostic["high"],
                    "low": diagnostic["low"],
                    "close": diagnostic["close"],
                    "sourceSequenceStart": f"raw-sequence-{index * 5 + 1:04d}",
                    "sourceSequenceEnd": f"raw-sequence-{index * 5 + 5:04d}",
                    "eventCount": 1,
                    "rawEventChainHash": sha(f"raw-event-chain-{index}"),
                }
            )
        source_proof = {
            "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
            "route": archive_module.ROUTE,
            "pdno": archive_module.PDNO,
            "sourceProvider": "kis",
            "sourceGeneration": grant_body["sourceGeneration"],
            "firstSourceSequence": bars[0]["sourceSequenceStart"],
            "lastSourceSequence": bars[-1]["sourceSequenceEnd"],
            "sourceEventCount": sum(bar["eventCount"] for bar in bars),
            "barRawEventChainHashes": [bar["rawEventChainHash"] for bar in bars],
        }
        observed_at = utc_text(self.candle_fixture.observed + timedelta(milliseconds=200))
        window = {
            "schemaVersion": "kis-domestic-official-5m-window/v1",
            "route": archive_module.ROUTE,
            "origin": archive_module.LIVE_ORIGIN,
            "pdno": archive_module.PDNO,
            "source": "KIS_WEBSOCKET_H0STCNT0",
            "sourceProvider": "kis",
            "sourceGeneration": grant_body["sourceGeneration"],
            "firstSourceSequence": source_proof["firstSourceSequence"],
            "lastSourceSequence": source_proof["lastSourceSequence"],
            "sourceEventCount": source_proof["sourceEventCount"],
            "sourceProofHash": hash_value(source_proof),
            "interval": "5m",
            "artifactContentHash": sha("artifact-content"),
            "artifactFileSha256": sha("artifact-file"),
            "instanceContentHash": sha("instance-content"),
            "instanceFileSha256": sha("instance-file"),
            "bars": bars,
            "observedAt": observed_at,
        }
        evaluation = {
            "schemaVersion": "kis-domestic-functional-breakout-evaluation/v1",
            "route": archive_module.ROUTE,
            "pdno": archive_module.PDNO,
            "sourceGeneration": grant_body["sourceGeneration"],
            "windowHash": hash_value(window),
            "naturalSignal": "BUY",
            "observedAt": observed_at,
        }
        capture_head = sha("source-capture-head")
        frame_body = {
            "schemaVersion": "kis-h0stcnt0-raw-frame-republication/v1",
            "sourceGeneration": grant_body["sourceGeneration"],
            "receivedAt": observed_at,
            "rawFrameHash": sha("raw-frame"),
        }
        frame = {
            "body": frame_body,
            "envelopeHash": hash_value(frame_body),
            "serverSignature": registry_signature(
                source_key[0],
                frame_body,
                prefix=archive_module._SOURCE_FRAME_DOMAIN.encode("ascii") + b"\0",
            ),
            "frameHeadHash": capture_head,
        }
        ingress_head = sha("market-ingress-head")
        raw_archive = {
            "schemaVersion": "kis-domestic-h0stcnt0-durable-window-archive/v1",
            "route": archive_module.ROUTE,
            "pdno": archive_module.PDNO,
            "armId": grant_body["armId"],
            "sourceGeneration": grant_body["sourceGeneration"],
            "socketIdentityHash": sha("socket-identity"),
            "firstSourceSequence": source_proof["firstSourceSequence"],
            "lastSourceSequence": source_proof["lastSourceSequence"],
            "sourceEventCount": source_proof["sourceEventCount"],
            "captureHeadHash": capture_head,
            "authorityKeyIdHash": source_key[1],
            "upstreamExchangeSequenceAvailable": False,
            "upstreamPacketCompletenessAttested": False,
            "acceptedIngressContinuityOnly": True,
            "marketSourceIntegrationComplete": True,
            "marketSourceIngressLinkCount": 1,
            "marketSourceIngressLinkHeadHash": ingress_head,
            "marketSourceIngressLinks": [{"linkHash": ingress_head}],
            "frames": [frame],
            "events": [bar["sourceSequenceStart"] for bar in bars],
            "nextOpenEvent": {"observedAt": observed_at},
            "recomputedBars": copy.deepcopy(bars),
        }
        boundary = {
            "barOpenAt": bars[-1]["closeAt"],
            "observedAt": observed_at,
            "openPriceKrw": bars[-1]["close"],
            "sourceSequence": "next-open-sequence-0001",
            "rawEventHash": sha("next-open-event"),
        }
        observation = {
            "schemaVersion": "kis-domestic-functional-source-observation-record/v1",
            "observationId": "kis-source-observation-" + "c" * 32,
            "armId": grant_body["armId"],
            "sourceGeneration": grant_body["sourceGeneration"],
            "socketIdentityHash": raw_archive["socketIdentityHash"],
            "captureHeadHash": capture_head,
            "windowBody": window,
            "windowSignature": registry_signature(
                source_key[0],
                window,
                prefix=archive_module._SOURCE_WINDOW_DOMAIN.encode("ascii") + b"\0",
            ),
            "rawArchive": raw_archive,
            "rawArchiveHash": hash_value(raw_archive),
            "evaluationProof": evaluation,
            "evaluationProofHash": hash_value(evaluation),
            "evaluationSignature": registry_signature(
                source_key[0],
                evaluation,
                prefix=archive_module._SOURCE_EVALUATION_DOMAIN.encode("ascii")
                + b"\0",
            ),
            "averageRange": "10",
            "triggerPrice": bars[-1]["close"],
            "naturalSignal": "BUY",
            "boundary": boundary,
        }
        observation_envelope = self._registry_envelope(
            "SOURCE_RECORD_VERIFY",
            archive_module._SOURCE_OBSERVATION_DOMAIN,
            observation,
        )
        prefix = {
            "sourceGeneration": grant_body["sourceGeneration"],
            "armId": grant_body["armId"],
            "marketIngressCount": 1,
            "marketTransitionCount": 1,
            "marketIngressHeadHash": ingress_head,
            "sourceFrameCount": 1,
            "sourceEventCount": raw_archive["sourceEventCount"] + 1,
            "sourceFrameHeadHash": capture_head,
            "sourceTransitionCount": 1,
            "sourceTransitionHeadHash": sha("source-transition-head"),
            "sourceObservationCount": 0,
            "allObservationRowsIndependentlyReplayed": True,
            "allProducerRowProjectionsExact": True,
            "freshDedicatedProducerDatabasesVerified": True,
            "allRawFramesReparsed46Fields": True,
            "allProducerSignaturesVerified": True,
            "allTransitionChainsVerified": True,
            "marketSourceBijectionVerified": True,
        }
        prefix["summaryHash"] = hash_value(prefix)
        body = {
            "schemaVersion": "kis-domestic-functional-candle-dual-source-archive/v1",
            "route": archive_module.ROUTE,
            "pdno": archive_module.PDNO,
            "accountFingerprint": self.account,
            "registryAcceptedHeadHash": self.binding["registryAcceptedHeadHash"],
            "armId": grant_body["armId"],
            "sourceGeneration": grant_body["sourceGeneration"],
            "marketArchiveFileHash": sha("market-archive-file"),
            "marketArchiveCaptureHash": sha("market-archive-capture"),
            "archiveAuthorityKeyIdHash": self.factory_fixture.registry_fixture.keys[
                "ARCHIVE_EXTRACTION_VERIFY"
            ][1],
            "marketArchivePrefix": prefix,
            "sourceObservation": observation_envelope,
            "capturedAt": utc_text(self.candle_fixture.observed + timedelta(seconds=1)),
            "productionAvailable": False,
            "orderAuthorityAvailable": False,
            "releaseAvailable": False,
        }
        return self._registry_envelope(
            "ARCHIVE_EXTRACTION_VERIFY",
            archive_module._DUAL_ARCHIVE_DOMAIN,
            body,
        )

    def _fixture_set(self):
        bundle = self._candle_bundle()
        signed_get, result = self._signed_get(bundle)
        grant = self._grant(result)
        dual = self._source_archive(result, grant)
        return bundle, signed_get, grant, dual, result

    def _verify(self, bundle, signed_get, grant, dual):
        return self.verifier.verify(
            candle_bundle=bundle,
            signed_get_archive=signed_get,
            lane_grant_projection=grant,
            market_source_archive=dual,
        )

    def _resign_source_and_dual(self, dual: dict) -> None:
        source_key = self.factory_fixture.registry_fixture.keys[
            "SOURCE_RECORD_VERIFY"
        ]
        observation_envelope = dual["body"]["sourceObservation"]
        observation = observation_envelope["body"]
        window = observation["windowBody"]
        raw_archive = observation["rawArchive"]
        evaluation = observation["evaluationProof"]
        observation["windowSignature"] = registry_signature(
            source_key[0],
            window,
            prefix=archive_module._SOURCE_WINDOW_DOMAIN.encode("ascii") + b"\0",
        )
        observation["rawArchiveHash"] = hash_value(raw_archive)
        observation["evaluationProofHash"] = hash_value(evaluation)
        observation["evaluationSignature"] = registry_signature(
            source_key[0],
            evaluation,
            prefix=archive_module._SOURCE_EVALUATION_DOMAIN.encode("ascii") + b"\0",
        )
        self._resign_registry_envelope(
            observation_envelope,
            "SOURCE_RECORD_VERIFY",
            archive_module._SOURCE_OBSERVATION_DOMAIN,
        )
        self._resign_registry_envelope(
            dual,
            "ARCHIVE_EXTRACTION_VERIFY",
            archive_module._DUAL_ARCHIVE_DOMAIN,
        )

    def test_matching_sources_are_diagnostic_only_and_never_authorize(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        result = self._verify(bundle, signed_get, grant, dual)
        self.assertEqual(DIAGNOSTIC_CLASSIFICATION, result["classification"])
        self.assertTrue(result["officialMinuteRowsIndependentlyReplayed"])
        self.assertTrue(result["diagnosticFiveMinuteBarsIndependentlyRecomputed"])
        self.assertTrue(result["authenticatedMarketSourceArchiveAvailable"])
        self.assertTrue(result["dualSourceOhlcHashesMatched"])
        self.assertTrue(result["selectedWindowStrictlyBeforeGrant"])
        for field in (
            "upstreamPacketCompletenessAttested",
            "officialFinalizationGuaranteed",
            "officialServerTimeAvailable",
            "officialPaginationCompletenessProven",
            "productionAvailable",
            "networkAvailable",
            "orderAuthorityAvailable",
            "releaseAvailable",
            "promotionEligible",
        ):
            self.assertFalse(result[field], field)

    def test_missing_specialized_archive_is_explicit_diagnostic_blocker(self) -> None:
        bundle, signed_get, grant, _, _ = self._fixture_set()
        result = self.verifier.verify(
            candle_bundle=bundle,
            signed_get_archive=signed_get,
            lane_grant_projection=grant,
            market_source_archive=None,
        )
        self.assertFalse(result["authenticatedMarketSourceArchiveAvailable"])
        self.assertFalse(result["dualSourceOhlcHashesMatched"])
        self.assertIn(
            "AUTHENTICATED_MARKET_SOURCE_ARCHIVE_NOT_AVAILABLE",
            result["blockedReasons"],
        )

    def test_mismatched_source_bar_is_rejected_after_full_resign(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        observation = dual["body"]["sourceObservation"]["body"]
        observation["windowBody"]["bars"][3]["high"] = "99999"
        observation["rawArchive"]["recomputedBars"] = copy.deepcopy(
            observation["windowBody"]["bars"]
        )
        self._resign_source_and_dual(dual)
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "dual-source-ohlc-mismatch"
        ):
            self._verify(bundle, signed_get, grant, dual)

    def test_moving_official_minute_changes_replay_and_rejects_old_source(self) -> None:
        bundle, _, grant, dual, _ = self._fixture_set()
        page = bundle["body"]["pages"][0]
        page["body"]["output2"][0]["stck_prpr"] = page["body"]["output2"][0][
            "stck_hgpr"
        ]
        CandleFixture.rebuild_and_resign_page(page)
        CandleFixture.resign_bundle(bundle)
        signed_get, _ = self._signed_get(bundle)
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "dual-source-ohlc-mismatch"
        ):
            self._verify(bundle, signed_get, grant, dual)

    def test_duplicate_official_minute_timestamp_is_rejected(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        rows = bundle["body"]["pages"][0]["body"]["output2"]
        rows[1]["stck_bsop_date"] = rows[0]["stck_bsop_date"]
        rows[1]["stck_cntg_hour"] = rows[0]["stck_cntg_hour"]
        CandleFixture.rebuild_and_resign_page(bundle["body"]["pages"][0])
        CandleFixture.resign_bundle(bundle)
        with self.assertRaises(KisDomesticFunctionalCandleArchiveBlocked):
            self._verify(bundle, signed_get, grant, dual)

    def test_raw_response_byte_tamper_is_rejected(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        page = bundle["body"]["pages"][0]
        raw = base64.b64decode(page["rawResponseBytesBase64"])
        page["rawResponseBytesBase64"] = base64.b64encode(raw + b" ").decode()
        CandleFixture.resign_page(page)
        CandleFixture.resign_bundle(bundle)
        with self.assertRaises(KisDomesticFunctionalCandleArchiveBlocked):
            self._verify(bundle, signed_get, grant, dual)

    def test_selected_window_must_be_strictly_before_grant(self) -> None:
        bundle, signed_get, grant, dual, result = self._fixture_set()
        grant["body"]["grantWallAt"] = result["diagnosticBars"][-1]["closeAt"]
        self._resign_registry_envelope(
            grant, "LANE_RECORD_VERIFY", archive_module._GRANT_DOMAIN
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "strictly-before-grant"
        ):
            self._verify(bundle, signed_get, grant, dual)

    def test_nested_source_observation_signature_tamper_is_rejected(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        dual["body"]["sourceObservation"]["signature"] = base64.b64encode(
            b"\0" * 64
        ).decode("ascii")
        self._resign_registry_envelope(
            dual,
            "ARCHIVE_EXTRACTION_VERIFY",
            archive_module._DUAL_ARCHIVE_DOMAIN,
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "source-observation-signature-invalid"
        ):
            self._verify(bundle, signed_get, grant, dual)

    def test_market_archive_prefix_head_mismatch_is_rejected(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        prefix = dual["body"]["marketArchivePrefix"]
        prefix["sourceFrameHeadHash"] = sha("wrong-source-frame-head")
        unsigned = dict(prefix)
        unsigned.pop("summaryHash")
        prefix["summaryHash"] = hash_value(unsigned)
        self._resign_registry_envelope(
            dual,
            "ARCHIVE_EXTRACTION_VERIFY",
            archive_module._DUAL_ARCHIVE_DOMAIN,
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "head-join-mismatch"
        ):
            self._verify(bundle, signed_get, grant, dual)

    def test_raw_source_frame_body_tamper_is_rejected(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        observation = dual["body"]["sourceObservation"]["body"]
        observation["rawArchive"]["frames"][0]["body"]["rawFrameHash"] = sha(
            "tampered-raw-frame"
        )
        self._resign_source_and_dual(dual)
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "frame-envelope-hash-mismatch"
        ):
            self._verify(bundle, signed_get, grant, dual)

    def test_exact_outer_archive_schema_rejects_extra_fields(self) -> None:
        bundle, signed_get, grant, dual, _ = self._fixture_set()
        dual["body"]["unexpected"] = True
        self._resign_registry_envelope(
            dual,
            "ARCHIVE_EXTRACTION_VERIFY",
            archive_module._DUAL_ARCHIVE_DOMAIN,
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "fields-not-exact"
        ):
            self._verify(bundle, signed_get, grant, dual)

    def test_registry_verifier_purpose_cannot_be_substituted(self) -> None:
        with self.assertRaisesRegex(
            KisDomesticFunctionalCandleArchiveBlocked, "binding-invalid"
        ):
            KisDomesticFunctionalCandleArchiveVerifier(
                candle_get_verifier=self.candle_verifier,
                signed_get_verifier=self.factory.verifier("SIGNED_GET_CAPTURE_VERIFY"),
                lane_grant_verifier=self.factory.verifier("LANE_RECORD_VERIFY"),
                source_record_verifier=self.factory.verifier("LANE_RECORD_VERIFY"),
                archive_extraction_verifier=self.factory.verifier(
                    "ARCHIVE_EXTRACTION_VERIFY"
                ),
                trusted_clock=lambda: FAKE_NOW,
            )

    def test_status_never_exposes_network_order_or_release_authority(self) -> None:
        status = production_entrypoint_status()
        self.assertEqual(DIAGNOSTIC_CLASSIFICATION, status["classification"])
        self.assertTrue(status["concreteRegistryDerivedVerifierRequired"])
        for field in (
            "officialFinalizationGuaranteed",
            "upstreamPacketCompletenessAttested",
            "productionAvailable",
            "networkAvailable",
            "orderAuthorityAvailable",
            "releaseAvailable",
            "promotionAvailable",
        ):
            self.assertFalse(status[field], field)


if __name__ == "__main__":
    unittest.main()
