from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import hashlib
import hmac
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from copy import deepcopy

from live_trader.kis_domestic_functional_market_archive import (
    FENCE_SCHEMA,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_RELEASE_AVAILABLE,
    KisDomesticFunctionalMarketArchiveBlocked,
    build_market_source_archive,
    build_market_source_post_observation_archive,
    market_archive_component_status,
    verify_market_source_archive,
    verify_market_source_post_observation_archive,
)
from live_trader import kis_domestic_functional_source as source_module
from live_trader import kis_domestic_functional_market_archive as archive_module
from live_trader.kis_domestic_functional_source import (
    DurableKisDomesticPublicArmJournal,
    KisDomesticFunctionalMarketSourceDurableWriter,
)
from tests.test_kis_domestic_functional_market_source import (
    ACCOUNT,
    AUTHORITY_KEY_ID,
    GENERATION_ONE,
    KEY,
    NOW,
    OWNER_EPOCH_HASH,
    OWNER_EPOCH_ID,
    PROCESS_GENERATION,
    SESSION,
    SOCKET_ONE,
    MarketSourceHarness,
    canonical,
    digest,
    source_sign,
    source_verify,
    signed,
    verify,
    verify_transition,
)


ARM_ID = "kis-public-source-arm-" + "1" * 32
OWNER_TOKEN_HASH = "2" * 64
ARCHIVE_AUTHORITY_KEY_ID_HASH = "a" * 64


def fence_signed(body):
    value_hash = digest(body)
    return {
        **body,
        "fenceHash": value_hash,
        "signature": hmac.new(KEY, value_hash.encode(), hashlib.sha256).hexdigest(),
    }


def fence_verify(candidate) -> bool:
    try:
        value = dict(candidate)
        signature = value.pop("signature")
        value_hash = value.pop("fenceHash")
        return bool(
            hmac.compare_digest(value_hash, digest(value))
            and hmac.compare_digest(
                signature,
                hmac.new(KEY, value_hash.encode(), hashlib.sha256).hexdigest(),
            )
        )
    except Exception:
        return False


def archive_capture_sign(domain, candidate) -> str:
    return hmac.new(
        KEY,
        (domain + "\0" + ARCHIVE_AUTHORITY_KEY_ID_HASH + "\0" + canonical(candidate)).encode(),
        hashlib.sha256,
    ).hexdigest()


def archive_capture_verify(domain, candidate, signature, key_id_hash) -> bool:
    try:
        return bool(
            key_id_hash == ARCHIVE_AUTHORITY_KEY_ID_HASH
            and hmac.compare_digest(
                signature, archive_capture_sign(domain, candidate)
            )
        )
    except Exception:
        return False


class MarketArchiveHarness:
    def __init__(self, *, ingress_count=12):
        self.market = MarketSourceHarness()
        self.journal = DurableKisDomesticPublicArmJournal(
            Path(self.market.temp.name) / "public-source.sqlite3",
            capture_signer=source_sign,
            capture_verifier=source_verify,
            server_authority_key_id="source-server-authority-v1",
        )
        arm_body = {
            "schemaVersion": "kis-domestic-functional-public-arm/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "state": "ARMED_WAIT_PUBLIC",
            "armId": ARM_ID,
            "source": "KIS_WEBSOCKET_H0STCNT0",
            "sourceProvider": "kis",
            "sourceGeneration": GENERATION_ONE,
            "socketIdentityHash": SOCKET_ONE,
            "connectedAt": (NOW - timedelta(seconds=1)).isoformat().replace(
                "+00:00", "Z"
            ),
            "createdAt": (NOW - timedelta(seconds=1)).isoformat().replace(
                "+00:00", "Z"
            ),
            "serverAuthorityKeyIdHash": self.journal.server_authority_key_id_hash,
            "publicMarketDataOnly": True,
            "accountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False,
            "mutationAuthorityAvailable": False,
            "networkAvailable": False,
            "productionAvailable": False,
            "marketSourceSessionId": SESSION,
            "marketSourceAccountFingerprint": ACCOUNT,
            "marketSourceOwnerEpoch": 7,
            "marketSourceOwnerEpochId": OWNER_EPOCH_ID,
            "marketSourceOwnerEpochHash": OWNER_EPOCH_HASH,
            "marketSourceProcessGeneration": PROCESS_GENERATION,
            "marketSourceAuthorityKeyIdHash": AUTHORITY_KEY_ID,
        }
        self.journal.begin_arm(
            arm_record=arm_body,
            arm_signature=source_sign("PUBLIC_ARM", arm_body),
            owner_token_hash=OWNER_TOKEN_HASH,
        )
        self.adapter = KisDomesticFunctionalMarketSourceDurableWriter(
            journal=self.journal,
            arm_id=ARM_ID,
            owner_token_hash=OWNER_TOKEN_HASH,
            market_record_verifier=verify,
            trusted_clock=lambda: self.market.now,
        )
        self.market.source = self.market.build(
            writer=self.adapter,
            ack_verifier=self.adapter.verify_ack,
        )
        self.market.source.begin_generation(self.market.handshake())
        for index in range(ingress_count):
            self.market.now = NOW + timedelta(minutes=5 * index)
            prior = self.market.source.snapshot(GENERATION_ONE)["ingressHeadHash"]
            self.market.source.ingest_signed_frame(
                self.market.raw(ordinal=index + 1, previous=prior)
            )
        self.destination = Path(self.market.temp.name) / "market-archive.sqlite3"
        self.post_destination = (
            Path(self.market.temp.name) / "market-post-archive.sqlite3"
        )
        self.fence_overrides = {}
        self.fence_entered = 0
        self.fence_exited = 0

    def close(self):
        self.market.close()

    def fence_body(self):
        body = {
            "schemaVersion": FENCE_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "sourceGeneration": GENERATION_ONE,
            "armId": ARM_ID,
            "ownerEpochId": OWNER_EPOCH_ID,
            "ownerEpochHash": OWNER_EPOCH_HASH,
            "routeFenceRevision": 9,
            "observedAt": self.market.now.isoformat(),
            "routeLockHeld": True,
            "accountAuthorityAvailable": False,
            "mutationAuthorityAvailable": False,
            "productionAvailable": False,
        }
        body.update(self.fence_overrides)
        return fence_signed(body)

    def fence(self):
        @contextmanager
        def held():
            self.fence_entered += 1
            try:
                yield self.fence_body()
            finally:
                self.fence_exited += 1
        return held()

    def build(self, **overrides):
        values = {
            "market_database": self.market.ledger.path,
            "source_database": self.journal.path,
            "destination": self.destination,
            "source_generation": GENERATION_ONE,
            "arm_id": ARM_ID,
            "observation_fence": self.fence,
            "fence_verifier": fence_verify,
            "market_verifiers": {
                "handshake": verify,
                "raw": verify,
                "ack": self.adapter.verify_ack,
                "reducer": verify,
            },
            "transition_verifier": verify_transition,
            "source_verifier": source_verify,
            "archive_capture_signer": archive_capture_sign,
            "archive_capture_verifier": archive_capture_verify,
            "archive_authority_key_id_hash": ARCHIVE_AUTHORITY_KEY_ID_HASH,
            "trusted_clock": lambda: self.market.now,
        }
        values.update(overrides)
        return build_market_source_archive(**values)

    def verify(self, result, **overrides):
        values = {
            "path": self.destination,
            "expected_file_hash": result["archiveFileHash"],
            "source_generation": GENERATION_ONE,
            "arm_id": ARM_ID,
            "fence_verifier": fence_verify,
            "market_verifiers": {
                "handshake": verify,
                "raw": verify,
                "ack": self.adapter.verify_ack,
                "reducer": verify,
            },
            "transition_verifier": verify_transition,
            "source_verifier": source_verify,
            "archive_capture_verifier": archive_capture_verify,
            "expected_archive_authority_key_id_hash": (
                ARCHIVE_AUTHORITY_KEY_ID_HASH
            ),
        }
        values.update(overrides)
        return verify_market_source_archive(**values)

    def ingest_next(self):
        snapshot = self.market.source.snapshot(GENERATION_ONE)
        ordinal = int(snapshot["lastIngressOrdinal"]) + 1
        self.market.now = NOW + timedelta(minutes=5 * (ordinal - 1))
        self.market.source.ingest_signed_frame(
            self.market.raw(
                ordinal=ordinal,
                previous=snapshot["ingressHeadHash"],
            )
        )

    def seal_observation_and_trigger(self):
        conn = sqlite3.connect(self.journal.path)
        conn.row_factory = sqlite3.Row
        try:
            events = [
                json.loads(row[0])
                for row in conn.execute(
                    "SELECT event_record_json FROM kis_public_source_event "
                    "ORDER BY source_sequence"
                )
            ]
        finally:
            conn.close()
        if len(events) != 12:
            raise AssertionError("post observation fixture requires 12 events")
        window_events = events[:11]
        boundary_event = events[11]
        bars = source_module._independent_bars_from_events(window_events)
        source_proof = {
            "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "sourceProvider": "kis",
            "sourceGeneration": GENERATION_ONE,
            "firstSourceSequence": "1",
            "lastSourceSequence": "11",
            "sourceEventCount": 11,
            "barRawEventChainHashes": [
                item["rawEventChainHash"] for item in bars
            ],
        }
        window = {
            "schemaVersion": "kis-domestic-official-5m-window/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "origin": source_module.LIVE_ORIGIN,
            "pdno": "010140",
            "source": "KIS_WEBSOCKET_H0STCNT0",
            "sourceProvider": "kis",
            "sourceGeneration": GENERATION_ONE,
            "firstSourceSequence": "1",
            "lastSourceSequence": "11",
            "sourceEventCount": 11,
            "sourceProofHash": digest(source_proof),
            "interval": "5m",
            "artifactContentHash": source_module.APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": source_module.APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": source_module.APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": source_module.APPROVED_INSTANCE_FILE_SHA256,
            "bars": bars,
            "observedAt": boundary_event["receivedAt"],
        }
        raw_archive = self.journal.window_archive(
            arm_id=ARM_ID,
            owner_token_hash=OWNER_TOKEN_HASH,
            first_sequence=1,
            last_sequence=11,
            next_open_sequence=12,
            expected_bars=bars,
            expected_source_proof_hash=window["sourceProofHash"],
        )
        arm = self.journal.snapshot(ARM_ID)
        observation_id = "kis-source-observation-" + "c" * 32
        evaluation = {
            "schemaVersion": "kis-domestic-natural-breakout-evaluation-proof/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "armId": ARM_ID,
            "sourceGeneration": GENERATION_ONE,
            "socketIdentityHash": SOCKET_ONE,
            "windowHash": digest(window),
            "rawArchiveHash": digest(raw_archive),
            "strategy": "KIS_DOMESTIC_VOLATILITY_BREAKOUT_10X0.3",
            "priorBarCount": 10,
            "averageRange": "0",
            "breakoutMultiplier": "0.3",
            "priorClose": "80000",
            "currentHigh": "80000",
            "triggerPrice": "80000",
            "naturalSignal": "BUY",
            "barCloseAt": bars[-1]["closeAt"],
            "nextOpenAt": boundary_event["bucketOpenAt"],
            "nextOpenObservedAt": boundary_event["receivedAt"],
        }
        boundary = {
            "barOpenAt": boundary_event["bucketOpenAt"],
            "observedAt": boundary_event["receivedAt"],
            "openPriceKrw": "80000",
            "sourceSequence": "12",
            "rawEventHash": boundary_event["rawEventHash"],
        }
        observation = {
            "schemaVersion": (
                "kis-domestic-functional-source-observation-record/v1"
            ),
            "observationId": observation_id,
            "armId": ARM_ID,
            "sourceGeneration": GENERATION_ONE,
            "socketIdentityHash": SOCKET_ONE,
            "captureHeadHash": arm["rawHeadHash"],
            "windowBody": window,
            "windowSignature": source_sign("BAR_WINDOW", window),
            "rawArchive": raw_archive,
            "rawArchiveHash": digest(raw_archive),
            "evaluationProof": evaluation,
            "evaluationProofHash": digest(evaluation),
            "evaluationSignature": source_sign(
                "NATURAL_BREAKOUT_EVALUATION", evaluation
            ),
            "averageRange": "0",
            "triggerPrice": "80000",
            "naturalSignal": "BUY",
            "boundary": boundary,
        }
        self.journal.seal_observation(
            arm_id=ARM_ID,
            owner_token_hash=OWNER_TOKEN_HASH,
            observation_record=observation,
            observation_signature=source_sign("SOURCE_OBSERVATION", observation),
            created_at=boundary["observedAt"],
        )
        evaluation_id = "kis-eval-" + "d" * 32
        trigger_proof = {
            "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "sourceProvider": "kis",
            "sourceGeneration": GENERATION_ONE,
            "sourceSequence": "12",
            "rawEventHash": boundary_event["rawEventHash"],
            "barOpenAt": boundary_event["bucketOpenAt"],
            "observedAt": boundary_event["receivedAt"],
        }
        trigger = {
            "schemaVersion": "kis-domestic-next-open-trigger/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "source": "KIS_WEBSOCKET",
            "sourceProvider": "kis",
            "sourceGeneration": GENERATION_ONE,
            "sourceSequence": "12",
            "rawEventHash": boundary_event["rawEventHash"],
            "sourceProofHash": digest(trigger_proof),
            "eventType": "NEXT_BAR_OPEN",
            "evaluationId": evaluation_id,
            "barOpenAt": boundary_event["bucketOpenAt"],
            "observedAt": boundary_event["receivedAt"],
            "openPriceKrw": "80000",
        }
        self.journal.seal_trigger(
            arm_id=ARM_ID,
            owner_token_hash=OWNER_TOKEN_HASH,
            observation_id=observation_id,
            evaluation_id=evaluation_id,
            trigger_record=trigger,
            trigger_signature=source_sign("NEXT_OPEN", trigger),
            updated_at=boundary["observedAt"],
        )
        return observation_id

    def build_post(self, pre_result, observation_id, **overrides):
        values = {
            "market_database": self.market.ledger.path,
            "source_database": self.journal.path,
            "destination": self.post_destination,
            "pre_observation_archive": self.destination,
            "pre_observation_file_hash": pre_result["archiveFileHash"],
            "pre_observation_capture_hash": pre_result["captureHash"],
            "source_generation": GENERATION_ONE,
            "arm_id": ARM_ID,
            "observation_id": observation_id,
            "observation_fence": self.fence,
            "fence_verifier": fence_verify,
            "market_verifiers": {
                "handshake": verify,
                "raw": verify,
                "ack": self.adapter.verify_ack,
                "reducer": verify,
            },
            "transition_verifier": verify_transition,
            "source_verifier": source_verify,
            "archive_capture_signer": archive_capture_sign,
            "archive_capture_verifier": archive_capture_verify,
            "archive_authority_key_id_hash": ARCHIVE_AUTHORITY_KEY_ID_HASH,
            "trusted_clock": lambda: self.market.now,
        }
        values.update(overrides)
        return build_market_source_post_observation_archive(**values)

    def verify_post(self, pre_result, post_result, observation_id, **overrides):
        values = {
            "path": self.post_destination,
            "expected_file_hash": post_result["archiveFileHash"],
            "pre_observation_archive": self.destination,
            "pre_observation_file_hash": pre_result["archiveFileHash"],
            "pre_observation_capture_hash": pre_result["captureHash"],
            "source_generation": GENERATION_ONE,
            "arm_id": ARM_ID,
            "observation_id": observation_id,
            "fence_verifier": fence_verify,
            "market_verifiers": {
                "handshake": verify,
                "raw": verify,
                "ack": self.adapter.verify_ack,
                "reducer": verify,
            },
            "transition_verifier": verify_transition,
            "source_verifier": source_verify,
            "archive_capture_verifier": archive_capture_verify,
            "expected_archive_authority_key_id_hash": (
                ARCHIVE_AUTHORITY_KEY_ID_HASH
            ),
        }
        values.update(overrides)
        return verify_market_source_post_observation_archive(**values)


class KisDomesticFunctionalMarketArchiveTests(unittest.TestCase):
    def test_post_observation_archive_is_exact_predecessor_prefix_extension(self):
        fixture = MarketArchiveHarness(ingress_count=11)
        try:
            pre = fixture.build()
            fixture.ingest_next()
            observation_id = fixture.seal_observation_and_trigger()
            post = fixture.build_post(pre, observation_id)
            verified = fixture.verify_post(pre, post, observation_id)
            prefix = verified["prefixExtensionSummary"]
            self.assertEqual(11, prefix["preMarketIngressCount"])
            self.assertEqual(12, prefix["postMarketIngressCount"])
            self.assertTrue(prefix["immutablePredecessorRowsByteIdentical"])
            self.assertTrue(prefix["countsAndHeadsExtendContiguously"])
            self.assertTrue(prefix["sameArmGenerationSocketOwnerAndKeys"])
            self.assertTrue(prefix["observationAndTriggerExactJoined"])
            self.assertTrue(verified["postObservationPrefixExtensionProven"])
            self.assertFalse(verified["externalAsymmetricArchiveAuthorityPinned"])
            self.assertFalse(verified["releaseCompletenessProven"])
        finally:
            fixture.close()

    def test_prefix_consumer_rejects_rewrite_fork_and_drop(self):
        fixture = MarketArchiveHarness(ingress_count=11)
        try:
            pre = fixture.build()
            fixture.ingest_next()
            observation_id = fixture.seal_observation_and_trigger()
            post = fixture.build_post(pre, observation_id)
            before = archive_module._load_archive_snapshot(
                fixture.destination,
                expected_file_hash=pre["archiveFileHash"],
            )
            after = archive_module._load_archive_snapshot(
                fixture.post_destination,
                expected_file_hash=post["archiveFileHash"],
            )
            cases = []
            rewritten = deepcopy(after)
            rewritten["SOURCE"]["kis_public_source_event"][0][
                "event_record_hash"
            ] = "f" * 64
            cases.append(rewritten)
            forked = deepcopy(after)
            forked["MARKET_SOURCE"][
                "kis_functional_market_source_transition"
            ][0]["record_hash"] = "e" * 64
            cases.append(forked)
            dropped = deepcopy(after)
            dropped["SOURCE"]["kis_public_source_frame"].pop(0)
            cases.append(dropped)
            for changed in cases:
                with self.subTest(kind=len(cases)):
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalMarketArchiveBlocked,
                        "post-prefix-",
                    ):
                        archive_module._verify_prefix_extension(
                            before,
                            changed,
                            source_generation=GENERATION_ONE,
                            arm_id=ARM_ID,
                        )
        finally:
            fixture.close()

    def test_post_archive_rejects_extra_generation_history(self):
        fixture = MarketArchiveHarness(ingress_count=11)
        try:
            pre = fixture.build()
            fixture.ingest_next()
            observation_id = fixture.seal_observation_and_trigger()
            fixture.market.now += timedelta(seconds=1)
            fixture.market.source.begin_generation(
                fixture.market.handshake(
                    generation="kis-ws-generation-" + "2" * 32,
                    socket="b" * 64,
                )
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "producer-route-cardinality-not-exact",
            ):
                fixture.build_post(pre, observation_id)
        finally:
            fixture.close()

    def test_post_archive_rejects_acked_not_reduced_ingress(self):
        fixture = MarketArchiveHarness(ingress_count=11)
        try:
            pre = fixture.build()
            fixture.ingest_next()
            observation_id = fixture.seal_observation_and_trigger()
            conn = fixture.market.ledger.connect()
            try:
                trigger_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' AND "
                    "name='kis_functional_market_source_ingress_projection_guard'"
                ).fetchone()[0]
                conn.execute(
                    "DROP TRIGGER "
                    "kis_functional_market_source_ingress_projection_guard"
                )
                conn.execute(
                    "UPDATE kis_functional_market_source_ingress SET "
                    "state='ACKED',reducer_receipt_json='',"
                    "reducer_receipt_hash='',reducer_authority_key_id_hash='' "
                    "WHERE ingress_ordinal=12"
                )
                conn.execute(trigger_sql)
                conn.commit()
            finally:
                conn.close()
            with self.assertRaises(KisDomesticFunctionalMarketArchiveBlocked):
                fixture.build_post(pre, observation_id)
        finally:
            fixture.close()

    def test_post_archive_rejects_observation_trigger_projection_tamper(self):
        fixture = MarketArchiveHarness(ingress_count=11)
        try:
            pre = fixture.build()
            fixture.ingest_next()
            observation_id = fixture.seal_observation_and_trigger()
            conn = sqlite3.connect(fixture.journal.path)
            try:
                row = conn.execute(
                    "SELECT trigger_record_json FROM "
                    "kis_public_source_observation"
                ).fetchone()
                trigger = json.loads(row[0])
                trigger["sourceSequence"] = "13"
                conn.execute(
                    "UPDATE kis_public_source_observation SET "
                    "trigger_record_json=?,trigger_record_hash=?",
                    (canonical(trigger), digest(trigger)),
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaises(KisDomesticFunctionalMarketArchiveBlocked):
                fixture.build_post(pre, observation_id)
        finally:
            fixture.close()

    def test_atomic_two_ledger_archive_and_independent_replay(self):
        fixture = MarketArchiveHarness()
        try:
            result = fixture.build()
            verified = fixture.verify(result)
            summary = verified["replaySummary"]
            self.assertEqual(12, summary["marketIngressCount"])
            self.assertEqual(12, summary["sourceFrameCount"])
            self.assertEqual(12, summary["sourceEventCount"])
            self.assertTrue(summary["allRawFramesReparsed46Fields"])
            self.assertTrue(summary["marketSourceBijectionVerified"])
            self.assertTrue(summary["allProducerRowProjectionsExact"])
            self.assertTrue(summary["allObservationRowsIndependentlyReplayed"])
            self.assertEqual(0, summary["sourceObservationCount"])
            self.assertEqual(1, fixture.fence_entered)
            self.assertEqual(1, fixture.fence_exited)
            self.assertTrue(result["atomicCreateIfAbsent"])
            self.assertTrue(result["fileAndParentSynced"])
        finally:
            fixture.close()

    def test_capture_seals_before_after_hashes_heads_counts_and_fence(self):
        fixture = MarketArchiveHarness()
        try:
            result = fixture.build()
            conn = sqlite3.connect(fixture.destination); conn.row_factory = sqlite3.Row
            try:
                capture = json.loads(conn.execute(
                    "SELECT capture_json FROM kis_market_archive_capture"
                ).fetchone()[0])
            finally:
                conn.close()
            self.assertEqual(
                capture["marketDatabaseBundleHashBefore"],
                capture["marketDatabaseBundleHashAfter"],
            )
            self.assertEqual(
                capture["sourceDatabaseBundleHashBefore"],
                capture["sourceDatabaseBundleHashAfter"],
            )
            self.assertEqual(
                capture["logicalSnapshotHashBefore"],
                capture["logicalSnapshotHashAfter"],
            )
            self.assertTrue(capture["atomicRouteOwnerObservationFenceHeld"])
            self.assertTrue(capture["freshDedicatedProducerDatabasesRequired"])
            self.assertEqual(
                ARCHIVE_AUTHORITY_KEY_ID_HASH,
                capture["archiveAuthorityKeyIdHash"],
            )
            self.assertEqual(result["captureHash"], digest(capture))
        finally:
            fixture.close()

    def test_stale_wrong_owner_or_unlocked_fence_rejects_before_publish(self):
        cases = (
            {"routeLockHeld": False},
            {"ownerEpochHash": "f" * 64},
            {"observedAt": (NOW - timedelta(seconds=3)).isoformat()},
        )
        for changed in cases:
            with self.subTest(changed=changed):
                fixture = MarketArchiveHarness()
                try:
                    fixture.fence_overrides = changed
                    with self.assertRaises(KisDomesticFunctionalMarketArchiveBlocked):
                        fixture.build()
                    self.assertFalse(fixture.destination.exists())
                finally:
                    fixture.close()

    def test_extra_producer_schema_object_is_fail_closed(self):
        fixture = MarketArchiveHarness()
        try:
            conn = sqlite3.connect(fixture.journal.path)
            try:
                conn.execute("CREATE TABLE kis_public_source_evil(value TEXT)")
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "producer-schema-invalid",
            ):
                fixture.build()
        finally:
            fixture.close()

    def test_market_source_cross_link_tamper_is_rejected(self):
        fixture = MarketArchiveHarness()
        try:
            conn = sqlite3.connect(fixture.journal.path)
            try:
                row = conn.execute(
                    "SELECT frame_record_json FROM kis_public_source_frame "
                    "WHERE frame_index=1"
                ).fetchone()
                body = json.loads(row[0])
                body["marketSourceRawRecordHash"] = "f" * 64
                conn.execute(
                    "UPDATE kis_public_source_frame SET frame_record_json=?,"
                    "frame_record_hash=? WHERE frame_index=1",
                    (canonical(body), digest(body)),
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaises(KisDomesticFunctionalMarketArchiveBlocked):
                fixture.build()
        finally:
            fixture.close()

    def test_validly_resigned_wrong_handshake_semantics_are_rejected(self):
        cases = (
            ("websocketUrl", "ws://evil.invalid/tryitout"),
            ("publicMarketDataOnly", False),
            ("ackTrKey", "999999"),
        )
        for key, wrong in cases:
            with self.subTest(key=key):
                fixture = MarketArchiveHarness()
                try:
                    conn = fixture.market.ledger.connect()
                    try:
                        trigger_sql = conn.execute(
                            "SELECT sql FROM sqlite_master WHERE type='trigger' "
                            "AND name='kis_functional_market_source_generation_identity_immutable'"
                        ).fetchone()[0]
                        row = conn.execute(
                            "SELECT handshake_json FROM "
                            "kis_functional_market_source_generation"
                        ).fetchone()
                        body = json.loads(row[0])
                        body.pop("handshakeHash")
                        body.pop("signature")
                        body[key] = wrong
                        resigned = signed(body, "handshakeHash")
                        conn.execute(
                            "DROP TRIGGER "
                            "kis_functional_market_source_generation_identity_immutable"
                        )
                        conn.execute(
                            "UPDATE kis_functional_market_source_generation SET "
                            "handshake_json=?,handshake_hash=?,handshake_signature=?",
                            (
                                canonical(resigned),
                                resigned["handshakeHash"],
                                resigned["signature"],
                            ),
                        )
                        conn.execute(trigger_sql)
                        conn.commit()
                    finally:
                        conn.close()
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalMarketArchiveBlocked,
                        "market-handshake-row-projection-invalid",
                    ):
                        fixture.build()
                finally:
                    fixture.close()

    def test_validly_resigned_wrong_source_arm_cross_binding_is_rejected(self):
        cases = (
            ("marketSourceAccountFingerprint", "f" * 64),
            ("marketSourceOwnerEpoch", 8),
            (
                "marketSourceProcessGeneration",
                "kis-market-source-process-" + "f" * 32,
            ),
        )
        for key, wrong in cases:
            with self.subTest(key=key):
                fixture = MarketArchiveHarness()
                try:
                    conn = sqlite3.connect(fixture.journal.path)
                    try:
                        row = conn.execute(
                            "SELECT arm_record_json FROM kis_public_source_arm"
                        ).fetchone()
                        body = json.loads(row[0])
                        body[key] = wrong
                        signature = source_sign("PUBLIC_ARM", body)
                        conn.execute(
                            "UPDATE kis_public_source_arm SET arm_record_json=?,"
                            "arm_record_hash=?,arm_signature=?",
                            (canonical(body), digest(body), signature),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalMarketArchiveBlocked,
                        "source-arm-market-binding-invalid",
                    ):
                        fixture.build()
                finally:
                    fixture.close()

    def test_missing_reducer_truth_is_rejected(self):
        fixture = MarketArchiveHarness()
        try:
            conn = fixture.market.ledger.connect()
            try:
                conn.execute(
                    "UPDATE kis_functional_market_source_ingress SET "
                    "reducer_receipt_json='' WHERE ingress_ordinal=1"
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaises(KisDomesticFunctionalMarketArchiveBlocked):
                fixture.build()
        finally:
            fixture.close()

    def test_existing_destination_and_concurrent_race_never_overwrite(self):
        fixture = MarketArchiveHarness()
        try:
            fixture.destination.write_bytes(b"winner")
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "destination-exists",
            ):
                fixture.build()
            self.assertEqual(b"winner", fixture.destination.read_bytes())
        finally:
            fixture.close()

        fixture = MarketArchiveHarness()
        try:
            original_fence = fixture.fence

            def racing_fence():
                @contextmanager
                def held():
                    with original_fence() as evidence:
                        fixture.destination.write_bytes(b"concurrent-winner")
                        yield evidence
                return held()

            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "destination-race-lost",
            ):
                fixture.build(observation_fence=racing_fence)
            self.assertEqual(b"concurrent-winner", fixture.destination.read_bytes())
        finally:
            fixture.close()

    def test_archive_file_or_row_tamper_is_rejected(self):
        fixture = MarketArchiveHarness()
        try:
            result = fixture.build()
            with fixture.destination.open("ab") as handle:
                handle.write(b"tamper")
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "archive-file-drift",
            ):
                fixture.verify(result)
        finally:
            fixture.close()

    def test_wrong_archive_verifiers_cannot_reendorse_records(self):
        fixture = MarketArchiveHarness()
        try:
            result = fixture.build()
            with self.assertRaises(KisDomesticFunctionalMarketArchiveBlocked):
                fixture.verify(result, source_verifier=lambda *_args: False)
            with self.assertRaises(KisDomesticFunctionalMarketArchiveBlocked):
                fixture.verify(
                    result,
                    archive_capture_verifier=lambda *_args: False,
                )
        finally:
            fixture.close()

    def test_clock_is_sampled_inside_fence_and_rollback_is_rejected(self):
        fixture = MarketArchiveHarness()
        try:
            samples = iter((fixture.market.now, fixture.market.now + timedelta(seconds=1)))

            def clock():
                self.assertEqual(1, fixture.fence_entered)
                self.assertEqual(0, fixture.fence_exited)
                return next(samples)

            fixture.build(trusted_clock=clock)
        finally:
            fixture.close()
        fixture = MarketArchiveHarness()
        try:
            samples = iter((fixture.market.now, fixture.market.now - timedelta(seconds=1)))
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "trusted-clock-regressed",
            ):
                fixture.build(trusted_clock=lambda: next(samples))
        finally:
            fixture.close()

    def test_retained_generation_history_and_observation_rows_are_rejected(self):
        fixture = MarketArchiveHarness()
        try:
            fixture.market.now += timedelta(seconds=1)
            fixture.market.source.begin_generation(
                fixture.market.handshake(
                    generation="kis-ws-generation-" + "2" * 32,
                    socket="b" * 64,
                )
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "producer-route-cardinality-not-exact",
            ):
                fixture.build()
        finally:
            fixture.close()

        fixture = MarketArchiveHarness()
        try:
            body = {"schemaVersion": "hostile-observation/v1"}
            conn = sqlite3.connect(fixture.journal.path)
            try:
                conn.execute(
                    "INSERT INTO kis_public_source_observation "
                    "(observation_id,arm_id,state,observation_record_json,"
                    "observation_record_hash,observation_signature,created_at,"
                    "updated_at,revision) VALUES(?,?,?,?,?,?,?,?,0)",
                    (
                        "kis-source-observation-" + "f" * 32,
                        ARM_ID,
                        "NATURAL_BUY_OBSERVED",
                        canonical(body),
                        digest(body),
                        "f" * 64,
                        fixture.market.now.isoformat(),
                        fixture.market.now.isoformat(),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketArchiveBlocked,
                "observation-table-not-empty",
            ):
                fixture.build()
        finally:
            fixture.close()

    def test_all_network_mutation_production_release_flags_remain_false(self):
        status = market_archive_component_status()
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_PRODUCTION_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_NETWORK_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_MUTATION_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_RELEASE_AVAILABLE)
        self.assertFalse(status["externalAsymmetricArchiveAuthorityPinned"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["mutationAvailable"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["releaseAvailable"])


if __name__ == "__main__":
    unittest.main()
