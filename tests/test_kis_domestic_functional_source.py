from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_contract import KST, ROUTE
from live_trader.kis_domestic_functional_lane import (
    _BAR_WINDOW_KEYS,
    _NEXT_OPEN_KEYS,
    sign_kis_domestic_lane_capture,
)
from live_trader.kis_domestic_functional_source import (
    KIS_DOMESTIC_FUNCTIONAL_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_SOURCE_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_SOURCE_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_SOURCE_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_SOURCE_TOKEN_AUTHORITY_AVAILABLE,
    SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
    DurableKisDomesticPublicArmJournal,
    KisDomesticFunctionalPublicSource,
    KisDomesticFunctionalSourceBlocked,
    exact_kis_domestic_functional_subscription,
    source_component_status,
)
from trading_runtime.realtime_feeds import (
    KIS_DOMESTIC_TRADE_FIELD_COUNT,
    KisWebSocketClosedBarFeed,
)


SERVER_KEY = b"kis-source-server-authority-key-48-bytes-value!!"
SERVER_KEY_ID = "test-kis-source-authority-key-v1"
GENERATION = "kis-ws-generation-0123456789abcdef0123456789abcdef"
GENERATION_2 = "kis-ws-generation-fedcba9876543210fedcba9876543210"
SOCKET_IDENTITY = "kis-ws-socket-test-public-h0stcnt0-connection-0001"
BASE = datetime(2026, 8, 13, 10, 0, tzinfo=KST)
_TEMP_DIRECTORIES: list[tempfile.TemporaryDirectory] = []


def tearDownModule() -> None:
    while _TEMP_DIRECTORIES:
        _TEMP_DIRECTORIES.pop().cleanup()


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _source_sign(domain: str, body) -> str:
    if domain in {"BAR_WINDOW", "NEXT_OPEN"}:
        return sign_kis_domestic_lane_capture(SERVER_KEY, domain, body)
    return hmac.new(
        SERVER_KEY,
        domain.encode("ascii") + b"\n" + _canonical(body),
        hashlib.sha256,
    ).hexdigest()


def _source_verify(domain: str, body, signature: str) -> bool:
    return type(signature) is str and hmac.compare_digest(
        _source_sign(domain, body), signature
    )


def _journal(path: Path) -> DurableKisDomesticPublicArmJournal:
    return DurableKisDomesticPublicArmJournal(
        path,
        capture_signer=_source_sign,
        capture_verifier=_source_verify,
        server_authority_key_id=SERVER_KEY_ID,
    )


def _frame(
    at: datetime,
    price: str,
    *,
    volume: str = "1",
    pdno: str = "010140",
    tr_id: str = "H0STCNT0",
) -> str:
    local = at.astimezone(KST)
    fields = [""] * KIS_DOMESTIC_TRADE_FIELD_COUNT
    fields[0] = pdno
    fields[1] = local.strftime("%H%M%S")
    fields[2] = price
    fields[12] = volume
    fields[33] = local.strftime("%Y%m%d")
    return f"0|{tr_id}|1|" + "^".join(fields)


def _public_feed(**overrides) -> KisWebSocketClosedBarFeed:
    values = {
        "app_key": "",
        "app_secret": "",
        "demo": False,
    }
    values.update(overrides)
    return KisWebSocketClosedBarFeed(
        (exact_kis_domestic_functional_subscription(),),
        **values,
    )


def _source(**overrides):
    feed = overrides.pop("feed", _public_feed())
    temp = overrides.pop("temp", None)
    journal = overrides.pop("journal", None)
    if journal is None:
        temp = temp or tempfile.TemporaryDirectory()
        _TEMP_DIRECTORIES.append(temp)
        journal = _journal(Path(temp.name) / "public-source.sqlite3")
    values = {
        "feed": feed,
        "journal": journal,
        "capture_signer": _source_sign,
        "generation_factory": lambda: GENERATION,
        "socket_identity_factory": lambda: SOCKET_IDENTITY,
        "owner_token_factory": lambda: b"process-local-owner-token-32-byte-value!!",
        "allow_mock_source": True,
    }
    values.update(overrides)
    source = KisDomesticFunctionalPublicSource(**values)
    source._test_temp_directory = temp
    source.begin_mock_generation(connected_at=BASE - timedelta(seconds=1))
    return source, feed


def _ingest(
    source: KisDomesticFunctionalPublicSource,
    at: datetime,
    price: str,
    *,
    lag: float = 0.25,
    **frame_overrides,
):
    return source.ingest_h0stcnt0_frame(
        _frame(at, price, **frame_overrides),
        received_at=at + timedelta(seconds=lag),
    )


def _natural_observation(*, target_high: str = "109", **source_overrides):
    source, feed = _source(**source_overrides)
    _ingest_natural_window(source, target_high=target_high)
    next_open = BASE + timedelta(minutes=55)
    candidate = _ingest(source, next_open, "107", lag=0.5)
    return source, feed, candidate


def _ingest_natural_window(
    source: KisDomesticFunctionalPublicSource,
    *,
    target_high: str = "109",
) -> None:
    for index in range(10):
        opened = BASE + timedelta(minutes=5 * index)
        for offset, price in ((0, "100"), (60, "95"), (240, "105")):
            self_result = _ingest(
                source, opened + timedelta(seconds=offset), price
            )
            if self_result is not None:
                raise AssertionError("window emitted before its next-open boundary")
    target = BASE + timedelta(minutes=50)
    for offset, price in ((0, "105"), (60, "104"), (240, target_high)):
        result = _ingest(source, target + timedelta(seconds=offset), price)
        if result is not None:
            raise AssertionError("window emitted before its next-open boundary")


class KisDomesticFunctionalPublicSourceTest(unittest.TestCase):
    def test_registry_public_key_identity_is_pem_derived_and_signature_bound(self) -> None:
        first = ECC.generate(curve="Ed25519")
        second = ECC.generate(curve="Ed25519")
        first_public = first.public_key().export_key(format="PEM")
        second_public = second.public_key().export_key(format="PEM")

        def sign(key, domain, body):
            return base64.b64encode(
                eddsa.new(key, mode="rfc8032").sign(
                    domain.encode("ascii") + b"\x00" + _canonical(body)
                )
            ).decode("ascii")

        with tempfile.TemporaryDirectory() as folder:
            journal = DurableKisDomesticPublicArmJournal(
                Path(folder) / "public-source.sqlite3",
                capture_signer=lambda domain, body: sign(first, domain, body),
                server_authority_public_key_pem=first_public,
            )
            self.assertEqual(
                hashlib.sha256(first_public.encode("utf-8")).hexdigest(),
                journal.server_authority_key_id_hash,
            )
            self.assertEqual(
                "REGISTRY_ED25519_PUBLIC_KEY",
                journal.server_authority_identity_mode,
            )
            self.assertFalse(journal.server_authority_offline_mock_only)
            body = {"schemaVersion": "registry-binding-test/v1"}
            journal._verify_signature(
                "REGISTRY_BINDING_TEST",
                body,
                sign(first, "REGISTRY_BINDING_TEST", body),
                "registry-binding-test",
            )

        with tempfile.TemporaryDirectory() as folder:
            substituted = DurableKisDomesticPublicArmJournal(
                Path(folder) / "public-source.sqlite3",
                capture_signer=lambda domain, body: sign(first, domain, body),
                server_authority_public_key_pem=second_public,
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "trusted-signature-mismatch",
            ):
                substituted._verify_signature(
                    "REGISTRY_BINDING_TEST",
                    body,
                    sign(first, "REGISTRY_BINDING_TEST", body),
                    "registry-binding-test",
                )

    def test_server_authority_identity_is_exact_and_legacy_is_mock_only(self) -> None:
        key = ECC.generate(curve="Ed25519")
        public_pem = key.public_key().export_key(format="PEM")
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "server-authority-identity-not-exact",
            ):
                DurableKisDomesticPublicArmJournal(
                    Path(folder) / "both.sqlite3",
                    capture_signer=_source_sign,
                    capture_verifier=_source_verify,
                    server_authority_key_id=SERVER_KEY_ID,
                    server_authority_public_key_pem=public_pem,
                )
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "not-canonical-ed25519",
            ):
                DurableKisDomesticPublicArmJournal(
                    Path(folder) / "private.sqlite3",
                    capture_signer=_source_sign,
                    server_authority_public_key_pem=key.export_key(format="PEM"),
                )
            legacy = DurableKisDomesticPublicArmJournal(
                Path(folder) / "legacy.sqlite3",
                capture_signer=_source_sign,
                capture_verifier=_source_verify,
                server_authority_key_id=SERVER_KEY_ID,
            )
            self.assertEqual(
                "OFFLINE_MOCK_STRING_KEY_ID",
                legacy.server_authority_identity_mode,
            )
            self.assertTrue(legacy.server_authority_offline_mock_only)

    def test_component_is_mock_only_public_and_has_no_account_or_order_surface(self) -> None:
        status = source_component_status()
        self.assertTrue(status["registryEd25519PublicKeyIdentitySupported"])
        self.assertTrue(status["legacyStringKeyIdentityOfflineMockOnly"])
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_SOURCE_PRODUCTION_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_SOURCE_NETWORK_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_SOURCE_TOKEN_AUTHORITY_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_SOURCE_MUTATION_AVAILABLE)
        self.assertTrue(status["armedPublicDataOnly"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["releaseEvidenceEligible"])
        self.assertFalse(status["upstreamExchangeSequenceAvailable"])
        self.assertFalse(status["upstreamPacketCompletenessAttested"])
        self.assertTrue(status["acceptedIngressContinuityOnly"])
        source, _feed = _source()
        snapshot = source.status()
        for key in (
            "accountAuthorityAvailable",
            "tokenAuthorityAvailable",
            "orderAuthorityAvailable",
            "cancelAuthorityAvailable",
            "networkAvailable",
            "productionAvailable",
            "mutationAvailable",
        ):
            self.assertFalse(snapshot[key], key)
        for name in (
            "connect",
            "poll",
            "get",
            "post",
            "delete",
            "request_token",
            "order",
            "cancel",
            "account",
        ):
            self.assertFalse(hasattr(source, name), name)

    def test_constructor_rejects_app_private_account_or_socket_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = _journal(Path(temp) / "public-source.sqlite3")
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "no-private-account-or-app-authority",
            ):
                KisDomesticFunctionalPublicSource(
                    feed=_public_feed(app_key="app", app_secret="secret"),
                    journal=journal,
                    capture_signer=lambda _domain, _body: "a" * 64,
                    allow_mock_source=True,
                )
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "mock-source-flag-required",
            ):
                KisDomesticFunctionalPublicSource(
                    feed=_public_feed(),
                    journal=journal,
                    capture_signer=lambda _domain, _body: "a" * 64,
                )

    def test_exact_h0stcnt0_window_and_next_open_are_lane_ready(self) -> None:
        source, feed, observation = _natural_observation()
        self.assertIsNotNone(observation)
        self.assertEqual("BUY", observation["naturalSignal"])
        self.assertEqual("10", observation["averageRange"])
        self.assertEqual("108", observation["triggerPrice"])
        self.assertTrue(observation["armedPublicDataOnly"])
        self.assertFalse(observation["accountAuthorityAvailable"])
        self.assertFalse(observation["tokenAuthorityAvailable"])
        self.assertFalse(observation["mutationAuthorityAvailable"])
        self.assertFalse(observation["networkAvailable"])
        self.assertFalse(observation["productionAvailable"])

        window_args = source.lane_window_arguments(observation["observationId"])
        window = window_args["window_body"]
        self.assertEqual(_BAR_WINDOW_KEYS, set(window))
        self.assertEqual("KIS_WEBSOCKET_H0STCNT0", window["source"])
        self.assertEqual("kis", window["sourceProvider"])
        self.assertEqual(GENERATION, window["sourceGeneration"])
        self.assertEqual(11, len(window["bars"]))
        self.assertEqual(
            window["sourceEventCount"],
            sum(row["eventCount"] for row in window["bars"]),
        )
        self.assertEqual("1", window["firstSourceSequence"])
        self.assertEqual("33", window["lastSourceSequence"])
        for previous, current in zip(window["bars"], window["bars"][1:]):
            self.assertEqual(previous["closeAt"], current["openAt"])
            self.assertLess(
                int(previous["sourceSequenceEnd"]),
                int(current["sourceSequenceStart"]),
            )
        expected_window_proof = {
            "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
            "route": ROUTE,
            "pdno": "010140",
            "sourceProvider": "kis",
            "sourceGeneration": GENERATION,
            "firstSourceSequence": "1",
            "lastSourceSequence": "33",
            "sourceEventCount": 33,
            "barRawEventChainHashes": [
                row["rawEventChainHash"] for row in window["bars"]
            ],
        }
        self.assertEqual(_hash(expected_window_proof), window["sourceProofHash"])
        self.assertTrue(
            hmac.compare_digest(
                sign_kis_domestic_lane_capture(SERVER_KEY, "BAR_WINDOW", window),
                window_args["server_authority_signature"],
            )
        )

        evaluation_id = "kis-eval-abcdefabcdefabcdefabcdefabcdefab"
        trigger_args = source.lane_next_open_arguments(
            observation["observationId"],
            evaluation_id=evaluation_id,
        )
        trigger = trigger_args["trigger_body"]
        self.assertEqual(_NEXT_OPEN_KEYS, set(trigger))
        self.assertEqual(evaluation_id, trigger["evaluationId"])
        self.assertEqual(GENERATION, trigger["sourceGeneration"])
        self.assertGreater(int(trigger["sourceSequence"]), 33)
        opened = datetime.fromisoformat(trigger["barOpenAt"].replace("Z", "+00:00"))
        observed = datetime.fromisoformat(trigger["observedAt"].replace("Z", "+00:00"))
        self.assertGreaterEqual((observed - opened).total_seconds(), 0)
        self.assertLessEqual((observed - opened).total_seconds(), 2)
        expected_trigger_proof = {
            "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1",
            "route": ROUTE,
            "pdno": "010140",
            "sourceProvider": "kis",
            "sourceGeneration": GENERATION,
            "sourceSequence": trigger["sourceSequence"],
            "rawEventHash": trigger["rawEventHash"],
            "barOpenAt": trigger["barOpenAt"],
            "observedAt": trigger["observedAt"],
        }
        self.assertEqual(_hash(expected_trigger_proof), trigger["sourceProofHash"])
        self.assertTrue(
            hmac.compare_digest(
                sign_kis_domestic_lane_capture(SERVER_KEY, "NEXT_OPEN", trigger),
                trigger_args["server_authority_signature"],
            )
        )
        self.assertIsNone(feed.socket)
        self.assertEqual(34, source.status()["ingestedFrameCount"])

    def test_raw_archive_is_exact_public_window_and_hash_tamper_evident(self) -> None:
        source, _feed, observation = _natural_observation()
        archive = source.raw_archive(observation["observationId"])
        self.assertEqual(33, len(archive["body"]["events"]))
        self.assertEqual(34, len(archive["body"]["frames"]))
        self.assertEqual(_hash(archive["body"]), archive["archiveHash"])
        self.assertEqual(GENERATION, archive["body"]["sourceGeneration"])
        self.assertEqual(
            source.status()["socketIdentityHash"],
            archive["body"]["socketIdentityHash"],
        )
        sequences = [int(row["sourceSequence"]) for row in archive["body"]["events"]]
        self.assertEqual(list(range(1, 34)), sequences)
        self.assertEqual("34", archive["body"]["nextOpenEvent"]["sourceSequence"])
        self.assertFalse(archive["body"]["upstreamExchangeSequenceAvailable"])
        self.assertTrue(archive["body"]["acceptedIngressContinuityOnly"])
        self.assertEqual(
            source.lane_window_arguments(observation["observationId"])[
                "window_body"
            ]["bars"],
            archive["body"]["recomputedBars"],
        )
        for frame_capture in archive["body"]["frames"]:
            frame = frame_capture["body"]
            self.assertEqual(_hash(frame), frame_capture["envelopeHash"])
            self.assertRegex(frame_capture["serverSignature"], r"^[0-9a-f]{64}$")
            self.assertEqual(
                hashlib.sha256(frame["rawFrame"].encode("utf-8")).hexdigest(),
                frame["rawFrameHash"],
            )
        for row in archive["body"]["events"]:
            self.assertEqual(KIS_DOMESTIC_TRADE_FIELD_COUNT, len(row["recordFields"]))
            self.assertRegex(row["rawFrameHash"], r"^[0-9a-f]{64}$")
            self.assertRegex(row["rawEventHash"], r"^[0-9a-f]{64}$")
        tampered = deepcopy(archive["body"])
        tampered["events"][0]["recordFields"][2] = "999999"
        self.assertNotEqual(archive["archiveHash"], _hash(tampered))
        serialized = json.dumps(archive, ensure_ascii=False, sort_keys=True)
        for forbidden in ("CANO", "ACNT_PRDT_CD", "authorization", "access_token"):
            self.assertNotIn(forbidden, serialized)

    def test_durable_raw_events_independently_recompute_all_eleven_bars(self) -> None:
        source, _feed, observation = _natural_observation()
        archive = source.raw_archive(observation["observationId"])["body"]
        window = source.lane_window_arguments(observation["observationId"])[
            "window_body"
        ]
        self.assertEqual(11, len(window["bars"]))
        for bar in window["bars"]:
            rows = [
                row
                for row in archive["events"]
                if row["bucketOpenAt"] == bar["openAt"]
                and row["bucketCloseAt"] == bar["closeAt"]
            ]
            prices = [Decimal(row["recordFields"][2]) for row in rows]
            self.assertEqual(int(bar["eventCount"]), len(rows))
            self.assertEqual(bar["sourceSequenceStart"], rows[0]["sourceSequence"])
            self.assertEqual(bar["sourceSequenceEnd"], rows[-1]["sourceSequence"])
            self.assertEqual(
                [Decimal(bar[key]) for key in ("open", "high", "low", "close")],
                [prices[0], max(prices), min(prices), prices[-1]],
            )
        snapshot = source.status()
        self.assertEqual("NATURAL_BUY_OBSERVED", snapshot["durablePublicArmState"])
        self.assertEqual(34, snapshot["ingestedFrameCount"])
        self.assertEqual(
            archive["captureHeadHash"],
            source.durable_arm_snapshot()["rawHeadHash"],
        )

    def test_hold_window_does_not_emit_natural_buy(self) -> None:
        source, _feed, observation = _natural_observation(target_high="107")
        self.assertIsNone(observation)
        self.assertEqual(11, source.status()["closedWindowSize"])
        self.assertEqual(0, source.status()["observationCount"])

    def test_wrong_channel_symbol_and_late_observation_fail_closed(self) -> None:
        source, _feed = _source()
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "h0stcnt0-frame-required",
        ):
            _ingest(source, BASE, "100", tr_id="H0STCNI0")
        source, _feed = _source()
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "pdno-mismatch",
        ):
            _ingest(source, BASE, "100", pdno="005930")

        source, _feed = _source()
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "outside-two-seconds",
        ):
            _ingest(source, BASE, "100", lag=2.001)

    def test_generation_and_evaluation_binding_are_single_identity(self) -> None:
        source, _feed, observation = _natural_observation()
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "evaluation-id-invalid",
        ):
            source.lane_next_open_arguments(
                observation["observationId"],
                evaluation_id="caller-eval",
            )
        first = source.lane_next_open_arguments(
            observation["observationId"],
            evaluation_id="kis-eval-11111111111111111111111111111111",
        )
        again = source.lane_next_open_arguments(
            observation["observationId"],
            evaluation_id="kis-eval-11111111111111111111111111111111",
        )
        self.assertEqual(first, again)
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "another-evaluation",
        ):
            source.lane_next_open_arguments(
                observation["observationId"],
                evaluation_id="kis-eval-22222222222222222222222222222222",
            )

    def test_new_process_generation_terminalizes_unfinished_public_arm(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            journal = _journal(Path(temp) / "public-source.sqlite3")
            first, _feed = _source(journal=journal)
            first_arm_id = first.status()["publicArmId"]
            _ingest(first, BASE, "100")
            self.assertEqual(
                "ARMED_WAIT_PUBLIC", journal.snapshot(first_arm_id)["state"]
            )

            successor, _feed = _source(
                journal=journal,
                generation_factory=lambda: GENERATION_2,
                socket_identity_factory=lambda: (
                    "kis-ws-socket-test-public-h0stcnt0-connection-0002"
                ),
                owner_token_factory=lambda: (
                    b"different-process-owner-token-32-byte-value"
                ),
            )
            terminated = journal.snapshot(first_arm_id)
            self.assertEqual("TERMINATED_OWNER_LOST", terminated["state"])
            self.assertEqual(
                "SOURCE_OWNER_OR_PROCESS_REPLACED", terminated["terminalReason"]
            )
            self.assertEqual(
                [first_arm_id], successor.status()["terminatedPredecessorArmIds"]
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "no-longer-armed",
            ):
                _ingest(first, BASE + timedelta(seconds=1), "101")
            self.assertEqual(
                "ARMED_WAIT_PUBLIC", successor.status()["durablePublicArmState"]
            )

    def test_duplicate_raw_frame_is_not_retried_and_terminalizes_arm(self) -> None:
        source, _feed = _source()
        raw = _frame(BASE, "100")
        source.ingest_h0stcnt0_frame(
            raw,
            received_at=BASE + timedelta(milliseconds=250),
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "IntegrityError",
        ):
            source.ingest_h0stcnt0_frame(
                raw,
                received_at=BASE + timedelta(milliseconds=250),
            )
        snapshot = source.durable_arm_snapshot()
        self.assertEqual("TERMINATED_FAIL_CLOSED", snapshot["state"])
        self.assertEqual(1, snapshot["rawFrameCount"])
        self.assertEqual(1, snapshot["rawEventCount"])

    def test_raw_frame_commit_precedes_reducer_and_failure_terminalizes(self) -> None:
        source, feed = _source()

        def fail_reducer(*_args, **_kwargs):
            raise RuntimeError("synthetic reducer failure")

        feed._consume_socket_frame = fail_reducer
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "kis-feed-reducer-failed:RuntimeError",
        ):
            _ingest(source, BASE, "100")
        snapshot = source.durable_arm_snapshot()
        self.assertEqual("TERMINATED_FAIL_CLOSED", snapshot["state"])
        self.assertEqual("PUBLIC_INGRESS_OR_REDUCER_EXCEPTION", snapshot["terminalReason"])
        self.assertEqual(1, snapshot["rawFrameCount"])
        self.assertEqual(1, snapshot["rawEventCount"])

    def test_public_trigger_cutoff_is_exactly_1315_kst(self) -> None:
        exact, _feed = _source()
        at_cutoff = BASE.replace(hour=13, minute=15, second=0, microsecond=0)
        _ingest(exact, at_cutoff, "100", lag=0)
        self.assertEqual("ARMED_WAIT_PUBLIC", exact.durable_arm_snapshot()["state"])

        late, _feed = _source()
        with self.assertRaisesRegex(
            KisDomesticFunctionalSourceBlocked,
            "after-armed-deadline",
        ):
            late.ingest_h0stcnt0_frame(
                _frame(at_cutoff, "100"),
                received_at=at_cutoff + timedelta(milliseconds=1),
            )
        self.assertEqual("TERMINATED_FAIL_CLOSED", late.durable_arm_snapshot()["state"])

    def test_durable_observation_tamper_is_rejected_before_lane_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "public-source.sqlite3"
            journal = _journal(path)
            source, _feed, observation = _natural_observation(journal=journal)
            conn = sqlite3.connect(path)
            try:
                row = conn.execute(
                    "SELECT observation_record_json FROM kis_public_source_observation"
                ).fetchone()
                record = json.loads(row[0])
                record["evaluationProof"]["naturalSignal"] = "HOLD"
                conn.execute(
                    "UPDATE kis_public_source_observation SET observation_record_json=?",
                    (_canonical(record).decode("utf-8"),),
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "durable-observation-hash-mismatch",
            ):
                source.lane_window_arguments(observation["observationId"])

    def test_self_consistent_event_price_rehash_cannot_override_signed_frame(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "public-source.sqlite3"
            journal = _journal(path)
            source, _feed = _source(journal=journal)
            _ingest_natural_window(source)
            conn = sqlite3.connect(path)
            try:
                row = conn.execute(
                    """SELECT event_record_json FROM kis_public_source_event
                       WHERE source_sequence=1"""
                ).fetchone()
                event = json.loads(row[0])
                event["recordFields"][2] = "999999"
                raw_event_body = {
                    "schemaVersion": "kis-domestic-h0stcnt0-raw-event/v1",
                    "route": ROUTE,
                    "pdno": "010140",
                    "sourceGeneration": event["sourceGeneration"],
                    "socketIdentityHash": event["socketIdentityHash"],
                    "sourceSequence": event["sourceSequence"],
                    "recordIndex": event["recordIndex"],
                    "rawFrameHash": event["rawFrameHash"],
                    "recordFields": event["recordFields"],
                    "receivedAt": event["receivedAt"],
                }
                event["rawEventHash"] = _hash(raw_event_body)
                conn.execute(
                    """UPDATE kis_public_source_event
                       SET raw_event_hash=?, event_record_json=?, event_record_hash=?
                       WHERE source_sequence=1""",
                    (
                        event["rawEventHash"],
                        _canonical(event).decode("utf-8"),
                        _hash(event),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "authenticated-frame-mismatch",
            ):
                _ingest(source, BASE + timedelta(minutes=55), "107", lag=0.5)
            self.assertEqual(
                "TERMINATED_FAIL_CLOSED",
                source.durable_arm_snapshot()["state"],
            )

    def test_forged_frame_signature_and_head_are_rejected_before_observation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "public-source.sqlite3"
            journal = _journal(path)
            source, _feed = _source(journal=journal)
            _ingest_natural_window(source)
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    """UPDATE kis_public_source_frame
                       SET frame_signature=?, frame_head_hash=? WHERE frame_index=1""",
                    ("f" * 64, "e" * 64),
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "trusted-signature-mismatch",
            ):
                _ingest(source, BASE + timedelta(minutes=55), "107", lag=0.5)

    def test_exact_schema_trigger_and_meta_fingerprint_fail_closed(self) -> None:
        self.assertRegex(SOURCE_JOURNAL_SCHEMA_FINGERPRINT, r"^[0-9a-f]{64}$")
        for mutation, message in (
            (
                "ALTER TABLE kis_public_source_arm ADD COLUMN hostile TEXT",
                "schema-dirty",
            ),
            (
                "UPDATE kis_public_source_schema_meta SET schema_fingerprint='0'",
                "schema-meta-dirty",
            ),
            (
                "DROP TRIGGER kis_public_source_transition_projection_guard",
                "schema-dirty",
            ),
            (
                "CREATE TRIGGER hostile_source_trigger AFTER UPDATE "
                "ON kis_public_source_arm BEGIN SELECT 1; END",
                "schema-dirty",
            ),
        ):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / "public-source.sqlite3"
                journal = _journal(path)
                source, _feed = _source(journal=journal)
                arm_id = source.status()["publicArmId"]
                conn = sqlite3.connect(path)
                try:
                    conn.execute(mutation)
                    conn.commit()
                finally:
                    conn.close()
                with self.assertRaisesRegex(
                    KisDomesticFunctionalSourceBlocked, message
                ):
                    journal.snapshot(arm_id)

    def test_signed_arm_transition_chain_covers_owner_observation_trigger_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "public-source.sqlite3"
            journal = _journal(path)
            source, _feed, observation = _natural_observation(journal=journal)
            arm_id = source.status()["publicArmId"]
            before_trigger = journal.snapshot(arm_id)
            self.assertEqual(2, before_trigger["transitionCount"])
            self.assertRegex(before_trigger["transitionHeadHash"], r"^[0-9a-f]{64}$")
            source.lane_next_open_arguments(
                observation["observationId"],
                evaluation_id="kis-eval-11111111111111111111111111111111",
            )
            after_trigger = journal.snapshot(arm_id)
            self.assertEqual(3, after_trigger["transitionCount"])
            journal.terminalize(
                arm_id=arm_id,
                owner_token_hash=source._owner_token_hash,
                reason="TEST_TERMINAL",
                terminal_at=(BASE + timedelta(minutes=56)).astimezone(
                    timezone.utc
                ).isoformat().replace("+00:00", "Z"),
            )
            terminal = journal.snapshot(arm_id)
            self.assertEqual(4, terminal["transitionCount"])
            self.assertEqual("TERMINATED_FAIL_CLOSED", terminal["state"])
            conn = sqlite3.connect(path)
            try:
                rows = conn.execute(
                    "SELECT sequence,transition_kind,from_state,to_state,"
                    "record_json,record_hash,signature,previous_hash "
                    "FROM kis_public_source_arm_transition ORDER BY sequence"
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual([1, 2, 3, 4], [row[0] for row in rows])
            self.assertEqual(
                [
                    "ARM_CREATED",
                    "NATURAL_BUY_OBSERVATION_SEALED",
                    "NEXT_OPEN_TRIGGER_SEALED",
                    "FAIL_CLOSED_TERMINAL",
                ],
                [row[1] for row in rows],
            )
            previous = "0" * 64
            for row in rows:
                body = json.loads(row[4])
                self.assertEqual(previous, row[7])
                self.assertEqual(_hash(body), row[5])
                self.assertTrue(
                    _source_verify(
                        "PUBLIC_ARM_TRANSITION",
                        {**body, "recordHash": row[5]},
                        row[6],
                    )
                )
                previous = row[5]

    def test_transition_update_delete_and_self_consistent_rewrite_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "public-source.sqlite3"
            journal = _journal(path)
            source, _feed = _source(journal=journal)
            arm_id = source.status()["publicArmId"]
            conn = sqlite3.connect(path)
            try:
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "transition-immutable"
                ):
                    conn.execute(
                        "UPDATE kis_public_source_arm_transition "
                        "SET reason='ATTACK' WHERE arm_id=?",
                        (arm_id,),
                    )
                conn.rollback()
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "transition-immutable"
                ):
                    conn.execute(
                        "DELETE FROM kis_public_source_arm_transition WHERE arm_id=?",
                        (arm_id,),
                    )
                conn.rollback()

                trigger_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' AND "
                    "name='kis_public_source_transition_update_forbidden'"
                ).fetchone()[0]
                conn.execute(
                    "DROP TRIGGER kis_public_source_transition_update_forbidden"
                )
                row = conn.execute(
                    "SELECT record_json FROM kis_public_source_arm_transition "
                    "WHERE arm_id=? AND sequence=1",
                    (arm_id,),
                ).fetchone()
                body = json.loads(row[0])
                body["reason"] = "SELF_CONSISTENT_REWRITE"
                conn.execute(
                    "UPDATE kis_public_source_arm_transition SET record_json=? "
                    "WHERE arm_id=? AND sequence=1",
                    (_canonical(body).decode("utf-8"), arm_id),
                )
                conn.execute(trigger_sql)
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalSourceBlocked,
                "arm-transition-record-hash-mismatch",
            ):
                journal.snapshot(arm_id)


if __name__ == "__main__":
    unittest.main()
