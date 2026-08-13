from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_market_source import (
    ACK_SCHEMA,
    APPROVAL_ENDPOINT,
    HANDSHAKE_SCHEMA,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_DUAL_SOURCE_CONFIRMATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_NETWORK_EXECUTOR_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_RELEASE_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_UPSTREAM_COMPLETENESS_AVAILABLE,
    LIVE_ORIGIN,
    LIVE_WEBSOCKET_URL,
    OWNER_SCHEMA,
    RAW_RECORD_SCHEMA,
    REDUCER_SCHEMA,
    DisabledKisDomesticFunctionalMarketSource,
    KisDomesticFunctionalMarketSourceBlocked,
    market_source_component_status,
)
from live_trader.program_ledger import ProgramLedger
from live_trader.kis_domestic_functional_source import (
    DurableKisDomesticPublicArmJournal,
    KisDomesticFunctionalMarketSourceDurableWriter,
    _independent_bars_from_events,
)


KEY = b"kis-market-source-offline-authority-key-at-least-32-bytes"
NOW = datetime(2026, 8, 14, 3, 5, 1, tzinfo=timezone.utc)
SESSION = "kis-market-source-session-one"
ACCOUNT = "a" * 64
OWNER_EPOCH_ID = "kis-owner-epoch-source-one"
OWNER_EPOCH_HASH = "b" * 64
PROCESS_GENERATION = "kis-market-source-process-" + "c" * 32
GENERATION_ONE = "kis-ws-generation-" + "d" * 32
GENERATION_TWO = "kis-ws-generation-" + "e" * 32
SOCKET_ONE = "1" * 64
SOCKET_TWO = "2" * 64
AUTHORITY_KEY_ID = "3" * 64
SOURCE_AUTHORITY_KEY_ID = "7" * 64
TRANSITION_AUTHORITY_KEY_ID = "8" * 64


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def signed(body: dict, hash_key: str) -> dict:
    value_hash = digest(body)
    return {
        **body,
        hash_key: value_hash,
        "signature": hmac.new(KEY, value_hash.encode(), hashlib.sha256).hexdigest(),
    }


def verify(candidate) -> bool:
    try:
        value = dict(candidate)
        signature = value.pop("signature")
        hash_keys = [
            key
            for key in (
                "handshakeHash", "snapshotHash", "recordHash", "ackHash",
                "receiptHash",
            )
            if key in value
        ]
        if len(hash_keys) != 1:
            return False
        value_hash = value.pop(hash_keys[0])
        return bool(
            hmac.compare_digest(value_hash, digest(value))
            and hmac.compare_digest(
                signature,
                hmac.new(KEY, value_hash.encode(), hashlib.sha256).hexdigest(),
            )
        )
    except Exception:
        return False


def sign_transition(domain, body) -> str:
    return hmac.new(
        KEY, (domain + "\0" + canonical(body)).encode(), hashlib.sha256
    ).hexdigest()


def verify_transition(domain, body, signature, key_id_hash) -> bool:
    return bool(
        key_id_hash == TRANSITION_AUTHORITY_KEY_ID
        and hmac.compare_digest(signature, sign_transition(domain, body))
    )


def source_sign(domain, body) -> str:
    return hmac.new(
        KEY, (domain + "\0" + canonical(body)).encode(), hashlib.sha256
    ).hexdigest()


def source_verify(domain, body, signature) -> bool:
    try:
        return hmac.compare_digest(signature, source_sign(domain, body))
    except Exception:
        return False


def subscription_hash() -> str:
    return digest(
        {
            "header": {
                "content-type": "utf-8",
                "custtype": "P",
                "tr_type": "1",
            },
            "body": {"input": {"tr_id": "H0STCNT0", "tr_key": "010140"}},
        }
    )


def frame(*, price: str = "80000", date: str = "20260814", clock="120501") -> str:
    fields = ["0"] * 46
    fields[0] = "010140"
    fields[1] = clock
    fields[2] = price
    fields[12] = "1"
    fields[33] = date
    return "0|H0STCNT0|1|" + "^".join(fields)


class MarketSourceHarness:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.ledger = ProgramLedger(Path(self.temp.name) / "program.sqlite3")
        self.current_generation = GENERATION_ONE
        self.current_socket = SOCKET_ONE
        self.now = NOW
        self.owner_epoch = 7
        self.owner_hash = OWNER_EPOCH_HASH
        self.order: list[str] = []
        self.writer_mode = "OK"
        self.reducer_mode = "OK"
        self.writer_calls = 0
        self.reducer_calls = 0
        self.source = self.build()

    def close(self):
        self.temp.cleanup()

    def owner(self):
        body = {
            "schemaVersion": OWNER_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "sessionId": SESSION,
            "accountFingerprint": ACCOUNT,
            "ownerEpoch": self.owner_epoch,
            "ownerEpochId": OWNER_EPOCH_ID,
            "ownerEpochHash": self.owner_hash,
            "processGeneration": PROCESS_GENERATION,
            "statusRevision": 3,
            "statusHeadHash": "4" * 64,
            "observedAt": self.now.isoformat(),
            "authorityFresh": True,
            "hazardousAuthorityOpen": False,
            "productionAvailable": False,
        }
        return signed(body, "snapshotHash")

    def handshake(self, *, generation=None, socket=None):
        body = {
            "schemaVersion": HANDSHAKE_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "trId": "H0STCNT0",
            "approvalOrigin": LIVE_ORIGIN,
            "approvalEndpoint": APPROVAL_ENDPOINT,
            "websocketUrl": LIVE_WEBSOCKET_URL,
            "subscriptionBodyHash": subscription_hash(),
            "sessionId": SESSION,
            "accountFingerprint": ACCOUNT,
            "ownerEpoch": self.owner_epoch,
            "ownerEpochId": OWNER_EPOCH_ID,
            "ownerEpochHash": self.owner_hash,
            "processGeneration": PROCESS_GENERATION,
            "sourceGeneration": generation or self.current_generation,
            "socketIdentityHash": socket or self.current_socket,
            "appKeyIdHash": "5" * 64,
            "approvalKeyHash": "6" * 64,
            "connectedAt": (self.now - timedelta(seconds=1)).isoformat(),
            "subscriptionAckAt": self.now.isoformat(),
            "ackRtCd": "0",
            "ackTrId": "H0STCNT0",
            "ackTrKey": "010140",
            "publicMarketDataOnly": True,
            "privateStreamConfigured": False,
            "accountAuthorityAvailable": False,
            "mutationAuthorityAvailable": False,
            "networkExecutorAvailable": False,
            "productionAvailable": False,
            "authorityKeyIdHash": AUTHORITY_KEY_ID,
            "authorityPurpose": "MARKET_SOURCE_RECORD_VERIFY",
        }
        return signed(body, "handshakeHash")

    def raw(self, *, ordinal=1, previous="0" * 64, generation=None, raw=None):
        local = self.now.astimezone(timezone(timedelta(hours=9)))
        payload = raw or frame(
            date=local.strftime("%Y%m%d"), clock=local.strftime("%H%M%S")
        )
        body = {
            "schemaVersion": RAW_RECORD_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "trId": "H0STCNT0",
            "sessionId": SESSION,
            "accountFingerprint": ACCOUNT,
            "ownerEpoch": self.owner_epoch,
            "ownerEpochId": OWNER_EPOCH_ID,
            "ownerEpochHash": self.owner_hash,
            "processGeneration": PROCESS_GENERATION,
            "sourceGeneration": generation or self.current_generation,
            "socketIdentityHash": self.current_socket,
            "ingressOrdinal": ordinal,
            "receivedAt": self.now.isoformat(),
            "rawFrame": payload,
            "rawFrameHash": hashlib.sha256(payload.encode()).hexdigest(),
            "recordCount": 1,
            "previousIngressHeadHash": previous,
            "authorityKeyIdHash": AUTHORITY_KEY_ID,
            "upstreamExchangeSequenceAvailable": False,
            "upstreamPacketCompletenessAttested": False,
            "productionAvailable": False,
            "authorityPurpose": "MARKET_SOURCE_RECORD_VERIFY",
        }
        return signed(body, "recordHash")

    def writer(self, record):
        self.order.append("writer")
        self.writer_calls += 1
        if self.writer_mode == "RAISE":
            raise RuntimeError("writer failed")
        source_frame_envelope_hash = "9" * 64
        source_frame_head_hash = "a" * 64
        source_arm_transition_head_hash = "b" * 64
        head = digest(
            {
                "schemaVersion": "kis-domestic-functional-market-source-head/v2",
                "sourceGeneration": record["sourceGeneration"],
                "ingressOrdinal": record["ingressOrdinal"],
                "previousIngressHeadHash": record["previousIngressHeadHash"],
                "rawRecordHash": record["recordHash"],
                "sourceArmId": "kis-public-arm-one",
                "sourceFrameIndex": record["ingressOrdinal"],
                "sourceFrameEnvelopeHash": source_frame_envelope_hash,
                "sourceFrameHeadHash": source_frame_head_hash,
                "sourceArmTransitionHeadHash": source_arm_transition_head_hash,
            }
        )
        body = {
            "schemaVersion": ACK_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "sessionId": SESSION,
            "ownerEpochHash": OWNER_EPOCH_HASH,
            "sourceGeneration": record["sourceGeneration"],
            "ingressOrdinal": record["ingressOrdinal"],
            "rawFrameHash": record["rawFrameHash"],
            "rawRecordHash": record["recordHash"],
            "previousIngressHeadHash": record["previousIngressHeadHash"],
            "durableRecordHash": source_frame_envelope_hash,
            "durableHeadHash": head,
            "ackedAt": self.now.isoformat(),
            "authorityKeyIdHash": SOURCE_AUTHORITY_KEY_ID,
            "authorityPurpose": "SOURCE_RECORD_VERIFY",
            "sourceArmId": "kis-public-arm-one",
            "sourceFrameIndex": record["ingressOrdinal"],
            "firstSourceSequence": record["ingressOrdinal"],
            "lastSourceSequence": record["ingressOrdinal"],
            "sourceFrameEnvelopeHash": source_frame_envelope_hash,
            "sourceFrameHeadHash": source_frame_head_hash,
            "sourceArmTransitionHeadHash": source_arm_transition_head_hash,
            "productionAvailable": False,
        }
        result = signed(body, "ackHash")
        if self.writer_mode == "TAMPER":
            result["durableRecordHash"] = "f" * 64
        return result

    def reducer(self, raw, record, ack):
        self.order.append("reducer")
        self.reducer_calls += 1
        if self.reducer_mode == "RAISE":
            raise RuntimeError("reducer failed")
        body = {
            "schemaVersion": REDUCER_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "sessionId": SESSION,
            "sourceGeneration": record["sourceGeneration"],
            "ingressOrdinal": record["ingressOrdinal"],
            "rawRecordHash": record["recordHash"],
            "durableRecordHash": ack["durableRecordHash"],
            "durableHeadHash": ack["durableHeadHash"],
            "reducerState": "ACCEPTED",
            "closedBarCount": 0,
            "nextOpenObserved": False,
            "reducedAt": self.now.isoformat(),
            "authorityKeyIdHash": ack["authorityKeyIdHash"],
            "authorityPurpose": "SOURCE_RECORD_VERIFY",
            "productionAvailable": False,
        }
        if self.reducer_mode == "WRONG_KEY":
            body["authorityKeyIdHash"] = "f" * 64
        return signed(body, "receiptHash")

    def build(self, *, writer=None, ack_verifier=None):
        return DisabledKisDomesticFunctionalMarketSource(
            program_ledger=self.ledger,
            owner_epoch_reader=self.owner,
            owner_epoch_verifier=verify,
            handshake_verifier=verify,
            raw_record_verifier=verify,
            durable_ingress_writer=writer or self.writer,
            durable_ack_verifier=ack_verifier or verify,
            reducer=self.reducer,
            reducer_receipt_verifier=verify,
            transition_signer=sign_transition,
            transition_verifier=verify_transition,
            transition_authority_key_id_hash=TRANSITION_AUTHORITY_KEY_ID,
            trusted_clock=lambda: self.now,
        )


class KisDomesticFunctionalMarketSourceTests(unittest.TestCase):
    def test_transition_accepts_only_verified_canonical_ed25519_base64(self):
        authority = ECC.generate(curve="Ed25519")
        public = authority.public_key()
        authority_id = hashlib.sha256(
            public.export_key(format="PEM").encode("utf-8")
        ).hexdigest()

        def ed_sign(domain, body):
            return base64.b64encode(
                eddsa.new(authority, mode="rfc8032").sign(
                    domain.encode("ascii") + b"\x00" + canonical(body).encode()
                )
            ).decode("ascii")

        def ed_verify(domain, body, signature, key_id_hash):
            try:
                if key_id_hash != authority_id:
                    return False
                eddsa.new(public, mode="rfc8032").verify(
                    domain.encode("ascii") + b"\x00" + canonical(body).encode(),
                    base64.b64decode(signature, validate=True),
                )
                return True
            except BaseException:
                return False

        fixture = MarketSourceHarness()
        try:
            fixture.source = DisabledKisDomesticFunctionalMarketSource(
                program_ledger=fixture.ledger,
                owner_epoch_reader=fixture.owner,
                owner_epoch_verifier=verify,
                handshake_verifier=verify,
                raw_record_verifier=verify,
                durable_ingress_writer=fixture.writer,
                durable_ack_verifier=verify,
                reducer=fixture.reducer,
                reducer_receipt_verifier=verify,
                transition_signer=ed_sign,
                transition_verifier=ed_verify,
                transition_authority_key_id_hash=authority_id,
                trusted_clock=lambda: fixture.now,
            )
            result = fixture.source.begin_generation(fixture.handshake())
            self.assertEqual("ARMED_WAIT_PUBLIC", result["state"])
        finally:
            fixture.close()

        for malformed in (
            base64.b64encode(b"short").decode("ascii"),
            "%%%not-base64%%%",
        ):
            fixture = MarketSourceHarness()
            try:
                fixture.source = DisabledKisDomesticFunctionalMarketSource(
                    program_ledger=fixture.ledger,
                    owner_epoch_reader=fixture.owner,
                    owner_epoch_verifier=verify,
                    handshake_verifier=verify,
                    raw_record_verifier=verify,
                    durable_ingress_writer=fixture.writer,
                    durable_ack_verifier=verify,
                    reducer=fixture.reducer,
                    reducer_receipt_verifier=verify,
                    transition_signer=lambda *_args, value=malformed: value,
                    transition_verifier=lambda *_args: True,
                    transition_authority_key_id_hash=authority_id,
                    trusted_clock=lambda: fixture.now,
                )
                with self.assertRaisesRegex(
                    KisDomesticFunctionalMarketSourceBlocked,
                    "transition-signature-invalid",
                ):
                    fixture.source.begin_generation(fixture.handshake())
            finally:
                fixture.close()

        fixture = MarketSourceHarness()
        try:
            fixture.source = DisabledKisDomesticFunctionalMarketSource(
                program_ledger=fixture.ledger,
                owner_epoch_reader=fixture.owner,
                owner_epoch_verifier=verify,
                handshake_verifier=verify,
                raw_record_verifier=verify,
                durable_ingress_writer=fixture.writer,
                durable_ack_verifier=verify,
                reducer=fixture.reducer,
                reducer_receipt_verifier=verify,
                transition_signer=ed_sign,
                transition_verifier=lambda *_args: False,
                transition_authority_key_id_hash="f" * 64,
                trusted_clock=lambda: fixture.now,
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "transition-signature-unverified",
            ):
                fixture.source.begin_generation(fixture.handshake())
        finally:
            fixture.close()

    def test_exact_live_handshake_and_disabled_surface(self):
        fixture = MarketSourceHarness()
        try:
            status = fixture.source.begin_generation(fixture.handshake())
            self.assertEqual("ARMED_WAIT_PUBLIC", status["state"])
            self.assertEqual(GENERATION_ONE, status["sourceGeneration"])
            self.assertFalse(status["networkExecutorAvailable"])
            self.assertFalse(status["productionAvailable"])
            self.assertFalse(hasattr(fixture.source, "connect"))
            self.assertFalse(hasattr(fixture.source, "send"))
            self.assertFalse(hasattr(fixture.source, "token"))
        finally:
            fixture.close()

    def test_live_origin_path_tr_symbol_and_ack_are_exact(self):
        fixture = MarketSourceHarness()
        try:
            changes = {
                "approvalOrigin": "https://openapivts.koreainvestment.com:29443",
                "approvalEndpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "websocketUrl": "ws://ops.koreainvestment.com:31000/tryitout",
                "trId": "H0STCNI0",
                "ackTrKey": "005930",
                "ackRtCd": "1",
            }
            for key, changed in changes.items():
                with self.subTest(key=key):
                    value = fixture.handshake()
                    raw = dict(value)
                    raw.pop("signature"); raw.pop("handshakeHash")
                    raw[key] = changed
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalMarketSourceBlocked,
                        "live-binding-mismatch",
                    ):
                        fixture.source.begin_generation(
                            signed(raw, "handshakeHash")
                        )
        finally:
            fixture.close()

    def test_owner_epoch_is_rechecked_at_generation_and_ingress(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            record = fixture.raw()
            fixture.owner_hash = "f" * 64
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "owner-epoch-binding",
            ):
                fixture.source.ingest_signed_frame(record)
            status = fixture.source.snapshot(GENERATION_ONE)
            self.assertEqual("SAFE_INCOMPLETE", status["state"])
            self.assertEqual(0, fixture.writer_calls)
        finally:
            fixture.close()

    def test_durable_callback_ack_strictly_precedes_reducer(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            result = fixture.source.ingest_signed_frame(fixture.raw())
            self.assertEqual(["writer", "reducer"], fixture.order)
            self.assertTrue(result["rawIngressAckedBeforeReducer"])
            self.assertEqual("9" * 64, result["durableRecordHash"])
            self.assertEqual("kis-public-arm-one", result["sourceArmId"])
            status = fixture.source.snapshot(GENERATION_ONE)
            self.assertEqual(1, status["lastIngressOrdinal"])
            self.assertEqual(0, status["pendingIngressCount"])
        finally:
            fixture.close()

    def test_unverified_or_failed_durable_ack_never_reaches_reducer(self):
        for mode in ("RAISE", "TAMPER"):
            with self.subTest(mode=mode):
                fixture = MarketSourceHarness()
                try:
                    fixture.writer_mode = mode
                    fixture.source.begin_generation(fixture.handshake())
                    with self.assertRaises(Exception):
                        fixture.source.ingest_signed_frame(fixture.raw())
                    self.assertEqual(0, fixture.reducer_calls)
                    status = fixture.source.snapshot(GENERATION_ONE)
                    self.assertEqual("SAFE_INCOMPLETE", status["state"])
                    self.assertEqual(1, status["pendingIngressCount"])
                finally:
                    fixture.close()

    def test_validly_signed_wrong_ack_purpose_or_reducer_key_fails_closed(self):
        fixture = MarketSourceHarness()
        try:
            original = fixture.writer

            def wrong_purpose(record):
                value = original(record)
                body = dict(value)
                body.pop("signature"); body.pop("ackHash")
                body["authorityPurpose"] = "MARKET_SOURCE_RECORD_VERIFY"
                return signed(body, "ackHash")

            fixture.source = fixture.build(writer=wrong_purpose)
            fixture.source.begin_generation(fixture.handshake())
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "ack-binding-mismatch",
            ):
                fixture.source.ingest_signed_frame(fixture.raw())
            self.assertEqual(0, fixture.reducer_calls)
        finally:
            fixture.close()

        fixture = MarketSourceHarness()
        try:
            fixture.reducer_mode = "WRONG_KEY"
            fixture.source.begin_generation(fixture.handshake())
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "reducer-receipt-binding-mismatch",
            ):
                fixture.source.ingest_signed_frame(fixture.raw())
            self.assertEqual("SAFE_INCOMPLETE", fixture.source.snapshot(
                GENERATION_ONE
            )["state"])
        finally:
            fixture.close()

    def test_reducer_failure_after_ack_is_safe_incomplete(self):
        fixture = MarketSourceHarness()
        try:
            fixture.reducer_mode = "RAISE"
            fixture.source.begin_generation(fixture.handshake())
            with self.assertRaisesRegex(RuntimeError, "reducer failed"):
                fixture.source.ingest_signed_frame(fixture.raw())
            status = fixture.source.snapshot(GENERATION_ONE)
            self.assertEqual("SAFE_INCOMPLETE", status["state"])
            self.assertEqual(1, status["pendingIngressCount"])
            self.assertEqual(["writer", "reducer"], fixture.order)
        finally:
            fixture.close()

    def test_reconnect_new_generation_terminalizes_predecessor(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            fixture.current_generation = GENERATION_TWO
            fixture.current_socket = SOCKET_TWO
            second = fixture.source.begin_generation(
                fixture.handshake(generation=GENERATION_TWO, socket=SOCKET_TWO)
            )
            first = fixture.source.snapshot(GENERATION_ONE)
            self.assertEqual("SAFE_INCOMPLETE", first["state"])
            self.assertEqual(
                "SOCKET_RECONNECT_GENERATION_CHANGED", first["failureReason"]
            )
            self.assertEqual(GENERATION_ONE, second["reconnectPredecessorGeneration"])
            self.assertEqual("ARMED_WAIT_PUBLIC", second["state"])
            old = fixture.raw(generation=GENERATION_ONE)
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "not-active",
            ):
                fixture.source.ingest_signed_frame(old)
        finally:
            fixture.close()

    def test_reconnect_cannot_reuse_generation(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "reuse-forbidden",
            ):
                fixture.source.begin_generation(fixture.handshake())
        finally:
            fixture.close()

    def test_reconnect_requires_new_socket_identity_and_burns_predecessor(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            fixture.current_generation = GENERATION_TWO
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "new-socket-identity",
            ):
                fixture.source.begin_generation(
                    fixture.handshake(
                        generation=GENERATION_TWO,
                        socket=SOCKET_ONE,
                    )
                )
            first = fixture.source.snapshot(GENERATION_ONE)
            self.assertEqual("SAFE_INCOMPLETE", first["state"])
            self.assertEqual(
                "RECONNECT_SOCKET_IDENTITY_REUSED", first["failureReason"]
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "not-found",
            ):
                fixture.source.snapshot(GENERATION_TWO)
        finally:
            fixture.close()

    def test_local_ingress_gap_or_previous_head_change_is_safe_incomplete(self):
        for changed in (
            {"ordinal": 2, "previous": "0" * 64},
            {"ordinal": 1, "previous": "f" * 64},
        ):
            with self.subTest(changed=changed):
                fixture = MarketSourceHarness()
                try:
                    fixture.source.begin_generation(fixture.handshake())
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalMarketSourceBlocked,
                        "ordinal-gap",
                    ):
                        fixture.source.ingest_signed_frame(
                            fixture.raw(**changed)
                        )
                    self.assertEqual(
                        "SAFE_INCOMPLETE",
                        fixture.source.snapshot(GENERATION_ONE)["state"],
                    )
                    self.assertEqual(0, fixture.writer_calls)
                finally:
                    fixture.close()

    def test_malformed_frame_or_forged_record_is_rejected_before_callback(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            value = fixture.raw(raw="0|H0STCNT0|1|too-short")
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked, "record-width"
            ):
                fixture.source.ingest_signed_frame(value)
            self.assertEqual(0, fixture.writer_calls)
        finally:
            fixture.close()

    def test_raw_frame_signer_and_exchange_event_time_are_generation_bound(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            candidate = fixture.raw()
            unsigned = dict(candidate)
            unsigned.pop("signature")
            unsigned.pop("recordHash")
            unsigned["authorityKeyIdHash"] = "8" * 64
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "generation-owner-or-ordinal-gap",
            ):
                fixture.source.ingest_signed_frame(
                    signed(unsigned, "recordHash")
                )
            self.assertEqual(0, fixture.writer_calls)
            self.assertEqual(
                "SAFE_INCOMPLETE",
                fixture.source.snapshot(GENERATION_ONE)["state"],
            )
        finally:
            fixture.close()

        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "event-time-lineage",
            ):
                fixture.source.ingest_signed_frame(
                    fixture.raw(raw=frame(clock="120458"))
                )
            self.assertEqual(0, fixture.writer_calls)
            self.assertEqual(
                "SAFE_INCOMPLETE",
                fixture.source.snapshot(GENERATION_ONE)["state"],
            )
        finally:
            fixture.close()

        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            forged = fixture.raw()
            forged["signature"] = "f" * 64
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "signature-unverified",
            ):
                fixture.source.ingest_signed_frame(forged)
            self.assertEqual(0, fixture.writer_calls)
        finally:
            fixture.close()

    def test_restart_pending_intent_terminalizes_generation(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            fixture.writer_mode = "RAISE"
            with self.assertRaises(RuntimeError):
                fixture.source.ingest_signed_frame(fixture.raw())
            replacement = fixture.build()
            self.assertEqual(
                "SAFE_INCOMPLETE",
                replacement.snapshot(GENERATION_ONE)["state"],
            )
        finally:
            fixture.close()

    def test_restart_without_pending_ingress_still_terminalizes_socket_owner(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            replacement = fixture.build()
            status = replacement.snapshot(GENERATION_ONE)
            self.assertEqual("SAFE_INCOMPLETE", status["state"])
            self.assertEqual(
                "STARTUP_OWNER_LOSS_REQUIRES_NEW_SOCKET_GENERATION",
                status["failureReason"],
            )
            self.assertEqual(
                (GENERATION_ONE,),
                replacement.startup_terminalized_generations,
            )
            self.assertFalse(status["crossProcessSocketOwnerLeaseWired"])
        finally:
            fixture.close()

    def test_signed_transition_chains_and_full_46_field_projection_replay(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            fixture.source.ingest_signed_frame(fixture.raw())
            status = fixture.source.snapshot(GENERATION_ONE)
            self.assertEqual(2, status["generationTransitionCount"])
            self.assertTrue(status["allIngressTransitionChainsVerified"])
            self.assertTrue(status["allRawFramesReparsedAsExact46FieldRecords"])
            conn = fixture.ledger.connect()
            try:
                row = conn.execute(
                    "SELECT parsed_records_json,parsed_records_hash,"
                    "transition_count,state FROM "
                    "kis_functional_market_source_ingress"
                ).fetchone()
                parsed = json.loads(row["parsed_records_json"])
                self.assertEqual(46, len(parsed[0]))
                self.assertEqual(3, row["transition_count"])
                self.assertEqual("REDUCED", row["state"])
                self.assertEqual(
                    hashlib.sha256(row["parsed_records_json"].encode()).hexdigest(),
                    row["parsed_records_hash"],
                )
            finally:
                conn.close()
        finally:
            fixture.close()

    def test_forged_signed_transition_is_rejected_by_independent_replay(self):
        fixture = MarketSourceHarness()
        try:
            fixture.source.begin_generation(fixture.handshake())
            conn = fixture.ledger.connect()
            try:
                trigger = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE name="
                    "'kis_functional_market_source_transition_update_forbidden'"
                ).fetchone()[0]
                conn.execute(
                    "DROP TRIGGER "
                    "kis_functional_market_source_transition_update_forbidden"
                )
                conn.execute(
                    "UPDATE kis_functional_market_source_transition SET "
                    "signature=? WHERE source_generation=? AND ingress_ordinal=0",
                    ("f" * 64, GENERATION_ONE),
                )
                conn.execute(trigger)
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "transition-signature-unverified",
            ):
                fixture.source.snapshot(GENERATION_ONE)
        finally:
            fixture.close()

    def test_v1_or_dirty_schema_is_migration_hold(self):
        fixture = MarketSourceHarness()
        try:
            conn = fixture.ledger.connect()
            try:
                conn.execute(
                    "UPDATE kis_functional_market_source_manifest SET "
                    "schema_version='kis-domestic-functional-market-source-sqlite/v1'"
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalMarketSourceBlocked,
                "schema-manifest-dirty",
            ):
                fixture.build()
        finally:
            fixture.close()

    def test_concrete_source_journal_writer_binds_market_ingress_before_reducer(self):
        fixture = MarketSourceHarness()
        try:
            journal = DurableKisDomesticPublicArmJournal(
                Path(fixture.temp.name) / "public-source.sqlite3",
                capture_signer=source_sign,
                capture_verifier=source_verify,
                server_authority_key_id="source-server-authority-v1",
            )
            arm_id = "kis-public-source-arm-" + "1" * 32
            owner_token_hash = "2" * 64
            arm_body = {
                "schemaVersion": "kis-domestic-functional-public-arm/v1",
                "route": "KIS_KR_LIVE_CONTINUOUS",
                "pdno": "010140",
                "state": "ARMED_WAIT_PUBLIC",
                "armId": arm_id,
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
                "serverAuthorityKeyIdHash": journal.server_authority_key_id_hash,
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
            journal.begin_arm(
                arm_record=arm_body,
                arm_signature=source_sign("PUBLIC_ARM", arm_body),
                owner_token_hash=owner_token_hash,
            )
            adapter = KisDomesticFunctionalMarketSourceDurableWriter(
                journal=journal,
                arm_id=arm_id,
                owner_token_hash=owner_token_hash,
                market_record_verifier=verify,
                trusted_clock=lambda: fixture.now,
            )
            fixture.source = fixture.build(
                writer=adapter,
                ack_verifier=adapter.verify_ack,
            )
            fixture.source.begin_generation(fixture.handshake())
            result = None
            for index in range(12):
                fixture.now = NOW + timedelta(minutes=5 * index)
                prior = fixture.source.snapshot(GENERATION_ONE)[
                    "ingressHeadHash"
                ]
                result = fixture.source.ingest_signed_frame(
                    fixture.raw(ordinal=index + 1, previous=prior)
                )
            assert result is not None
            self.assertEqual(arm_id, result["sourceArmId"])
            self.assertEqual(["reducer"] * 12, fixture.order)
            source_conn = journal._connect()
            try:
                frame_body = json.loads(source_conn.execute(
                    "SELECT frame_record_json FROM kis_public_source_frame "
                    "ORDER BY frame_index LIMIT 1"
                ).fetchone()[0])
                self.assertEqual(
                    fixture.source.snapshot(GENERATION_ONE)["ingressRecordCount"],
                    12,
                )
                market_conn = fixture.ledger.connect()
                try:
                    first_market_record = json.loads(market_conn.execute(
                        "SELECT raw_record_json FROM "
                        "kis_functional_market_source_ingress "
                        "ORDER BY ingress_ordinal LIMIT 1"
                    ).fetchone()[0])
                finally:
                    market_conn.close()
                self.assertEqual(
                    first_market_record["recordHash"],
                    frame_body["marketSourceRawRecordHash"],
                )
                self.assertEqual(46, len(frame_body["recordFields"][0]))
                self.assertEqual(1, frame_body["marketSourceIngressOrdinal"])
                events = [
                    json.loads(row[0])
                    for row in source_conn.execute(
                        "SELECT event_record_json FROM kis_public_source_event "
                        "ORDER BY source_sequence"
                    ).fetchall()
                ]
            finally:
                source_conn.close()
            bars = _independent_bars_from_events(events[:11])
            proof = {
                "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
                "route": "KIS_KR_LIVE_CONTINUOUS",
                "pdno": "010140",
                "sourceProvider": "kis",
                "sourceGeneration": GENERATION_ONE,
                "firstSourceSequence": "1",
                "lastSourceSequence": "11",
                "sourceEventCount": 11,
                "barRawEventChainHashes": [
                    bar["rawEventChainHash"] for bar in bars
                ],
            }
            archive = journal.window_archive(
                arm_id=arm_id,
                owner_token_hash=owner_token_hash,
                first_sequence=1,
                last_sequence=11,
                next_open_sequence=12,
                expected_bars=bars,
                expected_source_proof_hash=digest(proof),
            )
            self.assertTrue(archive["marketSourceIntegrationComplete"])
            self.assertEqual(12, archive["marketSourceIngressLinkCount"])
            self.assertEqual(
                fixture.source.snapshot(GENERATION_ONE)["ingressHeadHash"],
                archive["marketSourceIngressLinkHeadHash"],
            )
        finally:
            fixture.close()

    def test_completeness_and_dual_source_claims_remain_false(self):
        status = market_source_component_status()
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_PRODUCTION_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_NETWORK_EXECUTOR_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_RELEASE_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_MUTATION_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_UPSTREAM_COMPLETENESS_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_DUAL_SOURCE_CONFIRMATION_AVAILABLE)
        self.assertFalse(status["upstreamExchangeSequenceAvailable"])
        self.assertFalse(status["upstreamPacketCompletenessAttested"])
        self.assertTrue(status["acceptedIngressContinuityOnly"])
        self.assertFalse(status["officialKisMinuteCandleGetAdapterAvailable"])
        self.assertFalse(status["dualSourceBarConfirmationAvailable"])
        self.assertEqual(
            "OFFICIAL_KIS_MINUTE_CANDLE_GET_ADAPTER_NOT_FOUND",
            status["dualSourceBlocker"],
        )


if __name__ == "__main__":
    unittest.main()
