from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from live_trader.upbit_continuous_functional import UpbitFunctionalBlocked
from live_trader.upbit_functional_transport import (
    DurableUpbitMyOrderJournal,
    OfficialUpbitFunctionalGetClient,
    _protected_upbit_functional_get_network_capability,
    build_upbit_functional_get_request,
    normalize_upbit_myorder_event,
    upbit_credential_fingerprint,
)
from live_trader.upbit_functional_truth import (
    UPBIT_OPEN_ORDERS_ENDPOINT,
    UPBIT_ORDER_DETAIL_ENDPOINT,
)


NOW = datetime(2026, 8, 13, 2, 0, tzinfo=timezone.utc)
ACCESS = "upbit-access-functional-test"
FINGERPRINT = hashlib.sha256(f"UPBIT_SPOT\0{ACCESS}".encode()).hexdigest()
SESSION = "upbit-functional-session-transport-0001"


def raw_event(*, identifier: str = "uft-" + "a" * 28) -> dict[str, object]:
    return {
        "type": "myOrder",
        "code": "KRW-BTC",
        "uuid": "broker-order-uuid-0001",
        "trade_uuid": "broker-trade-uuid-0001",
        "identifier": identifier,
        "ask_bid": "BID",
        "state": "trade",
        "timestamp": int(NOW.timestamp() * 1000),
        "trade_volume": "0.0001",
        "trade_price": "100000000",
        "trade_fee": "5",
    }


class UpbitFunctionalTransportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.journal = DurableUpbitMyOrderJournal(
            Path(self.temporary.name) / "upbit-journal.sqlite3",
            clock=lambda: NOW,
        )

    def ready_env(self):
        return patch.dict(
            os.environ,
            {
                "UPBIT_ACCESS_KEY": ACCESS,
                "UPBIT_SECRET_KEY": "upbit-secret-functional-test",
                "UPBIT_BASE_URL": "https://api.upbit.com",
            },
            clear=False,
        )

    def begin_writer(self) -> dict[str, object]:
        writer = self.journal.begin_authenticated_session(
            session_id=SESSION,
            account_fingerprint=FINGERPRINT,
            started_at=NOW - timedelta(seconds=1),
        )
        self.journal.attest_authenticated_connection(
            SESSION,
            writer_token=str(writer["writerToken"]),
            writer_generation=int(writer["writerGeneration"]),
        )
        return writer

    def test_get_request_preserves_repeated_states_and_redacts_credentials(self) -> None:
        with self.ready_env():
            request = build_upbit_functional_get_request(
                UPBIT_OPEN_ORDERS_ENDPOINT,
                (
                    ("states[]", "wait"),
                    ("states[]", "watch"),
                    ("page", "1"),
                ),
            )
        self.assertEqual("GET", request.method)
        self.assertIn("states%5B%5D=wait&states%5B%5D=watch", request.url)
        self.assertNotIn(ACCESS, str(request.preview()))
        self.assertNotIn("upbit-secret", str(request.preview()))
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "not-allowlisted"):
            build_upbit_functional_get_request(
                "/v1/withdraws/coin", ()
            )

    def test_production_get_rejects_nonofficial_origin_before_signing(self) -> None:
        for origin in (
            "http://api.upbit.com",
            "https://upbit.example.test",
            "https://api.upbit.com:443",
            "https://api.upbit.com/redirect",
        ):
            with self.subTest(origin=origin), patch.dict(
                os.environ,
                {
                    "UPBIT_ACCESS_KEY": ACCESS,
                    "UPBIT_SECRET_KEY": "upbit-secret-functional-test",
                    "UPBIT_BASE_URL": origin,
                },
                clear=False,
            ), patch(
                "live_trader.upbit_functional_transport."
                "build_upbit_functional_authorization",
                side_effect=AssertionError("must block before signing"),
            ), self.assertRaisesRegex(
                UpbitFunctionalBlocked, "origin-not-official"
            ):
                build_upbit_functional_get_request(
                    UPBIT_OPEN_ORDERS_ENDPOINT, ()
                )

    def test_get_client_is_exact_account_bound_and_has_no_mutation_surface(self) -> None:
        sent: list[object] = []

        def sender(request: object) -> dict[str, object]:
            sent.append(request)
            return {"ok": True, "statusCode": 200, "json": []}

        with self.ready_env(), patch(
            "live_trader.upbit_functional_transport."
            "UPBIT_FUNCTIONAL_GET_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_functional_get_network_capability()
            client = OfficialUpbitFunctionalGetClient(
                expected_account_fingerprint=FINGERPRINT,
                sender=sender,
                network_capability=capability,
            )
            self.assertEqual([], client.get(UPBIT_OPEN_ORDERS_ENDPOINT, ()))
        self.assertEqual(1, len(sent))
        self.assertFalse(hasattr(client, "post"))
        self.assertFalse(hasattr(client, "cancel"))
        self.assertFalse(hasattr(client, "withdraw"))

        with self.ready_env(), patch(
            "live_trader.upbit_functional_transport."
            "UPBIT_FUNCTIONAL_GET_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_functional_get_network_capability()
            changed = OfficialUpbitFunctionalGetClient(
                expected_account_fingerprint="c" * 64,
                sender=lambda _request: (_ for _ in ()).throw(
                    AssertionError("mismatch must block before HTTP")
                ),
                network_capability=capability,
            )
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "fingerprint-mismatch"
            ):
                changed.get(UPBIT_OPEN_ORDERS_ENDPOINT, ())

    def test_public_get_client_cannot_reach_sender_or_signer_while_held(self) -> None:
        sender_calls = 0

        def hostile_sender(_request):
            nonlocal sender_calls
            sender_calls += 1
            raise AssertionError("held client must not reach sender")

        client = OfficialUpbitFunctionalGetClient(
            expected_account_fingerprint=FINGERPRINT,
            sender=hostile_sender,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_transport."
            "build_upbit_functional_authorization",
            side_effect=AssertionError("held client must not sign"),
        ), self.assertRaisesRegex(
            UpbitFunctionalBlocked, "network-capability-closed"
        ):
            client.get(UPBIT_OPEN_ORDERS_ENDPOINT, ())

        forged = OfficialUpbitFunctionalGetClient(
            expected_account_fingerprint=FINGERPRINT,
            sender=hostile_sender,
            network_capability=object(),
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_transport."
            "UPBIT_FUNCTIONAL_GET_NETWORK_RELEASED",
            True,
        ), patch(
            "live_trader.upbit_functional_transport."
            "build_upbit_functional_authorization",
            side_effect=AssertionError("forged capability must not sign"),
        ), self.assertRaisesRegex(
            UpbitFunctionalBlocked, "network-capability-closed"
        ):
            forged.get(UPBIT_OPEN_ORDERS_ENDPOINT, ())
        self.assertEqual(0, sender_calls)

    def test_detail_404_is_typed_absence_but_other_errors_fail_closed(self) -> None:
        with self.ready_env(), patch(
            "live_trader.upbit_functional_transport."
            "UPBIT_FUNCTIONAL_GET_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_functional_get_network_capability()
            missing = OfficialUpbitFunctionalGetClient(
                expected_account_fingerprint=FINGERPRINT,
                sender=lambda _request: {
                    "ok": False,
                    "statusCode": 404,
                    "json": {"error": {"name": "order_not_found"}},
                },
                network_capability=capability,
            )
            self.assertEqual(
                {"_notFound": True},
                missing.get(
                    UPBIT_ORDER_DETAIL_ENDPOINT,
                    (("identifier", "uft-" + "a" * 28),),
                ),
            )
            unproven = OfficialUpbitFunctionalGetClient(
                expected_account_fingerprint=FINGERPRINT,
                sender=lambda _request: {
                    "ok": False,
                    "statusCode": 404,
                    "json": {},
                },
                network_capability=capability,
            )
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "absence-not-proven"
            ):
                unproven.get(
                    UPBIT_ORDER_DETAIL_ENDPOINT,
                    (("identifier", "uft-" + "a" * 28),),
                )
            unavailable = OfficialUpbitFunctionalGetClient(
                expected_account_fingerprint=FINGERPRINT,
                sender=lambda _request: {
                    "ok": False,
                    "statusCode": 500,
                    "json": {},
                },
                network_capability=capability,
            )
            with self.assertRaisesRegex(UpbitFunctionalBlocked, "get-failed"):
                unavailable.get(UPBIT_OPEN_ORDERS_ENDPOINT, ())

    def test_durable_journal_seals_owned_event_and_detects_external_activity(self) -> None:
        writer = self.begin_writer()
        event = self.journal.ingest(
            SESSION,
            raw_event(),
            writer_token=str(writer["writerToken"]),
            writer_generation=int(writer["writerGeneration"]),
        )
        owned = self.journal.snapshot(
            session_id=SESSION,
            identifiers=("uft-" + "a" * 28,),
        )
        self.assertTrue(owned["eventsComplete"])
        self.assertTrue(owned["externalActivityAbsent"])
        self.assertEqual(event["eventId"], owned["events"][0]["eventId"])

        not_owned = self.journal.snapshot(session_id=SESSION, identifiers=())
        self.assertFalse(not_owned["externalActivityAbsent"])

    def test_disconnect_or_parser_failure_is_durable_and_cannot_be_cleared(self) -> None:
        writer = self.begin_writer()
        with self.assertRaises(UpbitFunctionalBlocked):
            self.journal.ingest(
                SESSION,
                {"type": "not-myOrder"},
                writer_token=str(writer["writerToken"]),
                writer_generation=int(writer["writerGeneration"]),
            )
        proof = self.journal.snapshot(session_id=SESSION, identifiers=())
        self.assertTrue(proof["gapDetected"])
        self.assertFalse(proof["eventsComplete"])
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "not-continuous"):
            self.journal.observe(
                SESSION,
                writer_token=str(writer["writerToken"]),
                writer_generation=int(writer["writerGeneration"]),
            )

    def test_private_stream_writer_token_generation_blocks_spoof_and_takeover(self) -> None:
        writer = self.begin_writer()
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "writer-authority-invalid"
        ):
            self.journal.observe(
                SESSION,
                writer_token="forged-writer",
                writer_generation=int(writer["writerGeneration"]),
            )
        self.journal.observe(
            SESSION,
            writer_token=str(writer["writerToken"]),
            writer_generation=int(writer["writerGeneration"]),
        )
        self.journal.mark_gap(
            SESSION,
            detail="socket-disconnected",
            writer_token=str(writer["writerToken"]),
            writer_generation=int(writer["writerGeneration"]),
        )
        replacement = self.journal.recover_cleanup_authenticated(
            session_id=SESSION,
            account_fingerprint=FINGERPRINT,
        )
        self.assertGreater(
            int(replacement["writerGeneration"]),
            int(writer["writerGeneration"]),
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "writer-authority-invalid"
        ):
            self.journal.attest_authenticated_connection(
                SESSION,
                writer_token=str(writer["writerToken"]),
                writer_generation=int(writer["writerGeneration"]),
            )
        self.journal.attest_authenticated_connection(
            SESSION,
            writer_token=str(replacement["writerToken"]),
            writer_generation=int(replacement["writerGeneration"]),
        )
        proof = self.journal.snapshot(session_id=SESSION, identifiers=())
        self.assertTrue(proof["gapDetected"])
        self.assertFalse(proof["eventsComplete"])

    def test_terminal_cursor_cas_rejects_late_received_event(self) -> None:
        writer = self.begin_writer()
        identifier = "uft-" + "a" * 28
        expected = self.journal.prepare_terminal_attestation(
            session_id=SESSION,
            identifiers=(identifier,),
        )
        self.journal.ingest(
            SESSION,
            raw_event(identifier=identifier),
            writer_token=str(writer["writerToken"]),
            writer_generation=int(writer["writerGeneration"]),
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "terminal-cursor-changed"
        ):
            self.journal.complete_with_attestation(
                session_id=SESSION,
                identifiers=(identifier,),
                expected=expected,
                writer_token=str(writer["writerToken"]),
                writer_generation=int(writer["writerGeneration"]),
            )
        self.assertFalse(
            self.journal.snapshot(
                session_id=SESSION, identifiers=(identifier,)
            )["completed"]
        )

    def test_terminal_cursor_seal_is_exact_and_idempotent(self) -> None:
        writer = self.begin_writer()
        expected = self.journal.prepare_terminal_attestation(
            session_id=SESSION,
            identifiers=(),
        )
        sealed = self.journal.complete_with_attestation(
            session_id=SESSION,
            identifiers=(),
            expected=expected,
            writer_token=str(writer["writerToken"]),
            writer_generation=int(writer["writerGeneration"]),
        )
        self.assertEqual(expected, sealed)
        self.assertEqual(expected, self.journal.terminal_seal(session_id=SESSION))
        self.assertEqual(
            expected,
            self.journal.complete_with_attestation(
                session_id=SESSION,
                identifiers=(),
                expected=expected,
                startup_recovery=True,
            ),
        )

    def test_normalizer_uses_exact_private_order_identity(self) -> None:
        event = normalize_upbit_myorder_event(raw_event())
        self.assertEqual("KRW-BTC", event["market"])
        self.assertEqual("BID", event["side"])
        self.assertEqual("broker-order-uuid-0001", event["orderUuid"])
        self.assertEqual("broker-trade-uuid-0001", event["tradeUuid"])
        self.assertEqual(FINGERPRINT, upbit_credential_fingerprint(ACCESS))


if __name__ == "__main__":
    unittest.main()
