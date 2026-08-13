from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import tempfile
import unittest

from live_trader.upbit_continuous_functional import UpbitFunctionalBlocked
from live_trader.upbit_functional_sources import (
    OfficialUpbitFinalizedFiveMinuteWindowReader,
    OfficialUpbitFunctionalMyOrderPump,
)
from live_trader.upbit_functional_transport import (
    DurableUpbitMyOrderJournal,
    upbit_credential_fingerprint,
)


NOW = datetime(2026, 8, 13, 2, 0, tzinfo=timezone.utc)


class FakeSocket:
    def __init__(self, frames):
        self.frames = list(frames)
        self.sent = []
        self.probe = b""
        self.closed = False

    def send(self, payload):
        self.sent.append(json.loads(payload))

    def ping(self, payload):
        self.probe = bytes(payload)

    def pong(self, _payload):
        pass

    def recv_data(self, control_frame=True):
        del control_frame
        if self.frames:
            opcode, payload = self.frames.pop(0)
            return opcode, self.probe if payload == "PROBE" else payload
        raise RuntimeError("disconnect")

    def close(self):
        self.closed = True


class OfficialUpbitFunctionalSourcesTest(unittest.TestCase):
    def test_terminal_pong_barrier_drains_queued_myorder_before_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            session_id = "upbit-source-terminal-barrier-0001"
            fingerprint = upbit_credential_fingerprint("access")
            journal = DurableUpbitMyOrderJournal(
                Path(temporary) / "source-terminal.sqlite3", clock=lambda: NOW
            )
            writer = journal.begin_authenticated_session(
                session_id=session_id,
                account_fingerprint=fingerprint,
                started_at=NOW,
            )
            identifier = "uft-" + "a" * 28
            queued = {
                "type": "myOrder",
                "code": "KRW-BTC",
                "uuid": "source-terminal-order-0001",
                "trade_uuid": "source-terminal-trade-0001",
                "identifier": identifier,
                "ask_bid": "BID",
                "state": "trade",
                "timestamp": int(NOW.timestamp() * 1000),
            }
            socket = FakeSocket(
                [(10, "PROBE"), (1, json.dumps(queued)), (10, "PROBE")]
            )
            source = OfficialUpbitFunctionalMyOrderPump(
                expected_account_fingerprint=fingerprint,
                clock=lambda: NOW,
                socket_factory=lambda _authorization: socket,
                credential_reader=lambda: ("access", "secret"),
                monotonic=lambda: 1.0,
            )
            source.handshake(
                session_id=session_id,
                journal=journal,
                writer_authority=writer,
            )
            journal.attest_authenticated_connection(
                session_id,
                writer_token=str(writer["writerToken"]),
                writer_generation=int(writer["writerGeneration"]),
            )
            barrier = source.terminal_barrier(session_id=session_id)
            self.assertTrue(barrier["cutoffEstablished"])
            proof = journal.snapshot(
                session_id=session_id, identifiers=(identifier,)
            )
            self.assertEqual(1, proof["eventCursor"])
            self.assertEqual(
                "source-terminal-trade-0001",
                proof["events"][0]["tradeUuid"],
            )

    def test_myorder_handshake_binds_private_upgrade_pong_and_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = DurableUpbitMyOrderJournal(
                Path(temporary) / "source.sqlite3", clock=lambda: NOW
            )
            writer = journal.begin_authenticated_session(
                session_id="upbit-source-session-0001",
                account_fingerprint=upbit_credential_fingerprint("access"),
                started_at=NOW,
            )
            socket = FakeSocket([(10, "PROBE")])
            source = OfficialUpbitFunctionalMyOrderPump(
                expected_account_fingerprint=upbit_credential_fingerprint(
                    "access"
                ),
                clock=lambda: NOW,
                socket_factory=lambda authorization: (
                    self.assertTrue(authorization.startswith("Bearer "))
                    or socket
                ),
                credential_reader=lambda: ("access", "secret"),
                monotonic=lambda: 1.0,
            )
            handshake = source.handshake(
                session_id="upbit-source-session-0001",
                journal=journal,
                writer_authority=writer,
            )
            self.assertTrue(handshake["connected"])
            self.assertTrue(handshake["authenticated"])
            self.assertTrue(handshake["myOrderSubscribed"])
            self.assertEqual(
                hashlib.sha256(
                    writer["writerToken"].encode("utf-8")
                ).hexdigest(),
                handshake["writerTokenHash"],
            )
            self.assertEqual(
                ["KRW-BTC"], socket.sent[0][1]["codes"]
            )
            self.assertEqual("myOrder", socket.sent[0][1]["type"])
            handshake["closePump"]()
            self.assertTrue(socket.closed)

    def test_myorder_handshake_rejects_subscription_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            journal = DurableUpbitMyOrderJournal(
                Path(temporary) / "source-error.sqlite3", clock=lambda: NOW
            )
            fingerprint = upbit_credential_fingerprint("access")
            writer = journal.begin_authenticated_session(
                session_id="upbit-source-session-error-0001",
                account_fingerprint=fingerprint,
                started_at=NOW,
            )
            socket = FakeSocket(
                [
                    (
                        1,
                        json.dumps(
                            {
                                "error": {
                                    "name": "INVALID_AUTH",
                                    "message": "invalid",
                                }
                            }
                        ),
                    )
                ]
            )
            source = OfficialUpbitFunctionalMyOrderPump(
                expected_account_fingerprint=fingerprint,
                clock=lambda: NOW,
                socket_factory=lambda _authorization: socket,
                credential_reader=lambda: ("access", "secret"),
                monotonic=lambda: 1.0,
            )
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "subscription-rejected"
            ):
                source.handshake(
                    session_id="upbit-source-session-error-0001",
                    journal=journal,
                    writer_authority=writer,
                )
            self.assertTrue(socket.closed)

    def test_candle_reader_excludes_forming_bar_and_requires_contiguous_11(self) -> None:
        rows = []
        start = NOW - timedelta(minutes=55)
        for index in range(12):
            opened = start + timedelta(minutes=5 * index)
            rows.append(
                {
                    "market": "KRW-BTC",
                    "candle_date_time_utc": opened.replace(
                        tzinfo=None
                    ).isoformat(timespec="seconds"),
                    "trade_price": str(100_000_000 + index),
                    "timestamp": int(
                        min(opened + timedelta(minutes=5), NOW).timestamp()
                        * 1000
                    ),
                }
            )
        # API returns newest first. The final row opens at NOW and is forming.
        reader = OfficialUpbitFinalizedFiveMinuteWindowReader(
            clock=lambda: NOW,
            public_get=lambda endpoint, query: (
                self.assertEqual("/v1/candles/minutes/5", endpoint)
                or self.assertIn(("count", "20"), tuple(query))
                or list(reversed(rows))
            ),
        )
        window = reader()
        self.assertEqual(11, len(window["bars"]))
        self.assertEqual(NOW, datetime.fromisoformat(window["closedAt"].replace("Z", "+00:00")))
        self.assertEqual("UPBIT_REST", window["source"])
        self.assertEqual(
            "/v1/candles/minutes/5",
            window["officialCandleEvidence"]["endpoint"],
        )
        self.assertEqual(
            list(reversed(rows)),
            window["officialCandleEvidence"]["rawResponse"],
        )

        broken = [row for index, row in enumerate(rows) if index != 5]
        broken_reader = OfficialUpbitFinalizedFiveMinuteWindowReader(
            clock=lambda: NOW,
            public_get=lambda _endpoint, _query: list(reversed(broken)),
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "history-(incomplete|not-contiguous)"
        ):
            broken_reader()


if __name__ == "__main__":
    unittest.main()
