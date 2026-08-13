from __future__ import annotations

from pathlib import Path
import hashlib
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from live_trader.binance_spot_functional_transport import (
    BinanceSpotOfficialTruthReader,
    BinanceSpotTruthError,
    UserStreamProof,
)
from live_trader.binance_spot_stream_journal import (
    BinanceSpotDurableStreamBridge,
    DurableBinanceSpotUserStreamJournal,
)
from live_trader.execution_streams import ExecutionStreamManager
from tests.test_binance_spot_functional_transport import (
    BASELINE,
    FINGERPRINT,
    FakeClient,
    NOW,
OWNER_PREFIX,
    raw_account_event,
    raw_execution_event,
)


SESSION_ID = "bnsft-0123456789abcdef0123456789abcdef"
PERMIT_ID = "functional-test-binance-spot-0001"
PERMIT_HASH = "a" * 64
OWNER_PREFIX = f"ftb-{hashlib.sha256(SESSION_ID.encode()).hexdigest()[:12]}-"


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class BinanceSpotStreamJournalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.clock = MutableClock(NOW)
        self.path = Path(self.temporary.name) / "binance-stream.sqlite3"
        self.journal = DurableBinanceSpotUserStreamJournal(
            self.path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
        )
        self.token = self.journal.begin_authenticated_subscription(
            writer_id="stream-writer-a",
            owner_prefix="",
            subscribed_epoch=BASELINE - 10,
        )
        self.journal.bind_functional_session(
            OWNER_PREFIX,
            SESSION_ID,
            PERMIT_ID,
            PERMIT_HASH,
            writer_id="stream-writer-a",
            writer_token=self.token,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_restart_reader_keeps_proof_only_while_writer_heartbeat_is_fresh(self) -> None:
        self.journal.ingest(
            raw_execution_event(c=OWNER_PREFIX),
            writer_id="stream-writer-a",
            writer_token=self.token,
        )
        self.journal.ingest(
            raw_account_event(),
            writer_id="stream-writer-a",
            writer_token=self.token,
        )
        restarted_reader = DurableBinanceSpotUserStreamJournal(
            self.path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
        )
        parsed = UserStreamProof.parse(
            restarted_reader.snapshot(), now_epoch=NOW, baseline_epoch=BASELINE
        )
        self.assertEqual(2, len(parsed.events))
        self.assertTrue(parsed.external_activity_absent)

        self.clock.value += 6
        with self.assertRaisesRegex(BinanceSpotTruthError, "connected"):
            UserStreamProof.parse(
                restarted_reader.snapshot(),
                now_epoch=self.clock.value,
                baseline_epoch=BASELINE,
            )

    def test_wrong_writer_duplicate_or_recorded_gap_is_fail_closed(self) -> None:
        with self.assertRaisesRegex(BinanceSpotTruthError, "writer identity"):
            self.journal.ingest(
                raw_execution_event(c=OWNER_PREFIX),
                writer_id="stream-writer-b",
                writer_token=self.token,
            )
        self.journal.ingest(
            raw_execution_event(c=OWNER_PREFIX),
            writer_id="stream-writer-a",
            writer_token=self.token,
        )
        with self.assertRaisesRegex(BinanceSpotTruthError, "duplicate"):
            self.journal.ingest(
                raw_execution_event(c=OWNER_PREFIX),
                writer_id="stream-writer-a",
                writer_token=self.token,
            )
        self.journal.mark_gap(
            writer_id="stream-writer-a",
            writer_token=self.token,
            detail="queue overflow",
        )
        snapshot = self.journal.snapshot()
        self.assertTrue(snapshot["gapDetected"])
        self.assertFalse(snapshot["externalActivityAbsent"])
        with self.assertRaisesRegex(BinanceSpotTruthError, "gap"):
            UserStreamProof.parse(
                snapshot, now_epoch=NOW, baseline_epoch=BASELINE
            )

    def test_reconnect_after_baseline_cannot_forge_prebaseline_continuity(self) -> None:
        self.journal.ingest(
            raw_execution_event(c=OWNER_PREFIX),
            writer_id="stream-writer-a",
            writer_token=self.token,
        )
        self.clock.value += 30
        with self.assertRaisesRegex(BinanceSpotTruthError, "cannot be replaced"):
            self.journal.begin_authenticated_subscription(
                writer_id="stream-writer-reconnected",
                owner_prefix="",
                subscribed_epoch=self.clock.value,
            )
        snapshot = self.journal.snapshot()
        self.assertEqual(1, len(snapshot["events"]))
        self.assertEqual(SESSION_ID, snapshot["sessionId"])
        self.assertTrue(snapshot["gapDetected"])

    def test_missed_heartbeat_is_a_sticky_gap_not_cleared_by_late_heartbeat(self) -> None:
        self.clock.value += 6
        self.assertTrue(self.journal.snapshot()["gapDetected"])
        with self.assertRaisesRegex(BinanceSpotTruthError, "cannot clear"):
            self.journal.heartbeat(
                writer_id="stream-writer-a", writer_token=self.token
            )
        self.assertTrue(self.journal.snapshot()["gapDetected"])

    def test_nonowned_execution_is_durably_external_activity(self) -> None:
        self.journal.ingest(
            raw_execution_event(c="operator-external", i=909, t=9909),
            writer_id="stream-writer-a",
            writer_token=self.token,
        )
        self.assertFalse(self.journal.snapshot()["externalActivityAbsent"])

    def test_prebaseline_subscription_binds_exact_session_prefix_without_reset(self) -> None:
        unbound_path = Path(self.temporary.name) / "unbound-stream.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            unbound_path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
        )
        token = journal.begin_authenticated_subscription(
            writer_id="stream-writer-unbound",
            owner_prefix="",
            subscribed_epoch=BASELINE - 10,
        )
        journal.bind_functional_session(
            OWNER_PREFIX,
            SESSION_ID,
            PERMIT_ID,
            PERMIT_HASH,
            writer_id="stream-writer-unbound",
            writer_token=token,
        )
        journal.ingest(
            raw_execution_event(c=OWNER_PREFIX),
            writer_id="stream-writer-unbound",
            writer_token=token,
        )
        self.assertTrue(journal.snapshot()["externalActivityAbsent"])
        with self.assertRaisesRegex(BinanceSpotTruthError, "binding is invalid"):
            journal.bind_functional_session(
                "ftb-aaaaaaaaaaaa-",
                "bnsft-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                PERMIT_ID,
                PERMIT_HASH,
                writer_id="stream-writer-unbound",
                writer_token=token,
            )

    def test_execution_stream_ack_journals_raw_event_and_disconnect_blocks_truth(self) -> None:
        journal_path = Path(self.temporary.name) / "execution-hook.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            journal_path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
        )
        bridge = BinanceSpotDurableStreamBridge(
            journal, writer_id="execution-manager-writer", clock=self.clock
        )
        manager = ExecutionStreamManager(
            Path(self.temporary.name),
            binance_functional_stream_bridge=bridge,
        )
        manager._status["binance"] = {
            "running": True,
            "connected": False,
            "lastEventAt": "",
            "lastError": "",
            "reconnectCount": 0,
        }
        stop = threading.Event()

        class Socket:
            def __init__(self) -> None:
                self.frames = [
                    {"id": "request-id", "status": 200, "result": {}},
                    raw_execution_event(),
                ]

            def send(self, _: str) -> None:
                return None

            def recv(self) -> str:
                frame = self.frames.pop(0)
                if not self.frames:
                    stop.set()
                return json.dumps(frame)

            def close(self) -> None:
                return None

        with patch.dict(
            "os.environ",
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ), patch(
            "live_trader.execution_streams.binance_stream_subscription_params",
            return_value={},
        ), patch(
            "live_trader.execution_streams.uuid.uuid4",
            return_value="request-id",
        ), patch(
            "websocket.create_connection", return_value=Socket()
        ):
            manager._run_binance(stop)

        snapshot = journal.snapshot()
        self.assertEqual(1, len(snapshot["events"]))
        self.assertTrue(snapshot["gapDetected"])
        self.assertFalse(snapshot["connected"])
        with self.assertRaisesRegex(BinanceSpotTruthError, "connected"):
            UserStreamProof.parse(
                snapshot, now_epoch=NOW, baseline_epoch=BASELINE
            )

    def test_disconnect_blocks_order_truth_even_after_all_fresh_rest_reads(self) -> None:
        path = Path(self.temporary.name) / "disconnect-reconcile.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            path, account_fingerprint=FINGERPRINT, clock=self.clock
        )
        bridge = BinanceSpotDurableStreamBridge(
            journal, writer_id="disconnect-writer", clock=self.clock
        )
        bridge.on_subscription_confirmed()
        bridge.bind_functional_session(
            OWNER_PREFIX, SESSION_ID, PERMIT_ID, PERMIT_HASH
        )
        fake = FakeClient()
        fake.payloads["/api/v3/allOrders"] = []
        fake.payloads["/api/v3/myTrades"] = []
        reader = BinanceSpotOfficialTruthReader(
            client=fake,  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=bridge.snapshot,
            clock=self.clock,
        )
        reader.read(baseline_epoch=NOW, owner_prefix=OWNER_PREFIX)
        bridge.on_disconnect("test disconnect")
        calls_before = len(fake.calls)
        with self.assertRaisesRegex(BinanceSpotTruthError, "connected"):
            reader.read(baseline_epoch=NOW, owner_prefix=OWNER_PREFIX)
        # Every official REST read was fresh again; it still cannot replace a
        # continuous authenticated stream proof or authorize an order.
        self.assertEqual(calls_before + 6, len(fake.calls))

    def test_terminal_retirement_archives_history_then_allows_new_prebaseline_epoch(self) -> None:
        path = Path(self.temporary.name) / "terminal-retire.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
            terminal_verifier=lambda attestation: (
                attestation["sessionId"] == SESSION_ID
                and attestation["terminalReason"] == "FINALIZED"
            ),
        )
        token = journal.begin_authenticated_subscription(
            writer_id="terminal-writer",
            owner_prefix="",
            subscribed_epoch=BASELINE - 10,
        )
        journal.bind_functional_session(
            OWNER_PREFIX,
            SESSION_ID,
            PERMIT_ID,
            PERMIT_HASH,
            writer_id="terminal-writer",
            writer_token=token,
        )
        journal.ingest(
            raw_execution_event(c=OWNER_PREFIX),
            writer_id="terminal-writer",
            writer_token=token,
        )
        journal.record_terminal_marker(
            writer_id="terminal-writer",
            writer_token=token,
            marker_id="binance-terminal-test-retirement-0001",
            server_time_ms=int(self.clock.value * 1000),
        )
        final_stream_seal = journal.snapshot()["durableJournalSealHash"]
        retired = journal.retire_terminal_session(
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            final_evidence_hash="e" * 64,
            expected_journal_seal_hash=str(final_stream_seal),
        )
        self.assertTrue(retired["retired"])
        with self.assertRaisesRegex(BinanceSpotTruthError, "retired"):
            journal.snapshot()
        self.clock.value += 1
        second_token = journal.begin_authenticated_subscription(
            writer_id="next-writer",
            owner_prefix="",
            subscribed_epoch=self.clock.value,
        )
        self.assertGreater(len(second_token), 24)
        second = journal.snapshot()
        self.assertEqual("", second["sessionId"])
        self.assertEqual([], second["events"])
        connection = sqlite3.connect(path)
        try:
            archive_count = connection.execute(
                "SELECT COUNT(*) FROM binance_spot_stream_journal_archives"
            ).fetchone()[0]
            event_count = connection.execute(
                "SELECT COUNT(*) FROM binance_spot_stream_journal_archive_events"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(1, archive_count)
        self.assertEqual(1, event_count)

    def test_terminal_retirement_without_backend_attestation_is_rejected(self) -> None:
        with self.assertRaisesRegex(BinanceSpotTruthError, "attestation"):
            self.journal.retire_terminal_session(
                session_id=SESSION_ID,
                permit_id=PERMIT_ID,
                permit_hash=PERMIT_HASH,
                final_evidence_hash="e" * 64,
            )
        self.assertEqual(SESSION_ID, self.journal.snapshot()["sessionId"])

    def test_start_failed_archives_unbound_prebaseline_stream_idempotently(self) -> None:
        path = Path(self.temporary.name) / "startup-unbound-retire.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
            terminal_verifier=lambda attestation: (
                attestation["sessionId"] == SESSION_ID
                and attestation["terminalReason"] == "START_FAILED"
            ),
        )
        journal.begin_authenticated_subscription(
            writer_id="startup-writer",
            owner_prefix="",
            subscribed_epoch=BASELINE - 10,
        )
        retired = journal.retire_terminal_session(
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            final_evidence_hash="f" * 64,
            terminal_reason="START_FAILED",
        )
        self.assertTrue(retired["retired"])
        repeated = journal.retire_terminal_session(
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            final_evidence_hash="f" * 64,
            terminal_reason="START_FAILED",
        )
        self.assertTrue(repeated["retired"])
        self.clock.value += 1
        next_token = journal.begin_authenticated_subscription(
            writer_id="next-startup-writer",
            owner_prefix="",
            subscribed_epoch=self.clock.value,
        )
        self.assertGreater(len(next_token), 24)

    def test_late_stream_event_after_final_snapshot_blocks_stale_retirement(self) -> None:
        path = Path(self.temporary.name) / "late-event-final-race.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
            terminal_verifier=lambda attestation: (
                attestation["sessionId"] == SESSION_ID
                and attestation["terminalReason"] == "FINALIZED"
            ),
        )
        token = journal.begin_authenticated_subscription(
            writer_id="late-event-writer",
            owner_prefix="",
            subscribed_epoch=BASELINE - 10,
        )
        journal.bind_functional_session(
            OWNER_PREFIX,
            SESSION_ID,
            PERMIT_ID,
            PERMIT_HASH,
            writer_id="late-event-writer",
            writer_token=token,
        )
        journal.record_terminal_marker(
            writer_id="late-event-writer",
            writer_token=token,
            marker_id="binance-terminal-late-race-0001",
            server_time_ms=int(self.clock.value * 1000),
        )
        stale_seal = str(journal.snapshot()["durableJournalSealHash"])
        late = raw_execution_event(c="external-late-order")
        late["i"] = 99999
        late["t"] = 88888
        journal.ingest(
            late,
            writer_id="late-event-writer",
            writer_token=token,
        )
        with self.assertRaisesRegex(BinanceSpotTruthError, "final truth cutoff"):
            journal.retire_terminal_session(
                session_id=SESSION_ID,
                permit_id=PERMIT_ID,
                permit_hash=PERMIT_HASH,
                final_evidence_hash="d" * 64,
                expected_journal_seal_hash=stale_seal,
            )
        self.assertEqual(SESSION_ID, journal.snapshot()["sessionId"])

    def test_terminal_barrier_drains_frame_received_before_callback_and_archives_it(self) -> None:
        path = Path(self.temporary.name) / "terminal-inbound-barrier.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
            terminal_verifier=lambda attestation: (
                attestation["sessionId"] == SESSION_ID
                and attestation["terminalReason"] == "FINALIZED"
            ),
        )
        bridge = BinanceSpotDurableStreamBridge(
            journal, writer_id="barrier-writer", clock=self.clock
        )
        bridge.on_subscription_confirmed()
        bridge.bind_functional_session(
            OWNER_PREFIX, SESSION_ID, PERMIT_ID, PERMIT_HASH
        )
        # The socket has already returned this frame and fenced it, but the
        # callback is deliberately delayed behind the terminal operation.
        ticket = bridge.begin_inbound_frame()
        completed = threading.Event()
        errors: list[Exception] = []

        def barrier_worker() -> None:
            try:
                bridge.close_terminal_intake(timeout_seconds=2)
            except Exception as exc:  # pragma: no cover - assertion below
                errors.append(exc)
            finally:
                completed.set()

        thread = threading.Thread(target=barrier_worker)
        thread.start()
        time.sleep(0.02)
        self.assertFalse(completed.is_set())
        bridge.on_payload(raw_execution_event(c=OWNER_PREFIX))
        bridge.finish_inbound_frame(ticket)
        thread.join(2)
        self.assertFalse(errors)
        self.assertTrue(completed.is_set())
        # The production reader owns this marker.  This focused bridge test
        # records the same ordered ACK explicitly after the queued callback.
        bridge._accepting_inbound = True
        bridge.on_terminal_marker(
            marker_id="binance-terminal-drain-test-0001",
            server_time_ms=int(self.clock.value * 1000),
        )
        bridge.close_terminal_intake(timeout_seconds=2)
        snapshot = bridge.snapshot()
        self.assertEqual(1, snapshot["durableJournalEventCount"])
        retired = bridge.retire_terminal_session(
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            final_evidence_hash="7" * 64,
            expected_journal_seal_hash=snapshot["durableJournalSealHash"],
        )
        self.assertTrue(retired["retired"])

    def test_idle_socket_renews_only_after_verified_application_pong(self) -> None:
        events: list[str] = []
        stop = threading.Event()

        class Bridge:
            def on_subscription_confirmed(self) -> None:
                events.append("ack")

            def on_transport_liveness(self) -> None:
                events.append("verified-pong")

            def on_payload(self, payload: dict[str, object]) -> None:
                events.append("payload")

            def on_disconnect(self, detail: str) -> None:
                events.append("disconnect")

        class WebSocketTimeoutException(Exception):
            pass

        class Socket:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.reads = 0

            def send(self, raw: str) -> None:
                self.sent.append(json.loads(raw))

            def recv(self) -> str:
                self.reads += 1
                if self.reads == 1:
                    return json.dumps(
                        {"id": "request-id", "status": 200, "result": {}}
                    )
                if self.reads == 2:
                    raise WebSocketTimeoutException()
                liveness = self.sent[-1]
                stop.set()
                return json.dumps(
                    {
                        "id": liveness["id"],
                        "status": 200,
                        "result": {"serverTime": int(NOW * 1000)},
                    }
                )

            def close(self) -> None:
                return None

        manager = ExecutionStreamManager(
            Path(self.temporary.name),
            binance_functional_stream_bridge=Bridge(),
        )
        manager._status["binance"] = {
            "running": True,
            "connected": False,
            "lastEventAt": "",
            "lastError": "",
            "reconnectCount": 0,
        }
        socket = Socket()
        with patch.dict(
            "os.environ",
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ), patch(
            "live_trader.execution_streams.binance_stream_subscription_params",
            return_value={},
        ), patch(
            "live_trader.execution_streams.uuid.uuid4",
            side_effect=["request-id", "liveness-id"],
        ), patch("websocket.create_connection", return_value=socket):
            manager._run_binance(stop)
        self.assertEqual(["ack", "verified-pong", "disconnect"], events)
        self.assertEqual("time", socket.sent[-1]["method"])

    def test_reader_owned_in_band_marker_drains_socket_order_before_terminal_return(self) -> None:
        path = Path(self.temporary.name) / "reader-owned-terminal.sqlite3"
        journal = DurableBinanceSpotUserStreamJournal(
            path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
            terminal_verifier=lambda _: True,
        )
        bridge = BinanceSpotDurableStreamBridge(
            journal, writer_id="reader-owned-writer", clock=self.clock
        )
        manager = ExecutionStreamManager(
            Path(self.temporary.name),
            binance_functional_stream_bridge=bridge,
        )
        stop = threading.Event()

        class WebSocketTimeoutException(Exception):
            pass

        class Socket:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.responses: list[str] = []
                self.condition = threading.Condition()

            def send(self, raw: str) -> None:
                command = json.loads(raw)
                with self.condition:
                    self.sent.append(command)
                    if command.get("method") == "userDataStream.subscribe.signature":
                        self.responses.append(
                            json.dumps(
                                {
                                    "id": command["id"],
                                    "status": 200,
                                    "result": {},
                                }
                            )
                        )
                        self.condition.notify_all()
                    elif command.get("method") == "time":
                        # This event was already queued by the venue ahead of
                        # the marker response.  The reader must journal it
                        # before returning the terminal cutoff.
                        if str(command["id"]).startswith(
                            "binance-terminal-"
                        ):
                            self.responses.append(
                                json.dumps(
                                    raw_execution_event(c=OWNER_PREFIX)
                                )
                            )
                        self.responses.append(
                            json.dumps(
                                {
                                    "id": command["id"],
                                    "status": 200,
                                    "result": {
                                        "serverTime": int(
                                            self_clock.value * 1000
                                        )
                                    },
                                }
                            )
                        )
                        self.condition.notify_all()

            def recv(self) -> str:
                with self.condition:
                    if not self.responses:
                        self.condition.wait(0.01)
                    if not self.responses:
                        raise WebSocketTimeoutException()
                    return self.responses.pop(0)

            def close(self) -> None:
                return None

        self_clock = self.clock
        socket = Socket()
        manager._status["binance"] = {
            "running": True,
            "connected": False,
            "lastEventAt": "",
            "lastError": "",
            "reconnectCount": 0,
        }
        worker = threading.Thread(
            target=manager._run_binance, args=(stop,), daemon=True
        )
        with patch.dict(
            "os.environ",
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ), patch(
            "live_trader.execution_streams.binance_stream_subscription_params",
            return_value={},
        ), patch("websocket.create_connection", return_value=socket):
            manager._stop_events["binance"] = stop
            manager._threads["binance"] = worker
            worker.start()
            deadline = time.time() + 2
            while time.time() < deadline:
                try:
                    if bridge.snapshot().get("authenticated") is True:
                        break
                except Exception:
                    pass
                time.sleep(0.005)
            bridge.bind_functional_session(
                OWNER_PREFIX, SESSION_ID, PERMIT_ID, PERMIT_HASH
            )
            barrier = manager.begin_binance_functional_terminal_barrier(
                timeout=2
            )
        self.assertTrue(barrier["inBandMarkerReceived"])
        self.assertTrue(barrier["stream"]["terminalMarkerAcknowledged"])
        self.assertEqual(1, barrier["stream"]["durableJournalEventCount"])
        self.assertFalse(worker.is_alive())

    def test_recovered_gap_can_archive_with_preserved_journal_seal_only(self) -> None:
        self.journal.mark_bound_owner_loss_gap(
            session_id=SESSION_ID,
            detail="process owner and socket lost",
        )
        preserved = self.journal.snapshot()
        retired = DurableBinanceSpotUserStreamJournal(
            self.path,
            account_fingerprint=FINGERPRINT,
            clock=self.clock,
            terminal_verifier=lambda attestation: (
                attestation["terminalReason"] == "RECOVERED_FINALIZED"
            ),
        ).retire_terminal_session(
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            final_evidence_hash="6" * 64,
            terminal_reason="RECOVERED_FINALIZED",
            expected_journal_seal_hash=preserved[
                "durableJournalSealHash"
            ],
        )
        self.assertTrue(retired["retired"])

    def test_half_open_socket_without_inbound_pong_never_renews_continuity(self) -> None:
        events: list[str] = []

        class Bridge:
            def on_subscription_confirmed(self) -> None:
                events.append("ack")

            def on_transport_liveness(self) -> None:
                events.append("verified-pong")

            def on_payload(self, payload: dict[str, object]) -> None:
                events.append("payload")

            def on_disconnect(self, detail: str) -> None:
                events.append("disconnect")

        class WebSocketTimeoutException(Exception):
            pass

        class Socket:
            def __init__(self) -> None:
                self.reads = 0

            def send(self, raw: str) -> None:
                return None

            def recv(self) -> str:
                self.reads += 1
                if self.reads == 1:
                    return json.dumps(
                        {"id": "request-id", "status": 200, "result": {}}
                    )
                raise WebSocketTimeoutException()

            def close(self) -> None:
                return None

        manager = ExecutionStreamManager(
            Path(self.temporary.name),
            binance_functional_stream_bridge=Bridge(),
        )
        manager._status["binance"] = {
            "running": True,
            "connected": False,
            "lastEventAt": "",
            "lastError": "",
            "reconnectCount": 0,
        }
        with patch.dict(
            "os.environ",
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ), patch(
            "live_trader.execution_streams.binance_stream_subscription_params",
            return_value={},
        ), patch(
            "live_trader.execution_streams.uuid.uuid4",
            side_effect=["request-id", "liveness-id"],
        ), patch(
            "live_trader.execution_streams.time.monotonic",
            side_effect=[0.0, 2.0, 5.0],
        ), patch("websocket.create_connection", return_value=Socket()):
            with self.assertRaisesRegex(RuntimeError, "deadline missed"):
                manager._run_binance(threading.Event())
        self.assertEqual(["ack", "disconnect"], events)


if __name__ == "__main__":
    unittest.main()
