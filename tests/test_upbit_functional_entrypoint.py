from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
import hashlib
import tempfile
import threading
import time
import unittest

from live_trader.upbit_continuous_functional import (
    FinalizedFiveMinuteBar,
    UpbitFunctionalBlocked,
    UpbitFunctionalLedger,
    _activate_for_test,
)
from live_trader.upbit_functional_transport import DurableUpbitMyOrderJournal
from live_trader.upbit_functional_entrypoint import (
    UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE,
    UpbitFunctionalProductionGraph,
    build_upbit_functional_production_graph,
    production_entrypoint_status,
)
from live_trader.upbit_functional_mutation import (
    UPBIT_FUNCTIONAL_MUTATION_AVAILABLE,
)
from tests.test_upbit_continuous_functional import (
    ACCOUNT,
    NOW,
    FakeBoundaries,
    TEST_EXCLUSIVITY_VERIFIER,
    TEST_EXCLUSIVITY_VERIFIER_PIN,
    UpbitContinuousFunctionalTest,
    permit,
)


class UpbitFunctionalEntrypointTest(unittest.TestCase):
    def test_scheduler_start_failure_requires_durable_cleanup_capability_revoked(
        self,
    ) -> None:
        graph = object.__new__(UpbitFunctionalProductionGraph)
        graph._lock = threading.RLock()
        graph.account_fingerprint = ACCOUNT
        calls: list[str] = []

        class Controller:
            @staticmethod
            def snapshot():
                return {
                    "status": "ACTIVE",
                    "sessionId": "upbit-scheduler-failed-session-0001",
                }

        class Journal:
            @staticmethod
            def startup_fail_closed(**kwargs):
                calls.append("journal:" + str(kwargs["detail"]))

        class Ledger:
            @staticmethod
            def session(_session_id):
                return {"state": "CLEANUP", "capability_hash": ""}

        graph.controller = Controller()
        graph.journal = Journal()
        graph.ledger = Ledger()
        graph._fail_private_stream_liveness = lambda *, detail: calls.append(
            "revoke:" + detail
        )
        result = graph.fail_closed_scheduler_start(
            reason="scheduler-start-failed"
        )
        self.assertEqual(
            [
                "revoke:scheduler-start-failed",
                "journal:scheduler-start-failed",
            ],
            calls,
        )
        self.assertEqual(
            "RECONCILIATION_REQUIRED", result["snapshot"]["status"]
        )

    def test_direct_production_graph_is_blocked_before_database_mutation(self) -> None:
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "must-not-exist.sqlite3"
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "direct-construction-forbidden"
            ):
                build_upbit_functional_production_graph(
                    database_path=database,
                    publication_proof_path=Path(temporary) / "proof.json",
                    account_fingerprint=ACCOUNT,
                    clock=lambda: NOW,
                    runtime_reader=lambda: {},
                    runtime_capability_registrar=lambda _value: calls.append("register"),
                    enter_cleanup_latch=lambda: calls.append("cleanup"),
                    disarm_functional_orders=lambda: calls.append("disarm"),
                    functional_orders_reader=lambda: False,
                    lease_reader_factory=lambda **_kwargs: None,
                    sender=lambda *_args, **_kwargs: {},
                    approved_permit_reader=lambda *_args: {},
                    approved_recovery_reader=lambda *_args: {},
                    websocket_handshake=lambda **_kwargs: {},
                    finalized_bar_window_reader=lambda: {},
                )
            self.assertFalse(database.exists())
            self.assertEqual([], calls)

    @staticmethod
    def graph(path: Path, *, calls: list[str]):
        safe_runtime = {
            "newEntriesBlocked": True,
            "realOrdersEnabled": False,
            "functionalMutationEnabled": False,
            "functionalCapabilityHash": "",
        }
        return build_upbit_functional_production_graph(
            allow_mock_graph=True,
            database_path=path,
            publication_proof_path=path.parent / "missing-proof.json",
            account_fingerprint=ACCOUNT,
            clock=lambda: NOW,
            runtime_reader=lambda: dict(safe_runtime),
            runtime_capability_registrar=lambda value: calls.append(
                f"register:{value}"
            ),
            enter_cleanup_latch=lambda: calls.append("cleanup"),
            disarm_functional_orders=lambda: calls.append("disarm"),
            functional_orders_reader=lambda: False,
            approved_permit_reader=lambda *_args: {},
            approved_recovery_reader=lambda *_args: {},
            websocket_handshake=lambda **_kwargs: {},
            finalized_bar_window_reader=lambda: {},
            lease_reader_factory=lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("audit must not lease")
            ),
            sender=lambda _request: (_ for _ in ()).throw(
                AssertionError("audit must not use network")
            ),
            clear_runtime_capability=lambda: calls.append("clear"),
        )

    def test_graph_constructs_offline_but_start_is_hard_unavailable(self) -> None:
        self.assertFalse(UPBIT_FUNCTIONAL_ENTRYPOINT_AVAILABLE)
        self.assertFalse(UPBIT_FUNCTIONAL_MUTATION_AVAILABLE)
        self.assertFalse(production_entrypoint_status()["available"])
        self.assertFalse(
            production_entrypoint_status()["verifierAuthorityPinned"]
        )
        self.assertFalse(
            production_entrypoint_status()[
                "productionExclusivityVerifierWired"
            ]
        )
        self.assertFalse(
            production_entrypoint_status()["accountExclusivityPreSendReady"]
        )
        self.assertEqual(
            "PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_NOT_WIRED",
            production_entrypoint_status()["reason"],
        )
        self.assertFalse(
            production_entrypoint_status()["networkOrderPostAllowed"]
        )
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            graph = build_upbit_functional_production_graph(
                allow_mock_graph=True,
                database_path=Path(temporary) / "upbit-production.sqlite3",
                publication_proof_path=Path(temporary) / "missing-proof.json",
                account_fingerprint=ACCOUNT,
                clock=lambda: NOW,
                runtime_reader=lambda: calls.append("runtime")
                or {
                    "newEntriesBlocked": True,
                    "realOrdersEnabled": False,
                    "functionalMutationEnabled": False,
                    "functionalCapabilityHash": "",
                },
                runtime_capability_registrar=lambda _value: calls.append(
                    "register"
                ),
                enter_cleanup_latch=lambda: calls.append("cleanup"),
                disarm_functional_orders=lambda: calls.append("disarm"),
                functional_orders_reader=lambda: calls.append("orders") or False,
                approved_permit_reader=lambda _permit_id, _permit_hash: (
                    _ for _ in ()
                ).throw(AssertionError("unavailable start must not read permit")),
                approved_recovery_reader=lambda *_args: (
                    _ for _ in ()
                ).throw(AssertionError("unavailable start must not read recovery")),
                websocket_handshake=lambda **_kwargs: (
                    _ for _ in ()
                ).throw(AssertionError("unavailable start must not handshake")),
                finalized_bar_window_reader=lambda: (
                    _ for _ in ()
                ).throw(AssertionError("unavailable start must not read bars")),
                lease_reader_factory=lambda **_kwargs: (_ for _ in ()).throw(
                    AssertionError("unavailable start must not lease")
                ),
                sender=lambda _request: (_ for _ in ()).throw(
                    AssertionError("unavailable start must not use network")
                ),
            )
            status = graph.status()
            self.assertFalse(status["available"])
            self.assertFalse(status["legacyOneShotImported"])
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "entrypoint-unavailable"
            ):
                graph.start(permit={}, session_id="session-id")
        self.assertEqual(
            ["disarm", "register", "runtime", "orders"],
            calls,
        )

    def test_startup_audit_aborts_orphan_journal_without_network(self) -> None:
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "startup-orphan.sqlite3"
            journal = DurableUpbitMyOrderJournal(path, clock=lambda: NOW)
            journal.begin_authenticated_session(
                session_id="upbit-orphan-journal-session-0001",
                account_fingerprint=ACCOUNT,
                started_at=NOW,
            )
            graph = self.graph(path, calls=calls)
            audit = graph.status()["startupAudit"]
            self.assertEqual("ORPHAN_JOURNAL_ABORTED", audit["actions"][0]["action"])
            self.assertEqual([], journal.active_sessions())
        self.assertIn("disarm", calls)
        self.assertIn("register:", calls)
        self.assertIn("clear", calls)

    def test_startup_audit_revokes_orphan_active_and_resumes_final_reset(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "startup-durable.sqlite3"
            functional_permit = permit()
            fake = FakeBoundaries(functional_permit)
            ledger = UpbitFunctionalLedger(path)
            _activate_for_test(
                permit=functional_permit,
                ledger=ledger,
                session_id=fake.session_id,
                truth_reader=fake.truth,
                post_order=fake.post,
                cancel_order=fake.cancel,
                lease_factory=fake.lease,
                runtime_reader=fake.runtime,
                immutable_selection_reader=fake.immutable_selection,
                runtime_capability_registrar=fake.register_capability,
                real_orders_reader=lambda: fake.real_orders,
                clock=fake.clock,
                account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
                account_exclusivity_verifier_pin=TEST_EXCLUSIVITY_VERIFIER_PIN,
            )
            calls: list[str] = []
            graph = self.graph(path, calls=calls)
            durable = ledger.session(fake.session_id)
            self.assertEqual("CLEANUP", durable["state"])
            self.assertEqual("", durable["capability_hash"])
            self.assertTrue(graph.status()["startupAudit"]["recoveryRequired"])
            self.assertIn("disarm", calls)
            self.assertIn("register:", calls)

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "startup-final.sqlite3"
            functional_permit = permit()
            fake = FakeBoundaries(functional_permit)
            ledger = UpbitFunctionalLedger(path)
            service = _activate_for_test(
                permit=functional_permit,
                ledger=ledger,
                session_id=fake.session_id,
                truth_reader=fake.truth,
                post_order=fake.post,
                cancel_order=fake.cancel,
                lease_factory=fake.lease,
                runtime_reader=fake.runtime,
                immutable_selection_reader=fake.immutable_selection,
                runtime_capability_registrar=fake.register_capability,
                real_orders_reader=lambda: fake.real_orders,
                clock=fake.clock,
                account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
                account_exclusivity_verifier_pin=TEST_EXCLUSIVITY_VERIFIER_PIN,
            )
            service.recover_or_expire(reason="operator-stop")
            journal = DurableUpbitMyOrderJournal(path, clock=fake.clock)
            writer = journal.begin_authenticated_session(
                session_id=fake.session_id,
                account_fingerprint=ACCOUNT,
                started_at=fake.now,
            )
            journal.attest_authenticated_connection(
                fake.session_id,
                writer_token=str(writer["writerToken"]),
                writer_generation=int(writer["writerGeneration"]),
            )
            terminal = journal.prepare_terminal_attestation(
                session_id=fake.session_id,
                identifiers=(),
            )
            evidence = {
                "schemaVersion": "upbit-final-test-evidence/v1",
                "ok": True,
                "terminalPrivateStreamSeal": terminal,
                "terminalPrivateStreamSealHash": terminal["sealHash"],
            }
            ledger.begin_final_reset(fake.session_id, evidence)
            graph = self.graph(path, calls=[])
            self.assertEqual("FINALIZED", ledger.session(fake.session_id)["state"])
            self.assertEqual(
                "FINAL_RESET_COMPLETED",
                graph.status()["startupAudit"]["actions"][0]["action"],
            )

    def test_final_reset_startup_requires_observed_authority_clear(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "startup-final-lying-clear.sqlite3"
            functional_permit = permit()
            fake = FakeBoundaries(functional_permit)
            ledger = UpbitFunctionalLedger(path)
            service = _activate_for_test(
                permit=functional_permit,
                ledger=ledger,
                session_id=fake.session_id,
                truth_reader=fake.truth,
                post_order=fake.post,
                cancel_order=fake.cancel,
                lease_factory=fake.lease,
                runtime_reader=fake.runtime,
                immutable_selection_reader=fake.immutable_selection,
                runtime_capability_registrar=fake.register_capability,
                real_orders_reader=lambda: fake.real_orders,
                clock=fake.clock,
                account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
                account_exclusivity_verifier_pin=TEST_EXCLUSIVITY_VERIFIER_PIN,
            )
            service.recover_or_expire(reason="operator-stop")
            ledger.begin_final_reset(
                fake.session_id,
                {"schemaVersion": "upbit-final-test-evidence/v1", "ok": True},
            )
            fake.real_orders = True
            fake.runtime_updates.update(
                {
                    "newEntriesBlocked": True,
                    "realOrdersEnabled": True,
                    "functionalMutationEnabled": True,
                    "functionalCapabilityHash": "e" * 64,
                }
            )
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "authority-reset-not-observed"
            ):
                build_upbit_functional_production_graph(
                    allow_mock_graph=True,
                    database_path=path,
                    publication_proof_path=path.parent / "missing-proof.json",
                    account_fingerprint=ACCOUNT,
                    clock=fake.clock,
                    runtime_reader=fake.runtime,
                    runtime_capability_registrar=lambda _value: None,
                    enter_cleanup_latch=lambda: None,
                    disarm_functional_orders=lambda: None,
                    functional_orders_reader=lambda: fake.real_orders,
                    approved_permit_reader=lambda *_args: {},
                    approved_recovery_reader=lambda *_args: {},
                    websocket_handshake=lambda **_kwargs: {},
                    finalized_bar_window_reader=lambda: {},
                    lease_reader_factory=fake.lease,
                    sender=lambda _request: {},
                    clear_runtime_capability=lambda: None,
                )
            self.assertEqual(
                "FINAL_RESET_PENDING", ledger.session(fake.session_id)["state"]
            )

    def test_socket_owned_liveness_renews_and_stale_frame_is_sticky_gap(self) -> None:
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "liveness.sqlite3"
            graph = self.graph(path, calls=calls)
            session_id = "upbit-liveness-session-0001"
            writer = graph.journal.begin_authenticated_session(
                session_id=session_id,
                account_fingerprint=ACCOUNT,
                started_at=NOW,
            )
            token_hash = hashlib.sha256(
                str(writer["writerToken"]).encode("utf-8")
            ).hexdigest()
            liveness = {
                "sessionId": session_id,
                "writerGeneration": writer["writerGeneration"],
                "writerTokenHash": token_hash,
                "connected": True,
                "authenticated": True,
                "myOrderSubscribed": True,
                "lastFrameAt": NOW.isoformat().replace("+00:00", "Z"),
            }
            graph._UpbitFunctionalProductionGraph__journal_writer = dict(writer)
            graph.controller.snapshot = lambda: {
                "status": "STOPPED",
                "sessionId": session_id,
            }
            graph._bind_authenticated_private_stream(
                session_id=session_id,
                writer=writer,
                handshake={
                    **liveness,
                    "livenessReader": lambda: dict(liveness),
                    "closePump": lambda: None,
                },
            )
            graph._refresh_private_stream_liveness()
            self.assertTrue(
                graph.journal.snapshot(
                    session_id=session_id, identifiers=()
                )["eventsComplete"]
            )
            liveness["lastFrameAt"] = (
                NOW - timedelta(seconds=11)
            ).isoformat().replace("+00:00", "Z")
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "private-stream-liveness-lost"
            ):
                graph._refresh_private_stream_liveness()
            snapshot = graph.journal.snapshot(
                session_id=session_id, identifiers=()
            )
            self.assertTrue(snapshot["gapDetected"])
            self.assertFalse(snapshot["eventsComplete"])

    def test_scheduler_duplicate_finalized_window_is_no_new_bar(self) -> None:
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "duplicate-window.sqlite3"
            graph = self.graph(path, calls=calls)
            functional_permit = permit()
            fake = FakeBoundaries(functional_permit)
            service = _activate_for_test(
                permit=functional_permit,
                ledger=graph.ledger,
                session_id=fake.session_id,
                truth_reader=fake.truth,
                post_order=fake.post,
                cancel_order=fake.cancel,
                lease_factory=fake.lease,
                runtime_reader=fake.runtime,
                immutable_selection_reader=fake.immutable_selection,
                runtime_capability_registrar=fake.register_capability,
                real_orders_reader=lambda: fake.real_orders,
                clock=fake.clock,
                account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
                account_exclusivity_verifier_pin=TEST_EXCLUSIVITY_VERIFIER_PIN,
            )
            graph.controller._attach_for_test(service)
            writer = graph.journal.begin_authenticated_session(
                session_id=fake.session_id,
                account_fingerprint=ACCOUNT,
                started_at=NOW,
            )
            token_hash = hashlib.sha256(
                str(writer["writerToken"]).encode("utf-8")
            ).hexdigest()
            live = {
                "sessionId": fake.session_id,
                "writerGeneration": writer["writerGeneration"],
                "writerTokenHash": token_hash,
                "connected": True,
                "authenticated": True,
                "myOrderSubscribed": True,
                "lastFrameAt": NOW.isoformat().replace("+00:00", "Z"),
            }
            graph._UpbitFunctionalProductionGraph__journal_writer = dict(writer)
            graph._bind_authenticated_private_stream(
                session_id=fake.session_id,
                writer=writer,
                handshake={
                    **live,
                    "livenessReader": lambda: dict(live),
                    "closePump": lambda: None,
                },
            )

            sealed_duplicate = FinalizedFiveMinuteBar.parse(
                UpbitContinuousFunctionalTest.bar(
                    "HOLD",
                    bar_id="upbit-five-minute-bar-duplicate-0001",
                ),
                now=fake.now,
                strategy_artifact_id="crypto-btc-finalized-5m",
                strategy_artifact_hash="a" * 64,
                strategy_artifact_file_sha256="f" * 64,
                strategy_instance_id="crypto-btc-finalized-5m-instance",
                strategy_instance_hash="d" * 64,
                strategy_instance_file_sha256="9" * 64,
                publication_proof_hash="1" * 64,
                publication_proof_file_sha256="2" * 64,
            )
            graph.ledger.note_bar(fake.session_id, sealed_duplicate)

            class DuplicateEvaluator:
                @staticmethod
                def evaluate(_window):
                    return {"barId": sealed_duplicate.bar_id}

            graph._UpbitFunctionalProductionGraph__strategy_evaluator = (
                DuplicateEvaluator()
            )
            result = graph.pump()
            self.assertEqual("NO_NEW_BAR", result["result"]["action"])

    def test_public_lifecycle_start_is_single_owner_serialized(self) -> None:
        calls: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            graph = self.graph(
                Path(temporary) / "lifecycle-lock.sqlite3", calls=calls
            )
            guard = threading.Lock()
            active = 0
            maximum = 0

            def fake_start(**activation):
                nonlocal active, maximum
                with guard:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.05)
                with guard:
                    active -= 1
                return dict(activation)

            graph._start_locked = fake_start
            barrier = threading.Barrier(2)

            def invoke(index: int):
                barrier.wait()
                return graph.start(index=index)

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(invoke, (1, 2)))
            self.assertEqual(1, maximum)
            self.assertEqual({1, 2}, {row["index"] for row in results})


if __name__ == "__main__":
    unittest.main()
