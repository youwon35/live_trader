from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

from live_trader import kis_order_authority as kis_order_authority_module
from live_trader.kis_domestic_functional_state import (
    DurableKisDomesticFunctionalState,
    KisDomesticFunctionalStateBlocked,
    production_entrypoint_status,
)
from live_trader.kis_order_authority import (
    KisOrderAuthorityError,
    _reset_kis_order_authority_reader_for_tests,
    kis_route_authority_serialization,
    ordinary_kis_final_mutation_boundary,
    register_kis_order_authority_reader,
)
from live_trader.program_ledger import ProgramLedger


ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
OWNERS = {
    "graph": "kis-graph-owner-v1", "backend": "kis-backend-owner-v1",
    "capability": "kis-capability-owner-v1", "transport": "kis-transport-owner-v1",
}
RECEIPT = "f" * 64
STATE_KEY = b"isolated-kis-state-signing-key-0001"
NOW = datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc)


class KisFunctionalStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.original_kis_authority_reader = (
            kis_order_authority_module._AUTHORITY_READER
        )
        self.original_kis_kill_cancel_journal_path = (
            kis_order_authority_module._KILL_CANCEL_JOURNAL_PATH
        )
        self.addCleanup(self._restore_kis_authority_provider)
        self.path = Path(self.temp.name) / "state.sqlite3"
        self.ledger = ProgramLedger(self.path)
        self.account = ACCOUNT; self.credential = CREDENTIAL; self.session = ""
        self.hazards = {name: [] for name in OWNERS}
        self.graph_intent = {}
        self.kill_allowed = False; self.kill_revision = 0; self.kill_intent = {}
        self.unreadable = set()
        self.readers = {name: self._reader(name) for name in OWNERS}
        self.state = self._new_state()
        _reset_kis_order_authority_reader_for_tests()
        register_kis_order_authority_reader(self.state.authority_snapshot)

    def tearDown(self) -> None:
        _reset_kis_order_authority_reader_for_tests()

    def _restore_kis_authority_provider(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        if self.original_kis_authority_reader is not None:
            register_kis_order_authority_reader(
                self.original_kis_authority_reader,
                kill_cancel_journal_path=(
                    self.original_kis_kill_cancel_journal_path
                ),
            )

    def _reader(self, name):
        def read():
            if name in self.unreadable:
                raise OSError("reader down")
            return {
                "schemaVersion": "kis-domestic-functional-component-status/v1",
                "component": name,
                "ownerHash": hashlib.sha256(OWNERS[name].encode()).hexdigest(),
                "route": "KIS_KR_LIVE_CONTINUOUS", "readable": True,
                "sessionId": self.session,
                "accountFingerprint": self.account,
                "credentialConfigurationHash": self.credential,
                "hazards": list(self.hazards[name]),
                "functionalMutationIntent": dict(self.graph_intent) if name == "graph" else {},
                "killOrdinaryCancelAllowed": self.kill_allowed if name == "graph" else False,
                "killOrdinaryCancelRevision": self.kill_revision if name == "graph" else 0,
                "killOrdinaryCancelIntent": dict(self.kill_intent) if name == "graph" else {},
                "productionAvailable": False,
            }
        return read

    def _new_state(self, *, lease=True, ledger=None, account=ACCOUNT, credential=CREDENTIAL, clock=None):
        return DurableKisDomesticFunctionalState(
            program_ledger=ledger or self.ledger,
            owner_id="state-owned-kis-composition-v1",
            component_owner_ids=OWNERS,
            component_readers=self.readers,
            account_fingerprint=account,
            credential_configuration_hash=credential,
            application_lease_held=lease,
            state_signer_key=STATE_KEY,
            state_signer_key_id="state-test-key-v1",
            clock=clock or (lambda: NOW),
        )

    @staticmethod
    def ok(_reservation):
        return {"ok": True, "mutationMayHaveOccurred": False, "receiptHash": RECEIPT}

    @staticmethod
    def failed(*, ambiguous):
        return lambda _reservation: {
            "ok": False, "mutationMayHaveOccurred": ambiguous,
            "receiptHash": RECEIPT,
        }

    def test_flags_false_and_exact_owner_component_hazard_union(self) -> None:
        for key in ("available", "backendAvailable", "networkAvailable", "managerWiringAvailable", "releaseEvidenceAvailable"):
            self.assertFalse(production_entrypoint_status()[key])
        self.hazards["backend"] = ["BACKEND_STALE"]
        self.hazards["transport"] = ["ORPHAN_PRE_POST_MARKER"]
        snapshot = self.state.authority_snapshot()
        self.assertEqual(["BACKEND_STALE", "ORPHAN_PRE_POST_MARKER"], snapshot["hazards"])
        self.assertTrue(snapshot["functionalAuthorityOpen"])
        self.assertTrue(snapshot["ordinaryRoutesClosed"])
        self.assertFalse(snapshot["productionAvailable"])

    def test_initial_authority_is_revision_one_signed_init_full_projection(self) -> None:
        snapshot = self.state.authority_snapshot()
        self.assertEqual(1, snapshot["functionalRevision"])
        self.assertEqual("IDLE", snapshot["functionalPhase"])
        self.assertEqual({}, snapshot["controlReservation"])
        with self.ledger.connection() as conn:
            authority = conn.execute(
                "SELECT * FROM kis_functional_state_authority WHERE route=?",
                ("KIS_KR_LIVE_CONTINUOUS",),
            ).fetchone()
            transitions = conn.execute(
                "SELECT * FROM kis_functional_state_transition ORDER BY revision"
            ).fetchall()
        self.assertEqual(1, len(transitions))
        body = json.loads(transitions[0]["body_json"])
        self.assertEqual(1, body["revision"])
        self.assertEqual("IDLE", body["phase"])
        self.assertEqual(ACCOUNT, body["accountFingerprint"])
        self.assertEqual(CREDENTIAL, body["credentialConfigurationHash"])
        self.assertEqual([], body["hazards"])
        self.assertEqual(authority["transition_head_hash"], transitions[0]["body_hash"])
        self.assertEqual(authority["updated_at"], body["occurredAt"])

    def test_initial_signed_transition_tamper_fails_closed(self) -> None:
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_state_transition SET signature_hash=? WHERE revision=1",
                ("0" * 64,),
            )
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "integrity"):
            self.state.authority_snapshot()

    def test_clock_rollback_rejected_before_transition_cas(self) -> None:
        path = Path(self.temp.name) / "rollback.sqlite3"
        ledger = ProgramLedger(path)
        clock_value = [NOW]
        state = self._new_state(ledger=ledger, clock=lambda: clock_value[0])
        clock_value[0] = NOW - timedelta(microseconds=1)
        calls = []
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "clock moved backwards"):
            state.start(
                session_id="kis-session-clock-rollback",
                manager=lambda reservation: calls.append(reservation) or self.ok({}),
            )
        self.assertEqual([], calls)
        with ledger.connection() as conn:
            authority = conn.execute(
                "SELECT revision,phase,reservation_id,transition_head_hash FROM kis_functional_state_authority"
            ).fetchone()
            transitions = conn.execute(
                "SELECT revision,body_hash FROM kis_functional_state_transition ORDER BY revision"
            ).fetchall()
        self.assertEqual((1, "IDLE", ""), tuple(authority)[:3])
        self.assertEqual([1], [row["revision"] for row in transitions])
        self.assertEqual(transitions[0]["body_hash"], authority["transition_head_hash"])

    def test_component_unreadable_or_wrong_owner_fails_closed(self) -> None:
        self.unreadable.add("transport")
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "unreadable"):
            self.state.authority_snapshot()
        self.unreadable.clear()
        original = self.readers["graph"]
        self.state._readers["graph"] = lambda: {**original(), "ownerHash": "0" * 64}
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "ownerHash"):
            self.state.authority_snapshot()

    def test_start_reserves_under_route_but_manager_runs_after_release(self) -> None:
        acquired = threading.Event()
        def manager(_reservation):
            thread = threading.Thread(target=lambda: self._acquire_route(acquired))
            thread.start(); self.assertTrue(acquired.wait(1)); thread.join(1)
            return self.ok({})
        result = self.state.start(session_id="kis-session-one", manager=manager)
        self.session = "kis-session-one"
        self.assertEqual("ACTIVE", result["phase"])
        snapshot = self.state.authority_snapshot()
        self.assertEqual("kis-session-one", snapshot["functionalSessionId"])
        self.assertTrue(snapshot["ordinaryRoutesClosed"])

    @staticmethod
    def _acquire_route(event):
        with kis_route_authority_serialization():
            event.set()

    def test_start_ambiguous_manager_outcome_is_reconciliation_only(self) -> None:
        result = self.state.start(
            session_id="kis-session-ambiguous", manager=self.failed(ambiguous=True)
        )
        self.session = "kis-session-ambiguous"
        self.assertEqual("RECONCILIATION_REQUIRED", result["phase"])
        self.assertIn("MANAGER_OUTCOME_AMBIGUOUS", self.state.authority_snapshot()["hazards"])

    def test_stop_releases_route_for_manager_then_reopens_ordinary(self) -> None:
        self.state.start(session_id="kis-session-stop", manager=self.ok); self.session = "kis-session-stop"
        acquired = threading.Event()
        def manager(_reservation):
            thread = threading.Thread(target=lambda: self._acquire_route(acquired))
            thread.start(); self.assertTrue(acquired.wait(1)); thread.join(1)
            return self.ok({})
        result = self.state.stop(manager=manager); self.session = ""
        self.assertEqual("IDLE", result["phase"])
        self.assertFalse(self.state.authority_snapshot()["ordinaryRoutesClosed"])

    def test_kill_preempts_paused_start_and_stale_start_cannot_finalize(self) -> None:
        entered = threading.Event(); release = threading.Event(); errors = []
        def paused(_reservation):
            entered.set(); release.wait(2); return self.ok({})
        starter = threading.Thread(target=lambda: self._capture(
            errors, lambda: self.state.start(session_id="kis-session-preempt", manager=paused)
        ))
        starter.start(); self.assertTrue(entered.wait(1))
        killed = self.state.kill(manager=self.ok); self.session = killed["sessionId"]
        self.assertEqual("CLEANUP", killed["phase"])
        release.set(); starter.join(2)
        self.assertTrue(any("superseded" in str(item) for item in errors))
        self.assertIn(
            "SUPERSEDED_START_REQUIRES_CLEANUP",
            self.state.authority_snapshot()["hazards"],
        )

    @staticmethod
    def _capture(errors, call):
        try: call()
        except Exception as exc: errors.append(exc)

    def test_settings_two_phase_success_and_known_failure_rollback(self) -> None:
        new_account = "c" * 64; new_credential = "d" * 64
        def apply_new_settings(_reservation):
            self.account = new_account
            self.credential = new_credential
            return self.ok({})
        changed = self.state.apply_settings(
            account_fingerprint=new_account,
            credential_configuration_hash=new_credential,
            manager=apply_new_settings,
        )
        self.assertEqual("IDLE", changed["phase"])
        self.assertEqual(new_account, self.state.authority_snapshot()["functionalAccountFingerprint"])

        reverted = self.state.apply_settings(
            account_fingerprint="e" * 64,
            credential_configuration_hash="f" * 64,
            manager=self.failed(ambiguous=False),
        )
        self.assertEqual("IDLE", reverted["phase"])
        self.assertEqual(new_account, self.state.authority_snapshot()["functionalAccountFingerprint"])

    def test_settings_blocked_while_active_without_manager_call(self) -> None:
        self.state.start(session_id="kis-session-active", manager=self.ok); self.session = "kis-session-active"
        calls = []
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "phase"):
            self.state.apply_settings(
                account_fingerprint="c" * 64, credential_configuration_hash="d" * 64,
                manager=lambda reservation: calls.append(reservation) or self.ok({}),
            )
        self.assertEqual([], calls)

    def test_ordinary_boundary_blocks_during_start_reservation_and_active(self) -> None:
        entered = threading.Event(); release = threading.Event()
        thread = threading.Thread(target=lambda: self.state.start(
            session_id="kis-session-block", manager=lambda _r: (
                entered.set(), release.wait(2), self.ok({})
            )[2]
        ))
        thread.start(); self.assertTrue(entered.wait(1))
        with self.assertRaisesRegex(
            KisOrderAuthorityError,
            "functional authority|active control reservation",
        ):
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=self._ordinary_intent()
            ):
                pass
        release.set(); thread.join(2); self.session = "kis-session-block"
        with self.assertRaisesRegex(
            KisOrderAuthorityError,
            "functional authority|active control reservation",
        ):
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=self._ordinary_intent()
            ):
                pass

    @staticmethod
    def _ordinary_intent():
        return {
            "operation": "PLACE_ORDER", "claimId": "ordinary-claim-one",
            "ownedOrderKey": {"orderDate": "", "organizationNo": "", "orderNo": ""},
            "accountFingerprint": ACCOUNT, "credentialConfigurationHash": CREDENTIAL,
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "payloadHash": "1" * 64,
        }

    def test_three_thread_start_kill_settings_has_no_lock_inversion(self) -> None:
        entered = threading.Event(); release = threading.Event(); outcomes = []
        start = threading.Thread(target=lambda: self._capture(outcomes, lambda: self.state.start(
            session_id="kis-session-race", manager=lambda _r: (
                entered.set(), release.wait(2), self.ok({})
            )[2]
        )))
        start.start(); self.assertTrue(entered.wait(1))
        kill = threading.Thread(target=lambda: self._capture(outcomes, lambda: self.state.kill(manager=self.ok)))
        settings = threading.Thread(target=lambda: self._capture(outcomes, lambda: self.state.apply_settings(
            account_fingerprint="c" * 64, credential_configuration_hash="d" * 64,
            manager=self.ok,
        )))
        kill.start(); settings.start(); kill.join(1); settings.join(1)
        self.assertFalse(kill.is_alive()); self.assertFalse(settings.is_alive())
        release.set(); start.join(2); self.assertFalse(start.is_alive())
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT phase,reservation_id FROM kis_functional_state_authority").fetchone()
        self.assertEqual("CLEANUP", row["phase"]); self.assertEqual("", row["reservation_id"])

    def test_application_lease_false_blocks_start_before_manager(self) -> None:
        path = Path(self.temp.name) / "no-lease.sqlite3"; ledger = ProgramLedger(path)
        state = self._new_state(lease=False, ledger=ledger)
        calls = []
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "application lease"):
            state.start(session_id="kis-session-no-lease", manager=lambda item: calls.append(item) or self.ok({}))
        self.assertEqual([], calls)

    def test_owner_epoch_signature_and_rotation_are_rechecked(self) -> None:
        original = dict(self.state._disabled_owner_epoch_snapshot)
        self.state._disabled_owner_epoch_snapshot = {
            **original, "signature": "0" * 64,
        }
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "signature is unverified"
        ):
            self.state.authority_snapshot()

        self.state.clock = lambda: NOW + timedelta(seconds=1)
        self.state._disabled_owner_epoch_snapshot = (
            self.state._disabled_owner_epoch(True)
        )
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "owner epoch changed"):
            self.state.authority_snapshot()

    def test_exact_signed_manager_receipt_is_bound_to_fresh_components(self) -> None:
        def signed(reservation):
            return self.state.sign_disabled_manager_result_for_tests(
                reservation=reservation, ok=True,
                mutation_may_have_occurred=False,
                components_hash=reservation["componentReadersHash"],
            )

        result = self.state.start(
            session_id="kis-session-signed-manager", manager=signed
        )
        self.assertEqual("ACTIVE", result["phase"])
        self.assertRegex(result["managerReceiptHash"], r"^[0-9a-f]{64}$")

    def test_signed_manager_component_projection_substitution_rejected(self) -> None:
        def substituted(reservation):
            return self.state.sign_disabled_manager_result_for_tests(
                reservation=reservation, ok=True,
                mutation_may_have_occurred=False,
                components_hash="0" * 64,
            )

        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "binding mismatch"
        ):
            self.state.start(
                session_id="kis-session-manager-substitution",
                manager=substituted,
            )

    def test_final_mutation_boundary_holds_route_and_releases_it(self) -> None:
        reservation = self.state._reserve(
            kind="START", allowed_phases={"IDLE"},
            pending_session="kis-session-final-boundary",
        )
        snapshot = self.state.authority_snapshot()
        self.assertEqual(
            {
                "reservationId": reservation["reservationId"],
                "reservationKind": "START",
                "reservationRevision": reservation["revision"],
                "stateRevision": snapshot["stateRevision"],
                "phase": "ARMED_WAIT_PUBLIC",
                "reservationBindingHash": snapshot[
                    "reservationBindingHash"
                ],
            },
            snapshot["controlReservation"],
        )
        acquired = threading.Event()
        thread = threading.Thread(
            target=lambda: self._acquire_route(acquired), daemon=True
        )
        with self.state.final_mutation_boundary(reservation=reservation) as lease:
            self.assertTrue(lease["routeLockHeld"])
            self.assertEqual(
                reservation["finalMutationBoundaryHandle"],
                lease["finalMutationBoundaryHandle"],
            )
            thread.start()
            self.assertFalse(acquired.wait(0.1))
        self.assertTrue(acquired.wait(1))
        thread.join(1)

    def test_final_mutation_boundary_rejects_tamper_and_extra_fields(self) -> None:
        reservation = self.state._reserve(
            kind="START", allowed_phases={"IDLE"},
            pending_session="kis-session-final-tamper",
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "binding mismatch"
        ):
            with self.state.final_mutation_boundary(
                reservation={
                    **reservation,
                    "finalMutationBoundaryHandle": "0" * 64,
                }
            ):
                pass
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "not exact"
        ):
            with self.state.final_mutation_boundary(
                reservation={**reservation, "unexpected": False}
            ):
                pass

    def test_final_mutation_boundary_rejects_superseded_reservation(self) -> None:
        reservation = self.state._reserve(
            kind="START", allowed_phases={"IDLE"},
            pending_session="kis-session-final-stale",
        )
        killed = self.state.kill(manager=self.ok)
        self.session = killed["sessionId"]
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "superseded"
        ):
            with self.state.final_mutation_boundary(reservation=reservation):
                pass

    def test_manager_receipt_body_hash_is_independently_recomputed(self) -> None:
        def tampered(reservation):
            receipt = self.state.sign_disabled_manager_result_for_tests(
                reservation=reservation, ok=True,
                mutation_may_have_occurred=False,
                components_hash=reservation["componentReadersHash"],
            )
            receipt["ok"] = False
            return receipt

        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "body hash|signature"
        ):
            self.state.start(
                session_id="kis-session-manager-hash-tamper", manager=tampered
            )

    def test_component_hazard_is_persisted_and_blocks_start_settings(self) -> None:
        self.hazards["transport"] = ["ORPHAN_PRE_POST_MARKER"]
        calls = []
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "hazard union"):
            self.state.start(
                session_id="kis-session-hazard",
                manager=lambda item: calls.append(item) or self.ok({}),
            )
        self.assertEqual([], calls)
        with self.ledger.connection() as conn:
            row = conn.execute(
                "SELECT durable_hazards_json,ordinary_routes_closed FROM kis_functional_state_authority"
            ).fetchone()
        self.assertEqual(["ORPHAN_PRE_POST_MARKER"], json.loads(row[0]))
        self.assertEqual(1, row[1])
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "hazard union"):
            self.state.apply_settings(
                account_fingerprint="c" * 64,
                credential_configuration_hash="d" * 64,
                manager=lambda item: calls.append(item) or self.ok({}),
            )
        self.assertEqual([], calls)

    def test_stop_success_cannot_reopen_routes_when_component_hazard_appears(self) -> None:
        self.state.start(session_id="kis-session-stop-hazard", manager=self.ok)
        self.session = "kis-session-stop-hazard"
        def manager(_reservation):
            self.hazards["backend"] = ["BACKEND_STOP_UNPROVEN"]
            return self.ok({})
        result = self.state.stop(manager=manager)
        self.assertEqual("RECONCILIATION_REQUIRED", result["phase"])
        snapshot = self.state.authority_snapshot()
        self.assertTrue(snapshot["ordinaryRoutesClosed"])
        self.assertIn("BACKEND_STOP_UNPROVEN", snapshot["hazards"])

    def test_signed_transition_chain_and_full_row_projection_tamper_reject(self) -> None:
        self.state.start(session_id="kis-session-signed", manager=self.ok)
        self.session = "kis-session-signed"
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_state_authority SET pending_session_id='tampered'"
            )
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "projection"):
            self.state.authority_snapshot()
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_state_authority SET pending_session_id=''"
            )
            conn.execute(
                "UPDATE kis_functional_state_transition SET signature_hash=? WHERE revision=1",
                ("0" * 64,),
            )
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "integrity"):
            self.state.authority_snapshot()

    def test_dirty_schema_and_revision_history_fail_closed(self) -> None:
        with self.ledger.connection() as conn:
            conn.execute("CREATE TABLE kis_functional_state_extra(value TEXT)")
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "schema fingerprint"):
            self._new_state()
        with self.ledger.connection() as conn:
            conn.execute("DROP TABLE kis_functional_state_extra")
            conn.execute("UPDATE kis_functional_state_authority SET revision=99")
        with self.assertRaisesRegex(KisDomesticFunctionalStateBlocked, "component|state"):
            # A reserve cannot silently overwrite an impossible phase/revision lineage.
            self.state.start(session_id="kis-session-dirty", manager=self.ok)


if __name__ == "__main__":
    unittest.main()
