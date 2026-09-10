from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from contextlib import closing
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.continuous_dispatch import (
    ContinuousDispatcher, IntentJournal, begin_control, end_control,
    continuous_dispatch_allowed,
)
from live_trader.continuous_live import LiveContinuousController, LiveContinuousRuntimeManager
from live_trader.server import LiveTraderHandler
from trading_runtime.continuous_runtime import ClosedBar, StrategyBarDecision
from live_trader.portfolio_execution import build_symbol_net_plan, SleeveTarget, LivePortfolioLedger
from tests import test_order_dispatch_safety as safety_fixture


class ContinuousDispatchJournalTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "intents.sqlite3"
        self.lock = state._OwnedRLock()
        self.failures = []

    def intent(self, key="one"):
        original = safety_fixture.OrderDispatchSafetyTest.intent()
        return replace(original, metadata={**original.metadata, "runtime_evaluation_key": key})

    def dispatcher(self, submit):
        return ContinuousDispatcher(self.path, self.lock, submit, self.failures.append)

    def test_nested_cycle_checkpoint_finishes_before_unlocked_immutable_dispatch(self):
        sent = []
        checkpoint = []
        original = self.intent()
        def submit(intent):
            self.assertFalse(self.lock.owned_by_current_thread())
            self.assertEqual(["saved"], checkpoint)
            self.assertEqual("kis", intent.metadata["broker_id"])
            self.assertTrue(continuous_dispatch_allowed(intent)[0])
            sent.append(intent)
            return {"ok": True}
        dispatcher = self.dispatcher(submit)
        with dispatcher:
            with dispatcher:
                queued = dispatcher.defer(original)
                original.metadata["broker_id"] = "tampered-after-enqueue"
            self.assertFalse(sent)
            self.assertTrue(self.path.exists())
            checkpoint.append("saved")
        self.assertTrue(queued["deferred"])
        self.assertEqual(1, len(sent))
        self.assertEqual("COMPLETED", dispatcher.journal.rows()[0]["state"])

    def test_failed_engine_checkpoint_aborts_without_submission_or_replay(self):
        submit = Mock()
        dispatcher = self.dispatcher(submit)
        with self.assertRaisesRegex(OSError, "checkpoint"):
            with dispatcher:
                dispatcher.defer(self.intent())
                raise OSError("checkpoint")
        with dispatcher:
            duplicate = dispatcher.defer(self.intent())
        self.assertTrue(duplicate["duplicate"])
        submit.assert_not_called()
        self.assertEqual("ABORTED", dispatcher.journal.rows()[0]["state"])

    def test_unrecognized_outer_runtime_lock_never_dispatches(self):
        submit = Mock()
        dispatcher = self.dispatcher(submit)
        with self.lock:
            with dispatcher:
                dispatcher.defer(self.intent())
        submit.assert_not_called()
        self.assertEqual("ABORTED", dispatcher.journal.rows()[0]["state"])

    def test_concurrent_claim_consumes_exactly_one_attempt(self):
        entered, release = threading.Event(), threading.Event()
        def submit(intent):
            entered.set()
            self.assertTrue(release.wait(2))
            return {"ok": True}
        submit = Mock(side_effect=submit)
        dispatcher = self.dispatcher(submit)
        row = dispatcher.journal.enqueue(self.intent())
        first = threading.Thread(target=lambda: dispatcher.dispatch(row["command_id"]))
        first.start()
        self.assertTrue(entered.wait(2))
        rejected = []
        second = threading.Thread(target=lambda: rejected.append(dispatcher.dispatch(row["command_id"])))
        second.start()
        release.set()
        first.join(2)
        second.join(2)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertFalse(rejected[0]["ok"])
        self.assertEqual(1, submit.call_count)

    def test_restart_never_replays_queued_or_possibly_sent_commands(self):
        script = """
import os, sys
from pathlib import Path
from live_trader.continuous_dispatch import IntentJournal
from live_trader.order_management import OrderIntent
j=IntentJournal(Path(sys.argv[1]))
i=OrderIntent('s','CRYPTO','BTCUSDT','BUY',1,1,'SMALL_LIVE','fixture',{'broker_id':'binance','runtime_evaluation_key':'crash'})
r=j.enqueue(i)
if sys.argv[2]=='claimed': j.claim(r['command_id'])
os._exit(0)
"""
        for claimed in (False, True):
            with self.subTest(claimed=claimed):
                path = self.path.with_name(f"restart-{claimed}.sqlite3")
                result = subprocess.run(
                    [sys.executable, "-B", "-c", script, str(path), "claimed" if claimed else "queued"],
                    capture_output=True, text=True, timeout=10,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                self.assertEqual(0, result.returncode, result.stderr)
                fresh = IntentJournal(path)
                rows = fresh.rows()
                self.assertEqual("UNKNOWN" if claimed else "INTERRUPTED", rows[0]["state"])
                self.assertIsNone(fresh.claim(rows[0]["command_id"]))

    def test_other_owner_never_steals_active_attempt(self):
        first = IntentJournal(self.path)
        row = first.enqueue(self.intent())
        self.assertIsNotNone(first.claim(row["command_id"]))
        second = IntentJournal(self.path)
        self.assertIsNone(second.claim(row["command_id"]))
        first.finish(row["command_id"], "COMPLETED", {"ok": True})
        self.assertEqual("COMPLETED", second.rows()[0]["state"])

    def test_control_invalidates_queued_and_during_control_intents(self):
        dispatcher = self.dispatcher(Mock())
        prior = dispatcher.journal.enqueue(self.intent("prior"))
        with state.SAFETY_CONFIRMATION_MUTATION_LOCK:
            token = begin_control()
        during = dispatcher.journal.enqueue(self.intent("during"))
        with state.SAFETY_CONFIRMATION_MUTATION_LOCK:
            end_control(token)
        self.assertIsNone(dispatcher.journal.claim(prior["command_id"]))
        self.assertEqual("BLOCKED", during["state"])
        dispatcher.submit.assert_not_called()

    def test_ambiguous_ack_and_terminal_write_failure_never_retry(self):
        for failure in ("ack", "journal"):
            with self.subTest(failure=failure):
                self.path = self.path.with_name(f"{failure}.sqlite3")
                submit = Mock(side_effect=TimeoutError("lost ACK")) if failure == "ack" else Mock(return_value={"ok": True})
                dispatcher = self.dispatcher(submit)
                row = dispatcher.journal.enqueue(self.intent(failure))
                if failure == "journal":
                    with patch.object(dispatcher.journal, "finish", side_effect=OSError("disk full")):
                        result = dispatcher.dispatch(row["command_id"])
                else:
                    result = dispatcher.dispatch(row["command_id"])
                self.assertFalse(result["ok"])
                self.assertEqual(1, submit.call_count)
                self.assertEqual("UNKNOWN", IntentJournal(self.path).rows()[0]["state"])
                self.assertIsNone(IntentJournal(self.path).claim(row["command_id"]))
                self.assertTrue(self.failures)

    def test_corrupt_or_unsupported_journal_is_preserved(self):
        self.path.write_bytes(b"not a sqlite database")
        with self.assertRaises(sqlite3.DatabaseError):
            IntentJournal(self.path).enqueue(self.intent())
        self.assertEqual(b"not a sqlite database", self.path.read_bytes())
        other = self.path.with_name("future.sqlite3")
        with closing(sqlite3.connect(other)) as connection, connection:
            connection.execute("PRAGMA user_version=99")
        before = other.read_bytes()
        with self.assertRaisesRegex(ValueError, "version"):
            IntentJournal(other).enqueue(self.intent())
        self.assertEqual(before, other.read_bytes())

    def test_restart_uncertainty_blocks_new_evaluations_until_exact_evidence(self):
        old = IntentJournal(self.path)
        row = old.enqueue(self.intent("old"))
        old.claim(row["command_id"])
        fresh = IntentJournal(self.path)
        self.assertTrue(fresh.reconciliation_required())
        blocked = fresh.enqueue(self.intent("new"))
        self.assertEqual("BLOCKED", blocked["state"])
        self.assertEqual(0, fresh.reconcile([]))
        seal = {"commandId": row["command_id"], "owner": row["owner"], "epoch": row["epoch"], "payloadHash": row["payload_hash"]}
        order = {"continuous_dispatch": seal, "state": "acknowledged", "symbol": "005930", "broker_id": "kis", "broker_order_id": "ACK"}
        self.assertEqual(0, fresh.reconcile([{**order, "continuous_dispatch": {**seal, "payloadHash": "wrong"}}]))
        self.assertEqual(0, fresh.reconcile([{**order, "state": "unknown"}]))
        self.assertEqual(1, fresh.reconcile([order]))
        self.assertFalse(fresh.reconciliation_required())
        allowed = fresh.enqueue(self.intent("after-reconcile"))
        self.assertEqual("QUEUED", allowed["state"])
        self.assertIsNone(fresh.claim(row["command_id"]))

    def test_reconciliation_rehashes_durable_payload_and_requires_broker_ack_id(self):
        old = IntentJournal(self.path)
        row = old.enqueue(self.intent("corrupt-reconcile"))
        old.claim(row["command_id"])
        fresh = IntentJournal(self.path)
        seal = {"commandId": row["command_id"], "owner": row["owner"], "epoch": row["epoch"], "payloadHash": row["payload_hash"]}
        order = {"continuous_dispatch": seal, "state": "acknowledged", "symbol": "005930", "broker_id": "kis"}
        self.assertEqual(0, fresh.reconcile([order]))
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute("UPDATE continuous_intents SET payload=?", ('{}',))
        self.assertEqual(0, fresh.reconcile([{**order, "broker_order_id": "ACK"}]))
        self.assertTrue(fresh.reconciliation_required())

    def test_added_route_and_authority_metadata_is_rejected(self):
        for key in ("functional_test_session_id", "environment", "exchange", "request_url"):
            with self.subTest(key=key):
                def submit(intent):
                    intent.metadata[key] = "injected"
                    allowed, reason = continuous_dispatch_allowed(intent)
                    self.assertFalse(allowed)
                    self.assertEqual("continuous-dispatch-metadata-key-added", reason)
                    return {"ok": False, "reason": reason}
                dispatcher = self.dispatcher(submit)
                with dispatcher:
                    dispatcher.defer(self.intent(key))
                self.assertFalse(dispatcher.last_results[0]["ok"])

    def test_tampered_payload_and_forged_capability_fail_closed(self):
        dispatcher = self.dispatcher(Mock())
        row = dispatcher.journal.enqueue(self.intent())
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute("UPDATE continuous_intents SET payload=?", ('{}',))
        self.assertFalse(dispatcher.dispatch(row["command_id"])["ok"])
        dispatcher.submit.assert_not_called()
        forged = replace(self.intent(), metadata={"continuous_dispatch": {"epoch": "fake"}})
        self.assertEqual((False, "continuous-dispatch-capability-missing"), continuous_dispatch_allowed(forged))


class ContinuousDispatchStateIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.fixture = safety_fixture.OrderDispatchSafetyTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root = Path(self.fixture.temporary.name)
        state.STATE["kill_switch"] = False
        state.STATE["active_runtime_session_ids"] = {}
        self.failures = []
        self.dispatcher = ContinuousDispatcher(
            self.root / "deferred.sqlite3", state.RUNTIME_MODE_LOCK,
            self.fixture.submit_with_passing_gate, self.failures.append,
        )
        self.router = Mock()
        self.router.place_order.return_value = {
            "ok": True, "statusCode": 200,
            "kisOrderAccountBinding": self.fixture.kis_wire_account_binding(),
            "json": {"output": {"ODNO": "DEFERRED-ACK", "ORD_DT": "20260911", "KRX_FWDG_ORD_ORGNO": "001"}},
        }

    def intent(self, key="one"):
        value = self.fixture.intent(target_revision=2101)
        return replace(value, metadata={**value.metadata, "runtime_evaluation_key": key})

    def run_queued(self, intent=None):
        with patch.object(state, "LiveBrokerRouter", return_value=self.router):
            with self.dispatcher:
                result = self.dispatcher.defer(intent or self.intent())
        return result

    def test_restarted_controller_blocks_live_start_before_feed_or_broker(self):
        controller = LiveContinuousController(self.root)
        controller.profile_id = "crypto"
        journal = controller._dispatcher().journal
        row = journal.enqueue(self.intent("unresolved"))
        journal.claim(row["command_id"])
        journal.finish(row["command_id"], "UNKNOWN")
        fresh = LiveContinuousController(self.root)
        fresh.profile_id = "crypto"
        with patch.object(state, "snapshot", return_value={}), patch(
            "live_trader.continuous_live.feeds_for_specs"
        ) as feeds, patch.object(state, "LiveBrokerRouter") as router:
            result = fresh.start("crypto", "SMALL_LIVE")
        self.assertFalse(result["ok"])
        self.assertTrue(result["dispatch"]["reconciliationRequired"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        feeds.assert_not_called()
        router.assert_not_called()

    def test_restore_seal_matches_engine_canonical_numbers_and_rejects_old_mismatch(self):
        from hashlib import sha256
        from trading_runtime.artifact_governance import stable_sha256
        body = {"quantity": 0.0, "nested": {"position": 1.0}}
        current = {"body": body, "bodyHash": state._restore_context_hash(body)}
        self.assertEqual(stable_sha256(body), current["bodyHash"])
        self.assertEqual(state._restore_context_hash({"quantity": 0, "nested": {"position": 1}}), current["bodyHash"])
        legacy_hash = sha256(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertFalse(state._restore_context_seal_matches({"body": body, "bodyHash": legacy_hash}, current, current_positions_are_flat=True, position_context_complete=True))
        changed = {"body": {"quantity": 0.01, "nested": {"position": 1.0}}}
        changed["bodyHash"] = state._restore_context_hash(changed["body"])
        self.assertFalse(state._restore_context_seal_matches(changed, current, current_positions_are_flat=True, position_context_complete=True))
        self.assertFalse(state._restore_context_seal_matches(current, {}, current_positions_are_flat=True, position_context_complete=True))
        with self.assertRaises(ValueError):
            state._restore_context_hash({"quantity": float("nan")})

    def test_start_endpoint_closed_bar_checkpoint_and_fake_broker_form_one_path(self):
        # Fake only external authority/evidence, strategy signal and market feed.
        # HTTP handler, start controls, supervisor, engine, queue, OMS and final
        # dispatch fences are the same implementations used by the application.
        manager = LiveContinuousRuntimeManager(self.root)
        controller = manager.controllers["stock"]
        poll_release, posted = threading.Event(), threading.Event()
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        strategy = {
            "strategy_id": "dispatch-safety", "instance_id": "dispatch-safety",
            "symbol": "005930", "provider": "kis", "broker_id": "kis",
            "timeframe": "1m", "plugin": "ma-cross",
            "permissions": {"live_small_eligible": True},
            "parameters": {"paperOrderQuantity": 1},
        }
        spec = controller._standalone_spec(strategy)
        bar = ClosedBar(spec.instrument_id, spec.symbol, spec.provider, spec.timeframe,
                        (now - timedelta(minutes=1)).isoformat(), now.isoformat(),
                        70000, 70000, 70000, 70000, 1, received_time=now.isoformat())
        class Feed:
            provider_id = "kis"
            delivered = False
            def connect(self): pass
            def disconnect(self): pass
            def warmup(self, subscription, count): return ()
            def poll(self, timeout):
                if not self.delivered and poll_release.wait(.1):
                    self.delivered = True
                    return (bar, bar)
                return ()
        def evaluate(spec, closed, history):
            return StrategyBarDecision(spec.strategy_instance_id, spec.strategy_id, "BUY", "fixture signal",
                closed, controller.supervisor.engine._evaluation_key(spec, closed), {"positionQuantity": 0})
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/runtime/start"
        handler.read_json = Mock(return_value={"profile_id": "stock", "mode": "SMALL_LIVE", "strategy_id": "dispatch-safety"})
        handler.send_json = Mock()
        self.router.list_positions.return_value = []
        ack = self.router.place_order.return_value
        def broker(payload):
            self.assertFalse(state.RUNTIME_MODE_LOCK.owned_by_current_thread())
            checkpoint = controller.supervisor.engine.state_store.read()
            self.assertEqual(1, len(checkpoint["recentCycles"]))
            self.assertEqual("bar-cycle", checkpoint["checkpointReason"])
            self.assertEqual("DISPATCHING", controller._dispatcher().journal.rows()[0]["state"])
            posted.set()
            return ack
        self.router.place_order.side_effect = broker
        with (
            patch("socket.socket.connect", side_effect=AssertionError("external socket forbidden")),
            patch.object(state, "LIVE_CONTINUOUS_CONTROLLER", manager),
            patch.object(state, "LiveBrokerRouter", return_value=self.router),
            patch.object(state, "_prepare_operational_runtime_session", return_value=(None, "fixture approval")) as prepare,
            patch.object(state, "_finish_operational_runtime_start"),
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit"),
            patch.object(state, "strategy_rows", return_value=[strategy]),
            patch.object(state, "portfolio_rows", return_value=[]),
            patch.object(state, "paper_live_qualification_gate_for_strategy", return_value={"required": True, "ready": True, "strategyInstanceId": "dispatch-safety"}),
            patch.object(controller, "_select_standalone_strategy", return_value=strategy),
            patch.object(controller, "_submit_deferred_intent", side_effect=self.fixture.submit_with_passing_gate),
            patch("live_trader.continuous_live.feeds_for_specs", return_value=(Feed(),)),
            patch("live_trader.continuous_live.BuiltinBarSignalEvaluator", return_value=evaluate),
        ):
            try:
                handler.do_POST()
                started = handler.send_json.call_args.args[0]
                self.assertTrue(started["ok"], started)
                prepare.assert_called_once()
                self.assertEqual("SMALL_LIVE", state.STATE["mode"])
                self.router.place_order.assert_not_called()
                poll_release.set()
                self.assertTrue(posted.wait(5), controller.snapshot())
            finally:
                poll_release.set()
                if controller.supervisor is not None:
                    controller.stop()
            self.assertEqual(1, self.router.place_order.call_count)
            self.assertEqual("COMPLETED", controller._dispatcher().journal.rows()[0]["state"])
            self.assertEqual("acknowledged", self.fixture.ledger.order_dispatch_rows()[0]["state"])
            self.assertEqual(1, len(controller.supervisor.engine.cycles))

    def test_real_submit_and_sqlite_checkpoint_reach_fake_broker_after_unlock(self):
        original = self.router.place_order.return_value
        def broker(payload):
            self.assertFalse(state.RUNTIME_MODE_LOCK.owned_by_current_thread())
            durable = self.fixture.ledger.order_dispatch_for_idempotency_key(payload["identifier"])
            self.assertEqual("dispatch_pending", durable["state"])
            self.assertEqual("DISPATCHING", self.dispatcher.journal.rows()[0]["state"])
            return original
        self.router.place_order.side_effect = broker
        self.run_queued()
        self.assertEqual(1, self.router.place_order.call_count)
        self.assertTrue(self.dispatcher.last_results[0]["ok"])
        self.assertEqual("COMPLETED", self.dispatcher.journal.rows()[0]["state"])

    def test_stop_after_checkpoint_invalidates_final_post_even_if_mode_unchanged(self):
        checkpoint = self.fixture.ledger.checkpoint_order_dispatch
        manager = Mock()
        manager.stop.return_value = {"ok": True, "reason": "fixture stopped"}
        def save_and_stop(order):
            saved = checkpoint(order)
            with patch.object(state, "LIVE_CONTINUOUS_CONTROLLER", manager):
                stopped = state.stop_continuous_runtime("crypto")
            self.assertTrue(stopped["ok"])
            return saved
        with patch.object(self.fixture.ledger, "checkpoint_order_dispatch", side_effect=save_and_stop):
            self.run_queued()
        self.router.place_order.assert_not_called()
        self.assertIn("generation-changed", self.dispatcher.last_results[0]["reason"])

    def test_mode_change_after_checkpoint_cannot_reuse_old_generation(self):
        checkpoint = self.fixture.ledger.checkpoint_order_dispatch
        def save_and_change(order):
            saved = checkpoint(order)
            with patch.object(state, "_set_mode_serialized", return_value={"ok": True}):
                state.set_mode("MONITOR")
                state.set_mode("SMALL_LIVE")
            return saved
        with patch.object(self.fixture.ledger, "checkpoint_order_dispatch", side_effect=save_and_change):
            self.run_queued()
        self.router.place_order.assert_not_called()
        self.assertIn("generation-changed", self.dispatcher.last_results[0]["reason"])

    def test_durable_kill_after_checkpoint_blocks_final_post(self):
        checkpoint = self.fixture.ledger.checkpoint_order_dispatch
        def save_and_kill(order):
            saved = checkpoint(order)
            safety_fixture.engage_emergency_stop(source="deferred-fixture", reason="test only")
            return saved
        with patch.object(self.fixture.ledger, "checkpoint_order_dispatch", side_effect=save_and_kill):
            self.run_queued()
        self.router.place_order.assert_not_called()
        self.assertFalse(self.dispatcher.last_results[0]["ok"])

    def test_ack_loss_is_unknown_in_both_journals_and_duplicate_is_blocked(self):
        self.router.place_order.side_effect = TimeoutError("fixture ACK lost")
        self.run_queued()
        self.assertEqual(1, self.router.place_order.call_count)
        self.assertEqual("UNKNOWN", self.dispatcher.journal.rows()[0]["state"])
        self.assertEqual("unknown", self.fixture.ledger.order_dispatch_rows()[0]["state"])
        self.run_queued()
        self.assertEqual(1, self.router.place_order.call_count)

    def test_mode_control_waiting_for_cycle_does_not_deadlock_dispatch(self):
        prepared, release_cycle, changing = threading.Event(), threading.Event(), threading.Event()
        def worker():
            with self.dispatcher:
                self.dispatcher.defer(self.intent())
                prepared.set()
                release_cycle.wait(2)
        def transition(mode):
            changing.set()
            with state.RUNTIME_MODE_LOCK:
                return {"ok": True}
        thread = threading.Thread(target=worker)
        thread.start()
        self.assertTrue(prepared.wait(2))
        with patch.object(state, "_set_mode_serialized", side_effect=transition):
            control = threading.Thread(target=lambda: state.set_mode("MONITOR"))
            control.start()
            self.assertTrue(changing.wait(2))
            release_cycle.set()
            thread.join(2)
            control.join(2)
        self.assertFalse(thread.is_alive())
        self.assertFalse(control.is_alive())
        self.assertEqual([], self.fixture.ledger.order_dispatch_rows())
        self.assertEqual("BLOCKED", self.dispatcher.journal.rows()[0]["state"])

    def test_inflight_post_finishes_before_stop_and_next_queued_attempt_is_blocked(self):
        entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
        second = self.dispatcher.journal.enqueue(self.intent("second"))
        ack = self.router.place_order.return_value
        def broker(payload):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("fixture release missing")
            return ack
        self.router.place_order.side_effect = broker
        @state._continuous_stop_control
        def fake_stop():
            with state.RUNTIME_MODE_LOCK:
                stopped.set()
        with patch.object(state, "LiveBrokerRouter", return_value=self.router):
            worker = threading.Thread(target=self.run_queued)
            worker.start()
            self.assertTrue(entered.wait(2))
            control = threading.Thread(target=fake_stop)
            control.start()
            self.assertFalse(stopped.wait(.05))
            release.set()
            worker.join(2)
            control.join(2)
            self.assertFalse(worker.is_alive())
            self.assertFalse(control.is_alive())
            self.assertTrue(stopped.is_set())
            self.assertFalse(self.dispatcher.dispatch(second["command_id"])["ok"])
        self.assertEqual(1, self.router.place_order.call_count)

    def test_exact_portfolio_no_order_outcomes_resolve_without_creating_ack_ledger(self):
        controller = LiveContinuousController(self.root)
        controller.profile_id = "stock"
        journal = controller._dispatcher().journal
        plan = build_symbol_net_plan(
            scope_id="live:old:old-hash", portfolio_id="old", portfolio_hash="old-hash",
            targets=(SleeveTarget("i", "s", "005930", 1),), current_positions={},
            broker_quantity=0, reference_price=70000,
        )
        evidence = []
        for status in ("risk_blocked", "adapter_blocked", "rejected", "acknowledged"):
            intent = self.intent(status)
            row = journal.enqueue(replace(intent, metadata={**intent.metadata, "portfolio_execution": plan.metadata()}))
            evidence.append({"state": status, "symbol": "005930", "broker_id": "kis", "portfolio_execution": plan.metadata(),
                "continuous_dispatch": {"commandId": row["command_id"], "owner": row["owner"], "epoch": row["epoch"], "payloadHash": row["payload_hash"]}})
        for row in evidence:
            journal.finish(row["continuous_dispatch"]["commandId"], "UNKNOWN")
        self.assertEqual(3, controller.reconcile_deferred_dispatches(evidence)["resolved"])
        self.assertTrue(journal.reconciliation_required())
        self.assertIsNone(controller.portfolio_ledger)
        self.assertEqual(1, sum(row["state"] == "UNKNOWN" for row in journal.rows()))

    def test_reconcile_recovers_only_exact_current_portfolio_and_hash(self):
        controller = LiveContinuousController(self.root)
        controller.profile_id = "stock"
        controller.portfolio_id = "current"
        controller.portfolio_execution_scope_id = "live:current:hash-current"
        controller.portfolio_ledger = LivePortfolioLedger(self.root / "sleeves.sqlite3")
        controller.portfolio_ledger.register_scope(
            scope_id=controller.portfolio_execution_scope_id, portfolio_id="current",
            portfolio_hash="hash-current", account_id="kis-account:fixture",
        )
        journal = controller._dispatcher().journal
        evidence = []
        for name, digest in (("current", "hash-current"), ("old", "hash-old"), ("current", "hash-previous")):
            plan = build_symbol_net_plan(
                scope_id=f"live:{name}:{digest}", portfolio_id=name, portfolio_hash=digest,
                targets=(SleeveTarget("i", "s", "005930", 1),), current_positions={},
                broker_quantity=0, reference_price=70000,
            )
            intent = self.intent(digest)
            intent = replace(intent, metadata={**intent.metadata, "portfolio_execution": plan.metadata()})
            row = journal.enqueue(intent)
            # Set each independently to UNKNOWN only after all unique rows exist.
            evidence.append({
                "order_id": "local-" + digest, "broker_order_id": "ACK-" + digest,
                "state": "acknowledged", "symbol": "005930", "broker_id": "kis",
                "portfolio_execution": plan.metadata(),
                "continuous_dispatch": {"commandId": row["command_id"], "owner": row["owner"], "epoch": row["epoch"], "payloadHash": row["payload_hash"]},
            })
        for order in evidence:
            journal.finish(order["continuous_dispatch"]["commandId"], "UNKNOWN")
        result = controller.reconcile_deferred_dispatches(evidence)
        self.assertEqual(1, result["resolved"])
        self.assertTrue(result["reconciliationRequired"])
        self.assertEqual(("ACK-hash-current",), controller.portfolio_ledger.pending_orders("live:current:hash-current"))
        self.assertEqual((), controller.portfolio_ledger.pending_orders("live:old:hash-old"))
        self.assertEqual((), controller.portfolio_ledger.pending_orders("live:current:hash-previous"))
        self.assertEqual(["RESOLVED", "UNKNOWN", "UNKNOWN"], sorted(row["state"] for row in journal.rows()))

    def test_portfolio_ack_checkpoint_failure_blocks_entries_without_resending(self):
        controller = LiveContinuousController(self.root)
        controller.profile_id = "stock"
        plan = build_symbol_net_plan(
            scope_id="fixture-scope", portfolio_id="fixture-portfolio", portfolio_hash="fixture-hash",
            targets=(SleeveTarget("i", "s", "005930", 1),), current_positions={},
            broker_quantity=0, reference_price=70000,
        )
        ledger = Mock()
        ledger.record_accepted_order.side_effect = OSError("fixture ACK journal failed")
        intent = self.intent("portfolio")
        intent = replace(intent, metadata={**intent.metadata, "portfolio_execution": plan.metadata()})
        with patch.object(state, "append_audit"), patch.object(
            controller, "_submit_deferred_intent",
            return_value={"ok": True, "order": {"broker_order_id": "ACK", "order_id": "local"}},
        ) as submit:
            with controller._dispatcher():
                controller._submit_cycle_intent(intent, audit_event="fixture", portfolio_ledger=ledger)
        self.assertEqual(1, submit.call_count)
        ledger.record_accepted_order.assert_called_once()
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertEqual("UNKNOWN", controller._dispatcher().journal.rows()[0]["state"])


if __name__ == "__main__":
    unittest.main()
