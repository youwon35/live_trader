from __future__ import annotations

import ast
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]


def isolated_function(path, name, namespace, owner=None):
    """Execute real function bodies without importing startup/env/DB side effects."""
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    nodes = tree.body
    if owner:
        nodes = next(node for node in nodes if isinstance(node, ast.ClassDef) and node.name == owner).body
    node = next(node for node in nodes if isinstance(node, ast.FunctionDef) and node.name == name)
    node.decorator_list = []
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(ROOT / path), "exec"), namespace)
    return namespace[name]


class LedgerDisplayEvidenceTests(unittest.TestCase):
    def test_summary_preserves_legacy_records_and_counts_only_identified_fills(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE cash_balances (updated_at TEXT);
            CREATE TABLE positions (updated_at TEXT);
            CREATE TABLE execution_events (occurred_at TEXT, state TEXT, raw_json TEXT, quantity REAL, side TEXT, broker_order_id TEXT);
        """)
        rows = [
            ("2026-07-26", "UNKNOWN", '{"raw":{"state":"account_snapshot"}}', 0, "", ""),
            ("2026-07-27", "UNKNOWN", '{"raw":{"state":"position_snapshot"}}', 2, "BUY", ""),
            ("2026-10-20", "FILLED", '{}', 1, "BUY", "order-one"),
            ("2026-10-21", "partially_filled", '{}', 1, "SELL", "order-two"),
            ("2026-10-22", "FILLED", '{}', 1, "BUY", ""),
            ("2026-10-23", "UNKNOWN", 'broken-json', 0, "", ""),
            ("2026-10-24", "ACCEPTED", '{}', 0, "BUY", "order-three"),
            ("2026-10-25", "FILLED", '{"raw":{"state":"position_snapshot"}}', 2, "BUY", "snapshot"),
        ]
        conn.executemany("INSERT INTO execution_events VALUES (?,?,?,?,?,?)", rows)
        @contextmanager
        def connection():
            yield conn
        obj = SimpleNamespace(connection=connection, path=Path("isolated.sqlite3"))
        summary = isolated_function("live_trader/program_ledger.py", "summary", {}, owner="ProgramLedger")(obj)
        self.assertEqual(8, summary["execution_event_count"])
        self.assertEqual(2, summary["fill_event_count"])
        self.assertEqual(3, summary["snapshot_event_count"])
        self.assertEqual(3, summary["other_event_count"])
        self.assertEqual("2026-10-21", summary["last_fill_event"])
        self.assertEqual(8, conn.execute("SELECT COUNT(*) FROM execution_events").fetchone()[0])


class DisplayOnlyRefreshTests(unittest.TestCase):
    def reconcile_namespace(self):
        summary = {"status_label": "정상", "mismatch_count": 0, "api_required_count": 0}
        return {
            "STATE": {"broker_truth_blocked": True, "new_entries_blocked": True, "broker_reconciliation": {"errors": []}},
            "datetime": datetime, "sqlite3": sqlite3,
            "refresh_broker_reconciliation": Mock(return_value={"errors": []}),
            "reconciliation_snapshot": Mock(return_value={"summary": summary}),
            "broker_position_truth_snapshot": Mock(return_value={"newEntriesBlocked": False}),
            "append_audit": Mock(), "sync_operational_incident": Mock(),
            "LIVE_CONTINUOUS_CONTROLLER": Mock(), "PROGRAM_LEDGER": Mock(),
            "automatic_live_promotion_sweep": Mock(return_value=[{"promoted": True}]),
            "snapshot": Mock(return_value={}),
        }

    def test_display_read_does_not_promote_resume_dispatch_or_clear_entry_latch(self):
        ns = self.reconcile_namespace()
        run = isolated_function("live_trader/state.py", "_run_reconciliation_without_public_fence", ns)
        result = run(refresh_brokers=True, include_snapshot=False, display_only=True)
        self.assertTrue(result["ok"])
        self.assertEqual([], result["automatic_promotion"])
        ns["automatic_live_promotion_sweep"].assert_not_called()
        ns["LIVE_CONTINUOUS_CONTROLLER"].reconcile_deferred_dispatches.assert_not_called()
        self.assertTrue(ns["STATE"]["new_entries_blocked"])

    def test_runtime_default_keeps_reconciliation_and_promotion_behavior(self):
        ns = self.reconcile_namespace()
        run = isolated_function("live_trader/state.py", "_run_reconciliation_without_public_fence", ns)
        run(refresh_brokers=True, include_snapshot=False)
        ns["automatic_live_promotion_sweep"].assert_called_once_with()
        ns["LIVE_CONTINUOUS_CONTROLLER"].reconcile_deferred_dispatches.assert_called_once()
        self.assertFalse(ns["STATE"]["new_entries_blocked"])

    def test_poll_records_observation_without_promoting_even_with_new_fill(self):
        ns = {"datetime": datetime, "time": time, "hashlib": hashlib, "json": json,
              "STATE": {"program_ledger": {}}, "LiveBrokerRouter": Mock(),
              "LIVE_EXECUTION_STREAMS": Mock(), "PROGRAM_LEDGER": Mock()}
        ns["LIVE_EXECUTION_STREAMS"].drain.return_value = []
        ns["PROGRAM_LEDGER"].existing_execution_event_ids.return_value = set()
        ns["PROGRAM_LEDGER"].record_execution_events.return_value = 1
        for name in ["observe_execution_stream_connectivity", "mark_broker_snapshot_failure", "record_connectivity_state", "stable_broker_snapshot_digest", "record_new_execution_traces", "merge_broker_reconciliation_cache", "append_audit"]:
            ns[name] = Mock()
        for name, value in {"broker_snapshot_poll_interval_seconds": 60, "broker_snapshot_is_due": True, "mark_broker_snapshot_success": True,
                            "apply_execution_events_to_local_orders": 1, "kis_order_truth_snapshot": {}, "notify_new_live_fills": 0,
                            "should_append_execution_sync_audit": False, "program_ledger_snapshot": {}, "execution_event_snapshot": {},
                            "automatic_live_promotion_sweep": []}.items():
            ns[name] = Mock(return_value=value)
        ns["fetch_broker_snapshots"] = Mock(return_value={"upbit": {"events": [{"state": "filled", "side": "BUY"}], "accounts": [], "positions": []}})
        ns["normalize_broker_execution"] = Mock(return_value=SimpleNamespace(event_id="fill-one", client_order_id="client-one", broker_order_id="order-one", symbol="KRW-BTC", filled_quantity=1, fill_price=10, fee=0, status="filled", occurred_at="2026-10-28", instrument_id="", raw={}))
        ns["deduplicate_execution_events"] = lambda events: events
        poll = isolated_function("live_trader/state.py", "_poll_execution_events_unlocked", ns)
        poll("upbit", force_snapshot=True, include_snapshot=False, display_only=True)
        ns["PROGRAM_LEDGER"].record_execution_events.assert_called_once()
        ns["automatic_live_promotion_sweep"].assert_not_called()

    def test_public_wrappers_forward_display_only(self):
        ns = {"_run_reconciliation_without_public_fence": Mock(return_value={})}
        run = isolated_function("live_trader/state.py", "run_reconciliation", ns)
        run(display_only=True)
        self.assertTrue(ns["_run_reconciliation_without_public_fence"].call_args.kwargs["display_only"])
        ns = {"BROKER_POLL_LOCK": Mock(), "_poll_execution_events_unlocked": Mock(return_value={})}
        run = isolated_function("live_trader/state.py", "poll_execution_events", ns)
        run(display_only=True)
        self.assertTrue(ns["_poll_execution_events_unlocked"].call_args.kwargs["display_only"])

    def test_routes_accept_only_boolean_true_for_display_only(self):
        for route, target in [("/api/reconcile", "run_reconciliation"), ("/api/execution-events", "poll_execution_events")]:
            for value in [True, False, "true", 1, None]:
                with self.subTest(route=route, value=value):
                    state = Mock()
                    ns = {"state": state, "urlparse": urlparse, "_FUNCTIONAL_MUTATION_PATHS": set(), "_FUNCTIONAL_STATUS_PATHS": set()}
                    post = isolated_function("live_trader/server.py", "do_POST", ns, owner="LiveTraderHandler")
                    handler = SimpleNamespace(path=route, read_json=Mock(return_value={"display_only": value}), send_json=Mock())
                    post(handler)
                    self.assertIs(getattr(state, target).call_args.kwargs["display_only"], value is True)


if __name__ == "__main__":
    unittest.main()
