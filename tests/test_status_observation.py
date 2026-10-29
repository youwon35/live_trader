from __future__ import annotations

from contextlib import ExitStack, nullcontext
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
import unittest
import ast
from pathlib import Path
import tempfile
import threading
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.status_observation import broker_account_observation, cached_preflight_reconciliation, completed_broker_poll_observations, explicit_strategy_broker_id, broker_execution_read_coverage, strategy_execution_observation_scope, DeploymentContextError


class StatusObservationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now().replace(microsecond=0)
        self.at = self.now.isoformat(sep=" ")
        self.old = (self.now - timedelta(minutes=5)).isoformat(sep=" ")
        self.touched = ("broker_reconciliation", "execution_events", "program_ledger", "checklist", "selected_deployment_id",
                        "selected_strategy_id", "dry_run", "operator_confirmed", "new_entries_blocked", "kill_switch", "broker_order_truth", "broker_truth_blocked")
        self.original = {key: deepcopy(state.STATE.get(key)) for key in self.touched}
        self.cache = {"accounts": [{"broker_id": "binance", "currency": "USDT", "account": "SYNTHETIC", "broker_cash": 100}],
                      "positions": [], "successful_account_brokers": ["binance"], "successful_position_brokers": ["binance"],
                      "errors": [], "account_observations": {"binance": {"observedAt": self.at, "lastSuccessAt": self.at, "lastAttemptAt": self.at}},
                      "position_observations": {"binance": {"observedAt": self.at}}, "fetched_at": self.at}
        self.reconciliation = {"accounts": [{"broker_id": "binance", "status": "pass", "program_source": "execution_ledger", "program_updated_at": self.old}],
                               "positions": [{"broker_id": "binance", "status": "pass"}], "errors": [],
                               "summary": {"status": "pass", "status_label": "정상", "last_run": self.at, "blocking_count": 0}}
        self.execution_reads = {"binance": {"status": "success", "coverage": "COMPLETE_ORDER_EXECUTION_READ", "source": "SYNTHETIC_EXACT_EXECUTION_READER"}}
        proof = completed_broker_poll_observations(self.cache, {"binance"}, {}, [], observed_at=self.at, execution_reads=self.execution_reads)
        self.events, self.ledger = {"last_poll": self.at, "errors": [], "broker_observations": deepcopy(proof)}, {"last_event_sync": self.at, "broker_observations": deepcopy(proof)}
        state.STATE.update(broker_reconciliation=deepcopy(self.cache), execution_events=deepcopy(self.events),
                           program_ledger=deepcopy(self.ledger), checklist={item["key"]: True for item in state.CHECKLIST_ITEMS},
                           selected_deployment_id="synthetic-deployment", selected_strategy_id="synthetic-strategy",
                           dry_run=False, operator_confirmed=True, new_entries_blocked=False, kill_switch=False)

    def tearDown(self):
        state.STATE.update(self.original)

    def project(self, cache=None, reconciliation=None, events=None, ledger=None):
        return cached_preflight_reconciliation("binance", reconciliation or self.reconciliation,
            self.cache if cache is None else cache, self.events if events is None else events,
            self.ledger if ledger is None else ledger, now=self.now)

    def test_aggregate_fetch_time_never_fills_missing_broker_account_observation(self):
        self.cache["account_observations"] = {}
        result = broker_account_observation(self.cache, "binance", now=self.now)
        self.assertEqual("unknown", result["lookup_status"])
        self.assertEqual("", result["observed_at"])
        self.assertFalse(self.project()["summary"]["fresh"])

    def test_upbit_refresh_does_not_make_old_binance_balance_fresh(self):
        self.cache["account_observations"]["binance"]["observedAt"] = self.old
        self.cache["account_observations"]["upbit"] = {"observedAt": self.at}
        self.cache["fetched_at"] = self.at
        result = broker_account_observation(self.cache, "binance", now=self.now)
        self.assertEqual("stale", result["lookup_status"])
        self.assertGreater(result["observation_age_seconds"], 60)

    def test_future_or_malformed_timestamp_remains_unknown(self):
        for value in ((self.now + timedelta(seconds=5)).isoformat(), "bad-time", "20:22:49"):
            with self.subTest(value=value):
                self.cache["account_observations"]["binance"]["observedAt"] = value
                self.assertEqual("unknown", broker_account_observation(self.cache, "binance", now=self.now)["lookup_status"])

    def test_failed_lookup_keeps_last_success_time_without_claiming_current_success(self):
        self.cache["errors"] = [{"broker_id": "binance", "scope": "snapshot", "detail": "synthetic timeout"}]
        result = broker_account_observation(self.cache, "binance", now=self.now)
        self.assertEqual("error", result["lookup_status"])
        self.assertEqual(self.at, result["last_success_at"])
        self.assertFalse(result["account_observation_fresh"])

    def test_empty_account_rows_never_become_success_from_success_list_alone(self):
        self.cache["accounts"] = []
        self.assertEqual("unknown", broker_account_observation(self.cache, "binance", now=self.now)["lookup_status"])

    def test_selected_cached_preflight_has_fresh_flags_without_network_or_authority(self):
        before = deepcopy((self.cache, self.reconciliation, self.events, self.ledger))
        result = self.project()
        self.assertTrue(result["summary"]["fresh"])
        self.assertTrue(result["summary"]["three_way_verified"])
        self.assertTrue(result["observation_only"])
        self.assertFalse(result["authority_granted"])
        self.assertTrue(result["requires_fresh_read_for_admission"])
        self.assertEqual(before, (self.cache, self.reconciliation, self.events, self.ledger))

    def test_unrelated_broker_mismatch_does_not_change_selected_broker_projection(self):
        data = deepcopy(self.reconciliation)
        data["accounts"].append({"broker_id": "kis", "status": "mismatch"})
        data["errors"].append({"broker_id": "kis", "scope": "snapshot", "detail": "synthetic other route"})
        result = self.project(reconciliation=data)
        self.assertTrue(result["summary"]["three_way_verified"])
        self.assertEqual("binance", result["summary"]["broker_id"])
        self.assertEqual([], result["errors"])

    def test_stale_ledger_or_position_missing_never_becomes_three_way_pass(self):
        self.assertFalse(self.project(ledger={"last_event_sync": self.old})["summary"]["fresh"])
        self.cache["successful_position_brokers"] = []
        self.assertFalse(self.project()["summary"]["three_way_verified"])

    def test_unknown_failed_or_missing_status_never_becomes_a_green_comparison(self):
        for status in ("unknown", "failed", None, "pending", "success"):
            with self.subTest(status=status):
                data = deepcopy(self.reconciliation)
                data["accounts"][0]["status"] = status
                result = self.project(reconciliation=data)["summary"]
                self.assertFalse(result["fresh"])
                self.assertFalse(result["three_way_verified"])
                self.assertEqual("warn", result["status"])

    def test_account_comparison_row_is_required_even_with_passed_positions(self):
        data = deepcopy(self.reconciliation)
        data["accounts"] = []
        result = self.project(reconciliation=data)["summary"]
        self.assertFalse(result["three_way_verified"])
        self.assertIn("broker-account-reconciliation-row-missing", result["freshness_detail"])

    def test_missing_original_ledger_source_never_becomes_a_green_comparison(self):
        for source, stamp in ((None, self.old), ("missing", self.old), ("execution_ledger", "")):
            with self.subTest(source=source, stamp=stamp):
                data = deepcopy(self.reconciliation)
                data["accounts"][0].update(program_source=source, program_updated_at=stamp)
                self.assertFalse(self.project(reconciliation=data)["summary"]["three_way_verified"])

    def test_other_broker_global_attempts_never_replace_exact_event_and_ledger_success(self):
        events = {"last_poll": self.at, "errors": [], "snapshot_skipped_brokers": ["binance"],
                  "snapshot_changed_brokers": ["upbit"], "snapshot_poll": {"brokers": {"binance": {"last_success_at": self.old}}}}
        ledger = {"last_event_sync": self.at, "last_baseline": None}
        result = self.project(events=events, ledger=ledger)["summary"]
        self.assertFalse(result["fresh"])
        self.assertFalse(result["three_way_verified"])

    def test_new_account_read_cannot_reuse_previous_poll_processing_proof(self):
        self.cache["account_observations"]["binance"]["observedAt"] = (self.now - timedelta(seconds=1)).isoformat()
        result = self.project()["summary"]
        self.assertFalse(result["fresh"])
        self.assertIn("execution_event-snapshot-binding-unverified", result["freshness_detail"])

    def test_event_processing_for_other_broker_does_not_advance_selected_proof(self):
        old = deepcopy(self.events["broker_observations"])
        updated = completed_broker_poll_observations(self.cache, {"upbit"}, old, [], observed_at=self.old)
        self.assertEqual(old["binance"], updated["binance"])
        self.assertEqual(old, self.events["broker_observations"])

    def test_failed_poll_retains_original_timestamp_but_invalidates_success(self):
        proofs = completed_broker_poll_observations(self.cache, {"binance"}, self.events["broker_observations"],
            [{"broker_id": "binance", "detail": "synthetic failed read"}], observed_at=self.at, execution_reads=self.execution_reads)
        self.assertEqual("error", proofs["binance"]["status"])
        events = {**self.events, "broker_observations": proofs}
        self.assertFalse(self.project(events=events)["summary"]["three_way_verified"])

    def test_a_complete_empty_event_poll_is_read_evidence_without_fabricated_fills_or_baseline(self):
        proof = completed_broker_poll_observations(self.cache, {"binance"}, {}, [], observed_at=self.at, execution_reads=self.execution_reads)
        self.assertEqual("success", proof["binance"]["status"])
        self.assertEqual(self.at, proof["binance"]["accountObservedAt"])
        self.assertNotIn("fill_count", proof["binance"])
        self.assertNotIn("last_baseline", proof["binance"])

    def test_partial_position_only_merge_preserves_account_time_and_row(self):
        state.STATE["broker_reconciliation"]["account_observations"]["binance"]["observedAt"] = self.old
        with patch.object(state, "refresh_account_risk_budget") as risk_update:
            state.merge_broker_reconciliation_cache([], [{"broker_id": "binance", "symbol": "SYNTHETIC"}], {"binance"}, [],
                successful_account_brokers=set(), successful_position_brokers={"binance"})
        cache = state.STATE["broker_reconciliation"]
        self.assertEqual(self.old, cache["account_observations"]["binance"]["observedAt"])
        self.assertEqual(1, len(cache["accounts"]))
        self.assertEqual("stale", broker_account_observation(cache, "binance")["lookup_status"])
        risk_update.assert_not_called()

    def test_partial_account_only_merge_does_not_refresh_position_time(self):
        state.STATE["broker_reconciliation"]["position_observations"]["binance"]["observedAt"] = self.old
        with patch.object(state, "refresh_account_risk_budget"):
            state.merge_broker_reconciliation_cache(self.cache["accounts"], [], {"binance"}, [],
                successful_account_brokers={"binance"}, successful_position_brokers=set())
        self.assertEqual(self.old, state.STATE["broker_reconciliation"]["position_observations"]["binance"]["observedAt"])

    def test_other_broker_merge_never_advances_selected_account_observation(self):
        original = deepcopy(state.STATE["broker_reconciliation"]["account_observations"]["binance"])
        with patch.object(state, "refresh_account_risk_budget"):
            state.merge_broker_reconciliation_cache([{"broker_id": "upbit"}], [], {"upbit"}, [])
        self.assertEqual(original, state.STATE["broker_reconciliation"]["account_observations"]["binance"])

    def test_observation_route_never_guesses_from_symbol_asset_or_market_type(self):
        for row in ({"asset": "CRYPTO", "symbol": "BTCUSDT"}, {"symbol": "KRW-BTC"},
                    {"symbol": "005930", "asset": "STOCK"}, {"marketType": "futures"},
                    {"broker_id": "CRYPTO", "provider": "binance"},
                    {"broker_id": "binance", "route": "upbit"},
                    {"provider": "binance", "marketType": "futures"}):
            with self.subTest(row=row):
                self.assertEqual("", explicit_strategy_broker_id(row))

    def test_observation_route_accepts_exact_supported_execution_route(self):
        self.assertEqual("binance-futures", explicit_strategy_broker_id({"broker_id": "binance-futures", "dataset": {"provider": "binance"}}))
        self.assertEqual("upbit", explicit_strategy_broker_id({"provider": "upbit"}))
        self.assertEqual("kis", explicit_strategy_broker_id({"route": "kis"}))

    def test_forced_preflight_requires_actual_broker_processing_even_with_new_global_attempt(self):
        state.STATE["execution_events"] = {"last_poll": self.at, "errors": []}
        state.STATE["program_ledger"] = {"last_event_sync": self.at}
        with patch.object(state, "strategy_broker_id", return_value="binance"), patch.object(state, "poll_execution_events", return_value={"ok": True}), patch.object(state, "reconciliation_snapshot", return_value=self.reconciliation):
            result = state.refresh_preflight_reconciliation({"broker_id": "binance"})
        self.assertFalse(result["summary"]["fresh"])
        self.assertFalse(result["summary"]["three_way_verified"])
        self.assertIn("execution_event-broker-success-unverified", result["summary"]["freshness_detail"])

    def test_machine_checklist_is_scoped_fresh_and_does_not_change_human_checks(self):
        state.STATE["checklist"] = {item["key"]: False for item in state.CHECKLIST_ITEMS}
        broker = SimpleNamespace(to_dict=lambda: {"broker_id": "binance", "missing_env": []})
        with patch.object(state, "broker_readiness", return_value=[broker]):
            selected = {row["key"]: row for row in state.checklist_rows(self.project()["summary"], broker_ids=["binance"])}
            global_rows = state.automatic_checklist_evidence(self.project()["summary"])
        self.assertTrue(selected["api_keys_reviewed"]["checked"])
        self.assertTrue(selected["position_reconcile_reviewed"]["checked"])
        self.assertFalse(selected["risk_limits_reviewed"]["checked"])
        self.assertFalse(selected["operator_takeover_ready"]["checked"])
        self.assertFalse(global_rows["api_keys_reviewed"][0])
        self.assertTrue(all(not checked for checked in state.STATE["checklist"].values()))

    def test_stored_manual_true_never_overrides_stale_machine_evidence(self):
        state.STATE["broker_reconciliation"]["account_observations"]["binance"]["observedAt"] = self.old
        broker = SimpleNamespace(to_dict=lambda: {"broker_id": "binance", "missing_env": []})
        with patch.object(state, "broker_readiness", return_value=[broker]):
            self.assertFalse(state.automatic_checklist_evidence(self.project()["summary"], broker_ids=["binance"])["api_keys_reviewed"][0])

    def test_account_rows_expose_only_exact_broker_observation_time(self):
        book = [{"broker_id": "binance", "broker_cash": None, "program_cash": None, "currency": "USDT", "detail": "synthetic", "account": "***", "broker_name": "Binance"}]
        with patch.object(state, "ACCOUNT_RECONCILIATION_BOOK", book), patch.object(state.PROGRAM_LEDGER, "cash_rows", return_value=[]):
            row = state.account_reconciliation_rows()[0]
        self.assertEqual(self.at, row["observed_at"])
        self.assertEqual("success", row["lookup_status"])
        self.assertFalse(row["authority_granted"])

    def test_forced_preflight_still_requires_new_noncoalesced_poll(self):
        strategy = {"broker_id": "binance"}
        with patch.object(state, "strategy_broker_id", return_value="binance"), patch.object(state, "poll_execution_events", return_value={"ok": True, "coalesced": True}), patch.object(state, "reconciliation_snapshot", return_value=self.reconciliation):
            result = state.refresh_preflight_reconciliation(strategy)
        self.assertFalse(result["summary"]["fresh"])
        self.assertFalse(result["summary"]["three_way_verified"])
        self.assertIn("broker-poll-coalesced", result["summary"]["freshness_detail"])

    def test_missing_account_timestamp_blocks_actual_forced_preflight_even_with_fresh_positions(self):
        state.STATE["broker_reconciliation"]["account_observations"] = {}
        with patch.object(state, "strategy_broker_id", return_value="binance"), patch.object(state, "poll_execution_events", return_value={"ok": True}), patch.object(state, "reconciliation_snapshot", return_value=self.reconciliation):
            result = state.refresh_preflight_reconciliation({"broker_id": "binance"})
        self.assertFalse(result["summary"]["fresh"])
        self.assertIn("account_snapshot-timestamp-missing", result["summary"]["freshness_detail"])

    def run_synthetic_poll(self, result, *, due=True, broker_id="binance"):
        stream = Mock()
        stream.drain.return_value = []
        ledger = Mock()
        ledger.existing_execution_event_ids.return_value = set()
        ledger.record_execution_events.return_value = 0
        ledger.order_dispatch_rows.return_value = []
        patches = {
            "LiveBrokerRouter": Mock(), "LIVE_EXECUTION_STREAMS": stream, "PROGRAM_LEDGER": ledger,
            "fetch_broker_snapshots": Mock(return_value={broker_id: result}),
            "broker_snapshot_poll_interval_seconds": Mock(return_value=60), "broker_snapshot_is_due": Mock(return_value=due),
            "observe_execution_stream_connectivity": Mock(), "record_connectivity_state": Mock(),
            "mark_broker_snapshot_success": Mock(return_value=True), "mark_broker_snapshot_failure": Mock(),
            "apply_execution_events_to_local_orders": Mock(return_value=0), "record_new_execution_traces": Mock(),
            "refresh_account_risk_budget": Mock(), "notify_new_live_fills": Mock(return_value=0),
            "should_append_execution_sync_audit": Mock(return_value=False), "program_ledger_snapshot": Mock(return_value={}),
            "execution_event_snapshot": Mock(return_value={}), "kis_order_truth_snapshot": Mock(return_value={}),
            "automatic_live_promotion_sweep": Mock(), "append_audit": Mock(),
        }
        if broker_id == "kis":
            patches.pop("kis_order_truth_snapshot")  # Exercise the real existing truth validator/snapshot.
            patches["_kis_environment_order_account_binding"] = Mock(return_value={
                "schemaVersion": "kis-order-account-binding/v1", "accountCanoHash": "a" * 64, "accountProductCode": "01",
                "accountFingerprint": "b" * 64, "credentialConfigurationHash": "c" * 64, "environmentRevision": "d" * 64})
        with ExitStack() as stack:
            for name, value in patches.items():
                stack.enter_context(patch.object(state, name, value))
            state._poll_execution_events_unlocked(broker_id, force_snapshot=True, include_snapshot=False, display_only=True)
        patches["automatic_live_promotion_sweep"].assert_not_called()
        ledger.seed_snapshot.assert_not_called()
        return ledger

    def test_snapshot_only_crypto_poll_never_claims_actual_execution_read(self):
        for broker_id, source in (("binance", "binance_account_poll"), ("binance-futures", "binance_futures_account_poll"), ("upbit", "upbit_accounts_poll")):
            with self.subTest(broker_id=broker_id):
                state.STATE["execution_events"] = {"errors": []}
                state.STATE["program_ledger"] = {}
                row = {**self.cache["accounts"][0], "broker_id": broker_id}
                result = {"broker_id": broker_id, "accounts": [row], "positions": [],
                          "events": [{"state": "account_snapshot", "broker_id": broker_id}], "source": source}
                ledger = self.run_synthetic_poll(result, broker_id=broker_id)
                ledger.record_execution_events.assert_called_once_with([])
                self.assertNotIn(broker_id, state.STATE["execution_events"]["broker_observations"])
                self.assertEqual("ACCOUNT_POSITION_SNAPSHOT_ONLY", state.STATE["execution_events"]["read_coverage"][broker_id]["coverage"])

    def test_validated_kis_order_truth_records_exact_processing_after_ledger_completion(self):
        result = {"broker_id": "kis", "accounts": [{**self.cache["accounts"][0], "broker_id": "kis"}], "positions": [], "events": [],
                  "source": "kis_balance_and_domestic_order_truth_poll",
                  "execution_truth": {"complete": True, "pagination_complete": True, "absence_is_authoritative": True, "orders": [], "page_count": 1}}
        ledger = self.run_synthetic_poll(result, broker_id="kis")
        ledger.record_execution_events.assert_called_once_with([])
        cache = state.STATE["broker_reconciliation"]
        proof = state.STATE["execution_events"]["broker_observations"]["kis"]
        self.assertEqual(cache["account_observations"]["kis"]["observedAt"], proof["accountObservedAt"])
        self.assertEqual(cache["position_observations"]["kis"]["observedAt"], proof["positionObservedAt"])
        self.assertEqual(proof, state.STATE["program_ledger"]["broker_observations"]["kis"])
        self.assertEqual("COMPLETE_ORDER_EXECUTION_READ", proof["executionReadCoverage"])
        self.assertEqual("kis_balance_and_domestic_order_truth_poll", proof["executionReadSource"])
        self.assertTrue(state.STATE["broker_order_truth"]["kis"]["complete"])

    def test_execution_read_capability_cannot_be_inferred_from_events_list_or_source_string_alone(self):
        for broker_id, payload, validated in (("kis", {"broker_id": "kis", "source": "kis_balance_and_domestic_order_truth_poll", "events": []}, False),
                                            ("binance", {"broker_id": "binance", "source": "kis_balance_and_domestic_order_truth_poll", "events": []}, True),
                                            ("upbit", {"events": []}, False)):
            with self.subTest(broker_id=broker_id):
                self.assertNotEqual("success", broker_execution_read_coverage(broker_id, payload, validated_kis_order_truth=validated)["status"])
        self.assertEqual({}, completed_broker_poll_observations(self.cache, {"binance"}, {}, [], observed_at=self.at))

    def test_snapshot_only_read_cannot_reuse_previous_complete_execution_proof(self):
        events = {**self.events, "read_coverage": {"binance": {"status": "unverified", "coverage": "ACCOUNT_POSITION_SNAPSHOT_ONLY", "source": "binance_account_poll"}}}
        projected = self.project(events=events)["summary"]
        self.assertFalse(projected["fresh"])
        self.assertFalse(projected["three_way_verified"])
        self.assertIn("account-position-snapshot-is-not-execution-read", projected["freshness_detail"])

    def test_kis_domestic_execution_read_does_not_certify_foreign_account_or_position_rows(self):
        at = self.at
        cache = deepcopy(self.cache)
        cache["accounts"] = [{"broker_id": "kis", "account": "SYNTHETIC", "currency": "KRW", "broker_cash": 100}]
        for key in ("successful_account_brokers", "successful_position_brokers"):
            cache[key] = ["kis"]
        cache["account_observations"] = {"kis": {"observedAt": at}}
        cache["position_observations"] = {"kis": {"observedAt": at}}
        reads = {"kis": broker_execution_read_coverage("kis", {"broker_id": "kis", "source": "kis_balance_and_domestic_order_truth_poll"}, validated_kis_order_truth=True)}
        proof = completed_broker_poll_observations(cache, {"kis"}, {}, [], observed_at=at, execution_reads=reads)
        events = {"broker_observations": proof, "read_coverage": reads}
        ledger = {"broker_observations": proof}
        data = {"accounts": [{"broker_id": "kis", "currency": "KRW", "status": "pass", "program_source": "synthetic_execution_ledger", "program_updated_at": at}], "positions": [], "errors": []}
        self.assertTrue(cached_preflight_reconciliation("kis", data, cache, events, ledger, now=self.now, required_execution_scope="KIS_DOMESTIC_ONLY")["summary"]["three_way_verified"])
        data["positions"].append({"broker_id": "kis", "currency": "USD", "status": "mismatch"})
        original = deepcopy(data)
        result = cached_preflight_reconciliation("kis", data, cache, events, ledger, now=self.now, required_execution_scope="KIS_DOMESTIC_ONLY")
        self.assertTrue(result["summary"]["three_way_verified"])
        self.assertEqual([], result["positions"])
        self.assertEqual(original, data)  # Foreign facts remain intact in the global view.
        data["positions"].append({"broker_id": "kis", "status": "pass"})
        result = cached_preflight_reconciliation("kis", data, cache, events, ledger, now=self.now, required_execution_scope="KIS_DOMESTIC_ONLY")["summary"]
        self.assertFalse(result["three_way_verified"])
        self.assertIn("kis-domestic-execution-read-currency-scope-unverified", result["freshness_detail"])

    def test_domestic_kis_read_requires_explicit_selected_domestic_market_even_when_flat(self):
        for strategy in ({"broker_id": "kis", "symbol": "005930", "currency": "KRW"},
                         {"broker_id": "kis", "symbol": "AAPL", "exchange": "NASDAQ"},
                         {"broker_id": "kis", "exchange": "KRX", "parameters": {"exchange": "NYSE"}},
                         {"broker_id": "kis", "exchange": "KRX", "currency": "USD"},
                         {"broker_id": "kis", "exchange": "KRX", "portfolio_gate": {"portfolioId": "mixed"}}):
            with self.subTest(strategy=strategy):
                self.assertEqual("UNVERIFIED", strategy_execution_observation_scope(strategy))
        self.assertEqual("KIS_DOMESTIC_ONLY", strategy_execution_observation_scope({"broker_id": "kis", "exchange": "KRX", "currency": "KRW"}))
        self.assertEqual("KIS_DOMESTIC_ONLY", strategy_execution_observation_scope({"broker_id": "kis", "dataset": {"exchange": "XKRX"}}))
        cache = deepcopy(self.cache)
        cache["accounts"] = [{"broker_id": "kis", "currency": "KRW"}]
        cache["successful_account_brokers"] = cache["successful_position_brokers"] = ["kis"]
        cache["account_observations"] = cache["position_observations"] = {"kis": {"observedAt": self.at}}
        reads = {"kis": broker_execution_read_coverage("kis", {"broker_id": "kis", "source": "kis_balance_and_domestic_order_truth_poll"}, validated_kis_order_truth=True)}
        proof = completed_broker_poll_observations(cache, {"kis"}, {}, [], observed_at=self.at, execution_reads=reads)
        data = {"accounts": [{"broker_id": "kis", "currency": "KRW", "status": "pass", "program_source": "synthetic_ledger", "program_updated_at": self.at}], "positions": []}
        for scope in ("UNVERIFIED", "KIS_US_ONLY", ""):
            with self.subTest(scope=scope):
                result = cached_preflight_reconciliation("kis", data, cache, {"broker_observations": proof, "read_coverage": reads}, {"broker_observations": proof}, now=self.now, required_execution_scope=scope)
                self.assertFalse(result["summary"]["three_way_verified"])
                self.assertIn("kis-selected-execution-scope-unverified", result["summary"]["freshness_detail"])

    def test_actual_cleanup_caller_and_forced_preflight_share_original_sealed_domestic_scope(self):
        from live_trader.kis_owned_capital import KisOwnedCapitalStore
        from test_kis_equity_operational import fixture as source_fixture
        from test_kis_scoped_cleanup_truth import fixture as owned_fixture, NOW
        scope, _owned, _working, _history = owned_fixture('KRX')
        source_spec, _contract, _context = source_fixture('KRX')
        context = {'deploymentId':scope['deploymentId'], 'accountFingerprint':scope['accountFingerprint'],
                   'credentialConfigurationHash':scope['credentialConfigurationHash'], 'capitalCurrency':'KRW'}
        with tempfile.TemporaryDirectory() as directory:
            store = KisOwnedCapitalStore(Path(directory) / 'original-principal.sqlite3')
            store.initialize(scope, {'currency':'KRW','profileId':scope['accountProfileId'],
                'accountFingerprint':scope['accountFingerprint'],'credentialConfigurationHash':scope['credentialConfigurationHash'],
                'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE','asOf':NOW.isoformat(),'availableCash':'100000'}, now=NOW)
            native = SimpleNamespace(store=store, specs=[source_spec], observer=object(), sync_owned_fills=Mock())
            controller = SimpleNamespace(_native_runtime=native)
            cache = deepcopy(self.cache)
            cache['accounts'] = [{'broker_id':'kis','account':'SYNTHETIC','currency':'KRW','broker_cash':100}]
            cache['successful_account_brokers'] = cache['successful_position_brokers'] = ['kis']
            cache['account_observations'] = cache['position_observations'] = {'kis':{'observedAt':self.at}}
            reads = {'kis':broker_execution_read_coverage('kis',{'broker_id':'kis','source':'kis_balance_and_domestic_order_truth_poll'},validated_kis_order_truth=True)}
            proof = completed_broker_poll_observations(cache, {'kis'}, {}, [], observed_at=self.at, execution_reads=reads)
            state.STATE.update(broker_reconciliation=cache, execution_events={'errors':[],'last_poll':self.at,'broker_observations':proof,'read_coverage':reads},
                               program_ledger={'last_event_sync':self.at,'broker_observations':proof})
            reconciliation = {'accounts':[{'broker_id':'kis','currency':'KRW','status':'pass','program_source':'synthetic_ledger','program_updated_at':self.at}],
                              'positions':[{'broker_id':'kis','currency':'USD','status':'mismatch'}], 'errors':[]}
            cleanup = Mock()
            def finish(_key, **result):
                if result['reconciliation_passed'] is not True:
                    raise ValueError('test-final-clean-needs-actual-scoped-reconciliation')
            cleanup.finish.side_effect = finish
            wrapped_preflight = Mock(wraps=state.refresh_preflight_reconciliation)
            manager = SimpleNamespace(complete_cleanup_writer=Mock())
            ns = {'__package__':'live_trader','datetime':datetime, 'STATE':{'active_runtime_session_ids':{'runtime-kis':'session-kis'}},
                  'SAFETY_CONFIRMATION_MUTATION_LOCK':threading.RLock(), 'kis_route_authority_serialization':lambda:nullcontext(),
                  '_assert_kis_preflight_collection_outside_authority_locks_for_runtime':lambda _key:True,
                  '_kis_cleanup_current_owner':lambda *_args,**_kwargs:(object(),controller),
                  '_recover_kis_cleanup_owned_identities':lambda *_args:[], '_kis_cleanup_store':lambda:cleanup,
                  'refresh_preflight_reconciliation':wrapped_preflight, 'OPERATIONAL_GOVERNANCE':SimpleNamespace(transition_runtime_session=Mock()),
                  '_revoke_runtime_order_authority':Mock(), 'LIVE_CONTINUOUS_CONTROLLER':manager}
            tree=ast.parse(Path(state.__file__).read_text(encoding='utf-8'))
            node=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='_finish_kis_owned_stop_cleanup')
            exec(compile(ast.Module(body=[node],type_ignores=[]), '<actual-cleanup-source>', 'exec'), ns)
            plan={'cleanupId':'cleanup-kis','runtimeScopeId':'runtime-kis','sessionId':'session-kis','context':context,'reserved':[]}
            with patch.object(state,'poll_execution_events',return_value={'ok':True,'coalesced':False,'errors':[]}), patch.object(state,'reconciliation_snapshot',return_value=reconciliation):
                result=ns['_finish_kis_owned_stop_cleanup'](plan)
            self.assertTrue(result['ok'],result)
            self.assertEqual('KRX', wrapped_preflight.call_args.args[0]['exchange'])
            self.assertEqual(scope['scopeHash'], wrapped_preflight.call_args.args[0]['cleanup_source_scope_hash'])
            self.assertTrue(cleanup.finish.call_args.kwargs['reconciliation_passed'])
            manager.complete_cleanup_writer.assert_called_once_with('runtime-kis')
            native.sync_owned_fills.assert_called_once_with(scope)
            self.assertEqual('mismatch', reconciliation['positions'][0]['status'])

    def test_risk_engine_session_label_does_not_invent_a_global_block(self):
        with patch.object(state, "market_session_state", return_value={"state": "open", "detail": "SYNTHETIC"}):
            risk = next(row for row in state.market_sessions() if row["label"] == "Risk Engine")
        self.assertEqual("unknown", risk["state"])
        self.assertIn("개별 실행", risk["detail"])

    def test_missing_events_or_partial_snapshot_does_not_claim_completed_broker_processing(self):
        for result in ({"accounts": deepcopy(self.cache["accounts"]), "positions": []},
                       {"positions": [], "events": []}, {"accounts": [], "positions": [], "events": []}):
            with self.subTest(keys=tuple(result)):
                state.STATE["broker_reconciliation"] = deepcopy(self.cache)
                state.STATE["execution_events"] = {"errors": []}
                state.STATE["program_ledger"] = {}
                self.run_synthetic_poll(result)
                self.assertNotIn("binance", state.STATE["execution_events"]["broker_observations"])
                self.assertFalse(cached_preflight_reconciliation("binance", self.reconciliation, state.STATE["broker_reconciliation"],
                    state.STATE["execution_events"], state.STATE["program_ledger"])["summary"]["three_way_verified"])

    def test_skipped_actual_poll_preserves_old_broker_time_despite_new_global_attempt(self):
        state.STATE["execution_events"]["broker_observations"]["binance"]["observedAt"] = self.old
        old_proof = deepcopy(state.STATE["execution_events"]["broker_observations"])
        self.run_synthetic_poll({}, due=False)
        self.assertEqual(old_proof, state.STATE["execution_events"]["broker_observations"])
        self.assertFalse(cached_preflight_reconciliation("binance", self.reconciliation, state.STATE["broker_reconciliation"],
            state.STATE["execution_events"], state.STATE["program_ledger"])["summary"]["three_way_verified"])

    def test_snapshot_selected_scope_projects_fresh_flags_and_keeps_unrelated_global_failure(self):
        strategy = {"broker_id": "binance", "strategy_id": "synthetic-strategy", "deployment_id": "synthetic-deployment", "live_allowed": True}
        reconciliation = deepcopy(self.reconciliation)
        reconciliation["summary"].update(status="fail", status_label="불일치", blocking_count=1)
        reconciliation["accounts"].append({"broker_id": "upbit", "status": "mismatch"})
        broker = SimpleNamespace(to_dict=lambda: {"broker_id": "binance", "missing_env": [], "status": "ready", "live_order_adapter_ready": True, "order_ready": True})
        patches = {"broker_readiness": Mock(return_value=[broker]), "portfolio_rows": Mock(return_value=[]),
            "strategy_rows": Mock(return_value=[strategy]), "_attach_canary_execution_for_display": Mock(),
            "automation_profiles": Mock(return_value=[]), "reconciliation_snapshot": Mock(return_value=reconciliation),
            "order_queue_summary": Mock(return_value={"active": 0, "retryable": 0}),
            "watchdog_snapshot": Mock(return_value={"critical_count": 0, "warning_count": 0}), "readiness_checks": Mock(return_value=[]),
            "runtime_operational_readiness": Mock(return_value={}), "broker_position_truth_snapshot": Mock(return_value={}),
            "restart_recovery_plan_snapshot": Mock(return_value={}), "operational_governance_snapshot": Mock(return_value={}),
            "durable_audit_rows": Mock(return_value=[]), "emergency_stop_status": Mock(return_value={}),
            "market_sessions": Mock(return_value=[]), "risk_checks": Mock(return_value=[]), "risk_setting_rows": Mock(return_value=[]),
            "retry_policy_rows": Mock(return_value=[]), "retry_policy_matrix": Mock(return_value={}), "broker_diagnostics": Mock(return_value=[]),
            "broker_adapter_contract": Mock(return_value={}), "_kis_scoped_authority_status": Mock(return_value={}),
            "_kis_cleanup_status": Mock(return_value={}), "functional_test_runtime_guard_snapshot": Mock(return_value={}),
            "strategy_plugin_status": Mock(return_value={}), "order_rows": Mock(return_value=[]), "dry_run_ledger_rows": Mock(return_value=[]),
            "program_ledger_snapshot": Mock(return_value={}), "execution_event_snapshot": Mock(return_value={}), "kis_order_truth_snapshot": Mock(return_value={}),
            "binance_futures_canary_status": Mock(return_value={}), "binance_futures_settings_status": Mock(return_value={}),
            "binance_futures_fill_soak_status": Mock(return_value={}), "futures_capital_rollout_snapshot": Mock(return_value={}),
            "lineage_flow_snapshot": Mock(return_value={}), "execution_calibration_snapshot": Mock(return_value={}),
            "doctor_diagnostics_document": Mock(return_value={}), "operator_runtime_bindings": Mock(return_value={}),
            "operation_report": Mock(return_value={}), "_reconciliation_summary_for_broker_snapshot": Mock(return_value={}),
        }
        with ExitStack() as stack:
            for name, value in patches.items():
                stack.enter_context(patch.object(state, name, value))
            stack.enter_context(patch.object(state.LIVE_CONTINUOUS_CONTROLLER, "snapshot", return_value={}))
            stack.enter_context(patch.object(state.LIVE_EXECUTION_STREAMS, "snapshot", return_value={}))
            payload = state.snapshot()
        self.assertEqual("SELECTED_DEPLOYMENT", payload["preflight_scope"]["kind"])
        self.assertEqual("binance", payload["preflight_scope"]["broker_id"])
        self.assertEqual("fail", payload["reconciliation"]["summary"]["status"])
        selected = {row["label"]: row for row in payload["final_preflight"]}
        self.assertEqual("pass", selected["대조 증거 신선도"]["status"])
        self.assertEqual("pass", selected["포지션·계좌 대조"]["status"])
        self.assertFalse(payload["preflight_scope"]["authority_granted"])

    def test_source_context_validation_failure_is_not_mislabelled_as_database_failure(self):
        with patch.object(state, "_operational_manifest_inputs", side_effect=ValueError("synthetic unresolved route")), patch.object(state.OPERATIONAL_GOVERNANCE, "get_deployment_manifest") as database:
            with self.assertRaisesRegex(DeploymentContextError, "unresolved route"):
                state.ensure_operational_deployment_manifest({})
        database.assert_not_called()

    def test_store_integrity_value_error_keeps_its_original_failure_category(self):
        with patch.object(state, "_operational_manifest_inputs", return_value={"deployment_id": "synthetic"}), patch.object(state.OPERATIONAL_GOVERNANCE, "get_deployment_manifest", side_effect=ValueError("synthetic manifest hash mismatch")):
            with self.assertRaises(ValueError) as error:
                state.ensure_operational_deployment_manifest({})
        self.assertNotIsInstance(error.exception, DeploymentContextError)

    def test_dry_run_report_names_active_simulation_instead_of_disabled_protection(self):
        code, tab, action = state.doctor_issue_metadata("preflight", "Dry Run 보호")
        self.assertEqual("PREFLIGHT_DRY_RUN_ACTIVE", code)
        self.assertIn("모의 실행", action)
        self.assertNotIn("해제", action)


if __name__ == "__main__":
    unittest.main()
