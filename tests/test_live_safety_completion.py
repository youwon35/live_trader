from __future__ import annotations

import copy
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from live_trader import env_settings, state
from live_trader.operational_governance import OperationalGovernanceStore
from live_trader.order_management import OrderIntent


class LiveSafetyCompletionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))

    @staticmethod
    def _intent(*, risk_reducing: bool = False) -> OrderIntent:
        return OrderIntent(
            strategy_id="strategy-1",
            asset="CRYPTO",
            symbol="BTCUSDT",
            side="SELL" if risk_reducing else "BUY",
            quantity=0.001,
            reference_price=60_000,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={
                "broker_id": "binance",
                "risk_reducing": risk_reducing,
                "confirmed_bar_end": datetime.now().astimezone().isoformat(),
            },
        )

    def test_watchdog_incident_opens_and_resolves_with_health(self) -> None:
        critical = {
            "status": "fail",
            "status_label": "차단",
            "checks": [],
            "critical_count": 1,
            "warning_count": 0,
            "next_actions": ["시장 데이터 신선도"],
        }
        healthy = {
            "status": "pass",
            "status_label": "정상",
            "checks": [],
            "critical_count": 0,
            "warning_count": 0,
            "next_actions": ["Watchdog 정상"],
        }
        incident = Mock()
        with patch.object(state, "broker_readiness", return_value=[]), patch.object(
            state, "reconciliation_snapshot", return_value={"summary": {}}
        ), patch.object(state, "order_queue_summary", return_value={}), patch.object(
            state, "watchdog_snapshot", side_effect=[critical, critical]
        ), patch.object(state, "apply_watchdog_fail_closed", return_value=False), patch.object(
            state, "append_audit"
        ), patch.object(state, "sync_operational_incident", incident):
            state.run_watchdog(include_snapshot=False)

        self.assertTrue(incident.call_args.kwargs["active"])
        self.assertEqual("WATCHDOG_CRITICAL", incident.call_args.kwargs["code"])

        incident.reset_mock()
        with patch.object(state, "broker_readiness", return_value=[]), patch.object(
            state, "reconciliation_snapshot", return_value={"summary": {}}
        ), patch.object(state, "order_queue_summary", return_value={}), patch.object(
            state, "watchdog_snapshot", side_effect=[healthy, healthy]
        ), patch.object(state, "apply_watchdog_fail_closed", return_value=False), patch.object(
            state, "append_audit"
        ), patch.object(state, "sync_operational_incident", incident):
            state.run_watchdog(include_snapshot=False)

        self.assertFalse(incident.call_args.kwargs["active"])

    def test_reconciliation_incident_tracks_block_and_recovery(self) -> None:
        blocked_summary = {
            "status": "fail",
            "status_label": "차단",
            "api_required_count": 0,
            "capability_gap_count": 0,
            "mismatch_count": 1,
        }
        broker_data = {"errors": [], "accounts": [], "positions": []}
        incident = Mock()
        with patch.object(state, "refresh_broker_reconciliation", return_value=broker_data), patch.object(
            state, "reconciliation_snapshot", return_value={"summary": blocked_summary}
        ), patch.object(
            state,
            "broker_position_truth_snapshot",
            return_value={"newEntriesBlocked": True},
        ), patch.object(state, "automatic_live_promotion_sweep", return_value=[]), patch.object(
            state, "append_audit"
        ), patch.object(state, "sync_operational_incident", incident):
            state.run_reconciliation(include_snapshot=False)

        self.assertTrue(incident.call_args.kwargs["active"])
        self.assertEqual("CRITICAL", incident.call_args.kwargs["severity"])

        recovered_summary = {
            **blocked_summary,
            "status": "pass",
            "status_label": "정상",
            "mismatch_count": 0,
        }
        incident.reset_mock()
        with patch.object(state, "refresh_broker_reconciliation", return_value=broker_data), patch.object(
            state, "reconciliation_snapshot", return_value={"summary": recovered_summary}
        ), patch.object(
            state,
            "broker_position_truth_snapshot",
            return_value={"newEntriesBlocked": False},
        ), patch.object(state, "automatic_live_promotion_sweep", return_value=[]), patch.object(
            state, "append_audit"
        ), patch.object(state, "sync_operational_incident", incident):
            state.run_reconciliation(include_snapshot=False)

        self.assertFalse(incident.call_args.kwargs["active"])

    def test_kill_switch_cancels_known_working_orders_without_flattening(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        state.STATE["automation"]["crypto"].update(
            {"enabled": True, "mode": "SMALL_LIVE"}
        )
        state.STATE["orders"] = [
            {
                "order_id": "known-working",
                "state": "acknowledged",
                "queue_state": "submitted",
                "broker_order_id": "broker-1",
                "dry_run": False,
            },
            {
                "order_id": "ambiguous-working",
                "state": "unknown",
                "queue_state": "reconcile_required",
                "broker_order_id": "",
                "dry_run": False,
            },
            {
                "order_id": "already-filled",
                "state": "filled",
                "queue_state": "filled",
                "broker_order_id": "broker-2",
                "dry_run": False,
            },
        ]
        state.STATE["active_runtime_session_ids"] = {"crypto": "session-1"}

        class FakeGovernance:
            lifecycle = "RUNNING"

            def __init__(self) -> None:
                self.transitions: list[str] = []

            def get_runtime_session(self, _session_id: str) -> SimpleNamespace:
                return SimpleNamespace(session_id="session-1", lifecycle=self.lifecycle)

            def transition_runtime_session(self, _session_id: str, lifecycle: str, **_kwargs: object) -> SimpleNamespace:
                self.lifecycle = lifecycle
                self.transitions.append(lifecycle)
                return SimpleNamespace(session_id="session-1", lifecycle=lifecycle)

        governance = FakeGovernance()
        controller = Mock()
        controller.transition_running.return_value = {"ok": True, "results": {"crypto": {}}}
        cancel = Mock(return_value={"ok": True, "reason": "order canceled"})
        with patch.object(state, "LIVE_CONTINUOUS_CONTROLLER", controller), patch.object(
            state, "OPERATIONAL_GOVERNANCE", governance
        ), patch.object(state, "cancel_order", cancel), patch.object(
            state, "append_audit"
        ), patch.object(state, "sync_operational_incident"), patch.object(
            state, "snapshot", return_value={}
        ):
            result = state.set_flag("kill_switch", True)

        self.assertTrue(result["ok"])
        self.assertTrue(state.STATE["kill_switch"])
        self.assertTrue(state.STATE["kill_switch_rearm_required"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertEqual("MONITOR", state.STATE["mode"])
        cancel.assert_called_once_with("known-working")
        action = result["kill_switch_action"]
        self.assertFalse(action["flatten_requested"])
        self.assertEqual(1, action["cancellation"]["unresolved_count"])
        self.assertEqual(["DRAINING", "STOPPING", "STOPPED"], governance.transitions)
        self.assertNotIn("crypto", state.STATE["active_runtime_session_ids"])

    def test_dispatch_rechecks_kill_real_route_and_risk_increase(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["operator_confirmed"] = True
        state.STATE["kill_switch"] = False
        state.STATE["new_entries_blocked"] = True
        state.STATE["broker_reconciliation"] = {
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "successful_position_brokers": ["binance"],
            "errors": [],
            "positions": [
                {
                    "broker_id": "binance",
                    "symbol": "BTCUSDT",
                    "broker_qty": 0.01,
                }
            ],
        }
        with patch.object(state, "real_orders_enabled", return_value=True), patch.object(
            state, "durable_control_halt_active", return_value=False
        ):
            allowed, reason = state.live_broker_dispatch_allowed(
                self._intent(), dry_run=False
            )
            reduce_allowed, _ = state.live_broker_dispatch_allowed(
                self._intent(risk_reducing=True), dry_run=False
            )
            state.STATE["kill_switch"] = True
            killed, kill_reason = state.live_broker_dispatch_allowed(
                self._intent(risk_reducing=True), dry_run=False
            )

        self.assertFalse(allowed)
        self.assertEqual("risk-increasing-order-blocked", reason)
        self.assertTrue(reduce_allowed)
        self.assertFalse(killed)
        self.assertEqual("kill-switch-broker-dispatch-forbidden", kill_reason)

    def test_market_freshness_uses_actual_closed_bar_not_heartbeat(self) -> None:
        now = datetime(2026, 8, 1, 12, 0, 0)
        stale_end = (now - timedelta(days=10)).isoformat()
        continuous = {
            "profiles": {
                "crypto": {
                    "running": True,
                    "phase": "RUNNING",
                    "lastHeartbeat": now.isoformat(),
                    "lastDataAt": now.isoformat(),
                    "engine": {
                        "latestBars": {
                            "strategy-1": {
                                "endTime": stale_end,
                                "receivedTime": now.isoformat(),
                                "timeframe": "1h",
                            }
                        }
                    },
                }
            }
        }
        result = state.market_data_freshness_snapshot(
            continuous, now=now, stale_limit=90
        )

        self.assertTrue(result["available"])
        self.assertFalse(result["fresh"])
        self.assertEqual("closed-bar-end-time", result["source"])
        self.assertGreater(result["age_seconds"], result["allowed_age_seconds"])

    def test_live_intent_without_confirmed_market_time_fails_closed(self) -> None:
        self.assertEqual(
            121,
            state.intent_market_data_age_seconds({}, required=True),
        )
        current = datetime.now().astimezone().isoformat()
        self.assertLessEqual(
            state.intent_market_data_age_seconds(
                {"confirmed_bar_end": current}, required=True
            ),
            1,
        )

    def test_environment_change_invalidates_preflight_without_logging_values(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        state.STATE["config_revision"] = 7
        state.STATE["latest_preflight_snapshot_id"] = "preflight-old"
        before = {
            "fields": [
                {"key": "BINANCE_API_SECRET", "value": "", "configured": True},
                {"key": "LIVE_TRADER_ENABLE_REAL_ORDERS", "value": "false", "configured": True},
            ]
        }
        after = {
            "fields": [
                {"key": "BINANCE_API_SECRET", "value": "", "configured": True},
                {"key": "LIVE_TRADER_ENABLE_REAL_ORDERS", "value": "true", "configured": True},
            ]
        }
        audit = Mock()
        with patch.object(env_settings, "env_settings_snapshot", return_value=before), patch.object(
            env_settings, "save_env_settings", return_value=after
        ), patch.object(state, "append_audit", audit), patch.object(
            state, "snapshot", return_value={}
        ):
            result = state.save_environment_settings(
                {
                    "BINANCE_API_SECRET": "never-log-this-secret",
                    "LIVE_TRADER_ENABLE_REAL_ORDERS": True,
                }
            )

        self.assertEqual(8, state.STATE["config_revision"])
        self.assertEqual("", state.STATE["latest_preflight_snapshot_id"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertCountEqual(
            ["BINANCE_API_SECRET", "LIVE_TRADER_ENABLE_REAL_ORDERS"],
            result["changed_keys"],
        )
        audit_text = " ".join(str(item) for item in audit.call_args.args)
        self.assertNotIn("never-log-this-secret", audit_text)

    def test_incident_transition_limits_actions_and_redacts_operator_note(self) -> None:
        incident = SimpleNamespace(
            to_dict=lambda: {"incidentId": "incident-1", "state": "ACKNOWLEDGED"}
        )
        governance = Mock()
        governance.transition_incident.return_value = incident
        with patch.object(state, "OPERATIONAL_GOVERNANCE", governance), patch.object(
            state, "append_audit"
        ), patch.object(state, "snapshot", return_value={"governance": {}}), patch.dict(
            state.os.environ,
            {"BINANCE_API_SECRET": "actual-secret-value"},
            clear=False,
        ):
            result = state.transition_operational_incident(
                "incident-1",
                "acknowledge",
                "api_secret=actual-secret-value investigated",
            )
            rejected = state.transition_operational_incident(
                "incident-1", "reopen", "not allowed"
            )

        self.assertTrue(result["ok"])
        self.assertIn("snapshot", result)
        payload = governance.transition_incident.call_args.kwargs["payload"]
        self.assertNotIn("actual-secret-value", payload["note"])
        self.assertIn("***", payload["note"])
        self.assertFalse(rejected["ok"])
        self.assertIn("snapshot", rejected)

    def test_explicit_unknown_preflight_context_is_fail_closed(self) -> None:
        data = {
            "strategies": [
                {"deployment_id": "dep-real", "strategy_id": "strategy-real"}
            ],
            "final_preflight": [],
            "mode": "MONITOR",
        }
        diagnostic = Mock(return_value={"latest": {}})
        with patch.object(state, "snapshot", return_value=data), patch.object(
            state, "persist_doctor_diagnostic_snapshot", diagnostic
        ), patch.object(state, "append_audit"), patch.object(
            state, "ensure_operational_deployment_manifest"
        ) as ensure_manifest:
            result = state.run_final_preflight("dep-missing", "strategy-missing")

        self.assertFalse(result["ok"])
        self.assertEqual("BLOCKED", result["preflight_snapshot"]["status"])
        ensure_manifest.assert_not_called()
        recorded = diagnostic.call_args.args[0]
        self.assertEqual("Deployment 실행 컨텍스트", recorded["final_preflight"][0]["label"])

    def test_scoped_preflight_is_written_to_doctor_history(self) -> None:
        state.STATE["kill_switch_rearm_required"] = True
        strategy = {
            "deployment_id": "dep-1",
            "strategy_id": "strategy-1",
            "portfolio_gate": {},
        }
        data = {
            "strategies": [strategy],
            "brokers": [],
            "reconciliation": {"summary": {}, "positions": [], "accounts": []},
            "execution_streams": {},
            "generated_at": "2026-08-01 12:00:00",
            "mode": "MONITOR",
            "final_preflight": [{"label": "global", "status": "fail", "detail": "inventory"}],
        }
        scoped = [{"label": "현재 Deployment", "status": "pass", "detail": "ok"}]
        manifest = SimpleNamespace(deployment_id="dep-1", manifest_hash="a" * 64)
        preflight = SimpleNamespace(
            snapshot_id="preflight-1",
            to_dict=lambda: {"snapshotId": "preflight-1", "status": "PASS"},
        )
        diagnostic = Mock(return_value={"latest": {}})
        with patch.object(state, "snapshot", return_value=data), patch.object(
            state, "final_preflight_checks", return_value=scoped
        ), patch.object(
            state,
            "refresh_preflight_reconciliation",
            return_value=data["reconciliation"],
        ), patch.object(
            state, "ensure_operational_deployment_manifest", return_value=manifest
        ), patch.object(
            state.OPERATIONAL_GOVERNANCE,
            "create_preflight_snapshot",
            return_value=preflight,
        ), patch.object(
            state, "persist_doctor_diagnostic_snapshot", diagnostic
        ), patch.object(state, "append_audit"):
            result = state.run_final_preflight("dep-1", "strategy-1")

        self.assertTrue(result["ok"])
        self.assertFalse(state.STATE["kill_switch_rearm_required"])
        recorded = diagnostic.call_args.args[0]
        self.assertEqual(scoped, recorded["final_preflight"])
        self.assertEqual(0, recorded["launch_report"]["hard_stop_count"])

    def test_runtime_start_rejects_mixed_explicit_context(self) -> None:
        strategy = {
            "deployment_id": "dep-old",
            "strategy_id": "strategy-old",
            "portfolio_gate": {"portfolioId": "portfolio-old"},
        }
        with patch.object(state, "portfolio_rows", return_value=[]), patch.object(
            state, "strategy_rows", return_value=[strategy]
        ), patch.object(state, "ensure_operational_deployment_manifest") as ensure_manifest:
            with self.assertRaisesRegex(ValueError, "실행 컨텍스트"):
                state._prepare_operational_runtime_session(
                    "crypto",
                    "SMALL_LIVE",
                    "portfolio-new",
                    "dep-new",
                    "strategy-new",
                )

        ensure_manifest.assert_not_called()

    def test_monitor_start_does_not_fallback_when_context_is_explicit(self) -> None:
        with patch.object(state, "portfolio_rows", return_value=[]), patch.object(
            state, "strategy_rows", return_value=[]
        ):
            with self.assertRaisesRegex(ValueError, "요청한 Live Deployment"):
                state._prepare_operational_runtime_session(
                    "crypto",
                    "MONITOR",
                    "",
                    "dep-missing",
                    "strategy-missing",
                )

    def test_order_queue_reports_active_separately_from_history(self) -> None:
        state.STATE["orders"] = [
            {"state": "acknowledged", "queue_state": "submitted"},
            {"state": "filled", "queue_state": "filled"},
            {"state": "canceled", "queue_state": "canceled"},
        ]
        summary = state.order_queue_summary()
        self.assertEqual(3, summary["total"])
        self.assertEqual(1, summary["active"])

    def test_preflight_reconciliation_rejects_coalesced_stale_cache(self) -> None:
        stale = (datetime.now() - timedelta(minutes=5)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        state.STATE["broker_reconciliation"] = {
            "fetched_at": stale,
            "accounts": [],
            "positions": [],
            "errors": [],
            "successful_account_brokers": ["binance"],
            "successful_position_brokers": ["binance"],
        }
        state.STATE["execution_events"].update(
            {"last_poll": stale, "errors": []}
        )
        state.STATE["program_ledger"]["last_event_sync"] = stale
        state.STATE["new_entries_blocked"] = False
        cached_pass = {
            "summary": {"status": "pass", "status_label": "정상"},
            "positions": [{"broker_id": "binance", "status": "pass"}],
            "accounts": [{"broker_id": "binance", "status": "pass"}],
            "errors": [],
        }
        with patch.object(
            state,
            "poll_execution_events",
            return_value={"ok": True, "coalesced": True, "errors": []},
        ), patch.object(state, "reconciliation_snapshot", return_value=cached_pass):
            result = state.refresh_preflight_reconciliation(
                {"broker_id": "binance", "symbol": "BTCUSDT"},
                maximum_age_seconds=60,
            )

        self.assertFalse(result["summary"]["fresh"])
        self.assertFalse(result["summary"]["three_way_verified"])
        self.assertIn(
            "broker-poll-coalesced",
            result["summary"]["freshness_detail"],
        )
        self.assertTrue(state.STATE["new_entries_blocked"])

    def test_safety_tightening_keeps_revision_but_release_invalidates(self) -> None:
        state.STATE["config_revision"] = 9
        state.STATE["latest_preflight_snapshot_id"] = "preflight-current"
        state.STATE["new_entries_blocked"] = False
        state.STATE["manual_new_entries_blocked"] = False
        with patch.object(state, "append_audit"), patch.object(
            state, "snapshot", return_value={}
        ):
            tightened = state.set_flag("new_entries_blocked", True)

        self.assertTrue(tightened["ok"])
        self.assertEqual(9, state.STATE["config_revision"])
        self.assertEqual(
            "preflight-current", state.STATE["latest_preflight_snapshot_id"]
        )

        with patch.object(state, "append_audit"), patch.object(
            state, "snapshot", return_value={}
        ):
            released = state.set_flag(
                "new_entries_blocked",
                False,
                confirmed=True,
            )

        self.assertTrue(released["ok"])
        self.assertEqual(10, state.STATE["config_revision"])
        self.assertEqual("", state.STATE["latest_preflight_snapshot_id"])

    def test_manifest_hash_excludes_dynamic_account_and_transient_controls(self) -> None:
        strategy = {
            "deployment_id": "dep-hash",
            "strategy_id": "strategy-hash",
            "broker_id": "binance",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "portfolio_gate": {},
            "artifact_reference": {
                "artifactId": "artifact-hash",
                "artifactHash": "c" * 64,
                "contentHash": "d" * 64,
            },
        }
        state.STATE["risk_policy_revision"] = 4
        state.STATE["account_risk"] = {
            "fetched_at": "2026-08-01 12:00:00",
            "equity": 100,
        }
        state.STATE.update(
            {
                "dry_run": False,
                "operator_confirmed": True,
                "new_entries_blocked": False,
                "kill_switch": False,
            }
        )
        with patch.object(state, "real_orders_enabled", return_value=True):
            before = state._operational_manifest_inputs(strategy)
            state.STATE["account_risk"] = {
                "fetched_at": "2026-08-01 12:00:30",
                "equity": 87,
                "daily_pnl_pct": -2.0,
            }
            state.STATE.update(
                {
                    "dry_run": True,
                    "operator_confirmed": False,
                    "new_entries_blocked": True,
                    "kill_switch": True,
                }
            )
            after_dynamic_change = state._operational_manifest_inputs(strategy)
            state.STATE["risk_settings"]["max_open_orders"] = (
                float(state.STATE["risk_settings"]["max_open_orders"]) + 1
            )
            state.STATE["risk_policy_revision"] = 5
            after_policy_change = state._operational_manifest_inputs(strategy)

        self.assertEqual(
            before["risk_policy_hash"],
            after_dynamic_change["risk_policy_hash"],
        )
        self.assertEqual(before["config_hash"], after_dynamic_change["config_hash"])
        self.assertNotEqual(
            before["risk_policy_hash"],
            after_policy_change["risk_policy_hash"],
        )

    def test_real_intent_fresh_preflight_reaches_runtime_authorization(self) -> None:
        strategy = {
            "deployment_id": "dep-real-e2e",
            "strategy_id": "strategy-real-e2e",
            "broker_id": "binance",
            "provider": "binance",
            "asset": "CRYPTO",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "live_allowed": True,
            "live_small_eligible": True,
            "live_eligible": False,
            "lifecycle_status": "before-live-small",
            "portfolio_gate": {},
            "artifact_reference": {
                "artifactId": "artifact-real-e2e",
                "artifactHash": "a" * 64,
                "contentHash": "b" * 64,
            },
            "strategy_instance_id": "standalone:strategy-real-e2e",
            "paper_live_qualification": {
                "required": True,
                "ready": True,
                "issues": [],
                "evidenceId": "paper-e2e",
                "evidenceHash": "c" * 64,
                "evidenceBundleHash": "e" * 64,
                "bindingHash": "d" * 64,
                "paperGovernanceDeploymentId": "paper-deployment-e2e",
                "strategyArtifactId": "artifact-real-e2e",
                "strategyArtifactHash": "a" * 64,
                "strategyInstanceId": "standalone:strategy-real-e2e",
                "portfolioRequired": False,
                "portfolioArtifactId": "",
                "portfolioArtifactHash": "",
                "portfolioInstanceId": "",
            },
        }
        deployment_binding = {
            "source": "deployment-store",
            "deploymentId": "dep-real-e2e",
            "revision": 2,
            "lifecycle": "before-live-small",
            "environment": "LIVE",
            "mode": "SMALL_LIVE",
            "executionPermissionDigest": "f" * 64,
            "strategyArtifact": dict(strategy["artifact_reference"]),
            "portfolioArtifact": {
                "artifactId": "",
                "artifactHash": "",
                "contentHash": "",
            },
        }
        deployment_binding["bindingHash"] = state.governance_sha256(
            deployment_binding
        )
        brokers = [
            {
                "broker_id": "binance",
                "status": "ready",
                "live_order_adapter_ready": True,
            }
        ]
        reconciliation = {
            "summary": {"status": "pass", "status_label": "정상"},
            "positions": [{"broker_id": "binance", "status": "pass"}],
            "accounts": [{"broker_id": "binance", "status": "pass"}],
            "errors": [],
        }
        data = {
            "strategies": [strategy],
            "brokers": brokers,
            "reconciliation": reconciliation,
            "execution_streams": {},
            "generated_at": datetime.now().isoformat(),
            "mode": "MONITOR",
            "final_preflight": [],
        }
        state.STATE.update(
            {
                "mode": "SMALL_LIVE",
                "dry_run": False,
                "operator_confirmed": True,
                "new_entries_blocked": False,
                "manual_new_entries_blocked": False,
                "kill_switch": False,
                "kill_switch_rearm_required": False,
                "config_revision": 17,
                "risk_policy_revision": 5,
                "latest_preflight_snapshot_id": "",
                "active_runtime_session_ids": {},
                "orders": [],
            }
        )

        def fresh_poll(
            broker_id: str,
            *,
            force_snapshot: bool | None = None,
            include_snapshot: bool = True,
        ) -> dict[str, object]:
            self.assertEqual("binance", broker_id)
            self.assertTrue(force_snapshot)
            self.assertFalse(include_snapshot)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            state.STATE["broker_reconciliation"] = {
                "fetched_at": now,
                "accounts": [],
                "positions": [],
                "errors": [],
                "successful_account_brokers": ["binance"],
                "successful_position_brokers": ["binance"],
                "position_observations": {
                    "binance": {"observedAt": now}
                },
            }
            state.STATE["execution_events"].update(
                {"last_poll": now, "errors": []}
            )
            state.STATE["program_ledger"]["last_event_sync"] = now
            return {"ok": True, "errors": []}

        with TemporaryDirectory() as directory:
            governance = OperationalGovernanceStore(
                Path(directory) / "operational-governance.sqlite3"
            )
            router_factory = Mock(
                side_effect=AssertionError("broker router must not be called")
            )
            with patch.object(
                state, "OPERATIONAL_GOVERNANCE", governance
            ), patch.object(
                state, "snapshot", return_value=data
            ), patch.object(
                state, "portfolio_rows", return_value=[]
            ), patch.object(
                state, "strategy_rows", return_value=[strategy]
            ), patch.object(
                state,
                "_current_live_deployment_binding",
                return_value=deployment_binding,
            ), patch.object(
                state, "checklist_rows", return_value=[]
            ), patch.object(
                state, "order_queue_summary", return_value={
                    "total": 0,
                    "active": 0,
                    "blocked": 0,
                    "dry_run": 0,
                    "retryable": 0,
                    "canceled": 0,
                }
            ), patch.object(
                state, "real_orders_enabled", return_value=True
            ), patch.object(
                state, "poll_execution_events", side_effect=fresh_poll
            ), patch.object(
                state, "reconciliation_snapshot", return_value=reconciliation
            ), patch.object(
                state, "persist_doctor_diagnostic_snapshot", return_value={"latest": {}}
            ), patch.object(
                state, "append_audit"
            ), patch.object(
                state, "LiveBrokerRouter", router_factory
            ):
                preflight_result = state.run_final_preflight(
                    "dep-real-e2e",
                    "strategy-real-e2e",
                )
                self.assertTrue(preflight_result["ok"])
                self.assertEqual(
                    "PASS", preflight_result["preflight_snapshot"]["status"]
                )
                preflight_id = str(
                    preflight_result["preflight_snapshot"]["snapshotId"]
                )
                self.assertTrue(
                    governance.preflight_validity(preflight_id)["valid"]
                )

                session, _ = state._prepare_operational_runtime_session(
                    "crypto",
                    "SMALL_LIVE",
                    "",
                    "dep-real-e2e",
                    "strategy-real-e2e",
                )
                self.assertIsNotNone(session)
                state._finish_operational_runtime_start(
                    session,
                    True,
                    "runtime started",
                )
                intent = OrderIntent(
                    strategy_id="strategy-real-e2e",
                    asset="CRYPTO",
                    symbol="BTCUSDT",
                    side="BUY",
                    quantity=0.0001,
                    reference_price=60_000,
                    mode="SMALL_LIVE",
                    reason="e2e authorization only",
                    metadata={
                        "broker_id": "binance",
                        "strategy_instance_id": "standalone:strategy-real-e2e",
                        "confirmed_bar_end": datetime.now().astimezone().isoformat(),
                    },
                )
                allowed, reason, authorization = (
                    state.operational_runtime_dispatch_allowed(intent)
                )
                missing_instance_allowed, missing_instance_reason, _ = (
                    state.operational_runtime_dispatch_allowed(
                        OrderIntent(
                            **{
                                **intent.__dict__,
                                "metadata": {
                                    "broker_id": "binance",
                                    "confirmed_bar_end": intent.metadata[
                                        "confirmed_bar_end"
                                    ],
                                },
                            }
                        )
                    )
                )
                standalone_portfolio_allowed, standalone_portfolio_reason, _ = (
                    state.operational_runtime_dispatch_allowed(
                        OrderIntent(
                            **{
                                **intent.__dict__,
                                "metadata": {
                                    **intent.metadata,
                                    "portfolio_id": "portfolio-not-authorized",
                                },
                            }
                        )
                    )
                )
                state.STATE["config_revision"] = 18
                stale_allowed, stale_reason, _ = (
                    state.operational_runtime_dispatch_allowed(intent)
                )

            self.assertTrue(allowed, reason)
            self.assertTrue(authorization["allowed"])
            self.assertFalse(missing_instance_allowed)
            self.assertEqual(
                "operational-strategy-instance-context-missing",
                missing_instance_reason,
            )
            self.assertFalse(standalone_portfolio_allowed)
            self.assertEqual(
                "operational-standalone-portfolio-context-not-empty",
                standalone_portfolio_reason,
            )
            self.assertFalse(stale_allowed)
            self.assertEqual(
                "operational-config-revision-changed",
                stale_reason,
            )
            router_factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
