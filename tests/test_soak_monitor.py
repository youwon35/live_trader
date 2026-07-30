from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_trader.soak_monitor import (
    LiveDaemonSoakSession,
    collect_live_soak_sample,
    configured_transient_recovery_window_seconds,
    latest_live_soak_report,
    should_auto_stop_soak,
)


class FakeLedger:
    def execution_event_rows(self, _limit: int = 50):
        return [
            {"event_id": "e1", "state": "filled"},
            {"event_id": "e2", "state": "new"},
        ]

    def position_rows(self):
        return [{"broker_id": "binance-futures", "symbol": "BTCUSDT", "quantity": -0.01}]


class FakeAudit:
    def list_events(self, **_kwargs):
        return [
            {"event_id": "a1", "level": "INFO", "order_id": "o1"},
            {"event_id": "a2", "level": "ERROR", "order_id": ""},
        ]


class FakeState:
    PROGRAM_LEDGER = FakeLedger()
    AUDIT_STORE = FakeAudit()
    STATE = {
        "risk_settings": {"daily_loss_limit_pct": -10},
        "account_risk": {
            "budgets": {
                "binance-futures:USDT": {
                    "broker_id": "binance-futures",
                    "currency": "USDT",
                    "current_equity": 100,
                    "daily_pnl": -1,
                    "daily_pnl_pct": -1,
                }
            }
        },
        "broker_reconciliation": {
            "positions": [{"symbol": "BTCUSDT", "unrealized_profit": -2.5}]
        },
    }

    @staticmethod
    def order_rows():
        return [
            {"order_id": "o1", "state": "dry_run", "dry_run": True},
            {"order_id": "o2", "state": "risk_blocked", "dry_run": True},
        ]

    @staticmethod
    def broker_account_risk(snapshot, broker_id, *, currency=""):
        return {
            **snapshot["budgets"][f"{broker_id}:{currency}"],
            "known": True,
            "fresh": True,
        }


def status(heartbeat: str, *, bars: int = 2, decisions: int = 1):
    return {
        "phase": "RUNNING",
        "lastHeartbeat": heartbeat,
        "runtime": {
            "profiles": {
                "crypto": {
                    "reconnectCount": 1,
                    "lastDataAt": "2026-07-30T00:00:00Z",
                    "engine": {
                        "barCount": bars,
                        "decisionCount": decisions,
                        "lastCycleAt": "2026-07-30T00:00:00Z",
                    },
                }
            }
        },
        "executionStreams": {
            "brokers": {"binance": {"reconnectCount": 2, "lastError": ""}}
        },
        "lastExecutionPoll": {"ok": True},
    }


class MutableLedger(FakeLedger):
    positions = [
        {
            "broker_id": "binance-futures",
            "symbol": "BTCUSDT",
            "quantity": -0.01,
        }
    ]

    def position_rows(self):
        return [dict(row) for row in self.positions]


class MutableSafetyState(FakeState):
    STATE = deepcopy(FakeState.STATE)
    PROGRAM_LEDGER = MutableLedger()
    orders = [
        {"order_id": "o1", "state": "dry_run", "dry_run": True},
        {"order_id": "o2", "state": "risk_blocked", "dry_run": True},
    ]

    @staticmethod
    def order_rows():
        return [dict(row) for row in MutableSafetyState.orders]


def poll_status(
    heartbeat: str,
    *,
    ok: bool,
    detail: str = "TimeoutError: The read operation timed out",
    phase: str = "",
    stream_connected: bool = True,
):
    payload = status(heartbeat)
    payload["phase"] = phase or ("RUNNING" if ok else "DEGRADED")
    payload["executionStreams"]["brokers"]["kis"] = {
        "running": True,
        "connected": stream_connected,
        "lastError": "" if stream_connected else "private stream unavailable",
        "reconnectCount": 0,
    }
    payload["lastExecutionPoll"] = (
        {"ok": True, "reason": "read-only poll completed"}
        if ok
        else {
            "ok": False,
            "reason": "one broker failed",
            "errors": [{"brokerId": "kis", "detail": detail}],
        }
    )
    return payload


class LiveSoakMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        import os

        self.original_auto_stop = os.environ.get("LIVE_TRADER_SOAK_AUTO_STOP")
        self.original_recovery_window = os.environ.get(
            "LIVE_TRADER_SOAK_TRANSIENT_RECOVERY_SECONDS"
        )
        MutableSafetyState.STATE = deepcopy(FakeState.STATE)
        MutableSafetyState.STATE["broker_snapshot_poll"] = {
            "connectivity": {
                "broker_api:kis": {
                    "broker_id": "kis",
                    "status": "healthy",
                }
            }
        }
        MutableSafetyState.orders = [
            {"order_id": "o1", "state": "dry_run", "dry_run": True},
            {"order_id": "o2", "state": "risk_blocked", "dry_run": True},
        ]
        MutableSafetyState.PROGRAM_LEDGER.positions = [
            {
                "broker_id": "binance-futures",
                "symbol": "BTCUSDT",
                "quantity": -0.01,
            }
        ]

    def tearDown(self) -> None:
        import os

        if self.original_auto_stop is None:
            os.environ.pop("LIVE_TRADER_SOAK_AUTO_STOP", None)
        else:
            os.environ["LIVE_TRADER_SOAK_AUTO_STOP"] = self.original_auto_stop
        if self.original_recovery_window is None:
            os.environ.pop(
                "LIVE_TRADER_SOAK_TRANSIENT_RECOVERY_SECONDS",
                None,
            )
        else:
            os.environ[
                "LIVE_TRADER_SOAK_TRANSIENT_RECOVERY_SECONDS"
            ] = self.original_recovery_window

    def test_collector_counts_monitor_activity_without_claiming_real_orders(self) -> None:
        sample = collect_live_soak_sample(
            status("2026-07-30T00:00:00Z"),
            FakeState,
            resources={"processCpuPercent": 3, "processMemoryBytes": 42},
        )
        self.assertEqual(2, sample["barCount"])
        self.assertEqual(1, sample["decisionCount"])
        self.assertEqual(1, sample["fillCount"])
        self.assertEqual(1, sample["blockCount"])
        self.assertEqual(3, sample["reconnectCount"])
        self.assertEqual(0, sample["realOrderCount"])
        self.assertEqual(-1.0, sample["dailyPnl"])
        self.assertEqual(-1.0, sample["dailyPnlPct"])
        self.assertEqual(100.0, sample["equity"])
        self.assertEqual("durable-account-risk-budget", sample["dailyPnlSource"])
        self.assertEqual(-10.0, sample["dailyLossLimitPct"])
        self.assertTrue(sample["requiredObservationsAvailable"])
        self.assertEqual(
            {
                "auditEvents",
                "executionEvents",
                "orders",
                "programPositions",
            },
            set(sample["observationAvailability"]),
        )
        self.assertTrue(
            all(
                observation["available"] is True
                for observation in sample[
                    "observationAvailability"
                ].values()
            )
        )

    def test_each_required_reader_failure_is_redacted_and_fails_verdict(
        self,
    ) -> None:
        failure_targets = {
            "orders": ("state", "order_rows"),
            "executionEvents": ("ledger", "execution_event_rows"),
            "programPositions": ("ledger", "position_rows"),
            "auditEvents": ("audit", "list_events"),
        }
        for reader, (target_name, attribute) in failure_targets.items():
            with self.subTest(reader=reader), tempfile.TemporaryDirectory() as temp_dir:
                ledger = SimpleNamespace(
                    execution_event_rows=Mock(
                        return_value=[
                            {"event_id": "e1", "state": "filled"}
                        ]
                    ),
                    position_rows=Mock(
                        return_value=[
                            {
                                "broker_id": "binance-futures",
                                "symbol": "BTCUSDT",
                                "quantity": -0.01,
                            }
                        ]
                    ),
                )
                audit = SimpleNamespace(
                    list_events=Mock(
                        return_value=[
                            {
                                "event_id": "a1",
                                "level": "INFO",
                                "order_id": "o1",
                            }
                        ]
                    )
                )
                state_module = SimpleNamespace(
                    PROGRAM_LEDGER=ledger,
                    AUDIT_STORE=audit,
                    STATE=deepcopy(FakeState.STATE),
                    order_rows=Mock(
                        return_value=[
                            {
                                "order_id": "o1",
                                "state": "dry_run",
                                "dry_run": True,
                            }
                        ]
                    ),
                    broker_account_risk=FakeState.broker_account_risk,
                )
                target = (
                    state_module
                    if target_name == "state"
                    else ledger
                    if target_name == "ledger"
                    else audit
                )
                setattr(
                    target,
                    attribute,
                    Mock(
                        side_effect=RuntimeError(
                            "super-secret-token=must-not-be-persisted"
                        )
                    ),
                )

                sample = collect_live_soak_sample(
                    status("2026-07-30T00:00:00Z"),
                    state_module,
                )
                observation = sample["observationAvailability"][reader]
                self.assertFalse(sample["requiredObservationsAvailable"])
                self.assertFalse(observation["available"])
                self.assertEqual("reader-error", observation["errorCode"])
                self.assertEqual("RuntimeError", observation["errorType"])
                self.assertNotIn(
                    "super-secret",
                    str(sample),
                )

                session = LiveDaemonSoakSession(
                    log_dir=Path(temp_dir),
                    state_module=state_module,
                    profiles=("crypto",),
                    heartbeat_gap_limit_seconds=90,
                    target_duration_seconds=1,
                )
                session.sample(
                    status("2026-07-30T00:00:00Z")
                )
                session.reporter.started_at = datetime.now(
                    timezone.utc
                ) - timedelta(seconds=2)
                final = session.finish(
                    {
                        **status("2026-07-30T00:00:01Z"),
                        "phase": "STOPPED",
                    },
                    reason="target-duration-complete",
                )
                self.assertEqual("FAIL", final["verdict"])
                self.assertFalse(
                    final["requiredObservations"]["available"]
                )
                latest_observations = {
                    row["reader"]: row
                    for row in final["requiredObservations"]["latest"]
                }
                self.assertFalse(
                    latest_observations[reader]["available"]
                )
                self.assertEqual(
                    "RuntimeError",
                    latest_observations[reader]["errorType"],
                )
                self.assertEqual(
                    [reader],
                    [
                        row["reader"]
                        for row in final["requiredObservations"][
                            "failures"
                        ]
                    ],
                )
                required_criterion = next(
                    row
                    for row in final["criteria"]
                    if row["id"] == "required-observations"
                )
                self.assertFalse(required_criterion["passed"])
                self.assertIn(
                    f"required-observation-unavailable:{reader}",
                    final["classification"]["fatalReasons"],
                )
                self.assertNotIn(
                    "super-secret",
                    Path(final["exportPath"]).read_text(
                        encoding="utf-8"
                    ),
                )

    def test_collector_identifies_only_bounded_read_only_connectivity_errors(
        self,
    ) -> None:
        MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
            "broker_api:kis"
        ]["status"] = "down"
        transient = collect_live_soak_sample(
            poll_status("2026-07-30T00:00:30Z", ok=False),
            MutableSafetyState,
        )
        self.assertTrue(
            transient["readOnlyPoll"]["transientConnectivity"]
        )
        self.assertEqual(["kis"], transient["readOnlyPoll"]["brokers"])
        self.assertEqual(1, transient["errorBreakdown"]["readOnlyPoll"])

        authentication = collect_live_soak_sample(
            poll_status(
                "2026-07-30T00:00:30Z",
                ok=False,
                detail="HTTPError: 401 invalid API key",
            ),
            MutableSafetyState,
        )
        self.assertFalse(
            authentication["readOnlyPoll"]["transientConnectivity"]
        )

        stream_ambiguous = collect_live_soak_sample(
            poll_status(
                "2026-07-30T00:00:30Z",
                ok=False,
                stream_connected=False,
            ),
            MutableSafetyState,
        )
        self.assertFalse(
            stream_ambiguous["readOnlyPoll"]["transientConnectivity"]
        )
        self.assertTrue(
            stream_ambiguous["readOnlyPoll"]["executionStreamAmbiguous"]
        )

    def test_recovery_window_is_bounded_and_configurable(self) -> None:
        import os

        os.environ["LIVE_TRADER_SOAK_TRANSIENT_RECOVERY_SECONDS"] = "45"
        self.assertEqual(
            45.0,
            configured_transient_recovery_window_seconds(),
        )
        os.environ["LIVE_TRADER_SOAK_TRANSIENT_RECOVERY_SECONDS"] = "9999"
        self.assertEqual(
            300.0,
            configured_transient_recovery_window_seconds(),
        )

    def test_session_exports_report_under_runtime_log_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = LiveDaemonSoakSession(
                log_dir=Path(temp_dir),
                state_module=FakeState,
                profiles=("crypto",),
                heartbeat_gap_limit_seconds=90,
                target_duration_seconds=1,
            )
            running = session.sample(status("2026-07-30T00:00:00Z"))
            self.assertEqual("RUNNING", running["verdict"])
            final = session.finish(
                {**status("2026-07-30T00:00:01Z"), "phase": "STOPPED"},
                reason="test",
            )
            self.assertTrue(Path(final["exportPath"]).is_file())
            self.assertTrue(Path(final["viewPath"]).is_file())
            self.assertEqual(0, final["counts"]["realOrderCount"])
            latest = latest_live_soak_report(temp_dir)
            self.assertEqual(final["runId"], latest["runId"])

    def test_recovered_read_only_disconnect_is_pass_with_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = LiveDaemonSoakSession(
                log_dir=Path(temp_dir),
                state_module=MutableSafetyState,
                profiles=("stock", "crypto"),
                heartbeat_gap_limit_seconds=90,
                target_duration_seconds=1,
            )
            session.sample(poll_status("2026-07-30T00:00:00Z", ok=True))
            session.reporter.started_at = datetime.now(
                timezone.utc
            ) - timedelta(seconds=2)

            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "down"
            session.sample(
                poll_status("2026-07-30T00:00:30Z", ok=False)
            )
            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "healthy"
            session.sample(
                poll_status("2026-07-30T00:01:00Z", ok=True)
            )
            final = session.finish(
                {
                    **status("2026-07-30T00:01:01Z"),
                    "phase": "STOPPED",
                },
                reason="target-duration-complete",
            )

            self.assertEqual("PASS_WITH_WARNING", final["verdict"])
            self.assertEqual(1, final["counts"]["errorCount"])
            self.assertEqual(1, len(final["incidents"]))
            self.assertEqual("RECOVERED", final["incidents"][0]["status"])
            self.assertEqual(
                30.0,
                final["incidents"][0]["durationSeconds"],
            )
            self.assertEqual(
                [],
                final["incidents"][0]["integrityIssues"],
            )
            durable_report = latest_live_soak_report(temp_dir)
            self.assertEqual(
                "PASS_WITH_WARNING",
                durable_report["verdict"],
            )
            self.assertEqual(
                final["incidents"],
                durable_report["incidents"],
            )
            html_report = Path(final["viewPath"]).read_text(
                encoding="utf-8"
            )
            self.assertIn("PASS_WITH_WARNING", html_report)
            self.assertIn("복구된 읽기 전용 연결 장애", html_report)

    def test_unrecovered_or_late_disconnect_remains_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = LiveDaemonSoakSession(
                log_dir=Path(temp_dir),
                state_module=MutableSafetyState,
                profiles=("stock",),
                heartbeat_gap_limit_seconds=300,
                target_duration_seconds=1,
            )
            session.sample(poll_status("2026-07-30T00:00:00Z", ok=True))
            session.reporter.started_at = datetime.now(
                timezone.utc
            ) - timedelta(seconds=2)
            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "down"
            session.sample(
                poll_status("2026-07-30T00:00:30Z", ok=False)
            )
            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "healthy"
            session.sample(
                poll_status("2026-07-30T00:03:00Z", ok=True)
            )
            final = session.finish(
                {
                    **status("2026-07-30T00:03:01Z"),
                    "phase": "STOPPED",
                },
                reason="target-duration-complete",
            )
            self.assertEqual("FAIL", final["verdict"])
            self.assertEqual(
                "RECOVERED_UNSAFE",
                final["incidents"][0]["status"],
            )
            self.assertIn(
                "recovery-window-exceeded",
                final["incidents"][0]["integrityIssues"],
            )

    def test_order_or_position_ambiguity_during_disconnect_remains_fail(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = LiveDaemonSoakSession(
                log_dir=Path(temp_dir),
                state_module=MutableSafetyState,
                profiles=("stock",),
                heartbeat_gap_limit_seconds=90,
                target_duration_seconds=1,
            )
            session.sample(poll_status("2026-07-30T00:00:00Z", ok=True))
            session.reporter.started_at = datetime.now(
                timezone.utc
            ) - timedelta(seconds=2)
            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "down"
            session.sample(
                poll_status("2026-07-30T00:00:30Z", ok=False)
            )
            MutableSafetyState.orders.append(
                {
                    "order_id": "live-order",
                    "state": "submitted",
                    "dry_run": False,
                }
            )
            MutableSafetyState.PROGRAM_LEDGER.positions[0][
                "quantity"
            ] = -0.02
            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "healthy"
            session.sample(
                poll_status("2026-07-30T00:01:00Z", ok=True)
            )
            final = session.finish(
                {
                    **status("2026-07-30T00:01:01Z"),
                    "phase": "STOPPED",
                },
                reason="target-duration-complete",
            )
            self.assertEqual("FAIL", final["verdict"])
            self.assertIn(
                "real-order-observed-in-monitor",
                final["incidents"][0]["integrityIssues"],
            )
            self.assertIn(
                "order-ledger-changed-during-disconnect",
                final["incidents"][0]["integrityIssues"],
            )
            self.assertIn(
                "program-position-changed-during-disconnect",
                final["incidents"][0]["integrityIssues"],
            )

    def test_heartbeat_gap_or_loss_gate_cannot_be_downgraded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = LiveDaemonSoakSession(
                log_dir=Path(temp_dir),
                state_module=MutableSafetyState,
                profiles=("stock",),
                heartbeat_gap_limit_seconds=90,
                target_duration_seconds=1,
            )
            session.sample(poll_status("2026-07-30T00:00:00Z", ok=True))
            session.reporter.started_at = datetime.now(
                timezone.utc
            ) - timedelta(seconds=2)
            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "down"
            session.sample(
                poll_status("2026-07-30T00:00:30Z", ok=False)
            )
            risk = MutableSafetyState.STATE["account_risk"]["budgets"][
                "binance-futures:USDT"
            ]
            risk["daily_pnl"] = -11
            risk["daily_pnl_pct"] = -11
            MutableSafetyState.STATE["broker_snapshot_poll"]["connectivity"][
                "broker_api:kis"
            ]["status"] = "healthy"
            session.sample(
                poll_status("2026-07-30T00:02:10Z", ok=True)
            )
            final = session.finish(
                {
                    **status("2026-07-30T00:02:11Z"),
                    "phase": "STOPPED",
                },
                reason="target-duration-complete",
            )

            self.assertEqual("FAIL", final["verdict"])
            failed_criteria = {
                row["id"]
                for row in final["criteria"]
                if row["passed"] is False
            }
            self.assertIn("heartbeat", failed_criteria)
            self.assertIn("loss-gate", failed_criteria)
            self.assertIn(
                "daily-loss-gate-tripped",
                final["incidents"][0]["integrityIssues"],
            )

    def test_non_connectivity_or_failed_runtime_remains_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = LiveDaemonSoakSession(
                log_dir=Path(temp_dir),
                state_module=MutableSafetyState,
                profiles=("stock",),
                heartbeat_gap_limit_seconds=90,
                target_duration_seconds=1,
            )
            session.sample(poll_status("2026-07-30T00:00:00Z", ok=True))
            session.reporter.started_at = datetime.now(
                timezone.utc
            ) - timedelta(seconds=2)
            session.sample(
                poll_status(
                    "2026-07-30T00:00:30Z",
                    ok=False,
                    detail="ValueError: malformed position payload",
                    phase="FAILED",
                )
            )
            final = session.finish(
                {
                    **status("2026-07-30T00:00:31Z"),
                    "phase": "STOPPED",
                },
                reason="runtime-failed",
                failed=True,
            )
            self.assertEqual("FAIL", final["verdict"])
            self.assertIn(
                "FAILED",
                final["classification"]["hardRuntimePhases"],
            )
            self.assertEqual(
                1,
                final["classification"][
                    "unclassifiedPollFailureCount"
                ],
            )

    def test_latest_helper_returns_idle_without_touching_broker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            latest = latest_live_soak_report(temp_dir)
            self.assertEqual("IDLE", latest["status"])
            self.assertEqual(0, latest["counts"]["realOrderCount"])

    def test_auto_stop_is_opt_in_and_requires_target_duration(self) -> None:
        import os

        report = {"durationSeconds": 18_000, "targetDurationSeconds": 18_000}
        os.environ.pop("LIVE_TRADER_SOAK_AUTO_STOP", None)
        self.assertFalse(should_auto_stop_soak(report))
        os.environ["LIVE_TRADER_SOAK_AUTO_STOP"] = "true"
        self.assertTrue(should_auto_stop_soak(report))
        self.assertFalse(
            should_auto_stop_soak(
                {"durationSeconds": 17_999, "targetDurationSeconds": 18_000}
            )
        )

    def test_daemon_auto_stop_finishes_with_target_duration_reason(self) -> None:
        import os
        from live_trader import daemon, state

        os.environ["LIVE_TRADER_SOAK_AUTO_STOP"] = "true"
        observed: dict[str, str] = {}

        class FakeSoak:
            def __init__(self, **_kwargs):
                pass

            def sample(self, _payload):
                return {
                    "durationSeconds": 10,
                    "targetDurationSeconds": 10,
                    "exportPath": "report.json",
                }

            def finish(self, _payload, *, reason, failed=False):
                observed["reason"] = reason
                observed["failed"] = str(failed)
                return {
                    "status": "STOPPED",
                    "verdict": "PASS",
                    "stopReason": reason,
                }

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "live_trader.daemon.default_runtime_data_root",
            return_value=Path(temp_dir),
        ), patch(
            "live_trader.soak_monitor.LiveDaemonSoakSession",
            FakeSoak,
        ), patch.object(
            state, "start_execution_streams", return_value={"ok": True}
        ), patch.object(
            state, "start_continuous_runtime", return_value={"ok": True}
        ), patch.object(
            state, "stop_continuous_runtime", return_value={"ok": True}
        ), patch.object(
            state, "stop_execution_streams", return_value={"ok": True}
        ), patch.object(
            state.LIVE_CONTINUOUS_CONTROLLER,
            "snapshot",
            Mock(return_value={"running": True}),
        ), patch.object(
            state.LIVE_EXECUTION_STREAMS,
            "snapshot",
            Mock(return_value={"running": True}),
        ):
            self.assertEqual(0, daemon._run_daemon(("crypto",), "MONITOR", 0.01))
        self.assertEqual("target-duration-complete", observed["reason"])
        self.assertEqual("False", observed["failed"])


if __name__ == "__main__":
    unittest.main()
