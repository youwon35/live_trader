from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from live_trader.soak_monitor import (
    LiveDaemonSoakSession,
    collect_live_soak_sample,
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


class LiveSoakMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        import os

        self.original_auto_stop = os.environ.get("LIVE_TRADER_SOAK_AUTO_STOP")

    def tearDown(self) -> None:
        import os

        if self.original_auto_stop is None:
            os.environ.pop("LIVE_TRADER_SOAK_AUTO_STOP", None)
        else:
            os.environ["LIVE_TRADER_SOAK_AUTO_STOP"] = self.original_auto_stop

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
