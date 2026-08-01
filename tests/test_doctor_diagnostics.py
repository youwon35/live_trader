from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from live_trader import brokers, state
from live_trader.program_ledger import ProgramLedger
from live_trader.server import LiveTraderHandler


class DoctorDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_checklist = dict(state.STATE["checklist"])
        self.original_reconciliation = dict(state.STATE["broker_reconciliation"])
        self.original_reconciliation_last_run = state.STATE["reconciliation_last_run"]

    def tearDown(self) -> None:
        state.STATE["checklist"] = self.original_checklist
        state.STATE["broker_reconciliation"] = self.original_reconciliation
        state.STATE["reconciliation_last_run"] = self.original_reconciliation_last_run

    def test_route_lock_does_not_report_implemented_adapter_as_missing(self) -> None:
        environment = {
            "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
            "KIS_APP_KEY": "configured",
            "KIS_APP_SECRET": "configured",
            "KIS_ACCOUNT_NO": "configured",
            "KIS_ACCOUNT_PRODUCT_CODE": "01",
            "BINANCE_API_KEY": "configured",
            "BINANCE_API_SECRET": "configured",
            "UPBIT_ACCESS_KEY": "configured",
            "UPBIT_SECRET_KEY": "configured",
        }
        with patch.dict(os.environ, environment, clear=False):
            readiness = brokers.broker_readiness()
            diagnostics = brokers.broker_diagnostics()

        self.assertEqual(4, len(readiness))
        self.assertTrue(all(item.status == "disabled" for item in readiness))
        self.assertTrue(all(item.live_order_adapter_ready for item in readiness))
        self.assertTrue(all(item.order_ready is False for item in readiness))
        for item in diagnostics:
            steps = {step["key"]: step for step in item["steps"]}
            self.assertEqual("fail", steps["live_route"]["status"])
            self.assertEqual("pass", steps["adapter_code"]["status"])

    def test_machine_verifiable_checklist_items_survive_restart_semantics(self) -> None:
        state.STATE["checklist"] = {
            str(item["key"]): False
            for item in state.CHECKLIST_ITEMS
        }
        state.STATE["broker_reconciliation"] = {
            "fetched_at": "2026-07-30 12:00:00",
            "accounts": [],
            "positions": [],
            "errors": [],
            "successful_account_brokers": [
                "kis",
                "binance",
                "binance-futures",
                "upbit",
            ],
            "successful_position_brokers": [
                "kis",
                "binance",
                "binance-futures",
                "upbit",
            ],
        }
        broker = Mock()
        broker.to_dict.return_value = {"missing_env": []}
        reconciliation = {
            "summary": {
                "status": "pass",
                "blocking_count": 0,
                "last_run": "2026-07-30 12:00:00",
            }
        }
        with patch.object(state, "broker_readiness", return_value=[broker]), patch.object(
            state,
            "reconciliation_snapshot",
            return_value=reconciliation,
        ):
            rows = {
                item["key"]: item
                for item in state.checklist_rows()
            }

        self.assertTrue(rows["api_keys_reviewed"]["checked"])
        self.assertTrue(rows["api_keys_reviewed"]["automatic_checked"])
        self.assertEqual("automatic", rows["api_keys_reviewed"]["source"])
        self.assertTrue(rows["position_reconcile_reviewed"]["checked"])
        self.assertFalse(rows["risk_limits_reviewed"]["checked"])
        self.assertFalse(rows["operator_takeover_ready"]["checked"])

    def test_stale_manual_api_check_cannot_override_fresh_account_failure(self) -> None:
        state.STATE["checklist"] = {
            str(item["key"]): item["key"] == "api_keys_reviewed"
            for item in state.CHECKLIST_ITEMS
        }
        state.STATE["broker_reconciliation"] = {
            "fetched_at": "2026-07-30 12:00:00",
            "accounts": [],
            "positions": [],
            "errors": [
                {
                    "broker_id": "binance-futures",
                    "scope": "account",
                    "detail": "fresh account lookup failed",
                }
            ],
            "successful_account_brokers": [
                "kis",
                "binance",
                "upbit",
            ],
            "successful_position_brokers": [],
        }
        broker = Mock()
        broker.to_dict.return_value = {"missing_env": []}
        with patch.object(
            state,
            "broker_readiness",
            return_value=[broker],
        ):
            rows = {
                item["key"]: item
                for item in state.checklist_rows(
                    {
                        "status": "pass",
                        "blocking_count": 0,
                        "last_run": "2026-07-30 12:00:00",
                    }
                )
            }

        api_check = rows["api_keys_reviewed"]
        self.assertTrue(api_check["manual_checked"])
        self.assertFalse(api_check["automatic_checked"])
        self.assertFalse(api_check["checked"])
        self.assertEqual("failed", api_check["source"])

    def test_stale_manual_reconcile_check_cannot_override_fresh_mismatch(self) -> None:
        state.STATE["checklist"] = {
            str(item["key"]): item["key"] == "position_reconcile_reviewed"
            for item in state.CHECKLIST_ITEMS
        }
        state.STATE["broker_reconciliation"] = {
            "fetched_at": "2026-07-30 12:00:00",
            "accounts": [],
            "positions": [],
            "errors": [],
            "successful_account_brokers": [
                "kis",
                "binance",
                "binance-futures",
                "upbit",
            ],
            "successful_position_brokers": [
                "kis",
                "binance",
                "binance-futures",
                "upbit",
            ],
        }
        broker = Mock()
        broker.to_dict.return_value = {"missing_env": []}
        with patch.object(
            state,
            "broker_readiness",
            return_value=[broker],
        ):
            rows = {
                item["key"]: item
                for item in state.checklist_rows(
                    {
                        "status": "fail",
                        "blocking_count": 1,
                        "last_run": "2026-07-30 12:00:00",
                    }
                )
            }

        reconcile_check = rows["position_reconcile_reviewed"]
        self.assertTrue(reconcile_check["manual_checked"])
        self.assertFalse(reconcile_check["automatic_checked"])
        self.assertFalse(reconcile_check["checked"])
        self.assertEqual("failed", reconcile_check["source"])

    def test_manual_checklist_is_persisted_as_strict_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checklist_path = Path(tmp) / "operator-checklist.json"
            values = {
                str(item["key"]): item["key"] == "risk_limits_reviewed"
                for item in state.CHECKLIST_ITEMS
            }
            with patch.object(state, "OPERATOR_CHECKLIST_PATH", checklist_path):
                state.persist_operator_checklist_values(values)
                loaded = state.load_operator_checklist_values()

            payload = json.loads(checklist_path.read_text(encoding="utf-8"))
            self.assertEqual(1, payload["schema_version"])
            self.assertTrue(loaded["risk_limits_reviewed"])
            self.assertFalse(loaded["operator_takeover_ready"])

    def test_diagnostic_history_is_bounded_and_contains_actionable_issues(self) -> None:
        data = {
            "mode": "MONITOR",
            "dry_run": True,
            "summary": {
                "live_strategy_count": 0,
                "full_live_strategy_count": 0,
            },
            "final_preflight": [
                {
                    "label": "전략 승인",
                    "status": "fail",
                    "detail": "Live-Small 이상 0개 · Full Live 0개",
                },
                {
                    "label": "운용자 확인",
                    "status": "warn",
                    "detail": "첫 주문 전 운용자의 수동 확인이 필요합니다.",
                },
            ],
            "checklist": [],
            "risk_checks": [],
            "watchdog": {"checks": []},
            "reconciliation": {
                "summary": {
                    "status": "pass",
                    "last_run": "2026-07-30 12:00:00",
                },
                "errors": [],
            },
        }
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            state,
            "DOCTOR_DIAGNOSTICS_PATH",
            Path(tmp) / "doctor-diagnostics.json",
        ), patch.object(state, "DOCTOR_DIAGNOSTIC_HISTORY_LIMIT", 2):
            state.persist_doctor_diagnostic_snapshot(data)
            state.persist_doctor_diagnostic_snapshot(data)
            document = state.persist_doctor_diagnostic_snapshot(data)

            raw = json.dumps(document, ensure_ascii=False, allow_nan=False)

        self.assertEqual(2, document["history_count"])
        latest = document["latest"]
        self.assertEqual(1, latest["summary"]["hard_stop_count"])
        self.assertEqual(1, latest["summary"]["warning_count"])
        codes = {item["issue_code"] for item in latest["issues"]}
        self.assertIn("PREFLIGHT_STRATEGY_LIFECYCLE_INELIGIBLE", codes)
        self.assertIn("PREFLIGHT_OPERATOR_CONFIRMATION_REQUIRED", codes)
        self.assertIn("remediation", raw)
        self.assertIn("related_tab", raw)

    def test_monitor_mode_does_not_emit_fake_market_data_delay_warning(self) -> None:
        reconciliation = {"status": "pass", "status_label": "정상"}
        with patch.object(state, "live_exposure_active", return_value=False):
            checks = {
                item["label"]: item
                for item in state.risk_checks(reconciliation)
            }

        self.assertEqual("na", checks["데이터 지연"]["status"])
        self.assertEqual("해당 없음", checks["데이터 지연"]["value"])

    def test_inactive_monitor_watchdog_has_no_periodic_readiness_warnings(self) -> None:
        brokers_snapshot = [
            {
                "broker_id": "binance",
                "order_ready": False,
            }
        ]
        reconciliation = {
            "status": "pass",
            "status_label": "정상",
            "last_run": "2026-07-30 12:00:00",
        }
        queue = {
            "retryable": 0,
            "blocked": 0,
        }
        with patch.object(state, "live_exposure_active", return_value=False), patch.object(
            state,
            "active_watchdog_broker_ids",
            return_value=set(),
        ), patch.object(
            state,
            "seconds_since",
            return_value=None,
        ), patch.object(
            state,
            "execution_event_snapshot",
            return_value={"last_poll": None, "errors": []},
        ):
            report = state.watchdog_snapshot(
                brokers_snapshot,
                reconciliation,
                queue,
            )

        checks = {
            item["label"]: item
            for item in report["checks"]
        }
        for label in (
            "Watchdog heartbeat",
            "시장 데이터 신선도",
            "브로커/API 상태",
            "체결 이벤트 동기화",
        ):
            self.assertEqual("na", checks[label]["status"])
            self.assertEqual("해당 없음", checks[label]["value"])
        self.assertEqual("na", report["status"])
        self.assertEqual("비활성", report["status_label"])
        self.assertEqual(8, report["check_count"])
        self.assertEqual(4, report["pass_count"])
        self.assertEqual(4, report["not_applicable_count"])
        self.assertEqual(0, report["warning_count"])
        self.assertEqual(0, report["critical_count"])

    def test_retry_policy_matrix_never_replays_ambiguous_order_submit(self) -> None:
        with patch.dict(
            state.STATE["retry_policy"],
            {"retry_on_network_error": True, "retry_on_rate_limit": True},
        ):
            rows = {row["key"]: row for row in state.retry_policy_matrix()}

        self.assertTrue(rows["read_network_or_server_error"]["automatic_retry"])
        self.assertTrue(rows["read_rate_limited"]["automatic_retry"])
        self.assertFalse(rows["order_outcome_unknown"]["automatic_retry"])
        self.assertTrue(rows["order_outcome_unknown"]["requires_idempotency_lookup"])
        self.assertIn("POST", rows["order_outcome_unknown"]["next_action"])
        self.assertFalse(rows["order_explicit_reject"]["automatic_retry"])

    def test_diagnostics_endpoint_returns_persisted_document(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/doctor-diagnostics"
        handler.send_json = Mock()
        expected = {
            "schema_version": 1,
            "latest": {"run_id": "doctor-1"},
            "history": [],
        }
        with patch.object(
            state,
            "doctor_diagnostics_document",
            return_value=expected,
        ):
            handler.do_GET()

        handler.send_json.assert_called_once_with(
            {
                "ok": True,
                "doctor_diagnostics": expected,
            }
        )

    def test_futures_zero_cash_account_is_present_in_reconciliation(self) -> None:
        live_accounts = {
            broker_id: {
                "broker_id": broker_id,
                "broker_name": broker_name,
                "account": account,
                "currency": currency,
                "broker_cash": 0.0,
                "detail": "signed read-only snapshot",
            }
            for broker_id, broker_name, account, currency in (
                ("kis", "한국투자증권", "KIS 실계좌", "KRW"),
                ("binance", "Binance", "Binance Spot", "USDT"),
                (
                    "binance-futures",
                    "Binance USD-M Futures",
                    "Binance Futures",
                    "USDT",
                ),
                ("upbit", "Upbit", "Upbit KRW", "KRW"),
            )
        }
        ledger_accounts = {
            broker_id: {
                "cash": 0.0,
                "source": "broker_snapshot",
            }
            for broker_id in live_accounts
        }
        with patch.object(
            state,
            "live_account_rows",
            return_value=live_accounts,
        ), patch.object(
            state,
            "program_cash_rows",
            return_value=ledger_accounts,
        ), patch.object(
            state,
            "broker_reconciliation_errors",
            return_value={},
        ):
            rows = state.account_reconciliation_rows()

        self.assertEqual(4, len(rows))
        futures = next(
            item
            for item in rows
            if item["broker_id"] == "binance-futures"
        )
        self.assertEqual("0.00 USDT", futures["broker_cash"])
        self.assertEqual("pass", futures["status"])

    def test_broker_only_negative_futures_position_is_not_dropped(self) -> None:
        short_position = {
            "symbol": "ETHUSDT",
            "asset": "CRYPTO_FUTURES",
            "broker_id": "binance-futures",
            "broker_name": "Binance USD-M Futures",
            "currency": "USDT",
            "broker_qty": -0.25,
            "broker_value": 500.0,
            "average_price": 2000.0,
            "current_price": 1990.0,
            "position_side": "SHORT",
            "detail": "SHORT position",
        }
        with patch.object(
            state,
            "live_position_rows",
            return_value={
                ("binance-futures", "ETHUSDT", "SHORT"): short_position,
            },
        ), patch.object(
            state,
            "program_position_rows",
            return_value={},
        ), patch.object(
            state,
            "broker_reconciliation_errors",
            return_value={},
        ), patch.object(
            state,
            "successful_position_brokers",
            return_value={"binance-futures"},
        ):
            rows = state.positions()

        futures = next(
            item
            for item in rows
            if item["broker_id"] == "binance-futures"
            and item["symbol"] == "ETHUSDT"
        )
        self.assertEqual("-0.25", futures["broker_qty"])
        self.assertEqual("0.25", futures["delta_qty"])
        self.assertEqual("mismatch", futures["status"])

    def test_hedge_long_and_short_are_reconciled_as_distinct_ledger_rows(self) -> None:
        original_ledger = state.PROGRAM_LEDGER
        with tempfile.TemporaryDirectory() as temp_dir:
            state.PROGRAM_LEDGER = ProgramLedger(
                Path(temp_dir) / "dual-side-ledger.sqlite3"
            )
            try:
                broker_positions = [
                    {
                        "symbol": "BTCUSDT",
                        "asset": "코인 USD-M 선물",
                        "broker_id": "binance-futures",
                        "broker_name": "Binance USD-M Futures",
                        "currency": "USDT",
                        "broker_qty": 0.03,
                        "broker_value": 1950.0,
                        "position_side": "LONG",
                    },
                    {
                        "symbol": "BTCUSDT",
                        "asset": "코인 USD-M 선물",
                        "broker_id": "binance-futures",
                        "broker_name": "Binance USD-M Futures",
                        "currency": "USDT",
                        "broker_qty": -0.02,
                        "broker_value": 1300.0,
                        "position_side": "SHORT",
                    },
                ]
                state.PROGRAM_LEDGER.sync_position_rows(
                    broker_positions,
                    ["binance-futures"],
                    "test",
                )
                state.STATE["broker_reconciliation"] = {
                    "fetched_at": "2026-07-30 12:00:00",
                    "accounts": [],
                    "positions": broker_positions,
                    "errors": [],
                    "successful_position_brokers": ["binance-futures"],
                }

                reconciled = [
                    item
                    for item in state.positions()
                    if item["broker_id"] == "binance-futures"
                    and item["symbol"] == "BTCUSDT"
                ]
                truth = state.broker_position_truth_snapshot(
                    {
                        "summary": {
                            "api_required_count": 0,
                            "mismatch_count": 0,
                        }
                    }
                )
            finally:
                state.PROGRAM_LEDGER = original_ledger

        self.assertEqual(2, len(reconciled))
        self.assertEqual(
            {"LONG", "SHORT"},
            {item["position_side"] for item in reconciled},
        )
        self.assertTrue(all(item["status"] == "pass" for item in reconciled))
        self.assertTrue(truth["matched"])
        self.assertEqual(2, len(truth["lines"]))

    def test_legacy_futures_ledger_without_side_fails_closed(self) -> None:
        original_ledger = state.PROGRAM_LEDGER
        with tempfile.TemporaryDirectory() as temp_dir:
            state.PROGRAM_LEDGER = ProgramLedger(
                Path(temp_dir) / "legacy-side-ledger.sqlite3"
            )
            try:
                state.PROGRAM_LEDGER.replace_position_rows(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "asset": "코인 USD-M 선물",
                            "broker_id": "binance-futures",
                            "currency": "USDT",
                            "broker_qty": 0.03,
                        }
                    ],
                    "legacy",
                )
                state.STATE["broker_reconciliation"] = {
                    "fetched_at": "2026-07-30 12:00:00",
                    "accounts": [],
                    "positions": [
                        {
                            "symbol": "BTCUSDT",
                            "asset": "코인 USD-M 선물",
                            "broker_id": "binance-futures",
                            "broker_name": "Binance USD-M Futures",
                            "currency": "USDT",
                            "broker_qty": 0.03,
                            "position_side": "LONG",
                        }
                    ],
                    "errors": [],
                    "successful_position_brokers": ["binance-futures"],
                }
                reconciled = [
                    item
                    for item in state.positions()
                    if item["broker_id"] == "binance-futures"
                    and item["symbol"] == "BTCUSDT"
                ]
            finally:
                state.PROGRAM_LEDGER = original_ledger

        self.assertEqual(2, len(reconciled))
        self.assertTrue(all(item["status"] == "mismatch" for item in reconciled))
        legacy = next(
            item for item in reconciled if item["position_side"] == "LEGACY"
        )
        self.assertIn("position side", legacy["detail"])

    def test_dual_side_quantity_requires_explicit_leg(self) -> None:
        state.STATE["broker_reconciliation"] = {
            "fetched_at": "2026-07-30 12:00:00",
            "accounts": [],
            "positions": [
                {
                    "broker_id": "binance-futures",
                    "symbol": "BTCUSDT",
                    "broker_qty": 0.03,
                    "position_side": "LONG",
                },
                {
                    "broker_id": "binance-futures",
                    "symbol": "BTCUSDT",
                    "broker_qty": -0.02,
                    "position_side": "SHORT",
                },
            ],
            "errors": [],
            "successful_position_brokers": ["binance-futures"],
        }

        self.assertEqual(
            0.03,
            state.broker_position_quantity(
                "BTCUSDT",
                "binance-futures",
                "LONG",
            ),
        )
        self.assertEqual(
            -0.02,
            state.broker_position_quantity(
                "BTCUSDT",
                "binance-futures",
                "SHORT",
            ),
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "dual-side-position-ambiguous",
        ):
            state.broker_position_quantity(
                "BTCUSDT",
                "binance-futures",
            )

    def test_legacy_position_schema_migrates_without_losing_existing_row(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "legacy-ledger.sqlite3"
            with closing(sqlite3.connect(database_path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE positions (
                        broker_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        asset TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        value REAL NOT NULL,
                        updated_at TEXT NOT NULL,
                        source TEXT NOT NULL,
                        PRIMARY KEY (broker_id, symbol)
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO positions
                    (broker_id, symbol, asset, currency, quantity, value,
                     updated_at, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "binance-futures",
                        "BTCUSDT",
                        "코인 USD-M 선물",
                        "USDT",
                        -0.02,
                        1300.0,
                        "2026-07-30 12:00:00",
                        "legacy",
                    ),
                )
                connection.commit()

            ledger = ProgramLedger(database_path)
            rows = ledger.position_rows()
            with closing(sqlite3.connect(database_path)) as connection:
                primary_key = [
                    str(row[1])
                    for row in sorted(
                        (
                            row
                            for row in connection.execute(
                                "PRAGMA table_info(positions)"
                            ).fetchall()
                            if int(row[5] or 0) > 0
                        ),
                        key=lambda row: int(row[5]),
                    )
                ]

        self.assertEqual(
            ["broker_id", "symbol", "position_side"],
            primary_key,
        )
        self.assertEqual(1, len(rows))
        self.assertEqual("BTCUSDT", rows[0]["symbol"])
        self.assertEqual(-0.02, rows[0]["quantity"])
        self.assertEqual("", rows[0]["position_side"])


if __name__ == "__main__":
    unittest.main()
