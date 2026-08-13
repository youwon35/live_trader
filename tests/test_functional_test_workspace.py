from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from live_trader.functional_test_workspace import (
    FunctionalTestWorkspace,
    canonical_kis_domestic_symbol,
    canonical_kis_us_symbol,
    kis_account_binding_id,
)


STRATEGY_HASH = "a" * 64
PORTFOLIO_HASH = "b" * 64


def catalog() -> dict[str, object]:
    return {
        "strategies": [
            {
                "strategy_id": "kr-momentum",
                "name": "KR Momentum",
                "broker_id": "kis",
                "symbol": "069500.KS",
                "timeframe": "1d",
                "strategy_instance_id": "standalone:kr-momentum",
                "artifact_reference": {
                    "artifactId": "strategy-kr-momentum",
                    "artifactHash": STRATEGY_HASH,
                },
                "artifact_integrity": {"valid": True},
            },
            {
                "strategy_id": "btc-breakout",
                "name": "BTC Breakout",
                "broker_id": "binance",
                "symbol": "BTCUSDT",
                "artifact_reference": {
                    "artifactId": "strategy-btc-breakout",
                    "artifactHash": "c" * 64,
                },
            },
        ],
        "portfolios": [
            {
                "id": "portfolio-kis-etf",
                "name": "KIS ETF Portfolio",
                "artifact_reference": {
                    "artifactId": "portfolio-kis-etf",
                    "artifactHash": PORTFOLIO_HASH,
                },
                "artifact_integrity": {"valid": True},
                "strategy_instances": [
                    {"instanceId": "sleeve-a", "symbol": "KRX:069500"},
                    {"instanceId": "sleeve-b", "symbol": "114800.KS"},
                ],
            }
        ],
    }


def environment() -> dict[str, str]:
    return {
        "KIS_APP_KEY": "app-key",
        "KIS_APP_SECRET": "app-secret",
        "KIS_ACCOUNT_NO": "12345678",
        "KIS_ACCOUNT_PRODUCT_CODE": "01",
        "KIS_HTS_ID": "test-hts-id",
        "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
    }


def us_catalog() -> dict[str, object]:
    return {
        "strategies": [
            {
                "strategy_id": "us-f-5m-functional",
                "name": "F 5m Functional",
                "broker_id": "kis",
                "provider": "yahoo",
                "symbol": "F",
                "exchange": "NYSE",
                "timeframe": "5m",
                "strategy_instance_id": "standalone:us-f-5m-functional",
                "artifact_reference": {
                    "artifactId": "strategy-us-f-5m-functional",
                    "artifactHash": "d" * 64,
                },
                "artifact_integrity": {"valid": True},
            }
        ],
        "portfolios": [],
    }


class FunctionalTestWorkspaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.now = datetime(2026, 8, 5, 1, 0, tzinfo=timezone.utc)  # 10:00 KST
        self.workspace = FunctionalTestWorkspace(
            root=Path(self.temporary.name),
            now_provider=lambda: self.now,
            catalog_provider=catalog,
            environment_provider=environment,
        )

    def test_snapshot_exposes_only_domestic_exact_candidates_and_masked_account(self) -> None:
        snapshot = self.workspace.snapshot()

        self.assertEqual("KIS_LIVE", snapshot["environment"])
        self.assertFalse(snapshot["brokerSubmissionAllowed"])
        self.assertFalse(snapshot["promotionEligible"])
        self.assertEqual(2, len(snapshot["candidates"]))
        self.assertEqual(
            {"STRATEGY", "PORTFOLIO"},
            {item["kind"] for item in snapshot["candidates"]},
        )
        serialized = json.dumps(snapshot, ensure_ascii=False)
        self.assertNotIn("12345678", serialized)
        self.assertIn("******78-01", snapshot["account"]["label"])
        self.assertTrue(snapshot["account"]["bindingId"].startswith("kis-account:"))

    def test_create_days_permit_binds_selected_strategy_and_tiny_caps(self) -> None:
        target = next(
            item for item in self.workspace.snapshot()["candidates"]
            if item["kind"] == "STRATEGY"
        )

        result = self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 12, "durationUnit": "DAYS"}
        )

        self.assertTrue(result["ok"])
        current = result["workspace"]["current"]
        permit = current["permit"]
        self.assertEqual({"value": 12, "unit": "DAYS"}, permit["duration"])
        self.assertEqual("strategy-kr-momentum", permit["binding"]["strategyArtifactId"])
        self.assertEqual(["069500"], permit["binding"]["symbols"])
        self.assertEqual(1, permit["caps"]["maxOrderQuantity"])
        self.assertEqual(100_000.0, permit["caps"]["maxOrderNotional"])
        self.assertEqual(300_000.0, permit["caps"]["maxGrossExposure"])
        self.assertEqual(20, permit["caps"]["maxOrders"])
        self.assertEqual(3, permit["caps"]["maxOpenPositions"])
        self.assertFalse(permit["promotionEligible"])
        self.assertFalse(result["brokerSubmissionPerformed"])
        self.assertTrue(self.workspace.current_permit_path.exists())

    def test_us_f_nyse_candidate_is_visible_but_release_blocked(self) -> None:
        workspace = FunctionalTestWorkspace(
            root=Path(self.temporary.name) / "us-blocked",
            now_provider=lambda: datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc),
            catalog_provider=us_catalog,
            environment_provider=environment,
        )

        target = workspace.snapshot()["candidates"][0]

        self.assertEqual("US_STOCK", target["marketGroup"])
        self.assertEqual("KIS_US_LIVE_CONTINUOUS", target["executionRoute"])
        self.assertEqual(["NYSE"], target["exchanges"])
        self.assertEqual(50.0, target["functionalTestCaps"]["maxOrderNotional"])
        self.assertEqual(2.5, target["functionalTestCaps"]["maxLoss"])
        self.assertFalse(target["available"])
        self.assertIn(
            "functional-test-us-live-final-flat-not-released",
            target["blockers"],
        )
        self.assertFalse(
            workspace.create_permit(
                {
                    "targetKey": target["key"],
                    "durationValue": 2,
                    "durationUnit": "HOURS",
                }
            )["ok"]
        )

    def test_released_us_f_permit_is_exact_two_hours_and_uses_xnys(self) -> None:
        now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)  # 11:00 EDT
        workspace = FunctionalTestWorkspace(
            root=Path(self.temporary.name) / "us-released",
            now_provider=lambda: now,
            catalog_provider=us_catalog,
            environment_provider=environment,
        )
        with patch(
            "live_trader.functional_test_workspace.FUNCTIONAL_TEST_US_LIVE_AVAILABLE",
            True,
        ):
            target = workspace.snapshot()["candidates"][0]
            wrong_duration = workspace.create_permit(
                {
                    "targetKey": target["key"],
                    "durationValue": 3,
                    "durationUnit": "HOURS",
                }
            )
            self.assertFalse(wrong_duration["ok"])
            self.assertIn("정확히 2시간", wrong_duration["reason"])

            created = workspace.create_permit(
                {
                    "targetKey": target["key"],
                    "durationValue": 2,
                    "durationUnit": "HOURS",
                }
            )
            self.assertTrue(created["ok"])
            permit = created["workspace"]["current"]["permit"]
            self.assertEqual("KIS_LIVE", permit["environment"])
            self.assertEqual("US_STOCK", permit["binding"]["marketGroup"])
            self.assertEqual(
                "KIS_US_LIVE_CONTINUOUS",
                permit["binding"]["executionRoute"],
            )
            self.assertEqual(
                [{"symbol": "F", "exchange": "NYSE"}],
                permit["binding"]["symbolRoutes"],
            )
            self.assertEqual(50.0, permit["caps"]["maxGrossExposure"])
            self.assertEqual(2.5, permit["caps"]["maxLoss"])

            activated = workspace.activate_today(
                {"authorizedBy": "operator-us", "confirmed": True}
            )
            self.assertTrue(activated["ok"])
            # The exact two-hour permit ends before XNYS close and therefore
            # bounds the daily activation as well.
            self.assertEqual(
                "2026-08-05T17:00:00.000000Z",
                activated["workspace"]["current"]["activation"]["expiresAt"],
            )

    def test_portfolio_permit_binds_all_domestic_symbols(self) -> None:
        target = next(
            item for item in self.workspace.snapshot()["candidates"]
            if item["kind"] == "PORTFOLIO"
        )

        result = self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 24, "durationUnit": "HOURS"}
        )

        permit = result["workspace"]["current"]["permit"]
        self.assertTrue(permit["binding"]["portfolioRequired"])
        self.assertEqual("portfolio-kis-etf", permit["binding"]["portfolioArtifactId"])
        self.assertEqual(["069500", "114800"], permit["binding"]["symbols"])
        self.assertEqual("", permit["binding"]["strategyArtifactId"])

    def test_rejects_duration_longer_than_ninety_days(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]

        result = self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 91, "durationUnit": "DAYS"}
        )

        self.assertFalse(result["ok"])
        self.assertIn("functional-test-duration-too-long", result["reason"])
        self.assertFalse(self.workspace.current_permit_path.exists())

    def test_mixed_market_portfolio_is_visible_but_cannot_receive_a_kis_permit(self) -> None:
        mixed = catalog()
        mixed["portfolios"][0]["strategy_instances"].append(
            {"instanceId": "crypto-sleeve", "symbol": "BTCUSDT"}
        )
        workspace = FunctionalTestWorkspace(
            root=Path(self.temporary.name) / "mixed",
            now_provider=lambda: self.now,
            catalog_provider=lambda: mixed,
            environment_provider=environment,
        )
        target = next(
            item for item in workspace.snapshot()["candidates"]
            if item["kind"] == "PORTFOLIO"
        )

        result = workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 1, "durationUnit": "DAYS"}
        )

        self.assertFalse(target["available"])
        self.assertIn("portfolio-non-domestic-sleeve-present", target["blockers"])
        self.assertFalse(result["ok"])

    def test_daily_activation_expires_at_market_close_and_sends_no_order(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]
        self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 2, "durationUnit": "DAYS"}
        )

        result = self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )

        self.assertTrue(result["ok"])
        activation = result["workspace"]["current"]["activation"]
        # Started at 10:00 KST; the six-hour limit would be 16:00, so KRX
        # regular close at 15:30 wins.
        self.assertEqual("2026-08-05T06:30:00.000000Z", activation["expiresAt"])
        self.assertTrue(activation["dailyReauthorizationRequired"])
        self.assertFalse(activation["promotionEligible"])
        self.assertFalse(result["brokerSubmissionPerformed"])

    def test_calendar_day_plan_pauses_and_reactivates_with_a_new_daily_token(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]
        created = self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 2, "durationUnit": "DAYS"}
        )
        permit_id = created["workspace"]["current"]["permit"]["permitId"]

        day_one = self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )
        day_one_token = day_one["workspace"]["current"]["activation"]["tokenId"]
        self.assertTrue(self.workspace.begin_pause_today({"confirmed": True})["ok"])
        paused = self.workspace.complete_pause_today()["workspace"]

        self.assertEqual("PAUSED", paused["status"])
        self.assertEqual(permit_id, paused["current"]["permit"]["permitId"])
        self.assertIsNone(paused["current"]["activation"])
        self.assertTrue(self.workspace.current_permit_path.exists())

        self.now = datetime(2026, 8, 6, 1, 0, tzinfo=timezone.utc)  # next 10:00 KST
        day_two = self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )
        day_two_workspace = day_two["workspace"]
        day_two_token = day_two_workspace["current"]["activation"]["tokenId"]

        self.assertTrue(day_two["ok"])
        self.assertEqual("ACTIVE", day_two_workspace["status"])
        self.assertEqual(permit_id, day_two_workspace["current"]["permit"]["permitId"])
        self.assertNotEqual(day_one_token, day_two_token)
        self.assertEqual(2, len(list((self.workspace.root / "activations").glob("*.json"))))

    def test_daily_activation_rejects_account_binding_drift(self) -> None:
        mutable_environment = environment()
        workspace = FunctionalTestWorkspace(
            root=Path(self.temporary.name) / "account-drift",
            now_provider=lambda: self.now,
            catalog_provider=catalog,
            environment_provider=lambda: mutable_environment,
        )
        target = workspace.snapshot()["candidates"][0]
        workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 1, "durationUnit": "DAYS"}
        )
        mutable_environment["KIS_ACCOUNT_NO"] = "87654321"

        result = workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )

        self.assertFalse(result["ok"])
        self.assertIn("바인딩이 허가서 생성 후 변경", result["reason"])
        self.assertFalse(workspace.current_activation_path.exists())

    def test_stop_revokes_active_pointers_but_keeps_immutable_history(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]
        created = self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 6, "durationUnit": "HOURS"}
        )
        permit_id = created["workspace"]["current"]["permit"]["permitId"]
        self.workspace.activate_today({"authorizedBy": "operator-a", "confirmed": True})

        result = self.workspace.stop({"confirmed": True})

        self.assertTrue(result["ok"])
        self.assertEqual("STOPPED", result["workspace"]["status"])
        self.assertFalse(self.workspace.current_permit_path.exists())
        self.assertFalse(self.workspace.current_activation_path.exists())
        self.assertTrue((self.workspace.root / "permits" / f"{permit_id}.json").exists())

    def test_stop_failed_preserves_pointers_and_exposes_recovery_state(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]
        self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 6, "durationUnit": "HOURS"}
        )
        self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )

        result = self.workspace.record_stop_failed("broker reconciliation pending")

        self.assertFalse(result["ok"])
        self.assertEqual("STOP_FAILED", result["workspace"]["status"])
        self.assertTrue(self.workspace.current_permit_path.exists())
        self.assertTrue(self.workspace.current_activation_path.exists())
        self.assertIn(
            "functional-test-stop-failed",
            result["workspace"]["current"]["blockers"],
        )

    def test_authority_scope_recovers_an_expired_permit_reference(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]
        created = self.workspace.create_permit(
            {
                "targetKey": target["key"],
                "durationValue": 1,
                "durationUnit": "HOURS",
            }
        )
        permit_id = created["workspace"]["current"]["permit"]["permitId"]
        self.now = datetime(2026, 8, 5, 3, 0, tzinfo=timezone.utc)

        snapshot = self.workspace.snapshot()
        scope = self.workspace.authority_scope()

        self.assertIsNone(snapshot["current"]["permit"])
        self.assertTrue(snapshot["current"]["authorityReferencePresent"])
        self.assertTrue(scope["present"])
        self.assertTrue(scope["resolved"])
        self.assertEqual(permit_id, scope["permitId"])

    def test_authority_scope_fails_closed_for_corrupt_pointer_and_history(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]
        created = self.workspace.create_permit(
            {
                "targetKey": target["key"],
                "durationValue": 1,
                "durationUnit": "DAYS",
            }
        )
        permit_id = created["workspace"]["current"]["permit"]["permitId"]
        self.workspace.current_permit_path.write_text("{broken", encoding="utf-8")
        (self.workspace.root / "permits" / f"{permit_id}.json").write_text(
            "{also-broken",
            encoding="utf-8",
        )

        scope = self.workspace.authority_scope()

        self.assertTrue(scope["present"])
        self.assertFalse(scope["resolved"])
        self.assertIn("current-permit-pointer-invalid", scope["reason"])
        self.assertIn("immutable-permit-history-invalid", scope["reason"])

    def test_confirmation_and_market_boundaries_fail_closed(self) -> None:
        target = self.workspace.snapshot()["candidates"][0]
        self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 6, "durationUnit": "HOURS"}
        )
        missing_confirmation = self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": False}
        )
        self.assertFalse(missing_confirmation["ok"])

        self.now = datetime(2026, 8, 5, 7, 0, tzinfo=timezone.utc)  # 16:00 KST
        after_close = self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )
        self.assertFalse(after_close["ok"])
        self.assertIn("정규장이 종료", after_close["reason"])

    def test_daily_activation_uses_xkrx_holiday_calendar(self) -> None:
        self.now = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
        target = self.workspace.snapshot()["candidates"][0]
        self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 1, "durationUnit": "DAYS"}
        )

        result = self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )

        self.assertFalse(result["ok"])
        self.assertIn("XKRX 공식 캘린더", result["reason"])
        self.assertFalse(self.workspace.current_activation_path.exists())

    def test_daily_activation_fails_closed_outside_calendar_coverage(self) -> None:
        self.now = datetime(2040, 8, 1, 1, 0, tzinfo=timezone.utc)
        target = self.workspace.snapshot()["candidates"][0]
        self.workspace.create_permit(
            {"targetKey": target["key"], "durationValue": 1, "durationUnit": "DAYS"}
        )

        result = self.workspace.activate_today(
            {"authorizedBy": "operator-a", "confirmed": True}
        )

        self.assertFalse(result["ok"])
        self.assertIn("XKRX 공식 캘린더", result["reason"])

    def test_symbol_and_account_identity_helpers_are_canonical(self) -> None:
        self.assertEqual("069500", canonical_kis_domestic_symbol("KRX:069500"))
        self.assertEqual("069500", canonical_kis_domestic_symbol("069500.KS"))
        self.assertEqual("", canonical_kis_domestic_symbol("BTCUSDT"))
        self.assertEqual("F", canonical_kis_us_symbol("NYSE:F"))
        self.assertEqual("", canonical_kis_us_symbol("BTC-USDT"))
        self.assertEqual(
            kis_account_binding_id("1234-5678", "01"),
            kis_account_binding_id("12345678", "01"),
        )


if __name__ == "__main__":
    unittest.main()
