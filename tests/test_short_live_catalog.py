"""Synthetic canonical files only: no real artifacts, account, DB or broker I/O."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from trading_runtime.artifact_governance import (
    compute_strategy_artifact_hash, compute_strategy_instance_hash,
    seal_strategy_artifact, seal_strategy_instance, seal_portfolio_artifact,
)
from live_trader.short_live_catalog import ShortLiveCatalogue


ROUTES = {
    "KIS_KR_CASH": ("kis", "kis", "005930", "KR_STOCK"),
    "BINANCE_SPOT": ("binance", "binance", "BTCUSDT", "CRYPTO"),
    "BINANCE_USDM_FUTURES": ("binance-futures", "binance-futures", "BTCUSDT.PERP", "CRYPTO"),
    "UPBIT_KRW_SPOT": ("upbit", "upbit", "KRW-BTC", "CRYPTO"),
}


class ShortLiveCatalogueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "strategy-instances").mkdir()
        (self.root / "portfolios").mkdir()
        self.catalog = ShortLiveCatalogue(self.root)

    def write(self, path, value, **options):
        path.write_text(json.dumps(value, ensure_ascii=False, **options), encoding="utf-8")
        return path

    def pair(self, *, route="BINANCE_SPOT", timeframe="5m", suffix="one", source_changes=None, instance_changes=None):
        broker, provider, symbol, asset = ROUTES[route]
        params = {"shortMa": 1, "longMa": 3, "executionTiming": "next-open"}
        source = {"id": "source-" + suffix, "strategy_id": "source-" + suffix,
                  "artifact_schema_version": "strategy-artifact-v2", "sharedSchemaVersion": "market-strategy-v1",
                  "asset": asset, "symbol": symbol, "timeframe": timeframe,
                  "plugin": "moving_average_cross", "brokerId": broker,
                  "marketDataProvider": provider, "parameters": deepcopy(params),
                  "finalTest": {"status": "PASS"}, "executionPurpose": "FUNCTIONAL_TEST",
                  "promotionEligible": False, "useAsPromotionEvidence": False}
        source.update(source_changes or {})
        source = seal_strategy_artifact(source)
        instance = {"schemaVersion": "strategy-instance-template-v1", "instanceId": "instance-" + suffix,
                    "sourceStrategyId": source["id"], "sourceArtifactHash": compute_strategy_artifact_hash(source),
                    "qualifiedSymbol": symbol, "qualifiedTimeframe": timeframe,
                    "brokerId": broker, "marketDataProvider": provider, "asset": asset,
                    "pluginId": "moving_average_cross", "parameters": deepcopy(params)}
        instance.update(instance_changes or {})
        instance = seal_strategy_instance(instance)
        sp = self.write(self.root / (source["id"] + ".json"), source)
        ip = self.write(self.root / "strategy-instances" / (instance["instanceId"] + ".json"), instance)
        return source, instance, sp, ip

    def portfolio(self, source, instance, *, suffix="one", mutate=None):
        member = {**deepcopy(instance), "templateInstanceId": instance["instanceId"],
                  "strategyId": source["id"], "sourceInstanceHash": compute_strategy_instance_hash(instance),
                  "allocation": {"normalizedWeight": 1.0}}
        member.pop("instanceLock", None)
        if mutate:
            mutate(member)
        value = seal_portfolio_artifact({"id": "portfolio-" + suffix,
                  "schemaVersion": "portfolio-artifact-v1", "strategyInstances": [member],
                  "portfolio": {"strategyMode": "multi_strategy"},
                  "promotionEligible": False, "useAsPromotionEvidence": False})
        return self.write(self.root / "portfolios" / (value["id"] + ".json"), value)

    def row(self, kind="STRATEGY_INSTANCE"):
        return next(row for row in self.catalog.snapshot()["candidates"] if row["kind"] == kind)

    def selection(self, row):
        return {"selectionKey": row["selectionKey"], "artifactFileSha256": row["artifactFileSha256"],
                "instanceFileSha256": row["instanceFileSha256"]}

    def test_each_route_and_timeframe_retains_exact_binding_and_no_promotion(self):
        for route in ROUTES:
            for timeframe in ("5m", "1h", "1d"):
                self.pair(route=route, timeframe=timeframe, suffix=f"{route}-{timeframe}")
        snapshot = self.catalog.snapshot()
        self.assertEqual(24, len(snapshot["coverage"]))
        self.assertEqual(12, len(snapshot["candidates"]))
        for row in snapshot["candidates"]:
            self.assertTrue(row["eligibleForFunctionalPreparation"], row["blockers"])
            self.assertFalse(row["promotionEligible"])
            resolved = self.catalog.resolve([self.selection(row)])
            self.assertEqual(row["route"], resolved["specs"][0]["route"])
            self.assertEqual(row["timeframe"], resolved["specs"][0]["timeframe"])
            self.assertEqual(2, len(resolved["sourcePins"]))

    def test_backtest_absence_is_not_invented_as_pass(self):
        self.pair(source_changes={"finalTest": {"status": "NOT_OBSERVED"}})
        row = self.row()
        self.assertFalse(row["eligibleForFunctionalPreparation"])
        self.assertIn("functional-backtest-review-required", row["blockers"])

    def test_canonical_instance_raw_tamper_blocks_without_resealing(self):
        _, instance, _, ip = self.pair()
        instance["parameters"]["longMa"] = 99
        self.write(ip, instance)
        self.assertFalse(self.row()["eligibleForFunctionalPreparation"])

    def test_wrong_source_hash_remains_blocked_even_with_new_instance_seal(self):
        _, instance, _, ip = self.pair()
        instance["sourceArtifactHash"] = "e" * 64
        self.write(ip, seal_strategy_instance(instance))
        self.assertIn("source-strategy-link-hash-mismatch", self.row()["blockers"])

    def test_duplicate_instance_id_is_not_selected_by_latest_file(self):
        _, instance, _, _ = self.pair()
        self.write(self.root / "strategy-instances" / "duplicate.json", instance)
        rows = self.catalog.snapshot()["candidates"]
        self.assertEqual(2, len(rows))
        self.assertTrue(all("duplicate-exact-artifact-id" in row["blockers"] for row in rows))

    def test_duplicate_source_id_is_ambiguous(self):
        source, _, _, _ = self.pair()
        self.write(self.root / "duplicate-source.json", source)
        self.assertIn("source-strategy-missing-or-ambiguous", self.row()["blockers"])

    def test_selection_requires_raw_pin_and_same_raw_file(self):
        _, instance, _, ip = self.pair()
        row = self.row()
        with self.assertRaises(ValueError):
            self.catalog.resolve([{"selectionKey": row["selectionKey"]}])
        self.write(ip, instance, indent=3)
        with self.assertRaises(ValueError):
            self.catalog.resolve([self.selection(row)])

    def test_duplicate_selection_and_double_parent_cannot_duplicate_instance(self):
        source, instance, _, _ = self.pair()
        row = self.row()
        with self.assertRaises(ValueError):
            self.catalog.resolve([self.selection(row), self.selection(row)])
        self.portfolio(source, instance)
        portfolio = self.row("PORTFOLIO_INSTANCE")
        self.assertTrue(portfolio["eligibleForFunctionalPreparation"], portfolio["blockers"])
        with self.assertRaisesRegex(ValueError, "same-strategy-instance"):
            self.catalog.resolve([self.selection(row), self.selection(portfolio)])

    def test_portfolio_missing_exact_child_instance_hash_blocks(self):
        source, instance, _, _ = self.pair()
        self.portfolio(source, instance, mutate=lambda row: row.pop("sourceInstanceHash"))
        self.assertFalse(self.row("PORTFOLIO_INSTANCE")["eligibleForFunctionalPreparation"])

    def test_source_timeframe_mismatch_cannot_relabel_original_strategy(self):
        self.pair(timeframe="5m", instance_changes={"qualifiedTimeframe": "1h"})
        self.assertFalse(self.row()["eligibleForFunctionalPreparation"], "5m original was silently relabeled to 1h")

    def test_crypto_provider_mismatch_cannot_use_other_exchange_candles(self):
        self.pair(route="BINANCE_SPOT", instance_changes={"marketDataProvider": "upbit"})
        self.assertFalse(self.row()["eligibleForFunctionalPreparation"], "Binance order route was bound to Upbit data")

    def test_original_timing_missing_or_conflicting_is_not_defaulted(self):
        for params, source_changes in [
            ({"shortMa": 1, "longMa": 3}, {"parameters": {"shortMa": 1, "longMa": 3}}),
            ({"shortMa": 1, "longMa": 3, "executionTiming": "next-open"},
             {"executionTiming": "signal-close"}),
        ]:
            with self.subTest(params=params):
                for path in self.root.rglob("*.json"):
                    path.unlink()
                self.pair(source_changes=source_changes, instance_changes={"parameters": params})
                self.assertFalse(self.row()["eligibleForFunctionalPreparation"])

    def test_catalog_snapshot_is_read_only(self):
        self.pair()
        def tree():
            return {str(path.relative_to(self.root)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in self.root.rglob("*") if path.is_file()}
        before = tree()
        self.catalog.snapshot(); self.catalog.resolve([self.selection(self.row())])
        self.assertEqual(before, tree())

    def functional_pair(self, *, parity_changes=None):
        report={"schemaVersion":"short-functional-actual-backtest-paper-parity-v1","promotionEligible":False,
            "parity":{"status":"PASS","meaning":"SHORT_FUNCTIONAL_HISTORY_PARITY_ONLY_V1","finiteEquity":True,"compared":1,"matches":1,
                      "comparisons":[{"match":True,"paper":"HOLD","backtester":"HOLD"}]},
            "settings":{"interval":"5m"},"dataset":{"sha256":"c"*64}}
        report["parity"].update(parity_changes or {})
        report_path=self.write(self.root / "fixture-only-report.json",report)
        ref={"path":str(report_path),"sha256":hashlib.sha256(report_path.read_bytes()).hexdigest()}
        self.pair(source_changes={"executionPurpose":"FUNCTIONAL_TEST_ONLY",
            "evidenceClass":"SHORT_FUNCTIONAL_HISTORY_PARITY_ONLY_V1",
            "dataArtifact":{"sha256":"c"*64},"finalTest":{"status":"PASS","meaning":"SHORT_FUNCTIONAL_HISTORY_PARITY_ONLY_V1","report":ref}})
        return report_path,report

    def test_functional_report_actual_raw_pin_is_bound_and_reformat_invalidates(self):
        path,report=self.functional_pair(); row=self.row()
        self.assertTrue(row["eligibleForFunctionalPreparation"],row["blockers"])
        pins=self.catalog.resolve([self.selection(row)])["sourcePins"]
        self.assertTrue(any(pin["path"]==str(path) for pin in pins))
        self.write(path,report,indent=2)
        self.assertFalse(self.row()["eligibleForFunctionalPreparation"])

    def test_functional_report_missing_observation_or_false_parity_is_not_promoted(self):
        for change in ({"compared":0,"matches":0,"comparisons":[]}, {"finiteEquity":False},
                       {"comparisons":[{"match":False,"paper":"BUY","backtester":"HOLD"}]}):
            with self.subTest(change=change):
                for path in self.root.rglob("*.json"): path.unlink()
                self.functional_pair(parity_changes=change)
                self.assertFalse(self.row()["eligibleForFunctionalPreparation"])


    def test_source_size_display_retains_five_percent_independent_of_allocation(self):
        params = {"shortMa": 1, "longMa": 3, "executionTiming": "next-open", "positionSize": 5}
        source, instance, _, _ = self.pair(source_changes={"parameters": params}, instance_changes={"parameters": params})
        self.portfolio(source, instance, mutate=lambda member: member.update(allocation={"normalizedWeight": 0.5}))
        for row in self.catalog.snapshot()["candidates"]:
            self.assertEqual([5.0], row["positionSizePercents"])
            self.assertFalse(row["sourcePositionSizeWasDefaulted"])

    def test_explicit_bounded_profile_size_is_visible_without_ordinary_promotion(self):
        params = {"shortMa": 1, "longMa": 3, "executionTiming": "next-open", "positionSize": 100}
        self.pair(source_changes={"parameters": params, "functionalSizingProfile": "BOUNDED_TEST_SLEEVE_100_PERCENT_V1"},
                  instance_changes={"parameters": params})
        row = self.row()
        self.assertEqual([100.0], row["positionSizePercents"])
        self.assertEqual(["BOUNDED_TEST_SLEEVE_100_PERCENT_V1"], row["functionalSizingProfiles"])
        self.assertEqual(0, self.catalog.snapshot()["coverage"][0]["actualLivePassCount"])


if __name__ == "__main__":
    unittest.main()
