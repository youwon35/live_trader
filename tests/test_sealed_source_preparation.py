from __future__ import annotations
import hashlib, importlib, json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import test_paper_candidate_inbox as base
from trading_runtime.promotion_policy import build_paper_promotion_policy

service = importlib.import_module(base.PACKAGE + ".sealed_source_preparation")
preview = importlib.import_module(base.PACKAGE + ".cash_preparation_preview")

class SealedPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name); self.artifacts=self.root/"artifacts";self.artifacts.mkdir()
        self.data=self.root/"data";self.data.mkdir();blob=self.data/"bars.parquet";blob.write_bytes(b"sealed actual test bytes")
        self.snapshot={"publicationStatus":"PUBLISHED","immutable":True,"integrityStatus":"VERIFIED","objectPath":"bars.parquet","contentSha256":hashlib.sha256(blob.read_bytes()).hexdigest(),"versionId":"test-version"}
        self.strategy=base.seal_strategy_artifact({"id":"source","plugin":"moving_average_cross","symbol":"BTCUSDT","timeframe":"5m","dataArtifact":self.snapshot,"parameters":{"positionSize":5,"fee":.05,"slippage":.03}})
        self.digest=base.artifact_reference(self.strategy)["artifactHash"]
        self.instance=base.seal_strategy_instance({"instanceId":"instance","sourceStrategyId":"source","sourceArtifactHash":self.digest,"qualifiedSymbol":"BTCUSDT","qualifiedTimeframe":"5m","historicalSourceSnapshot":self.snapshot,"marketDataProvider":"binance","brokerId":"binance","parameters":self.strategy["parameters"]})
        self.instance_hash=base.assert_verified_strategy_instance(self.instance)
        base.write(self.artifacts/"source.json",self.strategy);base.write(self.artifacts/"strategy-instances"/"instance.json",self.instance)
        self.approvals=self.root/"approvals.json";base.write(self.approvals,{"records":[{"strategy_id":"source","stage":"PAPER","promotionPolicy":build_paper_promotion_policy()}]})
        self.manifest={"artifactRoot":str(self.artifacts),"dataRoot":str(self.data),"approvalPath":str(self.approvals)}
        self.target={"targetType":"INSTANCE","targetId":"instance","targetHash":self.instance_hash}

    def test_exact_current_source_is_pending_and_historical_binding_gap_explicit(self):
        before=base.file_bytes(self.root);result=service.target_snapshot(self.manifest,self.target)
        self.assertEqual(before,base.file_bytes(self.root));self.assertTrue(result["proofPending"])
        self.assertEqual("HISTORICAL_APPROVAL_SOURCE_HASH_NOT_RECORDED",result["approvalBindingStatus"])
        self.assertEqual("PAPER_EVIDENCE_PENDING",result["qualification"])
        for key in service._FALSE_FLAGS:self.assertIs(result[key],False)

    def test_source_and_instance_changes_even_resealed_are_stale(self):
        changed=base.seal_strategy_artifact({**self.strategy,"parameters":{"positionSize":4}});base.write(self.artifacts/"source.json",changed)
        with self.assertRaises(ValueError):service.target_snapshot(self.manifest,self.target)
        base.write(self.artifacts/"source.json",self.strategy)
        changed=base.seal_strategy_instance({**self.instance,"parameters":{"positionSize":4}});base.write(self.artifacts/"strategy-instances"/"instance.json",changed)
        with self.assertRaises(ValueError):service.target_snapshot(self.manifest,self.target)

    def test_revocation_and_explicit_wrong_hash_fail_closed(self):
        row=json.loads(self.approvals.read_text())["records"][0]
        for mutation in ({"stage":"SHADOW"},{"sourceArtifactHash":"0"*64}):
            base.write(self.approvals,{"records":[{**row,**mutation}]})
            with self.assertRaises(ValueError):service.target_snapshot(self.manifest,self.target)

    def test_data_content_and_path_escape_are_rejected(self):
        (self.data/"bars.parquet").write_bytes(b"changed")
        with self.assertRaises(ValueError):service.target_snapshot(self.manifest,self.target)
        with self.assertRaises(ValueError):service._data_receipt({"dataArtifact":{**self.snapshot,"objectPath":"../outside"}},self.data)

    def test_registration_rechecks_approval_record_and_policy_without_cache(self):
        recipe=self.root/"recipe.json";receipt=self.root/"receipt.json"
        base.write(recipe,{"dataRoot":str(self.data),"strategies":[{"id":"source"}],"instances":[{"id":"instance","artifactHash":self.instance_hash}],"portfolios":[]})
        base.write(receipt,{"status":"PASS","approvedSourceCount":1,"artifactRoot":str(self.artifacts)})
        registered=service.register_sources(recipe,receipt,self.approvals,self.root/"registrations");folder=Path(registered["root"])
        rows=service.list_sources(folder);self.assertEqual(1,len(rows));self.assertFalse(rows[0]["source"]["liveAuthorized"])
        row=json.loads(self.approvals.read_text())["records"][0];row["reviewNote"]="changed review record";base.write(self.approvals,{"records":[row]})
        with self.assertRaises(ValueError):service.list_sources(folder)
        preparation=importlib.import_module(base.PACKAGE+".read_only_preparation")
        self.assertEqual([],preparation.list_preparation_sources(roots=[folder]))
        with self.assertRaises(ValueError):service.source_snapshot(rows[0]["request"],folder)
        row.pop("reviewNote");row["promotionPolicy"]=build_paper_promotion_policy(profile="custom",thresholds={"minimumOrders":1});base.write(self.approvals,{"records":[row]})
        with self.assertRaises(ValueError):service.list_sources(folder)

    def test_ambiguous_sealed_identity_is_rejected(self):
        base.write(self.artifacts/"source-copy.json",self.strategy)
        with self.assertRaises(ValueError):service.target_snapshot(self.manifest,self.target)

class CashSizingTests(unittest.TestCase):
    def fixture(self):
        instrument={"sourceMaxOrderPercent":5,"allocationWeight":.2,"executionPolicy":{"feePercent":.05,"slippagePercent":.03}}
        observation={"availableCash":"100000","feeRate":"0.001","accountBinding":"SAVED_CREDENTIAL_CONFIGURATION_OBSERVED","orderability":"MARKET_AND_ACCOUNT_ENABLED","constraints":{"LOT_SIZE":{"minQty":"1","maxQty":"10000","stepSize":"1"},"PRICE_FILTER":{"minPrice":".01","maxPrice":"100000","tickSize":".01"},"NOTIONAL":{"minNotional":"10","maxNotional":"1000000"}}}
        return instrument,observation,{"side":"BUY","limitPrice":"100"}
    def test_portfolio_weight_and_cap_costs_lot_are_exact_and_never_authorize(self):
        i,o,d=self.fixture();r=preview.cash_preview(i,o,d)
        self.assertEqual(preview.Decimal("1000"),preview.Decimal(r["sourceOrderBudget"]));self.assertEqual("9",r["maximumQuantity"])
        self.assertFalse(r["executable"]);self.assertFalse(r["orderSubmissionEnabled"])
    def test_sell_does_not_derive_quantity_from_cash_even_without_holdings(self):
        i,o,d=self.fixture();d["side"]="SELL";o["availableAssetQuantity"]="0"
        r=preview.cash_preview(i,o,d)
        self.assertEqual("NOT_APPLICABLE",r["status"]);self.assertEqual("BLOCKED",r["quantityStatus"])
        self.assertIsNone(r["maximumQuantity"]);self.assertNotIn("sourceOrderBudget",r)
        self.assertIn("reduce-only",r["reasons"][0]);self.assertFalse(r["executable"])
    def test_unselected_direction_cannot_confirm_buy_quantity(self):
        for side in (None,"","HOLD"):
            i,o,d=self.fixture();d["side"]=side;r=preview.cash_preview(i,o,d)
            self.assertEqual("BLOCKED",r["status"]);self.assertIsNone(r["maximumQuantity"])
            self.assertNotIn("notionalAfterCostAllowance",r);self.assertFalse(r["orderSubmissionEnabled"])
    def test_unknown_grid_fee_or_cash_does_not_guess_quantity(self):
        for field in ("availableCash","feeRate","constraints"):
            i,o,d=self.fixture();o[field]=None;r=preview.cash_preview(i,o,d)
            self.assertEqual("BLOCKED",r["quantityStatus"]);self.assertIsNone(r["maximumQuantity"])
    def test_below_minimum_and_bad_price_grid_block(self):
        for mutation in ({"availableCash":"1"},{"constraints":{"LOT_SIZE":{"minQty":"1","stepSize":"1"},"PRICE_FILTER":{"tickSize":"3"},"MIN_NOTIONAL":{"minNotional":"10"}}}):
            i,o,d=self.fixture();o.update(mutation);r=preview.cash_preview(i,o,d);self.assertIsNone(r["maximumQuantity"])

if __name__ == "__main__":unittest.main()
