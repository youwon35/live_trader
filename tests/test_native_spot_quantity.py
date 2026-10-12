from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from fractions import Fraction
import importlib, json, socket, sys, types, unittest
from pathlib import Path
from unittest.mock import Mock, patch

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP.parents[1]/"packages/trading_runtime"))
PACKAGE = "_native_spot_quantity_isolated"
package = types.ModuleType(PACKAGE); package.__path__ = [str(APP/"live_trader")]; sys.modules[PACKAGE] = package
service = importlib.import_module(PACKAGE+".native_spot_quantity")
preview = importlib.import_module(PACKAGE+".cash_preparation_preview")
from trading_runtime.execution_quantity import seal_quantity_contract
NOW = datetime(2026,10,12,8,tzinfo=timezone.utc)


def contract(**changes):
    return seal_quantity_contract({"schemaVersion":"native-spot-quantity-v1", "provider":"binance", "symbol":"ETHUSDT", "baseCurrency":"ETH", "quoteCurrency":"USDT", "orderType":"MARKET", "stepSize":"0.001", "minQuantity":"0.001", "maxQuantity":"1000", "minNotional":"5", "maxNotional":"0", "notionalReferenceMinutes":0, "capabilityStatus":"VERIFIED", "provenance":{"references":["isolated fixture"]}, **changes})


def sizing(**changes):
    return {"mode":"capital_percentage", "fractionalAllowed":True, "capitalBasis":service.CAPITAL_BASIS, "quantityContract":contract(**changes)}


def spec(sizing_value=None):
    return types.SimpleNamespace(broker_id="binance",provider="binance",symbol="ETHUSDT",strategy_id="source",strategy_instance_id="member",portfolio_hash="portfolio",artifact_hash="sourceHash",target_weight=1,parameters={"positionSize":5,"fee":0.1,"slippage":0.2},artifact={"standalone":True,"executionSizing":sizing_value or sizing()})


def context(sizing_value=None):
    return {"deploymentId":"deployment", "sourceArtifactHash":"sourceHash", "strategyInstanceId":"member", "portfolioHash":"portfolio", "quoteCurrency":"USDT", "equityScope":"OWNED_DEPLOYMENT_ONLY", "accountKnown":True,"accountFresh":True, "asOf":NOW.isoformat(), "deploymentEquity":"1000", "availableCash":"700", "actualFeeRate":"0.001", "currentQuantityContract":(sizing_value or sizing())["quantityContract"]}


def model(sizing_value=None,**changes):
    return service.calculate_model_quantity(sizing_value or sizing(), side=changes.pop("side","BUY"),price=changes.pop("price","100"),deployment_equity=changes.pop("deployment_equity","1000"),available_cash=changes.pop("available_cash","700"),allocation_weight=changes.pop("allocation_weight","1"),position_percent=changes.pop("position_percent","5"),source_fee_rate=changes.pop("source_fee_rate","0.001"),source_slippage_rate=changes.pop("source_slippage_rate","0.002"),**changes)


class NativeSpotQuantityTests(unittest.TestCase):
    def setUp(self):
        self.net=patch.object(socket,"socket",side_effect=AssertionError("network forbidden"));self.net.start();self.addCleanup(self.net.stop)

    def test_fraction_oracle_buy_cost_floor_and_cash_affordability(self):
        # Independent rational-number arithmetic, without shared Decimal helper.
        expected=Fraction(50)/(Fraction(100)*Fraction(1002,1000)*Fraction(1001,1000)); units=expected//Fraction(1,1000);quantity=Fraction(units,1000)
        result=model();self.assertEqual(Decimal(quantity.numerator)/quantity.denominator,Decimal(result["quantity"]));self.assertLessEqual(Decimal(result["totalSpend"]),Decimal(50));self.assertEqual("READY",result["status"])

    def test_equity_basis_is_not_recomputed_from_remaining_cash(self):
        self.assertEqual(model(available_cash="700")["quantity"],model(available_cash="100")["quantity"])
        capped=model(available_cash="10");self.assertLessEqual(Decimal(capped["totalSpend"]),10);self.assertLess(Decimal(capped["quantity"]),Decimal(model()["quantity"]))

    def test_portfolio_normalized_weight_applies_source_position_percent_once(self):
        result=model(allocation_weight="0.25");self.assertLessEqual(Decimal(result["totalSpend"]),Decimal("12.5"));self.assertEqual("0.124",result["quantity"])
        result=model(allocation_weight="0.5");self.assertEqual("0.249",result["quantity"])

    def test_integer_step_and_minimum_notional_do_not_round_up(self):
        result=model(sizing(stepSize="1",minQuantity="1"),price="31",source_fee_rate=0,source_slippage_rate=0);self.assertEqual("1",result["quantity"])
        result=model(sizing(minNotional="51"),source_fee_rate=0,source_slippage_rate=0);self.assertEqual(("BLOCKED","0","BELOW_MIN_NOTIONAL"),(result["status"],result["quantity"],result["reason"]))

    def test_fee_difference_is_conservative_and_explicit(self):
        base=model(actual_fee_rate="0.001");cost=model(actual_fee_rate="0.005");self.assertLess(Decimal(cost["quantity"]),Decimal(base["quantity"]));self.assertTrue(cost["feeAssumptionAdjusted"]);self.assertEqual("0.005",cost["feeRateUsed"])

    def test_unknown_capability_or_average_reference_stays_non_native(self):
        for value in (sizing(capabilityStatus="UNKNOWN"),sizing(notionalReferenceMinutes=5)):
            result=model(value);self.assertEqual("READY",result["status"]);self.assertEqual("UNKNOWN",result["nativeEligibility"])
            result=service.live_quantity(spec(value),side="BUY",price=100,allocation_weight=1,identity={"deploymentId":"deployment"},context=context(value),now=NOW);self.assertEqual("BLOCKED",result["status"]);self.assertIsNone(result["quantityForSubmission"])
        result=model(sizing(notionalReferenceMinutes=5),notional_reference_price="100");self.assertEqual("UNKNOWN",result["nativeEligibility"])

    def test_invalid_numeric_inputs_rejected_before_quantity(self):
        for value in (True,"NaN","Infinity","-1",None):
            for field in ("deployment_equity","available_cash","allocation_weight","position_percent","source_fee_rate","source_slippage_rate"):
                with self.subTest(value=value,field=field),self.assertRaises(ValueError):model(**{field:value})
        with self.assertRaises(ValueError):model(allocation_weight="1.1")
        with self.assertRaises(ValueError):model(position_percent="101")

    def test_sell_quantity_uses_held_asset_and_reports_exact_residual(self):
        result=model(side="SELL",position_quantity="0.1234");self.assertEqual("0.123",result["quantity"]);self.assertEqual("0.0004",result["residualQuantity"])
        result=model(side="SELL",position_quantity="0.004");self.assertEqual("BLOCKED",result["status"]);self.assertEqual("0.004",result["residualQuantity"])
        result=model(side="SELL",position_quantity="0.05");self.assertEqual("BLOCKED",result["status"]);self.assertEqual("BELOW_MIN_NOTIONAL",result["reason"])

    def test_exact_scope_and_fresh_owned_equity_required_no_cash_substitution(self):
        current=context();result=service.live_quantity(spec(),side="BUY",price=100,allocation_weight=1,identity={"deploymentId":"deployment"},context=current,now=NOW);self.assertEqual("READY",result["status"]);self.assertEqual(0.498,result["quantityForSubmission"]);self.assertFalse(result["orderAuthorityGranted"])
        for field,value in (("deploymentId","other"),("sourceArtifactHash","changed"),("strategyInstanceId","other"),("portfolioHash","other"),("equityScope","WHOLE_ACCOUNT"),("accountKnown",False),("accountFresh",False),("asOf",(NOW-timedelta(seconds=121)).isoformat()),("asOf",(NOW+timedelta(seconds=1)).isoformat()),("deploymentEquity",None),("actualFeeRate",None)):
            changed={**current,field:value};result=service.live_quantity(spec(),side="BUY",price=100,allocation_weight=1,identity={"deploymentId":"deployment"},context=changed,now=NOW);self.assertEqual("BLOCKED",result["status"],(field,result));self.assertIsNone(result["quantityForSubmission"])
        result=service.live_quantity(spec(),side="BUY",price=100,allocation_weight=1,identity={"deploymentId":"deployment"},now=NOW);self.assertIn("OWNED_DEPLOYMENT",result["reasons"][0])
        result=service.live_quantity(spec(),side="BUY",price=100,allocation_weight=1,identity={"deploymentId":""},context={**current,"deploymentId":""},now=NOW);self.assertEqual("BLOCKED",result["status"]);self.assertIn("DEPLOYMENT_BINDING",result["reasons"][0])

    def test_source_and_current_market_rules_mismatch_blocks(self):
        current=context();current["currentQuantityContract"]=contract(stepSize="0.01");result=service.live_quantity(spec(),side="BUY",price=100,allocation_weight=1,identity={"deploymentId":"deployment"},context=current,now=NOW);self.assertEqual("BLOCKED",result["status"]);self.assertIn("CURRENT_RULES",result["reasons"][0])
        changed=spec();changed.symbol="BTCUSDT";result=service.live_quantity(changed,side="BUY",price=100,allocation_weight=1,identity={"deploymentId":"deployment"},context=context(),now=NOW);self.assertIn("SOURCE_ROUTE",result["reasons"][0])

    def test_preview_uses_same_model_but_unknown_or_missing_equity_blocks(self):
        instrument={"executionSizing":sizing(),"sourceMaxOrderPercent":5,"allocationWeight":0.25,"executionPolicy":{"feePercent":0.1,"slippagePercent":0.2}}
        observation={"availableCash":"700","deploymentEquity":"1000","equityScope":"OWNED_DEPLOYMENT_ONLY","feeRate":"0.001","currentQuantityContract":contract(),"accountBinding":"SAVED_CREDENTIAL_CONFIGURATION_OBSERVED","orderability":"MARKET_AND_ACCOUNT_ENABLED"};draft={"side":"BUY","limitPrice":"100"}
        result=preview.cash_preview(instrument,observation,draft);self.assertEqual(model(allocation_weight="0.25",actual_fee_rate="0.001")["quantity"],result["maximumQuantity"]);self.assertFalse(result["executable"])
        result=preview.cash_preview(instrument,{**observation,"deploymentEquity":None},draft);self.assertEqual("BLOCKED",result["status"]);self.assertIsNone(result["maximumQuantity"])
        instrument["executionSizing"]=sizing(notionalReferenceMinutes=5);observation["currentQuantityContract"]=instrument["executionSizing"]["quantityContract"];result=preview.cash_preview(instrument,observation,draft);self.assertIsNone(result["maximumQuantity"]);self.assertEqual("UNKNOWN",result["nativeEligibility"])
        for side in ("SELL",""):
            result=preview.cash_preview(instrument,observation,{**draft,"side":side});self.assertIsNone(result["maximumQuantity"])

    def test_continuous_consumer_blocks_before_intent_without_owned_capital(self):
        fake_state=types.ModuleType(PACKAGE+".state")
        fake_state.STATE={"strategy_runner":{},"native_spot_quantity_contexts":{}}
        fake_state.broker_position_quantity=Mock(return_value=0)
        fake_state.append_audit=Mock();fake_state.account_risk_for_intent=Mock(side_effect=AssertionError("account access should not occur without exact context"))
        with patch.dict(sys.modules,{PACKAGE+".state":fake_state}),patch.object(package,"state",fake_state,create=True):
            controller_module=importlib.import_module(PACKAGE+".continuous_live")
            controller=controller_module.LiveContinuousController(APP)
            controller.mode="SMALL_LIVE";controller.deployment_id="deployment"
            selected=spec();controller.supervisor=types.SimpleNamespace(engine=types.SimpleNamespace(specs=(selected,)))
            controller._decision_context_blocker=Mock(return_value="")
            controller._submit_cycle_intent=Mock(side_effect=AssertionError("submit must stay unreachable"))
            decision=types.SimpleNamespace(strategy_id="source",strategy_instance_id="member",signal="BUY",reason="fixture",bar=types.SimpleNamespace(close=100))
            result=controller._handle_cycle_locked(types.SimpleNamespace(decisions=(decision,)))
            self.assertEqual("BLOCKED",result["results"][0]["action"])
            controller._submit_cycle_intent.assert_not_called();fake_state.account_risk_for_intent.assert_not_called()
            with self.assertRaises(ValueError):controller._order_quantity(selected,100)

    def test_continuous_helper_uses_fresh_cash_separate_from_equity_and_normalized_weight(self):
        fake_state=types.ModuleType(PACKAGE+".state")
        selected=spec();selected.artifact["standalone"]=False;selected.target_weight=2
        second=deepcopy(selected);second.strategy_instance_id="other";second.target_weight=6
        fake_state.STATE={"native_spot_quantity_contexts":{"member":{**context(),"asOf":datetime.now(timezone.utc).isoformat()}}}
        fake_state.account_risk_for_intent=Mock(return_value={"known":True,"fresh":True,"available_cash":"700"});fake_state.asset_from_symbol=Mock(return_value="CRYPTO")
        with patch.dict(sys.modules,{PACKAGE+".state":fake_state}),patch.object(package,"state",fake_state,create=True):
            controller_module=importlib.import_module(PACKAGE+".continuous_live");controller=controller_module.LiveContinuousController(APP);controller.mode="SMALL_LIVE";controller.deployment_id="deployment"
            result=controller._native_quantity_result(selected,"BUY",100,0,(selected,second));self.assertEqual("READY",result["status"]);self.assertEqual(0.124,result["quantityForSubmission"]);self.assertEqual("0.25",result["modelResult"]["allocationWeight"])
            fake_state.account_risk_for_intent.return_value={"known":True,"fresh":False,"available_cash":"700"};self.assertEqual("BLOCKED",controller._native_quantity_result(selected,"BUY",100,0,(selected,second))["status"])
            legacy=deepcopy(selected);legacy.artifact.pop("executionSizing");self.assertEqual(.055,controller._order_quantity(legacy,100))
            short=deepcopy(selected);short.artifact["position_direction"]="short";self.assertEqual(.055,controller._order_quantity(short,100))

    def test_legacy_artifact_remains_on_existing_model(self):
        self.assertIsNone(service.sizing_from_artifact({"executionSizing":{"mode":"fixed_quantity","paperOrderQuantity":1}}))
        value=sizing();value.pop("capitalBasis")
        with self.assertRaises(ValueError):service.sizing_from_artifact({"executionSizing":value})


if __name__=="__main__":unittest.main()
