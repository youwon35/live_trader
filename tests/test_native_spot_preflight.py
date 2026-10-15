from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import ast, contextlib, importlib, json, socket, sys, tempfile, types, unittest
from pathlib import Path
from unittest.mock import Mock, patch
import test_native_spot_quantity as base

package=base.package;P=base.PACKAGE
pre=importlib.import_module(P+".native_spot_preflight")
cap=importlib.import_module(P+".owned_deployment_capital")
boundary_module=importlib.import_module(P+".native_open_boundary")
runtime_module=importlib.import_module(P+".native_spot_runtime")
ticket=importlib.import_module(P+".native_order_ticket")
account_module=importlib.import_module(P+".read_only_account")
profiles=importlib.import_module(P+".account_profiles")
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.continuous_runtime import ClosedBar, OpenBoundary
from trading_runtime.execution_quantity import seal_quantity_contract
SETTINGS={"BINANCE_API_KEY":"fixture-key","BINANCE_API_SECRET":"fixture-secret","UPBIT_ACCESS_KEY":"fixture-key","UPBIT_SECRET_KEY":"fixture-secret"}
NOW=datetime.now(timezone.utc)
SHA="a"*64
PH="b"*64

def spec(provider="binance",minutes=5):
    c=base.contract(notionalReferenceMinutes=minutes,capabilityStatus="UNKNOWN")
    if provider=="upbit":
        c=seal_quantity_contract({**{k:v for k,v in c.items() if k!="contractHash"},"provider":"upbit","symbol":"KRW-ETH","baseCurrency":"ETH","quoteCurrency":"KRW","stepSize":"0.00000001","minQuantity":"0.00000001","maxQuantity":"1000000000000","minNotional":"5000","maxNotional":"0","notionalReferenceMinutes":0,"provenance":{"references":[pre.UPBIT_PRECISION_REFERENCE]}})
    s=base.spec({"mode":"capital_percentage","fractionalAllowed":True,"capitalBasis":base.service.CAPITAL_BASIS,"quantityContract":c})
    s.broker_id=provider;s.provider=provider;s.symbol=c["symbol"];s.instrument_id=provider+":spot:"+s.symbol;s.timeframe="5m";s.artifact_hash=SHA;s.portfolio_hash=PH
    return s

def deployment(s,settings=SETTINGS):
    p=next(p for p in profiles.saved_account_profiles(settings) if p["brokerId"]==s.provider)
    d={"deploymentId":"deployment","environment":"LIVE","lifecycle":"before-live-small","accountId":p["accountBindingId"],"route":s.provider,
       "permissions":{"liveReviewBindingHash":"c"*64,"liveCapitalPolicy":{"basis":"ACCOUNT_AVAILABLE_CASH","percent":100},"liveAccountProfileId":p["profileId"],"liveAccountFingerprint":p["accountFingerprint"],"liveAccountCredentialConfigurationHash":p["credentialConfigurationHash"]}}
    bindings=[{"strategyInstanceId":s.strategy_instance_id,"sourceArtifactHash":s.artifact_hash,"portfolioHash":s.portfolio_hash,"symbol":s.symbol,"quoteCurrency":s.artifact["executionSizing"]["quantityContract"]["quoteCurrency"]}]
    return d,p,cap.reviewed_scope(d,p,bindings)

def market(s):
    c=s.artifact["executionSizing"]["quantityContract"]
    return {"symbol":s.symbol,"baseAsset":c["baseCurrency"],"quoteAsset":c["quoteCurrency"],"status":"TRADING","isSpotTradingAllowed":True,"orderTypes":["MARKET"],
      "filters":[{"filterType":"LOT_SIZE","stepSize":c["stepSize"],"minQty":c["minQuantity"],"maxQty":c["maxQuantity"]},{"filterType":"MARKET_LOT_SIZE","stepSize":"0","minQty":"0","maxQty":c["maxQuantity"]},{"filterType":"NOTIONAL","minNotional":c["minNotional"],"maxNotional":c["maxNotional"],"applyMinToMarket":True,"applyMaxToMarket":False,"avgPriceMins":c["notionalReferenceMinutes"]}]}

class Reader:
    def __init__(self,s,cash="1000"):
        self.settings=dict(SETTINGS);self.spec=s;self.cash=cash;self.calls=[];self.mutations={};self.trades=[];self.order={}
    def _credentials(self,provider):pass
    def observe_preparation(self,provider,symbol,**kwargs):
        p=next(p for p in profiles.saved_account_profiles(self.settings) if p["brokerId"]==provider)
        return {"account":"AVAILABLE","accountBinding":"SAVED_CREDENTIAL_CONFIGURATION_OBSERVED","profileId":p["profileId"],"currency":self.spec.artifact["executionSizing"]["quantityContract"]["quoteCurrency"],"availableCash":self.cash,"openOrders":"SYMBOL_NONE_OBSERVED","orderability":"MARKET_AND_ACCOUNT_ENABLED","asOf":datetime.now(timezone.utc).isoformat()}
    def _get(self,provider,path,**kwargs):
        self.calls.append((provider,path))
        c=self.spec.artifact["executionSizing"]["quantityContract"]
        now=int(datetime.now(timezone.utc).timestamp()*1000)
        chance={"market":{"id":self.spec.symbol,"state":"active","bid_types":["price","limit"],"ask_types":["market","limit"],"bid":{"min_total":"5000"},"max_total":"1000000000"},"bid_fee":"0.0005","ask_fee":"0.0007"}
        values={"/api/v3/exchangeInfo":{"symbols":[market(self.spec)]},"/api/v3/time":{"serverTime":now},"/api/v3/avgPrice":{"mins":c["notionalReferenceMinutes"],"price":"100","closeTime":now},
          "/api/v3/account/commission":{"symbol":self.spec.symbol,**{key:{"taker":"0.001","buyer":"0.0001","seller":"0.0002"} for key in ("standardCommission","specialCommission","taxCommission")}},
          "/api/v3/ticker/price":{"symbol":self.spec.symbol,"price":"110"},"/api/v3/order":self.order,"/api/v3/myTrades":self.trades,"/v1/orders/chance":chance,
          "/v1/ticker":[{"market":self.spec.symbol,"trade_price":100000,"timestamp":now}],"/v1/order":self.order}
        value=deepcopy(self.mutations.get(path,values[path]))
        return value,{}

def observed(s,side="BUY"):
    return pre.observe_native_spot(Reader(s),s.artifact["executionSizing"]["quantityContract"],side=side)

def open_event(s,start=None,**changes):
    stamp=start or datetime.now(timezone.utc)
    values={"instrument_id":s.instrument_id,"symbol":s.symbol,"provider":s.provider,"timeframe":s.timeframe,"start_time":stamp.isoformat(),"open":100,"observed_time":(stamp+timedelta(milliseconds=1)).isoformat(),"source_sequence":"fixture","raw_hash":"d"*64,"attestation":s.provider.upper()+"_WEBSOCKET","promotion_eligible":True,"recovery_only":False}
    values.update(changes);return OpenBoundary(**values)

def decision(s,end=None,side="BUY"):
    end=end or datetime.now(timezone.utc)
    bar=ClosedBar(instrument_id=s.instrument_id,symbol=s.symbol,provider=s.provider,timeframe=s.timeframe,start_time=(end-timedelta(minutes=5)).isoformat(),end_time=end.isoformat(),open=900,high=1000,low=900,close=999,volume=1,received_time=end.isoformat())
    return types.SimpleNamespace(strategy_id=s.strategy_id,strategy_instance_id=s.strategy_instance_id,signal=side,reason="fixture",evaluation_key="evaluation:"+end.isoformat(),bar=bar)

class NativePreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.net=patch.object(socket,"socket",side_effect=AssertionError("real network forbidden"));self.net.start();self.addCleanup(self.net.stop)

    def test_first_native_start_binds_reconciliation_to_requested_deployment(self):
        from threading import RLock
        from unittest.mock import MagicMock

        fake = types.ModuleType(P + ".state")
        fake.STATE = {}
        fake.snapshot = Mock(return_value={})
        fake.append_audit = Mock()
        fake.FUNCTIONAL_TEST_EXECUTION_PURPOSE = "FUNCTIONAL_TEST"
        fake.RUNTIME_MODE_LOCK = RLock()
        with patch.dict(sys.modules, {P + ".state": fake}), patch.object(package, "state", fake, create=True):
            module = importlib.import_module(P + ".continuous_live")
            source = spec()
            strategy = {
                **source.artifact,
                "strategy_id": source.strategy_id,
                "strategy_instance_id": source.strategy_instance_id,
                "symbol": source.symbol,
                "timeframe": source.timeframe,
                "provider": source.provider,
                "broker_id": source.broker_id,
                "artifact_hash": source.artifact_hash,
                "parameters": source.parameters,
                "permissions": {"live_small_eligible": True},
            }
            controller = module.LiveContinuousController(self.root, latency_diagnostics_enabled=False)
            native = MagicMock()
            native.deployment_id = "requested-deployment"
            engine = MagicMock()
            engine.transition_mode.return_value = None
            engine.seed_history.return_value = 0
            supervisor = MagicMock()
            supervisor.snapshot.return_value = {"phase": "RUNNING", "running": True}
            feed = MagicMock()
            feed.provider_id = source.provider
            feed.warmup.return_value = []
            with (
                patch.object(controller, "_dispatch_status", return_value={"reconciliationRequired": False}),
                patch.object(controller, "_select_standalone_strategy", return_value=strategy),
                patch.object(controller, "_paper_final_binding_blocker", return_value=""),
                patch.object(controller, "_restore_context_assessment", return_value={}),
                patch.object(controller, "_dispatcher", return_value=contextlib.nullcontext()),
                patch.object(runtime_module, "NativeSpotRuntime", return_value=native) as runtime_factory,
                patch.object(module, "PortfolioRuntimeEngine", return_value=engine),
                patch.object(module, "ContinuousRuntimeSupervisor", return_value=supervisor),
                patch.object(module, "DurableRuntimeState"),
                patch.object(module, "feeds_for_specs", return_value=[feed]),
                patch.object(module, "required_warmup_bars", return_value=1),
            ):
                result = controller.start("crypto", "SMALL_LIVE", strategy_id=source.strategy_id, deployment_id="requested-deployment")

            self.assertTrue(result["ok"], result.get("reason"))
            self.assertEqual("requested-deployment", controller.deployment_id)
            self.assertEqual("SMALL_LIVE", controller.mode)
            self.assertEqual("requested-deployment", runtime_factory.call_args.args[1])
            native.initialize_at_reviewed_manual_start.assert_called_once_with()
            native.scope.assert_called_once_with()
            supervisor.start.assert_called_once_with()

    def test_native_reconciliation_keeps_active_deployment_on_mode_transition(self):
        fake = types.ModuleType(P + ".state")
        with patch.dict(sys.modules, {P + ".state": fake}), patch.object(package, "state", fake, create=True):
            module = importlib.import_module(P + ".continuous_live")
            controller = module.LiveContinuousController(self.root)
            controller.deployment_id = "active-deployment"
            with patch.object(controller, "_ensure_native_runtime") as ensure:
                self.assertEqual("", controller._portfolio_reconciliation_blocker((spec(),)))
            ensure.assert_called_once_with((unittest.mock.ANY,), "active-deployment")

    def test_stopped_native_runtime_same_deployment_reuses_owned_principal(self):
        from unittest.mock import MagicMock
        module = importlib.import_module(P + ".continuous_live")
        source = spec()
        current_deployment, account, old_scope = deployment(source)
        runtime = runtime_module.NativeSpotRuntime(self.root, "deployment", (source,), reader=Reader(source), roots=[], deployment_reader=lambda *args: current_deployment, dispatch_reader=lambda: [])
        runtime.initialize_at_reviewed_manual_start()
        controller = module.LiveContinuousController(self.root)
        controller._native_runtime = runtime
        controller.native_context_provider = runtime.context
        controller.supervisor = MagicMock()
        controller.supervisor.running = False
        controller.supervisor.snapshot.return_value = {"phase": "STOPPED", "running": False}
        with patch.object(runtime_module, "NativeSpotRuntime") as create:
            self.assertIs(runtime, controller._ensure_native_runtime((source,), "deployment"))
        create.assert_not_called()
        self.assertEqual("1000", runtime.store.project(old_scope, {})["initialPrincipal"])

    def test_clean_stop_allows_reviewed_other_account_preserving_prior_capital(self):
        from unittest.mock import MagicMock
        module = importlib.import_module(P + ".continuous_live")
        source = spec()
        prior_deployment, account, old_scope = deployment(source)
        prior = runtime_module.NativeSpotRuntime(self.root, "deployment", (source,), reader=Reader(source), roots=[], deployment_reader=lambda *args: prior_deployment, dispatch_reader=lambda: [])
        prior.initialize_at_reviewed_manual_start()
        next_source = spec("upbit")
        next_deployment, next_account, _ = deployment(next_source)
        next_deployment["deploymentId"] = "next-deployment"
        candidate = runtime_module.NativeSpotRuntime(self.root, "next-deployment", (next_source,), reader=Reader(next_source), roots=[], deployment_reader=lambda *args: next_deployment, dispatch_reader=lambda: [])
        controller = module.LiveContinuousController(self.root)
        controller._native_runtime = prior
        controller.native_context_provider = prior.context
        controller.supervisor = MagicMock()
        controller.supervisor.running = False
        controller.supervisor.snapshot.return_value = {"phase": "STOPPED", "running": False}
        with patch.object(controller, "_dispatch_status", return_value={"reconciliationRequired": False}), patch.object(runtime_module, "NativeSpotRuntime", return_value=candidate):
            self.assertIs(candidate, controller._ensure_native_runtime((next_source,), "next-deployment"))
        self.assertEqual("1000", prior.store.project(old_scope, {})["initialPrincipal"])
        self.assertEqual("1000", candidate.store.project(candidate.scope(), {})["initialPrincipal"])
        self.assertEqual(candidate.context, controller.native_context_provider)

    def test_restart_cannot_reallocate_reserved_same_account_cash(self):
        from unittest.mock import MagicMock
        module = importlib.import_module(P + ".continuous_live")
        source = spec()
        prior_deployment, account, old_scope = deployment(source)
        prior = runtime_module.NativeSpotRuntime(self.root, "deployment", (source,), reader=Reader(source), roots=[], deployment_reader=lambda *args: prior_deployment, dispatch_reader=lambda: [])
        prior.initialize_at_reviewed_manual_start()
        next_deployment = deepcopy(prior_deployment)
        next_deployment["deploymentId"] = "next-deployment"
        candidate = runtime_module.NativeSpotRuntime(self.root, "next-deployment", (source,), reader=Reader(source), roots=[], deployment_reader=lambda *args: next_deployment, dispatch_reader=lambda: [])
        controller = module.LiveContinuousController(self.root)
        controller._native_runtime = prior
        controller.native_context_provider = prior.context
        controller.supervisor = MagicMock()
        controller.supervisor.running = False
        controller.supervisor.snapshot.return_value = {"phase": "STOPPED", "running": False}
        with patch.object(controller, "_dispatch_status", return_value={"reconciliationRequired": False}), patch.object(runtime_module, "NativeSpotRuntime", return_value=candidate), self.assertRaisesRegex(ValueError, "OWNED_CAPITAL_ALREADY_RESERVED"):
            controller._ensure_native_runtime((source,), "next-deployment")
        self.assertIs(prior, controller._native_runtime)
        self.assertEqual(prior.context, controller.native_context_provider)
        self.assertEqual("1000", prior.store.project(old_scope, {})["initialPrincipal"])

    def test_other_deployment_requires_clean_stop_and_resolved_dispatch(self):
        from unittest.mock import MagicMock
        module = importlib.import_module(P + ".continuous_live")
        for phase, running, unresolved in (("RUNNING", True, False), ("FAILED", False, False), ("STOPPED", False, True), ("STOPPED", False, None)):
            with self.subTest(phase=phase, running=running, unresolved=unresolved):
                prior = MagicMock()
                prior.deployment_id = "prior-deployment"
                controller = module.LiveContinuousController(self.root)
                controller._native_runtime = prior
                controller.supervisor = MagicMock()
                controller.supervisor.running = running
                controller.supervisor.snapshot.return_value = {"phase": phase, "running": running}
                with patch.object(controller, "_dispatch_status", return_value={"reconciliationRequired": unresolved}), patch.object(runtime_module, "NativeSpotRuntime") as create, self.assertRaises(ValueError):
                    controller._ensure_native_runtime((spec(),), "next-deployment")
                create.assert_not_called()
                self.assertIs(prior, controller._native_runtime)

    def test_other_deployment_waits_for_prior_native_order_truth(self):
        from unittest.mock import MagicMock
        module = importlib.import_module(P + ".continuous_live")
        prior = MagicMock()
        prior.deployment_id = "prior-deployment"
        prior.sync_owned_fills.side_effect = ValueError("OWNED_DISPATCH_UNRESOLVED")
        controller = module.LiveContinuousController(self.root)
        controller._native_runtime = prior
        controller.supervisor = MagicMock()
        controller.supervisor.running = False
        controller.supervisor.snapshot.return_value = {"phase": "STOPPED", "running": False}
        with patch.object(controller, "_dispatch_status", return_value={"reconciliationRequired": False}), patch.object(runtime_module, "NativeSpotRuntime") as create, self.assertRaisesRegex(ValueError, "OWNED_DISPATCH_UNRESOLVED"):
            controller._ensure_native_runtime((spec(),), "next-deployment")
        create.assert_not_called()
        prior.sync_owned_fills.assert_called_once_with(prior.scope.return_value)
        self.assertIs(prior, controller._native_runtime)

    def test_fresh_average_and_directional_all_commissions_are_bound(self):
        s=spec();r=Reader(s);o=pre.observe_native_spot(r,s.artifact["executionSizing"]["quantityContract"],side="BUY")
        self.assertEqual(Decimal("0.0033"),Decimal(o["feeRate"]));self.assertFalse(o["orderAuthorityGranted"]);self.assertEqual(5,o["marketReference"]["minutes"])
        self.assertEqual(Decimal("0.0036"),Decimal(observed(s,"SELL")["feeRate"]));self.assertIn(("binance","/api/v3/account/commission"),r.calls)

    def test_interval_stale_rule_scope_and_response_mutations_block(self):
        s=spec();c=s.artifact["executionSizing"]["quantityContract"]
        for value in ({"mins":1,"price":"100","closeTime":int(NOW.timestamp()*1000)},{"mins":5,"price":"100","closeTime":int((NOW-timedelta(seconds=60)).timestamp()*1000)},{"mins":True,"price":"100","closeTime":int(NOW.timestamp()*1000)}):
            r=Reader(s);r.mutations["/api/v3/avgPrice"]=value
            with self.subTest(value=value),self.assertRaises(ValueError):pre.observe_native_spot(r,c,side="BUY")
        o=observed(s)
        for key,value in (("symbol","BTCUSDT"),("side","SELL"),("feeRate","NaN"),("asOf",(NOW-timedelta(minutes=1)).isoformat())):
            bad=pre.seal({**o,key:value})
            with self.subTest(key=key),self.assertRaises(ValueError):pre.valid_observation(bad,c,side="BUY")
        m=market(s);m["filters"][0]["stepSize"]="0.01";r=Reader(s);r.mutations["/api/v3/exchangeInfo"]={"symbols":[m]}
        with self.assertRaises(ValueError):pre.observe_native_spot(r,c,side="BUY")

    def test_unknown_research_model_is_not_changed_by_live_observation(self):
        s=spec();c=deepcopy(s.artifact["executionSizing"]["quantityContract"]);o=observed(s)
        ctx={**base.context(s.artifact["executionSizing"]),"asOf":o["asOf"],"sourceArtifactHash":SHA,"portfolioHash":PH,"currentQuantityContract":o["currentQuantityContract"],"nativeMarketObservation":o,"actualFeeRate":o["feeRate"],"notionalReferencePrice":o["marketReference"]["price"]}
        result=base.service.live_quantity(s,side="BUY",price="100",allocation_weight=1,identity={"deploymentId":"deployment"},context=ctx)
        self.assertEqual("READY",result["status"]);self.assertEqual("UNKNOWN",result["modelResult"]["nativeEligibility"]);self.assertEqual("VERIFIED",result["nativeEligibility"]);self.assertEqual(c,s.artifact["executionSizing"]["quantityContract"])

    def test_current_higher_maximum_retains_frozen_cap_other_changes_block(self):
        s=spec();source=s.artifact["executionSizing"]["quantityContract"]
        higher=seal_quantity_contract({**source,"maxQuantity":"2000"})
        diff=pre.source_rules_fit_current(source,higher)
        self.assertEqual("FROZEN_SOURCE_LOWER_CAP_RETAINED",diff[0]["effect"])
        model=base.service.calculate_model_quantity(s.artifact["executionSizing"],side="BUY",price=100,deployment_equity="10000000",available_cash="10000000",allocation_weight=1,position_percent=100,source_fee_rate=0,source_slippage_rate=0)
        self.assertEqual("1000",model["quantity"])
        for changes in ({"stepSize":"0.0001"},{"minQuantity":"0.01"},{"notionalReferenceMinutes":1}):
            current=seal_quantity_contract({**source,**changes})
            with self.assertRaises(ValueError):pre.source_rules_fit_current(source,current)
        for value in ("0","NaN","Infinity",True):
            with self.assertRaises(ValueError):seal_quantity_contract({**source,"maxQuantity":value})

    def test_context_cannot_substitute_typed_fee_average_rules_or_cash(self):
        s=spec();o=observed(s)
        context={**base.context(s.artifact["executionSizing"]),"asOf":o["asOf"],"sourceArtifactHash":SHA,"portfolioHash":PH,"currentQuantityContract":o["currentQuantityContract"],"nativeMarketObservation":o,"actualFeeRate":o["feeRate"],"notionalReferencePrice":o["marketReference"]["price"]}
        for key,value in (("actualFeeRate","0"),("notionalReferencePrice","999"),("availableCash","1001"),("currentQuantityContract",s.artifact["executionSizing"]["quantityContract"]),("asOf",(NOW-timedelta(seconds=1)).isoformat())):
            result=base.service.live_quantity(s,side="BUY",price=100,allocation_weight=1,identity={"deploymentId":"deployment"},context={**context,key:value})
            self.assertEqual("BLOCKED",result["status"],(key,result));self.assertIsNone(result["quantityForSubmission"])

    def test_upbit_sell_fee_and_owned_buy_formed_precision_are_explicit(self):
        s=spec("upbit",0);o=observed(s,"SELL");self.assertEqual("0.0007",o["feeRate"]);self.assertEqual("OWNED_BUY_FORMED_QUANTITY_ONLY",o["currentQuantityContract"]["provenance"]["scope"])

    def test_transport_only_exact_get_allowlist_no_order_or_cancel(self):
        transport=account_module.ReadOnlyTransport()
        for provider,method,path in (("binance","POST","/api/v3/order"),("binance","DELETE","/api/v3/order"),("upbit","POST","/v1/orders"),("upbit","DELETE","/v1/order"),("binance","GET","/api/v3/order/test")):
            with self.assertRaises(account_module.ReadBoundaryError):transport.request(provider,method,path)

    def test_initial_principal_is_once_and_uses_only_owned_fills(self):
        s=spec();d,a,scope=deployment(s);reader=Reader(s);store=cap.OwnedCapitalStore(self.root/"capital.sqlite")
        initial=store.initialize(scope,reader.observe_preparation("binance",s.symbol))
        self.assertEqual("1000",initial["deploymentEquity"]);self.assertFalse(initial["orderAuthorityGranted"])
        reader.cash="600";self.assertEqual("1000",store.initialize(scope,reader.observe_preparation("binance",s.symbol))["deploymentEquity"])
        dispatch={"brokerOrderId":"42","persistedDispatchVerified":True,"metadata":{"deployment_id":"deployment","strategy_instance_id":"member","source_artifact_hash":SHA,"portfolio_hash":PH}}
        fill={"eventId":"fill1","brokerOrderId":"42","strategyInstanceId":"member","symbol":s.symbol,"side":"BUY","quantity":"1","price":"100","fee":"1","feeCurrency":"USDT","occurredAt":NOW.isoformat(),"accountFingerprint":a["accountFingerprint"]}
        self.assertTrue(store.append_verified_fill(scope,fill,dispatch));self.assertFalse(store.append_verified_fill(scope,fill,dispatch))
        projection=store.project(scope,{"member":"110"});self.assertEqual(("899","1009","1"),(projection["ownedCash"],projection["deploymentEquity"],projection["ownedPositions"]["member"]))
        self.assertIsNone(store.project(scope,{})["deploymentEquity"])
        for mutated in ({**fill,"price":"200"},{**fill,"eventId":"other","feeCurrency":"BNB"},{**fill,"eventId":"other","accountFingerprint":"foreign"}):
            with self.assertRaises(ValueError):store.append_verified_fill(scope,mutated,dispatch)

    def test_foreign_holdings_and_duplicate_initial_cash_are_not_owned(self):
        s=spec();d,a,scope=deployment(s);store=cap.OwnedCapitalStore(self.root/"capital.sqlite");store.initialize(scope,Reader(s).observe_preparation("binance",s.symbol))
        d["deploymentId"]="other";other=cap.reviewed_scope(d,a,scope["members"])
        with self.assertRaises(ValueError):store.initialize(other,Reader(s).observe_preparation("binance",s.symbol))
        bad=deepcopy(d);bad["lifecycle"]="draft"
        with self.assertRaises(ValueError):cap.reviewed_scope(bad,a,scope["members"])
        self.assertEqual({"member":"0"},store.project(scope,{})["ownedPositions"])

    def test_boundary_pending_does_not_size_or_submit_at_closed_price(self):
        s=spec();d=decision(s,NOW);q=boundary_module.NativeBoundaryQueue(self.root/"queue.sqlite")
        result=q.stage(d,s,"deployment");self.assertEqual("WAIT_OPEN_BOUNDARY",result["action"]);self.assertFalse(result["quantityCalculated"])
        callback=Mock(return_value={"ok":True,"price":100,"orderSubmitted":False})
        exact=open_event(s,NOW);result=q.release(exact,(s,),"deployment",callback,now=NOW+timedelta(seconds=1))
        self.assertEqual(100,result["results"][0]["price"]);self.assertEqual("999",callback.call_args[0][0]["closedSignalPrice"])
        self.assertEqual([],q.release(exact,(s,),"deployment",callback,now=NOW+timedelta(seconds=1))["results"]);callback.assert_called_once()

    def test_late_recovered_wrong_current_source_or_process_owner_blocks(self):
        s=spec()
        for value in ("late","recovery","source","owner"):
            root=self.root/value;q=boundary_module.NativeBoundaryQueue(root/"q.sqlite");q.stage(decision(s,NOW),s,"deployment");event=open_event(s,NOW);current=s
            if value=="recovery":event=open_event(s,NOW,attestation="RECOVERED",promotion_eligible=False,recovery_only=True)
            if value=="source":current=deepcopy(s);current.artifact_hash="x"*64
            if value=="owner":q=boundary_module.NativeBoundaryQueue(root/"q.sqlite")
            callback=Mock(side_effect=AssertionError("prepare forbidden"))
            if value=="owner":
                with self.assertRaises(ValueError):q.release(event,(current,),"deployment",callback,now=NOW+timedelta(seconds=1))
            else:
                result=q.release(event,(current,),"deployment",callback,now=NOW+timedelta(seconds=31 if value=="late" else 1));self.assertEqual("BLOCKED",result["results"][0]["action"])
            callback.assert_not_called()

    def test_readonly_cash_initial_proposal_never_substitutes_current_equity(self):
        s=spec();instrument={"executionSizing":s.artifact["executionSizing"],"sourceMaxOrderPercent":5,"allocationWeight":"0.25","executionPolicy":{"feePercent":"0.1","slippagePercent":"0.2"}}
        result=base.preview.cash_preview(instrument,{"availableCash":"1000","feeRate":"0.001"},{"side":"BUY","limitPrice":"100"})
        self.assertEqual("BLOCKED",result["status"]);self.assertIsNone(result["maximumQuantity"]);self.assertEqual("0.124",result["proposedInitialQuantity"]);self.assertEqual("INITIAL_REVIEW_PROPOSAL_NOT_CURRENT_DEPLOYMENT_EQUITY",result["proposalScope"])

    def test_producer_correlates_actual_trade_fee_and_rejects_unrelated_order(self):
        s=spec();d,a,scope=deployment(s);reader=Reader(s);runtime=runtime_module.NativeSpotRuntime(self.root,"deployment",(s,),reader=reader,roots=[],deployment_reader=lambda *args:d,dispatch_reader=lambda:[])
        runtime.initialize_at_reviewed_manual_start()
        row={"side":"BUY","broker_order_id":"42","metadata":{"deployment_id":"deployment","strategy_instance_id":"member","source_artifact_hash":SHA,"portfolio_hash":PH,"native_account_fingerprint":a["accountFingerprint"]}}
        runtime.dispatch_reader=lambda:[row]
        reader.order={"symbol":s.symbol,"orderId":42,"side":"BUY","status":"FILLED","executedQty":"1"}
        reader.trades=[{"symbol":s.symbol,"orderId":42,"id":1,"isBuyer":True,"qty":"1","price":"100","commission":"0.01","commissionAsset":"ETH","time":int(NOW.timestamp()*1000)}]
        runtime.sync_owned_fills(scope)
        result=runtime.store.project(scope,{"member":"110"});self.assertEqual("0.99",result["ownedPositions"]["member"]);self.assertEqual("1008.90",result["deploymentEquity"])
        calls = len(reader.calls)
        reader.trades[0]["orderId"]=43
        runtime.sync_owned_fills(scope)
        self.assertEqual(calls, len(reader.calls), "complete terminal receipt must not repeat financial history GETs")
        self.assertEqual("0.99", runtime.store.project(scope,{"member":"110"})["ownedPositions"]["member"])
        # A different order has no cached authority: unrelated remote trade data
        # must still fail its first complete terminal receipt verification.
        uncached = {**row, "broker_order_id":"43"}
        runtime.dispatch_reader=lambda:[row, uncached]
        reader.order={**reader.order, "orderId":43}
        reader.trades[0]["orderId"]=44
        with self.assertRaises(ValueError):runtime.sync_owned_fills(scope)

    def test_trade_identity_is_account_and_symbol_scoped(self):
        s=spec();d,a,scope=deployment(s)
        row={"broker_order_id":"42","side":"BUY"}
        first=runtime_module.NativeSpotRuntime._fill(scope,s,row,"1",1,100,0,"USDT",NOW.isoformat())
        second=deepcopy(s);second.symbol="BTCUSDT";second.strategy_instance_id="second"
        other=runtime_module.NativeSpotRuntime._fill(scope,second,row,"1",1,100,0,"USDT",NOW.isoformat())
        self.assertNotEqual(first["eventId"],other["eventId"])
        foreign=runtime_module.NativeSpotRuntime._fill({**scope,"accountFingerprint":"f"*64},s,row,"1",1,100,0,"USDT",NOW.isoformat())
        self.assertNotEqual(first["eventId"],foreign["eventId"])

    def test_boundary_callback_failure_is_consumed_and_dispatcher_posts_zero(self):
        dispatch_module=importlib.import_module(P+".continuous_dispatch")
        from trading_runtime.order_management import OrderIntent
        from threading import RLock
        class OwnedLock:
            def __init__(self):self.lock=RLock()
            def acquire(self):return self.lock.acquire()
            def release(self):return self.lock.release()
            def owned_by_current_thread(self):return self.lock._is_owned()
        q=boundary_module.NativeBoundaryQueue(self.root/"q.sqlite");s=spec();d=decision(s,NOW);q.stage(d,s,"deployment")
        submit=Mock(side_effect=AssertionError("financial submit forbidden"))
        dispatcher=dispatch_module.ContinuousDispatcher(self.root/"dispatch.sqlite",OwnedLock(),submit,Mock())
        def fail(payload,selected,exact):
            # The persisted boundary claim is already visible before any defer.
            self.assertEqual(1,q.snapshot()["states"]["BOUNDARY_CLAIMED"])
            dispatcher.defer(OrderIntent("source","CRYPTO",s.symbol,"BUY",1,100,"SMALL_LIVE","fixture",{"broker_id":"binance","runtime_evaluation_key":"fixture","deployment_id":"deployment"}))
            raise ValueError("later member fixture failure")
        with self.assertRaisesRegex(ValueError,"later member"):
            with dispatcher:q.release(open_event(s,NOW),(s,),"deployment",fail,now=NOW+timedelta(seconds=1))
        submit.assert_not_called();self.assertEqual("ABORTED",dispatcher.journal.rows()[0]["state"])
        retry=Mock(side_effect=AssertionError("must not replay consumed signal"))
        self.assertEqual([],q.release(open_event(s,NOW),(s,),"deployment",retry,now=NOW+timedelta(seconds=2))["results"]);retry.assert_not_called()

    def test_native_portfolio_members_use_canonical_allocations_and_owned_positions(self):
        s=spec();s.artifact["standalone"]=False;s.target_weight=2;s.portfolio_id="portfolio"
        other=deepcopy(s);other.strategy_instance_id="member2";other.target_weight=6
        observed_receipt=observed(s)
        contexts={}
        for selected in (s,other):
            contexts[selected.strategy_instance_id]={"deploymentId":"deployment","sourceArtifactHash":SHA,"portfolioHash":PH,"strategyInstanceId":selected.strategy_instance_id,"quoteCurrency":"USDT","equityScope":"OWNED_DEPLOYMENT_ONLY","deploymentEquity":"1000","availableCash":"1000","accountKnown":True,"accountFresh":True,"asOf":observed_receipt["asOf"],"actualFeeRate":observed_receipt["feeRate"],"notionalReferencePrice":observed_receipt["marketReference"]["price"],"currentQuantityContract":observed_receipt["currentQuantityContract"],"nativeMarketObservation":observed_receipt,"ownedPositions":{"member":"0","member2":"0"},"accountFingerprint":"a"*64,"capitalScopeHash":"b"*64}
        fake=types.ModuleType(P+".state");fake.STATE={"strategy_runner":{},"risk_settings":{"strategy_capital_limit_krw":1000},"dry_run":True};fake.append_audit=Mock();fake.asset_from_symbol=lambda symbol,**route:"CRYPTO";fake.FUNCTIONAL_TEST_EXECUTION_PURPOSE="FUNCTIONAL_TEST"
        with patch.dict(sys.modules,{P+".state":fake}),patch.object(package,"state",fake,create=True):
            module=importlib.import_module(P+".continuous_live");controller=module.LiveContinuousController(self.root);controller.mode="SMALL_LIVE";controller.profile_id="crypto";controller.deployment_id="deployment";controller.portfolio_id="portfolio";controller.strategy_ids=(s.strategy_id,);controller.allowed_symbols=(s.symbol,);controller.supervisor=types.SimpleNamespace(engine=types.SimpleNamespace(specs=(s,other)));controller.native_context_provider=lambda selected,*args:contexts[selected.strategy_instance_id]
            controller._dispatcher=Mock(return_value=contextlib.nullcontext());captured=[]
            controller._submit_cycle_intent=lambda intent,**kw:(captured.append(intent) or {"ok":True,"reason":"FIXTURE_ONLY"})
            end=datetime.now(timezone.utc)-timedelta(seconds=1);first=decision(s,end);second=decision(other,end);second.evaluation_key="second:"+first.evaluation_key
            row=controller._handle_cycle_locked(types.SimpleNamespace(decisions=(first,second)))
            self.assertEqual(["WAIT_OPEN_BOUNDARY"]*2,[r["action"] for r in row["results"]]);self.assertEqual([],captured)
            row=controller._handle_native_open_boundary(open_event(s,end));self.assertEqual(2,len(captured));self.assertTrue(all(r["ok"] for r in row["results"]))
            weights={i.metadata["strategy_instance_id"]:i.metadata["native_quantity_model"]["allocationWeight"] for i in captured}
            self.assertEqual({"member":"0.25","member2":"0.75"},weights)
            for intent in captured:self.assertFalse(ticket.validate_native_ticket(intent)["orderAuthorityGranted"])

    def test_external_cash_change_blocks_instead_of_resetting_principal(self):
        s=spec();d,a,scope=deployment(s);reader=Reader(s);runtime=runtime_module.NativeSpotRuntime(self.root,"deployment",(s,),reader=reader,roots=[],deployment_reader=lambda *args:d,dispatch_reader=lambda:[])
        runtime.initialize_at_reviewed_manual_start();reader.cash="900"
        result=runtime.context(s,"BUY",100,(s,),None)
        self.assertEqual("BLOCKED",result["status"]);self.assertIn("EXTERNAL_CASH_CHANGE",result["reasons"][0])
        self.assertEqual("1000",runtime.store.project(scope,{})["initialPrincipal"])

    def test_upbit_exact_decimal_quote_and_final_state_seam_without_post(self):
        from trading_runtime.order_management import OrderIntent
        now=datetime.now(timezone.utc)
        model={"status":"READY","contractHash":"e"*64,"actualFeeObserved":True,"quantity":"0.00012345","fillPrice":"100200","notional":"12.36969","fee":"0.01236969","feeRateUsed":"0.001"}
        boundary={"provider":"upbit","sourceProvider":"upbit","symbol":"KRW-ETH","attestation":"UPBIT_WEBSOCKET","recoveryOnly":False,"promotionEligible":True,"evaluationKey":"eval","sourceArtifactHash":SHA,"rawHash":"d"*64,"closedBarHash":"c"*64,"startTime":now.isoformat(),"observedTime":now.isoformat(),"open":100000}
        binding={"deploymentId":"deployment","sourceContractHash":"e"*64,"marketObservationHash":"f"*64,"capitalScopeHash":"b"*64,"accountFingerprint":"a"*64,"asOf":now.isoformat(),"sourceSlippageRate":"0.002","actualFeeRate":"0.0005"}
        metadata={"broker_id":"upbit","order_type":"price","deployment_id":"deployment","runtime_evaluation_key":"eval","source_artifact_hash":SHA,"native_quantity_status":"READY","native_quantity_model":model,"native_open_boundary":boundary,"native_preflight":binding}
        intent=OrderIntent("source","CRYPTO","KRW-ETH","BUY",0.00012345,100200,"SMALL_LIVE","fixture",metadata)
        self.assertEqual("12.36969",ticket.native_upbit_quote_amount(intent))
        # Extract only the pure existing payload function: no state imports,
        # constructor, settings migration, broker adapter or request can execute.
        tree=ast.parse((base.APP/"live_trader/state.py").read_text(encoding="utf-8"))
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and "native_upbit_quote_amount" in ast.unparse(n))
        self.assertIn("native_upbit_quote_amount",ast.unparse(function))
        module=ast.Module(body=[function],type_ignores=[]);ast.fix_missing_locations(module)
        ns={"__name__":P+".state","__package__":P,"OrderIntent":OrderIntent,"Any":object,"safe_float":lambda v,default=0:float(v) if v is not None else default,"broker_id_from_symbol":lambda *args:"upbit"}
        exec(compile(module,"<isolated-native-payload>","exec"),ns)
        payload=ns[function.name](intent,idempotency_key="fixture")
        self.assertEqual("12.36969",payload["price"])
        self.assertIsInstance(payload["price"],str)
        self.assertEqual(Decimal(model["notional"]),Decimal(payload["price"]))

    def test_actual_controller_source_closed_open_quantity_ticket_e2e_without_order(self):
        s=spec();d,a,scope=deployment(s);reader=Reader(s);runtime=runtime_module.NativeSpotRuntime(self.root,"deployment",(s,),reader=reader,roots=[],deployment_reader=lambda *args:d,dispatch_reader=lambda:[])
        runtime.initialize_at_reviewed_manual_start()
        fake=types.ModuleType(P+".state");fake.STATE={"strategy_runner":{},"risk_settings":{"strategy_capital_limit_krw":1000},"dry_run":True};fake.append_audit=Mock();fake.asset_from_symbol=lambda symbol,**route:"CRYPTO";fake.FUNCTIONAL_TEST_EXECUTION_PURPOSE="FUNCTIONAL_TEST"
        with patch.dict(sys.modules,{P+".state":fake}),patch.object(package,"state",fake,create=True):
            module=importlib.import_module(P+".continuous_live");controller=module.LiveContinuousController(self.root);controller.mode="SMALL_LIVE";controller.profile_id="crypto";controller.deployment_id="deployment";controller.requested_strategy_id=s.strategy_id;controller.strategy_ids=(s.strategy_id,);controller.allowed_symbols=(s.symbol,);controller.supervisor=types.SimpleNamespace(engine=types.SimpleNamespace(specs=(s,)));controller.native_context_provider=runtime.context
            controller._dispatcher=Mock(return_value=contextlib.nullcontext());captured=[]
            controller._submit_cycle_intent=lambda intent,**kw:(captured.append(intent) or {"ok":True,"reason":"FIXTURE_PREFLIGHT_ONLY"})
            end=datetime.now(timezone.utc)-timedelta(seconds=1)
            staged=controller._handle_cycle_locked(types.SimpleNamespace(decisions=(decision(s,end),)))
            self.assertEqual("WAIT_OPEN_BOUNDARY",staged["results"][0]["action"]);self.assertEqual([],captured)
            result=controller._handle_native_open_boundary(open_event(s,end));self.assertTrue(result["results"][0]["ok"]);self.assertEqual(1,len(captured))
            intent=captured[0];self.assertEqual(100.2,intent.reference_price);self.assertEqual(100,intent.metadata["native_open_boundary"]["open"]);self.assertEqual("999",str(999))
            validated=ticket.validate_native_ticket(intent);self.assertFalse(validated["orderAuthorityGranted"])
            altered=types.SimpleNamespace(**vars(intent));altered.quantity=intent.quantity+0.001
            with self.assertRaises(ValueError):ticket.validate_native_ticket(altered)
            with self.assertRaises(ValueError):ticket.validate_native_ticket(intent,now=end+timedelta(seconds=31))
            self.assertEqual([],controller._handle_native_open_boundary(open_event(s,end))["results"])

if __name__=="__main__":unittest.main()
