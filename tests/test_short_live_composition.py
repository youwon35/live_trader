"""Synthetic integration through the real catalogue/service/adapter/router.

All HTTP is intercepted at urllib's owned opener. Environment keys, authority
readers, app lease, account truth and candles are test fixtures. No product
package __init__, real settings file, account, network or broker order is used.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from trading_runtime.artifact_governance import (
    seal_strategy_artifact, seal_strategy_instance, seal_portfolio_artifact,
    compute_strategy_artifact_hash, compute_strategy_instance_hash,
)
from trading_runtime.continuous_runtime import ClosedBar, OpenBoundary

ROOT=Path(__file__).resolve().parents[1] / "live_trader"
PKG="_short_live_composition_tests"
if PKG not in sys.modules:
    package=types.ModuleType(PKG); package.__path__=[str(ROOT)]; sys.modules[PKG]=package
catalog_module=importlib.import_module(PKG+".short_live_catalog")
session_module=importlib.import_module(PKG+".short_live_session")
adapter_module=importlib.import_module(PKG+".short_live_brokers")
protection_module=importlib.import_module(PKG+".futures_protection_transport")
router_module=importlib.import_module(PKG+".brokers")
native_module=importlib.import_module(PKG+".short_live_runtime")
authority=importlib.import_module(PKG+".short_live_order_authority")
adapters=importlib.import_module(PKG+".live_adapters")
emergency=importlib.import_module(PKG+".emergency_stop")
kis_authority=importlib.import_module(PKG+".kis_order_authority")
binance_authority=importlib.import_module(PKG+".binance_order_authority")
upbit_authority=importlib.import_module(PKG+".upbit_order_authority")
kis_credentials=importlib.import_module(PKG+".kis_domestic_functional_get_client")

NOW=datetime(2026,10,22,1,0,tzinfo=timezone.utc)
INFO={"KIS_KR_CASH":("kis","kis","003490","KR_STOCK"),
      "BINANCE_SPOT":("binance","binance","BTCUSDT","CRYPTO"),
      "BINANCE_USDM_FUTURES":("binance-futures","binance-futures","BTCUSDT.PERP","CRYPTO"),
      "UPBIT_KRW_SPOT":("upbit","upbit","KRW-BTC","CRYPTO")}


def stamp(at): return at.isoformat().replace("+00:00","Z")


def candle(spec,end,close):
    return ClosedBar(instrument_id=spec.instrument_id,symbol=spec.symbol,provider=spec.provider,timeframe="5m",
        start_time=stamp(end-timedelta(minutes=5)),end_time=stamp(end),open=close,high=close,low=close,close=close,
        volume=1,received_time=stamp(end+timedelta(milliseconds=100)))


class Response:
    status=200
    headers={}
    def __init__(self,url,body): self.url=url; self.raw=json.dumps(body).encode()
    def __enter__(self): return self
    def __exit__(self,*_): return False
    def geturl(self): return self.url
    def read(self,*_): return self.raw


class CandleSocketFixture:
    """Only the socket/history edge is replaced; NativeFeed remains real."""
    def __init__(self,specs):
        self.specs=tuple(specs); self.events=[]; self.connected=False; self.disconnected=False
        self.subscriptions=[]
    def connect(self): self.connected=True
    def disconnect(self): self.disconnected=True
    def mark_emitted(self,_): pass
    def warmup(self,subscription,limit):
        spec=next(row for row in self.specs if row.instrument_id==subscription.instrument_id and row.timeframe==subscription.timeframe)
        return self.history(spec,limit)
    def history(self,spec,limit):
        return [candle(spec,NOW-timedelta(minutes=5*(limit-index)),1000+limit-index) for index in range(limit)]
    def poll(self,_): events,self.events=self.events,[]; return events


class CompositionFixture:
    def __init__(self,root,route,*,count=1,portfolio=False,timing="signal-close"):
        self.root,self.route=root,route
        root.mkdir(parents=True)
        self.now=NOW; self.mono=10.0; self.alive=True
        self.calls=[]; self.mutations=[]; self.rows={}; self.position=Decimal(0)
        self.protection_mutations=[]; self.algos={}
        self.unknown_protection_read=False; self.protection_cancel_unconfirmed=False
        self.futures_policy={"leverage":5,"marginMode":"ISOLATED","stopLossPercent":2}
        self.quote_balance=Decimal("200000"); self.external_assets={}
        self.accept_only=False; self.lose_reply=False; self.unknown_cleanup=False; self.unknown_read=False
        self.error_1021=False; self.wire_defects=[]; self.wire_errors=[]
        self.symbol="003490" if route=="KIS_KR_CASH" else "KRW-BTC" if route=="UPBIT_KRW_SPOT" else "BTCUSDT"
        self.account="fixture-account-"+route
        self.currency=adapter_module.ROUTES[route][2]
        self.sockets=[]; self.adapters=[]; self.service=None
        self.catalog_root=root/"synthetic-canonical"; self.catalog_root.mkdir()
        (self.catalog_root/"strategy-instances").mkdir(); (self.catalog_root/"portfolios").mkdir()
        self.catalog=catalog_module.ShortLiveCatalogue(self.catalog_root)
        broker,provider,symbol,asset=INFO[route]
        members=[]
        for index in range(count):
            parameters={"shortMa":index+1,"longMa":index+3,"executionTiming":timing}
            source=seal_strategy_artifact({"id":f"fixture-source-{index}","strategy_id":f"fixture-source-{index}",
                "artifact_schema_version":"strategy-artifact-v2","name":"SYNTHETIC COMPOSITION FIXTURE ONLY",
                "asset":asset,"symbol":symbol,"timeframe":"5m","plugin":"moving_average_cross",
                "brokerId":broker,"marketDataProvider":provider,"parameters":parameters,
                "finalTest":{"status":"PASS"},"executionPurpose":"FUNCTIONAL_TEST",
                "promotionEligible":False,"useAsPromotionEvidence":False})
            instance=seal_strategy_instance({"schemaVersion":"strategy-instance-template-v1","instanceId":f"fixture-instance-{index}",
                "sourceStrategyId":source["id"],"sourceArtifactHash":compute_strategy_artifact_hash(source),
                "qualifiedSymbol":symbol,"qualifiedTimeframe":"5m","brokerId":broker,"marketDataProvider":provider,
                "asset":asset,"pluginId":"moving_average_cross","parameters":parameters})
            (self.catalog_root/(source["id"]+".json")).write_text(json.dumps(source),encoding="utf-8")
            (self.catalog_root/"strategy-instances"/(instance["instanceId"]+".json")).write_text(json.dumps(instance),encoding="utf-8")
            members.append({**instance,"templateInstanceId":instance["instanceId"],"strategyId":source["id"],
                "sourceInstanceHash":compute_strategy_instance_hash(instance),"allocation":{"normalizedWeight":1/count}})
        if portfolio:
            document=seal_portfolio_artifact({"id":"fixture-portfolio","schemaVersion":"portfolio-artifact-v1",
                "strategyInstances":members,"portfolio":{"strategyMode":"multi_strategy"},"promotionEligible":False})
            (self.catalog_root/"portfolios"/"fixture-portfolio.json").write_text(json.dumps(document),encoding="utf-8")
        kind="PORTFOLIO_INSTANCE" if portfolio else "STRATEGY_INSTANCE"
        rows=[row for row in self.catalog.snapshot()["candidates"] if row["kind"]==kind]
        self.selections=[{key:row[key] for key in ("selectionKey","artifactFileSha256","instanceFileSha256")} for row in rows]
        if not rows or any(row["blockers"] for row in rows): raise AssertionError(rows)
        self.budget=adapter_module.SharedExchangeBudget()
        def broker_factory(selected):
            if selected!=route: raise AssertionError(selected)
            router=router_module.LiveBrokerRouter()
            transport=adapter_module.OfficialBrokerTransport(router,enabled_reader=lambda:True,order_reader=self.read_order)
            protection_transport=protection_module.FuturesProtectionTransport(router,clock=lambda:self.now.timestamp())
            original_submit=transport.submit_authorized
            def submit_authorized(*args):
                try: return original_submit(*args)
                except Exception as exc:
                    self.wire_errors.append(type(exc).__name__+":"+str(exc)); raise
            transport.submit_authorized=submit_authorized
            edge=adapter_module.ShortLiveBrokerAdapter(route,snapshot_reader=self.snapshot,market_reader=self.market,
                transport=transport,authority_guard=lambda order:self.alive and self.service.dispatch_authorized(order),
                reserve_cleanup_order=lambda order:self.service.reserve_cleanup_order(order),
                reserve_cleanup_cancel=lambda order:self.service.reserve_cleanup_cancel(order),
                observe_cleanup_order=lambda client_id,result:self.service.observe_cleanup_order(client_id,result),
                protection_reader=lambda selected,claim:protection_transport.lookup(claim),
                protection_transport=protection_transport,
                force_order_reader=lambda symbol,since:{"known":True,"events":[],"observedAt":self.now.timestamp()},
                reserve_protection=lambda order:self.service.reserve_protection(order),
                record_protection=lambda event,detail:self.service.record_protection(event,detail),
                budget=self.budget,clock=lambda:self.now.timestamp())
            self.adapters.append(edge); return edge
        self.service=session_module.ShortLiveTestService(self.catalog,broker_factory,
            lambda manifest:native_module.NativeFeed(manifest,{}),root/"session-store",clock=lambda:self.now,
            monotonic=lambda:self.mono,clock_guard=lambda _:{"ready":True,"blockers":[]},
            owner_is_alive=lambda:self.alive,background=False)

    def snapshot(self,route,symbols):
        if route!=self.route or list(symbols)!=[self.symbol]: raise AssertionError((route,symbols))
        positions=[{"symbol":self.symbol,"quantity":str(self.position),"markPrice":"1000"}] if self.position else []
        if route=="BINANCE_USDM_FUTURES":
            for row in positions:
                row.update(entryPrice="1000",isolatedWallet=str(abs(self.position)*1000/self.futures_policy["leverage"]),
                           liquidationPrice="800",leverage=str(self.futures_policy["leverage"]),positionSide="BOTH")
        spot = route in {"BINANCE_SPOT", "UPBIT_KRW_SPOT"}
        locked_quote = sum((Decimal(row["quantity"])*Decimal("1000") for row in self.rows.values()
                            if row["status"]=="ACCEPTED" and row["side"]=="BUY"), Decimal(0)) if spot else Decimal(0)
        inventory = ({"spotInventoryComplete":True,
                      "spotInventory":{**self.external_assets, "BTC":str(self.position)},
                      "equityScope":"QUOTE_AND_SELECTED_SPOT"} if spot else {})
        # Synthetic official account truth: free quote excludes accepted buy
        # reservations; equity includes quote plus only selected spot units.
        available = self.quote_balance-locked_quote if spot else Decimal("200000")
        equity = self.quote_balance+self.position*Decimal("1000") if spot else Decimal("200000")
        return {"known":True,"ordersComplete":True,"positionsComplete":True,"accountId":self.account,
            "currency":self.currency,"observedAt":self.now.timestamp(),"availableNative":str(available),"equityNative":str(equity),
            **inventory,
            **({"openAlgoOrders":[deepcopy(row) for row in self.algos.values() if row["algoStatus"]=="NEW"]}
               if route=="BINANCE_USDM_FUTURES" else {}),
            "maximumOrderQuantities":{self.symbol:"200000"},"positions":positions,
            "openOrders":[deepcopy(row) for row in self.rows.values() if row["status"]=="ACCEPTED"]}

    def market(self,route,symbol):
        return {"known":True,"symbol":symbol,"nativePrice":"1000","quoteCurrency":self.currency,"krwPerQuote":"1",
            "fxObservedAt":self.now.timestamp(),"observedAt":self.now.timestamp(),"feeRate":"0.001","sellTaxRate":"0",
            "lotStep":"1","minQuantity":"1","minNotional":"1","priceTick":"1","sessionOpen":True,
            "marketQuantityStep":"1","marketMinQuantity":"1","marketMaxQuantity":"1000000",
            "sessionRemainingSeconds":19800,"marginMode":"ISOLATED","leverage":str(self.futures_policy["leverage"]),"positionMode":"ONE_WAY",
            "fundingRate":"0.0001","markPrice":"1000","isAutoAddMargin":False,"maxNotionalValue":"1000000",
            "leverageBrackets":[{"notionalFloor":"0","notionalCap":"1000000","initialLeverage":125,"maintMarginRatio":"0.004","cum":"0"}]}

    def read_order(self,route,claim):
        if self.unknown_read: return {"known":False}
        row=deepcopy(self.rows[claim["brokerOrderId"]]); row["observedAt"]=self.now.timestamp()
        return row

    def socket_factory(self,specs,**_):
        child=CandleSocketFixture(specs); self.sockets.append(child); return [child]

    def _fill(self,row):
        if row["status"]=="FILLED": return
        qty=Decimal(row["quantity"]); self.position+=qty if row["side"]=="BUY" else -qty
        if self.route in {"BINANCE_SPOT", "UPBIT_KRW_SPOT"}:
            self.quote_balance += (-qty if row["side"]=="BUY" else qty)*Decimal("1000")-Decimal("0.01")
        row.update(status="FILLED",filledQuantity=str(qty),remainingQuantity="0",feeNative="0.01")

    def fill_pending(self):
        for row in self.rows.values():
            if row["status"]=="ACCEPTED": self._fill(row)

    def open(self,request,**_):
        url=request.full_url; parsed=urlsplit(url); method=request.get_method(); query={k:v[0] for k,v in parse_qs(parsed.query).items()}
        body=json.loads(request.data) if request.data else {}
        self.calls.append((method,parsed.path))
        if method=="GET":
            if parsed.path.endswith("/exchangeInfo"):
                payload={"symbols":[{"symbol":"BTCUSDT","filters":[{"filterType":"LOT_SIZE","stepSize":"1","minQty":"1","maxQty":"1000000"},{"filterType":"MARKET_LOT_SIZE","stepSize":"1","minQty":"1","maxQty":"1000000"},{"filterType":"MIN_NOTIONAL","minNotional":"1"}]}]}
            elif parsed.path.endswith("/positionSide/dual"): payload={"dualSidePosition":False}
            elif parsed.path.endswith("/symbolConfig"): payload=[{"symbol":"BTCUSDT","marginType":"ISOLATED","leverage":self.futures_policy["leverage"]}]
            elif parsed.path=="/fapi/v1/algoOrder":
                if self.unknown_protection_read: raise TimeoutError("SYNTHETIC PROTECTION READ UNKNOWN")
                matches=[row for row in self.algos.values() if
                         (query.get("algoId") and str(row["algoId"])==query["algoId"]) or
                         (query.get("clientAlgoId") and row["clientAlgoId"]==query["clientAlgoId"])]
                if len(matches)!=1: raise AssertionError("Exact synthetic protection lookup required")
                payload=deepcopy(matches[0])
            elif parsed.path.endswith("/time"): payload={"serverTime":int(self.now.timestamp()*1000)}
            else: raise AssertionError("Unexpected fixture GET: "+parsed.path)
            return Response(url,payload)
        if parsed.path=="/oauth2/tokenP": return Response(url,{"access_token":"SYNTHETIC-ONLY-TOKEN","expires_in":1000})
        if parsed.path=="/fapi/v1/algoOrder":
            context=getattr(authority._LOCAL,"dispatch",None)
            if context is None: raise AssertionError("Protection request escaped exact short dispatch")
            order=deepcopy(context["order"])
            self.protection_mutations.append({"method":method,"path":parsed.path,"query":query,"order":order})
            if method=="POST":
                if (order["operation"]!="PROTECT" or query.get("clientAlgoId")!=order["clientAlgoId"]
                        or query.get("type")!="STOP_MARKET" or query.get("workingType")!="MARK_PRICE"
                        or query.get("closePosition")!="true" or query.get("priceProtect")!="false"
                        or "quantity" in query or "reduceOnly" in query):
                    raise AssertionError("Exact close-all protection wire terms required")
                oid=str(1000+len(self.algos))
                row={"algoId":oid,"clientAlgoId":query["clientAlgoId"],"symbol":query["symbol"],
                     "side":query["side"],"positionSide":query["positionSide"],"orderType":"STOP_MARKET",
                     "algoType":"CONDITIONAL","closePosition":True,"workingType":"MARK_PRICE","priceProtect":False,
                     "triggerPrice":query["triggerPrice"],"algoStatus":"NEW","actualOrderId":""}
                self.algos[oid]=row
            elif method=="DELETE":
                if order["operation"]!="CANCEL_PROTECTION" or query.get("algoId")!=order["algoId"]:
                    raise AssertionError("Exact owned protection cancellation required")
                row=self.algos[query["algoId"]]
                if not self.protection_cancel_unconfirmed: row["algoStatus"]="CANCELED"
            else: raise AssertionError("Unexpected protection mutation")
            return Response(url,deepcopy(row))
        if parsed.path not in {"/api/v3/order","/fapi/v1/order","/v1/orders","/uapi/domestic-stock/v1/trading/order-cash"}:
            raise AssertionError("Unexpected synthetic mutation URL: "+parsed.path)
        context=getattr(authority._LOCAL,"dispatch",None)
        if context is None: raise AssertionError("Normal HTTP request escaped exact short dispatch")
        order=deepcopy(context["order"])
        self.mutations.append({"method":method,"path":parsed.path,"query":query,"body":body,"order":order})
        if self.error_1021 and parsed.path=="/api/v3/order": return Response(url,{"code":-1021,"msg":"synthetic timestamp rejection"})
        if self.route.startswith("BINANCE") and query.get("newClientOrderId")!=order["clientOrderId"]:
            self.wire_defects.append("Exact Binance newClientOrderId missing from physical request")
        if self.route=="KIS_KR_CASH" and (body.get("ORD_DVSN")!="00" or body.get("PDNO")!=self.symbol):
            self.wire_defects.append("KIS domestic limit code/symbol mismatch")
        if self.route=="UPBIT_KRW_SPOT" and (body.get("identifier")!=order["clientOrderId"] or body.get("ord_type")!="limit"):
            self.wire_defects.append("Upbit exact identifier/limit type missing")
        if self.lose_reply or (self.unknown_cleanup and order["riskReducing"]): raise TimeoutError("SYNTHETIC LOST REPLY")
        oid=str(len(self.mutations))
        row={"known":True,"brokerOrderId":oid,"clientOrderId":order["clientOrderId"],"symbol":self.symbol,
            "status":"ACCEPTED","side":order["side"],"positionSide":"BOTH","quantity":str(order["quantity"]),"filledQuantity":"0",
            "remainingQuantity":str(order["quantity"]),"fillPrice":"1000","feeNative":"0",
            "baseCommissionKnown":True,"baseCommissionQuantity":"0","observedAt":self.now.timestamp(),
            "orderDate":"20261022","organizationNo":"00001"}
        self.rows[oid]=row
        if not self.accept_only or order["riskReducing"]: self._fill(row)
        if self.route=="KIS_KR_CASH": payload={"rt_cd":"0","output":{"ODNO":oid,"KRX_FWDG_ORD_ORGNO":"00001"}}
        elif self.route=="UPBIT_KRW_SPOT": payload={"uuid":oid,"identifier":order["clientOrderId"],"state":"wait","executed_volume":row["filledQuantity"],"price":"1000"}
        else: payload={"orderId":oid,"clientOrderId":order["clientOrderId"],"status":row["status"],"executedQty":row["filledQuantity"],"avgPrice":"1000"}
        return Response(url,payload)

    def prepare(self):
        return self.service.prepare({"selections":self.selections,"durationHours":5,"capitalKrwPerBroker":100000,"dailyLossStopKrwPerBroker":50000,
            **({"futuresPolicy":deepcopy(self.futures_policy)} if self.route=="BINANCE_USDM_FUTURES" else {})})["prepared"]

    def start(self):
        prepared=self.prepare()
        if not prepared["readyToStart"]: raise AssertionError(prepared["blockers"])
        return self.service.start({"preparationId":prepared["preparationId"],"scopeHash":prepared["scopeHash"],"confirmed":True,
            "confirmationText":"실제 주문 시험 시작"})

    def forward(self):
        self.now=NOW+timedelta(milliseconds=200); self.mono+=0.2
        child=self.sockets[-1]; child.events=[candle(child.specs[0],NOW,1006)]
        return self.service.tick()


class ShortLiveCompositionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name); self.fixture=None
        self.environ={"KIS_APP_KEY":"SYNTHETIC-ONLY-KEY","KIS_APP_SECRET":"SYNTHETIC-ONLY-SECRET",
            "KIS_ACCOUNT_NO":"12345678","KIS_ACCOUNT_PRODUCT_CODE":"01","KIS_ENV":"real","LIVE_TRADER_ENABLE_REAL_ORDERS":"true",
            "BINANCE_API_KEY":"SYNTHETIC-ONLY-KEY","BINANCE_API_SECRET":"SYNTHETIC-ONLY-SECRET",
            "UPBIT_ACCESS_KEY":"SYNTHETIC-ONLY-KEY","UPBIT_SECRET_KEY":"SYNTHETIC-ONLY-SECRET"}
        self.enter(patch.dict(os.environ,self.environ,clear=True))
        self.enter(patch.object(emergency,"emergency_stop_status",return_value={"active":False,"durable":True,"revision":"fixture-revision"}))
        authority._reset_for_tests(); self.addCleanup(authority._reset_for_tests)
        for mod in (binance_authority,upbit_authority):
            self.enter(patch.object(mod,"_AUTHORITY_READER",lambda:{"functionalAuthorityOpen":False,"functionalPhase":"IDLE",
                "functionalRevision":0,"functionalSessionId":"","functionalAccountFingerprint":"",
                "applicationInstanceLeaseHeld":True,"ordinaryRoutesClosed":False,"ordinaryMutationEnabled":True}))
        account=kis_credentials.kis_domestic_functional_account_fingerprint("12345678","01")
        credential=kis_credentials._credential_configuration_hash(app_key=self.environ["KIS_APP_KEY"],app_secret=self.environ["KIS_APP_SECRET"],account_fingerprint=account)
        self.enter(patch.object(kis_authority,"_AUTHORITY_READER",lambda:{"durableAuthorityReadable":True,"functionalAuthorityOpen":False,
            "functionalPhase":"IDLE","functionalRevision":0,"stateRevision":1,"ownerEpochId":"fixture-owner","ownerEpochHash":"e"*64,
            "functionalSessionId":"","functionalAccountFingerprint":account,"credentialConfigurationHash":credential,"functionalMutationIntent":{},
            "killOrdinaryCancelAllowed":False,"killOrdinaryCancelRevision":0,"killOrdinaryCancelIntent":{},"applicationInstanceLeaseHeld":True,
            "ordinaryRoutesClosed":False,"controlReservation":{}}))
        self.enter(patch("socket.create_connection",side_effect=AssertionError("Network forbidden in composition fixture")))
        self.enter(patch.object(adapters.urllib.request,"build_opener",side_effect=lambda *_:types.SimpleNamespace(open=lambda request,**kw:self.fixture.open(request,**kw))))
        self.enter(patch.object(native_module,"feeds_for_specs",side_effect=lambda specs,**kw:self.fixture.socket_factory(specs,**kw)))
        history=importlib.import_module(PKG+".short_live_kis_history")
        self.enter(patch.object(history,"get_warmup",side_effect=lambda spec,limit,_:CandleSocketFixture((spec,)).history(spec,limit)))
        adapters._BINANCE_SYMBOL_RULE_CACHE.clear(); adapters._clear_binance_time_offset_cache(); adapters._clear_kis_access_token_cache()

    def enter(self,manager): value=manager.start(); self.addCleanup(manager.stop); return value

    def fixture_for(self,route,**kwargs):
        if self.fixture:
            self.fixture.service.close(); authority._reset_for_tests()
        self.fixture=CompositionFixture(self.root/str(len(list(self.root.iterdir()))),route,**kwargs)
        self.addCleanup(self.fixture.service.close); return self.fixture

    def test_four_routes_single_and_multi_use_real_adapter_router_and_owned_cleanup(self):
        for route in INFO:
            for count,portfolio in ((1,False),(2,False),(1,True),(2,True)):
                with self.subTest(route=route,count=count,portfolio=portfolio):
                    f=self.fixture_for(route,count=count,portfolio=portfolio)
                    prepared=f.prepare(); self.assertEqual([],f.mutations); self.assertEqual([],f.protection_mutations)
                    if route=="BINANCE_USDM_FUTURES":
                        self.assertEqual(5,prepared["futuresPolicy"]["leverage"])
                        self.assertEqual("MARK_PRICE",prepared["futuresPolicy"]["workingType"])
                        self.assertTrue(prepared["futuresPolicy"]["closePosition"])
                    f.start(); self.assertEqual([],f.mutations); self.assertEqual([],f.protection_mutations)
                    self.assertTrue(f.sockets[-1].connected)
                    view=f.forward()["runtime"]
                    self.assertEqual("RUNNING",view["status"],(view,f.calls,f.wire_errors))
                    self.assertEqual(1,view["orderCount"],view)
                    self.assertEqual(1,view["fillCount"],view)
                    self.assertEqual([],f.wire_defects)
                    self.assertTrue(all(s["decisionCount"]==1 for s in view["strategyRows"]))
                    if route=="BINANCE_USDM_FUTURES":
                        stop_index=f.calls.index(("POST","/fapi/v1/algoOrder"))
                        entry_index=f.calls.index(("POST","/fapi/v1/order"))
                        self.assertLess(stop_index,entry_index)
                        self.assertIn(("GET","/fapi/v1/algoOrder"),f.calls[stop_index+1:entry_index])
                        self.assertEqual(1,len(f.protection_mutations))
                        row=view["brokerRows"][0]
                        self.assertGreater(row["notionalExposureKrw"],row["committedCapitalKrw"])
                        self.assertEqual("NEW",row["protections"][0]["status"])
                        self.assertTrue(row["protections"][0]["verified"])
                    terminal=f.service.stop({"reason":"SYNTHETIC TEST COMPLETE"})["runtime"]
                    self.assertTrue(terminal["terminalVerified"],terminal)
                    self.assertEqual(Decimal(0),f.position)
                    self.assertEqual(2,len(f.mutations))
                    self.assertFalse(f.mutations[0]["order"]["riskReducing"])
                    self.assertTrue(f.mutations[-1]["order"]["riskReducing"])
                    self.assertTrue(f.sockets[-1].disconnected)
                    if route=="BINANCE_USDM_FUTURES":
                        self.assertEqual(["POST","DELETE"],[row["method"] for row in f.protection_mutations])
                        self.assertTrue(all(row["algoStatus"]=="CANCELED" for row in f.algos.values()))
                        self.assertTrue(f.service._session["brokerRows"][route]["protectionRetirementVerified"])
                        self.assertFalse(f.service._session["brokerRows"][route]["ownedProtectionOrders"])
                        self.assertTrue(f.service._session["sleeves"])
                        self.assertTrue(all(row["quantity"]==0 for row in f.service._session["sleeves"].values()),
                                        "Actual cleanup fills were not attributed back to every futures sleeve")

    def test_actual_accepted_observation_is_not_inferred_as_fill_or_reposted(self):
        f=self.fixture_for("BINANCE_SPOT"); f.accept_only=True; f.start()
        pending=f.forward()["runtime"]
        self.assertEqual(1,len(f.mutations)); self.assertEqual(0,pending["fillCount"])
        f.service.tick(); self.assertEqual(1,len(f.mutations))
        f.fill_pending(); observed=f.service.tick()["runtime"]
        self.assertEqual(1,observed["fillCount"],observed); self.assertEqual(1,len(f.mutations))
        self.assertTrue(f.service.stop({})["runtime"]["terminalVerified"])

    def test_futures_unconfirmed_protection_never_submits_entry_or_retries_stop(self):
        f=self.fixture_for("BINANCE_USDM_FUTURES"); f.start(); f.unknown_protection_read=True
        view=f.forward()["runtime"]
        self.assertEqual([],f.mutations,"An entry reached the router without an exact protection read")
        self.assertEqual(["POST"],[row["method"] for row in f.protection_mutations])
        self.assertTrue(view["newEntriesBlocked"]); self.assertFalse(view["terminalVerified"])
        f.service.tick(); f.service.stop({})
        self.assertEqual([],f.mutations)
        self.assertEqual(["POST"],[row["method"] for row in f.protection_mutations])
        self.assertFalse(f.service.snapshot()["runtime"]["terminalVerified"])

    def test_futures_cancel_ack_without_retirement_never_claims_terminal_or_reposts(self):
        f=self.fixture_for("BINANCE_USDM_FUTURES"); f.start(); f.forward()
        f.protection_cancel_unconfirmed=True
        view=f.service.stop({})["runtime"]
        self.assertEqual(Decimal(0),f.position,"Owned position was not closed by the real reducing path")
        self.assertTrue(all(row["quantity"]==0 for row in f.service._session["sleeves"].values()),
                        "Unknown stop retirement must not discard confirmed position-close fills")
        self.assertEqual(["POST","DELETE"],[row["method"] for row in f.protection_mutations])
        self.assertTrue(view["newEntriesBlocked"]); self.assertFalse(view["terminalVerified"])
        self.assertTrue(all(row["algoStatus"]=="NEW" for row in f.algos.values()))
        f.service.tick(); f.service.stop({})
        self.assertEqual(["POST","DELETE"],[row["method"] for row in f.protection_mutations])
        self.assertEqual(2,len(f.mutations),"A cleanup order was resubmitted while stop retirement remained unknown")
        self.assertFalse(f.service.snapshot()["runtime"]["terminalVerified"])

    def test_futures_finished_algo_with_unknown_child_never_claims_flat_terminal(self):
        f=self.fixture_for("BINANCE_USDM_FUTURES"); f.start(); f.forward()
        # The exact algo ID reports its real child ID, but no child fill/fee
        # observation is supplied. FINISHED alone cannot prove child retirement.
        algo=next(iter(f.algos.values()))
        algo.update(algoStatus="FINISHED",actualOrderId="777")
        view=f.service.stop({})["runtime"]
        self.assertEqual(Decimal(0),f.position,"The independently confirmed owned close did not complete")
        self.assertTrue(all(row["quantity"]==0 for row in f.service._session["sleeves"].values()))
        self.assertTrue(view["newEntriesBlocked"]); self.assertFalse(view["terminalVerified"])
        self.assertFalse(f.service._session["brokerRows"][f.route]["protectionRetirementVerified"])
        self.assertEqual([],f.service._session["brokerRows"][f.route]["ownedProtectionOrders"])
        attempts=(len(f.mutations),len(f.protection_mutations))
        f.service.tick(); f.service.stop({})
        self.assertEqual(attempts,(len(f.mutations),len(f.protection_mutations)))
        self.assertFalse(f.service.snapshot()["runtime"]["terminalVerified"])

    def test_complete_spot_inventory_preserves_preexisting_units_through_owned_round_trip(self):
        for route in ("BINANCE_SPOT", "UPBIT_KRW_SPOT"):
            with self.subTest(route=route):
                f=self.fixture_for(route)
                f.position=Decimal("3"); f.external_assets={"ETH":"2"}
                prepared=f.prepare()
                self.assertTrue(prepared["readyToStart"])
                self.assertEqual(["BTC", "ETH"], prepared["brokerReadiness"][0]["preservedAssetSymbols"])
                f.start(); view=f.forward()["runtime"]
                self.assertEqual("RUNNING", view["status"], view)
                self.assertEqual(1, view["fillCount"])
                terminal=f.service.stop({})["runtime"]
                self.assertTrue(terminal["terminalVerified"], terminal)
                self.assertEqual(Decimal("3"), f.position)
                self.assertEqual({"ETH":"2"}, f.external_assets)
                self.assertEqual(Decimal("199999.98"), f.quote_balance)

    def test_source_next_open_waits_for_actual_native_boundary_and_deduplicates(self):
        f=self.fixture_for("BINANCE_SPOT",timing="next-open"); f.start()
        f.forward(); self.assertEqual([],f.mutations)
        spec=f.sockets[-1].specs[0]
        opened=OpenBoundary(instrument_id=spec.instrument_id,symbol=spec.symbol,provider=spec.provider,timeframe="5m",
            start_time=stamp(NOW),open=1007,observed_time=stamp(NOW+timedelta(milliseconds=100)),
            raw_hash="c"*64,attestation="BINANCE_WEBSOCKET",source_sequence="synthetic-native-event",promotion_eligible=False)
        f.sockets[-1].events=[opened]; view=f.service.tick()["runtime"]
        self.assertEqual(1,len(f.mutations)); self.assertEqual(1,view["fillCount"],view)
        f.sockets[-1].events=[opened]; f.service.tick(); self.assertEqual(1,len(f.mutations))
        self.assertTrue(f.service.stop({})["runtime"]["terminalVerified"])

    def test_missing_manual_confirmation_never_reaches_router(self):
        f=self.fixture_for("UPBIT_KRW_SPOT"); prepared=f.prepare()
        with self.assertRaisesRegex(session_module.ShortLiveContractError,"MANUAL"):
            f.service.start({"preparationId":prepared["preparationId"],"scopeHash":prepared["scopeHash"],"confirmed":True})
        self.assertEqual([],f.mutations); self.assertEqual([],f.sockets)

    def test_timestamp_rejected_short_post_is_not_automatically_retried(self):
        f=self.fixture_for("BINANCE_SPOT"); f.error_1021=True; f.start(); f.forward()
        self.assertEqual(1,len(f.mutations),"A rejected short-functional POST was automatically replayed")

    def test_unknown_cleanup_is_not_retried_or_declared_terminal(self):
        f=self.fixture_for("UPBIT_KRW_SPOT"); f.start(); f.forward(); f.unknown_cleanup=True
        view=f.service.stop({})["runtime"]
        self.assertFalse(view["terminalVerified"]); self.assertTrue(view["newEntriesBlocked"])
        attempts=len(f.mutations); f.service.tick(); f.service.stop({})
        self.assertEqual(attempts,len(f.mutations)); self.assertGreater(f.position,0)


if __name__=="__main__": unittest.main()
