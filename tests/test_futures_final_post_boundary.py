"""Actual final router/risk/wire source with fake HTTP, no app imports."""
import ast
from contextlib import nullcontext
import math
from datetime import datetime,timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import socket
import sys
import types
import unittest

ROOT=Path(__file__).resolve().parents[1]/"live_trader"
# Only injected fake HTTP is bound into the AST execution namespace.
sys.path.insert(0,str(ROOT.parents[2]/"packages/trading_runtime"))
from trading_runtime import compare_futures_policy_to_broker
class BoundaryError(RuntimeError):pass
package=types.ModuleType("_offline_final_futures_boundary");package.__path__=[]
authority=types.ModuleType(package.__name__+".short_live_order_authority")
for name in ("validate_short_live_native_payload","validate_short_live_normalized_payload","validate_short_live_prepared_request"):
    setattr(authority,name,lambda *a,**k:None)
authority.short_live_dispatch_present=lambda:False
sys.modules[package.__name__]=package;sys.modules[authority.__name__]=authority
tree=ast.parse((ROOT/"brokers.py").read_text(encoding="utf-8-sig"))
router=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=="LiveBrokerRouter")
functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in
    {"_strict_finite_number","_existing_futures_position_notional","_select_futures_maintenance_bracket","validate_binance_futures_execution_policy"}]
selected=[n for n in router.body if isinstance(n,ast.FunctionDef) and n.name in
    {"place_order","get_binance_futures_risk_inputs","_verify_futures_entry_bracket","_verify_futures_exit_position"}]
wrapper=ast.ClassDef(name="ActualRouter",bases=[],keywords=[],decorator_list=[],body=selected)
wire_tree=ast.parse((ROOT/"live_adapters.py").read_text(encoding="utf-8-sig"))
wire=next(n for n in wire_tree.body if isinstance(n,ast.FunctionDef) and n.name=="build_binance_futures_order_request")
ns={"__package__":package.__name__,"math":math,"BrokerNotReadyError":BoundaryError,
    "BinanceOrderAuthorityError":BoundaryError,"MAX_SAFE_LEVERAGE_INTEGER":9007199254740991,
    "PreparedRequest":dict,"Callable":object,"Any":object,
    "real_orders_enabled":lambda:True,"normalize_binance_futures_intent":lambda value:dict(value),
    "compare_futures_policy_to_broker":compare_futures_policy_to_broker,"datetime":datetime,"timezone":timezone,"Decimal":Decimal,"InvalidOperation":InvalidOperation,
    "normalize_binance_futures_symbol_config":lambda payload,symbol:payload,
    "ordinary_binance_final_mutation_boundary":lambda **k:nullcontext(),
    "ensure_response_ok":lambda provider,response:response["json"],
    "numeric_value":lambda value,default=0:float(value) if value is not None else default,
    "normalize_side":lambda value:str(value).upper(),"normalize_decimal_text":lambda value:str(value),
    "BINANCE_FUTURES_ORDER_ENDPOINT":"order","BINANCE_FUTURES_TEST_ORDER_ENDPOINT":"order-test",
    "_build_binance_futures_signed_request":lambda method,endpoint,query,**kw:{"method":method,"endpoint":endpoint,"query":query}}
exec(compile(ast.fix_missing_locations(ast.Module(body=functions+[wire,wrapper],type_ignores=[])),"actual-final-boundary-ast","exec"),ns)
for name,path in (("build_binance_futures_position_mode_request","mode"),("build_binance_futures_positions_request","positions")):
    ns[name]=(lambda endpoint=path:{"method":"GET","endpoint":endpoint})
for name,path in (("build_binance_futures_symbol_config_request","config"),("build_binance_futures_premium_index_request","premium"),
                  ("build_binance_futures_leverage_bracket_request","brackets"),("build_binance_futures_commission_rate_request","commission")):
    ns[name]=(lambda symbol,endpoint=path:{"method":"GET","endpoint":endpoint,"symbol":symbol})

class FinalFakePostTests(unittest.TestCase):
    def setUp(self):
        self.posts=[];self.reads=[];self.multiplier=3;self.maximum=150;self.hedge=True
        self.mark=100;self.premium_override={};self.commission_override={};self.position=0;self.position_side="SHORT";self.bracket_symbol="BTCUSDT";self.cap=1000
        self.intent={"broker_id":"binance-futures","symbol":"BTCUSDT","quantity":"1","price":"100",
            "side":"SELL","position_direction":"short","max_leverage":3,"required_margin_type":"ISOLATED","risk_reducing":False,"order_type":"LIMIT","identifier":"fake-exact"}
        ns["send_binance_signed_request"]=lambda builder,**kw:self.http(builder())
        ns["send_prepared_request"]=self.http
        self.router=ns["ActualRouter"]()
    def http(self,request):
        path=request["endpoint"]
        if request["method"]=="POST":self.posts.append(request);return {"json":{"orderId":"synthetic-1"}}
        self.reads.append(path)
        values={"mode":{"dualSidePosition":self.hedge},"config":{"leverage":self.multiplier,"margin_type":"ISOLATED","max_notional":1000},
            "positions":[{"symbol":"BTCUSDT","positionSide":self.position_side if self.hedge else "BOTH",
                "positionAmt":str(self.position),"notional":str(self.position*self.mark),"markPrice":str(self.mark)}],
            "premium":{"symbol":"BTCUSDT","markPrice":str(self.mark),"indexPrice":str(self.mark),"lastFundingRate":"0", "time":int(datetime.now(timezone.utc).timestamp()*1000), "nextFundingTime":int(datetime.now(timezone.utc).timestamp()*1000)+3600000, **self.premium_override},
            "brackets":[{"symbol":self.bracket_symbol,"brackets":[{"notionalFloor":0,"notionalCap":self.cap,
                "initialLeverage":self.maximum,"maintMarginRatio":".001","cum":0}]}],
            "commission":{"symbol":"BTCUSDT","makerCommissionRate":".0001","takerCommissionRate":".0001", **self.commission_override}}
        return {"json":values[path]}
    def test_actual_policy_admitted_three_reaches_single_fake_post_with_exact_hedge_short_wire(self):
        self.router.place_order(self.intent)
        self.assertEqual(len(self.posts),1)
        self.assertEqual(self.posts[0]["query"]["positionSide"],"SHORT")
        self.assertEqual(self.posts[0]["query"]["side"],"SELL")
        self.assertNotIn("reduceOnly",self.posts[0]["query"])
        self.assertIn("brackets",self.reads);self.assertIn("positions",self.reads)
    def test_actual_shared_validator_permits_explicit_nonpreset_integers_only_inside_actual_tier(self):
        for multiplier in (7,17,37,125,150):
            self.setUp();self.multiplier=multiplier;self.intent["max_leverage"]=multiplier
            self.router.place_order(self.intent)
            self.assertEqual(len(self.posts),1)
            self.assertEqual(self.posts[0]["query"]["positionSide"],"SHORT")
        self.setUp();self.multiplier=151;self.intent["max_leverage"]=151
        with self.assertRaises(BoundaryError):self.router.place_order(self.intent)
        self.assertEqual(self.posts,[])
    def test_ordinary_new_entry_direction_mismatch_cannot_open_opposite_position(self):
        self.intent["position_direction"]="long"
        with self.assertRaises(BoundaryError):self.router.place_order(self.intent)
        self.assertEqual(self.posts,[])
    def test_missing_wrong_symbol_or_stale_cost_observations_never_become_zero_or_fresh(self):
        for premium,commission in (({"symbol":"ETHUSDT"},{}),({"time":1},{}),({"lastFundingRate":None},{}),
                                   ({},{"symbol":"ETHUSDT"}),({},{"takerCommissionRate":None}),({},{"makerCommissionRate":None})):
            self.setUp();self.premium_override=premium;self.commission_override=commission
            with self.subTest(premium=premium,commission=commission),self.assertRaises(BoundaryError):self.router.place_order(self.intent)
            self.assertEqual(self.posts,[])
    def test_tier_limit_and_wrong_symbol_prevent_any_fake_post(self):
        for field,value in (("maximum",2),("bracket_symbol","ETHUSDT")):
            self.setUp();setattr(self,field,value)
            with self.subTest(field=field),self.assertRaises(BoundaryError):self.router.place_order(self.intent)
            self.assertEqual(self.posts,[])
    def test_fresh_mark_and_opposite_existing_gross_notional_enforce_current_tier(self):
        self.cap=110;self.mark=120
        with self.assertRaises(BoundaryError):self.router.place_order(self.intent)
        self.assertEqual(self.posts,[])
        self.setUp();self.cap=300;self.position=2;self.position_side="LONG"
        with self.assertRaises(BoundaryError):self.router.place_order(self.intent)
        self.assertEqual(self.posts,[])
    def test_reduce_flag_cannot_bypass_entry_guard_as_opening_or_oversized_order(self):
        for side,quantity in (("SELL","1"),("BUY","3")):
            self.setUp();self.position=-2;self.intent.update(risk_reducing=True,side=side,quantity=quantity)
            with self.subTest(side=side),self.assertRaises(BoundaryError):self.router.place_order(self.intent)
            self.assertEqual(self.posts,[])
    def test_exact_short_cover_uses_observed_position_and_correct_oneway_or_hedge_wire(self):
        for hedge in (False,True):
            self.setUp();self.hedge=hedge;self.position=-2
            self.intent.update(risk_reducing=True,side="BUY")
            self.router.place_order(self.intent)
            self.assertEqual(len(self.posts),1)
            query=self.posts[0]["query"]
            self.assertEqual(query["positionSide"],"SHORT" if hedge else "BOTH")
            if hedge:self.assertNotIn("reduceOnly",query)
            else:self.assertEqual(query["reduceOnly"],"true")
            self.assertNotIn("brackets",self.reads)

if __name__=="__main__":unittest.main(verbosity=2)
