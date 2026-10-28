"""Execute actual guard AST without importing app startup, env, or networking."""
import ast
import math
from pathlib import Path
import socket
import unittest

# The AST only receives injected mathematical helpers and fake readers.
source=Path(__file__).resolve().parents[1]/"live_trader/brokers.py"
tree=ast.parse(source.read_text(encoding="utf-8-sig"))
number=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=="_strict_finite_number")
router=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=="LiveBrokerRouter")
guard=next(n for n in router.body if isinstance(n,ast.FunctionDef) and n.name=="_verify_futures_entry_bracket")
class Error(RuntimeError): pass
namespace={"math":math,"BrokerNotReadyError":Error,"MAX_SAFE_LEVERAGE_INTEGER":9007199254740991}
exec(compile(ast.fix_missing_locations(ast.Module(body=[number,guard],type_ignores=[])),str(source),"exec"),namespace)

class FinalEntryGuardTests(unittest.TestCase):
    def fixture(self):
        class Fake:
            def get_binance_futures_risk_inputs(self,symbol,**kwargs):
                self.calls.append((symbol,kwargs))
                if self.fail: raise Error("fresh bracket unavailable")
                return dict(self.market)
        fake=Fake();fake.calls=[];fake.fail=False
        fake.market={"symbol":"BTCUSDT","initial_leverage_limit":75,"combined_notional_usdt":120,
            "notional_floor":100,"notional_cap":1000}
        return fake,{"symbol":"BTCUSDT","quantity":"1","price":"100","position_direction":"short","side":"SELL"},{"leverage":37,"max_notional":1000}
    def apply(self,fake,intent,config): return namespace["_verify_futures_entry_bracket"](fake,intent,config)
    def test_actual_nonpreset_integer_within_tier_is_allowed(self):
        f,i,c=self.fixture();self.apply(f,i,c)
        self.assertEqual(f.calls,[("BTCUSDT",{"notional_usdt":100.0,"requested_quantity":1.0})])
    def test_exchange_limit_cannot_be_replaced_by_artifact_policy(self):
        f,i,c=self.fixture();i["max_leverage"]=1000;c["leverage"]=76
        with self.assertRaises(Error):self.apply(f,i,c)
    def test_missing_or_malformed_or_wrong_symbol_bracket_blocks(self):
        for change in ({"initial_leverage_limit":None},{"symbol":"ETHUSDT"},{"notional_cap":119},{"initial_leverage_limit":37.5}):
            f,i,c=self.fixture();f.market.update(change)
            with self.subTest(change=change),self.assertRaises(Error):self.apply(f,i,c)
    def test_fresh_lookup_failure_blocks_new_risk(self):
        f,i,c=self.fixture();f.fail=True
        with self.assertRaises(Error):self.apply(f,i,c)
    def test_account_max_notional_is_retained(self):
        f,i,c=self.fixture();c["max_notional"]=119
        with self.assertRaises(Error):self.apply(f,i,c)
    def test_reduction_does_not_depend_on_new_entry_tier_lookup(self):
        f,i,c=self.fixture();f.fail=True;i["risk_reducing"]=True
        self.apply(f,i,c);self.assertEqual(f.calls,[])
    def test_final_place_edge_calls_guard_before_send(self):
        method=next(n for n in router.body if isinstance(n,ast.FunctionDef) and n.name=="place_order")
        branch=next(n for n in method.body if isinstance(n,ast.If) and ast.unparse(n.test)=="broker_id == 'binance-futures'")
        calls=[n for n in ast.walk(branch) if isinstance(n,ast.Call)]
        guard_line=next(n.lineno for n in calls if ast.unparse(n.func)=="self._verify_futures_entry_bracket")
        post_line=max(n.lineno for n in calls if ast.unparse(n.func)=="send_binance_signed_request")
        self.assertLess(guard_line,post_line)

if __name__=="__main__":unittest.main(verbosity=2)
