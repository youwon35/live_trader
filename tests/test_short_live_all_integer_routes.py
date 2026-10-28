"""Synthetic symbol limits and explicit route lifecycles; no app/config/API."""
import copy
import importlib
import socket
import unittest
from unittest.mock import patch

import test_short_live_brokers as base
import test_short_live_broker_readers as reader_tests
import test_short_live_session as session

adapters=importlib.import_module(base.PKG+".live_adapters")
contract=importlib.import_module(base.PKG+".short_live_contract")

class AllIntegerFuturesTests(unittest.TestCase):
    def setUp(self):
        self.network=patch.object(socket.socket,"connect",side_effect=AssertionError("NETWORK_FORBIDDEN"))
        self.network.start();self.addCleanup(self.network.stop)

    def futures(self,multiplier,direction,mode):
        f=base.Fixture("BINANCE_USDM_FUTURES")
        f.manifest["futuresPolicy"].update(leverage=multiplier,stopLossPercent="0.01")
        f.manifest["specs"]=[{"symbol":f.symbol,"positionDirection":direction,"targetWeight":1}]
        f.quote.update(leverage=str(multiplier),positionMode=mode,feeRate="0.0001",fundingRate="0")
        f.quote["leverageBrackets"][0].update(initialLeverage="150",maintMarginRatio=".001")
        original=f.adapter.transport.read_order
        def exact_read(route,claim):
            row=original(route,claim)
            row["positionSide"]=direction.upper() if mode=="HEDGE" else "BOTH"
            return row
        f.adapter.transport.read_order=exact_read
        return f

    def test_currently_admitted_integers_long_short_oneway_hedge_round_trip(self):
        for multiplier in (1,2,3,5,7,17,37,75,125,150):
            for direction in ("long","short"):
                for mode in ("ONE_WAY","HEDGE"):
                    with self.subTest(multiplier=multiplier,direction=direction,mode=mode):
                        f=self.futures(multiplier,direction,mode)
                        ready=f.adapter.readiness(f.manifest);self.assertTrue(ready["ready"],ready)
                        side="BUY" if direction=="long" else "SELL"
                        sized=f.adapter.size_order(f.proposal("10000",side=side,direction=direction))
                        self.assertTrue(sized["ready"],sized)
                        entry=f.adapter.submit(sized);self.assertEqual(entry["status"],"FILLED",entry)
                        native=f.calls[0];self.assertFalse(native["risk_reducing"])
                        self.assertEqual(native["expected_leverage"],str(multiplier))
                        prepared=adapters.build_binance_futures_order_request(native,hedge_mode=mode=="HEDGE")
                        self.assertEqual(prepared.query["positionSide"],direction.upper() if mode=="HEDGE" else "BOTH")
                        self.assertNotIn("reduceOnly",prepared.query)
                        protection=next(iter(f.protections.values()))
                        self.assertTrue(protection["closePosition"])
                        self.assertEqual(protection["side"],"SELL" if direction=="long" else "BUY")
                        self.assertEqual(protection["positionSide"],direction.upper() if mode=="HEDGE" else "BOTH")
                        self.assertTrue(f.adapter.reconcile(f.manifest)["known"])
                        close=f.proposal("10000",side="SELL" if side=="BUY" else "BUY",reducing=True,
                            direction=direction,cid="exit-exact-0002")
                        close["closeQuantity"]=entry["filledQuantity"]
                        exit_order=f.adapter.size_order(close);self.assertTrue(exit_order["ready"],exit_order)
                        result=f.adapter.submit(exit_order);self.assertEqual(result["status"],"FILLED",result)
                        wire=adapters.build_binance_futures_order_request(f.calls[-1],hedge_mode=mode=="HEDGE")
                        self.assertEqual(wire.query["positionSide"],direction.upper() if mode=="HEDGE" else "BOTH")
                        if mode=="HEDGE":self.assertNotIn("reduceOnly",wire.query)
                        else:self.assertEqual(wire.query["reduceOnly"],"true")
                        self.assertEqual(f.truth["positions"],[])
                        self.assertTrue(f.adapter.retire_flat_protection(f.manifest))
                        self.assertEqual(f.truth["openAlgoOrders"],[])
                        self.assertEqual(len(f.calls),2)

    def test_actual_symbol_limit_still_blocks_151_when_fresh_exchange_maximum_is150(self):
        f=self.futures(151,"short","HEDGE")
        ready=f.adapter.readiness(f.manifest);self.assertFalse(ready["ready"])
        self.assertIn("FUTURES_LEVERAGE_BRACKET_EXCEEDED",ready["blockers"])
        self.assertEqual(f.calls,[]);self.assertEqual(f.protection_calls,[])

    def test_readers_and_contract_preserve_actual150_without_claiming_real_exchange_support(self):
        parsed=contract.normalize_futures_policy({"leverage":150,"marginMode":"ISOLATED","stopLossPercent":"0.01"})
        self.assertEqual(parsed["leverage"],150)
        reader=reader_tests.readers.ProductionShortLiveReaders
        config=reader._symbol_config([{"symbol":"BTCUSDT","leverage":"150","marginType":"ISOLATED"}],"BTCUSDT")
        self.assertEqual(config["leverage"],"150")
        tiers,_=reader._leverage_brackets([{"symbol":"BTCUSDT","brackets":[{"notionalFloor":"0", "notionalCap":"10000",
            "initialLeverage":"150","maintMarginRatio":".001","cum":"0"}]}],"BTCUSDT")
        self.assertEqual(tiers[0]["initialLeverage"],"150")
        for invalid in (True,0,1.5,9007199254740992,"1.0000000000000001"):
            with self.subTest(invalid=invalid),self.assertRaises(ValueError):
                contract.normalize_futures_policy({"leverage":invalid,"marginMode":"ISOLATED","stopLossPercent":"0.01"})

    def test_binance_and_upbit_spot_sell_never_becomes_naked_short(self):
        for route in ("BINANCE_SPOT","UPBIT_KRW_SPOT"):
            f=base.Fixture(route);self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
            result=f.adapter.size_order(f.proposal(side="SELL",direction="short"))
            self.assertFalse(result["ready"]);self.assertIn("SPOT_SHORT_FORBIDDEN",result["blockers"])
            self.assertEqual(f.calls,[])

    def test_simultaneous_binance_and_upbit_selection_dispatches_only_signalled_explicit_route(self):
        h=session.ShortLiveSessionTests();h.setUp()
        try:
            binance=session.runtime_spec("BINANCE_SPOT","btc-binance","1h")
            upbit=session.runtime_spec("UPBIT_KRW_SPOT","btc-upbit","1h")
            service,brokers,feeds=h.service([binance,upbit]);h.start(service)
            h.first_buy(service,binance)
            self.assertEqual(len(brokers["BINANCE_SPOT"].submits),1)
            self.assertEqual(brokers["UPBIT_KRW_SPOT"].submits,[])
            h.first_buy(service,upbit)
            self.assertEqual(len(brokers["UPBIT_KRW_SPOT"].submits),1)
            self.assertEqual(brokers["UPBIT_KRW_SPOT"].submits[0]["route"],"UPBIT_KRW_SPOT")
        finally:h.tearDown()

if __name__=="__main__":unittest.main(verbosity=2)
