"""Synthetic account/tier bounds, full owned lifecycle and liquidation evidence.

No exchange, settings or app initializer is called. 150 is a deliberately
synthetic bracket maximum, not a claim about any real Binance symbol.
"""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import unittest
import test_short_live_composition as comp
import test_short_live_futures_protection as pf
import test_short_live_futures_readers as readers


class ApprovedLeverageCompositionTests(unittest.TestCase):
    setUp=comp.ShortLiveCompositionTests.setUp
    enter=comp.ShortLiveCompositionTests.enter
    fixture_for=comp.ShortLiveCompositionTests.fixture_for

    def test_actual_catalog_service_adapter_shared_validator_wire_and_owned_long_short_retirement(self):
        for multiplier in (7,17,37,125,150):
            for mode in ('ONE_WAY','HEDGE'):
                for direction in ('long','short'):
                    with self.subTest(multiplier=multiplier,mode=mode,direction=direction):
                        f=self.fixture_for('BINANCE_USDM_FUTURES',futures_leverage=multiplier,
                            position_mode=mode,direction=direction)
                        prepared=f.prepare()
                        self.assertTrue(prepared['readyToStart'],prepared)
                        self.assertEqual([],f.mutations)
                        f.start()
                        view=f.forward()['runtime']
                        self.assertEqual('RUNNING',view['status'],(view,f.wire_errors))
                        self.assertEqual(1,view['fillCount'],(view,f.wire_errors))
                        self.assertEqual([],f.wire_errors)
                        self.assertEqual(1,len(f.mutations))
                        opening=f.mutations[0]
                        self.assertEqual('BUY' if direction=='long' else 'SELL',opening['query']['side'])
                        self.assertEqual(direction.upper() if mode=='HEDGE' else 'BOTH',opening['query']['positionSide'])
                        self.assertNotIn('reduceOnly',opening['query'])
                        self.assertEqual(multiplier,int(opening['order']['leverage']))
                        self.assertLess(f.calls.index(('POST','/fapi/v1/algoOrder')),f.calls.index(('POST','/fapi/v1/order')))
                        self.assertGreater(f.position if direction=='long' else -f.position,0)
                        # Real native strategy signal closes the observed owned leg.
                        f.now=comp.NOW+timedelta(minutes=5,milliseconds=200);f.mono+=300
                        child=f.sockets[-1]
                        child.events=[comp.candle(child.specs[0],comp.NOW+timedelta(minutes=5),1007)]
                        closed=f.service.tick()['runtime']
                        self.assertEqual(Decimal(0),f.position,(closed,f.wire_errors))
                        self.assertEqual(2,len(f.mutations),(closed,f.wire_errors))
                        reducing=f.mutations[-1]
                        self.assertTrue(reducing['order']['riskReducing'])
                        self.assertEqual('SELL' if direction=='long' else 'BUY',reducing['query']['side'])
                        self.assertEqual(direction.upper() if mode=='HEDGE' else 'BOTH',reducing['query']['positionSide'])
                        if mode=='HEDGE':self.assertNotIn('reduceOnly',reducing['query'])
                        else:self.assertEqual('true',reducing['query']['reduceOnly'])
                        terminal=f.service.stop({})['runtime']
                        self.assertTrue(terminal['terminalVerified'],terminal)
                        self.assertEqual(['POST','DELETE'],[r['method'] for r in f.protection_mutations])
                        self.assertTrue(all(row['algoStatus']=='CANCELED' for row in f.algos.values()))
                        self.assertTrue(f.sockets[-1].disconnected)

    def test_unknown_protection_read_blocks_new_risk_without_cancelling_acknowledged_stop(self):
        f=self.fixture_for('BINANCE_USDM_FUTURES',futures_leverage=17,direction='short',position_mode='HEDGE')
        f.start();view=f.forward()['runtime']
        self.assertEqual(1,view['fillCount'],(view,f.wire_errors))
        acknowledged=deepcopy(f.algos)
        f.unknown_protection_read=True
        f.mono+=f.service.account_poll_seconds; f.now+=timedelta(seconds=f.service.account_poll_seconds)
        f.service.tick()
        self.assertTrue(f.service.snapshot()['runtime']['newEntriesBlocked'])
        self.assertEqual(acknowledged,f.algos)
        self.assertFalse(any(row['method']=='DELETE' for row in f.protection_mutations))
        self.assertEqual(1,sum(not row['order']['riskReducing'] for row in f.mutations))
        self.assertFalse(f.service.snapshot()['runtime']['terminalVerified'])


class ActualLiquidationEvidenceTests(unittest.TestCase):
    def position(self,direction='long',mode='ONE_WAY',**changes):
        return {'symbol':'SOLUSDT','quantity':'1' if direction=='long' else '-1',
            'positionSide':direction.upper() if mode=='HEDGE' else 'BOTH','marginMode':'ISOLATED',
            'markPrice':'100','entryPrice':'100','isolatedWallet':'100','liquidationPrice':'0',**changes}

    def test_zero_liquidation_requires_exact_full_isolated_long_collateral(self):
        for mode in ('ONE_WAY','HEDGE'):
            f=pf.ProtectionFixture('long',mode);f.ensure()
            f.manager.check_positions([self.position(mode=mode)])
            self.assertEqual([],f.manager.incidents)
            self.assertFalse(any(kind=='DELETE' for kind,_ in f.calls))
        for direction,changes in (('short',{}),('long',{'isolatedWallet':'99.99'}),
                                  ('long',{'entryPrice':None}),('long',{'isolatedWallet':None}),
                                  ('long',{'marginMode':'CROSSED'}),('long',{'quantity':'2'})):
            with self.subTest(direction=direction,changes=changes):
                f=pf.ProtectionFixture(direction);f.ensure()
                f.manager.check_positions([self.position(direction,**changes)])
                self.assertEqual('FUTURES_LIQUIDATION_PRICE_UNVERIFIED',f.manager.incidents[-1]['code'])
                self.assertTrue(all(row['status']=='NEW' for row in f.rows.values()))
                self.assertFalse(any(kind=='DELETE' for kind,_ in f.calls))

    def test_unknown_or_wrong_position_side_cannot_use_prior_stop_observation(self):
        for changes in ({'positionSide':'SHORT'},{'positionSide':None}):
            f=pf.ProtectionFixture();f.ensure()
            f.manager.check_positions([self.position(**changes)])
            self.assertEqual('FUTURES_POSITION_PROTECTION_UNVERIFIED',f.manager.incidents[-1]['code'])
        f=pf.ProtectionFixture();f.ensure()
        next(iter(f.manager.claims.values()))['lastReadVerified']=False
        f.manager.check_positions([self.position()])
        self.assertEqual('FUTURES_POSITION_PROTECTION_UNVERIFIED',f.manager.incidents[-1]['code'])

    def test_actual_child_wrong_hedge_side_is_never_joined_as_owned_fill(self):
        f=pf.ProtectionFixture('short','HEDGE');ack=f.ensure()
        row=f.rows[ack['clientAlgoId']]
        row.update(status='TRIGGERED',actualOrderId='777',executionKnown=True,
            actualOrder={'known':True,'brokerOrderId':'777','symbol':'SOLUSDT','side':'BUY',
                         'positionSide':'LONG','status':'PARTIALLY_FILLED','filledQuantity':'.25','feeNative':'.01'})
        with self.assertRaisesRegex(pf.mod.FuturesProtectionError,'IDENTITY_CHANGED'):f.manager.poll()
        self.assertEqual([],f.manager.incidents)
        row['actualOrder']['positionSide']='SHORT'
        result=f.manager.poll()
        self.assertEqual('.25',result[0]['order']['filledQuantity'])

    def test_gap_through_actual_buffer_is_incident_without_invented_fill(self):
        for direction,mark,liquidation in (('long','97','99'),('short','103','101')):
            f=pf.ProtectionFixture(direction);f.ensure()
            f.manager.check_positions([self.position(direction,markPrice=mark,liquidationPrice=liquidation)])
            self.assertEqual('FUTURES_ACTUAL_LIQUIDATION_BUFFER_BREACHED',f.manager.incidents[-1]['code'])
            self.assertTrue(all(row['status']=='NEW' for row in f.rows.values()))
            self.assertFalse(any(kind=='DELETE' for kind,_ in f.calls))

    def test_same_side_addition_keeps_stronger_exact_close_all_stop(self):
        for direction,reference in (('long','99'),('short','101')):
            f=pf.ProtectionFixture(direction);original=f.ensure()
            result=f.ensure(quantity='2',projectedGrossExposureKrw='300',referencePrice=reference,
                clientOrderId='same-owned-leg-addition')
            self.assertEqual(original['clientAlgoId'],result['clientAlgoId'])
            self.assertTrue(result['closePosition'])
            self.assertEqual(1,sum(kind=='POST' for kind,_ in f.calls))
            self.assertFalse(any(kind=='DELETE' for kind,_ in f.calls))


class RawRiskSymbolTests(unittest.TestCase):
    def test_market_requires_raw_symbol_on_premium_and_commission_response(self):
        helper=readers.FuturesReaderTests()
        for endpoint,field,value in (('/fapi/v1/premiumIndex','symbol','BTCUSDT'),
                                     ('/fapi/v1/premiumIndex','symbol',None),
                                     ('/fapi/v1/commissionRate','symbol','BTCUSDT'),
                                     ('/fapi/v1/commissionRate','symbol',None)):
            with self.subTest(endpoint=endpoint,value=value):
                r,values=helper.market_fixture();values[endpoint][field]=value
                self.assertFalse(r.market(readers.ROUTE,readers.SYMBOL)['known'])


if __name__=='__main__':unittest.main(verbosity=2)
