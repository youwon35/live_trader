"""Synthetic ticker clocks and existing reader/adapter/session paths only."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib
import json
import unittest

from test_short_live_brokers import Fixture, mod, PKG, NOW
from test_short_live_broker_readers import readers

quote = importlib.import_module(PKG + '.short_live_upbit_quote')


def ticker(now=NOW, *, source_age=0, trade_age=3600):
    return {'market': 'KRW-BTC', 'trade_price': '1000',
            'timestamp': int((now-source_age)*1000), 'trade_timestamp': int((now-trade_age)*1000)}


def observed(row=None, *, started=NOW, received=NOW, checked=NOW, purpose='INVENTORY_VALUATION'):
    return quote.validate_upbit_quote_observation(ticker() if row is None else row, 'KRW-BTC',
        purpose=purpose, request_started=started, received_at=received, checked_at=checked)


class QuoteTimeTests(unittest.TestCase):
    def test_last_trade_frequency_does_not_renew_or_expire_current_ticker_information(self):
        row=ticker(trade_age=3600); original=deepcopy(row)
        result=observed(row)
        self.assertEqual('FRESH',result['status']); self.assertEqual(0,result['sourceAgeSeconds'])
        self.assertEqual(3600,result['lastTradeAgeSeconds']); self.assertEqual(row,original)
        self.assertNotEqual(result['sourceObservedAt'],result['lastTradeObservedAt'])
        self.assertTrue(result['observationOnly']); self.assertNotIn('trade_price',result)

    def test_stale_source_is_not_renewed_by_fresh_receive_and_retains_actual_measurements(self):
        with self.assertRaisesRegex(quote.UpbitQuoteObservationError,'SOURCE_QUOTE_OBSERVATION_STALE') as raised:
            observed(ticker(source_age=11,trade_age=3600))
        detail=raised.exception.observation
        self.assertEqual('SOURCE_TIMESTAMP_STALE',detail['issue']); self.assertEqual(11,detail['sourceAgeSeconds'])
        self.assertEqual(0,detail['receiveAgeSeconds']); self.assertEqual(3600,detail['lastTradeAgeSeconds'])
        self.assertEqual(10,detail['maxSourceAgeSeconds']); self.assertEqual(2,detail['maxFutureSeconds'])

    def test_exact_ten_second_source_and_two_second_future_bounds_are_unchanged(self):
        self.assertEqual('FRESH',observed(ticker(source_age=10,trade_age=10))['status'])
        self.assertEqual('FRESH',observed(ticker(source_age=-2,trade_age=-2))['status'])
        for age in (10.001,-2.001):
            with self.subTest(age=age),self.assertRaisesRegex(quote.UpbitQuoteObservationError,'SOURCE_QUOTE_OBSERVATION_STALE'):
                observed(ticker(source_age=age,trade_age=age))

    def test_invalid_missing_boolean_or_unbounded_provider_time_stays_unknown_not_zero(self):
        for field in ('timestamp','trade_timestamp'):
            for bad in (None,True,'1792652400000',0,-1,10**50):
                row=ticker(); row[field]=bad
                with self.subTest(field=field,bad=repr(bad)),self.assertRaisesRegex(quote.UpbitQuoteObservationError,'UPBIT_QUOTE_SOURCE_TIME_UNKNOWN') as raised:
                    observed(row)
                key='sourceTimestampMs' if field=='timestamp' else 'lastTradeTimestampMs'
                age='sourceAgeSeconds' if field=='timestamp' else 'lastTradeAgeSeconds'
                self.assertIsNone(raised.exception.observation[key]); self.assertIsNone(raised.exception.observation[age])
                self.assertEqual('BLOCKED',raised.exception.observation['status'])
                self.assertEqual(raised.exception.observation,quote.sanitize_quote_observation(raised.exception.observation))

    def test_future_or_self_conflicting_last_trade_is_rejected(self):
        for trade_age,code in ((-3,'SOURCE_QUOTE_OBSERVATION_STALE'),(-1,'UPBIT_QUOTE_SOURCE_TIME_UNKNOWN')):
            with self.subTest(age=trade_age),self.assertRaisesRegex(quote.UpbitQuoteObservationError,code):
                observed(ticker(trade_age=trade_age))

    def test_slow_receive_stale_receive_and_rollback_are_not_hidden_by_fresh_source(self):
        for changes in ({'started':NOW-11},{'started':NOW-11.1,'received':NOW-11},
                        {'started':NOW+3},{'received':NOW+3}):
            with self.subTest(changes=changes),self.assertRaisesRegex(quote.UpbitQuoteObservationError,'MARKET_OBSERVATION_STALE'):
                observed(**changes)
        self.assertEqual('FRESH',observed(started=NOW-10,received=NOW)['status'])

    def test_wrong_market_or_unknown_purpose_does_not_produce_timing_evidence(self):
        row=ticker();row['market']='KRW-ETH'
        with self.assertRaisesRegex(quote.UpbitQuoteObservationError,'EXACT_MARKET_TRUTH_REQUIRED') as raised:
            observed(row)
        self.assertIsNone(raised.exception.observation)
        for purpose in ('raw account secret',[],{},None,True,4):
            with self.subTest(purpose=repr(purpose)),self.assertRaises(quote.UpbitQuoteObservationError):
                observed(purpose=purpose)
            self.assertIsNone(quote.sanitize_quote_observation({**observed(),'purpose':purpose}))

    def test_sanitizer_recomputes_status_and_discards_raw_fields_or_forged_fresh_values(self):
        result=observed()
        dirty={**result,'headers':{'Authorization':'SYNTHETIC_SECRET'},'price':'private','body':'private'}
        self.assertEqual(result,quote.sanitize_quote_observation(dirty))
        for changes in ({'maxSourceAgeSeconds':999},{'sourceAgeSeconds':False},{'status':'BLOCKED'},
                        {'sourceTimestampMs':True},{'receivedAt':'account:SYNTHETIC_SECRET'},
                        {'symbol':'account:SYNTHETIC_SECRET'},{'issue':'private secret'}):
            self.assertIsNone(quote.sanitize_quote_observation({**result,**changes}))
        with self.assertRaises(quote.UpbitQuoteObservationError) as raised:observed(ticker(source_age=20))
        blocked=raised.exception.observation
        self.assertEqual(blocked,quote.sanitize_quote_observation(blocked))
        self.assertIsNone(quote.sanitize_quote_observation({**blocked,'status':'FRESH','sourceAgeSeconds':0,'issue':'NONE'}))

    def test_sanitizer_only_attaches_quote_metadata_to_exact_public_upbit_ticker_route(self):
        result=observed();base={'provider':'upbit','path':'/v1/ticker','status':'AVAILABLE','elapsedMs':5,'quoteObservation':result}
        self.assertEqual(result,readers.sanitize_read_diagnostics([base])[0]['quoteObservation'])
        for changes in ({'provider':'binance','path':'/api/v3/time'},{'path':'/v1/accounts'}):
            self.assertNotIn('quoteObservation',readers.sanitize_read_diagnostics([{**base,**changes}])[0])


class ReaderFixture:
    def __init__(self, clock=lambda:NOW, identity='live-account:test-exact'):
        self.clock=clock;self.source_age=0;self.trade_age=3600;self.calls=[];self.stale_once=False
        owner=self
        class Reader:
            def _get(self,provider,path,**kwargs):
                owner.calls.append((provider,path,kwargs.get('query')))
                if path=='/v1/accounts':
                    return [{'currency':'KRW','balance':'100000','locked':'0'},
                            {'currency':'BTC','balance':'2','locked':'0'},
                            {'currency':'OLD_DELISTED','balance':'.00001','locked':'0'}],{}
                if path=='/v1/orders/open':return [],{}
                if path=='/v1/orders/chance':
                    return {'bid_fee':'0.0005','ask_fee':'0.0005','market':{'state':'active',
                        'bid':{'min_total':'5000'},'ask':{'min_total':'5000'}}},{}
                if path=='/v1/orderbook/instruments':return [{'market':'KRW-BTC','tick_size':'1'}],{}
                if path=='/v1/ticker':
                    age=11 if owner.stale_once else owner.source_age
                    owner.stale_once=False
                    return [ticker(owner.clock(),source_age=age,trade_age=owner.trade_age)],{}
                raise AssertionError('Unexpected synthetic path')
        self.reader=readers.ProductionShortLiveReaders(settings={},reader=Reader(),clock=clock,
            fx_reader=lambda _: (_ for _ in ()).throw(AssertionError('KRW quote needs no FX')))
        self.reader._identity=lambda _:identity
        self.reader._upbit=lambda path,params=None,signed=True:self.reader._read('upbit',path,query=params)[0]


class ReaderAndAdapterPathTests(unittest.TestCase):
    def test_actual_account_inventory_and_market_paths_use_current_snapshot_with_old_last_trade(self):
        f=ReaderFixture();r=f.reader
        snap=r.snapshot('UPBIT_KRW_SPOT',('KRW-BTC',))
        self.assertTrue(snap['known'],snap);self.assertEqual('102000',snap['equityNative'])
        self.assertEqual({'BTC':'2','OLD_DELISTED':'0.00001'},snap['spotInventory'])
        observation=next(row['quoteObservation'] for row in snap['readDiagnostics'] if row['path']=='/v1/ticker')
        self.assertEqual('INVENTORY_VALUATION',observation['purpose']);self.assertEqual(3600,observation['lastTradeAgeSeconds'])
        market=r.market('UPBIT_KRW_SPOT','KRW-BTC')
        self.assertTrue(market['known'],market)
        self.assertEqual(observation['sourceObservedAt'],market['sourceQuoteObservedAt'])
        self.assertEqual(observation['lastTradeObservedAt'],market['lastTradeObservedAt'])
        self.assertEqual(market['observedAt'],market['quoteReceivedAt'])
        self.assertEqual('ORDER_MARKET',market['readDiagnostics'][-1]['quoteObservation']['purpose'])
        self.assertEqual(7,len(f.calls))

    def test_actual_adapter_preserves_old_inventory_and_blocks_a_new_stale_source_with_safe_diagnostics(self):
        f=ReaderFixture();broker=Fixture('UPBIT_KRW_SPOT')
        broker.adapter.snapshot_reader=f.reader.snapshot;broker.adapter.market_reader=f.reader.market
        ready=broker.adapter.readiness(broker.manifest)
        self.assertTrue(ready['ready'],ready);self.assertEqual([],broker.calls)
        good=broker.adapter.reconcile(broker.manifest)
        self.assertTrue(good['known'],good);self.assertEqual({},good['positionsBySymbol'])
        f.source_age=11
        bad=broker.adapter.reconcile(broker.manifest)
        self.assertFalse(bad['known']);self.assertIn('SOURCE_QUOTE_OBSERVATION_STALE',bad['blockers'])
        last=bad['readDiagnostics'][-1]['quoteObservation']
        self.assertEqual(11,last['sourceAgeSeconds']);self.assertEqual('BLOCKED',last['status'])
        self.assertEqual([],broker.calls)


class SessionTimingDiagnosticTests(unittest.TestCase):
    def setUp(self):
        import test_short_live_session as base
        self.base=base;base.ShortLiveSessionTests.setUp(self)

    def tearDown(self):self.base.ShortLiveSessionTests.tearDown(self)

    def prepare(self,service,**changes):
        return self.base.ShortLiveSessionTests.prepare(self,service,**changes)

    def test_first_inventory_failure_in_three_route_hourly_session_is_retained_through_natural_cleanup(self):
        specs=[self.base.runtime_spec(route,f'i-{n}','1h') for n,route in enumerate(
            ('BINANCE_SPOT','BINANCE_USDM_FUTURES','UPBIT_KRW_SPOT'))]
        service,brokers,_=self.base.ShortLiveSessionTests.service(self,specs)
        f=ReaderFixture(clock=lambda:self.now.timestamp(),identity='fixture-UPBIT_KRW_SPOT')
        synthetic=Fixture('UPBIT_KRW_SPOT');synthetic.adapter.clock=lambda:self.now.timestamp()
        synthetic.adapter.snapshot_reader=f.reader.snapshot;synthetic.adapter.market_reader=f.reader.market
        brokers['UPBIT_KRW_SPOT']=synthetic.adapter
        self.base.ShortLiveSessionTests.start(self,service)
        f.stale_once=True
        service.tick()
        runtime=service.snapshot(include_catalog=False)['runtime']
        self.assertEqual('UPBIT_KRW_SPOT:SOURCE_QUOTE_OBSERVATION_STALE',runtime['firstFailure'])
        first=deepcopy(runtime['firstFailureDiagnostics'])
        self.assertEqual(11,first[-1]['quoteObservation']['sourceAgeSeconds'])
        self.assertEqual('INVENTORY_VALUATION',first[-1]['quoteObservation']['purpose'])
        self.assertEqual(0,runtime['orderCount']);self.assertEqual(0,runtime['fillCount'])
        self.assertEqual([],synthetic.calls)
        service._failure('LATER_SYNTHETIC_CLEANUP_FAILURE')
        service.tick()
        final=service.snapshot(include_catalog=False)['runtime']
        self.assertEqual(first,final['firstFailureDiagnostics'])
        rows=[json.loads(p.read_text(encoding='utf-8')) for p in service.store_root.glob('event-*.json')]
        failures=[row for row in rows if row['kind']=='FIRST_FAILURE']
        self.assertEqual(1,len(failures));self.assertEqual(first,failures[0]['readDiagnostics'])
        self.assertNotIn('trade_price',json.dumps(first));self.assertNotIn('fixture-UPBIT',json.dumps(first))
        self.assertTrue(final['terminalVerified'])
        terminal=json.loads(next(service.store_root.glob('session-terminal-*.json')).read_text(encoding='utf-8'))
        self.assertTrue(terminal['normalCleanupConfirmed']);self.assertEqual(first,terminal['firstFailureDiagnostics'])


if __name__=='__main__':unittest.main()
