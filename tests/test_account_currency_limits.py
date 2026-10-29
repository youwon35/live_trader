from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from email.utils import format_datetime
import importlib
import unittest
import threading
from unittest.mock import Mock

import test_native_spot_preflight as fixture

fx = importlib.import_module(fixture.P + '.account_currency_limits')


class AccountCurrencyLimitsTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        with fx._CACHE_LOCK:
            fx._CACHE.clear()
        self.addCleanup(fx._CACHE.clear)

    def reader(self, *, price='1475.5', age=0, market='KRW-USDT'):
        stamp = int((self.now-timedelta(seconds=age)).timestamp()*1000)
        reader = Mock()
        reader._get.return_value = ([{'market':market, 'timestamp':stamp, 'level':0, 'total_ask_size':'10', 'total_bid_size':'10', 'orderbook_units':[{'ask_price':price, 'bid_price':price, 'ask_size':'1', 'bid_size':'1'}]}], {'date':format_datetime(self.now.replace(microsecond=0),usegmt=True)})
        return reader

    def receipt(self, **kwargs):
        return fx.observe_upbit_usdt_krw(self.reader(**kwargs), now=self.now)

    def test_krw_account_ceiling_keeps_saved_units_and_value_without_fx(self):
        for broker in ('upbit','kis'):
            result=fx.native_absolute_limits('20000000',native_currency='KRW',broker_id=broker,now=self.now)
            self.assertEqual('READY',result['status'])
            self.assertEqual(Decimal('20000000'),Decimal(result['maxOrderValue']))
            self.assertEqual('0',result['fxBufferBps'])
            self.assertFalse(result['orderAuthorityGranted'])

    def test_usdt_cap_uses_actual_public_trade_with_saved_buffer_never_usd_peg(self):
        receipt=self.receipt()
        result=fx.native_absolute_limits('20000000',native_currency='USDT',broker_id='binance',conversion_observation=receipt,fx_buffer_bps='50',now=self.now)
        self.assertEqual('READY',result['status'],result['reasons'])
        oracle=Fraction(20000000)/(Fraction('1475.5')*Fraction('1.005'))
        self.assertLessEqual(Fraction(result['nativeLimitExact']),oracle)
        self.assertLessEqual(Fraction.from_float(float(result['maxOrderValue'])),oracle)
        self.assertLess(Decimal(result['maxOrderValue']),Decimal('20000000'))
        self.assertEqual(receipt['contentHash'],result['conversionObservationHash'])
        self.assertEqual('OBSERVED_BEST_ASK_NOT_USD_PEG',receipt['rateRole'])
        self.assertTrue(result['sourcePolicyUnchanged'])

    def test_missing_or_unimplemented_usd_conversion_cannot_mix_krw_and_usd(self):
        for broker,currency,observation in (('binance','USDT',None),('kis','USD',None),('kis','USD',self.receipt()),('binance','USDC',self.receipt())):
            result=fx.native_absolute_limits('20000000',native_currency=currency,broker_id=broker,conversion_observation=observation,now=self.now)
            self.assertEqual('BLOCKED',result['status'])
            self.assertEqual('0',result['maxOrderValue'])
            self.assertEqual('0',result['strategyCapitalLimit'])

    def test_upbit_producer_is_public_only_and_does_not_read_credentials(self):
        reader=self.reader()
        receipt=fx.observe_upbit_usdt_krw(reader,now=self.now)
        reader._get.assert_called_once_with('upbit','/v1/orderbook',query={'markets':'KRW-USDT','count':1,'level':'0'})
        reader._credentials.assert_not_called()
        self.assertNotIn('accountFingerprint',receipt)

    def test_stale_future_mismatched_pair_or_incomplete_time_is_rejected(self):
        for kwargs in ({'age':16},{'age':-1},{'market':'KRW-USD'},{'price':0}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):
                self.receipt(**kwargs)
        reader=self.reader()
        reader._get.return_value[0][0]['timestamp']=True
        with self.assertRaises(ValueError):fx.observe_upbit_usdt_krw(reader,now=self.now)

    def test_receipt_payload_rate_currency_and_timestamp_are_exactly_bound(self):
        receipt=self.receipt()
        for name in ('rate','currency','payload','time'):
            changed=deepcopy(receipt)
            if name=='rate':changed['krwPerNativeCurrency']='1'
            elif name=='currency':changed['nativeCurrency']='USD'
            elif name=='payload':changed['officialResponse'][0]['orderbook_units'][0]['ask_price']='1'
            else:changed['rateAsOf']=self.now.isoformat()
            changed=fx._seal(changed)
            with self.subTest(name=name),self.assertRaises(ValueError):
                fx.validate_conversion_observation(changed,native_currency='USDT',now=self.now)

    def test_internal_ttl_cache_reuses_public_get_and_returns_defensive_copy(self):
        reader=self.reader()
        first=fx.refresh_conversion_observation(reader,native_currency='USDT',broker_id='binance',now=self.now)
        self.assertIsNotNone(first)
        first['krwPerNativeCurrency']='1'
        second=fx.refresh_conversion_observation(reader,native_currency='USDT',broker_id='binance',now=self.now+timedelta(seconds=4))
        self.assertEqual('1475.5',second['krwPerNativeCurrency'])
        self.assertEqual(1,reader._get.call_count)
        self.assertIsNone(fx.latest_conversion_observation('USDT','binance',now=self.now+timedelta(seconds=16)))

    def test_failed_refresh_clears_cache_and_does_not_reuse_earlier_rate(self):
        reader=self.reader()
        fx.refresh_conversion_observation(reader,native_currency='USDT',broker_id='binance',now=self.now)
        reader._get.side_effect=OSError('fixture transport unavailable')
        self.assertIsNone(fx.refresh_conversion_observation(reader,native_currency='USDT',broker_id='binance',now=self.now+timedelta(seconds=6)))
        self.assertIsNone(fx.latest_conversion_observation('USDT','binance',now=self.now+timedelta(seconds=6)))

    def test_risk_cache_reader_never_waits_on_a_public_network_refresh(self):
        reader=self.reader()
        response=reader._get.return_value
        entered,release=threading.Event(),threading.Event()
        def pending(*args,**kwargs):
            entered.set()
            if not release.wait(2):raise AssertionError('fixture producer was not released')
            return response
        reader._get.side_effect=pending
        worker=threading.Thread(target=lambda:fx.refresh_conversion_observation(reader,native_currency='USDT',broker_id='binance',now=self.now))
        worker.start()
        try:
            self.assertTrue(entered.wait(1))
            # The network worker cannot own the cache-reader lock while in I/O.
            self.assertTrue(fx._CACHE_LOCK.acquire(blocking=False))
            fx._CACHE_LOCK.release()
            self.assertIsNone(fx.latest_conversion_observation('USDT','binance',now=self.now))
            self.assertIsNone(fx.refresh_conversion_observation(reader,native_currency='USDT',broker_id='binance',now=self.now))
            self.assertEqual(1,reader._get.call_count)
        finally:
            release.set();worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertIsNotNone(fx.latest_conversion_observation('USDT','binance',now=self.now))

    def test_buffer_and_float_transport_never_raise_native_money_ceiling(self):
        receipt=self.receipt(price='3')
        for cap in ('1','0.000000000000000001','20000000','0'):
            for buffer in ('0','50','10000'):
                with self.subTest(cap=cap,buffer=buffer):
                    result=fx.native_absolute_limits(cap,native_currency='USDT',broker_id='binance',conversion_observation=receipt,fx_buffer_bps=buffer,now=self.now)
                    self.assertEqual('READY',result['status'])
                    exact=Fraction(cap)/(Fraction(3)*(1+Fraction(buffer)/10000))
                    self.assertLessEqual(Fraction.from_float(float(result['maxOrderValue'])),exact)
        self.assertEqual('BLOCKED',fx.native_absolute_limits(1,native_currency='USDT',broker_id='binance',conversion_observation=receipt,fx_buffer_bps=-1,now=self.now)['status'])

    def test_official_date_interval_allows_bounded_local_skew_without_extending_expiry(self):
        reader=self.reader(age=-1)
        reader._get.return_value[1]['date']=format_datetime((self.now+timedelta(seconds=2)).replace(microsecond=0),usegmt=True)
        receipt=fx.observe_upbit_usdt_krw(reader,now=self.now)
        self.assertGreater(fx._timestamp(receipt['rateAsOf']),self.now)
        self.assertEqual(1,receipt['httpClock']['datePrecisionSeconds'])
        fx.validate_conversion_observation(receipt,native_currency='USDT',now=self.now)
        with self.assertRaises(ValueError):fx.validate_conversion_observation(receipt,native_currency='USDT',now=self.now+timedelta(seconds=16))

    def test_http_missing_date_cached_age_clock_skew_slow_rtt_and_server_future_reject(self):
        for name in ('missing','cached','invalid-age','skew','rtt','server-future'):
            reader=self.reader()
            headers=reader._get.return_value[1]
            if name=='missing':headers.pop('date')
            elif name=='cached':headers['age']='1'
            elif name=='invalid-age':headers['age']='-1'
            elif name=='skew':headers['date']=format_datetime((self.now+timedelta(seconds=16)).replace(microsecond=0),usegmt=True)
            elif name=='server-future':reader._get.return_value[0][0]['timestamp']+=2000
            if name=='rtt':
                changed=self.receipt()
                changed['httpClock']['roundTripNanoseconds']=15_000_000_001
                with self.assertRaises(ValueError):fx.validate_conversion_observation(fx._seal(changed),native_currency='USDT',now=self.now)
            else:
                with self.subTest(name=name),self.assertRaises(ValueError):fx.observe_upbit_usdt_krw(reader,now=self.now)


if __name__=='__main__': unittest.main()
