"""Actual conversion producer/reader/adapter pipeline; all I/O is synthetic."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from email.utils import format_datetime
import importlib
import json
import unittest
from unittest.mock import Mock, patch
import test_native_spot_preflight as fixture
from test_short_live_reader_diagnostics import build_readers
import test_short_live_reader_diagnostics as reader_fixture
from test_short_live_brokers import mod, PKG, NOW, Fixture

fx=importlib.import_module(fixture.P+'.account_currency_limits')
safe=importlib.import_module(fixture.P+'.native_fx_observation')
diagnostics=importlib.import_module(fixture.P+'.short_live_read_diagnostics')
readers=importlib.import_module(PKG+'.short_live_broker_readers')


class CurrentOrderbookFxTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,10,30,0,0,0,250000,tzinfo=timezone.utc)

    def book(self,age=0):
        return [{'market':'KRW-USDT','timestamp':int((self.now-timedelta(seconds=age)).timestamp()*1000),
            'level':0,'total_ask_size':'10','total_bid_size':'20',
            'orderbook_units':[{'ask_price':'1401','bid_price':'1400','ask_size':'1','bid_size':'2'}]}]

    def reader(self,payload=None):
        reader=Mock()
        reader._get.return_value=(self.book() if payload is None else payload,
            {'date':format_datetime(self.now.replace(microsecond=0),usegmt=True)})
        return reader

    def receipt(self):
        return fx.observe_upbit_usdt_krw(self.reader(),now=self.now)

    def test_current_book_does_not_require_a_recent_trade_or_assume_a_usd_peg(self):
        book=self.book();book[0]['trade_timestamp']=int((self.now-timedelta(hours=4)).timestamp()*1000)
        reader=self.reader(book);receipt=fx.observe_upbit_usdt_krw(reader,now=self.now)
        self.assertEqual(fx.SCHEMA,receipt['schemaVersion'])
        self.assertEqual('1401',receipt['krwPerNativeCurrency'])
        self.assertEqual(receipt['sourceUpdateAsOf'],receipt['rateAsOf'])
        self.assertEqual('UPBIT_ORDERBOOK_REQUEST_TIMESTAMP',receipt['sourceTimeBasis'])
        reader._get.assert_called_once_with('upbit','/v1/orderbook',query={'markets':'KRW-USDT','count':1,'level':'0'})
        reader._credentials.assert_not_called()
        self.assertFalse(receipt['orderAuthorityGranted'])

    def test_ask_rounds_native_ceiling_down_and_native_loss_conversion_up(self):
        receipt=self.receipt()
        limits=fx.native_absolute_limits('100000',native_currency='USDT',broker_id='binance',
            conversion_observation=receipt,fx_buffer_bps='0',now=self.now)
        self.assertEqual('READY',limits['status'],limits['reasons'])
        self.assertLess(Decimal(limits['nativeLimitExact']),Decimal('100000')/Decimal('1400'))
        self.assertGreater(Decimal(receipt['krwPerNativeCurrency'])*10,Decimal('1400')*10)

    def test_no_ticker_retry_or_fallback_when_current_book_is_unavailable(self):
        reader=self.reader();reader._get.side_effect=TimeoutError('SYNTHETIC_PRIVATE_BODY')
        with self.assertRaises(TimeoutError):fx.observe_upbit_usdt_krw(reader,now=self.now)
        self.assertEqual(1,reader._get.call_count)
        self.assertEqual('/v1/orderbook',reader._get.call_args.args[1])

    def test_malformed_scope_size_price_future_and_stale_book_fail_closed(self):
        changes=[lambda b:b.clear(),lambda b:b.append(deepcopy(b[0])),
            lambda b:b[0].update(market='KRW-BTC'),lambda b:b[0].update(level=1),
            lambda b:b[0].update(timestamp=True),lambda b:b[0].update(timestamp=0),
            lambda b:b[0].update(timestamp=int((self.now-timedelta(seconds=16)).timestamp()*1000)),
            lambda b:b[0].update(timestamp=int((self.now+timedelta(seconds=3)).timestamp()*1000)),
            lambda b:b[0]['orderbook_units'][0].update(ask_price='1399'),
            lambda b:b[0]['orderbook_units'][0].update(bid_price='NaN'),
            lambda b:b[0]['orderbook_units'][0].update(ask_size=0),
            lambda b:b[0]['orderbook_units'][0].update(bid_size='21'),
            lambda b:b[0]['orderbook_units'].append(deepcopy(b[0]['orderbook_units'][0]))]
        for change in changes:
            book=self.book();change(book)
            with self.subTest(change=change),self.assertRaises(ValueError):
                fx.observe_upbit_usdt_krw(self.reader(book),now=self.now)

    def test_http_cache_clock_or_payload_changes_never_become_a_fresh_receipt(self):
        receipt=self.receipt()
        for change in [lambda r:r['officialResponse'][0].update(timestamp=1),
            lambda r:r.update(krwPerNativeCurrency='1'),
            lambda r:r.update(rateAsOf=self.now.replace(year=2025).isoformat()),
            lambda r:r['requestQuery'].update(count=2),
            lambda r:r['httpClock'].update(ageHeader='3'),
            lambda r:r['httpClock'].update(roundTripNanoseconds=15_000_000_001)]:
            changed=deepcopy(receipt);change(changed)
            with self.subTest(change=change),self.assertRaises(ValueError):
                fx.validate_conversion_observation(fx._seal(changed),native_currency='USDT',now=self.now)
        with self.assertRaises(ValueError):fx.validate_conversion_observation(receipt,native_currency='USDT',now=self.now+timedelta(seconds=16))

    def test_legacy_ticker_v1_can_be_explicitly_read_but_cannot_back_a_new_ceiling(self):
        old=self.receipt();stamp=int(self.now.timestamp()*1000)
        old.update(schemaVersion=fx.LEGACY_SCHEMA,endpoint='/v1/ticker',reference=fx.LEGACY_REFERENCE,
            rateRole='OBSERVED_MARKET_TRADE_NOT_USD_PEG',officialResponse=[{'market':'KRW-USDT',
                'trade_price':'1401','trade_timestamp':stamp,'timestamp':stamp}])
        old['officialResponseHash']=fx.stable_sha256(old['officialResponse']);old=fx._seal(old)
        fx.validate_conversion_observation(old,native_currency='USDT',now=self.now,allow_legacy=True)
        with self.assertRaisesRegex(ValueError,'NATIVE_FX_CURRENT_OBSERVATION_REQUIRED'):
            fx.validate_conversion_observation(old,native_currency='USDT',now=self.now)
        result=fx.native_absolute_limits('100000',native_currency='USDT',broker_id='binance',conversion_observation=old,now=self.now)
        self.assertEqual('BLOCKED',result['status']);self.assertEqual('0',result['maxOrderValue'])

    def test_diagnostic_binds_original_times_and_drops_prices_keys_raw_and_forged_age(self):
        receipt=self.receipt();value=fx.conversion_observation_diagnostic(receipt,now=self.now)
        self.assertEqual('FRESH',value['status']);self.assertEqual(15,value['maxAgeSeconds'])
        original=deepcopy(value);value.update(apiKey='SYNTHETIC_SECRET',askPrice='1401',account='private')
        self.assertEqual(original,safe.sanitize_fx_observation(value))
        changed={**original,'sourceAgeSeconds':0.5}
        self.assertIsNone(safe.sanitize_fx_observation(changed))
        changed={**original,'sourceObservedAt':self.now.replace(year=2025).isoformat()}
        self.assertIsNone(safe.sanitize_fx_observation(changed))
        changed={**original,'validationCode':'NONE','status':'FRESH','sourceTimestampMs':1}
        self.assertIsNone(safe.sanitize_fx_observation(changed))
        self.assertNotIn('1401',json.dumps(original));self.assertNotIn('SYNTHETIC_SECRET',json.dumps(safe.sanitize_fx_observation(value)))

    def test_fx_failure_preserves_exact_current_book_timing_through_public_read_allowlist(self):
        with self.assertRaises(fx.CurrentFxObservationError) as raised:
            fx.observe_upbit_usdt_krw(self.reader(self.book(age=20)),now=self.now)
        evidence=raised.exception.fxObservation
        self.assertEqual('BLOCKED',evidence['status'])
        self.assertEqual('NATIVE_FX_OBSERVATION_STALE_OR_FUTURE',evidence['validationCode'])
        r=build_readers();r.fx_reader=lambda _:(_ for _ in ()).throw(raised.exception)
        with self.assertRaises(mod.ShortLiveBrokerBlocked):r._fx('USDT')
        row=r._failure_reads()[-1]
        self.assertEqual('/v1/orderbook',row['path']);self.assertEqual('FX_VALIDATION',row['failureKind'])
        self.assertEqual(evidence,row['fxObservation'])
        self.assertEqual([row],diagnostics.sanitize_read_diagnostics([row]))

    def test_real_producer_factory_market_adapter_and_late_consumer_share_one_source_time(self):
        reader,calls,current=reader_fixture.QuoteReadOrderingTests().build()
        actual_fx=importlib.import_module(PKG+'.account_currency_limits')
        class ClockDateTime(datetime):
            @classmethod
            def now(cls,tz=None): return datetime.fromtimestamp(current[0],timezone.utc)
        public=Mock()
        def read(provider,path,*,query):
            self.assertEqual(('upbit','/v1/orderbook',{'markets':'KRW-USDT','count':1,'level':'0'}),(provider,path,query))
            stamp=int(current[0]*1000);current[0]+=.04
            row=self.book();row[0]['timestamp']=stamp
            return row,{'date':format_datetime(datetime.fromtimestamp(current[0],timezone.utc).replace(microsecond=0),usegmt=True)}
        public._get.side_effect=read
        reader.fx_reader=readers.production_fx_reader(public,clock=lambda:current[0])
        adapter=Fixture('BINANCE_SPOT').adapter
        adapter.clock=lambda:current[0]
        adapter.market_reader=reader.market
        with patch.object(actual_fx,'datetime',ClockDateTime):
            quote=adapter._market('SOLUSDT')
        self.assertEqual(1,public._get.call_count)
        self.assertEqual(datetime.fromtimestamp(NOW+11,timezone.utc),datetime.fromisoformat(quote['fxObservedAt']))
        evidence=quote['readDiagnostics'][-1]['fxObservation']
        self.assertEqual('FRESH',evidence['status'])
        self.assertEqual(evidence['sourceObservedAt'],datetime.fromtimestamp(NOW+11,timezone.utc).isoformat(timespec='microseconds'))
        current[0]+=16
        # A newly observed native price must not refresh an old FX source.
        quote['observedAt']=current[0]
        adapter.market_reader=lambda *_:deepcopy(quote)
        with self.assertRaisesRegex(mod.ShortLiveBrokerBlocked,'FX_OBSERVATION_STALE') as raised:
            adapter._market('SOLUSDT')
        later=raised.exception.readDiagnostics[-1]['fxObservation']
        self.assertEqual('BLOCKED',later['status'])
        self.assertEqual('NATIVE_FX_OBSERVATION_STALE_OR_FUTURE',later['validationCode'])
        for key in ['sourceTimestampMs','sourceObservedAt','requestStartedAt','receivedAt','roundTripNanoseconds']:
            self.assertEqual(evidence[key],later[key])
        self.assertGreater(later['receiveAgeSeconds'],15)
        self.assertEqual(1,public._get.call_count)

    def test_diagnostic_never_throws_for_malformed_validation_code(self):
        value=fx.conversion_observation_diagnostic(self.receipt(),now=self.now)
        for code in [[],{},None,True,5]:
            with self.subTest(code=code):
                self.assertIsNone(safe.sanitize_fx_observation({**value,'validationCode':code}))

    def test_nanosecond_expiry_is_not_rounded_into_a_fresh_observation(self):
        zero=self.now.replace(microsecond=0)
        for nanoseconds in [0,1,99,100,101,399,400,499,500,999,1000]:
            evidence=safe.fx_clock_observation(source_ms=int(zero.timestamp()*1000),
                started=zero+timedelta(seconds=13),received=zero+timedelta(seconds=13),
                checked=zero+timedelta(seconds=14),date_header=format_datetime(zero+timedelta(seconds=13),usegmt=True),
                age_header=None,round_trip_ns=nanoseconds)
            self.assertEqual('FRESH' if nanoseconds==0 else 'BLOCKED',evidence['status'])
            self.assertEqual(15 if nanoseconds==0 else 15.000001,evidence['sourceAgeUpperSeconds'])
            self.assertEqual(evidence,safe.sanitize_fx_observation(evidence))

    def test_numeric_boolean_diagnostic_fields_are_never_accepted(self):
        evidence=fx.conversion_observation_diagnostic(self.receipt(),now=self.now)
        for key,value in [('observationOnly',1),('orderAuthorityGranted',0),
            ('sourceAgeSeconds',False),('receiveAgeSeconds',False),('sourceTimestampMs',True),
            ('roundTripNanoseconds',False)]:
            with self.subTest(key=key):
                self.assertIsNone(safe.sanitize_fx_observation({**evidence,key:value}))


if __name__=='__main__':unittest.main()
