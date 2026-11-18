"""Synthetic calendar/loop checks. No application, account, quote or order API."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime,timedelta,timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import threading,unittest
from unittest.mock import patch

from trading_runtime.continuous_runtime import RuntimeStrategySpec
from trading_runtime.stock_session_profile import normalize_stock_session_profile,profile_for_spec
from trading_runtime.toss_session_scope import TossSessionSource,TossCalendarArchive
from test_toss_session_profiles import calendar
from live_trader.toss_cash_operation import TossCashOperation

UTC=timezone.utc;KST=timezone(timedelta(hours=9))
def spec(symbol='005930',mode='REGULAR'):
    market='KR' if symbol.isdigit() else 'US'
    p=normalize_stock_session_profile(mode,market=market,provider='toss')
    return RuntimeStrategySpec('p','a'*64,'test:'+symbol,'s','b'*64,'moving_average_cross',
        'toss:'+market+':'+symbol,symbol,'1m','toss','toss',1,{'stockSessionProfile':p},{'asset':'stock'})

class Feed:
    def __init__(self,specs):
        self.subscriptions=tuple(SimpleNamespace(instrument_id=s.instrument_id,symbol=s.symbol,
            timeframe=s.timeframe,stock_session_profile=profile_for_spec(s)) for s in specs)
        self.polls=[];self.connected=False;self.rows=[];self.action=None
    def connect(self):self.connected=True
    def disconnect(self):self.connected=False
    def poll(self,timeout):
        self.polls.append(tuple(s.symbol for s in self.subscriptions))
        if self.action:self.action()
        return list(self.rows)

class IdleMarketDataTests(unittest.TestCase):
    def operation(self,specs=None):
        specs=tuple(specs or [spec()]);clock=[datetime(2026,11,18,23,tzinfo=UTC).timestamp()]
        op=TossCashOperation.__new__(TossCashOperation)
        op.prepared={'specs':specs,'durationHours':6};op.clock=lambda:clock[0];op.monotonic=lambda:clock[0]
        op.started_mono=clock[0];op.stop_event=threading.Event();op.api=object()
        return op,Feed(specs),clock
    def test_closed_regular_reads_zero_charts_and_preserves_original_subscriptions(self):
        op,feed,_=self.operation();original=feed.subscriptions
        self.assertEqual([],op._market_data_poll(feed))
        self.assertEqual([],feed.polls);self.assertIs(original,feed.subscriptions)
    def test_profile_shared_once_and_only_open_market_subscriptions_polled(self):
        first=spec();duplicate=replace(first,strategy_instance_id='other');us=spec('AAPL')
        op,feed,_=self.operation((first,duplicate,us));original=feed.subscriptions
        with patch('live_trader.toss_cash_operation.profile_open',side_effect=lambda s,*a:s.symbol=='AAPL') as gate:
            op._market_data_poll(feed)
        self.assertEqual(2,gate.call_count);self.assertEqual([('AAPL',)],feed.polls)
        self.assertIs(original,feed.subscriptions)
    def test_extended_refresh_ttl_cross_date_and_natural_resume(self):
        op,feed,clock=self.operation((spec(mode='EXTENDED'),))
        tmp=TemporaryDirectory();self.addCleanup(tmp.cleanup)
        class PublicCalendar:
            last_observation=None
            def __init__(self):self.calls=[]
            def market_calendar(self,market):
                self.calls.append(market);now=datetime.fromtimestamp(clock[0],UTC)
                self.last_observation={'receivedAt':now.isoformat()}
                return calendar(market,today=now.astimezone(KST).date().isoformat())
        api=PublicCalendar();op.api=api
        api._stock_session_source=TossSessionSource(api,archive=TossCalendarArchive(Path(tmp.name)/'calendar.sqlite3'),clock=lambda:clock[0])
        clock[0]=datetime(2026,11,19,7,55,tzinfo=KST).timestamp();op.started_mono=clock[0]
        self.assertEqual([],op._market_data_poll(feed));self.assertEqual(1,len(api.calls))
        clock[0]+=10*60;op._market_data_poll(feed)
        self.assertEqual([('005930',)],feed.polls);self.assertEqual(1,len(api.calls))
        # A wider observation duration isolates TTL/date handling from run expiry.
        op.prepared['durationHours']=48
        clock[0]=datetime(2026,11,19,15,45,tzinfo=KST).timestamp();op._market_data_poll(feed)
        self.assertEqual(2,len(api.calls));self.assertEqual(2,len(feed.polls))
        clock[0]=datetime(2026,11,20,7,55,tzinfo=KST).timestamp();op._market_data_poll(feed)
        self.assertEqual(3,len(api.calls));self.assertEqual(2,len(feed.polls))
    def test_unknown_calendar_is_failure_not_closed_success(self):
        op,feed,_=self.operation()
        with patch('live_trader.toss_cash_operation.profile_open',side_effect=ValueError('TOSS_CALENDAR_STALE_OR_WRONG_DATE')):
            with self.assertRaisesRegex(ValueError,'TOSS_CALENDAR_STALE_OR_WRONG_DATE'):op._market_data_poll(feed)
        self.assertEqual([],feed.polls)
    def test_stop_during_session_check_never_starts_chart(self):
        op,feed,_=self.operation()
        def gate(*a):op.stop_event.set();return True
        with patch('live_trader.toss_cash_operation.profile_open',side_effect=gate):self.assertEqual([],op._market_data_poll(feed))
        self.assertEqual([],feed.polls)
    def test_stop_or_duration_expiry_during_http_discards_returned_bars(self):
        for stopping in (True,False):
            op,feed,clock=self.operation();original=feed.subscriptions;feed.rows=[object()]
            feed.action=(lambda:op.stop_event.set()) if stopping else lambda:clock.__setitem__(0,clock[0]+21601)
            with patch('live_trader.toss_cash_operation.profile_open',return_value=True):self.assertEqual([],op._market_data_poll(feed))
            self.assertIs(original,feed.subscriptions)
    def test_poll_exception_restores_original_subscriptions(self):
        op,feed,_=self.operation();original=feed.subscriptions
        def fail():raise ValueError('ORIGINAL_HTTP_FAILURE')
        feed.action=fail
        with patch('live_trader.toss_cash_operation.profile_open',return_value=True):
            with self.assertRaisesRegex(ValueError,'ORIGINAL_HTTP_FAILURE'):op._market_data_poll(feed)
        self.assertIs(original,feed.subscriptions)
    def test_closed_run_keeps_reconcile_protection_stop_responsive(self):
        op,feed,clock=self.operation();calls=[];waits=[]
        @contextmanager
        def boundary():calls.append('protection');yield {'active':False}
        class Event:
            stopped=False
            def is_set(self):return self.stopped
            def set(self):self.stopped=True
            def wait(self,seconds):
                waits.append(seconds);clock[0]+=seconds
                if len(waits)==3:self.set()
        op.stop_event=Event();op.feed_factory=lambda *a:feed;op.boundary=boundary;op.view={};op.pending_signals={}
        op._guard=lambda:calls.append('guard')
        def reconcile(**kw):calls.append(('reconcile',kw));return {'unresolvedOrders':[]}
        op.reconcile=reconcile;op.dispatch_pending=lambda:calls.append('dispatch')
        op.ingest_bar=lambda bar:self.fail('No closed-market bars may be evaluated')
        op._finish=lambda:calls.append('finish');op._note=lambda *a:calls.append(a)
        op._run()
        self.assertEqual(3,calls.count('guard'));self.assertEqual(3,calls.count('protection'))
        self.assertEqual(3,len([c for c in calls if isinstance(c,tuple) and c[0]=='reconcile']))
        self.assertEqual(3,calls.count('dispatch'));self.assertEqual([1,1,1],waits)
        self.assertEqual([],feed.polls);self.assertFalse(feed.connected);self.assertNotIn('firstFailure',op.view)

if __name__=='__main__':unittest.main()
