"""Session-bound cash execution fixtures; all API calls use in-memory doubles."""
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import unittest
import test_toss_cash_operation as fixture
from trading_runtime.stock_session_profile import profile_hash,profile_for_spec
from trading_runtime.toss_session_scope import TossSessionSource,TossCalendarArchive

KST=timezone(timedelta(hours=9))

def payload(day):
    rows=[]
    for delta in (-1,0,1):
        date=day+timedelta(days=delta)
        def stamp(hour,minute=0):return datetime(date.year,date.month,date.day,hour,minute,tzinfo=KST).isoformat()
        rows.append({'date':date.isoformat(),'integrated':{
            'preMarket':{'startTime':stamp(8),'singlePriceAuctionStartTime':stamp(8,50),'endTime':stamp(9)},
            'regularMarket':{'startTime':stamp(9),'singlePriceAuctionStartTime':stamp(15,20),'endTime':stamp(15,30)},
            'afterMarket':{'startTime':stamp(15,30),'singlePriceAuctionEndTime':stamp(15,40),'endTime':stamp(20)}}})
    return dict(zip(('previousBusinessDay','today','nextBusinessDay'),rows))

class ExtendedCashOperationTests(unittest.TestCase):
    def setUp(self):
        self.fx=fixture.OperationTests('runTest');self.fx.setUp();self.addCleanup(self.fx.doCleanups)
        fx=self.fx
        fx.clock.now=datetime(2026,11,18,8,20,5,tzinfo=KST).timestamp()
        fx.specs=[replace(fx.specs[0],parameters={**fx.specs[0].parameters,'stockSessionProfile':'EXTENDED'})]
        original=fx.api.stocks
        fx.api.stocks=lambda symbols:[{**row,'koreanMarketDetail':{'nxtSupported':True,'nxtTradingSuspended':False,'krxTradingSuspended':False}} for row in original(symbols)]
        def calendar(market):
            fx.api.last_observation={'receivedAt':fx.clock()}
            return payload(datetime.fromtimestamp(fx.clock(),KST).date())
        fx.api.market_calendar=calendar
        fx.api._stock_session_source=TossSessionSource(fx.api,archive=TossCalendarArchive(fx.root/'calendar.sqlite3'),clock=fx.clock)
        class ExtendedFeed(fixture.Feed):
            def __init__(self,specs,clock):
                super().__init__(specs,clock)
                for sub in self.subscriptions:sub.stock_session_profile=profile_for_spec(specs[0])
            def warmup(self,subscription,limit):
                profile=subscription.stock_session_profile
                return [replace(b,session='pre',session_profile_hash=profile_hash(profile),source_venue=profile['venue']) for b in super().warmup(subscription,limit)]
        fx.op.feed_factory=lambda specs,api:ExtendedFeed(specs,fx.clock)

    def decision(self):
        decision=self.fx.decision()
        profile=profile_for_spec(self.fx.specs[0])
        return replace(decision,bar=replace(decision.bar,session='pre',session_profile_hash=profile_hash(profile),source_venue=profile['venue']))

    def test_preparation_is_readonly_and_dispatch_keeps_profile_out_of_wire(self):
        fx=self.fx;prepared=fx.prepare()['preparation']
        self.assertEqual(fx.api.calls,[]);self.assertFalse(fx.op.ledger.path.exists())
        fx.op.start({'preparationId':prepared['preparationId'],'sourceHash':prepared['sourceHash'],'liveConsent':True},spawn=False)
        self.assertTrue(fx.op.dispatch_signal(fx.specs[0],self.decision(),fx.clock()))
        wire=fx.api.calls[-1][1]
        self.assertEqual(set(wire),{'clientOrderId','symbol','side','quantity','price','orderType','timeInForce'})
        self.assertEqual((wire['orderType'],wire['timeInForce']),('LIMIT','DAY'))
        request=fx.op.ledger.orders(fx.op.owner)[0]['request']
        self.assertEqual(request['stockSessionProfileHash'],profile_hash(profile_for_spec(fx.specs[0])))
        self.assertEqual(request['stockSessionProfile']['mode'],'EXTENDED')

    def test_regular_bar_cannot_drive_an_extended_order(self):
        fx=self.fx;fx.start()
        with self.assertRaisesRegex(ValueError,'SIGNAL_PROFILE_MISMATCH'):
            fx.op.dispatch_signal(fx.specs[0],fx.decision(),fx.clock())
        self.assertEqual(fx.api.calls,[])

    def test_pre_auction_boundary_cancels_exact_owned_order_once(self):
        fx=self.fx;fx.start();fx.op.dispatch_signal(fx.specs[0],self.decision(),fx.clock())
        fx.clock.now=datetime(2026,11,18,8,50,tzinfo=KST).timestamp()
        fx.op.reconcile(cancel=True,closed_scope_only=True)
        fx.op.reconcile(cancel=True,closed_scope_only=True)
        self.assertEqual([c for c in fx.api.calls if c[0]=='CANCEL'],[('CANCEL','o1')])
        self.assertEqual(fx.op.ledger.project(fx.op.owner)['unresolvedOrders'],0)

    def test_unfilled_order_in_active_profile_is_not_canceled(self):
        fx=self.fx;fx.start();fx.op.dispatch_signal(fx.specs[0],self.decision(),fx.clock())
        fx.op.reconcile(cancel=True,closed_scope_only=True)
        self.assertFalse(any(c[0]=='CANCEL' for c in fx.api.calls))

    def test_quote_from_prior_session_is_rejected(self):
        from trading_runtime.toss_session_scope import assert_quote_session
        context={'start':'2026-11-18T08:00:00+09:00','end':'2026-11-18T08:50:00+09:00'}
        with self.assertRaisesRegex(ValueError,'OUTSIDE_PROFILE_SESSION'):
            assert_quote_session({'sourceTimestamp':'2026-11-18T07:59:59+09:00'},context)

    def test_us_day_pre_after_each_submit_only_the_selected_original_window(self):
        fx=self.fx
        def us_calendar(market):
            fx.api.last_observation={'receivedAt':fx.clock()}
            today=datetime.fromtimestamp(fx.clock(),KST).date()
            rows=[]
            for offset in (-1,0,1):
                day=today+timedelta(days=offset)
                def stamp(hour):return datetime(day.year,day.month,day.day,hour,tzinfo=timezone.utc).isoformat()
                rows.append({'date':day.isoformat(),'dayMarket':{'startTime':stamp(1),'endTime':stamp(9)},
                    'preMarket':{'startTime':stamp(9),'endTime':stamp(14)},'regularMarket':{'startTime':stamp(14),'endTime':stamp(21)},
                    'afterMarket':{'startTime':stamp(21),'endTime':stamp(23)}})
            return dict(zip(('previousBusinessDay','today','nextBusinessDay'),rows))
        fx.api.market_calendar=us_calendar
        class USFeed(fixture.Feed):
            def __init__(self,specs,clock):
                super().__init__(specs,clock)
                for sub in self.subscriptions:sub.stock_session_profile=profile_for_spec(specs[0])
            def warmup(self,sub,limit):
                phase=sub.stock_session_profile['segments'][0].lower()
                return [replace(b,session=phase,session_profile_hash=profile_hash(sub.stock_session_profile),source_venue='TOSS_US') for b in super().warmup(sub,limit)]
        fx.op.feed_factory=lambda specs,api:USFeed(specs,fx.clock)
        for phase,hour in (('DAY',2),('PRE',10),('AFTER',22)):
            with self.subTest(phase=phase):
                fx.clock.now=datetime(2026,11,18,hour,tzinfo=timezone.utc).timestamp()
                fx.specs=[replace(fixture.spec('AAPL'),parameters={**fixture.spec('AAPL').parameters,'stockSessionProfile':{'mode':'EXTENDED','segments':[phase]}})]
                fx.start();decision=fx.decision();profile=profile_for_spec(fx.specs[0])
                decision=replace(decision,bar=replace(decision.bar,session=phase.lower(),session_profile_hash=profile_hash(profile),source_venue='TOSS_US'))
                self.assertTrue(fx.op.dispatch_signal(fx.specs[0],decision,fx.clock()))
                entry=fx.op.ledger.orders(fx.op.owner)[0]
                self.assertEqual(entry['request']['stockSessionProfile']['segments'],[phase])
                self.assertEqual(entry['request']['currency'],'USD')
                fx.op.reconcile(cancel=True);fx.op._finish()

if __name__=='__main__':unittest.main()
