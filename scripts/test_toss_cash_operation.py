"""Offline contract regressions; no app, credential, private API or live order."""
import copy
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from trading_runtime.continuous_runtime import ClosedBar, RuntimeStrategySpec, StrategyBarDecision
from trading_runtime.order_coordination import OrderCoordinator
from trading_runtime.toss_budget import TossBudgetPool
from live_trader.toss_cash_operation import TossCashOperation, stock_metadata, PURPOSE
from live_trader.toss_cash_ledger import TossCashLedger, terminal_fill
from live_trader.toss_cash_sources import validate_spec


def iso(value): return datetime.fromtimestamp(value,timezone.utc).isoformat()


def spec(symbol='005930',identifier='target-1:s1',timeframe='1m'):
    market='KR' if symbol=='005930' else 'US'
    return RuntimeStrategySpec('p','a'*64,identifier,'s','b'*64,'moving_average_cross',
        'toss:'+market+':'+symbol,symbol,timeframe,'toss','toss',1,
        {'shortMa':2,'longMa':3,'orderQuantity':1},{'asset':'stock'})


def bar(s,end):
    width=60 if s.timeframe=='1m' else 3600
    return ClosedBar(s.instrument_id,s.symbol,'toss',s.timeframe,iso(end-width),iso(end),100,101,99,100,10)


class FixtureAPI:
    def __init__(self,clock):
        self.clock=clock; self.positions={'005930':Decimal(4),'AAPL':Decimal(2)}
        self.original=dict(self.positions); self.calls=[]; self.book={}; self.fail_ack=False; self.fee_null=False
        self.fx_rate='1500'; self.receipt_offset=0; self.last_observation=None
    def accounts(self): return [{'accountSeq':1,'accountNo':'1234567890','accountType':'BROKERAGE'}]
    def stocks(self,symbols):
        return [{'symbol':s,'currency':'KRW' if s=='005930' else 'USD','market':'KOSPI' if s=='005930' else 'NASDAQ',
                 'status':'ACTIVE','securityType':'STOCK' if s=='005930' else 'FOREIGN_STOCK'} for s in symbols]
    def holdings(self,symbol=None):
        return {'items':[{'symbol':symbol,'currency':'KRW' if symbol=='005930' else 'USD','quantity':str(self.positions[symbol])}]}
    def buying_power(self,currency): return {'currency':currency,'cashBuyingPower':'1000000' if currency=='KRW' else '1000'}
    def sellable_quantity(self,symbol): return {'sellableQuantity':str(self.positions[symbol])}
    def commissions(self): return [{'marketCountry':m,'commissionRate':'0.001'} for m in ('KR','US')]
    def orders(self,**kwargs):
        return {'orders':[r for r in self.book.values() if r['status']=='PENDING' and r['symbol']==kwargs.get('symbol')],'hasNext':False}
    def conditional_orders(self,**kwargs): return {'conditionalOrders':[],'hasNext':False}
    def exchange_rate(self):
        self.last_observation={'receivedAt':self.clock()+self.receipt_offset}
        return {'baseCurrency':'USD','quoteCurrency':'KRW','rate':self.fx_rate,'validFrom':iso(self.clock()-3600),'validUntil':iso(self.clock()+3600)}
    def orderbook(self,symbol):
        self.clock.advance(.01); now=self.clock()
        self.last_observation={'receivedAt':now+self.receipt_offset}
        return {'currency':'KRW' if symbol=='005930' else 'USD','timestamp':iso(now),'asks':[{'price':'100','volume':'100'}],'bids':[{'price':'99','volume':'100'}]}
    def place_order(self,wire):
        self.calls.append(('POST',dict(wire)))
        if self.fail_ack: raise RuntimeError('FIXTURE_LOST_ACK')
        oid='o'+str(len(self.book)+1)
        self.book[oid]={**wire,'orderId':oid,'currency':'KRW' if wire['symbol']=='005930' else 'USD','status':'PENDING',
                       'execution':{'filledQuantity':'0','filledAmount':'0','commission':'0','tax':'0','filledAt':None}}
        return {'orderId':oid,'clientOrderId':wire['clientOrderId']}
    def get_order(self,oid): return copy.deepcopy(self.book[oid])
    def cancel_order(self,oid):
        self.calls.append(('CANCEL',oid)); self.book[oid]['status']='CANCELED'; return {'orderId':oid}
    def fill(self,oid,quantity=None,*,status='FILLED'):
        row=self.book[oid]; qty=Decimal(quantity or row['quantity']); fee=Decimal('0.1')
        self.positions[row['symbol']]+=qty*(1 if row['side']=='BUY' else -1)
        row.update(status=status,execution={'filledQuantity':str(qty),'filledAmount':str(qty*Decimal(row['price'])),
            'commission':None if self.fee_null else str(fee),'tax':'0','filledAt':iso(self.clock())})


class Clock:
    def __init__(self): self.now=datetime(2026,11,6,4,0,5,tzinfo=timezone.utc).timestamp()
    def __call__(self): return self.now
    def advance(self,s): self.now+=s


class Feed:
    def __init__(self,specs,clock):
        self.specs=specs; self.clock=clock
        self.subscriptions=list({s.stream_key:SimpleNamespace(instrument_id=s.instrument_id,symbol=s.symbol,timeframe=s.timeframe) for s in specs}.values())
    def connect(self): pass
    def disconnect(self): pass
    def warmup(self,subscription,limit):
        s=next(s for s in self.specs if s.symbol==subscription.symbol and s.timeframe==subscription.timeframe)
        step=60 if s.timeframe=='1m' else 3600
        return [bar(s,int(self.clock()//step)*step-(limit-1-i)*step) for i in range(limit)]


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory(); self.addCleanup(self.tmp.cleanup); self.root=Path(self.tmp.name)
        self.clock=Clock(); self.api=FixtureAPI(self.clock); self.specs=[spec()]; self.hash='c'*64
        self.settings={'TOSS_CLIENT_ID':'fixture','TOSS_CLIENT_SECRET':'not-real','TOSS_ACCOUNT_SEQ':'1'}
        self.writable=[]
        def factory(values,readonly):
            self.writable.append(not readonly); return self.api
        def resolve(config):
            return tuple(self.specs),[],{s.strategy_instance_id:str(1/len([x for x in self.specs if (x.symbol=='005930')==(s.symbol=='005930')])) for s in self.specs},self.hash
        @contextmanager
        def boundary(): yield {'active':False}
        self.op=TossCashOperation(self.root/'op',settings=lambda:dict(self.settings),client_factory=factory,resolver=resolve,
            feed_factory=lambda specs,api:Feed(specs,self.clock),pool=TossBudgetPool(self.root/'pool.sqlite3',clock=self.clock),
            coordinator=OrderCoordinator(self.root/'owners.sqlite3',clock=self.clock),clock=self.clock,monotonic=self.clock,boundary=boundary)
        self.open=patch('live_trader.toss_cash_operation.regular_open',return_value=True); self.open.start(); self.addCleanup(self.open.stop)
    def prepare(self):
        return self.op.prepare({'targets':[{'fixture':'immutable'}],'tossBindingConfirmed':True,'budgetKrwByMarket':{'KR':300000,'US':300000},'durationHours':6})
    def start(self):
        p=self.prepare()['preparation']; return self.op.start({'preparationId':p['preparationId'],'sourceHash':p['sourceHash'],'liveConsent':True},spawn=False)
    def decision(self,s=None,side='BUY'):
        s=s or self.specs[0]
        return StrategyBarDecision(s.strategy_instance_id,s.strategy_id,side,'fixture',bar(s,int(self.clock()//60)*60),'e')
    def dispatch(self,side='BUY'):
        return self.op.dispatch_signal(self.specs[0],self.decision(side=side),self.clock())

    def test_prepare_never_orders_claims_or_creates_owned_ledger(self):
        result=self.prepare()
        self.assertTrue(result['readOnly']); self.assertEqual(result['purpose'],PURPOSE)
        self.assertFalse(result['nativePromotion']); self.assertEqual(self.api.calls,[])
        self.assertFalse(any(self.writable)); self.assertFalse(self.op.ledger.path.exists())
        self.assertFalse(self.op.pool.path.exists()); self.assertFalse(self.op.coordinator.path.exists())

    def test_preferences_save_without_credentials_or_network(self):
        self.op.factory=lambda *_:self.fail('preferences must not contact broker')
        value=self.op.preferences({'budgetKrwByMarket':{'KR':300000,'US':300000},'durationHours':6})
        self.assertTrue(value['ok']); self.assertEqual(self.op.preferences()['budgetKrwByMarket']['US'],300000)

    def test_us_foreign_stock_and_fixed_usd_budget(self):
        self.specs=[spec('AAPL')]
        p=self.prepare()['preparation']; self.assertEqual(p['budgets'],{'USD':'200.00'})
        self.api.fx_rate='1000'
        self.op.start({'preparationId':p['preparationId'],'sourceHash':p['sourceHash'],'liveConsent':True},spawn=False)
        self.assertEqual(self.op.ledger.project(self.op.owner)['cash'],{'USD':'200.00'})

    def test_us_foreign_etf_and_numeric_baseline_format(self):
        self.specs=[spec('AAPL')]
        original=self.api.stocks
        self.api.stocks=lambda symbols:[{**r,'securityType':'FOREIGN_ETF'} for r in original(symbols)]
        p=self.prepare()['preparation']
        self.api.positions['AAPL']=Decimal('2.000')
        self.op.start({'preparationId':p['preparationId'],'sourceHash':p['sourceHash'],'liveConsent':True},spawn=False)
        self.assertEqual(self.op.ledger.project(self.op.owner)['cash'],{'USD':'200.00'})

    def test_account_picker_requires_no_seq_and_masks_account(self):
        from live_trader.toss_adapter import account_choices
        result=account_choices(self.api,{'TOSS_CLIENT_ID':'fixture','TOSS_CLIENT_SECRET':'not-real'})
        self.assertEqual(result['accounts'],[{'accountSeq':'1','label':'토스증권 · ••••7890','selected':False}])
        self.assertNotIn('1234567890',str(result)); self.assertEqual(self.api.calls,[])
        self.settings.pop('TOSS_ACCOUNT_SEQ')
        with self.assertRaisesRegex(ValueError,'ACCOUNT_SELECTION_REQUIRED'): self.prepare()

    def test_identity_edit_denied_until_owned_cleanup_finished(self):
        writes=[]
        self.assertTrue(self.op.save_settings({'TOSS_CLIENT_ID':'new'},lambda:writes.append(1) or {'ok':True})['ok'])
        self.start()
        self.assertFalse(self.op.save_settings({'TOSS_ACCOUNT_SEQ':'2'},lambda:writes.append(2) or {'ok':True})['ok'])
        self.assertEqual(writes,[1])
        self.op._finish()
        self.assertTrue(self.op.save_settings({'TOSS_ACCOUNT_SEQ':'2'},lambda:writes.append(3) or {'ok':True})['ok'])

    def test_external_identity_change_never_cancels_with_another_account(self):
        self.start(); self.dispatch()
        self.settings['TOSS_CLIENT_SECRET']='changed-outside-app'
        with self.assertRaisesRegex(ValueError,'CREDENTIALS_CHANGED'): self.op._finish()
        self.assertEqual([row[0] for row in self.api.calls],['POST'])
        self.assertEqual(self.op.ledger.project(self.op.owner)['unresolvedOrders'],1)
        self.assertEqual(self.op.ledger.unfinished(),[self.op.owner])
        self.assertFalse(any(row['kind']=='CANCEL_INTENT' for row in self.op.ledger.facts(self.op.owner)))

    def test_post_uses_same_settings_snapshot_after_account_verification(self):
        self.start(); captured=[]; original=self.op.factory
        def factory(values,read_only):
            if read_only:
                api=original(values,read_only)
                class Checked:
                    def accounts(inner):
                        result=api.accounts(); self.settings['TOSS_ACCOUNT_SEQ']='2'; return result
                return Checked()
            captured.append(dict(values)); return original(values,read_only)
        self.op.factory=factory
        self.dispatch()
        self.assertEqual(captured[0]['TOSS_ACCOUNT_SEQ'],'1')
        self.assertEqual(len(self.api.calls),1)

    def test_concurrent_cancel_claim_sends_only_once(self):
        self.start(); self.dispatch()
        barrier=threading.Barrier(2); original=self.op.ledger.append
        def append(owner,kind,key,value):
            if kind=='CANCEL_INTENT': barrier.wait(timeout=5)
            return original(owner,kind,key,value)
        self.op.ledger.append=append
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(self.op.reconcile,cancel=True) for _ in range(2)]
            for future in futures: future.result(timeout=10)
        self.assertEqual([row[0] for row in self.api.calls],['POST','CANCEL'])

    def test_recovery_and_owned_close_share_serialization_lock(self):
        self.start(); entered=threading.Event(); release=threading.Event(); calls=[]
        original_finish=self.op._finish
        def finish():
            entered.set(); self.assertTrue(release.wait(5)); original_finish()
        self.op._finish=finish
        with ThreadPoolExecutor(max_workers=2) as pool:
            recovery=pool.submit(self.op.recover,{'ownerId':self.op.owner,'cancelOwnedOrders':True})
            self.assertTrue(entered.wait(5))
            close=pool.submit(self.op.close_owned,{'ownerId':self.op.owner,'liveConsent':True})
            self.assertFalse(close.done())
            release.set(); self.assertTrue(recovery.result(timeout=5)['ok'])
            with self.assertRaisesRegex(ValueError,'EXACT_RECOVERY_OWNER_REQUIRED'): close.result(timeout=5)
        self.assertEqual(self.api.calls,[])

    def test_shared_quote_before_signal_defers_without_order(self):
        self.start(); observed=self.clock(); self.api.receipt_offset=-.5
        self.assertFalse(self.op.dispatch_signal(self.specs[0],self.decision(),observed))
        self.assertEqual(self.api.calls,[]); self.assertEqual(self.op.ledger.orders(self.op.owner),[])
        self.api.receipt_offset=0
        self.assertTrue(self.op.dispatch_signal(self.specs[0],self.decision(),observed))
        self.assertEqual(len(self.api.calls),1)

    def test_native_owned_capital_reserves_same_currency_only(self):
        from live_trader.owned_deployment_capital import OwnedCapitalStore, reviewed_scope
        account={'accountFingerprint':'f'*64,'credentialConfigurationHash':'c'*64,'profileId':'toss-primary','accountBindingId':'binding','brokerId':'toss'}
        store=OwnedCapitalStore(self.root/'native.sqlite3')
        def initialize(identifier,currency):
            deployment={'deploymentId':identifier,'environment':'LIVE','lifecycle':'before-live-small','accountId':'binding','route':'toss',
                'permissions':{'liveReviewBindingHash':'r'*64,'liveAccountProfileId':'toss-primary','liveAccountFingerprint':'f'*64,
                    'liveAccountCredentialConfigurationHash':'c'*64,'liveCapitalPolicy':{'basis':'ACCOUNT_AVAILABLE_CASH','percent':'100'}}}
            scope=reviewed_scope(deployment,account,[{'strategyInstanceId':'s1','sourceArtifactHash':'a'*64,'portfolioHash':'p'*64,'symbol':'005930' if currency=='KRW' else 'AAPL','quoteCurrency':currency}])
            return store.initialize(scope,{'accountBinding':'SAVED_CREDENTIAL_CONFIGURATION_OBSERVED','profileId':'toss-primary','currency':currency,
                'openOrders':'SYMBOL_NONE_OBSERVED','asOf':iso(self.clock()),'availableCash':'300000' if currency=='KRW' else '200'},now=datetime.fromtimestamp(self.clock(),timezone.utc))
        initialize('domestic','KRW'); initialize('us','USD')
        with self.assertRaisesRegex(ValueError,'ALREADY_RESERVED'): initialize('us-duplicate','USD')

    def test_fx_shared_receipt_is_not_refreshed_by_read(self):
        self.specs=[spec('AAPL')]; self.api.receipt_offset=-61
        with self.assertRaisesRegex(ValueError,'FX_RECEIPT'): self.prepare()

    def test_exact_consent_source_and_ttl(self):
        p=self.prepare()['preparation']; request={'preparationId':p['preparationId'],'sourceHash':p['sourceHash'],'liveConsent':False}
        with self.assertRaisesRegex(ValueError,'CONSENT'): self.op.start(request,spawn=False)
        request['liveConsent']=True; self.hash='d'*64
        with self.assertRaisesRegex(ValueError,'SCOPE_CHANGED'): self.op.start(request,spawn=False)
        self.hash='c'*64; self.clock.advance(181)
        with self.assertRaisesRegex(ValueError,'EXPIRED'): self.op.start(request,spawn=False)
        self.assertEqual(self.api.calls,[])

    def test_expiry_during_slow_start_reads_rechecked_before_claim(self):
        p=self.prepare()['preparation']; original=self.api.buying_power
        def slow(currency): self.clock.advance(181); return original(currency)
        self.api.buying_power=slow
        with self.assertRaisesRegex(ValueError,'EXPIRED'): self.op.start({'preparationId':p['preparationId'],'sourceHash':p['sourceHash'],'liveConsent':True},spawn=False)
        self.assertFalse(self.op.ledger.path.exists())

    def test_owned_roundtrip_exact_terminal_amount_fees_and_existing_holdings_preserved(self):
        self.start(); self.dispatch(); self.api.fill('o1'); self.op.reconcile()
        p=self.op.ledger.project(self.op.owner); self.assertEqual(p['cash']['KRW'],'299899.9'); self.assertEqual(p['positions']['target-1:s1'],'1')
        self.dispatch('SELL'); self.api.fill('o2'); self.op.reconcile(); self.op._finish()
        self.assertEqual(self.api.positions,self.api.original)
        self.assertEqual(self.op.ledger.project(self.op.owner)['cash']['KRW'],'299998.8')
        self.assertEqual(self.op.ledger.unfinished(),[])
        self.assertEqual([c[1]['side'] for c in self.api.calls],['BUY','SELL'])

    def test_unknown_ack_has_one_intent_and_no_retry_or_guess_cancel(self):
        self.start(); self.api.fail_ack=True
        with self.assertRaisesRegex(RuntimeError,'LOST_ACK'): self.dispatch()
        with self.assertRaisesRegex(ValueError,'UNKNOWN_NO_RETRY'): self.op.reconcile(cancel=True)
        self.assertFalse(self.dispatch())
        self.assertEqual(len(self.api.calls),1); self.assertEqual(self.op.ledger.project(self.op.owner)['unresolvedOrders'],1)
        self.assertEqual(len(self.op.pool.inspect(account_identity=self.op.prepared['identity'],currency='KRW')),1)

    def test_cancel_is_at_most_once_and_partial_fill_is_owned(self):
        self.specs=[replace(spec(),parameters={'shortMa':2,'longMa':3,'orderQuantity':2})]
        self.start(); self.dispatch(); self.api.fill('o1','1',status='PARTIAL_FILLED')
        self.op.reconcile(cancel=True); self.op.reconcile(cancel=True)
        self.assertEqual([c[0] for c in self.api.calls],['POST','CANCEL'])
        self.assertEqual(self.op.ledger.project(self.op.owner)['positions']['target-1:s1'],'1')
        self.op._finish(); self.assertEqual(self.op.view['state'],'RECONCILIATION_REQUIRED')

    def test_missing_terminal_fee_does_not_become_zero(self):
        self.start(); self.dispatch(); self.api.fee_null=True; self.api.fill('o1')
        with self.assertRaises(ValueError): self.op.reconcile()
        self.assertEqual(self.op.ledger.project(self.op.owner)['unresolvedOrders'],1)

    def test_existing_shares_cannot_be_sold_and_external_change_blocks(self):
        self.start(); self.dispatch('SELL'); self.assertEqual(self.api.calls,[])
        self.api.positions['005930']+=1
        with self.assertRaisesRegex(ValueError,'EXTERNAL_POSITION_CHANGE'): self.dispatch()
        self.assertEqual(self.api.calls,[])

    def test_stale_original_quote_receipt_never_orders(self):
        self.start(); self.api.receipt_offset=-31
        with self.assertRaisesRegex(ValueError,'QUOTE_STALE'): self.dispatch()
        self.assertEqual(self.api.calls,[])

    def test_same_stream_all_instances_evaluated_once_even_while_order_pending(self):
        self.specs=[spec(),spec(identifier='target-2:s2')]; self.start(); self.dispatch()
        self.op.evaluator=lambda s,b,h:StrategyBarDecision(s.strategy_instance_id,s.strategy_id,'HOLD','fixture',b,'e')
        self.clock.advance(60); b=bar(self.specs[0],int(self.clock()//60)*60)
        before=len(self.op.histories[self.specs[0].stream_key]); self.op.ingest_bar(b)
        self.assertEqual(self.op.view['evaluations'],2)
        self.assertEqual(len(self.op.histories[self.specs[0].stream_key]),before+1)
        self.op.ingest_bar(b); self.assertEqual(self.op.view['evaluations'],2)

    def test_closed_session_defers_observed_signal_and_stop_discards(self):
        self.start(); self.op.pending_signals={'target-1:s1':(self.specs[0],self.decision(),self.clock())}
        with patch('live_trader.toss_cash_operation.regular_open',return_value=False): self.op.dispatch_pending()
        self.assertEqual(self.api.calls,[]); self.assertEqual(len(self.op.pending_signals),1)
        observed=self.clock(); self.clock.advance(3600)
        with patch('live_trader.toss_cash_operation.regular_open',side_effect=lambda symbol,stamp:stamp>observed),patch('live_trader.toss_cash_operation.next_session_window',return_value=True): self.op.dispatch_pending()
        self.assertEqual(len(self.api.calls),1)
        self.op.pending_signals={'again':(self.specs[0],self.decision(),self.clock())}
        self.op.stop({'ownerId':self.op.owner}); self.assertEqual(self.op.pending_signals,{})

    def test_partial_release_can_finish_again_without_releasing_other_owner(self):
        self.start(); claim=self.op.claims[0]; original=self.op.pool.release
        self.op.pool.release=lambda **kw:(_ for _ in ()).throw(RuntimeError('fixture release interrupted'))
        with self.assertRaisesRegex(RuntimeError,'interrupted'): self.op._finish()
        self.assertEqual(self.op.coordinator.inspect_scope(scope_key=self.op.scopes[0]['scopeKey'])['status'],'RELEASED')
        self.op.pool.release=original; self.op._finish(); self.assertEqual(self.op.ledger.unfinished(),[])

    def test_shared_account_currency_claim_blocks_gpt_but_other_currency_independent(self):
        self.start(); identity=self.op.prepared['identity']
        with self.assertRaises(RuntimeError): self.op.pool.claim(account_identity=identity,owner_id='gpt-other',currency='KRW',limit='300000')
        usd=self.op.pool.claim(account_identity=identity,owner_id='gpt-other',currency='USD',limit='200')
        self.assertEqual(usd['currency'],'USD')

    def test_cash_sources_reject_crypto_short_and_leverage(self):
        for s in (replace(spec(),artifact={'asset':'crypto'}),replace(spec(),parameters={'positionDirection':'short'}),replace(spec(),parameters={'leverage':2})):
            with self.subTest(s=s),self.assertRaises(ValueError): validate_spec(s)
        self.assertEqual(validate_spec(spec())[2],'KRW')

    def test_explicit_owned_close_requires_consent_and_sells_no_existing_asset(self):
        self.start(); self.dispatch(); self.api.fill('o1'); self.op.reconcile(); self.op._finish()
        self.assertEqual(self.api.positions['005930'],5)
        with self.assertRaisesRegex(ValueError,'CONSENT'): self.op.close_owned({'ownerId':self.op.owner,'liveConsent':False})
        with patch('live_trader.toss_cash_operation.threading.Thread') as thread:
            thread.return_value.is_alive.return_value=False
            self.op.close_owned({'ownerId':self.op.owner,'liveConsent':True})
        self.op.thread=None
        def fill_wait(_seconds):
            self.clock.advance(2)
            for oid,row in self.api.book.items():
                if row['status']=='PENDING': self.api.fill(oid)
            return False
        self.op.stop_event.wait=fill_wait
        self.op._close_owned_worker()
        self.assertEqual(self.api.positions,self.api.original)
        self.assertEqual(self.api.calls[-1][1]['quantity'],'1')
        self.assertEqual(self.api.calls[-1][1]['side'],'SELL')
        self.assertEqual(self.op.ledger.unfinished(),[])

    def test_terminal_limit_price_change_and_ledger_rewrite_are_rejected(self):
        self.start(); self.dispatch(); self.api.fill('o1'); self.api.book['o1']['price']='999'
        with self.assertRaisesRegex(ValueError,'PRICE_MISMATCH'): self.op.reconcile()
        fact=self.op.ledger.facts(self.op.owner)[0]
        with self.assertRaisesRegex(ValueError,'IDENTITY_CONFLICT'): self.op.ledger.append(self.op.owner,'START',self.op.owner+':start',{'different':True})

    def test_async_preparation_status_does_not_wait_for_network(self):
        import threading
        waiting=threading.Event(); proceed=threading.Event(); original=self.op.factory
        def slow(values,readonly): waiting.set(); proceed.wait(2); return original(values,readonly)
        self.op.factory=slow
        try:
            value=self.op.begin_prepare({'targets':[{}],'tossBindingConfirmed':True,'budgetKrwByMarket':{'KR':300000,'US':300000},'durationHours':6})
            self.assertTrue(waiting.wait(1)); self.assertTrue(self.op.status()['preparing'])
            with self.assertRaisesRegex(ValueError,'ALREADY_RUNNING'): self.op.begin_prepare({})
        finally:
            proceed.set(); self.op.preparation_thread.join(2)
        self.assertEqual(self.op.status()['state'],'PREPARED'); self.assertEqual(self.api.calls,[])

    def test_source_percentage_and_integer_quantity_are_preserved(self):
        from live_trader.toss_cash_sources import source_sizing
        s=replace(spec(),artifact={'asset':'stock','executionSizing':{'paperOrderQuantity':'3'}},parameters={'positionSize':5})
        self.assertEqual(source_sizing(s),(Decimal(3),Decimal(5)))
        with self.assertRaisesRegex(ValueError,'LONG'): validate_spec(replace(spec(),artifact={'asset':'stock','position_direction':'short'}))
        with self.assertRaisesRegex(ValueError,'WHOLE_SHARE'): source_sizing(replace(s,artifact={'order_quantity':.5}))


if __name__=='__main__': unittest.main()
