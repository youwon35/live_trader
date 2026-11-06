"""User-started Toss cash operation; no native-promotion or account-wide trades.

Preparation is read-only. The only mutation path starts after the exact prepared
scope and live consent are bound, and is protected by shared account budgets,
symbol ownership, durable intents, immutable sources and the emergency latch.
"""
from __future__ import annotations
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import os
import threading
import time
import uuid

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.order_coordination import OrderCoordinator, credential_account_identity
from trading_runtime.provider_backfill_toss import TossClosedBarFeed, toss_market
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator, required_warmup_bars
from trading_runtime.toss_budget import TossBudgetPool
from trading_runtime.toss_execution import (EXECUTION_MODEL, amount, epoch, normalize_orderbook,
    budget_ticket, native_budget_from_krw, normalize_exchange_rate)
from trading_runtime.toss_securities import TossClient
from trading_runtime.market_calendar import market_session_state, load_exchange_calendar
from trading_runtime.market_data import parse_timestamp, timeframe_seconds
from trading_runtime.provider_backfill_toss import _session
from .toss_cash_ledger import TossCashLedger
from .toss_cash_sources import resolve_sources, source_catalog, source_sizing
from .toss_adapter import exact_owned_order

PURPOSE='TOSS_CASH_OPERATION'


def consecutive(previous,current):
    market=toss_market(current.symbol)[1]
    pend=parse_timestamp(previous.end_time); cstart=parse_timestamp(current.start_time)
    if cstart==pend: return True
    popen,pclose=_session(parse_timestamp(previous.start_time),market)
    copen,_=_session(cstart,market)
    if copen==popen or cstart!=copen: return False
    width=timeframe_seconds(previous.timeframe)
    if previous.timeframe=='1d':
        if pend!=pclose: return False
    elif (pclose-popen).total_seconds()//width*width!=(pend-popen).total_seconds(): return False
    dates=load_exchange_calendar('XKRX' if market=='KR' else 'XNYS').sessions_between(popen.date(),copen.date(),inclusive=True)
    return len(dates)==2 and dates[0]==popen.date() and dates[-1]==copen.date()


def regular_open(symbol,now):
    market=toss_market(symbol)[1]
    result=market_session_state('XKRX' if market=='KR' else 'XNYS',datetime.fromtimestamp(now,timezone.utc),
        regular_open='09:00' if market=='KR' else '09:30',regular_close='15:30' if market=='KR' else '16:00')
    if result['state']=='unknown': raise ValueError('TOSS_MARKET_CALENDAR_UNKNOWN')
    return result['orderable'] is True


def next_session_window(symbol,prior,current):
    """Only the next verified regular opening, never an arbitrary future day."""
    from zoneinfo import ZoneInfo
    market=toss_market(symbol)[1]; calendar=load_exchange_calendar('XKRX' if market=='KR' else 'XNYS')
    previous_open,_=_session(datetime.fromtimestamp(prior,timezone.utc),market)
    opened,_=_session(datetime.fromtimestamp(current,timezone.utc),market)
    zone=ZoneInfo(calendar.timezone)
    days=calendar.sessions_between(previous_open.astimezone(zone).date(),opened.astimezone(zone).date(),inclusive=True)
    return len(days)==2 and 0<=current-opened.timestamp()<=300 and 0<=current-prior<=7*86400


def signal_current(symbol,observed,current):
    if not regular_open(symbol,observed): return next_session_window(symbol,observed,current)
    return 0<=current-observed<=30


def settings_fingerprint(settings):
    return stable_sha256({key:settings.get(key,'') for key in ('TOSS_CLIENT_ID','TOSS_CLIENT_SECRET','TOSS_ACCOUNT_SEQ')})


def exact_account(api, settings):
    if not str(settings.get('TOSS_ACCOUNT_SEQ') or '').strip():
        raise ValueError('TOSS_ACCOUNT_SELECTION_REQUIRED')
    rows=api.accounts()
    if not isinstance(rows,list): raise ValueError('TOSS_ACCOUNT_RESPONSE_INVALID')
    found=[row for row in rows if str(row.get('accountSeq'))==str(settings.get('TOSS_ACCOUNT_SEQ')) and row.get('accountType')=='BROKERAGE']
    if len(found)!=1: raise ValueError('TOSS_EXACT_ACCOUNT_REQUIRED')
    return credential_account_identity(broker='toss',api_key=settings.get('TOSS_CLIENT_ID',''),
        account_number=found[0].get('accountNo',''),account_product='VERIFIED_ACCOUNT_NUMBER')


def stock_metadata(api, symbols):
    rows=api.stocks(sorted(set(symbols)))
    if not isinstance(rows,list) or len(rows)!=len(set(symbols)) or {r.get('symbol') for r in rows}!=set(symbols):
        raise ValueError('TOSS_STOCK_METADATA_COVERAGE')
    for r in rows:
        _,market=toss_market(r['symbol']); currency='KRW' if market=='KR' else 'USD'
        venues={'KOSPI','KOSDAQ','KONEX'} if market=='KR' else {'NASDAQ','NYSE','AMEX'}
        kr=r.get('koreanMarketDetail') or {}
        if r.get('currency')!=currency or r.get('market') not in venues or r.get('status')!='ACTIVE' or r.get('securityType') not in ({'STOCK','ETF'} if market=='KR' else {'STOCK','FOREIGN_STOCK','ETF','FOREIGN_ETF'}) or kr.get('liquidationTrading') or kr.get('krxTradingSuspended'):
            raise ValueError('TOSS_STOCK_NOT_CASH_ORDERABLE')
    return rows


def open_orders(api, symbols):
    result=[]
    for symbol in sorted(set(symbols)):
        for method,field in ((api.orders,'orders'),(api.conditional_orders,'conditionalOrders')):
            raw=method(status='OPEN',symbol=symbol)
            if not isinstance(raw,dict) or raw.get('hasNext') is not False or not isinstance(raw.get(field),list):
                raise ValueError('TOSS_PENDING_ORDER_COVERAGE_INCOMPLETE')
            if any(row.get('symbol')!=symbol or not row.get('orderId') and field=='orders' for row in raw[field]):
                raise ValueError('TOSS_PENDING_ORDER_IDENTITY')
            result.extend(raw[field])
    return result


def holdings(api, symbols):
    result={}
    for symbol in sorted(set(symbols)):
        raw=api.holdings(symbol=symbol)
        if not isinstance(raw,dict) or not isinstance(raw.get('items'),list):
            raise ValueError('TOSS_HOLDINGS_INCOMPLETE')
        rows=raw['items']
        currency='KRW' if toss_market(symbol)[1]=='KR' else 'USD'
        if len(rows)>1 or any(r.get('symbol')!=symbol or (r.get('marketValue') or {}).get('currency',r.get('currency'))!=currency for r in rows):
            raise ValueError('TOSS_HOLDINGS_IDENTITY_MISMATCH')
        result[symbol]=str(amount(rows[0].get('quantity'))) if rows else '0'
    return result


def buying_power(api,currency):
    row=api.buying_power(currency=currency)
    if not isinstance(row,dict) or row.get('currency')!=currency: raise ValueError('TOSS_BUYING_POWER_CURRENCY_MISMATCH')
    return amount(row.get('cashBuyingPower'))


def receipt_time(api):
    observation=getattr(api,'last_observation',None)
    if not isinstance(observation,dict): raise ValueError('TOSS_ORIGINAL_RECEIPT_REQUIRED')
    return epoch(observation.get('receivedAt'))


def fx_observation(api,clock):
    payload=api.exchange_rate()
    return normalize_exchange_rate(payload,received_at=receipt_time(api),now=clock())


def commission_rates(api, markets):
    rows=api.commissions()
    if not isinstance(rows,list): raise ValueError('TOSS_COMMISSION_UNCONFIRMED')
    day=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo('Asia/Seoul')).date().isoformat()
    result={}
    for market in markets:
        selected=[r for r in rows if r.get('marketCountry')==market and (not r.get('startDate') or r['startDate']<=day) and (not r.get('endDate') or day<=r['endDate'])]
        if len(selected)!=1: raise ValueError('TOSS_COMMISSION_SCOPE_UNCONFIRMED')
        rate=amount(selected[0].get('commissionRate'))
        if rate>Decimal('.1'): raise ValueError('TOSS_COMMISSION_RATE_INVALID')
        # Additional 1% stays reserved for fees/rounding/taxes; actual official
        # terminal cost, not this estimate, drives the owned ledger.
        result[market]=str(max(rate,Decimal('.01')))
    return result


class TossCashOperation:
    def __init__(self,state_dir,*,settings=None,client_factory=None,resolver=resolve_sources,
                 feed_factory=None,pool=None,coordinator=None,clock=time.time,monotonic=time.monotonic,
                 boundary=None):
        self.root=Path(state_dir); self.ledger=TossCashLedger(self.root/'owned-cash.sqlite3')
        self.settings=settings or (lambda:dict(os.environ))
        self.factory=client_factory or (lambda values,read_only:TossClient.from_settings(values,read_only=read_only))
        self.resolve=resolver; self.feed_factory=feed_factory or (lambda specs,api:TossClosedBarFeed.from_specs(specs,client=api))
        self.pool=pool or TossBudgetPool(); self.coordinator=coordinator or OrderCoordinator()
        self.clock=clock; self.monotonic=monotonic
        if boundary is None:
            from .emergency_stop import emergency_stop_dispatch_boundary
            boundary=emergency_stop_dispatch_boundary
        self.boundary=boundary; self.lock=threading.RLock(); self.work_lock=threading.RLock(); self.stop_event=threading.Event()
        self.preparation_thread=None; self.pending_signals={}
        self.thread=None; self.prepared=None; self.owner=''; self.claims=[]; self.scopes=[]
        self.view={'state':'IDLE','purpose':PURPOSE,'executionModel':EXECUTION_MODEL,'nativePromotion':False,'ordersSubmitted':0,'evaluations':0,'activity':[]}

    def status(self):
        with self.lock:
            value=json.loads(json.dumps(self.view))
            value['running']=bool(self.thread and self.thread.is_alive())
            value['preparing']=bool(self.preparation_thread and self.preparation_thread.is_alive())
            value['unfinishedOwners']=self.ledger.unfinished()
            if not value['running'] and value['unfinishedOwners'] and value['state']=='IDLE':
                value['state']='RECOVERY_REQUIRED'; value['reason']='이전 운용의 소유 주문·보유분 대조가 필요합니다. 자동 재시작하지 않습니다.'
            if self.prepared and 'preparationId' in self.prepared:
                value['preparation']={k:self.prepared[k] for k in ('preparationId','expiresAt','sourceHash','budgets','budgetKrwByMarket','markets','durationHours')}
            return value

    def preferences(self,payload=None):
        path=self.root/'preferences.json'
        if payload is None:
            defaults={'budgetKrwByMarket':{'KR':300000,'US':300000},'durationHours':6}
            if path.exists(): defaults.update(json.loads(path.read_text(encoding='utf-8')))
            return {'ok':True,**defaults}
        if not isinstance(payload,dict) or set(payload)!={'budgetKrwByMarket','durationHours'} or set(payload['budgetKrwByMarket'])!={'KR','US'}:
            raise ValueError('TOSS_PREFERENCES_INVALID')
        if any(amount(v,positive=True)>1_000_000_000 for v in payload['budgetKrwByMarket'].values()) or amount(payload['durationHours'],positive=True)>24:
            raise ValueError('TOSS_PREFERENCES_RANGE_INVALID')
        self.root.mkdir(parents=True,exist_ok=True)
        temp=path.with_suffix('.'+uuid.uuid4().hex+'.tmp')
        temp.write_text(json.dumps(payload,ensure_ascii=False,allow_nan=False),encoding='utf-8'); temp.replace(path)
        return {'ok':True,**payload,'detail':'다음 준비 검사에 사용할 예산을 저장했습니다. 실행 중인 범위는 바뀌지 않습니다.'}

    def save_settings(self,values,writer):
        """Serialize identity edits with start; keep cleanup bound to its account."""
        with self.work_lock:
            if any(str(values.get(key) or '').strip() for key in ('TOSS_CLIENT_ID','TOSS_CLIENT_SECRET','TOSS_ACCOUNT_SEQ')):
                if ((self.thread and self.thread.is_alive()) or (self.preparation_thread and self.preparation_thread.is_alive())
                        or self.ledger.unfinished()):
                    return {'ok':False,'reason':'토스 준비·운용 또는 소유 주문 정리가 남아 있어 연결 계좌를 바꿀 수 없습니다.'}
                self.prepared=None
                self.view.update(state='IDLE',reason='연결 정보를 변경하여 주문 없는 준비 검사가 필요합니다.')
            return writer()

    def _note(self,kind,detail):
        with self.lock:
            self.view['activity']=([{'at':self.clock(),'kind':kind,'detail':detail}]+self.view.get('activity',[]))[:30]

    def begin_prepare(self,config):
        with self.lock:
            if self.preparation_thread and self.preparation_thread.is_alive(): raise ValueError('TOSS_PREPARATION_ALREADY_RUNNING')
            if self.thread and self.thread.is_alive() or self.ledger.unfinished(): raise ValueError('TOSS_EXISTING_RUN_RECONCILIATION_REQUIRED')
            self.prepared=None; self.view.update(state='PREPARING',reason='전략·계좌·과거 봉을 주문 없이 확인합니다.')
            def work():
                try: self.prepare(config)
                except Exception as exc:
                    self.view.update(state='PREPARATION_FAILED',reason=str(getattr(exc,'code',str(exc))))
            self.preparation_thread=threading.Thread(target=work,name='TossReadOnlyPreparation',daemon=True)
            self.preparation_thread.start()
            return {'ok':True,'readOnly':True,**self.status()}

    def _guard(self):
        if settings_fingerprint(self.settings())!=self.prepared['settingsHash']:
            raise ValueError('TOSS_CREDENTIALS_CHANGED')
        if abs((self.clock()-self.started_wall)-(self.monotonic()-self.started_mono))>2:
            raise ValueError('TOSS_RUN_CLOCK_CHANGED')
        for scope in self.scopes:
            self.coordinator.assert_scope_owner(scope_key=scope['scopeKey'],owner_id=self.owner)
        for claim in self.claims:
            self.pool.assert_claim(claim_id=claim['claimId'],owner_id=self.owner)

    def _mutation_settings(self):
        """One immutable settings snapshot binds both account check and POST."""
        settings=dict(self.settings())
        if settings_fingerprint(settings)!=self.prepared['settingsHash']:
            raise ValueError('TOSS_CREDENTIALS_CHANGED')
        checked=self.factory(settings,True)
        if exact_account(checked,settings)!=self.prepared['identity']:
            raise ValueError('TOSS_MUTATION_ACCOUNT_CHANGED')
        return settings

    def prepare(self,config):
        with self.work_lock:
            if self.thread and self.thread.is_alive() or self.ledger.unfinished():
                raise ValueError('TOSS_EXISTING_RUN_RECONCILIATION_REQUIRED')
            self.prepared=None
            if not isinstance(config,dict) or set(config)!={'targets','tossBindingConfirmed','budgetKrwByMarket','durationHours'}:
                raise ValueError('TOSS_OPERATION_SCOPE_INVALID')
            duration=amount(config['durationHours'],positive=True)
            if duration>24: raise ValueError('TOSS_DURATION_TOO_LONG')
            budgets_krw=config['budgetKrwByMarket']
            if not isinstance(budgets_krw,dict) or set(budgets_krw)!={'KR','US'} or any(amount(v,positive=True)>1_000_000_000 for v in budgets_krw.values()):
                raise ValueError('TOSS_MARKET_BUDGET_INVALID')
            specs,sources,factors,source_hash=self.resolve(config)
            settings=self.settings(); api=self.factory(settings,True)
            identity=exact_account(api,settings)
            symbols=[s.symbol for s in specs]; markets=sorted({toss_market(s.symbol)[1] for s in specs})
            stock_metadata(api,symbols)
            if open_orders(api,symbols): raise ValueError('TOSS_EXISTING_ORDERS_CONFLICT')
            baseline=holdings(api,symbols); rates=commission_rates(api,markets)
            fx=fx_observation(api,self.clock) if 'US' in markets else None
            budgets={('KRW' if m=='KR' else 'USD'):native_budget_from_krw(budgets_krw[m],currency='KRW' if m=='KR' else 'USD',usd_krw=fx['rate'] if fx else None) for m in markets}
            for currency,value in budgets.items():
                if buying_power(api,currency)<amount(value): raise ValueError('TOSS_CASH_BUYING_POWER_BELOW_BUDGET')
                if self.pool.inspect(account_identity=identity,currency=currency): raise ValueError('TOSS_BUDGET_RESERVED_BY_ANOTHER_RUN')
            feed=self.feed_factory(specs,api); history={}; feed.connect()
            try:
                for subscription in feed.subscriptions:
                    self.view.update(reason=subscription.symbol+' '+subscription.timeframe+' 과거 봉을 확인합니다.')
                    matches=[s for s in specs if s.instrument_id==subscription.instrument_id and s.timeframe==subscription.timeframe]
                    need=max(required_warmup_bars(s) for s in matches)
                    if need>2000: raise ValueError('TOSS_LOOKBACK_TOO_LARGE')
                    bars=feed.warmup(subscription,limit=need)
                    if len(bars)<need: raise ValueError('TOSS_HISTORY_INCOMPLETE')
                    if any(not consecutive(a,b) for a,b in zip(bars,bars[1:])): raise ValueError('TOSS_HISTORY_GAP')
                    if any(b.provider!='toss' or b.symbol!=subscription.symbol or b.instrument_id!=subscription.instrument_id or b.timeframe!=subscription.timeframe or epoch(b.end_time)>self.clock() for b in bars):
                        raise ValueError('TOSS_HISTORY_SCOPE_INVALID')
                    history[matches[0].stream_key]=bars
            finally: feed.disconnect()
            # Sources can change during slow history preparation; re-read at end.
            if self.resolve(config)[3]!=source_hash: raise ValueError('TOSS_SOURCE_CHANGED_DURING_PREPARE')
            now=self.clock()
            self.prepared={'preparationId':str(uuid.uuid4()),'expiresAt':now+180,'preparedMono':self.monotonic(),
                'config':config,'specs':specs,'sources':sources,'factors':factors,'sourceHash':source_hash,
                'settingsHash':settings_fingerprint(settings),'identity':identity,'budgets':budgets,
                'budgetKrwByMarket':budgets_krw,'markets':markets,'durationHours':float(duration),'baseline':baseline,
                'rates':rates,'fx':fx,'history':history}
            self.view.update(state='PREPARED',reason='자료·계좌 준비 완료. 실제 주문은 별도 시작 동의 후에만 전송합니다.')
            return {'ok':True,'readOnly':True,'ordersSubmitted':0,**self.status()}

    def start(self,payload,*,spawn=True):
        with self.work_lock:
            if self.preparation_thread and self.preparation_thread.is_alive(): raise ValueError('TOSS_PREPARATION_STILL_RUNNING')
            p=self.prepared
            if not p or set(payload)!={'preparationId','sourceHash','liveConsent'} or payload.get('liveConsent') is not True or payload.get('preparationId')!=p['preparationId'] or payload.get('sourceHash')!=p['sourceHash']:
                raise ValueError('TOSS_EXACT_PREPARATION_AND_CONSENT_REQUIRED')
            if self.thread and self.thread.is_alive() or self.ledger.unfinished(): raise ValueError('TOSS_RUN_ALREADY_EXISTS')
            if not 0<=self.clock()-(p['expiresAt']-180)<=180 or not 0<=self.monotonic()-p['preparedMono']<=180:
                raise ValueError('TOSS_PREPARATION_EXPIRED')
            settings=self.settings()
            if settings_fingerprint(settings)!=p['settingsHash'] or self.resolve(p['config'])[3]!=p['sourceHash']:
                raise ValueError('TOSS_PREPARED_SCOPE_CHANGED')
            api=self.factory(settings,True); symbols=[s.symbol for s in p['specs']]
            actual_holdings=holdings(api,symbols)
            if (exact_account(api,settings)!=p['identity'] or set(actual_holdings)!=set(p['baseline'])
                    or any(amount(actual_holdings[k])!=amount(p['baseline'][k]) for k in actual_holdings)):
                raise ValueError('TOSS_PREPARED_ACCOUNT_CHANGED')
            if open_orders(api,symbols): raise ValueError('TOSS_EXISTING_ORDERS_CONFLICT')
            if 'US' in p['markets']:
                fx_observation(api,self.clock)
                # Revalidate validity only: the USD cap remains the prepared cap.
            for c,v in p['budgets'].items():
                if buying_power(api,c)<amount(v): raise ValueError('TOSS_CASH_BUYING_POWER_BELOW_BUDGET')
            with self.boundary() as emergency:
                if emergency.get('active') is not False: raise ValueError('TOSS_EMERGENCY_STOP_ACTIVE')
                wall_elapsed=self.clock()-(p['expiresAt']-180); mono_elapsed=self.monotonic()-p['preparedMono']
                if not 0<=wall_elapsed<=180 or not 0<=mono_elapsed<=180 or abs(wall_elapsed-mono_elapsed)>2:
                    raise ValueError('TOSS_PREPARATION_EXPIRED')
                self.owner='live-runtime:toss-'+str(uuid.uuid4()); self.claims=[]; self.scopes=[]
                try:
                    for c,v in p['budgets'].items():
                        self.claims.append(self.pool.claim(account_identity=p['identity'],owner_id=self.owner,currency=c,limit=v))
                    for symbol in sorted(set(symbols)):
                        self.scopes.append(self.coordinator.claim_scope(owner_id=self.owner,broker='toss',account_identity=p['identity'],market='CASH',symbol=symbol))
                    scope={k:p[k] for k in ('sourceHash','budgets','factors','baseline','identity','settingsHash','config')}
                    scope.update(specs=[s.to_dict() for s in p['specs']],claims=self.claims,scopes=self.scopes)
                    self.ledger.initialize(self.owner,scope,p['budgets'])
                except Exception:
                    for scope in self.scopes: self.coordinator.release_unsubmitted_scope(scope_key=scope['scopeKey'],owner_id=self.owner)
                    for claim in self.claims: self.pool.release_unstarted(claim_id=claim['claimId'],owner_id=self.owner)
                    raise
            self.stop_event.clear(); self.started_wall=self.clock(); self.started_mono=self.monotonic()
            self.pending_signals={}
            self.view.update(state='RUNNING',ownerId=self.owner,startedAt=self.started_wall,reason='완성봉을 평가하고 이후 실제 호가에서 지정가 주문을 준비합니다.',ordersSubmitted=0,evaluations=0,activity=[])
            self.api=api; self.histories={k:list(v) for k,v in p['history'].items()}
            self.evaluator=BuiltinBarSignalEvaluator(lambda spec:float(self.ledger.project(self.owner)['positions'].get(spec.strategy_instance_id,0)))
            if spawn:
                self.thread=threading.Thread(target=self._run,name='TossCashOperation',daemon=False); self.thread.start()
            return {'ok':True,**self.status()}

    def _sleeve_cash(self,spec):
        p=self.prepared; currency='KRW' if toss_market(spec.symbol)[1]=='KR' else 'USD'
        cash=amount(p['budgets'][currency])*amount(p['factors'][spec.strategy_instance_id])
        for order in self.ledger.orders(self.owner):
            fill=order['terminal']
            if fill and fill['strategyInstanceId']==spec.strategy_instance_id:
                cash+=(-amount(fill['amount'])-amount(fill['fee']) if fill['side']=='BUY' else amount(fill['amount'])-amount(fill['fee']))
        if cash<0: raise ValueError('TOSS_SLEEVE_OVERSPEND')
        return cash

    def dispatch_signal(self,spec,decision,observed_at):
        if decision.signal not in {'BUY','SELL'} or self.stop_event.is_set(): return
        self._guard(); p=self.prepared; projection=self.ledger.project(self.owner)
        if projection['unresolvedOrders']: return False
        if self.resolve(p['config'])[3]!=p['sourceHash']: raise ValueError('TOSS_SOURCE_CHANGED')
        symbols=[s.symbol for s in p['specs']]
        expected=dict(p['baseline'])
        for source in p['specs']: expected[source.symbol]=str(amount(expected[source.symbol])+amount(projection['positions'].get(source.strategy_instance_id,0)))
        if exact_account(self.api,self.settings())!=p['identity']: raise ValueError('TOSS_ACCOUNT_CHANGED')
        actual=holdings(self.api,symbols)
        if set(actual)!=set(expected) or any(amount(actual[k])!=amount(expected[k]) for k in expected):
            raise ValueError('TOSS_EXTERNAL_POSITION_CHANGE_RECONCILE')
        if not regular_open(spec.symbol,self.clock()):
            return False
        if not signal_current(spec.symbol,observed_at,self.clock()): return True
        if open_orders(self.api,symbols): raise ValueError('TOSS_EXTERNAL_PENDING_ORDER')
        stock_metadata(self.api,[spec.symbol])
        market=toss_market(spec.symbol)[1]; currency='KRW' if market=='KR' else 'USD'
        rate=commission_rates(self.api,[market])[market]
        power=buying_power(self.api,currency) if decision.signal=='BUY' else None
        sellable=amount(self.api.sellable_quantity(spec.symbol).get('sellableQuantity')) if decision.signal=='SELL' else None
        raw=self.api.orderbook(spec.symbol); received=receipt_time(self.api)
        try:
            quote=normalize_orderbook(raw,symbol=spec.symbol,currency=currency,received_at=received,now=self.clock(),after=observed_at,bar_end=decision.bar.end_time)
        except ValueError as exc:
            if str(exc)=='TOSS_QUOTE_PREDATES_SIGNAL_OBSERVATION':
                return False  # Shared pre-signal quote is not an order failure; wait for the next actual receipt.
            raise
        quantity,percent=source_sizing(spec)
        sleeve=self._sleeve_cash(spec)
        capital=sleeve*percent/100 if percent is not None and decision.signal=='BUY' else sleeve
        requested=None if percent is not None and decision.signal=='BUY' else quantity
        ticket=budget_ticket(quote=quote,side=decision.signal,budget_available=min(capital,amount(projection['cash'][currency])),
            cash_buying_power=power,owned_quantity=projection['positions'].get(spec.strategy_instance_id,0),sellable_quantity=sellable,
            commission_rate=rate,requested_quantity=requested)
        if not amount(ticket['quantity']):
            self._note('NO_ORDER','예산 또는 소유 수량 내 주문 가능한 정수 수량이 없습니다.'); return True
        cid='lt'+uuid.uuid4().hex; request={'clientOrderId':cid,'symbol':spec.symbol,'side':decision.signal,'quantity':ticket['quantity'],
            'price':ticket['limitPrice'],'orderType':'LIMIT','timeInForce':'DAY'}
        with self.boundary() as emergency:
            if emergency.get('active') is not False or self.stop_event.is_set(): raise ValueError('TOSS_STOPPED_BEFORE_DISPATCH')
            self._guard()
            mutation_settings=self._mutation_settings()
            if not regular_open(spec.symbol,self.clock()): return False
            if not signal_current(spec.symbol,observed_at,self.clock()): return True
            normalize_orderbook(raw,symbol=spec.symbol,currency=currency,received_at=received,now=self.clock(),after=observed_at,bar_end=decision.bar.end_time)
            self.ledger.register(self.owner,cid,{**request,'currency':currency},spec.strategy_instance_id)
            for claim in self.claims: self.pool.mark_started(claim_id=claim['claimId'],owner_id=self.owner)
            response=self.factory(mutation_settings,False).place_order(request)
            self.ledger.acknowledge(self.owner,cid,response)
        self.view['ordersSubmitted']+=1; self._note('ORDER',spec.symbol+' '+decision.signal+' '+ticket['quantity'])
        return True

    def reconcile(self,*,cancel=False):
        """Read exact original IDs; cancel at most once, never infer a lost ACK."""
        for entry in self.ledger.orders(self.owner):
            if entry['terminal']: continue
            if not entry['ack']: raise ValueError('TOSS_ORDER_ACK_UNKNOWN_NO_RETRY')
            order_id=entry['ack']['orderId']; raw=self.api.get_order(order_id)
            proof={**entry['request'],'response':{'brokerOrderId':order_id,'responseHash':entry['ack']['rawHash']}}
            row=exact_owned_order(raw,proof)
            if row['state'] in {'filled','canceled','rejected'}:
                self.ledger.reconcile_order(self.owner,entry,raw); continue
            if cancel and row['state'] in {'new','partially_filled'}:
                exists=any(f['kind']=='CANCEL_INTENT' and f['payload']['orderId']==order_id for f in self.ledger.facts(self.owner))
                if not exists:
                    for scope in self.scopes: self.coordinator.assert_scope_owner(scope_key=scope['scopeKey'],owner_id=self.owner)
                    mutation_settings=self._mutation_settings()
                    claimed=self.ledger.append(self.owner,'CANCEL_INTENT',self.owner+':cancel:'+order_id,{'orderId':order_id})
                    if claimed:
                        self.factory(mutation_settings,False).cancel_order(order_id)
        return self.ledger.project(self.owner)

    def stop(self,payload):
        with self.lock:
            if set(payload)!={'ownerId'} or payload['ownerId']!=self.owner: raise ValueError('TOSS_EXACT_OWNER_REQUIRED')
            self.stop_event.set(); self.pending_signals={}; self.view['state']='STOPPING'
            return {'ok':True,**self.status()}

    def recover(self,payload):
        """Explicit user-requested cleanup only; never resume the old evaluator."""
        with self.work_lock:
            if self.thread and self.thread.is_alive(): raise ValueError('TOSS_WORKER_STILL_RUNNING')
            if set(payload)!={'ownerId','cancelOwnedOrders'} or payload.get('cancelOwnedOrders') is not True or payload['ownerId'] not in self.ledger.unfinished():
                raise ValueError('TOSS_EXACT_RECOVERY_OWNER_REQUIRED')
            owner=payload['ownerId']; scope=self.ledger.project(owner)['scope']; settings=self.settings()
            if settings_fingerprint(settings)!=scope['settingsHash']: raise ValueError('TOSS_RECOVERY_CREDENTIALS_CHANGED')
            api=self.factory(settings,True)
            if exact_account(api,settings)!=scope['identity']: raise ValueError('TOSS_RECOVERY_ACCOUNT_CHANGED')
            from types import SimpleNamespace
            self.owner=owner; self.api=api; self.claims=scope['claims']; self.scopes=scope['scopes']
            self.prepared={**scope,'specs':[SimpleNamespace(symbol=s['symbol']) for s in scope['specs']]}
            self.view.update(state='STOPPING',ownerId=owner,reason='이전 운용의 정확한 주문 ID를 대조합니다.')
            try: self._finish()
            except Exception as exc: self.view.update(state='RECONCILIATION_REQUIRED',reason=str(exc))
            return {'ok':self.view['state']=='STOPPED',**self.status()}

    def close_owned(self,payload):
        """A separate explicit click sells only this run's proven whole shares.

        This is never called by time expiry, emergency stop, preparation or
        restart. A single bounded attempt uses current bid LIMIT orders; an
        unfilled order is canceled once and remaining holdings stay reserved.
        """
        with self.work_lock:
            if self.thread and self.thread.is_alive(): raise ValueError('TOSS_STOP_RUN_BEFORE_CLOSE')
            if set(payload)!={'ownerId','liveConsent'} or payload.get('liveConsent') is not True:
                raise ValueError('TOSS_OWNED_CLOSE_CONSENT_REQUIRED')
            owner=payload['ownerId']
            if owner not in self.ledger.unfinished(): raise ValueError('TOSS_EXACT_RECOVERY_OWNER_REQUIRED')
            scope=self.ledger.project(owner)['scope']; settings=self.settings()
            if settings_fingerprint(settings)!=scope['settingsHash']: raise ValueError('TOSS_RECOVERY_CREDENTIALS_CHANGED')
            api=self.factory(settings,True)
            if exact_account(api,settings)!=scope['identity']: raise ValueError('TOSS_RECOVERY_ACCOUNT_CHANGED')
            from types import SimpleNamespace
            self.owner=owner; self.api=api; self.claims=scope['claims']; self.scopes=scope['scopes']
            specs=[SimpleNamespace(symbol=s['symbol'],strategy_instance_id=s['strategyInstanceId']) for s in scope['specs']]
            self.prepared={**scope,'specs':specs}; self.stop_event.clear()
            self.started_wall=self.clock(); self.started_mono=self.monotonic()
            self.view.update(state='CLOSING',ownerId=owner,reason='이번 운용의 확인된 보유분만 현재 매수 호가의 지정가로 매도합니다.')
            self.thread=threading.Thread(target=self._close_owned_worker,name='TossOwnedPositionClose',daemon=False)
            self.thread.start()
            return {'ok':True,**self.status()}

    def _close_owned_worker(self):
        try:
            projection=self.reconcile(cancel=True)
            for _ in range(15):
                if not projection['unresolvedOrders']: break
                if self.stop_event.wait(2): break
                projection=self.reconcile()
            if projection['unresolvedOrders']: raise ValueError('TOSS_OWNED_CLOSE_PENDING_UNRESOLVED')
            for spec in self.prepared['specs']:
                self._guard()
                if self.stop_event.is_set(): break
                projection=self.ledger.project(self.owner); qty=amount(projection['positions'].get(spec.strategy_instance_id,0))
                if not qty: continue
                if not regular_open(spec.symbol,self.clock()): raise ValueError('TOSS_OWNED_CLOSE_REGULAR_SESSION_REQUIRED')
                expected={k:amount(v) for k,v in self.prepared['baseline'].items()}
                for s in self.prepared['specs']: expected[s.symbol]+=amount(projection['positions'].get(s.strategy_instance_id,0))
                actual=holdings(self.api,expected)
                if any(amount(actual[k])!=v for k,v in expected.items()): raise ValueError('TOSS_EXTERNAL_POSITION_CHANGE_RECONCILE')
                if exact_account(self.api,self.settings())!=self.prepared['identity']: raise ValueError('TOSS_ACCOUNT_CHANGED')
                if open_orders(self.api,expected): raise ValueError('TOSS_EXTERNAL_PENDING_ORDER')
                stock_metadata(self.api,[spec.symbol])
                currency='KRW' if toss_market(spec.symbol)[1]=='KR' else 'USD'
                sellable=amount(self.api.sellable_quantity(spec.symbol).get('sellableQuantity'))
                observed=self.clock()
                for attempt in range(3):
                    raw=self.api.orderbook(spec.symbol); received=receipt_time(self.api)
                    try:
                        quote=normalize_orderbook(raw,symbol=spec.symbol,currency=currency,received_at=received,now=self.clock(),after=observed)
                        break
                    except ValueError as exc:
                        if str(exc)!='TOSS_QUOTE_PREDATES_SIGNAL_OBSERVATION' or attempt==2 or self.stop_event.wait(1): raise
                # This short wait only obtains a later public quote. No order has
                # been written or sent, and the subsequent POST is never retried.
                ticket=budget_ticket(quote=quote,side='SELL',budget_available=0,owned_quantity=qty,sellable_quantity=sellable)
                if not amount(ticket['quantity']): raise ValueError('TOSS_OWNED_CLOSE_NO_SELLABLE_WHOLE_SHARES')
                cid='lt'+uuid.uuid4().hex
                request={'clientOrderId':cid,'symbol':spec.symbol,'side':'SELL','quantity':ticket['quantity'],'price':ticket['limitPrice'],'orderType':'LIMIT','timeInForce':'DAY'}
                with self.boundary() as emergency:
                    if emergency.get('active') is not False: raise ValueError('TOSS_EMERGENCY_STOP_ACTIVE')
                    self._guard()
                    mutation_settings=self._mutation_settings()
                    if not regular_open(spec.symbol,self.clock()) or self.stop_event.is_set(): raise ValueError('TOSS_STOPPED_BEFORE_DISPATCH')
                    normalize_orderbook(raw,symbol=spec.symbol,currency=currency,received_at=received,now=self.clock(),after=observed)
                    self.ledger.register(self.owner,cid,{**request,'currency':currency},spec.strategy_instance_id)
                    for claim in self.claims: self.pool.mark_started(claim_id=claim['claimId'],owner_id=self.owner)
                    self.ledger.acknowledge(self.owner,cid,self.factory(mutation_settings,False).place_order(request))
                self.view['ordersSubmitted']=self.view.get('ordersSubmitted',0)+1
                for _ in range(30):
                    if self.stop_event.wait(2): break
                    if not self.reconcile()['unresolvedOrders']: break
                if self.ledger.project(self.owner)['unresolvedOrders']: break
        except Exception as exc:
            self.view.update(state='RECONCILIATION_REQUIRED',reason=str(getattr(exc,'code',str(exc))))
        finally:
            self.stop_event.set()
            try: self._finish()
            except Exception as exc: self.view.update(state='RECONCILIATION_REQUIRED',reason=str(getattr(exc,'code',str(exc))))

    def shutdown(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set(); self.thread.join(timeout=40)
        return not (self.thread and self.thread.is_alive())

    def _finish(self):
        projection=self.reconcile(cancel=True)
        for _ in range(15):
            if not projection['unresolvedOrders']: break
            time.sleep(2); projection=self.reconcile()
        if projection['unresolvedOrders'] or any(amount(v) for v in projection['positions'].values()):
            self.view.update(state='RECONCILIATION_REQUIRED',reason='이번 운용의 미체결 또는 보유 주식이 남아 있습니다. 예산을 유지하며 임의 청산하지 않습니다.',ledger=projection)
            return
        pending=open_orders(self.api,[s.symbol for s in self.prepared['specs']])
        actual=holdings(self.api,[s.symbol for s in self.prepared['specs']])
        if pending or any(amount(actual[k])!=amount(v) for k,v in self.prepared['baseline'].items()): raise ValueError('TOSS_TERMINAL_ACCOUNT_RECONCILE')
        seen=self.clock(); digest=stable_sha256({'ledger':projection,'holdings':actual,'pending':pending,'observedAt':seen})
        for scope in self.scopes:
            state=self.coordinator.inspect_scope(scope_key=scope['scopeKey'])
            if not state or state['ownerId']!=self.owner: raise ValueError('TOSS_SCOPE_RELEASE_OWNERSHIP_UNKNOWN')
            if state['status']!='RELEASED': self.coordinator.release_scope(scope_key=scope['scopeKey'],owner_id=self.owner,owned_position_flat=True,pending_order_count=0,evidence_hash=digest,observed_at=seen)
        for claim in self.claims:
            state=self.pool.inspect_claim(claim_id=claim['claimId'],owner_id=self.owner)
            if not state: raise ValueError('TOSS_BUDGET_RELEASE_OWNERSHIP_UNKNOWN')
            if state['state']!='RELEASED': self.pool.release(claim_id=claim['claimId'],owner_id=self.owner,owned_position_flat=True,pending_order_count=0,evidence_hash=digest,observed_at=seen)
        self.ledger.append(self.owner,'RELEASED_FLAT',self.owner+':flat',{'evidenceHash':digest,'observedAt':seen})
        self.view.update(state='STOPPED',endedAt=seen,ledger=projection,reason='소유 주문의 종료와 보유분 0을 확인했습니다.')

    def _run(self):
        feed=self.feed_factory(self.prepared['specs'],self.api)
        try:
            feed.connect()
            while not self.stop_event.is_set() and self.monotonic()-self.started_mono<self.prepared['durationHours']*3600:
                self._guard()
                with self.boundary() as emergency:
                    if emergency.get('active') is not False: raise ValueError('TOSS_EMERGENCY_STOP_ACTIVE')
                projection=self.reconcile(); self.view['ledger']=projection
                for bar in feed.poll(1): self.ingest_bar(bar)
                self.dispatch_pending()
                self.stop_event.wait(1)
        except Exception as exc:
            self.view.update(state='FAILED',firstFailure=str(getattr(exc,'code',type(exc).__name__)),reason=str(exc))
            self._note('FAILED',str(getattr(exc,'code',type(exc).__name__)))
        finally:
            self.stop_event.set(); self.pending_signals={}
            try: feed.disconnect()
            except Exception as exc:
                if not self.view.get('firstFailure'): self.view.update(firstFailure='TOSS_FEED_CLOSE_FAILED',reason=str(exc))
            try: self._finish()
            except Exception as exc:
                self.view.update(state='RECONCILIATION_REQUIRED',reason=str(exc))
            if self.view.get('firstFailure') and self.view.get('state')=='STOPPED': self.view['state']='STOPPED_WITH_ERROR'

    def ingest_bar(self,bar):
        matches=[s for s in self.prepared['specs'] if (bar.instrument_id,bar.timeframe)==(s.instrument_id,s.timeframe)]
        if not matches: raise ValueError('TOSS_UNREQUESTED_BAR')
        history=self.histories[matches[0].stream_key]
        if epoch(bar.end_time)<=epoch(history[-1].end_time): return
        if bar.provider!='toss' or bar.symbol!=matches[0].symbol or bar.price_basis!='unadjusted' or epoch(bar.end_time)>self.clock():
            raise ValueError('TOSS_BAR_SCOPE_OR_TIME_INVALID')
        if not consecutive(history[-1],bar): raise ValueError('TOSS_LIVE_HISTORY_GAP')
        history.append(bar); del history[:-max(256,*(required_warmup_bars(s) for s in matches))]
        for spec in matches:
            observed=self.clock()
            if epoch(bar.end_time)<=self.started_wall: continue
            if observed-epoch(bar.end_time)>90 and not (bar.timeframe=='1d' and next_session_window(spec.symbol,epoch(bar.start_time),observed)): continue
            decision=self.evaluator(spec,bar,tuple(history)); self.view['evaluations']+=1
            if decision.metadata.get('unsupported'): raise ValueError('TOSS_PLUGIN_UNSUPPORTED')
            self._note('EVALUATION',spec.symbol+' '+decision.signal)
            # A new HOLD/SELL/BUY supersedes the previous unsubmitted signal.
            self.pending_signals.pop(spec.strategy_instance_id,None)
            if decision.signal in {'BUY','SELL'}:
                self.pending_signals[spec.strategy_instance_id]=(spec,decision,observed)
        self.dispatch_pending()

    def dispatch_pending(self):
        if self.stop_event.is_set(): return
        for key,(spec,decision,observed) in list(self.pending_signals.items()):
            # Only signals observed in this genuine run survive a closed market.
            # No restart replay, old-history signal or quote from the past.
            now=self.clock()
            deferred=not regular_open(spec.symbol,observed)
            if (deferred and now-observed>7*86400) or (not deferred and now-observed>30):
                self.pending_signals.pop(key,None); self._note('SIGNAL_EXPIRED',spec.symbol+' 장 대기 신호가 만료되었습니다.'); continue
            if self.ledger.project(self.owner)['unresolvedOrders']: break
            if not regular_open(spec.symbol,self.clock()): continue
            if deferred and not next_session_window(spec.symbol,observed,now):
                self.pending_signals.pop(key,None); self._note('SIGNAL_EXPIRED',spec.symbol+' 다음 정규장 개장 대기 범위를 지났습니다.'); continue
            if self.dispatch_signal(spec,decision,observed): self.pending_signals.pop(key,None)
        self.view['waitingSignals']=len(self.pending_signals)
