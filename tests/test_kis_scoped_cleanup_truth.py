"""Synthetic account/official GET fixtures only; no real socket may connect."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import importlib
from pathlib import Path
import socket
import sys
import types
import unittest
from unittest.mock import patch

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(APP.parents[1]/'packages/trading_runtime'))
NAME = '_kis_cleanup_truth_isolated'
pkg = types.ModuleType(NAME); pkg.__path__ = [str(APP/'live_trader')]
sys.modules[NAME] = pkg
truth = importlib.import_module(NAME+'.kis_scoped_cleanup_truth')
service = importlib.import_module(NAME+'.kis_equity_execution')
profiles = importlib.import_module(NAME+'.account_profiles')

NOW = datetime(2026,10,16,1,30,tzinfo=timezone.utc)
SETTINGS = {'KIS_APP_KEY':'SYNTHETIC_APPKEY','KIS_APP_SECRET':'SYNTHETIC_SECRET',
    'KIS_ACCOUNT_NO':'12345678','KIS_ACCOUNT_PRODUCT_CODE':'01'}
ACCOUNT = profiles.saved_account_profiles(SETTINGS)[0]


def fixture(venue='NASD'):
    domestic = venue == 'KRX'
    currency = 'KRW' if domestic else 'USD'; symbol = '005930' if domestic else 'AAPL'
    day = '20261016' if domestic else '20261015'
    scope = {'version':'kis-owned-capital-v2','deploymentId':'deployment',
        'accountFingerprint':ACCOUNT['accountFingerprint'],
        'credentialConfigurationHash':ACCOUNT['credentialConfigurationHash'],
        'accountProfileId':ACCOUNT['profileId'],'reviewBindingHash':'a'*64,
        'capitalPercent':'5','quoteCurrency':currency,'members':[{
            'strategyInstanceId':'member','sourceArtifactHash':'c'*64,
            'portfolioHash':'e'*64,'symbol':symbol,'quoteCurrency':currency}]}
    scope['scopeHash'] = truth._hash(scope)
    ticket = service.seal({'version':service.RESULT,'accountFingerprint':ACCOUNT['accountFingerprint'],
        'credentialConfigurationHash':ACCOUNT['credentialConfigurationHash'],
        'exchange':venue,'currency':currency,'quantity':'3','costReserveRate':'0.01','minimumCostReserve':'1',
        'deploymentId':'deployment','sourceArtifactHash':'c'*64,'symbol':symbol,'side':'BUY','capitalScopeHash':scope['scopeHash']})
    dispatch = {'broker_id':'kis','idempotency_key':'dispatch-1','symbol':symbol,'side':'BUY',
        'order_date':day,'organization_no':'001','broker_order_id':'0002',
        'broker_order_key':day+':001:0002','broker_order_identity_verified':True,
        'order_date_source':'OFFICIAL_ORDER_QUERY_EXACT_COMPOSITE_IDENTITY',
        'state':'accepted','response':{'synthetic':'original receipt untouched'},'metadata':{
            'deployment_id':'deployment','runtime_scope_id':'runtime-1','strategy_instance_id':'member',
            'source_artifact_hash':'c'*64,'portfolio_hash':'e'*64,
            'native_account_fingerprint':ACCOUNT['accountFingerprint'],'native_quantity_model':ticket}}
    owned = {'dispatchKey':'dispatch-1','runtimeScopeId':'runtime-1','sessionId':'session-1',
        'accountFingerprint':ACCOUNT['accountFingerprint'],
        'credentialConfigurationHash':ACCOUNT['credentialConfigurationHash'],
        'deploymentId':'deployment','market':'KR' if domestic else 'US','currency':currency,
        'symbol':symbol,'side':'BUY','orderDate':day,'organizationNo':'001','orderNo':'0002',
        'venue':venue,'originalQuantity':'3','businessHash':truth.dispatch_business_hash(dispatch),
        'dispatchHash':truth._hash(dispatch),'dispatch':dispatch}
    common = {'ord_dt':day,'ord_gno_brno':'001','odno':'0002','pdno':symbol,
        'sll_buy_dvsn_cd':'02','ord_tmd':'103000'}
    history = {**common,**({'excg_id_dvsn_cd':'KRX','ord_qty':'3','tot_ccld_qty':'1',
        'rmn_qty':'2','tot_ccld_amt':'100','cnc_cfrm_qty':'0','rjct_qty':'0'} if domestic else {
        'ovrs_excg_cd':venue,'tr_crcy_cd':'USD','ft_ord_qty':'3','ft_ccld_qty':'1',
        'nccs_qty':'2','ft_ccld_unpr3':'100','ft_ccld_amt3':'100','prcs_stat_name':'부분체결'})}
    working = deepcopy(history)
    if domestic:
        working.pop('ord_dt');working['psbl_qty'] = working.pop('rmn_qty')
    return scope,owned,working,history


class Observer:
    def __init__(self,working,history):
        self.reader=types.SimpleNamespace(settings=dict(SETTINGS));self.calls=[]
        self.working=[working] if working is not None else []
        self.history=[history] if history is not None else []
        self.account_value=deepcopy(ACCOUNT);self.get_hook=None;self.account_hook=None
        self.date_header=format_datetime(NOW,usegmt=True);self.age_header=''
    def account(self):
        if self.account_hook:self.account_hook(self)
        return deepcopy(self.account_value)
    def _get(self,path,query,*,continuation=''):
        self.calls.append((path,deepcopy(query),continuation))
        if self.get_hook:
            result=self.get_hook(path,query,continuation)
            if result is not None:
                payload,headers=result
                return payload,{'date':self.date_header,'age':self.age_header,**headers}
        working=path.endswith(('inquire-nccs','inquire-psbl-rvsecncl'))
        output='output1' if path.endswith('inquire-daily-ccld') else 'output'
        return {'rt_cd':'0',output:deepcopy(self.working if working else self.history)}, {
            'tr_cont':'D','date':self.date_header,'age':self.age_header}


class CleanupTruthTests(unittest.TestCase):
    def setUp(self):
        self.connect=patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden'))
        self.connect_ex=patch.object(socket.socket,'connect_ex',side_effect=AssertionError('network forbidden'))
        self.connect.start();self.connect_ex.start();self.addCleanup(self.connect.stop);self.addCleanup(self.connect_ex.stop)
        self.scope,self.owned,self.working,self.history=fixture()
        self.observer=Observer(self.working,self.history)
        self.runtime=types.SimpleNamespace(scope=lambda:deepcopy(self.scope),observer=self.observer)
    def collect(self,**kwargs):
        return truth.collect_cleanup_truth(self.runtime,[self.owned],now=kwargs.pop('now',NOW),
            monotonic_clock=kwargs.pop('monotonic_clock',lambda:1000.),**kwargs)
    def update_dispatch(self,**values):
        self.owned['dispatch'].update(values)
        self.owned['dispatchHash']=truth._hash(self.owned['dispatch'])
        self.owned['businessHash']=truth.dispatch_business_hash(self.owned['dispatch'])
    def reseal(self,value):
        value['rawResponseHash']=truth._hash(value['queries'])
        value['proofHash']=truth._hash({k:v for k,v in value.items() if k!='proofHash'})
    def rekey_day(self,day):
        self.owned['orderDate']=day
        self.update_dispatch(order_date=day,broker_order_key=day+':001:0002')
        self.observer.history[0]['ord_dt']=day
        if self.observer.working and 'ord_dt' in self.observer.working[0]:
            self.observer.working[0]['ord_dt']=day
    def test_partial_remaining_is_exact_raw_integer_and_history_joined(self):
        result=self.collect();row=result['orders'][0]
        self.assertEqual(('WORKING','1','2'),(row['state'],row['filledQuantity'],row['remainingQuantity']))
        self.assertFalse(row['terminalVerified']);self.assertIsNone(row['terminalReceipt'])
        self.assertEqual(result,truth.validate_cleanup_truth(result,[self.owned],now=NOW))
        self.assertEqual(len(self.observer.calls),2)
        self.assertEqual(self.observer.calls[0][1]['OVRS_EXCG_CD'],'NASD')
        self.assertEqual(self.observer.calls[1][1]['ORD_STRT_DT'],'20261015')
        self.assertEqual(self.observer.calls[1][1]['ODNO'],'')
        self.assertFalse(result['orderAuthorityGranted'])
    def test_us_broker_order_time_is_not_inferred_as_receipt_time(self):
        self.observer.working[0]['ord_tmd']='UNPROVEN_ZONE'
        self.observer.history[0]['ord_tmd']='UNPROVEN_ZONE'
        result=self.collect()
        self.assertEqual(result['observedAt'],NOW.isoformat())
        self.assertNotIn('occurredAt',result['orders'][0])
    def test_exact_full_terminal_fill_has_official_receipt_and_no_actual_fee_claim(self):
        self.observer.working=[]
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',ft_ccld_amt3='300',prcs_stat_name='체결')
        row=self.collect()['orders'][0]
        self.assertTrue(row['terminalVerified']);self.assertEqual(row['state'],'TERMINAL')
        self.assertEqual(row['terminalReceipt']['quantity'],'3')
        self.assertFalse(row['terminalReceipt']['actualFeeObserved'])
    def test_partial_cancel_and_zero_fill_reject_have_explicit_terminal_status(self):
        self.observer.working=[]
        for status,filled,amount in [('취소완료','1','100'),('주문거부','0','0')]:
            with self.subTest(status=status):
                self.observer.history[0].update(ft_ccld_qty=filled,nccs_qty='0',ft_ccld_amt3=amount,prcs_stat_name=status)
                result=self.collect()['orders'][0]
                self.assertEqual(result['state'],'TERMINAL')
                self.assertEqual(result['terminalReceipt'] is None,filled=='0')
        self.observer.history[0]['prcs_stat_name']='UNKNOWN'
        with self.assertRaisesRegex(ValueError,'TERMINAL_STATUS_UNPROVEN'):self.collect()
    def test_absent_working_is_never_terminal_evidence(self):
        self.observer.working=[]
        with self.assertRaisesRegex(ValueError,'MISSING_WORKING_ROW_IS_NOT_TERMINAL'):self.collect()
        self.observer.history=[]
        with self.assertRaisesRegex(ValueError,'EXACT_HISTORY_ROW_REQUIRED'):self.collect()
    def test_working_and_history_race_blocks_instead_of_overcancel(self):
        self.observer.history[0].update(ft_ccld_qty='2',nccs_qty='1')
        with self.assertRaisesRegex(ValueError,'WORKING_HISTORY_QUANTITY_RACE_OR_MISMATCH'):self.collect()
    def test_fractional_float_negative_nonfinite_or_exponent_never_truncated(self):
        for value in ['2.5',2.0,-1,True,'NaN','1e1','-1','0.0000000000000000000001']:
            with self.subTest(value=value):
                self.observer.working[0]['nccs_qty']=value
                with self.assertRaises(ValueError):self.collect()
    def test_full_composite_identity_scope_and_whole_order_quantity_required(self):
        for field,value in [('ord_dt','20261014'),('ovrs_excg_cd','NYSE'),('tr_crcy_cd','KRW'),
            ('pdno','MSFT'),('sll_buy_dvsn_cd','01'),('ft_ord_qty','4'),('ord_gno_brno','999')]:
            with self.subTest(field=field):
                self.observer.history=[{**self.history,field:value}]
                with self.assertRaises(ValueError):self.collect()
    def test_duplicate_raw_identity_and_duplicate_owned_set_fail_closed(self):
        self.observer.history.append(deepcopy(self.history))
        with self.assertRaisesRegex(ValueError,'AMBIGUOUS'):self.collect()
        with self.assertRaisesRegex(ValueError,'DUPLICATE_OWNED'):
            truth.collect_cleanup_truth(self.runtime,[self.owned,self.owned],now=NOW)
    def test_provisional_identity_rejected_before_any_get_or_ledger_write(self):
        self.update_dispatch(broker_order_identity_verified=False,order_date_source='MARKET_SESSION_DATE_PROVISIONAL')
        with self.assertRaisesRegex(ValueError,'DURABLE_VERIFIED_IDENTITY_REQUIRED'):self.collect()
        self.assertEqual(self.observer.calls,[])
    def test_mutated_dispatch_or_current_account_or_source_fails(self):
        self.owned['dispatch']['response']['changed']=True
        with self.assertRaisesRegex(ValueError,'IMMUTABLE_DISPATCH_HASH_CHANGED'):self.collect()
        self.owned['dispatchHash']=truth._hash(self.owned['dispatch'])
        self.owned['businessHash']=truth.dispatch_business_hash(self.owned['dispatch'])
        self.observer.account_value['accountFingerprint']='0'*64
        with self.assertRaisesRegex(ValueError,'CURRENT_ACCOUNT_SCOPE_CHANGED'):self.collect()
        self.observer.account_value=deepcopy(ACCOUNT)
        self.owned['dispatch']['metadata']['source_artifact_hash']='0'*64
        self.owned['dispatchHash']=truth._hash(self.owned['dispatch'])
        self.owned['businessHash']=truth.dispatch_business_hash(self.owned['dispatch'])
        with self.assertRaisesRegex(ValueError,'ACCOUNT_SOURCE_SCOPE_CHANGED'):self.collect()
    def test_changed_scope_during_get_and_old_market_date_block(self):
        calls=[]
        def scope():
            calls.append(1)
            return deepcopy(self.scope) if len(calls)==1 else {**self.scope,'changed':True}
        self.runtime.scope=scope
        with self.assertRaisesRegex(ValueError,'SCOPE_CHANGED_DURING_GET'):self.collect()
        self.runtime.scope=lambda:deepcopy(self.scope)
        with self.assertRaisesRegex(ValueError,'CURRENT_MARKET_DAY_REQUIRED'):self.collect(now=NOW+timedelta(days=1))
    def test_timeout_clock_rollback_future_or_old_proof_blocks(self):
        result=self.collect()
        for current in [NOW-timedelta(microseconds=1),NOW+timedelta(seconds=15.001)]:
            with self.subTest(current=current),self.assertRaisesRegex(ValueError,'STALE_OR_CLOCK'):
                truth.validate_cleanup_truth(result,[self.owned],now=current)
        for times in [(1000.,1015.001),(1000.,999.)]:
            clock=iter(times)
            with self.subTest(times=times),self.assertRaisesRegex(ValueError,'STALE_OR_CLOCK'):
                self.collect(monotonic_clock=lambda:next(clock))
    def test_before_and_after_real_clock_are_not_a_single_fetch_now(self):
        times=iter([NOW,NOW+timedelta(seconds=2)])
        result=self.collect(now=lambda:next(times),monotonic_clock=iter([1000.,1000.,1001.,1001.,1002.,1002.]).__next__)
        self.assertEqual(result['durationSeconds'],2)
        self.assertEqual(result['completedAt'],(NOW+timedelta(seconds=2)).isoformat())
        truth.validate_cleanup_truth(result,[self.owned],now=NOW+timedelta(seconds=15))
        with self.assertRaisesRegex(ValueError,'STALE_OR_CLOCK'):
            truth.validate_cleanup_truth(result,[self.owned],now=NOW+timedelta(seconds=16))
    def test_pagination_complete_all_pages_and_raw_hashes(self):
        def pages(path,query,continuation):
            if not path.endswith('inquire-nccs'):return None
            if continuation=='':return {'rt_cd':'0','output':[], 'ctx_area_fk200':'f','ctx_area_nk200':'n'},{'tr_cont':'M'}
            return {'rt_cd':'0','output':[deepcopy(self.working)]},{'tr_cont':'D'}
        self.observer.get_hook=pages
        result=self.collect();query=result['queries'][0]
        self.assertEqual(query['pageCount'],2);self.assertEqual(len(query['rawPageHashes']),2)
        self.assertEqual(self.observer.calls[1][2],'N')
        self.assertEqual(self.observer.calls[1][1]['CTX_AREA_NK200'],'n')
    def test_repeated_missing_unknown_or_unbounded_pagination_blocks(self):
        for kind in ['repeat','missing','unknown','rows','pages','rejected']:
            with self.subTest(kind=kind):
                def pages(path,query,continuation):
                    if not path.endswith('inquire-nccs'):return None
                    if kind=='unknown':return {'rt_cd':'0','output':[]},{'tr_cont':'X'}
                    if kind=='rejected':return {'rt_cd':'1','output':[]},{'tr_cont':'D'}
                    if kind=='rows':return {'rt_cd':'0','output':[deepcopy(self.working)]*129},{'tr_cont':'D'}
                    if kind=='missing':return {'rt_cd':'0','output':[]},{'tr_cont':'M'}
                    marker=str(len(self.observer.calls)) if kind=='pages' else 'f'
                    return {'rt_cd':'0','output':[], 'ctx_area_fk200':marker,'ctx_area_nk200':'n'},{'tr_cont':'M'}
                self.observer.calls=[];self.observer.get_hook=pages
                with self.assertRaises(ValueError):self.collect()
    def test_corruption_rehash_or_rebinding_cannot_change_projection(self):
        result=self.collect()
        mutations=[lambda v:v['orders'][0].update(remainingQuantity='3'),
            lambda v:v['orders'][0].update(terminalVerified=True),
            lambda v:v.update(paginationComplete=False),
            lambda v:v['queries'][0].update(pageCount=99),
            lambda v:v['queries'][0].update(market='KR'),
            lambda v:v['queries'][0]['rows'][0].update(nccs_qty='0.5')]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                changed=deepcopy(result);mutate(changed);self.reseal(changed)
                with self.assertRaises(ValueError):truth.validate_cleanup_truth(changed,[self.owned],now=NOW)
        changed=deepcopy(self.owned);changed['sessionId']='other'
        with self.assertRaisesRegex(ValueError,'CURRENT_OWNED_SET_OR_SCOPE_CHANGED'):
            truth.validate_cleanup_truth(result,[changed],now=NOW)
    def test_krx_raw_psbl_quantity_joins_current_day_ccnl_without_invented_date(self):
        self.scope,self.owned,self.working,self.history=fixture('KRX')
        self.runtime.observer=self.observer=Observer(self.working,self.history)
        result=self.collect()['orders'][0]
        self.assertEqual(result['remainingQuantity'],'2')
        self.assertNotIn('ord_dt',result['officialWorkingRow'])
        self.observer.working[0]['excg_id_dvsn_cd']='NXT'
        with self.assertRaisesRegex(ValueError,'OFFICIAL_KRX_EXCHANGE_REQUIRED'):self.collect()
    def test_krx_partial_cancel_requires_complete_terminal_accounting(self):
        self.scope,self.owned,self.working,self.history=fixture('KRX')
        self.runtime.observer=self.observer=Observer(None,{**self.history,'rmn_qty':'0','cnc_cfrm_qty':'2'})
        self.assertEqual(self.collect()['orders'][0]['state'],'TERMINAL')
        self.observer.history[0]['cnc_cfrm_qty']='1'
        with self.assertRaisesRegex(ValueError,'TERMINAL_ACCOUNTING_INCOMPLETE'):self.collect()
    def test_krx_cancel_and_reject_units_must_be_whole_shares_even_if_sum_matches(self):
        self.scope,self.owned,self.working,self.history=fixture('KRX')
        self.runtime.observer=self.observer=Observer(None,{**self.history,'rmn_qty':'0',
            'cnc_cfrm_qty':'1.5','rjct_qty':'0.5'})
        with self.assertRaisesRegex(ValueError,'DECIMAL_WHOLE_QUANTITY_REQUIRED'):self.collect()
    def test_yesterday_exact_terminal_is_allowed_using_original_day_query_only(self):
        self.rekey_day('20261014');self.observer.working=[]
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',ft_ccld_amt3='300',prcs_stat_name='체결')
        result=self.collect();row=result['orders'][0]
        self.assertEqual(row['state'],'TERMINAL');self.assertTrue(row['historicalTerminalOnly'])
        self.assertEqual(row['orderDate'],'20261014')
        self.assertEqual(self.observer.calls[1][1]['ORD_STRT_DT'],'20261014')
        self.assertEqual(self.observer.calls[1][1]['ORD_END_DT'],'20261014')
        self.assertIsNone(row['officialWorkingRow'])
    def test_past_day_working_and_future_day_any_order_are_blocked(self):
        self.rekey_day('20261014')
        with self.assertRaisesRegex(ValueError,'CURRENT_MARKET_DAY_REQUIRED'):self.collect()
        self.rekey_day('20261016');self.observer.calls=[]
        with self.assertRaisesRegex(ValueError,'FUTURE_OFFICIAL_ORDER_DATE'):self.collect()
        self.assertEqual(self.observer.calls,[])
    def test_krx_yesterday_terminal_cannot_claim_today_reused_date_less_psbl_identity(self):
        self.scope,self.owned,self.working,self.history=fixture('KRX')
        self.runtime.observer=self.observer=Observer(self.working,self.history)
        self.rekey_day('20261015')
        self.observer.history[0].update(tot_ccld_qty='3',rmn_qty='0',tot_ccld_amt='300')
        # Same branch/ODNO/symbol/side/current PSBL quantities. Its date is
        # unavailable, so it must not be attributed to yesterday's terminal.
        result=self.collect()['orders'][0]
        self.assertEqual(result['state'],'TERMINAL');self.assertTrue(result['historicalTerminalOnly'])
        self.assertIsNone(result['officialWorkingRow']);self.assertEqual(result['remainingQuantity'],'0')
    def test_midnight_crossing_never_cancels_previous_day_working_identity(self):
        self.scope,self.owned,self.working,self.history=fixture('KRX')
        self.runtime.observer=self.observer=Observer(self.working,self.history)
        start=datetime(2026,10,16,14,59,58,tzinfo=timezone.utc)
        self.observer.date_header=format_datetime(start,usegmt=True)
        wall=iter([start,start+timedelta(seconds=3)])
        mono=iter([1000.,1000.,1001.,1001.,1002.,1003.])
        with self.assertRaisesRegex(ValueError,'CURRENT_MARKET_DAY_REQUIRED'):
            self.collect(now=lambda:next(wall),monotonic_clock=lambda:next(mono))
    def test_http_date_second_interval_must_stay_in_same_market_day(self):
        self.scope,self.owned,self.working,self.history=fixture('KRX')
        self.runtime.observer=self.observer=Observer(self.working,self.history)
        start=datetime(2026,10,16,14,59,59,tzinfo=timezone.utc)
        self.observer.date_header=format_datetime(start,usegmt=True)
        with self.assertRaisesRegex(ValueError,'CURRENT_MARKET_DAY_REQUIRED'):self.collect(now=start)
    def test_http_server_and_local_day_disagreement_blocks_krx_identifier_reuse(self):
        self.scope,self.owned,self.working,self.history=fixture('KRX')
        self.runtime.observer=self.observer=Observer(self.working,self.history)
        self.rekey_day('20261017')
        start=datetime(2026,10,16,15,0,3,tzinfo=timezone.utc)
        self.observer.date_header=format_datetime(start-timedelta(seconds=5),usegmt=True)
        with self.assertRaisesRegex(ValueError,'FUTURE_OFFICIAL_ORDER_DATE_VS_HTTP_SERVER'):self.collect(now=start)
    def test_working_requires_canonical_uncached_http_date_and_bounded_clock_skew(self):
        for date,age in [('', ''),('not a date',''),(format_datetime(NOW,usegmt=True),'1'),
            (format_datetime(NOW,usegmt=True),'unknown'),
            (format_datetime(NOW+timedelta(minutes=2),usegmt=True),'')]:
            with self.subTest(date=date,age=age):
                self.observer.date_header=date;self.observer.age_header=age
                with self.assertRaisesRegex(ValueError,'HTTP_SERVER|HTTP_DATE|CACHE_AGE'):self.collect()
    def test_http_calendar_proof_tampering_and_monotonic_reordering_block(self):
        result=self.collect()
        for change in ({'minimumServerUtc':(NOW-timedelta(days=1)).isoformat()},
            {'clockRole':'BROKER_EXECUTION_TIME'},{'requestElapsedSeconds':-1},
            {'status':'UNVERIFIED'}):
            with self.subTest(change=change):
                corrupted=deepcopy(result);corrupted['queries'][0]['httpDateEvidence'][0].update(change)
                self.reseal(corrupted)
                with self.assertRaisesRegex(ValueError,'HTTP_DATE_PAGE'):
                    truth.validate_cleanup_truth(corrupted,[self.owned],now=NOW)
    def test_zero_terminal_fill_and_positive_notional_cannot_become_a_clean_noop(self):
        for venue in ('NASD','KRX'):
            with self.subTest(venue=venue):
                self.scope,self.owned,self.working,self.history=fixture(venue)
                history=deepcopy(self.history)
                if venue=='KRX':history.update(tot_ccld_qty='0',rmn_qty='0',cnc_cfrm_qty='3',tot_ccld_amt='100')
                else:history.update(ft_ccld_qty='0',nccs_qty='0',ft_ccld_amt3='100',prcs_stat_name='취소완료')
                self.runtime.observer=self.observer=Observer(None,history)
                with self.assertRaisesRegex(ValueError,'OFFICIAL_FILL_NOTIONAL_CONTRADICTION'):self.collect()
    def test_positive_terminal_fill_needs_positive_decimal_gross_notional(self):
        self.observer.working=[]
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',prcs_stat_name='체결')
        for amount in ('0','-1','NaN',100.0,None):
            with self.subTest(amount=amount):
                self.observer.history[0]['ft_ccld_amt3']=amount
                with self.assertRaisesRegex(ValueError,'NOTIONAL'):self.collect()
    def test_same_quantity_but_different_gross_notional_is_a_read_race(self):
        self.observer.working[0]['ft_ccld_amt3']='101'
        with self.assertRaisesRegex(ValueError,'WORKING_HISTORY_NOTIONAL_RACE_OR_MISMATCH'):self.collect()
    def test_old_us_same_official_day_working_cannot_be_ignored_by_old_terminal_history(self):
        self.rekey_day('20261014')
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',ft_ccld_amt3='300',prcs_stat_name='체결')
        with self.assertRaisesRegex(ValueError,'WORKING_HISTORY_QUANTITY_RACE_OR_MISMATCH'):self.collect()
    def test_old_us_terminal_does_not_claim_different_echoed_day_reused_nccs_identity(self):
        self.rekey_day('20261014')
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',ft_ccld_amt3='300',prcs_stat_name='체결')
        self.observer.working[0]['ord_dt']='20261015'
        result=self.collect()['orders'][0]
        self.assertEqual(result['state'],'TERMINAL');self.assertIsNone(result['officialWorkingRow'])
        self.assertTrue(result['historicalTerminalOnly']);self.assertEqual(result['orderDate'],'20261014')
        self.observer.working[0].pop('ord_dt')
        with self.assertRaisesRegex(ValueError,'OFFICIAL_DATE_REQUIRED'):self.collect()
    def test_new_historical_terminal_get_rejects_explicit_cached_or_invalid_age(self):
        self.rekey_day('20261014');self.observer.working=[]
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',ft_ccld_amt3='300',prcs_stat_name='체결')
        for age in ('1','60','unknown','-1','0.5'):
            with self.subTest(age=age):
                def hook(path,query,continuation):
                    if path.endswith('inquire-ccnl'):
                        return {'rt_cd':'0','output':deepcopy(self.observer.history)},{'tr_cont':'D','age':age}
                self.observer.get_hook=hook
                with self.assertRaisesRegex(ValueError,'FRESH_GET_CACHE_AGE_UNVERIFIED'):self.collect()
    def test_historical_terminal_missing_date_is_calendar_unknown_not_explicit_stale(self):
        self.rekey_day('20261014');self.observer.working=[];self.observer.date_header=''
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',ft_ccld_amt3='300',prcs_stat_name='체결')
        result=self.collect()
        self.assertEqual(result['orders'][0]['state'],'TERMINAL')
        self.assertTrue(result['orders'][0]['historicalTerminalOnly'])
        self.assertEqual(result['queries'][1]['httpDateEvidence'][0]['status'],'UNVERIFIED')
        self.assertFalse(result['orderAuthorityGranted'])
    def test_rehashed_historical_query_cannot_insert_positive_cache_age(self):
        self.rekey_day('20261014');self.observer.working=[]
        self.observer.history[0].update(ft_ccld_qty='3',nccs_qty='0',ft_ccld_amt3='300',prcs_stat_name='체결')
        result=self.collect()
        result['queries'][1]['httpDateEvidence'][0]['ageHeader']='1';self.reseal(result)
        with self.assertRaisesRegex(ValueError,'FRESH_GET_CACHE_AGE_UNVERIFIED'):
            truth.validate_cleanup_truth(result,[self.owned],now=NOW)


if __name__=='__main__':unittest.main()
