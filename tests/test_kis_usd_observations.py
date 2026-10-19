from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

# No operational package __init__/env loader/order imports. The only adapter
# import used by the old reader is stubbed; USD paths never call these helpers.
ROOT = Path(__file__).resolve().parents[1]
# A private package keeps the intentionally forbidden signing adapters local
# to this read-only fixture. It must never replace a real running package.
PACKAGE = '_kis_usd_observation_fixture_isolated'
pkg = types.ModuleType(PACKAGE)
pkg.__path__ = [str(ROOT / 'live_trader')]
sys.modules[PACKAGE] = pkg
adapters = types.ModuleType(PACKAGE + '.live_adapters')
def forbidden(*args, **kwargs):
    raise AssertionError('ORDER_OR_SIGNING_HELPER_USED')
for key in ('build_upbit_authorization', 'sign_binance_query', 'split_kis_account'):
    setattr(adapters, key, forbidden)
sys.modules[PACKAGE + '.live_adapters'] = adapters
import importlib
usd = importlib.import_module(PACKAGE + '.kis_usd_observations')
ro = importlib.import_module(PACKAGE + '.read_only_account')

SETTINGS = {'KIS_APP_KEY': 'SYNTHETIC_KEY', 'KIS_APP_SECRET': 'SYNTHETIC_SECRET',
            'KIS_ACCOUNT_NO': '12345678-01', 'KIS_ACCOUNT_PRODUCT_CODE': '01',
            'KIS_ENV': 'real', 'KIS_BASE_URL': 'https://openapi.koreainvestment.com:9443'}
CASH = 'SETTLEMENT_BASIS_CASH'
PERIOD = 'REPORTED_PERIOD_TRANSACTIONS'
PRESENT = 'PRESENT_BALANCE'

def cash(amount='117.04', currency='USD'):
    return {'rt_cd': '0', 'output1': [], 'output2': [{'crcy_cd': currency, 'frcr_dncl_amt_2': amount}], 'output3': {}}

def period(**updates):
    row = {'trad_dt': '20261014', 'sttl_dt': '20261016', 'sll_buy_dvsn_cd': '02', 'pdno': 'AAPL',
           'crcy_cd': 'USD', 'ccld_qty': '3', 'tr_frcr_amt2': '603.17', 'frcr_excc_amt_1': '604.29',
           'dmst_frcr_fee1': '0.98', 'frcr_fee1': '0.14'}
    row.update(updates)
    return {'rt_cd': '0', 'output1': [row], 'output2': {'dmst_fee_smtl': '3.8'}, 'ctx_area_fk100': '', 'ctx_area_nk100': ''}

def response(kind=CASH, body=None, continuation='D', **headers):
    return deepcopy(body if body is not None else cash()), {'tr_id': usd.PATHS[kind][1], 'tr_cont': continuation, 'date': 'Fri, 16 Oct 2026 05:00:00 GMT', 'age': '0', **headers}

class Reader:
    def __init__(self, pages):
        self.settings = dict(SETTINGS)
        self.pages = list(pages)
        self.requests = []
        self.credentials = self.tokens = self.paces = 0
        self.on_get = None
    def _credentials(self, provider):
        assert provider == 'kis'
        self.credentials += 1
    def _kis_token(self):
        self.tokens += 1
        return 'SYNTHETIC_TOKEN'
    def _pace_kis(self):
        self.paces += 1
    def _get(self, provider, path, **kwargs):
        self.requests.append((provider, path, deepcopy(kwargs)))
        if self.on_get:
            self.on_get()
        item = self.pages.pop(0)
        if isinstance(item, BaseException):
            raise item
        return deepcopy(item)

def observe(reader, kind=CASH, **kwargs):
    return usd.KisUsdObservationReader(reader).observe(kind, **({'basis_date': '20261016'} if kind == CASH else {'start_date': '20261014', 'end_date': '20261016'} if kind == PERIOD else {}), **kwargs)

def rehash(body):
    result = deepcopy(body)
    result.pop('contentHash', None)
    result['contentHash'] = usd.canonical_hash(result)
    return result

class ObservationTests(unittest.TestCase):
    def test_constructor_has_no_io(self):
        r = Reader([])
        usd.KisUsdObservationReader(r)
        self.assertEqual((r.credentials,r.tokens,r.paces,r.requests), (0,0,0,[]))
    def test_cash_role_exact_query_and_all_authorities_false(self):
        r = Reader([response()])
        captured = observe(r)
        result = usd.parse_reported_observation(captured)
        self.assertEqual(result['reportedUsdCashLiteral'], '117.04')
        self.assertEqual(result['reportedUsdRows'][0]['reportedCashRole'], 'SETTLEMENT_BASIS_CURRENCY_DEPOSIT')
        self.assertTrue(all(result[k] is False for k in usd.FALSE_CLAIMS))
        self.assertEqual(r.requests[0][2]['query'], {'CANO':'12345678','ACNT_PRDT_CD':'01','BASS_DT':'20261016','WCRC_FRCR_DVSN_CD':'02','INQR_DVSN_CD':'01'})
        self.assertEqual(r.tokens, 1)
        self.assertEqual(r.paces, 1)
    def test_present_cash_is_not_settlement_baseline(self):
        result = usd.parse_reported_observation(observe(Reader([response(PRESENT)]), PRESENT))
        self.assertEqual(result['reportedUsdRows'][0]['reportedCashRole'], 'PRESENT_FOREIGN_USABLE_AMOUNT_NOT_SETTLEMENT_BASELINE')
        self.assertFalse(result['accountBaselineEstablished'])
    def test_period_signed_reported_fee_no_net_equation_or_order_allocation(self):
        result = usd.parse_reported_observation(observe(Reader([response(PERIOD, period(frcr_fee1='-0.14'))]), PERIOD))
        row = result['reportedUsdRows'][0]
        self.assertEqual(row['frcr_fee1'], '-0.14')
        self.assertEqual(row['frcr_excc_amt_1'], '604.29')
        self.assertNotIn('netCash', row)
        self.assertNotIn('orderId', row)
        self.assertFalse(result['actualFeeObserved'])
    def test_period_all_venues_official_blank_and_explicit_registration_dates(self):
        r = Reader([response(PERIOD, period())])
        observe(r, PERIOD)
        q = r.requests[0][2]['query']
        self.assertEqual(q['OVRS_EXCG_CD'], '')
        self.assertEqual(q['ERLM_STRT_DT'], '20261014')
        self.assertEqual(q['ERLM_END_DT'], '20261016')
    def test_blank_or_absent_continuation_is_unknown(self):
        for marker in (None, ''):
            body, headers = response(continuation='')
            if marker is None:
                headers.pop('tr_cont')
            result = observe(Reader([(body, headers)]))
            self.assertEqual(result['paginationStatus'], 'UNKNOWN')
            self.assertFalse(result['settlementFinalityVerified'])
    def test_explicit_last_page_required_for_complete(self):
        for marker in ('D','E'):
            self.assertEqual(observe(Reader([response(continuation=marker)]))['paginationStatus'], 'COMPLETE')
    def test_page_limit_preserves_partial_never_complete(self):
        result = observe(Reader([response(continuation='F')]), max_pages=1)
        self.assertEqual(result['paginationStatus'], 'PARTIAL')
        self.assertEqual(result['paginationReason'], 'KIS_USD_PAGE_LIMIT')
    def test_period_cursor_copied_exactly_and_n_header(self):
        first = period(); first.update(ctx_area_fk100=' FK ', ctx_area_nk100='opaque')
        r = Reader([response(PERIOD,first,'M'),response(PERIOD,period(),'E')])
        result = observe(r,PERIOD)
        self.assertEqual(result['paginationStatus'],'COMPLETE')
        self.assertEqual(r.requests[1][2]['query']['CTX_AREA_FK100'],' FK ')
        self.assertEqual(r.requests[1][2]['headers']['tr_cont'],'N')
        self.assertEqual(r.tokens,1)
        self.assertEqual(r.paces,2)
    def test_repeated_cursor_fails_before_third_get(self):
        page = period(); page.update(ctx_area_fk100='a',ctx_area_nk100='b')
        r=Reader([response(PERIOD,page,'M'),response(PERIOD,page,'F')])
        with self.assertRaisesRegex(usd.KisUsdObservationError,'CURSOR'):
            observe(r,PERIOD)
        self.assertEqual(len(r.requests),2)
    def test_empty_or_nonstring_cursor_more_fails(self):
        for cursor in ('',None,3):
            page=period();page['ctx_area_nk100']=cursor
            with self.assertRaises(usd.KisUsdObservationError):
                observe(Reader([response(PERIOD,page,'M')]),PERIOD)
    def test_no_cursor_cash_repeated_more_body_fails(self):
        with self.assertRaisesRegex(usd.KisUsdObservationError,'REPEATED'):
            observe(Reader([response(continuation='M'),response(continuation='M')]))
    def test_repeated_currency_balances_never_summed(self):
        result=usd.parse_reported_observation(observe(Reader([response(continuation='M'),response(continuation='D')])))
        self.assertEqual(len(result['reportedUsdRows']),2)
        self.assertEqual(result['reportedUsdCashLiteral'],'117.04')
    def test_conflicting_currency_balances_remain_ambiguous(self):
        result=usd.parse_reported_observation(observe(Reader([response(continuation='M'),response(body=cash('999'),continuation='D')])))
        self.assertEqual(result['usdRowStatus'],'CONFLICTING_REPORTED_BALANCES')
        self.assertIsNone(result['reportedUsdCashLiteral'])
    def test_absent_usd_is_not_zero(self):
        result=usd.parse_reported_observation(observe(Reader([response(body=cash('1','HKD'))])))
        self.assertEqual(result['usdRowStatus'],'ABSENT_NOT_ZERO')
        self.assertIsNone(result['reportedUsdCashLiteral'])
    def test_missing_currency_cannot_be_inferred_from_query(self):
        b=cash();del b['output2'][0]['crcy_cd']
        with self.assertRaisesRegex(usd.KisUsdObservationError,'CURRENCY'):
            observe(Reader([response(body=b)]))
    def test_missing_fee_not_default_zero(self):
        b=period();del b['output1'][0]['dmst_frcr_fee1']
        with self.assertRaisesRegex(usd.KisUsdObservationError,'NUMBER'):
            observe(Reader([response(PERIOD,b)]),PERIOD)
    def test_nan_float_bool_blank_numbers_fail(self):
        for value in ('NaN','Infinity','',None,True,1.1,'1,000'):
            with self.subTest(value=value),self.assertRaises(usd.KisUsdObservationError):
                observe(Reader([response(body=cash(value))]))
    def test_invalid_explicit_date_before_oauth(self):
        for value in ('2026-10-16','20260230',None,''):
            r=Reader([])
            with self.assertRaises(usd.KisUsdObservationError):
                usd.KisUsdObservationReader(r).observe(CASH,basis_date=value)
            self.assertEqual(r.tokens,0)
    def test_wrong_real_origin_or_demo_before_oauth(self):
        for key,value in (('KIS_ENV','demo'),('KIS_ENV','vts'),('KIS_BASE_URL','https://example.invalid')):
            r=Reader([]);r.settings[key]=value
            with self.assertRaises(usd.KisUsdObservationError):observe(r)
            self.assertEqual(r.tokens,0)
            self.assertEqual(r.requests,[])
    def test_account_product_disagreement_before_oauth(self):
        r=Reader([]);r.settings['KIS_ACCOUNT_PRODUCT_CODE']='02'
        with self.assertRaises(usd.KisUsdObservationError):observe(r)
        self.assertEqual(r.tokens,0)
    def test_mid_get_credential_scope_change_fails(self):
        r=Reader([response()]);r.on_get=lambda:r.settings.update(KIS_APP_SECRET='OTHER')
        with self.assertRaisesRegex(usd.KisUsdObservationError,'SCOPE_CHANGED'):observe(r)
    def test_remote_reject_and_wrong_echo_fail(self):
        for b,h in (({'rt_cd':'1'},response()[1]),(cash(),{'tr_id':'CTOS4001R','tr_cont':'D'})):
            with self.assertRaises(usd.KisUsdObservationError):observe(Reader([(b,h)]))
    def test_second_page_transport_error_never_returns_partial_as_complete(self):
        with self.assertRaisesRegex(ro.ReadFailure,'UNAVAILABLE'):
            observe(Reader([response(continuation='M'),ro.ReadFailure('READ_UNAVAILABLE')]))
    def test_unknown_continuation_rejected(self):
        with self.assertRaisesRegex(usd.KisUsdObservationError,'CONTINUATION'):
            observe(Reader([response(continuation='N')]))
    def test_capture_page_hash_tamper_rejected(self):
        c=observe(Reader([response()]));c['pages'][0]['body']['output2'][0]['frcr_dncl_amt_2']='1'
        c=rehash(c)
        with self.assertRaisesRegex(usd.KisUsdObservationError,'PAGE_HASH'):usd.parse_reported_observation(c)
    def test_rehashed_positive_authority_rejected(self):
        for key in usd.FALSE_CLAIMS:
            c=observe(Reader([response()]));c[key]=True
            with self.assertRaisesRegex(usd.KisUsdObservationError,'AUTHORITY'):usd.parse_reported_observation(rehash(c))
    def test_rehashed_query_swap_rejected(self):
        c=observe(Reader([response()]));c['pages'][0]['queryWithoutAccount']['BASS_DT']='20261015'
        with self.assertRaisesRegex(usd.KisUsdObservationError,'QUERY'):usd.parse_reported_observation(rehash(c))
    def test_http_date_age_are_retrieval_only_not_posting_or_fx(self):
        c=observe(Reader([response(age='30')]))
        self.assertEqual(c['pages'][0]['httpAgeRaw'],'30')
        self.assertFalse(c['finalPostingAuthority'])
        self.assertFalse(c['fxFreshnessVerified'])
        self.assertNotIn('brokerPostingAt',c)
    def test_safe_summary_has_no_private_literals_or_raw_values(self):
        p=usd.parse_reported_observation(observe(Reader([response()])))
        encoded=json.dumps(usd.safe_observation_summary(p))
        for token in ('117.04','12345678','SYNTHETIC_KEY','SYNTHETIC_SECRET','SYNTHETIC_TOKEN','frcr_dncl_amt_2'):
            self.assertNotIn(token,encoded)
    def test_derived_value_tamper_not_logged(self):
        p=usd.parse_reported_observation(observe(Reader([response()])))
        p['reportedUsdCashLiteral']='999'
        with self.assertRaisesRegex(usd.KisUsdObservationError,'DERIVED'):usd.safe_observation_summary(p)
    def test_rehashed_arbitrary_pagination_reason_never_reaches_safe_summary(self):
        for marker in ('D','', 'F'):
            capture=observe(Reader([response(continuation=marker)]),max_pages=1)
            capture['paginationReason']='SYNTHETIC_PRIVATE_LITERAL'
            with self.assertRaisesRegex(usd.KisUsdObservationError,'PAGINATION'):
                usd.parse_reported_observation(rehash(capture))

class FakeResponse:
    def __init__(self,url,raw=b'{"rt_cd":"0"}',status=200,headers=None):
        self.url,self.raw,self.status=url,raw,status
        self.headers=headers or {}
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def geturl(self):return self.url
    def read(self,n):return self.raw[:n]

class Opener:
    def __init__(self, mutate=None,raw=b'{"rt_cd":"0"}',status=200):
        self.calls=[];self.mutate=mutate;self.raw=raw;self.status=status
    def open(self,req,timeout):
        self.calls.append((req,timeout))
        return FakeResponse(self.mutate(req.full_url) if self.mutate else req.full_url,self.raw,self.status)

class TransportTests(unittest.TestCase):
    def setUp(self):
        self.transport=ro.ReadOnlyTransport();self.opener=Opener();self.transport.opener=self.opener
    def test_all_three_exact_real_pairs_use_fixed_origin_get_8s(self):
        for path,tr in usd.PATHS.values():
            self.transport.request('kis','GET',path,query={'PDNO':'x&path=/order'},headers={'tr_id':tr})
            req,timeout=self.opener.calls[-1]
            self.assertTrue(req.full_url.startswith(ro.ORIGINS['kis']+path+'?'))
            self.assertEqual(req.method,'GET');self.assertIsNone(req.data);self.assertEqual(timeout,8)
            self.assertIn('%26path%3D%2Forder',req.full_url)
    def test_pair_swap_and_mock_ids_reject_before_hop(self):
        for path,tr in usd.PATHS.values():
            for wrong in ('VTRP6504R','VTOS4001R','VTRP6010R',next(t for p,t in usd.PATHS.values() if p!=path)):
                with self.assertRaisesRegex(ro.ReadBoundaryError,'TR_ID'):self.transport.request('kis','GET',path,headers={'tr_id':wrong})
        self.assertEqual(self.opener.calls,[])
    def test_mutation_method_body_url_and_order_route_block_before_hop(self):
        p,t=usd.PATHS[CASH]
        for method,path,body in (('POST',p,None),('DELETE',p,None),('GET',p,{}),('GET','https://evil.invalid'+p,None),('GET',p+'/../order',None),('GET','/uapi/overseas-stock/v1/trading/order',None)):
            with self.assertRaises(ro.ReadBoundaryError):self.transport.request('kis',method,path,headers={'tr_id':t},body=body)
        self.assertEqual(self.opener.calls,[])
    def test_redirect_no_second_hop(self):
        self.opener.mutate=lambda _: 'https://example.invalid'
        p,t=usd.PATHS[CASH]
        with self.assertRaisesRegex(ro.ReadFailure,'REDIRECT_BLOCKED'):self.transport.request('kis','GET',p,headers={'tr_id':t})
        self.assertEqual(len(self.opener.calls),1)
    def test_redirect_handler_rejects_without_request(self):
        with self.assertRaisesRegex(ro.ReadFailure,'REDIRECT_BLOCKED'):ro.NoRedirect().redirect_request(None,None,302,'',{},'https://example.invalid')
    def test_four_mib_limit_preserved(self):
        self.opener.raw=b' '*(4*1024*1024+1)
        p,t=usd.PATHS[CASH]
        with self.assertRaisesRegex(ro.ReadFailure,'RESPONSE_LIMIT'):self.transport.request('kis','GET',p,headers={'tr_id':t})
    def test_new_routes_duplicate_keys_and_nan_rejected_no_private_error_text(self):
        for raw in (b'{"rt_cd":"0","rt_cd":"1"}',b'{"a":NaN}',b'{"secret":"remote-private-value"'):
            self.opener.raw=raw;p,t=usd.PATHS[CASH]
            with self.assertRaisesRegex(ro.ReadFailure,'^READ_UNAVAILABLE$'):self.transport.request('kis','GET',p,headers={'tr_id':t})
    def test_inventory_routes_reject_duplicate_keys_without_changing_quote_routes(self):
        self.opener.raw=b'{"x":1,"x":2}'
        for p in ('/uapi/overseas-stock/v1/trading/inquire-balance',
                  '/uapi/domestic-stock/v1/trading/inquire-balance',
                  '/uapi/overseas-stock/v1/trading/inquire-nccs',
                  '/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl'):
            with self.assertRaisesRegex(ro.ReadFailure,'^READ_UNAVAILABLE$'):
                self.transport.request('kis','GET',p,headers={'tr_id':ro.KIS_TR_IDS[p]})
        p='/uapi/overseas-price/v1/quotations/price'
        value,_=self.transport.request('kis','GET',p,headers={'tr_id':ro.KIS_TR_IDS[p]})
        self.assertEqual(value,{'x':2})
    def test_oauth_only_exact_token_schema_not_other_posts(self):
        self.transport.request('kis','POST','/oauth2/tokenP',body={'grant_type':'client_credentials','appkey':'SYNTHETIC_KEY','appsecret':'SYNTHETIC_SECRET'})
        for path,body in (('/oauth2/tokenP',{'grant_type':'other','appkey':'k','appsecret':'s'}),('/oauth2/Approval',{'grant_type':'client_credentials','appkey':'k','appsecret':'s'})):
            with self.assertRaises(ro.ReadBoundaryError):self.transport.request('kis','POST',path,body=body)
        self.assertEqual(len(self.opener.calls),1)
    def test_event_metadata_contains_no_query_headers_or_private_values(self):
        p,t=usd.PATHS[CASH]
        self.transport.request('kis','GET',p,query={'CANO':'12345678'},headers={'tr_id':t,'authorization':'Bearer SYNTHETIC_TOKEN'})
        self.assertEqual(set(self.transport.events[0]),{'provider','method','path','status'})
        self.assertNotIn('12345678',json.dumps(self.transport.events))

class ActualReaderIntegrationTests(unittest.TestCase):
    def test_actual_reader_oauth_memory_reuse_three_new_gets_no_signing_or_disk(self):
        class FixtureOpener:
            def __init__(self):self.calls=[]
            def open(self,req,timeout):
                self.calls.append((req.method,req.full_url))
                if req.full_url.endswith('/oauth2/tokenP'):
                    return FakeResponse(req.full_url,b'{"access_token":"SYNTHETIC_TOKEN","expires_in":3600}')
                kind=next(k for k,(p,_) in usd.PATHS.items() if p in req.full_url)
                payload=period() if kind==PERIOD else cash()
                return FakeResponse(req.full_url,json.dumps(payload).encode(),headers={'tr_id':usd.PATHS[kind][1],'tr_cont':'D'})
        transport=ro.ReadOnlyTransport();opener=FixtureOpener();transport.opener=opener
        with patch.dict(ro._TOKEN_CACHE,{},clear=True),patch.object(ro.ReadOnlyBrokerReader,'_pace_kis') as pace:
            reader=ro.ReadOnlyBrokerReader(SETTINGS,transport=transport)
            self.assertEqual(opener.calls,[])
            for kind in (CASH,PERIOD,PRESENT):
                result=observe(reader,kind)
                self.assertEqual(result['status'],'OBSERVED')
                self.assertFalse(result['actualFeeObserved'])
            self.assertEqual(sum(method=='POST' for method,_ in opener.calls),1)
            self.assertEqual(sum(method=='GET' for method,_ in opener.calls),3)
            self.assertEqual(pace.call_count,4) # normal token + every GET retained
            self.assertEqual(len(ro._TOKEN_CACHE),1)
    def test_actual_reader_env_demo_prevents_even_oauth(self):
        transport=ro.ReadOnlyTransport();opener=Opener();transport.opener=opener
        settings={**SETTINGS,'KIS_ENV':'demo'}
        reader=ro.ReadOnlyBrokerReader(settings,transport=transport)
        with self.assertRaisesRegex(usd.KisUsdObservationError,'REAL_ENVIRONMENT'):
            observe(reader)
        self.assertEqual(opener.calls,[])

if __name__=='__main__':unittest.main()

class ImportIsolationTests(unittest.TestCase):
    def test_loading_readonly_fixture_preserves_existing_production_package_and_adapters(self):
        import runpy
        production = types.ModuleType('live_trader')
        production_adapters = types.ModuleType('live_trader.live_adapters')
        production_adapters.build_kis_live_order_request = object()
        production_adapters.kis_prepared_payload_hash = object()
        with patch.dict(sys.modules, {'live_trader': production,
                                     'live_trader.live_adapters': production_adapters}):
            runpy.run_path(str(Path(__file__).resolve()), run_name='_readonly_fixture_import_probe')
            self.assertIs(sys.modules['live_trader'], production)
            self.assertIs(sys.modules['live_trader.live_adapters'], production_adapters)
            self.assertIs(production_adapters.build_kis_live_order_request,
                          sys.modules['live_trader.live_adapters'].build_kis_live_order_request)
            self.assertIs(production_adapters.kis_prepared_payload_hash,
                          sys.modules['live_trader.live_adapters'].kis_prepared_payload_hash)

