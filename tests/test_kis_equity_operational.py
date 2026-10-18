from __future__ import annotations
from copy import deepcopy
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import importlib,json,socket,sqlite3,sys,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch

APP=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(APP.parents[1]/'packages/trading_runtime'))
PACKAGE='_kis_operational_v2_isolated';pkg=types.ModuleType(PACKAGE);pkg.__path__=[str(APP/'live_trader')];sys.modules[PACKAGE]=pkg
service=importlib.import_module(PACKAGE+'.kis_equity_execution')
reader_module=importlib.import_module(PACKAGE+'.kis_equity_reader')
ledger=importlib.import_module(PACKAGE+'.kis_owned_capital')
runtime_module=importlib.import_module(PACKAGE+'.kis_equity_runtime')
program_ledger_module=importlib.import_module(PACKAGE+'.program_ledger')
ticket_module=importlib.import_module(PACKAGE+'.native_order_ticket')
profiles=importlib.import_module(PACKAGE+'.account_profiles')
from trading_runtime.execution_quantity import seal_quantity_contract

NOW=datetime.now(timezone.utc)
SETTINGS={'KIS_OVERSEAS_FUNDING_MODE':'FOREIGN_CASH','KIS_APP_KEY':'SYNTHETIC_APPKEY','KIS_APP_SECRET':'SYNTHETIC_SECRET','KIS_ACCOUNT_NO':'12345678','KIS_ACCOUNT_PRODUCT_CODE':'01'}
ACCOUNT=profiles.saved_account_profiles(SETTINGS)[0]


def fixture(exchange='NASD',side='BUY'):
    symbol='005930' if exchange=='KRX' else {'NASD':'AAPL','NYSE':'IBM','AMEX':'SPY'}[exchange]
    currency='KRW' if exchange=='KRX' else 'USD'
    contract=seal_quantity_contract({'schemaVersion':'native-equity-quantity-v1','provider':'kis','assetClass':'EQUITY',
        'quantityUnit':'SHARE','executionScope':'LOCAL_MODEL_ONLY','symbol':symbol,'baseCurrency':symbol,
        'quoteCurrency':currency,'exchange':exchange,'orderType':'MARKET','stepSize':'1','minQuantity':'1','maxQuantity':'1000000',
        'minNotional':'0','maxNotional':'0','notionalReferenceMinutes':0,'capabilityStatus':'UNKNOWN','provenance':{'references':['synthetic source fixture']}})
    artifact={'standalone':True,'executionSizing':{'mode':'capital_percentage','fractionalAllowed':False,
        'capitalBasis':'PORTFOLIO_EQUITY_WITH_CASH_CAP','quantityContract':contract}}
    spec=types.SimpleNamespace(provider='kis',broker_id='kis',symbol=symbol,instrument_id=symbol,timeframe='5m',
        strategy_id='source',strategy_instance_id='member',portfolio_hash='e'*64,artifact_hash='c'*64,target_weight=1,
        parameters={'positionSize':5,'fee':0.1,'slippage':0.2,'executionTiming':'next-open'},artifact=artifact)
    policies=service.prepare_reviewed_policies(ACCOUNT,spec,authorization_text='User explicitly authorized preparation; no financial execution',now=NOW)
    boundary={'provider':'kis','symbol':symbol,'attestation':'KIS_WEBSOCKET','recoveryOnly':False,'promotionEligible':True,
        'sourceArtifactHash':spec.artifact_hash,'startTime':NOW.isoformat(),'observedTime':NOW.isoformat(),
        'open':'100','rawHash':'a'*64,'closedBarHash':'b'*64,'evaluationKey':'evaluation-exact'}
    price=service.limit_price(100,side,'1' if exchange=='KRX' else '0.01',50)
    observation=service.seal({'version':service.OBSERVATION,'brokerId':'kis','exchange':exchange,'currency':currency,
        'symbol':symbol,'side':side,'orderClass':'00','queryPrice':price,'queryPriceRole':'REQUEST_LIMIT_PRICE_NOT_BROKER_ECHO',
        'quotePriceRole':'CURRENT_MARK_NOT_NEXT_OPEN','profileId':ACCOUNT['profileId'],'accountFingerprint':ACCOUNT['accountFingerprint'],
        'credentialConfigurationHash':ACCOUNT['credentialConfigurationHash'],'complete':True,'orderable':True,
        'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE','priceTick':'1' if exchange=='KRX' else '0.01','lotSize':'1',
        'availableCash':'100000','maximumBuyQuantity':'3','sellableQuantity':'4','holdingQuantity':'10',
        'startedAt':NOW.isoformat(),'asOf':NOW.isoformat()})
    context={'deploymentId':'deployment','sourceArtifactHash':spec.artifact_hash,'strategyInstanceId':spec.strategy_instance_id,
        'portfolioHash':spec.portfolio_hash,'quoteCurrency':currency,'equityScope':'OWNED_DEPLOYMENT_ONLY','accountKnown':True,
        'accountFresh':True,'capitalAccounting':'OWNED_CONSERVATIVE_COST_RESERVE','capitalScopeHash':'d'*64,
        'deploymentEquity':'100000' if exchange=='KRX' else '10000','availableCash':'100000',
        'savedAccountProfile':ACCOUNT,'executionPolicy':policies['executionPolicy'],'feeReservePolicy':policies['feeReservePolicy'],
        'savedSlippageBps':50,'nativeMarketObservation':observation,'exactOpenBoundary':boundary}
    return spec,contract,context


def quantity(spec,context,side='BUY'):
    return service.live_equity_quantity(spec,side=side,price=100,allocation_weight=1,identity={'deploymentId':'deployment'},
        context=context,position_quantity=9,now=NOW)


def intent(spec,context,result,side='BUY'):
    return types.SimpleNamespace(symbol=spec.symbol,side=side,quantity=result['quantityForSubmission'],
        reference_price=float(result['modelResult']['fillPrice']),metadata={'broker_id':'kis','native_quantity_status':'READY',
            'native_quantity_model':result['modelResult'],'native_preflight':result['preflightBinding'],
            'native_open_boundary':context['exactOpenBoundary'],'source_artifact_hash':spec.artifact_hash,
            'deployment_id':'deployment','runtime_evaluation_key':'evaluation-exact'})


class FakeReader:
    def __init__(self,exchange='NASD'):
        self.settings=dict(SETTINGS);self.events=[];self.exchange=exchange;self.mutate=None
    def _credentials(self,provider):
        if not self.settings.get('KIS_APP_SECRET'):raise reader_module.ReadFailure('CREDENTIALS_MISSING')
    def _kis_token(self):return 'synthetic-token'
    def _pace_kis(self):pass
    def _get(self,provider,path,*,query,headers):
        self.events.append((path,deepcopy(query),headers.get('tr_id')))
        symbol='005930' if self.exchange=='KRX' else {'NASD':'AAPL','NYSE':'IBM','AMEX':'SPY'}[self.exchange]
        if path.endswith('price-detail'):
            data={'rsym':'D'+service.VENUES[self.exchange][1]+symbol,'curr':'USD','last':'100','e_ordyn':'거래 가능','e_hogau':'0.0100','vnit':'1','zdiv':'4'}
            out={'rt_cd':'0','output':data}
        elif path.endswith('/inquire-price'):
            out={'rt_cd':'0','output':{'stck_shrn_iscd':symbol,'temp_stop_yn':'N','stck_prpr':'100','aspr_unit':'1','hts_deal_qty_unit_val':'1'}}
        elif path.endswith('/inquire-balance'):out={'rt_cd':'0','output1':[]}
        elif path.endswith('/inquire-nccs') or path.endswith('/inquire-psbl-rvsecncl'):out={'rt_cd':'0','output':[]}
        elif path.endswith('/inquire-psamount'):out={'rt_cd':'0','output':{'tr_crcy_cd':'USD','ord_psbl_frcr_amt':'10000','ovrs_ord_psbl_amt':'9000','ord_psbl_qty':'3','max_ord_psbl_qty':'999999','echm_af_ord_psbl_qty':'999999'}}
        elif path.endswith('/inquire-psbl-order'):out={'rt_cd':'0','output':{'ord_psbl_cash':'100000','nrcvb_buy_amt':'90000','nrcvb_buy_qty':'3' if query['ORD_DVSN']=='01' else '5','psbl_qty_calc_unpr':'130','max_buy_qty':'999999'}}
        else:raise AssertionError('Unexpected endpoint '+path)
        if self.mutate:out=self.mutate(path,out)
        return out,{}


class KisEquityOperationalTests(unittest.TestCase):
    def setUp(self):
        self.net=patch.object(socket,'socket',side_effect=AssertionError('all real network forbidden'))
        self.net.start();self.addCleanup(self.net.stop)

    def test_all_four_venues_ready_with_separate_bounded_limit_model_without_v1_changes(self):
        for exchange in service.VENUES:
            spec,contract,context=fixture(exchange);before=deepcopy(contract)
            result=quantity(spec,context)
            self.assertEqual(result['status'],'READY',(exchange,result));self.assertEqual(result['quantityForSubmission'],3)
            self.assertEqual(contract,before);self.assertEqual(contract['capabilityStatus'],'UNKNOWN')
            self.assertFalse(result['modelResult']['actualFeeObserved']);self.assertFalse(result['orderAuthorityGranted'])
            self.assertEqual(result['modelResult']['fillPriceRole'],'PROTECTIVE_LIMIT_NOT_ACTUAL_FILL')
            wire=ticket_module.validate_native_ticket(intent(spec,context,result),now=NOW)
            self.assertEqual(wire['orderClass'],'00');self.assertEqual(wire['exchange'],exchange)

    def test_sell_only_owned_whole_shares_and_broker_available_quantity(self):
        spec,contract,context=fixture('NASD','SELL');result=quantity(spec,context,'SELL')
        self.assertEqual(result['status'],'READY',result);self.assertEqual(result['quantityForSubmission'],4)
        self.assertFalse(result['modelResult']['actualPersonalFeeObserved'])

    def test_missing_unknown_zero_or_insufficient_fee_bounds_fail_before_ready(self):
        for change in ('missing','zero','public-as-personal','too-low','missing-minimum'):
            spec,contract,context=fixture()
            if change=='missing':context.pop('feeReservePolicy')
            else:
                fee=deepcopy(context['feeReservePolicy'])
                if change=='zero':fee['reserveRates']['BUY']='0'
                elif change=='too-low':fee['reserveRates']['BUY']='0.0001'
                elif change=='public-as-personal':fee['actualPersonalFeeObserved']=True
                else:fee['minimumCostReserves']['BUY']='0'
                context['feeReservePolicy']=service.seal(fee)
            self.assertEqual(quantity(spec,context)['status'],'BLOCKED',change)

    def test_minimum_reserve_can_block_small_capital_without_rounding_up(self):
        spec,contract,context=fixture();context['deploymentEquity']='10'
        self.assertEqual(quantity(spec,context)['status'],'BLOCKED')

    def test_query_request_price_is_never_relabelled_as_a_broker_echo(self):
        spec,contract,context=fixture();observation=deepcopy(context['nativeMarketObservation'])
        observation['queryPriceRole']='BROKER_ECHO';context['nativeMarketObservation']=service.seal(observation)
        self.assertEqual(quantity(spec,context)['status'],'BLOCKED')

    def test_stale_foreign_or_wrong_source_policies_fail_closed(self):
        for change in ('stale','account','source','slippage'):
            spec,contract,context=fixture();policy=deepcopy(context['executionPolicy'])
            if change=='stale':policy['validUntil']=(NOW-timedelta(seconds=1)).isoformat()
            elif change=='account':policy['accountFingerprint']='f'*64
            elif change=='source':policy['sourceArtifactHash']='f'*64
            else:policy['protectiveLimitBps']='51'
            context['executionPolicy']=service.seal(policy)
            self.assertEqual(quantity(spec,context)['status'],'BLOCKED',change)

    def test_no_next_open_no_ownership_or_stale_cap_does_not_use_a_ticker_as_open(self):
        for change in ('ticker','whole-account','stale'):
            spec,contract,context=fixture()
            if change=='ticker':context['exactOpenBoundary']['attestation']='REST_TICKER'
            elif change=='whole-account':context['equityScope']='ACCOUNT_TOTAL'
            else:
                obs=deepcopy(context['nativeMarketObservation']);obs['startedAt']=(NOW-timedelta(seconds=16)).isoformat();context['nativeMarketObservation']=service.seal(obs)
            self.assertEqual(quantity(spec,context)['status'],'BLOCKED',change)

    def test_final_ticket_detects_changed_quantity_decimal_cost_and_credentials(self):
        spec,contract,context=fixture();result=quantity(spec,context);ticket=intent(spec,context,result)
        service.validate_ticket(ticket,now=NOW,settings=SETTINGS)
        with self.assertRaises(ValueError):service.validate_ticket(ticket,now=NOW,settings={**SETTINGS,'KIS_APP_SECRET':'changed'})
        ticket.quantity+=1
        with self.assertRaises(ValueError):service.validate_ticket(ticket,now=NOW)
        ticket=intent(spec,context,result);bad=service.seal({**ticket.metadata['native_quantity_model'],'costReserve':'0'})
        ticket.metadata['native_quantity_model']=bad;ticket.metadata['native_preflight']['kisEquityTicket']=bad
        with self.assertRaises(ValueError):service.validate_ticket(ticket,now=NOW)

    def test_authenticated_get_reader_uses_exact_all_venues_and_no_us_echo(self):
        for exchange in service.VENUES:
            spec,contract,context=fixture(exchange);fake=FakeReader(exchange)
            observed=reader_module.KisEquityReader(fake,clock=lambda:NOW).observe(contract,side='BUY',raw_open=100,protective_bps=50)
            self.assertEqual(observed['maximumBuyQuantity'],'3')
            self.assertEqual(observed['queryPriceRole'],'REQUEST_LIMIT_PRICE_NOT_BROKER_ECHO')
            self.assertEqual(observed['brokerCalculationPrice'], '130' if exchange=='KRX' else None)
            self.assertFalse(observed['actualPersonalFeeObserved'])
            self.assertTrue(all('order-cash' not in event[0] and not event[0].endswith('/order') for event in fake.events))
            if exchange!='KRX':
                cap=next(e for e in fake.events if e[0].endswith('/inquire-psamount'))
                self.assertEqual(cap[1]['OVRS_EXCG_CD'],exchange)
                pending=next(e for e in fake.events if e[0].endswith('/inquire-nccs'))
                self.assertEqual(pending[1]['OVRS_EXCG_CD'],'NASD')

    def test_current_currency_quote_lot_halt_and_open_orders_are_not_assumed(self):
        for field,value in (('curr','KRW'),('rsym','DNASOTHER'),('vnit','10'),('e_ordyn','거래 불가')):
            spec,contract,context=fixture();fake=FakeReader();fake.mutate=lambda path,out:{**out,'output':{**out['output'],field:value}} if path.endswith('price-detail') else out
            with self.assertRaises((ValueError,reader_module.ReadFailure)):
                reader_module.KisEquityReader(fake,clock=lambda:NOW).observe(contract,side='BUY',raw_open=100,protective_bps=50)

    def _scope(self,currency='USD',deployment='deployment'):
        value={'deploymentId':deployment,'accountFingerprint':ACCOUNT['accountFingerprint'],'credentialConfigurationHash':ACCOUNT['credentialConfigurationHash'],
            'accountProfileId':ACCOUNT['profileId'],'reviewBindingHash':'d'*64,'capitalPercent':'100','quoteCurrency':currency,
            'members':[{'strategyInstanceId':'member','sourceArtifactHash':'c'*64,'portfolioHash':'e'*64,'symbol':'AAPL' if currency=='USD' else '005930','quoteCurrency':currency}]}
        return {**value,'scopeHash':service.stable_sha256(value)}

    def _observation(self,currency='USD',cash='10000'):
        return {'profileId':ACCOUNT['profileId'],'accountFingerprint':ACCOUNT['accountFingerprint'],'credentialConfigurationHash':ACCOUNT['credentialConfigurationHash'],
            'currency':currency,'availableCash':cash,'asOf':NOW.isoformat(),'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE'}

    def test_kis_reservations_separate_currency_and_reject_same_cash_double_claim(self):
        with tempfile.TemporaryDirectory() as folder:
            book=ledger.KisOwnedCapitalStore(Path(folder)/'book.sqlite3')
            book.initialize(self._scope(),self._observation(),now=NOW)
            book.initialize(self._scope('KRW','kr-deployment'),self._observation('KRW','1000000'),now=NOW)
            with self.assertRaises(ValueError):book.initialize(self._scope('USD','duplicate-dollar'),self._observation(),now=NOW)
            projected=book.project(self._scope(),{})
            self.assertEqual(projected['ownedCash'],'10000');self.assertFalse(projected['actualFeesReconciled'])

    def test_exact_terminal_fill_preserves_owned_positions_and_cost_holdback_is_not_actual_fee(self):
        spec,contract,context=fixture();result=quantity(spec,context);ticket=result['modelResult']
        metadata={'deployment_id':'deployment','strategy_instance_id':'member','source_artifact_hash':'c'*64,'portfolio_hash':'e'*64,
            'native_account_fingerprint':ACCOUNT['accountFingerprint'],'native_quantity_model':ticket}
        dispatch={'broker_id':'kis','metadata':metadata,'order_date':'20261015','organization_no':'1','broker_order_id':'2','side':'BUY'}
        official={'ord_dt':'20261015','ord_gno_brno':'1','odno':'2','pdno':'AAPL','sll_buy_dvsn_cd':'02','ovrs_excg_cd':'NASD','tr_crcy_cd':'USD',
            'ft_ord_qty':'3','ft_ccld_qty':'3','nccs_qty':'0','ft_ccld_amt3':'300','prcs_stat_name':'체결','ord_tmd':'103000'}
        scope=self._scope();receipt=ledger.terminal_receipt(scope,spec,dispatch,official)
        with tempfile.TemporaryDirectory() as folder:
            book=ledger.KisOwnedCapitalStore(Path(folder)/'book.sqlite3');book.initialize(scope,self._observation(),now=NOW)
            self.assertTrue(book.append_terminal_receipt(scope,receipt));self.assertFalse(book.append_terminal_receipt(scope,receipt))
            projected=book.project(scope,{'member':'110'})
            self.assertEqual(projected['ownedPositions']['member'],'3');self.assertEqual(projected['grossCash'],'9700')
            self.assertEqual(projected['costReserveOutstanding'],'3');self.assertEqual(projected['ownedCash'],'9697')
            self.assertFalse(projected['actualFeesReconciled'])
        for field,value in (('ord_dt','20261014'),('ord_gno_brno','9'),('odno','99'),('tr_crcy_cd','KRW'),('ovrs_excg_cd','NYSE'),('nccs_qty','1')):
            with self.subTest(field=field),self.assertRaises(ValueError):ledger.terminal_receipt(scope,spec,dispatch,{**official,field:value})

    def test_preparation_status_has_no_broker_calls_or_authority(self):
        spec,contract,context=fixture();permissions={'kisEquityExecutionPolicies':{spec.artifact_hash:context['executionPolicy']},'kisEquityFeeReservePolicy':context['feeReservePolicy']}
        status=runtime_module.preparation_status({'permissions':permissions},[spec],SETTINGS)
        self.assertFalse(status['available'],status);self.assertTrue(status['localPoliciesPrepared'])
        self.assertEqual(status['status'],'BLOCKED_BY_PRODUCTION_AUTHORITY')
        self.assertFalse(status['orderAuthorityGranted']);self.assertEqual(status['brokerCalls'],0)

    def _identity_fixture(self):
        spec,contract,context=fixture();ticket=quantity(spec,context)['modelResult']
        boundary={**context['exactOpenBoundary'],'startTime':'2026-10-16T01:30:00+00:00'}
        ticket=service.seal({**ticket,'boundaryHash':service.stable_sha256(boundary)})
        metadata={'deployment_id':'deployment','strategy_instance_id':'member','source_artifact_hash':'c'*64,'portfolio_hash':'e'*64,
            'native_account_fingerprint':ACCOUNT['accountFingerprint'],'native_quantity_model':ticket,'native_open_boundary':boundary}
        dispatch={'broker_id':'kis','metadata':metadata,'order_date':'','organization_no':'1','broker_order_id':'2','side':'BUY','symbol':'AAPL',
            'order_id':'order-1','idempotency_key':'dispatch-1','state':'accepted','order_date_source':'AWAITING_BROKER_TRADING_DATE',
            'broker_order_identity_verified':False,'response':{'fixture':'unchanged broker acknowledgement'}}
        official={'ord_dt':'20261015','ord_gno_brno':'1','odno':'2','pdno':'AAPL','sll_buy_dvsn_cd':'02','ovrs_excg_cd':'NASD','tr_crcy_cd':'USD',
            'ft_ord_qty':'3','ft_ccld_qty':'0','nccs_qty':'3','ft_ccld_amt3':'0','prcs_stat_name':'미체결','ord_tmd':'103000'}
        return spec,context,dispatch,official

    def test_missing_ack_date_recovers_only_exact_official_identity_and_does_not_book_an_unfilled_order(self):
        spec,context,dispatch,official=self._identity_fixture()
        recovered=ledger.resolve_official_order_identity(spec,dispatch,[official],request_session_date='20261015')
        self.assertEqual(recovered['order_date'],'20261015');self.assertEqual(recovered['broker_order_key'],'20261015:1:2')
        self.assertTrue(recovered['broker_order_identity_verified']);self.assertFalse(recovered['orderAuthorityGranted'])
        self.assertEqual(dispatch['order_date'],'')
        with self.assertRaises(ValueError):ledger.terminal_receipt(self._scope(),spec,{**dispatch,**recovered},official)

    def test_identity_recovery_never_guesses_date_branch_order_venue_or_quantity(self):
        spec,context,dispatch,official=self._identity_fixture()
        for field,value in (('ord_dt','20261014'),('ord_gno_brno','9'),('odno','99'),('pdno','MSFT'),('sll_buy_dvsn_cd','01'),
                            ('ovrs_excg_cd','NYSE'),('tr_crcy_cd','KRW'),('ft_ord_qty','4')):
            with self.subTest(field=field),self.assertRaises(ValueError):
                ledger.resolve_official_order_identity(spec,dispatch,[{**official,field:value}],request_session_date='20261015')
        for rows in ([],[official,official]):
            with self.assertRaises(ValueError):ledger.resolve_official_order_identity(spec,dispatch,rows,request_session_date='20261015')
        for day in ('','20261016','2026-10-15','20261399'):
            with self.assertRaises(ValueError):ledger.resolve_official_order_identity(spec,dispatch,[official],request_session_date=day)
        with self.assertRaises(ValueError):
            ledger.resolve_official_order_identity(spec,{**dispatch,'organization_no':''},[official],request_session_date='20261015')

    def test_identity_query_date_uses_sealed_new_york_event_instead_of_kst_submission_date(self):
        spec,context,dispatch,official=self._identity_fixture()
        # The immutable event is 2026-10-16 in Seoul and 2026-10-15 in New York.
        self.assertEqual(ledger.request_session_date_from_dispatch(spec,dispatch),'20261015')
        with self.assertRaises(ValueError):
            ledger.resolve_official_order_identity(spec,dispatch,[official],request_session_date='20261016')
        tampered=deepcopy(dispatch);tampered['metadata']['native_open_boundary']['startTime']='2026-10-17T01:30:00+00:00'
        with self.assertRaises(ValueError):ledger.request_session_date_from_dispatch(spec,tampered)

    def test_local_identity_cas_preserves_state_broker_payload_and_exact_replay_has_one_append_only_audit(self):
        spec,context,dispatch,official=self._identity_fixture()
        with tempfile.TemporaryDirectory() as folder:
            book=program_ledger_module.ProgramLedger(Path(folder)/'program.sqlite3');book.checkpoint_order_dispatch(dispatch)
            current=book.order_dispatch_for_idempotency_key('dispatch-1')
            identity=ledger.resolve_official_order_identity(spec,current,[official],request_session_date='20261015')
            result=book.record_verified_kis_order_identity('dispatch-1',service.stable_sha256(current),identity)
            self.assertTrue(result['applied']);self.assertFalse(result['orderAuthorityGranted'])
            saved=book.order_dispatch_for_idempotency_key('dispatch-1')
            changed={'order_date','broker_order_key','broker_order_identity_verified','order_date_source','broker_order_identity_receipt_hash'}
            self.assertEqual({k:v for k,v in saved.items() if k not in changed},{k:v for k,v in current.items() if k not in changed})
            self.assertEqual(saved['broker_order_key'],'20261015:1:2')
            replay=ledger.resolve_official_order_identity(spec,saved,[official],request_session_date='20261015')
            self.assertFalse(book.record_verified_kis_order_identity('dispatch-1',service.stable_sha256(saved),replay)['applied'])
            with book.connection() as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM kis_order_identity_verifications').fetchone()[0],1)
                with self.assertRaises(sqlite3.IntegrityError):conn.execute('DELETE FROM kis_order_identity_verifications')
            with self.assertRaises(ValueError):book.record_verified_kis_order_identity('dispatch-1',service.stable_sha256(current),identity)

    def test_local_identity_cas_rejects_race_unverified_receipt_and_conflicting_verified_date(self):
        spec,context,dispatch,official=self._identity_fixture()
        with tempfile.TemporaryDirectory() as folder:
            book=program_ledger_module.ProgramLedger(Path(folder)/'program.sqlite3');book.checkpoint_order_dispatch(dispatch)
            current=book.order_dispatch_for_idempotency_key('dispatch-1')
            identity=ledger.resolve_official_order_identity(spec,current,[official],request_session_date='20261015')
            book.update_order_dispatch({**current,'state':'changed-concurrently'})
            with self.assertRaisesRegex(ValueError,'STALE_DISPATCH_CAS'):
                book.record_verified_kis_order_identity('dispatch-1',service.stable_sha256(current),identity)
            raced=book.order_dispatch_for_idempotency_key('dispatch-1');identity={**identity,'verifiedDispatchHash':service.stable_sha256(raced)}
            with self.assertRaises(ValueError):book.record_verified_kis_order_identity('dispatch-1',service.stable_sha256(raced),{**identity,'officialReceiptHash':''})
            conflicting={**raced,'broker_order_identity_verified':True,'order_date':'20261014','broker_order_key':'20261014:1:2','order_date_source':'BROKER_ACK'}
            book.update_order_dispatch(conflicting);identity={**identity,'verifiedDispatchHash':service.stable_sha256(conflicting)}
            with self.assertRaisesRegex(ValueError,'KEY_CONFLICT'):
                book.record_verified_kis_order_identity('dispatch-1',service.stable_sha256(conflicting),identity)
            with book.connection() as conn:self.assertEqual(conn.execute('SELECT COUNT(*) FROM kis_order_identity_verifications').fetchone()[0],0)

    def test_runtime_recovers_missing_us_date_through_official_get_and_durable_cas_before_owned_fill(self):
        spec,context,dispatch,official=self._identity_fixture()
        official={**official,'ft_ccld_qty':'3','nccs_qty':'0','ft_ccld_amt3':'300','prcs_stat_name':'체결'}
        with tempfile.TemporaryDirectory() as folder:
            book=program_ledger_module.ProgramLedger(Path(folder)/'program.sqlite3');book.checkpoint_order_dispatch(dispatch)
            runtime=runtime_module.NativeEquityRuntime(folder,'deployment',[spec],reader=FakeReader(),
                dispatch_reader=lambda:book.order_dispatch_rows(),dispatch_identity_writer=book.record_verified_kis_order_identity,
                dispatch_lookup=book.order_dispatch_for_idempotency_key)
            calls=[]
            def official_get(exchange,day):calls.append((exchange,day));return [official]
            runtime.observer.terminal_orders=official_get;scope=self._scope();runtime.store.initialize(scope,self._observation(),now=NOW)
            self.assertEqual(runtime.sync_owned_fills(scope),1)
            self.assertTrue(all(item==('NASD','20261015') for item in calls))
            self.assertTrue(book.order_dispatch_for_idempotency_key('dispatch-1')['broker_order_identity_verified'])
            self.assertEqual(runtime.store.project(scope,{'member':'100'})['ownedPositions']['member'],'3')

    def test_terminal_receipts_use_numeric_order_number_after_official_order_time(self):
        spec,contract,context=fixture();buy=quantity(spec,context)['modelResult']
        sell_spec,sell_contract,sell_context=fixture('NASD','SELL')
        sell=service.live_equity_quantity(sell_spec,side='SELL',price=100,allocation_weight=1,
            identity={'deploymentId':'deployment'},context=sell_context,position_quantity=3,now=NOW)['modelResult']
        def dispatch(number,side,ticket):
            return {'broker_id':'kis','side':side,'order_date':'20261015','organization_no':'1','broker_order_id':number,
                'metadata':{'deployment_id':'deployment','strategy_instance_id':'member','source_artifact_hash':'c'*64,'portfolio_hash':'e'*64,
                    'native_account_fingerprint':ACCOUNT['accountFingerprint'],'native_quantity_model':ticket}}
        def official(number,side):
            return {'ord_dt':'20261015','ord_gno_brno':'1','odno':number,'pdno':'AAPL','sll_buy_dvsn_cd':'02' if side=='BUY' else '01',
                'ovrs_excg_cd':'NASD','tr_crcy_cd':'USD','ft_ord_qty':'3','ft_ccld_qty':'3','nccs_qty':'0','ft_ccld_amt3':'300',
                'prcs_stat_name':'체결','ord_tmd':'103000'}
        # Lexical order 10 then 2 would create a false owned short position.
        rows=[dispatch('10','SELL',sell),dispatch('2','BUY',buy)]
        with tempfile.TemporaryDirectory() as folder:
            runtime=runtime_module.NativeEquityRuntime(folder,'deployment',[spec],reader=FakeReader(),dispatch_reader=lambda:rows)
            scope=self._scope();runtime.store.initialize(scope,self._observation(),now=NOW)
            runtime.observer.terminal_orders=lambda *args:[official('10','SELL'),official('2','BUY')]
            self.assertEqual(runtime.sync_owned_fills(scope),2)
            projection=runtime.store.project(scope,{})
            self.assertEqual(projection['ownedPositions']['member'],'0');self.assertEqual(projection['fillCount'],2)

    def test_repeated_or_unknown_continuation_does_not_publish_partial_account_scope(self):
        fake=FakeReader();observer=reader_module.KisEquityReader(fake,clock=lambda:NOW)
        for header in ('M','unrecognized'):
            def get(provider,path,*,query,headers):
                return {'rt_cd':'0','output1':[],'ctx_area_fk200':'repeated','ctx_area_nk200':'cursor'},{'tr_cont':header}
            fake._get=get
            with self.assertRaises(reader_module.ReadFailure):observer.account_scope('NASD','AAPL')

    def test_open_working_order_is_explicit_and_cannot_be_called_none(self):
        spec,contract,context=fixture();fake=FakeReader()
        def mutate(path,out):
            if path.endswith('/inquire-nccs'):return {**out,'output':[{'odno':'other-user-order'}]}
            return out
        fake.mutate=mutate
        observed=reader_module.KisEquityReader(fake,clock=lambda:NOW).observe(contract,side='BUY',raw_open=100,protective_bps=50)
        self.assertEqual(observed['openOrders'],'PRESENT')
        context['nativeMarketObservation']=observed
        self.assertEqual(quantity(spec,context)['status'],'BLOCKED')

    def test_reader_rejects_stale_multi_request_receipt_and_foreign_fractional_cap(self):
        spec,contract,context=fixture();fake=FakeReader();times=iter((NOW,NOW+timedelta(seconds=16)))
        with self.assertRaises(reader_module.ReadFailure):
            reader_module.KisEquityReader(fake,clock=lambda:next(times)).observe(contract,side='BUY',raw_open=100,protective_bps=50)
        fake=FakeReader()
        fake.mutate=lambda path,out:{**out,'output':{**out['output'],'ord_psbl_qty':'1.5'}} if path.endswith('/inquire-psamount') else out
        with self.assertRaises(reader_module.ReadFailure):
            reader_module.KisEquityReader(fake,clock=lambda:NOW).observe(contract,side='BUY',raw_open=100,protective_bps=50)

    def test_runtime_init_context_and_position_are_canonical_without_starting_a_worker(self):
        spec,contract,context=fixture();scope_permissions={'liveReviewBindingHash':'f'*64,'liveAccountProfileId':ACCOUNT['profileId'],
            'liveAccountFingerprint':ACCOUNT['accountFingerprint'],'liveAccountCredentialConfigurationHash':ACCOUNT['credentialConfigurationHash'],
            'liveCapitalPolicy':{'basis':'ACCOUNT_AVAILABLE_CASH','percent':100},
            'kisEquityExecutionPolicies':{spec.artifact_hash:context['executionPolicy']},'kisEquityFeeReservePolicy':context['feeReservePolicy']}
        deployment={'deploymentId':'deployment','environment':'LIVE','lifecycle':'before-live-small','accountId':ACCOUNT['accountBindingId'],
            'route':'kis','permissions':scope_permissions}
        with tempfile.TemporaryDirectory() as folder:
            runtime=runtime_module.NativeEquityRuntime(folder,'deployment',[spec],reader=FakeReader(),
                deployment_reader=lambda *args:deployment,dispatch_reader=lambda:[])
            initial=runtime.initialize_at_reviewed_manual_start()
            self.assertEqual(initial['ownedCash'],'9000');self.assertEqual(initial['ownedPositions']['member'],'0')
            current=runtime.context(spec,'BUY',100,[spec],context['exactOpenBoundary'])
            self.assertEqual(current['equityScope'],'OWNED_DEPLOYMENT_ONLY',current)
            self.assertEqual(runtime.context(spec,'POSITION',None,[spec],None)['ownedPositions']['member'],'0')
            self.assertFalse(hasattr(runtime,'start'));self.assertFalse(hasattr(runtime,'place_order'))


if __name__=='__main__':unittest.main()
