"""Normal read-only Source->state wrapper->reader flow, fake GET only."""
import ast,copy,importlib,json,tempfile,types,unittest
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock,patch
import test_paper_candidate_inbox as base
from trading_runtime.artifact_governance import seal_strategy_artifact,seal_strategy_instance,seal_portfolio_artifact,artifact_reference,assert_verified_strategy_instance
service=importlib.import_module(base.PACKAGE+'.read_only_preparation')
account=importlib.import_module(base.PACKAGE+'.read_only_account')
margin=importlib.import_module(base.PACKAGE+'.kis_margin_observation')
usd=importlib.import_module(base.PACKAGE+'.kis_usd_preparation')

def settings():return {'KIS_APP_KEY':'private-fixture-key','KIS_APP_SECRET':'private-fixture-secret','KIS_ACCOUNT_NO':'12345678-01','KIS_ACCOUNT_PRODUCT_CODE':'01','KIS_ENV':'real'}
def raw_row(currency='USD'):return {'crcy_cd':currency,'natn_name':'private-country',**{k:'12345.6789' for k in margin.ROLE_FIELDS}}
def make_source(root,*,symbol='AAPL',broker='kis'):
    flags={'evidenceClass':'FUNCTIONAL_TEST_NON_PROMOTION','promotionEligible':False,'useAsPromotionEvidence':False,'backtester_verified':True,'permissions':{'live_allowed':False,'live_small_eligible':False,'live_eligible':False}}
    raw={**copy.deepcopy(flags),'id':'usd-test-source','artifact_schema_version':'strategy-artifact-v1','symbol':symbol,'timeframe':'5m','plugin':'moving_average_cross','parameters':{'shortMa':3,'longMa':5},'traderContract':{'canPlaceOrders':False},'sample_prices':[100+i%20 for i in range(260)],'dataArtifact':{'closedBarProvenance':{'confirmed':True,'interval':'5m','finalBarEnd':'2026-08-03T18:20:00Z','datasetUpdatedAt':'2026-08-03T18:30:00Z'}}}
    source=seal_strategy_artifact(raw);ref=artifact_reference(source)
    instance=seal_strategy_instance({'instanceId':'usd-instance','sourceStrategyId':ref['artifactId'],'sourceArtifactHash':ref['artifactHash'],'symbol':symbol,'timeframe':'5m','pluginId':'moving_average_cross','parameters':raw['parameters'],'marketDataProvider':broker,'brokerId':broker})
    ih=assert_verified_strategy_instance(instance)
    portfolio=seal_portfolio_artifact({**copy.deepcopy(flags),'id':'usd-portfolio','strategyInstances':[{**instance,'sourceInstanceHash':ih}]})
    base.write(root/'strategy.json',source);base.write(root/'strategy-instances/instance.json',instance);base.write(root/'portfolios/portfolio.json',portfolio)

class UsdPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        make_source(self.root)
        self.request={'source':service.list_preparation_sources(roots=[self.root])[0]['request'],'draft':{'instanceId':'usd-instance','side':'BUY','quantity':'1','limitPrice':'10'},'readAccount':True}
        self.transport=Mock();self.transport.request.return_value=({'rt_cd':'0','output':[raw_row(),raw_row(),raw_row(),{'crcy_cd':''}]},{'tr_id':'TTTC2101R','tr_cont':''})
        self.reader=account.ReadOnlyBrokerReader(settings(),transport=self.transport)
        self.reader._kis_token=Mock(return_value='private-bearer');self.reader._pace_kis=Mock()
        self.reader._kis=Mock(return_value={'account':'AVAILABLE','openOrders':'NONE_OBSERVED','orderability':'UNKNOWN','_quote':Decimal('55'),'_base':Decimal('2')})
    def run_preview(self):return service.prepare_read_only(self.request,roots=[self.root],reader=self.reader)
    def diagnostic(self):return self.reader.observe_kis_usd_financial_preparation()
    def test_normal_source_and_read_flow_adds_one_get_only(self):
        before=base.file_bytes(self.root);report=self.run_preview();self.assertTrue(report['ok'])
        self.assertEqual(base.file_bytes(self.root),before)
        self.transport.request.assert_called_once()
        call=self.transport.request.call_args;self.assertEqual(call.args,('kis','GET',margin.PATH))
        self.assertEqual(call.kwargs['query'],{'CANO':'12345678','ACNT_PRDT_CD':'01'})
        self.assertEqual(call.kwargs['headers']['tr_id'],'TTTC2101R')
        d=report['observation']['usdCashDiagnostics'];self.assertEqual(d['observation']['usdRowCount'],3)
        self.assertIn('KIS_USD_MULTIPLE_ROWS_UNMERGED',d['reasonCodes']);self.assertIn('KIS_USD_PAGINATION_UNCONFIRMED',d['reasonCodes'])
        for k in service.FLAGS:self.assertEqual(report[k],service.FLAGS[k])
        for k in usd.FLAGS:self.assertIs(d[k],False)
    def test_main_state_entry_function_uses_actual_normal_preparation(self):
        tree=ast.parse((base.APP_ROOT/'live_trader/state.py').read_text(encoding='utf-8'))
        fn=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='read_only_order_preparation')
        ns={'__package__':base.PACKAGE,'Any':object};exec(compile(ast.Module(body=[fn],type_ignores=[]),'<state-readonly-entry>','exec'),ns)
        with patch.object(service,'preparation_roots',return_value=[self.root]),patch.object(service,'runtime_settings',return_value=settings()),patch.object(service,'ReadOnlyBrokerReader',return_value=self.reader):
            report=ns['read_only_order_preparation'](self.request)
        self.assertTrue(report['ok']);self.assertIn('usdCashDiagnostics',report['observation']);self.transport.request.assert_called_once()
        self.assertFalse(report['authorityGranted']);self.assertEqual(report['runtimeSessionsCreated'],0)
    def test_read_account_false_makes_zero_usd_and_normal_reads(self):
        self.request['readAccount']=False;report=self.run_preview()
        self.transport.request.assert_not_called();self.reader._kis.assert_not_called();self.assertNotIn('usdCashDiagnostics',report['observation'])
    def test_kr_selection_does_not_call_usd(self):
        make_source(self.root,symbol='005930');self.request['source']=service.list_preparation_sources(roots=[self.root])[0]['request']
        report=self.run_preview();self.transport.request.assert_not_called();self.assertNotIn('usdCashDiagnostics',report['observation'])
    def test_no_selected_instance_fails_before_any_read(self):
        self.request['draft']['instanceId']='wrong'
        with self.assertRaises(ValueError):self.run_preview()
        self.transport.request.assert_not_called();self.reader._kis.assert_not_called()
    def test_old_funds_comparison_is_not_replaced_or_aggregated(self):
        self.reader.settings['KIS_OVERSEAS_FUNDING_MODE']='FOREIGN_CASH'
        report=self.run_preview();self.assertEqual(report['observation']['fundsCheck'],'WITHIN_OBSERVED_BALANCE')
        self.assertNotIn('availableCash',report['observation']['usdCashDiagnostics'])
        self.assertEqual(self.reader._kis.call_args.args,('kis-us','AAPL'))
        self.assertIn('KIS_USD_DEPOSIT_SCOPE_UNPROVEN',[x['code'] for x in report['checks']])
    def test_integrated_default_discards_cash_fallback_and_does_not_grant_financial_authority(self):
        report=self.run_preview();observation=report['observation']
        self.assertEqual(observation['fundsCheck'],'UNKNOWN')
        self.assertIsNone(observation['availableCash'])
        self.assertEqual(observation['fundingDiagnostics']['fundingMode'],'INTEGRATED_MARGIN')
        check=next(x for x in report['checks'] if x['code']=='KIS_OVERSEAS_FUNDING')
        self.assertEqual(check['status'],'BLOCKED')
        self.assertFalse(report['authorityGranted'])
        self.assertFalse(observation['usdCashDiagnostics']['productionReady'])
    def test_funding_choice_change_during_usd_read_discards_report(self):
        def mutate(*args,**kwargs):
            self.reader.settings['KIS_OVERSEAS_FUNDING_MODE']='FOREIGN_CASH'
            return ({'rt_cd':'0','output':[]},{'tr_id':'TTTC2101R','tr_cont':'D'})
        self.transport.request.side_effect=mutate
        with self.assertRaisesRegex(ValueError,'설정'):self.run_preview()
    def test_actual_fee_and_posting_always_blocked(self):
        report=self.run_preview();checks={x['code']:x for x in report['checks']}
        for code in usd.BASE_REASONS:self.assertEqual(checks[code]['status'],'BLOCKED')
        self.assertFalse(report['observation']['usdCashDiagnostics']['accountFeeRateApproved'])
    def test_currency_padding_and_missing_usd_never_become_zero(self):
        for rows,code in (([], 'KIS_USD_ROW_ABSENT_NOT_ZERO'),([{'crcy_cd':''}], 'KIS_USD_CURRENCY_UNKNOWN')):
            self.transport.request.return_value=({'rt_cd':'0','output':rows},{'tr_id':'TTTC2101R','tr_cont':'D'})
            d=self.diagnostic();self.assertIn(code,d['reasonCodes']);self.assertFalse(d['usdCashReconciled'])
    def test_complete_collection_does_not_unlock_financial_checks(self):
        self.transport.request.return_value=({'rt_cd':'0','output':[raw_row()]},{'tr_id':'TTTC2101R','tr_cont':'D'})
        d=self.diagnostic();self.assertNotIn('KIS_USD_PAGINATION_UNCONFIRMED',d['reasonCodes'])
        self.assertEqual(d['reasonCodes'],list(usd.BASE_REASONS));self.assertFalse(d['productionReady'])
    def test_partial_observation_never_follows_next_page(self):
        self.transport.request.return_value=({'rt_cd':'0','output':[raw_row()]},{'tr_id':'TTTC2101R','tr_cont':'M'})
        d=self.diagnostic();self.transport.request.assert_called_once();self.assertIn('KIS_USD_PAGINATION_UNCONFIRMED',d['reasonCodes'])
    def test_transport_exception_safe_and_no_retry(self):
        self.transport.request.side_effect=RuntimeError('private-remote-text-12345678')
        d=self.diagnostic();self.transport.request.assert_called_once();self.assertEqual(d['status'],'UNAVAILABLE')
        self.assertNotIn('private-remote',json.dumps(d))
    def test_demo_and_custom_origin_make_zero_get(self):
        for key,value in (('KIS_ENV','demo'),('KIS_BASE_URL','https://evil.invalid')):
            self.reader.settings=settings();self.reader.settings[key]=value
            self.assertEqual(self.diagnostic()['status'],'UNAVAILABLE');self.transport.request.assert_not_called()
    def test_raw_account_token_country_and_amount_never_escape(self):
        report=self.run_preview();text=json.dumps(report)
        for secret in ('private-fixture-key','private-fixture-secret','private-bearer','12345678','12345.6789','private-country','frcr_dncl_amt1": "12345'):
            self.assertNotIn(secret,text)
    def test_saved_settings_change_during_usd_read_discards_report(self):
        def mutate(*a,**kw):self.reader.settings['KIS_APP_SECRET']='changed';return ({'rt_cd':'0','output':[]},{'tr_id':'TTTC2101R','tr_cont':'D'})
        self.transport.request.side_effect=mutate
        with self.assertRaisesRegex(ValueError,'설정'):self.run_preview()
    def test_source_change_during_usd_read_discards_report(self):
        def mutate(*a,**kw):
            source=json.loads((self.root/'strategy.json').read_text(encoding='utf-8'));source['sample_prices'][0]+=1;base.write(self.root/'strategy.json',source)
            return ({'rt_cd':'0','output':[]},{'tr_id':'TTTC2101R','tr_cont':'D'})
        self.transport.request.side_effect=mutate
        self.assertFalse(service.preparation_response(self.request,roots=[self.root],reader=self.reader)['ok'])
    def test_forged_summary_extra_fields_or_positive_authority_rejected(self):
        d=self.diagnostic()
        for key,value in (('orderAuthorityGranted',True),('authorityGranted',0),('status','READY'),('privateResponse',{'account':'secret'}),('userMessages',['private-text'])):
            bad=copy.deepcopy(d);bad[key]=value
            with self.assertRaises(ValueError):usd.validate_usd_preparation_diagnostic(bad)
    def test_malformed_custom_reader_diagnostic_cannot_leak_or_unlock(self):
        self.reader.observe_kis_usd_financial_preparation=Mock(return_value={'rawAccount':'private-leak','orderAuthorityGranted':True})
        report=self.run_preview();d=report['observation']['usdCashDiagnostics']
        self.assertEqual(d['status'],'UNAVAILABLE');self.assertNotIn('private-leak',json.dumps(report));self.assertFalse(d['orderAuthorityGranted'])
    def test_summary_numeric_types_and_remote_field_names_are_not_accepted(self):
        summary=self.diagnostic()['observation']
        for key,value in (('usdRowCount',True),('recognizedFieldNames',['remote-secret-field']),('reportedRoleNames',{'private':'value'}),('contentHash','not-a-hash')):
            bad=copy.deepcopy(summary);bad[key]=value
            with self.assertRaises(ValueError):usd.validate_sanitized_margin_summary(bad)
    def test_margin_strict_decoder_rejects_numeric_overflow_in_any_field(self):
        for raw in (b'{"rt_cd":"0","output":[],"unknown":1e999}', b'{"rt_cd":"0","output":[],"unknown":-1e999}'):
            with self.assertRaises(ValueError):margin.decode_margin_response(raw)
        self.assertEqual(margin.decode_margin_response(b'{"rt_cd":"0","output":[],"unknown":"1e999"}')["unknown"],"1e999")
        self.assertEqual(margin.decode_margin_response(b'{"rt_cd":"0","output":[],"unknown":1e200}')["unknown"],1e200)
    def test_current_usd_native_gate_stays_blocked(self):
        gate=importlib.import_module(base.PACKAGE+'.kis_native_usd_reconciliation')
        for operation in ('START','RENEWAL','CLEAN'):
            self.assertEqual(gate.native_usd_reconciliation_status(operation=operation)['status'],'BLOCKED')
    def test_no_network_or_settings_read_on_module_import(self):
        self.assertNotIn(base.PACKAGE+'.state',__import__('sys').modules)
        self.assertNotIn(base.PACKAGE+'.continuous_live',__import__('sys').modules)

if __name__=='__main__':unittest.main()
