import ast
import copy
import importlib.util
import math
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('_spot_display_valuation_test', ROOT / 'live_trader/spot_account_valuation.py')
valuation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(valuation)
NOW = 1793150000.0


def position(provider='binance', symbol='SOL', **extra):
    return dict(broker_id=provider, symbol=symbol, broker_qty=2.0, currency='SOL' if provider == 'binance' else 'KRW',
                current_price=0.0, broker_value=12.0, average_price=6.0, valuation_basis='cost_basis', **extra)


def enrich(rows, payload, provider='binance', **kwargs):
    reader = Mock(return_value={'ok': True, 'json': payload})
    result = valuation.enrich_spot_display_valuation(provider, rows, reader,
        configured_origin=valuation.PUBLIC_ORIGINS.get(provider, ''),
        wall_clock=lambda: NOW, monotonic=lambda: 10.0, **kwargs)
    return result, reader


class SpotDisplayValuationTests(unittest.TestCase):
    def test_binance_quote_is_display_only_and_preserves_original_financial_fields(self):
        rows=[position()]; before=copy.deepcopy(rows)
        result,reader=enrich(rows,[{'symbol':'SOLUSDT','price':'120.5'}])
        quote=result[0].pop('display_valuation')
        self.assertEqual(before,rows); self.assertEqual(before,[result[0]])
        self.assertEqual(241.0,quote['value']); self.assertEqual('USDT',quote['currency'])
        self.assertFalse(quote['order_authority_granted'])
        self.assertEqual('http_receipt',quote['timestamp_basis'])
        reader.assert_called_once_with('https://api.binance.com/api/v3/ticker/price')

    def test_upbit_krw_quote_does_not_relabel_a_btc_cost_basis(self):
        row=position('upbit','BTC-SOL'); row['currency']='BTC'; row['average_price']=0.001
        result,reader=enrich([row],[{'market':'KRW-SOL','trade_price':170000,'timestamp':NOW*1000}], 'upbit')
        self.assertEqual('BTC',result[0]['currency']); self.assertEqual(0.001,result[0]['average_price'])
        quote=result[0]['display_valuation']; self.assertEqual(340000,quote['value']); self.assertEqual('KRW',quote['currency'])
        self.assertEqual('exchange_snapshot',quote['timestamp_basis'])
        reader.assert_called_once_with('https://api.upbit.com/v1/ticker/all?quote_currencies=KRW')

    def test_missing_market_keeps_unknown_without_poisoning_other_holdings(self):
        result,_=enrich([position(),position(symbol='DELISTED')],[{'symbol':'SOLUSDT','price':'120'}])
        self.assertEqual(['OBSERVED','UNKNOWN'],[r['display_valuation']['status'] for r in result])
        self.assertIsNone(result[1]['display_valuation']['value'])

    def test_duplicate_nonfinite_wrong_pair_and_nonpositive_quotes_remain_unknown(self):
        for rows in ([{'symbol':'SOLBTC','price':'10'}], [{'symbol':'SOLUSDT','price':v} for v in ('1','2')],
                     *[[{'symbol':'SOLUSDT','price':v}] for v in ('NaN','Infinity','0','-1',True,None)]):
            with self.subTest(rows=rows):
                result,_=enrich([position()],rows)
                self.assertEqual('UNKNOWN',result[0]['display_valuation']['status'])

    def test_upbit_stale_future_missing_snapshot_timestamp_stays_unknown(self):
        for timestamp in (None,(NOW-61)*1000,(NOW+6)*1000):
            result,_=enrich([position('upbit','KRW-SOL')],[{'market':'KRW-SOL','trade_price':1,'timestamp':timestamp}], 'upbit')
            self.assertEqual('UNKNOWN',result[0]['display_valuation']['status'])

    def test_timeout_and_transport_errors_never_hide_balance_or_expose_exception(self):
        reader=Mock(side_effect=TimeoutError('PRIVATE_TOKEN_MUST_NOT_ESCAPE'))
        result=valuation.enrich_spot_display_valuation('binance',[position()],reader,
            configured_origin=valuation.PUBLIC_ORIGINS['binance'])
        self.assertEqual(2,result[0]['broker_qty']); self.assertEqual('UNKNOWN',result[0]['display_valuation']['status'])
        self.assertNotIn('PRIVATE_TOKEN',str(result)); self.assertEqual(1,reader.call_count)

    def test_failed_malformed_or_too_slow_response_is_not_a_quote(self):
        for response in ({'ok':False,'json':[]},{'ok':True,'json':{}},{'ok':'true','json':[]}):
            result=valuation.enrich_spot_display_valuation('binance',[position()],lambda _:response,
                configured_origin=valuation.PUBLIC_ORIGINS['binance'])
            self.assertEqual('UNKNOWN',result[0]['display_valuation']['status'])
        mono=iter([0,6]); wall=iter([NOW,NOW+6])
        result=valuation.enrich_spot_display_valuation('binance',[position()],lambda _:{'ok':True,'json':[{'symbol':'SOLUSDT','price':1}]},
            configured_origin=valuation.PUBLIC_ORIGINS['binance'],monotonic=lambda:next(mono),wall_clock=lambda:next(wall))
        self.assertEqual('UNKNOWN',result[0]['display_valuation']['status'])

    def test_wrong_origin_and_empty_holdings_never_send_request(self):
        reader=Mock()
        for origin,rows in (('https://testnet.binance.vision',[position()]),('https://api.binance.com',[])):
            valuation.enrich_spot_display_valuation('binance',rows,reader,configured_origin=origin)
        reader.assert_not_called()

    def test_futures_does_not_use_spot_quotes_or_change_exposure(self):
        rows=[position('binance-futures','SOLUSDT')]; reader=Mock()
        result=valuation.enrich_spot_display_valuation('binance-futures',rows,reader,configured_origin='https://fapi.binance.com')
        self.assertEqual(rows,result); reader.assert_not_called()


class RouterDisplayIntegrationTests(unittest.TestCase):
    """Execute exact production parser/poll methods with only transport substituted.

    AST extraction intentionally avoids importing app __init__, credential loading,
    migrations, DBs or financial workers during this offline regression.
    """
    def setUp(self):
        tree=ast.parse((ROOT/'live_trader/brokers.py').read_text(encoding='utf-8'))
        functions={'numeric_value','first_numeric','parse_binance_accounts','parse_binance_positions','parse_upbit_accounts','parse_upbit_positions'}
        selected=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in functions]
        router=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='LiveBrokerRouter')
        router.body=[n for n in router.body if isinstance(n,ast.FunctionDef) and n.name=='poll_execution_events']
        selected.append(router)
        self.ns={'math':math,'now_datetime_text':lambda:'fixture',
                 'ensure_response_ok':lambda broker,response:response['json'],
                 'broker_snapshot_events':lambda accounts,positions:[],
                 'build_binance_account_request':lambda:'signed-account',
                 'build_upbit_accounts_request':lambda:'upbit-account'}
        exec(compile(ast.Module(body=selected,type_ignores=[]),'<production broker AST>','exec'),self.ns)
        self.router=self.ns['LiveBrokerRouter']()
        self.router._spot_display_valuation=lambda broker,rows:enrich(rows,
            [{'symbol':'SOLUSDT','price':120},{'symbol':'USDCUSDT','price':0.99}] if broker=='binance' else
            [{'market':'KRW-SOL','trade_price':170000,'timestamp':NOW*1000}],broker)[0]

    def test_actual_binance_poll_keeps_cash_and_quantities_with_non_usdt_stablecoin_valuation(self):
        payload={'balances':[{'asset':'USDT','free':'10','locked':'2'},{'asset':'SOL','free':'1','locked':'1'},{'asset':'USDC','free':'4','locked':'0'}]}
        send=Mock(return_value={'json':payload}); self.ns['send_binance_signed_request']=send
        result=self.router.poll_execution_events('binance')
        self.assertEqual(12,result['accounts'][0]['broker_cash']); self.assertEqual(12,result['accounts'][0]['broker_equity'])
        self.assertEqual('cash_only',result['accounts'][0]['valuation_basis'])
        self.assertEqual(['SOL','USDC'],[r['symbol'] for r in result['positions']])
        self.assertEqual([240,3.96],[r['display_valuation']['value'] for r in result['positions']])
        self.assertEqual(['SOL'],[r['symbol'] for r in self.ns['parse_binance_positions'](payload)])
        send.assert_called_once()

    def test_actual_upbit_poll_preserves_original_cost_in_financial_event(self):
        payload=[{'currency':'KRW','balance':'100','locked':'20'}, {'currency':'SOL','unit_currency':'KRW','balance':'1','locked':'1','avg_buy_price':'100000'}]
        send=Mock(return_value={'json':payload}); self.ns['send_prepared_request']=send
        result=self.router.poll_execution_events('upbit'); row=result['positions'][0]
        self.assertEqual(120,result['accounts'][0]['broker_cash']); self.assertEqual(200000,row['broker_value'])
        self.assertEqual(0,row['current_price']); self.assertEqual('cost_basis',row['valuation_basis'])
        self.assertEqual(340000,row['display_valuation']['value']); send.assert_called_once()



class DisplayProjectionBoundaryTests(unittest.TestCase):
    def test_actual_public_reader_has_no_credentials_no_mutation_and_bounded_timeout(self):
        tree=ast.parse((ROOT/'live_trader/brokers.py').read_text(encoding='utf-8'))
        router=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='LiveBrokerRouter')
        method=next(n for n in router.body if isinstance(n,ast.FunctionDef) and n.name=='_spot_display_valuation')
        method.decorator_list=[]
        alias='_display_transport_fixture'
        package=types.ModuleType(alias); package.__path__=[]
        before={key:sys.modules.get(key) for key in (alias,alias+'.spot_account_valuation')}
        sys.modules[alias]=package; sys.modules[alias+'.spot_account_valuation']=valuation
        try:
            request=Mock(return_value={'ok':True,'json':[{'symbol':'SOLUSDT','price':100}]})
            ns={'__package__':alias,'http_json':request,'os':types.SimpleNamespace(getenv=lambda *args:'')}
            exec(compile(ast.Module(body=[method],type_ignores=[]),'<production public transport>','exec'),ns)
            result=ns['_spot_display_valuation']('binance',[position()])
            self.assertEqual('OBSERVED',result[0]['display_valuation']['status'])
            request.assert_called_once_with('GET','https://api.binance.com/api/v3/ticker/price',body=None,headers={},timeout_seconds=3.0)
        finally:
            for key,value in before.items():
                if value is None: sys.modules.pop(key,None)
                else: sys.modules[key]=value

    def test_actual_state_projection_keeps_quote_on_matched_and_external_positions(self):
        tree=ast.parse((ROOT/'live_trader/state.py').read_text(encoding='utf-8'))
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='positions')
        quote=enrich([position()],[{'symbol':'SOLUSDT','price':120}])[0][0]
        key=('binance','SOL','')
        for has_ledger in (False,True):
            with self.subTest(has_ledger=has_ledger):
                ns={'live_position_rows':lambda:{key:copy.deepcopy(quote)},
                    'program_position_rows':lambda:{key:dict(symbol='SOL',broker_id='binance',currency='SOL',quantity=2)} if has_ledger else {},
                    'broker_reconciliation_errors':lambda:{},'successful_position_brokers':lambda:{'binance'},
                    'normalized_reconciliation_position_side':lambda *args,**kwargs:'',
                    'format_quantity':str,'format_money':lambda amount,currency:str(amount),
                    'safe_float':lambda value,default=0:float(value) if value is not None else default}
                exec(compile(ast.Module(body=[function],type_ignores=[]),'<production position projection>','exec'),ns)
                result=ns['positions']()[0]
                self.assertEqual(240,result['display_valuation']['value'])
                self.assertEqual(12,result['broker_value']); self.assertEqual(0,result['current_price'])
                self.assertEqual('SOL',result['currency'])

if __name__=='__main__':
    unittest.main()
