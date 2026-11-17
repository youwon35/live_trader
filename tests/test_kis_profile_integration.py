"""Offline session routing / actual native ticket pipeline, no transport."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import ast,importlib,os,tempfile,unittest
from unittest.mock import patch
import test_kis_equity_operational as fixture_module
from trading_runtime.stock_session_profile import normalize_stock_session_profile as profile,profile_hash,session_window_at
from trading_runtime.kis_session_routes import kis_order_route,validate_kis_session_evidence
from trading_runtime.continuous_runtime import ClosedBar,OpenBoundary
from live_trader.live_adapters import build_kis_live_order_request,build_kis_cancel_order_request
from live_trader.native_open_boundary import NativeBoundaryQueue,decision_payload
from live_trader import kis_equity_execution as native
UTC=timezone.utc
ENV={'KIS_APP_KEY':'SYNTHETIC','KIS_APP_SECRET':'SYNTHETIC','KIS_ACCOUNT_NO':'12345678','KIS_ACCOUNT_PRODUCT_CODE':'01','KIS_ENV':'real'}


def evidence(selected,phase,symbol,now,exchange='KRX'):
 return {'profileHash':profile_hash(selected),'symbol':symbol,'phase':phase,'observedAt':now.isoformat(),
  'sourceHash':'a'*64,'continuous':True,'orderable':True,'symbolEligible':True,
  'venue':selected['venue'] if selected['market']=='KR' else exchange,'instrumentType':'STOCK',
  'window':session_window_at(selected,now)}

class ProfileIntegrationTests(unittest.TestCase):
 def test_actual_live_builder_and_cancel_keep_original_nxt_pre_class(self):
  selected=profile('EXTENDED',market='KR',provider='kis');now=datetime(2026,11,17,23,tzinfo=UTC)
  p={'stockSessionProfile':selected,'stockSessionProfileHash':profile_hash(selected),'sessionPhase':'PRE',
   'symbol':'005930','asset':'KR','side':'BUY','quantity':1,'price':100,'exchange':'KRX','order_type':'00',
   'sessionEvidence':evidence(selected,'PRE','005930',now)}
  def fixed(*args,**kwargs):return validate_kis_session_evidence(*args,**{**kwargs,'now':now})
  with patch.dict(os.environ,ENV,clear=False),patch('trading_runtime.kis_session_routes.validate_kis_session_evidence',side_effect=fixed):
   request=build_kis_live_order_request(p,access_token='synthetic')
   self.assertTrue(request.can_send,request.blocked_reasons);self.assertEqual(request.body['ORD_DVSN'],'27');self.assertEqual(request.body['EXCG_ID_DVSN_CD'],'NXT')
   canceled=build_kis_cancel_order_request({**p,'organization_no':'001','broker_order_id':'2','sessionEvidence':None},access_token='synthetic')
   self.assertTrue(canceled.can_send,canceled.blocked_reasons);self.assertEqual(canceled.body['ORD_DVSN'],'27');self.assertEqual(canceled.body['EXCG_ID_DVSN_CD'],'NXT')
   p['sessionEvidence']={**p['sessionEvidence'],'venue':'KRX'}
   self.assertFalse(build_kis_live_order_request(p,access_token='synthetic').can_send)
 def test_actual_day_wire_and_cancel_are_separate_from_regular_us(self):
  selected=profile({'market':'US','mode':'EXTENDED','segments':['DAY']},provider='kis');now=datetime(2026,11,18,2,tzinfo=UTC)
  p={'stockSessionProfile':selected,'sessionPhase':'DAY','symbol':'AAPL','asset':'US','side':'SELL','quantity':2,'price':100,'exchange':'NASD','sessionEvidence':evidence(selected,'DAY','AAPL',now,'NASD')}
  def fixed(*args,**kwargs):return validate_kis_session_evidence(*args,**{**kwargs,'now':now})
  with patch.dict(os.environ,ENV,clear=False),patch('trading_runtime.kis_session_routes.validate_kis_session_evidence',side_effect=fixed):
   request=build_kis_live_order_request(p,access_token='synthetic');self.assertTrue(request.can_send,request.blocked_reasons)
   self.assertTrue(request.endpoint.endswith('/daytime-order'));self.assertEqual(request.headers['tr_id'],'TTTS6037U')
   canceled=build_kis_cancel_order_request({**p,'organization_no':'001','broker_order_id':'2','sessionEvidence':None},access_token='synthetic')
   self.assertTrue(canceled.endpoint.endswith('/daytime-order-rvsecncl'));self.assertEqual(canceled.headers['tr_id'],'TTTS6038U')
 def test_native_ticket_to_actual_state_payload_preserves_profile_proof_and_limit(self):
  now=datetime(2026,11,17,23,tzinfo=UTC);selected=profile('EXTENDED',market='KR',provider='kis')
  with patch.object(fixture_module,'NOW',now):
   spec,contract,context=fixture_module.fixture('KRX');spec.parameters['stockSessionProfile']=selected
   boundary=context['exactOpenBoundary'];boundary.update(session='pre',sessionProfileHash=profile_hash(selected),sourceVenue='NXT')
   observation=context['nativeMarketObservation'];observation.update(stockSessionProfile=selected,sessionPhase='PRE',sessionProfileHash=profile_hash(selected),executionVenue='NXT',orderClass='27',sessionEvidence=evidence(selected,'PRE',spec.symbol,now))
   context['nativeMarketObservation']=fixture_module.service.seal(observation)
   result=fixture_module.quantity(spec,context);self.assertEqual(result['status'],'READY',result)
   intent=fixture_module.intent(spec,context,result);intent.asset='KR';intent.notional=float(result['modelResult']['notional'])
   ticket=fixture_module.service.validate_ticket(intent,now=now,settings=fixture_module.SETTINGS)
   self.assertEqual(ticket['orderClass'],'27');self.assertEqual(ticket['executionVenue'],'NXT')
   p=Path(__import__('live_trader').__file__).resolve().parent/'state.py';tree=ast.parse(p.read_text(encoding='utf-8-sig'))
   func=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='live_broker_payload')
   module=ast.Module(body=[func],type_ignores=[])
   scope={'__package__':'live_trader','OrderIntent':object,'Any':object,'safe_float':lambda value,default=0:float(value) if value is not None else default,'broker_id_from_symbol':lambda *args:'kis','runtime_settings':lambda:fixture_module.SETTINGS}
   with patch('live_trader.kis_equity_execution.validate_ticket',return_value=ticket):
    exec(compile(module,str(p),'exec'),scope);wire=scope['live_broker_payload'](intent,idempotency_key='fixture')
   self.assertEqual(wire['order_type'],'27');self.assertEqual(wire['stockSessionProfile'],selected);self.assertEqual(wire['sessionPhase'],'PRE');self.assertEqual(wire['price'],float(ticket['limitPrice']))
   tampered=deepcopy(context);tampered['exactOpenBoundary']['sourceVenue']='KRX';bad=fixture_module.quantity(spec,tampered)
   self.assertEqual(bad['status'],'BLOCKED')
 def test_profile_queue_waits_for_own_real_open_and_next_phase_not_regular(self):
  selected=profile('EXTENDED',market='KR',provider='kis');opened=datetime(2026,11,17,23,40,tzinfo=UTC)
  bar=ClosedBar('005930','005930','kis','5m',(opened-timedelta(minutes=5)).isoformat(),opened.isoformat(),100,101,99,100,1,source_provider='kis',session='pre',session_profile_hash=profile_hash(selected),source_venue='NXT')
  spec=SimpleNamespace(strategy_instance_id='member',strategy_id='ma',artifact_hash='c'*64,portfolio_hash='e'*64,provider='kis',instrument_id='005930',symbol='005930',timeframe='5m',parameters={'stockSessionProfile':selected,'executionTiming':'next-open'},artifact={})
  decision=SimpleNamespace(bar=bar,evaluation_key='e1',signal='BUY',reason='fixture');self.assertEqual(decision_payload(decision,spec,'d')['expectedStart'],opened.isoformat())
  boundary=OpenBoundary('005930','005930','kis','5m',opened.isoformat(),101,opened.isoformat(),'kis:fixture','a'*64,'KIS_WEBSOCKET',True,source_provider='kis',session='pre',session_profile_hash=profile_hash(selected),source_venue='NXT')
  with tempfile.TemporaryDirectory() as tmp:
   q=NativeBoundaryQueue(Path(tmp)/'queue.db');q.stage(decision,spec,'d')
   self.assertEqual(q.release(replace(boundary,session_profile_hash='b'*64),[spec],'d',lambda *args: {'ok':True},now=opened)['results'],[])
   self.assertEqual(q.snapshot()['pending'],1)
   result=q.release(boundary,[spec],'d',lambda *args:{'ok':True,'orderSubmitted':False},now=opened)
   self.assertEqual(len(result['results']),1);self.assertEqual(q.snapshot()['pending'],0)

 def test_cleanup_proof_keeps_nxt_venue_and_original_cancel_session(self):
  import test_kis_scoped_cleanup_truth as t
  from live_trader.kis_scoped_cleanup_adapter import build_kis_scoped_cleanup_request
  selected=profile('EXTENDED',market='KR',provider='kis');now=datetime(2026,11,18,7,tzinfo=UTC)
  scope,owned,working,history=t.fixture('KRX')
  self._profile_cleanup_fixture(t,scope,owned,working,history,selected,'AFTER','20261118')
  for row in (working,history):row.update(excg_id_dvsn_cd='NXT',ord_dvsn_cd='00')
  with patch.object(t,'NOW',now):
   observer=t.Observer(working,history);runtime=SimpleNamespace(scope=lambda:deepcopy(scope),observer=observer)
   proof=t.truth.collect_cleanup_truth(runtime,[owned],now=now,monotonic_clock=lambda:1000.)
  t.truth.validate_cleanup_truth(proof,[owned],now=now)
  with patch.dict(os.environ,ENV,clear=False):request=build_kis_scoped_cleanup_request(proof['orders'][0],access_token='synthetic')
  self.assertEqual(request.body['EXCG_ID_DVSN_CD'],'NXT');self.assertEqual(request.body['QTY_ALL_ORD_YN'],'N');self.assertEqual(request.body['ORD_QTY'],'2')
  self.assertEqual(observer.calls[1][1]['EXCG_ID_DVSN_CD'],'ALL')
 def test_day_cleanup_does_not_reject_official_trading_label_as_future_ny_date(self):
  import test_kis_scoped_cleanup_truth as t
  selected=profile({'market':'US','mode':'EXTENDED','segments':['DAY']},provider='kis');now=datetime(2026,11,18,1,30,tzinfo=UTC)
  scope,owned,working,history=t.fixture('NASD')
  self._profile_cleanup_fixture(t,scope,owned,working,history,selected,'DAY','20261118')
  with patch.object(t,'NOW',now):
   observer=t.Observer(working,history);runtime=SimpleNamespace(scope=lambda:deepcopy(scope),observer=observer)
   proof=t.truth.collect_cleanup_truth(runtime,[owned],now=now,monotonic_clock=lambda:1000.)
  self.assertEqual(proof['orders'][0]['state'],'WORKING');self.assertEqual(proof['orders'][0]['sessionPhase'],'DAY')
  t.truth.validate_cleanup_truth(proof,[owned],now=now)
 def _profile_cleanup_fixture(self,t,scope,owned,working,history,selected,phase,day):
  scope['members'][0]['stockSessionProfile']=selected;scope.pop('scopeHash');scope['scopeHash']=t.truth._hash(scope)
  ticket=owned['dispatch']['metadata']['native_quantity_model'];route=kis_order_route(selected,exchange=owned['venue'],session_phase=phase)
  ticket.update(stockSessionProfile=selected,stockSessionProfileHash=profile_hash(selected),sessionPhase=phase,executionVenue=route['exchangeId'],orderClass=route['orderClass'],capitalScopeHash=scope['scopeHash'])
  owned['dispatch']['metadata']['native_quantity_model']=t.service.seal(ticket)
  owned['orderDate']=day;owned['dispatch'].update(order_date=day,broker_order_key=day+':001:0002')
  owned['dispatchHash']=t.truth._hash(owned['dispatch']);owned['businessHash']=t.truth.dispatch_business_hash(owned['dispatch'])
  history['ord_dt']=day
  if 'ord_dt' in working:working['ord_dt']=day

if __name__=='__main__':unittest.main(verbosity=2)
