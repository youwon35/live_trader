"""Offline original-wire → REST → Live prepared data, no real API/lease/order."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta
from pathlib import Path
from types import SimpleNamespace
import hashlib,unittest
from unittest.mock import patch
import os,sys
_MONOREPO=Path(os.environ.get('LIVE_NATIVE_TEST_MONOREPO_ROOT') or Path(__file__).resolve().parents[3])
sys.path.insert(0,str(_MONOREPO/'packages/trading_runtime/tests'))
from native_session_fixture import fixture
from trading_runtime.native_session_initialization import build_session_initialization
from trading_runtime.realtime_feeds import KisWebSocketClosedBarFeed
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.market_clock_integrity import StickyMarketClockIntegrity
from trading_runtime.market_source_integrity import StickyMarketSourceIntegrity
from live_trader.runtime_scope import RuntimeScope
from live_trader.kis_native_initialization import build_live_initialization,validate_live_initialization,SESSION_SCHEMA,LiveKisNativeInitializedEngine
from live_trader.kis_native_session_preparation import prepare_live_kis_session
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator
UTC=timezone.utc
ENV={'KIS_APP_KEY':'SYNTHETIC','KIS_APP_SECRET':'SYNTHETIC','KIS_ACCOUNT_NO':'12345678','KIS_ACCOUNT_PRODUCT_CODE':'01','KIS_ENV':'real'}

class Tests(unittest.TestCase):
 def setUp(self):
  self.f=fixture();self.specs=self.f['specs'];self.scope=RuntimeScope('kis','b'*64,'deployment','stock','binding-fixture','c'*64,'KRW')
  self.context={'schemaVersion':'kis-scoped-manual-start-context/v1','scopeHash':'e'*64,'profileId':'stock','mode':'SMALL_LIVE','portfolioId':'p','deploymentId':'deployment','strategyId':'source','runtimeScopeId':self.scope.runtime_id,'capitalCurrency':'KRW','accountFingerprint':'b'*64,'credentialConfigurationHash':'c'*64,'wireAccountFingerprint':'d'*64,'wireCredentialConfigurationHash':'e'*64,'ownerEpochHash':'f'*64}
  self.f['capture_scope']['runtimeScopeId']=self.scope.runtime_id
  self.data=build_session_initialization(**self.f);self.binding={'context':self.context,'runtimeScope':self.scope.to_dict(),'deploymentManifestHash':'1'*64}
 def receipt(self):return build_live_initialization(self.data,context=self.context,runtime_scope=self.scope,deployment_manifest_hash='1'*64,specs=self.specs)
 def test_new_separate_schema_never_grants_or_imports_legacy_authority(self):
  row=self.receipt();self.assertEqual(row['schemaVersion'],SESSION_SCHEMA);self.assertFalse(row['executionAuthorityGranted']);self.assertFalse(row['promotionEvidence'])
  self.assertEqual(validate_live_initialization(row,context=self.context,runtime_scope=self.scope,deployment_manifest_hash='1'*64,specs=self.specs),row)
 def test_wrong_currency_and_account_do_not_fit_new_wrapper(self):
  for context,scope in [({**self.context,'capitalCurrency':'USD'},RuntimeScope('kis','b'*64,'deployment','stock','binding-fixture','c'*64,'USD')),({**self.context,'accountFingerprint':'a'*64},self.scope)]:
   with self.assertRaises(ValueError):build_live_initialization(self.data,context=context,runtime_scope=scope,deployment_manifest_hash='1'*64,specs=self.specs)
 def test_engine_seed_and_profile_phase_are_separate_from_regular_guard(self):
  engine=LiveKisNativeInitializedEngine(self.specs,mode='MONITOR',evaluator=BuiltinBarSignalEvaluator(),cycle_handler=lambda _:None)
  receipt=self.receipt();counts=engine.seed_live_initialization(receipt,context=self.context,runtime_scope=self.scope,deployment_manifest_hash='1'*64)
  self.assertEqual(counts,{'template':3});self.assertEqual(engine.bar_count,0)
  from trading_runtime.provider_backfill_kis_sessions import verify_segment
  from dataclasses import replace
  event=verify_segment(self.f['streams']['template']['historyReceipt'])[-1]
  later=replace(event,start_time='2026-11-18T06:55:00Z',end_time='2026-11-18T07:00:00Z',evidence_class='NATIVE_FORWARD')
  engine._assert_native_phase(later.start_time,later.end_time,event=later)
  with self.assertRaises(ValueError):engine._assert_native_phase(later.start_time,later.end_time,event=replace(later,session='regular',source_venue='KRX'))
  closed=replace(later,start_time='2026-11-18T11:00:00Z',end_time='2026-11-18T11:05:00Z')
  with self.assertRaises(ValueError):engine._assert_native_phase(closed.start_time,closed.end_time,event=closed)
 def test_us_original_dual_clock_proof_requires_owned_usd_scope(self):
  from native_session_us_fixture import fixture_us
  f=fixture_us();scope=RuntimeScope('kis','b'*64,'deployment','stock','binding-fixture','c'*64,'USD')
  f['capture_scope']['runtimeScopeId']=scope.runtime_id;data=build_session_initialization(**f)
  context={**self.context,'runtimeScopeId':scope.runtime_id,'capitalCurrency':'USD'}
  receipt=build_live_initialization(data,context=context,runtime_scope=scope,deployment_manifest_hash='1'*64,specs=f['specs'])
  self.assertEqual(receipt['schemaVersion'],SESSION_SCHEMA);self.assertEqual(receipt['runtimeScope']['capitalCurrency'],'USD')
  with self.assertRaises(ValueError):build_live_initialization(data,context=self.context,runtime_scope=self.scope,deployment_manifest_hash='1'*64,specs=f['specs'])
 def test_generic_profile_history_uses_exact_subscription_and_seed_only(self):
  from live_trader.live_stock_profile_history import seed_profile_history,exact_subscription
  from trading_runtime.provider_backfill_kis_sessions import verify_segment
  from trading_runtime.realtime_feeds import FeedSubscription
  stream=self.f['streams']['template'];receipt=stream['historyReceipt'];bars=verify_segment(receipt)
  feed=SimpleNamespace(provider_id='kis',subscriptions=[FeedSubscription('005930','005930','5m','NXT',stream['stockSessionProfile'])],authoritative_warmup=lambda sub,limit:(bars,receipt),mark_emitted=lambda rows:None)
  engine=LiveKisNativeInitializedEngine(self.specs,mode='MONITOR',evaluator=BuiltinBarSignalEvaluator(),cycle_handler=lambda _:None)
  self.assertEqual(seed_profile_history([feed],self.specs[0],engine,3),3)
  self.assertEqual((engine.bar_count,engine.decision_count),(0,0));self.assertIsNone(engine.live_native_initialization)
  feed.subscriptions[0]=FeedSubscription('005930','005930','5m','KRX',stream['stockSessionProfile'])
  with self.assertRaisesRegex(ValueError,'EXACT_PROFILE_EXCHANGE'):exact_subscription([feed],self.specs[0])
 def test_actual_producer_replays_one_original_socket_and_paired_rest(self):
  source=self.f['streams']['template'];clock=[datetime.fromisoformat(source['connectedAt'])];mono=[1000.];created=[]
  class Feed(KisWebSocketClosedBarFeed):
   def connect(feed):
    feed.socket=object();feed._clock_generation=7;feed._connected_at=clock[0].timestamp();feed._fixture_index=0
   def poll(feed,timeout):
    if feed._fixture_index<len(source['rawFrames']):
     row=source['rawFrames'][feed._fixture_index];feed._fixture_index+=1;clock[0]=datetime.fromisoformat(row['receivedAt']);mono[0]=row['receivedMonotonic'];feed._socket_receipt_monotonic=mono[0];out=[];feed._consume_socket_frame(row['text'],out,received_epoch=clock[0].timestamp());return out
    return []
   def readiness_snapshot(feed):return {'ready':feed.socket is not None,'connected':feed.socket is not None,'transport':'kis-websocket'}
   def disconnect(feed):feed.socket=None
  def factory(specs,**kwargs):
   from trading_runtime.realtime_feeds import FeedSubscription
   feed=Feed([FeedSubscription('005930','005930','5m','NXT',source['stockSessionProfile'])],app_key=kwargs['kis_app_key'],app_secret=kwargs['kis_app_secret'],kis_account_id=kwargs['kis_account_id'],websocket_owner_id=kwargs['kis_websocket_owner_id'],artifact_provider_id='kis',now=lambda:clock[0].timestamp())
   created.append(feed);return [feed]
  def fetch(sub,params):return deepcopy(source['historyReceipt']['pages'][0]['publicResponse'])
  plan={'loaded':SimpleNamespace(specs=self.specs,portfolio_id='p',portfolio_hash='a'*64),**self.binding,'portfolioPath':Path('synthetic-portfolio'),'artifactRoot':Path('synthetic-artifacts')}
  with patch('live_trader.kis_native_session_preparation.saved_account_profiles',return_value=[{'brokerId':'kis',**self.scope.to_dict()}]):
   prepared=prepare_live_kis_session(Path('.'),plan=plan,binding_reader=lambda:deepcopy(self.binding),preparation_guard=lambda:None,settings=ENV,utc_clock=lambda:clock[0],monotonic_clock=lambda:mono[0],maximum_seconds=660,feed_factory=factory,history_fetcher=fetch)
  self.assertEqual(prepared.receipt['schemaVersion'],SESSION_SCHEMA);self.assertEqual(len(created),1)
  self.assertEqual(prepared.receipt['publicInitializationData']['captureScope']['appKeyIdHash'],hashlib.sha256(b'SYNTHETIC').hexdigest())
  prepared.assert_prepared();self.assertFalse(hasattr(prepared,'engine'))
  self.assertIs(prepared.feeds[0],created[0]);created[0]._clock_generation+=1
  with self.assertRaises((ValueError,RuntimeError)):prepared.assert_prepared()

if __name__=='__main__':unittest.main()
