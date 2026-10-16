"""Public ticks and UI method options only. All account/clock data synthetic."""
from copy import deepcopy
from datetime import datetime,timedelta,timezone
from pathlib import Path
import tempfile,unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch

from native_tick_fixture import inputs,ARTIFACTS,calendar_objects
from trading_runtime.native_tick_warmup import timestamp
from trading_runtime.native_tick_capture_store import native_tick_artifact_scope
from trading_runtime.continuous_runtime import ClosedBar,DurableRuntimeState
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator
from live_trader.kis_native_preparation_options import parse_native_preparation_options,NativePreparationOptions
from live_trader.kis_native_public_preparation import prepare_live_kis_native
from live_trader.kis_native_initialization import build_live_initialization,LiveKisNativeInitializedEngine
from live_trader.kis_native_start_registry import KisNativeStartRegistry
from live_trader.kis_native_start_preparation_jobs import KisNativePreparationJobs
from live_trader.runtime_scope import RuntimeScope
from test_native_fixture_support import SETTINGS,native_fixture_account
from test_kis_native_start_registry import Timer,QueuedThread


class NativeTickOptionsTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,10,19,0,4,50,tzinfo=timezone.utc)
        self.value={'method':'PUBLIC_H0STCNT0','cutoff':'2026-10-19T04:15:00Z'}
    def test_strict_public_method_has_code_owned_270m_only_default_rest_660(self):
        self.assertEqual(parse_native_preparation_options().maximum_seconds,660)
        self.assertEqual(parse_native_preparation_options(self.value,now=self.now).maximum_seconds,16200)
        for value in ({**self.value,'receipt':{}},{**self.value,'authority':True},{**self.value,'method':'MARKET'},
                {'method':'REVIEWED_REST','cutoff':self.value['cutoff']},True,{}, {'method':'PUBLIC_H0STCNT0','cutoff':False}):
            with self.subTest(value=value),self.assertRaises(ValueError):parse_native_preparation_options(value,now=self.now)
    def test_old_future_non5m_same_day_phase_and_duration_invalid_before_capture(self):
        for point in ('2026-10-19T04:14:00Z','2026-10-19T04:15:00','2026-10-20T04:15:00Z',
                '2026-10-19T06:20:00Z','2026-10-19T04:15:01Z','2026-10-19T04:10:00Z'):
            with self.subTest(point=point),self.assertRaises(ValueError):parse_native_preparation_options({**self.value,'cutoff':point},now=self.now)
    def test_long_public_job_cancel_timeout_and_progress_never_arm_native(self):
        now=[1000.0];registry=KisNativeStartRegistry(monotonic_clock=lambda:now[0],timer_factory=Timer,close_dispatch=lambda value:None)
        jobs=KisNativePreparationJobs(registry=registry,clock=lambda:now[0],thread_factory=QueuedThread,timer_factory=Timer)
        QueuedThread.pending=[]
        entry=registry.reserve({'context':{'runtimeScopeId':'offline'},'runtimeScope':{},'deploymentManifestHash':'1'*64})
        options=parse_native_preparation_options(self.value,now=self.now)
        def run(publish):
            now[0]+=1000
            publish('PREPARING_PUBLIC',{'phase':'CAPTURING_NATIVE_TICKS','frameCount':123,'elapsedSeconds':1000.0,'cutoff':options.cutoff})
            self.assertEqual(jobs.status(result['preparationId'])['status'],'PREPARING_PUBLIC')
            jobs.cancel(result['preparationId'])
            with self.assertRaises(RuntimeError):publish('CHECKING_ACCOUNT')
            return {'ok':True,'nativeNonce':'synthetic-no-authority'}
        result=jobs.begin(entry=entry,run=run,initialization_options=options)
        self.assertEqual(result['maximumPreparationSeconds'],16200);self.assertNotIn('nativeNonce',result)
        QueuedThread.pending.pop()()
        status=jobs.status(result['preparationId']);self.assertEqual(status['status'],'CANCELLED');self.assertFalse(status['runtimeStarted'])
        self.assertEqual(status['publicProgress']['frameCount'],123)
        entry=registry.reserve({'context':{'runtimeScopeId':'offline'},'runtimeScope':{},'deploymentManifestHash':'1'*64})
        producer=Mock();result=jobs.begin(entry=entry,run=producer,initialization_options=options)
        now[0]+=16201
        self.assertEqual(jobs.status(result['preparationId'])['status'],'EXPIRED')
        QueuedThread.pending.pop()();producer.assert_not_called()


class NativeTickLiveIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.data,self.loaded,self.policy,self.clock,self.feed,self.scope,self.calls,self.frames=inputs('LIVE')
        account=native_fixture_account()
        self.runtime=RuntimeScope('kis',account['accountFingerprint'],'offline-selected-native','stock',account['accountBindingId'],account['credentialConfigurationHash'],'KRW')
        self.context={'schemaVersion':'kis-scoped-manual-start-context/v1','scopeHash':'c'*64,'profileId':'stock',
            'mode':'SMALL_LIVE','portfolioId':self.loaded.portfolio_id,'deploymentId':self.runtime.deployment_id,
            'strategyId':self.loaded.specs[0].strategy_id,'runtimeScopeId':self.runtime.runtime_id,'capitalCurrency':'KRW',
            'accountFingerprint':self.runtime.account_fingerprint,'credentialConfigurationHash':self.runtime.credential_configuration_hash,
            'wireAccountFingerprint':'d'*64,'wireCredentialConfigurationHash':'e'*64,'ownerEpochHash':'f'*64}
        self.binding={'context':self.context,'runtimeScope':self.runtime.to_dict(),'deploymentManifestHash':'1'*64}
        self.plan={**self.binding,'loaded':self.loaded,'promotionPolicy':self.policy,'portfolioPath':str(ARTIFACTS/'portfolios/selected.json'),
            'artifactRoot':str(ARTIFACTS),'calendarObjects':calendar_objects(self.data)}
        self.options=parse_native_preparation_options({'method':'PUBLIC_H0STCNT0','cutoff':self.data['requirements'].cutoff},now=self.clock.utc())
    def factory(self,specs,**kwargs):
        self.feed.app_key=kwargs['kis_app_key'];self.feed.app_secret=kwargs['kis_app_secret'];self.feed.kis_account_id=kwargs['kis_account_id']
        return (self.feed,)
    def prepare(self,guard=lambda:None):
        return prepare_live_kis_native(self.root,plan=self.plan,binding_reader=lambda:deepcopy(self.binding),
            preparation_guard=guard,settings=dict(SETTINGS),utc_clock=self.clock.utc,monotonic_clock=self.clock.mono,
            feed_factory=self.factory,initialization_options=self.options)
    def test_normal_typed_producer_to_live_engine_is_source_account_bound_no_authority(self):
        prepared=self.prepare()
        self.assertEqual((self.calls['connect'],self.calls['private'],self.calls['orders']),(1,0,0))
        self.assertFalse(prepared.receipt['executionAuthorityGranted']);self.assertFalse(prepared.receipt['paperSessionImported'])
        with prepared.initialization_scope():
            engine=LiveKisNativeInitializedEngine(self.loaded.specs,mode='MONITOR',evaluator=BuiltinBarSignalEvaluator(),cycle_handler=lambda _:None,
                state_store=DurableRuntimeState(self.root/'offline-engine.json'))
            counts=engine.seed_live_initialization(prepared.receipt,context=self.context,runtime_scope=self.runtime,
                deployment_manifest_hash=self.binding['deploymentManifestHash'])
            self.assertEqual(sum(counts.values()),111);self.assertEqual((engine.bar_count,engine.decision_count),(0,0))
            self.assertTrue(engine.transition_mode('SMALL_LIVE',restore_context=None))
            self.assertEqual(engine.mode,'MONITOR')
        prepared.begin_native_confirmation_window()
        with self.assertRaises(RuntimeError):prepared.begin_native_confirmation_window()
        prepared.revoke()
        with self.assertRaises(RuntimeError):prepared.assert_prepared()
        prepared.close()
    def test_paper_capture_cannot_be_reused_as_live_even_with_same_bars(self):
        prepared=self.prepare();public=deepcopy(prepared.receipt['publicInitializationData'])
        public['captureManifest']['scope']['mode']='PAPER'
        from trading_runtime.native_tick_warmup import digest
        public['captureManifest']['contentHash']=digest({k:v for k,v in public['captureManifest'].items() if k!='contentHash'})
        public['contentHash']=digest({k:v for k,v in public.items() if k!='contentHash'})
        with prepared.initialization_scope(),self.assertRaises(ValueError):
            build_live_initialization(public,context=self.context,runtime_scope=self.runtime,deployment_manifest_hash='1'*64,specs=self.loaded.specs)
    def test_method_selection_never_accepts_serialized_raw_or_trusted_reader(self):
        with self.assertRaisesRegex(ValueError,'TYPED_OPTIONS'):
            prepare_live_kis_native(self.root,plan=self.plan,binding_reader=lambda:self.binding,preparation_guard=lambda:None,
                settings=dict(SETTINGS),initialization_options={'method':'PUBLIC_H0STCNT0','cutoff':self.options.cutoff})
        self.assertEqual(self.calls['connect'],0)
    def test_cancelled_scope_during_public_capture_does_not_create_engine_or_principal(self):
        def cancelled():
            if self.calls['poll']>4:raise RuntimeError('SYNTHETIC_PREPARATION_CANCELLED')
        with self.assertRaises(RuntimeError):self.prepare(cancelled)
        self.assertEqual(self.calls['disconnect'],1)
        self.assertFalse(list(self.root.rglob('*.sqlite3')))

if __name__=='__main__':unittest.main()
