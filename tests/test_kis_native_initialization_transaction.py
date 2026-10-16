"""Synthetic private observation / local principal commit separation only."""
from copy import deepcopy
from datetime import datetime,timedelta,timezone
from pathlib import Path
from types import SimpleNamespace
from contextlib import closing
import sqlite3,tempfile,threading,unittest
from trading_runtime.artifact_governance import stable_sha256
from live_trader.kis_equity_runtime import NativeEquityRuntime
from live_trader.kis_owned_capital import KisOwnedCapitalStore
import test_kis_equity_operational as fixtures


class NativeInitializationTransactionTests(unittest.TestCase):
    def fixture(self,folder):
        spec,contract,context=fixtures.fixture('KRX')
        account=context['savedAccountProfile']
        scope={'version':'kis-owned-capital-v2','deploymentId':'fixture-deployment',
            'accountProfileId':account['profileId'],'accountFingerprint':account['accountFingerprint'],
            'credentialConfigurationHash':account['credentialConfigurationHash'],'quoteCurrency':'KRW','capitalPercent':'10',
            'members':[{'strategyInstanceId':spec.strategy_instance_id,'symbol':spec.symbol}]}
        scope['scopeHash']=stable_sha256(scope)
        policies={'executionPolicies':{spec.artifact_hash:context['executionPolicy']}}
        observation=deepcopy(context['nativeMarketObservation'])
        observation.update(asOf=datetime.now(timezone.utc).isoformat())
        runtime=object.__new__(NativeEquityRuntime);runtime.specs=(spec,)
        runtime._details=lambda:(deepcopy(scope),account,policies)
        runtime.store=KisOwnedCapitalStore(Path(folder)/'owned.sqlite3')
        runtime.observer=SimpleNamespace(quote=lambda *args:{'price':'100'},observe=lambda *a,**kw:deepcopy(observation))
        return runtime,scope,observation

    def test_stop_during_actual_observe_does_not_initialize_or_change_existing_local_book(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime,scope,observation=self.fixture(folder)
            runtime.store.initialize(scope,observation)
            before=runtime.store.path.read_bytes();entered,released,cancelled=threading.Event(),threading.Event(),threading.Event()
            def observe(*args,**kwargs):
                entered.set();self.assertTrue(released.wait(3));return deepcopy(observation)
            runtime.observer.observe=observe;facts=[]
            worker=threading.Thread(target=lambda:facts.append(runtime.observe_manual_start_initialization()),daemon=True)
            worker.start();self.assertTrue(entered.wait(1));cancelled.set();released.set();worker.join(3)
            self.assertFalse(worker.is_alive());self.assertEqual(before,runtime.store.path.read_bytes())
            def guard():
                if cancelled.is_set():raise RuntimeError('STOP_BEFORE_LOCAL_COMMIT')
            with self.assertRaisesRegex(RuntimeError,'STOP_BEFORE_LOCAL_COMMIT'):
                runtime.commit_manual_start_initialization(facts[0],preparation_guard=guard)
            self.assertEqual(before,runtime.store.path.read_bytes())

    def test_commit_guard_failure_inside_same_transaction_rolls_back_append_without_rebasing(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime,scope,observation=self.fixture(folder);runtime.store.initialize(scope,observation)
            before=runtime.store.path.read_bytes();other={**scope,'deploymentId':'fixture-other'}
            other['scopeHash']=stable_sha256({k:v for k,v in other.items() if k!='scopeHash'})
            calls=[]
            def guard():
                calls.append(None)
                if len(calls)==3:raise RuntimeError('CANCELLED_BEFORE_TRANSACTION_COMMIT')
            with self.assertRaisesRegex(RuntimeError,'CANCELLED_BEFORE_TRANSACTION_COMMIT'):
                runtime.store.initialize(other,observation,commit_guard=guard)
            self.assertEqual(3,len(calls));self.assertEqual(before,runtime.store.path.read_bytes())
            with closing(sqlite3.connect(runtime.store.path)) as db:
                self.assertEqual(1,db.execute('SELECT COUNT(*) FROM principals').fetchone()[0])

    def test_current_scope_freshness_and_unchanged_fact_required_at_commit(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime,scope,observation=self.fixture(folder);fact=runtime.observe_manual_start_initialization()
            self.assertFalse(runtime.store.path.exists())
            old=deepcopy(fact);old['observedAt']=(datetime.now(timezone.utc)-timedelta(seconds=16)).isoformat()
            old['contentHash']=stable_sha256({k:v for k,v in old.items() if k!='contentHash'})
            with self.assertRaisesRegex(ValueError,'FRESHNESS_CHANGED'):runtime.commit_manual_start_initialization(old)
            changed=deepcopy(fact);changed['observation']['availableCash']='999999'
            with self.assertRaisesRegex(ValueError,'OBSERVATION_INVALID'):runtime.commit_manual_start_initialization(changed)
            scope['credentialConfigurationHash']='f'*64
            with self.assertRaisesRegex(ValueError,'SCOPE_OR_FRESHNESS_CHANGED'):runtime.commit_manual_start_initialization(fact)
            self.assertFalse(runtime.store.path.exists())

    def test_linearized_valid_commit_is_preserved_after_later_stop(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime,scope,observation=self.fixture(folder);fact=runtime.observe_manual_start_initialization();calls=[]
            projected=runtime.commit_manual_start_initialization(fact,preparation_guard=lambda:calls.append(None))
            self.assertGreaterEqual(len(calls),4);self.assertEqual('10000',projected['ownedCash'])
            before=runtime.store.path.read_bytes()
            def cancelled():raise RuntimeError('LATER_STOP')
            with self.assertRaisesRegex(RuntimeError,'LATER_STOP'):
                runtime.commit_manual_start_initialization(fact,preparation_guard=cancelled)
            self.assertEqual(before,runtime.store.path.read_bytes())

    def test_full_native_fixture_reader_observe_commit_and_three_member_context_are_real_consumers(self):
        import test_live_kis_native_bootstrap as bootstrap
        from test_native_fixture_support import native_fixture_account,native_fixture_clock,build_native_runtime
        from live_trader.runtime_scope import RuntimeScope
        case=bootstrap.LiveKisNativeBootstrapTests();case.setUp();account=native_fixture_account()
        case.scope=RuntimeScope('kis',account['accountFingerprint'],'fixture-deployment','stock',
            account['accountBindingId'],account['credentialConfigurationHash'],'KRW')
        case.context.update(runtimeScopeId=case.scope.runtime_id,accountFingerprint=account['accountFingerprint'],
            credentialConfigurationHash=account['credentialConfigurationHash'])
        case.binding.update(context=case.context,runtimeScope=case.scope.to_dict())
        with tempfile.TemporaryDirectory() as folder:
            root,loaded,prepared=case.canonical_live_fixture(folder)
            with native_fixture_clock(case.clock.utc):
                fake=build_native_runtime(root,specs=loaded.specs,runtime_scope=case.scope,clock=case.clock.utc)
                fact=fake.runtime.observe_manual_start_initialization()
                self.assertFalse(fake.runtime.store.path.exists())
                result=fake.runtime.commit_manual_start_initialization(fact,preparation_guard=prepared.assert_current)
                self.assertEqual('9000000',result['ownedCash'])
                member=next(spec for spec in loaded.specs if spec.symbol=='000660')
                context=fake.runtime.context(member,'BUY',95000,loaded.specs,{'fixture':'unprivileged-boundary-input'})
                self.assertNotEqual('BLOCKED',context.get('status'),context)
                self.assertEqual('9000000',context['availableCash']);self.assertEqual('KRW',context['quoteCurrency'])
                self.assertEqual(3,len(context['ownedPositions']));self.assertFalse(context['orderAuthorityGranted'])
                self.assertTrue(fake.reader.events)
            prepared.close()
