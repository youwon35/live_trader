from datetime import datetime,timedelta,timezone
import copy
import threading
import unittest
from live_trader.kis_operational_preflight_refresh import refresh_kis_operational_preflight,_hash

class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,10,16,1,tzinfo=timezone.utc);self.mono=1000.
        context=dict(schemaVersion='kis-scoped-manual-start-context/v1',scopeHash='a'*64,
            profileId='stock',mode='SMALL_LIVE',portfolioId='p',deploymentId='d',strategyId='s',
            runtimeScopeId='r',capitalCurrency='KRW',accountFingerprint='b'*64,
            credentialConfigurationHash='c'*64,wireAccountFingerprint='d'*64,
            wireCredentialConfigurationHash='e'*64,ownerEpochHash='f'*64)
        self.current=dict(schemaVersion='kis-scoped-preflight-expected/v1',sessionId='session',
            runtimeScopeId='r',context=context,manifestHash='1'*64,configRevision=1,
            configHash='2'*64,preflightSnapshotId='old',preflightValid=False)
        self.collected=[];self.committed=[];self.revoked=[]
        self.collect_hook=None;self.commit_hook=None
    def capture(self,_intent):return copy.deepcopy(self.current)
    def collect(self,expected):
        self.collected.append(copy.deepcopy(expected))
        if self.collect_hook:return self.collect_hook(expected)
        return self.truth(expected)
    def truth(self,expected):
        return dict(schemaVersion='kis-scoped-preflight-truth/v1',expectedHash=_hash(expected),
            collectedAt=self.now.isoformat(),reconciliation={'summary':{'status':'pass'}},
            brokers=[{'id':'kis','ready':True}])
    def commit(self,expected,truth):
        self.committed.append(copy.deepcopy(expected))
        if self.commit_hook:return self.commit_hook(expected,truth)
        self.current.update(preflightSnapshotId='new',preflightValid=True)
        return dict(ok=True,action='REFRESHED',sessionId='session',preflightSnapshotId='new',
            preflightHash='3'*64,expiresAt=(self.now+timedelta(seconds=300)).isoformat())
    def run_refresh(self,**overrides):
        kwargs=dict(capture_expected=self.capture,collect_fresh_truth=self.collect,
            commit_rebind=self.commit,revoke_on_failure=lambda *args:self.revoked.append(args),
            assert_outside_authority_locks=lambda:True,
            clock=lambda:self.now,monotonic_clock=lambda:self.mono)
        kwargs.update(overrides)
        return refresh_kis_operational_preflight(object(),**kwargs)
    def test_fresh_actual_rebind_required(self):
        result=self.run_refresh()
        self.assertTrue(result['ok'],result);self.assertEqual(result['action'],'REFRESHED')
        self.assertEqual(len(self.collected),1);self.assertEqual(len(self.committed),1)
        self.assertEqual(self.revoked,[])
    def test_current_preflight_never_collects(self):
        self.current['preflightValid']=True
        self.assertTrue(self.run_refresh()['ok']);self.assertEqual(self.collected,[])
    def test_future_cycle_needs_new_truth_and_new_snapshot(self):
        self.assertTrue(self.run_refresh()['ok'])
        self.now+=timedelta(seconds=301);self.mono+=301
        self.current.update(preflightValid=False,preflightSnapshotId='expired')
        self.assertTrue(self.run_refresh()['ok'])
        self.assertEqual(len(self.collected),2)
    def test_authority_lock_blocks_all_collection(self):
        result=self.run_refresh(assert_outside_authority_locks=lambda:False)
        self.assertFalse(result['ok']);self.assertEqual(self.collected,[])
        self.assertFalse(result['revocationAcknowledged'])
        self.assertEqual(self.revoked,[])
    def test_initial_capture_failure_cannot_claim_revocation(self):
        def capture(_intent):raise ValueError('fixture lease was lost')
        result=self.run_refresh(capture_expected=capture)
        self.assertFalse(result['ok']);self.assertFalse(result['revocationAcknowledged'])
        self.assertEqual(self.collected,[]);self.assertEqual(self.revoked,[])
    def test_scope_change_during_network_rejects_and_revokes(self):
        def collect(expected):
            truth=self.truth(expected);self.current['context']['credentialConfigurationHash']='0'*64
            return truth
        self.collect_hook=collect
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(self.committed,[])
        self.assertEqual(len(self.revoked),1)
    def test_failed_get_never_rebinds(self):
        def collect(expected):raise OSError('fixture read-only transport unavailable')
        self.collect_hook=collect
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(self.committed,[])
        self.assertEqual(len(self.revoked),1)
    def test_stale_collected_timestamp(self):
        self.collect_hook=lambda expected:{**self.truth(expected),'collectedAt':(self.now-timedelta(seconds=1)).isoformat()}
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(self.committed,[])
    def test_truth_bound_to_original_expected_hash(self):
        self.collect_hook=lambda expected:{**self.truth(expected),'expectedHash':'0'*64}
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(self.committed,[])
    def test_collector_cannot_mutate_captured_scope(self):
        def collect(expected):
            expected['context']['capitalCurrency']='USD'
            return self.truth(expected)
        self.collect_hook=collect
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(self.committed,[])
    def test_collection_deadline(self):
        def collect(expected):
            self.mono+=11;return self.truth(expected)
        self.collect_hook=collect
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(self.committed,[])
    def test_collection_clock_rollback(self):
        def collect(expected):
            self.mono-=1;return self.truth(expected)
        self.collect_hook=collect
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(self.committed,[])
    def test_commit_failure_does_not_report_success(self):
        self.commit_hook=lambda expected,truth:{'ok':False}
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(len(self.revoked),1)
    def test_forged_rebind_ack_without_state_transition(self):
        self.commit_hook=lambda expected,truth:dict(ok=True,action='REFRESHED',sessionId='session',
            preflightSnapshotId='new',preflightHash='3'*64,
            expiresAt=(self.now+timedelta(seconds=300)).isoformat())
        self.assertFalse(self.run_refresh()['ok']);self.assertEqual(len(self.revoked),1)
    def test_expired_replacement_not_authority(self):
        self.commit_hook=lambda expected,truth:dict(ok=True,action='REFRESHED',sessionId='session',
            preflightSnapshotId='new',preflightHash='3'*64,expiresAt=self.now.isoformat())
        self.assertFalse(self.run_refresh()['ok'])
    def test_same_scope_concurrent_refresh_collects_once(self):
        entered=threading.Event();release=threading.Event();outcomes=[]
        def collect(expected):
            entered.set();release.wait(2);return self.truth(expected)
        self.collect_hook=collect
        a=threading.Thread(target=lambda:outcomes.append(self.run_refresh()))
        b=threading.Thread(target=lambda:outcomes.append(self.run_refresh()))
        a.start();self.assertTrue(entered.wait(1));b.start();release.set()
        a.join(2);b.join(2)
        self.assertEqual(len(outcomes),2)
        self.assertTrue(all(row['ok'] for row in outcomes),outcomes)
        self.assertEqual(len(self.collected),1)
    def test_current_context_still_requires_exact_schema(self):
        for field,bad in [('configRevision',True),('configHash','bad'),('preflightValid','true')]:
            original=self.current[field];self.current[field]=bad
            self.assertFalse(self.run_refresh()['ok'])
            self.current[field]=original

if __name__=='__main__':unittest.main()

