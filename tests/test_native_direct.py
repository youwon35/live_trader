"""Synthetic public frames only; preserves original receipt/clock/capture guards."""
from test_direct_strategy import *
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
from types import MethodType
sys.path.insert(0,str(SYSTEM/'packages/trading_runtime/tests'))
from native_tick_fixture import calendar_objects, Clock
from test_native_tick_warmup import fixture as tick_fixture, rows, set_rows
from trading_runtime import native_tick_warmup as warmup
from trading_runtime.native_execution_policy import direct_policy, validate_initialization_policy
from trading_runtime.native_tick_initialization import canonical_graph, graph_member_pins, graph_requirements, independently_replay_tick_initialization
from trading_runtime.native_tick_capture_store import NativeTickArtifactReader,native_tick_artifact_scope
from trading_runtime.native_tick_collector import collect_native_tick_initialization, public_feed_binding
from trading_runtime.realtime_feeds import KisWebSocketClosedBarFeed

class NativeDirectTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=STAGE);self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.network=patch.object(socket,'socket',side_effect=AssertionError('No network'))
        self.network.start();self.addCleanup(self.network.stop)
        originals=SYSTEM/'packages/trading_runtime/tests/fixtures/native_tick_selected'
        for path in originals.glob('*.json'):
            write(self.root/path.name,json.loads(path.read_text(encoding='utf-8')))
        for path in (originals/'strategy-instances').glob('*.json'):
            write(self.root/'strategy-instances'/path.name,json.loads(path.read_text(encoding='utf-8')))
        original=json.loads((originals/'portfolios/selected.json').read_text(encoding='utf-8'))
        self.portfolio=seal_portfolio_artifact({**original,'id':'direct-two-krx','lifecycle_status':'backtested',
            'strategyInstances':original['strategyInstances'][:2]})
        write(self.root/'portfolios/direct.json',self.portfolio)
        self.loaded=load_portfolio_runtime(self.root,'direct-two-krx')
        members=[]
        for spec in self.loaded.specs:
            source={k:v for k,v in spec.artifact.items() if k not in {'strategyInstance','portfolioExecutionPolicy','portfolioPermissions','historicalWarmupReceipt'}}
            members.append(service._member(source,spec.strategy_instance_id,spec.symbol,spec.timeframe,spec.broker_id,spec.parameters))
        binding={'schemaVersion':service.SCHEMA,'strategyArtifact':members[0]['strategyArtifactReference'],
            'portfolioArtifact':artifact_reference(self.portfolio),'members':members,'brokerId':'kis',
            'accountProfileId':'f'*64,'accountFingerprint':'a'*64,'credentialConfigurationHash':'b'*64,
            'accountBindingId':'offline-account','capitalPolicy':{'basis':'ACCOUNT_AVAILABLE_CASH','percent':10.0,'balanceQueriedAtReview':False}}
        binding['bindingHash']=stable_sha256(binding)
        self.policy=direct_policy(binding)
        self.graph=canonical_graph(self.loaded,self.policy)

    def test_direct_policy_uses_exact_new_portfolio_without_paper_pass(self):
        validated=validate_initialization_policy(self.policy,graph=self.graph)
        self.assertFalse(validated['promotionEvidence'])
        self.assertFalse(validated['orderAuthorized'])
        pins=graph_member_pins(self.graph)
        self.assertEqual(len(pins),2)
        self.assertEqual([pin.required_5m_bars for pin in pins],[50,11])
        broken=deepcopy(self.policy);broken['directExecutionBinding']['members'][0]['symbol']='066570'
        with self.assertRaises(ValueError):validate_initialization_policy(broken,graph=self.graph)
        changed=deepcopy(self.graph);changed['sources'][pins[0].source_id]['parameters']['shortMa']=2
        with self.assertRaises(ValueError):validate_initialization_policy(self.policy,graph=changed)

    def test_two_member_capture_same_socket_and_independent_raw_replay(self):
        data=tick_fixture();symbols={spec.symbol for spec in self.loaded.specs}
        for frame in data['frames']:set_rows(frame,[row for row in rows(frame) if row['MKSC_SHRN_ISCD'] in symbols])
        clock=Clock(warmup.timestamp(data['requirements'].connected_at))
        feed=KisWebSocketClosedBarFeed.from_specs(self.loaded.specs,app_key='synthetic',app_secret='synthetic',
            now=lambda:clock.utc().timestamp(),monotonic=clock.mono,kis_account_id='offline',websocket_owner_id='offline')
        frames=list(data['frames']);calls={'connect':0,'disconnect':0}
        def connect(owner):
            calls['connect']+=1;owner.socket=object();owner._clock_generation+=1;owner._connected_at=clock.utc().timestamp();owner.transport='kis-websocket'
        def disconnect(owner):calls['disconnect']+=1;owner.socket=None
        def poll(owner,timeout):
            frame=frames.pop(0);clock.instant=warmup.timestamp(frame['socketReceivedAt']);clock.seconds=frame['receivedMonotonicNs']/1e9
            owner._socket_receipt_monotonic=clock.seconds;events=[]
            owner._consume_socket_frame(frame['raw'],events,received_epoch=clock.utc().timestamp())
            return owner._dedupe_events(events)
        feed.connect=MethodType(connect,feed);feed.disconnect=MethodType(disconnect,feed);feed.poll=MethodType(poll,feed)
        feed.readiness_snapshot=lambda:{'ready':True,'connected':feed.socket is not None,'transport':'kis-websocket'}
        scope={'mode':'LIVE','portfolioHash':self.loaded.portfolio_hash,'promotionPolicyHash':self.policy['policyHash'],
            'accountFingerprint':'a'*64,'credentialConfigurationHash':'b'*64,'runtimeScopeId':'offline-direct',
            'sourceGraphHash':stable_sha256(self.graph),**public_feed_binding(feed)}
        capture=collect_native_tick_initialization(feed=feed,loaded=self.loaded,promotion_policy=self.policy,
            scope=scope,scope_reader=lambda:scope,raw_root=self.root/'raw',calendar_bytes=data['calendar_bytes'],
            cutoff=data['requirements'].cutoff,preparation_guard=lambda:None,utc_clock=clock.utc,
            monotonic_clock=clock.mono,calendar_objects=calendar_objects(data))
        self.assertEqual(calls,{'connect':1,'disconnect':0})
        self.assertFalse(capture.receipt['promotionEvidence'] or capture.receipt['orderAuthorized'])
        with native_tick_artifact_scope(NativeTickArtifactReader(self.root/'raw')):
            proof=independently_replay_tick_initialization(capture.receipt,portfolio_hash=self.loaded.portfolio_hash,promotion_policy_hash=self.policy['policyHash'])
            self.assertEqual(proof['verifiedNativeRows'],61)
            self.assertEqual(proof['verifiedNativeMinutes'],305)

    def test_dynamic_shorter_lookback_and_gap_retains_conservative_guards(self):
        data=tick_fixture();base=data['requirements'];pins=(replace(base.members[0],required_5m_bars=3),)
        cutoff=warmup.timestamp(base.cutoff);start=cutoff-timedelta(minutes=15)
        data['requirements']=warmup.DirectWarmupRequirements(**{**base.__dict__,'portfolio_id':'direct-short','portfolio_hash':'f'*64,
            'members':pins,'connected_at':warmup.iso(start-timedelta(seconds=5)), 'subscribed_at':warmup.iso(start-timedelta(seconds=3))})
        selected=data['frames'][-17:]
        for index,frame in enumerate(selected):
            values=[rows(frame)[0]]
            if index==0:
                event=start-timedelta(seconds=1);values[0]['STCK_CNTG_HOUR']=event.astimezone(warmup.KST).strftime('%H%M%S')
                frame['socketReceivedAt']=(event+timedelta(milliseconds=500)).isoformat()
            set_rows(frame,values)
            frame['receivedMonotonicNs']=1000000+int((warmup.timestamp(frame['socketReceivedAt'])-warmup.timestamp(selected[0]['socketReceivedAt'])).total_seconds())*1000000000
        data['frames']=selected
        result=warmup.build_selected_tick_warmup(**data)
        self.assertEqual(result['requiredBarTotal'],3)
        self.assertEqual(result['requiredMinuteTotal'],15)
        self.assertFalse(result['orderAuthorized'])
        del data['frames'][5]
        with self.assertRaises(ValueError):warmup.build_selected_tick_warmup(**data)

if __name__=='__main__':unittest.main()
