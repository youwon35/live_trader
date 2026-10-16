"""Public KIS preparation and one-shot Live handoff; no order authority API."""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import threading,time

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.continuous_runtime import ClosedBar
from trading_runtime.market_clock_integrity import market_clock_integrity_failure_codes
from trading_runtime.market_source_integrity import source_integrity_failure_codes
from trading_runtime.native_rest_initialization import _time,KST
from .kis_native_initialization import build_live_initialization,validate_live_initialization,LiveKisNativeInitializedEngine


class LivePreparedKisFeed:
    def __init__(self,*,feeds,specs,receipt,binding_reader,utc_clock,monotonic_clock,
            portfolio_path=None,artifact_root=None):
        self.feeds,self.specs=tuple(feeds),tuple(specs)
        self.receipt=deepcopy(receipt)
        self._receipt_hash=stable_sha256(receipt)
        self._binding_reader=binding_reader
        self._binding={k:deepcopy(receipt[k]) for k in ('context','runtimeScope','deploymentManifestHash')}
        self._utc_clock,self._monotonic_clock=utc_clock,monotonic_clock
        self._prepared_monotonic=monotonic_clock()
        self._native_confirmation_monotonic=None
        self._activated_monotonic=None
        self._snapshots=tuple((feed,feed.socket,feed._clock_generation) for feed in self.feeds)
        self._lock=threading.RLock()
        self._revoked=threading.Event()
        self._state='PREPARED'
        self._session=None
        self._activation_reader=None
        self._running_confirmed=False
        self.portfolio_path=Path(portfolio_path).resolve() if portfolio_path is not None else None
        self.artifact_root=Path(artifact_root).resolve() if artifact_root is not None else None
        self._assert_public()

    def __reduce__(self):
        raise TypeError('Live prepared native sockets cannot be serialized or cold-restored')

    def revoke(self,reason='REVOKED'):
        # Safe while a caller holds state/route locks: no close/join/I/O here.
        self._revoked.set()

    def close(self):
        self.revoke()
        for feed in self.feeds:
            feed.disconnect()

    def _assert_public(self):
        if self._revoked.is_set() or stable_sha256(self.receipt)!=self._receipt_hash:
            raise RuntimeError('LIVE_NATIVE_PREPARED_REVOKED_OR_RECEIPT_CHANGED')
        for feed,socket,generation in self._snapshots:
            if feed.socket is not socket or socket is None or feed._clock_generation!=generation:
                raise RuntimeError('LIVE_NATIVE_PREPARED_SOCKET_OR_GENERATION_CHANGED')
            ready=feed.readiness_snapshot()
            if ready.get('ready') is not True or ready.get('connected') is not True or ready.get('transport')!='kis-websocket':
                raise RuntimeError('LIVE_NATIVE_PREPARED_PUBLIC_FEED_NOT_READY')
            if market_clock_integrity_failure_codes({'marketClockIntegrity':feed.market_clock_integrity_snapshot()}):
                raise RuntimeError('LIVE_NATIVE_PREPARED_CLOCK_INTEGRITY_FAILED')
            source=getattr(feed,'market_source_integrity_snapshot',None)
            if hasattr(feed,'market_source_integrity_snapshot') and not callable(source):
                raise RuntimeError('LIVE_NATIVE_PREPARED_SOURCE_READER_INVALID')
            if callable(source) and source_integrity_failure_codes({'sourceIntegrity':source()}):
                raise RuntimeError('LIVE_NATIVE_PREPARED_SOURCE_INTEGRITY_FAILED')

    def assert_prepared(self):
        with self._lock:
            if self._state!='PREPARED':
                raise RuntimeError('LIVE_NATIVE_PREPARED_ALREADY_CONSUMED')
            self._assert_public()
            anchor=self._prepared_monotonic if self._native_confirmation_monotonic is None else self._native_confirmation_monotonic
            elapsed=self._monotonic_clock()-anchor
            if not 0<=elapsed<=45:
                raise RuntimeError('LIVE_NATIVE_PREPARED_NATIVE_CONFIRMATION_EXPIRED')
            if self._binding_reader()!=self._binding:
                raise RuntimeError('LIVE_NATIVE_PREPARED_ACCOUNT_SOURCE_OR_SCOPE_CHANGED')

    def begin_native_confirmation_window(self):
        """One actual 45s confirmation window, after <=15s START account read.

        Raw public capture/receipt/cutoff and bind-to-claim lease never change.
        This method grants no native confirmation or execution authority.
        """
        with self._lock:
            self.assert_prepared()
            if self._native_confirmation_monotonic is not None:
                raise RuntimeError('LIVE_NATIVE_PREPARED_CONFIRMATION_WINDOW_ALREADY_STARTED')
            current=self._monotonic_clock()
            now=self._utc_clock().astimezone(timezone.utc)
            if (not 0<=current-self._prepared_monotonic<=15
                    or now.replace(minute=now.minute-now.minute%5,second=0,microsecond=0)!=_time(self.receipt['cutoff'])):
                raise RuntimeError('LIVE_NATIVE_PREPARED_START_PREFLIGHT_EXPIRED')
            self._native_confirmation_monotonic=current

    def bind_activation(self,*,session,activation_reader):
        with self._lock:
            self.assert_prepared()
            scope=self.receipt['runtimeScope']
            metadata=session.get('metadata') or {}
            created=_time(session['createdAt'])
            latest=created.replace(minute=created.minute-created.minute%5,second=0,microsecond=0)
            if (session.get('lifecycle') not in {'STARTING','RUNNING'} or session.get('mode')!=self.receipt['context']['mode']
                    or session.get('profile')!='stock' or not session.get('sessionId') or not session.get('runtimeInstanceId')
                    or session.get('deploymentManifestHash')!=self.receipt['deploymentManifestHash']
                    or session.get('deploymentId')!=scope['deploymentId']
                    or any(metadata.get(k)!=scope[k] for k in ('runtimeScopeId','brokerId','accountFingerprint',
                        'credentialConfigurationHash','accountBindingId','capitalCurrency','profileId','deploymentId'))
                    or latest!=_time(self.receipt['cutoff'])
                    or created<_time(self.receipt['publicInitializationData']['admissionRequestedAt'])
                    or activation_reader()!=session):
                raise RuntimeError('LIVE_NATIVE_PREPARED_EXACT_ACTUAL_MANUAL_SESSION_REQUIRED')
            self._session=deepcopy(session)
            self._activation_reader=activation_reader
            self._activated_monotonic=self._monotonic_clock()
            self._state='ACTIVATED'

    def confirm_running(self,*,session,activation_reader):
        with self._lock:
            if self._state!='ACTIVATED' or self._running_confirmed or self._session is None:
                raise RuntimeError('LIVE_NATIVE_PREPARED_ONE_RUNNING_CONFIRMATION_REQUIRED')
            self._assert_public()
            expected={**self._session,'lifecycle':'RUNNING'}
            if (session!=expected or activation_reader()!=session or self._binding_reader()!=self._binding):
                raise RuntimeError('LIVE_NATIVE_PREPARED_EXACT_RUNNING_CAS_REQUIRED')
            self._assert_claim_window()
            self._session=deepcopy(session)
            self._activation_reader=activation_reader
            self._running_confirmed=True
            self.assert_current()

    def _assert_claim_window(self):
        # The original activation lease is never renewed by a later RUNNING CAS.
        elapsed=self._monotonic_clock()-self._activated_monotonic
        if not 0<=elapsed<=15:
            raise RuntimeError('LIVE_NATIVE_PREPARED_ACTIVATED_CLAIM_EXPIRED')
        now=self._utc_clock().astimezone(timezone.utc)
        latest=now.replace(minute=now.minute-now.minute%5,second=0,microsecond=0)
        if latest!=_time(self.receipt['cutoff']):
            raise RuntimeError('LIVE_NATIVE_PREPARED_ACTIVATION_PREDECESSOR_EXPIRED')

    def assert_activation_claimable(self):
        with self._lock:
            if self._state!='ACTIVATED' or not self._running_confirmed:
                raise RuntimeError('LIVE_NATIVE_PREPARED_RUNNING_ACTIVATION_REQUIRED')
            self._assert_claim_window()
            self.assert_current()

    def assert_current(self):
        with self._lock:
            if self._state not in {'ACTIVATED','SUPERVISOR'}:
                raise RuntimeError('LIVE_NATIVE_PREPARED_NOT_MANUALLY_ACTIVATED')
            self._assert_public()
            if self._activation_reader()!=self._session or self._binding_reader()!=self._binding:
                raise RuntimeError('LIVE_NATIVE_PREPARED_SESSION_ACCOUNT_OR_CONFIG_CHANGED')
            now=self._utc_clock().astimezone(KST)
            cutoff=_time(self.receipt['cutoff']).astimezone(KST)
            if now.date()!=cutoff.date() or (now.hour,now.minute)>=(15,20):
                raise RuntimeError('LIVE_NATIVE_PREPARED_PHASE_UNSUPPORTED_REQUIRE_DRAINING')

    def claim_for_supervisor(self,*,feeds,engine):
        with self._lock:
            if self._state!='ACTIVATED' or not self._running_confirmed or len(feeds)!=len(self.feeds) or any(a is not b for a,b in zip(feeds,self.feeds)):
                raise RuntimeError('LIVE_NATIVE_PREPARED_ONE_SHOT_SAME_FEED_REQUIRED')
            if (not isinstance(engine,LiveKisNativeInitializedEngine) or engine.live_native_initialization!=self.receipt
                    or engine.mode!=self.receipt['context']['mode'] or engine.bar_count or engine.decision_count
                    or not engine.restore_context_reconciled or engine.restore_order_blocked):
                raise RuntimeError('LIVE_NATIVE_PREPARED_RECONCILED_INITIALIZED_ENGINE_REQUIRED')
            self._assert_claim_window()
            self.assert_current()
            self._state='SUPERVISOR'
            return {feed.provider_id:True for feed in self.feeds}


def prepare_live_kis_feed(*,feeds,specs,context,runtime_scope,deployment_manifest_hash,initialize,
        binding_reader,preparation_guard,utc_clock=None,monotonic_clock=None,maximum_seconds=660,
        portfolio_path=None,artifact_root=None):
    """All public I/O belongs OUTSIDE state/route/controller/engine locks."""
    utc_clock=utc_clock or (lambda:datetime.now(timezone.utc))
    monotonic_clock=monotonic_clock or time.monotonic
    feeds,specs=tuple(feeds),tuple(specs)
    if len(feeds)!=1 or feeds[0].provider_id!='kis' or not specs or not 0<maximum_seconds<=660:
        raise ValueError('LIVE_NATIVE_PREPARATION_ONE_KIS_FEED_BOUNDED_REQUIRED')
    expected={(s.provider,s.instrument_id,s.timeframe) for s in specs}
    if len(expected)!=len(specs):
        raise ValueError('LIVE_NATIVE_PREPARATION_DISTINCT_STREAMS_REQUIRED')
    preparation_guard()
    begin=monotonic_clock();observed={};connected=[];sockets=[]
    def assert_live_generation():
        if not 0<=monotonic_clock()-begin<=maximum_seconds:
            raise RuntimeError('LIVE_NATIVE_PREPARATION_TIMEOUT')
        for feed,socket,generation in sockets:
            if socket is None or generation<1 or feed.socket is not socket or feed._clock_generation!=generation:
                raise RuntimeError('LIVE_NATIVE_PREPARATION_GENERATION_CHANGED')
    try:
        for feed in feeds:
            preparation_guard();feed.connect();connected.append(feed)
            sockets.append((feed,feed.socket,feed._clock_generation))
        while True:
            preparation_guard();assert_live_generation()
            for feed in feeds:
                events=tuple(feed.poll(.2));assert_live_generation()
                for event in events:
                    if not isinstance(event,ClosedBar):
                        continue  # No engine/decision/intent exists before native confirmation.
                    key=event.provider,event.instrument_id,event.timeframe
                    if key not in expected or event.source_provider!='kis' or event.evidence_class not in {'','NATIVE_FORWARD'}:
                        raise RuntimeError('LIVE_NATIVE_PREPARATION_FOREIGN_OR_SYNTHETIC_EVENT')
                    prior=observed.get(key)
                    if prior is not None and (_time(event.end_time)<_time(prior.end_time) or
                            event.end_time==prior.end_time and event.to_dict()!=prior.to_dict()):
                        raise RuntimeError('LIVE_NATIVE_PREPARATION_CONTRADICTORY_OR_REORDERED_EVENT')
                    observed[key]=event
                if market_clock_integrity_failure_codes({'marketClockIntegrity':feed.market_clock_integrity_snapshot()}):
                    raise RuntimeError('LIVE_NATIVE_PREPARATION_CLOCK_FAILED')
                source=getattr(feed,'market_source_integrity_snapshot',None)
                if hasattr(feed,'market_source_integrity_snapshot') and not callable(source):
                    raise RuntimeError('LIVE_NATIVE_PREPARATION_SOURCE_READER_INVALID')
                if callable(source) and source_integrity_failure_codes({'sourceIntegrity':source()}):
                    raise RuntimeError('LIVE_NATIVE_PREPARATION_SOURCE_FAILED')
            if set(observed)!=expected or len({b.end_time for b in observed.values()})!=1:
                continue
            cutoff=next(iter(observed.values())).end_time
            data=initialize(cutoff)
            assert_live_generation();preparation_guard()
            receipt=build_live_initialization(data,context=context,runtime_scope=runtime_scope,
                deployment_manifest_hash=deployment_manifest_hash,specs=specs)
            now=utc_clock().astimezone(timezone.utc)
            latest=now.replace(minute=now.minute-now.minute%5,second=0,microsecond=0)
            if _time(cutoff)!=latest or data['cutoff']!=cutoff:
                raise RuntimeError('LIVE_NATIVE_PREPARATION_PREDECESSOR_EXPIRED')
            for member in data['members']:
                identity=member['specIdentity'];native=observed[(identity['provider'],identity['instrumentId'],identity['timeframe'])].to_dict()
                if any(native[k]!=member['historyRows'][-1][k] for k in ('startTime','endTime','open','high','low','close','volume')):
                    raise RuntimeError('LIVE_NATIVE_PREPARATION_NATIVE_REST_PREDECESSOR_CONTRADICTION')
            return LivePreparedKisFeed(feeds=feeds,specs=specs,receipt=receipt,binding_reader=binding_reader,
                utc_clock=utc_clock,monotonic_clock=monotonic_clock,portfolio_path=portfolio_path,artifact_root=artifact_root)
    except BaseException:
        for feed in connected:
            feed.disconnect()
        raise
