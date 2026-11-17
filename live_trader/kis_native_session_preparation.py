"""Separate current-source session preparation on one original public socket.

Only public chart GET/OAuth and public trade WS are available here. The actual
manual confirmation, account restore, scope CAS and final order gates remain in
state-owned callers. The old regular scientific preparation is not relaxed.
"""
from __future__ import annotations
from datetime import datetime,timezone,timedelta
from pathlib import Path
from types import SimpleNamespace
import hashlib,time
from uuid import uuid4
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.continuous_runtime import ClosedBar
from trading_runtime.market_data import parse_timestamp,timeframe_seconds,canonical_timestamp
from trading_runtime.native_session_initialization import build_session_initialization,validate_session_initialization,spec_bindings
from trading_runtime.provider_backfill_kis_sessions import fetch_segment,_latest_end
from trading_runtime.portfolio_runtime import required_warmup_bars
from trading_runtime.stock_session_profile import profile_for_spec,profile_hash,session_window_at
from trading_runtime.realtime_feeds import feeds_for_specs,FeedSubscription,KisWebSocketClosedBarFeed,_runtime_spec_kis_exchange
from trading_runtime.kis_profile_feed import make_readonly_kis_profile_history_fetcher
from trading_runtime.kis_session_routes import kis_websocket_route
from .account_profiles import saved_account_profiles
from .live_adapters import split_kis_account
from .kis_native_initialization import build_live_initialization
from .kis_prepared_native_feed import LivePreparedKisFeed


def needs_session_initialization(specs):
    return any((getattr(spec,'parameters',{}) or {}).get('stockSessionProfile') is not None or
        (profile_for_spec(spec) or {}).get('market')=='US' for spec in specs if spec.provider=='kis')


def capture_scope(feed,context,scope):
    if (feed.provider_id!='kis' or feed.demo or feed.private_tr_key or not feed.kis_account_id
            or not feed.websocket_owner_id or not feed.app_key or feed.socket is None or feed._clock_generation<1):
        raise ValueError('LIVE_NATIVE_SESSION_SAME_PUBLIC_ACCOUNT_OWNER_REQUIRED')
    return {'mode':'LIVE','accountFingerprint':scope['accountFingerprint'],
        'credentialConfigurationHash':scope['credentialConfigurationHash'],'runtimeScopeId':context['runtimeScopeId'],
        'profileId':context['profileId'],'deploymentId':context['deploymentId'],
        'appKeyIdHash':hashlib.sha256(feed.app_key.encode()).hexdigest(),
        'websocketOwnerId':feed.websocket_owner_id,'generation':feed._clock_generation}


def _frame_matches(text,route):
    parts=text.split('|',3)
    if len(parts)!=4 or parts[1]!=route['trId']:return False
    # Keep the complete original multi-record wire text, never synthesize a
    # single-record replacement. Unrelated records are ignored by the decoder.
    fields=parts[3].split('^')
    try:count=int(parts[2])
    except ValueError:return False
    if count<1 or len(fields)%count:return False
    width=len(fields)//count
    return any(fields[i*width]==route['trKey'] for i in range(count))


def prepare_live_kis_session(root,*,plan,binding_reader,preparation_guard,settings,
        utc_clock=None,monotonic_clock=None,maximum_seconds=660,feed_factory=None,
        history_fetcher=None,daily_reference_fetcher=None,public_progress=None):
    utc_clock=utc_clock or (lambda:datetime.now(timezone.utc));monotonic_clock=monotonic_clock or time.monotonic
    specs=tuple(plan['loaded'].specs);scope=plan['runtimeScope'];context=plan['context']
    if not specs or len(specs)>32 or any(spec.provider!='kis' or spec.broker_id!='kis' for spec in specs):
        raise ValueError('LIVE_NATIVE_SESSION_EXACT_KIS_PORTFOLIO_REQUIRED')
    if not 0<maximum_seconds<=660:raise ValueError('LIVE_NATIVE_SESSION_BOUNDED_CAPTURE_REQUIRED')
    profiles=[row for row in saved_account_profiles(settings) if row.get('brokerId')=='kis']
    if len(profiles)!=1 or any(profiles[0].get(k)!=scope.get(k) for k in ('accountFingerprint','credentialConfigurationHash','accountBindingId')):
        raise ValueError('LIVE_NATIVE_CURRENT_SAVED_ACCOUNT_CHANGED')
    if str(settings.get('KIS_ENV','')).lower() not in {'','real','live'}:raise ValueError('LIVE_NATIVE_PRODUCTION_PUBLIC_DATA_REQUIRED')
    if len({profile_hash(profile_for_spec(spec)) for spec in specs})!=1:
        raise ValueError('LIVE_NATIVE_SESSION_DIFFERENT_PROFILES_REQUIRE_SEPARATE_DEPLOYMENTS')
    now=utc_clock();streams={};replays={};probe_bars={};routes={};frames={};subscriptions={};clocks={}
    for spec in specs:
        spec_bindings(spec)  # Independent source/instance contract before IO.
        profile=profile_for_spec(spec)
        if profile is None or not session_window_at(profile,now):raise ValueError('LIVE_NATIVE_SESSION_SELECTED_WINDOW_NOT_OPEN')
        if profile['segments']==['DAY']:raise ValueError('KIS_US_DAY_REALTIME_WEBSOCKET_NOT_OFFICIALLY_VERIFIED_USE_RESEARCH_ONLY')
        width=timeframe_seconds(spec.timeframe)
        if not 60<=width<=3600 or width%60:raise ValueError('LIVE_NATIVE_SESSION_MINUTE_TIMEFRAME_REQUIRED')
        sid=spec.strategy_instance_id;exchange=_runtime_spec_kis_exchange(spec)
        subscriptions[sid]=FeedSubscription(spec.instrument_id,spec.symbol,spec.timeframe,exchange=exchange,stock_session_profile=profile)
        routes[sid]=kis_websocket_route(profile,exchange=exchange,symbol=spec.symbol)
        clock=[now.timestamp()];clocks[sid]=clock
        replays[sid]=KisWebSocketClosedBarFeed([FeedSubscription(spec.instrument_id,spec.symbol,'1m',exchange=exchange,stock_session_profile=profile)],
            app_key='',app_secret='',artifact_provider_id='kis',now=lambda clock=clock:clock[0])
        frames[sid]=[]
    expected={k:plan[k] for k in ('context','runtimeScope','deploymentManifestHash')}
    fetcher=history_fetcher or make_readonly_kis_profile_history_fetcher(app_key=settings['KIS_APP_KEY'],app_secret=settings['KIS_APP_SECRET'],demo=False)
    def guarded_fetch(sub,params):
        guard();value=fetcher(sub,params);guard();return value
    cano,product=split_kis_account(settings['KIS_ACCOUNT_NO'],settings['KIS_ACCOUNT_PRODUCT_CODE'])
    feeds=tuple((feed_factory or feeds_for_specs)(specs,prefer_kis=True,kis_demo=False,kis_app_key=settings['KIS_APP_KEY'],
        kis_app_secret=settings['KIS_APP_SECRET'],kis_account_id=cano+'-'+product,
        kis_websocket_owner_id='live-native-session:'+uuid4().hex,profile_history_fetcher=guarded_fetch))
    if len(feeds)!=1 or not isinstance(feeds[0],KisWebSocketClosedBarFeed):raise ValueError('LIVE_NATIVE_SESSION_ONE_ORIGINAL_PUBLIC_SOCKET_REQUIRED')
    feed=feeds[0];started=monotonic_clock();snapshot=None;captured_scope=None;total=0
    def guard():
        preparation_guard()
        if binding_reader()!=expected:raise ValueError('LIVE_NATIVE_SESSION_STATE_SCOPE_CHANGED')
        if not 0<=monotonic_clock()-started<=maximum_seconds:raise ValueError('LIVE_NATIVE_SESSION_CAPTURE_TIMEOUT')
        if snapshot is not None and (feed.socket is not snapshot[0] or feed._clock_generation!=snapshot[1] or capture_scope(feed,context,scope)!=captured_scope):
            raise ValueError('LIVE_NATIVE_SESSION_SOCKET_OWNER_OR_GENERATION_CHANGED')
    def observe(text,*,received_epoch,received_monotonic,generation):
        nonlocal total
        guard();total+=len(text.encode('utf-8'))
        if total>16*1024*1024:raise ValueError('LIVE_NATIVE_SESSION_CAPTURE_BYTE_LIMIT')
        for sid,route in routes.items():
            if not _frame_matches(text,route):continue
            row={'text':text,'receivedAt':datetime.fromtimestamp(received_epoch,timezone.utc).isoformat(timespec='microseconds'),
                'receivedMonotonic':received_monotonic,'generation':generation}
            frames[sid].append(row)
            if len(frames[sid])>10000:raise ValueError('LIVE_NATIVE_SESSION_CAPTURE_FRAME_LIMIT')
            replay=replays[sid];clocks[sid][0]=received_epoch;replay._connected_at=feed._connected_at
            replay._clock_generation=generation;replay._socket_receipt_monotonic=received_monotonic
            events=[];replay._consume_socket_frame(text,events,received_epoch=received_epoch)
            for event in events:
                if isinstance(event,ClosedBar):probe_bars[sid]=event
    try:
        guard();feed.set_profile_tick_observer(observe);feed.connect()
        snapshot=(feed.socket,feed._clock_generation);captured_scope=capture_scope(feed,context,scope)
        while True:
            guard();feed.poll(.2);guard()
            if public_progress:public_progress({'phase':'CAPTURING_NATIVE_TICKS','frameCount':sum(map(len,frames.values())),'elapsedSeconds':max(0,monotonic_clock()-started),'cutoff':''})
            if len(probe_bars)!=len(specs):continue
            streams={}
            for spec in specs:
                guard();sid=spec.strategy_instance_id;probe=probe_bars[sid];sub=subscriptions[sid]
                stamp=utc_clock();end=_latest_end(sub.stock_session_profile,stamp,timeframe_seconds(spec.timeframe))
                history_bars,history=fetch_segment(sub,guarded_fetch,now=stamp,through_end=end,limit=required_warmup_bars(spec),observed_clock=lambda:utc_clock().timestamp())
                from trading_runtime.stock_indicator_basis import adjusted_profile_requires_proof
                indicator_evidence=None
                if adjusted_profile_requires_proof(spec):
                    from trading_runtime.kis_adjusted_history import attach_kis_history_adjustments
                    daily=daily_reference_fetcher or getattr(fetcher,'daily_reference',None)
                    if not callable(daily):raise ValueError('INDICATOR_PRICE_BASIS_REVALIDATION_REQUIRED: KIS_DAILY_REFERENCE_TRANSPORT_MISSING')
                    def guarded_daily(subscription,request):
                        guard();value=daily(subscription,request);guard();return value
                    _paired,pair_receipt=attach_kis_history_adjustments(sub,history_bars,history,guarded_daily,
                        observed_clock=lambda:utc_clock().timestamp(),allow_unadjusted_when_pending=True)
                    indicator_evidence=pair_receipt.get('priceAdjustmentEvidence')
                probe_sub=FeedSubscription(sub.instrument_id,sub.symbol,'1m',exchange=sub.exchange,stock_session_profile=sub.stock_session_profile)
                _bars,proof=fetch_segment(probe_sub,guarded_fetch,now=utc_clock(),after_end=probe.start_time,through_end=probe.end_time,limit=1,observed_clock=lambda:utc_clock().timestamp())
                streams[sid]={'strategyInstanceId':sid,**spec_bindings(spec),'instrumentId':spec.instrument_id,'symbol':spec.symbol,
                    'timeframe':spec.timeframe,'stockSessionProfile':sub.stock_session_profile,'exchange':sub.exchange,
                    'windows':history['windows'],'historyReceipt':history,'probeClosedBar':probe.to_dict(),'probeRestReceipt':proof,
                    'rawFrames':list(frames[sid]),'connectedAt':canonical_timestamp(datetime.fromtimestamp(feed._connected_at,timezone.utc))}
                if indicator_evidence is not None:streams[sid]['indicatorBasisEvidence']=indicator_evidence
            guard();data=build_session_initialization(specs=specs,capture_scope=captured_scope,streams=streams,
                portfolio_id=plan['loaded'].portfolio_id,portfolio_hash=plan['loaded'].portfolio_hash)
            validate_session_initialization(data,specs=specs,capture_scope=capture_scope(feed,context,scope))
            feed.set_profile_tick_observer(None)
            # Drain queued originals only; this never creates engine decisions.
            feed.poll(.01);guard()
            receipt=build_live_initialization(data,context=context,runtime_scope=scope,deployment_manifest_hash=plan['deploymentManifestHash'],specs=specs)
            prepared=LivePreparedKisFeed(feeds=feeds,specs=specs,receipt=receipt,binding_reader=binding_reader,
                utc_clock=utc_clock,monotonic_clock=monotonic_clock,portfolio_path=plan['portfolioPath'],artifact_root=plan['artifactRoot'])
            prepared._session_capture_scope_reader=lambda:capture_scope(feed,context,scope)
            return prepared
    except BaseException:
        feed.set_profile_tick_observer(None);feed.disconnect();raise
