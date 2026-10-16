"""Bounded production PUBLIC history + public WS preparation, no account GET.

Caller supplies an exact state-owned plan and calls this outside all state,
route/controller locks. Missing actual reviewed label scope fails before OAuth.
"""
from datetime import datetime,timedelta,timezone
import hashlib,json,re,urllib.parse,urllib.request,urllib.error
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.native_rest_initialization import NativeRestInitializationInputs,build_native_rest_initialization,_time,KST
from trading_runtime.portfolio_runtime import required_warmup_bars
from trading_runtime.promotion_policy import validate_paper_promotion_policy
from trading_runtime.provider_backfill_kis_kr import ENDPOINT,TR_ID,PARAMS,validate_params,capture,parse,seal,minute_timestamp,calendar_objects
from trading_runtime.realtime_feeds import feeds_for_specs
from .account_profiles import saved_account_profiles
from .read_only_account import ReadOnlyBrokerReader,ReadOnlyTransport,ReadBoundaryError,ReadFailure,ORIGINS
from .live_adapters import split_kis_account
from .kis_prepared_native_feed import prepare_live_kis_feed

# This is an interpretation review, not a self-authorized local JSON scope.
# The only current independent proof covers one actual minute of 005930.
# Extending this list requires a new separately reviewed source change.
_CODE_PINNED_LABEL_REVIEWS=({'symbol':'005930','businessDate':'20261016',
    'scopeStart':'2026-10-16T03:56:00Z','scopeEnd':'2026-10-16T03:57:00Z',
    'evidenceHashes':(
        '59a8ec77c739c7e1070c83f94a0cefc8bfd52d70b756094b82f6fdb3635bff8f',
        '0d0689aba16f62e276f397804818e3ee758af52eda93bc6586f1a50ab314a1d9',
        '1aeda67b9e89931d2255d14d369696d8881d202eae7278f877ae2085a31227e5'),
    'files':{
        'tick-label-actual-independent-recalculation-v2.json':'59a8ec77c739c7e1070c83f94a0cefc8bfd52d70b756094b82f6fdb3635bff8f',
        'tick-label-probe-execution-v2.json':'0d0689aba16f62e276f397804818e3ee758af52eda93bc6586f1a50ab314a1d9',
        'received-public-frames.jsonl':'1aeda67b9e89931d2255d14d369696d8881d202eae7278f877ae2085a31227e5',
        'dated-minute-1.json':'0a44af9f2a06d18c7c171d011937599d46fa3cfeae2b89850b0e8e8f06b2b3ae',
        'dated-minute-2.json':'37d732d63b1a7df16b59c7ec70e1b89f01f7a14a944945c63c5208130b66ec84',
        'execution-summary.json':'ef637b82ff86931854e4a9dbf164f175eb16790b5cb84f0be72fc7039a1101e9',
        'http-clock-intervals.json':'420fd490c1467e575aafcf9de97ee08dc5ebd2772757ccab1318eada16f91e7e',
        'public-ticks.json':'de2d901a607076eacc82751ec28012208e6141fc58b918ab5a4d972bbff4b9df',
        'scoped-label-diagnostic.json':'fdc791fc617feba6b1402daa2ef028aa25953eb935cea54b786294029c3727ed'}},)


def _load_code_pinned_label_scopes(root):
    """Portable proof installation only; embedded absolute review paths ignored."""
    folder=Path(root)/'logs/native_initialization/label_proofs'
    for review in _CODE_PINNED_LABEL_REVIEWS:
        for name,digest in review['files'].items():
            try:
                with (folder/name).open('rb') as stream:
                    raw=stream.read(8*1024*1024+1)
            except OSError:
                raise ValueError('LIVE_NATIVE_INDEPENDENT_LABEL_PROOF_MISSING') from None
            if len(raw)>8*1024*1024 or hashlib.sha256(raw).hexdigest()!=digest:
                raise ValueError('LIVE_NATIVE_INDEPENDENT_LABEL_PROOF_CHANGED')
    return _CODE_PINNED_LABEL_REVIEWS


def _assert_labels_within_independent_review(labels,reviews):
    for label in labels.values():
        matches=[review for review in reviews if review['symbol']==label['symbol']
            and review['businessDate']==label['businessDate']
            and _time(review['scopeStart'])<=_time(label['scopeStart'])
            and _time(label['scopeEnd'])<=_time(review['scopeEnd'])
            and set(review['evidenceHashes'])<=set(label['evidenceHashes'])]
        if len(matches)!=1:
            raise ValueError('LIVE_NATIVE_INDEPENDENT_LABEL_REVIEW_SCOPE_UNPROVEN')


class PublicKisInitializationTransport(ReadOnlyTransport):
    """New closed whitelist; inherited account endpoints stay unreachable."""
    def request(self,provider,method,path,*,query=None,headers=None,body=None):
        if provider=='kis' and method=='POST' and path=='/oauth2/tokenP':
            return super().request(provider,method,path,query=query,headers=headers,body=body)
        if (provider!='kis' or method!='GET' or path!=ENDPOINT or body is not None
                or not isinstance(query,dict) or set(query)!=PARAMS or (headers or {}).get('tr_id')!=TR_ID
                or query.get('FID_COND_MRKT_DIV_CODE')!='J' or query.get('FID_PW_DATA_INCU_YN')!='Y'
                or query.get('FID_FAKE_TICK_INCU_YN')!=''
                or not re.fullmatch(r'[0-9]{6}',str(query.get('FID_INPUT_ISCD') or ''))
                or not re.fullmatch(r'[0-9]{8}',str(query.get('FID_INPUT_DATE_1') or ''))
                or not re.fullmatch(r'[0-9]{6}',str(query.get('FID_INPUT_HOUR_1') or ''))):
            raise ReadBoundaryError('LIVE_NATIVE_PUBLIC_ONLY_ENDPOINT_BLOCKED')
        url=ORIGINS['kis']+ENDPOINT+'?'+urllib.parse.urlencode(query)
        request=urllib.request.Request(url,headers=headers or {},method='GET')
        self.events.append({'provider':'kis','method':'GET','path':ENDPOINT,'status':'STARTED'})
        try:
            with self.opener.open(request,timeout=self.timeout) as response:
                if response.status!=200 or response.geturl()!=url:
                    raise ReadFailure('LIVE_NATIVE_PUBLIC_RESPONSE_REJECTED')
                raw=response.read(4*1024*1024+1)
                if len(raw)>4*1024*1024:
                    raise ReadFailure('LIVE_NATIVE_PUBLIC_RESPONSE_LIMIT')
                payload=json.loads(raw)
                self.events[-1]['status']='AVAILABLE'
                return payload,{}  # Authentication and arbitrary headers are not source evidence.
        except (OSError,ValueError,urllib.error.URLError):
            self.events[-1]['status']='UNAVAILABLE'
            raise ReadFailure('LIVE_NATIVE_PUBLIC_HISTORY_READ_FAILED') from None


def fetch_current_initialization(spec,fetcher,*,cutoff,objects,utc_clock):
    end=_time(cutoff);required=required_warmup_bars(spec)
    sub=SimpleNamespace(instrument_id=spec.instrument_id,symbol=spec.symbol,timeframe=spec.timeframe,exchange='KRX')
    cursor=end-timedelta(minutes=1);day=cursor.astimezone(KST).date();pages=[]
    for _ in range((required*5+119)//120):
        if cursor.astimezone(KST).date()!=day or cursor.astimezone(KST).hour<9:
            raise ValueError('LIVE_NATIVE_CURRENT_DAY_LOOKBACK_UNAVAILABLE')
        params={'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':spec.symbol,'FID_INPUT_DATE_1':day.strftime('%Y%m%d'),
            'FID_INPUT_HOUR_1':cursor.astimezone(KST).strftime('%H%M%S'),'FID_PW_DATA_INCU_YN':'Y','FID_FAKE_TICK_INCU_YN':''}
        validate_params(sub,params)
        requested=utc_clock().astimezone(timezone.utc).isoformat()
        payload=fetcher(sub,params)
        pages.append(capture(sub,params,payload,requested,utc_clock().astimezone(timezone.utc).isoformat()))
        rows=[bar for bar in parse(sub,pages,cutoff,objects=objects) if _time(bar.start_time).astimezone(KST).date()==day]
        if len(rows)>=required:
            return seal(sub,pages,through_end=cutoff,after_end=None,limit=required,objects=objects)[1]
        current=[minute_timestamp(row) for row in payload['output2'] if minute_timestamp(row).astimezone(KST).date()==day]
        if not current:
            raise ValueError('LIVE_NATIVE_CURRENT_DAY_PAGE_MISSING')
        next_cursor=min(current)-timedelta(minutes=1)
        if next_cursor>=cursor:
            raise ValueError('LIVE_NATIVE_CURRENT_DAY_CURSOR_NOT_ADVANCING')
        cursor=next_cursor
    raise ValueError('LIVE_NATIVE_CURRENT_DAY_BOUNDED_PAGES_INCOMPLETE')


def prepare_live_kis_native(root,*,plan,binding_reader,preparation_guard,settings,
        utc_clock=None,monotonic_clock=None,maximum_seconds=660,feed_factory=None,transport=None):
    """State-owned plan only; serialized request data is never accepted as it."""
    utc_clock=utc_clock or (lambda:datetime.now(timezone.utc))
    loaded=plan['loaded'];specs=tuple(loaded.specs)
    scope=plan['runtimeScope'];context=plan['context']
    policy=validate_paper_promotion_policy(plan['promotionPolicy'],require_promotion=True)
    # The full policy must come from existing exact validated qualification.
    # No global/default policy is created or substituted for a missing pin.
    labels=plan.get('labelAcceptances')
    if labels is None:
        path=Path(root)/'logs/native_initialization/label_acceptances.json'
        try:
            labels=json.loads(path.read_text(encoding='utf-8'))
        except (OSError,ValueError):
            raise ValueError('LIVE_NATIVE_REVIEWED_LABEL_SCOPE_UNPROVEN') from None
    expected={spec.strategy_instance_id for spec in specs}
    now=utc_clock().astimezone(timezone.utc)
    latest=now.replace(minute=now.minute-now.minute%5,second=0,microsecond=0)
    if type(labels) is not dict or set(labels)!=expected:
        raise ValueError('LIVE_NATIVE_REVIEWED_LABEL_SCOPE_REQUIRED')
    for spec in specs:
        label=labels[spec.strategy_instance_id]
        if (not isinstance(label,dict) or label.get('symbol')!=spec.symbol or label.get('reviewStatus')!='SCOPED_REVIEWED'
                or label.get('businessDate')!=now.astimezone(KST).strftime('%Y%m%d')
                or label.get('contentHash')!=stable_sha256({k:v for k,v in label.items() if k!='contentHash'})
                or _time(label['scopeStart'])>latest-timedelta(minutes=5*required_warmup_bars(spec))
                or _time(label['scopeEnd'])<latest):
            raise ValueError('LIVE_NATIVE_REVIEWED_LABEL_SCOPE_UNPROVEN')
    _assert_labels_within_independent_review(labels,_load_code_pinned_label_scopes(root))
    profiles=[row for row in saved_account_profiles(settings) if row.get('brokerId')=='kis']
    if len(profiles)!=1 or any(profiles[0].get(k)!=scope.get(k) for k in ('accountFingerprint','credentialConfigurationHash','accountBindingId')):
        raise ValueError('LIVE_NATIVE_CURRENT_SAVED_ACCOUNT_CHANGED')
    reader=ReadOnlyBrokerReader(settings,transport=transport or PublicKisInitializationTransport())
    reader._credentials('kis')
    if settings.get('KIS_ENV','').lower() not in {'','real','live'}:
        raise ValueError('LIVE_NATIVE_PRODUCTION_PUBLIC_DATA_REQUIRED')
    def fetcher(sub,params):
        validate_params(sub,params);preparation_guard()
        token=reader._kis_token();reader._pace_kis()
        return reader._get('kis',ENDPOINT,query=params,headers={'authorization':'Bearer '+token,
            'appkey':settings['KIS_APP_KEY'],'appsecret':settings['KIS_APP_SECRET'],'tr_id':TR_ID,'custtype':'P'})[0]
    objects=plan.get('calendarObjects') or calendar_objects()
    def initialize(cutoff):
        segments={spec.strategy_instance_id:fetch_current_initialization(spec,fetcher,cutoff=cutoff,objects=objects,utc_clock=utc_clock) for spec in specs}
        sources={spec.strategy_id:{key:value for key,value in spec.artifact.items() if key not in
            {'strategyInstance','portfolioExecutionPolicy','portfolioPermissions','historicalWarmupReceipt'}} for spec in specs}
        instances={spec.artifact['strategyInstance']['verifiedStrategyInstance']['instanceId']:
            spec.artifact['strategyInstance']['verifiedStrategyInstance'] for spec in specs}
        return build_native_rest_initialization(expected=NativeRestInitializationInputs(specs,sources,instances,loaded.payload,
            policy,objects,segments,labels,utc_clock().astimezone(timezone.utc).isoformat()))
    cano,product=split_kis_account(settings['KIS_ACCOUNT_NO'],settings['KIS_ACCOUNT_PRODUCT_CODE'])
    factory=feed_factory or feeds_for_specs
    feeds=tuple(factory(specs,prefer_kis=True,kis_demo=False,kis_app_key=settings['KIS_APP_KEY'],
        kis_app_secret=settings['KIS_APP_SECRET'],kis_account_id=cano+'-'+product,
        kis_websocket_owner_id='live-native-preparation:'+uuid4().hex))
    return prepare_live_kis_feed(feeds=feeds,specs=specs,context=context,runtime_scope=scope,
        deployment_manifest_hash=plan['deploymentManifestHash'],initialize=initialize,binding_reader=binding_reader,
        preparation_guard=preparation_guard,utc_clock=utc_clock,monotonic_clock=monotonic_clock,
        maximum_seconds=maximum_seconds,portfolio_path=plan['portfolioPath'],artifact_root=plan['artifactRoot'])
