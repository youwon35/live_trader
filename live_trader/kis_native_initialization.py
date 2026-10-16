"""Live-scoped PUBLIC initialization data, never execution authority.

Paper sessions/positions/evidence are not imported. The shared scientific raw
data validator is reused; native confirmation, broker/owned-capital restore
attestation and every final order gate remain owned by the existing state path.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import re
from collections.abc import Mapping

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.continuous_runtime import PortfolioRuntimeEngine
from trading_runtime.native_rest_initialization import (
    validate_bound_native_initialization, assert_initialization_matches_runtime,
    initialization_bars, _time, KST,
)
from .kis_scoped_operational_authority import exact_context
from .runtime_scope import scope_from_mapping

SCHEMA = 'live-kis-native-initialization-data-v1'
_SHA = re.compile(r'^[a-f0-9]{64}$')
_FIELDS = {'schemaVersion','context','contextHash','runtimeScope','deploymentManifestHash',
    'publicInitializationData','publicInitializationHash','cutoff','portfolioId','portfolioHash',
    'initializationOnly','paperSessionImported','executionAuthorityGranted','promotionEvidence','contentHash'}


def build_live_initialization(public_data, *, context, runtime_scope, deployment_manifest_hash, specs):
    context = exact_context(context)
    scope = scope_from_mapping(runtime_scope)
    public_data = validate_bound_native_initialization(public_data, portfolio_hash=specs[0].portfolio_hash,
        promotion_policy_hash=public_data['promotionPolicyHash'])
    assert_initialization_matches_runtime(public_data, specs)
    if public_data.get('schemaVersion') == 'source-bound-native-tick-initialization-v1':
        capture = public_data['captureManifest']['scope']
        if capture['mode'] != 'LIVE' or (capture['accountFingerprint'], capture['credentialConfigurationHash'], capture['runtimeScopeId']) != (
                scope.account_fingerprint, scope.credential_configuration_hash, scope.runtime_id):
            raise ValueError('LIVE_NATIVE_TICK_PAPER_OR_FOREIGN_ACCOUNT_CAPTURE_REJECTED')
    if (scope.broker_id != 'kis' or scope.capital_currency != 'KRW' or not scope.account_binding_id
            or context['capitalCurrency'] != 'KRW' or context['runtimeScopeId'] != scope.runtime_id
            or context['deploymentId'] != scope.deployment_id or context['profileId'] != scope.profile_id
            or context['accountFingerprint'] != scope.account_fingerprint
            or context['credentialConfigurationHash'] != scope.credential_configuration_hash
            or not _SHA.fullmatch(deployment_manifest_hash)
            or public_data['portfolioId'] != context['portfolioId']):
        raise ValueError('LIVE_NATIVE_INITIALIZATION_ACCOUNT_RUNTIME_SOURCE_BINDING_INVALID')
    body = {'schemaVersion':SCHEMA,'context':deepcopy(context),'contextHash':stable_sha256(context),
        'runtimeScope':scope.to_dict(),'deploymentManifestHash':deployment_manifest_hash,
        'publicInitializationData':public_data,'publicInitializationHash':public_data['contentHash'],
        'cutoff':public_data['cutoff'],'portfolioId':public_data['portfolioId'],'portfolioHash':public_data['portfolioHash'],
        'initializationOnly':True,'paperSessionImported':False,'executionAuthorityGranted':False,'promotionEvidence':False}
    return {**body,'contentHash':stable_sha256(body)}


def validate_live_initialization(value, *, context, runtime_scope, deployment_manifest_hash, specs):
    if (not isinstance(value,Mapping) or set(value)!=_FIELDS
            or value.get('schemaVersion') != SCHEMA or type(value.get('contentHash')) is not str
            or stable_sha256({k:v for k,v in value.items() if k!='contentHash'}) != value['contentHash']):
        raise ValueError('LIVE_NATIVE_INITIALIZATION_SCHEMA_OR_HASH_INVALID')
    expected = build_live_initialization(value['publicInitializationData'],context=context,runtime_scope=runtime_scope,
        deployment_manifest_hash=deployment_manifest_hash,specs=specs)
    if value != expected:
        raise ValueError('LIVE_NATIVE_INITIALIZATION_EXACT_SCOPE_CHANGED')
    return deepcopy(expected)


class LiveKisNativeInitializedEngine(PortfolioRuntimeEngine):
    """A separate Live consumer; the shared PAPER-only entry stays unchanged."""
    def __init__(self,*args,**kwargs):
        self.live_native_initialization = None
        self._live_initialization_content_hash = None
        super().__init__(*args,**kwargs)

    def _restore(self):
        if self.state_store is not None:
            try:
                saved = self.state_store.read()
            except (OSError,ValueError):
                return super()._restore()
            if isinstance(saved,dict) and 'liveNativeInitialization' in saved:
                self._reject_checkpoint_restore('live-native-initialization-cold-restore-requires-independent-live-proof-provider')
                return
        return super()._restore()

    def seed_live_initialization(self,receipt,*,context,runtime_scope,deployment_manifest_hash):
        if (self.mode not in {'MONITOR','SMALL_LIVE','FULL_LIVE'} or self.live_native_initialization is not None
                or self.bar_count or self.decision_count or self.evaluated or self.processed_open_boundaries
                or any(self.histories.values()) or self.state_store is not None and self.restore_status!='NO_CHECKPOINT'):
            raise ValueError('LIVE_NATIVE_INITIALIZATION_FRESH_ENGINE_REQUIRED')
        receipt = validate_live_initialization(receipt,context=context,runtime_scope=runtime_scope,
            deployment_manifest_hash=deployment_manifest_hash,specs=self.specs)
        # Seal before the first seed checkpoint. A partial/crashed initialization
        # can never be cold-restored through the old generic checkpoint parser.
        self.live_native_initialization = receipt
        self._live_initialization_content_hash = stable_sha256(receipt)
        data=receipt['publicInitializationData']
        bars=initialization_bars(data,portfolio_hash=receipt['portfolioHash'],promotion_policy_hash=data['promotionPolicyHash'])
        return {spec.strategy_instance_id:super(LiveKisNativeInitializedEngine,self).seed_history(spec.strategy_instance_id,
            bars[spec.strategy_instance_id]) for spec in self.specs}

    def _assert_native_phase(self,start,end=None):
        if self.live_native_initialization is None:
            raise ValueError('LIVE_NATIVE_INITIALIZATION_REQUIRED')
        if stable_sha256(self.live_native_initialization)!=self._live_initialization_content_hash:
            raise ValueError('LIVE_NATIVE_INITIALIZATION_CHANGED_AFTER_SEED')
        cutoff=_time(self.live_native_initialization['cutoff'])
        point=_time(start)
        local=point.astimezone(KST)
        if (point<cutoff or end is not None and _time(end)<=cutoff
                or local.date()!=cutoff.astimezone(KST).date()
                or (local.hour,local.minute)>=(15,20)
                or end is not None and (_time(end).astimezone(KST).hour,_time(end).astimezone(KST).minute)>=(15,20)):
            raise ValueError('LIVE_NATIVE_INITIALIZATION_BEFORE_CUTOFF_OR_PHASE_UNSUPPORTED_REQUIRE_DRAINING')

    def _validate_restore_attestation(self,attestation):
        blocker=super()._validate_restore_attestation(attestation)
        if blocker:
            return blocker
        body=(attestation.get('contextSeal') or {}).get('body') or {}
        if (self.live_native_initialization is None
                or body.get('liveNativeInitializationHash')!=self.live_native_initialization['contentHash']):
            return 'live-native-initialization-current-broker-restore-seal-required'
        return ''

    def ingest_closed_bar(self,bar):
        self._assert_native_phase(bar.start_time,bar.end_time)
        return super().ingest_closed_bar(bar)

    def ingest_open_boundary(self,boundary):
        self._assert_native_phase(boundary.start_time)
        if _time(boundary.start_time)<=_time(self.live_native_initialization['cutoff']):
            raise ValueError('LIVE_NATIVE_INITIALIZATION_PREACTIVATION_OPEN_FORBIDDEN')
        return super().ingest_open_boundary(boundary)

    def snapshot(self):
        body=super().snapshot()
        if self.live_native_initialization is not None:
            body['liveNativeInitialization']=deepcopy(self.live_native_initialization)
        return body
