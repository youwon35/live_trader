"""Live public-only diagnostics. No operational consent or preflight is issued."""
from pathlib import Path
from datetime import datetime,timezone
import json,re
from trading_runtime.artifact_governance import assert_verified_strategy_instance,stable_sha256
from trading_runtime.market_gap_preparation import build_plan,load_source_target,MarketGapError,need
from trading_runtime.promotion_policy import validate_paper_promotion_policy

SELECTOR_FIELDS={'profile_id','mode','portfolio_id','deployment_id','strategy_id'}
def selector(payload):
    need(type(payload) is dict and set(payload)==SELECTOR_FIELDS,'EXACT_SELECTION_REQUIRED')
    need(payload['profile_id']=='crypto' and payload['mode'] in {'MONITOR','SMALL_LIVE','FULL_LIVE'},'CRYPTO_DIAGNOSTIC_ONLY')
    need(all(type(value) is str and len(value)<=512 for value in payload.values()),'SELECTION_INVALID')
    need(bool(payload['deployment_id']),'EXACT_DEPLOYMENT_REQUIRED')
    return dict(payload)

def capture_plan(state,payload,*,observed_at_utc=None,_guard_sink=None):
    """Read current saved account binding + normally verified Source graph.

    Reuses only disk/registry/config readers, never broker reconciliation,
    native confirmation, fresh preflight, engine creation or a financial API.
    """
    selection=selector(payload)
    before=(int(state.STATE.get('config_revision') or 1),bool(state.STATE.get('kill_switch')))
    rows=state.strategy_rows(state.portfolio_rows())
    strategy=state._operational_strategy(rows,deployment_id=selection['deployment_id'],
        strategy_id=selection['strategy_id'],portfolio_id=selection['portfolio_id'])
    need(type(strategy) is dict,'CURRENT_SOURCE_UNAVAILABLE')
    broker=state.strategy_broker_id(strategy)
    need(broker in {'upbit','binance','binance-futures'},'CRYPTO_DIAGNOSTIC_ONLY')
    # MONITOR is used solely to read the saved binding, not to start a monitor.
    account=state.resolve_continuous_runtime_scope('crypto','MONITOR',selection['portfolio_id'],
        selection['strategy_id'],selection['deployment_id'])
    need(type(account) is dict and account.get('brokerId')==broker,'CURRENT_ACCOUNT_UNAVAILABLE')
    inputs=state._operational_manifest_inputs(strategy)
    need(inputs.get('config_revision')==before[0],'SOURCE_OR_SETTINGS_CHANGED')
    actual_portfolio=str((inputs.get('metadata') or {}).get('portfolioId') or '')
    actual_strategy=str(strategy.get('strategy_id') or (inputs.get('metadata') or {}).get('strategyId') or '')
    actual_deployment=str(inputs.get('deployment_id') or '')
    need(actual_deployment==selection['deployment_id'],'CURRENT_SELECTION_MISMATCH')
    need(not selection['strategy_id'] or selection['strategy_id']==actual_strategy,'CURRENT_SELECTION_MISMATCH')
    need(not selection['portfolio_id'] or selection['portfolio_id']==actual_portfolio,'CURRENT_SELECTION_MISMATCH')
    source=Path(str(strategy.get('artifact_source_path') or '')).resolve()
    need(source.is_file(),'SOURCE_PATH_UNAVAILABLE')
    root=source.parent
    guard=None
    if _guard_sink is not None:
        from trading_runtime.market_gap_source_guard import SourceSnapshotGuard
        guard=SourceSnapshotGuard(root,state_reader=lambda:(int(state.STATE.get('config_revision') or 1),bool(state.STATE.get('kill_switch'))))
    metadata=inputs['metadata'];portfolio_id=str(metadata.get('portfolioId') or '')
    if portfolio_id:
        target={'kind':'portfolio','portfolioId':portfolio_id,'portfolioHash':inputs['portfolio_artifact_hash']}
    else:
        members=metadata.get('strategyMembers') or []
        need(len(members)==1,'STANDALONE_INSTANCE_UNAVAILABLE')
        item=members[0];instance_id=item.get('strategyInstanceId')
        instances=[]
        for path in sorted((root/'strategy-instances').glob('*.json')):
            body=json.loads(path.read_bytes())
            if type(body) is dict and (body.get('instanceId') or body.get('strategyInstanceId'))==instance_id:
                instances.append((body,assert_verified_strategy_instance(body)))
        need(len(instances)==1,'STANDALONE_INSTANCE_UNAVAILABLE')
        target={'kind':'strategy','strategyId':item['strategyId'],'strategyInstanceId':instance_id,
            'artifactHash':item['artifactHash'],'instanceHash':instances[0][1]}
    loaded=load_source_target(root,target)
    artifacts=state.load_strategy_artifacts();policies=[]
    for item in loaded['members']:
        candidates=[row for row in artifacts if state.verified_strategy_artifact_reference(row).get('artifactHash')==item['sourceArtifactHash']]
        need(candidates,'EXACT_SOURCE_POLICY_UNAVAILABLE')
        q=[state.paper_live_qualification_gate_for_strategy(row) for row in candidates]
        found=[validate_paper_promotion_policy(value['promotionPolicy']) for value in q if value.get('promotionPolicy')]
        need(found and all(value==found[0] for value in found),'EXACT_SOURCE_POLICY_UNAVAILABLE')
        policies.append(found[0])
    need(policies and all(value==policies[0] for value in policies),'SOURCE_POLICIES_DIFFER')
    after=(int(state.STATE.get('config_revision') or 1),bool(state.STATE.get('kill_switch')))
    need(before==after,'SOURCE_OR_SETTINGS_CHANGED')
    binding={'application':'LIVE','selection':selection,'runtimeScopeId':account['runtimeScopeId'],
        'accountFingerprint':account['accountFingerprint'],
        'credentialConfigurationHash':account['credentialConfigurationHash'],
        'deploymentId':inputs['deployment_id'],'configRevision':inputs['config_revision'],
        'configHash':inputs['config_hash'],'riskPolicyHash':inputs['risk_policy_hash'],
        'sourceGraphHash':loaded['sourceGraphHash'],'policyHash':policies[0]['policyHash'],
        'killSwitch':before[1],'selectedPortfolioHash':loaded['portfolioHash']}
    plan=build_plan(owner_key=account['runtimeScopeId'],binding=binding,members=loaded['members'],policy=policies[0],
        observed_at_utc=observed_at_utc or datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'))
    if guard is not None:
        need(guard.is_current(),'SOURCE_OR_SETTINGS_CHANGED');_guard_sink.append(guard)
    return plan

def capture_preparation(state,payload):
    guards=[];plan=capture_plan(state,payload,_guard_sink=guards)
    def current(_):return guards[0].is_current()
    def final(original):return current(original) and binding_is_current(state,original)
    return plan,current,final

def binding_is_current(state,plan):
    try:
        current=capture_plan(state,plan['binding']['selection'],observed_at_utc=plan['observedAtUtc'])
        return current['bindingHash']==plan['bindingHash'] and current['contentHash']==plan['contentHash']
    except Exception:return False

def safe_failure():
    return {'ok':False,'status':'FAILED','reasonCode':'PUBLIC_GAP_PREPARATION_UNAVAILABLE',
        'userMessage':'선택한 구성의 최근 체결 공백을 확인하지 못했습니다.',
        'authorityGranted':False,'qualificationGranted':False,'runtimeStarted':False,'brokerSubmissionPerformed':False}
