"""Explicit test-model binding; never changes or qualifies a source artifact."""
from copy import deepcopy
import hashlib,json
from trading_runtime.futures_accounting import leverage_integer
from trading_runtime.futures_execution_policy import futures_execution_policy_from_artifact

def canonical(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()

def declared_models(artifact):
    rows=[];seen=set();todo=[artifact]
    while todo:
        value=todo.pop()
        if not isinstance(value,dict) or id(value) in seen:continue
        seen.add(id(value))
        if 'executionModel' in value:
            if not isinstance(value['executionModel'],dict):raise ValueError('SOURCE_EXECUTION_MODEL_INVALID')
            rows.append(value['executionModel'])
        for key in ('execution','settings','parameters','strategyInstance','executionProfile'):
            if isinstance(value.get(key),dict):todo.append(value[key])
    if len({canonical(row) for row in rows})>1:raise ValueError('SOURCE_EXECUTION_MODEL_DECLARATIONS_CONFLICT')
    return deepcopy(rows[0]) if rows else None

def model_binding(specs,leverage):
    multiplier=leverage_integer(leverage)
    actual={'schemaVersion':'live-exchange-margin-v1','sizingBasis':'MARGIN_BUDGET',
        'executionLeverage':multiplier,'priceSource':'ACTUAL_MARK_PRICE_AND_ORDER_FILLS',
        'capitalBudgetBasis':'INITIAL_MARGIN_PLUS_ENTRY_EXIT_FEES_AND_FUNDING_RESERVE',
        'fundingModel':'ACTUAL_RATE_ABSOLUTE_TWO_ALLOWANCES','maintenanceModel':'ACTUAL_ACCOUNT_SYMBOL_TIERS'}
    sources=[];ceilings=[]
    for spec in specs:
        if spec.get('route')!='BINANCE_USDM_FUTURES':continue
        artifact=spec.get('artifact') or {};declared=declared_models(artifact)
        policy=futures_execution_policy_from_artifact(artifact)
        cap=None
        if str(policy.get('source','')).startswith('artifact:'):
            if not policy.get('valid'):raise ValueError('SOURCE_FUTURES_POLICY_INVALID')
            cap=leverage_integer(policy['maxLeverageMultiplier']);ceilings.append(cap)
            if multiplier>cap:raise ValueError('EXECUTION_LEVERAGE_EXCEEDS_SOURCE_MAXIMUM')
        sources.append({'strategyInstanceId':spec['strategyInstanceId'],'artifactHash':spec['artifactHash'],
            'sourceExecutionModel':declared,'sourceExecutionModelHash':canonical(declared) if declared else None,
            'sourceMaxLeverageMultiplier':cap,'status':'MATCH' if declared==actual else 'DIFFERENT_MODEL' if declared else 'UNVERIFIED_LEGACY',
            'sourceEvidenceRequalified':False})
    body={'schemaVersion':'functional-execution-model-binding-v1','sizingBasis':'MARGIN_BUDGET',
        'executionLeverage':multiplier,'maxLeverageMultiplier':min(ceilings) if ceilings else None,
        'runtimeExecutionModel':actual,'sources':sources,'overrideRequired':any(row['status']!='MATCH' for row in sources),
        'promotionEligible':False,'originalEvidenceModified':False}
    return {**body,'bindingHash':canonical(body)}
