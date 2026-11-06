"""Exact immutable stock sources with an explicit, run-local Toss cash binding."""
from dataclasses import replace
from decimal import Decimal, InvalidOperation
import math

from trading_runtime.artifact_governance import artifact_reference, stable_sha256
from trading_runtime.continuous_runtime import RuntimeStrategySpec
from trading_runtime.portfolio_runtime import load_portfolio_runtime, infer_market_route
from trading_runtime.provider_backfill_toss import toss_market
from trading_runtime.exposure_contract import exposure_contract_from_artifact
from .paper_candidate_inbox import configured_artifact_roots, _catalog, _instance_catalog, _exact_artifact, _exact_instance, _standalone_scope_hash

PLUGINS = {'moving_average_cross', 'rsi_revert', 'breakout', 'threshold_momentum', 'strategy_builder_custom'}


def source_sizing(spec):
    """Keep the sealed Paper quantity/position-percent semantics for cash."""
    artifact=spec.artifact; params=spec.parameters
    execution=artifact.get('execution') or {}; trader=artifact.get('trader_contract') or artifact.get('traderContract') or {}
    sizing=artifact.get('execution_sizing') or artifact.get('executionSizing') or execution.get('paper_execution_sizing') or execution.get('paperExecutionSizing') or trader.get('executionSizing') or {}
    raw=sizing.get('paperOrderQuantity',sizing.get('fixedQuantity',artifact.get('order_quantity',params.get('orderQuantity',1))))
    try: quantity=Decimal(str(raw))
    except (InvalidOperation,ValueError,TypeError): raise ValueError('TOSS_WHOLE_SHARE_QUANTITY_REQUIRED') from None
    if isinstance(raw,bool) or not quantity.is_finite() or quantity<=0 or quantity!=quantity.to_integral_value(): raise ValueError('TOSS_WHOLE_SHARE_QUANTITY_REQUIRED')
    risk=(params.get('customStrategyDefinition') or {}).get('riskRules') or {}
    percent=risk.get('positionSize',params.get('positionSize'))
    if percent is not None:
        try: value=Decimal(str(percent))
        except (InvalidOperation,ValueError,TypeError): raise ValueError('TOSS_POSITION_PERCENT_INVALID') from None
        if isinstance(percent,bool) or not value.is_finite() or not 0<value<=100: raise ValueError('TOSS_POSITION_PERCENT_INVALID')
        percent=value
    return quantity,percent


def standalone(root, target):
    source = _exact_artifact(_catalog(root, 'strategy'), target['strategyId'], target['artifactHash'])
    instance, digest = _exact_instance(_instance_catalog(root), target['strategyInstanceId'], source)
    if digest != target.get('instanceHash'):
        raise ValueError('TOSS_INSTANCE_HASH_CHANGED')
    symbol = str(instance.get('qualifiedSymbol') or instance.get('symbol') or '').upper()
    timeframe = str(instance.get('qualifiedTimeframe') or instance.get('timeframe') or '').lower()
    plugin = str(instance.get('pluginId') or instance.get('plugin') or '')
    if (symbol != str(source.get('symbol') or '').upper() or timeframe != str(source.get('timeframe') or '').lower()
            or plugin != str(source.get('plugin') or source.get('pluginId') or '')
            or instance.get('parameters') != source.get('parameters')):
        raise ValueError('TOSS_SOURCE_INSTANCE_EXECUTION_MISMATCH')
    provider, broker, instrument = infer_market_route(symbol)
    provider = str(instance.get('marketDataProvider') or (source.get('dataArtifact') or {}).get('provider') or provider).lower()
    broker = str(instance.get('brokerId') or broker).lower()
    return RuntimeStrategySpec(portfolio_id='standalone:'+target['strategyId']+':'+target['strategyInstanceId'],
        portfolio_hash=_standalone_scope_hash(source, instance, digest), strategy_instance_id=target['strategyInstanceId'],
        strategy_id=target['strategyId'], artifact_hash=target['artifactHash'], plugin_id=plugin,
        instrument_id=instrument, symbol=symbol, timeframe=timeframe, provider=provider, broker_id=broker,
        target_weight=1, parameters=dict(instance.get('parameters') or {}), artifact={**source, 'strategyInstance':instance})


def validate_spec(spec):
    artifact = spec.artifact
    asset = str(artifact.get('asset') or artifact.get('assetClass') or (artifact.get('dataset') or {}).get('assetClass') or (artifact.get('dataArtifact') or {}).get('assetClass') or '').lower()
    if asset not in {'stock','stocks','equity','equities','etf','kr_stock','us_stock'} or spec.provider not in {'kis','toss','yahoo'} or spec.broker_id not in {'kis','toss'}:
        raise ValueError('TOSS_CASH_EQUITY_SOURCE_REQUIRED')
    params = spec.parameters
    direction = str(params.get('positionDirection') or artifact.get('positionDirection') or 'long').lower()
    if any(str(row.get('positionDirection') or row.get('position_direction') or 'long').lower()!='long' for row in (artifact,artifact.get('strategyInstance') or {},params)):
        raise ValueError('TOSS_CASH_LONG_SUPPORTED_PLUGIN_REQUIRED')
    contract=exposure_contract_from_artifact({**artifact,'symbol':spec.symbol,'parameters':params})
    if direction != 'long' or contract.get('valid') is not True or contract.get('positionDirection')!='long' or spec.plugin_id.lower().replace('-', '_') not in PLUGINS:
        raise ValueError('TOSS_CASH_LONG_SUPPORTED_PLUGIN_REQUIRED')
    if spec.timeframe not in {'1m','5m','15m','1h','1d'}:
        raise ValueError('TOSS_TIMEFRAME_UNSUPPORTED')
    symbol, market = toss_market(spec.symbol)
    if toss_market(contract.get('executionInstrument',''))[0]!=symbol or toss_market(contract.get('signalInstrument',''))[0]!=symbol:
        raise ValueError('TOSS_EXPOSURE_INSTRUMENT_MISMATCH')
    currency = 'KRW' if market == 'KR' else 'USD'
    declarations = [artifact.get('currency'), params.get('currency')]
    for row in (artifact,artifact.get('strategyInstance') or {},artifact.get('quantityContract') or {}):
        declarations.extend([row.get('quoteCurrency'),row.get('settlementCurrency')])
    if any(value and value != currency for value in declarations):
        raise ValueError('TOSS_SOURCE_CURRENCY_MISMATCH')
    # A strategy's requested quantity remains an upper bound. No hidden leverage.
    source_sizing(spec)
    if float(params.get('leverage', 1)) != 1:
        raise ValueError('TOSS_CASH_LEVERAGE_UNSUPPORTED')
    return symbol, market, currency


def resolve_sources(config, roots=None):
    if not isinstance(config, dict) or config.get('tossBindingConfirmed') is not True:
        raise ValueError('TOSS_EXPLICIT_BINDING_REQUIRED')
    targets = config.get('targets')
    if not isinstance(targets, list) or not 1 <= len(targets) <= 8:
        raise ValueError('TOSS_EXACT_TARGETS_REQUIRED')
    roots = configured_artifact_roots() if roots is None else roots
    by_key = {stable_sha256(str(root.resolve())): root for root in roots}
    specs, sources, seen, targets_by_currency = [], [], set(), {'KRW':set(), 'USD':set()}
    for i, target in enumerate(targets):
        root = by_key.get(target.get('rootKey'))
        if root is None:
            raise ValueError('TOSS_CONFIGURED_SOURCE_ROOT_REQUIRED')
        if target.get('kind') == 'portfolio':
            source = _exact_artifact(_catalog(root, 'portfolio'), target['portfolioId'], target['portfolioHash'])
            loaded = load_portfolio_runtime(root, target['portfolioId'])
            if not loaded.path.resolve().is_relative_to(root.resolve()):
                raise ValueError('TOSS_PORTFOLIO_OUTSIDE_CONFIGURED_ROOT')
            if loaded.portfolio_hash != target['portfolioHash']:
                raise ValueError('TOSS_PORTFOLIO_HASH_CHANGED')
            items = loaded.specs
        elif target.get('kind') == 'strategy':
            items = (standalone(root,target),)
        else:
            raise ValueError('TOSS_TARGET_KIND_INVALID')
        identity = (items[0].portfolio_id, items[0].portfolio_hash)
        if identity in seen:
            raise ValueError('TOSS_DUPLICATE_TARGET')
        seen.add(identity)
        weights = [float(spec.target_weight) for spec in items]
        if any(not math.isfinite(w) or w<0 for w in weights) or not 0<sum(weights)<=1.000001:
            raise ValueError('TOSS_SOURCE_WEIGHTS_INVALID')
        for spec in items:
            symbol, market, currency = validate_spec(spec)
            if not spec.target_weight:
                continue
            targets_by_currency[currency].add(i)
            key = 'target-'+str(i+1)+':'+spec.strategy_instance_id
            sources.append({'key':key,'currency':currency,'market':market,'targetIndex':i,'original':spec.to_dict(), 'sourceBodyHash':stable_sha256(spec.artifact)})
            specs.append(replace(spec,strategy_instance_id=key,symbol=symbol,provider='toss',broker_id='toss',instrument_id='toss:'+market+':'+symbol))
    if not 1 <= len(specs) <= 32:
        raise ValueError('TOSS_SPEC_COUNT_INVALID')
    factors = {row['key']:str(Decimal(str(row['original']['targetWeight']))/len(targets_by_currency[row['currency']])) for row in sources}
    return tuple(specs), sources, factors, stable_sha256({'targets':targets,'sources':sources,'factors':factors})


def source_catalog(roots=None):
    result=[]
    for root in (configured_artifact_roots() if roots is None else roots):
        root_key=stable_sha256(str(root.resolve()))
        strategies=_catalog(root,'strategy')
        for identifier, rows in _instance_catalog(root).items():
            if len(rows)!=1: continue
            item=rows[0]
            sid=str(item.get('sourceStrategyId') or item.get('strategyId') or '')
            sha=str(item.get('sourceArtifactHash') or item.get('strategyArtifactHash') or '')
            try:
                source=_exact_artifact(strategies,sid,sha)
                _, ih=_exact_instance({identifier:rows},identifier,source)
                target={'kind':'strategy','rootKey':root_key,'strategyId':sid,'artifactHash':sha,'strategyInstanceId':identifier,'instanceHash':ih}
                spec=standalone(root,target); validate_spec(spec)
                result.append({'key':stable_sha256(target),'label':str(source.get('name') or sid)+' · '+spec.symbol+' '+spec.timeframe,'target':target})
            except (ValueError,KeyError,TypeError): continue
        for pid, rows in _catalog(root,'portfolio').items():
            try:
                if not rows or not rows[0]: continue
                sha=artifact_reference(rows[0])['artifactHash']
                target={'kind':'portfolio','rootKey':root_key,'portfolioId':pid,'portfolioHash':sha}
                resolve_sources({'targets':[target],'tossBindingConfirmed':True},[root])
                result.append({'key':stable_sha256(target),'label':str(rows[0].get('name') or pid)+' · 포트폴리오','target':target})
            except (ValueError,KeyError,TypeError,OSError): continue
    return {'items':result[:200], 'purpose':'TOSS_CASH_OPERATION','nativePromotion':False}
