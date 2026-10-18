"""KIS operational v2: exact limit caps, separate from reference/research v1.

No I/O, order authority, policy approval or public-fee-to-personal-fee conversion.
An explicit reviewed execution overlay accepts that a protective LIMIT can remain
unfilled and does not reproduce a research MARKET fill.
"""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP, localcontext
from collections.abc import Mapping
import re
from trading_runtime.artifact_governance import stable_sha256
from .native_spot_quantity import decimal_value, sizing_from_artifact, source_cost_rates, calculate_model_quantity

EXECUTION_POLICY = 'kis-equity-execution-policy-v2'
FEE_POLICY = 'kis-equity-fee-reserve-v2'
OBSERVATION = 'kis-equity-operational-observation-v2'
RESULT = 'kis-equity-operational-quantity-v2'
VENUES = {'KRX': ('KRW', 'J'), 'NASD': ('USD', 'NAS'), 'NYSE': ('USD', 'NYS'), 'AMEX': ('USD', 'AMS')}


def prepare_reviewed_policies(account,spec,*,authorization_text,now=None,saved_slippage_bps=50,valid_seconds=86400):
    """Pure documents for the already authorized account-preparation workflow.

    1% and KRW1000/USD1 are explicit local sizing holdbacks, never reported as
    the customer's actual rate or the broker's observed minimum charge.
    The caller must persist these exact documents with its normal review CAS.
    """
    from datetime import timedelta
    current=now or datetime.now(timezone.utc)
    contract=sizing_from_artifact(spec.artifact)['quantityContract'];currency=contract['quoteCurrency']
    if type(valid_seconds)is not int or not 1<=valid_seconds<=604800:raise ValueError('KIS_V2_POLICY_LIFETIME_INVALID')
    base={'brokerId':'kis','accountFingerprint':account['accountFingerprint'],
        'credentialConfigurationHash':account['credentialConfigurationHash'],'currency':currency,
        'venues':[contract['exchange']],'reviewed':True,'authorizationText':authorization_text,
        'reviewedAt':current.isoformat(),'validUntil':(current+timedelta(seconds=valid_seconds)).isoformat()}
    execution=seal({**base,'version':EXECUTION_POLICY,'sourceModelHash':contract['contractHash'],
        'sourceArtifactHash':spec.artifact_hash,'sourceOrderType':'MARKET','executionOrderType':'LIMIT',
        'semantics':'PROTECTIVE_LIMIT_NOT_RESEARCH_MARKET_FILL','protectiveLimitBps':text(decimal_value(saved_slippage_bps,'savedslippage'))})
    fee=seal({**base,'version':FEE_POLICY,'assetClass':'EQUITY','actualPersonalFeeObserved':False,
        'actualBrokerMinimumKnown':False,'kind':'EXPLICIT_CONSERVATIVE_COST_RESERVE_NOT_PERSONAL_RATE',
        'minimumFeeStatus':'OPERATOR_BOUND_NOT_OBSERVED_BROKER_MINIMUM',
        'reserveRates':{'BUY':'0.01','SELL':'0.01'},
        'minimumCostReserves':{'BUY':'1000' if currency=='KRW' else '1','SELL':'1000' if currency=='KRW' else '1'},
        'sourceURLs':['https://advisor.koreainvestment.com/main/customer/guide/_static/TF04ae010000.jsp',
                      'https://advisor.koreainvestment.com/main/bond/research/_static/TF03ca050000.jsp'],
        'referenceRetrievedAt':current.isoformat(),'effectiveRules':'1%+minimum holdback reserve for commission/tax uncertainty; no auto FX; unused reserve stays owned and unspent; not actual commission or reconciled net PnL'})
    validate_policies(execution,fee,account=account,contract=contract,artifact_hash=spec.artifact_hash,now=current,saved_slippage_bps=saved_slippage_bps)
    return {'executionPolicy':execution,'feeReservePolicy':fee,'authorizationGranted':False,'actualPersonalFeeObserved':False}


def timestamp(value):
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None: raise ValueError('KIS_V2_TIMEZONE_REQUIRED')
    return parsed


def text(value):
    out = format(value, 'f')
    return out.rstrip('0').rstrip('.') if '.' in out else out


def seal(payload):
    body = dict(payload); body.pop('contentHash', None)
    return {**body, 'contentHash': stable_sha256(body)}


def checked(payload, version):
    if not isinstance(payload, Mapping) or payload.get('version') != version or payload.get('contentHash') != stable_sha256({k:v for k,v in payload.items() if k!='contentHash'}):
        raise ValueError('KIS_V2_SEAL_OR_VERSION_INVALID')
    return dict(payload)


def validate_policies(execution, fee, *, account, contract, artifact_hash, now=None, saved_slippage_bps=50):
    current = now or datetime.now(timezone.utc)
    execution, fee = checked(execution, EXECUTION_POLICY), checked(fee, FEE_POLICY)
    exchange = contract['exchange']; currency = VENUES[exchange][0]
    for policy in (execution, fee):
        venues = policy.get('venues')
        if (policy.get('brokerId')!='kis' or policy.get('accountFingerprint')!=account['accountFingerprint']
            or policy.get('credentialConfigurationHash')!=account['credentialConfigurationHash']
            or policy.get('currency')!=currency or type(venues) is not list or exchange not in venues
            or len(set(venues))!=len(venues) or any(v not in VENUES or VENUES[v][0]!=currency for v in venues)
            or policy.get('reviewed') is not True or not isinstance(policy.get('authorizationText'),str)
            or not policy['authorizationText'].strip() or len(policy['authorizationText'])>2000
            or not timestamp(policy.get('reviewedAt'))<=current<timestamp(policy.get('validUntil'))):
            raise ValueError('KIS_V2_REVIEWED_ACCOUNT_CURRENCY_POLICY_REQUIRED')
    if (execution.get('sourceModelHash')!=contract['contractHash'] or execution.get('sourceArtifactHash')!=artifact_hash
        or execution.get('sourceOrderType')!='MARKET' or execution.get('executionOrderType')!='LIMIT'
        or execution.get('semantics')!='PROTECTIVE_LIMIT_NOT_RESEARCH_MARKET_FILL'):
        raise ValueError('KIS_V2_IMMUTABLE_EXECUTION_OVERLAY_REQUIRED')
    slip = decimal_value(execution.get('protectiveLimitBps'),'protective limit bps')
    if slip>decimal_value(saved_slippage_bps,'saved slippage bps') or slip>500:
        raise ValueError('KIS_V2_PROTECTIVE_LIMIT_EXCEEDS_SAVED_RISK')
    if (fee.get('assetClass')!='EQUITY' or fee.get('actualPersonalFeeObserved') is not False
        or fee.get('kind')!='EXPLICIT_CONSERVATIVE_COST_RESERVE_NOT_PERSONAL_RATE'
        or fee.get('minimumFeeStatus')!='OPERATOR_BOUND_NOT_OBSERVED_BROKER_MINIMUM'
        or not isinstance(fee.get('sourceURLs'),list) or not fee['sourceURLs']
        or any(not isinstance(url,str) or not url.startswith(('https://securities.koreainvestment.com/','https://advisor.koreainvestment.com/','https://apiportal.koreainvestment.com/')) for url in fee['sourceURLs'])):
        raise ValueError('KIS_V2_FEE_RESERVE_PROVENANCE_REQUIRED')
    floor = Decimal('0.00147') if currency=='KRW' else Decimal('0.0025')
    for side in ('BUY','SELL'):
        rate = decimal_value((fee.get('reserveRates') or {}).get(side),'cost reserve rate',positive=True)
        minimum = decimal_value((fee.get('minimumCostReserves') or {}).get(side),'minimum cost reserve bound',positive=True)
        if rate<floor or rate>Decimal('0.1') or minimum<=0:
            raise ValueError('KIS_V2_FEE_RESERVE_BOUND_MISSING_OR_TOO_LOW')
    return execution, fee


def limit_price(raw_open, side, tick, protective_bps):
    if side not in {'BUY','SELL'}: raise ValueError('KIS_V2_SIDE_REQUIRED')
    with localcontext() as precision:
        precision.prec=2048
        opening=decimal_value(raw_open,'real next open',positive=True)
        grid=decimal_value(tick,'broker price tick',positive=True)
        bps=decimal_value(protective_bps,'protective bps')/10000
        # BUY never pays above the cap; SELL never accepts below the floor.
        value=opening*(1+bps if side=='BUY' else 1-bps)
        value=(value/grid).to_integral_value(rounding=ROUND_DOWN if side=='BUY' else ROUND_UP)*grid
        if value<=0: raise ValueError('KIS_V2_LIMIT_NONPOSITIVE')
        return text(value)


def validate_observation(value, *, account, contract, side, limit, now=None):
    value=checked(value,OBSERVATION); current=now or datetime.now(timezone.utc)
    expected={'brokerId':'kis','exchange':contract['exchange'],'currency':contract['quoteCurrency'],
              'symbol':contract['symbol'],'side':side,'orderClass':'00','queryPrice':text(decimal_value(limit,'limit',positive=True)),
              'profileId':account['profileId'],'accountFingerprint':account['accountFingerprint'],
              'credentialConfigurationHash':account['credentialConfigurationHash']}
    if any(value.get(k)!=v for k,v in expected.items()): raise ValueError('KIS_V2_OBSERVATION_EXACT_SCOPE_CHANGED')
    if (value.get('queryPriceRole')!='REQUEST_LIMIT_PRICE_NOT_BROKER_ECHO'
        or value.get('quotePriceRole')!='CURRENT_MARK_NOT_NEXT_OPEN'
        or value.get('complete') is not True or value.get('orderable') is not True
        or value.get('openOrders')!='NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE'
        or value.get('lotSize')!='1'
        or not 0<=(current-timestamp(value.get('startedAt'))).total_seconds()<=15
        or not timestamp(value.get('startedAt'))<=timestamp(value.get('asOf'))<=current):
        raise ValueError('KIS_V2_CURRENT_COMPLETE_BROKER_TRUTH_REQUIRED')
    grid=decimal_value(value.get('priceTick'),'current price tick',positive=True)
    if decimal_value(limit,'limit',positive=True)%grid: raise ValueError('KIS_V2_LIMIT_OFF_BROKER_GRID')
    for key in ('availableCash','maximumBuyQuantity','sellableQuantity','holdingQuantity'):
        number=decimal_value(value.get(key),key)
        if 'Quantity' in key and number!=number.to_integral_value(): raise ValueError('KIS_V2_WHOLE_SHARES_REQUIRED')
    return value


def live_equity_quantity(spec, *, side, price, allocation_weight, identity, context=None, position_quantity=0, now=None):
    blocked={'status':'BLOCKED','quantityForSubmission':None,'nativeEligibility':'UNKNOWN','modelResult':None,'reasons':[],'orderAuthorityGranted':False}
    try:
        sizing=sizing_from_artifact(spec.artifact)
        if not sizing or sizing['quantityContract']['schemaVersion']!='native-equity-quantity-v1':
            raise ValueError('KIS_V2_EXACT_SEALED_EQUITY_SOURCE_REQUIRED')
        contract=sizing['quantityContract']; current=now or datetime.now(timezone.utc)
        if spec.broker_id!='kis' or spec.provider!='kis' or spec.symbol!=contract['symbol']:
            raise ValueError('KIS_V2_SOURCE_ROUTE_MISMATCH')
        if not isinstance(context,Mapping): raise ValueError('KIS_V2_CONTEXT_REQUIRED')
        account=context['savedAccountProfile']; execution,fee=validate_policies(context.get('executionPolicy'),context.get('feeReservePolicy'),account=account,contract=contract,artifact_hash=spec.artifact_hash,now=current,saved_slippage_bps=context.get('savedSlippageBps',50))
        expected={**identity,'sourceArtifactHash':spec.artifact_hash,'strategyInstanceId':spec.strategy_instance_id,'portfolioHash':spec.portfolio_hash,'quoteCurrency':contract['quoteCurrency']}
        if (any(context.get(k)!=v for k,v in expected.items()) or not identity.get('deploymentId')
            or context.get('equityScope')!='OWNED_DEPLOYMENT_ONLY' or context.get('accountKnown') is not True
            or context.get('accountFresh') is not True or context.get('capitalAccounting')!='OWNED_CONSERVATIVE_COST_RESERVE'):
            raise ValueError('KIS_V2_OWNED_PORTFOLIO_ACCOUNT_BINDING_REQUIRED')
        boundary=context.get('exactOpenBoundary') or {}
        if (boundary.get('provider')!='kis' or boundary.get('symbol')!=spec.symbol
            or boundary.get('attestation')!='KIS_WEBSOCKET' or boundary.get('recoveryOnly') is not False
            or boundary.get('promotionEligible') is not True or boundary.get('sourceArtifactHash')!=spec.artifact_hash
            or not 0<=(current-timestamp(boundary.get('startTime'))).total_seconds()<=30
            or not 0<=(timestamp(boundary.get('observedTime'))-timestamp(boundary.get('startTime'))).total_seconds()<=30
            or decimal_value(boundary.get('open'),'real open',positive=True)!=decimal_value(price,'price',positive=True)):
            raise ValueError('KIS_V2_REAL_NEXT_OPEN_REQUIRED')
        for key in ('rawHash','closedBarHash'):
            if not re.fullmatch('[a-f0-9]{64}',str(boundary.get(key) or '')): raise ValueError('KIS_V2_BOUNDARY_PROOFS_REQUIRED')
        observation=context['nativeMarketObservation']
        limit=limit_price(price,side,observation.get('priceTick'),execution['protectiveLimitBps'])
        observation=validate_observation(observation,account=account,contract=contract,side=side,limit=limit,now=current)
        rate=decimal_value(fee['reserveRates'][side],'reserve rate',positive=True)
        minimum=decimal_value(fee['minimumCostReserves'][side],'minimum reserve',positive=True)
        source_fee,source_slip=source_cost_rates(spec.artifact,spec.parameters)
        model=calculate_model_quantity(sizing,side=side,price=price,deployment_equity=context.get('deploymentEquity'),available_cash=context.get('availableCash'),allocation_weight=allocation_weight,position_percent=spec.parameters.get('positionSize'),source_fee_rate=source_fee,source_slippage_rate=source_slip,position_quantity=position_quantity)
        if model.get('status')!='READY': raise ValueError('KIS_V2_SOURCE_MODEL_'+str(model.get('reason')))
        with localcontext() as precision:
            precision.prec=2048
            wire=decimal_value(limit,'protective limit',positive=True)
            qty=decimal_value(model['quantity'],'source model quantity',positive=True)
            if side=='BUY':
                capital=min(decimal_value(context['deploymentEquity'],'equity',positive=True)*decimal_value(allocation_weight,'allocation',positive=True)*decimal_value(spec.parameters.get('positionSize'),'position percent',positive=True)/100,decimal_value(context['availableCash'],'owned cash'),decimal_value(observation['availableCash'],'broker cash'))
                qty=min(qty,decimal_value(observation['maximumBuyQuantity'],'broker max quantity'),max(Decimal(0),(capital-minimum)/wire),capital/(wire*(1+rate)))
            else:
                qty=min(qty,decimal_value(position_quantity,'owned position'),decimal_value(observation['sellableQuantity'],'broker sellable'))
            qty=qty.to_integral_value(rounding=ROUND_DOWN)
            if qty<=0: raise ValueError('KIS_V2_NO_AFFORDABLE_WHOLE_SHARE')
            notional=qty*wire; reserve=max(minimum,notional*rate)
            if side=='SELL' and reserve>=notional: raise ValueError('KIS_V2_SELL_COST_BOUND_EXCEEDS_PROCEEDS')
            result=seal({'version':RESULT,'status':'READY','quantity':text(qty),'fillPrice':limit,
                'fillPriceRole':'PROTECTIVE_LIMIT_NOT_ACTUAL_FILL','notional':text(notional),'fee':text(reserve),
                'costReserve':text(reserve),'costReserveRate':text(rate),'minimumCostReserve':text(minimum),'actualFeeObserved':False,
                'actualPersonalFeeObserved':False,'feeAccounting':'EXPLICIT_COST_RESERVE_NOT_ACTUAL_FEE',
                'contractHash':contract['contractHash'],'sourceModel':model,'deploymentEquity':str(context['deploymentEquity']),
                'sourceArtifactHash':spec.artifact_hash,'executionPolicyHash':execution['contentHash'],
                'feeReservePolicyHash':fee['contentHash'],'observationHash':observation['contentHash'],
                'executionPolicyValidUntil':execution['validUntil'],'feePolicyValidUntil':fee['validUntil'],
                'exchange':contract['exchange'],'currency':contract['quoteCurrency'],'side':side,'symbol':spec.symbol,
                'capitalScopeHash':context['capitalScopeHash'],'profileId':account['profileId'],
                'accountFingerprint':account['accountFingerprint'],'credentialConfigurationHash':account['credentialConfigurationHash'],
                'asOf':observation['asOf'],'deploymentId':identity['deploymentId'],'evaluationKey':boundary['evaluationKey'],
                'boundaryHash':stable_sha256(boundary),'orderClass':'00','quantityUnit':'SHARE'})
        transported=float(qty)
        if Decimal(str(transported))!=qty or Decimal(str(float(wire)))!=wire: raise ValueError('KIS_V2_FLOAT_WIRE_TRANSPORT_LOSS')
        return {**blocked,'status':'READY','quantityForSubmission':transported,'nativeEligibility':'VERIFIED','modelResult':result,'preflightBinding':{'version':RESULT,'asOf':observation['asOf'],'deploymentId':identity['deploymentId'],'accountFingerprint':account['accountFingerprint'],'capitalScopeHash':context['capitalScopeHash'],'sourceContractHash':contract['contractHash'],'marketObservationHash':observation['contentHash'],'kisEquityTicket':result}}
    except (ValueError,KeyError,TypeError,ArithmeticError) as error:
        blocked['reasons'].append(str(error)); return blocked


def validate_ticket(intent, *, now=None, settings=None):
    # A local shared-KRW reservation is not a verified fee/FX/settlement
    # producer. Guard every venue, including domestic KRX, at the final ticket
    # boundary. The generic native-ticket path omits settings; read only its
    # public mode preference, so the requested integrated default cannot bypass
    # this guard. Explicit FOREIGN_CASH keeps its existing validation semantics.
    from .kis_overseas_funding import KEY, INTEGRATED_MARGIN, resolve_funding_mode
    if settings is None:
        import os
        funding_settings = {KEY: os.environ.get(KEY, '')}
    else:
        funding_settings = settings
    if resolve_funding_mode(funding_settings) == INTEGRATED_MARGIN:
        raise ValueError('KIS_INTEGRATED_MARGIN_EXECUTION_NOT_VERIFIED')
    meta=intent.metadata or {}; model=checked(meta.get('native_quantity_model'),RESULT)
    binding=meta.get('native_preflight') or {}; boundary=meta.get('native_open_boundary') or {}
    if (meta.get('broker_id')!='kis' or meta.get('native_quantity_status')!='READY'
        or binding.get('kisEquityTicket')!=model or model.get('status')!='READY'
        or model.get('side')!=intent.side or model.get('symbol')!=intent.symbol
        or model.get('sourceArtifactHash')!=meta.get('source_artifact_hash')
        or model.get('deploymentId')!=meta.get('deployment_id')
        or model.get('evaluationKey')!=meta.get('runtime_evaluation_key')
        or model.get('boundaryHash')!=stable_sha256(boundary)
        or boundary.get('attestation')!='KIS_WEBSOCKET' or boundary.get('recoveryOnly') is not False
        or model.get('actualFeeObserved') is not False or model.get('feeAccounting')!='EXPLICIT_COST_RESERVE_NOT_ACTUAL_FEE'):
        raise ValueError('KIS_V2_TICKET_EXACT_SCOPE_CHANGED')
    current=now or datetime.now(timezone.utc)
    if (not 0<=(current-timestamp(model['asOf'])).total_seconds()<=15 or not 0<=(current-timestamp(boundary['startTime'])).total_seconds()<=30
        or current>=timestamp(model['executionPolicyValidUntil']) or current>=timestamp(model['feePolicyValidUntil'])):
        raise ValueError('KIS_V2_TICKET_EXPIRED')
    qty=decimal_value(model['quantity'],'quantity',positive=True); price=decimal_value(model['fillPrice'],'limit',positive=True)
    if qty!=qty.to_integral_value() or Decimal(str(intent.quantity))!=qty or Decimal(str(intent.reference_price))!=price:
        raise ValueError('KIS_V2_TICKET_WIRE_CHANGED')
    with localcontext() as precision:
        precision.prec=2048
        if (decimal_value(model['notional'],'notional')!=qty*price
            or decimal_value(model['costReserve'],'reserve')!=max(decimal_value(model['minimumCostReserve'],'minimum reserve',positive=True),qty*price*decimal_value(model['costReserveRate'],'reserve rate',positive=True))):
            raise ValueError('KIS_V2_TICKET_DECIMAL_ACCOUNTING_CHANGED')
    if (boundary.get('provider')!='kis' or boundary.get('symbol')!=intent.symbol or boundary.get('promotionEligible') is not True
        or model.get('orderClass')!='00' or model.get('exchange') not in VENUES or model.get('currency')!=VENUES[model['exchange']][0]):
        raise ValueError('KIS_V2_TICKET_BOUNDARY_OR_VENUE_CHANGED')
    for key in ('executionPolicyHash','feeReservePolicyHash','observationHash','capitalScopeHash','accountFingerprint','credentialConfigurationHash','sourceArtifactHash'):
        if not re.fullmatch('[a-f0-9]{64}',str(model.get(key) or '')):raise ValueError('KIS_V2_TICKET_PROOF_HASH_REQUIRED')
    if settings is not None:
        from .account_profiles import saved_account_profiles
        profiles=[p for p in saved_account_profiles(settings) if p['brokerId']=='kis']
        if len(profiles)!=1 or any(model.get(k)!=profiles[0][k] for k in ('profileId','accountFingerprint','credentialConfigurationHash')):
            raise ValueError('KIS_V2_TICKET_CURRENT_CREDENTIALS_CHANGED')
    return {'orderClass':'00','exchange':model['exchange'],'currency':model['currency'],'quantity':text(qty),
            'limitPrice':text(price),'costReserve':model['costReserve'],'actualPersonalFeeObserved':False,
            'orderAuthorityGranted':False}
