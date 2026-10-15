"""Owned KIS execution context; no order/cancel/start/arming method exists."""
from __future__ import annotations
from datetime import datetime,timezone
from pathlib import Path
from decimal import Decimal
import sqlite3
from trading_runtime.artifact_governance import stable_sha256
from .native_spot_runtime import current_deployment
from .native_spot_quantity import sizing_from_artifact,decimal_value
from .read_only_account import ReadOnlyBrokerReader,runtime_settings,ReadFailure
from .account_profiles import saved_account_profiles
from . import paper_candidate_inbox as inbox
from .kis_equity_execution import validate_policies,VENUES
from .kis_equity_reader import KisEquityReader
from .kis_owned_capital import KisOwnedCapitalStore,terminal_receipt,resolve_official_order_identity,request_session_date_from_dispatch
from .verified_terminal_cache import VerifiedTerminalCache,terminal_identity,terminal_proof


class NativeEquityRuntime:
    def __init__(self,root,deployment_id,specs,*,reader=None,roots=None,deployment_reader=None,dispatch_reader=None,dispatch_identity_writer=None,dispatch_lookup=None,policy_reader=None,mark_provider=None):
        self.root=Path(root);self.deployment_id=deployment_id;self.specs=tuple(specs)
        self.reader=reader or ReadOnlyBrokerReader(runtime_settings())
        self.observer=KisEquityReader(self.reader)
        self.store=KisOwnedCapitalStore(self.root/'logs'/'kis_owned_capital_v2.sqlite3')
        self.terminal_cache=VerifiedTerminalCache(self.root/'logs'/'verified_terminal_order_cache.sqlite3')
        self.roots=roots;self.deployment_reader=deployment_reader or current_deployment
        self.dispatch_reader=dispatch_reader;self.policy_reader=policy_reader;self.mark_provider=mark_provider
        self.dispatch_identity_writer=dispatch_identity_writer;self.dispatch_lookup=dispatch_lookup

    def _details(self):
        deployment=self.deployment_reader(self.deployment_id,self.specs,self.roots if self.roots is not None else inbox.configured_artifact_roots())
        account=self.observer.account();permissions=deployment.get('permissions') or {}
        if (deployment.get('environment') not in {'LIVE','SMALL_LIVE','FULL_LIVE'}
            or deployment.get('lifecycle') not in {'before-live-small','live-small','live-full'}
            or deployment.get('accountId')!=account['accountBindingId'] or deployment.get('route')!='kis'
            or not permissions.get('liveReviewBindingHash')
            or any(permissions.get(k)!=account[v] for k,v in {'liveAccountProfileId':'profileId','liveAccountFingerprint':'accountFingerprint','liveAccountCredentialConfigurationHash':'credentialConfigurationHash'}.items())):
            raise ValueError('KIS_V2_REVIEWED_EXACT_DEPLOYMENT_ACCOUNT_REQUIRED')
        if self.policy_reader is not None:policies=self.policy_reader(deployment,account,self.specs)
        else:policies={'executionPolicies':permissions.get('kisEquityExecutionPolicies') or {},'feeReservePolicy':permissions.get('kisEquityFeeReservePolicy'),'savedSlippageBps':permissions.get('kisSavedSlippageBps',50)}
        bindings=[];currencies=set();exchanges=set()
        if not self.specs or len(self.specs)>32:raise ValueError('KIS_V2_BOUNDED_CANONICAL_MEMBERS_REQUIRED')
        for spec in self.specs:
            sizing=sizing_from_artifact(spec.artifact);contract=(sizing or {}).get('quantityContract') or {}
            if spec.provider!='kis' or spec.broker_id!='kis' or contract.get('schemaVersion')!='native-equity-quantity-v1' or spec.symbol!=contract.get('symbol'):
                raise ValueError('KIS_V2_SEALED_EQUITY_MEMBER_REQUIRED')
            validate_policies(policies['executionPolicies'].get(spec.artifact_hash),policies.get('feeReservePolicy'),account=account,contract=contract,artifact_hash=spec.artifact_hash,saved_slippage_bps=policies.get('savedSlippageBps',50))
            currencies.add(contract['quoteCurrency']);exchanges.add(contract['exchange'])
            bindings.append({'strategyInstanceId':spec.strategy_instance_id,'sourceArtifactHash':spec.artifact_hash,'portfolioHash':spec.portfolio_hash,'symbol':spec.symbol,'quoteCurrency':contract['quoteCurrency']})
        if len(currencies)!=1 or len({m['strategyInstanceId'] for m in bindings})!=len(bindings):
            raise ValueError('KIS_V2_ONE_CURRENCY_BOOK_AND_UNIQUE_MEMBERS_REQUIRED')
        policy=permissions.get('liveCapitalPolicy') or {};percent=decimal_value(policy.get('percent'),'reviewed capital percent',positive=True)
        if policy.get('basis')!='ACCOUNT_AVAILABLE_CASH' or percent>100:raise ValueError('KIS_V2_REVIEWED_CAPITAL_POLICY_REQUIRED')
        scope={'version':'kis-owned-capital-v2','deploymentId':self.deployment_id,'accountFingerprint':account['accountFingerprint'],
            'credentialConfigurationHash':account['credentialConfigurationHash'],'accountProfileId':account['profileId'],
            'reviewBindingHash':permissions['liveReviewBindingHash'],'capitalPercent':str(percent),
            'quoteCurrency':next(iter(currencies)),'members':sorted(bindings,key=lambda m:m['strategyInstanceId'])}
        scope['scopeHash']=stable_sha256(scope)
        return scope,account,policies

    def scope(self):return self._details()[0]

    def recover_order_identity(self,spec,dispatch,*,request_session_date=None):
        """Read-only recovery; the caller must persist verified identity by CAS.

        No automatic date fallback is used by sync_owned_fills. The supplied NY
        session date only bounds the official query; its echoed ord_dt is truth.
        """
        if spec not in self.specs:raise ValueError('KIS_IDENTITY_CANONICAL_MEMBER_REQUIRED')
        from .kis_equity_execution import checked,RESULT
        ticket=checked((dispatch.get('metadata') or {}).get('native_quantity_model'),RESULT)
        account=self.observer.account()
        if any(ticket.get(key)!=account.get(key) for key in ('profileId','accountFingerprint','credentialConfigurationHash')):
            raise ValueError('KIS_IDENTITY_CURRENT_SAVED_ACCOUNT_CHANGED')
        derived=request_session_date_from_dispatch(spec,dispatch)
        if request_session_date is not None and request_session_date!=derived:raise ValueError('KIS_IDENTITY_QUERY_DATE_DIFFERS_FROM_SEALED_MARKET_SESSION')
        request_session_date=derived
        exchange=sizing_from_artifact(spec.artifact)['quantityContract']['exchange']
        rows=self.observer.terminal_orders(exchange,request_session_date)
        return resolve_official_order_identity(spec,dispatch,rows,request_session_date=request_session_date)

    def initialize_at_reviewed_manual_start(self):
        scope,account,policies=self._details();spec=self.specs[0];contract=sizing_from_artifact(spec.artifact)['quantityContract']
        quote=self.observer.quote(contract['exchange'],spec.symbol)
        observation=self.observer.observe(contract,side='BUY',raw_open=quote['price'],protective_bps=policies['executionPolicies'][spec.artifact_hash]['protectiveLimitBps'])
        if self.scope()!=scope:raise ValueError('KIS_V2_BINDING_CHANGED_DURING_INITIALIZATION')
        return self.store.initialize(scope,observation)

    def sync_owned_fills(self,scope):
        writer=self.dispatch_identity_writer;lookup=self.dispatch_lookup
        if self.dispatch_reader is None:
            from . import state
            rows=state.PROGRAM_LEDGER.order_dispatch_rows(1000000)
            writer=writer or state.PROGRAM_LEDGER.record_verified_kis_order_identity
            lookup=lookup or state.PROGRAM_LEDGER.order_dispatch_for_idempotency_key
        else:rows=self.dispatch_reader()
        if not isinstance(rows,list) or len(rows)>=1000000:raise ValueError('KIS_V2_COMPLETE_DISPATCH_LEDGER_REQUIRED')
        members={s.strategy_instance_id:s for s in self.specs};official={};receipts=[];verified=[]
        for row in rows:
            metadata=row.get('metadata') or {}
            if metadata.get('deployment_id')!=self.deployment_id:continue
            if row.get('broker_id')!='kis':raise ValueError('KIS_V2_DISPATCH_BROKER_CHANGED')
            sid=metadata.get('strategy_instance_id')
            if sid not in members:raise ValueError('KIS_V2_DISPATCH_MEMBER_CHANGED')
            if not row.get('broker_order_id') or row.get('broker_order_id')=='-':
                if row.get('state') not in {'risk_blocked','adapter_blocked','rejected'}:raise ValueError('KIS_V2_DISPATCH_RESULT_UNRESOLVED')
                continue
            spec=members[sid];exchange=sizing_from_artifact(spec.artifact)['quantityContract']['exchange'];day=str(row.get('order_date') or '')
            if not day or (row.get('broker_order_identity_verified') is not True and row.get('order_date_source') in {
                'KST_SUBMISSION_DATE_PROVISIONAL','MARKET_SESSION_DATE_PROVISIONAL','AWAITING_BROKER_TRADING_DATE'}):
                if writer is None or lookup is None:raise ValueError('KIS_IDENTITY_DURABLE_CAS_WRITER_REQUIRED')
                identity=self.recover_order_identity(spec,row)
                writer(row.get('idempotency_key'),stable_sha256(row),identity)
                recovered=lookup(row.get('idempotency_key'))
                if (not isinstance(recovered,dict) or recovered.get('broker_order_identity_verified') is not True
                    or any(recovered.get(k)!=identity[k] for k in ('order_date','organization_no','broker_order_id','broker_order_key'))):
                    raise ValueError('KIS_IDENTITY_DURABLE_VERIFIED_ACK_REREAD_REQUIRED')
                row=recovered;day=row['order_date']
            identity=terminal_identity(scope,spec,row)
            cached=self.terminal_cache.get(identity)
            if cached is not None:
                receipts.extend(cached['receipts'])
                continue
            key=(exchange,day)
            if key not in official:official[key]=self.observer.terminal_orders(exchange,day)
            matches=[item for item in official[key] if str(item.get('ord_dt'))==day and str(item.get('ord_gno_brno'))==str(row.get('organization_no')) and str(item.get('odno'))==str(row['broker_order_id'])]
            if len(matches)!=1:raise ValueError('KIS_V2_OWNED_OFFICIAL_ORDER_UNRESOLVED_OR_AMBIGUOUS')
            receipt=terminal_receipt(scope,spec,row,matches[0])
            if receipt:receipts.append(receipt)
            current=[receipt] if receipt else []
            proof=terminal_proof(identity,current,broker_response=matches[0],
                executed_quantity=matches[0].get('tot_ccld_qty' if exchange=='KRX' else 'ft_ccld_qty'))
            verified.append((identity,current,proof))
        # Official order timestamps and numeric ODNO give deterministic order;
        # neither field is represented as an actual execution/fill timestamp.
        for receipt in sorted(receipts,key=lambda r:(r['orderDate'],r['orderTime'],int(r['orderNo']),r['eventKey'])):self.store.append_terminal_receipt(scope,receipt)
        for identity,current,proof in verified:self.terminal_cache.put(identity,current,proof)
        return len(receipts)

    def context(self,spec,side,price,specs,boundary):
        try:
            scope,account,policies=self._details()
            if tuple(s.strategy_instance_id for s in specs)!=tuple(s.strategy_instance_id for s in self.specs):raise ValueError('KIS_V2_CANONICAL_MEMBERS_CHANGED')
            self.sync_owned_fills(scope);projection=self.store.project(scope,{})
            if side=='POSITION':return projection
            started=datetime.now(timezone.utc);contract=sizing_from_artifact(spec.artifact)['quantityContract']
            execution=policies['executionPolicies'][spec.artifact_hash]
            observation=self.observer.observe(contract,side=side,raw_open=price,protective_bps=execution['protectiveLimitBps'])
            marks={}
            for member in self.specs:
                if decimal_value(projection['ownedPositions'][member.strategy_instance_id],'owned shares'):
                    if member.symbol==spec.symbol:marks[member.strategy_instance_id]=str(price)
                    else:
                        if self.mark_provider is None:raise ValueError('KIS_V2_OTHER_HELD_MEMBER_FRESH_MARK_REQUIRED')
                        mark=self.mark_provider(member)
                        from .kis_equity_execution import timestamp
                        if (not isinstance(mark,dict) or mark.get('provider')!='kis' or mark.get('symbol')!=member.symbol
                            or mark.get('attestation')!='KIS_WEBSOCKET'
                            or not 0<=(datetime.now(timezone.utc)-timestamp(mark.get('observedAt'))).total_seconds()<=15):
                            raise ValueError('KIS_V2_OTHER_HELD_MEMBER_MARK_UNVERIFIED')
                        marks[member.strategy_instance_id]=str(decimal_value(mark.get('price'),'fresh owned mark',positive=True))
            projection=self.store.project(scope,marks)
            totals=self.store.account_owned_totals(scope)
            if decimal_value(observation['availableCash'],'broker free cash')<decimal_value(totals['ownedCash'],'all owned account cash'):
                raise ValueError('KIS_V2_EXTERNAL_CASH_CHANGE_RECONCILIATION_REQUIRED')
            if decimal_value(observation['holdingQuantity'],'broker holdings')<decimal_value(totals['ownedPositionsBySymbol'].get(spec.symbol,0),'all owned account shares'):
                raise ValueError('KIS_V2_OWNED_BROKER_POSITION_RECONCILIATION_REQUIRED')
            if (datetime.now(timezone.utc)-started).total_seconds()>15 or self.scope()!=scope:raise ValueError('KIS_V2_CONTEXT_STALE_OR_SCOPE_CHANGED')
            return {**projection,'deploymentId':self.deployment_id,'sourceArtifactHash':spec.artifact_hash,
                'strategyInstanceId':spec.strategy_instance_id,'portfolioHash':spec.portfolio_hash,'quoteCurrency':scope['quoteCurrency'],
                'accountFingerprint':account['accountFingerprint'],'savedAccountProfile':account,
                'accountKnown':True,'accountFresh':True,'asOf':observation['asOf'],
                'availableCash':str(min(decimal_value(projection['ownedCash'],'owned cash'),decimal_value(observation['availableCash'],'broker cash'))),
                'executionPolicy':execution,'feeReservePolicy':policies['feeReservePolicy'],'savedSlippageBps':policies.get('savedSlippageBps',50),
                'nativeMarketObservation':observation,'exactOpenBoundary':boundary,'orderAuthorityGranted':False}
        except (ValueError,TypeError,KeyError,OSError,ArithmeticError,ReadFailure,sqlite3.Error) as error:
            return {'status':'BLOCKED','reasons':[str(error)],'orderAuthorityGranted':False}


def preparation_status(deployment,specs,settings,*,production_authority_available=False):
    """No broker calls, ledger writes or initialization. For Root's preparation UI."""
    try:
        accounts=[p for p in saved_account_profiles(settings) if p['brokerId']=='kis']
        if len(accounts)!=1:raise ValueError('KIS_V2_SAVED_ACCOUNT_MISSING')
        account=accounts[0];permissions=deployment.get('permissions') or {};currencies=set()
        for spec in specs:
            contract=sizing_from_artifact(spec.artifact)['quantityContract'];currencies.add(contract['quoteCurrency'])
            validate_policies((permissions.get('kisEquityExecutionPolicies') or {}).get(spec.artifact_hash),permissions.get('kisEquityFeeReservePolicy'),account=account,contract=contract,artifact_hash=spec.artifact_hash,saved_slippage_bps=permissions.get('kisSavedSlippageBps',50))
        if not specs or len(currencies)!=1:raise ValueError('KIS_V2_ONE_CURRENCY_PORTFOLIO_REQUIRED')
        return {'status':'LOCAL_POLICIES_PREPARED_FRESH_PREFLIGHT_REQUIRED' if production_authority_available is True else 'BLOCKED_BY_PRODUCTION_AUTHORITY',
            'available':production_authority_available is True,'localPoliciesPrepared':True,
            'productionAuthorityAvailable':production_authority_available is True,'liveReady':False,'actualPersonalFeeObserved':False,
            'currentBrokerEvidence':'NOT_QUERIED','orderAuthorityGranted':False,'brokerCalls':0,'runtimeStarts':0}
    except (ValueError,TypeError,KeyError,ArithmeticError) as error:
        return {'status':'BLOCKED','available':False,'reasons':[str(error)],'actualPersonalFeeObserved':False,'orderAuthorityGranted':False,'brokerCalls':0,'runtimeStarts':0}
