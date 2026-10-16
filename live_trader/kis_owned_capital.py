"""Currency-scoped KIS owned books with explicit estimated cost holdbacks.

Only exact owned terminal orders change positions. Account-wide holdings never
become portfolio capital. A cost holdback is never booked as an actual fee or
reported as reconciled net performance.
"""
from __future__ import annotations
from contextlib import contextmanager,closing
from datetime import datetime,timezone
from decimal import Decimal,localcontext
from pathlib import Path
import json,sqlite3,re
from zoneinfo import ZoneInfo
from trading_runtime.artifact_governance import stable_sha256
from .native_spot_quantity import decimal_value,sizing_from_artifact
from .kis_equity_execution import text,timestamp,checked,RESULT


class KisOwnedCapitalStore:
    def __init__(self,path):self.path=Path(path)

    @contextmanager
    def _db(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            with db:
                db.execute('CREATE TABLE IF NOT EXISTS principals (deployment_id TEXT PRIMARY KEY, account_currency TEXT NOT NULL, payload TEXT NOT NULL, content_hash TEXT NOT NULL)')
                db.execute('CREATE TABLE IF NOT EXISTS receipts (event_key TEXT PRIMARY KEY, deployment_id TEXT NOT NULL, payload TEXT NOT NULL, content_hash TEXT NOT NULL)')
                for table in ('principals','receipts'):
                    for operation in ('UPDATE','DELETE'):
                        db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation.lower()} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'kis-owned-append-only'); END")
                yield db

    def initialize(self,scope,observation,*,now=None,commit_guard=None):
        if commit_guard is not None:commit_guard()
        now=now or datetime.now(timezone.utc)
        if (scope['quoteCurrency']!=observation.get('currency') or scope['accountProfileId']!=observation.get('profileId')
            or scope['accountFingerprint']!=observation.get('accountFingerprint')
            or scope['credentialConfigurationHash']!=observation.get('credentialConfigurationHash')
            or observation.get('openOrders')!='NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE'
            or not 0<=(now-timestamp(observation.get('asOf'))).total_seconds()<=15):
            raise ValueError('KIS_OWNED_FRESH_EXACT_ACCOUNT_SCOPE_REQUIRED')
        with localcontext() as precision:
            precision.prec=2048
            cash=decimal_value(observation.get('availableCash'),'available cash',positive=True)
            principal=cash*decimal_value(scope['capitalPercent'],'capital percent',positive=True)/100
        key=scope['accountFingerprint']+':'+scope['quoteCurrency']
        body={'scope':scope,'principal':text(principal),'occurredAt':now.isoformat()}
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if commit_guard is not None:commit_guard()
            prior=db.execute('SELECT payload FROM principals WHERE deployment_id=?',(scope['deploymentId'],)).fetchone()
            if prior:
                if json.loads(prior[0]).get('scope')!=scope:raise ValueError('KIS_OWNED_SCOPE_CHANGED')
            else:
                reserved=Decimal(0)
                for raw,digest in db.execute('SELECT payload,content_hash FROM principals WHERE account_currency=?',(key,)):
                    item=json.loads(raw)
                    if stable_sha256(item)!=digest:raise ValueError('KIS_OWNED_RESERVATION_SEAL_INVALID')
                    reserved+=decimal_value(item['principal'],'existing reserved principal')
                if reserved+principal>cash:raise ValueError('KIS_OWNED_CAPITAL_ALREADY_RESERVED')
                db.execute('INSERT INTO principals VALUES (?,?,?,?)',(scope['deploymentId'],key,json.dumps(body,sort_keys=True),stable_sha256(body)))
            # Cancellation/scope/freshness is checked inside this same local
            # transaction. Failure rolls back the append instead of rebasing it.
            if commit_guard is not None:commit_guard()
        return self.project(scope,{})

    def append_terminal_receipt(self,scope,receipt):
        if (receipt.get('deploymentId')!=scope['deploymentId'] or receipt.get('accountFingerprint')!=scope['accountFingerprint']
            or receipt.get('currency')!=scope['quoteCurrency'] or receipt.get('actualFeeObserved') is not False
            or receipt.get('costAccounting')!='CONSERVATIVE_RESERVE_NOT_ACTUAL_FEE'
            or receipt.get('terminalVerified') is not True or receipt.get('side') not in {'BUY','SELL'}):
            raise ValueError('KIS_OWNED_TERMINAL_RECEIPT_SCOPE_REQUIRED')
        members=[m for m in scope['members'] if m['strategyInstanceId']==receipt.get('strategyInstanceId')]
        if len(members)!=1 or members[0]['symbol']!=receipt.get('symbol'):raise ValueError('KIS_OWNED_MEMBER_SCOPE_CHANGED')
        quantity=decimal_value(receipt.get('quantity'),'terminal fill quantity',positive=True)
        if quantity!=quantity.to_integral_value():raise ValueError('KIS_OWNED_WHOLE_SHARES_REQUIRED')
        decimal_value(receipt.get('filledNotional'),'actual fill notional',positive=True)
        decimal_value(receipt.get('costReserve'),'estimated cost holdback',positive=True)
        digest=stable_sha256(receipt)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            principal=db.execute('SELECT payload,content_hash FROM principals WHERE deployment_id=?',(scope['deploymentId'],)).fetchone()
            if not principal or json.loads(principal[0]).get('scope')!=scope or stable_sha256(json.loads(principal[0]))!=principal[1]:
                raise ValueError('KIS_OWNED_PRINCIPAL_BINDING_INVALID')
            old=db.execute('SELECT content_hash FROM receipts WHERE event_key=?',(receipt['eventKey'],)).fetchone()
            if old:
                if old[0]!=digest:raise ValueError('KIS_OWNED_TERMINAL_RECEIPT_CHANGED')
                return False
            db.execute('INSERT INTO receipts VALUES (?,?,?,?)',(receipt['eventKey'],scope['deploymentId'],json.dumps(receipt,sort_keys=True),digest))
        return True

    def project(self,scope,marks):
        if not self.path.is_file():raise ValueError('KIS_OWNED_PRINCIPAL_NOT_INITIALIZED')
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            principal=db.execute('SELECT payload,content_hash FROM principals WHERE deployment_id=?',(scope['deploymentId'],)).fetchone()
            records=db.execute('SELECT payload,content_hash FROM receipts WHERE deployment_id=? ORDER BY rowid',(scope['deploymentId'],)).fetchall()
        if not principal:raise ValueError('KIS_OWNED_PRINCIPAL_NOT_INITIALIZED')
        body=json.loads(principal[0])
        if body['scope']!=scope or stable_sha256(body)!=principal[1]:raise ValueError('KIS_OWNED_PRINCIPAL_CHANGED')
        with localcontext() as precision:
            precision.prec=2048
            cash=decimal_value(body['principal'],'principal');gross=cash;holdback=Decimal(0)
            positions={m['strategyInstanceId']:Decimal(0) for m in scope['members']}
            for raw,digest in records:
                receipt=json.loads(raw)
                if stable_sha256(receipt)!=digest:raise ValueError('KIS_OWNED_RECEIPT_SEAL_INVALID')
                quantity=decimal_value(receipt['quantity'],'fill quantity');notional=decimal_value(receipt['filledNotional'],'fill notional')
                reserve=decimal_value(receipt['costReserve'],'cost holdback');sid=receipt['strategyInstanceId']
                gross+=notional if receipt['side']=='SELL' else -notional
                holdback+=reserve;cash=gross-holdback
                positions[sid]+=quantity if receipt['side']=='BUY' else -quantity
                if cash<0 or positions[sid]<0:raise ValueError('KIS_OWNED_OVERSPEND_OR_OVERSELL_RECONCILIATION_REQUIRED')
            equity=cash
            for sid,quantity in positions.items():
                if quantity and sid not in marks:equity=None;break
                if quantity:equity+=quantity*decimal_value(marks[sid],'current owned mark',positive=True)
        return {'equityScope':'OWNED_DEPLOYMENT_ONLY','capitalAccounting':'OWNED_CONSERVATIVE_COST_RESERVE',
            'deploymentEquity':text(equity) if equity is not None else None,'ownedCash':text(cash),'grossCash':text(gross),
            'costReserveOutstanding':text(holdback),'ownedPositions':{k:text(v) for k,v in positions.items()},
            'capitalScopeHash':scope['scopeHash'],'fillCount':len(records),'actualFeesReconciled':False,
            'netPerformanceStatus':'ESTIMATED_UNTIL_ACTUAL_COST_RECONCILIATION','orderAuthorityGranted':False}

    def account_owned_totals(self,scope):
        """Across every KIS portfolio in this account/currency, excluding manual holdings."""
        key=scope['accountFingerprint']+':'+scope['quoteCurrency']
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            rows=db.execute('SELECT payload,content_hash FROM principals WHERE account_currency=?',(key,)).fetchall()
        cash=Decimal(0);positions={}
        for raw,digest in rows:
            body=json.loads(raw)
            if stable_sha256(body)!=digest:raise ValueError('KIS_OWNED_ACCOUNT_PRINCIPAL_SEAL_INVALID')
            current=body['scope'];projection=self.project(current,{})
            cash+=decimal_value(projection['ownedCash'],'owned account cash')
            members={m['strategyInstanceId']:m for m in current['members']}
            for sid,quantity in projection['ownedPositions'].items():
                symbol=members[sid]['symbol'];positions[symbol]=positions.get(symbol,Decimal(0))+decimal_value(quantity,'owned account position')
        return {'ownedCash':text(cash),'ownedPositionsBySymbol':{k:text(v) for k,v in positions.items()}}


def request_session_date_from_dispatch(spec,dispatch):
    """Exact query date from the sealed WS event, never from local submit time."""
    metadata=dispatch.get('metadata') or {};ticket=checked(metadata.get('native_quantity_model'),RESULT)
    boundary=metadata.get('native_open_boundary') or {}
    contract=(sizing_from_artifact(spec.artifact) or {}).get('quantityContract') or {}
    if (not isinstance(boundary,dict) or ticket.get('boundaryHash')!=stable_sha256(boundary)
        or ticket.get('sourceArtifactHash')!=spec.artifact_hash or ticket.get('contractHash')!=contract.get('contractHash')
        or ticket.get('symbol')!=spec.symbol or ticket.get('exchange')!=contract.get('exchange')
        or boundary.get('provider')!='kis' or boundary.get('symbol')!=spec.symbol
        or boundary.get('sourceArtifactHash')!=spec.artifact_hash or boundary.get('attestation')!='KIS_WEBSOCKET'
        or boundary.get('recoveryOnly') is not False or boundary.get('promotionEligible') is not True):
        raise ValueError('KIS_IDENTITY_SEALED_MARKET_SESSION_BOUNDARY_REQUIRED')
    event=timestamp(boundary.get('startTime'))
    return event.astimezone(ZoneInfo('Asia/Seoul' if ticket['exchange']=='KRX' else 'America/New_York')).strftime('%Y%m%d')


def resolve_official_order_identity(spec,dispatch,official_rows,*,request_session_date):
    """Recover a missing ACK date from one exact official order query result.

    The caller supplies the exact request session date as a *query scope*, never
    as a broker date. Known ACK branch and ODNO are mandatory; matching by ticker,
    quantity, proximity or a guessed date is forbidden. This pure helper neither
    writes a ledger nor grants authority, and may resolve an unfilled order.
    """
    day=str(request_session_date or '')
    if not re.fullmatch('[0-9]{8}',day):raise ValueError('KIS_IDENTITY_EXACT_REQUEST_SESSION_DATE_REQUIRED')
    try:datetime.strptime(day,'%Y%m%d')
    except ValueError:raise ValueError('KIS_IDENTITY_EXACT_REQUEST_SESSION_DATE_REQUIRED') from None
    if day!=request_session_date_from_dispatch(spec,dispatch):raise ValueError('KIS_IDENTITY_QUERY_DATE_DIFFERS_FROM_SEALED_MARKET_SESSION')
    metadata=dispatch.get('metadata') or {};ticket=checked(metadata.get('native_quantity_model'),RESULT)
    contract=(sizing_from_artifact(spec.artifact) or {}).get('quantityContract') or {}
    branch=str(dispatch.get('organization_no') or '');number=str(dispatch.get('broker_order_id') or '')
    if (dispatch.get('broker_id')!='kis' or not re.fullmatch('[0-9]{1,16}',branch)
        or not re.fullmatch('[0-9]{1,16}',number)):
        raise ValueError('KIS_IDENTITY_KNOWN_EXACT_ACK_BRANCH_AND_ODNO_REQUIRED')
    if (dispatch.get('side') not in {'BUY','SELL'} or ticket.get('side')!=dispatch['side']
        or ticket.get('symbol')!=spec.symbol or ticket.get('sourceArtifactHash')!=spec.artifact_hash
        or ticket.get('contractHash')!=contract.get('contractHash')
        or ticket.get('exchange')!=contract.get('exchange') or ticket.get('currency')!=contract.get('quoteCurrency')
        or metadata.get('deployment_id')!=ticket.get('deploymentId') or not ticket.get('deploymentId')
        or metadata.get('strategy_instance_id')!=spec.strategy_instance_id
        or metadata.get('source_artifact_hash')!=spec.artifact_hash or metadata.get('portfolio_hash')!=spec.portfolio_hash
        or metadata.get('native_account_fingerprint')!=ticket.get('accountFingerprint')
        or not re.fullmatch('[a-f0-9]{64}',str(ticket.get('accountFingerprint') or ''))):
        raise ValueError('KIS_IDENTITY_EXACT_OWNED_DISPATCH_REQUIRED')
    existing_day=str(dispatch.get('order_date') or '')
    provisional=dispatch.get('broker_order_identity_verified') is not True and dispatch.get('order_date_source') in {
        'KST_SUBMISSION_DATE_PROVISIONAL','MARKET_SESSION_DATE_PROVISIONAL','AWAITING_BROKER_TRADING_DATE'}
    if existing_day and existing_day!=day and not provisional:raise ValueError('KIS_IDENTITY_EXISTING_DATE_CONFLICT')
    if not isinstance(official_rows,list) or any(not isinstance(row,dict) for row in official_rows):
        raise ValueError('KIS_IDENTITY_COMPLETE_OFFICIAL_ORDER_ROWS_REQUIRED')
    matches=[row for row in official_rows if str(row.get('ord_gno_brno') or '')==branch and str(row.get('odno') or '')==number]
    if len(matches)!=1:raise ValueError('KIS_IDENTITY_EXACT_ACK_NOT_FOUND_OR_AMBIGUOUS')
    official=matches[0];domestic=ticket['exchange']=='KRX'
    if (str(official.get('ord_dt') or '')!=day or official.get('pdno')!=spec.symbol
        or official.get('sll_buy_dvsn_cd')!=('02' if dispatch['side']=='BUY' else '01')
        or (not domestic and (official.get('ovrs_excg_cd')!=ticket['exchange'] or official.get('tr_crcy_cd')!='USD'))
        or decimal_value(official.get('ord_qty' if domestic else 'ft_ord_qty'),'official ordered shares',positive=True)
            !=decimal_value(ticket.get('quantity'),'sealed ordered shares',positive=True)):
        raise ValueError('KIS_IDENTITY_OFFICIAL_EXACT_ORDER_SCOPE_MISMATCH')
    return {'order_date':str(official['ord_dt']),'organization_no':str(official['ord_gno_brno']),
        'broker_order_id':str(official['odno']),'broker_order_key':day+':'+branch+':'+number,
        'broker_order_identity_verified':True,'order_date_source':'OFFICIAL_ORDER_QUERY_EXACT_COMPOSITE_IDENTITY',
        'officialReceiptHash':stable_sha256(official),'verifiedDispatchHash':stable_sha256(dispatch),
        'requestSessionDate':day,'requestSessionBoundaryHash':ticket['boundaryHash'],
        'querySessionDateWasProvisional':True,'orderAuthorityGranted':False}


def terminal_receipt(scope,spec,dispatch,official):
    metadata=dispatch.get('metadata') or {};ticket=checked(metadata.get('native_quantity_model'),RESULT)
    key={name:str(dispatch.get(name) or '') for name in ('order_date','organization_no','broker_order_id')}
    if not re.fullmatch('[0-9]{8}',key['order_date']) or not all(re.fullmatch('[0-9]{1,16}',key[k]) for k in ('organization_no','broker_order_id')):
        raise ValueError('KIS_OWNED_EXACT_BROKER_ACK_KEY_REQUIRED')
    expected={'deployment_id':scope['deploymentId'],'strategy_instance_id':spec.strategy_instance_id,
              'source_artifact_hash':spec.artifact_hash,'portfolio_hash':spec.portfolio_hash,
              'native_account_fingerprint':scope['accountFingerprint']}
    if any(metadata.get(k)!=v for k,v in expected.items()) or ticket['accountFingerprint']!=scope['accountFingerprint']:
        raise ValueError('KIS_OWNED_EXACT_PERSISTED_DISPATCH_REQUIRED')
    if (str(official.get('ord_dt'))!=key['order_date'] or str(official.get('ord_gno_brno'))!=key['organization_no']
        or str(official.get('odno'))!=key['broker_order_id'] or official.get('pdno')!=spec.symbol
        or official.get('sll_buy_dvsn_cd')!=('02' if dispatch.get('side')=='BUY' else '01')):
        raise ValueError('KIS_OWNED_OFFICIAL_COMPOSITE_ORDER_KEY_MISMATCH')
    domestic=ticket['exchange']=='KRX'
    if not domestic and (official.get('ovrs_excg_cd')!=ticket['exchange'] or official.get('tr_crcy_cd')!='USD'):
        raise ValueError('KIS_OWNED_OFFICIAL_VENUE_CURRENCY_CHANGED')
    quantity=decimal_value(official.get('tot_ccld_qty' if domestic else 'ft_ccld_qty'),'actual total fill quantity')
    ordered=decimal_value(official.get('ord_qty' if domestic else 'ft_ord_qty'),'official order quantity',positive=True)
    remaining=decimal_value(official.get('rmn_qty' if domestic else 'nccs_qty'),'official remaining quantity')
    if ordered!=decimal_value(ticket['quantity'],'ticket quantity') or quantity>ordered or remaining:
        raise ValueError('KIS_OWNED_ORDER_NOT_TERMINAL_OR_SCOPE_CHANGED')
    if quantity!=ordered:
        if domestic:
            canceled=decimal_value(official.get('cnc_cfrm_qty'),'confirmed cancel quantity')
            rejected=decimal_value(official.get('rjct_qty'),'rejected quantity')
            if quantity+canceled+rejected!=ordered:raise ValueError('KIS_OWNED_DOMESTIC_TERMINAL_ACCOUNTING_INCOMPLETE')
        elif str(official.get('prcs_stat_name') or '').strip() not in {'취소','취소완료','거부','주문거부'}:
            raise ValueError('KIS_OWNED_US_TERMINAL_STATUS_UNPROVEN')
    if not quantity:return None
    order_time=str(official.get('ord_tmd') or '')
    if not re.fullmatch('[0-9]{6}',order_time):raise ValueError('KIS_OWNED_OFFICIAL_ORDER_TIME_REQUIRED')
    notional=decimal_value(official.get('tot_ccld_amt' if domestic else 'ft_ccld_amt3'),'official fill amount',positive=True)
    with localcontext() as precision:
        precision.prec=2048
        reserve=max(decimal_value(ticket['minimumCostReserve'],'minimum reserve'),notional*decimal_value(ticket['costReserveRate'],'cost bound'))
    return {'eventKey':'kis:'+scope['accountFingerprint']+':'+ticket['exchange']+':'+':'.join(key.values()),
        'deploymentId':scope['deploymentId'],'strategyInstanceId':spec.strategy_instance_id,'symbol':spec.symbol,
        'side':dispatch['side'],'quantity':text(quantity),'filledNotional':text(notional),'costReserve':text(reserve),
        'currency':ticket['currency'],'accountFingerprint':scope['accountFingerprint'],'terminalVerified':True,
        'actualFeeObserved':False,'costAccounting':'CONSERVATIVE_RESERVE_NOT_ACTUAL_FEE',
        'orderDate':key['order_date'],'orderTime':order_time,'orderNo':key['broker_order_id'],'receiptHash':stable_sha256(official)}
