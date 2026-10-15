"""Sealed append-only history receipts, never current account/market evidence.

Only fully verified terminal orders enter this cache. Every submission still
needs its own fresh broker/account/rules preflight and existing order authority.
"""
from __future__ import annotations
from contextlib import closing
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
import json,re,sqlite3
from trading_runtime.artifact_governance import stable_sha256
from .native_spot_quantity import decimal_value

SCHEMA='verified-terminal-order-history-cache-v1'
SPOT='COMPLETE_TERMINAL_SPOT_TRADES_AND_ACTUAL_FEES'
KIS='EXACT_OFFICIAL_TERMINAL_KIS_RECEIPT'


def terminal_identity(scope,spec,dispatch):
    metadata=dispatch.get('metadata') or {};number=str(dispatch.get('broker_order_id') or '')
    if (spec.broker_id not in {'binance','upbit','kis'} or dispatch.get('broker_id',spec.broker_id)!=spec.broker_id
        or not number or number=='-' or dispatch.get('side') not in {'BUY','SELL'}
        or metadata.get('deployment_id')!=scope['deploymentId']
        or metadata.get('strategy_instance_id')!=spec.strategy_instance_id
        or metadata.get('source_artifact_hash')!=spec.artifact_hash or metadata.get('portfolio_hash')!=spec.portfolio_hash
        or metadata.get('native_account_fingerprint')!=scope['accountFingerprint']):
        raise ValueError('TERMINAL_CACHE_EXACT_OWNED_DISPATCH_REQUIRED')
    immutable={key:dispatch[key] for key in ('order_id','idempotency_key','broker_id','broker_order_id','symbol','side',
        'quantity','reference_price','price','order_type','time_in_force','order_date','organization_no','broker_order_key') if key in dispatch}
    immutable['metadata']=metadata
    identity={'version':SCHEMA,'brokerId':spec.broker_id,'deploymentId':scope['deploymentId'],
        'accountProfileId':scope['accountProfileId'],'accountFingerprint':scope['accountFingerprint'],
        'credentialConfigurationHash':scope['credentialConfigurationHash'],'deploymentScopeHash':scope['scopeHash'],
        'strategyInstanceId':spec.strategy_instance_id,'sourceArtifactHash':spec.artifact_hash,'portfolioHash':spec.portfolio_hash,
        'symbol':spec.symbol,'side':dispatch['side'],'brokerOrderId':number,'quoteCurrency':scope['quoteCurrency'],
        'dispatchOrderId':str(dispatch.get('order_id') or ''),'idempotencyKey':str(dispatch.get('idempotency_key') or ''),
        'immutableDispatchHash':stable_sha256(immutable)}
    if spec.broker_id=='kis':
        from .native_spot_quantity import sizing_from_artifact
        identity.update({'exchange':sizing_from_artifact(spec.artifact)['quantityContract']['exchange'],
            'orderDate':str(dispatch.get('order_date') or ''),'organizationNo':str(dispatch.get('organization_no') or '')})
    return _identity(identity)


def _identity(identity):
    if not isinstance(identity,dict) or identity.get('version')!=SCHEMA or identity.get('brokerId') not in {'binance','upbit','kis'}:
        raise ValueError('TERMINAL_CACHE_IDENTITY_INVALID')
    for key in ('accountFingerprint','credentialConfigurationHash','deploymentScopeHash','sourceArtifactHash','portfolioHash','immutableDispatchHash'):
        if not re.fullmatch('[a-f0-9]{64}',str(identity.get(key) or '')):raise ValueError('TERMINAL_CACHE_PROOF_HASH_REQUIRED')
    for key in ('deploymentId','accountProfileId','strategyInstanceId','symbol','brokerOrderId','quoteCurrency'):
        if not isinstance(identity.get(key),str) or not identity[key]:raise ValueError('TERMINAL_CACHE_EXACT_IDENTITY_REQUIRED')
    if identity.get('side') not in {'BUY','SELL'}:raise ValueError('TERMINAL_CACHE_SIDE_INVALID')
    if identity['brokerId']=='kis' and (identity.get('exchange') not in {'KRX','NASD','NYSE','AMEX'}
        or not re.fullmatch('[0-9]{8}',str(identity.get('orderDate') or ''))
        or not re.fullmatch('[0-9]{1,16}',str(identity.get('organizationNo') or ''))):
        raise ValueError('TERMINAL_CACHE_KIS_COMPOSITE_IDENTITY_REQUIRED')
    return dict(identity)


def _keys(identity):
    fallback='BROKER:'+stable_sha256({key:identity.get(key,'') for key in ('symbol','brokerOrderId','exchange','orderDate','organizationNo')})
    slot=stable_sha256({'brokerId':identity['brokerId'],'deploymentId':identity['deploymentId'],
        'localDispatch':identity['idempotencyKey'] or identity['dispatchOrderId'] or fallback})
    broker=stable_sha256({key:identity.get(key,'') for key in ('brokerId','accountFingerprint','symbol','brokerOrderId','exchange','orderDate','organizationNo')})
    return slot,broker


def _validate(identity,receipts,proof):
    _identity(identity)
    if (not isinstance(receipts,list) or any(not isinstance(row,dict) for row in receipts)
        or not isinstance(proof,dict) or proof.get('terminalVerified') is not True or proof.get('complete') is not True
        or proof.get('identityHash')!=stable_sha256(identity) or proof.get('receiptsHash')!=stable_sha256(receipts)
        or proof.get('orderAuthorityGranted') is not False
        or not re.fullmatch('[a-f0-9]{64}',str(proof.get('brokerResponseHash') or ''))):
        raise ValueError('TERMINAL_CACHE_COMPLETE_TERMINAL_PROOF_REQUIRED')
    total=Decimal(0);events=set()
    for receipt in receipts:
        if (receipt.get('accountFingerprint')!=identity['accountFingerprint'] or receipt.get('symbol')!=identity['symbol']
            or receipt.get('strategyInstanceId')!=identity['strategyInstanceId'] or receipt.get('side')!=identity['side']):
            raise ValueError('TERMINAL_CACHE_RECEIPT_SCOPE_CHANGED')
        quantity=decimal_value(receipt.get('quantity'),'verified terminal quantity',positive=True);total+=quantity
        if identity['brokerId']=='kis':
            event=receipt.get('eventKey')
            if (receipt.get('terminalVerified') is not True or receipt.get('actualFeeObserved') is not False
                or receipt.get('deploymentId')!=identity['deploymentId'] or receipt.get('currency')!=identity['quoteCurrency']
                or receipt.get('costAccounting')!='CONSERVATIVE_RESERVE_NOT_ACTUAL_FEE'
                or receipt.get('orderDate')!=identity['orderDate'] or receipt.get('orderNo')!=identity['brokerOrderId']
                or quantity!=quantity.to_integral_value()
                or not re.fullmatch('[a-f0-9]{64}',str(receipt.get('receiptHash') or ''))):
                raise ValueError('TERMINAL_CACHE_KIS_RECEIPT_INCOMPLETE')
            decimal_value(receipt.get('filledNotional'),'actual gross fill notional',positive=True)
            decimal_value(receipt.get('costReserve'),'estimated cost holdback',positive=True)
        else:
            event=receipt.get('eventId')
            if receipt.get('brokerOrderId')!=identity['brokerOrderId'] or receipt.get('feeCurrency') not in {'BASE',identity['quoteCurrency']}:
                raise ValueError('TERMINAL_CACHE_ACTUAL_SPOT_FEE_SCOPE_UNPROVEN')
            decimal_value(receipt.get('price'),'actual trade price',positive=True)
            decimal_value(receipt.get('fee'),'actual paid fee')
            stamp=datetime.fromisoformat(str(receipt.get('occurredAt') or '').replace('Z','+00:00'))
            if stamp.tzinfo is None:raise ValueError('TERMINAL_CACHE_ACTUAL_FILL_TIMESTAMP_REQUIRED')
        if not isinstance(event,str) or not event or event in events:raise ValueError('TERMINAL_CACHE_DUPLICATE_OR_MISSING_EVENT')
        events.add(event)
    if total!=decimal_value(proof.get('executedQuantity'),'complete terminal executed quantity'):
        raise ValueError('TERMINAL_CACHE_EXECUTED_TOTAL_UNPROVEN')
    if identity['brokerId']=='kis':
        if proof.get('kind')!=KIS or proof.get('officialTerminalReceiptComplete') is not True or proof.get('actualFeesObserved') is not False:
            raise ValueError('TERMINAL_CACHE_KIS_EXACT_TERMINAL_PROOF_REQUIRED')
        facts=proof.get('terminalFacts') or {}
        ordered=decimal_value(facts.get('orderedQuantity'),'official ordered quantity',positive=True)
        if (ordered!=ordered.to_integral_value() or total>ordered
            or decimal_value(facts.get('remainingQuantity'),'official remaining quantity')!=0):
            raise ValueError('TERMINAL_CACHE_KIS_ORDER_NOT_TERMINAL')
        if total!=ordered:
            if identity['exchange']=='KRX':
                if total+decimal_value(facts.get('canceledQuantity'),'confirmed canceled shares')+decimal_value(facts.get('rejectedQuantity'),'rejected shares')!=ordered:
                    raise ValueError('TERMINAL_CACHE_KIS_ORDER_NOT_TERMINAL')
            elif facts.get('terminalStatus') not in {'취소','취소완료','거부','주문거부'}:
                raise ValueError('TERMINAL_CACHE_KIS_ORDER_NOT_TERMINAL')
    elif (proof.get('kind')!=SPOT or proof.get('allTradesComplete') is not True or proof.get('actualFeesObserved') is not True
        or proof.get('terminalStatus') not in ({'FILLED','CANCELED','EXPIRED','REJECTED'} if identity['brokerId']=='binance' else {'done','cancel'})
        or not re.fullmatch('[a-f0-9]{64}',str(proof.get('allTradesHash') or ''))
        or type(proof.get('tradeCount')) is not int or proof['tradeCount']<0
        or (bool(total)!=bool(proof['tradeCount']))
        or (identity['brokerId']=='binance' and proof['tradeCount']!=len(receipts))):
        raise ValueError('TERMINAL_CACHE_ALL_TRADES_AND_FEES_REQUIRED')


def terminal_proof(identity,receipts,*,broker_response,executed_quantity,all_trades=None):
    if identity['brokerId']!='kis' and not isinstance(all_trades,list):raise ValueError('TERMINAL_CACHE_ALL_TRADES_AND_FEES_REQUIRED')
    quantity_field=('tot_ccld_qty' if identity.get('exchange')=='KRX' else 'ft_ccld_qty') if identity['brokerId']=='kis' else ('executedQty' if identity['brokerId']=='binance' else 'executed_volume')
    if decimal_value(executed_quantity,'terminal executed shares')!=decimal_value(broker_response.get(quantity_field),'official terminal executed shares'):
        raise ValueError('TERMINAL_CACHE_EXECUTED_TOTAL_UNPROVEN')
    proof={'kind':KIS if identity['brokerId']=='kis' else SPOT,'terminalVerified':True,'complete':True,
        'identityHash':stable_sha256(identity),'receiptsHash':stable_sha256(receipts),
        'executedQuantity':str(executed_quantity),'brokerResponseHash':stable_sha256(broker_response),'orderAuthorityGranted':False}
    if identity['brokerId']=='kis':
        domestic=identity['exchange']=='KRX'
        if (str(broker_response.get('ord_dt'))!=identity['orderDate'] or str(broker_response.get('ord_gno_brno'))!=identity['organizationNo']
            or str(broker_response.get('odno'))!=identity['brokerOrderId'] or broker_response.get('pdno')!=identity['symbol']
            or broker_response.get('sll_buy_dvsn_cd')!=('02' if identity['side']=='BUY' else '01')
            or (not domestic and (broker_response.get('ovrs_excg_cd')!=identity['exchange'] or broker_response.get('tr_crcy_cd')!=identity['quoteCurrency']))):
            raise ValueError('TERMINAL_CACHE_KIS_OFFICIAL_COMPOSITE_IDENTITY_CHANGED')
        proof.update({'officialTerminalReceiptComplete':True,'actualFeesObserved':False,'terminalFacts':{
            'orderedQuantity':broker_response.get('ord_qty' if domestic else 'ft_ord_qty'),
            'remainingQuantity':broker_response.get('rmn_qty' if domestic else 'nccs_qty'),
            'terminalStatus':broker_response.get('prcs_stat_name'),
            'canceledQuantity':broker_response.get('cnc_cfrm_qty'),'rejectedQuantity':broker_response.get('rjct_qty')}})
    else:proof.update({'allTradesComplete':True,'actualFeesObserved':True,'terminalStatus':broker_response.get('status' if identity['brokerId']=='binance' else 'state'),'allTradesHash':stable_sha256(all_trades),'tradeCount':len(all_trades)})
    _validate(identity,receipts,proof)
    return proof


class VerifiedTerminalCache:
    def __init__(self,path):self.path=Path(path)

    @staticmethod
    def _decode(row,identity):
        try:
            body=json.loads(row[2])
            if not isinstance(body,dict) or stable_sha256(body)!=row[3] or body.get('version')!=SCHEMA:raise ValueError('TERMINAL_CACHE_SEAL_INVALID')
            stored=body.get('identity');_validate(stored,body.get('receipts'),body.get('proof'))
            if _keys(stored)!=(row[0],row[1]):raise ValueError('TERMINAL_CACHE_INDEX_BINDING_INVALID')
            if stored!=identity:raise ValueError('TERMINAL_CACHE_IMMUTABLE_DISPATCH_SCOPE_CHANGED')
            return body
        except (KeyError,TypeError,json.JSONDecodeError):raise ValueError('TERMINAL_CACHE_RECORD_CORRUPT') from None

    def get(self,identity):
        identity=_identity(identity);slot,broker=_keys(identity)
        if not self.path.exists():return None
        try:
            with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                rows=db.execute('SELECT slot_key,broker_key,payload,content_hash FROM terminal_receipts WHERE slot_key=? OR broker_key=?',(slot,broker)).fetchall()
            if not rows:return None
            if len(rows)!=1:raise ValueError('TERMINAL_CACHE_AMBIGUOUS_ATTRIBUTION')
            return self._decode(rows[0],identity)
        except sqlite3.Error:raise ValueError('TERMINAL_CACHE_DATABASE_INVALID') from None

    def put(self,identity,receipts,proof):
        identity=_identity(identity);_validate(identity,receipts,proof);slot,broker=_keys(identity)
        body={'version':SCHEMA,'identity':identity,'receipts':receipts,'proof':proof}
        digest=stable_sha256(body)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        try:
            with closing(sqlite3.connect(self.path)) as db:
                db.execute('CREATE TABLE IF NOT EXISTS terminal_receipts (slot_key TEXT PRIMARY KEY, broker_key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, content_hash TEXT NOT NULL)')
                for op in ('UPDATE','DELETE'):
                    db.execute(f"CREATE TRIGGER IF NOT EXISTS terminal_cache_no_{op.lower()} BEFORE {op} ON terminal_receipts BEGIN SELECT RAISE(ABORT,'verified-terminal-cache-append-only'); END")
                db.execute('BEGIN IMMEDIATE')
                rows=db.execute('SELECT slot_key,broker_key,payload,content_hash FROM terminal_receipts WHERE slot_key=? OR broker_key=?',(slot,broker)).fetchall()
                if rows:
                    if len(rows)!=1:raise ValueError('TERMINAL_CACHE_AMBIGUOUS_ATTRIBUTION')
                    previous=self._decode(rows[0],identity)
                    if previous!=body:raise ValueError('TERMINAL_CACHE_TERMINAL_RECEIPT_CHANGED')
                    db.commit();return False
                db.execute('INSERT INTO terminal_receipts VALUES (?,?,?,?)',(slot,broker,json.dumps(body,sort_keys=True),digest));db.commit()
            return True
        except sqlite3.Error:raise ValueError('TERMINAL_CACHE_DATABASE_INVALID') from None
