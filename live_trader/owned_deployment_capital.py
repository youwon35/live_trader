"""Deployment-owned capital, never the account-wide holdings/equity.

Principal is fixed once after the existing native account review. A transaction
prevents two deployments claiming the same initial available cash. Fills are
append-only and must be independently correlated to an existing dispatch.
"""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path
import json, sqlite3
from contextlib import contextmanager
from trading_runtime.artifact_governance import stable_sha256
from .native_spot_quantity import decimal_value

SCHEMA="live-owned-capital-v1"


def reviewed_scope(deployment, account, member_bindings):
    permissions=deployment.get("permissions") or {}
    if deployment.get("environment")!="LIVE" or deployment.get("lifecycle") not in {"before-live-small","live-small","live-full"}:
        raise ValueError("OWNED_CAPITAL_NATIVE_ACCOUNT_REVIEW_REQUIRED")
    if not permissions.get("liveReviewBindingHash") or deployment.get("accountId")!=account.get("accountBindingId") or deployment.get("route")!=account.get("brokerId"):
        raise ValueError("OWNED_CAPITAL_EXACT_ACCOUNT_BINDING_REQUIRED")
    expected={"liveAccountProfileId":"profileId","liveAccountFingerprint":"accountFingerprint","liveAccountCredentialConfigurationHash":"credentialConfigurationHash"}
    if any(permissions.get(key)!=account.get(field) for key,field in expected.items()):
        raise ValueError("OWNED_CAPITAL_SAVED_ACCOUNT_CHANGED")
    policy=permissions.get("liveCapitalPolicy") or {}
    percent=decimal_value(policy.get("percent"),"capital percent",positive=True)
    if policy.get("basis")!="ACCOUNT_AVAILABLE_CASH" or percent>100:
        raise ValueError("OWNED_CAPITAL_REVIEWED_INITIAL_BASIS_MISSING")
    if not isinstance(member_bindings,list) or not member_bindings or len(member_bindings)>32:
        raise ValueError("OWNED_CAPITAL_MEMBER_SCOPE_MISSING")
    ids=[row.get("strategyInstanceId") for row in member_bindings]
    if any(not isinstance(v,str) or not v for v in ids) or len(set(ids))!=len(ids):
        raise ValueError("OWNED_CAPITAL_MEMBER_SCOPE_AMBIGUOUS")
    required={"strategyInstanceId","sourceArtifactHash","portfolioHash","symbol","quoteCurrency"}
    if any(set(row)!=required or any(not isinstance(v,str) or not v for v in row.values()) for row in member_bindings):
        raise ValueError("OWNED_CAPITAL_MEMBER_SCOPE_INVALID")
    quotes={row["quoteCurrency"] for row in member_bindings}
    if len(quotes)!=1:raise ValueError("OWNED_CAPITAL_SINGLE_QUOTE_CURRENCY_REQUIRED")
    body={"schemaVersion":SCHEMA,"deploymentId":deployment["deploymentId"],"accountFingerprint":account["accountFingerprint"],"credentialConfigurationHash":account["credentialConfigurationHash"],"accountProfileId":account["profileId"],"reviewBindingHash":permissions["liveReviewBindingHash"],"capitalPercent":str(percent),"quoteCurrency":next(iter(quotes)),"members":sorted(member_bindings,key=lambda x:x["strategyInstanceId"])}
    return {**body,"scopeHash":stable_sha256(body)}


class OwnedCapitalStore:
    def __init__(self,path):self.path=Path(path)

    def _read(self):
        if not self.path.exists():return None
        return sqlite3.connect(self.path.resolve().as_uri()+"?mode=ro",uri=True)

    @contextmanager
    def _connection(self):
        db=sqlite3.connect(self.path)
        try:
            with db:yield db
        finally:db.close()

    def initialize(self,scope,observation,*,now=None):
        """Call only from the user's reviewed manual start; never an inactive import."""
        now=now or datetime.now(timezone.utc)
        if observation.get("accountBinding")!="SAVED_CREDENTIAL_CONFIGURATION_OBSERVED" or observation.get("profileId")!=scope["accountProfileId"] or observation.get("currency")!=scope["quoteCurrency"] or observation.get("openOrders")!="SYMBOL_NONE_OBSERVED":
            raise ValueError("OWNED_CAPITAL_FRESH_FREE_CASH_NOT_VERIFIED")
        seen=datetime.fromisoformat(str(observation.get("asOf") or "").replace("Z","+00:00"))
        if seen.tzinfo is None or not 0<=(now-seen).total_seconds()<=15:
            raise ValueError("OWNED_CAPITAL_INITIAL_OBSERVATION_STALE")
        with localcontext() as context:
            context.prec=2048
            available=decimal_value(observation.get("availableCash"),"initial available cash",positive=True)
            principal=available*decimal_value(scope["capitalPercent"],"capital percent")/100
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS principals (deployment_id TEXT PRIMARY KEY, account_key TEXT NOT NULL, scope_json TEXT NOT NULL, principal TEXT NOT NULL, occurred_at TEXT NOT NULL, content_hash TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS fills (event_key TEXT PRIMARY KEY, deployment_id TEXT NOT NULL, payload TEXT NOT NULL, content_hash TEXT NOT NULL)")
            for table in ("principals", "fills"):
                for operation in ("UPDATE", "DELETE"):
                    db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation.lower()} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'native-owned-capital-append-only'); END")
            prior=db.execute("SELECT scope_json,principal FROM principals WHERE deployment_id=?",(scope["deploymentId"],)).fetchone()
            if prior:
                if json.loads(prior[0])!=scope:raise ValueError("OWNED_CAPITAL_SCOPE_CHANGED")
                return self.project(scope,{})
            # Existing allocations reserve principal until an explicit audited release
            # exists. Never silently reuse all cash for another deployment.
            reserved=sum((decimal_value(row[0],"reserved principal") for row in db.execute("SELECT principal FROM principals WHERE account_key=?",(scope["accountFingerprint"],))),Decimal(0))
            if reserved+principal>available:
                raise ValueError("OWNED_CAPITAL_ALREADY_RESERVED_FOR_ANOTHER_DEPLOYMENT")
            body={"scope":scope,"principal":str(principal),"occurredAt":now.isoformat()}
            db.execute("INSERT INTO principals VALUES (?,?,?,?,?,?)",(scope["deploymentId"],scope["accountFingerprint"],json.dumps(scope,sort_keys=True),str(principal),now.isoformat(),stable_sha256(body)))
        return self.project(scope,{})

    def append_verified_fill(self,scope,fill,dispatch):
        """Append an actual immutable fill only with exact persisted dispatch binding."""
        required={"eventId","brokerOrderId","strategyInstanceId","symbol","side","quantity","price","fee","feeCurrency","occurredAt","accountFingerprint"}
        if not isinstance(fill,dict) or set(fill)!=required or fill["side"] not in {"BUY","SELL"}:
            raise ValueError("OWNED_FILL_COMPLETE_EXACT_FIELDS_REQUIRED")
        members=[x for x in scope["members"] if x["strategyInstanceId"]==fill["strategyInstanceId"]]
        if len(members)!=1 or fill["symbol"]!=members[0]["symbol"] or fill["accountFingerprint"]!=scope["accountFingerprint"]:
            raise ValueError("OWNED_FILL_SCOPE_CHANGED")
        metadata=dispatch.get("metadata") or {}
        if dispatch.get("brokerOrderId")!=fill["brokerOrderId"] or dispatch.get("persistedDispatchVerified") is not True or metadata.get("deployment_id")!=scope["deploymentId"] or metadata.get("strategy_instance_id")!=fill["strategyInstanceId"] or metadata.get("source_artifact_hash")!=members[0]["sourceArtifactHash"] or metadata.get("portfolio_hash")!=members[0]["portfolioHash"]:
            raise ValueError("OWNED_FILL_EXACT_PERSISTED_DISPATCH_REQUIRED")
        if fill["feeCurrency"] not in {scope["quoteCurrency"],"BASE"}:
            raise ValueError("OWNED_FILL_THIRD_ASSET_FEE_VALUATION_UNPROVEN")
        decimal_value(fill["quantity"],"filled quantity",positive=True);decimal_value(fill["price"],"filled price",positive=True);decimal_value(fill["fee"],"actual fee")
        content=stable_sha256(fill)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            prior=db.execute("SELECT scope_json FROM principals WHERE deployment_id=?",(scope["deploymentId"],)).fetchone()
            if not prior or json.loads(prior[0])!=scope:raise ValueError("OWNED_FILL_PRINCIPAL_SCOPE_MISSING")
            old=db.execute("SELECT content_hash FROM fills WHERE event_key=?",(fill["eventId"],)).fetchone()
            if old:
                if old[0]!=content:raise ValueError("OWNED_FILL_IDEMPOTENCY_CONFLICT")
                return False
            db.execute("INSERT INTO fills VALUES (?,?,?,?)",(fill["eventId"],scope["deploymentId"],json.dumps(fill,sort_keys=True),content))
        return True

    def project(self,scope,marks):
        db=self._read()
        if db is None:raise ValueError("OWNED_DEPLOYMENT_PRINCIPAL_NOT_INITIALIZED")
        try:
            row=db.execute("SELECT scope_json,principal,occurred_at,content_hash FROM principals WHERE deployment_id=?",(scope["deploymentId"],)).fetchone()
            if not row or json.loads(row[0])!=scope:raise ValueError("OWNED_CAPITAL_CURRENT_BINDING_CHANGED")
            if stable_sha256({"scope":scope,"principal":row[1],"occurredAt":row[2]})!=row[3]:raise ValueError("OWNED_CAPITAL_PRINCIPAL_SEAL_INVALID")
            records=db.execute("SELECT payload,content_hash FROM fills WHERE deployment_id=? ORDER BY rowid",(scope["deploymentId"],)).fetchall()
        finally:db.close()
        with localcontext() as precision:
            precision.prec=2048
            cash=decimal_value(row[1],"principal")
            positions={m["strategyInstanceId"]:Decimal(0) for m in scope["members"]}
            for raw,digest in records:
                fill=json.loads(raw)
                if stable_sha256(fill)!=digest:raise ValueError("OWNED_FILL_SEAL_INVALID")
                sid=fill["strategyInstanceId"]
                quantity=decimal_value(fill["quantity"],"quantity")
                price=decimal_value(fill["price"],"price")
                fee=decimal_value(fill["fee"],"fee")
                if fill["side"]=="BUY":
                    cash-=quantity*price+(fee if fill["feeCurrency"]==scope["quoteCurrency"] else 0)
                    positions[sid]+=quantity-(fee if fill["feeCurrency"]=="BASE" else 0)
                else:
                    positions[sid]-=quantity+(fee if fill["feeCurrency"]=="BASE" else 0)
                    cash+=quantity*price-(fee if fill["feeCurrency"]==scope["quoteCurrency"] else 0)
                if cash<0 or positions[sid]<0:raise ValueError("OWNED_FILL_OVESPEND_OR_OVERSELL")
            equity=cash
            for sid,quantity in positions.items():
                if quantity:
                    if marks.get(sid) is None:
                        equity=None
                        break
                    equity+=quantity*decimal_value(marks.get(sid),"owned position current mark",positive=True)
        return {"equityScope":"OWNED_DEPLOYMENT_ONLY","deploymentEquity":str(equity) if equity is not None else None,"ownedCash":str(cash),"ownedPositions":{k:str(v) for k,v in positions.items()},"initialPrincipal":row[1],"capitalScopeHash":scope["scopeHash"],"fillCount":len(records),"orderAuthorityGranted":False}
