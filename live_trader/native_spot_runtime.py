"""Production wiring for native spot quantity contexts. No order/authority APIs."""
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
from trading_runtime.artifact_governance import stable_sha256, safe_file_token
from . import paper_candidate_inbox as inbox
from .account_profiles import saved_account_profiles
from .owned_deployment_capital import OwnedCapitalStore, reviewed_scope
from .native_spot_quantity import sizing_from_artifact, decimal_value
from .native_spot_preflight import observe_native_spot
from .read_only_account import ReadOnlyBrokerReader, runtime_settings, ReadFailure
from .verified_terminal_cache import VerifiedTerminalCache,terminal_identity,terminal_proof

def current_deployment(deployment_id,specs,roots):
    matches=[]
    for root in roots:
        root=Path(root).resolve()
        entry=inbox._registry(root)["entries"].get(deployment_id)
        if entry:
            definition=inbox._read(root/"deployments"/"definitions"/(safe_file_token(deployment_id,"deployment")+".json"),root)
            digest=stable_sha256({k:v for k,v in definition.items() if k!="definitionHash"})
            if definition.get("definitionHash")!=digest or entry.get("definitionHash")!=digest or any(definition.get(k)!=entry.get(k) for k in ("deploymentId","accountId","environment","symbol","instrumentId","route","strategyArtifact","portfolioArtifact")):
                raise ValueError("NATIVE_CAPITAL_DEPLOYMENT_DEFINITION_CHANGED")
            hashes={spec.artifact_hash for spec in specs}
            primary=entry.get("strategyArtifact") or {}
            portfolio=entry.get("portfolioArtifact") or {}
            if primary.get("artifactHash") not in hashes or (len(specs)>1 and portfolio.get("artifactHash")!={s.portfolio_hash for s in specs}.pop()):
                raise ValueError("NATIVE_CAPITAL_DEPLOYMENT_SCIENTIFIC_BINDING_CHANGED")
            # Re-read every scientific Source and canonical Portfolio from disk;
            # a current registry alone is not proof that loaded files stayed sealed.
            for spec in specs:
                inbox._exact_artifact(inbox._catalog(root, "strategy"), spec.strategy_id, spec.artifact_hash)
            if len(specs) > 1:
                from trading_runtime.portfolio_runtime import load_portfolio_runtime
                loaded = load_portfolio_runtime(root, portfolio["artifactId"])
                expected = {(s.strategy_instance_id, s.artifact_hash, s.portfolio_hash, s.symbol, str(s.target_weight)) for s in specs}
                actual = {(s.strategy_instance_id, s.artifact_hash, s.portfolio_hash, s.symbol, str(s.target_weight)) for s in loaded.specs}
                if expected != actual:
                    raise ValueError("NATIVE_CAPITAL_CANONICAL_PORTFOLIO_MEMBERS_CHANGED")
            matches.append(entry)
    if len(matches)!=1:raise ValueError("NATIVE_CAPITAL_EXACT_CURRENT_DEPLOYMENT_REQUIRED")
    return matches[0]


class NativeSpotRuntime:
    def __init__(self,root,deployment_id,specs,*,reader=None,roots=None,deployment_reader=None,dispatch_reader=None):
        self.store=OwnedCapitalStore(Path(root)/"logs"/"native_owned_capital.sqlite3")
        self.terminal_cache=VerifiedTerminalCache(Path(root)/"logs"/"verified_terminal_order_cache.sqlite3")
        self.deployment_id=deployment_id;self.specs=tuple(specs)
        self.reader=reader or ReadOnlyBrokerReader(runtime_settings())
        self.roots=roots
        self.deployment_reader=deployment_reader or current_deployment
        self._position_context=None
        self.dispatch_reader=dispatch_reader

    def scope(self):
        before=stable_sha256(self.reader.settings)
        deployment=self.deployment_reader(self.deployment_id,self.specs,self.roots if self.roots is not None else inbox.configured_artifact_roots())
        providers={s.broker_id for s in self.specs}
        if len(providers)!=1 or next(iter(providers)) not in {"binance","upbit"}:raise ValueError("NATIVE_CAPITAL_SPOT_ONE_ACCOUNT_REQUIRED")
        accounts=[p for p in saved_account_profiles(self.reader.settings) if p["brokerId"]==next(iter(providers))]
        if len(accounts)!=1:raise ValueError("NATIVE_CAPITAL_SAVED_ACCOUNT_MISSING")
        bindings=[]
        for s in self.specs:
            contract=sizing_from_artifact(s.artifact)["quantityContract"]
            if contract["provider"]!=s.broker_id or contract["symbol"]!=s.symbol:raise ValueError("NATIVE_CAPITAL_SOURCE_ROUTE_CHANGED")
            bindings.append({"strategyInstanceId":s.strategy_instance_id,"sourceArtifactHash":s.artifact_hash,"portfolioHash":s.portfolio_hash,"symbol":s.symbol,"quoteCurrency":contract["quoteCurrency"]})
        scope=reviewed_scope(deployment,accounts[0],bindings)
        if before!=stable_sha256(self.reader.settings):raise ValueError("NATIVE_CAPITAL_CREDENTIAL_CONFIGURATION_CHANGED")
        return scope

    def initialize_at_reviewed_manual_start(self):
        scope=self.scope()
        observation=observe_native_spot(self.reader,sizing_from_artifact(self.specs[0].artifact)["quantityContract"],side="BUY")
        if scope["quoteCurrency"] == "USDT" and self.specs[0].provider == "binance":
            from .account_currency_limits import refresh_conversion_observation
            refresh_conversion_observation(self.reader, native_currency="USDT", broker_id="binance")
        result=self.store.initialize(scope,observation)
        self._position_context=result
        return result

    def _mark(self,spec):
        if spec.provider=="binance":
            response,_=self.reader._get("binance","/api/v3/ticker/price",query={"symbol":spec.symbol})
            if response.get("symbol")!=spec.symbol:raise ValueError("OWNED_MARK_SYMBOL_MISMATCH")
            return str(decimal_value(response.get("price"),"owned mark",positive=True))
        response,_=self.reader._get("upbit","/v1/ticker",query={"markets":spec.symbol})
        if not isinstance(response,list) or len(response)!=1 or response[0].get("market")!=spec.symbol:raise ValueError("OWNED_MARK_SYMBOL_MISMATCH")
        stamp=response[0].get("timestamp")
        if type(stamp)is not int or not 0<=datetime.now(timezone.utc).timestamp()-stamp/1000<=15:raise ValueError("OWNED_MARK_STALE")
        return str(decimal_value(response[0].get("trade_price"),"owned mark",positive=True))

    def sync_owned_fills(self,scope):
        from .live_adapters import sign_binance_query, build_upbit_authorization
        if self.dispatch_reader is None:
            from . import state
            rows=state.PROGRAM_LEDGER.order_dispatch_rows(1000000)
        else:rows=self.dispatch_reader()
        if not isinstance(rows,list) or len(rows)>=1000000:raise ValueError("OWNED_DISPATCH_HISTORY_INCOMPLETE")
        pending=[];verified=[]
        members={s.strategy_instance_id:s for s in self.specs}
        for row in rows:
            metadata=row.get("metadata") or {}
            if metadata.get("deployment_id")!=scope["deploymentId"]:continue
            sid=metadata.get("strategy_instance_id")
            if sid not in members or metadata.get("source_artifact_hash")!=members[sid].artifact_hash or metadata.get("portfolio_hash")!=members[sid].portfolio_hash or metadata.get("native_account_fingerprint")!=scope["accountFingerprint"]:
                raise ValueError("OWNED_DISPATCH_SCOPE_UNPROVEN")
            broker_order=str(row.get("broker_order_id") or "")
            if broker_order in {"","-"}:
                if row.get("state") not in {"risk_blocked","adapter_blocked","rejected"}:raise ValueError("OWNED_DISPATCH_UNRESOLVED")
                continue
            spec=members[sid];contract=sizing_from_artifact(spec.artifact)["quantityContract"]
            identity=terminal_identity(scope,spec,row)
            cached=self.terminal_cache.get(identity)
            if cached is not None:
                pending.extend((fill,row) for fill in cached['receipts'])
                continue
            first=len(pending)
            if spec.provider=="binance":
                clock,_=self.reader._get("binance","/api/v3/time")
                def get(path,query):
                    signed=sign_binance_query({**query,"timestamp":clock["serverTime"],"recvWindow":5000},self.reader.settings["BINANCE_API_SECRET"])
                    return self.reader._get("binance",path,query=signed,headers={"X-MBX-APIKEY":self.reader.settings["BINANCE_API_KEY"]})[0]
                if not broker_order.isascii() or not broker_order.isdigit():raise ValueError("OWNED_BROKER_ORDER_ID_INVALID")
                params={"symbol":spec.symbol,"orderId":int(broker_order)}
                order=get("/api/v3/order",params)
                if order.get("symbol")!=spec.symbol or str(order.get("orderId"))!=broker_order or order.get("side")!=row.get("side") or order.get("status") not in {"FILLED","CANCELED","EXPIRED","REJECTED"}:
                    raise ValueError("OWNED_ORDER_TERMINAL_FILL_TRUTH_PENDING")
                trades=get("/api/v3/myTrades",{**params,"limit":1000})
                if not isinstance(trades,list) or len(trades)>=1000:raise ValueError("OWNED_TRADES_PAGINATION_UNPROVEN")
                total=Decimal(0)
                for t in trades:
                    if t.get("symbol")!=spec.symbol or str(t.get("orderId"))!=broker_order or type(t.get("isBuyer")) is not bool or ("BUY" if t["isBuyer"] else "SELL")!=row.get("side"):
                        raise ValueError("OWNED_TRADE_EXACT_ORDER_MISMATCH")
                    quantity=decimal_value(t.get("qty"),"actual trade quantity",positive=True);price=decimal_value(t.get("price"),"actual trade price",positive=True)
                    total+=quantity
                    asset=t.get("commissionAsset");currency="BASE" if asset==contract["baseCurrency"] else asset
                    pending.append((self._fill(scope,spec,row,str(t["id"]),quantity,price,t.get("commission"),currency,datetime.fromtimestamp(t["time"]/1000,timezone.utc).isoformat()),row))
                if total!=decimal_value(order.get("executedQty"),"actual executed quantity"):raise ValueError("OWNED_TRADES_TOTAL_DIFFERS_FROM_ORDER")
            else:
                query={"uuid":broker_order}
                order,_=self.reader._get("upbit","/v1/order",query=query,headers={"Authorization":build_upbit_authorization(self.reader.settings["UPBIT_ACCESS_KEY"],self.reader.settings["UPBIT_SECRET_KEY"],query)})
                if order.get("uuid")!=broker_order or order.get("market")!=spec.symbol or order.get("side")!=("bid" if row.get("side")=="BUY" else "ask") or order.get("state") not in {"done","cancel"}:
                    raise ValueError("OWNED_ORDER_TERMINAL_FILL_TRUTH_PENDING")
                trades=order.get("trades")
                if not isinstance(trades,list) or type(order.get("trades_count")) is not int or len(trades)!=order["trades_count"]:
                    raise ValueError("OWNED_UPBIT_TRADES_INCOMPLETE")
                volume=sum((decimal_value(t.get("volume"),"trade volume",positive=True) for t in trades),Decimal(0))
                if volume!=decimal_value(order.get("executed_volume"),"executed volume"):raise ValueError("OWNED_UPBIT_TRADES_TOTAL_MISMATCH")
                fee=decimal_value(order.get("paid_fee"),"paid fee")
                if not volume and fee:raise ValueError('OWNED_ZERO_FILL_FEE_UNRECONCILED')
                # Final immutable order-level receipt avoids reassigning a changing
                # cumulative paid_fee to partial trades. Actual VWAP is exact Decimal.
                if volume:
                    from decimal import localcontext
                    with localcontext() as precision:
                        precision.prec=2048
                        funds=sum((decimal_value(t.get("funds"),"trade funds",positive=True) for t in trades),Decimal(0))
                        pending.append((self._fill(scope,spec,row,"terminal-order",volume,funds/volume,fee,scope["quoteCurrency"],max(str(t["created_at"]) for t in trades)),row))
            fills=[fill for fill,_ in pending[first:]]
            proof=terminal_proof(identity,fills,broker_response=order,
                executed_quantity=order.get('executedQty' if spec.provider=='binance' else 'executed_volume'),all_trades=trades)
            verified.append((identity,fills,proof))
        for fill,row in sorted(pending,key=lambda pair:(pair[0]["occurredAt"],pair[0]["eventId"])):
            self.store.append_verified_fill(scope,fill,{"brokerOrderId":str(row["broker_order_id"]),"metadata":row["metadata"],"persistedDispatchVerified":True})
        for identity,fills,proof in verified:self.terminal_cache.put(identity,fills,proof)
        return len(pending)

    @staticmethod
    def _fill(scope,spec,row,event,quantity,price,fee,currency,occurred):
        return {"eventId":spec.provider+":"+scope["accountFingerprint"]+":"+spec.symbol+":"+str(row["broker_order_id"])+":"+event,"brokerOrderId":str(row["broker_order_id"]),"strategyInstanceId":spec.strategy_instance_id,"symbol":spec.symbol,"side":row["side"],"quantity":str(quantity),"price":str(price),"fee":str(fee),"feeCurrency":currency,"occurredAt":occurred,"accountFingerprint":scope["accountFingerprint"]}

    def context(self,spec,side,price,specs,boundary):
        try:
            scope=self.scope()
            self.sync_owned_fills(scope)
            if tuple(s.strategy_instance_id for s in specs)!=tuple(s.strategy_instance_id for s in self.specs):raise ValueError("NATIVE_CAPITAL_MEMBERS_CHANGED")
            projection=self.store.project(scope,{})
            if side=="POSITION":return projection
            started=datetime.now(timezone.utc)
            observation=observe_native_spot(self.reader,sizing_from_artifact(spec.artifact)["quantityContract"],side=side)
            if scope["quoteCurrency"] == "USDT" and spec.provider == "binance":
                from .account_currency_limits import refresh_conversion_observation
                refresh_conversion_observation(self.reader, native_currency="USDT", broker_id="binance")
            marks={s.strategy_instance_id:self._mark(s) for s in self.specs if decimal_value(projection["ownedPositions"][s.strategy_instance_id],"owned position")>0}
            projection=self.store.project(scope,marks)
            if decimal_value(observation["availableCash"],"available cash") < decimal_value(projection["ownedCash"],"owned cash"):
                raise ValueError("OWNED_CAPITAL_EXTERNAL_CASH_CHANGE_RECONCILIATION_REQUIRED")
            if (datetime.now(timezone.utc)-started).total_seconds()>15:raise ValueError("NATIVE_QUANTITY_MULTIPLE_READS_STALE")
            if self.scope()!=scope:raise ValueError("NATIVE_CAPITAL_CURRENT_BINDING_CHANGED_DURING_READ")
            reference=observation.get("marketReference") or {}
            return {**projection,"deploymentId":self.deployment_id,"sourceArtifactHash":spec.artifact_hash,"strategyInstanceId":spec.strategy_instance_id,"portfolioHash":spec.portfolio_hash,"quoteCurrency":scope["quoteCurrency"],"accountFingerprint":scope["accountFingerprint"],"accountKnown":True,"accountFresh":True,"asOf":observation["asOf"],"availableCash":str(min(decimal_value(projection["ownedCash"],"owned cash"),decimal_value(observation["availableCash"],"available cash"))),"actualFeeRate":observation["feeRate"],"currentQuantityContract":observation["currentQuantityContract"],"nativeMarketObservation":observation,"notionalReferencePrice":reference.get("price"),"exactOpenBoundary":boundary,"orderAuthorityGranted":False}
        except (ValueError,TypeError,KeyError,OSError,ArithmeticError,ReadFailure) as error:
            return {"status":"BLOCKED","reasons":[str(error)],"orderAuthorityGranted":False}
