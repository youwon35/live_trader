"""One-attempt native intents wait for an exact observed NEXT_BAR_OPEN.

This is not a restart replay queue. A different process owner cannot release a
previous process's pending signal. Closed close and ticker prices are never opens.
"""
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import json, sqlite3, uuid
from contextlib import contextmanager
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.market_calendar import next_tradable_boundary_start

def _time(value):
    value=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    if value.tzinfo is None:raise ValueError("BOUNDARY_TIME_TIMEZONE_REQUIRED")
    return value

def decision_payload(decision,spec,deployment_id):
    declared = []
    for container in (spec.parameters, spec.artifact.get("executionPolicy") or {}, spec.artifact.get("portfolioExecutionPolicy") or {}):
        if container.get("executionTiming") is not None:
            declared.append(container["executionTiming"])
    if not declared or any(value not in {"next-open", "NEXT_BAR_OPEN"} for value in declared):
        raise ValueError("NATIVE_SOURCE_NEXT_OPEN_CONTRACT_NOT_DECLARED")
    bar=decision.bar.to_dict() if hasattr(decision.bar,"to_dict") else {key:getattr(decision.bar,key) for key in ("end_time","close")}
    return {"deploymentId":deployment_id,"sourceArtifactHash":spec.artifact_hash,"portfolioHash":spec.portfolio_hash,"strategyInstanceId":spec.strategy_instance_id,"strategyId":spec.strategy_id,"evaluationKey":decision.evaluation_key,"signal":decision.signal,"reason":decision.reason,"provider":spec.provider,"instrumentId":spec.instrument_id,"symbol":spec.symbol,"timeframe":spec.timeframe,"closedBar":bar,"closedBarEnd":decision.bar.end_time,"closedSignalPrice":str(decision.bar.close),"expectedStart":next_tradable_boundary_start(provider=spec.provider,instrument_id=spec.instrument_id,symbol=spec.symbol,closed_bar_end=decision.bar.end_time).isoformat()}

class NativeBoundaryQueue:
    def __init__(self,path):
        self.path=Path(path);self.owner=uuid.uuid4().hex

    @contextmanager
    def _db(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        db=sqlite3.connect(self.path)
        try:
            with db:
                db.execute("CREATE TABLE IF NOT EXISTS pending (evaluation_key TEXT PRIMARY KEY, owner TEXT NOT NULL, payload TEXT NOT NULL, content_hash TEXT NOT NULL, state TEXT NOT NULL, result TEXT NOT NULL)")
                yield db
        finally:
            db.close()

    def stage(self,decision,spec,deployment_id):
        if decision.signal not in {"BUY","SELL"} or not deployment_id:raise ValueError("BOUNDARY_SIGNAL_SCOPE_MISSING")
        payload=decision_payload(decision,spec,deployment_id);digest=stable_sha256(payload)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            old=db.execute("SELECT owner,content_hash,state FROM pending WHERE evaluation_key=?",(payload["evaluationKey"],)).fetchone()
            if old:
                if old[0]!=self.owner:raise ValueError("BOUNDARY_PREVIOUS_OWNER_RECONCILIATION_REQUIRED")
                if old[1]!=digest:raise ValueError("BOUNDARY_SIGNAL_IDEMPOTENCY_CONFLICT")
                return {"action":old[2],"ok":True,"evaluationKey":payload["evaluationKey"]}
            db.execute("INSERT INTO pending VALUES (?,?,?,?,?,?)",(payload["evaluationKey"],self.owner,json.dumps(payload,sort_keys=True),digest,"WAIT_OPEN_BOUNDARY","{}"))
        return {"action":"WAIT_OPEN_BOUNDARY","ok":True,"evaluationKey":payload["evaluationKey"],"quantityCalculated":False,"orderSubmitted":False}

    def release(self,boundary,specs,deployment_id,prepare,*,now=None):
        now=now or datetime.now(timezone.utc)
        stream=(boundary.provider,boundary.instrument_id,boundary.symbol,boundary.timeframe)
        specs={s.strategy_instance_id:s for s in specs}
        results=[]
        with self._db() as db:
            rows=db.execute("SELECT evaluation_key,owner,payload,content_hash FROM pending WHERE state='WAIT_OPEN_BOUNDARY' ORDER BY evaluation_key").fetchall()
        for key,owner,raw,digest in rows:
            p=json.loads(raw)
            if (p["provider"],p["instrumentId"],p["symbol"],p["timeframe"])!=stream:continue
            if owner!=self.owner:raise ValueError("BOUNDARY_PREVIOUS_OWNER_RECONCILIATION_REQUIRED")
            if stable_sha256(p)!=digest:raise ValueError("BOUNDARY_SIGNAL_SEAL_INVALID")
            if _time(p["expectedStart"])>_time(boundary.start_time):continue
            spec=specs.get(p["strategyInstanceId"])
            reason=None
            if spec is None or p["deploymentId"]!=deployment_id or p["sourceArtifactHash"]!=spec.artifact_hash or p["portfolioHash"]!=spec.portfolio_hash:reason="BOUNDARY_CURRENT_SOURCE_SCOPE_CHANGED"
            elif _time(p["expectedStart"])!=_time(boundary.start_time):reason="EXACT_NEXT_OPEN_BOUNDARY_MISSED"
            elif boundary.instrument_id!=spec.instrument_id or boundary.symbol!=spec.symbol or boundary.provider!=spec.provider or boundary.timeframe!=spec.timeframe:reason="BOUNDARY_CURRENT_INSTRUMENT_CHANGED"
            elif boundary.recovery_only or not boundary.promotion_eligible or boundary.attestation!={"binance":"BINANCE_WEBSOCKET","upbit":"UPBIT_WEBSOCKET","kis":"KIS_WEBSOCKET"}.get(spec.provider) or boundary.source_provider!=spec.provider:reason="BOUNDARY_REALTIME_SOURCE_NOT_VERIFIED"
            elif not 0<=(_time(boundary.observed_time)-_time(boundary.start_time)).total_seconds()<=30 or not 0<=(now-_time(boundary.start_time)).total_seconds()<=30:reason="EXACT_NEXT_OPEN_BOUNDARY_EXPIRED"
            # Commit a one-shot consumption before the callback. Exceptions or a
            # later member failure never restore a previous WAIT row. The outer
            # dispatcher aborts every deferred POST when any exception escapes.
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                consumed=db.execute("UPDATE pending SET state='BOUNDARY_CLAIMED',result=? WHERE evaluation_key=? AND owner=? AND state='WAIT_OPEN_BOUNDARY'",(json.dumps({"boundaryHash":boundary.market_data_hash}),key,self.owner))
                if consumed.rowcount!=1:continue
            if reason:
                result={"ok":False,"action":"BLOCKED","reason":reason,"orderSubmitted":False}
            else:
                result=prepare(p,spec,boundary)
                if not isinstance(result,dict):raise ValueError("BOUNDARY_PREPARATION_RESULT_INVALID")
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("UPDATE pending SET state=?,result=? WHERE evaluation_key=? AND owner=? AND state='BOUNDARY_CLAIMED'",("BLOCKED" if result.get("ok") is not True else "PREPARED_ONCE",json.dumps(result,sort_keys=True),key,self.owner))
            results.append({"evaluationKey":key,**result})
        return {"ok":True,"action":"BOUNDARY_PROCESSED","results":results,"orderAuthorityGranted":False}

    def snapshot(self):
        if not self.path.exists():return {"pending":0,"restartReplayEnabled":False}
        db=sqlite3.connect(self.path.resolve().as_uri()+"?mode=ro",uri=True)
        try:
            counts={r[0]:r[1] for r in db.execute("SELECT state,COUNT(*) FROM pending GROUP BY state")}
        finally:db.close()
        return {"pending":counts.get("WAIT_OPEN_BOUNDARY",0),"states":counts,"restartReplayEnabled":False}
