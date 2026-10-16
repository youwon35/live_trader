"""Two-phase fresh KIS preflight production; no account or order transport.

Callbacks capture/commit under short state locks. Actual read-only collection
runs only outside authority locks. A new immutable snapshot must be rebound to
the same consent/session before any final one-use wire grant can be considered.
"""
from __future__ import annotations
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import re
import threading
import time
from .kis_scoped_operational_authority import exact_context

MAX_COLLECTION_SECONDS = 10.0
MAX_LOCK_WAIT_SECONDS = 3.0
MAX_PREFLIGHT_SECONDS = 300.0
_EXPECTED_FIELDS = frozenset(("schemaVersion","sessionId","runtimeScopeId","context",
    "manifestHash","configRevision","configHash","preflightSnapshotId","preflightValid"))
_REGISTRY_LOCK = threading.Lock()
_SCOPE_LOCKS = {}
_SHA = re.compile(r"[a-f0-9]{64}")

def _hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,
        separators=(",",":"),allow_nan=False).encode("utf-8")).hexdigest()

def _time(value):
    result=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    if result.tzinfo is None:raise ValueError("Timezone-aware fresh preflight time required")
    return result.astimezone(timezone.utc)

def _expected(value):
    if type(value) is not dict or set(value)!=_EXPECTED_FIELDS:
        raise ValueError("Exact KIS preflight refresh capture required")
    if value["schemaVersion"]!="kis-scoped-preflight-expected/v1":
        raise ValueError("KIS preflight capture schema differs")
    context=exact_context(value["context"])
    if (type(value["configRevision"]) is not int or value["configRevision"]<1
            or type(value["preflightValid"]) is not bool
            or value["runtimeScopeId"]!=context["runtimeScopeId"]):
        raise ValueError("KIS preflight capture state differs")
    for key in ("manifestHash","configHash"):
        if type(value[key]) is not str or not _SHA.fullmatch(value[key]):
            raise ValueError("KIS preflight capture seal is invalid")
    for key in ("sessionId","runtimeScopeId","preflightSnapshotId"):
        if (type(value[key]) is not str or not 0<len(value[key])<=240
                or any(ord(char)<32 for char in value[key])):
            raise ValueError("KIS preflight exact runtime identity missing")
    return json.loads(json.dumps(value,allow_nan=False))

def _static(value):
    return {key:body for key,body in value.items()
            if key not in {"preflightSnapshotId","preflightValid"}}

@contextmanager
def _scope_lock(scope):
    with _REGISTRY_LOCK:
        if scope not in _SCOPE_LOCKS:
            if len(_SCOPE_LOCKS)>=16:raise ValueError("KIS preflight renewal capacity reached")
            _SCOPE_LOCKS[scope]={"lock":threading.Lock(),"users":0}
        record=_SCOPE_LOCKS[scope]
        record["users"]+=1
    acquired=False
    try:
        acquired=record["lock"].acquire(timeout=MAX_LOCK_WAIT_SECONDS)
        if not acquired:raise ValueError("KIS preflight renewal busy")
        yield
    finally:
        if acquired:record["lock"].release()
        with _REGISTRY_LOCK:
            record["users"]-=1
            if record["users"]==0:del _SCOPE_LOCKS[scope]

def refresh_kis_operational_preflight(intent, *, capture_expected, collect_fresh_truth,
        commit_rebind, revoke_on_failure, assert_outside_authority_locks,
        clock=lambda:datetime.now(timezone.utc), monotonic_clock=time.monotonic):
    expected=None
    try:
        if assert_outside_authority_locks() is not True:
            raise ValueError("KIS preflight collection is inside an authority lock")
        expected=_expected(capture_expected(intent))
        if expected["preflightValid"]:
            return {"ok":True,"action":"CURRENT","sessionId":expected["sessionId"],
                    "preflightSnapshotId":expected["preflightSnapshotId"]}
        with _scope_lock(expected["runtimeScopeId"]):
            current=_expected(capture_expected(intent))
            if _static(current)!=_static(expected):
                raise ValueError("KIS consent or runtime changed before refresh")
            expected=current
            if expected["preflightValid"]:
                return {"ok":True,"action":"CURRENT","sessionId":expected["sessionId"],
                        "preflightSnapshotId":expected["preflightSnapshotId"]}
            if assert_outside_authority_locks() is not True:
                raise ValueError("KIS preflight collection is inside an authority lock")
            began=monotonic_clock()
            started=clock()
            truth=collect_fresh_truth(json.loads(json.dumps(expected,allow_nan=False)))
            received=clock()
            elapsed=monotonic_clock()-began
            if not 0<=elapsed<=MAX_COLLECTION_SECONDS:
                raise ValueError("KIS preflight read-only collection deadline exceeded")
            if (type(truth) is not dict
                    or truth.get("schemaVersion")!="kis-scoped-preflight-truth/v1"
                    or truth.get("expectedHash")!=_hash(expected)
                    or type(truth.get("reconciliation")) is not dict
                    or type(truth.get("brokers")) is not list
                    or not _time(started)<=_time(truth.get("collectedAt"))<=_time(received)):
                raise ValueError("KIS preflight collected truth has changed or is stale")
            current=_expected(capture_expected(intent))
            if _static(current)!=_static(expected):
                raise ValueError("KIS consent or runtime changed during read-only collection")
            result=commit_rebind(json.loads(json.dumps(expected,allow_nan=False)),truth)
            if (type(result) is not dict or result.get("ok") is not True
                    or result.get("action")!="REFRESHED"
                    or result.get("sessionId")!=expected["sessionId"]
                    or not isinstance(result.get("preflightSnapshotId"),str)
                    or not result["preflightSnapshotId"]
                    or result["preflightSnapshotId"]==expected["preflightSnapshotId"]
                    or not _SHA.fullmatch(str(result.get("preflightHash") or ""))
                    or not 0<(_time(result.get("expiresAt"))-_time(clock())).total_seconds()<=MAX_PREFLIGHT_SECONDS):
                raise ValueError("KIS preflight replacement snapshot is not a fresh exact rebind")
            rebound=_expected(capture_expected(intent))
            if (_static(rebound)!=_static(expected) or not rebound["preflightValid"]
                    or rebound["preflightSnapshotId"]!=result["preflightSnapshotId"]):
                raise ValueError("KIS preflight rebind acknowledgement does not match current state")
            return result
    except Exception as error:
        revoked=False
        if expected is not None:
            try:
                revoke_on_failure(expected,type(error).__name__)
                revoked=True
            except Exception:
                pass
        return {"ok":False,"action":"BLOCKED","code":"KIS_PREFLIGHT_RENEWAL_BLOCKED",
                "reason":str(error)[:200],"revocationAcknowledged":revoked}

