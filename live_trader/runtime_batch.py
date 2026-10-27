"""Exact deployment groups; orchestration never grants execution authority.

Each member still traverses the ordinary start admission (including KIS native
confirmation). Claims precede that call, so retries/status reads cannot launch a
second worker. Independent account/currency ledgers are never merged.
"""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import threading
from typing import Any, Callable, Mapping
from uuid import uuid4


_SELECTION_KEYS = {"profile_id", "mode", "portfolio_id", "deployment_id", "strategy_id"}
_START_KEYS = {"batchId", "scopeHash", "memberId", "confirmed",
               "safety_confirmation", "marketGapPreparationId"}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class RuntimeBatchError(ValueError):
    pass


class RuntimeBatchService:
    """Process-owned plan and durable once-only member launch claims."""
    def __init__(self, root: Path, *, resolve: Callable, start: Callable,
                 observe: Callable, clock=None, ttl_seconds=180):
        self.root = Path(root)
        self.resolve = resolve
        self.start_runtime = start
        self.observe = observe
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.ttl_seconds = max(1, min(300, int(ttl_seconds)))
        self._lock = threading.RLock()
        self._groups = {}

    def _write(self, name, document):
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / (name + ".json")).open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(document, stream, ensure_ascii=False, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())

    @staticmethod
    def _selection(value):
        if (not isinstance(value, dict) or set(value) - _SELECTION_KEYS
                or value.get("profile_id") not in {"stock", "crypto"}
                or value.get("mode") not in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"}
                or not isinstance(value.get("deployment_id"), str) or not value["deployment_id"].strip()):
            raise RuntimeBatchError("EXACT_DEPLOYMENT_SELECTION_REQUIRED")
        result = {}
        for key in _SELECTION_KEYS:
            item = value.get(key, "")
            if not isinstance(item, str) or len(item) > 512 or any(ord(c) < 32 for c in item):
                raise RuntimeBatchError("EXACT_DEPLOYMENT_SELECTION_REQUIRED")
            result[key] = item.strip()
        return result

    def prepare(self, payload):
        if not isinstance(payload, dict) or set(payload) != {"selections"}:
            raise RuntimeBatchError("EXACT_BATCH_SELECTIONS_REQUIRED")
        rows = payload["selections"]
        if not isinstance(rows, list) or not 1 <= len(rows) <= 20:
            raise RuntimeBatchError("BATCH_SELECTION_COUNT_INVALID")
        selections = [self._selection(row) for row in rows]
        if len({row["deployment_id"] for row in selections}) != len(selections):
            raise RuntimeBatchError("DUPLICATE_DEPLOYMENT_SELECTION")
        with self._lock:
            now = self.clock()
            members = []
            for index, selection in enumerate(selections):
                member = {"memberId": "member-" + str(index + 1), "selection": selection,
                          "status": "PREPARED", "reasonCode": "", "attempted": False,
                          "binding": None, "result": None}
                try:
                    binding = self.resolve(deepcopy(selection))
                    scope = binding.get("scope") if isinstance(binding, Mapping) else None
                    if (not isinstance(scope, Mapping) or not scope.get("runtimeScopeId")
                            or scope.get("deploymentId") != selection["deployment_id"]
                            or scope.get("profileId") != selection["profile_id"]):
                        raise RuntimeBatchError("EXACT_RUNTIME_BINDING_REQUIRED")
                    member["binding"] = deepcopy(dict(binding))
                except Exception as exc:
                    # Provider/error text is never echoed into the public batch.
                    member.update(status="BLOCKED", reasonCode="BATCH_MEMBER_SELECTION_BLOCKED",
                                  errorType=type(exc).__name__)
                members.append(member)
            now = self.clock()
            batch_id = "batch-" + str(uuid4())
            group = {"schemaVersion": "live-runtime-batch-v1", "batchId": batch_id,
                     "preparedAt": _stamp(now), "expiresAt": _stamp(now + timedelta(seconds=self.ttl_seconds)),
                     "members": members}
            group["scopeHash"] = _digest(group)
            self._write("plan-" + batch_id, group)
            self._groups[batch_id] = group
            return {"ok": any(row["status"] == "PREPARED" for row in members),
                    "batch": self._public(group), "authorityGranted": False,
                    "accountPreflightPerformed": False, "brokerSubmissionPerformed": False}

    def _group(self, batch_id):
        if batch_id in self._groups:
            return self._groups[batch_id]
        # A restarted process may display an old plan, never resume its starts.
        if (not isinstance(batch_id, str) or len(batch_id) != 42
                or not batch_id.startswith("batch-")
                or any(c not in "0123456789abcdef-" for c in batch_id[6:])):
            raise RuntimeBatchError("EXACT_BATCH_ID_REQUIRED")
        path = self.root / ("plan-" + batch_id + ".json")
        if not path.is_file():
            raise RuntimeBatchError("BATCH_NOT_FOUND")
        group = json.loads(path.read_text(encoding="utf-8"))
        if group.get("batchId") != batch_id:
            raise RuntimeBatchError("BATCH_RECORD_INVALID")
        group["restoredReadOnly"] = True
        for member in group["members"]:
            member.update(status="UNKNOWN", attempted=True, reasonCode="BATCH_OWNER_RESTARTED_NO_RESUME")
        self._groups[batch_id] = group
        return group

    def status(self, batch_id):
        with self._lock:
            return {"ok": True, "batch": self._public(self._group(batch_id))}

    def start_member(self, payload):
        if (not isinstance(payload, dict) or set(payload) - _START_KEYS
                or not {"batchId", "scopeHash", "memberId", "confirmed"} <= set(payload)
                or payload.get("confirmed") is not True):
            raise RuntimeBatchError("EXPLICIT_BATCH_MEMBER_START_REQUIRED")
        with self._lock:
            group = self._group(payload["batchId"])
            if payload["scopeHash"] != group["scopeHash"]:
                raise RuntimeBatchError("EXACT_BATCH_SCOPE_HASH_REQUIRED")
            members = [row for row in group["members"] if row["memberId"] == payload["memberId"]]
            if len(members) != 1:
                raise RuntimeBatchError("EXACT_BATCH_MEMBER_REQUIRED")
            member = members[0]
            if member["attempted"] or member["status"] != "PREPARED":
                return {"ok": False, "reason": "BATCH_MEMBER_ALREADY_ATTEMPTED_OR_BLOCKED",
                        "batch": self._public(group), "replayed": True,
                        "memberResult": deepcopy(member.get("result"))}
            if self.clock() >= datetime.fromisoformat(group["expiresAt"].replace("Z", "+00:00")):
                raise RuntimeBatchError("BATCH_PREPARATION_EXPIRED")
            try:
                current = self.resolve(deepcopy(member["selection"]))
                if current != member["binding"]:
                    raise RuntimeBatchError("BATCH_MEMBER_BINDING_CHANGED")
            except Exception:
                member.update(status="BLOCKED", reasonCode="BATCH_MEMBER_BINDING_CHANGED")
                return {"ok": False, "reason": member["reasonCode"], "batch": self._public(group),
                        "runtimeStarted": False, "brokerSubmissionPerformed": False}
            if self.clock() >= datetime.fromisoformat(group["expiresAt"].replace("Z", "+00:00")):
                raise RuntimeBatchError("BATCH_PREPARATION_EXPIRED")
            # Claim before calling existing authority code, outside this lock.
            # A concurrent call observes STARTING and cannot call it again.
            self._write("claim-" + group["batchId"] + "-" + member["memberId"],
                        {"batchId": group["batchId"], "scopeHash": group["scopeHash"],
                         "memberId": member["memberId"], "claimedAt": _stamp(self.clock()),
                         "selection": member["selection"], "bindingHash": _digest(current)})
            member.update(attempted=True, status="STARTING", reasonCode="")
            selection = deepcopy(member["selection"])
            options = {key: deepcopy(payload[key]) for key in ("safety_confirmation", "marketGapPreparationId")
                       if key in payload}
        try:
            result = self.start_runtime(selection, options)
            if not isinstance(result, Mapping):
                raise RuntimeBatchError("START_RESULT_UNAVAILABLE")
            # Never persist an attestation, private account snapshot or arbitrary response.
            public_result = {key: deepcopy(result[key]) for key in (
                "ok", "reason", "runtimeStarted", "brokerSubmissionPerformed",
                "runtimeId", "runtime_id", "runtime_session_id") if key in result}
            status = "STARTED" if result.get("ok") is True else "BLOCKED"
            reason = "" if status == "STARTED" else "EXISTING_RUNTIME_ADMISSION_BLOCKED"
        except Exception as exc:
            public_result = {"ok": False, "reason": "BATCH_START_OUTCOME_UNKNOWN", "errorType": type(exc).__name__}
            status, reason = "UNKNOWN", "BATCH_START_OUTCOME_UNKNOWN"
        with self._lock:
            member.update(status=status, reasonCode=reason, result=public_result)
            try:
                self._write("result-" + group["batchId"] + "-" + member["memberId"],
                            {"batchId": group["batchId"], "memberId": member["memberId"],
                             "observedAt": _stamp(self.clock()), "status": status,
                             "result": public_result})
            except Exception:
                member.update(status="UNKNOWN", reasonCode="BATCH_RESULT_PERSISTENCE_UNKNOWN")
            public_batch = self._public(group)
            public_member = next(row for row in public_batch["members"] if row["memberId"] == member["memberId"])
            verified_running = member["status"] == "STARTED" and public_member["status"] == "RUNNING"
            return {"ok": verified_running,
                    "reason": "" if verified_running else member["reasonCode"] or "BATCH_START_NOT_OBSERVED_RUNNING",
                    "memberResult": deepcopy(public_result), "batch": public_batch}

    def _public(self, group):
        try:
            observed = self.observe()
            runtimes = observed.get("runtimes", {}) if isinstance(observed, Mapping) else {}
        except Exception:
            runtimes = {}
        rows = []
        for member in group["members"]:
            scope = (member.get("binding") or {}).get("scope", {})
            runtime_id = scope.get("runtimeScopeId", "")
            actual = runtimes.get(runtime_id)
            status = member["status"]
            observation = None
            if isinstance(actual, Mapping) and actual.get("runtimeScopeId") == runtime_id:
                observation = {key: deepcopy(actual.get(key)) for key in (
                    "running", "phase", "mode", "lastError", "lastCycleAt")}
                # Current running fact is never inferred from a successful HTTP reply.
                if status == "STARTED" and actual.get("running") is True:
                    status = "RUNNING"
                elif status == "STARTED" and actual.get("running") is False:
                    status = "STOPPED"
            elif status == "STARTED":
                status = "UNKNOWN"
            rows.append({"memberId": member["memberId"], "selection": deepcopy(member["selection"]),
                         "runtimeId": runtime_id, "brokerId": scope.get("brokerId", ""),
                         "capitalCurrency": scope.get("capitalCurrency", ""),
                         "status": status, "reasonCode": member["reasonCode"],
                         "attempted": member["attempted"], "runtimeObservation": observation})
        states = [row["status"] for row in rows]
        if states and all(state == "RUNNING" for state in states):
            status = "RUNNING"
        elif "RUNNING" in states:
            status = "PARTIAL"
        elif "STARTING" in states:
            status = "STARTING"
        elif "UNKNOWN" in states:
            status = "UNKNOWN"
        elif all(state == "STOPPED" for state in states):
            status = "STOPPED"
        elif any(state == "BLOCKED" for state in states):
            status = "PARTIAL" if "PREPARED" in states else "BLOCKED"
        else:
            status = "PREPARED"
        return {key: deepcopy(group[key]) for key in (
            "schemaVersion", "batchId", "scopeHash", "preparedAt", "expiresAt")} | {
            "status": status, "members": rows,
            "selectionOnly": True, "admissionCheckedAtEachStart": True,
            "automaticRestartAllowed": False, "automaticRetryAllowed": False,
            "fundsMerged": False, "partialFailurePolicy": "PRESERVE_EXISTING_RUNTIME_SAFETY_CONTROLS",
            "restoredReadOnly": group.get("restoredReadOnly") is True}
