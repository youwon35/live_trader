from __future__ import annotations

"""Disabled HTTP-shaped adapter for the isolated KIS functional state."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
import sqlite3
import time
from typing import Any, Callable, Mapping

from .functional_http_session import (
    FunctionalHttpSessionAuthority,
    FunctionalHttpSessionError,
)
from .kis_domestic_functional_state import (
    DurableKisDomesticFunctionalState,
    KisDomesticFunctionalStateBlocked,
)
from .kis_order_authority import kis_route_authority_serialization
from .safety_confirmation import SafetyConfirmationStore


KIS_DOMESTIC_FUNCTIONAL_SERVER_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_SERVER_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_SERVER_BOOTSTRAP_AVAILABLE = False

ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
_SHA = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_STATUS = "/api/kis-domestic-functional/status"
_CHALLENGE = "/api/kis-domestic-functional/safety-confirmation/challenge"
_START = "/api/kis-domestic-functional/start"
_STOP = "/api/kis-domestic-functional/stop"
_RECOVER = "/api/kis-domestic-functional/recover"
_BOOTSTRAP = "/__lt_native_bootstrap"
_ACTIONS = {"KIS_START", "KIS_STOP", "KIS_RECOVER"}
_BASE_CONTEXT_KEYS = {
    "schemaVersion", "route", "pdno", "action", "publicArmId",
    "publicArmHash", "accountFingerprint", "artifactCanonicalHash",
    "artifactFileSha256", "instanceCanonicalHash", "instanceFileSha256",
    "codeManifestHash", "capsHash", "oneShotId", "oneShotHash",
    "credentialConfigurationHash", "productionAvailable",
}
_CONTEXT_KEYS = _BASE_CONTEXT_KEYS | {
    "stateRevision", "stateSessionId", "stateOwnerEpochHash",
    "managerBindingsHash",
}

_DDL_CURRENT = """
CREATE TABLE IF NOT EXISTS kis_functional_server_safety (
    one_shot_id TEXT PRIMARY KEY,
    context_base_hash TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('ISSUED','CONSUMED')),
    consumed_action TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL CHECK(revision >= 1),
    transition_head_hash TEXT NOT NULL,
    signer_key_id_hash TEXT NOT NULL
)
""".strip()
_DDL_TRANSITION = """
CREATE TABLE IF NOT EXISTS kis_functional_server_safety_transition (
    one_shot_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision >= 1),
    state TEXT NOT NULL CHECK(state IN ('ISSUED','CONSUMED')),
    action TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    state_revision INTEGER NOT NULL CHECK(state_revision >= 0),
    session_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    body_json TEXT NOT NULL,
    body_hash TEXT NOT NULL UNIQUE,
    signature TEXT NOT NULL,
    signer_key_id_hash TEXT NOT NULL,
    PRIMARY KEY (one_shot_id, revision),
    FOREIGN KEY (one_shot_id) REFERENCES kis_functional_server_safety(one_shot_id)
)
""".strip()


class KisDomesticFunctionalServerBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class KisFunctionalHttpResult:
    status: int
    body: Mapping[str, Any]
    headers: Mapping[str, str]


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _utc_now(clock: Callable[[], float]) -> str:
    value = clock()
    if type(value) not in {int, float}:
        raise KisDomesticFunctionalServerBlocked("server clock is invalid")
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()


def _exact_context(value: Mapping[str, Any], *, action: str | None = None) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _CONTEXT_KEYS:
        raise KisDomesticFunctionalServerBlocked("safety confirmation context is not exact")
    result = dict(value)
    exact = {
        "schemaVersion": "kis-domestic-functional-safety-context/v1",
        "route": ROUTE, "pdno": PDNO, "productionAvailable": False,
    }
    for key, expected in exact.items():
        if type(result.get(key)) is not type(expected) or result.get(key) != expected:
            raise KisDomesticFunctionalServerBlocked(f"safety context {key} mismatch")
    if result.get("action") not in _ACTIONS or action is not None and result["action"] != action:
        raise KisDomesticFunctionalServerBlocked("safety context action mismatch")
    for key in (
        "publicArmHash", "accountFingerprint", "artifactCanonicalHash",
        "artifactFileSha256", "instanceCanonicalHash", "instanceFileSha256",
        "codeManifestHash", "capsHash", "oneShotHash",
        "credentialConfigurationHash", "stateOwnerEpochHash",
        "managerBindingsHash",
    ):
        if type(result.get(key)) is not str or not _SHA.fullmatch(result[key]):
            raise KisDomesticFunctionalServerBlocked(f"safety context {key} is invalid")
    for key in ("publicArmId", "oneShotId"):
        if type(result.get(key)) is not str or not _ID.fullmatch(result[key]):
            raise KisDomesticFunctionalServerBlocked(f"safety context {key} is invalid")
    if type(result.get("stateRevision")) is not int or result["stateRevision"] < 0:
        raise KisDomesticFunctionalServerBlocked("safety context state revision is invalid")
    if type(result.get("stateSessionId")) is not str or (
        result["stateSessionId"] and not _ID.fullmatch(result["stateSessionId"])
    ):
        raise KisDomesticFunctionalServerBlocked("safety context state session is invalid")
    return result


class DisabledKisFunctionalOfflineManager:
    """Exact, diagnostic-only manager wrapper; it cannot establish live authority."""

    def __init__(self, *, name: str, owner_id: str, code_hash: str,
                 status_reader: Callable[[], Mapping[str, Any]],
                 callback: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> None:
        if name not in {"start", "stop", "recover"}:
            raise KisDomesticFunctionalServerBlocked("offline manager name is invalid")
        if type(owner_id) is not str or not _ID.fullmatch(owner_id):
            raise KisDomesticFunctionalServerBlocked("offline manager owner is invalid")
        if type(code_hash) is not str or not _SHA.fullmatch(code_hash):
            raise KisDomesticFunctionalServerBlocked("offline manager code hash is invalid")
        if not callable(status_reader) or not callable(callback):
            raise KisDomesticFunctionalServerBlocked("offline manager reader/callback is invalid")
        self.name = name
        self.owner_hash = hashlib.sha256(owner_id.encode()).hexdigest()
        self.code_hash = code_hash
        self._reader = status_reader
        self._callback = callback

    def binding(self) -> dict[str, Any]:
        return {"name": self.name, "ownerHash": self.owner_hash,
                "codeHash": self.code_hash, "productionAvailable": False}

    def _status(self) -> dict[str, Any]:
        raw = self._reader()
        expected = {
            "schemaVersion": "kis-domestic-functional-offline-manager-status/v1",
            "manager": self.name, "ownerHash": self.owner_hash,
            "codeHash": self.code_hash, "readable": True,
            "networkDispatchCount": 0, "tradingMutationCount": 0,
            "productionAvailable": False,
        }
        if not isinstance(raw, Mapping) or set(raw) != set(expected):
            raise KisDomesticFunctionalServerBlocked("offline manager status is not exact")
        value = dict(raw)
        for key, wanted in expected.items():
            if type(value.get(key)) is not type(wanted) or value[key] != wanted:
                raise KisDomesticFunctionalServerBlocked(f"offline manager {key} mismatch")
        return value

    def invoke(self, reservation: Mapping[str, Any]) -> Mapping[str, Any]:
        before = self._status()
        result = self._callback(dict(reservation))
        after = self._status()
        if before != after:
            raise KisDomesticFunctionalServerBlocked("offline manager status changed")
        if not isinstance(result, Mapping) or set(result) != {
            "ok", "mutationMayHaveOccurred", "receiptHash"
        }:
            raise KisDomesticFunctionalServerBlocked("offline manager result is not exact")
        result = dict(result)
        if type(result["ok"]) is not bool or type(result["mutationMayHaveOccurred"]) is not bool:
            raise KisDomesticFunctionalServerBlocked("offline manager result flags are invalid")
        if type(result["receiptHash"]) is not str or not _SHA.fullmatch(result["receiptHash"]):
            raise KisDomesticFunctionalServerBlocked("offline manager receipt is invalid")
        return result


class DisabledKisDomesticFunctionalServer:
    def __init__(
        self,
        *,
        state: DurableKisDomesticFunctionalState,
        http_authority: FunctionalHttpSessionAuthority,
        safety_confirmations: SafetyConfirmationStore,
        approved_context_base: Mapping[str, Any],
        offline_managers: Mapping[str, DisabledKisFunctionalOfflineManager],
        allow_offline_managers: bool,
        server_signer_key: bytes,
        server_signer_key_id: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if type(state) is not DurableKisDomesticFunctionalState:
            raise KisDomesticFunctionalServerBlocked("exact isolated KIS state is required")
        if type(http_authority) is not FunctionalHttpSessionAuthority:
            raise KisDomesticFunctionalServerBlocked("exact functional HTTP authority is required")
        if type(safety_confirmations) is not SafetyConfirmationStore:
            raise KisDomesticFunctionalServerBlocked("exact safety confirmation store is required")
        if type(allow_offline_managers) is not bool or allow_offline_managers is not True:
            raise KisDomesticFunctionalServerBlocked("only explicit offline managers are allowed")
        if not isinstance(offline_managers, Mapping) or set(offline_managers) != {"start", "stop", "recover"} or any(
            type(offline_managers[key]) is not DisabledKisFunctionalOfflineManager
            or offline_managers[key].name != key for key in offline_managers
        ):
            raise KisDomesticFunctionalServerBlocked("offline manager set is not exact")
        if type(server_signer_key) is not bytes or len(server_signer_key) < 32:
            raise KisDomesticFunctionalServerBlocked("server signer key is invalid")
        if type(server_signer_key_id) is not str or not _ID.fullmatch(server_signer_key_id):
            raise KisDomesticFunctionalServerBlocked("server signer key id is invalid")
        if not callable(clock):
            raise KisDomesticFunctionalServerBlocked("server clock is invalid")
        base = dict(approved_context_base)
        if set(base) != _BASE_CONTEXT_KEYS - {"action"}:
            raise KisDomesticFunctionalServerBlocked("approved safety context base is not exact")
        snapshot = state.authority_snapshot()
        if base["accountFingerprint"] != snapshot["functionalAccountFingerprint"]:
            raise KisDomesticFunctionalServerBlocked("safety context account/state mismatch")
        if base["credentialConfigurationHash"] != snapshot["credentialConfigurationHash"]:
            raise KisDomesticFunctionalServerBlocked("safety context credential/state mismatch")
        self.state = state
        self.authority = http_authority
        self.safety = safety_confirmations
        self.context_base = base
        self.managers = dict(offline_managers)
        self._signer_key = server_signer_key
        self._signer_key_id_hash = hashlib.sha256(server_signer_key_id.encode()).hexdigest()
        self._clock = clock
        self._manager_bindings_hash = _hash({
            key: self.managers[key].binding() for key in sorted(self.managers)
        })
        self._ensure_one_shot()

    @staticmethod
    def _owner_epoch(snapshot: Mapping[str, Any]) -> str:
        return _hash({
            "ownerHash": snapshot.get("ownerHash"),
            "componentOwnerHashes": snapshot.get("componentOwnerHashes"),
        })

    def _signature(self, body_hash: str) -> str:
        return hmac.new(
            self._signer_key,
            ("KIS_SERVER_ONE_SHOT\n" + body_hash).encode(), hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.split()).lower()

    def _ensure_one_shot(self) -> None:
        base_hash = _hash(self.context_base)
        with self.state.ledger.connection() as conn:
            expected_db = sqlite3.connect(":memory:")
            try:
                expected_db.execute("PRAGMA foreign_keys=ON")
                expected_db.execute(_DDL_CURRENT)
                expected_db.execute(_DDL_TRANSITION)
                expected_sql = {
                    str(row[0]): self._normalized_sql(str(row[1]))
                    for row in expected_db.execute(
                        "SELECT name,sql FROM sqlite_master WHERE name IN (?,?)",
                        ("kis_functional_server_safety", "kis_functional_server_safety_transition"),
                    )
                }
            finally:
                expected_db.close()
            actual_sql = {
                str(row[0]): self._normalized_sql(str(row[1]))
                for row in conn.execute(
                    "SELECT name,sql FROM sqlite_master WHERE name IN (?,?)",
                    ("kis_functional_server_safety", "kis_functional_server_safety_transition"),
                )
            }
            if actual_sql and actual_sql != expected_sql:
                raise KisDomesticFunctionalServerBlocked("server safety schema is dirty")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(_DDL_CURRENT)
            conn.execute(_DDL_TRANSITION)
            rows = conn.execute("SELECT * FROM kis_functional_server_safety").fetchall()
            if not rows:
                occurred_at = _utc_now(self._clock)
                body = {
                    "schemaVersion": "kis-domestic-functional-server-one-shot-transition/v1",
                    "oneShotId": self.context_base["oneShotId"],
                    "contextBaseHash": base_hash, "revision": 1,
                    "state": "ISSUED", "action": "", "reservationId": "",
                    "stateRevision": 0, "sessionId": "", "occurredAt": occurred_at,
                    "previousHash": "0" * 64,
                    "signerKeyIdHash": self._signer_key_id_hash,
                    "productionAvailable": False,
                }
                body_json = _canonical(body); body_hash = _hash(body)
                conn.execute(
                    "INSERT INTO kis_functional_server_safety VALUES (?,?,?,?,?,?,?)",
                    (self.context_base["oneShotId"], base_hash, "ISSUED", "", 1,
                     body_hash, self._signer_key_id_hash),
                )
                conn.execute(
                    "INSERT INTO kis_functional_server_safety_transition VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (self.context_base["oneShotId"], 1, "ISSUED", "", "", 0, "",
                     occurred_at, "0" * 64, body_json, body_hash,
                     self._signature(body_hash), self._signer_key_id_hash),
                )
            elif len(rows) != 1 or rows[0]["one_shot_id"] != self.context_base["oneShotId"] or rows[0]["context_base_hash"] != base_hash:
                raise KisDomesticFunctionalServerBlocked("server one-shot context changed")
            self._verify_one_shot(conn)

    def _verify_one_shot(self, conn: sqlite3.Connection) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM kis_functional_server_safety WHERE one_shot_id=?",
            (self.context_base["oneShotId"],),
        ).fetchone()
        transitions = conn.execute(
            "SELECT * FROM kis_functional_server_safety_transition WHERE one_shot_id=? ORDER BY revision",
            (self.context_base["oneShotId"],),
        ).fetchall()
        if row is None or not transitions or len(transitions) != int(row["revision"]):
            raise KisDomesticFunctionalServerBlocked("server one-shot history is incomplete")
        previous = "0" * 64
        previous_time = ""
        for index, transition in enumerate(transitions, 1):
            try:
                body = json.loads(transition["body_json"])
            except json.JSONDecodeError:
                raise KisDomesticFunctionalServerBlocked("server one-shot body is invalid") from None
            expected = {
                "schemaVersion": "kis-domestic-functional-server-one-shot-transition/v1",
                "oneShotId": self.context_base["oneShotId"],
                "contextBaseHash": _hash(self.context_base), "revision": index,
                "state": str(transition["state"]), "action": str(transition["action"]),
                "reservationId": str(transition["reservation_id"]),
                "stateRevision": int(transition["state_revision"]),
                "sessionId": str(transition["session_id"]),
                "occurredAt": str(transition["occurred_at"]),
                "previousHash": previous, "signerKeyIdHash": self._signer_key_id_hash,
                "productionAvailable": False,
            }
            body_hash = _hash(expected)
            if body != expected or transition["body_hash"] != body_hash or transition["previous_hash"] != previous or transition["signature"] != self._signature(body_hash) or transition["signer_key_id_hash"] != self._signer_key_id_hash:
                raise KisDomesticFunctionalServerBlocked("server one-shot history integrity failed")
            if index == 1 and (
                expected["state"] != "ISSUED" or expected["action"]
                or expected["reservationId"] or expected["stateRevision"] != 0
                or expected["sessionId"]
            ):
                raise KisDomesticFunctionalServerBlocked("server one-shot issue transition is invalid")
            if index == 2 and (
                expected["state"] != "CONSUMED" or expected["action"] != "KIS_START"
                or not _ID.fullmatch(expected["reservationId"])
                or expected["stateRevision"] < 1
                or not _ID.fullmatch(expected["sessionId"])
            ):
                raise KisDomesticFunctionalServerBlocked("server one-shot consume transition is invalid")
            if index > 2 or (previous_time and expected["occurredAt"] < previous_time):
                raise KisDomesticFunctionalServerBlocked("server one-shot transition order is invalid")
            previous = body_hash
            previous_time = expected["occurredAt"]
        tail = transitions[-1]
        if (row["state"], row["consumed_action"], row["transition_head_hash"], row["signer_key_id_hash"]) != (
            tail["state"], tail["action"], tail["body_hash"], self._signer_key_id_hash,
        ):
            raise KisDomesticFunctionalServerBlocked("server one-shot row projection failed")
        return row

    def _context(self, action: str, snapshot: Mapping[str, Any] | None = None) -> dict[str, Any]:
        snapshot = dict(snapshot or self.state.authority_snapshot())
        if snapshot.get("functionalAccountFingerprint") != self.context_base["accountFingerprint"] or snapshot.get("credentialConfigurationHash") != self.context_base["credentialConfigurationHash"]:
            raise KisDomesticFunctionalServerBlocked("approved account/credential no longer matches state")
        return _exact_context({
            **self.context_base, "action": action,
            "stateRevision": snapshot["functionalRevision"],
            "stateSessionId": snapshot["functionalSessionId"],
            "stateOwnerEpochHash": self._owner_epoch(snapshot),
            "managerBindingsHash": self._manager_bindings_hash,
        }, action=action)

    def safety_context(self, action: str) -> dict[str, Any]:
        if action not in _ACTIONS:
            raise KisDomesticFunctionalServerBlocked("safety context action mismatch")
        return self._context(action)

    @staticmethod
    def _result(status: int, body: Mapping[str, Any]) -> KisFunctionalHttpResult:
        return KisFunctionalHttpResult(
            status=status,
            body=dict(body),
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )

    @staticmethod
    def _json_body(body_reader: Callable[[], bytes]) -> dict[str, Any]:
        if not callable(body_reader):
            raise KisDomesticFunctionalServerBlocked("request body reader is invalid")
        raw = body_reader()
        if type(raw) is not bytes or not raw or len(raw) > 64 * 1024:
            raise KisDomesticFunctionalServerBlocked("request body is invalid")
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise KisDomesticFunctionalServerBlocked("request JSON key is duplicated")
                result[key] = value
            return result
        try:
            value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise KisDomesticFunctionalServerBlocked("request JSON is invalid") from None
        if type(value) is not dict:
            raise KisDomesticFunctionalServerBlocked("request JSON object is required")
        return value

    def _authorize(self, *, headers: Mapping[str, object], peer_host: object, mutation: bool) -> KisFunctionalHttpResult | None:
        try:
            self.authority.assert_request(
                headers=headers, peer_host=peer_host, require_origin=mutation
            )
        except (FunctionalHttpSessionError, AttributeError, TypeError) as exc:
            return self._result(403, {
                "ok": False, "reason": "trusted-app-session-required",
                "detail": str(exc)[:240], "brokerSubmissionPerformed": False,
            })
        return None

    def _precheck_confirmation(self, action: str, payload: Mapping[str, Any]) -> tuple[dict[str, Any], Mapping[str, Any]] | KisFunctionalHttpResult:
        context = payload.get("safetyContext")
        confirmation = payload.get("safetyConfirmation")
        try:
            exact = _exact_context(context, action=action)
        except KisDomesticFunctionalServerBlocked as exc:
            return self._result(409, {"ok": False, "reason": str(exc), "brokerSubmissionPerformed": False})
        try:
            approved = self._context(action)
        except KisDomesticFunctionalServerBlocked as exc:
            self.safety.consume(action=action, context=exact, confirmation=confirmation)
            return self._result(409, {"ok": False, "reason": str(exc), "brokerSubmissionPerformed": False})
        if exact != approved:
            self.safety.consume(action=action, context=exact, confirmation=confirmation)
            return self._result(409, {"ok": False, "reason": "safety-context-not-approved", "brokerSubmissionPerformed": False})
        if not isinstance(confirmation, Mapping):
            return self._result(409, {"ok": False, "reason": "safety-confirmation-required", "brokerSubmissionPerformed": False})
        return exact, confirmation

    def _consume_start_one_shot(self, *, reservation: Mapping[str, Any]) -> bool:
        with self.state.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._verify_one_shot(conn)
            if row["state"] != "ISSUED":
                return False
            revision = int(row["revision"]) + 1
            occurred_at = _utc_now(self._clock)
            body = {
                "schemaVersion": "kis-domestic-functional-server-one-shot-transition/v1",
                "oneShotId": self.context_base["oneShotId"],
                "contextBaseHash": _hash(self.context_base), "revision": revision,
                "state": "CONSUMED", "action": "KIS_START",
                "reservationId": reservation["reservationId"],
                "stateRevision": reservation["revision"],
                "sessionId": reservation["sessionId"], "occurredAt": occurred_at,
                "previousHash": row["transition_head_hash"],
                "signerKeyIdHash": self._signer_key_id_hash,
                "productionAvailable": False,
            }
            body_json = _canonical(body); body_hash = _hash(body)
            changed = conn.execute(
                """UPDATE kis_functional_server_safety SET state='CONSUMED',
                   consumed_action='KIS_START',revision=?,transition_head_hash=?
                   WHERE one_shot_id=? AND revision=? AND state='ISSUED' AND transition_head_hash=?""",
                (revision, body_hash, self.context_base["oneShotId"],
                 int(row["revision"]), row["transition_head_hash"]),
            ).rowcount
            if changed != 1:
                return False
            conn.execute(
                "INSERT INTO kis_functional_server_safety_transition VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (self.context_base["oneShotId"], revision, "CONSUMED", "KIS_START",
                 reservation["reservationId"], reservation["revision"],
                 reservation["sessionId"], occurred_at, row["transition_head_hash"],
                 body_json, body_hash, self._signature(body_hash), self._signer_key_id_hash),
            )
            return True

    def _manager_boundary(
        self, *, action: str, context: Mapping[str, Any],
        confirmation: Mapping[str, Any], manager_name: str,
        rejection: dict[str, str],
    ) -> Callable[[Mapping[str, Any]], Mapping[str, Any]]:
        manager = self.managers[manager_name]

        def blocked(reason: str) -> Mapping[str, Any]:
            rejection["reason"] = reason
            return {
                "ok": False, "mutationMayHaveOccurred": False,
                "receiptHash": _hash({"action": action, "reason": reason}),
            }

        def run(reservation: Mapping[str, Any]) -> Mapping[str, Any]:
            try:
                with kis_route_authority_serialization():
                    snapshot = self.state.authority_snapshot()
                    if snapshot["reservationId"] != reservation["reservationId"] or snapshot["reservationKind"] != reservation["reservationKind"] or snapshot["functionalRevision"] != reservation["revision"]:
                        return blocked("state-reservation-changed")
                    if context["stateRevision"] + 1 != reservation["revision"]:
                        return blocked("state-revision-changed")
                    if context["accountFingerprint"] != snapshot["functionalAccountFingerprint"] or context["credentialConfigurationHash"] != snapshot["credentialConfigurationHash"]:
                        return blocked("state-account-or-credential-changed")
                    if context["stateOwnerEpochHash"] != self._owner_epoch(snapshot) or context["managerBindingsHash"] != self._manager_bindings_hash:
                        return blocked("state-owner-or-manager-binding-changed")
                    current_session = snapshot["functionalSessionId"]
                    if action == "KIS_START":
                        if context["stateSessionId"] or current_session:
                            return blocked("start-session-lineage-changed")
                    elif context["stateSessionId"]:
                        if current_session != context["stateSessionId"]:
                            return blocked("cleanup-session-lineage-changed")
                    elif action == "KIS_RECOVER" and current_session != reservation["sessionId"]:
                        return blocked("recover-session-lineage-changed")
                    consumed = self.safety.consume(
                        action=action, context=context, confirmation=confirmation,
                    )
                    if consumed.get("ok") is not True:
                        return blocked(str(consumed.get("reason") or "safety-confirmation-rejected"))
                    if action == "KIS_START" and not self._consume_start_one_shot(reservation=reservation):
                        return blocked("start-one-shot-consumed")
                return manager.invoke(reservation)
            except (KisDomesticFunctionalServerBlocked, KisDomesticFunctionalStateBlocked) as exc:
                return blocked(str(exc))

        return run

    def handle(
        self,
        *,
        method: str,
        path: str,
        headers: Mapping[str, object],
        peer_host: object,
        body_reader: Callable[[], bytes] | None = None,
    ) -> KisFunctionalHttpResult:
        if type(method) is not str or type(path) is not str:
            return self._result(404, {"ok": False, "reason": "route-not-found"})
        if path == _BOOTSTRAP or path.startswith(_BOOTSTRAP + "?"):
            return self._result(503, {
                "ok": False, "reason": "kis-functional-bootstrap-unavailable",
                "setCookiePerformed": False, "brokerSubmissionPerformed": False,
            })
        if "?" in path or "#" in path or not path.startswith("/") or "//" in path or any(character.isspace() for character in path):
            return self._result(404, {"ok": False, "reason": "route-not-found"})
        mutation = method == "POST"
        if method not in {"GET", "POST"}:
            return self._result(405, {"ok": False, "reason": "method-not-allowed"})
        denied = self._authorize(headers=headers, peer_host=peer_host, mutation=mutation)
        if denied is not None:
            return denied
        if method == "GET" and path == _STATUS:
            return self._result(200, {"ok": True, "state": self.state.status(), **production_entrypoint_status()})
        if method != "POST" or path not in {_CHALLENGE, _START, _STOP, _RECOVER}:
            return self._result(404, {"ok": False, "reason": "route-not-found"})
        try:
            payload = self._json_body(body_reader)  # auth is deliberately complete first
        except KisDomesticFunctionalServerBlocked as exc:
            return self._result(400, {"ok": False, "reason": str(exc), "brokerSubmissionPerformed": False})
        if path == _CHALLENGE:
            if set(payload) != {"action", "safetyContext"} or payload.get("action") not in _ACTIONS:
                return self._result(400, {"ok": False, "reason": "challenge payload is not exact"})
            action = payload["action"]
            try:
                context = _exact_context(payload["safetyContext"], action=action)
            except KisDomesticFunctionalServerBlocked as exc:
                return self._result(409, {"ok": False, "reason": str(exc)})
            try:
                approved = self._context(action)
            except KisDomesticFunctionalServerBlocked as exc:
                return self._result(409, {"ok": False, "reason": str(exc)})
            if context != approved:
                return self._result(409, {"ok": False, "reason": "safety-context-not-approved"})
            if action == "KIS_START":
                try:
                    with self.state.ledger.connection() as conn:
                        if self._verify_one_shot(conn)["state"] != "ISSUED":
                            return self._result(409, {"ok": False, "reason": "start-one-shot-consumed"})
                except KisDomesticFunctionalServerBlocked as exc:
                    return self._result(409, {"ok": False, "reason": str(exc)})
            issued = self.safety.issue(
                action=action, context=context,
                expected_phrase=f"{action} {self.context_base['oneShotId']}",
                display_context={"route": ROUTE, "pdno": PDNO, "action": action, "oneShotId": self.context_base["oneShotId"]},
            )
            return self._result(200, issued)

        action = {_START: "KIS_START", _STOP: "KIS_STOP", _RECOVER: "KIS_RECOVER"}[path]
        required = {"safetyContext", "safetyConfirmation"} | ({"sessionId"} if path == _START else set())
        if set(payload) != required:
            return self._result(400, {"ok": False, "reason": "mutation payload is not exact", "brokerSubmissionPerformed": False})
        prechecked = self._precheck_confirmation(action, payload)
        if isinstance(prechecked, KisFunctionalHttpResult):
            return prechecked
        context, confirmation = prechecked
        rejection: dict[str, str] = {}
        if path == _START:
            session_id = payload.get("sessionId")
            if type(session_id) is not str or not _ID.fullmatch(session_id):
                return self._result(400, {"ok": False, "reason": "session id is invalid", "brokerSubmissionPerformed": False})
            manager = self._manager_boundary(
                action=action, context=context, confirmation=confirmation,
                manager_name="start", rejection=rejection,
            )
            call = lambda: self.state.start(session_id=session_id, manager=manager)
        elif path == _STOP:
            manager = self._manager_boundary(
                action=action, context=context, confirmation=confirmation,
                manager_name="stop", rejection=rejection,
            )
            call = lambda: self.state.stop(manager=manager)
        else:
            manager = self._manager_boundary(
                action=action, context=context, confirmation=confirmation,
                manager_name="recover", rejection=rejection,
            )
            call = lambda: self.state.kill(manager=manager)
        try:
            state_result = call()
        except KisDomesticFunctionalStateBlocked as exc:
            return self._result(409, {"ok": False, "reason": str(exc), "brokerSubmissionPerformed": False})
        if rejection:
            return self._result(409, {
                "ok": False, "reason": rejection["reason"],
                "stateResult": state_result, "brokerSubmissionPerformed": False,
            })
        return self._result(200, {
            "ok": True, "stateResult": state_result,
            "brokerSubmissionPerformed": False, **production_entrypoint_status(),
        })


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False, "networkAvailable": False,
        "bootstrapAvailable": False, "backendAvailable": False,
        "brokerMutationAvailable": False, "releaseEvidenceAvailable": False,
        "route": ROUTE, "pdno": PDNO,
        "reason": "DISABLED_OFFLINE_HTTP_ADAPTER_NO_BOOTSTRAP_OR_BACKEND",
    }


__all__ = [
    "DisabledKisDomesticFunctionalServer", "KisDomesticFunctionalServerBlocked",
    "DisabledKisFunctionalOfflineManager", "KisFunctionalHttpResult",
    "production_entrypoint_status",
]
