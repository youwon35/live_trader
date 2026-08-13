from __future__ import annotations

"""Disabled final KIS mutation transport gate.

The gate owns no production sender.  Tests may inject one explicit mock sender;
the gate independently joins the durable mutation request, capability lease and
the state-owned KIS final authority boundary before recording a pre-POST marker.
"""

from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
import sqlite3
from typing import Any, Callable, Mapping

from .kis_domestic_functional_capability import (
    DurableKisDomesticFunctionalCapabilityLedger,
    KisDomesticFunctionalCapabilityBlocked,
)
from .kis_domestic_functional_mutation import (
    DurableKisDomesticFunctionalMutationJournal,
    KisDomesticFunctionalMutationBlocked,
)
from .kis_order_authority import (
    KisOrderAuthorityError,
    functional_kis_final_mutation_boundary,
    kis_route_authority_serialization,
)
from .emergency_stop import emergency_stop_status
from .program_ledger import ProgramLedger


KIS_DOMESTIC_FUNCTIONAL_TRANSPORT_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TRANSPORT_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TRANSPORT_SENDER_AVAILABLE = False

ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
LIVE_ORIGIN = "https://openapi.koreainvestment.com:9443"
_SCHEMA_VERSION = "kis-domestic-functional-transport-schema/v2"
_RECORD_DOMAIN = b"kis-domestic-functional-transport-record/v1\x00"
_SHA = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_OFFICIAL_ID = re.compile(r"^[0-9]{1,16}$", flags=re.ASCII)
_TERMINAL_MUTATION_STATES = {"NOT_SENT", "FILLED", "CANCELED", "REJECTED"}

_OPERATIONS = {
    "NATURAL_BUY": (
        "/uapi/domestic-stock/v1/trading/order-cash", "TTTC0012U", "BUY", False
    ),
    "CLEANUP_SELL": (
        "/uapi/domestic-stock/v1/trading/order-cash", "TTTC0011U", "SELL", True
    ),
    "CLEANUP_CANCEL": (
        "/uapi/domestic-stock/v1/trading/order-rvsecncl", "TTTC0013U", "CANCEL", True
    ),
}

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS kis_functional_transport_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    owner_hash TEXT NOT NULL,
    signer_key_id_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kis_functional_transport_dispatch (
    claim_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    authority_revision INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('PREPARED','PRE_POST','RESPONSE_ARCHIVED')),
    request_json TEXT NOT NULL,
    request_hash TEXT NOT NULL UNIQUE,
    request_signature TEXT NOT NULL,
    sender_kind TEXT NOT NULL CHECK(sender_kind IN ('MOCK','OWNED_PRODUCTION_DISABLED')),
    sender_owner_hash TEXT NOT NULL,
    sender_code_hash TEXT NOT NULL,
    sender_credential_configuration_hash TEXT NOT NULL,
    response_json TEXT NOT NULL DEFAULT '',
    response_hash TEXT NOT NULL DEFAULT '',
    response_signature TEXT NOT NULL DEFAULT '',
    response_ack_json TEXT NOT NULL DEFAULT '',
    response_ack_hash TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    pre_post_at TEXT NOT NULL DEFAULT '',
    response_at TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL CHECK(revision>=1)
)
""".strip()


class KisDomesticFunctionalTransportBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _utc(value: datetime, label: str) -> str:
    if type(value) is not datetime or value.tzinfo is None:
        raise KisDomesticFunctionalTransportBlocked(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _identity(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalTransportBlocked(f"{label} is invalid")
    return value


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalTransportBlocked(f"{label} is invalid")
    return value


def _parse_utc(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalTransportBlocked(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise KisDomesticFunctionalTransportBlocked(f"{label} is invalid") from None
    if parsed.tzinfo is None or _utc(parsed, label) != value:
        raise KisDomesticFunctionalTransportBlocked(f"{label} is not canonical")
    return parsed.astimezone(timezone.utc)


def _snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = [
        tuple(row)
        for row in conn.execute(
            """SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master
               WHERE name LIKE 'kis_functional_transport_%'
                  OR tbl_name LIKE 'kis_functional_transport_%'
               ORDER BY type,name"""
        )
    ]
    tables: dict[str, Any] = {}
    for kind, name, _, _ in objects:
        if kind != "table":
            continue
        escaped = str(name).replace('"', '""')
        tables[str(name)] = {
            "tableInfo": tuple(
                tuple(row) for row in conn.execute(f'PRAGMA table_xinfo("{escaped}")')
            ),
            "indexes": tuple(
                tuple(row) for row in conn.execute(f'PRAGMA index_list("{escaped}")')
            ),
            "foreignKeys": tuple(
                tuple(row) for row in conn.execute(f'PRAGMA foreign_key_list("{escaped}")')
            ),
        }
    return {"objects": objects, "tables": tables}


class DurableKisDomesticFunctionalTransport:
    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        mutation_journal: DurableKisDomesticFunctionalMutationJournal,
        capability_ledger: DurableKisDomesticFunctionalCapabilityLedger,
        signer_key: bytes,
        signer_key_id: str,
        owner_id: str,
        mock_sender: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        allow_mock_sender: bool,
        sender_owner_id: str = "disabled-mock-kis-transport-sender-v1",
        sender_code_hash: str = "0" * 64,
        sender_credential_configuration_hash: str | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalTransportBlocked("exact ProgramLedger is required")
        if type(mutation_journal) is not DurableKisDomesticFunctionalMutationJournal:
            raise KisDomesticFunctionalTransportBlocked("exact mutation journal is required")
        if type(capability_ledger) is not DurableKisDomesticFunctionalCapabilityLedger:
            raise KisDomesticFunctionalTransportBlocked("exact capability ledger is required")
        if (
            mutation_journal.ledger.path.resolve() != program_ledger.path.resolve()
            or capability_ledger.ledger.path.resolve() != program_ledger.path.resolve()
        ):
            raise KisDomesticFunctionalTransportBlocked("durable component ledger paths differ")
        if type(allow_mock_sender) is not bool or allow_mock_sender is not True:
            raise KisDomesticFunctionalTransportBlocked("only explicit mock sender is allowed")
        if not callable(mock_sender):
            raise KisDomesticFunctionalTransportBlocked("mock sender is invalid")
        if type(signer_key) is not bytes or len(signer_key) < 32:
            raise KisDomesticFunctionalTransportBlocked("transport signer key is invalid")
        if type(signer_key_id) is not str or not _ID.fullmatch(signer_key_id):
            raise KisDomesticFunctionalTransportBlocked("transport signer key id is invalid")
        if type(owner_id) is not str or not _ID.fullmatch(owner_id):
            raise KisDomesticFunctionalTransportBlocked("transport owner id is invalid")
        self.ledger = program_ledger
        self.mutation = mutation_journal
        self.capability = capability_ledger
        self._key = bytes(signer_key)
        self._key_id_hash = hashlib.sha256(signer_key_id.encode()).hexdigest()
        self._owner_hash = hashlib.sha256(owner_id.encode()).hexdigest()
        self._sender = mock_sender
        self._sender_kind = "MOCK"
        self._sender_owner_hash = hashlib.sha256(
            _identity(sender_owner_id, "transport sender owner id").encode()
        ).hexdigest()
        self._sender_code_hash = _sha(
            sender_code_hash, "transport sender code hash"
        )
        self._sender_credential_configuration_hash = _sha(
            sender_credential_configuration_hash or "0" * 64,
            "transport sender credential configuration hash",
        )
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._ensure_schema()

    def _now(self) -> str:
        return _utc(self.clock(), "transport clock")

    def _signed(self, body: Mapping[str, Any]) -> tuple[str, str]:
        digest = _hash(body)
        signature = hmac.new(
            self._key,
            _RECORD_DOMAIN + _canonical({**dict(body), "recordHash": digest}).encode(),
            hashlib.sha256,
        ).hexdigest()
        return digest, signature

    @classmethod
    def from_disabled_production_sender(
        cls, *,
        program_ledger: ProgramLedger,
        mutation_journal: DurableKisDomesticFunctionalMutationJournal,
        capability_ledger: DurableKisDomesticFunctionalCapabilityLedger,
        signer_key: bytes, signer_key_id: str, owner_id: str,
        production_transport: Any,
        sender_owner_id: str, sender_code_hash: str,
        credential_configuration_hash: str,
        production_binding_pins: Mapping[str, str],
        clock: Callable[[], datetime] | None = None,
    ) -> "DurableKisDomesticFunctionalTransport":
        from .kis_domestic_functional_production_transport import (
            DisabledKisDomesticFunctionalProductionTransport,
        )

        if type(production_transport) is not DisabledKisDomesticFunctionalProductionTransport:
            raise KisDomesticFunctionalTransportBlocked(
                "exact disabled production transport is required"
            )
        binding = production_transport.production_binding_status()
        if production_transport.verify_production_binding_status(binding) is not True:
            raise KisDomesticFunctionalTransportBlocked(
                "production transport binding envelope is unverified"
            )
        expected_owner_hash = hashlib.sha256(
            _identity(sender_owner_id, "transport sender owner id").encode()
        ).hexdigest()
        pin_keys = {
            "credentialEnvironmentKeyIdHash",
            "tokenVerifierKeyIdHash",
            "authorizationVerifierKeyIdHash",
            "tokenReaderCodeHash",
            "tokenVerifierCodeHash",
            "authorizationBuilderCodeHash",
            "authorizationVerifierCodeHash",
            "credentialEnvironmentReaderCodeHash",
            "credentialEnvironmentVerifierCodeHash",
        }
        if not isinstance(production_binding_pins, Mapping) or set(
            production_binding_pins
        ) != pin_keys:
            raise KisDomesticFunctionalTransportBlocked(
                "production transport verifier/callback pins are not exact"
            )
        pinned = {
            key: _sha(production_binding_pins[key], f"transport {key}")
            for key in sorted(pin_keys)
        }
        exact_binding = {
            "schemaVersion": "kis-domestic-functional-production-binding/v1",
            "route": ROUTE, "pdno": PDNO, "origin": LIVE_ORIGIN,
            "gateOwnerHash": expected_owner_hash,
            "gateCodeHash": _sha(sender_code_hash, "transport sender code hash"),
            "credentialConfigurationHash": _sha(
                credential_configuration_hash,
                "transport sender credential configuration hash",
            ),
            **pinned,
            "networkCompiled": False, "productionAvailable": False,
        }
        for key, expected in exact_binding.items():
            if type(binding.get(key)) is not type(expected) or binding.get(key) != expected:
                raise KisDomesticFunctionalTransportBlocked(
                    f"production transport {key} binding mismatch"
                )
        gate, gate_token = production_transport._gate_binding_for_transport_owner()

        def sender(request: Mapping[str, Any]) -> Mapping[str, Any]:
            try:
                raw = gate.send(request, gate_call_token=gate_token)
            except Exception:
                raw = production_transport.last_failed_attempt()
            return {
                "schemaVersion": "kis-domestic-functional-owned-response/v1",
                "method": raw["method"], "origin": raw["origin"],
                "endpoint": raw["endpoint"], "trId": raw["trId"],
                "effectiveUrl": raw["effectiveUrl"],
                "requestHash": raw["requestHash"],
                "physicalAttemptCount": raw["physicalAttemptCount"],
                "hiddenRetryCount": raw["hiddenRetryCount"],
                "redirectFollowCount": raw["redirectFollowCount"],
                "statusCode": raw["statusCode"],
                "observedAt": raw["observedAt"], "body": raw["body"],
                "physicalTrace": raw["physicalTrace"],
                "physicalTraceHash": raw["physicalTraceHash"],
                "physicalTraceOwned": raw["physicalTraceOwned"],
                "attemptBinding": raw["attemptBinding"],
                "attemptBindingHash": raw["attemptBindingHash"],
                "errorArchive": raw["errorArchive"],
                "errorArchiveHash": raw["errorArchiveHash"],
                "authorizationMaterialArchived": raw["authorizationMaterialArchived"],
                "accountIdentifiersArchived": raw["accountIdentifiersArchived"],
            }

        value = cls(
            program_ledger=program_ledger, mutation_journal=mutation_journal,
            capability_ledger=capability_ledger, signer_key=signer_key,
            signer_key_id=signer_key_id, owner_id=owner_id,
            mock_sender=sender, allow_mock_sender=True,
            sender_owner_id=sender_owner_id, sender_code_hash=sender_code_hash,
            sender_credential_configuration_hash=credential_configuration_hash,
            clock=clock,
        )
        value._sender_kind = "OWNED_PRODUCTION_DISABLED"
        value._production_binding_hash = binding["bindingHash"]
        value._production_binding = dict(binding)
        return value

    def _ensure_schema(self) -> None:
        statements = [item.strip() for item in _SCHEMA_SQL.split(";") if item.strip()]
        expected = sqlite3.connect(":memory:")
        try:
            for statement in statements:
                expected.execute(statement)
            expected_hash = _hash(_snapshot(expected))
        finally:
            expected.close()
        with self.ledger.connection() as conn:
            before = _snapshot(conn)
            if before["objects"] and not hmac.compare_digest(_hash(before), expected_hash):
                raise KisDomesticFunctionalTransportBlocked(
                    "transport SQLite schema fingerprint mismatch"
                )
            conn.execute("BEGIN IMMEDIATE")
            for statement in statements:
                conn.execute(statement)
            if not hmac.compare_digest(_hash(_snapshot(conn)), expected_hash):
                raise KisDomesticFunctionalTransportBlocked(
                    "transport SQLite schema fingerprint mismatch"
                )
            expected_row = (
                1, _SCHEMA_VERSION, expected_hash, self._owner_hash,
                self._key_id_hash,
            )
            rows = conn.execute("SELECT * FROM kis_functional_transport_schema").fetchall()
            if not rows:
                conn.execute(
                    "INSERT INTO kis_functional_transport_schema VALUES (?,?,?,?,?)",
                    expected_row,
                )
            elif len(rows) != 1 or tuple(rows[0]) != expected_row:
                raise KisDomesticFunctionalTransportBlocked(
                    "transport schema owner/key manifest mismatch"
                )
        self._schema_hash = expected_hash

    @staticmethod
    def _request_from_mutation(row: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        operation = str(row["operation"])
        spec = _OPERATIONS.get(operation)
        if spec is None or (
            str(row["endpoint"]), str(row["tr_id"]), str(row["side"]),
            bool(row["cleanup_only"]),
        ) != spec:
            raise KisDomesticFunctionalTransportBlocked("mutation route allowlist mismatch")
        try:
            payload = json.loads(str(row["payload_json"]))
            sealed_intent = json.loads(str(row["authority_intent_json"]))
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalTransportBlocked("sealed mutation JSON is invalid") from None
        if not isinstance(payload, dict) or _hash(payload) != str(row["payload_hash"]):
            raise KisDomesticFunctionalTransportBlocked("sealed mutation payload hash mismatch")
        authority_intent = {
            "operation": operation,
            "claimId": str(row["claim_id"]),
            "ownedOrderKey": dict(sealed_intent["ownedOrderKey"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(row["credential_configuration_hash"]),
            "endpoint": str(row["endpoint"]),
            "payloadHash": str(row["payload_hash"]),
        }
        request = {
            "schemaVersion": "kis-domestic-functional-transport-request/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "origin": LIVE_ORIGIN,
            "method": "POST",
            "endpoint": str(row["endpoint"]),
            "trId": str(row["tr_id"]),
            "query": [],
            "headers": {"custtype": "P", "tr_id": str(row["tr_id"])},
            "operation": operation,
            "side": str(row["side"]),
            "cleanupOnly": bool(row["cleanup_only"]),
            "claimId": str(row["claim_id"]),
            "sessionId": str(row["session_id"]),
            "authorityRevision": int(row["authority_revision"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(row["credential_configuration_hash"]),
            "payload": payload,
            "payloadHash": str(row["payload_hash"]),
        }
        return request, authority_intent

    def _verify_capability(
        self, lease: Mapping[str, Any], request: Mapping[str, Any]
    ) -> dict[str, Any]:
        try:
            verified = self.capability.verify_authorization_lease(lease)
        except KisDomesticFunctionalCapabilityBlocked as exc:
            raise KisDomesticFunctionalTransportBlocked("capability lease rejected") from exc
        exact = {
            "route": ROUTE,
            "pdno": PDNO,
            "active": True,
            "sessionId": request["sessionId"],
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request["credentialConfigurationHash"],
            "revision": request["authorityRevision"],
            "operation": request["operation"],
            "cleanupOnly": request["cleanupOnly"],
            "productionAvailable": False,
        }
        for key, expected in exact.items():
            if type(verified.get(key)) is not type(expected) or verified.get(key) != expected:
                raise KisDomesticFunctionalTransportBlocked(
                    f"capability lease {key} binding mismatch"
                )
        return verified

    def _verify_response(self, response: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
        common = {
            "schemaVersion", "method", "origin", "endpoint", "trId",
            "effectiveUrl", "requestHash", "physicalAttemptCount",
            "hiddenRetryCount", "redirectFollowCount", "statusCode",
            "observedAt", "body",
        }
        owned_extra = {
            "physicalTrace", "physicalTraceHash", "authorizationMaterialArchived",
            "accountIdentifiersArchived", "physicalTraceOwned",
            "attemptBinding", "attemptBindingHash", "errorArchive",
            "errorArchiveHash",
        }
        expected_keys = common | (
            owned_extra if self._sender_kind == "OWNED_PRODUCTION_DISABLED" else set()
        )
        if not isinstance(response, Mapping) or set(response) != expected_keys:
            raise KisDomesticFunctionalTransportBlocked("mock response schema is not exact")
        value = dict(response)
        exact = {
            "schemaVersion": (
                "kis-domestic-functional-owned-response/v1"
                if self._sender_kind == "OWNED_PRODUCTION_DISABLED"
                else "kis-domestic-functional-mock-response/v1"
            ),
            "method": "POST",
            "origin": LIVE_ORIGIN,
            "endpoint": request["endpoint"],
            "trId": request["trId"],
            "effectiveUrl": LIVE_ORIGIN + str(request["endpoint"]),
            "requestHash": _hash(request),
            "physicalAttemptCount": 1,
            "hiddenRetryCount": 0,
            "redirectFollowCount": 0,
        }
        for key, expected in exact.items():
            if type(value.get(key)) is not type(expected) or value.get(key) != expected:
                raise KisDomesticFunctionalTransportBlocked(
                    f"mock response {key} mismatch"
                )
        if type(value.get("statusCode")) is not int or not 100 <= value["statusCode"] <= 599:
            raise KisDomesticFunctionalTransportBlocked("mock response status is invalid")
        if not isinstance(value.get("body"), Mapping):
            raise KisDomesticFunctionalTransportBlocked("mock raw response body is invalid")
        if type(value.get("observedAt")) is not str:
            raise KisDomesticFunctionalTransportBlocked("mock response observedAt is invalid")
        try:
            parsed = datetime.fromisoformat(value["observedAt"].replace("Z", "+00:00"))
        except ValueError:
            raise KisDomesticFunctionalTransportBlocked("mock response observedAt is invalid") from None
        if _utc(parsed, "mock response observedAt") != value["observedAt"]:
            raise KisDomesticFunctionalTransportBlocked("mock response observedAt is not canonical")
        attempt_failed = bool(value.get("errorArchive"))
        expected_attempt_binding = {
            "schemaVersion": "kis-domestic-functional-pre-socket-binding/v1",
            "requestHash": _hash(request), "sessionId": request["sessionId"],
            "authorityRevision": request["authorityRevision"],
            "operation": request["operation"], "endpoint": request["endpoint"],
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request[
                "credentialConfigurationHash"
            ],
            "productionBindingHash": getattr(
                self, "_production_binding_hash", ""
            ),
        }
        if self._sender_kind == "OWNED_PRODUCTION_DISABLED" and (
            not isinstance(value.get("physicalTrace"), Mapping)
            or type(value.get("physicalTraceHash")) is not str
            or not _SHA.fullmatch(value["physicalTraceHash"])
            or _hash(dict(value["physicalTrace"])) != value["physicalTraceHash"]
            or value["physicalTrace"].get("requestHash") != _hash(request)
            or value["physicalTrace"].get("physicalAttemptCount") != 1
            or value["physicalTrace"].get("physicalAttemptComplete")
            is not (not attempt_failed)
            or value["physicalTrace"].get("hiddenRetryCount") != 0
            or value["physicalTrace"].get("redirectFollowCount") != 0
            or value["physicalTrace"].get("effectiveUrl")
            != LIVE_ORIGIN + str(request["endpoint"])
            or value["physicalTrace"].get("effectiveUrlExact") is not True
            or value["physicalTrace"].get("statusCode") != value["statusCode"]
            or value["physicalTrace"].get("observedAt") != value["observedAt"]
            or value["physicalTrace"].get("responseBodyHash") != _hash(value["body"])
            or not isinstance(value.get("attemptBinding"), Mapping)
            or any(
                type(value["attemptBinding"].get(key)) is not type(expected)
                or value["attemptBinding"].get(key) != expected
                for key, expected in expected_attempt_binding.items()
            )
            or set(value["attemptBinding"]) != set(expected_attempt_binding) | {
                "environmentRevision", "credentialEnvironmentKeyIdHash"
            }
            or type(value["attemptBinding"].get("environmentRevision")) is not int
            or value["attemptBinding"]["environmentRevision"] < 1
            or type(value["attemptBinding"].get("credentialEnvironmentKeyIdHash"))
            is not str
            or not _SHA.fullmatch(
                value["attemptBinding"]["credentialEnvironmentKeyIdHash"]
            )
            or value.get("attemptBindingHash") != _hash(
                dict(value.get("attemptBinding") or {})
            )
            or value["physicalTrace"].get("attemptBindingHash")
            != value.get("attemptBindingHash")
            or value.get("physicalTraceOwned") is not True
            or not isinstance(value.get("errorArchive"), Mapping)
            or (
                attempt_failed
                and value.get("errorArchiveHash")
                != _hash(dict(value["errorArchive"]))
            )
            or (
                not attempt_failed and value.get("errorArchiveHash") != ""
            )
            or (
                attempt_failed
                and value["physicalTrace"].get("errorArchiveHash")
                != value.get("errorArchiveHash")
            )
            or value["physicalTrace"].get("productionAvailable") is not False
            or value.get("authorizationMaterialArchived") is not False
            or value.get("accountIdentifiersArchived") is not False
        ):
            raise KisDomesticFunctionalTransportBlocked(
                "owned response physical/auth trace is invalid"
            )
        return value

    @staticmethod
    def _response_ack(response: Mapping[str, Any]) -> dict[str, Any]:
        body = response["body"]
        rt_cd = body.get("rt_cd")
        if type(rt_cd) is not str:
            return {"result": "UNPROVEN", "orderDate": "", "organizationNo": "", "orderNo": "", "exact": False}
        if rt_cd != "0":
            return {"result": "REJECTED", "orderDate": "", "organizationNo": "", "orderNo": "", "exact": False}
        output = body.get("output")
        if not isinstance(output, Mapping):
            return {"result": "ACK_INCOMPLETE", "orderDate": "", "organizationNo": "", "orderNo": "", "exact": False}

        def aliases(names: tuple[str, ...]) -> str:
            values = {output[name] for name in names if name in output}
            if len(values) > 1 or any(type(item) is not str for item in values):
                raise KisDomesticFunctionalTransportBlocked("KIS POST ACK aliases conflict")
            return next(iter(values), "")

        order_date = aliases(("ORD_DT", "ORD_DATE"))
        organization = aliases(("KRX_FWDG_ORD_ORGNO", "KRX_FWDG_ORD_ORG_NO"))
        order_no = aliases(("ODNO",))
        exact = (
            bool(re.fullmatch(r"[0-9]{8}", order_date))
            and bool(_OFFICIAL_ID.fullmatch(organization))
            and bool(_OFFICIAL_ID.fullmatch(order_no))
        )
        return {
            "result": "ACK_EXACT" if exact else "ACK_INCOMPLETE",
            "orderDate": order_date if exact else "",
            "organizationNo": organization if exact else "",
            "orderNo": order_no if exact else "", "exact": exact,
        }

    def dispatch(
        self,
        *,
        claim_id: str,
        capability_lease: Mapping[str, Any],
        crash_hook: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        if type(claim_id) is not str or not _ID.fullmatch(claim_id):
            raise KisDomesticFunctionalTransportBlocked("transport claim id is invalid")
        if crash_hook is not None and not callable(crash_hook):
            raise KisDomesticFunctionalTransportBlocked("transport crash hook is invalid")
        try:
            mutation_row = self.mutation.read(claim_id)
        except KisDomesticFunctionalMutationBlocked as exc:
            raise KisDomesticFunctionalTransportBlocked("sealed mutation request rejected") from exc
        if mutation_row["state"] not in {"SEALED", "SENDER_ENTERED"}:
            raise KisDomesticFunctionalTransportBlocked(
                "mutation is not retryable before sender entry"
            )
        request, intent = self._request_from_mutation(mutation_row)
        self._verify_capability(capability_lease, request)
        if (
            self._sender_kind == "OWNED_PRODUCTION_DISABLED"
            and self._sender_credential_configuration_hash
            != request["credentialConfigurationHash"]
        ):
            raise KisDomesticFunctionalTransportBlocked(
                "owned sender credential configuration changed"
            )
        request_hash, request_signature = self._signed(request)
        now = self._now()
        if _parse_utc(now, "transport preparedAt") < _parse_utc(
            mutation_row["created_at"], "mutation createdAt"
        ):
            raise KisDomesticFunctionalTransportBlocked(
                "transport prepared before mutation seal"
            )
        with self.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM kis_functional_transport_dispatch WHERE claim_id=?",
                (claim_id,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO kis_functional_transport_dispatch
                       (claim_id,session_id,operation,authority_revision,state,
                        request_json,request_hash,request_signature,sender_kind,
                        sender_owner_hash,sender_code_hash,
                        sender_credential_configuration_hash,created_at,revision)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                    (
                        claim_id, request["sessionId"], request["operation"],
                        request["authorityRevision"], "PREPARED", _canonical(request),
                        request_hash, request_signature, self._sender_kind,
                        self._sender_owner_hash, self._sender_code_hash,
                        self._sender_credential_configuration_hash, now,
                    ),
                )
            elif (
                existing["state"] != "PREPARED"
                or existing["request_hash"] != request_hash
                or existing["request_json"] != _canonical(request)
                or not hmac.compare_digest(existing["request_signature"], request_signature)
            ):
                raise KisDomesticFunctionalTransportBlocked(
                    "transport request was already attempted or changed"
                )

        try:
            boundary = functional_kis_final_mutation_boundary(
                operation=request["operation"],
                session_id=request["sessionId"],
                cleanup_only=request["cleanupOnly"],
                expected_revision=request["authorityRevision"],
                intent=intent,
            )
            with boundary as authority_read:
                if mutation_row["state"] == "SEALED":
                    transitioned = self.mutation.transition(
                        claim_id=claim_id,
                        expected_revision=int(mutation_row["revision"]),
                        target_state="SENDER_ENTERED",
                    )
                    mutation_revision = int(transitioned["revision"])
                else:
                    mutation_revision = int(mutation_row["revision"])
                if crash_hook is not None:
                    crash_hook("AFTER_SENDER_ENTERED")

                # Final Kill/STOP, route revision, identity and capability reads
                # occur while route -> emergency serialization is held.
                authority = authority_read(
                    endpoint=request["endpoint"], payload_hash=request["payloadHash"]
                )
                capability = self._verify_capability(capability_lease, request)
                emergency = emergency_stop_status()
                if (
                    emergency.get("active") is True and not request["cleanupOnly"]
                    or str(authority.get("emergencyRevision") or "")
                    != str(emergency.get("revision") or "")
                    or authority.get("expectedRevision") != request["authorityRevision"]
                    or capability.get("revision") != request["authorityRevision"]
                ):
                    raise KisDomesticFunctionalTransportBlocked(
                        "final Kill/STOP or authority revision changed"
                    )

                marker = self.mutation.transition(
                    claim_id=claim_id,
                    expected_revision=mutation_revision,
                    target_state="POST_MAY_HAVE_CROSSED",
                )
                pre_post_at = self._now()
                marker_row = self.mutation.read(claim_id)
                if not (
                    _parse_utc(marker_row["sender_entered_at"], "mutation senderEnteredAt")
                    <= _parse_utc(marker_row["post_marker_at"], "mutation postMarkerAt")
                    <= _parse_utc(pre_post_at, "transport prePostAt")
                ):
                    raise KisDomesticFunctionalTransportBlocked(
                        "mutation/transport pre-POST time lineage failed"
                    )
                with self.ledger.connection() as conn:
                    cursor = conn.execute(
                        """UPDATE kis_functional_transport_dispatch
                           SET state='PRE_POST',pre_post_at=?,revision=2
                           WHERE claim_id=? AND state='PREPARED' AND revision=1""",
                        (pre_post_at, claim_id),
                    )
                    if cursor.rowcount != 1:
                        raise KisDomesticFunctionalTransportBlocked(
                            "transport pre-POST CAS failed"
                        )
                if crash_hook is not None:
                    crash_hook("AFTER_PRE_POST_MARKER")
                raw_response = self._sender(request)
        except (KisOrderAuthorityError, KisDomesticFunctionalCapabilityBlocked) as exc:
            raise KisDomesticFunctionalTransportBlocked("final authority gate rejected") from exc

        if not isinstance(raw_response, Mapping):
            raise KisDomesticFunctionalTransportBlocked(
                "mock sender returned no archivable raw response"
            )
        response = dict(raw_response)
        response_hash, response_signature = self._signed(response)
        response_at = self._now()
        try:
            verified_response = self._verify_response(response, request)
            response_ack = self._response_ack(verified_response)
        except KisDomesticFunctionalTransportBlocked:
            verified_response = None
            response_ack = {
                "result": "INVALID_RESPONSE", "orderDate": "",
                "organizationNo": "", "orderNo": "", "exact": False,
            }
        ack_json = _canonical(response_ack); ack_hash = _hash(response_ack)
        with self.ledger.connection() as conn:
            cursor = conn.execute(
                """UPDATE kis_functional_transport_dispatch
                   SET state='RESPONSE_ARCHIVED',response_json=?,response_hash=?,
                       response_signature=?,response_ack_json=?,response_ack_hash=?,
                       response_at=?,revision=3
                   WHERE claim_id=? AND state='PRE_POST' AND revision=2""",
                (
                    _canonical(response), response_hash, response_signature,
                    ack_json, ack_hash, response_at, claim_id,
                ),
            )
            if cursor.rowcount != 1:
                raise KisDomesticFunctionalTransportBlocked(
                    "transport response archive CAS failed"
                )
        # Archive the one physical response before interpreting its schema.
        # A malformed/retry/redirect response remains durable ambiguity.
        if verified_response is None:
            self._verify_response(response, request)
        self.read(claim_id)
        return {
            "claimId": claim_id,
            "state": "POST_MAY_HAVE_CROSSED",
            "mutationRevision": marker["revision"],
            "requestHash": request_hash,
            "responseHash": response_hash,
            "physicalAttemptCount": 1,
            "retryAllowed": False,
            "productionAvailable": False,
        }

    def _read_locked(
        self, conn: sqlite3.Connection, claim_id: str,
        *, mutation_row: sqlite3.Row | None = None,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM kis_functional_transport_dispatch WHERE claim_id=?",
            (claim_id,),
        ).fetchone()
        if row is None:
            raise KisDomesticFunctionalTransportBlocked("transport dispatch is absent")
        request = json.loads(row["request_json"])
        request_hash, request_signature = self._signed(request)
        if (
            request_hash != row["request_hash"]
            or not hmac.compare_digest(request_signature, row["request_signature"])
            or row["session_id"] != request.get("sessionId")
            or row["operation"] != request.get("operation")
            or int(row["authority_revision"]) != request.get("authorityRevision")
            or row["sender_kind"] != self._sender_kind
            or row["sender_owner_hash"] != self._sender_owner_hash
            or row["sender_code_hash"] != self._sender_code_hash
            or row["sender_credential_configuration_hash"]
            != self._sender_credential_configuration_hash
        ):
            raise KisDomesticFunctionalTransportBlocked("transport request archive failed integrity")
        if mutation_row is None:
            mutation_row = conn.execute(
                "SELECT * FROM kis_mutation_request WHERE claim_id=?", (claim_id,)
            ).fetchone()
        if mutation_row is None:
            raise KisDomesticFunctionalTransportBlocked(
                "transport mutation row is absent"
            )
        try:
            self.mutation._verify_request_locked(conn, mutation_row)
        except KisDomesticFunctionalMutationBlocked as exc:
            raise KisDomesticFunctionalTransportBlocked(
                "transport mutation row failed integrity"
            ) from exc
        mutation = dict(mutation_row)
        created = _parse_utc(row["created_at"], "transport createdAt")
        if created < _parse_utc(mutation["created_at"], "mutation createdAt"):
            raise KisDomesticFunctionalTransportBlocked(
                "transport/mutation creation time lineage failed"
            )
        if row["state"] == "RESPONSE_ARCHIVED":
            response = json.loads(row["response_json"])
            response_hash, response_signature = self._signed(response)
            if (
                response_hash != row["response_hash"]
                or not hmac.compare_digest(response_signature, row["response_signature"])
            ):
                raise KisDomesticFunctionalTransportBlocked(
                    "transport response archive failed integrity"
                )
            try:
                verified_response = self._verify_response(response, request)
                ack = self._response_ack(verified_response)
            except KisDomesticFunctionalTransportBlocked:
                verified_response = None
                ack = {
                    "result": "INVALID_RESPONSE", "orderDate": "",
                    "organizationNo": "", "orderNo": "", "exact": False,
                }
            if (
                row["response_ack_json"] != _canonical(ack)
                or row["response_ack_hash"] != _hash(ack)
            ):
                raise KisDomesticFunctionalTransportBlocked(
                    "transport response ACK archive failed integrity"
                )
            if verified_response is not None and not (
                _parse_utc(mutation["sender_entered_at"], "mutation senderEnteredAt")
                <= _parse_utc(mutation["post_marker_at"], "mutation postMarkerAt")
                <= _parse_utc(row["pre_post_at"], "transport prePostAt")
                <= _parse_utc(verified_response["observedAt"], "response observedAt")
                <= _parse_utc(row["response_at"], "transport responseAt")
            ):
                raise KisDomesticFunctionalTransportBlocked(
                    "transport response time lineage failed"
                )
        elif any(row[key] for key in (
            "response_json", "response_hash", "response_signature",
            "response_ack_json", "response_ack_hash", "response_at",
        )):
            raise KisDomesticFunctionalTransportBlocked("transport premature response archive")
        if row["state"] in {"PRE_POST", "RESPONSE_ARCHIVED"} and not row["pre_post_at"]:
            raise KisDomesticFunctionalTransportBlocked("transport pre-POST marker is absent")
        return dict(row)

    def read(self, claim_id: str) -> dict[str, Any]:
        if type(claim_id) is not str or not _ID.fullmatch(claim_id):
            raise KisDomesticFunctionalTransportBlocked("transport claim id is invalid")
        with self.ledger.connection() as conn:
            conn.execute("BEGIN")
            return self._read_locked(conn, claim_id)

    def authority_snapshot(self) -> dict[str, Any]:
        """Join mutation and transport journals into a fail-closed state reader."""
        # One route-fenced SQLite read transaction is the authority snapshot.
        # No component reader opens a second connection or observes a later DB
        # revision while the orphan union is reduced.
        with kis_route_authority_serialization():
            with self.ledger.connection() as conn:
                conn.execute("BEGIN")
                manifest = conn.execute(
                    "SELECT * FROM kis_functional_transport_schema"
                ).fetchall()
                expected_manifest = (
                    1, _SCHEMA_VERSION, self._schema_hash, self._owner_hash,
                    self._key_id_hash,
                )
                if (
                    _hash(_snapshot(conn)) != self._schema_hash
                    or len(manifest) != 1
                    or tuple(manifest[0]) != expected_manifest
                ):
                    raise KisDomesticFunctionalTransportBlocked(
                        "transport snapshot schema/owner projection mismatch"
                    )
                mutation_rows = conn.execute(
                    "SELECT * FROM kis_mutation_request ORDER BY claim_id"
                ).fetchall()
                mutations: dict[str, dict[str, Any]] = {}
                for row in mutation_rows:
                    try:
                        self.mutation._verify_request_locked(conn, row)
                    except KisDomesticFunctionalMutationBlocked as exc:
                        raise KisDomesticFunctionalTransportBlocked(
                            "mutation snapshot failed integrity"
                        ) from exc
                    mutations[str(row["claim_id"])] = dict(row)
                transport_rows = conn.execute(
                    "SELECT * FROM kis_functional_transport_dispatch ORDER BY claim_id"
                ).fetchall()
                transport_ids = {str(row["claim_id"]) for row in transport_rows}
                dispatches: dict[str, dict[str, Any]] = {}
                orphan_transport_ids = transport_ids - set(mutations)
                for claim in sorted(transport_ids - orphan_transport_ids):
                    dispatches[claim] = self._read_locked(
                        conn, claim, mutation_row=next(
                            row for row in mutation_rows
                            if str(row["claim_id"]) == claim
                        ),
                    )
                capability = conn.execute(
                    "SELECT * FROM kis_capability_authority WHERE route=?",
                    (ROUTE,),
                ).fetchone()
                if capability is None:
                    raise KisDomesticFunctionalTransportBlocked(
                        "durable capability/account binding is absent"
                    )
                self.capability._verify(conn, capability)
                capability = dict(capability)
        hazards: set[str] = set()
        if orphan_transport_ids:
            hazards.add("TRANSPORT_REQUEST_WITHOUT_MUTATION")
        all_ids = set(mutations) | set(dispatches)
        for claim in all_ids:
            mutation = mutations.get(claim); dispatch = dispatches.get(claim)
            if mutation is None:
                hazards.add("TRANSPORT_REQUEST_WITHOUT_MUTATION")
                continue
            state = str(mutation["state"])
            if dispatch is None:
                if state == "SENDER_ENTERED":
                    hazards.add("MUTATION_SENDER_ENTERED_WITHOUT_TRANSPORT")
                elif state not in {"SEALED", "NOT_SENT"}:
                    hazards.add("MUTATION_POST_MARKER_WITHOUT_TRANSPORT")
                continue
            dispatch_state = str(dispatch["state"])
            if state == "SEALED" and dispatch_state != "PREPARED":
                hazards.add("TRANSPORT_STATE_PRECEDES_MUTATION")
            if state == "SENDER_ENTERED" and dispatch_state != "PREPARED":
                hazards.add("TRANSPORT_PRE_POST_WITHOUT_MUTATION_MARKER")
            if state not in {"SEALED", "SENDER_ENTERED", "NOT_SENT"} and dispatch_state == "PREPARED":
                hazards.add("MUTATION_MARKER_WITHOUT_TRANSPORT_PRE_POST")
            if state not in {"SEALED", "SENDER_ENTERED", "NOT_SENT"} and dispatch_state == "PRE_POST":
                hazards.add("POST_MAY_HAVE_CROSSED_WITHOUT_RESPONSE")
            if state == "NOT_SENT" and dispatch_state != "PREPARED":
                hazards.add("NOT_SENT_HAS_PHYSICAL_ATTEMPT_MARKER")
            if dispatch_state == "RESPONSE_ARCHIVED":
                response = json.loads(dispatch["response_json"])
                ack = json.loads(dispatch["response_ack_json"])
                try:
                    self._verify_response(response, json.loads(dispatch["request_json"]))
                except KisDomesticFunctionalTransportBlocked:
                    hazards.add("TRANSPORT_RESPONSE_SCHEMA_INVALID")
                official_key = {
                    "orderDate": str(mutation["ack_order_date"]),
                    "organizationNo": str(mutation["ack_organization_no"]),
                    "orderNo": str(mutation["ack_order_no"]),
                }
                if ack["result"] == "ACK_INCOMPLETE":
                    hazards.add("POST_ACK_IDENTITY_INCOMPLETE")
                elif ack["result"] == "INVALID_RESPONSE" or ack["result"] == "UNPROVEN":
                    hazards.add("POST_RESPONSE_UNPROVEN")
                elif ack["result"] == "REJECTED" and state != "REJECTED":
                    hazards.add("POST_REJECTION_NOT_OFFICIALLY_RECONCILED")
                elif ack["exact"] and state not in {
                    "ACKNOWLEDGED", "PARTIAL", "FILLED", "CANCEL_PENDING",
                    "CANCELED", "REJECTED",
                }:
                    hazards.add("POST_ACK_NOT_OFFICIALLY_RECONCILED")
                elif ack["exact"] and any(official_key.values()) and official_key != {
                    key: ack[key] for key in official_key
                }:
                    hazards.add("POST_ACK_OFFICIAL_TRUTH_MISMATCH")
                elif ack["exact"] and state == "UNKNOWN":
                    hazards.add("POST_ACK_CONTRADICTS_UNKNOWN")
                if response["requestHash"] != dispatch["request_hash"]:
                    hazards.add("PHYSICAL_TRACE_REQUEST_HASH_MISMATCH")
        sessions = {
            str(row["session_id"]) for row in mutations.values()
            if str(row["state"]) not in _TERMINAL_MUTATION_STATES
        }
        if len(sessions) > 1:
            hazards.add("MULTIPLE_FUNCTIONAL_TRANSPORT_SESSIONS")
        session = next(iter(sessions), str(capability["session_id"]))
        active = any(
            str(row["state"]) not in _TERMINAL_MUTATION_STATES
            for row in mutations.values()
        )
        return {
            "schemaVersion": "kis-domestic-functional-component-status/v1",
            "component": "transport", "ownerHash": self._owner_hash,
            "route": ROUTE, "readable": True, "sessionId": session,
            "accountFingerprint": str(capability["account_fingerprint"]),
            "credentialConfigurationHash": str(
                capability["credential_configuration_hash"]
            ),
            "hazards": sorted(hazards), "functionalMutationIntent": {},
            "killOrdinaryCancelAllowed": False,
            "killOrdinaryCancelRevision": 0,
            "killOrdinaryCancelIntent": {}, "productionAvailable": False,
        }

    def integration_status(self) -> dict[str, Any]:
        snapshot = self.authority_snapshot()
        hazards = list(snapshot["hazards"])
        physical_trace_owned = False
        if self._sender_kind == "OWNED_PRODUCTION_DISABLED":
            with kis_route_authority_serialization():
                with self.ledger.connection() as conn:
                    conn.execute("BEGIN")
                    rows = conn.execute(
                        "SELECT * FROM kis_functional_transport_dispatch ORDER BY claim_id"
                    ).fetchall()
                    physical_trace_owned = bool(rows)
                    for row in rows:
                        if row["state"] != "RESPONSE_ARCHIVED":
                            physical_trace_owned = False
                            break
                        verified = self._read_locked(conn, str(row["claim_id"]))
                        request = json.loads(verified["request_json"])
                        response = json.loads(verified["response_json"])
                        try:
                            response = self._verify_response(response, request)
                        except KisDomesticFunctionalTransportBlocked:
                            physical_trace_owned = False
                            break
                        if response.get("physicalTraceOwned") is not True:
                            physical_trace_owned = False
                            break
        return {
            **production_entrypoint_status(),
            "transportAuthorityOpen": bool(snapshot["sessionId"]) or bool(hazards),
            "orphanUnionComplete": True,
            "physicalTraceOwned": physical_trace_owned,
            "officialAckJoinComplete": not any(
                item.startswith("POST_") for item in hazards
            ),
            "hazards": hazards,
        }


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False,
        "networkAvailable": False,
        "senderAvailable": False,
        "productionSenderAvailable": False,
        "releaseEvidenceAvailable": False,
        "route": ROUTE,
        "pdno": PDNO,
        "reason": "DISABLED_MOCK_ONLY_TRANSPORT_NO_PRODUCTION_WIRING",
    }


__all__ = [
    "DurableKisDomesticFunctionalTransport",
    "KIS_DOMESTIC_FUNCTIONAL_TRANSPORT_AVAILABLE",
    "KisDomesticFunctionalTransportBlocked",
    "production_entrypoint_status",
]
