from __future__ import annotations

"""Disabled verify-only production seam for public KIS H0STCNT0 ingress.

This module owns no HTTP/WebSocket executor, credential, token, account read,
or broker mutation surface.  A future reviewed socket owner may present signed
LIVE handshake and raw-frame records here.  The pump persists an intent before
calling a durable raw-ingress writer and will invoke the reducer only after the
writer's signed ACK is independently verified.

H0STCNT0 has no exchange-native packet sequence in the repository contract.
The local ingress ordinal therefore detects only gaps at this seam and is never
reported as upstream completeness.  No official KIS minute-candle GET adapter
exists in this repository, so dual-source bar confirmation remains an explicit
release blocker.
"""

from datetime import datetime, timedelta, timezone
import base64
import binascii
import hashlib
import hmac
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any, Callable, Mapping

from .program_ledger import ProgramLedger


ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
TR_ID = "H0STCNT0"
LIVE_ORIGIN = "https://openapi.koreainvestment.com:9443"
APPROVAL_ENDPOINT = "/oauth2/Approval"
LIVE_WEBSOCKET_URL = "ws://ops.koreainvestment.com:21000/tryitout"
KIS_DOMESTIC_TRADE_FIELD_COUNT = 46

KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_NETWORK_EXECUTOR_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_RELEASE_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_UPSTREAM_COMPLETENESS_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_DUAL_SOURCE_CONFIRMATION_AVAILABLE = False

HANDSHAKE_SCHEMA = "kis-domestic-functional-market-source-handshake/v1"
OWNER_SCHEMA = "kis-domestic-functional-market-source-owner-epoch/v1"
RAW_RECORD_SCHEMA = "kis-domestic-functional-market-source-raw-frame/v1"
ACK_SCHEMA = "kis-domestic-functional-market-source-durable-ack/v2"
REDUCER_SCHEMA = "kis-domestic-functional-market-source-reducer-receipt/v2"
STATUS_SCHEMA = "kis-domestic-functional-market-source-status/v1"
SCHEMA_VERSION = "kis-domestic-functional-market-source-sqlite/v2"
TRANSITION_SCHEMA = "kis-domestic-functional-market-source-transition/v1"
MARKET_SOURCE_AUTHORITY_PURPOSE = "MARKET_SOURCE_RECORD_VERIFY"
SOURCE_ACK_AUTHORITY_PURPOSE = "SOURCE_RECORD_VERIFY"

_GENERATION = re.compile(r"^kis-ws-generation-[0-9a-f]{32}$", re.ASCII)
_PROCESS_GENERATION = re.compile(
    r"^kis-market-source-process-[0-9a-f]{32}$", re.ASCII
)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,159}$", re.ASCII)
_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_DATE = re.compile(r"^[0-9]{8}$", re.ASCII)
_TIME = re.compile(r"^[0-9]{6}$", re.ASCII)

_HANDSHAKE_KEYS = {
    "schemaVersion", "route", "pdno", "trId", "approvalOrigin",
    "approvalEndpoint", "websocketUrl", "subscriptionBodyHash",
    "sessionId", "accountFingerprint", "ownerEpoch", "ownerEpochId",
    "ownerEpochHash", "processGeneration", "sourceGeneration",
    "socketIdentityHash", "appKeyIdHash", "approvalKeyHash",
    "connectedAt", "subscriptionAckAt", "ackRtCd", "ackTrId",
    "ackTrKey", "publicMarketDataOnly", "privateStreamConfigured",
    "accountAuthorityAvailable", "mutationAuthorityAvailable",
    "networkExecutorAvailable", "productionAvailable", "authorityKeyIdHash",
    "authorityPurpose", "handshakeHash", "signature",
}
_OWNER_KEYS = {
    "schemaVersion", "route", "sessionId", "accountFingerprint",
    "ownerEpoch", "ownerEpochId", "ownerEpochHash", "processGeneration",
    "statusRevision", "statusHeadHash", "observedAt", "authorityFresh",
    "hazardousAuthorityOpen", "productionAvailable", "snapshotHash",
    "signature",
}
_RAW_KEYS = {
    "schemaVersion", "route", "pdno", "trId", "sessionId",
    "accountFingerprint", "ownerEpoch", "ownerEpochId", "ownerEpochHash",
    "processGeneration", "sourceGeneration", "socketIdentityHash",
    "ingressOrdinal", "receivedAt", "rawFrame", "rawFrameHash",
    "recordCount", "previousIngressHeadHash", "authorityKeyIdHash",
    "upstreamExchangeSequenceAvailable", "upstreamPacketCompletenessAttested",
    "productionAvailable", "authorityPurpose", "recordHash", "signature",
}
_ACK_KEYS = {
    "schemaVersion", "route", "pdno", "sessionId", "ownerEpochHash",
    "sourceGeneration", "ingressOrdinal", "rawFrameHash", "rawRecordHash",
    "previousIngressHeadHash", "durableRecordHash", "durableHeadHash",
    "ackedAt", "authorityKeyIdHash", "authorityPurpose",
    "sourceArmId", "sourceFrameIndex", "firstSourceSequence",
    "lastSourceSequence", "sourceFrameEnvelopeHash", "sourceFrameHeadHash",
    "sourceArmTransitionHeadHash", "productionAvailable", "ackHash", "signature",
}
_REDUCER_KEYS = {
    "schemaVersion", "route", "pdno", "sessionId", "sourceGeneration",
    "ingressOrdinal", "rawRecordHash", "durableRecordHash",
    "durableHeadHash", "reducerState", "closedBarCount",
    "nextOpenObserved", "reducedAt", "authorityKeyIdHash",
    "authorityPurpose", "productionAvailable", "receiptHash", "signature",
}
_TRANSITION_KEYS = {
    "schemaVersion", "route", "pdno", "sourceGeneration",
    "ingressOrdinal", "sequence", "revision", "transitionKind",
    "fromState", "toState", "occurredAt", "reason", "anchorHash",
    "previousHash", "authorityKeyIdHash", "authorityPurpose",
}

_SQL = """
CREATE TABLE kis_functional_market_source_manifest (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL
);
CREATE TABLE kis_functional_market_source_generation (
    source_generation TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    owner_epoch INTEGER NOT NULL CHECK(owner_epoch>=1),
    owner_epoch_id TEXT NOT NULL,
    owner_epoch_hash TEXT NOT NULL,
    process_generation TEXT NOT NULL,
    socket_identity_hash TEXT NOT NULL UNIQUE,
    authority_key_id_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'ARMED_WAIT_PUBLIC','SAFE_INCOMPLETE','STOPPED'
    )),
    handshake_json TEXT NOT NULL,
    handshake_hash TEXT NOT NULL UNIQUE,
    handshake_signature TEXT NOT NULL,
    connected_at TEXT NOT NULL,
    terminal_at TEXT NOT NULL DEFAULT '',
    failure_reason TEXT NOT NULL DEFAULT '',
    last_ingress_ordinal INTEGER NOT NULL CHECK(last_ingress_ordinal>=0),
    ingress_head_hash TEXT NOT NULL,
    reconnect_predecessor_generation TEXT NOT NULL DEFAULT '',
    transition_count INTEGER NOT NULL DEFAULT 0 CHECK(transition_count>=0),
    transition_head_hash TEXT NOT NULL DEFAULT
        '0000000000000000000000000000000000000000000000000000000000000000',
    revision INTEGER NOT NULL CHECK(revision>=0)
);
CREATE UNIQUE INDEX kis_functional_market_source_active_idx
    ON kis_functional_market_source_generation(state)
    WHERE state='ARMED_WAIT_PUBLIC';
CREATE TABLE kis_functional_market_source_ingress (
    source_generation TEXT NOT NULL,
    ingress_ordinal INTEGER NOT NULL CHECK(ingress_ordinal>=1),
    state TEXT NOT NULL CHECK(state IN ('INTENT','ACKED','REDUCED')),
    raw_record_json TEXT NOT NULL,
    raw_record_hash TEXT NOT NULL UNIQUE,
    raw_record_signature TEXT NOT NULL,
    parsed_records_json TEXT NOT NULL,
    parsed_records_hash TEXT NOT NULL,
    previous_head_hash TEXT NOT NULL,
    durable_record_hash TEXT NOT NULL DEFAULT '',
    durable_head_hash TEXT NOT NULL DEFAULT '',
    ack_json TEXT NOT NULL DEFAULT '',
    ack_hash TEXT NOT NULL DEFAULT '',
    ack_authority_key_id_hash TEXT NOT NULL DEFAULT '',
    reducer_receipt_json TEXT NOT NULL DEFAULT '',
    reducer_receipt_hash TEXT NOT NULL DEFAULT '',
    reducer_authority_key_id_hash TEXT NOT NULL DEFAULT '',
    transition_count INTEGER NOT NULL DEFAULT 0 CHECK(transition_count>=0),
    transition_head_hash TEXT NOT NULL DEFAULT
        '0000000000000000000000000000000000000000000000000000000000000000',
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
    PRIMARY KEY(source_generation, ingress_ordinal),
    FOREIGN KEY(source_generation)
        REFERENCES kis_functional_market_source_generation(source_generation)
);
CREATE TABLE kis_functional_market_source_transition (
    source_generation TEXT NOT NULL,
    ingress_ordinal INTEGER NOT NULL CHECK(ingress_ordinal>=0),
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    revision INTEGER NOT NULL CHECK(revision>=0),
    transition_kind TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    anchor_hash TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    record_json TEXT NOT NULL,
    record_hash TEXT NOT NULL UNIQUE,
    signature TEXT NOT NULL,
    authority_key_id_hash TEXT NOT NULL,
    PRIMARY KEY(source_generation,ingress_ordinal,sequence)
);
CREATE TRIGGER kis_functional_market_source_transition_update_forbidden
BEFORE UPDATE ON kis_functional_market_source_transition
BEGIN SELECT RAISE(ABORT,'kis-market-source-transition-immutable'); END;
CREATE TRIGGER kis_functional_market_source_transition_delete_forbidden
BEFORE DELETE ON kis_functional_market_source_transition
BEGIN SELECT RAISE(ABORT,'kis-market-source-transition-immutable'); END;
CREATE TRIGGER kis_functional_market_source_transition_insert_guard
BEFORE INSERT ON kis_functional_market_source_transition
BEGIN
    SELECT CASE WHEN NOT (
        (NEW.ingress_ordinal=0 AND EXISTS (
            SELECT 1 FROM kis_functional_market_source_generation g
            WHERE g.source_generation=NEW.source_generation
              AND NEW.sequence=g.transition_count+1
              AND NEW.revision=g.revision+1
              AND NEW.from_state=g.state
              AND NEW.previous_hash=g.transition_head_hash
        )) OR
        (NEW.ingress_ordinal>0 AND EXISTS (
            SELECT 1 FROM kis_functional_market_source_ingress i
            WHERE i.source_generation=NEW.source_generation
              AND i.ingress_ordinal=NEW.ingress_ordinal
              AND NEW.sequence=i.transition_count+1
              AND NEW.revision=i.revision+1
              AND NEW.from_state=i.state
              AND NEW.previous_hash=i.transition_head_hash
        ))
    ) THEN RAISE(ABORT,'kis-market-source-transition-order-invalid') END;
END;
CREATE TRIGGER kis_functional_market_source_generation_identity_immutable
BEFORE UPDATE ON kis_functional_market_source_generation
WHEN NEW.source_generation<>OLD.source_generation
  OR NEW.session_id<>OLD.session_id
  OR NEW.account_fingerprint<>OLD.account_fingerprint
  OR NEW.owner_epoch<>OLD.owner_epoch
  OR NEW.owner_epoch_id<>OLD.owner_epoch_id
  OR NEW.owner_epoch_hash<>OLD.owner_epoch_hash
  OR NEW.process_generation<>OLD.process_generation
  OR NEW.socket_identity_hash<>OLD.socket_identity_hash
  OR NEW.authority_key_id_hash<>OLD.authority_key_id_hash
  OR NEW.handshake_json<>OLD.handshake_json
  OR NEW.handshake_hash<>OLD.handshake_hash
  OR NEW.handshake_signature<>OLD.handshake_signature
  OR NEW.connected_at<>OLD.connected_at
  OR NEW.reconnect_predecessor_generation<>OLD.reconnect_predecessor_generation
BEGIN SELECT RAISE(ABORT,'kis-market-source-generation-identity-immutable'); END;
CREATE TRIGGER kis_functional_market_source_ingress_identity_immutable
BEFORE UPDATE ON kis_functional_market_source_ingress
WHEN NEW.source_generation<>OLD.source_generation
  OR NEW.ingress_ordinal<>OLD.ingress_ordinal
  OR NEW.raw_record_json<>OLD.raw_record_json
  OR NEW.raw_record_hash<>OLD.raw_record_hash
  OR NEW.raw_record_signature<>OLD.raw_record_signature
  OR NEW.parsed_records_json<>OLD.parsed_records_json
  OR NEW.parsed_records_hash<>OLD.parsed_records_hash
  OR NEW.previous_head_hash<>OLD.previous_head_hash
BEGIN SELECT RAISE(ABORT,'kis-market-source-ingress-identity-immutable'); END;
CREATE TRIGGER kis_functional_market_source_ingress_projection_guard
BEFORE UPDATE OF state,transition_count,transition_head_hash,revision
ON kis_functional_market_source_ingress
WHEN NEW.state<>OLD.state OR NEW.transition_count<>OLD.transition_count
     OR NEW.transition_head_hash<>OLD.transition_head_hash
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM kis_functional_market_source_transition t
        WHERE t.source_generation=OLD.source_generation
          AND t.ingress_ordinal=OLD.ingress_ordinal
          AND t.sequence=OLD.transition_count+1
          AND t.revision=NEW.revision
          AND t.from_state=OLD.state AND t.to_state=NEW.state
          AND t.record_hash=NEW.transition_head_hash
          AND NEW.transition_count=OLD.transition_count+1
    ) THEN RAISE(ABORT,'kis-market-source-ingress-transition-missing') END;
END;
CREATE TRIGGER kis_functional_market_source_generation_projection_guard
BEFORE UPDATE OF state,transition_count,transition_head_hash,revision
ON kis_functional_market_source_generation
WHEN NEW.state<>OLD.state OR NEW.transition_count<>OLD.transition_count
     OR NEW.transition_head_hash<>OLD.transition_head_hash
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM kis_functional_market_source_transition t
        WHERE t.source_generation=OLD.source_generation
          AND t.ingress_ordinal=0
          AND t.sequence=OLD.transition_count+1
          AND t.revision=NEW.revision
          AND t.from_state=OLD.state AND t.to_state=NEW.state
          AND t.record_hash=NEW.transition_head_hash
          AND NEW.transition_count=OLD.transition_count+1
    ) THEN RAISE(ABORT,'kis-market-source-generation-transition-missing') END;
END;
"""


class KisDomesticFunctionalMarketSourceBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalMarketSourceBlocked(
            "market-source-evidence-not-canonical"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalMarketSourceBlocked(f"{label}-invalid")
    return value


def _signature_text(value: Any) -> bool:
    """Validate frozen legacy-HMAC or canonical Ed25519 encoding only."""

    if type(value) is not str:
        return False
    if _SHA.fullmatch(value):
        return True
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return False
    return len(decoded) == 64 and base64.b64encode(decoded).decode("ascii") == value


def _identity(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalMarketSourceBlocked(f"{label}-invalid")
    return value


def _utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalMarketSourceBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise KisDomesticFunctionalMarketSourceBlocked(f"{label}-invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise KisDomesticFunctionalMarketSourceBlocked(f"{label}-not-aware")
    return parsed.astimezone(timezone.utc)


def _time_text(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalMarketSourceBlocked("trusted-time-invalid")
    return value.astimezone(timezone.utc).isoformat()


def _subscription_body_hash() -> str:
    return _hash(
        {
            "header": {
                "content-type": "utf-8",
                "custtype": "P",
                "tr_type": "1",
            },
            "body": {"input": {"tr_id": TR_ID, "tr_key": PDNO}},
        }
    )


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = {
        str(row[0]): _normalize_sql(row[1])
        for row in conn.execute(
            "SELECT name,sql FROM sqlite_master "
            "WHERE name LIKE 'kis_functional_market_source_%' ORDER BY name"
        )
    }
    tables: dict[str, Any] = {}
    for name in sorted(
        key for key, sql in objects.items() if sql.startswith("CREATE TABLE")
    ):
        indexes = [
            tuple(row) for row in conn.execute(f'PRAGMA index_list("{name}")')
        ]
        tables[name] = {
            "tableInfo": [
                tuple(row) for row in conn.execute(f'PRAGMA table_info("{name}")')
            ],
            "foreignKeys": [
                tuple(row)
                for row in conn.execute(f'PRAGMA foreign_key_list("{name}")')
            ],
            "indexes": indexes,
        }
        for index in indexes:
            tables[name][f"indexXInfo:{index[1]}"] = [
                tuple(row)
                for row in conn.execute(f'PRAGMA index_xinfo("{index[1]}")')
            ]
    return {"objects": objects, "tables": tables}


def _expected_schema_snapshot() -> dict[str, Any]:
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(_SQL)
        return _schema_snapshot(conn)
    finally:
        conn.close()


_EXPECTED_SCHEMA = _expected_schema_snapshot()
SCHEMA_FINGERPRINT = _hash(
    {"schemaVersion": SCHEMA_VERSION, "schema": _EXPECTED_SCHEMA}
)


def _verify_schema(conn: sqlite3.Connection) -> None:
    if _schema_snapshot(conn) != _EXPECTED_SCHEMA:
        raise KisDomesticFunctionalMarketSourceBlocked(
            "market-source-schema-dirty"
        )
    rows = [
        tuple(row)
        for row in conn.execute(
            "SELECT singleton,schema_version,schema_fingerprint "
            "FROM kis_functional_market_source_manifest"
        )
    ]
    if rows != [(1, SCHEMA_VERSION, SCHEMA_FINGERPRINT)]:
        raise KisDomesticFunctionalMarketSourceBlocked(
            "market-source-schema-manifest-dirty"
        )


def _verified_envelope(
    raw: Mapping[str, Any],
    *,
    keys: set[str],
    hash_key: str,
    verifier: Callable[[Mapping[str, Any]], bool],
    label: str,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != keys:
        raise KisDomesticFunctionalMarketSourceBlocked(f"{label}-not-exact")
    value = dict(raw)
    if not _signature_text(value.get("signature")):
        raise KisDomesticFunctionalMarketSourceBlocked(
            f"{label}-signature-invalid"
        )
    digest = value.get(hash_key)
    _sha(digest, f"{label}-{hash_key}")
    unsigned = dict(value)
    unsigned.pop("signature")
    unsigned.pop(hash_key)
    if not hmac.compare_digest(digest, _hash(unsigned)):
        raise KisDomesticFunctionalMarketSourceBlocked(f"{label}-hash-mismatch")
    try:
        valid = verifier(dict(value))
    except Exception:
        valid = False
    if valid is not True:
        raise KisDomesticFunctionalMarketSourceBlocked(
            f"{label}-signature-unverified"
        )
    return value


class DisabledKisDomesticFunctionalMarketSource:
    """Verify-only socket-generation/ingress coordinator; never a network owner."""

    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        owner_epoch_reader: Callable[[], Mapping[str, Any]],
        owner_epoch_verifier: Callable[[Mapping[str, Any]], bool],
        handshake_verifier: Callable[[Mapping[str, Any]], bool],
        raw_record_verifier: Callable[[Mapping[str, Any]], bool],
        durable_ingress_writer: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        durable_ack_verifier: Callable[[Mapping[str, Any]], bool],
        reducer: Callable[
            [str, Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]
        ],
        reducer_receipt_verifier: Callable[[Mapping[str, Any]], bool],
        transition_signer: Callable[[str, Mapping[str, Any]], str],
        transition_verifier: Callable[
            [str, Mapping[str, Any], str, str], bool
        ],
        transition_authority_key_id_hash: str,
        trusted_clock: Callable[[], datetime],
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "exact-program-ledger-required"
            )
        callables = (
            owner_epoch_reader,
            owner_epoch_verifier,
            handshake_verifier,
            raw_record_verifier,
            durable_ingress_writer,
            durable_ack_verifier,
            reducer,
            reducer_receipt_verifier,
            transition_signer,
            transition_verifier,
            trusted_clock,
        )
        if not all(callable(item) for item in callables):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-port-not-callable"
            )
        self.ledger = program_ledger
        self.owner_epoch_reader = owner_epoch_reader
        self.owner_epoch_verifier = owner_epoch_verifier
        self.handshake_verifier = handshake_verifier
        self.raw_record_verifier = raw_record_verifier
        self.durable_ingress_writer = durable_ingress_writer
        self.durable_ack_verifier = durable_ack_verifier
        self.reducer = reducer
        self.reducer_receipt_verifier = reducer_receipt_verifier
        self.transition_signer = transition_signer
        self.transition_verifier = transition_verifier
        self.transition_authority_key_id_hash = _sha(
            transition_authority_key_id_hash,
            "transition-authority-key-id-hash",
        )
        self.trusted_clock = trusted_clock
        self._lock = threading.RLock()
        self._initialize()
        self.startup_terminalized_generations = self._startup_audit()

    def __repr__(self) -> str:
        return (
            "DisabledKisDomesticFunctionalMarketSource("
            "route='KIS_KR_LIVE_CONTINUOUS',trId='H0STCNT0',"
            "networkExecutor=False,production=False)"
        )

    def _connect(self) -> sqlite3.Connection:
        conn = self.ledger.connect()
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialize(self) -> None:
        conn = self._connect()
        try:
            existing = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE name LIKE 'kis_functional_market_source_%'"
            ).fetchall()
            if not existing:
                conn.executescript(_SQL)
                conn.execute(
                    "INSERT INTO kis_functional_market_source_manifest "
                    "VALUES(1,?,?)",
                    (SCHEMA_VERSION, SCHEMA_FINGERPRINT),
                )
                conn.commit()
            _verify_schema(conn)
        finally:
            conn.close()

    def _trusted_now(self) -> datetime:
        value = self.trusted_clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-trusted-clock-invalid"
            )
        return value.astimezone(timezone.utc)

    def _append_transition(
        self,
        conn: sqlite3.Connection,
        *,
        source_generation: str,
        ingress_ordinal: int,
        transition_kind: str,
        from_state: str,
        to_state: str,
        occurred_at: str,
        reason: str,
        anchor_hash: str,
    ) -> tuple[int, int, str]:
        if type(transition_kind) is not str or not _ID.fullmatch(transition_kind):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-transition-kind-invalid"
            )
        if type(reason) is not str or not reason or len(reason) > 160:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-transition-reason-invalid"
            )
        _utc(occurred_at, "market-source-transition-time")
        _sha(anchor_hash, "market-source-transition-anchor")
        if ingress_ordinal == 0:
            row = conn.execute(
                "SELECT state,transition_count,transition_head_hash,revision "
                "FROM kis_functional_market_source_generation "
                "WHERE source_generation=?",
                (source_generation,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT state,transition_count,transition_head_hash,revision "
                "FROM kis_functional_market_source_ingress WHERE "
                "source_generation=? AND ingress_ordinal=?",
                (source_generation, ingress_ordinal),
            ).fetchone()
        if row is None or str(row["state"]) != from_state:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-transition-source-state-mismatch"
            )
        sequence = int(row["transition_count"]) + 1
        revision = int(row["revision"]) + 1
        previous_hash = str(row["transition_head_hash"])
        body = {
            "schemaVersion": TRANSITION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "sourceGeneration": source_generation,
            "ingressOrdinal": ingress_ordinal,
            "sequence": sequence,
            "revision": revision,
            "transitionKind": transition_kind,
            "fromState": from_state,
            "toState": to_state,
            "occurredAt": occurred_at,
            "reason": reason,
            "anchorHash": anchor_hash,
            "previousHash": previous_hash,
            "authorityKeyIdHash": self.transition_authority_key_id_hash,
            "authorityPurpose": MARKET_SOURCE_AUTHORITY_PURPOSE,
        }
        record_hash = _hash(body)
        try:
            signature = self.transition_signer(
                "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_TRANSITION",
                {**body, "recordHash": record_hash},
            )
        except Exception as exc:
            raise KisDomesticFunctionalMarketSourceBlocked(
                f"market-source-transition-sign-failed:{type(exc).__name__}"
            ) from None
        if not _signature_text(signature):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-transition-signature-invalid"
            )
        try:
            verified = self.transition_verifier(
                "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_TRANSITION",
                {**body, "recordHash": record_hash},
                signature,
                self.transition_authority_key_id_hash,
            )
        except Exception:
            verified = False
        if verified is not True:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-transition-signature-unverified"
            )
        conn.execute(
            "INSERT INTO kis_functional_market_source_transition "
            "(source_generation,ingress_ordinal,sequence,revision,"
            "transition_kind,from_state,to_state,occurred_at,reason,anchor_hash,"
            "previous_hash,record_json,record_hash,signature,"
            "authority_key_id_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                source_generation, ingress_ordinal, sequence, revision,
                transition_kind, from_state, to_state, occurred_at, reason,
                anchor_hash, previous_hash, _canonical(body), record_hash,
                signature, self.transition_authority_key_id_hash,
            ),
        )
        return sequence, revision, record_hash

    def _verify_transition_chain(
        self,
        conn: sqlite3.Connection,
        *,
        source_generation: str,
        ingress_ordinal: int,
        expected_state: str,
        expected_count: int,
        expected_head: str,
        expected_revision: int,
    ) -> None:
        rows = conn.execute(
            "SELECT * FROM kis_functional_market_source_transition WHERE "
            "source_generation=? AND ingress_ordinal=? ORDER BY sequence",
            (source_generation, ingress_ordinal),
        ).fetchall()
        if len(rows) != expected_count or expected_revision != expected_count:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-transition-cardinality-mismatch"
            )
        previous = "0" * 64
        state = "ARMED_WAIT_PUBLIC" if ingress_ordinal == 0 else "INTENT"
        for index, row in enumerate(rows, 1):
            try:
                body = json.loads(str(row["record_json"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "market-source-transition-json-invalid"
                ) from None
            if not isinstance(body, dict) or set(body) != _TRANSITION_KEYS:
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "market-source-transition-body-not-exact"
                )
            exact = {
                "schemaVersion": TRANSITION_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "sourceGeneration": source_generation,
                "ingressOrdinal": ingress_ordinal,
                "sequence": index,
                "revision": index,
                "fromState": state,
                "previousHash": previous,
                "authorityKeyIdHash": self.transition_authority_key_id_hash,
                "authorityPurpose": MARKET_SOURCE_AUTHORITY_PURPOSE,
            }
            if any(
                type(body.get(key)) is not type(wanted)
                or body.get(key) != wanted
                for key, wanted in exact.items()
            ):
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "market-source-transition-binding-mismatch"
                )
            record_hash = _hash(body)
            if (
                not hmac.compare_digest(str(row["record_hash"]), record_hash)
                or str(row["signature"]) == ""
                or str(row["authority_key_id_hash"])
                != self.transition_authority_key_id_hash
            ):
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "market-source-transition-row-mismatch"
                )
            try:
                valid = self.transition_verifier(
                    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_TRANSITION",
                    {**body, "recordHash": record_hash},
                    str(row["signature"]),
                    self.transition_authority_key_id_hash,
                )
            except Exception:
                valid = False
            if valid is not True:
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "market-source-transition-signature-unverified"
                )
            state = str(body["toState"])
            previous = record_hash
        if state != expected_state or previous != expected_head:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-transition-terminal-projection-mismatch"
            )

    def _transition_generation(
        self,
        conn: sqlite3.Connection,
        *,
        source_generation: str,
        transition_kind: str,
        to_state: str,
        occurred_at: str,
        reason: str,
        anchor_hash: str,
        terminal_at: str | None = None,
        failure_reason: str | None = None,
        last_ingress_ordinal: int | None = None,
        ingress_head_hash: str | None = None,
    ) -> None:
        row = conn.execute(
            "SELECT * FROM kis_functional_market_source_generation WHERE "
            "source_generation=?", (source_generation,),
        ).fetchone()
        if row is None:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "source-generation-not-found"
            )
        _, revision, head = self._append_transition(
            conn,
            source_generation=source_generation,
            ingress_ordinal=0,
            transition_kind=transition_kind,
            from_state=str(row["state"]),
            to_state=to_state,
            occurred_at=occurred_at,
            reason=reason,
            anchor_hash=anchor_hash,
        )
        updates = [
            "state=?", "transition_count=transition_count+1",
            "transition_head_hash=?", "revision=?",
        ]
        values: list[Any] = [to_state, head, revision]
        if terminal_at is not None:
            updates.append("terminal_at=?"); values.append(terminal_at)
        if failure_reason is not None:
            updates.append("failure_reason=?"); values.append(failure_reason)
        if last_ingress_ordinal is not None:
            updates.append("last_ingress_ordinal=?")
            values.append(last_ingress_ordinal)
        if ingress_head_hash is not None:
            updates.append("ingress_head_hash=?")
            values.append(ingress_head_hash)
        values.extend([source_generation, int(row["revision"])])
        changed = conn.execute(
            "UPDATE kis_functional_market_source_generation SET "
            + ",".join(updates)
            + " WHERE source_generation=? AND revision=?",
            tuple(values),
        ).rowcount
        if changed != 1:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-generation-transition-cas-failed"
            )

    def _transition_ingress(
        self,
        conn: sqlite3.Connection,
        *,
        source_generation: str,
        ingress_ordinal: int,
        transition_kind: str,
        to_state: str,
        occurred_at: str,
        reason: str,
        anchor_hash: str,
        projection: Mapping[str, Any] | None = None,
    ) -> None:
        row = conn.execute(
            "SELECT * FROM kis_functional_market_source_ingress WHERE "
            "source_generation=? AND ingress_ordinal=?",
            (source_generation, ingress_ordinal),
        ).fetchone()
        if row is None:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-ingress-not-found"
            )
        _, revision, head = self._append_transition(
            conn,
            source_generation=source_generation,
            ingress_ordinal=ingress_ordinal,
            transition_kind=transition_kind,
            from_state=str(row["state"]),
            to_state=to_state,
            occurred_at=occurred_at,
            reason=reason,
            anchor_hash=anchor_hash,
        )
        allowed = {
            "durable_record_hash", "durable_head_hash", "ack_json", "ack_hash",
            "ack_authority_key_id_hash", "reducer_receipt_json",
            "reducer_receipt_hash", "reducer_authority_key_id_hash",
        }
        projection = dict(projection or {})
        if not set(projection) <= allowed:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-ingress-projection-field-invalid"
            )
        updates = [
            "state=?", "transition_count=transition_count+1",
            "transition_head_hash=?", "revision=?",
        ]
        values: list[Any] = [to_state, head, revision]
        for key in sorted(projection):
            updates.append(f"{key}=?")
            values.append(projection[key])
        values.extend([source_generation, ingress_ordinal, int(row["revision"])])
        changed = conn.execute(
            "UPDATE kis_functional_market_source_ingress SET "
            + ",".join(updates)
            + " WHERE source_generation=? AND ingress_ordinal=? AND revision=?",
            tuple(values),
        ).rowcount
        if changed != 1:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-ingress-transition-cas-failed"
            )

    def _owner_snapshot(self, expected: Mapping[str, Any]) -> dict[str, Any]:
        try:
            raw = self.owner_epoch_reader()
        except Exception as exc:
            raise KisDomesticFunctionalMarketSourceBlocked(
                f"owner-epoch-read-failed:{type(exc).__name__}"
            ) from None
        value = _verified_envelope(
            raw,
            keys=_OWNER_KEYS,
            hash_key="snapshotHash",
            verifier=self.owner_epoch_verifier,
            label="owner-epoch",
        )
        exact = {
            "schemaVersion": OWNER_SCHEMA,
            "route": ROUTE,
            "sessionId": expected["sessionId"],
            "accountFingerprint": expected["accountFingerprint"],
            "ownerEpoch": expected["ownerEpoch"],
            "ownerEpochId": expected["ownerEpochId"],
            "ownerEpochHash": expected["ownerEpochHash"],
            "processGeneration": expected["processGeneration"],
            "authorityFresh": True,
            "hazardousAuthorityOpen": False,
            "productionAvailable": False,
        }
        if any(
            type(value.get(key)) is not type(wanted) or value.get(key) != wanted
            for key, wanted in exact.items()
        ):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "owner-epoch-binding-or-freshness-mismatch"
            )
        if type(value.get("statusRevision")) is not int or value[
            "statusRevision"
        ] < 1:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "owner-epoch-revision-invalid"
            )
        _sha(value.get("statusHeadHash"), "owner-status-head")
        observed = _utc(value.get("observedAt"), "owner-observed-at")
        now = self._trusted_now()
        if observed > now or (now - observed).total_seconds() > 2:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "owner-epoch-observation-stale"
            )
        return value

    def _handshake(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        value = _verified_envelope(
            raw,
            keys=_HANDSHAKE_KEYS,
            hash_key="handshakeHash",
            verifier=self.handshake_verifier,
            label="market-source-handshake",
        )
        exact = {
            "schemaVersion": HANDSHAKE_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "trId": TR_ID,
            "approvalOrigin": LIVE_ORIGIN,
            "approvalEndpoint": APPROVAL_ENDPOINT,
            "websocketUrl": LIVE_WEBSOCKET_URL,
            "subscriptionBodyHash": _subscription_body_hash(),
            "ackRtCd": "0",
            "ackTrId": TR_ID,
            "ackTrKey": PDNO,
            "publicMarketDataOnly": True,
            "privateStreamConfigured": False,
            "accountAuthorityAvailable": False,
            "mutationAuthorityAvailable": False,
            "networkExecutorAvailable": False,
            "productionAvailable": False,
            "authorityPurpose": MARKET_SOURCE_AUTHORITY_PURPOSE,
        }
        if any(
            type(value.get(key)) is not type(wanted) or value.get(key) != wanted
            for key, wanted in exact.items()
        ):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "market-source-handshake-live-binding-mismatch"
            )
        for key in (
            "accountFingerprint", "ownerEpochHash", "socketIdentityHash",
            "appKeyIdHash", "approvalKeyHash", "authorityKeyIdHash",
        ):
            _sha(value.get(key), f"handshake-{key}")
        _identity(value.get("sessionId"), "handshake-session")
        _identity(value.get("ownerEpochId"), "handshake-owner-epoch")
        if type(value.get("ownerEpoch")) is not int or value["ownerEpoch"] < 1:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "handshake-owner-epoch-invalid"
            )
        if type(value.get("processGeneration")) is not str or not (
            _PROCESS_GENERATION.fullmatch(value["processGeneration"])
        ):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "handshake-process-generation-invalid"
            )
        if type(value.get("sourceGeneration")) is not str or not (
            _GENERATION.fullmatch(value["sourceGeneration"])
        ):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "handshake-source-generation-invalid"
            )
        connected = _utc(value.get("connectedAt"), "handshake-connected-at")
        acked = _utc(value.get("subscriptionAckAt"), "handshake-ack-at")
        now = self._trusted_now()
        if not connected <= acked <= now or (now - acked).total_seconds() > 2:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "handshake-time-lineage-invalid"
            )
        self._owner_snapshot(value)
        return value

    def begin_generation(self, handshake: Mapping[str, Any]) -> dict[str, Any]:
        value = self._handshake(handshake)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                _verify_schema(conn)
                if conn.execute(
                    "SELECT 1 FROM kis_functional_market_source_generation "
                    "WHERE source_generation=?",
                    (value["sourceGeneration"],),
                ).fetchone() is not None:
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "source-generation-reuse-forbidden"
                    )
                active = conn.execute(
                    "SELECT * FROM kis_functional_market_source_generation "
                    "WHERE state='ARMED_WAIT_PUBLIC'",
                ).fetchone()
                predecessor = ""
                if active is not None:
                    predecessor = str(active["source_generation"])
                    if predecessor == value["sourceGeneration"]:
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "reconnect-must-use-new-source-generation"
                        )
                    reconnect_reason = "SOCKET_RECONNECT_GENERATION_CHANGED"
                    invalid_reconnect = ""
                    if str(active["session_id"]) != value["sessionId"]:
                        reconnect_reason = "ACTIVE_SESSION_OWNER_LOSS"
                        invalid_reconnect = "active-route-session-changed"
                    elif str(active["account_fingerprint"]) != value[
                        "accountFingerprint"
                    ]:
                        reconnect_reason = "ACTIVE_ACCOUNT_BINDING_CHANGED"
                        invalid_reconnect = "active-route-account-changed"
                    elif str(active["socket_identity_hash"]) == value[
                        "socketIdentityHash"
                    ]:
                        reconnect_reason = "RECONNECT_SOCKET_IDENTITY_REUSED"
                        invalid_reconnect = "reconnect-must-use-new-socket-identity"
                    self._transition_generation(
                        conn,
                        source_generation=predecessor,
                        transition_kind="GENERATION_RECONNECT_TERMINAL",
                        to_state="SAFE_INCOMPLETE",
                        occurred_at=value["connectedAt"],
                        reason=reconnect_reason,
                        anchor_hash=value["handshakeHash"],
                        terminal_at=value["connectedAt"],
                        failure_reason=reconnect_reason,
                    )
                    if invalid_reconnect:
                        conn.commit()
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            invalid_reconnect
                        )
                conn.execute(
                    "INSERT INTO kis_functional_market_source_generation "
                    "(source_generation,session_id,account_fingerprint,owner_epoch,"
                    "owner_epoch_id,owner_epoch_hash,process_generation,"
                    "socket_identity_hash,authority_key_id_hash,state,"
                    "handshake_json,handshake_hash,"
                    "handshake_signature,connected_at,last_ingress_ordinal,"
                    "ingress_head_hash,reconnect_predecessor_generation,revision) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,0)",
                    (
                        value["sourceGeneration"], value["sessionId"],
                        value["accountFingerprint"], value["ownerEpoch"],
                        value["ownerEpochId"], value["ownerEpochHash"],
                        value["processGeneration"], value["socketIdentityHash"],
                        value["authorityKeyIdHash"], "ARMED_WAIT_PUBLIC",
                        _canonical(value),
                        value["handshakeHash"], value["signature"],
                        value["connectedAt"], "0" * 64, predecessor,
                    ),
                )
                self._transition_generation(
                    conn,
                    source_generation=value["sourceGeneration"],
                    transition_kind="GENERATION_CREATED",
                    to_state="ARMED_WAIT_PUBLIC",
                    occurred_at=value["connectedAt"],
                    reason="SIGNED_LIVE_PUBLIC_GENERATION_ACCEPTED",
                    anchor_hash=value["handshakeHash"],
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return self.snapshot(value["sourceGeneration"])

    @staticmethod
    def _frame(raw: str) -> tuple[int, list[list[str]]]:
        if type(raw) is not str or not raw.startswith(f"0|{TR_ID}|"):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "exact-h0stcnt0-frame-required"
            )
        parts = raw.split("|", 3)
        if len(parts) != 4:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "h0stcnt0-frame-malformed"
            )
        try:
            count = int(parts[2])
        except (TypeError, ValueError):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "h0stcnt0-record-count-invalid"
            ) from None
        if not 1 <= count <= 512:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "h0stcnt0-record-count-invalid"
            )
        fields = parts[3].split("^")
        if len(fields) != count * KIS_DOMESTIC_TRADE_FIELD_COUNT:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "h0stcnt0-record-width-invalid"
            )
        records = [
            fields[index * KIS_DOMESTIC_TRADE_FIELD_COUNT :
                   (index + 1) * KIS_DOMESTIC_TRADE_FIELD_COUNT]
            for index in range(count)
        ]
        for record in records:
            if (
                record[0] != PDNO
                or not _TIME.fullmatch(record[1])
                or not _DATE.fullmatch(record[33])
            ):
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "h0stcnt0-record-route-or-time-invalid"
                )
            try:
                price = float(record[2])
                volume = float(record[12])
            except (TypeError, ValueError):
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "h0stcnt0-price-or-volume-invalid"
                ) from None
            if not math.isfinite(price) or not math.isfinite(volume) or price <= 0 or volume < 0:
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "h0stcnt0-price-or-volume-invalid"
                )
        return count, records

    def _raw_record(
        self,
        raw: Mapping[str, Any],
        row: sqlite3.Row,
    ) -> dict[str, Any]:
        value = _verified_envelope(
            raw,
            keys=_RAW_KEYS,
            hash_key="recordHash",
            verifier=self.raw_record_verifier,
            label="raw-market-frame",
        )
        exact = {
            "schemaVersion": RAW_RECORD_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "trId": TR_ID,
            "sessionId": str(row["session_id"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "ownerEpoch": int(row["owner_epoch"]),
            "ownerEpochId": str(row["owner_epoch_id"]),
            "ownerEpochHash": str(row["owner_epoch_hash"]),
            "processGeneration": str(row["process_generation"]),
            "sourceGeneration": str(row["source_generation"]),
            "socketIdentityHash": str(row["socket_identity_hash"]),
            "authorityKeyIdHash": str(row["authority_key_id_hash"]),
            "ingressOrdinal": int(row["last_ingress_ordinal"]) + 1,
            "previousIngressHeadHash": str(row["ingress_head_hash"]),
            "upstreamExchangeSequenceAvailable": False,
            "upstreamPacketCompletenessAttested": False,
            "productionAvailable": False,
            "authorityPurpose": MARKET_SOURCE_AUTHORITY_PURPOSE,
        }
        if any(
            type(value.get(key)) is not type(wanted) or value.get(key) != wanted
            for key, wanted in exact.items()
        ):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "raw-market-frame-generation-owner-or-ordinal-gap"
            )
        for key in (
            "accountFingerprint", "ownerEpochHash", "socketIdentityHash",
            "rawFrameHash", "previousIngressHeadHash", "authorityKeyIdHash",
        ):
            _sha(value.get(key), f"raw-frame-{key}")
        count, records = self._frame(value.get("rawFrame"))
        if (
            type(value.get("recordCount")) is not int
            or value["recordCount"] != count
            or not hmac.compare_digest(
                value["rawFrameHash"],
                hashlib.sha256(value["rawFrame"].encode("utf-8")).hexdigest(),
            )
        ):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "raw-market-frame-body-hash-or-count-mismatch"
            )
        received = _utc(value.get("receivedAt"), "raw-frame-received-at")
        connected = _utc(row["connected_at"], "generation-connected-at")
        now = self._trusted_now()
        if not connected <= received <= now or (now - received).total_seconds() > 2:
            raise KisDomesticFunctionalMarketSourceBlocked(
                "raw-market-frame-time-lineage-invalid"
            )
        event_times: list[datetime] = []
        korea = timezone(timedelta(hours=9))
        for fields in records:
            try:
                event_time = datetime.strptime(
                    fields[33] + fields[1], "%Y%m%d%H%M%S"
                ).replace(tzinfo=korea).astimezone(timezone.utc)
            except ValueError:
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "h0stcnt0-record-event-time-invalid"
                ) from None
            lag = (received - event_time).total_seconds()
            if event_time < connected or not 0 <= lag <= 2:
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "h0stcnt0-record-event-time-lineage-invalid"
                )
            event_times.append(event_time)
        if event_times != sorted(event_times):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "h0stcnt0-record-event-time-order-invalid"
            )
        self._owner_snapshot(value)
        return value

    def _terminalize(
        self,
        source_generation: str,
        *,
        reason: str,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        owned = conn is None
        current = conn or self._connect()
        try:
            if owned:
                current.execute("BEGIN IMMEDIATE")
            row = current.execute(
                "SELECT state FROM kis_functional_market_source_generation "
                "WHERE source_generation=?", (source_generation,),
            ).fetchone()
            if row is not None and str(row["state"]) == "ARMED_WAIT_PUBLIC":
                terminal_at = _time_text(self._trusted_now())
                self._transition_generation(
                    current,
                    source_generation=source_generation,
                    transition_kind="GENERATION_TERMINALIZED",
                    to_state="SAFE_INCOMPLETE",
                    occurred_at=terminal_at,
                    reason=reason,
                    anchor_hash=_hash(
                        {
                            "schemaVersion": "kis-market-source-terminal-anchor/v1",
                            "sourceGeneration": source_generation,
                            "reason": reason,
                            "terminalAt": terminal_at,
                        }
                    ),
                    terminal_at=terminal_at,
                    failure_reason=reason,
                )
            if owned:
                current.commit()
        except BaseException:
            if owned:
                current.rollback()
            raise
        finally:
            if owned:
                current.close()

    def _ingest_signed_frame_v1_unreachable(
        self, record: Mapping[str, Any]
    ) -> dict[str, Any]:
        raise KisDomesticFunctionalMarketSourceBlocked(
            "legacy-market-source-ingress-v1-permanently-disabled"
        )
        if not isinstance(record, Mapping):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "raw-market-frame-not-mapping"
            )
        source_generation = str(record.get("sourceGeneration") or "")
        if not _GENERATION.fullmatch(source_generation):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "raw-frame-source-generation-invalid"
            )
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                _verify_schema(conn)
                row = conn.execute(
                    "SELECT * FROM kis_functional_market_source_generation "
                    "WHERE source_generation=?",
                    (source_generation,),
                ).fetchone()
                if row is None or str(row["state"]) != "ARMED_WAIT_PUBLIC":
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "source-generation-not-active"
                    )
                try:
                    value = self._raw_record(record, row)
                except BaseException:
                    self._terminalize(
                        source_generation,
                        reason="RAW_FRAME_OWNER_GENERATION_OR_GAP_REJECTED",
                        conn=conn,
                    )
                    conn.commit()
                    raise
                conn.execute(
                    "INSERT INTO kis_functional_market_source_ingress "
                    "(source_generation,ingress_ordinal,state,raw_record_json,"
                    "raw_record_hash,raw_record_signature,previous_head_hash) "
                    "VALUES(?,?,'INTENT',?,?,?,?)",
                    (
                        source_generation, value["ingressOrdinal"],
                        _canonical(value), value["recordHash"], value["signature"],
                        value["previousIngressHeadHash"],
                    ),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

            try:
                raw_ack = self.durable_ingress_writer(dict(value))
                ack = _verified_envelope(
                    raw_ack,
                    keys=_ACK_KEYS,
                    hash_key="ackHash",
                    verifier=self.durable_ack_verifier,
                    label="durable-ingress-ack",
                )
                expected_ack = {
                    "schemaVersion": ACK_SCHEMA,
                    "route": ROUTE,
                    "pdno": PDNO,
                    "sessionId": value["sessionId"],
                    "ownerEpochHash": value["ownerEpochHash"],
                    "sourceGeneration": source_generation,
                    "ingressOrdinal": value["ingressOrdinal"],
                    "rawFrameHash": value["rawFrameHash"],
                    "rawRecordHash": value["recordHash"],
                    "previousIngressHeadHash": value["previousIngressHeadHash"],
                    "durableRecordHash": value["recordHash"],
                    "productionAvailable": False,
                }
                if any(
                    type(ack.get(key)) is not type(wanted)
                    or ack.get(key) != wanted
                    for key, wanted in expected_ack.items()
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "durable-ingress-ack-binding-mismatch"
                    )
                _sha(ack.get("durableHeadHash"), "durable-ingress-head")
                _sha(ack.get("authorityKeyIdHash"), "durable-ack-key-id")
                received = _utc(value["receivedAt"], "raw-received-at")
                acked = _utc(ack.get("ackedAt"), "durable-ack-at")
                now = self._trusted_now()
                if not received <= acked <= now or (now - acked).total_seconds() > 2:
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "durable-ingress-ack-time-lineage-invalid"
                    )
                expected_head = _hash(
                    {
                        "schemaVersion": "kis-domestic-functional-market-source-head/v1",
                        "sourceGeneration": source_generation,
                        "ingressOrdinal": value["ingressOrdinal"],
                        "previousIngressHeadHash": value["previousIngressHeadHash"],
                        "rawRecordHash": value["recordHash"],
                        "durableRecordHash": ack["durableRecordHash"],
                    }
                )
                if not hmac.compare_digest(ack["durableHeadHash"], expected_head):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "durable-ingress-head-mismatch"
                    )
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    changed = conn.execute(
                        "UPDATE kis_functional_market_source_ingress SET "
                        "state='ACKED',durable_record_hash=?,durable_head_hash=?,"
                        "ack_json=?,ack_hash=? WHERE source_generation=? AND "
                        "ingress_ordinal=? AND state='INTENT' AND "
                        "raw_record_hash=?",
                        (
                            ack["durableRecordHash"], ack["durableHeadHash"],
                            _canonical(ack), ack["ackHash"], source_generation,
                            value["ingressOrdinal"], value["recordHash"],
                        ),
                    ).rowcount
                    if changed != 1:
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "durable-ingress-ack-cas-failed"
                        )
                    changed = conn.execute(
                        "UPDATE kis_functional_market_source_generation SET "
                        "last_ingress_ordinal=?,ingress_head_hash=?,revision=revision+1 "
                        "WHERE source_generation=? AND state='ARMED_WAIT_PUBLIC' "
                        "AND last_ingress_ordinal=? AND ingress_head_hash=?",
                        (
                            value["ingressOrdinal"], ack["durableHeadHash"],
                            source_generation, value["ingressOrdinal"] - 1,
                            value["previousIngressHeadHash"],
                        ),
                    ).rowcount
                    if changed != 1:
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "generation-ingress-head-cas-failed"
                        )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()

                raw_reducer = self.reducer(value["rawFrame"], dict(value), dict(ack))
                receipt = _verified_envelope(
                    raw_reducer,
                    keys=_REDUCER_KEYS,
                    hash_key="receiptHash",
                    verifier=self.reducer_receipt_verifier,
                    label="market-source-reducer-receipt",
                )
                expected_receipt = {
                    "schemaVersion": REDUCER_SCHEMA,
                    "route": ROUTE,
                    "pdno": PDNO,
                    "sessionId": value["sessionId"],
                    "sourceGeneration": source_generation,
                    "ingressOrdinal": value["ingressOrdinal"],
                    "rawRecordHash": value["recordHash"],
                    "durableRecordHash": ack["durableRecordHash"],
                    "durableHeadHash": ack["durableHeadHash"],
                    "reducerState": "ACCEPTED",
                    "productionAvailable": False,
                }
                if any(
                    type(receipt.get(key)) is not type(wanted)
                    or receipt.get(key) != wanted
                    for key, wanted in expected_receipt.items()
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-reducer-receipt-binding-mismatch"
                    )
                if (
                    type(receipt.get("closedBarCount")) is not int
                    or receipt["closedBarCount"] < 0
                    or type(receipt.get("nextOpenObserved")) is not bool
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-reducer-receipt-values-invalid"
                    )
                reduced = _utc(receipt.get("reducedAt"), "reducer-reduced-at")
                if reduced < acked or reduced > self._trusted_now():
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-reducer-time-lineage-invalid"
                    )
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    changed = conn.execute(
                        "UPDATE kis_functional_market_source_ingress SET "
                        "state='REDUCED',reducer_receipt_json=?,"
                        "reducer_receipt_hash=? WHERE source_generation=? AND "
                        "ingress_ordinal=? AND state='ACKED'",
                        (
                            _canonical(receipt), receipt["receiptHash"],
                            source_generation, value["ingressOrdinal"],
                        ),
                    ).rowcount
                    if changed != 1:
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "market-source-reducer-receipt-cas-failed"
                        )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                return {
                    "schemaVersion": "kis-domestic-functional-market-source-ingress-result/v1",
                    "sourceGeneration": source_generation,
                    "ingressOrdinal": value["ingressOrdinal"],
                    "rawRecordHash": value["recordHash"],
                    "durableRecordHash": ack["durableRecordHash"],
                    "durableHeadHash": ack["durableHeadHash"],
                    "reducerReceiptHash": receipt["receiptHash"],
                    "rawIngressAckedBeforeReducer": True,
                    "upstreamExchangeSequenceAvailable": False,
                    "upstreamPacketCompletenessAttested": False,
                    "dualSourceBarConfirmationAvailable": False,
                    "productionAvailable": False,
                }
            except BaseException:
                self._terminalize(
                    source_generation,
                    reason="INGRESS_ACK_OR_REDUCER_RECONCILIATION_REQUIRED",
                )
                raise

    def ingest_signed_frame(self, record: Mapping[str, Any]) -> dict[str, Any]:
        """Persist raw intent, verify the source ACK, then invoke the reducer."""
        if not isinstance(record, Mapping):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "raw-market-frame-not-mapping"
            )
        source_generation = str(record.get("sourceGeneration") or "")
        if not _GENERATION.fullmatch(source_generation):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "raw-frame-source-generation-invalid"
            )
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                _verify_schema(conn)
                generation = conn.execute(
                    "SELECT * FROM kis_functional_market_source_generation "
                    "WHERE source_generation=?", (source_generation,),
                ).fetchone()
                if generation is None or str(generation["state"]) != "ARMED_WAIT_PUBLIC":
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "source-generation-not-active"
                    )
                try:
                    value = self._raw_record(record, generation)
                except BaseException:
                    self._terminalize(
                        source_generation,
                        reason="RAW_FRAME_OWNER_GENERATION_OR_GAP_REJECTED",
                        conn=conn,
                    )
                    conn.commit()
                    raise
                _, parsed_records = self._frame(value["rawFrame"])
                parsed_json = _canonical(parsed_records)
                parsed_hash = hashlib.sha256(parsed_json.encode("utf-8")).hexdigest()
                conn.execute(
                    "INSERT INTO kis_functional_market_source_ingress "
                    "(source_generation,ingress_ordinal,state,raw_record_json,"
                    "raw_record_hash,raw_record_signature,parsed_records_json,"
                    "parsed_records_hash,previous_head_hash) "
                    "VALUES(?,?,'INTENT',?,?,?,?,?,?)",
                    (
                        source_generation, value["ingressOrdinal"],
                        _canonical(value), value["recordHash"], value["signature"],
                        parsed_json, parsed_hash, value["previousIngressHeadHash"],
                    ),
                )
                self._transition_ingress(
                    conn,
                    source_generation=source_generation,
                    ingress_ordinal=value["ingressOrdinal"],
                    transition_kind="INGRESS_INTENT_PERSISTED",
                    to_state="INTENT",
                    occurred_at=value["receivedAt"],
                    reason="RAW_FRAME_VERIFIED_AND_INTENT_DURABLE",
                    anchor_hash=value["recordHash"],
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

            try:
                ack = _verified_envelope(
                    self.durable_ingress_writer(dict(value)),
                    keys=_ACK_KEYS,
                    hash_key="ackHash",
                    verifier=self.durable_ack_verifier,
                    label="durable-ingress-ack",
                )
                expected_ack = {
                    "schemaVersion": ACK_SCHEMA,
                    "route": ROUTE,
                    "pdno": PDNO,
                    "sessionId": value["sessionId"],
                    "ownerEpochHash": value["ownerEpochHash"],
                    "sourceGeneration": source_generation,
                    "ingressOrdinal": value["ingressOrdinal"],
                    "rawFrameHash": value["rawFrameHash"],
                    "rawRecordHash": value["recordHash"],
                    "previousIngressHeadHash": value["previousIngressHeadHash"],
                    "authorityPurpose": SOURCE_ACK_AUTHORITY_PURPOSE,
                    "productionAvailable": False,
                }
                if any(
                    type(ack.get(key)) is not type(wanted)
                    or ack.get(key) != wanted
                    for key, wanted in expected_ack.items()
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "durable-ingress-ack-binding-mismatch"
                    )
                for key in (
                    "durableRecordHash", "durableHeadHash", "authorityKeyIdHash",
                    "sourceFrameEnvelopeHash", "sourceFrameHeadHash",
                    "sourceArmTransitionHeadHash",
                ):
                    _sha(ack.get(key), f"durable-ack-{key}")
                _identity(ack.get("sourceArmId"), "durable-ack-source-arm")
                if (
                    type(ack.get("sourceFrameIndex")) is not int
                    or ack["sourceFrameIndex"] < 1
                    or ack["sourceFrameIndex"] != value["ingressOrdinal"]
                    or type(ack.get("firstSourceSequence")) is not int
                    or type(ack.get("lastSourceSequence")) is not int
                    or ack["firstSourceSequence"] < 1
                    or ack["lastSourceSequence"] - ack["firstSourceSequence"] + 1
                    != value["recordCount"]
                    or ack["durableRecordHash"] != ack["sourceFrameEnvelopeHash"]
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "durable-ingress-source-projection-invalid"
                    )
                received = _utc(value["receivedAt"], "raw-received-at")
                acked = _utc(ack.get("ackedAt"), "durable-ack-at")
                now = self._trusted_now()
                if not received <= acked <= now or (now - acked).total_seconds() > 2:
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "durable-ingress-ack-time-lineage-invalid"
                    )
                expected_head = _hash(
                    {
                        "schemaVersion": "kis-domestic-functional-market-source-head/v2",
                        "sourceGeneration": source_generation,
                        "ingressOrdinal": value["ingressOrdinal"],
                        "previousIngressHeadHash": value["previousIngressHeadHash"],
                        "rawRecordHash": value["recordHash"],
                        "sourceArmId": ack["sourceArmId"],
                        "sourceFrameIndex": ack["sourceFrameIndex"],
                        "sourceFrameEnvelopeHash": ack["sourceFrameEnvelopeHash"],
                        "sourceFrameHeadHash": ack["sourceFrameHeadHash"],
                        "sourceArmTransitionHeadHash": ack[
                            "sourceArmTransitionHeadHash"
                        ],
                    }
                )
                if not hmac.compare_digest(ack["durableHeadHash"], expected_head):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "durable-ingress-head-mismatch"
                    )
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    self._transition_ingress(
                        conn,
                        source_generation=source_generation,
                        ingress_ordinal=value["ingressOrdinal"],
                        transition_kind="SOURCE_JOURNAL_ACK_VERIFIED",
                        to_state="ACKED",
                        occurred_at=ack["ackedAt"],
                        reason="SIGNED_SOURCE_JOURNAL_ACK_BOUND",
                        anchor_hash=ack["ackHash"],
                        projection={
                            "durable_record_hash": ack["durableRecordHash"],
                            "durable_head_hash": ack["durableHeadHash"],
                            "ack_json": _canonical(ack),
                            "ack_hash": ack["ackHash"],
                            "ack_authority_key_id_hash": ack["authorityKeyIdHash"],
                        },
                    )
                    self._transition_generation(
                        conn,
                        source_generation=source_generation,
                        transition_kind="INGRESS_HEAD_ADVANCED",
                        to_state="ARMED_WAIT_PUBLIC",
                        occurred_at=ack["ackedAt"],
                        reason="SOURCE_JOURNAL_ACK_ADVANCED_LOCAL_HEAD",
                        anchor_hash=ack["ackHash"],
                        last_ingress_ordinal=value["ingressOrdinal"],
                        ingress_head_hash=ack["durableHeadHash"],
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()

                receipt = _verified_envelope(
                    self.reducer(value["rawFrame"], dict(value), dict(ack)),
                    keys=_REDUCER_KEYS,
                    hash_key="receiptHash",
                    verifier=self.reducer_receipt_verifier,
                    label="market-source-reducer-receipt",
                )
                expected_receipt = {
                    "schemaVersion": REDUCER_SCHEMA,
                    "route": ROUTE,
                    "pdno": PDNO,
                    "sessionId": value["sessionId"],
                    "sourceGeneration": source_generation,
                    "ingressOrdinal": value["ingressOrdinal"],
                    "rawRecordHash": value["recordHash"],
                    "durableRecordHash": ack["durableRecordHash"],
                    "durableHeadHash": ack["durableHeadHash"],
                    "reducerState": "ACCEPTED",
                    "authorityKeyIdHash": ack["authorityKeyIdHash"],
                    "authorityPurpose": SOURCE_ACK_AUTHORITY_PURPOSE,
                    "productionAvailable": False,
                }
                if any(
                    type(receipt.get(key)) is not type(wanted)
                    or receipt.get(key) != wanted
                    for key, wanted in expected_receipt.items()
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-reducer-receipt-binding-mismatch"
                    )
                if (
                    type(receipt.get("closedBarCount")) is not int
                    or receipt["closedBarCount"] < 0
                    or type(receipt.get("nextOpenObserved")) is not bool
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-reducer-receipt-values-invalid"
                    )
                reduced = _utc(receipt.get("reducedAt"), "reducer-reduced-at")
                if reduced < acked or reduced > self._trusted_now():
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-reducer-time-lineage-invalid"
                    )
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    self._transition_ingress(
                        conn,
                        source_generation=source_generation,
                        ingress_ordinal=value["ingressOrdinal"],
                        transition_kind="REDUCER_RECEIPT_VERIFIED",
                        to_state="REDUCED",
                        occurred_at=receipt["reducedAt"],
                        reason="SIGNED_REDUCER_RECEIPT_BOUND",
                        anchor_hash=receipt["receiptHash"],
                        projection={
                            "reducer_receipt_json": _canonical(receipt),
                            "reducer_receipt_hash": receipt["receiptHash"],
                            "reducer_authority_key_id_hash": receipt[
                                "authorityKeyIdHash"
                            ],
                        },
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                return {
                    "schemaVersion": (
                        "kis-domestic-functional-market-source-ingress-result/v2"
                    ),
                    "sourceGeneration": source_generation,
                    "ingressOrdinal": value["ingressOrdinal"],
                    "rawRecordHash": value["recordHash"],
                    "durableRecordHash": ack["durableRecordHash"],
                    "durableHeadHash": ack["durableHeadHash"],
                    "sourceArmId": ack["sourceArmId"],
                    "sourceFrameIndex": ack["sourceFrameIndex"],
                    "sourceFrameEnvelopeHash": ack["sourceFrameEnvelopeHash"],
                    "sourceFrameHeadHash": ack["sourceFrameHeadHash"],
                    "sourceArmTransitionHeadHash": ack[
                        "sourceArmTransitionHeadHash"
                    ],
                    "reducerReceiptHash": receipt["receiptHash"],
                    "rawIngressAckedBeforeReducer": True,
                    "upstreamExchangeSequenceAvailable": False,
                    "upstreamPacketCompletenessAttested": False,
                    "dualSourceBarConfirmationAvailable": False,
                    "productionAvailable": False,
                }
            except BaseException:
                self._terminalize(
                    source_generation,
                    reason="INGRESS_ACK_OR_REDUCER_RECONCILIATION_REQUIRED",
                )
                raise

    def _startup_audit(self) -> tuple[str, ...]:
        conn = self._connect()
        terminalized: list[str] = []
        try:
            conn.execute("BEGIN IMMEDIATE")
            _verify_schema(conn)
            rows = conn.execute(
                "SELECT g.source_generation,"
                "SUM(CASE WHEN i.state IN ('INTENT','ACKED') THEN 1 ELSE 0 END) "
                "AS pending_count FROM "
                "kis_functional_market_source_generation g LEFT JOIN "
                "kis_functional_market_source_ingress i ON "
                "i.source_generation=g.source_generation WHERE "
                "g.state='ARMED_WAIT_PUBLIC' GROUP BY g.source_generation"
            ).fetchall()
            for row in rows:
                generation = str(row["source_generation"])
                reason = (
                    "STARTUP_OWNER_LOSS_WITH_PENDING_INGRESS"
                    if int(row["pending_count"] or 0) > 0
                    else "STARTUP_OWNER_LOSS_REQUIRES_NEW_SOCKET_GENERATION"
                )
                now = _time_text(self._trusted_now())
                self._transition_generation(
                    conn,
                    source_generation=generation,
                    transition_kind="STARTUP_OWNER_LOSS",
                    to_state="SAFE_INCOMPLETE",
                    occurred_at=now,
                    reason=reason,
                    anchor_hash=_hash(
                        {
                            "schemaVersion": "kis-market-source-startup-anchor/v1",
                            "sourceGeneration": generation,
                            "reason": reason,
                            "observedAt": now,
                        }
                    ),
                    terminal_at=now,
                    failure_reason=reason,
                )
                terminalized.append(generation)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return tuple(terminalized)

    def snapshot(self, source_generation: str) -> dict[str, Any]:
        if type(source_generation) is not str or not _GENERATION.fullmatch(
            source_generation
        ):
            raise KisDomesticFunctionalMarketSourceBlocked(
                "snapshot-source-generation-invalid"
            )
        conn = self._connect()
        try:
            _verify_schema(conn)
            row = conn.execute(
                "SELECT * FROM kis_functional_market_source_generation "
                "WHERE source_generation=?",
                (source_generation,),
            ).fetchone()
            if row is None:
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "source-generation-not-found"
                )
            self._verify_transition_chain(
                conn,
                source_generation=source_generation,
                ingress_ordinal=0,
                expected_state=str(row["state"]),
                expected_count=int(row["transition_count"]),
                expected_head=str(row["transition_head_hash"]),
                expected_revision=int(row["revision"]),
            )
            ingress_rows = conn.execute(
                "SELECT * FROM kis_functional_market_source_ingress WHERE "
                "source_generation=? ORDER BY ingress_ordinal",
                (source_generation,),
            ).fetchall()
            if [int(item["ingress_ordinal"]) for item in ingress_rows] != list(
                range(1, len(ingress_rows) + 1)
            ):
                raise KisDomesticFunctionalMarketSourceBlocked(
                    "market-source-ingress-ordinal-cardinality-mismatch"
                )
            for item in ingress_rows:
                ordinal = int(item["ingress_ordinal"])
                self._verify_transition_chain(
                    conn,
                    source_generation=source_generation,
                    ingress_ordinal=ordinal,
                    expected_state=str(item["state"]),
                    expected_count=int(item["transition_count"]),
                    expected_head=str(item["transition_head_hash"]),
                    expected_revision=int(item["revision"]),
                )
                try:
                    raw = json.loads(str(item["raw_record_json"]))
                    stored_records = json.loads(str(item["parsed_records_json"]))
                except (TypeError, ValueError, json.JSONDecodeError):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-ingress-json-invalid"
                    ) from None
                raw = _verified_envelope(
                    raw,
                    keys=_RAW_KEYS,
                    hash_key="recordHash",
                    verifier=self.raw_record_verifier,
                    label="stored-raw-market-frame",
                )
                _, records = self._frame(raw["rawFrame"])
                parsed_json = _canonical(records)
                if (
                    raw["sourceGeneration"] != source_generation
                    or raw["ingressOrdinal"] != ordinal
                    or raw["recordHash"] != str(item["raw_record_hash"])
                    or raw["signature"] != str(item["raw_record_signature"])
                    or stored_records != records
                    or parsed_json != str(item["parsed_records_json"])
                    or hashlib.sha256(parsed_json.encode("utf-8")).hexdigest()
                    != str(item["parsed_records_hash"])
                ):
                    raise KisDomesticFunctionalMarketSourceBlocked(
                        "market-source-ingress-raw-projection-mismatch"
                    )
                if str(item["state"]) in {"ACKED", "REDUCED"}:
                    try:
                        stored_ack = json.loads(str(item["ack_json"]))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "market-source-ack-json-invalid"
                        ) from None
                    stored_ack = _verified_envelope(
                        stored_ack,
                        keys=_ACK_KEYS,
                        hash_key="ackHash",
                        verifier=self.durable_ack_verifier,
                        label="stored-source-ack",
                    )
                    if (
                        stored_ack["sourceGeneration"] != source_generation
                        or stored_ack["ingressOrdinal"] != ordinal
                        or stored_ack["rawRecordHash"] != raw["recordHash"]
                        or stored_ack["ackHash"] != str(item["ack_hash"])
                        or stored_ack["authorityKeyIdHash"]
                        != str(item["ack_authority_key_id_hash"])
                        or stored_ack["durableRecordHash"]
                        != str(item["durable_record_hash"])
                        or stored_ack["durableHeadHash"]
                        != str(item["durable_head_hash"])
                    ):
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "market-source-ack-projection-mismatch"
                        )
                if str(item["state"]) == "REDUCED":
                    try:
                        stored_receipt = json.loads(
                            str(item["reducer_receipt_json"])
                        )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "market-source-reducer-json-invalid"
                        ) from None
                    stored_receipt = _verified_envelope(
                        stored_receipt,
                        keys=_REDUCER_KEYS,
                        hash_key="receiptHash",
                        verifier=self.reducer_receipt_verifier,
                        label="stored-reducer-receipt",
                    )
                    if (
                        stored_receipt["sourceGeneration"] != source_generation
                        or stored_receipt["ingressOrdinal"] != ordinal
                        or stored_receipt["rawRecordHash"] != raw["recordHash"]
                        or stored_receipt["receiptHash"]
                        != str(item["reducer_receipt_hash"])
                        or stored_receipt["authorityKeyIdHash"]
                        != str(item["reducer_authority_key_id_hash"])
                    ):
                        raise KisDomesticFunctionalMarketSourceBlocked(
                            "market-source-reducer-projection-mismatch"
                        )
            pending = conn.execute(
                "SELECT COUNT(*) FROM kis_functional_market_source_ingress "
                "WHERE source_generation=? AND state IN ('INTENT','ACKED')",
                (source_generation,),
            ).fetchone()[0]
            body = {
                "schemaVersion": STATUS_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "trId": TR_ID,
                "sessionId": str(row["session_id"]),
                "accountFingerprint": str(row["account_fingerprint"]),
                "ownerEpoch": int(row["owner_epoch"]),
                "ownerEpochId": str(row["owner_epoch_id"]),
                "ownerEpochHash": str(row["owner_epoch_hash"]),
                "processGeneration": str(row["process_generation"]),
                "sourceGeneration": str(row["source_generation"]),
                "socketIdentityHash": str(row["socket_identity_hash"]),
                "captureAuthorityKeyIdHash": str(
                    row["authority_key_id_hash"]
                ),
                "state": str(row["state"]),
                "lastIngressOrdinal": int(row["last_ingress_ordinal"]),
                "ingressHeadHash": str(row["ingress_head_hash"]),
                "pendingIngressCount": int(pending),
                "reconnectPredecessorGeneration": str(
                    row["reconnect_predecessor_generation"]
                ),
                "failureReason": str(row["failure_reason"]),
                "generationTransitionCount": int(row["transition_count"]),
                "generationTransitionHeadHash": str(
                    row["transition_head_hash"]
                ),
                "ingressRecordCount": len(ingress_rows),
                "allIngressTransitionChainsVerified": True,
                "allRawFramesReparsedAsExact46FieldRecords": True,
                "upstreamExchangeSequenceAvailable": False,
                "upstreamPacketCompletenessAttested": False,
                "acceptedIngressContinuityOnly": True,
                "officialKisMinuteCandleGetAdapterAvailable": False,
                "dualSourceBarConfirmationAvailable": False,
                "dualSourceBlocker": (
                    "OFFICIAL_KIS_MINUTE_CANDLE_GET_ADAPTER_NOT_FOUND"
                ),
                "rawIngressAckRequiredBeforeReducer": True,
                "restartRequiresNewSocketGeneration": True,
                "externalSocketOwnerEpochRequired": True,
                "crossProcessSocketOwnerLeaseWired": False,
                "signedVerifyOnlyRecordsRequired": True,
                "publicMarketDataOnly": True,
                "accountAuthorityAvailable": False,
                "mutationAuthorityAvailable": False,
                "networkExecutorAvailable": False,
                "productionAvailable": False,
                "releaseAvailable": False,
                "revision": int(row["revision"]),
            }
            return {**body, "statusHash": _hash(body)}
        finally:
            conn.close()

    def component_status(self) -> dict[str, Any]:
        return market_source_component_status()


def market_source_component_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-market-source-component/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "trId": TR_ID,
        "approvalOrigin": LIVE_ORIGIN,
        "approvalEndpoint": APPROVAL_ENDPOINT,
        "websocketUrl": LIVE_WEBSOCKET_URL,
        "subscriptionBodyHash": _subscription_body_hash(),
        "rawCallbackBeforeReducerRequired": True,
        "restartRequiresNewSocketGeneration": True,
        "externalSocketOwnerEpochRequired": True,
        "crossProcessSocketOwnerLeaseWired": False,
        "signedVerifyOnlyRecordsRequired": True,
        "reconnectRequiresNewGeneration": True,
        "reconnectPriorGenerationSafeIncomplete": True,
        "upstreamExchangeSequenceAvailable": False,
        "upstreamPacketCompletenessAttested": False,
        "acceptedIngressContinuityOnly": True,
        "officialKisMinuteCandleGetAdapterAvailable": False,
        "dualSourceBarConfirmationAvailable": False,
        "dualSourceBlocker": "OFFICIAL_KIS_MINUTE_CANDLE_GET_ADAPTER_NOT_FOUND",
        "networkExecutorAvailable": False,
        "productionAvailable": False,
        "releaseAvailable": False,
        "accountAuthorityAvailable": False,
        "mutationAuthorityAvailable": False,
        "schemaFingerprint": SCHEMA_FINGERPRINT,
    }


__all__ = [
    "ACK_SCHEMA",
    "APPROVAL_ENDPOINT",
    "DisabledKisDomesticFunctionalMarketSource",
    "HANDSHAKE_SCHEMA",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_DUAL_SOURCE_CONFIRMATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_NETWORK_EXECUTOR_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_RELEASE_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_UPSTREAM_COMPLETENESS_AVAILABLE",
    "KisDomesticFunctionalMarketSourceBlocked",
    "LIVE_ORIGIN",
    "LIVE_WEBSOCKET_URL",
    "OWNER_SCHEMA",
    "RAW_RECORD_SCHEMA",
    "REDUCER_SCHEMA",
    "SCHEMA_FINGERPRINT",
    "SCHEMA_VERSION",
    "market_source_component_status",
]
