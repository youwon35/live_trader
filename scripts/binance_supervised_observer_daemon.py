from __future__ import annotations

"""Independent Binance supervised observer daemon.

The trader imports only the public verifier protocol.  This executable alone
loads the Ed25519 private key, owns its private SQLite journal, performs the
official baseline GETs, and subscribes to the account-wide Spot user stream.
"""

import argparse
import base64
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from typing import Any, Mapping
import uuid

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.binance_spot_functional_supervised_exclusivity import (
    ASSURANCE_MODE,
    BinanceSpotSupervisedOfficialGetProvider,
    _protected_binance_spot_supervised_get_network_capability,
)
from live_trader.binance_spot_functional_transport import (
    BINANCE_SPOT_PRODUCTION_ORIGIN,
    binance_api_key_fingerprint,
)
from live_trader.binance_spot_supervised_authority_protocol import (
    PROCESS_AUDIT_SCHEMA_VERSION,
    SNAPSHOT_SCHEMA_VERSION,
    STREAM_AUDIT_SCHEMA_VERSION,
    authority_hash,
    canonical_authority_message,
)
from live_trader.execution_streams import binance_stream_subscription_params
from live_trader.live_adapters import env_value
from live_trader.process_safety import hold_process_lease
from trading_runtime.secret_store import WindowsDpapiProtector


ZERO_HASH = "0" * 64
CONFIG_SCHEMA = "binance-supervised-observer-config/v1"
PROTECTED_CREDENTIAL_SCHEMA = (
    "crypto-first-live-machine-protected-broker-credential/v1"
)
PROTECTED_CREDENTIAL_ENTROPY = (
    b"crypto-first-live-machine-credential:v1\x00"
)
PREARMED_READY_SCHEMA = "binance-supervised-observer-prearmed-ready/v1"
MAX_CONFIG_BYTES = 64 * 1024
MAX_EVENT_BYTES = 512 * 1024
LIVENESS_SECONDS = 5.0
SNAPSHOT_INTERVAL_SECONDS = 1.0
PROCESS_AUDIT_INTERVAL_SECONDS = 5.0


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _text(value: object) -> str:
    return str(value or "").strip()


def _strict_json(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise RuntimeError("observer config is missing or a link")
    raw = path.read_bytes()
    if not 1 <= len(raw) <= MAX_CONFIG_BYTES:
        raise RuntimeError("observer config size is invalid")
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("observer config must be an object")
    return dict(value)


def _config(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    fields = {
        "schemaVersion",
        "authorityId",
        "keyId",
        "sessionId",
        "permitId",
        "permitHash",
        "credentialFingerprint",
        "credentialAuthorityId",
        "credentialGenerationId",
        "credentialEnvelopeHash",
        "bundleManifestSha256",
        "ownerClientOrderPrefix",
        "authorizedTraderPid",
        "authorizedTraderCommandSha256",
        "botCommandMarker",
        "maxRuntimeSeconds",
    }
    if (
        set(row) != fields
        or row.get("schemaVersion") != CONFIG_SCHEMA
        or any(not _text(row.get(field)) for field in fields - {
            "schemaVersion", "authorizedTraderPid", "maxRuntimeSeconds"
        })
        or isinstance(row.get("authorizedTraderPid"), bool)
        or not isinstance(row.get("authorizedTraderPid"), int)
        or row["authorizedTraderPid"] <= 0
        or row.get("maxRuntimeSeconds") != 10800
        or len(row["permitHash"]) != 64
        or len(row["credentialFingerprint"]) != 64
        or len(row["credentialEnvelopeHash"]) != 64
        or len(row["bundleManifestSha256"]) != 64
        or len(row["authorizedTraderCommandSha256"]) != 64
        or not row["ownerClientOrderPrefix"].startswith("ftb-")
    ):
        raise RuntimeError("observer config fields are not exact")
    return row


def _load_protected_credentials(
    path: Path,
    *,
    authority_id: str,
    manifest_sha256: str,
    account_fingerprint: str,
    credential_generation_id: str,
    expected_envelope_hash: str,
    unprotect: Any | None = None,
) -> dict[str, str]:
    """Decrypt one LocalMachine DPAPI blob only inside authority memory."""

    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        raise RuntimeError("observer machine credential blob is unavailable")
    ciphertext = path.read_bytes()
    if not 32 <= len(ciphertext) <= MAX_CONFIG_BYTES:
        raise RuntimeError("observer machine credential blob size is invalid")
    decrypt = (
        unprotect
        if callable(unprotect)
        else WindowsDpapiProtector().unprotect
    )
    entropy_parts = (
        _text(authority_id),
        _text(manifest_sha256),
        _text(account_fingerprint),
        _text(credential_generation_id),
        "BINANCE_SPOT",
    )
    if (
        any(not item for item in entropy_parts)
        or any(len(item) > 256 for item in entropy_parts)
        or any("\x00" in item for item in entropy_parts)
    ):
        raise RuntimeError("observer machine credential entropy is invalid")
    entropy = PROTECTED_CREDENTIAL_ENTROPY + b"\x00".join(
        item.encode("utf-8") for item in entropy_parts
    )
    try:
        plaintext = bytes(decrypt(ciphertext, entropy))
        value = json.loads(plaintext.decode("utf-8"))
    except (OSError, ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("observer machine credential decrypt failed") from exc
    if not isinstance(value, dict) or set(value) != {
        "schemaVersion",
        "authorityId",
        "credentialGenerationId",
        "lane",
        "origin",
        "accessKey",
        "secretKey",
        "credentialFingerprint",
        "accountFingerprint",
        "envelopeHash",
    }:
        raise RuntimeError("observer machine credential fields are not exact")
    body = {
        key: item for key, item in value.items() if key != "envelopeHash"
    }
    expected_plaintext = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    api_key = _text(value.get("accessKey"))
    api_secret = _text(value.get("secretKey"))
    fingerprint = binance_api_key_fingerprint(api_key)
    envelope_hash = hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()
    if (
        value.get("schemaVersion") != PROTECTED_CREDENTIAL_SCHEMA
        or value.get("authorityId") != authority_id
        or value.get("credentialGenerationId") != credential_generation_id
        or value.get("lane") != "BINANCE_SPOT"
        or value.get("origin") != BINANCE_SPOT_PRODUCTION_ORIGIN
        or value.get("credentialFingerprint") != fingerprint
        or value.get("accountFingerprint") != fingerprint
        or fingerprint != account_fingerprint
        or value.get("envelopeHash") != envelope_hash
        or envelope_hash != expected_envelope_hash
        or plaintext != expected_plaintext
        or not 16 <= len(api_key) <= 256
        or not 16 <= len(api_secret) <= 512
    ):
        raise RuntimeError("observer machine credentials are invalid")
    os.environ["BINANCE_API_KEY"] = api_key
    os.environ["BINANCE_API_SECRET"] = api_secret
    os.environ["BINANCE_BASE_URL"] = BINANCE_SPOT_PRODUCTION_ORIGIN
    return {
        "credentialFingerprint": fingerprint,
        "credentialGenerationId": credential_generation_id,
        "envelopeHash": envelope_hash,
        "origin": BINANCE_SPOT_PRODUCTION_ORIGIN,
    }


def _write_prearmed_ready(path: Path, *, config: Mapping[str, Any]) -> None:
    if not path.is_absolute() or path.exists() or path.is_symlink():
        raise RuntimeError("observer prearmed ready path is invalid")
    body = {
        "schemaVersion": PREARMED_READY_SCHEMA,
        "observerProcessId": os.getpid(),
        "configHash": authority_hash(dict(config)),
        "signedGetAttemptCount": 0,
        "mutationAttemptCount": 0,
        "networkCapabilityOpen": False,
    }
    payload = {**body, "readyHash": authority_hash(body)}
    encoded = _canonical(payload).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def _wait_for_start_gate(path: Path, *, timeout_seconds: float = 5.0) -> None:
    if not path.is_absolute() or path.exists() or path.is_symlink():
        raise RuntimeError("observer prearmed start gate path is invalid")
    deadline = time.monotonic() + float(timeout_seconds)
    while time.monotonic() < deadline:
        if path.is_file() and not path.is_symlink():
            if path.stat().st_size != 0:
                raise RuntimeError("observer prearmed start gate is malformed")
            path.unlink()
            return
        time.sleep(0.005)
    raise RuntimeError("observer prearmed start gate timed out")


def _load_private_key(path: Path) -> ECC.EccKey:
    if not path.is_file() or path.is_symlink():
        raise RuntimeError("observer private key is missing or a link")
    key = ECC.import_key(path.read_bytes())
    if not key.has_private() or getattr(key, "curve", None) != "Ed25519":
        raise RuntimeError("observer private Ed25519 key is invalid")
    return key


def _process_rows_windows() -> list[dict[str, Any]]:
    if os.name != "nt":
        raise RuntimeError("observer process registry requires Windows CIM")
    command = (
        "Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,CreationDate,ExecutablePath,CommandLine | "
        "ConvertTo-Json -Compress"
    )
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            command,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=4.0,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    value = json.loads(completed.stdout or "[]")
    rows = value if isinstance(value, list) else [value]
    if any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("Windows process registry response is malformed")
    return [dict(row) for row in rows]


def _command_hash(value: object) -> str:
    normalized = " ".join(_text(value).split()).lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _process_audit(config: Mapping[str, Any], now: float) -> dict[str, Any]:
    rows = _process_rows_windows()
    trader_rows = [
        row
        for row in rows
        if int(row.get("ProcessId") or 0) == int(config["authorizedTraderPid"])
        and _command_hash(row.get("CommandLine"))
        == config["authorizedTraderCommandSha256"]
    ]
    marker = _text(config["botCommandMarker"]).lower()
    registered = [
        row
        for row in rows
        if marker in _text(row.get("CommandLine")).lower()
    ]
    other = [
        row
        for row in registered
        if int(row.get("ProcessId") or 0) != int(config["authorizedTraderPid"])
    ]
    if len(trader_rows) != 1 or len(registered) != 1 or other:
        raise RuntimeError("OS process registry does not contain exact bot 1")
    identity = {
        "pid": int(trader_rows[0]["ProcessId"]),
        "creationDate": _text(trader_rows[0].get("CreationDate")),
        "executablePathHash": hashlib.sha256(
            _text(trader_rows[0].get("ExecutablePath")).lower().encode("utf-8")
        ).hexdigest(),
        "commandHash": config["authorizedTraderCommandSha256"],
    }
    body = {
        "schemaVersion": PROCESS_AUDIT_SCHEMA_VERSION,
        "source": "WINDOWS_CIM_PROCESS_REGISTRY",
        "observedEpoch": now,
        "authorizedTraderProcessIdentityHash": authority_hash(identity),
        "authorizedFunctionalBotCount": 1,
        "otherRegisteredBotCount": 0,
        "observerProcessSeparate": os.getpid() != int(config["authorizedTraderPid"]),
        "independentlyVerified": True,
    }
    if body["observerProcessSeparate"] is not True:
        raise RuntimeError("observer process is not separate from trader")
    return {**body, "auditHash": authority_hash(body)}


def _event_object(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    if isinstance(payload.get("event"), Mapping):
        return dict(payload["event"])
    if _text(payload.get("e") or payload.get("eventType")):
        return dict(payload)
    return None


def _order_client_ids(event: Mapping[str, Any]) -> tuple[str, ...]:
    event_type = _text(event.get("e") or event.get("eventType")).lower()
    if event_type == "executionreport":
        return tuple(
            value
            for value in (
                _text(event.get("c") or event.get("clientOrderId")),
                _text(event.get("C") or event.get("originalClientOrderId")),
            )
            if value
        )
    if event_type == "liststatus":
        result = [_text(event.get("C") or event.get("listClientOrderId"))]
        orders = event.get("O") or event.get("orders") or []
        if isinstance(orders, list):
            for row in orders:
                if isinstance(row, Mapping):
                    result.append(_text(row.get("c") or row.get("clientOrderId")))
        return tuple(item for item in result if item)
    return ()


class AuthorityJournal:
    def __init__(self, path: Path, *, config: Mapping[str, Any]) -> None:
        if path.exists() and (path.is_symlink() or not path.is_file()):
            raise RuntimeError("observer database path is invalid")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path.resolve()
        self.config = dict(config)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS authority_meta(
                   singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                   config_hash TEXT NOT NULL, coverage_started_epoch REAL NOT NULL,
                   subscribed_epoch REAL NOT NULL, last_liveness_epoch REAL NOT NULL,
                   observed_epoch REAL NOT NULL, authority_sequence INTEGER NOT NULL,
                   previous_snapshot_hash TEXT NOT NULL, event_chain_hash TEXT NOT NULL,
                   event_count INTEGER NOT NULL, order_event_count INTEGER NOT NULL,
                   unowned_order_event_count INTEGER NOT NULL, connected INTEGER NOT NULL,
                   authenticated INTEGER NOT NULL, gap_detected INTEGER NOT NULL,
                   crash_detected INTEGER NOT NULL, revoked INTEGER NOT NULL,
                   revoke_reason TEXT NOT NULL, official_json TEXT NOT NULL,
                   process_audit_json TEXT NOT NULL)"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS authority_events(
                   sequence INTEGER PRIMARY KEY, received_epoch REAL NOT NULL,
                   event_type TEXT NOT NULL, payload_hash TEXT NOT NULL,
                   client_ids_hash TEXT NOT NULL, owned INTEGER NOT NULL,
                   previous_event_hash TEXT NOT NULL, event_hash TEXT NOT NULL UNIQUE)"""
            )
            existing = connection.execute(
                "SELECT * FROM authority_meta WHERE singleton=1"
            ).fetchone()
            if existing is not None:
                if int(existing["revoked"]) == 0:
                    connection.execute(
                        """UPDATE authority_meta SET revoked=1,gap_detected=1,
                           crash_detected=1,connected=0,
                           revoke_reason='observer restarted before terminal handoff'
                           WHERE singleton=1"""
                    )
                connection.execute("COMMIT")
                raise RuntimeError("observer database contains a prior session")
            connection.execute("COMMIT")

    def begin(self, *, official: Mapping[str, Any], coverage: float) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """INSERT INTO authority_meta VALUES(
                   1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    authority_hash(self.config), coverage, 0.0, 0.0, coverage,
                    0, ZERO_HASH, ZERO_HASH, 0, 0, 0, 0, 0, 0, 0, 0, "",
                    _canonical(dict(official)), "",
                ),
            )
            connection.execute("COMMIT")

    def authenticated(self, *, subscribed: float, process_audit: Mapping[str, Any]) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """UPDATE authority_meta SET subscribed_epoch=?,
                   last_liveness_epoch=?,observed_epoch=?,connected=1,
                   authenticated=1,process_audit_json=? WHERE singleton=1""",
                (subscribed, subscribed, subscribed, _canonical(process_audit)),
            )
            connection.execute("COMMIT")

    def liveness(self, now: float, process_audit: Mapping[str, Any]) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """UPDATE authority_meta SET last_liveness_epoch=?,observed_epoch=?,
                   process_audit_json=? WHERE singleton=1 AND revoked=0""",
                (now, now, _canonical(process_audit)),
            )
            connection.execute("COMMIT")

    def event(self, event: Mapping[str, Any], *, now: float) -> bool:
        client_ids = _order_client_ids(event)
        owned = all(
            item.startswith(self.config["ownerClientOrderPrefix"])
            for item in client_ids
        )
        order_event = bool(client_ids)
        payload = _canonical(dict(event)).encode("utf-8")
        if len(payload) > MAX_EVENT_BYTES:
            raise RuntimeError("observer user-data event is too large")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            meta = connection.execute(
                "SELECT * FROM authority_meta WHERE singleton=1"
            ).fetchone()
            if meta is None or int(meta["revoked"]):
                raise RuntimeError("observer authority is already revoked")
            sequence = int(meta["event_count"]) + 1
            body = {
                "sequence": sequence,
                "receivedEpoch": now,
                "eventType": _text(event.get("e") or event.get("eventType")),
                "payloadHash": hashlib.sha256(payload).hexdigest(),
                "clientIdsHash": authority_hash(sorted(client_ids)),
                "owned": owned,
                "previousEventHash": _text(meta["event_chain_hash"]),
            }
            event_hash = authority_hash(body)
            connection.execute(
                "INSERT INTO authority_events VALUES(?,?,?,?,?,?,?,?)",
                (
                    sequence, now, body["eventType"], body["payloadHash"],
                    body["clientIdsHash"], 1 if owned else 0,
                    body["previousEventHash"], event_hash,
                ),
            )
            connection.execute(
                """UPDATE authority_meta SET observed_epoch=?,event_chain_hash=?,
                   event_count=event_count+1,
                   order_event_count=order_event_count+?,
                   unowned_order_event_count=unowned_order_event_count+?
                   WHERE singleton=1""",
                (now, event_hash, 1 if order_event else 0,
                 1 if order_event and not owned else 0),
            )
            connection.execute("COMMIT")
        return not (order_event and not owned)

    def revoke(self, reason: str, *, crash: bool = False) -> None:
        now = time.time()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """UPDATE authority_meta SET observed_epoch=?,connected=0,
                   gap_detected=1,crash_detected=?,revoked=1,revoke_reason=?
                   WHERE singleton=1""",
                (now, 1 if crash else 0, _text(reason)[:500]),
            )
            connection.execute("COMMIT")

    def row(self) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM authority_meta WHERE singleton=1"
            ).fetchone()
        if row is None:
            raise RuntimeError("observer authority database is empty")
        return dict(row)

    def advance_snapshot(self, snapshot_hash: str) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """UPDATE authority_meta SET authority_sequence=authority_sequence+1,
                   previous_snapshot_hash=? WHERE singleton=1""",
                (snapshot_hash,),
            )
            connection.execute("COMMIT")


class Observer:
    def __init__(
        self,
        *,
        config: Mapping[str, Any],
        journal: AuthorityJournal,
        private_key: ECC.EccKey,
        snapshot_path: Path,
    ) -> None:
        self.config = dict(config)
        self.journal = journal
        self.private_key = private_key
        self.snapshot_path = snapshot_path.resolve()
        self.process_identity_hash = authority_hash(
            {"pid": os.getpid(), "startedEpoch": time.time(), "executable": sys.executable}
        )

    def publish(self) -> None:
        row = self.journal.row()
        official = json.loads(row["official_json"])
        process = json.loads(row["process_audit_json"] or "{}")
        stream_body = {
            "schemaVersion": STREAM_AUDIT_SCHEMA_VERSION,
            "transportKind": "SIGNED_WS_API_USER_DATA_STREAM",
            "listenKeyRequired": False,
            "subscriptionAuthenticated": bool(row["authenticated"]),
            "connected": bool(row["connected"]),
            "gapDetected": bool(row["gap_detected"]),
            "crashDetected": bool(row["crash_detected"]),
            "continuousCoverage": bool(
                row["authenticated"] and not row["gap_detected"]
            ),
            "sessionId": self.config["sessionId"],
            "permitId": self.config["permitId"],
            "permitHash": self.config["permitHash"],
            "ownerClientOrderPrefix": self.config["ownerClientOrderPrefix"],
            "subscribedEpoch": float(row["subscribed_epoch"]),
            "lastLivenessEpoch": float(row["last_liveness_epoch"] or row["observed_epoch"]),
            "eventCount": int(row["event_count"]),
            "orderEventCount": int(row["order_event_count"]),
            "unownedOrderEventCount": int(row["unowned_order_event_count"]),
            "eventChainHash": _text(row["event_chain_hash"]),
            "journalDatabaseIdentityHash": authority_hash(
                {"path": str(self.journal.path), "configHash": row["config_hash"]}
            ),
        }
        stream = {**stream_body, "auditHash": authority_hash(stream_body)}
        revoked = bool(row["revoked"])
        body = {
            "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
            "assuranceMode": ASSURANCE_MODE,
            "authorityId": self.config["authorityId"],
            "keyId": self.config["keyId"],
            "authorityProcessIdentityHash": self.process_identity_hash,
            "sessionId": self.config["sessionId"],
            "permitId": self.config["permitId"],
            "permitHash": self.config["permitHash"],
            "credentialFingerprint": self.config["credentialFingerprint"],
            "ownerClientOrderPrefix": self.config["ownerClientOrderPrefix"],
            "coverageStartedEpoch": float(row["coverage_started_epoch"]),
            "observedEpoch": float(row["observed_epoch"]),
            "authoritySequence": int(row["authority_sequence"]) + 1,
            "previousSnapshotHash": _text(row["previous_snapshot_hash"]),
            "officialBaseline": official,
            "processAudit": process,
            "userDataStreamAudit": stream,
            "revoked": revoked,
            "cleanupOnlyRequired": revoked,
            "revokeReason": _text(row["revoke_reason"]),
            "otherApiKeyInventoryProven": False,
            "manualOrderCausalAuditIndependentlyVerified": not revoked,
            "botRegistryIndependentlyVerified": not revoked,
            "accountWideCausalClosureProven": False,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
        }
        payload = {**body, "payloadHash": authority_hash(body)}
        signature = eddsa.new(self.private_key, "rfc8032").sign(
            canonical_authority_message(payload)
        )
        envelope = {
            **payload,
            "signature": base64.urlsafe_b64encode(signature).decode("ascii").rstrip("="),
        }
        encoded = _canonical(envelope).encode("utf-8")
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.snapshot_path.with_name(
            "." + self.snapshot_path.name + "." + uuid.uuid4().hex + ".tmp"
        )
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.snapshot_path)
        self.journal.advance_snapshot(authority_hash(envelope))

    def run(self) -> int:
        import websocket  # type: ignore[import-not-found]

        actual_fingerprint = binance_api_key_fingerprint(env_value("BINANCE_API_KEY"))
        if actual_fingerprint != self.config["credentialFingerprint"]:
            raise RuntimeError("observer Binance credential changed")
        audit = _process_audit(self.config, time.time())
        api_key = env_value("BINANCE_API_KEY")
        api_secret = env_value("BINANCE_API_SECRET")
        socket = websocket.create_connection(
            "wss://ws-api.binance.com:443/ws-api/v3", timeout=2
        )
        subscription_id = str(uuid.uuid4())
        socket.send(
            _canonical(
                {
                    "id": subscription_id,
                    "method": "userDataStream.subscribe.signature",
                    "params": binance_stream_subscription_params(api_key, api_secret),
                }
            )
        )
        raw = socket.recv()
        ack = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        if ack.get("id") != subscription_id or int(ack.get("status") or 0) != 200:
            socket.close()
            raise RuntimeError("observer user stream authentication failed")
        subscribed = time.time()
        # Establish account-event coverage before the open-order baseline.
        # Frames arriving while the three no-retry GETs run remain queued on
        # this authenticated socket and are consumed by the sole reader below.
        # Reversing this order would leave an unobserved manual/bot order race
        # between GET /openOrders and the user-data subscription ACK.
        try:
            official = BinanceSpotSupervisedOfficialGetProvider(
                network_capability=(
                    _protected_binance_spot_supervised_get_network_capability()
                )
            )()
            if (
                float(official["observedEpoch"]) < subscribed
                or float(official["observedEpoch"]) - subscribed
                > LIVENESS_SECONDS
            ):
                raise RuntimeError(
                    "observer official baseline exceeded subscribed coverage window"
                )
        except BaseException:
            socket.close()
            raise
        self.journal.begin(official=official, coverage=subscribed)
        self.journal.authenticated(subscribed=subscribed, process_audit=audit)
        # Drain every frame already delivered during the GET baseline before
        # publishing the first healthy snapshot.  The time RPC is only a
        # liveness/drain boundary (Binance does not document it as an
        # account-wide causal barrier, so causalClosure remains false).
        initial_liveness_id = str(uuid.uuid4())
        initial_liveness_deadline = time.monotonic() + LIVENESS_SECONDS
        socket.send(_canonical({"id": initial_liveness_id, "method": "time"}))
        while True:
            if time.monotonic() >= initial_liveness_deadline:
                self.journal.revoke(
                    "observer initial subscribed-baseline drain timed out",
                    crash=True,
                )
                self.publish()
                socket.close()
                return 5
            try:
                raw = socket.recv()
            except Exception as exc:
                if type(exc).__name__ == "WebSocketTimeoutException":
                    continue
                self.journal.revoke(
                    "observer initial subscribed-baseline drain failed",
                    crash=True,
                )
                self.publish()
                socket.close()
                raise
            payload = json.loads(
                raw.decode("utf-8") if isinstance(raw, bytes) else raw
            )
            if payload.get("id") == initial_liveness_id:
                if int(payload.get("status") or 0) != 200:
                    self.journal.revoke(
                        "observer initial liveness response invalid",
                        crash=True,
                    )
                    self.publish()
                    socket.close()
                    return 5
                now = time.time()
                audit = _process_audit(self.config, now)
                self.journal.liveness(now, audit)
                break
            event = _event_object(payload)
            if event is not None:
                owned = self.journal.event(event, now=time.time())
                if not owned:
                    self.journal.revoke(
                        "unowned account order/execution event observed during baseline"
                    )
                    self.publish()
                    socket.close()
                    return 3
        self.publish()
        started = time.monotonic()
        last_publish = 0.0
        last_process_audit = 0.0
        liveness_id = ""
        liveness_sent = 0.0
        try:
            while time.monotonic() - started < self.config["maxRuntimeSeconds"]:
                now_mono = time.monotonic()
                if now_mono - last_process_audit >= PROCESS_AUDIT_INTERVAL_SECONDS:
                    audit = _process_audit(self.config, time.time())
                    last_process_audit = now_mono
                try:
                    raw = socket.recv()
                except Exception as exc:
                    if type(exc).__name__ != "WebSocketTimeoutException":
                        raise
                    if liveness_id and now_mono - liveness_sent >= LIVENESS_SECONDS:
                        raise RuntimeError("observer stream liveness gap")
                    if not liveness_id:
                        liveness_id = str(uuid.uuid4())
                        liveness_sent = now_mono
                        socket.send(_canonical({"id": liveness_id, "method": "time"}))
                    raw = None
                if raw is not None:
                    payload = json.loads(
                        raw.decode("utf-8") if isinstance(raw, bytes) else raw
                    )
                    if liveness_id and payload.get("id") == liveness_id:
                        if int(payload.get("status") or 0) != 200:
                            raise RuntimeError("observer liveness response invalid")
                        now = time.time()
                        audit = _process_audit(self.config, now)
                        self.journal.liveness(now, audit)
                        liveness_id = ""
                    else:
                        event = _event_object(payload)
                        if event is not None:
                            owned = self.journal.event(event, now=time.time())
                            if not owned:
                                self.journal.revoke(
                                    "unowned account order/execution event observed"
                                )
                                self.publish()
                                return 3
                if now_mono - last_publish >= SNAPSHOT_INTERVAL_SECONDS:
                    self.publish()
                    last_publish = now_mono
            self.journal.revoke("observer maximum runtime reached")
            self.publish()
            return 4
        except BaseException as exc:
            self.journal.revoke(
                "observer stream/process gap:" + type(exc).__name__, crash=True
            )
            self.publish()
            raise
        finally:
            socket.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--private-key", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--credential-file", required=True)
    parser.add_argument("--prearmed-ready-file", required=True)
    parser.add_argument("--start-gate", required=True)
    args = parser.parse_args()
    config = _config(_strict_json(Path(args.config).resolve()))
    credential = _load_protected_credentials(
        Path(args.credential_file).resolve(),
        authority_id=config["credentialAuthorityId"],
        manifest_sha256=config["bundleManifestSha256"],
        account_fingerprint=config["credentialFingerprint"],
        credential_generation_id=config["credentialGenerationId"],
        expected_envelope_hash=config["credentialEnvelopeHash"],
    )
    if credential["credentialFingerprint"] != config["credentialFingerprint"]:
        raise RuntimeError("observer config/credential fingerprint changed")
    lease = hold_process_lease(
        "binance-supervised-observer:" + config["credentialFingerprint"]
    )
    if lease.get("acquired") is not True:
        raise RuntimeError("another supervised observer process owns the account")
    key = _load_private_key(Path(args.private_key).resolve())
    _write_prearmed_ready(
        Path(args.prearmed_ready_file).resolve(), config=config
    )
    _wait_for_start_gate(Path(args.start_gate).resolve())
    journal = AuthorityJournal(Path(args.database), config=config)
    observer = Observer(
        config=config,
        journal=journal,
        private_key=key,
        snapshot_path=Path(args.snapshot),
    )
    return observer.run()


if __name__ == "__main__":
    raise SystemExit(main())
