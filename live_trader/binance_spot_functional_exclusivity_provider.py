from __future__ import annotations

"""Public-key-only durable provider for Binance exclusivity evidence.

The trading process publishes an exact, hash-addressed request envelope and
consumes only an independently written Ed25519 proof.  It has no private key,
signing callback, broker transport, or release switch.  A SQLite cursor binds
the signed authority sequence to an append-only local event chain so a valid
but non-head proof cannot be replayed after restart.
"""

import base64
from contextlib import closing
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import types
from typing import Any, Callable, Mapping

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from .binance_spot_functional_exclusivity import (
    MAX_PROOF_AGE_SECONDS,
    PROOF_REQUEST_SCHEMA_VERSION,
    PROOF_SCHEMA_VERSION,
    VERIFIER_PIN_SCHEMA_VERSION,
    BinanceSpotExclusivityError,
    BinanceSpotExclusivityVerifier,
    _is_hash,
    _stable_hash,
    _text,
    _utc_epoch,
    exclusivity_proof_request_payload,
    normalize_verifier_pin,
    verifier_wiring_status,
    verify_exclusivity_proof,
)


BINANCE_SPOT_EXCLUSIVITY_PROVIDER_PRODUCTION_RELEASED = False
BINANCE_SPOT_EXCLUSIVITY_PROVIDER_NETWORK_ALLOWED = False
BINANCE_SPOT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT = False
PROVIDER_SCHEMA_VERSION = "binance-spot-exclusivity-durable-provider/v1"
OUTBOX_SCHEMA_VERSION = "binance-spot-exclusivity-proof-request-outbox/v1"
CURSOR_SCHEMA_VERSION = "binance-spot-exclusivity-consumer-cursor/v1"
VERIFIER_TYPE = "PINNED_ED25519_DURABLE_BINANCE_EXCLUSIVITY_V1"
ALGORITHM = "ED25519_RFC8032_SHA512"
MAX_PROOF_FILE_BYTES = 256 * 1024
_ZERO_HASH = "0" * 64
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
_SIGNATURE_RE = re.compile(r"^[A-Za-z0-9_-]{86}$")
_CURSOR_APPLICATION_ID = 0x424E5845
_CURSOR_USER_VERSION = 1
_CURSOR_TABLE_SQL = """CREATE TABLE binance_exclusivity_cursor (
    session_id TEXT NOT NULL,
    account_identity_fingerprint TEXT NOT NULL,
    credential_fingerprint TEXT NOT NULL,
    server_owner_identity_sha256 TEXT NOT NULL,
    authority_journal_id TEXT NOT NULL,
    authority_sequence INTEGER NOT NULL,
    proof_hash TEXT NOT NULL,
    proof_request_hash TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    last_phase TEXT NOT NULL,
    terminal_verified INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    row_hash TEXT NOT NULL
)"""
_CURSOR_SESSION_INDEX_SQL = """CREATE UNIQUE INDEX
binance_exclusivity_cursor_session_uq
ON binance_exclusivity_cursor(session_id)"""
_CURSOR_EVENT_TABLE_SQL = """CREATE TABLE binance_exclusivity_cursor_event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    authority_sequence INTEGER NOT NULL,
    phase TEXT NOT NULL,
    proof_hash TEXT NOT NULL,
    proof_request_hash TEXT NOT NULL,
    previous_authority_proof_hash TEXT NOT NULL,
    previous_event_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL
)"""
_CURSOR_EVENT_SEQUENCE_INDEX_SQL = """CREATE UNIQUE INDEX
binance_exclusivity_event_session_sequence_uq
ON binance_exclusivity_cursor_event(session_id, authority_sequence)"""
_CURSOR_EVENT_REQUEST_INDEX_SQL = """CREATE UNIQUE INDEX
binance_exclusivity_event_session_request_uq
ON binance_exclusivity_cursor_event(session_id, proof_request_hash)"""
_READER_FIELDS = frozenset(
    {
        "phase",
        "sessionId",
        "permitId",
        "permitHash",
        "accountIdentityFingerprint",
        "credentialFingerprint",
        "boundaryId",
        "boundaryHash",
        "coverageStartedAt",
        "requestedAt",
        "requireCausalClosure",
    }
)


def _normalized_sql(value: object) -> str:
    return " ".join(_text(value).split())


def _quoted_identifier(value: object) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _expected_cursor_schema_snapshot() -> dict[str, Any]:
    def columns(*rows: tuple[str, str, int, int]) -> list[dict[str, Any]]:
        return [
            {
                "cid": index,
                "name": name,
                "type": column_type,
                "notNull": not_null,
                "default": None,
                "primaryKey": primary_key,
            }
            for index, (name, column_type, not_null, primary_key) in enumerate(
                rows
            )
        ]

    return {
        "applicationId": _CURSOR_APPLICATION_ID,
        "userVersion": _CURSOR_USER_VERSION,
        "journalMode": "wal",
        "objects": sorted(
            [
                {
                    "type": "table",
                    "name": "sqlite_sequence",
                    "tableName": "sqlite_sequence",
                    "sql": "CREATE TABLE sqlite_sequence(name,seq)",
                },
                {
                    "type": "table",
                    "name": "binance_exclusivity_cursor",
                    "tableName": "binance_exclusivity_cursor",
                    "sql": _normalized_sql(_CURSOR_TABLE_SQL),
                },
                {
                    "type": "index",
                    "name": "binance_exclusivity_cursor_session_uq",
                    "tableName": "binance_exclusivity_cursor",
                    "sql": _normalized_sql(_CURSOR_SESSION_INDEX_SQL),
                },
                {
                    "type": "table",
                    "name": "binance_exclusivity_cursor_event",
                    "tableName": "binance_exclusivity_cursor_event",
                    "sql": _normalized_sql(_CURSOR_EVENT_TABLE_SQL),
                },
                {
                    "type": "index",
                    "name": (
                        "binance_exclusivity_event_session_request_uq"
                    ),
                    "tableName": "binance_exclusivity_cursor_event",
                    "sql": _normalized_sql(
                        _CURSOR_EVENT_REQUEST_INDEX_SQL
                    ),
                },
                {
                    "type": "index",
                    "name": (
                        "binance_exclusivity_event_session_sequence_uq"
                    ),
                    "tableName": "binance_exclusivity_cursor_event",
                    "sql": _normalized_sql(
                        _CURSOR_EVENT_SEQUENCE_INDEX_SQL
                    ),
                },
            ],
            key=lambda row: (row["type"], row["name"]),
        ),
        "tables": {
            "sqlite_sequence": {
                "columns": columns(("name", "", 0, 0), ("seq", "", 0, 0)),
                "foreignKeys": [],
                "indexes": [],
            },
            "binance_exclusivity_cursor": {
                "columns": columns(
                    ("session_id", "TEXT", 1, 0),
                    ("account_identity_fingerprint", "TEXT", 1, 0),
                    ("credential_fingerprint", "TEXT", 1, 0),
                    ("server_owner_identity_sha256", "TEXT", 1, 0),
                    ("authority_journal_id", "TEXT", 1, 0),
                    ("authority_sequence", "INTEGER", 1, 0),
                    ("proof_hash", "TEXT", 1, 0),
                    ("proof_request_hash", "TEXT", 1, 0),
                    ("observed_at", "TEXT", 1, 0),
                    ("last_phase", "TEXT", 1, 0),
                    ("terminal_verified", "INTEGER", 1, 0),
                    ("revision", "INTEGER", 1, 0),
                    ("row_hash", "TEXT", 1, 0),
                ),
                "foreignKeys": [],
                "indexes": [
                    {
                        "name": "binance_exclusivity_cursor_session_uq",
                        "unique": 1,
                        "origin": "c",
                        "partial": 0,
                        "columns": ["session_id"],
                    }
                ],
            },
            "binance_exclusivity_cursor_event": {
                "columns": columns(
                    ("event_id", "INTEGER", 0, 1),
                    ("session_id", "TEXT", 1, 0),
                    ("authority_sequence", "INTEGER", 1, 0),
                    ("phase", "TEXT", 1, 0),
                    ("proof_hash", "TEXT", 1, 0),
                    ("proof_request_hash", "TEXT", 1, 0),
                    ("previous_authority_proof_hash", "TEXT", 1, 0),
                    ("previous_event_hash", "TEXT", 1, 0),
                    ("event_hash", "TEXT", 1, 0),
                ),
                "foreignKeys": [],
                "indexes": sorted(
                    [
                        {
                            "name": (
                                "binance_exclusivity_event_session_request_uq"
                            ),
                            "unique": 1,
                            "origin": "c",
                            "partial": 0,
                            "columns": ["session_id", "proof_request_hash"],
                        },
                        {
                            "name": (
                                "binance_exclusivity_event_session_sequence_uq"
                            ),
                            "unique": 1,
                            "origin": "c",
                            "partial": 0,
                            "columns": ["session_id", "authority_sequence"],
                        },
                    ],
                    key=lambda row: row["name"],
                ),
            },
        },
    }


BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT = _stable_hash(
    _expected_cursor_schema_snapshot()
)


def canonical_exclusivity_signature_message(
    payload: Mapping[str, Any],
) -> bytes:
    return json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _require_hash(value: object, label: str) -> str:
    text = _text(value).lower()
    if type(value) is not str or value != text or not _is_hash(text):
        raise BinanceSpotExclusivityError(f"{label} is not an exact SHA-256")
    return text


def _require_id(value: object, label: str) -> str:
    text = _text(value)
    if (
        type(value) is not str
        or value != text
        or _SAFE_ID_RE.fullmatch(text) is None
    ):
        raise BinanceSpotExclusivityError(f"{label} is invalid")
    return text


def _code_value(value: object) -> object:
    if isinstance(value, types.CodeType):
        return {
            "bytecode": value.co_code.hex(),
            "constants": [_code_value(item) for item in value.co_consts],
            "names": list(value.co_names),
            "variables": list(value.co_varnames),
            "argcount": value.co_argcount,
            "kwonlyargcount": value.co_kwonlyargcount,
        }
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}"}


def _verifier_code_sha256() -> str:
    methods = (
        PinnedEd25519BinanceSpotExclusivityVerifier.identity,
        PinnedEd25519BinanceSpotExclusivityVerifier.__call__,
        PinnedEd25519BinanceSpotExclusivityVerifier._payload_context_exact,
    )
    return _stable_hash(
        {
            "contract": "binance-pinned-ed25519-exclusivity-verifier/v1",
            "methods": [_code_value(method.__code__) for method in methods],
        }
    )


class PinnedEd25519BinanceSpotExclusivityVerifier:
    """Pinned verifier containing an Ed25519 public key and no private key."""

    def __init__(
        self,
        *,
        public_key: bytes | str,
        verifier_id: str,
        key_id: str,
        authority_journal_id: str,
        expected_account_identity_fingerprint: str,
        expected_credential_fingerprint: str,
        expected_server_owner_identity_sha256: str,
    ) -> None:
        self.verifier_id = _require_id(verifier_id, "verifier id")
        self.key_id = _require_id(key_id, "key id")
        self.authority_journal_id = _require_id(
            authority_journal_id, "authority journal id"
        )
        self.expected_account_identity_fingerprint = _require_hash(
            expected_account_identity_fingerprint,
            "expected account identity fingerprint",
        )
        self.expected_credential_fingerprint = _require_hash(
            expected_credential_fingerprint,
            "expected credential fingerprint",
        )
        self.expected_server_owner_identity_sha256 = _require_hash(
            expected_server_owner_identity_sha256,
            "expected server owner identity",
        )
        if hmac.compare_digest(
            self.expected_account_identity_fingerprint,
            self.expected_credential_fingerprint,
        ):
            raise BinanceSpotExclusivityError(
                "account identity and credential fingerprints must be independent"
            )
        try:
            public = ECC.import_key(public_key)
        except (ValueError, TypeError, IndexError) as exc:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity public key is invalid"
            ) from exc
        if public.has_private() or getattr(public, "curve", None) != "Ed25519":
            raise BinanceSpotExclusivityError(
                "public-key-only Ed25519 verifier is required"
            )
        self._public_key = public
        public_fingerprint = hashlib.sha256(
            public.export_key(format="DER")
        ).hexdigest()
        config = {
            "schemaVersion": "binance-exclusivity-verifier-config/v1",
            "verifierId": self.verifier_id,
            "keyId": self.key_id,
            "authorityJournalId": self.authority_journal_id,
            "algorithm": ALGORITHM,
            "verifierType": VERIFIER_TYPE,
            "accountIdentityFingerprint": (
                self.expected_account_identity_fingerprint
            ),
            "credentialFingerprint": self.expected_credential_fingerprint,
            "serverOwnerIdentitySha256": (
                self.expected_server_owner_identity_sha256
            ),
            "maxProofAgeSeconds": MAX_PROOF_AGE_SECONDS,
            "publicKeyFingerprintSha256": public_fingerprint,
        }
        self._pin = {
            "schemaVersion": VERIFIER_PIN_SCHEMA_VERSION,
            "verifierId": self.verifier_id,
            "keyId": self.key_id,
            "algorithm": ALGORITHM,
            "verifierType": VERIFIER_TYPE,
            "verifierCodeSha256": _verifier_code_sha256(),
            "verifierConfigSha256": _stable_hash(config),
            "keyFingerprintSha256": public_fingerprint,
            "authorityPinned": True,
        }

    def identity(self) -> Mapping[str, Any]:
        return dict(self._pin)

    def _payload_context_exact(self, payload: Mapping[str, Any]) -> bool:
        return bool(
            payload.get("schemaVersion") == PROOF_SCHEMA_VERSION
            and hmac.compare_digest(
                _text(payload.get("accountIdentityFingerprint")).lower(),
                self.expected_account_identity_fingerprint,
            )
            and hmac.compare_digest(
                _text(payload.get("credentialFingerprint")).lower(),
                self.expected_credential_fingerprint,
            )
            and hmac.compare_digest(
                _text(payload.get("serverOwnerIdentitySha256")).lower(),
                self.expected_server_owner_identity_sha256,
            )
            and hmac.compare_digest(
                _text(payload.get("authorityJournalId")),
                self.authority_journal_id,
            )
            and isinstance(payload.get("authority"), Mapping)
            and dict(payload["authority"]) == self._pin
        )

    def __call__(
        self,
        *,
        payload: Mapping[str, Any],
        signature: str,
        verifier_pin: Mapping[str, Any],
    ) -> bool:
        if (
            normalize_verifier_pin(verifier_pin) != self._pin
            or not self._payload_context_exact(payload)
            or type(signature) is not str
            or _SIGNATURE_RE.fullmatch(signature) is None
        ):
            return False
        try:
            raw_signature = base64.urlsafe_b64decode(signature + "==")
            if len(raw_signature) != 64:
                return False
            eddsa.new(self._public_key, "rfc8032").verify(
                canonical_exclusivity_signature_message(payload),
                raw_signature,
            )
        except (ValueError, TypeError):
            return False
        return True


def _strict_json_file(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise BinanceSpotExclusivityError(
            "Binance exclusivity proof/pin file is missing or a link"
        )
    size = path.stat().st_size
    if size <= 1 or size > MAX_PROOF_FILE_BYTES:
        raise BinanceSpotExclusivityError(
            "Binance exclusivity proof/pin file size is invalid"
        )

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        raw = path.read_bytes()
        if len(raw) != size or raw.startswith(b"\xef\xbb\xbf"):
            raise ValueError("unstable or BOM-prefixed JSON")
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON")
            ),
        )
    except (OSError, UnicodeDecodeError, ValueError, TypeError) as exc:
        raise BinanceSpotExclusivityError(
            "Binance exclusivity proof/pin JSON is invalid"
        ) from exc
    if not isinstance(value, dict):
        raise BinanceSpotExclusivityError(
            "Binance exclusivity proof/pin JSON is not an object"
        )
    return value


class DurableBinanceSpotExclusivityProofProvider:
    """Hash-addressed proof-file reader plus durable signed-head cursor."""

    def __init__(
        self,
        *,
        proof_directory: str | Path,
        cursor_database_path: str | Path,
        verifier: PinnedEd25519BinanceSpotExclusivityVerifier,
        expected_verifier_pin: Mapping[str, Any],
        account_identity_reader: Callable[[], str],
        credential_fingerprint_reader: Callable[[], str],
        server_owner_identity_reader: Callable[[], str],
        clock: Callable[[], float] = time.time,
        proof_wait_seconds: float = 0.0,
    ) -> None:
        raw_directory = Path(proof_directory)
        proof_directory_absolute = raw_directory.absolute()
        if (
            raw_directory.is_symlink()
            or not proof_directory_absolute.is_dir()
            or proof_directory_absolute.resolve()
            != proof_directory_absolute
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity proof directory is a link"
            )
        self.proof_directory = proof_directory_absolute.resolve()
        self._cursor_raw_path = Path(cursor_database_path).absolute()
        if (
            self._cursor_raw_path.is_symlink()
            or (
                self._cursor_raw_path.exists()
                and not self._cursor_raw_path.is_file()
            )
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor path is invalid"
            )
        raw_parent = self._cursor_raw_path.parent
        raw_parent.mkdir(parents=True, exist_ok=True)
        if (
            raw_parent.is_symlink()
            or raw_parent.absolute() != raw_parent.resolve()
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor parent contains a link"
            )
        self.cursor_database_path = (
            raw_parent.resolve() / self._cursor_raw_path.name
        )
        self._cursor_file_identity: tuple[int, int] | None = None
        pin = normalize_verifier_pin(expected_verifier_pin)
        if pin is None or pin != dict(verifier.identity()):
            raise BinanceSpotExclusivityError(
                "durable Binance exclusivity verifier pin mismatch"
            )
        self.verifier = verifier
        self.verifier_pin = pin
        self.account_identity_reader = account_identity_reader
        self.credential_fingerprint_reader = credential_fingerprint_reader
        self.server_owner_identity_reader = server_owner_identity_reader
        self.clock = clock
        try:
            wait = float(proof_wait_seconds)
        except (TypeError, ValueError) as exc:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity proof wait is invalid"
            ) from exc
        if (
            isinstance(proof_wait_seconds, bool)
            or not math.isfinite(wait)
            or wait < 0
            or wait > 5
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity proof wait is invalid"
            )
        self.proof_wait_seconds = wait
        self._lock = threading.RLock()
        self._last_failure = ""
        self._initialize_cursor()

    @staticmethod
    def _file_identity(path: Path) -> tuple[int, int]:
        try:
            stat = path.stat()
        except OSError as exc:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor path is unavailable"
            ) from exc
        identity = (int(stat.st_dev), int(stat.st_ino))
        if identity[1] <= 0:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor file identity is unavailable"
            )
        return identity

    def _assert_cursor_path_exact(self) -> tuple[int, int]:
        raw = self._cursor_raw_path
        try:
            if (
                raw.parent.is_symlink()
                or raw.parent.absolute() != raw.parent.resolve()
                or raw.is_symlink()
                or not raw.is_file()
                or raw.resolve() != self.cursor_database_path
            ):
                raise BinanceSpotExclusivityError(
                    "Binance exclusivity cursor path drifted"
                )
        except OSError as exc:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor path is unavailable"
            ) from exc
        identity = self._file_identity(raw)
        if (
            self._cursor_file_identity is not None
            and identity != self._cursor_file_identity
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor file was replaced"
            )
        return identity

    def _assert_connection_path_exact(
        self, connection: sqlite3.Connection
    ) -> None:
        rows = connection.execute("PRAGMA database_list").fetchall()
        if len(rows) != 1:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor database list is invalid"
            )
        row = dict(rows[0])
        try:
            actual = Path(str(row.get("file") or "")).resolve()
        except OSError as exc:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity connected cursor path is invalid"
            ) from exc
        if row.get("name") != "main" or actual != self.cursor_database_path:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity connected cursor path is invalid"
            )

    @staticmethod
    def _schema_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
        objects = [
            {
                "type": str(row["type"]),
                "name": str(row["name"]),
                "tableName": str(row["tbl_name"]),
                "sql": _normalized_sql(row["sql"]),
            }
            for row in connection.execute(
                """SELECT type,name,tbl_name,sql FROM sqlite_master
                   ORDER BY type,name"""
            ).fetchall()
        ]
        tables: dict[str, Any] = {}
        table_names = sorted(
            row["name"] for row in objects if row["type"] == "table"
        )
        for table_name in table_names:
            quoted_table = _quoted_identifier(table_name)
            columns = [
                {
                    "cid": int(row["cid"]),
                    "name": str(row["name"]),
                    "type": str(row["type"]),
                    "notNull": int(row["notnull"]),
                    "default": row["dflt_value"],
                    "primaryKey": int(row["pk"]),
                }
                for row in connection.execute(
                    f"PRAGMA table_info({quoted_table})"
                ).fetchall()
            ]
            foreign_keys = [
                dict(row)
                for row in connection.execute(
                    f"PRAGMA foreign_key_list({quoted_table})"
                ).fetchall()
            ]
            indexes: list[dict[str, Any]] = []
            for raw_index in connection.execute(
                f"PRAGMA index_list({quoted_table})"
            ).fetchall():
                index = dict(raw_index)
                index_name = str(index["name"])
                quoted_index = _quoted_identifier(index_name)
                indexes.append(
                    {
                        "name": index_name,
                        "unique": int(index["unique"]),
                        "origin": str(index["origin"]),
                        "partial": int(index["partial"]),
                        "columns": [
                            str(row["name"])
                            for row in connection.execute(
                                f"PRAGMA index_info({quoted_index})"
                            ).fetchall()
                        ],
                    }
                )
            tables[table_name] = {
                "columns": columns,
                "foreignKeys": foreign_keys,
                "indexes": sorted(indexes, key=lambda row: row["name"]),
            }
        return {
            "applicationId": int(
                connection.execute("PRAGMA application_id").fetchone()[0]
            ),
            "userVersion": int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            ),
            "journalMode": str(
                connection.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower(),
            "objects": objects,
            "tables": tables,
        }

    def _validate_cursor_schema(
        self, connection: sqlite3.Connection
    ) -> str:
        snapshot = self._schema_snapshot(connection)
        fingerprint = _stable_hash(snapshot)
        if (
            snapshot != _expected_cursor_schema_snapshot()
            or not hmac.compare_digest(
                fingerprint,
                BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
            )
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor schema fingerprint is invalid"
            )
        return fingerprint

    def _connect(self) -> sqlite3.Connection:
        self._assert_cursor_path_exact()
        try:
            connection = sqlite3.connect(
                self.cursor_database_path, timeout=5.0, isolation_level=None
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("PRAGMA synchronous=FULL")
            self._assert_connection_path_exact(connection)
            self._assert_cursor_path_exact()
            self._validate_cursor_schema(connection)
            return connection
        except Exception:
            try:
                connection.close()
            except (NameError, sqlite3.Error):
                pass
            raise

    def _initialize_cursor(self) -> None:
        raw = self._cursor_raw_path
        fresh = not raw.exists()
        if raw.is_symlink() or (not fresh and not raw.is_file()):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity cursor path is invalid"
            )
        try:
            connection = sqlite3.connect(
                self.cursor_database_path, timeout=5.0, isolation_level=None
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("PRAGMA synchronous=FULL")
            self._assert_connection_path_exact(connection)
            if fresh:
                existing = connection.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master"
                ).fetchall()
                if existing:
                    raise BinanceSpotExclusivityError(
                        "Binance exclusivity cursor is not a fresh database"
                    )
                journal_mode = str(
                    connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                ).lower()
                if journal_mode != "wal":
                    raise BinanceSpotExclusivityError(
                        "Binance exclusivity cursor WAL mode is required"
                    )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_CURSOR_TABLE_SQL)
                    connection.execute(_CURSOR_SESSION_INDEX_SQL)
                    connection.execute(_CURSOR_EVENT_TABLE_SQL)
                    connection.execute(_CURSOR_EVENT_SEQUENCE_INDEX_SQL)
                    connection.execute(_CURSOR_EVENT_REQUEST_INDEX_SQL)
                    connection.execute(
                        f"PRAGMA application_id={_CURSOR_APPLICATION_ID}"
                    )
                    connection.execute(
                        f"PRAGMA user_version={_CURSOR_USER_VERSION}"
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
        except Exception:
            try:
                connection.close()
            except (NameError, sqlite3.Error):
                pass
            raise
        else:
            connection.close()
        self._cursor_file_identity = self._assert_cursor_path_exact()
        with closing(self._connect()) as verified:
            self._validate_cursor_schema(verified)

    @staticmethod
    def _row_body(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: row[key]
            for key in (
                "session_id",
                "account_identity_fingerprint",
                "credential_fingerprint",
                "server_owner_identity_sha256",
                "authority_journal_id",
                "authority_sequence",
                "proof_hash",
                "proof_request_hash",
                "observed_at",
                "last_phase",
                "terminal_verified",
                "revision",
            )
        }

    @classmethod
    def _row_valid(cls, row: Mapping[str, Any]) -> bool:
        try:
            return hmac.compare_digest(
                _text(row.get("row_hash")), _stable_hash(cls._row_body(row))
            )
        except (KeyError, TypeError, ValueError):
            return False

    def request_descriptor(self, **kwargs: Any) -> dict[str, Any]:
        if (
            set(kwargs) != _READER_FIELDS
            or type(kwargs.get("requireCausalClosure")) is not bool
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity provider request fields are not exact"
            )
        account = _require_hash(
            self.account_identity_reader(), "current account identity"
        )
        credential = _require_hash(
            self.credential_fingerprint_reader(), "current credential"
        )
        owner = _require_hash(
            self.server_owner_identity_reader(), "current server owner"
        )
        if (
            not hmac.compare_digest(
                account, self.verifier.expected_account_identity_fingerprint
            )
            or not hmac.compare_digest(
                credential, self.verifier.expected_credential_fingerprint
            )
            or not hmac.compare_digest(
                owner, self.verifier.expected_server_owner_identity_sha256
            )
            or not hmac.compare_digest(
                account, _text(kwargs["accountIdentityFingerprint"]).lower()
            )
            or not hmac.compare_digest(
                credential, _text(kwargs["credentialFingerprint"]).lower()
            )
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity current account/credential/owner identity rotated"
            )
        coverage_epoch = _utc_epoch(
            kwargs["coverageStartedAt"], "provider coverageStartedAt"
        )
        requested_epoch = _utc_epoch(
            kwargs["requestedAt"], "provider requestedAt"
        )
        request = exclusivity_proof_request_payload(
            phase=kwargs["phase"],
            session_id=kwargs["sessionId"],
            permit_id=kwargs["permitId"],
            permit_hash=kwargs["permitHash"],
            account_identity_fingerprint=kwargs[
                "accountIdentityFingerprint"
            ],
            credential_fingerprint=kwargs["credentialFingerprint"],
            boundary_id=kwargs["boundaryId"],
            boundary_hash=kwargs["boundaryHash"],
            coverage_started_epoch=coverage_epoch,
            requested_epoch=requested_epoch,
            require_causal_closure=kwargs["requireCausalClosure"] is True,
        )
        if {key: value for key, value in request.items() if key != "schemaVersion"} != dict(
            kwargs
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity provider request is not canonical"
            )
        return {**request, "proofRequestHash": _stable_hash(request)}

    def _proof_path(self, request_hash: str) -> Path:
        path = self.proof_directory / f"{request_hash}.json"
        if path.resolve().parent != self.proof_directory:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity proof path escaped its directory"
            )
        return path

    def _publish_request(self, descriptor: Mapping[str, Any]) -> None:
        request_hash = _require_hash(
            descriptor.get("proofRequestHash"), "proof request hash"
        )
        body = {
            "schemaVersion": OUTBOX_SCHEMA_VERSION,
            "authorityJournalId": self.verifier.authority_journal_id,
            "verifierPinHash": _stable_hash(self.verifier_pin),
            "request": dict(descriptor),
        }
        envelope = {**body, "contentHash": _stable_hash(body)}
        encoded = json.dumps(
            envelope,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        path = self.proof_directory / f"{request_hash}.request.json"
        try:
            with path.open("xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            try:
                if path.is_symlink() or path.read_bytes() != encoded:
                    raise BinanceSpotExclusivityError(
                        "Binance exclusivity request outbox conflict"
                    )
            except OSError as exc:
                raise BinanceSpotExclusivityError(
                    "Binance exclusivity request outbox unavailable"
                ) from exc
        except OSError as exc:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity request outbox unavailable"
            ) from exc

    @staticmethod
    def _phase_transition(previous: str, current: str) -> bool:
        return bool(
            (previous == "BASELINE" and current == "ACTIVATION")
            or (
                previous in {"ACTIVATION", "PRE_POST"}
                and current in {"PRE_POST", "TERMINAL"}
            )
        )

    @classmethod
    def _validate_events_locked(
        cls, connection: sqlite3.Connection, session_id: str
    ) -> dict[str, Any]:
        rows = connection.execute(
            """SELECT authority_sequence,phase,proof_hash,proof_request_hash,
               previous_authority_proof_hash,previous_event_hash,event_hash
               FROM binance_exclusivity_cursor_event WHERE session_id=?
               ORDER BY authority_sequence""",
            (_text(session_id),),
        ).fetchall()
        previous_proof = _ZERO_HASH
        previous_event = _ZERO_HASH
        previous_phase = ""
        last: dict[str, Any] | None = None
        for index, raw in enumerate(rows, 1):
            row = dict(raw)
            body = {
                "schemaVersion": CURSOR_SCHEMA_VERSION,
                "sessionId": session_id,
                "authoritySequence": int(row["authority_sequence"]),
                "phase": row["phase"],
                "proofHash": row["proof_hash"],
                "proofRequestHash": row["proof_request_hash"],
                "previousAuthorityProofHash": row[
                    "previous_authority_proof_hash"
                ],
                "previousEventHash": row["previous_event_hash"],
            }
            phase = _text(row["phase"]).upper()
            if (
                int(row["authority_sequence"]) != index
                or row["previous_authority_proof_hash"] != previous_proof
                or row["previous_event_hash"] != previous_event
                or row["event_hash"] != _stable_hash(body)
                or (
                    index == 1
                    and phase != "BASELINE"
                )
                or (
                    index > 1
                    and not cls._phase_transition(previous_phase, phase)
                )
            ):
                raise BinanceSpotExclusivityError(
                    "durable Binance exclusivity event chain is tampered"
                )
            previous_proof = row["proof_hash"]
            previous_event = row["event_hash"]
            previous_phase = phase
            last = {
                "authoritySequence": index,
                "phase": phase,
                "proofHash": row["proof_hash"],
                "proofRequestHash": row["proof_request_hash"],
                "eventHash": row["event_hash"],
            }
        if last is None:
            raise BinanceSpotExclusivityError(
                "durable Binance exclusivity event chain is missing"
            )
        return last

    def _consume_cursor(
        self, *, proof: Mapping[str, Any], proof_hash: str
    ) -> None:
        session_id = _text(proof.get("sessionId"))
        sequence = int(proof.get("authoritySequence"))
        phase = _text(proof.get("phase")).upper()
        request_hash = _require_hash(
            proof.get("proofRequestHash"), "consumed proof request hash"
        )
        previous_proof = _require_hash(
            proof.get("previousAuthorityProofHash"),
            "previous authority proof hash",
        )
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._assert_cursor_path_exact()
                self._validate_cursor_schema(connection)
                raw = connection.execute(
                    "SELECT * FROM binance_exclusivity_cursor WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if raw is None:
                    if sequence != 1 or phase != "BASELINE" or previous_proof != _ZERO_HASH:
                        raise BinanceSpotExclusivityError(
                            "Binance exclusivity authority chain genesis is invalid"
                        )
                    revision = 1
                    previous_event = _ZERO_HASH
                else:
                    current = dict(raw)
                    if not self._row_valid(current):
                        raise BinanceSpotExclusivityError(
                            "durable Binance exclusivity cursor is tampered"
                        )
                    event = self._validate_events_locked(connection, session_id)
                    if (
                        event["authoritySequence"]
                        != int(current["authority_sequence"])
                        or event["proofHash"] != current["proof_hash"]
                        or event["proofRequestHash"]
                        != current["proof_request_hash"]
                    ):
                        raise BinanceSpotExclusivityError(
                            "durable Binance exclusivity cursor/event mismatch"
                        )
                    if (
                        current["proof_hash"] == proof_hash
                        and current["proof_request_hash"] == request_hash
                        and int(current["authority_sequence"]) == sequence
                    ):
                        self._validate_cursor_schema(connection)
                        self._assert_cursor_path_exact()
                        connection.commit()
                        return
                    if (
                        int(current["terminal_verified"]) == 1
                        or sequence != int(current["authority_sequence"]) + 1
                        or previous_proof != current["proof_hash"]
                        or not self._phase_transition(current["last_phase"], phase)
                        or _utc_epoch(proof.get("observedAt"), "proof observedAt")
                        <= _utc_epoch(current["observed_at"], "cursor observedAt")
                    ):
                        raise BinanceSpotExclusivityError(
                            "Binance exclusivity authority chain is discontinuous"
                        )
                    revision = int(current["revision"]) + 1
                    previous_event = event["eventHash"]
                event_body = {
                    "schemaVersion": CURSOR_SCHEMA_VERSION,
                    "sessionId": session_id,
                    "authoritySequence": sequence,
                    "phase": phase,
                    "proofHash": proof_hash,
                    "proofRequestHash": request_hash,
                    "previousAuthorityProofHash": previous_proof,
                    "previousEventHash": previous_event,
                }
                event_hash = _stable_hash(event_body)
                connection.execute(
                    """INSERT INTO binance_exclusivity_cursor_event
                    (session_id,authority_sequence,phase,proof_hash,
                     proof_request_hash,previous_authority_proof_hash,
                     previous_event_hash,event_hash) VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        session_id,
                        sequence,
                        phase,
                        proof_hash,
                        request_hash,
                        previous_proof,
                        previous_event,
                        event_hash,
                    ),
                )
                body = {
                    "session_id": session_id,
                    "account_identity_fingerprint": proof[
                        "accountIdentityFingerprint"
                    ],
                    "credential_fingerprint": proof["credentialFingerprint"],
                    "server_owner_identity_sha256": proof[
                        "serverOwnerIdentitySha256"
                    ],
                    "authority_journal_id": proof["authorityJournalId"],
                    "authority_sequence": sequence,
                    "proof_hash": proof_hash,
                    "proof_request_hash": request_hash,
                    "observed_at": proof["observedAt"],
                    "last_phase": phase,
                    "terminal_verified": int(phase == "TERMINAL"),
                    "revision": revision,
                }
                connection.execute(
                    """INSERT INTO binance_exclusivity_cursor
                    (session_id,account_identity_fingerprint,
                     credential_fingerprint,server_owner_identity_sha256,
                     authority_journal_id,authority_sequence,proof_hash,
                     proof_request_hash,observed_at,last_phase,
                     terminal_verified,revision,row_hash)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(session_id) DO UPDATE SET
                     account_identity_fingerprint=excluded.account_identity_fingerprint,
                     credential_fingerprint=excluded.credential_fingerprint,
                     server_owner_identity_sha256=excluded.server_owner_identity_sha256,
                     authority_journal_id=excluded.authority_journal_id,
                     authority_sequence=excluded.authority_sequence,
                     proof_hash=excluded.proof_hash,
                     proof_request_hash=excluded.proof_request_hash,
                     observed_at=excluded.observed_at,
                     last_phase=excluded.last_phase,
                     terminal_verified=excluded.terminal_verified,
                     revision=excluded.revision,row_hash=excluded.row_hash""",
                    (*body.values(), _stable_hash(body)),
                )
                self._validate_cursor_schema(connection)
                self._assert_cursor_path_exact()
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def read_strict(self, **kwargs: Any) -> Mapping[str, Any]:
        descriptor = self.request_descriptor(**kwargs)
        requested_epoch = _utc_epoch(
            descriptor["requestedAt"], "provider requestedAt"
        )
        now = float(self.clock())
        if (
            not math.isfinite(now)
            or now - requested_epoch < -1.0
            or now - requested_epoch > MAX_PROOF_AGE_SECONDS
        ):
            raise BinanceSpotExclusivityError(
                "Binance exclusivity request is stale or future-dated"
            )
        self._publish_request(descriptor)
        proof_path = self._proof_path(descriptor["proofRequestHash"])
        deadline = time.monotonic() + self.proof_wait_seconds
        while not proof_path.is_file() and time.monotonic() < deadline:
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        proof = _strict_json_file(proof_path)
        consumed_now = float(self.clock())
        verified = verify_exclusivity_proof(
            proof,
            phase=descriptor["phase"],
            session_id=descriptor["sessionId"],
            permit_id=descriptor["permitId"],
            permit_hash=descriptor["permitHash"],
            account_identity_fingerprint=descriptor[
                "accountIdentityFingerprint"
            ],
            credential_fingerprint=descriptor["credentialFingerprint"],
            boundary_id=descriptor["boundaryId"],
            boundary_hash=descriptor["boundaryHash"],
            coverage_started_epoch=_utc_epoch(
                descriptor["coverageStartedAt"], "provider coverageStartedAt"
            ),
            requested_epoch=requested_epoch,
            now_epoch=consumed_now,
            verifier=self.verifier,
            verifier_pin=self.verifier_pin,
            require_causal_closure=(
                descriptor["requireCausalClosure"] is True
            ),
        )
        if dict(verified.proof) != proof:
            raise BinanceSpotExclusivityError(
                "Binance exclusivity signed proof normalization changed"
            )
        with self._lock:
            self._consume_cursor(
                proof=proof, proof_hash=verified.proof_hash
            )
            self._last_failure = ""
        return proof

    def __call__(self, **kwargs: Any) -> Mapping[str, Any]:
        try:
            return self.read_strict(**kwargs)
        except Exception as exc:
            self._last_failure = (
                str(exc)
                if isinstance(exc, BinanceSpotExclusivityError)
                else "Binance exclusivity durable provider unavailable"
            )
            raise

    def _restart_verifiable(self) -> bool:
        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    "SELECT * FROM binance_exclusivity_cursor"
                ).fetchall()
                event_sessions = {
                    _text(row["session_id"])
                    for row in connection.execute(
                        "SELECT DISTINCT session_id FROM binance_exclusivity_cursor_event"
                    ).fetchall()
                }
                cursor_sessions: set[str] = set()
                for raw in rows:
                    row = dict(raw)
                    if not self._row_valid(row):
                        return False
                    event = self._validate_events_locked(
                        connection, row["session_id"]
                    )
                    if (
                        event["authoritySequence"]
                        != int(row["authority_sequence"])
                        or event["proofHash"] != row["proof_hash"]
                        or event["proofRequestHash"]
                        != row["proof_request_hash"]
                    ):
                        return False
                    cursor_sessions.add(row["session_id"])
                result = cursor_sessions == event_sessions
                self._validate_cursor_schema(connection)
                self._assert_cursor_path_exact()
                return result
        except Exception:
            return False

    def consumed_payload_verified(
        self, payload: Mapping[str, Any], *, signature: str
    ) -> bool:
        try:
            session_id = _text(payload.get("sessionId"))
            sequence = int(payload.get("authoritySequence"))
            proof_hash = _stable_hash(
                {
                    **dict(payload),
                    "payloadHash": _stable_hash(dict(payload)),
                    "signature": signature,
                }
            )
            if (
                _require_hash(
                    self.account_identity_reader(), "current account identity"
                )
                != payload.get("accountIdentityFingerprint")
                or _require_hash(
                    self.credential_fingerprint_reader(), "current credential"
                )
                != payload.get("credentialFingerprint")
                or _require_hash(
                    self.server_owner_identity_reader(), "current server owner"
                )
                != payload.get("serverOwnerIdentitySha256")
            ):
                return False
            with closing(self._connect()) as connection:
                raw = connection.execute(
                    "SELECT * FROM binance_exclusivity_cursor WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if raw is None:
                    return False
                row = dict(raw)
                head = self._validate_events_locked(connection, session_id)
                selected_raw = connection.execute(
                    """SELECT authority_sequence,phase,proof_hash,
                       proof_request_hash FROM binance_exclusivity_cursor_event
                       WHERE session_id=? AND authority_sequence=?""",
                    (session_id, sequence),
                ).fetchone()
                if selected_raw is None:
                    return False
                selected = dict(selected_raw)
                self._validate_cursor_schema(connection)
                self._assert_cursor_path_exact()
            return bool(
                self._row_valid(row)
                and sequence <= int(row["authority_sequence"])
                and selected["proof_hash"] == proof_hash
                and selected["proof_request_hash"]
                == payload.get("proofRequestHash")
                and row["authority_journal_id"]
                == payload.get("authorityJournalId")
                and head["authoritySequence"]
                == int(row["authority_sequence"])
                and head["proofHash"] == row["proof_hash"]
                and head["proofRequestHash"]
                == row["proof_request_hash"]
            )
        except Exception:
            return False

    def status(self) -> dict[str, Any]:
        wiring = verifier_wiring_status(
            self.verifier,
            self.verifier_pin,
            self.verifier.expected_account_identity_fingerprint,
        )
        identity_matched = False
        try:
            identity_matched = bool(
                _require_hash(
                    self.account_identity_reader(), "current account identity"
                )
                == self.verifier.expected_account_identity_fingerprint
                and _require_hash(
                    self.credential_fingerprint_reader(), "current credential"
                )
                == self.verifier.expected_credential_fingerprint
                and _require_hash(
                    self.server_owner_identity_reader(), "current server owner"
                )
                == self.verifier.expected_server_owner_identity_sha256
            )
        except BinanceSpotExclusivityError:
            identity_matched = False
        restart = self._restart_verifiable()
        return {
            "schemaVersion": PROVIDER_SCHEMA_VERSION,
            "injectionReady": bool(
                wiring.get("ready") is True
                and identity_matched
                and restart
                and self.proof_directory.is_dir()
                and self.cursor_database_path.is_file()
            ),
            "liveActivationReleased": (
                BINANCE_SPOT_EXCLUSIVITY_PROVIDER_PRODUCTION_RELEASED
            ),
            "networkAllowed": BINANCE_SPOT_EXCLUSIVITY_PROVIDER_NETWORK_ALLOWED,
            "signingPrimitivePresent": (
                BINANCE_SPOT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT
            ),
            "asymmetricPublicKeyOnly": True,
            "durableProofSource": True,
            "durableRequestOutbox": True,
            "durableConsumerCursor": True,
            "continuousPerSessionHashChainRequired": True,
            "durable": True,
            "restartVerifiable": restart,
            "cursorSchemaFingerprint": (
                BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT
                if restart
                else ""
            ),
            "cursorPathIdentityPinned": bool(
                restart and self._cursor_file_identity is not None
            ),
            "exactCurrentIdentityMatched": identity_matched,
            "verifier": wiring,
            "proofDirectoryHash": hashlib.sha256(
                str(self.proof_directory).encode("utf-8")
            ).hexdigest(),
            "cursorDatabasePathHash": hashlib.sha256(
                str(self.cursor_database_path).encode("utf-8")
            ).hexdigest(),
            "proofWaitSeconds": self.proof_wait_seconds,
            "lastFailure": self._last_failure,
            "networkOrderPostAllowed": False,
        }


class DurableCursorBoundBinanceSpotExclusivityVerifier:
    def __init__(
        self,
        *,
        cryptographic_verifier: PinnedEd25519BinanceSpotExclusivityVerifier,
        provider: DurableBinanceSpotExclusivityProofProvider,
    ) -> None:
        self.cryptographic_verifier = cryptographic_verifier
        self.provider = provider

    def identity(self) -> Mapping[str, Any]:
        return self.cryptographic_verifier.identity()

    def __call__(
        self,
        *,
        payload: Mapping[str, Any],
        signature: str,
        verifier_pin: Mapping[str, Any],
    ) -> bool:
        return bool(
            self.cryptographic_verifier(
                payload=payload,
                signature=signature,
                verifier_pin=verifier_pin,
            )
            and self.provider.consumed_payload_verified(
                payload, signature=signature
            )
        )


@dataclass(frozen=True, slots=True)
class BinanceSpotExclusivityInjection:
    proof_reader: DurableBinanceSpotExclusivityProofProvider
    verifier: BinanceSpotExclusivityVerifier
    verifier_pin: Mapping[str, Any]
    account_identity_fingerprint: str

    def status(self) -> dict[str, Any]:
        return self.proof_reader.status()

    def facade_kwargs(self) -> dict[str, Any]:
        return {
            "account_exclusivity_proof_reader": self.proof_reader,
            "account_exclusivity_verifier": self.verifier,
            "account_exclusivity_verifier_pin": dict(self.verifier_pin),
            "account_identity_fingerprint": (
                self.account_identity_fingerprint
            ),
        }


def build_binance_spot_exclusivity_injection(
    *,
    proof_directory: str | Path,
    cursor_database_path: str | Path,
    public_key_path: str | Path,
    verifier_pin_path: str | Path,
    verifier_id: str,
    key_id: str,
    authority_journal_id: str,
    expected_account_identity_fingerprint: str,
    expected_credential_fingerprint: str,
    expected_server_owner_identity_sha256: str,
    account_identity_reader: Callable[[], str],
    credential_fingerprint_reader: Callable[[], str],
    server_owner_identity_reader: Callable[[], str],
    clock: Callable[[], float] = time.time,
    proof_wait_seconds: float = 0.0,
) -> BinanceSpotExclusivityInjection:
    """Build the four injection values consumed by the Binance facade."""

    raw_key_path = Path(public_key_path).absolute()
    key_path = raw_key_path.resolve()
    if (
        raw_key_path.is_symlink()
        or raw_key_path.parent.is_symlink()
        or raw_key_path.parent.absolute() != raw_key_path.parent.resolve()
        or not key_path.is_file()
        or key_path.is_symlink()
        or key_path.stat().st_size > 16 * 1024
    ):
        raise BinanceSpotExclusivityError(
            "Binance exclusivity public key file is invalid"
        )
    verifier = PinnedEd25519BinanceSpotExclusivityVerifier(
        public_key=key_path.read_bytes(),
        verifier_id=verifier_id,
        key_id=key_id,
        authority_journal_id=authority_journal_id,
        expected_account_identity_fingerprint=(
            expected_account_identity_fingerprint
        ),
        expected_credential_fingerprint=expected_credential_fingerprint,
        expected_server_owner_identity_sha256=(
            expected_server_owner_identity_sha256
        ),
    )
    raw_pin_path = Path(verifier_pin_path).absolute()
    if (
        raw_pin_path.is_symlink()
        or raw_pin_path.parent.is_symlink()
        or raw_pin_path.parent.absolute() != raw_pin_path.parent.resolve()
    ):
        raise BinanceSpotExclusivityError(
            "Binance exclusivity verifier pin file is invalid"
        )
    pin = _strict_json_file(raw_pin_path.resolve())
    provider = DurableBinanceSpotExclusivityProofProvider(
        proof_directory=proof_directory,
        cursor_database_path=cursor_database_path,
        verifier=verifier,
        expected_verifier_pin=pin,
        account_identity_reader=account_identity_reader,
        credential_fingerprint_reader=credential_fingerprint_reader,
        server_owner_identity_reader=server_owner_identity_reader,
        clock=clock,
        proof_wait_seconds=proof_wait_seconds,
    )
    return BinanceSpotExclusivityInjection(
        proof_reader=provider,
        verifier=DurableCursorBoundBinanceSpotExclusivityVerifier(
            cryptographic_verifier=verifier,
            provider=provider,
        ),
        verifier_pin=dict(pin),
        account_identity_fingerprint=(
            verifier.expected_account_identity_fingerprint
        ),
    )


__all__ = [
    "ALGORITHM",
    "BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT",
    "BINANCE_SPOT_EXCLUSIVITY_PROVIDER_NETWORK_ALLOWED",
    "BINANCE_SPOT_EXCLUSIVITY_PROVIDER_PRODUCTION_RELEASED",
    "BINANCE_SPOT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT",
    "BinanceSpotExclusivityInjection",
    "DurableBinanceSpotExclusivityProofProvider",
    "DurableCursorBoundBinanceSpotExclusivityVerifier",
    "PinnedEd25519BinanceSpotExclusivityVerifier",
    "VERIFIER_TYPE",
    "build_binance_spot_exclusivity_injection",
    "canonical_exclusivity_signature_message",
]
