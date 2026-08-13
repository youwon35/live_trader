from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import secrets
import sqlite3
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping

from .kis_domestic_functional_contract import PDNO, ROUTE


KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_RELEASE_AVAILABLE = False

ACTIVE_SECONDS = 7200
DEFAULT_MAX_GAP_SECONDS = 10
DEFAULT_CLOCK_DIVERGENCE_SECONDS = Decimal("2")
_ZERO_HASH = "0" * 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_SESSION_ID = re.compile(r"^kis-session-[0-9a-f]{32}$", flags=re.ASCII)
_PROCESS_GENERATION = re.compile(
    r"^kis-process-generation-[0-9a-f]{32}$", flags=re.ASCII
)
_SOCKET_GENERATION = re.compile(
    r"^kis-ws-generation-[0-9a-f]{32}$", flags=re.ASCII
)
_TERMINAL_STATES = {
    "OBSERVATION_COMPLETE",
    "SAFE_INCOMPLETE_EARLY_STOP",
    "SAFE_INCOMPLETE_KILL",
    "SAFE_INCOMPLETE_PROCESS_RESTART",
    "SAFE_INCOMPLETE_SOCKET_GENERATION_CHANGED",
    "SAFE_INCOMPLETE_HEARTBEAT_GAP",
    "SAFE_INCOMPLETE_CLOCK_ROLLBACK",
    "SAFE_INCOMPLETE_CLOCK_FORWARD_JUMP",
    "SAFE_INCOMPLETE_MONOTONIC_REGRESSION",
}


class KisDomesticFunctionalHeartbeatBlocked(RuntimeError):
    pass


CaptureSigner = Callable[[str, Mapping[str, Any]], str]
CaptureVerifier = Callable[[str, Mapping[str, Any], str], bool]
WallClock = Callable[[], datetime]
MonotonicClock = Callable[[], int]


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalHeartbeatBlocked(f"{label}-not-aware")
    converted = value.astimezone(timezone.utc)
    if not math.isfinite(converted.timestamp()):
        raise KisDomesticFunctionalHeartbeatBlocked(f"{label}-not-finite")
    return converted


def _utc_text(value: datetime, label: str) -> str:
    return _utc(value, label).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalHeartbeatBlocked(
            f"{label}-not-canonical-utc"
        )
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalHeartbeatBlocked(
            f"{label}-not-canonical-utc"
        ) from None
    if _utc_text(parsed, label) != value:
        raise KisDomesticFunctionalHeartbeatBlocked(
            f"{label}-not-canonical-utc"
        )
    return parsed


def _monotonic(value: Any, label: str) -> int:
    if type(value) is not int or value < 0 or value > 9_223_372_036_854_775_807:
        raise KisDomesticFunctionalHeartbeatBlocked(f"{label}-invalid")
    return value


def _seconds_from_ns(value: int) -> Decimal:
    return Decimal(value) / Decimal(1_000_000_000)


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


_SCHEMA_DESCRIPTOR = {
    "schemaVersion": "kis-domestic-functional-heartbeat-schema/v1",
    "tables": {
        "kis_functional_heartbeat_meta": [
            ("singleton", "INTEGER", 0, 1),
            ("schema_version", "TEXT", 1, 0),
            ("schema_fingerprint", "TEXT", 1, 0),
        ],
        "kis_functional_heartbeat_session": [
            ("session_id", "TEXT", 0, 1),
            ("state", "TEXT", 1, 0),
            ("outcome", "TEXT", 1, 0),
            ("process_generation", "TEXT", 1, 0),
            ("socket_generation", "TEXT", 1, 0),
            ("owner_token_hash", "TEXT", 1, 0),
            ("authority_key_id_hash", "TEXT", 1, 0),
            ("activated_at", "TEXT", 1, 0),
            ("expires_at", "TEXT", 1, 0),
            ("activation_record_hash", "TEXT", 1, 0),
            ("binding_json", "TEXT", 1, 0),
            ("binding_hash", "TEXT", 1, 0),
            ("binding_signature", "TEXT", 1, 0),
            ("started_monotonic_ns", "INTEGER", 1, 0),
            ("last_wall_at", "TEXT", 1, 0),
            ("last_monotonic_ns", "INTEGER", 1, 0),
            ("max_gap_ns", "INTEGER", 1, 0),
            ("sample_count", "INTEGER", 1, 0),
            ("sample_head_hash", "TEXT", 1, 0),
            ("wall_rollback_detected", "INTEGER", 1, 0),
            ("wall_forward_jump_detected", "INTEGER", 1, 0),
            ("process_restart_detected", "INTEGER", 1, 0),
            ("socket_generation_changed", "INTEGER", 1, 0),
            ("terminal_at", "TEXT", 1, 0),
            ("revision", "INTEGER", 1, 0),
        ],
        "kis_functional_heartbeat_sample": [
            ("session_id", "TEXT", 1, 1),
            ("sequence", "INTEGER", 1, 2),
            ("kind", "TEXT", 1, 0),
            ("wall_at", "TEXT", 1, 0),
            ("monotonic_ns", "INTEGER", 1, 0),
            ("previous_hash", "TEXT", 1, 0),
            ("record_json", "TEXT", 1, 0),
            ("record_hash", "TEXT", 1, 0),
            ("signature", "TEXT", 1, 0),
        ],
    },
    "indexes": {
        "kis_functional_heartbeat_active_idx": (
            "CREATE UNIQUE INDEX kis_functional_heartbeat_active_idx "
            "ON kis_functional_heartbeat_session(state) WHERE state='ACTIVE'"
        ),
    },
}
_SCHEMA_SQL = """
CREATE TABLE kis_functional_heartbeat_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    schema_version TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL
);
CREATE TABLE kis_functional_heartbeat_session (
    session_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    outcome TEXT NOT NULL,
    process_generation TEXT NOT NULL,
    socket_generation TEXT NOT NULL,
    owner_token_hash TEXT NOT NULL,
    authority_key_id_hash TEXT NOT NULL,
    activated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    activation_record_hash TEXT NOT NULL,
    binding_json TEXT NOT NULL,
    binding_hash TEXT NOT NULL UNIQUE,
    binding_signature TEXT NOT NULL,
    started_monotonic_ns INTEGER NOT NULL,
    last_wall_at TEXT NOT NULL,
    last_monotonic_ns INTEGER NOT NULL,
    max_gap_ns INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    sample_head_hash TEXT NOT NULL,
    wall_rollback_detected INTEGER NOT NULL,
    wall_forward_jump_detected INTEGER NOT NULL,
    process_restart_detected INTEGER NOT NULL,
    socket_generation_changed INTEGER NOT NULL,
    terminal_at TEXT NOT NULL,
    revision INTEGER NOT NULL
);
CREATE UNIQUE INDEX kis_functional_heartbeat_active_idx
    ON kis_functional_heartbeat_session(state) WHERE state='ACTIVE';
CREATE TABLE kis_functional_heartbeat_sample (
    session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    kind TEXT NOT NULL,
    wall_at TEXT NOT NULL,
    monotonic_ns INTEGER NOT NULL,
    previous_hash TEXT NOT NULL,
    record_json TEXT NOT NULL,
    record_hash TEXT NOT NULL UNIQUE,
    signature TEXT NOT NULL,
    PRIMARY KEY (session_id, sequence),
    FOREIGN KEY (session_id)
        REFERENCES kis_functional_heartbeat_session(session_id)
);
"""
SCHEMA_FINGERPRINT = _hash(
    {
        "descriptor": _SCHEMA_DESCRIPTOR,
        "canonicalSchemaSql": " ".join(_SCHEMA_SQL.strip().split()),
    }
)


def _schema_columns(conn: sqlite3.Connection, table: str) -> list[tuple[Any, ...]]:
    return [
        (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
        for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    ]


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().rstrip(";").split())


def _expected_schema_sql() -> dict[str, str]:
    expected: dict[str, str] = {}
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(_SCHEMA_SQL)
        for row in connection.execute(
            """SELECT name, sql FROM sqlite_master
               WHERE name NOT LIKE 'sqlite_%' ORDER BY name"""
        ).fetchall():
            expected[str(row[0])] = _normalize_sql(row[1])
    finally:
        connection.close()
    return expected


def _verify_exact_schema(conn: sqlite3.Connection) -> None:
    objects = conn.execute(
        """SELECT type, name, sql FROM sqlite_master
           WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"""
    ).fetchall()
    expected_names = set(_SCHEMA_DESCRIPTOR["tables"]) | set(
        _SCHEMA_DESCRIPTOR["indexes"]
    )
    actual_names = {str(row[1]) for row in objects}
    if actual_names != expected_names:
        raise KisDomesticFunctionalHeartbeatBlocked("heartbeat-db-schema-dirty")
    actual_sql = {str(row[1]): _normalize_sql(row[2]) for row in objects}
    if actual_sql != _expected_schema_sql():
        raise KisDomesticFunctionalHeartbeatBlocked(
            "heartbeat-db-definition-schema-dirty"
        )
    for table, columns in _SCHEMA_DESCRIPTOR["tables"].items():
        if _schema_columns(conn, table) != [tuple(row) for row in columns]:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-db-column-schema-dirty"
            )
    indexes = {
        str(row[1]): " ".join(str(row[2] or "").split())
        for row in objects
        if str(row[0]) == "index"
    }
    expected_indexes = {
        name: " ".join(sql.split())
        for name, sql in _SCHEMA_DESCRIPTOR["indexes"].items()
    }
    if indexes != expected_indexes:
        raise KisDomesticFunctionalHeartbeatBlocked(
            "heartbeat-db-index-schema-dirty"
        )
    expected_index_list = {
        "kis_functional_heartbeat_meta": [],
        "kis_functional_heartbeat_session": [
            ("kis_functional_heartbeat_active_idx", 1, 1),
            ("sqlite_autoindex_kis_functional_heartbeat_session_2", 1, 0),
            ("sqlite_autoindex_kis_functional_heartbeat_session_1", 1, 0),
        ],
        "kis_functional_heartbeat_sample": [
            ("sqlite_autoindex_kis_functional_heartbeat_sample_2", 1, 0),
            ("sqlite_autoindex_kis_functional_heartbeat_sample_1", 1, 0),
        ],
    }
    for table, expected in expected_index_list.items():
        actual = [
            (str(row[1]), int(row[2]), int(row[4]))
            for row in conn.execute(f'PRAGMA index_list("{table}")').fetchall()
        ]
        if actual != expected:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-db-index-list-dirty"
            )
    expected_index_columns = {
        "kis_functional_heartbeat_active_idx": ["state"],
        "sqlite_autoindex_kis_functional_heartbeat_session_1": ["session_id"],
        "sqlite_autoindex_kis_functional_heartbeat_session_2": ["binding_hash"],
        "sqlite_autoindex_kis_functional_heartbeat_sample_2": [
            "session_id",
            "sequence",
        ],
        "sqlite_autoindex_kis_functional_heartbeat_sample_1": ["record_hash"],
    }
    for index, expected in expected_index_columns.items():
        actual = [
            str(row[2])
            for row in conn.execute(f'PRAGMA index_xinfo("{index}")').fetchall()
            if int(row[5]) == 1
        ]
        if actual != expected:
            raise KisDomesticFunctionalHeartbeatBlocked(
                f"heartbeat-db-index-columns-dirty:{index}:{actual!r}:{expected!r}"
            )
    expected_foreign_keys = {
        "kis_functional_heartbeat_meta": [],
        "kis_functional_heartbeat_session": [],
        "kis_functional_heartbeat_sample": [
            (
                "kis_functional_heartbeat_session",
                "session_id",
                "session_id",
                "NO ACTION",
                "NO ACTION",
            )
        ],
    }
    for table, expected in expected_foreign_keys.items():
        actual = [
            (str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]))
            for row in conn.execute(
                f'PRAGMA foreign_key_list("{table}")'
            ).fetchall()
        ]
        if actual != expected:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-db-foreign-key-schema-dirty"
            )
    meta = conn.execute(
        "SELECT singleton, schema_version, schema_fingerprint "
        "FROM kis_functional_heartbeat_meta"
    ).fetchall()
    if len(meta) != 1 or (
        int(meta[0][0]), str(meta[0][1]), str(meta[0][2])
    ) != (1, _SCHEMA_DESCRIPTOR["schemaVersion"], SCHEMA_FINGERPRINT):
        raise KisDomesticFunctionalHeartbeatBlocked(
            "heartbeat-db-schema-fingerprint-mismatch"
        )


def _decode_record(text: Any, label: str) -> dict[str, Any]:
    if type(text) is not str or not text:
        raise KisDomesticFunctionalHeartbeatBlocked(f"{label}-missing")
    try:
        value = json.loads(text)
    except (TypeError, ValueError):
        raise KisDomesticFunctionalHeartbeatBlocked(f"{label}-invalid") from None
    if not isinstance(value, dict) or _canonical(value).decode("utf-8") != text:
        raise KisDomesticFunctionalHeartbeatBlocked(f"{label}-not-canonical")
    return value


def _require_sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalHeartbeatBlocked(f"{label}-invalid")
    return value


def _exact_activation(value: Mapping[str, Any]) -> dict[str, Any]:
    body = deepcopy(dict(value))
    required = {
        "schemaVersion",
        "sessionId",
        "activatedAt",
        "expiresAt",
        "activeSeconds",
        "activationRecordHash",
    }
    if set(body) != required:
        raise KisDomesticFunctionalHeartbeatBlocked(
            "activation-binding-fields-not-exact"
        )
    if (
        body["schemaVersion"] != "kis-domestic-functional-activation/v1"
        or type(body["sessionId"]) is not str
        or not _SESSION_ID.fullmatch(body["sessionId"])
        or body["activeSeconds"] != ACTIVE_SECONDS
    ):
        raise KisDomesticFunctionalHeartbeatBlocked("activation-binding-invalid")
    activated = _parse_utc(body["activatedAt"], "activation.activatedAt")
    expires = _parse_utc(body["expiresAt"], "activation.expiresAt")
    if expires - activated != timedelta(seconds=ACTIVE_SECONDS):
        raise KisDomesticFunctionalHeartbeatBlocked(
            "activation-window-not-exact-7200"
        )
    _require_sha(body["activationRecordHash"], "activationRecordHash")
    return body


class DurableKisDomesticFunctionalHeartbeat:
    def __init__(
        self,
        path: str | Path,
        *,
        capture_signer: CaptureSigner,
        capture_verifier: CaptureVerifier,
        server_authority_key_id: str,
        process_generation: str,
        wall_clock: WallClock,
        monotonic_clock: MonotonicClock,
        max_gap_seconds: int = DEFAULT_MAX_GAP_SECONDS,
        clock_divergence_seconds: Decimal = DEFAULT_CLOCK_DIVERGENCE_SECONDS,
        owner_token_factory: Callable[[], bytes] | None = None,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        if not self.path.parent.is_dir():
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-journal-parent-missing"
            )
        if not callable(capture_signer) or not callable(capture_verifier):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-signer-and-verifier-required"
            )
        if type(server_authority_key_id) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", server_authority_key_id
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-authority-key-id-invalid"
            )
        if type(process_generation) is not str or not _PROCESS_GENERATION.fullmatch(
            process_generation
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-process-generation-invalid"
            )
        if not callable(wall_clock) or not callable(monotonic_clock):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-trusted-clocks-required"
            )
        if (
            type(max_gap_seconds) is not int
            or max_gap_seconds != DEFAULT_MAX_GAP_SECONDS
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-max-gap-policy-mismatch"
            )
        if (
            not isinstance(clock_divergence_seconds, Decimal)
            or not clock_divergence_seconds.is_finite()
            or clock_divergence_seconds != DEFAULT_CLOCK_DIVERGENCE_SECONDS
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-clock-divergence-policy-mismatch"
            )
        token_factory = owner_token_factory or (lambda: secrets.token_bytes(32))
        token = token_factory()
        if type(token) is not bytes or len(token) < 32:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-owner-token-invalid"
            )
        self._owner_token_hash = hashlib.sha256(token).hexdigest()
        self._signer = capture_signer
        self._verifier = capture_verifier
        self.authority_key_id_hash = hashlib.sha256(
            server_authority_key_id.encode("utf-8")
        ).hexdigest()
        self.process_generation = process_generation
        self._wall_clock = wall_clock
        self._monotonic_clock = monotonic_clock
        self.max_gap_ns = max_gap_seconds * 1_000_000_000
        self.clock_divergence_ns = int(
            clock_divergence_seconds * Decimal(1_000_000_000)
        )
        self._lock = threading.RLock()
        self._prepare_schema()
        self.startup_terminalized_session_ids = self.audit_restart()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _prepare_schema(self) -> None:
        with self._lock:
            existed = self.path.exists() and self.path.stat().st_size > 0
            conn = self._connect()
            try:
                if not existed:
                    conn.executescript(_SCHEMA_SQL)
                    conn.execute(
                        "INSERT INTO kis_functional_heartbeat_meta VALUES (1, ?, ?)",
                        (_SCHEMA_DESCRIPTOR["schemaVersion"], SCHEMA_FINGERPRINT),
                    )
                    conn.commit()
                _verify_exact_schema(conn)
            finally:
                conn.close()

    def _now(self) -> tuple[datetime, int]:
        wall = _utc(self._wall_clock(), "trustedWallClock")
        mono = _monotonic(self._monotonic_clock(), "trustedMonotonicClock")
        return wall, mono

    def _sign(self, domain: str, body: Mapping[str, Any]) -> str:
        try:
            signature = self._signer(domain, deepcopy(dict(body)))
        except BaseException as exc:
            raise KisDomesticFunctionalHeartbeatBlocked(
                f"heartbeat-signing-failed:{type(exc).__name__}"
            ) from None
        _require_sha(signature, "heartbeatSignature")
        try:
            verified = self._verifier(
                domain, deepcopy(dict(body)), signature
            )
        except BaseException as exc:
            raise KisDomesticFunctionalHeartbeatBlocked(
                f"heartbeat-verifier-failed:{type(exc).__name__}"
            ) from None
        if verified is not True:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-signer-verifier-mismatch"
            )
        return signature

    def _insert_sample(
        self,
        conn: sqlite3.Connection,
        session: sqlite3.Row,
        *,
        kind: str,
        wall: datetime,
        monotonic_ns: int,
    ) -> tuple[str, int]:
        sequence = int(session["sample_count"]) + 1
        previous_hash = str(session["sample_head_hash"])
        body = {
            "schemaVersion": "kis-domestic-functional-heartbeat-sample/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": str(session["session_id"]),
            "bindingHash": str(session["binding_hash"]),
            "processGeneration": str(session["process_generation"]),
            "socketGeneration": str(session["socket_generation"]),
            "sequence": sequence,
            "kind": kind,
            "wallAt": _utc_text(wall, "sample.wallAt"),
            "monotonicNs": monotonic_ns,
            "previousHash": previous_hash,
            "authorityKeyIdHash": self.authority_key_id_hash,
        }
        record_hash = _hash(body)
        signature = self._sign("HEARTBEAT_SAMPLE", body)
        conn.execute(
            """INSERT INTO kis_functional_heartbeat_sample
               (session_id, sequence, kind, wall_at, monotonic_ns,
                previous_hash, record_json, record_hash, signature)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session["session_id"],
                sequence,
                kind,
                body["wallAt"],
                monotonic_ns,
                previous_hash,
                _canonical(body).decode("utf-8"),
                record_hash,
                signature,
            ),
        )
        return record_hash, sequence

    def audit_restart(self) -> tuple[str, ...]:
        wall, mono = self._now()
        terminalized: list[str] = []
        with self._lock:
            conn = self._connect()
            try:
                _verify_exact_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                rows = conn.execute(
                    "SELECT * FROM kis_functional_heartbeat_session "
                    "WHERE state='ACTIVE'"
                ).fetchall()
                for row in rows:
                    head, sequence = self._insert_sample(
                        conn,
                        row,
                        kind="PROCESS_RESTART",
                        wall=wall,
                        monotonic_ns=mono,
                    )
                    changed = conn.execute(
                        """UPDATE kis_functional_heartbeat_session
                           SET state='SAFE_INCOMPLETE_PROCESS_RESTART',
                               outcome='SAFE_INCOMPLETE_PROCESS_RESTART',
                               process_restart_detected=1, terminal_at=?,
                               last_wall_at=?, last_monotonic_ns=?,
                               sample_count=?, sample_head_hash=?, revision=revision+1
                           WHERE session_id=? AND state='ACTIVE' AND revision=?""",
                        (
                            _utc_text(wall, "restart.wallAt"),
                            _utc_text(wall, "restart.wallAt"),
                            mono,
                            sequence,
                            head,
                            row["session_id"],
                            int(row["revision"]),
                        ),
                    )
                    if changed.rowcount != 1:
                        raise KisDomesticFunctionalHeartbeatBlocked(
                            "heartbeat-restart-cas-failed"
                        )
                    terminalized.append(str(row["session_id"]))
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return tuple(terminalized)

    def start(
        self,
        *,
        activation: Mapping[str, Any],
        socket_generation: str,
    ) -> dict[str, Any]:
        activation_body = _exact_activation(activation)
        if type(socket_generation) is not str or not _SOCKET_GENERATION.fullmatch(
            socket_generation
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-socket-generation-invalid"
            )
        wall, mono = self._now()
        activated = _parse_utc(
            activation_body["activatedAt"], "activation.activatedAt"
        )
        if wall != activated:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-start-not-atomic-with-activation"
            )
        binding = {
            "schemaVersion": "kis-domestic-functional-heartbeat-binding/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": activation_body["sessionId"],
            "activatedAt": activation_body["activatedAt"],
            "expiresAt": activation_body["expiresAt"],
            "activeSeconds": ACTIVE_SECONDS,
            "activationRecordHash": activation_body["activationRecordHash"],
            "processGeneration": self.process_generation,
            "socketGeneration": socket_generation,
            "authorityKeyIdHash": self.authority_key_id_hash,
            "maxHeartbeatGapSeconds": self.max_gap_ns // 1_000_000_000,
            "maxWallMonotonicDivergenceSeconds": _decimal_text(
                _seconds_from_ns(self.clock_divergence_ns)
            ),
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
        }
        binding_hash = _hash(binding)
        binding_signature = self._sign("HEARTBEAT_BINDING", binding)
        with self._lock:
            conn = self._connect()
            try:
                _verify_exact_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                if conn.execute(
                    "SELECT 1 FROM kis_functional_heartbeat_session "
                    "WHERE state='ACTIVE'"
                ).fetchone() is not None:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "another-heartbeat-session-active"
                    )
                conn.execute(
                    """INSERT INTO kis_functional_heartbeat_session
                       (session_id, state, outcome, process_generation,
                        socket_generation, owner_token_hash,
                        authority_key_id_hash, activated_at, expires_at,
                        activation_record_hash, binding_json, binding_hash,
                        binding_signature, started_monotonic_ns, last_wall_at,
                        last_monotonic_ns, max_gap_ns, sample_count,
                        sample_head_hash, wall_rollback_detected,
                        wall_forward_jump_detected, process_restart_detected,
                        socket_generation_changed, terminal_at, revision)
                       VALUES (?, 'ACTIVE', 'ACTIVE_OBSERVATION', ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, 0, 0, 0, '', 0)""",
                    (
                        activation_body["sessionId"],
                        self.process_generation,
                        socket_generation,
                        self._owner_token_hash,
                        self.authority_key_id_hash,
                        activation_body["activatedAt"],
                        activation_body["expiresAt"],
                        activation_body["activationRecordHash"],
                        _canonical(binding).decode("utf-8"),
                        binding_hash,
                        binding_signature,
                        mono,
                        activation_body["activatedAt"],
                        mono,
                        _ZERO_HASH,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM kis_functional_heartbeat_session WHERE session_id=?",
                    (activation_body["sessionId"],),
                ).fetchone()
                head, sequence = self._insert_sample(
                    conn, row, kind="ACTIVE_START", wall=wall, monotonic_ns=mono
                )
                changed = conn.execute(
                    """UPDATE kis_functional_heartbeat_session
                       SET sample_count=?, sample_head_hash=?, revision=revision+1
                       WHERE session_id=? AND state='ACTIVE' AND revision=0""",
                    (sequence, head, activation_body["sessionId"]),
                )
                if changed.rowcount != 1:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-start-cas-failed"
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return self.snapshot(activation_body["sessionId"])

    def _active_row(
        self,
        conn: sqlite3.Connection,
        session_id: str,
    ) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM kis_functional_heartbeat_session WHERE session_id=?",
            (session_id,),
        ).fetchone()
        if (
            row is None
            or str(row["state"]) != "ACTIVE"
            or str(row["process_generation"]) != self.process_generation
            or not hmac.compare_digest(
                str(row["owner_token_hash"]), self._owner_token_hash
            )
            or str(row["authority_key_id_hash"]) != self.authority_key_id_hash
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-session-not-active-owner"
            )
        return row

    def observe(
        self,
        *,
        session_id: str,
        socket_generation: str,
    ) -> dict[str, Any]:
        wall, mono = self._now()
        with self._lock:
            conn = self._connect()
            try:
                _verify_exact_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                row = self._active_row(conn, session_id)
                expected_socket = str(row["socket_generation"])
                if socket_generation != expected_socket:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="SOCKET_GENERATION_CHANGED",
                        state="SAFE_INCOMPLETE_SOCKET_GENERATION_CHANGED",
                        flag_column="socket_generation_changed",
                    )
                activated = _parse_utc(row["activated_at"], "session.activatedAt")
                expires = _parse_utc(row["expires_at"], "session.expiresAt")
                previous_wall = _parse_utc(
                    row["last_wall_at"], "session.lastWallAt"
                )
                previous_mono = int(row["last_monotonic_ns"])
                mono_delta = mono - previous_mono
                wall_delta_ns = int(
                    Decimal(str((wall - previous_wall).total_seconds()))
                    * Decimal(1_000_000_000)
                )
                if mono_delta < 0:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="MONOTONIC_REGRESSION",
                        state="SAFE_INCOMPLETE_MONOTONIC_REGRESSION",
                    )
                if wall_delta_ns < 0:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="CLOCK_ROLLBACK",
                        state="SAFE_INCOMPLETE_CLOCK_ROLLBACK",
                        flag_column="wall_rollback_detected",
                    )
                if wall_delta_ns - mono_delta > self.clock_divergence_ns:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="CLOCK_FORWARD_JUMP",
                        state="SAFE_INCOMPLETE_CLOCK_FORWARD_JUMP",
                        flag_column="wall_forward_jump_detected",
                    )
                if abs(wall_delta_ns - mono_delta) > self.clock_divergence_ns:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="CLOCK_ROLLBACK",
                        state="SAFE_INCOMPLETE_CLOCK_ROLLBACK",
                        flag_column="wall_rollback_detected",
                    )
                if not activated <= wall < expires:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-sample-outside-active-half-open-window"
                    )
                if mono_delta > self.max_gap_ns:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="HEARTBEAT_GAP",
                        state="SAFE_INCOMPLETE_HEARTBEAT_GAP",
                    )
                head, sequence = self._insert_sample(
                    conn, row, kind="HEARTBEAT", wall=wall, monotonic_ns=mono
                )
                changed = conn.execute(
                    """UPDATE kis_functional_heartbeat_session
                       SET last_wall_at=?, last_monotonic_ns=?,
                           max_gap_ns=MAX(max_gap_ns, ?), sample_count=?,
                           sample_head_hash=?, revision=revision+1
                       WHERE session_id=? AND state='ACTIVE' AND revision=?""",
                    (
                        _utc_text(wall, "heartbeat.wallAt"),
                        mono,
                        mono_delta,
                        sequence,
                        head,
                        session_id,
                        int(row["revision"]),
                    ),
                )
                if changed.rowcount != 1:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-observe-cas-failed"
                    )
                conn.commit()
                return self.snapshot(session_id)
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    def _terminalize_locked(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        wall: datetime,
        mono: int,
        kind: str,
        state: str,
        flag_column: str | None = None,
    ) -> dict[str, Any]:
        if state not in _TERMINAL_STATES:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-terminal-state-invalid"
            )
        head, sequence = self._insert_sample(
            conn, row, kind=kind, wall=wall, monotonic_ns=mono
        )
        previous_mono = int(row["last_monotonic_ns"])
        gap = max(0, mono - previous_mono)
        flag_sql = f", {flag_column}=1" if flag_column else ""
        changed = conn.execute(
            f"""UPDATE kis_functional_heartbeat_session
                SET state=?, outcome=?, terminal_at=?, last_wall_at=?,
                    last_monotonic_ns=?, max_gap_ns=MAX(max_gap_ns, ?),
                    sample_count=?, sample_head_hash=?, revision=revision+1
                    {flag_sql}
                WHERE session_id=? AND state='ACTIVE' AND revision=?""",
            (
                state,
                state,
                _utc_text(wall, "terminal.wallAt"),
                _utc_text(wall, "terminal.wallAt"),
                mono,
                gap,
                sequence,
                head,
                row["session_id"],
                int(row["revision"]),
            ),
        )
        if changed.rowcount != 1:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-terminal-cas-failed"
            )
        conn.commit()
        return self.snapshot(str(row["session_id"]))

    def record_control(self, *, session_id: str, control: str) -> dict[str, Any]:
        if control not in {"STOP", "KILL"}:
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-control-invalid"
            )
        wall, mono = self._now()
        state = (
            "SAFE_INCOMPLETE_EARLY_STOP"
            if control == "STOP"
            else "SAFE_INCOMPLETE_KILL"
        )
        with self._lock:
            conn = self._connect()
            try:
                _verify_exact_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                row = self._active_row(conn, session_id)
                return self._terminalize_locked(
                    conn,
                    row,
                    wall=wall,
                    mono=mono,
                    kind=control,
                    state=state,
                )
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    def complete(self, *, session_id: str) -> dict[str, Any]:
        wall, mono = self._now()
        with self._lock:
            conn = self._connect()
            try:
                _verify_exact_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                row = self._active_row(conn, session_id)
                expires = _parse_utc(row["expires_at"], "session.expiresAt")
                previous_wall = _parse_utc(
                    row["last_wall_at"], "session.lastWallAt"
                )
                previous_mono = int(row["last_monotonic_ns"])
                final_gap = mono - previous_mono
                wall_delta_ns = int(
                    Decimal(str((wall - previous_wall).total_seconds()))
                    * Decimal(1_000_000_000)
                )
                if final_gap < 0:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="MONOTONIC_REGRESSION",
                        state="SAFE_INCOMPLETE_MONOTONIC_REGRESSION",
                    )
                if wall_delta_ns < 0 or (
                    wall_delta_ns - final_gap < -self.clock_divergence_ns
                ):
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="CLOCK_ROLLBACK",
                        state="SAFE_INCOMPLETE_CLOCK_ROLLBACK",
                        flag_column="wall_rollback_detected",
                    )
                if wall_delta_ns - final_gap > self.clock_divergence_ns:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="CLOCK_FORWARD_JUMP",
                        state="SAFE_INCOMPLETE_CLOCK_FORWARD_JUMP",
                        flag_column="wall_forward_jump_detected",
                    )
                if wall < expires:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-cannot-complete-before-active-end"
                    )
                mono_elapsed = mono - int(row["started_monotonic_ns"])
                if mono_elapsed < ACTIVE_SECONDS * 1_000_000_000:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="CLOCK_FORWARD_JUMP",
                        state="SAFE_INCOMPLETE_CLOCK_FORWARD_JUMP",
                        flag_column="wall_forward_jump_detected",
                    )
                if final_gap > self.max_gap_ns:
                    return self._terminalize_locked(
                        conn,
                        row,
                        wall=wall,
                        mono=mono,
                        kind="HEARTBEAT_GAP",
                        state="SAFE_INCOMPLETE_HEARTBEAT_GAP",
                    )
                head, sequence = self._insert_sample(
                    conn,
                    row,
                    kind="ACTIVE_END_OBSERVED",
                    wall=wall,
                    monotonic_ns=mono,
                )
                changed = conn.execute(
                    """UPDATE kis_functional_heartbeat_session
                       SET state='OBSERVATION_COMPLETE',
                           outcome='ELIGIBLE_FOR_INDEPENDENT_WIRING_VERIFICATION',
                           terminal_at=?, last_wall_at=?, last_monotonic_ns=?,
                           max_gap_ns=MAX(max_gap_ns, ?), sample_count=?,
                           sample_head_hash=?, revision=revision+1
                       WHERE session_id=? AND state='ACTIVE' AND revision=?""",
                    (
                        _utc_text(wall, "complete.wallAt"),
                        _utc_text(wall, "complete.wallAt"),
                        mono,
                        final_gap,
                        sequence,
                        head,
                        session_id,
                        int(row["revision"]),
                    ),
                )
                if changed.rowcount != 1:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-complete-cas-failed"
                    )
                conn.commit()
                return self.snapshot(session_id)
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    def snapshot(self, session_id: str) -> dict[str, Any]:
        with self._lock:
            conn = self._connect()
            try:
                _verify_exact_schema(conn)
                row = conn.execute(
                    "SELECT * FROM kis_functional_heartbeat_session WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if row is None:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-session-missing"
                    )
                return {
                    "schemaVersion": "kis-domestic-functional-heartbeat-status/v1",
                    "sessionId": str(row["session_id"]),
                    "state": str(row["state"]),
                    "outcome": str(row["outcome"]),
                    "processGeneration": str(row["process_generation"]),
                    "socketGeneration": str(row["socket_generation"]),
                    "activatedAt": str(row["activated_at"]),
                    "expiresAt": str(row["expires_at"]),
                    "sampleCount": int(row["sample_count"]),
                    "sampleHeadHash": str(row["sample_head_hash"]),
                    "maxObservedGapSeconds": _decimal_text(
                        _seconds_from_ns(int(row["max_gap_ns"]))
                    ),
                    "wallRollbackDetected": bool(row["wall_rollback_detected"]),
                    "wallForwardJumpDetected": bool(
                        row["wall_forward_jump_detected"]
                    ),
                    "processRestartDetected": bool(
                        row["process_restart_detected"]
                    ),
                    "socketGenerationChanged": bool(
                        row["socket_generation_changed"]
                    ),
                    "uninterrupted": str(row["state"]) == "OBSERVATION_COMPLETE",
                    "productionAvailable": False,
                    "networkAvailable": False,
                    "mutationAvailable": False,
                    "releaseAvailable": False,
                    "revision": int(row["revision"]),
                }
            finally:
                conn.close()


class KisDomesticFunctionalHeartbeatVerifier:
    def __init__(
        self,
        path: str | Path,
        *,
        capture_verifier: CaptureVerifier,
        server_authority_key_id: str,
        trusted_wall_clock: WallClock,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        if not self.path.is_file():
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-verifier-db-missing"
            )
        if not callable(capture_verifier) or not callable(trusted_wall_clock):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-verifier-authority-invalid"
            )
        if type(server_authority_key_id) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", server_authority_key_id
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "heartbeat-verifier-key-id-invalid"
            )
        self._verifier = capture_verifier
        self._trusted_wall_clock = trusted_wall_clock
        self.authority_key_id_hash = hashlib.sha256(
            server_authority_key_id.encode("utf-8")
        ).hexdigest()

    def _connect_read_only(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            f"file:{self.path.as_posix()}?mode=ro", uri=True, timeout=5.0
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn

    def _verify_signature(
        self,
        domain: str,
        body: Mapping[str, Any],
        signature: Any,
        label: str,
    ) -> None:
        _require_sha(signature, f"{label}.signature")
        try:
            result = self._verifier(
                domain, deepcopy(dict(body)), str(signature)
            )
        except BaseException as exc:
            raise KisDomesticFunctionalHeartbeatBlocked(
                f"{label}-verifier-failed:{type(exc).__name__}"
            ) from None
        if result is not True:
            raise KisDomesticFunctionalHeartbeatBlocked(
                f"{label}-signature-mismatch"
            )

    def verify(
        self,
        *,
        expected_activation: Mapping[str, Any],
        expected_socket_generation: str,
    ) -> dict[str, Any]:
        activation = _exact_activation(expected_activation)
        if (
            type(expected_socket_generation) is not str
            or not _SOCKET_GENERATION.fullmatch(expected_socket_generation)
        ):
            raise KisDomesticFunctionalHeartbeatBlocked(
                "expected-socket-generation-invalid"
            )
        trusted_now = _utc(
            self._trusted_wall_clock(), "heartbeatVerifier.trustedNow"
        )
        conn = self._connect_read_only()
        try:
            conn.execute("BEGIN")
            _verify_exact_schema(conn)
            row = conn.execute(
                "SELECT * FROM kis_functional_heartbeat_session WHERE session_id=?",
                (activation["sessionId"],),
            ).fetchone()
            if row is None:
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-verifier-session-missing"
                )
            binding = _decode_record(row["binding_json"], "heartbeat.binding")
            if (
                not hmac.compare_digest(str(row["binding_hash"]), _hash(binding))
                or str(row["authority_key_id_hash"])
                != self.authority_key_id_hash
                or binding.get("authorityKeyIdHash")
                != self.authority_key_id_hash
            ):
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-binding-hash-or-key-mismatch"
                )
            self._verify_signature(
                "HEARTBEAT_BINDING",
                binding,
                row["binding_signature"],
                "heartbeat-binding",
            )
            if (
                set(binding)
                != {
                    "schemaVersion",
                    "route",
                    "pdno",
                    "sessionId",
                    "activatedAt",
                    "expiresAt",
                    "activeSeconds",
                    "activationRecordHash",
                    "processGeneration",
                    "socketGeneration",
                    "authorityKeyIdHash",
                    "maxHeartbeatGapSeconds",
                    "maxWallMonotonicDivergenceSeconds",
                    "productionAvailable",
                    "networkAvailable",
                    "mutationAvailable",
                }
                or
                binding.get("schemaVersion")
                != "kis-domestic-functional-heartbeat-binding/v1"
                or binding.get("route") != ROUTE
                or binding.get("pdno") != PDNO
                or binding.get("sessionId") != activation["sessionId"]
                or binding.get("activatedAt") != activation["activatedAt"]
                or binding.get("expiresAt") != activation["expiresAt"]
                or binding.get("activeSeconds") != ACTIVE_SECONDS
                or binding.get("activationRecordHash")
                != activation["activationRecordHash"]
                or binding.get("socketGeneration")
                != expected_socket_generation
                or str(row["socket_generation"])
                != expected_socket_generation
                or str(row["activated_at"]) != activation["activatedAt"]
                or str(row["expires_at"]) != activation["expiresAt"]
                or str(row["activation_record_hash"])
                != activation["activationRecordHash"]
                or binding.get("processGeneration")
                != str(row["process_generation"])
                or type(binding.get("processGeneration")) is not str
                or not _PROCESS_GENERATION.fullmatch(
                    binding["processGeneration"]
                )
                or binding.get("productionAvailable") is not False
                or binding.get("networkAvailable") is not False
                or binding.get("mutationAvailable") is not False
                or type(binding.get("maxHeartbeatGapSeconds")) is not int
                or binding["maxHeartbeatGapSeconds"]
                != DEFAULT_MAX_GAP_SECONDS
                or binding.get("maxWallMonotonicDivergenceSeconds")
                != _decimal_text(DEFAULT_CLOCK_DIVERGENCE_SECONDS)
            ):
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-activation-lineage-mismatch"
                )
            samples = conn.execute(
                """SELECT * FROM kis_functional_heartbeat_sample
                   WHERE session_id=? ORDER BY sequence""",
                (activation["sessionId"],),
            ).fetchall()
            if (
                not samples
                or [int(sample["sequence"]) for sample in samples]
                != list(range(1, len(samples) + 1))
                or int(row["sample_count"]) != len(samples)
            ):
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-sample-sequence-incomplete"
                )
            previous_hash = _ZERO_HASH
            previous_wall: datetime | None = None
            previous_mono: int | None = None
            max_gap_ns = 0
            rollback = False
            forward = False
            process_restart = False
            socket_change = False
            monotonic_regression = False
            kinds: list[str] = []
            max_gap_limit_ns = DEFAULT_MAX_GAP_SECONDS * 1_000_000_000
            divergence_ns = int(
                DEFAULT_CLOCK_DIVERGENCE_SECONDS * Decimal(1_000_000_000)
            )
            activated = _parse_utc(activation["activatedAt"], "activatedAt")
            expires = _parse_utc(activation["expiresAt"], "expiresAt")
            started_mono = int(row["started_monotonic_ns"])
            for index, sample in enumerate(samples, start=1):
                body = _decode_record(sample["record_json"], "heartbeat.sample")
                if (
                    set(body)
                    != {
                        "schemaVersion",
                        "route",
                        "pdno",
                        "sessionId",
                        "bindingHash",
                        "processGeneration",
                        "socketGeneration",
                        "sequence",
                        "kind",
                        "wallAt",
                        "monotonicNs",
                        "previousHash",
                        "authorityKeyIdHash",
                    }
                    or body.get("schemaVersion")
                    != "kis-domestic-functional-heartbeat-sample/v1"
                    or body.get("route") != ROUTE
                    or body.get("pdno") != PDNO
                    or not hmac.compare_digest(
                        str(sample["record_hash"]), _hash(body)
                    )
                    or body.get("sequence") != index
                    or body.get("previousHash") != previous_hash
                    or str(sample["previous_hash"]) != previous_hash
                    or body.get("sessionId") != activation["sessionId"]
                    or body.get("bindingHash") != str(row["binding_hash"])
                    or body.get("processGeneration")
                    != str(row["process_generation"])
                    or body.get("socketGeneration")
                    != expected_socket_generation
                    or body.get("authorityKeyIdHash")
                    != self.authority_key_id_hash
                    or str(sample["kind"]) != body.get("kind")
                    or str(sample["wall_at"]) != body.get("wallAt")
                    or int(sample["monotonic_ns"])
                    != body.get("monotonicNs")
                ):
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-sample-record-mismatch"
                    )
                self._verify_signature(
                    "HEARTBEAT_SAMPLE",
                    body,
                    sample["signature"],
                    "heartbeat-sample",
                )
                wall = _parse_utc(body["wallAt"], "sample.wallAt")
                mono = _monotonic(body["monotonicNs"], "sample.monotonicNs")
                if wall > trusted_now:
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-sample-future-dated"
                    )
                kind = str(body["kind"])
                kinds.append(kind)
                if index == 1:
                    if (
                        kind != "ACTIVE_START"
                        or wall != activated
                        or mono != started_mono
                    ):
                        raise KisDomesticFunctionalHeartbeatBlocked(
                            "heartbeat-first-sample-not-exact-activation"
                        )
                elif kind != "PROCESS_RESTART":
                    assert previous_wall is not None and previous_mono is not None
                    mono_delta = mono - previous_mono
                    wall_delta_ns = int(
                        Decimal(str((wall - previous_wall).total_seconds()))
                        * Decimal(1_000_000_000)
                    )
                    max_gap_ns = max(max_gap_ns, max(0, mono_delta))
                    if mono_delta < 0:
                        monotonic_regression = True
                    if wall_delta_ns < 0 or wall_delta_ns - mono_delta < -divergence_ns:
                        rollback = True
                    if wall_delta_ns - mono_delta > divergence_ns:
                        forward = True
                if kind == "PROCESS_RESTART":
                    process_restart = True
                if kind == "SOCKET_GENERATION_CHANGED":
                    socket_change = True
                if kind in {"HEARTBEAT", "ACTIVE_START"} and not (
                    activated <= wall < expires
                ):
                    raise KisDomesticFunctionalHeartbeatBlocked(
                        "heartbeat-active-sample-outside-half-open-window"
                    )
                previous_hash = str(sample["record_hash"])
                previous_wall = wall
                previous_mono = mono
            if (
                not hmac.compare_digest(previous_hash, str(row["sample_head_hash"]))
                or max_gap_ns != int(row["max_gap_ns"])
                or str(row["last_wall_at"]) != samples[-1]["wall_at"]
                or int(row["last_monotonic_ns"])
                != int(samples[-1]["monotonic_ns"])
                or int(row["revision"]) != len(samples)
                or rollback != bool(row["wall_rollback_detected"])
                or forward != bool(row["wall_forward_jump_detected"])
                or process_restart != bool(row["process_restart_detected"])
                or socket_change != bool(row["socket_generation_changed"])
            ):
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-summary-does-not-match-samples"
                )
            state = str(row["state"])
            outcome = str(row["outcome"])
            expected_last_kind = {
                "OBSERVATION_COMPLETE": "ACTIVE_END_OBSERVED",
                "SAFE_INCOMPLETE_EARLY_STOP": "STOP",
                "SAFE_INCOMPLETE_KILL": "KILL",
                "SAFE_INCOMPLETE_PROCESS_RESTART": "PROCESS_RESTART",
                "SAFE_INCOMPLETE_SOCKET_GENERATION_CHANGED": (
                    "SOCKET_GENERATION_CHANGED"
                ),
                "SAFE_INCOMPLETE_HEARTBEAT_GAP": "HEARTBEAT_GAP",
                "SAFE_INCOMPLETE_CLOCK_ROLLBACK": "CLOCK_ROLLBACK",
                "SAFE_INCOMPLETE_CLOCK_FORWARD_JUMP": "CLOCK_FORWARD_JUMP",
                "SAFE_INCOMPLETE_MONOTONIC_REGRESSION": "MONOTONIC_REGRESSION",
            }
            expected_terminal_kind = expected_last_kind.get(state)
            exact_sample_topology = (
                len(kinds) >= 2
                and kinds[0] == "ACTIVE_START"
                and kinds[-1] == expected_terminal_kind
                and all(kind == "HEARTBEAT" for kind in kinds[1:-1])
            )
            if (
                not exact_sample_topology
                or (
                    state != "OBSERVATION_COMPLETE"
                    and outcome != state
                )
                or (
                    state == "SAFE_INCOMPLETE_MONOTONIC_REGRESSION"
                    and not monotonic_regression
                )
                or (
                    state != "SAFE_INCOMPLETE_MONOTONIC_REGRESSION"
                    and monotonic_regression
                )
                or (
                    state == "SAFE_INCOMPLETE_HEARTBEAT_GAP"
                    and max_gap_ns <= max_gap_limit_ns
                )
                or (
                    state == "SAFE_INCOMPLETE_CLOCK_ROLLBACK"
                    and not rollback
                )
                or (
                    state == "SAFE_INCOMPLETE_CLOCK_FORWARD_JUMP"
                    and not forward
                )
                or (
                    state == "SAFE_INCOMPLETE_PROCESS_RESTART"
                    and not process_restart
                )
                or (
                    state == "SAFE_INCOMPLETE_SOCKET_GENERATION_CHANGED"
                    and not socket_change
                )
            ):
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-terminal-state-sample-mismatch"
                )
            terminal_wall = previous_wall
            terminal_mono = previous_mono
            if terminal_wall is None or terminal_mono is None:
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-terminal-sample-missing"
                )
            elapsed_ns = terminal_mono - started_mono
            exact_complete = (
                state == "OBSERVATION_COMPLETE"
                and outcome == "ELIGIBLE_FOR_INDEPENDENT_WIRING_VERIFICATION"
                and kinds[-1] == "ACTIVE_END_OBSERVED"
                and trusted_now >= expires
                and terminal_wall >= expires
                and elapsed_ns >= ACTIVE_SECONDS * 1_000_000_000
                and max_gap_ns <= max_gap_limit_ns
                and not rollback
                and not forward
                and not process_restart
                and not socket_change
                and "STOP" not in kinds
                and "KILL" not in kinds
            )
            if state == "OBSERVATION_COMPLETE" and not exact_complete:
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-complete-state-not-independently-proven"
                )
            if state != "OBSERVATION_COMPLETE" and state not in _TERMINAL_STATES:
                raise KisDomesticFunctionalHeartbeatBlocked(
                    "heartbeat-session-not-terminal"
                )
            return {
                "schemaVersion": "kis-domestic-functional-heartbeat-evidence/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": activation["sessionId"],
                "activatedAt": activation["activatedAt"],
                "activeEndsAt": activation["expiresAt"],
                "trustedNow": _utc_text(trusted_now, "trustedNow"),
                "terminalObservedAt": _utc_text(
                    terminal_wall, "terminalObservedAt"
                ),
                "processGeneration": str(row["process_generation"]),
                "socketGeneration": str(row["socket_generation"]),
                "sampleCount": len(samples),
                "sampleHeadHash": previous_hash,
                "actualMonotonicElapsedSeconds": _decimal_text(
                    _seconds_from_ns(elapsed_ns)
                ),
                "maxHeartbeatGapSeconds": _decimal_text(
                    _seconds_from_ns(max_gap_ns)
                ),
                "wallRollbackDetected": rollback,
                "wallForwardJumpDetected": forward,
                "processRestartDetected": process_restart,
                "socketGenerationChanged": socket_change,
                "uninterrupted": exact_complete,
                "exact7200ObservationPassed": exact_complete,
                "outcome": outcome if not exact_complete else (
                    "ELIGIBLE_FOR_INDEPENDENT_WIRING_VERIFICATION"
                ),
                "functionalTestPassed": False,
                "promotionEligible": False,
                "releaseAvailable": False,
                "productionAvailable": False,
                "networkAvailable": False,
                "mutationAvailable": False,
                "schemaFingerprint": SCHEMA_FINGERPRINT,
            }
        finally:
            conn.rollback()
            conn.close()


def heartbeat_component_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-heartbeat-component/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "activeSeconds": ACTIVE_SECONDS,
        "schemaFingerprint": SCHEMA_FINGERPRINT,
        "durableMonotonicJournalAvailable": True,
        "independentConsumerAvailable": True,
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
    }


__all__ = [
    "ACTIVE_SECONDS",
    "DEFAULT_CLOCK_DIVERGENCE_SECONDS",
    "DEFAULT_MAX_GAP_SECONDS",
    "KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_RELEASE_AVAILABLE",
    "SCHEMA_FINGERPRINT",
    "DurableKisDomesticFunctionalHeartbeat",
    "KisDomesticFunctionalHeartbeatBlocked",
    "KisDomesticFunctionalHeartbeatVerifier",
    "heartbeat_component_status",
]
