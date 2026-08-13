from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
import sqlite3
import threading
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from .kis_domestic_functional_contract import PDNO, ROUTE
from .kis_domestic_functional_high_water import (
    AppendOnlyKisBootstrapHighWater,
    MAIN_PROJECTION_SCHEMA,
)
from .process_safety import CrossProcessLease, acquire_process_lease
from trading_runtime.market_calendar import session_bounds_utc


KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_MINT_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_RELEASE_AVAILABLE = False

SCHEMA_VERSION = "kis-domestic-functional-bootstrap-schema/v2"
PREAPPROVAL_SCHEMA = "kis-domestic-functional-bootstrap-preapproval/v1"
ISSUANCE_BUNDLE_SCHEMA = "kis-domestic-functional-bootstrap-issuance-bundle/v1"
ROUTE_RECORD_SCHEMA = "kis-domestic-functional-bootstrap-route-record/v1"
STATUS_SCHEMA = "kis-domestic-functional-bootstrap-status/v1"

_PREAPPROVAL_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_PREAPPROVAL\0"
_ISSUANCE_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_ISSUANCE\0"
_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", re.ASCII)
_KST = ZoneInfo("Asia/Seoul")
_ZERO = "0" * 64
_CAPS = {
    "orderQuantity": 1,
    "maxOrderKrw": "100000",
    "maxGrossKrw": "100000",
    "ownerLossTriggerStrictlyBelowKrw": "5000",
    "activeSeconds": 7200,
    "naturalSellExpected": False,
    "promotionAllowed": False,
}
_PREAPPROVAL_KEYS = {
    "schemaVersion", "route", "pdno", "armId", "templateId",
    "tradingDate", "ownerEpoch", "ownerRecordHash",
    "registryAcceptedHeadHash", "accountFingerprint",
    "credentialConfigurationHash", "codeManifestHash",
    "artifactCanonicalHash", "instanceCanonicalHash",
    "userExactRestatementHash", "approvedAt", "expiresAt", "caps",
    "publicMarketDataOnlyBeforeIssue", "privateAccountAuthority",
    "orderAuthority", "oneUse", "nonPromotion", "productionMinted",
    "authorityKeyIdHash",
}
_BUNDLE_KEYS = {
    "schemaVersion", "route", "pdno", "issuanceId", "armId",
    "approvalRecordHash", "ownerEpoch", "ownerRecordHash",
    "registryAcceptedHeadHash", "accountFingerprint",
    "credentialConfigurationHash", "codeManifestHash",
    "artifactCanonicalHash", "instanceCanonicalHash",
    "userExactRestatementHash", "naturalSignal", "rollingPreflight",
    "freshQuote", "laneGrant", "observedAt", "observedMonotonicNs",
    "externalComponentCasRequested", "productionMinted",
    "authorityKeyIdHash",
}
_SIGNAL_KEYS = {
    "classification", "present", "evaluationId", "evaluationHash",
    "triggerId", "triggerHash", "triggerOpenAt", "observedAt", "source",
}
_ROLLING_KEYS = {
    "snapshotId", "snapshotHash", "receiptHash", "state", "completedAt",
    "expiresAt",
}
_QUOTE_KEYS = {
    "receiptId", "receiptHash", "state", "observedAt", "expiresAt",
    "orderAuthorityFresh",
}
_GRANT_KEYS = {
    "receiptId", "receiptHash", "grantWallAt", "grantMonotonicNs", "state",
}
_ROUTE_KEYS = {
    "schemaVersion", "route", "pdno", "phase", "revision", "everIssued",
    "activeArmId", "issuanceId", "ownerEpoch", "ownerRecordHash",
    "registryAcceptedHeadHash", "accountFingerprint",
    "credentialConfigurationHash", "codeManifestHash",
    "externalHighWaterBindingHash", "updatedAt", "updatedMonotonicNs",
    "publicMarketDataOnly", "privateAccountAuthority", "orderAuthority",
    "previousTransitionHash", "reason",
}
_MAIN_SQL = (
    """CREATE TABLE IF NOT EXISTS kis_functional_bootstrap_meta(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        schema_version TEXT NOT NULL,
        schema_fingerprint TEXT NOT NULL,
        bindings_hash TEXT NOT NULL,
        external_high_water_binding_hash TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS kis_functional_bootstrap_route(
        route TEXT PRIMARY KEY,
        phase TEXT NOT NULL CHECK(phase IN ('ARMED_WAIT','EXPIRED','ISSUED','BURNED')),
        revision INTEGER NOT NULL CHECK(revision>=1),
        ever_issued INTEGER NOT NULL CHECK(ever_issued IN (0,1)),
        active_arm_id TEXT,
        issuance_id TEXT,
        updated_at TEXT NOT NULL,
        updated_monotonic_ns INTEGER NOT NULL CHECK(updated_monotonic_ns>=0),
        record_json TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        transition_head_hash TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS kis_functional_bootstrap_arm(
        arm_id TEXT PRIMARY KEY,
        route TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('ARMED_WAIT','EXPIRED','CONSUMED')),
        approval_hash TEXT NOT NULL UNIQUE,
        user_restatement_hash TEXT NOT NULL,
        approved_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        body_json TEXT NOT NULL,
        body_hash TEXT NOT NULL,
        signature TEXT NOT NULL,
        authority_key_id_hash TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK(revision>=1)
    )""",
    """CREATE TABLE IF NOT EXISTS kis_functional_bootstrap_issue(
        route TEXT PRIMARY KEY,
        issuance_id TEXT NOT NULL UNIQUE,
        arm_id TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL CHECK(state IN ('ISSUED','BURNED')),
        issuance_binding_hash TEXT NOT NULL,
        bundle_hash TEXT NOT NULL,
        external_high_water_epoch INTEGER NOT NULL CHECK(external_high_water_epoch=1),
        external_high_water_head_hash TEXT NOT NULL,
        issued_at TEXT NOT NULL,
        body_json TEXT NOT NULL,
        body_hash TEXT NOT NULL,
        signature TEXT NOT NULL,
        authority_key_id_hash TEXT NOT NULL,
        failure_reason TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS kis_functional_bootstrap_transition(
        route TEXT NOT NULL,
        revision INTEGER NOT NULL,
        phase TEXT NOT NULL,
        previous_hash TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        occurred_monotonic_ns INTEGER NOT NULL CHECK(occurred_monotonic_ns>=0),
        record_json TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        PRIMARY KEY(route,revision)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_kis_functional_bootstrap_arm_route ON kis_functional_bootstrap_arm(route,state)",
    "CREATE INDEX IF NOT EXISTS idx_kis_functional_bootstrap_transition_route ON kis_functional_bootstrap_transition(route,revision)",
)


class KisDomesticFunctionalBootstrapBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True,
            separators=(",", ":"),
        ).encode()
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalBootstrapBlocked("bootstrap-json-invalid") from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalBootstrapBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalBootstrapBlocked(f"{label}-invalid")
    return value


def _utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif type(value) is str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise KisDomesticFunctionalBootstrapBlocked(f"{label}-invalid") from exc
    else:
        raise KisDomesticFunctionalBootstrapBlocked(f"{label}-invalid")
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalBootstrapBlocked(f"{label}-invalid")
    result = parsed.astimezone(timezone.utc)
    if type(value) is str and _iso(result) != value:
        raise KisDomesticFunctionalBootstrapBlocked(f"{label}-not-canonical")
    return result


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _mono(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise KisDomesticFunctionalBootstrapBlocked(f"{label}-invalid")
    return value


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().split()).lower()


def _schema_snapshot(conn: sqlite3.Connection, prefix: str) -> dict[str, Any]:
    objects = [
        (str(row[0]), str(row[1]), str(row[2]), _normalize_sql(row[3]))
        for row in conn.execute(
            "SELECT name,type,tbl_name,sql FROM sqlite_master "
            "WHERE name LIKE ? OR tbl_name LIKE ? ORDER BY type,name",
            (prefix + "%", prefix + "%"),
        ).fetchall()
    ]
    tables = sorted({row[2] for row in objects if row[1] == "table"})
    columns = {
        table: [tuple(item) for item in conn.execute(
            f'PRAGMA table_xinfo("{table}")'
        )]
        for table in tables
    }
    indexes = {
        table: [tuple(item) for item in conn.execute(
            f'PRAGMA index_list("{table}")'
        )]
        for table in tables
    }
    index_names = sorted({str(row[1]) for rows in indexes.values() for row in rows})
    index_info = {
        name: [tuple(item) for item in conn.execute(
            f'PRAGMA index_xinfo("{name}")'
        )]
        for name in index_names
    }
    return {
        "objects": objects,
        "columns": columns,
        "indexes": indexes,
        "indexInfo": index_info,
    }


def _expected_schema(sql: tuple[str, ...], prefix: str) -> tuple[dict[str, Any], str]:
    conn = sqlite3.connect(":memory:")
    try:
        for statement in sql:
            conn.execute(statement)
        snapshot = _schema_snapshot(conn, prefix)
        return snapshot, _hash(snapshot)
    finally:
        conn.close()


_EXPECTED_MAIN, MAIN_SCHEMA_FINGERPRINT = _expected_schema(
    _MAIN_SQL, "kis_functional_bootstrap_"
)


def _public_key(pem: Any, key_id_hash: str) -> ECC.EccKey:
    if type(pem) is not str or "PRIVATE" in pem:
        raise KisDomesticFunctionalBootstrapBlocked("bootstrap-public-key-invalid")
    try:
        key = ECC.import_key(pem)
    except (ValueError, TypeError) as exc:
        raise KisDomesticFunctionalBootstrapBlocked("bootstrap-public-key-invalid") from exc
    if key.has_private() or key.curve != "Ed25519":
        raise KisDomesticFunctionalBootstrapBlocked("bootstrap-public-key-invalid")
    exported = key.export_key(format="PEM")
    if not hmac.compare_digest(
        hashlib.sha256(exported.encode()).hexdigest(),
        _sha(key_id_hash, "bootstrap-authority-key-id"),
    ):
        raise KisDomesticFunctionalBootstrapBlocked("bootstrap-public-key-id-mismatch")
    return key


def _verify_signature(
    key: ECC.EccKey, domain: bytes, body: Mapping[str, Any], signature: Any
) -> bool:
    if type(signature) is not str:
        return False
    try:
        raw = base64.b64decode(signature, validate=True)
        eddsa.new(key, mode="rfc8032").verify(domain + _canonical(body), raw)
        return True
    except (ValueError, TypeError):
        return False


@dataclass(frozen=True, slots=True)
class BootstrapBindings:
    owner_epoch: int
    owner_record_hash: str
    registry_accepted_head_hash: str
    account_fingerprint: str
    credential_configuration_hash: str
    code_manifest_hash: str
    artifact_canonical_hash: str
    instance_canonical_hash: str
    user_exact_restatement_hash: str | None
    authority_key_id_hash: str

    def body(self) -> dict[str, Any]:
        if type(self.owner_epoch) is not int or self.owner_epoch < 1:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-owner-epoch-invalid")
        restatement = self.user_exact_restatement_hash
        if restatement is not None:
            restatement = _sha(restatement, "bootstrap-user-restatement")
        return {
            "schemaVersion": "kis-domestic-functional-bootstrap-bindings/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "ownerEpoch": self.owner_epoch,
            "ownerRecordHash": _sha(self.owner_record_hash, "bootstrap-owner-record"),
            "registryAcceptedHeadHash": _sha(
                self.registry_accepted_head_hash, "bootstrap-registry-head"
            ),
            "accountFingerprint": _sha(self.account_fingerprint, "bootstrap-account"),
            "credentialConfigurationHash": _sha(
                self.credential_configuration_hash, "bootstrap-credential"
            ),
            "codeManifestHash": _sha(self.code_manifest_hash, "bootstrap-code"),
            "artifactCanonicalHash": _sha(
                self.artifact_canonical_hash, "bootstrap-artifact"
            ),
            "instanceCanonicalHash": _sha(
                self.instance_canonical_hash, "bootstrap-instance"
            ),
            "userExactRestatementHash": restatement,
            "authorityKeyIdHash": _sha(
                self.authority_key_id_hash, "bootstrap-authority-key"
            ),
        }


class _RemovedLocalSqliteHighWater:
    """Non-callable migration tombstone; external signed anchor is mandatory."""

    def __init__(self, path: str | Path, *, expected_installation_hash: str) -> None:
        raise KisDomesticFunctionalBootstrapBlocked(
            "bootstrap-local-sqlite-high-water-removed"
        )
        self.path = Path(path).expanduser().resolve()
        self.installation_hash = _sha(
            expected_installation_hash, "bootstrap-high-water-installation"
        )
        self._thread_lock = threading.RLock()
        self._closed = False
        if not self.path.is_file():
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-missing")
        scope = (
            "live-trader:kis-domestic-functional-bootstrap-high-water:"
            + self.installation_hash
        )
        lease = acquire_process_lease(scope)
        if type(lease) is not CrossProcessLease:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-os-lease-unavailable"
            )
        self._lease = lease
        try:
            self.read()
        except BaseException:
            self.close()
            raise

    @classmethod
    def provision(cls, path: str | Path) -> "_RemovedLocalSqliteHighWater":
        raise KisDomesticFunctionalBootstrapBlocked(
            "bootstrap-local-sqlite-high-water-removed"
        )
        target = Path(path).expanduser().resolve()
        if target.exists():
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-provision-target-exists"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        installation_id = "kis-bootstrap-high-water-" + uuid.uuid4().hex
        installation_body = {
            "schemaVersion": "kis-domestic-functional-bootstrap-high-water-installation/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "installationId": installation_id,
        }
        installation_hash = _hash(installation_body)
        conn = sqlite3.connect(target)
        try:
            for statement in _HIGH_WATER_SQL:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO kis_functional_bootstrap_high_water_meta VALUES(1,?,?,?,?)",
                (
                    HIGH_WATER_SCHEMA_VERSION,
                    HIGH_WATER_SCHEMA_FINGERPRINT,
                    installation_id,
                    installation_hash,
                ),
            )
            body = {
                "schemaVersion": HIGH_WATER_RECORD_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "installationHash": installation_hash,
                "everIssued": False,
                "version": 0,
                "issuanceBindingHash": None,
                "issuedAt": None,
                "issuedMonotonicNs": None,
                "previousHeadHash": _ZERO,
            }
            conn.execute(
                "INSERT INTO kis_functional_bootstrap_high_water_route VALUES(?,?,?,?,?,?,?,?,?)",
                (ROUTE, 0, 0, None, None, None, _canonical(body).decode(), _hash(body), _ZERO),
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return cls(target, expected_installation_hash=installation_hash)

    def close(self) -> None:
        with self._thread_lock:
            if not self._closed:
                self._lease.release()
                self._closed = True

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _verify(self, conn: sqlite3.Connection) -> tuple[sqlite3.Row, dict[str, Any]]:
        if self._closed:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-closed")
        status = self._lease.status(reused=True)
        if status.get("acquired") is not True or status.get("reused") is not True:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-os-lease-lost")
        if _schema_snapshot(conn, "kis_functional_bootstrap_high_water_") != _EXPECTED_HIGH_WATER:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-schema-dirty")
        meta = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_high_water_meta"
        ).fetchall()
        if len(meta) != 1 or tuple(meta[0]) != (
            1,
            HIGH_WATER_SCHEMA_VERSION,
            HIGH_WATER_SCHEMA_FINGERPRINT,
            meta[0][3],
            self.installation_hash,
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-meta-dirty")
        installation_body = {
            "schemaVersion": "kis-domestic-functional-bootstrap-high-water-installation/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "installationId": meta[0][3],
        }
        if _hash(installation_body) != self.installation_hash:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-installation-mismatch"
            )
        row = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_high_water_route WHERE route=?",
            (ROUTE,),
        ).fetchone()
        if row is None:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-row-missing")
        try:
            body = json.loads(row["record_json"])
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-json-invalid") from None
        projection = {
            "schemaVersion": HIGH_WATER_RECORD_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "installationHash": self.installation_hash,
            "everIssued": bool(row["ever_issued"]),
            "version": row["version"],
            "issuanceBindingHash": row["issuance_binding_hash"],
            "issuedAt": row["issued_at"],
            "issuedMonotonicNs": row["issued_monotonic_ns"],
            "previousHeadHash": _ZERO,
        }
        if (
            type(row["ever_issued"]) is not int
            or row["ever_issued"] not in (0, 1)
            or body != projection
            or row["record_hash"] != _hash(body)
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-row-dirty")
        transitions = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_high_water_transition "
            "WHERE route=? ORDER BY version",
            (ROUTE,),
        ).fetchall()
        if not body["everIssued"]:
            if body["version"] != 0 or transitions or row["transition_head_hash"] != _ZERO:
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-unissued-dirty")
        else:
            transition_body: Mapping[str, Any] | None = None
            if len(transitions) == 1:
                try:
                    parsed_transition = json.loads(transitions[0]["record_json"])
                    transition_body = (
                        parsed_transition
                        if isinstance(parsed_transition, Mapping)
                        else None
                    )
                except (TypeError, json.JSONDecodeError):
                    transition_body = None
            expected_transition = {
                "schemaVersion": "kis-domestic-functional-bootstrap-high-water-transition/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "installationHash": self.installation_hash,
                "version": 1,
                "previousHash": _ZERO,
                "issuanceBindingHash": body["issuanceBindingHash"],
                "occurredAt": body["issuedAt"],
                "occurredMonotonicNs": body["issuedMonotonicNs"],
                "recordHash": row["record_hash"],
            }
            if (
                body["version"] != 1
                or len(transitions) != 1
                or transition_body != expected_transition
                or transitions[0]["previous_hash"] != _ZERO
                or transitions[0]["issuance_binding_hash"] != body["issuanceBindingHash"]
                or transitions[0]["occurred_at"] != body["issuedAt"]
                or transitions[0]["occurred_monotonic_ns"]
                != body["issuedMonotonicNs"]
                or transitions[0]["record_hash"] != _hash(expected_transition)
                or row["transition_head_hash"] != transitions[0]["record_hash"]
            ):
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-transition-dirty")
        return row, body

    def read(self) -> dict[str, Any]:
        with self._thread_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN")
                row, body = self._verify(conn)
                result = {
                    "body": deepcopy(body),
                    "recordHash": row["record_hash"],
                    "transitionHeadHash": row["transition_head_hash"],
                    "osLeaseHeld": True,
                    "productionAuthorityAvailable": False,
                }
                conn.commit()
                return {**result, "receiptHash": _hash(result)}
            finally:
                conn.close()

    def reserve(
        self,
        *,
        issuance_binding_hash: str,
        occurred_at: datetime,
        occurred_monotonic_ns: int,
    ) -> dict[str, Any]:
        binding = _sha(issuance_binding_hash, "bootstrap-high-water-issuance-binding")
        now = _utc(occurred_at, "bootstrap-high-water-occurred-at")
        mono = _mono(occurred_monotonic_ns, "bootstrap-high-water-monotonic")
        with self._thread_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row, prior = self._verify(conn)
                if prior["everIssued"]:
                    raise KisDomesticFunctionalBootstrapBlocked(
                        "bootstrap-route-ever-issued-burned"
                    )
                body = {
                    **prior,
                    "everIssued": True,
                    "version": 1,
                    "issuanceBindingHash": binding,
                    "issuedAt": _iso(now),
                    "issuedMonotonicNs": mono,
                }
                record_hash = _hash(body)
                transition = {
                    "schemaVersion": "kis-domestic-functional-bootstrap-high-water-transition/v1",
                    "route": ROUTE,
                    "pdno": PDNO,
                    "installationHash": self.installation_hash,
                    "version": 1,
                    "previousHash": _ZERO,
                    "issuanceBindingHash": binding,
                    "occurredAt": _iso(now),
                    "occurredMonotonicNs": mono,
                    "recordHash": record_hash,
                }
                transition_hash = _hash(transition)
                changed = conn.execute(
                    "UPDATE kis_functional_bootstrap_high_water_route SET "
                    "ever_issued=1,version=1,issuance_binding_hash=?,issued_at=?,"
                    "issued_monotonic_ns=?,record_json=?,record_hash=?,transition_head_hash=? "
                    "WHERE route=? AND ever_issued=0 AND version=0 AND record_hash=?",
                    (
                        binding, _iso(now), mono, _canonical(body).decode(),
                        record_hash, transition_hash, ROUTE, row["record_hash"],
                    ),
                ).rowcount
                if changed != 1:
                    raise KisDomesticFunctionalBootstrapBlocked(
                        "bootstrap-high-water-reserve-cas-failed"
                    )
                conn.execute(
                    "INSERT INTO kis_functional_bootstrap_high_water_transition "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (
                        ROUTE, 1, _ZERO, _iso(now), mono, binding,
                        _canonical(transition).decode(), transition_hash,
                    ),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
            return self.read()


class DurableKisDomesticFunctionalBootstrap:
    def __init__(
        self,
        database_path: str | Path,
        *,
        bindings: BootstrapBindings,
        preapproval_public_key_pem: str,
        high_water: AppendOnlyKisBootstrapHighWater,
        failure_injector: Callable[[str], None] | None = None,
    ) -> None:
        if type(bindings) is not BootstrapBindings:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-bindings-type-invalid")
        if type(high_water) is not AppendOnlyKisBootstrapHighWater:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-high-water-type-invalid")
        if failure_injector is not None and not callable(failure_injector):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-failure-injector-invalid")
        self.path = Path(database_path).expanduser().resolve()
        self.bindings = bindings.body()
        self.bindings_hash = _hash(self.bindings)
        self.high_water = high_water
        initial_high_water = self._read_external_high_water()
        self.high_water_binding = self._external_high_water_binding(
            initial_high_water
        )
        self.high_water_binding_hash = _hash(self.high_water_binding)
        self.authority_key = _public_key(
            preapproval_public_key_pem, self.bindings["authorityKeyIdHash"]
        )
        self.failure_injector = failure_injector
        self._lock = threading.RLock()
        self._prepare_schema()
        self._reconcile_high_water()

    def _read_external_high_water(self) -> dict[str, Any]:
        status = self.high_water.read()
        status_hash = status.get("statusHash")
        unsigned = {key: value for key, value in status.items() if key != "statusHash"}
        if (
            type(status_hash) is not str
            or status_hash != _hash(unsigned)
            or status.get("route") != ROUTE
            or status.get("pdno") != PDNO
            or status.get("ownerEpoch") != self.bindings["ownerEpoch"]
            or status.get("ownerRecordHash") != self.bindings["ownerRecordHash"]
            or status.get("registryAcceptedHeadHash")
            != self.bindings["registryAcceptedHeadHash"]
            or status.get("verifyOnlyConsumer") is not True
            or status.get("privateSignerPresent") is not False
            or status.get("rootRegistrySignatureVerified") is not True
            or status.get("writerCertificateRootVerified") is not True
            or status.get("appendOnlyChainVerified") is not True
            or status.get("minimumRollbackPinSuppliedAndVerified") is not True
            or status.get("osProcessLeaseHeld") is not True
            or status.get("productionWriterAvailable") is not False
            or status.get("productionAvailable") is not False
            or status.get("networkOrderPostAllowed") is not False
            or status.get("tradingMutationCount") != 0
        ):
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-external-high-water-status-invalid"
            )
        return status

    def _external_high_water_binding(
        self, status: Mapping[str, Any]
    ) -> dict[str, Any]:
        binding = {
            "schemaVersion": "kis-domestic-functional-bootstrap-external-high-water-binding/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "anchorId": _identifier(
                status.get("anchorId"), "bootstrap-high-water-anchor-id"
            ),
            "anchorPathHash": _sha(
                status.get("anchorPathHash"), "bootstrap-high-water-path"
            ),
            "ownerEpoch": self.bindings["ownerEpoch"],
            "ownerRecordHash": self.bindings["ownerRecordHash"],
            "registryId": _identifier(
                status.get("registryId"), "bootstrap-high-water-registry-id"
            ),
            "registryEpoch": status.get("registryEpoch"),
            "registryAcceptedHeadHash": self.bindings[
                "registryAcceptedHeadHash"
            ],
            "rootKeyIdHash": _sha(
                status.get("rootKeyIdHash"), "bootstrap-high-water-root-key"
            ),
            "writerKeyIdHash": _sha(
                status.get("writerKeyIdHash"), "bootstrap-high-water-writer-key"
            ),
        }
        if type(binding["registryEpoch"]) is not int or binding["registryEpoch"] < 1:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-registry-epoch-invalid"
            )
        return binding

    def _assert_external_high_water_binding(self) -> dict[str, Any]:
        status = self._read_external_high_water()
        if self._external_high_water_binding(status) != self.high_water_binding:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-external-high-water-binding-drift"
            )
        return status

    def _external_main_projection(
        self,
        status: Mapping[str, Any],
        *,
        ever_issued: bool,
        issuance_binding_hash: str | None,
    ) -> dict[str, Any]:
        if type(ever_issued) is not bool:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-main-ever-issued-invalid"
            )
        if ever_issued:
            binding = _sha(
                issuance_binding_hash,
                "bootstrap-high-water-main-issuance-binding",
            )
        elif issuance_binding_hash is not None:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-main-unissued-binding-present"
            )
        else:
            binding = None
        return {
            "schemaVersion": MAIN_PROJECTION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "anchorId": self.high_water_binding["anchorId"],
            "anchorEpoch": status["epoch"],
            "anchorHeadHash": status["headHash"],
            "everIssued": ever_issued,
            "issuanceBindingHash": binding,
            "ownerEpoch": self.bindings["ownerEpoch"],
            "ownerRecordHash": self.bindings["ownerRecordHash"],
            "registryAcceptedHeadHash": self.bindings[
                "registryAcceptedHeadHash"
            ],
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _prepare_schema(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            before = _schema_snapshot(conn, "kis_functional_bootstrap_")
            if before["objects"] and before != _EXPECTED_MAIN:
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-schema-dirty")
            for statement in _MAIN_SQL:
                conn.execute(statement)
            self._assert_external_high_water_binding()
            conn.execute(
                "INSERT OR IGNORE INTO kis_functional_bootstrap_meta VALUES(1,?,?,?,?)",
                (
                    SCHEMA_VERSION, MAIN_SCHEMA_FINGERPRINT, self.bindings_hash,
                    self.high_water_binding_hash,
                ),
            )
            self._verify_meta(conn)
            conn.commit()
        finally:
            conn.close()

    def _verify_meta(self, conn: sqlite3.Connection) -> None:
        if _schema_snapshot(conn, "kis_functional_bootstrap_") != _EXPECTED_MAIN:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-schema-dirty")
        rows = conn.execute("SELECT * FROM kis_functional_bootstrap_meta").fetchall()
        if len(rows) != 1 or tuple(rows[0]) != (
            1,
            SCHEMA_VERSION,
            MAIN_SCHEMA_FINGERPRINT,
            self.bindings_hash,
            self.high_water_binding_hash,
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-schema-meta-dirty")

    def _route_body(
        self,
        *,
        phase: str,
        revision: int,
        ever_issued: bool,
        active_arm_id: str | None,
        issuance_id: str | None,
        now: datetime,
        monotonic_ns: int,
        previous_hash: str,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "schemaVersion": ROUTE_RECORD_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "phase": phase,
            "revision": revision,
            "everIssued": ever_issued,
            "activeArmId": active_arm_id,
            "issuanceId": issuance_id,
            "ownerEpoch": self.bindings["ownerEpoch"],
            "ownerRecordHash": self.bindings["ownerRecordHash"],
            "registryAcceptedHeadHash": self.bindings["registryAcceptedHeadHash"],
            "accountFingerprint": self.bindings["accountFingerprint"],
            "credentialConfigurationHash": self.bindings["credentialConfigurationHash"],
            "codeManifestHash": self.bindings["codeManifestHash"],
            "externalHighWaterBindingHash": self.high_water_binding_hash,
            "updatedAt": _iso(now),
            "updatedMonotonicNs": monotonic_ns,
            "publicMarketDataOnly": True,
            "privateAccountAuthority": False,
            "orderAuthority": False,
            "previousTransitionHash": previous_hash,
            "reason": reason,
        }

    def _write_route(
        self,
        conn: sqlite3.Connection,
        body: Mapping[str, Any],
        *,
        expected_revision: int | None,
    ) -> None:
        if set(body) != _ROUTE_KEYS or type(body.get("everIssued")) is not bool:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-body-not-exact")
        _mono(body.get("updatedMonotonicNs"), "bootstrap-route-updated-monotonic")
        updated = _utc(body.get("updatedAt"), "bootstrap-route-updated-at")
        if expected_revision is not None:
            prior = conn.execute(
                "SELECT updated_at,updated_monotonic_ns FROM "
                "kis_functional_bootstrap_route WHERE route=? AND revision=?",
                (ROUTE, expected_revision),
            ).fetchone()
            if (
                prior is None
                or updated < _utc(prior[0], "bootstrap-route-prior-updated-at")
                or body["updatedMonotonicNs"] < prior[1]
            ):
                raise KisDomesticFunctionalBootstrapBlocked(
                    "bootstrap-route-clock-rollback"
                )
        record_hash = _hash(body)
        values = (
            body["phase"], body["revision"], int(body["everIssued"]),
            body["activeArmId"], body["issuanceId"], body["updatedAt"],
            body["updatedMonotonicNs"], _canonical(body).decode(), record_hash,
            record_hash,
        )
        if expected_revision is None:
            conn.execute(
                "INSERT INTO kis_functional_bootstrap_route VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (ROUTE,) + values,
            )
        else:
            changed = conn.execute(
                "UPDATE kis_functional_bootstrap_route SET phase=?,revision=?,"
                "ever_issued=?,active_arm_id=?,issuance_id=?,updated_at=?,"
                "updated_monotonic_ns=?,record_json=?,record_hash=?,transition_head_hash=? "
                "WHERE route=? AND revision=?",
                values + (ROUTE, expected_revision),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-cas-failed")
        conn.execute(
            "INSERT INTO kis_functional_bootstrap_transition VALUES(?,?,?,?,?,?,?,?)",
            (
                ROUTE, body["revision"], body["phase"],
                body["previousTransitionHash"], body["updatedAt"],
                body["updatedMonotonicNs"], _canonical(body).decode(), record_hash,
            ),
        )

    def _load_route(self, conn: sqlite3.Connection) -> tuple[sqlite3.Row | None, dict[str, Any] | None]:
        self._verify_meta(conn)
        row = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_route WHERE route=?", (ROUTE,)
        ).fetchone()
        if row is None:
            if conn.execute("SELECT COUNT(*) FROM kis_functional_bootstrap_transition").fetchone()[0]:
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-history-orphaned")
            return None, None
        try:
            body = json.loads(row["record_json"])
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-json-invalid") from None
        projection = {
            "phase": row["phase"], "revision": row["revision"],
            "everIssued": bool(row["ever_issued"]),
            "activeArmId": row["active_arm_id"], "issuanceId": row["issuance_id"],
            "updatedAt": row["updated_at"],
            "updatedMonotonicNs": row["updated_monotonic_ns"],
        }
        if (
            not isinstance(body, Mapping)
            or set(body) != _ROUTE_KEYS
            or body.get("schemaVersion") != ROUTE_RECORD_SCHEMA
            or body.get("route") != ROUTE
            or body.get("pdno") != PDNO
            or any(body.get(key) != value for key, value in projection.items())
            or any(body.get(key) != self.bindings[key] for key in (
                "ownerEpoch", "ownerRecordHash", "registryAcceptedHeadHash",
                "accountFingerprint", "credentialConfigurationHash", "codeManifestHash",
            ))
            or body.get("externalHighWaterBindingHash")
            != self.high_water_binding_hash
            or body.get("publicMarketDataOnly") is not True
            or body.get("privateAccountAuthority") is not False
            or body.get("orderAuthority") is not False
            or type(row["ever_issued"]) is not int
            or row["ever_issued"] not in (0, 1)
            or row["record_hash"] != _hash(body)
            or row["transition_head_hash"] != row["record_hash"]
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-row-dirty")
        transitions = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_transition WHERE route=? ORDER BY revision",
            (ROUTE,),
        ).fetchall()
        previous = _ZERO
        previous_at: datetime | None = None
        previous_mono: int | None = None
        last_item: Mapping[str, Any] | None = None
        for revision, transition in enumerate(transitions, 1):
            try:
                item = json.loads(transition["record_json"])
            except (TypeError, json.JSONDecodeError):
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-transition-json-invalid") from None
            occurred = _utc(transition["occurred_at"], "bootstrap-transition-time")
            mono = _mono(transition["occurred_monotonic_ns"], "bootstrap-transition-monotonic")
            if (
                not isinstance(item, Mapping)
                or set(item) != _ROUTE_KEYS
                or item.get("schemaVersion") != ROUTE_RECORD_SCHEMA
                or item.get("route") != ROUTE
                or item.get("pdno") != PDNO
                or any(item.get(key) != self.bindings[key] for key in (
                    "ownerEpoch", "ownerRecordHash", "registryAcceptedHeadHash",
                    "accountFingerprint", "credentialConfigurationHash",
                    "codeManifestHash",
                ))
                or item.get("externalHighWaterBindingHash")
                != self.high_water_binding_hash
                or item.get("publicMarketDataOnly") is not True
                or item.get("privateAccountAuthority") is not False
                or item.get("orderAuthority") is not False
                or type(item.get("everIssued")) is not bool
                or (
                    item.get("phase") in {"ARMED_WAIT", "EXPIRED"}
                    and item.get("everIssued") is not False
                )
                or (
                    item.get("phase") in {"ISSUED", "BURNED"}
                    and item.get("everIssued") is not True
                )
                or transition["revision"] != revision
                or item.get("revision") != revision
                or transition["phase"] != item.get("phase")
                or transition["previous_hash"] != previous
                or item.get("previousTransitionHash") != previous
                or transition["record_hash"] != _hash(item)
                or item.get("updatedAt") != transition["occurred_at"]
                or item.get("updatedMonotonicNs") != mono
                or (previous_at is not None and occurred < previous_at)
                or (previous_mono is not None and mono < previous_mono)
            ):
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-transition-chain-dirty")
            previous = transition["record_hash"]
            previous_at = occurred
            previous_mono = mono
            last_item = item
        if (
            len(transitions) != body["revision"]
            or previous != row["record_hash"]
            or last_item != body
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-transition-head-dirty")
        return row, dict(body)

    def _verify_envelope(
        self, envelope: Mapping[str, Any], *, domain: bytes, expected_keys: set[str]
    ) -> tuple[dict[str, Any], str, str]:
        if not isinstance(envelope, Mapping) or set(envelope) != {
            "body", "recordHash", "signature", "authorityKeyIdHash"
        }:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-envelope-not-exact")
        body = envelope.get("body")
        if not isinstance(body, Mapping) or set(body) != expected_keys:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-envelope-body-not-exact")
        body = dict(body)
        record_hash = _sha(envelope.get("recordHash"), "bootstrap-envelope-hash")
        key_id = _sha(envelope.get("authorityKeyIdHash"), "bootstrap-envelope-key")
        if (
            record_hash != _hash(body)
            or key_id != self.bindings["authorityKeyIdHash"]
            or body.get("authorityKeyIdHash") != key_id
            or not _verify_signature(self.authority_key, domain, body, envelope.get("signature"))
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-envelope-signature-invalid")
        return body, record_hash, str(envelope["signature"])

    def _verify_preapproval(
        self,
        envelope: Mapping[str, Any],
        *,
        now: datetime,
        require_active: bool = True,
    ) -> tuple[dict[str, Any], str, str]:
        body, record_hash, signature = self._verify_envelope(
            envelope, domain=_PREAPPROVAL_DOMAIN, expected_keys=_PREAPPROVAL_KEYS
        )
        expected = {
            "schemaVersion": PREAPPROVAL_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "ownerEpoch": self.bindings["ownerEpoch"],
            "ownerRecordHash": self.bindings["ownerRecordHash"],
            "registryAcceptedHeadHash": self.bindings["registryAcceptedHeadHash"],
            "accountFingerprint": self.bindings["accountFingerprint"],
            "credentialConfigurationHash": self.bindings["credentialConfigurationHash"],
            "codeManifestHash": self.bindings["codeManifestHash"],
            "artifactCanonicalHash": self.bindings["artifactCanonicalHash"],
            "instanceCanonicalHash": self.bindings["instanceCanonicalHash"],
            "userExactRestatementHash": self.bindings["userExactRestatementHash"],
            "caps": _CAPS,
            "publicMarketDataOnlyBeforeIssue": True,
            "privateAccountAuthority": False,
            "orderAuthority": False,
            "oneUse": True,
            "nonPromotion": True,
            "productionMinted": False,
            "authorityKeyIdHash": self.bindings["authorityKeyIdHash"],
        }
        if any(type(body.get(key)) is not type(value) or body.get(key) != value for key, value in expected.items()):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-preapproval-binding-mismatch")
        _identifier(body.get("armId"), "bootstrap-arm-id")
        _identifier(body.get("templateId"), "bootstrap-template-id")
        if type(body.get("tradingDate")) is not str or not _DATE.fullmatch(body["tradingDate"]):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-trading-date-invalid")
        approved = _utc(body.get("approvedAt"), "bootstrap-approved-at")
        expires = _utc(body.get("expiresAt"), "bootstrap-expires-at")
        try:
            trading_date = date.fromisoformat(body["tradingDate"])
            session_open, session_close = session_bounds_utc("XKRX", trading_date)
        except ValueError as exc:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-preapproval-not-xkrx-session"
            ) from exc
        approved_kst = approved.astimezone(_KST)
        expires_kst = expires.astimezone(_KST)
        if (
            approved > now
            or (require_active and now >= expires)
            or approved >= expires
            or approved_kst.date().isoformat() != body["tradingDate"]
            or expires_kst.date().isoformat() != body["tradingDate"]
            or approved_kst.timetz().replace(tzinfo=None) >= wall_time(9, 0)
            or expires_kst.timetz().replace(tzinfo=None) > wall_time(13, 15)
            or expires_kst.timetz().replace(tzinfo=None) < wall_time(9, 0)
            or approved >= session_open
            or expires < session_open
            or expires > session_close
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-preapproval-time-invalid")
        return body, record_hash, signature

    def _load_arm_verified(
        self,
        conn: sqlite3.Connection,
        *,
        arm_id: str,
        now: datetime,
        require_active: bool,
    ) -> sqlite3.Row:
        arm = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_arm WHERE arm_id=?",
            (arm_id,),
        ).fetchone()
        if arm is None:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-arm-missing")
        try:
            stored_body = json.loads(arm["body_json"])
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-arm-json-invalid"
            ) from None
        envelope = {
            "body": stored_body,
            "recordHash": arm["body_hash"],
            "signature": arm["signature"],
            "authorityKeyIdHash": arm["authority_key_id_hash"],
        }
        body, record_hash, signature = self._verify_preapproval(
            envelope, now=now, require_active=require_active
        )
        if (
            arm["route"] != ROUTE
            or arm["state"] not in {"ARMED_WAIT", "EXPIRED", "CONSUMED"}
            or arm["approval_hash"] != record_hash
            or arm["user_restatement_hash"] != body["userExactRestatementHash"]
            or arm["approved_at"] != body["approvedAt"]
            or arm["expires_at"] != body["expiresAt"]
            or arm["body_hash"] != record_hash
            or arm["signature"] != signature
            or arm["authority_key_id_hash"] != self.bindings["authorityKeyIdHash"]
            or type(arm["revision"]) is not int
            or arm["revision"] < 1
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-arm-row-dirty")
        return arm

    def provision_arm(
        self,
        preapproval_envelope: Mapping[str, Any],
        *,
        observed_at: datetime,
        observed_monotonic_ns: int,
    ) -> dict[str, Any]:
        now = _utc(observed_at, "bootstrap-arm-observed-at")
        mono = _mono(observed_monotonic_ns, "bootstrap-arm-monotonic")
        if self.bindings["userExactRestatementHash"] is None:
            raise KisDomesticFunctionalBootstrapBlocked(
                "USER_EXACT_BOOTSTRAP_RESTATEMENT_MISSING"
            )
        preapproval, approval_hash, signature = self._verify_preapproval(
            preapproval_envelope, now=now
        )
        if self._assert_external_high_water_binding()["everIssued"]:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-ever-issued-burned")
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row, route = self._load_route(conn)
                if route is not None and route["phase"] not in {"EXPIRED"}:
                    raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-not-armable")
                revision = 1 if route is None else route["revision"] + 1
                previous = _ZERO if route is None else row["record_hash"]
                arm_id = preapproval["armId"]
                conn.execute(
                    "INSERT INTO kis_functional_bootstrap_arm VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        arm_id, ROUTE, "ARMED_WAIT", approval_hash,
                        preapproval["userExactRestatementHash"], preapproval["approvedAt"],
                        preapproval["expiresAt"], _canonical(preapproval).decode(),
                        approval_hash, signature, self.bindings["authorityKeyIdHash"], revision,
                    ),
                )
                body = self._route_body(
                    phase="ARMED_WAIT", revision=revision, ever_issued=False,
                    active_arm_id=arm_id, issuance_id=None, now=now,
                    monotonic_ns=mono, previous_hash=previous,
                    reason="PREAPPROVED_PUBLIC_ARM_WAITING_FOR_NATURAL_SIGNAL",
                )
                self._write_route(conn, body, expected_revision=None if row is None else route["revision"])
                conn.commit()
            except sqlite3.IntegrityError:
                conn.rollback()
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-arm-duplicate") from None
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return self.status()

    def expire_arm(
        self,
        *,
        arm_id: str,
        observed_at: datetime,
        observed_monotonic_ns: int,
    ) -> dict[str, Any]:
        arm = _identifier(arm_id, "bootstrap-expire-arm-id")
        now = _utc(observed_at, "bootstrap-expire-observed-at")
        mono = _mono(observed_monotonic_ns, "bootstrap-expire-monotonic")
        if self._assert_external_high_water_binding()["everIssued"]:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-main-reconciliation-required"
            )
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row, route = self._load_route(conn)
                arm_row = self._load_arm_verified(
                    conn, arm_id=arm, now=now, require_active=False
                )
                if (
                    route is None or route["phase"] != "ARMED_WAIT"
                    or route["activeArmId"] != arm or arm_row is None
                    or arm_row["state"] != "ARMED_WAIT"
                    or now < _utc(arm_row["expires_at"], "bootstrap-arm-expiry")
                ):
                    raise KisDomesticFunctionalBootstrapBlocked("bootstrap-arm-not-expirable")
                conn.execute(
                    "UPDATE kis_functional_bootstrap_arm SET state='EXPIRED' "
                    "WHERE arm_id=? AND state='ARMED_WAIT'", (arm,),
                )
                body = self._route_body(
                    phase="EXPIRED", revision=route["revision"] + 1,
                    ever_issued=False, active_arm_id=None, issuance_id=None,
                    now=now, monotonic_ns=mono, previous_hash=row["record_hash"],
                    reason="NO_NATURAL_SIGNAL_ARM_EXPIRED_POST_ZERO",
                )
                self._write_route(conn, body, expected_revision=route["revision"])
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return self.status()

    def _verify_bundle(
        self,
        envelope: Mapping[str, Any],
        *,
        approval: sqlite3.Row,
        now: datetime,
        monotonic_ns: int,
    ) -> tuple[dict[str, Any], str, str]:
        body, bundle_hash, signature = self._verify_envelope(
            envelope, domain=_ISSUANCE_DOMAIN, expected_keys=_BUNDLE_KEYS
        )
        expected = {
            "schemaVersion": ISSUANCE_BUNDLE_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "armId": approval["arm_id"],
            "approvalRecordHash": approval["approval_hash"],
            "ownerEpoch": self.bindings["ownerEpoch"],
            "ownerRecordHash": self.bindings["ownerRecordHash"],
            "registryAcceptedHeadHash": self.bindings["registryAcceptedHeadHash"],
            "accountFingerprint": self.bindings["accountFingerprint"],
            "credentialConfigurationHash": self.bindings["credentialConfigurationHash"],
            "codeManifestHash": self.bindings["codeManifestHash"],
            "artifactCanonicalHash": self.bindings["artifactCanonicalHash"],
            "instanceCanonicalHash": self.bindings["instanceCanonicalHash"],
            "userExactRestatementHash": self.bindings["userExactRestatementHash"],
            "externalComponentCasRequested": True,
            "productionMinted": False,
            "authorityKeyIdHash": self.bindings["authorityKeyIdHash"],
        }
        if any(type(body.get(key)) is not type(value) or body.get(key) != value for key, value in expected.items()):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-bundle-binding-mismatch")
        _identifier(body.get("issuanceId"), "bootstrap-issuance-id")
        signal = body.get("naturalSignal")
        rolling = body.get("rollingPreflight")
        quote = body.get("freshQuote")
        grant = body.get("laneGrant")
        if (
            not isinstance(signal, Mapping) or set(signal) != _SIGNAL_KEYS
            or not isinstance(rolling, Mapping) or set(rolling) != _ROLLING_KEYS
            or not isinstance(quote, Mapping) or set(quote) != _QUOTE_KEYS
            or not isinstance(grant, Mapping) or set(grant) != _GRANT_KEYS
            or signal.get("classification") != "NATURAL_BUY"
            or signal.get("present") is not True
            or signal.get("source") != "KIS_WEBSOCKET_H0STCNT0"
            or rolling.get("state") != "READY"
            or quote.get("state") != "READY"
            or quote.get("orderAuthorityFresh") is not True
            or grant.get("state") != "READY"
            or body.get("observedAt") != grant.get("grantWallAt")
            or body.get("observedMonotonicNs") != grant.get("grantMonotonicNs")
            or body.get("observedMonotonicNs") != monotonic_ns
            or _utc(body.get("observedAt"), "bootstrap-bundle-observed") != now
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-bundle-readiness-invalid")
        for part, fields in (
            (signal, ("evaluationId", "triggerId")),
            (rolling, ("snapshotId",)),
            (quote, ("receiptId",)),
            (grant, ("receiptId",)),
        ):
            for field in fields:
                _identifier(part.get(field), f"bootstrap-bundle-{field}")
        for part, fields in (
            (signal, ("evaluationHash", "triggerHash")),
            (rolling, ("snapshotHash", "receiptHash")),
            (quote, ("receiptHash",)),
            (grant, ("receiptHash",)),
        ):
            for field in fields:
                _sha(part.get(field), f"bootstrap-bundle-{field}")
        trigger_open = _utc(signal["triggerOpenAt"], "bootstrap-trigger-open")
        signal_observed = _utc(signal["observedAt"], "bootstrap-signal-observed")
        rolling_completed = _utc(rolling["completedAt"], "bootstrap-rolling-completed")
        rolling_expires = _utc(rolling["expiresAt"], "bootstrap-rolling-expires")
        quote_observed = _utc(quote["observedAt"], "bootstrap-quote-observed")
        quote_expires = _utc(quote["expiresAt"], "bootstrap-quote-expires")
        grant_at = _utc(grant["grantWallAt"], "bootstrap-grant-at")
        arm_expires = _utc(approval["expires_at"], "bootstrap-approval-expires")
        try:
            approval_body = json.loads(approval["body_json"])
            trading_date = date.fromisoformat(approval_body["tradingDate"])
            session_open, session_close = session_bounds_utc("XKRX", trading_date)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-bundle-trading-session-invalid"
            ) from exc
        activation_cutoff = datetime.combine(
            trading_date,
            wall_time(13, 15),
            tzinfo=_KST,
        ).astimezone(timezone.utc)
        if (
            trigger_open < session_open
            or trigger_open >= activation_cutoff
            or grant_at < session_open
            or grant_at >= activation_cutoff
            or grant_at >= session_close
            or trigger_open.astimezone(_KST).date() != trading_date
            or grant_at.astimezone(_KST).date() != trading_date
            or rolling_completed > trigger_open
            or signal_observed < trigger_open
            or signal_observed > grant_at
            or grant_at < trigger_open
            or grant_at > trigger_open + timedelta(seconds=2)
            or quote_observed < signal_observed
            or quote_observed > grant_at
            or grant_at - quote_observed > timedelta(seconds=5)
            or rolling_expires < grant_at
            or quote_expires < grant_at
            or grant_at >= arm_expires
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-bundle-time-invalid")
        return body, bundle_hash, signature

    def _prepare_issue_candidate(
        self,
        issuance_envelope: Mapping[str, Any],
        *,
        now: datetime,
        mono: int,
    ) -> dict[str, Any]:
        high_water = self._assert_external_high_water_binding()
        if high_water["everIssued"]:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-route-ever-issued-burned"
            )
        reconciliation = self.high_water.reconcile_main(
            self._external_main_projection(
                high_water,
                ever_issued=False,
                issuance_binding_hash=None,
            )
        )
        if (
            reconciliation.get("classification") != "UNISSUED_EXACT"
            or reconciliation.get("mayIssue") is not True
        ):
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-high-water-main-not-exact-before-issue"
            )
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            row, route = self._load_route(conn)
            if route is None or route["phase"] != "ARMED_WAIT":
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-route-not-ready")
            approval = self._load_arm_verified(
                conn,
                arm_id=route["activeArmId"],
                now=now,
                require_active=True,
            )
            if approval["state"] != "ARMED_WAIT":
                raise KisDomesticFunctionalBootstrapBlocked("bootstrap-arm-not-ready")
            body, bundle_hash, signature = self._verify_bundle(
                issuance_envelope, approval=approval, now=now, monotonic_ns=mono
            )
            if (
                now < _utc(route["updatedAt"], "bootstrap-issue-route-updated-at")
                or mono < _mono(
                    route["updatedMonotonicNs"],
                    "bootstrap-issue-route-updated-monotonic",
                )
            ):
                raise KisDomesticFunctionalBootstrapBlocked(
                    "bootstrap-issue-clock-rollback-before-high-water"
                )
            route_revision = route["revision"]
            route_hash = row["record_hash"]
            approval_hash = approval["approval_hash"]
            conn.rollback()
        finally:
            conn.close()
        issuance_binding = _hash(
            {
                "schemaVersion": "kis-domestic-functional-bootstrap-issuance-binding/v1",
                "route": ROUTE,
                "ownerEpoch": self.bindings["ownerEpoch"],
                "ownerRecordHash": self.bindings["ownerRecordHash"],
                "registryAcceptedHeadHash": self.bindings["registryAcceptedHeadHash"],
                "approvalHash": approval_hash,
                "bundleHash": bundle_hash,
                "routeRevision": route_revision,
                "routeRecordHash": route_hash,
            }
        )
        burn_body = self.high_water.next_burn_body(
            issuance_binding_hash=issuance_binding,
            occurred_at=now,
            occurred_monotonic_ns=mono,
        )
        return {
            "body": body,
            "bundleHash": bundle_hash,
            "bundleSignature": signature,
            "issuanceBindingHash": issuance_binding,
            "routeRevision": route_revision,
            "routeRecordHash": route_hash,
            "externalHighWaterBurnBody": burn_body,
        }

    def prepare_external_high_water_burn(
        self,
        issuance_envelope: Mapping[str, Any],
        *,
        observed_at: datetime,
        observed_monotonic_ns: int,
    ) -> dict[str, Any]:
        now = _utc(observed_at, "bootstrap-prepare-observed-at")
        mono = _mono(observed_monotonic_ns, "bootstrap-prepare-monotonic")
        with self._lock:
            candidate = self._prepare_issue_candidate(
                issuance_envelope,
                now=now,
                mono=mono,
            )
            result = {
                "schemaVersion": "kis-domestic-functional-bootstrap-external-burn-preparation/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "issuanceBindingHash": candidate["issuanceBindingHash"],
                "bundleHash": candidate["bundleHash"],
                "routeRevision": candidate["routeRevision"],
                "routeRecordHash": candidate["routeRecordHash"],
                "externalHighWaterBurnBody": candidate[
                    "externalHighWaterBurnBody"
                ],
                "productionWriterAvailable": False,
            }
            return {**result, "preparationHash": _hash(result)}

    def consume_and_issue(
        self,
        issuance_envelope: Mapping[str, Any],
        *,
        external_high_water_burn_envelope: Mapping[str, Any],
        observed_at: datetime,
        observed_monotonic_ns: int,
    ) -> dict[str, Any]:
        now = _utc(observed_at, "bootstrap-issue-observed-at")
        mono = _mono(observed_monotonic_ns, "bootstrap-issue-monotonic")
        with self._lock:
            candidate = self._prepare_issue_candidate(
                issuance_envelope,
                now=now,
                mono=mono,
            )
            body = candidate["body"]
            bundle_hash = candidate["bundleHash"]
            signature = candidate["bundleSignature"]
            issuance_binding = candidate["issuanceBindingHash"]
            route_revision = candidate["routeRevision"]
            route_hash = candidate["routeRecordHash"]
            if (
                not isinstance(external_high_water_burn_envelope, Mapping)
                or external_high_water_burn_envelope.get("body")
                != candidate["externalHighWaterBurnBody"]
            ):
                raise KisDomesticFunctionalBootstrapBlocked(
                    "bootstrap-external-high-water-burn-envelope-mismatch"
                )
            high_water_receipt = self.high_water.append_signed_burn(
                external_high_water_burn_envelope
            )
            if self.failure_injector is not None:
                self.failure_injector("AFTER_HIGH_WATER_RESERVED")
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                current_row, current = self._load_route(conn)
                arm = self._load_arm_verified(
                    conn,
                    arm_id=body["armId"],
                    now=now,
                    require_active=True,
                )
                if (
                    current is None
                    or current["phase"] != "ARMED_WAIT"
                    or current["revision"] != route_revision
                    or current_row["record_hash"] != route_hash
                    or arm is None
                    or arm["state"] != "ARMED_WAIT"
                    or high_water_receipt["issuanceBindingHash"] != issuance_binding
                ):
                    raise KisDomesticFunctionalBootstrapBlocked(
                        "bootstrap-post-high-water-cas-mismatch-burned"
                    )
                changed = conn.execute(
                    "UPDATE kis_functional_bootstrap_arm SET state='CONSUMED' "
                    "WHERE arm_id=? AND state='ARMED_WAIT'", (body["armId"],)
                ).rowcount
                if changed != 1:
                    raise KisDomesticFunctionalBootstrapBlocked(
                        "bootstrap-arm-consume-cas-failed-burned"
                    )
                conn.execute(
                    "INSERT INTO kis_functional_bootstrap_issue VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        ROUTE, body["issuanceId"], body["armId"], "ISSUED",
                        issuance_binding, bundle_hash, 1,
                        high_water_receipt["headHash"], _iso(now),
                        _canonical(body).decode(), bundle_hash, signature,
                        self.bindings["authorityKeyIdHash"], None,
                    ),
                )
                next_body = self._route_body(
                    phase="ISSUED", revision=current["revision"] + 1,
                    ever_issued=True, active_arm_id=None,
                    issuance_id=body["issuanceId"], now=now, monotonic_ns=mono,
                    previous_hash=current_row["record_hash"],
                    reason="ROUTE_EVER_ONE_USE_ISSUED_AND_BURNED",
                )
                self._write_route(conn, next_body, expected_revision=current["revision"])
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return self.status()

    def _load_issue_verified(
        self,
        conn: sqlite3.Connection,
        *,
        high_water_receipt: Mapping[str, Any],
    ) -> sqlite3.Row | None:
        issue = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_issue WHERE route=?", (ROUTE,)
        ).fetchone()
        if issue is None:
            return None
        try:
            bundle_body = json.loads(issue["body_json"])
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-issue-json-invalid"
            ) from None
        observed = _utc(bundle_body.get("observedAt"), "bootstrap-stored-issue-observed")
        observed_mono = _mono(
            bundle_body.get("observedMonotonicNs"),
            "bootstrap-stored-issue-monotonic",
        )
        approval = self._load_arm_verified(
            conn,
            arm_id=str(issue["arm_id"]),
            now=observed,
            require_active=True,
        )
        envelope = {
            "body": bundle_body,
            "recordHash": issue["body_hash"],
            "signature": issue["signature"],
            "authorityKeyIdHash": issue["authority_key_id_hash"],
        }
        verified_body, bundle_hash, signature = self._verify_bundle(
            envelope,
            approval=approval,
            now=observed,
            monotonic_ns=observed_mono,
        )
        issued_transition = conn.execute(
            "SELECT * FROM kis_functional_bootstrap_transition "
            "WHERE route=? AND phase='ISSUED'",
            (ROUTE,),
        ).fetchall()
        if len(issued_transition) != 1:
            raise KisDomesticFunctionalBootstrapBlocked(
                "bootstrap-issued-transition-not-exact"
            )
        transition = issued_transition[0]
        issuance_binding = _hash(
            {
                "schemaVersion": "kis-domestic-functional-bootstrap-issuance-binding/v1",
                "route": ROUTE,
                "ownerEpoch": self.bindings["ownerEpoch"],
                "ownerRecordHash": self.bindings["ownerRecordHash"],
                "registryAcceptedHeadHash": self.bindings["registryAcceptedHeadHash"],
                "approvalHash": approval["approval_hash"],
                "bundleHash": bundle_hash,
                "routeRevision": transition["revision"] - 1,
                "routeRecordHash": transition["previous_hash"],
            }
        )
        if (
            issue["issuance_id"] != verified_body["issuanceId"]
            or issue["arm_id"] != verified_body["armId"]
            or issue["state"] not in {"ISSUED", "BURNED"}
            or issue["issuance_binding_hash"] != issuance_binding
            or issue["bundle_hash"] != bundle_hash
            or issue["external_high_water_epoch"] != 1
            or issue["external_high_water_head_hash"]
            != high_water_receipt.get("headHash")
            or issue["issued_at"] != verified_body["observedAt"]
            or issue["body_hash"] != bundle_hash
            or issue["signature"] != signature
            or issue["authority_key_id_hash"] != self.bindings["authorityKeyIdHash"]
            or high_water_receipt.get("everIssued") is not True
            or high_water_receipt.get("epoch") != 1
            or high_water_receipt.get("issuanceBindingHash") != issuance_binding
            or (issue["state"] == "ISSUED" and issue["failure_reason"] is not None)
            or (
                issue["state"] == "BURNED"
                and issue["failure_reason"] not in {
                    "ISSUE_FAILURE", "ISSUE_EXPIRED", "SI_FAILURE",
                    "PROCESS_RESTART", "AMBIGUOUS_POST_ISSUE",
                }
            )
        ):
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-issue-row-dirty")
        return issue

    def mark_failure(
        self,
        *,
        reason: str,
        observed_at: datetime,
        observed_monotonic_ns: int,
    ) -> dict[str, Any]:
        allowed = {
            "ISSUE_FAILURE", "ISSUE_EXPIRED", "SI_FAILURE",
            "PROCESS_RESTART", "AMBIGUOUS_POST_ISSUE",
        }
        if reason not in allowed:
            raise KisDomesticFunctionalBootstrapBlocked("bootstrap-failure-reason-invalid")
        now = _utc(observed_at, "bootstrap-failure-observed-at")
        mono = _mono(observed_monotonic_ns, "bootstrap-failure-monotonic")
        with self._lock:
            high_water = self._assert_external_high_water_binding()
            if high_water["everIssued"] is not True:
                raise KisDomesticFunctionalBootstrapBlocked(
                    "bootstrap-issued-high-water-missing"
                )
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row, route = self._load_route(conn)
                if route is None or route["phase"] not in {"ISSUED", "BURNED"}:
                    raise KisDomesticFunctionalBootstrapBlocked("bootstrap-no-issued-route-to-burn")
                if route["phase"] == "BURNED":
                    conn.rollback()
                    return self.status()
                self._load_issue_verified(conn, high_water_receipt=high_water)
                conn.execute(
                    "UPDATE kis_functional_bootstrap_issue SET state='BURNED',failure_reason=? "
                    "WHERE route=? AND state='ISSUED'", (reason, ROUTE),
                )
                body = self._route_body(
                    phase="BURNED", revision=route["revision"] + 1,
                    ever_issued=True, active_arm_id=None,
                    issuance_id=route["issuanceId"], now=now, monotonic_ns=mono,
                    previous_hash=row["record_hash"], reason=reason,
                )
                self._write_route(conn, body, expected_revision=route["revision"])
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return self.status()

    def _reconcile_high_water(self) -> None:
        high_water = self._assert_external_high_water_binding()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row, route = self._load_route(conn)
                issue_row = conn.execute(
                    "SELECT issuance_binding_hash FROM "
                    "kis_functional_bootstrap_issue WHERE route=?",
                    (ROUTE,),
                ).fetchone()
                if route is None:
                    main_projection = None
                elif route["everIssued"] and issue_row is not None:
                    main_projection = self._external_main_projection(
                        high_water,
                        ever_issued=True,
                        issuance_binding_hash=issue_row["issuance_binding_hash"],
                    )
                elif not route["everIssued"] and not high_water["everIssued"]:
                    main_projection = self._external_main_projection(
                        high_water,
                        ever_issued=False,
                        issuance_binding_hash=None,
                    )
                else:
                    main_projection = None
                reconciliation = self.high_water.reconcile_main(main_projection)
                burned = high_water["everIssued"]
                expected_reconciliation = (
                    "UNISSUED_EXACT"
                    if not burned
                    else (
                        "BURNED_CONFIRMED"
                        if route is not None
                        and route["everIssued"]
                        and issue_row is not None
                        else "BURNED_RECONCILIATION_REQUIRED"
                    )
                )
                if (
                    reconciliation.get("classification")
                    != expected_reconciliation
                    or reconciliation.get("routeEverIssuedBurned") is not burned
                    or type(reconciliation.get("mayIssue")) is not bool
                    or reconciliation.get("mayIssue") != (not burned)
                ):
                    raise KisDomesticFunctionalBootstrapBlocked(
                        "bootstrap-external-high-water-reconciliation-invalid"
                    )
                if not burned:
                    if route is not None and route["everIssued"]:
                        raise KisDomesticFunctionalBootstrapBlocked(
                            "bootstrap-main-ahead-of-high-water"
                        )
                    conn.rollback()
                    return
                if route is not None and route["everIssued"]:
                    issue = self._load_issue_verified(
                        conn, high_water_receipt=high_water
                    )
                    if route["phase"] == "ISSUED" and issue is None:
                        raise KisDomesticFunctionalBootstrapBlocked(
                            "bootstrap-issued-route-row-missing"
                        )
                    if (
                        issue is not None
                        and issue["issuance_binding_hash"]
                        != high_water["issuanceBindingHash"]
                    ):
                        raise KisDomesticFunctionalBootstrapBlocked(
                            "bootstrap-high-water-main-binding-mismatch"
                        )
                    conn.rollback()
                    return
                now = _utc(
                    high_water["issuedAt"],
                    "bootstrap-high-water-reconcile-issued-at",
                )
                mono = _mono(
                    high_water["issuedMonotonicNs"],
                    "bootstrap-high-water-reconcile-monotonic",
                )
                revision = 1 if route is None else route["revision"] + 1
                previous = _ZERO if route is None else row["record_hash"]
                if route is not None and route.get("activeArmId") is not None:
                    conn.execute(
                        "UPDATE kis_functional_bootstrap_arm SET state='EXPIRED' "
                        "WHERE arm_id=? AND state='ARMED_WAIT'",
                        (route["activeArmId"],),
                    )
                body = self._route_body(
                    phase="BURNED", revision=revision, ever_issued=True,
                    active_arm_id=None, issuance_id=None, now=now,
                    monotonic_ns=mono, previous_hash=previous,
                    reason="HIGH_WATER_BURN_RECONCILED_AFTER_CRASH_OR_ROLLBACK",
                )
                self._write_route(
                    conn, body, expected_revision=None if route is None else route["revision"]
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    def status(self) -> dict[str, Any]:
        high_water = self._assert_external_high_water_binding()
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            row, route = self._load_route(conn)
            arm_rows = conn.execute(
                "SELECT arm_id,state,approved_at FROM kis_functional_bootstrap_arm "
                "ORDER BY revision,arm_id"
            ).fetchall()
            arms_by_id: dict[str, sqlite3.Row] = {}
            for arm_row in arm_rows:
                verified_arm = self._load_arm_verified(
                    conn,
                    arm_id=arm_row["arm_id"],
                    now=_utc(arm_row["approved_at"], "bootstrap-status-arm-approved"),
                    require_active=False,
                )
                arms_by_id[arm_row["arm_id"]] = verified_arm
            issue = self._load_issue_verified(
                conn, high_water_receipt=high_water
            )
            arm_count = conn.execute(
                "SELECT COUNT(*) FROM kis_functional_bootstrap_arm"
            ).fetchone()[0]
            issue_count = conn.execute(
                "SELECT COUNT(*) FROM kis_functional_bootstrap_issue"
            ).fetchone()[0]
            if (
                high_water["everIssued"]
                and (route is None or route["everIssued"] is not True)
            ):
                raise KisDomesticFunctionalBootstrapBlocked(
                    "bootstrap-high-water-main-reconciliation-required"
                )
            if (
                route is not None
                and (
                    (route["phase"] == "ARMED_WAIT" and (
                        route["activeArmId"] is None
                        or issue is not None
                        or route["activeArmId"] not in arms_by_id
                        or arms_by_id[route["activeArmId"]]["state"] != "ARMED_WAIT"
                        or sum(
                            1 for arm in arm_rows if arm["state"] == "ARMED_WAIT"
                        ) != 1
                    ))
                    or (route["phase"] == "EXPIRED" and (
                        route["activeArmId"] is not None
                        or any(arm["state"] == "ARMED_WAIT" for arm in arm_rows)
                    ))
                    or (route["phase"] == "ISSUED" and (
                        issue is None
                        or route["activeArmId"] is not None
                        or issue["arm_id"] not in arms_by_id
                        or arms_by_id[issue["arm_id"]]["state"] != "CONSUMED"
                    ))
                    or (route["phase"] == "BURNED" and not route["everIssued"])
                )
            ):
                raise KisDomesticFunctionalBootstrapBlocked(
                    "bootstrap-route-related-row-mismatch"
                )
            conn.commit()
        finally:
            conn.close()
        restatement_present = self.bindings["userExactRestatementHash"] is not None
        blockers = [
            "PRODUCTION_BOOTSTRAP_MINT_NOT_WIRED",
            "EXTERNAL_MINIMUM_ROLLBACK_PIN_STORE_NOT_WIRED",
            "PRODUCTION_EXTERNAL_HIGH_WATER_WRITER_NOT_PROVISIONED",
            "EXTERNAL_WRITER_BURN_COMMIT_PRECEDES_LOCAL_APPEND_NOT_WIRED",
            "INDEPENDENT_NATURAL_SIGNAL_ROLLING_QUOTE_LANE_CAS_NOT_WIRED",
            "SHARED_KIS_ROUTE_FENCE_NOT_WIRED",
        ]
        if not restatement_present:
            blockers.append("USER_EXACT_BOOTSTRAP_RESTATEMENT_MISSING")
        body = {
            "schemaVersion": STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "phase": "NEVER" if route is None else route["phase"],
            "revision": 0 if route is None else route["revision"],
            "routeEverIssuedBurned": bool(high_water["everIssued"]),
            "externalHighWaterEpoch": high_water["epoch"],
            "externalHighWaterBindingHash": self.high_water_binding_hash,
            "externalHighWaterHeadHash": high_water["headHash"],
            "armCount": arm_count,
            "issueCount": issue_count,
            "userExactRestatementHashPresent": restatement_present,
            "preSignalPrivateAccountAuthority": False,
            "preSignalOrderAuthority": False,
            "publicArmedWaitOnly": route is not None and route["phase"] == "ARMED_WAIT",
            "noSignalExpiryBurnsRoute": False,
            "independentOsProcessLeaseHeld": high_water["osProcessLeaseHeld"],
            "externalRollbackPinSuppliedAndVerified": high_water[
                "minimumRollbackPinSuppliedAndVerified"
            ],
            "externalRollbackPinStoreAvailable": False,
            "externalRollbackResistantHighWaterAvailable": False,
            "powerLossDurabilityIndependentlyProven": high_water[
                "powerLossDurabilityIndependentlyProven"
            ],
            "externalWriterBurnCommitPrecedesLocalAppend": high_water[
                "externalWriterBurnCommitPrecedesLocalAppend"
            ],
            "externalComponentAtomicCasAvailable": False,
            "readinessBlockers": sorted(blockers),
            "productionAvailable": False,
            "mintAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
            "sharedRouteFenceWired": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**body, "statusHash": _hash(body)}


def bootstrap_component_status() -> dict[str, Any]:
    body = {
        "schemaVersion": STATUS_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "publicArmedWaitImplemented": True,
        "routeEverIssuedHighWaterImplementedOffline": True,
        "userExactRestatementRequired": True,
        "productionAvailable": False,
        "mintAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "sharedRouteFenceWired": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
    }
    return {**body, "statusHash": _hash(body)}


__all__ = [
    "BootstrapBindings",
    "DurableKisDomesticFunctionalBootstrap",
    "ISSUANCE_BUNDLE_SCHEMA",
    "KisDomesticFunctionalBootstrapBlocked",
    "PREAPPROVAL_SCHEMA",
    "bootstrap_component_status",
]
