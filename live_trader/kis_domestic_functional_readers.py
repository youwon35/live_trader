from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import sqlite3
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .kis_domestic_functional_contract import PDNO, ROUTE
from .kis_domestic_functional_key_registry import (
    VerifyOnlyKeyRegistry,
)
from .kis_domestic_functional_market_source import (
    ACK_SCHEMA as MARKET_SOURCE_ACK_SCHEMA,
    HANDSHAKE_SCHEMA as MARKET_SOURCE_HANDSHAKE_SCHEMA,
    RAW_RECORD_SCHEMA as MARKET_SOURCE_RAW_RECORD_SCHEMA,
    REDUCER_SCHEMA as MARKET_SOURCE_REDUCER_SCHEMA,
    SCHEMA_FINGERPRINT as MARKET_SOURCE_SCHEMA_FINGERPRINT,
    SCHEMA_VERSION as MARKET_SOURCE_SCHEMA_VERSION,
    market_source_component_status,
)
from .kis_domestic_functional_source import (
    SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
    SOURCE_JOURNAL_SCHEMA_VERSION,
)

MARKET_ARCHIVE_SCHEMA_VERSION = (
    "kis-domestic-functional-market-archive-sqlite/v1"
)
MARKET_ARCHIVE_SCHEMA_FINGERPRINT = (
    "7c6ee7805b193274f99203ab70a47ad7ad9916bf842e046dfa01399f0d0055d3"
)
MARKET_ARCHIVE_AUTHORITY_PURPOSE = "MARKET_ARCHIVE_CAPTURE_VERIFY"
MARKET_ARCHIVE_CAPTURE_SCHEMA = (
    "kis-domestic-functional-market-archive-capture/v1"
)
MARKET_ARCHIVE_CAPTURE_SIGNATURE_DOMAIN = (
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_CAPTURE"
)


def _market_archive_module():
    from . import kis_domestic_functional_market_archive as module

    return module


KIS_DOMESTIC_FUNCTIONAL_READERS_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_READERS_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_READERS_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_READERS_RELEASE_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_READERS_STATE_SERVER_WIRED = False

READER_INPUT_SCHEMA = "kis-domestic-functional-frozen-reader-input/v1"
COMPONENT_ENVELOPE_SCHEMA = "kis-domestic-functional-reader-component/v1"
READ_PROVENANCE_SCHEMA = "kis-domestic-functional-reader-provenance/v1"
READER_OUTPUT_SCHEMA = "kis-domestic-functional-reader-bundle/v1"
SQLITE_ARCHIVE_SCHEMA = "kis-domestic-functional-reader-sqlite-archive/v1"
TRUTH_ARCHIVE_SCHEMA = "kis-domestic-functional-reader-truth-archive/v1"

_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:@/-]{1,200}$", re.ASCII)
_COMPONENTS = (
    "lane",
    "source",
    "rolling",
    "heartbeat",
    "mutation",
    "capability",
    "quote",
    "graph",
    "truth",
)
_COMPONENT_KEY_PURPOSES = {
    "lane": "LANE_RECORD_VERIFY",
    "source": "SOURCE_RECORD_VERIFY",
    "rolling": "ROLLING_RECORD_VERIFY",
    "heartbeat": "HEARTBEAT_RECORD_VERIFY",
    "mutation": "MUTATION_RECORD_VERIFY",
    "capability": "CAPABILITY_REVOKE_VERIFY",
    "quote": "QUOTE_RECORD_VERIFY",
    "graph": "GRAPH_RECORD_VERIFY",
    "truth": "TRUTH_RECORD_VERIFY",
}
_SPECIALIZED_KEY_PURPOSES = {
    "market_source": "MARKET_SOURCE_RECORD_VERIFY",
    "market_archive": "MARKET_ARCHIVE_CAPTURE_VERIFY",
    "owner": "OWNER_STATE_VERIFY",
}
_KEY_COMPONENTS = _COMPONENTS + ("market_source", "market_archive", "owner")

# These hashes pin the exact frozen producer/verifier bytes audited for this
# offline reader tranche. Any producer edit requires an explicit reader update.
PINNED_COMPONENT_FILE_HASHES = {
    "lane": "e5ff57817b8f25008454df3147d91ee93a69ff4227925fcfa03913fd1994643e",
    "source": "ab82de0030e680edbd5be9259cbb52d4915eb74af59e9c637ee3ea180c2eb9f3",
    "market_source": "85b0e2217c95e9a9364f69341af525a9410c2c2f1387dbbade7edf002a28c544",
    "market_archive": "d3eba77c5454331504d0a9ffba53250dac179771436847531aaeb6a928c0997e",
    "rolling": "949dfed9eb778ce69edef71fbeff6b02a2f76585a61de2d87e42c13e251ccb7b",
    "heartbeat": "f298834f21abdcc7c43c108ef444cbb92fe7fc32441ba058118b264520fa51ec",
    "mutation": "9b5cc2bb901633f02bdd0404d5b08fcaa3adcfe31527f57b503c67e85b52b3d5",
    "capability": "09bbfe9e4842fdedf6eb88cdcb6b2dd6a1af89a1ccd5ca89df6db63da673bb89",
    "quote": "4b518526cf36215b3b63b6df7ad5d682180e88a5b35c799fb2732b815fe0c1b9",
    "graph": "e532eab127d2f62e8751d0abc0b718cb99784221a52b82a4c2aafd0f4c6b3405",
    "truth": "488068d514f647a9ed9aca74c3deded32f3cdab793d5602487b3b9601b99cd9b",
}

_COMPONENT_FILES = {
    name: f"kis_domestic_functional_{'rolling_preflight' if name == 'rolling' else name}.py"
    for name in _COMPONENTS
}
_COMPONENT_FILES.update(
    {
        "market_source": "kis_domestic_functional_market_source.py",
        "market_archive": "kis_domestic_functional_market_archive.py",
    }
)

COMPONENT_SCHEMA_VERSIONS = {
    "lane": "kis-domestic-functional-lane-schema/v2",
    "source": SOURCE_JOURNAL_SCHEMA_VERSION,
    "market_source": MARKET_SOURCE_SCHEMA_VERSION,
    "market_archive": MARKET_ARCHIVE_SCHEMA_VERSION,
    "rolling": "kis-domestic-functional-rolling-preflight-schema/v1",
    "heartbeat": "kis-domestic-functional-heartbeat-schema/v1",
    "mutation": "kis-domestic-functional-mutation-schema/v1",
    "capability": "kis-domestic-functional-capability-schema/v1",
    "quote": "kis-domestic-functional-quote-schema/v1",
    "graph": "kis-domestic-functional-graph-schema/v1",
    "truth": "kis-domestic-functional-truth-capture-schema/v1",
}

_RECORD_TYPES = {
    "lane": {
        "LANE_SESSION": "LANE_SESSION",
        "BOOTSTRAP": "LANE_BOOTSTRAP",
        "APPROVAL": "LANE_APPROVAL",
        "EVALUATION": "LANE_EVALUATION",
        "TRIGGER": "LANE_TRIGGER",
        "ACTION": "LANE_ACTION",
    },
    "source": {"SOURCE_WINDOW_ARCHIVE": "SOURCE_WINDOW_ARCHIVE"},
    "rolling": {
        "ROLLING_DIAGNOSTIC": "ROLLING_DIAGNOSTIC",
        "ROLLING_BASELINE": "ROLLING_BASELINE",
        "ROLLING_CONSUMPTION": "ROLLING_CONSUMPTION",
    },
    "heartbeat": {"HEARTBEAT_EVIDENCE": "HEARTBEAT_EVIDENCE"},
    "mutation": {
        "MUTATION_INTEGRITY": "MUTATION_INTEGRITY",
        "MUTATION_ACTION": "MUTATION_ACTION",
    },
    "capability": {"CAPABILITY_REVOKE": "CAPABILITY_REVOKE"},
    "quote": {"QUOTE_RECEIPT": "QUOTE_RECEIPT"},
    "graph": {"GRAPH_ACTIVATION": "GRAPH_ACTIVATION"},
    "truth": {
        "PREACTIVATION_BASELINE": "TRUTH_PREACTIVATION_BASELINE",
        "TERMINAL_TRUTH": "TRUTH_TERMINAL",
    },
}

_REQUIRED_TYPES = {
    component: frozenset(record_types)
    for component, record_types in _RECORD_TYPES.items()
}

_PROVENANCE_KEYS = {
    "schemaVersion",
    "sourceKind",
    "databaseIdentityHash",
    "schemaFingerprint",
    "queryHash",
    "sqliteOpenMode",
    "transactionMode",
    "pragmaQueryOnly",
    "immutableUri",
    "rowCount",
    "writesAttempted",
    "networkAccessed",
    "readSetHash",
    "recordHighWaterRevision",
    "recordHeadHash",
    "recordCardinality",
    "primaryKeyCardinality",
}
_ROW_KEYS = {
    "recordType",
    "signatureDomain",
    "primaryKey",
    "revision",
    "body",
    "recordHash",
    "signature",
    "authorityKeyIdHash",
    "storedColumns",
    "projectionRules",
}
_ENVELOPE_KEYS = {
    "schemaVersion",
    "component",
    "route",
    "pdno",
    "sourceFileHash",
    "componentSchemaVersion",
    "componentSchemaFingerprint",
    "componentStatusHash",
    "componentStatus",
    "componentProtocolHash",
    "authorityKeyIdHash",
    "readProvenance",
    "records",
    "envelopeHash",
    "signature",
}

_COMPONENT_STATUS_KEYS = {
    "schemaVersion",
    "component",
    "route",
    "pdno",
    "productionAvailable",
    "networkAvailable",
    "mutationAvailable",
    "releaseAvailable",
    "networkOrderPostAllowed",
    "tradingMutationCount",
}

_EXPECTED_TYPE_CARDINALITY = {
    component: {
        record_type: (
            2
            if (component, record_type)
            in {("lane", "ACTION"), ("mutation", "MUTATION_ACTION")}
            else 1
        )
        for record_type in record_types
    }
    for component, record_types in _RECORD_TYPES.items()
}

SQLITE_ARCHIVE_SCHEMA_SQL = (
    """
    CREATE TABLE kis_reader_archive_meta(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        schema_version TEXT NOT NULL,
        component TEXT NOT NULL,
        source_schema_version TEXT NOT NULL,
        source_schema_fingerprint TEXT NOT NULL,
        source_file_hash TEXT NOT NULL,
        component_status_json TEXT NOT NULL,
        component_status_hash TEXT NOT NULL,
        query_hash TEXT NOT NULL,
        record_count INTEGER NOT NULL CHECK(record_count>=1),
        high_water_revision INTEGER NOT NULL CHECK(high_water_revision>=1),
        record_head_hash TEXT NOT NULL,
        cardinality_json TEXT NOT NULL,
        database_identity_hash TEXT NOT NULL,
        envelope_json TEXT NOT NULL,
        envelope_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE kis_reader_archive_record(
        ordinal INTEGER PRIMARY KEY CHECK(ordinal>=1),
        record_type TEXT NOT NULL,
        primary_key TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK(revision>=1),
        previous_hash TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        record_json TEXT NOT NULL
    )
    """,
    "CREATE UNIQUE INDEX ux_kis_reader_archive_identity ON kis_reader_archive_record(record_type,primary_key)",
)

_SQLITE_META_QUERY = (
    "SELECT singleton,schema_version,component,source_schema_version,"
    "source_schema_fingerprint,source_file_hash,component_status_json,"
    "component_status_hash,query_hash,record_count,high_water_revision,"
    "record_head_hash,cardinality_json,database_identity_hash,envelope_json,"
    "envelope_hash FROM kis_reader_archive_meta WHERE singleton=1"
)
_SQLITE_RECORD_QUERY = (
    "SELECT ordinal,record_type,primary_key,revision,previous_hash,record_hash,"
    "record_json FROM kis_reader_archive_record ORDER BY ordinal"
)

class KisDomesticFunctionalReadersBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalReadersBlocked("reader-json-invalid") from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


SQLITE_ARCHIVE_QUERY_HASH = _hash(
    {"metaQuery": _SQLITE_META_QUERY, "recordQuery": _SQLITE_RECORD_QUERY}
)
TRUTH_ARCHIVE_QUERY_HASH = _hash(
    {"operation": "READ_EXACT_SIGNED_TRUTH_ARCHIVE", "version": 1}
)


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalReadersBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise KisDomesticFunctionalReadersBlocked(f"{label}-invalid")
    return value


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KisDomesticFunctionalReadersBlocked(f"{label}-not-object")
    return value


def _exact_keys(value: Mapping[str, Any], keys: set[str], label: str) -> None:
    if set(value) != keys:
        raise KisDomesticFunctionalReadersBlocked(f"{label}-keys-not-exact")


def _json_pointer(value: Mapping[str, Any], pointer: str, label: str) -> Any:
    if type(pointer) is not str or not pointer.startswith("/"):
        raise KisDomesticFunctionalReadersBlocked(f"{label}-pointer-invalid")
    current: Any = value
    for raw in pointer[1:].split("/"):
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping) and token in current:
            current = current[token]
        elif isinstance(current, list) and token.isdigit() and int(token) < len(current):
            current = current[int(token)]
        else:
            raise KisDomesticFunctionalReadersBlocked(f"{label}-pointer-missing")
    return current


def _record_completeness(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    cardinality = Counter(str(item.get("recordType")) for item in records)
    primary_keys = [str(item.get("primaryKey")) for item in records]
    revisions = [int(item.get("revision")) for item in records]
    previous = "0" * 64
    for ordinal, item in enumerate(records, 1):
        previous = _hash(
            {
                "ordinal": ordinal,
                "recordType": item.get("recordType"),
                "primaryKey": item.get("primaryKey"),
                "revision": item.get("revision"),
                "recordHash": item.get("recordHash"),
                "previousHash": previous,
            }
        )
    return {
        "recordHighWaterRevision": max(revisions, default=0),
        "recordHeadHash": previous,
        "recordCardinality": {
            key: cardinality[key] for key in sorted(cardinality)
        },
        "primaryKeyCardinality": len(set(primary_keys)),
    }


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().split()).lower()


def _sqlite_schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT name,type,sql FROM sqlite_master "
        "WHERE name LIKE 'kis_reader_archive_%' "
        "OR name LIKE 'ux_kis_reader_archive_%' ORDER BY name"
    ).fetchall()
    objects = {
        str(row[0]): {"type": str(row[1]), "sql": _normalize_sql(row[2])}
        for row in rows
    }
    tables = {
        name: {
            "tableInfo": [
                tuple(item)
                for item in conn.execute(f"PRAGMA table_info({name})").fetchall()
            ],
            "foreignKeys": [
                tuple(item)
                for item in conn.execute(f"PRAGMA foreign_key_list({name})").fetchall()
            ],
            "indexList": [
                tuple(item)
                for item in conn.execute(f"PRAGMA index_list({name})").fetchall()
            ],
        }
        for name, value in objects.items()
        if value["type"] == "table"
    }
    indexes = {
        name: [
            tuple(item)
            for item in conn.execute(f"PRAGMA index_xinfo({name})").fetchall()
        ]
        for name, value in objects.items()
        if value["type"] == "index"
    }
    return {"objects": objects, "tables": tables, "indexes": indexes}


def _expected_sqlite_archive_schema() -> tuple[dict[str, Any], str]:
    conn = sqlite3.connect(":memory:")
    try:
        for statement in SQLITE_ARCHIVE_SCHEMA_SQL:
            conn.execute(statement)
        snapshot = _sqlite_schema_snapshot(conn)
        return snapshot, _hash(snapshot)
    finally:
        conn.close()


SQLITE_ARCHIVE_SCHEMA_SNAPSHOT, SQLITE_ARCHIVE_SCHEMA_FINGERPRINT = (
    _expected_sqlite_archive_schema()
)


def component_protocol_hash(component: str) -> str:
    if component not in _COMPONENTS:
        raise KisDomesticFunctionalReadersBlocked("reader-component-invalid")
    return _hash(
        {
            "schemaVersion": "kis-domestic-functional-reader-component-protocol/v1",
            "component": component,
            "componentSchemaVersion": COMPONENT_SCHEMA_VERSIONS[component],
            "recordTypes": _RECORD_TYPES[component],
            "rowKeys": sorted(_ROW_KEYS),
            "provenanceKeys": sorted(_PROVENANCE_KEYS),
            "verifyOnly": True,
            "networkAllowed": False,
            "writesAllowed": False,
        }
    )


COMPONENT_PROTOCOL_HASHES = {
    component: component_protocol_hash(component) for component in _COMPONENTS
}

MARKET_ARCHIVE_READER_PROTOCOL_HASH = _hash(
    {
        "schemaVersion": "kis-domestic-functional-market-archive-reader-protocol/v1",
        "marketSchemaVersion": MARKET_SOURCE_SCHEMA_VERSION,
        "marketSchemaFingerprint": MARKET_SOURCE_SCHEMA_FINGERPRINT,
        "sourceSchemaVersion": SOURCE_JOURNAL_SCHEMA_VERSION,
        "sourceSchemaFingerprint": SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
        "archiveSchemaVersion": MARKET_ARCHIVE_SCHEMA_VERSION,
        "archiveSchemaFingerprint": MARKET_ARCHIVE_SCHEMA_FINGERPRINT,
        "fullProducerReplayRequired": True,
        "postObservationPrefixExtensionRequired": True,
        "externalAsymmetricArchiveAuthorityRequired": True,
        "verifyOnly": True,
    }
)
READERS_COMPONENT_SCHEMA_FINGERPRINT = _hash(
    {
        "readerInputSchema": READER_INPUT_SCHEMA,
        "readerOutputSchema": READER_OUTPUT_SCHEMA,
        "componentEnvelopeSchema": COMPONENT_ENVELOPE_SCHEMA,
        "componentSchemas": COMPONENT_SCHEMA_VERSIONS,
        "marketArchiveProtocolHash": MARKET_ARCHIVE_READER_PROTOCOL_HASH,
        "genericComponents": list(_COMPONENTS),
    }
)
READERS_COMPONENT_PROTOCOL_HASH = _hash(
    {
        "schemaVersion": "kis-domestic-functional-readers-component-protocol/v2",
        "schemaFingerprint": READERS_COMPONENT_SCHEMA_FINGERPRINT,
        "componentProtocolHashes": COMPONENT_PROTOCOL_HASHES,
        "marketArchiveProtocolHash": MARKET_ARCHIVE_READER_PROTOCOL_HASH,
        "registrySignedComponentBindingRequired": True,
        "writesAllowed": False,
        "networkAllowed": False,
    }
)


def _actual_component_file_hash(component: str) -> str:
    path = Path(__file__).resolve().parent / _COMPONENT_FILES[component]
    if not path.is_file():
        raise KisDomesticFunctionalReadersBlocked(
            f"reader-component-file-missing:{component}"
        )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _archive_logical_identity(
    *,
    component: str,
    schema_fingerprint: str,
    status_hash: str,
    query_hash: str,
    records: Sequence[Mapping[str, Any]],
) -> str:
    completeness = _record_completeness(records)
    return _hash(
        {
            "schemaVersion": "kis-domestic-functional-reader-database-identity/v1",
            "component": component,
            "schemaFingerprint": schema_fingerprint,
            "componentStatusHash": status_hash,
            "queryHash": query_hash,
            "recordCount": len(records),
            **completeness,
            "rows": [
                {
                    "ordinal": ordinal,
                    "recordType": item.get("recordType"),
                    "primaryKey": item.get("primaryKey"),
                    "revision": item.get("revision"),
                    "recordHash": item.get("recordHash"),
                }
                for ordinal, item in enumerate(records, 1)
            ],
        }
    )


class ImmutableSqliteComponentArchiveReader:
    """Read one materialized frozen component archive with SQLite immutable=1.

    This class never creates a schema and has no writer method. The archive
    producer is external; this reader independently verifies exact SQLite
    objects, the code-owned query, status/body hash, row projections,
    cardinality/high-water/head, and file stability before returning the
    signed component envelope to the verify-only consumer.
    """

    def __init__(self, path: str | Path, *, component: str) -> None:
        self.path = Path(path).expanduser().resolve()
        if component not in _COMPONENTS or component == "truth":
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-component-invalid"
            )
        if not self.path.is_file():
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-file-missing"
            )
        self.component = component

    def _connect(self) -> sqlite3.Connection:
        uri = self.path.as_uri() + "?mode=ro&immutable=1"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn

    def read(self) -> tuple[dict[str, Any], dict[str, Any]]:
        for suffix in ("-wal", "-shm", "-journal"):
            if Path(str(self.path) + suffix).exists():
                raise KisDomesticFunctionalReadersBlocked(
                    "sqlite-archive-sidecar-present"
                )
        before_hash = hashlib.sha256(self.path.read_bytes()).hexdigest()
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            schema_snapshot = _sqlite_schema_snapshot(conn)
            if schema_snapshot != SQLITE_ARCHIVE_SCHEMA_SNAPSHOT:
                raise KisDomesticFunctionalReadersBlocked(
                    "sqlite-archive-schema-dirty"
                )
            meta = conn.execute(_SQLITE_META_QUERY).fetchone()
            rows = conn.execute(_SQLITE_RECORD_QUERY).fetchall()
            conn.commit()
        finally:
            conn.close()
        after_hash = hashlib.sha256(self.path.read_bytes()).hexdigest()
        if not hmac.compare_digest(before_hash, after_hash):
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-file-changed-during-read"
            )
        if meta is None or int(meta["singleton"]) != 1:
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-meta-missing"
            )
        try:
            status = json.loads(meta["component_status_json"])
            cardinality = json.loads(meta["cardinality_json"])
            envelope = json.loads(meta["envelope_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-json-invalid"
            ) from exc
        if (
            meta["schema_version"] != SQLITE_ARCHIVE_SCHEMA
            or meta["component"] != self.component
            or meta["source_schema_version"]
            != COMPONENT_SCHEMA_VERSIONS[self.component]
            or meta["source_schema_fingerprint"]
            != SQLITE_ARCHIVE_SCHEMA_FINGERPRINT
            or meta["source_file_hash"]
            != PINNED_COMPONENT_FILE_HASHES[self.component]
            or meta["query_hash"] != SQLITE_ARCHIVE_QUERY_HASH
            or not isinstance(status, Mapping)
            or _hash(status) != meta["component_status_hash"]
            or not isinstance(cardinality, Mapping)
            or not isinstance(envelope, Mapping)
            or envelope.get("envelopeHash") != meta["envelope_hash"]
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-meta-contract-invalid"
            )
        parsed_records: list[dict[str, Any]] = []
        expected_previous = "0" * 64
        for expected_ordinal, row in enumerate(rows, 1):
            try:
                record = json.loads(row["record_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise KisDomesticFunctionalReadersBlocked(
                    "sqlite-archive-record-json-invalid"
                ) from exc
            if (
                int(row["ordinal"]) != expected_ordinal
                or not isinstance(record, Mapping)
                or row["record_type"] != record.get("recordType")
                or row["primary_key"] != record.get("primaryKey")
                or int(row["revision"]) != record.get("revision")
                or row["record_hash"] != record.get("recordHash")
                or row["previous_hash"] != expected_previous
            ):
                raise KisDomesticFunctionalReadersBlocked(
                    "sqlite-archive-row-projection-or-chain-invalid"
                )
            parsed_records.append(deepcopy(dict(record)))
            expected_previous = _record_completeness(parsed_records)[
                "recordHeadHash"
            ]
        completeness = _record_completeness(parsed_records)
        database_identity = _archive_logical_identity(
            component=self.component,
            schema_fingerprint=SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            status_hash=str(meta["component_status_hash"]),
            query_hash=SQLITE_ARCHIVE_QUERY_HASH,
            records=parsed_records,
        )
        if (
            int(meta["record_count"]) != len(parsed_records)
            or int(meta["high_water_revision"])
            != completeness["recordHighWaterRevision"]
            or meta["record_head_hash"] != completeness["recordHeadHash"]
            or dict(cardinality) != completeness["recordCardinality"]
            or meta["database_identity_hash"] != database_identity
            or list(envelope.get("records", [])) != parsed_records
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-completeness-invalid"
            )
        provenance = envelope.get("readProvenance")
        if not isinstance(provenance, Mapping) or (
            provenance.get("schemaFingerprint")
            != SQLITE_ARCHIVE_SCHEMA_FINGERPRINT
            or provenance.get("queryHash") != SQLITE_ARCHIVE_QUERY_HASH
            or provenance.get("databaseIdentityHash") != database_identity
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "sqlite-archive-envelope-provenance-mismatch"
            )
        evidence = {
            "schemaVersion": "kis-domestic-functional-reader-archive-evidence/v1",
            "component": self.component,
            "archiveKind": "SQLITE_IMMUTABLE",
            "archiveFileHash": before_hash,
            "schemaFingerprint": SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            "queryHash": SQLITE_ARCHIVE_QUERY_HASH,
            "databaseIdentityHash": database_identity,
            "recordCount": len(parsed_records),
            **completeness,
            "sqliteOpenMode": "mode=ro&immutable=1",
            "pragmaQueryOnly": True,
            "networkAccessed": False,
            "writesAttempted": 0,
        }
        return deepcopy(dict(envelope)), {
            **evidence,
            "evidenceHash": _hash(evidence),
        }


_TRUTH_ARCHIVE_KEYS = {
    "schemaVersion",
    "component",
    "sourceSchemaVersion",
    "sourceSchemaFingerprint",
    "sourceFileHash",
    "componentStatus",
    "componentStatusHash",
    "queryHash",
    "recordCount",
    "highWaterRevision",
    "recordHeadHash",
    "cardinality",
    "databaseIdentityHash",
    "envelope",
}
TRUTH_ARCHIVE_SCHEMA_FINGERPRINT = _hash(
    {
        "schemaVersion": TRUTH_ARCHIVE_SCHEMA,
        "keys": sorted(_TRUTH_ARCHIVE_KEYS),
        "component": "truth",
    }
)


class ImmutableTruthArchiveReader:
    """Independently fetch an already-signed truth archive from local bytes."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        if not self.path.is_file():
            raise KisDomesticFunctionalReadersBlocked(
                "truth-archive-file-missing"
            )

    def read(self) -> tuple[dict[str, Any], dict[str, Any]]:
        before = self.path.read_bytes()
        before_hash = hashlib.sha256(before).hexdigest()
        try:
            archive = json.loads(before.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalReadersBlocked(
                "truth-archive-json-invalid"
            ) from exc
        after_hash = hashlib.sha256(self.path.read_bytes()).hexdigest()
        if not hmac.compare_digest(before_hash, after_hash):
            raise KisDomesticFunctionalReadersBlocked(
                "truth-archive-file-changed-during-read"
            )
        if not isinstance(archive, Mapping):
            raise KisDomesticFunctionalReadersBlocked(
                "truth-archive-not-object"
            )
        _exact_keys(archive, _TRUTH_ARCHIVE_KEYS, "truth-archive")
        status = _mapping(archive.get("componentStatus"), "truth-archive-status")
        envelope = _mapping(archive.get("envelope"), "truth-archive-envelope")
        records = envelope.get("records")
        if not isinstance(records, list):
            raise KisDomesticFunctionalReadersBlocked(
                "truth-archive-records-invalid"
            )
        completeness = _record_completeness(records)
        database_identity = _archive_logical_identity(
            component="truth",
            schema_fingerprint=TRUTH_ARCHIVE_SCHEMA_FINGERPRINT,
            status_hash=_hash(status),
            query_hash=TRUTH_ARCHIVE_QUERY_HASH,
            records=records,
        )
        if (
            archive.get("schemaVersion") != TRUTH_ARCHIVE_SCHEMA
            or archive.get("component") != "truth"
            or archive.get("sourceSchemaVersion")
            != COMPONENT_SCHEMA_VERSIONS["truth"]
            or archive.get("sourceSchemaFingerprint")
            != TRUTH_ARCHIVE_SCHEMA_FINGERPRINT
            or archive.get("sourceFileHash")
            != PINNED_COMPONENT_FILE_HASHES["truth"]
            or archive.get("componentStatusHash") != _hash(status)
            or archive.get("queryHash") != TRUTH_ARCHIVE_QUERY_HASH
            or archive.get("recordCount") != len(records)
            or archive.get("highWaterRevision")
            != completeness["recordHighWaterRevision"]
            or archive.get("recordHeadHash") != completeness["recordHeadHash"]
            or archive.get("cardinality") != completeness["recordCardinality"]
            or archive.get("databaseIdentityHash") != database_identity
            or envelope.get("componentStatus") != status
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "truth-archive-contract-or-completeness-invalid"
            )
        provenance = envelope.get("readProvenance")
        if not isinstance(provenance, Mapping) or (
            provenance.get("schemaFingerprint")
            != TRUTH_ARCHIVE_SCHEMA_FINGERPRINT
            or provenance.get("queryHash") != TRUTH_ARCHIVE_QUERY_HASH
            or provenance.get("databaseIdentityHash") != database_identity
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "truth-archive-envelope-provenance-mismatch"
            )
        evidence = {
            "schemaVersion": "kis-domestic-functional-reader-archive-evidence/v1",
            "component": "truth",
            "archiveKind": "SIGNED_TRUTH_JSON",
            "archiveFileHash": before_hash,
            "schemaFingerprint": TRUTH_ARCHIVE_SCHEMA_FINGERPRINT,
            "queryHash": TRUTH_ARCHIVE_QUERY_HASH,
            "databaseIdentityHash": database_identity,
            "recordCount": len(records),
            **completeness,
            "sqliteOpenMode": "NOT_APPLICABLE",
            "pragmaQueryOnly": True,
            "networkAccessed": False,
            "writesAttempted": 0,
        }
        return deepcopy(dict(envelope)), {
            **evidence,
            "evidenceHash": _hash(evidence),
        }


class ImmutableMarketSourceArchiveReader:
    """Verify the exact two-ledger market/source archive without a signer.

    Every producer and capture signature is checked through the concrete
    accepted key registry.  The pre-observation archive is deliberately not
    promoted to release truth: a later post-observation prefix extension and
    external asymmetric archive authority remain required.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        expected_file_hash: str,
        source_generation: str,
        arm_id: str,
        key_registry: VerifyOnlyKeyRegistry,
        expected_registry_manifest_hash: str,
        expected_registry_epoch: int,
    ) -> None:
        if type(key_registry) is not VerifyOnlyKeyRegistry:
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-key-registry-type-invalid"
            )
        self.path = Path(path).expanduser().resolve()
        self.expected_file_hash = _sha(
            expected_file_hash, "market-archive-expected-file-hash"
        )
        self.source_generation = _identifier(
            source_generation, "market-archive-source-generation"
        )
        self.arm_id = _identifier(arm_id, "market-archive-arm-id")
        self.registry = key_registry
        self.expected_manifest_hash = _sha(
            expected_registry_manifest_hash,
            "market-archive-registry-manifest-hash",
        )
        if type(expected_registry_epoch) is not int or expected_registry_epoch < 1:
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-registry-epoch-invalid"
            )
        self.expected_registry_epoch = expected_registry_epoch

    def _assert_registry_current(self) -> None:
        status = self.registry.status()
        if (
            status.get("manifestFresh") is not True
            or status.get("verifyOnly") is not True
            or status.get("privateKeyMaterialPresent") is not False
            or status.get("signingSurfacePresent") is not False
            or status.get("manifestHash") != self.expected_manifest_hash
            or status.get("registryEpoch") != self.expected_registry_epoch
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-registry-not-current"
            )

    def _candidate_verifier(self, *, purpose: str, domain: str):
        expected_key_id = self.registry.active_key_id_for(purpose)

        def verify(candidate: Mapping[str, Any]) -> bool:
            try:
                value = deepcopy(dict(candidate))
                signature = value.pop("signature")
                key_id = value.get("authorityKeyIdHash", expected_key_id)
                return bool(
                    key_id == expected_key_id
                    and self.registry.verify(
                        purpose=purpose,
                        domain=domain,
                        body=value,
                        signature=signature,
                        key_id_hash=expected_key_id,
                    ) is True
                )
            except BaseException:
                return False

        return verify

    def _domain_verifier(self, *, purpose: str):
        expected_key_id = self.registry.active_key_id_for(purpose)

        def verify(domain, body, signature, key_id_hash=None) -> bool:
            try:
                candidate_key = key_id_hash or body.get(
                    "authorityKeyIdHash", expected_key_id
                )
                return bool(
                    candidate_key == expected_key_id
                    and self.registry.verify(
                        purpose=purpose,
                        domain=domain,
                        body=deepcopy(dict(body)),
                        signature=signature,
                        key_id_hash=expected_key_id,
                    ) is True
                )
            except BaseException:
                return False

        return verify

    def read(self) -> dict[str, Any]:
        self._assert_registry_current()
        for component in ("market_source", "source", "market_archive"):
            if not hmac.compare_digest(
                _actual_component_file_hash(component),
                PINNED_COMPONENT_FILE_HASHES[component],
            ):
                raise KisDomesticFunctionalReadersBlocked(
                    f"market-archive-component-file-drift:{component}"
                )
        market_key = self.registry.active_key_id_for(
            "MARKET_SOURCE_RECORD_VERIFY"
        )
        source_key = self.registry.active_key_id_for("SOURCE_RECORD_VERIFY")
        capture_key = self.registry.active_key_id_for(
            MARKET_ARCHIVE_AUTHORITY_PURPOSE
        )
        before = self.path.read_bytes()
        before_hash = hashlib.sha256(before).hexdigest()
        if not hmac.compare_digest(before_hash, self.expected_file_hash):
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-file-drift"
            )
        try:
            verified = _market_archive_module().verify_market_source_archive(
                self.path,
                expected_file_hash=self.expected_file_hash,
                source_generation=self.source_generation,
                arm_id=self.arm_id,
                fence_verifier=self._candidate_verifier(
                    purpose="OWNER_STATE_VERIFY",
                    domain="KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_FENCE",
                ),
                market_verifiers={
                    "handshake": self._candidate_verifier(
                        purpose="MARKET_SOURCE_RECORD_VERIFY",
                        domain="KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_HANDSHAKE",
                    ),
                    "raw": self._candidate_verifier(
                        purpose="MARKET_SOURCE_RECORD_VERIFY",
                        domain="KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_RAW_FRAME",
                    ),
                    "ack": self._candidate_verifier(
                        purpose="SOURCE_RECORD_VERIFY",
                        domain="MARKET_SOURCE_DURABLE_ACK",
                    ),
                    "reducer": self._candidate_verifier(
                        purpose="SOURCE_RECORD_VERIFY",
                        domain=(
                            "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_REDUCER_RECEIPT"
                        ),
                    ),
                },
                transition_verifier=self._domain_verifier(
                    purpose="MARKET_SOURCE_RECORD_VERIFY"
                ),
                source_verifier=self._domain_verifier(
                    purpose="SOURCE_RECORD_VERIFY"
                ),
                archive_capture_verifier=self._domain_verifier(
                    purpose=MARKET_ARCHIVE_AUTHORITY_PURPOSE
                ),
                expected_archive_authority_key_id_hash=capture_key,
            )
        except BaseException as exc:
            raise KisDomesticFunctionalReadersBlocked(
                f"market-archive-independent-verification-failed:{type(exc).__name__}"
            ) from None
        after = self.path.read_bytes()
        if before != after or not hmac.compare_digest(
            hashlib.sha256(after).hexdigest(), before_hash
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-changed-during-read"
            )
        conn = sqlite3.connect(
            f"file:{self.path.as_posix()}?mode=ro&immutable=1", uri=True
        )
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT capture_json,capture_hash,archive_authority_key_id_hash "
                "FROM kis_market_archive_capture"
            ).fetchall()
        finally:
            conn.close()
        if len(rows) != 1:
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-capture-cardinality-invalid"
            )
        try:
            capture = json.loads(str(rows[0]["capture_json"]))
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-capture-json-invalid"
            ) from None
        summary = _mapping(verified.get("replaySummary"), "market-archive-summary")
        status = _market_archive_module().market_archive_component_status()
        if (
            capture.get("schemaVersion") != MARKET_ARCHIVE_CAPTURE_SCHEMA
            or capture.get("sourceGeneration") != self.source_generation
            or capture.get("armId") != self.arm_id
            or rows[0]["capture_hash"] != verified.get("captureHash")
            or rows[0]["archive_authority_key_id_hash"] != capture_key
            or capture.get("archiveAuthorityKeyIdHash") != capture_key
            or capture.get("archiveAuthorityPurpose")
            != MARKET_ARCHIVE_AUTHORITY_PURPOSE
            or capture.get("replaySummary") != summary
            or verified.get("archiveFileHash") != self.expected_file_hash
            or verified.get("allProducerRecordsIndependentlyReplayed") is not True
            or verified.get("freshDedicatedProducerDatabasesVerified") is not True
            or verified.get("atomicRouteOwnerObservationFenceHeld") is not True
            or verified.get("externalAsymmetricArchiveAuthorityPinned") is not False
            or verified.get("releaseCompletenessProven") is not False
            or verified.get("productionAvailable") is not False
            or verified.get("releaseAvailable") is not False
            or status.get("externalAsymmetricArchiveAuthorityPinned") is not False
            or status.get("releaseCompletenessProven") is not False
            or summary.get("sourceObservationCount") != 0
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "market-archive-result-or-capture-not-exact"
            )
        body = {
            "schemaVersion": "kis-domestic-functional-market-archive-reader/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sourceGeneration": self.source_generation,
            "armId": self.arm_id,
            "archiveFileHash": self.expected_file_hash,
            "captureHash": verified["captureHash"],
            "captureFenceHash": capture["fence"]["fenceHash"],
            "captureFenceObservedAt": capture["fenceObservedAt"],
            "marketDatabaseBundleHash": capture[
                "marketDatabaseBundleHashAfter"
            ],
            "sourceDatabaseBundleHash": capture[
                "sourceDatabaseBundleHashAfter"
            ],
            "logicalSnapshotHash": capture["logicalSnapshotHashAfter"],
            "replaySummary": deepcopy(dict(summary)),
            "marketSourceFileHash": PINNED_COMPONENT_FILE_HASHES["market_source"],
            "sourceFileHash": PINNED_COMPONENT_FILE_HASHES["source"],
            "marketArchiveFileHash": PINNED_COMPONENT_FILE_HASHES["market_archive"],
            "marketSourceSchemaVersion": MARKET_SOURCE_SCHEMA_VERSION,
            "marketSourceSchemaFingerprint": MARKET_SOURCE_SCHEMA_FINGERPRINT,
            "sourceSchemaVersion": SOURCE_JOURNAL_SCHEMA_VERSION,
            "sourceSchemaFingerprint": SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
            "archiveSchemaVersion": MARKET_ARCHIVE_SCHEMA_VERSION,
            "archiveSchemaFingerprint": MARKET_ARCHIVE_SCHEMA_FINGERPRINT,
            "archiveReaderProtocolHash": MARKET_ARCHIVE_READER_PROTOCOL_HASH,
            "marketSourceAuthorityKeyIdHash": market_key,
            "sourceAuthorityKeyIdHash": source_key,
            "archiveAuthorityKeyIdHash": capture_key,
            "allProducerRecordsIndependentlyReplayed": True,
            "atomicRouteOwnerObservationFenceHeld": True,
            "freshDedicatedProducerDatabasesVerified": True,
            "postObservationPrefixExtensionProven": False,
            "externalAsymmetricArchiveAuthorityPinned": False,
            "releaseCompletenessProven": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
        }
        return {**body, "marketSourceArchiveEvidenceHash": _hash(body)}


class KisDomesticFunctionalVerifyOnlyReaders:
    """Verify frozen component read envelopes without holding signing authority.

    Verification is available only through the exact external verify-only key
    registry type. The caller separately pins the manifest/root/epoch/current
    key IDs, so a substituted but internally valid manifest cannot authorize a
    frozen component bundle. This object has no DB writer, network, credential,
    token, sender, signer, or mutation surface.
    """

    def __init__(
        self,
        *,
        key_registry: VerifyOnlyKeyRegistry,
        expected_registry_manifest_hash: str,
        expected_registry_root_key_id_hash: str,
        expected_registry_epoch: int,
        expected_component_key_id_hashes: Mapping[str, str],
    ) -> None:
        if type(key_registry) is not VerifyOnlyKeyRegistry:
            raise KisDomesticFunctionalReadersBlocked(
                "reader-key-registry-type-invalid"
            )
        manifest_hash = _sha(
            expected_registry_manifest_hash,
            "reader-expected-registry-manifest-hash",
        )
        root_key_id_hash = _sha(
            expected_registry_root_key_id_hash,
            "reader-expected-registry-root-key-id-hash",
        )
        if type(expected_registry_epoch) is not int or expected_registry_epoch < 1:
            raise KisDomesticFunctionalReadersBlocked(
                "reader-expected-registry-epoch-invalid"
            )
        if set(expected_component_key_id_hashes) != set(_KEY_COMPONENTS):
            raise KisDomesticFunctionalReadersBlocked(
                "reader-expected-component-key-ids-not-exact"
            )
        expected_key_ids = {
            component: _sha(
                expected_component_key_id_hashes[component],
                f"reader-expected-component-key-id:{component}",
            )
            for component in _KEY_COMPONENTS
        }
        registry_status = key_registry.status()
        if (
            registry_status.get("manifestFresh") is not True
            or registry_status.get("allPurposesCovered") is not True
            or registry_status.get("verifyOnly") is not True
            or registry_status.get("privateKeyMaterialPresent") is not False
            or registry_status.get("signingSurfacePresent") is not False
            or not hmac.compare_digest(
                registry_status.get("manifestHash", ""), manifest_hash
            )
            or not hmac.compare_digest(
                registry_status.get("rootKeyIdHash", ""), root_key_id_hash
            )
            or registry_status.get("registryEpoch") != expected_registry_epoch
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "reader-key-registry-binding-invalid"
            )
        now_value = key_registry.clock()
        if not isinstance(now_value, datetime) or now_value.tzinfo is None:
            raise KisDomesticFunctionalReadersBlocked(
                "reader-key-registry-clock-invalid"
            )
        now = now_value.astimezone(timezone.utc)
        revoked = {
            item["keyIdHash"] for item in key_registry.manifest["revocations"]
        }
        active_ids: dict[str, str] = {}
        for component in _KEY_COMPONENTS:
            purpose = (
                _COMPONENT_KEY_PURPOSES.get(component)
                or _SPECIALIZED_KEY_PURPOSES[component]
            )
            candidates = []
            for item in key_registry.manifest["keys"]:
                if item["purpose"] != purpose or item["keyIdHash"] in revoked:
                    continue
                try:
                    not_before = datetime.fromisoformat(
                        item["notBefore"].replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                    not_after = datetime.fromisoformat(
                        item["notAfter"].replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                except (AttributeError, ValueError) as exc:
                    raise KisDomesticFunctionalReadersBlocked(
                        f"reader-key-registry-time-invalid:{component}"
                    ) from exc
                if not_before <= now < not_after:
                    candidates.append(item["keyIdHash"])
            if len(candidates) != 1:
                raise KisDomesticFunctionalReadersBlocked(
                    f"reader-key-registry-active-key-not-exact:{component}"
                )
            active_ids[component] = _sha(
                candidates[0], f"reader-registry-active-key-id:{component}"
            )
            if not hmac.compare_digest(
                active_ids[component], expected_key_ids[component]
            ):
                raise KisDomesticFunctionalReadersBlocked(
                    f"reader-key-registry-key-pin-mismatch:{component}"
                )
        self._registry = key_registry
        self._registry_manifest_hash = manifest_hash
        self._registry_root_key_id_hash = root_key_id_hash
        self._registry_epoch = expected_registry_epoch
        self._registry_account_fingerprint = _sha(
            key_registry.account_fingerprint,
            "reader-key-registry-account-fingerprint",
        )
        self._registry_credential_configuration_hash = _sha(
            key_registry.credential_configuration_hash,
            "reader-key-registry-credential-configuration-hash",
        )
        self._registry_code_manifest_hash = _sha(
            key_registry.code_manifest_hash,
            "reader-key-registry-code-manifest-hash",
        )
        self._registry_file_hash = _sha(
            registry_status.get("manifestFileHash"),
            "reader-key-registry-file-hash",
        )
        self._registry_asymmetric_root_verified = (
            registry_status.get("productionAuthorityPinned") is True
        )
        self._key_ids = active_ids

    def _assert_registry_current(self) -> None:
        try:
            before = self._registry.path.read_bytes()
            after = self._registry.path.read_bytes()
        except OSError as exc:
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-key-registry-file-unreadable:{type(exc).__name__}"
            ) from None
        before_hash = hashlib.sha256(before).hexdigest()
        after_hash = hashlib.sha256(after).hexdigest()
        status = self._registry.status()
        if (
            before != after
            or not hmac.compare_digest(before_hash, after_hash)
            or not hmac.compare_digest(before_hash, self._registry_file_hash)
            or status.get("manifestFresh") is not True
            or status.get("manifestHash") != self._registry_manifest_hash
            or status.get("rootKeyIdHash") != self._registry_root_key_id_hash
            or status.get("registryEpoch") != self._registry_epoch
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "reader-key-registry-current-binding-invalid"
            )

    def _verify_signature(
        self,
        *,
        component: str,
        domain: str,
        body: Mapping[str, Any],
        signature: Any,
        key_id_hash: Any,
        label: str,
    ) -> None:
        if (
            type(signature) is not str
            or not signature
            or len(signature) > 256
            or not signature.isascii()
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"{label}-signature-invalid"
            )
        signature_value = signature
        key_id_value = _sha(key_id_hash, f"{label}-key-id")
        if not hmac.compare_digest(key_id_value, self._key_ids[component]):
            raise KisDomesticFunctionalReadersBlocked(f"{label}-key-id-mismatch")
        try:
            valid = self._registry.verify(
                purpose=_COMPONENT_KEY_PURPOSES[component],
                domain=domain,
                body=deepcopy(dict(body)),
                signature=signature_value,
                key_id_hash=key_id_value,
            )
        except BaseException as exc:
            raise KisDomesticFunctionalReadersBlocked(
                f"{label}-verifier-failed:{type(exc).__name__}"
            ) from None
        if valid is not True:
            raise KisDomesticFunctionalReadersBlocked(
                f"{label}-signature-mismatch"
            )

    def _verify_provenance(
        self,
        *,
        component: str,
        provenance_value: Any,
        records: Sequence[Mapping[str, Any]],
        component_schema_fingerprint: str,
    ) -> dict[str, Any]:
        provenance = _mapping(
            provenance_value, f"reader-provenance:{component}"
        )
        _exact_keys(provenance, _PROVENANCE_KEYS, f"reader-provenance:{component}")
        expected_source = (
            "SIGNED_TRANSPORT_ARCHIVE_READ"
            if component == "truth"
            else "SQLITE_IMMUTABLE_READ"
        )
        if (
            provenance.get("schemaVersion") != READ_PROVENANCE_SCHEMA
            or provenance.get("sourceKind") != expected_source
            or provenance.get("sqliteOpenMode")
            != ("NOT_APPLICABLE" if component == "truth" else "mode=ro&immutable=1")
            or provenance.get("transactionMode") != "READ_ONLY_SNAPSHOT"
            or type(provenance.get("pragmaQueryOnly")) is not bool
            or provenance.get("pragmaQueryOnly") is not True
            or type(provenance.get("immutableUri")) is not bool
            or provenance.get("immutableUri") is not True
            or type(provenance.get("writesAttempted")) is not int
            or provenance.get("writesAttempted") != 0
            or type(provenance.get("networkAccessed")) is not bool
            or provenance.get("networkAccessed") is not False
            or type(provenance.get("rowCount")) is not int
            or provenance.get("rowCount") != len(records)
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-provenance-contract-invalid:{component}"
            )
        _sha(provenance.get("databaseIdentityHash"), "reader-database-identity")
        _sha(provenance.get("queryHash"), "reader-query-hash")
        if not hmac.compare_digest(
            _sha(provenance.get("schemaFingerprint"), "reader-schema-fingerprint"),
            component_schema_fingerprint,
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-provenance-schema-mismatch:{component}"
            )
        read_set_hash = _hash([record.get("recordHash") for record in records])
        if not hmac.compare_digest(
            _sha(provenance.get("readSetHash"), "reader-read-set-hash"),
            read_set_hash,
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-provenance-read-set-mismatch:{component}"
            )
        completeness = _record_completeness(records)
        if any(
            provenance.get(key) != value
            for key, value in completeness.items()
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-provenance-completeness-mismatch:{component}"
            )
        return deepcopy(dict(provenance))

    def _verify_record(
        self,
        *,
        component: str,
        value: Any,
    ) -> dict[str, Any]:
        record = _mapping(value, f"reader-record:{component}")
        _exact_keys(record, _ROW_KEYS, f"reader-record:{component}")
        record_type = record.get("recordType")
        if (
            type(record_type) is not str
            or record_type not in _RECORD_TYPES[component]
            or record.get("signatureDomain") != _RECORD_TYPES[component][record_type]
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-record-type-invalid:{component}"
            )
        primary_key = _identifier(
            record.get("primaryKey"), f"reader-record-primary-key:{component}"
        )
        revision = record.get("revision")
        if type(revision) is not int or revision < 1:
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-record-revision-invalid:{component}"
            )
        body = _mapping(record.get("body"), f"reader-record-body:{component}")
        required_join_fields = {
            "route",
            "pdno",
            "sessionId",
            "accountFingerprint",
            "preactivationBaselineHash",
        }
        if (
            not required_join_fields.issubset(body)
            or body["route"] != ROUTE
            or body["pdno"] != PDNO
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-record-required-join-fields-invalid:{component}"
            )
        record_hash = _sha(
            record.get("recordHash"), f"reader-record-hash:{component}"
        )
        if not hmac.compare_digest(record_hash, _hash(body)):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-record-body-hash-mismatch:{component}"
            )
        stored = _mapping(
            record.get("storedColumns"), f"reader-stored-columns:{component}"
        )
        rules = _mapping(
            record.get("projectionRules"), f"reader-projection-rules:{component}"
        )
        base_columns = {
            "primary_key",
            "revision",
            "record_hash",
            "signature",
            "authority_key_id_hash",
        }
        if set(stored) != base_columns | set(rules):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-row-projection-columns-invalid:{component}"
            )
        if (
            stored.get("primary_key") != primary_key
            or type(stored.get("revision")) is not int
            or stored.get("revision") != revision
            or stored.get("record_hash") != record_hash
            or stored.get("signature") != record.get("signature")
            or stored.get("authority_key_id_hash")
            != record.get("authorityKeyIdHash")
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-row-base-projection-mismatch:{component}"
            )
        for column, pointer in rules.items():
            _identifier(column, f"reader-projection-column:{component}")
            if stored[column] != _json_pointer(
                body, pointer, f"reader-projection:{component}:{column}"
            ):
                raise KisDomesticFunctionalReadersBlocked(
                    f"reader-row-projection-mismatch:{component}:{column}"
                )
        signed = {**dict(body), "recordHash": record_hash}
        self._verify_signature(
            component=component,
            domain=str(record["signatureDomain"]),
            body=signed,
            signature=record.get("signature"),
            key_id_hash=record.get("authorityKeyIdHash"),
            label=f"reader-record:{component}:{record_type}",
        )
        return deepcopy(dict(record))

    def _verify_component(self, component: str, value: Any) -> dict[str, Any]:
        envelope = _mapping(value, f"reader-component:{component}")
        _exact_keys(envelope, _ENVELOPE_KEYS, f"reader-component:{component}")
        if (
            envelope.get("schemaVersion") != COMPONENT_ENVELOPE_SCHEMA
            or envelope.get("component") != component
            or envelope.get("route") != ROUTE
            or envelope.get("pdno") != PDNO
            or envelope.get("componentSchemaVersion")
            != COMPONENT_SCHEMA_VERSIONS[component]
            or envelope.get("componentProtocolHash")
            != COMPONENT_PROTOCOL_HASHES[component]
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-component-contract-invalid:{component}"
            )
        actual_file_hash = _actual_component_file_hash(component)
        pinned_file_hash = PINNED_COMPONENT_FILE_HASHES[component]
        if (
            envelope.get("sourceFileHash") != pinned_file_hash
            or not hmac.compare_digest(actual_file_hash, pinned_file_hash)
        ):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-component-file-drift:{component}"
            )
        schema_fingerprint = _sha(
            envelope.get("componentSchemaFingerprint"),
            f"reader-component-schema:{component}",
        )
        _sha(
            envelope.get("componentStatusHash"),
            f"reader-component-status:{component}",
        )
        component_status = _mapping(
            envelope.get("componentStatus"),
            f"reader-component-status-body:{component}",
        )
        if component == "market_source":
            # Unlike generic archive adapters, this new production seam has a
            # frozen code-owned status contract.  Require the complete raw
            # component status (including every false availability and
            # provenance blocker) rather than accepting a caller-shaped
            # generic disabled status.
            expected_status = market_source_component_status()
            if (
                dict(component_status) != expected_status
                or envelope.get("componentStatusHash") != _hash(expected_status)
            ):
                raise KisDomesticFunctionalReadersBlocked(
                    "reader-component-status-invalid:market_source"
                )
        else:
            _exact_keys(
                component_status,
                _COMPONENT_STATUS_KEYS,
                f"reader-component-status-body:{component}",
            )
            if (
                component_status.get("component") != component
                or component_status.get("route") != ROUTE
                or component_status.get("pdno") != PDNO
                or type(component_status.get("schemaVersion")) is not str
                or not component_status.get("schemaVersion")
                or type(component_status.get("productionAvailable")) is not bool
                or component_status.get("productionAvailable") is not False
                or type(component_status.get("networkAvailable")) is not bool
                or component_status.get("networkAvailable") is not False
                or type(component_status.get("mutationAvailable")) is not bool
                or component_status.get("mutationAvailable") is not False
                or type(component_status.get("releaseAvailable")) is not bool
                or component_status.get("releaseAvailable") is not False
                or type(component_status.get("networkOrderPostAllowed")) is not bool
                or component_status.get("networkOrderPostAllowed") is not False
                or type(component_status.get("tradingMutationCount")) is not int
                or component_status.get("tradingMutationCount") != 0
                or envelope.get("componentStatusHash") != _hash(component_status)
            ):
                raise KisDomesticFunctionalReadersBlocked(
                    f"reader-component-status-invalid:{component}"
                )
        records_value = envelope.get("records")
        if not isinstance(records_value, list):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-component-records-invalid:{component}"
            )
        records = [
            self._verify_record(component=component, value=item)
            for item in records_value
        ]
        identities = [
            (item["recordType"], item["primaryKey"]) for item in records
        ]
        if len(set(identities)) != len(identities):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-component-record-duplicate:{component}"
            )
        provenance = self._verify_provenance(
            component=component,
            provenance_value=envelope.get("readProvenance"),
            records=records,
            component_schema_fingerprint=schema_fingerprint,
        )
        unsigned = {
            key: deepcopy(envelope[key])
            for key in sorted(_ENVELOPE_KEYS - {"envelopeHash", "signature"})
        }
        envelope_hash = _sha(
            envelope.get("envelopeHash"), f"reader-envelope-hash:{component}"
        )
        if not hmac.compare_digest(envelope_hash, _hash(unsigned)):
            raise KisDomesticFunctionalReadersBlocked(
                f"reader-envelope-hash-mismatch:{component}"
            )
        self._verify_signature(
            component=component,
            domain=f"KIS_DOMESTIC_FUNCTIONAL_FROZEN_READER:{component.upper()}",
            body={**unsigned, "envelopeHash": envelope_hash},
            signature=envelope.get("signature"),
            key_id_hash=envelope.get("authorityKeyIdHash"),
            label=f"reader-envelope:{component}",
        )
        return {
            **deepcopy(dict(envelope)),
            "readProvenance": provenance,
            "records": records,
        }

    @staticmethod
    def _record_bodies(
        component_record: Mapping[str, Any], record_type: str
    ) -> list[Mapping[str, Any]]:
        return [
            _mapping(item["body"], "reader-canonical-record-body")
            for item in component_record["records"]
            if item["recordType"] == record_type
        ]

    def read(self, request: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_registry_current()
        request_value = _mapping(request, "reader-request")
        if set(request_value) != {
            "schemaVersion",
            "sessionId",
            "accountFingerprint",
            "preactivationBaselineHash",
            "components",
        }:
            raise KisDomesticFunctionalReadersBlocked(
                "reader-request-keys-not-exact"
            )
        if request_value.get("schemaVersion") != READER_INPUT_SCHEMA:
            raise KisDomesticFunctionalReadersBlocked("reader-request-schema-invalid")
        session_id = _identifier(request_value.get("sessionId"), "reader-session-id")
        account_fingerprint = _sha(
            request_value.get("accountFingerprint"), "reader-account-fingerprint"
        )
        if not hmac.compare_digest(
            account_fingerprint, self._registry_account_fingerprint
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "reader-key-registry-account-binding-mismatch"
            )
        baseline_hash = _sha(
            request_value.get("preactivationBaselineHash"),
            "reader-preactivation-baseline-hash",
        )
        component_values = _mapping(
            request_value.get("components"), "reader-components"
        )
        unknown = set(component_values) - set(_COMPONENTS)
        if unknown:
            raise KisDomesticFunctionalReadersBlocked(
                "reader-components-unknown:" + ",".join(sorted(unknown))
            )

        blockers = [
            "IMMUTABLE_SQLITE_COMPONENT_ARCHIVES_NOT_FETCHED",
            "INDEPENDENT_TRUTH_ARCHIVE_NOT_FETCHED",
            "MARKET_SOURCE_SPECIALIZED_ARCHIVE_NOT_FETCHED",
            "MARKET_SOURCE_POST_OBSERVATION_PREFIX_EXTENSION_NOT_JOINED",
            "MARKET_ARCHIVE_EXTERNAL_ASYMMETRIC_AUTHORITY_NOT_PINNED",
            "MARKET_ARCHIVE_RELEASE_COMPLETENESS_FALSE",
            "ACCOUNT_WIDE_CAUSAL_CLOSURE_UNPROVEN",
            "OPERATOR_EXCLUSIVITY_UNPROVEN",
            "MANUAL_OR_OTHER_KEY_ACTIVITY_EXCLUSION_UNPROVEN",
        ]
        blockers.extend(
            (
                "VERIFY_ONLY_PRODUCTION_AUTHORITY_NOT_WIRED",
                "KEY_REGISTRY_DURABLE_ANTI_ROLLBACK_NOT_WIRED",
                "PRODUCTION_KEY_REGISTRY_FACTORY_PINS_NOT_WIRED",
                "KEY_REGISTRY_TRUSTED_WALL_MONOTONIC_LINEAGE_NOT_WIRED",
            )
        )
        verified: dict[str, dict[str, Any]] = {}
        canonical_records: dict[str, list[dict[str, Any]]] = {}
        for component in _COMPONENTS:
            raw = component_values.get(component)
            if raw is None:
                blockers.append(f"COMPONENT_RECORDS_MISSING:{component}")
                canonical_records[component] = []
                continue
            envelope = self._verify_component(component, raw)
            verified[component] = envelope
            canonical_records[component] = [
                deepcopy(dict(item["body"])) for item in envelope["records"]
            ]
            present_types = {item["recordType"] for item in envelope["records"]}
            cardinality = Counter(
                item["recordType"] for item in envelope["records"]
            )
            for record_type, expected_count in sorted(
                _EXPECTED_TYPE_CARDINALITY[component].items()
            ):
                observed_count = cardinality.get(record_type, 0)
                if observed_count != expected_count:
                    blockers.append(
                        "COMPONENT_RECORD_CARDINALITY_MISMATCH:"
                        f"{component}:{record_type}:{observed_count}:{expected_count}"
                    )
            for item in envelope["records"]:
                body = item["body"]
                if body["sessionId"] != session_id:
                    blockers.append(f"SESSION_JOIN_MISMATCH:{component}")
                if body["accountFingerprint"] != account_fingerprint:
                    blockers.append(f"ACCOUNT_JOIN_MISMATCH:{component}")
                if body["preactivationBaselineHash"] != baseline_hash:
                    blockers.append(f"BASELINE_JOIN_MISMATCH:{component}")

        mutation_keys: set[str] | None = None
        rolling_keys: set[str] | None = None
        if "mutation" in verified:
            integrity = self._record_bodies(
                verified["mutation"], "MUTATION_INTEGRITY"
            )
            if len(integrity) == 1:
                raw_keys = integrity[0].get("baselineOrderKeys")
                if isinstance(raw_keys, list) and all(
                    type(item) is str and item for item in raw_keys
                ) and len(set(raw_keys)) == len(raw_keys):
                    mutation_keys = set(raw_keys)
        if "rolling" in verified:
            baselines = self._record_bodies(
                verified["rolling"], "ROLLING_BASELINE"
            )
            if len(baselines) == 1:
                normalized = baselines[0].get("normalized")
                if isinstance(normalized, Mapping):
                    rows_by_key = normalized.get("accountWideOrderRowsByKey")
                    if isinstance(rows_by_key, Mapping) and all(
                        type(key) is str and key for key in rows_by_key
                    ):
                        rolling_keys = set(rows_by_key)
        if mutation_keys is None or rolling_keys is None:
            blockers.append("MUTATION_BASELINE_ORDER_KEYS_NOT_JOINED")
        elif mutation_keys != rolling_keys:
            blockers.append("MUTATION_BASELINE_ORDER_KEYS_MISMATCH")

        capability_records = (
            self._record_bodies(verified["capability"], "CAPABILITY_REVOKE")
            if "capability" in verified
            else []
        )
        if len(capability_records) != 1 or not all(
            capability_records[0].get(key) is True
            for key in (
                "externallyRevoked",
                "runtimeReaderConfirmedClear",
                "globalReaderConfirmedClear",
            )
        ):
            blockers.append("EXTERNAL_CAPABILITY_REVOKE_NOT_JOINED")

        join_blocker_prefixes = (
            "COMPONENT_",
            "MARKET_SOURCE_",
            "MARKET_ARCHIVE_",
            "SESSION_JOIN_",
            "ACCOUNT_JOIN_",
            "BASELINE_JOIN_",
            "MUTATION_BASELINE_",
            "EXTERNAL_CAPABILITY_",
        )
        exact_joins_passed = not any(
            blocker.startswith(join_blocker_prefixes) for blocker in blockers
        )
        body = {
            "schemaVersion": READER_OUTPUT_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": session_id,
            "accountFingerprint": account_fingerprint,
            "preactivationBaselineHash": baseline_hash,
            "verifiedComponentEnvelopes": verified,
            "canonicalRawRecords": canonical_records,
            "marketSourceArchiveEvidence": None,
            "componentFileHashes": dict(PINNED_COMPONENT_FILE_HASHES),
            "componentProtocolHashes": dict(COMPONENT_PROTOCOL_HASHES),
            "componentKeyPurposes": {
                **dict(_COMPONENT_KEY_PURPOSES),
                **dict(_SPECIALIZED_KEY_PURPOSES),
            },
            "componentAuthorityKeyIdHashes": dict(self._key_ids),
            "keyRegistryManifestHash": self._registry_manifest_hash,
            "keyRegistryManifestFileHash": self._registry_file_hash,
            "keyRegistryRootKeyIdHash": self._registry_root_key_id_hash,
            "keyRegistryEpoch": self._registry_epoch,
            "keyRegistryAccountFingerprint": (
                self._registry_account_fingerprint
            ),
            "keyRegistryCredentialConfigurationHash": (
                self._registry_credential_configuration_hash
            ),
            "keyRegistryCodeManifestHash": self._registry_code_manifest_hash,
            "keyRegistryAsymmetricRootVerified": (
                self._registry_asymmetric_root_verified
            ),
            "productionAuthorityPinned": False,
            "readinessBlockers": sorted(set(blockers)),
            "allRequiredRawRecordsJoined": exact_joins_passed,
            "allExactJoinsPassed": exact_joins_passed,
            "mutationBaselineOrderKeysJoined": (
                mutation_keys is not None
                and rolling_keys is not None
                and mutation_keys == rolling_keys
            ),
            "operatorExclusivityProven": False,
            "accountWideCausalClosureProven": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
            "stateServerWired": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**body, "readerBundleHash": _hash(body)}

    def read_from_archives(
        self,
        *,
        session_id: str,
        account_fingerprint: str,
        preactivation_baseline_hash: str,
        sqlite_archives: Mapping[str, ImmutableSqliteComponentArchiveReader],
        truth_archive: ImmutableTruthArchiveReader,
        market_archive: ImmutableMarketSourceArchiveReader | None = None,
    ) -> dict[str, Any]:
        expected_sqlite = set(_COMPONENTS) - {"truth"}
        if set(sqlite_archives) != expected_sqlite:
            raise KisDomesticFunctionalReadersBlocked(
                "reader-sqlite-archives-not-exact"
            )
        if type(truth_archive) is not ImmutableTruthArchiveReader:
            raise KisDomesticFunctionalReadersBlocked(
                "reader-truth-archive-type-invalid"
            )
        if market_archive is not None and (
            type(market_archive) is not ImmutableMarketSourceArchiveReader
            or market_archive.registry is not self._registry
            or market_archive.expected_manifest_hash
            != self._registry_manifest_hash
            or market_archive.expected_registry_epoch != self._registry_epoch
        ):
            raise KisDomesticFunctionalReadersBlocked(
                "reader-market-archive-type-or-registry-invalid"
            )
        components: dict[str, Any] = {}
        evidence: dict[str, Any] = {}
        for component in sorted(expected_sqlite):
            archive = sqlite_archives[component]
            if (
                type(archive) is not ImmutableSqliteComponentArchiveReader
                or archive.component != component
            ):
                raise KisDomesticFunctionalReadersBlocked(
                    f"reader-sqlite-archive-type-invalid:{component}"
                )
            components[component], evidence[component] = archive.read()
        components["truth"], evidence["truth"] = truth_archive.read()
        market_evidence = (
            market_archive.read() if market_archive is not None else None
        )
        result = self.read(
            {
                "schemaVersion": READER_INPUT_SCHEMA,
                "sessionId": session_id,
                "accountFingerprint": account_fingerprint,
                "preactivationBaselineHash": preactivation_baseline_hash,
                "components": components,
            }
        )
        removable = {
            "IMMUTABLE_SQLITE_COMPONENT_ARCHIVES_NOT_FETCHED",
            "INDEPENDENT_TRUTH_ARCHIVE_NOT_FETCHED",
        }
        if market_evidence is not None:
            removable.add("MARKET_SOURCE_SPECIALIZED_ARCHIVE_NOT_FETCHED")
        blockers = [
            item
            for item in result["readinessBlockers"]
            if item not in removable
        ]
        body = {
            key: deepcopy(value)
            for key, value in result.items()
            if key != "readerBundleHash"
        }
        body["readinessBlockers"] = blockers
        body["marketSourceArchiveEvidence"] = market_evidence
        body["immutableArchiveEvidence"] = evidence
        if market_evidence is not None:
            body["immutableArchiveEvidence"]["market_source"] = market_evidence
        body["allImmutableArchivesFetched"] = True
        body["independentTruthArchiveFetched"] = True
        # Exact immutable archives and a valid asymmetric manifest are
        # necessary, but not sufficient production authority.  The registry
        # still lacks the durable accepted-epoch/head CAS and independently
        # pinned production factory required to prevent rollback/substitution.
        body["productionAuthorityPinned"] = False
        return {**body, "readerBundleHash": _hash(body)}


def readers_component_status() -> dict[str, Any]:
    market_archive_status = _market_archive_module().market_archive_component_status()
    body = {
        "schemaVersion": "kis-domestic-functional-readers-status/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "componentNames": list(_COMPONENTS) + ["market_source"],
        "componentProtocolHash": READERS_COMPONENT_PROTOCOL_HASH,
        "componentSchemaFingerprint": READERS_COMPONENT_SCHEMA_FINGERPRINT,
        "componentFileHashes": dict(PINNED_COMPONENT_FILE_HASHES),
        "componentProtocolHashes": dict(COMPONENT_PROTOCOL_HASHES),
        "marketArchiveReaderProtocolHash": MARKET_ARCHIVE_READER_PROTOCOL_HASH,
        "marketArchiveComponentStatus": market_archive_status,
        "marketArchiveComponentStatusHash": _hash(market_archive_status),
        "sqliteArchiveSchemaFingerprint": SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
        "sqliteArchiveQueryHash": SQLITE_ARCHIVE_QUERY_HASH,
        "truthArchiveSchemaFingerprint": TRUTH_ARCHIVE_SCHEMA_FINGERPRINT,
        "truthArchiveQueryHash": TRUTH_ARCHIVE_QUERY_HASH,
        "componentKeyPurposes": {
            **dict(_COMPONENT_KEY_PURPOSES),
            **dict(_SPECIALIZED_KEY_PURPOSES),
        },
        "exactKeyRegistryTypeRequired": "VerifyOnlyKeyRegistry",
        "verifyOnly": True,
        "productionAuthorityPinned": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "stateServerWired": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
    }
    return {**body, "statusHash": _hash(body)}


__all__ = [
    "COMPONENT_ENVELOPE_SCHEMA",
    "COMPONENT_PROTOCOL_HASHES",
    "COMPONENT_SCHEMA_VERSIONS",
    "KIS_DOMESTIC_FUNCTIONAL_READERS_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_READERS_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_READERS_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_READERS_RELEASE_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_READERS_STATE_SERVER_WIRED",
    "KisDomesticFunctionalReadersBlocked",
    "KisDomesticFunctionalVerifyOnlyReaders",
    "ImmutableSqliteComponentArchiveReader",
    "ImmutableMarketSourceArchiveReader",
    "ImmutableTruthArchiveReader",
    "PINNED_COMPONENT_FILE_HASHES",
    "MARKET_ARCHIVE_READER_PROTOCOL_HASH",
    "READ_PROVENANCE_SCHEMA",
    "READER_INPUT_SCHEMA",
    "READER_OUTPUT_SCHEMA",
    "READERS_COMPONENT_PROTOCOL_HASH",
    "READERS_COMPONENT_SCHEMA_FINGERPRINT",
    "SQLITE_ARCHIVE_QUERY_HASH",
    "SQLITE_ARCHIVE_SCHEMA",
    "SQLITE_ARCHIVE_SCHEMA_FINGERPRINT",
    "SQLITE_ARCHIVE_SCHEMA_SQL",
    "TRUTH_ARCHIVE_QUERY_HASH",
    "TRUTH_ARCHIVE_SCHEMA",
    "TRUTH_ARCHIVE_SCHEMA_FINGERPRINT",
    "component_protocol_hash",
    "readers_component_status",
]
