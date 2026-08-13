from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import tempfile
import ctypes
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .kis_domestic_functional_readers import (
    COMPONENT_SCHEMA_VERSIONS,
    PINNED_COMPONENT_FILE_HASHES,
    READER_INPUT_SCHEMA,
    SQLITE_ARCHIVE_QUERY_HASH,
    SQLITE_ARCHIVE_SCHEMA,
    SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
    SQLITE_ARCHIVE_SCHEMA_SQL,
    ImmutableSqliteComponentArchiveReader,
    KisDomesticFunctionalVerifyOnlyReaders,
    _COMPONENTS,
    _archive_logical_identity,
    _canonical,
    _hash,
    _normalize_sql,
    _record_completeness,
)


KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_RELEASE_AVAILABLE = False

PRODUCER_LEDGER_SCHEMA = "kis-domestic-functional-producer-ledger-export/v1"
ARCHIVE_BUILD_EVIDENCE_SCHEMA = "kis-domestic-functional-archive-build-evidence/v1"
_SQLITE_COMPONENTS = tuple(item for item in _COMPONENTS if item != "truth")

PRODUCER_LEDGER_SCHEMA_SQL = (
    """
    CREATE TABLE kis_functional_producer_snapshot_meta(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        schema_version TEXT NOT NULL,
        schema_fingerprint TEXT NOT NULL,
        component TEXT NOT NULL,
        component_schema_version TEXT NOT NULL,
        source_file_hash TEXT NOT NULL,
        component_status_hash TEXT NOT NULL,
        source_query_hash TEXT NOT NULL,
        record_count INTEGER NOT NULL CHECK(record_count>=1),
        high_water_revision INTEGER NOT NULL CHECK(high_water_revision>=1),
        record_head_hash TEXT NOT NULL,
        envelope_json TEXT NOT NULL,
        envelope_hash TEXT NOT NULL,
        source_binding_json TEXT NOT NULL,
        source_binding_hash TEXT NOT NULL,
        source_binding_signature TEXT NOT NULL,
        authority_key_id_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE kis_functional_producer_snapshot_record(
        ordinal INTEGER PRIMARY KEY CHECK(ordinal>=1),
        record_type TEXT NOT NULL,
        primary_key TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK(revision>=1),
        previous_hash TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        record_json TEXT NOT NULL
    )
    """,
    "CREATE UNIQUE INDEX ux_kis_functional_producer_snapshot_identity "
    "ON kis_functional_producer_snapshot_record(record_type,primary_key)",
)

PRODUCER_META_QUERY = (
    "SELECT singleton,schema_version,schema_fingerprint,component,"
    "component_schema_version,source_file_hash,component_status_hash,"
    "source_query_hash,record_count,high_water_revision,record_head_hash,"
    "envelope_json,envelope_hash,source_binding_json,source_binding_hash,"
    "source_binding_signature,authority_key_id_hash "
    "FROM kis_functional_producer_snapshot_meta "
    "WHERE singleton=1"
)
PRODUCER_RECORD_QUERY = (
    "SELECT ordinal,record_type,primary_key,revision,previous_hash,record_hash,"
    "record_json FROM kis_functional_producer_snapshot_record ORDER BY ordinal"
)


class KisDomesticFunctionalArchiveBuilderBlocked(RuntimeError):
    pass


def _schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT name,type,sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
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
                for item in conn.execute(
                    f"PRAGMA foreign_key_list({name})"
                ).fetchall()
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


def _expected_schema() -> tuple[dict[str, Any], str]:
    conn = sqlite3.connect(":memory:")
    try:
        for statement in PRODUCER_LEDGER_SCHEMA_SQL:
            conn.execute(statement)
        snapshot = _schema_snapshot(conn)
        return snapshot, _hash(snapshot)
    finally:
        conn.close()


PRODUCER_LEDGER_SCHEMA_SNAPSHOT, PRODUCER_LEDGER_SCHEMA_FINGERPRINT = (
    _expected_schema()
)
PRODUCER_LEDGER_QUERY_HASH = _hash(
    {
        "schemaVersion": "kis-domestic-functional-producer-query/v1",
        "metaQuery": _normalize_sql(PRODUCER_META_QUERY),
        "recordQuery": _normalize_sql(PRODUCER_RECORD_QUERY),
        "transactionMode": "READ_ONLY_SNAPSHOT",
        "ordering": "ordinal ASC",
    }
)


def _read_bytes_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fsync_file(path: Path) -> None:
    handle = os.open(path, os.O_RDWR)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def _fsync_directory(path: Path) -> str:
    if os.name != "nt":
        handle = os.open(path, os.O_RDONLY)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)
        return "POSIX_DIRECTORY_FSYNC"
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.FlushFileBuffers.argtypes = [wintypes.HANDLE]
    kernel32.FlushFileBuffers.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.CreateFileW(
        str(path),
        0x40000000,  # GENERIC_WRITE; required for FlushFileBuffers.
        0x00000007,  # FILE_SHARE_READ | WRITE | DELETE.
        None,
        3,  # OPEN_EXISTING.
        0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS for a directory.
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle in (None, invalid_handle):
        raise OSError(ctypes.get_last_error(), "CreateFileW(directory) failed")
    try:
        if not kernel32.FlushFileBuffers(handle):
            raise OSError(
                ctypes.get_last_error(), "FlushFileBuffers(directory) failed"
            )
    finally:
        kernel32.CloseHandle(handle)
    return "WINDOWS_DIRECTORY_FLUSH_FILE_BUFFERS"


def _publish_create_if_absent(temporary: Path, destination: Path) -> None:
    try:
        os.link(temporary, destination)
    except FileExistsError as exc:
        raise KisDomesticFunctionalArchiveBuilderBlocked(
            "archive-builder-destination-race-lost"
        ) from exc
    except OSError as exc:
        raise KisDomesticFunctionalArchiveBuilderBlocked(
            "archive-builder-atomic-create-if-absent-failed"
        ) from exc


def _parse_records(rows: Sequence[sqlite3.Row]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    previous = "0" * 64
    for ordinal, row in enumerate(rows, 1):
        try:
            record = json.loads(row["record_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-record-json-invalid"
            ) from exc
        if (
            int(row["ordinal"]) != ordinal
            or not isinstance(record, Mapping)
            or row["record_type"] != record.get("recordType")
            or row["primary_key"] != record.get("primaryKey")
            or int(row["revision"]) != record.get("revision")
            or row["record_hash"] != record.get("recordHash")
            or row["previous_hash"] != previous
        ):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-row-projection-or-chain-invalid"
            )
        result.append(deepcopy(dict(record)))
        previous = _record_completeness(result)["recordHeadHash"]
    return result


class TrustedAtomicProducerLedgerArchiveBuilder:
    """Copy a frozen producer-ledger snapshot into the reader archive format.

    The source is opened read-only and every non-SQLite object must equal the
    code-owned producer export schema.  The builder owns no signing key: it
    only accepts an already-signed frozen-reader envelope after the injected
    verify-only reader registry validates all record and envelope signatures.
    """

    def __init__(
        self,
        *,
        verify_only_readers: KisDomesticFunctionalVerifyOnlyReaders,
        failure_injector: Callable[[str, Path], None] | None = None,
    ) -> None:
        if type(verify_only_readers) is not KisDomesticFunctionalVerifyOnlyReaders:
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-verify-only-registry-type-invalid"
            )
        if failure_injector is not None and not callable(failure_injector):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-failure-injector-invalid"
            )
        self._readers = verify_only_readers
        self._injector = failure_injector

    def _inject(self, stage: str, path: Path) -> None:
        if self._injector is not None:
            self._injector(stage, path)

    def build(
        self,
        *,
        component: str,
        source_path: str | Path,
        archive_path: str | Path,
    ) -> dict[str, Any]:
        if component not in _SQLITE_COMPONENTS:
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-component-invalid"
            )
        source = Path(source_path).expanduser().resolve()
        destination = Path(archive_path).expanduser().resolve()
        if not source.is_file():
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-missing"
            )
        if destination.exists():
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-destination-exists"
            )
        for suffix in ("-wal", "-shm", "-journal"):
            if Path(str(source) + suffix).exists():
                raise KisDomesticFunctionalArchiveBuilderBlocked(
                    "archive-builder-source-sidecar-present"
                )

        before_hash = _read_bytes_hash(source)
        uri = source.as_uri() + "?mode=ro&immutable=1"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            if _schema_snapshot(conn) != PRODUCER_LEDGER_SCHEMA_SNAPSHOT:
                raise KisDomesticFunctionalArchiveBuilderBlocked(
                    "archive-builder-source-schema-or-extra-object-dirty"
                )
            meta = conn.execute(PRODUCER_META_QUERY).fetchone()
            rows = conn.execute(PRODUCER_RECORD_QUERY).fetchall()
            self._inject("AFTER_SOURCE_SNAPSHOT_READ", source)
            conn.commit()
        finally:
            conn.close()
        after_hash = _read_bytes_hash(source)
        if not hmac.compare_digest(before_hash, after_hash):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-file-changed"
            )
        if meta is None:
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-meta-missing"
            )
        try:
            envelope = json.loads(meta["envelope_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-envelope-json-invalid"
            ) from exc
        records = _parse_records(rows)
        completeness = _record_completeness(records)
        if (
            int(meta["singleton"]) != 1
            or meta["schema_version"] != PRODUCER_LEDGER_SCHEMA
            or meta["schema_fingerprint"] != PRODUCER_LEDGER_SCHEMA_FINGERPRINT
            or meta["component"] != component
            or meta["component_schema_version"]
            != COMPONENT_SCHEMA_VERSIONS[component]
            or meta["source_file_hash"]
            != PINNED_COMPONENT_FILE_HASHES[component]
            or meta["source_query_hash"] != PRODUCER_LEDGER_QUERY_HASH
            or int(meta["record_count"]) != len(records)
            or int(meta["high_water_revision"])
            != completeness["recordHighWaterRevision"]
            or meta["record_head_hash"] != completeness["recordHeadHash"]
            or not isinstance(envelope, Mapping)
            or envelope.get("envelopeHash") != meta["envelope_hash"]
            or envelope.get("records") != records
            or envelope.get("componentSchemaFingerprint")
            != SQLITE_ARCHIVE_SCHEMA_FINGERPRINT
            or envelope.get("sourceFileHash")
            != PINNED_COMPONENT_FILE_HASHES[component]
        ):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-completeness-or-binding-invalid"
            )
        status = envelope.get("componentStatus")
        provenance = envelope.get("readProvenance")
        if not isinstance(status, Mapping) or not isinstance(provenance, Mapping):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-envelope-contract-invalid"
            )
        source_identity = _archive_logical_identity(
            component=component,
            schema_fingerprint=PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
            status_hash=str(meta["component_status_hash"]),
            query_hash=PRODUCER_LEDGER_QUERY_HASH,
            records=records,
        )
        archive_identity = _archive_logical_identity(
            component=component,
            schema_fingerprint=SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            status_hash=str(meta["component_status_hash"]),
            query_hash=SQLITE_ARCHIVE_QUERY_HASH,
            records=records,
        )
        if (
            envelope.get("componentStatusHash")
            != meta["component_status_hash"]
            or provenance.get("schemaFingerprint")
            != SQLITE_ARCHIVE_SCHEMA_FINGERPRINT
            or provenance.get("queryHash") != SQLITE_ARCHIVE_QUERY_HASH
            or provenance.get("databaseIdentityHash") != archive_identity
            or provenance.get("rowCount") != len(records)
            or any(
                provenance.get(key) != value
                for key, value in completeness.items()
            )
        ):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-provenance-invalid"
            )
        try:
            source_binding = json.loads(meta["source_binding_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-binding-json-invalid"
            ) from exc
        expected_binding = {
            "schemaVersion": "kis-domestic-functional-producer-extraction-binding/v1",
            "component": component,
            "sourceSchemaFingerprint": PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
            "sourceQueryHash": PRODUCER_LEDGER_QUERY_HASH,
            "sourceFileHash": PINNED_COMPONENT_FILE_HASHES[component],
            "componentStatusHash": meta["component_status_hash"],
            "recordCount": len(records),
            **completeness,
            "sourceDatabaseIdentityHash": source_identity,
            "archiveDatabaseIdentityHash": archive_identity,
            "envelopeHash": envelope["envelopeHash"],
        }
        if (
            source_binding != expected_binding
            or meta["source_binding_hash"] != _hash(source_binding)
        ):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-binding-invalid"
            )
        try:
            self._readers._verify_signature(
                component=component,
                domain=f"KIS_DOMESTIC_FUNCTIONAL_PRODUCER_EXTRACTION:{component.upper()}",
                body={
                    **source_binding,
                    "sourceBindingHash": meta["source_binding_hash"],
                },
                signature=meta["source_binding_signature"],
                key_id_hash=meta["authority_key_id_hash"],
                label=f"archive-builder-source-binding:{component}",
            )
        except BaseException as exc:
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                f"archive-builder-source-binding-signature-invalid:{type(exc).__name__}"
            ) from None

        first_body = records[0].get("body") if records else None
        if not isinstance(first_body, Mapping):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-first-record-invalid"
            )
        verified = self._readers.read(
            {
                "schemaVersion": READER_INPUT_SCHEMA,
                "sessionId": first_body.get("sessionId"),
                "accountFingerprint": first_body.get("accountFingerprint"),
                "preactivationBaselineHash": first_body.get(
                    "preactivationBaselineHash"
                ),
                "components": {component: envelope},
            }
        )
        if component not in verified.get("verifiedComponentEnvelopes", {}):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-source-signatures-unverified"
            )

        destination.parent.mkdir(parents=True, exist_ok=True)
        handle, temp_name = tempfile.mkstemp(
            prefix=destination.name + ".", suffix=".tmp", dir=destination.parent
        )
        os.close(handle)
        temporary = Path(temp_name)
        try:
            temporary.unlink()
            out = sqlite3.connect(temporary)
            try:
                for statement in SQLITE_ARCHIVE_SCHEMA_SQL:
                    out.execute(statement)
                previous = "0" * 64
                for ordinal, record in enumerate(records, 1):
                    out.execute(
                        "INSERT INTO kis_reader_archive_record VALUES(?,?,?,?,?,?,?)",
                        (
                            ordinal,
                            record["recordType"],
                            record["primaryKey"],
                            record["revision"],
                            previous,
                            record["recordHash"],
                            _canonical(record).decode("utf-8"),
                        ),
                    )
                    previous = _record_completeness(records[:ordinal])[
                        "recordHeadHash"
                    ]
                out.execute(
                    "INSERT INTO kis_reader_archive_meta VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        1,
                        SQLITE_ARCHIVE_SCHEMA,
                        component,
                        COMPONENT_SCHEMA_VERSIONS[component],
                        SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
                        PINNED_COMPONENT_FILE_HASHES[component],
                        _canonical(status).decode("utf-8"),
                        meta["component_status_hash"],
                        SQLITE_ARCHIVE_QUERY_HASH,
                        len(records),
                        completeness["recordHighWaterRevision"],
                        completeness["recordHeadHash"],
                        _canonical(completeness["recordCardinality"]).decode(
                            "utf-8"
                        ),
                        archive_identity,
                        _canonical(envelope).decode("utf-8"),
                        envelope["envelopeHash"],
                    ),
                )
                out.commit()
            finally:
                out.close()
            temporary_hash = _read_bytes_hash(temporary)
            self._inject("BEFORE_ARCHIVE_PUBLISH", temporary)
            _envelope, temporary_evidence = ImmutableSqliteComponentArchiveReader(
                temporary, component=component
            ).read()
            if temporary_evidence["archiveFileHash"] != temporary_hash:
                raise KisDomesticFunctionalArchiveBuilderBlocked(
                    "archive-builder-temporary-final-hash-mismatch"
                )
            _fsync_file(temporary)
            self._inject("AFTER_TEMP_FILE_FSYNC", temporary)
            _publish_create_if_absent(temporary, destination)
            try:
                self._inject("AFTER_ATOMIC_PUBLISH", destination)
                published_hash = _read_bytes_hash(destination)
                if not hmac.compare_digest(published_hash, temporary_hash):
                    raise KisDomesticFunctionalArchiveBuilderBlocked(
                        "archive-builder-published-final-hash-mismatch"
                    )
                _published_envelope, published_evidence = (
                    ImmutableSqliteComponentArchiveReader(
                        destination, component=component
                    ).read()
                )
                if not hmac.compare_digest(
                    published_evidence["archiveFileHash"], temporary_hash
                ):
                    raise KisDomesticFunctionalArchiveBuilderBlocked(
                        "archive-builder-published-reader-hash-mismatch"
                    )
                _fsync_file(destination)
                directory_sync_method = _fsync_directory(destination.parent)
                self._inject("AFTER_DIRECTORY_FSYNC", destination.parent)
            except BaseException:
                # Publication may already be durable; never unlink an exact
                # create-if-absent winner after the namespace mutation.
                raise
            finally:
                temporary.unlink(missing_ok=True)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

        archive_hash = _read_bytes_hash(destination)
        if not hmac.compare_digest(archive_hash, temporary_hash):
            raise KisDomesticFunctionalArchiveBuilderBlocked(
                "archive-builder-returned-final-hash-mismatch"
            )
        evidence = {
            "schemaVersion": ARCHIVE_BUILD_EVIDENCE_SCHEMA,
            "component": component,
            "sourceFileBeforeHash": before_hash,
            "sourceFileAfterHash": after_hash,
            "sourceSchemaFingerprint": PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
            "sourceQueryHash": PRODUCER_LEDGER_QUERY_HASH,
            "sourceDatabaseIdentityHash": source_identity,
            "sourceRecordCount": len(records),
            **completeness,
            "archiveFileHash": archive_hash,
            "temporaryFinalHash": temporary_hash,
            "archiveSchemaFingerprint": SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            "archiveQueryHash": SQLITE_ARCHIVE_QUERY_HASH,
            "sourceSnapshotAtomic": True,
            "sourceExtractionComplete": True,
            "atomicCreateIfAbsentPublished": True,
            "destinationOverwriteAttempted": False,
            "publishedFileFsynced": True,
            "parentDirectoryFsynced": True,
            "parentDirectorySyncMethod": directory_sync_method,
            "publishedArchiveReverified": True,
            "extraSourceObjectsAbsent": True,
            "verifyOnlyRegistryUsed": True,
            "productionAuthorityRegistryPinned": False,
            "readinessBlockers": [
                "EXTERNAL_PRODUCTION_VERIFY_ONLY_KEY_REGISTRY_NOT_WIRED"
            ],
            "networkAccessed": False,
            "sourceWritesAttempted": 0,
            "tradingMutationCount": 0,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
        }
        return {**evidence, "evidenceHash": _hash(evidence)}


def archive_builder_component_status() -> dict[str, Any]:
    body = {
        "schemaVersion": "kis-domestic-functional-archive-builder-status/v1",
        "producerLedgerSchemaFingerprint": PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
        "producerLedgerQueryHash": PRODUCER_LEDGER_QUERY_HASH,
        "archiveSchemaFingerprint": SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
        "archiveQueryHash": SQLITE_ARCHIVE_QUERY_HASH,
        "sqliteComponents": list(_SQLITE_COMPONENTS),
        "verifyOnly": True,
        "productionAuthorityRegistryPinned": False,
        "readinessBlockers": [
            "EXTERNAL_PRODUCTION_VERIFY_ONLY_KEY_REGISTRY_NOT_WIRED"
        ],
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
    }
    return {**body, "statusHash": _hash(body)}


__all__ = [
    "ARCHIVE_BUILD_EVIDENCE_SCHEMA",
    "KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ARCHIVE_BUILDER_RELEASE_AVAILABLE",
    "KisDomesticFunctionalArchiveBuilderBlocked",
    "PRODUCER_LEDGER_QUERY_HASH",
    "PRODUCER_LEDGER_SCHEMA",
    "PRODUCER_LEDGER_SCHEMA_FINGERPRINT",
    "PRODUCER_LEDGER_SCHEMA_SNAPSHOT",
    "PRODUCER_LEDGER_SCHEMA_SQL",
    "PRODUCER_META_QUERY",
    "PRODUCER_RECORD_QUERY",
    "TrustedAtomicProducerLedgerArchiveBuilder",
    "archive_builder_component_status",
]
