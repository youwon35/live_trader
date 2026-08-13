from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from live_trader.kis_domestic_functional_archive_builder import (
    PRODUCER_LEDGER_QUERY_HASH,
    PRODUCER_LEDGER_SCHEMA,
    PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
    PRODUCER_LEDGER_SCHEMA_SQL,
    TrustedAtomicProducerLedgerArchiveBuilder,
    KisDomesticFunctionalArchiveBuilderBlocked,
    archive_builder_component_status,
)
from live_trader.kis_domestic_functional_contract import PDNO, ROUTE
from live_trader.kis_domestic_functional_market_source import (
    market_source_component_status,
)
from live_trader.kis_domestic_functional_readers import (
    COMPONENT_ENVELOPE_SCHEMA,
    COMPONENT_PROTOCOL_HASHES,
    COMPONENT_SCHEMA_VERSIONS,
    PINNED_COMPONENT_FILE_HASHES,
    READ_PROVENANCE_SCHEMA,
    SQLITE_ARCHIVE_QUERY_HASH,
    SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
    ImmutableSqliteComponentArchiveReader,
    KisDomesticFunctionalVerifyOnlyReaders,
    _COMPONENTS,
    _archive_logical_identity,
    _canonical,
    _hash,
    _record_completeness,
)
from tests.test_kis_domestic_functional_readers import (
    _Authority,
    _registry,
)


SESSION_ID = "kis-session-archive-builder-0001"
ACCOUNT = hashlib.sha256(b"archive-account").hexdigest()
BASELINE = hashlib.sha256(b"archive-baseline").hexdigest()


class _Fixture:
    def __init__(self, component: str = "capability") -> None:
        self.component = component
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "producer.sqlite3"
        self.archive = self.root / "archive.sqlite3"
        self.authorities = {
            component: _Authority(component) for component in _COMPONENTS
        }
        self.registry, self.registry_path, component_ids = _registry(
            self.authorities,
            account_fingerprint=ACCOUNT,
        )
        self.readers = KisDomesticFunctionalVerifyOnlyReaders(
            key_registry=self.registry,
            expected_registry_manifest_hash=self.registry.manifest_hash,
            expected_registry_root_key_id_hash=self.registry.root_key_id_hash,
            expected_registry_epoch=self.registry.registry_epoch,
            expected_component_key_id_hashes=component_ids,
        )
        self.envelope = self._envelope()
        self._write_source(self.envelope)

    def _envelope(self):
        component = self.component
        authority = self.authorities[component]
        body = {
            "schemaVersion": "test-capability-revoke/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": SESSION_ID,
            "accountFingerprint": ACCOUNT,
            "preactivationBaselineHash": BASELINE,
        }
        if component == "capability":
            body.update(
                {
                    "externallyRevoked": True,
                    "runtimeReaderConfirmedClear": True,
                    "globalReaderConfirmedClear": True,
                }
            )
        record_type = (
            "MARKET_SOURCE_RECORD"
            if component == "market_source"
            else "CAPABILITY_REVOKE"
        )
        record_hash = _hash(body)
        record = {
            "recordType": record_type,
            "signatureDomain": record_type,
            "primaryKey": f"{component}-record-0001",
            "revision": 1,
            "body": body,
            "recordHash": record_hash,
            "signature": authority.sign(
                record_type, {**body, "recordHash": record_hash}
            ),
            "authorityKeyIdHash": authority.key_id_hash,
            "storedColumns": {
                "primary_key": f"{component}-record-0001",
                "revision": 1,
                "record_hash": record_hash,
                "signature": "",
                "authority_key_id_hash": authority.key_id_hash,
            },
            "projectionRules": {},
        }
        record["storedColumns"]["signature"] = record["signature"]
        records = [record]
        completeness = _record_completeness(records)
        status = (
            market_source_component_status()
            if component == "market_source"
            else {
                "schemaVersion": "test-capability-status/v1",
                "component": component,
                "route": ROUTE,
                "pdno": PDNO,
                "productionAvailable": False,
                "networkAvailable": False,
                "mutationAvailable": False,
                "releaseAvailable": False,
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
            }
        )
        status_hash = _hash(status)
        database_identity = _archive_logical_identity(
            component=component,
            schema_fingerprint=SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            status_hash=status_hash,
            query_hash=SQLITE_ARCHIVE_QUERY_HASH,
            records=records,
        )
        provenance = {
            "schemaVersion": READ_PROVENANCE_SCHEMA,
            "sourceKind": "SQLITE_IMMUTABLE_READ",
            "databaseIdentityHash": database_identity,
            "schemaFingerprint": SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            "queryHash": SQLITE_ARCHIVE_QUERY_HASH,
            "sqliteOpenMode": "mode=ro&immutable=1",
            "transactionMode": "READ_ONLY_SNAPSHOT",
            "pragmaQueryOnly": True,
            "immutableUri": True,
            "rowCount": 1,
            "writesAttempted": 0,
            "networkAccessed": False,
            "readSetHash": _hash([record_hash]),
            **completeness,
        }
        unsigned = {
            "schemaVersion": COMPONENT_ENVELOPE_SCHEMA,
            "component": component,
            "route": ROUTE,
            "pdno": PDNO,
            "sourceFileHash": PINNED_COMPONENT_FILE_HASHES[component],
            "componentSchemaVersion": COMPONENT_SCHEMA_VERSIONS[component],
            "componentSchemaFingerprint": SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            "componentStatusHash": status_hash,
            "componentStatus": status,
            "componentProtocolHash": COMPONENT_PROTOCOL_HASHES[component],
            "authorityKeyIdHash": authority.key_id_hash,
            "readProvenance": provenance,
            "records": records,
        }
        envelope_hash = _hash(unsigned)
        return {
            **unsigned,
            "envelopeHash": envelope_hash,
            "signature": authority.sign(
                f"KIS_DOMESTIC_FUNCTIONAL_FROZEN_READER:{component.upper()}",
                {**unsigned, "envelopeHash": envelope_hash},
            ),
        }

    def _write_source(self, envelope):
        records = envelope["records"]
        completeness = _record_completeness(records)
        source_identity = _archive_logical_identity(
            component=self.component,
            schema_fingerprint=PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
            status_hash=envelope["componentStatusHash"],
            query_hash=PRODUCER_LEDGER_QUERY_HASH,
            records=records,
        )
        archive_identity = _archive_logical_identity(
            component=self.component,
            schema_fingerprint=SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
            status_hash=envelope["componentStatusHash"],
            query_hash=SQLITE_ARCHIVE_QUERY_HASH,
            records=records,
        )
        source_binding = {
            "schemaVersion": "kis-domestic-functional-producer-extraction-binding/v1",
            "component": self.component,
            "sourceSchemaFingerprint": PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
            "sourceQueryHash": PRODUCER_LEDGER_QUERY_HASH,
            "sourceFileHash": PINNED_COMPONENT_FILE_HASHES[self.component],
            "componentStatusHash": envelope["componentStatusHash"],
            "recordCount": len(records),
            **completeness,
            "sourceDatabaseIdentityHash": source_identity,
            "archiveDatabaseIdentityHash": archive_identity,
            "envelopeHash": envelope["envelopeHash"],
        }
        source_binding_hash = _hash(source_binding)
        authority = self.authorities[self.component]
        conn = sqlite3.connect(self.source)
        try:
            for statement in PRODUCER_LEDGER_SCHEMA_SQL:
                conn.execute(statement)
            previous = "0" * 64
            for ordinal, record in enumerate(records, 1):
                conn.execute(
                    "INSERT INTO kis_functional_producer_snapshot_record "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        ordinal,
                        record["recordType"],
                        record["primaryKey"],
                        record["revision"],
                        previous,
                        record["recordHash"],
                        _canonical(record).decode(),
                    ),
                )
                previous = _record_completeness(records[:ordinal])[
                    "recordHeadHash"
                ]
            conn.execute(
                "INSERT INTO kis_functional_producer_snapshot_meta "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    1,
                    PRODUCER_LEDGER_SCHEMA,
                    PRODUCER_LEDGER_SCHEMA_FINGERPRINT,
                    self.component,
                    COMPONENT_SCHEMA_VERSIONS[self.component],
                    PINNED_COMPONENT_FILE_HASHES[self.component],
                    envelope["componentStatusHash"],
                    PRODUCER_LEDGER_QUERY_HASH,
                    len(records),
                    completeness["recordHighWaterRevision"],
                    completeness["recordHeadHash"],
                    _canonical(envelope).decode(),
                    envelope["envelopeHash"],
                    _canonical(source_binding).decode(),
                    source_binding_hash,
                    authority.sign(
                        "KIS_DOMESTIC_FUNCTIONAL_PRODUCER_EXTRACTION:"
                        f"{self.component.upper()}",
                        {
                            **source_binding,
                            "sourceBindingHash": source_binding_hash,
                        },
                    ),
                    authority.key_id_hash,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def update(self, sql, params=()):
        conn = sqlite3.connect(self.source)
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()

    def builder(self, **kwargs):
        return TrustedAtomicProducerLedgerArchiveBuilder(
            verify_only_readers=self.readers, **kwargs
        )

    def build(self, **kwargs):
        return self.builder(**kwargs).build(
            component=self.component,
            source_path=self.source,
            archive_path=self.archive,
        )

    def cleanup(self):
        self.registry_path.unlink(missing_ok=True)
        self.temp.cleanup()


class KisDomesticFunctionalArchiveBuilderTest(unittest.TestCase):
    def test_happy_atomic_build_is_reader_consumable(self):
        fixture = _Fixture()
        try:
            evidence = fixture.build()
            self.assertTrue(evidence["sourceSnapshotAtomic"])
            self.assertTrue(evidence["sourceExtractionComplete"])
            self.assertEqual(1, evidence["sourceRecordCount"])
            self.assertTrue(evidence["atomicCreateIfAbsentPublished"])
            self.assertTrue(evidence["publishedFileFsynced"])
            self.assertTrue(evidence["parentDirectoryFsynced"])
            self.assertTrue(evidence["publishedArchiveReverified"])
            self.assertEqual(
                evidence["temporaryFinalHash"], evidence["archiveFileHash"]
            )
            envelope, archive_evidence = ImmutableSqliteComponentArchiveReader(
                fixture.archive, component="capability"
            ).read()
            self.assertEqual(fixture.envelope, envelope)
            self.assertEqual(evidence["archiveFileHash"], archive_evidence["archiveFileHash"])
            self.assertFalse(evidence["productionAuthorityRegistryPinned"])
        finally:
            fixture.cleanup()

    def test_generic_builder_rejects_market_source_specialized_archive(self):
        self.assertNotIn("market_source", _COMPONENTS)
        with self.assertRaises(KeyError):
            _Fixture(component="market_source")
        self.assertRegex(
            PINNED_COMPONENT_FILE_HASHES["market_archive"], r"^[0-9a-f]{64}$"
        )
        self.assertEqual(
            "kis-domestic-functional-market-archive-sqlite/v1",
            COMPONENT_SCHEMA_VERSIONS["market_archive"],
        )

    def test_extra_source_object_is_rejected(self):
        fixture = _Fixture()
        try:
            fixture.update("CREATE TABLE unrelated_extra(value TEXT)")
            with self.assertRaisesRegex(
                KisDomesticFunctionalArchiveBuilderBlocked, "extra-object-dirty"
            ):
                fixture.build()
        finally:
            fixture.cleanup()

    def test_dirty_source_shape_is_rejected(self):
        fixture = _Fixture()
        try:
            fixture.update(
                "ALTER TABLE kis_functional_producer_snapshot_record "
                "ADD COLUMN hidden TEXT"
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalArchiveBuilderBlocked, "schema-or-extra-object-dirty"
            ):
                fixture.build()
        finally:
            fixture.cleanup()

    def test_query_schema_file_and_status_binding_tamper_rejected(self):
        cases = (
            ("source_query_hash", "f" * 64),
            ("schema_fingerprint", "f" * 64),
            ("source_file_hash", "f" * 64),
            ("component_status_hash", "f" * 64),
        )
        for column, value in cases:
            with self.subTest(column=column):
                fixture = _Fixture()
                try:
                    fixture.update(
                        f"UPDATE kis_functional_producer_snapshot_meta SET {column}=?",
                        (value,),
                    )
                    with self.assertRaises(KisDomesticFunctionalArchiveBuilderBlocked):
                        fixture.build()
                finally:
                    fixture.cleanup()

    def test_count_highwater_and_head_tamper_rejected(self):
        cases = (
            ("record_count", 2),
            ("high_water_revision", 9),
            ("record_head_hash", "f" * 64),
        )
        for column, value in cases:
            with self.subTest(column=column):
                fixture = _Fixture()
                try:
                    fixture.update(
                        f"UPDATE kis_functional_producer_snapshot_meta SET {column}=?",
                        (value,),
                    )
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalArchiveBuilderBlocked,
                        "completeness-or-binding-invalid",
                    ):
                        fixture.build()
                finally:
                    fixture.cleanup()

    def test_row_gap_projection_and_chain_tamper_rejected(self):
        cases = (
            "UPDATE kis_functional_producer_snapshot_record SET ordinal=2",
            "UPDATE kis_functional_producer_snapshot_record SET revision=2",
            "UPDATE kis_functional_producer_snapshot_record SET previous_hash='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'",
        )
        for sql in cases:
            with self.subTest(sql=sql):
                fixture = _Fixture()
                try:
                    fixture.update(sql)
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalArchiveBuilderBlocked,
                        "row-projection-or-chain-invalid",
                    ):
                        fixture.build()
                finally:
                    fixture.cleanup()

    def test_envelope_vs_source_record_mismatch_is_rejected(self):
        fixture = _Fixture()
        try:
            envelope = json.loads(_canonical(fixture.envelope))
            envelope["records"] = []
            fixture.update(
                "UPDATE kis_functional_producer_snapshot_meta SET envelope_json=?",
                (_canonical(envelope).decode(),),
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalArchiveBuilderBlocked,
                "completeness-or-binding-invalid",
            ):
                fixture.build()
        finally:
            fixture.cleanup()

    def test_unverified_source_signature_is_rejected(self):
        fixture = _Fixture()
        try:
            envelope = json.loads(_canonical(fixture.envelope))
            envelope["signature"] = "f" * 64
            fixture.update(
                "UPDATE kis_functional_producer_snapshot_meta SET envelope_json=?",
                (_canonical(envelope).decode(),),
            )
            with self.assertRaises(Exception):
                fixture.build()
            self.assertFalse(fixture.archive.exists())
        finally:
            fixture.cleanup()

    def test_source_change_during_snapshot_is_rejected(self):
        fixture = _Fixture()
        try:
            def mutate(stage, path):
                if stage == "AFTER_SOURCE_SNAPSHOT_READ":
                    with path.open("ab") as handle:
                        handle.write(b"tamper")

            with self.assertRaisesRegex(
                KisDomesticFunctionalArchiveBuilderBlocked, "source-file-changed"
            ):
                fixture.build(failure_injector=mutate)
            self.assertFalse(fixture.archive.exists())
        finally:
            fixture.cleanup()

    def test_publish_failure_or_existing_destination_never_overwrites(self):
        fixture = _Fixture()
        try:
            def fail(stage, _path):
                if stage == "BEFORE_ARCHIVE_PUBLISH":
                    raise RuntimeError("injected")

            with self.assertRaisesRegex(RuntimeError, "injected"):
                fixture.build(failure_injector=fail)
            self.assertFalse(fixture.archive.exists())
            fixture.archive.write_bytes(b"existing")
            with self.assertRaisesRegex(
                KisDomesticFunctionalArchiveBuilderBlocked, "destination-exists"
            ):
                fixture.build()
            self.assertEqual(b"existing", fixture.archive.read_bytes())
            status = archive_builder_component_status()
            self.assertFalse(status["productionAvailable"])
            self.assertFalse(status["networkOrderPostAllowed"])
            self.assertEqual(0, status["tradingMutationCount"])
        finally:
            fixture.cleanup()

    def test_concurrent_destination_race_is_lost_without_overwrite(self):
        fixture = _Fixture()
        try:
            competing = b"concurrent-winner"

            def race(stage, _path):
                if stage == "BEFORE_ARCHIVE_PUBLISH":
                    fixture.archive.write_bytes(competing)

            with self.assertRaisesRegex(
                KisDomesticFunctionalArchiveBuilderBlocked,
                "destination-race-lost",
            ):
                fixture.build(failure_injector=race)
            self.assertEqual(competing, fixture.archive.read_bytes())
            self.assertEqual([], list(fixture.root.glob("archive.sqlite3.*.tmp")))
        finally:
            fixture.cleanup()

    def test_post_publish_crash_preserves_exact_durable_archive(self):
        fixture = _Fixture()
        try:
            def crash(stage, _path):
                if stage == "AFTER_DIRECTORY_FSYNC":
                    raise RuntimeError("crash-after-durable-publish")

            with self.assertRaisesRegex(RuntimeError, "crash-after-durable-publish"):
                fixture.build(failure_injector=crash)
            self.assertTrue(fixture.archive.is_file())
            envelope, evidence = ImmutableSqliteComponentArchiveReader(
                fixture.archive, component="capability"
            ).read()
            self.assertEqual(fixture.envelope, envelope)
            self.assertEqual(
                hashlib.sha256(fixture.archive.read_bytes()).hexdigest(),
                evidence["archiveFileHash"],
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalArchiveBuilderBlocked,
                "destination-exists",
            ):
                fixture.build()
        finally:
            fixture.cleanup()


if __name__ == "__main__":
    unittest.main()
