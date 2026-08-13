from __future__ import annotations

import copy
import base64
import hashlib
import hmac
import json
import os
import sqlite3
import tempfile
import unittest
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_contract import PDNO, ROUTE
from live_trader.kis_domestic_functional_key_registry import (
    KEY_PURPOSES,
    REGISTRY_SCHEMA,
    VerifyOnlyKeyRegistry,
)
from live_trader.kis_domestic_functional_market_source import (
    market_source_component_status,
)
from live_trader.kis_domestic_functional_readers import (
    COMPONENT_ENVELOPE_SCHEMA,
    COMPONENT_PROTOCOL_HASHES,
    COMPONENT_SCHEMA_VERSIONS,
    PINNED_COMPONENT_FILE_HASHES,
    READERS_COMPONENT_PROTOCOL_HASH,
    READERS_COMPONENT_SCHEMA_FINGERPRINT,
    READ_PROVENANCE_SCHEMA,
    READER_INPUT_SCHEMA,
    SQLITE_ARCHIVE_QUERY_HASH,
    SQLITE_ARCHIVE_SCHEMA,
    SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
    SQLITE_ARCHIVE_SCHEMA_SQL,
    TRUTH_ARCHIVE_QUERY_HASH,
    TRUTH_ARCHIVE_SCHEMA,
    TRUTH_ARCHIVE_SCHEMA_FINGERPRINT,
    ImmutableSqliteComponentArchiveReader,
    ImmutableMarketSourceArchiveReader,
    ImmutableTruthArchiveReader,
    KisDomesticFunctionalReadersBlocked,
    KisDomesticFunctionalVerifyOnlyReaders,
    readers_component_status,
)


SESSION_ID = "kis-session-readers-0001"
ACCOUNT = hashlib.sha256(b"account").hexdigest()
BASELINE = hashlib.sha256(b"baseline").hexdigest()
COMPONENT_TYPES = {
    "lane": (
        ("LANE_SESSION", "LANE_SESSION"),
        ("BOOTSTRAP", "LANE_BOOTSTRAP"),
        ("APPROVAL", "LANE_APPROVAL"),
        ("EVALUATION", "LANE_EVALUATION"),
        ("TRIGGER", "LANE_TRIGGER"),
        ("ACTION", "LANE_ACTION"),
        ("ACTION", "LANE_ACTION"),
    ),
    "source": (("SOURCE_WINDOW_ARCHIVE", "SOURCE_WINDOW_ARCHIVE"),),
    "rolling": (
        ("ROLLING_DIAGNOSTIC", "ROLLING_DIAGNOSTIC"),
        ("ROLLING_BASELINE", "ROLLING_BASELINE"),
        ("ROLLING_CONSUMPTION", "ROLLING_CONSUMPTION"),
    ),
    "heartbeat": (("HEARTBEAT_EVIDENCE", "HEARTBEAT_EVIDENCE"),),
    "mutation": (
        ("MUTATION_INTEGRITY", "MUTATION_INTEGRITY"),
        ("MUTATION_ACTION", "MUTATION_ACTION"),
        ("MUTATION_ACTION", "MUTATION_ACTION"),
    ),
    "capability": (("CAPABILITY_REVOKE", "CAPABILITY_REVOKE"),),
    "quote": (("QUOTE_RECEIPT", "QUOTE_RECEIPT"),),
    "graph": (("GRAPH_ACTIVATION", "GRAPH_ACTIVATION"),),
    "truth": (
        ("PREACTIVATION_BASELINE", "TRUTH_PREACTIVATION_BASELINE"),
        ("TERMINAL_TRUTH", "TRUTH_TERMINAL"),
    ),
}
COMPONENT_PURPOSES = {
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
NOW = datetime(2026, 8, 14, 0, 0, tzinfo=timezone.utc)


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _completeness(records):
    cardinality = Counter(item["recordType"] for item in records)
    previous = "0" * 64
    for ordinal, item in enumerate(records, 1):
        previous = _hash(
            {
                "ordinal": ordinal,
                "recordType": item["recordType"],
                "primaryKey": item["primaryKey"],
                "revision": item["revision"],
                "recordHash": item["recordHash"],
                "previousHash": previous,
            }
        )
    return {
        "recordHighWaterRevision": max(item["revision"] for item in records),
        "recordHeadHash": previous,
        "recordCardinality": {
            key: cardinality[key] for key in sorted(cardinality)
        },
        "primaryKeyCardinality": len(
            {item["primaryKey"] for item in records}
        ),
    }


class _Authority:
    def __init__(self, component: str) -> None:
        self.key = ECC.generate(curve="Ed25519")
        self.public_pem = self.key.public_key().export_key(format="PEM")
        self.key_id_hash = hashlib.sha256(self.public_pem.encode()).hexdigest()

    def sign(self, domain: str, body) -> str:
        return base64.b64encode(
            eddsa.new(self.key, mode="rfc8032").sign(
                domain.encode("ascii") + b"\x00" + _canonical(body)
            )
        ).decode()


class _Clock:
    def __call__(self):
        return NOW


def _time(value):
    return value.isoformat().replace("+00:00", "Z")


def _registry(
    authorities,
    *,
    account_fingerprint=ACCOUNT,
    include_purpose_authorities=False,
):
    root = ECC.generate(curve="Ed25519")
    root_public = root.public_key().export_key(format="PEM")
    root_key_id_hash = hashlib.sha256(root_public.encode()).hexdigest()
    purpose_authorities = {
        COMPONENT_PURPOSES[component]: authority
        for component, authority in authorities.items()
    }
    for purpose in KEY_PURPOSES:
        purpose_authorities.setdefault(purpose, _Authority(purpose))
    keys = []
    for purpose in KEY_PURPOSES:
        authority = purpose_authorities[purpose]
        keys.append(
            {
                "keyId": f"reader-{purpose.lower().replace('_', '-')}-v1",
                "keyIdHash": authority.key_id_hash,
                "purpose": purpose,
                "algorithm": "ED25519",
                "rotationEpoch": 1,
                "notBefore": _time(NOW - timedelta(minutes=1)),
                "notAfter": _time(NOW + timedelta(hours=2)),
                "accountFingerprint": account_fingerprint,
                "credentialConfigurationHash": hashlib.sha256(
                    b"reader-credential"
                ).hexdigest(),
                "codeManifestHash": hashlib.sha256(b"reader-code").hexdigest(),
                "publicKeyPem": authority.public_pem,
            }
        )
    credential_hash = keys[0]["credentialConfigurationHash"]
    code_hash = keys[0]["codeManifestHash"]
    manifest = {
        "schemaVersion": REGISTRY_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "registryId": "reader-test-registry-v1",
        "registryEpoch": 1,
        "notBefore": _time(NOW - timedelta(minutes=2)),
        "notAfter": _time(NOW + timedelta(hours=3)),
        "accountFingerprint": account_fingerprint,
        "credentialConfigurationHash": credential_hash,
        "codeManifestHash": code_hash,
        "previousManifestHash": None,
        "keys": keys,
        "revocations": [],
        "componentBindings": [
            {
                "schemaVersion": (
                    "kis-domestic-functional-key-registry-component-binding/v1"
                ),
                "route": ROUTE,
                "pdno": PDNO,
                "component": "readers",
                "sourceFileHash": hashlib.sha256(
                    (
                        Path(__file__).resolve().parents[1]
                        / "live_trader"
                        / "kis_domestic_functional_readers.py"
                    ).read_bytes()
                ).hexdigest(),
                "protocolHash": READERS_COMPONENT_PROTOCOL_HASH,
                "schemaFingerprint": READERS_COMPONENT_SCHEMA_FINGERPRINT,
                "statusHash": readers_component_status()["statusHash"],
                "authorityKeyIdHash": purpose_authorities[
                    "READERS_COMPONENT_VERIFY"
                ].key_id_hash,
                "authorityPurpose": "READERS_COMPONENT_VERIFY",
                "signatureDomain": (
                    "KIS_DOMESTIC_FUNCTIONAL_READERS_COMPONENT"
                ),
            }
        ],
    }
    manifest_hash = _hash(manifest)
    signed = {**manifest, "manifestHash": manifest_hash}
    signature = base64.b64encode(
        eddsa.new(root, mode="rfc8032").sign(
            b"KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY\0" + _canonical(signed)
        )
    ).decode()
    fd, name = tempfile.mkstemp(prefix="kis-readers-registry-", suffix=".json")
    os.close(fd)
    path = Path(name)
    path.write_bytes(
        _canonical(
            {
                "manifest": manifest,
                "manifestHash": manifest_hash,
                "rootKeyIdHash": root_key_id_hash,
                "rootSignature": signature,
            }
        )
    )
    registry = VerifyOnlyKeyRegistry(
        path,
        pinned_root_public_key_pem=root_public,
        pinned_root_key_id_hash=root_key_id_hash,
        expected_account_fingerprint=account_fingerprint,
        expected_credential_configuration_hash=credential_hash,
        expected_code_manifest_hash=code_hash,
        trusted_clock=_Clock(),
    )
    component_ids = {
        component: authorities[component].key_id_hash
        for component in COMPONENT_TYPES
    }
    component_ids.update(
        {
            "market_source": purpose_authorities[
                "MARKET_SOURCE_RECORD_VERIFY"
            ].key_id_hash,
            "market_archive": purpose_authorities[
                "MARKET_ARCHIVE_CAPTURE_VERIFY"
            ].key_id_hash,
            "owner": purpose_authorities["OWNER_STATE_VERIFY"].key_id_hash,
        }
    )
    if include_purpose_authorities:
        return registry, path, component_ids, purpose_authorities
    return registry, path, component_ids


class _Fixture:
    def __init__(self) -> None:
        self.authorities = {
            component: _Authority(component) for component in COMPONENT_TYPES
        }
        (
            self.registry,
            self.registry_path,
            component_ids,
            self.purpose_authorities,
        ) = _registry(
            self.authorities,
            include_purpose_authorities=True,
        )
        self.component_ids = component_ids
        self.reader = KisDomesticFunctionalVerifyOnlyReaders(
            key_registry=self.registry,
            expected_registry_manifest_hash=self.registry.manifest_hash,
            expected_registry_root_key_id_hash=self.registry.root_key_id_hash,
            expected_registry_epoch=self.registry.registry_epoch,
            expected_component_key_id_hashes=component_ids,
        )

    def __del__(self):
        try:
            self.registry_path.unlink(missing_ok=True)
        except (AttributeError, OSError):
            pass

    def _body(self, component: str, record_type: str, ordinal: int) -> dict:
        body = {
            "schemaVersion": f"test-{component}-{record_type.lower()}/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": SESSION_ID,
            "accountFingerprint": ACCOUNT,
            "preactivationBaselineHash": BASELINE,
            "recordOrdinal": ordinal,
        }
        if component == "rolling" and record_type == "ROLLING_BASELINE":
            body["normalized"] = {
                "accountWideOrderRowsByKey": {
                    "20260814|001|0001": {"state": "FILLED"}
                }
            }
        if component == "mutation" and record_type == "MUTATION_INTEGRITY":
            body["baselineOrderKeys"] = ["20260814|001|0001"]
        if component == "capability" and record_type == "CAPABILITY_REVOKE":
            body.update(
                {
                    "externallyRevoked": True,
                    "runtimeReaderConfirmedClear": True,
                    "globalReaderConfirmedClear": True,
                }
            )
        return body

    def _record(
        self, component: str, record_type: str, domain: str, ordinal: int
    ) -> dict:
        authority = self.authorities[component]
        body = self._body(component, record_type, ordinal)
        record_hash = _hash(body)
        signature = authority.sign(domain, {**body, "recordHash": record_hash})
        primary_key = f"{component}-{record_type.lower()}-{ordinal}"
        stored = {
            "primary_key": primary_key,
            "revision": 1,
            "record_hash": record_hash,
            "signature": signature,
            "authority_key_id_hash": authority.key_id_hash,
            "route": ROUTE,
            "session_id": SESSION_ID,
        }
        return {
            "recordType": record_type,
            "signatureDomain": domain,
            "primaryKey": primary_key,
            "revision": 1,
            "body": body,
            "recordHash": record_hash,
            "signature": signature,
            "authorityKeyIdHash": authority.key_id_hash,
            "storedColumns": stored,
            "projectionRules": {
                "route": "/route",
                "session_id": "/sessionId",
            },
        }

    def component(self, component: str) -> dict:
        authority = self.authorities[component]
        records = [
            self._record(component, record_type, domain, ordinal)
            for ordinal, (record_type, domain) in enumerate(
                COMPONENT_TYPES[component], 1
            )
        ]
        schema_fingerprint = hashlib.sha256(
            f"schema:{component}".encode()
        ).hexdigest()
        component_status = (
            market_source_component_status()
            if component == "market_source"
            else {
                "schemaVersion": f"test-{component}-status/v1",
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
        completeness = _completeness(records)
        provenance = {
            "schemaVersion": READ_PROVENANCE_SCHEMA,
            "sourceKind": (
                "SIGNED_TRANSPORT_ARCHIVE_READ"
                if component == "truth"
                else "SQLITE_IMMUTABLE_READ"
            ),
            "databaseIdentityHash": hashlib.sha256(
                f"database:{component}".encode()
            ).hexdigest(),
            "schemaFingerprint": schema_fingerprint,
            "queryHash": hashlib.sha256(f"query:{component}".encode()).hexdigest(),
            "sqliteOpenMode": (
                "NOT_APPLICABLE" if component == "truth" else "mode=ro&immutable=1"
            ),
            "transactionMode": "READ_ONLY_SNAPSHOT",
            "pragmaQueryOnly": True,
            "immutableUri": True,
            "rowCount": len(records),
            "writesAttempted": 0,
            "networkAccessed": False,
            "readSetHash": _hash([item["recordHash"] for item in records]),
            **completeness,
        }
        unsigned = {
            "schemaVersion": COMPONENT_ENVELOPE_SCHEMA,
            "component": component,
            "route": ROUTE,
            "pdno": PDNO,
            "sourceFileHash": PINNED_COMPONENT_FILE_HASHES[component],
            "componentSchemaVersion": COMPONENT_SCHEMA_VERSIONS[component],
            "componentSchemaFingerprint": schema_fingerprint,
            "componentStatusHash": _hash(component_status),
            "componentStatus": component_status,
            "componentProtocolHash": COMPONENT_PROTOCOL_HASHES[component],
            "authorityKeyIdHash": authority.key_id_hash,
            "readProvenance": provenance,
            "records": records,
        }
        envelope_hash = _hash(unsigned)
        domain = f"KIS_DOMESTIC_FUNCTIONAL_FROZEN_READER:{component.upper()}"
        return {
            **unsigned,
            "envelopeHash": envelope_hash,
            "signature": authority.sign(
                domain, {**unsigned, "envelopeHash": envelope_hash}
            ),
        }

    def request(self) -> dict:
        return {
            "schemaVersion": READER_INPUT_SCHEMA,
            "sessionId": SESSION_ID,
            "accountFingerprint": ACCOUNT,
            "preactivationBaselineHash": BASELINE,
            "components": {
                component: self.component(component)
                for component in COMPONENT_TYPES
            },
        }

    def resign_record_and_envelope(
        self, request: dict, component: str, record_index: int
    ) -> None:
        envelope = request["components"][component]
        record = envelope["records"][record_index]
        authority = self.authorities[component]
        record["recordHash"] = _hash(record["body"])
        record["signature"] = authority.sign(
            record["signatureDomain"],
            {**record["body"], "recordHash": record["recordHash"]},
        )
        record["storedColumns"]["record_hash"] = record["recordHash"]
        record["storedColumns"]["signature"] = record["signature"]
        envelope["readProvenance"]["readSetHash"] = _hash(
            [item["recordHash"] for item in envelope["records"]]
        )
        envelope["readProvenance"].update(
            _completeness(envelope["records"])
        )
        unsigned = {
            key: value
            for key, value in envelope.items()
            if key not in {"envelopeHash", "signature"}
        }
        envelope["envelopeHash"] = _hash(unsigned)
        envelope["signature"] = authority.sign(
            f"KIS_DOMESTIC_FUNCTIONAL_FROZEN_READER:{component.upper()}",
            {**unsigned, "envelopeHash": envelope["envelopeHash"]},
        )

    @staticmethod
    def _database_identity(component, schema_fingerprint, status_hash, query_hash, records):
        completeness = _completeness(records)
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
                        "recordType": item["recordType"],
                        "primaryKey": item["primaryKey"],
                        "revision": item["revision"],
                        "recordHash": item["recordHash"],
                    }
                    for ordinal, item in enumerate(records, 1)
                ],
            }
        )

    def archive_envelope(self, component, *, truth=False):
        envelope = self.component(component)
        schema_fingerprint = (
            TRUTH_ARCHIVE_SCHEMA_FINGERPRINT
            if truth
            else SQLITE_ARCHIVE_SCHEMA_FINGERPRINT
        )
        query_hash = TRUTH_ARCHIVE_QUERY_HASH if truth else SQLITE_ARCHIVE_QUERY_HASH
        status_hash = _hash(envelope["componentStatus"])
        identity = self._database_identity(
            component,
            schema_fingerprint,
            status_hash,
            query_hash,
            envelope["records"],
        )
        envelope["componentSchemaFingerprint"] = schema_fingerprint
        envelope["readProvenance"].update(
            {
                "databaseIdentityHash": identity,
                "schemaFingerprint": schema_fingerprint,
                "queryHash": query_hash,
                "sourceKind": (
                    "SIGNED_TRANSPORT_ARCHIVE_READ"
                    if truth
                    else "SQLITE_IMMUTABLE_READ"
                ),
                "sqliteOpenMode": (
                    "NOT_APPLICABLE" if truth else "mode=ro&immutable=1"
                ),
            }
        )
        unsigned = {
            key: value
            for key, value in envelope.items()
            if key not in {"envelopeHash", "signature"}
        }
        envelope["envelopeHash"] = _hash(unsigned)
        envelope["signature"] = self.authorities[component].sign(
            f"KIS_DOMESTIC_FUNCTIONAL_FROZEN_READER:{component.upper()}",
            {**unsigned, "envelopeHash": envelope["envelopeHash"]},
        )
        return envelope, identity

    def write_sqlite_archive(self, path: Path, component: str):
        envelope, identity = self.archive_envelope(component)
        completeness = _completeness(envelope["records"])
        conn = sqlite3.connect(path)
        try:
            for statement in SQLITE_ARCHIVE_SCHEMA_SQL:
                conn.execute(statement)
            previous = "0" * 64
            for ordinal, record in enumerate(envelope["records"], 1):
                conn.execute(
                    "INSERT INTO kis_reader_archive_record VALUES(?,?,?,?,?,?,?)",
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
                previous = _completeness(envelope["records"][:ordinal])[
                    "recordHeadHash"
                ]
            conn.execute(
                "INSERT INTO kis_reader_archive_meta VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    1,
                    SQLITE_ARCHIVE_SCHEMA,
                    component,
                    COMPONENT_SCHEMA_VERSIONS[component],
                    SQLITE_ARCHIVE_SCHEMA_FINGERPRINT,
                    PINNED_COMPONENT_FILE_HASHES[component],
                    _canonical(envelope["componentStatus"]).decode(),
                    envelope["componentStatusHash"],
                    SQLITE_ARCHIVE_QUERY_HASH,
                    len(envelope["records"]),
                    completeness["recordHighWaterRevision"],
                    completeness["recordHeadHash"],
                    _canonical(completeness["recordCardinality"]).decode(),
                    identity,
                    _canonical(envelope).decode(),
                    envelope["envelopeHash"],
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return envelope

    def write_truth_archive(self, path: Path):
        envelope, identity = self.archive_envelope("truth", truth=True)
        completeness = _completeness(envelope["records"])
        archive = {
            "schemaVersion": TRUTH_ARCHIVE_SCHEMA,
            "component": "truth",
            "sourceSchemaVersion": COMPONENT_SCHEMA_VERSIONS["truth"],
            "sourceSchemaFingerprint": TRUTH_ARCHIVE_SCHEMA_FINGERPRINT,
            "sourceFileHash": PINNED_COMPONENT_FILE_HASHES["truth"],
            "componentStatus": envelope["componentStatus"],
            "componentStatusHash": envelope["componentStatusHash"],
            "queryHash": TRUTH_ARCHIVE_QUERY_HASH,
            "recordCount": len(envelope["records"]),
            "highWaterRevision": completeness["recordHighWaterRevision"],
            "recordHeadHash": completeness["recordHeadHash"],
            "cardinality": completeness["recordCardinality"],
            "databaseIdentityHash": identity,
            "envelope": envelope,
        }
        path.write_bytes(_canonical(archive))
        return envelope


class KisDomesticFunctionalReadersTest(unittest.TestCase):
    def test_happy_exact_raw_bundle_is_retained_and_remains_disabled(self) -> None:
        fixture = _Fixture()
        result = fixture.reader.read(fixture.request())
        self.assertFalse(result["allRequiredRawRecordsJoined"])
        self.assertTrue(result["mutationBaselineOrderKeysJoined"])
        self.assertEqual(set(COMPONENT_TYPES), set(result["canonicalRawRecords"]))
        self.assertIn(
            "VERIFY_ONLY_PRODUCTION_AUTHORITY_NOT_WIRED",
            result["readinessBlockers"],
        )
        self.assertIn(
            "MARKET_SOURCE_SPECIALIZED_ARCHIVE_NOT_FETCHED",
            result["readinessBlockers"],
        )
        self.assertIn(
            "KEY_REGISTRY_DURABLE_ANTI_ROLLBACK_NOT_WIRED",
            result["readinessBlockers"],
        )
        self.assertIn(
            "PRODUCTION_KEY_REGISTRY_FACTORY_PINS_NOT_WIRED",
            result["readinessBlockers"],
        )
        self.assertTrue(result["keyRegistryAsymmetricRootVerified"])
        self.assertFalse(result["productionAuthorityPinned"])
        self.assertEqual(
            fixture.registry.manifest_hash,
            result["keyRegistryManifestHash"],
        )
        self.assertEqual(
            fixture.registry.root_key_id_hash,
            result["keyRegistryRootKeyIdHash"],
        )
        self.assertEqual(1, result["keyRegistryEpoch"])
        self.assertIn(
            "OPERATOR_EXCLUSIVITY_UNPROVEN", result["readinessBlockers"]
        )
        self.assertFalse(result["productionAvailable"])
        self.assertFalse(result["networkOrderPostAllowed"])
        self.assertEqual(0, result["tradingMutationCount"])
        self.assertRegex(result["readerBundleHash"], r"^[0-9a-f]{64}$")

    def test_generic_market_source_summary_is_replaced_by_specialized_archive_hold(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        self.assertNotIn("market_source", request["components"])
        self.assertEqual(
            hashlib.sha256(
                (
                    Path(__file__).resolve().parents[1]
                    / "live_trader"
                    / "kis_domestic_functional_market_source.py"
                ).read_bytes()
            ).hexdigest(),
            PINNED_COMPONENT_FILE_HASHES["market_source"],
        )
        result = fixture.reader.read(request)
        self.assertIsNone(result["marketSourceArchiveEvidence"])
        self.assertIn(
            "MARKET_SOURCE_SPECIALIZED_ARCHIVE_NOT_FETCHED",
            result["readinessBlockers"],
        )
        self.assertIn(
            "MARKET_SOURCE_POST_OBSERVATION_PREFIX_EXTENSION_NOT_JOINED",
            result["readinessBlockers"],
        )
        self.assertFalse(result["allRequiredRawRecordsJoined"])

    def test_specialized_archive_has_no_summary_or_hmac_authority_fallback(self) -> None:
        from tests.test_kis_domestic_functional_market_archive import (
            ARM_ID,
            GENERATION_ONE,
            MarketArchiveHarness,
        )

        fixture = _Fixture()
        archive = MarketArchiveHarness()
        try:
            built = archive.build()
            reader = ImmutableMarketSourceArchiveReader(
                archive.destination,
                expected_file_hash=built["archiveFileHash"],
                source_generation=GENERATION_ONE,
                arm_id=ARM_ID,
                key_registry=fixture.registry,
                expected_registry_manifest_hash=fixture.registry.manifest_hash,
                expected_registry_epoch=fixture.registry.registry_epoch,
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalReadersBlocked,
                "market-archive-independent-verification-failed",
            ):
                reader.read()
            status = readers_component_status()
            self.assertFalse(
                status["marketArchiveComponentStatus"][
                    "externalAsymmetricArchiveAuthorityPinned"
                ]
            )
            self.assertFalse(
                status["marketArchiveComponentStatus"][
                    "releaseCompletenessProven"
                ]
            )
        finally:
            archive.close()

    def test_specialized_archive_actual_aligned_registry_e2e_positive(self) -> None:
        from live_trader.kis_domestic_functional_market_archive import (
            FENCE_SCHEMA,
            build_market_source_archive,
        )
        from live_trader.kis_domestic_functional_market_source import (
            DisabledKisDomesticFunctionalMarketSource,
        )
        from live_trader.kis_domestic_functional_source import (
            DurableKisDomesticPublicArmJournal,
            KisDomesticFunctionalMarketSourceDurableWriter,
        )
        from tests.test_kis_domestic_functional_market_archive import (
            ARM_ID,
            OWNER_TOKEN_HASH,
        )
        from tests.test_kis_domestic_functional_market_source import (
            ACCOUNT as MARKET_ACCOUNT,
            GENERATION_ONE,
            NOW as MARKET_NOW,
            OWNER_EPOCH_HASH,
            OWNER_EPOCH_ID,
            PROCESS_GENERATION,
            SESSION,
            SOCKET_ONE,
            MarketSourceHarness,
        )

        fixture = _Fixture()
        market = MarketSourceHarness()
        source_authority = fixture.purpose_authorities["SOURCE_RECORD_VERIFY"]
        market_authority = fixture.purpose_authorities[
            "MARKET_SOURCE_RECORD_VERIFY"
        ]
        owner_authority = fixture.purpose_authorities["OWNER_STATE_VERIFY"]
        archive_authority = fixture.purpose_authorities[
            "MARKET_ARCHIVE_CAPTURE_VERIFY"
        ]

        def candidate_sign(authority, domain, body, hash_key):
            value = {**dict(body), hash_key: _hash(body)}
            return {**value, "signature": authority.sign(domain, value)}

        def candidate_verifier(purpose, domain):
            expected = fixture.registry.active_key_id_for(purpose)

            def verify(candidate):
                try:
                    value = copy.deepcopy(dict(candidate))
                    signature = value.pop("signature")
                    return bool(
                        value.get("authorityKeyIdHash", expected) == expected
                        and fixture.registry.verify(
                            purpose=purpose,
                            domain=domain,
                            body=value,
                            signature=signature,
                            key_id_hash=expected,
                        ) is True
                    )
                except BaseException:
                    return False

            return verify

        def domain_verifier(purpose):
            expected = fixture.registry.active_key_id_for(purpose)

            def verify(domain, body, signature, key_id_hash=None):
                try:
                    return bool(
                        (key_id_hash or body.get("authorityKeyIdHash", expected))
                        == expected
                        and fixture.registry.verify(
                            purpose=purpose,
                            domain=domain,
                            body=copy.deepcopy(dict(body)),
                            signature=signature,
                            key_id_hash=expected,
                        ) is True
                    )
                except BaseException:
                    return False

            return verify

        owner_verify = candidate_verifier(
            "OWNER_STATE_VERIFY",
            "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_OWNER_EPOCH",
        )
        handshake_verify = candidate_verifier(
            "MARKET_SOURCE_RECORD_VERIFY",
            "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_HANDSHAKE",
        )
        raw_verify = candidate_verifier(
            "MARKET_SOURCE_RECORD_VERIFY",
            "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_RAW_FRAME",
        )
        ack_verify = candidate_verifier(
            "SOURCE_RECORD_VERIFY", "MARKET_SOURCE_DURABLE_ACK"
        )
        reducer_verify = candidate_verifier(
            "SOURCE_RECORD_VERIFY",
            "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_REDUCER_RECEIPT",
        )
        transition_verify = domain_verifier("MARKET_SOURCE_RECORD_VERIFY")
        source_verify = domain_verifier("SOURCE_RECORD_VERIFY")
        archive_verify = domain_verifier("MARKET_ARCHIVE_CAPTURE_VERIFY")
        owner_fence_verify = candidate_verifier(
            "OWNER_STATE_VERIFY",
            "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_FENCE",
        )

        try:
            journal = DurableKisDomesticPublicArmJournal(
                Path(market.temp.name) / "registry-public-source.sqlite3",
                capture_signer=source_authority.sign,
                server_authority_public_key_pem=source_authority.public_pem,
            )
            arm_body = {
                "schemaVersion": "kis-domestic-functional-public-arm/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "state": "ARMED_WAIT_PUBLIC",
                "armId": ARM_ID,
                "source": "KIS_WEBSOCKET_H0STCNT0",
                "sourceProvider": "kis",
                "sourceGeneration": GENERATION_ONE,
                "socketIdentityHash": SOCKET_ONE,
                "connectedAt": _time(MARKET_NOW - timedelta(seconds=1)),
                "createdAt": _time(MARKET_NOW - timedelta(seconds=1)),
                "serverAuthorityKeyIdHash": source_authority.key_id_hash,
                "publicMarketDataOnly": True,
                "accountAuthorityAvailable": False,
                "tokenAuthorityAvailable": False,
                "mutationAuthorityAvailable": False,
                "networkAvailable": False,
                "productionAvailable": False,
                "marketSourceSessionId": SESSION,
                "marketSourceAccountFingerprint": MARKET_ACCOUNT,
                "marketSourceOwnerEpoch": 7,
                "marketSourceOwnerEpochId": OWNER_EPOCH_ID,
                "marketSourceOwnerEpochHash": OWNER_EPOCH_HASH,
                "marketSourceProcessGeneration": PROCESS_GENERATION,
                "marketSourceAuthorityKeyIdHash": market_authority.key_id_hash,
            }
            journal.begin_arm(
                arm_record=arm_body,
                arm_signature=source_authority.sign("PUBLIC_ARM", arm_body),
                owner_token_hash=OWNER_TOKEN_HASH,
            )
            writer = KisDomesticFunctionalMarketSourceDurableWriter(
                journal=journal,
                arm_id=ARM_ID,
                owner_token_hash=OWNER_TOKEN_HASH,
                market_record_verifier=raw_verify,
                trusted_clock=lambda: market.now,
            )

            def owner_reader():
                value = market.owner()
                body = dict(value)
                body.pop("signature")
                body.pop("snapshotHash")
                return candidate_sign(
                    owner_authority,
                    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_OWNER_EPOCH",
                    body,
                    "snapshotHash",
                )

            def market_handshake():
                value = market.handshake()
                body = dict(value)
                body.pop("signature")
                body.pop("handshakeHash")
                body["authorityKeyIdHash"] = market_authority.key_id_hash
                return candidate_sign(
                    market_authority,
                    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_HANDSHAKE",
                    body,
                    "handshakeHash",
                )

            def market_raw(ordinal, previous):
                value = market.raw(ordinal=ordinal, previous=previous)
                body = dict(value)
                body.pop("signature")
                body.pop("recordHash")
                body["authorityKeyIdHash"] = market_authority.key_id_hash
                return candidate_sign(
                    market_authority,
                    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_RAW_FRAME",
                    body,
                    "recordHash",
                )

            def reducer(raw, record, ack):
                value = market.reducer(raw, record, ack)
                body = dict(value)
                body.pop("signature")
                body.pop("receiptHash")
                body["authorityKeyIdHash"] = source_authority.key_id_hash
                return candidate_sign(
                    source_authority,
                    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_REDUCER_RECEIPT",
                    body,
                    "receiptHash",
                )

            market.source = DisabledKisDomesticFunctionalMarketSource(
                program_ledger=market.ledger,
                owner_epoch_reader=owner_reader,
                owner_epoch_verifier=owner_verify,
                handshake_verifier=handshake_verify,
                raw_record_verifier=raw_verify,
                durable_ingress_writer=writer,
                durable_ack_verifier=ack_verify,
                reducer=reducer,
                reducer_receipt_verifier=reducer_verify,
                transition_signer=market_authority.sign,
                transition_verifier=transition_verify,
                transition_authority_key_id_hash=market_authority.key_id_hash,
                trusted_clock=lambda: market.now,
            )
            market.source.begin_generation(market_handshake())
            for index in range(12):
                market.now = MARKET_NOW + timedelta(minutes=5 * index)
                current = market.source.snapshot(GENERATION_ONE)
                market.source.ingest_signed_frame(
                    market_raw(index + 1, current["ingressHeadHash"])
                )

            @contextmanager
            def fence():
                body = {
                    "schemaVersion": FENCE_SCHEMA,
                    "route": ROUTE,
                    "sourceGeneration": GENERATION_ONE,
                    "armId": ARM_ID,
                    "ownerEpochId": OWNER_EPOCH_ID,
                    "ownerEpochHash": OWNER_EPOCH_HASH,
                    "routeFenceRevision": 9,
                    "observedAt": market.now.isoformat(),
                    "routeLockHeld": True,
                    "accountAuthorityAvailable": False,
                    "mutationAuthorityAvailable": False,
                    "productionAvailable": False,
                }
                yield candidate_sign(
                    owner_authority,
                    "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_FENCE",
                    body,
                    "fenceHash",
                )

            destination = Path(market.temp.name) / "registry-market-archive.sqlite3"
            built = build_market_source_archive(
                market_database=market.ledger.path,
                source_database=journal.path,
                destination=destination,
                source_generation=GENERATION_ONE,
                arm_id=ARM_ID,
                observation_fence=fence,
                fence_verifier=owner_fence_verify,
                market_verifiers={
                    "handshake": handshake_verify,
                    "raw": raw_verify,
                    "ack": ack_verify,
                    "reducer": reducer_verify,
                },
                transition_verifier=transition_verify,
                source_verifier=source_verify,
                archive_capture_signer=archive_authority.sign,
                archive_capture_verifier=archive_verify,
                archive_authority_key_id_hash=archive_authority.key_id_hash,
                trusted_clock=lambda: market.now,
            )
            reader = ImmutableMarketSourceArchiveReader(
                destination,
                expected_file_hash=built["archiveFileHash"],
                source_generation=GENERATION_ONE,
                arm_id=ARM_ID,
                key_registry=fixture.registry,
                expected_registry_manifest_hash=fixture.registry.manifest_hash,
                expected_registry_epoch=fixture.registry.registry_epoch,
            )
            evidence = reader.read()
            self.assertTrue(evidence["allProducerRecordsIndependentlyReplayed"])
            self.assertTrue(evidence["atomicRouteOwnerObservationFenceHeld"])
            self.assertEqual(
                market_authority.key_id_hash,
                evidence["marketSourceAuthorityKeyIdHash"],
            )
            self.assertEqual(
                source_authority.key_id_hash,
                evidence["sourceAuthorityKeyIdHash"],
            )
            self.assertEqual(
                archive_authority.key_id_hash,
                evidence["archiveAuthorityKeyIdHash"],
            )
            self.assertFalse(evidence["externalAsymmetricArchiveAuthorityPinned"])
            self.assertFalse(evidence["releaseCompletenessProven"])
            self.assertFalse(evidence["productionAvailable"])
            self.assertFalse(evidence["networkAvailable"])
            self.assertFalse(evidence["mutationAvailable"])
            self.assertFalse(evidence["releaseAvailable"])
        finally:
            market.close()

    def test_missing_component_and_record_type_are_explicit_blockers(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        request["components"].pop("quote")
        request["components"]["lane"]["records"] = request["components"]["lane"]["records"][:-1]
        lane = request["components"]["lane"]
        lane["readProvenance"]["rowCount"] -= 1
        lane["readProvenance"]["readSetHash"] = _hash(
            [item["recordHash"] for item in lane["records"]]
        )
        lane["readProvenance"].update(_completeness(lane["records"]))
        unsigned = {k: v for k, v in lane.items() if k not in {"envelopeHash", "signature"}}
        lane["envelopeHash"] = _hash(unsigned)
        lane["signature"] = fixture.authorities["lane"].sign(
            "KIS_DOMESTIC_FUNCTIONAL_FROZEN_READER:LANE",
            {**unsigned, "envelopeHash": lane["envelopeHash"]},
        )
        result = fixture.reader.read(request)
        self.assertIn("COMPONENT_RECORDS_MISSING:quote", result["readinessBlockers"])
        self.assertIn(
            "COMPONENT_RECORD_CARDINALITY_MISMATCH:lane:ACTION:1:2",
            result["readinessBlockers"],
        )
        self.assertFalse(result["allRequiredRawRecordsJoined"])

    def test_archive_adapter_without_specialized_market_archive_fails_closed(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sqlite_archives = {}
            for component in set(COMPONENT_TYPES) - {"truth"}:
                path = root / f"{component}.sqlite3"
                fixture.write_sqlite_archive(path, component)
                sqlite_archives[component] = ImmutableSqliteComponentArchiveReader(
                    path, component=component
                )
            truth_path = root / "truth.json"
            fixture.write_truth_archive(truth_path)
            result = fixture.reader.read_from_archives(
                session_id=SESSION_ID,
                account_fingerprint=ACCOUNT,
                preactivation_baseline_hash=BASELINE,
                sqlite_archives=sqlite_archives,
                truth_archive=ImmutableTruthArchiveReader(truth_path),
            )
        self.assertIsNone(result["marketSourceArchiveEvidence"])
        self.assertNotIn("market_source", result["immutableArchiveEvidence"])
        self.assertIn(
            "MARKET_SOURCE_SPECIALIZED_ARCHIVE_NOT_FETCHED",
            result["readinessBlockers"],
        )
        self.assertFalse(result["allExactJoinsPassed"])

    def test_forged_record_signature_is_rejected(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        request["components"]["source"]["records"][0]["signature"] = "0" * 64
        request["components"]["source"]["records"][0]["storedColumns"]["signature"] = "0" * 64
        with self.assertRaisesRegex(KisDomesticFunctionalReadersBlocked, "signature-mismatch"):
            fixture.reader.read(request)

    def test_self_consistent_body_rehash_without_authority_is_rejected(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        record = request["components"]["truth"]["records"][0]
        record["body"]["accountFingerprint"] = "f" * 64
        record["recordHash"] = _hash(record["body"])
        record["storedColumns"]["record_hash"] = record["recordHash"]
        with self.assertRaisesRegex(KisDomesticFunctionalReadersBlocked, "signature-mismatch"):
            fixture.reader.read(request)

    def test_row_projection_mismatch_is_rejected(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        request["components"]["graph"]["records"][0]["storedColumns"]["session_id"] = "other-session"
        with self.assertRaisesRegex(KisDomesticFunctionalReadersBlocked, "row-projection-mismatch"):
            fixture.reader.read(request)

    def test_read_provenance_write_network_and_readset_are_rejected(self) -> None:
        for field, value, message in (
            ("writesAttempted", 1, "provenance-contract-invalid"),
            ("networkAccessed", True, "provenance-contract-invalid"),
            ("readSetHash", "0" * 64, "provenance-read-set-mismatch"),
        ):
            with self.subTest(field=field):
                fixture = _Fixture()
                request = fixture.request()
                request["components"]["heartbeat"]["readProvenance"][field] = value
                with self.assertRaisesRegex(KisDomesticFunctionalReadersBlocked, message):
                    fixture.reader.read(request)

    def test_component_file_protocol_and_schema_drift_fail_closed(self) -> None:
        cases = (
            ("sourceFileHash", "0" * 64, "component-file-drift"),
            ("componentProtocolHash", "0" * 64, "component-contract-invalid"),
            ("componentSchemaVersion", "dirty/v1", "component-contract-invalid"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                fixture = _Fixture()
                request = fixture.request()
                request["components"]["mutation"][field] = value
                with self.assertRaisesRegex(KisDomesticFunctionalReadersBlocked, message):
                    fixture.reader.read(request)

    def test_mutation_baseline_order_keys_must_equal_rolling_account_wide_keys(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        mutation = request["components"]["mutation"]
        mutation["records"][0]["body"]["baselineOrderKeys"] = ["different-key"]
        fixture.resign_record_and_envelope(request, "mutation", 0)
        result = fixture.reader.read(request)
        self.assertFalse(result["mutationBaselineOrderKeysJoined"])
        self.assertIn(
            "MUTATION_BASELINE_ORDER_KEYS_MISMATCH", result["readinessBlockers"]
        )

    def test_session_account_baseline_and_capability_joins_fail_closed(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        lane = request["components"]["lane"]
        lane["records"][0]["body"]["sessionId"] = "different-session"
        lane["records"][0]["storedColumns"]["session_id"] = "different-session"
        fixture.resign_record_and_envelope(request, "lane", 0)
        capability = request["components"]["capability"]
        capability["records"][0]["body"]["externallyRevoked"] = False
        fixture.resign_record_and_envelope(request, "capability", 0)
        result = fixture.reader.read(request)
        self.assertIn("SESSION_JOIN_MISMATCH:lane", result["readinessBlockers"])
        self.assertIn(
            "EXTERNAL_CAPABILITY_REVOKE_NOT_JOINED", result["readinessBlockers"]
        )

    def test_status_has_exact_hashes_and_no_authority_surface(self) -> None:
        status = readers_component_status()
        self.assertEqual(
            set(COMPONENT_TYPES) | {"market_source", "market_archive"},
            set(status["componentFileHashes"]),
        )
        self.assertEqual(set(COMPONENT_TYPES), set(status["componentProtocolHashes"]))
        self.assertEqual(
            "kis-domestic-functional-lane-schema/v2",
            COMPONENT_SCHEMA_VERSIONS["lane"],
        )
        self.assertFalse(
            status["marketArchiveComponentStatus"][
                "externalAsymmetricArchiveAuthorityPinned"
            ]
        )
        self.assertTrue(status["verifyOnly"])
        self.assertFalse(status["productionAuthorityPinned"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["mutationAvailable"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertEqual(0, status["tradingMutationCount"])
        self.assertRegex(status["statusHash"], r"^[0-9a-f]{64}$")

    def test_registry_type_manifest_root_epoch_and_component_key_pins_are_exact(self) -> None:
        fixture = _Fixture()
        common = {
            "key_registry": fixture.registry,
            "expected_registry_manifest_hash": fixture.registry.manifest_hash,
            "expected_registry_root_key_id_hash": fixture.registry.root_key_id_hash,
            "expected_registry_epoch": fixture.registry.registry_epoch,
            "expected_component_key_id_hashes": fixture.component_ids,
        }
        cases = (
            (
                {**common, "key_registry": object()},
                "reader-key-registry-type-invalid",
            ),
            (
                {**common, "expected_registry_manifest_hash": "0" * 64},
                "reader-key-registry-binding-invalid",
            ),
            (
                {**common, "expected_registry_root_key_id_hash": "0" * 64},
                "reader-key-registry-binding-invalid",
            ),
            (
                {**common, "expected_registry_epoch": 2},
                "reader-key-registry-binding-invalid",
            ),
            (
                {
                    **common,
                    "expected_component_key_id_hashes": {
                        **fixture.component_ids,
                        "source": fixture.component_ids["lane"],
                    },
                },
                "reader-key-registry-key-pin-mismatch:source",
            ),
        )
        for kwargs, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(
                KisDomesticFunctionalReadersBlocked, message
            ):
                KisDomesticFunctionalVerifyOnlyReaders(**kwargs)

    def test_registry_file_substitution_and_request_account_mismatch_fail_closed(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        request["accountFingerprint"] = "0" * 64
        with self.assertRaisesRegex(
            KisDomesticFunctionalReadersBlocked,
            "reader-key-registry-account-binding-mismatch",
        ):
            fixture.reader.read(request)

        fixture = _Fixture()
        fixture.registry_path.write_bytes(fixture.registry_path.read_bytes() + b"\n")
        with self.assertRaisesRegex(
            KisDomesticFunctionalReadersBlocked,
            "reader-key-registry-current-binding-invalid",
        ):
            fixture.reader.read(fixture.request())

    def test_actual_immutable_sqlite_and_truth_archives_join_exactly(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sqlite_archives = {}
            for component in set(COMPONENT_TYPES) - {"truth"}:
                path = root / f"{component}.sqlite3"
                fixture.write_sqlite_archive(path, component)
                sqlite_archives[component] = ImmutableSqliteComponentArchiveReader(
                    path, component=component
                )
            truth_path = root / "truth.json"
            fixture.write_truth_archive(truth_path)
            market_archive = object.__new__(ImmutableMarketSourceArchiveReader)
            market_archive.registry = fixture.registry
            market_archive.expected_manifest_hash = fixture.registry.manifest_hash
            market_archive.expected_registry_epoch = fixture.registry.registry_epoch
            market_evidence = {
                "schemaVersion": "kis-domestic-functional-market-archive-reader/v1",
                "postObservationPrefixExtensionProven": False,
                "externalAsymmetricArchiveAuthorityPinned": False,
                "releaseCompletenessProven": False,
                "productionAvailable": False,
            }
            with patch.object(
                ImmutableMarketSourceArchiveReader,
                "read",
                return_value=market_evidence,
            ):
                result = fixture.reader.read_from_archives(
                    session_id=SESSION_ID,
                    account_fingerprint=ACCOUNT,
                    preactivation_baseline_hash=BASELINE,
                    sqlite_archives=sqlite_archives,
                    truth_archive=ImmutableTruthArchiveReader(truth_path),
                    market_archive=market_archive,
                )
        self.assertTrue(result["allImmutableArchivesFetched"])
        self.assertTrue(result["independentTruthArchiveFetched"])
        self.assertFalse(result["allExactJoinsPassed"])
        self.assertNotIn(
            "IMMUTABLE_SQLITE_COMPONENT_ARCHIVES_NOT_FETCHED",
            result["readinessBlockers"],
        )
        self.assertNotIn(
            "INDEPENDENT_TRUTH_ARCHIVE_NOT_FETCHED",
            result["readinessBlockers"],
        )
        self.assertIn(
            "VERIFY_ONLY_PRODUCTION_AUTHORITY_NOT_WIRED",
            result["readinessBlockers"],
        )
        self.assertEqual(
            set(COMPONENT_TYPES) | {"market_source"},
            set(result["immutableArchiveEvidence"]),
        )
        self.assertEqual(market_evidence, result["marketSourceArchiveEvidence"])
        self.assertIn(
            "MARKET_SOURCE_POST_OBSERVATION_PREFIX_EXTENSION_NOT_JOINED",
            result["readinessBlockers"],
        )

    def test_sqlite_archive_recomputes_schema_query_status_and_completeness(self) -> None:
        cases = (
            (
                "UPDATE kis_reader_archive_meta SET query_hash=?",
                ("0" * 64,),
                "meta-contract-invalid",
            ),
            (
                "UPDATE kis_reader_archive_meta SET high_water_revision=99",
                (),
                "completeness-invalid",
            ),
            (
                "UPDATE kis_reader_archive_meta SET component_status_hash=?",
                ("0" * 64,),
                "meta-contract-invalid",
            ),
        )
        for index, (sql, params, message) in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as directory:
                fixture = _Fixture()
                path = Path(directory) / "lane.sqlite3"
                fixture.write_sqlite_archive(path, "lane")
                conn = sqlite3.connect(path)
                try:
                    conn.execute(sql, params)
                    conn.commit()
                finally:
                    conn.close()
                with self.assertRaisesRegex(
                    KisDomesticFunctionalReadersBlocked, message
                ):
                    ImmutableSqliteComponentArchiveReader(
                        path, component="lane"
                    ).read()
        with tempfile.TemporaryDirectory() as directory:
            fixture = _Fixture()
            path = Path(directory) / "lane.sqlite3"
            fixture.write_sqlite_archive(path, "lane")
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    "ALTER TABLE kis_reader_archive_record ADD COLUMN dirty TEXT"
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalReadersBlocked, "schema-dirty"
            ):
                ImmutableSqliteComponentArchiveReader(path, component="lane").read()

    def test_sqlite_archive_rejects_gap_head_and_row_projection_tamper(self) -> None:
        cases = (
            (
                "UPDATE kis_reader_archive_record SET previous_hash=? WHERE ordinal=2",
                ("f" * 64,),
            ),
            (
                "UPDATE kis_reader_archive_record SET revision=7 WHERE ordinal=1",
                (),
            ),
            (
                "DELETE FROM kis_reader_archive_record WHERE ordinal=2",
                (),
            ),
        )
        for index, (sql, params) in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as directory:
                fixture = _Fixture()
                path = Path(directory) / "mutation.sqlite3"
                fixture.write_sqlite_archive(path, "mutation")
                conn = sqlite3.connect(path)
                try:
                    conn.execute(sql, params)
                    conn.commit()
                finally:
                    conn.close()
                with self.assertRaises(KisDomesticFunctionalReadersBlocked):
                    ImmutableSqliteComponentArchiveReader(
                        path, component="mutation"
                    ).read()

    def test_independent_truth_archive_rejects_hidden_or_tampered_records(self) -> None:
        fixture = _Fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "truth.json"
            fixture.write_truth_archive(path)
            archive = json.loads(path.read_text(encoding="utf-8"))
            archive["envelope"]["records"].pop()
            path.write_bytes(_canonical(archive))
            with self.assertRaisesRegex(
                KisDomesticFunctionalReadersBlocked,
                "contract-or-completeness-invalid",
            ):
                ImmutableTruthArchiveReader(path).read()

    def test_required_join_field_absence_and_duplicate_identity_fail_closed(self) -> None:
        fixture = _Fixture()
        request = fixture.request()
        record = request["components"]["quote"]["records"][0]
        record["body"].pop("accountFingerprint")
        with self.assertRaisesRegex(
            KisDomesticFunctionalReadersBlocked,
            "required-join-fields-invalid",
        ):
            fixture.reader.read(request)

        fixture = _Fixture()
        request = fixture.request()
        lane = request["components"]["lane"]
        lane["records"].append(copy.deepcopy(lane["records"][0]))
        lane["readProvenance"]["rowCount"] += 1
        lane["readProvenance"]["readSetHash"] = _hash(
            [item["recordHash"] for item in lane["records"]]
        )
        lane["readProvenance"].update(_completeness(lane["records"]))
        unsigned = {
            key: value
            for key, value in lane.items()
            if key not in {"envelopeHash", "signature"}
        }
        lane["envelopeHash"] = _hash(unsigned)
        lane["signature"] = fixture.authorities["lane"].sign(
            "KIS_DOMESTIC_FUNCTIONAL_FROZEN_READER:LANE",
            {**unsigned, "envelopeHash": lane["envelopeHash"]},
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalReadersBlocked, "record-duplicate"
        ):
            fixture.reader.read(request)


if __name__ == "__main__":
    unittest.main()
