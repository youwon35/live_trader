from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import time
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from live_trader.kis_domestic_functional_contract import PDNO, ROUTE
from live_trader.kis_domestic_functional_key_registry import KEY_PURPOSES
from live_trader.kis_domestic_functional_owner import (
    OWNER_RECORD_SCHEMA,
    SCHEMA_FINGERPRINT as OWNER_SCHEMA_FINGERPRINT,
    SCHEMA_VERSION as OWNER_SCHEMA_VERSION,
    _SCHEMA_SQL as OWNER_SCHEMA_SQL,
)
from live_trader.kis_domestic_functional_production_factory import (
    DisabledKisDomesticFunctionalProductionFactory,
    KisDomesticFunctionalProductionFactoryBlocked,
    ProductionFactoryPins,
    RegistryBoundStateManagerConstructors,
    RegistryDerivedStateManagerVerifier,
    RegistryDerivedVerifier,
    production_entrypoint_status,
)
from tests.test_kis_domestic_functional_key_registry import (
    _Fixture as RegistryFixture,
    _signature,
)


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _hash(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class _FactoryFixture:
    def __init__(self) -> None:
        self.registry_fixture = RegistryFixture()
        self.now = datetime.now(timezone.utc)
        self.monotonic_ns = time.monotonic_ns()
        self.registry_fixture.clock.value = self.now
        self.registry_fixture.clock.monotonic_ns = self.monotonic_ns
        self.registry_fixture.manifest["notBefore"] = _time(
            self.now - timedelta(minutes=10)
        )
        self.registry_fixture.manifest["notAfter"] = _time(
            self.now + timedelta(hours=3)
        )
        for key in self.registry_fixture.manifest["keys"]:
            key["notBefore"] = _time(self.now - timedelta(minutes=5))
            key["notAfter"] = _time(self.now + timedelta(hours=2))
        self.registry_fixture.write()
        base = Path(__file__).resolve().parents[1] / "live_trader"
        graph_file_hash = hashlib.sha256(
            (base / "kis_domestic_functional_graph.py").read_bytes()
        ).hexdigest()
        self.registry = self.registry_fixture.production_registry(
            graph_file_hash=graph_file_hash
        )
        self.temp = tempfile.TemporaryDirectory()
        self.owner_path = Path(self.temp.name) / "owner.sqlite3"
        self.owner_body = self._write_owner()

    def cleanup(self) -> None:
        self.temp.cleanup()
        self.registry_fixture.cleanup()

    def _write_owner(
        self,
        *,
        heartbeat_at: datetime | None = None,
        heartbeat_monotonic_ns: int | None = None,
    ) -> dict:
        heartbeat = heartbeat_at or datetime.now(timezone.utc)
        heartbeat_mono = (
            time.monotonic_ns()
            if heartbeat_monotonic_ns is None
            else heartbeat_monotonic_ns
        )
        owner_key = self.registry_fixture.keys["OWNER_STATE_VERIFY"]
        body = {
            "schemaVersion": OWNER_RECORD_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "epoch": 1,
            "state": "ACTIVE",
            "ownerIdHash": _sha("owner-id"),
            "processIdentityHash": _sha("process-id"),
            "leaseScopeHash": _sha("lease-scope"),
            "leaseFactoryHash": _sha("lease-factory"),
            "acquiredAt": _time(heartbeat),
            "acquiredMonotonicNs": heartbeat_mono,
            "heartbeatAt": _time(heartbeat),
            "heartbeatMonotonicNs": heartbeat_mono,
            "heartbeatCount": 1,
            "hazardousAuthorityOpen": False,
            "ownedExposurePresent": False,
            "orphanCount": 0,
            "timedOutCallCount": 0,
            "detachedCallCount": 0,
            "hazardUnionHash": _sha("hazard-union"),
            "hazardComponents": {
                component: {"component": component, "hazard": False}
                for component in (
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
            },
            "routeObservationId": _sha("route-observation"),
            "routeFenceRevision": 1,
            "routeFenceHash": _sha("route-fence"),
            "hazardObservedAt": _time(heartbeat),
            "hazardObservedMonotonicNs": heartbeat_mono,
            "sessionId": "kis-session-" + "a" * 32,
            "authorityExpiresAt": _time(heartbeat + timedelta(hours=2)),
            "sharedRouteFenceWired": False,
            "hazardReaderRegistryHash": _sha("hazard-reader-registry"),
            "reason": "TEST_ACTIVE_OWNER",
            "revision": 1,
            "authorityKeyIdHash": owner_key[1],
        }
        record_text = _canonical(body).decode()
        record_hash = _hash(body)
        record_signature = _signature(
            owner_key[0], body, prefix=b"KIS_FUNCTIONAL_OWNER\0"
        )
        transition = {
            **body,
            "previousHash": "0" * 64,
            "occurredAt": body["heartbeatAt"],
            "occurredMonotonicNs": body["heartbeatMonotonicNs"],
        }
        transition_text = _canonical(transition).decode()
        transition_hash = _hash(transition)
        transition_signature = _signature(
            owner_key[0],
            transition,
            prefix=b"KIS_FUNCTIONAL_OWNER_TRANSITION\0",
        )
        conn = sqlite3.connect(self.owner_path)
        try:
            for statement in OWNER_SCHEMA_SQL:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO kis_functional_owner_meta VALUES(1,?,?)",
                (OWNER_SCHEMA_VERSION, OWNER_SCHEMA_FINGERPRINT),
            )
            conn.execute(
                "INSERT INTO kis_functional_route_owner VALUES("
                + ",".join("?" for _ in range(34))
                + ")",
                (
                    ROUTE,
                    PDNO,
                    1,
                    "ACTIVE",
                    body["ownerIdHash"],
                    body["processIdentityHash"],
                    body["leaseScopeHash"],
                    body["leaseFactoryHash"],
                    body["acquiredAt"],
                    body["acquiredMonotonicNs"],
                    body["heartbeatAt"],
                    body["heartbeatMonotonicNs"],
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    body["hazardUnionHash"],
                    body["routeObservationId"],
                    1,
                    body["routeFenceHash"],
                    body["hazardObservedAt"],
                    body["hazardObservedMonotonicNs"],
                    body["sessionId"],
                    body["authorityExpiresAt"],
                    0,
                    body["hazardReaderRegistryHash"],
                    body["reason"],
                    1,
                    record_text,
                    record_hash,
                    record_signature,
                    body["authorityKeyIdHash"],
                ),
            )
            conn.execute(
                "INSERT INTO kis_functional_owner_transition VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ROUTE,
                    1,
                    1,
                    "ACTIVE",
                    body["heartbeatAt"],
                    body["heartbeatMonotonicNs"],
                    "0" * 64,
                    transition_text,
                    transition_hash,
                    transition_signature,
                    body["authorityKeyIdHash"],
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return body

    def pins(self, **changes) -> ProductionFactoryPins:
        status = self.registry.status()
        base = Path(__file__).resolve().parents[1] / "live_trader"
        values = {
            "registry_id": status["registryId"],
            "registry_epoch": status["registryEpoch"],
            "registry_manifest_hash": status["manifestHash"],
            "registry_manifest_file_hash": status["manifestFileHash"],
            "registry_accepted_head_hash": status["acceptedManifestHeadHash"],
            "registry_acceptance_revision": status["acceptanceRevision"],
            "registry_factory_binding_hash": status[
                "productionFactoryBindingHash"
            ],
            "registry_graph_binding_hash": status["graphRegistryBindingHash"],
            "root_key_id_hash": status["rootKeyIdHash"],
            "account_fingerprint": status["accountFingerprint"],
            "credential_configuration_hash": status[
                "credentialConfigurationHash"
            ],
            "code_manifest_hash": status["codeManifestHash"],
            "owner_epoch": 1,
            "owner_authority_key_id_hash": self.owner_body[
                "authorityKeyIdHash"
            ],
            "production_factory_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_production_factory.py").read_bytes()
            ).hexdigest(),
            "key_registry_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_key_registry.py").read_bytes()
            ).hexdigest(),
            "owner_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_owner.py").read_bytes()
            ).hexdigest(),
            "graph_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_graph.py").read_bytes()
            ).hexdigest(),
            "lane_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_lane.py").read_bytes()
            ).hexdigest(),
            "manager_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_manager.py").read_bytes()
            ).hexdigest(),
            "state_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_state.py").read_bytes()
            ).hexdigest(),
            "transport_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_transport.py").read_bytes()
            ).hexdigest(),
            "production_transport_file_hash": hashlib.sha256(
                (base / "kis_domestic_functional_production_transport.py").read_bytes()
            ).hexdigest(),
            "clock_generation": "factory-process-generation-0001",
        }
        values.update(changes)
        return ProductionFactoryPins(**values)

    def factory(self, **pin_changes):
        return DisabledKisDomesticFunctionalProductionFactory(
            registry=self.registry,
            owner_database_path=self.owner_path,
            pins=self.pins(**pin_changes),
        )


class KisDomesticFunctionalProductionFactoryTest(unittest.TestCase):
    def test_state_manager_exact_constructors_and_registry_verifiers_are_wired_offline(self):
        fixture = _FactoryFixture()
        try:
            factory = fixture.factory()
            bundle = factory.state_manager_constructors()
            self.assertIs(type(bundle), RegistryBoundStateManagerConstructors)
            self.assertIs(
                type(bundle.verifier), RegistryDerivedStateManagerVerifier
            )
            status = bundle.status()
            self.assertTrue(status["stateReceiptV2IntegrationWired"])
            self.assertTrue(status["exactConstructorsPinned"])
            self.assertFalse(status["dedicatedManagerKeyPurposeAvailable"])
            self.assertFalse(status["sharedRouteFenceWired"])
            self.assertFalse(status["productionAvailable"])
            self.assertEqual(
                bundle.state_constructor.__module__,
                "live_trader.kis_domestic_functional_state",
            )
            self.assertEqual(
                bundle.manager_constructor.__module__,
                "live_trader.kis_domestic_functional_manager",
            )
            self.assertEqual(
                bundle.graph_port_constructor.__module__,
                "live_trader.kis_domestic_functional_graph",
            )
            private, key_id = fixture.registry_fixture.keys[
                "GRAPH_RECORD_VERIFY"
            ]
            binding_body = {
                "schemaVersion": "test-state-manager-binding/v1",
                "reservedAt": _time(fixture.now),
                "managerKeyIdHash": key_id,
            }
            binding = {
                **binding_body,
                "signature": _signature(
                    private,
                    binding_body,
                    prefix=b"KIS_STATE_MANAGER_BINDING\0",
                ),
            }
            self.assertTrue(bundle.verifier.verify_binding(binding))
            self.assertFalse(
                bundle.verifier.verify_binding(
                    {**binding, "schemaVersion": "tampered"}
                )
            )
            receipt_body = {
                "schemaVersion": "test-state-manager-receipt/v2",
                "occurredAt": _time(fixture.now),
                "keyIdHash": key_id,
            }
            receipt = {
                **receipt_body,
                "signature": _signature(
                    private,
                    receipt_body,
                    prefix=b"KIS_MANAGER_RECEIPT\0",
                ),
            }
            self.assertTrue(bundle.verifier.verify_receipt(receipt))
            kwargs = bundle.state_verifier_kwargs(
                manager_binding_reader=lambda _request: {},
                manager_key_id_hash=key_id,
            )
            self.assertIs(kwargs["manager_binding_reader"].__class__, type(lambda: None))
            self.assertEqual(kwargs["manager_receipt_key_id_hash"], key_id)
        finally:
            fixture.cleanup()

    def test_disabled_status_has_no_private_signing_or_network_surface(self):
        fixture = _FactoryFixture()
        try:
            status = fixture.factory().status()
            self.assertFalse(status["productionAvailable"])
            self.assertFalse(status["networkAvailable"])
            self.assertFalse(status["mutationAvailable"])
            self.assertFalse(status["releaseAvailable"])
            self.assertFalse(status["sharedRouteFenceWired"])
            self.assertFalse(status["privateKeyMaterialPresent"])
            self.assertFalse(status["signingSurfacePresent"])
            self.assertFalse(status["networkOrderPostAllowed"])
            self.assertEqual(status["tradingMutationCount"], 0)
        finally:
            fixture.cleanup()

    def test_all_verifiers_are_registry_derived_and_have_exact_purpose_binding(self):
        fixture = _FactoryFixture()
        try:
            factory = fixture.factory()
            seen = set()
            closure_hashes = set()
            for purpose in KEY_PURPOSES:
                verifier = factory.verifier(purpose)
                self.assertIs(type(verifier), RegistryDerivedVerifier)
                binding = verifier.binding_status()
                self.assertEqual(binding["purpose"], purpose)
                self.assertTrue(binding["verifyOnly"])
                self.assertFalse(binding["productionAvailable"])
                seen.add(purpose)
                closure_hashes.add(binding["closureConfigHash"])
            self.assertEqual(seen, set(KEY_PURPOSES))
            self.assertEqual(len(closure_hashes), len(KEY_PURPOSES))
        finally:
            fixture.cleanup()

    def test_registry_derived_verifier_accepts_only_exact_signature_domain_and_key(self):
        fixture = _FactoryFixture()
        try:
            verifier = fixture.factory().verifier("OWNER_STATE_VERIFY")
            body = {"schemaVersion": "test/v1", "value": "exact"}
            private, key_id = fixture.registry_fixture.keys["OWNER_STATE_VERIFY"]
            signature = _signature(
                private, body, prefix=b"KIS_FUNCTIONAL_OWNER\0"
            )
            self.assertTrue(
                verifier.verify(
                    domain="KIS_FUNCTIONAL_OWNER",
                    body=body,
                    signature=signature,
                    key_id_hash=key_id,
                    observed_at=fixture.now,
                )
            )
            self.assertFalse(
                verifier.verify(
                    domain="KIS_FUNCTIONAL_OWNER_CHANGED",
                    body=body,
                    signature=signature,
                    key_id_hash=key_id,
                    observed_at=fixture.now,
                )
            )
            self.assertFalse(
                verifier.verify(
                    domain="KIS_FUNCTIONAL_OWNER",
                    body={**body, "value": "tampered"},
                    signature=signature,
                    key_id_hash=key_id,
                    observed_at=fixture.now,
                )
            )
        finally:
            fixture.cleanup()

    def test_verifier_cannot_be_constructed_outside_factory(self):
        fixture = _FactoryFixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "construction-forbidden",
            ):
                RegistryDerivedVerifier(
                    token=object(),
                    registry=fixture.registry,
                    purpose="OWNER_STATE_VERIFY",
                    binding={},
                    factory_binding_hash=_sha("binding"),
                    clock=object(),
                    file_guard=object(),
                )
        finally:
            fixture.cleanup()

    def test_exact_registry_and_pins_types_are_required(self):
        fixture = _FactoryFixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "exact-registry-required",
            ):
                DisabledKisDomesticFunctionalProductionFactory(
                    registry=object(),
                    owner_database_path=fixture.owner_path,
                    pins=fixture.pins(),
                )
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "exact-pins-required",
            ):
                DisabledKisDomesticFunctionalProductionFactory(
                    registry=fixture.registry,
                    owner_database_path=fixture.owner_path,
                    pins=object(),
                )
        finally:
            fixture.cleanup()

    def test_registry_identity_substitution_is_rejected(self):
        fixture = _FactoryFixture()
        try:
            for field in (
                "registry_manifest_hash",
                "registry_accepted_head_hash",
                "registry_factory_binding_hash",
                "registry_graph_binding_hash",
                "root_key_id_hash",
                "account_fingerprint",
                "credential_configuration_hash",
                "code_manifest_hash",
                "owner_authority_key_id_hash",
            ):
                with self.subTest(field=field):
                    pins = replace(fixture.pins(), **{field: _sha("wrong-" + field)})
                    with self.assertRaises(KisDomesticFunctionalProductionFactoryBlocked):
                        DisabledKisDomesticFunctionalProductionFactory(
                            registry=fixture.registry,
                            owner_database_path=fixture.owner_path,
                            pins=pins,
                        )
        finally:
            fixture.cleanup()

    def test_component_file_substitution_is_rejected(self):
        fixture = _FactoryFixture()
        try:
            fields = (
                "production_factory_file_hash",
                "key_registry_file_hash",
                "owner_file_hash",
                "graph_file_hash",
                "lane_file_hash",
                "manager_file_hash",
                "state_file_hash",
                "transport_file_hash",
                "production_transport_file_hash",
            )
            for field in fields:
                with self.subTest(field=field):
                    with self.assertRaises(
                        KisDomesticFunctionalProductionFactoryBlocked
                    ):
                        fixture.factory(**{field: _sha("substituted-" + field)})
        finally:
            fixture.cleanup()

    def test_component_file_drift_after_construction_blocks_status_and_verifier(self):
        fixture = _FactoryFixture()
        try:
            factory = fixture.factory()
            verifier = factory.verifier("OWNER_STATE_VERIFY")
            private, key_id = fixture.registry_fixture.keys["OWNER_STATE_VERIFY"]
            body = {"value": "exact"}
            signature = _signature(private, body, prefix=b"DOMAIN\0")
            with patch(
                "live_trader.kis_domestic_functional_production_factory."
                "_stable_file_hash",
                return_value="0" * 64,
            ):
                with self.assertRaisesRegex(
                    KisDomesticFunctionalProductionFactoryBlocked,
                    "file-guard-drift",
                ):
                    factory.status()
                self.assertFalse(
                    verifier.verify(
                        domain="DOMAIN",
                        body=body,
                        signature=signature,
                        key_id_hash=key_id,
                        observed_at=fixture.now,
                    )
                )
        finally:
            fixture.cleanup()

    def test_manifest_file_drift_after_construction_fails_closed(self):
        fixture = _FactoryFixture()
        try:
            factory = fixture.factory()
            fixture.registry_fixture.path.write_bytes(
                fixture.registry_fixture.path.read_bytes() + b" "
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "file-guard-drift",
            ):
                factory.status()
            verifier = factory.verifier("OWNER_STATE_VERIFY")
            self.assertFalse(
                verifier.verify(
                    domain="DOMAIN",
                    body={"value": "untrusted"},
                    signature="invalid",
                    key_id_hash=fixture.owner_body["authorityKeyIdHash"],
                    observed_at=fixture.now,
                )
            )
        finally:
            fixture.cleanup()

    def test_system_wall_or_monotonic_rollback_fails_closed(self):
        fixture = _FactoryFixture()
        try:
            factory = fixture.factory()
            with patch(
                "live_trader.kis_domestic_functional_production_factory."
                "_system_clock_pair",
                return_value=(
                    datetime.now(timezone.utc) - timedelta(minutes=1),
                    0,
                ),
            ):
                with self.assertRaisesRegex(
                    KisDomesticFunctionalProductionFactoryBlocked,
                    "clock-rollback",
                ):
                    factory.status()
        finally:
            fixture.cleanup()

    def test_owner_epoch_reader_replays_exact_record_and_transition(self):
        fixture = _FactoryFixture()
        try:
            factory = fixture.factory()
            owner = factory.owner_epoch_reader().read()
            self.assertEqual(owner["ownerEpoch"], 1)
            self.assertEqual(owner["ownerRevision"], 1)
            self.assertEqual(owner["sessionId"], fixture.owner_body["sessionId"])
            self.assertFalse(owner["sharedRouteFenceWired"])
            self.assertTrue(owner["verifyOnly"])
            self.assertFalse(owner["productionAvailable"])
            self.assertEqual(len(owner["readerResultHash"]), 64)
        finally:
            fixture.cleanup()

    def test_owner_current_row_projection_tamper_is_rejected(self):
        fixture = _FactoryFixture()
        try:
            conn = sqlite3.connect(fixture.owner_path)
            conn.execute(
                "UPDATE kis_functional_route_owner SET state='RELEASED' WHERE route=?",
                (ROUTE,),
            )
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "owner-record-unverified",
            ):
                fixture.factory()
        finally:
            fixture.cleanup()

    def test_owner_transition_signature_or_phase_tamper_is_rejected(self):
        fixture = _FactoryFixture()
        try:
            conn = sqlite3.connect(fixture.owner_path)
            conn.execute(
                "UPDATE kis_functional_owner_transition SET signature='bad',phase='RELEASED' "
                "WHERE route=?",
                (ROUTE,),
            )
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "owner-transition-unverified",
            ):
                fixture.factory()
        finally:
            fixture.cleanup()

    def test_owner_schema_drift_is_rejected(self):
        fixture = _FactoryFixture()
        try:
            conn = sqlite3.connect(fixture.owner_path)
            conn.execute("ALTER TABLE kis_functional_route_owner ADD COLUMN injected TEXT")
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "owner-schema-dirty",
            ):
                fixture.factory()
        finally:
            fixture.cleanup()

    def test_owner_epoch_missing_or_stale_is_rejected(self):
        fixture = _FactoryFixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "owner-epoch-missing",
            ):
                fixture.factory(owner_epoch=2)
            old_wall = datetime.now(timezone.utc) - timedelta(minutes=2)
            old_mono = time.monotonic_ns() - 120_000_000_000
            fixture.owner_path.unlink()
            fixture.owner_body = fixture._write_owner(
                heartbeat_at=old_wall,
                heartbeat_monotonic_ns=old_mono,
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalProductionFactoryBlocked,
                "owner-epoch-not-fresh",
            ):
                fixture.factory()
        finally:
            fixture.cleanup()

    def test_registry_acceptance_change_invalidates_existing_verifier(self):
        fixture = _FactoryFixture()
        try:
            factory = fixture.factory()
            verifier = factory.verifier("OWNER_STATE_VERIFY")
            conn = sqlite3.connect(fixture.registry_fixture.acceptance_path)
            conn.execute(
                "UPDATE kis_key_registry_acceptance SET revision=revision+1 WHERE singleton=1"
            )
            conn.commit()
            conn.close()
            body = {"value": "x"}
            private, key_id = fixture.registry_fixture.keys["OWNER_STATE_VERIFY"]
            signature = _signature(private, body, prefix=b"DOMAIN\0")
            self.assertFalse(
                verifier.verify(
                    domain="DOMAIN",
                    body=body,
                    signature=signature,
                    key_id_hash=key_id,
                    observed_at=fixture.now,
                )
            )
        finally:
            fixture.cleanup()

    def test_entrypoint_is_explicitly_disabled(self):
        status = production_entrypoint_status()
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["mutationAvailable"])
        self.assertFalse(status["releaseAvailable"])
        self.assertFalse(status["sharedRouteFenceWired"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertEqual(status["tradingMutationCount"], 0)
        self.assertTrue(status["verifyOnlyRegistryFactoryImplemented"])
        self.assertFalse(status["verifyOnlyRegistryFactoryAvailable"])


if __name__ == "__main__":
    unittest.main()
