from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_contract import PDNO, ROUTE
from live_trader.kis_domestic_functional_key_registry import (
    KEY_ALGORITHM,
    KEY_PURPOSES,
    ACCEPTANCE_SCHEMA_FINGERPRINT,
    REGISTRY_SCHEMA,
    KisDomesticFunctionalKeyRegistryBlocked,
    ProductionKeyRegistryPins,
    VerifyOnlyKeyRegistry,
    build_production_key_registry,
    key_registry_component_status,
)


NOW = datetime(2026, 8, 14, 0, 0, tzinfo=timezone.utc)
ACCOUNT = hashlib.sha256(b"registry-account").hexdigest()
CREDENTIAL = hashlib.sha256(b"registry-credential").hexdigest()
CODE = hashlib.sha256(b"registry-code").hexdigest()


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _signature(private_key, body, *, prefix=b"") -> str:
    return base64.b64encode(
        eddsa.new(private_key, mode="rfc8032").sign(prefix + _canonical(body))
    ).decode()


class _Clock:
    def __init__(self):
        self.value = NOW
        self.monotonic_ns = 10_000_000_000

    def __call__(self):
        return self.value

    def monotonic(self):
        return self.monotonic_ns

    def advance(self, seconds: float):
        self.value += timedelta(seconds=seconds)
        self.monotonic_ns += int(seconds * 1_000_000_000)


class _Fixture:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "registry.json"
        self.acceptance_path = Path(self.temp.name) / "acceptance.sqlite3"
        self.clock = _Clock()
        self.root_private = ECC.generate(curve="Ed25519")
        self.root_public_pem = self.root_private.public_key().export_key(format="PEM")
        self.root_key_id_hash = hashlib.sha256(
            self.root_public_pem.encode()
        ).hexdigest()
        self.keys = {}
        rows = []
        for purpose in KEY_PURPOSES:
            private = ECC.generate(curve="Ed25519")
            public_pem = private.public_key().export_key(format="PEM")
            key_id_hash = hashlib.sha256(public_pem.encode()).hexdigest()
            self.keys[purpose] = (private, key_id_hash)
            rows.append(
                {
                    "keyId": f"kis-{purpose.lower().replace('_', '-')}-v1",
                    "keyIdHash": key_id_hash,
                    "purpose": purpose,
                    "algorithm": KEY_ALGORITHM,
                    "rotationEpoch": 1,
                    "notBefore": _time(NOW - timedelta(minutes=5)),
                    "notAfter": _time(NOW + timedelta(hours=2)),
                    "accountFingerprint": ACCOUNT,
                    "credentialConfigurationHash": CREDENTIAL,
                    "codeManifestHash": CODE,
                    "publicKeyPem": public_pem,
                }
            )
        self.manifest = {
            "schemaVersion": REGISTRY_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": "kis-domestic-functional-registry-0001",
            "registryEpoch": 1,
            "notBefore": _time(NOW - timedelta(minutes=10)),
            "notAfter": _time(NOW + timedelta(hours=3)),
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "codeManifestHash": CODE,
            "previousManifestHash": None,
            "keys": rows,
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
                        b"readers-component-file"
                    ).hexdigest(),
                    "protocolHash": hashlib.sha256(
                        b"readers-component-protocol"
                    ).hexdigest(),
                    "schemaFingerprint": hashlib.sha256(
                        b"readers-component-schema"
                    ).hexdigest(),
                    "statusHash": hashlib.sha256(
                        b"readers-component-status"
                    ).hexdigest(),
                    "authorityKeyIdHash": self.keys[
                        "READERS_COMPONENT_VERIFY"
                    ][1],
                    "authorityPurpose": "READERS_COMPONENT_VERIFY",
                    "signatureDomain": (
                        "KIS_DOMESTIC_FUNCTIONAL_READERS_COMPONENT"
                    ),
                }
            ],
        }
        self.write()

    def write(self, *, root_private=None):
        manifest_hash = hashlib.sha256(_canonical(self.manifest)).hexdigest()
        signed = {**self.manifest, "manifestHash": manifest_hash}
        private = root_private or self.root_private
        document = {
            "manifest": self.manifest,
            "manifestHash": manifest_hash,
            "rootKeyIdHash": self.root_key_id_hash,
            "rootSignature": _signature(
                private,
                signed,
                prefix=b"KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY\0",
            ),
        }
        self.path.write_bytes(_canonical(document))

    def registry(self, **kwargs):
        return VerifyOnlyKeyRegistry(
            self.path,
            pinned_root_public_key_pem=self.root_public_pem,
            pinned_root_key_id_hash=self.root_key_id_hash,
            expected_account_fingerprint=ACCOUNT,
            expected_credential_configuration_hash=CREDENTIAL,
            expected_code_manifest_hash=CODE,
            trusted_clock=self.clock,
            **kwargs,
        )

    def production_pins(self, **changes):
        values = {
            "registry_id": self.manifest["registryId"],
            "manifest_file_hash": hashlib.sha256(self.path.read_bytes()).hexdigest(),
            "root_public_key_pem": self.root_public_pem,
            "root_key_id_hash": self.root_key_id_hash,
            "account_fingerprint": ACCOUNT,
            "credential_configuration_hash": CREDENTIAL,
            "code_manifest_hash": CODE,
            "graph_file_hash": hashlib.sha256(b"graph-file").hexdigest(),
            "graph_protocol_hash": hashlib.sha256(b"graph-protocol").hexdigest(),
            "graph_schema_fingerprint": hashlib.sha256(b"graph-schema").hexdigest(),
        }
        values.update(changes)
        return ProductionKeyRegistryPins(**values)

    def production_registry(self, **pin_changes):
        return build_production_key_registry(
            self.path,
            self.acceptance_path,
            pins=self.production_pins(**pin_changes),
            trusted_wall_clock=self.clock,
            trusted_monotonic_clock=self.clock.monotonic,
            clock_generation="test-process-generation-0001",
        )

    def cleanup(self):
        self.temp.cleanup()


class KisDomesticFunctionalKeyRegistryTest(unittest.TestCase):
    def test_asymmetric_root_and_all_purposes_load_verify_only(self):
        fixture = _Fixture()
        try:
            registry = fixture.registry()
            status = registry.status()
            self.assertTrue(status["asymmetricRootVerified"])
            self.assertTrue(status["productionAuthorityPinned"])
            self.assertFalse(status["productionFactoryAuthorityPinned"])
            self.assertIn(
                "KEY_REGISTRY_DURABLE_ANTI_ROLLBACK_NOT_WIRED",
                status["readinessBlockers"],
            )
            self.assertTrue(status["allPurposesCovered"])
            self.assertEqual(len(KEY_PURPOSES), status["purposeCount"])
            self.assertFalse(status["privateKeyMaterialPresent"])
            self.assertFalse(status["signingSurfacePresent"])
            self.assertFalse(hasattr(registry, "sign"))
            self.assertFalse(status["productionAvailable"])
        finally:
            fixture.cleanup()

    def test_component_signature_verifies_exact_purpose_domain_and_body(self):
        fixture = _Fixture()
        try:
            registry = fixture.registry()
            purpose = "LANE_RECORD_VERIFY"
            private, key_id_hash = fixture.keys[purpose]
            body = {"authorityKeyIdHash": key_id_hash, "sessionId": "session-1"}
            domain = "KIS_LANE_RECORD"
            signature = _signature(
                private, body, prefix=domain.encode() + b"\0"
            )
            self.assertTrue(
                registry.verify(
                    purpose=purpose,
                    domain=domain,
                    body=body,
                    signature=signature,
                    key_id_hash=key_id_hash,
                )
            )
            adapter = registry.component_verifier_for(purpose)
            self.assertTrue(
                adapter("lane", domain, body, signature, key_id_hash)
            )
            for changed in (
                {**body, "sessionId": "session-2"},
                body,
            ):
                with self.subTest(changed=changed):
                    self.assertFalse(
                        registry.verify(
                            purpose=(
                                purpose
                                if changed is not body
                                else "SOURCE_RECORD_VERIFY"
                            ),
                            domain=domain,
                            body=changed,
                            signature=signature,
                            key_id_hash=key_id_hash,
                        )
                    )
        finally:
            fixture.cleanup()

    def test_root_signed_readers_component_binding_is_exact_and_current(self):
        fixture = _Fixture()
        try:
            registry = fixture.production_registry()
            result = registry.component_binding("readers")
            binding = result["componentBinding"]
            self.assertEqual("readers", binding["component"])
            self.assertEqual(
                "READERS_COMPONENT_VERIFY", binding["authorityPurpose"]
            )
            self.assertEqual(
                fixture.keys["READERS_COMPONENT_VERIFY"][1],
                binding["authorityKeyIdHash"],
            )
            self.assertEqual(
                registry.acceptance_head_hash,
                result["acceptedManifestHeadHash"],
            )
            self.assertTrue(result["durableAcceptanceVerified"])
            self.assertTrue(result["productionFactoryAuthorityPinned"])
            self.assertFalse(result["productionAvailable"])
        finally:
            fixture.cleanup()

        for mutation, message in (
            (
                lambda manifest: manifest.__setitem__("componentBindings", []),
                "component-binding-cardinality",
            ),
            (
                lambda manifest: manifest["componentBindings"][0].__setitem__(
                    "protocolHash", "not-a-sha"
                ),
                "component-binding-protocolHash-invalid",
            ),
            (
                lambda manifest: manifest["componentBindings"][0].__setitem__(
                    "authorityKeyIdHash", "f" * 64
                ),
                "component-binding-key-not-current",
            ),
        ):
            fixture = _Fixture()
            try:
                mutation(fixture.manifest)
                fixture.write()
                with self.assertRaisesRegex(
                    KisDomesticFunctionalKeyRegistryBlocked, message
                ):
                    fixture.registry()
            finally:
                fixture.cleanup()

    def test_market_source_purpose_is_verify_only_and_durable_antirobllback_bound(self):
        fixture = _Fixture()
        try:
            first = fixture.production_registry()
            purpose = "MARKET_SOURCE_RECORD_VERIFY"
            self.assertIn(purpose, KEY_PURPOSES)
            private, key_id_hash = fixture.keys[purpose]
            body = {
                "authorityKeyIdHash": key_id_hash,
                "sourceGeneration": "kis-ws-generation-" + "a" * 32,
            }
            domain = "MARKET_SOURCE_RECORD"
            signature = _signature(
                private, body, prefix=domain.encode() + b"\0"
            )
            self.assertTrue(
                first.verify(
                    purpose=purpose,
                    domain=domain,
                    body=body,
                    signature=signature,
                    key_id_hash=key_id_hash,
                )
            )
            first_bytes = fixture.path.read_bytes()
            fixture.manifest["registryEpoch"] = 2
            fixture.manifest["previousManifestHash"] = first.manifest_hash
            fixture.write()
            second = fixture.production_registry()
            self.assertEqual(2, second.status()["registryEpoch"])
            self.assertTrue(second.status()["durableAcceptanceVerified"])
            fixture.path.write_bytes(first_bytes)
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "rollback-or-lineage-gap",
            ):
                fixture.production_registry()
        finally:
            fixture.cleanup()

    def test_root_signature_and_root_key_pin_tamper_rejected(self):
        fixture = _Fixture()
        try:
            other = ECC.generate(curve="Ed25519")
            fixture.write(root_private=other)
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked, "root-signature"
            ):
                fixture.registry()
            fixture.write()
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked, "root-key-id-mismatch"
            ):
                VerifyOnlyKeyRegistry(
                    fixture.path,
                    pinned_root_public_key_pem=other.public_key().export_key(
                        format="PEM"
                    ),
                    pinned_root_key_id_hash=fixture.root_key_id_hash,
                    expected_account_fingerprint=ACCOUNT,
                    expected_credential_configuration_hash=CREDENTIAL,
                    expected_code_manifest_hash=CODE,
                    trusted_clock=fixture.clock,
                )
        finally:
            fixture.cleanup()

    def test_private_root_material_is_rejected(self):
        fixture = _Fixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked, "root-public-key"
            ):
                VerifyOnlyKeyRegistry(
                    fixture.path,
                    pinned_root_public_key_pem=fixture.root_private.export_key(
                        format="PEM"
                    ),
                    pinned_root_key_id_hash=fixture.root_key_id_hash,
                    expected_account_fingerprint=ACCOUNT,
                    expected_credential_configuration_hash=CREDENTIAL,
                    expected_code_manifest_hash=CODE,
                    trusted_clock=fixture.clock,
                )
        finally:
            fixture.cleanup()

    def test_manifest_account_credential_code_and_time_binding_exact(self):
        cases = (
            ("accountFingerprint", "f" * 64),
            ("credentialConfigurationHash", "f" * 64),
            ("codeManifestHash", "f" * 64),
            ("notBefore", _time(NOW + timedelta(seconds=1))),
            ("notAfter", _time(NOW)),
        )
        for field, value in cases:
            with self.subTest(field=field):
                fixture = _Fixture()
                try:
                    fixture.manifest[field] = value
                    fixture.write()
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalKeyRegistryBlocked,
                        "binding-or-time-invalid",
                    ):
                        fixture.registry()
                finally:
                    fixture.cleanup()

    def test_missing_or_duplicate_purpose_and_rotation_epoch_rejected(self):
        cases = ("missing", "duplicate-epoch")
        for kind in cases:
            with self.subTest(kind=kind):
                fixture = _Fixture()
                try:
                    if kind == "missing":
                        fixture.manifest["keys"] = fixture.manifest["keys"][1:]
                        message = "coverage-incomplete"
                    else:
                        duplicate = deepcopy(fixture.manifest["keys"][0])
                        private = ECC.generate(curve="Ed25519")
                        public = private.public_key().export_key(format="PEM")
                        duplicate["keyId"] += "-duplicate"
                        duplicate["keyIdHash"] = hashlib.sha256(
                            public.encode()
                        ).hexdigest()
                        duplicate["publicKeyPem"] = public
                        fixture.manifest["keys"].append(duplicate)
                        message = "rotation-epoch-duplicate"
                    fixture.write()
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalKeyRegistryBlocked, message
                    ):
                        fixture.registry()
                finally:
                    fixture.cleanup()

    def test_revoked_key_never_verifies_and_unknown_revocation_rejected(self):
        fixture = _Fixture()
        try:
            old = fixture.manifest["keys"][0]
            purpose = old["purpose"]
            old_private, old_hash = fixture.keys[purpose]
            new_private = ECC.generate(curve="Ed25519")
            new_public = new_private.public_key().export_key(format="PEM")
            new_hash = hashlib.sha256(new_public.encode()).hexdigest()
            replacement = {
                **deepcopy(old),
                "keyId": old["keyId"] + "-v2",
                "keyIdHash": new_hash,
                "rotationEpoch": 2,
                "publicKeyPem": new_public,
            }
            fixture.manifest["registryEpoch"] = 2
            fixture.manifest["previousManifestHash"] = "a" * 64
            fixture.manifest["keys"].append(replacement)
            fixture.manifest["revocations"] = [
                {
                    "keyIdHash": old_hash,
                    "revokedAt": _time(NOW - timedelta(seconds=1)),
                    "reason": "rotation",
                }
            ]
            fixture.write()
            registry = fixture.registry()
            body = {"authorityKeyIdHash": old_hash, "value": 1}
            signature = _signature(
                old_private, body, prefix=b"DOMAIN\0"
            )
            self.assertFalse(
                registry.verify(
                    purpose=purpose,
                    domain="DOMAIN",
                    body=body,
                    signature=signature,
                    key_id_hash=old_hash,
                )
            )
            new_body = {"authorityKeyIdHash": new_hash, "value": 1}
            self.assertTrue(
                registry.verify(
                    purpose=purpose,
                    domain="DOMAIN",
                    body=new_body,
                    signature=_signature(
                        new_private, new_body, prefix=b"DOMAIN\0"
                    ),
                    key_id_hash=new_hash,
                )
            )

            fixture.cleanup()
            fixture = _Fixture()
            try:
                fixture.manifest["revocations"] = [
                    {
                        "keyIdHash": "f" * 64,
                        "revokedAt": _time(NOW),
                        "reason": "unknown",
                    }
                ]
                fixture.write()
                with self.assertRaisesRegex(
                    KisDomesticFunctionalKeyRegistryBlocked,
                    "revocation-key-missing",
                ):
                    fixture.registry()
            finally:
                fixture.cleanup()
                fixture = None
        finally:
            if fixture is not None:
                fixture.cleanup()

    def test_key_level_time_account_and_code_binding_rejected(self):
        cases = (
            ("notBefore", _time(NOW - timedelta(hours=1))),
            ("notAfter", _time(NOW + timedelta(hours=4))),
            ("accountFingerprint", "f" * 64),
            ("credentialConfigurationHash", "f" * 64),
            ("codeManifestHash", "f" * 64),
            ("rotationEpoch", 2),
        )
        for field, value in cases:
            with self.subTest(field=field):
                fixture = _Fixture()
                try:
                    fixture.manifest["keys"][0][field] = value
                    fixture.write()
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalKeyRegistryBlocked,
                        "key-binding-invalid",
                    ):
                        fixture.registry()
                finally:
                    fixture.cleanup()

    def test_atomic_file_change_and_manifest_hash_tamper_rejected(self):
        fixture = _Fixture()
        try:
            before = fixture.path.read_bytes()
            document = json.loads(before)
            document["manifestHash"] = "f" * 64
            fixture.path.write_bytes(_canonical(document))
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked, "root-signature"
            ):
                fixture.registry()

            fixture.write()
            before = fixture.path.read_bytes()
            after = before + b" "
            original = Path.read_bytes

            def changing(path):
                if Path(path) == fixture.path:
                    return changing.values.pop(0)
                return original(path)

            changing.values = [before, after]
            with patch.object(Path, "read_bytes", changing):
                with self.assertRaisesRegex(
                    KisDomesticFunctionalKeyRegistryBlocked,
                    "changed-during-load",
                ):
                    fixture.registry()
        finally:
            fixture.cleanup()

    def test_mock_root_is_explicit_offline_and_component_status_disabled(self):
        fixture = _Fixture()
        try:
            secret = hashlib.sha256(b"mock-root").digest()
            manifest_hash = hashlib.sha256(
                _canonical(fixture.manifest)
            ).hexdigest()
            signed = {**fixture.manifest, "manifestHash": manifest_hash}
            signature = hmac.new(secret, _canonical(signed), hashlib.sha256).hexdigest()
            fixture.path.write_bytes(
                _canonical(
                    {
                        "manifest": fixture.manifest,
                        "manifestHash": manifest_hash,
                        "rootKeyIdHash": fixture.root_key_id_hash,
                        "rootSignature": signature,
                    }
                )
            )

            def verify(domain, body, candidate):
                return domain == "KIS_DOMESTIC_FUNCTIONAL_KEY_REGISTRY" and hmac.compare_digest(
                    candidate,
                    hmac.new(secret, _canonical(body), hashlib.sha256).hexdigest(),
                )

            registry = VerifyOnlyKeyRegistry(
                fixture.path,
                pinned_root_public_key_pem=None,
                pinned_root_key_id_hash=fixture.root_key_id_hash,
                expected_account_fingerprint=ACCOUNT,
                expected_credential_configuration_hash=CREDENTIAL,
                expected_code_manifest_hash=CODE,
                trusted_clock=fixture.clock,
                allow_mock_root_verifier=True,
                mock_root_verifier=verify,
            )
            self.assertFalse(registry.status()["productionAuthorityPinned"])
            self.assertIn(
                "ASYMMETRIC_PRODUCTION_ROOT_NOT_PINNED",
                registry.status()["readinessBlockers"],
            )
            component = key_registry_component_status()
            self.assertTrue(component["verifyOnly"])
            self.assertFalse(component["productionAvailable"])
            self.assertFalse(component["networkOrderPostAllowed"])
            self.assertEqual(0, component["tradingMutationCount"])
        finally:
            fixture.cleanup()

    def test_rotation_lineage_manifest_expiry_and_future_revocation_fail_closed(self):
        cases = ("missing-previous", "future-revocation")
        for kind in cases:
            with self.subTest(kind=kind):
                fixture = _Fixture()
                try:
                    if kind == "missing-previous":
                        fixture.manifest["registryEpoch"] = 2
                        fixture.manifest["keys"][0]["rotationEpoch"] = 2
                        message = "previous-hash-missing"
                    else:
                        fixture.manifest["revocations"] = [
                            {
                                "keyIdHash": fixture.manifest["keys"][0][
                                    "keyIdHash"
                                ],
                                "revokedAt": _time(NOW + timedelta(seconds=1)),
                                "reason": "future",
                            }
                        ]
                        message = "future-dated"
                    fixture.write()
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalKeyRegistryBlocked, message
                    ):
                        fixture.registry()
                finally:
                    fixture.cleanup()

        fixture = _Fixture()
        try:
            registry = fixture.registry()
            fixture.clock.value = NOW + timedelta(hours=4)
            status = registry.status()
            self.assertFalse(status["manifestFresh"])
            self.assertIn(
                "KEY_REGISTRY_MANIFEST_EXPIRED", status["readinessBlockers"]
            )
            private, key_id_hash = fixture.keys["LANE_RECORD_VERIFY"]
            body = {"authorityKeyIdHash": key_id_hash}
            self.assertFalse(
                registry.verify(
                    purpose="LANE_RECORD_VERIFY",
                    domain="DOMAIN",
                    body=body,
                    signature=_signature(private, body, prefix=b"DOMAIN\0"),
                    key_id_hash=key_id_hash,
                )
            )
        finally:
            fixture.cleanup()

    def test_production_factory_binds_durable_acceptance_dual_clock_and_graph_fields(self):
        fixture = _Fixture()
        try:
            registry = fixture.production_registry()
            status = registry.status()
            self.assertTrue(status["productionAuthorityPinned"])
            self.assertTrue(status["productionFactoryAuthorityPinned"])
            self.assertTrue(status["durableAcceptanceVerified"])
            self.assertTrue(status["trustedWallMonotonicLineageVerified"])
            self.assertTrue(status["productionFactoryPinsBound"])
            self.assertEqual(1, status["acceptanceRevision"])
            self.assertEqual(
                ACCEPTANCE_SCHEMA_FINGERPRINT,
                status["acceptanceSchemaFingerprint"],
            )
            self.assertRegex(status["acceptedManifestHeadHash"], r"^[0-9a-f]{64}$")
            self.assertRegex(status["graphRegistryBindingHash"], r"^[0-9a-f]{64}$")
            self.assertFalse(status["graphRegistryBindingWired"])
            self.assertIn(
                "GRAPH_KEY_REGISTRY_BINDING_NOT_WIRED",
                status["readinessBlockers"],
            )
            self.assertFalse(status["productionAvailable"])
            self.assertFalse(status["networkOrderPostAllowed"])
        finally:
            fixture.cleanup()

    def test_durable_epoch_predecessor_chain_rejects_rollback_conflict_and_gap(self):
        fixture = _Fixture()
        try:
            first = fixture.production_registry()
            first_hash = first.manifest_hash
            first_bytes = fixture.path.read_bytes()
            fixture.manifest["registryEpoch"] = 2
            fixture.manifest["previousManifestHash"] = first_hash
            fixture.write()
            second = fixture.production_registry()
            self.assertEqual(2, second.registry_epoch)
            self.assertEqual(2, second.acceptance_revision)

            second_bytes = fixture.path.read_bytes()
            fixture.path.write_bytes(first_bytes)
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "rollback-or-lineage-gap",
            ):
                fixture.production_registry()

            fixture.path.write_bytes(second_bytes)
            fixture.manifest = json.loads(second_bytes)["manifest"]
            fixture.manifest["notAfter"] = _time(NOW + timedelta(hours=4))
            fixture.write()
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "acceptance-epoch-conflict",
            ):
                fixture.production_registry()
        finally:
            fixture.cleanup()

        fixture = _Fixture()
        try:
            fixture.manifest["registryEpoch"] = 2
            fixture.manifest["previousManifestHash"] = "a" * 64
            fixture.write()
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "predecessor-unproven",
            ):
                fixture.production_registry()
        finally:
            fixture.cleanup()

    def test_durable_acceptance_tamper_and_dirty_schema_fail_closed(self):
        fixture = _Fixture()
        try:
            registry = fixture.production_registry()
            conn = sqlite3.connect(fixture.acceptance_path)
            try:
                conn.execute(
                    "UPDATE kis_key_registry_acceptance SET transition_head_hash=?",
                    ("f" * 64,),
                )
                conn.commit()
            finally:
                conn.close()
            private, key_id_hash = fixture.keys["LANE_RECORD_VERIFY"]
            body = {"authorityKeyIdHash": key_id_hash}
            self.assertFalse(
                registry.verify(
                    purpose="LANE_RECORD_VERIFY",
                    domain="DOMAIN",
                    body=body,
                    signature=_signature(private, body, prefix=b"DOMAIN\0"),
                    key_id_hash=key_id_hash,
                )
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "acceptance-current-binding-invalid",
            ):
                registry.status()
        finally:
            fixture.cleanup()

        fixture = _Fixture()
        try:
            conn = sqlite3.connect(fixture.acceptance_path)
            try:
                conn.execute("CREATE TABLE dirty(value TEXT)")
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "acceptance-schema-dirty",
            ):
                fixture.production_registry()
        finally:
            fixture.cleanup()

    def test_trusted_wall_and_monotonic_rollback_or_divergence_fail_closed(self):
        fixture = _Fixture()
        try:
            registry = fixture.production_registry()
            fixture.clock.value += timedelta(seconds=10)
            fixture.clock.monotonic_ns += 1_000_000_000
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "trusted-clock-divergence",
            ):
                registry.status()
        finally:
            fixture.cleanup()

        fixture = _Fixture()
        try:
            registry = fixture.production_registry()
            fixture.clock.monotonic_ns -= 1
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "trusted-monotonic-rollback",
            ):
                registry.status()
        finally:
            fixture.cleanup()

    def test_verify_all_malformed_inputs_return_false_and_registry_id_is_strict(self):
        fixture = _Fixture()
        try:
            registry = fixture.registry()
            private, key_id_hash = fixture.keys["LANE_RECORD_VERIFY"]
            valid_body = {"authorityKeyIdHash": key_id_hash}
            valid_signature = _signature(
                private, valid_body, prefix=b"DOMAIN\0"
            )
            malformed = (
                {"purpose": "UNKNOWN"},
                {"domain": 7},
                {"body": {"bad": float("nan")}},
                {"body": {"bad": {1, 2}}},
                {"signature": object()},
                {"key_id_hash": object()},
            )
            defaults = {
                "purpose": "LANE_RECORD_VERIFY",
                "domain": "DOMAIN",
                "body": valid_body,
                "signature": valid_signature,
                "key_id_hash": key_id_hash,
            }
            for changes in malformed:
                with self.subTest(changes=changes):
                    self.assertFalse(registry.verify(**{**defaults, **changes}))
        finally:
            fixture.cleanup()

        fixture = _Fixture()
        try:
            fixture.manifest["registryId"] = 123
            fixture.write()
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "binding-or-time-invalid",
            ):
                fixture.registry()
        finally:
            fixture.cleanup()

    def test_production_factory_rejects_manifest_and_binding_pin_substitution(self):
        fixture = _Fixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "manifest-file-pin-mismatch",
            ):
                build_production_key_registry(
                    fixture.path,
                    fixture.acceptance_path,
                    pins=fixture.production_pins(manifest_file_hash="0" * 64),
                    trusted_wall_clock=fixture.clock,
                    trusted_monotonic_clock=fixture.clock.monotonic,
                    clock_generation="test-process-generation-0001",
                )
            with self.assertRaisesRegex(
                KisDomesticFunctionalKeyRegistryBlocked,
                "binding-or-time-invalid|factory-binding-mismatch",
            ):
                build_production_key_registry(
                    fixture.path,
                    fixture.acceptance_path,
                    pins=fixture.production_pins(account_fingerprint="f" * 64),
                    trusted_wall_clock=fixture.clock,
                    trusted_monotonic_clock=fixture.clock.monotonic,
                    clock_generation="test-process-generation-0001",
                )
        finally:
            fixture.cleanup()


if __name__ == "__main__":
    unittest.main()
