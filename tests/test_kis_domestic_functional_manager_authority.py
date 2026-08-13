from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import unittest
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_manager_authority import (
    BINDING_SIGNATURE_DOMAIN,
    DETAILED_RECEIPT_SIGNATURE_DOMAIN,
    MANAGER_KEY_PURPOSE,
    MANIFEST_SCHEMA,
    RECEIPT_SIGNATURE_DOMAIN,
    ROOT_SIGNATURE_DOMAIN,
    KisDomesticFunctionalManagerAuthorityBlocked,
    ManagerAuthorityPins,
    VerifyOnlyKisDomesticFunctionalManagerAuthority,
    production_entrypoint_status,
)


ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
ZERO = "0" * 64
NOW = datetime(2026, 8, 14, 5, 0, tzinfo=timezone.utc)


def canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def public_pem(key) -> str:
    return key.public_key().export_key(format="PEM")


def key_hash(key) -> str:
    return hashlib.sha256(public_pem(key).encode("utf-8")).hexdigest()


def signature(key, message: bytes) -> str:
    return base64.b64encode(
        eddsa.new(key, mode="rfc8032").sign(message)
    ).decode("ascii")


class KisDomesticFunctionalManagerAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "manager-authority.json"
        self.root = ECC.generate(curve="Ed25519")
        self.manager = ECC.generate(curve="Ed25519")
        self.account = hashlib.sha256(b"account").hexdigest()
        self.credential = hashlib.sha256(b"credential").hexdigest()
        self.code = hashlib.sha256(b"code-manifest").hexdigest()
        self.body = {
            "schemaVersion": MANIFEST_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": "kis-manager-authority-test",
            "registryEpoch": 7,
            "previousManifestHash": ZERO,
            "accountFingerprint": self.account,
            "credentialConfigurationHash": self.credential,
            "codeManifestHash": self.code,
            "rootKeyIdHash": key_hash(self.root),
            "managerKey": {
                "purpose": MANAGER_KEY_PURPOSE,
                "algorithm": "ED25519",
                "keyIdHash": key_hash(self.manager),
                "publicKeyPem": public_pem(self.manager),
                "rotationEpoch": 7,
                "notBefore": "2026-08-13T05:00:00Z",
                "notAfter": "2026-08-15T05:00:00Z",
                "signatureDomains": [
                    "KIS_STATE_MANAGER_BINDING",
                    "KIS_MANAGER_RECEIPT",
                    "KIS_FUNCTIONAL_MANAGER_RECEIPT",
                ],
            },
            "issuedAt": "2026-08-14T05:00:00Z",
            "issuedMonotonicNs": 100,
            "productionProvisioned": False,
        }

    def _write(self, body=None, *, root=None, canonical_file=True):
        body = deepcopy(self.body if body is None else body)
        root = self.root if root is None else root
        envelope = {
            "body": body,
            "manifestHash": digest(body),
            "rootKeyIdHash": body["rootKeyIdHash"],
            "rootSignature": signature(
                root, ROOT_SIGNATURE_DOMAIN + canonical(body)
            ),
        }
        raw = canonical(envelope)
        if not canonical_file:
            raw += b"\n"
        self.path.write_bytes(raw)
        pins = ManagerAuthorityPins(
            registry_id="kis-manager-authority-test",
            registry_epoch=7,
            manifest_file_hash=hashlib.sha256(raw).hexdigest(),
            root_public_key_pem=public_pem(self.root),
            root_key_id_hash=key_hash(self.root),
            manager_key_id_hash=key_hash(self.manager),
            account_fingerprint=self.account,
            credential_configuration_hash=self.credential,
            code_manifest_hash=self.code,
        )
        return envelope, pins

    def _authority(self):
        _, pins = self._write()
        return VerifyOnlyKisDomesticFunctionalManagerAuthority(
            self.path,
            pins=pins,
            trusted_clock=lambda: NOW,
        )

    def _signed(self, body, hash_field, domain, *, key=None):
        key = self.manager if key is None else key
        value_hash = digest(body)
        return {
            **body,
            hash_field: value_hash,
            "signature": signature(key, domain + value_hash.encode("ascii")),
        }

    def _binding(self):
        return self._signed(
            {
                "schemaVersion":
                    "kis-domestic-functional-state-manager-binding/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "reservationId": "reservation-1",
                "managerKeyIdHash": key_hash(self.manager),
                "productionAvailable": False,
            },
            "bindingHash",
            BINDING_SIGNATURE_DOMAIN,
        )

    def _receipt(self, *, detailed=False):
        body = {
            "schemaVersion": "kis-domestic-functional-manager-receipt/v2",
            "route": ROUTE,
            "reservationId": "reservation-1",
            "productionAvailable": False,
        }
        if detailed:
            body.update(
                pdno=PDNO,
                signerKeyIdHash=key_hash(self.manager),
            )
            return self._signed(
                body,
                "receiptHash",
                DETAILED_RECEIPT_SIGNATURE_DOMAIN,
            )
        body["keyIdHash"] = key_hash(self.manager)
        return self._signed(
            body,
            "receiptHash",
            RECEIPT_SIGNATURE_DOMAIN,
        )

    def test_happy_path_verifies_all_three_exact_manager_domains(self):
        authority = self._authority()
        self.assertTrue(authority.verify_binding(self._binding()))
        self.assertTrue(authority.verify_receipt(self._receipt()))
        self.assertTrue(
            authority.verify_pending_reservation_proof(self._receipt())
        )
        self.assertTrue(
            authority.verify_detailed_receipt(self._receipt(detailed=True))
        )

    def test_status_is_verify_only_and_all_production_flags_are_false(self):
        authority = self._authority()
        status = authority.status()
        self.assertTrue(status["rootSignatureVerified"])
        self.assertTrue(status["dedicatedManagerPurposeVerified"])
        self.assertTrue(status["verifyOnlyConsumer"])
        self.assertFalse(status["privateSignerPresent"])
        self.assertFalse(status["consumerSigningSurface"])
        self.assertFalse(status["productionProvisioningAvailable"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertFalse(hasattr(authority, "sign"))
        self.assertFalse(any("private" in name for name in vars(authority)))

    def test_root_signature_tamper_is_rejected_even_with_updated_file_pin(self):
        envelope, pins = self._write()
        envelope["rootSignature"] = base64.b64encode(b"x" * 64).decode()
        raw = canonical(envelope)
        self.path.write_bytes(raw)
        pins = ManagerAuthorityPins(
            **{**asdict(pins), "manifest_file_hash": hashlib.sha256(raw).hexdigest()}
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalManagerAuthorityBlocked,
            "root-signature-invalid",
        ):
            VerifyOnlyKisDomesticFunctionalManagerAuthority(
                self.path, pins=pins, trusted_clock=lambda: NOW
            )

    def test_root_signed_wrong_purpose_or_domain_set_is_rejected(self):
        for mutation in ("purpose", "domains"):
            with self.subTest(mutation=mutation):
                body = deepcopy(self.body)
                if mutation == "purpose":
                    body["managerKey"]["purpose"] = "GRAPH_RECORD_VERIFY"
                else:
                    body["managerKey"]["signatureDomains"] = [
                        "KIS_MANAGER_RECEIPT"
                    ]
                _, pins = self._write(body)
                with self.assertRaisesRegex(
                    KisDomesticFunctionalManagerAuthorityBlocked,
                    "binding-or-validity-invalid",
                ):
                    VerifyOnlyKisDomesticFunctionalManagerAuthority(
                        self.path, pins=pins, trusted_clock=lambda: NOW
                    )

    def test_private_manager_key_material_is_rejected(self):
        body = deepcopy(self.body)
        body["managerKey"]["publicKeyPem"] = self.manager.export_key(format="PEM")
        _, pins = self._write(body)
        with self.assertRaisesRegex(
            KisDomesticFunctionalManagerAuthorityBlocked,
            "manager-key-invalid",
        ):
            VerifyOnlyKisDomesticFunctionalManagerAuthority(
                self.path, pins=pins, trusted_clock=lambda: NOW
            )

    def test_account_credential_and_code_pins_fail_closed(self):
        _, pins = self._write()
        for field in (
            "account_fingerprint",
            "credential_configuration_hash",
            "code_manifest_hash",
        ):
            with self.subTest(field=field):
                values = asdict(pins)
                values[field] = hashlib.sha256(field.encode()).hexdigest()
                with self.assertRaisesRegex(
                    KisDomesticFunctionalManagerAuthorityBlocked,
                    "binding-or-validity-invalid",
                ):
                    VerifyOnlyKisDomesticFunctionalManagerAuthority(
                        self.path,
                        pins=ManagerAuthorityPins(**values),
                        trusted_clock=lambda: NOW,
                    )

    def test_future_and_expired_manager_certificate_fail_closed(self):
        _, pins = self._write()
        for label, now in (
            ("future", NOW - timedelta(days=2)),
            ("expired", NOW + timedelta(days=2)),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    KisDomesticFunctionalManagerAuthorityBlocked,
                    "binding-or-validity-invalid",
                ):
                    VerifyOnlyKisDomesticFunctionalManagerAuthority(
                        self.path, pins=pins, trusted_clock=lambda now=now: now
                    )

        body = deepcopy(self.body)
        body["issuedAt"] = "2026-08-14T06:00:00Z"
        _, pins = self._write(body)
        with self.subTest(label="future-manifest-inside-active-certificate"):
            with self.assertRaisesRegex(
                KisDomesticFunctionalManagerAuthorityBlocked,
                "binding-or-validity-invalid",
            ):
                VerifyOnlyKisDomesticFunctionalManagerAuthority(
                    self.path, pins=pins, trusted_clock=lambda: NOW
                )

    def test_noncanonical_manifest_is_rejected(self):
        _, pins = self._write(canonical_file=False)
        with self.assertRaisesRegex(
            KisDomesticFunctionalManagerAuthorityBlocked,
            "envelope-not-exact",
        ):
            VerifyOnlyKisDomesticFunctionalManagerAuthority(
                self.path, pins=pins, trusted_clock=lambda: NOW
            )

    def test_unstable_double_read_is_rejected(self):
        _, pins = self._write()
        raw = self.path.read_bytes()
        with patch.object(Path, "read_bytes", side_effect=[raw, raw + b" "]):
            with self.assertRaisesRegex(
                KisDomesticFunctionalManagerAuthorityBlocked,
                "manifest-unstable",
            ):
                VerifyOnlyKisDomesticFunctionalManagerAuthority(
                    self.path, pins=pins, trusted_clock=lambda: NOW
                )

    def test_binding_tamper_wrong_domain_wrong_key_and_wrong_id_fail(self):
        authority = self._authority()
        bad_key = ECC.generate(curve="Ed25519")
        cases = []
        tamper = self._binding()
        tamper["reservationId"] = "reservation-2"
        cases.append(tamper)
        body = self._binding()
        body.pop("signature")
        body.pop("bindingHash")
        cases.append(
            self._signed(body, "bindingHash", RECEIPT_SIGNATURE_DOMAIN)
        )
        cases.append(
            self._signed(body, "bindingHash", BINDING_SIGNATURE_DOMAIN, key=bad_key)
        )
        wrong_id = deepcopy(body)
        wrong_id["managerKeyIdHash"] = key_hash(bad_key)
        cases.append(
            self._signed(wrong_id, "bindingHash", BINDING_SIGNATURE_DOMAIN)
        )
        for index, case in enumerate(cases):
            with self.subTest(index=index):
                self.assertFalse(authority.verify_binding(case))

    def test_receipt_domains_are_not_interchangeable(self):
        authority = self._authority()
        state = self._receipt()
        detailed = self._receipt(detailed=True)
        self.assertFalse(authority.verify_detailed_receipt(state))
        self.assertFalse(authority.verify_receipt(detailed))
        tampered = deepcopy(state)
        tampered["reservationId"] = "substituted"
        self.assertFalse(authority.verify_receipt(tampered))

    def test_manifest_is_reverified_on_every_receipt_boundary(self):
        authority = self._authority()
        self.assertTrue(authority.verify_receipt(self._receipt()))
        raw = bytearray(self.path.read_bytes())
        raw[-1] = ord(" ")
        self.path.write_bytes(bytes(raw))
        self.assertFalse(authority.verify_receipt(self._receipt()))

    def test_wrong_root_pin_and_manifest_hash_pin_fail_closed(self):
        _, pins = self._write()
        other = ECC.generate(curve="Ed25519")
        with self.subTest("root"):
            values = asdict(pins)
            values["root_public_key_pem"] = public_pem(other)
            values["root_key_id_hash"] = key_hash(other)
            with self.assertRaises(KisDomesticFunctionalManagerAuthorityBlocked):
                VerifyOnlyKisDomesticFunctionalManagerAuthority(
                    self.path,
                    pins=ManagerAuthorityPins(**values),
                    trusted_clock=lambda: NOW,
                )
        with self.subTest("file"):
            values = asdict(pins)
            values["manifest_file_hash"] = ZERO
            with self.assertRaisesRegex(
                KisDomesticFunctionalManagerAuthorityBlocked,
                "file-hash-mismatch",
            ):
                VerifyOnlyKisDomesticFunctionalManagerAuthority(
                    self.path,
                    pins=ManagerAuthorityPins(**values),
                    trusted_clock=lambda: NOW,
                )

    def test_entrypoint_remains_compile_disabled(self):
        status = production_entrypoint_status()
        self.assertTrue(status["dedicatedManagerPurposeImplemented"])
        self.assertFalse(status["productionProvisioningAvailable"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["releaseAvailable"])
        self.assertEqual(status["tradingMutationCount"], 0)


if __name__ == "__main__":
    unittest.main()
