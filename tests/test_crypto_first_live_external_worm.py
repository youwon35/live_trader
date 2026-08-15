from __future__ import annotations

import base64
import hashlib
import json
import unittest

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.crypto_first_live_external_worm import (
    CryptoFirstLiveExternalWormError,
    PinnedExternalWormAuthorityClient,
    SIGNATURE_DOMAIN,
    WIRE_RESPONSE_SCHEMA,
    provisioning_request,
)
from live_trader.crypto_first_live_high_water import (
    EXTERNAL_WORM_SCHEMA_VERSION,
    GLOBAL_SCOPE,
)


def canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


class ExternalWormClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.private_key = ECC.generate(curve="Ed25519")
        self.cert_hash = hashlib.sha256(b"authority-cert-der").hexdigest()
        self.tamper_signature = False
        self.tamper_cert = False
        self.transport_calls = 0

        def transport(_url, wire, *, timeout_seconds):
            self.transport_calls += 1
            self.assertLessEqual(timeout_seconds, 15)
            checkpoint = wire["checkpointRequest"]
            receipt_body = {
                "schemaVersion": EXTERNAL_WORM_SCHEMA_VERSION,
                "scope": GLOBAL_SCOPE,
                "authorityId": "external-worm-authority-0001",
                "databaseId": checkpoint["databaseId"],
                "revision": checkpoint["revision"],
                "publicationHash": checkpoint["publicationHash"],
                "checkpointId": "external-worm-checkpoint-0001",
                "checkpointHash": hashlib.sha256(
                    canonical(checkpoint)
                ).hexdigest(),
                "monotonic": True,
                "appendOnly": True,
                "worm": True,
                "durable": True,
                "restartVerifiable": True,
            }
            receipt = {
                **receipt_body,
                "receiptHash": digest(receipt_body),
            }
            signed = {
                "schemaVersion": WIRE_RESPONSE_SCHEMA,
                "namespaceId": "external-worm-namespace-0001",
                "authorityId": "external-worm-authority-0001",
                "keyId": "external-worm-key-0001",
                "requestHash": digest(wire),
                "receipt": receipt,
            }
            signature = eddsa.new(
                self.private_key, "rfc8032"
            ).sign(SIGNATURE_DOMAIN + canonical(signed))
            if self.tamper_signature:
                signature = bytes([signature[0] ^ 1]) + signature[1:]
            return {
                "status": 200,
                "peerCertificateSha256": (
                    hashlib.sha256(b"hostile-cert").hexdigest()
                    if self.tamper_cert
                    else self.cert_hash
                ),
                "json": {
                    **signed,
                    "signatureBase64": base64.b64encode(signature).decode(),
                },
            }

        self.client = PinnedExternalWormAuthorityClient(
            endpoint_url="https://worm.example.test/v1/checkpoint",
            namespace_id="external-worm-namespace-0001",
            authority_id="external-worm-authority-0001",
            key_id="external-worm-key-0001",
            public_key=self.private_key.public_key().export_key(format="PEM"),
            tls_certificate_sha256=self.cert_hash,
            transport=transport,
            clock=lambda: 2_200_000_000.0,
        )

    @staticmethod
    def checkpoint(revision: int = 0) -> dict:
        publication = (
            "" if revision == 0 else hashlib.sha256(b"publication").hexdigest()
        )
        return {
            "schemaVersion": EXTERNAL_WORM_SCHEMA_VERSION,
            "action": "OBSERVE_OR_ADVANCE",
            "purpose": "TEST_CHECKPOINT",
            "scope": GLOBAL_SCOPE,
            "databaseId": "crypto-first-live-db-external-0001",
            "priorRevision": max(0, revision - 1),
            "priorPublicationHash": "",
            "revision": revision,
            "publicationHash": publication,
            "localAnchorRevision": revision + 1,
            "localAnchorHeadHash": hashlib.sha256(
                f"anchor-{revision}".encode()
            ).hexdigest(),
        }

    def test_tls_pinned_ed25519_receipt_is_accepted(self) -> None:
        receipt = self.client(self.checkpoint())
        self.assertEqual(0, receipt["revision"])
        self.assertTrue(receipt["worm"])
        self.assertTrue(receipt["restartVerifiable"])
        self.assertEqual(1, self.transport_calls)

    def test_tls_pin_drift_fails_closed(self) -> None:
        self.tamper_cert = True
        with self.assertRaisesRegex(
            CryptoFirstLiveExternalWormError, "tls-or-status-invalid"
        ):
            self.client(self.checkpoint())

    def test_signature_drift_fails_closed(self) -> None:
        self.tamper_signature = True
        with self.assertRaisesRegex(
            CryptoFirstLiveExternalWormError, "signature-invalid"
        ):
            self.client(self.checkpoint())

    def test_provisioning_requires_pristine_revision_zero(self) -> None:
        descriptor = {
            key: value
            for key, value in self.checkpoint(1).items()
            if key not in {"schemaVersion", "action", "purpose", "scope"}
        }
        with self.assertRaisesRegex(
            CryptoFirstLiveExternalWormError, "requires-revision-zero"
        ):
            provisioning_request(descriptor)
        self.assertEqual(0, self.transport_calls)


if __name__ == "__main__":
    unittest.main()
