from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "crypto_first_live_broker_credential_rewrap.py"
SPEC = importlib.util.spec_from_file_location("broker_credential_rewrap", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("credential rewrap tool is not importable")
rewrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rewrap)


class BrokerCredentialRewrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.authority_id = "supervised-authority-0001"
        self.manifest_hash = "a" * 64
        self.account = "b" * 64
        self.generation = "credential-generation-0001"

    def entropy(self, **overrides) -> bytes:
        values = {
            "authority_id": self.authority_id,
            "manifest_sha256": self.manifest_hash,
            "account_fingerprint": self.account,
            "generation_id": self.generation,
            "lane": "UPBIT",
        }
        values.update(overrides)
        return rewrap._entropy(**values)

    def test_envelope_metadata_never_contains_raw_secret(self) -> None:
        envelope = rewrap._envelope(
            lane="UPBIT",
            authority_id=self.authority_id,
            generation_id=self.generation,
            access_key="access-value",
            secret_key="secret-value",
        )
        metadata = rewrap._metadata(
            envelope, rewrap.EXACT_DESTINATIONS["UPBIT"]
        )
        encoded = json.dumps(metadata)
        self.assertNotIn("access-value", encoded)
        self.assertNotIn("secret-value", encoded)
        self.assertEqual(
            rewrap._fingerprint("UPBIT", "access-value"),
            metadata["accountFingerprint"],
        )

    def test_entropy_binds_manifest_account_generation_and_lane(self) -> None:
        baseline = self.entropy()
        self.assertNotEqual(
            baseline, self.entropy(manifest_sha256="c" * 64)
        )
        self.assertNotEqual(
            baseline, self.entropy(account_fingerprint="d" * 64)
        )
        self.assertNotEqual(
            baseline,
            self.entropy(generation_id="credential-generation-0002"),
        )
        self.assertNotEqual(baseline, self.entropy(lane="BINANCE_SPOT"))

    @unittest.skipUnless(os.name == "nt", "Windows DPAPI only")
    def test_machine_dpapi_tamper_wrong_entropy_and_replay_fail(self) -> None:
        plain = b'{"secret":"never-print"}'
        entropy = self.entropy()
        ciphertext = rewrap._protect_local_machine(plain, entropy)
        self.assertNotIn(plain, ciphertext)
        self.assertEqual(
            plain, rewrap._unprotect_local_machine(ciphertext, entropy)
        )
        with self.assertRaises(rewrap.CredentialRewrapError):
            rewrap._unprotect_local_machine(
                ciphertext, self.entropy(account_fingerprint="e" * 64)
            )
        with self.assertRaises(rewrap.CredentialRewrapError):
            rewrap._unprotect_local_machine(
                ciphertext,
                self.entropy(generation_id="credential-generation-replay"),
            )
        tampered = bytearray(ciphertext)
        tampered[len(tampered) // 2] ^= 1
        with self.assertRaises(rewrap.CredentialRewrapError):
            rewrap._unprotect_local_machine(bytes(tampered), entropy)


if __name__ == "__main__":
    unittest.main()
