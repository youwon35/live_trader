from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_capability import (
    DurableKisDomesticFunctionalCapabilityLedger,
    KisDomesticFunctionalCapabilityBlocked,
    production_entrypoint_status,
    sign_kis_domestic_external_revoke_proof,
)
from live_trader.program_ledger import ProgramLedger


KEY = b"c" * 32
PROVIDER_KEY = b"p" * 32
PROVIDER_KEY_ID = "state-owned-revoke-provider-v1"
SHA = {key: character * 64 for key, character in zip(
    ("account", "credential", "permit", "code", "baseline", "caps", "rolling", "heartbeat", "reason", "receipt"),
    "abcdef1234",
)}


class KisCapabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.sqlite3"
        self.now = datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc)
        self.ledger = ProgramLedger(self.path)
        self.cap = DurableKisDomesticFunctionalCapabilityLedger(
            program_ledger=self.ledger, signer_key=KEY,
            signer_key_id="test-capability-key-v1", owner_id="state-owned-kis-graph-v1",
            revoke_provider_key=PROVIDER_KEY,
            revoke_provider_key_id=PROVIDER_KEY_ID,
            clock=lambda: self.now,
        )

    def mint(self):
        return self.cap.mint(
            capability_id="kis-capability-one", raw_capability="raw-capability-secret-one",
            arm_id="kis-arm-one", session_id="kis-session-one",
            account_fingerprint=SHA["account"],
            credential_configuration_hash=SHA["credential"],
            permit_id="permit-one", permit_hash=SHA["permit"],
            code_manifest_hash=SHA["code"], baseline_hash=SHA["baseline"], caps_hash=SHA["caps"],
            rolling_snapshot_hash=SHA["rolling"], heartbeat_binding_hash=SHA["heartbeat"],
        )

    def revoke_proof(self, **changes):
        body = {
            "schemaVersion": "kis-domestic-functional-capability-revoke-proof/v1",
            "provider": "STATE_OWNED_KIS_CAPABILITY_PROVIDER",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "capabilityId": "kis-capability-one",
            "sessionId": "kis-session-one",
            "accountFingerprint": SHA["account"],
            "credentialConfigurationHash": SHA["credential"],
            "revokedAt": self.now.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "providerReceiptHash": SHA["receipt"],
            "capabilityAbsentVerified": True,
            "providerKeyIdHash": __import__("hashlib").sha256(PROVIDER_KEY_ID.encode()).hexdigest(),
        }
        body.update(changes)
        return {**body, "signatureHash": sign_kis_domestic_external_revoke_proof(PROVIDER_KEY, body)}

    def test_flags_false_and_exact_bindings_mint(self):
        self.assertFalse(production_entrypoint_status()["available"])
        minted = self.mint(); self.assertEqual("ACTIVE", minted["phase"])
        lease = self.cap.authorize(raw_capability="raw-capability-secret-one", operation="NATURAL_BUY", expected_revision=1)
        self.assertTrue(lease["active"])
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT * FROM kis_capability_authority").fetchone()
        self.assertEqual(SHA["rolling"], row["rolling_snapshot_hash"])
        self.assertEqual(SHA["heartbeat"], row["heartbeat_binding_hash"])

    def test_one_route_owner_and_key_owner_are_fail_closed(self):
        self.mint()
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "already has an owner"):
            self.cap.mint(
                capability_id="kis-capability-two", raw_capability="raw-capability-secret-two",
                arm_id="arm-two", session_id="session-two", account_fingerprint=SHA["account"],
                credential_configuration_hash=SHA["credential"],
                permit_id="permit-two", permit_hash=SHA["permit"], code_manifest_hash=SHA["code"],
                baseline_hash=SHA["baseline"], caps_hash=SHA["caps"], rolling_snapshot_hash=SHA["rolling"],
                heartbeat_binding_hash=SHA["heartbeat"],
            )
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "owner/key/schema"):
            DurableKisDomesticFunctionalCapabilityLedger(
                program_ledger=ProgramLedger(self.path), signer_key=b"z" * 32,
                signer_key_id="different-key", owner_id="different-owner",
                revoke_provider_key=PROVIDER_KEY,
                revoke_provider_key_id=PROVIDER_KEY_ID,
            )

    def test_active_entry_cleanup_and_reconciliation_operations(self):
        self.mint()
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "not authorized"):
            self.cap.authorize(raw_capability="raw-capability-secret-one", operation="CLEANUP_SELL", expected_revision=1)
        cleanup = self.cap.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
        self.assertEqual("CLEANUP", cleanup["phase"])
        self.assertTrue(self.cap.authorize(raw_capability="raw-capability-secret-one", operation="CLEANUP_CANCEL", expected_revision=2)["active"])
        with self.assertRaises(KisDomesticFunctionalCapabilityBlocked):
            self.cap.authorize(raw_capability="raw-capability-secret-one", operation="NATURAL_BUY", expected_revision=2)

        # A fresh ledger shows restart/failure recovery remains cleanup-only.
        restarted = DurableKisDomesticFunctionalCapabilityLedger(
            program_ledger=ProgramLedger(self.path), signer_key=KEY,
            signer_key_id="test-capability-key-v1", owner_id="state-owned-kis-graph-v1",
            revoke_provider_key=PROVIDER_KEY,
            revoke_provider_key_id=PROVIDER_KEY_ID,
            clock=lambda: self.now,
        )
        recovery = restarted.begin_cleanup(expected_revision=2, reason_hash=SHA["reason"], reconciliation_required=True)
        self.assertEqual("RECONCILIATION_REQUIRED", recovery["phase"])
        self.assertTrue(restarted.authorize(raw_capability="raw-capability-secret-one", operation="CLEANUP_SELL", expected_revision=3)["active"])

    def test_revoke_requires_external_proof_and_then_authorizes_nothing(self):
        self.mint(); self.cap.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "proof"):
            self.cap.revoke(expected_revision=2, external_revoke_proof={})
        revoked = self.cap.revoke(
            expected_revision=2, external_revoke_proof=self.revoke_proof()
        )
        self.assertTrue(revoked["externallyRevoked"])
        for operation in ("NATURAL_BUY", "CLEANUP_CANCEL"):
            with self.assertRaises(KisDomesticFunctionalCapabilityBlocked):
                self.cap.authorize(raw_capability="raw-capability-secret-one", operation=operation, expected_revision=3)

    def test_tampered_grant_or_transition_is_rejected_on_read(self):
        self.mint()
        with self.ledger.connection() as conn:
            conn.execute("UPDATE kis_capability_authority SET baseline_hash=?", ("9" * 64,))
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "grant"):
            self.cap.status()

    def test_dirty_schema_without_primary_key_fails_before_repair(self):
        dirty_path = Path(self.temp.name) / "dirty.sqlite3"; dirty = ProgramLedger(dirty_path)
        with dirty.connection() as conn:
            conn.execute("CREATE TABLE kis_capability_authority (route TEXT, phase TEXT)")
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "schema fingerprint"):
            DurableKisDomesticFunctionalCapabilityLedger(
                program_ledger=ProgramLedger(dirty_path), signer_key=KEY,
                signer_key_id="test-capability-key-v1", owner_id="state-owned-kis-graph-v1",
                revoke_provider_key=PROVIDER_KEY,
                revoke_provider_key_id=PROVIDER_KEY_ID,
            )
        conn = sqlite3.connect(dirty_path)
        try:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kis_capability_%'")}
        finally:
            conn.close()
        self.assertEqual({"kis_capability_authority"}, tables)

    def test_provider_signature_account_credential_and_key_are_exact(self):
        self.mint(); self.cap.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
        for field, value in (
            ("accountFingerprint", "9" * 64),
            ("credentialConfigurationHash", "8" * 64),
            ("providerKeyIdHash", "7" * 64),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, field):
                    self.cap.revoke(
                        expected_revision=2,
                        external_revoke_proof=self.revoke_proof(**{field: value}),
                    )
        proof = self.revoke_proof()
        proof["signatureHash"] = "0" * 64
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "signature"):
            self.cap.revoke(expected_revision=2, external_revoke_proof=proof)

    def test_external_revoke_time_is_current_not_stale_or_future(self):
        self.mint(); self.cap.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
        for delta in (timedelta(seconds=-6), timedelta(microseconds=1)):
            timestamp = (self.now + delta).isoformat(timespec="microseconds").replace("+00:00", "Z")
            with self.subTest(delta=delta):
                with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "stale or future"):
                    self.cap.revoke(
                        expected_revision=2,
                        external_revoke_proof=self.revoke_proof(revokedAt=timestamp),
                    )

    def test_authorization_lease_contains_and_rechecks_all_sealed_bindings(self):
        self.mint()
        lease = self.cap.authorize(
            raw_capability="raw-capability-secret-one",
            operation="NATURAL_BUY",
            expected_revision=1,
        )
        for key, value in (
            ("armId", "kis-arm-one"), ("sessionId", "kis-session-one"),
            ("accountFingerprint", SHA["account"]),
            ("credentialConfigurationHash", SHA["credential"]),
            ("permitHash", SHA["permit"]), ("codeManifestHash", SHA["code"]),
            ("baselineHash", SHA["baseline"]), ("capsHash", SHA["caps"]),
            ("rollingSnapshotHash", SHA["rolling"]),
            ("heartbeatBindingHash", SHA["heartbeat"]),
        ):
            self.assertEqual(value, lease[key])
        self.assertEqual(lease, self.cap.verify_authorization_lease(lease))
        tampered = dict(lease); tampered["baselineHash"] = "9" * 64
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "signature"):
            self.cap.verify_authorization_lease(tampered)
        self.cap.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "stale"):
            self.cap.verify_authorization_lease(lease)

    def test_exact_transition_and_revoke_records_are_reconstructed(self):
        self.mint()
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT record_json FROM kis_capability_transition WHERE revision=1").fetchone()
            body = __import__("json").loads(row[0]); body["unexpected"] = True
            conn.execute(
                "UPDATE kis_capability_transition SET record_json=? WHERE revision=1",
                (__import__("json").dumps(body, sort_keys=True),),
            )
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "exact reconstruction"):
            self.cap.status()

        # Fresh ledger: provider evidence is also reconstructed from row + signature.
        other_path = Path(self.temp.name) / "revoke.sqlite3"
        other_ledger = ProgramLedger(other_path)
        other = DurableKisDomesticFunctionalCapabilityLedger(
            program_ledger=other_ledger, signer_key=KEY,
            signer_key_id="test-capability-key-v1", owner_id="state-owned-kis-graph-v1",
            revoke_provider_key=PROVIDER_KEY, revoke_provider_key_id=PROVIDER_KEY_ID,
            clock=lambda: self.now,
        )
        original = self.cap; self.cap = other
        try:
            self.mint(); other.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
            other.revoke(expected_revision=2, external_revoke_proof=self.revoke_proof())
        finally:
            self.cap = original
        with other_ledger.connection() as conn:
            conn.execute("UPDATE kis_capability_authority SET revoke_record_hash=?", ("0" * 64,))
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "revoke record"):
            other.status()

    def test_strict_revision_clock_and_stale_cas_are_fail_closed(self):
        self.mint()
        for value in (True, 0, 1.0, "1"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "revision"):
                    self.cap.authorize(
                        raw_capability="raw-capability-secret-one",
                        operation="NATURAL_BUY", expected_revision=value,
                    )
        self.cap.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "CAS"):
            self.cap.begin_cleanup(expected_revision=1, reason_hash=SHA["reason"])
        self.cap.clock = lambda: datetime(2026, 8, 14, 4, 15)
        with self.assertRaisesRegex(KisDomesticFunctionalCapabilityBlocked, "timezone-aware"):
            self.cap.authorize(
                raw_capability="raw-capability-secret-one",
                operation="CLEANUP_SELL", expected_revision=2,
            )


if __name__ == "__main__":
    unittest.main()
