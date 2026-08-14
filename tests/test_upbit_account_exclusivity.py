from __future__ import annotations

import base64
import copy
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.upbit_account_exclusivity import (
    PinnedEd25519UpbitAccountExclusivityVerifier,
    UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
    UPBIT_ACCOUNT_EXCLUSIVITY_NETWORK_ALLOWED,
    UPBIT_ACCOUNT_EXCLUSIVITY_PRODUCTION_RELEASED,
    UPBIT_ACCOUNT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT,
    build_upbit_account_exclusivity_injection,
    canonical_exclusivity_signature_message,
    upbit_spot_credential_binding_sha256,
)
from live_trader.upbit_continuous_functional import (
    ACCOUNT_API_KEY_INVENTORY_SOURCE,
    ACCOUNT_BOT_REGISTRY_SOURCE,
    ACCOUNT_MANUAL_TRADE_AUDIT_SOURCE,
    GLOBAL_FIRST_LIVE_AUTHORITY_SCHEMA_VERSION,
    UpbitFunctionalBlocked,
    _strict_stable_hash,
    _utc_text,
    verify_upbit_global_first_live_authority,
)


NOW = datetime(2026, 8, 15, 1, 0, tzinfo=timezone.utc)
SESSION_STARTED = NOW - timedelta(minutes=1)
ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
OWNER = "c" * 64
SESSION = "upbit-functional-session-proof-0001"
JOURNAL = "upbit-independent-authority-journal-0001"


class UpbitAccountExclusivityProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.proof_directory = root / "proofs"
        self.proof_directory.mkdir()
        self.cursor = root / "consumer.sqlite3"
        self.public_key_path = root / "authority-public.pem"
        self.pin_path = root / "verifier-pin.json"
        self.private_key = ECC.generate(curve="Ed25519")
        self.public_key_path.write_text(
            self.private_key.public_key().export_key(format="PEM"),
            encoding="utf-8",
        )
        self.account = ACCOUNT
        self.credential = CREDENTIAL
        self.owner = OWNER
        self.now = NOW
        verifier = PinnedEd25519UpbitAccountExclusivityVerifier(
            public_key=self.public_key_path.read_bytes(),
            verifier_id="upbit-independent-ed25519-verifier-0001",
            key_id="upbit-independent-authority-key-0001",
            authority_journal_id=JOURNAL,
            expected_account_fingerprint=ACCOUNT,
            expected_credential_binding_sha256=CREDENTIAL,
            expected_server_owner_identity_sha256=OWNER,
        )
        self.pin_path.write_text(
            json.dumps(verifier.identity(), sort_keys=True),
            encoding="utf-8",
        )
        self.injection = build_upbit_account_exclusivity_injection(
            proof_directory=self.proof_directory,
            cursor_database_path=self.cursor,
            public_key_path=self.public_key_path,
            verifier_pin_path=self.pin_path,
            verifier_id="upbit-independent-ed25519-verifier-0001",
            key_id="upbit-independent-authority-key-0001",
            authority_journal_id=JOURNAL,
            expected_account_fingerprint=ACCOUNT,
            expected_credential_binding_sha256=CREDENTIAL,
            expected_server_owner_identity_sha256=OWNER,
            account_fingerprint_reader=lambda: self.account,
            credential_binding_reader=lambda: self.credential,
            server_owner_identity_reader=lambda: self.owner,
            clock=lambda: self.now,
        )

    def build_with_cursor(self, cursor: Path):
        return build_upbit_account_exclusivity_injection(
            proof_directory=self.proof_directory,
            cursor_database_path=cursor,
            public_key_path=self.public_key_path,
            verifier_pin_path=self.pin_path,
            verifier_id="upbit-independent-ed25519-verifier-0001",
            key_id="upbit-independent-authority-key-0001",
            authority_journal_id=JOURNAL,
            expected_account_fingerprint=ACCOUNT,
            expected_credential_binding_sha256=CREDENTIAL,
            expected_server_owner_identity_sha256=OWNER,
            account_fingerprint_reader=lambda: self.account,
            credential_binding_reader=lambda: self.credential,
            server_owner_identity_reader=lambda: self.owner,
            clock=lambda: self.now,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def component(
        *,
        name: str,
        account: str,
        started: datetime,
        observed: datetime,
        credential: str,
        owner: str,
    ) -> dict[str, object]:
        if name == "apiKeyInventory":
            schema = "upbit-account-api-key-inventory-evidence/v1"
            source = ACCOUNT_API_KEY_INVENTORY_SOURCE
            values: dict[str, object] = {
                "activeApiKeyCount": 1,
                "authorizedFunctionalApiKeyCount": 1,
                "otherActiveApiKeyCount": 0,
                "authorizedCredentialBindingSha256": credential,
            }
        elif name == "manualTradeAudit":
            schema = "upbit-account-manual-trade-audit-evidence/v1"
            source = ACCOUNT_MANUAL_TRADE_AUDIT_SOURCE
            values = {"manualOrderCount": 0}
        else:
            schema = "upbit-account-bot-registry-evidence/v1"
            source = ACCOUNT_BOT_REGISTRY_SOURCE
            values = {
                "activeBotCount": 1,
                "authorizedFunctionalBotCount": 1,
                "otherActiveBotCount": 0,
                "authorizedServerOwnerIdentitySha256": owner,
            }
        body: dict[str, object] = {
            "schemaVersion": schema,
            "source": source,
            "accountFingerprint": account,
            "coverageStartedAt": _utc_text(started),
            "coverageEndedAt": _utc_text(observed),
            "complete": True,
            "independentlyVerified": True,
            "continuousCoverage": True,
            **values,
            "authorityArtifactHash": {
                "apiKeyInventory": "d" * 64,
                "manualTradeAudit": "e" * 64,
                "botRegistry": "f" * 64,
            }[name],
        }
        return {**body, "evidenceHash": _strict_stable_hash(body)}

    def write_proof(
        self,
        *,
        phase: str,
        observed: datetime,
        sequence: int,
        previous: str,
        mutate=None,
    ) -> tuple[dict[str, object], str]:
        descriptor = self.injection.proof_reader.request_descriptor(
            session_id=SESSION,
            phase=phase,
            account_fingerprint=ACCOUNT,
            session_started_at=SESSION_STARTED,
            observation_started_at=observed,
            observed_at=observed,
        )
        signed: dict[str, object] = {
            "schemaVersion": "upbit-functional-account-exclusivity-proof/v2",
            "sessionId": SESSION,
            "phase": phase,
            "accountFingerprint": ACCOUNT,
            "credentialBindingSha256": CREDENTIAL,
            "serverOwnerIdentitySha256": OWNER,
            "sessionStartedAt": _utc_text(SESSION_STARTED),
            "observationStartedAt": _utc_text(observed),
            "observedAt": _utc_text(observed),
            "proofRequestHash": descriptor["proofRequestHash"],
            "authorityJournalId": JOURNAL,
            "authoritySequence": sequence,
            "previousAuthorityProofHash": previous,
            "authority": dict(self.injection.verifier_pin),
            "apiKeyInventory": self.component(
                name="apiKeyInventory",
                account=ACCOUNT,
                started=SESSION_STARTED,
                observed=observed,
                credential=CREDENTIAL,
                owner=OWNER,
            ),
            "manualTradeAudit": self.component(
                name="manualTradeAudit",
                account=ACCOUNT,
                started=SESSION_STARTED,
                observed=observed,
                credential=CREDENTIAL,
                owner=OWNER,
            ),
            "botRegistry": self.component(
                name="botRegistry",
                account=ACCOUNT,
                started=SESSION_STARTED,
                observed=observed,
                credential=CREDENTIAL,
                owner=OWNER,
            ),
        }
        if mutate is not None:
            mutate(signed)
        signature = eddsa.new(self.private_key, "rfc8032").sign(
            canonical_exclusivity_signature_message(signed)
        )
        proof = {
            **signed,
            "payloadHash": _strict_stable_hash(signed),
            "signature": base64.urlsafe_b64encode(signature)
            .rstrip(b"=")
            .decode("ascii"),
        }
        path = self.proof_directory / f'{descriptor["proofRequestHash"]}.json'
        path.write_text(
            json.dumps(proof, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        return proof, descriptor["proofRequestHash"]

    def read(self, *, phase: str, observed: datetime):
        return self.injection.proof_reader.read_strict(
            session_id=SESSION,
            phase=phase,
            account_fingerprint=ACCOUNT,
            session_started_at=SESSION_STARTED,
            observation_started_at=observed,
            observed_at=observed,
        )

    def test_release_and_network_latches_remain_false(self) -> None:
        self.assertFalse(UPBIT_ACCOUNT_EXCLUSIVITY_PRODUCTION_RELEASED)
        self.assertFalse(UPBIT_ACCOUNT_EXCLUSIVITY_NETWORK_ALLOWED)
        self.assertFalse(UPBIT_ACCOUNT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT)
        status = self.injection.status()
        self.assertTrue(status["injectionReady"])
        self.assertFalse(status["liveActivationReleased"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertFalse(status["signingPrimitivePresent"])
        self.assertTrue(status["durable"])
        self.assertTrue(status["restartVerifiable"])
        self.assertEqual(
            UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
            status["cursorSchemaFingerprint"],
        )
        self.assertTrue(status["cursorPathIdentityPinned"])

    def test_exact_cursor_schema_is_restart_verifiable(self) -> None:
        restarted = self.build_with_cursor(self.cursor)
        status = restarted.status()
        self.assertTrue(status["injectionReady"])
        self.assertTrue(status["restartVerifiable"])
        self.assertEqual(
            UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
            status["cursorSchemaFingerprint"],
        )

    def test_exact_baseline_to_terminal_chain_is_durable_and_reverified(self) -> None:
        first, _ = self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        accepted = self.read(phase="BASELINE", observed=NOW)
        first_hash = _strict_stable_hash(first)
        self.assertEqual(first, accepted)
        self.assertTrue(
            self.injection.verifier(
                payload={
                    key: value
                    for key, value in first.items()
                    if key not in {"payloadHash", "signature"}
                },
                signature=first["signature"],
                verifier_pin=self.injection.verifier_pin,
            )
        )
        self.now = NOW + timedelta(seconds=1)
        final, _ = self.write_proof(
            phase="FINAL",
            observed=self.now,
            sequence=2,
            previous=first_hash,
        )
        self.assertEqual(final, self.read(phase="FINAL", observed=self.now))
        # The service establishes a private-stream barrier between two exact
        # terminal REST reads.  Both must remain chainable; only the latest
        # consumed FINAL head is accepted by terminal evidence verification.
        second_final_at = self.now + timedelta(seconds=1)
        self.now = second_final_at
        second_final, _ = self.write_proof(
            phase="FINAL",
            observed=second_final_at,
            sequence=3,
            previous=_strict_stable_hash(final),
        )
        self.assertEqual(
            second_final,
            self.read(phase="FINAL", observed=second_final_at),
        )
        status = self.injection.proof_reader.session_status(SESSION)
        self.assertTrue(status["recordHashVerified"])
        self.assertTrue(status["terminalVerified"])
        self.assertEqual(3, status["authoritySequence"])

    def test_cursor_bound_verifier_rejects_conflicting_signed_head_and_rotation(
        self,
    ) -> None:
        first, _ = self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        self.read(phase="BASELINE", observed=NOW)
        signed = {
            key: value
            for key, value in first.items()
            if key not in {"payloadHash", "signature"}
        }
        conflicting = copy.deepcopy(signed)
        component = dict(conflicting["apiKeyInventory"])
        component["authorityArtifactHash"] = "1" * 64
        component.pop("evidenceHash")
        conflicting["apiKeyInventory"] = {
            **component,
            "evidenceHash": _strict_stable_hash(component),
        }
        conflicting_signature = base64.urlsafe_b64encode(
            eddsa.new(self.private_key, "rfc8032").sign(
                canonical_exclusivity_signature_message(conflicting)
            )
        ).rstrip(b"=").decode("ascii")
        self.assertTrue(
            self.injection.verifier.cryptographic_verifier(
                payload=conflicting,
                signature=conflicting_signature,
                verifier_pin=self.injection.verifier_pin,
            )
        )
        self.assertFalse(
            self.injection.verifier(
                payload=conflicting,
                signature=conflicting_signature,
                verifier_pin=self.injection.verifier_pin,
            )
        )
        self.credential = "8" * 64
        self.assertFalse(
            self.injection.verifier(
                payload=signed,
                signature=first["signature"],
                verifier_pin=self.injection.verifier_pin,
            )
        )

    def test_missing_proof_returns_safe_incomplete_without_advancing_cursor(self) -> None:
        value = self.injection.proof_reader(
            session_id=SESSION,
            phase="BASELINE",
            account_fingerprint=ACCOUNT,
            session_started_at=SESSION_STARTED,
            observation_started_at=NOW,
            observed_at=NOW,
        )
        self.assertEqual("SAFE_INCOMPLETE", value["verificationState"])
        self.assertFalse(
            self.injection.proof_reader.session_status(SESSION)["present"]
        )
        requests = list(self.proof_directory.glob("*.request.json"))
        self.assertEqual(1, len(requests))
        request_text = requests[0].read_text(encoding="utf-8")
        self.assertIn("proofRequestHash", request_text)
        self.assertNotIn("secret", request_text.lower())

    def test_tampered_signed_payload_fails_closed(self) -> None:
        _proof, request_hash = self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        path = self.proof_directory / f"{request_hash}.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["manualTradeAudit"]["manualOrderCount"] = 1
        path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "signed-proof-invalid"
        ):
            self.read(phase="BASELINE", observed=NOW)

    def test_stale_proof_is_rejected_before_file_consumption(self) -> None:
        self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        self.now = NOW + timedelta(seconds=16)
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "stale-or-future"
        ):
            self.read(phase="BASELINE", observed=NOW)

    def test_proof_that_becomes_stale_while_waiting_is_not_consumed(self) -> None:
        self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        moments = iter((NOW, NOW + timedelta(seconds=16)))
        self.injection.proof_reader.clock = lambda: next(moments)
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "stale-or-future"
        ):
            self.read(phase="BASELINE", observed=NOW)
        self.assertFalse(
            self.injection.proof_reader.session_status(SESSION)["present"]
        )

    def test_account_and_credential_rotation_are_detected(self) -> None:
        self.account = "9" * 64
        value = self.injection.proof_reader(
            session_id=SESSION,
            phase="BASELINE",
            account_fingerprint=ACCOUNT,
            session_started_at=SESSION_STARTED,
            observation_started_at=NOW,
            observed_at=NOW,
        )
        self.assertIn("identity-rotated", value["verificationReason"])
        self.account = ACCOUNT
        self.credential = "8" * 64
        value = self.injection.proof_reader(
            session_id=SESSION,
            phase="BASELINE",
            account_fingerprint=ACCOUNT,
            session_started_at=SESSION_STARTED,
            observation_started_at=NOW,
            observed_at=NOW,
        )
        self.assertIn("identity-rotated", value["verificationReason"])

    def test_sequence_gap_and_replay_are_rejected(self) -> None:
        first, _ = self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        self.read(phase="BASELINE", observed=NOW)
        self.now = NOW + timedelta(seconds=1)
        self.write_proof(
            phase="FINAL_PRE_POST",
            observed=self.now,
            sequence=3,
            previous=_strict_stable_hash(first),
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "chain-discontinuity"
        ):
            self.read(phase="FINAL_PRE_POST", observed=self.now)

    def test_cursor_and_event_tamper_are_detected(self) -> None:
        first, _ = self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        self.read(phase="BASELINE", observed=NOW)
        with closing(sqlite3.connect(self.cursor)) as connection:
            connection.execute(
                "UPDATE upbit_exclusivity_cursor SET revision=99"
            )
            connection.commit()
        self.assertFalse(self.injection.status()["restartVerifiable"])
        self.assertFalse(self.injection.status()["injectionReady"])
        self.now = NOW + timedelta(seconds=1)
        self.write_proof(
            phase="FINAL_PRE_POST",
            observed=self.now,
            sequence=2,
            previous=_strict_stable_hash(first),
        )
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "cursor-tampered"):
            self.read(phase="FINAL_PRE_POST", observed=self.now)

    def test_constructor_rejects_nonexact_existing_cursor_schema(self) -> None:
        cursor = Path(self.temporary.name) / "malformed.sqlite3"
        with closing(sqlite3.connect(cursor)) as connection:
            connection.execute(
                """CREATE TABLE upbit_exclusivity_cursor (
                       session_id BLOB NOT NULL DEFAULT 'attacker'
                   )"""
            )
            connection.commit()
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "schema-fingerprint-invalid"
        ):
            self.build_with_cursor(cursor)

    def test_every_status_read_rejects_schema_object_and_column_tamper(self) -> None:
        mutations = {
            "extra-column-default": (
                "ALTER TABLE upbit_exclusivity_cursor "
                "ADD COLUMN attacker TEXT DEFAULT 'x'"
            ),
            "extra-index": (
                "CREATE INDEX attacker_index "
                "ON upbit_exclusivity_cursor(account_fingerprint)"
            ),
            "extra-trigger": (
                "CREATE TRIGGER attacker_trigger AFTER INSERT ON "
                "upbit_exclusivity_cursor BEGIN SELECT 1; END"
            ),
            "foreign-table": "CREATE TABLE attacker_object(value TEXT)",
        }
        for index, (label, statement) in enumerate(mutations.items(), 1):
            with self.subTest(label=label):
                cursor = Path(self.temporary.name) / f"tampered-{index}.sqlite3"
                injection = self.build_with_cursor(cursor)
                with closing(sqlite3.connect(cursor)) as connection:
                    connection.execute(statement)
                    connection.commit()
                with self.assertRaisesRegex(
                    UpbitFunctionalBlocked, "schema-fingerprint-invalid"
                ):
                    injection.proof_reader.session_status(SESSION)
                status = injection.status()
                self.assertFalse(status["restartVerifiable"])
                self.assertFalse(status["injectionReady"])

    def test_cursor_mutation_revalidates_schema_before_consuming_proof(self) -> None:
        self.write_proof(
            phase="BASELINE", observed=NOW, sequence=1, previous="0" * 64
        )
        with closing(sqlite3.connect(self.cursor)) as connection:
            connection.execute("CREATE TABLE attacker_object(value TEXT)")
            connection.commit()
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "schema-fingerprint-invalid"
        ):
            self.read(phase="BASELINE", observed=NOW)
        with closing(sqlite3.connect(self.cursor)) as connection:
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM upbit_exclusivity_cursor"
                ).fetchone()[0]
            )
        self.assertEqual(0, count)

    def test_every_status_read_rejects_type_default_and_pragma_tamper(self) -> None:
        mutations = {
            "wrong-type": (
                "account_fingerprint TEXT NOT NULL",
                "account_fingerprint BLOB NOT NULL",
            ),
            "wrong-default": (
                "account_fingerprint TEXT NOT NULL",
                "account_fingerprint TEXT NOT NULL DEFAULT 'x'",
            ),
        }
        for index, (label, replacements) in enumerate(mutations.items(), 1):
            with self.subTest(label=label):
                cursor = Path(self.temporary.name) / f"metadata-{index}.sqlite3"
                injection = self.build_with_cursor(cursor)
                with closing(sqlite3.connect(cursor)) as connection:
                    connection.execute("PRAGMA writable_schema=ON")
                    row = connection.execute(
                        """SELECT sql FROM sqlite_master
                           WHERE name='upbit_exclusivity_cursor'"""
                    ).fetchone()
                    changed = str(row[0]).replace(*replacements)
                    connection.execute(
                        """UPDATE sqlite_master SET sql=?
                           WHERE name='upbit_exclusivity_cursor'""",
                        (changed,),
                    )
                    connection.execute("PRAGMA writable_schema=OFF")
                    version = int(
                        connection.execute(
                            "PRAGMA schema_version"
                        ).fetchone()[0]
                    )
                    connection.execute(f"PRAGMA schema_version={version + 1}")
                    connection.commit()
                with self.assertRaisesRegex(
                    UpbitFunctionalBlocked, "schema-fingerprint-invalid"
                ):
                    injection.proof_reader.session_status(SESSION)

        cursor = Path(self.temporary.name) / "pragma-tampered.sqlite3"
        injection = self.build_with_cursor(cursor)
        with closing(sqlite3.connect(cursor)) as connection:
            connection.execute("PRAGMA user_version=99")
            connection.commit()
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "schema-fingerprint-invalid"
        ):
            injection.proof_reader.session_status(SESSION)

    def test_cursor_symlink_drift_and_file_replacement_fail_closed(self) -> None:
        cursor = Path(self.temporary.name) / "path-pinned.sqlite3"
        injection = self.build_with_cursor(cursor)
        original_is_symlink = Path.is_symlink

        def cursor_is_symlink(path: Path) -> bool:
            return bool(
                path.absolute() == cursor.absolute()
                or original_is_symlink(path)
            )

        with patch.object(
            Path,
            "is_symlink",
            autospec=True,
            side_effect=cursor_is_symlink,
        ):
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "cursor-path-drift"
            ):
                injection.proof_reader.session_status(SESSION)

        replacement_cursor = (
            Path(self.temporary.name) / "replacement.sqlite3"
        )
        replacement = self.build_with_cursor(replacement_cursor)
        self.assertTrue(replacement.status()["restartVerifiable"])
        os.replace(replacement_cursor, cursor)
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "cursor-file-replaced"
        ):
            injection.proof_reader.session_status(SESSION)
        self.assertFalse(injection.status()["restartVerifiable"])

    def test_constructor_rejects_raw_cursor_symlink(self) -> None:
        cursor = Path(self.temporary.name) / "linked.sqlite3"
        original_is_symlink = Path.is_symlink

        def cursor_is_symlink(path: Path) -> bool:
            return bool(
                path.absolute() == cursor.absolute()
                or original_is_symlink(path)
            )

        with patch.object(
            Path,
            "is_symlink",
            autospec=True,
            side_effect=cursor_is_symlink,
        ):
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "cursor-path-invalid"
            ):
                self.build_with_cursor(cursor)

    def test_durable_pin_mismatch_and_private_key_input_are_rejected(self) -> None:
        bad_pin = json.loads(self.pin_path.read_text(encoding="utf-8"))
        bad_pin["verifierConfigSha256"] = "1" * 64
        self.pin_path.write_text(json.dumps(bad_pin), encoding="utf-8")
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "pin-mismatch"):
            build_upbit_account_exclusivity_injection(
                proof_directory=self.proof_directory,
                cursor_database_path=Path(self.temporary.name) / "bad.sqlite3",
                public_key_path=self.public_key_path,
                verifier_pin_path=self.pin_path,
                verifier_id="upbit-independent-ed25519-verifier-0001",
                key_id="upbit-independent-authority-key-0001",
                authority_journal_id=JOURNAL,
                expected_account_fingerprint=ACCOUNT,
                expected_credential_binding_sha256=CREDENTIAL,
                expected_server_owner_identity_sha256=OWNER,
                account_fingerprint_reader=lambda: ACCOUNT,
                credential_binding_reader=lambda: CREDENTIAL,
                server_owner_identity_reader=lambda: OWNER,
                clock=lambda: NOW,
            )
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "public-key-only"):
            PinnedEd25519UpbitAccountExclusivityVerifier(
                public_key=self.private_key.export_key(format="PEM"),
                verifier_id="upbit-independent-ed25519-verifier-0001",
                key_id="upbit-independent-authority-key-0001",
                authority_journal_id=JOURNAL,
                expected_account_fingerprint=ACCOUNT,
                expected_credential_binding_sha256=CREDENTIAL,
                expected_server_owner_identity_sha256=OWNER,
            )

    def test_credential_binding_changes_on_secret_rotation(self) -> None:
        first = upbit_spot_credential_binding_sha256("access", "secret-1")
        second = upbit_spot_credential_binding_sha256("access", "secret-2")
        self.assertRegex(first, r"^[0-9a-f]{64}$")
        self.assertNotEqual(first, second)
        self.assertNotEqual(
            first,
            upbit_spot_credential_binding_sha256("access ", "secret-1"),
        )
        self.assertEqual("", upbit_spot_credential_binding_sha256("", "x"))


class UpbitGlobalFirstLiveFenceContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.now = NOW
        self.scope = SimpleNamespace(
            permit_id="functional-permit-upbit-0001",
            permit_hash="1" * 64,
            account_fingerprint=ACCOUNT,
            route_scope_hash="2" * 64,
            ends_at=NOW + timedelta(hours=2),
        )
        self.session = "upbit-functional-session-global-0001"

    def reader(self, request):
        body = {
            "schemaVersion": GLOBAL_FIRST_LIVE_AUTHORITY_SCHEMA_VERSION,
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "lane": "UPBIT",
            "phase": "CLEANUP_ONLY" if request["cleanup"] else "ACTIVE",
            "runId": "crypto-first-live-upbit-run-0001",
            "sessionId": self.session,
            "permitId": self.scope.permit_id,
            "permitHash": self.scope.permit_hash,
            "accountFingerprint": ACCOUNT,
            "routeScopeHash": self.scope.route_scope_hash,
            "ownerIdentityHash": OWNER,
            "ownerLeaseActive": True,
            "entryAuthorityOpen": not request["cleanup"],
            "cleanupAuthorityOpen": request["cleanup"],
            "hardStopEpoch": self.scope.ends_at.timestamp(),
            "ownerLeaseExpiresEpoch": self.now.timestamp() + 30,
            "revision": 7,
            "observedEpoch": self.now.timestamp(),
            "killSwitch": False,
            "stopRequested": False,
        }
        return {**body, "authorityHash": _strict_stable_hash(body)}

    def verify(self, *, cleanup=False, reader=None):
        return verify_upbit_global_first_live_authority(
            reader or self.reader,
            scope=self.scope,
            session_id=self.session,
            owner_identity_hash=OWNER,
            action="CLEANUP_SELL" if cleanup else "STRATEGY_BUY",
            cleanup=cleanup,
            now=self.now,
            claim_id="claim-global-upbit-0001",
            request_hash="3" * 64,
        )

    def test_natural_entry_and_cleanup_contracts_are_distinct(self) -> None:
        self.assertTrue(self.verify()["entryAuthorityOpen"])
        cleanup = self.verify(cleanup=True)
        self.assertFalse(cleanup["entryAuthorityOpen"])
        self.assertTrue(cleanup["cleanupAuthorityOpen"])

    def test_tamper_stale_and_identity_rotation_fail_closed(self) -> None:
        def tampered(request):
            value = self.reader(request)
            value["entryAuthorityOpen"] = False
            return value

        with self.assertRaisesRegex(UpbitFunctionalBlocked, "integrity-invalid"):
            self.verify(reader=tampered)

        def stale(request):
            value = self.reader(request)
            body = {key: item for key, item in value.items() if key != "authorityHash"}
            body["observedEpoch"] = self.now.timestamp() - 6
            return {**body, "authorityHash": _strict_stable_hash(body)}

        with self.assertRaisesRegex(UpbitFunctionalBlocked, "stale-or-expired"):
            self.verify(reader=stale)

        def rotated(request):
            value = self.reader(request)
            body = {key: item for key, item in value.items() if key != "authorityHash"}
            body["accountFingerprint"] = "9" * 64
            return {**body, "authorityHash": _strict_stable_hash(body)}

        with self.assertRaisesRegex(UpbitFunctionalBlocked, "identity-mismatch"):
            self.verify(reader=rotated)


if __name__ == "__main__":
    unittest.main()
