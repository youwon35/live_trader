from __future__ import annotations

import base64
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.binance_spot_functional_exclusivity import (
    API_INVENTORY_SOURCE,
    BOT_REGISTRY_SOURCE,
    CAUSAL_AUDIT_SOURCE,
    MANUAL_AUDIT_SOURCE,
    PROOF_SCHEMA_VERSION,
    BinanceSpotExclusivityError,
    BinanceSpotExclusivityGuard,
    DurableBinanceSpotExclusivityProofStore,
    _stable_hash,
    _utc_text,
)
from live_trader.binance_spot_functional_exclusivity_provider import (
    BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
    BINANCE_SPOT_EXCLUSIVITY_PROVIDER_NETWORK_ALLOWED,
    BINANCE_SPOT_EXCLUSIVITY_PROVIDER_PRODUCTION_RELEASED,
    BINANCE_SPOT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT,
    PinnedEd25519BinanceSpotExclusivityVerifier,
    build_binance_spot_exclusivity_injection,
    canonical_exclusivity_signature_message,
)


ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
OWNER = "c" * 64
PERMIT_ID = "binance-provider-permit-0001"
PERMIT_HASH = "d" * 64
SESSION = "bnsft-provider-session-0001"
JOURNAL = "binance-independent-authority-journal-0001"
BOUNDARY_HASH = "e" * 64


class BinanceSpotExclusivityProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.proof_directory = root / "proofs"
        self.proof_directory.mkdir()
        self.cursor_path = root / "cursor.sqlite3"
        self.guard_path = root / "guard.sqlite3"
        self.public_path = root / "authority-public.pem"
        self.pin_path = root / "verifier-pin.json"
        self.private_key = ECC.generate(curve="Ed25519")
        self.public_path.write_text(
            self.private_key.public_key().export_key(format="PEM"),
            encoding="utf-8",
        )
        self.account = ACCOUNT
        self.credential = CREDENTIAL
        self.owner = OWNER
        self.now = 1_900_000_000.0
        self.started = self.now
        verifier = PinnedEd25519BinanceSpotExclusivityVerifier(
            public_key=self.public_path.read_bytes(),
            verifier_id="binance-independent-ed25519-verifier-0001",
            key_id="binance-independent-authority-key-0001",
            authority_journal_id=JOURNAL,
            expected_account_identity_fingerprint=ACCOUNT,
            expected_credential_fingerprint=CREDENTIAL,
            expected_server_owner_identity_sha256=OWNER,
        )
        self.pin_path.write_text(
            json.dumps(verifier.identity(), sort_keys=True), encoding="utf-8"
        )
        self.injection = build_binance_spot_exclusivity_injection(
            proof_directory=self.proof_directory,
            cursor_database_path=self.cursor_path,
            public_key_path=self.public_path,
            verifier_pin_path=self.pin_path,
            verifier_id="binance-independent-ed25519-verifier-0001",
            key_id="binance-independent-authority-key-0001",
            authority_journal_id=JOURNAL,
            expected_account_identity_fingerprint=ACCOUNT,
            expected_credential_fingerprint=CREDENTIAL,
            expected_server_owner_identity_sha256=OWNER,
            account_identity_reader=lambda: self.account,
            credential_fingerprint_reader=lambda: self.credential,
            server_owner_identity_reader=lambda: self.owner,
            clock=lambda: self.now,
        )
        self.guard = BinanceSpotExclusivityGuard(
            store=DurableBinanceSpotExclusivityProofStore(self.guard_path),
            proof_reader=self.injection.proof_reader,
            verifier=self.injection.verifier,
            verifier_pin=self.injection.verifier_pin,
            account_identity_fingerprint=(
                self.injection.account_identity_fingerprint
            ),
            clock=lambda: self.now,
        )
        self.previous = "0" * 64
        self.sequence = 0

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def build_injection(
        self,
        cursor_path: Path,
        *,
        proof_directory: Path | None = None,
    ):
        return build_binance_spot_exclusivity_injection(
            proof_directory=proof_directory or self.proof_directory,
            cursor_database_path=cursor_path,
            public_key_path=self.public_path,
            verifier_pin_path=self.pin_path,
            verifier_id="binance-independent-ed25519-verifier-0001",
            key_id="binance-independent-authority-key-0001",
            authority_journal_id=JOURNAL,
            expected_account_identity_fingerprint=ACCOUNT,
            expected_credential_fingerprint=CREDENTIAL,
            expected_server_owner_identity_sha256=OWNER,
            account_identity_reader=lambda: self.account,
            credential_fingerprint_reader=lambda: self.credential,
            server_owner_identity_reader=lambda: self.owner,
            clock=lambda: self.now,
        )

    @staticmethod
    def component(
        *,
        name: str,
        request: dict[str, object],
        causal: bool,
    ) -> dict[str, object]:
        common: dict[str, object] = {
            "sessionId": request["sessionId"],
            "accountIdentityFingerprint": request[
                "accountIdentityFingerprint"
            ],
            "credentialFingerprint": request["credentialFingerprint"],
            "coverageStartedAt": request["coverageStartedAt"],
            "coverageEndedAt": request["requestedAt"],
            "complete": True,
            "independentlyVerified": True,
            "continuousCoverage": True,
            "authorityArtifactHash": "f" * 64,
        }
        if name == "apiCredentialInventory":
            schema = "binance-account-api-credential-inventory-evidence/v1"
            source = API_INVENTORY_SOURCE
            extra: dict[str, object] = {
                "activeApiCredentialCount": 1,
                "authorizedFunctionalCredentialCount": 1,
                "otherActiveApiCredentialCount": 0,
            }
        elif name == "manualTradeAudit":
            schema = "binance-account-manual-trade-audit-evidence/v1"
            source = MANUAL_AUDIT_SOURCE
            extra = {"manualOrderCount": 0}
        elif name == "botRegistry":
            schema = "binance-account-bot-registry-evidence/v1"
            source = BOT_REGISTRY_SOURCE
            extra = {
                "activeBotCount": 1,
                "authorizedFunctionalBotCount": 1,
                "otherActiveBotCount": 0,
            }
        else:
            schema = "binance-account-wide-causal-audit-evidence/v1"
            source = CAUSAL_AUDIT_SOURCE
            extra = {
                "allSymbolsCovered": True,
                "accountWideOrderEventCount": 0,
                "accountWideTradeEventCount": 0,
                "unownedOrderEventCount": 0,
                "unownedTradeEventCount": 0,
                "boundaryMarkerId": request["boundaryId"],
                "boundaryMarkerHash": request["boundaryHash"],
                "causalClosureProven": causal,
            }
        body = {
            **common,
            "schemaVersion": schema,
            "source": source,
            **extra,
        }
        return {**body, "evidenceHash": _stable_hash(body)}

    def request(
        self,
        phase: str,
        boundary_id: str,
        *,
        require_causal: bool = False,
    ) -> dict[str, object]:
        return {
            "phase": phase,
            "sessionId": SESSION,
            "permitId": PERMIT_ID,
            "permitHash": PERMIT_HASH,
            "accountIdentityFingerprint": ACCOUNT,
            "credentialFingerprint": CREDENTIAL,
            "boundaryId": boundary_id,
            "boundaryHash": BOUNDARY_HASH,
            "coverageStartedAt": _utc_text(self.started),
            "requestedAt": _utc_text(self.now),
            "requireCausalClosure": require_causal,
        }

    def write_proof(
        self,
        request: dict[str, object],
        *,
        sequence: int | None = None,
        previous: str | None = None,
        causal: bool | None = None,
        mutate=None,
    ) -> dict[str, object]:
        descriptor = self.injection.proof_reader.request_descriptor(**request)
        self.sequence = self.sequence + 1 if sequence is None else sequence
        signed: dict[str, object] = {
            "schemaVersion": PROOF_SCHEMA_VERSION,
            "proofId": f"binance-ed25519-proof-{self.sequence:08d}",
            **request,
            "observedAt": request["requestedAt"],
            "authorityJournalId": JOURNAL,
            "authoritySequence": self.sequence,
            "previousAuthorityProofHash": (
                self.previous if previous is None else previous
            ),
            "proofRequestHash": descriptor["proofRequestHash"],
            "serverOwnerIdentitySha256": OWNER,
            "authority": dict(self.injection.verifier_pin),
        }
        causal_value = (
            request["phase"] == "TERMINAL" if causal is None else causal
        )
        for name in (
            "apiCredentialInventory",
            "manualTradeAudit",
            "botRegistry",
            "accountWideCausalAudit",
        ):
            signed[name] = self.component(
                name=name, request=request, causal=bool(causal_value)
            )
        if mutate is not None:
            mutate(signed)
        signature = eddsa.new(self.private_key, "rfc8032").sign(
            canonical_exclusivity_signature_message(signed)
        )
        proof = {
            **signed,
            "payloadHash": _stable_hash(signed),
            "signature": base64.urlsafe_b64encode(signature)
            .rstrip(b"=")
            .decode("ascii"),
        }
        path = self.proof_directory / f'{descriptor["proofRequestHash"]}.json'
        path.write_text(
            json.dumps(proof, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        self.previous = _stable_hash(proof)
        return proof

    def consume(
        self, phase: str, boundary_id: str, *, require_causal: bool = False
    ) -> dict[str, object]:
        return self.guard.verify_and_record(
            phase=phase,
            session_id=SESSION,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            credential_fingerprint=CREDENTIAL,
            boundary_id=boundary_id,
            boundary_hash=BOUNDARY_HASH,
            coverage_started_epoch=self.started,
            require_causal_closure=require_causal,
        )

    def test_public_key_injection_consumes_exact_durable_phase_chain(self) -> None:
        phases = (
            ("BASELINE", f"{SESSION}:baseline"),
            ("ACTIVATION", f"{SESSION}:activation"),
            ("PRE_POST", "claim-provider-buy-0001"),
            ("TERMINAL", f"{SESSION}:terminal"),
        )
        for index, (phase, boundary) in enumerate(phases):
            if index:
                self.now += 1
            request = self.request(phase, boundary)
            self.write_proof(request)
            result = self.consume(phase, boundary)
            self.assertTrue(result["verified"])
            self.assertTrue(result["durable"])
            self.assertTrue(result["restartVerifiable"])
        self.assertEqual(4, len(self.guard.session_records(SESSION)))
        status = self.injection.status()
        self.assertTrue(status["injectionReady"])
        self.assertTrue(status["restartVerifiable"])
        self.assertEqual(
            BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
            status["cursorSchemaFingerprint"],
        )
        self.assertTrue(status["cursorPathIdentityPinned"])
        self.assertTrue(status["asymmetricPublicKeyOnly"])
        self.assertFalse(status["signingPrimitivePresent"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertEqual(4, len(list(self.proof_directory.glob("*.request.json"))))
        self.assertEqual(
            {
                "account_exclusivity_proof_reader",
                "account_exclusivity_verifier",
                "account_exclusivity_verifier_pin",
                "account_identity_fingerprint",
            },
            set(self.injection.facade_kwargs()),
        )

    def test_release_network_and_signing_latches_remain_false(self) -> None:
        self.assertFalse(BINANCE_SPOT_EXCLUSIVITY_PROVIDER_PRODUCTION_RELEASED)
        self.assertFalse(BINANCE_SPOT_EXCLUSIVITY_PROVIDER_NETWORK_ALLOWED)
        self.assertFalse(BINANCE_SPOT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT)
        self.assertFalse(self.injection.verifier.cryptographic_verifier._public_key.has_private())

    def test_missing_proof_fails_closed_after_durable_request_only(self) -> None:
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "missing"):
            self.consume("BASELINE", f"{SESSION}:baseline")
        self.assertEqual(1, len(list(self.proof_directory.glob("*.request.json"))))
        self.assertEqual([], self.guard.session_records(SESSION))

    def test_sequence_gap_does_not_advance_cursor(self) -> None:
        baseline = self.request("BASELINE", f"{SESSION}:baseline")
        self.write_proof(baseline)
        self.consume("BASELINE", f"{SESSION}:baseline")
        self.now += 1
        activation = self.request("ACTIVATION", f"{SESSION}:activation")
        self.write_proof(activation, sequence=3)
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "discontinuous"):
            self.consume("ACTIVATION", f"{SESSION}:activation")
        self.assertEqual(
            1,
            self.injection.proof_reader.status()["verifier"][
                "runtimeIdentityMatched"
            ],
        )
        with closing(sqlite3.connect(self.cursor_path)) as connection:
            sequence = connection.execute(
                "SELECT authority_sequence FROM binance_exclusivity_cursor"
            ).fetchone()[0]
        self.assertEqual(1, sequence)

    def test_identity_rotation_fails_before_proof_consumption(self) -> None:
        request = self.request("BASELINE", f"{SESSION}:baseline")
        self.write_proof(request)
        self.credential = "9" * 64
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "rotated"):
            self.consume("BASELINE", f"{SESSION}:baseline")
        with closing(sqlite3.connect(self.cursor_path)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM binance_exclusivity_cursor"
            ).fetchone()[0]
        self.assertEqual(0, count)

    def test_cursor_tamper_breaks_restart_verification_and_next_proof(self) -> None:
        baseline = self.request("BASELINE", f"{SESSION}:baseline")
        self.write_proof(baseline)
        self.consume("BASELINE", f"{SESSION}:baseline")
        with closing(sqlite3.connect(self.cursor_path)) as connection:
            connection.execute(
                "UPDATE binance_exclusivity_cursor SET revision=99"
            )
            connection.commit()
        self.assertFalse(self.injection.status()["restartVerifiable"])
        self.now += 1
        activation = self.request("ACTIVATION", f"{SESSION}:activation")
        self.write_proof(activation)
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "tampered"):
            self.consume("ACTIVATION", f"{SESSION}:activation")

    def test_cursor_schema_extra_column_index_trigger_and_object_fail_closed(
        self,
    ) -> None:
        mutations = {
            "column_default": (
                "ALTER TABLE binance_exclusivity_cursor "
                "ADD COLUMN injected TEXT DEFAULT 'x'"
            ),
            "index": (
                "DROP INDEX "
                "binance_exclusivity_event_session_request_uq"
            ),
            "trigger": (
                "CREATE TRIGGER injected_trigger AFTER INSERT ON "
                "binance_exclusivity_cursor BEGIN SELECT 1; END"
            ),
            "foreign_object": "CREATE TABLE injected_table(value TEXT)",
        }
        root = Path(self.temporary.name)
        for suffix, statement in mutations.items():
            with self.subTest(suffix=suffix):
                cursor = root / f"cursor-{suffix}.sqlite3"
                injection = self.build_injection(cursor)
                self.assertTrue(injection.status()["restartVerifiable"])
                with closing(sqlite3.connect(cursor)) as connection:
                    connection.execute(statement)
                    connection.commit()
                status = injection.status()
                self.assertFalse(status["injectionReady"])
                self.assertFalse(status["restartVerifiable"])
                self.assertEqual("", status["cursorSchemaFingerprint"])

    def test_constructor_rejects_wrong_type_default_and_nonfresh_schema(
        self,
    ) -> None:
        cursor = Path(self.temporary.name) / "cursor-wrong-schema.sqlite3"
        with closing(sqlite3.connect(cursor)) as connection:
            connection.execute(
                """CREATE TABLE binance_exclusivity_cursor (
                   session_id INTEGER DEFAULT 'forged')"""
            )
            connection.commit()
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError, "schema fingerprint"
        ):
            self.build_injection(cursor)

    def test_schema_mutation_between_proofs_blocks_before_cursor_advance(
        self,
    ) -> None:
        baseline = self.request("BASELINE", f"{SESSION}:baseline")
        self.write_proof(baseline)
        self.consume("BASELINE", f"{SESSION}:baseline")
        with closing(sqlite3.connect(self.cursor_path)) as connection:
            connection.execute(
                """CREATE TRIGGER forged_cursor_trigger
                   AFTER UPDATE ON binance_exclusivity_cursor
                   BEGIN SELECT 1; END"""
            )
            connection.commit()
        self.assertFalse(self.injection.status()["restartVerifiable"])
        self.now += 1
        activation = self.request("ACTIVATION", f"{SESSION}:activation")
        self.write_proof(activation)
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError, "schema fingerprint"
        ):
            self.consume("ACTIVATION", f"{SESSION}:activation")
        with closing(sqlite3.connect(self.cursor_path)) as connection:
            sequence = connection.execute(
                "SELECT authority_sequence FROM binance_exclusivity_cursor"
            ).fetchone()[0]
        self.assertEqual(1, sequence)

    def test_cursor_file_replacement_is_rejected_by_pinned_identity(self) -> None:
        baseline = self.request("BASELINE", f"{SESSION}:baseline")
        self.write_proof(baseline)
        self.consume("BASELINE", f"{SESSION}:baseline")
        replacement = Path(self.temporary.name) / "replacement.sqlite3"
        shutil.copy2(self.cursor_path, replacement)
        os.replace(replacement, self.cursor_path)
        status = self.injection.status()
        self.assertFalse(status["injectionReady"])
        self.assertFalse(status["restartVerifiable"])
        self.assertFalse(status["cursorPathIdentityPinned"])
        self.now += 1
        activation = self.request("ACTIVATION", f"{SESSION}:activation")
        self.write_proof(activation)
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "replaced"):
            self.consume("ACTIVATION", f"{SESSION}:activation")

    def test_cursor_symlink_is_rejected_before_sqlite_open(self) -> None:
        target = Path(self.temporary.name) / "cursor-target.sqlite3"
        target.touch()
        link = Path(self.temporary.name) / "cursor-link.sqlite3"
        try:
            link.symlink_to(target)
        except OSError as exc:
            self.skipTest(f"symlink creation unavailable: {exc}")
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "path"):
            self.build_injection(link)

    def test_bad_pin_and_private_key_input_are_rejected(self) -> None:
        bad_pin = json.loads(self.pin_path.read_text(encoding="utf-8"))
        bad_pin["verifierConfigSha256"] = "9" * 64
        self.pin_path.write_text(json.dumps(bad_pin), encoding="utf-8")
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "pin mismatch"):
            build_binance_spot_exclusivity_injection(
                proof_directory=self.proof_directory,
                cursor_database_path=Path(self.temporary.name) / "bad.sqlite3",
                public_key_path=self.public_path,
                verifier_pin_path=self.pin_path,
                verifier_id="binance-independent-ed25519-verifier-0001",
                key_id="binance-independent-authority-key-0001",
                authority_journal_id=JOURNAL,
                expected_account_identity_fingerprint=ACCOUNT,
                expected_credential_fingerprint=CREDENTIAL,
                expected_server_owner_identity_sha256=OWNER,
                account_identity_reader=lambda: ACCOUNT,
                credential_fingerprint_reader=lambda: CREDENTIAL,
                server_owner_identity_reader=lambda: OWNER,
                clock=lambda: self.now,
            )
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "public-key-only"):
            PinnedEd25519BinanceSpotExclusivityVerifier(
                public_key=self.private_key.export_key(format="PEM"),
                verifier_id="binance-independent-ed25519-verifier-0001",
                key_id="binance-independent-authority-key-0001",
                authority_journal_id=JOURNAL,
                expected_account_identity_fingerprint=ACCOUNT,
                expected_credential_fingerprint=CREDENTIAL,
                expected_server_owner_identity_sha256=OWNER,
            )


if __name__ == "__main__":
    unittest.main()
