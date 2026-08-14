from __future__ import annotations

from contextlib import closing
import hashlib
import hmac
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from live_trader.binance_spot_functional_exclusivity import (
    API_INVENTORY_SOURCE,
    BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED,
    BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED,
    BINANCE_SPOT_ACCOUNT_WIDE_CAUSAL_AUTHORITY_AVAILABLE,
    BINANCE_SPOT_GLOBAL_FIRST_LIVE_AUTHORITY_WIRED,
    BOT_REGISTRY_SOURCE,
    CAUSAL_AUDIT_SOURCE,
    MANUAL_AUDIT_SOURCE,
    PROOF_SCHEMA_VERSION,
    PROOF_REQUEST_SCHEMA_VERSION,
    VERIFIER_PIN_SCHEMA_VERSION,
    BinanceSpotExclusivityError,
    BinanceSpotExclusivityGuard,
    DurableBinanceSpotExclusivityProofStore,
    verifier_wiring_status,
    verify_global_first_live_authority,
)


def stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


PIN = {
    "schemaVersion": VERIFIER_PIN_SCHEMA_VERSION,
    "verifierId": "binance-admin-auditor-0001",
    "keyId": "binance-audit-key-0001",
    "algorithm": "HMAC-SHA256-TEST-ONLY",
    "verifierType": "DETACHED-TEST-VERIFIER",
    "verifierCodeSha256": "1" * 64,
    "verifierConfigSha256": "2" * 64,
    "keyFingerprintSha256": "3" * 64,
    "authorityPinned": True,
}
ACCOUNT_IDENTITY = "a" * 64
CREDENTIAL = "b" * 64
PERMIT_ID = "functional-test-binance-proof-0001"
PERMIT_HASH = "c" * 64
SESSION_ID = "bnsft-proof-session-00000001"
BOUNDARY_HASH = "d" * 64
SECRET = b"unit-test-detached-verifier-secret"


class Clock:
    def __init__(self) -> None:
        self.value = 1_900_000_000.0

    def __call__(self) -> float:
        return self.value


class HmacTestVerifier:
    def __init__(self, identity: dict[str, object] | None = None) -> None:
        self._identity = dict(identity or PIN)

    def identity(self):
        return dict(self._identity)

    def __call__(self, *, payload, signature, verifier_pin):
        if dict(verifier_pin) != self._identity:
            return False
        expected = hmac.new(
            SECRET,
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)


class ProofFactory:
    def __init__(self) -> None:
        self.counter = 0
        self.mutate = None
        self.previous_proof_hash = "0" * 64

    @staticmethod
    def _component(common, schema, source, **extra):
        body = {
            **common,
            "schemaVersion": schema,
            "source": source,
            "complete": True,
            "independentlyVerified": True,
            "continuousCoverage": True,
            "authorityArtifactHash": "e" * 64,
            **extra,
        }
        return {**body, "evidenceHash": stable_hash(body)}

    def __call__(self, **request):
        self.counter += 1
        common = {
            "sessionId": request["sessionId"],
            "accountIdentityFingerprint": request[
                "accountIdentityFingerprint"
            ],
            "credentialFingerprint": request["credentialFingerprint"],
            "coverageStartedAt": request["coverageStartedAt"],
            "coverageEndedAt": request["requestedAt"],
        }
        payload = {
            "schemaVersion": PROOF_SCHEMA_VERSION,
            "proofId": f"binance-proof-{self.counter:08d}",
            "phase": request["phase"],
            "sessionId": request["sessionId"],
            "permitId": request["permitId"],
            "permitHash": request["permitHash"],
            "accountIdentityFingerprint": request[
                "accountIdentityFingerprint"
            ],
            "credentialFingerprint": request["credentialFingerprint"],
            "boundaryId": request["boundaryId"],
            "boundaryHash": request["boundaryHash"],
            "coverageStartedAt": request["coverageStartedAt"],
            "requestedAt": request["requestedAt"],
            "requireCausalClosure": request["requireCausalClosure"],
            "observedAt": request["requestedAt"],
            "authorityJournalId": "binance-authority-journal-test-0001",
            "authoritySequence": self.counter,
            "previousAuthorityProofHash": self.previous_proof_hash,
            "proofRequestHash": stable_hash(
                {"schemaVersion": PROOF_REQUEST_SCHEMA_VERSION, **request}
            ),
            "serverOwnerIdentitySha256": "6" * 64,
            "authority": dict(PIN),
            "apiCredentialInventory": self._component(
                common,
                "binance-account-api-credential-inventory-evidence/v1",
                API_INVENTORY_SOURCE,
                activeApiCredentialCount=1,
                authorizedFunctionalCredentialCount=1,
                otherActiveApiCredentialCount=0,
            ),
            "manualTradeAudit": self._component(
                common,
                "binance-account-manual-trade-audit-evidence/v1",
                MANUAL_AUDIT_SOURCE,
                manualOrderCount=0,
            ),
            "botRegistry": self._component(
                common,
                "binance-account-bot-registry-evidence/v1",
                BOT_REGISTRY_SOURCE,
                activeBotCount=1,
                authorizedFunctionalBotCount=1,
                otherActiveBotCount=0,
            ),
            "accountWideCausalAudit": self._component(
                common,
                "binance-account-wide-causal-audit-evidence/v1",
                CAUSAL_AUDIT_SOURCE,
                allSymbolsCovered=True,
                accountWideOrderEventCount=2,
                accountWideTradeEventCount=2,
                unownedOrderEventCount=0,
                unownedTradeEventCount=0,
                boundaryMarkerId=request["boundaryId"],
                boundaryMarkerHash=request["boundaryHash"],
                causalClosureProven=(request["phase"] == "TERMINAL"),
            ),
        }
        if self.mutate is not None:
            self.mutate(payload)
        payload_hash = stable_hash(payload)
        signature = hmac.new(
            SECRET,
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        proof = {**payload, "payloadHash": payload_hash, "signature": signature}
        self.previous_proof_hash = stable_hash(proof)
        return proof


class BinanceSpotFunctionalExclusivityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "proofs.sqlite3"
        self.clock = Clock()
        self.factory = ProofFactory()
        self.store = DurableBinanceSpotExclusivityProofStore(self.path)
        self.guard = BinanceSpotExclusivityGuard(
            store=self.store,
            proof_reader=self.factory,
            verifier=HmacTestVerifier(),
            verifier_pin=PIN,
            account_identity_fingerprint=ACCOUNT_IDENTITY,
            clock=self.clock,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def verify(self, phase="BASELINE", boundary="proof-boundary-0001"):
        return self.guard.verify_and_record(
            phase=phase,
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            credential_fingerprint=CREDENTIAL,
            boundary_id=boundary,
            boundary_hash=BOUNDARY_HASH,
            coverage_started_epoch=self.clock(),
            require_causal_closure=(phase == "TERMINAL"),
        )

    def test_release_and_network_authority_flags_remain_false(self) -> None:
        self.assertFalse(BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED)
        self.assertFalse(BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED)
        self.assertFalse(BINANCE_SPOT_ACCOUNT_WIDE_CAUSAL_AUTHORITY_AVAILABLE)
        self.assertFalse(BINANCE_SPOT_GLOBAL_FIRST_LIVE_AUTHORITY_WIRED)

    def test_exact_four_phase_proofs_are_signed_and_durable(self) -> None:
        for phase, boundary in (
            ("BASELINE", "proof-baseline-0001"),
            ("ACTIVATION", "proof-activation-0001"),
            ("PRE_POST", "proof-prepost-0001"),
            ("TERMINAL", "proof-terminal-0001"),
        ):
            result = self.verify(phase, boundary)
            self.assertTrue(result["verified"])
            self.assertTrue(result["durable"])
        records = self.store.session_records(SESSION_ID)
        self.assertEqual(4, len(records))
        self.assertEqual(
            {"BASELINE", "ACTIVATION", "PRE_POST", "TERMINAL"},
            {row["phase"] for row in records},
        )
        self.assertNotIn(SECRET.decode("utf-8"), json.dumps(records, default=str))

    def test_missing_or_mismatched_verifier_pin_is_hold(self) -> None:
        self.assertFalse(
            verifier_wiring_status(None, PIN, ACCOUNT_IDENTITY)["ready"]
        )
        wrong = {**PIN, "verifierCodeSha256": "9" * 64}
        self.assertFalse(
            verifier_wiring_status(
                HmacTestVerifier(), wrong, ACCOUNT_IDENTITY
            )["ready"]
        )
        self.assertFalse(
            verifier_wiring_status(HmacTestVerifier(), PIN, "")["ready"]
        )

    def test_stale_proof_fails_without_durable_record(self) -> None:
        def stale(payload):
            payload["observedAt"] = "2020-01-01T00:00:00.000000Z"
            for field in (
                "apiCredentialInventory",
                "manualTradeAudit",
                "botRegistry",
                "accountWideCausalAudit",
            ):
                payload[field]["coverageEndedAt"] = payload["observedAt"]
                body = dict(payload[field])
                body.pop("evidenceHash")
                payload[field]["evidenceHash"] = stable_hash(body)

        self.factory.mutate = stale
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "stale"):
            self.verify()
        self.assertEqual([], self.store.session_records(SESSION_ID))

    def test_session_account_credential_and_boundary_swaps_fail(self) -> None:
        for field, changed in (
            ("sessionId", "bnsft-swapped-session-000001"),
            ("permitHash", "9" * 64),
            ("accountIdentityFingerprint", "8" * 64),
            ("credentialFingerprint", "7" * 64),
            ("boundaryHash", "6" * 64),
        ):
            with self.subTest(field=field):
                self.factory.mutate = lambda payload, f=field, v=changed: payload.__setitem__(f, v)
                with self.assertRaises(BinanceSpotExclusivityError):
                    self.verify(boundary=f"swap-{field}-0001")

    def test_other_key_manual_order_or_bot_fails(self) -> None:
        mutations = (
            ("apiCredentialInventory", "otherActiveApiCredentialCount", 1),
            ("manualTradeAudit", "manualOrderCount", 1),
            ("botRegistry", "otherActiveBotCount", 1),
        )
        for index, (component, field, value) in enumerate(mutations, 1):
            with self.subTest(component=component):
                def mutate(payload, c=component, f=field, v=value):
                    payload[c][f] = v
                    body = dict(payload[c])
                    body.pop("evidenceHash")
                    payload[c]["evidenceHash"] = stable_hash(body)

                self.factory.mutate = mutate
                with self.assertRaises(BinanceSpotExclusivityError):
                    self.verify(boundary=f"activity-boundary-{index:04d}")

    def test_component_hash_and_signature_tampering_fail(self) -> None:
        self.factory.mutate = lambda payload: payload[
            "manualTradeAudit"
        ].__setitem__("evidenceHash", "0" * 64)
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "hash"):
            self.verify(boundary="tamper-component-0001")

        class BadSignatureFactory(ProofFactory):
            def __call__(self, **request):
                proof = super().__call__(**request)
                proof["signature"] = "0" * 64
                return proof

        bad = BinanceSpotExclusivityGuard(
            store=self.store,
            proof_reader=BadSignatureFactory(),
            verifier=HmacTestVerifier(),
            verifier_pin=PIN,
            account_identity_fingerprint=ACCOUNT_IDENTITY,
            clock=self.clock,
        )
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "signature"):
            bad.verify_and_record(
                phase="BASELINE",
                session_id=SESSION_ID,
                permit_id=PERMIT_ID,
                permit_hash=PERMIT_HASH,
                credential_fingerprint=CREDENTIAL,
                boundary_id="tamper-signature-0001",
                boundary_hash=BOUNDARY_HASH,
                coverage_started_epoch=self.clock(),
            )

    def test_causal_component_must_bind_the_exact_phase_boundary(self) -> None:
        def swap_marker(payload):
            component = payload["accountWideCausalAudit"]
            component["boundaryMarkerId"] = "swapped-causal-marker-0001"
            component["boundaryMarkerHash"] = "f" * 64
            body = dict(component)
            body.pop("evidenceHash")
            component["evidenceHash"] = stable_hash(body)

        self.factory.mutate = swap_marker
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "boundary"):
            self.verify(boundary="exact-causal-boundary-0001")

    def test_proof_reader_failure_is_fail_closed_and_not_durable(self) -> None:
        guard = BinanceSpotExclusivityGuard(
            store=self.store,
            proof_reader=lambda **_request: (_ for _ in ()).throw(
                TimeoutError("independent authority timed out")
            ),
            verifier=HmacTestVerifier(),
            verifier_pin=PIN,
            account_identity_fingerprint=ACCOUNT_IDENTITY,
            clock=self.clock,
        )
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "failed closed"):
            guard.verify_and_record(
                phase="BASELINE",
                session_id=SESSION_ID,
                permit_id=PERMIT_ID,
                permit_hash=PERMIT_HASH,
                credential_fingerprint=CREDENTIAL,
                boundary_id="proof-reader-failure-0001",
                boundary_hash=BOUNDARY_HASH,
                coverage_started_epoch=self.clock(),
            )
        self.assertEqual([], self.store.session_records(SESSION_ID))

    def test_terminal_causal_false_is_safe_incomplete_not_pass(self) -> None:
        def no_causal(payload):
            component = payload["accountWideCausalAudit"]
            component["causalClosureProven"] = False
            body = dict(component)
            body.pop("evidenceHash")
            component["evidenceHash"] = stable_hash(body)

        self.factory.mutate = no_causal
        result = self.guard.verify_and_record(
            phase="TERMINAL",
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            credential_fingerprint=CREDENTIAL,
            boundary_id="terminal-safe-incomplete-0001",
            boundary_hash=BOUNDARY_HASH,
            coverage_started_epoch=self.clock(),
            require_causal_closure=False,
        )
        self.assertFalse(result["accountWideCausalClosureProven"])
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "causal"):
            self.guard.verify_and_record(
                phase="TERMINAL",
                session_id=SESSION_ID,
                permit_id=PERMIT_ID,
                permit_hash=PERMIT_HASH,
                credential_fingerprint=CREDENTIAL,
                boundary_id="terminal-pass-forbidden-0001",
                boundary_hash=BOUNDARY_HASH,
                coverage_started_epoch=self.clock(),
                require_causal_closure=True,
            )

    def test_durable_row_tampering_and_phase_replacement_fail(self) -> None:
        self.verify("BASELINE", "durable-tamper-0001")
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                """UPDATE binance_spot_functional_exclusivity_proofs
                SET proof_json='{}' WHERE session_id=?""",
                (SESSION_ID,),
            )
            connection.commit()
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError, "changed|observedAt"
        ):
            self.store.session_records(SESSION_ID)

    def test_restart_reverification_rejects_rehashed_but_unsigned_tamper(self) -> None:
        result = self.verify("BASELINE", "durable-resign-tamper-0001")
        proof = json.loads(json.dumps(result["proof"]))
        component = proof["manualTradeAudit"]
        component["authorityArtifactHash"] = "9" * 64
        component_body = dict(component)
        component_body.pop("evidenceHash")
        component["evidenceHash"] = stable_hash(component_body)
        payload = {
            key: value
            for key, value in proof.items()
            if key not in {"payloadHash", "signature"}
        }
        proof["payloadHash"] = stable_hash(payload)
        forged_proof_hash = stable_hash(proof)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                """UPDATE binance_spot_functional_exclusivity_proofs
                SET proof_json=?, proof_hash=? WHERE session_id=?""",
                (
                    json.dumps(
                        proof,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    forged_proof_hash,
                    SESSION_ID,
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "signature"):
            self.guard.session_records(SESSION_ID)

    def test_durable_causal_metadata_tamper_is_rejected(self) -> None:
        self.verify("TERMINAL", "durable-causal-tamper-0001")
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                """UPDATE binance_spot_functional_exclusivity_proofs
                SET causal_closure_proven=0 WHERE session_id=?""",
                (SESSION_ID,),
            )
            connection.commit()
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "changed"):
            self.store.session_records(SESSION_ID)

    def test_global_authority_contract_is_exact_and_fresh(self) -> None:
        projection = {
            "schemaVersion": "crypto-first-live-binance-authority-snapshot/v1",
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "lane": "BINANCE_SPOT",
            "phase": "ACTIVE",
            "runId": "crypto-binance-run-0001",
            "sessionId": SESSION_ID,
            "permitId": PERMIT_ID,
            "permitHash": PERMIT_HASH,
            "accountFingerprint": CREDENTIAL,
            "ownerLeaseActive": True,
            "entryAuthorityOpen": True,
            "hardStopEpoch": self.clock() + 7200,
            "revision": 7,
            "observedEpoch": self.clock(),
        }
        snapshot = {**projection, "authorityHash": stable_hash(projection)}
        verified = verify_global_first_live_authority(
            snapshot,
            purpose="FINAL_PRE_POST",
            session_id=SESSION_ID,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            account_fingerprint=CREDENTIAL,
            cleanup_only=False,
            now_epoch=self.clock(),
        )
        self.assertTrue(verified["verified"])
        for field, changed in (
            ("lane", "UPBIT"),
            ("sessionId", "bnsft-other-session-000001"),
            ("permitHash", "9" * 64),
            ("accountFingerprint", "8" * 64),
            ("ownerLeaseActive", False),
            ("entryAuthorityOpen", False),
            ("hardStopEpoch", self.clock()),
            ("observedEpoch", self.clock() - 6),
        ):
            with self.subTest(field=field):
                changed_projection = {**projection, field: changed}
                changed_snapshot = {
                    **changed_projection,
                    "authorityHash": stable_hash(changed_projection),
                }
                with self.assertRaises(BinanceSpotExclusivityError):
                    verify_global_first_live_authority(
                        changed_snapshot,
                        purpose="FINAL_PRE_POST",
                        session_id=SESSION_ID,
                        permit_id=PERMIT_ID,
                        permit_hash=PERMIT_HASH,
                        account_fingerprint=CREDENTIAL,
                        cleanup_only=False,
                        now_epoch=self.clock(),
                    )

    def test_global_cleanup_projection_never_opens_entry(self) -> None:
        projection = {
            "schemaVersion": "crypto-first-live-binance-authority-snapshot/v1",
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "lane": "BINANCE_SPOT",
            "phase": "CLEANUP_ONLY",
            "runId": "crypto-binance-run-cleanup-0001",
            "sessionId": SESSION_ID,
            "permitId": PERMIT_ID,
            "permitHash": PERMIT_HASH,
            "accountFingerprint": CREDENTIAL,
            "ownerLeaseActive": True,
            "entryAuthorityOpen": False,
            "hardStopEpoch": self.clock(),
            "revision": 8,
            "observedEpoch": self.clock(),
        }
        snapshot = {**projection, "authorityHash": stable_hash(projection)}
        self.assertTrue(
            verify_global_first_live_authority(
                snapshot,
                purpose="CLEANUP",
                session_id=SESSION_ID,
                permit_id=PERMIT_ID,
                permit_hash=PERMIT_HASH,
                account_fingerprint=CREDENTIAL,
                cleanup_only=True,
                now_epoch=self.clock(),
            )["verified"]
        )
        with self.assertRaisesRegex(BinanceSpotExclusivityError, "entry"):
            verify_global_first_live_authority(
                snapshot,
                purpose="FINAL_PRE_POST",
                session_id=SESSION_ID,
                permit_id=PERMIT_ID,
                permit_hash=PERMIT_HASH,
                account_fingerprint=CREDENTIAL,
                cleanup_only=False,
                now_epoch=self.clock(),
            )


if __name__ == "__main__":
    unittest.main()
