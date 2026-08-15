from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

from Crypto.PublicKey import ECC

from live_trader.upbit_account_exclusivity import (
    PinnedEd25519UpbitAccountExclusivityVerifier,
    build_upbit_account_exclusivity_injection,
)
from live_trader.upbit_continuous_functional import (
    _account_exclusivity_request_payload,
    _strict_stable_hash,
    upbit_functional_session_identifier_prefix,
)
from tools.upbit_account_exclusivity_authority import (
    BUNDLE_MANIFEST_SCHEMA,
    OBSERVATION_SCHEMA,
    OUTBOX_SCHEMA,
    DurableAuthorityJournal,
    IndependentProofSigner,
    UpbitIndependentAuthorityError,
    audit_live_trader_process,
    load_authority_config,
    provision_authority,
    store_authority_credentials,
    verify_protected_authority_bundle,
)


NOW = datetime(2026, 8, 15, 2, 0, tzinfo=timezone.utc)
AUTHORITY = "TESTHOST\\upbit-authority"
TRADER = "TESTHOST\\trader"
OWNER = "a" * 64
SESSION = "upbit-functional-authority-test-0001"


class UpbitIndependentAuthorityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "workspace"
        self.trader_data = self.root / "trader-data"
        self.process_locks = self.root / "shared-process-locks"
        self.private = self.root / "authority-private"
        self.public = self.root / "authority-public"
        self.workspace.mkdir()
        self.trader_data.mkdir()
        self.process_locks.mkdir()
        self.bundle_manifest = self.root / "bundle-manifest.json"
        self.bundle_manifest.write_text("{}", encoding="utf-8")
        self.access = "upbit-authority-test-access"
        self.secret = "upbit-authority-test-secret"
        self.provision = provision_authority(
            private_root=self.private,
            public_root=self.public,
            authority_principal=AUTHORITY,
            trader_principal=TRADER,
            server_owner_identity_sha256=OWNER,
            canonical_python_executable=sys.executable,
            process_lock_directory=self.process_locks,
            workspace_root=self.workspace,
            trader_data_root=self.trader_data,
            bundle_manifest_path=self.bundle_manifest,
            secret_store_path=self.root / "authority-secrets.json",
            principal_reader=lambda: AUTHORITY,
            credential_reader=lambda _path: (self.access, self.secret),
            acl_hardener=lambda *_args: True,
            protected_bundle_verifier=self.fake_bundle_verifier,
        )
        self.config = load_authority_config(
            self.provision["authorityConfigPath"],
            principal_reader=lambda: AUTHORITY,
            protected_bundle_verifier=self.fake_bundle_verifier,
        )
        self.pin = json.loads(
            Path(self.config["verifierPinPath"]).read_text(encoding="utf-8")
        )
        self.private_key = ECC.import_key(
            Path(self.config["privateKeyPath"]).read_bytes()
        )
        self.now = NOW
        self.journal = DurableAuthorityJournal(
            database_path=self.config["databasePath"],
            proof_loss_path=self.config["proofLossPath"],
            authority_journal_id=self.config["authorityJournalId"],
            config_hash=self.config["configHash"],
            private_key=self.private_key,
            clock=lambda: self.now,
        )
        self.journal.begin_observer(coverage_started_at=NOW - timedelta(minutes=5))
        self.signer = IndependentProofSigner(
            config=self.config,
            verifier_pin=self.pin,
            private_key=self.private_key,
            journal=self.journal,
            clock=lambda: self.now,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def fake_bundle_verifier(self, **request: object) -> dict[str, object]:
        manifest = Path(str(request["manifest_path"])).resolve()
        workspace = Path(str(request["workspace_root"])).resolve()
        python = Path(str(request["canonical_python_executable"])).resolve()
        entrypoint = Path(str(request["authority_entrypoint"])).resolve()
        return {
            "schemaVersion": BUNDLE_MANIFEST_SCHEMA,
            "manifestPath": str(manifest),
            "manifestSha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            "workspaceRoot": str(workspace),
            "canonicalPythonExecutable": str(python),
            "authorityEntrypointPath": str(entrypoint),
            "authorityEntrypointSha256": hashlib.sha256(
                entrypoint.read_bytes()
            ).hexdigest(),
            "aclExclusive": True,
            "sealed": True,
            "restartVerifiable": True,
        }

    def observation(
        self,
        *,
        foreign: int = 0,
        key_count: int = 1,
        bot_count: int = 1,
        stream_gap: bool = False,
    ) -> dict[str, object]:
        return {
            "schemaVersion": OBSERVATION_SCHEMA,
            "observedAt": self.now.isoformat().replace("+00:00", "Z"),
            "apiKeyInventory": {
                "complete": True,
                "activeApiKeyCount": key_count,
                "authorizedFunctionalApiKeyCount": 1,
                "otherActiveApiKeyCount": max(0, key_count - 1),
                "expirySummaryHash": "1" * 64,
            },
            "orderAudit": {
                "complete": True,
                "ownedOrderCount": 0,
                "foreignOrderCount": foreign,
                "orderEventSummaryHash": "2" * 64,
            },
            "streamAudit": {
                "connected": not stream_gap,
                "authenticated": not stream_gap,
                "allMarketsSubscribed": True,
                "continuous": not stream_gap,
                "gapDetected": stream_gap,
                "eventCursor": 0,
                "eventHeadHash": "3" * 64,
            },
            "botAudit": {
                "complete": True,
                "activeBotCount": bot_count,
                "authorizedFunctionalBotCount": min(bot_count, 1),
                "otherActiveBotCount": max(0, bot_count - 1),
                "processAuditHash": "4" * 64,
            },
            "transport": {
                "physicalGetAttemptCount": 3,
                "authenticatedGetCount": 3,
                "retryCount": 0,
                "redirectCount": 0,
                "mutationAttemptCount": 0,
            },
        }

    def request(self, phase: str) -> tuple[dict[str, object], Path]:
        unsigned = _account_exclusivity_request_payload(
            session_id=SESSION,
            phase=phase,
            account_fingerprint=self.config["accountFingerprint"],
            credential_binding_sha256=self.config[
                "credentialBindingSha256"
            ],
            server_owner_identity_sha256=OWNER,
            session_started_at=NOW - timedelta(minutes=1),
            observation_started_at=self.now - timedelta(seconds=1),
            observed_at=self.now,
        )
        request = {
            **unsigned,
            "proofRequestHash": _strict_stable_hash(unsigned),
        }
        body = {
            "schemaVersion": OUTBOX_SCHEMA,
            "authorityJournalId": self.config["authorityJournalId"],
            "verifierPinHash": _strict_stable_hash(self.pin),
            "request": request,
        }
        envelope = {**body, "contentHash": _strict_stable_hash(body)}
        path = (
            Path(self.config["proofDirectory"])
            / f'{request["proofRequestHash"]}.request.json'
        )
        path.write_text(
            json.dumps(envelope, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        return request, path

    def admit_baseline(self) -> dict[str, object]:
        self.journal.record_observation(self.observation())
        _request, path = self.request("BASELINE")
        proof_path = self.signer.sign_request_file(path)
        return json.loads(proof_path.read_text(encoding="utf-8"))

    def test_provision_keeps_private_key_and_signer_out_of_trader_material(
        self,
    ) -> None:
        public = Path(self.provision["publicConfigPath"]).read_text(
            encoding="utf-8"
        )
        private_path = str(Path(self.config["privateKeyPath"]))
        self.assertNotIn(private_path, public)
        self.assertNotIn("privateKeyPath", public)
        self.assertNotIn("BEGIN PRIVATE KEY", public)
        self.assertTrue(self.provision["privateKeyReturned"] is False)
        self.assertNotEqual(
            Path(self.config["privateKeyPath"]).parent,
            Path(self.config["publicKeyPath"]).parent,
        )
        live_package = Path(__file__).resolve().parents[1] / "live_trader"
        live_sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in live_package.glob("*.py")
        )
        self.assertNotIn("upbit_account_exclusivity_authority", live_sources)

    def test_credential_store_is_distinct_principal_and_never_returns_values(
        self,
    ) -> None:
        class Store:
            def __init__(self, path: Path) -> None:
                self.path = path
                self.values: dict[str, str] = {}
                self.protector = SimpleNamespace(
                    name="windows-dpapi-current-user"
                )

            def get(self, key: str) -> str:
                return self.values.get(key, "")

            def set(self, key: str, value: str) -> None:
                self.values[key] = value

            def delete(self, key: str) -> None:
                self.values.pop(key, None)

        result = store_authority_credentials(
            authority_principal=AUTHORITY,
            trader_principal=TRADER,
            secret_store_path=self.root / "authority-dpapi.json",
            principal_reader=lambda: AUTHORITY,
            secret_reader=lambda: (self.access, self.secret),
            store_factory=Store,
        )
        self.assertTrue(result["stored"])
        self.assertFalse(result["secretValuesReturned"])
        self.assertEqual(0, result["networkRequestCount"])
        self.assertNotIn(self.access, json.dumps(result))
        self.assertNotIn(self.secret, json.dumps(result))

    def test_provision_rejects_same_principal_and_private_root_in_workspace(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError,
            "distinct-current-principal",
        ):
            provision_authority(
                private_root=self.root / "bad-private-1",
                public_root=self.root / "bad-public-1",
                authority_principal=AUTHORITY,
                trader_principal=AUTHORITY,
                server_owner_identity_sha256=OWNER,
                canonical_python_executable=sys.executable,
                process_lock_directory=self.process_locks,
                workspace_root=self.workspace,
                trader_data_root=self.trader_data,
                bundle_manifest_path=self.bundle_manifest,
                principal_reader=lambda: AUTHORITY,
                credential_reader=lambda _path: (self.access, self.secret),
                acl_hardener=lambda *_args: True,
            )
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError,
            "private-root-not-separated",
        ):
            provision_authority(
                private_root=self.workspace / "bad-private-2",
                public_root=self.root / "bad-public-2",
                authority_principal=AUTHORITY,
                trader_principal=TRADER,
                server_owner_identity_sha256=OWNER,
                canonical_python_executable=sys.executable,
                process_lock_directory=self.process_locks,
                workspace_root=self.workspace,
                trader_data_root=self.trader_data,
                bundle_manifest_path=self.bundle_manifest,
                principal_reader=lambda: AUTHORITY,
                credential_reader=lambda _path: (self.access, self.secret),
                acl_hardener=lambda *_args: True,
            )

    def test_protected_bundle_verifier_rejects_changed_source_extra_file_and_acl(
        self,
    ) -> None:
        authority_root = self.root / "protected-authority"
        shared_root = self.root / "protected-shared"
        app = authority_root / "app"
        venv = authority_root / "venv"
        runtime = app / "broker_authorities" / "upbit" / "runtime"
        binance_runtime = app / "broker_authorities" / "binance" / "runtime"
        scripts = venv / "Scripts"
        runtime.mkdir(parents=True)
        binance_runtime.mkdir(parents=True)
        scripts.mkdir(parents=True)
        shared_root.mkdir()
        entrypoint = runtime / "ENTRYPOINT.py"
        binance_entrypoint = binance_runtime / "ENTRYPOINT.py"
        python = scripts / "python.exe"
        entrypoint.write_bytes(b"raise SystemExit(0)\n")
        binance_entrypoint.write_bytes(b"raise SystemExit(0)\n")
        python.write_bytes(b"pinned-python-test")
        public_key = shared_root / "upbit-public.pem"
        public_key.write_bytes(b"pinned-public-test")
        secrets_root = authority_root / "secrets"
        secrets_root.mkdir()
        upbit_credential = secrets_root / "upbit-credential.dpapi"
        binance_credential = secrets_root / "binance-credential.dpapi"
        upbit_credential.write_bytes(b"upbit-credential-envelope")
        binance_credential.write_bytes(b"binance-credential-envelope")
        manifest = authority_root / "bundle-manifest.json"

        def write_manifest() -> None:
            value = {
                "schemaVersion": BUNDLE_MANIFEST_SCHEMA,
                "authorityOsSid": "S-1-5-18",
                "traderOsSid": "S-1-5-21-100-200-300-400",
                "authorityRoot": str(authority_root.resolve()),
                "sharedRoot": str(shared_root.resolve()),
                "sourcePins": {
                    "authorityToolSha256": "1" * 64,
                    "anchorModuleSha256": "2" * 64,
                    "brokerBundleDescriptorSha256": "3" * 64,
                    "credentialRewrapToolSha256": "a" * 64,
                },
                "sealedRoots": [str(app.resolve()), str(venv.resolve())],
                "files": [
                    {
                        "path": str(entrypoint.resolve()),
                        "sha256": hashlib.sha256(
                            entrypoint.read_bytes()
                        ).hexdigest(),
                    },
                    {
                        "path": str(binance_entrypoint.resolve()),
                        "sha256": hashlib.sha256(
                            binance_entrypoint.read_bytes()
                        ).hexdigest(),
                    },
                    {
                        "path": str(python.resolve()),
                        "sha256": hashlib.sha256(python.read_bytes()).hexdigest(),
                    },
                ],
                "pinnedFiles": [
                    {
                        "path": str(public_key.resolve()),
                        "sha256": hashlib.sha256(
                            public_key.read_bytes()
                        ).hexdigest(),
                    },
                ],
                "externalBinaries": [],
                "pycryptodomeWheelSha256": "4" * 64,
                "githubHostKeyRawSha256": "5" * 64,
                "remoteRef": "refs/heads/supervised-authority-test",
                "brokerBundleDescriptorSha256": "3" * 64,
                "brokerModes": [
                    {
                        "mode": "UPBIT_AUTHORITY",
                        "taskName": "CryptoFirstLive-UpbitAuthority",
                        "pipeAddress": r"\\.\pipe\upbit-authority-test",
                        "entryPoint": str(entrypoint.resolve()),
                        "importRoot": str(app.resolve()),
                        "arguments": [],
                        "environment": [],
                    },
                    {
                        "mode": "BINANCE_OBSERVER",
                        "taskName": "CryptoFirstLive-BinanceObserver",
                        "pipeAddress": r"\\.\pipe\binance-observer-test",
                        "entryPoint": str(binance_entrypoint.resolve()),
                        "importRoot": str(app.resolve()),
                        "arguments": [],
                        "environment": [],
                    },
                ],
                "brokerCredentialAuthorityId": "broker-credential-authority-test",
                "machineProtectedCredentials": [
                    {
                        "lane": "UPBIT",
                        "path": str(upbit_credential.resolve()),
                        "credentialFingerprint": "6" * 64,
                        "accountFingerprint": "6" * 64,
                        "envelopeHash": "7" * 64,
                        "credentialGenerationId": "upbit-generation-test-0001",
                    },
                    {
                        "lane": "BINANCE_SPOT",
                        "path": str(binance_credential.resolve()),
                        "credentialFingerprint": "8" * 64,
                        "accountFingerprint": "8" * 64,
                        "envelopeHash": "9" * 64,
                        "credentialGenerationId": "binance-generation-test-0001",
                    },
                ],
                "formalWorm": False,
                "promotionEligible": False,
            }
            manifest.write_text(
                json.dumps(value, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )

        good_acl = {
            "ownerSid": "S-1-5-32-544",
            "protected": True,
            "rules": [
                {
                    "sid": "S-1-5-18",
                    "type": "Allow",
                    "rights": 2032127,
                    "inherited": False,
                },
                {
                    "sid": "S-1-5-32-544",
                    "type": "Allow",
                    "rights": 2032127,
                    "inherited": False,
                },
            ],
        }
        shared_acl = {
            "ownerSid": "S-1-5-32-544",
            "protected": True,
            "rules": [
                *good_acl["rules"],
                {
                    "sid": "S-1-5-21-100-200-300-400",
                    "type": "Allow",
                    "rights": 131241,
                    "inherited": False,
                },
            ],
        }

        def acl_reader(path: Path) -> dict[str, object]:
            return shared_acl if path.resolve() == shared_root.resolve() else good_acl

        write_manifest()
        result = verify_protected_authority_bundle(
            manifest_path=manifest,
            workspace_root=app,
            canonical_python_executable=python,
            authority_entrypoint=entrypoint,
            acl_reader=acl_reader,
        )
        self.assertTrue(result["sealed"])
        self.assertTrue(result["aclExclusive"])

        entrypoint.write_bytes(b"# changed source\n")
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError, "bundle-file-changed"
        ):
            verify_protected_authority_bundle(
                manifest_path=manifest,
                workspace_root=app,
                canonical_python_executable=python,
                authority_entrypoint=entrypoint,
                acl_reader=acl_reader,
            )

        entrypoint.write_bytes(b"raise SystemExit(0)\n")
        extra = runtime / "unexpected.py"
        extra.write_bytes(b"pass\n")
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError, "extra-or-missing-file"
        ):
            verify_protected_authority_bundle(
                manifest_path=manifest,
                workspace_root=app,
                canonical_python_executable=python,
                authority_entrypoint=entrypoint,
                acl_reader=acl_reader,
            )
        extra.unlink()

        hostile_acl = {
            **good_acl,
            "rules": [
                *good_acl["rules"],
                {
                    "sid": "S-1-5-21-999",
                    "type": "Allow",
                    "rights": 1,
                    "inherited": False,
                },
            ],
        }
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError, "acl-not-exclusive"
        ):
            verify_protected_authority_bundle(
                manifest_path=manifest,
                workspace_root=app,
                canonical_python_executable=python,
                authority_entrypoint=entrypoint,
                acl_reader=lambda path: (
                    shared_acl
                    if path.resolve() == shared_root.resolve()
                    else hostile_acl
                ),
            )

    def test_signed_baseline_is_accepted_by_existing_public_only_consumer(
        self,
    ) -> None:
        proof = self.admit_baseline()
        public_key = Path(self.config["publicKeyPath"]).read_bytes()
        self.assertNotIn(b"PRIVATE", public_key)
        verifier = PinnedEd25519UpbitAccountExclusivityVerifier(
            public_key=public_key,
            verifier_id=self.config["verifierId"],
            key_id=self.config["keyId"],
            authority_journal_id=self.config["authorityJournalId"],
            expected_account_fingerprint=self.config["accountFingerprint"],
            expected_credential_binding_sha256=self.config[
                "credentialBindingSha256"
            ],
            expected_server_owner_identity_sha256=OWNER,
        )
        payload = {
            key: value
            for key, value in proof.items()
            if key not in {"payloadHash", "signature"}
        }
        self.assertTrue(
            verifier(
                payload=payload,
                signature=proof["signature"],
                verifier_pin=self.pin,
            )
        )
        injection = build_upbit_account_exclusivity_injection(
            proof_directory=self.config["proofDirectory"],
            cursor_database_path=self.public / "consumer.sqlite3",
            public_key_path=self.config["publicKeyPath"],
            verifier_pin_path=self.config["verifierPinPath"],
            verifier_id=self.config["verifierId"],
            key_id=self.config["keyId"],
            authority_journal_id=self.config["authorityJournalId"],
            expected_account_fingerprint=self.config["accountFingerprint"],
            expected_credential_binding_sha256=self.config[
                "credentialBindingSha256"
            ],
            expected_server_owner_identity_sha256=OWNER,
            account_fingerprint_reader=lambda: self.config[
                "accountFingerprint"
            ],
            credential_binding_reader=lambda: self.config[
                "credentialBindingSha256"
            ],
            server_owner_identity_reader=lambda: OWNER,
            clock=lambda: self.now,
        )
        accepted = injection.proof_reader.read_strict(
            session_id=SESSION,
            phase="BASELINE",
            account_fingerprint=self.config["accountFingerprint"],
            session_started_at=NOW - timedelta(minutes=1),
            observation_started_at=self.now - timedelta(seconds=1),
            observed_at=self.now,
        )
        self.assertEqual(proof, accepted)
        self.assertTrue(injection.status()["restartVerifiable"])

    def test_proof_chain_survives_restart_and_duplicate_request_is_idempotent(
        self,
    ) -> None:
        first = self.admit_baseline()
        self.assertEqual(1, first["authoritySequence"])
        self.now += timedelta(seconds=1)
        self.journal.record_observation(self.observation())
        _request, path = self.request("PRE_DISPATCH")
        second_path = self.signer.sign_request_file(path)
        second = json.loads(second_path.read_text(encoding="utf-8"))
        self.assertEqual(2, second["authoritySequence"])
        self.assertEqual(_strict_stable_hash(first), second["previousAuthorityProofHash"])
        self.assertEqual(second_path, self.signer.sign_request_file(path))
        restarted = DurableAuthorityJournal(
            database_path=self.config["databasePath"],
            proof_loss_path=self.config["proofLossPath"],
            authority_journal_id=self.config["authorityJournalId"],
            config_hash=self.config["configHash"],
            private_key=self.private_key,
            clock=lambda: self.now,
        )
        self.assertTrue(restarted.verify_restart())
        self.assertEqual(2, int(restarted.active_session()["last_sequence"]))

    def test_unclean_observer_restart_during_active_session_latches_loss(
        self,
    ) -> None:
        self.admit_baseline()
        restarted = DurableAuthorityJournal(
            database_path=self.config["databasePath"],
            proof_loss_path=self.config["proofLossPath"],
            authority_journal_id=self.config["authorityJournalId"],
            config_hash=self.config["configHash"],
            private_key=self.private_key,
            clock=lambda: self.now,
        )
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError,
            "unclean-observer-restart",
        ):
            restarted.begin_observer(
                coverage_started_at=self.now - timedelta(minutes=5)
            )
        self.assertEqual(
            "OBSERVER_RESTART_DURING_ACTIVE_SESSION",
            restarted.loss()["reasonCode"],
        )
        self.now += timedelta(seconds=1)
        restarted.record_observation(self.observation())
        request, _path = self.request("PRE_DISPATCH")
        signer = IndependentProofSigner(
            config=self.config,
            verifier_pin=self.pin,
            private_key=self.private_key,
            journal=restarted,
            clock=lambda: self.now,
        )
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError,
            "proof-loss",
        ):
            signer.build_proof(request)

    def test_final_proof_closes_chain_and_allows_clean_observer_stop(self) -> None:
        first = self.admit_baseline()
        self.now += timedelta(seconds=1)
        self.journal.record_observation(self.observation())
        _request, path = self.request("FINAL")
        final = json.loads(
            self.signer.sign_request_file(path).read_text(encoding="utf-8")
        )
        self.assertEqual(2, final["authoritySequence"])
        self.assertEqual(
            _strict_stable_hash(first), final["previousAuthorityProofHash"]
        )
        self.assertIsNone(self.journal.active_session())
        self.journal.end_observer()
        restarted = DurableAuthorityJournal(
            database_path=self.config["databasePath"],
            proof_loss_path=self.config["proofLossPath"],
            authority_journal_id=self.config["authorityJournalId"],
            config_hash=self.config["configHash"],
            private_key=self.private_key,
            clock=lambda: self.now,
        )
        self.assertTrue(restarted.verify_restart())
        self.assertIsNone(restarted.loss())

    def test_each_hostile_observation_latches_in_isolated_authority(self) -> None:
        cases = (
            ({"foreign": 1}, "FOREIGN_ACCOUNT_ORDER_ACTIVITY"),
            ({"stream_gap": True}, "MYORDER_STREAM_GAP"),
            ({"key_count": 2}, "API_KEY_INVENTORY_DRIFT"),
            ({"bot_count": 0}, "AUTHORIZED_BOT_PROCESS_DRIFT"),
        )
        for index, (mutation, reason) in enumerate(cases):
            with self.subTest(reason=reason):
                database = self.private / f"case-{index}.sqlite3"
                loss_path = self.private / f"case-{index}-loss.json"
                journal = DurableAuthorityJournal(
                    database_path=database,
                    proof_loss_path=loss_path,
                    authority_journal_id=self.config["authorityJournalId"],
                    config_hash=self.config["configHash"],
                    private_key=self.private_key,
                    clock=lambda: self.now,
                )
                journal.begin_observer(
                    coverage_started_at=NOW - timedelta(minutes=5)
                )
                journal.record_observation(self.observation())
                signer = IndependentProofSigner(
                    config=self.config,
                    verifier_pin=self.pin,
                    private_key=self.private_key,
                    journal=journal,
                    clock=lambda: self.now,
                )
                request, _path = self.request("BASELINE")
                signer.build_proof(request)
                self.now += timedelta(milliseconds=1)
                with self.assertRaisesRegex(
                    UpbitIndependentAuthorityError, "proof-loss"
                ):
                    journal.record_observation(self.observation(**mutation))
                self.assertEqual(reason, journal.loss()["reasonCode"])
                self.assertTrue(loss_path.is_file())

    def test_malformed_duplicate_key_and_wrong_hash_outboxes_are_rejected(
        self,
    ) -> None:
        self.journal.record_observation(self.observation())
        request, path = self.request("BASELINE")
        envelope = json.loads(path.read_text(encoding="utf-8"))
        envelope["contentHash"] = "0" * 64
        path.write_text(json.dumps(envelope), encoding="utf-8")
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError, "outbox-binding"
        ):
            self.signer.sign_request_file(path)
        duplicate = (
            '{"schemaVersion":"%s","schemaVersion":"%s"}'
            % (OUTBOX_SCHEMA, OUTBOX_SCHEMA)
        )
        duplicate_path = Path(self.config["proofDirectory"]) / (
            str(request["proofRequestHash"]) + ".request.json"
        )
        duplicate_path.write_text(duplicate, encoding="utf-8")
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError, "json-file-invalid"
        ):
            self.signer.sign_request_file(duplicate_path)

    def test_session_identifier_prefix_is_deterministic_and_session_unique(
        self,
    ) -> None:
        first = upbit_functional_session_identifier_prefix(SESSION)
        second = upbit_functional_session_identifier_prefix(SESSION + "-other")
        self.assertRegex(first, r"^uft-[0-9a-f]{8}-$")
        self.assertNotEqual(first, second)
        self.assertEqual(first, upbit_functional_session_identifier_prefix(SESSION))

    def test_missing_os_leases_reports_zero_authorized_bot(self) -> None:
        class Lease:
            def __init__(self) -> None:
                self.released = False

            def release(self) -> None:
                self.released = True

        leases: list[Lease] = []

        def acquire(_scope: str):
            lease = Lease()
            leases.append(lease)
            return lease

        result = audit_live_trader_process(
            account_fingerprint=self.config["accountFingerprint"],
            canonical_python_executable=sys.executable,
            process_reader=lambda: [],
            lease_acquirer=acquire,
            lock_root_reader=lambda: Path(self.config["processLockDirectory"]),
        )
        self.assertEqual(0, result["activeBotCount"])
        self.assertEqual(0, result["authorizedFunctionalBotCount"])
        self.assertTrue(leases[0].released)

    def test_tampered_observation_chain_fails_restart_verification(self) -> None:
        self.journal.record_observation(self.observation())
        with closing(self.journal._connect()) as connection:
            connection.execute(
                "UPDATE observation SET event_hash=? WHERE sequence=1",
                ("f" * 64,),
            )
        with self.assertRaisesRegex(
            UpbitIndependentAuthorityError, "observation-chain-tampered"
        ):
            DurableAuthorityJournal(
                database_path=self.config["databasePath"],
                proof_loss_path=self.config["proofLossPath"],
                authority_journal_id=self.config["authorityJournalId"],
                config_hash=self.config["configHash"],
                private_key=self.private_key,
                clock=lambda: self.now,
            )


if __name__ == "__main__":
    unittest.main()
