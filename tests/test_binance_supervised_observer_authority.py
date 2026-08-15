from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from live_trader.binance_spot_supervised_authority_protocol import authority_hash
from live_trader.binance_spot_supervised_observer_launch import (
    build_prearmed_observer_launch_request,
)
from live_trader.binance_spot_functional_transport import (
    binance_api_key_fingerprint,
)
import scripts.binance_supervised_observer_authority as authority
from scripts.binance_supervised_observer_daemon import (
    PREARMED_READY_SCHEMA,
    _load_protected_credentials,
)
from tests.test_binance_spot_supervised_observer_launch import (
    NOW,
    prepared_plan,
)


class FakeProcess:
    def __init__(self, pid: int = 4242) -> None:
        self.pid = pid

    def poll(self):
        return None


class BinanceSupervisedObserverAuthorityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "authority"
        self.bundle = self.root / "bundle"
        self.bundle.mkdir(parents=True)
        self.pipe_key = self.root / "pipe-auth.key"
        self.pipe_key.write_bytes(b"k" * 32)
        self.credentials = self.root / "credentials.json"
        self.credentials.write_text("{}", encoding="utf-8")
        self.private_key = self.root / "private.pem"
        self.private_key.write_text("private", encoding="ascii")
        self.manifest = self.root / "bundle-manifest.json"
        self.manifest.write_text("{}", encoding="utf-8")
        self.snapshot = Path(self.temporary.name) / "public" / "snapshot.json"
        self.config = {
            "schemaVersion": authority.AUTHORITY_CONFIG_SCHEMA,
            "authorityId": "binance-observer-authority-0001",
            "keyId": "binance-observer-key-0001",
            "authorityOsSid": "S-1-5-18",
            "traderOsSid": "S-1-5-21-100-200-300-400",
            "pipeAddress": r"\\.\pipe\binance-observer-authority-test",
            "pipeAuthKeyPath": str(self.pipe_key),
            "authorityRoot": str(self.root),
            "bundleRoot": str(self.bundle),
            "credentialFilePath": str(self.credentials),
            "privateKeyPath": str(self.private_key),
            "snapshotPath": str(self.snapshot),
            "credentialFingerprint": "2" * 64,
            "botCommandMarker": "canonical-live-trader-marker",
            "maxRuntimeSeconds": 10800,
            "pythonExecutableSha256": "4" * 64,
            "serverSourceSha256": "5" * 64,
            "daemonSourceSha256": "6" * 64,
            "bundleManifestPath": str(self.manifest),
        }
        self.credential_record = {
            "lane": "BINANCE_SPOT",
            "path": str(self.credentials),
            "credentialFingerprint": "2" * 64,
            "accountFingerprint": "2" * 64,
            "envelopeHash": "b" * 64,
            "credentialGenerationId": "binance-credential-generation-0001",
        }

    def request(self):
        return build_prearmed_observer_launch_request(
            prepared_plan(),
            authority_id=self.config["authorityId"],
            key_id=self.config["keyId"],
            authorized_trader_pid=1234,
            authorized_trader_command_sha256="3" * 64,
            clock=lambda: NOW,
        )

    def test_authority_network_release_is_compile_time_held(self) -> None:
        self.assertFalse(authority.BINANCE_OBSERVER_AUTHORITY_NETWORK_RELEASED)
        with (
            patch.object(authority, "build_observer_config") as build,
            patch.object(authority.subprocess, "Popen") as spawn,
        ):
            with self.assertRaisesRegex(
                authority.BinanceObserverAuthorityError,
                "network release is held",
            ):
                authority.build_released_observer_config(
                    self.config,
                    self.request(),
                    verified_manifest_sha256="c" * 64,
                    credential_record=self.credential_record,
                )
        build.assert_not_called()
        spawn.assert_not_called()

    def test_config_and_dynamic_observer_binding_are_exact(self) -> None:
        config = authority.validate_authority_config(self.config)
        observer = authority.build_observer_config(
            config,
            self.request(),
            verified_manifest_sha256="c" * 64,
            credential_record=self.credential_record,
        )
        self.assertEqual(self.request()["sessionId"], observer["sessionId"])
        self.assertEqual("2" * 64, observer["credentialFingerprint"])
        self.assertEqual(1234, observer["authorizedTraderPid"])
        self.assertEqual(
            "binance-credential-generation-0001",
            observer["credentialGenerationId"],
        )
        self.assertTrue(observer["ownerClientOrderPrefix"].startswith("ftb-"))

    def test_dynamic_binding_rejects_credential_swap(self) -> None:
        config = authority.validate_authority_config(self.config)
        for field, value in (("accountFingerprint", "9" * 64),):
            request = self.request()
            request[field] = value
            with self.subTest(field=field):
                with self.assertRaises(authority.BinanceObserverAuthorityError):
                    authority.build_observer_config(config, request)

    def test_pipe_peer_pid_and_sid_bind_dynamic_trader_process(self) -> None:
        config = authority.validate_authority_config(self.config)

        class Connection:
            peer_process_id = 1234
            peer_os_sid = "S-1-5-21-100-200-300-400"

        authority.verify_pipe_request_peer(Connection(), self.request(), config)
        for field, value in (
            ("peer_process_id", 9999),
            ("peer_os_sid", "S-1-5-21-999-999-999-999"),
        ):
            connection = Connection()
            setattr(connection, field, value)
            with self.subTest(field=field):
                with self.assertRaises(authority.BinanceObserverAuthorityError):
                    authority.verify_pipe_request_peer(
                        connection, self.request(), config
                    )

    def test_ack_is_zero_attempt_non_authorizing_projection(self) -> None:
        request = self.request()
        ack = authority.build_launch_ack(
            request,
            request_id="binance-observer-launch-request-0001",
            observer_process_id=4242,
            accepted_epoch=NOW + 0.25,
        )
        self.assertEqual(0, ack["signedGetAttemptCountBeforeAck"])
        self.assertEqual(0, ack["orderMutationAttemptCountBeforeAck"])
        self.assertEqual(0, ack["withdrawMutationAttemptCountBeforeAck"])
        self.assertFalse(ack["networkCapabilityOpen"])
        self.assertEqual(
            ack["ackHash"],
            authority_hash(
                {key: item for key, item in ack.items() if key != "ackHash"}
            ),
        )

    def test_ready_file_proves_child_waits_with_zero_attempts(self) -> None:
        observer = authority.build_observer_config(
            authority.validate_authority_config(self.config),
            self.request(),
            verified_manifest_sha256="c" * 64,
            credential_record=self.credential_record,
        )
        body = {
            "schemaVersion": PREARMED_READY_SCHEMA,
            "observerProcessId": 4242,
            "configHash": authority_hash(observer),
            "signedGetAttemptCount": 0,
            "mutationAttemptCount": 0,
            "networkCapabilityOpen": False,
        }
        ready = {**body, "readyHash": authority_hash(body)}
        path = self.root / "ready.json"
        path.write_text(
            json.dumps(ready, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        result = authority._read_ready(
            path,
            process=FakeProcess(),  # type: ignore[arg-type]
            observer_config=observer,
            deadline_epoch=NOW + 5,
            clock=lambda: NOW + 0.5,
        )
        self.assertEqual(0, result["signedGetAttemptCount"])

        ready["signedGetAttemptCount"] = 1
        path.write_text(
            json.dumps(ready, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        with self.assertRaises(authority.BinanceObserverAuthorityError):
            authority._read_ready(
                path,
                process=FakeProcess(),  # type: ignore[arg-type]
                observer_config=observer,
                deadline_epoch=NOW + 5,
                clock=lambda: NOW + 0.5,
            )

    def test_manifest_rehashes_every_sealed_file_and_rejects_extra(self) -> None:
        app = self.root / "app"
        venv = self.root / "venv"
        runtime = app / "broker_authorities" / "binance" / "runtime"
        scripts = runtime / "scripts"
        scripts.mkdir(parents=True)
        python = venv / "Scripts" / "python.exe"
        python.parent.mkdir(parents=True)
        server = scripts / "binance_supervised_observer_authority.py"
        daemon = scripts / "binance_supervised_observer_daemon.py"
        server.write_bytes(b"frozen-server")
        daemon.write_bytes(b"frozen-daemon")
        python.write_bytes(b"frozen-python")
        shared = self.root / "shared"
        shared.mkdir()
        config = {
            **self.config,
            "bundleRoot": str(runtime),
            "pythonExecutableSha256": authority._sha256_file(python),
            "serverSourceSha256": authority._sha256_file(server),
            "daemonSourceSha256": authority._sha256_file(daemon),
        }
        config_path = self.root / "binance-authority-config.json"
        config_path.write_text(
            json.dumps(config, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )

        def record(path: Path) -> dict[str, str]:
            return {
                "path": str(path.resolve()),
                "sha256": authority._sha256_file(path),
            }

        manifest = {
            "schemaVersion": (
                "crypto-first-live-supervised-authority-bundle-manifest/v1"
            ),
            "authorityOsSid": config["authorityOsSid"],
            "traderOsSid": config["traderOsSid"],
            "authorityRoot": str(self.root.resolve()),
            "sharedRoot": str(shared.resolve()),
            "sourcePins": [],
            "sealedRoots": [str(app.resolve()), str(venv.resolve())],
            "files": [record(server), record(daemon), record(python)],
            "pinnedFiles": [
                record(config_path),
                record(self.pipe_key),
                record(self.private_key),
            ],
            "externalBinaries": [],
            "pycryptodomeWheelSha256": "8" * 64,
            "githubHostKeyRawSha256": "9" * 64,
            "remoteRef": "refs/heads/crypto-first-live-audit",
            "brokerBundleDescriptorSha256": "a" * 64,
            "brokerCredentialAuthorityId": config["authorityId"],
            "machineProtectedCredentials": [self.credential_record],
            "brokerModes": [
                {
                    "mode": "BINANCE_OBSERVER",
                    "taskName": "CryptoFirstLive-BinanceObserver",
                    "pipeAddress": config["pipeAddress"],
                    "entryPoint": str(server.resolve()),
                    "importRoot": str(runtime.resolve()),
                    "arguments": ["--config", str(config_path.resolve())],
                    "environment": [
                        {"name": "PYTHONNOUSERSITE", "value": "1"},
                        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                        {"name": "PYTHONSAFEPATH", "value": "1"},
                    ],
                }
            ],
            "formalWorm": False,
            "promotionEligible": False,
        }
        self.manifest.write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        with patch.object(authority, "__file__", str(server)):
            result = authority.verify_bundle_manifest(
                config, config_path=config_path
            )
            self.assertEqual("BINANCE_OBSERVER", result["brokerModes"][0]["mode"])
            extra = app / "unmanifested.py"
            extra.write_bytes(b"drift")
            with self.assertRaisesRegex(
                authority.BinanceObserverAuthorityError, "missing or extra"
            ):
                authority.verify_bundle_manifest(
                    config, config_path=config_path
                )

    def test_child_environment_strips_secret_and_python_injection(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BINANCE_API_KEY": "raw-key",
                "BINANCE_API_SECRET": "raw-secret",
                "PYTHONPATH": "trader-writable",
                "PYTHONHOME": "trader-writable",
            },
            clear=False,
        ):
            child = authority._child_environment()
        self.assertNotIn("BINANCE_API_KEY", child)
        self.assertNotIn("BINANCE_API_SECRET", child)
        self.assertNotIn("PYTHONPATH", child)
        self.assertNotIn("PYTHONHOME", child)
        self.assertEqual("1", child["PYTHONNOUSERSITE"])

    def test_protected_credential_file_loads_without_console_or_command_secret(
        self,
    ) -> None:
        api_key = "protected-binance-api-key-00000001"
        fingerprint = binance_api_key_fingerprint(api_key)
        value = {
            "schemaVersion": (
                "crypto-first-live-machine-protected-broker-credential/v1"
            ),
            "authorityId": self.config["authorityId"],
            "credentialGenerationId": "binance-credential-generation-0001",
            "lane": "BINANCE_SPOT",
            "origin": "https://api.binance.com",
            "accessKey": api_key,
            "secretKey": "protected-binance-api-secret-00000001",
            "credentialFingerprint": fingerprint,
            "accountFingerprint": fingerprint,
        }
        envelope_hash = hashlib.sha256(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        value["envelopeHash"] = envelope_hash
        plaintext = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        self.credentials.write_bytes(b"machine-dpapi-ciphertext-placeholder")
        with patch.dict(os.environ, {}, clear=True):
            projection = _load_protected_credentials(
                self.credentials,
                authority_id=self.config["authorityId"],
                manifest_sha256="c" * 64,
                account_fingerprint=fingerprint,
                credential_generation_id=(
                    "binance-credential-generation-0001"
                ),
                expected_envelope_hash=envelope_hash,
                unprotect=lambda _ciphertext, _entropy: plaintext,
            )
            self.assertEqual(api_key, os.environ["BINANCE_API_KEY"])
            self.assertEqual(
                "protected-binance-api-secret-00000001",
                os.environ["BINANCE_API_SECRET"],
            )
        self.assertEqual(fingerprint, projection["credentialFingerprint"])
        self.assertNotIn("apiKey", projection)
        self.assertNotIn("apiSecret", projection)


if __name__ == "__main__":
    unittest.main()
