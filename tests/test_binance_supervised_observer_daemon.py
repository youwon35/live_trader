from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from Crypto.PublicKey import ECC

from scripts.binance_supervised_observer_daemon import (
    AuthorityJournal,
    Observer,
    main,
)


class ObserverDaemonJournalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.config = {
            "schemaVersion": "binance-supervised-observer-config/v1",
            "authorityId": "binance-supervised-authority-0001",
            "keyId": "binance-supervised-key-0001",
            "sessionId": "bnsft-observer-journal-session-0001",
            "permitId": "binance-observer-journal-permit-0001",
            "permitHash": "1" * 64,
            "credentialFingerprint": "2" * 64,
            "credentialAuthorityId": "binance-supervised-authority-0001",
            "credentialGenerationId": "binance-credential-generation-0001",
            "credentialEnvelopeHash": "4" * 64,
            "bundleManifestSha256": "5" * 64,
            "ownerClientOrderPrefix": "ftb-deadbeef1234-",
            "authorizedTraderPid": 1234,
            "authorizedTraderCommandSha256": "3" * 64,
            "botCommandMarker": "canonical-live-trader-marker",
            "maxRuntimeSeconds": 10800,
        }
        self.journal = AuthorityJournal(
            Path(self.temporary.name) / "observer-private.sqlite3",
            config=self.config,
        )
        self.official = {
            "schemaVersion": "test-official/v1",
            "observedEpoch": 1_800_000_000.0,
        }
        self.audit = {
            "schemaVersion": "test-process/v1",
            "observedEpoch": 1_800_000_000.0,
        }

    def test_event_chain_counts_owned_and_revokes_unowned_causally(self) -> None:
        self.journal.begin(official=self.official, coverage=1_800_000_000.0)
        self.journal.authenticated(
            subscribed=1_800_000_001.0, process_audit=self.audit
        )
        self.assertTrue(
            self.journal.event(
                {
                    "e": "executionReport",
                    "c": "ftb-deadbeef1234-b",
                    "i": 101,
                },
                now=1_800_000_002.0,
            )
        )
        self.assertFalse(
            self.journal.event(
                {"e": "executionReport", "c": "manual-order", "i": 202},
                now=1_800_000_003.0,
            )
        )
        row = self.journal.row()
        self.assertEqual(2, row["event_count"])
        self.assertEqual(2, row["order_event_count"])
        self.assertEqual(1, row["unowned_order_event_count"])
        self.assertNotEqual("0" * 64, row["event_chain_hash"])
        self.journal.revoke("unowned event")
        self.assertEqual(1, self.journal.row()["revoked"])

    def test_existing_unterminalized_database_is_crash_latched(self) -> None:
        self.journal.begin(official=self.official, coverage=1_800_000_000.0)
        with self.assertRaisesRegex(RuntimeError, "prior session"):
            AuthorityJournal(self.journal.path, config=self.config)
        self.assertEqual(1, self.journal.row()["crash_detected"])
        self.assertEqual(1, self.journal.row()["revoked"])

    def test_subscription_precedes_get_baseline_and_drains_unowned_event(self) -> None:
        sent: list[dict[str, object]] = []

        class Socket:
            closed = False

            def send(self, value: str) -> None:
                sent.append(json.loads(value))

            def recv(self) -> str:
                if len(sent) == 1:
                    return json.dumps(
                        {"id": sent[0]["id"], "status": 200}
                    )
                return json.dumps(
                    {
                        "event": {
                            "e": "executionReport",
                            "c": "manual-order-during-baseline",
                            "i": 303,
                        }
                    }
                )

            def close(self) -> None:
                self.closed = True

        socket = Socket()

        def official_provider() -> dict[str, object]:
            self.assertEqual(
                "userDataStream.subscribe.signature",
                sent[0]["method"],
            )
            import time

            return {
                "schemaVersion": "test-official/v1",
                "observedEpoch": time.time(),
            }

        process_audit = {
            "schemaVersion": "test-process/v1",
            "observedEpoch": 1_800_000_000.0,
        }
        websocket = types.SimpleNamespace(
            create_connection=lambda *_args, **_kwargs: socket
        )
        observer = Observer(
            config=self.config,
            journal=self.journal,
            private_key=ECC.generate(curve="Ed25519"),
            snapshot_path=Path(self.temporary.name) / "snapshot.json",
        )
        with (
            patch.dict(sys.modules, {"websocket": websocket}),
            patch(
                "scripts.binance_supervised_observer_daemon.binance_api_key_fingerprint",
                return_value=self.config["credentialFingerprint"],
            ),
            patch(
                "scripts.binance_supervised_observer_daemon.env_value",
                side_effect=lambda key: (
                    "test-api-key" if key == "BINANCE_API_KEY" else "test-secret"
                ),
            ),
            patch(
                "scripts.binance_supervised_observer_daemon._process_audit",
                return_value=process_audit,
            ),
            patch(
                "scripts.binance_supervised_observer_daemon.BinanceSpotSupervisedOfficialGetProvider",
                return_value=official_provider,
            ),
            patch(
                "scripts.binance_supervised_observer_daemon."
                "_protected_binance_spot_supervised_get_network_capability",
                return_value=object(),
            ),
        ):
            result = observer.run()
        self.assertEqual(3, result)
        self.assertEqual("userDataStream.subscribe.signature", sent[0]["method"])
        self.assertEqual("time", sent[1]["method"])
        row = self.journal.row()
        self.assertEqual(1, row["unowned_order_event_count"])
        self.assertEqual(1, row["revoked"])
        self.assertTrue(socket.closed)
        self.assertTrue((Path(self.temporary.name) / "snapshot.json").is_file())

    def test_main_loads_protected_credentials_and_waits_for_ack_gate(self) -> None:
        events: list[str] = []

        class FakeObserver:
            def __init__(self, **_kwargs: object) -> None:
                events.append("observer-constructed")

            def run(self) -> int:
                events.append("observer-run")
                return 0

        argv = [
            "binance_supervised_observer_daemon.py",
            "--config",
            str(Path(self.temporary.name) / "config.json"),
            "--database",
            str(Path(self.temporary.name) / "main-observer.sqlite3"),
            "--private-key",
            str(Path(self.temporary.name) / "private.pem"),
            "--snapshot",
            str(Path(self.temporary.name) / "snapshot.json"),
            "--credential-file",
            str(Path(self.temporary.name) / "credentials.json"),
            "--prearmed-ready-file",
            str(Path(self.temporary.name) / "ready.json"),
            "--start-gate",
            str(Path(self.temporary.name) / "start.gate"),
        ]
        with (
            patch.object(sys, "argv", argv),
            patch(
                "scripts.binance_supervised_observer_daemon._load_protected_credentials",
                side_effect=lambda _path, **_kwargs: (
                    events.append("protected-credentials-loaded")
                    or {
                        "credentialFingerprint": self.config[
                            "credentialFingerprint"
                        ],
                        "origin": "https://api.binance.com",
                    }
                ),
            ),
            patch(
                "scripts.binance_supervised_observer_daemon._strict_json",
                return_value=self.config,
            ),
            patch(
                "scripts.binance_supervised_observer_daemon._config",
                return_value=self.config,
            ),
            patch(
                "scripts.binance_supervised_observer_daemon.hold_process_lease",
                return_value={"acquired": True},
            ),
            patch(
                "scripts.binance_supervised_observer_daemon._load_private_key",
                return_value=ECC.generate(curve="Ed25519"),
            ),
            patch(
                "scripts.binance_supervised_observer_daemon._write_prearmed_ready",
                side_effect=lambda *_args, **_kwargs: events.append(
                    "prearmed-ready"
                ),
            ),
            patch(
                "scripts.binance_supervised_observer_daemon._wait_for_start_gate",
                side_effect=lambda *_args, **_kwargs: events.append(
                    "ack-gate-opened"
                ),
            ),
            patch(
                "scripts.binance_supervised_observer_daemon.Observer",
                FakeObserver,
            ),
        ):
            result = main()
        self.assertEqual(0, result)
        self.assertEqual(
            [
                "protected-credentials-loaded",
                "prearmed-ready",
                "ack-gate-opened",
                "observer-constructed",
                "observer-run",
            ],
            events,
        )


if __name__ == "__main__":
    unittest.main()
