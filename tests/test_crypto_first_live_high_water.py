from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from live_trader.crypto_first_live_high_water import (
    CryptoFirstLiveHighWaterError,
    DurableCryptoFirstLiveHighWaterAnchor,
    GLOBAL_SCOPE,
    HIGH_WATER_SCHEMA_VERSION,
)


class DurableHighWaterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "independent" / "anchor.sqlite3"
        self.now = 2_000_000_000.0
        self.anchor = DurableCryptoFirstLiveHighWaterAnchor(
            self.path, clock=lambda: self.now
        )
        self.database_id = "crypto-first-live-db-test-00000001"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def observe(self, database_id: str | None = None):
        return self.anchor({
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "action": "REGISTER_OR_OBSERVE",
            "purpose": "TEST_OBSERVE",
            "scope": GLOBAL_SCOPE,
            "databaseId": database_id or self.database_id,
            "localRevision": 0,
            "localPublicationHash": "",
        })

    def advance(self, expected_revision: int, expected_hash: str, new_hash: str):
        return self.anchor({
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "action": "ADVANCE",
            "purpose": "TEST_ADVANCE",
            "scope": GLOBAL_SCOPE,
            "databaseId": self.database_id,
            "expectedRevision": expected_revision,
            "expectedPublicationHash": expected_hash,
            "newRevision": expected_revision + 1,
            "newPublicationHash": new_hash,
        })

    def test_register_advance_and_restart_are_durable(self) -> None:
        registered = self.observe()
        self.assertEqual(0, registered["revision"])
        publication = hashlib.sha256(b"publication-1").hexdigest()
        advanced = self.advance(0, "", publication)
        self.assertEqual(1, advanced["revision"])
        restarted = DurableCryptoFirstLiveHighWaterAnchor(
            self.path, clock=lambda: self.now
        )
        observed = restarted({
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "action": "REGISTER_OR_OBSERVE",
            "purpose": "RESTART",
            "scope": GLOBAL_SCOPE,
            "databaseId": self.database_id,
            "localRevision": 1,
            "localPublicationHash": publication,
        })
        self.assertEqual(advanced, observed)

    def test_exact_cas_allows_one_concurrent_advance(self) -> None:
        self.observe()
        hashes = [hashlib.sha256(f"p-{i}".encode()).hexdigest() for i in range(2)]
        barrier = threading.Barrier(2)
        wins, errors = [], []

        def run(publication):
            barrier.wait()
            try:
                wins.append(self.advance(0, "", publication))
            except CryptoFirstLiveHighWaterError as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=run, args=(value,)) for value in hashes]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertEqual((1, 1), (len(wins), len(errors)))
        self.assertIn("advance-cas-changed", errors[0])

    def test_replaced_database_identity_is_rejected(self) -> None:
        self.observe()
        with self.assertRaisesRegex(
            CryptoFirstLiveHighWaterError, "database-replaced"
        ):
            self.observe("crypto-first-live-db-other-00000001")

    def test_event_tamper_and_extra_schema_object_are_rejected(self) -> None:
        self.observe()
        conn = sqlite3.connect(self.path)
        conn.execute(
            "UPDATE crypto_first_live_high_water_events SET content_json='{}'"
        )
        conn.commit(); conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveHighWaterError, "event-chain-invalid"
        ):
            self.anchor.status()

        clean_path = Path(self.temp.name) / "extra.sqlite3"
        clean = DurableCryptoFirstLiveHighWaterAnchor(
            clean_path, clock=lambda: self.now
        )
        conn = sqlite3.connect(clean_path)
        conn.execute("CREATE TABLE injected_anchor(value TEXT)")
        conn.commit(); conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveHighWaterError, "sqlite-objects-mismatch"
        ):
            clean.status()

    def test_request_fields_are_exact(self) -> None:
        request = {
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "action": "REGISTER_OR_OBSERVE",
            "purpose": "TEST",
            "scope": GLOBAL_SCOPE,
            "databaseId": self.database_id,
            "localRevision": 0,
            "localPublicationHash": "",
            "unexpected": True,
        }
        with self.assertRaisesRegex(
            CryptoFirstLiveHighWaterError, "request-fields-not-exact"
        ):
            self.anchor(request)


if __name__ == "__main__":
    unittest.main()
