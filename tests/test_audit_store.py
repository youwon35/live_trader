import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from live_trader.audit_store import SQLiteAuditEventStore
from trading_runtime import build_audit_event


class SQLiteAuditEventStoreTest(unittest.TestCase):
    def test_duplicate_event_id_preserves_the_first_append(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLiteAuditEventStore(Path(temp_dir) / "audit.sqlite3")
            first = build_audit_event(
                app="live_trader",
                category="SYSTEM",
                level="INFO",
                source="unit-test",
                message="first immutable event",
                event_id="event-immutable-1",
                occurred_at=datetime(2026, 8, 1, 9, 0, 0),
                payload={"revision": 1},
            )
            duplicate = build_audit_event(
                app="live_trader",
                category="SYSTEM",
                level="ERROR",
                source="unit-test",
                message="attempted overwrite",
                event_id="event-immutable-1",
                occurred_at=datetime(2026, 8, 1, 9, 1, 0),
                payload={"revision": 2},
            )

            store.append(first)
            store.append(duplicate)

            rows = store.list_events(newest_first=False)
            stored_count = store.count()

        self.assertEqual(1, stored_count)
        self.assertEqual(1, len(rows))
        self.assertEqual("first immutable event", rows[0]["message"])
        self.assertEqual("INFO", rows[0]["level"])
        self.assertEqual({"revision": 1}, rows[0]["payload"])


if __name__ == "__main__":
    unittest.main()
