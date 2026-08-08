from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from live_trader.program_ledger import ProgramLedger


class FunctionalTestEquityLedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "program-ledger.sqlite3"
        self.ledger = ProgramLedger(self.path)
        self.permit_id = "permit-multiday"
        self.account_fingerprint = "a" * 64

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def observe(
        self,
        equity: float,
        observed_at: str,
        *,
        allow_create: bool = False,
    ) -> dict[str, object]:
        return self.ledger.observe_functional_test_equity(
            permit_id=self.permit_id,
            account_fingerprint=self.account_fingerprint,
            current_equity=equity,
            observed_at=observed_at,
            allow_create=allow_create,
        )

    def test_multiday_worst_drawdown_survives_restart_and_daily_reset(self) -> None:
        first = self.observe(
            1_000_000,
            "2026-08-05T01:00:00+00:00",
            allow_create=True,
        )
        self.assertEqual(1_000_000, first["starting_equity"])
        self.observe(1_100_000, "2026-08-05T02:00:00+00:00")
        day_one = self.observe(950_000, "2026-08-05T03:00:00+00:00")
        self.assertEqual(150_000, day_one["worst_drawdown"])

        reloaded = ProgramLedger(self.path)
        day_two = reloaded.observe_functional_test_equity(
            permit_id=self.permit_id,
            account_fingerprint=self.account_fingerprint,
            current_equity=1_020_000,
            observed_at="2026-08-06T01:00:00+00:00",
        )
        self.assertEqual(1_100_000, day_two["peak_equity"])
        self.assertEqual(150_000, day_two["worst_drawdown"])
        self.assertEqual(4, day_two["observation_count"])

    def test_missing_scope_and_regressed_or_conflicting_truth_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-required"):
            self.observe(
                1_000_000,
                "2026-08-05T01:00:00",
                allow_create=True,
            )
        with self.assertRaisesRegex(ValueError, "scope-missing"):
            self.observe(1_000_000, "2026-08-05T01:00:00+00:00")
        self.observe(
            1_000_000,
            "2026-08-05T01:00:00+00:00",
            allow_create=True,
        )
        with self.assertRaisesRegex(ValueError, "observation-regressed"):
            self.observe(999_000, "2026-08-05T00:59:59+00:00")
        with self.assertRaisesRegex(ValueError, "observation-conflict"):
            self.observe(999_000, "2026-08-05T01:00:00+00:00")

    def test_stale_and_corrupt_scope_fail_closed(self) -> None:
        self.observe(
            1_000_000,
            "2026-08-05T01:00:00+00:00",
            allow_create=True,
        )
        with self.assertRaisesRegex(ValueError, "scope-stale"):
            self.ledger.functional_test_equity_scope(
                permit_id=self.permit_id,
                account_fingerprint=self.account_fingerprint,
                maximum_age_seconds=60,
                now="2026-08-05T01:02:00+00:00",
            )
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                "UPDATE functional_test_equity_scopes "
                "SET worst_drawdown = 999999 WHERE permit_id = ?",
                (self.permit_id,),
            )
            connection.commit()
        with self.assertRaisesRegex(ValueError, "integrity-failed"):
            self.ledger.functional_test_equity_scope(
                permit_id=self.permit_id,
                account_fingerprint=self.account_fingerprint,
            )

    def test_permit_and_account_scopes_are_isolated(self) -> None:
        self.observe(
            1_000_000,
            "2026-08-05T01:00:00+00:00",
            allow_create=True,
        )
        other = self.ledger.observe_functional_test_equity(
            permit_id="permit-other",
            account_fingerprint="b" * 64,
            current_equity=500_000,
            observed_at="2026-08-05T01:00:00+00:00",
            allow_create=True,
        )
        self.assertEqual(500_000, other["starting_equity"])
        original = self.ledger.functional_test_equity_scope(
            permit_id=self.permit_id,
            account_fingerprint=self.account_fingerprint,
        )
        self.assertEqual(1_000_000, original["current_equity"])

    def test_authority_close_is_durable_irreversible_and_idempotent(self) -> None:
        initial = self.ledger.functional_test_authority_status(
            permit_id=self.permit_id,
            account_fingerprint=self.account_fingerprint,
        )
        self.assertFalse(initial["closed"])
        closed = self.ledger.close_functional_test_authority(
            permit_id=self.permit_id,
            account_fingerprint=self.account_fingerprint,
            reason="operator-stop",
            occurred_at="2026-08-05T01:00:00+00:00",
        )
        self.assertTrue(closed["created"])
        repeated = self.ledger.close_functional_test_authority(
            permit_id=self.permit_id,
            account_fingerprint=self.account_fingerprint,
            reason="repeated-stop",
            occurred_at="2026-08-05T01:01:00+00:00",
        )
        self.assertFalse(repeated["created"])
        reloaded = ProgramLedger(self.path)
        status = reloaded.functional_test_authority_status(
            permit_id=self.permit_id,
            account_fingerprint=self.account_fingerprint,
        )
        self.assertTrue(status["closed"])
        self.assertEqual("operator-stop", status["reason"])


if __name__ == "__main__":
    unittest.main()
