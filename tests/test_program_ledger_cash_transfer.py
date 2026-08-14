from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from live_trader.program_ledger import ProgramLedger


class ProgramLedgerCashTransferTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release_patch = mock.patch(
            "live_trader.program_ledger."
            "BINANCE_CASH_TRANSFER_ADJUSTMENT_RELEASED",
            True,
        )
        self.release_patch.start()
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "ledger.sqlite3"
        self.ledger = ProgramLedger(
            self.path,
            cash_transfer_authority_verifier=lambda value: (
                value.get("officialTransfer", {}).get("tranId")
                == "binance-transfer-0001"
                and value.get("accountFingerprint")
                == hashlib.sha256(b"account").hexdigest()
            ),
        )
        self.ledger.replace_cash_rows(
            [
                {
                    "broker_id": "binance",
                    "account": "Binance Spot",
                    "currency": "USDT",
                    "broker_cash": "23.80494270",
                },
                {
                    "broker_id": "binance-futures",
                    "account": "Binance USD-M Futures",
                    "currency": "USDT",
                    "broker_cash": "0",
                },
                {
                    "broker_id": "upbit",
                    "account": "Upbit KRW",
                    "currency": "KRW",
                    "broker_cash": "49973.17",
                },
            ],
            source="event_poll",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()
        self.release_patch.stop()

    def apply(self) -> dict[str, object]:
        observed = datetime.now(timezone.utc).isoformat()
        evidence = {
            "schemaVersion": "binance-spot-futures-cash-transfer-truth/v1",
            "accountFingerprint": hashlib.sha256(b"account").hexdigest(),
            "spotCash": "13.8049427",
            "futuresCash": "10",
            "spotOpenOrderCount": 0,
            "futuresOpenOrderCount": 0,
            "futuresPositionCount": 0,
            "signedGetComplete": True,
            "observedAt": observed,
            "officialTransfer": {
                "tranId": "binance-transfer-0001",
                "asset": "USDT",
                "amount": "10",
                "fromAccount": "SPOT",
                "toAccount": "USD_M_FUTURES",
                "status": "CONFIRMED",
                "eventTime": "2026-07-31T00:00:00+00:00",
            },
        }
        encoded = json.dumps(
            evidence,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
        return self.ledger.apply_binance_spot_futures_cash_transfer_adjustment(
            source_account="Binance Spot",
            destination_account="Binance USD-M Futures",
            amount="10",
            source_cash_before="23.8049427",
            source_cash_after="13.8049427",
            destination_cash_before="0",
            destination_cash_after="10",
            observed_at=observed,
            truth_evidence=evidence,
            truth_hash=hashlib.sha256(encoded).hexdigest(),
        )

    def test_exact_two_leg_adjustment_preserves_unrelated_rows(self) -> None:
        result = self.apply()

        self.assertTrue(result["ok"])
        rows = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        self.assertEqual(13.8049427, rows["binance"]["cash"])
        self.assertEqual(10.0, rows["binance-futures"]["cash"])
        self.assertEqual(49973.17, rows["upbit"]["cash"])
        evidence = self.ledger.cash_transfer_adjustment_rows()
        self.assertEqual(1, len(evidence))
        self.assertEqual(result["contentHash"], evidence[0]["content_hash"])

    def test_production_release_latch_keeps_adjustment_inert(self) -> None:
        with mock.patch(
            "live_trader.program_ledger."
            "BINANCE_CASH_TRANSFER_ADJUSTMENT_RELEASED",
            False,
        ):
            with self.assertRaisesRegex(ValueError, "adjustment-not-released"):
                self.apply()
        rows = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        self.assertEqual(23.8049427, rows["binance"]["cash"])
        self.assertEqual(0.0, rows["binance-futures"]["cash"])

    def test_authority_is_required_before_any_ledger_mutation(self) -> None:
        self.ledger.cash_transfer_authority_verifier = None
        with self.assertRaisesRegex(ValueError, "authority-unverified"):
            self.apply()
        rows = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        self.assertEqual(23.8049427, rows["binance"]["cash"])
        self.assertEqual(0.0, rows["binance-futures"]["cash"])

    def test_adjustment_evidence_is_append_only(self) -> None:
        self.apply()
        conn = sqlite3.connect(self.path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM cash_transfer_adjustments")
        finally:
            conn.close()
        self.assertEqual(1, len(self.ledger.cash_transfer_adjustment_rows()))

    def test_stale_or_wrong_amount_fails_without_mutation(self) -> None:
        with self.assertRaisesRegex(ValueError, "amount-must-be-exact-10-usdt"):
            self.ledger.apply_binance_spot_futures_cash_transfer_adjustment(
                source_account="Binance Spot",
                destination_account="Binance USD-M Futures",
                amount="9",
                source_cash_before="23.8049427",
                source_cash_after="13.8049427",
                destination_cash_before="0",
                destination_cash_after="10",
                observed_at="2026-08-14T13:30:00+00:00",
                truth_evidence={
                    "schemaVersion": "binance-spot-futures-cash-transfer-truth/v1",
                    "accountFingerprint": hashlib.sha256(b"account").hexdigest(),
                    "spotCash": "13.8049427",
                    "futuresCash": "10",
                    "spotOpenOrderCount": 0,
                    "futuresOpenOrderCount": 0,
                    "futuresPositionCount": 0,
                    "signedGetComplete": True,
                    "observedAt": "2026-08-14T13:30:00+00:00",
                },
                truth_hash=hashlib.sha256(b"truth").hexdigest(),
            )
        rows = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        self.assertEqual(23.8049427, rows["binance"]["cash"])
        self.assertEqual(0.0, rows["binance-futures"]["cash"])
        self.assertEqual([], self.ledger.cash_transfer_adjustment_rows())

    def test_second_leg_sql_failure_rolls_back_first_leg_and_evidence(self) -> None:
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                """
                CREATE TRIGGER fail_futures_cash_update
                BEFORE UPDATE ON cash_balances
                WHEN OLD.broker_id='binance-futures'
                BEGIN SELECT RAISE(ABORT, 'injected-second-leg-failure'); END
                """
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaises(sqlite3.IntegrityError):
            self.apply()
        rows = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        self.assertEqual(23.8049427, rows["binance"]["cash"])
        self.assertEqual(0.0, rows["binance-futures"]["cash"])
        self.assertEqual([], self.ledger.cash_transfer_adjustment_rows())

    def test_legacy_wholesale_seed_is_atomic_even_though_public_route_is_closed(self) -> None:
        self.ledger.replace_position_rows(
            [
                {
                    "broker_id": "upbit",
                    "symbol": "KRW-BTC",
                    "asset": "BTC",
                    "currency": "KRW",
                    "broker_qty": "0.0000526",
                    "broker_value": "4999",
                }
            ],
            source="event_poll",
        )
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                """
                CREATE TRIGGER fail_seed_position_insert
                BEFORE INSERT ON positions
                BEGIN SELECT RAISE(ABORT, 'injected-position-seed-failure'); END
                """
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.seed_from_broker_snapshot(
                [
                    {
                        "broker_id": "binance",
                        "account": "Binance Spot",
                        "currency": "USDT",
                        "broker_cash": "13.8049427",
                    }
                ],
                [
                    {
                        "broker_id": "binance",
                        "symbol": "BTCUSDT",
                        "asset": "BTC",
                        "currency": "USDT",
                        "broker_qty": "0.00010441",
                        "broker_value": "0",
                    }
                ],
            )

        cash = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        positions = {
            (row["broker_id"], row["symbol"]): row
            for row in self.ledger.position_rows()
        }
        self.assertEqual(23.8049427, cash["binance"]["cash"])
        self.assertEqual(49973.17, cash["upbit"]["cash"])
        self.assertIn(("upbit", "KRW-BTC"), positions)
        self.assertNotIn(("binance", "BTCUSDT"), positions)


if __name__ == "__main__":
    unittest.main()
