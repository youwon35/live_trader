from __future__ import annotations

import concurrent.futures
from contextlib import contextmanager
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from live_trader.binance_cash_transfer_evidence import (
    BinanceCashTransferEvidenceVerifier,
)
from live_trader.program_ledger import ProgramLedger


ACCOUNT = hashlib.sha256(b"configured-binance-account").hexdigest()
API_KEY = hashlib.sha256(b"configured-binance-api-key").hexdigest()
TRAN_ID = 11415955596


class ProgramLedgerCashTransferTests(unittest.TestCase):
    def setUp(self) -> None:
        self.release_patches = [
            mock.patch(
                "live_trader.program_ledger."
                "BINANCE_CASH_TRANSFER_ADJUSTMENT_RELEASED",
                True,
            ),
            mock.patch(
                "live_trader.program_ledger."
                "BINANCE_CASH_TRANSFER_CONSUMPTION_RELEASED",
                True,
            ),
            mock.patch(
                "live_trader.binance_cash_transfer_evidence."
                "BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED",
                True,
            ),
        ]
        for patcher in self.release_patches:
            patcher.start()
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "ledger.sqlite3"
        self.now = datetime.now(timezone.utc)
        self.evidence_verifier = BinanceCashTransferEvidenceVerifier(
            configured_account_fingerprint=ACCOUNT,
            configured_api_key_fingerprint=API_KEY,
            signed_get_verifier=lambda value: (
                value.get("detachedCaptureHash")
                == hashlib.sha256(b"signed-capture").hexdigest()
            ),
            idle_barrier_verifier=lambda value: (
                value.get("detachedEvidenceHash")
                == hashlib.sha256(b"idle-barrier").hexdigest()
            ),
            clock=lambda: self.now,
        )
        self.global_idle_held = False
        self.ledger = self.new_ledger()
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
        for patcher in reversed(self.release_patches):
            patcher.stop()

    def new_ledger(self) -> ProgramLedger:
        return ProgramLedger(
            self.path,
            cash_transfer_authority_verifier=(
                self.evidence_verifier.verify_ledger_authority_request
            ),
            cash_transfer_high_water_verifier=(
                self.evidence_verifier.verify_high_water_request
            ),
            cash_transfer_global_idle_reserver=(
                self.global_idle_reservation
            ),
        )

    @contextmanager
    def global_idle_reservation(self, request):
        if self.global_idle_held:
            raise RuntimeError("test-global-idle-reservation-reentered")
        self.global_idle_held = True
        body = {
            "schemaVersion": "crypto-first-live-global-idle-reservation/v1",
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "purpose": request["purpose"],
            "phase": "IDLE",
            "coordinatorStoredPhase": "IDLE",
            "coordinatorDatabaseId": "crypto-first-live-db-test-0001",
            "coordinatorRevision": 0,
            "publicationHash": "",
            "accountFingerprint": request["accountFingerprint"],
            "consumptionKey": request["consumptionKey"],
            "truthHash": request["truthHash"],
            "reservationId": "crypto-first-live-global-idle-test-0001",
            "acquiredEpoch": self.now.timestamp(),
            "held": True,
            "exclusive": True,
            "durableAuthority": True,
            "restartVerifiable": True,
        }
        digest = hashlib.sha256(
            json.dumps(
                body,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        try:
            yield {**body, "reservationHash": digest}
        finally:
            self.global_idle_held = False

    def envelope(self) -> dict[str, object]:
        observed = self.now.isoformat()
        request_timestamp = int(self.now.timestamp() * 1000)
        return {
            "schemaVersion": "binance-universal-transfer-signed-get/v1",
            "accountFingerprint": ACCOUNT,
            "apiKeyFingerprint": API_KEY,
            "method": "GET",
            "path": "/sapi/v1/asset/transfer",
            "securityType": "USER_DATA",
            "signed": True,
            "transferType": "MAIN_UMFUTURE",
            "queryStartTime": int(
                (self.now - timedelta(days=30)).timestamp() * 1000
            ),
            "queryEndTime": int(
                (self.now - timedelta(seconds=1)).timestamp() * 1000
            ),
            "pageSize": 100,
            "pages": [
                {
                    "current": 1,
                    "httpStatus": 200,
                    "total": 1,
                    "rows": [
                        {
                            "asset": "USDT",
                            "amount": "10.00000000",
                            "type": "MAIN_UMFUTURE",
                            "status": "CONFIRMED",
                            "tranId": TRAN_ID,
                            "timestamp": int(
                                (self.now - timedelta(days=15)).timestamp()
                                * 1000
                            ),
                        }
                    ],
                    "requestTimestamp": request_timestamp,
                    "receivedAt": observed,
                    "requestHash": hashlib.sha256(b"request").hexdigest(),
                    "responseHash": hashlib.sha256(b"response").hexdigest(),
                }
            ],
            "allPagesComplete": True,
            "requestCount": 1,
            "retryCount": 0,
            "redirectCount": 0,
            "mutationCount": 0,
            "observedAt": observed,
            "detachedCaptureHash": hashlib.sha256(
                b"signed-capture"
            ).hexdigest(),
        }

    def barrier(self) -> dict[str, object]:
        return {
            "schemaVersion": "binance-cash-transfer-idle-barrier/v1",
            "barrierId": "global-idle-barrier-0001",
            "accountFingerprint": ACCOUNT,
            "apiKeyFingerprint": API_KEY,
            "coordinatorPhase": "IDLE",
            "coordinatorRevision": 9,
            "globalLeaseState": "IDLE",
            "activeOwnerCount": 0,
            "mutationInFlightCount": 0,
            "spotOpenOrderCount": 0,
            "futuresOpenOrderCount": 0,
            "futuresPositionCount": 0,
            "spotCash": "13.80494270",
            "futuresCash": "10.00000000",
            "observedAt": self.now.isoformat(),
            "detachedEvidenceHash": hashlib.sha256(
                b"idle-barrier"
            ).hexdigest(),
        }

    def certified(self) -> dict[str, object]:
        return self.evidence_verifier.certify(
            expected_tran_id=TRAN_ID,
            signed_get_envelope=self.envelope(),
            idle_barrier_evidence=self.barrier(),
            prior_consumed_high_water=(
                self.ledger.cash_transfer_consumption_high_water(
                    account_fingerprint=ACCOUNT,
                    api_key_fingerprint=API_KEY,
                )
            ),
        )

    def apply(
        self,
        *,
        ledger: ProgramLedger | None = None,
        certified: dict[str, object] | None = None,
    ) -> dict[str, object]:
        target = ledger or self.ledger
        proof = certified or self.certified()
        return target.apply_binance_spot_futures_cash_transfer_adjustment(
            source_account="Binance Spot",
            destination_account="Binance USD-M Futures",
            amount="10",
            source_cash_before="23.8049427",
            source_cash_after="13.8049427",
            destination_cash_before="0",
            destination_cash_after="10",
            observed_at=self.now.isoformat(),
            truth_evidence=proof["truthEvidence"],
            truth_hash=proof["truthHash"],
        )

    def assert_original_balances(self) -> None:
        rows = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        self.assertEqual(23.8049427, rows["binance"]["cash"])
        self.assertEqual(0.0, rows["binance-futures"]["cash"])

    def test_exact_two_leg_adjustment_and_consumption_are_one_commit(self) -> None:
        result = self.apply()

        self.assertTrue(result["ok"])
        rows = {row["broker_id"]: row for row in self.ledger.cash_rows()}
        self.assertEqual(13.8049427, rows["binance"]["cash"])
        self.assertEqual(10.0, rows["binance-futures"]["cash"])
        self.assertEqual(49973.17, rows["upbit"]["cash"])
        adjustments = self.ledger.cash_transfer_adjustment_rows()
        consumptions = self.ledger.cash_transfer_consumption_rows()
        self.assertEqual(1, len(adjustments))
        self.assertEqual(1, len(consumptions))
        self.assertEqual(result["contentHash"], adjustments[0]["content_hash"])
        self.assertEqual(
            result["consumptionHeadHash"], consumptions[0]["content_hash"]
        )
        high = self.ledger.cash_transfer_consumption_high_water(
            account_fingerprint=ACCOUNT,
            api_key_fingerprint=API_KEY,
        )
        self.assertEqual(1, high["revision"])
        self.assertEqual(str(TRAN_ID), high["tranId"])
        self.assertEqual(result["consumptionKey"], high["consumptionKey"])

    def test_both_production_latches_are_independently_required(self) -> None:
        for field, message in (
            (
                "BINANCE_CASH_TRANSFER_ADJUSTMENT_RELEASED",
                "adjustment-not-released",
            ),
            (
                "BINANCE_CASH_TRANSFER_CONSUMPTION_RELEASED",
                "consumption-not-released",
            ),
        ):
            with self.subTest(field=field), mock.patch(
                f"live_trader.program_ledger.{field}", False
            ):
                with self.assertRaisesRegex(ValueError, message):
                    self.apply()
                self.assert_original_balances()
                self.assertEqual([], self.ledger.cash_transfer_consumption_rows())

    def test_both_independent_verifiers_are_required(self) -> None:
        proof = self.certified()
        cases = (
            (
                ProgramLedger(
                    self.path,
                    cash_transfer_high_water_verifier=(
                        self.evidence_verifier.verify_high_water_request
                    ),
                ),
                "authority-unverified",
            ),
            (
                ProgramLedger(
                    self.path,
                    cash_transfer_authority_verifier=(
                        self.evidence_verifier.verify_ledger_authority_request
                    ),
                ),
                "high-water-unverified",
            ),
        )
        for ledger, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self.apply(ledger=ledger, certified=proof)
                self.assert_original_balances()
                self.assertEqual([], self.ledger.cash_transfer_consumption_rows())

    def test_global_idle_reserver_is_required_before_ledger_commit(self) -> None:
        ledger = ProgramLedger(
            self.path,
            cash_transfer_authority_verifier=(
                self.evidence_verifier.verify_ledger_authority_request
            ),
            cash_transfer_high_water_verifier=(
                self.evidence_verifier.verify_high_water_request
            ),
        )
        with self.assertRaisesRegex(ValueError, "idle-reserver-unavailable"):
            self.apply(ledger=ledger)
        self.assert_original_balances()
        self.assertEqual([], self.ledger.cash_transfer_consumption_rows())

    def test_global_idle_reservation_is_held_during_sqlite_commit(self) -> None:
        original_connect = self.ledger.connect

        def checked_connect():
            conn = original_connect()
            conn.create_function(
                "global_idle_held",
                0,
                lambda: 1 if self.global_idle_held else 0,
            )
            return conn

        self.ledger.connect = checked_connect  # type: ignore[method-assign]
        conn = self.ledger.connect()
        try:
            conn.execute(
                """
                CREATE TRIGGER require_global_idle_for_transfer_consumption
                BEFORE INSERT ON binance_cash_transfer_consumptions
                WHEN global_idle_held() != 1
                BEGIN SELECT RAISE(ABORT, 'global-idle-not-held'); END
                """
            )
            conn.commit()
        finally:
            conn.close()
        result = self.apply()
        self.assertTrue(result["ok"])
        self.assertFalse(self.global_idle_held)

    def test_replay_is_rejected_by_unique_consumed_high_water(self) -> None:
        proof = self.certified()
        self.apply(certified=proof)
        with self.assertRaisesRegex(
            ValueError, "consumed-high-water-cas-changed"
        ):
            self.apply(certified=proof)
        self.assertEqual(1, len(self.ledger.cash_transfer_adjustment_rows()))
        self.assertEqual(1, len(self.ledger.cash_transfer_consumption_rows()))

    def test_adjustment_and_consumption_evidence_are_append_only(self) -> None:
        self.apply()
        conn = sqlite3.connect(self.path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM cash_transfer_adjustments")
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM binance_cash_transfer_consumptions")
        finally:
            conn.close()

    def test_second_leg_sql_failure_rolls_back_all_four_writes(self) -> None:
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
        self.assert_original_balances()
        self.assertEqual([], self.ledger.cash_transfer_adjustment_rows())
        self.assertEqual([], self.ledger.cash_transfer_consumption_rows())

    def test_consumption_insert_failure_rolls_back_both_legs_and_adjustment(self) -> None:
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                """
                CREATE TRIGGER fail_consumption_insert
                BEFORE INSERT ON binance_cash_transfer_consumptions
                BEGIN SELECT RAISE(ABORT, 'injected-consumption-failure'); END
                """
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaises(sqlite3.IntegrityError):
            self.apply()
        self.assert_original_balances()
        self.assertEqual([], self.ledger.cash_transfer_adjustment_rows())
        self.assertEqual([], self.ledger.cash_transfer_consumption_rows())

    def test_concurrent_replay_has_exactly_one_winner(self) -> None:
        proof = self.certified()
        ledgers = (self.new_ledger(), self.new_ledger())

        def attempt(ledger: ProgramLedger) -> str:
            try:
                self.apply(ledger=ledger, certified=proof)
                return "ok"
            except (ValueError, sqlite3.Error) as exc:
                return str(exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, ledgers))
        self.assertEqual(1, results.count("ok"), results)
        self.assertEqual(1, len(self.ledger.cash_transfer_adjustment_rows()))
        self.assertEqual(1, len(self.ledger.cash_transfer_consumption_rows()))

    def test_tampered_consumption_chain_fails_closed(self) -> None:
        self.apply()
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                "DROP TRIGGER binance_cash_transfer_consumptions_no_update"
            )
            conn.execute(
                """
                UPDATE binance_cash_transfer_consumptions
                SET content_json='{}' WHERE sequence=1
                """
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(ValueError, "consumption-chain-invalid"):
            self.ledger.cash_transfer_consumption_rows()

    def test_restored_adjustment_without_consumption_lineage_fails_closed(self) -> None:
        self.apply()
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                "DROP TRIGGER binance_cash_transfer_consumptions_no_delete"
            )
            conn.execute("DELETE FROM binance_cash_transfer_consumptions")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(ValueError, "lineage-incomplete"):
            self.ledger.cash_transfer_consumption_high_water(
                account_fingerprint=ACCOUNT,
                api_key_fingerprint=API_KEY,
            )

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
