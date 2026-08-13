from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from live_trader.binance_spot_continuous_functional import (
    BinanceSpotFunctionalError,
)
from live_trader.binance_spot_functional_scheduler import (
    BinanceSpotFunctionalManagedScheduler,
)
from live_trader.binance_spot_functional_strategy import (
    OfficialBinanceSpotFinalizedKlineReader,
    SealedBinanceSpotMovingAverageEvaluator,
)
from live_trader.binance_spot_functional_lifecycle import LifecycleHandle
from live_trader.binance_spot_publication import verify_binance_spot_publication
from tests.test_binance_spot_publication import PROOF, actual_binding


class FakeClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def wait(self, seconds: float) -> None:
        self.value += seconds


class FakePublicClient:
    def __init__(
        self,
        *,
        now_ms: int,
        rows: list[list[object]],
        time_values: list[int] | None = None,
        row_values: list[list[list[object]]] | None = None,
    ) -> None:
        self.now_ms = now_ms
        self.rows = rows
        self.time_values = list(time_values or [])
        self.row_values = list(row_values or [])
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, endpoint: str, query: dict[str, object]):
        self.calls.append((endpoint, dict(query)))
        if endpoint == "/api/v3/time":
            return {
                "serverTime": (
                    self.time_values.pop(0)
                    if self.time_values
                    else self.now_ms
                )
            }
        if endpoint == "/api/v3/klines":
            return self.row_values.pop(0) if self.row_values else self.rows
        raise AssertionError(endpoint)


def kline_rows(*, start_ms: int, closes: list[float]) -> list[list[object]]:
    rows: list[list[object]] = []
    for index, close in enumerate(closes):
        opened = start_ms + index * 300_000
        rows.append(
            [
                opened,
                str(close),
                str(close + 1),
                str(close - 1),
                str(close),
                "1",
                opened + 300_000 - 1,
                "1",
                1,
                "0.5",
                "0.5",
                "0",
            ]
        )
    return rows


class BinanceSpotFunctionalStrategyTest(unittest.TestCase):
    def test_official_reader_uses_server_time_and_only_closed_contiguous_bars(self) -> None:
        start_ms = 1_800_000_000_000
        rows = kline_rows(start_ms=start_ms, closes=[100 + i for i in range(12)])
        # The twelfth row is the currently open kline and must be excluded.
        now_ms = start_ms + 11 * 300_000 + 100_000
        clock = FakeClock(now_ms / 1000)
        client = FakePublicClient(now_ms=now_ms, rows=rows)
        window = OfficialBinanceSpotFinalizedKlineReader(
            client=client,  # type: ignore[arg-type]
            clock=clock,
        ).read_window()
        self.assertEqual(11, len(window["bars"]))
        self.assertEqual(rows[10][0], int(window["barId"].rsplit("-", 1)[1]))
        self.assertEqual(
            ["/api/v3/time", "/api/v3/klines", "/api/v3/time"],
            [call[0] for call in client.calls],
        )
        self.assertEqual(
            {"symbol": "BTCUSDT", "interval": "5m", "limit": 13},
            client.calls[1][1],
        )

    def test_boundary_cross_refetches_once_and_uses_new_latest_close(self) -> None:
        start_ms = 1_800_000_000_000
        before = start_ms + 12 * 300_000 - 100
        after = start_ms + 12 * 300_000 + 100
        first = kline_rows(start_ms=start_ms, closes=[100 + i for i in range(12)])
        second = kline_rows(start_ms=start_ms, closes=[100 + i for i in range(13)])
        clock = FakeClock(after / 1000)
        client = FakePublicClient(
            now_ms=after,
            rows=second,
            time_values=[before, after, after + 50],
            row_values=[first, second],
        )
        window = OfficialBinanceSpotFinalizedKlineReader(
            client=client,  # type: ignore[arg-type]
            clock=clock,
        ).read_window()
        self.assertEqual(second[11][0], int(window["barId"].rsplit("-", 1)[1]))
        self.assertEqual(2, [item[0] for item in client.calls].count("/api/v3/klines"))

    def test_official_reader_fails_closed_on_a_gap(self) -> None:
        start_ms = 1_800_000_000_000
        rows = kline_rows(start_ms=start_ms, closes=[100 + i for i in range(11)])
        rows[6][0] = int(rows[6][0]) + 300_000
        rows[6][6] = int(rows[6][0]) + 300_000 - 1
        now_ms = start_ms + 12 * 300_000
        with self.assertRaisesRegex(BinanceSpotFunctionalError, "gap"):
            OfficialBinanceSpotFinalizedKlineReader(
                client=FakePublicClient(now_ms=now_ms, rows=rows),  # type: ignore[arg-type]
                clock=FakeClock(now_ms / 1000),
            ).read_window()

    def test_official_reader_rejects_contiguous_but_stale_replayed_window(self) -> None:
        start_ms = 1_800_000_000_000
        rows = kline_rows(start_ms=start_ms, closes=[100 + i for i in range(11)])
        now_ms = start_ms + 13 * 300_000
        with self.assertRaisesRegex(BinanceSpotFunctionalError, "latest closed"):
            OfficialBinanceSpotFinalizedKlineReader(
                client=FakePublicClient(now_ms=now_ms, rows=rows),  # type: ignore[arg-type]
                clock=FakeClock(now_ms / 1000),
            ).read_window()

    def test_actual_published_pair_derives_natural_buy_without_signal_input(self) -> None:
        start_ms = 1_800_000_000_000
        now_ms = start_ms + 11 * 300_000 + 1_000
        raw = OfficialBinanceSpotFinalizedKlineReader(
            client=FakePublicClient(  # type: ignore[arg-type]
                now_ms=now_ms,
                rows=kline_rows(
                    start_ms=start_ms,
                    closes=[10, 10, 10, 10, 10, 10, 10, 9, 9, 9, 20],
                ),
            ),
            clock=FakeClock(now_ms / 1000),
        ).read_window()
        binding = actual_binding()
        binding_payload = {
            "strategyArtifactId": binding.strategy_artifact_id,
            "strategyArtifactHash": binding.strategy_artifact_hash,
            "artifactFileSha256": binding.artifact_file_sha256,
            "strategyInstanceId": binding.strategy_instance_id,
            "strategyInstanceHash": binding.strategy_instance_hash,
            "instanceFileSha256": binding.instance_file_sha256,
            "publicationProofHash": binding.publication_proof_hash,
            "publicationProofFileSha256": binding.publication_proof_file_sha256,
            "accountFingerprint": binding.account_fingerprint,
            "broker": binding.broker,
            "venue": binding.venue,
            "asset": binding.asset,
            "market": binding.market,
            "executionRoute": binding.execution_route,
            "symbol": binding.symbol,
            "baseAsset": binding.base_asset,
            "quoteAsset": binding.quote_asset,
            "interval": binding.interval,
        }
        evaluator = SealedBinanceSpotMovingAverageEvaluator(
            binding_reader=lambda: binding_payload,
            publication_verifier=lambda exact: verify_binance_spot_publication(
                exact, proof_path=PROOF
            ),
        )
        evaluated = evaluator.evaluate(raw)
        self.assertEqual("BUY", evaluated["signal"])
        self.assertTrue(evaluated["naturalSignal"])
        self.assertFalse(evaluated["forced"])
        self.assertEqual("moving_average_cross", evaluated["strategyPluginId"])
        self.assertEqual(binding.strategy_artifact_hash, evaluated["strategyArtifactHash"])

        forged = dict(raw)
        forged["signal"] = "BUY"
        with self.assertRaisesRegex(BinanceSpotFunctionalError, "signal fields"):
            evaluator.evaluate(forged)


class FakeManagedLifecycle:
    def __init__(
        self,
        clock: FakeClock,
        *,
        fail_heartbeat_at: int = 0,
        fail_tick_once: bool = False,
        keep_cleanup_pending: bool = False,
        transient_tick_once: bool = False,
        heartbeat_cleanup_ticks: int = 0,
        ambiguous: bool = False,
        fail_heartbeat_without_cleanup: bool = False,
        fail_finalize_after_reset: int = 0,
    ) -> None:
        self.clock = clock
        self.phase = "ACTIVE"
        self.heartbeat_times: list[float] = []
        self.tick_times: list[float] = []
        self.fail_heartbeat_at = fail_heartbeat_at
        self.fail_tick_once = fail_tick_once
        self.keep_cleanup_pending = keep_cleanup_pending
        self.transient_tick_once = transient_tick_once
        self.heartbeat_cleanup_ticks = heartbeat_cleanup_ticks
        self.ambiguous = ambiguous
        self.fail_heartbeat_without_cleanup = fail_heartbeat_without_cleanup
        self.ambiguous_observations = 0
        self.ambiguous_last_epoch = 0.0
        self.ambiguous_failed = False
        self.deadline_revoked = False
        self.fail_finalize_after_reset = int(fail_finalize_after_reset)
        self.final_reset_resume_calls = 0

    def status(self) -> dict[str, object]:
        return {"phase": self.phase}

    def heartbeat(self, _handle: LifecycleHandle) -> dict[str, object]:
        self.heartbeat_times.append(self.clock())
        if self.fail_heartbeat_at and len(self.heartbeat_times) == self.fail_heartbeat_at:
            if not self.fail_heartbeat_without_cleanup:
                self.phase = "CLEANUP"
            raise RuntimeError("verified private stream lost")
        return {"phase": "ACTIVE"}

    def tick(self, handle: LifecycleHandle) -> dict[str, object]:
        self.tick_times.append(self.clock())
        if self.transient_tick_once:
            self.transient_tick_once = False
            error = RuntimeError("5-minute boundary moved")
            error.transient_market_data = True  # type: ignore[attr-defined]
            raise error
        if self.fail_tick_once:
            self.fail_tick_once = False
            raise RuntimeError("sealed evaluator failed")
        if self.keep_cleanup_pending:
            self.phase = "CLEANUP"
            return {"ok": True, "status": "CLEANUP_PENDING", "claim": {"id": "x"}}
        if self.phase == "CLEANUP" and self.heartbeat_cleanup_ticks > 0:
            self.heartbeat_cleanup_ticks -= 1
            return {
                "ok": True,
                "status": "CLEANUP_ACTION_PENDING",
                "claim": {"id": f"cleanup-{self.heartbeat_cleanup_ticks}"},
            }
        if self.phase == "CLEANUP" and self.ambiguous_observations < 2 and self.ambiguous:
            return {
                "ok": False,
                "status": "RECONCILIATION_REQUIRED",
                "claim": {"id": "ambiguous-claim-1"},
            }
        if self.phase == "CLEANUP":
            return {"ok": True, "status": "CLEANUP_BASELINE_READY", "action": None}
        if self.clock() >= handle.expires_epoch:
            self.phase = "CLEANUP"
            return {"ok": True, "status": "CLEANUP_BASELINE_READY", "action": None}
        return {"ok": True, "status": "ACTIVE", "action": None}

    def finalize(self, _handle: LifecycleHandle) -> dict[str, object]:
        if self.fail_finalize_after_reset > 0:
            self.fail_finalize_after_reset -= 1
            self.phase = "FINAL_RESET"
            raise RuntimeError("stream retirement temporarily unavailable")
        self.phase = "FINALIZED"
        return {"status": "FINALIZED", "outcome": "INCONCLUSIVE_NO_SIGNAL"}

    def resume_final_reset(self, *, session_id: str) -> dict[str, object]:
        self.assert_resume_session_id = session_id
        self.final_reset_resume_calls += 1
        if self.fail_finalize_after_reset > 0:
            self.fail_finalize_after_reset -= 1
            raise RuntimeError("final reset dependency still unavailable")
        self.phase = "FINALIZED"
        return {"status": "FINALIZED", "resumed": True}

    def begin_cleanup(self, _handle: LifecycleHandle, *, reason: str):
        self.phase = "CLEANUP"
        return {"phase": "CLEANUP", "reason": reason}

    def fail_cleanup_deadline(self, _handle: LifecycleHandle, *, reason: str):
        self.phase = "FAILED"
        self.deadline_revoked = True
        return {"phase": "FAILED", "detail": reason}

    def next_due_ambiguous_claim(self, _handle: LifecycleHandle):
        if not self.ambiguous or self.ambiguous_observations >= 2:
            return None
        due = 60.0 if self.ambiguous_observations == 0 else self.ambiguous_last_epoch + 5
        if self.clock() < due:
            return None
        return {"claimId": "ambiguous-claim-1", "dueEpoch": due}

    def prove_ambiguous_not_accepted(
        self, _handle: LifecycleHandle, *, claim_id: str
    ) -> dict[str, object]:
        self.assert_claim_id = claim_id
        self.ambiguous_observations += 1
        self.ambiguous_last_epoch = self.clock()
        return {
            "ok": True,
            "status": (
                "AMBIGUOUS_PROVEN_NOT_ACCEPTED"
                if self.ambiguous_observations == 2
                else "NONACCEPTANCE_OBSERVATION_RECORDED"
            ),
            "retryAttempted": False,
        }

    def fail_ambiguous_reconciliation(
        self, _handle: LifecycleHandle, *, reason: str
    ) -> dict[str, object]:
        self.phase = "FAILED"
        self.ambiguous_failed = True
        return {"phase": "FAILED", "detail": reason}


class BinanceSpotFunctionalManagedSchedulerTest(unittest.TestCase):
    @staticmethod
    def handle(*, start: float = 0) -> LifecycleHandle:
        return LifecycleHandle(
            session_id="bnsft-scheduler-0001",
            capability="scheduler-functional-capability-00000001",
            owner_id="scheduler-owner-0001",
            owner_token="scheduler-owner-token-000000000001",
            expires_epoch=start + 7200,
            cleanup_deadline_epoch=start + 10800,
        )

    def test_accelerated_two_hours_keeps_owner_lease_and_finalizes_no_signal(self) -> None:
        clock = FakeClock(0)
        manager = FakeManagedLifecycle(clock)
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
            heartbeat_interval_seconds=20,
            market_poll_interval_seconds=5,
        ).run(self.handle())
        self.assertEqual("FINALIZED", result["status"])
        self.assertGreaterEqual(len(manager.heartbeat_times), 359)
        self.assertTrue(
            all(
                current - previous <= 20
                for previous, current in zip(
                    manager.heartbeat_times, manager.heartbeat_times[1:]
                )
            )
        )
        self.assertEqual(7200, manager.tick_times[-1])

    def test_heartbeat_loss_never_returns_to_entry_and_drives_cleanup(self) -> None:
        clock = FakeClock(0)
        manager = FakeManagedLifecycle(
            clock, fail_heartbeat_at=3, heartbeat_cleanup_ticks=3
        )
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
        ).run(self.handle())
        self.assertEqual("FINALIZED", result["status"])
        self.assertEqual("FINALIZED", manager.phase)
        self.assertEqual(3, len(manager.heartbeat_times))
        self.assertGreaterEqual(len(manager.tick_times), 4)
        self.assertIn("verified private stream lost", result["ownerFailure"])

    def test_ambiguous_submit_is_proved_twice_without_retry_then_finalized(self) -> None:
        clock = FakeClock(60)
        manager = FakeManagedLifecycle(clock, ambiguous=True)
        manager.phase = "CLEANUP"
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
            market_poll_interval_seconds=5,
        ).run(self.handle())
        self.assertEqual("FINALIZED", result["status"])
        self.assertEqual(2, manager.ambiguous_observations)
        self.assertEqual("ambiguous-claim-1", manager.assert_claim_id)
        self.assertFalse(manager.ambiguous_failed)

    def test_heartbeat_failure_without_durable_cleanup_never_ticks_again(self) -> None:
        clock = FakeClock(0)
        manager = FakeManagedLifecycle(
            clock,
            fail_heartbeat_at=1,
            fail_heartbeat_without_cleanup=True,
        )
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
        ).run(self.handle())
        self.assertEqual("RECONCILIATION_REQUIRED", result["status"])
        self.assertEqual([], manager.tick_times)
        self.assertFalse(result["entryRetryAttempted"])

    def test_tick_or_evaluator_exception_latches_cleanup_and_then_finalizes(self) -> None:
        clock = FakeClock(0)
        manager = FakeManagedLifecycle(clock, fail_tick_once=True)
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
        ).run(self.handle())
        self.assertEqual("FINALIZED", result["status"])
        self.assertEqual(1, len(manager.heartbeat_times))
        self.assertEqual("FINALIZED", manager.phase)

    def test_cleanup_deadline_revokes_capability_to_manual_reconciliation(self) -> None:
        clock = FakeClock(10_799)
        manager = FakeManagedLifecycle(clock, keep_cleanup_pending=True)
        manager.phase = "CLEANUP"
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
        ).run(self.handle())
        self.assertEqual("RECONCILIATION_REQUIRED", result["status"])
        self.assertTrue(manager.deadline_revoked)
        self.assertEqual("FAILED", manager.phase)

    def test_transient_kline_boundary_retry_keeps_entry_authority(self) -> None:
        clock = FakeClock(0)
        manager = FakeManagedLifecycle(clock, transient_tick_once=True)
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
        ).run(self.handle(start=-7190))
        self.assertEqual("FINALIZED", result["status"])
        self.assertGreaterEqual(len(manager.tick_times), 3)
        self.assertEqual("FINALIZED", manager.phase)

    def test_transient_final_reset_failure_resumes_without_process_restart(self) -> None:
        clock = FakeClock(7200)
        manager = FakeManagedLifecycle(clock, fail_finalize_after_reset=2)
        manager.phase = "CLEANUP"
        result = BinanceSpotFunctionalManagedScheduler(
            manager=manager,  # type: ignore[arg-type]
            clock=clock,
            wait=clock.wait,
            market_poll_interval_seconds=5,
        ).run(self.handle())
        self.assertEqual("FINALIZED", result["status"])
        self.assertTrue(result["resumedFinalReset"])
        self.assertEqual(2, manager.final_reset_resume_calls)
        self.assertEqual("bnsft-scheduler-0001", manager.assert_resume_session_id)
        self.assertEqual(1, len(manager.tick_times))


if __name__ == "__main__":
    unittest.main()
