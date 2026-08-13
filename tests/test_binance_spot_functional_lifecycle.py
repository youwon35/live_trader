from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.parse

from live_trader.binance_spot_continuous_functional import (
    BinanceSpotContinuousFunctionalService,
    DurableFunctionalLedger,
)
from live_trader.binance_spot_functional_lifecycle import (
    BinanceSpotFunctionalLifecycleManager,
    BinanceSpotLifecycleError,
    DurableBinanceSpotFunctionalControl,
    PRODUCTION_LIFECYCLE_AVAILABLE,
    build_binance_spot_production_lifecycle,
    production_entrypoint_status,
)
from live_trader.binance_spot_functional_mutation import (
    BinanceSpotFunctionalMutationEdge,
)
from live_trader.binance_spot_functional_transport import binance_api_key_fingerprint
from tests.test_binance_spot_continuous_functional import (
    Clock,
    bar,
    binding,
    permit,
    rules,
    truth,
)


OWNER_TOKEN = "owner-token-binance-functional-00000001"
RECOVERY_TOKEN = "owner-token-binance-recovery-0000002"


class FakeOfficialTruthAndMutation:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.base = Decimal("0.001")
        self.quote = Decimal("100")
        self.mark = Decimal("60000")
        self.sell_quote = Decimal("9.5")
        self.orders: list[dict[str, object]] = []
        self.fills: list[dict[str, object]] = []
        self.send_count = 0

    def read(self, *, baseline_epoch: float, owner_prefix: str):
        _ = baseline_epoch, owner_prefix
        return (
            truth(
                self.clock,
                base=format(self.base, "f"),
                quote=format(self.quote, "f"),
                mark=format(self.mark, "f"),
                closed_orders=list(self.orders),
                fills=list(self.fills),
            ),
            rules(),
        )

    def send(self, request: object) -> dict[str, object]:
        self.send_count += 1
        parsed = urllib.parse.urlsplit(request.url)  # type: ignore[attr-defined]
        query = dict(urllib.parse.parse_qsl(parsed.query))
        side = query.get("side", "")
        client_id = query.get("newClientOrderId", "")
        order_id = str(100 + self.send_count)
        if side == "BUY":
            quantity = Decimal("0.00016")
            quote_quantity = Decimal("9.6")
            commission = Decimal("0.01")
            self.base += quantity
            self.quote -= quote_quantity + commission
            original_quote = query["quoteOrderQty"]
        elif side == "SELL":
            quantity = Decimal(query["quantity"])
            quote_quantity = self.sell_quote
            commission = Decimal("0.01")
            self.base -= quantity
            self.quote += quote_quantity - commission
            original_quote = "0"
        else:
            raise AssertionError("test broker received non-trade mutation")
        self.orders.append(
            {
                "orderId": order_id,
                "clientOrderId": client_id,
                "symbol": "BTCUSDT",
                "product": "SPOT",
                "side": side,
                "type": "MARKET",
                "status": "FILLED",
                "origQty": format(quantity, "f"),
                "executedQty": format(quantity, "f"),
                "origQuoteOrderQty": original_quote,
                "cummulativeQuoteQty": format(quote_quantity, "f"),
                "isMargin": False,
                "reduceOnly": False,
            }
        )
        self.fills.append(
            {
                "tradeId": str(500 + self.send_count),
                "orderId": order_id,
                "clientOrderId": client_id,
                "symbol": "BTCUSDT",
                "side": side,
                "quantity": format(quantity, "f"),
                "quoteQuantity": format(quote_quantity, "f"),
                "commission": format(commission, "f"),
                "commissionAsset": "USDT",
                "feeQuoteValue": format(commission, "f"),
            }
        )
        return {
            "ok": True,
            "json": {
                "orderId": order_id,
                "clientOrderId": client_id,
                "symbol": "BTCUSDT",
                "side": side,
                "type": "MARKET",
                "status": "FILLED",
            },
        }


class BinanceSpotFunctionalLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.path = Path(self.temporary.name) / "managed-binance.sqlite3"
        self.broker = FakeOfficialTruthAndMutation(self.clock)
        self.manager = self.build_manager()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def build_manager(self) -> BinanceSpotFunctionalLifecycleManager:
        ledger = DurableFunctionalLedger(self.path)
        control = DurableBinanceSpotFunctionalControl(
            self.path, clock=self.clock
        )
        service = BinanceSpotContinuousFunctionalService(
            ledger=ledger,
            binding_reader=lambda: binding(),
            authority_reader=control.authority_snapshot,
            publication_verifier=lambda _: {
                "complete": True,
                "strategyArtifactHash": "a" * 64,
                "artifactFileSha256": "1" * 64,
                "strategyInstanceHash": "b" * 64,
                "instanceFileSha256": "2" * 64,
                "publicationProofHash": "3" * 64,
                "publicationProofFileSha256": "4" * 64,
            },
            clock=self.clock,
            monotonic_clock=self.clock,
        )
        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=control.authority_snapshot,
            claim_reader=ledger.action,
            sender=self.broker.send,
            allow_mock_transport=True,
        )
        return BinanceSpotFunctionalLifecycleManager(
            ledger=ledger,
            control=control,
            service=service,
            truth_reader=self.broker,
            mutation_edge=edge,
            stream_owner_binder=lambda _prefix, session_id, _permit_id, _permit_hash: setattr(
                self.clock, "session_id", session_id
            ),
            clock=self.clock,
            allow_mock_lifecycle=True,
        )

    def ready_env(self):
        return patch.dict(
            "os.environ",
            {
                "BINANCE_API_KEY": "api-key",
                "BINANCE_API_SECRET": "secret",
                "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
                "BINANCE_SPOT_FUNCTIONAL_LIVE_ENABLED": "true",
            },
            clear=False,
        )

    def keep_owner_alive_for_next_bar(self, handle: object) -> None:
        for _ in range(6):
            self.clock.value += 50
            self.manager.heartbeat(handle)  # type: ignore[arg-type]

    def test_mock_managed_buy_sell_round_trip_seals_baseline_flat(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
            authority = self.manager.control.authority_snapshot()
            self.assertTrue(authority["newEntriesBlocked"])
            self.assertFalse(authority["ordinaryLiveAllowed"])
            self.assertFalse(authority["smokeAllowed"])

            bought = self.manager.tick(
                handle,
                permit(self.clock),
                finalized_bar=bar(self.clock, "BUY"),
            )
            self.assertEqual("ACKNOWLEDGED", bought["dispatch"]["status"])
            self.keep_owner_alive_for_next_bar(handle)
            sold = self.manager.tick(
                handle,
                permit(self.clock),
                finalized_bar=bar(self.clock, "SELL"),
            )
            self.assertEqual("ACKNOWLEDGED", sold["dispatch"]["status"])
            final = self.manager.finalize(handle, permit(self.clock))

        self.assertEqual("FINALIZED", final["status"])
        self.assertEqual(
            "SAFE_INCOMPLETE_EARLY_TERMINATION",
            final["evidence"]["outcome"],
        )
        self.assertFalse(final["evidence"]["exactTwoHourRuntimeComplete"])
        self.assertTrue(final["evidence"]["baselineFlat"])
        self.assertEqual(Decimal("0.001"), self.broker.base)
        self.assertEqual(2, self.broker.send_count)
        self.assertEqual("FINALIZED", self.manager.status()["phase"])
        self.assertTrue(self.manager.status()["functionalCapabilityReset"])
        self.assertTrue(self.manager.status()["ownerTokenReset"])
        sealed = self.manager.control.authority_snapshot()
        self.assertFalse(sealed["realOrdersEnabled"])
        self.assertFalse(sealed["functionalOnlyRouting"])
        self.assertTrue(sealed["newEntriesBlocked"])

    def test_owner_loss_restart_can_only_take_over_cleanup_and_flatten(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
            self.manager.tick(
                handle,
                permit(self.clock),
                finalized_bar=bar(self.clock, "BUY"),
            )
            self.clock.value += 61

            restarted = self.build_manager()
            recovered = restarted.takeover_expired_cleanup(
                session_id=handle.session_id,
                capability=handle.capability,
                owner_id="owner-process-b",
                owner_token=RECOVERY_TOKEN,
            )
            authority = restarted.control.authority_snapshot()
            self.assertTrue(authority["killSwitch"])
            self.assertTrue(authority["cleanupOnlyAuthority"])
            self.assertTrue(authority["newEntriesBlocked"])
            with self.assertRaisesRegex(BinanceSpotLifecycleError, "owner changed"):
                self.manager.tick(handle, permit(self.clock))
            cleanup = restarted.tick(recovered, permit(self.clock))
            self.assertEqual("CLEANUP_FLATTEN_CLAIMED", cleanup["status"])
            self.assertEqual("ACKNOWLEDGED", cleanup["dispatch"]["status"])
            final = restarted.finalize(recovered, permit(self.clock))

        self.assertEqual("FINALIZED", final["status"])
        self.assertEqual(Decimal("0.001"), self.broker.base)
        self.assertEqual(2, self.broker.send_count)

    def test_hard_crash_after_arm_without_session_audits_to_failed(self) -> None:
        exact = __import__(
            "live_trader.binance_spot_continuous_functional",
            fromlist=["ExactPermit"],
        ).ExactPermit.parse(permit(self.clock), now_epoch=self.clock())
        armed = self.manager.control.arm(
            exact,
            owner_id="crashed-owner-a",
            owner_token=OWNER_TOKEN,
        )
        self.clock.value += 61
        result = self.manager.audit_incomplete_startup()
        self.assertIsInstance(result, dict)
        self.assertEqual("FAILED_NO_SESSION", result["startupRecovery"])
        self.assertEqual("FAILED", self.manager.status()["phase"])
        self.assertTrue(self.manager.status()["functionalCapabilityReset"])
        with self.assertRaisesRegex(Exception, "activation fence changed"):
            parsed_truth = __import__(
                "live_trader.binance_spot_continuous_functional",
                fromlist=["AccountTruth"],
            ).AccountTruth.parse(
                truth(self.clock, session_id=""),
                binding=exact.binding,
                now_epoch=self.clock(),
            )
            self.manager.ledger.create_session(
                exact,
                parsed_truth,
                now_epoch=self.clock(),
                activation_fence={
                    "routeKey": "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m",
                    "revision": int(armed["revision"]),
                    "ownerId": "crashed-owner-a",
                    "ownerTokenHash": __import__("hashlib").sha256(
                        OWNER_TOKEN.encode("utf-8")
                    ).hexdigest(),
                },
            )
        self.assertEqual([], self.manager.ledger.nonterminal_sessions())

    def test_hard_crash_after_core_before_activation_seals_attested_start_failed(self) -> None:
        exact_module = __import__(
            "live_trader.binance_spot_continuous_functional",
            fromlist=["ExactPermit", "AccountTruth"],
        )
        exact = exact_module.ExactPermit.parse(
            permit(self.clock), now_epoch=self.clock()
        )
        self.manager.control.arm(
            exact,
            owner_id="crashed-owner-a",
            owner_token=OWNER_TOKEN,
        )
        raw_truth = truth(self.clock, session_id="")
        parsed_truth = exact_module.AccountTruth.parse(
            raw_truth,
            binding=exact.binding,
            now_epoch=self.clock(),
        )
        session, lost_capability = self.manager.ledger.create_session(
            exact, parsed_truth, now_epoch=self.clock()
        )
        self.clock.value += 61
        recovered = self.manager.audit_incomplete_startup()
        self.assertIsInstance(recovered, dict)
        self.assertEqual("START_FAILED_ATTESTED", recovered["startupRecovery"])
        self.assertEqual(session["session_id"], recovered["session_id"])
        self.assertEqual(0, recovered["brokerMutationCount"])
        self.assertEqual("FAILED", self.manager.status()["phase"])
        authority = self.manager.control.authority_snapshot()
        self.assertFalse(authority["cleanupOnlyAuthority"])
        self.assertFalse(authority["realOrdersEnabled"])
        self.assertTrue(authority["newEntriesBlocked"])
        self.assertEqual([], self.manager.ledger.nonterminal_sessions())
        durable = self.manager.ledger.final_evidence(session["session_id"])
        self.assertEqual(
            "START_FAILED_BEFORE_ACTIVATION",
            durable["evidence"]["outcome"],
        )
        self.assertTrue(
            durable["evidence"]["startupAbortAttestation"][
                "baselineBalancesUnchanged"
            ]
        )
        self.assertEqual(0, self.broker.send_count)
        with self.assertRaises(Exception):
            self.manager.ledger.assert_capability(
                self.manager.ledger.session(session["session_id"]),
                lost_capability,
            )

    def test_startup_abort_retire_crash_is_retried_from_failed_control(self) -> None:
        exact_module = __import__(
            "live_trader.binance_spot_continuous_functional",
            fromlist=["ExactPermit", "AccountTruth"],
        )
        exact = exact_module.ExactPermit.parse(
            permit(self.clock), now_epoch=self.clock()
        )
        self.manager.control.arm(
            exact,
            owner_id="crashed-owner-a",
            owner_token=OWNER_TOKEN,
        )
        parsed_truth = exact_module.AccountTruth.parse(
            truth(self.clock, session_id=""),
            binding=exact.binding,
            now_epoch=self.clock(),
        )
        session, _lost_capability = self.manager.ledger.create_session(
            exact, parsed_truth, now_epoch=self.clock()
        )
        calls: list[dict[str, object]] = []

        def retirer(**kwargs: object) -> dict[str, object]:
            calls.append(dict(kwargs))
            if len(calls) == 1:
                raise RuntimeError("simulated startup archive crash")
            return {
                "retired": True,
                "sessionId": kwargs["session_id"],
                "finalEvidenceHash": kwargs["final_evidence_hash"],
            }

        self.manager.stream_terminal_retirer = retirer
        self.clock.value += 61
        with self.assertRaisesRegex(RuntimeError, "startup archive crash"):
            self.manager.audit_incomplete_startup()
        self.assertEqual("FAILED", self.manager.status()["phase"])
        self.assertEqual("FAILED", self.manager.ledger.session(session["session_id"])["state"])
        resumed = self.manager.audit_incomplete_startup()
        self.assertEqual("START_FAILED_ATTESTED", resumed["startupRecovery"])
        self.assertEqual(2, len(calls))
        self.assertEqual(calls[0]["session_id"], calls[1]["session_id"])
        self.assertEqual(
            calls[0]["final_evidence_hash"], calls[1]["final_evidence_hash"]
        )

    def test_caught_stream_bind_failure_stays_pending_until_official_audit(self) -> None:
        manager = self.build_manager()
        manager.stream_owner_binder = lambda *_: (_ for _ in ()).throw(
            RuntimeError("simulated stream bind failure")
        )
        with self.ready_env(), self.assertRaisesRegex(
            RuntimeError, "stream bind failure"
        ):
            manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
        self.assertEqual("ARMED", manager.status()["phase"])
        nonterminal = manager.ledger.nonterminal_sessions()
        self.assertEqual(1, len(nonterminal))
        self.assertEqual([], manager.ledger.actions(nonterminal[0]["session_id"]))
        self.clock.value += 61
        audited = manager.audit_incomplete_startup()
        self.assertEqual("START_FAILED_ATTESTED", audited["startupRecovery"])
        self.assertEqual("FAILED", manager.status()["phase"])
        self.assertEqual([], manager.ledger.nonterminal_sessions())
        self.assertEqual(0, self.broker.send_count)

    def test_heartbeat_truth_and_cleanup_latch_failure_revokes_every_capability(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
        self.manager.truth_reader.read = lambda **_: (_ for _ in ()).throw(
            RuntimeError("stream truth lost")
        )
        with patch.object(
            self.manager.control,
            "begin_cleanup",
            side_effect=RuntimeError("cleanup CAS failed"),
        ), self.assertRaisesRegex(RuntimeError, "stream truth lost"):
            self.manager.heartbeat(handle)
        self.assertEqual("FAILED", self.manager.status()["phase"])
        session = self.manager.ledger.session(handle.session_id)
        self.assertEqual("RECONCILIATION_REQUIRED", session["state"])
        self.assertEqual("", session["capability_hash"])
        authority = self.manager.control.authority_snapshot()
        self.assertFalse(authority["realOrdersEnabled"])
        self.assertTrue(authority["newEntriesBlocked"])

    def test_expired_active_owner_is_cross_audited_to_new_cleanup_handle(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
        self.clock.value += 61
        recovered = self.manager.audit_incomplete_startup()
        self.assertFalse(isinstance(recovered, dict))
        self.assertEqual(handle.session_id, recovered.session_id)
        self.assertNotEqual(handle.capability, recovered.capability)
        self.assertEqual("CLEANUP", self.manager.status()["phase"])
        with self.assertRaises(Exception):
            self.manager.ledger.assert_capability(
                self.manager.ledger.session(handle.session_id),
                handle.capability,
            )

    def test_owner_loss_limit_triggers_cleanup_before_any_new_signal(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
            self.manager.tick(
                handle,
                permit(self.clock),
                finalized_bar=bar(self.clock, "BUY"),
            )
            # Bought 0.00016 BTC for 9.6 USDT with a 0.01 USDT fee.
            # At 53,812.5 the exact owner loss is 1.00 USDT.
            self.broker.mark = Decimal("53812.5")
            self.broker.sell_quote = Decimal("8.6")
            cleanup = self.manager.tick(handle, permit(self.clock))
            cleanup_authority = self.manager.control.authority_snapshot()[
                "cleanupOnlyAuthority"
            ]
            final = self.manager.finalize(handle, permit(self.clock))

        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", cleanup["status"])
        self.assertEqual("ACKNOWLEDGED", cleanup["dispatch"]["status"])
        self.assertTrue(cleanup_authority)
        self.assertEqual(Decimal("0.001"), self.broker.base)
        self.assertEqual("FINALIZED", final["status"])
        self.assertEqual("1.02", final["evidence"]["ownerLoss"])

    def test_not_sent_preflight_enters_cleanup_without_transport_call(self) -> None:
        handle = self.manager.start(
            permit(self.clock),
            owner_id="owner-process-a",
            owner_token=OWNER_TOKEN,
        )
        result = self.manager.tick(
            handle,
            permit(self.clock),
            finalized_bar=bar(self.clock, "BUY"),
        )
        self.assertEqual("NOT_SENT", result["dispatch"]["status"])
        self.assertEqual(0, self.broker.send_count)
        self.assertEqual("CLEANUP", self.manager.status()["phase"])
        self.assertTrue(
            self.manager.control.authority_snapshot()["cleanupOnlyAuthority"]
        )

    def test_production_entrypoint_remains_hard_unavailable(self) -> None:
        self.assertFalse(PRODUCTION_LIFECYCLE_AVAILABLE)
        self.assertFalse(production_entrypoint_status()["available"])
        blocked = BinanceSpotFunctionalLifecycleManager(
            ledger=self.manager.ledger,
            control=self.manager.control,
            service=self.manager.service,
            truth_reader=self.broker,
            mutation_edge=lambda *_: {},
            clock=self.clock,
        )
        with self.assertRaisesRegex(BinanceSpotLifecycleError, "not production-available"):
            blocked.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )

        production = build_binance_spot_production_lifecycle(
            database_path=Path(self.temporary.name) / "production-graph.sqlite3",
            binding_reader=lambda: binding(),
            publication_proof_path=Path(self.temporary.name) / "missing-proof.json",
            account_fingerprint="c" * 64,
            stream_reader=lambda: (_ for _ in ()).throw(
                AssertionError("unavailable start must not read stream")
            ),
            stream_owner_binder=lambda *_: (_ for _ in ()).throw(
                AssertionError("unavailable start must not bind stream owner")
            ),
            stream_terminal_barrier=lambda: (_ for _ in ()).throw(
                AssertionError("unavailable start must not fence stream")
            ),
            stream_cleanup_recovery_latcher=lambda **_: (_ for _ in ()).throw(
                AssertionError("unavailable start must not latch cleanup")
            ),
            stream_startup_recovery_latcher=lambda **_: (_ for _ in ()).throw(
                AssertionError("unavailable start must not latch startup recovery")
            ),
            dispatch_lease_factory=lambda **_: (_ for _ in ()).throw(
                AssertionError("unavailable start must not acquire dispatch lease")
            ),
            stream_terminal_retirer=lambda **_: (_ for _ in ()).throw(
                AssertionError("unavailable start must not retire stream")
            ),
            activation_permit_issuer=lambda _binding, _now: permit(self.clock),
            clock=self.clock,
        )
        with self.assertRaisesRegex(BinanceSpotLifecycleError, "not production-available"):
            production.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )

    def test_stream_retire_failure_stays_final_reset_and_restart_retries_exact_hash(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
        self.clock.value += 7200
        self.manager.tick(handle, permit(self.clock))
        calls: list[dict[str, object]] = []

        def retirer(**kwargs: object) -> dict[str, object]:
            calls.append(dict(kwargs))
            if len(calls) == 1:
                raise RuntimeError("simulated archive crash")
            return {
                "retired": True,
                "sessionId": kwargs["session_id"],
                "finalEvidenceHash": kwargs["final_evidence_hash"],
            }

        self.manager.stream_terminal_retirer = retirer
        barrier_calls: list[str] = []
        self.manager.stream_terminal_barrier = lambda: (
            barrier_calls.append("closed-and-drained")
            or {
                "barrierClosed": True,
                "readerJoined": True,
                "inBandMarkerReceived": True,
                "terminalMarkerId": "binance-terminal-lifecycle-test-0001",
            }
        )
        with self.assertRaisesRegex(RuntimeError, "archive crash"):
            self.manager.finalize(handle, permit(self.clock))
        self.assertEqual("FINAL_RESET", self.manager.status()["phase"])
        self.assertEqual(
            "FINAL_PREPARED", self.manager.ledger.session(handle.session_id)["state"]
        )
        resumed = self.manager.audit_incomplete_startup()
        self.assertEqual("FINALIZED", self.manager.status()["phase"])
        self.assertEqual(calls[0]["session_id"], calls[1]["session_id"])
        self.assertEqual(
            calls[0]["final_evidence_hash"], calls[1]["final_evidence_hash"]
        )
        self.assertEqual(calls[1]["final_evidence_hash"], resumed["evidenceHash"])
        self.assertEqual(["closed-and-drained"], barrier_calls)

    def test_terminal_barrier_truth_failure_switches_same_process_to_rest_cleanup(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
        self.clock.value += 7200
        self.manager.tick(handle, permit(self.clock))
        broker = self.broker

        class RecoveryReader:
            fail_normal = True

            def read(self, *, baseline_epoch: float, owner_prefix: str):
                if self.fail_normal:
                    raise RuntimeError("final REST snapshot transient failure")
                return broker.read(
                    baseline_epoch=baseline_epoch, owner_prefix=owner_prefix
                )

            def read_cleanup_recovery(
                self, *, baseline_epoch: float, owner_prefix: str
            ):
                value, exchange_rules = broker.read(
                    baseline_epoch=baseline_epoch, owner_prefix=owner_prefix
                )
                value.update(
                    {
                        "externalActivityAbsent": False,
                        "restUserStreamCrossChecked": False,
                        "cleanupRecoveryMode": "REST_RECONCILED_CLEANUP_ONLY",
                        "preservedStreamGap": True,
                        "streamGapEvidenceHash": "8" * 64,
                        "recoveryAttestationHash": "9" * 64,
                        "streamJournalSealHash": "7" * 64,
                        "streamJournalEventCount": 0,
                    }
                )
                return value, exchange_rules

        recovery_reader = RecoveryReader()
        self.manager.truth_reader = recovery_reader
        barrier_calls: list[str] = []
        latch_calls: list[dict[str, object]] = []
        self.manager.stream_terminal_barrier = lambda: (
            barrier_calls.append("in-band-cutoff")
            or {
                "barrierClosed": True,
                "readerJoined": True,
                "inBandMarkerReceived": True,
                "terminalMarkerId": "binance-terminal-recovery-test-0001",
            }
        )
        self.manager.stream_cleanup_recovery_latcher = (
            lambda **kwargs: latch_calls.append(dict(kwargs))
            or {"cleanupRecoveryOnly": True}
        )
        with self.assertRaisesRegex(RuntimeError, "transient failure"):
            self.manager.finalize(handle, permit(self.clock))
        self.assertEqual("CLEANUP", self.manager.status()["phase"])
        self.assertTrue(
            self.manager.ledger.session(handle.session_id)[
                "cleanup_recovery_used"
            ]
        )
        final = self.manager.finalize(handle, permit(self.clock))
        self.assertEqual(["in-band-cutoff"], barrier_calls)
        self.assertEqual(1, len(latch_calls))
        self.assertEqual(
            "SAFE_INCOMPLETE_RECOVERED_STREAM_GAP",
            final["evidence"]["outcome"],
        )

    def test_terminal_barrier_timeout_is_ambiguous_cutover_and_never_retried(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
        self.clock.value += 7200
        self.manager.tick(handle, permit(self.clock))
        calls: list[str] = []
        latches: list[str] = []

        def timed_out_barrier() -> dict[str, object]:
            calls.append("requested")
            raise RuntimeError("terminal marker waiter timed out")

        self.manager.stream_terminal_barrier = timed_out_barrier
        self.manager.stream_cleanup_recovery_latcher = lambda **kwargs: (
            latches.append(str(kwargs["session_id"]))
            or {"cleanupRecoveryOnly": True}
        )
        with self.assertRaisesRegex(RuntimeError, "waiter timed out"):
            self.manager.finalize(handle, permit(self.clock))
        self.assertTrue(
            self.manager.ledger.session(handle.session_id)[
                "cleanup_recovery_used"
            ]
        )
        self.assertEqual([handle.session_id], latches)
        # The late ACK/socket-stop may happen after the waiter error.  The
        # durable recovery flag means no later finalization can ask that dead
        # reader for another barrier.
        self.manager.stream_terminal_barrier = lambda: (_ for _ in ()).throw(
            AssertionError("dead terminal stream barrier was retried")
        )

        original = self.broker.read

        def recovered(*, baseline_epoch: float, owner_prefix: str):
            value, exchange_rules = original(
                baseline_epoch=baseline_epoch, owner_prefix=owner_prefix
            )
            value.update(
                {
                    "externalActivityAbsent": False,
                    "restUserStreamCrossChecked": False,
                    "cleanupRecoveryMode": "REST_RECONCILED_CLEANUP_ONLY",
                    "preservedStreamGap": True,
                    "streamGapEvidenceHash": "5" * 64,
                    "recoveryAttestationHash": "6" * 64,
                    "streamJournalSealHash": "7" * 64,
                    "streamJournalEventCount": 0,
                }
            )
            return value, exchange_rules

        self.broker.read_cleanup_recovery = recovered  # type: ignore[attr-defined]
        final = self.manager.finalize(handle, permit(self.clock))
        self.assertEqual(["requested"], calls)
        self.assertEqual(
            "SAFE_INCOMPLETE_RECOVERED_STREAM_GAP",
            final["evidence"]["outcome"],
        )


if __name__ == "__main__":
    unittest.main()
