from __future__ import annotations

from contextlib import closing, contextmanager
from decimal import Decimal
import hashlib
import json
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
    PREPARED_ACTIVATION_RECEIPT_SCHEMA_VERSION,
    build_binance_spot_production_lifecycle,
    prepared_lifecycle_plan,
    production_entrypoint_status,
)
from live_trader.binance_spot_functional_approval import (
    DurableBinanceSpotApprovedPermitStore,
)
from live_trader.binance_spot_functional_mutation import (
    BinanceSpotFunctionalMutationEdge,
)
from live_trader.binance_spot_functional_transport import binance_api_key_fingerprint
from tests.test_binance_spot_continuous_functional import (
    Clock,
    FakeVerifiedExclusivityGuard,
    bar,
    binding,
    global_authority_reader,
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
        self.read_count = 0

    def read(self, *, baseline_epoch: float, owner_prefix: str):
        self.read_count += 1
        _ = baseline_epoch, owner_prefix
        return (
            truth(
                self.clock,
                historyBaselineAt=self.clock.iso(
                    float(baseline_epoch) - self.clock()
                ),
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
        mutation_release = patch(
            "live_trader.binance_spot_functional_mutation."
            "PRODUCTION_MUTATION_AVAILABLE",
            True,
        )
        mutation_release.start()
        self.addCleanup(mutation_release.stop)
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
            account_exclusivity_guard=FakeVerifiedExclusivityGuard(),
            global_first_live_authority_reader=(
                global_authority_reader(self.clock)
            ),
            clock=self.clock,
            monotonic_clock=self.clock,
        )
        global_reader = global_authority_reader(self.clock)

        @contextmanager
        def global_reservation(**request: object):
            yield global_reader(**request)

        @contextmanager
        def dispatch_lease(**request: object):
            yield lambda: {
                "active": True,
                "sessionId": request["session_id"],
                "claimId": request["claim_id"],
                "ordinaryRoutesClosed": True,
            }

        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=control.authority_snapshot,
            claim_reader=ledger.action,
            claim_marker=lambda claim_id: ledger.mark_post_may_have_crossed(
                claim_id, now_epoch=float(self.clock())
            ),
            dispatch_lease_factory=dispatch_lease,
            global_first_live_dispatch_reservation=global_reservation,
            sender=self.broker.send,
            clock=self.clock,
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

    def approve_supervised_permit(self) -> tuple[str, dict[str, object]]:
        approval_id = "binance-prepared-approval-0001"
        payload = permit(self.clock)
        store = DurableBinanceSpotApprovedPermitStore(
            self.path,
            approval_verifier=lambda _value: True,
            clock=self.clock,
        )
        store.approve(
            payload,
            {
                "approvalId": approval_id,
                "operatorId": "authenticated-operator",
                "operatorAuthenticated": True,
                "operatorApproved": True,
                "permitId": payload["permitId"],
                "permitHash": payload["permitHash"],
                "accountFingerprint": binding()["accountFingerprint"],
                "executionRoute": "BINANCE_SPOT_CONTINUOUS",
                "symbol": "BTCUSDT",
                "approvedAt": self.clock.iso(),
                "nonce": "prepared-approval-nonce-0000000001",
                "activationResealAuthorized": True,
                "activeDurationSeconds": 7200,
                "accountExclusivityProofRequired": True,
                "accountWideCausalClosureProofRequired": True,
                "firstLiveBootstrapAuthorized": True,
                "firstLiveBootstrapRequired": False,
                "firstLiveBootstrapId": "",
                "firstLiveBootstrapHash": "",
                "firstLiveSessionNonceHash": "",
                "firstLiveCodeHash": "",
            },
        )
        self.manager.permit_store = store
        self.manager.assurance_mode = "SUPERVISED_NON_PROMOTION"
        self.manager.activation_permit_issuer = (
            lambda _binding, _now: permit(self.clock)
        )
        return approval_id, payload

    def activation_receipt(self, prepared: object, **updates: object):
        plan = prepared_lifecycle_plan(prepared)  # type: ignore[arg-type]
        body: dict[str, object] = {
            "schemaVersion": PREPARED_ACTIVATION_RECEIPT_SCHEMA_VERSION,
            "assuranceMode": "SUPERVISED_NON_PROMOTION",
            "lane": "BINANCE_SPOT",
            "approvalId": plan["approvalId"],
            "sessionId": plan["sessionId"],
            "permitId": plan["permitId"],
            "permitHash": plan["permitHash"],
            "accountFingerprint": plan["accountFingerprint"],
            "preparedPlanHash": plan["preparedPlanHash"],
            "globalRunId": "crypto-first-live-run-binance-prepared-0001",
            "globalCoordinatorRevision": 7,
            "globalPhase": "ACTIVE",
            "supervisedContractHash": "c" * 64,
            "approvalReceiptHash": "d" * 64,
            "observerCoverageStartedEpoch": plan["preparedEpoch"],
            "observerSnapshotPayloadHash": "e" * 64,
            "exactUserApprovalConsumed": True,
            "oneUse": True,
            "durable": True,
            "restartVerifiable": True,
            "networkCapabilityOpen": True,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
            "observedEpoch": self.clock(),
        }
        body.update(updates)
        encoded = json.dumps(
            body,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return {**body, "receiptHash": hashlib.sha256(encoded).hexdigest()}

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

    def test_supervised_prepare_is_durable_inert_then_exact_receipt_activates(
        self,
    ) -> None:
        approval_id, original = self.approve_supervised_permit()
        prepared = self.manager.prepare_inert(
            {
                "permitId": original["permitId"],
                "permitHash": original["permitHash"],
            },
            approval_id=approval_id,
            owner_id="prepared-owner-process-0001",
            owner_token=OWNER_TOKEN,
        )
        plan = prepared_lifecycle_plan(prepared)
        durable = self.manager.status()
        self.assertEqual("ARMED", durable["phase"])
        self.assertEqual(prepared.session_id, durable["sessionId"])
        self.assertTrue(durable["functionalCapabilityReset"])
        self.assertFalse(plan["networkCapabilityOpen"])
        self.assertFalse(plan["promotionEligible"])
        self.assertFalse(plan["realE2EEligible"])
        self.assertEqual(0, self.broker.read_count)
        self.assertEqual(0, self.broker.send_count)

        self.clock.value += 30
        heartbeat = self.manager.heartbeat_prepared(prepared)
        self.assertEqual("PREPARED_INERT", heartbeat["phase"])
        self.assertFalse(heartbeat["networkCapabilityOpen"])
        self.assertEqual(0, self.broker.read_count)

        handle = self.manager.activate_prepared(
            prepared, self.activation_receipt(prepared)
        )
        self.assertEqual(prepared.session_id, handle.session_id)
        self.assertEqual("ACTIVE", self.manager.status()["phase"])
        self.assertEqual(1, self.broker.read_count)
        self.assertEqual(0, self.broker.send_count)

    def test_supervised_bad_activation_receipt_fails_before_broker_read(self) -> None:
        approval_id, original = self.approve_supervised_permit()
        prepared = self.manager.prepare_inert(
            {
                "permitId": original["permitId"],
                "permitHash": original["permitHash"],
            },
            approval_id=approval_id,
            owner_id="prepared-owner-process-0002",
            owner_token=OWNER_TOKEN,
        )
        bad = self.activation_receipt(
            prepared,
            networkCapabilityOpen=False,
        )
        with self.assertRaisesRegex(
            BinanceSpotLifecycleError, "stale, incomplete, or promotive"
        ):
            self.manager.activate_prepared(prepared, bad)
        self.assertEqual("ARMED", self.manager.status()["phase"])
        self.assertTrue(self.manager.status()["functionalCapabilityReset"])
        self.assertEqual(0, self.broker.read_count)
        self.assertEqual(0, self.broker.send_count)

    def test_supervised_activation_receipt_rejects_noncanonical_identity_and_time(
        self,
    ) -> None:
        approval_id, original = self.approve_supervised_permit()
        prepared = self.manager.prepare_inert(
            {
                "permitId": original["permitId"],
                "permitHash": original["permitHash"],
            },
            approval_id=approval_id,
            owner_id="prepared-owner-process-hostile-0001",
            owner_token=OWNER_TOKEN,
        )
        hostile_updates = (
            {"globalRunId": "x"},
            {"observerCoverageStartedEpoch": float("nan")},
            {"supervisedContractHash": "C" * 64},
            {"globalCoordinatorRevision": True},
        )
        for update in hostile_updates:
            with self.subTest(update=tuple(update)):
                with self.assertRaises(BinanceSpotLifecycleError):
                    self.manager.activate_prepared(
                        prepared, self.activation_receipt(prepared, **update)
                    )
        self.assertEqual("ARMED", self.manager.status()["phase"])
        self.assertEqual(0, self.broker.read_count)
        self.assertEqual(0, self.broker.send_count)

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

    def test_cleanup_and_final_reset_cas_exact_owner_session_revision(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-exact-cas",
                owner_token=OWNER_TOKEN,
            )
            active = self.manager.status()
            cleanup = self.manager.begin_cleanup(
                handle, reason="exact CAS regression"
            )
            reset = self.manager.control.prepare_final_reset(
                session_id=handle.session_id,
                owner_id=handle.owner_id,
                owner_token=handle.owner_token,
            )

        self.assertEqual("CLEANUP", cleanup["phase"])
        self.assertEqual(handle.session_id, cleanup["session_id"])
        self.assertEqual(handle.owner_id, cleanup["owner_id"])
        self.assertEqual(int(active["revision"]) + 1, cleanup["revision"])
        self.assertEqual("FINAL_RESET", reset["phase"])
        self.assertEqual(handle.session_id, reset["session_id"])
        self.assertEqual(handle.owner_id, reset["owner_id"])
        self.assertEqual(int(cleanup["revision"]) + 1, reset["revision"])

    def test_begin_cleanup_stale_precheck_cannot_cross_owner_epoch(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-stale-cleanup",
                owner_token=OWNER_TOKEN,
            )
        stale = self.manager.control._row()
        self.assertIsNotNone(stale)
        replacement_token = "replacement-owner-token-000000000001"
        with closing(self.manager.control._connect()) as connection:
            connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET owner_id=?, owner_token_hash=?, revision=revision+1
                """,
                (
                    "replacement-cleanup-owner",
                    hashlib.sha256(replacement_token.encode("utf-8")).hexdigest(),
                ),
            )
            connection.commit()

        with patch.object(self.manager.control, "_row", return_value=stale):
            with self.assertRaisesRegex(
                BinanceSpotLifecycleError, "owner/session epoch changed"
            ):
                self.manager.begin_cleanup(handle, reason="must not cross epoch")

        durable = self.manager.status()
        self.assertEqual("ACTIVE", durable["phase"])
        self.assertEqual("replacement-cleanup-owner", durable["ownerId"])

    def test_prepare_final_reset_stale_precheck_cannot_cross_owner_epoch(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-stale-final",
                owner_token=OWNER_TOKEN,
            )
        stale = self.manager.control._row()
        self.assertIsNotNone(stale)
        replacement_token = "replacement-final-token-0000000000001"
        with closing(self.manager.control._connect()) as connection:
            connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET owner_id=?, owner_token_hash=?, revision=revision+1
                """,
                (
                    "replacement-final-owner",
                    hashlib.sha256(replacement_token.encode("utf-8")).hexdigest(),
                ),
            )
            connection.commit()

        with patch.object(self.manager.control, "_row", return_value=stale):
            with self.assertRaisesRegex(
                BinanceSpotLifecycleError, "owner/session epoch changed"
            ):
                self.manager.control.prepare_final_reset(
                    session_id=handle.session_id,
                    owner_id=handle.owner_id,
                    owner_token=handle.owner_token,
                )

        durable = self.manager.status()
        self.assertEqual("ACTIVE", durable["phase"])
        self.assertEqual("replacement-final-owner", durable["ownerId"])

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

    def test_supervised_observer_stale_heartbeat_latches_cleanup_only(self) -> None:
        with self.ready_env():
            handle = self.manager.start(
                permit(self.clock),
                owner_id="owner-process-a",
                owner_token=OWNER_TOKEN,
            )
        self.manager.assurance_mode = "SUPERVISED_NON_PROMOTION"
        calls: list[str] = []

        def stale_observer(**request: object) -> dict[str, object]:
            calls.append(str(request["purpose"]))
            raise TimeoutError("observer snapshot older than five seconds")

        self.manager.continuous_exclusivity_health_reader = stale_observer
        with self.assertRaisesRegex(
            BinanceSpotLifecycleError,
            "independent supervised observer health failed closed",
        ):
            self.manager.heartbeat(handle)
        self.assertEqual(["CONTINUOUS_HEALTH"], calls)
        self.assertEqual("CLEANUP", self.manager.status()["phase"])
        authority = self.manager.control.authority_snapshot()
        self.assertTrue(authority["cleanupOnlyAuthority"])
        self.assertTrue(authority["killSwitch"])
        self.assertTrue(authority["newEntriesBlocked"])
        self.assertEqual(0, self.broker.send_count)

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
            global_first_live_dispatch_reservation=lambda **_: (
                _ for _ in ()
            ).throw(
                AssertionError(
                    "unavailable start must not reserve global dispatch"
                )
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
