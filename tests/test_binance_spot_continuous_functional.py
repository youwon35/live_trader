from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from live_trader.binance_spot_continuous_functional import (
    AccountTruth,
    BinanceSpotBoundaryBlocked,
    BinanceSpotContinuousFunctionalService,
    BinanceSpotFunctionalError,
    DuplicateActionClaim,
    DurableFunctionalLedger,
    ExactBinding,
    ExactPermit,
    PRODUCTION_AVAILABLE,
    SymbolRules,
    owner_metrics,
    permit_content_hash,
)
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.functional_test import (
    FunctionalTestBinding,
    FunctionalTestDurationUnit,
    FunctionalTestEnvironment,
    issue_functional_test_permit,
)


PERMIT_ID = "functional-test-binance-spot-0001"
ACCOUNT_FINGERPRINT = hashlib.sha256(
    b"trading-system:binance-spot-account:v1\x00api-key"
).hexdigest()


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


class FakeVerifiedExclusivityGuard:
    def __init__(self, *, terminal_causal_closure: bool = True) -> None:
        self.calls: list[dict[str, object]] = []
        self.records: list[dict[str, object]] = []
        self.terminal_causal_closure = bool(terminal_causal_closure)

    def verify_and_record(self, **request: object) -> dict[str, object]:
        self.calls.append(dict(request))
        causal = bool(
            request["phase"] == "TERMINAL"
            and self.terminal_causal_closure
        )
        proof = {
            "schemaVersion": "test-binance-exclusivity-proof/v1",
            "phase": request["phase"],
            "sessionId": request["session_id"],
            "permitId": request["permit_id"],
            "permitHash": request["permit_hash"],
            "credentialFingerprint": request["credential_fingerprint"],
            "boundaryId": request["boundary_id"],
            "boundaryHash": request["boundary_hash"],
            "causalClosureProven": causal,
        }
        proof_hash = _hash(proof)
        self.records.append(
            {
                "session_id": request["session_id"],
                "phase": request["phase"],
                "boundary_id": request["boundary_id"],
                "proof_hash": proof_hash,
                "proof": proof,
            }
        )
        return {
            "verified": True,
            "phase": request["phase"],
            "sessionId": request["session_id"],
            "boundaryId": request["boundary_id"],
            "proofId": f"proof-{len(self.calls):08d}",
            "proofHash": proof_hash,
            "observedEpoch": 0.0,
            "exclusiveAccountConfirmed": True,
            "noManualTradingConfirmed": True,
            "noBotsConfirmed": True,
            "noOtherApiKeysConfirmed": True,
            "accountWideCausalClosureProven": (
                causal
            ),
            "proof": proof,
            "durable": True,
            "durableProofHash": proof_hash,
            "restartVerifiable": True,
        }

    def session_records(self, session_id: str) -> list[dict[str, object]]:
        return [
            dict(row)
            for row in self.records
            if row["session_id"] == session_id
        ]


def global_authority_reader(clock: "Clock"):
    def read(**request: object) -> dict[str, object]:
        projection: dict[str, object] = {
            "schemaVersion": "crypto-first-live-binance-authority-snapshot/v1",
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "lane": "BINANCE_SPOT",
            "phase": "ACTIVE",
            "runId": "crypto-run-binance-test-0001",
            "sessionId": request["session_id"],
            "permitId": request["permit_id"],
            "permitHash": request["permit_hash"],
            "accountFingerprint": request["account_fingerprint"],
            "ownerLeaseActive": True,
            "entryAuthorityOpen": True,
            "hardStopEpoch": clock() + 7200.0,
            "revision": 1,
            "observedEpoch": clock(),
        }
        return {**projection, "authorityHash": _hash(projection)}

    return read


def permission_proof() -> dict[str, object]:
    proof: dict[str, object] = {
        "accountCanTrade": True,
        "accountType": "SPOT",
        "accountPermissions": ["SPOT"],
        "symbolPermissionSets": [["SPOT", "MARGIN"]],
        "permissionSemantics": "AND_OF_OR_SETS",
        "symbolPermissionsAuthorized": True,
    }
    proof["accountSymbolPermissionProofHash"] = hashlib.sha256(
        json.dumps(
            proof,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return proof


class Clock:
    def __init__(self, value: float = 1_800_000_000.0) -> None:
        # Exact 5-minute boundary.
        self.value = value - (value % 300)
        self.origin = self.value
        self.session_id = ""

    def __call__(self) -> float:
        return self.value

    def iso(self, offset: float = 0) -> str:
        return datetime.fromtimestamp(
            self.value + offset, tz=timezone.utc
        ).isoformat().replace("+00:00", "Z")

    def permit_iso(self, offset: float = 0) -> str:
        return datetime.fromtimestamp(
            self.origin + offset, tz=timezone.utc
        ).isoformat().replace("+00:00", "Z")


def binding(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "strategyArtifactId": "crypto-binance-btc-functional-v1",
        "strategyArtifactHash": "a" * 64,
        "artifactFileSha256": "1" * 64,
        "strategyInstanceId": "crypto-binance-btc-functional-instance-v1",
        "strategyInstanceHash": "b" * 64,
        "instanceFileSha256": "2" * 64,
        "publicationProofHash": "3" * 64,
        "publicationProofFileSha256": "4" * 64,
        "accountFingerprint": ACCOUNT_FINGERPRINT,
        "broker": "BINANCE",
        "venue": "BINANCE_SPOT",
        "asset": "CRYPTO",
        "market": "CRYPTO_SPOT",
        "executionRoute": "BINANCE_SPOT_CONTINUOUS",
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "interval": "5m",
    }
    result.update(updates)
    return result


def shared_permit(clock: Clock, **updates: object) -> dict[str, object]:
    result = issue_functional_test_permit(
        binding=FunctionalTestBinding(
            strategy_artifact_id="crypto-binance-btc-functional-v1",
            strategy_artifact_hash="a" * 64,
            strategy_instance_id="crypto-binance-btc-functional-instance-v1",
            portfolio_required=False,
            portfolio_artifact_id="",
            portfolio_artifact_hash="",
            portfolio_instance_id="",
            account_id=ACCOUNT_FINGERPRINT,
            symbols=("BTCUSDT",),
            market_group="CRYPTO_SPOT",
            execution_route="BINANCE_SPOT_CONTINUOUS",
            settlement_currency="USDT",
            exchanges=("BINANCE_SPOT",),
            symbol_routes=(("BTCUSDT", "BINANCE_SPOT"),),
        ),
        environment=FunctionalTestEnvironment.BINANCE_LIVE,
        duration_value=2,
        duration_unit=FunctionalTestDurationUnit.HOURS,
        now=datetime.fromtimestamp(clock.origin, tz=timezone.utc),
    ).to_dict()
    result["permitId"] = PERMIT_ID
    result.update(updates)
    body = dict(result)
    body.pop("contentHash", None)
    result["contentHash"] = stable_sha256(body)
    return result


def permit(clock: Clock, **updates: object) -> dict[str, object]:
    shared = shared_permit(clock)
    result: dict[str, object] = {
        "schemaVersion": "binance-spot-continuous-functional-v1",
        "permitId": PERMIT_ID,
        "permitHash": "",
        "sharedPermit": shared,
        "sharedPermitContentHash": shared["contentHash"],
        "environment": "BINANCE_LIVE",
        "status": "ACTIVE",
        "functionalOnly": True,
        "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
        "promotionEligible": False,
        # The app-specific permit is activation-resealed.  The shared
        # operator permit above may predate activation, but this exact live
        # extension starts its immutable 7200-second window now.
        "issuedAt": clock.permit_iso(),
        "expiresAt": clock.permit_iso(7200),
        "cleanupDeadlineAt": clock.permit_iso(10800),
        "maxOrderNotional": "10",
        "maxGrossExposure": "10",
        "maxOwnerLoss": "1",
        "maxBuyOrders": 1,
        "maxSellOrders": 1,
        "noReentry": True,
        "allowShort": False,
        "futuresAllowed": False,
        "marginAllowed": False,
        "borrowAllowed": False,
        "transferAllowed": False,
        "withdrawalAllowed": False,
        "activeDurationSeconds": 7200,
        "activationResealRequired": True,
        "exclusiveAccountRequired": True,
        "manualTradingAllowed": False,
        "externalBotsAllowed": False,
        "otherApiKeysAllowed": False,
        "terminalAccountWideCausalProofRequired": True,
        "binding": binding(),
    }
    result.update(updates)
    if "permitHash" not in updates:
        result["permitHash"] = permit_content_hash(result)
    return result


def authority(
    *,
    session_id: str = "",
    capability_hash: str = "",
    final: bool = False,
    **updates: object,
) -> dict[str, object]:
    result: dict[str, object] = {
        "realOrdersEnabled": not final,
        "dryRun": False,
        "killSwitch": False,
        "newEntriesBlocked": True,
        "ordinaryLiveAllowed": False,
        "smokeAllowed": False,
        "functionalOnlyRouting": not final,
        "activePermitId": "" if final else PERMIT_ID,
        "activePermitHash": "",
        "activeSessionId": "" if final else session_id,
        "functionalCapabilityHash": "" if final else capability_hash,
        "cleanupOnlyAuthority": False,
        "cleanupSessionId": "",
        "cleanupCapabilityHash": "",
        "authorityRevision": "authority-revision-1",
    }
    result.update(updates)
    return result


def rules(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        **permission_proof(),
        "exchangeInfoComplete": True,
        "symbol": "BTCUSDT",
        "status": "TRADING",
        "spotTradingAllowed": True,
        "quoteOrderQtyMarketAllowed": True,
        "marginMode": False,
        "futuresMode": False,
        "borrowMode": False,
        "withdrawalAction": False,
        "minQty": "0.00001",
        "maxQty": "10",
        "stepSize": "0.00001",
        "minNotional": "5",
        "maxNotional": "1000000",
        "minNotionalAppliesToMarket": True,
        "maxNotionalAppliesToMarket": False,
        "avgPriceMins": 0,
        "marketReferencePrice": "60000",
        "marketReferenceSource": "BINANCE_TICKER_PRICE",
        "quantityFilterType": "LOT_SIZE",
    }
    result.update(updates)
    return result


def truth(
    clock: Clock,
    *,
    base: str = "0.00100000",
    quote: str = "100",
    mark: str = "60000",
    open_orders: list[dict[str, object]] | None = None,
    closed_orders: list[dict[str, object]] | None = None,
    fills: list[dict[str, object]] | None = None,
    session_id: str | None = None,
    **updates: object,
) -> dict[str, object]:
    bound_session = clock.session_id if session_id is None else session_id
    result: dict[str, object] = {
        **permission_proof(),
        "observedAt": clock.iso(),
        "historyBaselineAt": clock.permit_iso(),
        "historyCutoffAt": clock.iso(),
        "broker": "BINANCE",
        "venue": "BINANCE_SPOT",
        "accountFingerprint": ACCOUNT_FINGERPRINT,
        "accountComplete": True,
        "balancesComplete": True,
        "openOrdersComplete": True,
        "closedOrdersComplete": True,
        "fillsComplete": True,
        "feesComplete": True,
        "balancesScope": "ACCOUNT_ALL_BALANCES",
        "openOrdersScope": "ACCOUNT_ALL_OPEN_ORDERS",
        "closedOrdersScope": "BTCUSDT_ALL_ORDERS_SINCE_BASELINE",
        "fillsScope": "BTCUSDT_ALL_TRADES_SINCE_BASELINE",
        "feesScope": "BTCUSDT_ALL_TRADE_FEES_SINCE_BASELINE",
        "balances": [
            {"asset": "BTC", "free": base, "locked": "0"},
            {"asset": "USDT", "free": quote, "locked": "0"},
        ],
        "openOrders": open_orders or [],
        "closedOrders": closed_orders or [],
        "fills": fills or [],
        "markPrice": mark,
        "externalActivityAbsent": True,
        "restUserStreamCrossChecked": True,
        "userStreamObservedAt": clock.iso(),
        "streamSessionId": bound_session,
        "streamPermitId": PERMIT_ID if bound_session else "",
        "streamPermitHash": (
            str(permit(clock)["permitHash"]) if bound_session else ""
        ),
        "streamJournalSealHash": "5" * 64 if bound_session else "",
        "streamJournalEventCount": 0,
    }
    result.update(updates)
    if "officialRestSnapshot" not in result:
        snapshot = {
            "schemaVersion": "binance-spot-functional-official-rest-set/v1",
            "baselineEpoch": clock.origin,
            "historyCutoffEpoch": clock.value,
            "observedEpoch": clock.value,
            "account": {"balances": list(result["balances"])},
            "openOrders": list(result["openOrders"]),
            "allOrders": list(result["closedOrders"]),
            "myTrades": list(result["fills"]),
            "exchangeInfo": {},
            "tickerPrice": {"symbol": "BTCUSDT", "price": mark},
            "averagePrice": None,
            "normalizedRules": {},
            "requestEnvelopes": {},
        }
        result["officialRestSnapshot"] = snapshot
        result["officialRestTruthHash"] = hashlib.sha256(
            json.dumps(
                snapshot,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
    return result


def stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def natural_evaluation(
    *, close_epoch: float, signal: str, observed_epoch: float
) -> dict[str, object]:
    final_close = "20" if signal.upper() == "BUY" else "1"
    closes = ["10"] * 12 + [final_close]
    raw_rows: list[list[object]] = []
    normalized: list[dict[str, object]] = []
    first_open_ms = int(close_epoch * 1000) - 13 * 300_000
    for index, close_text in enumerate(closes):
        opened_ms = first_open_ms + index * 300_000
        close_value = Decimal(close_text)
        raw = [
            opened_ms,
            close_text,
            str(close_value + 1),
            str(close_value - 1),
            close_text,
            "1",
            opened_ms + 299_999,
            "1",
            1,
            "0.5",
            "0.5",
            "0",
        ]
        raw_rows.append(raw)
        if index >= 2:
            normalized.append(
                {
                    "barId": f"BTCUSDT-5m-{opened_ms}",
                    "openTime": datetime.fromtimestamp(
                        opened_ms / 1000, tz=timezone.utc
                    ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    "barCloseAt": datetime.fromtimestamp(
                        (opened_ms + 300_000) / 1000, tz=timezone.utc
                    ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    "open": close_text,
                    "high": str(close_value + 1),
                    "low": str(close_value - 1),
                    "close": close_text,
                    "volume": "1",
                    "tradeCount": 1,
                    "finalized": True,
                    "closed": True,
                }
            )
    window: dict[str, object] = {
        "schemaVersion": "binance-spot-official-finalized-5m-window-v1",
        "symbol": "BTCUSDT",
        "interval": "5m",
        "source": "BINANCE_SPOT_KLINE",
        "finalized": True,
        "closed": True,
        "barId": normalized[-1]["barId"],
        "barCloseAt": normalized[-1]["barCloseAt"],
        "observedAt": datetime.fromtimestamp(
            observed_epoch, tz=timezone.utc
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "serverTime": int(close_epoch * 1000),
        "rawKlines": raw_rows,
        "rawKlinesHash": stable_hash({"rows": raw_rows}),
        "klineRequest": {
            "endpoint": "/api/v3/klines",
            "query": {"symbol": "BTCUSDT", "interval": "5m", "limit": 13},
        },
        "bars": normalized,
    }
    window_hash = stable_hash(window)
    evaluation_id = "binance-ma-eval-" + stable_hash(
        {
            "windowHash": window_hash,
            "strategyArtifactHash": "a" * 64,
            "strategyInstanceHash": "b" * 64,
        }
    )[:32]
    return {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "executionRoute": "BINANCE_SPOT_CONTINUOUS",
        "strategyArtifactId": "crypto-binance-btc-functional-v1",
        "strategyArtifactHash": "a" * 64,
        "strategyArtifactFileSha256": "1" * 64,
        "strategyInstanceId": "crypto-binance-btc-functional-instance-v1",
        "strategyInstanceHash": "b" * 64,
        "strategyInstanceFileSha256": "2" * 64,
        "publicationProofHash": "3" * 64,
        "publicationProofFileSha256": "4" * 64,
        "accountFingerprint": ACCOUNT_FINGERPRINT,
        "bindingHash": stable_hash(binding()),
        "finalized": True,
        "strategyEvaluationComplete": True,
        "naturalSignal": True,
        "forced": False,
        "barSource": "BINANCE_SPOT_KLINE",
        "barId": window["barId"],
        "barHash": window_hash,
        "barCloseAt": window["barCloseAt"],
        "observedAt": datetime.fromtimestamp(
            observed_epoch, tz=timezone.utc
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "signal": signal.upper(),
        "evaluationId": evaluation_id,
        "strategyPluginId": "moving_average_cross",
        "strategyShortMa": 3,
        "strategyLongMa": 10,
        "officialWindowHash": window_hash,
        "officialWindow": window,
    }


def bar(clock: Clock, signal: str, **updates: object) -> dict[str, object]:
    result = natural_evaluation(
        close_epoch=clock.value - 300,
        signal=signal,
        observed_epoch=clock.value,
    )
    result.update(updates)
    return result


def buy_fill(client_id: str, **updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "tradeId": "trade-buy-0001",
        "clientOrderId": client_id,
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": "0.00015",
        "quoteQuantity": "9",
        "commission": "0",
        "commissionAsset": "USDT",
        "feeQuoteValue": "0.01",
    }
    result.update(updates)
    return result


def sell_fill(client_id: str, **updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "tradeId": "trade-sell-0001",
        "clientOrderId": client_id,
        "symbol": "BTCUSDT",
        "side": "SELL",
        "quantity": "0.00015",
        "quoteQuantity": "9.15",
        "commission": "0.00915",
        "commissionAsset": "USDT",
        "feeQuoteValue": "0.00915",
    }
    result.update(updates)
    return result


def buy_order(client_id: str, **updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "orderId": "binance-buy-order-0001",
        "clientOrderId": client_id,
        "symbol": "BTCUSDT",
        "product": "SPOT",
        "side": "BUY",
        "type": "MARKET",
        "status": "FILLED",
        "origQuoteOrderQty": "10",
        "executedQty": "0.00015",
        "cummulativeQuoteQty": "9",
        "isMargin": False,
        "reduceOnly": False,
    }
    result.update(updates)
    return result


def sell_order(client_id: str, **updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "orderId": "binance-sell-order-0001",
        "clientOrderId": client_id,
        "symbol": "BTCUSDT",
        "product": "SPOT",
        "side": "SELL",
        "type": "MARKET",
        "status": "FILLED",
        "origQty": "0.00015",
        "executedQty": "0.00015",
        "cummulativeQuoteQty": "9.15",
        "isMargin": False,
        "reduceOnly": False,
    }
    result.update(updates)
    return result


class BinanceSpotContinuousFunctionalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.current_authority = authority()
        self.ledger = DurableFunctionalLedger(
            Path(self.temporary.name) / "binance-functional.sqlite3"
        )
        self.exclusivity_guard = FakeVerifiedExclusivityGuard()
        self.global_authority_reader = global_authority_reader(self.clock)
        self.service = BinanceSpotContinuousFunctionalService(
            ledger=self.ledger,
            binding_reader=lambda: binding(),
            authority_reader=lambda: dict(self.current_authority),
            publication_verifier=lambda _: {
                "complete": True,
                "strategyArtifactHash": "a" * 64,
                "artifactFileSha256": "1" * 64,
                "strategyInstanceHash": "b" * 64,
                "instanceFileSha256": "2" * 64,
                "publicationProofHash": "3" * 64,
                "publicationProofFileSha256": "4" * 64,
            },
            account_exclusivity_guard=self.exclusivity_guard,
            global_first_live_authority_reader=(
                self.global_authority_reader
            ),
            clock=self.clock,
            monotonic_clock=self.clock,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def start(self) -> tuple[str, str]:
        self.clock.session_id = ""
        result = self.service.start(permit(self.clock), truth(self.clock))
        session_id = str(result["sessionId"])
        self.clock.session_id = session_id
        capability = str(result["functionalCapability"])
        self.current_authority = authority(
            session_id=session_id,
            capability_hash=str(result["functionalCapabilityHash"]),
            activePermitHash=str(permit(self.clock)["permitHash"]),
        )
        self.service.assert_activation_guards(
            session_id, permit(self.clock)
        )
        return session_id, capability

    def dispatched_round_trip_ready_for_finalize(
        self,
    ) -> tuple[str, str, dict[str, object]]:
        session_id, capability = self.start()
        buy = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            bar(self.clock, "BUY"),
        )
        buy_claim_id = str(buy["claim"]["claim_id"])
        buy_id = str(buy["action"]["clientOrderId"])
        self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            buy_claim_id,
            submitter=lambda _action: buy_order(buy_id),
        )
        self.clock.value += 300
        after_buy = truth(
            self.clock,
            base="0.00115000",
            quote="91",
            closed_orders=[buy_order(buy_id)],
            fills=[buy_fill(buy_id)],
        )
        sell = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            after_buy,
            rules(),
            bar(self.clock, "SELL"),
        )
        sell_claim_id = str(sell["claim"]["claim_id"])
        sell_id = str(sell["action"]["clientOrderId"])
        self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            after_buy,
            rules(),
            sell_claim_id,
            submitter=lambda _action: sell_order(sell_id),
        )
        self.current_authority = authority(final=True)
        self.clock.value += 7200
        return (
            session_id,
            capability,
            truth(
                self.clock,
                base="0.00100000",
                quote="100.15",
                closed_orders=[buy_order(buy_id), sell_order(sell_id)],
                fills=[buy_fill(buy_id), sell_fill(sell_id)],
            ),
        )

    def test_production_lane_stays_unavailable_and_is_not_wired(self) -> None:
        self.assertFalse(PRODUCTION_AVAILABLE)
        import live_trader.state as state

        self.assertFalse(
            hasattr(state, "BINANCE_SPOT_CONTINUOUS_FUNCTIONAL_SERVICE")
        )
        self.assertFalse(
            hasattr(state, "start_binance_spot_continuous_functional")
        )

    def test_exact_route_rejects_futures_margin_other_symbol_and_interval(self) -> None:
        for change in (
            {"venue": "BINANCE_FUTURES"},
            {"market": "FUTURES"},
            {"executionRoute": "BINANCE_FUTURES_CONTINUOUS"},
            {"symbol": "ETHUSDT"},
            {"interval": "1m"},
        ):
            with self.subTest(change=change):
                with self.assertRaises(BinanceSpotFunctionalError):
                    ExactBinding.parse(binding(**change))

    def test_declared_hash_file_sha_and_publication_proof_are_distinct_and_rechecked(self) -> None:
        parsed = ExactBinding.parse(binding())
        self.assertNotEqual(parsed.strategy_artifact_hash, parsed.artifact_file_sha256)
        self.assertNotEqual(parsed.strategy_instance_hash, parsed.instance_file_sha256)
        bad = BinanceSpotContinuousFunctionalService(
            ledger=self.ledger,
            binding_reader=lambda: binding(),
            authority_reader=lambda: authority(),
            publication_verifier=lambda _: {
                "complete": True,
                "strategyArtifactHash": "a" * 64,
                "artifactFileSha256": "9" * 64,
                "strategyInstanceHash": "b" * 64,
                "instanceFileSha256": "2" * 64,
                "publicationProofHash": "3" * 64,
                "publicationProofFileSha256": "4" * 64,
            },
            account_exclusivity_guard=self.exclusivity_guard,
            global_first_live_authority_reader=(
                self.global_authority_reader
            ),
            clock=self.clock,
        )
        with self.assertRaisesRegex(BinanceSpotBoundaryBlocked, "artifactFileSha256"):
            bad.start(permit(self.clock), truth(self.clock))

    def test_permit_is_exact_two_hours_bounded_and_forbids_dangerous_products(self) -> None:
        parsed = ExactPermit.parse(permit(self.clock), now_epoch=self.clock())
        self.assertEqual(7200, parsed.expires_epoch - parsed.issued_epoch)
        self.assertEqual(10800, parsed.cleanup_deadline_epoch - parsed.issued_epoch)
        for change in (
            {"maxOrderNotional": "10.01"},
            {"maxOwnerLoss": "1.01"},
            {"maxBuyOrders": 2},
            {"noReentry": False},
            {"futuresAllowed": True},
            {"marginAllowed": True},
            {"withdrawalAllowed": True},
        ):
            with self.subTest(change=change):
                with self.assertRaises(BinanceSpotFunctionalError):
                    ExactPermit.parse(
                        permit(self.clock, **change), now_epoch=self.clock()
                    )
        tampered = permit(self.clock)
        tampered["maxOwnerLoss"] = "0.5"
        with self.assertRaises(BinanceSpotFunctionalError):
            ExactPermit.parse(tampered, now_epoch=self.clock())
        extra = permit(self.clock)
        extra["unsealedExtension"] = "forbidden"
        extra["permitHash"] = permit_content_hash(extra)
        with self.assertRaises(BinanceSpotFunctionalError):
            ExactPermit.parse(extra, now_epoch=self.clock())

    def test_shared_v2_permit_is_hash_bound_and_cannot_cross_route_or_extension(self) -> None:
        tampered_shared = permit(self.clock)
        tampered_shared["sharedPermit"]["caps"]["maxLoss"] = 0.5
        tampered_shared["permitHash"] = permit_content_hash(tampered_shared)
        with self.assertRaisesRegex(BinanceSpotFunctionalError, "shared v2"):
            ExactPermit.parse(tampered_shared, now_epoch=self.clock())

        cross_route = permit(self.clock)
        upbit = issue_functional_test_permit(
            binding=FunctionalTestBinding(
                strategy_artifact_id="crypto-upbit-btc-functional-v1",
                strategy_artifact_hash="d" * 64,
                strategy_instance_id="crypto-upbit-btc-functional-instance-v1",
                portfolio_required=False,
                portfolio_artifact_id="",
                portfolio_artifact_hash="",
                portfolio_instance_id="",
                account_id="e" * 64,
                symbols=("KRW-BTC",),
                market_group="CRYPTO_SPOT",
                execution_route="UPBIT_KRW_SPOT_CONTINUOUS",
                settlement_currency="KRW",
                exchanges=("UPBIT_SPOT",),
                symbol_routes=(("KRW-BTC", "UPBIT_SPOT"),),
            ),
            environment=FunctionalTestEnvironment.UPBIT_LIVE,
            duration_value=2,
            duration_unit=FunctionalTestDurationUnit.HOURS,
            now=datetime.fromtimestamp(self.clock.origin - 60, tz=timezone.utc),
        ).to_dict()
        upbit["permitId"] = PERMIT_ID
        upbit_body = dict(upbit)
        upbit_body.pop("contentHash", None)
        upbit["contentHash"] = stable_sha256(upbit_body)
        cross_route["sharedPermit"] = upbit
        cross_route["sharedPermitContentHash"] = upbit["contentHash"]
        cross_route["permitHash"] = permit_content_hash(cross_route)
        with self.assertRaisesRegex(BinanceSpotFunctionalError, "environment/duration"):
            ExactPermit.parse(cross_route, now_epoch=self.clock())

        mismatched_extension = permit(
            self.clock,
            binding=binding(
                strategyInstanceId="crypto-binance-btc-functional-instance-v2"
            ),
        )
        with self.assertRaisesRegex(BinanceSpotFunctionalError, "selection extension"):
            ExactPermit.parse(mismatched_extension, now_epoch=self.clock())

    def test_start_requires_global_entry_block_and_closes_ordinary_and_smoke(self) -> None:
        for change in (
            {"newEntriesBlocked": False},
            {"ordinaryLiveAllowed": True},
            {"smokeAllowed": True},
            {"functionalOnlyRouting": False},
        ):
            with self.subTest(change=change):
                self.current_authority = authority(**change)
                with self.assertRaises(BinanceSpotBoundaryBlocked):
                    self.service.start(permit(self.clock), truth(self.clock))

    def test_account_truth_is_account_wide_and_complete(self) -> None:
        for change in (
            {"openOrdersComplete": False},
            {"closedOrdersScope": "BTCUSDT_ONLY"},
            {"fillsComplete": False},
            {"feesComplete": False},
            {"restUserStreamCrossChecked": False},
            {"userStreamObservedAt": self.clock.iso(-16)},
            {"externalActivityAbsent": False},
            {"balances": [{"asset": "USDT", "free": "100", "locked": "0"}]},
        ):
            with self.subTest(change=change):
                with self.assertRaises((BinanceSpotFunctionalError, BinanceSpotBoundaryBlocked)):
                    self.service.start(
                        permit(self.clock), truth(self.clock, **change)
                    )

    def test_history_baseline_is_exactly_bound_to_durable_session_start(self) -> None:
        with self.assertRaisesRegex(BinanceSpotBoundaryBlocked, "prestart history baseline"):
            self.service.start(
                permit(self.clock),
                truth(self.clock, historyBaselineAt=self.clock.iso(-16)),
            )

        session_id, capability = self.start()
        with self.assertRaisesRegex(BinanceSpotBoundaryBlocked, "history baseline changed"):
            self.service.risk_status(
                session_id,
                capability,
                permit(self.clock),
                truth(self.clock, historyBaselineAt=self.clock.iso(-1)),
            )

    def test_exchange_rules_min_notional_and_step_are_fail_closed(self) -> None:
        parsed = SymbolRules.parse(rules())
        self.assertEqual("0.00015", str(parsed.floor_quantity(parsed.min_quantity * 15)))
        for change in (
            {"exchangeInfoComplete": False},
            {"symbol": "ETHUSDT"},
            {"stepSize": "0"},
            {"minNotional": "11"},
            {"marginMode": True},
        ):
            with self.subTest(change=change):
                with self.assertRaises(BinanceSpotFunctionalError):
                    SymbolRules.parse(rules(**change))

    def test_finalized_5m_buy_is_quote_capped_and_duplicate_bar_is_blocked(self) -> None:
        session_id, capability = self.start()
        claimed = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            bar(self.clock, "BUY"),
            buy_notional="10",
        )
        self.assertEqual("BUY", claimed["action"]["kind"])
        self.assertEqual("10", claimed["action"]["quoteOrderQty"])
        self.assertEqual("0", claimed["action"]["quantity"])
        with self.assertRaises((BinanceSpotFunctionalError, BinanceSpotBoundaryBlocked)):
            self.service.observe_bar(
                session_id,
                capability,
                permit(self.clock),
                truth(self.clock),
                rules(),
                bar(self.clock, "BUY"),
            )

    def test_nonfinalized_or_misaligned_bar_never_claims(self) -> None:
        for change in (
            {"finalized": False},
            {"barCloseAt": self.clock.iso(-299)},
            {"interval": "1m"},
            {"naturalSignal": False},
            {"forced": True},
        ):
            with self.subTest(change=change):
                session_id, capability = self.start()
                with self.assertRaises(BinanceSpotFunctionalError):
                    self.service.observe_bar(
                        session_id,
                        capability,
                        permit(self.clock),
                        truth(self.clock),
                        rules(),
                        bar(self.clock, "BUY", **change),
                    )
                self.current_authority = authority(final=True)
                self.ledger.set_session(
                    session_id,
                    finalize=True,
                    final_new_entries_blocked=True,
                    now_epoch=self.clock(),
                )
                self.current_authority = authority()

    def test_durable_claim_precedes_post_and_blind_retry_is_blocked(self) -> None:
        session_id, capability = self.start()
        claimed = self.service.observe_bar(
            session_id, capability, permit(self.clock), truth(self.clock), rules(), bar(self.clock, "BUY")
        )
        calls: list[str] = []

        def submit(action: dict[str, object]) -> dict[str, object]:
            calls.append(self.ledger.action(claimed["claim"]["claim_id"])["state"])
            return {
                "orderId": "binance-order-0001",
                "clientOrderId": action["clientOrderId"],
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "MARKET",
                "status": "FILLED",
            }

        result = self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            claimed["claim"]["claim_id"],
            submitter=submit,
        )
        self.assertEqual(["SUBMITTING"], calls)
        self.assertEqual("ACKNOWLEDGED", result["status"])
        with self.assertRaises(DuplicateActionClaim):
            self.service.dispatch_claim(
                session_id,
                capability,
                permit(self.clock),
                truth(self.clock),
                rules(),
                claimed["claim"]["claim_id"],
                submitter=submit,
            )
        self.assertEqual(1, len(calls))

    def test_ambiguous_post_is_never_retryable(self) -> None:
        session_id, capability = self.start()
        claimed = self.service.observe_bar(
            session_id, capability, permit(self.clock), truth(self.clock), rules(), bar(self.clock, "BUY")
        )
        calls = 0

        def fail(_: dict[str, object]) -> dict[str, object]:
            nonlocal calls
            calls += 1
            raise TimeoutError("unknown broker outcome")

        result = self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            claimed["claim"]["claim_id"],
            submitter=fail,
        )
        self.assertEqual("RECONCILIATION_REQUIRED", result["status"])
        with self.assertRaises(DuplicateActionClaim):
            self.service.dispatch_claim(
                session_id,
                capability,
                permit(self.clock),
                truth(self.clock),
                rules(),
                claimed["claim"]["claim_id"],
                submitter=fail,
            )
        self.assertEqual(1, calls)

    def test_marker_aware_transport_distinguishes_not_sent_from_ambiguous(self) -> None:
        session_id, capability = self.start()
        claimed = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            bar(self.clock, "BUY"),
        )

        def fail_before_marker(
            _: dict[str, object], marker: object
        ) -> dict[str, object]:
            _ = marker
            raise RuntimeError("local request preflight blocked")

        fail_before_marker.functional_marker_aware = True  # type: ignore[attr-defined]
        result = self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            claimed["claim"]["claim_id"],
            submitter=fail_before_marker,
        )
        self.assertEqual("NOT_SENT", result["status"])
        self.assertEqual("NOT_SENT", self.ledger.action(claimed["claim"]["claim_id"])["state"])

        self.current_authority = authority(final=True)
        self.ledger.set_session(
            session_id,
            finalize=True,
            final_new_entries_blocked=True,
            now_epoch=self.clock(),
        )
        self.current_authority = authority()
        session_id, capability = self.start()
        self.clock.value += 300
        claimed = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            bar(self.clock, "BUY"),
        )

        def fail_after_marker(
            _: dict[str, object], marker: object
        ) -> dict[str, object]:
            marker()  # type: ignore[operator]
            self.assertEqual(
                "POST_MAY_HAVE_CROSSED",
                self.ledger.action(claimed["claim"]["claim_id"])["state"],
            )
            raise TimeoutError("socket outcome unknown")

        fail_after_marker.functional_marker_aware = True  # type: ignore[attr-defined]
        result = self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            claimed["claim"]["claim_id"],
            submitter=fail_after_marker,
        )
        self.assertEqual("RECONCILIATION_REQUIRED", result["status"])

    def test_ambiguous_sell_uses_claim_prebalances_and_two_spaced_absence_reads(self) -> None:
        session_id, capability = self.start()
        prefix = "ftb-" + hashlib.sha256(session_id.encode()).hexdigest()[:12] + "-"
        buy_id = prefix + "b"
        buy = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            bar(self.clock, "BUY"),
        )
        self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            buy["claim"]["claim_id"],
            submitter=lambda action: {
                "orderId": "101",
                "clientOrderId": buy_id,
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "MARKET",
                "status": "FILLED",
            },
        )
        bought_truth = truth(
            self.clock,
            base="0.00115",
            quote="91",
            closed_orders=[buy_order(buy_id, orderId="101")],
            fills=[buy_fill(buy_id, commission="0", feeQuoteValue="0")],
        )
        self.clock.value += 300
        sell = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            bought_truth | {
                "observedAt": self.clock.iso(),
                "historyCutoffAt": self.clock.iso(),
                "userStreamObservedAt": self.clock.iso(),
            },
            rules(),
            bar(self.clock, "SELL"),
        )
        sell_id = prefix + "s"

        def crash_after_marker(action: dict[str, object], marker: object) -> dict[str, object]:
            marker()  # type: ignore[operator]
            raise TimeoutError("crash before socket send")

        crash_after_marker.functional_marker_aware = True  # type: ignore[attr-defined]
        dispatched = self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            bought_truth | {
                "observedAt": self.clock.iso(),
                "historyCutoffAt": self.clock.iso(),
                "userStreamObservedAt": self.clock.iso(),
            },
            rules(),
            sell["claim"]["claim_id"],
            submitter=crash_after_marker,
        )
        self.assertEqual("RECONCILIATION_REQUIRED", dispatched["status"])

        self.clock.value += 61
        proof_truth = truth(
            self.clock,
            base="0.00115",
            quote="91",
            closed_orders=[buy_order(buy_id, orderId="101")],
            fills=[buy_fill(buy_id, commission="0", feeQuoteValue="0")],
        )
        proof = {
            "complete": True,
            "symbol": "BTCUSDT",
            "origClientOrderId": sell_id,
            "notFound": True,
            "errorCode": -2013,
            "observedAt": self.clock.iso(),
        }
        first = self.service.prove_ambiguous_not_accepted(
            session_id,
            capability,
            permit(self.clock),
            proof_truth,
            sell["claim"]["claim_id"],
            exact_client_order_query=proof,
        )
        self.assertEqual("RECONCILIATION_REQUIRED", first["state"])
        self.assertEqual(1, first["absence_proof_count"])
        with self.assertRaises(BinanceSpotBoundaryBlocked):
            self.service.prove_ambiguous_not_accepted(
                session_id,
                capability,
                permit(self.clock),
                proof_truth,
                sell["claim"]["claim_id"],
                exact_client_order_query=proof,
            )

        self.clock.value += 5
        proof_truth = truth(
            self.clock,
            base="0.00115",
            quote="91",
            closed_orders=[buy_order(buy_id, orderId="101")],
            fills=[buy_fill(buy_id, commission="0", feeQuoteValue="0")],
        )
        proof["observedAt"] = self.clock.iso()
        second = self.service.prove_ambiguous_not_accepted(
            session_id,
            capability,
            permit(self.clock),
            proof_truth,
            sell["claim"]["claim_id"],
            exact_client_order_query=proof,
        )
        self.assertEqual("AMBIGUOUS_PROVEN_NOT_ACCEPTED", second["state"])
        self.assertEqual(2, second["absence_proof_count"])
        recovered = self.service.recover(
            session_id,
            capability,
            permit(self.clock),
            proof_truth,
            rules(),
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", recovered["status"])

    def test_capability_is_exact_and_global_real_orders_is_insufficient(self) -> None:
        session_id, capability = self.start()
        claimed = self.service.observe_bar(
            session_id, capability, permit(self.clock), truth(self.clock), rules(), bar(self.clock, "BUY")
        )
        called = False

        def submit(_: dict[str, object]) -> dict[str, object]:
            nonlocal called
            called = True
            return {}

        self.current_authority = authority(
            session_id=session_id,
            capability_hash="e" * 64,
            realOrdersEnabled=True,
        )
        with self.assertRaises(BinanceSpotBoundaryBlocked):
            self.service.dispatch_claim(
                session_id,
                capability,
                permit(self.clock),
                truth(self.clock),
                rules(),
                claimed["claim"]["claim_id"],
                submitter=submit,
            )
        self.assertFalse(called)

    def test_owner_loss_and_account_delta_are_fail_closed(self) -> None:
        session_id, capability = self.start()
        buy_id = f"ftb-" + __import__("hashlib").sha256(session_id.encode()).hexdigest()[:12] + "-b"
        bad_delta = truth(
            self.clock,
            base="0.00114000",
            fills=[buy_fill(buy_id)],
        )
        with self.assertRaises(BinanceSpotFunctionalError):
            self.service.recover(
                session_id, capability, permit(self.clock), bad_delta, rules()
            )
        loss_truth = truth(
            self.clock,
            base="0.00115000",
            mark="50000",
            fills=[buy_fill(buy_id, quoteQuantity="9", feeQuoteValue="0.01")],
        )
        cleanup = self.service.recover(
            session_id, capability, permit(self.clock), loss_truth, rules()
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", cleanup["status"])
        self.assertEqual("SELL", cleanup["action"]["kind"])
        self.assertTrue(cleanup["action"]["cleanupOnly"])

    def test_cleanup_cancels_only_owned_order_then_flattens_owned_decimal_quantity(self) -> None:
        session_id, capability = self.start()
        buy_id = f"ftb-" + __import__("hashlib").sha256(session_id.encode()).hexdigest()[:12] + "-b"
        owned_order = {
            "orderId": "owned-order-1",
            "clientOrderId": buy_id,
            "symbol": "BTCUSDT",
            "product": "SPOT",
            "side": "BUY",
            "status": "NEW",
            "isMargin": False,
            "reduceOnly": False,
        }
        self.clock.value += 7200
        canceled = self.service.recover(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock, open_orders=[owned_order]),
                rules(),
            )
        self.assertEqual("CLEANUP_CANCEL_CLAIMED", canceled["status"])
        self.assertEqual("owned-order-1", canceled["action"]["brokerOrderId"])

        # Use a fresh session for the filled position branch.
        self.current_authority = authority(final=True)
        self.ledger.set_session(
            session_id,
            finalize=True,
            final_new_entries_blocked=True,
            now_epoch=self.clock(),
        )
        self.clock.value -= 7200
        self.current_authority = authority()
        session_id, capability = self.start()
        buy_id = f"ftb-" + hashlib.sha256(session_id.encode()).hexdigest()[:12] + "-b"
        self.clock.value += 7200
        flattened = self.service.recover(
            session_id,
            capability,
            permit(self.clock),
            truth(
                self.clock,
                base="0.00115000",
                mark="100000",
                fills=[buy_fill(buy_id)],
            ),
            rules(),
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", flattened["status"])
        self.assertEqual("0.00015", flattened["action"]["quantity"])
        # Price appreciation makes this SELL worth 15 USDT. It is still an
        # exact owned position reduction and must not be blocked by entry cap.

    def test_btc_commission_reduces_inventory_without_double_counting_loss(self) -> None:
        session_id, _ = self.start()
        buy_id = (
            "ftb-" + hashlib.sha256(session_id.encode()).hexdigest()[:12] + "-b"
        )
        parsed = AccountTruth.parse(
            truth(
                self.clock,
                base="0.001149833333333333333333333333",
                quote="91",
                closed_orders=[buy_order(buy_id)],
                fills=[
                    buy_fill(
                        buy_id,
                        commission="0.000000166666666666666666666667",
                        commissionAsset="BTC",
                        feeQuoteValue="0.01",
                    )
                ],
            ),
            binding=ExactBinding.parse(binding()),
            now_epoch=self.clock(),
        )
        metrics = owner_metrics(
            parsed,
            session_id=session_id,
            baseline_base=Decimal("0.001"),
        )
        self.assertEqual(Decimal("0.01"), metrics["feesQuote"])
        self.assertEqual(Decimal("0"), metrics["cashFeesQuote"])
        self.assertEqual(
            Decimal("0.01000000"),
            metrics["ownerLoss"].quantize(Decimal("0.00000001")),
        )

    def test_restart_recovers_same_durable_session_and_blocks_second_buy(self) -> None:
        session_id, capability = self.start()
        first = self.service.observe_bar(
            session_id, capability, permit(self.clock), truth(self.clock), rules(), bar(self.clock, "BUY")
        )
        restarted = BinanceSpotContinuousFunctionalService(
            ledger=DurableFunctionalLedger(self.ledger.path),
            binding_reader=lambda: binding(),
            authority_reader=lambda: dict(self.current_authority),
            publication_verifier=lambda _: {
                "complete": True,
                "strategyArtifactHash": "a" * 64,
                "artifactFileSha256": "1" * 64,
                "strategyInstanceHash": "b" * 64,
                "instanceFileSha256": "2" * 64,
                "publicationProofHash": "3" * 64,
                "publicationProofFileSha256": "4" * 64,
            },
            account_exclusivity_guard=self.exclusivity_guard,
            global_first_live_authority_reader=(
                self.global_authority_reader
            ),
            clock=self.clock,
        )
        self.assertEqual("CLAIMED", restarted.ledger.action(first["claim"]["claim_id"])["state"])
        self.clock.value += 300
        with self.assertRaises((BinanceSpotBoundaryBlocked, DuplicateActionClaim)):
            restarted.observe_bar(
                session_id,
                capability,
                permit(self.clock),
                truth(self.clock),
                rules(),
                bar(self.clock, "BUY", barCloseAt=self.clock.iso(-300)),
            )

    def test_final_baseline_flat_no_signal_requires_capability_reset(self) -> None:
        session_id, capability = self.start()
        with self.assertRaises(BinanceSpotBoundaryBlocked):
            self.service.finalize(
                session_id, capability, permit(self.clock), truth(self.clock)
            )
        self.current_authority = authority(final=True)
        final = self.service.finalize(
            session_id, capability, permit(self.clock), truth(self.clock)
        )
        self.assertEqual("INCONCLUSIVE_NO_SIGNAL", final["evidence"]["outcome"])
        self.assertTrue(final["evidence"]["functionalCapabilityReset"])

    def test_complete_buy_sell_fee_truth_finalizes_and_keeps_entry_blocked(self) -> None:
        session_id, capability, final_truth = (
            self.dispatched_round_trip_ready_for_finalize()
        )
        final = self.service.finalize(
            session_id, capability, permit(self.clock), final_truth, rules()
        )
        self.assertEqual("FINALIZED", final["status"])
        self.assertTrue(final["evidence"]["baselineFlat"])
        self.assertTrue(final["evidence"]["newEntriesBlocked"])
        self.assertTrue(final["evidence"]["functionalCapabilityReset"])
        self.assertEqual(2, final["evidence"]["reconciledActionCount"])
        self.assertFalse(final["evidence"]["productionAvailable"])
        self.assertEqual(
            "PASS_FULL_ROUND_TRIP",
            final["evidence"]["outcome"],
        )
        self.assertTrue(
            final["evidence"]["accountWideCausalClosureProven"]
        )
        self.assertFalse(
            final["evidence"]["nativeAccountWideCausalClosureProven"]
        )
        self.assertTrue(final["evidence"]["functionalWiringPassed"])
        self.assertTrue(
            final["evidence"]["accountExclusivityPhaseChainComplete"]
        )
        self.assertTrue(
            final["evidence"]["accountExclusivityRestartVerifiable"]
        )
        self.assertEqual(5, final["evidence"]["accountExclusivityPhaseProofCount"])
        self.assertTrue(final["evidence"]["runtimeClockConsistencyProven"])
        self.assertEqual("7500", final["evidence"]["monotonicRuntimeSeconds"])

    def test_terminal_causal_false_can_finalize_but_can_never_pass(self) -> None:
        self.exclusivity_guard.terminal_causal_closure = False
        session_id, capability, final_truth = (
            self.dispatched_round_trip_ready_for_finalize()
        )
        final = self.service.finalize(
            session_id, capability, permit(self.clock), final_truth, rules()
        )
        self.assertEqual("FINALIZED", final["status"])
        self.assertEqual(
            "SAFE_INCOMPLETE_ACCOUNT_WIDE_CAUSAL_CLOSURE_UNPROVEN",
            final["evidence"]["outcome"],
        )
        self.assertTrue(
            final["evidence"]["accountExclusivityPhaseChainComplete"]
        )
        self.assertFalse(final["evidence"]["accountWideCausalClosureProven"])
        self.assertFalse(final["evidence"]["functionalWiringPassed"])

    def test_missing_pre_post_phase_proof_can_finalize_but_can_never_pass(self) -> None:
        session_id, capability, final_truth = (
            self.dispatched_round_trip_ready_for_finalize()
        )
        removed = False
        retained: list[dict[str, object]] = []
        for row in self.exclusivity_guard.records:
            if row["phase"] == "PRE_POST" and not removed:
                removed = True
                continue
            retained.append(row)
        self.exclusivity_guard.records = retained
        self.assertTrue(removed)
        final = self.service.finalize(
            session_id, capability, permit(self.clock), final_truth, rules()
        )
        self.assertEqual("FINALIZED", final["status"])
        self.assertEqual(
            "SAFE_INCOMPLETE_ACCOUNT_EXCLUSIVITY_PHASE_CHAIN_UNPROVEN",
            final["evidence"]["outcome"],
        )
        self.assertFalse(
            final["evidence"]["accountExclusivityPhaseChainComplete"]
        )
        self.assertFalse(
            final["evidence"]["accountExclusivityRestartVerifiable"]
        )
        self.assertFalse(final["evidence"]["functionalWiringPassed"])

    def test_post_round_trip_buy_signal_is_no_reentry_not_runner_exception(self) -> None:
        session_id, capability = self.start()
        buy = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            bar(self.clock, "BUY"),
        )
        buy_id = str(buy["action"]["clientOrderId"])
        self.ledger.transition_action(
            str(buy["claim"]["claim_id"]),
            expected_state="CLAIMED",
            state="ACKNOWLEDGED",
            now_epoch=self.clock(),
            broker_order_id="binance-buy-order-0001",
        )
        self.clock.value += 300
        after_buy = truth(
            self.clock,
            base="0.00115000",
            quote="91",
            closed_orders=[buy_order(buy_id)],
            fills=[buy_fill(buy_id)],
        )
        sell = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            after_buy,
            rules(),
            bar(self.clock, "SELL"),
        )
        sell_id = str(sell["action"]["clientOrderId"])
        self.ledger.transition_action(
            str(sell["claim"]["claim_id"]),
            expected_state="CLAIMED",
            state="ACKNOWLEDGED",
            now_epoch=self.clock(),
            broker_order_id="binance-sell-order-0001",
        )
        self.clock.value += 300
        result = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(
                self.clock,
                base="0.00100000",
                quote="100.15",
                closed_orders=[buy_order(buy_id), sell_order(sell_id)],
                fills=[buy_fill(buy_id), sell_fill(sell_id)],
            ),
            rules(),
            bar(self.clock, "BUY"),
        )
        self.assertEqual("NO_REENTRY_CAP_REACHED", result["status"])
        self.assertIsNone(result["action"])
        self.assertEqual(2, len(self.ledger.actions(session_id)))

    def test_sticky_stream_gap_rest_cleanup_flattens_but_can_never_pass(self) -> None:
        session_id, capability = self.start()
        buy = self.service.observe_bar(
            session_id,
            capability,
            permit(self.clock),
            truth(self.clock),
            rules(),
            bar(self.clock, "BUY"),
        )
        buy_id = str(buy["action"]["clientOrderId"])
        self.ledger.transition_action(
            str(buy["claim"]["claim_id"]),
            expected_state="CLAIMED",
            state="ACKNOWLEDGED",
            now_epoch=self.clock(),
            broker_order_id="binance-buy-order-gap-recovery",
        )
        recovery_fields = {
            "restUserStreamCrossChecked": False,
            "externalActivityAbsent": False,
            "cleanupRecoveryMode": "REST_RECONCILED_CLEANUP_ONLY",
            "preservedStreamGap": True,
            "streamGapEvidenceHash": "7" * 64,
            "recoveryAttestationHash": "8" * 64,
        }
        bought = truth(
            self.clock,
            base="0.00115000",
            quote="91",
            closed_orders=[buy_order(buy_id)],
            fills=[buy_fill(buy_id)],
            **recovery_fields,
        )
        with self.assertRaisesRegex(
            BinanceSpotBoundaryBlocked, "cleanup-only"
        ):
            self.service.observe_bar(
                session_id,
                capability,
                permit(self.clock),
                bought,
                rules(),
                bar(self.clock, "SELL"),
            )
        self.current_authority = authority(
            session_id=session_id,
            capability_hash=__import__("hashlib").sha256(
                capability.encode("utf-8")
            ).hexdigest(),
            activePermitHash=str(permit(self.clock)["permitHash"]),
            killSwitch=True,
            cleanupOnlyAuthority=True,
            cleanupSessionId=session_id,
            cleanupCapabilityHash=__import__("hashlib").sha256(
                capability.encode("utf-8")
            ).hexdigest(),
        )
        cleanup = self.service.recover(
            session_id,
            capability,
            permit(self.clock),
            bought,
            rules(),
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", cleanup["status"])
        cleanup_id = str(cleanup["action"]["clientOrderId"])
        self.ledger.transition_action(
            str(cleanup["claim"]["claim_id"]),
            expected_state="CLAIMED",
            state="ACKNOWLEDGED",
            now_epoch=self.clock(),
            broker_order_id="binance-cleanup-order-gap-recovery",
        )
        self.current_authority = authority(final=True)
        final = self.service.finalize(
            session_id,
            capability,
            permit(self.clock),
            truth(
                self.clock,
                base="0.00100000",
                quote="100.15",
                closed_orders=[
                    buy_order(buy_id),
                    sell_order(cleanup_id),
                ],
                fills=[buy_fill(buy_id), sell_fill(cleanup_id)],
                **recovery_fields,
            ),
            rules(),
        )
        self.assertEqual(
            "SAFE_INCOMPLETE_RECOVERED_STREAM_GAP",
            final["evidence"]["outcome"],
        )
        self.assertTrue(
            final["evidence"]["privateStreamGapRecoveredCleanupOnly"]
        )
        self.assertFalse(final["evidence"]["promotionEligible"])

    def test_kill_allows_only_exact_owned_cleanup_sell_with_separate_authority(self) -> None:
        session_id, capability = self.start()
        buy_action = {
            "kind": "BUY",
            "product": "SPOT",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "orderType": "MARKET",
            "quoteOrderQty": "10",
            "quantity": "0",
            "clientOrderId": f"ftb-" + __import__("hashlib").sha256(session_id.encode()).hexdigest()[:12] + "-b",
            "functionalOnly": True,
            "cleanupOnly": False,
            "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
        }
        buy_claim = self.ledger.claim_action(
            session_id, buy_action, now_epoch=self.clock()
        )
        self.ledger.transition_action(
            buy_claim["claim_id"],
            expected_state="CLAIMED",
            state="ACKNOWLEDGED",
            now_epoch=self.clock(),
            broker_order_id="binance-buy-order-0001",
        )
        self.clock.value += 7200
        position_truth = truth(
            self.clock,
            base="0.00115000",
            quote="91",
            mark="100000",
            closed_orders=[buy_order(buy_action["clientOrderId"])],
            fills=[buy_fill(buy_action["clientOrderId"])],
        )
        cleanup = self.service.recover(
            session_id,
            capability,
            permit(self.clock),
            position_truth,
            rules(),
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", cleanup["status"])
        cap_hash = str(self.current_authority["functionalCapabilityHash"])
        self.current_authority.update(
            {
                "killSwitch": True,
                "cleanupOnlyAuthority": True,
                "cleanupSessionId": session_id,
                "cleanupCapabilityHash": cap_hash,
            }
        )
        calls: list[dict[str, object]] = []

        def submit(action: dict[str, object]) -> dict[str, object]:
            calls.append(action)
            return {
                "orderId": "binance-sell-order-0001",
                "clientOrderId": action["clientOrderId"],
                "symbol": "BTCUSDT",
                "side": "SELL",
                "type": "MARKET",
                "status": "FILLED",
            }

        dispatched = self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            position_truth,
            rules(),
            cleanup["claim"]["claim_id"],
            submitter=submit,
        )
        self.assertEqual("ACKNOWLEDGED", dispatched["status"])
        self.assertEqual("SELL", calls[0]["kind"])
        self.assertTrue(calls[0]["cleanupOnly"])
        self.assertEqual("0.00015", calls[0]["quantity"])

    def test_cleanup_never_cancels_external_working_order(self) -> None:
        session_id, capability = self.start()
        self.clock.value += 7200
        external = {
            "orderId": "external-order-1",
            "clientOrderId": "operator-external-order-1",
            "symbol": "BTCUSDT",
            "product": "SPOT",
            "side": "BUY",
            "type": "LIMIT",
            "status": "NEW",
            "isMargin": False,
            "reduceOnly": False,
        }
        recovered = self.service.recover(
            session_id,
            capability,
            permit(self.clock),
            truth(
                self.clock,
                open_orders=[external],
                externalActivityAbsent=False,
            ),
            rules(),
        )
        self.assertEqual("RECONCILIATION_REQUIRED", recovered["status"])
        self.assertEqual(
            "external-account-activity-observed-no-cancel",
            recovered["reason"],
        )
        self.assertIsNone(recovered["action"])
        self.assertEqual([], self.ledger.actions(session_id))

    def test_partial_cleanup_can_cancel_then_use_one_exact_residual_generation(self) -> None:
        session_id, capability = self.start()
        prefix = "ftb-" + hashlib.sha256(session_id.encode()).hexdigest()[:12] + "-"
        buy_id = prefix + "b"
        buy_action = {
            "kind": "BUY",
            "product": "SPOT",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "orderType": "MARKET",
            "quoteOrderQty": "10",
            "quantity": "0",
            "clientOrderId": buy_id,
            "functionalOnly": True,
            "cleanupOnly": False,
            "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
        }
        buy_claim = self.ledger.claim_action(
            session_id,
            buy_action,
            now_epoch=self.clock(),
            pre_base_total=Decimal("0.001"),
            pre_quote_total=Decimal("100"),
        )
        self.ledger.transition_action(
            buy_claim["claim_id"],
            expected_state="CLAIMED",
            state="ACKNOWLEDGED",
            now_epoch=self.clock(),
            broker_order_id="101",
        )
        self.clock.value += 7200
        bought = truth(
            self.clock,
            base="0.00115",
            quote="91",
            closed_orders=[buy_order(buy_id, orderId="101")],
            fills=[buy_fill(buy_id, commission="0", feeQuoteValue="0")],
        )
        first = self.service.recover(
            session_id, capability, permit(self.clock), bought, rules()
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", first["status"])
        self.assertTrue(str(first["action"]["clientOrderId"]).endswith("-f"))
        cap_hash = str(self.current_authority["functionalCapabilityHash"])
        self.current_authority.update(
            {
                "killSwitch": True,
                "cleanupOnlyAuthority": True,
                "cleanupSessionId": session_id,
                "cleanupCapabilityHash": cap_hash,
            }
        )
        first_id = str(first["action"]["clientOrderId"])
        self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            bought,
            rules(),
            first["claim"]["claim_id"],
            submitter=lambda action: {
                "orderId": "201",
                "clientOrderId": first_id,
                "symbol": "BTCUSDT",
                "side": "SELL",
                "type": "MARKET",
                "status": "PARTIALLY_FILLED",
            },
        )
        partial_order = sell_order(
            first_id,
            orderId="201",
            status="PARTIALLY_FILLED",
            origQty="0.00015",
            executedQty="0.00005",
            cummulativeQuoteQty="3.05",
        )
        partial_fill = sell_fill(
            first_id,
            tradeId="trade-cleanup-1",
            quantity="0.00005",
            quoteQuantity="3.05",
            commission="0",
            feeQuoteValue="0",
        )
        partial = truth(
            self.clock,
            base="0.00110",
            quote="94.05",
            open_orders=[partial_order],
            closed_orders=[buy_order(buy_id, orderId="101")],
            fills=[
                buy_fill(buy_id, commission="0", feeQuoteValue="0"),
                partial_fill,
            ],
        )
        cancel = self.service.recover(
            session_id, capability, permit(self.clock), partial, rules()
        )
        self.assertEqual("CLEANUP_CANCEL_CLAIMED", cancel["status"])
        self.assertTrue(str(cancel["action"]["clientOrderId"]).endswith("-c"))
        self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            partial,
            rules(),
            cancel["claim"]["claim_id"],
            submitter=lambda action: {
                "orderId": "201",
                "origClientOrderId": first_id,
                "symbol": "BTCUSDT",
                "status": "CANCELED",
            },
        )
        canceled_order = dict(partial_order, status="CANCELED")
        canceled = truth(
            self.clock,
            base="0.00110",
            quote="94.05",
            closed_orders=[buy_order(buy_id, orderId="101"), canceled_order],
            fills=[
                buy_fill(buy_id, commission="0", feeQuoteValue="0"),
                partial_fill,
            ],
        )
        second = self.service.recover(
            session_id, capability, permit(self.clock), canceled, rules()
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", second["status"])
        self.assertTrue(str(second["action"]["clientOrderId"]).endswith("-f2"))
        self.assertEqual("0.0001", second["action"]["quantity"])
        second_id = str(second["action"]["clientOrderId"])
        self.service.dispatch_claim(
            session_id,
            capability,
            permit(self.clock),
            canceled,
            rules(),
            second["claim"]["claim_id"],
            submitter=lambda action: {
                "orderId": "202",
                "clientOrderId": second_id,
                "symbol": "BTCUSDT",
                "side": "SELL",
                "type": "MARKET",
                "status": "FILLED",
            },
        )
        final_truth = truth(
            self.clock,
            base="0.001",
            quote="100.15",
            closed_orders=[
                buy_order(buy_id, orderId="101"),
                canceled_order,
                sell_order(
                    second_id,
                    orderId="202",
                    origQty="0.0001",
                    executedQty="0.0001",
                    cummulativeQuoteQty="6.1",
                ),
            ],
            fills=[
                buy_fill(buy_id, commission="0", feeQuoteValue="0"),
                partial_fill,
                sell_fill(
                    second_id,
                    tradeId="trade-cleanup-2",
                    quantity="0.0001",
                    quoteQuantity="6.1",
                    commission="0",
                    feeQuoteValue="0",
                ),
            ],
        )
        self.current_authority = authority(final=True)
        finalized = self.service.finalize(
            session_id, capability, permit(self.clock), final_truth, rules()
        )
        self.assertEqual(
            "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT",
            finalized["evidence"]["outcome"],
        )
        self.assertTrue(finalized["evidence"]["cleanupWiringPassed"])
        self.assertEqual(4, finalized["evidence"]["reconciledActionCount"])

    def test_three_partial_cleanup_targets_get_distinct_cancels_and_next_residual(self) -> None:
        session_id, capability = self.start()
        prefix = "ftb-" + hashlib.sha256(session_id.encode()).hexdigest()[:12] + "-"
        buy_id = prefix + "b"
        buy_action = {
            "kind": "BUY",
            "product": "SPOT",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "orderType": "MARKET",
            "quoteOrderQty": "10",
            "quantity": "0",
            "clientOrderId": buy_id,
            "functionalOnly": True,
            "cleanupOnly": False,
            "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
        }
        buy_claim = self.ledger.claim_action(
            session_id,
            buy_action,
            now_epoch=self.clock(),
            pre_base_total=Decimal("0.001"),
            pre_quote_total=Decimal("100"),
        )
        self.ledger.transition_action(
            buy_claim["claim_id"],
            expected_state="CLAIMED",
            state="ACKNOWLEDGED",
            now_epoch=self.clock(),
            broker_order_id="101",
        )
        self.clock.value += 7200
        closed = [buy_order(buy_id, orderId="101")]
        fills = [buy_fill(buy_id, commission="0", feeQuoteValue="0")]
        remaining = Decimal("0.00015")
        quote_total = Decimal("91")

        for generation in range(1, 4):
            flat_truth = truth(
                self.clock,
                base=format(Decimal("0.001") + remaining, "f"),
                quote=format(quote_total, "f"),
                closed_orders=list(closed),
                fills=list(fills),
            )
            sell = self.service.recover(
                session_id, capability, permit(self.clock), flat_truth, rules()
            )
            expected_sell_suffix = "-f" if generation == 1 else f"-f{generation}"
            self.assertTrue(
                str(sell["action"]["clientOrderId"]).endswith(expected_sell_suffix)
            )
            sell_id = str(sell["action"]["clientOrderId"])
            order_id = str(300 + generation)
            self.ledger.transition_action(
                sell["claim"]["claim_id"],
                expected_state="CLAIMED",
                state="ACKNOWLEDGED",
                now_epoch=self.clock(),
                broker_order_id=order_id,
            )
            partial_order = sell_order(
                sell_id,
                orderId=order_id,
                status="PARTIALLY_FILLED",
                origQty=str(sell["action"]["quantity"]),
                executedQty="0.00001",
                cummulativeQuoteQty="0.61",
            )
            partial_fill = sell_fill(
                sell_id,
                tradeId=f"trade-cleanup-partial-{generation}",
                quantity="0.00001",
                quoteQuantity="0.61",
                commission="0",
                feeQuoteValue="0",
            )
            remaining -= Decimal("0.00001")
            quote_total += Decimal("0.61")
            working_truth = truth(
                self.clock,
                base=format(Decimal("0.001") + remaining, "f"),
                quote=format(quote_total, "f"),
                open_orders=[partial_order],
                closed_orders=list(closed),
                fills=[*fills, partial_fill],
            )
            cancel = self.service.recover(
                session_id, capability, permit(self.clock), working_truth, rules()
            )
            expected_cancel_suffix = "-c" if generation == 1 else f"-c{generation}"
            self.assertTrue(
                str(cancel["action"]["clientOrderId"]).endswith(
                    expected_cancel_suffix
                )
            )
            self.assertEqual(sell_id, cancel["action"]["origClientOrderId"])
            self.ledger.transition_action(
                cancel["claim"]["claim_id"],
                expected_state="CLAIMED",
                state="ACKNOWLEDGED",
                now_epoch=self.clock(),
                broker_order_id=order_id,
            )
            closed.append(dict(partial_order, status="CANCELED"))
            fills.append(partial_fill)

        after_third_cancel = truth(
            self.clock,
            base=format(Decimal("0.001") + remaining, "f"),
            quote=format(quote_total, "f"),
            closed_orders=list(closed),
            fills=list(fills),
        )
        residual = self.service.recover(
            session_id, capability, permit(self.clock), after_third_cancel, rules()
        )
        self.assertEqual("CLEANUP_FLATTEN_CLAIMED", residual["status"])
        self.assertTrue(str(residual["action"]["clientOrderId"]).endswith("-f4"))
        self.assertEqual("0.00012", residual["action"]["quantity"])
        self.assertEqual(8, len(self.ledger.actions(session_id)))


if __name__ == "__main__":
    unittest.main()
