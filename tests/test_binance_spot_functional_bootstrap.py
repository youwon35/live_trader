from __future__ import annotations

from contextlib import closing
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from live_trader.binance_spot_continuous_functional import ExactBinding
from live_trader.binance_spot_functional_bootstrap import (
    BinanceSpotFirstLiveBootstrapError,
    DurableBinanceSpotFirstLiveBootstrapStore,
    compute_binance_spot_functional_code_hash,
    default_binance_spot_functional_code_paths,
)
from live_trader.binance_spot_stream_journal import (
    DurableBinanceSpotUserStreamJournal,
)
from live_trader.binance_spot_functional_transport import (
    assemble_binance_spot_rules,
)
from tests.test_binance_spot_continuous_functional import (
    Clock,
    binding,
    natural_evaluation,
)


def canonical_hash(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


class BinanceSpotFirstLiveBootstrapTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "bootstrap.sqlite3"
        self.clock = Clock()
        self.code_hash = "d" * 64
        self.gates = {
            "allOtherProductionComponentsAvailable": True,
            "ordinaryBinanceRoutesClosed": True,
            "emergencyKillInactive": True,
            "applicationInstanceLeaseHeld": True,
            "operatorApprovalBound": True,
            "accountExclusivityVerifierReady": True,
            "accountExclusivityDurableProviderReady": True,
            "accountExclusivitySigningPrimitiveAbsent": True,
            "accountExclusivityAuthorityPinned": True,
            "accountIdentityPinned": True,
            "globalFirstLiveAuthorityReaderWired": True,
            "realE2EAvailable": False,
            "firstLiveBootstrapFeatureEnabled": True,
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def store(
        self, path: Path | None = None
    ) -> DurableBinanceSpotFirstLiveBootstrapStore:
        return DurableBinanceSpotFirstLiveBootstrapStore(
            path or self.path,
            gate_reader=lambda: dict(self.gates),
            server_record_signer=lambda body: {
                **dict(body),
                "serverSignature": canonical_hash(dict(body)),
            },
            code_hash_reader=lambda: self.code_hash,
            clock=self.clock,
        )

    def claimed_bootstrap(
        self, *, label: str
    ) -> tuple[
        DurableBinanceSpotFirstLiveBootstrapStore,
        ExactBinding,
        dict[str, object],
        str,
        str,
    ]:
        store = self.store()
        exact_binding = ExactBinding.parse(binding())
        approval_id = f"approval-bind-{label}-0001"
        permit_id = f"permit-bind-{label}-0000001"
        issued, raw = store.issue(
            binding=exact_binding,
            approval_id=approval_id,
            permit_id=permit_id,
            permit_hash="a" * 64,
        )
        claim_token = store.claim(
            bootstrap_id=str(issued["bootstrap_id"]),
            raw_capability=raw,
            approval_id=approval_id,
            permit_id=permit_id,
            permit_hash="a" * 64,
        )
        return store, exact_binding, issued, claim_token, approval_id

    def malformed_schema_copy(
        self, *, name: str, table_rewrite=lambda value: value
    ) -> Path:
        """Create a separate DB from the exact DDL, then mutate that DDL."""

        source = Path(self.temporary.name) / f"schema-source-{name}.sqlite3"
        self.store(source)
        with closing(sqlite3.connect(str(source))) as connection:
            table_sql = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' "
                    "AND name='binance_spot_first_live_bootstraps'"
                ).fetchone()[0]
            )
            index_sql = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='index' "
                    "AND name='ux_binance_spot_first_live_ever_route'"
                ).fetchone()[0]
            )
        target = Path(self.temporary.name) / f"schema-{name}.sqlite3"
        with closing(sqlite3.connect(str(target))) as connection:
            connection.execute(table_rewrite(table_sql))
            connection.execute(index_sql)
            connection.commit()
        return target

    def eligible_evidence(
        self, *, session_id: str, permit_id: str, permit_hash: str
    ) -> dict[str, object]:
        activated = float(self.clock())
        proof: dict[str, object] = {
            "schemaVersion": "test-binance-exclusivity-proof/v1",
            "sessionId": session_id,
            "permitId": permit_id,
            "permitHash": permit_hash,
            "phase": "TERMINAL",
            "accountWideCausalAudit": {"causalClosureProven": True},
        }
        proof_hash = canonical_hash(proof)
        return {
            "sessionId": session_id,
            "permitId": permit_id,
            "permitHash": permit_hash,
            "outcome": "PASS_FULL_ROUND_TRIP",
            "functionalCapabilityReset": True,
            "newEntriesBlocked": True,
            "promotionEligible": False,
            "useAsPromotionEvidence": False,
            "preexistingBaselinePreserved": True,
            "orderableResidualZero": True,
            "baselineRestoredWithinExchangePrecision": True,
            "exactTwoHourRuntimeComplete": True,
            "actualRuntimeSeconds": "7200",
            "exchangeRuntimeSeconds": "7200",
            "monotonicRuntimeSeconds": "7200",
            "runtimeClockConsistencyProven": True,
            "activatedEpoch": str(int(activated)),
            "activeEndsEpoch": str(int(activated + 7200)),
            "terminalObservedEpoch": str(int(activated + 7200)),
            "naturalBuyFilled": True,
            "naturalSellFilled": True,
            "fullRoundTripWiringPassed": True,
            "orderCapsAndNoReentryProven": True,
            "externalActivityAbsent": True,
            "openOrdersZero": True,
            "privateStreamGapRecoveredCleanupOnly": False,
            "functionalWiringPassed": True,
            "feesQuoteExact": True,
            "ownerLoss": "0.25",
            "exclusiveAccountOperatorAttested": False,
            "exclusiveAccountIndependentlyProven": True,
            "noManualTradingAttested": False,
            "noManualTradingIndependentlyProven": True,
            "noExternalBotsAttested": False,
            "noExternalBotsIndependentlyProven": True,
            "noOtherApiKeysAttested": False,
            "noOtherApiKeysIndependentlyProven": True,
            "accountWideCausalClosureProven": True,
            "otherApiKeysAbsenceAuthoritativelyProven": True,
            "accountExclusivityProof": proof,
            "accountExclusivityProofHash": proof_hash,
            "accountExclusivityProofDurable": True,
            "accountExclusivityPhaseChainComplete": True,
            "accountExclusivityPhaseChainHash": "8" * 64,
            "accountExclusivityPhaseProofCount": 5,
            "accountExclusivityPhaseProofRequiredCount": 5,
            "accountExclusivityRestartVerifiable": True,
        }

    @staticmethod
    def seal_durable_execution(
        store: DurableBinanceSpotFirstLiveBootstrapStore,
        evidence: dict[str, object],
        *,
        extra_buy: bool = False,
        over_cap: bool = False,
        omit_actions: bool = False,
        raw_cutoff_short: bool = False,
        hidden_raw_open: bool = False,
        extra_raw_fill: bool = False,
        truncated_raw_page: bool = False,
        extra_stream_event: bool = False,
        raw_fee_tamper: bool = False,
        raw_status_tamper: bool = False,
        raw_hash_mismatch: bool = False,
        omit_terminal_truth: bool = False,
        omit_stream_archive: bool = False,
        external_quote_delta: bool = False,
        missing_trade_side: bool = False,
        forced_signal_label: bool = False,
        bootstrap_permit_mismatch: bool = False,
        approval_mismatch: bool = False,
        bootstrap_signature_tamper: bool = False,
        keep_trusted_clock_at_activation: bool = False,
        evaluation_lineage_tamper: str = "",
        stale_pre_activation_evaluation: bool = False,
        late_natural_action: bool = False,
        late_raw_fill: bool = False,
        evaluation_observed_mismatch: bool = False,
        post_expiry_server_time: bool = False,
    ) -> None:
        proof = dict(evidence.get("accountExclusivityProof") or {})
        proof["accountWideCausalAudit"] = {
            "causalClosureProven": (
                evidence.get("accountWideCausalClosureProven") is True
            )
        }
        evidence["accountExclusivityProof"] = proof
        evidence["accountExclusivityProofHash"] = canonical_hash(proof)
        session_id = str(evidence["sessionId"])
        baseline_epoch = float(evidence["activatedEpoch"])
        cutoff_epoch = float(evidence["activeEndsEpoch"]) - (
            1.0 if raw_cutoff_short else 0.0
        )
        observed_epoch = float(evidence["terminalObservedEpoch"])
        buy_close_epoch = baseline_epoch + (
            -300 if stale_pre_activation_evaluation else 300
        )
        buy_evaluation = natural_evaluation(
            close_epoch=buy_close_epoch,
            signal="BUY",
            observed_epoch=buy_close_epoch,
        )
        sell_evaluation = natural_evaluation(
            close_epoch=baseline_epoch + 600,
            signal="SELL",
            observed_epoch=baseline_epoch + 600,
        )
        evaluation_lineage = {
            "sessionId": session_id,
            "permitId": str(evidence["permitId"]),
            "permitHash": str(evidence["permitHash"]),
            "accountFingerprint": str(binding()["accountFingerprint"]),
            "bindingHash": canonical_hash(binding()),
        }
        buy_evaluation.update(evaluation_lineage)
        sell_evaluation.update(evaluation_lineage)
        if evaluation_observed_mismatch:
            buy_evaluation["observedAt"] = datetime.fromtimestamp(
                buy_close_epoch + 1, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        if post_expiry_server_time:
            changed_window = dict(buy_evaluation["officialWindow"])
            changed_window["serverTime"] = int(
                float(evidence["activeEndsEpoch"]) * 1000
            )
            changed_window["observedAt"] = datetime.fromtimestamp(
                float(evidence["activeEndsEpoch"]), tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            buy_evaluation["officialWindow"] = changed_window
        if evaluation_lineage_tamper:
            replacement = {
                "strategyArtifactId": "crypto-binance-other-artifact-v1",
                "strategyArtifactFileSha256": "9" * 64,
                "strategyInstanceId": "crypto-binance-other-instance-v1",
                "strategyInstanceFileSha256": "8" * 64,
                "publicationProofHash": "7" * 64,
                "publicationProofFileSha256": "6" * 64,
                "accountFingerprint": "5" * 64,
                "bindingHash": "4" * 64,
                "sessionId": "bnsft-evaluation-lineage-swap",
                "permitId": "permit-evaluation-lineage-swap",
                "permitHash": "3" * 64,
            }.get(evaluation_lineage_tamper)
            if replacement is None:
                raise AssertionError("unknown evaluation lineage mutation")
            buy_evaluation[evaluation_lineage_tamper] = replacement
        if forced_signal_label:
            forced_window = dict(buy_evaluation["officialWindow"])
            forced_bars = [dict(row) for row in forced_window["bars"]]
            for row in forced_bars:
                row["close"] = "10"
                row["open"] = "10"
                row["high"] = "11"
                row["low"] = "9"
            forced_window["bars"] = forced_bars
            buy_evaluation["officialWindow"] = forced_window
        prefix = "ftb-" + hashlib.sha256(session_id.encode()).hexdigest()[:12]
        buy_id = prefix + "-b"
        sell_id = prefix + "-s"
        buy_action = {
            "kind": "BUY",
            "product": "SPOT",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "orderType": "MARKET",
            "quoteOrderQty": "11" if over_cap else "10",
            "quantity": "0",
            "clientOrderId": buy_id,
            "evaluationId": buy_evaluation["evaluationId"],
            "evaluationHash": canonical_hash(buy_evaluation),
            "officialWindowHash": buy_evaluation["officialWindowHash"],
            "barCloseEpoch": buy_close_epoch,
            "functionalOnly": True,
            "cleanupOnly": False,
        }
        sell_action = {
            "kind": "SELL",
            "product": "SPOT",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "orderType": "MARKET",
            "quoteOrderQty": "0",
            "quantity": "0.00015",
            "clientOrderId": sell_id,
            "evaluationId": sell_evaluation["evaluationId"],
            "evaluationHash": canonical_hash(sell_evaluation),
            "officialWindowHash": sell_evaluation["officialWindowHash"],
            "barCloseEpoch": baseline_epoch + 600,
            "functionalOnly": True,
            "cleanupOnly": False,
        }
        buy_action_epoch = (
            float(evidence["activeEndsEpoch"])
            if late_natural_action
            else baseline_epoch + 301
        )
        actions = [
            ("claim-buy", "BUY", buy_id, buy_action, buy_action_epoch),
            ("claim-sell", "SELL", sell_id, sell_action, baseline_epoch + 601),
        ]
        if extra_buy:
            actions.append(
                (
                    "claim-buy-extra",
                    "BUY",
                    prefix + "-extra-b",
                    {**buy_action, "clientOrderId": prefix + "-extra-b"},
                    baseline_epoch + 602,
                )
            )
        proof_actions = []
        for claim_id, kind, client_id, action, _created in actions:
            broker_order_id = {
                "claim-buy": "1001",
                "claim-sell": "1002",
            }.get(claim_id, "1003")
            sealed = json.dumps(
                action,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            proof_actions.append(
                {
                    "claimId": claim_id,
                    "actionKind": kind,
                    "clientOrderId": client_id,
                    "state": "RECONCILED",
                    "sealedActionHash": hashlib.sha256(
                        sealed.encode("utf-8")
                    ).hexdigest(),
                    "responseHash": "c" * 64,
                    "brokerOrderId": broker_order_id,
                }
            )
        now_ms = int(cutoff_epoch * 1000)
        raw_orders = [
            {
                "orderId": "1001",
                "clientOrderId": buy_id,
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "MARKET",
                "status": "FILLED",
                "origQty": "0",
                "executedQty": "0.00015",
                "origQuoteOrderQty": "10",
                "cummulativeQuoteQty": "10",
                "time": now_ms - 3000,
                "updateTime": now_ms - 2000,
            },
            {
                "orderId": "1002",
                "clientOrderId": sell_id,
                "symbol": "BTCUSDT",
                "side": "SELL",
                "type": "MARKET",
                "status": "FILLED",
                "origQty": "0.00015",
                "executedQty": "0.00015",
                "origQuoteOrderQty": "0",
                "cummulativeQuoteQty": "9.75",
                "time": now_ms - 1500,
                "updateTime": now_ms - 1000,
            },
        ]
        sell_fill_ms = now_ms if late_raw_fill else now_ms - 1000
        raw_trades = [
            {
                "id": "1",
                "orderId": "1001",
                "symbol": "BTCUSDT",
                "qty": "0.00015",
                "quoteQty": "10",
                "commission": "0",
                "commissionAsset": "USDT",
                "isBuyer": True,
                "time": now_ms - 2000,
            },
            {
                "id": "2",
                "orderId": "1002",
                "symbol": "BTCUSDT",
                "qty": "0.00015",
                "quoteQty": "9.75",
                "commission": "0",
                "commissionAsset": "USDT",
                "isBuyer": False,
                "time": sell_fill_ms,
            },
        ]
        if raw_fee_tamper:
            raw_trades[0]["commission"] = "0.000001"
            raw_trades[0]["commissionAsset"] = "BTC"
        if raw_status_tamper:
            raw_orders[0]["status"] = "CANCELED"
        if missing_trade_side:
            raw_trades[1].pop("isBuyer", None)
        if extra_raw_fill:
            extra_client_id = prefix + "-f"
            raw_orders.append(
                {
                    "orderId": "1003",
                    "clientOrderId": extra_client_id,
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "MARKET",
                    "status": "FILLED",
                    "origQty": "0.00001",
                    "executedQty": "0.00001",
                    "origQuoteOrderQty": "0",
                    "cummulativeQuoteQty": "0.65",
                    "time": now_ms - 750,
                    "updateTime": now_ms - 700,
                }
            )
            raw_trades.append(
                {
                    "id": "3",
                    "orderId": "1003",
                    "symbol": "BTCUSDT",
                    "qty": "0.00001",
                    "quoteQty": "0.65",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "isBuyer": False,
                    "time": now_ms - 700,
                }
            )
        account = {
            "canTrade": True,
            "accountType": "SPOT",
            "permissions": ["SPOT"],
            "balances": [
                {"asset": "BTC", "free": "0.001", "locked": "0"},
                {
                    "asset": "USDT",
                    "free": "100.75" if external_quote_delta else "99.75",
                    "locked": "0",
                },
            ]
        }
        exchange_info: dict[str, object] = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "isSpotTradingAllowed": True,
                    "quoteOrderQtyMarketAllowed": True,
                    "permissions": [],
                    "permissionSets": [["SPOT"]],
                    "filters": [
                        {
                            "filterType": "MARKET_LOT_SIZE",
                            "minQty": "0.00001",
                            "maxQty": "100",
                            "stepSize": "0.00001",
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "5",
                            "applyToMarket": True,
                            "avgPriceMins": 0,
                        },
                    ],
                }
            ]
        }
        ticker = {"symbol": "BTCUSDT", "price": "60000"}
        normalized_rules = assemble_binance_spot_rules(
            exchange_info, account=account
        )
        normalized_rules.update(
            {
                "marketReferencePrice": "60000",
                "marketReferenceSource": "BINANCE_TICKER_PRICE",
                "rulesObservedAt": datetime.fromtimestamp(
                    observed_epoch, tz=timezone.utc
                ).isoformat().replace("+00:00", "Z"),
            }
        )
        def pages(endpoint: str, cursor: str, field: str, rows: list[dict[str, object]]) -> list[dict[str, object]]:
            return [{
                "endpoint": endpoint,
                "query": {
                    "symbol": "BTCUSDT",
                    "limit": 1000,
                    "startTime": now_ms - 7200 * 1000,
                    "endTime": now_ms,
                },
                "cursorParameter": cursor,
                "rowIdField": field,
                "pageIndex": 0,
                "responseRows": rows,
                "responseHash": canonical_hash({"rows": rows}),
                "responseCount": len(rows),
                "completion": "CONTINUE" if truncated_raw_page else "SHORT_PAGE",
                **(
                    {"nextCursor": int(str(rows[-1][field])) + 1}
                    if truncated_raw_page and rows
                    else {}
                ),
            }]
        raw_open_orders = (
            [
                {
                    "orderId": "9001",
                    "clientOrderId": "external-open-order",
                    "symbol": "ETHUSDT",
                    "side": "BUY",
                    "type": "LIMIT",
                    "status": "NEW",
                    "origQty": "0.1",
                    "executedQty": "0",
                    "origQuoteOrderQty": "0",
                    "cummulativeQuoteQty": "0",
                    "time": now_ms - 500,
                    "updateTime": now_ms - 500,
                }
            ]
            if hidden_raw_open
            else []
        )
        raw_snapshot = {
            "schemaVersion": "binance-spot-functional-official-rest-set/v1",
            "baselineEpoch": baseline_epoch,
            "historyCutoffEpoch": cutoff_epoch,
            "observedEpoch": observed_epoch,
            "account": account,
            "openOrders": raw_open_orders,
            "allOrders": raw_orders,
            "myTrades": raw_trades,
            "exchangeInfo": exchange_info,
            "tickerPrice": ticker,
            "averagePrice": None,
            "normalizedRules": normalized_rules,
            "requestEnvelopes": {
                "account": {
                    "endpoint": "/api/v3/account",
                    "query": {"omitZeroBalances": False},
                    "responseHash": canonical_hash(account),
                },
                "openOrders": {
                    "endpoint": "/api/v3/openOrders",
                    "query": {},
                    "responseHash": canonical_hash({"rows": raw_open_orders}),
                },
                "allOrdersPages": pages(
                    "/api/v3/allOrders", "orderId", "orderId", raw_orders
                ),
                "myTradesPages": pages(
                    "/api/v3/myTrades", "fromId", "id", raw_trades
                ),
                "exchangeInfo": {
                    "endpoint": "/api/v3/exchangeInfo",
                    "query": {"symbol": "BTCUSDT"},
                    "responseHash": canonical_hash(exchange_info),
                },
                "tickerPrice": {
                    "endpoint": "/api/v3/ticker/price",
                    "query": {"symbol": "BTCUSDT"},
                    "responseHash": canonical_hash(ticker),
                },
                "averagePrice": None,
            },
        }
        terminal_truth = {
            "schemaVersion": "binance-spot-functional-terminal-official-truth/v1",
            "sessionId": session_id,
            "permitId": str(evidence["permitId"]),
            "permitHash": str(evidence["permitHash"]),
            "accountFingerprint": str(binding()["accountFingerprint"]),
            "observedEpoch": observed_epoch,
            "historyBaselineEpoch": baseline_epoch,
            "historyCutoffEpoch": cutoff_epoch,
            "baselineBase": "0.001",
            "balances": account["balances"],
            "finalBaseTotal": "0.001",
            "finalQuoteTotal": "100.75" if external_quote_delta else "99.75",
            "markPrice": "60000",
            "accountOpenOrders": [],
            "closedOrders": [
                {
                    "orderId": "1001",
                    "clientOrderId": buy_id,
                    "symbol": "BTCUSDT",
                    "product": "SPOT",
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                    "origQty": "0",
                    "executedQty": "0.00015",
                    "origQuoteOrderQty": "10",
                    "cummulativeQuoteQty": "10",
                    "time": now_ms - 3000,
                    "updateTime": now_ms - 2000,
                    "isMargin": False,
                    "reduceOnly": False,
                },
                {
                    "orderId": "1002",
                    "clientOrderId": sell_id,
                    "symbol": "BTCUSDT",
                    "product": "SPOT",
                    "side": "SELL",
                    "type": "MARKET",
                    "status": "FILLED",
                    "origQty": "0.00015",
                    "executedQty": "0.00015",
                    "origQuoteOrderQty": "0",
                    "cummulativeQuoteQty": "9.75",
                    "time": now_ms - 1500,
                    "updateTime": now_ms - 1000,
                    "isMargin": False,
                    "reduceOnly": False,
                },
            ],
            "fills": [
                {
                    "tradeId": "1",
                    "orderId": "1001",
                    "clientOrderId": buy_id,
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "quantity": "0.00015",
                    "quoteQuantity": "10",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "feeQuoteValue": "0",
                    "feeQuoteValueExact": True,
                    "time": now_ms - 2000,
                },
                {
                    "tradeId": "2",
                    "orderId": "1002",
                    "clientOrderId": sell_id,
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "quantity": "0.00015",
                    "quoteQuantity": "9.75",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "feeQuoteValue": "0",
                    "feeQuoteValueExact": True,
                    "time": sell_fill_ms,
                },
            ],
            "feeQuoteValuationComplete": True,
            "externalActivityAbsent": True,
            "accountWideCausalClosureProven": bool(
                evidence.get("accountWideCausalClosureProven")
            ),
            "accountExclusivityProof": dict(
                evidence.get("accountExclusivityProof") or {}
            ),
            "accountExclusivityProofHash": str(
                evidence.get("accountExclusivityProofHash") or ""
            ),
            "accountExclusivityPhaseChainComplete": bool(
                evidence.get("accountExclusivityPhaseChainComplete")
            ),
            "accountExclusivityPhaseChainHash": str(
                evidence.get("accountExclusivityPhaseChainHash") or ""
            ),
            "accountExclusivityPhaseProofCount": int(
                evidence.get("accountExclusivityPhaseProofCount") or 0
            ),
            "accountExclusivityPhaseProofRequiredCount": int(
                evidence.get("accountExclusivityPhaseProofRequiredCount") or 0
            ),
            "accountExclusivityRestartVerifiable": bool(
                evidence.get("accountExclusivityRestartVerifiable")
            ),
            "streamSessionId": session_id,
            "streamPermitId": str(evidence["permitId"]),
            "streamPermitHash": str(evidence["permitHash"]),
            "streamJournalSealHash": "",
            "streamJournalEventCount": 3,
            "accountSymbolPermissionProofHash": "d" * 64,
            "officialRestSnapshot": raw_snapshot,
            "officialRestTruthHash": (
                "0" * 64 if raw_hash_mismatch else canonical_hash(raw_snapshot)
            ),
            "rules": normalized_rules,
        }
        stream_payloads = [
            {
                "eventId": f"execution:1001:1:TRADE:{now_ms - 2000}",
                "eventType": "executionReport",
                "eventTime": now_ms - 2000,
                "symbol": "BTCUSDT",
                "clientOrderId": buy_id,
                "originalClientOrderId": "",
                "orderId": "1001",
                "tradeId": "1",
                "side": "BUY",
                "orderType": "MARKET",
                "executionType": "TRADE",
                "orderStatus": "FILLED",
                "lastQty": "0.00015",
                "lastQuoteQty": "10",
                "lastPrice": "66666.6666666667",
                "commission": "0",
                "commissionAsset": "USDT",
                "cumulativeQty": "0.00015",
                "cumulativeQuoteQty": "10",
            },
            {
                "eventId": f"execution:1002:2:TRADE:{sell_fill_ms}",
                "eventType": "executionReport",
                "eventTime": sell_fill_ms,
                "symbol": "BTCUSDT",
                "clientOrderId": sell_id,
                "originalClientOrderId": "",
                "orderId": "1002",
                "tradeId": "2",
                "side": "SELL",
                "orderType": "MARKET",
                "executionType": "TRADE",
                "orderStatus": "FILLED",
                "lastQty": "0.00015",
                "lastQuoteQty": "9.75",
                "lastPrice": "65000",
                "commission": "0",
                "commissionAsset": "USDT",
                "cumulativeQty": "0.00015",
                "cumulativeQuoteQty": "9.75",
            },
            {
                **(
                    {
                        "eventId": f"execution:1003:0:CANCELED:{now_ms - 750}",
                        "eventType": "executionReport",
                        "eventTime": now_ms - 750,
                        "symbol": "BTCUSDT",
                        "clientOrderId": prefix + "-f3",
                        "originalClientOrderId": "",
                        "orderId": "1003",
                        "tradeId": "0",
                        "side": "SELL",
                        "orderType": "MARKET",
                        "executionType": "CANCELED",
                        "orderStatus": "CANCELED",
                        "lastQty": "0",
                        "lastQuoteQty": "0",
                        "lastPrice": "0",
                        "commission": "0",
                        "commissionAsset": "USDT",
                        "cumulativeQty": "0",
                        "cumulativeQuoteQty": "0",
                    }
                    if extra_stream_event
                    else {
                        "eventId": f"account:{now_ms - 500}:{now_ms - 500}",
                        "eventType": "outboundAccountPosition",
                        "eventTime": now_ms - 500,
                        "lastAccountUpdateTime": now_ms - 500,
                        "balances": account["balances"],
                    }
                )
            },
            *(
                [
                    {
                        "eventId": f"account:{now_ms - 500}:{now_ms - 500}",
                        "eventType": "outboundAccountPosition",
                        "eventTime": now_ms - 500,
                        "lastAccountUpdateTime": now_ms - 500,
                        "balances": account["balances"],
                    }
                ]
                if extra_stream_event
                else []
            ),
        ]
        stream_rows = [
            {
                "event_id": str(payload["eventId"]),
                "event_epoch": float(payload["eventTime"]) / 1000.0,
                "payload_json": json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ),
            }
            for payload in stream_payloads
        ]
        stream_meta = {
            "route_key": "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m",
            "account_fingerprint": str(binding()["accountFingerprint"]),
            "writer_id": "writer-terminal-proof",
            "writer_token_hash": "",
            "owner_prefix": prefix + "-",
            "session_id": session_id,
            "permit_id": str(evidence["permitId"]),
            "permit_hash": str(evidence["permitHash"]),
            "subscribed_epoch": baseline_epoch - 10,
            "observed_epoch": observed_epoch,
            "heartbeat_epoch": observed_epoch,
            "connected": 1,
            "authenticated": 1,
            "gap_detected": 0,
            "external_activity_absent": 1,
            "retired": 0,
            "retired_epoch": 0,
            "retirement_evidence_hash": "",
            "terminal_marker_id": "terminal-marker-1",
            "terminal_marker_server_epoch": cutoff_epoch,
            "terminal_marker_epoch": cutoff_epoch,
            "detail": "terminal barrier sealed",
        }
        stream_seal_material = {
            "routeKey": stream_meta["route_key"],
            "accountFingerprint": stream_meta["account_fingerprint"],
            "writerId": stream_meta["writer_id"],
            "ownerPrefix": stream_meta["owner_prefix"],
            "sessionId": stream_meta["session_id"],
            "permitId": stream_meta["permit_id"],
            "permitHash": stream_meta["permit_hash"],
            "subscribedEpoch": stream_meta["subscribed_epoch"],
            "connected": True,
            "authenticated": True,
            "gapDetected": False,
            "externalActivityAbsent": True,
            "retired": False,
            "terminalMarkerId": stream_meta["terminal_marker_id"],
            "terminalMarkerServerEpoch": stream_meta[
                "terminal_marker_server_epoch"
            ],
            "terminalMarkerEpoch": stream_meta["terminal_marker_epoch"],
            "events": [
                {
                    "eventId": row["event_id"],
                    "eventEpoch": row["event_epoch"],
                    "payloadJson": row["payload_json"],
                }
                for row in stream_rows
            ],
        }
        terminal_truth["streamJournalSealHash"] = canonical_hash(
            stream_seal_material
        )
        evidence["terminalOfficialTruthHash"] = canonical_hash(terminal_truth)
        canonical = json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        evidence_hash = hashlib.sha256(canonical.encode()).hexdigest()
        attestation = {
            "routeKey": "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m",
            "accountFingerprint": str(binding()["accountFingerprint"]),
            "sessionId": session_id,
            "permitId": str(evidence["permitId"]),
            "permitHash": str(evidence["permitHash"]),
            "finalEvidenceHash": evidence_hash,
            "terminalReason": "FINALIZED",
        }
        archive_material = {
            "attestation": attestation,
            "meta": stream_meta,
            "events": stream_rows,
        }
        archive_hash = canonical_hash(archive_material)
        archive_id = "bssja-" + archive_hash[:32]
        with closing(sqlite3.connect(store.path)) as connection:
            connection.row_factory = sqlite3.Row
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_spot_functional_sessions (
                    session_id TEXT PRIMARY KEY, state TEXT NOT NULL,
                    capability_hash TEXT NOT NULL,
                    final_new_entries_blocked INTEGER NOT NULL,
                    baseline_base TEXT NOT NULL,
                    baseline_quote TEXT NOT NULL,
                    final_evidence_json TEXT NOT NULL,
                    final_evidence_hash TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    binding_hash TEXT NOT NULL,
                    started_epoch REAL NOT NULL,
                    expires_epoch REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_spot_functional_actions (
                    claim_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    action_kind TEXT NOT NULL, client_order_id TEXT NOT NULL,
                    state TEXT NOT NULL, sealed_action_json TEXT NOT NULL,
                    response_hash TEXT NOT NULL, broker_order_id TEXT NOT NULL,
                    post_marker_epoch REAL NOT NULL,
                    created_epoch REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_spot_functional_terminal_truth (
                    session_id TEXT PRIMARY KEY, truth_json TEXT NOT NULL,
                    truth_hash TEXT NOT NULL, observed_epoch REAL NOT NULL,
                    stream_journal_seal_hash TEXT NOT NULL,
                    stream_journal_event_count INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_spot_functional_strategy_evaluations (
                    evaluation_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    bar_close_epoch REAL NOT NULL, signal TEXT NOT NULL,
                    evaluation_json TEXT NOT NULL, evaluation_hash TEXT NOT NULL,
                    window_json TEXT NOT NULL, window_hash TEXT NOT NULL,
                    created_epoch REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_spot_functional_approvals (
                    permit_id TEXT PRIMARY KEY, permit_hash TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    strategy_artifact_hash TEXT NOT NULL,
                    strategy_instance_hash TEXT NOT NULL,
                    route_key TEXT NOT NULL, approval_id TEXT NOT NULL,
                    state TEXT NOT NULL, session_id TEXT NOT NULL,
                    first_live_bootstrap_required INTEGER NOT NULL,
                    first_live_bootstrap_id TEXT NOT NULL,
                    first_live_bootstrap_hash TEXT NOT NULL,
                    first_live_session_nonce_hash TEXT NOT NULL,
                    first_live_code_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_spot_stream_journal_archives (
                    archive_id TEXT PRIMARY KEY, route_key TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL, session_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL, permit_hash TEXT NOT NULL,
                    final_evidence_hash TEXT NOT NULL, retired_epoch REAL NOT NULL,
                    meta_json TEXT NOT NULL, archive_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_spot_stream_journal_archive_events (
                    archive_id TEXT NOT NULL, event_id TEXT NOT NULL,
                    event_epoch REAL NOT NULL, payload_json TEXT NOT NULL,
                    PRIMARY KEY(archive_id,event_id)
                );
                """
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO binance_spot_functional_sessions
                VALUES (?, 'FINALIZED', '', 1, '0.001', '100', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    canonical,
                    evidence_hash,
                    str(evidence["permitId"]),
                    str(evidence["permitHash"]),
                    json.dumps(
                        binding(),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ),
                    canonical_hash(binding()),
                    float(evidence["activatedEpoch"]),
                    float(evidence["activeEndsEpoch"]),
                ),
            )
            bootstrap = connection.execute(
                "SELECT * FROM binance_spot_first_live_bootstraps WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if bootstrap is None:
                raise AssertionError("active bootstrap fixture is missing")
            connection.execute(
                """INSERT OR REPLACE INTO binance_spot_functional_approvals
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    str(evidence["permitId"]),
                    str(evidence["permitHash"]),
                    str(binding()["accountFingerprint"]),
                    str(binding()["strategyArtifactHash"]),
                    str(binding()["strategyInstanceHash"]),
                    "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m",
                    str(bootstrap["approval_id"]),
                    "CONSUMED",
                    session_id,
                    1,
                    str(bootstrap["bootstrap_id"]),
                    str(bootstrap["bootstrap_hash"]),
                    str(bootstrap["capability_hash"]),
                    str(bootstrap["code_hash"]),
                ),
            )
            if not omit_terminal_truth:
                connection.execute(
                    """INSERT OR REPLACE INTO binance_spot_functional_terminal_truth
                    VALUES (?,?,?,?,?,?)""",
                    (
                        session_id,
                        json.dumps(
                            terminal_truth,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                            allow_nan=False,
                        ),
                        canonical_hash(terminal_truth),
                        observed_epoch,
                        str(terminal_truth["streamJournalSealHash"]),
                        len(stream_rows),
                    ),
                )
            if not omit_stream_archive:
                connection.execute(
                    """INSERT OR REPLACE INTO binance_spot_stream_journal_archives
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        archive_id,
                        attestation["routeKey"],
                        attestation["accountFingerprint"],
                        session_id,
                        attestation["permitId"],
                        attestation["permitHash"],
                        evidence_hash,
                        observed_epoch,
                        json.dumps(
                            stream_meta,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                            allow_nan=False,
                        ),
                        archive_hash,
                    ),
                )
            connection.execute(
                "DELETE FROM binance_spot_stream_journal_archive_events "
                "WHERE archive_id=?",
                (archive_id,),
            )
            for stream_row in ([] if omit_stream_archive else stream_rows):
                connection.execute(
                    """INSERT INTO binance_spot_stream_journal_archive_events
                    VALUES (?,?,?,?)""",
                    (
                        archive_id,
                        stream_row["event_id"],
                        stream_row["event_epoch"],
                        stream_row["payload_json"],
                    ),
                )
            connection.execute(
                "DELETE FROM binance_spot_functional_actions WHERE session_id=?",
                (session_id,),
            )
            connection.execute(
                "DELETE FROM binance_spot_functional_strategy_evaluations "
                "WHERE session_id=?",
                (session_id,),
            )
            for evaluation, signal, created_epoch in (
                (buy_evaluation, "BUY", baseline_epoch + 300),
                (sell_evaluation, "SELL", baseline_epoch + 600),
            ):
                window = dict(evaluation["officialWindow"])
                connection.execute(
                    """INSERT INTO binance_spot_functional_strategy_evaluations
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        str(evaluation["evaluationId"]),
                        session_id,
                        float(
                            datetime.fromisoformat(
                                str(evaluation["barCloseAt"]).replace("Z", "+00:00")
                            ).timestamp()
                        ),
                        signal,
                        json.dumps(
                            evaluation,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                            allow_nan=False,
                        ),
                        canonical_hash(evaluation),
                        json.dumps(
                            window,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                            allow_nan=False,
                        ),
                        canonical_hash(window),
                        created_epoch,
                    ),
                )
            if not omit_actions:
                for claim_id, kind, client_id, action, created in actions:
                    sealed = json.dumps(
                        action,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                    connection.execute(
                        """
                        INSERT INTO binance_spot_functional_actions
                        VALUES (?, ?, ?, ?, 'RECONCILED', ?, ?, ?, ?, ?)
                        """,
                        (
                            claim_id,
                            session_id,
                            kind,
                            client_id,
                            sealed,
                            "c" * 64,
                            {
                                "claim-buy": "1001",
                                "claim-sell": "1002",
                            }.get(claim_id, "1003"),
                            created + 0.5,
                            created,
                        ),
                    )
            if bootstrap_permit_mismatch:
                connection.execute(
                    """UPDATE binance_spot_first_live_bootstraps
                    SET active_permit_id='permit-swapped-bootstrap-lineage'
                    WHERE session_id=?""",
                    (session_id,),
                )
            if approval_mismatch:
                connection.execute(
                    """UPDATE binance_spot_functional_approvals
                    SET approval_id='approval-swapped-bootstrap-lineage'
                    WHERE session_id=?""",
                    (session_id,),
                )
            if bootstrap_signature_tamper:
                record = json.loads(str(bootstrap["record_json"]))
                record["maxOwnerLoss"] = "2"
                record_json = json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                changed_hash = canonical_hash(record)
                connection.execute(
                    """UPDATE binance_spot_first_live_bootstraps
                    SET record_json=?,bootstrap_hash=? WHERE session_id=?""",
                    (record_json, changed_hash, session_id),
                )
                connection.execute(
                    """UPDATE binance_spot_functional_approvals
                    SET first_live_bootstrap_hash=? WHERE session_id=?""",
                    (changed_hash, session_id),
                )
            connection.commit()
        if hasattr(store.clock, "value") and not keep_trusted_clock_at_activation:
            store.clock.value = max(float(store.clock()), observed_epoch)

    def test_one_shot_claim_bind_and_exact_causal_pass_is_eligible(self) -> None:
        store = self.store()
        exact_binding = ExactBinding.parse(binding())
        issued, raw = store.issue(
            binding=exact_binding,
            approval_id="approval-first-live-0001",
            permit_id="permit-first-live-0000001",
            permit_hash="a" * 64,
        )
        self.assertEqual("ISSUED", issued["state"])
        self.assertNotIn("capability_hash", issued)
        with self.assertRaises(BinanceSpotFirstLiveBootstrapError):
            store.claim(
                bootstrap_id=str(issued["bootstrap_id"]),
                raw_capability="wrong-raw-capability",
                approval_id="approval-first-live-0001",
                permit_id="permit-first-live-0000001",
                permit_hash="a" * 64,
            )
        claim_token = store.claim(
            bootstrap_id=str(issued["bootstrap_id"]),
            raw_capability=raw,
            approval_id="approval-first-live-0001",
            permit_id="permit-first-live-0000001",
            permit_hash="a" * 64,
        )
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError, "already consumed"
        ):
            store.claim(
                bootstrap_id=str(issued["bootstrap_id"]),
                raw_capability=raw,
                approval_id="approval-first-live-0001",
                permit_id="permit-first-live-0000001",
                permit_hash="a" * 64,
            )
        session_id = "bnsft-first-live-session-0001"
        active_permit_id = "permit-active-first-live-0001"
        active_permit_hash = "b" * 64
        active = store.bind_session(
            bootstrap_id=str(issued["bootstrap_id"]),
            claim_token=claim_token,
            approval_id="approval-first-live-0001",
            active_permit_id=active_permit_id,
            active_permit_hash=active_permit_hash,
            session_id=session_id,
            binding=exact_binding,
            activated_epoch=self.clock(),
            active_ends_epoch=self.clock() + 7200,
        )
        self.assertEqual("ACTIVE", active["state"])
        evidence = self.eligible_evidence(
            session_id=session_id,
            permit_id=active_permit_id,
            permit_hash=active_permit_hash,
        )
        self.seal_durable_execution(store, evidence)
        transaction_observed: list[bool] = []
        original_verify = store._verify_durable_terminal_execution

        def verify_in_consume_transaction(**kwargs):
            connection = kwargs.get("connection")
            transaction_observed.append(
                isinstance(connection, sqlite3.Connection)
                and connection.in_transaction
            )
            return original_verify(**kwargs)

        with patch.object(
            store,
            "_verify_durable_terminal_execution",
            side_effect=verify_in_consume_transaction,
        ):
            consumed = store.consume_terminal(
                bootstrap_id=str(issued["bootstrap_id"]),
                session_id=session_id,
                permit_id=active_permit_id,
                permit_hash=active_permit_hash,
                evidence=evidence,
                evidence_hash=canonical_hash(evidence),
            )
        self.assertEqual([True], transaction_observed)
        self.assertEqual("CONSUMED", consumed["state"])
        self.assertEqual(1, consumed["e2e_evidence_eligible"])
        self.assertEqual(1, consumed["functional_wiring_passed"])

    def test_claim_expired_before_bind_is_burned_without_session_binding(
        self,
    ) -> None:
        store, exact_binding, issued, claim_token, approval_id = (
            self.claimed_bootstrap(label="expired")
        )
        self.clock.value += 301
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError,
            "expired before session bind",
        ):
            store.bind_session(
                bootstrap_id=str(issued["bootstrap_id"]),
                claim_token=claim_token,
                approval_id=approval_id,
                active_permit_id="permit-active-expired-bind-0001",
                active_permit_hash="b" * 64,
                session_id="bnsft-expired-bind-session-0001",
                binding=exact_binding,
                activated_epoch=self.clock(),
                active_ends_epoch=self.clock() + 7200,
            )
        status = store.status(str(issued["bootstrap_id"]))
        self.assertEqual("FAILED", status["state"])
        self.assertEqual("", status["session_id"])
        self.assertEqual("", status["active_permit_id"])
        self.assertEqual("", status["active_permit_hash"])
        self.assertEqual("", status["session_nonce_hash"])
        self.assertIsNone(store.pointer_for_approval(approval_id))

    def test_claim_at_exact_expiry_boundary_cannot_bind(self) -> None:
        store, exact_binding, issued, claim_token, approval_id = (
            self.claimed_bootstrap(label="boundary")
        )
        self.clock.value += 300
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError,
            "expired before session bind",
        ):
            store.bind_session(
                bootstrap_id=str(issued["bootstrap_id"]),
                claim_token=claim_token,
                approval_id=approval_id,
                active_permit_id="permit-active-boundary-bind-0001",
                active_permit_hash="b" * 64,
                session_id="bnsft-boundary-bind-session-0001",
                binding=exact_binding,
                activated_epoch=self.clock(),
                active_ends_epoch=self.clock() + 7200,
            )
        status = store.status(str(issued["bootstrap_id"]))
        self.assertEqual("FAILED", status["state"])
        self.assertEqual("", status["session_id"])
        self.assertIsNone(
            store.active_terminal_pointer_for_session(
                "bnsft-boundary-bind-session-0001"
            )
        )

    def test_bind_rejects_activation_timestamp_before_durable_claim(self) -> None:
        store, exact_binding, issued, claim_token, approval_id = (
            self.claimed_bootstrap(label="time-lineage")
        )
        activated_epoch = self.clock() - 1
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError,
            "time lineage invalid before session bind",
        ):
            store.bind_session(
                bootstrap_id=str(issued["bootstrap_id"]),
                claim_token=claim_token,
                approval_id=approval_id,
                active_permit_id="permit-active-time-lineage-0001",
                active_permit_hash="b" * 64,
                session_id="bnsft-time-lineage-session-0001",
                binding=exact_binding,
                activated_epoch=activated_epoch,
                active_ends_epoch=activated_epoch + 7200,
            )
        status = store.status(str(issued["bootstrap_id"]))
        self.assertEqual("FAILED", status["state"])
        self.assertEqual("", status["session_id"])
        self.assertEqual("", status["session_nonce_hash"])

    def test_concurrent_expired_bind_attempts_cannot_create_active_session(
        self,
    ) -> None:
        first, exact_binding, issued, claim_token, approval_id = (
            self.claimed_bootstrap(label="race")
        )
        second = self.store()
        self.clock.value += 301
        barrier = threading.Barrier(3)
        results: list[dict[str, object]] = []
        errors: list[Exception] = []

        def bind(
            store: DurableBinanceSpotFirstLiveBootstrapStore,
            session_id: str,
        ) -> None:
            barrier.wait()
            try:
                results.append(
                    store.bind_session(
                        bootstrap_id=str(issued["bootstrap_id"]),
                        claim_token=claim_token,
                        approval_id=approval_id,
                        active_permit_id="permit-active-raced-bind-0001",
                        active_permit_hash="b" * 64,
                        session_id=session_id,
                        binding=exact_binding,
                        activated_epoch=self.clock(),
                        active_ends_epoch=self.clock() + 7200,
                    )
                )
            except Exception as exc:
                errors.append(exc)

        session_ids = (
            "bnsft-expired-bind-race-a-0001",
            "bnsft-expired-bind-race-b-0001",
        )
        threads = (
            threading.Thread(target=bind, args=(first, session_ids[0])),
            threading.Thread(target=bind, args=(second, session_ids[1])),
        )
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual([], results)
        self.assertEqual(2, len(errors))
        self.assertTrue(
            all(
                isinstance(error, BinanceSpotFirstLiveBootstrapError)
                for error in errors
            )
        )
        status = first.status(str(issued["bootstrap_id"]))
        self.assertEqual("FAILED", status["state"])
        self.assertEqual("", status["session_id"])
        self.assertEqual("", status["session_nonce_hash"])
        for session_id in session_ids:
            self.assertIsNone(
                first.active_terminal_pointer_for_session(session_id)
            )

    def test_only_real_e2e_gate_can_be_bypassed(self) -> None:
        store = self.store()
        self.gates["ordinaryBinanceRoutesClosed"] = False
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError, "prerequisite"
        ):
            store.issue(
                binding=ExactBinding.parse(binding()),
                approval_id="approval-first-live-0002",
                permit_id="permit-first-live-0000002",
                permit_hash="a" * 64,
            )

    def test_exact_schema_rejects_wrong_named_route_indexes(self) -> None:
        definitions = {
            "nonunique": (
                "CREATE INDEX ux_binance_spot_first_live_ever_route "
                "ON binance_spot_first_live_bootstraps(route_key)"
            ),
            "wrong-column": (
                "CREATE UNIQUE INDEX ux_binance_spot_first_live_ever_route "
                "ON binance_spot_first_live_bootstraps(approval_id)"
            ),
            "partial": (
                "CREATE UNIQUE INDEX ux_binance_spot_first_live_ever_route "
                "ON binance_spot_first_live_bootstraps(route_key) "
                "WHERE state='ACTIVE'"
            ),
        }
        for name, definition in definitions.items():
            with self.subTest(name=name):
                path = Path(self.temporary.name) / f"wrong-index-{name}.sqlite3"
                self.store(path)
                with closing(sqlite3.connect(str(path))) as connection:
                    connection.execute(
                        "DROP INDEX ux_binance_spot_first_live_ever_route"
                    )
                    connection.execute(definition)
                    connection.commit()
                with self.assertRaisesRegex(
                    BinanceSpotFirstLiveBootstrapError, "schema fingerprint"
                ):
                    self.store(path)

    def test_exact_schema_rejects_extra_column_trigger_type_and_default(self) -> None:
        extra_column = Path(self.temporary.name) / "extra-column.sqlite3"
        self.store(extra_column)
        with closing(sqlite3.connect(str(extra_column))) as connection:
            connection.execute(
                "ALTER TABLE binance_spot_first_live_bootstraps "
                "ADD COLUMN rogue TEXT"
            )
            connection.commit()

        extra_trigger = Path(self.temporary.name) / "extra-trigger.sqlite3"
        self.store(extra_trigger)
        with closing(sqlite3.connect(str(extra_trigger))) as connection:
            connection.execute(
                "CREATE TRIGGER rogue_bootstrap_trigger AFTER INSERT ON "
                "binance_spot_first_live_bootstraps BEGIN SELECT 1; END"
            )
            connection.commit()

        wrong_type = self.malformed_schema_copy(
            name="wrong-type",
            table_rewrite=lambda sql: sql.replace(
                "route_key TEXT NOT NULL", "route_key BLOB NOT NULL"
            ),
        )
        wrong_default = self.malformed_schema_copy(
            name="wrong-default",
            table_rewrite=lambda sql: sql.replace(
                "detail TEXT NOT NULL DEFAULT ''",
                "detail TEXT NOT NULL DEFAULT 'rogue'",
            ),
        )
        for path in (extra_column, extra_trigger, wrong_type, wrong_default):
            with self.subTest(path=path.name), self.assertRaisesRegex(
                BinanceSpotFirstLiveBootstrapError, "schema fingerprint"
            ):
                self.store(path)

    def test_mutation_rechecks_schema_after_store_construction(self) -> None:
        store = self.store()
        with closing(sqlite3.connect(str(self.path))) as connection:
            connection.execute(
                "DROP INDEX ux_binance_spot_first_live_ever_route"
            )
            connection.execute(
                "CREATE INDEX ux_binance_spot_first_live_ever_route "
                "ON binance_spot_first_live_bootstraps(route_key)"
            )
            connection.commit()
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError, "schema fingerprint"
        ):
            store.issue(
                binding=ExactBinding.parse(binding()),
                approval_id="approval-schema-drift-0001",
                permit_id="permit-schema-drift-0000001",
                permit_hash="a" * 64,
            )
        with closing(sqlite3.connect(str(self.path))) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM binance_spot_first_live_bootstraps"
            ).fetchone()[0]
        self.assertEqual(0, count)

    def test_claim_rejects_exact_expiry_boundary(self) -> None:
        store = self.store()
        issued, raw = store.issue(
            binding=ExactBinding.parse(binding()),
            approval_id="approval-expiry-boundary-0001",
            permit_id="permit-expiry-boundary-000001",
            permit_hash="a" * 64,
        )
        self.clock.value += 300
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError, "stale"
        ):
            store.claim(
                bootstrap_id=str(issued["bootstrap_id"]),
                raw_capability=raw,
                approval_id="approval-expiry-boundary-0001",
                permit_id="permit-expiry-boundary-000001",
                permit_hash="a" * 64,
            )

    def test_two_store_instances_concurrently_commit_exactly_one_issue(self) -> None:
        first = self.store()
        second = self.store()
        start = threading.Barrier(2)
        outcomes: list[tuple[str, object]] = []
        outcomes_lock = threading.Lock()

        def issue(store, suffix: str) -> None:
            start.wait()
            try:
                result = store.issue(
                    binding=ExactBinding.parse(binding()),
                    approval_id=f"approval-concurrent-{suffix}-0001",
                    permit_id=f"permit-concurrent-{suffix}-000001",
                    permit_hash="a" * 64,
                )
            except Exception as exc:
                outcome: tuple[str, object] = ("error", exc)
            else:
                outcome = ("ok", result[0]["bootstrap_id"])
            with outcomes_lock:
                outcomes.append(outcome)

        threads = [
            threading.Thread(target=issue, args=(first, "first")),
            threading.Thread(target=issue, args=(second, "second")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(1, sum(kind == "ok" for kind, _ in outcomes))
        self.assertEqual(1, sum(kind == "error" for kind, _ in outcomes))
        self.assertIsInstance(
            next(value for kind, value in outcomes if kind == "error"),
            BinanceSpotFirstLiveBootstrapError,
        )
        with closing(sqlite3.connect(str(self.path))) as connection:
            self.assertEqual(
                1,
                connection.execute(
                    "SELECT COUNT(*) FROM binance_spot_first_live_bootstraps"
                ).fetchone()[0],
            )

    def test_orphan_burn_serializes_against_claim_and_reports_exact_rows(self) -> None:
        store = self.store()
        issued, raw = store.issue(
            binding=ExactBinding.parse(binding()),
            approval_id="approval-orphan-race-0001",
            permit_id="permit-orphan-race-000001",
            permit_hash="a" * 64,
        )
        selected = threading.Event()
        release = threading.Event()

        def blocking_clock() -> float:
            if threading.current_thread().name == "orphan-burn":
                selected.set()
                if not release.wait(5):
                    raise TimeoutError("orphan burn test clock timed out")
            return self.clock()

        store.clock = blocking_clock
        burned: list[str] = []
        claim_errors: list[Exception] = []

        def burn() -> None:
            burned.extend(store.fail_orphans_after_process_loss())

        def claim() -> None:
            try:
                store.claim(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    raw_capability=raw,
                    approval_id="approval-orphan-race-0001",
                    permit_id="permit-orphan-race-000001",
                    permit_hash="a" * 64,
                )
            except Exception as exc:
                claim_errors.append(exc)

        burn_thread = threading.Thread(target=burn, name="orphan-burn")
        burn_thread.start()
        self.assertTrue(selected.wait(2))
        claim_thread = threading.Thread(target=claim, name="concurrent-claim")
        claim_thread.start()
        self.assertTrue(claim_thread.is_alive())
        release.set()
        burn_thread.join(timeout=5)
        claim_thread.join(timeout=5)
        self.assertFalse(burn_thread.is_alive())
        self.assertFalse(claim_thread.is_alive())
        self.assertEqual([issued["bootstrap_id"]], burned)
        self.assertEqual(1, len(claim_errors))
        self.assertIsInstance(
            claim_errors[0], BinanceSpotFirstLiveBootstrapError
        )
        self.assertEqual(
            "FAILED", store.status(str(issued["bootstrap_id"]))["state"]
        )
        self.gates["ordinaryBinanceRoutesClosed"] = True
        self.gates["realE2EAvailable"] = True
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError, "before permanent"
        ):
            store.issue(
                binding=ExactBinding.parse(binding()),
                approval_id="approval-first-live-0002",
                permit_id="permit-first-live-0000002",
                permit_hash="a" * 64,
            )

    def test_process_loss_burns_raw_capability_and_code_swap_blocks_claim(self) -> None:
        store = self.store()
        issued, raw = store.issue(
            binding=ExactBinding.parse(binding()),
            approval_id="approval-first-live-0003",
            permit_id="permit-first-live-0000003",
            permit_hash="a" * 64,
        )
        self.code_hash = "e" * 64
        with self.assertRaisesRegex(BinanceSpotFirstLiveBootstrapError, "changed"):
            store.claim(
                bootstrap_id=str(issued["bootstrap_id"]),
                raw_capability=raw,
                approval_id="approval-first-live-0003",
                permit_id="permit-first-live-0000003",
                permit_hash="a" * 64,
            )
        self.code_hash = "d" * 64
        self.assertEqual(
            [issued["bootstrap_id"]], store.fail_orphans_after_process_loss()
        )
        with self.assertRaises(BinanceSpotFirstLiveBootstrapError):
            store.claim(
                bootstrap_id=str(issued["bootstrap_id"]),
                raw_capability=raw,
                approval_id="approval-first-live-0003",
                permit_id="permit-first-live-0000003",
                permit_hash="a" * 64,
            )
        self.assertEqual("FAILED", store.status(str(issued["bootstrap_id"]))["state"])

    def test_code_swap_blocks_session_bind_and_terminal_consume(self) -> None:
        store = self.store()
        exact_binding = ExactBinding.parse(binding())
        issued, raw = store.issue(
            binding=exact_binding,
            approval_id="approval-code-drift-0001",
            permit_id="permit-code-drift-0000001",
            permit_hash="a" * 64,
        )
        claim = store.claim(
            bootstrap_id=str(issued["bootstrap_id"]),
            raw_capability=raw,
            approval_id="approval-code-drift-0001",
            permit_id="permit-code-drift-0000001",
            permit_hash="a" * 64,
        )
        self.code_hash = "e" * 64
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError, "bind changed"
        ):
            store.bind_session(
                bootstrap_id=str(issued["bootstrap_id"]),
                claim_token=claim,
                approval_id="approval-code-drift-0001",
                active_permit_id="permit-code-drift-active-1",
                active_permit_hash="b" * 64,
                session_id="bnsft-code-drift-session-1",
                binding=exact_binding,
                activated_epoch=self.clock(),
                active_ends_epoch=self.clock() + 7200,
            )
        self.code_hash = "d" * 64
        active = store.bind_session(
            bootstrap_id=str(issued["bootstrap_id"]),
            claim_token=claim,
            approval_id="approval-code-drift-0001",
            active_permit_id="permit-code-drift-active-1",
            active_permit_hash="b" * 64,
            session_id="bnsft-code-drift-session-1",
            binding=exact_binding,
            activated_epoch=self.clock(),
            active_ends_epoch=self.clock() + 7200,
        )
        self.assertEqual("ACTIVE", active["state"])
        evidence = self.eligible_evidence(
            session_id="bnsft-code-drift-session-1",
            permit_id="permit-code-drift-active-1",
            permit_hash="b" * 64,
        )
        self.seal_durable_execution(store, evidence)
        self.code_hash = "f" * 64
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError,
            "terminal.*changed",
        ):
            store.consume_terminal(
                bootstrap_id=str(issued["bootstrap_id"]),
                session_id="bnsft-code-drift-session-1",
                permit_id="permit-code-drift-active-1",
                permit_hash="b" * 64,
                evidence=evidence,
                evidence_hash=canonical_hash(evidence),
            )

    def test_terminal_without_natural_sell_consumes_but_cannot_release_e2e(self) -> None:
        store = self.store()
        exact_binding = ExactBinding.parse(binding())
        issued, raw = store.issue(
            binding=exact_binding,
            approval_id="approval-first-live-0004",
            permit_id="permit-first-live-0000004",
            permit_hash="a" * 64,
        )
        token = store.claim(
            bootstrap_id=str(issued["bootstrap_id"]),
            raw_capability=raw,
            approval_id="approval-first-live-0004",
            permit_id="permit-first-live-0000004",
            permit_hash="a" * 64,
        )
        session_id = "bnsft-first-live-session-0004"
        store.bind_session(
            bootstrap_id=str(issued["bootstrap_id"]),
            claim_token=token,
            approval_id="approval-first-live-0004",
            active_permit_id="permit-active-first-live-0004",
            active_permit_hash="b" * 64,
            session_id=session_id,
            binding=exact_binding,
            activated_epoch=self.clock(),
            active_ends_epoch=self.clock() + 7200,
        )
        evidence = self.eligible_evidence(
            session_id=session_id,
            permit_id="permit-active-first-live-0004",
            permit_hash="b" * 64,
        )
        evidence["naturalSellFilled"] = False
        evidence["fullRoundTripWiringPassed"] = False
        evidence["functionalWiringPassed"] = False
        evidence["outcome"] = "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT"
        self.clock.value += 7200
        consumed = store.consume_terminal(
            bootstrap_id=str(issued["bootstrap_id"]),
            session_id=session_id,
            permit_id="permit-active-first-live-0004",
            permit_hash="b" * 64,
            evidence=evidence,
            evidence_hash=canonical_hash(evidence),
        )
        self.assertEqual("CONSUMED", consumed["state"])
        self.assertEqual(0, consumed["e2e_evidence_eligible"])
        self.assertEqual(0, consumed["functional_wiring_passed"])

    def test_causal_unproven_terminal_consumes_with_separate_wiring_pass(self) -> None:
        store = self.store()
        exact_binding = ExactBinding.parse(binding())
        issued, raw = store.issue(
            binding=exact_binding,
            approval_id="approval-first-live-causal-safe-0005",
            permit_id="permit-first-live-causal-safe-0005",
            permit_hash="a" * 64,
        )
        token = store.claim(
            bootstrap_id=str(issued["bootstrap_id"]),
            raw_capability=raw,
            approval_id="approval-first-live-causal-safe-0005",
            permit_id="permit-first-live-causal-safe-0005",
            permit_hash="a" * 64,
        )
        session_id = "bnsft-first-live-causal-safe-0005"
        active_permit_id = "permit-active-causal-safe-0005"
        active_permit_hash = "b" * 64
        store.bind_session(
            bootstrap_id=str(issued["bootstrap_id"]),
            claim_token=token,
            approval_id="approval-first-live-causal-safe-0005",
            active_permit_id=active_permit_id,
            active_permit_hash=active_permit_hash,
            session_id=session_id,
            binding=exact_binding,
            activated_epoch=self.clock(),
            active_ends_epoch=self.clock() + 7200,
        )
        evidence = self.eligible_evidence(
            session_id=session_id,
            permit_id=active_permit_id,
            permit_hash=active_permit_hash,
        )
        evidence["outcome"] = (
            "SAFE_INCOMPLETE_ACCOUNT_WIDE_CAUSAL_CLOSURE_UNPROVEN"
        )
        evidence["accountWideCausalClosureProven"] = False
        evidence["otherApiKeysAbsenceAuthoritativelyProven"] = False
        self.seal_durable_execution(store, evidence)
        consumed = store.consume_terminal(
            bootstrap_id=str(issued["bootstrap_id"]),
            session_id=session_id,
            permit_id=active_permit_id,
            permit_hash=active_permit_hash,
            evidence=evidence,
            evidence_hash=canonical_hash(evidence),
        )
        self.assertEqual("CONSUMED", consumed["state"])
        self.assertEqual(0, consumed["functional_wiring_passed"])
        self.assertEqual(0, consumed["e2e_evidence_eligible"])

    def test_wiring_pass_requires_every_runtime_risk_and_cleanup_proof(self) -> None:
        mutations: tuple[tuple[str, object], ...] = (
            ("actualRuntimeSeconds", "7199"),
            ("exchangeRuntimeSeconds", "7199"),
            ("monotonicRuntimeSeconds", "7199"),
            ("runtimeClockConsistencyProven", False),
            ("feesQuoteExact", False),
            ("orderCapsAndNoReentryProven", False),
            ("baselineRestoredWithinExchangePrecision", False),
            ("externalActivityAbsent", False),
            ("privateStreamGapRecoveredCleanupOnly", True),
        )
        exact_binding = ExactBinding.parse(binding())
        for index, (field, changed) in enumerate(mutations, start=1):
            with self.subTest(field=field):
                path = Path(self.temporary.name) / f"negative-{index}.sqlite3"
                store = DurableBinanceSpotFirstLiveBootstrapStore(
                    path,
                    gate_reader=lambda: dict(self.gates),
                    server_record_signer=lambda body: {
                        **dict(body),
                        "serverSignature": canonical_hash(dict(body)),
                    },
                    code_hash_reader=lambda: self.code_hash,
                    clock=self.clock,
                )
                approval_id = f"approval-negative-wiring-{index:04d}"
                initial_permit_id = f"permit-negative-wiring-{index:04d}"
                issued, raw = store.issue(
                    binding=exact_binding,
                    approval_id=approval_id,
                    permit_id=initial_permit_id,
                    permit_hash="a" * 64,
                )
                token = store.claim(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    raw_capability=raw,
                    approval_id=approval_id,
                    permit_id=initial_permit_id,
                    permit_hash="a" * 64,
                )
                session_id = f"bnsft-negative-wiring-{index:04d}"
                active_permit_id = f"permit-active-negative-{index:04d}"
                active_permit_hash = "b" * 64
                store.bind_session(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    claim_token=token,
                    approval_id=approval_id,
                    active_permit_id=active_permit_id,
                    active_permit_hash=active_permit_hash,
                    session_id=session_id,
                    binding=exact_binding,
                    activated_epoch=self.clock(),
                    active_ends_epoch=self.clock() + 7200,
                )
                evidence = self.eligible_evidence(
                    session_id=session_id,
                    permit_id=active_permit_id,
                    permit_hash=active_permit_hash,
                )
                evidence["outcome"] = (
                    "SAFE_INCOMPLETE_ACCOUNT_WIDE_CAUSAL_CLOSURE_UNPROVEN"
                )
                evidence["accountWideCausalClosureProven"] = False
                evidence["otherApiKeysAbsenceAuthoritativelyProven"] = False
                evidence[field] = changed
                self.seal_durable_execution(store, evidence)
                consumed = store.consume_terminal(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    session_id=session_id,
                    permit_id=active_permit_id,
                    permit_hash=active_permit_hash,
                    evidence=evidence,
                    evidence_hash=canonical_hash(evidence),
                )
                self.assertEqual(0, consumed["functional_wiring_passed"])
                self.assertEqual(0, consumed["e2e_evidence_eligible"])

    def test_terminal_bootstrap_rejects_working_order_evidence(self) -> None:
        store = self.store()
        exact_binding = ExactBinding.parse(binding())
        issued, raw = store.issue(
            binding=exact_binding,
            approval_id="approval-open-order-0001",
            permit_id="permit-open-order-0000001",
            permit_hash="a" * 64,
        )
        token = store.claim(
            bootstrap_id=str(issued["bootstrap_id"]),
            raw_capability=raw,
            approval_id="approval-open-order-0001",
            permit_id="permit-open-order-0000001",
            permit_hash="a" * 64,
        )
        session_id = "bnsft-open-order-session-0001"
        active_permit_id = "permit-open-order-active-0001"
        store.bind_session(
            bootstrap_id=str(issued["bootstrap_id"]),
            claim_token=token,
            approval_id="approval-open-order-0001",
            active_permit_id=active_permit_id,
            active_permit_hash="b" * 64,
            session_id=session_id,
            binding=exact_binding,
            activated_epoch=self.clock(),
            active_ends_epoch=self.clock() + 7200,
        )
        evidence = self.eligible_evidence(
            session_id=session_id,
            permit_id=active_permit_id,
            permit_hash="b" * 64,
        )
        evidence["openOrdersZero"] = False
        self.seal_durable_execution(store, evidence)
        with self.assertRaisesRegex(
            BinanceSpotFirstLiveBootstrapError, "not safely sealed"
        ):
            store.consume_terminal(
                bootstrap_id=str(issued["bootstrap_id"]),
                session_id=session_id,
                permit_id=active_permit_id,
                permit_hash="b" * 64,
                evidence=evidence,
                evidence_hash=canonical_hash(evidence),
            )

    def test_terminal_summary_booleans_cannot_replace_durable_execution(
        self,
    ) -> None:
        cases = (
            ("missing-actions", {"omit_actions": True}),
            ("extra-buy", {"extra_buy": True}),
            ("over-cap", {"over_cap": True}),
        )
        exact_binding = ExactBinding.parse(binding())
        for index, (name, mutation) in enumerate(cases, start=1):
            with self.subTest(name=name):
                path = Path(self.temporary.name) / f"durable-{index}.sqlite3"
                store = DurableBinanceSpotFirstLiveBootstrapStore(
                    path,
                    gate_reader=lambda: dict(self.gates),
                    server_record_signer=lambda body: {
                        **dict(body),
                        "serverSignature": canonical_hash(dict(body)),
                    },
                    code_hash_reader=lambda: self.code_hash,
                    clock=self.clock,
                )
                approval_id = f"approval-durable-proof-{index:04d}"
                initial_permit_id = f"permit-durable-proof-{index:04d}"
                issued, raw = store.issue(
                    binding=exact_binding,
                    approval_id=approval_id,
                    permit_id=initial_permit_id,
                    permit_hash="a" * 64,
                )
                token = store.claim(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    raw_capability=raw,
                    approval_id=approval_id,
                    permit_id=initial_permit_id,
                    permit_hash="a" * 64,
                )
                session_id = f"bnsft-durable-proof-{index:04d}"
                active_permit_id = f"permit-durable-active-{index:04d}"
                store.bind_session(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    claim_token=token,
                    approval_id=approval_id,
                    active_permit_id=active_permit_id,
                    active_permit_hash="b" * 64,
                    session_id=session_id,
                    binding=exact_binding,
                    activated_epoch=self.clock(),
                    active_ends_epoch=self.clock() + 7200,
                )
                evidence = self.eligible_evidence(
                    session_id=session_id,
                    permit_id=active_permit_id,
                    permit_hash="b" * 64,
                )
                self.seal_durable_execution(store, evidence, **mutation)
                with self.assertRaises(BinanceSpotFirstLiveBootstrapError):
                    store.consume_terminal(
                        bootstrap_id=str(issued["bootstrap_id"]),
                        session_id=session_id,
                        permit_id=active_permit_id,
                        permit_hash="b" * 64,
                        evidence=evidence,
                        evidence_hash=canonical_hash(evidence),
                    )

    def test_independent_raw_truth_and_stream_archive_reject_resealed_lies(
        self,
    ) -> None:
        cases = (
            ("raw-cutoff-7199", {"raw_cutoff_short": True}),
            ("hidden-open-order", {"hidden_raw_open": True}),
            ("extra-raw-fill", {"extra_raw_fill": True}),
            ("raw-fee-tamper", {"raw_fee_tamper": True}),
            ("raw-status-tamper", {"raw_status_tamper": True}),
            ("malformed-trade-side", {"missing_trade_side": True}),
            ("external-quote-delta", {"external_quote_delta": True}),
            ("forced-signal-label", {"forced_signal_label": True}),
            (
                "stale-pre-activation-evaluation",
                {"stale_pre_activation_evaluation": True},
            ),
            ("late-natural-action", {"late_natural_action": True}),
            ("late-raw-fill", {"late_raw_fill": True}),
            (
                "evaluation-window-observed-mismatch",
                {"evaluation_observed_mismatch": True},
            ),
            (
                "post-expiry-official-server-time",
                {"post_expiry_server_time": True},
            ),
            (
                "bootstrap-permit-swap",
                {"bootstrap_permit_mismatch": True},
            ),
            ("approval-swap", {"approval_mismatch": True}),
            (
                "bootstrap-signature-tamper",
                {"bootstrap_signature_tamper": True},
            ),
            (
                "future-dated-terminal",
                {"keep_trusted_clock_at_activation": True},
            ),
            ("raw-hash-mismatch", {"raw_hash_mismatch": True}),
            ("truncated-history-page", {"truncated_raw_page": True}),
            ("extra-stream-event", {"extra_stream_event": True}),
            ("missing-terminal-truth", {"omit_terminal_truth": True}),
            ("missing-stream-archive", {"omit_stream_archive": True}),
            *tuple(
                (
                    f"evaluation-{field}-swap",
                    {"evaluation_lineage_tamper": field},
                )
                for field in (
                    "strategyArtifactId",
                    "strategyArtifactFileSha256",
                    "strategyInstanceId",
                    "strategyInstanceFileSha256",
                    "publicationProofHash",
                    "publicationProofFileSha256",
                    "accountFingerprint",
                    "bindingHash",
                    "sessionId",
                    "permitId",
                    "permitHash",
                )
            ),
        )
        exact_binding = ExactBinding.parse(binding())
        for index, (name, mutation) in enumerate(cases, start=1):
            with self.subTest(name=name):
                path = Path(self.temporary.name) / f"raw-lie-{index}.sqlite3"
                store = DurableBinanceSpotFirstLiveBootstrapStore(
                    path,
                    gate_reader=lambda: dict(self.gates),
                    server_record_signer=lambda body: {
                        **dict(body),
                        "serverSignature": canonical_hash(dict(body)),
                    },
                    code_hash_reader=lambda: self.code_hash,
                    clock=self.clock,
                )
                approval_id = f"approval-raw-lie-{index:04d}"
                initial_permit_id = f"permit-raw-lie-{index:04d}"
                issued, raw = store.issue(
                    binding=exact_binding,
                    approval_id=approval_id,
                    permit_id=initial_permit_id,
                    permit_hash="a" * 64,
                )
                token = store.claim(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    raw_capability=raw,
                    approval_id=approval_id,
                    permit_id=initial_permit_id,
                    permit_hash="a" * 64,
                )
                session_id = f"bnsft-raw-lie-{index:04d}"
                active_permit_id = f"permit-raw-lie-active-{index:04d}"
                store.bind_session(
                    bootstrap_id=str(issued["bootstrap_id"]),
                    claim_token=token,
                    approval_id=approval_id,
                    active_permit_id=active_permit_id,
                    active_permit_hash="b" * 64,
                    session_id=session_id,
                    binding=exact_binding,
                    activated_epoch=self.clock(),
                    active_ends_epoch=self.clock() + 7200,
                )
                evidence = self.eligible_evidence(
                    session_id=session_id,
                    permit_id=active_permit_id,
                    permit_hash="b" * 64,
                )
                self.seal_durable_execution(store, evidence, **mutation)
                with self.assertRaises(BinanceSpotFirstLiveBootstrapError):
                    store.consume_terminal(
                        bootstrap_id=str(issued["bootstrap_id"]),
                        session_id=session_id,
                        permit_id=active_permit_id,
                        permit_hash="b" * 64,
                        evidence=evidence,
                        evidence_hash=canonical_hash(evidence),
                    )

    def test_code_hash_binds_names_and_bytes(self) -> None:
        first = Path(self.temporary.name) / "a.py"
        second = Path(self.temporary.name) / "b.py"
        first.write_bytes(b"alpha")
        second.write_bytes(b"beta")
        original = compute_binance_spot_functional_code_hash((first, second))
        second.write_bytes(b"gamma")
        self.assertNotEqual(
            original, compute_binance_spot_functional_code_hash((first, second))
        )

    def test_code_hash_binds_path_qualified_manifest(self) -> None:
        root = Path(self.temporary.name)
        shared = root / "shared" / "authority.py"
        first = root / "route_a" / "edge.py"
        second = root / "route_b" / "edge.py"
        for path in (shared, first, second):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"same bytes")
        self.assertNotEqual(
            compute_binance_spot_functional_code_hash((shared, first)),
            compute_binance_spot_functional_code_hash((shared, second)),
        )

    def test_transitive_settings_and_confirmation_sources_are_drift_bound(
        self,
    ) -> None:
        source_root = Path(__file__).resolve().parents[1] / "live_trader"
        names = (
            "continuous_live.py",
            "env_loader.py",
            "env_settings.py",
            "safety_confirmation.py",
        )
        copied_root = Path(self.temporary.name) / "transitive-manifest"
        copied_root.mkdir()
        copied = []
        for name in names:
            target = copied_root / name
            target.write_bytes((source_root / name).read_bytes())
            copied.append(target)
        baseline = compute_binance_spot_functional_code_hash(copied)
        for target in copied:
            with self.subTest(path=target.name):
                original = target.read_bytes()
                target.write_bytes(original + b"\n# drift probe\n")
                self.assertNotEqual(
                    baseline, compute_binance_spot_functional_code_hash(copied)
                )
                target.write_bytes(original)

    def test_default_manifest_covers_shared_permit_and_final_fences(self) -> None:
        paths = default_binance_spot_functional_code_paths()
        normalized = {path.as_posix() for path in paths}
        required_suffixes = {
            "/live_trader/binance_order_authority.py",
            "/live_trader/binance_spot_publication.py",
            "/live_trader/emergency_stop.py",
            "/live_trader/binance_spot_functional_mutation.py",
            "/live_trader/binance_spot_functional_transport.py",
            "/live_trader/binance_spot_functional_exclusivity.py",
            "/live_trader/binance_spot_functional_exclusivity_provider.py",
            "/live_trader/continuous_live.py",
            "/live_trader/crypto_first_live_coordinator.py",
            "/live_trader/crypto_first_live_high_water.py",
            "/live_trader/crypto_first_live_runtime.py",
            "/live_trader/env_loader.py",
            "/live_trader/env_settings.py",
            "/live_trader/safety_confirmation.py",
            "/packages/trading_runtime/trading_runtime/functional_test.py",
        }
        for suffix in required_suffixes:
            with self.subTest(suffix=suffix):
                self.assertTrue(
                    any(value.endswith(suffix) for value in normalized),
                    suffix,
                )
        self.assertTrue(all(path.is_file() for path in paths))


if __name__ == "__main__":
    unittest.main()
