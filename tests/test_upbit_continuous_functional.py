from __future__ import annotations

from contextlib import contextmanager
import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from trading_runtime.functional_test import (
    FunctionalTestBinding,
    FunctionalTestEnvironment,
    issue_functional_test_permit,
)
from live_trader import upbit_continuous_functional as subject
from live_trader.upbit_continuous_functional import _stable_hash


NOW = datetime(2026, 8, 13, 1, 0, tzinfo=timezone.utc)
ACTIVATED_AT = NOW + timedelta(minutes=10)
ACCOUNT = "c" * 64
GLOBAL_OWNER = "8" * 64
TEST_EXCLUSIVITY_SECRET = b"offline-upbit-exclusivity-test-authority-v1"
TEST_EXCLUSIVITY_VERIFIER_PIN = {
    "schemaVersion": "upbit-account-exclusivity-verifier-pin/v1",
    "verifierId": "offline-test-exclusivity-verifier-v1",
    "keyId": "offline-test-authority-key-v1",
    "algorithm": "HMAC_SHA256_TEST_ONLY",
    "verifierType": "OFFLINE_EXACT_TEST_AUTHORITY",
    "verifierCodeSha256": "3" * 64,
    "verifierConfigSha256": "4" * 64,
    "keyFingerprintSha256": hashlib.sha256(
        TEST_EXCLUSIVITY_SECRET
    ).hexdigest(),
    "authorityPinned": True,
}


class ExactTestAccountExclusivityVerifier:
    """Offline-only authority fixture; never installed by production."""

    def identity(self) -> dict[str, object]:
        return dict(TEST_EXCLUSIVITY_VERIFIER_PIN)

    @staticmethod
    def sign(payload: dict[str, object]) -> str:
        return hmac.new(
            TEST_EXCLUSIVITY_SECRET,
            _stable_hash(payload).encode("ascii"),
            hashlib.sha256,
        ).hexdigest()

    def __call__(
        self,
        *,
        payload,
        signature: str,
        verifier_pin,
    ) -> bool:
        return bool(
            dict(verifier_pin) == TEST_EXCLUSIVITY_VERIFIER_PIN
            and hmac.compare_digest(signature, self.sign(dict(payload)))
        )


TEST_EXCLUSIVITY_VERIFIER = ExactTestAccountExclusivityVerifier()


def resign_test_account_exclusivity_proof(
    proof: dict[str, object],
) -> dict[str, object]:
    result = copy.deepcopy(proof)
    for field in ("apiKeyInventory", "manualTradeAudit", "botRegistry"):
        component = dict(result[field])
        component.pop("evidenceHash", None)
        result[field] = {
            **component,
            "evidenceHash": _stable_hash(component),
        }
    signed_payload = {
        key: value
        for key, value in result.items()
        if key not in {"payloadHash", "signature"}
    }
    result["payloadHash"] = _stable_hash(signed_payload)
    result["signature"] = TEST_EXCLUSIVITY_VERIFIER.sign(signed_payload)
    return result


def permit(*, binding: FunctionalTestBinding | None = None, hours: int = 2):
    return issue_functional_test_permit(
        binding=binding
        or FunctionalTestBinding(
            strategy_artifact_id="crypto-btc-finalized-5m",
            strategy_artifact_hash="a" * 64,
            strategy_instance_id="crypto-btc-finalized-5m-instance",
            portfolio_required=False,
            portfolio_artifact_id="",
            portfolio_artifact_hash="",
            portfolio_instance_id="",
            account_id=ACCOUNT,
            symbols=("KRW-BTC",),
            market_group="CRYPTO_SPOT",
            execution_route="UPBIT_KRW_SPOT_CONTINUOUS",
            settlement_currency="KRW",
            exchanges=("UPBIT_SPOT",),
            symbol_routes=(("KRW-BTC", "UPBIT_SPOT"),),
        ),
        environment=FunctionalTestEnvironment.UPBIT_LIVE,
        duration_value=hours,
        now=ACTIVATED_AT,
    )


class FakeBoundaries:
    def __init__(self, functional_permit) -> None:
        self.now = ACTIVATED_AT
        self.permit = functional_permit
        self.session_id = "upbit-functional-session-0001"
        self.base = Decimal("0.01000000")
        self.quote = Decimal("50000")
        self.mark = Decimal("100000000")
        self.fills: list[dict[str, str]] = []
        self.open_orders: list[dict[str, str]] = []
        self.closed_orders: list[dict[str, str]] = []
        self.post_calls = 0
        self.cancel_calls = 0
        self.real_orders = False
        self.post_error: Exception | None = None
        self.truth_errors: dict[str, Exception] = {}
        self.preused_identifier = False
        self.lease_updates: dict[str, object] = {}
        self.runtime_updates: dict[str, object] = {}
        self.capability_hash = ""
        self.selection_updates: dict[str, object] = {}
        self.register_wrong_capability = False
        self.monotonic_origin = self.now
        self.include_account_exclusivity_proof = True
        self.account_exclusivity_proof_mutator = None
        self.account_exclusivity_proof_mutators_by_phase: dict[str, object] = {}
        self.exclusivity_proof_missing_phases: set[str] = set()

    def account_exclusivity_proof(self) -> dict[str, object]:
        coverage_started_at = subject._utc_text(self.permit.starts_at)
        coverage_ended_at = subject._utc_text(self.now)

        def component(
            *,
            schema_version: str,
            source: str,
            counts: dict[str, int],
            artifact_hash: str,
        ) -> dict[str, object]:
            projection: dict[str, object] = {
                "schemaVersion": schema_version,
                "source": source,
                "accountFingerprint": ACCOUNT,
                "coverageStartedAt": coverage_started_at,
                "coverageEndedAt": coverage_ended_at,
                "complete": True,
                "independentlyVerified": True,
                "continuousCoverage": True,
                **counts,
                "authorityArtifactHash": artifact_hash,
            }
            return {**projection, "evidenceHash": _stable_hash(projection)}

        signed_payload: dict[str, object] = {
            "schemaVersion": (
                "upbit-functional-account-exclusivity-proof/v1"
            ),
            "sessionId": self.session_id,
            "accountFingerprint": ACCOUNT,
            "sessionStartedAt": coverage_started_at,
            "observationStartedAt": coverage_ended_at,
            "observedAt": coverage_ended_at,
            "authority": dict(TEST_EXCLUSIVITY_VERIFIER_PIN),
            "apiKeyInventory": component(
                schema_version=(
                    "upbit-account-api-key-inventory-evidence/v1"
                ),
                source=subject.ACCOUNT_API_KEY_INVENTORY_SOURCE,
                counts={
                    "activeApiKeyCount": 1,
                    "authorizedFunctionalApiKeyCount": 1,
                    "otherActiveApiKeyCount": 0,
                },
                artifact_hash="5" * 64,
            ),
            "manualTradeAudit": component(
                schema_version=(
                    "upbit-account-manual-trade-audit-evidence/v1"
                ),
                source=subject.ACCOUNT_MANUAL_TRADE_AUDIT_SOURCE,
                counts={"manualOrderCount": 0},
                artifact_hash="6" * 64,
            ),
            "botRegistry": component(
                schema_version=(
                    "upbit-account-bot-registry-evidence/v1"
                ),
                source=subject.ACCOUNT_BOT_REGISTRY_SOURCE,
                counts={
                    "activeBotCount": 1,
                    "authorizedFunctionalBotCount": 1,
                    "otherActiveBotCount": 0,
                },
                artifact_hash="7" * 64,
            ),
        }
        result = {
            **signed_payload,
            "payloadHash": _stable_hash(signed_payload),
            "signature": TEST_EXCLUSIVITY_VERIFIER.sign(signed_payload),
        }
        return result

    def clock(self) -> datetime:
        return self.now

    def monotonic(self) -> float:
        return (self.now - self.monotonic_origin).total_seconds()

    def runtime(self) -> dict[str, object]:
        scope = subject.UpbitPermitScope.parse(
            self.permit,
            immutable_selection=self.immutable_selection(),
        )
        result: dict[str, object] = {
            "executionPurpose": "FUNCTIONAL_TEST",
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "functionalTestSessionId": self.session_id,
            "functionalTestPermitId": scope.permit_id,
            "functionalTestPermitHash": scope.permit_hash,
            "functionalTestRouteScopeHash": scope.route_scope_hash,
            "functionalTestAccountFingerprint": ACCOUNT,
            "functionalTestSessionScopeHash": subject._stable_hash(scope.snapshot()),
            "killSwitch": False,
            "dryRun": False,
            "operatorConfirmed": True,
            "newEntriesBlocked": True,
            "realOrdersEnabled": False,
            "functionalOnlyRouting": True,
            "ordinaryRoutesClosed": True,
            "upbitSmokeRouteClosed": True,
        }
        result.update(self.runtime_updates)
        if self.capability_hash:
            result["functionalCapabilityHash"] = self.capability_hash
        return result

    def register_capability(self, capability_hash: str) -> None:
        if not capability_hash:
            self.capability_hash = ""
            return
        self.capability_hash = (
            "e" * 64 if self.register_wrong_capability else capability_hash
        )

    def immutable_selection(self) -> dict[str, object]:
        result: dict[str, object] = {
            "strategyArtifactId": self.permit.binding.strategy_artifact_id,
            "strategyArtifactHash": self.permit.binding.strategy_artifact_hash,
            "strategyArtifactFileSha256": "f" * 64,
            "strategyInstanceId": self.permit.binding.strategy_instance_id,
            "strategyInstanceHash": "d" * 64,
            "strategyInstanceFileSha256": "9" * 64,
            "strategyInstanceArtifactHash": self.permit.binding.strategy_artifact_hash,
            "accountFingerprint": ACCOUNT,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "interval": "5m",
            "verified": True,
            "publicationProofHash": "1" * 64,
            "publicationProofFileSha256": "2" * 64,
            "publicationProofVerified": True,
            "publishedProvider": "upbit",
            "publishedGroup": "crypto-upbit",
            "publishedSymbol": "KRW-BTC",
            "publishedStrategyArtifactHash": self.permit.binding.strategy_artifact_hash,
            "publishedStrategyArtifactFileSha256": "f" * 64,
            "publishedStrategyInstanceHash": "d" * 64,
            "publishedStrategyInstanceFileSha256": "9" * 64,
            "publishedActiveCatalogVisible": True,
            "publishedNaturalSignalsOnly": True,
            "publishedPromotionEligible": False,
        }
        result.update(self.selection_updates)
        return result

    def truth(
        self,
        *,
        session_id: str,
        phase: str,
        identifiers: tuple[str, ...] = (),
    ) -> dict[str, object]:
        self.assert_session(session_id)
        if phase in self.truth_errors:
            raise self.truth_errors[phase]
        total_fees = sum((Decimal(row["fee"]) for row in self.fills), Decimal("0"))
        all_orders = [*self.open_orders, *self.closed_orders]
        identifier_truth = {
            identifier: next(
                (
                    dict(row)
                    for row in all_orders
                    if row.get("identifier") == identifier
                ),
                None,
            )
            for identifier in identifiers
        }
        if phase == "FINAL_PRE_POST" and self.preused_identifier and identifiers:
            identifier = identifiers[-1]
            identifier_truth[identifier] = {
                "market": "KRW-BTC",
                "uuid": "external-order-with-colliding-identifier",
                "identifier": identifier,
                "side": "BID",
                "state": "done",
            }
        account_rows = [
            {
                "currency": "BTC",
                "available": subject._decimal_text(self.base),
                "locked": "0",
            },
            {
                "currency": "KRW",
                "available": subject._decimal_text(self.quote),
                "locked": "0",
            },
        ]
        raw_accounts = [
            {
                "currency": row["currency"],
                "balance": row["available"],
                "locked": row["locked"],
            }
            for row in account_rows
        ]
        raw_details: list[dict[str, object]] = []
        for order in all_orders:
            order_fills = [
                row
                for row in self.fills
                if row["identifier"] == order.get("identifier")
            ]
            raw_details.append(
                {
                    **dict(order),
                    "side": str(order.get("side") or "").lower(),
                    "state": str(order.get("state") or "").lower(),
                    "trades_count": len(order_fills),
                    "paid_fee": str(
                        sum(
                            (Decimal(row["fee"]) for row in order_fills),
                            Decimal("0"),
                        )
                    ),
                    "executed_volume": str(
                        sum(
                            (Decimal(row["volume"]) for row in order_fills),
                            Decimal("0"),
                        )
                    ),
                    "executed_funds": str(
                        sum(
                            (Decimal(row["funds"]) for row in order_fills),
                            Decimal("0"),
                        )
                    ),
                    "remaining_volume": "0",
                    "ord_type": "market",
                    "trades": [
                        {
                            "uuid": row["tradeUuid"],
                            "volume": row["volume"],
                            "funds": row["funds"],
                            "price": str(
                                Decimal(row["funds"])
                                / Decimal(row["volume"])
                            ),
                        }
                        for row in order_fills
                    ],
                }
            )
        details_by_identifier = {
            str(row["identifier"]): row for row in raw_details
        }
        if not (phase == "FINAL_PRE_POST" and self.preused_identifier):
            identifier_truth = {
                identifier: (
                    {
                        **dict(details_by_identifier[identifier]),
                        "side": str(
                            details_by_identifier[identifier].get("side") or ""
                        ).upper(),
                        "market": str(
                            details_by_identifier[identifier].get("market") or ""
                        ).upper(),
                        "state": str(
                            details_by_identifier[identifier].get("state") or ""
                        ).lower(),
                    }
                    if identifier in details_by_identifier
                    else None
                )
                for identifier in identifiers
            }
        raw_rest = {
            "schemaVersion": "upbit-functional-official-rest-raw/v2",
            "sessionId": session_id,
            "accountFingerprint": ACCOUNT,
            "sessionStartedAt": self.permit.starts_at.isoformat().replace(
                "+00:00", "Z"
            ),
            "observationCutoff": self.now.isoformat().replace("+00:00", "Z"),
            "accounts": {
                "endpoint": "/v1/accounts",
                "query": [],
                "payload": raw_accounts,
            },
            "orderChance": {
                "endpoint": "/v1/orders/chance",
                "query": [["market", "KRW-BTC"]],
                "payload": {
                "market": {
                    "id": "KRW-BTC",
                    "bid": {"min_total": "5000"},
                    "ask": {"min_total": "5000"},
                    "bid_types": ["price"],
                    "ask_types": ["market"],
                },
                "bid_account": {
                    "currency": "KRW",
                    "balance": str(self.quote),
                },
                "ask_account": {
                    "currency": "BTC",
                    "balance": str(self.base),
                },
                "bid_fee": "0.0005",
                "ask_fee": "0.0005",
                },
            },
            "ticker": {
                "endpoint": "/v1/ticker",
                "query": [["markets", "KRW-BTC"]],
                "payload": [
                    {"market": "KRW-BTC", "trade_price": str(self.mark)}
                ],
            },
            "openOrderPages": [{
                "page": 1,
                "endpoint": "/v1/orders/open",
                "query": [
                    ["states[]", "wait"],
                    ["states[]", "watch"],
                    ["page", "1"],
                    ["limit", "100"],
                    ["order_by", "asc"],
                ],
                "payload": [
                    {**row, "side": str(row.get("side") or "").lower()}
                    for row in self.open_orders
                ],
            }],
            "closedOrders": {
                "endpoint": "/v1/orders/closed",
                "query": [
                    ["states[]", "done"],
                    ["states[]", "cancel"],
                    ["start_time", self.permit.starts_at.isoformat().replace(
                        "+00:00", "Z"
                    )],
                    ["end_time", self.now.isoformat().replace("+00:00", "Z")],
                    ["limit", "1000"],
                    ["order_by", "asc"],
                ],
                "payload": [
                    {**row, "side": str(row.get("side") or "").lower()}
                    for row in self.closed_orders
                ],
            },
            "detailsByUuid": [
                {
                    "uuid": row["uuid"],
                    "endpoint": "/v1/order",
                    "query": [["uuid", row["uuid"]]],
                    "payload": row,
                }
                for row in raw_details
            ],
            "detailsByIdentifier": [
                {
                    "identifier": identifier,
                    "endpoint": "/v1/order",
                    "query": [["identifier", identifier]],
                    "payload": details_by_identifier.get(
                        identifier, {"_notFound": True}
                    ),
                }
                for identifier in identifiers
            ],
        }
        result = {
            "broker": "UPBIT",
            "market": "KRW-BTC",
            "accountFingerprint": ACCOUNT,
            "observedAt": self.now.isoformat().replace("+00:00", "Z"),
            "observationStartedAt": self.now.isoformat().replace("+00:00", "Z"),
            "truthReadDurationSeconds": 0,
            "accountComplete": True,
            "openOrdersComplete": True,
            "closedOrdersComplete": True,
            "fillsComplete": True,
            "feesComplete": True,
            "orderChanceComplete": True,
            "tickerComplete": True,
            "identifierTruthComplete": True,
            "privateStreamComplete": True,
            "privateStreamGapDetected": False,
            "privateStreamExternalActivityAbsent": True,
            "accountExternalActivityAbsent": True,
            "externalActivityScope": "UPBIT_ACCOUNT_ALL_MARKETS",
            "accountSource": "GET /v1/accounts",
            "orderChanceSource": "GET /v1/orders/chance",
            "tickerSource": "GET /v1/ticker",
            "quantityRuleSource": "UPBIT OFFICIAL MARKET ORDER 8-DECIMAL POLICY",
            "openOrdersScope": "ACCOUNT_ALL_OPEN_ORDERS",
            "closedOrdersScope": "ACCOUNT_SESSION_INTERVAL",
            "fillsScope": "ACCOUNT_SESSION_INTERVAL",
            "identifierTruthScope": "ALL_OWNED_IDENTIFIERS",
            "identifierTruth": identifier_truth,
            "privateStreamConnected": True,
            "privateStreamAuthenticated": True,
            "privateStreamSource": "UPBIT_WEBSOCKET_MYORDER",
            "privateStreamScope": "ACCOUNT_MYORDER_SESSION",
            "privateStreamEvents": [
                {
                    "eventId": row["tradeUuid"],
                    "orderUuid": row["orderUuid"],
                    "identifier": row["identifier"],
                    "market": row["market"],
                    "tradeUuid": row["tradeUuid"],
                }
                for row in self.fills
            ],
            "privateStreamWriterGeneration": 1,
            "privateStreamRevision": 1,
            "privateStreamEventCursor": len(self.fills),
            "privateStreamLastEventId": (
                self.fills[-1]["tradeUuid"] if self.fills else ""
            ),
            "privateStreamEventHeadHash": "b" * 64,
            "quoteAvailable": str(self.quote),
            "baseAvailable": str(self.base),
            "baseTotal": str(self.base),
            "accountRows": account_rows,
            "accountRowsHash": _stable_hash(account_rows),
            "markPrice": str(self.mark),
            "orderRules": {
                "bidMinTotal": "5000",
                "askMinTotal": "5000",
                "quantityStep": "0.00000001",
                "quantityScale": 8,
                "bidFeeRate": "0.0005",
                "askFeeRate": "0.0005",
            },
            "openOrders": list(self.open_orders),
            "closedOrders": list(self.closed_orders),
            "fills": list(self.fills),
            "totalFees": str(total_fees),
            "officialRestRawSnapshot": raw_rest,
            "officialRestRawSnapshotHash": _stable_hash(raw_rest),
        }
        if (
            self.include_account_exclusivity_proof
            and phase not in self.exclusivity_proof_missing_phases
        ):
            proof = self.account_exclusivity_proof()
            mutator = self.account_exclusivity_proof_mutators_by_phase.get(
                phase,
                self.account_exclusivity_proof_mutator,
            )
            if mutator is not None:
                proof = mutator(proof)
            result["accountExclusivityProof"] = proof
        return result

    def assert_session(self, session_id: str) -> None:
        if session_id != self.session_id:
            raise AssertionError("wrong session")

    def post(
        self,
        payload,
        *,
        functional_capability: str,
        functional_action: str,
        claim_id: str,
        request_hash: str,
    ) -> dict[str, str]:
        self.assert_capability(functional_capability)
        if not functional_action or not claim_id or len(request_hash) != 64:
            raise AssertionError("functional claim binding missing")
        self.post_calls += 1
        if self.post_error:
            raise self.post_error
        identifier = str(payload["identifier"])
        uuid = f"broker-order-{self.post_calls:04d}"
        if payload["side"] == "bid":
            funds = Decimal(str(payload["price"]))
            volume = (funds / self.mark).quantize(Decimal("0.00000001"))
            fee = funds * Decimal("0.0005")
            self.quote -= funds + fee
            self.base += volume
            side = "BID"
        else:
            volume = Decimal(str(payload["volume"]))
            funds = volume * self.mark
            fee = funds * Decimal("0.0005")
            self.quote += funds - fee
            self.base -= volume
            side = "ASK"
        self.fills.append(
            {
                "market": "KRW-BTC",
                "tradeUuid": f"trade-{self.post_calls:04d}",
                "orderUuid": uuid,
                "identifier": identifier,
                "side": side,
                "volume": str(volume),
                "funds": str(funds),
                "fee": str(fee),
            }
        )
        self.closed_orders.append(
            {
                "market": "KRW-BTC",
                "uuid": uuid,
                "identifier": identifier,
                "side": side,
                "state": "done",
            }
        )
        # Each successful strategy fill advances the deterministic test clock
        # to the next finalized 5m observation.  Production time is supplied
        # by the server clock; this keeps strict [start,end) fixtures honest.
        if functional_action in {"STRATEGY_BUY", "STRATEGY_SELL"}:
            self.now += timedelta(minutes=5)
        return {"uuid": uuid, "identifier": identifier, "state": "done"}

    def cancel(
        self,
        *,
        identifier: str,
        functional_capability: str,
        functional_action: str,
        claim_id: str,
        request_hash: str,
    ) -> dict[str, str]:
        self.assert_capability(functional_capability)
        if not functional_action or not claim_id or len(request_hash) != 64:
            raise AssertionError("functional claim binding missing")
        self.cancel_calls += 1
        matching = [row for row in self.open_orders if row["identifier"] == identifier]
        if len(matching) != 1:
            raise RuntimeError("not exactly one")
        row = matching[0]
        self.open_orders.remove(row)
        self.closed_orders.append({**row, "state": "cancel"})
        return {"uuid": row["uuid"], "identifier": identifier, "state": "cancel"}

    def assert_capability(self, raw: str) -> None:
        self.assert_session(self.session_id)
        if hashlib.sha256(raw.encode("utf-8")).hexdigest() != self.capability_hash:
            raise AssertionError("wrong functional capability")

    @contextmanager
    def lease(self, *, session_id: str, claim_id: str):
        def read():
            result: dict[str, object] = {
                "active": True,
                "sessionId": session_id,
                "claimId": claim_id,
                "permitHash": self.permit.content_hash,
            }
            result.update(self.lease_updates)
            return result

        yield read


class UpbitContinuousFunctionalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.permit = permit()
        self.fake = FakeBoundaries(self.permit)
        self.ledger = subject.UpbitFunctionalLedger(
            Path(self.temp.name) / "upbit.sqlite3",
            clock=self.fake.clock,
        )

    def activate(
        self,
        *,
        account_authority: bool = True,
        global_authority_reader=None,
        global_owner_identity_hash: str = "",
    ):
        service = subject._activate_for_test(
            permit=self.permit,
            ledger=self.ledger,
            session_id=self.fake.session_id,
            truth_reader=self.fake.truth,
            post_order=self.fake.post,
            cancel_order=self.fake.cancel,
            lease_factory=self.fake.lease,
            runtime_reader=self.fake.runtime,
            immutable_selection_reader=self.fake.immutable_selection,
            runtime_capability_registrar=self.fake.register_capability,
            real_orders_reader=lambda: self.fake.real_orders,
            clock=self.fake.clock,
            monotonic_clock=self.fake.monotonic,
            account_exclusivity_verifier=(
                TEST_EXCLUSIVITY_VERIFIER if account_authority else None
            ),
            account_exclusivity_verifier_pin=(
                TEST_EXCLUSIVITY_VERIFIER_PIN if account_authority else None
            ),
            global_first_live_authority_reader=global_authority_reader,
            global_first_live_owner_identity_hash=(
                global_owner_identity_hash
            ),
        )
        self.fake.real_orders = True
        self.fake.runtime_updates.update(
            {"newEntriesBlocked": True, "realOrdersEnabled": True}
        )
        return service

    def disarm_for_final(self) -> None:
        self.fake.real_orders = False
        self.fake.runtime_updates.update(
            {"newEntriesBlocked": True, "realOrdersEnabled": False}
        )

    def enter_cleanup_runtime(self) -> None:
        self.fake.runtime_updates["newEntriesBlocked"] = True

    def exact_roundtrip_final(
        self, *, account_authority: bool = True
    ) -> dict[str, object]:
        service = self.activate(account_authority=account_authority)
        service.on_bar(self.bar("BUY"))
        service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
        )
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(seconds=7200)
        return service.finalize_if_flat()

    def sealed_bar(
        self, signal: str, *, bar_id: str = "upbit-five-minute-bar-0001"
    ) -> subject.FinalizedFiveMinuteBar:
        return subject.FinalizedFiveMinuteBar.parse(
            self.bar(signal, bar_id=bar_id),
            now=self.fake.now,
            strategy_artifact_id="crypto-btc-finalized-5m",
            strategy_artifact_hash="a" * 64,
            strategy_artifact_file_sha256="f" * 64,
            strategy_instance_id="crypto-btc-finalized-5m-instance",
            strategy_instance_hash="d" * 64,
            strategy_instance_file_sha256="9" * 64,
            publication_proof_hash="1" * 64,
            publication_proof_file_sha256="2" * 64,
        )

    @staticmethod
    def bar(
        signal: str,
        *,
        bar_id: str = "upbit-five-minute-bar-0001",
        closed_at: datetime | None = None,
        observed_at: datetime | None = None,
    ) -> dict[str, object]:
        try:
            index = int(bar_id.rsplit("-", 1)[-1])
        except ValueError:
            index = 1
        # The immediate-activation lane has no pre-activation ARMED_WAIT
        # authority.  Default fixtures therefore start exactly at the
        # durable activation boundary and advance in finalized 5m windows.
        closed_at = closed_at or ACTIVATED_AT + timedelta(
            minutes=(index - 1) * 5
        )
        closes = (
            [10] * 10 + [20]
            if signal == "BUY"
            else [20] * 10 + [10]
            if signal == "SELL"
            else [10] * 11
        )
        rows: list[dict[str, object]] = []
        raw_response: list[dict[str, object]] = []
        for offset, close in enumerate(closes):
            row_closed_at = closed_at - timedelta(
                minutes=5 * (len(closes) - 1 - offset)
            )
            opened_at = row_closed_at - timedelta(minutes=5)
            official_bar_id = "upbit-rest-five-minute-" + opened_at.strftime(
                "%Y%m%dT%H%M%SZ"
            )
            rows.append(
                {
                    "barId": official_bar_id,
                    "closedAt": row_closed_at.isoformat(
                        timespec="microseconds"
                    ).replace(
                        "+00:00", "Z"
                    ),
                    "close": str(close),
                    "finalized": True,
                    "closed": True,
                }
            )
            raw_response.append(
                {
                    "market": "KRW-BTC",
                    "candle_date_time_utc": opened_at.replace(
                        tzinfo=None
                    ).isoformat(timespec="seconds"),
                    "trade_price": str(close),
                    "timestamp": int(row_closed_at.timestamp() * 1000),
                }
            )
        raw_response.reverse()
        final_bar_id = str(rows[-1]["barId"])
        raw_response_hash = _stable_hash(raw_response)
        observed_at = observed_at or max(ACTIVATED_AT, closed_at)
        raw_window = {
            "schemaVersion": "upbit-official-finalized-5m-window-v1",
            "symbol": "KRW-BTC",
            "interval": "5m",
            "source": "UPBIT_REST",
            "finalized": True,
            "closed": True,
            "barId": final_bar_id,
            "closedAt": closed_at.isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z"),
            "bars": rows,
            "officialCandleEvidence": {
                "schemaVersion": "upbit-official-candle-rest-evidence/v1",
                "origin": "https://api.upbit.com",
                "endpoint": "/v1/candles/minutes/5",
                "orderedQuery": [["market", "KRW-BTC"], ["count", "20"]],
                "observedAt": observed_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "maxResponseTimestampMs": max(
                    int(row["timestamp"]) for row in raw_response
                ),
                "rawResponse": raw_response,
                "rawResponseHash": raw_response_hash,
            },
        }
        bar_hash = _stable_hash(raw_window)
        evaluation_id = "upbit-ma-eval-" + _stable_hash(
            {
                "windowHash": bar_hash,
                "strategyArtifactHash": "a" * 64,
                "strategyInstanceHash": "d" * 64,
            }
        )[:32]
        return {
            "schemaVersion": "upbit-natural-ma-evaluation/v1",
            "symbol": "KRW-BTC",
            "interval": "5m",
            "finalized": True,
            "closed": True,
            "source": "UPBIT_REST",
            "barId": final_bar_id,
            "barHash": bar_hash,
            "closedAt": closed_at.isoformat().replace("+00:00", "Z"),
            "signal": signal,
            "evaluationId": evaluation_id,
            "strategyEvaluationComplete": True,
            "naturalSignal": True,
            "forcedSignal": False,
            "signalOverrideUsed": False,
            "manualSignal": False,
            "strategyArtifactId": "crypto-btc-finalized-5m",
            "strategyArtifactHash": "a" * 64,
            "strategyArtifactFileSha256": "f" * 64,
            "strategyInstanceId": "crypto-btc-finalized-5m-instance",
            "strategyInstanceHash": "d" * 64,
            "strategyInstanceFileSha256": "9" * 64,
            "publicationProofHash": "1" * 64,
            "publicationProofFileSha256": "2" * 64,
            "strategyPluginId": "moving_average_cross",
            "strategyShortMa": 3,
            "strategyLongMa": 10,
            "rawFinalizedWindow": raw_window,
        }

    def test_production_entry_stays_unavailable_until_e2e(self) -> None:
        self.assertFalse(subject.UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE)
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "availability-false"):
            subject.activate_upbit_continuous_functional(
                permit=self.permit,
                ledger=self.ledger,
                session_id=self.fake.session_id,
                truth_reader=self.fake.truth,
                post_order=self.fake.post,
                cancel_order=self.fake.cancel,
                lease_factory=self.fake.lease,
                runtime_reader=self.fake.runtime,
                immutable_selection_reader=self.fake.immutable_selection,
                runtime_capability_registrar=self.fake.register_capability,
                real_orders_reader=lambda: True,
                clock=self.fake.clock,
            )
        self.assertEqual(0, self.fake.post_calls)

    def test_production_activation_stays_hold_when_exclusivity_authority_unpinned(
        self,
    ) -> None:
        with patch.object(
            subject, "UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE", True
        ):
            with self.assertRaisesRegex(
                subject.UpbitFunctionalBlocked,
                "account-exclusivity-authority-unpinned",
            ):
                subject.activate_upbit_continuous_functional(
                    permit=self.permit,
                    ledger=self.ledger,
                    session_id=self.fake.session_id,
                    truth_reader=self.fake.truth,
                    post_order=self.fake.post,
                    cancel_order=self.fake.cancel,
                    lease_factory=self.fake.lease,
                    runtime_reader=self.fake.runtime,
                    immutable_selection_reader=(
                        self.fake.immutable_selection
                    ),
                    runtime_capability_registrar=(
                        self.fake.register_capability
                    ),
                    real_orders_reader=lambda: self.fake.real_orders,
                    clock=self.fake.clock,
                    account_exclusivity_verifier=(
                        TEST_EXCLUSIVITY_VERIFIER
                    ),
                    account_exclusivity_verifier_pin=(
                        TEST_EXCLUSIVITY_VERIFIER_PIN
                    ),
                )
        self.assertFalse(
            subject.UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED
        )
        self.assertEqual(
            "HOLD", subject.UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_STATUS
        )
        self.assertEqual(0, self.fake.post_calls)

    def test_exact_permit_scope_fingerprint_and_two_hour_deadline(self) -> None:
        scope = subject.UpbitPermitScope.parse(
            self.permit,
            immutable_selection=self.fake.immutable_selection(),
        )
        self.assertEqual(ACTIVATED_AT + timedelta(hours=2), scope.ends_at)
        self.assertEqual(ACTIVATED_AT + timedelta(hours=3), scope.cleanup_deadline)
        self.assertEqual("UPBIT_KRW_SPOT_CONTINUOUS", subject.EXECUTION_ROUTE)

        bad_binding = replace(self.permit.binding, account_id="not-a-hash")
        bad_permit = permit(binding=bad_binding)
        with self.assertRaises(subject.UpbitFunctionalBlocked):
            subject.UpbitPermitScope.parse(
                bad_permit,
                immutable_selection=self.fake.immutable_selection(),
            )
        with self.assertRaisesRegex(Exception, "two-hours"):
            permit(hours=3)

    def test_actual_published_krw_btc_proof_keeps_declared_and_file_hashes_distinct(self) -> None:
        root = Path(__file__).resolve().parents[3]
        proof_path = root / "apps" / "backtester" / "tmp" / "crypto-dual-5m-publication-proof-v1.json"
        proof_bytes = proof_path.read_bytes()
        proof = json.loads(proof_bytes)
        publication = next(
            row for row in proof["publications"]
            if row.get("provider") == "upbit" and row.get("symbol") == "KRW-BTC"
        )
        self.assertEqual(
            publication["strategyArtifactFileSha256"],
            hashlib.sha256(Path(publication["strategyArtifactPath"]).read_bytes()).hexdigest(),
        )
        self.assertEqual(
            publication["strategyInstanceFileSha256"],
            hashlib.sha256(Path(publication["strategyInstancePath"]).read_bytes()).hexdigest(),
        )
        self.assertNotEqual(publication["strategyArtifactHash"], publication["strategyArtifactFileSha256"])
        self.assertNotEqual(publication["strategyInstanceHash"], publication["strategyInstanceFileSha256"])
        actual_permit = permit(binding=FunctionalTestBinding(
            strategy_artifact_id=publication["strategyArtifactId"],
            strategy_artifact_hash=publication["strategyArtifactHash"],
            strategy_instance_id=publication["strategyInstanceId"],
            portfolio_required=False,
            portfolio_artifact_id="",
            portfolio_artifact_hash="",
            portfolio_instance_id="",
            account_id=ACCOUNT,
            symbols=("KRW-BTC",),
            market_group="CRYPTO_SPOT",
            execution_route="UPBIT_KRW_SPOT_CONTINUOUS",
            settlement_currency="KRW",
            exchanges=("UPBIT_SPOT",),
            symbol_routes=(("KRW-BTC", "UPBIT_SPOT"),),
        ))
        selection = {
            "strategyArtifactId": publication["strategyArtifactId"],
            "strategyArtifactHash": publication["strategyArtifactHash"],
            "strategyArtifactFileSha256": publication["strategyArtifactFileSha256"],
            "strategyInstanceId": publication["strategyInstanceId"],
            "strategyInstanceHash": publication["strategyInstanceHash"],
            "strategyInstanceFileSha256": publication["strategyInstanceFileSha256"],
            "strategyInstanceArtifactHash": publication["strategyArtifactHash"],
            "accountFingerprint": ACCOUNT,
            "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
            "symbol": "KRW-BTC",
            "interval": "5m",
            "verified": True,
            "publicationProofHash": proof["proofHash"],
            "publicationProofFileSha256": hashlib.sha256(proof_bytes).hexdigest(),
            "publicationProofVerified": True,
            "publishedProvider": publication["provider"],
            "publishedGroup": publication["group"],
            "publishedSymbol": publication["symbol"],
            "publishedStrategyArtifactHash": publication["strategyArtifactHash"],
            "publishedStrategyArtifactFileSha256": publication["strategyArtifactFileSha256"],
            "publishedStrategyInstanceHash": publication["strategyInstanceHash"],
            "publishedStrategyInstanceFileSha256": publication["strategyInstanceFileSha256"],
            "publishedActiveCatalogVisible": publication["activeCatalogVisible"],
            "publishedNaturalSignalsOnly": proof["naturalSignalsOnly"],
            "publishedPromotionEligible": proof["promotionEligible"],
        }
        scope = subject.UpbitPermitScope.parse(actual_permit, immutable_selection=selection)
        self.assertEqual(publication["strategyArtifactHash"], scope.strategy_artifact_hash)
        self.assertEqual(publication["strategyArtifactFileSha256"], scope.strategy_artifact_file_sha256)
        self.assertEqual(publication["strategyInstanceHash"], scope.strategy_instance_hash)
        self.assertEqual(publication["strategyInstanceFileSha256"], scope.strategy_instance_file_sha256)

    def test_truth_requires_all_official_complete_scopes_fees_and_rules(self) -> None:
        payload = self.fake.truth(session_id=self.fake.session_id, phase="BASELINE")
        for field in (
            "openOrdersComplete",
            "closedOrdersComplete",
            "fillsComplete",
            "feesComplete",
            "orderChanceComplete",
            "tickerComplete",
        ):
            with self.subTest(field=field):
                broken = dict(payload)
                broken[field] = False
                with self.assertRaises(subject.UpbitFunctionalBlocked):
                    subject.UpbitTruth.parse(broken, account_fingerprint=ACCOUNT, now=self.fake.now)

        broken = dict(payload)
        broken["orderRules"] = {"bidMinTotal": "5000"}
        with self.assertRaises(subject.UpbitFunctionalBlocked):
            subject.UpbitTruth.parse(broken, account_fingerprint=ACCOUNT, now=self.fake.now)

    def test_only_fresh_finalized_official_five_minute_bar_is_accepted(self) -> None:
        subject.FinalizedFiveMinuteBar.parse(
            self.bar("HOLD"),
            now=self.fake.now,
            strategy_artifact_id="crypto-btc-finalized-5m",
            strategy_artifact_hash="a" * 64,
            strategy_artifact_file_sha256="f" * 64,
            strategy_instance_id="crypto-btc-finalized-5m-instance",
            strategy_instance_hash="d" * 64,
            strategy_instance_file_sha256="9" * 64,
            publication_proof_hash="1" * 64,
            publication_proof_file_sha256="2" * 64,
        )
        for updates in (
            {"finalized": False},
            {"interval": "1m"},
            {"source": "UNVERIFIED"},
            {"closedAt": (NOW - timedelta(minutes=11)).isoformat().replace("+00:00", "Z")},
        ):
            with self.subTest(updates=updates):
                payload = {**self.bar("HOLD"), **updates}
                with self.assertRaises(subject.UpbitFunctionalBlocked):
                    subject.FinalizedFiveMinuteBar.parse(
                        payload,
                        now=self.fake.now,
                        strategy_artifact_id="crypto-btc-finalized-5m",
                        strategy_artifact_hash="a" * 64,
                        strategy_artifact_file_sha256="f" * 64,
                        strategy_instance_id="crypto-btc-finalized-5m-instance",
                        strategy_instance_hash="d" * 64,
                        strategy_instance_file_sha256="9" * 64,
                        publication_proof_hash="1" * 64,
                        publication_proof_file_sha256="2" * 64,
                    )

        for updates in (
            {"naturalSignal": False},
            {"forcedSignal": True},
            {"manualSignal": True},
            {"strategyArtifactHash": "e" * 64},
            {"strategyInstanceHash": "e" * 64},
            {"evaluationId": ""},
        ):
            with self.subTest(provenance=updates), self.assertRaisesRegex(
                subject.UpbitFunctionalBlocked,
                "provenance-invalid|evaluation-id-invalid",
            ):
                subject.FinalizedFiveMinuteBar.parse(
                    {**self.bar("BUY"), **updates},
                    now=self.fake.now,
                    strategy_artifact_id="crypto-btc-finalized-5m",
                    strategy_artifact_hash="a" * 64,
                    strategy_artifact_file_sha256="f" * 64,
                    strategy_instance_id="crypto-btc-finalized-5m-instance",
                    strategy_instance_hash="d" * 64,
                    strategy_instance_file_sha256="9" * 64,
                    publication_proof_hash="1" * 64,
                    publication_proof_file_sha256="2" * 64,
                )

    def test_strategy_bar_close_must_be_inside_active_permit_window(self) -> None:
        service = self.activate()
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "outside-active-permit-window",
        ):
            service.on_bar(
                self.bar(
                    "HOLD",
                    closed_at=ACTIVATED_AT - timedelta(minutes=5),
                    observed_at=ACTIVATED_AT,
                )
            )
        self.assertEqual(
            "HOLD",
            service.on_bar(
                self.bar(
                    "HOLD",
                    closed_at=ACTIVATED_AT,
                    observed_at=ACTIVATED_AT,
                )
            )["action"],
        )
        self.fake.now = self.permit.ends_at - timedelta(minutes=5)
        self.assertEqual(
            "HOLD",
            service.on_bar(
                self.bar(
                    "HOLD",
                    closed_at=self.fake.now,
                    observed_at=self.fake.now,
                )
            )["action"],
        )
        self.fake.now = self.permit.ends_at
        result = service.on_bar(
            self.bar(
                "BUY",
                closed_at=self.fake.now,
                observed_at=self.fake.now,
            )
        )
        self.assertEqual("CLEANUP", result["action"])
        self.assertEqual([], self.ledger.claims(self.fake.session_id))

    def test_buy_once_sell_once_no_reentry_and_final_baseline_flat(self) -> None:
        service = self.activate()
        baseline = self.fake.base
        buy = service.on_bar(self.bar("BUY"))
        self.assertEqual("STRATEGY_BUY", buy["action"])
        sell = service.on_bar(self.bar("SELL", bar_id="upbit-five-minute-bar-0002"))
        self.assertEqual("STRATEGY_SELL", sell["action"])
        third = service.on_bar(
            self.bar("BUY", bar_id="upbit-five-minute-bar-0003")
        )
        self.fake.now += timedelta(minutes=5)
        fourth = service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0004")
        )
        self.assertEqual("HOLD", third["action"])
        self.assertEqual("NO_REENTRY_STRATEGY_SLOT_CONSUMED", third["reason"])
        self.assertEqual("HOLD", fourth["action"])
        self.assertEqual("NO_REENTRY_STRATEGY_SLOT_CONSUMED", fourth["reason"])
        self.assertEqual(2, self.fake.post_calls)
        self.assertEqual(2, len(self.ledger.claims(self.fake.session_id)))
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(hours=2)
        result = service.finalize_if_flat()
        self.assertEqual("FINALIZED", result["state"])
        self.assertEqual(baseline, self.fake.base)
        self.assertFalse(result["evidence"]["promotionEligible"])
        self.assertEqual("PASS", result["testOutcome"])
        self.assertTrue(result["evidence"]["functionalTestPassed"])
        self.assertTrue(result["evidence"]["functionalWiringPassed"])
        self.assertTrue(result["evidence"]["strategyOrderCountExact"])
        self.assertTrue(result["evidence"]["noReentryVerified"])
        self.assertTrue(
            result["evidence"]["strategyNotionalCapSatisfied"]
        )
        self.assertTrue(
            result["evidence"]["strategyGrossExposureCapSatisfied"]
        )
        self.assertTrue(
            result["evidence"]["exactTwoHourRuntimeComplete"]
        )
        self.assertTrue(
            result["evidence"]["accountExclusivityProofVerified"]
        )
        self.assertTrue(
            result["evidence"]["accountExclusivityAuthorityPinned"]
        )
        self.assertTrue(result["evidence"]["otherApiKeysAbsent"])
        self.assertTrue(result["evidence"]["manualTradingAbsent"])
        self.assertTrue(result["evidence"]["otherBotsAbsent"])
        self.assertEqual(
            _stable_hash(result["evidence"]["accountExclusivityProof"]),
            result["evidence"]["accountExclusivityProofHash"],
        )
        self.assertEqual("7200", result["evidence"]["actualDurationSeconds"])
        durable = self.ledger.session(self.fake.session_id)
        self.assertEqual(1, durable["new_entries_blocked"])
        self.assertEqual(0, durable["real_orders_enabled"])
        self.assertEqual("", durable["capability_hash"])
        self.assertEqual("", self.fake.capability_hash)

    def assert_exclusivity_safe_incomplete(
        self, result: dict[str, object]
    ) -> None:
        evidence = result["evidence"]
        self.assertEqual("SAFE_INCOMPLETE", result["testOutcome"])
        self.assertFalse(evidence["functionalTestPassed"])
        self.assertFalse(evidence["promotionEligible"])
        self.assertFalse(evidence["accountExclusivityProofVerified"])
        self.assertFalse(evidence["accountExclusivityAuthorityPinned"])
        self.assertFalse(evidence["otherApiKeysAbsent"])
        self.assertFalse(evidence["manualTradingAbsent"])
        self.assertFalse(evidence["otherBotsAbsent"])
        self.assertFalse(evidence["exclusiveAccountCausalProofComplete"])
        self.assertFalse(evidence["realOrdersEnabled"])
        self.assertFalse(evidence["functionalMutationEnabled"])

    def test_missing_exclusivity_primitives_never_pass_after_exact_roundtrip(
        self,
    ) -> None:
        self.fake.include_account_exclusivity_proof = False
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_unpinned_exclusivity_authority_never_passes_exact_roundtrip(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate(account_authority=False)
        self.assertEqual(0, self.fake.post_calls)

    def test_false_other_api_key_primitive_never_passes(self) -> None:
        def false_api_key(proof):
            proof = copy.deepcopy(proof)
            proof["apiKeyInventory"]["activeApiKeyCount"] = 2
            proof["apiKeyInventory"]["otherActiveApiKeyCount"] = 1
            return resign_test_account_exclusivity_proof(proof)

        self.fake.account_exclusivity_proof_mutator = false_api_key
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_false_manual_trade_primitive_never_passes(self) -> None:
        def false_manual_trade(proof):
            proof = copy.deepcopy(proof)
            proof["manualTradeAudit"]["manualOrderCount"] = 1
            return resign_test_account_exclusivity_proof(proof)

        self.fake.account_exclusivity_proof_mutator = false_manual_trade
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_false_other_bot_primitive_never_passes(self) -> None:
        def false_other_bot(proof):
            proof = copy.deepcopy(proof)
            proof["botRegistry"]["activeBotCount"] = 2
            proof["botRegistry"]["otherActiveBotCount"] = 1
            return resign_test_account_exclusivity_proof(proof)

        self.fake.account_exclusivity_proof_mutator = false_other_bot
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_tampered_exclusivity_primitive_never_passes(self) -> None:
        def tampered(proof):
            proof = copy.deepcopy(proof)
            proof["manualTradeAudit"]["manualOrderCount"] = 1
            return proof

        self.fake.account_exclusivity_proof_mutator = tampered
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_caller_chosen_exclusivity_source_is_rejected_even_if_resigned(
        self,
    ) -> None:
        def caller_source(proof):
            proof = copy.deepcopy(proof)
            proof["botRegistry"]["source"] = "CALLER_SAYS_NO_OTHER_BOTS"
            return resign_test_account_exclusivity_proof(proof)

        self.fake.account_exclusivity_proof_mutator = caller_source
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_natural_buy_missing_proof_at_predispatch_latches_cleanup_socket_zero(
        self,
    ) -> None:
        service = self.activate()
        self.fake.exclusivity_proof_missing_phases.add("PRE_DISPATCH")
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "natural-buy-account-exclusivity-proof-required",
        ):
            service.on_bar(self.bar("BUY"))
        self.assertEqual(0, self.fake.post_calls)
        self.assertEqual([], self.ledger.claims(self.fake.session_id))
        durable = self.ledger.session(self.fake.session_id)
        self.assertEqual("CLEANUP", durable["state"])
        self.assertEqual(1, durable["account_exclusivity_breach"])

    def test_natural_buy_loses_proof_at_final_prepost_latches_cleanup_socket_zero(
        self,
    ) -> None:
        service = self.activate()
        self.fake.exclusivity_proof_missing_phases.add("FINAL_PRE_POST")
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "natural-buy-account-exclusivity-proof-required",
        ):
            service.on_bar(self.bar("BUY"))
        self.assertEqual(0, self.fake.post_calls)
        self.assertEqual(
            "BLOCKED_BEFORE_POST",
            self.ledger.claims(self.fake.session_id)[0]["state"],
        )
        durable = self.ledger.session(self.fake.session_id)
        self.assertEqual("CLEANUP", durable["state"])
        self.assertEqual(1, durable["account_exclusivity_breach"])

    def test_global_fence_loss_at_final_prepost_latches_cleanup_socket_zero(
        self,
    ) -> None:
        def authority_reader(request):
            entry_open = request["action"] == "ACTIVATE"
            body = {
                "schemaVersion": (
                    subject.GLOBAL_FIRST_LIVE_AUTHORITY_SCHEMA_VERSION
                ),
                "scope": subject.GLOBAL_FIRST_LIVE_SCOPE,
                "lane": "UPBIT",
                "phase": "ACTIVE",
                "runId": request["runId"],
                "sessionId": request["sessionId"],
                "permitId": request["permitId"],
                "permitHash": request["permitHash"],
                "accountFingerprint": request["accountFingerprint"],
                "routeScopeHash": request["routeScopeHash"],
                "ownerIdentityHash": request["ownerIdentityHash"],
                "ownerLeaseActive": True,
                "entryAuthorityOpen": entry_open,
                "cleanupAuthorityOpen": False,
                "hardStopEpoch": self.permit.ends_at.timestamp(),
                "ownerLeaseExpiresEpoch": self.fake.now.timestamp() + 30,
                "revision": 2,
                "observedEpoch": self.fake.now.timestamp(),
                "killSwitch": False,
                "stopRequested": False,
            }
            return {
                **body,
                "authorityHash": subject._strict_stable_hash(body),
            }

        service = self.activate(
            global_authority_reader=authority_reader,
            global_owner_identity_hash=GLOBAL_OWNER,
        )
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "global-first-live-dispatch-fence-closed",
        ):
            service.on_bar(self.bar("BUY"))
        self.assertEqual(0, self.fake.post_calls)
        self.assertEqual(
            "BLOCKED_BEFORE_POST",
            self.ledger.claims(self.fake.session_id)[0]["state"],
        )
        self.assertEqual(
            "CLEANUP",
            self.ledger.session(self.fake.session_id)["state"],
        )

    def test_cleanup_sell_remains_allowed_after_exclusivity_proof_loss(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.recover_or_expire(reason="operator-stop")
        self.fake.include_account_exclusivity_proof = False
        action = service.cleanup_plan()["actions"][0]
        self.assertEqual("CLEANUP_SELL", action.slot)
        result = service.dispatch(action)
        self.assertEqual("CLEANUP_SELL", result["action"])
        self.assertEqual(2, self.fake.post_calls)

    def test_exotic_stringlike_proof_scalar_is_rejected_before_activation(self) -> None:
        class StringLike:
            def __str__(self) -> str:
                return ACCOUNT

        def exotic(proof):
            proof = copy.deepcopy(proof)
            proof["accountFingerprint"] = StringLike()
            return resign_test_account_exclusivity_proof(proof)

        self.fake.account_exclusivity_proof_mutator = exotic
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_nonlowercase_component_hash_is_rejected_before_activation(self) -> None:
        def uppercase_hash(proof):
            proof = copy.deepcopy(proof)
            proof["apiKeyInventory"]["authorityArtifactHash"] = "A" * 64
            return resign_test_account_exclusivity_proof(proof)

        self.fake.account_exclusivity_proof_mutator = uppercase_hash
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "activation-account-exclusivity-proof-required",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

    def test_final_result_rejects_pass_summary_when_primitive_proof_is_missing(
        self,
    ) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
        )
        service.recover_or_expire(reason="operator-stop")
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(seconds=7200)
        result = service.finalize_if_flat()
        tampered = copy.deepcopy(result["evidence"])
        tampered.pop("accountExclusivityProof")
        tampered["functionalWiringPassed"] = True
        tampered["functionalTestPassed"] = True
        finalized = dict(self.ledger.session(self.fake.session_id))
        finalized["final_evidence_hash"] = _stable_hash(tampered)
        finalized["final_evidence_json"] = json.dumps(
            tampered,
            sort_keys=True,
            separators=(",", ":"),
        )
        direct = service._final_result(tampered, finalized)
        self.assertEqual("SAFE_INCOMPLETE", direct["testOutcome"])
        self.assertFalse(direct["evidence"]["functionalWiringPassed"])
        self.assertFalse(direct["evidence"]["functionalTestPassed"])
        self.assertFalse(direct["evidence"]["promotionEligible"])

    def test_final_result_rejects_uppercase_proof_or_durable_hash(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
        )
        service.recover_or_expire(reason="operator-stop")
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(seconds=7200)
        result = service.finalize_if_flat()
        for field in ("proof", "durable"):
            with self.subTest(field=field):
                evidence = copy.deepcopy(result["evidence"])
                finalized = dict(self.ledger.session(self.fake.session_id))
                if field == "proof":
                    evidence["accountExclusivityProofHash"] = str(
                        evidence["accountExclusivityProofHash"]
                    ).upper()
                    finalized["final_evidence_hash"] = _stable_hash(evidence)
                    finalized["final_evidence_json"] = json.dumps(
                        evidence,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                else:
                    finalized["final_evidence_hash"] = str(
                        finalized["final_evidence_hash"]
                    ).upper()
                direct = service._final_result(evidence, finalized)
                self.assertEqual("SAFE_INCOMPLETE", direct["testOutcome"])
                self.assertFalse(direct["evidence"]["functionalTestPassed"])

    def test_full_roundtrip_before_7200_seconds_is_safe_incomplete(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
        )
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(
            seconds=7199, microseconds=999000
        )
        result = service.finalize_if_flat()
        self.assertEqual("SAFE_INCOMPLETE", result["testOutcome"])
        self.assertFalse(result["evidence"]["functionalTestPassed"])
        self.assertFalse(result["evidence"]["functionalWiringPassed"])
        self.assertFalse(
            result["evidence"]["exactTwoHourRuntimeComplete"]
        )
        self.assertEqual(
            "7199.999", result["evidence"]["actualDurationSeconds"]
        )

    def test_wall_clock_forward_jump_without_monotonic_elapsed_never_passes(
        self,
    ) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
        )
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(hours=2)
        service.monotonic_clock = lambda: 1.0
        result = service.finalize_if_flat()
        self.assertEqual("SAFE_INCOMPLETE", result["testOutcome"])
        self.assertFalse(result["evidence"]["clockDiscontinuityAbsent"])
        self.assertFalse(result["evidence"]["functionalTestPassed"])
        self.assertFalse(result["evidence"]["functionalWiringPassed"])

    def test_complete_wiring_with_account_causal_gap_is_sealed_safe_incomplete(
        self,
    ) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
        )
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(hours=2)
        original_truth = self.fake.truth

        def account_changed_truth(**kwargs):
            value = original_truth(**kwargs)
            if kwargs.get("phase") == "FINAL":
                rows = [
                    *[dict(row) for row in value["accountRows"]],
                    {"currency": "ETH", "available": "1", "locked": "0"},
                ]
                rows.sort(key=lambda row: row["currency"])
                value["accountRows"] = rows
                value["accountRowsHash"] = _stable_hash(rows)
            return value

        service.truth_reader = account_changed_truth
        result = service.finalize_if_flat()
        self.assertEqual(
            "SAFE_INCOMPLETE_CAUSAL_UNPROVEN", result["testOutcome"]
        )
        self.assertTrue(result["evidence"]["functionalWiringPassed"])
        self.assertFalse(result["evidence"]["functionalTestPassed"])
        self.assertFalse(
            result["evidence"]["exclusiveAccountCausalProofComplete"]
        )

    def test_final_reset_pending_is_restartable_after_runtime_clear_crash(self) -> None:
        service = self.activate()
        service.recover_or_expire(reason="operator-stop")
        self.disarm_for_final()
        original = self.ledger.complete_final_reset
        calls = 0

        def crash_once(session_id: str, *, evidence_hash: str):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("simulated-crash-before-final-seal")
            return original(session_id, evidence_hash=evidence_hash)

        self.ledger.complete_final_reset = crash_once  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "simulated-crash"):
            service.finalize_if_flat()
        pending = self.ledger.session(self.fake.session_id)
        self.assertEqual("FINAL_RESET_PENDING", pending["state"])
        self.assertEqual("", pending["capability_hash"])
        self.assertEqual("", self.fake.capability_hash)
        resumed = service.resume_final_reset()
        self.assertEqual("FINALIZED", resumed["state"])
        again = service.resume_final_reset()
        self.assertEqual(resumed["evidenceHash"], again["evidenceHash"])

    def test_terminal_buy_sell_above_owner_loss_limit_is_not_pass(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.on_bar(self.bar("SELL", bar_id="upbit-five-minute-bar-0002"))
        # Preserve official terminal/fill truth while making the final
        # owner-only P&L exceed the sealed 1,000 KRW loss cap.
        self.fake.fills[-1]["funds"] = "8000"
        service.recover_or_expire(reason="operator-stop")
        self.disarm_for_final()
        final = service.finalize_if_flat()
        self.assertEqual("SAFE_INCOMPLETE", final["testOutcome"])
        self.assertFalse(final["evidence"]["ownerLossLimitSatisfied"])
        self.assertFalse(final["evidence"]["functionalTestPassed"])

    def test_profitable_roundtrip_clamps_owner_loss_to_zero(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"), buy_notional=9000)
        # Keep owner gross below the 10,000 KRW cap while producing proceeds
        # above cost plus both official fees.
        self.fake.mark = Decimal("101000000")
        service.on_bar(
            self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
        )
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        self.disarm_for_final()
        self.fake.now = ACTIVATED_AT + timedelta(hours=2)
        final = service.finalize_if_flat()
        self.assertEqual("0", final["evidence"]["ownerLoss"])
        self.assertTrue(final["evidence"]["ownerLossLimitSatisfied"])
        self.assertTrue(final["evidence"]["functionalWiringPassed"])

    def test_global_flag_or_forged_runtime_never_reaches_post_or_claim(self) -> None:
        service = self.activate()
        self.fake.real_orders = False
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "global-flag-off"):
            service.on_bar(self.bar("BUY"))
        self.assertEqual([], self.ledger.claims(self.fake.session_id))
        self.fake.real_orders = True
        self.fake.runtime_updates["executionPurpose"] = "SMALL_LIVE"
        self.fake.now += timedelta(minutes=5)
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "executionPurpose"):
            service.on_bar(self.bar("BUY", bar_id="upbit-five-minute-bar-0002"))
        self.assertEqual(0, self.fake.post_calls)

    def test_capability_hash_is_durable_but_secret_is_not_and_forgery_cannot_start(self) -> None:
        service = self.activate()
        durable = self.ledger.session(self.fake.session_id)
        self.assertEqual(self.fake.capability_hash, durable["capability_hash"])
        self.assertEqual(64, len(durable["capability_hash"]))
        self.assertNotIn("capability_secret", durable)
        self.assertIsNotNone(service)

        other_temp = tempfile.TemporaryDirectory()
        self.addCleanup(other_temp.cleanup)
        other_ledger = subject.UpbitFunctionalLedger(
            Path(other_temp.name) / "forged.sqlite3"
        )
        other = FakeBoundaries(self.permit)
        other.register_wrong_capability = True
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "functionalCapabilityHash",
        ):
            subject._activate_for_test(
                permit=self.permit,
                ledger=other_ledger,
                session_id=other.session_id,
                truth_reader=other.truth,
                post_order=other.post,
                cancel_order=other.cancel,
                lease_factory=other.lease,
                runtime_reader=other.runtime,
                immutable_selection_reader=other.immutable_selection,
                runtime_capability_registrar=other.register_capability,
                real_orders_reader=lambda: other.real_orders,
                clock=other.clock,
                account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
                account_exclusivity_verifier_pin=TEST_EXCLUSIVITY_VERIFIER_PIN,
            )
        self.assertEqual(0, other.post_calls)
        with self.assertRaises(subject.UpbitFunctionalBlocked):
            other_ledger.session(other.session_id)

    def test_instance_file_replacement_blocks_prestart_and_prepost(self) -> None:
        self.fake.selection_updates["strategyInstanceFileSha256"] = "e" * 64
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "publication-proof.*InstanceFileSha256|instance-file-sha256",
        ):
            self.activate()
        self.assertEqual(0, self.fake.post_calls)

        self.fake.selection_updates = {}
        service = self.activate()
        self.fake.selection_updates["strategyInstanceFileSha256"] = "e" * 64
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "current-selection-strategyInstanceFileSha256",
        ):
            service.on_bar(self.bar("BUY"))
        self.assertEqual(0, self.fake.post_calls)
        self.assertEqual([], self.ledger.claims(self.fake.session_id))

    def test_sell_signal_without_owned_position_is_hold(self) -> None:
        service = self.activate()
        result = service.on_bar(self.bar("SELL"))
        self.assertEqual("HOLD", result["action"])
        self.assertEqual(0, self.fake.post_calls)

    def test_finalized_bar_is_durably_once_and_strictly_ordered(self) -> None:
        service = self.activate()
        self.fake.now += timedelta(minutes=5)
        first = self.bar(
            "HOLD",
            bar_id="upbit-five-minute-bar-0002",
            closed_at=self.fake.now,
            observed_at=self.fake.now,
        )
        self.assertEqual("HOLD", service.on_bar(first)["action"])
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "already-consumed"):
            service.on_bar(first)
        older_new_id = self.bar(
            "HOLD",
            closed_at=ACTIVATED_AT,
            observed_at=self.fake.now,
        )
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "not-strictly-newer"):
            service.on_bar(older_new_id)

    def test_restart_reattaches_cleanup_only_with_fresh_owner_attestation(self) -> None:
        original = self.activate()
        original.on_bar(self.bar("BUY"))
        old_capability_hash = self.fake.capability_hash
        recovery = {
            "schemaVersion": "upbit-functional-recovery-approval/v1",
            "recoveryId": "upbit-recovery-approval-0001",
            "mode": "CLEANUP_ONLY",
            "sessionId": self.fake.session_id,
            "permitId": self.permit.permit_id,
            "permitHash": self.permit.content_hash,
            "accountFingerprint": ACCOUNT,
            "approvalState": "ACTIVE",
            "serverManaged": True,
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "singleUse": True,
            "previousOwnerLost": True,
            "previousOwnerLeaseExpired": True,
            "officialRestReconciled": True,
            "observedAt": self.fake.now.isoformat().replace("+00:00", "Z"),
        }
        recovery["contentHash"] = subject._stable_hash(recovery)
        recovered = subject.UpbitContinuousFunctionalService.reattach_cleanup_after_owner_loss(
            permit=self.permit,
            ledger=self.ledger,
            session_id=self.fake.session_id,
            owner_recovery_attestation=recovery,
            truth_reader=self.fake.truth,
            post_order=self.fake.post,
            cancel_order=self.fake.cancel,
            lease_factory=self.fake.lease,
            runtime_reader=self.fake.runtime,
            immutable_selection_reader=self.fake.immutable_selection,
            runtime_capability_registrar=self.fake.register_capability,
            real_orders_reader=lambda: self.fake.real_orders,
            clock=self.fake.clock,
            _capability=subject._TEST_CAPABILITY,
        )
        self.assertNotEqual(old_capability_hash, self.fake.capability_hash)
        self.assertEqual("CLEANUP", self.ledger.session(self.fake.session_id)["state"])
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "capability-invalid"):
            original.dispatch(subject.UpbitLeg.buy(10_000))
        plan = recovered.cleanup_plan()
        self.assertEqual("CLEANUP_SELL", plan["actions"][0].slot)
        recovered.dispatch(plan["actions"][0])
        self.disarm_for_final()
        self.assertEqual("FINALIZED", recovered.finalize_if_flat()["state"])

        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked, "already-consumed"
        ):
            subject.UpbitContinuousFunctionalService.reattach_cleanup_after_owner_loss(
                permit=self.permit,
                ledger=self.ledger,
                session_id=self.fake.session_id,
                owner_recovery_attestation=recovery,
                truth_reader=self.fake.truth,
                post_order=self.fake.post,
                cancel_order=self.fake.cancel,
                lease_factory=self.fake.lease,
                runtime_reader=self.fake.runtime,
                immutable_selection_reader=self.fake.immutable_selection,
                runtime_capability_registrar=self.fake.register_capability,
                real_orders_reader=lambda: self.fake.real_orders,
                clock=self.fake.clock,
                _capability=subject._TEST_CAPABILITY,
            )

    def test_durable_prepost_claim_blocks_blind_retry_after_ambiguous_post(self) -> None:
        service = self.activate()
        self.fake.post_error = TimeoutError("unknown outcome")
        with self.assertRaises(subject.UpbitFunctionalAmbiguous):
            service.on_bar(self.bar("BUY"))
        claims = self.ledger.claims(self.fake.session_id)
        self.assertEqual("AMBIGUOUS", claims[0]["state"])
        self.assertEqual("CLEANUP", self.ledger.session(self.fake.session_id)["state"])
        with self.assertRaises(subject.UpbitFunctionalBlocked):
            service.dispatch(subject.UpbitLeg.buy(10_000))
        self.assertEqual(1, self.fake.post_calls)

    def test_cleanup_rejects_nonowned_working_order_without_cancel_or_sell(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        self.fake.open_orders.append(
            {
                "market": "KRW-ETH",
                "uuid": "external-working-order-0001",
                "identifier": "external-unowned-order-0001",
                "side": "BID",
                "state": "wait",
            }
        )
        service.recover_or_expire(reason="kill-switch")
        self.enter_cleanup_runtime()
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "nonowned-working-order-present",
        ):
            service.cleanup_plan()
        self.assertEqual(0, self.fake.cancel_calls)
        self.assertEqual(1, self.fake.post_calls)

    def test_ambiguous_timeout_is_reconciled_by_exact_truth_never_retried(self) -> None:
        service = self.activate()
        self.fake.post_error = TimeoutError("unknown outcome")
        with self.assertRaises(subject.UpbitFunctionalAmbiguous):
            service.on_bar(self.bar("BUY"))
        claim = self.ledger.claims(self.fake.session_id)[0]
        self.assertEqual("AMBIGUOUS", claim["state"])

        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "absence-not-terminal-proof",
        ):
            service.cleanup_plan()
        self.assertEqual(
            "AMBIGUOUS", self.ledger.claims(self.fake.session_id)[0]["state"]
        )
        self.assertEqual(1, self.fake.post_calls)
        self.disarm_for_final()
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked,
            "unresolved-claims",
        ):
            service.finalize_if_flat()

    def test_ambiguous_absence_needs_three_fresh_observations_over_horizon(self) -> None:
        service = self.activate()
        self.fake.post_error = TimeoutError("unknown outcome")
        with self.assertRaises(subject.UpbitFunctionalAmbiguous):
            service.on_bar(self.bar("BUY"))
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked, "absence-not-terminal-proof"
        ):
            service.cleanup_plan()
        self.fake.now += timedelta(seconds=15)
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked, "absence-not-terminal-proof"
        ):
            service.cleanup_plan()
        self.fake.now += timedelta(seconds=16)
        plan = service.cleanup_plan()
        self.assertTrue(plan["readyToFinalize"])
        claim = self.ledger.claims(self.fake.session_id)[0]
        self.assertEqual("AMBIGUOUS_PROVEN_NOT_ACCEPTED", claim["state"])
        self.assertEqual(3, claim["absence_observations"])
        self.assertEqual(1, self.fake.post_calls)
        self.disarm_for_final()
        final = service.finalize_if_flat()
        self.assertEqual("SAFE_INCOMPLETE", final["testOutcome"])
        self.assertFalse(final["evidence"]["functionalTestPassed"])

    def test_ambiguous_accepted_buy_is_found_then_owned_delta_is_flattened(self) -> None:
        service = self.activate()
        self.fake.post_error = TimeoutError("response lost")
        with self.assertRaises(subject.UpbitFunctionalAmbiguous):
            service.on_bar(self.bar("BUY"))
        claim = self.ledger.claims(self.fake.session_id)[0]
        volume = Decimal("0.0001")
        funds = Decimal("10000")
        fee = Decimal("5")
        self.fake.base += volume
        self.fake.quote -= funds + fee
        self.fake.fills.append(
            {
                "market": "KRW-BTC",
                "tradeUuid": "trade-late-truth-0001",
                "orderUuid": "broker-late-truth-0001",
                "identifier": claim["identifier"],
                "side": "BID",
                "volume": str(volume),
                "funds": str(funds),
                "fee": str(fee),
            }
        )
        self.fake.closed_orders.append(
            {
                "market": "KRW-BTC",
                "uuid": "broker-late-truth-0001",
                "identifier": claim["identifier"],
                "side": "BID",
                "state": "done",
            }
        )
        self.fake.post_error = None
        plan = service.cleanup_plan()
        self.assertEqual("RECONCILED", self.ledger.claims(self.fake.session_id)[0]["state"])
        self.assertEqual("CLEANUP_SELL", plan["actions"][0].slot)
        service.dispatch(plan["actions"][0])
        self.assertEqual(Decimal("0.01000000"), self.fake.base)
        self.disarm_for_final()
        final = service.finalize_if_flat()
        self.assertEqual("SAFE_INCOMPLETE", final["testOutcome"])
        self.assertTrue(final["evidence"]["cleanupFlattenUsed"])

    def test_final_lease_change_is_durable_block_before_post(self) -> None:
        service = self.activate()
        self.fake.lease_updates["active"] = False
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "lease-invalid"):
            service.on_bar(self.bar("BUY"))
        self.assertEqual(0, self.fake.post_calls)
        self.assertEqual("BLOCKED_BEFORE_POST", self.ledger.claims(self.fake.session_id)[0]["state"])

    def test_final_truth_failure_and_identifier_collision_are_proved_not_sent(self) -> None:
        service = self.activate()
        self.fake.truth_errors["FINAL_PRE_POST"] = RuntimeError("read failed")
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "not-sent"):
            service.on_bar(self.bar("BUY"))
        self.assertEqual(0, self.fake.post_calls)
        self.assertEqual(
            "BLOCKED_BEFORE_POST",
            self.ledger.claims(self.fake.session_id)[0]["state"],
        )

        other_temp = tempfile.TemporaryDirectory()
        self.addCleanup(other_temp.cleanup)
        other = FakeBoundaries(self.permit)
        other.preused_identifier = True
        other_ledger = subject.UpbitFunctionalLedger(
            Path(other_temp.name) / "identifier.sqlite3"
        )
        collision_service = subject._activate_for_test(
            permit=self.permit,
            ledger=other_ledger,
            session_id=other.session_id,
            truth_reader=other.truth,
            post_order=other.post,
            cancel_order=other.cancel,
            lease_factory=other.lease,
            runtime_reader=other.runtime,
            immutable_selection_reader=other.immutable_selection,
            runtime_capability_registrar=other.register_capability,
            real_orders_reader=lambda: other.real_orders,
            clock=other.clock,
            account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
            account_exclusivity_verifier_pin=TEST_EXCLUSIVITY_VERIFIER_PIN,
        )
        other.real_orders = True
        other.runtime_updates["realOrdersEnabled"] = True
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "identifier-already-used"):
            collision_service.on_bar(self.bar("BUY"))
        self.assertEqual(0, other.post_calls)
        self.assertEqual(
            "BLOCKED_BEFORE_POST",
            other_ledger.claims(other.session_id)[0]["state"],
        )

    def test_adapter_can_prove_post_not_sent_without_ambiguous_cleanup(self) -> None:
        service = self.activate()
        self.fake.post_error = subject.UpbitBrokerPostNotSent("socket-not-opened")
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "proved-post-not-sent"):
            service.on_bar(self.bar("BUY"))
        self.assertEqual(1, self.fake.post_calls)
        self.assertEqual(
            "BLOCKED_BEFORE_POST",
            self.ledger.claims(self.fake.session_id)[0]["state"],
        )
        self.assertEqual("ACTIVE", self.ledger.session(self.fake.session_id)["state"])
        first_claim = self.ledger.claims(self.fake.session_id)[0]
        self.fake.post_error = None
        self.fake.now += timedelta(minutes=5)
        retry = service.on_bar(
            self.bar("BUY", bar_id="upbit-five-minute-bar-0002")
        )
        retried_claim = self.ledger.claims(self.fake.session_id)[0]
        self.assertEqual("STRATEGY_BUY", retry["action"])
        self.assertEqual(first_claim["claim_id"], retried_claim["claim_id"])
        self.assertEqual(1, retried_claim["proven_not_sent_retries"])
        self.assertEqual(1, len(self.ledger.claims(self.fake.session_id)))
        self.assertEqual(2, self.fake.post_calls)

    def test_proven_not_sent_cleanup_sell_reuses_exact_claim_once(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        leg = service.cleanup_plan()["actions"][0]
        self.fake.post_error = subject.UpbitBrokerPostNotSent("not-opened")
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "proved-post-not-sent"):
            service.dispatch(leg)
        first = self.ledger.claims(self.fake.session_id)[-1]
        self.assertEqual("BLOCKED_BEFORE_POST", first["state"])
        self.fake.post_error = None
        retry = service.cleanup_plan()["actions"][0]
        result = service.dispatch(retry)
        second = self.ledger.claims(self.fake.session_id)[-1]
        self.assertEqual(first["claim_id"], second["claim_id"])
        self.assertEqual(1, second["proven_not_sent_retries"])
        self.assertEqual("CLEANUP_SELL", result["action"])
        self.assertEqual(Decimal("0.01000000"), self.fake.base)

    def test_post_boundary_marker_survives_hard_crash_and_never_retries(self) -> None:
        service = self.activate()
        original_marker = self.ledger.mark_post_may_have_crossed

        def crash_after_marker(claim_id: str, request_hash: str):
            result = original_marker(claim_id, request_hash)
            raise KeyboardInterrupt("simulated-process-death")

        # The production edge owns this callback.  Simulate its durable CAS
        # followed by process death before any response can be observed.
        claim_payload = {
            "market": "KRW-BTC",
            "side": "bid",
            "ord_type": "price",
            "price": "10000",
        }
        natural_buy = self.sealed_bar("BUY")
        self.ledger.note_bar(self.fake.session_id, natural_buy)
        claim = self.ledger.claim(
            self.fake.session_id,
            subject.UpbitLeg.buy(10_000),
            claim_payload,
            functional_capability_verified=True,
            natural_evaluation=natural_buy,
        )
        with self.assertRaises(KeyboardInterrupt):
            crash_after_marker(claim["claimId"], claim["requestHash"])
        durable = self.ledger.claims(self.fake.session_id)[0]
        self.assertEqual("POST_MAY_HAVE_CROSSED", durable["state"])
        self.ledger.enter_cleanup(self.fake.session_id, reason="owner-restart")
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked, "absence-not-terminal-proof"
        ):
            service.cleanup_plan()
        self.assertEqual(0, self.fake.post_calls)

    def test_cleanup_cancels_one_owned_order_then_flattens_only_owned_delta(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        buy_identifier = self.ledger.claims(self.fake.session_id)[0]["identifier"]
        buy_order = self.fake.closed_orders.pop()
        self.fake.open_orders.append(
            {
                "market": "KRW-BTC",
                "uuid": buy_order["uuid"],
                "identifier": buy_identifier,
                "side": "BID",
                "state": "wait",
            }
        )
        service.recover_or_expire(reason="operator-stop")
        self.enter_cleanup_runtime()
        plan = service.cleanup_plan()
        self.assertEqual("CLEANUP_CANCEL", plan["actions"][0].slot)
        service.dispatch(plan["actions"][0])
        plan = service.cleanup_plan()
        self.assertEqual("CLEANUP_SELL", plan["actions"][0].slot)
        owned = Decimal(plan["ownedQuantity"])
        service.dispatch(plan["actions"][0])
        self.assertGreater(owned, 0)
        self.assertEqual(1, self.fake.cancel_calls)
        self.assertEqual(2, self.fake.post_calls)
        self.assertEqual(Decimal("0.01000000"), self.fake.base)
        self.disarm_for_final()
        self.assertEqual("FINALIZED", service.finalize_if_flat()["state"])

    def test_unorderable_exchange_precision_dust_never_posts_and_seals_safe(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        partial_leg = subject.UpbitLeg.sell(Decimal("0.00010000"))
        partial_payload = {
            "market": "KRW-BTC",
            "side": "ask",
            "ord_type": "market",
            "volume": "0.0001",
        }
        natural_sell = self.sealed_bar(
            "SELL", bar_id="upbit-five-minute-bar-0002"
        )
        self.ledger.note_bar(self.fake.session_id, natural_sell)
        partial = self.ledger.claim(
            self.fake.session_id,
            partial_leg,
            partial_payload,
            functional_capability_verified=True,
            natural_evaluation=natural_sell,
        )
        partial_uuid = "broker-partial-natural-sell-0001"
        self.ledger.resolve_claim(
            partial["claimId"],
            state="RECONCILED",
            response={"uuid": partial_uuid},
        )
        sold = Decimal("0.00009999")
        funds = sold * self.fake.mark
        fee = funds * Decimal("0.0005")
        self.fake.base -= sold
        self.fake.quote += funds - fee
        self.fake.fills.append(
            {
                "market": "KRW-BTC",
                "tradeUuid": "trade-partial-natural-sell-0001",
                "orderUuid": partial_uuid,
                "identifier": partial["identifier"],
                "side": "ASK",
                "volume": str(sold),
                "funds": str(funds),
                "fee": str(fee),
            }
        )
        self.fake.closed_orders.append(
            {
                "market": "KRW-BTC",
                "uuid": partial_uuid,
                "identifier": partial["identifier"],
                "side": "ASK",
                "state": "cancel",
            }
        )
        service.recover_or_expire(reason="operator-stop")
        plan = service.cleanup_plan()
        self.assertEqual([], plan["actions"])
        self.assertFalse(plan["orderableResidual"])
        self.assertTrue(plan["readyToFinalize"])
        self.assertEqual(1, self.fake.post_calls)
        self.disarm_for_final()
        final = service.finalize_if_flat()
        self.assertEqual("SAFE_INCOMPLETE", final["testOutcome"])
        self.assertEqual("0.00000001", final["evidence"]["residualQuantity"])
        self.assertEqual("1", final["evidence"]["residualValue"])
        self.assertTrue(
            final["evidence"]["baselineRestoredWithinExchangePrecision"]
        )
        self.assertEqual(1, self.fake.post_calls)

    def test_existing_btc_is_never_sell_authority_and_owner_loss_forces_cleanup(self) -> None:
        service = self.activate()
        with self.assertRaisesRegex(subject.UpbitFunctionalBlocked, "exceeds-owned-delta"):
            service.dispatch(subject.UpbitLeg.sell(self.fake.base))
        self.assertEqual(0, self.fake.post_calls)
        service.on_bar(self.bar("BUY"))
        self.fake.mark = Decimal("80000000")
        result = service.on_bar(
            self.bar("HOLD", bar_id="upbit-five-minute-bar-0002")
        )
        self.assertEqual("CLEANUP", result["action"])
        self.assertEqual("owner-loss-limit-reached", result["reason"])
        self.assertEqual("CLEANUP", self.ledger.session(self.fake.session_id)["state"])

    def test_sell_requires_official_base_available_not_locked_total(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        original_truth = self.fake.truth

        def locked_truth(**kwargs):
            value = original_truth(**kwargs)
            value["baseAvailable"] = "0.00001"
            rows = [dict(row) for row in value["accountRows"]]
            btc = next(row for row in rows if row["currency"] == "BTC")
            btc["available"] = "0.00001"
            btc["locked"] = subject._decimal_text(
                Decimal(str(value["baseTotal"])) - Decimal("0.00001")
            )
            value["accountRows"] = rows
            value["accountRowsHash"] = _stable_hash(rows)
            return value

        service.truth_reader = locked_truth
        with self.assertRaisesRegex(
            subject.UpbitFunctionalBlocked, "base-available"
        ):
            service.on_bar(
                self.bar("SELL", bar_id="upbit-five-minute-bar-0002")
            )

    def test_hold_bar_monitors_gross_exposure_without_waiting_for_a_signal(self) -> None:
        service = self.activate()
        service.on_bar(self.bar("BUY"))
        self.fake.mark = Decimal("110000000")
        result = service.on_bar(
            self.bar("HOLD", bar_id="upbit-five-minute-bar-0002")
        )
        self.assertEqual("CLEANUP", result["action"])
        self.assertEqual("owner-gross-exposure-limit-reached", result["reason"])
        self.assertEqual("CLEANUP", self.ledger.session(self.fake.session_id)["state"])


if __name__ == "__main__":
    unittest.main()
