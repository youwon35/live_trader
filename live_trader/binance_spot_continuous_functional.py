from __future__ import annotations

"""Fail-closed Binance Spot continuous functional-test safety core.

The module deliberately is *not* connected to :mod:`live_trader.state` or a
network adapter.  Production availability stays false until the application
has authoritative Binance account-wide order/fill/fee readers and a restart
E2E has proved cleanup.  Tests may inject a POST callable to exercise the
durable no-retry boundary without network access.

This lane is intentionally narrower than normal SMALL_LIVE:

* Binance Spot ``BTCUSDT`` only (no futures, margin, short, transfer or
  withdrawal action);
* an exact two-hour non-promotion permit and no more than one extra hour for
  cleanup;
* at most 10 USDT of owned gross exposure and 1 USDT owner-attributed loss;
* one BUY and one position-reducing SELL, with no re-entry;
* signals from finalized, monotonically increasing 5-minute bars only;
* exact Strategy Artifact, Strategy Instance, account fingerprint and route;
* complete fresh account/open/closed/fill/fee truth at every boundary; and
* a durable claim before each POST.  Ambiguous submits are reconciled, never
  retried blindly.
"""

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import json
import math
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping

from .binance_spot_functional_exclusivity import (
    BinanceSpotExclusivityError,
    BinanceSpotExclusivityGuard,
    verify_global_first_live_authority,
)


PRODUCTION_AVAILABLE = False
SCHEMA_VERSION = "binance-spot-continuous-functional-v1"
EVIDENCE_CLASS = "FUNCTIONAL_TEST_NON_PROMOTION"
ENVIRONMENT = "BINANCE_LIVE"
BROKER = "BINANCE"
VENUE = "BINANCE_SPOT"
ASSET = "CRYPTO"
MARKET = "CRYPTO_SPOT"
EXECUTION_ROUTE = "BINANCE_SPOT_CONTINUOUS"
SYMBOL = "BTCUSDT"
BASE_ASSET = "BTC"
QUOTE_ASSET = "USDT"
INTERVAL = "5m"
PERMIT_SECONDS = 2 * 60 * 60
MAX_CLEANUP_SECONDS = 3 * 60 * 60
MAX_ORDER_NOTIONAL = Decimal("10")
MAX_GROSS_EXPOSURE = Decimal("10")
MAX_OWNER_LOSS = Decimal("1")
MAX_TRUTH_AGE_SECONDS = 15.0
MAX_BAR_OBSERVATION_AGE_SECONDS = 90.0
# Cleanup is risk-reducing authority, separate from the one natural BUY/SELL
# pair.  Keep it finite, and always reserve the last slot for cancelling a
# working cleanup order instead of leaving an exchange order behind.
MAX_CLEANUP_ACTIONS = 12

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,120}$")
_TERMINAL_ORDER_STATES = frozenset(
    {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
)
AMBIGUOUS_NONACCEPTANCE_MIN_AGE_SECONDS = 60.0
AMBIGUOUS_NONACCEPTANCE_MIN_OBSERVATIONS = 2
AMBIGUOUS_NONACCEPTANCE_MIN_SPACING_SECONDS = 5.0
_OPEN_ORDER_STATES = frozenset(
    {"NEW", "PENDING_NEW", "PARTIALLY_FILLED", "PENDING_CANCEL"}
)
_ALL_ORDER_STATES = _TERMINAL_ORDER_STATES | _OPEN_ORDER_STATES
_BINDING_FIELDS = frozenset(
    {
        "strategyArtifactId",
        "strategyArtifactHash",
        "artifactFileSha256",
        "strategyInstanceId",
        "strategyInstanceHash",
        "instanceFileSha256",
        "publicationProofHash",
        "publicationProofFileSha256",
        "accountFingerprint",
        "broker",
        "venue",
        "asset",
        "market",
        "executionRoute",
        "symbol",
        "baseAsset",
        "quoteAsset",
        "interval",
    }
)
_PERMIT_FIELDS = frozenset(
    {
        "schemaVersion",
        "permitId",
        "permitHash",
        "sharedPermit",
        "sharedPermitContentHash",
        "environment",
        "status",
        "functionalOnly",
        "evidenceClass",
        "promotionEligible",
        "issuedAt",
        "expiresAt",
        "cleanupDeadlineAt",
        "maxOrderNotional",
        "maxGrossExposure",
        "maxOwnerLoss",
        "maxBuyOrders",
        "maxSellOrders",
        "noReentry",
        "allowShort",
        "futuresAllowed",
        "marginAllowed",
        "borrowAllowed",
        "transferAllowed",
        "withdrawalAllowed",
        "activeDurationSeconds",
        "activationResealRequired",
        "exclusiveAccountRequired",
        "manualTradingAllowed",
        "externalBotsAllowed",
        "otherApiKeysAllowed",
        "terminalAccountWideCausalProofRequired",
        "binding",
    }
)


class BinanceSpotFunctionalError(ValueError):
    """A sealed contract or authoritative-truth invariant failed."""


class BinanceSpotBoundaryBlocked(BinanceSpotFunctionalError):
    """The final functional-only dispatch boundary is closed."""


class DuplicateActionClaim(BinanceSpotFunctionalError):
    """A durable action already exists and must not be submitted again."""


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _stable_hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def permit_content_hash(value: Mapping[str, Any]) -> str:
    content = dict(value)
    content.pop("permitHash", None)
    return _stable_hash(content)


def _text(value: object) -> str:
    return str(value or "").strip()


def _upper(value: object) -> str:
    return _text(value).upper()


def _sha256(value: object, *, label: str) -> str:
    normalized = _text(value).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise BinanceSpotFunctionalError(
            f"{label} must be an exact SHA-256 hex digest"
        )
    return normalized


def _identifier(value: object, *, label: str) -> str:
    normalized = _text(value)
    if not _SAFE_ID_RE.fullmatch(normalized):
        raise BinanceSpotFunctionalError(
            f"{label} must contain 8-120 safe identifier characters"
        )
    return normalized


def _decimal(value: object, *, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BinanceSpotFunctionalError(f"{label} must be a finite decimal") from exc
    if not result.is_finite():
        raise BinanceSpotFunctionalError(f"{label} must be a finite decimal")
    return result


def _decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return "0" if text in {"", "-0"} else text


def _utc_epoch(value: object, *, label: str) -> float:
    text = _text(value)
    if not text:
        raise BinanceSpotFunctionalError(f"{label} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BinanceSpotFunctionalError(f"{label} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise BinanceSpotFunctionalError(f"{label} must include a UTC offset")
    return parsed.astimezone(timezone.utc).timestamp()


def _fresh_epoch(value: object, *, now_epoch: float, label: str) -> float:
    observed = _utc_epoch(value, label=label)
    age = float(now_epoch) - observed
    if age < -1.0 or age > MAX_TRUTH_AGE_SECONDS:
        raise BinanceSpotFunctionalError(f"{label} is stale or future-dated")
    return observed


@dataclass(frozen=True)
class ExactBinding:
    strategy_artifact_id: str
    strategy_artifact_hash: str
    artifact_file_sha256: str
    strategy_instance_id: str
    strategy_instance_hash: str
    instance_file_sha256: str
    publication_proof_hash: str
    publication_proof_file_sha256: str
    account_fingerprint: str
    broker: str = BROKER
    venue: str = VENUE
    asset: str = ASSET
    market: str = MARKET
    execution_route: str = EXECUTION_ROUTE
    symbol: str = SYMBOL
    base_asset: str = BASE_ASSET
    quote_asset: str = QUOTE_ASSET
    interval: str = INTERVAL

    @classmethod
    def parse(cls, value: Mapping[str, Any]) -> "ExactBinding":
        unknown = set(value) - _BINDING_FIELDS
        missing = _BINDING_FIELDS - set(value)
        if unknown or missing:
            raise BinanceSpotFunctionalError(
                "exact binding schema fields changed: "
                f"missing={sorted(missing)}, unknown={sorted(unknown)}"
            )
        exact = {
            "broker": BROKER,
            "venue": VENUE,
            "asset": ASSET,
            "market": MARKET,
            "executionRoute": EXECUTION_ROUTE,
            "symbol": SYMBOL,
            "baseAsset": BASE_ASSET,
            "quoteAsset": QUOTE_ASSET,
            "interval": INTERVAL,
        }
        for field, expected in exact.items():
            if _text(value.get(field)).upper() != expected.upper():
                raise BinanceSpotFunctionalError(
                    f"exact Binance Spot binding requires {field}={expected}"
                )
        artifact_id = _identifier(
            value.get("strategyArtifactId"), label="strategyArtifactId"
        )
        instance_id = _identifier(
            value.get("strategyInstanceId"), label="strategyInstanceId"
        )
        return cls(
            strategy_artifact_id=artifact_id,
            strategy_artifact_hash=_sha256(
                value.get("strategyArtifactHash"), label="strategyArtifactHash"
            ),
            artifact_file_sha256=_sha256(
                value.get("artifactFileSha256"), label="artifactFileSha256"
            ),
            strategy_instance_id=instance_id,
            strategy_instance_hash=_sha256(
                value.get("strategyInstanceHash"), label="strategyInstanceHash"
            ),
            instance_file_sha256=_sha256(
                value.get("instanceFileSha256"), label="instanceFileSha256"
            ),
            publication_proof_hash=_sha256(
                value.get("publicationProofHash"), label="publicationProofHash"
            ),
            publication_proof_file_sha256=_sha256(
                value.get("publicationProofFileSha256"),
                label="publicationProofFileSha256",
            ),
            account_fingerprint=_sha256(
                value.get("accountFingerprint"), label="accountFingerprint"
            ),
        )

    def payload(self) -> dict[str, str]:
        return {
            "strategyArtifactId": self.strategy_artifact_id,
            "strategyArtifactHash": self.strategy_artifact_hash,
            "artifactFileSha256": self.artifact_file_sha256,
            "strategyInstanceId": self.strategy_instance_id,
            "strategyInstanceHash": self.strategy_instance_hash,
            "instanceFileSha256": self.instance_file_sha256,
            "publicationProofHash": self.publication_proof_hash,
            "publicationProofFileSha256": self.publication_proof_file_sha256,
            "accountFingerprint": self.account_fingerprint,
            "broker": self.broker,
            "venue": self.venue,
            "asset": self.asset,
            "market": self.market,
            "executionRoute": self.execution_route,
            "symbol": self.symbol,
            "baseAsset": self.base_asset,
            "quoteAsset": self.quote_asset,
            "interval": self.interval,
        }


@dataclass(frozen=True)
class ExactPermit:
    permit_id: str
    permit_hash: str
    issued_epoch: float
    expires_epoch: float
    cleanup_deadline_epoch: float
    binding: ExactBinding
    active_duration_seconds: int = PERMIT_SECONDS
    activation_reseal_required: bool = True
    exclusive_account_required: bool = True

    @classmethod
    def parse(
        cls,
        value: Mapping[str, Any],
        *,
        now_epoch: float,
    ) -> "ExactPermit":
        unknown = set(value) - _PERMIT_FIELDS
        missing = _PERMIT_FIELDS - set(value)
        if unknown or missing:
            raise BinanceSpotFunctionalError(
                "exact permit schema fields changed: "
                f"missing={sorted(missing)}, unknown={sorted(unknown)}"
            )
        if _text(value.get("schemaVersion")) != SCHEMA_VERSION:
            raise BinanceSpotFunctionalError("permit schemaVersion changed")
        supplied_hash = _sha256(value.get("permitHash"), label="permitHash")
        calculated_hash = permit_content_hash(value)
        if not secrets.compare_digest(supplied_hash, calculated_hash):
            raise BinanceSpotFunctionalError(
                "permitHash does not match the complete immutable permit content"
            )
        shared_payload = value.get("sharedPermit")
        if not isinstance(shared_payload, Mapping):
            raise BinanceSpotFunctionalError("shared v2 functional permit is required")
        try:
            from trading_runtime.functional_test import (
                FunctionalTestEnvironment,
                FunctionalTestDurationUnit,
                parse_functional_test_permit,
            )

            shared = parse_functional_test_permit(shared_payload)
        except Exception as exc:
            raise BinanceSpotFunctionalError(
                "shared v2 functional permit is invalid"
            ) from exc
        shared_hash = _sha256(
            value.get("sharedPermitContentHash"),
            label="sharedPermitContentHash",
        )
        if (
            shared.schema_version != "functional-test-permit-v2"
            or shared.content_hash != shared_hash
            or shared.environment is not FunctionalTestEnvironment.BINANCE_LIVE
            or shared.duration_unit is not FunctionalTestDurationUnit.HOURS
            or shared.duration_value != 2
            or shared.promotion_eligible
        ):
            raise BinanceSpotFunctionalError(
                "shared permit environment/duration/hash/promotion binding changed"
            )
        if _upper(value.get("environment")) != ENVIRONMENT:
            raise BinanceSpotFunctionalError("permit environment must be BINANCE_LIVE")
        if value.get("functionalOnly") is not True:
            raise BinanceSpotFunctionalError("permit must be functional-only")
        if value.get("promotionEligible") not in (None, False):
            raise BinanceSpotFunctionalError("permit cannot be promotion evidence")
        if _text(value.get("evidenceClass") or EVIDENCE_CLASS) != EVIDENCE_CLASS:
            raise BinanceSpotFunctionalError("permit evidence class changed")
        if _upper(value.get("status")) != "ACTIVE":
            raise BinanceSpotFunctionalError("permit is not ACTIVE")
        if _decimal(value.get("maxOrderNotional"), label="maxOrderNotional") != MAX_ORDER_NOTIONAL:
            raise BinanceSpotFunctionalError("permit maxOrderNotional must equal 10 USDT")
        if _decimal(value.get("maxGrossExposure"), label="maxGrossExposure") != MAX_GROSS_EXPOSURE:
            raise BinanceSpotFunctionalError("permit maxGrossExposure must equal 10 USDT")
        if _decimal(value.get("maxOwnerLoss"), label="maxOwnerLoss") != MAX_OWNER_LOSS:
            raise BinanceSpotFunctionalError("permit maxOwnerLoss must equal 1 USDT")
        if int(value.get("maxBuyOrders", -1)) != 1 or int(value.get("maxSellOrders", -1)) != 1:
            raise BinanceSpotFunctionalError("permit requires exactly BUY1 and SELL1 caps")
        if value.get("noReentry") is not True:
            raise BinanceSpotFunctionalError("permit must seal noReentry=true")
        if (
            int(value.get("activeDurationSeconds") or 0) != PERMIT_SECONDS
            or value.get("activationResealRequired") is not True
        ):
            raise BinanceSpotFunctionalError(
                "permit must be activation-resealed for exactly 7200 seconds"
            )
        if (
            value.get("exclusiveAccountRequired") is not True
            or value.get("manualTradingAllowed") is not False
            or value.get("externalBotsAllowed") is not False
            or value.get("otherApiKeysAllowed") is not False
            or value.get("terminalAccountWideCausalProofRequired") is not True
        ):
            raise BinanceSpotFunctionalError(
                "permit must seal exclusive-account/no-manual/no-bot/no-other-key use"
            )
        if value.get("allowShort") not in (None, False):
            raise BinanceSpotFunctionalError("short is forbidden")
        forbidden = (
            "futuresAllowed",
            "marginAllowed",
            "borrowAllowed",
            "transferAllowed",
            "withdrawalAllowed",
        )
        if any(value.get(field) not in (None, False) for field in forbidden):
            raise BinanceSpotFunctionalError(
                "futures/margin/borrow/transfer/withdrawal are forbidden"
            )
        issued = _utc_epoch(value.get("issuedAt"), label="permit issuedAt")
        expires = _utc_epoch(value.get("expiresAt"), label="permit expiresAt")
        cleanup = _utc_epoch(
            value.get("cleanupDeadlineAt"), label="permit cleanupDeadlineAt"
        )
        if abs((expires - issued) - PERMIT_SECONDS) > 0.001:
            raise BinanceSpotFunctionalError("permit must last exactly two hours")
        if cleanup < expires or cleanup - issued > MAX_CLEANUP_SECONDS + 0.001:
            raise BinanceSpotFunctionalError(
                "cleanup deadline must be between permit expiry and issuedAt+3h"
            )
        if now_epoch < issued - 1.0 or now_epoch >= cleanup:
            raise BinanceSpotFunctionalError("permit is not in its active/cleanup window")
        binding_value = value.get("binding")
        if not isinstance(binding_value, Mapping):
            raise BinanceSpotFunctionalError("permit exact binding is required")
        parsed_binding = ExactBinding.parse(binding_value)
        shared_binding = shared.binding
        if (
            shared.permit_id != _text(value.get("permitId"))
            or shared_binding.strategy_artifact_id
            != parsed_binding.strategy_artifact_id
            or shared_binding.strategy_artifact_hash
            != parsed_binding.strategy_artifact_hash
            or shared_binding.strategy_instance_id
            != parsed_binding.strategy_instance_id
            or shared_binding.portfolio_required
            or shared_binding.portfolio_artifact_id
            or shared_binding.portfolio_artifact_hash
            or shared_binding.portfolio_instance_id
            or shared_binding.account_id != parsed_binding.account_fingerprint
            or shared_binding.symbols != (SYMBOL,)
            or shared_binding.market_group != MARKET
            or shared_binding.execution_route != EXECUTION_ROUTE
            or shared_binding.settlement_currency != QUOTE_ASSET
            or shared_binding.exchanges != (VENUE,)
            or shared_binding.symbol_routes != ((SYMBOL, VENUE),)
        ):
            raise BinanceSpotFunctionalError(
                "shared permit and immutable Binance selection extension differ"
            )
        if (
            Decimal(str(shared.caps.max_order_notional)) != MAX_ORDER_NOTIONAL
            or Decimal(str(shared.caps.max_gross_exposure)) != MAX_GROSS_EXPOSURE
            or Decimal(str(shared.caps.max_loss)) != MAX_OWNER_LOSS
            or shared.caps.max_orders != 2
            or shared.caps.max_open_positions != 1
        ):
            raise BinanceSpotFunctionalError("shared Binance permit caps changed")
        if (
            abs(shared.issued_at.timestamp() - issued) > 0.001
            or abs(shared.starts_at.timestamp() - issued) > 0.001
            or abs(shared.ends_at.timestamp() - expires) > 0.001
        ):
            raise BinanceSpotFunctionalError(
                "shared permit time window and extension differ"
            )
        return cls(
            permit_id=_identifier(value.get("permitId"), label="permitId"),
            permit_hash=supplied_hash,
            issued_epoch=issued,
            expires_epoch=expires,
            cleanup_deadline_epoch=cleanup,
            binding=parsed_binding,
            active_duration_seconds=PERMIT_SECONDS,
            activation_reseal_required=True,
            exclusive_account_required=True,
        )


@dataclass(frozen=True)
class FunctionalAuthority:
    real_orders_enabled: bool
    dry_run: bool
    kill_switch: bool
    new_entries_blocked: bool
    ordinary_live_allowed: bool
    smoke_allowed: bool
    functional_only_routing: bool
    active_permit_id: str
    active_permit_hash: str
    active_session_id: str
    capability_hash: str
    cleanup_authority: bool
    cleanup_session_id: str
    cleanup_capability_hash: str
    authority_revision: str

    @classmethod
    def parse(cls, value: Mapping[str, Any]) -> "FunctionalAuthority":
        return cls(
            real_orders_enabled=value.get("realOrdersEnabled") is True,
            dry_run=value.get("dryRun") is True,
            kill_switch=value.get("killSwitch") is True,
            new_entries_blocked=value.get("newEntriesBlocked") is True,
            ordinary_live_allowed=value.get("ordinaryLiveAllowed") is True,
            smoke_allowed=value.get("smokeAllowed") is True,
            functional_only_routing=value.get("functionalOnlyRouting") is True,
            active_permit_id=_text(value.get("activePermitId")),
            active_permit_hash=_text(value.get("activePermitHash")).lower(),
            active_session_id=_text(value.get("activeSessionId")),
            capability_hash=_text(value.get("functionalCapabilityHash")).lower(),
            cleanup_authority=value.get("cleanupOnlyAuthority") is True,
            cleanup_session_id=_text(value.get("cleanupSessionId")),
            cleanup_capability_hash=_text(
                value.get("cleanupCapabilityHash")
            ).lower(),
            authority_revision=_text(value.get("authorityRevision")),
        )

    def assert_prestart(self, permit: ExactPermit) -> None:
        blockers: list[str] = []
        if not self.new_entries_blocked:
            blockers.append("prestart-new-entries-must-be-blocked")
        if self.ordinary_live_allowed:
            blockers.append("ordinary-live-route-open")
        if self.smoke_allowed:
            blockers.append("smoke-route-open")
        if not self.functional_only_routing:
            blockers.append("functional-only-router-disabled")
        if self.active_permit_id not in {"", permit.permit_id}:
            blockers.append("different-active-permit")
        if self.active_permit_hash not in {"", permit.permit_hash}:
            blockers.append("active-permit-hash-changed")
        if self.active_session_id or self.capability_hash:
            blockers.append("functional-session-already-active")
        if (
            self.cleanup_authority
            or self.cleanup_session_id
            or self.cleanup_capability_hash
        ):
            blockers.append("cleanup-authority-already-active")
        if not self.authority_revision:
            blockers.append("authority-revision-missing")
        if blockers:
            raise BinanceSpotBoundaryBlocked(",".join(blockers))

    def assert_dispatch(
        self,
        permit: ExactPermit,
        *,
        session_id: str,
        capability_hash: str,
        cleanup_only: bool,
    ) -> None:
        blockers: list[str] = []
        if not self.real_orders_enabled:
            blockers.append("real-orders-disabled")
        if self.dry_run:
            blockers.append("dry-run-enabled")
        # Global new-entry protection deliberately remains ON.  Only this
        # exact unforgeable capability may cross the functional-only router.
        if not self.new_entries_blocked:
            blockers.append("global-new-entry-protection-open")
        if self.ordinary_live_allowed:
            blockers.append("ordinary-live-route-open")
        if self.smoke_allowed:
            blockers.append("smoke-route-open")
        if not self.functional_only_routing:
            blockers.append("functional-only-router-disabled")
        if self.active_permit_id != permit.permit_id:
            blockers.append("active-permit-id-changed")
        if self.active_permit_hash != permit.permit_hash:
            blockers.append("active-permit-hash-changed")
        if self.active_session_id != session_id:
            blockers.append("active-session-changed")
        if not secrets.compare_digest(self.capability_hash, capability_hash):
            blockers.append("functional-capability-changed")
        if not self.authority_revision:
            blockers.append("authority-revision-missing")
        if cleanup_only:
            if not self.cleanup_authority:
                blockers.append("cleanup-only-authority-missing")
            if self.cleanup_session_id != session_id:
                blockers.append("cleanup-session-changed")
            if not secrets.compare_digest(
                self.cleanup_capability_hash, capability_hash
            ):
                blockers.append("cleanup-capability-changed")
            # Kill is allowed only for the independently verified exact-owned
            # CANCEL/SELL cleanup shape.  It never permits BUY.
        elif self.kill_switch:
            blockers.append("kill-switch-active")
        if blockers:
            raise BinanceSpotBoundaryBlocked(",".join(blockers))

    def assert_final_reset(self) -> None:
        blockers: list[str] = []
        if not self.new_entries_blocked:
            blockers.append("final-new-entries-not-blocked")
        if self.ordinary_live_allowed or self.smoke_allowed:
            blockers.append("ordinary-or-smoke-route-open")
        if self.functional_only_routing:
            blockers.append("functional-only-router-not-reset")
        if self.active_permit_id or self.active_permit_hash:
            blockers.append("functional-permit-pointer-not-reset")
        if self.active_session_id or self.capability_hash:
            blockers.append("functional-capability-not-reset")
        if (
            self.cleanup_authority
            or self.cleanup_session_id
            or self.cleanup_capability_hash
        ):
            blockers.append("cleanup-capability-not-reset")
        if blockers:
            raise BinanceSpotBoundaryBlocked(",".join(blockers))


@dataclass(frozen=True)
class SymbolRules:
    min_quantity: Decimal
    max_quantity: Decimal
    step_size: Decimal
    min_notional: Decimal
    max_notional: Decimal
    min_notional_applies_to_market: bool
    max_notional_applies_to_market: bool
    avg_price_mins: int
    market_reference_price: Decimal
    market_reference_source: str
    quantity_filter_type: str
    account_symbol_permission_proof_hash: str

    @classmethod
    def parse(cls, truth: Mapping[str, Any]) -> "SymbolRules":
        if truth.get("exchangeInfoComplete") is not True:
            raise BinanceSpotFunctionalError("exchange-info truth is incomplete")
        if _upper(truth.get("symbol")) != SYMBOL:
            raise BinanceSpotFunctionalError("exchange-info symbol changed")
        if _upper(truth.get("status")) != "TRADING":
            raise BinanceSpotFunctionalError("BTCUSDT is not TRADING")
        if truth.get("spotTradingAllowed") is not True:
            raise BinanceSpotFunctionalError("spot trading is not allowed")
        if truth.get("quoteOrderQtyMarketAllowed") is not True:
            raise BinanceSpotFunctionalError("quote-order-qty market BUY unavailable")
        permission_proof = {
            field: truth.get(field)
            for field in (
                "accountCanTrade",
                "accountType",
                "accountPermissions",
                "symbolPermissionSets",
                "permissionSemantics",
                "symbolPermissionsAuthorized",
            )
        }
        permission_hash = _text(
            truth.get("accountSymbolPermissionProofHash")
        ).lower()
        if (
            permission_proof["accountCanTrade"] is not True
            or permission_proof["accountType"] != "SPOT"
            or not isinstance(permission_proof["accountPermissions"], list)
            or not permission_proof["accountPermissions"]
            or not isinstance(permission_proof["symbolPermissionSets"], list)
            or not permission_proof["symbolPermissionSets"]
            or permission_proof["permissionSemantics"] != "AND_OF_OR_SETS"
            or permission_proof["symbolPermissionsAuthorized"] is not True
            or not _SHA256_RE.fullmatch(permission_hash)
            or permission_hash != _stable_hash(permission_proof)
        ):
            raise BinanceSpotFunctionalError(
                "account/symbol permission proof is incomplete"
            )
        if any(
            truth.get(field) is True
            for field in ("marginMode", "futuresMode", "borrowMode", "withdrawalAction")
        ):
            raise BinanceSpotFunctionalError("non-spot product/action truth is forbidden")
        minimum = _decimal(truth.get("minQty"), label="minQty")
        maximum = _decimal(truth.get("maxQty"), label="maxQty")
        step = _decimal(truth.get("stepSize"), label="stepSize")
        min_notional = _decimal(truth.get("minNotional"), label="minNotional")
        max_notional = _decimal(
            truth.get("maxNotional", MAX_ORDER_NOTIONAL), label="maxNotional"
        )
        if not isinstance(truth.get("minNotionalAppliesToMarket"), bool) or not isinstance(
            truth.get("maxNotionalAppliesToMarket"), bool
        ):
            raise BinanceSpotFunctionalError(
                "market notional applicability is not authoritative"
            )
        try:
            avg_price_mins = int(truth.get("avgPriceMins"))
        except (TypeError, ValueError) as exc:
            raise BinanceSpotFunctionalError("avgPriceMins is invalid") from exc
        reference_price = _decimal(
            truth.get("marketReferencePrice"), label="marketReferencePrice"
        )
        reference_source = _upper(truth.get("marketReferenceSource"))
        quantity_filter_type = _upper(truth.get("quantityFilterType"))
        if minimum <= 0 or maximum < minimum or step <= 0:
            raise BinanceSpotFunctionalError("quantity filters are invalid")
        if min_notional <= 0 or max_notional < min_notional:
            raise BinanceSpotFunctionalError("notional filters are invalid")
        if (
            truth.get("minNotionalAppliesToMarket") is True
            and min_notional > MAX_ORDER_NOTIONAL
        ):
            raise BinanceSpotFunctionalError(
                "exchange minimum exceeds the 10 USDT functional cap"
            )
        if (
            avg_price_mins < 0
            or reference_price <= 0
            or reference_source
            not in {"BINANCE_TICKER_PRICE", "BINANCE_AVG_PRICE"}
            or quantity_filter_type not in {"LOT_SIZE", "MARKET_LOT_SIZE"}
        ):
            raise BinanceSpotFunctionalError(
                "market filter reference/quantity semantics are incomplete"
            )
        if avg_price_mins == 0 and reference_source != "BINANCE_TICKER_PRICE":
            raise BinanceSpotFunctionalError("zero-minute market reference must use ticker")
        if avg_price_mins > 0 and reference_source != "BINANCE_AVG_PRICE":
            raise BinanceSpotFunctionalError(
                "positive avgPriceMins requires official Binance average price"
            )
        return cls(
            minimum,
            maximum,
            step,
            min_notional,
            max_notional,
            bool(truth["minNotionalAppliesToMarket"]),
            bool(truth["maxNotionalAppliesToMarket"]),
            avg_price_mins,
            reference_price,
            reference_source,
            quantity_filter_type,
            permission_hash,
        )

    def floor_quantity(self, quantity: Decimal) -> Decimal:
        floored = (quantity / self.step_size).to_integral_value(
            rounding=ROUND_DOWN
        ) * self.step_size
        return floored

    def assert_permission_proof(self, truth: "AccountTruth") -> None:
        if not secrets.compare_digest(
            self.account_symbol_permission_proof_hash,
            truth.account_symbol_permission_proof_hash,
        ):
            raise BinanceSpotBoundaryBlocked(
                "account/symbol permission proof changed across truth and rules"
            )

    def assert_buy_notional(self, notional: Decimal) -> None:
        if (
            (
                self.min_notional_applies_to_market
                and notional < self.min_notional
            )
            or (
                self.max_notional_applies_to_market
                and notional > self.max_notional
            )
            or notional > MAX_ORDER_NOTIONAL
        ):
            raise BinanceSpotFunctionalError(
                "BUY notional violates exchange or functional caps"
            )

    def normalize_flatten_quantity(
        self,
        quantity: Decimal,
        *,
        price: Decimal,
    ) -> Decimal:
        normalized = self.floor_quantity(quantity)
        if normalized <= 0 or normalized < self.min_quantity:
            raise BinanceSpotFunctionalError("owned flatten quantity is below minQty")
        if normalized > self.max_quantity:
            raise BinanceSpotFunctionalError("owned flatten quantity exceeds maxQty")
        notional = normalized * self.market_reference_price
        if self.min_notional_applies_to_market and notional < self.min_notional:
            raise BinanceSpotFunctionalError("owned flatten is below minNotional")
        if self.max_notional_applies_to_market and notional > self.max_notional:
            raise BinanceSpotFunctionalError("owned flatten exceeds market maxNotional")
        # A price rise after a capped entry can make the reducing SELL worth
        # more than 10 USDT.  The cap applies to entry cost/exposure creation;
        # an exact owned-quantity flatten must never be blocked for increasing
        # in value.
        return normalized

    def is_unorderable_residual(
        self,
        quantity: Decimal,
        *,
        price: Decimal,
    ) -> bool:
        """Prove that an owned positive residue cannot cross a Spot SELL filter.

        A BUY commission paid in BTC can leave less than one LOT_SIZE step after
        the orderable quantity is sold.  That dust is still session-owned, but
        attempting to round it up would consume the user's pre-existing BTC.
        """

        if quantity < 0 or price <= 0:
            raise BinanceSpotFunctionalError("owned residual/price is invalid")
        if quantity == 0:
            return True
        orderable = self.floor_quantity(quantity)
        return (
            orderable <= 0
            or orderable < self.min_quantity
            or (
                self.min_notional_applies_to_market
                and orderable * self.market_reference_price < self.min_notional
            )
        )


@dataclass(frozen=True)
class AccountTruth:
    observed_epoch: float
    history_baseline_epoch: float
    history_cutoff_epoch: float
    base_total: Decimal
    quote_total: Decimal
    base_available: Decimal
    quote_available: Decimal
    mark_price: Decimal
    open_orders: tuple[Mapping[str, Any], ...]
    closed_orders: tuple[Mapping[str, Any], ...]
    fills: tuple[Mapping[str, Any], ...]
    external_activity_absent: bool
    third_asset_fee_funding_absent: bool
    fee_quote_valuation_complete: bool
    stream_session_id: str
    stream_permit_id: str
    stream_permit_hash: str
    cleanup_recovery_only: bool
    stream_gap_evidence_hash: str
    recovery_attestation_hash: str
    stream_journal_seal_hash: str
    stream_journal_event_count: int
    account_symbol_permission_proof_hash: str
    account_wide_causal_closure_proven: bool
    official_rest_snapshot: Mapping[str, Any]
    official_rest_truth_hash: str

    @classmethod
    def parse(
        cls,
        value: Mapping[str, Any],
        *,
        binding: ExactBinding,
        now_epoch: float,
    ) -> "AccountTruth":
        if _upper(value.get("broker")) != BROKER or _upper(value.get("venue")) != VENUE:
            raise BinanceSpotFunctionalError("broker truth is not Binance Spot")
        if _text(value.get("accountFingerprint")).lower() != binding.account_fingerprint:
            raise BinanceSpotFunctionalError("account fingerprint changed")
        observed = _fresh_epoch(
            value.get("observedAt"), now_epoch=now_epoch, label="account observedAt"
        )
        history_baseline = _utc_epoch(
            value.get("historyBaselineAt"), label="historyBaselineAt"
        )
        history_cutoff = _fresh_epoch(
            value.get("historyCutoffAt"),
            now_epoch=now_epoch,
            label="historyCutoffAt",
        )
        if history_baseline > history_cutoff or history_cutoff > observed + 1.0:
            raise BinanceSpotFunctionalError(
                "history baseline/cutoff/observation order is invalid"
            )
        cleanup_recovery_only = (
            _upper(value.get("cleanupRecoveryMode"))
            == "REST_RECONCILED_CLEANUP_ONLY"
        )
        stream_gap_evidence_hash = _text(
            value.get("streamGapEvidenceHash")
        ).lower()
        recovery_attestation_hash = _text(
            value.get("recoveryAttestationHash")
        ).lower()
        if value.get("restUserStreamCrossChecked") is not True:
            if (
                not cleanup_recovery_only
                or value.get("preservedStreamGap") is not True
                or not _SHA256_RE.fullmatch(stream_gap_evidence_hash)
                or not _SHA256_RE.fullmatch(recovery_attestation_hash)
            ):
                raise BinanceSpotFunctionalError(
                    "REST truth lacks a gapless stream or typed cleanup recovery"
                )
        elif cleanup_recovery_only:
            raise BinanceSpotFunctionalError(
                "cleanup recovery cannot claim a gapless stream cross-check"
            )
        stream_observed = _fresh_epoch(
            value.get("userStreamObservedAt"),
            now_epoch=now_epoch,
            label="userStreamObservedAt",
        )
        if stream_observed > observed + 1.0:
            raise BinanceSpotFunctionalError(
                "user stream proof is newer than account truth"
            )
        stream_session_id = _text(value.get("streamSessionId"))
        stream_permit_id = _text(value.get("streamPermitId"))
        stream_permit_hash = _text(value.get("streamPermitHash")).lower()
        stream_journal_seal_hash = _text(
            value.get("streamJournalSealHash")
        ).lower()
        stream_journal_event_count = int(
            value.get("streamJournalEventCount") or 0
        )
        if stream_journal_seal_hash and (
            not _SHA256_RE.fullmatch(stream_journal_seal_hash)
            or stream_journal_event_count < 0
        ):
            raise BinanceSpotFunctionalError(
                "durable stream journal seal/count is malformed"
            )
        if any((stream_session_id, stream_permit_id, stream_permit_hash)) and (
            not stream_session_id.startswith("bnsft-")
            or not stream_permit_id.startswith("functional-test-")
            or not _SHA256_RE.fullmatch(stream_permit_hash)
        ):
            raise BinanceSpotFunctionalError(
                "user stream session/permit binding is incomplete"
            )
        required_true = (
            "accountComplete",
            "balancesComplete",
            "openOrdersComplete",
            "closedOrdersComplete",
            "fillsComplete",
            "feesComplete",
        )
        for field in required_true:
            if value.get(field) is not True:
                raise BinanceSpotFunctionalError(f"{field} is not authoritative")
        permission_proof = {
            field: value.get(field)
            for field in (
                "accountCanTrade",
                "accountType",
                "accountPermissions",
                "symbolPermissionSets",
                "permissionSemantics",
                "symbolPermissionsAuthorized",
            )
        }
        permission_hash = _text(
            value.get("accountSymbolPermissionProofHash")
        ).lower()
        if (
            permission_proof["accountCanTrade"] is not True
            or permission_proof["accountType"] != "SPOT"
            or not isinstance(permission_proof["accountPermissions"], list)
            or not permission_proof["accountPermissions"]
            or not isinstance(permission_proof["symbolPermissionSets"], list)
            or not permission_proof["symbolPermissionSets"]
            or permission_proof["permissionSemantics"] != "AND_OF_OR_SETS"
            or permission_proof["symbolPermissionsAuthorized"] is not True
            or not _SHA256_RE.fullmatch(permission_hash)
            or permission_hash != _stable_hash(permission_proof)
        ):
            raise BinanceSpotFunctionalError(
                "account/symbol permission proof is incomplete"
            )
        raw_rest_snapshot = value.get("officialRestSnapshot")
        official_rest_hash = _text(value.get("officialRestTruthHash")).lower()
        if raw_rest_snapshot is None and not official_rest_hash:
            official_rest_snapshot: Mapping[str, Any] = {}
        elif (
            not isinstance(raw_rest_snapshot, Mapping)
            or raw_rest_snapshot.get("schemaVersion")
            != "binance-spot-functional-official-rest-set/v1"
            or not _SHA256_RE.fullmatch(official_rest_hash)
            or not secrets.compare_digest(
                _stable_hash(dict(raw_rest_snapshot)), official_rest_hash
            )
        ):
            raise BinanceSpotFunctionalError(
                "official REST response-set hash is incomplete"
            )
        else:
            official_rest_snapshot = dict(raw_rest_snapshot)
        exact_scopes = {
            "balancesScope": "ACCOUNT_ALL_BALANCES",
            "openOrdersScope": "ACCOUNT_ALL_OPEN_ORDERS",
            # Binance's official allOrders/myTrades endpoints require a
            # symbol.  The permit is exact-one-symbol, so completeness is
            # sealed over BTCUSDT since baseline while open orders and
            # balances remain account-wide.
            "closedOrdersScope": "BTCUSDT_ALL_ORDERS_SINCE_BASELINE",
            "fillsScope": "BTCUSDT_ALL_TRADES_SINCE_BASELINE",
            "feesScope": "BTCUSDT_ALL_TRADE_FEES_SINCE_BASELINE",
        }
        for field, expected in exact_scopes.items():
            if _upper(value.get(field)) != expected:
                raise BinanceSpotFunctionalError(f"{field} must equal {expected}")
        balances = value.get("balances")
        if not isinstance(balances, list) or any(
            not isinstance(item, Mapping) for item in balances
        ):
            raise BinanceSpotFunctionalError("balances must be a complete list")
        totals: dict[str, Decimal] = {}
        available: dict[str, Decimal] = {}
        for row in balances:
            asset = _upper(row.get("asset"))
            if not asset or asset in totals:
                raise BinanceSpotFunctionalError("balance assets must be nonempty and unique")
            free = _decimal(row.get("free"), label=f"{asset} free")
            locked = _decimal(row.get("locked"), label=f"{asset} locked")
            if free < 0 or locked < 0:
                raise BinanceSpotFunctionalError("balances cannot be negative")
            totals[asset] = free + locked
            available[asset] = free
        if BASE_ASSET not in totals or QUOTE_ASSET not in totals:
            raise BinanceSpotFunctionalError("BTC and USDT balances are required")
        collections: dict[str, tuple[Mapping[str, Any], ...]] = {}
        for field in ("openOrders", "closedOrders", "fills"):
            rows = value.get(field)
            if not isinstance(rows, list) or any(
                not isinstance(item, Mapping) for item in rows
            ):
                raise BinanceSpotFunctionalError(f"{field} must be a complete list")
            collections[field] = tuple(rows)
        mark = _decimal(value.get("markPrice"), label="markPrice")
        if mark <= 0:
            raise BinanceSpotFunctionalError("fresh markPrice must be positive")
        return cls(
            observed_epoch=observed,
            history_baseline_epoch=history_baseline,
            history_cutoff_epoch=history_cutoff,
            base_total=totals[BASE_ASSET],
            quote_total=totals[QUOTE_ASSET],
            base_available=available[BASE_ASSET],
            quote_available=available[QUOTE_ASSET],
            mark_price=mark,
            open_orders=collections["openOrders"],
            closed_orders=collections["closedOrders"],
            fills=collections["fills"],
            external_activity_absent=value.get("externalActivityAbsent") is True,
            # Binance's regular third-asset Spot commission is funded from BNB.
            # A complete account response with no positive BNB balance proves
            # that the next order cannot consume BNB even if the preference is
            # enabled.  This is required for entry, never for risk-reducing exit.
            third_asset_fee_funding_absent=totals.get("BNB", Decimal("0")) == 0,
            fee_quote_valuation_complete=value.get(
                "feeQuoteValuationComplete", True
            ) is True,
            stream_session_id=stream_session_id,
            stream_permit_id=stream_permit_id,
            stream_permit_hash=stream_permit_hash,
            cleanup_recovery_only=cleanup_recovery_only,
            stream_gap_evidence_hash=stream_gap_evidence_hash,
            recovery_attestation_hash=recovery_attestation_hash,
            stream_journal_seal_hash=stream_journal_seal_hash,
            stream_journal_event_count=stream_journal_event_count,
            account_symbol_permission_proof_hash=permission_hash,
            account_wide_causal_closure_proven=(
                value.get("accountWideCausalClosureProven") is True
            ),
            official_rest_snapshot=official_rest_snapshot,
            official_rest_truth_hash=official_rest_hash,
        )


@dataclass(frozen=True)
class FinalizedBarSignal:
    close_epoch: float
    signal: str
    evaluation_id: str
    evaluation: Mapping[str, Any]

    @classmethod
    def parse(
        cls,
        value: Mapping[str, Any],
        *,
        binding: ExactBinding,
        now_epoch: float,
        previous_close_epoch: float,
    ) -> "FinalizedBarSignal":
        exact = {
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "executionRoute": EXECUTION_ROUTE,
            "strategyArtifactId": binding.strategy_artifact_id,
            "strategyArtifactHash": binding.strategy_artifact_hash,
            "strategyArtifactFileSha256": binding.artifact_file_sha256,
            "strategyInstanceId": binding.strategy_instance_id,
            "strategyInstanceHash": binding.strategy_instance_hash,
            "strategyInstanceFileSha256": binding.instance_file_sha256,
            "publicationProofHash": binding.publication_proof_hash,
            "publicationProofFileSha256": binding.publication_proof_file_sha256,
            "accountFingerprint": binding.account_fingerprint,
            "bindingHash": _stable_hash(binding.payload()),
        }
        for field, expected in exact.items():
            if _text(value.get(field)).upper() != expected.upper():
                raise BinanceSpotFunctionalError(f"bar evaluation {field} changed")
        if value.get("finalized") is not True:
            raise BinanceSpotFunctionalError("only finalized 5-minute bars are allowed")
        if (
            value.get("strategyEvaluationComplete") is not True
            or value.get("naturalSignal") is not True
            or value.get("forced") not in (None, False)
            or _upper(value.get("barSource")) != "BINANCE_SPOT_KLINE"
        ):
            raise BinanceSpotFunctionalError(
                "only complete natural strategy evaluation on Binance finalized klines is allowed"
            )
        close_epoch = _utc_epoch(value.get("barCloseAt"), label="barCloseAt")
        if close_epoch % 300 != 0:
            raise BinanceSpotFunctionalError("barCloseAt is not on a 5-minute boundary")
        if close_epoch <= previous_close_epoch:
            raise BinanceSpotFunctionalError("barCloseAt is duplicate or out of order")
        observed = _utc_epoch(value.get("observedAt"), label="bar observedAt")
        if observed < close_epoch or now_epoch - observed > MAX_BAR_OBSERVATION_AGE_SECONDS:
            raise BinanceSpotFunctionalError("finalized bar observation is stale/invalid")
        signal = _upper(value.get("signal"))
        if signal not in {"BUY", "SELL", "HOLD"}:
            raise BinanceSpotFunctionalError("signal must be BUY, SELL, or HOLD")
        return cls(
            close_epoch=close_epoch,
            signal=signal,
            evaluation_id=_identifier(value.get("evaluationId"), label="evaluationId"),
            evaluation=dict(value),
        )


class DurableFunctionalLedger:
    """SQLite session/action ledger used before every side effect."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_spot_functional_sessions (
                    session_id TEXT PRIMARY KEY,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    binding_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    capability_hash TEXT NOT NULL,
                    capability_seal_hash TEXT NOT NULL,
                    baseline_base TEXT NOT NULL,
                    baseline_quote TEXT NOT NULL,
                    baseline_open_ids_json TEXT NOT NULL,
                    exclusivity_coverage_started_epoch REAL NOT NULL DEFAULT 0,
                    last_bar_close_epoch REAL NOT NULL DEFAULT 0,
                    buy_claimed INTEGER NOT NULL DEFAULT 0,
                    sell_claimed INTEGER NOT NULL DEFAULT 0,
                    cleanup_sell_claimed INTEGER NOT NULL DEFAULT 0,
                    cleanup_started INTEGER NOT NULL DEFAULT 0,
                    cleanup_recovery_used INTEGER NOT NULL DEFAULT 0,
                    final_new_entries_blocked INTEGER NOT NULL DEFAULT 0,
                    started_epoch REAL NOT NULL,
                    expires_epoch REAL NOT NULL,
                    cleanup_deadline_epoch REAL NOT NULL,
                    finalized_epoch REAL,
                    final_evidence_json TEXT NOT NULL DEFAULT '',
                    final_evidence_hash TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT ''
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_binance_functional_active_route
                ON binance_spot_functional_sessions(binding_hash)
                WHERE state IN ('RUNNING','CLEANUP','RECONCILIATION_REQUIRED');
                CREATE TABLE IF NOT EXISTS binance_spot_functional_actions (
                    claim_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    action_kind TEXT NOT NULL,
                    client_order_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    sealed_action_json TEXT NOT NULL,
                    response_hash TEXT NOT NULL DEFAULT '',
                    broker_order_id TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    pre_base_total TEXT NOT NULL DEFAULT '',
                    pre_quote_total TEXT NOT NULL DEFAULT '',
                    absence_proof_count INTEGER NOT NULL DEFAULT 0,
                    absence_first_epoch REAL NOT NULL DEFAULT 0,
                    absence_last_epoch REAL NOT NULL DEFAULT 0,
                    absence_proof_hash TEXT NOT NULL DEFAULT '',
                    post_marker_epoch REAL NOT NULL DEFAULT 0,
                    created_epoch REAL NOT NULL,
                    updated_epoch REAL NOT NULL,
                    UNIQUE(session_id, action_kind),
                    FOREIGN KEY(session_id) REFERENCES binance_spot_functional_sessions(session_id)
                );
                CREATE TABLE IF NOT EXISTS binance_spot_functional_terminal_truth (
                    session_id TEXT PRIMARY KEY,
                    truth_json TEXT NOT NULL,
                    truth_hash TEXT NOT NULL UNIQUE,
                    observed_epoch REAL NOT NULL,
                    stream_journal_seal_hash TEXT NOT NULL,
                    stream_journal_event_count INTEGER NOT NULL,
                    FOREIGN KEY(session_id)
                        REFERENCES binance_spot_functional_sessions(session_id)
                );
                CREATE TABLE IF NOT EXISTS binance_spot_functional_strategy_evaluations (
                    evaluation_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    bar_close_epoch REAL NOT NULL,
                    signal TEXT NOT NULL,
                    evaluation_json TEXT NOT NULL,
                    evaluation_hash TEXT NOT NULL UNIQUE,
                    window_json TEXT NOT NULL,
                    window_hash TEXT NOT NULL,
                    created_epoch REAL NOT NULL,
                    UNIQUE(session_id, bar_close_epoch),
                    FOREIGN KEY(session_id)
                        REFERENCES binance_spot_functional_sessions(session_id)
                );
                """
            )
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(binance_spot_functional_sessions)"
                ).fetchall()
            }
            if "cleanup_sell_claimed" not in columns:
                connection.execute(
                    "ALTER TABLE binance_spot_functional_sessions "
                    "ADD COLUMN cleanup_sell_claimed INTEGER NOT NULL DEFAULT 0"
                )
            if "exclusivity_coverage_started_epoch" not in columns:
                connection.execute(
                    "ALTER TABLE binance_spot_functional_sessions "
                    "ADD COLUMN exclusivity_coverage_started_epoch "
                    "REAL NOT NULL DEFAULT 0"
                )
            if "cleanup_recovery_used" not in columns:
                connection.execute(
                    "ALTER TABLE binance_spot_functional_sessions "
                    "ADD COLUMN cleanup_recovery_used INTEGER NOT NULL DEFAULT 0"
                )
            if "final_evidence_json" not in columns:
                connection.execute(
                    "ALTER TABLE binance_spot_functional_sessions "
                    "ADD COLUMN final_evidence_json TEXT NOT NULL DEFAULT ''"
                )
            if "final_evidence_hash" not in columns:
                connection.execute(
                    "ALTER TABLE binance_spot_functional_sessions "
                    "ADD COLUMN final_evidence_hash TEXT NOT NULL DEFAULT ''"
                )
            action_columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(binance_spot_functional_actions)"
                ).fetchall()
            }
            for name, declaration in {
                "pre_base_total": "TEXT NOT NULL DEFAULT ''",
                "pre_quote_total": "TEXT NOT NULL DEFAULT ''",
                "absence_proof_count": "INTEGER NOT NULL DEFAULT 0",
                "absence_first_epoch": "REAL NOT NULL DEFAULT 0",
                "absence_last_epoch": "REAL NOT NULL DEFAULT 0",
                "absence_proof_hash": "TEXT NOT NULL DEFAULT ''",
                "post_marker_epoch": "REAL NOT NULL DEFAULT 0",
            }.items():
                if name not in action_columns:
                    connection.execute(
                        "ALTER TABLE binance_spot_functional_actions "
                        f"ADD COLUMN {name} {declaration}"
                    )
            connection.commit()

    def record_strategy_evaluation(
        self,
        session_id: str,
        signal: FinalizedBarSignal,
        *,
        now_epoch: float,
    ) -> dict[str, Any]:
        evaluation = dict(signal.evaluation)
        window = evaluation.get("officialWindow")
        if not isinstance(window, Mapping):
            raise BinanceSpotBoundaryBlocked(
                "natural signal lacks its exact official finalized window"
            )
        window_value = dict(window)
        window_hash = _stable_hash(window_value)
        evaluation_hash = _stable_hash(evaluation)
        if (
            _text(evaluation.get("officialWindowHash")).lower() != window_hash
            or _text(evaluation.get("barHash")).lower() != window_hash
            or _text(evaluation.get("evaluationId")) != signal.evaluation_id
            or _upper(evaluation.get("signal")) != signal.signal
        ):
            raise BinanceSpotBoundaryBlocked(
                "natural signal official window/evaluation hash changed"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                """SELECT state,last_bar_close_epoch,permit_id,permit_hash,
                binding_json,binding_hash FROM
                binance_spot_functional_sessions WHERE session_id=?""",
                (_text(session_id),),
            ).fetchone()
            if (
                session is None
                or _upper(session["state"]) != "RUNNING"
                or float(signal.close_epoch)
                <= float(session["last_bar_close_epoch"] or 0)
            ):
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "strategy evaluation cannot bind outside RUNNING"
                )
            try:
                session_binding = json.loads(_text(session["binding_json"]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "strategy evaluation session binding is malformed"
                ) from exc
            if (
                not isinstance(session_binding, Mapping)
                or _stable_hash(dict(session_binding))
                != _text(session["binding_hash"]).lower()
            ):
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "strategy evaluation session binding changed"
                )
            evaluation.update(
                {
                    "sessionId": _text(session_id),
                    "permitId": _text(session["permit_id"]),
                    "permitHash": _text(session["permit_hash"]).lower(),
                    "accountFingerprint": _text(
                        session_binding.get("accountFingerprint")
                    ).lower(),
                    "bindingHash": _text(session["binding_hash"]).lower(),
                }
            )
            evaluation_hash = _stable_hash(evaluation)
            try:
                connection.execute(
                    """INSERT INTO binance_spot_functional_strategy_evaluations (
                    evaluation_id,session_id,bar_close_epoch,signal,
                    evaluation_json,evaluation_hash,window_json,window_hash,
                    created_epoch) VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        signal.evaluation_id,
                        _text(session_id),
                        float(signal.close_epoch),
                        signal.signal,
                        _canonical_json(evaluation),
                        evaluation_hash,
                        _canonical_json(window_value),
                        window_hash,
                        float(now_epoch),
                    ),
                )
                connection.execute(
                    """UPDATE binance_spot_functional_sessions
                    SET last_bar_close_epoch=? WHERE session_id=?
                    AND state='RUNNING' AND last_bar_close_epoch<?""",
                    (
                        float(signal.close_epoch),
                        _text(session_id),
                        float(signal.close_epoch),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "strategy evaluation is duplicate or out of order"
                ) from exc
            connection.commit()
        return {
            "evaluationId": signal.evaluation_id,
            "evaluationHash": evaluation_hash,
            "windowHash": window_hash,
        }

    def create_session(
        self,
        permit: ExactPermit,
        truth: AccountTruth,
        *,
        now_epoch: float,
        activation_fence: Mapping[str, Any] | None = None,
        session_id: str | None = None,
        exclusivity_coverage_started_epoch: float | None = None,
    ) -> tuple[dict[str, Any], str]:
        session_id = _text(session_id) or f"bnsft-{secrets.token_hex(16)}"
        if not session_id.startswith("bnsft-") or _SAFE_ID_RE.fullmatch(session_id) is None:
            raise BinanceSpotBoundaryBlocked("functional session identity is invalid")
        capability = secrets.token_urlsafe(32)
        capability_hash = hashlib.sha256(capability.encode("utf-8")).hexdigest()
        binding_json = _canonical_json(permit.binding.payload())
        binding_hash = _stable_hash(permit.binding.payload())
        baseline_ids = sorted(
            _text(row.get("clientOrderId") or row.get("orderId"))
            for row in truth.open_orders
        )
        coverage_started = (
            truth.history_baseline_epoch
            if exclusivity_coverage_started_epoch is None
            else float(exclusivity_coverage_started_epoch)
        )
        if (
            not math.isfinite(coverage_started)
            or coverage_started < permit.issued_epoch - 1.0
            or coverage_started > truth.observed_epoch
        ):
            raise BinanceSpotBoundaryBlocked(
                "session exclusivity coverage epoch is invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                if activation_fence is not None:
                    control = connection.execute(
                        """
                        SELECT phase, permit_id, permit_hash, session_id,
                               revision, owner_id, owner_token_hash,
                               owner_lease_expires_epoch
                        FROM binance_spot_functional_control
                        WHERE route_key=?
                        """,
                        (
                            _text(
                                activation_fence.get(
                                    "routeKey",
                                    "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m",
                                )
                            ),
                        ),
                    ).fetchone()
                    expected_revision = int(
                        activation_fence.get("revision") or -1
                    )
                    expected_planned_session = _text(
                        activation_fence.get("plannedSessionId")
                    )
                    if (
                        control is None
                        or _upper(control["phase"]) != "ARMED"
                        or _text(control["permit_id"]) != permit.permit_id
                        or not secrets.compare_digest(
                            _text(control["permit_hash"]), permit.permit_hash
                        )
                        or _text(control["session_id"])
                        != expected_planned_session
                        or int(control["revision"]) != expected_revision
                        or _text(control["owner_id"])
                        != _text(activation_fence.get("ownerId"))
                        or not secrets.compare_digest(
                            _text(control["owner_token_hash"]),
                            _text(activation_fence.get("ownerTokenHash")),
                        )
                        or float(now_epoch)
                        >= float(control["owner_lease_expires_epoch"])
                    ):
                        connection.rollback()
                        raise BinanceSpotBoundaryBlocked(
                            "startup activation fence changed before session create"
                        )
                connection.execute(
                    """
                    INSERT INTO binance_spot_functional_sessions (
                        session_id, permit_id, permit_hash, binding_json,
                        binding_hash, state, capability_hash,
                        capability_seal_hash, baseline_base, baseline_quote,
                        baseline_open_ids_json,
                        exclusivity_coverage_started_epoch,
                        started_epoch, expires_epoch,
                        cleanup_deadline_epoch
                    ) VALUES (?, ?, ?, ?, ?, 'RUNNING', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        permit.permit_id,
                        permit.permit_hash,
                        binding_json,
                        binding_hash,
                        capability_hash,
                        capability_hash,
                        _decimal_text(truth.base_total),
                        _decimal_text(truth.quote_total),
                        _canonical_json(baseline_ids),
                        coverage_started,
                        truth.history_baseline_epoch,
                        permit.expires_epoch,
                        permit.cleanup_deadline_epoch,
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "another functional session is active for this exact route"
                ) from exc
        return self.session(session_id), capability

    def nonterminal_sessions(self) -> list[dict[str, Any]]:
        """Return every route session that still requires startup ownership."""

        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM binance_spot_functional_sessions
                WHERE state IN ('RUNNING','CLEANUP','RECONCILIATION_REQUIRED')
                ORDER BY started_epoch, session_id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def session(self, session_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
        if row is None:
            raise BinanceSpotFunctionalError("functional session does not exist")
        return dict(row)

    def assert_capability(self, session: Mapping[str, Any], capability: str) -> str:
        supplied_hash = hashlib.sha256(_text(capability).encode("utf-8")).hexdigest()
        stored = _text(session.get("capability_hash"))
        if not stored or not secrets.compare_digest(stored, supplied_hash):
            raise BinanceSpotBoundaryBlocked("functional session capability is invalid/revoked")
        return supplied_hash

    def set_session(
        self,
        session_id: str,
        *,
        state: str | None = None,
        last_bar_close_epoch: float | None = None,
        cleanup_started: bool | None = None,
        cleanup_recovery_used: bool | None = None,
        finalize: bool = False,
        final_new_entries_blocked: bool = False,
        detail: str = "",
        now_epoch: float | None = None,
    ) -> dict[str, Any]:
        fields: list[str] = []
        params: list[Any] = []
        if state is not None:
            fields.append("state=?")
            params.append(_upper(state))
        if last_bar_close_epoch is not None:
            fields.append("last_bar_close_epoch=?")
            params.append(float(last_bar_close_epoch))
        if cleanup_started is not None:
            fields.append("cleanup_started=?")
            params.append(1 if cleanup_started else 0)
        if cleanup_recovery_used is not None:
            fields.append("cleanup_recovery_used=?")
            params.append(1 if cleanup_recovery_used else 0)
        if detail:
            fields.append("detail=?")
            params.append(_text(detail)[:1000])
        if finalize:
            fields.extend(
                [
                    "state='FINALIZED'",
                    "capability_hash=''",
                    "finalized_epoch=?",
                    "final_new_entries_blocked=?",
                ]
            )
            params.extend([float(now_epoch or time.time()), 1 if final_new_entries_blocked else 0])
        if not fields:
            return self.session(session_id)
        params.append(_text(session_id))
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                f"UPDATE binance_spot_functional_sessions SET {', '.join(fields)} WHERE session_id=?",
                params,
            )
            if cursor.rowcount != 1:
                raise BinanceSpotFunctionalError("functional session does not exist")
            connection.commit()
        return self.session(session_id)

    def finalize_with_evidence(
        self,
        session_id: str,
        *,
        evidence: Mapping[str, Any],
        terminal_truth: Mapping[str, Any],
        now_epoch: float,
        detail: str,
    ) -> dict[str, Any]:
        """Atomically revoke capability and persist immutable final evidence."""

        canonical = _canonical_json(dict(evidence))
        evidence_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        truth_value = dict(terminal_truth)
        truth_canonical = _canonical_json(truth_value)
        truth_hash = hashlib.sha256(truth_canonical.encode("utf-8")).hexdigest()
        if (
            _text(truth_value.get("schemaVersion"))
            != "binance-spot-functional-terminal-official-truth/v1"
            or _text(truth_value.get("sessionId")) != _text(session_id)
            or _text(evidence.get("terminalOfficialTruthHash")).lower()
            != truth_hash
        ):
            raise BinanceSpotBoundaryBlocked(
                "terminal official truth identity/hash is invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise BinanceSpotFunctionalError("functional session does not exist")
            if _upper(row["state"]) == "FINALIZED":
                sealed_truth = connection.execute(
                    """SELECT truth_json,truth_hash FROM
                    binance_spot_functional_terminal_truth WHERE session_id=?""",
                    (_text(session_id),),
                ).fetchone()
                if (
                    _text(row["final_evidence_hash"]) == evidence_hash
                    and _text(row["final_evidence_json"]) == canonical
                    and sealed_truth is not None
                    and _text(sealed_truth["truth_hash"]) == truth_hash
                    and _text(sealed_truth["truth_json"]) == truth_canonical
                ):
                    connection.commit()
                    return dict(row)
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "final evidence is immutable and already sealed"
                )
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='FINALIZED', capability_hash='', finalized_epoch=?,
                    final_new_entries_blocked=1, final_evidence_json=?,
                    final_evidence_hash=?, detail=?
                WHERE session_id=? AND state IN (
                    'RUNNING','CLEANUP','RECONCILIATION_REQUIRED'
                ) AND final_evidence_hash=''
                """,
                (
                    float(now_epoch),
                    canonical,
                    evidence_hash,
                    _text(detail)[:1000],
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "session is not eligible for final evidence seal"
                )
            connection.execute(
                """INSERT INTO binance_spot_functional_terminal_truth (
                    session_id,truth_json,truth_hash,observed_epoch,
                    stream_journal_seal_hash,stream_journal_event_count
                ) VALUES (?,?,?,?,?,?)""",
                (
                    _text(session_id),
                    truth_canonical,
                    truth_hash,
                    float(truth_value.get("observedEpoch") or 0),
                    _text(truth_value.get("streamJournalSealHash")).lower(),
                    int(truth_value.get("streamJournalEventCount") or 0),
                ),
            )
            connection.commit()
        return self.session(session_id)

    def prepare_final_with_evidence(
        self,
        session_id: str,
        *,
        evidence: Mapping[str, Any],
        terminal_truth: Mapping[str, Any],
        now_epoch: float,
        detail: str,
    ) -> dict[str, Any]:
        """Persist immutable evidence while keeping route terminal-pending."""

        canonical = _canonical_json(dict(evidence))
        evidence_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        truth_value = dict(terminal_truth)
        truth_canonical = _canonical_json(truth_value)
        truth_hash = hashlib.sha256(truth_canonical.encode("utf-8")).hexdigest()
        if (
            _text(truth_value.get("schemaVersion"))
            != "binance-spot-functional-terminal-official-truth/v1"
            or _text(truth_value.get("sessionId")) != _text(session_id)
            or _text(evidence.get("terminalOfficialTruthHash")).lower()
            != truth_hash
        ):
            raise BinanceSpotBoundaryBlocked(
                "terminal official truth identity/hash is invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise BinanceSpotFunctionalError("functional session does not exist")
            if _upper(row["state"]) == "FINAL_PREPARED":
                sealed_truth = connection.execute(
                    """SELECT truth_json,truth_hash FROM
                    binance_spot_functional_terminal_truth WHERE session_id=?""",
                    (_text(session_id),),
                ).fetchone()
                if (
                    _text(row["final_evidence_hash"]) == evidence_hash
                    and _text(row["final_evidence_json"]) == canonical
                    and sealed_truth is not None
                    and _text(sealed_truth["truth_hash"]) == truth_hash
                    and _text(sealed_truth["truth_json"]) == truth_canonical
                ):
                    connection.commit()
                    return dict(row)
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "prepared final evidence is immutable"
                )
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='FINAL_PREPARED', capability_hash='',
                    final_new_entries_blocked=1, final_evidence_json=?,
                    final_evidence_hash=?, detail=?
                WHERE session_id=? AND state IN (
                    'RUNNING','CLEANUP','RECONCILIATION_REQUIRED'
                ) AND final_evidence_hash=''
                """,
                (
                    canonical,
                    evidence_hash,
                    _text(detail)[:1000],
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "session is not eligible for prepared final evidence"
                )
            connection.execute(
                """INSERT INTO binance_spot_functional_terminal_truth (
                    session_id,truth_json,truth_hash,observed_epoch,
                    stream_journal_seal_hash,stream_journal_event_count
                ) VALUES (?,?,?,?,?,?)""",
                (
                    _text(session_id),
                    truth_canonical,
                    truth_hash,
                    float(truth_value.get("observedEpoch") or 0),
                    _text(truth_value.get("streamJournalSealHash")).lower(),
                    int(truth_value.get("streamJournalEventCount") or 0),
                ),
            )
            connection.commit()
        return self.session(session_id)

    def commit_prepared_final(
        self, session_id: str, *, now_epoch: float
    ) -> dict[str, Any]:
        """Commit a previously hashed final only after stream retirement."""

        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='FINALIZED', finalized_epoch=?,
                    detail='prepared evidence and exact stream retirement sealed'
                WHERE session_id=? AND state='FINAL_PREPARED'
                    AND capability_hash='' AND final_evidence_hash!=''
                """,
                (float(now_epoch), _text(session_id)),
            )
            if cursor.rowcount != 1:
                row = connection.execute(
                    """
                    SELECT * FROM binance_spot_functional_sessions
                    WHERE session_id=?
                    """,
                    (_text(session_id),),
                ).fetchone()
                if row is not None and _upper(row["state"]) == "FINALIZED":
                    connection.commit()
                    return dict(row)
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "prepared final commit state changed"
                )
            connection.commit()
        return self.session(session_id)

    def final_evidence(self, session_id: str) -> dict[str, Any]:
        session = self.session(session_id)
        canonical = _text(session.get("final_evidence_json"))
        expected = _text(session.get("final_evidence_hash")).lower()
        if not canonical or not _SHA256_RE.fullmatch(expected):
            raise BinanceSpotBoundaryBlocked("durable final evidence is absent")
        actual = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if not secrets.compare_digest(actual, expected):
            raise BinanceSpotBoundaryBlocked("durable final evidence hash changed")
        value = json.loads(canonical)
        if not isinstance(value, dict):
            raise BinanceSpotBoundaryBlocked("durable final evidence is malformed")
        with closing(self._connect()) as connection:
            terminal_row = connection.execute(
                """SELECT truth_json,truth_hash FROM
                binance_spot_functional_terminal_truth WHERE session_id=?""",
                (_text(session_id),),
            ).fetchone()
        result: dict[str, Any] = {"evidence": value, "evidenceHash": expected}
        if terminal_row is not None:
            terminal_raw = _text(terminal_row["truth_json"])
            terminal_hash = _text(terminal_row["truth_hash"]).lower()
            if (
                not terminal_raw
                or not _SHA256_RE.fullmatch(terminal_hash)
                or not secrets.compare_digest(
                    hashlib.sha256(terminal_raw.encode("utf-8")).hexdigest(),
                    terminal_hash,
                )
            ):
                raise BinanceSpotBoundaryBlocked(
                    "durable terminal official truth hash changed"
                )
            terminal_value = json.loads(terminal_raw)
            if not isinstance(terminal_value, dict):
                raise BinanceSpotBoundaryBlocked(
                    "durable terminal official truth is malformed"
                )
            result.update(
                {
                    "terminalOfficialTruth": terminal_value,
                    "terminalOfficialTruthHash": terminal_hash,
                }
            )
        return result

    def abort_session_before_activation(
        self,
        session_id: str,
        *,
        detail: str,
        now_epoch: float,
        attestation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Revoke a just-created session only when it has no claimed action."""

        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            action_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM binance_spot_functional_actions WHERE session_id=?",
                    (_text(session_id),),
                ).fetchone()[0]
            )
            if action_count:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "session with durable actions cannot use pre-activation abort"
                )
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "session is not eligible for pre-activation abort"
                )
            evidence = {
                "schemaVersion": SCHEMA_VERSION,
                "sessionId": _text(session_id),
                "permitId": _text(row["permit_id"]),
                "permitHash": _text(row["permit_hash"]),
                "bindingHash": _text(row["binding_hash"]),
                "outcome": "START_FAILED_BEFORE_ACTIVATION",
                "functionalCapabilityReset": True,
                "newEntriesBlocked": True,
                "promotionEligible": False,
                "useAsPromotionEvidence": False,
                "detail": _text(detail)[:1000],
            }
            if attestation is not None:
                sealed_attestation = dict(attestation)
                if (
                    sealed_attestation.get("startupAbortAttestation") is not True
                    or not _SHA256_RE.fullmatch(
                        _text(sealed_attestation.get("officialTruthHash")).lower()
                    )
                    or sealed_attestation.get("baselineBalancesUnchanged") is not True
                    or sealed_attestation.get("baselineWorkingOrdersUnchanged") is not True
                    or sealed_attestation.get("ownedOrderFillActivityAbsent") is not True
                ):
                    connection.rollback()
                    raise BinanceSpotBoundaryBlocked(
                        "pre-activation abort attestation is incomplete"
                    )
                evidence["startupAbortAttestation"] = sealed_attestation
            canonical = _canonical_json(evidence)
            evidence_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='FAILED', capability_hash='', finalized_epoch=?,
                    final_new_entries_blocked=1, final_evidence_json=?,
                    final_evidence_hash=?, detail=?
                WHERE session_id=? AND state='RUNNING'
                """,
                (
                    float(now_epoch),
                    canonical,
                    evidence_hash,
                    _text(detail)[:1000],
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "session is not eligible for pre-activation abort"
                )
            connection.commit()
        return self.session(session_id)

    def claim_action(
        self,
        session_id: str,
        action: Mapping[str, Any],
        *,
        now_epoch: float,
        pre_base_total: Decimal | None = None,
        pre_quote_total: Decimal | None = None,
    ) -> dict[str, Any]:
        kind = _upper(action.get("kind"))
        if kind not in {"BUY", "SELL", "CANCEL"}:
            raise BinanceSpotFunctionalError("invalid functional action kind")
        client_order_id = _text(action.get("clientOrderId"))
        if kind == "SELL" and action.get("cleanupOnly") is True:
            action_kind = _cleanup_action_kind(
                "CLEANUP_SELL",
                _cleanup_generation_from_client_id(client_order_id, prefix="f"),
            )
        elif kind == "CANCEL":
            action_kind = _cleanup_action_kind(
                "CANCEL",
                _cleanup_generation_from_client_id(client_order_id, prefix="c"),
            )
        else:
            action_kind = kind
        action_json = _canonical_json(dict(action))
        claim_id = _stable_hash({"sessionId": session_id, "action": dict(action)})
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                session = connection.execute(
                    "SELECT * FROM binance_spot_functional_sessions WHERE session_id=?",
                    (_text(session_id),),
                ).fetchone()
                if session is None:
                    raise BinanceSpotFunctionalError("functional session does not exist")
                if kind == "BUY" and int(session["buy_claimed"]) != 0:
                    raise DuplicateActionClaim("BUY was already claimed; re-entry is forbidden")
                if action_kind == "SELL" and int(session["sell_claimed"]) != 0:
                    raise DuplicateActionClaim("SELL was already claimed")
                cleanup_count = int(session["cleanup_sell_claimed"])
                cleanup_action_count = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM binance_spot_functional_actions
                        WHERE session_id=? AND (
                            action_kind LIKE 'CLEANUP_SELL%'
                            OR action_kind LIKE 'CANCEL%'
                        )
                        """,
                        (session_id,),
                    ).fetchone()[0]
                )
                if (
                    action_kind.startswith("CLEANUP_SELL")
                    or action_kind.startswith("CANCEL")
                ) and cleanup_action_count >= MAX_CLEANUP_ACTIONS:
                    raise DuplicateActionClaim("bounded cleanup action budget is exhausted")
                if action_kind.startswith("CLEANUP_SELL"):
                    expected_kind = _cleanup_action_kind(
                        "CLEANUP_SELL", cleanup_count + 1
                    )
                    if action_kind != expected_kind:
                        raise DuplicateActionClaim(
                            "cleanup SELL generation cap/order changed"
                        )
                if action_kind.startswith("CANCEL"):
                    prior_cancels = connection.execute(
                        """
                        SELECT sealed_action_json FROM binance_spot_functional_actions
                        WHERE session_id=? AND action_kind LIKE 'CANCEL%'
                        ORDER BY created_epoch, claim_id
                        """,
                        (session_id,),
                    ).fetchall()
                    expected_kind = _cleanup_action_kind(
                        "CANCEL", len(prior_cancels) + 1
                    )
                    if action_kind != expected_kind:
                        raise DuplicateActionClaim(
                            "cleanup CANCEL generation cap/order changed"
                        )
                    target = _text(action.get("origClientOrderId"))
                    if any(
                        _text(json.loads(row["sealed_action_json"]).get("origClientOrderId"))
                        == target
                        for row in prior_cancels
                    ):
                        raise DuplicateActionClaim(
                            "an exact owned target can be canceled only once"
                        )
                connection.execute(
                    """
                    INSERT INTO binance_spot_functional_actions (
                        claim_id, session_id, action_kind, client_order_id,
                        state, sealed_action_json, pre_base_total,
                        pre_quote_total, created_epoch, updated_epoch
                    ) VALUES (?, ?, ?, ?, 'CLAIMED', ?, ?, ?, ?, ?)
                    """,
                    (
                        claim_id,
                        session_id,
                        action_kind,
                        client_order_id,
                        action_json,
                        (
                            _decimal_text(pre_base_total)
                            if pre_base_total is not None
                            else ""
                        ),
                        (
                            _decimal_text(pre_quote_total)
                            if pre_quote_total is not None
                            else ""
                        ),
                        float(now_epoch),
                        float(now_epoch),
                    ),
                )
                if kind == "BUY":
                    connection.execute(
                        "UPDATE binance_spot_functional_sessions SET buy_claimed=1 WHERE session_id=?",
                        (session_id,),
                    )
                elif action_kind == "SELL":
                    connection.execute(
                        "UPDATE binance_spot_functional_sessions SET sell_claimed=1 WHERE session_id=?",
                        (session_id,),
                    )
                elif action_kind.startswith("CLEANUP_SELL"):
                    connection.execute(
                        """
                        UPDATE binance_spot_functional_sessions
                        SET cleanup_sell_claimed=cleanup_sell_claimed+1
                        WHERE session_id=?
                        """,
                        (session_id,),
                    )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise DuplicateActionClaim(
                    "exact functional action is already durably claimed"
                ) from exc
        return self.action(claim_id)

    def record_ambiguous_absence_observation(
        self,
        claim_id: str,
        *,
        expected_state: str,
        observed_epoch: float,
        proof: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one spaced, immutable broker nonacceptance observation.

        The rolling proof hash prevents a restart from silently replacing a
        prior observation.  Two fresh observations are required before the
        action can be terminalized without ever retrying the mutation.
        """

        canonical = _canonical_json(dict(proof))
        now = float(observed_epoch)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_actions WHERE claim_id=?",
                (_text(claim_id),),
            ).fetchone()
            if row is None or _upper(row["state"]) != _upper(expected_state):
                connection.rollback()
                raise DuplicateActionClaim(
                    "ambiguous action state changed; nonacceptance proof cannot be appended"
                )
            count = int(row["absence_proof_count"] or 0)
            last = float(row["absence_last_epoch"] or 0)
            if count and now - last < AMBIGUOUS_NONACCEPTANCE_MIN_SPACING_SECONDS:
                connection.rollback()
                raise BinanceSpotBoundaryBlocked(
                    "ambiguous nonacceptance observations are not sufficiently spaced"
                )
            prior_hash = _text(row["absence_proof_hash"]).lower()
            rolling_hash = hashlib.sha256(
                (prior_hash + "\n" + canonical).encode("utf-8")
            ).hexdigest()
            first = float(row["absence_first_epoch"] or 0) or now
            count += 1
            terminal = count >= AMBIGUOUS_NONACCEPTANCE_MIN_OBSERVATIONS
            new_state = (
                "AMBIGUOUS_PROVEN_NOT_ACCEPTED" if terminal else _upper(expected_state)
            )
            connection.execute(
                """
                UPDATE binance_spot_functional_actions
                SET state=?, absence_proof_count=?, absence_first_epoch=?,
                    absence_last_epoch=?, absence_proof_hash=?, updated_epoch=?,
                    detail=?
                WHERE claim_id=? AND state=?
                """,
                (
                    new_state,
                    count,
                    first,
                    now,
                    rolling_hash,
                    now,
                    (
                        "delayed complete broker nonacceptance observations sealed; no retry"
                        if terminal
                        else "first delayed complete broker nonacceptance observation sealed"
                    ),
                    _text(claim_id),
                    _upper(expected_state),
                ),
            )
            connection.commit()
        return self.action(claim_id)

    def action(self, claim_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_actions WHERE claim_id=?",
                (_text(claim_id),),
            ).fetchone()
        if row is None:
            raise BinanceSpotFunctionalError("functional action claim does not exist")
        return dict(row)

    def actions(self, session_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM binance_spot_functional_actions WHERE session_id=? ORDER BY created_epoch, claim_id",
                (_text(session_id),),
            ).fetchall()
        return [dict(row) for row in rows]

    def transition_action(
        self,
        claim_id: str,
        *,
        expected_state: str,
        state: str,
        now_epoch: float,
        response: Mapping[str, Any] | None = None,
        broker_order_id: str = "",
        detail: str = "",
    ) -> dict[str, Any]:
        response_hash = _stable_hash(dict(response)) if response is not None else ""
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_actions
                SET state=?, response_hash=?, broker_order_id=?, detail=?, updated_epoch=?
                WHERE claim_id=? AND state=?
                """,
                (
                    _upper(state),
                    response_hash,
                    _text(broker_order_id),
                    _text(detail)[:1000],
                    float(now_epoch),
                    _text(claim_id),
                    _upper(expected_state),
                ),
            )
            if cursor.rowcount != 1:
                raise DuplicateActionClaim(
                    "action is not in the expected state; blind retry blocked"
                )
            connection.commit()
        return self.action(claim_id)

    def mark_post_may_have_crossed(
        self, claim_id: str, *, now_epoch: float | None = None
    ) -> None:
        """Backend-owned one-time CAS immediately before mutation transport."""
        marker_epoch = float(time.time() if now_epoch is None else now_epoch)
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """UPDATE binance_spot_functional_actions
                SET state='POST_MAY_HAVE_CROSSED',
                    detail='production mutation edge durable boundary marker',
                    post_marker_epoch=?, updated_epoch=?
                WHERE claim_id=? AND state='SUBMITTING'
                AND post_marker_epoch=0""",
                (marker_epoch, marker_epoch, _text(claim_id)),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise DuplicateActionClaim(
                    "action is not SUBMITTING or its POST marker already exists"
                )
            connection.commit()


def _order_client_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("clientOrderId") or row.get("origClientOrderId"))


def _owned_prefix(session_id: str) -> str:
    return f"ftb-{hashlib.sha256(session_id.encode()).hexdigest()[:12]}-"


def _client_order_id(session_id: str, kind: str) -> str:
    normalized = _upper(kind)
    if normalized in {"BUY", "SELL"}:
        suffix = {"BUY": "b", "SELL": "s"}[normalized]
    else:
        match = re.fullmatch(r"(CLEANUP_SELL|CANCEL)(?:_([2-9]|1[0-2]))?", normalized)
        if match is None:
            raise BinanceSpotFunctionalError("cleanup action generation is invalid")
        generation = int(match.group(2) or 1)
        prefix = "f" if match.group(1) == "CLEANUP_SELL" else "c"
        suffix = prefix if generation == 1 else f"{prefix}{generation}"
    return f"{_owned_prefix(session_id)}{suffix}"


def _cleanup_action_kind(kind: str, generation: int) -> str:
    normalized = _upper(kind)
    if normalized not in {"CLEANUP_SELL", "CANCEL"}:
        raise BinanceSpotFunctionalError("cleanup action kind is invalid")
    if generation < 1 or generation > MAX_CLEANUP_ACTIONS:
        raise BinanceSpotFunctionalError("cleanup action generation is outside the cap")
    return normalized if generation == 1 else f"{normalized}_{generation}"


def _cleanup_generation_from_client_id(client_order_id: str, *, prefix: str) -> int:
    match = re.search(rf"-{re.escape(prefix)}([2-9]|1[0-2])?$", _text(client_order_id))
    if match is None:
        raise BinanceSpotFunctionalError("cleanup client order id generation changed")
    return int(match.group(1) or 1)


def _owned_rows(truth: AccountTruth, session_id: str) -> list[Mapping[str, Any]]:
    prefix = _owned_prefix(session_id)
    return [
        row
        for row in (*truth.open_orders, *truth.closed_orders)
        if _order_client_id(row).startswith(prefix)
    ]


def _validate_order_row(row: Mapping[str, Any], *, binding: ExactBinding) -> None:
    if _upper(row.get("symbol")) != binding.symbol:
        raise BinanceSpotFunctionalError("owned order symbol changed")
    if _upper(row.get("product")) != "SPOT":
        raise BinanceSpotFunctionalError("owned order product is not SPOT")
    if row.get("isMargin") is True or row.get("reduceOnly") is True:
        raise BinanceSpotFunctionalError("margin/futures order shape is forbidden")
    if _upper(row.get("side")) not in {"BUY", "SELL"}:
        raise BinanceSpotFunctionalError("owned order side is invalid")
    if _upper(row.get("status")) not in _ALL_ORDER_STATES:
        raise BinanceSpotFunctionalError("owned order status is incomplete")


def _validate_receipt(
    action: Mapping[str, Any], receipt: Mapping[str, Any]
) -> str:
    broker_order_id = _text(receipt.get("orderId"))
    if not broker_order_id or _upper(receipt.get("symbol")) != SYMBOL:
        raise BinanceSpotFunctionalError("broker receipt identity is incomplete")
    status = _upper(receipt.get("status"))
    if status not in _ALL_ORDER_STATES:
        raise BinanceSpotFunctionalError("broker receipt status is incomplete")
    kind = _upper(action.get("kind"))
    if kind == "CANCEL":
        if (
            _text(receipt.get("origClientOrderId") or receipt.get("clientOrderId"))
            != _text(action.get("origClientOrderId"))
            or broker_order_id != _text(action.get("brokerOrderId"))
        ):
            raise BinanceSpotFunctionalError("cancel receipt target changed")
        if status not in _TERMINAL_ORDER_STATES:
            raise BinanceSpotFunctionalError("cancel receipt is not terminal")
        return broker_order_id
    if (
        _text(receipt.get("clientOrderId")) != _text(action.get("clientOrderId"))
        or _upper(receipt.get("side")) != kind
        or _upper(receipt.get("type")) != "MARKET"
    ):
        raise BinanceSpotFunctionalError("broker receipt changed the sealed order")
    return broker_order_id


def owner_metrics(
    truth: AccountTruth,
    *,
    session_id: str,
    baseline_base: Decimal,
    allow_cleanup_recovery: bool = False,
) -> dict[str, Any]:
    """Return owner-attributed position/gross/PnL from complete fill+fee truth."""

    prefix = _owned_prefix(session_id)
    seen_trade_ids: set[str] = set()
    bought = Decimal("0")
    sold = Decimal("0")
    buy_quote = Decimal("0")
    sell_quote = Decimal("0")
    fees_quote = Decimal("0")
    cash_fees_quote = Decimal("0")
    base_fees = Decimal("0")
    fee_quote_exact = True
    third_asset_fees: list[dict[str, str]] = []
    for fill in truth.fills:
        client_id = _order_client_id(fill)
        if not client_id.startswith(prefix):
            continue
        trade_id = _text(fill.get("tradeId"))
        if not trade_id or trade_id in seen_trade_ids:
            raise BinanceSpotFunctionalError("owned fills require unique tradeId")
        seen_trade_ids.add(trade_id)
        if _upper(fill.get("symbol")) != SYMBOL:
            raise BinanceSpotFunctionalError("owned fill symbol changed")
        side = _upper(fill.get("side"))
        if side not in {"BUY", "SELL"}:
            raise BinanceSpotFunctionalError("owned fill side is invalid")
        qty = _decimal(fill.get("quantity"), label="fill quantity")
        quote = _decimal(fill.get("quoteQuantity"), label="fill quoteQuantity")
        fee = _decimal(fill.get("commission"), label="fill commission")
        fee_asset = _upper(fill.get("commissionAsset"))
        exact_fee_quote = fill.get(
            "feeQuoteValueExact", fee_asset in {BASE_ASSET, QUOTE_ASSET}
        ) is True
        fee_quote = (
            _decimal(fill.get("feeQuoteValue"), label="fill feeQuoteValue")
            if exact_fee_quote
            else Decimal("0")
        )
        if qty <= 0 or quote <= 0 or fee < 0 or fee_quote < 0 or not fee_asset:
            raise BinanceSpotFunctionalError("owned fill/fee truth is malformed")
        fees_quote += fee_quote
        # A BTC-denominated commission already reduces ``owned_qty`` below and
        # therefore its value appears through less mark value / lower eventual
        # SELL proceeds.  Subtracting its quote valuation here as well would
        # double-count the same economic loss.  Quote commissions, by
        # contrast, are a separate cash outflow and must be subtracted.
        if fee_asset == QUOTE_ASSET:
            cash_fees_quote += fee_quote
        if not exact_fee_quote:
            fee_quote_exact = False
            third_asset_fees.append(
                {
                    "tradeId": trade_id,
                    "asset": fee_asset,
                    "amount": _decimal_text(fee),
                }
            )
        if fee_asset == BASE_ASSET:
            base_fees += fee
        if side == "BUY":
            bought += qty
            buy_quote += quote
        else:
            sold += qty
            sell_quote += quote
    owned_qty = bought - sold - base_fees
    if owned_qty < 0:
        raise BinanceSpotFunctionalError("owned fill ledger implies a short position")
    account_delta = truth.base_total - baseline_base
    if truth.cleanup_recovery_only:
        if not allow_cleanup_recovery:
            raise BinanceSpotFunctionalError(
                "REST-reconciled truth is cleanup-only"
            )
        # The fill ledger caps the most that this lane may sell.  Requiring the
        # current total minus that exact owned delta to remain at/above the
        # pre-existing baseline prevents a cleanup from consuming user BTC
        # even when the broken stream cannot prove absence of later deposits.
        if account_delta < owned_qty:
            raise BinanceSpotFunctionalError(
                "REST cleanup cannot preserve the pre-existing BTC baseline"
            )
    else:
        # Without absence of external account activity, balance delta cannot
        # be safely attributed to this functional session.
        if not truth.external_activity_absent:
            raise BinanceSpotFunctionalError(
                "account-wide external activity absence is unproven"
            )
        if account_delta != owned_qty:
            raise BinanceSpotFunctionalError(
                "account BTC delta does not equal owner-attributed fill delta"
            )
    gross = owned_qty * truth.mark_price
    pnl = sell_quote + gross - buy_quote - cash_fees_quote
    loss = max(Decimal("0"), -pnl)
    return {
        "ownedQuantity": owned_qty,
        "grossExposure": gross,
        "ownerPnl": pnl,
        "ownerLoss": loss,
        "buyQuote": buy_quote,
        "sellQuote": sell_quote,
        "feesQuote": fees_quote,
        "cashFeesQuote": cash_fees_quote,
        "feesQuoteExact": fee_quote_exact and truth.fee_quote_valuation_complete,
        "thirdAssetFees": tuple(third_asset_fees),
    }


class BinanceSpotContinuousFunctionalService:
    """Dependency-injected orchestration core, intentionally not wired live."""

    def __init__(
        self,
        *,
        ledger: DurableFunctionalLedger,
        binding_reader: Callable[[], Mapping[str, Any]],
        authority_reader: Callable[[], Mapping[str, Any]],
        publication_verifier: Callable[[ExactBinding], Mapping[str, Any]],
        account_exclusivity_guard: BinanceSpotExclusivityGuard | None = None,
        global_first_live_authority_reader: (
            Callable[..., Mapping[str, Any]] | None
        ) = None,
        clock: Callable[[], float] = time.time,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ledger = ledger
        self.binding_reader = binding_reader
        self.authority_reader = authority_reader
        self.publication_verifier = publication_verifier
        self.account_exclusivity_guard = account_exclusivity_guard
        self.global_first_live_authority_reader = (
            global_first_live_authority_reader
        )
        self.clock = clock
        self.monotonic_clock = monotonic_clock
        self._lock = threading.RLock()
        # Monotonic time is intentionally process-local.  A restart loses this
        # witness and therefore can finish cleanup safely, but can never claim
        # that the uninterrupted two-hour wiring observation was completed.
        self._monotonic_started: dict[str, float] = {}

    def _require_exclusivity(
        self,
        *,
        phase: str,
        session_id: str,
        permit: ExactPermit,
        boundary_id: str,
        boundary_hash: str,
        coverage_started_epoch: float,
        require_causal_closure: bool = False,
    ) -> dict[str, Any]:
        guard = self.account_exclusivity_guard
        if guard is None:
            raise BinanceSpotBoundaryBlocked(
                "independent Binance account-exclusivity guard is not wired"
            )
        try:
            return dict(
                guard.verify_and_record(
                    phase=phase,
                    session_id=session_id,
                    permit_id=permit.permit_id,
                    permit_hash=permit.permit_hash,
                    credential_fingerprint=permit.binding.account_fingerprint,
                    boundary_id=boundary_id,
                    boundary_hash=boundary_hash,
                    coverage_started_epoch=coverage_started_epoch,
                    require_causal_closure=require_causal_closure,
                )
            )
        except BinanceSpotExclusivityError as exc:
            raise BinanceSpotBoundaryBlocked(str(exc)) from exc

    def _require_global_first_live_authority(
        self,
        *,
        purpose: str,
        session_id: str,
        permit: ExactPermit,
        cleanup_only: bool,
    ) -> dict[str, Any]:
        reader = self.global_first_live_authority_reader
        if reader is None:
            raise BinanceSpotBoundaryBlocked(
                "global crypto first-live authority reader is not wired"
            )
        now = float(self.clock())
        try:
            snapshot = reader(
                purpose=_text(purpose).upper(),
                session_id=_text(session_id),
                permit_id=permit.permit_id,
                permit_hash=permit.permit_hash,
                account_fingerprint=permit.binding.account_fingerprint,
                cleanup_only=bool(cleanup_only),
            )
            return verify_global_first_live_authority(
                snapshot,
                purpose=purpose,
                session_id=session_id,
                permit_id=permit.permit_id,
                permit_hash=permit.permit_hash,
                account_fingerprint=permit.binding.account_fingerprint,
                cleanup_only=cleanup_only,
                now_epoch=now,
            )
        except BinanceSpotExclusivityError as exc:
            raise BinanceSpotBoundaryBlocked(str(exc)) from exc
        except Exception as exc:
            raise BinanceSpotBoundaryBlocked(
                "global crypto first-live authority reader failed closed"
            ) from exc

    def assert_activation_guards(
        self,
        session_id: str,
        permit_payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Re-prove exact session exclusivity immediately before ACTIVE."""

        with self._lock:
            session = self.ledger.session(session_id)
            permit = ExactPermit.parse(permit_payload, now_epoch=float(self.clock()))
            if (
                _text(session.get("permit_id")) != permit.permit_id
                or not secrets.compare_digest(
                    _text(session.get("permit_hash")), permit.permit_hash
                )
            ):
                raise BinanceSpotBoundaryBlocked(
                    "activation exclusivity session permit changed"
                )
            proof = self._require_exclusivity(
                phase="ACTIVATION",
                session_id=session_id,
                permit=permit,
                boundary_id=f"{session_id}:activation",
                boundary_hash=_text(session["binding_hash"]).lower(),
                coverage_started_epoch=float(
                    session.get("exclusivity_coverage_started_epoch")
                    or session["started_epoch"]
                ),
            )
            authority = self._require_global_first_live_authority(
                purpose="ACTIVATION",
                session_id=session_id,
                permit=permit,
                cleanup_only=False,
            )
            return {"exclusivity": proof, "globalAuthority": authority}

    def _exclusivity_phase_chain(
        self,
        *,
        session_id: str,
        permit: ExactPermit,
        actions: list[dict[str, Any]],
        terminal_proof: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Re-verify the durable proof chain without blocking safe cleanup."""

        guard = self.account_exclusivity_guard
        if guard is None or not callable(getattr(guard, "session_records", None)):
            return {
                "complete": False,
                "restartVerifiable": False,
                "recordCount": 0,
                "phaseChainHash": "",
                "reason": "EXCLUSIVITY_PHASE_RECORD_READER_MISSING",
            }
        try:
            rows = list(guard.session_records(session_id))
        except Exception:
            return {
                "complete": False,
                "restartVerifiable": False,
                "recordCount": 0,
                "phaseChainHash": "",
                "reason": "EXCLUSIVITY_PHASE_RECORD_REVERIFY_FAILED",
            }
        expected: dict[tuple[str, str], str | None] = {
            ("BASELINE", f"{session_id}:baseline"): None,
            ("ACTIVATION", f"{session_id}:activation"): None,
            ("TERMINAL", f"{session_id}:terminal"): _text(
                terminal_proof.get("boundaryHash")
            ).lower(),
        }
        try:
            for action in actions:
                sealed = json.loads(_text(action.get("sealed_action_json")))
                if (
                    isinstance(sealed, Mapping)
                    and sealed.get("cleanupOnly") is False
                    and _upper(sealed.get("kind")) in {"BUY", "SELL"}
                ):
                    expected[("PRE_POST", _text(action.get("claim_id")))] = (
                        _stable_hash(dict(sealed))
                    )
        except (TypeError, ValueError, json.JSONDecodeError):
            return {
                "complete": False,
                "restartVerifiable": False,
                "recordCount": len(rows),
                "phaseChainHash": "",
                "reason": "EXCLUSIVITY_PHASE_ACTION_SEAL_MALFORMED",
            }

        summaries: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        valid = True
        for row in rows:
            proof = row.get("proof") if isinstance(row, Mapping) else None
            if not isinstance(proof, Mapping):
                valid = False
                continue
            phase = _upper(proof.get("phase"))
            boundary_id = _text(proof.get("boundaryId"))
            key = (phase, boundary_id)
            proof_hash = _text(
                row.get("proof_hash") or row.get("proofHash")
            ).lower()
            expected_boundary_hash = expected.get(key)
            current_valid = bool(
                key in expected
                and key not in seen
                and _SHA256_RE.fullmatch(proof_hash) is not None
                and secrets.compare_digest(_stable_hash(dict(proof)), proof_hash)
                and _text(proof.get("sessionId")) == session_id
                and _text(proof.get("permitId")) == permit.permit_id
                and secrets.compare_digest(
                    _text(proof.get("permitHash")).lower(), permit.permit_hash
                )
                and secrets.compare_digest(
                    _text(proof.get("credentialFingerprint")).lower(),
                    permit.binding.account_fingerprint,
                )
                and (
                    expected_boundary_hash is None
                    or secrets.compare_digest(
                        _text(proof.get("boundaryHash")).lower(),
                        expected_boundary_hash,
                    )
                )
            )
            if key == ("TERMINAL", f"{session_id}:terminal"):
                current_valid = bool(
                    current_valid
                    and dict(proof) == dict(terminal_proof)
                )
            valid = valid and current_valid
            seen.add(key)
            summaries.append(
                {
                    "phase": phase,
                    "boundaryId": boundary_id,
                    "boundaryHash": _text(proof.get("boundaryHash")).lower(),
                    "proofHash": proof_hash,
                }
            )
        summaries.sort(key=lambda item: (item["phase"], item["boundaryId"]))
        complete = bool(valid and seen == set(expected) and len(rows) == len(expected))
        return {
            "complete": complete,
            "restartVerifiable": complete,
            "recordCount": len(rows),
            "requiredRecordCount": len(expected),
            "phaseChainHash": (
                _stable_hash({"records": summaries}) if complete else ""
            ),
            "records": summaries if complete else [],
            "reason": "VERIFIED" if complete else "EXCLUSIVITY_PHASE_CHAIN_INCOMPLETE",
        }

    def _read_binding(self, expected: ExactBinding) -> None:
        if ExactBinding.parse(self.binding_reader()) != expected:
            raise BinanceSpotBoundaryBlocked(
                "Strategy Artifact/Instance/account/route binding changed"
            )
        proof = self.publication_verifier(expected)
        exact_proof = {
            "complete": True,
            "strategyArtifactHash": expected.strategy_artifact_hash,
            "artifactFileSha256": expected.artifact_file_sha256,
            "strategyInstanceHash": expected.strategy_instance_hash,
            "instanceFileSha256": expected.instance_file_sha256,
            "publicationProofHash": expected.publication_proof_hash,
            "publicationProofFileSha256": expected.publication_proof_file_sha256,
        }
        for field, expected_value in exact_proof.items():
            actual = proof.get(field)
            if isinstance(expected_value, str):
                actual = _text(actual).lower()
            if actual != expected_value:
                raise BinanceSpotBoundaryBlocked(
                    f"published Strategy/Instance proof changed at {field}"
                )

    def start(
        self,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
        *,
        activation_fence: Mapping[str, Any] | None = None,
        planned_session_id: str = "",
        exclusivity_coverage_started_epoch: float | None = None,
    ) -> dict[str, Any]:
        now = float(self.clock())
        permit = ExactPermit.parse(permit_payload, now_epoch=now)
        if now >= permit.expires_epoch:
            raise BinanceSpotBoundaryBlocked("entry window already expired")
        self._read_binding(permit.binding)
        FunctionalAuthority.parse(self.authority_reader()).assert_prestart(permit)
        truth = AccountTruth.parse(
            account_truth, binding=permit.binding, now_epoch=now
        )
        baseline_age = now - truth.history_baseline_epoch
        if baseline_age < 0 or baseline_age > MAX_TRUTH_AGE_SECONDS:
            raise BinanceSpotBoundaryBlocked(
                "prestart history baseline is stale or future-dated"
            )
        if any(
            (
                truth.stream_session_id,
                truth.stream_permit_id,
                truth.stream_permit_hash,
            )
        ):
            raise BinanceSpotBoundaryBlocked(
                "prestart user stream is already bound to a functional session"
            )
        if truth.open_orders:
            raise BinanceSpotBoundaryBlocked(
                "exclusive account baseline contains a working order"
            )
        if not truth.external_activity_absent:
            raise BinanceSpotBoundaryBlocked(
                "account-wide external activity absence is unproven"
            )
        session_id = _text(planned_session_id) or f"bnsft-{secrets.token_hex(16)}"
        if (
            not session_id.startswith("bnsft-")
            or _SAFE_ID_RE.fullmatch(session_id) is None
        ):
            raise BinanceSpotBoundaryBlocked(
                "prepared session identity is invalid"
            )
        baseline_boundary_hash = (
            truth.official_rest_truth_hash
            if _SHA256_RE.fullmatch(truth.official_rest_truth_hash)
            else _stable_hash(dict(account_truth))
        )
        exclusivity_coverage = (
            truth.history_baseline_epoch
            if exclusivity_coverage_started_epoch is None
            else float(exclusivity_coverage_started_epoch)
        )
        if (
            not math.isfinite(exclusivity_coverage)
            or exclusivity_coverage < permit.issued_epoch - 1.0
            or exclusivity_coverage > truth.observed_epoch
        ):
            raise BinanceSpotBoundaryBlocked(
                "prepared exclusivity coverage epoch is invalid"
            )
        self._require_exclusivity(
            phase="BASELINE",
            session_id=session_id,
            permit=permit,
            boundary_id=f"{session_id}:baseline",
            boundary_hash=baseline_boundary_hash,
            coverage_started_epoch=exclusivity_coverage,
        )
        session, capability = self.ledger.create_session(
            permit,
            truth,
            # The state-owned lifecycle supplies one activation epoch to the
            # approval reseal and the official history baseline.  Persisting
            # that same epoch here makes the active window exactly 7200
            # seconds even if prestart REST/publication checks take time.
            now_epoch=permit.issued_epoch,
            activation_fence=activation_fence,
            session_id=session_id,
            exclusivity_coverage_started_epoch=exclusivity_coverage,
        )
        with self._lock:
            self._monotonic_started[_text(session["session_id"])] = float(
                self.monotonic_clock()
            )
        return {
            "ok": True,
            "status": "RUNNING",
            "sessionId": session["session_id"],
            "functionalCapability": capability,
            "functionalCapabilityHash": session["capability_hash"],
            "expiresEpoch": session["expires_epoch"],
            "cleanupDeadlineEpoch": session["cleanup_deadline_epoch"],
            "productionAvailable": PRODUCTION_AVAILABLE,
            "evidenceClass": EVIDENCE_CLASS,
            "promotionEligible": False,
        }

    def _context(
        self,
        session_id: str,
        capability: str,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
        *,
        exact_owned_cleanup_only: bool = False,
        allow_external_activity_reconciliation_only: bool = False,
    ) -> tuple[dict[str, Any], ExactPermit, AccountTruth, str]:
        now = float(self.clock())
        session = self.ledger.session(session_id)
        capability_hash = self.ledger.assert_capability(session, capability)
        permit = ExactPermit.parse(permit_payload, now_epoch=now)
        if session["permit_id"] != permit.permit_id or session["permit_hash"] != permit.permit_hash:
            raise BinanceSpotBoundaryBlocked("session permit changed")
        if session["binding_hash"] != _stable_hash(permit.binding.payload()):
            raise BinanceSpotBoundaryBlocked("session exact binding changed")
        self._read_binding(permit.binding)
        truth = AccountTruth.parse(
            account_truth, binding=permit.binding, now_epoch=now
        )
        if truth.cleanup_recovery_only:
            if not exact_owned_cleanup_only:
                raise BinanceSpotBoundaryBlocked(
                    "REST-reconciled stream gap truth is cleanup-only"
                )
            session = self.ledger.set_session(
                session_id,
                state="CLEANUP",
                cleanup_started=True,
                cleanup_recovery_used=True,
                detail="sticky private-stream gap; REST-reconciled cleanup only",
            )
        if abs(
            truth.history_baseline_epoch - float(session["started_epoch"])
        ) > 0.001:
            raise BinanceSpotBoundaryBlocked(
                "broker history baseline changed from the durable session start"
            )
        if (
            truth.stream_session_id != session_id
            or truth.stream_permit_id != permit.permit_id
            or not secrets.compare_digest(
                truth.stream_permit_hash, permit.permit_hash
            )
        ):
            raise BinanceSpotBoundaryBlocked(
                "user stream exact session/permit binding changed"
            )
        for row in _owned_rows(truth, session_id):
            _validate_order_row(row, binding=permit.binding)
        if (
            allow_external_activity_reconciliation_only
            and not truth.cleanup_recovery_only
            and not truth.external_activity_absent
        ):
            # Identity, capability, binding, completeness and owned-row shape
            # are now proven, but attribution is intentionally not attempted.
            # The sole caller immediately latches RECONCILIATION_REQUIRED and
            # creates no cancel/flatten claim.
            return session, permit, truth, capability_hash
        metrics = owner_metrics(
            truth,
            session_id=session_id,
            baseline_base=_decimal(session["baseline_base"], label="baseline BTC"),
            allow_cleanup_recovery=(
                exact_owned_cleanup_only and truth.cleanup_recovery_only
            ),
        )
        # The owner can only create exposure through the one quote-capped BUY.
        # Current mark value may later exceed 10 USDT through appreciation and
        # must not prevent an exact position-reducing cleanup SELL.
        if metrics["buyQuote"] > MAX_GROSS_EXPOSURE:
            raise BinanceSpotBoundaryBlocked("owned entry gross exceeds 10 USDT")
        if not metrics["feesQuoteExact"] and not exact_owned_cleanup_only:
            raise BinanceSpotBoundaryBlocked(
                "third-asset fee valuation is incomplete; cleanup-only"
            )
        if metrics["ownerLoss"] >= MAX_OWNER_LOSS and not exact_owned_cleanup_only:
            raise BinanceSpotBoundaryBlocked("owner-attributed loss exceeds 1 USDT")
        return session, permit, truth, capability_hash

    def _reconcile_actions(
        self,
        session_id: str,
        truth: AccountTruth,
        *,
        require_all_terminal: bool,
    ) -> list[dict[str, Any]]:
        """Reconcile durable claims from complete broker truth.

        ``SUBMITTING`` is intentionally reconcilable by exact client order id
        after a crash, but is never submitted a second time.  An absent exact
        order remains unresolved even when the read is complete because the
        prior POST outcome was ambiguous.
        """

        open_rows = list(truth.open_orders)
        closed_rows = list(truth.closed_orders)
        all_rows = [*open_rows, *closed_rows]
        actions = self.ledger.actions(session_id)
        terminal: list[dict[str, Any]] = []
        binding = ExactBinding.parse(
            json.loads(self.ledger.session(session_id)["binding_json"])
        )
        for claim in actions:
            state = _upper(claim["state"])
            action = json.loads(claim["sealed_action_json"])
            kind = _upper(action.get("kind"))
            if state == "RECONCILED":
                terminal.append(claim)
                continue
            if state in {
                "PROVEN_NOT_ACCEPTED",
                "AMBIGUOUS_PROVEN_NOT_ACCEPTED",
            }:
                terminal.append(claim)
                continue
            if state == "NOT_SENT":
                if truth.observed_epoch + 0.001 < float(claim["updated_epoch"]):
                    raise BinanceSpotBoundaryBlocked(
                        "broker truth predates the proven NOT_SENT state"
                    )
                client_id = _text(action.get("clientOrderId"))
                if kind != "CANCEL" and (
                    any(_order_client_id(row) == client_id for row in all_rows)
                    or any(_order_client_id(fill) == client_id for fill in truth.fills)
                ):
                    raise BinanceSpotBoundaryBlocked(
                        "NOT_SENT action unexpectedly exists in complete broker truth"
                    )
                proven = self.ledger.transition_action(
                    claim["claim_id"],
                    expected_state="NOT_SENT",
                    state="PROVEN_NOT_ACCEPTED",
                    now_epoch=float(self.clock()),
                    detail="pre-boundary failure plus fresh complete broker absence",
                )
                terminal.append(proven)
                continue
            if state not in {
                "ACKNOWLEDGED",
                "SUBMITTING",
                "POST_MAY_HAVE_CROSSED",
                "RECONCILIATION_REQUIRED",
            }:
                if require_all_terminal:
                    raise BinanceSpotBoundaryBlocked(
                        f"durable {kind} action remains {state}"
                    )
                continue
            if truth.observed_epoch + 0.001 < float(claim["updated_epoch"]):
                raise BinanceSpotBoundaryBlocked(
                    "broker truth predates the latest durable action state"
                )
            if kind == "CANCEL":
                target_client_id = _text(action.get("origClientOrderId"))
                target_order_id = _text(action.get("brokerOrderId"))
                matches = [
                    row
                    for row in all_rows
                    if _order_client_id(row) == target_client_id
                    and _text(row.get("orderId")) == target_order_id
                ]
                if len(matches) != 1:
                    raise BinanceSpotBoundaryBlocked(
                        "cancel target is absent or duplicated in complete truth"
                    )
                row = matches[0]
                _validate_order_row(row, binding=binding)
                if row in open_rows or _upper(row.get("status")) not in _TERMINAL_ORDER_STATES:
                    if require_all_terminal:
                        raise BinanceSpotBoundaryBlocked("cancel target is still working")
                    continue
            else:
                client_id = _text(action.get("clientOrderId"))
                matches = [row for row in all_rows if _order_client_id(row) == client_id]
                if len(matches) != 1:
                    raise BinanceSpotBoundaryBlocked(
                        f"exact {kind} order is absent or duplicated in complete truth"
                    )
                row = matches[0]
                _validate_order_row(row, binding=binding)
                if _upper(row.get("side")) != kind or _upper(row.get("type")) != "MARKET":
                    raise BinanceSpotBoundaryBlocked(f"exact {kind} order shape changed")
                if kind == "BUY":
                    requested = _decimal(
                        row.get("origQuoteOrderQty"), label="BUY origQuoteOrderQty"
                    )
                    if requested != _decimal(
                        action.get("quoteOrderQty"), label="sealed BUY quoteOrderQty"
                    ):
                        raise BinanceSpotBoundaryBlocked("BUY quoteOrderQty changed")
                else:
                    requested = _decimal(row.get("origQty"), label="SELL origQty")
                    if requested != _decimal(
                        action.get("quantity"), label="sealed SELL quantity"
                    ):
                        raise BinanceSpotBoundaryBlocked("SELL quantity changed")
                if row in open_rows or _upper(row.get("status")) not in _TERMINAL_ORDER_STATES:
                    if require_all_terminal:
                        raise BinanceSpotBoundaryBlocked(f"exact {kind} order is not terminal")
                    # A validated owned working order stays pending so recovery
                    # can reach the exact-owned CANCEL planner.  It is never
                    # resubmitted and still blocks a new natural signal.
                    continue
                matching_fills = [
                    fill
                    for fill in truth.fills
                    if _order_client_id(fill) == client_id
                ]
                executed_quantity = _decimal(
                    row.get("executedQty"), label=f"{kind} executedQty"
                )
                executed_quote = _decimal(
                    row.get("cummulativeQuoteQty", "0"),
                    label=f"{kind} cummulativeQuoteQty",
                )
                if executed_quantity > 0 and not matching_fills:
                    raise BinanceSpotBoundaryBlocked(f"exact {kind} has no complete fill truth")
                if executed_quantity == 0 and matching_fills:
                    raise BinanceSpotBoundaryBlocked(
                        f"exact {kind} zero execution unexpectedly has fills"
                    )
                fill_quantity = sum(
                    (
                        _decimal(fill.get("quantity"), label=f"{kind} fill quantity")
                        for fill in matching_fills
                    ),
                    Decimal("0"),
                )
                fill_quote = sum(
                    (
                        _decimal(
                            fill.get("quoteQuantity"),
                            label=f"{kind} fill quoteQuantity",
                        )
                        for fill in matching_fills
                    ),
                    Decimal("0"),
                )
                if fill_quantity != executed_quantity or fill_quote != executed_quote:
                    raise BinanceSpotBoundaryBlocked(
                        f"exact {kind} aggregate fill quantity/quote disagrees with order"
                    )
                if _upper(row.get("status")) == "FILLED" and (
                    executed_quantity <= 0 or fill_quantity <= 0 or fill_quote <= 0
                ):
                    raise BinanceSpotBoundaryBlocked(
                        f"FILLED {kind} needs positive exact fill truth"
                    )
            reconciled_claim = self.ledger.transition_action(
                claim["claim_id"],
                expected_state=state,
                state="RECONCILED",
                now_epoch=float(self.clock()),
                broker_order_id=_text(claim.get("broker_order_id")),
                detail="complete terminal order/fill/fee truth reconciled",
            )
            terminal.append(reconciled_claim)
        if require_all_terminal and len(terminal) != len(actions):
            raise BinanceSpotBoundaryBlocked("not every durable action is reconciled")
        return terminal

    def prove_ambiguous_not_accepted(
        self,
        session_id: str,
        capability: str,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
        claim_id: str,
        *,
        exact_client_order_query: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Terminalize a marker-before-send crash without retrying the order.

        This proof is intentionally delayed.  It combines complete account-wide
        open/history/trade truth, a gapless authenticated private stream, and an
        exact ``origClientOrderId`` official lookup which returned 404/-2013.
        """

        with self._lock:
            session, permit, truth, _ = self._context(
                session_id,
                capability,
                permit_payload,
                account_truth,
                exact_owned_cleanup_only=True,
            )
            claim = self.ledger.action(claim_id)
            state = _upper(claim["state"])
            if claim["session_id"] != session_id or state not in {
                "POST_MAY_HAVE_CROSSED",
                "RECONCILIATION_REQUIRED",
            }:
                raise BinanceSpotBoundaryBlocked(
                    "claim is not an ambiguous no-retry action"
                )
            prior_observations = int(claim.get("absence_proof_count") or 0)
            age = truth.observed_epoch - float(claim["updated_epoch"])
            if (
                prior_observations == 0
                and age < AMBIGUOUS_NONACCEPTANCE_MIN_AGE_SECONDS
            ):
                raise BinanceSpotBoundaryBlocked(
                    "broker acceptance visibility horizon has not elapsed"
                )
            if (
                prior_observations > 0
                and truth.observed_epoch
                - float(claim.get("absence_last_epoch") or 0)
                < AMBIGUOUS_NONACCEPTANCE_MIN_SPACING_SECONDS
            ):
                raise BinanceSpotBoundaryBlocked(
                    "ambiguous nonacceptance observations are not sufficiently spaced"
                )
            action = json.loads(claim["sealed_action_json"])
            client_id = _text(action.get("clientOrderId"))
            if set(exact_client_order_query) != {
                "complete",
                "symbol",
                "origClientOrderId",
                "notFound",
                "errorCode",
                "observedAt",
            }:
                raise BinanceSpotBoundaryBlocked(
                    "exact client-order nonacceptance proof fields changed"
                )
            if (
                exact_client_order_query.get("complete") is not True
                or _upper(exact_client_order_query.get("symbol")) != SYMBOL
                or _text(exact_client_order_query.get("origClientOrderId"))
                != client_id
                or exact_client_order_query.get("notFound") is not True
                or int(exact_client_order_query.get("errorCode") or 0) != -2013
                or abs(
                    _fresh_epoch(
                        exact_client_order_query.get("observedAt"),
                        now_epoch=float(self.clock()),
                        label="exact client-order query observedAt",
                    )
                    - truth.observed_epoch
                )
                > MAX_TRUTH_AGE_SECONDS
            ):
                raise BinanceSpotBoundaryBlocked(
                    "exact client-order lookup does not prove nonacceptance"
                )
            if any(
                _order_client_id(row) == client_id
                for row in (*truth.open_orders, *truth.closed_orders, *truth.fills)
            ):
                raise BinanceSpotBoundaryBlocked(
                    "ambiguous order/fill exists in complete broker truth"
                )
            if not _text(claim.get("pre_base_total")) or not _text(
                claim.get("pre_quote_total")
            ):
                raise BinanceSpotBoundaryBlocked(
                    "durable pre-POST account balance seal is absent"
                )
            pre_base = _decimal(claim["pre_base_total"], label="pre-POST BTC")
            pre_quote = _decimal(claim["pre_quote_total"], label="pre-POST USDT")
            if truth.base_total != pre_base or truth.quote_total != pre_quote:
                raise BinanceSpotBoundaryBlocked(
                    "account balances changed during ambiguous nonacceptance proof"
                )
            proof = {
                "claimId": _text(claim_id),
                "clientOrderId": client_id,
                "truthObservedEpoch": truth.observed_epoch,
                "historyCutoffEpoch": truth.history_cutoff_epoch,
                "baseTotal": _decimal_text(truth.base_total),
                "quoteTotal": _decimal_text(truth.quote_total),
                "openOrdersHash": _stable_hash(list(truth.open_orders)),
                "closedOrdersHash": _stable_hash(list(truth.closed_orders)),
                "fillsHash": _stable_hash(list(truth.fills)),
                "privateStreamSessionId": truth.stream_session_id,
                "privateStreamPermitId": truth.stream_permit_id,
                "privateStreamPermitHash": truth.stream_permit_hash,
                "exactClientOrderQuery": dict(exact_client_order_query),
            }
            result = self.ledger.record_ambiguous_absence_observation(
                claim_id,
                expected_state=state,
                observed_epoch=truth.observed_epoch,
                proof=proof,
            )
            if _upper(result["state"]) == "AMBIGUOUS_PROVEN_NOT_ACCEPTED":
                self.ledger.set_session(
                    session_id,
                    state="CLEANUP",
                    cleanup_started=True,
                    detail="ambiguous action safely terminalized as nonaccepted",
                )
            return result

    def observe_bar(
        self,
        session_id: str,
        capability: str,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
        exchange_info_truth: Mapping[str, Any],
        bar_evaluation: Mapping[str, Any],
        *,
        buy_notional: object = MAX_ORDER_NOTIONAL,
    ) -> dict[str, Any]:
        """Record one finalized bar and, at most, durably claim a natural leg."""

        with self._lock:
            session, permit, truth, _ = self._context(
                session_id, capability, permit_payload, account_truth
            )
            self._reconcile_actions(
                session_id, truth, require_all_terminal=False
            )
            session = self.ledger.session(session_id)
            pending = [
                item
                for item in self.ledger.actions(session_id)
                if _upper(item["state"]) != "RECONCILED"
            ]
            if pending:
                raise BinanceSpotBoundaryBlocked(
                    "prior durable action must be reconciled before a new signal"
                )
            now = float(self.clock())
            if now >= permit.expires_epoch:
                self.ledger.set_session(
                    session_id,
                    state="CLEANUP",
                    cleanup_started=True,
                    detail="entry permit expired; cleanup-only",
                )
                return {"ok": True, "status": "CLEANUP", "action": None}
            if _upper(session["state"]) != "RUNNING":
                raise BinanceSpotBoundaryBlocked("session is cleanup-only or finalized")
            signal = FinalizedBarSignal.parse(
                bar_evaluation,
                binding=permit.binding,
                now_epoch=now,
                previous_close_epoch=float(session["last_bar_close_epoch"]),
            )
            evaluation_seal = self.ledger.record_strategy_evaluation(
                session_id, signal, now_epoch=now
            )
            if signal.signal == "HOLD":
                return {
                    "ok": True,
                    "status": "HOLD",
                    "action": None,
                    "evaluation": evaluation_seal,
                }
            rules = SymbolRules.parse(exchange_info_truth)
            rules.assert_permission_proof(truth)
            metrics = owner_metrics(
                truth,
                session_id=session_id,
                baseline_base=_decimal(session["baseline_base"], label="baseline BTC"),
            )
            if signal.signal == "BUY":
                if int(session["buy_claimed"]) or int(session["sell_claimed"]):
                    return {
                        "ok": True,
                        "status": "NO_REENTRY_CAP_REACHED",
                        "action": None,
                    }
                if metrics["ownedQuantity"] != 0:
                    raise BinanceSpotBoundaryBlocked("owned position is not flat")
                if not truth.third_asset_fee_funding_absent:
                    raise BinanceSpotBoundaryBlocked(
                        "positive BNB funding can make the next fee asset non-base/quote"
                    )
                notional = _decimal(buy_notional, label="BUY notional")
                rules.assert_buy_notional(notional)
                if truth.quote_available < notional:
                    raise BinanceSpotBoundaryBlocked("available account USDT is insufficient")
                action = {
                    "kind": "BUY",
                    "product": "SPOT",
                    "symbol": SYMBOL,
                    "side": "BUY",
                    "orderType": "MARKET",
                    "quoteOrderQty": _decimal_text(notional),
                    "quantity": "0",
                    "clientOrderId": _client_order_id(session_id, "BUY"),
                    "evaluationId": signal.evaluation_id,
                    "evaluationHash": evaluation_seal["evaluationHash"],
                    "officialWindowHash": evaluation_seal["windowHash"],
                    "barCloseEpoch": signal.close_epoch,
                    "functionalOnly": True,
                    "cleanupOnly": False,
                    "evidenceClass": EVIDENCE_CLASS,
                }
            else:
                owned = metrics["ownedQuantity"]
                if int(session["sell_claimed"]):
                    return {
                        "ok": True,
                        "status": "NATURAL_SELL_CAP_REACHED",
                        "action": None,
                    }
                if owned <= 0 or not int(session["buy_claimed"]):
                    return {"ok": True, "status": "HOLD", "action": None}
                quantity = rules.normalize_flatten_quantity(
                    owned, price=truth.mark_price
                )
                action = {
                    "kind": "SELL",
                    "product": "SPOT",
                    "symbol": SYMBOL,
                    "side": "SELL",
                    "orderType": "MARKET",
                    "quantity": _decimal_text(quantity),
                    "quoteOrderQty": "0",
                    "clientOrderId": _client_order_id(session_id, "SELL"),
                    "evaluationId": signal.evaluation_id,
                    "evaluationHash": evaluation_seal["evaluationHash"],
                    "officialWindowHash": evaluation_seal["windowHash"],
                    "barCloseEpoch": signal.close_epoch,
                    "functionalOnly": True,
                    "cleanupOnly": False,
                    "evidenceClass": EVIDENCE_CLASS,
                }
            claim = self.ledger.claim_action(
                session_id,
                action,
                now_epoch=now,
                pre_base_total=truth.base_total,
                pre_quote_total=truth.quote_total,
            )
            return {
                "ok": True,
                "status": "CLAIMED",
                "claim": claim,
                "action": action,
                "productionAvailable": PRODUCTION_AVAILABLE,
            }

    def risk_status(
        self,
        session_id: str,
        capability: str,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Read exact owner risk without granting any mutation authority."""

        with self._lock:
            session, _, truth, _ = self._context(
                session_id,
                capability,
                permit_payload,
                account_truth,
                exact_owned_cleanup_only=True,
            )
            metrics = owner_metrics(
                truth,
                session_id=session_id,
                baseline_base=_decimal(
                    session["baseline_base"], label="baseline BTC"
                ),
                allow_cleanup_recovery=truth.cleanup_recovery_only,
            )
            return {
                "ownerLoss": _decimal_text(metrics["ownerLoss"]),
                "ownerQuantity": _decimal_text(metrics["ownedQuantity"]),
                "cleanupRequired": (
                    metrics["ownerLoss"] >= MAX_OWNER_LOSS
                    or not metrics["feesQuoteExact"]
                ),
                "feesQuoteExact": bool(metrics["feesQuoteExact"]),
                "ownerLossLimitSatisfied": (
                    bool(metrics["feesQuoteExact"])
                    and metrics["ownerLoss"] < MAX_OWNER_LOSS
                ),
            }

    def dispatch_claim(
        self,
        session_id: str,
        capability: str,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
        exchange_info_truth: Mapping[str, Any],
        claim_id: str,
        *,
        submitter: Callable[..., Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Cross the injected POST boundary exactly once after durable claim."""

        with self._lock:
            claim = self.ledger.action(claim_id)
            if claim["session_id"] != session_id or _upper(claim["state"]) != "CLAIMED":
                raise DuplicateActionClaim("claim is not dispatchable; blind retry blocked")
            action = json.loads(claim["sealed_action_json"])
            cleanup_only = action.get("cleanupOnly") is True
            try:
                session, permit, truth, capability_hash = self._context(
                    session_id,
                    capability,
                    permit_payload,
                    account_truth,
                    exact_owned_cleanup_only=cleanup_only,
                )
                kind = _upper(action.get("kind"))
                if kind == "BUY":
                    if cleanup_only or float(self.clock()) >= permit.expires_epoch:
                        raise BinanceSpotBoundaryBlocked(
                            "BUY is forbidden in cleanup/expiry"
                        )
                    rules = SymbolRules.parse(exchange_info_truth)
                    rules.assert_permission_proof(truth)
                    rules.assert_buy_notional(
                        _decimal(
                            action.get("quoteOrderQty"), label="BUY quoteOrderQty"
                        )
                    )
                elif kind == "SELL":
                    rules = SymbolRules.parse(exchange_info_truth)
                    rules.assert_permission_proof(truth)
                    metrics = owner_metrics(
                        truth,
                        session_id=session_id,
                        baseline_base=_decimal(
                            session["baseline_base"], label="baseline BTC"
                        ),
                        allow_cleanup_recovery=truth.cleanup_recovery_only,
                    )
                    expected_quantity = rules.normalize_flatten_quantity(
                        metrics["ownedQuantity"], price=truth.mark_price
                    )
                    if truth.base_available < expected_quantity:
                        raise BinanceSpotBoundaryBlocked(
                            "available BTC cannot cover the exact owned SELL"
                        )
                    if expected_quantity != _decimal(
                        action.get("quantity"), label="SELL quantity"
                    ):
                        raise BinanceSpotBoundaryBlocked(
                            "SELL is not the exact current session-owned reduction"
                        )
                elif kind == "CANCEL":
                    if not cleanup_only:
                        raise BinanceSpotBoundaryBlocked("CANCEL must be cleanup-only")
                    matches = [
                        row
                        for row in truth.open_orders
                        if _order_client_id(row)
                        == _text(action.get("origClientOrderId"))
                        and _text(row.get("orderId"))
                        == _text(action.get("brokerOrderId"))
                        and _order_client_id(row).startswith(
                            _owned_prefix(session_id)
                        )
                    ]
                    if len(matches) != 1:
                        raise BinanceSpotBoundaryBlocked(
                            "CANCEL target is not the exact owned working order"
                        )
                    _validate_order_row(matches[0], binding=permit.binding)
                else:
                    raise BinanceSpotBoundaryBlocked(
                        "unsupported functional action"
                    )
                authority_before = FunctionalAuthority.parse(
                    self.authority_reader()
                )
                authority_before.assert_dispatch(
                    permit,
                    session_id=session_id,
                    capability_hash=capability_hash,
                    cleanup_only=cleanup_only,
                )
            except Exception as exc:
                detail = (
                    f"not-sent-pre-submit:{type(exc).__name__}:"
                    f"{str(exc)[:300]}"
                )
                self.ledger.transition_action(
                    claim_id,
                    expected_state="CLAIMED",
                    state="NOT_SENT",
                    now_epoch=float(self.clock()),
                    detail=detail,
                )
                self.ledger.set_session(
                    session_id,
                    state="CLEANUP",
                    cleanup_started=True,
                    detail=detail,
                )
                raise
            self.ledger.transition_action(
                claim_id,
                expected_state="CLAIMED",
                state="SUBMITTING",
                now_epoch=float(self.clock()),
                detail="durable no-retry claim committed before injected POST",
            )
            # Final mutable authority and exact binding re-read occurs after
            # SUBMITTING was committed and immediately before the callable.
            try:
                if not cleanup_only:
                    self._require_exclusivity(
                        phase="PRE_POST",
                        session_id=session_id,
                        permit=permit,
                        boundary_id=_text(claim_id),
                        boundary_hash=_stable_hash(dict(action)),
                        coverage_started_epoch=float(
                            session.get("exclusivity_coverage_started_epoch")
                            or session["started_epoch"]
                        ),
                    )
                    self._require_global_first_live_authority(
                        purpose="FINAL_PRE_POST",
                        session_id=session_id,
                        permit=permit,
                        cleanup_only=False,
                    )
                authority_final = FunctionalAuthority.parse(self.authority_reader())
                authority_final.assert_dispatch(
                    permit,
                    session_id=session_id,
                    capability_hash=capability_hash,
                    cleanup_only=cleanup_only,
                )
                if authority_final.authority_revision != authority_before.authority_revision:
                    raise BinanceSpotBoundaryBlocked(
                        "authority revision changed before POST"
                    )
                self._read_binding(permit.binding)
            except Exception as exc:
                # No transport callable has been entered.  This is a durable,
                # auditable NOT_SENT outcome, not an ambiguous broker result.
                self.ledger.transition_action(
                    claim_id,
                    expected_state="SUBMITTING",
                    state="NOT_SENT",
                    now_epoch=float(self.clock()),
                    detail=f"not-sent-final-prepost:{type(exc).__name__}:{str(exc)[:300]}",
                )
                self.ledger.set_session(
                    session_id,
                    state="CLEANUP",
                    cleanup_started=True,
                    detail="final pre-POST validation blocked without transport entry",
                )
                raise

            marker_aware = (
                getattr(submitter, "functional_marker_aware", False) is True
            )
            exact_context_aware = (
                getattr(submitter, "functional_exact_context_aware", False)
                is True
            )

            def mark_may_have_been_sent() -> None:
                self.ledger.transition_action(
                    claim_id,
                    expected_state="SUBMITTING",
                    state="POST_MAY_HAVE_CROSSED",
                    now_epoch=float(self.clock()),
                    detail="transport boundary marker committed immediately before send",
                )

            try:
                if exact_context_aware:
                    receipt = submitter(
                        action,
                        mark_may_have_been_sent,
                        claim_id=claim_id,
                        sealed_action_hash=_stable_hash(dict(action)),
                        functional_capability=capability,
                        session_id=session_id,
                        permit_id=permit.permit_id,
                        permit_hash=permit.permit_hash,
                        account_fingerprint=permit.binding.account_fingerprint,
                        authority_revision=authority_final.authority_revision,
                    )
                elif marker_aware:
                    receipt = submitter(action, mark_may_have_been_sent)
                else:
                    receipt = submitter(action)
            except Exception as exc:
                current_state = _upper(self.ledger.action(claim_id)["state"])
                if marker_aware and current_state == "SUBMITTING":
                    detail = f"not-sent:{type(exc).__name__}:{str(exc)[:300]}"
                    self.ledger.transition_action(
                        claim_id,
                        expected_state="SUBMITTING",
                        state="NOT_SENT",
                        now_epoch=float(self.clock()),
                        detail=detail,
                    )
                    self.ledger.set_session(
                        session_id,
                        state="CLEANUP",
                        cleanup_started=True,
                        detail=detail,
                    )
                    return {"ok": False, "status": "NOT_SENT", "reason": detail}
                detail = f"ambiguous-submit:{type(exc).__name__}:{str(exc)[:300]}"
                self.ledger.transition_action(
                    claim_id,
                    expected_state=(
                        "POST_MAY_HAVE_CROSSED"
                        if marker_aware
                        else "SUBMITTING"
                    ),
                    state="RECONCILIATION_REQUIRED",
                    now_epoch=float(self.clock()),
                    detail=detail,
                )
                self.ledger.set_session(
                    session_id,
                    state="RECONCILIATION_REQUIRED",
                    cleanup_started=True,
                    detail=detail,
                )
                return {"ok": False, "status": "RECONCILIATION_REQUIRED", "reason": detail}
            if not isinstance(receipt, Mapping):
                receipt = {}
            try:
                broker_order_id = _validate_receipt(action, receipt)
            except BinanceSpotFunctionalError as exc:
                detail = f"ambiguous-submit:{str(exc)}"
                self.ledger.transition_action(
                    claim_id,
                    expected_state=(
                        "POST_MAY_HAVE_CROSSED"
                        if marker_aware
                        else "SUBMITTING"
                    ),
                    state="RECONCILIATION_REQUIRED",
                    now_epoch=float(self.clock()),
                    response=receipt,
                    detail=detail,
                )
                self.ledger.set_session(
                    session_id,
                    state="RECONCILIATION_REQUIRED",
                    cleanup_started=True,
                    detail=detail,
                )
                return {"ok": False, "status": "RECONCILIATION_REQUIRED", "reason": detail}
            result = self.ledger.transition_action(
                claim_id,
                expected_state=(
                    "POST_MAY_HAVE_CROSSED"
                    if marker_aware
                    else "SUBMITTING"
                ),
                state="ACKNOWLEDGED",
                now_epoch=float(self.clock()),
                response=receipt,
                broker_order_id=broker_order_id,
                detail="injected POST acknowledged; broker truth reconciliation still required",
            )
            return {"ok": True, "status": "ACKNOWLEDGED", "claim": result}

    def recover(
        self,
        session_id: str,
        capability: str,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
        exchange_info_truth: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Restart/expiry recovery: reconcile, cancel owned, flatten, then seal."""

        with self._lock:
            session, permit, truth, _ = self._context(
                session_id,
                capability,
                permit_payload,
                account_truth,
                exact_owned_cleanup_only=True,
                allow_external_activity_reconciliation_only=True,
            )
            if (
                not truth.cleanup_recovery_only
                and not truth.external_activity_absent
            ):
                # The approved lane owns only its deterministic client-id
                # namespace.  Any account-wide external activity invalidates
                # the exclusive-account contract; never cancel or flatten in
                # that mixed-owner snapshot.
                self.ledger.set_session(
                    session_id,
                    state="RECONCILIATION_REQUIRED",
                    cleanup_started=True,
                    detail=(
                        "external account activity observed; no functional "
                        "cleanup mutation was claimed"
                    ),
                )
                return {
                    "ok": False,
                    "status": "RECONCILIATION_REQUIRED",
                    "reason": "external-account-activity-observed-no-cancel",
                    "action": None,
                }
            self._reconcile_actions(
                session_id, truth, require_all_terminal=False
            )
            session = self.ledger.session(session_id)
            pending = [
                item
                for item in self.ledger.actions(session_id)
                if _upper(item["state"])
                not in {
                    "RECONCILED",
                    "PROVEN_NOT_ACCEPTED",
                    "AMBIGUOUS_PROVEN_NOT_ACCEPTED",
                }
            ]
            # A validated owned working order is intentionally left pending so
            # the exact-owned CANCEL planner below can run.  Every other
            # unresolved/ambiguous claim remains a hard no-retry barrier.
            working_ids = {
                _order_client_id(row)
                for row in truth.open_orders
                if _order_client_id(row).startswith(_owned_prefix(session_id))
            }
            unsafe_pending = [
                item
                for item in pending
                if _text(item["client_order_id"]) not in working_ids
            ]
            if unsafe_pending:
                return {
                    "ok": False,
                    "status": "RECONCILIATION_REQUIRED",
                    "reason": "prior durable action has no terminal broker truth",
                    "claims": unsafe_pending,
                }
            now = float(self.clock())
            metrics = owner_metrics(
                truth,
                session_id=session_id,
                baseline_base=_decimal(
                    session["baseline_base"], label="baseline BTC"
                ),
                allow_cleanup_recovery=truth.cleanup_recovery_only,
            )
            if metrics["ownerLoss"] >= MAX_OWNER_LOSS:
                self.ledger.set_session(
                    session_id,
                    state="CLEANUP",
                    cleanup_started=True,
                    detail="owner loss limit reached; exact-owned cleanup only",
                )
                session = self.ledger.session(session_id)
            if now >= permit.cleanup_deadline_epoch:
                self.ledger.set_session(
                    session_id,
                    state="RECONCILIATION_REQUIRED",
                    cleanup_started=True,
                    detail="cleanup deadline exceeded before baseline flat",
                )
                return {"ok": False, "status": "RECONCILIATION_REQUIRED", "reason": "cleanup-deadline-exceeded"}
            if now >= permit.expires_epoch or _upper(session["state"]) != "RUNNING":
                self.ledger.set_session(
                    session_id,
                    state="CLEANUP",
                    cleanup_started=True,
                    detail="restart/expiry cleanup-only recovery",
                )
            prefix = _owned_prefix(session_id)
            owned_open = [
                row for row in truth.open_orders if _order_client_id(row).startswith(prefix)
            ]
            all_actions = self.ledger.actions(session_id)
            cleanup_actions = [
                action
                for action in all_actions
                if _upper(action["action_kind"]).startswith("CANCEL")
                or _upper(action["action_kind"]).startswith("CLEANUP_SELL")
            ]
            if len(owned_open) > 1:
                raise BinanceSpotBoundaryBlocked("more than one owned working order exists")
            if owned_open:
                row = owned_open[0]
                _validate_order_row(row, binding=permit.binding)
                existing_cancel = [
                    action
                    for action in all_actions
                    if _upper(action["action_kind"]).startswith("CANCEL")
                ]
                target_client_id = _order_client_id(row)
                canceled_targets = {
                    _text(
                        json.loads(action["sealed_action_json"]).get(
                            "origClientOrderId"
                        )
                    )
                    for action in existing_cancel
                }
                if target_client_id in canceled_targets:
                    return {
                        "ok": False,
                        "status": "RECONCILIATION_REQUIRED",
                        "reason": (
                            "exact owned target cancel was already claimed; "
                            "never retry the same cancel"
                        ),
                    }
                if len(cleanup_actions) >= MAX_CLEANUP_ACTIONS:
                    return {
                        "ok": False,
                        "status": "RECONCILIATION_REQUIRED",
                        "reason": "bounded cleanup budget exhausted after final cancel reserve",
                    }
                cancel_kind = _cleanup_action_kind(
                    "CANCEL", len(existing_cancel) + 1
                )
                action = {
                    "kind": "CANCEL",
                    "product": "SPOT",
                    "symbol": SYMBOL,
                    "brokerOrderId": _text(row.get("orderId")),
                    "origClientOrderId": target_client_id,
                    "clientOrderId": _client_order_id(session_id, cancel_kind),
                    "functionalOnly": True,
                    "cleanupOnly": True,
                    "evidenceClass": EVIDENCE_CLASS,
                }
                claim = self.ledger.claim_action(
                    session_id,
                    action,
                    now_epoch=now,
                    pre_base_total=truth.base_total,
                    pre_quote_total=truth.quote_total,
                )
                return {"ok": True, "status": "CLEANUP_CANCEL_CLAIMED", "claim": claim, "action": action}
            if metrics["ownedQuantity"] > 0:
                cleanup_count = int(session["cleanup_sell_claimed"])
                # A cleanup SELL may itself remain working/partial.  Do not
                # spend the twelfth and final cleanup slot on a new SELL: it
                # is permanently reserved for cancelling that exact target.
                if len(cleanup_actions) >= MAX_CLEANUP_ACTIONS - 1:
                    return {
                        "ok": False,
                        "status": "RECONCILIATION_REQUIRED",
                        "reason": "bounded cleanup budget preserved its final cancel slot",
                    }
                rules = SymbolRules.parse(exchange_info_truth)
                rules.assert_permission_proof(truth)
                if rules.is_unorderable_residual(
                    metrics["ownedQuantity"], price=truth.mark_price
                ):
                    return {
                        "ok": True,
                        "status": "UNORDERABLE_OWNED_DUST",
                        "action": None,
                        "ownedResidual": _decimal_text(metrics["ownedQuantity"]),
                    }
                quantity = rules.normalize_flatten_quantity(
                    metrics["ownedQuantity"], price=truth.mark_price
                )
                action = {
                    "kind": "SELL",
                    "product": "SPOT",
                    "symbol": SYMBOL,
                    "side": "SELL",
                    "orderType": "MARKET",
                    "quantity": _decimal_text(quantity),
                    "quoteOrderQty": "0",
                    "clientOrderId": _client_order_id(
                        session_id,
                        _cleanup_action_kind("CLEANUP_SELL", cleanup_count + 1),
                    ),
                    "functionalOnly": True,
                    "cleanupOnly": True,
                    "evidenceClass": EVIDENCE_CLASS,
                }
                claim = self.ledger.claim_action(
                    session_id,
                    action,
                    now_epoch=now,
                    pre_base_total=truth.base_total,
                    pre_quote_total=truth.quote_total,
                )
                return {"ok": True, "status": "CLEANUP_FLATTEN_CLAIMED", "claim": claim, "action": action}
            return {"ok": True, "status": "BASELINE_FLAT", "action": None}

    def finalize(
        self,
        session_id: str,
        capability: str,
        permit_payload: Mapping[str, Any],
        account_truth: Mapping[str, Any],
        exchange_info_truth: Mapping[str, Any] | None = None,
        prepare_only: bool = False,
    ) -> dict[str, Any]:
        """Seal only complete owned baseline-flat truth and revoked capability."""

        with self._lock:
            session, permit, truth, _ = self._context(
                session_id,
                capability,
                permit_payload,
                account_truth,
                exact_owned_cleanup_only=True,
            )
            if any(
                _order_client_id(row).startswith(_owned_prefix(session_id))
                for row in truth.open_orders
            ):
                raise BinanceSpotBoundaryBlocked("owned working order remains")
            metrics = owner_metrics(
                truth,
                session_id=session_id,
                baseline_base=_decimal(session["baseline_base"], label="baseline BTC"),
                allow_cleanup_recovery=truth.cleanup_recovery_only,
            )
            baseline_base = _decimal(session["baseline_base"], label="baseline BTC")
            residual = metrics["ownedQuantity"]
            baseline_delta = truth.base_total - baseline_base
            recovered_stream_gap = bool(session["cleanup_recovery_used"]) or (
                truth.cleanup_recovery_only
            )
            terminal_observed_epoch = float(self.clock())
            actual_duration_seconds = max(
                Decimal("0"),
                Decimal(
                    str(
                        terminal_observed_epoch
                        - float(session["started_epoch"])
                    )
                ),
            )
            exchange_duration_seconds = max(
                Decimal("0"),
                Decimal(
                    str(
                        truth.observed_epoch
                        - float(session["started_epoch"])
                    )
                ),
            )
            monotonic_started = self._monotonic_started.get(session_id)
            monotonic_duration_seconds: Decimal | None = None
            if monotonic_started is not None:
                monotonic_duration_seconds = max(
                    Decimal("0"),
                    Decimal(
                        str(float(self.monotonic_clock()) - monotonic_started)
                    ),
                )
            runtime_clock_consistency_proven = bool(
                monotonic_duration_seconds is not None
                and abs(actual_duration_seconds - exchange_duration_seconds)
                <= Decimal(str(MAX_TRUTH_AGE_SECONDS))
                and abs(actual_duration_seconds - monotonic_duration_seconds)
                <= Decimal(str(MAX_TRUTH_AGE_SECONDS))
            )
            exact_two_hour_runtime_complete = (
                actual_duration_seconds >= Decimal(str(PERMIT_SECONDS))
                and exchange_duration_seconds >= Decimal(str(PERMIT_SECONDS))
                and monotonic_duration_seconds is not None
                and monotonic_duration_seconds >= Decimal(str(PERMIT_SECONDS))
                and runtime_clock_consistency_proven
                and Decimal(str(session["expires_epoch"]))
                - Decimal(str(session["started_epoch"]))
                == Decimal(str(PERMIT_SECONDS))
            )
            residual_is_dust = False
            residual_rules: SymbolRules | None = None
            if residual != 0 or baseline_delta != 0:
                if recovered_stream_gap and residual == 0:
                    if baseline_delta < 0:
                        raise BinanceSpotBoundaryBlocked(
                            "REST cleanup would leave BTC below pre-existing baseline"
                        )
                else:
                    baseline_preserved = (
                        baseline_delta >= residual
                        if recovered_stream_gap
                        else baseline_delta == residual
                    )
                    if residual <= 0 or not baseline_preserved:
                        raise BinanceSpotBoundaryBlocked(
                            "final BTC balance is not baseline-owned"
                        )
                    if exchange_info_truth is None:
                        raise BinanceSpotBoundaryBlocked(
                            "exchange rules are required to prove final owned dust"
                        )
                    residual_rules = SymbolRules.parse(exchange_info_truth)
                    residual_rules.assert_permission_proof(truth)
                    residual_is_dust = residual_rules.is_unorderable_residual(
                        residual, price=truth.mark_price
                    )
                    if not residual_is_dust:
                        raise BinanceSpotBoundaryBlocked(
                            "orderable session-owned BTC remains"
                        )
            actions = self._reconcile_actions(
                session_id, truth, require_all_terminal=True
            )
            session = self.ledger.session(session_id)
            if any(
                _upper(action["state"])
                not in {
                    "RECONCILED",
                    "PROVEN_NOT_ACCEPTED",
                    "AMBIGUOUS_PROVEN_NOT_ACCEPTED",
                }
                for action in actions
            ):
                raise BinanceSpotBoundaryBlocked("durable action reconciliation remains")
            closed_by_client = {
                _order_client_id(row): row for row in truth.closed_orders
            }
            accepted = [
                action
                for action in actions
                if _upper(action["state"]) == "RECONCILED"
            ]
            filled_kinds: set[str] = set()
            for action in accepted:
                client_id = _text(action["client_order_id"])
                row = closed_by_client.get(client_id, {})
                if _upper(row.get("status")) != "FILLED":
                    continue
                matching_fills = [
                    fill for fill in truth.fills if _order_client_id(fill) == client_id
                ]
                executed_quantity = _decimal(
                    row.get("executedQty"), label="final FILLED executedQty"
                )
                executed_quote = _decimal(
                    row.get("cummulativeQuoteQty"),
                    label="final FILLED cummulativeQuoteQty",
                )
                aggregate_quantity = sum(
                    (
                        _decimal(fill.get("quantity"), label="final fill quantity")
                        for fill in matching_fills
                    ),
                    Decimal("0"),
                )
                aggregate_quote = sum(
                    (
                        _decimal(
                            fill.get("quoteQuantity"),
                            label="final fill quoteQuantity",
                        )
                        for fill in matching_fills
                    ),
                    Decimal("0"),
                )
                if (
                    executed_quantity <= 0
                    or executed_quote <= 0
                    or aggregate_quantity != executed_quantity
                    or aggregate_quote != executed_quote
                ):
                    raise BinanceSpotBoundaryBlocked(
                        "terminal FILLED leg lacks exact positive aggregate fill truth"
                    )
                filled_kinds.add(_upper(action["action_kind"]))
            terminal_exclusivity = self._require_exclusivity(
                phase="TERMINAL",
                session_id=session_id,
                permit=permit,
                boundary_id=f"{session_id}:terminal",
                boundary_hash=(
                    truth.official_rest_truth_hash
                    if _SHA256_RE.fullmatch(truth.official_rest_truth_hash)
                    else _stable_hash(dict(account_truth))
                ),
                coverage_started_epoch=float(
                    session.get("exclusivity_coverage_started_epoch")
                    or session["started_epoch"]
                ),
                # A signed SAFE_INCOMPLETE proof may honestly report that a
                # causal account-wide terminal barrier is unavailable.  It may
                # finalize cleanup, but can never become PASS/REAL_E2E.
                require_causal_closure=False,
            )
            independent_causal_closure_proven = bool(
                terminal_exclusivity.get("accountWideCausalClosureProven")
                is True
            )
            assurance_mode = _text(
                terminal_exclusivity.get("assuranceMode")
            ).upper()
            supervised_non_promotion = (
                assurance_mode == "SUPERVISED_NON_PROMOTION"
            )
            exclusivity_phase_chain = self._exclusivity_phase_chain(
                session_id=session_id,
                permit=permit,
                actions=actions,
                terminal_proof=dict(terminal_exclusivity.get("proof") or {}),
            )
            if {"BUY", "SELL"}.issubset(filled_kinds):
                if not metrics["feesQuoteExact"]:
                    raise BinanceSpotBoundaryBlocked(
                        "full round-trip fee valuation is incomplete"
                    )
                outcome = "PASS_FULL_ROUND_TRIP"
            elif "BUY" in filled_kinds and any(
                kind.startswith("CLEANUP_SELL") for kind in filled_kinds
            ):
                outcome = "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT"
            elif not int(session["buy_claimed"]):
                outcome = "INCONCLUSIVE_NO_SIGNAL"
            else:
                outcome = "INCONCLUSIVE_NO_ENTRY"
            if metrics["feesQuoteExact"] and metrics["ownerLoss"] >= MAX_OWNER_LOSS:
                outcome = "SAFE_INCOMPLETE_OWNER_LOSS_LIMIT_REACHED"
            if recovered_stream_gap:
                outcome = "SAFE_INCOMPLETE_RECOVERED_STREAM_GAP"
            elif (
                outcome == "PASS_FULL_ROUND_TRIP"
                and not exact_two_hour_runtime_complete
            ):
                outcome = "SAFE_INCOMPLETE_EARLY_TERMINATION"
            elif (
                outcome == "PASS_FULL_ROUND_TRIP"
                and exclusivity_phase_chain.get("complete") is not True
            ):
                outcome = (
                    "SAFE_INCOMPLETE_ACCOUNT_EXCLUSIVITY_PHASE_CHAIN_UNPROVEN"
                )
            elif (
                outcome == "PASS_FULL_ROUND_TRIP"
                and supervised_non_promotion
            ):
                outcome = "SAFE_INCOMPLETE_SUPERVISED_NON_PROMOTION"
            elif (
                outcome == "PASS_FULL_ROUND_TRIP"
                and not independent_causal_closure_proven
            ):
                # Binance documents neither a causal ordering guarantee
                # between the generic WebSocket time RPC and account events,
                # nor an account-wide all-symbol closed-order/trade endpoint.
                # A clean BTCUSDT round trip proves wiring, but cannot be
                # labeled PASS for an exclusive account without that proof.
                outcome = "SAFE_INCOMPLETE_ACCOUNT_WIDE_CAUSAL_CLOSURE_UNPROVEN"
            final_authority = FunctionalAuthority.parse(self.authority_reader())
            # The application must first atomically clear the active session
            # pointer/capability while leaving global new entries blocked.
            final_authority.assert_final_reset()
            natural_round_trip_filled = {"BUY", "SELL"}.issubset(
                filled_kinds
            )
            natural_action_kinds = [
                _upper(action["action_kind"])
                for action in actions
                if _upper(action["action_kind"]) in {"BUY", "SELL"}
            ]
            order_caps_and_no_reentry_proven = bool(
                natural_action_kinds.count("BUY") == 1
                and natural_action_kinds.count("SELL") == 1
                and len(natural_action_kinds) == 2
                and metrics["buyQuote"] <= MAX_ORDER_NOTIONAL
                and metrics["ownedQuantity"] * truth.mark_price
                <= MAX_GROSS_EXPOSURE
            )
            preexisting_baseline_preserved = (
                truth.base_total - residual >= baseline_base
            )
            baseline_restored_within_precision = bool(
                (residual == 0 and truth.base_total >= baseline_base)
                or (
                    residual_is_dust
                    and truth.base_total - residual >= baseline_base
                    and bool(metrics["feesQuoteExact"])
                    and metrics["ownerLoss"] < MAX_OWNER_LOSS
                )
            )
            orderable_residual_zero = residual == 0 or residual_is_dust
            functional_wiring_passed = bool(
                natural_round_trip_filled
                and exact_two_hour_runtime_complete
                and runtime_clock_consistency_proven
                and order_caps_and_no_reentry_proven
                and metrics["feesQuoteExact"]
                and metrics["ownerLoss"] < MAX_OWNER_LOSS
                and preexisting_baseline_preserved
                and baseline_restored_within_precision
                and orderable_residual_zero
                and not truth.open_orders
                and truth.external_activity_absent
                and terminal_exclusivity.get("verified") is True
                and exclusivity_phase_chain.get("complete") is True
                and exclusivity_phase_chain.get("restartVerifiable") is True
                and independent_causal_closure_proven
                and not recovered_stream_gap
                and _SHA256_RE.fullmatch(truth.stream_journal_seal_hash)
                is not None
                and truth.stream_journal_event_count >= 0
                and _SHA256_RE.fullmatch(truth.official_rest_truth_hash)
                is not None
                and bool(truth.official_rest_snapshot)
            )
            supervised_functional_wiring_passed = bool(
                supervised_non_promotion
                and natural_round_trip_filled
                and exact_two_hour_runtime_complete
                and runtime_clock_consistency_proven
                and order_caps_and_no_reentry_proven
                and metrics["feesQuoteExact"]
                and metrics["ownerLoss"] < MAX_OWNER_LOSS
                and preexisting_baseline_preserved
                and baseline_restored_within_precision
                and orderable_residual_zero
                and not truth.open_orders
                and truth.external_activity_absent
                and terminal_exclusivity.get("verified") is True
                and terminal_exclusivity.get("supervisedControlsVerified")
                is True
                and terminal_exclusivity.get("noManualTradingConfirmed") is True
                and terminal_exclusivity.get("noBotsConfirmed") is True
                and terminal_exclusivity.get("noOtherApiKeysConfirmed") is False
                and exclusivity_phase_chain.get("complete") is True
                and exclusivity_phase_chain.get("restartVerifiable") is True
                and not independent_causal_closure_proven
                and not recovered_stream_gap
                and _SHA256_RE.fullmatch(truth.stream_journal_seal_hash)
                is not None
                and truth.stream_journal_event_count >= 0
                and _SHA256_RE.fullmatch(truth.official_rest_truth_hash)
                is not None
                and bool(truth.official_rest_snapshot)
            )
            # This normalized official snapshot is intentionally not embedded
            # in the producer-authored evidence document.  The ledger stores it
            # in a separate immutable table in the same final transaction, and
            # the first-live consumer later joins it with durable actions and
            # the independently archived user-stream rows.
            terminal_official_truth = {
                "schemaVersion": (
                    "binance-spot-functional-terminal-official-truth/v1"
                ),
                "sessionId": session_id,
                "permitId": _text(session["permit_id"]),
                "permitHash": _text(session["permit_hash"]).lower(),
                "accountFingerprint": permit.binding.account_fingerprint,
                "observedEpoch": truth.observed_epoch,
                "historyBaselineEpoch": truth.history_baseline_epoch,
                "historyCutoffEpoch": truth.history_cutoff_epoch,
                "baselineBase": _decimal_text(baseline_base),
                "balances": [
                    dict(row)
                    for row in account_truth.get("balances", [])
                    if isinstance(row, Mapping)
                ],
                "finalBaseTotal": _decimal_text(truth.base_total),
                "finalQuoteTotal": _decimal_text(truth.quote_total),
                "markPrice": _decimal_text(truth.mark_price),
                "accountOpenOrders": [dict(row) for row in truth.open_orders],
                "closedOrders": [dict(row) for row in truth.closed_orders],
                "fills": [dict(row) for row in truth.fills],
                "feeQuoteValuationComplete": (
                    truth.fee_quote_valuation_complete
                ),
                "externalActivityAbsent": truth.external_activity_absent,
                "nativeAccountWideCausalClosureProven": (
                    truth.account_wide_causal_closure_proven
                ),
                "accountWideCausalClosureProven": (
                    independent_causal_closure_proven
                ),
                "assuranceMode": assurance_mode,
                "supervisedNonPromotion": supervised_non_promotion,
                "supervisedFunctionalWiringPassed": (
                    supervised_functional_wiring_passed
                ),
                "promotionEligible": False,
                "realE2EEligible": False,
                "accountExclusivityProof": dict(
                    terminal_exclusivity.get("proof") or {}
                ),
                "accountExclusivityProofHash": _text(
                    terminal_exclusivity.get("proofHash")
                ).lower(),
                "accountExclusivityPhaseChainComplete": (
                    exclusivity_phase_chain.get("complete") is True
                ),
                "accountExclusivityPhaseChainHash": _text(
                    exclusivity_phase_chain.get("phaseChainHash")
                ).lower(),
                "accountExclusivityPhaseProofCount": int(
                    exclusivity_phase_chain.get("recordCount") or 0
                ),
                "accountExclusivityPhaseProofRequiredCount": int(
                    exclusivity_phase_chain.get("requiredRecordCount") or 0
                ),
                "accountExclusivityRestartVerifiable": (
                    exclusivity_phase_chain.get("restartVerifiable") is True
                ),
                "streamSessionId": truth.stream_session_id,
                "streamPermitId": truth.stream_permit_id,
                "streamPermitHash": truth.stream_permit_hash,
                "streamJournalSealHash": truth.stream_journal_seal_hash,
                "streamJournalEventCount": truth.stream_journal_event_count,
                "accountSymbolPermissionProofHash": (
                    truth.account_symbol_permission_proof_hash
                ),
                "officialRestSnapshot": dict(truth.official_rest_snapshot),
                "officialRestTruthHash": truth.official_rest_truth_hash,
                "rules": dict(exchange_info_truth or {}),
            }
            terminal_official_truth_hash = _stable_hash(terminal_official_truth)
            evidence = {
                "schemaVersion": SCHEMA_VERSION,
                "sessionId": session_id,
                "permitId": session["permit_id"],
                "permitHash": session["permit_hash"],
                "bindingHash": session["binding_hash"],
                "capabilitySealHash": session["capability_seal_hash"],
                "buyClaimed": bool(session["buy_claimed"]),
                "sellClaimed": bool(session["sell_claimed"]),
                "reconciledActionCount": len(actions),
                "ownerLoss": _decimal_text(metrics["ownerLoss"]),
                "feesQuote": _decimal_text(metrics["feesQuote"]),
                "feesQuoteExact": bool(metrics["feesQuoteExact"]),
                "thirdAssetFees": list(metrics["thirdAssetFees"]),
                "baselineFlat": residual == 0,
                "preexistingBaselinePreserved": preexisting_baseline_preserved,
                "baselineRestoredWithinExchangePrecision": (
                    baseline_restored_within_precision
                ),
                "orderableResidualZero": orderable_residual_zero,
                "ownedResidual": _decimal_text(residual),
                "ownedResidualQuoteValue": _decimal_text(
                    residual * truth.mark_price
                ),
                "unorderableOwnedDust": residual_is_dust,
                "residualFilterProof": (
                    {
                        "stepSize": _decimal_text(residual_rules.step_size),
                        "minQty": _decimal_text(residual_rules.min_quantity),
                        "minNotional": _decimal_text(residual_rules.min_notional),
                        "orderableQty": _decimal_text(
                            residual_rules.floor_quantity(residual)
                        ),
                        "proofHash": _stable_hash(dict(exchange_info_truth or {})),
                    }
                    if residual_rules is not None
                    else None
                ),
                "outcome": outcome,
                "actualRuntimeSeconds": _decimal_text(
                    actual_duration_seconds
                ),
                "exchangeRuntimeSeconds": _decimal_text(
                    exchange_duration_seconds
                ),
                "monotonicRuntimeSeconds": (
                    _decimal_text(monotonic_duration_seconds)
                    if monotonic_duration_seconds is not None
                    else ""
                ),
                "runtimeClockConsistencyProven": (
                    runtime_clock_consistency_proven
                ),
                "activatedEpoch": _decimal_text(
                    Decimal(str(session["started_epoch"]))
                ),
                "activeEndsEpoch": _decimal_text(
                    Decimal(str(session["expires_epoch"]))
                ),
                "terminalObservedEpoch": _decimal_text(
                    Decimal(str(terminal_observed_epoch))
                ),
                "requiredRuntimeSeconds": int(PERMIT_SECONDS),
                "exactTwoHourRuntimeComplete": (
                    exact_two_hour_runtime_complete
                ),
                "exclusiveAccountRequired": True,
                "exclusiveAccountOperatorAttested": False,
                "exclusiveAccountIndependentlyProven": (
                    terminal_exclusivity.get("exclusiveAccountConfirmed")
                    is True
                ),
                "noManualTradingAttested": bool(
                    terminal_exclusivity.get(
                        "supervisedNoManualTradingAttested"
                    )
                    is True
                ),
                "noManualTradingIndependentlyProven": (
                    terminal_exclusivity.get("noManualTradingConfirmed") is True
                ),
                "noExternalBotsAttested": bool(
                    terminal_exclusivity.get("supervisedNoOtherBotsAttested")
                    is True
                ),
                "noExternalBotsIndependentlyProven": (
                    terminal_exclusivity.get("noBotsConfirmed") is True
                ),
                "noOtherApiKeysAttested": False,
                "noOtherApiKeysIndependentlyProven": (
                    terminal_exclusivity.get("noOtherApiKeysConfirmed") is True
                ),
                "accountWideCausalClosureProven": (
                    independent_causal_closure_proven
                ),
                "nativeAccountWideCausalClosureProven": (
                    truth.account_wide_causal_closure_proven
                ),
                "otherApiKeysAbsenceAuthoritativelyProven": (
                    terminal_exclusivity.get("noOtherApiKeysConfirmed") is True
                ),
                "otherApiKeyInventoryResidualUnknown": (
                    terminal_exclusivity.get("noOtherApiKeysConfirmed") is False
                ),
                "assuranceMode": assurance_mode,
                "supervisedNonPromotion": supervised_non_promotion,
                "supervisedControlsVerified": bool(
                    terminal_exclusivity.get("supervisedControlsVerified")
                    is True
                ),
                "accountExclusivityProof": dict(
                    terminal_exclusivity.get("proof") or {}
                ),
                "accountExclusivityProofHash": _text(
                    terminal_exclusivity.get("proofHash")
                ).lower(),
                "accountExclusivityProofDurable": (
                    terminal_exclusivity.get("durable") is True
                ),
                "accountExclusivityPhaseChainComplete": (
                    exclusivity_phase_chain.get("complete") is True
                ),
                "accountExclusivityPhaseChainHash": _text(
                    exclusivity_phase_chain.get("phaseChainHash")
                ).lower(),
                "accountExclusivityPhaseProofCount": int(
                    exclusivity_phase_chain.get("recordCount") or 0
                ),
                "accountExclusivityPhaseProofRequiredCount": int(
                    exclusivity_phase_chain.get("requiredRecordCount") or 0
                ),
                "accountExclusivityRestartVerifiable": (
                    exclusivity_phase_chain.get("restartVerifiable") is True
                ),
                "privateStreamGapRecoveredCleanupOnly": recovered_stream_gap,
                "streamGapEvidenceHash": (
                    truth.stream_gap_evidence_hash if recovered_stream_gap else ""
                ),
                "restCleanupRecoveryAttestationHash": (
                    truth.recovery_attestation_hash if recovered_stream_gap else ""
                ),
                "streamJournalSealHash": truth.stream_journal_seal_hash,
                "streamJournalEventCount": truth.stream_journal_event_count,
                "cleanupWiringPassed": (
                    "BUY" in filled_kinds
                    and any(
                        kind.startswith("CLEANUP_SELL")
                        for kind in filled_kinds
                    )
                ),
                "naturalBuyFilled": "BUY" in filled_kinds,
                "naturalSellFilled": "SELL" in filled_kinds,
                "fullRoundTripWiringPassed": {"BUY", "SELL"}.issubset(
                    filled_kinds
                ),
                "orderCapsAndNoReentryProven": (
                    order_caps_and_no_reentry_proven
                ),
                "externalActivityAbsent": truth.external_activity_absent,
                "openOrdersZero": not truth.open_orders,
                "terminalOfficialTruthHash": terminal_official_truth_hash,
                "functionalWiringPassed": functional_wiring_passed,
                "supervisedFunctionalWiringPassed": (
                    supervised_functional_wiring_passed
                ),
                "newEntriesBlocked": True,
                "functionalCapabilityReset": True,
                "evidenceClass": EVIDENCE_CLASS,
                "promotionEligible": False,
                "useAsPromotionEvidence": False,
                "fullLiveAllowed": False,
                "productionAvailable": PRODUCTION_AVAILABLE,
                "finalTruthHash": _stable_hash(
                    {
                        "observedEpoch": truth.observed_epoch,
                        "baseTotal": _decimal_text(truth.base_total),
                        "quoteTotal": _decimal_text(truth.quote_total),
                        "openOrders": list(truth.open_orders),
                        "closedOrders": list(truth.closed_orders),
                        "fills": list(truth.fills),
                    }
                ),
            }
            final_writer = (
                self.ledger.prepare_final_with_evidence
                if prepare_only
                else self.ledger.finalize_with_evidence
            )
            result = final_writer(
                session_id,
                evidence=evidence,
                terminal_truth=terminal_official_truth,
                detail=(
                    "complete account truth; baseline or unorderable owned dust; "
                    f"outcome={outcome}; evidence+capability atomically sealed"
                ),
                now_epoch=float(self.clock()),
            )
            durable = self.ledger.final_evidence(session_id)
            return {
                "ok": True,
                "status": "FINAL_PREPARED" if prepare_only else "FINALIZED",
                "evidence": durable["evidence"],
                "evidenceHash": durable["evidenceHash"],
                "functionalCapabilityReset": result["capability_hash"] == "",
            }


__all__ = [
    "AccountTruth",
    "BinanceSpotBoundaryBlocked",
    "BinanceSpotContinuousFunctionalService",
    "BinanceSpotFunctionalError",
    "DuplicateActionClaim",
    "DurableFunctionalLedger",
    "EVIDENCE_CLASS",
    "ExactBinding",
    "ExactPermit",
    "FunctionalAuthority",
    "PRODUCTION_AVAILABLE",
    "SymbolRules",
    "owner_metrics",
]
