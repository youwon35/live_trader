from __future__ import annotations

"""Read-only preparation contract for the Binance Spot first-live lane.

This module deliberately owns no broker transport and no release switch.  It
describes and verifies the durable pieces that must already be present before
the shared state/server layer may consider releasing the isolated first-live
lane.  A positive result means only *prepared while held*; it can never make a
POST eligible.
"""

from functools import lru_cache
import hashlib
import json
import re
from typing import Any, Mapping

from .binance_spot_continuous_functional import (
    MAX_CLEANUP_SECONDS,
    MAX_GROSS_EXPOSURE,
    MAX_ORDER_NOTIONAL,
    MAX_OWNER_LOSS,
    PERMIT_SECONDS,
    BinanceSpotContinuousFunctionalService,
    DurableFunctionalLedger,
    ExactPermit,
    owner_metrics,
)
from .binance_spot_functional_approval import (
    DurableBinanceSpotApprovedPermitStore,
)
from .binance_spot_functional_bootstrap import (
    BOOTSTRAP_DB_SCHEMA_FINGERPRINT,
    DurableBinanceSpotFirstLiveBootstrapStore,
    default_binance_spot_functional_code_hash,
)
from .binance_spot_functional_lifecycle import (
    MAX_OWNER_LEASE_SECONDS,
    BinanceSpotFunctionalLifecycleManager,
    DurableBinanceSpotFunctionalControl,
)
from .binance_spot_functional_mutation import (
    BinanceSpotFunctionalMutationEdge,
)


HOLD_PREPARATION_SCHEMA_VERSION = (
    "binance-spot-functional-hold-preparation/v1"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_APPROVAL_HOOKS = (
    "issue_candidate",
    "approve_issued_candidate",
    "claim",
    "bind_session",
    "startup_fail_lost_claim",
    "startup_bind_lost_claim_to_cleanup",
    "audit_orphaned_claims",
    "consume",
)
_BOOTSTRAP_HOOKS = (
    "issue",
    "claim",
    "bind_session",
    "fail_orphans_after_process_loss",
    "consume_terminal",
)
_CONTROL_HOOKS = (
    "arm",
    "activate",
    "heartbeat",
    "audit_all_incomplete_startup",
    "begin_cleanup",
    "takeover_expired_cleanup",
    "prepare_final_reset",
    "seal_final",
)
_LEDGER_HOOKS = (
    "create_session",
    "claim_action",
    "mark_post_may_have_crossed",
    "prepare_final_with_evidence",
    "commit_prepared_final",
)
_LIFECYCLE_HOOKS = (
    "start",
    "heartbeat",
    "audit_incomplete_startup",
    "takeover_expired_cleanup",
    "tick",
    "begin_cleanup",
    "finalize",
)
_SERVICE_HOOKS = (
    "start",
    "observe_bar",
    "dispatch_claim",
    "recover",
    "finalize",
)
_MUTATION_HOOKS = ("__call__", "_dispatch_under_lease")


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _hooks_available(owner: type[Any], names: tuple[str, ...]) -> bool:
    return all(callable(getattr(owner, name, None)) for name in names)


@lru_cache(maxsize=1)
def _production_code_hash() -> tuple[str, str]:
    """Return the transitive authority hash or a redacted failure label."""

    try:
        value = str(default_binance_spot_functional_code_hash()).lower()
    except Exception as exc:  # fail closed without leaking a local path
        return "", type(exc).__name__
    if _SHA256_RE.fullmatch(value) is None:
        return "", "InvalidCodeHash"
    return value, ""


def binance_spot_functional_hold_preparation_status(
    *,
    root_integration_released: bool = False,
) -> dict[str, Any]:
    """Verify internal first-live prerequisites without opening authority.

    ``root_integration_released`` is observational only.  This status remains
    HOLD even when a caller passes true; the final release decision belongs to
    the shared state/server composition and the production availability gates.
    """

    hooks = {
        "durableApproval": _hooks_available(
            DurableBinanceSpotApprovedPermitStore, _APPROVAL_HOOKS
        ),
        "durableFirstLiveBootstrap": _hooks_available(
            DurableBinanceSpotFirstLiveBootstrapStore, _BOOTSTRAP_HOOKS
        ),
        "durableOwnerControl": _hooks_available(
            DurableBinanceSpotFunctionalControl, _CONTROL_HOOKS
        ),
        "durableSessionLedger": _hooks_available(
            DurableFunctionalLedger, _LEDGER_HOOKS
        ),
        "restartCleanupLifecycle": _hooks_available(
            BinanceSpotFunctionalLifecycleManager, _LIFECYCLE_HOOKS
        ),
        "sessionOwnedService": _hooks_available(
            BinanceSpotContinuousFunctionalService, _SERVICE_HOOKS
        ),
        "singleAttemptMutation": _hooks_available(
            BinanceSpotFunctionalMutationEdge, _MUTATION_HOOKS
        ),
        "ownerMetricBaseline": callable(owner_metrics),
        "exactPermitParser": callable(getattr(ExactPermit, "parse", None)),
    }
    code_hash, code_hash_error = _production_code_hash()
    exact_limits = bool(
        PERMIT_SECONDS == 7200
        and MAX_CLEANUP_SECONDS == 10800
        and MAX_OWNER_LEASE_SECONDS > 0
        and MAX_OWNER_LEASE_SECONDS <= 60
        and str(MAX_ORDER_NOTIONAL) == "10"
        and str(MAX_GROSS_EXPOSURE) == "10"
        and str(MAX_OWNER_LOSS) == "1"
        and _SHA256_RE.fullmatch(
            str(BOOTSTRAP_DB_SCHEMA_FINGERPRINT).lower()
        )
        is not None
    )
    structural_ready = bool(
        all(hooks.values()) and exact_limits and code_hash and not code_hash_error
    )
    contract = {
        "schemaVersion": HOLD_PREPARATION_SCHEMA_VERSION,
        "route": "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m",
        "environment": "BINANCE_LIVE",
        "activeDurationSeconds": 7200,
        "cleanupDeadlineFromActivationSeconds": 10800,
        "ownerLeaseMaxSeconds": 60,
        "maxOrderNotional": "10",
        "maxGrossExposure": "10",
        "maxOwnerLoss": "1",
        "maxBuyOrders": 1,
        "maxSellOrders": 1,
        "noReentry": True,
        "activationResealRequired": True,
        "exclusiveAccountRequired": True,
        "manualTradingAllowed": False,
        "externalBotsAllowed": False,
        "otherApiKeysAllowed": False,
        "futuresAllowed": False,
        "marginAllowed": False,
        "borrowAllowed": False,
        "transferAllowed": False,
        "withdrawalAllowed": False,
        "preexistingBaseBalancePolicy": "IMMUTABLE_BASELINE",
        "cleanupQuantityPolicy": "SESSION_OWNED_DELTA_ONLY",
        "restartEntryPolicy": "REVOKE_ENTRY_ROTATE_CLEANUP_ONLY",
        "ambiguousSubmitPolicy": "RECONCILE_NEVER_BLIND_RETRY",
        "promotionEligible": False,
    }
    blockers = [
        "durable-first-live-candidate-not-issued",
        "typed-final-order-approval-not-present",
    ]
    if not root_integration_released:
        blockers.insert(0, "root-state-server-integration-not-released")
    if not structural_ready:
        blockers.insert(0, "binance-internal-preparation-contract-invalid")
    return {
        "schemaVersion": HOLD_PREPARATION_SCHEMA_VERSION,
        "preparedPrerequisites": structural_ready,
        "holdEnforced": True,
        "rootIntegrationReleased": bool(root_integration_released),
        "releaseAvailable": False,
        "candidateIssuanceAllowed": False,
        "networkOrderPostAllowed": False,
        "productionCodeHash": code_hash,
        "productionCodeHashError": code_hash_error,
        "bootstrapSchemaFingerprint": str(
            BOOTSTRAP_DB_SCHEMA_FINGERPRINT
        ).lower(),
        "preparationContract": contract,
        "preparationContractHash": _hash(contract),
        "verifiedHooks": hooks,
        "rootIntegrationHooks": {
            "prepare": "prepare_binance_spot_functional_backend_state",
            "status": "binance_spot_functional_backend_state_status",
            "candidate": "preissue_binance_spot_functional_candidate",
            "start": "start_binance_spot_functional_backend_state",
            "stop": "stop_binance_spot_functional_backend_state",
            "recover": "recover_binance_spot_functional_backend_state",
            "finalReleaseLatch": (
                "BINANCE_SPOT_FUNCTIONAL_ROOT_INTEGRATION_RELEASED"
            ),
        },
        "releaseBlockers": blockers,
    }


__all__ = [
    "HOLD_PREPARATION_SCHEMA_VERSION",
    "binance_spot_functional_hold_preparation_status",
]
