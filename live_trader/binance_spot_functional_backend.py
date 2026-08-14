from __future__ import annotations

"""State-owned backend boundary for the Binance Spot functional lane.

No public command accepts a permit document, bar, signal, broker payload,
account fingerprint, owner token, or functional capability.  The server
selects and persists one immutable candidate; this singleton keeps every raw
runtime secret inside a private handle vault and owns the scheduler thread.
"""

from datetime import datetime, timedelta, timezone
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from trading_runtime.functional_test import (
    FunctionalTestBinding,
    FunctionalTestDurationUnit,
    FunctionalTestEnvironment,
    issue_functional_test_permit,
)

from .binance_spot_continuous_functional import (
    ExactBinding,
    ExactPermit,
    permit_content_hash,
)
from .binance_spot_functional_approval import (
    DurableBinanceSpotApprovedPermitStore,
)
from .binance_spot_functional_bootstrap import (
    DurableBinanceSpotFirstLiveBootstrapStore,
    default_binance_spot_functional_code_hash,
)
from .binance_spot_functional_lifecycle import (
    BinanceSpotFunctionalLifecycleManager,
    BinanceSpotLifecycleError,
    LifecycleHandle,
    build_binance_spot_production_lifecycle,
    composite_production_available,
    production_entrypoint_status,
)
from .binance_spot_functional_scheduler import (
    BinanceSpotFunctionalManagedScheduler,
)
from .binance_spot_functional_transport import (
    assert_binance_spot_production_origin,
    binance_api_key_fingerprint,
)
from .binance_spot_publication import (
    load_binance_spot_publication_binding,
)
from .binance_spot_stream_journal import (
    BinanceSpotDurableStreamBridge,
    DurableBinanceSpotUserStreamJournal,
)
from .execution_streams import ExecutionStreamManager
from .live_adapters import BINANCE_BASE_URL, env_value
from .process_safety import live_trader_instance_lease_status


BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE = False
BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE = False
BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE = False
BINANCE_SPOT_FUNCTIONAL_FIRST_LIVE_BOOTSTRAP_AVAILABLE = False
BINANCE_SPOT_FUNCTIONAL_ORDINARY_FENCE_AVAILABLE = False
BINANCE_SPOT_FUNCTIONAL_EMERGENCY_FENCE_AVAILABLE = False
BINANCE_SPOT_FUNCTIONAL_EXCLUSIVE_ACCOUNT_AVAILABLE = False
# Final shared state/server/live-adapter integration owns this last latch.  It
# stays false in the Binance-only preparation tranche even if every internal
# implementation flag is later proven independently.
BINANCE_SPOT_FUNCTIONAL_ROOT_INTEGRATION_RELEASED = False
BINANCE_SPOT_FUNCTIONAL_ROUTE_KEY = (
    "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m"
)
_SINGLETON_LOCK = threading.RLock()
_SINGLETON: "BinanceSpotFunctionalBackendManager | None" = None
_STARTUP_ATTESTATION_LOCK = threading.RLock()
_STARTUP_ATTESTATION_SECRET = object()
_STARTUP_ATTESTATION_MINTED = False
_STARTUP_ATTESTATION_CONSUMED = False
_FORBIDDEN_COMMAND_FIELDS = frozenset(
    {
        "permit",
        "permitPayload",
        "bar",
        "finalizedBar",
        "signal",
        "evaluation",
        "capability",
        "ownerToken",
        "accountFingerprint",
        "binding",
        "caps",
        "order",
        "payload",
    }
)


class BinanceSpotFunctionalBackendError(RuntimeError):
    pass


def _mint_startup_owner_absence_attestation() -> object:
    """Mint once after the official process lease proves the old owner died."""

    global _STARTUP_ATTESTATION_MINTED
    lease = live_trader_instance_lease_status()
    if lease.get("acquired") is not True:
        raise BinanceSpotFunctionalBackendError(
            "official Live Trader application-instance lease is not held"
        )
    with _STARTUP_ATTESTATION_LOCK:
        if _STARTUP_ATTESTATION_MINTED:
            raise BinanceSpotFunctionalBackendError(
                "startup owner-absence attestation was already minted"
            )
        _STARTUP_ATTESTATION_MINTED = True
        return _STARTUP_ATTESTATION_SECRET


def _consume_startup_owner_absence_attestation(value: object | None) -> bool:
    global _STARTUP_ATTESTATION_CONSUMED
    if value is None:
        return False
    with _STARTUP_ATTESTATION_LOCK:
        if (
            value is not _STARTUP_ATTESTATION_SECRET
            or not _STARTUP_ATTESTATION_MINTED
            or _STARTUP_ATTESTATION_CONSUMED
        ):
            raise BinanceSpotFunctionalBackendError(
                "startup owner-absence attestation is invalid or consumed"
            )
        _STARTUP_ATTESTATION_CONSUMED = True
        return True


def binance_spot_functional_composite_available() -> bool:
    """The only live release gate; partial flag flips cannot arm the lane."""

    return all(
        (
            composite_production_available(),
            BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_ORDINARY_FENCE_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_EMERGENCY_FENCE_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_EXCLUSIVE_ACCOUNT_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_ROOT_INTEGRATION_RELEASED,
        )
    )


def binance_spot_first_live_bootstrap_available() -> bool:
    """Allow one E2E to bypass only the not-yet-earned REAL_E2E bit."""

    return all(
        (
            composite_production_available(),
            BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_FIRST_LIVE_BOOTSTRAP_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_ORDINARY_FENCE_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_EMERGENCY_FENCE_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_EXCLUSIVE_ACCOUNT_AVAILABLE,
            BINANCE_SPOT_FUNCTIONAL_ROOT_INTEGRATION_RELEASED,
            not BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE,
        )
    )


def _utc(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _exact_route_lock(value: Mapping[str, Any]) -> bool:
    expected = {
        "globalRealOrdersEnabled": False,
        "ordinaryRuntimeActive": False,
        "binanceSpotOrdinaryRouteClosed": True,
        "binanceSmokeRouteClosed": True,
        "binanceFuturesRouteClosed": True,
        "marginRouteClosed": True,
        "withdrawalRouteClosed": True,
    }
    return set(value) == set(expected) and all(
        value.get(field) is expected_value
        for field, expected_value in expected.items()
    )


def issue_binance_spot_functional_permit(
    *, binding: ExactBinding, now_epoch: float
) -> dict[str, Any]:
    """Issue only the fixed nonpromotion 2h/10 USDT/1 USDT permit."""

    now = datetime.fromtimestamp(float(now_epoch), tz=timezone.utc)
    shared = issue_functional_test_permit(
        binding=FunctionalTestBinding(
            strategy_artifact_id=binding.strategy_artifact_id,
            strategy_artifact_hash=binding.strategy_artifact_hash,
            strategy_instance_id=binding.strategy_instance_id,
            portfolio_required=False,
            portfolio_artifact_id="",
            portfolio_artifact_hash="",
            portfolio_instance_id="",
            account_id=binding.account_fingerprint,
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
        now=now,
    ).to_dict()
    payload: dict[str, Any] = {
        "schemaVersion": "binance-spot-continuous-functional-v1",
        "permitId": shared["permitId"],
        "permitHash": "",
        "sharedPermit": shared,
        "sharedPermitContentHash": shared["contentHash"],
        "environment": "BINANCE_LIVE",
        "status": "ACTIVE",
        "functionalOnly": True,
        "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
        "promotionEligible": False,
        "issuedAt": _utc(float(now_epoch)),
        "expiresAt": _utc(float(now_epoch) + 7200),
        "cleanupDeadlineAt": _utc(float(now_epoch) + 10800),
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
        "binding": binding.payload(),
    }
    payload["permitHash"] = permit_content_hash(payload)
    ExactPermit.parse(payload, now_epoch=float(now_epoch))
    return payload


class BinanceSpotFunctionalBackendManager:
    """Single state-owned command surface and private lifecycle handle vault."""

    def __init__(
        self,
        *,
        manager: BinanceSpotFunctionalLifecycleManager,
        approval_store: DurableBinanceSpotApprovedPermitStore,
        binding_reader: Callable[[], ExactBinding | Mapping[str, Any]],
        operator_confirmation_verifier: Callable[[Mapping[str, Any]], bool],
        server_record_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        route_lock_reader: Callable[[], Mapping[str, Any]],
        clock: Callable[[], float] = time.time,
        scheduler_factory: Callable[..., Any] = BinanceSpotFunctionalManagedScheduler,
        stream_start: Callable[[], None] | None = None,
        stream_ready: Callable[[], bool] | None = None,
        stream_stop: Callable[[], None] | None = None,
        terminal_callback: Callable[[Mapping[str, Any]], None] | None = None,
        first_live_bootstrap_store: (
            DurableBinanceSpotFirstLiveBootstrapStore | None
        ) = None,
        first_live_gate_reader: Callable[[], Mapping[str, Any]] | None = None,
        allow_mock_backend: bool = False,
    ) -> None:
        self.manager = manager
        self.approval_store = approval_store
        self.binding_reader = binding_reader
        self.operator_confirmation_verifier = operator_confirmation_verifier
        self.server_record_signer = server_record_signer
        self.route_lock_reader = route_lock_reader
        self.clock = clock
        self.scheduler_factory = scheduler_factory
        self.stream_start = stream_start
        self.stream_ready = stream_ready
        self.stream_stop = stream_stop
        self.terminal_callback = terminal_callback
        self.first_live_bootstrap_store = first_live_bootstrap_store
        self.first_live_gate_reader = first_live_gate_reader
        self.allow_mock_backend = bool(allow_mock_backend)
        self._lock = threading.RLock()
        self._generation = 0
        self._handle: LifecycleHandle | None = None
        self._scheduler_thread: threading.Thread | None = None
        self._scheduler_stop = threading.Event()
        self._terminal_state = "IDLE"
        self._terminal_detail = ""
        self._last_result: dict[str, Any] = {}
        self._scheduler_escape_count = 0
        self._first_live_vault: dict[str, tuple[str, str]] = {}
        self._active_first_live_bootstrap_id = ""
        # Startup recovery may need to terminalize the bootstrap lineage, but
        # this identifier is deliberately separate from the live-entry field
        # used by networkOrderPostAllowed below.
        self._recovered_first_live_terminal_id = ""
        try:
            self._startup_audit = manager.audit_incomplete_startup()
        except Exception as exc:
            if not self.allow_mock_backend:
                raise
            self._startup_audit = {
                "startupRecovery": "MOCK_AUDIT_BLOCKED",
                "detail": f"{type(exc).__name__}:{str(exc)[:200]}",
            }
        if isinstance(self._startup_audit, LifecycleHandle):
            # Startup audit rotates a cleanup-only secret exactly once.  It
            # must enter the private vault immediately; returning from the
            # constructor without scheduling would lose the only live handle
            # and strand owned cleanup until its next expiry.
            self._handle = self._startup_audit
            if self.first_live_bootstrap_store is not None:
                recovered_bootstrap = (
                    self.first_live_bootstrap_store.active_terminal_pointer_for_session(
                        self._startup_audit.session_id
                    )
                )
                if recovered_bootstrap is not None:
                    self._recovered_first_live_terminal_id = str(
                        recovered_bootstrap["bootstrap_id"]
                    )
            self._generation += 1
            self._terminal_state = "CLEANUP"
            self._terminal_detail = (
                "startup audit recovered REST-only cleanup owner"
            )
            self._try_start_cleanup_scheduler_locked(
                self._generation,
                self._startup_audit,
                source="constructor startup recovery",
            )

    @staticmethod
    def _assert_fields(command: Mapping[str, Any], exact: set[str]) -> None:
        if set(command) != exact or _FORBIDDEN_COMMAND_FIELDS & set(command):
            raise BinanceSpotFunctionalBackendError(
                "Binance functional backend command fields are not exact"
            )

    def _verify_operator(self, command: Mapping[str, Any]) -> None:
        confirmation = command.get("operatorConfirmation")
        if (
            not isinstance(confirmation, Mapping)
            or confirmation.get("authenticated") is not True
            or confirmation.get("confirmed") is not True
            or self.operator_confirmation_verifier(dict(confirmation)) is not True
        ):
            raise BinanceSpotFunctionalBackendError(
                "server-authenticated operator confirmation is invalid"
            )

    def _assert_route_lock(self) -> dict[str, Any]:
        lock = dict(self.route_lock_reader())
        if not _exact_route_lock(lock):
            raise BinanceSpotFunctionalBackendError(
                "ordinary/smoke/futures/margin/withdrawal routes are not closed"
            )
        return lock

    def _binding(self) -> ExactBinding:
        value = self.binding_reader()
        return value if isinstance(value, ExactBinding) else ExactBinding.parse(value)

    def _candidate_gate_available(self) -> bool:
        if self.allow_mock_backend:
            return True
        return bool(
            binance_spot_functional_composite_available()
            or binance_spot_first_live_bootstrap_available()
        )

    def _first_live_required(self) -> bool:
        return bool(
            self.first_live_bootstrap_store is not None
            and not BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE
        )

    def preissue_candidate(self, requested_approval_id: str = "") -> dict[str, Any]:
        """Persist an inert candidate selected entirely by the backend."""

        with self._lock:
            self._assert_route_lock()
            if not self._candidate_gate_available():
                raise BinanceSpotFunctionalBackendError(
                    "Binance functional backend/full first-live gate is unavailable"
                )
            existing = self.approval_store.issued_pointer()
            requested = str(requested_approval_id or "").strip()
            if existing is not None:
                age = float(self.clock()) - float(
                    existing.get("updated_epoch") or 0
                )
                if age < 0 or age > 300:
                    expired_id = str(existing["approval_id"])
                    self.approval_store.retire_issued_candidate(
                        approval_id=expired_id,
                        detail="inert server candidate expired before operator approval",
                    )
                    if requested:
                        raise BinanceSpotFunctionalBackendError(
                            "requested server permit candidate expired"
                        )
                    existing = None
            if existing is not None:
                if requested and not secrets.compare_digest(
                    requested, str(existing["approval_id"])
                ):
                    raise BinanceSpotFunctionalBackendError(
                        "server permit candidate id changed"
                    )
                result = {
                    "approvalId": str(existing["approval_id"]),
                    "permitId": str(existing["permit_id"]),
                    "permitHash": str(existing["permit_hash"]),
                    "serverManaged": True,
                }
                if self._first_live_required():
                    bootstrap = self.first_live_bootstrap_store.pointer_for_approval(
                        str(existing["approval_id"])
                    )
                    vault = self._first_live_vault.get(
                        str(existing["approval_id"])
                    )
                    if bootstrap is None or vault is None:
                        raise BinanceSpotFunctionalBackendError(
                            "first-live raw capability is absent after process loss"
                        )
                    result["firstLiveBootstrapPending"] = True
                    result.update(
                        {
                            "firstLiveBootstrapId": str(
                                bootstrap["bootstrap_id"]
                            ),
                            "firstLiveBootstrapHash": str(
                                bootstrap["bootstrap_hash"]
                            ),
                            "firstLiveSessionNonceHash": str(
                                bootstrap["session_nonce_hash"]
                            ),
                            "firstLiveCodeHash": str(
                                bootstrap["code_hash"]
                            ),
                            "accountFingerprint": str(
                                bootstrap["account_fingerprint"]
                            ),
                            "bindingHash": str(bootstrap["binding_hash"]),
                        }
                    )
                return result
            if requested:
                raise BinanceSpotFunctionalBackendError(
                    "caller cannot choose a new permit candidate id"
                )
            now = float(self.clock())
            permit = issue_binance_spot_functional_permit(
                binding=self._binding(), now_epoch=now
            )
            approval_id = "binance-functional-approval-" + secrets.token_hex(16)
            bootstrap: dict[str, Any] | None = None
            raw_bootstrap = ""
            if self._first_live_required():
                bootstrap, raw_bootstrap = self.first_live_bootstrap_store.issue(
                    binding=self._binding(),
                    approval_id=approval_id,
                    permit_id=str(permit["permitId"]),
                    permit_hash=str(permit["permitHash"]),
                )
            record = {
                "schemaVersion": "binance-spot-functional-permit-candidate/v1",
                "approvalId": approval_id,
                "permitId": permit["permitId"],
                "permitHash": permit["permitHash"],
                "accountFingerprint": permit["binding"]["accountFingerprint"],
                "executionRoute": "BINANCE_SPOT_CONTINUOUS",
                "symbol": "BTCUSDT",
                "serverManaged": True,
                "singleUse": True,
                "issuer": "LIVE_TRADER_SERVER",
                "issuedAt": _utc(now),
                "expiresAt": _utc(now + 300),
                "permitExpiresAt": permit["expiresAt"],
                "nonce": secrets.token_urlsafe(32),
                "firstLiveBootstrapRequired": bootstrap is not None,
                "firstLiveBootstrapId": (
                    str(bootstrap["bootstrap_id"]) if bootstrap else ""
                ),
                "firstLiveBootstrapHash": (
                    str(bootstrap["bootstrap_hash"]) if bootstrap else ""
                ),
                "firstLiveSessionNonceHash": (
                    str(bootstrap["session_nonce_hash"]) if bootstrap else ""
                ),
                "firstLiveCodeHash": (
                    str(bootstrap["code_hash"]) if bootstrap else ""
                ),
            }
            signed = dict(self.server_record_signer(record))
            try:
                stored = self.approval_store.issue_candidate(permit, signed)
            except Exception:
                if bootstrap is not None:
                    self.first_live_bootstrap_store.fail(
                        bootstrap_id=str(bootstrap["bootstrap_id"]),
                        detail="approval candidate persistence failed",
                    )
                raise
            result = {
                "approvalId": str(stored["approval_id"]),
                "permitId": str(stored["permit_id"]),
                "permitHash": str(stored["permit_hash"]),
                "expiresAt": record["expiresAt"],
                "permitExpiresAt": record["permitExpiresAt"],
                "serverManaged": True,
            }
            if bootstrap is not None:
                self._first_live_vault[str(stored["approval_id"])] = (
                    str(bootstrap["bootstrap_id"]),
                    raw_bootstrap,
                )
                result["firstLiveBootstrapPending"] = True
                result.update(
                    {
                        "firstLiveBootstrapId": str(
                            bootstrap["bootstrap_id"]
                        ),
                        "firstLiveBootstrapHash": str(
                            bootstrap["bootstrap_hash"]
                        ),
                        "firstLiveSessionNonceHash": str(
                            bootstrap["session_nonce_hash"]
                        ),
                        "firstLiveCodeHash": str(bootstrap["code_hash"]),
                        "accountFingerprint": str(
                            bootstrap["account_fingerprint"]
                        ),
                        "bindingHash": str(bootstrap["binding_hash"]),
                    }
                )
            return result

    def start(self, command: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_fields(command, {"approvalId", "operatorConfirmation"})
        self._verify_operator(command)
        with self._lock:
            self._assert_route_lock()
            if not self._candidate_gate_available():
                raise BinanceSpotFunctionalBackendError(
                    "Binance functional backend/full first-live gate is unavailable"
                )
            if self._scheduler_thread is not None and self._scheduler_thread.is_alive():
                raise BinanceSpotFunctionalBackendError(
                    "Binance functional backend scheduler already owns the route"
                )
            record = self.approval_store.candidate_status(
                str(command["approvalId"])
            )
            if str(record.get("state") or "").upper() != "APPROVED":
                raise BinanceSpotFunctionalBackendError(
                    "exact server-approved permit is absent"
                )
            bootstrap_id = ""
            bootstrap_claim_token = ""
            if self._first_live_required():
                vault = self._first_live_vault.pop(
                    str(command["approvalId"]), None
                )
                if vault is None:
                    raise BinanceSpotFunctionalBackendError(
                        "single-use first-live capability is absent or already claimed"
                    )
                bootstrap_id, raw_bootstrap = vault
                try:
                    bootstrap_claim_token = (
                        self.first_live_bootstrap_store.claim(
                            bootstrap_id=bootstrap_id,
                            raw_capability=raw_bootstrap,
                            approval_id=str(command["approvalId"]),
                            permit_id=str(record["permit_id"]),
                            permit_hash=str(record["permit_hash"]),
                        )
                    )
                except Exception:
                    try:
                        self.first_live_bootstrap_store.fail(
                            bootstrap_id=bootstrap_id,
                            detail="first-live claim failed; raw capability burned",
                        )
                    except Exception:
                        pass
                    raise
                finally:
                    # The raw bootstrap is deliberately destroyed even when
                    # the durable claim fails.  It is never retryable or
                    # reconstructable after this activation boundary.
                    raw_bootstrap = ""
            if self.stream_start is not None:
                try:
                    self.stream_start()
                except Exception:
                    if bootstrap_id:
                        self.first_live_bootstrap_store.fail(
                            bootstrap_id=bootstrap_id,
                            detail="prebaseline stream start failed after bootstrap claim",
                        )
                    self.approval_store.fail_approved_candidate(
                        approval_id=str(command["approvalId"]),
                        detail="prebaseline stream start failed after typed approval",
                    )
                    raise
            # Start the backend-owned worker before any durable lifecycle can
            # become ACTIVE.  The worker blocks on this vault lock until the
            # handle is installed.  If Thread.start itself fails, the exact
            # typed approval is consumed as FAILED and broker authority was
            # never armed, eliminating an ACTIVE-without-owner crash seam.
            self._generation += 1
            generation = self._generation
            try:
                self._start_scheduler_locked(generation)
            except Exception as exc:
                if bootstrap_id:
                    self.first_live_bootstrap_store.fail(
                        bootstrap_id=bootstrap_id,
                        detail="scheduler spawn failed before first-live activation",
                    )
                self.approval_store.fail_approved_candidate(
                    approval_id=str(command["approvalId"]),
                    detail=f"scheduler spawn failed before activation:{type(exc).__name__}",
                )
                if self.stream_stop is not None:
                    try:
                        self.stream_stop()
                    except Exception:
                        pass
                self._scheduler_thread = None
                self._terminal_state = "FAILED"
                self._terminal_detail = (
                    "scheduler spawn failed before lifecycle activation"
                )
                raise
            handle: LifecycleHandle | None = None
            try:
                if self.stream_ready is not None and self.stream_ready() is not True:
                    raise BinanceSpotFunctionalBackendError(
                        "authenticated prebaseline Binance stream is not ready"
                    )
                owner_id = "binance-functional-owner-" + secrets.token_hex(16)
                owner_token = secrets.token_urlsafe(48)
                handle = self.manager.start(
                    {
                        "permitId": str(record["permit_id"]),
                        "permitHash": str(record["permit_hash"]),
                    },
                    owner_id=owner_id,
                    owner_token=owner_token,
                )
                if bootstrap_id:
                    active_record = self.approval_store.candidate_status(
                        str(command["approvalId"])
                    )
                    active_permit = ExactPermit.parse(
                        self.approval_store.server_permit_for_approval(
                            str(command["approvalId"])
                        ),
                        now_epoch=float(self.clock()),
                    )
                    self.first_live_bootstrap_store.bind_session(
                        bootstrap_id=bootstrap_id,
                        claim_token=bootstrap_claim_token,
                        approval_id=str(command["approvalId"]),
                        active_permit_id=str(active_record["permit_id"]),
                        active_permit_hash=str(active_record["permit_hash"]),
                        session_id=handle.session_id,
                        binding=self._binding(),
                        activated_epoch=active_permit.issued_epoch,
                        active_ends_epoch=active_permit.expires_epoch,
                    )
            except Exception as exc:
                if bootstrap_id:
                    try:
                        self.first_live_bootstrap_store.fail(
                            bootstrap_id=bootstrap_id,
                            detail=f"first-live activation failed:{type(exc).__name__}",
                        )
                    except Exception:
                        pass
                if handle is not None:
                    # The lifecycle may already be ACTIVE when final bootstrap
                    # binding fails.  Preserve its raw handle and hand it to
                    # the already-started worker only after entry authority is
                    # durably revoked.
                    self._handle = handle
                    self._active_first_live_bootstrap_id = ""
                    try:
                        self.manager.begin_cleanup(
                            handle,
                            reason="first-live post-activation binding failed",
                        )
                    finally:
                        self._terminal_state = "CLEANUP"
                        self._terminal_detail = (
                            "first-live binding failed; cleanup-only worker retained"
                        )
                    raise
                self._generation += 1
                self._scheduler_stop.set()
                try:
                    current = self.approval_store.candidate_status(
                        str(command["approvalId"])
                    )
                    if str(current.get("state") or "").upper() == "APPROVED":
                        self.approval_store.fail_approved_candidate(
                            approval_id=str(command["approvalId"]),
                            detail=f"backend start failed:{type(exc).__name__}",
                        )
                finally:
                    if self.stream_stop is not None:
                        try:
                            self.stream_stop()
                        except Exception:
                            pass
                raise
            # The raw capability and owner token live only in this vault and
            # are never included in a status/result document.
            self._handle = handle
            self._active_first_live_bootstrap_id = bootstrap_id
            self._scheduler_escape_count = 0
            self._terminal_state = "ACTIVE"
            self._terminal_detail = "backend-owned scheduler started"
            return {
                "ok": True,
                "sessionId": handle.session_id,
                "status": self.status(),
            }

    def _start_scheduler_locked(self, generation: int) -> None:
        self._scheduler_stop.clear()
        thread = threading.Thread(
            target=self._run_scheduler,
            args=(generation,),
            name=f"binance-functional-scheduler-{generation}",
            daemon=True,
        )
        self._scheduler_thread = thread
        thread.start()

    def _record_cleanup_spawn_failure_locked(
        self,
        handle: LifecycleHandle,
        exc: BaseException,
        *,
        source: str,
    ) -> None:
        """Keep the only cleanup secret retryable after Thread.start fails."""

        self._handle = handle
        self._scheduler_thread = None
        self._scheduler_stop.set()
        self._terminal_state = "CLEANUP_RETRY_REQUIRED"
        self._terminal_detail = (
            f"{source} scheduler spawn failed; cleanup handle retained:"
            f"{type(exc).__name__}:{str(exc)[:240]}"
        )
        self._last_result = {
            "ok": False,
            "status": "CLEANUP_RETRY_REQUIRED",
            "detail": self._terminal_detail,
            "retryableInProcess": True,
            "entryAuthorityRestored": False,
        }

    def _try_start_cleanup_scheduler_locked(
        self,
        generation: int,
        handle: LifecycleHandle,
        *,
        source: str,
    ) -> bool:
        try:
            self._start_scheduler_locked(generation)
        except BaseException as exc:
            # Thread.start can raise OSError/RuntimeError after durable cleanup
            # ownership has rotated.  Never discard that raw handle and never
            # pretend a worker exists; the authenticated recover command can
            # retry in-process, while a process restart rotates it again.
            self._record_cleanup_spawn_failure_locked(
                handle, exc, source=source
            )
            return False
        return True

    def _verified_durable_phase(
        self,
        handle: LifecycleHandle,
        *,
        expected_phase: str,
        before_revision: int | None = None,
        transition: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Re-read one exact durable owner epoch before reporting success."""

        fresh = dict(self.manager.status())
        phase = str(fresh.get("phase") or "").upper()
        session_id = str(
            fresh.get("sessionId") or fresh.get("session_id") or ""
        )
        owner_id = str(fresh.get("ownerId") or fresh.get("owner_id") or "")
        try:
            revision = int(fresh.get("revision"))
        except (TypeError, ValueError):
            revision = -1
        if (
            phase != str(expected_phase).upper()
            or session_id != handle.session_id
            or owner_id != handle.owner_id
            or revision < 1
            or (
                before_revision is not None
                and revision <= int(before_revision)
            )
        ):
            raise BinanceSpotFunctionalBackendError(
                "durable functional owner phase was not freshly verified"
            )
        if transition is not None:
            transition_phase = str(transition.get("phase") or "").upper()
            transition_session = str(
                transition.get("sessionId")
                or transition.get("session_id")
                or ""
            )
            transition_owner = str(
                transition.get("ownerId")
                or transition.get("owner_id")
                or ""
            )
            try:
                transition_revision = int(transition.get("revision"))
            except (TypeError, ValueError):
                transition_revision = -1
            if (
                transition_phase != phase
                or transition_session != session_id
                or transition_owner != owner_id
                or transition_revision != revision
            ):
                raise BinanceSpotFunctionalBackendError(
                    "cleanup transition and fresh durable owner epoch differ"
                )
        control = getattr(self.manager, "control", None)
        verifier_name = (
            "verify_final_reset_handle"
            if str(expected_phase).upper() == "FINAL_RESET"
            else "verify_handle"
        )
        verifier = getattr(control, verifier_name, None)
        if callable(verifier):
            verified = dict(verifier(handle))
            if (
                str(verified.get("phase") or "").upper() != phase
                or str(verified.get("session_id") or "") != session_id
                or str(verified.get("owner_id") or "") != owner_id
                or int(verified.get("revision") or -1) != revision
            ):
                raise BinanceSpotFunctionalBackendError(
                    "cleanup owner token verification changed durable epoch"
                )
        elif not self.allow_mock_backend:
            raise BinanceSpotFunctionalBackendError(
                "production cleanup owner-token verifier is unavailable"
            )
        return fresh

    def _seal_first_live_terminal(
        self, *, bootstrap_id: str, session_id: str
    ) -> dict[str, Any]:
        if self.first_live_bootstrap_store is None:
            raise BinanceSpotFunctionalBackendError(
                "first-live terminal store is unavailable"
            )
        ledger = getattr(self.manager, "ledger", None)
        if ledger is None:
            raise BinanceSpotFunctionalBackendError(
                "first-live durable terminal ledger is unavailable"
            )
        session = ledger.session(session_id)
        durable = ledger.final_evidence(session_id)
        return self.first_live_bootstrap_store.consume_terminal(
            bootstrap_id=bootstrap_id,
            session_id=session_id,
            permit_id=str(session["permit_id"]),
            permit_hash=str(session["permit_hash"]),
            evidence=dict(durable["evidence"]),
            evidence_hash=str(durable["evidenceHash"]),
        )

    def _run_scheduler(self, generation: int) -> None:
        with self._lock:
            if generation != self._generation:
                return
            handle = self._handle
        if handle is None:
            return
        try:
            scheduler = self.scheduler_factory(manager=self.manager)
            result = dict(
                scheduler.run(handle, stop_event=self._scheduler_stop)
            )
        except Exception as exc:
            with self._lock:
                if generation == self._generation:
                    self._scheduler_escape_count += 1
                escape_count = self._scheduler_escape_count
            cleanup_latched = False
            cleanup_authority_fault = False
            cleanup_error_name = "unknown cleanup transition fault"
            try:
                before = dict(self.manager.status())
            except Exception:
                before = {}
            try:
                before_revision = int(before.get("revision"))
            except (TypeError, ValueError):
                before_revision = None
            try:
                transition = self.manager.begin_cleanup(
                    handle,
                    reason=(
                        "backend scheduler escaped unexpectedly:"
                        f"{type(exc).__name__}"
                    ),
                )
                self._verified_durable_phase(
                    handle,
                    expected_phase="CLEANUP",
                    before_revision=before_revision,
                    transition=transition,
                )
                cleanup_latched = True
            except Exception as cleanup_exc:
                cleanup_error_name = type(cleanup_exc).__name__
                try:
                    self._verified_durable_phase(
                        handle,
                        expected_phase="CLEANUP",
                        before_revision=before_revision,
                    )
                except Exception:
                    pass
                else:
                    cleanup_latched = True
            if not cleanup_latched:
                control = getattr(self.manager, "control", None)
                fail_closed = getattr(
                    control, "fail_closed_owner_health", None
                )
                if callable(fail_closed):
                    try:
                        fail_closed(
                            session_id=handle.session_id,
                            capability_hash=__import__(
                                "hashlib"
                            ).sha256(handle.capability.encode("utf-8")).hexdigest(),
                            detail=(
                                "scheduler escape cleanup latch failed:"
                                f"{type(exc).__name__}/"
                                f"{cleanup_error_name}"
                            ),
                        )
                    except Exception:
                        cleanup_authority_fault = True
                else:
                    cleanup_authority_fault = True
            if cleanup_latched and escape_count >= 3:
                control = getattr(self.manager, "control", None)
                fail_closed = getattr(
                    control, "fail_closed_owner_health", None
                )
                if callable(fail_closed):
                    try:
                        fail_closed(
                            session_id=handle.session_id,
                            capability_hash=__import__(
                                "hashlib"
                            ).sha256(handle.capability.encode("utf-8")).hexdigest(),
                            detail=(
                                "scheduler escaped three consecutive cleanup drivers; "
                                "manual reconciliation required"
                            ),
                        )
                    except Exception:
                        pass
                    else:
                        cleanup_latched = False
            result = {
                "ok": False,
                "status": (
                    "CLEANUP"
                    if cleanup_latched
                    else (
                        "CLEANUP_RETRY_REQUIRED"
                        if cleanup_authority_fault
                        else "RECONCILIATION_REQUIRED"
                    )
                ),
                "detail": f"{type(exc).__name__}:{str(exc)[:300]}",
            }
        with self._lock:
            bootstrap_id = (
                (
                    self._active_first_live_bootstrap_id
                    or self._recovered_first_live_terminal_id
                )
                if generation == self._generation
                else ""
            )
        raw_result_status = str(result.get("status") or "").upper()

        control_read_error = ""
        try:
            durable_control = dict(self.manager.status())
        except Exception as exc:
            durable_control = {}
            control_read_error = type(exc).__name__
        durable_phase = str(durable_control.get("phase") or "").upper()
        durable_session = str(
            durable_control.get("sessionId")
            or durable_control.get("session_id")
            or ""
        )
        durable_owner = str(
            durable_control.get("ownerId")
            or durable_control.get("owner_id")
            or ""
        )
        exact_owner_epoch = bool(
            durable_session == handle.session_id
            and durable_owner == handle.owner_id
        )
        terminal_capabilities_reset = bool(
            durable_control.get("functionalCapabilityReset") is True
            and durable_control.get("ownerTokenReset") is True
        )
        durable_terminal_clear = bool(
            durable_phase in {"FAILED", "FINALIZED"}
            and exact_owner_epoch
            and terminal_capabilities_reset
        )
        if not durable_terminal_clear:
            if durable_phase == "CLEANUP" and exact_owner_epoch:
                cleanup_driver_escaped = raw_result_status == "CLEANUP"
                result = {
                    **result,
                    "ok": False,
                    "status": (
                        "CLEANUP"
                        if cleanup_driver_escaped
                        else "CLEANUP_RETRY_REQUIRED"
                    ),
                    "detail": (
                        "scheduler stopped before durable cleanup terminal seal"
                    ),
                    "retryableInProcess": True,
                    "entryAuthorityRestored": False,
                }
            elif durable_phase == "FINAL_RESET" and exact_owner_epoch:
                result = {
                    **result,
                    "ok": False,
                    "status": "FINAL_RESET_RETRY_REQUIRED",
                    "detail": (
                        "durable FINAL_RESET remains; authenticated resume required"
                    ),
                    "retryableInProcess": True,
                    "entryAuthorityRestored": False,
                }
            else:
                # ACTIVE, an owner/session mismatch, or an unreadable control
                # row can never justify discarding the only raw handle.
                result = {
                    **result,
                    "ok": False,
                    "status": "CLEANUP_RETRY_REQUIRED",
                    "detail": (
                        "scheduler terminal result lacks exact durable revoked "
                        "control proof"
                        + (
                            f":{control_read_error}"
                            if control_read_error
                            else f":{durable_phase or 'UNKNOWN'}"
                        )
                    ),
                    "retryableInProcess": True,
                    "entryAuthorityRestored": False,
                }
        else:
            result = {
                **result,
                "ok": durable_phase == "FINALIZED",
                "status": durable_phase,
            }

        # Bootstrap disposition follows the normalized durable lifecycle, not
        # a scheduler object's raw return.  In particular, an escaped cleanup
        # driver must not burn a still-retryable first-live record.  Conversely
        # a consume/evidence failure after durable FINALIZED remains an
        # explicit reconciliation failure and can never be overwritten by the
        # already-sealed lifecycle phase.
        normalized_status = str(result.get("status") or "").upper()
        if bootstrap_id and normalized_status == "FINALIZED":
            try:
                bootstrap_terminal = self._seal_first_live_terminal(
                    bootstrap_id=bootstrap_id,
                    session_id=handle.session_id,
                )
            except Exception as exc:
                try:
                    self.first_live_bootstrap_store.fail(
                        bootstrap_id=bootstrap_id,
                        detail=(
                            "terminal first-live evidence verification failed:"
                            f"{type(exc).__name__}"
                        ),
                    )
                except Exception:
                    pass
                result = {
                    **result,
                    "ok": False,
                    "status": "RECONCILIATION_REQUIRED",
                    "detail": (
                        "first-live terminal evidence could not be consumed:"
                        f"{type(exc).__name__}"
                    ),
                    "durableLifecyclePhase": durable_phase,
                    "firstLiveBootstrapConsumed": False,
                }
            else:
                result = {
                    **result,
                    "firstLiveBootstrapConsumed": True,
                    "firstLiveFunctionalWiringPassed": bool(
                        bootstrap_terminal.get("functional_wiring_passed")
                    ),
                    "firstLiveE2EEvidenceEligible": bool(
                        bootstrap_terminal.get("e2e_evidence_eligible")
                    ),
                }
        elif bootstrap_id and normalized_status in {
            "FAILED",
            "RECONCILIATION_REQUIRED",
        }:
            try:
                self.first_live_bootstrap_store.fail(
                    bootstrap_id=bootstrap_id,
                    detail=(
                        "first-live session terminalized as "
                        f"{normalized_status}"
                    ),
                )
            except Exception:
                pass

        should_stop_stream = False
        notification: dict[str, Any] = {}
        with self._lock:
            if generation != self._generation:
                return
            session_id = self._handle.session_id if self._handle is not None else ""
            self._last_result = result
            status = str(result.get("status") or "").upper()
            self._terminal_state = status or "RECONCILIATION_REQUIRED"
            self._terminal_detail = str(result.get("detail") or "")[:500]
            if durable_terminal_clear:
                self._handle = None
                self._active_first_live_bootstrap_id = ""
                self._recovered_first_live_terminal_id = ""
            should_stop_stream = status in {
                "FINALIZED",
                "FAILED",
                "RECONCILIATION_REQUIRED",
            }
            notification = {
                "sessionId": session_id,
                "terminalState": self._terminal_state,
                "terminalDetail": self._terminal_detail,
                "route": BINANCE_SPOT_FUNCTIONAL_ROUTE_KEY,
            }
            if status == "CLEANUP":
                # Keep the only raw cleanup handle in the private vault and
                # schedule a fresh bounded cleanup driver.  Never abandon it
                # merely because one scheduler object escaped unexpectedly.
                self._generation += 1
                self._try_start_cleanup_scheduler_locked(
                    self._generation,
                    handle,
                    source="scheduler escape cleanup reschedule",
                )
                return
            if status == "CLEANUP_RETRY_REQUIRED":
                # Both durable CLEANUP and fail-closed revocation faulted.
                # Retain the only raw owner handle for authenticated retry;
                # never resume tick/dispatch from this scheduler generation.
                self._scheduler_stop.set()
                self._scheduler_thread = None
                return
            if status == "FINAL_RESET_RETRY_REQUIRED":
                self._scheduler_stop.set()
                self._scheduler_thread = None
                return
        # Never wait for execution-stream or shared state locks while holding
        # the backend vault lock.  This is the second phase of the state CAS.
        if should_stop_stream and self.stream_stop is not None:
            try:
                self.stream_stop()
            except Exception:
                with self._lock:
                    self._terminal_state = "RECONCILIATION_REQUIRED"
                    self._terminal_detail = "functional stream stop failed"
                    notification["terminalState"] = self._terminal_state
                    notification["terminalDetail"] = self._terminal_detail
        if should_stop_stream and self.terminal_callback is not None:
            self.terminal_callback(notification)

    def stop(self, command: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_fields(command, {"operatorConfirmation"})
        self._verify_operator(command)
        with self._lock:
            handle = self._handle
            if handle is None:
                return {"ok": True, "pending": False, "status": self.status()}
            self._assert_route_lock()
            self._scheduler_stop.set()
            before = dict(self.manager.status())
            try:
                before_revision = int(before.get("revision"))
            except (TypeError, ValueError):
                before_revision = None
            transition: Mapping[str, Any] | None = None
            transition_error = ""
            try:
                transition = self.manager.begin_cleanup(
                    handle, reason="operator stop"
                )
            except Exception as exc:
                transition_error = type(exc).__name__
            try:
                durable = self._verified_durable_phase(
                    handle,
                    expected_phase="CLEANUP",
                    before_revision=before_revision,
                    transition=transition,
                )
            except Exception as exc:
                self._terminal_state = "CLEANUP_RETRY_REQUIRED"
                self._terminal_detail = (
                    "operator stop has not durably latched CLEANUP:"
                    f"{transition_error or type(exc).__name__}"
                )
                self._last_result = {
                    "ok": False,
                    "status": "CLEANUP_RETRY_REQUIRED",
                    "detail": self._terminal_detail,
                    "retryableInProcess": True,
                    "entryAuthorityRestored": False,
                    "brokerSubmissionPerformed": False,
                }
                return {
                    "ok": False,
                    "pending": True,
                    "retryable": True,
                    "brokerSubmissionPerformed": False,
                    "status": self.status(),
                }
            self._terminal_state = "CLEANUP"
            self._terminal_detail = "operator stop latched; bounded cleanup continues"
            return {
                "ok": True,
                "pending": True,
                "durableCleanupRevision": int(durable["revision"]),
                "brokerSubmissionPerformed": False,
                "status": self.status(),
            }

    def recover(self, command: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_fields(command, {"operatorConfirmation"})
        self._verify_operator(command)
        with self._lock:
            self._assert_route_lock()
            if self._handle is not None:
                running = bool(
                    self._scheduler_thread is not None
                    and self._scheduler_thread.is_alive()
                )
                if running:
                    raise BinanceSpotFunctionalBackendError(
                        "current backend owner is still present"
                    )
                handle = self._handle
                current = dict(self.manager.status())
                phase = str(current.get("phase") or "").upper()
                if phase == "ACTIVE":
                    try:
                        before_revision = int(current.get("revision"))
                    except (TypeError, ValueError):
                        before_revision = None
                    try:
                        transition = self.manager.begin_cleanup(
                            handle,
                            reason=(
                                "authenticated in-process cleanup worker retry"
                            ),
                        )
                        self._verified_durable_phase(
                            handle,
                            expected_phase="CLEANUP",
                            before_revision=before_revision,
                            transition=transition,
                        )
                    except Exception as exc:
                        self._terminal_state = "CLEANUP_RETRY_REQUIRED"
                        self._terminal_detail = (
                            "in-process recovery could not latch CLEANUP:"
                            f"{type(exc).__name__}"
                        )
                        self._last_result = {
                            "ok": False,
                            "status": "CLEANUP_RETRY_REQUIRED",
                            "detail": self._terminal_detail,
                            "retryableInProcess": True,
                            "entryAuthorityRestored": False,
                        }
                        return {
                            "ok": False,
                            "sessionId": handle.session_id,
                            "cleanupOnly": True,
                            "pending": True,
                            "retryable": True,
                            "status": self.status(),
                        }
                    phase = "CLEANUP"
                elif phase == "CLEANUP":
                    try:
                        self._verified_durable_phase(
                            handle, expected_phase="CLEANUP"
                        )
                    except Exception as exc:
                        self._terminal_state = "CLEANUP_RETRY_REQUIRED"
                        self._terminal_detail = (
                            "existing cleanup owner epoch is unverified:"
                            f"{type(exc).__name__}"
                        )
                        return {
                            "ok": False,
                            "sessionId": handle.session_id,
                            "cleanupOnly": True,
                            "pending": True,
                            "retryable": True,
                            "status": self.status(),
                        }
                elif phase == "FINAL_RESET":
                    try:
                        self._verified_durable_phase(
                            handle, expected_phase="FINAL_RESET"
                        )
                    except Exception as exc:
                        self._terminal_state = "FINAL_RESET_RETRY_REQUIRED"
                        self._terminal_detail = (
                            "final-reset owner epoch is unverified:"
                            f"{type(exc).__name__}"
                        )
                        return {
                            "ok": False,
                            "sessionId": handle.session_id,
                            "cleanupOnly": True,
                            "pending": True,
                            "retryable": True,
                            "status": self.status(),
                        }
                else:
                    self._terminal_state = "RECONCILIATION_REQUIRED"
                    self._terminal_detail = (
                        "in-process handle durable phase is not recoverable:"
                        + phase
                    )
                    return {
                        "ok": False,
                        "sessionId": handle.session_id,
                        "cleanupOnly": True,
                        "pending": True,
                        "retryable": False,
                        "status": self.status(),
                    }
                self._generation += 1
                self._terminal_state = (
                    "FINAL_RESET_RETRY_REQUIRED"
                    if phase == "FINAL_RESET"
                    else "CLEANUP"
                )
                started = self._try_start_cleanup_scheduler_locked(
                    self._generation,
                    handle,
                    source="authenticated in-process recovery retry",
                )
                return {
                    "ok": started,
                    "sessionId": handle.session_id,
                    "cleanupOnly": True,
                    "pending": not started,
                    "retryable": not started,
                    "status": self.status(),
                }
            recovered = self.manager.audit_incomplete_startup()
            if not isinstance(recovered, LifecycleHandle):
                return {"ok": True, "recovery": recovered, "status": self.status()}
            self._handle = recovered
            self._generation += 1
            self._terminal_state = "CLEANUP"
            self._terminal_detail = "startup owner loss recovered cleanup-only"
            started = self._try_start_cleanup_scheduler_locked(
                self._generation,
                recovered,
                source="explicit startup cleanup recovery",
            )
            return {
                "ok": started,
                "sessionId": recovered.session_id,
                "cleanupOnly": True,
                "pending": not started,
                "retryable": not started,
                "status": self.status(),
            }

    def status(self) -> dict[str, Any]:
        with self._lock:
            lifecycle = dict(self.manager.status())
            full_available = binance_spot_functional_composite_available()
            first_live_available = binance_spot_first_live_bootstrap_available()
            hold_preparation = (
                binance_spot_functional_hold_preparation_status()
            )
            return {
                "available": full_available,
                "candidateIssuanceAvailable": (
                    full_available or first_live_available
                ),
                "networkOrderPostAllowed": bool(
                    full_available
                    or (
                        first_live_available
                        and self._active_first_live_bootstrap_id
                    )
                ),
                "firstLiveBootstrapAvailable": first_live_available,
                "firstLiveBootstrapActive": bool(
                    self._active_first_live_bootstrap_id
                ),
                "firstLiveBootstrapTerminalRecoveryPending": bool(
                    self._recovered_first_live_terminal_id
                ),
                "permanentRealE2EAvailable": (
                    BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE
                ),
                "route": BINANCE_SPOT_FUNCTIONAL_ROUTE_KEY,
                "generation": self._generation,
                "schedulerRunning": bool(
                    self._scheduler_thread is not None
                    and self._scheduler_thread.is_alive()
                ),
                "cleanupSchedulerRetryRequired": (
                    self._terminal_state == "CLEANUP_RETRY_REQUIRED"
                ),
                "terminalState": self._terminal_state,
                "terminalDetail": self._terminal_detail,
                "sessionId": str(lifecycle.get("sessionId") or ""),
                "lifecycle": lifecycle,
                "startupAudit": (
                    dict(self._startup_audit)
                    if isinstance(self._startup_audit, Mapping)
                    else {"result": type(self._startup_audit).__name__}
                ),
                "ordinaryLiveRouteChanged": False,
                "smokeRouteChanged": False,
                "futuresMarginWithdrawalChanged": False,
                "rawCapabilityExposed": False,
                "clientSignalAccepted": False,
                "clientPermitAccepted": False,
                "holdPreparation": hold_preparation,
            }


def _terminal_verifier_factory(
    holder: dict[str, Any],
) -> Callable[[Mapping[str, Any]], bool]:
    def verify(attestation: Mapping[str, Any]) -> bool:
        manager: BinanceSpotFunctionalLifecycleManager | None = holder.get(
            "manager"
        )
        store: DurableBinanceSpotApprovedPermitStore | None = holder.get("store")
        if manager is None or store is None:
            return False
        try:
            session = manager.ledger.session(str(attestation["sessionId"]))
            durable = manager.ledger.final_evidence(
                str(attestation["sessionId"])
            )
            control = manager.status()
            approval = store.status(str(attestation["permitId"]))
        except Exception:
            return False
        exact_identity = (
            str(session.get("permit_id") or "")
            == str(attestation.get("permitId") or "")
            and str(session.get("permit_hash") or "").lower()
            == str(attestation.get("permitHash") or "").lower()
            and str(durable.get("evidenceHash") or "").lower()
            == str(attestation.get("finalEvidenceHash") or "").lower()
            and str(approval.get("session_id") or "")
            in {"", str(attestation.get("sessionId") or "")}
        )
        reason = str(attestation.get("terminalReason") or "").upper()
        if reason in {"FINALIZED", "RECOVERED_FINALIZED"}:
            recovered = bool(
                durable.get("evidence", {}).get(
                    "privateStreamGapRecoveredCleanupOnly"
                )
            )
            return bool(
                exact_identity
                and recovered == (reason == "RECOVERED_FINALIZED")
                and (
                    not recovered
                    or str(
                        durable.get("evidence", {}).get("outcome") or ""
                    ).upper()
                    == "SAFE_INCOMPLETE_RECOVERED_STREAM_GAP"
                )
                and str(session.get("state") or "").upper()
                in {"FINAL_PREPARED", "FINALIZED"}
                and str(control.get("phase") or "").upper()
                in {"FINAL_RESET", "FINALIZED"}
                and str(approval.get("state") or "").upper()
                in {"ACTIVE", "CONSUMED"}
            )
        return bool(
            reason == "START_FAILED"
            and exact_identity
            and str(session.get("state") or "").upper() == "FAILED"
            and str(control.get("phase") or "").upper() == "FAILED"
            and str(approval.get("state") or "").upper() == "FAILED"
        )

    return verify


def build_binance_spot_functional_production_backend(
    *,
    database_path: str | Path,
    publication_proof_path: str | Path,
    data_root: str | Path,
    approval_verifier: Callable[[Mapping[str, Any]], bool],
    operator_confirmation_verifier: Callable[[Mapping[str, Any]], bool],
    server_record_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    route_lock_reader: Callable[[], Mapping[str, Any]],
    dispatch_lease_factory: Callable[..., Any],
    first_live_gate_reader: Callable[[], Mapping[str, Any]] | None = None,
    production_code_hash_reader: Callable[[], str] = (
        default_binance_spot_functional_code_hash
    ),
    terminal_callback: Callable[[Mapping[str, Any]], None] | None = None,
    startup_owner_absence_attestation: object | None = None,
    clock: Callable[[], float] = time.time,
) -> BinanceSpotFunctionalBackendManager:
    """Build the concrete journal/stream/lifecycle graph, still gated false."""

    assert_binance_spot_production_origin(
        env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    )
    application_lease = live_trader_instance_lease_status()
    if application_lease.get("acquired") is not True:
        raise BinanceSpotFunctionalBackendError(
            "official Live Trader application-instance lease is not held"
        )
    startup_owner_absence = _consume_startup_owner_absence_attestation(
        startup_owner_absence_attestation
    )
    account_fingerprint = binance_api_key_fingerprint(
        env_value("BINANCE_API_KEY")
    )
    if not account_fingerprint or not env_value("BINANCE_API_SECRET"):
        raise BinanceSpotFunctionalBackendError(
            "Binance Spot production credentials are missing"
        )
    proof_path = Path(publication_proof_path)
    binding_reader = lambda: load_binance_spot_publication_binding(
        proof_path=proof_path,
        account_fingerprint=account_fingerprint,
    )
    # Verify all publication bytes before constructing any broker edge.
    binding_reader()
    store = DurableBinanceSpotApprovedPermitStore(
        database_path, approval_verifier=approval_verifier, clock=clock
    )
    if (
        BINANCE_SPOT_FUNCTIONAL_FIRST_LIVE_BOOTSTRAP_AVAILABLE
        and first_live_gate_reader is None
    ):
        raise BinanceSpotFunctionalBackendError(
            "first-live bootstrap requires a state-owned exact gate reader"
        )

    def fail_closed_first_live_gates() -> Mapping[str, Any]:
        return {
            "allOtherProductionComponentsAvailable": False,
            "ordinaryBinanceRoutesClosed": False,
            "emergencyKillInactive": False,
            "applicationInstanceLeaseHeld": False,
            "exclusiveAccountConfirmed": False,
            "noManualTradingConfirmed": False,
            "noBotsConfirmed": False,
            "noOtherApiKeysConfirmed": False,
            "realE2EAvailable": BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE,
            "firstLiveBootstrapFeatureEnabled": False,
        }

    bootstrap_store = DurableBinanceSpotFirstLiveBootstrapStore(
        database_path,
        gate_reader=(first_live_gate_reader or fail_closed_first_live_gates),
        server_record_signer=server_record_signer,
        code_hash_reader=production_code_hash_reader,
        clock=clock,
    )
    if startup_owner_absence:
        bootstrap_store.fail_orphans_after_process_loss()
    holder: dict[str, Any] = {"store": store}
    journal = DurableBinanceSpotUserStreamJournal(
        database_path,
        account_fingerprint=account_fingerprint,
        clock=clock,
        terminal_verifier=_terminal_verifier_factory(holder),
    )
    bridge = BinanceSpotDurableStreamBridge(journal, clock=clock)
    manager = build_binance_spot_production_lifecycle(
        database_path=database_path,
        binding_reader=lambda: binding_reader().payload(),
        publication_proof_path=proof_path,
        account_fingerprint=account_fingerprint,
        stream_reader=bridge.snapshot,
        stream_owner_binder=bridge.bind_functional_session,
        # Bound below after the concrete stream owner is created.
        stream_terminal_barrier=lambda: holder["streamBarrier"](),
        stream_cleanup_recovery_latcher=(
            bridge.latch_terminal_failure_cleanup
        ),
        stream_startup_recovery_latcher=(
            bridge.latch_terminal_failure_cleanup
        ),
        stream_terminal_retirer=bridge.retire_terminal_session,
        dispatch_lease_factory=dispatch_lease_factory,
        startup_owner_process_absence_attested=startup_owner_absence,
        permit_approval_verifier=approval_verifier,
        activation_permit_issuer=lambda binding, activated_epoch: (
            issue_binance_spot_functional_permit(
                binding=(
                    binding
                    if isinstance(binding, ExactBinding)
                    else ExactBinding.parse(binding)
                ),
                now_epoch=activated_epoch,
            )
        ),
        clock=clock,
    )
    # The builder currently creates its own store against the same durable
    # database.  Reuse this server-owned instance for candidate issuance and
    # exact terminal verification; both implementations CAS the same rows.
    store = manager.permit_store or store
    holder["store"] = store
    holder["manager"] = manager
    streams = ExecutionStreamManager(
        Path(data_root), binance_functional_stream_bridge=bridge
    )
    holder["streamBarrier"] = (
        streams.begin_binance_functional_terminal_barrier
    )

    def stream_start() -> None:
        streams.start(("binance",))
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                proof = bridge.snapshot()
            except Exception:
                proof = {}
            if (
                proof.get("connected") is True
                and proof.get("authenticated") is True
                and proof.get("gapDetected") is False
                and proof.get("sessionId") in {None, ""}
            ):
                return
            time.sleep(0.05)
        streams.stop_brokers(("binance",))
        raise BinanceSpotFunctionalBackendError(
            "Binance authenticated prebaseline stream ACK timed out"
        )

    def stream_ready() -> bool:
        try:
            proof = bridge.snapshot()
        except Exception:
            return False
        return bool(
            proof.get("connected") is True
            and proof.get("authenticated") is True
            and proof.get("gapDetected") is False
        )

    return BinanceSpotFunctionalBackendManager(
        manager=manager,
        approval_store=store,
        binding_reader=binding_reader,
        operator_confirmation_verifier=operator_confirmation_verifier,
        server_record_signer=server_record_signer,
        route_lock_reader=route_lock_reader,
        clock=clock,
        stream_start=stream_start,
        stream_ready=stream_ready,
        stream_stop=lambda: streams.stop_brokers(("binance",)),
        terminal_callback=terminal_callback,
        first_live_bootstrap_store=bootstrap_store,
        first_live_gate_reader=first_live_gate_reader,
        allow_mock_backend=False,
    )


def prepare_binance_spot_functional_backend(**kwargs: Any) -> dict[str, Any]:
    global _SINGLETON
    with _SINGLETON_LOCK:
        if _SINGLETON is None:
            startup_attestation = _mint_startup_owner_absence_attestation()
            _SINGLETON = build_binance_spot_functional_production_backend(
                startup_owner_absence_attestation=startup_attestation,
                **kwargs,
            )
        return {"ok": True, "prepared": True, "status": _SINGLETON.status()}


def _required() -> BinanceSpotFunctionalBackendManager:
    with _SINGLETON_LOCK:
        value = _SINGLETON
    if value is None:
        raise BinanceSpotFunctionalBackendError(
            "Binance Spot functional backend is not prepared"
        )
    return value


def binance_spot_functional_backend_status() -> dict[str, Any]:
    with _SINGLETON_LOCK:
        value = _SINGLETON
    if value is None:
        status = production_entrypoint_status()
        return {
            "prepared": False,
            "available": False,
            "networkOrderPostAllowed": False,
            "terminalState": "UNAVAILABLE",
            "terminalDetail": status["reason"],
            "components": status["components"],
            "rawCapabilityExposed": False,
            "clientSignalAccepted": False,
            "clientPermitAccepted": False,
            "holdPreparation": (
                binance_spot_functional_hold_preparation_status()
            ),
        }
    return {"prepared": True, **value.status()}


def binance_spot_functional_hold_preparation_status() -> dict[str, Any]:
    """Expose verified Binance-only prerequisites while keeping HOLD."""

    from .binance_spot_functional_preparation import (
        binance_spot_functional_hold_preparation_status as _status,
    )

    return _status(
        root_integration_released=(
            BINANCE_SPOT_FUNCTIONAL_ROOT_INTEGRATION_RELEASED
        )
    )


def preissue_binance_spot_functional_candidate(
    requested_approval_id: str = "",
) -> dict[str, Any]:
    return _required().preissue_candidate(requested_approval_id)


def start_binance_spot_functional_backend(
    command: Mapping[str, Any]
) -> dict[str, Any]:
    return _required().start(command)


def stop_binance_spot_functional_backend(
    command: Mapping[str, Any]
) -> dict[str, Any]:
    return _required().stop(command)


def recover_binance_spot_functional_backend(
    command: Mapping[str, Any]
) -> dict[str, Any]:
    return _required().recover(command)


__all__ = [
    "BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE",
    "BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE",
    "BINANCE_SPOT_FUNCTIONAL_ROOT_INTEGRATION_RELEASED",
    "BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE",
    "BinanceSpotFunctionalBackendError",
    "BinanceSpotFunctionalBackendManager",
    "binance_spot_functional_backend_status",
    "binance_spot_functional_composite_available",
    "binance_spot_functional_hold_preparation_status",
    "build_binance_spot_functional_production_backend",
    "issue_binance_spot_functional_permit",
    "prepare_binance_spot_functional_backend",
    "preissue_binance_spot_functional_candidate",
    "recover_binance_spot_functional_backend",
    "start_binance_spot_functional_backend",
    "stop_binance_spot_functional_backend",
]
