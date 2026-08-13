from __future__ import annotations

"""Server-owned state facade for the Binance Spot functional lane.

This module deliberately has no HTTP surface.  It turns an already consumed
Live Trader safety confirmation into a server-authenticated approval and keeps
all permit documents, account bindings, owner tokens, and capabilities behind
the backend singleton.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import threading
from typing import Any, Callable, Iterator, Mapping

from .binance_spot_functional_approval import (
    DurableBinanceSpotApprovedPermitStore,
)
from .binance_spot_functional_backend import (
    BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE,
    BINANCE_SPOT_FUNCTIONAL_EMERGENCY_FENCE_AVAILABLE,
    BINANCE_SPOT_FUNCTIONAL_EXCLUSIVE_ACCOUNT_AVAILABLE,
    BINANCE_SPOT_FUNCTIONAL_FIRST_LIVE_BOOTSTRAP_AVAILABLE,
    BINANCE_SPOT_FUNCTIONAL_ORDINARY_FENCE_AVAILABLE,
    BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE,
    BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE,
    binance_spot_functional_backend_status,
    prepare_binance_spot_functional_backend,
    preissue_binance_spot_functional_candidate,
    recover_binance_spot_functional_backend,
    start_binance_spot_functional_backend,
    stop_binance_spot_functional_backend,
)
from .binance_spot_functional_lifecycle import (
    DurableBinanceSpotFunctionalControl,
    composite_production_available,
)
from .binance_order_authority import (
    binance_route_authority_serialization,
    functional_binance_final_mutation_boundary,
)
from .binance_spot_functional_transport import (
    assert_binance_spot_production_origin,
    binance_api_key_fingerprint,
)
from .live_adapters import BINANCE_BASE_URL, env_value
from .emergency_stop import emergency_stop_status
from .process_safety import live_trader_instance_lease_status


class BinanceSpotFunctionalStateError(RuntimeError):
    pass


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_or_create_server_secret(path: str | Path) -> bytes:
    """Load a durable local signing key without ever returning it to HTTP."""

    secret_path = Path(path)
    try:
        existing = secret_path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        existing = ""
    if existing:
        try:
            value = bytes.fromhex(existing)
        except ValueError as exc:
            raise BinanceSpotFunctionalStateError(
                "Binance functional server secret is malformed"
            ) from exc
        if len(value) != 48:
            raise BinanceSpotFunctionalStateError(
                "Binance functional server secret length changed"
            )
        return value
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    value = secrets.token_bytes(48)
    temporary = secret_path.with_name(
        f".{secret_path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            handle.write(value.hex())
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.replace(temporary, secret_path)
        except FileExistsError:
            temporary.unlink(missing_ok=True)
            return load_or_create_server_secret(secret_path)
    finally:
        temporary.unlink(missing_ok=True)
    return value


class BinanceSpotFunctionalStateFacade:
    """Thin state/server composition; no client supplies trading inputs."""

    def __init__(
        self,
        *,
        database_path: str | Path,
        publication_proof_path: str | Path,
        data_root: str | Path,
        server_secret_path: str | Path,
        ordinary_routes_closed_reader: Callable[[], bool],
    ) -> None:
        self.database_path = Path(database_path)
        self.publication_proof_path = Path(publication_proof_path)
        self.data_root = Path(data_root)
        self.server_secret_path = Path(server_secret_path)
        self.ordinary_routes_closed_reader = ordinary_routes_closed_reader
        self._lock = threading.RLock()
        self._dispatch_lock = threading.RLock()
        self._authority_reader = self.order_authority_snapshot
        self._secret: bytes | None = None
        self._store: DurableBinanceSpotApprovedPermitStore | None = None
        self._prepare_status: dict[str, Any] = {
            "ok": False,
            "prepared": False,
            "available": False,
            "networkOrderPostAllowed": False,
            "reason": "binance-functional-backend-not-prepared",
        }

    def _signature(self, domain: str, value: Mapping[str, Any]) -> str:
        if self._secret is None:
            raise BinanceSpotFunctionalStateError(
                "Binance functional server signing key is unavailable"
            )
        return hmac.new(
            self._secret,
            domain.encode("ascii") + b"\0" + _canonical(value),
            hashlib.sha256,
        ).hexdigest()

    def _sign_server_record(self, value: Mapping[str, Any]) -> dict[str, Any]:
        body = dict(value)
        body.pop("serverSignature", None)
        return {
            **body,
            "serverSignature": self._signature("candidate-v1", body),
        }

    def _verify_approval_record(self, value: Mapping[str, Any]) -> bool:
        body = dict(value)
        signature = str(body.pop("serverSignature", "") or "")
        if signature:
            return hmac.compare_digest(
                signature, self._signature("candidate-v1", body)
            )
        nonce = str(body.pop("nonce", "") or "")
        return bool(
            value.get("operatorAuthenticated") is True
            and value.get("operatorApproved") is True
            and nonce
            and hmac.compare_digest(
                nonce, self._signature("operator-approval-v1", body)
            )
        )

    def _operator_attestation(self, action: str) -> dict[str, Any]:
        body = {
            "authenticated": True,
            "confirmed": True,
            "source": "SERVER_SAFETY_CONFIRMATION",
            "action": str(action or "").strip().upper(),
        }
        return {
            **body,
            "serverSignature": self._signature("operator-command-v1", body),
        }

    def _verify_operator_attestation(self, value: Mapping[str, Any]) -> bool:
        body = dict(value)
        signature = str(body.pop("serverSignature", "") or "")
        return bool(
            signature
            and body.get("authenticated") is True
            and body.get("confirmed") is True
            and body.get("source") == "SERVER_SAFETY_CONFIRMATION"
            and hmac.compare_digest(
                signature, self._signature("operator-command-v1", body)
            )
        )

    def _route_snapshot(self) -> dict[str, bool]:
        closed = self.ordinary_routes_closed_reader() is True
        return {
            "globalRealOrdersEnabled": False,
            "ordinaryRuntimeActive": not closed,
            "binanceSpotOrdinaryRouteClosed": closed,
            "binanceSmokeRouteClosed": closed,
            "binanceFuturesRouteClosed": closed,
            "marginRouteClosed": closed,
            "withdrawalRouteClosed": closed,
        }

    def order_authority_snapshot(self) -> dict[str, Any]:
        """Read approval+control directly; unreadable always looks open."""

        try:
            store = self._store or DurableBinanceSpotApprovedPermitStore(
                self.database_path, approval_verifier=lambda _value: False
            )
            pointer = store.order_authority_pointer()
            control = DurableBinanceSpotFunctionalControl(
                self.database_path
            ).status()
            phase = str(control.get("phase") or "IDLE").upper()
            control_open = phase not in {"IDLE", "FAILED", "FINALIZED"}
            pointer_row = dict(pointer) if isinstance(pointer, Mapping) else {}
            application = live_trader_instance_lease_status()
            return {
                "functionalAuthorityOpen": bool(pointer or control_open),
                "functionalPhase": phase,
                "functionalRevision": int(control.get("revision") or 0),
                "functionalSessionId": str(
                    control.get("sessionId")
                    or pointer_row.get("session_id")
                    or ""
                ),
                "functionalAccountFingerprint": str(
                    pointer_row.get("account_fingerprint") or ""
                ).lower(),
                "applicationInstanceLeaseHeld": (
                    application.get("acquired") is True
                ),
                "ordinaryRoutesClosed": (
                    self.ordinary_routes_closed_reader() is True
                ),
            }
        except Exception:
            return {
                "functionalAuthorityOpen": True,
                "functionalPhase": "UNREADABLE",
                "functionalRevision": -1,
                "functionalSessionId": "",
                "functionalAccountFingerprint": "",
                "applicationInstanceLeaseHeld": False,
                "ordinaryRoutesClosed": False,
            }

    def _first_live_gate_snapshot(self) -> dict[str, Any]:
        """Derive operator exclusivity only from a validated durable approval."""

        try:
            store = self._store or DurableBinanceSpotApprovedPermitStore(
                self.database_path, approval_verifier=lambda _value: False
            )
            pointer = store.order_authority_pointer()
        except Exception:
            pointer = None
        pointer_row = dict(pointer) if isinstance(pointer, Mapping) else {}
        approved_contract = bool(
            str(pointer_row.get("state") or "").upper()
            in {"APPROVED", "CLAIMED", "ACTIVE"}
            and bool(pointer_row.get("first_live_bootstrap_required"))
            and str(pointer_row.get("first_live_bootstrap_id") or "")
            and str(pointer_row.get("first_live_bootstrap_hash") or "")
            and str(pointer_row.get("first_live_session_nonce_hash") or "")
        )
        emergency = emergency_stop_status()
        application = live_trader_instance_lease_status()
        return {
            "allOtherProductionComponentsAvailable": bool(
                composite_production_available()
                and BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE
                and BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE
                and BINANCE_SPOT_FUNCTIONAL_ORDINARY_FENCE_AVAILABLE
                and BINANCE_SPOT_FUNCTIONAL_EMERGENCY_FENCE_AVAILABLE
                and BINANCE_SPOT_FUNCTIONAL_EXCLUSIVE_ACCOUNT_AVAILABLE
            ),
            "ordinaryBinanceRoutesClosed": (
                self.ordinary_routes_closed_reader() is True
            ),
            "emergencyKillInactive": emergency.get("active") is False,
            "applicationInstanceLeaseHeld": (
                application.get("acquired") is True
            ),
            "exclusiveAccountConfirmed": approved_contract,
            "noManualTradingConfirmed": approved_contract,
            "noBotsConfirmed": approved_contract,
            "noOtherApiKeysConfirmed": approved_contract,
            "realE2EAvailable": BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE,
            "firstLiveBootstrapFeatureEnabled": (
                BINANCE_SPOT_FUNCTIONAL_FIRST_LIVE_BOOTSTRAP_AVAILABLE
            ),
        }

    @contextmanager
    def dispatch_lease(
        self,
        *,
        session_id: str,
        claim_id: str,
        cleanup_only: bool = False,
        authority_revision: object | None = None,
    ) -> Iterator[Callable[[], Mapping[str, Any]]]:
        """Fence the functional final edge against state-owned route changes.

        Existing ordinary Binance final edges do not yet take this lock.  The
        production composite gate therefore remains false until that
        separately authorized integration and its paused-POST tests exist.
        """

        with functional_binance_final_mutation_boundary(
            session_id=session_id,
            cleanup_only=cleanup_only,
            expected_revision=authority_revision,
        ) as shared_reader:
            def read() -> Mapping[str, Any]:
                value = dict(shared_reader())
                try:
                    current_fingerprint = binance_api_key_fingerprint(
                        env_value("BINANCE_API_KEY")
                    )
                except Exception:
                    current_fingerprint = ""
                matches = secrets.compare_digest(
                    str(value.get("functionalAccountFingerprint") or ""),
                    current_fingerprint,
                )
                return {
                    **value,
                    "active": value.get("active") is True and matches,
                    "sessionId": str(session_id or ""),
                    "claimId": str(claim_id or ""),
                    "ordinaryRoutesClosed": value.get(
                        "ordinaryRoutesClosed"
                    ) is True,
                    "applicationLeaseHeld": value.get(
                        "applicationInstanceLeaseHeld"
                    ) is True,
                }

            yield read

    def prepare(self) -> dict[str, Any]:
        """Prepare the singleton without opening a socket or sending an order."""

        with self._lock:
            configured = {
                "applicationInstanceLease": (
                    live_trader_instance_lease_status().get("acquired") is True
                ),
                "apiKey": bool(str(env_value("BINANCE_API_KEY") or "").strip()),
                "apiSecret": bool(
                    str(env_value("BINANCE_API_SECRET") or "").strip()
                ),
                "publicationProof": self.publication_proof_path.is_file(),
            }
            missing = [key for key, present in configured.items() if not present]
            if missing:
                self._prepare_status = {
                    "ok": False,
                    "prepared": False,
                    "available": False,
                    "networkOrderPostAllowed": False,
                    "reason": "binance-functional-configuration-missing:"
                    + ",".join(missing),
                    "configured": configured,
                }
                return dict(self._prepare_status)
            try:
                assert_binance_spot_production_origin(
                    env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
                )
                self._secret = load_or_create_server_secret(
                    self.server_secret_path
                )
                self._store = DurableBinanceSpotApprovedPermitStore(
                    self.database_path,
                    approval_verifier=self._verify_approval_record,
                )
                prepared = prepare_binance_spot_functional_backend(
                    database_path=self.database_path,
                    publication_proof_path=self.publication_proof_path,
                    data_root=self.data_root,
                    approval_verifier=self._verify_approval_record,
                    operator_confirmation_verifier=(
                        self._verify_operator_attestation
                    ),
                    server_record_signer=self._sign_server_record,
                    route_lock_reader=self._route_snapshot,
                    dispatch_lease_factory=self.dispatch_lease,
                    first_live_gate_reader=self._first_live_gate_snapshot,
                    terminal_callback=lambda _value: None,
                )
                status = dict(prepared.get("status") or {})
                self._prepare_status = {
                    "ok": True,
                    "prepared": True,
                    **status,
                    "configured": configured,
                    "officialServerProcessOnly": True,
                    "standaloneProductionLauncherAllowed": False,
                }
            except Exception as exc:
                self._prepare_status = {
                    "ok": False,
                    "prepared": False,
                    "available": False,
                    "networkOrderPostAllowed": False,
                    "reason": "binance-functional-backend-prepare-failed:"
                    + type(exc).__name__,
                    "configured": configured,
                }
            return dict(self._prepare_status)

    def status(self) -> dict[str, Any]:
        with self._lock:
            prepared = dict(self._prepare_status)
            store = self._store
        if prepared.get("prepared") is not True:
            return prepared
        try:
            result = {
                **prepared,
                **binance_spot_functional_backend_status(),
                "prepared": True,
            }
            if store is not None:
                issued = store.issued_pointer()
                authority = store.authority_pointer()
                if issued is not None:
                    result["pendingApprovalId"] = str(
                        issued.get("approval_id") or ""
                    )
                    result["pendingApprovalState"] = "ISSUED"
                if authority is not None:
                    result["approvalId"] = str(
                        authority.get("approval_id") or ""
                    )
                    result["approvalState"] = str(
                        authority.get("state") or ""
                    )
            return result
        except Exception as exc:
            return {
                "ok": False,
                "prepared": False,
                "available": False,
                "networkOrderPostAllowed": False,
                "reason": "binance-functional-backend-status-failed:"
                + type(exc).__name__,
            }

    def authority_open(self) -> bool:
        with self._lock:
            store = self._store
        if store is None:
            return False
        try:
            return store.authority_pointer() is not None
        except Exception:
            return True

    def preissue(self, requested_approval_id: str = "") -> dict[str, Any]:
        status = self.status()
        if (
            status.get("prepared") is not True
            or status.get("candidateIssuanceAvailable") is not True
        ):
            raise BinanceSpotFunctionalStateError(
                "Binance functional backend composite gate is unavailable"
            )
        if self.ordinary_routes_closed_reader() is not True:
            raise BinanceSpotFunctionalStateError(
                "ordinary Binance routes are not closed"
            )
        return preissue_binance_spot_functional_candidate(
            str(requested_approval_id or "")
        )

    def _approve(self, approval_id: str) -> dict[str, Any]:
        with self._lock:
            store = self._store
        if store is None:
            raise BinanceSpotFunctionalStateError(
                "Binance functional approval store is unavailable"
            )
        candidate = store.candidate_status(approval_id)
        body = {
            "approvalId": str(candidate["approval_id"]),
            "operatorId": "LIVE_TRADER_AUTHENTICATED_OPERATOR",
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "permitId": str(candidate["permit_id"]),
            "permitHash": str(candidate["permit_hash"]),
            "accountFingerprint": str(candidate["account_fingerprint"]),
            "executionRoute": "BINANCE_SPOT_CONTINUOUS",
            "symbol": "BTCUSDT",
            "approvedAt": _utc_now(),
            "activationResealAuthorized": True,
            "activeDurationSeconds": 7200,
            "exclusiveAccountConfirmed": True,
            "noManualTradingConfirmed": True,
            "noBotsConfirmed": True,
            "noOtherApiKeysConfirmed": True,
            "firstLiveBootstrapAuthorized": True,
            "firstLiveBootstrapRequired": bool(
                candidate.get("first_live_bootstrap_required")
            ),
            "firstLiveBootstrapId": str(
                candidate.get("first_live_bootstrap_id") or ""
            ),
            "firstLiveBootstrapHash": str(
                candidate.get("first_live_bootstrap_hash") or ""
            ),
            "firstLiveSessionNonceHash": str(
                candidate.get("first_live_session_nonce_hash") or ""
            ),
            "firstLiveCodeHash": str(
                candidate.get("first_live_code_hash") or ""
            ),
        }
        body["nonce"] = self._signature("operator-approval-v1", body)
        return store.approve_issued_candidate(
            approval_id=approval_id,
            approval_attestation=body,
        )

    def start(self, approval_id: str) -> dict[str, Any]:
        with binance_route_authority_serialization():
            if self.ordinary_routes_closed_reader() is not True:
                raise BinanceSpotFunctionalStateError(
                    "ordinary Binance routes are not closed"
                )
            self._approve(str(approval_id or ""))
        return start_binance_spot_functional_backend(
            {
                "approvalId": str(approval_id or ""),
                "operatorConfirmation": self._operator_attestation(
                    "BINANCE_SPOT_FUNCTIONAL_START"
                ),
            }
        )

    def stop(self) -> dict[str, Any]:
        with binance_route_authority_serialization():
            return stop_binance_spot_functional_backend(
                {
                    "operatorConfirmation": self._operator_attestation(
                        "BINANCE_SPOT_FUNCTIONAL_STOP"
                    )
                }
            )

    def recover(self) -> dict[str, Any]:
        if self.ordinary_routes_closed_reader() is not True:
            raise BinanceSpotFunctionalStateError(
                "ordinary Binance routes are not closed"
            )
        return recover_binance_spot_functional_backend(
            {
                "operatorConfirmation": self._operator_attestation(
                    "BINANCE_SPOT_FUNCTIONAL_RECOVER"
                )
            }
        )


__all__ = [
    "BinanceSpotFunctionalStateError",
    "BinanceSpotFunctionalStateFacade",
    "load_or_create_server_secret",
]
