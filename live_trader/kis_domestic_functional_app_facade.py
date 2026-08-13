"""Offline-only durable application facade for the domestic KIS route.

This module is intentionally isolated from ``state.py``.  It composes the
already frozen durable state, owner and verify-only production factory into a
single long-lived reader without registering that reader or exposing a socket.

The facade adds three pieces of application-bound evidence to the *same*
``ProgramLedger`` used by ``DurableKisDomesticFunctionalState``:

* a signed facade-epoch transition chain;
* a signed, fresh authority-snapshot chain; and
* an exactly-once cleanup/Kill grant-burn chain.

An independently stored signed high-water record detects restoration of an
older ledger copy.  It is deliberately not represented as a production-grade
external anti-rollback anchor; consequently every production/network/release
flag remains false.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any, Callable, Mapping
import uuid

from .kis_domestic_functional_contract import PDNO, ROUTE
from .kis_domestic_functional_graph import FrozenKisDomesticGraphLedgerPort
from .kis_domestic_functional_facade_anchor import (
    PROJECTION_SCHEMA as EXTERNAL_ANCHOR_PROJECTION_SCHEMA,
    AppendOnlyKisDomesticFunctionalFacadeAnchor,
)
from .kis_domestic_functional_manager import (
    DisabledKisDomesticFunctionalManager,
)
from .kis_domestic_functional_manager_authority import (
    MANAGER_KEY_PURPOSE,
    VerifyOnlyKisDomesticFunctionalManagerAuthority,
)
from .kis_domestic_functional_owner import (
    DurableKisDomesticFunctionalOwner,
)
from .kis_domestic_functional_production_factory import (
    DisabledKisDomesticFunctionalProductionFactory,
    RegistryBoundStateManagerConstructors,
)
from .kis_domestic_functional_state import (
    DurableKisDomesticFunctionalState,
)
from .kis_order_authority import kis_route_authority_serialization
from .program_ledger import ProgramLedger


SCHEMA_VERSION = 1
SCHEMA_FINGERPRINT = hashlib.sha256(
    b"kis-domestic-functional-app-facade-schema/v1\0"
    b"epoch-transition\0signed-snapshot\0cleanup-grant-burn"
).hexdigest()

SNAPSHOT_SCHEMA = "kis-domestic-functional-app-snapshot/v1"
EPOCH_SCHEMA = "kis-domestic-functional-app-epoch-transition/v1"
BURN_SCHEMA = "kis-domestic-functional-app-cleanup-burn/v1"
HIGH_WATER_SCHEMA = "kis-domestic-functional-app-high-water/v1"
STATUS_SCHEMA = "kis-domestic-functional-app-facade-status/v1"

_ZERO_HASH = "0" * 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,191}$")
_OFFICIAL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_ENDPOINT = re.compile(r"^/[A-Za-z0-9._~!$&'()*+,;=:@%/-]{1,511}$")
_OPERATIONS = frozenset(
    {"CLEANUP_CANCEL", "CLEANUP_SELL", "KILL_ORDINARY_CANCEL"}
)
_OPEN_PHASES = frozenset(
    {
        "ARMED_WAIT_PUBLIC",
        "BOOTSTRAP_ISSUED",
        "APPROVED",
        "ACTIVE",
        "CLEANUP",
        "RECONCILIATION_REQUIRED",
    }
)

ExternalAnchorWriter = Callable[
    [Mapping[str, Any]], Mapping[str, Any]
]


def derive_facade_anchor_ledger_id(ledger_path: str | Path) -> str:
    """Derive the immutable identity pinned by the external facade anchor."""

    path = Path(ledger_path).expanduser().resolve()
    return _hash(
        {
            "schemaVersion": "kis-domestic-functional-facade-ledger-id/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "absoluteLedgerPath": str(path),
            "schemaFingerprint": SCHEMA_FINGERPRINT,
        }
    )

_AUTHORITY_KEYS = frozenset(
    {
        "durableAuthorityReadable",
        "functionalAuthorityOpen",
        "functionalPhase",
        "functionalRevision",
        "stateRevision",
        "functionalSessionId",
        "functionalAccountFingerprint",
        "credentialConfigurationHash",
        "functionalMutationIntent",
        "killOrdinaryCancelAllowed",
        "killOrdinaryCancelRevision",
        "killOrdinaryCancelIntent",
        "applicationInstanceLeaseHeld",
        "ordinaryRoutesClosed",
        "controlReservation",
        "ownerEpochId",
        "ownerEpochHash",
    }
)

_SNAPSHOT_META_KEYS = frozenset(
    {
        "schemaVersion",
        "route",
        "pdno",
        "facadeEpoch",
        "snapshotSequence",
        "previousSnapshotHash",
        "observedAt",
        "observedMonotonicNs",
        "expiresAt",
        "ownerEpoch",
        "ownerStatusHash",
        "ownerIdHash",
        "processIdentityHash",
        "factoryStatusHash",
        "factoryBindingHash",
        "factoryOwnerReaderResultHash",
        "constructorsStatusHash",
        "stateAuthorityHash",
        "controlReservationHash",
        "killGrant",
        "snapshotKeyIdHash",
        "readerRegistered",
        "productionAvailable",
        "networkAvailable",
        "releaseAvailable",
        "networkOrderPostAllowed",
    }
)
_SNAPSHOT_BODY_KEYS = _AUTHORITY_KEYS | _SNAPSHOT_META_KEYS
_SNAPSHOT_KEYS = _SNAPSHOT_BODY_KEYS | {
    "snapshotBodyHash",
    "snapshotSignature",
}

_EPOCH_BODY_KEYS = frozenset(
    {
        "schemaVersion",
        "route",
        "pdno",
        "sequenceNo",
        "facadeEpoch",
        "ownerEpoch",
        "ownerIdHash",
        "processIdentityHash",
        "factoryBindingHash",
        "state",
        "previousEntryHash",
        "occurredAt",
        "occurredMonotonicNs",
        "keyIdHash",
        "productionAvailable",
    }
)

_BURN_BODY_KEYS = frozenset(
    {
        "schemaVersion",
        "route",
        "pdno",
        "sequenceNo",
        "facadeEpoch",
        "ownerEpoch",
        "ownerEpochId",
        "ownerEpochHash",
        "stateRevision",
        "controlReservationHash",
        "reservationId",
        "reservationKind",
        "reservationBindingHash",
        "operation",
        "intent",
        "intentHash",
        "grantHash",
        "grantBurnKey",
        "killRevision",
        "snapshotSequence",
        "snapshotBodyHash",
        "previousEntryHash",
        "burnedAt",
        "burnedMonotonicNs",
        "keyIdHash",
        "productionAvailable",
        "networkOrderPostAllowed",
    }
)

_HIGH_WATER_BODY_KEYS = frozenset(
    {
        "schemaVersion",
        "route",
        "pdno",
        "revision",
        "facadeEpoch",
        "epochSequence",
        "epochHeadHash",
        "snapshotSequence",
        "snapshotHeadHash",
        "burnSequence",
        "burnHeadHash",
        "updatedAt",
        "updatedMonotonicNs",
        "keyIdHash",
        "productionAvailable",
    }
)
_HIGH_WATER_KEYS = _HIGH_WATER_BODY_KEYS | {"recordHash", "signature"}

_SCHEMA_SQL = (
    """
    CREATE TABLE kis_functional_app_facade_meta (
        singleton INTEGER PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        schema_fingerprint TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE kis_functional_app_facade_epoch_transition (
        sequence_no INTEGER PRIMARY KEY,
        facade_epoch INTEGER NOT NULL,
        owner_epoch INTEGER NOT NULL,
        state TEXT NOT NULL,
        previous_entry_hash TEXT NOT NULL,
        body_json TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        signature TEXT NOT NULL,
        key_id_hash TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        occurred_monotonic_ns INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE kis_functional_app_facade_snapshot (
        sequence_no INTEGER PRIMARY KEY,
        facade_epoch INTEGER NOT NULL,
        owner_epoch INTEGER NOT NULL,
        state_revision INTEGER NOT NULL,
        previous_entry_hash TEXT NOT NULL,
        body_json TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        signature TEXT NOT NULL,
        key_id_hash TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        observed_monotonic_ns INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE kis_functional_app_facade_cleanup_burn (
        sequence_no INTEGER PRIMARY KEY,
        grant_hash TEXT NOT NULL,
        grant_burn_key TEXT NOT NULL,
        facade_epoch INTEGER NOT NULL,
        owner_epoch INTEGER NOT NULL,
        state_revision INTEGER NOT NULL,
        snapshot_sequence INTEGER NOT NULL,
        previous_entry_hash TEXT NOT NULL,
        body_json TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        signature TEXT NOT NULL,
        key_id_hash TEXT NOT NULL,
        burned_at TEXT NOT NULL,
        burned_monotonic_ns INTEGER NOT NULL
    )
    """,
)

_EXPECTED_COLUMNS = {
    "kis_functional_app_facade_meta": (
        "singleton",
        "schema_version",
        "schema_fingerprint",
    ),
    "kis_functional_app_facade_epoch_transition": (
        "sequence_no",
        "facade_epoch",
        "owner_epoch",
        "state",
        "previous_entry_hash",
        "body_json",
        "record_hash",
        "signature",
        "key_id_hash",
        "occurred_at",
        "occurred_monotonic_ns",
    ),
    "kis_functional_app_facade_snapshot": (
        "sequence_no",
        "facade_epoch",
        "owner_epoch",
        "state_revision",
        "previous_entry_hash",
        "body_json",
        "record_hash",
        "signature",
        "key_id_hash",
        "observed_at",
        "observed_monotonic_ns",
    ),
    "kis_functional_app_facade_cleanup_burn": (
        "sequence_no",
        "grant_hash",
        "grant_burn_key",
        "facade_epoch",
        "owner_epoch",
        "state_revision",
        "snapshot_sequence",
        "previous_entry_hash",
        "body_json",
        "record_hash",
        "signature",
        "key_id_hash",
        "burned_at",
        "burned_monotonic_ns",
    ),
}


class KisDomesticFunctionalAppFacadeBlocked(RuntimeError):
    """Fail-closed application-composition error."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-canonical-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalAppFacadeBlocked(f"{label}-invalid")
    return value


def _identity(value: Any, label: str) -> str:
    if type(value) is not str or not _IDENTITY.fullmatch(value):
        raise KisDomesticFunctionalAppFacadeBlocked(f"{label}-invalid")
    return value


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalAppFacadeBlocked(f"{label}-invalid")
    return value.astimezone(timezone.utc)


def _time_text(value: datetime) -> str:
    return _utc(value, "app-facade-time").isoformat().replace("+00:00", "Z")


def _parse_time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalAppFacadeBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalAppFacadeBlocked(f"{label}-invalid") from exc
    return _utc(parsed, label)


def _verify_status_hash(value: Mapping[str, Any], label: str) -> dict[str, Any]:
    raw = dict(value)
    digest = raw.pop("statusHash", None)
    if type(digest) is not str or not hmac.compare_digest(digest, _hash(raw)):
        raise KisDomesticFunctionalAppFacadeBlocked(f"{label}-hash-invalid")
    return dict(value)


def derive_state_owner_epoch_binding(
    *,
    owner_status: Mapping[str, Any],
    factory_status: Mapping[str, Any],
    owner_authority_key_id_hash: str,
) -> dict[str, Any]:
    """Derive the immutable state-owner binding from verified owner lineage.

    The result is stable across owner heartbeats but changes for every durable
    owner epoch/process identity or factory binding.  A state constructed with
    an arbitrary owner-epoch callback therefore cannot join this facade.
    """

    if not isinstance(owner_status, Mapping) or not isinstance(
        factory_status, Mapping
    ):
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-owner-binding-source-invalid"
        )
    epoch = owner_status.get("epoch")
    if (
        type(epoch) is not int
        or epoch < 1
        or factory_status.get("ownerEpoch") != epoch
        or owner_status.get("route") != ROUTE
        or factory_status.get("route") != ROUTE
        or owner_status.get("pdno") != PDNO
        or factory_status.get("pdno") != PDNO
    ):
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-owner-binding-epoch-route-invalid"
        )
    body = {
        "schemaVersion": "kis-domestic-functional-app-owner-binding/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "ownerEpoch": epoch,
        "ownerIdHash": _sha(
            owner_status.get("ownerIdHash"), "app-facade-binding-owner-id"
        ),
        "processIdentityHash": _sha(
            owner_status.get("processIdentityHash"),
            "app-facade-binding-process-identity",
        ),
        "leaseScopeHash": _sha(
            owner_status.get("leaseScopeHash"),
            "app-facade-binding-lease-scope",
        ),
        "leaseFactoryHash": _sha(
            owner_status.get("leaseFactoryHash"),
            "app-facade-binding-lease-factory",
        ),
        "ownerAuthorityKeyIdHash": _sha(
            owner_authority_key_id_hash,
            "app-facade-binding-owner-authority-key",
        ),
        "factoryBindingHash": _sha(
            factory_status.get("factoryBindingHash"),
            "app-facade-binding-factory",
        ),
        "productionAvailable": False,
    }
    binding_hash = _hash(body)
    return {
        "ownerEpochId": f"kis-owner-epoch-{epoch}-{binding_hash[:32]}",
        "ownerEpochHash": binding_hash,
        "bindingBody": body,
    }


def _normalize_control_reservation(
    value: Any, *, state_revision: int, phase: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-control-reservation-invalid"
        )
    if not value:
        return {}
    keys = {
        "reservationId",
        "reservationKind",
        "reservationRevision",
        "stateRevision",
        "phase",
        "reservationBindingHash",
    }
    if set(value) != keys:
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-control-reservation-shape-invalid"
        )
    result = {key: value[key] for key in sorted(keys)}
    _identity(result["reservationId"], "app-facade-reservation-id")
    if result["reservationKind"] not in {"START", "STOP", "KILL", "SETTINGS"}:
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-reservation-kind-invalid"
        )
    if (
        type(result["reservationRevision"]) is not int
        or result["reservationRevision"] < 2
        or type(result["stateRevision"]) is not int
        or result["stateRevision"] != state_revision
        or result["stateRevision"] < result["reservationRevision"]
        or result["phase"] != phase
    ):
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-control-reservation-revision-phase-invalid"
        )
    _sha(
        result["reservationBindingHash"],
        "app-facade-reservation-binding-hash",
    )
    return result


def _normalize_intent(value: Any, operation: str) -> dict[str, Any]:
    if operation not in _OPERATIONS:
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-cleanup-operation-invalid"
        )
    required = {
        "operation",
        "claimId",
        "ownedOrderKey",
        "accountFingerprint",
        "credentialConfigurationHash",
        "endpoint",
        "payloadHash",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-cleanup-intent-shape-invalid"
        )
    if value.get("operation") != operation:
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-cleanup-operation-changed"
        )
    claim_id = _identity(value.get("claimId"), "app-facade-cleanup-claim-id")
    account = _sha(
        value.get("accountFingerprint"), "app-facade-cleanup-account"
    )
    credential = _sha(
        value.get("credentialConfigurationHash"),
        "app-facade-cleanup-credential",
    )
    payload_hash = _sha(
        value.get("payloadHash"), "app-facade-cleanup-payload-hash"
    )
    endpoint = value.get("endpoint")
    if type(endpoint) is not str or not _ENDPOINT.fullmatch(endpoint):
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-cleanup-endpoint-invalid"
        )
    owned = value.get("ownedOrderKey")
    owned_keys = {"orderDate", "organizationNo", "orderNo"}
    if not isinstance(owned, Mapping) or set(owned) != owned_keys:
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-owned-order-key-shape-invalid"
        )
    normalized_owned = {key: owned[key] for key in sorted(owned_keys)}
    if any(type(item) is not str for item in normalized_owned.values()):
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-owned-order-key-value-invalid"
        )
    if operation in {"CLEANUP_CANCEL", "KILL_ORDINARY_CANCEL"}:
        if any(not _OFFICIAL_ID.fullmatch(item) for item in normalized_owned.values()):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-owned-cancel-key-invalid"
            )
    elif any(normalized_owned.values()):
        raise KisDomesticFunctionalAppFacadeBlocked(
            "app-facade-cleanup-sell-cannot-claim-order-key"
        )
    return {
        "operation": operation,
        "claimId": claim_id,
        "ownedOrderKey": normalized_owned,
        "accountFingerprint": account,
        "credentialConfigurationHash": credential,
        "endpoint": endpoint,
        "payloadHash": payload_hash,
    }


class DurableKisDomesticFunctionalAppFacade:
    """One offline process-lifetime composition of the frozen KIS objects."""

    def __init__(
        self,
        *,
        state: DurableKisDomesticFunctionalState,
        owner: DurableKisDomesticFunctionalOwner,
        factory: DisabledKisDomesticFunctionalProductionFactory,
        high_water_path: str | Path,
        manager_receipt_authority: (
            VerifyOnlyKisDomesticFunctionalManagerAuthority | None
        ) = None,
        independent_monotonic_anchor: (
            AppendOnlyKisDomesticFunctionalFacadeAnchor | None
        ) = None,
        opening_anchor_writer: ExternalAnchorWriter | None = None,
        maximum_snapshot_age_seconds: int = 2,
    ) -> None:
        if type(state) is not DurableKisDomesticFunctionalState:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-exact-state-required"
            )
        if type(owner) is not DurableKisDomesticFunctionalOwner:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-exact-owner-required"
            )
        if type(factory) is not DisabledKisDomesticFunctionalProductionFactory:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-exact-factory-required"
            )
        if type(state.ledger) is not ProgramLedger:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-exact-program-ledger-required"
            )
        if (
            type(maximum_snapshot_age_seconds) is not int
            or not 1 <= maximum_snapshot_age_seconds <= 30
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-snapshot-age-invalid"
            )
        self._state = state
        self._owner = owner
        self._factory = factory
        self._ledger_path = Path(state.ledger.path).expanduser().resolve()
        self._high_water_path = Path(high_water_path).expanduser().resolve()
        if self._ledger_path == self._high_water_path:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-must-be-independent-file"
            )
        if (manager_receipt_authority is None) is not (
            independent_monotonic_anchor is None
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-manager-authority-anchor-pair-required"
            )
        if (
            manager_receipt_authority is not None
            and type(manager_receipt_authority)
            is not VerifyOnlyKisDomesticFunctionalManagerAuthority
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-exact-manager-receipt-authority-required"
            )
        if (
            independent_monotonic_anchor is not None
            and type(independent_monotonic_anchor)
            is not AppendOnlyKisDomesticFunctionalFacadeAnchor
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-exact-independent-monotonic-anchor-required"
            )
        if manager_receipt_authority is None and opening_anchor_writer is not None:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-unconfigured-anchor-writer-forbidden"
            )
        self._manager_receipt_authority = manager_receipt_authority
        self._independent_monotonic_anchor = independent_monotonic_anchor
        self._facade_anchor_ledger_id = derive_facade_anchor_ledger_id(
            self._ledger_path
        )
        if independent_monotonic_anchor is not None:
            anchor_path = independent_monotonic_anchor.path.resolve()
            if anchor_path in {self._ledger_path, self._high_water_path}:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-external-anchor-path-not-independent"
                )
        self._maximum_snapshot_age_seconds = maximum_snapshot_age_seconds
        self._lock = threading.RLock()
        self._closed = False
        self._facade_epoch = 0
        self._reader = self.authority_snapshot
        self._signer = owner.signer
        self._verifier = owner.verifier
        self._key_id_hash = _sha(
            owner.authority_key_id_hash, "app-facade-owner-authority-key"
        )
        self._wall_clock = owner.clock
        self._monotonic_clock = owner.monotonic_clock
        if not all(
            callable(value)
            for value in (
                self._signer,
                self._verifier,
                self._wall_clock,
                self._monotonic_clock,
            )
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-owner-signing-clock-surface-invalid"
            )

        source = self._source_bundle()
        self._validate_offline_integrations(source)
        had_schema = self._prepare_schema()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            histories = self._verify_histories(conn)
            self._verify_high_water(histories, allow_absent=not had_schema)
            self._join_external_anchor(histories, require_exact=False)
            latest = histories["latestEpoch"]
            if latest is not None and latest["state"] == "OPEN":
                if int(source["owner"]["epoch"]) <= int(latest["ownerEpoch"]):
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-active-epoch-already-owned"
                    )
                self._append_epoch(
                    conn,
                    histories=histories,
                    facade_epoch=int(latest["facadeEpoch"]),
                    state="SUPERSEDED",
                    source=source,
                    owner_epoch=int(latest["ownerEpoch"]),
                )
                histories = self._verify_histories(conn)
            self._facade_epoch = (
                1
                if histories["latestEpoch"] is None
                else int(histories["latestEpoch"]["facadeEpoch"]) + 1
            )
            self._append_epoch(
                conn,
                histories=histories,
                facade_epoch=self._facade_epoch,
                state="OPEN",
                source=source,
                owner_epoch=int(source["owner"]["epoch"]),
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        self._synchronize_high_water()
        if self._independent_monotonic_anchor is not None:
            histories = self._current_verified_histories()
            if opening_anchor_writer is not None:
                self._seal_external_anchor(
                    histories, writer=opening_anchor_writer
                )

    @property
    def facade_epoch(self) -> int:
        return self._facade_epoch

    @property
    def reader(self):
        """Stable bound reader for a later explicit shared-state registration."""

        return self._reader

    @property
    def high_water_path(self) -> Path:
        return self._high_water_path

    @property
    def facade_anchor_ledger_id(self) -> str:
        return self._facade_anchor_ledger_id

    @property
    def offline_authority_anchor_integrated(self) -> bool:
        return self._manager_receipt_authority is not None

    @staticmethod
    def _verified_component_status(
        value: Mapping[str, Any], *, label: str
    ) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise KisDomesticFunctionalAppFacadeBlocked(
                f"app-facade-{label}-status-invalid"
            )
        result = dict(value)
        claimed = result.pop("statusHash", None)
        if (
            type(claimed) is not str
            or not hmac.compare_digest(claimed, _hash(result))
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                f"app-facade-{label}-status-hash-invalid"
            )
        return {**result, "statusHash": claimed}

    def _validate_offline_integrations(
        self, source: Mapping[str, Any]
    ) -> dict[str, Any]:
        if self._manager_receipt_authority is None:
            return {
                "configured": False,
                "manager": None,
                "anchor": None,
            }
        manager = self._verified_component_status(
            self._manager_receipt_authority.status(),
            label="manager-authority",
        )
        anchor = self._verified_component_status(
            self._independent_monotonic_anchor.read(),
            label="external-anchor",
        )
        manager_expected = {
            "route": ROUTE,
            "pdno": PDNO,
            "managerKeyPurpose": MANAGER_KEY_PURPOSE,
            "rootSignatureVerified": True,
            "dedicatedManagerPurposeVerified": True,
            "verifyOnlyConsumer": True,
            "privateSignerPresent": False,
            "consumerSigningSurface": False,
            "productionProvisioningAvailable": False,
            "integrationAccepted": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseAvailable": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        anchor_expected = {
            "route": ROUTE,
            "pdno": PDNO,
            "facadeLedgerIdHash": self._facade_anchor_ledger_id,
            "rootInstallationSignatureVerified": True,
            "writerPurposeVerified": True,
            "appendOnlyChainVerified": True,
            "osProcessLeaseHeld": True,
            "pathFileIdentityPinnedForProcessLifetime": True,
            "pairedFacadeAndLocalHighWaterRollbackDetected": True,
            "externalMinimumRollbackPinSuppliedAndVerified": True,
            "externalMinimumRollbackPinStoreWired": False,
            "hardwareOrWormMonotonicityProven": False,
            "verifyOnlyConsumer": True,
            "privateSignerPresent": False,
            "productionProvisioningAvailable": False,
            "facadeIntegrationWired": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseAvailable": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        if any(
            type(manager.get(key)) is not type(wanted)
            or manager.get(key) != wanted
            for key, wanted in manager_expected.items()
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-manager-authority-status-binding-invalid"
            )
        if any(
            type(anchor.get(key)) is not type(wanted)
            or anchor.get(key) != wanted
            for key, wanted in anchor_expected.items()
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-status-binding-invalid"
            )
        pins = self._manager_receipt_authority.pins
        factory_status = source["factory"]
        authority = source["authority"]
        if (
            pins.account_fingerprint
            != authority["functionalAccountFingerprint"]
            or pins.account_fingerprint
            != factory_status["accountFingerprint"]
            or pins.credential_configuration_hash
            != authority["credentialConfigurationHash"]
            or pins.credential_configuration_hash
            != factory_status["credentialConfigurationHash"]
            or pins.code_manifest_hash != factory_status["codeManifestHash"]
            or self._independent_monotonic_anchor.pins.facade_ledger_id_hash
            != self._facade_anchor_ledger_id
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-offline-authority-anchor-source-join-invalid"
            )
        return {"configured": True, "manager": manager, "anchor": anchor}

    def _current_verified_histories(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            histories = self._verify_histories(conn)
            self._verify_high_water(histories, allow_absent=False)
            conn.commit()
            return histories
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _external_anchor_projection(
        self, histories: Mapping[str, Any]
    ) -> dict[str, Any]:
        latest = histories["latestEpoch"]
        return {
            "schemaVersion": EXTERNAL_ANCHOR_PROJECTION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "facadeLedgerIdHash": self._facade_anchor_ledger_id,
            "facadeEpoch": 0 if latest is None else int(latest["facadeEpoch"]),
            "epochSequence": int(histories["epochSequence"]),
            "epochHeadHash": str(histories["epochHeadHash"]),
            "snapshotSequence": int(histories["snapshotSequence"]),
            "snapshotHeadHash": str(histories["snapshotHeadHash"]),
            "burnSequence": int(histories["burnSequence"]),
            "burnHeadHash": str(histories["burnHeadHash"]),
        }

    def _join_external_anchor(
        self,
        histories: Mapping[str, Any],
        *,
        require_exact: bool,
    ) -> dict[str, Any]:
        if self._independent_monotonic_anchor is None:
            return {
                "configured": False,
                "exact": False,
                "pending": False,
                "projection": None,
                "anchorProjection": None,
            }
        candidate = self._external_anchor_projection(histories)
        anchored = self._independent_monotonic_anchor.current_projection()
        if anchored.get("facadeLedgerIdHash") != self._facade_anchor_ledger_id:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-ledger-id-changed"
            )
        if int(anchored["facadeEpoch"]) > int(candidate["facadeEpoch"]):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-facade-epoch-rollback"
            )
        for sequence_key, head_key, hashes_key in (
            ("epochSequence", "epochHeadHash", "epochHashes"),
            ("snapshotSequence", "snapshotHeadHash", "snapshotHashes"),
            ("burnSequence", "burnHeadHash", "burnHashes"),
        ):
            anchored_sequence = int(anchored[sequence_key])
            local_sequence = int(candidate[sequence_key])
            if anchored_sequence > local_sequence:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-paired-ledger-high-water-rollback-detected"
                )
            if anchored_sequence == 0:
                if anchored[head_key] != _ZERO_HASH:
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-external-anchor-zero-head-invalid"
                    )
            elif (
                len(histories[hashes_key]) < anchored_sequence
                or histories[hashes_key][anchored_sequence - 1]
                != anchored[head_key]
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-external-anchor-history-prefix-mismatch"
                )
        exact = candidate == anchored
        if exact:
            self._independent_monotonic_anchor.assert_current_projection(
                candidate
            )
        elif require_exact:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-reconciliation-required"
            )
        return {
            "configured": True,
            "exact": exact,
            "pending": not exact,
            "projection": candidate,
            "anchorProjection": anchored,
        }

    def _seal_external_anchor(
        self,
        histories: Mapping[str, Any],
        *,
        writer: ExternalAnchorWriter | None,
    ) -> dict[str, Any]:
        joined = self._join_external_anchor(histories, require_exact=False)
        if not joined["configured"] or joined["exact"]:
            return joined
        if not callable(writer):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-writer-required"
            )
        wall, monotonic_ns = self._sample_time()
        body = self._independent_monotonic_anchor.next_transition_body(
            joined["projection"],
            observed_at=wall,
            observed_monotonic_ns=monotonic_ns,
        )
        try:
            envelope = writer(deepcopy(body))
        except BaseException as exc:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-writer-failed"
            ) from exc
        if not isinstance(envelope, Mapping):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-writer-result-invalid"
            )
        self._independent_monotonic_anchor.append_signed_transition(envelope)
        return self._join_external_anchor(histories, require_exact=True)

    def external_anchor_next_transition_body(self) -> dict[str, Any]:
        with kis_route_authority_serialization():
            with self._lock:
                if self._independent_monotonic_anchor is None:
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-external-anchor-not-configured"
                    )
                source = self._source_bundle()
                self._validate_offline_integrations(source)
                histories = self._current_verified_histories()
                joined = self._join_external_anchor(
                    histories, require_exact=False
                )
                if joined["exact"]:
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-external-anchor-already-exact"
                    )
                wall, monotonic_ns = self._sample_time()
                return self._independent_monotonic_anchor.next_transition_body(
                    joined["projection"],
                    observed_at=wall,
                    observed_monotonic_ns=monotonic_ns,
                )

    def accept_external_anchor_transition(
        self, envelope: Mapping[str, Any]
    ) -> dict[str, Any]:
        with kis_route_authority_serialization():
            with self._lock:
                if self._independent_monotonic_anchor is None:
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-external-anchor-not-configured"
                    )
                source = self._source_bundle()
                self._validate_offline_integrations(source)
                histories = self._current_verified_histories()
                self._join_external_anchor(histories, require_exact=False)
                self._independent_monotonic_anchor.append_signed_transition(
                    envelope
                )
                joined = self._join_external_anchor(
                    histories, require_exact=True
                )
                return {
                    "externalAnchorExactJoin": joined["exact"],
                    "externalAnchorStatus": (
                        self._independent_monotonic_anchor.read()
                    ),
                    "productionAvailable": False,
                    "networkOrderPostAllowed": False,
                    "tradingMutationCount": 0,
                }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._ledger_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _sample_time(self) -> tuple[datetime, int]:
        wall = _utc(self._wall_clock(), "app-facade-trusted-wall-clock")
        monotonic_ns = self._monotonic_clock()
        if type(monotonic_ns) is not int or monotonic_ns < 0:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-trusted-monotonic-clock-invalid"
            )
        return wall, monotonic_ns

    def _sign(self, domain: str, body: Mapping[str, Any]) -> str:
        try:
            signature = self._signer(domain, deepcopy(dict(body)))
            valid = self._verifier(
                domain, deepcopy(dict(body)), signature
            )
        except BaseException as exc:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signature-operation-failed"
            ) from exc
        if type(signature) is not str or not signature or valid is not True:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signature-self-verification-failed"
            )
        return signature

    def _verify_signature(
        self, domain: str, body: Mapping[str, Any], signature: Any
    ) -> None:
        if type(signature) is not str or not signature:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signature-invalid"
            )
        try:
            valid = self._verifier(
                domain, deepcopy(dict(body)), signature
            )
        except BaseException as exc:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signature-verifier-failed"
            ) from exc
        if valid is not True:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signature-unverified"
            )

    def _prepare_schema(self) -> bool:
        self._ledger_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            existing = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name LIKE 'kis_functional_app_facade_%'"
                ).fetchall()
            }
            had_schema = bool(existing)
            if existing and existing != set(_EXPECTED_COLUMNS):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-schema-partial-or-unknown"
                )
            if not existing:
                for statement in _SCHEMA_SQL:
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO kis_functional_app_facade_meta VALUES(1,?,?)",
                    (SCHEMA_VERSION, SCHEMA_FINGERPRINT),
                )
            self._verify_schema(conn)
            conn.commit()
            return had_schema
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _verify_schema(conn: sqlite3.Connection) -> None:
        objects = conn.execute(
            "SELECT type,name FROM sqlite_master "
            "WHERE name LIKE 'kis_functional_app_facade_%' "
            "ORDER BY type,name"
        ).fetchall()
        expected = sorted(("table", name) for name in _EXPECTED_COLUMNS)
        if [tuple(row) for row in objects] != expected:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-schema-object-set-invalid"
            )
        for table, columns in _EXPECTED_COLUMNS.items():
            observed = tuple(
                str(row[1])
                for row in conn.execute(f'PRAGMA table_info("{table}")')
            )
            if observed != columns:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    f"app-facade-schema-columns-invalid:{table}"
                )
        meta = conn.execute(
            "SELECT singleton,schema_version,schema_fingerprint "
            "FROM kis_functional_app_facade_meta"
        ).fetchall()
        if len(meta) != 1 or tuple(meta[0]) != (
            1,
            SCHEMA_VERSION,
            SCHEMA_FINGERPRINT,
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-schema-meta-invalid"
            )

    def _source_bundle(self) -> dict[str, Any]:
        if self._closed:
            raise KisDomesticFunctionalAppFacadeBlocked("app-facade-closed")
        try:
            owner_status = _verify_status_hash(
                self._owner.status(expected_epoch=self._owner.epoch),
                "app-facade-owner-status",
            )
            factory_status = _verify_status_hash(
                self._factory.status(), "app-facade-factory-status"
            )
            constructors = self._factory.state_manager_constructors()
            constructors_status = _verify_status_hash(
                constructors.status(), "app-facade-constructors-status"
            )
            state_snapshot = self._state.authority_snapshot()
        except KisDomesticFunctionalAppFacadeBlocked:
            raise
        except BaseException as exc:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-source-unreadable"
            ) from exc
        if type(constructors) is not RegistryBoundStateManagerConstructors:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-exact-constructor-bundle-required"
            )
        if (
            constructors.state_constructor is not DurableKisDomesticFunctionalState
            or constructors.manager_constructor is not DisabledKisDomesticFunctionalManager
            or constructors.graph_port_constructor is not FrozenKisDomesticGraphLedgerPort
            or constructors_status.get("exactConstructorsPinned") is not True
            or constructors_status.get("stateReceiptV2IntegrationWired") is not True
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-state-manager-graph-constructor-pin-invalid"
            )
        factory_owner_path = getattr(
            self._factory.owner_epoch_reader(), "_path", None
        )
        if not isinstance(factory_owner_path, Path) or (
            factory_owner_path.expanduser().resolve()
            != Path(self._owner.path).expanduser().resolve()
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-factory-owner-path-not-exact"
            )
        if not isinstance(state_snapshot, Mapping):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-state-snapshot-invalid"
            )
        authority = self._normalize_authority(state_snapshot)
        owner_epoch = self._owner.epoch
        owner_binding = derive_state_owner_epoch_binding(
            owner_status=owner_status,
            factory_status=factory_status,
            owner_authority_key_id_hash=self._key_id_hash,
        )
        if (
            owner_status.get("route") != ROUTE
            or owner_status.get("pdno") != PDNO
            or owner_status.get("epoch") != owner_epoch
            or owner_status.get("state") != "ACTIVE"
            or owner_status.get("heartbeatLeaseFresh") is not True
            or owner_status.get("osLeaseHeld") is not True
            or factory_status.get("route") != ROUTE
            or factory_status.get("pdno") != PDNO
            or factory_status.get("ownerEpoch") != owner_epoch
            or factory_status.get("accountFingerprint")
            != authority["functionalAccountFingerprint"]
            or factory_status.get("credentialConfigurationHash")
            != authority["credentialConfigurationHash"]
            or authority["applicationInstanceLeaseHeld"] is not True
            or authority["ownerEpochId"] != owner_binding["ownerEpochId"]
            or authority["ownerEpochHash"] != owner_binding["ownerEpochHash"]
            or state_snapshot.get("stateReceiptV2IntegrationWired") is not True
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-owner-state-factory-join-invalid"
            )
        for status in (owner_status, factory_status, constructors_status):
            if (
                status.get("productionAvailable") is not False
                or status.get("networkAvailable") is not False
                or status.get("releaseAvailable") is not False
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-offline-source-flag-invalid"
                )
        return {
            "owner": owner_status,
            "factory": factory_status,
            "constructors": constructors_status,
            "authority": authority,
            "ownerBinding": owner_binding,
        }

    @staticmethod
    def _normalize_authority(value: Mapping[str, Any]) -> dict[str, Any]:
        if not _AUTHORITY_KEYS <= set(value):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-state-authority-incomplete"
            )
        result = {key: deepcopy(value[key]) for key in sorted(_AUTHORITY_KEYS)}
        for key in (
            "durableAuthorityReadable",
            "functionalAuthorityOpen",
            "killOrdinaryCancelAllowed",
            "applicationInstanceLeaseHeld",
            "ordinaryRoutesClosed",
        ):
            if type(result[key]) is not bool:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    f"app-facade-state-authority-boolean-invalid:{key}"
                )
        if result["durableAuthorityReadable"] is not True:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-state-authority-unreadable"
            )
        phase = result["functionalPhase"]
        if type(phase) is not str or phase not in _OPEN_PHASES | {"IDLE"}:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-functional-phase-invalid"
            )
        revision = result["stateRevision"]
        if (
            type(revision) is not int
            or revision < 1
            or result["functionalRevision"] != revision
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-state-revision-invalid"
            )
        expected_open = phase in _OPEN_PHASES
        if (
            result["functionalAuthorityOpen"] is not expected_open
            or result["ordinaryRoutesClosed"] is not expected_open
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-phase-route-invariant-invalid"
            )
        _sha(
            result["functionalAccountFingerprint"],
            "app-facade-account-fingerprint",
        )
        _sha(
            result["credentialConfigurationHash"],
            "app-facade-credential-configuration",
        )
        _identity(result["ownerEpochId"], "app-facade-state-owner-epoch-id")
        _sha(result["ownerEpochHash"], "app-facade-state-owner-epoch-hash")
        session = result["functionalSessionId"]
        if type(session) is not str or (session and not _IDENTITY.fullmatch(session)):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-functional-session-invalid"
            )
        if not isinstance(result["functionalMutationIntent"], Mapping):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-functional-intent-invalid"
            )
        if not isinstance(result["killOrdinaryCancelIntent"], Mapping):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-kill-intent-invalid"
            )
        kill_revision = result["killOrdinaryCancelRevision"]
        if type(kill_revision) is not int or kill_revision < 0:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-kill-revision-invalid"
            )
        if result["killOrdinaryCancelAllowed"] is False and result[
            "killOrdinaryCancelIntent"
        ]:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-disabled-kill-retained-intent"
            )
        result["controlReservation"] = _normalize_control_reservation(
            result["controlReservation"],
            state_revision=revision,
            phase=phase,
        )
        return result

    def _kill_grant(self, authority: Mapping[str, Any]) -> dict[str, Any]:
        if authority["killOrdinaryCancelAllowed"] is not True:
            return {}
        intent = _normalize_intent(
            authority["killOrdinaryCancelIntent"], "KILL_ORDINARY_CANCEL"
        )
        revision = authority["killOrdinaryCancelRevision"]
        if revision < 1:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-kill-grant-revision-invalid"
            )
        return {
            "ownerEpochId": authority["ownerEpochId"],
            "ownerEpochHash": authority["ownerEpochHash"],
            "stateRevision": authority["stateRevision"],
            "killRevision": revision,
            "intent": intent,
            "intentHash": _hash(intent),
        }

    def _append_epoch(
        self,
        conn: sqlite3.Connection,
        *,
        histories: Mapping[str, Any],
        facade_epoch: int,
        state: str,
        source: Mapping[str, Any],
        owner_epoch: int,
    ) -> None:
        if state not in {"OPEN", "RELEASED", "SUPERSEDED"}:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-epoch-state-invalid"
            )
        wall, monotonic_ns = self._sample_time()
        sequence = int(histories["epochSequence"]) + 1
        previous = str(histories["epochHeadHash"])
        body = {
            "schemaVersion": EPOCH_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "sequenceNo": sequence,
            "facadeEpoch": facade_epoch,
            "ownerEpoch": owner_epoch,
            "ownerIdHash": source["owner"]["ownerIdHash"],
            "processIdentityHash": source["owner"]["processIdentityHash"],
            "factoryBindingHash": source["factory"]["factoryBindingHash"],
            "state": state,
            "previousEntryHash": previous,
            "occurredAt": _time_text(wall),
            "occurredMonotonicNs": monotonic_ns,
            "keyIdHash": self._key_id_hash,
            "productionAvailable": False,
        }
        signature = self._sign("KIS_FUNCTIONAL_APP_EPOCH", body)
        record_hash = _hash(body)
        conn.execute(
            "INSERT INTO kis_functional_app_facade_epoch_transition "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                sequence,
                facade_epoch,
                owner_epoch,
                state,
                previous,
                _canonical(body).decode("utf-8"),
                record_hash,
                signature,
                self._key_id_hash,
                body["occurredAt"],
                monotonic_ns,
            ),
        )

    def _snapshot_body(
        self,
        *,
        source: Mapping[str, Any],
        sequence: int,
        previous_hash: str,
        wall: datetime,
        monotonic_ns: int,
    ) -> dict[str, Any]:
        authority = dict(source["authority"])
        control = dict(authority["controlReservation"])
        body = {
            **authority,
            "schemaVersion": SNAPSHOT_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "facadeEpoch": self._facade_epoch,
            "snapshotSequence": sequence,
            "previousSnapshotHash": previous_hash,
            "observedAt": _time_text(wall),
            "observedMonotonicNs": monotonic_ns,
            "expiresAt": _time_text(
                wall + timedelta(seconds=self._maximum_snapshot_age_seconds)
            ),
            "ownerEpoch": source["owner"]["epoch"],
            "ownerStatusHash": source["owner"]["statusHash"],
            "ownerIdHash": source["owner"]["ownerIdHash"],
            "processIdentityHash": source["owner"]["processIdentityHash"],
            "factoryStatusHash": source["factory"]["statusHash"],
            "factoryBindingHash": source["factory"]["factoryBindingHash"],
            "factoryOwnerReaderResultHash": source["factory"][
                "ownerReaderResultHash"
            ],
            "constructorsStatusHash": source["constructors"]["statusHash"],
            "stateAuthorityHash": _hash(authority),
            "controlReservationHash": _hash(control),
            "killGrant": self._kill_grant(authority),
            "snapshotKeyIdHash": self._key_id_hash,
            "readerRegistered": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseAvailable": False,
            "networkOrderPostAllowed": False,
        }
        if set(body) != _SNAPSHOT_BODY_KEYS:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-snapshot-body-shape-invalid"
            )
        return body

    def _insert_snapshot(
        self,
        conn: sqlite3.Connection,
        *,
        histories: Mapping[str, Any],
        source: Mapping[str, Any],
    ) -> dict[str, Any]:
        wall, monotonic_ns = self._sample_time()
        last_wall = histories.get("lastSnapshotWall")
        last_mono = histories.get("lastSnapshotMonotonic")
        if last_wall is not None:
            wall_delta = (wall - last_wall).total_seconds()
            mono_delta = (monotonic_ns - int(last_mono)) / 1_000_000_000
            if wall_delta < 0 or mono_delta < 0 or abs(wall_delta - mono_delta) > 2:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-snapshot-clock-rollback-or-divergence"
                )
        sequence = int(histories["snapshotSequence"]) + 1
        previous = str(histories["snapshotHeadHash"])
        body = self._snapshot_body(
            source=source,
            sequence=sequence,
            previous_hash=previous,
            wall=wall,
            monotonic_ns=monotonic_ns,
        )
        signature = self._sign("KIS_FUNCTIONAL_APP_SNAPSHOT", body)
        record_hash = _hash(body)
        conn.execute(
            "INSERT INTO kis_functional_app_facade_snapshot "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                sequence,
                self._facade_epoch,
                int(source["owner"]["epoch"]),
                int(body["stateRevision"]),
                previous,
                _canonical(body).decode("utf-8"),
                record_hash,
                signature,
                self._key_id_hash,
                body["observedAt"],
                monotonic_ns,
            ),
        )
        return {
            **body,
            "snapshotBodyHash": record_hash,
            "snapshotSignature": signature,
        }

    def _verify_histories(self, conn: sqlite3.Connection) -> dict[str, Any]:
        self._verify_schema(conn)
        epoch_rows = conn.execute(
            "SELECT * FROM kis_functional_app_facade_epoch_transition "
            "ORDER BY sequence_no"
        ).fetchall()
        epoch_hashes: list[str] = []
        epoch_states: dict[int, dict[str, Any]] = {}
        previous = _ZERO_HASH
        last_time: datetime | None = None
        last_mono: int | None = None
        maximum_epoch = 0
        for sequence, row in enumerate(epoch_rows, 1):
            body = self._verified_row_body(
                row,
                expected_keys=_EPOCH_BODY_KEYS,
                domain="KIS_FUNCTIONAL_APP_EPOCH",
            )
            if (
                row["sequence_no"] != sequence
                or body["sequenceNo"] != sequence
                or body["schemaVersion"] != EPOCH_SCHEMA
                or body["route"] != ROUTE
                or body["pdno"] != PDNO
                or body["previousEntryHash"] != previous
                or row["previous_entry_hash"] != previous
                or row["facade_epoch"] != body["facadeEpoch"]
                or row["owner_epoch"] != body["ownerEpoch"]
                or row["state"] != body["state"]
                or row["key_id_hash"] != body["keyIdHash"]
                or row["occurred_at"] != body["occurredAt"]
                or row["occurred_monotonic_ns"] != body["occurredMonotonicNs"]
                or body["keyIdHash"] != self._key_id_hash
                or body["productionAvailable"] is not False
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-epoch-chain-projection-invalid"
                )
            occurred = _parse_time(body["occurredAt"], "app-facade-epoch-time")
            monotonic_ns = body["occurredMonotonicNs"]
            if type(monotonic_ns) is not int or monotonic_ns < 0:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-epoch-monotonic-invalid"
                )
            if last_time is not None and (
                occurred < last_time or monotonic_ns < int(last_mono)
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-epoch-time-rollback"
                )
            facade_epoch = body["facadeEpoch"]
            owner_epoch = body["ownerEpoch"]
            if (
                type(facade_epoch) is not int
                or facade_epoch < 1
                or type(owner_epoch) is not int
                or owner_epoch < 1
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-epoch-number-invalid"
                )
            prior = epoch_states.get(facade_epoch)
            if body["state"] == "OPEN":
                if prior is not None or facade_epoch != maximum_epoch + 1:
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-epoch-open-sequence-invalid"
                    )
                maximum_epoch = facade_epoch
            elif (
                prior is None
                or prior["state"] != "OPEN"
                or body["state"] not in {"RELEASED", "SUPERSEDED"}
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-epoch-transition-invalid"
                )
            if prior is not None and (
                body["ownerEpoch"] != prior["ownerEpoch"]
                or body["ownerIdHash"] != prior["ownerIdHash"]
                or body["processIdentityHash"] != prior["processIdentityHash"]
                or body["factoryBindingHash"] != prior["factoryBindingHash"]
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-epoch-identity-changed"
                )
            epoch_states[facade_epoch] = body
            previous = str(row["record_hash"])
            epoch_hashes.append(previous)
            last_time = occurred
            last_mono = monotonic_ns

        snapshot_rows = conn.execute(
            "SELECT * FROM kis_functional_app_facade_snapshot ORDER BY sequence_no"
        ).fetchall()
        snapshot_hashes: list[str] = []
        snapshot_bodies: list[dict[str, Any]] = []
        previous_snapshot = _ZERO_HASH
        last_snapshot_wall: datetime | None = None
        last_snapshot_mono: int | None = None
        for sequence, row in enumerate(snapshot_rows, 1):
            body = self._verified_row_body(
                row,
                expected_keys=_SNAPSHOT_BODY_KEYS,
                domain="KIS_FUNCTIONAL_APP_SNAPSHOT",
            )
            if (
                row["sequence_no"] != sequence
                or body["snapshotSequence"] != sequence
                or body["schemaVersion"] != SNAPSHOT_SCHEMA
                or body["route"] != ROUTE
                or body["pdno"] != PDNO
                or body["previousSnapshotHash"] != previous_snapshot
                or row["previous_entry_hash"] != previous_snapshot
                or row["facade_epoch"] != body["facadeEpoch"]
                or row["owner_epoch"] != body["ownerEpoch"]
                or row["state_revision"] != body["stateRevision"]
                or row["key_id_hash"] != body["snapshotKeyIdHash"]
                or row["observed_at"] != body["observedAt"]
                or row["observed_monotonic_ns"] != body["observedMonotonicNs"]
                or body["snapshotKeyIdHash"] != self._key_id_hash
                or body["readerRegistered"] is not False
                or any(
                    body[key] is not False
                    for key in (
                        "productionAvailable",
                        "networkAvailable",
                        "releaseAvailable",
                        "networkOrderPostAllowed",
                    )
                )
                or body["facadeEpoch"] not in epoch_states
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-snapshot-chain-projection-invalid"
                )
            observed = _parse_time(
                body["observedAt"], "app-facade-snapshot-observed-at"
            )
            expires = _parse_time(
                body["expiresAt"], "app-facade-snapshot-expires-at"
            )
            monotonic_ns = body["observedMonotonicNs"]
            if (
                type(monotonic_ns) is not int
                or monotonic_ns < 0
                or expires <= observed
                or (expires - observed).total_seconds()
                != self._maximum_snapshot_age_seconds
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-snapshot-time-window-invalid"
                )
            if last_snapshot_wall is not None:
                wall_delta = (observed - last_snapshot_wall).total_seconds()
                mono_delta = (
                    monotonic_ns - int(last_snapshot_mono)
                ) / 1_000_000_000
                if wall_delta < 0 or mono_delta < 0 or abs(wall_delta - mono_delta) > 2:
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-snapshot-history-clock-invalid"
                    )
            authority = {key: body[key] for key in _AUTHORITY_KEYS}
            normalized = self._normalize_authority(authority)
            if (
                body["stateAuthorityHash"] != _hash(normalized)
                or body["controlReservationHash"]
                != _hash(normalized["controlReservation"])
                or body["killGrant"] != self._kill_grant(normalized)
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-snapshot-authority-binding-invalid"
                )
            previous_snapshot = str(row["record_hash"])
            snapshot_hashes.append(previous_snapshot)
            snapshot_bodies.append(body)
            last_snapshot_wall = observed
            last_snapshot_mono = monotonic_ns

        burn_rows = conn.execute(
            "SELECT * FROM kis_functional_app_facade_cleanup_burn "
            "ORDER BY sequence_no"
        ).fetchall()
        burn_hashes: list[str] = []
        grants: set[str] = set()
        burn_keys: set[str] = set()
        previous_burn = _ZERO_HASH
        last_burn_wall: datetime | None = None
        last_burn_mono: int | None = None
        for sequence, row in enumerate(burn_rows, 1):
            body = self._verified_row_body(
                row,
                expected_keys=_BURN_BODY_KEYS,
                domain="KIS_FUNCTIONAL_APP_CLEANUP_BURN",
            )
            if (
                row["sequence_no"] != sequence
                or body["sequenceNo"] != sequence
                or body["schemaVersion"] != BURN_SCHEMA
                or body["route"] != ROUTE
                or body["pdno"] != PDNO
                or body["previousEntryHash"] != previous_burn
                or row["previous_entry_hash"] != previous_burn
                or row["grant_hash"] != body["grantHash"]
                or row["grant_burn_key"] != body["grantBurnKey"]
                or row["facade_epoch"] != body["facadeEpoch"]
                or row["owner_epoch"] != body["ownerEpoch"]
                or row["state_revision"] != body["stateRevision"]
                or row["snapshot_sequence"] != body["snapshotSequence"]
                or row["key_id_hash"] != body["keyIdHash"]
                or row["burned_at"] != body["burnedAt"]
                or row["burned_monotonic_ns"] != body["burnedMonotonicNs"]
                or body["keyIdHash"] != self._key_id_hash
                or body["productionAvailable"] is not False
                or body["networkOrderPostAllowed"] is not False
                or body["facadeEpoch"] not in epoch_states
                or not 1 <= body["snapshotSequence"] <= len(snapshot_hashes)
                or snapshot_hashes[body["snapshotSequence"] - 1]
                != body["snapshotBodyHash"]
                or body["grantHash"] in grants
                or body["grantBurnKey"] in burn_keys
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-burn-chain-projection-invalid"
                )
            normalized_intent = _normalize_intent(
                body["intent"], body["operation"]
            )
            snapshot_body = snapshot_bodies[body["snapshotSequence"] - 1]
            if body["operation"] == "KILL_ORDINARY_CANCEL":
                kill_grant = snapshot_body["killGrant"]
                exact_source = bool(
                    kill_grant
                    and kill_grant["intent"] == normalized_intent
                    and kill_grant["intentHash"] == _hash(normalized_intent)
                    and body["killRevision"] == kill_grant["killRevision"]
                    and body["reservationId"] == ""
                    and body["reservationKind"] == "KILL"
                    and body["reservationBindingHash"] == _ZERO_HASH
                )
            else:
                control = snapshot_body["controlReservation"]
                exact_source = bool(
                    snapshot_body["functionalPhase"] == "CLEANUP"
                    and control
                    and control["reservationKind"] in {"STOP", "KILL"}
                    and body["reservationId"] == control["reservationId"]
                    and body["reservationKind"]
                    == control["reservationKind"]
                    and body["reservationBindingHash"]
                    == control["reservationBindingHash"]
                    and body["killRevision"] == 0
                    and snapshot_body["functionalMutationIntent"]
                    == normalized_intent
                )
            grant_body = {
                "schemaVersion": "kis-domestic-functional-state-grant/v1",
                "ownerEpoch": body["ownerEpoch"],
                "ownerEpochId": body["ownerEpochId"],
                "ownerEpochHash": body["ownerEpochHash"],
                "stateRevision": body["stateRevision"],
                "controlReservationHash": body["controlReservationHash"],
                "reservationId": body["reservationId"],
                "reservationKind": body["reservationKind"],
                "reservationBindingHash": body["reservationBindingHash"],
                "operation": body["operation"],
                "intentHash": body["intentHash"],
                "killRevision": body["killRevision"],
            }
            expected_grant_hash = _hash(grant_body)
            expected_burn_key = _hash(
                {
                    "schemaVersion": (
                        "kis-domestic-functional-state-grant-burn-key/v1"
                    ),
                    "grantHash": expected_grant_hash,
                    "ownedOrderKey": normalized_intent["ownedOrderKey"],
                    "claimId": normalized_intent["claimId"],
                }
            )
            if (
                body["intentHash"] != _hash(normalized_intent)
                or body["facadeEpoch"] != snapshot_body["facadeEpoch"]
                or body["ownerEpoch"] != snapshot_body["ownerEpoch"]
                or body["ownerEpochId"] != snapshot_body["ownerEpochId"]
                or body["ownerEpochHash"] != snapshot_body["ownerEpochHash"]
                or body["stateRevision"] != snapshot_body["stateRevision"]
                or body["controlReservationHash"]
                != snapshot_body["controlReservationHash"]
                or not exact_source
                or body["grantHash"] != expected_grant_hash
                or body["grantBurnKey"] != expected_burn_key
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-burn-snapshot-grant-binding-invalid"
                )
            burned = _parse_time(body["burnedAt"], "app-facade-burn-time")
            burned_mono = body["burnedMonotonicNs"]
            if type(burned_mono) is not int or burned_mono < 0:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-burn-monotonic-invalid"
                )
            if last_burn_wall is not None and (
                burned < last_burn_wall or burned_mono < int(last_burn_mono)
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-burn-time-rollback"
                )
            grants.add(body["grantHash"])
            burn_keys.add(body["grantBurnKey"])
            previous_burn = str(row["record_hash"])
            burn_hashes.append(previous_burn)
            last_burn_wall = burned
            last_burn_mono = burned_mono

        latest_epoch = epoch_states.get(maximum_epoch) if maximum_epoch else None
        return {
            "epochSequence": len(epoch_hashes),
            "epochHeadHash": epoch_hashes[-1] if epoch_hashes else _ZERO_HASH,
            "epochHashes": epoch_hashes,
            "snapshotSequence": len(snapshot_hashes),
            "snapshotHeadHash": (
                snapshot_hashes[-1] if snapshot_hashes else _ZERO_HASH
            ),
            "snapshotHashes": snapshot_hashes,
            "snapshotBodies": snapshot_bodies,
            "burnSequence": len(burn_hashes),
            "burnHeadHash": burn_hashes[-1] if burn_hashes else _ZERO_HASH,
            "burnHashes": burn_hashes,
            "grantHashes": grants,
            "grantBurnKeys": burn_keys,
            "latestEpoch": latest_epoch,
            "lastSnapshotWall": last_snapshot_wall,
            "lastSnapshotMonotonic": last_snapshot_mono,
        }

    def _verified_row_body(
        self,
        row: sqlite3.Row,
        *,
        expected_keys: frozenset[str],
        domain: str,
    ) -> dict[str, Any]:
        try:
            body = json.loads(str(row["body_json"]))
        except (TypeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-journal-json-invalid"
            ) from exc
        if not isinstance(body, Mapping) or set(body) != expected_keys:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-journal-body-shape-invalid"
            )
        body = dict(body)
        digest = _hash(body)
        if (
            not hmac.compare_digest(str(row["record_hash"]), digest)
            or str(row["body_json"]) != _canonical(body).decode("utf-8")
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-journal-body-hash-invalid"
            )
        self._verify_signature(domain, body, row["signature"])
        return body

    def _read_high_water(self) -> dict[str, Any] | None:
        if not self._high_water_path.exists():
            return None
        try:
            before = self._high_water_path.read_bytes()
            after = self._high_water_path.read_bytes()
            if before != after or not before.endswith(b"\n"):
                raise ValueError("unstable-or-noncanonical")
            value = json.loads(before.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-unreadable"
            ) from exc
        if not isinstance(value, Mapping) or set(value) != _HIGH_WATER_KEYS:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-shape-invalid"
            )
        value = dict(value)
        signature = value.pop("signature")
        record_hash = value.pop("recordHash")
        if record_hash != _hash(value):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-hash-invalid"
            )
        self._verify_signature("KIS_FUNCTIONAL_APP_HIGH_WATER", value, signature)
        if (
            value["schemaVersion"] != HIGH_WATER_SCHEMA
            or value["route"] != ROUTE
            or value["pdno"] != PDNO
            or value["keyIdHash"] != self._key_id_hash
            or value["productionAvailable"] is not False
            or type(value["revision"]) is not int
            or value["revision"] < 1
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-projection-invalid"
            )
        return {**value, "recordHash": record_hash, "signature": signature}

    def _verify_high_water(
        self, histories: Mapping[str, Any], *, allow_absent: bool
    ) -> dict[str, Any] | None:
        high = self._read_high_water()
        any_records = any(
            int(histories[key])
            for key in ("epochSequence", "snapshotSequence", "burnSequence")
        )
        if high is None:
            if any_records or not allow_absent:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-high-water-missing"
                )
            return None
        dimensions = (
            ("epochSequence", "epochHeadHash", "epochHashes"),
            ("snapshotSequence", "snapshotHeadHash", "snapshotHashes"),
            ("burnSequence", "burnHeadHash", "burnHashes"),
        )
        for sequence_key, head_key, hashes_key in dimensions:
            anchored = high[sequence_key]
            observed = histories[sequence_key]
            if type(anchored) is not int or anchored < 0 or anchored > observed:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-ledger-rollback-below-high-water"
                )
            expected = (
                _ZERO_HASH
                if anchored == 0
                else histories[hashes_key][anchored - 1]
            )
            if high[head_key] != expected:
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-high-water-history-rewritten"
                )
        latest = histories["latestEpoch"]
        observed_epoch = 0 if latest is None else int(latest["facadeEpoch"])
        if type(high["facadeEpoch"]) is not int or not 0 <= high[
            "facadeEpoch"
        ] <= observed_epoch:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-epoch-rollback-below-high-water"
            )
        return high

    def _write_high_water(
        self, histories: Mapping[str, Any], prior: Mapping[str, Any] | None
    ) -> None:
        wall, monotonic_ns = self._sample_time()
        body = {
            "schemaVersion": HIGH_WATER_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "revision": 1 if prior is None else int(prior["revision"]) + 1,
            "facadeEpoch": (
                0
                if histories["latestEpoch"] is None
                else int(histories["latestEpoch"]["facadeEpoch"])
            ),
            "epochSequence": int(histories["epochSequence"]),
            "epochHeadHash": str(histories["epochHeadHash"]),
            "snapshotSequence": int(histories["snapshotSequence"]),
            "snapshotHeadHash": str(histories["snapshotHeadHash"]),
            "burnSequence": int(histories["burnSequence"]),
            "burnHeadHash": str(histories["burnHeadHash"]),
            "updatedAt": _time_text(wall),
            "updatedMonotonicNs": monotonic_ns,
            "keyIdHash": self._key_id_hash,
            "productionAvailable": False,
        }
        if set(body) != _HIGH_WATER_BODY_KEYS:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-body-shape-invalid"
            )
        record = {
            **body,
            "recordHash": _hash(body),
            "signature": self._sign("KIS_FUNCTIONAL_APP_HIGH_WATER", body),
        }
        payload = _canonical(record) + b"\n"
        self._high_water_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._high_water_path.with_name(
            self._high_water_path.name + ".tmp-" + uuid.uuid4().hex
        )
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._high_water_path)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-write-failed"
            ) from exc
        verified = self._read_high_water()
        if verified is None or verified["recordHash"] != record["recordHash"]:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-write-not-durable"
            )

    def _synchronize_high_water(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            histories = self._verify_histories(conn)
            prior = self._read_high_water()
            if prior is not None:
                prior = self._verify_high_water(histories, allow_absent=False)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        exact = all(
            prior is not None and prior[key] == histories[key]
            for key in ("epochSequence", "snapshotSequence", "burnSequence")
        )
        if not exact:
            self._write_high_water(histories, prior)
        current = self._read_high_water()
        if current is None:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-high-water-not-established"
            )
        return current

    def _assert_current_open(self, histories: Mapping[str, Any]) -> None:
        latest = histories["latestEpoch"]
        if (
            latest is None
            or latest["state"] != "OPEN"
            or latest["facadeEpoch"] != self._facade_epoch
            or latest["ownerEpoch"] != self._owner.epoch
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-current-epoch-not-open"
            )

    def authority_snapshot(
        self,
        *,
        external_anchor_writer: ExternalAnchorWriter | None = None,
    ) -> dict[str, Any]:
        """Return a newly read, signed snapshot; never a cached authority."""

        with kis_route_authority_serialization():
            with self._lock:
                if (
                    self._independent_monotonic_anchor is not None
                    and not callable(external_anchor_writer)
                ):
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-external-anchor-writer-required"
                    )
                source = self._source_bundle()
                self._validate_offline_integrations(source)
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    histories = self._verify_histories(conn)
                    self._verify_high_water(histories, allow_absent=False)
                    self._join_external_anchor(
                        histories, require_exact=True
                    )
                    self._assert_current_open(histories)
                    snapshot = self._insert_snapshot(
                        conn, histories=histories, source=source
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                self._synchronize_high_water()
                if self._independent_monotonic_anchor is not None:
                    histories = self._current_verified_histories()
                    self._seal_external_anchor(
                        histories, writer=external_anchor_writer
                    )
                return self.verify_snapshot(snapshot)

    def verify_snapshot(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """Independently verify signature, journal membership and freshness."""

        if self._manager_receipt_authority is not None:
            self._validate_offline_integrations(self._source_bundle())

        if not isinstance(value, Mapping) or set(value) != _SNAPSHOT_KEYS:
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signed-snapshot-shape-invalid"
            )
        raw = dict(value)
        signature = raw.pop("snapshotSignature")
        digest = raw.pop("snapshotBodyHash")
        if type(digest) is not str or not hmac.compare_digest(digest, _hash(raw)):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signed-snapshot-hash-invalid"
            )
        self._verify_signature("KIS_FUNCTIONAL_APP_SNAPSHOT", raw, signature)
        now, monotonic_ns = self._sample_time()
        observed = _parse_time(raw["observedAt"], "app-facade-verify-observed")
        expires = _parse_time(raw["expiresAt"], "app-facade-verify-expires")
        mono_age = (
            monotonic_ns - int(raw["observedMonotonicNs"])
        ) / 1_000_000_000
        wall_age = (now - observed).total_seconds()
        if (
            raw["facadeEpoch"] != self._facade_epoch
            or raw["snapshotKeyIdHash"] != self._key_id_hash
            or now < observed
            or now >= expires
            or mono_age < 0
            or mono_age >= self._maximum_snapshot_age_seconds
            or abs(wall_age - mono_age) > 2
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-signed-snapshot-stale-or-wrong-epoch"
            )
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            histories = self._verify_histories(conn)
            self._verify_high_water(histories, allow_absent=False)
            self._join_external_anchor(histories, require_exact=True)
            self._assert_current_open(histories)
            sequence = raw["snapshotSequence"]
            if (
                type(sequence) is not int
                or not 1 <= sequence <= histories["snapshotSequence"]
                or histories["snapshotHashes"][sequence - 1] != digest
            ):
                raise KisDomesticFunctionalAppFacadeBlocked(
                    "app-facade-signed-snapshot-not-in-journal"
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {**raw, "snapshotBodyHash": digest, "snapshotSignature": signature}

    @staticmethod
    def _expected_snapshot_join(
        expected: Mapping[str, Any], fresh: Mapping[str, Any]
    ) -> None:
        keys = (
            "facadeEpoch",
            "ownerEpoch",
            "ownerEpochId",
            "ownerEpochHash",
            "stateRevision",
            "functionalAccountFingerprint",
            "credentialConfigurationHash",
            "controlReservationHash",
            "killGrant",
            "functionalMutationIntent",
        )
        if any(expected[key] != fresh[key] for key in keys):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-cleanup-final-cas-changed"
            )

    def burn_cleanup_grant(
        self,
        *,
        expected_snapshot: Mapping[str, Any],
        operation: str,
        intent: Mapping[str, Any],
        external_anchor_writer: ExternalAnchorWriter | None = None,
    ) -> dict[str, Any]:
        """Burn one exact state-owned cleanup/Kill grant before any socket.

        The returned receipt explicitly is *not* transport authority.  A later
        production route must inherit and verify it under its own final
        boundary while holding the same KIS route serialization lock.
        """

        if (
            self._independent_monotonic_anchor is not None
            and not callable(external_anchor_writer)
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-external-anchor-writer-required"
            )
        expected = self.verify_snapshot(expected_snapshot)
        normalized_intent = _normalize_intent(intent, operation)
        if (
            normalized_intent["accountFingerprint"]
            != expected["functionalAccountFingerprint"]
            or normalized_intent["credentialConfigurationHash"]
            != expected["credentialConfigurationHash"]
        ):
            raise KisDomesticFunctionalAppFacadeBlocked(
                "app-facade-cleanup-account-credential-changed"
            )
        with kis_route_authority_serialization():
            with self._lock:
                source = self._source_bundle()
                self._validate_offline_integrations(source)
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    histories = self._verify_histories(conn)
                    self._verify_high_water(histories, allow_absent=False)
                    self._join_external_anchor(
                        histories, require_exact=True
                    )
                    self._assert_current_open(histories)
                    fresh = self._insert_snapshot(
                        conn, histories=histories, source=source
                    )
                    self._expected_snapshot_join(expected, fresh)
                    control = fresh["controlReservation"]
                    kill_revision = 0
                    if operation == "KILL_ORDINARY_CANCEL":
                        grant = fresh["killGrant"]
                        if (
                            not grant
                            or grant["intent"] != normalized_intent
                            or grant["intentHash"] != _hash(normalized_intent)
                        ):
                            raise KisDomesticFunctionalAppFacadeBlocked(
                                "app-facade-state-kill-grant-mismatch"
                            )
                        kill_revision = int(grant["killRevision"])
                        reservation_id = ""
                        reservation_kind = "KILL"
                        reservation_binding_hash = _ZERO_HASH
                    else:
                        if (
                            fresh["functionalPhase"] != "CLEANUP"
                            or not control
                            or control["reservationKind"] not in {"STOP", "KILL"}
                            or fresh["functionalMutationIntent"]
                            != normalized_intent
                        ):
                            raise KisDomesticFunctionalAppFacadeBlocked(
                                "app-facade-state-cleanup-grant-mismatch"
                            )
                        reservation_id = control["reservationId"]
                        reservation_kind = control["reservationKind"]
                        reservation_binding_hash = control[
                            "reservationBindingHash"
                        ]
                    grant_body = {
                        "schemaVersion": "kis-domestic-functional-state-grant/v1",
                        "ownerEpoch": fresh["ownerEpoch"],
                        "ownerEpochId": fresh["ownerEpochId"],
                        "ownerEpochHash": fresh["ownerEpochHash"],
                        "stateRevision": fresh["stateRevision"],
                        "controlReservationHash": fresh[
                            "controlReservationHash"
                        ],
                        "reservationId": reservation_id,
                        "reservationKind": reservation_kind,
                        "reservationBindingHash": reservation_binding_hash,
                        "operation": operation,
                        "intentHash": _hash(normalized_intent),
                        "killRevision": kill_revision,
                    }
                    grant_hash = _hash(grant_body)
                    grant_burn_key = _hash(
                        {
                            "schemaVersion": (
                                "kis-domestic-functional-state-grant-burn-key/v1"
                            ),
                            "grantHash": grant_hash,
                            "ownedOrderKey": normalized_intent["ownedOrderKey"],
                            "claimId": normalized_intent["claimId"],
                        }
                    )
                    if (
                        grant_hash in histories["grantHashes"]
                        or grant_burn_key in histories["grantBurnKeys"]
                    ):
                        raise KisDomesticFunctionalAppFacadeBlocked(
                            "app-facade-cleanup-grant-already-burned"
                        )
                    wall, monotonic_ns = self._sample_time()
                    sequence = int(histories["burnSequence"]) + 1
                    burn_body = {
                        "schemaVersion": BURN_SCHEMA,
                        "route": ROUTE,
                        "pdno": PDNO,
                        "sequenceNo": sequence,
                        "facadeEpoch": self._facade_epoch,
                        "ownerEpoch": fresh["ownerEpoch"],
                        "ownerEpochId": fresh["ownerEpochId"],
                        "ownerEpochHash": fresh["ownerEpochHash"],
                        "stateRevision": fresh["stateRevision"],
                        "controlReservationHash": fresh[
                            "controlReservationHash"
                        ],
                        "reservationId": reservation_id,
                        "reservationKind": reservation_kind,
                        "reservationBindingHash": reservation_binding_hash,
                        "operation": operation,
                        "intent": normalized_intent,
                        "intentHash": _hash(normalized_intent),
                        "grantHash": grant_hash,
                        "grantBurnKey": grant_burn_key,
                        "killRevision": kill_revision,
                        "snapshotSequence": fresh["snapshotSequence"],
                        "snapshotBodyHash": fresh["snapshotBodyHash"],
                        "previousEntryHash": histories["burnHeadHash"],
                        "burnedAt": _time_text(wall),
                        "burnedMonotonicNs": monotonic_ns,
                        "keyIdHash": self._key_id_hash,
                        "productionAvailable": False,
                        "networkOrderPostAllowed": False,
                    }
                    if set(burn_body) != _BURN_BODY_KEYS:
                        raise KisDomesticFunctionalAppFacadeBlocked(
                            "app-facade-cleanup-burn-body-shape-invalid"
                        )
                    signature = self._sign(
                        "KIS_FUNCTIONAL_APP_CLEANUP_BURN", burn_body
                    )
                    record_hash = _hash(burn_body)
                    conn.execute(
                        "INSERT INTO kis_functional_app_facade_cleanup_burn "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            sequence,
                            grant_hash,
                            grant_burn_key,
                            self._facade_epoch,
                            fresh["ownerEpoch"],
                            fresh["stateRevision"],
                            fresh["snapshotSequence"],
                            histories["burnHeadHash"],
                            _canonical(burn_body).decode("utf-8"),
                            record_hash,
                            signature,
                            self._key_id_hash,
                            burn_body["burnedAt"],
                            monotonic_ns,
                        ),
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                high = self._synchronize_high_water()
                if self._independent_monotonic_anchor is not None:
                    histories = self._current_verified_histories()
                    self._seal_external_anchor(
                        histories, writer=external_anchor_writer
                    )
                return {
                    **burn_body,
                    "burnRecordHash": record_hash,
                    "burnSignature": signature,
                    "highWaterRevision": high["revision"],
                    "grantBurnCommitted": True,
                    "socketReachable": False,
                    "tradingMutationCount": 0,
                }

    def status(self) -> dict[str, Any]:
        with kis_route_authority_serialization():
            with self._lock:
                source = self._source_bundle()
                integration_components = self._validate_offline_integrations(
                    source
                )
                conn = self._connect()
                try:
                    conn.execute("BEGIN")
                    histories = self._verify_histories(conn)
                    high = self._verify_high_water(histories, allow_absent=False)
                    anchor_join = self._join_external_anchor(
                        histories, require_exact=False
                    )
                    self._assert_current_open(histories)
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                blockers = [
                    "SHARED_STATE_READER_NOT_REGISTERED",
                    "SHARED_KIS_ROUTE_FENCE_NOT_WIRED",
                    "NETWORK_AND_RELEASE_ENTRYPOINTS_COMPILE_DISABLED",
                ]
                if not integration_components["configured"]:
                    blockers.extend(
                        [
                            "DEDICATED_MANAGER_KEY_PURPOSE_NOT_ACCEPTED",
                            "EXTERNAL_INDEPENDENT_HIGH_WATER_NOT_WIRED",
                        ]
                    )
                else:
                    blockers.extend(
                        [
                            "DEDICATED_MANAGER_AUTHORITY_NOT_WIRED_INTO_STATE_FACTORY",
                            "PRODUCTION_MANAGER_AUTHORITY_MANIFEST_NOT_PROVISIONED",
                            "PRODUCTION_EXTERNAL_ANCHOR_PATH_NOT_PROVISIONED",
                            "EXTERNAL_ANCHOR_MINIMUM_PIN_STORE_NOT_WIRED",
                            "HARDWARE_OR_WORM_MONOTONIC_COUNTER_NOT_WIRED",
                            "EXTERNAL_ANCHOR_WRITER_NOT_WIRED_TO_REGISTERED_READER",
                        ]
                    )
                    if anchor_join["pending"]:
                        blockers.append(
                            "EXTERNAL_ANCHOR_RECONCILIATION_PENDING"
                        )
                manager_status = integration_components["manager"]
                anchor_status = integration_components["anchor"]
                body = {
                    "schemaVersion": STATUS_SCHEMA,
                    "route": ROUTE,
                    "pdno": PDNO,
                    "facadeEpoch": self._facade_epoch,
                    "ownerEpoch": source["owner"]["epoch"],
                    "stateRevision": source["authority"]["stateRevision"],
                    "accountFingerprint": source["authority"][
                        "functionalAccountFingerprint"
                    ],
                    "credentialConfigurationHash": source["authority"][
                        "credentialConfigurationHash"
                    ],
                    "controlReservation": dict(
                        source["authority"]["controlReservation"]
                    ),
                    "killGrant": self._kill_grant(source["authority"]),
                    "epochSequence": histories["epochSequence"],
                    "snapshotSequence": histories["snapshotSequence"],
                    "cleanupBurnSequence": histories["burnSequence"],
                    "highWaterRevision": high["revision"] if high else 0,
                    "singleProcessLifetimeOsLeaseHeld": True,
                    "factoryPinnedStateManagerGraph": True,
                    "stateReceiptV2IntegrationWired": True,
                    "durableSignedFreshReaderImplemented": True,
                    "durableCleanupGrantBurnImplemented": True,
                    "dedicatedManagerKeyPurposeAvailable": bool(
                        integration_components["configured"]
                    ),
                    "independentFacadeMonotonicAnchorAvailable": bool(
                        integration_components["configured"]
                    ),
                    "managerAuthorityFacadeIntegrated": bool(
                        integration_components["configured"]
                    ),
                    "externalAnchorFacadeIntegrated": bool(
                        integration_components["configured"]
                    ),
                    "facadeIntegrationWired": bool(
                        integration_components["configured"]
                    ),
                    "offlineAuthorityAnchorCompositionIntegrated": bool(
                        integration_components["configured"]
                    ),
                    "externalAnchorExactJoin": bool(anchor_join["exact"]),
                    "externalAnchorReconciliationPending": bool(
                        anchor_join["pending"]
                    ),
                    "pairedLedgerAndLocalHighWaterRollbackAccepted": not bool(
                        integration_components["configured"]
                    ),
                    "pairedLedgerAndLocalHighWaterRollbackDetected": bool(
                        integration_components["configured"]
                    ),
                    "managerAuthorityProductionProvisioned": False,
                    "externalAnchorProductionProvisioned": False,
                    "externalAnchorHardwareOrWormMonotonicityProven": False,
                    "externalAnchorWriterRetainedByFacade": False,
                    "managerAuthorityKeyIdHash": (
                        None
                        if manager_status is None
                        else manager_status["managerKeyIdHash"]
                    ),
                    "externalAnchorEpoch": (
                        0
                        if anchor_status is None
                        else anchor_status["anchorEpoch"]
                    ),
                    "externalAnchorHeadHash": (
                        _ZERO_HASH
                        if anchor_status is None
                        else anchor_status["anchorHeadHash"]
                    ),
                    "readerRegistered": False,
                    "readinessBlockers": blockers,
                    "productionAvailable": False,
                    "networkAvailable": False,
                    "mutationAvailable": False,
                    "releaseAvailable": False,
                    "networkOrderPostAllowed": False,
                    "tradingMutationCount": 0,
                }
                return {**body, "statusHash": _hash(body)}

    def close(
        self,
        *,
        external_anchor_writer: ExternalAnchorWriter | None = None,
    ) -> None:
        with kis_route_authority_serialization():
            with self._lock:
                if self._closed:
                    return
                if (
                    self._independent_monotonic_anchor is not None
                    and not callable(external_anchor_writer)
                ):
                    raise KisDomesticFunctionalAppFacadeBlocked(
                        "app-facade-external-anchor-writer-required"
                    )
                source = self._source_bundle()
                self._validate_offline_integrations(source)
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    histories = self._verify_histories(conn)
                    self._verify_high_water(histories, allow_absent=False)
                    self._join_external_anchor(
                        histories, require_exact=True
                    )
                    self._assert_current_open(histories)
                    self._append_epoch(
                        conn,
                        histories=histories,
                        facade_epoch=self._facade_epoch,
                        state="RELEASED",
                        source=source,
                        owner_epoch=self._owner.epoch,
                    )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                self._synchronize_high_water()
                if self._independent_monotonic_anchor is not None:
                    histories = self._current_verified_histories()
                    self._seal_external_anchor(
                        histories, writer=external_anchor_writer
                    )
                self._closed = True

    def __enter__(self) -> "DurableKisDomesticFunctionalAppFacade":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "schemaVersion": STATUS_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "durableSignedFreshReaderImplemented": True,
        "durableSignedFreshReaderRegistered": False,
        "durableCleanupGrantBurnImplemented": True,
        "factoryPinnedStateManagerGraph": True,
        "dedicatedManagerKeyPurposeAvailable": False,
        "independentFacadeMonotonicAnchorAvailable": False,
        "facadeIntegrationWired": False,
        "productionManagerAuthorityProvisioned": False,
        "productionExternalMonotonicAnchorProvisioned": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
        "reason": "ISOLATED_APP_FACADE_ONLY_NOT_REGISTERED_NO_NETWORK_OR_RELEASE",
    }


__all__ = [
    "DurableKisDomesticFunctionalAppFacade",
    "KisDomesticFunctionalAppFacadeBlocked",
    "SCHEMA_FINGERPRINT",
    "SCHEMA_VERSION",
    "derive_facade_anchor_ledger_id",
    "derive_state_owner_epoch_binding",
    "production_entrypoint_status",
]
