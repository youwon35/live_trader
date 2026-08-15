from __future__ import annotations

"""Non-enabling contract for a supervised, non-promotion first-live lane.

This is deliberately distinct from formal external WORM authority.  It
documents and validates the lower-assurance controls but cannot grant network
or coordinator activation while both release constants remain false.
"""

import hashlib
import json
import math
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Mapping


SUPERVISED_NON_PROMOTION_RELEASED = False
SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED = False
SCHEMA_VERSION = "crypto-first-live-supervised-non-promotion/v1"
APPROVAL_RECEIPT_SCHEMA_VERSION = (
    "crypto-first-live-supervised-user-approval-receipt/v1"
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


class CryptoFirstLiveSupervisedReleaseError(RuntimeError):
    pass


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _text(value: object) -> str:
    return str(value or "").strip()


def _exact_id(value: object, label: str) -> str:
    result = _text(value)
    if _ID_RE.fullmatch(result) is None:
        raise CryptoFirstLiveSupervisedReleaseError(f"{label}-invalid")
    return result


def _exact_hash(value: object, label: str) -> str:
    result = _text(value).lower()
    if _HASH_RE.fullmatch(result) is None:
        raise CryptoFirstLiveSupervisedReleaseError(f"{label}-invalid")
    return result


def validate_supervised_non_promotion_contract(
    value: Mapping[str, Any],
    *,
    clock: Callable[[], float] = time.time,
) -> dict[str, Any]:
    contract = dict(value)
    if set(contract) != {
        "schemaVersion",
        "mode",
        "lane",
        "sessionId",
        "permitId",
        "permitHash",
        "operatorApproval",
        "riskCaps",
        "executionConstraints",
        "auditAnchor",
        "residualRisk",
        "contractHash",
    }:
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-contract-fields-not-exact"
        )
    body = {key: item for key, item in contract.items() if key != "contractHash"}
    lane = _text(contract.get("lane")).upper()
    if (
        contract.get("schemaVersion") != SCHEMA_VERSION
        or contract.get("mode") != "SUPERVISED_NON_PROMOTION"
        or lane not in {"UPBIT", "BINANCE_SPOT"}
        or not secrets.compare_digest(
            _exact_hash(contract.get("contractHash"), "contract-hash"),
            _digest(body),
        )
    ):
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-contract-identity-invalid"
        )
    _exact_id(contract.get("sessionId"), "session-id")
    _exact_id(contract.get("permitId"), "permit-id")
    _exact_hash(contract.get("permitHash"), "permit-hash")

    approval = contract.get("operatorApproval")
    if not isinstance(approval, Mapping) or set(approval) != {
        "schemaVersion",
        "approvalId",
        "approvalBindingHash",
        "consumptionId",
        "exactUserApproval",
        "consumed",
        "oneUse",
        "durable",
        "restartVerifiable",
        "approvedEpoch",
        "receiptHash",
    }:
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-operator-approval-invalid"
        )
    _exact_id(approval.get("approvalId"), "approval-id")
    _exact_hash(approval.get("approvalBindingHash"), "approval-binding-hash")
    _exact_id(approval.get("consumptionId"), "approval-consumption-id")
    approval_body = {
        key: item for key, item in approval.items() if key != "receiptHash"
    }
    try:
        approved_epoch = float(approval.get("approvedEpoch"))
        now = float(clock())
    except (TypeError, ValueError) as exc:
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-operator-approval-time-invalid"
        ) from exc
    if (
        approval.get("schemaVersion") != APPROVAL_RECEIPT_SCHEMA_VERSION
        or not secrets.compare_digest(
            _exact_hash(approval.get("receiptHash"), "approval-receipt-hash"),
            _digest(approval_body),
        )
        or any(
            approval.get(field) is not True
            for field in (
                "exactUserApproval",
                "consumed",
                "oneUse",
                "durable",
                "restartVerifiable",
            )
        )
        or not math.isfinite(approved_epoch)
        or not math.isfinite(now)
        or approved_epoch > now
        or now - approved_epoch > 60
    ):
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-operator-approval-invalid"
        )

    caps = contract.get("riskCaps")
    expected_caps = (
        {
            "currency": "KRW",
            "maxOrderNotional": "10000",
            "maxLoss": "1000",
            "activeSeconds": 7200,
        }
        if lane == "UPBIT"
        else {
            "currency": "USDT",
            "maxOrderNotional": "10",
            "maxLoss": "1",
            "activeSeconds": 7200,
        }
    )
    if not isinstance(caps, Mapping) or dict(caps) != expected_caps:
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-risk-caps-not-exact"
        )

    constraints = contract.get("executionConstraints")
    expected_constraints = {
        "singleLane": True,
        "foregroundMonitoringRequired": True,
        "dualDurableStoresRequired": True,
        "independentAccountOsLeaseRequired": True,
        "oneUseNetworkCapabilityOnly": True,
        "promotionEligible": False,
        "realE2EEligible": False,
        "productionPromotionAllowed": False,
    }
    if (
        not isinstance(constraints, Mapping)
        or dict(constraints) != expected_constraints
    ):
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-execution-constraints-not-exact"
        )

    anchor = contract.get("auditAnchor")
    if not isinstance(anchor, Mapping) or set(anchor) != {
        "schemaVersion",
        "kind",
        "authorityId",
        "checkpointId",
        "receiptHash",
        "signatureVerified",
        "appendOnlyObserved",
        "durable",
        "restartVerifiable",
        "formalWorm",
    }:
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-audit-anchor-invalid"
        )
    if (
        anchor.get("schemaVersion")
        != "crypto-first-live-supervised-audit-anchor/v1"
        or anchor.get("kind")
        not in {
            "WINDOWS_EVENT_LOG_SIGNED",
            "REMOTE_FAST_FORWARD_GIT_SIGNED",
        }
        or any(
            anchor.get(field) is not True
            for field in (
                "signatureVerified",
                "appendOnlyObserved",
                "durable",
                "restartVerifiable",
            )
        )
        or anchor.get("formalWorm") is not False
    ):
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-audit-anchor-invalid"
        )
    _exact_id(anchor.get("authorityId"), "audit-authority-id")
    _exact_id(anchor.get("checkpointId"), "audit-checkpoint-id")
    _exact_hash(anchor.get("receiptHash"), "audit-receipt-hash")

    residual = contract.get("residualRisk")
    expected_residual = {
        "formalWormAbsent": True,
        "sameHostAdministratorCanClearOrRewriteAudit": (
            anchor.get("kind") == "WINDOWS_EVENT_LOG_SIGNED"
        ),
        "acceptedByUser": True,
        "nonPromotionOnly": True,
    }
    if not isinstance(residual, Mapping) or dict(residual) != expected_residual:
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-residual-risk-not-exact"
        )
    return contract


def _validate_issue_request(value: Mapping[str, Any]) -> dict[str, Any]:
    request = dict(value)
    if set(request) != {
        "schemaVersion",
        "mode",
        "lane",
        "sessionId",
        "permitId",
        "permitHash",
        "riskCaps",
        "executionConstraints",
        "auditAnchor",
        "residualRisk",
    } or request.get("schemaVersion") != (
        "crypto-first-live-supervised-approval-issue/v1"
    ):
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-approval-issue-request-invalid"
        )
    now = time.time()
    approval_body = {
        "schemaVersion": APPROVAL_RECEIPT_SCHEMA_VERSION,
        "approvalId": "supervised-validation-approval-0001",
        "approvalBindingHash": _digest(request),
        "consumptionId": "supervised-validation-consumption-0001",
        "exactUserApproval": True,
        "consumed": True,
        "oneUse": True,
        "durable": True,
        "restartVerifiable": True,
        "approvedEpoch": now,
    }
    contract_body = {
        "schemaVersion": SCHEMA_VERSION,
        "mode": request["mode"],
        "lane": request["lane"],
        "sessionId": request["sessionId"],
        "permitId": request["permitId"],
        "permitHash": request["permitHash"],
        "operatorApproval": {
            **approval_body,
            "receiptHash": _digest(approval_body),
        },
        "riskCaps": request["riskCaps"],
        "executionConstraints": request["executionConstraints"],
        "auditAnchor": request["auditAnchor"],
        "residualRisk": request["residualRisk"],
    }
    validate_supervised_non_promotion_contract(
        {**contract_body, "contractHash": _digest(contract_body)},
        clock=lambda: now,
    )
    return request


def build_supervised_non_promotion_contract(
    issue_request: Mapping[str, Any],
    approval_receipt: Mapping[str, Any],
    *,
    clock: Callable[[], float] = time.time,
) -> dict[str, Any]:
    """Bind a consumed durable receipt into the exact non-enabling contract."""

    issue = _validate_issue_request(issue_request)
    approval = dict(approval_receipt)
    if (
        approval.get("approvalBindingHash") != _digest(issue)
        or approval.get("schemaVersion") != APPROVAL_RECEIPT_SCHEMA_VERSION
    ):
        raise CryptoFirstLiveSupervisedReleaseError(
            "supervised-approval-receipt-binding-changed"
        )
    body = {
        "schemaVersion": SCHEMA_VERSION,
        "mode": issue["mode"],
        "lane": issue["lane"],
        "sessionId": issue["sessionId"],
        "permitId": issue["permitId"],
        "permitHash": issue["permitHash"],
        "operatorApproval": approval,
        "riskCaps": issue["riskCaps"],
        "executionConstraints": issue["executionConstraints"],
        "auditAnchor": issue["auditAnchor"],
        "residualRisk": issue["residualRisk"],
    }
    return validate_supervised_non_promotion_contract(
        {**body, "contractHash": _digest(body)}, clock=clock
    )


class DurableSupervisedNonPromotionApprovalStore:
    """Durable issue/consume authority for one exact supervised approval."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path)
        self.clock = clock
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _now(self) -> float:
        value = float(self.clock())
        if not math.isfinite(value) or value <= 0:
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-clock-invalid"
            )
        return value

    def _initialize(self) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS supervised_non_promotion_approvals (
                    approval_id TEXT PRIMARY KEY,
                    approval_binding_json TEXT NOT NULL,
                    approval_binding_hash TEXT NOT NULL UNIQUE,
                    lane TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    phrase_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    issued_epoch REAL NOT NULL,
                    expires_epoch REAL NOT NULL,
                    consumption_id TEXT NOT NULL,
                    approved_epoch REAL NOT NULL,
                    receipt_json TEXT NOT NULL,
                    receipt_hash TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS supervised_approvals_no_delete
                BEFORE DELETE ON supervised_non_promotion_approvals
                BEGIN SELECT RAISE(ABORT, 'supervised-approval-delete-forbidden'); END
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS supervised_approvals_immutable
                BEFORE UPDATE ON supervised_non_promotion_approvals
                WHEN NEW.approval_id != OLD.approval_id
                  OR NEW.approval_binding_json != OLD.approval_binding_json
                  OR NEW.approval_binding_hash != OLD.approval_binding_hash
                  OR NEW.lane != OLD.lane OR NEW.session_id != OLD.session_id
                  OR NEW.permit_id != OLD.permit_id
                  OR NEW.permit_hash != OLD.permit_hash
                  OR NEW.phrase_hash != OLD.phrase_hash
                  OR NEW.issued_epoch != OLD.issued_epoch
                  OR NEW.expires_epoch != OLD.expires_epoch
                BEGIN SELECT RAISE(ABORT, 'supervised-approval-binding-immutable'); END
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS supervised_approvals_state_forward_only
                BEFORE UPDATE ON supervised_non_promotion_approvals
                WHEN NOT (
                    OLD.state='ISSUED' AND NEW.state='EXPIRED'
                    AND NEW.consumption_id='' AND NEW.approved_epoch=0
                    AND NEW.receipt_json='' AND NEW.receipt_hash=''
                ) AND NOT (
                    OLD.state='ISSUED' AND NEW.state='CONSUMED'
                    AND NEW.consumption_id!='' AND NEW.approved_epoch>0
                    AND NEW.receipt_json!='' AND NEW.receipt_hash!=''
                )
                BEGIN SELECT RAISE(ABORT, 'supervised-approval-transition-forbidden'); END
                """
            )
            integrity = str(
                conn.execute("PRAGMA integrity_check").fetchone()[0]
            )
            if integrity.lower() != "ok":
                raise CryptoFirstLiveSupervisedReleaseError(
                    "supervised-approval-database-integrity-invalid"
                )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def issue(
        self,
        request: Mapping[str, Any],
        *,
        validity_seconds: float = 300.0,
    ) -> dict[str, Any]:
        binding = _validate_issue_request(request)
        validity = float(validity_seconds)
        if not math.isfinite(validity) or not 30 <= validity <= 300:
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-validity-invalid"
            )
        now = self._now()
        approval_id = "supervised-approval-" + secrets.token_hex(18)
        typed_phrase = "SUPERVISED " + secrets.token_hex(4).upper()
        binding_json = _canonical(binding)
        binding_hash = hashlib.sha256(binding_json.encode("utf-8")).hexdigest()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                """
                SELECT approval_id, expires_epoch
                FROM supervised_non_promotion_approvals
                WHERE state='ISSUED' ORDER BY issued_epoch DESC LIMIT 1
                """
            ).fetchone()
            if active is not None and float(active["expires_epoch"]) > now:
                raise CryptoFirstLiveSupervisedReleaseError(
                    "supervised-approval-already-issued"
                )
            if active is not None:
                conn.execute(
                    """
                    UPDATE supervised_non_promotion_approvals
                    SET state='EXPIRED' WHERE approval_id=? AND state='ISSUED'
                    """,
                    (str(active["approval_id"]),),
                )
            conn.execute(
                """
                INSERT INTO supervised_non_promotion_approvals(
                    approval_id, approval_binding_json, approval_binding_hash,
                    lane, session_id, permit_id, permit_hash, phrase_hash,
                    state, issued_epoch, expires_epoch, consumption_id,
                    approved_epoch, receipt_json, receipt_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    approval_id,
                    binding_json,
                    binding_hash,
                    binding["lane"],
                    binding["sessionId"],
                    binding["permitId"],
                    binding["permitHash"],
                    hashlib.sha256(typed_phrase.encode("utf-8")).hexdigest(),
                    "ISSUED",
                    now,
                    now + validity,
                    "",
                    0.0,
                    "",
                    "",
                ),
            )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return {
            "schemaVersion": (
                "crypto-first-live-supervised-approval-candidate/v1"
            ),
            "approvalId": approval_id,
            "lane": binding["lane"],
            "sessionId": binding["sessionId"],
            "permitId": binding["permitId"],
            "permitHash": binding["permitHash"],
            "approvalBindingHash": binding_hash,
            "typedPhrase": typed_phrase,
            "issuedEpoch": now,
            "expiresEpoch": now + validity,
            "oneUse": True,
            "durable": True,
            "restartVerifiable": True,
            "networkCapabilityOpen": False,
        }

    def consume(self, request: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(request)
        if set(value) != {
            "schemaVersion",
            "approvalId",
            "approvalBindingHash",
            "typedPhrase",
            "exactUserApproval",
        } or value.get("schemaVersion") != (
            "crypto-first-live-supervised-approval-consume/v1"
        ):
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-consume-request-invalid"
            )
        approval_id = _exact_id(value.get("approvalId"), "approval-id")
        binding_hash = _exact_hash(
            value.get("approvalBindingHash"), "approval-binding-hash"
        )
        typed_phrase = _text(value.get("typedPhrase"))
        if value.get("exactUserApproval") is not True or not typed_phrase:
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-exact-user-approval-required"
            )
        now = self._now()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM supervised_non_promotion_approvals
                WHERE approval_id=?
                """,
                (approval_id,),
            ).fetchone()
            if (
                row is None
                or str(row["state"]) != "ISSUED"
                or str(row["approval_binding_hash"]) != binding_hash
                or now >= float(row["expires_epoch"])
                or not secrets.compare_digest(
                    str(row["phrase_hash"]),
                    hashlib.sha256(typed_phrase.encode("utf-8")).hexdigest(),
                )
            ):
                raise CryptoFirstLiveSupervisedReleaseError(
                    "supervised-approval-not-consumable"
                )
            consumption_id = (
                "supervised-consumption-" + secrets.token_hex(18)
            )
            receipt_body = {
                "schemaVersion": APPROVAL_RECEIPT_SCHEMA_VERSION,
                "approvalId": approval_id,
                "approvalBindingHash": binding_hash,
                "consumptionId": consumption_id,
                "exactUserApproval": True,
                "consumed": True,
                "oneUse": True,
                "durable": True,
                "restartVerifiable": True,
                "approvedEpoch": now,
            }
            receipt = {
                **receipt_body,
                "receiptHash": _digest(receipt_body),
            }
            receipt_json = _canonical(receipt)
            updated = conn.execute(
                """
                UPDATE supervised_non_promotion_approvals
                SET state='CONSUMED', consumption_id=?, approved_epoch=?,
                    receipt_json=?, receipt_hash=?
                WHERE approval_id=? AND state='ISSUED'
                  AND approval_binding_hash=? AND expires_epoch>?
                """,
                (
                    consumption_id,
                    now,
                    receipt_json,
                    receipt["receiptHash"],
                    approval_id,
                    binding_hash,
                    now,
                ),
            ).rowcount
            if updated != 1:
                raise CryptoFirstLiveSupervisedReleaseError(
                    "supervised-approval-consumption-cas-changed"
                )
            conn.execute("COMMIT")
            return receipt
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def status(self, approval_id: str = "") -> dict[str, Any]:
        identifier = _text(approval_id)
        conn = self._connect()
        try:
            row = (
                conn.execute(
                    """
                    SELECT * FROM supervised_non_promotion_approvals
                    WHERE approval_id=?
                    """,
                    (identifier,),
                ).fetchone()
                if identifier
                else conn.execute(
                    """
                    SELECT * FROM supervised_non_promotion_approvals
                    ORDER BY issued_epoch DESC LIMIT 1
                    """
                ).fetchone()
            )
            if row is None:
                return {"state": "EMPTY", "networkCapabilityOpen": False}
            self._validate_durable_row(row)
            state = str(row["state"])
            effective_state = (
                "EXPIRED"
                if state == "ISSUED"
                and self._now() >= float(row["expires_epoch"])
                else state
            )
            return {
                "approvalId": str(row["approval_id"]),
                "approvalBindingHash": str(row["approval_binding_hash"]),
                "lane": str(row["lane"]),
                "sessionId": str(row["session_id"]),
                "permitId": str(row["permit_id"]),
                "permitHash": str(row["permit_hash"]),
                "state": effective_state,
                "durableState": state,
                "issuedEpoch": float(row["issued_epoch"]),
                "expiresEpoch": float(row["expires_epoch"]),
                "consumptionId": str(row["consumption_id"]),
                "approvedEpoch": float(row["approved_epoch"]),
                "receiptHash": str(row["receipt_hash"]),
                "durable": True,
                "restartVerifiable": True,
                "networkCapabilityOpen": False,
            }
        finally:
            conn.close()

    @staticmethod
    def _validate_durable_row(row: sqlite3.Row) -> None:
        try:
            binding = json.loads(str(row["approval_binding_json"]))
        except json.JSONDecodeError as exc:
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-binding-json-invalid"
            ) from exc
        if (
            not isinstance(binding, Mapping)
            or _digest(binding) != str(row["approval_binding_hash"])
            or binding.get("lane") != str(row["lane"])
            or binding.get("sessionId") != str(row["session_id"])
            or binding.get("permitId") != str(row["permit_id"])
            or binding.get("permitHash") != str(row["permit_hash"])
            or _HASH_RE.fullmatch(str(row["phrase_hash"])) is None
            or str(row["state"]) not in {"ISSUED", "CONSUMED", "EXPIRED"}
        ):
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-durable-binding-invalid"
            )
        state = str(row["state"])
        if state in {"ISSUED", "EXPIRED"}:
            if (
                str(row["consumption_id"])
                or float(row["approved_epoch"]) != 0
                or str(row["receipt_json"])
                or str(row["receipt_hash"])
            ):
                raise CryptoFirstLiveSupervisedReleaseError(
                    "supervised-approval-unconsumed-row-invalid"
                )
            return
        try:
            receipt = json.loads(str(row["receipt_json"]))
        except json.JSONDecodeError as exc:
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-receipt-json-invalid"
            ) from exc
        if not isinstance(receipt, dict):
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-receipt-invalid"
            )
        body = {
            key: item for key, item in receipt.items() if key != "receiptHash"
        }
        if (
            set(receipt)
            != {
                "schemaVersion",
                "approvalId",
                "approvalBindingHash",
                "consumptionId",
                "exactUserApproval",
                "consumed",
                "oneUse",
                "durable",
                "restartVerifiable",
                "approvedEpoch",
                "receiptHash",
            }
            or receipt.get("schemaVersion")
            != APPROVAL_RECEIPT_SCHEMA_VERSION
            or receipt.get("approvalId") != str(row["approval_id"])
            or receipt.get("approvalBindingHash")
            != str(row["approval_binding_hash"])
            or receipt.get("consumptionId")
            != str(row["consumption_id"])
            or float(receipt.get("approvedEpoch") or 0)
            != float(row["approved_epoch"])
            or receipt.get("exactUserApproval") is not True
            or receipt.get("consumed") is not True
            or receipt.get("oneUse") is not True
            or receipt.get("durable") is not True
            or receipt.get("restartVerifiable") is not True
            or receipt.get("receiptHash") != _digest(body)
            or receipt.get("receiptHash") != str(row["receipt_hash"])
        ):
            raise CryptoFirstLiveSupervisedReleaseError(
                "supervised-approval-receipt-invalid"
            )


def supervised_non_promotion_release_status() -> dict[str, Any]:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "released": SUPERVISED_NON_PROMOTION_RELEASED,
        "oneUseNetworkCapabilityReleased": (
            SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED
        ),
        "formalExternalWorm": False,
        "promotionEligible": False,
        "realE2EEligible": False,
        "productionPromotionAllowed": False,
        "residualRisk": {
            "formalWormAbsent": True,
            "requiresExactUserAcceptance": True,
            "sameHostWindowsEventLogCanBeClearedByAdministrator": True,
        },
    }


__all__ = [
    "APPROVAL_RECEIPT_SCHEMA_VERSION",
    "CryptoFirstLiveSupervisedReleaseError",
    "DurableSupervisedNonPromotionApprovalStore",
    "SCHEMA_VERSION",
    "SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED",
    "SUPERVISED_NON_PROMOTION_RELEASED",
    "build_supervised_non_promotion_contract",
    "supervised_non_promotion_release_status",
    "validate_supervised_non_promotion_contract",
]
