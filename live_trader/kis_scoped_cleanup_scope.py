"""Read the original append-only owned scope solely for STOP reconciliation.

Expired entry/fee policy never opens a new order here. This read-only adapter
allows canceling an existing exact owned remainder after that entry policy has
expired. Current state authority still checks owner, credentials and receipts.
"""
from __future__ import annotations

from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace

from trading_runtime.artifact_governance import stable_sha256


def original_owned_cleanup_scope(native_runtime, context):
    path = Path(native_runtime.store.path)
    if not path.is_file():
        raise ValueError("KIS_CLEANUP_ORIGINAL_OWNED_PRINCIPAL_REQUIRED")
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        row = connection.execute("SELECT payload,content_hash FROM principals WHERE deployment_id=?", (context["deploymentId"],)).fetchone()
    if row is None:
        raise ValueError("KIS_CLEANUP_ORIGINAL_OWNED_PRINCIPAL_REQUIRED")
    body = json.loads(row[0]); scope = body.get("scope") if isinstance(body, dict) else None
    if (not isinstance(scope, dict) or stable_sha256(body) != row[1]
        or scope.get("version") != "kis-owned-capital-v2"
        or scope.get("scopeHash") != stable_sha256({key:value for key,value in scope.items() if key != "scopeHash"})
        or any(scope.get(key) != context[context_key] for key,context_key in (
            ("deploymentId","deploymentId"),("accountFingerprint","accountFingerprint"),
            ("credentialConfigurationHash","credentialConfigurationHash"),("quoteCurrency","capitalCurrency")))):
        raise ValueError("KIS_CLEANUP_ORIGINAL_PRINCIPAL_SCOPE_SEAL_CHANGED")
    members = sorted([{"strategyInstanceId":spec.strategy_instance_id,"sourceArtifactHash":spec.artifact_hash,
        "portfolioHash":spec.portfolio_hash,"symbol":spec.symbol,"quoteCurrency":scope["quoteCurrency"]}
        for spec in native_runtime.specs], key=lambda item:item["strategyInstanceId"])
    if not members or len(members) > 32 or scope.get("members") != members:
        raise ValueError("KIS_CLEANUP_ORIGINAL_CANONICAL_MEMBERS_CHANGED")
    return deepcopy(scope)


def cleanup_reconciliation_strategy(native_runtime, context):
    """Carry the original owned members' explicit source venue into status checks.

    Currency, a flat account and an order symbol do not establish a venue.
    Original principal/member pins and sealed source quantity contracts do.
    This adapter neither restores entry authority nor creates a new baseline.
    """
    from .native_spot_quantity import sizing_from_artifact

    scope = original_owned_cleanup_scope(native_runtime, context)
    contracts = []
    for spec in native_runtime.specs:
        sizing = sizing_from_artifact(getattr(spec, "artifact", None))
        contract = (sizing or {}).get("quantityContract") or {}
        if (contract.get("schemaVersion") != "native-equity-quantity-v1"
                or contract.get("provider") != "kis"
                or contract.get("symbol") != spec.symbol
                or contract.get("quoteCurrency") != scope["quoteCurrency"]
                or contract.get("exchange") not in {"KRX", "NASD", "NYSE", "AMEX"}):
            raise ValueError("KIS_CLEANUP_ORIGINAL_SOURCE_VENUE_CONTRACT_REQUIRED")
        if (contract["exchange"] == "KRX") != (contract["quoteCurrency"] == "KRW"):
            raise ValueError("KIS_CLEANUP_ORIGINAL_SOURCE_VENUE_CURRENCY_CONFLICT")
        contracts.append({"sourceArtifactHash": spec.artifact_hash, "strategyInstanceId": spec.strategy_instance_id,
                          "quantityContractHash": contract["contractHash"], "exchange": contract["exchange"]})
    venues = sorted({row["exchange"] for row in contracts})
    if not venues:
        raise ValueError("KIS_CLEANUP_ORIGINAL_SOURCE_VENUE_CONTRACT_REQUIRED")
    return {"broker_id": "kis", "exchange": venues[0] if len(venues) == 1 else "MIXED",
            "currency": scope["quoteCurrency"], "cleanup_source_scope_hash": scope["scopeHash"],
            "cleanup_source_venue_contracts": contracts,
            "observation_scope_source": "ORIGINAL_OWNED_PRINCIPAL_AND_SEALED_SOURCE_QUANTITY_CONTRACTS"}


def cleanup_read_runtime(native_runtime, context):
    """Narrow observer/scope facade: has no submit, cancel or initialize method."""
    def scope():
        # Re-read each time so a changed on-disk original cannot be hidden by
        # a mutable cached dictionary between the two official observations.
        return original_owned_cleanup_scope(native_runtime, context)
    scope()
    return SimpleNamespace(scope=scope, observer=native_runtime.observer)


def cached_cleanup_terminal_is_complete(cached):
    """Supplement the legacy cache's missing zero-gross/integer guarantees.

    Legacy zero-fill cache entries do not preserve official gross notional, so
    they need a fresh exact terminal GET; absence of a receipt is not proof of
    zero gross. Positive fills already passed the original positive-notional
    receipt validator. Partial domestic cancellation also needs whole counts.
    """
    if not isinstance(cached, dict):
        return False
    try:
        proof = cached["proof"]; facts = proof["terminalFacts"]
        filled = Decimal(str(proof["executedQuantity"])); ordered = Decimal(str(facts["orderedQuantity"]))
        if not filled.is_finite() or not ordered.is_finite() or not 0 < filled <= ordered or filled != filled.to_integral_value():
            return False
        if filled < ordered and cached["identity"]["exchange"] == "KRX":
            values = [Decimal(str(facts[name])) for name in ("canceledQuantity", "rejectedQuantity")]
            if any(not value.is_finite() or value < 0 or value != value.to_integral_value() for value in values):
                return False
            if filled + sum(values) != ordered:
                return False
        return bool(cached["receipts"])
    except (KeyError, ValueError, TypeError, InvalidOperation):
        return False
