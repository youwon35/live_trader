from __future__ import annotations

"""Canonical KRW-BTC Strategy/Instance publication re-reader."""

import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping

from .upbit_continuous_functional import EXECUTION_ROUTE, SYMBOL, UpbitFunctionalBlocked


_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def _text(value: object) -> str:
    return str(value or "").strip()


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _logical_hash(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("proofHash", None)
    return _sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )


def _json_file(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise UpbitFunctionalBlocked(
            f"upbit-publication-{label}-unreadable"
        ) from exc
    if not isinstance(parsed, dict):
        raise UpbitFunctionalBlocked(f"upbit-publication-{label}-not-object")
    return raw, parsed


def load_upbit_functional_selection(
    proof_path: str | Path,
    *,
    account_fingerprint: str,
) -> dict[str, Any]:
    """Re-read and verify proof plus both selected files on every call."""

    fingerprint = _text(account_fingerprint).lower()
    if _HASH_RE.fullmatch(fingerprint) is None:
        raise UpbitFunctionalBlocked("upbit-publication-account-fingerprint-invalid")
    proof_file = Path(proof_path)
    proof_raw, proof = _json_file(proof_file, "proof")
    if (
        _text(proof.get("schemaVersion"))
        != "crypto-dual-5m-functional-publication-proof-v1"
        or _text(proof.get("status")).upper() != "PASS"
        or proof.get("boundedFunctionalPermitRequired") is not True
        or proof.get("continuousRuntime") is not True
        or proof.get("naturalSignalsOnly") is not True
        or proof.get("signalForcingUsed") is not False
        or proof.get("promotionEligible") is not False
        or proof.get("useAsPromotionEvidence") is not False
        or proof.get("generalLiveAllowed") is not False
        or int(proof.get("networkOrderCount") or 0) != 0
    ):
        raise UpbitFunctionalBlocked("upbit-publication-proof-policy-invalid")
    declared_proof_hash = _text(proof.get("proofHash")).lower()
    if (
        _HASH_RE.fullmatch(declared_proof_hash) is None
        or declared_proof_hash != _logical_hash(proof)
    ):
        raise UpbitFunctionalBlocked("upbit-publication-proof-hash-mismatch")
    publications = proof.get("publications")
    if not isinstance(publications, list):
        raise UpbitFunctionalBlocked("upbit-publication-list-invalid")
    matches = [
        dict(row)
        for row in publications
        if isinstance(row, Mapping)
        and _text(row.get("provider")).lower() == "upbit"
        and _text(row.get("group")).lower() == "crypto-upbit"
        and _text(row.get("symbol")).upper() == SYMBOL
    ]
    if len(matches) != 1:
        raise UpbitFunctionalBlocked("upbit-publication-selected-row-not-exact")
    selected = matches[0]
    if selected.get("activeCatalogVisible") is not True:
        raise UpbitFunctionalBlocked("upbit-publication-not-active-catalog")
    artifact_path = Path(_text(selected.get("strategyArtifactPath")))
    instance_path = Path(_text(selected.get("strategyInstancePath")))
    if not artifact_path.is_absolute() or not instance_path.is_absolute():
        raise UpbitFunctionalBlocked("upbit-publication-path-not-absolute")
    artifact_raw, artifact = _json_file(artifact_path, "artifact")
    instance_raw, instance = _json_file(instance_path, "instance")
    artifact_file_hash = _sha256(artifact_raw)
    instance_file_hash = _sha256(instance_raw)
    exact_hashes = {
        "strategyArtifactFileSha256": artifact_file_hash,
        "strategyInstanceFileSha256": instance_file_hash,
    }
    for field, actual in exact_hashes.items():
        if _text(selected.get(field)).lower() != actual:
            raise UpbitFunctionalBlocked(
                f"upbit-publication-{field}-mismatch"
            )
    strategy_id = _text(selected.get("strategyArtifactId"))
    strategy_hash = _text(selected.get("strategyArtifactHash")).lower()
    instance_id = _text(selected.get("strategyInstanceId"))
    instance_hash = _text(selected.get("strategyInstanceHash")).lower()
    artifact_lock = artifact.get("artifactLock")
    artifact_lock_hash = (
        _text(artifact_lock.get("artifactHash")).lower()
        if isinstance(artifact_lock, Mapping)
        else ""
    )
    if (
        _text(artifact.get("id") or artifact.get("strategy_id")) != strategy_id
        or artifact_lock_hash != strategy_hash
        or _text(artifact.get("symbol")).upper() != SYMBOL
        or _text(artifact.get("timeframe")).lower() != "5m"
        or _text(instance.get("instanceId")) != instance_id
        or _text(instance.get("artifactHash")).lower() != instance_hash
        or _text(instance.get("sourceArtifactHash")).lower() != strategy_hash
        or _text(instance.get("qualifiedSymbol")).upper() != SYMBOL
        or _text(instance.get("qualifiedTimeframe")).lower() != "5m"
        or _text(instance.get("executionRoute")) != EXECUTION_ROUTE
        or _text(instance.get("exchange")).upper() != "UPBIT_SPOT"
        or instance.get("continuousRuntime") is not True
        or instance.get("immutable") is not True
    ):
        raise UpbitFunctionalBlocked(
            "upbit-publication-artifact-instance-identity-mismatch"
        )
    qualification = instance.get("qualification")
    artifact_parameters = artifact.get("parameters")
    instance_parameters = instance.get("parameters")
    if (
        not isinstance(qualification, Mapping)
        or qualification.get("naturalSignalsOnly") is not True
        or qualification.get("promotionEligible") is not False
        or _text(qualification.get("evidenceClass"))
        != "FUNCTIONAL_TEST_NON_PROMOTION"
    ):
        raise UpbitFunctionalBlocked(
            "upbit-publication-instance-policy-mismatch"
        )
    if (
        _text(artifact.get("plugin") or artifact.get("pluginId"))
        != "moving_average_cross"
        or _text(instance.get("pluginId")) != "moving_average_cross"
        or not isinstance(artifact_parameters, Mapping)
        or not isinstance(instance_parameters, Mapping)
        or int(artifact_parameters.get("shortMa") or 0) != 3
        or int(artifact_parameters.get("longMa") or 0) != 10
        or int(instance_parameters.get("shortMa") or 0) != 3
        or int(instance_parameters.get("longMa") or 0) != 10
    ):
        raise UpbitFunctionalBlocked(
            "upbit-publication-evaluator-contract-mismatch"
        )
    return {
        "strategyArtifactId": strategy_id,
        "strategyArtifactHash": strategy_hash,
        "strategyArtifactFileSha256": artifact_file_hash,
        "strategyInstanceId": instance_id,
        "strategyInstanceHash": instance_hash,
        "strategyInstanceFileSha256": instance_file_hash,
        "strategyInstanceArtifactHash": strategy_hash,
        "accountFingerprint": fingerprint,
        "executionRoute": EXECUTION_ROUTE,
        "symbol": SYMBOL,
        "interval": "5m",
        "verified": True,
        "publicationProofHash": declared_proof_hash,
        "publicationProofFileSha256": _sha256(proof_raw),
        "publicationProofVerified": True,
        "publishedProvider": "upbit",
        "publishedGroup": "crypto-upbit",
        "publishedSymbol": SYMBOL,
        "publishedStrategyArtifactHash": strategy_hash,
        "publishedStrategyArtifactFileSha256": artifact_file_hash,
        "publishedStrategyInstanceHash": instance_hash,
        "publishedStrategyInstanceFileSha256": instance_file_hash,
        "publishedActiveCatalogVisible": True,
        "publishedNaturalSignalsOnly": True,
        "publishedPromotionEligible": False,
        "strategyArtifactPath": str(artifact_path),
        "strategyInstancePath": str(instance_path),
        "strategyPluginId": "moving_average_cross",
        "strategyShortMa": 3,
        "strategyLongMa": 10,
    }


__all__ = ["load_upbit_functional_selection"]
