from __future__ import annotations

"""Byte-exact verifier for one published Binance BTCUSDT Strategy pair."""

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .binance_spot_continuous_functional import ExactBinding


class BinancePublicationError(ValueError):
    pass


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read_json(path: Path, *, label: str) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise BinancePublicationError(f"{label} file cannot be read") from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BinancePublicationError(f"{label} file is not exact UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise BinancePublicationError(f"{label} JSON must be an object")
    return payload, _sha256_bytes(raw)


def verify_binance_spot_publication(
    binding: ExactBinding,
    *,
    proof_path: str | Path,
) -> dict[str, Any]:
    """Verify proof content hash, exact row, bytes, and declared identities."""

    path = Path(proof_path)
    proof, proof_file_sha = _read_json(path, label="publication proof")
    if proof.get("schemaVersion") != "crypto-dual-5m-functional-publication-proof-v1":
        raise BinancePublicationError("publication proof schema changed")
    if (
        proof.get("status") != "PASS"
        or proof.get("canonicalSavedPayloadVerifier") != "PASS"
        or proof.get("localShadowPaperVerifier") != "PASS"
        or proof.get("boundedFunctionalPermitRequired") is not True
        or proof.get("continuousRuntime") is not True
        or proof.get("naturalSignalsOnly") is not True
        or proof.get("signalForcingUsed") is not False
        or proof.get("promotionEligible") is not False
        or proof.get("useAsPromotionEvidence") is not False
        or proof.get("generalLiveAllowed") is not False
        or int(proof.get("externalBrokerCalls") or 0) != 0
        or int(proof.get("networkOrderCount") or 0) != 0
    ):
        raise BinancePublicationError("publication proof safety attestations changed")
    declared_proof_hash = str(proof.get("proofHash") or "").lower()
    proof_body = dict(proof)
    proof_body.pop("proofHash", None)
    calculated_proof_hash = _sha256_bytes(_canonical_json(proof_body))
    if declared_proof_hash != calculated_proof_hash:
        raise BinancePublicationError("publication proofHash content mismatch")
    if declared_proof_hash != binding.publication_proof_hash:
        raise BinancePublicationError("binding publicationProofHash changed")
    if proof_file_sha != binding.publication_proof_file_sha256:
        raise BinancePublicationError("binding publicationProofFileSha256 changed")
    publications = proof.get("publications")
    if not isinstance(publications, list):
        raise BinancePublicationError("publication rows are missing")
    matches = [
        row
        for row in publications
        if isinstance(row, Mapping)
        and row.get("provider") == "binance"
        and row.get("group") == "crypto-binance"
        and row.get("symbol") == "BTCUSDT"
        and row.get("strategyArtifactId") == binding.strategy_artifact_id
        and row.get("strategyInstanceId") == binding.strategy_instance_id
    ]
    if len(matches) != 1:
        raise BinancePublicationError("exact BTCUSDT Strategy/Instance row is not unique")
    row = dict(matches[0])
    expected_row = {
        "activeCatalogVisible": True,
        "strategyArtifactHash": binding.strategy_artifact_hash,
        "strategyArtifactFileSha256": binding.artifact_file_sha256,
        "strategyInstanceHash": binding.strategy_instance_hash,
        "strategyInstanceFileSha256": binding.instance_file_sha256,
    }
    for field, expected in expected_row.items():
        actual = row.get(field)
        if isinstance(expected, str):
            actual = str(actual or "").lower()
        if actual != expected:
            raise BinancePublicationError(f"publication row changed at {field}")
    artifact_path = Path(str(row.get("strategyArtifactPath") or ""))
    instance_path = Path(str(row.get("strategyInstancePath") or ""))
    artifact, artifact_file_sha = _read_json(
        artifact_path, label="Strategy Artifact"
    )
    instance, instance_file_sha = _read_json(
        instance_path, label="Strategy Instance"
    )
    if artifact_file_sha != binding.artifact_file_sha256:
        raise BinancePublicationError("Strategy Artifact byte SHA changed")
    if instance_file_sha != binding.instance_file_sha256:
        raise BinancePublicationError("Strategy Instance byte SHA changed")
    artifact_exact = {
        "id": binding.strategy_artifact_id,
        "strategy_id": binding.strategy_artifact_id,
        "artifact_schema_version": "strategy-artifact-v2",
        "asset": "CRYPTO",
        "assetGroup": "crypto-binance",
        "instrumentType": "SPOT",
        "symbol": "BTCUSDT",
        "timeframe": "5m",
        "promotionEligible": False,
    }
    for field, expected in artifact_exact.items():
        if artifact.get(field) != expected:
            raise BinancePublicationError(f"Strategy Artifact changed at {field}")
    artifact_lock = artifact.get("artifactLock")
    if not isinstance(artifact_lock, Mapping):
        raise BinancePublicationError("Strategy Artifact lock is missing")
    if str(artifact_lock.get("artifactHash") or "").lower() != binding.strategy_artifact_hash:
        raise BinancePublicationError("Strategy Artifact declared hash changed")
    instance_exact = {
        "instanceId": binding.strategy_instance_id,
        "sourceStrategyId": binding.strategy_artifact_id,
        "sourceArtifactHash": binding.strategy_artifact_hash,
        "qualifiedSymbol": "BTCUSDT",
        "qualifiedTimeframe": "5m",
        "asset": "CRYPTO",
        "assetGroup": "crypto-binance",
        "instrumentType": "SPOT",
        "marketDataProvider": "binance",
        "realtimeMarketDataProvider": "binance",
        "brokerId": "binance",
        "marketGroup": "CRYPTO_SPOT",
        "executionRoute": "BINANCE_SPOT_CONTINUOUS",
        "exchange": "BINANCE_SPOT",
        "settlementCurrency": "USDT",
        "continuousRuntime": True,
        "immutable": True,
        "artifactHash": binding.strategy_instance_hash,
    }
    for field, expected in instance_exact.items():
        if instance.get(field) != expected:
            raise BinancePublicationError(f"Strategy Instance changed at {field}")
    qualification = instance.get("qualification")
    runtime_contract = instance.get("runtimeMarketDataContract")
    if not isinstance(qualification, Mapping) or not isinstance(runtime_contract, Mapping):
        raise BinancePublicationError("Instance qualification/runtime contract is missing")
    if (
        qualification.get("naturalSignalsOnly") is not True
        or qualification.get("promotionEligible") is not False
        or qualification.get("evidenceClass") != "FUNCTIONAL_TEST_NON_PROMOTION"
        or runtime_contract.get("realtimeProvider") != "binance"
        or runtime_contract.get("closedBarRequired") is not True
        or runtime_contract.get("openBoundaryAttestationRequired")
        != "BINANCE_WEBSOCKET"
    ):
        raise BinancePublicationError("Instance safety qualification changed")
    for document, label in ((artifact, "Artifact"), (instance, "Instance")):
        if document.get("marketGroup") not in {None, "CRYPTO_SPOT"}:
            raise BinancePublicationError(f"{label} market group changed")
        if document.get("executionRoute") not in {None, "BINANCE_SPOT_CONTINUOUS"}:
            raise BinancePublicationError(f"{label} execution route changed")
    return {
        "complete": True,
        "strategyArtifactHash": binding.strategy_artifact_hash,
        "artifactFileSha256": artifact_file_sha,
        "strategyInstanceHash": binding.strategy_instance_hash,
        "instanceFileSha256": instance_file_sha,
        "publicationProofHash": calculated_proof_hash,
        "publicationProofFileSha256": proof_file_sha,
        "proofPath": str(path.resolve()),
        "artifactPath": str(artifact_path.resolve()),
        "instancePath": str(instance_path.resolve()),
    }


def load_binance_spot_publication_binding(
    *,
    proof_path: str | Path,
    account_fingerprint: str,
) -> ExactBinding:
    """Resolve the one published BTCUSDT pair without client selection.

    The proof path is a server configuration input.  Artifact/instance ids and
    every declared/file hash come from its unique active BTCUSDT publication
    row, then the full byte-exact verifier re-opens all three documents before
    this binding is returned.
    """

    path = Path(proof_path)
    proof, proof_file_sha = _read_json(path, label="publication proof")
    body = dict(proof)
    declared_proof_hash = str(body.pop("proofHash", "")).lower()
    if declared_proof_hash != _sha256_bytes(_canonical_json(body)):
        raise BinancePublicationError("publication proofHash content mismatch")
    publications = proof.get("publications")
    if not isinstance(publications, list):
        raise BinancePublicationError("publication rows are missing")
    rows = [
        dict(row)
        for row in publications
        if isinstance(row, Mapping)
        and row.get("provider") == "binance"
        and row.get("group") == "crypto-binance"
        and row.get("symbol") == "BTCUSDT"
        and row.get("activeCatalogVisible") is True
    ]
    if len(rows) != 1:
        raise BinancePublicationError(
            "server publication requires one active BTCUSDT Strategy pair"
        )
    row = rows[0]
    binding = ExactBinding.parse(
        {
            "strategyArtifactId": row.get("strategyArtifactId"),
            "strategyArtifactHash": row.get("strategyArtifactHash"),
            "artifactFileSha256": row.get("strategyArtifactFileSha256"),
            "strategyInstanceId": row.get("strategyInstanceId"),
            "strategyInstanceHash": row.get("strategyInstanceHash"),
            "instanceFileSha256": row.get("strategyInstanceFileSha256"),
            "publicationProofHash": declared_proof_hash,
            "publicationProofFileSha256": proof_file_sha,
            "accountFingerprint": str(account_fingerprint or "").lower(),
            "broker": "BINANCE",
            "venue": "BINANCE_SPOT",
            "asset": "CRYPTO",
            "market": "CRYPTO_SPOT",
            "executionRoute": "BINANCE_SPOT_CONTINUOUS",
            "symbol": "BTCUSDT",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "interval": "5m",
        }
    )
    verify_binance_spot_publication(binding, proof_path=path)
    return binding


__all__ = [
    "BinancePublicationError",
    "load_binance_spot_publication_binding",
    "verify_binance_spot_publication",
]
