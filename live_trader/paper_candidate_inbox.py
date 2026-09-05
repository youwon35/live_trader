"""Read-only, bounded projection of exact published Paper Evidence.

No runtime, account, broker, daemon, migration or Deployment writes are invoked.
Only configured artifact roots are read; caller-provided file paths are absent.
"""
from __future__ import annotations

from collections.abc import Sequence
import json
import os
from pathlib import Path
import stat
from typing import Any

from .contracts import TRADING_SYSTEM_ROOT
from trading_runtime.artifact_paths import artifact_read_roots
from trading_runtime.artifact_governance import (
    DEPLOYMENT_DEFINITION_SCHEMA_VERSION, DEPLOYMENT_REGISTRY_SCHEMA_VERSION,
    artifact_reference, safe_file_token, stable_sha256,
    assert_verified_strategy_instance,
    verify_portfolio_artifact, verify_strategy_artifact,
)
from trading_runtime.paper_live_contract import validate_paper_live_evidence
from trading_runtime.portfolio_runtime import infer_market_route, _assert_instance_execution_parity

SCHEMA = "live-paper-evidence-inbox-v1"
MAX_BYTES = 8 * 1024 * 1024
MAX_FILES = 1000
READ_ONLY_REASON = "현재는 검증 근거 확인만 가능합니다. Live 후보 등록과 최초 제한 실거래 승인 기능은 준비 중입니다."


class CandidateBlocked(ValueError):
    pass


def configured_artifact_roots() -> list[Path]:
    """Same roots as the Live catalog, without its migration write side effect."""
    configured = [
        Path(os.path.expandvars(part.strip())).expanduser()
        for key in ("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", "TRADER_STRATEGY_ARTIFACT_DIR")
        for part in os.environ.get(key, "").split(os.pathsep) if part.strip()
    ]
    roots = configured or [
        *artifact_read_roots(TRADING_SYSTEM_ROOT),
        (Path(os.environ["APPDATA"]) if os.environ.get("APPDATA") else Path.home() / "AppData" / "Roaming") / "trading_programs" / "strategies",
    ]
    return list(dict.fromkeys(path.resolve() for path in roots))


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CandidateBlocked("JSON key가 중복되었습니다.")
        result[key] = value
    return result


def _read(path: Path, root: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise CandidateBlocked("설정된 저장소 밖의 파일은 읽지 않습니다.")
    if not stat.S_ISREG(path.stat().st_mode):
        raise CandidateBlocked("일반 JSON 파일이 필요합니다.")
    with path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise CandidateBlocked("JSON 파일이 읽기 한도를 초과했습니다.")
    value = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=_unique)
    if not isinstance(value, dict):
        raise CandidateBlocked("JSON object가 필요합니다.")
    return value


def _files(folder: Path) -> list[Path]:
    files = sorted(folder.glob("*.json")) if folder.is_dir() else []
    if len(files) > MAX_FILES:
        raise CandidateBlocked("저장소 파일 수가 읽기 한도를 초과했습니다.")
    return files


def _registry(root: Path) -> dict[str, Any]:
    path = root / "deployments" / "deployment-registry.json"
    if not path.exists():
        return {"schemaVersion": DEPLOYMENT_REGISTRY_SCHEMA_VERSION, "entries": {}}
    result = _read(path, root)
    if result.get("schemaVersion") != DEPLOYMENT_REGISTRY_SCHEMA_VERSION or not isinstance(result.get("entries"), dict):
        raise CandidateBlocked("Deployment registry 형식을 확인할 수 없습니다.")
    if any(not isinstance(entry, dict) for entry in result["entries"].values()):
        raise CandidateBlocked("Deployment registry 항목이 손상되었습니다.")
    return result


def _catalog(root: Path, kind: str) -> dict[str, list[dict[str, Any]]]:
    folder = root / "portfolios" if kind == "portfolio" else root
    verify = verify_portfolio_artifact if kind == "portfolio" else verify_strategy_artifact
    result: dict[str, list[dict[str, Any]]] = {}
    for path in _files(folder):
        try:
            payload = _read(path, root)
            check = verify(payload)
            identity = str(payload.get("id") or payload.get("artifactId") or payload.get("strategy_id") or "")
            if check.valid:
                ref = artifact_reference(payload)
                result.setdefault(ref["artifactId"], []).append(payload)
            elif identity:
                result.setdefault(identity, []).append({})
        except (OSError, ValueError, UnicodeError):
            continue
    return result


def _exact_artifact(catalog, artifact_id, artifact_hash):
    options = catalog.get(artifact_id, [])
    if not options or any(not item or artifact_reference(item)["artifactHash"] != artifact_hash for item in options):
        raise CandidateBlocked("현재 Artifact ID/hash가 Evidence와 같지 않거나 중복 버전이 있습니다.")
    return options[0]


def _instance_catalog(root: Path) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for path in _files(root / "strategy-instances"):
        item = _read(path, root)
        instance_id = str(item.get("instanceId") or item.get("strategyInstanceId") or item.get("id") or "")
        result.setdefault(instance_id, []).append(item)
    return result


def _exact_instance(catalog, instance_id, strategy):
    options = catalog.get(instance_id, [])
    if len(options) != 1:
        raise CandidateBlocked("Evidence의 Strategy Instance를 하나로 확인하지 못했습니다.")
    item = options[0]
    instance_hash = assert_verified_strategy_instance(item)
    ref = artifact_reference(strategy)
    if str(item.get("sourceArtifactHash") or item.get("strategyArtifactHash") or "") != ref["artifactHash"] or str(item.get("sourceStrategyId") or item.get("strategyId") or "") != ref["artifactId"]:
        raise CandidateBlocked("현재 Strategy Instance의 원본 ID/hash가 Evidence와 다릅니다.")
    return item, instance_hash


def _standalone_scope_hash(strategy: dict, instance: dict, instance_hash: str) -> str:
    """Wire schema emitted by Paper standalone-strategy-runtime-v1.

    Keep the scalar payload identical to standalone_runtime_scope_identity;
    no import from the Paper desktop installation is needed at runtime.
    """
    ref = artifact_reference(strategy)
    data = strategy.get("dataArtifact") if isinstance(strategy.get("dataArtifact"), dict) else {}
    instance_id = str(instance.get("instanceId") or instance.get("strategyInstanceId") or instance.get("id") or "")
    symbol = str(instance.get("qualifiedSymbol") or instance.get("symbol") or strategy.get("symbol") or "").strip().upper()
    timeframe = str(instance.get("qualifiedTimeframe") or instance.get("timeframe") or strategy.get("timeframe") or "").strip().lower()
    inferred_provider, inferred_broker, instrument_id = infer_market_route(symbol)
    provider = str(instance.get("marketDataProvider") or data.get("provider") or inferred_provider).strip().lower()
    broker = str(instance.get("brokerId") or inferred_broker).strip().lower()
    if not all((instance_id, symbol, timeframe, provider, broker)):
        raise CandidateBlocked("봉인된 standalone 실행 범위가 부족합니다.")
    return stable_sha256({
        "schemaVersion": "standalone-strategy-runtime-v1",
        "portfolioId": f"standalone:{ref['artifactId']}:{instance_id}",
        "strategyId": ref["artifactId"], "strategyInstanceId": instance_id,
        "symbol": instrument_id if provider == "yahoo" else symbol,
        "provider": provider, "inferredProvider": inferred_provider,
        "brokerId": broker, "timeframe": timeframe,
        "artifactHash": ref["artifactHash"], "instanceContentHash": instance_hash,
        "targetWeight": 1.0,
    })


def _check_instance(qualification, strategy, portfolio, catalogs):
    scope = qualification.forward_scope
    if portfolio is None:
        item, instance_hash = _exact_instance(catalogs["instance"], scope.strategy_instance_id, strategy)
        if _standalone_scope_hash(strategy, item, instance_hash) != scope.forward_evidence_portfolio_hash:
            raise CandidateBlocked("현재 Instance의 내용·설정 hash가 봉인 Evidence의 standalone 실행 범위와 다릅니다.")
        return instance_hash
    portfolio_ref = artifact_reference(portfolio)
    if scope.forward_evidence_portfolio_hash != portfolio_ref["artifactHash"]:
        raise CandidateBlocked("Portfolio 실행 범위 hash가 현재 봉인 저장본과 다릅니다.")
    if scope.portfolio_instance_id != str(portfolio.get("id") or portfolio.get("portfolioId") or ""):
        raise CandidateBlocked("Portfolio 실행 Instance ID가 현재 저장본과 다릅니다.")
    children = portfolio.get("strategyInstances")
    if not isinstance(children, list) or not children:
        raise CandidateBlocked("Portfolio의 Strategy Instance 배정이 없습니다.")
    selected_hashes = []
    for child in children:
        if not isinstance(child, dict):
            raise CandidateBlocked("Portfolio의 Strategy Instance 배정이 잘못되었습니다.")
        source_id = str(child.get("sourceStrategyId") or child.get("strategyId") or "")
        source_hash = str(child.get("sourceArtifactHash") or "")
        source = _exact_artifact(catalogs["strategy"], source_id, source_hash)
        template_id = str(child.get("templateInstanceId") or child.get("instanceId") or "")
        instance, instance_hash = _exact_instance(catalogs["instance"], template_id, source)
        if str(child.get("sourceInstanceHash") or "") != instance_hash:
            raise CandidateBlocked("Portfolio sourceInstanceHash가 현재 template Instance와 다릅니다.")
        _assert_instance_execution_parity(embedded=child, standalone=instance, strategy=source)
        if str(child.get("instanceId") or child.get("strategyInstanceId") or "") == scope.strategy_instance_id:
            if source_id != scope.strategy_artifact_id or source_hash != scope.strategy_artifact_hash:
                raise CandidateBlocked("선택 Portfolio Instance의 Strategy ID/hash가 Evidence와 다릅니다.")
            selected_hashes.append(instance_hash)
    if len(selected_hashes) != 1:
        raise CandidateBlocked("Evidence의 정확한 Portfolio Instance 배정을 하나로 확인하지 못했습니다.")
    return selected_hashes[0]


def _deployment(root: Path, registry: dict, strategy: dict, portfolio: dict | None):
    matches = [entry for entry in registry["entries"].values()
               if entry.get("strategyArtifact") == artifact_reference(strategy)
               and entry.get("portfolioArtifact") == (artifact_reference(portfolio) if portfolio else None)
               and entry.get("environment") in ("LIVE", "SMALL_LIVE", "FULL_LIVE")]
    if len(matches) > 1:
        raise CandidateBlocked("같은 저장본의 Live Deployment가 여러 개여서 대상을 확정할 수 없습니다.")
    if not matches:
        return {"deploymentId": "", "revision": 0, "mode": "UNREGISTERED", "lifecycle": "", "definitionHash": ""}
    entry = matches[0]
    deployment_id = str(entry.get("deploymentId") or "")
    definition = _read(root / "deployments" / "definitions" / f"{safe_file_token(deployment_id, 'deployment')}.json", root)
    digest = stable_sha256({key: value for key, value in definition.items() if key != "definitionHash"})
    if definition.get("schemaVersion") != DEPLOYMENT_DEFINITION_SCHEMA_VERSION or definition.get("definitionHash") != digest or entry.get("definitionHash") != digest or definition.get("deploymentId") != deployment_id:
        raise CandidateBlocked("Deployment의 불변 정의와 registry hash가 다릅니다.")
    for field in ("strategyArtifact", "portfolioArtifact", "accountId", "environment", "symbol", "instrumentId", "route"):
        if definition.get(field) != entry.get(field):
            raise CandidateBlocked("Deployment registry 범위가 불변 정의와 다릅니다.")
    if type(entry.get("revision")) is not int or entry["revision"] < 1:
        raise CandidateBlocked("Deployment revision을 확인할 수 없습니다.")
    return {key: entry.get(key) for key in ("deploymentId", "revision", "mode", "lifecycle", "definitionHash")}


def _candidate(root, evidence, registry, catalogs):
    result = validate_paper_live_evidence(evidence)
    if not result.valid or result.qualification is None:
        raise CandidateBlocked("Paper publication-v2 검증 실패: " + ", ".join(result.issues))
    qualification = result.qualification
    scope = qualification.forward_scope
    strategy = _exact_artifact(catalogs["strategy"], scope.strategy_artifact_id, scope.strategy_artifact_hash)
    portfolio = _exact_artifact(catalogs["portfolio"], scope.portfolio_artifact_id, scope.portfolio_artifact_hash) if scope.portfolio_required else None
    instance_hash = _check_instance(qualification, strategy, portfolio, catalogs)
    identity = qualification.snapshot()
    identity.pop("ready", None)
    identity.pop("schemaVersion", None)
    return {
        "evidenceId": identity["evidenceId"], "strategyId": scope.strategy_artifact_id,
        "strategyName": str(strategy.get("name") or scope.strategy_artifact_id),
        "portfolioId": scope.portfolio_artifact_id, "instanceHash": instance_hash,
        "rootKey": stable_sha256(str(root.resolve())), "identity": identity,
        "deployment": _deployment(root, registry, strategy, portfolio),
        "status": "VERIFIED_READ_ONLY", "canImport": False,
        "authorizationGranted": False, "detail": "현재 저장본과 봉인 검증 근거가 일치합니다. 확인 전용입니다.",
    }


def list_paper_candidates(*, roots: Sequence[Path] | None = None) -> dict[str, Any]:
    rows, errors = [], []
    try:
        folders = list(roots) if roots is not None else configured_artifact_roots()
        for root in dict.fromkeys(Path(path).resolve() for path in folders):
            if not root.exists():
                continue
            try:
                registry = _registry(root)
                catalogs = {kind: _catalog(root, kind) for kind in ("strategy", "portfolio")}
                catalogs["instance"] = _instance_catalog(root)
                for path in _files(root / "evidence" / "paper"):
                    try:
                        row = _candidate(root, _read(path, root), registry, catalogs)
                    except (OSError, ValueError, UnicodeError, RuntimeError, TypeError, KeyError) as exc:
                        rows.append({"evidenceId": path.stem, "status": "BLOCKED", "canImport": False, "detail": str(exc)})
                    else:
                        rows.append(row)
                    if len(rows) > MAX_FILES:
                        raise CandidateBlocked("후보 수가 읽기 한도를 초과했습니다.")
                if _registry(root) != registry:
                    raise CandidateBlocked("조회 중 Deployment revision이 변경되었습니다. 새로고침하세요.")
            except (OSError, ValueError, UnicodeError, RuntimeError, TypeError) as exc:
                errors.append(str(exc))
        if errors:
            rows = []
    except (OSError, ValueError, RuntimeError, TypeError) as exc:
        rows, errors = [], [str(exc)]
    return {"ok": not errors, "schemaVersion": SCHEMA, "candidates": rows, "errors": errors,
            "readOnly": True, "canImport": False, "authorizationGranted": False,
            "requiredNextStep": READ_ONLY_REASON}
