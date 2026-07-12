from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _ensure_shared_runtime_path() -> None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "packages" / "trading_runtime"
        if candidate.exists():
            path = str(candidate)
            if path not in sys.path:
                sys.path.insert(0, path)
            return


_ensure_shared_runtime_path()

from trading_runtime.artifact_governance import (  # noqa: E402
    DeploymentStore,
    EvidenceStore,
    artifact_reference,
)


ARTIFACT_SCHEMA_VERSION = "strategy-artifact-v2"
SHARED_STRATEGY_SCHEMA_VERSION = "market-strategy-v1"
TRADER_STRATEGY_CONTRACT_VERSION = "trader-strategy-contract-v2"


PLUGIN_ALIASES = {
    "ma-cross": "moving_average_cross",
    "moving-average-cross": "moving_average_cross",
    "rsi-revert": "rsi_revert",
    "custom-draft": "strategy_builder_custom",
    "strategy-builder-custom": "strategy_builder_custom",
}

PLUGIN_LABELS = {
    "moving_average_cross": "Backtester Moving Average Cross",
    "rsi_revert": "Backtester RSI Revert",
    "breakout": "Backtester Breakout",
    "strategy_builder_custom": "Backtester Strategy Builder Custom",
    "threshold_momentum": "Threshold Momentum",
}

TRADING_SYSTEM_ROOT = Path(__file__).resolve().parents[3]
PRIMARY_STRATEGY_ARTIFACT_DIR = TRADING_SYSTEM_ROOT / "packages" / "strategy-core"
IGNORED_STRATEGY_FILE_NAMES = {
    "package.json",
    "package-lock.json",
    "strategy-registry.json",
    "promotion-log.jsonl",
}
IGNORED_PORTFOLIO_FILE_NAMES = {
    "package.json",
    "package-lock.json",
    "portfolio-registry.json",
}
LIFECYCLE_STAGE_ALIASES = {
    "approved": "backtested",
    "final_tested": "backtested",
    "final-tested": "backtested",
    "candidate": "backtested",
    "shadow": "before-shadow",
    "shadow_candidate": "before-shadow",
    "shadow-candidate": "before-shadow",
    "paper_candidate": "before-shadow",
    "paper-candidate": "before-shadow",
    "paper": "papered",
    "live_candidate": "before-live-small",
    "live-candidate": "before-live-small",
    "live_small": "before-live-small",
    "live-small": "before-live-small",
    "live_canary": "before-live-small",
    "live-canary": "before-live-small",
    "live_active": "live",
    "live-active": "live",
    "production": "live",
}
LIFECYCLE_STAGE_ORDER = {
    "draft": 0,
    "backtested": 10,
    "before-shadow": 20,
    "shadowed": 30,
    "papered": 40,
    "before-live-small": 50,
    "live": 60,
    "paused": 70,
    "retired": 80,
}
LIFECYCLE_LABELS = {
    "draft": "Draft",
    "backtested": "Backtested",
    "before-shadow": "Before Shadow",
    "shadowed": "Shadowed",
    "papered": "Papered",
    "before-live-small": "Before Live-Small",
    "live": "Live",
    "paused": "Paused",
    "retired": "Retired",
}
NON_BLOCKING_CAPABILITY_REASONS = {"live-activation-required"}
ACTIVE_REVALIDATION_STAGES = {
    "backtested",
    "before-shadow",
    "shadowed",
    "papered",
    "before-live-small",
    "live",
}


def _dict_value(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def normalize_lifecycle_status(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("_", "-")
    return LIFECYCLE_STAGE_ALIASES.get(normalized, normalized or "draft")


def lifecycle_rank(value: Any) -> int:
    return LIFECYCLE_STAGE_ORDER.get(normalize_lifecycle_status(value), -1)


def lifecycle_at_least(value: Any, minimum: str) -> bool:
    return lifecycle_rank(value) >= lifecycle_rank(minimum)


def _status_pass(value: Any) -> bool:
    return str(value or "").strip().lower() in {"pass", "passed", "ok", "success"}


def _evidence_passed(evidence: dict[str, Any], key: str) -> bool:
    item = evidence.get(key)
    if not isinstance(item, dict):
        return False
    return item.get("passed") is True or _status_pass(item.get("status"))


def _artifact_capabilities(
    artifact: dict[str, Any],
    permissions: dict[str, Any],
    lifecycle_status: str,
    final_test_status: str,
) -> dict[str, Any]:
    evidence = _dict_value(artifact.get("evidence"))
    fail_reasons = _reason_list(permissions.get("fail_reasons") or artifact.get("fail_reasons"))
    blocking_failures = [reason for reason in fail_reasons if str(reason) not in NON_BLOCKING_CAPABILITY_REASONS]
    final_test = _dict_value(artifact.get("finalTest") or artifact.get("final_test"))
    final_passed = (
        _status_pass(final_test_status)
        or _status_pass(final_test.get("status"))
        or _evidence_passed(evidence, "finalTest")
    )
    paper_verified = (
        permissions.get("paper_trader_verified") is True
        or artifact.get("paper_trader_verified") is True
        or _evidence_passed(evidence, "paper")
        or lifecycle_at_least(lifecycle_status, "papered")
    )
    live_small_verified = _evidence_passed(evidence, "liveSmall") or lifecycle_at_least(lifecycle_status, "live")
    paused_or_retired = lifecycle_status in {"paused", "retired"}
    quality_ok = not blocking_failures
    live_small_eligible = (
        not paused_or_retired
        and final_passed
        and paper_verified
        and quality_ok
        and lifecycle_at_least(lifecycle_status, "before-live-small")
    )
    live_eligible = (
        not paused_or_retired
        and final_passed
        and paper_verified
        and live_small_verified
        and quality_ok
        and lifecycle_at_least(lifecycle_status, "live")
    )
    return {
        "schemaVersion": "strategy-capabilities-v1",
        "lifecycleStatus": lifecycle_status,
        "finalTestPassed": final_passed,
        "paperTraderVerified": paper_verified,
        "liveSmallVerified": live_small_verified,
        "traderExportAllowed": final_passed and quality_ok and lifecycle_at_least(lifecycle_status, "before-shadow"),
        "liveSmallEligible": live_small_eligible,
        "liveEligible": live_eligible,
        "canSubmitOrder": False,
        "failReasons": fail_reasons,
        "blockingFailReasons": blocking_failures,
    }


def _normalize_plugin_id(plugin_id: Any) -> str:
    text = str(plugin_id or "").strip()
    return PLUGIN_ALIASES.get(text, text or "unknown")


def _custom_definition_from_artifact(artifact: dict[str, Any], strategy_contract: dict[str, Any]) -> dict[str, Any]:
    parameters = _dict_value(artifact.get("parameters"))
    settings = _dict_value(artifact.get("settings"))
    for candidate in (
        artifact.get("customStrategyDefinition"),
        artifact.get("custom_strategy_definition"),
        parameters.get("customStrategyDefinition"),
        parameters.get("custom_strategy_definition"),
        settings.get("customStrategyDefinition"),
        settings.get("custom_strategy_definition"),
        strategy_contract.get("customStrategyDefinition"),
        strategy_contract.get("custom_strategy_definition"),
    ):
        if isinstance(candidate, dict):
            return candidate
    return {}


def can_trader_use_artifact(artifact: dict[str, Any]) -> bool:
    permissions = _dict_value(artifact.get("permissions"))
    return permissions.get("trader_export_allowed") is True or artifact.get("trader_export_allowed") is True


def can_live_use_artifact(artifact: dict[str, Any]) -> bool:
    if _dict_value(artifact.get("revalidation")).get("expired") is True or artifact.get("revalidation_expired") is True:
        return False
    capabilities = _dict_value(artifact.get("capabilities"))
    permissions = _dict_value(artifact.get("permissions"))
    return (
        capabilities.get("liveEligible") is True
        or permissions.get("live_eligible") is True
        or permissions.get("live_allowed") is True
        or artifact.get("live_eligible") is True
        or artifact.get("live_allowed") is True
    )


def can_live_small_use_artifact(artifact: dict[str, Any]) -> bool:
    if _dict_value(artifact.get("revalidation")).get("expired") is True or artifact.get("revalidation_expired") is True:
        return False
    capabilities = _dict_value(artifact.get("capabilities"))
    permissions = _dict_value(artifact.get("permissions"))
    return (
        capabilities.get("liveSmallEligible") is True
        or capabilities.get("liveEligible") is True
        or permissions.get("live_small_eligible") is True
        or permissions.get("live_eligible") is True
        or permissions.get("live_allowed") is True
        or artifact.get("live_small_eligible") is True
        or artifact.get("live_eligible") is True
        or artifact.get("live_allowed") is True
    )


def _verification_badge(status: str, label: str, detail: str) -> dict[str, str]:
    return {"status": status, "label": label, "detail": detail}


def _reason_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return fallback


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def normalize_paper_portfolio_evidence(artifact: dict[str, Any]) -> dict[str, Any]:
    external = _dict_value(artifact.get("_external_paper_evidence"))
    if external:
        strategy_ref = _dict_value(external.get("strategyArtifact"))
        portfolio_ref = _dict_value(external.get("portfolioArtifact"))
        filled_count = _safe_int(external.get("filledCount"))
        rejected_count = _safe_int(external.get("rejectedCount"))
        status = str(external.get("status") or "")
        integrity = _dict_value(external.get("integrity"))
        valid = artifact.get("_external_paper_evidence_valid") is True
        ready = valid and str(external.get("result") or "").upper() == "PASS" and status.lower() == "submitted" and filled_count > 0 and rejected_count == 0
        details = _dict_value(external.get("details"))
        legacy_detail = _dict_value(details.get("portfolioEvidence"))
        return {
            "required": external.get("portfolioRequired") is True,
            "ready": ready,
            "portfolioId": str(portfolio_ref.get("artifactId") or ""),
            "portfolioHash": str(portfolio_ref.get("artifactHash") or ""),
            "strategyArtifactId": str(strategy_ref.get("artifactId") or ""),
            "strategyArtifactHash": str(strategy_ref.get("artifactHash") or ""),
            "deploymentId": str(external.get("deploymentId") or ""),
            "evidenceId": str(external.get("evidenceId") or ""),
            "evidenceHash": str(integrity.get("contentHash") or ""),
            "source": "external",
            "legacy": False,
            "submittedAt": str(external.get("endedAt") or external.get("createdAt") or ""),
            "status": status,
            "orderCount": _safe_int(external.get("orderCount")),
            "filledCount": filled_count,
            "rejectedCount": rejected_count,
            "targetWeight": _safe_float(external.get("targetWeight")),
            "detail": str(legacy_detail.get("detail") or details.get("performanceDetail") or "별도 Paper evidence 파일"),
            "validationIssues": list(artifact.get("_external_paper_evidence_issues") or []),
        }
    evidence = _dict_value(artifact.get("paperPortfolioEvidence") or artifact.get("paper_portfolio_evidence"))
    if not evidence:
        return {"required": False, "ready": True, "detail": "Portfolio paper evidence 없음"}
    return {
        "required": evidence.get("required") is True,
        "ready": evidence.get("ready") is True,
        "portfolioId": str(evidence.get("portfolioId") or evidence.get("portfolio_id") or ""),
        "portfolioName": str(evidence.get("portfolioName") or evidence.get("portfolio_name") or ""),
        "submittedAt": str(evidence.get("submittedAt") or evidence.get("submitted_at") or ""),
        "status": str(evidence.get("status") or ""),
        "orderCount": _safe_int(evidence.get("orderCount") or evidence.get("order_count")),
        "filledCount": _safe_int(evidence.get("filledCount") or evidence.get("filled_count")),
        "rejectedCount": _safe_int(evidence.get("rejectedCount") or evidence.get("rejected_count")),
        "targetWeight": _safe_float(evidence.get("targetWeight") or evidence.get("target_weight")),
        "detail": str(evidence.get("detail") or ""),
        "source": "legacy-embedded",
        "legacy": True,
    }


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def strategy_revalidation_status(artifact: dict[str, Any], *, lifecycle_status: str | None = None, now: datetime | None = None) -> dict[str, Any]:
    revalidation = _dict_value(artifact.get("revalidation"))
    status = normalize_lifecycle_status(
        lifecycle_status
        or _dict_value(artifact.get("lifecycle")).get("status")
        or artifact.get("lifecycleStatus")
        or artifact.get("status")
        or _dict_value(artifact.get("promotion")).get("stage")
        or artifact.get("promotionStage")
    )
    required = bool(revalidation.get("required")) if revalidation else False
    if not revalidation and status in ACTIVE_REVALIDATION_STAGES and (artifact.get("validated_until") or artifact.get("validatedUntil")):
        required = True
    validated_until = str(revalidation.get("validatedUntil") or artifact.get("validated_until") or artifact.get("validatedUntil") or "")
    last_revalidated_at = str(
        revalidation.get("lastRevalidatedAt") or artifact.get("last_revalidated_at") or artifact.get("lastRevalidatedAt") or ""
    )
    expires_at = _parse_datetime(validated_until)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    expired = bool(required and (expires_at is None or expires_at <= current))
    detail = (
        f"전략 재검증 기한이 만료되었습니다(validated_until={validated_until or 'missing'}). 신규 진입은 차단하고 기존 포지션 관리 주문만 허용합니다."
        if expired
        else f"전략 재검증 유효기간이 남아 있습니다(validated_until={validated_until or 'not-required'})."
    )
    return {
        "schemaVersion": "strategy-revalidation-status-v1",
        "required": required,
        "expired": expired,
        "status": "expired" if expired else "valid" if required else "not-required",
        "validatedUntil": validated_until,
        "lastRevalidatedAt": last_revalidated_at,
        "reason": str(revalidation.get("reason") or artifact.get("revalidation_reason") or ""),
        "detail": detail,
    }


def _backtester_verification_badge(
    permissions: dict[str, Any],
    final_test_status: str,
) -> dict[str, str]:
    fail_reasons = _reason_list(permissions.get("fail_reasons"))
    if permissions.get("trader_export_allowed") is True:
        return _verification_badge("pass", "Backtester 검증", "trader_export_allowed=true artifact입니다.")
    if fail_reasons:
        return _verification_badge("watch", "Backtester 미검증", "; ".join(str(reason) for reason in fail_reasons))
    if final_test_status.lower() in {"pass", "passed", "ok", "success"}:
        return _verification_badge("pass", "Backtester pass", f"final test 상태가 {final_test_status}입니다.")
    if final_test_status.lower() in {"fail", "failed", "error", "blocked"}:
        return _verification_badge("fail", "Backtester fail", f"final test 상태가 {final_test_status}입니다.")
    return _verification_badge("unknown", "Backtester 정보 없음", "artifact에 Backtester 검증 정보가 없습니다.")


def _paper_verification_badge(
    artifact: dict[str, Any],
    permissions: dict[str, Any],
    lifecycle_status: str,
) -> dict[str, str]:
    normalized_status = normalize_lifecycle_status(lifecycle_status)
    paper_verified = (
        permissions.get("paper_trader_verified") is True
        or artifact.get("paper_trader_verified") is True
        or lifecycle_at_least(normalized_status, "papered")
    )
    if paper_verified:
        return _verification_badge("pass", "Paper 검증", f"lifecycle={lifecycle_status or 'paper'}")
    if normalized_status in {"before-shadow", "shadowed"}:
        return _verification_badge("watch", "Shadow 검증 중", "Paper Trader 승급 전 Shadow 검증 단계입니다.")
    return _verification_badge("wait", "Paper 미검증", "Paper Trader 승인/성과 검증 정보가 아직 없습니다.")


def _live_verification_badge(capabilities: dict[str, Any]) -> dict[str, str]:
    if capabilities.get("liveEligible") is True:
        return _verification_badge("pass", "정식 Live 가능", "lifecycle=live와 검증 evidence를 확인했습니다.")
    if capabilities.get("liveSmallEligible") is True:
        return _verification_badge("watch", "Live-Small 가능", "소액 실거래 준비 단계입니다. doctor 통과 후 제한 운용만 허용합니다.")
    fail_reasons = _reason_list(capabilities.get("blockingFailReasons") or capabilities.get("failReasons"))
    detail = "; ".join(str(reason) for reason in fail_reasons) if fail_reasons else "Live-Small/Live eligibility가 없습니다."
    return _verification_badge("fail", "Live 차단", detail)


def _promotion_snapshot(artifact: dict[str, Any], permissions: dict[str, Any], lifecycle_status: str) -> dict[str, Any]:
    promotion = _dict_value(artifact.get("promotion"))
    release = _dict_value(artifact.get("release"))
    stage = normalize_lifecycle_status(
        lifecycle_status
        or promotion.get("stage")
        or artifact.get("promotionStage")
        or release.get("stage")
        or artifact.get("stage")
        or "unknown"
    )
    return {
        "stage": stage or "unknown",
        "stage_label": str(promotion.get("stageLabel") or _promotion_stage_label(stage)),
        "parameter_summary": str(promotion.get("parameterSummary") or artifact.get("parameterSummary") or ""),
        "promoted_at": str(promotion.get("promotedAt") or artifact.get("updatedAt") or artifact.get("createdAt") or ""),
        "promoted_by": str(promotion.get("promotedBy") or artifact.get("savedBy") or ""),
        "history": promotion.get("history") if isinstance(promotion.get("history"), list) else [],
        "paper_eligible": lifecycle_at_least(stage, "before-shadow") or permissions.get("trader_export_allowed") is True,
        "live_candidate": lifecycle_at_least(stage, "before-live-small") or permissions.get("live_small_eligible") is True or permissions.get("live_allowed") is True,
    }


def _release_snapshot(artifact: dict[str, Any]) -> dict[str, Any]:
    release = _dict_value(artifact.get("release"))
    return {
        "release_id": str(release.get("releaseId") or artifact.get("releaseId") or artifact.get("id") or artifact.get("strategy_id") or ""),
        "version_label": str(release.get("versionLabel") or artifact.get("name") or ""),
        "parameter_hash": str(release.get("parameterHash") or ""),
        "validation_hash": str(release.get("validationHash") or ""),
        "immutable": bool(release.get("immutable", bool(release))),
        "source_app": str(release.get("sourceApp") or artifact.get("savedBy") or ""),
    }


def _promotion_stage_label(stage: str) -> str:
    labels = {
        "draft": "Draft",
        "backtested": "Backtested",
        "before-shadow": "Before Shadow",
        "shadowed": "Shadowed",
        "papered": "Papered",
        "before-live-small": "Before Live-Small",
        "live": "Live",
        "paused": "Paused",
        "retired": "Retired",
        "final_tested": "Final Tested",
        "paper_candidate": "Paper Candidate",
        "live_candidate": "Live Candidate",
        "live_canary": "Live Canary",
        "live_active": "Live Active",
        "approved": "Approved",
        "paper": "Paper",
    }
    return labels.get(str(stage or "").lower(), str(stage or "unknown"))


def _appdata_strategy_artifact_dir() -> Path:
    appdata = Path(os.getenv("APPDATA", Path.home() / "AppData" / "Roaming"))
    return appdata / "trading_programs" / "strategies"


def _env_paths(*keys: str) -> list[Path]:
    paths: list[Path] = []
    for key in keys:
        raw = os.getenv(key, "")
        for part in raw.split(os.pathsep):
            value = part.strip()
            if value:
                paths.append(Path(os.path.expandvars(value)).expanduser())
    return paths


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    output: list[Path] = []
    for path in paths:
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(path)
    return output


def strategy_artifact_dirs() -> list[Path]:
    configured = _env_paths("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", "TRADER_STRATEGY_ARTIFACT_DIR")
    return _dedupe_paths(
        configured
        + [
            PRIMARY_STRATEGY_ARTIFACT_DIR,
            _appdata_strategy_artifact_dir(),
        ]
    )


def strategy_plugin_dirs() -> list[Path]:
    configured = _env_paths("LIVE_TRADER_STRATEGY_PLUGIN_DIR", "TRADER_STRATEGY_PLUGIN_DIR")
    if configured:
        return _dedupe_paths(configured)
    return _dedupe_paths([folder / "plugins" for folder in strategy_artifact_dirs()])


def strategy_plugin_status() -> list[dict[str, Any]]:
    return [
        {
            "folder": str(folder),
            "exists": folder.exists(),
            "count": len([path for path in folder.glob("*.py") if path.is_file() and not path.name.startswith("_")])
            if folder.exists()
            else 0,
        }
        for folder in strategy_plugin_dirs()
    ]


def portfolio_artifact_dirs() -> list[Path]:
    return _dedupe_paths([folder / "portfolios" for folder in strategy_artifact_dirs()])


def load_portfolio_artifacts(limit: int = 16) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for folder in portfolio_artifact_dirs():
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
            if path.name in IGNORED_PORTFOLIO_FILE_NAMES:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("artifactType") != "portfolio" and payload.get("schemaVersion") != "portfolio-artifact-v1":
                continue
            payload["_source_path"] = str(path)
            artifacts.append(normalize_portfolio_artifact(payload))
            if len(artifacts) >= limit:
                return artifacts
    return artifacts


def normalize_portfolio_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    lifecycle = _dict_value(artifact.get("lifecycle"))
    framework = _dict_value(artifact.get("framework"))
    permissions = _dict_value(artifact.get("permissions"))
    strategy_instances = artifact.get("strategyInstances") if isinstance(artifact.get("strategyInstances"), list) else []
    target_portfolio = framework.get("targetPortfolio") if isinstance(framework.get("targetPortfolio"), list) else []
    risk_checks = framework.get("riskChecks") if isinstance(framework.get("riskChecks"), list) else []
    portfolio_policy = _dict_value(artifact.get("portfolioPolicy") or _dict_value(artifact.get("evidence")).get("portfolioPolicy"))
    advanced_operations = _dict_value(artifact.get("advancedOperations") or _dict_value(artifact.get("evidence")).get("advancedOperations"))
    lifecycle_status = normalize_lifecycle_status(
        lifecycle.get("status")
        or artifact.get("lifecycleStatus")
        or artifact.get("status")
        or _dict_value(artifact.get("promotion")).get("stage")
        or artifact.get("promotionStage")
        or "draft"
    )
    return {
        "id": str(artifact.get("id") or artifact.get("portfolio_id") or artifact.get("_source_path") or "portfolio"),
        "name": str(artifact.get("name") or artifact.get("portfolioName") or artifact.get("id") or "Portfolio Artifact"),
        "schema_version": str(artifact.get("schemaVersion") or "portfolio-artifact-v1"),
        "artifact_type": str(artifact.get("artifactType") or "portfolio"),
        "lifecycle_status": lifecycle_status,
        "lifecycle": {
            "status": lifecycle_status,
            "label": str(lifecycle.get("label") or _promotion_stage_label(lifecycle_status)),
            "history": lifecycle.get("history") if isinstance(lifecycle.get("history"), list) else [],
        },
        "permissions": {
            "paper_export_allowed": permissions.get("paper_export_allowed") is True,
            "live_small_allowed": permissions.get("live_small_allowed") is True or permissions.get("live_small_eligible") is True,
            "live_allowed": permissions.get("live_allowed") is True or permissions.get("live_export_allowed") is True,
            "live_export_allowed": permissions.get("live_export_allowed") is True or permissions.get("live_allowed") is True,
            "fail_reasons": _reason_list(permissions.get("fail_reasons") or artifact.get("fail_reasons")),
        },
        "strategy_instances": [item for item in strategy_instances if isinstance(item, dict)],
        "target_portfolio": [item for item in target_portfolio if isinstance(item, dict)],
        "risk_policy": _dict_value(artifact.get("riskPolicy") or artifact.get("risk_policy")),
        "risk_checks": [item for item in risk_checks if isinstance(item, dict)],
        "portfolio_policy": portfolio_policy,
        "portfolio_policy_hash": str(portfolio_policy.get("policyHash") or ""),
        "advanced_operations": advanced_operations,
        "advanced_operations_hash": str(advanced_operations.get("contentHash") or ""),
        "source_path": str(artifact.get("_source_path") or ""),
    }


def load_strategy_artifacts(limit: int = 16) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for folder in strategy_artifact_dirs():
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
            if path.name in IGNORED_STRATEGY_FILE_NAMES:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            try:
                reference = artifact_reference(payload)
            except ValueError:
                reference = {"artifactId": "", "artifactHash": ""}
            deployments = [
                item
                for item in DeploymentStore(folder).list()
                if _dict_value(item.get("strategyArtifact")).get("artifactId") == reference.get("artifactId")
                and _dict_value(item.get("strategyArtifact")).get("artifactHash") == reference.get("artifactHash")
            ]
            deployment = deployments[0] if deployments else {}
            if deployment:
                payload["_deployment"] = deployment
            portfolio_ref = _dict_value(deployment.get("portfolioArtifact"))
            evidence_record = EvidenceStore(folder).latest_for_strategy(
                str(reference.get("artifactId") or ""),
                strategy_artifact_hash=str(reference.get("artifactHash") or ""),
                portfolio_artifact_id=str(portfolio_ref.get("artifactId") or ""),
                portfolio_artifact_hash=str(portfolio_ref.get("artifactHash") or ""),
            )
            if evidence_record is not None:
                payload["_external_paper_evidence"] = evidence_record.payload
                payload["_external_paper_evidence_valid"] = evidence_record.valid
                payload["_external_paper_evidence_issues"] = list(evidence_record.issues)
            payload["_source_path"] = str(path)
            artifacts.append(normalize_strategy_artifact(payload))
            if len(artifacts) >= limit:
                return artifacts
    return artifacts


def normalize_strategy_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    dataset = _dict_value(artifact.get("dataset"))
    data_artifact = _dict_value(artifact.get("dataArtifact") or artifact.get("data_artifact"))
    parameters = _dict_value(artifact.get("parameters"))
    settings = _dict_value(artifact.get("settings"))
    lifecycle = _dict_value(artifact.get("lifecycle"))
    final_test = _dict_value(artifact.get("finalTest") or artifact.get("final_test"))
    strategy_contract = _dict_value(artifact.get("strategy_contract") or artifact.get("strategyContract"))
    trader_contract = _dict_value(artifact.get("trader_contract") or artifact.get("traderContract"))
    deployment = _dict_value(artifact.get("_deployment"))
    custom_definition = _custom_definition_from_artifact(artifact, strategy_contract)
    plugin_id = _normalize_plugin_id(
        artifact.get("plugin")
        or artifact.get("pluginId")
        or custom_definition.get("pluginId")
        or artifact.get("strategyId")
    )
    strategy_id = str(
        artifact.get("strategy_id")
        or artifact.get("strategyId")
        or artifact.get("id")
        or artifact.get("name")
        or "unknown-strategy"
    )
    permissions = dict(artifact.get("permissions") or {})
    permissions.update(_dict_value(deployment.get("permissions")))
    if "live_allowed" not in permissions and "live_allowed" in artifact:
        permissions["live_allowed"] = artifact.get("live_allowed") is True
    if "live_small_eligible" not in permissions and "live_small_eligible" in artifact:
        permissions["live_small_eligible"] = artifact.get("live_small_eligible") is True
    if "live_eligible" not in permissions and "live_eligible" in artifact:
        permissions["live_eligible"] = artifact.get("live_eligible") is True
    if "trader_export_allowed" not in permissions and "trader_export_allowed" in artifact:
        permissions["trader_export_allowed"] = artifact.get("trader_export_allowed") is True
    lifecycle_status = normalize_lifecycle_status(
        deployment.get("lifecycle")
        or lifecycle.get("status")
        or artifact.get("lifecycleStatus")
        or artifact.get("status")
        or artifact.get("stage")
        or _dict_value(artifact.get("promotion")).get("stage")
        or artifact.get("promotionStage")
        or "draft"
    )
    final_test_status = str(artifact.get("finalTestStatus") or final_test.get("status") or artifact.get("test_status") or "")
    revalidation = strategy_revalidation_status(artifact, lifecycle_status=lifecycle_status)
    normalized_permissions = {
        "trader_export_allowed": permissions.get("trader_export_allowed") is True,
        "live_small_eligible": permissions.get("live_small_eligible") is True,
        "live_eligible": permissions.get("live_eligible") is True,
        "live_allowed": permissions.get("live_allowed") is True,
        "paper_trader_verified": permissions.get("paper_trader_verified") is True,
        "fail_reasons": _reason_list(permissions.get("fail_reasons") or artifact.get("fail_reasons")),
    }
    capabilities = _artifact_capabilities(artifact, normalized_permissions, lifecycle_status, final_test_status)
    raw_capabilities = _dict_value(artifact.get("capabilities"))
    if raw_capabilities.get("liveSmallEligible") is True:
        capabilities["liveSmallEligible"] = True
    if raw_capabilities.get("liveEligible") is True:
        capabilities["liveEligible"] = True
        capabilities["liveSmallEligible"] = True
    if revalidation["expired"]:
        capabilities["liveSmallEligible"] = False
        capabilities["liveEligible"] = False
        capabilities["canSubmitOrder"] = False
        capabilities["blockingFailReasons"] = list(dict.fromkeys([*capabilities.get("blockingFailReasons", []), "revalidation-expired"]))
        capabilities["failReasons"] = list(dict.fromkeys([*capabilities.get("failReasons", []), "revalidation-expired"]))
    portfolio_candidate = _dict_value(artifact.get("portfolioCandidate") or artifact.get("portfolio_candidate"))
    candidate_required = bool(portfolio_candidate)
    candidate_blockers = _reason_list(portfolio_candidate.get("blockers"))
    candidate_approved = (portfolio_candidate.get("approved") is True and not candidate_blockers) if candidate_required else True
    if candidate_required and not candidate_approved:
        candidate_reason = "portfolio-candidate-not-approved"
        normalized_permissions["live_small_eligible"] = False
        normalized_permissions["live_eligible"] = False
        normalized_permissions["live_allowed"] = False
        normalized_permissions["fail_reasons"] = list(dict.fromkeys([*normalized_permissions["fail_reasons"], candidate_reason, *candidate_blockers]))
        capabilities["liveSmallEligible"] = False
        capabilities["liveEligible"] = False
        capabilities["canSubmitOrder"] = False
        capabilities["blockingFailReasons"] = list(dict.fromkeys([*capabilities.get("blockingFailReasons", []), candidate_reason, *candidate_blockers]))
        capabilities["failReasons"] = list(dict.fromkeys([*capabilities.get("failReasons", []), candidate_reason, *candidate_blockers]))
    verification = {
        "backtester": _backtester_verification_badge(normalized_permissions, final_test_status),
        "paper_trader": _paper_verification_badge(artifact, normalized_permissions, lifecycle_status),
        "live": _live_verification_badge(capabilities),
    }
    promotion = _promotion_snapshot(artifact, normalized_permissions, lifecycle_status)
    if deployment:
        promotion.update(
            {
                "stage": lifecycle_status,
                "stage_label": _promotion_stage_label(lifecycle_status),
                "source_stage": "deployment-registry",
            }
        )
    release = _release_snapshot(artifact)
    paper_portfolio_evidence = normalize_paper_portfolio_evidence(artifact)

    return {
        "strategy_id": strategy_id,
        "deployment_id": str(deployment.get("deploymentId") or ""),
        "deployment_revision": _safe_int(deployment.get("revision")),
        "deployment_environment": str(deployment.get("environment") or ""),
        "deployment_mode": str(deployment.get("mode") or ""),
        "deployment_source": "deployment-registry" if deployment else "legacy-artifact",
        "name": str(artifact.get("name") or artifact.get("strategyName") or strategy_id),
        "symbol": str(artifact.get("symbol") or dataset.get("symbol") or data_artifact.get("symbol") or artifact.get("ticker") or "UNKNOWN"),
        "asset": str(artifact.get("asset") or dataset.get("assetClass") or data_artifact.get("assetClass") or artifact.get("assetClass") or "unknown"),
        "timeframe": str(artifact.get("timeframe") or dataset.get("interval") or data_artifact.get("interval") or artifact.get("interval") or "-"),
        "plugin": plugin_id,
        "plugin_label": PLUGIN_LABELS.get(plugin_id, plugin_id),
        "plugin_source_dirs": [str(path) for path in strategy_plugin_dirs()],
        "plugin_version": str(artifact.get("plugin_version") or artifact.get("pluginVersion") or strategy_contract.get("strategyVersion") or "-"),
        "strategy_contract_version": str(strategy_contract.get("contractVersion") or artifact.get("strategy_plugin_contract_version") or ""),
        "strategy_engine_version": str(artifact.get("strategy_engine_version") or artifact.get("strategyEngineVersion") or ""),
        "lifecycle_status": lifecycle_status,
        "final_test_status": final_test_status,
        "score": artifact.get("score") or artifact.get("qualityScore") or "-",
        "parameters": parameters,
        "settings": settings,
        "promotion": promotion,
        "release": release,
        "lifecycle": {
            "status": lifecycle_status,
            "label": str(lifecycle.get("label") or _promotion_stage_label(lifecycle_status)),
            "updatedAt": str(lifecycle.get("updatedAt") or artifact.get("updatedAt") or ""),
            "history": lifecycle.get("history") if isinstance(lifecycle.get("history"), list) else [],
            "pausedFrom": str(lifecycle.get("pausedFrom") or artifact.get("pausedFrom") or ""),
        },
        "promotion_stage": promotion["stage"],
        "release_id": release["release_id"],
        "order_quantity": artifact.get("order_quantity") or artifact.get("orderQuantity") or parameters.get("positionSize") or settings.get("positionSize") or 1,
        "reference_price": artifact.get("reference_price") or artifact.get("referencePrice") or artifact.get("last_price") or artifact.get("price") or artifact.get("close_price"),
        "last_price": artifact.get("last_price") or artifact.get("reference_price") or artifact.get("price") or artifact.get("close_price"),
        "test_signal": str(artifact.get("test_signal") or artifact.get("manual_signal") or artifact.get("last_signal") or ""),
        "signals": _dict_value(artifact.get("signals")),
        "permissions": normalized_permissions,
        "capabilities": capabilities,
        "revalidation": revalidation,
        "revalidation_expired": revalidation["expired"],
        "validated_until": revalidation["validatedUntil"],
        "last_revalidated_at": revalidation["lastRevalidatedAt"],
        "live_small_eligible": capabilities["liveSmallEligible"],
        "live_eligible": capabilities["liveEligible"],
        "verification": verification,
        "paper_portfolio_evidence": paper_portfolio_evidence,
        "portfolio_candidate": {
            "required": candidate_required,
            "legacyGrandfathered": not candidate_required,
            "candidateId": str(portfolio_candidate.get("candidateId") or ""),
            "approved": candidate_approved,
            "blockers": candidate_blockers,
            "meaning": str(portfolio_candidate.get("meaning") or ("legacy-artifact-grandfathered" if not candidate_required else "portfolio-candidate-not-live-approval")),
        },
        "strategy_policy": _dict_value(artifact.get("strategyPolicy") or artifact.get("strategy_policy")),
        "backtester_verified": verification["backtester"]["status"] == "pass",
        "paper_trader_verified": verification["paper_trader"]["status"] == "pass",
        "contract_version": str(
            trader_contract.get("contract_version")
            or trader_contract.get("contractVersion")
            or artifact.get("contractVersion")
            or artifact.get("traderStrategyContractVersion")
            or TRADER_STRATEGY_CONTRACT_VERSION
        ),
        "data_mode": str(artifact.get("data_mode") or dataset.get("data_mode") or data_artifact.get("data_mode") or "real"),
        "source_app": str(artifact.get("savedBy") or artifact.get("sourceApp") or ""),
        "source_path": str(artifact.get("_source_path") or ""),
    }


def sample_strategy_artifacts() -> list[dict[str, Any]]:
    return [
        {
            "strategy_id": "KR-MOM-LIVE-01",
            "name": "KR Momentum Approved",
            "symbol": "069500.KS",
            "asset": "kr-stock",
            "timeframe": "1d",
            "lifecycle_status": "before-live-small",
            "final_test_status": "pass",
            "score": 87,
            "permissions": {
                "trader_export_allowed": True,
                "live_small_eligible": True,
                "live_eligible": False,
                "live_allowed": False,
                "fail_reasons": ["실계좌 API 키와 주문 어댑터 구현 전까지 doctor/runtime 게이트가 필요합니다."],
            },
            "contract_version": TRADER_STRATEGY_CONTRACT_VERSION,
            "source_path": "sample",
        },
        {
            "strategy_id": "BTC-BRK-SHADOW-04",
            "name": "BTC Breakout v4",
            "symbol": "BTCUSDT",
            "asset": "crypto",
            "timeframe": "5m",
            "lifecycle_status": "papered",
            "final_test_status": "pass",
            "score": 91,
            "permissions": {
                "trader_export_allowed": True,
                "live_small_eligible": False,
                "live_eligible": False,
                "live_allowed": False,
                "fail_reasons": ["Paper 검증은 통과했지만 before-live-small 승급 증거가 없습니다."],
            },
            "contract_version": TRADER_STRATEGY_CONTRACT_VERSION,
            "source_path": "sample",
        },
        {
            "strategy_id": "US-ETF-ROT-02",
            "name": "US ETF Rotation",
            "symbol": "SPY",
            "asset": "us-stock",
            "timeframe": "1d",
            "lifecycle_status": "monitor",
            "final_test_status": "review",
            "score": 82,
            "permissions": {
                "trader_export_allowed": False,
                "live_allowed": False,
                "fail_reasons": ["최종 테스트 pass와 shadow/paper 증거가 필요합니다."],
            },
            "contract_version": TRADER_STRATEGY_CONTRACT_VERSION,
            "source_path": "sample",
        },
    ]
