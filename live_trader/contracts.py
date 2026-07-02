from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


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
IGNORED_STRATEGY_FILE_NAMES = {"package.json", "package-lock.json"}


def _dict_value(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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
    permissions = _dict_value(artifact.get("permissions"))
    return permissions.get("live_allowed") is True or artifact.get("live_allowed") is True


def _verification_badge(status: str, label: str, detail: str) -> dict[str, str]:
    return {"status": status, "label": label, "detail": detail}


def _reason_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


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
    normalized_status = lifecycle_status.lower()
    paper_verified = (
        permissions.get("paper_trader_verified") is True
        or artifact.get("paper_trader_verified") is True
        or normalized_status in {"paper", "paper-approved", "live-small", "live", "live-full", "production"}
    )
    if paper_verified:
        return _verification_badge("pass", "Paper 검증", f"lifecycle={lifecycle_status or 'paper'}")
    if normalized_status == "shadow":
        return _verification_badge("watch", "Shadow 검증 중", "Paper Trader 승급 전 Shadow 검증 단계입니다.")
    return _verification_badge("wait", "Paper 미검증", "Paper Trader 승인/성과 검증 정보가 아직 없습니다.")


def _live_verification_badge(permissions: dict[str, Any]) -> dict[str, str]:
    if permissions.get("live_allowed") is True:
        return _verification_badge("pass", "Live 허용", "live_allowed=true artifact입니다.")
    fail_reasons = _reason_list(permissions.get("fail_reasons"))
    detail = "; ".join(str(reason) for reason in fail_reasons) if fail_reasons else "live_allowed 권한이 없습니다."
    return _verification_badge("fail", "Live 차단", detail)


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
    if "live_allowed" not in permissions and "live_allowed" in artifact:
        permissions["live_allowed"] = artifact.get("live_allowed") is True
    if "trader_export_allowed" not in permissions and "trader_export_allowed" in artifact:
        permissions["trader_export_allowed"] = artifact.get("trader_export_allowed") is True
    lifecycle_status = str(artifact.get("lifecycleStatus") or lifecycle.get("status") or artifact.get("status") or artifact.get("stage") or "draft")
    final_test_status = str(artifact.get("finalTestStatus") or final_test.get("status") or artifact.get("test_status") or "")
    normalized_permissions = {
        "trader_export_allowed": permissions.get("trader_export_allowed") is True,
        "live_allowed": permissions.get("live_allowed") is True,
        "paper_trader_verified": permissions.get("paper_trader_verified") is True,
        "fail_reasons": _reason_list(permissions.get("fail_reasons") or artifact.get("fail_reasons")),
    }
    verification = {
        "backtester": _backtester_verification_badge(normalized_permissions, final_test_status),
        "paper_trader": _paper_verification_badge(artifact, normalized_permissions, lifecycle_status),
        "live": _live_verification_badge(normalized_permissions),
    }

    return {
        "strategy_id": strategy_id,
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
        "order_quantity": artifact.get("order_quantity") or artifact.get("orderQuantity") or parameters.get("positionSize") or settings.get("positionSize") or 1,
        "reference_price": artifact.get("reference_price") or artifact.get("referencePrice") or artifact.get("last_price") or artifact.get("price") or artifact.get("close_price"),
        "last_price": artifact.get("last_price") or artifact.get("reference_price") or artifact.get("price") or artifact.get("close_price"),
        "test_signal": str(artifact.get("test_signal") or artifact.get("manual_signal") or artifact.get("last_signal") or ""),
        "signals": _dict_value(artifact.get("signals")),
        "permissions": normalized_permissions,
        "verification": verification,
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
            "lifecycle_status": "live-small",
            "final_test_status": "pass",
            "score": 87,
            "permissions": {
                "trader_export_allowed": True,
                "live_allowed": False,
                "fail_reasons": ["실계좌 API 키와 주문 어댑터 구현 전까지 live_allowed가 false입니다."],
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
            "lifecycle_status": "paper",
            "final_test_status": "pass",
            "score": 91,
            "permissions": {
                "trader_export_allowed": True,
                "live_allowed": False,
                "fail_reasons": ["Paper 검증은 통과했지만 live-small 승격 증거가 없습니다."],
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
