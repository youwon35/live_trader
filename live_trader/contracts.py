from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


ARTIFACT_SCHEMA_VERSION = "strategy-artifact-v2"
SHARED_STRATEGY_SCHEMA_VERSION = "market-strategy-v1"
TRADER_STRATEGY_CONTRACT_VERSION = "trader-strategy-contract-v2"


def can_trader_use_artifact(artifact: dict[str, Any]) -> bool:
    permissions = artifact.get("permissions") or {}
    return permissions.get("trader_export_allowed") is True or artifact.get("trader_export_allowed") is True


def can_live_use_artifact(artifact: dict[str, Any]) -> bool:
    permissions = artifact.get("permissions") or {}
    return permissions.get("live_allowed") is True or artifact.get("live_allowed") is True


def strategy_artifact_dirs() -> list[Path]:
    appdata = Path(os.getenv("APPDATA", Path.home() / "AppData" / "Roaming"))
    return [
        Path("F:/stock_market_data/strategies"),
        appdata / "trading_programs" / "strategies",
    ]


def load_strategy_artifacts(limit: int = 16) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for folder in strategy_artifact_dirs():
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
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

    return {
        "strategy_id": strategy_id,
        "name": str(artifact.get("name") or artifact.get("strategyName") or strategy_id),
        "symbol": str(artifact.get("symbol") or artifact.get("ticker") or "UNKNOWN"),
        "asset": str(artifact.get("asset") or artifact.get("assetClass") or "unknown"),
        "timeframe": str(artifact.get("timeframe") or artifact.get("interval") or "-"),
        "lifecycle_status": str(artifact.get("lifecycleStatus") or artifact.get("stage") or "draft"),
        "final_test_status": str(artifact.get("finalTestStatus") or artifact.get("test_status") or ""),
        "score": artifact.get("score") or artifact.get("qualityScore") or "-",
        "permissions": {
            "trader_export_allowed": permissions.get("trader_export_allowed") is True,
            "live_allowed": permissions.get("live_allowed") is True,
            "fail_reasons": list(permissions.get("fail_reasons") or artifact.get("fail_reasons") or []),
        },
        "contract_version": str(
            artifact.get("contractVersion")
            or artifact.get("traderStrategyContractVersion")
            or TRADER_STRATEGY_CONTRACT_VERSION
        ),
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
