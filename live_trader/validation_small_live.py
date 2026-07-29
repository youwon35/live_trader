from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Callable, Iterable

from trading_runtime import (
    BuiltinBarSignalEvaluator,
    FeedSubscription,
    PortfolioRuntimeEngine,
    RuntimeStrategySpec,
    feeds_for_specs,
    load_portfolio_runtime_path,
    required_warmup_bars,
)


VALIDATION_PLAN_SCHEMA = "validation-small-live-plan-v1"
VALIDATION_STAGE = "validation-before-live-small"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_LEGACY_HASH_RE = re.compile(r"^[0-9a-f]{16,64}$")
_LIFECYCLE_RANK = {
    "draft": 0,
    "backtested": 10,
    "before-shadow": 20,
    "shadowed": 30,
    "papered": 40,
    "before-live-small": 50,
    "live": 60,
}
_IGNORED_STRATEGY_FILES = {
    "package.json",
    "package-lock.json",
    "strategy-registry.json",
}
_IGNORED_PORTFOLIO_FILES = {
    "package.json",
    "package-lock.json",
    "portfolio-registry.json",
}
_FUTURES_MARKET_TYPES = {
    "future",
    "futures",
    "perpetual",
    "perp",
    "usd-m",
    "usdm",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00",
        "Z",
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object가 아닙니다: {path}")
    return payload


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _items(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _status_pass(value: Any) -> bool:
    return str(value or "").strip().lower() in {
        "pass",
        "passed",
        "ok",
        "success",
    }


def _lifecycle(payload: dict[str, Any]) -> str:
    value = (
        _mapping(payload.get("lifecycle")).get("status")
        or payload.get("lifecycleStatus")
        or payload.get("status")
        or _mapping(payload.get("promotion")).get("stage")
        or payload.get("promotionStage")
        or "draft"
    )
    normalized = str(value).strip().lower().replace("_", "-")
    aliases = {
        "approved": "backtested",
        "final-tested": "backtested",
        "paper": "papered",
        "live-small": "before-live-small",
        "live-canary": "before-live-small",
    }
    return aliases.get(normalized, normalized)


def _artifact_id(payload: dict[str, Any]) -> str:
    return str(
        payload.get("id")
        or payload.get("strategy_id")
        or payload.get("strategyId")
        or payload.get("portfolioId")
        or ""
    ).strip()


def _strategy_symbol(payload: dict[str, Any]) -> str:
    dataset = _mapping(payload.get("dataset"))
    data_artifact = _mapping(
        payload.get("dataArtifact") or payload.get("data_artifact")
    )
    return str(
        payload.get("symbol")
        or dataset.get("symbol")
        or data_artifact.get("symbol")
        or payload.get("ticker")
        or ""
    ).strip()


def _strategy_timeframe(payload: dict[str, Any]) -> str:
    dataset = _mapping(payload.get("dataset"))
    data_artifact = _mapping(
        payload.get("dataArtifact") or payload.get("data_artifact")
    )
    return str(
        payload.get("timeframe")
        or dataset.get("interval")
        or data_artifact.get("interval")
        or payload.get("interval")
        or ""
    ).strip()


def _strategy_plugin(payload: dict[str, Any]) -> str:
    contract = _mapping(
        payload.get("strategyContract") or payload.get("strategy_contract")
    )
    definition = _mapping(
        contract.get("customStrategyDefinition")
        or contract.get("custom_strategy_definition")
    )
    return str(
        payload.get("plugin")
        or payload.get("pluginId")
        or definition.get("pluginId")
        or ""
    ).strip()


def _custom_strategy_definition(payload: dict[str, Any]) -> dict[str, Any]:
    contract = _mapping(
        payload.get("strategyContract") or payload.get("strategy_contract")
    )
    settings = _mapping(payload.get("settings"))
    parameters = _mapping(payload.get("parameters"))
    for candidate in (
        parameters.get("customStrategyDefinition"),
        parameters.get("custom_strategy_definition"),
        contract.get("customStrategyDefinition"),
        contract.get("custom_strategy_definition"),
        payload.get("customStrategyDefinition"),
        payload.get("custom_strategy_definition"),
        settings.get("customStrategyDefinition"),
        settings.get("custom_strategy_definition"),
    ):
        if isinstance(candidate, dict):
            return dict(candidate)
    return {}


def _market_type(
    strategy: dict[str, Any],
    instance: dict[str, Any],
) -> str:
    dataset = _mapping(strategy.get("dataset"))
    settings = _mapping(strategy.get("settings"))
    definition = _custom_strategy_definition(strategy)
    explicit = str(
        instance.get("marketType")
        or instance.get("market_type")
        or strategy.get("marketType")
        or strategy.get("market_type")
        or dataset.get("marketType")
        or dataset.get("market_type")
        or settings.get("marketType")
        or settings.get("market_type")
        or definition.get("marketType")
        or definition.get("market_type")
        or ""
    ).strip().lower()
    if explicit:
        return "futures" if explicit in _FUTURES_MARKET_TYPES else explicit
    source_path = str(
        dataset.get("sourcePath")
        or dataset.get("path")
        or dataset.get("relativePath")
        or ""
    ).replace("\\", "/").lower()
    provider = str(
        instance.get("marketDataProvider")
        or instance.get("provider")
        or dataset.get("provider")
        or ""
    ).strip().lower()
    broker = str(
        instance.get("brokerId")
        or instance.get("broker_id")
        or strategy.get("brokerId")
        or strategy.get("broker_id")
        or ""
    ).strip().lower()
    if (
        broker == "binance-futures"
        or provider == "binance-futures"
        or "/futures/" in f"/{source_path.strip('/')}/"
    ):
        return "futures"
    return "spot"


def _position_direction(strategy: dict[str, Any]) -> str:
    definition = _custom_strategy_definition(strategy)
    value = (
        definition.get("positionDirection")
        or definition.get("position_direction")
        or strategy.get("positionDirection")
        or strategy.get("position_direction")
        or "long"
    )
    return "short" if str(value).strip().lower() == "short" else "long"


def _allow_short(strategy: dict[str, Any], direction: str) -> bool:
    definition = _custom_strategy_definition(strategy)
    execution_policy = _mapping(
        strategy.get("executionPolicy")
        or strategy.get("execution_policy")
    )
    return bool(
        direction == "short"
        or definition.get("allowShort") is True
        or definition.get("allow_short") is True
        or strategy.get("allowShort") is True
        or strategy.get("allow_short_requested") is True
        or execution_policy.get("allowShort") is True
        or execution_policy.get("allow_short") is True
    )


def _final_status(payload: dict[str, Any]) -> str:
    final = _mapping(payload.get("finalTest") or payload.get("final_test"))
    return str(
        final.get("status")
        or payload.get("finalTestStatus")
        or payload.get("test_status")
        or ""
    ).strip()


def _declared_lock(payload: dict[str, Any]) -> dict[str, Any]:
    return _mapping(payload.get("artifactLock"))


def _integrity_class(payload: dict[str, Any], artifact_kind: str) -> dict[str, Any]:
    lock = _declared_lock(payload)
    schema = str(lock.get("schemaVersion") or "")
    declared_hash = str(
        lock.get("artifactHash")
        or payload.get("artifactHash")
        or payload.get("artifact_hash")
        or ""
    ).lower()
    expected_schema = (
        "strategy-artifact-lock-v2"
        if artifact_kind == "strategy"
        else "portfolio-artifact-lock-v2"
    )
    if schema == expected_schema and _HASH_RE.fullmatch(declared_hash):
        return {
            "class": "canonical-current",
            "productionIntegrityReady": True,
            "schemaVersion": schema,
            "artifactHash": declared_hash,
            "issues": [],
        }
    if lock and _LEGACY_HASH_RE.fullmatch(declared_hash):
        return {
            "class": "legacy-lock-validation-only",
            "productionIntegrityReady": False,
            "schemaVersion": schema,
            "artifactHash": declared_hash,
            "issues": [
                f"canonical-lock-schema-upgrade-required:{schema or 'missing'}->{expected_schema}"
            ],
        }
    return {
        "class": "unsealed",
        "productionIntegrityReady": False,
        "schemaVersion": schema,
        "artifactHash": declared_hash,
        "issues": ["canonical-lock-missing-or-invalid"],
    }


def _strategy_precheck(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    lifecycle = _lifecycle(payload)
    permissions = _mapping(payload.get("permissions"))
    candidate = _mapping(
        payload.get("portfolioCandidate") or payload.get("portfolio_candidate")
    )
    fail_reasons = permissions.get("fail_reasons")
    if not _artifact_id(payload):
        issues.append("strategy-id-missing")
    if not _strategy_symbol(payload):
        issues.append("strategy-symbol-missing")
    if not _strategy_timeframe(payload):
        issues.append("strategy-timeframe-missing")
    if not _strategy_plugin(payload):
        issues.append("strategy-plugin-missing")
    if not _status_pass(_final_status(payload)):
        issues.append("final-test-not-passed")
    if permissions.get("trader_export_allowed") is not True:
        issues.append("trader-export-not-allowed")
    if lifecycle in {"paused", "retired"}:
        issues.append(f"lifecycle-{lifecycle}")
    elif _LIFECYCLE_RANK.get(lifecycle, -1) < _LIFECYCLE_RANK["backtested"]:
        issues.append(f"lifecycle-not-backtested:{lifecycle}")
    if candidate:
        if candidate.get("approved") is not True:
            issues.append("portfolio-candidate-not-approved")
        for blocker in candidate.get("blockers") or []:
            issues.append(f"portfolio-candidate:{blocker}")
    if isinstance(fail_reasons, list):
        issues.extend(f"strategy-fail-reason:{reason}" for reason in fail_reasons if str(reason))
    elif fail_reasons not in (None, ""):
        issues.append(f"strategy-fail-reason:{fail_reasons}")
    integrity = _integrity_class(payload, "strategy")
    if integrity["class"] == "unsealed":
        issues.extend(integrity["issues"])
    return list(dict.fromkeys(issues))


def _portfolio_precheck(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    lifecycle = _lifecycle(payload)
    permissions = _mapping(payload.get("permissions"))
    framework = _mapping(payload.get("framework"))
    if not _artifact_id(payload):
        issues.append("portfolio-id-missing")
    if lifecycle in {"paused", "retired"}:
        issues.append(f"portfolio-lifecycle-{lifecycle}")
    elif _LIFECYCLE_RANK.get(lifecycle, -1) < _LIFECYCLE_RANK["backtested"]:
        issues.append(f"portfolio-lifecycle-not-backtested:{lifecycle}")
    if permissions.get("paper_export_allowed") is not True:
        issues.append("portfolio-paper-export-not-allowed")
    failed_risk_checks = [
        str(item.get("label") or item.get("id") or "risk")
        for item in _items(framework.get("riskChecks"))
        if str(item.get("status") or "").lower() == "fail"
    ]
    issues.extend(f"portfolio-risk-check:{item}" for item in failed_risk_checks)
    integrity = _integrity_class(payload, "portfolio")
    if integrity["class"] == "unsealed":
        issues.extend(integrity["issues"])
    return list(dict.fromkeys(issues))


def _target_for_instance(
    portfolio: dict[str, Any],
    instance: dict[str, Any],
) -> tuple[dict[str, Any], float]:
    framework = _mapping(portfolio.get("framework"))
    targets = _items(framework.get("targetPortfolio"))
    strategy_id = str(
        instance.get("strategyId")
        or instance.get("strategy_id")
        or instance.get("sourceStrategyId")
        or ""
    )
    instance_id = str(
        instance.get("instanceId")
        or instance.get("strategyInstanceId")
        or instance.get("id")
        or ""
    )
    symbol = str(
        instance.get("symbol")
        or instance.get("qualifiedSymbol")
        or ""
    )
    target = next(
        (
            item
            for item in targets
            if (
                (
                    instance_id
                    and str(
                        item.get("strategyInstanceId")
                        or item.get("instanceId")
                        or item.get("id")
                        or ""
                    )
                    == instance_id
                )
                or (
                    strategy_id
                    and str(
                        item.get("strategyId")
                        or item.get("strategy_id")
                        or ""
                    )
                    == strategy_id
                    and (
                        not symbol
                        or not str(item.get("symbol") or "")
                        or str(item.get("symbol") or "") == symbol
                    )
                )
            )
        ),
        {},
    )
    allocation = _mapping(instance.get("allocation"))
    raw_weight = (
        target.get("targetWeight")
        if target
        else allocation.get("scoreTargetWeight")
        if allocation.get("scoreTargetWeight") is not None
        else allocation.get("normalizedWeight")
    )
    try:
        weight = float(raw_weight)
    except (TypeError, ValueError):
        weight = 0.0
    return target, weight


def _broker_hint(
    instance: dict[str, Any],
    symbol: str,
    *,
    market_type: str,
    position_direction: str,
    allow_short: bool,
) -> str:
    explicit = str(
        instance.get("brokerId")
        or instance.get("broker_id")
        or ""
    ).strip().lower()
    upper = symbol.upper()
    if (
        (position_direction == "short" or allow_short)
        and upper.endswith(("USDT", "USDC"))
    ):
        # Binance spot cannot open a direct short position.  A short candidate
        # is always described as a Futures route; route consistency is checked
        # separately before the candidate can be evaluated.
        return "binance-futures"
    if market_type in _FUTURES_MARKET_TYPES and upper.endswith(
        ("USDT", "USDC")
    ):
        return "binance-futures"
    if explicit:
        return explicit
    if upper.startswith("KRW-"):
        return "upbit"
    if upper.endswith(("USDT", "USDC")):
        return "binance"
    return "kis"


def _scan_payloads(
    folder: Path,
    *,
    ignored: set[str],
) -> list[tuple[Path, dict[str, Any]]]:
    output: list[tuple[Path, dict[str, Any]]] = []
    if not folder.is_dir():
        return output
    for path in sorted(folder.glob("*.json")):
        if path.name in ignored:
            continue
        try:
            output.append((path, _read_json(path)))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return output


def build_validation_plan(
    artifact_root: Path | str,
    *,
    symbols: Iterable[str] = (),
    timeframes: Iterable[str] = (),
    max_per_bucket: int = 1,
    research_short_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(artifact_root).expanduser().resolve(strict=False)
    symbol_filter = {
        str(item).strip().upper() for item in symbols if str(item).strip()
    }
    timeframe_filter = {
        str(item).strip().lower() for item in timeframes if str(item).strip()
    }
    strategy_files = _scan_payloads(
        root,
        ignored=_IGNORED_STRATEGY_FILES,
    )
    portfolio_files = _scan_payloads(
        root / "portfolios",
        ignored=_IGNORED_PORTFOLIO_FILES,
    )
    strategy_index: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    blocked: list[dict[str, Any]] = []
    for path, payload in strategy_files:
        strategy_id = _artifact_id(payload)
        issues = _strategy_precheck(payload)
        if issues:
            direction = _position_direction(payload)
            market_type = _market_type(payload, {})
            blocked.append(
                {
                    "kind": "strategy",
                    "id": strategy_id or path.stem,
                    "path": str(path),
                    "issues": issues,
                    "symbol": _strategy_symbol(payload),
                    "timeframe": _strategy_timeframe(payload),
                    "marketType": market_type,
                    "positionDirection": direction,
                    "allowShort": _allow_short(payload, direction),
                }
            )
            continue
        strategy_index.setdefault(strategy_id, []).append((path, payload))

    candidates: list[dict[str, Any]] = []
    for portfolio_path, portfolio in portfolio_files:
        portfolio_id = _artifact_id(portfolio)
        portfolio_issues = _portfolio_precheck(portfolio)
        if portfolio_issues:
            blocked.append(
                {
                    "kind": "portfolio",
                    "id": portfolio_id or portfolio_path.stem,
                    "path": str(portfolio_path),
                    "issues": portfolio_issues,
                }
            )
            continue
        portfolio_integrity = _integrity_class(portfolio, "portfolio")
        instances = _items(portfolio.get("strategyInstances"))
        for instance in instances:
            strategy_id = str(
                instance.get("strategyId")
                or instance.get("strategy_id")
                or instance.get("sourceStrategyId")
                or ""
            ).strip()
            if not strategy_id:
                blocked.append(
                    {
                        "kind": "portfolio-instance",
                        "id": str(
                            instance.get("instanceId")
                            or instance.get("id")
                            or "unknown"
                        ),
                        "path": str(portfolio_path),
                        "issues": ["strategy-id-missing"],
                    }
                )
                continue
            matches = strategy_index.get(strategy_id, [])
            if not matches:
                blocked.append(
                    {
                        "kind": "portfolio-instance",
                        "id": str(
                            instance.get("instanceId")
                            or strategy_id
                        ),
                        "path": str(portfolio_path),
                        "issues": [
                            f"eligible-strategy-artifact-not-found:{strategy_id}"
                        ],
                    }
                )
                continue
            target, target_weight = _target_for_instance(portfolio, instance)
            if target_weight <= 0:
                blocked.append(
                    {
                        "kind": "portfolio-instance",
                        "id": str(
                            instance.get("instanceId")
                            or strategy_id
                        ),
                        "path": str(portfolio_path),
                        "issues": ["positive-target-weight-required"],
                    }
                )
                continue

            for strategy_path, strategy in matches:
                symbol = _strategy_symbol(strategy)
                timeframe = _strategy_timeframe(strategy)
                instance_symbol = str(
                    instance.get("symbol")
                    or instance.get("qualifiedSymbol")
                    or ""
                ).strip()
                instance_timeframe = str(
                    instance.get("timeframe")
                    or instance.get("qualifiedTimeframe")
                    or ""
                ).strip()
                mismatch_issues: list[str] = []
                if instance_symbol and symbol != instance_symbol:
                    mismatch_issues.append(
                        f"symbol-mismatch:{symbol}!={instance_symbol}"
                    )
                if (
                    instance_timeframe
                    and timeframe.lower() != instance_timeframe.lower()
                ):
                    mismatch_issues.append(
                        f"timeframe-mismatch:{timeframe}!={instance_timeframe}"
                    )
                if symbol_filter and symbol.upper() not in symbol_filter:
                    continue
                if timeframe_filter and timeframe.lower() not in timeframe_filter:
                    continue
                if mismatch_issues:
                    blocked.append(
                        {
                            "kind": "portfolio-instance",
                            "id": str(
                                instance.get("instanceId")
                                or strategy_id
                            ),
                            "path": str(portfolio_path),
                            "issues": mismatch_issues,
                        }
                    )
                    continue
                strategy_integrity = _integrity_class(
                    strategy,
                    "strategy",
                )
                lifecycle = _lifecycle(strategy)
                market_type = _market_type(strategy, instance)
                position_direction = _position_direction(strategy)
                allow_short = _allow_short(
                    strategy,
                    position_direction,
                )
                broker_hint = _broker_hint(
                    instance,
                    symbol,
                    market_type=market_type,
                    position_direction=position_direction,
                    allow_short=allow_short,
                )
                declared_broker = str(
                    instance.get("brokerId")
                    or instance.get("broker_id")
                    or ""
                ).strip().lower()
                route_issues: list[str] = []
                if position_direction == "short":
                    if market_type not in _FUTURES_MARKET_TYPES:
                        route_issues.append(
                            f"short-market-must-be-futures:{market_type}"
                        )
                    if declared_broker != "binance-futures":
                        route_issues.append(
                            "short-broker-must-be-binance-futures:"
                            + (declared_broker or "missing")
                        )
                if (
                    market_type in _FUTURES_MARKET_TYPES
                    and symbol.upper().endswith(("USDT", "USDC"))
                    and declared_broker != "binance-futures"
                ):
                    route_issues.append(
                        "futures-broker-must-be-binance-futures:"
                        + (declared_broker or "missing")
                    )
                if route_issues:
                    blocked.append(
                        {
                            "kind": "portfolio-instance",
                            "id": str(
                                instance.get("instanceId")
                                or strategy_id
                            ),
                            "path": str(portfolio_path),
                            "issues": route_issues,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "marketType": market_type,
                            "positionDirection": position_direction,
                            "allowShort": allow_short,
                        }
                    )
                    continue
                futures_short_candidate = bool(
                    market_type in _FUTURES_MARKET_TYPES
                    and position_direction == "short"
                    and allow_short
                    and broker_hint == "binance-futures"
                )
                candidates.append(
                    {
                        "validationStage": (
                            VALIDATION_STAGE
                            if futures_short_candidate
                            else "integration-monitor-smoke"
                        ),
                        "candidateClass": (
                            "futures-short-monitor-smoke"
                            if futures_short_candidate
                            else "general-integration-smoke"
                        ),
                        "validationEligible": True,
                        "runtimeMode": "MONITOR",
                        "dryRunRequired": True,
                        "brokerSubmitAllowed": False,
                        "productionPermissionGranted": False,
                        "strategyId": strategy_id,
                        "strategyName": str(
                            strategy.get("name")
                            or strategy.get("strategyName")
                            or strategy_id
                        ),
                        "strategyPath": str(strategy_path),
                        "strategyFileSha256": _file_hash(strategy_path),
                        "strategyArtifactHash": str(
                            strategy_integrity["artifactHash"]
                        ),
                        "strategyIntegrity": strategy_integrity,
                        "productionLifecycle": lifecycle,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "plugin": _strategy_plugin(strategy),
                        "brokerHint": broker_hint,
                        "marketType": market_type,
                        "positionDirection": position_direction,
                        "allowShort": allow_short,
                        "futuresShortCandidate": (
                            futures_short_candidate
                        ),
                        "marketDataProvider": str(
                            instance.get("marketDataProvider")
                            or _mapping(strategy.get("dataset")).get("provider")
                            or ""
                        ),
                        "portfolioId": portfolio_id,
                        "portfolioName": str(
                            portfolio.get("name")
                            or portfolio.get("portfolioName")
                            or portfolio_id
                        ),
                        "portfolioPath": str(portfolio_path),
                        "portfolioFileSha256": _file_hash(portfolio_path),
                        "portfolioArtifactHash": str(
                            portfolio_integrity["artifactHash"]
                        ),
                        "portfolioIntegrity": portfolio_integrity,
                        "runtimeEvaluationReady": bool(
                            strategy_integrity[
                                "productionIntegrityReady"
                            ]
                            and portfolio_integrity[
                                "productionIntegrityReady"
                            ]
                        ),
                        "strategyInstanceId": str(
                            instance.get("instanceId")
                            or instance.get("strategyInstanceId")
                            or instance.get("id")
                            or ""
                        ),
                        "targetWeight": target_weight,
                        "targetStatus": str(
                            target.get("status")
                            or _mapping(
                                instance.get("signalScore")
                            ).get("status")
                            or ""
                        ),
                        "productionGatesPending": [
                            "shadow-evidence",
                            "paper-order-evidence",
                            "paper-observation-window",
                            "recovery-drill",
                            "broker-reconciliation",
                            "operator-confirmation",
                            "small-live-canary-fills",
                        ],
                    }
                )

    integrity_order = {
        "canonical-current": 0,
        "legacy-lock-validation-only": 1,
        "unsealed": 2,
    }
    candidates.sort(
        key=lambda item: (
            str(item["brokerHint"]),
            str(item["symbol"]),
            str(item["timeframe"]),
            integrity_order.get(
                str(item["strategyIntegrity"]["class"]),
                9,
            ),
            integrity_order.get(
                str(item["portfolioIntegrity"]["class"]),
                9,
            ),
            str(item["portfolioId"]),
            str(item["strategyId"]),
        )
    )
    selected: list[dict[str, Any]] = []
    bucket_counts: dict[tuple[str, str, str], int] = {}
    limit = max(1, int(max_per_bucket))
    for candidate in candidates:
        bucket = (
            str(candidate["brokerHint"]),
            str(candidate["symbol"]).upper(),
            str(candidate["timeframe"]).lower(),
        )
        if bucket_counts.get(bucket, 0) >= limit:
            continue
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        candidate["validationStrategyInstanceId"] = (
            "vsi:"
            + _stable_hash(
                {
                    "strategyId": candidate["strategyId"],
                    "strategyFileSha256": candidate[
                        "strategyFileSha256"
                    ],
                    "portfolioId": candidate["portfolioId"],
                    "strategyInstanceId": candidate[
                        "strategyInstanceId"
                    ],
                }
            )[:24]
        )
        candidate["validationPortfolioInstanceId"] = (
            "vpi:"
            + _stable_hash(
                {
                    "portfolioId": candidate["portfolioId"],
                    "portfolioFileSha256": candidate[
                        "portfolioFileSha256"
                    ],
                }
            )[:24]
        )
        selected.append(candidate)

    validation_portfolio_count = len(
        {
            str(item["validationPortfolioInstanceId"])
            for item in selected
        }
    )
    futures_short_candidate_count = sum(
        1
        for item in selected
        if item.get("futuresShortCandidate") is True
    )
    general_smoke_candidate_count = sum(
        1
        for item in selected
        if item.get("candidateClass") == "general-integration-smoke"
    )
    blocked_futures_short_count = sum(
        1
        for item in blocked
        if item.get("marketType") in _FUTURES_MARKET_TYPES
        and item.get("positionDirection") == "short"
    )
    coverage = sorted(
        {
            (
                str(item["brokerHint"]),
                str(item["symbol"]),
                str(item["timeframe"]),
            )
            for item in selected
        }
    )
    plan: dict[str, Any] = {
        "schemaVersion": VALIDATION_PLAN_SCHEMA,
        "planId": "",
        "createdAt": _utc_now(),
        "artifactRoot": str(root),
        "purpose": (
            "Backtest와 Portfolio를 통과한 후보를 실제 주문 권한 없이 "
            "Live Trader의 MONITOR/Dry-run 통합 검증 대상으로 고정합니다."
        ),
        "guardrails": {
            "runtimeMode": "MONITOR",
            "dryRunRequired": True,
            "networkRequired": False,
            "brokerSubmitAllowed": False,
            "maximumOrderNotional": 0,
            "productionLifecycleMutation": False,
            "liveSmallPermissionGranted": False,
            "fullLivePermissionGranted": False,
        },
        "selectionPolicy": {
            "strategy": (
                "final PASS + trader_export_allowed + portfolio candidate "
                "approval + sealed artifact"
            ),
            "portfolio": (
                "backtested + paper_export_allowed + risk check PASS + "
                "positive target weight + sealed artifact"
            ),
            "maxPerBrokerSymbolTimeframe": limit,
        },
        "coverage": [
            {
                "broker": broker,
                "symbol": symbol,
                "timeframe": timeframe,
            }
            for broker, symbol, timeframe in coverage
        ],
        "candidateCount": len(selected),
        "generalSmokeCandidateCount": general_smoke_candidate_count,
        "futuresShortCandidateCount": futures_short_candidate_count,
        "blockedFuturesShortCount": blocked_futures_short_count,
        "validationStrategyInstanceCount": len(selected),
        "validationPortfolioInstanceCount": validation_portfolio_count,
        "blockedCount": len(blocked),
        "candidates": selected,
        "blocked": blocked,
    }
    if isinstance(research_short_bundle, dict):
        plan["researchShortBundle"] = dict(research_short_bundle)
    plan["planId"] = f"validation-small-live-{_stable_hash(plan)[:16]}"
    plan["contentHash"] = _stable_hash(plan)
    return plan


def validate_monitor_only_plan(
    plan: dict[str, Any],
    *,
    verify_files: bool = True,
) -> dict[str, Any]:
    issues: list[str] = []
    if plan.get("schemaVersion") != VALIDATION_PLAN_SCHEMA:
        issues.append("schema-version-invalid")
    guardrails = _mapping(plan.get("guardrails"))
    expected_guardrails = {
        "runtimeMode": "MONITOR",
        "dryRunRequired": True,
        "networkRequired": False,
        "brokerSubmitAllowed": False,
        "maximumOrderNotional": 0,
        "productionLifecycleMutation": False,
        "liveSmallPermissionGranted": False,
        "fullLivePermissionGranted": False,
    }
    for key, expected in expected_guardrails.items():
        if guardrails.get(key) != expected:
            issues.append(f"guardrail-invalid:{key}")
    research_bundle = _mapping(plan.get("researchShortBundle"))
    if research_bundle:
        if research_bundle.get("researchOnly") is not True:
            issues.append("research-bundle-not-research-only")
        if research_bundle.get("artifactPromotionAllowed") is not False:
            issues.append("research-bundle-promotion-not-blocked")
        if research_bundle.get("productionPermissionGranted") is not False:
            issues.append("research-bundle-production-permission-granted")
        if research_bundle.get("brokerSubmitAllowed") is not False:
            issues.append("research-bundle-broker-submit-allowed")
    declared_hash = str(plan.get("contentHash") or "")
    body = {key: value for key, value in plan.items() if key != "contentHash"}
    if not _HASH_RE.fullmatch(declared_hash):
        issues.append("plan-content-hash-invalid")
    elif declared_hash != _stable_hash(body):
        issues.append("plan-content-hash-mismatch")

    root = Path(str(plan.get("artifactRoot") or "")).resolve(strict=False)
    candidates = _items(plan.get("candidates"))
    if int(plan.get("candidateCount") or 0) != len(candidates):
        issues.append("candidate-count-mismatch")
    strategy_instance_ids = {
        str(item.get("validationStrategyInstanceId") or "")
        for item in candidates
    }
    portfolio_instance_ids = {
        str(item.get("validationPortfolioInstanceId") or "")
        for item in candidates
    }
    if "" in strategy_instance_ids or len(strategy_instance_ids) != len(
        candidates
    ):
        issues.append("validation-strategy-instance-id-invalid")
    if "" in portfolio_instance_ids:
        issues.append("validation-portfolio-instance-id-invalid")
    if int(plan.get("validationStrategyInstanceCount") or 0) != len(
        candidates
    ):
        issues.append("validation-strategy-instance-count-mismatch")
    if int(plan.get("validationPortfolioInstanceCount") or 0) != len(
        portfolio_instance_ids
    ):
        issues.append("validation-portfolio-instance-count-mismatch")
    futures_short_count = sum(
        1
        for item in candidates
        if item.get("futuresShortCandidate") is True
    )
    general_smoke_count = sum(
        1
        for item in candidates
        if item.get("candidateClass") == "general-integration-smoke"
    )
    if int(plan.get("futuresShortCandidateCount") or 0) != (
        futures_short_count
    ):
        issues.append("futures-short-candidate-count-mismatch")
    if int(plan.get("generalSmokeCandidateCount") or 0) != (
        general_smoke_count
    ):
        issues.append("general-smoke-candidate-count-mismatch")
    for index, candidate in enumerate(candidates):
        prefix = f"candidate-{index}"
        if candidate.get("validationEligible") is not True:
            issues.append(f"{prefix}:validation-eligibility-missing")
        if candidate.get("runtimeMode") != "MONITOR":
            issues.append(f"{prefix}:runtime-mode-not-monitor")
        if candidate.get("dryRunRequired") is not True:
            issues.append(f"{prefix}:dry-run-not-required")
        if candidate.get("brokerSubmitAllowed") is not False:
            issues.append(f"{prefix}:broker-submit-not-blocked")
        if candidate.get("productionPermissionGranted") is not False:
            issues.append(f"{prefix}:production-permission-granted")
        if candidate.get("positionDirection") == "short":
            if candidate.get("marketType") != "futures":
                issues.append(f"{prefix}:short-market-not-futures")
            if candidate.get("brokerHint") != "binance-futures":
                issues.append(f"{prefix}:short-broker-not-binance-futures")
            if candidate.get("allowShort") is not True:
                issues.append(f"{prefix}:short-permission-not-declared")
        if not verify_files:
            continue
        for kind in ("strategy", "portfolio"):
            path = Path(str(candidate.get(f"{kind}Path") or "")).resolve(
                strict=False
            )
            expected_hash = str(
                candidate.get(f"{kind}FileSha256") or ""
            )
            try:
                path.relative_to(root)
            except ValueError:
                issues.append(f"{prefix}:{kind}-path-outside-root")
                continue
            if not path.is_file():
                issues.append(f"{prefix}:{kind}-file-missing")
                continue
            if not _HASH_RE.fullmatch(expected_hash):
                issues.append(f"{prefix}:{kind}-file-hash-invalid")
            elif _file_hash(path) != expected_hash:
                issues.append(f"{prefix}:{kind}-file-hash-mismatch")
    return {
        "ok": not issues,
        "schemaVersion": "validation-small-live-verification-v1",
        "checkedAt": _utc_now(),
        "candidateCount": len(candidates),
        "issues": issues,
        "brokerSubmitAllowed": False,
        "runtimeMode": "MONITOR",
    }


def write_validation_plan(path: Path | str, plan: dict[str, Any]) -> Path:
    verification = validate_monitor_only_plan(plan, verify_files=True)
    if verification["ok"] is not True:
        raise ValueError(
            "검증용 Small Live plan이 안전 계약을 통과하지 못했습니다: "
            + ", ".join(verification["issues"])
        )
    target = Path(path).expanduser().resolve(strict=False)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(
        target.suffix + f".{os.getpid()}.tmp"
    )
    temporary.write_text(
        json.dumps(plan, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temporary.replace(target)
    return target


def load_and_validate_plan(path: Path | str) -> dict[str, Any]:
    target = Path(path).expanduser().resolve(strict=False)
    plan = _read_json(target)
    verification = validate_monitor_only_plan(plan, verify_files=True)
    return {
        "path": str(target),
        "plan": plan,
        "verification": verification,
    }


def default_plan_path() -> Path:
    base = (
        os.environ.get("LOCALAPPDATA")
        or os.environ.get("APPDATA")
        or str(Path.home())
    )
    return (
        Path(base)
        / "live_trader"
        / "logs"
        / "validation-small-live-plan.json"
    )


def default_research_short_pointer_path() -> Path:
    app_root = Path(__file__).resolve().parents[1]
    return (
        app_root.parent
        / "backtester"
        / "tmp"
        / "binance-futures-short-research-bundles"
        / "latest-manifest.json"
    )


def default_short_qualification_report_path() -> Path:
    app_root = Path(__file__).resolve().parents[1]
    return (
        app_root.parent
        / "backtester"
        / "tmp"
        / "binance-futures-short-e2e"
        / "btc-ema-cross-live-validation.json"
    )


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
        return True
    except ValueError:
        return False


def _short_qualification_snapshot(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {
            "available": False,
            "path": str(path),
            "passedStrategyCount": 0,
            "note": "새 strict qualification 보고서가 없습니다.",
        }
    try:
        report = _read_json(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "available": False,
            "path": str(path),
            "passedStrategyCount": 0,
            "note": f"strict qualification 보고서를 읽지 못했습니다: {exc}",
        }
    results = _items(report.get("results"))
    summaries: list[dict[str, Any]] = []
    for result in results:
        evaluated = _items(result.get("evaluated"))
        summaries.append(
            {
                "symbol": str(result.get("symbol") or ""),
                "candidateCount": len(evaluated),
                "preHoldoutPassedCount": int(
                    result.get("preHoldoutPassedCount") or 0
                ),
                "passedCount": int(result.get("passedCount") or 0),
                "candidateDiagnostics": [
                    {
                        "candidateId": str(
                            item.get("candidateId") or ""
                        ),
                        "preHoldoutPassed": (
                            item.get("preHoldoutPassed") is True
                        ),
                        "trainReturn": _mapping(
                            item.get("train")
                        ).get("metrics", {}).get("totalReturn")
                        if isinstance(
                            _mapping(item.get("train")).get("metrics"),
                            dict,
                        )
                        else None,
                        "validationReturn": _mapping(
                            item.get("validation")
                        ).get("metrics", {}).get("totalReturn")
                        if isinstance(
                            _mapping(item.get("validation")).get(
                                "metrics"
                            ),
                            dict,
                        )
                        else None,
                    }
                    for item in evaluated
                ],
            }
        )
    passed_count = sum(item["passedCount"] for item in summaries)
    return {
        "available": True,
        "path": str(path.resolve(strict=False)),
        "generatedAt": str(report.get("generatedAt") or ""),
        "passedStrategyCount": passed_count,
        "summaries": summaries,
        "note": (
            "현재 strict Train/Validation/WF/Holdout 기준을 통과했습니다."
            if passed_count
            else (
                "실제 Futures 데이터로 재실행했지만 strict 승급 기준을 "
                "통과한 전략은 없습니다. 연구 bundle과 생산 승급을 "
                "분리합니다."
            )
        ),
    }


def research_short_bundle_snapshot(
    pointer_path: Path | str | None = None,
) -> dict[str, Any]:
    pointer = Path(
        pointer_path or default_research_short_pointer_path()
    ).expanduser().resolve(strict=False)
    empty = {
        "available": False,
        "researchOnly": True,
        "artifactPromotionAllowed": False,
        "productionPermissionGranted": False,
        "brokerSubmitAllowed": False,
        "strategyCount": 0,
        "functionalPass": False,
        "issues": [],
        "pointerPath": str(pointer),
        "strictQualification": _short_qualification_snapshot(
            default_short_qualification_report_path()
        ),
    }
    if not pointer.is_file():
        return {
            **empty,
            "issues": ["research-short-bundle-pointer-missing"],
        }
    try:
        pointer_payload = _read_json(pointer)
        manifest_path = Path(
            str(pointer_payload.get("manifestPath") or "")
        ).expanduser().resolve(strict=False)
        if not _path_within(manifest_path, pointer.parent):
            raise ValueError("research manifest path가 bundle root 밖입니다.")
        manifest = _read_json(manifest_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            **empty,
            "issues": [f"research-short-bundle-read-failed:{exc}"],
        }

    issues: list[str] = []
    declared_manifest_hash = str(
        pointer_payload.get("manifestHash")
        or manifest.get("manifestHash")
        or ""
    )
    manifest_body = {
        key: value
        for key, value in manifest.items()
        if key != "manifestHash"
    }
    if (
        not _HASH_RE.fullmatch(declared_manifest_hash)
        or _stable_hash(manifest_body) != declared_manifest_hash
    ):
        issues.append("research-short-manifest-hash-mismatch")
    if manifest.get("researchOnly") is not True:
        issues.append("research-only-flag-missing")
    if manifest.get("artifactPromotionAllowed") is not False:
        issues.append("research-promotion-must-be-blocked")
    if int(manifest.get("actualBrokerOrdersSent") or 0) != 0:
        issues.append("research-bundle-declares-broker-orders")

    portfolio = _mapping(manifest.get("portfolio"))
    execution = _mapping(portfolio.get("executionValidation"))
    if execution.get("passed") is not True:
        issues.append("research-portfolio-execution-not-passed")
    if execution.get("entrySide") != "SELL":
        issues.append("research-short-entry-side-not-sell")
    if execution.get("coverSide") != "BUY":
        issues.append("research-short-cover-side-not-buy")
    if execution.get("coverReduceOnly") is not True:
        issues.append("research-short-cover-not-reduce-only")

    paper_runner = _mapping(manifest.get("paperRunner"))
    paper_path = Path(
        str(paper_runner.get("expectedOutputPath") or "")
    ).expanduser().resolve(strict=False)
    paper_report: dict[str, Any] = {}
    if paper_path.is_file() and _path_within(paper_path, pointer.parent):
        try:
            paper_report = _read_json(paper_path)
        except (OSError, ValueError, json.JSONDecodeError):
            issues.append("research-paper-report-invalid")
    else:
        issues.append("research-paper-report-missing")
    functional_pass = bool(
        paper_report.get("functionalPass") is True
        and paper_report.get("promotionEligible") is False
        and int(paper_report.get("actualBrokerOrdersSent") or 0) == 0
    )
    if not functional_pass:
        issues.append("research-paper-functional-pass-missing")

    strategy_rows = [
        {
            "strategyId": str(item.get("strategyId") or ""),
            "symbol": str(item.get("symbol") or ""),
            "timeframe": str(item.get("timeframe") or ""),
            "marketType": "futures",
            "positionDirection": "short",
            "brokerHint": "binance-futures",
            "paperReplayRoundTrip": bool(
                _mapping(item.get("paperReplay")).get(
                    "expectedSideSequence"
                )
                == ["SELL", "BUY"]
            ),
            "researchHoldoutReturn": _mapping(
                item.get("researchHoldout")
            ).get("totalReturn"),
            "researchHoldoutStressReturn": _mapping(
                item.get("researchHoldoutStress")
            ).get("totalReturn"),
        }
        for item in _items(manifest.get("strategies"))
    ]
    return {
        **empty,
        "available": not issues,
        "bundleId": str(manifest.get("bundleId") or ""),
        "manifestPath": str(manifest_path),
        "strategyCount": len(strategy_rows),
        "portfolioId": str(portfolio.get("portfolioId") or ""),
        "portfolioExecutionPassed": execution.get("passed") is True,
        "orderCount": int(execution.get("orderCount") or 0),
        "functionalPass": functional_pass,
        "paperPassedStrategyCount": int(
            paper_report.get("passedStrategyCount") or 0
        ),
        "strategies": strategy_rows,
        "issues": list(dict.fromkeys(issues)),
    }


def validation_plan_snapshot(
    path: Path | str | None = None,
) -> dict[str, Any]:
    target = Path(path or default_plan_path()).expanduser().resolve(
        strict=False
    )
    research = research_short_bundle_snapshot()
    if not target.is_file():
        return {
            "ok": False,
            "schemaVersion": "validation-small-live-ui-v1",
            "planPath": str(target),
            "reason": "검증용 MONITOR plan이 없습니다.",
            "verification": {
                "ok": False,
                "issues": ["validation-plan-missing"],
            },
            "candidateCount": 0,
            "generalSmokeCandidateCount": 0,
            "futuresShortCandidateCount": 0,
            "runtimeEvaluationReadyCount": 0,
            "candidates": [],
            "researchShortBundle": research,
        }
    try:
        loaded = load_and_validate_plan(target)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "schemaVersion": "validation-small-live-ui-v1",
            "planPath": str(target),
            "reason": f"검증용 plan을 읽지 못했습니다: {exc}",
            "verification": {"ok": False, "issues": [str(exc)]},
            "candidateCount": 0,
            "generalSmokeCandidateCount": 0,
            "futuresShortCandidateCount": 0,
            "runtimeEvaluationReadyCount": 0,
            "candidates": [],
            "researchShortBundle": research,
        }
    plan = loaded["plan"]
    verification = loaded["verification"]
    embedded_research = _mapping(plan.get("researchShortBundle"))
    if (
        research.get("available") is not True
        and embedded_research.get("researchOnly") is True
        and embedded_research.get("artifactPromotionAllowed") is False
        and embedded_research.get("productionPermissionGranted") is False
        and embedded_research.get("brokerSubmitAllowed") is False
    ):
        research = {
            **embedded_research,
            "portablePlanFallback": True,
        }
    candidates: list[dict[str, Any]] = []
    portfolio_counts: dict[str, int] = {}
    for item in _items(plan.get("candidates")):
        portfolio_key = str(
            item.get("validationPortfolioInstanceId") or ""
        )
        portfolio_counts[portfolio_key] = (
            portfolio_counts.get(portfolio_key, 0) + 1
        )
    for item in _items(plan.get("candidates")):
        integrity_ready = bool(
            _mapping(item.get("strategyIntegrity")).get(
                "productionIntegrityReady"
            )
            and _mapping(item.get("portfolioIntegrity")).get(
                "productionIntegrityReady"
            )
        )
        candidates.append(
            {
                "validationStrategyInstanceId": str(
                    item.get("validationStrategyInstanceId") or ""
                ),
                "validationPortfolioInstanceId": str(
                    item.get("validationPortfolioInstanceId") or ""
                ),
                "strategyId": str(item.get("strategyId") or ""),
                "strategyName": str(item.get("strategyName") or ""),
                "portfolioId": str(item.get("portfolioId") or ""),
                "portfolioName": str(item.get("portfolioName") or ""),
                "symbol": str(item.get("symbol") or ""),
                "timeframe": str(item.get("timeframe") or ""),
                "plugin": str(item.get("plugin") or ""),
                "brokerHint": str(item.get("brokerHint") or ""),
                "marketType": str(item.get("marketType") or "spot"),
                "positionDirection": str(
                    item.get("positionDirection") or "long"
                ),
                "allowShort": item.get("allowShort") is True,
                "futuresShortCandidate": (
                    item.get("futuresShortCandidate") is True
                ),
                "candidateClass": str(
                    item.get("candidateClass")
                    or "general-integration-smoke"
                ),
                "productionLifecycle": str(
                    item.get("productionLifecycle") or ""
                ),
                "runtimeEvaluationReady": integrity_ready,
                "runtimeMode": "MONITOR",
                "dryRunRequired": True,
                "brokerSubmitAllowed": False,
                "productionPermissionGranted": False,
                "monitorScopeStrategyCount": portfolio_counts.get(
                    str(
                        item.get("validationPortfolioInstanceId") or ""
                    ),
                    1,
                ),
                "integrityClass": (
                    "canonical-current"
                    if integrity_ready
                    else "legacy-lock-validation-only"
                ),
            }
        )
    return {
        "ok": verification.get("ok") is True,
        "schemaVersion": "validation-small-live-ui-v1",
        "planPath": str(target),
        "planId": str(plan.get("planId") or ""),
        "createdAt": str(plan.get("createdAt") or ""),
        "verification": verification,
        "candidateCount": len(candidates),
        "generalSmokeCandidateCount": sum(
            1
            for item in candidates
            if item["candidateClass"] == "general-integration-smoke"
        ),
        "futuresShortCandidateCount": sum(
            1
            for item in candidates
            if item["futuresShortCandidate"]
        ),
        "blockedFuturesShortCount": int(
            plan.get("blockedFuturesShortCount") or 0
        ),
        "runtimeEvaluationReadyCount": sum(
            1 for item in candidates if item["runtimeEvaluationReady"]
        ),
        "guardrails": _mapping(plan.get("guardrails")),
        "candidates": candidates,
        "researchShortBundle": research,
    }


def resolve_runtime_candidate(
    validation_strategy_instance_id: str,
    *,
    path: Path | str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    target = Path(path or default_plan_path()).expanduser().resolve(
        strict=False
    )
    loaded = load_and_validate_plan(target)
    verification = loaded["verification"]
    if verification.get("ok") is not True:
        raise ValueError(
            "검증 plan이 무결성 검사를 통과하지 못했습니다: "
            + ", ".join(verification.get("issues") or [])
        )
    candidate = next(
        (
            item
            for item in _items(loaded["plan"].get("candidates"))
            if str(item.get("validationStrategyInstanceId") or "")
            == str(validation_strategy_instance_id or "").strip()
        ),
        None,
    )
    if candidate is None:
        raise ValueError("검증 후보를 찾을 수 없습니다.")
    runtime_ready = bool(
        _mapping(candidate.get("strategyIntegrity")).get(
            "productionIntegrityReady"
        )
        and _mapping(candidate.get("portfolioIntegrity")).get(
            "productionIntegrityReady"
        )
    )
    if not runtime_ready:
        raise ValueError(
            "legacy lock 후보는 목록 검토만 가능하며 실제 신호 평가는 "
            "Backtester canonical v2 재발행 후 가능합니다."
        )
    if (
        candidate.get("runtimeMode") != "MONITOR"
        or candidate.get("dryRunRequired") is not True
        or candidate.get("brokerSubmitAllowed") is not False
        or candidate.get("productionPermissionGranted") is not False
    ):
        raise ValueError("검증 후보의 MONITOR 안전 계약이 올바르지 않습니다.")
    return loaded["plan"], candidate


def validation_candidate_profile(candidate: dict[str, Any]) -> str:
    broker = str(candidate.get("brokerHint") or "").strip().lower()
    symbol = str(candidate.get("symbol") or "").strip().upper()
    return (
        "crypto"
        if broker in {
            "binance",
            "binance-futures",
            "upbit",
        }
        or symbol.startswith("KRW-")
        or symbol.endswith(("USDT", "USDC"))
        else "stock"
    )


def _candidate_runtime_spec(
    candidate: dict[str, Any],
) -> RuntimeStrategySpec:
    loaded = load_portfolio_runtime_path(
        str(candidate.get("portfolioPath") or "")
    )
    instance_id = str(candidate.get("strategyInstanceId") or "")
    strategy_id = str(candidate.get("strategyId") or "")
    symbol = str(candidate.get("symbol") or "")
    timeframe = str(candidate.get("timeframe") or "").lower()
    matches = [
        spec
        for spec in loaded.specs
        if spec.strategy_instance_id == instance_id
        and spec.strategy_id == strategy_id
        and spec.symbol == symbol
        and spec.timeframe.lower() == timeframe
    ]
    if len(matches) != 1:
        raise ValueError(
            "검증 plan과 canonical Portfolio runtime spec이 정확히 "
            "일치하지 않습니다."
        )
    return matches[0]


def evaluate_validation_candidate_once(
    validation_strategy_instance_id: str,
    *,
    path: Path | str | None = None,
    feed_factory: Callable[
        [tuple[RuntimeStrategySpec, ...]],
        list[Any],
    ]
    | None = None,
) -> dict[str, Any]:
    plan, candidate = resolve_runtime_candidate(
        validation_strategy_instance_id,
        path=path,
    )
    spec = _candidate_runtime_spec(candidate)
    captured: list[dict[str, Any]] = []

    def monitor_handler(cycle: Any) -> dict[str, Any]:
        payload = {
            "action": "MONITOR",
            "dryRunRequired": True,
            "brokerSubmitAllowed": False,
            "maximumOrderNotional": 0,
            "cycleId": cycle.cycle_id,
        }
        captured.append(payload)
        return payload

    evaluator = BuiltinBarSignalEvaluator(lambda _spec: 0.0)
    engine = PortfolioRuntimeEngine(
        (spec,),
        mode="MONITOR",
        evaluator=evaluator,
        cycle_handler=monitor_handler,
        state_store=None,
    )
    factory = feed_factory or (
        lambda specs: feeds_for_specs(
            specs,
            prefer_kis=True,
            kis_demo=False,
            kis_app_key=os.getenv("KIS_APP_KEY", ""),
            kis_app_secret=os.getenv("KIS_APP_SECRET", ""),
        )
    )
    feeds = list(factory((spec,)))
    feed = next(
        (
            item
            for item in feeds
            if str(getattr(item, "provider_id", "")).lower()
            == spec.provider.lower()
        ),
        None,
    )
    if feed is None:
        raise ValueError(
            f"{spec.provider} read-only market-data feed가 없습니다."
        )
    requested_bars = min(
        20_000,
        max(2, int(required_warmup_bars(spec)) + 1),
    )
    bars = sorted(
        list(
            feed.warmup(
                FeedSubscription(
                    spec.instrument_id,
                    spec.symbol,
                    spec.timeframe,
                ),
                requested_bars,
            )
        ),
        key=lambda item: item.end_time,
    )
    if not bars:
        raise ValueError("확정 봉 market-data를 한 건도 불러오지 못했습니다.")
    seeded = engine.seed_history(
        spec.strategy_instance_id,
        bars[:-1],
    )
    cycle = engine.ingest_closed_bar(bars[-1])
    if cycle is None or len(cycle.decisions) != 1:
        raise ValueError("확정 봉 1회 신호 평가 결과를 만들지 못했습니다.")
    decision = cycle.decisions[0]
    return {
        "ok": True,
        "schemaVersion": "validation-small-live-evaluation-v1",
        "evaluatedAt": _utc_now(),
        "planId": str(plan.get("planId") or ""),
        "validationStrategyInstanceId": str(
            candidate.get("validationStrategyInstanceId") or ""
        ),
        "validationPortfolioInstanceId": str(
            candidate.get("validationPortfolioInstanceId") or ""
        ),
        "candidateClass": str(candidate.get("candidateClass") or ""),
        "strategyId": spec.strategy_id,
        "portfolioId": spec.portfolio_id,
        "symbol": spec.symbol,
        "timeframe": spec.timeframe,
        "brokerId": spec.broker_id,
        "marketDataProvider": spec.provider,
        "marketType": str(candidate.get("marketType") or ""),
        "positionDirection": str(
            candidate.get("positionDirection") or ""
        ),
        "runtimeMode": "MONITOR",
        "dryRunRequired": True,
        "brokerSubmitAllowed": False,
        "maximumOrderNotional": 0,
        "warmupRequested": requested_bars,
        "warmupSeeded": seeded,
        "decision": decision.to_dict(),
        "monitorHandler": captured[-1] if captured else {},
        "engine": engine.snapshot(),
    }


__all__ = [
    "VALIDATION_PLAN_SCHEMA",
    "VALIDATION_STAGE",
    "build_validation_plan",
    "default_plan_path",
    "default_research_short_pointer_path",
    "evaluate_validation_candidate_once",
    "load_and_validate_plan",
    "research_short_bundle_snapshot",
    "resolve_runtime_candidate",
    "validate_monitor_only_plan",
    "validation_candidate_profile",
    "validation_plan_snapshot",
    "write_validation_plan",
]
