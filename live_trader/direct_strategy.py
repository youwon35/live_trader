"""Backtester -> Live metadata registration, independent of Paper results.

Registration never queries a broker, changes trading switches or starts a run.
The immutable source, instance, account and budget are re-read on each use.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from trading_runtime.artifact_governance import DeploymentStore, artifact_reference, stable_sha256
from trading_runtime.portfolio_runtime import load_portfolio_runtime
from trading_runtime.strategy_source_binding import canonical_source_parameters
from . import paper_candidate_inbox as files
from .account_profiles import saved_account_profiles

SCHEMA = "DIRECT_BACKTESTER_V1"
FIELDS = {"rootKey", "candidateId", "identityHash", "registryHash", "accountProfileId", "capitalPercent"}


def source_eligibility(source):
    from trading_runtime.strategy_runtime_eligibility import strategy_runtime_eligibility
    from trading_runtime.portfolio_runtime import _normalize_plugin
    supported = {"moving_average_cross", "rsi_revert", "breakout", "threshold_momentum", "strategy_builder_custom"}
    if _normalize_plugin(source.get("plugin") or source.get("pluginId") or "") not in supported:
        raise ValueError("지원하지 않는 실시간 전략 플러그인입니다.")
    reference = artifact_reference(source)
    return strategy_runtime_eligibility(source, reference["artifactHash"])


def _broker(source, symbol):
    from trading_runtime.strategy_source_binding import resolve_strategy_source_route
    return resolve_strategy_source_route(source)["brokerId"]


def _member(source, instance_id, symbol, timeframe, broker, parameters):
    eligibility = source_eligibility(source)
    if eligibility.get("liveExportAllowed") is not True:
        raise ValueError("실제 데이터 기반으로 실행 가능한 전략만 Live에 연결할 수 있습니다: " + ", ".join(eligibility.get("blockers") or []))
    if not instance_id or not symbol or not timeframe or not broker:
        raise ValueError("전략 실행 단위의 종목·주기·경로가 필요합니다.")
    return {"strategyId": artifact_reference(source)["artifactId"],
            "strategyInstanceId": instance_id, "symbol": symbol, "timeframe": timeframe,
            "brokerId": broker, "strategyArtifactReference": artifact_reference(source),
            "parametersHash": stable_sha256(parameters), "pluginId": str(source.get("plugin") or source.get("pluginId") or "")}


def _catalog(root):
    sources = files._catalog(root, "strategy")
    rows = []
    for items in sources.values():
        if len(items) != 1 or not items[0]:
            continue
        source = items[0]
        try:
            ref = artifact_reference(source)
            symbol = str(source.get("symbol") or (source.get("dataset") or {}).get("symbol") or "")
            timeframe = str(source.get("timeframe") or (source.get("dataset") or {}).get("timeframe") or (source.get("dataset") or {}).get("interval") or "")
            member = _member(source, "standalone:" + ref["artifactId"], symbol, timeframe, _broker(source, symbol), canonical_source_parameters(source))
            rows.append({"candidateId": "strategy:" + ref["artifactId"], "name": source.get("name") or ref["artifactId"],
                         "strategy": source, "portfolio": None, "members": [member]})
        except (ValueError, KeyError, TypeError):
            continue
    for items in files._catalog(root, "portfolio").values():
        if len(items) != 1 or not items[0]:
            continue
        portfolio = items[0]
        try:
            from trading_runtime.lifecycle import resolve_artifact_lifecycle
            stage = resolve_artifact_lifecycle(portfolio)
            if stage["status"] in {"paused", "retired", "archived", "draft", "unknown"} or stage.get("conflicts"):
                raise ValueError("Portfolio inactive")
            ref = artifact_reference(portfolio)
            runtime = load_portfolio_runtime(root, ref["artifactId"])
            members = []
            originals = []
            for spec in runtime.specs:
                # Runtime spec DTO adds routing fields; always verify its canonical source.
                original = files._exact_artifact(sources, spec.strategy_id, spec.artifact_hash)
                originals.append(original)
                members.append(_member(original, spec.strategy_instance_id, spec.symbol, spec.timeframe, spec.broker_id, spec.parameters))
            if not members or len({item["brokerId"] for item in members}) != 1:
                continue  # Account-scoped normal preflight cannot seal mixed-broker portfolios.
            rows.append({"candidateId": "portfolio:" + ref["artifactId"], "name": portfolio.get("name") or ref["artifactId"],
                         "strategy": originals[0], "portfolio": portfolio, "members": members})
        except (ValueError, KeyError, TypeError, OSError):
            continue
    for row in rows:
        row["identity"] = {"schemaVersion": SCHEMA,
                           "strategyArtifact": artifact_reference(row["strategy"]),
                           "portfolioArtifact": artifact_reference(row["portfolio"]) if row["portfolio"] else {},
                           "members": row["members"]}
        row["identityHash"] = stable_sha256(row["identity"])
    return rows


def list_candidates(*, roots=None, settings=None):
    candidates = []
    for root in roots if roots is not None else files.configured_artifact_roots():
        root = Path(root).resolve()
        if not root.exists():
            continue
        registry = files._registry(root)
        for row in _catalog(root):
            candidates.append({"rootKey": stable_sha256(str(root)), "candidateId": row["candidateId"],
                "identityHash": row["identityHash"], "registryHash": stable_sha256(registry),
                "name": row["name"], "members": row["members"], "brokerId": row["members"][0]["brokerId"],
                "paperRequired": False})
    return {"ok": True, "candidates": candidates, "accounts": saved_account_profiles(settings), "ordersSubmitted": 0}


def register(request, *, roots=None, settings=None, controls=None):
    if not isinstance(request, dict) or set(request) != FIELDS:
        raise ValueError("전략·계좌 연결 요청 형식을 다시 확인하세요.")
    percent = request["capitalPercent"]
    if type(percent) not in (int, float) or not math.isfinite(percent) or not 0 < percent <= 100:
        raise ValueError("가용 현금 사용 비율은 0% 초과 100% 이하로 입력하세요.")
    roots = files.configured_artifact_roots() if roots is None else roots
    matches = list(dict.fromkeys(Path(root).resolve() for root in roots if stable_sha256(str(Path(root).resolve())) == request["rootKey"]))
    if len(matches) != 1:
        raise ValueError("전략 저장 위치를 확인할 수 없습니다.")
    root = matches[0]
    store = DeploymentStore(root)
    with store.locked():
        registry = store.registry_snapshot()
        if stable_sha256(registry) != request["registryHash"]:
            raise ValueError("배포 목록이 변경됐습니다. 새로고침하세요.")
        choices = [row for row in _catalog(root) if row["candidateId"] == request["candidateId"] and row["identityHash"] == request["identityHash"]]
        if len(choices) != 1:
            raise ValueError("선택 후 전략 또는 포트폴리오가 바뀌었습니다.")
        row = choices[0]
        broker = row["members"][0]["brokerId"]
        accounts = [item for item in saved_account_profiles(settings) if item["profileId"] == request["accountProfileId"] and item["brokerId"] == broker]
        if len(accounts) != 1:
            raise ValueError("전략 거래소와 일치하는 저장 계좌를 선택하세요.")
        account = accounts[0]
        binding = {**row["identity"], "accountProfileId": account["profileId"],
            "accountFingerprint": account["accountFingerprint"], "credentialConfigurationHash": account["credentialConfigurationHash"],
            "accountBindingId": account["accountBindingId"], "brokerId": broker,
            "capitalPolicy": {"basis": "ACCOUNT_AVAILABLE_CASH", "percent": float(percent), "balanceQueriedAtReview": False}}
        binding["bindingHash"] = stable_sha256(binding)
        deployment_id = "direct:" + binding["bindingHash"][:32]
        previous = store.get(deployment_id)
        if previous is not None:
            raise ValueError("동일한 전략·계좌 설정이 이미 연결되어 있습니다. 운용 전략에서 선택하세요.")
        from .paper_candidate_review import _review_execution_policies
        policies = _review_execution_policies(root, row["strategy"], row["portfolio"], account, controls or {})
        permissions = {**policies, "directExecutionBinding": binding,
            "liveAccountProfileId": account["profileId"], "liveAccountFingerprint": account["accountFingerprint"],
            "liveAccountCredentialConfigurationHash": account["credentialConfigurationHash"],
            "liveAccountAlias": account["alias"] + " " + account["maskedAccount"], "liveCapitalPolicy": binding["capitalPolicy"],
            "live_small_eligible": True, "live_eligible": True, "live_allowed": True,
            "paper_trader_verified": False, "fail_reasons": []}
        first = row["members"][0]
        store.create_definition(deployment_id=deployment_id, strategy_artifact=row["strategy"], portfolio_artifact=row["portfolio"],
            account_id=account["accountBindingId"], environment="LIVE", symbol=first["symbol"], route=broker,
            expected_registry_hash=request["registryHash"], permissions=permissions)
        saved = store.get(deployment_id)
        saved = store.transition(deployment_id, lifecycle="backtested", mode="MONITOR", actor="live-direct-strategy",
            reason="Independent execution configuration; no Paper promotion or runtime start", permissions=permissions,
            expected_revision=saved["revision"])
        return {"ok": True, "deploymentId": deployment_id, "strategyId": first["strategyId"], "mode": "MONITOR",
                "paperRequired": False, "authorizationGranted": False, "realOrdersArmed": False, "brokerCalls": 0, "ordersSubmitted": 0,
                "detail": "전략과 계좌를 연결했습니다. 시작 점검 후 실거래 시작을 직접 눌러주세요."}


def assess(root, source, deployment, *, settings=None):
    """Recompute source/account identity; a stored ready flag is never trusted."""
    binding = (deployment.get("permissions") or {}).get("directExecutionBinding")
    if not isinstance(binding, dict) or binding.get("schemaVersion") != SCHEMA:
        return {"required": False, "ready": False, "issues": []}
    result = {"required": True, "ready": False, "issues": [], "binding": binding}
    try:
        if binding.get("bindingHash") != stable_sha256({key: value for key, value in binding.items() if key != "bindingHash"}):
            raise ValueError("direct-binding-hash-mismatch")
        if deployment.get("lifecycle") in {"paused", "retired"}:
            raise ValueError("direct-deployment-inactive")
        ref = artifact_reference(source)
        if ref != binding.get("strategyArtifact") or ref != deployment.get("strategyArtifact"):
            raise ValueError("direct-source-hash-mismatch")
        expected = {key: binding[key] for key in ("schemaVersion", "strategyArtifact", "portfolioArtifact", "members")}
        if len([row for row in _catalog(Path(root)) if row["identity"] == expected]) != 1:
            raise ValueError("direct-canonical-members-changed")
        accounts = [item for item in saved_account_profiles(settings) if all(item.get(key) == binding.get(key) for key in ("accountFingerprint", "credentialConfigurationHash", "accountBindingId", "brokerId")) and item["profileId"] == binding.get("accountProfileId")]
        if len(accounts) != 1 or deployment.get("accountId") != binding.get("accountBindingId"):
            raise ValueError("direct-account-binding-changed")
        result["ready"] = True
    except (ValueError, KeyError, TypeError, OSError) as exc:
        result["issues"] = [str(exc)]
    return result


def normalized_ready(strategy):
    result = strategy.get("direct_execution") if isinstance(strategy, dict) else None
    return isinstance(result, dict) and result.get("required") is True and result.get("ready") is True


def manifest_members(strategy):
    if not normalized_ready(strategy):
        raise ValueError("direct-execution-not-ready")
    binding = strategy["direct_execution"]["binding"]
    return binding.get("portfolioArtifact", {}).get("artifactId", ""), binding.get("portfolioArtifact", {}), [
        {**member, "artifactHash": stable_sha256({"strategyId": member["strategyId"], "artifactId": member["strategyArtifactReference"]["artifactId"], "artifactHash": member["strategyArtifactReference"]["artifactHash"], "contentHash": member["strategyArtifactReference"].get("contentHash", ""), "lifecycle": strategy.get("lifecycle_status", "backtested")}),
         "directExecutionBindingHash": binding["bindingHash"]} for member in binding["members"]]


def validate_manifest(metadata):
    """Validate the exact members, source, account and registered deployment together."""
    try:
        bindings = metadata.get("directBindings")
        if not isinstance(bindings, list) or len(bindings) != 1:
            return False
        binding = bindings[0]
        if (binding.get("schemaVersion") != SCHEMA
            or binding.get("bindingHash") != stable_sha256({key: value for key, value in binding.items() if key != "bindingHash"})
            or metadata.get("directBindingsHash") != stable_sha256(bindings)):
            return False
        members = metadata.get("strategyMembers")
        expected = binding.get("members")
        if not isinstance(members, list) or not expected or len(members) != len(expected):
            return False
        for current, original in zip(members, expected):
            expected_artifact_hash = stable_sha256({"strategyId": original["strategyId"],
                "artifactId": original["strategyArtifactReference"]["artifactId"],
                "artifactHash": original["strategyArtifactReference"]["artifactHash"],
                "contentHash": original["strategyArtifactReference"].get("contentHash", ""), "lifecycle": "backtested"})
            if (any(current.get(key) != value for key, value in original.items())
                or current.get("directExecutionBindingHash") != binding["bindingHash"]
                or current.get("artifactHash") != expected_artifact_hash):
                return False
        deployment = metadata.get("deploymentBinding") or {}
        deployment_portfolio = deployment.get("portfolioArtifact") or {}
        if not any(deployment_portfolio.values()):
            deployment_portfolio = {}
        return (deployment.get("directExecutionBindingHash") == binding["bindingHash"]
            and deployment.get("accountId") == binding["accountBindingId"]
            and deployment.get("strategyArtifact") == binding["strategyArtifact"]
            and deployment_portfolio == binding["portfolioArtifact"])
    except (KeyError, TypeError, ValueError):
        return False


def binding_covers_spec(row, spec):
    if not normalized_ready(row):
        return False
    binding = row["direct_execution"]["binding"]
    portfolio = binding.get("portfolioArtifact") or {}
    if portfolio:
        if (spec.portfolio_id, spec.portfolio_hash) != (portfolio["artifactId"], portfolio["artifactHash"]):
            return False
    elif not (isinstance(spec.artifact, dict) and spec.artifact.get("standalone") is True):
        return False
    return any((member["strategyId"], member["strategyInstanceId"], member["symbol"], member["timeframe"], member["brokerId"], member["strategyArtifactReference"]["artifactHash"], member["parametersHash"])
        == (spec.strategy_id, spec.strategy_instance_id, spec.symbol, spec.timeframe, spec.broker_id, spec.artifact_hash, stable_sha256(spec.parameters))
        for member in binding["members"])
