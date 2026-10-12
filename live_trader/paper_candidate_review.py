"""Explicit Paper candidate review: bind saved account, prepare limited-live.

No balance request, token request, order, runtime or real-order arm occurs here.
The existing native HTTP session and one-use SafetyConfirmationStore authorize
only this exact immutable successor, not its later broker execution.
"""
from __future__ import annotations
import math
from pathlib import Path
from typing import Any, Mapping, Sequence
from trading_runtime.artifact_governance import DeploymentStore, stable_sha256
from trading_runtime.portfolio_runtime import infer_market_route
from . import paper_candidate_inbox as inbox
from .account_profiles import saved_account_profiles

ACTION = "PAPER_CANDIDATE_REVIEW"
FIELDS = frozenset({"rootKey", "evidenceId", "deploymentId", "identityHash", "expectedRevision", "registryHash", "accountProfileId", "capitalBasis", "capitalPercent"})


def candidate_broker(strategy: Mapping[str, Any], instance: Mapping[str, Any]) -> str:
    trader = strategy.get("traderContract") or strategy.get("trader_contract") or {}
    symbol = str(instance.get("qualifiedSymbol") or instance.get("symbol") or strategy.get("symbol") or "")
    _, inferred, _ = infer_market_route(symbol)
    return str(instance.get("brokerId") or strategy.get("brokerId") or strategy.get("broker_id") or trader.get("brokerId") or inferred).strip().lower()


def _source(request, roots):
    if not isinstance(request, dict) or set(request) != FIELDS:
        raise ValueError("후보 계좌 심사 요청 형식이 올바르지 않습니다. 새로고침하세요.")
    if type(request["expectedRevision"]) is not int or request["expectedRevision"] < 1:
        raise ValueError("등록한 후보의 정확한 revision이 필요합니다.")
    for key in FIELDS - {"expectedRevision", "capitalPercent"}:
        if not isinstance(request[key], str) or not request[key]:
            raise ValueError("후보와 저장 계좌의 정확한 연결 정보가 필요합니다.")
    percent = request["capitalPercent"]
    if type(percent) not in (int, float) or not math.isfinite(percent) or not 0 < percent <= 100 or request["capitalBasis"] != "ACCOUNT_AVAILABLE_CASH":
        raise ValueError("총사용한도는 현재 주문가능액의 0% 초과 100% 이하로 설정하세요.")
    folders = inbox.configured_artifact_roots() if roots is None else roots
    matches = list(dict.fromkeys(Path(root).resolve() for root in folders if stable_sha256(str(Path(root).resolve())) == request["rootKey"]))
    if len(matches) != 1:
        raise ValueError("설정된 후보 저장소를 하나로 확인하지 못했습니다.")
    return matches[0]


def _plan(root, request, registry, settings, controls):
    catalogs = {kind: inbox._catalog(root, kind) for kind in ("strategy", "portfolio")}
    catalogs["instance"] = inbox._instance_catalog(root)
    matches = []
    for path in inbox._files(root / "evidence" / "paper"):
        evidence = inbox._read(path, root)
        if evidence.get("evidenceId") == request["evidenceId"]:
            matches.append((inbox._candidate(root, evidence, registry, catalogs), evidence))
    if len(matches) != 1:
        raise ValueError("선택한 Paper 발행 근거가 없거나 중복되었습니다.")
    row, evidence = matches[0]
    identity = row["identity"]
    prior = registry["entries"].get(request["deploymentId"], {})
    if (row["deployment"]["deploymentId"] != request["deploymentId"]
        or stable_sha256({"identity": identity, "instanceHash": row["instanceHash"]}) != request["identityHash"]
        or stable_sha256(registry) != request["registryHash"]
        or prior.get("revision") != request["expectedRevision"]
        or row.get("registered") is not True or row.get("canReview") is not True):
        raise ValueError("후보·저장본·등록부가 바뀌었거나 검토 대기 초안이 아닙니다. 새로고침하세요.")
    qualification = inbox.validate_paper_live_evidence(evidence).qualification
    scope = qualification.forward_scope
    strategy = inbox._exact_artifact(catalogs["strategy"], scope.strategy_artifact_id, scope.strategy_artifact_hash)
    if scope.portfolio_required:
        portfolio = inbox._exact_artifact(catalogs["portfolio"], scope.portfolio_artifact_id, scope.portfolio_artifact_hash)
        child = next(item for item in portfolio["strategyInstances"] if str(item.get("instanceId") or item.get("strategyInstanceId") or "") == scope.strategy_instance_id)
        instance, _ = inbox._exact_instance(catalogs["instance"], str(child.get("templateInstanceId") or child.get("instanceId") or ""), strategy)
    else:
        instance, _ = inbox._exact_instance(catalogs["instance"], scope.strategy_instance_id, strategy)
    broker = candidate_broker(strategy, instance)
    accounts = [account for account in saved_account_profiles(settings) if account["profileId"] == request["accountProfileId"] and account["brokerId"] == broker]
    if len(accounts) != 1:
        raise ValueError("전략 경로와 일치하는 저장 계좌가 없거나 계좌 설정이 바뀌었습니다.")
    account = accounts[0]
    capital = {"basis": "ACCOUNT_AVAILABLE_CASH", "percent": float(request["capitalPercent"]), "balanceQueriedAtReview": False}
    context = {"schemaVersion": "live-paper-candidate-review-context-v1", "action": ACTION,
        "request": dict(request), "paperIdentity": identity, "instanceHash": row["instanceHash"],
        "accountProfileId": account["profileId"], "accountFingerprint": account["accountFingerprint"],
        "credentialConfigurationHash": account["credentialConfigurationHash"], "capitalPolicy": capital,
        "controls": dict(controls or {})}
    context_hash = stable_sha256(context)
    successor_id = request["deploymentId"] + ":account:" + context_hash[:16]
    display = {"action": ACTION, "provider": account["alias"], "accountHint": account["maskedAccount"],
        "deploymentId": request["deploymentId"], "strategyId": row["strategyId"],
        "symbol": str(instance.get("qualifiedSymbol") or instance.get("symbol") or strategy.get("symbol") or ""),
        "validationProfile": (row.get("validationSummary") or {}).get("validationProfile") or "STANDARD",
        "actualObservationSeconds": (row.get("validationSummary") or {}).get("actualObservationSeconds"),
        "totalCapital": f"현재 계좌 주문가능액의 {capital['percent']:g}% (금액은 시작 점검에서 조회)",
        "orderSizing": str(strategy.get("order_quantity") or strategy.get("orderQuantity") or (instance.get("parameters") or {}).get("paperOrderQuantity") or (strategy.get("parameters") or {}).get("positionSize") or "봉인 전략의 주문 수량·위험 설정"),
        "execution": "계좌 연결과 제한 실거래 준비만 승인. 실주문 ON·시작 점검·운용 시작은 별도."}
    return row, account, capital, context, display, successor_id


def issue_review_confirmation(request, *, safety_store, roots: Sequence[Path] | None = None, settings=None, controls=None):
    root = _source(request, roots)
    store = DeploymentStore(root)
    with store.locked():
        plan = _plan(root, request, store.registry_snapshot(), settings, controls)
        _, account, _, context, display, _ = plan
        return safety_store.issue(action=ACTION, context=context,
            expected_phrase="LIVE " + account["maskedAccount"][-4:].upper(), display_context=display)


def approve_paper_candidate(request, confirmation, *, safety_store, roots: Sequence[Path] | None = None, settings=None, controls=None):
    root = _source(request, roots)
    store = DeploymentStore(root)
    with store.locked():
        row, account, capital, context, _, successor_id = _plan(root, request, store.registry_snapshot(), settings, controls)
        consumed = safety_store.consume(action=ACTION, context=context, confirmation=confirmation)
        if consumed.get("ok") is not True:
            return consumed
        permissions = {
            **inbox.candidate_pins(row["identity"]), "paper_candidate_imported": True,
            "paper_trader_verified": True, "live_small_eligible": True,
            "live_eligible": False, "live_allowed": False, "fail_reasons": [],
            "liveAccountProfileId": account["profileId"], "liveAccountFingerprint": account["accountFingerprint"],
            "liveAccountCredentialConfigurationHash": account["credentialConfigurationHash"],
            "liveAccountAlias": account["alias"] + " " + account["maskedAccount"],
            "liveCapitalPolicy": capital, "candidatePredecessorId": request["deploymentId"],
            "liveReviewBindingHash": stable_sha256(context),
        }
        entry = store.bind_live_candidate(request["deploymentId"], deployment_id=successor_id,
            account_id=account["accountBindingId"], route=account["brokerId"], permissions=permissions,
            expected_revision=request["expectedRevision"], expected_registry_hash=request["registryHash"])
        return {"ok": True, "schemaVersion": "live-paper-candidate-review-v1",
            "deploymentId": entry["deploymentId"], "strategyId": row["strategyId"],
            "lifecycle": entry["lifecycle"], "mode": entry["mode"], "revision": entry["revision"],
            "accountAlias": permissions["liveAccountAlias"], "capitalPolicy": capital,
            "liveSmallEligible": True, "authorizationGranted": False, "realOrdersArmed": False,
            "brokerCalls": 0, "ordersSubmitted": 0,
            "detail": "저장 계좌에 연결해 제한 실거래 준비로 전환했습니다. 현재 배포의 시작 점검과 실주문 확인·운용 시작이 필요합니다."}
