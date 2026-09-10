"""Explicit, CAS-protected import of verified Paper metadata. No runtime hooks."""
from __future__ import annotations
from pathlib import Path
from typing import Any, Sequence
from . import paper_candidate_inbox as inbox
from trading_runtime.artifact_governance import DeploymentStore, stable_sha256, safe_file_token

FIELDS={"rootKey", "evidenceId", "identityHash", "expectedRevision", "registryHash"}


def import_paper_candidate(request: dict[str, Any], *, roots: Sequence[Path] | None = None):
    if not isinstance(request, dict) or set(request) != FIELDS or type(request["expectedRevision"]) is not int or request["expectedRevision"] < 0:
        raise ValueError("후보 등록 요청 형식이 올바르지 않습니다. 새로고침하세요.")
    if any(not isinstance(request[key],str) or not request[key] for key in FIELDS-{"expectedRevision"}):
        raise ValueError("후보의 정확한 봉인 정보가 필요합니다.")
    folders = roots if roots is not None else inbox.configured_artifact_roots()
    matches=[Path(root).resolve() for root in folders if stable_sha256(str(Path(root).resolve()))==request["rootKey"]]
    matches=list(dict.fromkeys(matches))
    if len(matches)!=1:raise ValueError("설정된 후보 저장소를 하나로 확인하지 못했습니다.")
    root=matches[0];store=DeploymentStore(root)
    with store.locked():
        registry=store.registry_snapshot()
        catalogs={kind:inbox._catalog(root,kind) for kind in ("strategy","portfolio")}
        catalogs["instance"]=inbox._instance_catalog(root)
        candidates=[]
        for path in inbox._files(root/"evidence"/"paper"):
            evidence=inbox._read(path,root)
            if evidence.get("evidenceId")==request["evidenceId"]:
                candidates.append((inbox._candidate(root,evidence,registry,catalogs),evidence))
        if len(candidates)!=1:raise ValueError("선택한 발행 근거가 없거나 중복되었습니다.")
        row,evidence=candidates[0]
        identity=row["identity"]
        identity_hash=stable_sha256({"identity":identity,"instanceHash":row["instanceHash"]})
        if identity_hash!=request["identityHash"]:raise ValueError("선택 후 전략·실행 단위·봉인 근거가 바뀌었습니다.")
        if row.get("registered") is True:
            return {"ok":True,"alreadyRegistered":True,"deploymentId":row["deployment"]["deploymentId"],"strategyId":row["strategyId"],"authorizationGranted":False,"detail":"같은 근거가 이미 등록되어 있습니다."}
        if row.get("canImport") is not True or row.get("importRequest")!=request:
            raise ValueError("후보 또는 Deployment가 바뀌었거나 검토 대기 상태가 아닙니다. 새로고침하세요.")
        scope=inbox.validate_paper_live_evidence(evidence).qualification.forward_scope
        strategy=inbox._exact_artifact(catalogs["strategy"],scope.strategy_artifact_id,scope.strategy_artifact_hash)
        portfolio=inbox._exact_artifact(catalogs["portfolio"],scope.portfolio_artifact_id,scope.portfolio_artifact_hash) if scope.portfolio_required else None
        permissions={**inbox.candidate_pins(identity), "paper_candidate_imported":True,
            "paper_trader_verified":True,"live_small_eligible":False,"live_eligible":False,"live_allowed":False,
            "fail_reasons":["live-candidate-review-required","account-binding-required"]}
        deployment_id=row["deployment"]["deploymentId"]
        if not deployment_id:
            deployment_id=f"dep:{scope.strategy_artifact_id}:{scope.portfolio_artifact_id or 'standalone'}:{row['instanceHash'][:12]}:live"
            store.create_definition(deployment_id=deployment_id,strategy_artifact=strategy,portfolio_artifact=portfolio,
                account_id="live-account-unresolved",environment="SMALL_LIVE",symbol=str(strategy.get("symbol") or "UNKNOWN"),
                instrument_id=str(strategy.get("symbol") or ""),route="paper-candidate",
                expected_revision=0,expected_registry_hash=request["registryHash"],permissions=permissions)
        else:
            store.transition(deployment_id,lifecycle="draft",mode="MONITOR",actor="live-paper-candidate-import",
                reason="Exact Paper evidence imported for review; no trading authorization",
                permissions=permissions,expected_revision=request["expectedRevision"],expected_registry_hash=request["registryHash"])
        saved=store.get(deployment_id)
        if saved["mode"]!="MONITOR" or saved["lifecycle"]!="draft" or saved["permissions"].get("live_allowed") is not False:
            raise RuntimeError("후보 등록 후 안전 상태를 확인하지 못했습니다.")
        return {"ok":True,"alreadyRegistered":False,"deploymentId":deployment_id,"strategyId":scope.strategy_artifact_id,
            "revision":saved["revision"],"authorizationGranted":False,"detail":"검토 대기 후보로 등록했습니다. 계좌 연결과 승인·실행은 별도입니다."}
