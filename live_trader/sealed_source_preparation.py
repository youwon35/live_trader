"""Exact sealed research sources for information-only preparation; never Live authority."""
from __future__ import annotations
import hashlib,json,re,os
from pathlib import Path
from typing import Any
from trading_runtime.artifact_governance import (stable_sha256,assert_verified_strategy_artifact,assert_verified_strategy_instance,assert_verified_portfolio_artifact)
from trading_runtime.promotion_policy import validate_paper_promotion_policy
from trading_runtime.portfolio_runtime import load_portfolio_runtime

SCHEMA="live-sealed-source-preparation-manifest-v1"
FILE="sealed-source-preparation.json"
_FALSE_FLAGS={"liveAuthorized":False,"authorizationGranted":False,"promotionEligible":False,"useAsPromotionEvidence":False,"orderSubmissionEnabled":False,"armingEnabled":False}

def _json(path:Path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size>16*1024*1024:raise ValueError("Preparation source file is unavailable")
    return json.loads(path.read_text(encoding="utf-8-sig"))

def _digest(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()

def _exact(root:Path,kind:str,identity:str,digest:str):
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,200}",identity) or not re.fullmatch(r"[a-f0-9]{64}",digest):raise ValueError("Invalid sealed source identity")
    folder=root/{"strategy":"","instance":"strategy-instances","portfolio":"portfolios"}[kind]
    validator={"strategy":assert_verified_strategy_artifact,"instance":assert_verified_strategy_instance,"portfolio":assert_verified_portfolio_artifact}[kind]
    found=[]
    for path in folder.glob(identity+"*.json"):
        if path.resolve().parent!=folder.resolve():continue
        try:
            value=_json(path)
            value_id=value.get("instanceId") if kind=="instance" else value.get("id")
            if value_id==identity and validator(value)==digest:found.append((path,value))
        except (ValueError,KeyError,TypeError,OSError):continue
    if len(found)!=1:raise ValueError("Sealed source is missing, ambiguous, or changed")
    return found[0]

def _approval(path:Path,identity:str):
    records=_json(path).get("records",[])
    if isinstance(records,dict):records=list(records.values())
    matched=[row for row in records if isinstance(row,dict) and row.get("strategy_id")==identity]
    if len(matched)!=1 or matched[0].get("stage")!="PAPER":raise ValueError("Current Paper source approval is unavailable or revoked")
    row=matched[0];policy=validate_paper_promotion_policy(row.get("promotionPolicy"),require_promotion=True)
    return row,policy

def _data_receipt(source:dict,data_root:Path):
    snapshot=source.get("dataArtifact") or {}
    if snapshot.get("publicationStatus")!="PUBLISHED" or snapshot.get("immutable") is not True or snapshot.get("integrityStatus")!="VERIFIED":raise ValueError("Source data is not verified PUBLISHED")
    relative=Path(str(snapshot.get("objectPath") or ""));actual=(data_root/relative).resolve()
    if relative.is_absolute() or not actual.is_relative_to(data_root.resolve()) or not actual.is_file() or _digest(actual)!=snapshot.get("contentSha256"):raise ValueError("PUBLISHED source data SHA differs")
    return {"snapshotHash":stable_sha256(snapshot),"objectSha256":snapshot["contentSha256"],"versionId":str(snapshot.get("versionId") or ""),"datasetHash":str(snapshot.get("datasetHash") or ""),"publicationStatus":"PUBLISHED","integrityStatus":"VERIFIED"}

def _source_binding(root:Path,data_root:Path,approval_path:Path,strategy_id:str,strategy_hash:str):
    _,source=_exact(root,"strategy",strategy_id,strategy_hash);approval,policy=_approval(approval_path,strategy_id)
    recorded=str(approval.get("sourceArtifactHash") or approval.get("artifactHash") or "")
    if recorded and recorded!=strategy_hash:raise ValueError("Paper approval source hash differs")
    return {"strategyId":strategy_id,"strategyHash":strategy_hash,"approvalRecordHash":stable_sha256(approval),"paperPolicyHash":policy["policyHash"],"approvalSourceHashRecorded":bool(recorded),"dataReceipt":_data_receipt(source,data_root)}

def _instrument(spec,source_binding,instance_hash:str, *, allocation_weight=1.0, execution_policy=None):
    symbol=str(spec.symbol).upper();broker=str(spec.broker_id).lower();lane=("kis-kr" if re.fullmatch(r"[0-9]{6}(?:\.K[QS])?",symbol) else "kis-us") if broker=="kis" else broker
    if lane=="kis-kr":symbol=symbol.split(".")[0]
    if lane not in {"kis-kr","kis-us","binance","upbit"}:raise ValueError("Source is outside the selected cash/Spot scope")
    percent=spec.parameters.get("positionSize")
    if isinstance(percent,bool) or not isinstance(percent,(int,float)) or not 0<float(percent)<=100:raise ValueError("Source order percentage is missing")
    return {"instanceId":spec.strategy_instance_id,"instanceHash":instance_hash,"symbol":symbol,"provider":spec.provider,"broker":lane,"timeframe":spec.timeframe,"sourceMaxOrderPercent":float(percent),"allocationWeight":allocation_weight,"executionPolicy":execution_policy or {},"parametersHash":stable_sha256(spec.parameters),**source_binding}

def target_snapshot(manifest:dict,target:dict):
    original_root=Path(manifest["artifactRoot"]);original_data=Path(manifest["dataRoot"])
    root=original_root.resolve();data_root=original_data.resolve();approval_path=Path(manifest["approvalPath"]).resolve()
    if original_root.is_symlink() or original_data.is_symlink():raise ValueError("Preparation roots must be ordinary folders")
    instruments=[]
    if target["targetType"]=="INSTANCE":
        _,instance=_exact(root,"instance",target["targetId"],target["targetHash"])
        binding=_source_binding(root,data_root,approval_path,instance["sourceStrategyId"],instance["sourceArtifactHash"])
        if instance.get("historicalSourceSnapshot")!=_exact(root,"strategy",binding["strategyId"],binding["strategyHash"])[1].get("dataArtifact"):raise ValueError("Instance historical receipt differs")
        from types import SimpleNamespace
        spec=SimpleNamespace(strategy_instance_id=instance["instanceId"],symbol=instance.get("qualifiedSymbol"),provider=instance.get("marketDataProvider"),broker_id=instance.get("brokerId"),timeframe=instance.get("qualifiedTimeframe"),parameters=instance.get("parameters") or {})
        source=_exact(root,"strategy",binding["strategyId"],binding["strategyHash"])[1]
        if spec.parameters!=source.get("parameters") or spec.timeframe!=source.get("timeframe"):
            raise ValueError("Instance scientific parameters/timeframe differ from approved Source")
        instrument = _instrument(spec,binding,target["targetHash"],execution_policy=source.get("executionPolicy") or {"feePercent":spec.parameters.get("fee"),"slippagePercent":spec.parameters.get("slippage"),"executionTiming":spec.parameters.get("executionTiming")})
        if (source.get("executionSizing") or {}).get("quantityContract") is not None:
            instrument["executionSizing"] = source["executionSizing"]
        instruments.append(instrument)
    elif target["targetType"]=="PORTFOLIO":
        _,portfolio=_exact(root,"portfolio",target["targetId"],target["targetHash"]);loaded=load_portfolio_runtime(root,target["targetId"])
        if loaded.portfolio_hash!=target["targetHash"]:raise ValueError("Portfolio runtime identity differs")
        members={row["instanceId"]:row for row in portfolio["strategyInstances"]}
        total_weight=sum(spec.target_weight for spec in loaded.specs)
        if total_weight<=0:raise ValueError("Portfolio allocation is unavailable")
        for spec in loaded.specs:
            member=members[spec.strategy_instance_id];digest=str(member.get("sourceInstanceHash") or "")
            templates=portfolio.get("executionEdition",{}).get("memberMappings",[])
            matching=[row["newInstance"] for row in templates if row.get("newInstance",{}).get("instanceLock",{}).get("artifactHash")==digest]
            if len(matching)!=1 or assert_verified_strategy_instance(matching[0])!=digest:raise ValueError("Portfolio member instance seal differs")
            binding=_source_binding(root,data_root,approval_path,spec.strategy_id,spec.artifact_hash)
            instrument = _instrument(spec,binding,digest,allocation_weight=spec.target_weight/total_weight,execution_policy=portfolio.get("executionPolicy") or {})
            source = _exact(root,"strategy",binding["strategyId"],binding["strategyHash"])[1]
            if (source.get("executionSizing") or {}).get("quantityContract") is not None:
                instrument["executionSizing"] = source["executionSizing"]
            instruments.append(instrument)
    else:raise ValueError("Unsupported preparation target")
    body={"targetType":target["targetType"],"targetId":target["targetId"],"targetHash":target["targetHash"],"instruments":instruments}
    return {**body,"identityHash":stable_sha256(body),"paperStage":"PAPER_APPROVED","proofPending":True,"qualification":"PAPER_EVIDENCE_PENDING","approvalBindingStatus":"SOURCE_HASH_RECORDED" if all(row["approvalSourceHashRecorded"] for row in instruments) else "HISTORICAL_APPROVAL_SOURCE_HASH_NOT_RECORDED",**_FALSE_FLAGS}

def register_sources(recipe_path:Path,publication_path:Path,approval_path:Path,output_base:Path):
    recipe=_json(recipe_path);publication=_json(publication_path)
    if publication.get("status")!="PASS" or publication.get("approvedSourceCount")!=len(recipe.get("strategies",[])):raise ValueError("Canonical Paper publication receipt is incomplete")
    root=Path(publication["artifactRoot"]).resolve();manifest={"schemaVersion":SCHEMA,"artifactRoot":str(root),"dataRoot":str(Path(recipe["dataRoot"]).resolve()),"approvalPath":str(approval_path.resolve()),"recipeFileSha256":_digest(recipe_path),"publicationFileSha256":_digest(publication_path),"targets":[],**_FALSE_FLAGS}
    for kind,key in (("INSTANCE","instances"),("PORTFOLIO","portfolios")):
        for row in recipe.get(key,[]):
            target={"targetType":kind,"targetId":row["id"],"targetHash":row["artifactHash"]};current=target_snapshot(manifest,target);manifest["targets"].append({**target,"identityHash":current["identityHash"]})
    if not manifest["targets"]:raise ValueError("No exact preparation sources")
    manifest["contentHash"]=stable_sha256(manifest);folder=output_base.resolve()/manifest["contentHash"];folder.mkdir(parents=True,exist_ok=True);path=folder/FILE
    if path.exists() and _json(path)!=manifest:raise ValueError("Existing immutable preparation registration differs")
    if not path.exists():path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return {"ok":True,"root":str(folder),"sourceCount":len(manifest["targets"]),"manifestHash":manifest["contentHash"],**_FALSE_FLAGS}

def _manifest(root:Path):
    manifest=_json(root/FILE);digest=manifest.pop("contentHash","")
    if manifest.get("schemaVersion")!=SCHEMA or stable_sha256(manifest)!=digest or root.name!=digest or any(manifest.get(key) is not value for key,value in _FALSE_FLAGS.items()):raise ValueError("Preparation registration integrity differs")
    return {**manifest,"contentHash":digest}

def list_sources(root:Path):
    manifest=_manifest(root);rows=[]
    for target in manifest["targets"]:
        current=target_snapshot(manifest,target)
        if current["identityHash"]!=target["identityHash"]:raise ValueError("Source, approval or policy changed since registration")
        request={"kind":"SEALED_SOURCE","rootKey":stable_sha256(str(root.resolve())),"targetType":target["targetType"],"targetId":target["targetId"],"targetHash":target["targetHash"],"identityHash":target["identityHash"]}
        rows.append({"name":target["targetId"]+" · Paper 근거 대기","source":{"kind":"SEALED_SOURCE","evidenceClass":"PAPER_APPROVED_EVIDENCE_PENDING",**current},"request":request})
    return rows

def source_snapshot(request:dict,root:Path):
    expected={"kind","rootKey","targetType","targetId","targetHash","identityHash"}
    if set(request)!=expected or request["kind"]!="SEALED_SOURCE" or request["rootKey"]!=stable_sha256(str(root.resolve())):raise ValueError("Invalid sealed source request")
    manifest=_manifest(root)
    targets=[row for row in manifest["targets"] if all(row.get(key)==request[key] for key in ("targetType","targetId","targetHash","identityHash"))]
    if len(targets)!=1:raise ValueError("Exact preparation source changed")
    current=target_snapshot(manifest,targets[0])
    if current["identityHash"]!=targets[0]["identityHash"]:raise ValueError("Source, approval or policy changed since registration")
    return {"kind":"SEALED_SOURCE","evidenceClass":"PAPER_APPROVED_EVIDENCE_PENDING",**current}
