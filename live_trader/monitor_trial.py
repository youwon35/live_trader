"""Local close-sample wiring rehearsal. Never imports Live state, feeds or brokers.

Synthetic OHLC/cadence are an explicitly labelled input adapter for close-only MA
signals, not observed market bars. This report cannot qualify for live promotion.
"""
from __future__ import annotations
from collections import Counter
from datetime import timedelta
import json
import math
from pathlib import Path
import uuid
from . import paper_candidate_inbox as inbox
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.portfolio_runtime import load_portfolio_runtime, BuiltinBarSignalEvaluator, required_warmup_bars
from trading_runtime.continuous_runtime import PortfolioRuntimeEngine, ClosedBar, utc_now_text
from trading_runtime.market_data import parse_timestamp, timeframe_seconds

EVIDENCE_CLASS = "FUNCTIONAL_TEST_NON_PROMOTION"
SCHEMA = "live-local-monitor-trial-v1"
LIMITATIONS = [
    "실제 과거 종가 표본을 재생합니다. 실제 주문·체결·계좌 대조 시험이 아닙니다.",
    "표본에는 개별 봉 시각/OHLC/거래량이 없어 연속 가상 시각, 종가와 같은 OHLC, 거래량 0을 사용합니다.",
    "매수·매도 판단 연결을 위해 가상 보유 상태만 0/1로 바꿉니다. 주문과 수익률은 계산하지 않습니다.",
    "정규 승급 기준과 현재 운영 배포는 변경하지 않습니다. 실거래 자격을 부여하지 않습니다.",
]


def _flags(value, label):
    if value.get("evidenceClass") != EVIDENCE_CLASS or value.get("promotionEligible") is not False or value.get("useAsPromotionEvidence") is not False:
        raise ValueError(f"{label}: 명시적인 비승급 시험 저장본이 아닙니다.")
    if value.get("backtester_verified") is not True:
        raise ValueError(f"{label}: Backtester 확인 기록이 없습니다.")
    permissions = value.get("permissions")
    if not isinstance(permissions, dict) or any(permissions.get(key) is not False for key in ("live_allowed", "live_small_eligible", "live_eligible")):
        raise ValueError(f"{label}: 실거래 권한이 없는 시험 저장본만 허용합니다.")


def _load(root, portfolio_id, expected_hash):
    catalogs = {kind: inbox._catalog(root, kind) for kind in ("strategy", "portfolio")}
    portfolio = inbox._exact_artifact(catalogs["portfolio"], portfolio_id, expected_hash)
    _flags(portfolio, "포트폴리오")
    instances = inbox._instance_catalog(root)
    loaded = load_portfolio_runtime(root, portfolio_id)
    if loaded.portfolio_hash != expected_hash or not 1 <= len(loaded.specs) <= 32:
        raise ValueError("선택한 시험 구성의 hash 또는 종목 수가 다릅니다.")
    bindings = []
    streams = set()
    for spec in loaded.specs:
        if spec.stream_key in streams:
            raise ValueError("현재 연결 시험은 시세 경로마다 하나의 전략만 지원합니다.")
        streams.add(spec.stream_key)
        artifact = inbox._exact_artifact(catalogs["strategy"], spec.strategy_id, spec.artifact_hash)
        _flags(artifact, spec.symbol)
        instance, instance_hash = inbox._exact_instance(instances, spec.strategy_instance_id, artifact)
        trader = artifact.get("traderContract") or {}
        if trader.get("canPlaceOrders") is not False or spec.plugin_id != "moving_average_cross":
            raise ValueError(f"{spec.symbol}: 현재 종가 전용 연결 시험은 주문 권한 없는 이동평균 전략만 지원합니다.")
        raw = artifact.get("sample_prices")
        if not isinstance(raw, list) or not required_warmup_bars(spec) + 2 <= len(raw) <= 10000:
            raise ValueError(f"{spec.symbol}: 봉인된 종가 표본의 수가 부족하거나 한도를 넘었습니다.")
        if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0 for value in raw):
            raise ValueError(f"{spec.symbol}: 종가 표본에 잘못된 가격이 있습니다.")
        provenance = (artifact.get("dataArtifact") or {}).get("closedBarProvenance") or {}
        if provenance.get("confirmed") is not True or provenance.get("interval") != spec.timeframe:
            raise ValueError(f"{spec.symbol}: 봉인된 표본 기준 시각·주기를 확인할 수 없습니다.")
        final = parse_timestamp(str(provenance.get("finalBarEnd") or ""))
        updated = parse_timestamp(str(provenance.get("datasetUpdatedAt") or ""))
        if final > updated:
            raise ValueError(f"{spec.symbol}: 표본 기준 시각이 수집 시각보다 늦습니다.")
        bindings.append((spec, tuple(raw), final, {"strategyId":spec.strategy_id, "strategyHash":spec.artifact_hash,
            "instanceId":spec.strategy_instance_id, "instanceHash":instance_hash, "symbol":spec.symbol,
            "provider":spec.provider, "timeframe":spec.timeframe, "sampleCount":len(raw),
            "sampleHash":stable_sha256(raw), "sourceFinalBarEnd":provenance["finalBarEnd"]}))
    if sum(len(item[1]) for item in bindings) > 10000:
        raise ValueError("연결 시험은 전체 10,000개 종가 표본까지 허용합니다.")
    identity = {"portfolioId":loaded.portfolio_id,"portfolioHash":loaded.portfolio_hash,"bindings":[item[3] for item in bindings]}
    return loaded, bindings, identity


def list_monitor_trials(roots):
    rows=[]
    for root in dict.fromkeys(Path(path).resolve() for path in roots):
        if not root.exists():continue
        for portfolio_id, values in inbox._catalog(root,"portfolio").items():
            if not any(value.get("evidenceClass") == EVIDENCE_CLASS for value in values):continue
            row={"name":str(values[0].get("name") or portfolio_id),"portfolioId":portfolio_id,"canRun":False}
            try:
                digest=inbox.artifact_reference(values[0])["artifactHash"]
                loaded, bindings, identity = _load(root, portfolio_id, digest)
                row.update(canRun=True,detail=f"과거 종가 {sum(len(item[1]) for item in bindings):,}개 · {len(bindings)}종목 · 주문 없음",
                    request={"rootKey":stable_sha256(str(root)),"portfolioId":portfolio_id,"portfolioHash":digest,"identityHash":stable_sha256(identity)})
            except (ValueError,OSError,KeyError,TypeError,RuntimeError) as exc:row["detail"]=str(exc)
            rows.append(row)
    return rows


def run_monitor_trial(request, *, roots=None, report_root):
    fields={"rootKey","portfolioId","portfolioHash","identityHash"}
    if not isinstance(request,dict) or set(request)!=fields or any(not isinstance(request[key],str) or not request[key] for key in fields):
        raise ValueError("연결 시험 요청이 올바르지 않습니다. 목록을 다시 읽으세요.")
    folders = inbox.configured_artifact_roots() if roots is None else roots
    matches=list(dict.fromkeys(Path(root).resolve() for root in folders if stable_sha256(str(Path(root).resolve()))==request["rootKey"]))
    if len(matches)!=1:raise ValueError("설정된 시험 저장소를 하나로 확인하지 못했습니다.")
    root=matches[0]
    loaded, bindings, identity = _load(root,request["portfolioId"],request["portfolioHash"])
    if stable_sha256(identity)!=request["identityHash"]:raise ValueError("선택 후 시험 입력이 바뀌었습니다. 다시 선택하세요.")
    holdings={spec.strategy_instance_id:0 for spec in loaded.specs};decisions=[]
    def observe(cycle):
        for decision in cycle.decisions:
            decisions.append({"instanceId":decision.strategy_instance_id,"symbol":decision.bar.symbol,"signal":decision.signal,
                "reason":decision.reason,"evaluationKey":decision.evaluation_key,"replayTime":decision.bar.end_time})
            if decision.signal=="BUY":holdings[decision.strategy_instance_id]=1
            elif decision.signal=="SELL":holdings[decision.strategy_instance_id]=0
        return {"ok":True,"mode":"MONITOR","ordersSubmitted":0}
    engine=PortfolioRuntimeEngine(loaded.specs,mode="MONITOR",evaluator=BuiltinBarSignalEvaluator(lambda spec:holdings[spec.strategy_instance_id]),cycle_handler=observe)
    bars=[]
    for spec,prices,final,_ in bindings:
        seconds=timeframe_seconds(spec.timeframe)
        for index,price in enumerate(prices):
            end=final-timedelta(seconds=seconds*(len(prices)-index-1));start=end-timedelta(seconds=seconds)
            bars.append(ClosedBar(instrument_id=spec.instrument_id,symbol=spec.symbol,provider=spec.provider,timeframe=spec.timeframe,
                start_time=start.isoformat(),end_time=end.isoformat(),open=price,high=price,low=price,close=price,volume=0,
                received_time=end.isoformat(),source_sequence=f"local-close-sample-{index}",source_provider="local-replay"))
    for bar in sorted(bars,key=lambda item:(parse_timestamp(item.end_time),item.provider,item.instrument_id,item.timeframe)):
        engine.ingest_closed_bar(bar)
    _,_,after=_load(root,request["portfolioId"],request["portfolioHash"])
    if after!=identity:raise ValueError("시험 중 입력 저장본이 바뀌어 결과를 저장하지 않았습니다.")
    if engine.mode!="MONITOR" or len(decisions)!=len(bars):raise RuntimeError("관찰 전용 계산 수를 확인하지 못했습니다.")
    counts=Counter(item["signal"] for item in decisions)
    body={"schemaVersion":SCHEMA,"ok":True,"mode":"MONITOR","evidenceClass":EVIDENCE_CLASS,"historicalOnly":True,
        "promotionEligible":False,"useAsPromotionEvidence":False,"authorizationGranted":False,"tradingEnabled":False,
        "accountCalls":0,"ordersSubmitted":0,"currentDeploymentChanged":False,"createdAt":utc_now_text(),
        "source":identity,"barShape":"CLOSE_ONLY_FLAT_OHLC_ZERO_VOLUME","timestampMode":"SYNTHETIC_CONTIGUOUS_REPLAY_CADENCE",
        "actualPeriodStart":None,"actualPeriodEnd":None,"virtualPositionModel":"signal-only binary state, never account inventory",
        "summary":{"symbols":len(bindings),"sampleCount":len(bars),"decisionCount":len(decisions),"signals":{name:counts[name] for name in ("BUY","SELL","HOLD")}},
        "decisionsHash":stable_sha256(decisions),"decisions":decisions,"limitations":LIMITATIONS}
    body["reportHash"]=stable_sha256(body)
    output=Path(report_root)
    if output.is_symlink():raise ValueError("시험 결과 저장 경로는 링크일 수 없습니다.")
    output.mkdir(parents=True,exist_ok=True)
    target=output/f"monitor-trial-{uuid.uuid4().hex}.json"
    with target.open("x",encoding="utf-8") as stream:json.dump(body,stream,ensure_ascii=False,indent=2)
    return {**body,"reportPath":str(target)}
