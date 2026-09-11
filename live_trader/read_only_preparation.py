"""Read-only preparation projection. This report cannot authorize or submit orders."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
import re
import os
from . import paper_candidate_inbox as inbox
from .read_only_account import ReadOnlyBrokerReader, runtime_settings
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.portfolio_runtime import load_portfolio_runtime
from trading_runtime.portfolio_runtime import infer_market_route

SCHEMA = "live-read-only-preparation-v1"
FLAGS = {"readOnly": True, "reportPurpose": "READ_ONLY_PREPARATION", "authorityGranted": False,
         "authorizationGranted": False, "executable": False, "promotionEligible": False,
         "useAsPromotionEvidence": False, "tradingEnabled": False, "ordersSubmitted": 0,
         "confirmationTokensCreated": 0, "permitsCreated": 0, "runtimeSessionsCreated": 0,
         "currentDeploymentChanged": False}


def _root(request, roots):
    matches = list(dict.fromkeys(Path(root).resolve() for root in roots
                   if stable_sha256(str(Path(root).resolve())) == request.get("rootKey")))
    if len(matches) != 1:
        raise ValueError("설정된 원본 저장소를 하나로 확인하지 못했습니다.")
    return matches[0]


def _instrument(instance_id, symbol, provider, broker):
    symbol = str(symbol).upper()
    lane = str(broker).lower()
    if lane == "kis":
        lane = "kis-kr" if re.fullmatch(r"\d{6}", symbol) else "kis-us"
    if lane in {"binance-usdm", "binance_futures"}:
        lane = "binance-futures"
    if lane == "binance-futures":
        symbol = symbol.removesuffix(".PERP").replace("-", "")
    if not re.fullmatch(r"[A-Z0-9.-]{1,32}", symbol):
        raise ValueError("원본의 종목 형식을 확인하지 못했습니다.")
    return {"instanceId": instance_id, "symbol": symbol, "provider": provider, "broker": lane}


def dedicated_preparation_roots():
    folders = []
    # Dedicated immutable source copies do not change the operational catalog.
    base = Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local") / "live_trader" / "preparation-sources"
    if base.exists() and not base.is_symlink():
        folders += [path for path in sorted(base.iterdir())[:100] if path.is_dir() and not path.is_symlink()
                    and re.fullmatch(r"[a-f0-9]{64}", path.name) and path.resolve().parent == base.resolve()]
    return list(dict.fromkeys(Path(path).resolve() for path in folders))


def preparation_roots(roots=None):
    if roots is not None:
        return list(roots)
    return list(dict.fromkeys([*inbox.configured_artifact_roots(), *dedicated_preparation_roots()]))


def preparation_sources_response():
    try:
        return {"ok": True, "schemaVersion": "live-readonly-preparation-sources-v1", **FLAGS,
                "sources": list_preparation_sources()}
    except (ValueError, OSError, RuntimeError, TypeError, KeyError):
        return {"ok": False, "schemaVersion": "live-readonly-preparation-sources-v1", **FLAGS,
                "sources": [], "reason": "준비 전용 저장소를 읽지 못했습니다."}


def _nonpromotion_source(root, portfolio_id, portfolio_hash):
    catalogs = {kind: inbox._catalog(root, kind) for kind in ("strategy", "portfolio")}
    portfolio = inbox._exact_artifact(catalogs["portfolio"], portfolio_id, portfolio_hash)
    def check_flags(value):
        permissions = value.get("permissions") or {}
        if value.get("evidenceClass") != "FUNCTIONAL_TEST_NON_PROMOTION" or value.get("promotionEligible") is not False or value.get("useAsPromotionEvidence") is not False or value.get("backtester_verified") is not True or any(
                permissions.get(key) is not False for key in ("live_allowed", "live_small_eligible", "live_eligible")):
            raise ValueError("명시적인 비승급 원본과 금지 권한을 확인하지 못했습니다.")
    check_flags(portfolio)
    loaded = load_portfolio_runtime(root, portfolio_id)
    if loaded.portfolio_hash != portfolio_hash or not 1 <= len(loaded.specs) <= 32:
        raise ValueError("현재 Portfolio 구성과 hash가 다릅니다.")
    instances = inbox._instance_catalog(root)
    bindings = []
    for spec in loaded.specs:
        artifact = inbox._exact_artifact(catalogs["strategy"], spec.strategy_id, spec.artifact_hash)
        check_flags(artifact)
        if (artifact.get("traderContract") or {}).get("canPlaceOrders") is not False:
            raise ValueError("비승급 원본의 주문 금지 계약을 확인하지 못했습니다.")
        instance, digest = inbox._exact_instance(instances, spec.strategy_instance_id, artifact)
        bindings.append({"instanceId": spec.strategy_instance_id, "instanceHash": digest,
                         "strategyId": spec.strategy_id, "strategyHash": spec.artifact_hash,
                         **_instrument(spec.strategy_instance_id, spec.symbol, spec.provider, spec.broker_id)})
    identity = {"portfolioId": loaded.portfolio_id, "portfolioHash": loaded.portfolio_hash, "bindings": bindings}
    return {"kind": "NON_PROMOTION", "evidenceClass": portfolio["evidenceClass"],
            "portfolioId": loaded.portfolio_id, "portfolioHash": loaded.portfolio_hash,
            "identityHash": stable_sha256(identity), "qualification": "NON_PROMOTION",
            "instruments": [_instrument(spec.strategy_instance_id, spec.symbol, spec.provider, spec.broker_id) for spec in loaded.specs]}


def list_preparation_sources(*, roots=None):
    rows = []
    for root in dedicated_preparation_roots() if roots is None else roots:
        root = Path(root).resolve()
        if not root.exists():
            continue
        for portfolio_id, values in inbox._catalog(root, "portfolio").items():
            if not any(value.get("evidenceClass") == "FUNCTIONAL_TEST_NON_PROMOTION" for value in values):
                continue
            try:
                digest = inbox.artifact_reference(values[0])["artifactHash"]
                source = _nonpromotion_source(root, portfolio_id, digest)
                rows.append({"name": str(values[0].get("name") or portfolio_id), "source": source,
                    "request": {"kind": "NON_PROMOTION", "rootKey": stable_sha256(str(root)),
                    "portfolioId": portfolio_id, "portfolioHash": digest, "identityHash": source["identityHash"]}})
            except (ValueError, OSError, KeyError, TypeError, RuntimeError):
                continue
    return rows


def source_snapshot(request, roots):
    if not isinstance(request, dict):
        raise ValueError("원본을 선택하세요.")
    kind = request.get("kind")
    fields = {"kind", "rootKey", "portfolioId", "portfolioHash", "identityHash"} if kind == "NON_PROMOTION" else {
        "kind", "rootKey", "evidenceId", "evidenceHash", "instanceHash"}
    if kind not in {"NON_PROMOTION", "PAPER"} or set(request) != fields or any(
            not isinstance(value, str) or not value or len(value) > 256 for value in request.values()):
        raise ValueError("원본 선택 요청이 올바르지 않습니다.")
    root = _root(request, roots)
    if kind == "NON_PROMOTION":
        source = _nonpromotion_source(root, request["portfolioId"], request["portfolioHash"])
        if source["identityHash"] != request["identityHash"]:
            raise ValueError("선택 이후 원본이 변경되었습니다. 다시 선택하세요.")
        return source
    response = inbox.list_paper_candidates(roots=[root])
    candidates = [row for row in response["candidates"] if row.get("status") == "VERIFIED_READ_ONLY"
                  and row.get("evidenceId") == request["evidenceId"]
                  and row.get("identity", {}).get("evidenceHash") == request["evidenceHash"]
                  and row.get("instanceHash") == request["instanceHash"]]
    if not response["ok"] or len(candidates) != 1:
        raise ValueError("현재 원본과 정확히 일치하는 Paper 검증 근거를 확인하지 못했습니다.")
    row = candidates[0]
    identity = row["identity"]
    strategy = inbox._exact_artifact(inbox._catalog(root, "strategy"), row["strategyId"], identity["strategyArtifactHash"])
    if row.get("portfolioId"):
        loaded = load_portfolio_runtime(root, row["portfolioId"])
        if loaded.portfolio_hash != identity["portfolioArtifactHash"]:
            raise ValueError("Paper Portfolio hash가 현재 구성과 다릅니다.")
        specs = [spec for spec in loaded.specs if spec.strategy_instance_id == identity["strategyInstanceId"]]
        if len(specs) != 1:
            raise ValueError("검증된 실행 단위가 하나로 확인되지 않습니다.")
        instruments = [_instrument(spec.strategy_instance_id, spec.symbol, spec.provider, spec.broker_id) for spec in specs]
    else:
        instance, _ = inbox._exact_instance(inbox._instance_catalog(root), identity["strategyInstanceId"], strategy)
        symbol = instance.get("qualifiedSymbol") or instance.get("symbol") or strategy.get("symbol")
        provider, broker, _ = infer_market_route(symbol)
        instruments = [_instrument(identity["strategyInstanceId"], symbol,
            instance.get("marketDataProvider") or provider, instance.get("brokerId") or broker)]
    return {"kind": kind, "evidenceClass": strategy.get("evidenceClass"), "evidenceId": row["evidenceId"],
            "evidenceHash": identity["evidenceHash"], "portfolioId": row.get("portfolioId", ""),
            "portfolioHash": identity.get("portfolioArtifactHash", ""), "identityHash": stable_sha256(identity),
            "qualification": "SEALED_PAPER_EVIDENCE_ONLY", "instruments": instruments}


def _draft(value):
    if not isinstance(value, dict) or set(value) - {"instanceId", "side", "quantity", "limitPrice"}:
        raise ValueError("주문 초안 입력 형식이 올바르지 않습니다.")
    if any(not isinstance(item, str) or len(item) > 128 for item in value.values()):
        raise ValueError("주문 초안은 제한된 길이의 문자열로 입력하세요.")
    result = {key: value.get(key, "").strip() for key in ("instanceId", "side", "quantity", "limitPrice")}
    missing = [key for key, item in result.items() if not item]
    numbers = {}
    if result["side"] and result["side"] not in {"BUY", "SELL"}:
        raise ValueError("매수 또는 매도를 직접 선택하세요.")
    for field in ("quantity", "limitPrice"):
        if not result[field]:
            numbers[field] = None
            continue
        # Reject scientific notation/overflow; exact decimal math only.
        if not re.fullmatch(r"(?:0|[1-9]\d{0,17})(?:\.\d{1,18})?", result[field]):
            raise ValueError("수량과 지정가는 양의 일반 숫자로 입력하세요.")
        number = Decimal(result[field])
        if number <= 0:
            raise ValueError("수량과 지정가는 0보다 커야 합니다.")
        numbers[field] = number
    with localcontext() as context:
        context.prec = 80
        amount = numbers["quantity"] * numbers["limitPrice"] if all(value is not None for value in numbers.values()) else None
    return {**result, "missingInputs": missing, "notional": format(amount, "f") if amount is not None else None}, numbers


def prepare_read_only(request, *, roots=None, reader=None, clock=None):
    if not isinstance(request, dict) or set(request) != {"source", "draft", "readAccount"} or type(request["readAccount"]) is not bool:
        raise ValueError("읽기 전용 준비 요청 형식이 올바르지 않습니다.")
    folders = preparation_roots(roots)
    before = source_snapshot(request["source"], folders)
    draft, numbers = _draft(request["draft"])
    matches = [item for item in before["instruments"] if item["instanceId"] == draft["instanceId"]]
    if draft["instanceId"] and len(matches) != 1:
        raise ValueError("원본에 포함된 종목을 직접 선택하세요.")
    observation = {"status": "NOT_REQUESTED", "account": "UNKNOWN", "openOrders": "UNKNOWN",
                   "orderability": "UNKNOWN", "fundsCheck": "UNKNOWN", "accountBinding": "NOT_VERIFIED"}
    if request["readAccount"]:
        if len(matches) != 1:
            raise ValueError("계좌 조회 전에 원본의 종목을 선택하세요.")
        selected = matches[0]
        reader = reader or ReadOnlyBrokerReader(runtime_settings())
        observation = reader.read(selected["broker"], selected["symbol"], side=draft["side"],
                                  quantity=numbers["quantity"], limit_price=numbers["limitPrice"])
    after = source_snapshot(request["source"], folders)
    if before != after:
        raise ValueError("조회 중 원본이 변경되어 결과를 폐기했습니다. 새로고침하세요.")
    now = clock() if clock else datetime.now(timezone.utc)
    labels = {"AVAILABLE": "조회 완료", "UNKNOWN": "미확인", "PRESENT": "미체결 있음",
              "NONE_OBSERVED": "조회 범위에서 없음", "SYMBOL_NONE_OBSERVED": "선택 종목에서 없음",
              "REGULAR_SYMBOL_NONE_ALGO_UNKNOWN": "선택 종목 일반 주문 없음 · 조건부 주문 미확인",
              "MARKET_AND_ACCOUNT_ENABLED": "시장 및 계좌 거래 표시 활성", "BLOCKED": "차단",
              "WITHIN_OBSERVED_BALANCE": "관측 잔액 이내", "INSUFFICIENT_OBSERVED_BALANCE": "관측 잔액 부족",
              "SELECTED_SYMBOL_ONLY": "선택 종목", "SELECTED_SYMBOL_WAIT_AND_WATCH": "선택 종목 대기·예약",
              "KIS_NASD_QUERY": "해외 NASD 조회", "KIS_DOMESTIC_ACCOUNT": "국내 계좌"}
    label = lambda value: labels.get(value, "미확인")
    checks = [
        {"code": "SOURCE_IDENTITY", "status": "PASS", "detail": "현재 원본 hash와 실행 단위가 선택 내용과 일치합니다."},
        {"code": "PROMOTION", "status": "BLOCKED" if before["kind"] == "NON_PROMOTION" else "UNKNOWN",
         "detail": "기능시험 원본은 정규 Paper 승급 및 실거래 자격이 아닙니다." if before["kind"] == "NON_PROMOTION" else "봉인 근거만 확인했습니다. 현재 승급 유효기간·권한은 기존 Live 검사에서 별도 확인해야 합니다."},
        {"code": "USER_DRAFT", "status": "BLOCKED" if draft["missingInputs"] else "PASS",
         "detail": "종목·매수/매도·수량·지정가를 직접 입력하세요." if draft["missingInputs"] else "사용자가 입력한 지정가 초안의 금액만 계산했습니다."},
        {"code": "ACCOUNT_READ", "status": "PASS" if observation["account"] == "AVAILABLE" else "UNKNOWN", "detail": "Live 설정 계좌 읽기: " + label(observation["account"])},
        {"code": "OPEN_ORDERS", "status": "PASS" if observation["openOrders"] in {"NONE_OBSERVED", "SYMBOL_NONE_OBSERVED"} else "BLOCKED" if observation["openOrders"] == "PRESENT" else "UNKNOWN", "detail": "미체결 조회: " + label(observation["openOrders"]) + " · 범위 " + label(observation.get("openOrdersScope"))},
        {"code": "ORDERABILITY", "status": "BLOCKED" if observation["orderability"] == "BLOCKED" else "UNKNOWN",
         "detail": "거래소 관측: " + label(observation["orderability"]) + " · 주문별 필터·수수료·리스크 검사는 미완료"},
        {"code": "OBSERVED_FUNDS", "status": "BLOCKED" if observation["fundsCheck"] == "INSUFFICIENT_OBSERVED_BALANCE" else "UNKNOWN", "detail": "입력 금액과 관측 잔액 비교: " + label(observation["fundsCheck"]) + " · 수수료/마진/잠금 변동은 별도 검사"},
        {"code": "ORDER_SPECIFIC_RISK", "status": "UNKNOWN", "detail": "API 키 주문 권한·LOT_SIZE/MIN_NOTIONAL·수수료·노출 한도·선물 마진 및 조건부 주문은 이 조회로 검증되지 않습니다."},
        {"code": "LIVE_AUTHORITY", "status": "BLOCKED", "detail": "계좌 바인딩·정규 승급·현재 위험 한도·Kill/모드·수동 확인은 기존 Live 절차에서 직접 확인해야 합니다. 이 보고서는 주문 권한을 만들지 않습니다."},
    ]
    body = {"ok": True, "schemaVersion": SCHEMA, **FLAGS, "asOf": now.isoformat(),
            "expiresAt": (now + timedelta(seconds=60)).isoformat(), "status": "READ_ONLY_INFORMATION",
            "source": before, "draft": draft, "observation": observation, "checks": checks,
            "limitations": ["조회 시점의 정보이며 실행 직전에 다시 확인해야 합니다.",
                "총 보유액을 주문 수량으로 자동 선택하지 않습니다. 미입력은 미입력으로 남습니다.",
                "별도 감독 기능시험의 미출시 HOLD는 이 화면으로 해제되지 않습니다."]}
    body["reportHash"] = stable_sha256(body)
    return body


def preparation_response(request, **kwargs):
    try:
        return prepare_read_only(request, **kwargs)
    except (ValueError, OSError, RuntimeError, TypeError, KeyError):
        # Artifact errors can contain source text; do not expose them from an account view.
        return {"ok": False, "schemaVersion": SCHEMA, **FLAGS,
                "reason": "원본·종목·입력 형식 또는 조회 결과를 확인하지 못했습니다. 새로고침 후 입력을 확인하세요."}
