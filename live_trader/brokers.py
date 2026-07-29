from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Literal

from .live_adapters import (
    BINANCE_FUTURES_TEST_ORDER_ENDPOINT,
    build_binance_account_request,
    build_binance_cancel_order_request,
    build_binance_futures_account_request,
    build_binance_futures_account_config_request,
    build_binance_futures_cancel_order_request,
    build_binance_futures_open_orders_request,
    build_binance_futures_order_status_request,
    build_binance_futures_order_request,
    build_binance_futures_position_mode_request,
    build_binance_futures_positions_request,
    build_binance_futures_symbol_config_request,
    build_binance_spot_order_request,
    build_kis_cancel_order_request,
    build_kis_domestic_balance_request,
    build_kis_live_order_request,
    build_kis_overseas_balance_request,
    build_upbit_accounts_request,
    build_upbit_cancel_order_request,
    build_upbit_order_chance_request,
    build_upbit_order_detail_request,
    build_upbit_order_request,
    issue_kis_access_token,
    normalize_binance_futures_intent,
    normalize_binance_spot_intent,
    refresh_binance_time_offset,
    send_prepared_request,
)

BrokerStatus = Literal["connected", "missing_credentials", "adapter_required", "disabled"]
CheckStatus = Literal["pass", "warn", "fail"]


def send_binance_signed_request(
    builder: Callable[[], Any],
    *,
    futures: bool = False,
) -> dict[str, object]:
    response = send_prepared_request(builder())
    payload = response.get("json") if isinstance(response.get("json"), dict) else {}
    if int(payload.get("code") or 0) == -1021:
        refresh_binance_time_offset(futures=futures)
        response = send_prepared_request(builder())
    return response


@dataclass(frozen=True)
class BrokerReadiness:
    broker_id: str
    name: str
    role: str
    status: BrokerStatus
    required_env: tuple[str, ...]
    missing_env: tuple[str, ...]
    live_order_adapter_ready: bool
    detail: str

    @property
    def order_ready(self) -> bool:
        return self.status == "connected" and self.live_order_adapter_ready

    def to_dict(self) -> dict[str, object]:
        return {
            "broker_id": self.broker_id,
            "name": self.name,
            "role": self.role,
            "status": self.status,
            "required_env": list(self.required_env),
            "missing_env": list(self.missing_env),
            "live_order_adapter_ready": self.live_order_adapter_ready,
            "order_ready": self.order_ready,
            "detail": self.detail,
        }


BROKER_SPECS = (
    {
        "broker_id": "kis",
        "name": "한국투자증권 Open API",
        "role": "국내/미국 주식, ETF, 장내채권 실거래 후보",
        "required_env": ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_ACCOUNT_PRODUCT_CODE"),
        "base_urls": ("https://openapi.koreainvestment.com:9443",),
        "docs": "KIS Open API 실전/모의투자 REST",
        "capabilities": (
            ("auth_token", "접근 토큰 발급", True, "KIS OAuth token 발급 요청 구현됨"),
            ("account_balance", "계좌 잔고 조회", True, "국내·해외 주식 잔고 조회 요청 구현됨"),
            ("positions", "보유/체결 조회", True, "국내·해외 주식 보유 잔고 조회와 연속조회 구현됨"),
            ("place_order", "현금 주문 전송", True, "국내/해외 주식 실계좌 주문 요청 생성 구현됨"),
            ("cancel_order", "주문 취소/정정", True, "국내/해외 원주문번호 기반 전량 취소 요청 구현됨"),
        ),
        "automation_group": "stock",
        "asset_scope": ("한국주식", "미국주식", "금/오일 ETF", "ETF"),
    },
    {
        "broker_id": "binance",
        "name": "Binance API",
        "role": "코인 현물 실거래 후보",
        "required_env": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
        "base_urls": ("https://api.binance.com", "wss://stream.binance.com:9443"),
        "docs": "Binance Spot REST/User Data Stream",
        "capabilities": (
            ("account_balance", "계좌 잔고 조회", True, "signed account endpoint 조회 구현됨"),
            ("positions", "보유 자산 조회", True, "spot asset balance 대조 구현됨"),
            ("place_order", "현물 주문 전송", True, "POST /api/v3/order signed 요청 생성 구현됨"),
            ("cancel_order", "주문 취소", True, "orderId/clientOrderId signed 취소 요청 구현됨"),
            ("user_stream", "체결 스트림", True, "WebSocket API 서명 구독과 executionReport 수신 구현됨"),
        ),
        "automation_group": "crypto",
        "asset_scope": ("코인 현물",),
    },
    {
        "broker_id": "binance-futures",
        "name": "Binance USD-M Futures",
        "role": "USDⓈ-M 무기한 선물 LONG/SHORT 실거래",
        "required_env": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
        "base_urls": (
            "https://fapi.binance.com",
            "wss://fstream.binance.com",
        ),
        "docs": "Binance USDⓈ-M Futures REST/User Data Stream",
        "capabilities": (
            ("account_balance", "선물 계정 조회", True, "GET /fapi/v3/account signed 조회 구현됨"),
            ("positions", "부호 있는 선물 포지션", True, "GET /fapi/v3/positionRisk LONG/SHORT 대조 구현됨"),
            ("position_mode", "One-way/Hedge 모드", True, "주문 전 현재 모드를 읽어 positionSide를 결정함"),
            ("leverage_policy", "레버리지/증거금 검사", True, "계정 설정을 변경하지 않고 전략 한도와 불일치 시 차단함"),
            ("place_order", "선물 주문 전송", True, "POST /fapi/v1/order signed 요청 구현됨"),
            ("test_order", "매칭 없는 주문 검증", True, "POST /fapi/v1/order/test 구현됨"),
            ("cancel_order", "선물 주문 취소", True, "DELETE /fapi/v1/order 구현됨"),
            ("user_stream", "선물 체결 스트림", True, "ORDER_TRADE_UPDATE 정규화 경로 구현됨"),
        ),
        "automation_group": "crypto",
        "asset_scope": ("코인 USD-M 선물", "LONG", "SHORT"),
    },
    {
        "broker_id": "upbit",
        "name": "Upbit API",
        "role": "원화 마켓 코인 실거래 후보",
        "required_env": ("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY"),
        "base_urls": ("https://api.upbit.com",),
        "docs": "Upbit REST API Orders",
        "capabilities": (
            ("account_balance", "계좌 잔고 조회", True, "전체 계좌 조회 API 연결 구현됨"),
            ("positions", "보유 자산 조회", True, "원화/코인 잔고 대조 구현됨"),
            ("place_order", "코인 주문 전송", True, "POST /v1/orders JWT 서명 요청 생성 구현됨"),
            ("cancel_order", "주문 취소", True, "uuid/identifier 기반 JWT 취소 요청 구현됨"),
        ),
        "automation_group": "crypto",
        "asset_scope": ("KRW 코인",),
    },
)


def now_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


def now_datetime_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def real_orders_enabled() -> bool:
    return os.getenv("LIVE_TRADER_ENABLE_REAL_ORDERS", "").strip().lower() in {"1", "true", "yes", "on"}


def mask_env_value(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        return ""
    if len(value) <= 6:
        return "*" * len(value)
    return f"{value[:3]}...{value[-3:]}"


def spec_by_id(broker_id: str) -> dict[str, object] | None:
    return next((spec for spec in BROKER_SPECS if spec["broker_id"] == broker_id), None)


def broker_order_adapter_implemented(spec: dict[str, object]) -> bool:
    """Return whether the broker's signed place-order capability exists.

    The real-order environment switch is a runtime lock, not evidence about
    whether adapter code has been implemented.  Keeping those concepts
    separate prevents Doctor from reporting a false "adapter missing" error
    merely because the operator has left the route disabled.
    """

    capabilities = spec.get("capabilities")
    if not isinstance(capabilities, tuple):
        return False
    return any(
        str(capability[0]) == "place_order" and capability[2] is True
        for capability in capabilities
        if isinstance(capability, tuple) and len(capability) >= 3
    )


def broker_readiness() -> list[BrokerReadiness]:
    rows: list[BrokerReadiness] = []
    adapter_enabled = real_orders_enabled()
    for spec in BROKER_SPECS:
        required = tuple(spec["required_env"])
        missing = tuple(name for name in required if not os.getenv(name, "").strip())
        adapter_implemented = broker_order_adapter_implemented(spec)
        if missing:
            status: BrokerStatus = "missing_credentials"
            detail = "실거래 API 키/계좌 환경 변수가 비어 있습니다."
        elif not adapter_enabled:
            status = "disabled"
            detail = "주문 어댑터는 구현되어 있지만 LIVE_TRADER_ENABLE_REAL_ORDERS 잠금이 꺼져 있습니다."
        elif not adapter_implemented:
            status = "adapter_required"
            detail = "실주문 서명/전송 어댑터 구현이 필요합니다."
        else:
            status = "connected"
            detail = "주문 요청 생성 어댑터가 준비되었습니다. 계좌/포지션 대조와 운영 게이트를 통과해야 전송됩니다."
        rows.append(
            BrokerReadiness(
                broker_id=str(spec["broker_id"]),
                name=str(spec["name"]),
                role=str(spec["role"]),
                status=status,
                required_env=required,
                missing_env=missing,
                live_order_adapter_ready=adapter_implemented,
                detail=detail,
            )
        )
    return rows


def broker_diagnostics(broker_id: str | None = None) -> list[dict[str, Any]]:
    selected = [spec for spec in BROKER_SPECS if broker_id in {None, "", spec["broker_id"]}]
    rows: list[dict[str, Any]] = []
    adapter_enabled = real_orders_enabled()
    for spec in selected:
        required = tuple(spec["required_env"])
        adapter_implemented = broker_order_adapter_implemented(spec)
        env_rows = [
            {
                "name": name,
                "present": bool(os.getenv(name, "").strip()),
                "masked": mask_env_value(name),
                "secret": "SECRET" in name or "KEY" in name or name in {"KIS_ACCOUNT_NO", "KIS_HTS_ID"},
            }
            for name in required
        ]
        missing = [row["name"] for row in env_rows if not row["present"]]
        capabilities = [
            {
                "key": key,
                "label": label,
                "implemented": implemented,
                "status": "pass" if implemented else "fail",
                "detail": detail,
            }
            for key, label, implemented, detail in spec["capabilities"]
        ]
        steps = [
            {
                "key": "env",
                "label": "환경 변수",
                "status": "fail" if missing else "pass",
                "detail": f"{len(missing)}개 값이 비어 있습니다." if missing else "필수 환경 변수가 채워져 있습니다.",
            },
            {
                "key": "live_route",
                "label": "실거래 라우트",
                "status": "pass" if adapter_enabled else "fail",
                "detail": "LIVE_TRADER_ENABLE_REAL_ORDERS=true" if adapter_enabled else "LIVE_TRADER_ENABLE_REAL_ORDERS=true가 필요합니다.",
            },
            {
                "key": "adapter_code",
                "label": "주문 어댑터",
                "status": "pass" if adapter_implemented else "fail",
                "detail": "서명된 주문 요청 생성·전송 어댑터가 구현되어 있습니다." if adapter_implemented else "실주문 서명/전송 어댑터 구현이 필요합니다.",
            },
            {
                "key": "network_probe",
                "label": "실계좌 Read-only Probe",
                "status": "info",
                "detail": "실제 연결 성공 여부는 Doctor의 계좌·포지션 새로고침/대조 evidence로 판정합니다. 주문 제출은 별도 canary와 lifecycle 게이트를 통과해야 합니다.",
            },
        ]
        fail_count = sum(1 for step in steps if step["status"] == "fail")
        warn_count = sum(1 for step in steps if step["status"] == "warn")
        status: BrokerStatus = (
            "missing_credentials"
            if missing
            else "adapter_required"
            if not adapter_implemented
            else "connected"
            if adapter_enabled
            else "disabled"
        )
        rows.append(
            {
                "broker_id": spec["broker_id"],
                "name": spec["name"],
                "role": spec["role"],
                "status": status,
                "checked_at": now_text(),
                "docs": spec["docs"],
                "base_urls": list(spec["base_urls"]),
                "automation_group": spec.get("automation_group", ""),
                "asset_scope": list(spec.get("asset_scope", ())),
                "env": env_rows,
                "steps": steps,
                "capabilities": capabilities,
                "fail_count": fail_count,
                "warn_count": warn_count,
                "next_actions": broker_next_actions(missing, adapter_enabled),
            }
        )
    return rows


def broker_next_actions(missing: list[str], adapter_enabled: bool) -> list[str]:
    actions: list[str] = []
    if missing:
        actions.append(f"환경 변수 입력 필요: {', '.join(missing)}")
    if not adapter_enabled:
        actions.append("LIVE_TRADER_ENABLE_REAL_ORDERS=true 설정 필요")
    actions.append("공식 API 샌드박스/소액 주문으로 별도 테스트 필요")
    actions.append("소액 Dry Run/Shadow 검증 후 Small Live 승인 필요")
    return actions


def broker_adapter_contract() -> list[dict[str, str]]:
    return [
        {"method": "health_check", "purpose": "키/권한/시각 동기화 점검", "status": "interface_ready"},
        {"method": "get_account_snapshot", "purpose": "현금/잔고/평가금액 조회", "status": "interface_ready"},
        {"method": "list_positions", "purpose": "브로커 포지션 대조", "status": "interface_ready"},
        {"method": "place_order", "purpose": "서명된 실주문 요청 생성/전송", "status": "interface_ready"},
        {"method": "cancel_order", "purpose": "주문 취소/정정", "status": "interface_ready"},
        {"method": "stream_executions", "purpose": "체결/계좌 이벤트 스트림", "status": "interface_ready"},
        {"method": "poll_execution_events", "purpose": "체결/계좌 이벤트 폴링", "status": "account_poll_ready"},
    ]


class BrokerNotReadyError(RuntimeError):
    pass


def numeric_value(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value or "").replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def first_numeric(row: dict[str, object], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return numeric_value(row[key], default)
    return default


def first_text(row: dict[str, object], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return default


def ensure_response_ok(broker_id: str, response: dict[str, object]) -> object:
    if bool(response.get("ok")):
        return response.get("json")
    preview = response.get("preview")
    if isinstance(preview, dict):
        reasons = preview.get("blocked_reasons")
        if isinstance(reasons, list) and reasons:
            raise BrokerNotReadyError(f"{broker_id} 조회 차단: {', '.join(str(item) for item in reasons)}")
    status = response.get("statusCode") or response.get("status") or "unknown"
    text = str(response.get("text") or "")[:240]
    if str(broker_id).lower() == "kis" and str(status) in {"401", "403"}:
        raise BrokerNotReadyError(f"{broker_id} 인증 실패 ({status}): {text}")
    raise BrokerNotReadyError(f"{broker_id} API 조회 실패 ({status}): {text}")


def ensure_kis_payload_ok(response: dict[str, object], *, scope: str) -> dict[str, object]:
    """Validate both HTTP and KIS' application-level result code."""

    payload = ensure_response_ok("kis", response)
    if not isinstance(payload, dict):
        raise BrokerNotReadyError(f"kis {scope} API 응답 형식이 올바르지 않습니다.")
    result_code = str(payload.get("rt_cd") or "0").strip()
    if result_code == "0":
        return payload
    message_code = str(payload.get("msg_cd") or "").strip()
    message = str(payload.get("msg1") or "KIS API 오류").strip()
    auth_text = f"{message_code} {message}".lower()
    auth_error = any(
        token in auth_text
        for token in (
            "token",
            "oauth",
            "authorization",
            "인증",
            "접근토큰",
            "egw00121",
            "egw00122",
            "egw00123",
        )
    )
    error_kind = "인증 실패" if auth_error else "API 조회 실패"
    detail = f"{message_code}: {message}" if message_code else message
    raise BrokerNotReadyError(f"kis {scope} {error_kind}: {detail}")


def kis_symbol(pdno: str) -> str:
    text = pdno.strip().upper()
    if text.isdigit() and len(text) == 6:
        return f"{text}.KS"
    return text


def parse_kis_accounts(payload: object) -> list[dict[str, object]]:
    data = payload if isinstance(payload, dict) else {}
    output2 = data.get("output2")
    rows = output2 if isinstance(output2, list) else [output2] if isinstance(output2, dict) else []
    row = rows[0] if rows else {}
    cash = first_numeric(
        row,
        "dnca_tot_amt",
        "dnca_tot_amt2",
        "nxdy_excc_amt",
        "tot_evlu_amt",
        "nass_amt",
        default=0.0,
    )
    equity = first_numeric(
        row,
        "tot_evlu_amt",
        "nass_amt",
        default=cash,
    )
    return [
        {
            "broker_id": "kis",
            "broker_name": "한국투자증권 Open API",
            "account": "KIS 실계좌",
            "currency": "KRW",
            "broker_cash": cash,
            "broker_equity": equity,
            "valuation_basis": "broker_equity",
            "detail": "KIS 국내 주식 잔고 조회 결과입니다.",
        }
    ]


def parse_kis_positions(payload: object) -> list[dict[str, object]]:
    data = payload if isinstance(payload, dict) else {}
    output1 = data.get("output1")
    rows = output1 if isinstance(output1, list) else []
    positions: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        qty = first_numeric(row, "hldg_qty", "ord_psbl_qty", "qty")
        if qty <= 0:
            continue
        pdno = first_text(row, "pdno", "PDNO", "prdt_code", default="")
        if not pdno:
            continue
        positions.append(
            {
                "symbol": kis_symbol(pdno),
                "asset": "한국주식",
                "broker_id": "kis",
                "broker_name": "한국투자증권 Open API",
                "currency": "KRW",
                "broker_qty": qty,
                "broker_value": first_numeric(row, "evlu_amt", "pchs_amt", "pchs_avg_pric", default=0.0),
                "average_price": first_numeric(row, "pchs_avg_pric", "avg_pric", default=0.0),
                "current_price": first_numeric(row, "prpr", "stck_prpr", default=0.0),
                "valuation_basis": "market_value",
                "detail": first_text(row, "prdt_name", "prdt_name1", default="KIS 보유 종목"),
            }
        )
    return positions


def parse_kis_overseas_positions(payload: object) -> list[dict[str, object]]:
    data = payload if isinstance(payload, dict) else {}
    output1 = data.get("output1")
    rows = (
        output1
        if isinstance(output1, list)
        else [output1]
        if isinstance(output1, dict)
        else []
    )
    positions: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        qty = first_numeric(row, "ovrs_cblc_qty", "ord_psbl_qty")
        if qty <= 0:
            continue
        # The overseas endpoint's canonical field is ovrs_pdno.  Requiring it
        # prevents a domestic-balance payload from being misclassified.
        pdno = first_text(row, "ovrs_pdno", "OVRS_PDNO", default="").strip().upper()
        if not pdno:
            continue
        currency = first_text(row, "tr_crcy_cd", default="USD").strip().upper() or "USD"
        exchange = first_text(row, "ovrs_excg_cd", default="NASD").strip().upper() or "NASD"
        positions.append(
            {
                "symbol": pdno,
                "asset": "미국주식",
                "broker_id": "kis",
                "broker_name": "한국투자증권 Open API",
                "currency": currency,
                "broker_qty": qty,
                "broker_value": first_numeric(
                    row,
                    "ovrs_stck_evlu_amt",
                    "frcr_evlu_amt2",
                    "frcr_pchs_amt1",
                    default=0.0,
                ),
                "average_price": first_numeric(
                    row,
                    "pchs_avg_pric",
                    "avg_unpr3",
                    default=0.0,
                ),
                "current_price": first_numeric(row, "now_pric2", "last", default=0.0),
                "valuation_basis": "market_value",
                "exchange": exchange,
                "detail": first_text(
                    row,
                    "ovrs_item_name",
                    "prdt_name",
                    default="KIS 해외주식 보유 종목",
                ),
            }
        )
    return positions


def fetch_kis_overseas_balance(
    access_token: str,
    *,
    exchange: str = "NASD",
    currency: str = "USD",
    max_pages: int = 10,
) -> dict[str, object]:
    """Fetch a complete read-only overseas balance snapshot.

    Absence can only be interpreted as a zero position after every KIS
    continuation page has been consumed.  Broken/repeated continuation keys
    fail closed instead of publishing a partial snapshot.
    """

    output1: list[dict[str, object]] = []
    output2: list[dict[str, object]] = []
    context_fk200 = ""
    context_nk200 = ""
    continuation = ""
    seen_keys: set[tuple[str, str]] = set()
    for _ in range(max(1, int(max_pages))):
        response = send_prepared_request(
            build_kis_overseas_balance_request(
                access_token=access_token,
                exchange=exchange,
                currency=currency,
                context_fk200=context_fk200,
                context_nk200=context_nk200,
                continuation=continuation,
            )
        )
        payload = ensure_kis_payload_ok(response, scope="해외주식 잔고")
        page_positions = payload.get("output1")
        for item in (
            page_positions
            if isinstance(page_positions, list)
            else [page_positions]
            if isinstance(page_positions, dict)
            else []
        ):
            if isinstance(item, dict):
                output1.append(dict(item))
        page_summary = payload.get("output2")
        for item in (
            page_summary
            if isinstance(page_summary, list)
            else [page_summary]
            if isinstance(page_summary, dict)
            else []
        ):
            if isinstance(item, dict):
                output2.append(dict(item))

        tr_cont = str(response.get("trCont") or "").strip().upper()
        if tr_cont not in {"M", "F"}:
            return {"rt_cd": "0", "output1": output1, "output2": output2}
        next_fk200 = str(payload.get("ctx_area_fk200") or payload.get("CTX_AREA_FK200") or "")
        next_nk200 = str(payload.get("ctx_area_nk200") or payload.get("CTX_AREA_NK200") or "")
        next_key = (next_fk200, next_nk200)
        if not any(next_key) or next_key in seen_keys:
            raise BrokerNotReadyError(
                "kis 해외주식 잔고 API 조회 실패: 연속조회 키가 없거나 반복되어 전체 스냅샷을 증명할 수 없습니다."
            )
        seen_keys.add(next_key)
        context_fk200, context_nk200, continuation = next_fk200, next_nk200, "N"
    raise BrokerNotReadyError(
        f"kis 해외주식 잔고 API 조회 실패: {max_pages}페이지 안에 연속조회가 끝나지 않았습니다."
    )


def broker_snapshot_events(accounts: list[dict[str, object]], positions: list[dict[str, object]]) -> list[dict[str, object]]:
    occurred_at = now_datetime_text()
    events: list[dict[str, object]] = []
    for account in accounts:
        broker_id = str(account.get("broker_id") or "kis")
        account_name = str(account.get("account") or broker_id)
        currency = str(account.get("currency") or "KRW")
        cash = numeric_value(account.get("broker_cash"), 0.0)
        events.append(
            {
                "event_id": f"{broker_id}:account:{account_name}:{currency}:{occurred_at}",
                "broker_id": broker_id,
                "order_id": "",
                "broker_order_id": "",
                "symbol": "",
                "side": "",
                "quantity": 0.0,
                "price": cash,
                "state": "account_snapshot",
                "occurred_at": occurred_at,
                "account": account_name,
                "currency": currency,
                "cash": cash,
                "detail": account.get("detail", ""),
            }
        )

    for position in positions:
        broker_id = str(position.get("broker_id") or "kis")
        symbol = str(position.get("symbol") or "")
        position_side = str(
            position.get("position_side")
            or position.get("positionSide")
            or ""
        ).strip().upper()
        qty = numeric_value(position.get("broker_qty"), 0.0)
        value = numeric_value(position.get("broker_value"), 0.0)
        events.append(
            {
                "event_id": (
                    f"{broker_id}:position:{symbol}:"
                    f"{position_side or 'NET'}:{occurred_at}"
                ),
                "broker_id": broker_id,
                "order_id": "",
                "broker_order_id": "",
                "symbol": symbol,
                "side": "",
                "quantity": qty,
                "price": value,
                "state": "position_snapshot",
                "occurred_at": occurred_at,
                "asset": position.get("asset", ""),
                "currency": position.get("currency", ""),
                "position_side": position_side,
                "positionSide": position_side,
                "detail": position.get("detail", ""),
            }
        )
    return events


def parse_binance_accounts(payload: object) -> list[dict[str, object]]:
    data = payload if isinstance(payload, dict) else {}
    balances = data.get("balances")
    rows = balances if isinstance(balances, list) else []
    usdt = next((row for row in rows if isinstance(row, dict) and str(row.get("asset", "")).upper() == "USDT"), {})
    cash = first_numeric(usdt, "free") + first_numeric(usdt, "locked")
    return [
        {
            "broker_id": "binance",
            "broker_name": "Binance API",
            "account": "Binance Spot",
            "currency": "USDT",
            "broker_cash": cash,
            "broker_equity": cash,
            "valuation_basis": "cash_only",
            "detail": "Binance signed account endpoint 조회 결과입니다.",
        }
    ]


def parse_binance_positions(payload: object) -> list[dict[str, object]]:
    data = payload if isinstance(payload, dict) else {}
    balances = data.get("balances")
    rows = balances if isinstance(balances, list) else []
    cash_assets = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD"}
    positions: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset = str(row.get("asset") or "").upper()
        qty = first_numeric(row, "free") + first_numeric(row, "locked")
        if not asset or asset in cash_assets or qty <= 0:
            continue
        positions.append(
            {
                "symbol": asset,
                "asset": "코인",
                "broker_id": "binance",
                "broker_name": "Binance API",
                "currency": asset,
                "broker_qty": qty,
                "broker_value": 0.0,
                "average_price": 0.0,
                "current_price": 0.0,
                "valuation_basis": "unavailable",
                "detail": "Binance spot balance입니다.",
            }
        )
    return positions


def parse_binance_futures_accounts(
    payload: object,
) -> list[dict[str, object]]:
    data = payload if isinstance(payload, dict) else {}
    assets = data.get("assets")
    rows = assets if isinstance(assets, list) else []
    usdt = next(
        (
            row
            for row in rows
            if isinstance(row, dict)
            and str(row.get("asset") or "").upper() == "USDT"
        ),
        {},
    )
    available = first_numeric(
        usdt,
        "availableBalance",
        "marginBalance",
        "walletBalance",
        default=first_numeric(
            data,
            "availableBalance",
            "totalMarginBalance",
            "totalWalletBalance",
        ),
    )
    return [
        {
            "broker_id": "binance-futures",
            "broker_name": "Binance USD-M Futures",
            "account": "Binance USD-M Futures",
            "currency": "USDT",
            "broker_cash": available,
            "wallet_balance": first_numeric(
                usdt,
                "walletBalance",
                default=first_numeric(data, "totalWalletBalance"),
            ),
            "margin_balance": first_numeric(
                usdt,
                "marginBalance",
                default=first_numeric(data, "totalMarginBalance"),
            ),
            "broker_equity": first_numeric(
                usdt,
                "marginBalance",
                "walletBalance",
                default=first_numeric(
                    data,
                    "totalMarginBalance",
                    "totalWalletBalance",
                    default=available,
                ),
            ),
            "valuation_basis": "margin_balance",
            "detail": "Binance Futures signed account endpoint 조회 결과입니다.",
        }
    ]


def parse_binance_futures_positions(
    payload: object,
) -> list[dict[str, object]]:
    if isinstance(payload, dict):
        raw_positions = payload.get("positions")
        rows = (
            raw_positions
            if isinstance(raw_positions, list)
            else [payload]
            if payload.get("symbol")
            else []
        )
    else:
        rows = payload if isinstance(payload, list) else []
    positions: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = first_text(row, "symbol").strip().upper()
        if not symbol:
            continue
        position_side = first_text(
            row,
            "positionSide",
            default="BOTH",
        ).strip().upper()
        raw_quantity = first_numeric(row, "positionAmt")
        if position_side == "SHORT":
            quantity = -abs(raw_quantity)
        elif position_side == "LONG":
            quantity = abs(raw_quantity)
        else:
            quantity = raw_quantity
        if abs(quantity) <= 1e-12:
            continue
        mark_price = first_numeric(row, "markPrice")
        notional = first_numeric(row, "notional")
        positions.append(
            {
                "symbol": symbol,
                "asset": "코인 USD-M 선물",
                "broker_id": "binance-futures",
                "broker_name": "Binance USD-M Futures",
                "currency": "USDT",
                "broker_qty": quantity,
                "broker_value": (
                    abs(notional)
                    if notional
                    else abs(quantity * mark_price)
                ),
                "average_price": first_numeric(row, "entryPrice"),
                "current_price": mark_price,
                "valuation_basis": "market_notional",
                "position_side": position_side,
                "positionSide": position_side,
                "unrealized_profit": first_numeric(
                    row,
                    "unRealizedProfit",
                    "unrealizedProfit",
                ),
                "liquidation_price": first_numeric(
                    row,
                    "liquidationPrice",
                ),
                "leverage": first_numeric(row, "leverage"),
                "margin_type": first_text(
                    row,
                    "marginType",
                    default="",
                ).upper(),
                "detail": (
                    f"Binance Futures {position_side} "
                    f"positionAmt={quantity:g}"
                ),
            }
        )
    return positions


def normalize_binance_futures_symbol_config(
    payload: object,
    symbol: str,
) -> dict[str, object]:
    rows = payload if isinstance(payload, list) else [payload]
    normalized_symbol = (
        str(symbol or "").strip().upper().removesuffix(".PERP").replace("-", "")
    )
    row = next(
        (
            item
            for item in rows
            if isinstance(item, dict)
            and str(item.get("symbol") or "").upper() == normalized_symbol
        ),
        {},
    )
    return {
        "symbol": normalized_symbol,
        "margin_type": first_text(
            row,
            "marginType",
            default="",
        ).upper(),
        "leverage": first_numeric(row, "leverage"),
        "max_notional": first_numeric(
            row,
            "maxNotionalValue",
            "maxNotional",
        ),
    }


def validate_binance_futures_execution_policy(
    intent: dict[str, object],
    symbol_config: dict[str, object],
) -> None:
    if intent.get("risk_reducing") is True:
        return
    maximum_leverage = max(
        1.0,
        numeric_value(
            intent.get("max_leverage")
            or intent.get("maxLeverage"),
            1.0,
        ),
    )
    current_leverage = numeric_value(
        symbol_config.get("leverage"),
        0.0,
    )
    if current_leverage <= 0:
        raise BrokerNotReadyError(
            "Binance Futures 현재 레버리지를 확인하지 못해 신규 진입을 차단했습니다."
        )
    if current_leverage > maximum_leverage + 1e-12:
        raise BrokerNotReadyError(
            "Binance Futures 현재 레버리지 "
            f"{current_leverage:g}x가 전략 한도 "
            f"{maximum_leverage:g}x를 초과합니다."
        )
    required_margin_type = str(
        intent.get("required_margin_type")
        or intent.get("requiredMarginType")
        or "ISOLATED"
    ).strip().upper()
    current_margin_type = str(
        symbol_config.get("margin_type") or ""
    ).strip().upper()
    if required_margin_type not in {"", "ANY"} and (
        current_margin_type != required_margin_type
    ):
        raise BrokerNotReadyError(
            "Binance Futures 증거금 방식이 전략 요구와 다릅니다: "
            f"현재 {current_margin_type or 'UNKNOWN'}, "
            f"요구 {required_margin_type}."
        )


def parse_upbit_accounts(payload: object) -> list[dict[str, object]]:
    rows = payload if isinstance(payload, list) else []
    krw = next((row for row in rows if isinstance(row, dict) and str(row.get("currency", "")).upper() == "KRW"), {})
    cash = first_numeric(krw, "balance") + first_numeric(krw, "locked")
    return [
        {
            "broker_id": "upbit",
            "broker_name": "Upbit API",
            "account": "Upbit KRW",
            "currency": "KRW",
            "broker_cash": cash,
            "broker_equity": cash,
            "valuation_basis": "cash_only",
            "detail": "Upbit 전체 계좌 조회 결과입니다.",
        }
    ]


def parse_upbit_positions(payload: object) -> list[dict[str, object]]:
    rows = payload if isinstance(payload, list) else []
    positions: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        currency = str(row.get("currency") or "").upper()
        qty = first_numeric(row, "balance") + first_numeric(row, "locked")
        if not currency or currency == "KRW" or qty <= 0:
            continue
        unit = str(row.get("unit_currency") or "KRW").upper()
        positions.append(
            {
                "symbol": f"{unit}-{currency}" if unit else currency,
                "asset": "코인",
                "broker_id": "upbit",
                "broker_name": "Upbit API",
                # Upbit's average buy price and evaluated value are denominated
                # in unit_currency (normally KRW), not in the held asset.
                "currency": unit,
                "broker_qty": qty,
                "broker_value": first_numeric(row, "avg_buy_price") * qty,
                "average_price": first_numeric(row, "avg_buy_price"),
                "current_price": 0.0,
                "valuation_basis": "cost_basis",
                "detail": "Upbit 보유 자산입니다.",
            }
        )
    return positions


class LiveBrokerRouter:
    """Provider-specific live broker boundary.

    The router can build signed official API requests, but order submission is
    still gated by LIVE_TRADER_ENABLE_REAL_ORDERS and the state-layer risk checks.
    """

    def __init__(self, execution_stream_manager: Any | None = None) -> None:
        self.execution_stream_manager = execution_stream_manager

    def health_check(self, broker_id: str) -> dict[str, object]:
        return {"broker_id": broker_id, "diagnostics": broker_diagnostics(broker_id)}

    def get_account_snapshot(self, broker_id: str) -> dict[str, object]:
        broker_id = broker_id.lower().strip()
        if broker_id == "kis":
            token = issue_kis_access_token()
            payload = ensure_kis_payload_ok(
                send_prepared_request(build_kis_domestic_balance_request(access_token=token)),
                scope="국내주식 잔고",
            )
            return {"broker_id": "kis", "accounts": parse_kis_accounts(payload)}
        if broker_id == "binance":
            payload = ensure_response_ok("binance", send_binance_signed_request(build_binance_account_request))
            return {"broker_id": "binance", "accounts": parse_binance_accounts(payload)}
        if broker_id == "binance-futures":
            payload = ensure_response_ok(
                "binance-futures",
                send_binance_signed_request(
                    build_binance_futures_account_request,
                    futures=True,
                ),
            )
            return {
                "broker_id": "binance-futures",
                "accounts": parse_binance_futures_accounts(payload),
            }
        if broker_id == "upbit":
            payload = ensure_response_ok("upbit", send_prepared_request(build_upbit_accounts_request()))
            return {"broker_id": "upbit", "accounts": parse_upbit_accounts(payload)}
        raise BrokerNotReadyError(f"지원하지 않는 broker_id입니다: {broker_id}")

    def list_positions(self, broker_id: str) -> list[dict[str, object]]:
        broker_id = broker_id.lower().strip()
        if broker_id == "kis":
            token = issue_kis_access_token()
            domestic_payload = ensure_kis_payload_ok(
                send_prepared_request(build_kis_domestic_balance_request(access_token=token)),
                scope="국내주식 잔고",
            )
            overseas_payload = fetch_kis_overseas_balance(token)
            return [
                *parse_kis_positions(domestic_payload),
                *parse_kis_overseas_positions(overseas_payload),
            ]
        if broker_id == "binance":
            payload = ensure_response_ok("binance", send_binance_signed_request(build_binance_account_request))
            return parse_binance_positions(payload)
        if broker_id == "binance-futures":
            payload = ensure_response_ok(
                "binance-futures",
                send_binance_signed_request(
                    build_binance_futures_positions_request,
                    futures=True,
                ),
            )
            return parse_binance_futures_positions(payload)
        if broker_id == "upbit":
            payload = ensure_response_ok("upbit", send_prepared_request(build_upbit_accounts_request()))
            return parse_upbit_positions(payload)
        raise BrokerNotReadyError(f"지원하지 않는 broker_id입니다: {broker_id}")

    def place_order(self, intent: dict[str, object]) -> dict[str, object]:
        if not real_orders_enabled():
            raise BrokerNotReadyError("LIVE_TRADER_ENABLE_REAL_ORDERS=true가 아니므로 실주문 전송을 차단했습니다.")
        broker_id = str(intent.get("broker_id") or intent.get("broker") or "").lower()
        if broker_id == "kis":
            token = issue_kis_access_token()
            return send_prepared_request(build_kis_live_order_request(intent, access_token=token))
        if broker_id == "binance":
            try:
                normalized_intent = normalize_binance_spot_intent(intent)
            except RuntimeError as exc:
                raise BrokerNotReadyError(str(exc)) from exc
            return send_binance_signed_request(lambda: build_binance_spot_order_request(normalized_intent))
        if broker_id == "binance-futures":
            try:
                normalized_intent = normalize_binance_futures_intent(
                    intent
                )
            except RuntimeError as exc:
                raise BrokerNotReadyError(str(exc)) from exc
            position_mode_payload = ensure_response_ok(
                "binance-futures",
                send_binance_signed_request(
                    build_binance_futures_position_mode_request,
                    futures=True,
                ),
            )
            position_mode = (
                position_mode_payload
                if isinstance(position_mode_payload, dict)
                else {}
            )
            symbol = str(normalized_intent.get("symbol") or "")
            symbol_config_payload = ensure_response_ok(
                "binance-futures",
                send_binance_signed_request(
                    lambda: build_binance_futures_symbol_config_request(
                        symbol
                    ),
                    futures=True,
                ),
            )
            validate_binance_futures_execution_policy(
                normalized_intent,
                normalize_binance_futures_symbol_config(
                    symbol_config_payload,
                    symbol,
                ),
            )
            return send_binance_signed_request(
                lambda: build_binance_futures_order_request(
                    normalized_intent,
                    hedge_mode=position_mode.get(
                        "dualSidePosition"
                    ) is True,
                ),
                futures=True,
            )
        if broker_id == "upbit":
            return send_prepared_request(build_upbit_order_request(intent))
        raise BrokerNotReadyError(f"지원하지 않는 broker_id입니다: {broker_id}")

    def list_open_orders(
        self,
        broker_id: str,
        *,
        symbol: str = "",
    ) -> list[dict[str, object]]:
        broker_id = broker_id.lower().strip()
        if broker_id != "binance-futures":
            raise BrokerNotReadyError(
                f"미체결 주문 조회를 지원하지 않는 broker_id입니다: {broker_id}"
            )
        payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                lambda: build_binance_futures_open_orders_request(symbol),
                futures=True,
            ),
        )
        if not isinstance(payload, list):
            raise BrokerNotReadyError(
                "Binance Futures 미체결 주문 응답 형식이 올바르지 않습니다."
            )
        return [
            dict(item)
            for item in payload
            if isinstance(item, dict)
        ]

    def get_order_status(
        self,
        broker_id: str,
        *,
        symbol: str,
        broker_order_id: str,
        client_order_id: bool = False,
    ) -> dict[str, object]:
        broker_id = broker_id.lower().strip()
        if broker_id != "binance-futures":
            raise BrokerNotReadyError(
                f"주문 상태 조회를 지원하지 않는 broker_id입니다: {broker_id}"
            )
        payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                lambda: build_binance_futures_order_status_request(
                    symbol,
                    broker_order_id,
                    client_order_id=client_order_id,
                ),
                futures=True,
            ),
        )
        if not isinstance(payload, dict):
            raise BrokerNotReadyError(
                "Binance Futures 주문 상태 응답 형식이 올바르지 않습니다."
            )
        return dict(payload)

    def get_binance_futures_canary_observation(
        self,
        symbol: str,
    ) -> dict[str, object]:
        """Read the fresh account facts required by a Futures canary gate.

        The return value deliberately excludes request URLs, headers, API
        credentials, signatures, broker order payloads, and raw responses.
        """

        normalized_symbol = (
            str(symbol or "")
            .strip()
            .upper()
            .removesuffix(".PERP")
            .replace("-", "")
        )
        if not normalized_symbol:
            raise BrokerNotReadyError(
                "Binance Futures canary symbol이 필요합니다."
            )
        account_payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                build_binance_futures_account_request,
                futures=True,
            ),
        )
        account_config_payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                build_binance_futures_account_config_request,
                futures=True,
            ),
        )
        position_mode_payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                build_binance_futures_position_mode_request,
                futures=True,
            ),
        )
        positions_payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                build_binance_futures_positions_request,
                futures=True,
            ),
        )
        symbol_config_payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                lambda: build_binance_futures_symbol_config_request(
                    normalized_symbol
                ),
                futures=True,
            ),
        )
        open_orders = self.list_open_orders("binance-futures")
        if not isinstance(account_payload, dict):
            raise BrokerNotReadyError(
                "Binance Futures account 응답 형식이 올바르지 않습니다."
            )
        if not isinstance(account_config_payload, dict):
            raise BrokerNotReadyError(
                "Binance Futures accountConfig 응답 형식이 올바르지 않습니다."
            )
        if not isinstance(position_mode_payload, dict):
            raise BrokerNotReadyError(
                "Binance Futures position mode 응답 형식이 올바르지 않습니다."
            )
        if not isinstance(positions_payload, list):
            raise BrokerNotReadyError(
                "Binance Futures position 응답 형식이 올바르지 않습니다."
            )
        assets = (
            account_payload.get("assets")
            if isinstance(account_payload.get("assets"), list)
            else []
        )
        usdt = next(
            (
                item
                for item in assets
                if isinstance(item, dict)
                and str(item.get("asset") or "").upper() == "USDT"
            ),
            {},
        )
        available_known = (
            isinstance(usdt, dict)
            and "availableBalance" in usdt
        )
        available_usdt: float | None = None
        if available_known:
            try:
                available_usdt = float(usdt.get("availableBalance"))
            except (TypeError, ValueError):
                available_known = False
        position_count = 0
        for item in positions_payload:
            if not isinstance(item, dict):
                continue
            try:
                quantity = float(item.get("positionAmt") or 0)
            except (TypeError, ValueError):
                raise BrokerNotReadyError(
                    "Binance Futures positionAmt를 해석할 수 없습니다."
                )
            if abs(quantity) > 1e-12:
                position_count += 1
        can_trade = account_config_payload.get("canTrade")
        return {
            "account": {
                "can_trade": (
                    can_trade if isinstance(can_trade, bool) else None
                ),
                "available_usdt": available_usdt,
                "available_usdt_known": available_known,
            },
            "position_mode": {
                "dual_side_position": (
                    position_mode_payload.get("dualSidePosition")
                    if isinstance(
                        position_mode_payload.get("dualSidePosition"),
                        bool,
                    )
                    else None
                ),
            },
            "symbol_config": normalize_binance_futures_symbol_config(
                symbol_config_payload,
                normalized_symbol,
            ),
            "position_count": position_count,
            "open_order_count": len(open_orders),
        }

    def test_binance_futures_order(
        self,
        intent: dict[str, object],
    ) -> dict[str, object]:
        """Validate a signed Futures order without matching-engine submission."""

        try:
            normalized_intent = normalize_binance_futures_intent(intent)
        except RuntimeError as exc:
            raise BrokerNotReadyError(str(exc)) from exc
        position_mode_payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                build_binance_futures_position_mode_request,
                futures=True,
            ),
        )
        position_mode = (
            position_mode_payload
            if isinstance(position_mode_payload, dict)
            else {}
        )
        symbol = str(normalized_intent.get("symbol") or "")
        symbol_config_payload = ensure_response_ok(
            "binance-futures",
            send_binance_signed_request(
                lambda: build_binance_futures_symbol_config_request(
                    symbol
                ),
                futures=True,
            ),
        )
        validate_binance_futures_execution_policy(
            normalized_intent,
            normalize_binance_futures_symbol_config(
                symbol_config_payload,
                symbol,
            ),
        )

        def build_test_request() -> Any:
            prepared = build_binance_futures_order_request(
                normalized_intent,
                hedge_mode=position_mode.get("dualSidePosition") is True,
                test=True,
            )
            if (
                prepared.method != "POST"
                or prepared.endpoint
                != BINANCE_FUTURES_TEST_ORDER_ENDPOINT
            ):
                raise BrokerNotReadyError(
                    "Binance Futures test order endpoint 안전검사 실패"
                )
            return prepared

        return send_binance_signed_request(
            build_test_request,
            futures=True,
        )

    def get_upbit_order_chance(self, market: str) -> dict[str, object]:
        payload = ensure_response_ok(
            "upbit",
            send_prepared_request(build_upbit_order_chance_request(market)),
        )
        if not isinstance(payload, dict):
            raise BrokerNotReadyError("Upbit 주문 가능 정보 응답 형식이 올바르지 않습니다.")
        return payload

    def get_upbit_order(self, order_uuid: str) -> dict[str, object]:
        payload = ensure_response_ok(
            "upbit",
            send_prepared_request(build_upbit_order_detail_request(order_uuid)),
        )
        if not isinstance(payload, dict):
            raise BrokerNotReadyError("Upbit 개별 주문 응답 형식이 올바르지 않습니다.")
        return payload

    def cancel_order(self, broker_id: str, broker_order_id: str, **context: object) -> dict[str, object]:
        if not real_orders_enabled():
            raise BrokerNotReadyError("LIVE_TRADER_ENABLE_REAL_ORDERS=true가 아니므로 실제 주문 취소 전송을 차단했습니다.")
        broker_id = broker_id.lower().strip()
        if broker_id == "kis":
            token = issue_kis_access_token()
            order = {**context, "broker_order_id": broker_order_id}
            return send_prepared_request(build_kis_cancel_order_request(order, access_token=token))
        if broker_id == "binance":
            return send_binance_signed_request(
                lambda: build_binance_cancel_order_request(
                    str(context.get("symbol") or ""),
                    broker_order_id,
                    client_order_id=bool(context.get("client_order_id")),
                )
            )
        if broker_id == "binance-futures":
            return send_binance_signed_request(
                lambda: build_binance_futures_cancel_order_request(
                    str(context.get("symbol") or ""),
                    broker_order_id,
                    client_order_id=bool(
                        context.get("client_order_id")
                    ),
                ),
                futures=True,
            )
        if broker_id == "upbit":
            return send_prepared_request(build_upbit_cancel_order_request(
                broker_order_id,
                identifier=bool(context.get("identifier")),
            ))
        raise BrokerNotReadyError(f"지원하지 않는 broker_id입니다: {broker_id}")

    def stream_executions(self, broker_id: str) -> dict[str, object]:
        broker_id = broker_id.lower().strip()
        if broker_id not in {
            "kis",
            "binance",
            "binance-futures",
            "upbit",
        }:
            raise BrokerNotReadyError(f"지원하지 않는 broker_id입니다: {broker_id}")
        if self.execution_stream_manager is None:
            raise BrokerNotReadyError("ExecutionStreamManager를 LiveBrokerRouter에 연결해야 합니다.")
        return self.execution_stream_manager.start((broker_id,))

    def poll_execution_events(self, broker_id: str) -> dict[str, object]:
        broker_id = broker_id.lower().strip()
        if broker_id == "kis":
            token = issue_kis_access_token()
            domestic_payload = ensure_kis_payload_ok(
                send_prepared_request(build_kis_domestic_balance_request(access_token=token)),
                scope="국내주식 잔고",
            )
            overseas_payload = fetch_kis_overseas_balance(token)
            accounts = parse_kis_accounts(domestic_payload)
            positions = [
                *parse_kis_positions(domestic_payload),
                *parse_kis_overseas_positions(overseas_payload),
            ]
            return {
                "broker_id": "kis",
                "accounts": accounts,
                "positions": positions,
                "events": broker_snapshot_events(accounts, positions),
                "source": "kis_balance_poll",
            }
        if broker_id == "binance":
            payload = ensure_response_ok("binance", send_binance_signed_request(build_binance_account_request))
            accounts = parse_binance_accounts(payload)
            positions = parse_binance_positions(payload)
            return {
                "broker_id": "binance",
                "accounts": accounts,
                "positions": positions,
                "events": broker_snapshot_events(accounts, positions),
                "source": "binance_account_poll",
            }
        if broker_id == "binance-futures":
            account_payload = ensure_response_ok(
                "binance-futures",
                send_binance_signed_request(
                    build_binance_futures_account_request,
                    futures=True,
                ),
            )
            position_payload = ensure_response_ok(
                "binance-futures",
                send_binance_signed_request(
                    build_binance_futures_positions_request,
                    futures=True,
                ),
            )
            accounts = parse_binance_futures_accounts(account_payload)
            positions = parse_binance_futures_positions(position_payload)
            return {
                "broker_id": "binance-futures",
                "accounts": accounts,
                "positions": positions,
                "events": broker_snapshot_events(accounts, positions),
                "source": "binance_futures_account_poll",
            }
        if broker_id == "upbit":
            payload = ensure_response_ok("upbit", send_prepared_request(build_upbit_accounts_request()))
            accounts = parse_upbit_accounts(payload)
            positions = parse_upbit_positions(payload)
            return {
                "broker_id": "upbit",
                "accounts": accounts,
                "positions": positions,
                "events": broker_snapshot_events(accounts, positions),
                "source": "upbit_accounts_poll",
            }
        raise BrokerNotReadyError(f"지원하지 않는 broker_id입니다: {broker_id}")
