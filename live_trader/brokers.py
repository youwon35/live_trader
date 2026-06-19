from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


BrokerStatus = Literal["connected", "missing_credentials", "adapter_required", "disabled"]
CheckStatus = Literal["pass", "warn", "fail"]


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
            ("auth_token", "접근 토큰 발급", False, "KIS OAuth token 발급/갱신 구현 필요"),
            ("account_balance", "계좌 잔고 조회", False, "계좌 상품코드별 잔고 조회 API 연결 필요"),
            ("positions", "보유/체결 조회", False, "국내/해외 주식 포지션 대조 API 연결 필요"),
            ("place_order", "현금 주문 전송", False, "서명 헤더와 TR ID 검증 후 구현"),
            ("cancel_order", "주문 취소/정정", False, "원주문번호 기반 취소/정정 API 구현 필요"),
        ),
    },
    {
        "broker_id": "binance",
        "name": "Binance API",
        "role": "코인 현물 실거래 후보",
        "required_env": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
        "base_urls": ("https://api.binance.com", "wss://stream.binance.com:9443"),
        "docs": "Binance Spot REST/User Data Stream",
        "capabilities": (
            ("account_balance", "계좌 잔고 조회", False, "signed account endpoint 구현 필요"),
            ("positions", "보유 자산 조회", False, "spot asset balance 대조 구현 필요"),
            ("place_order", "현물 주문 전송", False, "HMAC 서명 주문 전송 구현 필요"),
            ("cancel_order", "주문 취소", False, "orderId/clientOrderId 취소 구현 필요"),
            ("user_stream", "체결 스트림", False, "listenKey 발급과 WebSocket 수신 구현 필요"),
        ),
    },
)


def now_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


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


def broker_readiness() -> list[BrokerReadiness]:
    rows: list[BrokerReadiness] = []
    adapter_enabled = real_orders_enabled()
    for spec in BROKER_SPECS:
        required = tuple(spec["required_env"])
        missing = tuple(name for name in required if not os.getenv(name, "").strip())
        if missing:
            status: BrokerStatus = "missing_credentials"
            detail = "실거래 API 키/계좌 환경 변수가 비어 있습니다."
        elif not adapter_enabled:
            status = "adapter_required"
            detail = "환경 변수는 준비될 수 있지만 LIVE_TRADER_ENABLE_REAL_ORDERS=true와 실제 주문 어댑터 검증이 필요합니다."
        else:
            status = "adapter_required"
            detail = "실제 주문 서명/전송 어댑터 코드가 아직 안전 검증 전이므로 주문은 차단됩니다."
        rows.append(
            BrokerReadiness(
                broker_id=str(spec["broker_id"]),
                name=str(spec["name"]),
                role=str(spec["role"]),
                status=status,
                required_env=required,
                missing_env=missing,
                live_order_adapter_ready=False,
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
        env_rows = [
            {
                "name": name,
                "present": bool(os.getenv(name, "").strip()),
                "masked": mask_env_value(name),
                "secret": "SECRET" in name or "KEY" in name,
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
                "status": "fail",
                "detail": "실제 서명/전송 어댑터가 아직 안전 검증 전입니다.",
            },
            {
                "key": "network_probe",
                "label": "네트워크 Probe",
                "status": "warn",
                "detail": "실제 HTTP/WebSocket Probe는 서명 어댑터 구현 후 수행합니다.",
            },
        ]
        fail_count = sum(1 for step in steps if step["status"] == "fail")
        warn_count = sum(1 for step in steps if step["status"] == "warn")
        status: BrokerStatus = "missing_credentials" if missing else "adapter_required"
        rows.append(
            {
                "broker_id": spec["broker_id"],
                "name": spec["name"],
                "role": spec["role"],
                "status": status,
                "checked_at": now_text(),
                "docs": spec["docs"],
                "base_urls": list(spec["base_urls"]),
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
    actions.append("실제 REST/WebSocket 서명 어댑터 구현 및 별도 테스트 필요")
    actions.append("소액 Dry Run/Shadow 검증 후 Small Live 승인 필요")
    return actions


def broker_adapter_contract() -> list[dict[str, str]]:
    return [
        {"method": "health_check", "purpose": "키/권한/시각 동기화 점검", "status": "interface_ready"},
        {"method": "get_account_snapshot", "purpose": "현금/잔고/평가금액 조회", "status": "interface_ready"},
        {"method": "list_positions", "purpose": "브로커 포지션 대조", "status": "interface_ready"},
        {"method": "place_order", "purpose": "서명된 실주문 전송", "status": "blocked_stub"},
        {"method": "cancel_order", "purpose": "주문 취소/정정", "status": "blocked_stub"},
        {"method": "stream_executions", "purpose": "체결/계좌 이벤트 스트림", "status": "blocked_stub"},
    ]


class BrokerNotReadyError(RuntimeError):
    pass


class LiveBrokerRouter:
    """Real broker router boundary.

    This class deliberately refuses live orders until provider-specific signed
    REST/WebSocket order adapters are implemented and audited.
    """

    def health_check(self, broker_id: str) -> dict[str, object]:
        return {"broker_id": broker_id, "diagnostics": broker_diagnostics(broker_id)}

    def get_account_snapshot(self, broker_id: str) -> dict[str, object]:
        _ = broker_id
        raise BrokerNotReadyError("Broker account snapshot adapters are not implemented yet.")

    def list_positions(self, broker_id: str) -> list[dict[str, object]]:
        _ = broker_id
        raise BrokerNotReadyError("Broker position adapters are not implemented yet.")

    def place_order(self, intent: dict[str, object]) -> dict[str, object]:
        _ = intent
        raise BrokerNotReadyError(
            "Live broker adapters are not implemented. Add provider-specific KIS/Binance signed order code before enabling real orders."
        )

    def cancel_order(self, broker_id: str, broker_order_id: str) -> dict[str, object]:
        _ = (broker_id, broker_order_id)
        raise BrokerNotReadyError("Broker cancel-order adapters are not implemented yet.")

    def stream_executions(self, broker_id: str) -> None:
        _ = broker_id
        raise BrokerNotReadyError("Broker execution stream adapters are not implemented yet.")
