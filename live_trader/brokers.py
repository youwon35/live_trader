from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal


BrokerStatus = Literal["connected", "missing_credentials", "adapter_required", "disabled"]


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
    },
    {
        "broker_id": "binance",
        "name": "Binance API",
        "role": "코인 현물 실거래 후보",
        "required_env": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
    },
)


def real_orders_enabled() -> bool:
    return os.getenv("LIVE_TRADER_ENABLE_REAL_ORDERS", "").strip().lower() in {"1", "true", "yes", "on"}


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


class BrokerNotReadyError(RuntimeError):
    pass


class LiveBrokerRouter:
    """Real broker router boundary.

    This class deliberately refuses live orders until provider-specific signed
    REST/WebSocket order adapters are implemented and audited.
    """

    def place_order(self, intent: dict[str, object]) -> dict[str, object]:
        _ = intent
        raise BrokerNotReadyError(
            "Live broker adapters are not implemented. Add provider-specific KIS/Binance signed order code before enabling real orders."
        )
