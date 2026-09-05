"""Read-only product facts about the ordinary continuous execution route.

These values describe current code support. They are not a runtime permit,
Preflight result, broker connection check, or authorization to place an order.
"""

CONTINUOUS_DISPATCH_HOLD_REASON = "CONTINUOUS_FINAL_DISPATCH_LOCK_ORDER_UNAVAILABLE"


def ordinary_execution_availability() -> dict[str, object]:
    """Describe the held route without importing state or reading user settings."""
    return {
        "schemaVersion": "live-execution-availability-v1",
        "authorizationGranted": False,
        "ordinaryContinuous": {
            "monitorSupported": True,
            "liveDispatchAvailable": False,
            "blockedModes": ["SMALL_LIVE", "FULL_LIVE"],
            "reasonCode": CONTINUOUS_DISPATCH_HOLD_REASON,
            "detail": (
                "현재 일반 자동매매의 실주문 전송은 안전 제어 연결을 "
                "정비하는 동안 차단되어 있습니다."
            ),
            "nextAction": "관찰 모드에서 시세 수신과 전략 판단을 확인할 수 있습니다.",
        },
    }