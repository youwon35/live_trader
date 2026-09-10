"""Read-only ordinary route support; this projection never grants authority."""

CONTINUOUS_DISPATCH_HOLD_REASON = "CONTINUOUS_FINAL_DISPATCH_LOCK_ORDER_UNAVAILABLE"


def ordinary_execution_availability(runtime=None) -> dict[str, object]:
    """Project already-collected queue health without importing runtime state."""
    profiles = runtime.get("profiles") if isinstance(runtime, dict) else None
    known = isinstance(profiles, dict) and all(
        isinstance(profiles.get(key), dict)
        and isinstance(profiles[key].get("dispatch"), dict)
        and type(profiles[key]["dispatch"].get("reconciliationRequired")) is bool
        for key in ("stock", "crypto")
    )
    held = not known or any(
        profiles[key]["dispatch"]["reconciliationRequired"]
        for key in ("stock", "crypto")
    )
    return {
        "schemaVersion": "live-execution-availability-v1",
        "authorizationGranted": False,
        "ordinaryContinuous": {
            "monitorSupported": True,
            "liveDispatchAvailable": not held,
            "blockedModes": ["SMALL_LIVE", "FULL_LIVE"] if held else [],
            "reasonCode": (
                "CONTINUOUS_DISPATCH_STATUS_UNAVAILABLE" if not known else
                "CONTINUOUS_DISPATCH_RECONCILIATION_REQUIRED" if held else
                "CONTINUOUS_DISPATCH_IMPLEMENTED"
            ),
            "detail": (
                "이전 주문의 전송 결과를 확인해야 실주문을 다시 시작할 수 있습니다."
                if known and held else
                "주문 대기열의 상태를 확인하지 못했습니다."
                if not known else
                "일반 자동매매 전송 경로가 준비되어 있습니다. 주문별 안전 검사와 운영자 승인이 필요합니다."
            ),
            "nextAction": (
                "대상 포트폴리오를 관찰 모드로 불러온 뒤 주문·계좌 대조 결과를 확인하세요."
                if held else "대상 전략의 사전 점검과 계좌·위험 한도·실주문 승인을 확인하세요."
            ),
        },
    }
