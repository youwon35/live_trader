"""Display-only projection of an existing evaluated closed bar."""
import math
import re
from typing import Any

from trading_runtime.continuous_runtime import ClosedBar, StrategyBarDecision

_CALCULATION_FIELDS = (
    "historyCount", "shortPeriod", "longPeriod", "previousShort", "previousLong",
    "currentShort", "currentLong", "regimeMa", "period", "rsi", "oversold", "exit",
    "window", "k", "averageRange", "trigger", "barHigh", "exitMa", "entryBelow",
    "exitAbove", "warmupBars", "warmupCount",
)
_REASON_NUMBER = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
_REASONS = re.compile("|".join((
    r"(?:(?:MA|RSI|Breakout) 워밍업|전략 빌더 (?:재)?워밍업) 중입니다: \d+/\d+개 확정 봉\.",
    r"MA\(\d+/\d+\) (?:상향 교차가 확정됐습니다\.|하향 교차가 확정됐습니다\.|교차 없음\. 다음 확정 봉에서 다시 평가합니다\.)",
    r"(?:MA 상향 교차지만|RSI 진입 조건이지만|Breakout 돌파지만) 종가가 추세 필터 MA\(\d+\) 아래라 진입하지 않습니다\.",
    rf"RSI\(\d+\) {_REASON_NUMBER}(?:: 진입/청산 조건 없음\. 다음 확정 봉에서 다시 평가합니다\.| ≤ {_REASON_NUMBER} 매수 조건입니다\.| ≥ {_REASON_NUMBER} 매도 조건입니다\.)",
    rf"확정 봉 고가 {_REASON_NUMBER}가 Breakout 기준 {_REASON_NUMBER} 이상입니다\.",
    rf"Breakout 기준 {_REASON_NUMBER} 미충족\. 다음 확정 봉에서 다시 평가합니다\.",
    r"종가가 (?:Breakout 청산 MA\(\d+\) 아래로 확정됐습니다\.|청산 MA\(\d+\) 위라 포지션을 유지합니다\.)",
    rf"종가 {_REASON_NUMBER}가 (?:진입 기준 {_REASON_NUMBER} 이하입니다\.|청산 기준 {_REASON_NUMBER} 이상입니다\.)",
    r"Threshold 조건 없음\. 다음 확정 봉에서 다시 평가합니다\.",
    r"전략 빌더 (?:entryRules|exitRules) 조건을 기다립니다\.",
    r"전략 빌더 filterRules가 신규 진입을 차단했습니다\.",
    r"데이터 연속성 안전 차단: (?:stale-data:\d+(?:\.\d+)?s>\d+s(?:, missing-bars:\d+)?|missing-bars:\d+)",
)))


def _number(value):
    return (type(value) is int and abs(value) <= 9007199254740991) or (
        type(value) is float and math.isfinite(value)
    )


def public_calculation_note(decision: StrategyBarDecision) -> dict[str, Any] | None:
    """Exclude all identifiers and unrecognized reason bodies from the view."""
    if not isinstance(decision, StrategyBarDecision) or decision.signal not in ("BUY", "SELL", "HOLD"):
        return None
    bar = decision.bar
    if (not isinstance(bar, ClosedBar) or not all(isinstance(value, str) and len(value) <= limit
            for value, limit in ((bar.symbol, 32), (bar.timeframe, 5), (bar.end_time, 35)))):
        return None
    source = decision.metadata if isinstance(decision.metadata, dict) else {}
    metadata = {key: source[key] for key in _CALCULATION_FIELDS if key in source and _number(source[key])}
    row = {
        "symbol": bar.symbol, "timeframe": bar.timeframe, "barEnd": bar.end_time,
        "signal": decision.signal, "metadata": metadata,
    }
    if _number(bar.close):
        row["close"] = bar.close
    reason = decision.reason
    if isinstance(reason, str) and len(reason) <= 160 and _REASONS.fullmatch(reason):
        row["reason"] = reason
    return row
