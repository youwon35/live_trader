from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from .order_management import LiveOrderMode, OrderIntent, OrderSide


RiskStatus = Literal["pass", "warn", "fail"]


@dataclass(frozen=True)
class RecentOrder:
    strategy_id: str
    symbol: str
    side: OrderSide
    occurred_at: datetime
    state: str


@dataclass(frozen=True)
class PreTradeContext:
    mode: LiveOrderMode
    dry_run: bool = True
    halted: bool = False
    new_entries_blocked: bool = False
    readiness_blockers: int = 0
    readiness_warnings: int = 0
    real_orders_enabled: bool = False
    live_order_adapter_verified: bool = False
    asset_enabled: bool = True
    market_orderable: bool = True
    duplicate_order_guard_enabled: bool = True
    max_order_value_guard_enabled: bool = True
    max_order_value: float = 20_000_000
    cooldown_seconds: int = 180
    symbol_weight_guard_enabled: bool = True
    symbol_exposure: float = 0.0
    portfolio_equity: float | None = None
    max_symbol_weight_pct: float = 20.0
    daily_loss_guard_enabled: bool = True
    daily_pnl_pct: float = 0.0
    daily_loss_limit_pct: float = -2.0
    strategy_exposure: float = 0.0
    strategy_capital_limit: float = 20_000_000
    available_cash: float | None = None
    estimated_slippage_bps: float = 0.0
    max_slippage_bps: float = 50.0
    max_open_orders_guard_enabled: bool = True
    open_order_count: int = 0
    max_open_orders: int = 5
    position_mismatch_guard_enabled: bool = True
    positions_matched: bool = True
    recent_orders: tuple[RecentOrder, ...] = ()


@dataclass(frozen=True)
class RiskCheck:
    label: str
    status: RiskStatus
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {"label": self.label, "status": self.status, "detail": self.detail}


@dataclass(frozen=True)
class PreTradeRiskReport:
    checked_at: datetime
    checks: tuple[RiskCheck, ...]

    @property
    def blockers(self) -> tuple[RiskCheck, ...]:
        return tuple(check for check in self.checks if check.status == "fail")

    @property
    def can_submit(self) -> bool:
        return not self.blockers

    @property
    def summary(self) -> str:
        if self.can_submit:
            return "실거래 주문 전 리스크 게이트를 통과했습니다."
        return "; ".join(f"{check.label}: {check.detail}" for check in self.blockers)

    def to_dict(self) -> dict[str, object]:
        return {
            "checked_at": self.checked_at.strftime("%Y-%m-%d %H:%M:%S"),
            "can_submit": self.can_submit,
            "summary": self.summary,
            "checks": [check.to_dict() for check in self.checks],
        }


class PreTradeRiskGate:
    """Paper Trader와 같은 OrderIntent -> RiskGate 경계로 live 주문 의도를 검문한다."""

    def evaluate(
        self,
        intent: OrderIntent,
        context: PreTradeContext,
        now: datetime | None = None,
    ) -> PreTradeRiskReport:
        current = now or datetime.now()
        checks: list[RiskCheck] = []

        if context.mode == "MONITOR":
            status: RiskStatus = "pass" if context.dry_run else "fail"
            detail = "MONITOR에서는 브로커 전송 없이 주문 의도만 기록합니다." if context.dry_run else "MONITOR에서는 실제 브로커 주문을 차단합니다."
            checks.append(RiskCheck("운용 모드", status, detail))
        else:
            checks.append(RiskCheck("운용 모드", "pass", f"{context.mode} 주문 검문을 진행합니다."))

        if context.halted:
            checks.append(RiskCheck("Kill Switch", "fail", "긴급 정지 상태입니다."))
        else:
            checks.append(RiskCheck("Kill Switch", "pass", "긴급 정지 상태가 아닙니다."))

        if context.new_entries_blocked and intent.side == "BUY":
            checks.append(RiskCheck("신규 진입 차단", "fail", "신규 매수/진입 주문 차단이 켜져 있습니다."))
        else:
            checks.append(RiskCheck("신규 진입 차단", "pass", "현재 주문 방향은 신규 진입 차단에 걸리지 않습니다."))

        if context.readiness_blockers > 0:
            checks.append(RiskCheck("Readiness", "fail", f"readiness blocker {context.readiness_blockers}개가 남아 있습니다."))
        elif context.readiness_warnings > 0:
            checks.append(RiskCheck("Readiness", "warn", f"readiness warning {context.readiness_warnings}개가 남아 있습니다."))
        else:
            checks.append(RiskCheck("Readiness", "pass", "실거래 준비 hard blocker가 없습니다."))

        if context.dry_run:
            checks.append(RiskCheck("Dry Run", "pass", "브로커 전송 없이 감사 로그에만 기록합니다."))
        elif not context.real_orders_enabled:
            checks.append(RiskCheck("실거래 환경 변수", "fail", "LIVE_TRADER_ENABLE_REAL_ORDERS=true가 필요합니다."))
        else:
            checks.append(RiskCheck("실거래 환경 변수", "pass", "실거래 환경 변수가 켜져 있습니다."))

        if context.dry_run:
            checks.append(RiskCheck("실주문 어댑터", "pass", "Dry Run이라 실주문 어댑터 전송을 사용하지 않습니다."))
        elif context.live_order_adapter_verified:
            checks.append(RiskCheck("실주문 어댑터", "pass", "실주문 어댑터가 검증되었습니다."))
        else:
            checks.append(RiskCheck("실주문 어댑터", "fail", "실제 주문 어댑터 안전 검증 전이므로 브로커 전송을 차단합니다."))

        if context.asset_enabled:
            checks.append(RiskCheck("자산군 스위치", "pass", f"{intent.asset} 라우트가 켜져 있습니다."))
        else:
            checks.append(RiskCheck("자산군 스위치", "fail", f"{intent.asset} 라우트가 꺼져 있습니다."))

        if context.market_orderable:
            checks.append(RiskCheck("시장 시간", "pass", "현재 주문 가능 세션으로 평가했습니다."))
        else:
            checks.append(RiskCheck("시장 시간", "fail", "현재 세션에서는 신규 주문을 차단합니다."))

        if intent.quantity > 0 and intent.reference_price > 0:
            checks.append(RiskCheck("주문 값", "pass", "수량과 기준 가격이 양수입니다."))
        else:
            checks.append(RiskCheck("주문 값", "fail", "수량과 기준 가격은 0보다 커야 합니다."))

        if not context.max_order_value_guard_enabled:
            checks.append(RiskCheck("주문 금액 한도", "pass", "최대주문금액 제한이 비활성화되어 있습니다."))
        elif intent.notional <= context.max_order_value:
            checks.append(RiskCheck("주문 금액 한도", "pass", f"주문 금액 {intent.notional:,.0f} <= {context.max_order_value:,.0f}"))
        else:
            checks.append(RiskCheck("주문 금액 한도", "fail", f"주문 금액 {intent.notional:,.0f}이 한도 {context.max_order_value:,.0f}을 초과했습니다."))

        if not context.daily_loss_guard_enabled:
            checks.append(RiskCheck("일일 손실 한도", "pass", "일일 최대 손실 제한이 비활성화되어 있습니다."))
        elif context.daily_pnl_pct <= context.daily_loss_limit_pct:
            checks.append(RiskCheck("일일 손실 한도", "fail", f"오늘 손익률 {context.daily_pnl_pct:.2f}%가 한도 {context.daily_loss_limit_pct:.2f}% 이하입니다."))
        else:
            checks.append(RiskCheck("일일 손실 한도", "pass", f"오늘 손익률 {context.daily_pnl_pct:.2f}%가 손실 한도 안에 있습니다."))

        projected_symbol_exposure = context.symbol_exposure + intent.notional if intent.side == "BUY" else max(0.0, context.symbol_exposure - intent.notional)
        if not context.symbol_weight_guard_enabled:
            checks.append(RiskCheck("종목별 최대 비중", "pass", "종목별 최대 비중 제한이 비활성화되어 있습니다."))
        elif context.portfolio_equity is None or context.portfolio_equity <= 0:
            checks.append(RiskCheck("종목별 최대 비중", "warn", "계좌 평가금액이 없어 비중을 계산하지 못했습니다."))
        else:
            projected_weight_pct = (projected_symbol_exposure / context.portfolio_equity) * 100
            if projected_weight_pct <= context.max_symbol_weight_pct:
                checks.append(RiskCheck("종목별 최대 비중", "pass", f"예상 비중 {projected_weight_pct:.2f}% <= {context.max_symbol_weight_pct:.2f}%"))
            else:
                checks.append(RiskCheck("종목별 최대 비중", "fail", f"예상 비중 {projected_weight_pct:.2f}%가 한도 {context.max_symbol_weight_pct:.2f}%를 초과했습니다."))

        projected_exposure = context.strategy_exposure + intent.notional if intent.side == "BUY" else max(0.0, context.strategy_exposure - intent.notional)
        if projected_exposure <= context.strategy_capital_limit:
            checks.append(RiskCheck("전략별 자본 한도", "pass", f"예상 노출 {projected_exposure:,.0f} <= {context.strategy_capital_limit:,.0f}"))
        else:
            checks.append(RiskCheck("전략별 자본 한도", "fail", f"예상 노출 {projected_exposure:,.0f}이 전략 한도 {context.strategy_capital_limit:,.0f}을 초과했습니다."))

        if context.available_cash is None or intent.side == "SELL":
            checks.append(RiskCheck("현금 잔고", "pass", "현재 주문은 현금 부족 차단 대상이 아닙니다."))
        elif intent.notional <= context.available_cash:
            checks.append(RiskCheck("현금 잔고", "pass", f"주문 금액 {intent.notional:,.0f} <= 가용 현금 {context.available_cash:,.0f}"))
        else:
            checks.append(RiskCheck("현금 잔고", "fail", f"주문 금액 {intent.notional:,.0f}이 가용 현금 {context.available_cash:,.0f}을 초과했습니다."))

        if context.estimated_slippage_bps <= context.max_slippage_bps:
            checks.append(RiskCheck("슬리피지 한도", "pass", f"예상 슬리피지 {context.estimated_slippage_bps:.1f} bps <= {context.max_slippage_bps:.1f} bps"))
        else:
            checks.append(RiskCheck("슬리피지 한도", "fail", f"예상 슬리피지 {context.estimated_slippage_bps:.1f} bps가 한도 {context.max_slippage_bps:.1f} bps를 넘었습니다."))

        if not context.max_open_orders_guard_enabled:
            checks.append(RiskCheck("최대 열린 주문", "pass", "최대 열린 주문 제한이 비활성화되어 있습니다."))
        elif context.open_order_count < context.max_open_orders:
            checks.append(RiskCheck("최대 열린 주문", "pass", f"열린 주문 {context.open_order_count}건 < 한도 {context.max_open_orders}건"))
        else:
            checks.append(RiskCheck("최대 열린 주문", "fail", f"열린 주문 {context.open_order_count}건이 한도 {context.max_open_orders}건에 도달했습니다."))

        if not context.position_mismatch_guard_enabled:
            checks.append(RiskCheck("포지션 불일치 차단", "pass", "포지션 불일치시 주문 금지가 비활성화되어 있습니다."))
        elif context.positions_matched:
            checks.append(RiskCheck("포지션 불일치 차단", "pass", "프로그램 포지션과 브로커 포지션이 일치합니다."))
        else:
            checks.append(RiskCheck("포지션 불일치 차단", "fail", "포지션 불일치가 있어 주문을 금지합니다."))

        if not context.duplicate_order_guard_enabled:
            checks.append(RiskCheck("중복 주문 쿨다운", "pass", "중복주문방지가 비활성화되어 있습니다."))
        else:
            cooldown_cutoff = current - timedelta(seconds=context.cooldown_seconds)
            duplicate = next(
                (
                    order
                    for order in context.recent_orders
                    if order.strategy_id == intent.strategy_id
                    and order.symbol == intent.symbol
                    and order.side == intent.side
                    and order.occurred_at >= cooldown_cutoff
                    and order.state in {"dry_run", "sent", "filled", "submitted"}
                ),
                None,
            )
            if duplicate:
                checks.append(RiskCheck("중복 주문 쿨다운", "fail", f"{context.cooldown_seconds}초 이내 같은 방향 주문이 있습니다."))
            else:
                checks.append(RiskCheck("중복 주문 쿨다운", "pass", "쿨다운에 걸리는 같은 방향 주문이 없습니다."))

        return PreTradeRiskReport(current, tuple(checks))
