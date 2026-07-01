from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


LiveOrderMode = Literal["MONITOR", "SMALL_LIVE", "FULL_LIVE"]
OrderSide = Literal["BUY", "SELL"]


@dataclass(frozen=True)
class OrderIntent:
    strategy_id: str
    asset: str
    symbol: str
    side: OrderSide
    quantity: float
    reference_price: float
    mode: LiveOrderMode
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def notional(self) -> float:
        return self.quantity * self.reference_price
