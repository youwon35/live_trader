from __future__ import annotations

from pathlib import Path
import sys


def _ensure_shared_runtime_path() -> None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "packages" / "trading_runtime"
        if candidate.exists():
            path = str(candidate)
            if path not in sys.path:
                sys.path.insert(0, path)
            return


_ensure_shared_runtime_path()

from trading_runtime.order_management import OrderIntent, OrderSide, TradingOrderMode  # noqa: E402

LiveOrderMode = TradingOrderMode

__all__ = ["LiveOrderMode", "OrderIntent", "OrderSide", "TradingOrderMode"]
