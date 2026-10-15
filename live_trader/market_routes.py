"""Market classification from route identity, with conservative symbol fallback."""
from __future__ import annotations

import re

from trading_runtime import infer_market_route


_CRYPTO_BROKERS = frozenset({"binance", "binance-futures", "upbit"})
_BROKERS = _CRYPTO_BROKERS | {"kis"}
_PROVIDERS = {"yahoo": "kis", "kis": "kis", **{key: key for key in _CRYPTO_BROKERS}}
_STOCK_ASSETS = frozenset({"한국주식", "미국주식", "주식", "ETF", "STOCK", "STOCKS", "KR_STOCK", "US_STOCK"})
_CRYPTO_ASSETS = frozenset({"코인", "CRYPTO", "CRYPTOCURRENCY"})
_PAIR = re.compile(r"(?:KRW-[A-Z0-9]+|[A-Z0-9]+-?(?:USDT|BUSD)(?:\.PERP)?)\Z")
_AMBIGUOUS_BASES = frozenset({"BTC", "ETH"})


def market_broker_id(symbol: str, *, broker_id: str = "", provider: str = "",
                     market_type: str = "", asset: str = "") -> str:
    """Resolve classification only; this never validates a tradable instrument."""
    broker = str(broker_id or "").strip().lower()
    if broker:
        return broker if broker in _BROKERS else ""
    data_provider = str(provider or "").strip().lower()
    if data_provider:
        return _PROVIDERS.get(data_provider, "")
    text = str(symbol or "").strip().upper()
    category = str(asset or "").strip().upper()
    if not text:
        return ""
    if category in _STOCK_ASSETS:
        return "kis"
    if text in _AMBIGUOUS_BASES:
        return ""
    if _PAIR.fullmatch(text):
        return infer_market_route(text, market_type)[1]
    if category in _CRYPTO_ASSETS or str(market_type).lower() in {"future", "futures", "perpetual"} or text.endswith(".PERP"):
        return ""
    return "kis"


def market_profile(symbol: str, **route: str) -> str:
    broker = market_broker_id(symbol, **route)
    return "crypto" if broker in _CRYPTO_BROKERS else "stock" if broker == "kis" else ""


def market_asset(symbol: str, **route: str) -> str:
    profile = market_profile(symbol, **route)
    if profile == "crypto":
        return "코인"
    if profile != "stock":
        return "미확인"
    text = str(symbol or "").strip().upper().removeprefix("KRX:")
    if text.endswith((".KS", ".KQ")) or (text.isdigit() and len(text) == 6):
        return "한국주식"
    return "미국주식"
