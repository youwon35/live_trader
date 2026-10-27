"""Bounded passive feed display. Inspect cached attributes only; no method calls."""
from datetime import datetime, timezone
import math

def public_feed_observation(feed):
    seen = set()
    leaves = []
    def walk(node, depth=0):
        if depth > 5 or id(node) in seen or len(leaves) >= 64:
            return
        seen.add(id(node))
        data = vars(node) if hasattr(node, '__dict__') else {}
        children = data.get('children', data.get('feeds'))
        if isinstance(children, (tuple, list)) and children:
            for child in children[:64]: walk(child, depth+1)
            return
        connected = data.get('connected') if isinstance(data.get('connected'), bool) else None
        if 'socket' in data:
            connected = data['socket'] is not None
        epoch = data.get('last_wire_received_at')
        received = None
        if isinstance(epoch, (int, float)) and not isinstance(epoch, bool) and math.isfinite(epoch) and epoch > 0:
            try: received = datetime.fromtimestamp(epoch, timezone.utc).isoformat()
            except (OverflowError, ValueError, OSError): pass
        leaves.append({'socketPresent': connected, 'lastFrameAt': received})
    walk(feed)
    return {'schemaVersion': 'short-live-feed-display-v1', 'streams': leaves,
            'meaning': 'CACHED_SOCKET_AND_LAST_PROCESSED_WIRE_RECEIPT_ONLY'}


def public_observed_at(value):
    """Normalize a real reader receipt timestamp; never substitute poll time."""
    try:
        if isinstance(value, (float, int)) and not isinstance(value, bool):
            if not math.isfinite(value) or value <= 0:
                return None
            parsed = datetime.fromtimestamp(value, timezone.utc)
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return None
        else:
            return None
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except (ValueError, OverflowError, OSError):
        return None


def append_equity_observation(history, route, observation):
    """Keep real account samples at most once/minute, bounded per route.

    This display projection never affects sizing, loss limits or admission.
    Native amounts are retained as observed; no pooled currencies or PnL claim.
    """
    from decimal import Decimal, InvalidOperation
    from collections.abc import Mapping
    if route not in {"BINANCE_SPOT", "BINANCE_USDM_FUTURES", "UPBIT_KRW_SPOT", "KIS_KR_CASH"}:
        return False
    if not isinstance(observation, Mapping):
        return False
    equity = observation.get("monitorEquity")
    if not isinstance(equity, Mapping) or equity.get("basis") != "OBSERVED_NATIVE_EQUITY_CHANGE":
        return False
    at = public_observed_at(observation.get("accountObservedAt"))
    if at is None or equity.get("quoteCurrency") not in {"KRW", "USDT", "USD"}:
        return False
    try:
        for field in ("baselineNative", "currentNative", "krwPerQuote"):
            value = equity.get(field)
            if not isinstance(value, (str, int, float)) or isinstance(value, bool):
                return False
            number = Decimal(str(value))
            if not number.is_finite() or number < 0 or (field != "currentNative" and number == 0):
                return False
    except (ValueError, InvalidOperation):
        return False
    fx_at = public_observed_at(equity.get("fxObservedAt"))
    if equity["quoteCurrency"] != "KRW" and fx_at is None:
        return False
    prior = next((row for row in reversed(history) if row["route"] == route), None)
    if prior and (datetime.fromisoformat(at.replace("Z", "+00:00")) -
                  datetime.fromisoformat(prior["observedAt"].replace("Z", "+00:00"))).total_seconds() < 60:
        return False
    row = {key: equity[key] for key in ("baselineNative", "currentNative", "krwPerQuote", "quoteCurrency", "basis")}
    row.update(route=route, observedAt=at,
               baselineObservedAt=public_observed_at(equity.get("baselineObservedAt")),
               fxObservedAt=fx_at,
               fxBasis="IDENTITY_KRW" if equity["quoteCurrency"] == "KRW" else "OBSERVED_FX",
               cashFlowAdjusted=equity.get("cashFlowAdjusted") is True,
               preservedSpotExcluded=equity.get("preservedSpotExcluded") is True)
    history.append(row)
    route_rows = [index for index, item in enumerate(history) if item["route"] == route]
    if len(route_rows) > 720:
        remove = set(route_rows[:-720])
        history[:] = [item for index, item in enumerate(history) if index not in remove]
    history.sort(key=lambda item: datetime.fromisoformat(item["observedAt"].replace("Z", "+00:00")))
    return True
