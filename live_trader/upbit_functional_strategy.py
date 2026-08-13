from __future__ import annotations

"""Sealed natural-signal evaluator for the published KRW-BTC MA strategy.

The production graph supplies a trusted finalized-bar window reader.  This
module accepts no signal input: it revalidates the immutable publication on
every evaluation and derives BUY/SELL/HOLD solely from official 5-minute close
prices using the published moving-average-cross parameters (3/10).
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import re
from typing import Any, Callable, Mapping

from .upbit_continuous_functional import (
    SYMBOL,
    UpbitFunctionalBlocked,
    UpbitPermitScope,
)


_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
_FORBIDDEN_SIGNAL_FIELDS = frozenset(
    {
        "signal",
        "naturalSignal",
        "forcedSignal",
        "manualSignal",
        "signalOverrideUsed",
        "evaluationId",
    }
)


def _text(value: object) -> str:
    return str(value or "").strip()


def _utc(value: object, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise UpbitFunctionalBlocked(f"upbit-evaluator-{label}-invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UpbitFunctionalBlocked(
            f"upbit-evaluator-{label}-timezone-missing"
        )
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise UpbitFunctionalBlocked(f"upbit-evaluator-{label}-invalid") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise UpbitFunctionalBlocked(f"upbit-evaluator-{label}-invalid")
    return parsed


def _stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


class SealedUpbitMovingAverageEvaluator:
    def __init__(
        self,
        *,
        scope: UpbitPermitScope,
        immutable_selection_reader: Callable[[], Mapping[str, Any]],
        clock: Callable[[], datetime],
    ) -> None:
        self.scope = scope
        self.immutable_selection_reader = immutable_selection_reader
        self.clock = clock

    def _assert_selection(self) -> Mapping[str, Any]:
        selection = dict(self.immutable_selection_reader())
        exact = {
            "strategyArtifactId": self.scope.strategy_artifact_id,
            "strategyArtifactHash": self.scope.strategy_artifact_hash,
            "strategyArtifactFileSha256": self.scope.strategy_artifact_file_sha256,
            "strategyInstanceId": self.scope.strategy_instance_id,
            "strategyInstanceHash": self.scope.strategy_instance_hash,
            "strategyInstanceFileSha256": self.scope.strategy_instance_file_sha256,
            "publicationProofHash": self.scope.publication_proof_hash,
            "publicationProofFileSha256": self.scope.publication_proof_file_sha256,
            "strategyPluginId": "moving_average_cross",
        }
        for field, expected in exact.items():
            if not hmac.compare_digest(_text(selection.get(field)), expected):
                raise UpbitFunctionalBlocked(
                    f"upbit-evaluator-selection-{field}-mismatch"
                )
        if (
            selection.get("verified") is not True
            or selection.get("publicationProofVerified") is not True
            or int(selection.get("strategyShortMa") or 0) != 3
            or int(selection.get("strategyLongMa") or 0) != 10
        ):
            raise UpbitFunctionalBlocked(
                "upbit-evaluator-published-contract-invalid"
            )
        return selection

    def evaluate(self, raw_window: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw_window, Mapping) or any(
            field in raw_window for field in _FORBIDDEN_SIGNAL_FIELDS
        ):
            raise UpbitFunctionalBlocked(
                "upbit-evaluator-caller-signal-forbidden"
            )
        if (
            _text(raw_window.get("schemaVersion"))
            != "upbit-official-finalized-5m-window-v1"
            or _text(raw_window.get("symbol")).upper() != SYMBOL
            or _text(raw_window.get("interval")).lower() != "5m"
            or _text(raw_window.get("source")).upper()
            not in {"UPBIT_WEBSOCKET", "UPBIT_REST"}
            or raw_window.get("finalized") is not True
            or raw_window.get("closed") is not True
        ):
            raise UpbitFunctionalBlocked(
                "upbit-evaluator-official-window-contract-invalid"
            )
        selection = self._assert_selection()
        rows = raw_window.get("bars")
        if not isinstance(rows, list) or len(rows) != 11 or any(
            not isinstance(row, Mapping) for row in rows
        ):
            raise UpbitFunctionalBlocked(
                "upbit-evaluator-history-window-incomplete"
            )
        parsed: list[tuple[str, datetime, Decimal]] = []
        for row in rows:
            if any(field in row for field in _FORBIDDEN_SIGNAL_FIELDS):
                raise UpbitFunctionalBlocked(
                    "upbit-evaluator-caller-signal-forbidden"
                )
            bar_id = _text(row.get("barId"))
            closed_at = _utc(row.get("closedAt"), "bar-closed-at")
            if (
                _SAFE_ID_RE.fullmatch(bar_id) is None
                or row.get("finalized") is not True
                or row.get("closed") is not True
                or closed_at.second != 0
                or closed_at.microsecond != 0
                or closed_at.minute % 5 != 0
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-evaluator-bar-contract-invalid"
                )
            parsed.append((bar_id, closed_at, _decimal(row.get("close"), "close")))
        for previous, current in zip(parsed, parsed[1:]):
            if current[1] - previous[1] != timedelta(minutes=5):
                raise UpbitFunctionalBlocked(
                    "upbit-evaluator-window-not-contiguous"
                )
        now = _utc(self.clock(), "current-time")
        final_id, final_closed_at, _final_close = parsed[-1]
        if final_closed_at > now or now - final_closed_at > timedelta(minutes=10):
            raise UpbitFunctionalBlocked(
                "upbit-evaluator-final-bar-stale-or-future"
            )
        if (
            _text(raw_window.get("barId")) != final_id
            or _utc(raw_window.get("closedAt"), "window-closed-at")
            != final_closed_at
        ):
            raise UpbitFunctionalBlocked(
                "upbit-evaluator-final-bar-identity-mismatch"
            )
        closes = [row[2] for row in parsed]
        previous_short = sum(closes[-4:-1], Decimal("0")) / Decimal("3")
        previous_long = sum(closes[-11:-1], Decimal("0")) / Decimal("10")
        current_short = sum(closes[-3:], Decimal("0")) / Decimal("3")
        current_long = sum(closes[-10:], Decimal("0")) / Decimal("10")
        if previous_short <= previous_long and current_short > current_long:
            signal = "BUY"
        elif previous_short >= previous_long and current_short < current_long:
            signal = "SELL"
        else:
            signal = "HOLD"
        window_hash = _stable_hash(dict(raw_window))
        evaluation_id = "upbit-ma-eval-" + _stable_hash(
            {
                "windowHash": window_hash,
                "strategyArtifactHash": self.scope.strategy_artifact_hash,
                "strategyInstanceHash": self.scope.strategy_instance_hash,
            }
        )[:32]
        return {
            "schemaVersion": "upbit-natural-ma-evaluation/v1",
            "symbol": SYMBOL,
            "interval": "5m",
            "finalized": True,
            "closed": True,
            "source": _text(raw_window.get("source")).upper(),
            "barId": final_id,
            "barHash": window_hash,
            "closedAt": _utc_text(final_closed_at),
            "signal": signal,
            "evaluationId": evaluation_id,
            "strategyEvaluationComplete": True,
            "naturalSignal": True,
            "forcedSignal": False,
            "signalOverrideUsed": False,
            "manualSignal": False,
            "strategyArtifactId": self.scope.strategy_artifact_id,
            "strategyArtifactHash": self.scope.strategy_artifact_hash,
            "strategyArtifactFileSha256": self.scope.strategy_artifact_file_sha256,
            "strategyInstanceId": self.scope.strategy_instance_id,
            "strategyInstanceHash": self.scope.strategy_instance_hash,
            "strategyInstanceFileSha256": self.scope.strategy_instance_file_sha256,
            "publicationProofHash": self.scope.publication_proof_hash,
            "publicationProofFileSha256": self.scope.publication_proof_file_sha256,
            "strategyPluginId": selection["strategyPluginId"],
            "strategyShortMa": 3,
            "strategyLongMa": 10,
            # The exact official input is part of the immutable evaluation
            # proof.  Downstream persistence and the terminal consumer hash
            # and independently replay this window; a caller-provided signal
            # can therefore never stand in for the published MA(3/10).
            "rawFinalizedWindow": json.loads(
                json.dumps(
                    dict(raw_window),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            ),
        }


__all__ = ["SealedUpbitMovingAverageEvaluator"]
