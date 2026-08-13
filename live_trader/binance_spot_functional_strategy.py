from __future__ import annotations

"""Official closed-kline source and sealed MA evaluator for the Binance lane.

The production composition owns both objects.  Callers cannot provide a bar or
signal: the reader fetches Binance server time plus the exact BTCUSDT 5-minute
kline window, and the evaluator re-verifies the published Strategy Artifact
and Strategy Instance bytes before deriving the fixed MA(3/10) crossover.
"""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
from pathlib import Path
import re
import time
from typing import Any, Callable, Mapping

from .binance_spot_continuous_functional import (
    EXECUTION_ROUTE,
    INTERVAL,
    SYMBOL,
    BinanceSpotFunctionalError,
    ExactBinding,
)
from .binance_spot_functional_transport import (
    BINANCE_SPOT_KLINES_ENDPOINT,
    BINANCE_SPOT_TIME_ENDPOINT,
    OfficialBinanceSpotGetClient,
)


WINDOW_SIZE = 11
FETCH_LIMIT = 13
INTERVAL_MILLISECONDS = 5 * 60 * 1000
MAX_SERVER_CLOCK_SKEW_SECONDS = 15.0
_SAFE_BAR_ID = re.compile(r"^BTCUSDT-5m-[0-9]{10,16}$")
_FORBIDDEN_SIGNAL_FIELDS = frozenset(
    {
        "signal",
        "naturalSignal",
        "forced",
        "forcedSignal",
        "manualSignal",
        "signalOverrideUsed",
        "evaluationId",
    }
)


class BinanceSpotKlineBoundaryRace(BinanceSpotFunctionalError):
    """A 5-minute boundary moved while the official window was sampled."""

    transient_market_data = True


def _text(value: object) -> str:
    return str(value or "").strip()


def _decimal(value: object, *, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BinanceSpotFunctionalError(
            f"Binance sealed evaluator {label} is invalid"
        ) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise BinanceSpotFunctionalError(
            f"Binance sealed evaluator {label} is invalid"
        )
    return parsed


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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


def _iso_from_milliseconds(value: int) -> str:
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


class OfficialBinanceSpotFinalizedKlineReader:
    """Read one complete, contiguous, officially closed 11-bar window."""

    def __init__(
        self,
        *,
        client: OfficialBinanceSpotGetClient,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.client = client
        self.clock = clock

    def read_window(self) -> dict[str, Any]:
        server = self.client.get(BINANCE_SPOT_TIME_ENDPOINT, {})
        if not isinstance(server, Mapping):
            raise BinanceSpotFunctionalError(
                "Binance official server-time response is malformed"
            )
        try:
            server_milliseconds = int(server.get("serverTime"))
        except (TypeError, ValueError) as exc:
            raise BinanceSpotFunctionalError(
                "Binance official server time is invalid"
            ) from exc
        local_milliseconds = int(float(self.clock()) * 1000)
        if (
            server_milliseconds <= 0
            or abs(local_milliseconds - server_milliseconds)
            > int(MAX_SERVER_CLOCK_SKEW_SECONDS * 1000)
        ):
            raise BinanceSpotFunctionalError(
                "Binance official server time is stale or locally skewed"
            )
        pre_boundary = server_milliseconds // INTERVAL_MILLISECONDS
        payload = self.client.get(
            BINANCE_SPOT_KLINES_ENDPOINT,
            {"symbol": SYMBOL, "interval": INTERVAL, "limit": FETCH_LIMIT},
        )
        post_server = self.client.get(BINANCE_SPOT_TIME_ENDPOINT, {})
        if not isinstance(post_server, Mapping):
            raise BinanceSpotFunctionalError(
                "Binance official post-kline server-time response is malformed"
            )
        try:
            post_milliseconds = int(post_server.get("serverTime"))
        except (TypeError, ValueError) as exc:
            raise BinanceSpotFunctionalError(
                "Binance official post-kline server time is invalid"
            ) from exc
        if post_milliseconds < server_milliseconds:
            raise BinanceSpotFunctionalError(
                "Binance official server time moved backwards"
            )
        post_boundary = post_milliseconds // INTERVAL_MILLISECONDS
        if post_boundary != pre_boundary:
            # A request spanning the close boundary can contain a different
            # current/open row.  Refetch exactly once under the post boundary.
            payload = self.client.get(
                BINANCE_SPOT_KLINES_ENDPOINT,
                {"symbol": SYMBOL, "interval": INTERVAL, "limit": FETCH_LIMIT},
            )
            confirmed = self.client.get(BINANCE_SPOT_TIME_ENDPOINT, {})
            if not isinstance(confirmed, Mapping):
                raise BinanceSpotKlineBoundaryRace(
                    "Binance boundary confirmation is unavailable"
                )
            try:
                confirmed_milliseconds = int(confirmed.get("serverTime"))
            except (TypeError, ValueError) as exc:
                raise BinanceSpotKlineBoundaryRace(
                    "Binance boundary confirmation is invalid"
                ) from exc
            if (
                confirmed_milliseconds < post_milliseconds
                or confirmed_milliseconds // INTERVAL_MILLISECONDS
                != post_boundary
            ):
                raise BinanceSpotKlineBoundaryRace(
                    "Binance 5-minute boundary moved twice; retry without evaluation"
                )
            post_milliseconds = confirmed_milliseconds
        server_milliseconds = post_milliseconds
        if (
            abs(local_milliseconds - server_milliseconds)
            > int(MAX_SERVER_CLOCK_SKEW_SECONDS * 1000)
        ):
            raise BinanceSpotFunctionalError(
                "Binance post-kline server time is locally skewed"
            )
        if not isinstance(payload, list) or any(
            not isinstance(row, list) for row in payload
        ):
            raise BinanceSpotFunctionalError(
                "Binance official kline response is malformed"
            )
        closed_rows: list[dict[str, Any]] = []
        for raw in payload:
            if len(raw) < 12:
                raise BinanceSpotFunctionalError(
                    "Binance official kline row is incomplete"
                )
            try:
                opened_ms = int(raw[0])
                closed_ms = int(raw[6])
                trade_count = int(raw[8])
            except (TypeError, ValueError) as exc:
                raise BinanceSpotFunctionalError(
                    "Binance official kline identity is invalid"
                ) from exc
            if opened_ms % INTERVAL_MILLISECONDS != 0 or (
                closed_ms != opened_ms + INTERVAL_MILLISECONDS - 1
            ):
                raise BinanceSpotFunctionalError(
                    "Binance official kline boundary changed"
                )
            if closed_ms >= server_milliseconds:
                continue
            opened = _decimal(raw[1], label="open")
            high = _decimal(raw[2], label="high")
            low = _decimal(raw[3], label="low")
            close = _decimal(raw[4], label="close")
            volume = _decimal(raw[5], label="volume")
            if high < max(opened, close) or low > min(opened, close) or high < low:
                raise BinanceSpotFunctionalError(
                    "Binance official kline OHLC relation is invalid"
                )
            if trade_count < 0:
                raise BinanceSpotFunctionalError(
                    "Binance official kline trade count is invalid"
                )
            close_boundary_ms = opened_ms + INTERVAL_MILLISECONDS
            closed_rows.append(
                {
                    "barId": f"{SYMBOL}-{INTERVAL}-{opened_ms}",
                    "openTime": _iso_from_milliseconds(opened_ms),
                    "barCloseAt": _iso_from_milliseconds(close_boundary_ms),
                    "open": format(opened, "f"),
                    "high": format(high, "f"),
                    "low": format(low, "f"),
                    "close": format(close, "f"),
                    "volume": format(volume, "f"),
                    "tradeCount": trade_count,
                    "finalized": True,
                    "closed": True,
                }
            )
        if len(closed_rows) < WINDOW_SIZE:
            raise BinanceSpotFunctionalError(
                "Binance official finalized 5-minute history is incomplete"
            )
        rows = closed_rows[-WINDOW_SIZE:]
        for previous, current in zip(rows, rows[1:]):
            previous_ms = int(previous["barId"].rsplit("-", 1)[1])
            current_ms = int(current["barId"].rsplit("-", 1)[1])
            if current_ms - previous_ms != INTERVAL_MILLISECONDS:
                raise BinanceSpotFunctionalError(
                    "Binance official finalized kline window has a gap"
                )
        final = rows[-1]
        latest_official_close_boundary = (
            server_milliseconds // INTERVAL_MILLISECONDS
        ) * INTERVAL_MILLISECONDS
        final_open_ms = int(final["barId"].rsplit("-", 1)[1])
        if final_open_ms + INTERVAL_MILLISECONDS != latest_official_close_boundary:
            raise BinanceSpotFunctionalError(
                "Binance official finalized kline window is not the latest closed bar"
            )
        return {
            "schemaVersion": "binance-spot-official-finalized-5m-window-v1",
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "source": "BINANCE_SPOT_KLINE",
            "finalized": True,
            "closed": True,
            "barId": final["barId"],
            "barCloseAt": final["barCloseAt"],
            "observedAt": _iso_from_milliseconds(server_milliseconds),
            "serverTime": server_milliseconds,
            "rawKlines": [list(row) for row in payload],
            "rawKlinesHash": _stable_hash({"rows": payload}),
            "klineRequest": {
                "endpoint": BINANCE_SPOT_KLINES_ENDPOINT,
                "query": {
                    "symbol": SYMBOL,
                    "interval": INTERVAL,
                    "limit": FETCH_LIMIT,
                },
            },
            "bars": rows,
        }


class SealedBinanceSpotMovingAverageEvaluator:
    """Derive MA(3/10) from official bars and byte-exact publication files."""

    def __init__(
        self,
        *,
        binding_reader: Callable[[], Mapping[str, Any]],
        publication_verifier: Callable[[ExactBinding], Mapping[str, Any]],
    ) -> None:
        self.binding_reader = binding_reader
        self.publication_verifier = publication_verifier

    @staticmethod
    def _read_exact_json(
        path_value: object, expected_sha: str, *, label: str
    ) -> dict[str, Any]:
        path = Path(_text(path_value))
        try:
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BinanceSpotFunctionalError(
                f"Binance sealed evaluator {label} is unreadable"
            ) from exc
        if not isinstance(payload, dict) or not hmac.compare_digest(
            _sha256_bytes(raw), expected_sha
        ):
            raise BinanceSpotFunctionalError(
                f"Binance sealed evaluator {label} byte identity changed"
            )
        return payload

    def _selection(self) -> tuple[ExactBinding, dict[str, Any]]:
        binding = ExactBinding.parse(self.binding_reader())
        verified = dict(self.publication_verifier(binding))
        expected = {
            "complete": True,
            "strategyArtifactHash": binding.strategy_artifact_hash,
            "artifactFileSha256": binding.artifact_file_sha256,
            "strategyInstanceHash": binding.strategy_instance_hash,
            "instanceFileSha256": binding.instance_file_sha256,
            "publicationProofHash": binding.publication_proof_hash,
            "publicationProofFileSha256": binding.publication_proof_file_sha256,
        }
        for field, value in expected.items():
            actual = verified.get(field)
            if isinstance(value, str):
                if not hmac.compare_digest(_text(actual).lower(), value.lower()):
                    raise BinanceSpotFunctionalError(
                        f"Binance sealed evaluator publication {field} changed"
                    )
            elif actual is not value:
                raise BinanceSpotFunctionalError(
                    f"Binance sealed evaluator publication {field} changed"
                )
        artifact = self._read_exact_json(
            verified.get("artifactPath"),
            binding.artifact_file_sha256,
            label="Strategy Artifact",
        )
        instance = self._read_exact_json(
            verified.get("instancePath"),
            binding.instance_file_sha256,
            label="Strategy Instance",
        )
        for document, label in ((artifact, "Artifact"), (instance, "Instance")):
            parameters = document.get("parameters")
            if (
                not isinstance(parameters, Mapping)
                or int(parameters.get("shortMa") or 0) != 3
                or int(parameters.get("longMa") or 0) != 10
                or _text(parameters.get("executionRoute")) != EXECUTION_ROUTE
                or _text(parameters.get("exchange")) != "BINANCE_SPOT"
            ):
                raise BinanceSpotFunctionalError(
                    f"Binance sealed evaluator {label} MA contract changed"
                )
        if (
            _text(artifact.get("plugin")) != "moving_average_cross"
            or _text(instance.get("pluginId")) != "moving_average_cross"
        ):
            raise BinanceSpotFunctionalError(
                "Binance sealed evaluator plugin contract changed"
            )
        return binding, verified

    def evaluate(self, window: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(window, Mapping) or any(
            field in window for field in _FORBIDDEN_SIGNAL_FIELDS
        ):
            raise BinanceSpotFunctionalError(
                "Binance sealed evaluator rejects caller signal fields"
            )
        if (
            _text(window.get("schemaVersion"))
            != "binance-spot-official-finalized-5m-window-v1"
            or _text(window.get("symbol")).upper() != SYMBOL
            or _text(window.get("interval")) != INTERVAL
            or _text(window.get("source")).upper() != "BINANCE_SPOT_KLINE"
            or window.get("finalized") is not True
            or window.get("closed") is not True
        ):
            raise BinanceSpotFunctionalError(
                "Binance sealed evaluator official window contract changed"
            )
        rows = window.get("bars")
        if not isinstance(rows, list) or len(rows) != WINDOW_SIZE or any(
            not isinstance(row, Mapping) for row in rows
        ):
            raise BinanceSpotFunctionalError(
                "Binance sealed evaluator requires an exact 11-bar window"
            )
        closes: list[Decimal] = []
        last_open_ms: int | None = None
        for row in rows:
            if any(field in row for field in _FORBIDDEN_SIGNAL_FIELDS):
                raise BinanceSpotFunctionalError(
                    "Binance sealed evaluator rejects caller signal fields"
                )
            bar_id = _text(row.get("barId"))
            if (
                _SAFE_BAR_ID.fullmatch(bar_id) is None
                or row.get("finalized") is not True
                or row.get("closed") is not True
            ):
                raise BinanceSpotFunctionalError(
                    "Binance sealed evaluator bar identity changed"
                )
            opened_ms = int(bar_id.rsplit("-", 1)[1])
            if last_open_ms is not None and (
                opened_ms - last_open_ms != INTERVAL_MILLISECONDS
            ):
                raise BinanceSpotFunctionalError(
                    "Binance sealed evaluator bar window is not contiguous"
                )
            last_open_ms = opened_ms
            closes.append(_decimal(row.get("close"), label="close"))
        final = rows[-1]
        if (
            _text(window.get("barId")) != _text(final.get("barId"))
            or _text(window.get("barCloseAt")) != _text(final.get("barCloseAt"))
        ):
            raise BinanceSpotFunctionalError(
                "Binance sealed evaluator final bar identity changed"
            )
        binding, verified = self._selection()
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
        window_hash = _stable_hash(dict(window))
        evaluation_id = "binance-ma-eval-" + _stable_hash(
            {
                "windowHash": window_hash,
                "strategyArtifactHash": binding.strategy_artifact_hash,
                "strategyInstanceHash": binding.strategy_instance_hash,
            }
        )[:32]
        return {
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "executionRoute": EXECUTION_ROUTE,
            "strategyArtifactId": binding.strategy_artifact_id,
            "strategyArtifactHash": binding.strategy_artifact_hash,
            "strategyArtifactFileSha256": binding.artifact_file_sha256,
            "strategyInstanceId": binding.strategy_instance_id,
            "strategyInstanceHash": binding.strategy_instance_hash,
            "strategyInstanceFileSha256": binding.instance_file_sha256,
            "publicationProofHash": binding.publication_proof_hash,
            "publicationProofFileSha256": binding.publication_proof_file_sha256,
            "accountFingerprint": binding.account_fingerprint,
            "bindingHash": _stable_hash(binding.payload()),
            "finalized": True,
            "strategyEvaluationComplete": True,
            "naturalSignal": True,
            "forced": False,
            "barSource": "BINANCE_SPOT_KLINE",
            "barId": window["barId"],
            "barHash": window_hash,
            "officialWindowHash": window_hash,
            "officialWindow": dict(window),
            "barCloseAt": window["barCloseAt"],
            "observedAt": window["observedAt"],
            "signal": signal,
            "evaluationId": evaluation_id,
            "strategyPluginId": "moving_average_cross",
            "strategyShortMa": 3,
            "strategyLongMa": 10,
            "publicationProofPath": verified.get("proofPath"),
        }


class BinanceSpotOfficialNaturalSignalReader:
    """No-input callable used by the managed lifecycle scheduler."""

    def __init__(
        self,
        *,
        kline_reader: OfficialBinanceSpotFinalizedKlineReader,
        evaluator: SealedBinanceSpotMovingAverageEvaluator,
    ) -> None:
        self.kline_reader = kline_reader
        self.evaluator = evaluator

    def __call__(self) -> dict[str, Any]:
        window = self.kline_reader.read_window()
        return self.evaluator.evaluate(window)


__all__ = [
    "BinanceSpotOfficialNaturalSignalReader",
    "BinanceSpotKlineBoundaryRace",
    "OfficialBinanceSpotFinalizedKlineReader",
    "SealedBinanceSpotMovingAverageEvaluator",
]
