from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import unittest

from live_trader.upbit_continuous_functional import (
    UpbitFunctionalBlocked,
    UpbitPermitScope,
)
from live_trader.upbit_functional_strategy import (
    SealedUpbitMovingAverageEvaluator,
)
from tests.test_upbit_continuous_functional import (
    ACTIVATED_AT,
    FakeBoundaries,
    NOW,
    permit,
)


class SealedUpbitMovingAverageEvaluatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.permit = permit()
        self.fake = FakeBoundaries(self.permit)
        self.fake.selection_updates.update(
            {
                "strategyPluginId": "moving_average_cross",
                "strategyShortMa": 3,
                "strategyLongMa": 10,
            }
        )
        scope = UpbitPermitScope.parse(
            self.permit,
            immutable_selection=self.fake.immutable_selection(),
        )
        self.evaluator = SealedUpbitMovingAverageEvaluator(
            scope=scope,
            immutable_selection_reader=self.fake.immutable_selection,
            clock=self.fake.clock,
        )

    @staticmethod
    def window(
        closes: list[int], *, final_closed_at: datetime = ACTIVATED_AT,
        observed_at: datetime = ACTIVATED_AT,
    ) -> dict[str, object]:
        rows = []
        raw_response = []
        start = final_closed_at - timedelta(minutes=5 * (len(closes) - 1))
        for index, close in enumerate(closes):
            closed_at = start + timedelta(minutes=5 * index)
            opened_at = closed_at - timedelta(minutes=5)
            rows.append(
                {
                    "barId": "upbit-rest-five-minute-"
                    + opened_at.strftime("%Y%m%dT%H%M%SZ"),
                    "closedAt": closed_at.isoformat(
                        timespec="microseconds"
                    ).replace("+00:00", "Z"),
                    "close": str(close),
                    "finalized": True,
                    "closed": True,
                }
            )
            raw_response.append(
                {
                    "market": "KRW-BTC",
                    "candle_date_time_utc": opened_at.replace(
                        tzinfo=None
                    ).isoformat(timespec="seconds"),
                    "trade_price": str(close),
                    "timestamp": int(closed_at.timestamp() * 1000),
                }
            )
        raw_response.reverse()
        raw_response_hash = hashlib.sha256(
            json.dumps(
                raw_response,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return {
            "schemaVersion": "upbit-official-finalized-5m-window-v1",
            "symbol": "KRW-BTC",
            "interval": "5m",
            "source": "UPBIT_WEBSOCKET",
            "finalized": True,
            "closed": True,
            "barId": rows[-1]["barId"],
            "closedAt": rows[-1]["closedAt"],
            "bars": rows,
            "officialCandleEvidence": {
                "schemaVersion": "upbit-official-candle-rest-evidence/v1",
                "origin": "https://api.upbit.com",
                "endpoint": "/v1/candles/minutes/5",
                "orderedQuery": [["market", "KRW-BTC"], ["count", "20"]],
                "observedAt": observed_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "maxResponseTimestampMs": max(
                    int(row["timestamp"]) for row in raw_response
                ),
                "rawResponse": raw_response,
                "rawResponseHash": raw_response_hash,
            },
        }

    def test_published_ma_3_10_derives_hold_and_buy_without_signal_input(self) -> None:
        hold = self.evaluator.evaluate(self.window([10] * 11))
        buy = self.evaluator.evaluate(self.window([10] * 10 + [20]))
        self.assertEqual("HOLD", hold["signal"])
        self.assertEqual("BUY", buy["signal"])
        self.assertTrue(buy["strategyEvaluationComplete"])
        self.assertTrue(buy["naturalSignal"])
        self.assertFalse(buy["forcedSignal"])
        self.assertEqual("moving_average_cross", buy["strategyPluginId"])
        self.assertEqual(64, len(str(buy["barHash"])))
        self.assertEqual(
            self.window([10] * 10 + [20]),
            buy["rawFinalizedWindow"],
        )

    def test_caller_signal_or_replaced_instance_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "caller-signal-forbidden"
        ):
            self.evaluator.evaluate(
                {**self.window([10] * 11), "signal": "BUY"}
            )
        self.fake.selection_updates["strategyInstanceFileSha256"] = "e" * 64
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "InstanceFileSha256-mismatch"
        ):
            self.evaluator.evaluate(self.window([10] * 11))


if __name__ == "__main__":
    unittest.main()
