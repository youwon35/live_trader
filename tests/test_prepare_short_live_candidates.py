"""Pure fixture checks of offline preparation; never publish fixture as real data."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from copy import deepcopy
import json
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from prepare_short_live_candidates import aggregate_utc_days, instant, parity_report, validate_metadata, select_observation_rows
from collect_short_live_public_futures import parse_closed_klines, NoRedirect, URL
from verify_short_live_publication import validate_report
from trading_runtime.continuous_runtime import RuntimeStrategySpec


def fixture_rows(count):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [{"date": (start + timedelta(hours=i)).isoformat(), "timestamp": int((start + timedelta(hours=i)).timestamp()),
             "open": 100, "high": 101, "low": 99, "close": 100, "volume": i+1} for i in range(count)]


class PrepareShortLiveCandidatesTests(unittest.TestCase):
    def spec(self):
        return RuntimeStrategySpec(portfolio_id="fixture-only", portfolio_hash="a"*64,
            strategy_instance_id="fixture-instance", strategy_id="fixture-source", artifact_hash="b"*64,
            plugin_id="moving_average_cross", instrument_id="BTCUSDT", symbol="BTCUSDT",
            timeframe="1h", provider="binance", broker_id="binance", parameters={"shortMa": 1, "longMa": 3})

    def test_only_exact_24_hour_original_group_is_aggregated(self):
        rows = fixture_rows(24)
        days = aggregate_utc_days(rows)
        self.assertEqual(1, len(days)); self.assertEqual(sum(range(1,25)), days[0]["volume"])
        self.assertEqual(100, days[0]["open"]); self.assertEqual(101, days[0]["high"])
        self.assertEqual(99, days[0]["low"]); self.assertEqual(100, days[0]["close"])

    def test_missing_duplicate_or_partial_hours_are_not_filled(self):
        rows = fixture_rows(24)
        for incomplete in (rows[:-1], rows[1:], rows[:10] + rows[11:], rows[:-1] + [rows[0]]):
            self.assertEqual([], aggregate_utc_days(incomplete))

    def test_timestamp_must_have_timezone(self):
        with self.assertRaisesRegex(ValueError, "timezone"):
            instant("2026-01-01T00:00:00")

    def test_futures_cannot_borrow_spot_metadata_or_wrong_timeframe(self):
        item = {"route": "BINANCE_USDM_FUTURES", "symbol": "BTCUSDT", "timeframe": "1h"}
        meta = {"provider": "BINANCE", "symbol": "BTCUSDT", "interval": "1h", "asset_class": "CRYPTO",
                "market_type": "futures", "quality_status": "ok", "raw_content_sha256": "a"*64,
                "lineage_run_id": "fixture-only", "dataset_id": "fixture-only"}
        validate_metadata(meta, item)
        for change in ({"market_type": "spot"}, {"interval": "5m"}, {"symbol": "ETHUSDT"}, {"provider": "upbit"}):
            with self.assertRaises(ValueError): validate_metadata({**meta, **change}, item)

    def test_warmup_plus_one_without_trade_is_limited_functional_observation(self):
        rows = fixture_rows(6)
        result = {"equity": [{"value": 100, "signal": None, "positionQuantity": 0} for _ in rows], "trades": []}
        report = parity_report(rows, result, self.spec())
        self.assertEqual("PASS", report["status"]); self.assertEqual(1, report["compared"])
        self.assertFalse(report["profitabilityRequired"]); self.assertFalse(report["minimumTradesRequired"])
        self.assertEqual(0, report["closedTradesObserved"])
        self.assertIn("NOT_A_FAILURE", report["signalObservation"])

    def test_insufficient_history_and_any_parity_difference_remain_hold(self):
        rows = fixture_rows(6)
        result = {"equity": [{"value": 100, "signal": None, "positionQuantity": 0} for _ in rows], "trades": []}
        result["equity"][-1]["signal"] = "buy"
        self.assertEqual("HOLD", parity_report(rows, result, self.spec())["status"])
        self.assertEqual("HOLD", parity_report(rows[:5], {"equity": result["equity"][:5]}, self.spec())["status"])

    def test_predeclared_observation_cost_cap_and_unclosed_exclusion(self):
        rows = fixture_rows(2100)
        selected = select_observation_rows(rows, "1h", observed_at=datetime(2027, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(2048, len(selected)); self.assertEqual(rows[52], selected[0]); self.assertEqual(rows[-1], selected[-1])
        selected = select_observation_rows(rows[:3], "1h", observed_at=datetime(2026,1,1,2,30,tzinfo=timezone.utc))
        self.assertEqual(rows[:2], selected)

    def test_public_futures_schema_and_response_date_excludes_current_bar(self):
        start = int(datetime(2026,1,1,tzinfo=timezone.utc).timestamp()*1000)
        def bar(at): return [at,"100","101","99","100","5",at+299999,"500",10,"2","200","0"]
        raw = json.dumps([bar(start), bar(start+300000)]).encode()
        rows = parse_closed_klines(raw, "Thu, 01 Jan 2026 00:06:00 GMT")
        self.assertEqual(1,len(rows)); self.assertEqual("2026-01-01T00:00:00Z",rows[0]["date"])
        for payload in ([bar(start),bar(start)], [bar(start+1)], {"code":-1}):
            with self.assertRaises(ValueError): parse_closed_klines(json.dumps(payload).encode(), "Thu, 01 Jan 2026 00:06:00 GMT")
        self.assertTrue(URL.startswith("https://fapi.binance.com/fapi/v1/klines?"))
        with self.assertRaisesRegex(ValueError,"redirect-forbidden"):
            NoRedirect().redirect_request(None,None,302,"",{},"https://example.com")

    def report_pair(self):
        rows=fixture_rows(6)
        result={"equity":[{"date":row["date"],"value":100,"signal":None,"positionQuantity":0} for row in rows],"trades":[]}
        parameters={"shortMa":1,"longMa":3,"executionTiming":"signal-close"}
        settings={**parameters,"interval":"1h"}
        dataset={"route":"BINANCE_SPOT","symbol":"BTCUSDT","timeframe":"1h","path":"fixture-only"}
        source={"parameters":parameters,"settings":settings,"timeframe":"1h","dataArtifact":dataset}
        report={"schemaVersion":"short-functional-actual-backtest-paper-parity-v1","promotionEligible":False,
            "settings":settings,"dataset":dataset,"parity":parity_report(rows,result,self.spec()),"backtester":result}
        return deepcopy(source),deepcopy(report)

    def test_publication_report_retains_original_source_timing(self):
        source,report=self.report_pair(); validate_report(source,report)
        report["settings"]["executionTiming"]="next-open"
        with self.assertRaisesRegex(ValueError,"timing-conflict"): validate_report(source,report)

    def test_publication_report_cannot_relabel_parameters_or_timeframe(self):
        for field,value in (("shortMa",2),("interval","5m")):
            source,report=self.report_pair(); report["settings"][field]=value
            with self.assertRaises(ValueError): validate_report(source,report)

    def test_publication_report_requires_actual_comparison_and_finite_output(self):
        for mutate in (lambda report:report["parity"]["comparisons"].clear(),
                       lambda report:report["parity"]["comparisons"][0].update(match=False),
                       lambda report:report["backtester"]["equity"][-1].update(value=float("nan")),
                       lambda report:report["parity"]["comparisons"][0].update(bar="old-unrelated-bar")):
            source,report=self.report_pair(); mutate(report)
            with self.assertRaises(ValueError): validate_report(source,report)


if __name__ == "__main__":
    unittest.main()
