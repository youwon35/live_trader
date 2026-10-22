"""Public candle fixtures only: no product package startup or network."""
from copy import deepcopy
import ast
from datetime import datetime, timezone
import importlib
from pathlib import Path
from types import SimpleNamespace
import sys
import types
import unittest
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1] / "live_trader"
PKG = "_short_live_upbit_history_tests"
package = types.ModuleType(PKG); package.__path__ = [str(ROOT)]; sys.modules[PKG] = package
native = importlib.import_module(PKG + ".short_live_upbit_history")
from trading_runtime.continuous_runtime import ClosedBar
from trading_runtime.realtime_feeds import FeedSubscription, StartupWarmupBridgeMixin
from trading_runtime.market_data import parse_timestamp


class UpbitHistoryTests(unittest.TestCase):
    def setUp(self):
        self.through = datetime(2026, 10, 23, tzinfo=timezone.utc).timestamp()
        self.now = self.through + 2
        self.calls = []
        self.missing = set()
        self.change = lambda row: row

    def spec(self, tf="1h"):
        return SimpleNamespace(instrument_id="upbit:KRW-BTC", symbol="KRW-BTC", provider="upbit",
                               timeframe=tf, strategy_instance_id="exact-native-fixture")

    def fetch(self, endpoint, params):
        self.calls.append((endpoint, deepcopy(params)))
        timeframe = next(tf for tf, (path, _) in native.ENDPOINTS.items() if path == endpoint)
        seconds = native.ENDPOINTS[timeframe][1]
        cursor = parse_timestamp(params["to"]).timestamp()
        rows = []
        for index in range(260):
            start = self.through - (index+1)*seconds
            if start >= cursor or start in self.missing:
                continue
            row = {"market": params["market"], "candle_date_time_utc": datetime.fromtimestamp(start, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                   "opening_price": 100, "high_price": 102, "low_price": 99, "trade_price": 101,
                   "candle_acc_trade_volume": .25, "timestamp": int((start+seconds-1)*1000),
                   "unit": seconds//60, "extraPublicField": "preserved"}
            rows.append(self.change(row))
        return rows[:params["count"]]

    def segment(self, tf="1h", count=1):
        seconds = native.ENDPOINTS[tf][1]
        return native.get_segment(self.spec(tf), after_end=native.iso(self.through-count*seconds),
            through_end=native.iso(self.through), fetcher=self.fetch, clock=lambda: self.now, limit=count)

    def test_exact_five_minute_hour_and_daily_endpoints_and_replay(self):
        for tf in ("5m", "1h", "1d"):
            with self.subTest(timeframe=tf):
                bars, receipt = self.segment(tf)
                self.assertEqual(native.ENDPOINTS[tf][0], self.calls[-1][0])
                self.assertEqual(tf, bars[0].timeframe)
                self.assertEqual("HISTORICAL_WARMUP", bars[0].evidence_class)
                self.assertEqual(0, receipt["naturalForwardCycles"])
                self.assertEqual(0, receipt["brokerOrders"])
                self.assertEqual("preserved", receipt["pages"][0]["publicResponse"][0]["extraPublicField"])
                self.assertEqual([bar.to_dict() for bar in bars], [bar.to_dict() for bar in native.verify_segment(self.spec(tf), receipt)])

    def test_pagination_keeps_exclusive_cursor_and_original_public_pages(self):
        bars, receipt = self.segment("1h", count=220)
        self.assertEqual(220, len(bars))
        self.assertEqual(2, len(receipt["pages"]))
        self.assertEqual(receipt["pages"][0]["publicResponse"][-1]["candle_date_time_utc"]+".000000Z",
                         receipt["pages"][1]["params"]["to"])

    def test_missing_candle_is_blocked_without_invented_empty_bar(self):
        self.missing.add(self.through-3600)
        with self.assertRaises(native.NativeUpbitHistoryBlocked):
            self.segment("1h", count=2)

    def test_wrong_market_unit_offgrid_or_future_tick_are_blocked(self):
        changes = (lambda r: {**r, "market": "KRW-ETH"}, lambda r: {**r, "unit": 5},
                   lambda r: {**r, "timestamp": int(self.now*1000)},
                   lambda r: {**r, "candle_date_time_utc": "2026-10-22T23:00:01"})
        for change in changes:
            with self.subTest(change=change):
                self.change = change
                with self.assertRaises(native.NativeUpbitHistoryBlocked):
                    self.segment()

    def test_tampered_response_or_scope_cannot_replay(self):
        _, receipt = self.segment()
        for mutate in (lambda r: r["pages"][0]["publicResponse"][0].update(trade_price=90),
                       lambda r: r.update(timeframe="5m")):
            changed = deepcopy(receipt); mutate(changed)
            with self.assertRaises(native.NativeUpbitHistoryBlocked):
                native.verify_segment(self.spec(), changed)

    def child(self):
        owner = self
        class Child(StartupWarmupBridgeMixin):
            provider_id = "upbit"
            subscriptions = [FeedSubscription(owner.spec().instrument_id, "KRW-BTC", "1h")]
            def __init__(self):
                def fetch_json(url, headers):
                    parsed = urlsplit(url); params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
                    params["count"] = int(params["count"])
                    return owner.fetch(parsed.scheme+"://"+parsed.netloc+parsed.path, params)
                self.rest = SimpleNamespace(fetch_json=fetch_json, now=lambda: owner.now, sleep=lambda _: None,
                                            _pagination_pause=lambda: None)
            def _await_startup_partial_close(self, end):
                self.asserted_end = end
        return Child()

    def test_hour_startup_partial_appends_exact_native_history_only_once(self):
        child, observed = self.child(), []
        spec = self.spec(); seconds = 3600
        seed = ClosedBar(spec.instrument_id, spec.symbol, "upbit", "1h", native.iso(self.through-2*seconds),
                         native.iso(self.through-seconds), 100, 102, 99, 101, 1)
        native.record_seed_anchor(SimpleNamespace(feeds=[child]), spec, [seed])
        native.configure_feed_bridge(child, [spec], lambda specs, bars, receipt: observed.append((specs, bars, receipt)))
        partial = ClosedBar(spec.instrument_id, spec.symbol, "upbit", "1h", native.iso(self.through-seconds),
                            native.iso(self.through), 10, 20, 1, 2, 3)
        sub = child.subscriptions[0]
        child._bridge_startup_partial(sub, partial, native.iso(self.through))
        child._bridge_startup_partial(sub, partial, native.iso(self.through))
        self.assertEqual(1, len(observed))
        self.assertEqual(101, observed[0][1][0].close)  # native REST, not partial placeholder2
        self.assertTrue(observed[0][2]["historicalOnly"])
        self.assertEqual(native.iso(self.through), child._startup_seed_anchor[(spec.instrument_id, "1h")])

    def test_failed_history_callback_restores_prior_anchor_without_done(self):
        child = self.child(); spec = self.spec(); key = (spec.instrument_id, "1h")
        def failed(*_): raise ValueError("callback failed")
        native.configure_feed_bridge(child, [spec], failed)
        anchor = native.iso(self.through-3600); child._startup_seed_anchor = {key: anchor}
        partial = ClosedBar(spec.instrument_id, spec.symbol, "upbit", "1h", anchor, native.iso(self.through), 100, 102, 99, 101, 1)
        with self.assertRaisesRegex(ValueError, "callback failed"):
            child._bridge_startup_partial(child.subscriptions[0], partial, native.iso(self.through))
        self.assertEqual(anchor, child._startup_seed_anchor[key])
        self.assertNotIn(key, child._startup_bridge_done)

    def test_actual_native_feed_wiring_records_seed_and_selects_local_bridge(self):
        # Compile only the two production methods. Product module imports and
        # process settings/startup are deliberately outside this fixture.
        tree = ast.parse((ROOT / "short_live_runtime.py").read_text(encoding="utf-8-sig"))
        production = next(row for row in tree.body if isinstance(row, ast.ClassDef) and row.name == "NativeFeed")
        methods = [row for row in production.body if isinstance(row, ast.FunctionDef)
                   and row.name in {"warmup", "configure_warmup_bridge"}]
        harness = ast.ClassDef(name="Harness", bases=[], keywords=[], body=methods, decorator_list=[])
        scope = {"__package__": PKG, "FeedSubscription": FeedSubscription}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[harness], type_ignores=[])), "native-methods", "exec"), scope)
        child, spec = self.child(), self.spec()
        seed = ClosedBar(spec.instrument_id, spec.symbol, "upbit", "1h", native.iso(self.through-7200),
                         native.iso(self.through-3600), 100, 102, 99, 101, 1)
        child.warmup = lambda subscription, limit: [seed]
        feed = scope["Harness"](); feed.route = "UPBIT_KRW_SPOT"; feed.children = [child]
        self.assertEqual([seed], feed.warmup(spec, 1))
        feed.configure_warmup_bridge([spec], lambda *_: None)
        self.assertEqual(seed.end_time, child._startup_seed_anchor[(spec.instrument_id, "1h")])
        self.assertEqual(native.__name__, child.authoritative_warmup.__func__.__module__)


if __name__ == "__main__":
    unittest.main()
