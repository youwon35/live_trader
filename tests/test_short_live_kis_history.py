from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import unittest

from live_trader.short_live_kis_history import (
    DAILY_ENDPOINT, DAILY_TR_ID, HISTORY_PATHS, NativeKisHistoryBlocked,
    OfficialHistoryGet, configure_feed_bridge, get_segment, get_warmup,
)
from trading_runtime import provider_backfill_kis_kr as native
from trading_runtime.continuous_runtime import ClosedBar
from trading_runtime.market_data import parse_timestamp
from trading_runtime.realtime_feeds import FeedSubscription, StartupWarmupBridgeMixin


class NativeKisHistoryTests(unittest.TestCase):
    def setUp(self):
        self.days = ["2026-10-20", "2026-10-21", "2026-10-22"]
        self.objects = {
            "XKRX": json.dumps({"schema_version": "exchange-session-calendar-v1", "calendar_id": "XKRX",
                "timezone": "Asia/Seoul", "coverage_start": "2026-10-19", "coverage_end": "2026-10-25",
                "sessions": self.days, "early_closes": {}}).encode(),
            "OVERRIDES": json.dumps({"schema_version": "exchange-session-calendar-overrides-v1", "calendars": {}}).encode(),
        }
        self.now = self.ts("2026-10-22T12:02:00+09:00")
        self.calls, self.missing, self.extra = [], set(), ""

    @staticmethod
    def ts(value):
        return datetime.fromisoformat(value).timestamp()

    @staticmethod
    def spec(timeframe="5m", provider="kis"):
        return SimpleNamespace(instrument_id="005930.KS", symbol="005930", provider=provider, timeframe=timeframe,
                               strategy_instance_id="native-fixture-instance")

    def fetch(self, endpoint, tr_id, params):
        self.assertEqual(HISTORY_PATHS[endpoint], tr_id)
        self.calls.append((endpoint, dict(params)))
        if endpoint == DAILY_ENDPOINT:
            self.assertEqual(params["FID_ORG_ADJ_PRC"], "1")
            self.assertEqual(params["FID_PERIOD_DIV_CODE"], "D")
            rows = [{"stck_bsop_date": day.replace("-", ""), "stck_oprc": "100", "stck_hgpr": "102",
                     "stck_lwpr": "99", "stck_clpr": "101", "acml_vol": "390"}
                    for day in reversed(self.days)
                    if params["FID_INPUT_DATE_1"] <= day.replace("-", "") <= params["FID_INPUT_DATE_2"]
                    and day not in self.missing]
            return {"rt_cd": "0", "output2": rows[:100], "msg1": self.extra}
        self.assertEqual(endpoint, native.ENDPOINT)
        self.assertEqual(params["FID_FAKE_TICK_INCU_YN"], "")
        cursor = datetime.strptime(params["FID_INPUT_DATE_1"]+params["FID_INPUT_HOUR_1"], "%Y%m%d%H%M%S").replace(tzinfo=native.KST)
        rows = []
        for day in self.days:
            opening = datetime.fromisoformat(day+"T09:00:00+09:00")
            for offset in range(390):
                start = opening + timedelta(minutes=offset)
                if start <= cursor and start.isoformat() not in self.missing:
                    rows.append({"stck_bsop_date": start.strftime("%Y%m%d"), "stck_cntg_hour": start.strftime("%H%M%S"),
                                 "stck_oprc": "100", "stck_hgpr": "102", "stck_lwpr": "99", "stck_prpr": "101", "cntg_vol": "1"})
        return {"rt_cd": "0", "output2": list(reversed(rows))[:120], "msg1": self.extra}

    def run_seed(self, tf="5m", count=2):
        return get_warmup(self.spec(tf), count, {}, fetcher=self.fetch, clock=lambda: self.now, calendar_objects=self.objects)

    def test_native_five_minutes_exact_closed_count_and_hash(self):
        bars = self.run_seed()
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[-1].end_time, "2026-10-22T03:00:00.000000Z")
        self.assertTrue(all(b.provider == b.source_provider == "kis" and b.volume == 5 for b in bars))
        self.assertTrue(all(b.evidence_class == "HISTORICAL_WARMUP" and len(b.historical_source_snapshot_hash) == 64 for b in bars))
        self.assertNotIn("authorization", json.dumps([b.to_dict() for b in bars]))

    def test_hour_is_complete_native_minutes_and_session_grid(self):
        bars = self.run_seed("1h")
        self.assertEqual([b.volume for b in bars], [60, 60])
        self.assertEqual(bars[-1].end_time, "2026-10-22T03:00:00.000000Z")
        self.assertEqual((bars[0].open, bars[0].high, bars[0].low, bars[0].close), (100, 102, 99, 101))
        self.assertTrue(all(b.timeframe == "1h" and b.provider == "kis" for b in bars))

    def test_hour_previous_final_half_hour_is_not_fabricated_full_hour(self):
        self.now = self.ts("2026-10-22T10:02:00+09:00")
        bars = self.run_seed("1h")
        self.assertEqual([b.volume for b in bars], [30, 60])
        self.assertEqual((parse_timestamp(bars[0].end_time)-parse_timestamp(bars[0].start_time)).total_seconds(), 1800)

    def test_daily_only_actual_completed_sessions_raw_price(self):
        bars = self.run_seed("1d")
        self.assertEqual([parse_timestamp(b.end_time).astimezone(native.KST).date().isoformat() for b in bars], self.days[:2])
        self.assertTrue(all(b.volume == 390 and b.timeframe == "1d" and b.price_basis == "unadjusted" for b in bars))
        self.assertTrue(all(endpoint == DAILY_ENDPOINT for endpoint, _ in self.calls))

    def test_daily_includes_today_only_after_actual_close(self):
        self.now = self.ts("2026-10-22T15:30:00+09:00")
        self.assertEqual(self.run_seed("1d", 1)[0].end_time, "2026-10-21T06:30:00.000000Z")
        self.now += .01
        self.assertEqual(self.run_seed("1d", 1)[0].end_time, "2026-10-22T06:30:00.000000Z")

    def test_missing_native_minute_is_blocked_no_padding(self):
        self.missing.add("2026-10-22T11:59:00+09:00")
        with self.assertRaises(ValueError):
            self.run_seed("1h", 1)

    def test_missing_daily_session_is_blocked_no_fill(self):
        self.missing.add("2026-10-21")
        with self.assertRaises(ValueError):
            self.run_seed("1d", 2)

    def test_raw_public_response_drift_changes_snapshot_pin(self):
        first = self.run_seed()[0].historical_source_snapshot_hash
        self.extra = "public response changed"
        self.assertNotEqual(first, self.run_seed()[0].historical_source_snapshot_hash)

    def test_wrong_provider_and_invalid_limit_never_reach_fetch(self):
        for spec, count in [(self.spec(provider="yahoo"), 2), (self.spec(), 0), (self.spec(), True)]:
            with self.assertRaises(ValueError):
                get_warmup(spec, count, {}, fetcher=self.fetch, clock=lambda: self.now, calendar_objects=self.objects)
        self.assertEqual(self.calls, [])

    def test_native_startup_hour_segment_is_history_not_signal(self):
        bars, receipt = get_segment(self.spec("1h"), after_end="2026-10-22T01:00:00Z",
            through_end="2026-10-22T02:00:00Z", settings={}, fetcher=self.fetch, clock=lambda: self.now,
            calendar_objects=self.objects)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].volume, 60)
        self.assertEqual(receipt["naturalForwardCycles"], 0)
        self.assertEqual(receipt["brokerOrders"], 0)
        self.assertTrue(receipt["historicalOnly"])

    def test_native_startup_overnight_has_calendar_gap_not_fake_bars(self):
        bars, receipt = get_segment(self.spec("1h"), after_end="2026-10-21T06:30:00Z",
            through_end="2026-10-22T01:00:00Z", settings={}, fetcher=self.fetch, clock=lambda: self.now,
            calendar_objects=self.objects)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].start_time, "2026-10-22T00:00:00.000000Z")
        self.assertEqual(receipt["afterEnd"], "2026-10-21T06:30:00.000000Z")

    def test_official_transport_rejects_nonwhitelisted_before_auth(self):
        class Reader:
            def _credentials(self, provider):
                raise AssertionError("Authentication must not happen")
        get = OfficialHistoryGet({}, reader=Reader())
        with self.assertRaises(NativeKisHistoryBlocked):
            get("/uapi/domestic-stock/v1/trading/order-cash", "TTTC0802U", {})

    def _bridge_fixture(self):
        class Feed(StartupWarmupBridgeMixin):
            provider_id = "kis"
            def _await_startup_partial_close(this, end):
                self.assertLess(parse_timestamp(end).timestamp(), self.now)
        child, callback_rows = Feed(), []
        spec = self.spec()
        sub = FeedSubscription(spec.instrument_id, spec.symbol, spec.timeframe)
        key = (spec.instrument_id, spec.timeframe)
        child._startup_seed_anchor = {key: "2026-10-21T06:30:00.000000Z"}
        def segment(spec, **kwargs):
            return get_segment(spec, **kwargs, fetcher=self.fetch, clock=lambda: self.now, calendar_objects=self.objects)
        configure_feed_bridge(child, (spec,), {}, lambda *args: callback_rows.append(args),
                              segment_reader=segment, calendar_objects=self.objects)
        partial = ClosedBar(spec.instrument_id, spec.symbol, "kis", "5m", "2026-10-22T00:00:00Z",
                            "2026-10-22T00:05:00Z", 100, 102, 99, 101, 2, source_provider="kis")
        return child, sub, partial, key, callback_rows

    def test_native_feed_overnight_bridge_appends_once_without_forward_order(self):
        child, sub, partial, key, callbacks = self._bridge_fixture()
        child._bridge_startup_partial(sub, partial, partial.end_time)
        self.assertEqual(len(callbacks), 1)
        _, bars, receipt = callbacks[0]
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].volume, 5)
        self.assertEqual(bars[0].start_time, "2026-10-22T00:00:00.000000Z")
        self.assertEqual(receipt["afterEnd"], "2026-10-21T06:30:00.000000Z")
        self.assertEqual((receipt["brokerOrders"], receipt["naturalForwardCycles"]), (0, 0))
        self.assertEqual(child._startup_seed_anchor[key], "2026-10-22T00:05:00.000000Z")
        child._bridge_startup_partial(sub, partial, partial.end_time)
        self.assertEqual(len(callbacks), 1)

    def test_native_feed_overnight_bridge_missing_minute_blocks_without_relabel(self):
        self.missing.add("2026-10-22T09:04:00+09:00")
        child, sub, partial, key, callbacks = self._bridge_fixture()
        with self.assertRaises(ValueError):
            child._bridge_startup_partial(sub, partial, partial.end_time)
        self.assertEqual(callbacks, [])
        self.assertEqual(child._startup_seed_anchor[key], "2026-10-21T06:30:00.000000Z")
        self.assertNotIn(key, child._startup_bridge_done)


if __name__ == "__main__":
    unittest.main()
