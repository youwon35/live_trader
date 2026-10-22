"""Pure SOL public-candle parser fixtures; no HTTP or publication writes."""
from datetime import datetime, timezone
from email.utils import format_datetime
import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"tools"))
from collect_short_live_public_sol import parse_closed, URLS, SECONDS


class PublicSolParserTests(unittest.TestCase):
    def payload(self,tf):
        start=int(datetime(2026,1,1,tzinfo=timezone.utc).timestamp()*1000)
        period=SECONDS[tf]*1000
        def bar(at): return [at,"100","101","99","100","4",at+period-1,"400",4,"2","200","0"]
        observed=datetime.fromtimestamp((start+period+period/2)/1000,timezone.utc)
        return [bar(start),bar(start+period)],format_datetime(observed,usegmt=True)

    def test_each_requested_timeframe_uses_original_closed_rows_only(self):
        for tf in SECONDS:
            with self.subTest(tf=tf):
                payload,date=self.payload(tf); rows,excluded=parse_closed(json.dumps(payload).encode(),date,tf)
                self.assertEqual(1,len(rows)); self.assertEqual(1,excluded)
                self.assertEqual(100,rows[0]["close"]); self.assertEqual(4,rows[0]["volume"])
                self.assertEqual("UTC",str(rows[0]["datetime"].tzinfo))

    def test_gap_duplicate_wrong_boundaries_and_nonfinite_values_are_rejected(self):
        for tf in SECONDS:
            payload,date=self.payload(tf)
            cases=[payload[:1]+payload[:1], payload[::-1]]
            broken=json.loads(json.dumps(payload)); broken[0][6]+=1; cases.append(broken)
            broken=json.loads(json.dumps(payload)); broken[0][4]="NaN"; cases.append(broken)
            broken=json.loads(json.dumps(payload)); broken[1][0]+=SECONDS[tf]*1000; broken[1][6]+=SECONDS[tf]*1000; cases.append(broken)
            for case in cases:
                with self.assertRaises(ValueError): parse_closed(json.dumps(case).encode(),date,tf)

    def test_fixed_origin_and_symbol_cannot_be_changed_by_a_manifest(self):
        self.assertEqual({"5m","1h","1d"},set(URLS))
        for tf,url in URLS.items():
            self.assertEqual(f"https://fapi.binance.com/fapi/v1/klines?symbol=SOLUSDT&interval={tf}&limit=500",url)


if __name__=="__main__": unittest.main()
