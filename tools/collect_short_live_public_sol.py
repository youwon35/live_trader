"""Three fixed unsigned official SOLUSDT futures candle GETs, create-only.

Stores HTTP raw bytes, provenance and a lossless-in-time normalized Parquet
copy for the existing offline functional parity tool. No accounts or orders.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
import hashlib
import json
from pathlib import Path
from urllib.request import HTTPSHandler, Request, build_opener

from collect_short_live_public_futures import NoRedirect

WORKSPACE=Path(__file__).resolve().parents[3]
SECONDS={"5m":300,"1h":3600,"1d":86400}
URLS={tf:f"https://fapi.binance.com/fapi/v1/klines?symbol=SOLUSDT&interval={tf}&limit=500" for tf in SECONDS}


def sha(raw): return hashlib.sha256(raw).hexdigest()


def parse_closed(raw,response_date,timeframe):
    period=SECONDS[timeframe]*1000
    observed=parsedate_to_datetime(response_date)
    if observed.tzinfo is None: raise ValueError("official-response-date-timezone-required")
    payload=json.loads(raw)
    if not isinstance(payload,list) or not payload or len(payload)>500: raise ValueError("official-kline-list-invalid")
    rows=[]; previous=None; excluded=0
    for row in payload:
        if not isinstance(row,list) or len(row)!=12: raise ValueError("official-kline-schema-invalid")
        if type(row[0]) is not int or type(row[6]) is not int or row[0]%period or row[6]!=row[0]+period-1:
            raise ValueError("official-kline-time-boundary-invalid")
        if previous is not None and row[0]!=previous+period: raise ValueError("official-original-kline-gap-or-duplicate")
        previous=row[0]
        try: values=[Decimal(str(value)) for value in row[1:6]]
        except InvalidOperation: raise ValueError("official-kline-ohlcv-invalid") from None
        if not all(value.is_finite() for value in values) or min(values[:4])<=0 or values[4]<0 or values[2]>min(values[0],values[3]) or values[1]<max(values[0],values[3]):
            raise ValueError("official-kline-ohlcv-invalid")
        if row[0]+period>int(observed.timestamp()*1000): excluded+=1; continue
        at=datetime.fromtimestamp(row[0]/1000,timezone.utc)
        rows.append({"datetime":at,"timestamp":row[0]//1000,"open":float(values[0]),"high":float(values[1]),
                     "low":float(values[2]),"close":float(values[3]),"volume":float(values[4]),"quality_status":"ok"})
    if not rows: raise ValueError("no-actual-original-closed-bars")
    return rows,excluded


def write_new(path,raw):
    with path.open("xb") as handle: handle.write(raw); handle.flush()
    return {"path":str(path.resolve()),"sha256":sha(raw)}


def encode(value): return json.dumps(value,sort_keys=True,ensure_ascii=True,indent=2,allow_nan=False).encode()


def collect(output):
    import pyarrow as pa
    import pyarrow.parquet as pq
    output=output.resolve()
    if output.exists() or not output.is_relative_to(WORKSPACE): raise ValueError("new-workspace-publication-root-required")
    output.mkdir(parents=True)
    items=[]; refs=[]
    opener=build_opener(HTTPSHandler(),NoRedirect())
    for tf,url in URLS.items():
        request=Request(url,headers={"Accept":"application/json","User-Agent":"short-functional-public-SOL-data/1"},method="GET")
        with opener.open(request,timeout=20) as response:
            if response.status!=200 or response.geturl()!=url: raise ValueError("official-public-response-invalid")
            response_date=response.headers.get("Date",""); raw=response.read(2_000_001)
            if len(raw)>2_000_000: raise ValueError("official-public-response-size-limit")
        rows,excluded=parse_closed(raw,response_date,tf)
        raw_ref=write_new(output/(tf+"-http-raw.json"),raw); refs.append(raw_ref)
        provenance={"schemaVersion":"short-live-official-public-sol-candles-v1","provider":"BINANCE_OFFICIAL_PUBLIC_KLINES",
            "symbol":"SOLUSDT","marketType":"futures","timeframe":tf,"url":url,"httpStatus":200,"responseDate":response_date,
            "rawSha256":sha(raw),"collectedAt":datetime.now(timezone.utc).isoformat(),"closedRows":len(rows),"excludedUnclosedRows":excluded,
            "firstOpenUtc":rows[0]["datetime"].isoformat(),"lastOpenUtc":rows[-1]["datetime"].isoformat(),
            "unsigned":True,"accountRequests":0,"orderRequests":0,"syntheticBars":0,"missingBarsFilled":0}
        prov_ref=write_new(output/(tf+"-provenance.json"),encode(provenance)); refs.append(prov_ref)
        metadata={"provider":"binance","source":"BINANCE_OFFICIAL_PUBLIC_KLINES","symbol":"SOLUSDT","interval":tf,
            "asset_class":"CRYPTO","market_type":"futures","timezone":"UTC","quality_status":"ok",
            "raw_content_sha256":sha(raw),"lineage_run_id":"official-public-sol-"+sha(raw)[:20],
            "dataset_id":"official-public-sol-futures-"+tf+"-"+sha(raw)[:20],
            "transformation_id":"official-original-closed-candles-to-parquet-v1",
            "public_provenance_sha256":prov_ref["sha256"],"public_url":url,"public_response_date":response_date}
        table=pa.Table.from_pylist(rows).replace_schema_metadata({("stock_data_scraper."+key).encode():value.encode() for key,value in metadata.items()})
        path=output/("SOLUSDT-"+tf+".parquet")
        with path.open("xb") as handle: pq.write_table(table,handle,compression="zstd")
        data_ref={"path":str(path),"sha256":sha(path.read_bytes())}; refs.append(data_ref)
        items.append({"route":"BINANCE_USDM_FUTURES","symbol":"SOLUSDT","timeframe":tf,**data_ref,
            "publicHttpRawPath":raw_ref["path"],"publicHttpRawSha256":raw_ref["sha256"],
            "publicProvenancePath":prov_ref["path"],"publicProvenanceSha256":prov_ref["sha256"]})
        print(json.dumps({"timeframe":tf,"closedBars":len(rows),"excludedUnclosedBars":excluded,"rawSha256":sha(raw)}),flush=True)
    manifest={"schemaVersion":"short-live-real-data-preparation-v1","purpose":"FUNCTIONAL_TEST_ONLY","datasets":items}
    manifest_ref=write_new(output/"sol-real-data-manifest.json",encode(manifest))
    receipt_ref=write_new(output/"collection-receipt.json",encode({"schemaVersion":"short-live-public-sol-collection-v1",
        "sourceToolSha256":sha(Path(__file__).read_bytes()),"outputs":refs,"manifest":manifest_ref,
        "actualAccountRequests":0,"actualOrderRequests":0,"canPlaceOrders":False}))
    return {"manifest":manifest_ref,"receipt":receipt_ref}


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--output",type=Path,required=True); parser.add_argument("--execute-public-get",action="store_true")
    args=parser.parse_args()
    if not args.execute_public_get: print(json.dumps({"status":"PLAN_ONLY","urls":URLS,"accountRequests":0,"orderRequests":0})); return
    print(json.dumps(collect(args.output)))


if __name__=="__main__": main()
