"""One unsigned official Binance public candle GET; create-only local evidence.

No key, account endpoint, broker object, signed API, redirect, or order path.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import math
from pathlib import Path
from urllib.request import HTTPSHandler, HTTPRedirectHandler, build_opener, Request

URL = "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=500"
WORKSPACE = Path(__file__).resolve().parents[3]


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("public-data-redirect-forbidden")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def parse_closed_klines(raw: bytes, response_date: str):
    observed = parsedate_to_datetime(response_date)
    if observed.tzinfo is None:
        raise ValueError("public-response-date-timezone-required")
    payload = json.loads(raw)
    if not isinstance(payload, list) or not payload or len(payload) > 500:
        raise ValueError("public-kline-list-invalid")
    rows = []; seen = set(); previous = -1
    for item in payload:
        if not isinstance(item, list) or len(item) != 12:
            raise ValueError("public-kline-schema-invalid")
        if isinstance(item[0], bool) or isinstance(item[6], bool):
            raise ValueError("public-kline-time-invalid")
        start, end = int(item[0]), int(item[6])
        if start != item[0] or end != item[6] or start % 300000 or end != start+299999 or start in seen or start <= previous:
            raise ValueError("public-kline-time-invalid")
        seen.add(start); previous = start
        values = {key: float(value) for key, value in zip(("open", "high", "low", "close", "volume"), item[1:6])}
        if not all(math.isfinite(v) for v in values.values()) or min(values[k] for k in ("open", "high", "low", "close")) <= 0:
            raise ValueError("public-kline-price-invalid")
        if values["volume"] < 0 or values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
            raise ValueError("public-kline-range-invalid")
        # The HTTP Date is the server observation, never a guessed local close.
        if start + 300000 > int(observed.timestamp()*1000):
            continue
        at = datetime.fromtimestamp(start/1000, timezone.utc)
        rows.append({"date": at.isoformat().replace("+00:00", "Z"), "timestamp": start//1000, **values})
    return rows


def new_file(path, raw):
    with path.open("xb") as handle:
        handle.write(raw); handle.flush()
    return {"path": str(path.resolve()), "sha256": sha(raw)}


def collect(output: Path):
    output = output.resolve()
    if output.exists() or not output.is_relative_to(WORKSPACE):
        raise ValueError("new-workspace-public-data-root-required")
    # A fixed public URL and no redirects prevent accidental private/signed paths.
    request = Request(URL, headers={"Accept": "application/json", "User-Agent": "short-functional-readonly-data/1"}, method="GET")
    opener = build_opener(HTTPSHandler(), NoRedirect())
    with opener.open(request, timeout=20) as response:
        if response.status != 200 or response.geturl() != URL:
            raise ValueError("public-data-response-invalid")
        response_date = response.headers.get("Date", "")
        raw = response.read(2_000_001)
        if len(raw) > 2_000_000:
            raise ValueError("public-response-size-limit")
    rows = parse_closed_klines(raw, response_date)
    if not rows:
        raise ValueError("no-original-closed-public-bars")
    output.mkdir(parents=True)
    raw_ref = new_file(output / "binance-usdm-btcusdt-5m-http-raw.json", raw)
    provenance = {"schemaVersion": "short-live-public-candle-provenance-v1", "url": URL, "httpStatus": 200,
        "responseDate": response_date, "collectedAt": datetime.now(timezone.utc).isoformat(), "rawSha256": sha(raw),
        "symbol": "BTCUSDT", "timeframe": "5m", "closedRows": len(rows), "firstBar": rows[0]["date"],
        "lastBar": rows[-1]["date"], "unsigned": True, "accountRequests": 0, "orderRequests": 0,
        "datasetMetadata": {"provider": "binance", "symbol": "BTCUSDT", "interval": "5m", "asset_class": "CRYPTO",
            "market_type": "futures", "quality_status": "ok", "raw_content_sha256": sha(raw),
            "lineage_run_id": "public-unsigned-"+sha(raw)[:16], "dataset_id": "public-binance-usdm-btcusdt-5m-"+sha(raw)[:16]}}
    provenance_ref = new_file(output / "provenance.json", json.dumps(provenance, ensure_ascii=True, sort_keys=True, indent=2).encode())
    item = {"route": "BINANCE_USDM_FUTURES", "symbol": "BTCUSDT", "timeframe": "5m",
        "format": "binance-public-klines-json-v1", **raw_ref,
        "provenancePath": provenance_ref["path"], "provenanceSha256": provenance_ref["sha256"]}
    new_file(output / "dataset-item.json", json.dumps(item, sort_keys=True, indent=2).encode())
    return item


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute-public-get", action="store_true"); args = parser.parse_args()
    if not args.execute_public_get:
        print(json.dumps({"status": "PLAN_ONLY", "url": URL, "accountRequests": 0, "orderRequests": 0})); return
    print(json.dumps(collect(args.output), ensure_ascii=True))


if __name__ == "__main__":
    main()
