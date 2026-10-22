"""Offline, create-only FUNCTIONAL_TEST_ONLY candidates from pinned real Parquet.

Never calls a broker, changes existing artifacts, or grants Live permission.
This separate profile explicitly declares 100% of each allocated small test
sleeve, retaining the shared broker capital cap. No original artifact is changed.
Only deterministic Backtester/Paper signal parity is scored; no profitability,
minimum trade count, STANDARD qualification, or fabricated candles are used.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import json
import math
from pathlib import Path
import sys
from typing import Any

APP_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = APP_ROOT.parents[1]
sys.path.insert(0, str(WORKSPACE / "packages" / "trading_runtime"))
from trading_runtime.artifact_governance import (
    seal_strategy_artifact, seal_strategy_instance, seal_portfolio_artifact,
    compute_strategy_artifact_hash, compute_strategy_instance_hash,
)
from trading_runtime.continuous_runtime import ClosedBar, RuntimeStrategySpec
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator, required_warmup_bars

PROFILE = "SHORT_FUNCTIONAL_HISTORY_PARITY_ONLY_V1"
SIZING_PROFILE = "BOUNDED_TEST_SLEEVE_100_PERCENT_V1"
POSITION_SIZE_PERCENT = 100
MAX_OBSERVATION_ROWS = 2048
MODELS = (("moving_average_cross", "ma-cross", {"shortMa": 3, "longMa": 10}),
          ("rsi_revert", "rsi-revert", {"rsiPeriod": 5, "rsiOversold": 30, "rsiExit": 55}))
ROUTES = {
    "KIS_KR_CASH": ("kis", "kis", "KR_STOCK", "KRW", ""),
    "BINANCE_SPOT": ("binance", "binance", "CRYPTO", "USDT", "spot"),
    "BINANCE_USDM_FUTURES": ("binance-futures", "binance-futures", "CRYPTO", "USDT", "futures"),
    "UPBIT_KRW_SPOT": ("upbit", "upbit", "CRYPTO", "KRW", "spot"),
}


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def hash_file(path: Path) -> str:
    return digest(path.read_bytes())


def instant(value) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        result = datetime.fromtimestamp(value / (1000 if abs(value) > 10_000_000_000 else 1), timezone.utc)
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("dataset-timezone-required")
    return result.astimezone(timezone.utc)


def utc(value):
    return instant(value).isoformat().replace("+00:00", "Z")


def aggregate_utc_days(rows):
    """Only exactly complete original 24x1h UTC days; never fill missing hours."""
    groups = defaultdict(list)
    for row in rows:
        groups[instant(row["date"]).date()].append(row)
    output = []
    for day, group in sorted(groups.items()):
        group.sort(key=lambda row: instant(row["date"]))
        expected = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        if len(group) != 24 or any(instant(row["date"]) != expected + timedelta(hours=i) for i, row in enumerate(group)):
            continue
        output.append({"date": utc(expected), "timestamp": int(expected.timestamp()),
                       "open": group[0]["open"], "high": max(row["high"] for row in group),
                       "low": min(row["low"] for row in group), "close": group[-1]["close"],
                       "volume": sum(row["volume"] for row in group)})
    return output


def validate_metadata(meta, item):
    broker, _, asset, _, market = ROUTES[item["route"]]
    expected_provider = "binance" if broker == "binance-futures" else broker
    if str(meta.get("provider", "")).lower() not in {expected_provider, "yfinance" if broker == "kis" else expected_provider}:
        raise ValueError("dataset-provider-route-mismatch")
    if meta.get("symbol") != item["symbol"] or meta.get("interval") != item.get("sourceTimeframe", item["timeframe"]):
        raise ValueError("dataset-symbol-or-timeframe-mismatch")
    allowed_market_types = {"", "stock"} if broker == "kis" else {market}
    if meta.get("asset_class") != asset or meta.get("market_type", "") not in allowed_market_types:
        raise ValueError("dataset-asset-or-market-type-mismatch")
    if str(meta.get("quality_status", "")).lower() not in {"ok", "pass", "passed"}:
        raise ValueError("dataset-quality-not-positive")
    if not meta.get("raw_content_sha256") or not meta.get("lineage_run_id") or not meta.get("dataset_id"):
        raise ValueError("real-source-lineage-required")


def load_rows(item):
    if item.get("format") == "binance-public-klines-json-v1":
        from collect_short_live_public_futures import parse_closed_klines, URL
        path = Path(item["path"]).resolve()
        raw = path.read_bytes()
        if digest(raw) != item["sha256"]:
            raise ValueError("dataset-raw-sha-changed")
        provenance = json.loads(Path(item["provenancePath"]).read_text(encoding="utf-8"))
        if hash_file(Path(item["provenancePath"])) != item["provenanceSha256"]:
            raise ValueError("public-provenance-raw-sha-changed")
        if provenance.get("rawSha256") != item["sha256"] or provenance.get("symbol") != item["symbol"] or provenance.get("timeframe") != item["timeframe"]:
            raise ValueError("public-provenance-scope-mismatch")
        if provenance.get("schemaVersion") != "short-live-public-candle-provenance-v1" or provenance.get("url") != URL or provenance.get("httpStatus") != 200 or provenance.get("unsigned") is not True or provenance.get("accountRequests") != 0 or provenance.get("orderRequests") != 0:
            raise ValueError("public-provenance-origin-invalid")
        rows = parse_closed_klines(raw, provenance["responseDate"])
        meta = provenance["datasetMetadata"]
        validate_metadata(meta, item)
        return select_observation_rows(rows, item["timeframe"]), meta
    import pyarrow.parquet as pq
    path = Path(item["path"]).resolve()
    before = hash_file(path)
    if before != item["sha256"]:
        raise ValueError("dataset-raw-sha-changed")
    table = pq.read_table(path)
    meta = {key.decode().removeprefix("stock_data_scraper."): value.decode()
            for key, value in (table.schema.metadata or {}).items() if key.startswith(b"stock_data_scraper.")}
    validate_metadata(meta, item)
    rows = []
    for raw in table.to_pylist():
        if str(raw.get("quality_status", "ok")).lower() not in {"ok", "pass", "passed"}:
            raise ValueError("dataset-row-quality-invalid")
        at = instant(raw.get("datetime") or raw.get("timestamp"))
        values = {key: float(raw[key]) for key in ("open", "high", "low", "close", "volume")}
        if not all(math.isfinite(v) for v in values.values()) or min(values[k] for k in ("open", "high", "low", "close")) <= 0:
            raise ValueError("invalid-real-ohlcv")
        if values["volume"] < 0 or values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
            raise ValueError("inconsistent-real-ohlcv")
        rows.append({"date": utc(at), "timestamp": int(at.timestamp()), **values})
    rows.sort(key=lambda row: row["timestamp"])
    if len({row["timestamp"] for row in rows}) != len(rows):
        raise ValueError("duplicate-original-bar-time")
    if hash_file(path) != before:
        raise ValueError("dataset-changed-during-read")
    source_tf = item.get("sourceTimeframe", item["timeframe"])
    if source_tf != item["timeframe"]:
        if source_tf != "1h" or item["timeframe"] != "1d" or item["route"] == "KIS_KR_CASH":
            raise ValueError("unsupported-resampling-no-synthetic-bars")
        rows = aggregate_utc_days(rows)
    return select_observation_rows(rows, item["timeframe"]), meta


def select_observation_rows(rows, timeframe, *, observed_at=None):
    """A predeclared latest 2048-row cost cap, independent of profit or signals."""
    seconds = {"5m": 300, "1h": 3600, "1d": 86400}[timeframe]
    cutoff = observed_at or datetime.now(timezone.utc)
    closed = [row for row in rows if instant(row["date"]) + timedelta(seconds=seconds) <= cutoff]
    return closed[-MAX_OBSERVATION_ROWS:]


def parity_report(rows, result, spec):
    equity = result.get("equity", [])
    warmup = required_warmup_bars(spec)
    if len(rows) <= warmup or len(equity) != len(rows):
        return {"status": "HOLD", "reason": "original-warmup-plus-one-observation-required",
                "bars": len(rows), "warmupBars": warmup, "compared": 0}
    seconds = {"5m": 300, "1h": 3600, "1d": 86400}[spec.timeframe]
    history = []; position = [0.0]; compared = []; evaluator = BuiltinBarSignalEvaluator(lambda _: position[0])
    for index, row in enumerate(rows):
        at = instant(row["date"])
        bar = ClosedBar(instrument_id=spec.instrument_id, symbol=spec.symbol, provider=spec.provider,
                        timeframe=spec.timeframe, start_time=utc(at), end_time=utc(at + timedelta(seconds=seconds)),
                        open=row["open"], high=row["high"], low=row["low"], close=row["close"], volume=row["volume"])
        history.append(bar)
        position[0] = float(equity[index-1].get("positionQuantity", 0)) if index else 0.0
        decision = evaluator(spec, bar, tuple(history))
        observed = {"buy": "BUY", "sell": "SELL", None: "HOLD"}.get(equity[index].get("signal"), "UNSUPPORTED")
        if index >= warmup:
            compared.append({"bar": row["date"], "paper": decision.signal, "backtester": observed,
                             "match": decision.signal == observed})
    finite = all(math.isfinite(float(row.get("value", float("nan")))) for row in equity)
    matches = sum(row["match"] for row in compared)
    return {"status": "PASS" if finite and matches == len(compared) else "HOLD",
            "meaning": PROFILE, "profitabilityRequired": False, "minimumTradesRequired": False,
            "bars": len(rows), "warmupBars": warmup, "compared": len(compared), "matches": matches,
            "finiteEquity": finite, "closedTradesObserved": len(result.get("trades", [])),
            "signalObservation": "OBSERVED" if result.get("trades") else "NO_ROUNDTRIP_OBSERVED_NOT_A_FAILURE",
            "comparisons": compared}


def write_new(path, value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode()
    with path.open("xb") as handle:
        handle.write(raw); handle.flush()
    return {"path": str(path.resolve()), "sha256": digest(raw)}


def prepare(manifest, output: Path, engine, *, manifest_sha256=None):
    output = output.resolve()
    if not output.is_relative_to(WORKSPACE.resolve()) or output.exists():
        raise ValueError("new-workspace-publication-root-required")
    if manifest.get("schemaVersion") != "short-live-real-data-preparation-v1":
        raise ValueError("manifest-schema-invalid")
    data = manifest.get("datasets")
    if not isinstance(data, list) or not data:
        raise ValueError("pinned-real-datasets-required")
    output.mkdir(parents=True); (output / "strategy-instances").mkdir(); (output / "portfolios").mkdir(); (output / "reports").mkdir()
    receipt = {"schemaVersion": "short-live-offline-publication-receipt-v1", "purpose": "FUNCTIONAL_TEST_ONLY",
               "promotionEligible": False, "actualLivePassCount": 0, "canPlaceOrders": False,
               "functionalSizingProfile": SIZING_PROFILE, "sourcePositionSizePercent": POSITION_SIZE_PERCENT,
               "sizingMeaning": "100% of allocated bounded small test sleeve; shared broker capital cap remains enforced",
               "artifactRoot": str(output), "published": [], "holes": [], "outputs": [], "datasets": data,
               "preparationToolSha256": hash_file(Path(__file__)),
               "inputManifestRawSha256": manifest_sha256,
               "observationPolicy": {"selection": "latest-original-closed-bars-before-signal-or-profit-evaluation",
                                     "maximumRows": MAX_OBSERVATION_ROWS, "minimumRows": "original-plugin-warmup-plus-one"}}
    for item in data:
        route, tf = item["route"], item["timeframe"]
        if route not in ROUTES or tf not in {"5m", "1h", "1d"}:
            raise ValueError("unsupported-dataset-scope")
        try:
            rows, meta = load_rows(item)
            observation = {"rows": len(rows), "firstBar": rows[0]["date"] if rows else None,
                           "lastBar": rows[-1]["date"] if rows else None, "maximumRows": MAX_OBSERVATION_ROWS,
                           "selectedRowsSha256": digest(json.dumps(rows, sort_keys=True, allow_nan=False).encode())}
            broker, provider, asset, currency, market = ROUTES[route]
            symbol = item["symbol"] + (".PERP" if route == "BINANCE_USDM_FUTURES" else "")
            children = []
            for plugin, strategy, params in MODELS:
                parameters = {**params, "executionTiming": "signal-close", "positionSize": POSITION_SIZE_PERCENT}
                parameter_digest = digest(json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode())[:12]
                model_id = f"bounded-p100-{route.lower()}-{tf}-{plugin}-{parameter_digest}-{item['sha256'][:12]}"
                settings = {**parameters, "strategyId": strategy, "interval": tf, "assetClass": asset,
                            "initialCapital": 100000, "fee": 0.1, "slippage": 0.05}
                result = engine.run_strategy_backtest(rows, settings)
                spec = RuntimeStrategySpec(portfolio_id=model_id, portfolio_hash="a"*64,
                    strategy_instance_id="si-"+model_id, strategy_id=model_id, artifact_hash="b"*64,
                    plugin_id=plugin, instrument_id=symbol, symbol=symbol, timeframe=tf,
                    broker_id=broker, provider=provider, parameters=parameters)
                parity = parity_report(rows, result, spec)
                report = {"schemaVersion": "short-functional-actual-backtest-paper-parity-v1", "dataset": item,
                          "dataMetadata": meta, "observation": observation, "settings": settings, "parity": parity, "backtester": result,
                          "functionalSizingProfile": SIZING_PROFILE, "sourcePositionSizePercent": POSITION_SIZE_PERCENT,
                          "parityScope": "original algorithm signal parity; actual native sizing is separately guarded, not claimed by this report",
                          "engineFileSha256": hash_file(Path(engine.__file__)), "promotionEligible": False}
                report_ref = write_new(output / "reports" / (model_id+".json"), report); receipt["outputs"].append(report_ref)
                print(json.dumps({"route": route, "timeframe": tf, "plugin": plugin,
                                  "status": parity["status"], "bars": len(rows)}, ensure_ascii=True), flush=True)
                if parity["status"] != "PASS":
                    receipt["holes"].append({"route": route, "timeframe": tf, "plugin": plugin, "reason": parity.get("reason", "actual-parity-hold"), "report": report_ref}); continue
                source = seal_strategy_artifact({"id": model_id, "strategy_id": model_id, "artifact_schema_version": "strategy-artifact-v2",
                    "name": f"소액 배정금 100% 기능시험 · {item['symbol']} · {tf} · {plugin}",
                    "functionalSizingProfile": SIZING_PROFILE, "sourcePositionSizePercent": POSITION_SIZE_PERCENT, "asset": asset, "symbol": symbol,
                    "timeframe": tf, "plugin": plugin, "brokerId": broker, "marketDataProvider": provider,
                    "marketType": market, "parameters": parameters, "settings": settings,
                    "executionPurpose": "FUNCTIONAL_TEST_ONLY", "evidenceClass": PROFILE,
                    "finalTest": {"status": "PASS", "meaning": PROFILE, "report": report_ref},
                    "backtest_metrics": result.get("metrics", {}), "dataArtifact": {**item, "metadata": meta, "observation": observation},
                    "promotionEligible": False, "useAsPromotionEvidence": False, "live_allowed": False,
                    "paper_trader_verified": False, "permissions": {"live_allowed": False, "trader_export_allowed": False}})
                instance = seal_strategy_instance({"schemaVersion": "strategy-instance-template-v1", "instanceId": "si-"+model_id,
                    "name": source["name"], "sourceStrategyId": model_id, "sourceArtifactHash": compute_strategy_artifact_hash(source),
                    "qualifiedSymbol": symbol, "qualifiedTimeframe": tf, "brokerId": broker, "marketDataProvider": provider,
                    "asset": asset, "marketType": market, "pluginId": plugin, "parameters": parameters,
                    "promotionEligible": False, "qualification": {"purpose": PROFILE, "report": report_ref}})
                receipt["outputs"].append(write_new(output / (model_id+".json"), source))
                receipt["outputs"].append(write_new(output / "strategy-instances" / (instance["instanceId"]+".json"), instance))
                children.append(instance); receipt["published"].append({"route": route, "timeframe": tf, "kind": "STRATEGY_INSTANCE", "id": instance["instanceId"]})
            for label, members in (("single", children[:1]), ("multi", children)):
                if not members or (label == "multi" and len(members) < 2):
                    receipt["holes"].append({"route": route, "timeframe": tf, "kind": label+"-portfolio", "reason": "insufficient-genuine-parity-candidates"}); continue
                member_digest = digest(json.dumps([member["parameters"] for member in members], sort_keys=True, separators=(",", ":")).encode())[:12]
                pid = f"bounded-p100-{route.lower()}-{tf}-{label}-portfolio-{member_digest}-{item['sha256'][:12]}"
                embedded = [{**member, "templateInstanceId": member["instanceId"], "strategyId": member["sourceStrategyId"],
                             "sourceInstanceHash": compute_strategy_instance_hash(member),
                             "allocation": {"normalizedWeight": 1/len(members)}} for member in members]
                portfolio = seal_portfolio_artifact({"id": pid, "name": f"소액 배정금 100% 기능시험 · {route} · {tf} · {label}",
                    "functionalSizingProfile": SIZING_PROFILE, "sourcePositionSizePercent": POSITION_SIZE_PERCENT,
                    "schemaVersion": "portfolio-artifact-v1", "strategyInstances": embedded,
                    "portfolio": {"strategyMode": "multi_strategy", "baseCurrency": currency},
                    "executionPurpose": "FUNCTIONAL_TEST_ONLY", "evidenceClass": PROFILE,
                    "promotionEligible": False, "useAsPromotionEvidence": False,
                    "permissions": {"live_allowed": False, "trader_export_allowed": False}})
                receipt["outputs"].append(write_new(output / "portfolios" / (pid+".json"), portfolio))
                receipt["published"].append({"route": route, "timeframe": tf, "kind": "PORTFOLIO_INSTANCE", "id": pid, "memberCount": len(members)})
        except (ValueError, OSError, KeyError) as exc:
            receipt["holes"].append({"route": route, "timeframe": tf, "reason": str(exc)[:200]})
    for route in ROUTES:
        for tf in ("5m", "1h", "1d"):
            if not any(item["route"] == route and item["timeframe"] == tf for item in data):
                receipt["holes"].append({"route": route, "timeframe": tf, "reason": "no-pinned-genuine-local-dataset"})
    write_new(output / "publication-receipt.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True); parser.add_argument("--backtester-root", type=Path, required=True)
    parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    manifest_raw = args.manifest.read_bytes()
    manifest = json.loads(manifest_raw)
    if not args.execute:
        print(json.dumps({"status": "PLAN_ONLY", "datasets": len(manifest.get("datasets", [])), "canPlaceOrders": False})); return
    sys.path.insert(0, str(args.backtester_root.resolve()))
    engine = importlib.import_module("server_compute")
    receipt = prepare(manifest, args.output, engine, manifest_sha256=digest(manifest_raw))
    print(json.dumps({"status": "OFFLINE_FUNCTIONAL_REVIEW_ONLY", "artifactRoot": receipt["artifactRoot"],
                      "published": len(receipt["published"]), "holes": receipt["holes"], "actualLivePassCount": 0}, ensure_ascii=True))


if __name__ == "__main__":
    main()
