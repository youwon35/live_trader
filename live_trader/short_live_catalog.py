"""Read-only exact canonical catalogue for explicitly bounded Live wiring tests.

Preparation eligibility is deliberately distinct from ordinary Live promotion.
No registry, permission, historical result, or source artifact is rewritten here.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from trading_runtime.artifact_governance import (
    assert_verified_strategy_artifact, assert_verified_strategy_instance,
)
from trading_runtime.artifact_paths import shared_artifact_root
from trading_runtime.continuous_runtime import RuntimeStrategySpec
from trading_runtime.portfolio_runtime import load_portfolio_runtime_path, required_warmup_bars, portfolio_snapshot_reads
from trading_runtime.portfolio_runtime import _assert_instance_execution_parity
from trading_runtime.realtime_feeds import infer_market_route
from .short_live_contract import source_position_size

ROUTES = ("KIS_KR_CASH", "BINANCE_SPOT", "BINANCE_USDM_FUTURES", "UPBIT_KRW_SPOT")
TIMEFRAMES = ("5m", "1h", "1d")
ALIASES = {"ma-cross": "moving_average_cross", "moving-average-cross": "moving_average_cross",
           "rsi-revert": "rsi_revert", "custom-draft": "strategy_builder_custom"}

def _read(path: Path) -> tuple[dict, str]:
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("artifact-object-required")
    return value, hashlib.sha256(raw).hexdigest()

def _identity(row: dict) -> str:
    return str(row.get("instanceId") or row.get("id") or row.get("artifactId") or "")

def _route(spec: RuntimeStrategySpec) -> str:
    broker, symbol = spec.broker_id.lower(), spec.symbol.upper()
    instance = spec.artifact.get("strategyInstance") or {}
    market_type = str(instance.get("marketType") or spec.artifact.get("marketType") or spec.parameters.get("marketType") or "").lower()
    if broker == "kis" and re.fullmatch(r"[0-9]{6}(?:\.K[QS])?", symbol):
        return "KIS_KR_CASH"
    if broker == "upbit" and symbol.startswith("KRW-"):
        return "UPBIT_KRW_SPOT"
    if broker in {"binance-futures", "binance_usdm", "binance-usdm"} or (broker == "binance" and market_type == "futures"):
        return "BINANCE_USDM_FUTURES"
    if broker == "binance" and symbol.endswith("USDT"):
        return "BINANCE_SPOT"
    raise ValueError("unsupported-short-live-market-route")

def _review(source: dict, root: Path | None = None) -> dict:
    _functional_report_pin(source, root)
    final = source.get("finalTest") or source.get("final_test") or {}
    return {"backtestStatus": str(final.get("status") or "NOT_OBSERVED").upper(),
            "originalExecutionPurpose": source.get("executionPurpose"),
            "ordinaryLivePromotionGranted": False,
            "paperQualificationClaimed": False}


def _functional_report_pin(source: dict, root: Path | None = None) -> list[dict]:
    if source.get("evidenceClass") != "SHORT_FUNCTIONAL_HISTORY_PARITY_ONLY_V1":
        return []
    final = source.get("finalTest") or {}
    reference = final.get("report") or {}
    path = Path(str(reference.get("path") or ""))
    if root is not None:
        portable = root / "reports" / path.name
        if portable.is_file() and not portable.is_symlink():
            # Relocation preserves the original report bytes and raw hash;
            # neither the canonical source nor its recorded URI is rewritten.
            path = portable
    if not path.is_file() or path.is_symlink():
        raise ValueError("functional-backtest-report-unavailable")
    report, raw_sha = _read(path)
    parity = report.get("parity") or {}
    comparisons = parity.get("comparisons")
    compared = parity.get("compared")
    if (raw_sha != reference.get("sha256")
            or report.get("schemaVersion") != "short-functional-actual-backtest-paper-parity-v1"
            or report.get("promotionEligible") is not False
            or final.get("meaning") != source["evidenceClass"]
            or parity.get("meaning") != source["evidenceClass"]
            or parity.get("status") != "PASS" or parity.get("finiteEquity") is not True
            or type(compared) is not int or compared < 1 or parity.get("matches") != compared
            or not isinstance(comparisons, list) or len(comparisons) != compared
            or any(not isinstance(r, dict) or r.get("match") is not True or r.get("backtester") != r.get("paper") for r in comparisons)
            or (report.get("settings") or {}).get("interval") != source.get("timeframe")
            or (report.get("dataset") or {}).get("sha256") != (source.get("dataArtifact") or {}).get("sha256")):
        raise ValueError("functional-backtest-report-or-parity-changed")
    return [{"path": str(path.resolve()), "sha256": raw_sha}]

def _timing(spec: RuntimeStrategySpec) -> str:
    artifact = spec.artifact
    values = [spec.parameters.get("executionTiming"), artifact.get("executionTiming")]
    for key in ("strategyContract", "executionContract", "execution"):
        body = artifact.get(key)
        if isinstance(body, dict):
            values.extend((body.get("executionTiming"), body.get("timing")))
    timings = {str(v).strip().lower() for v in values if v not in (None, "")}
    if len(timings) != 1 or not timings.issubset({"next-open", "signal-close"}):
        raise ValueError("original-execution-timing-missing-conflicting-or-unsupported")
    return next(iter(timings))


def _native_spec(spec: RuntimeStrategySpec) -> RuntimeStrategySpec:
    route = _route(spec)
    if route == "KIS_KR_CASH":
        symbol = spec.symbol.split(".")[0]
        return replace(spec, provider="kis", instrument_id=symbol, symbol=symbol)
    if route == "BINANCE_USDM_FUTURES":
        symbol = spec.symbol.removesuffix(".PERP").replace("-", "")
        provider, broker, instrument = infer_market_route(symbol, "futures")
        return replace(spec, provider=provider, broker_id=broker,
                       instrument_id=instrument, symbol=symbol)
    expected = "binance" if route == "BINANCE_SPOT" else "upbit"
    if spec.provider != expected:
        raise ValueError("native-provider-route-mismatch")
    return spec

class ShortLiveCatalogue:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root) if root is not None else shared_artifact_root()

    def _documents(self, root: Path):
        for path in sorted(root.glob("*.json")):
            if path.name.endswith("registry.json"):
                continue
            try:
                value, sha = _read(path)
                yield path, value, sha
            except (OSError, ValueError, TypeError):
                continue

    def _build(self):
        # One UI read/resolve shares verified graph reads only within this call.
        # A later prepare or start begins a new scope and re-reads source bytes.
        with portfolio_snapshot_reads():
            return self._build_uncached()

    def _build_uncached(self):
        sources = list(self._documents(self.root))
        instances = list(self._documents(self.root / "strategy-instances"))
        portfolios = list(self._documents(self.root / "portfolios"))
        source_ids = Counter(_identity(s) for _, s, _ in sources)
        instance_ids = Counter(_identity(s) for _, s, _ in instances)
        portfolio_ids = Counter(_identity(s) for _, s, _ in portfolios)
        rows, resolved = [], {}
        for kind, docs, counts in (("STRATEGY_INSTANCE", instances, instance_ids),
                                   ("PORTFOLIO_INSTANCE", portfolios, portfolio_ids)):
            for path, value, raw_sha in docs:
                identity = _identity(value)
                if not identity:
                    continue
                root_key = hashlib.sha256(str(self.root.resolve()).encode()).hexdigest()[:16]
                key = f"{root_key}:{kind}:{identity}:{raw_sha}"
                row = {"selectionKey": key, "targetKey": key, "id": identity,
                       "kind": kind, "label": str(value.get("name") or identity),
                       "artifactFileSha256": raw_sha, "instanceFileSha256": raw_sha if kind == "STRATEGY_INSTANCE" else "",
                       "blockers": [], "canPlaceOrders": False, "promotionEligible": False}
                pins = [{"path": str(path.resolve()), "sha256": raw_sha}]
                try:
                    if counts[identity] != 1:
                        raise ValueError("duplicate-exact-artifact-id")
                    if kind == "STRATEGY_INSTANCE":
                        instance_hash = assert_verified_strategy_instance(value)
                        source_id = str(value.get("sourceStrategyId") or "")
                        if source_ids[source_id] != 1:
                            raise ValueError("source-strategy-missing-or-ambiguous")
                        sp, source, source_sha = next(s for s in sources if _identity(s[1]) == source_id)
                        source_hash = assert_verified_strategy_artifact(source)
                        if source_hash != str(value.get("sourceArtifactHash") or ""):
                            raise ValueError("source-strategy-link-hash-mismatch")
                        _assert_instance_execution_parity(embedded=value, standalone=value, strategy=source)
                        pins.append({"path": str(sp.resolve()), "sha256": source_sha})
                        symbol = str(value.get("qualifiedSymbol") or value.get("symbol") or "").upper()
                        timeframe = str(value.get("qualifiedTimeframe") or value.get("timeframe") or "").lower()
                        broker = str(value.get("brokerId") or "").lower()
                        provider = str(value.get("marketDataProvider") or broker).lower()
                        provider = {"yfinance": "yahoo"}.get(provider, provider)
                        plugin = str(value.get("pluginId") or source.get("plugin") or "").lower()
                        parameters = value.get("parameters")
                        if not isinstance(parameters, dict) or not plugin:
                            raise ValueError("exact-plugin-parameters-required")
                        spec = RuntimeStrategySpec(
                            portfolio_id="short-live-standalone:" + identity,
                            portfolio_hash=instance_hash, strategy_instance_id=identity,
                            strategy_id=source_id, artifact_hash=source_hash,
                            plugin_id=ALIASES.get(plugin, plugin.replace("-", "_")),
                            instrument_id=symbol, symbol=symbol, timeframe=timeframe,
                            provider=provider, broker_id=broker, parameters=dict(parameters),
                            artifact={**source, "strategyInstance": value})
                        specs = [spec]
                        reviews = [_review(source, self.root)]
                    else:
                        loaded = load_portfolio_runtime_path(path, artifact_root=self.root)
                        specs = list(loaded.specs)
                        reviews = [_review(s.artifact, self.root) for s in specs]
                        # Pin all exact descendants of the verified portfolio graph.
                        for spec in specs:
                            source_matches = [d for d in sources if _identity(d[1]) == spec.strategy_id]
                            if len(source_matches) != 1:
                                raise ValueError("portfolio-source-identity-ambiguous")
                            sp, _, ss = source_matches[0]
                            pins.append({"path": str(sp.resolve()), "sha256": ss})
                            embedded = spec.artifact.get("strategyInstance") or {}
                            original = embedded.get("verifiedStrategyInstance") or {}
                            iid = _identity(original)
                            matches = [d for d in instances if _identity(d[1]) == iid]
                            if len(matches) != 1:
                                raise ValueError("portfolio-instance-link-missing-or-ambiguous")
                            ip, _, ins = matches[0]
                            pins.append({"path": str(ip.resolve()), "sha256": ins})
                    routes = sorted({_route(spec) for spec in specs})
                    if any(s.timeframe not in TIMEFRAMES for s in specs):
                        raise ValueError("unsupported-timeframe")
                    # Explicit KIS Live source; original research provider is retained
                    # in artifact metadata, never used as a fabricated native bar.
                    specs = [_native_spec(s) for s in specs]
                    sizing = [source_position_size(s.parameters, s.plugin_id, s.artifact) for s in specs]
                    if any(r["backtestStatus"] not in {"PASS", "PASSED"} for r in reviews):
                        row["blockers"].append("functional-backtest-review-required")
                    row.update({"route": routes[0] if len(routes) == 1 else "MULTI_ROUTE", "routes": routes,
                                "timeframe": specs[0].timeframe if len({s.timeframe for s in specs}) == 1 else "MULTI_TIMEFRAME",
                                "timeframes": sorted({s.timeframe for s in specs}),
                                "symbols": sorted({s.symbol for s in specs}),
                                "pluginIds": sorted({s.plugin_id for s in specs}),
                                "positionSizePercents": sorted({size[0] for size in sizing}),
                                "sourcePositionSizeWasDefaulted": any(size[2] for size in sizing),
                                "functionalSizingProfiles": sorted({str(s.artifact.get("functionalSizingProfile")) for s in specs
                                                                    if s.artifact.get("functionalSizingProfile")}),
                                "memberCount": len(specs), "reviews": reviews,
                                "requiredWarmupBars": max(required_warmup_bars(s) for s in specs)})
                    serialized = []
                    for spec in specs:
                        pins.extend(_functional_report_pin(spec.artifact, self.root))
                        serialized.append({**spec.to_dict(), "artifact": spec.artifact, "route": _route(spec),
                                           "executionTiming": _timing(spec), "selectionKey": key})
                    resolved[key] = {"target": row, "specs": serialized, "sourcePins": pins}
                except (ValueError, OSError, TypeError, KeyError) as exc:
                    row["blockers"].append(str(exc).splitlines()[0][:180])
                row["eligibleForFunctionalPreparation"] = not row["blockers"]
                rows.append(row)
        return rows, resolved

    def snapshot(self) -> dict[str, Any]:
        rows, _ = self._build()
        coverage = []
        for route in ROUTES:
            for timeframe in TIMEFRAMES:
                for kind in ("STRATEGY_INSTANCE", "PORTFOLIO_INSTANCE"):
                    group = [r for r in rows if route in r.get("routes", []) and timeframe in r.get("timeframes", []) and r["kind"] == kind]
                    coverage.append({"route": route, "timeframe": timeframe, "kind": kind,
                                     "candidateCount": len(group),
                                     "eligibleCount": sum(r["eligibleForFunctionalPreparation"] for r in group),
                                     "actualLivePassCount": 0})
        return {"candidates": rows, "coverage": coverage, "readOnly": True,
                "canPlaceOrders": False, "promotionEligible": False}

    def resolve(self, selections: list[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(selections, list) or not selections:
            raise ValueError("exact-selection-required")
        _, resolved = self._build()
        targets, specs, pins, keys = [], [], [], set()
        for selection in selections:
            key = str(selection.get("selectionKey") or selection.get("targetKey") or "")
            if key in keys:
                raise ValueError("duplicate-selection")
            keys.add(key)
            item = resolved.get(key)
            if item is None:
                raise ValueError("exact-selection-not-found-or-source-changed")
            target = item["target"]
            if selection.get("artifactFileSha256") != target["artifactFileSha256"]:
                raise ValueError("selection-file-hash-required-or-changed")
            if target["kind"] == "STRATEGY_INSTANCE" and selection.get("instanceFileSha256") != target["instanceFileSha256"]:
                raise ValueError("selection-instance-file-hash-required-or-changed")
            if target["blockers"]:
                raise ValueError(";".join(target["blockers"]))
            targets.append(dict(target)); specs.extend(item["specs"]); pins.extend(item["sourcePins"])
        ids = [s["strategyInstanceId"] for s in specs]
        if len(ids) != len(set(ids)):
            raise ValueError("same-strategy-instance-selected-through-multiple-parents")
        return {"targets": targets, "specs": specs,
                "sourcePins": list({p["path"]: p for p in pins}.values())}


class ShortLiveCatalogues:
    """Exact roots, including append-only non-promotional preparation copies."""
    def __init__(self, roots=None):
        self.roots = roots

    def _catalogues(self):
        if self.roots is None:
            from .read_only_preparation import dedicated_preparation_roots
            roots = [shared_artifact_root(), *dedicated_preparation_roots()]
        else:
            roots = self.roots
        return [ShortLiveCatalogue(path) for path in dict.fromkeys(Path(r).resolve() for r in roots)]

    def snapshot(self):
        rows = [row for catalog in self._catalogues() for row in catalog.snapshot()["candidates"]]
        coverage = []
        for route in ROUTES:
            for timeframe in TIMEFRAMES:
                for kind in ("STRATEGY_INSTANCE", "PORTFOLIO_INSTANCE"):
                    group = [r for r in rows if route in r.get("routes", []) and timeframe in r.get("timeframes", []) and r["kind"] == kind]
                    coverage.append({"route": route, "timeframe": timeframe, "kind": kind,
                                     "candidateCount": len(group),
                                     "eligibleCount": sum(r["eligibleForFunctionalPreparation"] for r in group),
                                     "actualLivePassCount": 0})
        return {"candidates": rows, "coverage": coverage, "readOnly": True, "promotionEligible": False}

    def resolve(self, selections):
        catalogs = self._catalogues()
        by_root = {hashlib.sha256(str(c.root.resolve()).encode()).hexdigest()[:16]: c for c in catalogs}
        grouped = {}
        for selection in selections:
            key = str(selection.get("selectionKey") or selection.get("targetKey") or "")
            root = key.split(":", 1)[0]
            if root not in by_root:
                raise ValueError("exact-artifact-root-unavailable")
            grouped.setdefault(root, []).append(selection)
        result = {"targets": [], "specs": [], "sourcePins": []}
        for root, values in grouped.items():
            resolved = by_root[root].resolve(values)
            for field in result:
                result[field].extend(resolved[field])
        ids = [s["strategyInstanceId"] for s in result["specs"]]
        if len(ids) != len(set(ids)):
            raise ValueError("same-strategy-instance-selected-through-multiple-roots")
        return result
