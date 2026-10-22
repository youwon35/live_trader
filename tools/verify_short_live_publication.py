"""Read-only publication verification; optional create-only raw-file manifest.

This verifies historical signal parity evidence and canonical links only.
It never reads accounts, starts a runtime, changes promotion, or grants orders.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_ROOT)); sys.path.insert(0, str(APP_ROOT.parents[1] / "packages" / "trading_runtime"))
from live_trader.short_live_catalog import ShortLiveCatalogue, ShortLiveCatalogues
from prepare_short_live_candidates import PROFILE, ROUTES, write_new


def hash_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_report(source, report):
    """Source and report must retain one declared execution timing and inputs."""
    parity = report.get("parity") or {}
    if report.get("schemaVersion") != "short-functional-actual-backtest-paper-parity-v1" or report.get("promotionEligible") is not False:
        raise ValueError("functional-report-schema-or-purpose-invalid")
    parameters, settings = source.get("parameters") or {}, report.get("settings") or {}
    timing = {parameters.get("executionTiming"), (source.get("settings") or {}).get("executionTiming"), settings.get("executionTiming")}
    if len(timing) != 1 or not timing.issubset({"signal-close", "next-open"}):
        raise ValueError("source-report-execution-timing-conflict")
    if any(settings.get(key) != value for key, value in parameters.items()):
        raise ValueError("source-report-parameter-conflict")
    if settings.get("interval") != source.get("timeframe") or report.get("dataset") != {k:v for k,v in (source.get("dataArtifact") or {}).items() if k not in {"metadata", "observation"}}:
        raise ValueError("source-report-dataset-or-timeframe-conflict")
    comparisons = parity.get("comparisons") or []
    if parity.get("status") != "PASS" or parity.get("meaning") != PROFILE or parity.get("finiteEquity") is not True or parity.get("compared", 0) < 1:
        raise ValueError("actual-functional-signal-parity-not-positive")
    if parity.get("profitabilityRequired") is not False or parity.get("minimumTradesRequired") is not False:
        raise ValueError("functional-criteria-changed")
    if len(comparisons) != parity["compared"] or parity.get("matches") != len(comparisons) or any(
        row.get("match") is not True or row.get("paper") != row.get("backtester") or row.get("paper") not in {"BUY","SELL","HOLD"} for row in comparisons):
        raise ValueError("actual-functional-comparisons-inconsistent")
    if parity.get("bars") != len((report.get("backtester") or {}).get("equity") or []) or parity["bars"]-parity.get("warmupBars", parity["bars"]) != len(comparisons):
        raise ValueError("actual-observation-count-inconsistent")
    equity = report["backtester"]["equity"]
    if any(not math.isfinite(float(row.get("value", float("nan")))) for row in equity):
        raise ValueError("actual-backtester-equity-not-finite")
    if any(row.get("bar") != equity[index + parity["warmupBars"]].get("date") for index,row in enumerate(comparisons)):
        raise ValueError("actual-comparison-bar-binding-changed")


def verify(root, shared_root=None):
    root = Path(root).resolve()
    receipt_path = root / "publication-receipt.json"
    receipt = json.loads(receipt_path.read_bytes())
    if receipt.get("schemaVersion") != "short-live-offline-publication-receipt-v1" or receipt.get("purpose") != "FUNCTIONAL_TEST_ONLY" or receipt.get("canPlaceOrders") is not False or receipt.get("actualLivePassCount") != 0 or receipt.get("promotionEligible") is not False:
        raise ValueError("functional-publication-purpose-invalid")
    for ref in receipt.get("outputs") or []:
        path = Path(ref["path"]).resolve()
        if not path.is_relative_to(root) or path.is_symlink() or hash_file(path) != ref["sha256"]:
            raise ValueError("publication-output-path-or-raw-hash-changed")
    reviewed = 0
    for path in root.glob("*.json"):
        source = json.loads(path.read_bytes())
        if source.get("artifact_schema_version") != "strategy-artifact-v2":
            continue
        permissions = source.get("permissions") or {}
        if source.get("executionPurpose") != "FUNCTIONAL_TEST_ONLY" or source.get("evidenceClass") != PROFILE or source.get("promotionEligible") is not False or source.get("useAsPromotionEvidence") is not False or source.get("live_allowed") is not False or source.get("paper_trader_verified") is not False or permissions.get("live_allowed") is not False or permissions.get("trader_export_allowed") is not False:
            raise ValueError("source-nonpromotion-flags-changed")
        ref = (source.get("finalTest") or {}).get("report") or {}
        report_path = Path(ref["path"]).resolve()
        if not report_path.is_relative_to(root / "reports") or hash_file(report_path) != ref["sha256"]:
            raise ValueError("source-report-raw-link-changed")
        validate_report(source, json.loads(report_path.read_bytes())); reviewed += 1
    catalogue = ShortLiveCatalogue(root).snapshot()
    if any(row["blockers"] for row in catalogue["candidates"]):
        raise ValueError("new-publication-canonical-catalogue-blocked")
    per_scope = []
    for route in ROUTES:
        for timeframe in ("5m","1h","1d"):
            group = [row for row in catalogue["candidates"] if route in row.get("routes",[]) and timeframe in row.get("timeframes",[])]
            instance_rows = [row for row in group if row["kind"] == "STRATEGY_INSTANCE"]
            portfolio_rows = [row for row in group if row["kind"] == "PORTFOLIO_INSTANCE"]
            per_scope.append({"route":route,"timeframe":timeframe,"singleInstance":len(instance_rows)>=1,
                "multiInstance":len(instance_rows)>=2,"singleStrategyPortfolio":any(row["memberCount"]==1 for row in portfolio_rows),
                "multiStrategyPortfolio":any(row["memberCount"]>=2 and len(row["pluginIds"])>=2 for row in portfolio_rows),
                "instanceIds":[row["id"] for row in instance_rows],"portfolioIds":[row["id"] for row in portfolio_rows]})
    combined = ShortLiveCatalogues([shared_root,root]).snapshot() if shared_root else catalogue
    files = [{"relativePath":path.relative_to(root).as_posix(),"sha256":hash_file(path),"bytes":path.stat().st_size}
             for path in sorted(root.rglob("*")) if path.is_file()]
    return {"schemaVersion":"short-live-publication-raw-files-manifest-v1","artifactRoot":str(root),
        "publicationReceiptSha256":hash_file(receipt_path),"inputManifestRawSha256":receipt.get("inputManifestRawSha256"),
        "files":files,"sourceReportPairsVerified":reviewed,"newCandidateCount":len(catalogue["candidates"]),
        "newCanonicalEligibleCount":sum(row["eligibleForFunctionalPreparation"] for row in catalogue["candidates"]),
        "combinedCoverage":combined["coverage"],"newScopeForms":per_scope,"holes":receipt["holes"],
        "readOnlyVerification":True,"actualLivePassCount":0,"canPlaceOrders":False,"promotionEligible":False}


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--shared-root",type=Path); parser.add_argument("--manifest-output",type=Path)
    args=parser.parse_args(); result=verify(args.root,args.shared_root)
    ref=None
    if args.manifest_output:
        output=args.manifest_output.resolve()
        if not output.is_relative_to(APP_ROOT.parents[1]): raise ValueError("workspace-manifest-only")
        ref=write_new(output,result)
    print(json.dumps({"sourceReportPairsVerified":result["sourceReportPairsVerified"],"newCandidateCount":result["newCandidateCount"],
        "newCanonicalEligibleCount":result["newCanonicalEligibleCount"],"all24CellsEligible":all(row["eligibleCount"]>0 for row in result["combinedCoverage"]),
        "all48SingleMultiScopeForms":all(all(row[key] for key in ("singleInstance","multiInstance","singleStrategyPortfolio","multiStrategyPortfolio")) for row in result["newScopeForms"]),
        "actualLivePassCount":0,"manifest":ref},ensure_ascii=True))


if __name__=="__main__": main()
