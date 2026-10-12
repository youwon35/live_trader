"""Plan or import one exact qualified Paper candidate as draft/MONITOR only.

No account review, challenge/confirmation, broker, arm or runtime APIs exist in
this tool. Default is a read-only plan. --apply invokes the existing CAS service.
"""
from __future__ import annotations
import argparse, importlib, json, sys, types
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP.parents[1]/"packages/trading_runtime"))
PACKAGE = "_inactive_paper_candidate_tool"
package = types.ModuleType(PACKAGE); package.__path__ = [str(APP/"live_trader")]; sys.modules[PACKAGE] = package
inbox = importlib.import_module(PACKAGE+".paper_candidate_inbox")
importer = importlib.import_module(PACKAGE+".paper_candidate_import")
from trading_runtime.artifact_governance import stable_sha256


def exact_candidate(*, evidence_id, strategy_id, strategy_hash, instance_id, portfolio_id="", portfolio_hash="", root=None):
    configured = inbox.configured_artifact_roots()
    if root is not None:
        matches = [folder for folder in configured if folder.resolve() == Path(root).resolve()]
        if len(matches) != 1:
            raise ValueError("The selected root is not an exact configured Live artifact root")
        configured = matches
    result = inbox.list_paper_candidates(roots=configured)
    if result.get("ok") is not True:
        raise ValueError("Current candidate root cannot be verified: " + "; ".join(result.get("errors") or []))
    rows = [row for row in result["candidates"] if row.get("evidenceId") == evidence_id]
    if len(rows) != 1 or rows[0].get("status") != "VERIFIED_READ_ONLY":
        raise ValueError("Exact requested Paper publication is absent, duplicated, or unqualified")
    row = rows[0]; identity = row["identity"]
    expected = {"strategyArtifactId": strategy_id, "strategyArtifactHash": strategy_hash, "strategyInstanceId": instance_id, "portfolioArtifactId": portfolio_id, "portfolioArtifactHash": portfolio_hash}
    if any(identity.get(key, "") != value for key, value in expected.items()):
        raise ValueError("Publication Source/Instance/Portfolio differs from the explicitly expected graph")
    if not row.get("canImport") and not (row.get("registered") and row.get("canReview")):
        raise ValueError("Exact candidate cannot be registered as an inactive unresolved draft")
    return configured, row


def run(args):
    roots, row = exact_candidate(evidence_id=args.evidence_id, strategy_id=args.strategy_id, strategy_hash=args.strategy_hash, instance_id=args.instance_id, portfolio_id=args.portfolio_id, portfolio_hash=args.portfolio_hash, root=args.root)
    result = {"ok": True, "operation": "READ_ONLY_IMPORT_PLAN", "evidenceId": row["evidenceId"], "strategyId": row["strategyId"], "portfolioId": row.get("portfolioId", ""), "canImport": row["canImport"], "alreadyRegistered": row.get("registered") is True, "expectedRevision": row["deployment"]["revision"], "registryHash": row["registryHash"], "authorizationGranted": False, "accountReviewPerformed": False, "armingCalls": 0, "runtimeStarts": 0, "brokerCalls": 0, "ordersSubmitted": 0}
    if args.apply:
        # Refresh the exact plan immediately before the service takes its CAS lock.
        roots, row = exact_candidate(evidence_id=args.evidence_id, strategy_id=args.strategy_id, strategy_hash=args.strategy_hash, instance_id=args.instance_id, portfolio_id=args.portfolio_id, portfolio_hash=args.portfolio_hash, root=args.root)
        request = row.get("importRequest") or {"rootKey": row["rootKey"], "evidenceId": row["evidenceId"], "identityHash": stable_sha256({"identity": row["identity"], "instanceHash": row["instanceHash"]}), "expectedRevision": row["deployment"]["revision"], "registryHash": row["registryHash"]}
        imported = importer.import_paper_candidate(request, roots=roots)
        root = next(folder for folder in roots if stable_sha256(str(folder.resolve())) == row["rootKey"])
        saved = inbox._registry(root)["entries"][imported["deploymentId"]]
        permissions = saved.get("permissions") or {}
        if imported.get("ok") is not True or saved.get("lifecycle") != "draft" or saved.get("mode") != "MONITOR" or saved.get("accountId") != "live-account-unresolved" or any(permissions.get(key) is not False for key in ("live_allowed", "live_small_eligible", "live_eligible")):
            raise RuntimeError("Post-import inactive state could not be verified")
        result.update(operation="INACTIVE_CANDIDATE_REGISTERED", deploymentId=saved["deploymentId"], revision=saved["revision"], lifecycle=saved["lifecycle"], mode=saved["mode"], alreadyRegistered=imported.get("alreadyRegistered") is True, authorizationGranted=False)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-id", required=True); parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--strategy-hash", required=True); parser.add_argument("--instance-id", required=True)
    parser.add_argument("--portfolio-id", default=""); parser.add_argument("--portfolio-hash", default="")
    parser.add_argument("--root", type=Path); parser.add_argument("--apply", action="store_true")
    args=parser.parse_args()
    if bool(args.portfolio_id) != bool(args.portfolio_hash):
        parser.error("Exact Portfolio ID and hash must be supplied together")
    try:
        result=run(args)
    except (OSError,ValueError,RuntimeError,KeyError,TypeError) as error:
        print(json.dumps({"ok":False,"reason":str(error),"authorizationGranted":False,"ordersSubmitted":0},ensure_ascii=False));return 1
    print(json.dumps(result,ensure_ascii=False,indent=2));return 0


if __name__=="__main__":raise SystemExit(main())
