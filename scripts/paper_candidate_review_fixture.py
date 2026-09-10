"""Create synthetic production-valid candidate responses in a temporary store."""
import json, sys, tempfile, os
from pathlib import Path
root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root/"tests"))
import test_paper_candidate_import as case
import test_monitor_trial as monitor
out=root/"output/sep11-functional-audit/candidate-ui"
out.mkdir(parents=True,exist_ok=True)
with tempfile.TemporaryDirectory() as folder:
 store=Path(folder)
 case.fixture.fixture(store)
 trial_root=Path(os.environ["LIVE_MONITOR_REVIEW_ROOT"]).resolve() if os.environ.get("LIVE_MONITOR_REVIEW_ROOT") else store/"trial-root"
 if not os.environ.get("LIVE_MONITOR_REVIEW_ROOT"):monitor.fixture(trial_root)
 before=case.fixture.service.list_paper_candidates(roots=[store,trial_root])
 request=before["candidates"][0]["importRequest"]
 result=case.service.import_paper_candidate(request,roots=[store])
 after=case.fixture.service.list_paper_candidates(roots=[store,trial_root])
 trial_request=before["monitorTrials"][0]["request"]
 trial_result=monitor.service.run_monitor_trial(trial_request,roots=[trial_root],report_root=store/"monitor-results")
 (out/"responses.json").write_text(json.dumps({"before":before,"request":request,"result":result,"after":after,"trialRequest":trial_request,"trialResult":trial_result},ensure_ascii=False,indent=2),encoding="utf-8")
