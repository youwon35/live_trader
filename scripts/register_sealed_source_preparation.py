"""Register information-only preparation identities; preserves all scientific and operational sources."""
from pathlib import Path
import argparse,importlib,json,os,sys,types
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT.parents[1]/"packages/trading_runtime"))
namespace="_sealed_preparation_registration"
package=types.ModuleType(namespace);package.__path__=[str(ROOT/"live_trader")];sys.modules[namespace]=package
service=importlib.import_module(namespace+".sealed_source_preparation")

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--recipe",type=Path,required=True);parser.add_argument("--publication",type=Path,required=True);parser.add_argument("--approvals",type=Path,required=True);parser.add_argument("--output-base",type=Path,default=Path(os.getenv("LOCALAPPDATA") or Path.home()/"AppData/Local")/"live_trader/preparation-sources")
    args=parser.parse_args();result=service.register_sources(args.recipe,args.publication,args.approvals,args.output_base);print(json.dumps(result,ensure_ascii=False));return 0
if __name__=="__main__":raise SystemExit(main())
