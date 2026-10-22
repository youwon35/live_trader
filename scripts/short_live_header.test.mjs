// Actual App.jsx helper bundled without rendering or calling application APIs.
import assert from "node:assert/strict";
import test from "node:test";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
const root=fileURLToPath(new URL("../",import.meta.url));
const require=createRequire(join(root,"node_modules","fixture-resolver.cjs"));
const {build}=require("esbuild");
const result=await build({stdin:{contents:"export {shortLiveHeaderState} from './src/App.jsx';",resolveDir:root,loader:"js"},
  bundle:true,write:false,format:"esm",platform:"node",treeShaking:true,resolveExtensions:[".js",".jsx",".json"],loader:{".css":"empty"},logLevel:"silent"});
const {shortLiveHeaderState}=await import(`data:text/javascript;base64,${Buffer.from(result.outputFiles[0].text).toString("base64")}`);
const active=(changes={})=>({api_connected:true,mode:"MONITOR",new_entries_blocked:true,order_queue:{active:0},
  short_live_test_status:{known:true,active:true,status:"RUNNING",ownerAlive:true,newEntriesBlocked:false,
    unknownOrderCount:0,pendingOrderCount:0,globalStopRequested:false,...changes}});
test("active separate functional Live overrides ordinary MONITOR no-order header",()=>{
  const view=shortLiveHeaderState(active());
  assert.match(view.label,/실거래 기능시험 실행 중/); assert.doesNotMatch(view.label,/주문 없음|MONITOR/);
  assert.equal(view.entriesLabel,"시험 신규 진입 허용");
});
test("stop request and unknown fills are not displayed as an actual clean retirement",()=>{
  const view=shortLiveHeaderState(active({globalStopRequested:true}));
  assert.match(view.label,/중지 요청됨/); assert.match(view.entriesLabel,/종료 대조 중/); assert.equal(view.entriesBlocked,true);
  assert.match(shortLiveHeaderState(active({unknownOrderCount:1})).label,/주문 상태 미확정/);
  assert.match(shortLiveHeaderState(active({status:"DRAINED",newEntriesBlocked:true})).label,/종료 확인 중/);
});
test("pending functional orders and disconnection cannot show orderless or entry-enabled state",()=>{
  let view=shortLiveHeaderState(active({pendingOrderCount:1}));
  assert.equal(view.queueLabel,"미결 주문 일반 0 / 시험 1"); assert.equal(view.entriesBlocked,true);
  view=shortLiveHeaderState({...active(),api_connected:false});
  assert.match(view.label,/상태 미확인/); assert.doesNotMatch(view.entriesLabel,/허용/);
});
test("disabled, unverified and retired cached views preserve the normal mode header",()=>{
  for(const status of [null,{known:false,active:true},{known:true,active:false}]) {
    assert.equal(shortLiveHeaderState({short_live_test_status:status}),null);
  }
});
