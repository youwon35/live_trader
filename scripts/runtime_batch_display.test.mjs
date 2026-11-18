import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import {spawnSync} from 'node:child_process';
import {runtimeBatchFromResponse,runtimeBatchReview,runtimeBatchReason} from '../src/runtimeBatchModel.js';
const require=createRequire(import.meta.url),{transformSync}=require('esbuild');
const root=fileURLToPath(new URL('../',import.meta.url));

function actualPrepare(blocked) {
  const code=`import importlib.util,json,pathlib,sys,tempfile
p=pathlib.Path(sys.argv[1]);s=importlib.util.spec_from_file_location('batch_ui_fixture',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
def resolve(selection):
 if sys.argv[2]=='blocked': raise ValueError('synthetic selection not connected')
 return {'scope':{'runtimeScopeId':'runtime-1','brokerId':'binance','capitalCurrency':'USDT','deploymentId':selection['deployment_id'],'profileId':selection['profile_id']},'manifestInputsHash':'fixture'}
def no_start(*args): raise AssertionError('preparation must not start')
with tempfile.TemporaryDirectory() as d:
 service=m.RuntimeBatchService(pathlib.Path(d),resolve=resolve,start=no_start,observe=lambda:{'runtimes':{}})
 print(json.dumps(service.prepare({'selections':[{'profile_id':'crypto','mode':'MONITOR','deployment_id':'deployment-1','strategy_id':'strategy-1','portfolio_id':'portfolio-1'}]})))`;
  const result=spawnSync(process.env.PYTHON || 'python',['-c',code,path.join(root,'live_trader/runtime_batch.py'),blocked?'blocked':'ready'],{encoding:'utf8',windowsHide:true});
  assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout);
}
const text=node=>node==null?'':typeof node!=='object'?String(node):(node.children||[]).map(text).join('');
const all=node=>node&&typeof node==='object'?[node,...(node.children||[]).flatMap(all)]:[];
function harness(prepare,read) {
  const cells=[];let index=0;
  const hooks={createElement:(type,props,...children)=>({type,props:props||{},children:children.flat(Infinity)}),
    useState(initial){const i=index++;if(!(i in cells))cells[i]=typeof initial==='function'?initial():initial;return[cells[i],v=>{cells[i]=typeof v==='function'?v(cells[i]):v;}];},
    useRef(initial){const i=index++;return cells[i]??=( {current:initial});},useEffect(){index++;}};
  const compiled=transformSync(fs.readFileSync(path.join(root,'src/RuntimeBatchLauncher.jsx'),'utf8'),{loader:'jsx',format:'cjs',jsx:'transform'}).code;
  const loaded={exports:{}};let starts=0;
  Function('require','module','exports',compiled)(id=>{
    if(id==='react')return {...hooks,default:hooks,__esModule:true};
    if(id==='lucide-react')return new Proxy({},{get:(_,k)=>k});
    if(id==='./api.js')return {prepareRuntimeBatch:prepare,getRuntimeBatchStatus:read};
    if(id==='./runtimeBatchModel.js')return {runtimeBatchFromResponse,runtimeBatchReview,runtimeBatchReason};
    if(id==='./liveDashboardModel.js')return {routeName:v=>v};
    if(id.endsWith('.css'))return {};throw Error('Unexpected dependency '+id);
  },loaded,loaded.exports);
  const render=()=>{index=0;return loaded.exports.default({options:[{id:'choice',profileId:'crypto',context:{id:'deployment-1',name:'검사용 전략',strategyId:'strategy-1',portfolioId:'portfolio-1',brokerId:'binance',symbol:'BTCUSDT',timeframe:'1h'}}],onStartMember:async()=>{starts++;return {};}});};
  const button=(tree,label)=>all(tree).find(n=>n.type==='button'&&text(n).includes(label));
  function choose(){const input=all(render()).find(n=>n.type==='input'&&n.props.type==='checkbox');input.props.onChange({target:{checked:true}});}
  return {render,button,choose,starts:()=>starts};
}

test('actual service all-blocked response never claims connection success or enables start',async()=>{
  const response=actualPrepare(true);assert.equal(response.ok,false);assert.equal(response.batch.members[0].status,'BLOCKED');
  const h=harness(async()=>response,async()=>response);h.choose();await h.button(h.render(),'구성 확인').props.onClick();
  const tree=h.render();assert.match(text(tree),/모든 경로가 차단되어 시작할 수 없습니다/);
  assert.doesNotMatch(text(tree),/구성 연결을 확인했습니다/);
  const start=h.button(tree,'함께 관찰');assert.equal(start.props.disabled,true);
  await start.props.onClick();assert.equal(h.starts(),0);
});
test('missing preparation batch is an error, not a retained ready result',async()=>{
  const h=harness(async()=>({ok:true}),async()=>({ok:true}));h.choose();await h.button(h.render(),'구성 확인').props.onClick();
  assert.match(text(h.render()),/같은 구성의 상태를 확인하지 못했습니다/);assert.equal(h.button(h.render(),'함께 관찰'),undefined);
});
test('missing or wrong-identity refresh locks old ready display until the same batch is observed',async()=>{
  const response=actualPrepare(false);let next={ok:true};
  const h=harness(async()=>response,async()=>next);h.choose();await h.button(h.render(),'구성 확인').props.onClick();
  assert.equal(h.button(h.render(),'함께 관찰').props.disabled,false);
  await h.button(h.render(),'실행 결과 조회').props.onClick();
  assert.equal(h.button(h.render(),'함께 관찰').props.disabled,true);assert.match(text(h.render()),/최근 실행 상태를 확인하지 못했습니다/);
  next={ok:true,batch:{...response.batch,batchId:'different-request'}};
  await h.button(h.render(),'실행 결과 조회').props.onClick();assert.equal(h.button(h.render(),'함께 관찰').props.disabled,true);
  next={ok:true,batch:response.batch};await h.button(h.render(),'실행 결과 조회').props.onClick();
  assert.equal(h.button(h.render(),'함께 관찰').props.disabled,false);assert.equal(h.starts(),0);
});
test('mixed batch and observed runtime facts are described independently',()=>{
  const batch=actualPrepare(false).batch;
  const mixed={...batch,members:[...batch.members,{memberId:'blocked',status:'BLOCKED'}]};
  assert.match(runtimeBatchReview(mixed).message,/1개/);assert.equal(runtimeBatchReview(mixed).canStart,true);
  assert.doesNotMatch(runtimeBatchReview(mixed).message,/선택한 2개 구성의 연결 정보를 확인했습니다/);
  assert.match(runtimeBatchReview({...mixed,members:[{memberId:'running',status:'RUNNING'},{memberId:'unknown',status:'UNKNOWN'}]}).message,/현재 1개 경로가 실행 중/);
});
test('a missing start outcome keeps the original once-only lock through a later PREPARED read',async()=>{
  const response=actualPrepare(false),h=harness(async()=>response,async()=>({ok:true,batch:response.batch}));
  h.choose();await h.button(h.render(),'구성 확인').props.onClick();await h.button(h.render(),'함께 관찰').props.onClick();
  assert.equal(h.starts(),1);assert.equal(h.button(h.render(),'함께 관찰').props.disabled,true);
  assert.equal(all(h.render()).filter(node=>node.props?.role==='alert').length,1,'one alert retains both failure and observation diagnostics');
  assert.equal(h.button(h.render(),'구성 확인').props.disabled,true);
  await h.button(h.render(),'실행 결과 조회').props.onClick();assert.equal(h.button(h.render(),'함께 관찰').props.disabled,true);assert.equal(h.starts(),1);
});
test('Toss diagnostics translate the actual prefixed basis error without changing unknown reasons',()=>{
  const code=transformSync(fs.readFileSync(path.join(root,'src/TossCashOperationPanel.jsx'),'utf8')+'\nexport {message};',{loader:'jsx',format:'cjs',jsx:'transform'}).code;
  const loaded={exports:{}};
  Function('require','module','exports',code)(id=>{
    if(id==='react')return {};if(id==='./api.js')return {request:()=>{throw Error('No real API');}};
    if(id.endsWith('.css'))return {};throw Error('Unexpected import '+id);
  },loaded,loaded.exports);
  assert.match(loaded.exports.message('INDICATOR_PRICE_BASIS_REVALIDATION_REQUIRED: WARMUP_NOT_EXACT_NEUTRAL_PAIR'),/근거가 준비될 때까지 운용을 시작하지 않습니다/);
  assert.match(loaded.exports.message('TOSS_EXIT_QUANTITY_CONTRACT_UNSUPPORTED'),/전체를 매도하는 방식만 지원/);
  assert.equal(loaded.exports.message('UNKNOWN_ORIGINAL_ERROR'),'UNKNOWN_ORIGINAL_ERROR');
  assert.match(runtimeBatchReason('BATCH_MEMBER_SELECTION_BLOCKED'),/운용 설정을 확인/);
});
