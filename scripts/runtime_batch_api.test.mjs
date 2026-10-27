import test from 'node:test';
import assert from 'node:assert/strict';
import {prepareRuntimeBatch,getRuntimeBatchStatus,startContinuousRuntime} from '../src/api.js';
const oldWindow=globalThis.window,oldFetch=globalThis.fetch;
const csrf='offline-only-native-session-token-'.repeat(3);
const batchContext={batchId:'batch-test',scopeHash:'a'.repeat(64),memberId:'member-1'};
const selection={profile_id:'stock',mode:'SMALL_LIVE',portfolio_id:'portfolio',deployment_id:'deployment',strategy_id:'strategy'};
function setup(native=true) {
 globalThis.window=globalThis;const calls=[];let confirmations=0;
 globalThis.pywebview={api:{functional_http_session:async()=>({ok:true,available:true,csrfHeader:'X-LiveTrader-CSRF',csrfToken:csrf}),request_kis_operational_start_confirmation:async()=>{confirmations++;return native?{ok:true,attestation:{test:'native'}}:{ok:false};}}};
 globalThis.fetch=async(path,options)=>{
  const body=options.body?JSON.parse(options.body):undefined;calls.push({path,body,method:options.method,headers:options.headers});
  let response={ok:true,batch:{...batchContext,members:[]}};
  if(path.endsWith('kis-manual-start-challenge'))response={ok:true,preparationId:'test-job',status:'PREPARING_PUBLIC',maximumPreparationSeconds:660,runtimeStarted:false};
  if(path.includes('kis-native-prepare-status?'))response={ok:true,preparationId:'test-job',status:'READY',nativeNonce:'test-nonce',expiresAt:new Date(Date.now()+45000).toISOString(),context:{schemaVersion:'kis-scoped-manual-start-context/v1',profileId:'stock',mode:'SMALL_LIVE',portfolioId:'portfolio',deploymentId:'deployment',strategyId:'strategy'}};
  return {ok:true,headers:{get:()=> 'application/json'},json:async()=>response};
 };return {calls,confirmations:()=>confirmations};
}
test.after(()=>{globalThis.window=oldWindow;globalThis.fetch=oldFetch;delete globalThis.pywebview;});
test('group preparation and status cannot submit a normal or member start',async()=>{
 const f=setup();await prepareRuntimeBatch([selection]);await getRuntimeBatchStatus(batchContext.batchId);
 assert.deepEqual(f.calls.map(c=>c.path),['/api/runtime/batches/prepare','/api/runtime/batches/status?batchId=batch-test']);
 assert.equal(f.calls[0].headers['X-LiveTrader-CSRF'],csrf);assert.equal(f.confirmations(),0);
});
test('explicit crypto member start uses bound group identity and preserves fresh gap note',async()=>{
 const f=setup();await startContinuousRuntime('crypto','MONITOR','portfolio','deployment','strategy',{batchContext,marketGapPreparationId:'fresh-note'});
 assert.equal(f.calls.length,1);assert.equal(f.calls[0].path,'/api/runtime/batches/start-member');
 assert.deepEqual(f.calls[0].body,{...batchContext,confirmed:true,marketGapPreparationId:'fresh-note'});assert.equal(f.calls[0].headers['X-LiveTrader-CSRF'],csrf);
});
test('KIS group preserves exact normal preparation and requires native confirmation before one member start',async()=>{
 const f=setup();await startContinuousRuntime('stock','SMALL_LIVE','portfolio','deployment','strategy',{batchContext});
 assert.deepEqual(f.calls[0].body,selection);assert.equal(f.confirmations(),1);
 const starts=f.calls.filter(c=>c.path==='/api/runtime/batches/start-member');assert.equal(starts.length,1);
 assert.deepEqual(starts[0].body,{...batchContext,confirmed:true,safety_confirmation:{nativeAttestation:{test:'native'}}});
 assert.equal(f.calls.filter(c=>c.path==='/api/runtime/start').length,0);
});
test('cancelled KIS native confirmation submits no group or normal start',async()=>{
 const f=setup(false);await assert.rejects(startContinuousRuntime('stock','SMALL_LIVE','portfolio','deployment','strategy',{batchContext}));
 assert.equal(f.calls.filter(c=>['/api/runtime/start','/api/runtime/batches/start-member'].includes(c.path)).length,0);
});

test('submitted batch network failure survives as an unknown outcome',async()=>{
 setup();globalThis.fetch=async()=>{throw new Error('fixture network lost');};
 await assert.rejects(startContinuousRuntime('crypto','MONITOR','portfolio','deployment','strategy',{batchContext}),
   error=>error.runtimeBatchSubmissionAttempted===true);
});
