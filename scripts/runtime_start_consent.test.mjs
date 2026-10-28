import test from 'node:test';
import assert from 'node:assert/strict';
import { startContinuousRuntime, registerSafetyConfirmationPresenter } from '../src/api.js';

const beforeWindow=globalThis.window, beforeFetch=globalThis.fetch;
const selection={profile_id:'crypto',mode:'SMALL_LIVE',portfolio_id:'portfolio-one',deployment_id:'deployment-one',strategy_id:'strategy-one'};
const response=value=>({ok:true,headers:{get:()=> 'application/json'},json:async()=>value});
let cleanup;
function setup(decision=true){
 const calls=[];let shown=0;
 globalThis.window=globalThis;
 globalThis.pywebview={api:{functional_http_session:async()=>({ok:true,available:true,csrfHeader:'X-LiveTrader-CSRF',csrfToken:'offline-native-session-token-'.repeat(3)})}};
 cleanup=registerSafetyConfirmationPresenter(async challenge=>{shown++;return {confirmed:decision,typedPhrase:challenge.expectedPhrase};});
 globalThis.fetch=async(path,options)=>{
  calls.push({path,body:options.body?JSON.parse(options.body):null});
  return response(path==='/api/safety-confirmation/challenge'?{ok:true,challengeId:'one-shot',token:'offline-token',expectedPhrase:'LIVE 1234',expiresAt:new Date(Date.now()+60000).toISOString(),displayContext:{provider:'BINANCE'}}:{ok:true});
 };
 return {calls,shown:()=>shown};
}
test.afterEach(()=>{cleanup?.();globalThis.window=beforeWindow;globalThis.fetch=beforeFetch;delete globalThis.pywebview;});
const start=options=>startContinuousRuntime('crypto','SMALL_LIVE','portfolio-one','deployment-one','strategy-one',options);

test('ordinary live start binds exact selection and submits once only after the final confirmation',async()=>{
 const f=setup();await start();
 assert.equal(f.shown(),1);assert.deepEqual(f.calls.map(x=>x.path),['/api/safety-confirmation/challenge','/api/runtime/start']);
 assert.deepEqual(f.calls[0].body,{action:'RUNTIME_START',context:selection});
 assert.deepEqual(f.calls[1].body,{...selection,safety_confirmation:{challengeId:'one-shot',token:'offline-token',typedPhrase:'LIVE 1234'}});
});
test('cancelled final confirmation sends no runtime or batch start',async()=>{
 const f=setup(false);await assert.rejects(start());assert.equal(f.shown(),1);
 assert.deepEqual(f.calls.map(x=>x.path),['/api/safety-confirmation/challenge']);
});
test('batch member receives the same exact one-time confirmation',async()=>{
 const f=setup();const batchContext={batchId:'batch-one',scopeHash:'a'.repeat(64),memberId:'member-one'};
 await start({batchContext,marketGapPreparationId:'gap-one'});
 assert.equal(f.shown(),1);assert.equal(f.calls.at(-1).path,'/api/runtime/batches/start-member');
 assert.deepEqual(f.calls.at(-1).body,{...batchContext,confirmed:true,marketGapPreparationId:'gap-one',safety_confirmation:{challengeId:'one-shot',token:'offline-token',typedPhrase:'LIVE 1234'}});
});
test('observation mode has no order consent or arming request',async()=>{
 const f=setup();await startContinuousRuntime('crypto','MONITOR','portfolio-one','deployment-one','strategy-one');
 assert.equal(f.shown(),0);assert.deepEqual(f.calls.map(x=>x.path),['/api/runtime/start']);
 assert.equal(f.calls[0].body.safety_confirmation,undefined);
});
