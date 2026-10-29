import test from 'node:test';import assert from 'node:assert/strict';import {readFileSync} from 'node:fs';
import {normalizeFxObservation} from '../src/fxObservation.js';
import {normalizeShortLiveFailureDiagnostics,normalizeShortLiveSnapshot} from '../src/shortLiveTestModel.js';
import {projectRealtimeMonitor} from '../src/realtimeMonitorModel.js';
const vectors=JSON.parse(readFileSync(new URL('./fx-clock-vectors.json',import.meta.url),'utf8'));
test('all pure producer clock cases retain exact diagnostic fields across the frontend boundary',()=>{
 for(const value of vectors){assert.deepEqual(normalizeFxObservation(value),value,JSON.stringify(value));}
});
test('FX metadata is scoped to the official book and strips account/key/price/raw fields',()=>{
 const value=vectors.find(row=>row.status==='FRESH');assert.ok(value);
 const result=normalizeShortLiveFailureDiagnostics([{provider:'upbit',path:'/v1/orderbook',status:'AVAILABLE',elapsedMs:35.977,fxObservation:{...value,key:'secret-test',account:'private-test',askPrice:1341,headers:{private:true}}}]);
 assert.deepEqual(result[0].fxObservation,value);assert.doesNotMatch(JSON.stringify(result),/secret-test|private-test|askPrice|headers/);
 assert.equal(normalizeShortLiveFailureDiagnostics([{provider:'upbit',path:'/v1/accounts',status:'AVAILABLE',elapsedMs:10,fxObservation:value}])[0].fxObservation,null);
});
test('forged scope, times, ages, status or clock proof cannot become a fresh diagnostic',()=>{
 const value=vectors.find(row=>row.status==='FRESH');
 for(const patch of [{symbol:'KRW-BTC'},{endpoint:'/v1/ticker'},{sourceAgeUpperSeconds:-1},{sourceTimestampMs:value.sourceTimestampMs+1},{sourceObservedAt:'2026-02-30T00:00:00Z'},{dateHeader:'invalid'},{ageHeader:'7'},{roundTripNanoseconds:-1},{observationOnly:false},{orderAuthorityGranted:true},{status:'BLOCKED'},{clockIssue:'NATIVE_FX_OBSERVATION_STALE_OR_FUTURE'}])assert.equal(normalizeFxObservation({...value,...patch}),null,JSON.stringify(patch));
});
test('stale at actual consumption remains a timing failure even if HTTP succeeded quickly',()=>{
 const value=vectors.find(row=>row.clockIssue==='NATIVE_FX_OBSERVATION_STALE_OR_FUTURE');assert.ok(value);
 const current=Date.parse(value.checkedAt),startedAt=new Date(current-30000).toISOString();
 const raw={observedAt:value.checkedAt,runtime:{sessionId:'fixture-only',status:'TERMINATED',terminalVerified:true,naturalExitObserved:true,startedAt,terminatedAt:value.checkedAt,firstFailure:'BINANCE_SPOT:NATIVE_FX_OBSERVATION_STALE_OR_FUTURE',firstFailureAt:new Date(current-5000).toISOString(),firstFailureDiagnostics:[{provider:'upbit',path:'/v1/orderbook',status:'BLOCKED',failureKind:'FX_VALIDATION',elapsedMs:35.977,fxObservation:value}]}};
 const result=projectRealtimeMonitor(normalizeShortLiveSnapshot(raw),{now:current,receivedAt:current,fresh:true});
 assert.equal(result.timing.firstFailureSeconds,25);assert.equal(result.failureDiagnostics[0].elapsedLabel,'36.0ms');assert.match(result.failureDiagnostics[0].resultLabel,/환산 관측 검증 차단/);assert.equal(result.failureDiagnostics[0].fxObservation.status,'BLOCKED');
});
