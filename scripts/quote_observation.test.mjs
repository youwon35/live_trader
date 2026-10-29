import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizeUpbitQuoteObservation,failureTiming} from '../src/quoteObservation.js';
import {normalizeShortLiveFailureDiagnostics} from '../src/shortLiveTestModel.js';
import {projectRealtimeMonitor} from '../src/realtimeMonitorModel.js';
const now=Date.parse('2026-10-30T00:00:00Z');
const iso=ms=>new Date(ms).toISOString();
const observation=()=>({schemaVersion:'upbit-quote-observation-v1',symbol:'KRW-BTC',purpose:'INVENTORY_VALUATION',boundary:'QUOTE_READ_VALIDATION',observationOnly:true,status:'BLOCKED',issue:'SOURCE_TIMESTAMP_STALE',sourceTimestampMs:now-11000,lastTradeTimestampMs:now-60000,sourceObservedAt:iso(now-11000),lastTradeObservedAt:iso(now-60000),requestStartedAt:iso(now-100),receivedAt:iso(now-50),checkedAt:iso(now),sourceAgeSeconds:11,lastTradeAgeSeconds:60,receiveAgeSeconds:0.05,readElapsedSeconds:0.05,maxSourceAgeSeconds:10,maxReceiveAgeSeconds:10,maxFutureSeconds:2});
test('safe quote metadata survives only on the exact Upbit ticker path and excludes credentials or price',()=>{
 const q={...observation(),apiKey:'must-not-leak',price:123};
 const result=normalizeShortLiveFailureDiagnostics([{provider:'upbit',path:'/v1/ticker',elapsedMs:33.448,status:'AVAILABLE',quoteObservation:q}]);
 assert.equal(result[0].quoteObservation.sourceAgeSeconds,11);assert.ok(!JSON.stringify(result).includes('must-not-leak'));assert.ok(!Object.hasOwn(result[0].quoteObservation,'price'));
 assert.equal(normalizeShortLiveFailureDiagnostics([{provider:'upbit',path:'/v1/accounts',elapsedMs:49,status:'AVAILABLE',quoteObservation:q}])[0].quoteObservation,null);
});
test('last-trade age is informational; stale snapshot, forged ages/status and invalid times never turn fresh in the display',()=>{
 const stale=observation();const fresh={...stale,sourceTimestampMs:now-1000,sourceObservedAt:iso(now-1000),sourceAgeSeconds:1,status:'FRESH',issue:'NONE'};
 assert.equal(normalizeUpbitQuoteObservation(fresh).lastTradeAgeSeconds,60);
 for(const modified of [{...stale,status:'FRESH',issue:'NONE'},{...stale,sourceAgeSeconds:0},{...stale,sourceTimestampMs:null},{...stale,sourceObservedAt:'2026-02-30T00:00:00Z'},{...stale,checkedAt:'bad'}, {...stale,readElapsedSeconds:NaN}]) assert.equal(normalizeUpbitQuoteObservation(modified),null);
 const edge={...stale,checkedAt:'2026-10-30T00:00:00.000001+00:00',sourceTimestampMs:now-10000,sourceObservedAt:iso(now-10000),sourceAgeSeconds:10.000001,lastTradeAgeSeconds:60.000001,receiveAgeSeconds:0.050001};
 assert.equal(normalizeUpbitQuoteObservation(edge).status,'BLOCKED');
});
test('real failure and natural retirement events determine early-stop and cleanup duration; missing data stays unknown',()=>{
 const run={startedAt:iso(now),recentActivity:[{kind:'FIRST_FAILURE',observedAt:iso(now+18417)}]};
 const result=failureTiming(run,iso(now+49340),now+60000);assert.equal(result.firstFailureSeconds,18.417);assert.equal(result.cleanupSeconds,30.923);
 assert.equal(failureTiming({...run,recentActivity:[]},iso(now+49340),now+60000).firstFailureSeconds,null);
 assert.equal(failureTiming(run,iso(now+1000),now+60000).cleanupSeconds,null);
});
test('HTTP AVAILABLE context remains visible when source validation fails and terminal data cannot become running',()=>{
 const snap={observedAt:iso(now),runtime:{status:'TERMINATED',sessionId:'synthetic',firstFailure:'UPBIT_KRW_SPOT:SOURCE_QUOTE_OBSERVATION_STALE',startedAt:iso(now-60000),terminatedAt:iso(now-1000),normalCleanupConfirmed:true,naturalExitObserved:true,firstFailureDiagnostics:[{provider:'upbit',path:'/v1/accounts',elapsedMs:49.079,status:'AVAILABLE'},{provider:'upbit',path:'/v1/ticker',elapsedMs:33.448,status:'AVAILABLE',quoteObservation:observation()}],brokers:[],strategies:[],recentActivity:[{kind:'FIRST_FAILURE',observedAt:iso(now-40000)}]}};
 const view=projectRealtimeMonitor(snap,{fresh:true,receivedAt:now,now});assert.equal(view.failureDiagnostics.length,2);assert.match(view.failureDiagnostics[1].resultLabel,/시세 시각 검증 차단/);assert.match(view.failureDiagnostics[0].elapsedLabel,/ms/);assert.equal(view.resultHasIssue,true);assert.equal(view.timing.firstFailureSeconds,20);
});

test('direct actual firstFailureAt survives publication and contradictory event time stays unknown',()=>{
 const startedAt=iso(now-60000),firstFailureAt=iso(now-35000),endedAt=iso(now-10000);
 const direct=failureTiming({startedAt,firstFailureAt,recentActivity:[]},endedAt,now);
 assert.equal(direct.firstFailureAt,firstFailureAt);assert.equal(direct.firstFailureSeconds,25);assert.equal(direct.cleanupSeconds,25);
 assert.equal(failureTiming({startedAt,firstFailureAt,recentActivity:[{kind:'FIRST_FAILURE',observedAt:iso(now-34997)}]},endedAt,now).firstFailureSeconds,25);
 for(const run of [
   {startedAt,firstFailureAt:'broken',recentActivity:[{kind:'FIRST_FAILURE',observedAt:firstFailureAt}]},
   {startedAt,firstFailureAt,recentActivity:[{kind:'FIRST_FAILURE',observedAt:iso(now-36000)}]},
   {startedAt,firstFailureAt:iso(now+6000),recentActivity:[]},
   {startedAt,firstFailureAt:iso(now-61000),recentActivity:[]},
   {startedAt,firstFailureAt,recentActivity:[{kind:'FIRST_FAILURE',observedAt:firstFailureAt},{kind:'FIRST_FAILURE',observedAt:firstFailureAt}]},
 ]) assert.equal(failureTiming(run,endedAt,now).firstFailureAt,null);
});