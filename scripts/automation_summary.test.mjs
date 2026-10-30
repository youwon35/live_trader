import test from 'node:test';
import assert from 'node:assert/strict';
import {historyReadinessLabel} from '../src/historyReadiness.js';
import {normalizeShortLiveSnapshot, shortLiveReadyForConsent} from '../src/shortLiveTestModel.js';
import {restoreOperatingProfile} from '../src/operatingProfile.js';

const strategy = {route:'UPBIT_KRW_SPOT',symbol:'KRW-BTC',timeframe:'1h',decisionCount:0};
const observations = {UPBIT_KRW_SPOT:[{streams:[{symbol:'KRW-BTC',timeframe:'1h',
  history:{observedBars:12},firstEligibleCloseAt:'2026-10-30T16:00:00Z',initialPartialExcluded:true}]}]};
test('restored selections require every current original pin and unique exact candidates',()=>{
  const row={selectionKey:'test',artifactFileSha256:'a'.repeat(64),instanceFileSha256:'b'.repeat(64)};
  const profile={schemaVersion:'operating-profile-v1',durationHours:6,selections:[row]};
  assert.deepEqual(restoreOperatingProfile(profile,[row]).selection,['test']);
  assert.equal(restoreOperatingProfile(profile,[{...row,instanceFileSha256:'c'.repeat(64)}]),null);
  assert.equal(restoreOperatingProfile(profile,[row,row]),null);
  assert.equal(restoreOperatingProfile({...profile,selections:[row,row]},[row]),null);
  assert.equal(restoreOperatingProfile({...profile,selections:[{selectionKey:'test'}]},[row]),null);
});
test('partial initial hour is history and later missing completion is visible',()=>{
  assert.match(historyReadinessLabel(observations,strategy,Date.parse('2026-10-30T15:01:00Z')),/과거 12봉 준비.*첫 평가 가능.*시작 시 진행 중이던 봉 제외/);
  assert.match(historyReadinessLabel(observations,strategy,Date.parse('2026-10-30T16:00:16Z')),/예정 시각 경과.*수신 확인 필요/);
  assert.match(historyReadinessLabel(observations,{...strategy,decisionCount:1}),/새 완성봉마다 평가/);
  assert.equal(historyReadinessLabel(observations,{...strategy,symbol:'SOLUSDT'}),'');
});
test('saved preferences and successful history never enable orders',()=>{
  const value=normalizeShortLiveSnapshot({savedProfile:{schemaVersion:'operating-profile-v1',
    durationHours:6,automaticStartAllowed:false,selections:[]},
    prepared:{historyPreparation:{schemaVersion:'history-preparation-v1',ready:true,streams:[]},readyToStart:false}});
  assert.equal(shortLiveReadyForConsent(value,{fresh:true}),false);
  assert.equal(value.savedProfile.durationHours,6);
});
test('completion report appears only after trusted terminal status',()=>{
  const report={schemaVersion:'short-live-report-v1',retirementVerified:true,orderAttempts:0};
  assert.equal(normalizeShortLiveSnapshot({runtime:{status:'RUNNING',terminalVerified:true,completionReport:report}}).runtime.completionReport,null);
  assert.equal(normalizeShortLiveSnapshot({runtime:{status:'TERMINATED',terminalVerified:false,completionReport:report}}).runtime.completionReport,null);
  assert.deepEqual(normalizeShortLiveSnapshot({runtime:{status:'TERMINATED',terminalVerified:true,completionReport:report}}).runtime.completionReport,report);
});
