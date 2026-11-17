import test from 'node:test';
import assert from 'node:assert/strict';
import { kisPreparationFailureMessage, kisSessionStatusMessage } from '../src/kisNativePreparation.js';

test('scope closure and mixed deployments explain the actual required action', () => {
  assert.match(kisPreparationFailureMessage({reasonCode:'LIVE_NATIVE_SESSION_WINDOW_CLOSED_REQUIRE_DRAINING'}),/선택 거래시간 종료/);
  assert.match(kisPreparationFailureMessage({reasonCode:'LIVE_NATIVE_SESSION_DIFFERENT_PROFILES_REQUIRE_SEPARATE_DEPLOYMENTS'}),/별도 배포/);
  assert.match(kisSessionStatusMessage('RuntimeError: KIS_PUBLIC_STREAM_ALREADY_OWNED'),/기존 연결/);
});

test('unknown runtime diagnostics stay intact at the existing display fallback', () => {
  const original='independent original diagnostic';
  assert.equal(kisSessionStatusMessage(original)||original,original);
});
