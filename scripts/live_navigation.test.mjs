import assert from 'node:assert/strict';
import test from 'node:test';
import { LIVE_WORKSPACE_ROUTE_IDS, liveNavigationRoot, liveNavigationRoute, liveSectionTabs } from '../src/liveNavigation.js';

test('every old stored or diagnostic route remains addressable', () => {
  const legacy = ['overview', 'gate', 'functional-test', 'risk', 'automation', 'accounts', 'orders', 'incidents', 'audit', 'settings'];
  assert.deepEqual([...LIVE_WORKSPACE_ROUTE_IDS].filter(id => id !== "monitor"), legacy);
  assert.equal(liveNavigationRoot("monitor"), "monitor");
  assert.equal(liveNavigationRoute("monitor"), "monitor");
  for (const route of legacy) assert.equal(liveNavigationRoute(route), route);
  assert.equal(liveNavigationRoute('unknown'), 'overview');
});

test('risk keeps its action tabs and both record routes share a single workspace', () => {
  assert.equal(liveNavigationRoot('risk'), 'automation');
  assert.equal(liveNavigationRoot('audit'), 'incidents');
  assert.deepEqual(liveSectionTabs('risk').map(({ id, label }) => [id, label]), [['automation', '실행·중지'], ['risk', '한도·안전장치'], ['functional-test', '주문 연결 시험']]);
  assert.deepEqual(liveSectionTabs('audit'), []);
  assert.deepEqual(liveSectionTabs('incidents'), []);
});

test('broker testing is reachable inside execution without becoming a strategy phase', () => {
  assert.equal(liveNavigationRoot('functional-test'), 'automation');
  assert.equal(liveNavigationRoute('functional-test'), 'functional-test');
  assert.ok(liveSectionTabs('automation').some(({ id }) => id === 'functional-test'));
  assert.deepEqual(liveSectionTabs('gate'), []);
});
