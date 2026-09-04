import assert from 'node:assert/strict';
import test from 'node:test';
import { LIVE_WORKSPACE_ROUTE_IDS, liveNavigationRoot, liveNavigationRoute, liveSectionTabs } from '../src/liveNavigation.js';

test('every old stored or diagnostic route remains addressable', () => {
  const legacy = ['overview', 'gate', 'functional-test', 'risk', 'automation', 'accounts', 'orders', 'incidents', 'audit', 'settings'];
  assert.deepEqual([...LIVE_WORKSPACE_ROUTE_IDS], legacy);
  for (const route of legacy) assert.equal(liveNavigationRoute(route), route);
  assert.equal(liveNavigationRoute('unknown'), 'overview');
});

test('risk and logs keep their route and highlight the correct merged section', () => {
  assert.equal(liveNavigationRoot('risk'), 'automation');
  assert.equal(liveNavigationRoot('audit'), 'incidents');
  assert.deepEqual(liveSectionTabs('risk').map(({ id, label }) => [id, label]), [['automation', '실행·중지'], ['risk', '한도·안전장치']]);
  assert.deepEqual(liveSectionTabs('audit').map(({ id, label }) => [id, label]), [['incidents', '운영 이력'], ['audit', '상세 로그']]);
});

test('broker testing is neither a strategy phase nor an automatic runtime subtab', () => {
  assert.equal(liveNavigationRoot('functional-test'), 'functional-test');
  assert.deepEqual(liveSectionTabs('functional-test'), []);
  assert.ok(liveSectionTabs('automation').every(({ id }) => id !== 'functional-test'));
});
