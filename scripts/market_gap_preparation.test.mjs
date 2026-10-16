import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createMarketGapCoordinator } from '../src/marketGapPreparation.js';
import { createKisPreparationCoordinator, sanitizePublicProgress, kisPreparationFailureMessage } from '../src/kisNativePreparation.js';
const turn = () => new Promise((resolve) => setImmediate(resolve));
const hold = () => { let resolve; const promise = new Promise((yes) => { resolve = yes; }); return { promise, resolve }; };
const observation = (status = 'WARNING') => ({ status, expiresAtUtc: new Date(Date.now() + 60000).toISOString(),
  authorityGranted: false, qualificationGranted: false, runtimeStarted: false, brokerSubmissionPerformed: false,
  members: [{ symbol: 'KRW-SOL', openingLimitSeconds: 30, status: 'WARNING' }] });
const reply = (id = 'a', status = 'WARNING') => ({ ok: true, preparationId: id, status, result: observation(status) });
function fixture(overrides = {}) {
  const views = [], cancelled = [], starts = [];
  const service = createMarketGapCoordinator({ begin: async () => reply(), status: async () => reply(),
    cancelRemote: async (id) => cancelled.push(id), onView: (view) => views.push(view), sleep: async () => turn(), ...overrides });
  const run = (scopeKey = 'upbit') => service.run({ scopeKey, label: scopeKey, body: { deployment_id: scopeKey }, continueStart: async (id) => { starts.push(id); return { ok: true }; } });
  return { service, run, views, cancelled, starts };
}
test('recent gaps need explicit user continue before the original Start; warning issues no authority', async () => {
  const f = fixture(); const pending = f.run(); await turn();
  assert.equal(f.views.at(-1).phase, 'REVIEW'); assert.deepEqual(f.starts, []);
  assert.equal(f.service.accept('upbit'), true); await pending;
  assert.deepEqual(f.starts, ['a']); assert.equal(f.views.at(-1).phase, 'COMPLETE');
});
test('duplicate same account is blocked; a second account diagnostic stays independent', async () => {
  const f = fixture(); const first = f.run(); await turn();
  await assert.rejects(f.run(), (error) => error.code === 'MARKET_GAP_IN_PROGRESS');
  const second = f.run('binance'); await turn(); f.service.accept('binance'); await second;
  assert.equal(f.service.activeCount(), 1); f.service.accept('upbit'); await first;
});
test('STOP/settings cancellation during begin cancels its late id and never starts', async () => {
  const deferred = hold(), f = fixture({ begin: () => deferred.promise });
  const pending = f.run().catch((error) => error); await turn(); await f.service.cancelAll();
  deferred.resolve(reply('late')); const error = await pending;
  assert.equal(error.code, 'MARKET_GAP_CANCELLED'); assert.deepEqual(f.cancelled, ['late']); assert.deepEqual(f.starts, []);
});
test('late old poll cannot overwrite the new same-account flow', async () => {
  const old = hold(); let attempts = 0;
  const f = fixture({ begin: async () => ++attempts === 1 ? { ok: true, preparationId: 'old', status: 'PREPARING' } : reply('new'), status: () => old.promise });
  const first = f.run().catch((error) => error); await turn(); await turn(); await f.service.cancel('upbit');
  const fresh = f.run(); await turn(); const count = f.views.length;
  old.resolve(reply('old')); await first; assert.equal(f.views.length, count);
  f.service.accept('upbit'); await fresh; assert.deepEqual(f.starts, ['new']);
});
test('Source changed status cancels user intent and cannot continue', async () => {
  const f = fixture({ begin: async () => ({ ok: true, preparationId: 'a', status: 'STALE' }) });
  const error = await f.run().catch((value) => value); assert.equal(error.code, 'MARKET_GAP_CANCELLED');
  assert.equal(f.service.accept('upbit'), false); assert.deepEqual(f.starts, []);
});
test('public observation failure stays unknown and explicit continue uses no fake result id', async () => {
  const f = fixture({ begin: async () => { throw Error('private remote literal'); } });
  const pending = f.run(); await turn(); assert.equal(f.views.at(-1).phase, 'REVIEW'); assert.equal(f.views.at(-1).result, null);
  assert.doesNotMatch(JSON.stringify(f.views), /private remote/); f.service.accept('upbit'); await pending; assert.deepEqual(f.starts, ['']);
});
test('expired observation cannot continue the original Start', async () => {
  let time = Date.now(); const f = fixture({ now: () => time }); const pending = f.run().catch((error) => error);
  await turn(); time += 61000; f.service.accept('upbit'); assert.equal((await pending).code, 'MARKET_GAP_EXPIRED'); assert.deepEqual(f.starts, []);
});
test('one status poll remains in flight and cancel is responsive while it waits', async () => {
  const deferred = hold(); let inFlight = 0, max = 0;
  const f = fixture({ begin: async () => ({ ok: true, preparationId: 'a', status: 'PREPARING' }), status: async () => {
    inFlight++; max = Math.max(inFlight, max); await deferred.promise; inFlight--; return reply();
  } });
  const pending = f.run().catch((error) => error); await turn(); await turn();
  assert.equal(inFlight, 1); await f.service.cancel('upbit'); deferred.resolve(); await pending;
  assert.equal(max, 1); assert.deepEqual(f.starts, []);
});
test('malformed positive-authority diagnostic is discarded rather than displayed', async () => {
  const f = fixture({ begin: async () => ({ ...reply(), result: { ...observation(), authorityGranted: true } }) });
  const pending = f.run(); await turn(); assert.equal(f.views.at(-1).result, null); f.service.accept('upbit'); await pending; assert.deepEqual(f.starts, ['']);
});
test('actual Live API adds trusted CSRF and carries optional fresh note only into original Start', async () => {
  const paths = []; const saved = { window: globalThis.window, fetch: globalThis.fetch };
  globalThis.window = { setTimeout, clearTimeout, pywebview: { api: { functional_http_session: async () => ({ ok: true, available: true, csrfHeader: 'X-LiveTrader-CSRF', csrfToken: 'x'.repeat(64) }) } } };
  globalThis.fetch = async (path, options) => {
    paths.push({ path, options }); const value = path.endsWith('/begin') ? reply('fresh') : { ok: true };
    return { ok: true, headers: { get: () => 'application/json' }, json: async () => value };
  };
  try {
    const api = await import(`../src/api.js?gap-${Date.now()}`);
    api.registerMarketGapPresenter((view) => { if (view.phase === 'REVIEW') api.continueMarketGapPreparation(view.scopeKey); });
    await api.prepareMarketGapsForStart({ profile_id: 'crypto', mode: 'SMALL_LIVE', portfolio_id: 'p', deployment_id: 'd', strategy_id: 's' }, {
      accountKey: 'a', label: '코인', continueStart: (id) => api.startContinuousRuntime('crypto', 'SMALL_LIVE', 'p', 'd', 's', { marketGapPreparationId: id }),
    });
    for (const row of paths.filter((value) => /market-gaps\/(begin|cancel)$/.test(value.path))) assert.equal(row.options.headers['X-LiveTrader-CSRF'], 'x'.repeat(64));
    assert.equal(JSON.parse(paths.find((row) => row.path === '/api/runtime/start').options.body).marketGapPreparationId, 'fresh');
    assert.equal(paths.filter((row) => row.path === '/api/runtime/start').length, 1);
  } finally { globalThis.window = saved.window; globalThis.fetch = saved.fetch; }
});
test('KIS long window is accepted only for typed explicit tick selection; ordinary REST cap stays660', async () => {
  const options = { method: 'PUBLIC_H0STCNT0', cutoff: '2026-10-19T06:00:00Z' };
  const calls = []; const request = async (path) => { calls.push(path); return path.endsWith('challenge')
    ? { ok: true, preparationId: 'tick', status: 'PREPARING_PUBLIC', maximumPreparationSeconds: 16200, runtimeStarted: false }
    : { ok: true, preparationId: 'tick', status: 'READY' }; };
  const c = createKisPreparationCoordinator({ request }); let start = 0;
  await c.run({ scopeKey: 'kr', body: { initializationOptions: options }, confirmAndStart: async () => { start++; return { ok: true }; } });
  assert.equal(start, 1);
  await assert.rejects(c.run({ scopeKey: 'kr', body: {}, confirmAndStart: async () => assert.fail() }), (error) => error.code === 'KIS_PREPARATION_INVALID');
  const before = calls.length;
  for (const bad of [{ ...options, receipt: {} }, { ...options, cutoff: '2026-10-19T06:01:00Z' }, { ...options, cutoff: '2026-10-19T06:00:00' }, { method: 'other', cutoff: null }]) {
    await assert.rejects(c.run({ scopeKey: 'kr', body: { initializationOptions: bad } }), (error) => error.code === 'KIS_PREPARATION_OPTIONS_INVALID');
  }
  assert.equal(calls.length, before);
});
test('public tick progress is projected narrowly; HOLD reason is plain Korean', () => {
  assert.equal(sanitizePublicProgress({ phase: 'PRIVATE', frameCount: 1, elapsedSeconds: 2, cutoff: 'x' }), null);
  assert.equal(sanitizePublicProgress({ phase: 'CAPTURING_NATIVE_TICKS', frameCount: -1, elapsedSeconds: 2, cutoff: '2026-10-19T06:00:00Z' }), null);
  assert.match(kisPreparationFailureMessage({ reasonCode: 'KIS_NATIVE_TICK_WINDOW_UNAVAILABLE' }), /필요한 길이.*시간이 부족/);
});
test('Paper and Live share the same no-authority coordinator and explicit review component', () => {
  for (const name of ['marketGapPreparation.js', 'MarketGapPreparationPanel.jsx']) {
    assert.equal(readFileSync(new URL(`../src/${name}`, import.meta.url), 'utf8'), readFileSync(new URL(`../../paper_trader/src/${name}`, import.meta.url), 'utf8'));
  }
});
