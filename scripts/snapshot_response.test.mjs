import assert from 'node:assert/strict';
import test from 'node:test';
import { getSnapshot } from '../src/api.js';

const valid = () => ({
  generated_at: '2026-10-28 01:30:00', mode: 'MONITOR',
  dry_run: true, kill_switch: false, new_entries_blocked: true, operator_confirmed: false,
  summary: { status: 'blocked' }, readiness: [], brokers: [], strategies: [],
  accounts: [], orders: [], positions: [],
});

test('snapshot response boundary preserves valid state and rejects missing or mistyped safety flags', async t => {
  const previousWindow = globalThis.window, previousFetch = globalThis.fetch;
  globalThis.window = globalThis;
  let response;
  globalThis.fetch = async (path, options) => {
    assert.equal(path, '/api/snapshot'); assert.equal(options.method, 'GET');
    return new Response(JSON.stringify(response), {status:200,headers:{'Content-Type':'application/json'}});
  };
  try {
    await t.test('known safety flags and empty account arrays are valid; optional fields are not required', async () => {
      response=valid(); assert.deepEqual(await getSnapshot(), response);
      response={...valid(),dry_run:false,kill_switch:true,new_entries_blocked:false,operator_confirmed:true};
      assert.deepEqual(await getSnapshot(), response);
    });
    const malformed=[{},[],true,'online',{ok:false,reason:'not ready'}, {...valid(),summary:null}];
    for (const field of ['dry_run','kill_switch','new_entries_blocked','operator_confirmed']) {
      const missing=valid(); delete missing[field]; malformed.push(missing);
      for(const value of ['false',0,null]) malformed.push({...valid(),[field]:value});
    }
    for(const field of ['readiness','brokers','strategies','accounts','orders','positions']) { for (const value of [{}, [null], [false], ['invalid']]) malformed.push({...valid(),[field]:value}); }
    malformed.push({...valid(),ok:false}, {...valid(),mode:''}, {...valid(),generated_at:''});
    for (const [index, value] of malformed.entries()) {
      await t.test(`incomplete or invalid snapshot ${index+1} fails closed`, async () => {
        response=value;
        await assert.rejects(getSnapshot(), error=>error.code==='INVALID_RESPONSE');
      });
    }
  } finally { globalThis.window=previousWindow; globalThis.fetch=previousFetch; }
});
