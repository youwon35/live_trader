import test from "node:test";
import assert from "node:assert/strict";
import {createVisibleStatusPoll} from "../src/visibleStatusPoll.js";
import {shortLiveRunning} from "../src/shortLiveTestModel.js";

const flush = () => new Promise(resolve => setImmediate(resolve));
function fixture(options = {}) {
  let reads = 0, invalidations = 0, sequence = 0;
  const callbacks = new Map(), listeners = new Set();
  const visibility = {visibilityState: "visible",
    addEventListener: (_, fn) => listeners.add(fn), removeEventListener: (_, fn) => listeners.delete(fn)};
  const timers = {setInterval(fn) {callbacks.set(++sequence, fn); return sequence;},
    clearInterval: id => callbacks.delete(id)};
  const poll = createVisibleStatusPoll({refresh: async () => {reads++;}, visibility, timers,
    invalidate: () => invalidations++, ...options});
  return {poll, callbacks, listeners, get reads() {return reads;}, get invalidations() {return invalidations;},
    hide(value) {visibility.visibilityState = value ? "hidden" : "visible"; for (const fn of listeners) fn();},
    tick() {for (const fn of callbacks.values()) fn();}};
}

test("idle tab navigation pauses reads and catches up once without unmounting", async () => {
  const f = fixture(); await flush(); assert.equal(f.reads, 1);
  f.poll.update({activeView:false, observationRequired:false}); f.tick(); await flush();
  assert.equal(f.reads,1); assert.equal(f.callbacks.size,0);
  f.poll.update({activeView:true, observationRequired:false}); await flush();
  assert.equal(f.reads,2); assert.equal(f.callbacks.size,1);
  f.poll.dispose(); assert.equal(f.callbacks.size,0); assert.equal(f.listeners.size,0);
});
test("background idle window pauses and foreground restores one current read", async () => {
  const f=fixture(); await flush(); f.hide(true); f.tick(); await flush();
  assert.equal(f.reads,1); f.hide(false); await flush(); assert.equal(f.reads,2);
  f.poll.dispose();
});
test("active, unknown, stopping and drained workers keep their terminal observer hidden", async () => {
  for (const status of ["RUNNING","UNKNOWN","STOPPING","DRAINED","TERMINATED"]) {
    const required=shortLiveRunning({runtime:{status,terminalVerified:false}});
    assert.equal(required,true);
    const f=fixture({activeView:false,observationRequired:required}); await flush();
    f.hide(true); f.tick(); await flush(); assert.equal(f.reads,2,status);
    f.poll.update({activeView:false,observationRequired:false});
    assert.equal(f.callbacks.size,0,status); f.poll.dispose();
  }
  assert.equal(shortLiveRunning({runtime:{status:"TERMINATED",terminalVerified:true,naturalExitObserved:true}}),false);
});
test("overlapping focus and ticks never duplicate an in-flight status read", async () => {
  let release, calls=0;
  const f=fixture({refresh:()=>{calls++;return new Promise(resolve=>{release=resolve;});}});
  await flush(); f.tick(); f.hide(true); f.hide(false); f.tick(); await flush();
  assert.equal(calls,1); release(); await flush(); f.tick(); await flush(); assert.equal(calls,2);
  f.poll.dispose(); release(); await flush();
});
test("unknown mutation or transport state continues observing without requiring an open tab", async () => {
  const f=fixture({activeView:false}); await flush(); assert.equal(f.reads,0);
  f.poll.update({activeView:false,observationRequired:true}); await flush(); assert.equal(f.reads,1);
  f.hide(true); f.tick(); await flush(); assert.equal(f.reads,2); f.poll.dispose();
});
