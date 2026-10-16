import test from "node:test";
import assert from "node:assert/strict";
import { createKisPreparationCoordinator, kisPreparationFailureMessage } from "../src/kisNativePreparation.js";

const deferred = () => { let resolve; let reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const begin = (id = "job-a") => ({ ok: true, preparationId: id, status: "PREPARING_PUBLIC", maximumPreparationSeconds: 660, runtimeStarted: false });
const status = (state = "READY", id = "job-a") => ({ ok: true, preparationId: id, status: state });
const turn = () => new Promise((resolve) => setImmediate(resolve));
function fixture(overrides = {}) {
  const calls = []; const progress = []; let tick = 0;
  const request = async (path, options) => {
    calls.push({ path, options });
    if (path.endsWith("-challenge")) return begin();
    if (path.includes("-cancel")) return status("CANCELLED");
    return status();
  };
  const coordinator = createKisPreparationCoordinator({ request, now: () => tick,
    sleep: async () => { tick += 1000; }, onProgress: (value) => progress.push(value), ...overrides });
  return { coordinator, calls, progress, advance: (value) => { tick += value; } };
}
const params = (confirmAndStart = async () => ({ ok: true }), scopeKey = "account-a") => ({ body: { profile_id: "stock", mode: "SMALL_LIVE" }, scopeKey, confirmAndStart });

test("bounded async begin/status precedes native confirmation and normal Start callback", async () => {
  const f = fixture(); let confirmed = 0;
  const result = await f.coordinator.run(params(async (challenge, current, starting) => {
    confirmed += 1; current(); starting(); assert.equal(challenge.status, "READY"); return { ok: true };
  }));
  assert.equal(result.ok, true); assert.equal(confirmed, 1);
  assert.match(f.calls[0].path, /kis-manual-start-challenge$/);
  assert.match(f.calls[1].path, /kis-native-prepare-status\?preparationId=job-a$/);
  assert.ok(f.calls.every((row) => row.options.timeoutMs <= 15000));
  assert.deepEqual(f.progress.map((row) => row.status), ["PREPARING_PUBLIC", "READY", "NATIVE_CONFIRMATION", "STARTING", "COMPLETE"]);
  assert.equal(f.progress.find((row) => row.status === "STARTING").canCancel, false);
});

test("one poll in flight and account-check progress does not imply permission", async () => {
  let concurrent = 0; let max = 0; let count = 0; let confirmed = 0;
  const f = fixture({ request: async (path) => {
    if (path.endsWith("-challenge")) return begin();
    concurrent += 1; max = Math.max(max, concurrent); await turn(); concurrent -= 1;
    return status(count++ === 0 ? "CHECKING_ACCOUNT" : "READY");
  } });
  await f.coordinator.run(params(async () => { confirmed += 1; return { ok: true }; }));
  assert.equal(max, 1); assert.equal(confirmed, 1);
  assert.ok(f.progress.some((row) => row.status === "CHECKING_ACCOUNT"));
});

test("same account duplicate is blocked while another account remains independent", async () => {
  const hold = deferred();
  const f = fixture({ request: async (path) => path.endsWith("-challenge") ? begin()
    : path.includes("-cancel") ? status("CANCELLED") : hold.promise });
  const first = f.coordinator.run(params()).catch((e) => e);
  await turn();
  await assert.rejects(f.coordinator.run(params()), (e) => e.code === "KIS_START_IN_PROGRESS");
  assert.equal(f.coordinator.activeCount(), 1);
  await f.coordinator.cancel("account-a");
  hold.resolve(status());
  assert.equal((await first).code, "KIS_PREPARATION_CANCELLED");
});

test("cancel during begin sends exact remote cancellation when its late id arrives", async () => {
  const hold = deferred(); const cancelled = []; let native = 0;
  const f = fixture({ request: async (path, options) => {
    if (path.endsWith("-challenge")) return hold.promise;
    cancelled.push(options.body.preparationId); return status("CANCELLED");
  } });
  const running = f.coordinator.run(params(async () => { native += 1; })).catch((e) => e);
  await turn(); await f.coordinator.cancel("account-a"); hold.resolve(begin());
  assert.equal((await running).code, "KIS_PREPARATION_CANCELLED");
  assert.equal(native, 0); assert.deepEqual(cancelled, ["job-a"]);
});

test("old READY response cannot confirm or overwrite a newer account job", async () => {
  const old = deferred(); const fresh = deferred(); const progress = []; let serial = 0; let confirmations = 0;
  const c = createKisPreparationCoordinator({ request: async (path) => {
    if (path.endsWith("-challenge")) return begin(++serial === 1 ? "old" : "new");
    if (path.includes("-cancel")) return status("CANCELLED");
    return path.endsWith("=old") ? old.promise : fresh.promise;
  }, onProgress: (value) => progress.push(value) });
  const a = c.run(params(async () => { confirmations += 1; })).catch((e) => e);
  await turn(); await c.cancel("account-a");
  const b = c.run(params(async () => { confirmations += 1; return { ok: true }; }));
  await turn(); const before = progress.length; old.resolve(status("READY", "old"));
  assert.equal((await a).code, "KIS_PREPARATION_CANCELLED");
  assert.equal(progress.length, before); assert.equal(confirmations, 0);
  fresh.resolve(status("READY", "new")); assert.equal((await b).ok, true); assert.equal(confirmations, 1);
});

test("settings invalidation during native confirmation blocks later normal Start", async () => {
  const hold = deferred(); let submitted = 0;
  const f = fixture();
  const running = f.coordinator.run(params(async (_, current) => { await hold.promise; current(); submitted += 1; })).catch((e) => e);
  await turn(); await f.coordinator.cancelAll("설정 변경"); hold.resolve();
  assert.equal((await running).code, "KIS_PREPARATION_CANCELLED"); assert.equal(submitted, 0);
});

test("660-second deadline cannot be refreshed by repeated pending status", async () => {
  let tick = 0; let submitted = 0;
  const f = fixture({ now: () => tick, sleep: async () => { tick += 660000; },
    request: async (path) => path.endsWith("-challenge") ? begin() : status("PREPARING_PUBLIC") });
  await assert.rejects(f.coordinator.run(params(async () => { submitted += 1; })), (e) => e.code === "KIS_PREPARATION_EXPIRED");
  assert.equal(submitted, 0);
});

test("READY received after deadline is rejected before native prompt", async () => {
  let tick = 0;
  const f = fixture({ now: () => tick, request: async (path) => {
    if (path.endsWith("-challenge")) return begin(); tick += 660001; return status();
  } });
  await assert.rejects(f.coordinator.run(params(() => assert.fail("native must not open"))), (e) => e.code === "KIS_PREPARATION_EXPIRED");
});

test("foreign status id, unknown status, invalid begin budget fail closed", async () => {
  for (const bad of [status("READY", "foreign"), status("RUNNING"), { ok: false }]) {
    const f = fixture({ request: async (path) => path.endsWith("-challenge") ? begin() : bad });
    await assert.rejects(f.coordinator.run(params(() => assert.fail("native must not open"))));
  }
  const f = fixture({ request: async () => ({ ...begin(), maximumPreparationSeconds: 661 }) });
  await assert.rejects(f.coordinator.run(params(() => assert.fail("native must not open"))));
});

test("status request timeout fails only its flow and cannot auto-retry or confirm", async () => {
  let requests = 0; let cancelled = 0;
  const f = fixture({ request: async (path) => {
    if (path.endsWith("-challenge")) return begin();
    if (path.includes("-cancel")) { cancelled += 1; return status("CANCELLED"); }
    requests += 1; throw Object.assign(new Error("timeout"), { code: "TIMEOUT" });
  } });
  await assert.rejects(f.coordinator.run(params(() => assert.fail("native must not open"))), (e) => e.code === "TIMEOUT" && /시작을 보류/.test(e.message));
  assert.equal(requests, 1); assert.equal(cancelled, 1); assert.equal(f.coordinator.activeCount(), 0);
});

test("failed cancel transport does not revive local cancelled intent", async () => {
  const hold = deferred();
  const f = fixture({ request: async (path) => {
    if (path.endsWith("-challenge")) return begin();
    if (path.includes("-cancel")) throw new Error("unavailable");
    return hold.promise;
  } });
  const promise = f.coordinator.run(params(() => assert.fail("native must not open"))).catch((e) => e);
  await turn(); await f.coordinator.cancel("account-a"); hold.resolve(status());
  assert.equal((await promise).code, "KIS_PREPARATION_CANCELLED");
});

test("two different accounts can prepare independently and cancelling one preserves the other", async () => {
  const a = deferred(); const b = deferred(); let confirmed = 0;
  const coordinator = createKisPreparationCoordinator({ request: async (path, options) => {
    if (path.endsWith("-challenge")) return begin(options.body.fixtureAccount);
    if (path.includes("-cancel")) return status("CANCELLED", options.body.preparationId);
    return path.endsWith("=a") ? a.promise : b.promise;
  } });
  const first = coordinator.run({ ...params(), body: { fixtureAccount: "a" } }).catch((e) => e);
  const second = coordinator.run({ ...params(async () => { confirmed += 1; return { ok: true }; }, "account-b"), body: { fixtureAccount: "b" } });
  await turn(); assert.equal(coordinator.activeCount(), 2);
  await coordinator.cancel("account-a"); a.resolve(status("READY", "a"));
  assert.equal((await first).code, "KIS_PREPARATION_CANCELLED"); assert.equal(coordinator.activeCount(), 1);
  b.resolve(status("READY", "b")); assert.equal((await second).ok, true); assert.equal(confirmed, 1);
});

test("current label HOLD and changed scope are plain Korean without audit codes or private hashes", async () => {
  const label = kisPreparationFailureMessage({ reasonCode: "KIS_LABEL_SCOPE_UNPROVEN", reason: "private-hash-123" });
  assert.equal(label, "국내 시세의 분봉 시각 검증이 완료되지 않아 시작을 보류했습니다.");
  assert.match(kisPreparationFailureMessage({ reasonCode: "KIS_NATIVE_CONTEXT_INVALID" }), /전략 또는 계좌의 검증 정보가 변경/);
  assert.doesNotMatch(kisPreparationFailureMessage({ reason: "UNRECOGNIZED_AUDIT_private-hash-123" }), /AUDIT|hash|123/);
  const f = fixture({ request: async (path) => path.endsWith("-challenge") ? begin()
    : { ...status("FAILED"), reasonCode: "KIS_LABEL_SCOPE_UNPROVEN" } });
  await assert.rejects(f.coordinator.run(params(() => assert.fail("No native prompt"))), (e) => e.message === label);
  assert.equal(f.progress.at(-1).detail, label);
});

test("logical normal Start rejection cannot hide the panel as a completed successful start", async () => {
  const f = fixture();
  await assert.rejects(f.coordinator.run(params(async () => ({ ok: false, reasonCode: "KIS_NATIVE_CONTEXT_INVALID" }))), /전략 또는 계좌의 검증 정보가 변경/);
  assert.equal(f.progress.at(-1).status, "FAILED");
  assert.equal(f.progress.some((row) => row.status === "COMPLETE"), false);
});
