import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { transformSync } from "esbuild";
import { createSnapshotReadCoordinator } from "../src/snapshotRefreshState.js";

const require = createRequire(import.meta.url);
const source = readFileSync(new URL("../src/DirectStrategyPanel.jsx", import.meta.url), "utf8");
const compiled = transformSync(source, { loader: "jsx", jsx: "automatic", format: "cjs" }).code;

const candidate = { rootKey: "root-a", candidateId: "portfolio:alpha", name: "검사용 포트폴리오",
  brokerId: "kis", members: [{}], identityHash: "identity-a", registryHash: "before" };
const matchingAccount = { profileId: "account-kis", brokerId: "kis", alias: "시험 계좌", maskedAccount: "••21" };
const catalog = (row = candidate) => ({ ok: true, candidates: [row],
  accounts: [matchingAccount, { profileId: "account-toss", brokerId: "toss", alias: "다른 계좌" }] });

function elements(node, kind) {
  if (!node || typeof node !== "object") return [];
  if (Array.isArray(node)) return node.flatMap(child => elements(child, kind));
  return [...(node.type === kind ? [node] : []), ...elements(node.props?.children, kind)];
}

function harness({ api, onConnected }) {
  const state = [], effects = [];
  let offset = 0, tree;
  const hooks = {
    useState(initial) {
      const index = offset++;
      if (!(index in state)) state[index] = initial;
      return [state[index], (value) => { state[index] = value; }];
    },
    useRef(initial) {
      const index = offset++;
      return state[index] ??= { current: initial };
    },
    useEffect(effect) {
      const index = offset++;
      if (index in state) return;
      state[index] = true;
      effects.push(effect);
    },
  };
  const module = { exports: {} };
  Function("require", "module", "exports", compiled)(
    (name) => name === "react" ? hooks : name === "./api" ? api : require(name),
    module, module.exports,
  );
  function render() {
    offset = 0;
    tree = module.exports.default({ onConnected });
    return renderToStaticMarkup(tree);
  }
  function choose(index, value) {
    render();
    elements(tree, "select")[index].props.onChange({ target: { value } });
  }
  function button(index) {
    render();
    return elements(tree, "button")[index];
  }
  render();
  return { render, choose, button,
    mountTwice() { const effect = effects[0]; effect(); effect(); return button(0).props.onClick(); },
    setCash(value) { render(); elements(tree, "input")[0].props.onChange({ target: { value } }); },
  };
}

test("the direct lists load once and selecting source, account or budget submits nothing", async () => {
  const reads = [], writes = [];
  const view = harness({ api: {
    getDirectStrategies: async () => { reads.push("source"); return catalog(); },
    registerDirectStrategy: async (body) => { writes.push(body); throw Error("unexpected metadata write"); },
  }});
  await view.mountTwice();
  assert.deepEqual(reads, ["source"]);
  assert.match(view.render(), /검사용 포트폴리오/);
  view.choose(0, "root-a:portfolio:alpha");
  assert.match(view.render(), /시험 계좌/);
  assert.doesNotMatch(view.render(), /다른 계좌/);
  view.choose(1, "account-toss");
  assert.equal(view.button(1).props.disabled, true);
  view.choose(1, "account-kis");
  view.setCash("101");
  assert.equal(view.button(1).props.disabled, true);
  assert.deepEqual(writes, []);
});

test("one connection submits exact metadata, selects the exact returned ID and reads today's catalog hash", async () => {
  const calls = [];
  let current = catalog();
  const view = harness({
    onConnected: async (id) => { calls.push(["select", id]); return true; },
    api: {
      getDirectStrategies: async () => { calls.push(["read"]); return current; },
      registerDirectStrategy: async (body) => {
        calls.push(["connect", body]);
        current = catalog({ ...candidate, registryHash: "after" });
        return { ok: true, deploymentId: "direct:returned", ordersSubmitted: 0 };
      },
    },
  });
  await view.mountTwice();
  view.choose(0, "root-a:portfolio:alpha");
  view.choose(1, "account-kis");
  view.setCash("11.5");
  const connect = view.button(1).props.onClick;
  assert.equal(connect(), connect(), "rapid clicks share the single save");
  await connect();
  assert.deepEqual(calls, [["read"], ["connect", {
    rootKey: "root-a", candidateId: "portfolio:alpha", identityHash: "identity-a",
    registryHash: "before", accountProfileId: "account-kis", capitalPercent: 11.5,
  }], ["select", "direct:returned"], ["read"]]);
  assert.match(view.render(), /전략과 계좌를 연결했습니다/);
});

test("a successful save is preserved when list readback is unavailable", async () => {
  let reads = 0, writes = 0;
  const view = harness({
    onConnected: async () => false,
    api: {
      getDirectStrategies: async () => ++reads === 1 ? catalog() : { ok: false, reason: "저장소 연결 확인" },
      registerDirectStrategy: async () => { writes++; return { ok: true, deploymentId: "direct:saved" }; },
    },
  });
  await view.mountTwice();
  view.choose(0, "root-a:portfolio:alpha");
  view.choose(1, "account-kis");
  await view.button(1).props.onClick();
  assert.match(view.render(), /전략과 계좌를 연결했습니다\. 운용 전략 목록을 다시 읽어 선택하세요/);
  assert.match(view.render(), /role="alert"[^>]*>저장소 연결 확인/);
  assert.equal(view.button(1).props.disabled, true);
  assert.equal(writes, 1);
});

test("post-registration readback waits for an in-progress account response before starting a fresh snapshot", async () => {
  const coordinator = createSnapshotReadCoordinator();
  let stopAccountRead;
  const account = coordinator.runAccount(() => new Promise(resolve => { stopAccountRead = resolve; }));
  const order = [];
  const check = coordinator.waitForPendingReads().then(() => coordinator.runSnapshot(() => order.push("new snapshot")));
  await Promise.resolve();
  assert.deepEqual(order, []);
  await Promise.resolve();
  stopAccountRead();
  await Promise.all([account, check]);
  assert.deepEqual(order, ["new snapshot"]);
});

