import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import Module, { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import test, { after } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { transformSync } from "esbuild";
import { setChecklistItem } from "../src/api.js";

// Exercise the actual JSX handlers and API request serializer entirely in memory.
// No Live process, server, account, native bridge or network can be reached.
const sourcePath = fileURLToPath(new URL("../src/OperationalChecklistPanel.jsx", import.meta.url));
const transformed = transformSync(readFileSync(sourcePath, "utf8"), {
  loader: "jsx", jsx: "automatic", format: "cjs", target: "es2022",
}).code;
const require = createRequire(import.meta.url);
const oldWindow = globalThis.window;
const oldFetch = globalThis.fetch;
let calls = [];
let respond;
globalThis.window = {
  setTimeout, clearTimeout,
  get pywebview() { throw new Error("Native bridge is forbidden in checklist UI test"); },
};
globalThis.fetch = async (path, options) => {
  assert.equal(path, "/api/checklist");
  assert.equal(options.method, "POST");
  assert.equal(options.credentials, "same-origin");
  const body = JSON.parse(options.body);
  assert.ok(["risk_limits_reviewed", "notification_channel_reviewed", "operator_takeover_ready"].includes(body.name));
  assert.equal(typeof body.value, "boolean");
  calls.push(body);
  return respond(body);
};
after(() => { globalThis.window = oldWindow; globalThis.fetch = oldFetch; });

const item = (key, source, checked = false, required = true) => ({
  key, label: key, detail: "실제 절차를 확인하세요.", source, checked, required,
});
const initialRows = () => [
  item("api_keys_reviewed", "failed"),
  item("risk_limits_reviewed", "pending"),
  item("position_reconcile_reviewed", "automatic", true),
  item("notification_channel_reviewed", "pending", false, false),
  item("operator_takeover_ready", "pending"),
];
const response = (body) => ({
  ok: true, status: 200,
  headers: { get: () => "application/json" },
  json: async () => body,
});
function elements(node, type) {
  if (!node || typeof node !== "object") return [];
  if (Array.isArray(node)) return node.flatMap((child) => elements(child, type));
  return [...(node.type === type ? [node] : []), ...elements(node.props?.children, type)];
}
function harness({ rows = initialRows(), connected = true, acceptSnapshot = true } = {}) {
  calls = [];
  const slots = [];
  let cursor = 0;
  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [slots[index], (value) => {
        slots[index] = typeof value === "function" ? value(slots[index]) : value;
      }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { current: initial };
      return slots[index];
    },
  };
  const module = new Module(sourcePath);
  module.filename = sourcePath;
  module.paths = Module._nodeModulePaths(fileURLToPath(new URL("../src", import.meta.url)));
  module.require = (specifier) => specifier === "react" ? hooks : require(specifier);
  module._compile(transformed, sourcePath);
  let tree;
  let props = {
    items: rows, apiConnected: connected,
    async onChange(key, checked) {
      const result = await setChecklistItem(key, checked);
      if (acceptSnapshot && result.ok === true && Array.isArray(result.snapshot?.checklist)) {
        props = { ...props, items: result.snapshot.checklist };
      }
      return result;
    },
  };
  const view = {
    render(updates = {}) {
      props = { ...props, ...updates };
      cursor = 0;
      tree = module.exports.default(props);
      return renderToStaticMarkup(tree);
    },
    inputs() { return elements(tree, "input"); },
    checkbox(key) { return view.inputs().find((input) => input.props["aria-label"] === key); },
    change(key, checked) { return view.checkbox(key).props.onChange({ currentTarget: { checked } }); },
  };
  view.render();
  respond = async ({ name, value }) => response({
    ok: true,
    snapshot: { checklist: rows.map((row) => row.key === name
      ? { ...row, checked: value, source: value ? "manual" : "pending" } : row) },
  });
  return view;
}

test("mount is passive; automatic and unknown checks cannot dispatch even through handlers", async () => {
  const view = harness({ rows: [...initialRows(), item("future_manual_key", "pending")] });
  assert.equal(calls.length, 0);
  for (const key of ["api_keys_reviewed", "position_reconcile_reviewed", "future_manual_key"]) {
    assert.equal(view.checkbox(key).props.disabled, true);
    await view.change(key, true);
  }
  assert.equal(calls.length, 0);
  assert.match(view.render(), /자동 확인 대기/);
  assert.match(view.render(), /읽기 전용/);
});

test("one explicit manual check posts the exact value and renders returned snapshot", async () => {
  const view = harness();
  assert.equal(view.checkbox("risk_limits_reviewed").props.checked, false);
  await view.change("risk_limits_reviewed", true);
  const html = view.render();
  assert.deepEqual(calls, [{ name: "risk_limits_reviewed", value: true }]);
  assert.equal(view.checkbox("risk_limits_reviewed").props.checked, true);
  assert.match(html, /확인을 저장했습니다/);
  assert.match(html, /새 Preflight/);
});

test("pending save blocks duplicate and cross-item changes without optimistic completion", async () => {
  const view = harness();
  let release;
  respond = () => new Promise((resolve) => { release = resolve; });
  const pending = view.change("risk_limits_reviewed", true);
  for (let index = 0; index < 4; index += 1) await Promise.resolve();
  await view.change("risk_limits_reviewed", true);
  await view.change("operator_takeover_ready", true);
  view.render();
  assert.equal(calls.length, 1);
  assert.ok(view.inputs().every((input) => input.props.disabled));
  assert.equal(view.checkbox("risk_limits_reviewed").props.checked, false);
  release(response({ ok: true, snapshot: { checklist: initialRows().map((row) => (
    row.key === "risk_limits_reviewed" ? { ...row, checked: true, source: "manual" } : row
  )) } }));
  await pending;
  view.render();
  assert.equal(view.checkbox("risk_limits_reviewed").props.checked, true);
  assert.equal(view.checkbox("operator_takeover_ready").props.disabled, false);
});

test("operator can revoke a manual acknowledgement with the saved false value", async () => {
  const view = harness({ rows: initialRows().map((row) => row.key === "risk_limits_reviewed"
    ? { ...row, checked: true, source: "manual" } : row) });
  await view.change("risk_limits_reviewed", false);
  assert.match(view.render(), /해제를 저장했습니다/);
  assert.equal(view.checkbox("risk_limits_reviewed").props.checked, false);
  assert.deepEqual(calls, [{ name: "risk_limits_reviewed", value: false }]);
});

test("logical failure preserves unchecked state and exposes server reason", async () => {
  const view = harness();
  respond = async () => response({ ok: false, reason: "합성 저장 오류" });
  await view.change("risk_limits_reviewed", true);
  assert.match(view.render(), /role="alert"/);
  assert.match(view.render(), /합성 저장 오류/);
  assert.equal(view.checkbox("risk_limits_reviewed").props.checked, false);
  assert.equal(view.checkbox("risk_limits_reviewed").props.disabled, false);
  assert.equal(calls.length, 1);
});

test("network failure shows an error without automatic retry or optimistic acknowledgement", async () => {
  const view = harness();
  respond = async () => { throw new TypeError("synthetic offline"); };
  await view.change("risk_limits_reviewed", true);
  assert.match(view.render(), /API 서버에 연결할 수 없습니다/);
  assert.equal(view.checkbox("risk_limits_reviewed").props.checked, false);
  assert.equal(calls.length, 1);
});

test("disconnected snapshot never presents saved checks as currently verified or dispatches", async () => {
  const view = harness({ connected: false });
  assert.ok(view.inputs().every((input) => input.props.disabled && !input.props.checked));
  await view.change("risk_limits_reviewed", true);
  assert.equal(calls.length, 0);
  assert.match(view.render(), /확인 불가/);
});

test("missing or mismatched success snapshot cannot claim a saved acknowledgement", async () => {
  for (const result of [{ ok: true }, { ok: true, snapshot: { checklist: initialRows() } }]) {
    const view = harness();
    respond = async () => response(result);
    await view.change("risk_limits_reviewed", true);
    const html = view.render();
    assert.match(html, /저장 결과를 확인하지 못했습니다/);
    assert.doesNotMatch(html, /확인을 저장했습니다/);
    assert.equal(view.checkbox("risk_limits_reviewed").props.checked, false);
  }
});

test("unexpected source blocks manual-looking keys and malformed rows remain renderable", async () => {
  const view = harness({ rows: [null, { key: "invalid" }, item("risk_limits_reviewed", "automatic")] });
  assert.equal(view.inputs().length, 1);
  assert.equal(view.checkbox("risk_limits_reviewed").props.disabled, true);
  await view.change("risk_limits_reviewed", true);
  assert.equal(calls.length, 0);
});
