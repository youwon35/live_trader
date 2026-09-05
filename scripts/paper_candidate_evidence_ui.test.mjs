import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import Module, { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import test, { after } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { transformSync } from "esbuild";
import * as api from "../src/api.js";

// No Live App/server is imported or started. Render the actual JSX in memory,
// retain hook state between renders, and exercise its handlers through real api.js.
// fetch is replaced before any handler can run; the native bridge always throws.
const sourcePath = fileURLToPath(new URL("../src/PaperCandidateEvidencePanel.jsx", import.meta.url));
const source = readFileSync(sourcePath, "utf8");
const transformed = transformSync(source, { loader: "jsx", jsx: "automatic", format: "cjs", target: "es2022" }).code;
const require = createRequire(import.meta.url);
const oldWindow = globalThis.window;
const oldFetch = globalThis.fetch;
let calls = [];
let responseForFetch;
let nativeBridgeReads = 0;
globalThis.window = {
  setTimeout, clearTimeout,
  get pywebview() { nativeBridgeReads += 1; throw new Error("Native bridge forbidden in read-only UI test"); },
};
globalThis.fetch = async (url, options) => {
  assert.equal(url, "/api/paper-candidates");
  assert.equal(options.method, "GET");
  assert.equal(options.body, undefined);
  calls.push({ url, method: options.method, credentials: options.credentials });
  return responseForFetch(url, options);
};
after(() => { globalThis.window = oldWindow; globalThis.fetch = oldFetch; });
const result = (candidates = [], extra = {}) => ({
  schemaVersion: "live-paper-evidence-inbox-v1", ok: true, readOnly: true,
  canImport: false, authorizationGranted: false, candidates, errors: [], ...extra,
});
const row = (extra = {}) => ({
  evidenceId: "paper-evidence-a", strategyId: "strategy-a", strategyName: "검증된 전략 A",
  status: "VERIFIED_READ_ONLY", canImport: false, authorizationGranted: false,
  detail: "현재 저장본과 봉인 검증 근거가 일치합니다. 확인 전용입니다.", instanceHash: "a".repeat(64),
  identity: { evidenceId: "paper-evidence-a", evidenceHash: "b".repeat(64), strategyInstanceId: "instance-a", sessionId: "session-a" },
  deployment: { deploymentId: "deployment-a", mode: "SMALL_LIVE", lifecycle: "before-live-small", definitionHash: "c".repeat(64), revision: 2 }, ...extra,
});
const response = (payload, ok = true, status = 200) => ({ ok, status, headers: { get: () => "application/json" }, json: async () => payload });
function useResponse(payload) {
  calls = [];
  responseForFetch = async () => response(payload);
}
function elements(node, type) {
  if (!node || typeof node !== "object") return [];
  if (Array.isArray(node)) return node.flatMap(child => elements(child, type));
  return [...(node.type === type ? [node] : []), ...elements(node.props?.children, type)];
}
function harness(props = {}) {
  const slots = [], effects = [];
  let cursor = 0;
  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = typeof initial === "function" ? initial() : initial;
      return [slots[index], value => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { current: initial };
      return slots[index];
    },
    useEffect(effect) { effects.push(effect); },
  };
  const module = new Module(sourcePath);
  module.filename = sourcePath;
  module.paths = Module._nodeModulePaths(fileURLToPath(new URL("../src", import.meta.url)));
  module.require = specifier => specifier === "react" ? hooks : specifier === "./api" ? api : require(specifier);
  module._compile(transformed, sourcePath);
  let tree;
  const view = {
    render(nextProps = props) {
      props = nextProps;
      cursor = 0;
      tree = module.exports.default(props);
      while (effects.length) effects.shift()();
      return renderToStaticMarkup(tree);
    },
    refresh() { return elements(tree, "button")[0].props.onClick(); },
    buttons() { return elements(tree, "button"); },
  };
  view.render();
  return view;
}

test("mount, disclosure render, and strategy changes never fetch and expose only refresh", () => {
  useResponse(result());
  const view = harness({ strategyId: "strategy-a" });
  const html = view.render({ strategyId: "strategy-b" });
  assert.equal(calls.length, 0);
  assert.equal(view.buttons().length, 1);
  assert.equal(view.buttons()[0].props.children, "Paper 검증 근거 새로고침");
  assert.doesNotMatch(html, /<(?:input|select|form)\b/);
  assert.equal(nativeBridgeReads, 0);
});

test("explicit refresh performs exactly one GET and empty response is visible", async () => {
  useResponse(result());
  const view = harness();
  await view.refresh();
  assert.match(view.render(), /확인할 근거가 없습니다/);
  assert.deepEqual(calls, [{ url: "/api/paper-candidates", method: "GET", credentials: "same-origin" }]);
  view.render();
  assert.equal(calls.length, 1);
  assert.equal(nativeBridgeReads, 0);
});

test("pending refresh immediately prevents duplicate GET and unlocks after completion", async () => {
  useResponse(result());
  let release;
  responseForFetch = () => new Promise(resolve => { release = resolve; });
  const view = harness();
  const pending = view.refresh();
  await Promise.resolve();
  await Promise.resolve();
  await view.refresh();
  view.render();
  assert.equal(calls.length, 1);
  assert.equal(view.buttons()[0].props.disabled, true);
  release(response(result()));
  await pending;
  view.render();
  assert.equal(view.buttons()[0].props.disabled, false);
  responseForFetch = async () => response(result());
  await view.refresh();
  assert.equal(calls.length, 2, "only a second explicit action can perform the second GET");
});

test("verified and blocked candidates display evidence without adoption or execution controls", async () => {
  useResponse(result([row(), { evidenceId: "blocked-evidence", status: "BLOCKED", canImport: false, detail: "현재 Instance hash가 다릅니다." }]));
  const view = harness({ strategyId: "strategy-a" });
  await view.refresh();
  const html = view.render();
  assert.match(html, /검증된 전략 A/);
  assert.match(html, /현재 Instance hash가 다릅니다/);
  assert.match(html, /deployment-a/);
  assert.match(html, /instance-a/);
  assert.match(html, /bbbbbbbbbbbbbbbb/);
  assert.equal(view.buttons().length, 1);
  assert.equal(calls.length, 1);
});

test("strategy changes filter verified rows without fetching and retain unscoped blocked reasons", async () => {
  useResponse(result([row(), row({ strategyId: "strategy-b", strategyName: "검증된 전략 B" }), { evidenceId: "blocked-evidence", status: "BLOCKED", canImport: false, detail: "공유 근거 손상" }]));
  const view = harness({ strategyId: "strategy-a" });
  await view.refresh();
  assert.doesNotMatch(view.render(), /검증된 전략 B/);
  const html = view.render({ strategyId: "strategy-b" });
  assert.doesNotMatch(html, /검증된 전략 A/);
  assert.match(html, /검증된 전략 B/);
  assert.match(html, /공유 근거 손상/);
  assert.equal(calls.length, 1);
});

const malformed = [
  ["null response", null],
  ["object candidates", result({})],
  ["null candidate", result([null])],
  ["string candidate", result(["invalid"])],
  ["object errors", result([], { errors: {} })],
  ["object error item", result([], { errors: [{}] })],
  ["object detail", result([row({ detail: {} })])],
  ["object identity value", result([row({ identity: { evidenceHash: {} } })])],
  ["object deployment value", result([row({ deployment: { deploymentId: {}, mode: "SMALL_LIVE", lifecycle: "before-live-small", definitionHash: "hash", revision: 1 } })])],
  ["object next step", result([], { requiredNextStep: {} })],
  ["unexpected schema", result([], { schemaVersion: "unknown" })],
  ["importable response", result([], { canImport: true })],
  ["importable candidate", result([row({ canImport: true })])],
];
for (const [name, payload] of malformed) {
  test(`malformed ${name} clears old rows and remains renderable with an error`, async () => {
    useResponse(result([row()]));
    const view = harness();
    await view.refresh();
    assert.match(view.render(), /검증된 전략 A/);
    responseForFetch = async () => response(payload);
    await view.refresh();
    const html = view.render();
    assert.match(html, /role="status"/);
    assert.doesNotMatch(html, /검증된 전략 A|확인할 근거가 없습니다/);
    assert.equal(view.buttons().length, 1);
    assert.equal(view.buttons()[0].props.disabled, false);
    assert.equal(calls.length, 2);
  });
}

test("failed refresh removes previously verified rows and reports network error without retry", async () => {
  useResponse(result([row()]));
  const view = harness();
  await view.refresh();
  responseForFetch = async () => { throw new TypeError("synthetic offline"); };
  await view.refresh();
  const html = view.render();
  assert.match(html, /API 서버에 연결할 수 없습니다/);
  assert.doesNotMatch(html, /검증된 전략 A/);
  assert.equal(calls.length, 2);
  assert.equal(view.buttons()[0].props.disabled, false);
});

test("logical failure does not show returned candidates as verified or hide a missing reason", async () => {
  for (const errors of [[], ["공유 등록부 조회 실패"]]) {
    useResponse(result([row()], { ok: false, errors }));
    const view = harness();
    await view.refresh();
    const html = view.render();
    assert.match(html, /role="status"/);
    assert.doesNotMatch(html, /검증된 전략 A|확인할 근거가 없습니다/);
    assert.equal(calls.length, 1);
  }
});

test("HTTP refresh failure makes one GET and preserves manual retry only", async () => {
  useResponse(result());
  responseForFetch = async () => response({ reason: "합성 접근 거부" }, false, 403);
  const view = harness();
  await view.refresh();
  assert.match(view.render(), /합성 접근 거부/);
  assert.equal(calls.length, 1);
  assert.equal(nativeBridgeReads, 0);
});

test("App integration displays the complete inbox without an automatic strategy filter or execution callbacks", () => {
  const appSource = readFileSync(new URL("../src/App.jsx", import.meta.url), "utf8");
  assert.equal((appSource.match(/<PaperCandidateEvidencePanel\b/g) || []).length, 1);
  assert.match(appSource, /<PaperCandidateEvidencePanel\s*\/>/);
  assert.doesNotMatch(source, /useEffect|setInterval|onImport|onApprove|onStart/);
});
