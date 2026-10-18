import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import Module, { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import test, { after } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { transformSync } from "esbuild";
import * as api from "../src/api.js";

// Compile the production component and its pure helpers, without importing App
// or starting Python. The real API module can reach only this mocked fetch.
const sourcePath = fileURLToPath(new URL("../src/App.jsx", import.meta.url));
const source = readFileSync(sourcePath, "utf8");
function functionSource(name) {
  const start = source.indexOf(`function ${name}(`);
  assert.ok(start >= 0, `production function ${name} must exist`);
  const next = source.indexOf("\nfunction ", start + 1);
  assert.ok(next > start, `production function ${name} must have a boundary`);
  return source.slice(start, next);
}
const isolatedSource = `
import React, { useEffect, useRef, useState } from "react";
import { getEnvSettings } from "./api";
import { PanelHeader, StatusPill, NestedTabs, ToggleSwitch, Save, semanticSurfaceProps } from "test-presentation";
${["settingsBooleanValue", "assistantFieldStatus", "clearSecretDrafts", "BrokerConnectionAssistant"].map(functionSource).join("\n")}
export default BrokerConnectionAssistant;
`;
const transformed = transformSync(isolatedSource, {
  loader: "jsx", jsx: "automatic", format: "cjs", target: "es2022",
}).code;
const require = createRequire(import.meta.url);
const presentation = {
  PanelHeader: ({ title, subtitle, suffix }) => React.createElement("header", null, title, subtitle, suffix),
  StatusPill: ({ tone, children }) => React.createElement("span", { "data-tone": tone }, children),
  NestedTabs: () => React.createElement("nav"),
  ToggleSwitch: ({ checked, label, onChange }) => React.createElement("input", {
    type: "checkbox", checked, "aria-label": label, onChange,
  }),
  Save: () => React.createElement("svg"),
  semanticSurfaceProps: (tone, className) => ({ className, "data-tone": tone }),
};
const oldWindow = globalThis.window;
const oldFetch = globalThis.fetch;
let calls = [];
let respond;
let nativeReads = 0;
globalThis.window = {
  setTimeout, clearTimeout,
  get pywebview() { nativeReads += 1; throw new Error("Native calls forbidden"); },
};
globalThis.fetch = async (url, options) => {
  assert.equal(url, "/api/env-settings", "only the mocked local settings API is allowed");
  assert.ok(["GET", "POST"].includes(options.method));
  calls.push({ method: options.method, body: options.body ? JSON.parse(options.body) : undefined });
  return respond(url, options);
};
after(() => {
  globalThis.window = oldWindow;
  globalThis.fetch = oldFetch;
  assert.equal(nativeReads, 0);
});

function settings() {
  return {
    envPath: "fixture-only/.env",
    groups: [{ id: "kis", label: "주식/ETF" }, { id: "binance", label: "Binance" }],
    fields: [
      { key: "KIS_APP_KEY", kind: "secret", group: "kis", label: "KIS app key", detail: "fixture", configured: true, required: true, value: "", default: "" },
      { key: "KIS_ACCOUNT_PRODUCT_CODE", kind: "text", group: "kis", label: "KIS 상품 코드", detail: "fixture", required: true, value: "01", default: "01" },
    ],
  };
}
function fundingSettings() {
  const value = settings();
  value.fields.push({
    key: "KIS_OVERSEAS_FUNDING_MODE", kind: "select", group: "kis", label: "해외주식 결제 방식",
    detail: "계좌의 통합증거금 가입을 대신하지 않습니다. 지원시장별 확인 후 사용하며 실거래 활성화와 별개입니다.",
    value: "INTEGRATED_MARGIN", default: "INTEGRATED_MARGIN", required: true,
    options: [
      { value: "INTEGRATED_MARGIN", label: "통합증거금 (원화·자동환전)" },
      { value: "FOREIGN_CASH", label: "외화 현금 (사전환전)" },
    ],
  });
  return value;
}
const success = () => ({ ok: true, settings: settings() });
const response = (payload) => ({ ok: true, status: 200, headers: { get: () => "application/json" }, json: async () => payload });
function setup(...responses) {
  const payload = responses.length ? responses[0] : success();
  calls = [];
  respond = async () => response(payload);
}
function elements(node, type) {
  if (!node || typeof node !== "object") return [];
  if (Array.isArray(node)) return node.flatMap((child) => elements(child, type));
  return [...(node.type === type ? [node] : []), ...elements(node.props?.children, type)];
}
function harness(props = {}) {
  const slots = [];
  const pendingEffects = [];
  let cursor = 0;
  let dirty = false;
  let tree;
  const hooks = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { value: typeof initial === "function" ? initial() : initial };
      return [slots[index].value, (update) => {
        const next = typeof update === "function" ? update(slots[index].value) : update;
        if (!Object.is(next, slots[index].value)) { slots[index].value = next; dirty = true; }
      }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = { current: initial };
      return slots[index];
    },
    useEffect(effect, dependencies) {
      const index = cursor++;
      const previous = slots[index];
      if (!previous || !dependencies || dependencies.some((value, n) => !Object.is(value, previous.dependencies[n]))) {
        slots[index] = { dependencies, cleanup: previous?.cleanup };
        pendingEffects.push(() => {
          slots[index].cleanup?.();
          slots[index].cleanup = effect();
        });
      }
    },
  };
  const module = new Module(sourcePath);
  module.filename = sourcePath;
  module.paths = Module._nodeModulePaths(fileURLToPath(new URL("../src", import.meta.url)));
  module.require = (specifier) => specifier === "react" ? hooks
    : specifier === "./api" ? api
      : specifier === "test-presentation" ? presentation : require(specifier);
  module._compile(transformed, sourcePath);
  props = { onSave: api.saveEnvSettings, ...props };
  const view = {
    render() {
      let renders = 0;
      do {
        dirty = false;
        cursor = 0;
        tree = module.exports.default(props);
        while (pendingEffects.length) pendingEffects.shift()();
        assert.ok(++renders < 10, "effects must settle");
      } while (dirty);
      return renderToStaticMarkup(tree);
    },
    async settle() {
      for (let count = 0; count < 12; count += 1) await Promise.resolve();
      return view.render();
    },
    saveButton() { return elements(tree, "button").find((button) => button.props.className.includes("primary-button")); },
    save() { return view.saveButton().props.onClick(); },
    retry() {
      const button = elements(tree, "button").find((item) => item.props.children === "설정 다시 확인");
      assert.ok(button, "unknown settings must have an explicit read retry");
      button.props.onClick();
      return view.render();
    },
    input(key) {
      const label = elements(tree, "label").find((item) => item.key === key);
      return elements(label, "input")[0];
    },
    select(key) {
      const label = elements(tree, "label").find((item) => item.key === key);
      return elements(label, "select")[0];
    },
    edit(key, value) {
      view.input(key).props.onChange({ target: { value } });
      return view.render();
    },
    group(group) {
      elements(tree, presentation.NestedTabs)[0].props.onChange(group);
      return view.render();
    },
    unmount() { slots.forEach((entry) => entry.cleanup?.()); },
  };
  view.render();
  return view;
}

test("pending settings read never presents READY or stale broker readiness", async () => {
  setup();
  let release;
  respond = () => new Promise((resolve) => { release = resolve; });
  const view = harness({ brokers: [{ broker_id: "kis", order_ready: true, name: "stale broker" }] });
  try {
    assert.match(view.render(), /읽는 중/);
    assert.doesNotMatch(view.render(), /READY|stale broker/);
    assert.equal(view.saveButton().props.disabled, true);
    await view.save();
    assert.equal(calls.length, 1);
  } finally {
    release(response(success()));
    await view.settle();
    view.unmount();
  }
});

test("missing, rejected, empty and malformed settings stay unverified", async () => {
  const invalid = [undefined, null, {}, [], { ok: false, settings: settings() }, { ok: true, settings: {} },
    { ok: true, settings: { fields: [] } }, { ok: true, settings: { fields: "invalid" } },
    { ok: true, settings: { fields: [null] } },
    { ok: true, settings: { ...settings(), envPath: {} } },
    { ok: true, settings: { fields: [{ ...settings().fields[0], label: {} }] } },
    { ok: true, settings: { fields: [{ ...settings().fields[0], configured: "false" }] } },
    { ok: true, settings: { ...settings(), groups: "invalid" } },
    { ok: true, settings: { ...settings(), fields: [settings().fields[0], settings().fields[0]] } }];
  for (const payload of invalid) {
    setup(payload);
    const view = harness();
    const html = await view.settle();
    assert.match(html, /확인 실패/, JSON.stringify(payload));
    assert.doesNotMatch(html, /READY|설정 확인<\/span>|저장했습니다/);
    assert.equal(view.saveButton().props.disabled, true);
    await view.save();
    assert.equal(calls.length, 1);
    view.unmount();
  }
});

test("read failure offers an explicit fresh read and accepts only verified fields", async () => {
  setup();
  respond = async () => { throw new TypeError("mock disconnected"); };
  const view = harness();
  assert.match(await view.settle(), /확인 실패/);
  respond = async () => response(success());
  assert.match(view.retry(), /읽는 중/);
  assert.match(await view.settle(), /설정 확인<\/span>/);
  assert.equal(view.saveButton().props.disabled, false);
  assert.deepEqual(calls.map((call) => call.method), ["GET", "GET"]);
  assert.equal(view.input("KIS_APP_KEY").props.value, "");
  assert.match(view.group("binance"), /항목 없음/);
  assert.equal(view.saveButton().props.disabled, true);
  view.unmount();
});

test("missing required values remain CHECK after a valid settings response", async () => {
  const payload = success();
  payload.settings.fields[0].configured = false;
  setup(payload);
  const view = harness();
  assert.match(await view.settle(), /1 CHECK/);
  assert.match(view.render(), /BLOCK/);
  assert.doesNotMatch(view.render(), /설정 확인<\/span>|READY/);
  view.unmount();
});

test("successful save uses returned values, clears secrets and blocks simultaneous submissions", async () => {
  setup();
  const view = harness();
  await view.settle();
  view.edit("KIS_APP_KEY", "fixture-only-secret");
  view.edit("KIS_ACCOUNT_PRODUCT_CODE", " 02 ");
  let release;
  respond = () => new Promise((resolve) => { release = resolve; });
  const pending = view.save();
  try {
    await view.save();
    view.render();
    assert.equal(view.saveButton().props.disabled, true);
    assert.deepEqual(calls.map((call) => call.method), ["GET", "POST"]);
    assert.equal(calls[1].body.confirmed, false);
    assert.equal(calls[1].body.values.LIVE_TRADER_ENABLE_REAL_ORDERS, undefined);
    const saved = success();
    saved.settings.fields[1].value = "02";
    release(response(saved));
    await pending;
    assert.match(view.render(), /2개 설정을 저장했습니다/);
    assert.equal(view.input("KIS_ACCOUNT_PRODUCT_CODE").props.value, "02");
    assert.equal(view.input("KIS_APP_KEY").props.value, "");
    assert.equal(view.saveButton().props.disabled, false);
  } finally {
    release(response(success()));
    await pending;
    view.unmount();
  }
});

test("undefined, rejected and malformed save results never report success", async () => {
  const changedScope = success();
  changedScope.settings.fields = [changedScope.settings.fields[1]];
  const invalid = [undefined, null, {}, { ok: true }, { ok: false, settings: settings(), reason: "mock save rejected" },
    { ok: true, settings: { fields: [] } }, changedScope];
  for (const payload of invalid) {
    setup();
    let writes = 0;
    const view = harness({ onSave: async () => { writes += 1; return payload; } });
    await view.settle();
    view.edit("KIS_APP_KEY", "fixture-only-secret");
    await view.save();
    const html = view.render();
    assert.match(html, /확인 실패/);
    assert.doesNotMatch(html, /저장했습니다|fixture-only-secret/);
    assert.equal(view.input("KIS_APP_KEY").props.value, "");
    assert.equal(view.saveButton().props.disabled, true);
    await view.save();
    assert.equal(writes, 1);
    view.retry();
    assert.match(await view.settle(), /설정 확인<\/span>/);
    view.unmount();
  }
});

test("save exceptions unlock the read retry and remove entered secrets", async () => {
  setup();
  const view = harness({ onSave: async () => { throw new Error("mock save interrupted"); } });
  await view.settle();
  view.edit("KIS_APP_KEY", "fixture-only-secret");
  await view.save();
  assert.match(view.render(), /mock save interrupted/);
  assert.doesNotMatch(view.render(), /저장했습니다|fixture-only-secret/);
  assert.equal(view.saveButton().props.disabled, true);
  view.retry();
  assert.match(await view.settle(), /설정 확인<\/span>/);
  view.unmount();
});

test("a cancelled component ignores a settings response arriving after unmount", async () => {
  setup();
  let release;
  respond = () => new Promise((resolve) => { release = resolve; });
  const view = harness();
  await view.settle();
  assert.equal(typeof release, "function", "the settings request must have started");
  view.unmount();
  release(response(success()));
  assert.match(await view.settle(), /읽는 중/);
  assert.equal(view.saveButton().props.disabled, true);
});

test("funding select shows verified labels, current choice and its scope detail", async () => {
  setup({ ok: true, settings: fundingSettings() });
  const view = harness();
  try {
    const html = await view.settle();
    assert.equal(view.select("KIS_OVERSEAS_FUNDING_MODE").props.value, "INTEGRATED_MARGIN");
    assert.match(html, /통합증거금 \(원화·자동환전\)/);
    assert.match(html, /외화 현금 \(사전환전\)/);
    assert.match(html, /계좌의 통합증거금 가입을 대신하지 않습니다/);
    assert.match(html, /지원시장별 확인 후 사용하며 실거래 활성화와 별개입니다/);
    assert.equal(view.input("KIS_OVERSEAS_FUNDING_MODE"), undefined, "a validated choice is not a free-form text input");
    assert.equal(view.saveButton().props.disabled, false);
    assert.equal(calls.length, 1, "loading funding preferences must not save or enable trading");
  } finally { view.unmount(); }
});

test("both funding choices round-trip through existing settings save without enabling real orders", async () => {
  setup({ ok: true, settings: fundingSettings() });
  respond = async (_url, options) => {
    const saved = fundingSettings();
    if (options.method === "POST") {
      const body = JSON.parse(options.body);
      saved.fields[2].value = body.values.KIS_OVERSEAS_FUNDING_MODE;
    }
    return response({ ok: true, settings: saved });
  };
  const view = harness();
  try {
    await view.settle();
    for (const value of ["FOREIGN_CASH", "INTEGRATED_MARGIN"]) {
      view.select("KIS_OVERSEAS_FUNDING_MODE").props.onChange({ target: { value } });
      view.render();
      assert.equal(view.select("KIS_OVERSEAS_FUNDING_MODE").props.value, value);
      await view.save();
      assert.match(view.render(), /2개 설정을 저장했습니다/);
      const write = calls.at(-1);
      assert.equal(write.method, "POST");
      assert.equal(write.body.values.KIS_OVERSEAS_FUNDING_MODE, value);
      assert.equal(write.body.values.KIS_ACCOUNT_PRODUCT_CODE, "01");
      assert.equal(write.body.values.KIS_APP_KEY, undefined);
      assert.equal(write.body.values.LIVE_TRADER_ENABLE_REAL_ORDERS, undefined);
      assert.equal(write.body.confirmed, false);
      assert.equal(view.select("KIS_OVERSEAS_FUNDING_MODE").props.value, value);
    }
    assert.deepEqual(calls.map((call) => call.method), ["GET", "POST", "POST"]);
  } finally { view.unmount(); }
});

test("unverified select options or values block settings load and save", async () => {
  const valid = fundingSettings().fields[2];
  const invalid = [
    { options: undefined }, { options: [] }, { options: {} }, { options: [null] },
    { options: [["INTEGRATED_MARGIN", "통합증거금"]] },
    { options: [{ value: "", label: "통합증거금" }] },
    { options: [{ value: " INTEGRATED_MARGIN ", label: "통합증거금" }] },
    { options: [{ value: "INTEGRATED_MARGIN", label: " " }] },
    { options: [{ value: true, label: "통합증거금" }] },
    { options: [{ value: "INTEGRATED_MARGIN", label: {} }] },
    { options: [valid.options[0], { ...valid.options[1], value: "INTEGRATED_MARGIN" }] },
    { options: [valid.options[0], { ...valid.options[1], label: valid.options[0].label }] },
    { options: [valid.options[0], { ...valid.options[1], label: ` ${valid.options[0].label} ` }] },
    { value: "UNKNOWN" }, { default: "UNKNOWN" }, { value: true }, { default: null },
  ];
  for (const change of invalid) {
    const value = fundingSettings();
    value.fields[2] = { ...value.fields[2], ...change };
    setup({ ok: true, settings: value });
    const view = harness();
    try {
      const html = await view.settle();
      assert.match(html, /확인 실패/, JSON.stringify(change));
      assert.equal(view.select("KIS_OVERSEAS_FUNDING_MODE"), undefined);
      assert.equal(view.saveButton().props.disabled, true);
      await view.save();
      assert.equal(calls.length, 1, "invalid metadata must never be sent back as saved settings");
    } finally { view.unmount(); }
  }
});

test("a saved funding response with invalid select metadata never reports success", async () => {
  setup({ ok: true, settings: fundingSettings() });
  const invalid = fundingSettings();
  invalid.fields[2].options = [invalid.fields[2].options[0], invalid.fields[2].options[0]];
  let writes = 0;
  const view = harness({ onSave: async () => { writes += 1; return { ok: true, settings: invalid }; } });
  try {
    await view.settle();
    await view.save();
    assert.match(view.render(), /확인 실패/);
    assert.doesNotMatch(view.render(), /저장했습니다/);
    assert.equal(view.saveButton().props.disabled, true);
    await view.save();
    assert.equal(writes, 1);
  } finally { view.unmount(); }
});
