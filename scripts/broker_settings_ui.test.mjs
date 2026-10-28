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
${["settingsBooleanValue", "isProvisionalKisFeeField", "assistantFieldStatus", "clearSecretDrafts", "BrokerConnectionAssistant"].map(functionSource).join("\n")}
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
function feeSettings() {
  const value = fundingSettings();
  for (const [key, amount, label] of [
    ["KIS_ESTIMATED_KRX_STOCK_FEE_PERCENT", "0.0140527", "KRX 주식 임시 수수료 (%)"],
    ["KIS_ESTIMATED_KRX_ETF_FEE_PERCENT", "0.0146527", "KRX ETF 임시 수수료 (%)"],
    ["KIS_ESTIMATED_NXT_STOCK_FEE_PERCENT", "0.0130527", "NXT 주식 임시 수수료 (%)"],
    ["KIS_ESTIMATED_US_FEE_PERCENT", "0.25", "미국 임시 수수료 (%)"],
  ]) value.fields.push({ key, kind: "text", group: "kis", label,
    detail: "공식표 참고 임시값 · 본인/API/우대율 및 최소수수료·제세금·자동환전 별도 확인",
    configured: true, required: false, value: amount, default: amount,
    confirmationRequired: true, estimateOnly: true, status: "PROVISIONAL_CONFIRMATION_REQUIRED",
    rateUnit: "PERCENT", sourceClassification: "PUBLIC_BASELINE_NOT_PERSONAL_AGREEMENT",
    sourceUrl: key === "KIS_ESTIMATED_US_FEE_PERCENT"
      ? "https://www.truefriend.com/main/bond/research/_static/TF03ca050000.jsp"
      : "https://www.truefriend.com/main/customer/guide/_static/TF04ae010000.jsp?templet-bypass=true",
    accountFeeRateApproved: false, actualPersonalFeeObserved: false, actualFeeObserved: false,
    quantityAuthorityGranted: false, orderAuthorityGranted: false, productionReady: false, financialReady: false });
  return value;
}
function shortLiveCostSettings() {
  const value = settings();
  for (const [key, amount, label] of [
    ["SHORT_LIVE_KIS_COST_CONFIRMED", "false", "본인 KIS 비용 확인"],
    ["SHORT_LIVE_KIS_COST_SOURCE", "본인 요율 안내 조회", "비용 확인 근거"],
    ["SHORT_LIVE_KIS_COST_SYMBOLS", "069500", "확인 요율 적용 종목"],
    ["SHORT_LIVE_KIS_COST_OBSERVED_AT", "2026-10-20T15:00:00+09:00", "비용 확인 시각"],
    ["SHORT_LIVE_KIS_FEE_RATE", "0.00015", "KIS 수수료율"],
    ["SHORT_LIVE_KIS_SELL_TAX_RATE", "0", "KIS 매도세율"],
  ]) value.fields.push({ key, kind: key.endsWith("CONFIRMED") ? "bool" : "text", group: "kis", label,
    detail: "fixture", configured: true, required: false, value: amount, default: "" });
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
    costTimeButton() {
      return elements(tree, "button").find((button) => button.props.children === "지금 확인한 시각 입력");
    },
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
    field(key) { return elements(tree, "label").find((item) => item.key === key); },
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

test("obsolete live-lock settings are absent and reading never enables trading", async () => {
  const saved = settings();
  saved.groups.push({id:"live-lock",label:"실거래 잠금"});
  saved.fields.push({key:"LIVE_TRADER_ENABLE_REAL_ORDERS",kind:"bool",group:"live-lock",label:"실전 주문 라우트",detail:"legacy",configured:true,value:"true",default:"false"});
  setup({ok:true,settings:saved});
  const view=harness();
  try {
    await view.settle();
    assert.doesNotMatch(view.render(), /실거래 잠금|실전 주문 라우트|앱을 다시 열면 실전 주문 허용이 해제/);
    assert.equal(calls.filter(call=>call.method==="POST").length,0);
  } finally {view.unmount();}
});

test("API essentials are open while fee detail stays collapsed with no data loss", async () => {
  const saved=settings();
  saved.fields.push({key:"SHORT_LIVE_KIS_COST_SOURCE",kind:"text",group:"kis",label:"비용 확인 근거",detail:"확인 메모",configured:true,value:"요율 조회",default:""});
  setup({ok:true,settings:saved});
  const view=harness();
  try {
    await view.settle();
    const html=view.render();
    assert.match(html, /<details class="ts-disclosure" open=""><summary class="ts-disclosure-summary">API 연결에 필요한 정보/);
    assert.match(html, /<details class="ts-disclosure"><summary class="ts-disclosure-summary">KIS 시험 비용 상세/);
    assert.match(html, /본인 계좌의 수수료·세율을 어디서 확인했는지/);
    assert.match(html, /value="요율 조회"/);
    assert.equal(calls.filter(call=>call.method==="POST").length,0);
  } finally {view.unmount();}
});

test("KIS cost time is never renewed by opening, switching tabs or saving", async () => {
  const saved = shortLiveCostSettings();
  setup({ ok: true, settings: saved });
  const view = harness();
  try {
    const html = await view.settle();
    assert.match(html, /KIS 국내 주문 연결 시험의 필수 확인 정보/);
    assert.match(html, /확인 시각부터 24시간 이내/);
    assert.match(html, /Binance·Upbit 시험이나 계좌 잔고 조회에는 필요하지 않습니다/);
    assert.doesNotMatch(html, /남기는 메모입니다/);
    assert.match(html, /확인하지 않았다면 누르지 마세요/);
    const observedAt = saved.fields.find((field) => field.key === "SHORT_LIVE_KIS_COST_OBSERVED_AT").value;
    assert.equal(view.input("SHORT_LIVE_KIS_COST_OBSERVED_AT").props.value, observedAt);
    assert.equal(view.costTimeButton().props.type, "button");
    view.group("binance");
    view.group("kis");
    assert.equal(view.input("SHORT_LIVE_KIS_COST_OBSERVED_AT").props.value, observedAt);
    assert.deepEqual(calls.map((call) => call.method), ["GET"]);
    await view.save();
    assert.equal(calls[1].body.values.SHORT_LIVE_KIS_COST_OBSERVED_AT, observedAt);
    assert.equal(calls[1].body.values.SHORT_LIVE_KIS_COST_CONFIRMED, "false");
    assert.equal(view.input("SHORT_LIVE_KIS_COST_OBSERVED_AT").props.value, observedAt);
  } finally { view.unmount(); }
});

test("explicit KIS time button updates only its draft with a timezone and never confirms costs", async () => {
  const saved = shortLiveCostSettings();
  setup({ ok: true, settings: saved });
  const view = harness();
  let observedAt;
  try {
    await view.settle();
    view.edit("SHORT_LIVE_KIS_COST_SOURCE", "직접 확인한 본인 요율 안내");
    view.edit("SHORT_LIVE_KIS_COST_SYMBOLS", "069500,102110");
    view.edit("SHORT_LIVE_KIS_FEE_RATE", "0.0001");
    const before = Date.now();
    view.costTimeButton().props.onClick();
    view.render();
    observedAt = view.input("SHORT_LIVE_KIS_COST_OBSERVED_AT").props.value;
    const after = Date.now();
    assert.match(observedAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{2}:\d{2}$/);
    assert.ok(Date.parse(observedAt) >= before && Date.parse(observedAt) <= after);
    const offset = -new Date().getTimezoneOffset();
    const expectedOffset = `${offset >= 0 ? "+" : "-"}${String(Math.floor(Math.abs(offset) / 60)).padStart(2, "0")}:${String(Math.abs(offset) % 60).padStart(2, "0")}`;
    assert.ok(observedAt.endsWith(expectedOffset));
    assert.equal(elements(view.field("SHORT_LIVE_KIS_COST_CONFIRMED"), presentation.ToggleSwitch)[0].props.checked, false);
    assert.equal(view.input("SHORT_LIVE_KIS_COST_SOURCE").props.value, "직접 확인한 본인 요율 안내");
    assert.equal(view.input("SHORT_LIVE_KIS_COST_SYMBOLS").props.value, "069500,102110");
    assert.equal(view.input("SHORT_LIVE_KIS_FEE_RATE").props.value, "0.0001");
    assert.equal(view.input("SHORT_LIVE_KIS_SELL_TAX_RATE").props.value, "0");
    assert.deepEqual(calls.map((call) => call.method), ["GET"], "button must not persist settings or call another API");
    respond = async (_url, options) => {
      const posted = JSON.parse(options.body);
      assert.equal(posted.values.SHORT_LIVE_KIS_COST_OBSERVED_AT, observedAt);
      assert.equal(posted.values.SHORT_LIVE_KIS_COST_CONFIRMED, "false");
      assert.equal(posted.confirmed, false);
      saved.fields.forEach((field) => {
        if (field.key in posted.values) field.value = posted.values[field.key];
      });
      return response({ ok: true, settings: saved });
    };
    await view.save();
    view.render();
    assert.equal(view.input("SHORT_LIVE_KIS_COST_OBSERVED_AT").props.value, observedAt);
  } finally { view.unmount(); }
  setup({ ok: true, settings: saved });
  const reopened = harness();
  try {
    await reopened.settle();
    assert.equal(reopened.input("SHORT_LIVE_KIS_COST_OBSERVED_AT").props.value, observedAt);
    assert.equal(elements(reopened.field("SHORT_LIVE_KIS_COST_CONFIRMED"), presentation.ToggleSwitch)[0].props.checked, false);
  } finally { reopened.unmount(); }
});

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

test("filled official fee defaults stay amber and unconfirmed despite stale broker READY", async () => {
  setup({ ok: true, settings: feeSettings() });
  const view = harness({ brokers: [{ broker_id: "kis", order_ready: true, name: "KIS fixture", detail: "연결 관측" }] });
  const html = await view.settle();
  assert.match(html, /공식 기본표 참고 임시값 · 본인 적용 수수료 확인 필요/);
  assert.match(html, /수수료 확인 필요/);
  assert.match(html, /KIS 시험 비용 상세/);
  assert.match(html, /다른 거래소의 연결 상태와 별개/);
  assert.doesNotMatch(html, /READY|설정 확인<\/span>/);
  for (const field of feeSettings().fields.filter((item) => item.key.startsWith("KIS_ESTIMATED_"))) {
    assert.equal(view.input(field.key).props.value, field.default, "default must be a real editable value");
    const label = view.field(field.key);
    assert.equal(label.props["data-tone"], "warning");
    assert.match(renderToStaticMarkup(label), /임시값 · 단위 % · 확인 필요/);
    assert.doesNotMatch(renderToStaticMarkup(label), /DONE/);
  }
  assert.equal(view.saveButton().props.disabled, false);
  view.unmount();
});

test("editing and saving provisional fees does not verify them or grant orders", async () => {
  setup({ ok: true, settings: feeSettings() });
  const view = harness();
  await view.settle();
  view.edit("KIS_ESTIMATED_US_FEE_PERCENT", "0.2");
  respond = async (_url, options) => {
    const posted = JSON.parse(options.body);
    assert.equal(posted.values.KIS_ESTIMATED_US_FEE_PERCENT, "0.2");
    assert.equal(posted.confirmed, false);
    const saved = feeSettings();
    saved.fields.find((field) => field.key === "KIS_ESTIMATED_US_FEE_PERCENT").value = "0.2";
    return response({ ok: true, settings: saved });
  };
  await view.save();
  const html = view.render();
  assert.match(html, /설정을 저장했습니다/);
  assert.match(html, /임시 수수료는 계속 확인이 필요/);
  assert.equal(view.input("KIS_ESTIMATED_US_FEE_PERCENT").props.value, "0.2");
  assert.equal(view.field("KIS_ESTIMATED_US_FEE_PERCENT").props["data-tone"], "warning");
  assert.doesNotMatch(renderToStaticMarkup(view.field("KIS_ESTIMATED_US_FEE_PERCENT")), /DONE/);
  assert.doesNotMatch(html, /READY|설정 확인<\/span>/);
  assert.deepEqual(calls.map((call) => call.method), ["GET", "POST"]);
  view.unmount();
});

test("known fee keys reject removed or forged provisional metadata on load and save", async () => {
  const mutations = [
    (field) => { delete field.confirmationRequired; },
    (field) => { field.confirmationRequired = false; },
    (field) => { delete field.estimateOnly; },
    (field) => { field.estimateOnly = false; },
    (field) => { field.status = "VERIFIED"; },
    (field) => { field.rateUnit = "FRACTION"; },
    (field) => { field.sourceClassification = "PERSONAL_AGREEMENT"; },
    (field) => { field.actualFeeObserved = true; },
    (field) => { field.orderAuthorityGranted = 0; },
    (field) => { field.sourceUrl = "javascript:alert(1)"; },
    (field) => { field.value = 0.25; },
    (field) => { field.value = "NaN"; },
    (field) => { field.default = "0"; },
    (field) => { field.group = "binance"; },
    (field) => { field.kind = "secret"; },
  ];
  for (const mutate of mutations) {
    const invalid = feeSettings();
    mutate(invalid.fields.at(-1));
    setup({ ok: true, settings: invalid });
    let view = harness();
    assert.match(await view.settle(), /확인 실패/);
    assert.equal(view.saveButton().props.disabled, true);
    assert.doesNotMatch(view.render(), /설정 확인<\/span>|READY/);
    view.unmount();
    setup({ ok: true, settings: feeSettings() });
    view = harness();
    await view.settle();
    respond = async () => response({ ok: true, settings: invalid });
    await view.save();
    assert.match(view.render(), /저장 결과를 확인하지 못했습니다/);
    assert.doesNotMatch(view.render(), /저장했습니다|설정 확인<\/span>|READY/);
    assert.equal(view.saveButton().props.disabled, true);
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
