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
let allowImport = false;
let allowPreparation = false;
globalThis.window = {
  setTimeout, clearTimeout,
  get pywebview() { nativeBridgeReads += 1; throw new Error("Native bridge forbidden in read-only UI test"); },
};
globalThis.fetch = async (url, options) => {
  if (options.method === "POST") {
    assert.equal(allowImport || allowPreparation, true);
    assert.equal(url, allowPreparation ? "/api/preparation/preview" : "/api/paper-candidates/import");
    assert.equal(options.headers["X-LiveTrader-CSRF"], "fixture-csrf-".repeat(4));
  } else {
    assert.ok(["/api/paper-candidates", "/api/preparation/sources"].includes(url));
    assert.equal(options.method, "GET");
    assert.equal(options.body, undefined);
  }
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
    elements(type) { return elements(tree, type); },
    legacyButtons() { return elements(tree, "button").filter(button => !["준비 자료 새로고침", "원본 확인 · 계좌 조회 없음", "계좌·미체결 읽기 및 입력 금액 계산"].includes(button.props.children)); },
  };
  view.render();
  return view;
}

test("mount, disclosure render, and strategy changes never fetch and expose only refresh", () => {
  useResponse(result());
  const view = harness({ strategyId: "strategy-a" });
  const html = view.render({ strategyId: "strategy-b" });
  assert.equal(calls.length, 0);
  assert.equal(view.legacyButtons().length, 1);
  assert.equal(view.buttons()[0].props.children, "Paper 검증 근거 새로고침");
  assert.doesNotMatch(html, /<(?:input|form)\b/);
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
  assert.equal(view.legacyButtons().length, 1);
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

test("registration blockers distinguish sealed evidence from registration and never expose an action", async () => {
  useResponse(result([row({ blockedReasons: [
    { code: "DEPLOYMENT_ACCOUNT_ALREADY_BOUND", detail: "기존 배포에 계좌 연결이 설정되어 있어 후보 등록으로 변경할 수 없습니다." },
    { code: "DEPLOYMENT_NOT_DRAFT", detail: "기존 배포가 검토 대기 초안이 아닙니다." },
  ] })]));
  const view = harness();
  await view.refresh();
  const html = view.render();
  assert.match(html, /Backtester 저장본 · 현재 전략과 실행 단위 일치/);
  assert.match(html, /Paper 검증 근거 · 봉인 연결 확인/);
  assert.match(html, /Live 후보 등록 · 기존 배포 조건으로 차단/);
  assert.match(html, /계좌 연결이 설정되어/);
  assert.match(html, /검토 대기 초안이 아닙니다/);
  assert.match(html, /실거래 승인과 현재 계좌 상태는 이 조회에서 확인하지 않습니다/);
  assert.equal(view.legacyButtons().length, 1);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "GET");
});

test("legacy candidate response remains readable without inventing a registration cause", async () => {
  useResponse(result([row()]));
  const view = harness();
  await view.refresh();
  const html = view.render();
  assert.match(html, /Live 후보 등록 · 상태 추가 확인 필요/);
  assert.doesNotMatch(html, /기존 배포 조건으로 차단|후보 등록 차단 사유/);
  assert.equal(view.legacyButtons().length, 1);
});

const malformed = [
  ["null response", null],
  ["object candidates", result({})],
  ["null candidate", result([null])],
  ["string candidate", result(["invalid"])],
  ["object errors", result([], { errors: {} })],
  ["object error item", result([], { errors: [{}] })],
  ["object detail", result([row({ detail: {} })])],
  ["object blocked reasons", result([row({ blockedReasons: {} })])],
  ["object blocked reason detail", result([row({ blockedReasons: [{ code: "CODE", detail: {} }] })])],
  ["registered candidate with blockers", result([row({ registered: true, blockedReasons: [{ code: "CODE", detail: "blocked" }] })])],
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
    assert.equal(view.legacyButtons().length, 1);
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
  assert.match(appSource, /<PaperCandidateEvidencePanel onRegistered=\{onPaperCandidateRegistered\}\s*\/>/);
  assert.match(appSource, /onPaperCandidateRegistered=\{selectImportedCandidate\}/);
  assert.doesNotMatch(source, /useEffect|setInterval|onImport|onApprove|onStart/);
});


test("explicit candidate registration uses the exact preview and native CSRF; never submits on refresh", async () => {
  const priorWindow = globalThis.window;
  const proposal = { rootKey: "a".repeat(64), evidenceId: "paper-evidence-a", identityHash: "b".repeat(64), registryHash: "c".repeat(64), expectedRevision: 0 };
  const ready = row({ canImport: true, importRequest: proposal, deployment: { deploymentId: "", mode: "UNREGISTERED", lifecycle: "", definitionHash: "", revision: 0 } });
  const selected = [];
  let posted = null;
  try {
    globalThis.window = { setTimeout, clearTimeout, pywebview: { api: { functional_http_session: async () => ({ ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: "fixture-csrf-".repeat(4) }) } } };
    allowImport = true;
    useResponse(result([ready], { canImport: true }));
    const view = harness({ onRegistered: async (id) => selected.push(id) });
    await view.refresh(); view.render();
    assert.deepEqual(calls.map(c => c.method), ["GET"]);
    responseForFetch = async (_url, options) => {
      if (options.method === "POST") {
        posted = JSON.parse(options.body);
        return response({ ok: true, deploymentId: "new-draft", authorizationGranted: false, detail: "검토 대기 후보로 등록했습니다." });
      }
      return response(result([row({ registered: true, deployment: { deploymentId: "new-draft", mode: "MONITOR", lifecycle: "draft", definitionHash: "d".repeat(64), revision: 1 } })]));
    };
    await view.buttons().find(button => button.props.children === "검토 대기 후보 등록").props.onClick();
    assert.deepEqual(posted, proposal);
    assert.deepEqual(calls.map(c => c.method), ["GET", "POST", "GET"]);
    assert.deepEqual(selected, ["new-draft"]);
    assert.match(view.render(), /등록한 배포 보기/);
    assert.doesNotMatch(view.render(), /실거래 시작|주문 제출/);
  } finally { allowImport = false; globalThis.window = priorWindow; }
});

test("stale import rejection clears the preview and does not automatically retry", async () => {
  const priorWindow = globalThis.window;
  const proposal = { rootKey: "a", evidenceId: "paper-evidence-a", identityHash: "b", registryHash: "c", expectedRevision: 0 };
  try {
    globalThis.window = { setTimeout, clearTimeout, pywebview: { api: { functional_http_session: async () => ({ ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: "fixture-csrf-".repeat(4) }) } } };
    allowImport = true;
    useResponse(result([row({ canImport: true, importRequest: proposal })], { canImport: true }));
    const view = harness(); await view.refresh(); view.render();
    responseForFetch = async () => response({ ok: false, reason: "Deployment가 바뀌었습니다. 새로고침하세요.", authorizationGranted: false });
    await view.buttons().find(button => button.props.children === "검토 대기 후보 등록").props.onClick();
    assert.match(view.render(), /Deployment가 바뀌었습니다/);
    assert.equal(view.legacyButtons().length, 1);
    assert.deepEqual(calls.map(c => c.method), ["GET", "POST"]);
  } finally { allowImport = false; globalThis.window = priorWindow; }
});

const preparationTrial = { name: "5종목 기능시험", detail: "비승급", portfolioId: "trial", canRun: true,
  request: { rootKey: "root-hash", portfolioId: "trial", portfolioHash: "portfolio-hash", identityHash: "identity-hash" } };
function preparationReply(draft = {}, overrides = {}) {
  const now = Date.now();
  return { ok: true, schemaVersion: "live-read-only-preparation-v1", readOnly: true, reportPurpose: "READ_ONLY_PREPARATION",
    authorityGranted: false, authorizationGranted: false, executable: false, promotionEligible: false,
    useAsPromotionEvidence: false, tradingEnabled: false, currentDeploymentChanged: false, ordersSubmitted: 0,
    confirmationTokensCreated: 0, permitsCreated: 0, runtimeSessionsCreated: 0,
    asOf: new Date(now).toISOString(), expiresAt: new Date(now + 60000).toISOString(),
    source: { kind: "NON_PROMOTION", portfolioHash: "portfolio-hash", evidenceClass: "FUNCTIONAL_TEST_NON_PROMOTION",
      qualification: "NON_PROMOTION", instruments: [{ instanceId: "instance-a", symbol: "BTCUSDT", broker: "binance" }] },
    draft: { missingInputs: ["instanceId", "side", "quantity", "limitPrice"], notional: null, ...draft },
    observation: { status: "NOT_REQUESTED" },
    checks: [{ code: "LIVE_AUTHORITY", status: "BLOCKED", detail: "권한 없음" }], limitations: ["주문을 실행하지 않습니다."], ...overrides };
}
function preparationSourcesReply() {
  return {ok:true,schemaVersion:"live-readonly-preparation-sources-v1",readOnly:true,executable:false,authorityGranted:false,sources:[{ name: preparationTrial.name, source: { evidenceClass: "FUNCTIONAL_TEST_NON_PROMOTION" }, request: { kind: "NON_PROMOTION", ...preparationTrial.request } }]};
}
async function preparationView() {
  useResponse(preparationSourcesReply());
  const view = harness();
  await view.buttons().find(button => button.props.children === "준비 자료 새로고침").props.onClick();view.render();
  const selector = view.elements("select").find(node => node.props.children?.flat?.().some?.(child => child?.props?.value === "trial:root-hash:trial"));
  selector.props.onChange({ target: { value: "trial:root-hash:trial" } }); view.render();
  return view;
}
function preparationWindow(timers) {
  return { setTimeout: (callback, ms) => {
    if (ms <= 60000) { timers.push(callback); return 0; }
    const timer = setTimeout(callback, ms); timer.unref?.(); return timer;
  }, clearTimeout, pywebview: { api: { functional_http_session: async () => ({
    ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: "fixture-csrf-".repeat(4),
  }) } } };
}
test("preparation explicit source/read actions keep missing quantity and exact input; changes clear old results", async () => {
  const previous = globalThis.window, timers = []; let posted;
  try {
    globalThis.window = preparationWindow(timers); allowPreparation = true;
    const view = await preparationView();
    responseForFetch = async (_url, options) => { posted = JSON.parse(options.body); return response(preparationReply()); };
    await view.buttons().find(button => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
    assert.equal(posted.readAccount, false); assert.equal(posted.draft.quantity, "");
    assert.match(view.render(), /입력 금액: 미입력/);
    assert.equal(calls.filter(call => call.method === "POST").length, 1);
    view.elements("select").find(node => node.props.value === "" && node.props.children?.flat?.().some?.(child => child?.props?.value === "instance-a")).props.onChange({ target: { value: "instance-a" } });
    assert.doesNotMatch(view.render(), /조회 시각/);
    view.elements("select").find(node => node.props.children?.flat?.().some?.(child => child?.props?.value === "BUY")).props.onChange({ target: { value: "BUY" } }); view.render();
    view.elements("input")[0].props.onChange({ target: { value: "0.1" } }); view.render();
    view.elements("input")[1].props.onChange({ target: { value: "0.2" } }); view.render();
    responseForFetch = async (_url, options) => { posted = JSON.parse(options.body); return response(preparationReply({ missingInputs: [], notional: "0.02" })); };
    await view.buttons().find(button => button.props.children === "계좌·미체결 읽기 및 입력 금액 계산").props.onClick();
    assert.deepEqual(posted.draft, { instanceId: "instance-a", side: "BUY", quantity: "0.1", limitPrice: "0.2" });
    assert.equal(posted.readAccount, true); assert.match(view.render(), /0.02/);
    assert.match(view.render(), /권한 없음 · 실행 불가/);
    timers.at(-1)(); assert.match(view.render(), /조회가 만료됐습니다/);
    assert.doesNotMatch(view.render(), /입력 금액: 0.02/);
  } finally { allowPreparation = false; globalThis.window = previous; }
});
test("preparation rejects authority-bearing or wrong-source responses", async () => {
  const previous = globalThis.window;
  try {
    globalThis.window = preparationWindow([]); allowPreparation = true;
    for (const overrides of [{ executable: true }, { permitsCreated: 1 }, { source: { ...preparationReply().source, portfolioHash: "changed" } }]) {
      const view = await preparationView();
      responseForFetch = async () => response(preparationReply({}, overrides));
      await view.buttons().find(button => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
      assert.match(view.render(), /범위와 유효시간을 확인하지 못했습니다/);
      assert.doesNotMatch(view.render(), /조회 시각/);
    }
  } finally { allowPreparation = false; globalThis.window = previous; }
});
test("changing preparation source or external strategy clears the previous snapshot", async () => {
  const previous = globalThis.window;
  try {
    globalThis.window = preparationWindow([]); allowPreparation = true;
    const view = await preparationView(); responseForFetch = async () => response(preparationReply());
    await view.buttons().find(button => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
    assert.match(view.render(), /조회 시각/);
    view.render({ strategyId: "changed-strategy" });
    assert.doesNotMatch(view.render(), /조회 시각/);
    assert.equal(view.elements("input").length, 0);
  } finally { allowPreparation = false; globalThis.window = previous; }
});

test("legacy candidate delay and failure cannot block or clear independent preparation sources", async () => {
  const previous=globalThis.window;
  try {
    globalThis.window=preparationWindow([]);allowPreparation=true;
    calls=[];const view=harness();let releaseLegacy;
    responseForFetch=async url=>{
      if(url==="/api/paper-candidates") return new Promise(resolve=>{releaseLegacy=resolve;});
      return response(preparationSourcesReply());
    };
    const legacy=view.refresh();view.render();
    await view.buttons().find(button=>button.props.children==="준비 자료 새로고침").props.onClick();
    view.render();
    assert.equal(view.elements("select").some(node=>node.props["aria-label"]==="준비할 원본"),true);
    releaseLegacy(response(result([], {ok:false,errors:["legacy unavailable"]})));await legacy;
    const html=view.render();assert.match(html,/5종목 기능시험/);assert.match(html,/legacy unavailable/);
    assert.equal(calls.filter(call=>call.url==="/api/preparation/sources").length,1);
  } finally {allowPreparation=false;globalThis.window=previous;}
});
