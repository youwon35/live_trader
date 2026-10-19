import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
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
let allowReview = false;
globalThis.window = {
  setTimeout, clearTimeout,
  get pywebview() { nativeBridgeReads += 1; throw new Error("Native bridge forbidden in read-only UI test"); },
};
globalThis.fetch = async (url, options) => {
  if (options.method === "POST") {
    assert.equal(allowImport || allowPreparation || allowReview, true);
    if (allowReview) assert.ok(["/api/safety-confirmation/challenge", "/api/paper-candidates/review"].includes(url));
    else assert.equal(url, allowPreparation ? "/api/preparation/preview" : "/api/paper-candidates/import");
    assert.equal(options.headers["X-LiveTrader-CSRF"], "fixture-csrf-".repeat(4));
  } else {
    assert.ok(["/api/paper-candidates", "/api/paper-candidates/accounts", "/api/preparation/sources"].includes(url));
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

// Public-only fixtures: no credentials, account balances, FX rate or settlement.
const provisionalFalseFlags = { accountFeeRateApproved: false, actualPersonalFeeObserved: false,
  actualFeeObserved: false, quantityAuthorityGranted: false, orderAuthorityGranted: false,
  productionReady: false, financialReady: false };
const provisionalUnknownCosts = () => ({ minimumCommission: null, buyTaxes: null, sellTaxes: null,
  automaticFxRate: null, automaticFxSpread: null, settlementAdjustment: null });
const provisionalStatus = "PROVISIONAL_CONFIRMATION_REQUIRED";
const provisionalScope = "UNCONFIRMED_COMMISSION_ONLY_NOT_TOTAL_COST";
function fixtureCanonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(fixtureCanonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") return `{${Object.keys(value).sort()
    .map((key) => `${JSON.stringify(key)}:${fixtureCanonicalJson(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}
function provisionalPreview(market = "KRX", notional = "1000000") {
  const rates = [
    ["KRX", "STOCK", "KIS_ESTIMATED_KRX_STOCK_FEE_PERCENT", "0.0140527", "0.000140527", "KRW"],
    ["KRX", "ETF", "KIS_ESTIMATED_KRX_ETF_FEE_PERCENT", "0.0146527", "0.000146527", "KRW"],
    ["NXT", "STOCK", "KIS_ESTIMATED_NXT_STOCK_FEE_PERCENT", "0.0130527", "0.000130527", "KRW"],
    ["US", "STOCK", "KIS_ESTIMATED_US_FEE_PERCENT", "0.25", "0.0025", "USD"],
    ["US", "ETF", "KIS_ESTIMATED_US_FEE_PERCENT", "0.25", "0.0025", "USD"],
  ].map(([rowMarket, product, settingKey, ratePercent, rateFraction, currency]) => ({
    market: rowMarket, product, settingKey, ratePercent, rateFraction, currency,
    publicDefaultPercent: ratePercent,
    sourceUrl: rowMarket === "US" ? "https://www.truefriend.com/main/bond/research/_static/TF03ca050000.jsp"
      : "https://www.truefriend.com/main/customer/guide/_static/TF04ae010000.jsp?templet-bypass=true",
    valueOrigin: "PUBLIC_DEFAULT", status: provisionalStatus, confirmationRequired: true, estimateOnly: true,
  }));
  const body = { schemaVersion: "kis-provisional-fee-profile-v1", status: provisionalStatus,
    confirmationRequired: true, estimateOnly: true, sourceClassification: "PUBLIC_BASELINE_NOT_PERSONAL_AGREEMENT",
    rateUnit: "PERCENT", rates, unknownCosts: provisionalUnknownCosts(), costScope: provisionalScope,
    ...provisionalFalseFlags };
  const profile = { ...body, feeProfileHash: createHash("sha256").update(fixtureCanonicalJson(body)).digest("hex") };
  const expected = market === "KRX" ? ["140.527", "146.527"] : ["2500", "2500"];
  return { schemaVersion: "kis-provisional-fee-preview-v1", status: provisionalStatus, profile,
    estimates: ["STOCK", "ETF"].map((product, index) => {
      const row = rates.find((item) => item.market === market && item.product === product);
      return { schemaVersion: "kis-provisional-fee-estimate-v1", status: provisionalStatus,
        confirmationRequired: true, estimateOnly: true, market, product, notional,
        notionalMeaning: "USER_ENTERED_NOTIONAL_NOT_EXECUTION", ratePercent: row.ratePercent,
        rateFraction: row.rateFraction, nativeEstimate: notional === null ? null : expected[index],
        currency: row.currency, feeProfileHash: profile.feeProfileHash, roundingApplied: false,
        settlementRoundingRule: null, unknownCosts: provisionalUnknownCosts(), costScope: provisionalScope,
        ...provisionalFalseFlags };
    }),
    calculationBasis: "USER_ENTERED_NOTIONAL_ONLY",
    productClassification: "UNKNOWN_SHOW_SEPARATE_STOCK_ETF_ALTERNATIVES",
    actualFeeObserved: false, orderAuthorityGranted: false, estimatedNotional: notional, accountReadRequired: false,
    followupSteps: [
      { code: "PERSONAL_FEE_TERMS", status: "CONFIRMATION_REQUIRED", owner: "USER_OR_BROKER" },
      { code: "ACCOUNT_SHARED_KRW_RECONCILIATION", status: "VERIFICATION_REQUIRED", owner: "PROGRAM" },
      { code: "ACTUAL_FX_AND_SETTLEMENT", status: "VERIFICATION_REQUIRED", owner: "PROGRAM_AND_BROKER" },
      { code: "PAPER_SOURCE_REVALIDATION", status: "VERIFICATION_REQUIRED", owner: "PROGRAM" },
      { code: "MANUAL_LIVE_START", status: "BLOCKED", owner: "USER" },
    ] };
}
async function kisProvisionalView(market = "KRX", quantity = "1", price = "1000000") {
  const view = await preparationView();
  const source = { ...preparationReply().source, instruments: [{ instanceId: "instance-a",
    symbol: market === "US" ? "AAPL" : "005930", broker: market === "US" ? "kis-us" : "kis-kr" }] };
  responseForFetch = async () => response(preparationReply({}, { source }));
  await view.buttons().find((button) => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
  view.render();
  view.elements("select").find((node) => node.props.children?.flat?.().some?.((child) => child?.props?.value === "instance-a"))
    .props.onChange({ target: { value: "instance-a" } });
  view.render();
  view.elements("input")[0].props.onChange({ target: { value: quantity } }); view.render();
  view.elements("input")[1].props.onChange({ target: { value: price } }); view.render();
  return { view, source };
}

test("KR and US local fee alternatives render on explicit no-account preview and stay unconfirmed", async () => {
  const previous = globalThis.window;
  try {
    globalThis.window = preparationWindow([]); allowPreparation = true;
    for (const market of ["KRX", "US"]) {
      const { view, source } = await kisProvisionalView(market);
      let posted;
      responseForFetch = async (_url, options) => {
        posted = JSON.parse(options.body);
        return response(preparationReply({ notional: "1000000" }, { source,
          observation: { status: "NOT_REQUESTED", provisionalFeeEstimate: provisionalPreview(market) } }));
      };
      await view.buttons().find((button) => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
      const html = view.render();
      assert.equal(posted.readAccount, false);
      assert.match(html, /임시 수수료 추정 · 확인 필요 · 실제 적용 요율 미확인/);
      assert.match(html, /주식·ETF 분류가 확인되지 않아 두 대안/);
      assert.match(html, market === "KRX" ? /140\.527 KRW/ : /2500 USD/);
      assert.match(html, market === "KRX" ? /146\.527 KRW/ : /미국 ETF 대안/);
      assert.match(html, /총비용·실제 환전 원화 금액·주문 수량은 확정하지 않습니다/);
      assert.match(html, /실제 주문은 차단 상태/);
      assert.match(html, /국내·해외 공통 원화 한도와 원장 대조/);
      assert.match(html, /직접 실거래 시작 · 현재 차단/);
      assert.doesNotMatch(html, /READY|수수료 확인 완료<|실제 적용 수수료 관측됨/);
      assert.equal(calls.filter((call) => call.method === "GET").length, 1);
      assert.ok(calls.filter((call) => call.method === "POST").every((call) => call.url === "/api/preparation/preview"));
    }
  } finally { allowPreparation = false; globalThis.window = previous; }
});

test("missing notional remains uncalculated and decimal commission is exact below Number precision", async () => {
  const previous = globalThis.window;
  try {
    globalThis.window = preparationWindow([]); allowPreparation = true;
    for (const [quantity, price, notional, estimates] of [
      ["", "", null, [null, null]],
      ["1", "0.000000000000000001", "0.000000000000000001", ["0.000000000000000000000140527", "0.000000000000000000000146527"]],
      ["0.000000000000000001", "0.000000000000000001", "0.000000000000000000000000000000000001", ["0.000000000000000000000000000000000000000140527", "0.000000000000000000000000000000000000000146527"]],
      ["999999999999999999", "999999999999999999", "999999999999999998000000000000000001", ["140526999999999999718946000000000.000140527", "146526999999999999706946000000000.000146527"]],
    ]) {
      const { view, source } = await kisProvisionalView("KRX", quantity, price);
      const preview = provisionalPreview("KRX", notional);
      preview.estimates.forEach((estimate, index) => { estimate.nativeEstimate = estimates[index]; });
      responseForFetch = async () => response(preparationReply({ notional }, { source,
        observation: { status: "NOT_REQUESTED", provisionalFeeEstimate: preview } }));
      await view.buttons().find((button) => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
      const html = view.render();
      assert.match(html, /임시 수수료 추정 · 확인 필요 · 실제 적용 요율 미확인/);
      assert.ok(html.includes(`수수료만 추정 ${estimates[0] ?? "미계산"} KRW`));
      assert.doesNotMatch(html, /추정의 범위·미확인 상태를 확인하지 못했습니다/);
    }
  } finally { allowPreparation = false; globalThis.window = previous; }
});

test("changing an input during fee profile digest discards the old asynchronous preview", async () => {
  const previous = globalThis.window;
  const originalDigest = globalThis.crypto.subtle.digest;
  try {
    globalThis.window = preparationWindow([]); allowPreparation = true;
    const { view, source } = await kisProvisionalView();
    responseForFetch = async () => response(preparationReply({ notional: "1000000" }, { source,
      observation: { status: "NOT_REQUESTED", provisionalFeeEstimate: provisionalPreview() } }));
    let release, digestResult;
    globalThis.crypto.subtle.digest = function (...args) {
      digestResult = originalDigest.apply(this, args);
      return new Promise((resolve) => { release = async () => resolve(await digestResult); });
    };
    const pending = view.buttons().find((button) => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
    for (let attempt = 0; attempt < 30 && !release; attempt += 1) await Promise.resolve();
    assert.ok(release, "preview must await the public profile hash before rendering");
    view.elements("input")[0].props.onChange({ target: { value: "2" } }); view.render();
    await release(); await pending;
    assert.doesNotMatch(view.render(), /조회 시각|임시 수수료 추정 · 확인 필요 · 실제 적용 요율 미확인/);
  } finally {
    globalThis.crypto.subtle.digest = originalDigest;
    allowPreparation = false; globalThis.window = previous;
  }
});

test("fee preview rejects nested private data, invented proofs, mismatched amounts and authority", async () => {
  const previous = globalThis.window;
  try {
    globalThis.window = preparationWindow([]); allowPreparation = true;
    const changes = [
      (p) => { p.orderAuthorityGranted = true; },
      (p) => { p.actualFeeObserved = 0; },
      (p) => { p.accountReadRequired = true; },
      (p) => { p.privateBalance = "PRIVATE_SENTINEL"; },
      (p) => { p.profile.remoteMessage = "PRIVATE_SENTINEL"; },
      (p) => { p.profile.rates[0].accountNumber = "PRIVATE_SENTINEL"; },
      (p) => { p.estimates[0].privateAmount = "PRIVATE_SENTINEL"; },
      (p) => { p.followupSteps[0].remoteMessage = "PRIVATE_SENTINEL"; },
      (p) => { p.profile.actualPersonalFeeObserved = true; },
      (p) => { p.profile.estimateOnly = 1; },
      (p) => { p.profile.rateUnit = "FRACTION"; },
      (p) => { p.profile.sourceClassification = "PERSONAL_AGREEMENT"; },
      (p) => { p.profile.feeProfileHash = "a".repeat(64); },
      (p) => { p.profile.unknownCosts.automaticFxRate = "0"; },
      (p) => { p.profile.rates[0].sourceUrl = "https://example.invalid/PRIVATE_SENTINEL"; },
      (p) => { p.profile.rates[0].rateFraction = "0.0140527"; },
      (p) => { p.estimates[0].nativeEstimate = "0"; },
      (p) => { p.estimates[0].feeProfileHash = "b".repeat(64); },
      (p) => { p.estimates[0].unknownCosts.minimumCommission = "0"; },
      (p) => { p.estimates[0].market = "US"; },
      (p) => { p.estimates[0].product = "UNKNOWN"; },
      (p) => { p.estimates[0].roundingApplied = true; },
      (p) => { p.estimatedNotional = "999"; },
      (p) => { p.followupSteps[1].status = "PASS"; },
      (p) => { p.followupSteps[4].status = "READY"; },
      (p) => { p.estimates.reverse(); },
    ];
    for (const change of changes) {
      const { view, source } = await kisProvisionalView();
      const preview = provisionalPreview(); change(preview);
      responseForFetch = async () => response(preparationReply({ notional: "1000000" }, { source,
        observation: { status: "NOT_REQUESTED", provisionalFeeEstimate: preview } }));
      await view.buttons().find((button) => button.props.children === "원본 확인 · 계좌 조회 없음").props.onClick();
      const html = view.render();
      assert.match(html, /임시 수수료 추정의 범위·미확인 상태를 확인하지 못했습니다/);
      assert.doesNotMatch(html, /조회 시각|PRIVATE_SENTINEL|수수료만 추정/);
    }
  } finally { allowPreparation = false; globalThis.window = previous; }
});
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


test("custom criteria and actual short observation are shown without claiming default long-term qualification", async () => {
  useResponse(result([row({
    validationSummary: { validationProfile: "CUSTOM_THRESHOLD", defaultForwardCriteriaMet: false,
      actualObservationSeconds: 3600, actualObservationDays: 0, actualOrderCount: 2, actualRegimeCount: 1 },
    promotionPolicy: { schemaVersion: "paper-promotion-policy-v1", promotionEligible: true,
      validationProfile: "CUSTOM_THRESHOLD", policyHash: "d".repeat(64),
      thresholds: { minimumObservationSeconds: 3600, minimumOrders: 2, minimumRegimes: 1 }, baselineDifferences: [] },
  })]));
  const view = harness();
  await view.refresh();
  const html = view.render();
  assert.match(html, /사용자 설정 기준 통과 · CUSTOM_THRESHOLD/);
  assert.match(html, /3,600초/);
  assert.match(html, /기본 forward 기준[^<]*: 미충족/);
  assert.match(html, /이 후보의 봉인 승급 기준/);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "GET");
});

test("malformed or non-promotional policy projection clears candidate actions", async () => {
  useResponse(result([row({ promotionPolicy: { schemaVersion: "paper-promotion-policy-v1", promotionEligible: false } })]));
  const view = harness();
  await view.refresh();
  assert.match(view.render(), /応答|응답 형식/);
  assert.doesNotMatch(view.render(), /검증된 전략 A/);
  assert.equal(calls.length, 1);
});


test("account review requires explicit saved-account choice, existing one-use native confirmation and stays monitor", async () => {
  const previous = globalThis.window; let selection; let reviewed; let challengeRequest; let reviewPost;
  const candidate = row({ canReview: true, registered: true, brokerId: "binance", rootKey: "root-a",
    reviewRequest: { rootKey: "root-a", evidenceId: "paper-evidence-a", deploymentId: "draft-a", identityHash: "e".repeat(64), expectedRevision: 1, registryHash: "f".repeat(64) } });
  const cleanup = api.registerSafetyConfirmationPresenter(async (challenge) => ({ confirmed: true, typedPhrase: challenge.expectedPhrase }));
  try {
    globalThis.window = preparationWindow([]); allowReview = true;
    responseForFetch = async (url, options) => {
      if (url === "/api/paper-candidates") return response(result([candidate]));
      if (url === "/api/paper-candidates/accounts") return response({ ok: true, schemaVersion: "live-saved-account-profiles-v1", readOnly: true, brokerCalls: 0, ordersSubmitted: 0,
        accounts: [{ profileId: "account-a", brokerId: "binance", alias: "Binance Spot", maskedAccount: "••••ABCD", balanceQueried: false }] });
      if (url === "/api/safety-confirmation/challenge") {
        challengeRequest = JSON.parse(options.body);
        return response({ ok: true, challengeId: "challenge-a", token: "opaque-token-a", expectedPhrase: "LIVE ABCD", expiresAt: new Date(Date.now() + 90000).toISOString(), displayContext: { totalCapital: "주문가능액 100%" } });
      }
      if (url === "/api/paper-candidates/review") {
        reviewPost = JSON.parse(options.body);
        return response({ ok: true, schemaVersion: "live-paper-candidate-review-v1", lifecycle: "before-live-small", mode: "MONITOR", authorizationGranted: false, realOrdersArmed: false,
          brokerCalls: 0, ordersSubmitted: 0, deploymentId: "account-bound-a", detail: "제한 실거래 준비 완료" });
      }
      throw new Error("unexpected request");
    };
    calls = [];
    const view = harness({ onRegistered: async (id) => { reviewed = id; } });
    await view.refresh(); view.render();
    const button = () => view.buttons().find(item => item.props.children === "계좌 연결·제한 실거래 준비 확인");
    assert.equal(button().props.disabled, true);
    await view.buttons().find(item => item.props.children === "저장 계좌 새로고침").props.onClick(); view.render();
    selection = view.elements("select").find(item => item.props["aria-label"] === "계좌 선택 paper-evidence-a");
    assert.equal(selection.props.value, "");
    selection.props.onChange({ target: { value: "account-a" } }); view.render();
    assert.equal(button().props.disabled, false);
    await button().props.onClick();
    assert.equal(reviewed, "account-bound-a");
    assert.equal(challengeRequest.action, "PAPER_CANDIDATE_REVIEW");
    assert.equal(reviewPost.accountProfileId, "account-a");
    assert.equal(reviewPost.capitalPercent, 100);
    assert.equal(reviewPost.capitalBasis, "ACCOUNT_AVAILABLE_CASH");
    assert.equal(reviewPost.safety_confirmation.typedPhrase, "LIVE ABCD");
    assert.equal(calls.filter(item => item.method === "POST").length, 2);
    assert.match(view.render(), /제한 실거래 준비 완료/);
  } finally { cleanup(); allowReview = false; globalThis.window = previous; }
});

function sealedPreparationSources() {
  const source = { kind: "SEALED_SOURCE", evidenceClass: "PAPER_APPROVED_EVIDENCE_PENDING", proofPending: true,
    qualification: "PAPER_EVIDENCE_PENDING", approvalBindingStatus: "HISTORICAL_APPROVAL_SOURCE_HASH_NOT_RECORDED",
    liveAuthorized: false, authorizationGranted: false, promotionEligible: false, useAsPromotionEvidence: false, orderSubmissionEnabled: false, armingEnabled: false };
  return { ...preparationSourcesReply(), sources: Array.from({length:20},(_,i)=>({name:`원본 ${i}`,
    source:{...source,targetType:i<12?"INSTANCE":"PORTFOLIO",targetId:`target-${i}`,targetHash:`hash-${i}`,identityHash:`identity-${i}`},
    request:{kind:"SEALED_SOURCE",rootKey:"sealed-root",targetType:i<12?"INSTANCE":"PORTFOLIO",targetId:`target-${i}`,targetHash:`hash-${i}`,identityHash:`identity-${i}`}})) };
}
test("20 sealed research targets remain unique pending choices with explicit historical approval hash gap",async()=>{
 const previous=globalThis.window;
 try {globalThis.window=preparationWindow([]);allowPreparation=true;useResponse(sealedPreparationSources());const view=harness();
  await view.buttons().find(b=>b.props.children==="준비 자료 새로고침").props.onClick();view.render();
  const selector=view.elements("select").find(n=>n.props["aria-label"]==="준비할 원본");
  const options=elements(selector,"option").filter(n=>n.props.value);assert.equal(options.length,20);assert.equal(new Set(options.map(n=>n.props.value)).size,20);
  selector.props.onChange({target:{value:options[0].props.value}});view.render();
  const data=sealedPreparationSources().sources[0];responseForFetch=async()=>response(preparationReply({}, {source:{...data.source,instruments:[{instanceId:"instance-a",symbol:"BTCUSDT",broker:"binance"}]}}));
  await view.buttons().find(b=>b.props.children==="원본 확인 · 계좌 조회 없음").props.onClick();const html=view.render();
  assert.match(html,/실제 Paper 승급 근거 대기/);assert.match(html,/Source hash 결합이 기록되지 않았습니다/);assert.match(html,/권한 없음 · 실행 불가/);
  assert.equal(calls.filter(c=>c.method==="POST").length,1);assert.ok(calls.every(c=>!c.url.endsWith("/import")&&!c.url.endsWith("/review")));
 } finally {allowPreparation=false;globalThis.window=previous;}
});
test("sealed source changes and authority-bearing source responses clear local preparation",async()=>{
 const previous=globalThis.window;
 try {globalThis.window=preparationWindow([]);allowPreparation=true;
  for(const mutation of [{targetHash:"changed"},{liveAuthorized:true},{proofPending:false}]){
   useResponse(sealedPreparationSources());const view=harness();await view.buttons().find(b=>b.props.children==="준비 자료 새로고침").props.onClick();view.render();
   view.elements("select").find(n=>n.props["aria-label"]==="준비할 원본").props.onChange({target:{value:"sealed:sealed-root:INSTANCE:target-0"}});view.render();
   responseForFetch=async()=>response(preparationReply({}, {source:{...sealedPreparationSources().sources[0].source,...mutation,instruments:[{instanceId:"instance-a",symbol:"BTCUSDT",broker:"binance"}]}}));
   await view.buttons().find(b=>b.props.children==="원본 확인 · 계좌 조회 없음").props.onClick();assert.match(view.render(),/범위와 유효시간을 확인하지 못했습니다/);assert.doesNotMatch(view.render(),/조회 시각/);
  }
 } finally {allowPreparation=false;globalThis.window=previous;}
});


test("native source preview separates initial cash budget from current owned equity and never displays unknown quantity as executable", async () => {
  const previous=globalThis.window;
  try {
    globalThis.window=preparationWindow([]);allowPreparation=true;
    for(const mutatedMaximum of [null,"0.1"]) {
      useResponse(sealedPreparationSources());const view=harness();await view.buttons().find(b=>b.props.children==="준비 자료 새로고침").props.onClick();view.render();
      view.elements("select").find(n=>n.props["aria-label"]==="준비할 원본").props.onChange({target:{value:"sealed:sealed-root:INSTANCE:target-0"}});view.render();
      responseForFetch=async()=>response(preparationReply({}, {source:{...sealedPreparationSources().sources[0].source,instruments:[{instanceId:"instance-a",symbol:"BTCUSDT",broker:"binance"}]},observation:{status:"OBSERVED",cashPreview:{capitalBasis:"PORTFOLIO_EQUITY_WITH_CASH_CAP",initialBudgetBasis:"ACCOUNT_AVAILABLE_CASH",capitalPercent:100,sourceMaxOrderPercent:5,allocationWeight:0.25,status:"BLOCKED",nativeEligibility:"UNKNOWN",maximumQuantity:mutatedMaximum,executable:false,orderSubmissionEnabled:false,armingEnabled:false,reasons:["owned deployment equity pending"]}}}));
      await view.buttons().find(b=>b.props.children==="원본 확인 · 계좌 조회 없음").props.onClick();const html=view.render();
      if(mutatedMaximum===null) {assert.match(html,/배포에 배정된 현재 자본/);assert.match(html,/초기 총 사용 한도/);assert.match(html,/UNKNOWN/);assert.match(html,/차단·미확정/);}
      else {assert.match(html,/범위와 유효시간을 확인하지 못했습니다/);assert.doesNotMatch(html,/조회 시각/);}
    }
  } finally {allowPreparation=false;globalThis.window=previous;}
});
