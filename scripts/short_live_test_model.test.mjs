// Pure fixture tests. No account, native bridge, app server, or broker API is opened.
import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import {
  SHORT_LIVE_CONFIRMATION, buildShortLivePrepare, exactShortLiveTarget, normalizeShortLiveCandidate,
  normalizeShortLiveSnapshot, sanitizeShortLiveText, shortLiveCanStart, shortLiveCoverage,
  shortLiveRuntimeMeaning, shortLiveSelectionSignature, shortLivePreparationMatches, shortLiveBlockerLabel,
  shortLivePreparationWindow, shortLiveFilterCandidates, shortLiveIsBoundedCandidate, shortLiveRunning,
} from "../src/shortLiveTestModel.js";

const instance = (changes = {}) => ({
  selectionKey: "STRATEGY_INSTANCE:fixture-1:a", kind: "STRATEGY_INSTANCE", id: "fixture-1",
  route: "KIS_KR_CASH", routes: ["KIS_KR_CASH"], timeframe: "5m", timeframes: ["5m"],
  label: "Fixture 국내 전략", artifactFileSha256: "a".repeat(64), instanceFileSha256: "b".repeat(64),
  memberCount: 1, pluginIds: ["moving_average_cross"], symbols: ["005930"],
  eligibleForFunctionalPreparation: true, blockers: [], ...changes,
});
const portfolio = (changes = {}) => ({
  selectionKey: "PORTFOLIO_INSTANCE:fixture-portfolio:c", kind: "PORTFOLIO_INSTANCE", id: "fixture-portfolio",
  route: "UPBIT_KRW_SPOT", routes: ["UPBIT_KRW_SPOT"], timeframe: "1h", timeframes: ["1h"],
  label: "Fixture 다전략", artifactFileSha256: "c".repeat(64), memberCount: 3,
  pluginIds: ["moving_average_cross", "rsi_revert"], eligibleForFunctionalPreparation: true, blockers: [], ...changes,
});
const ready = () => normalizeShortLiveSnapshot({ status: "PREPARED", catalog: { candidates: [instance()] },
  blockers: [], prepared: { preparationId: "fixture-preparation", scopeHash: "d".repeat(64), readyToStart: true,
    durationHours: 6, capitalKrwPerBroker: 100000, dailyLossStopKrwPerBroker: 50000, targets: [instance()], expiresAt: "2099-01-01T00:00:00Z", blockers: [] } });
const options = { fresh: true, acknowledged: true, confirmationText: SHORT_LIVE_CONFIRMATION,
  selectionUnchanged: true, preparedAuthority: `fixture-preparation:${"d".repeat(64)}`, preparedScopeMatches: true };

test("normalizes exact backend catalog without claiming live qualification", () => {
  const row = normalizeShortLiveCandidate(instance());
  assert.equal(row.kind, "instance"); assert.equal(row.instanceId, "fixture-1");
  assert.equal(row.available, true); assert.equal(row.distinctPluginCount, 1);
  assert.match(row.evidenceMeaning, /수익성/);
  assert.equal(normalizeShortLiveCandidate({ available: "true" }).available, false);
});
test("portfolio member count and distinct plugins remain separate", () => {
  const row = normalizeShortLiveCandidate(portfolio());
  assert.equal(row.strategyCount, 3); assert.equal(row.distinctPluginCount, 2);
  assert.equal(row.portfolioFileSha256, "c".repeat(64));
});
test("24 coverage cells include missing futures and daily combinations", () => {
  const rows = shortLiveCoverage([normalizeShortLiveCandidate(instance()), normalizeShortLiveCandidate(portfolio())]);
  assert.equal(rows.length, 24);
  const missing = rows.find((row) => row.route === "BINANCE_USDM_FUTURES" && row.timeframe === "1d" && row.kind === "portfolio");
  assert.equal(missing.count, 0); assert.deepEqual(missing.blockers, ["candidate-missing"]);
});
test("mixed timeframe portfolio is counted only in declared cells", () => {
  const row = normalizeShortLiveCandidate(portfolio({ timeframe: "MULTI_TIMEFRAME", timeframes: ["5m", "1h"] }));
  const cells = shortLiveCoverage([row]).filter((cell) => cell.count);
  assert.deepEqual(cells.map((cell) => cell.timeframe), ["5m", "1h"]);
  assert.equal(exactShortLiveTarget(row).portfolioId, "fixture-portfolio");
});
test("prepare pins selected originals and keeps futures changes disabled", () => {
  const catalog = [normalizeShortLiveCandidate(instance()), normalizeShortLiveCandidate(portfolio())];
  const body = buildShortLivePrepare(catalog, catalog.map((row) => row.selectionKey));
  assert.equal(body.durationHours, 6); assert.equal(body.capitalKrwPerBroker, 100000);
  assert.equal(body.dailyLossStopKrwPerBroker, 50000); assert.equal(body.selections.length, 2);
  assert.equal(body.selections[0].instanceFileSha256, "b".repeat(64));
  assert.deepEqual(body.futuresPreparation, { leverage: 1, marginMode: "ISOLATED", applySettings: false });
});
test("prepare rejects missing and duplicate exact selections", () => {
  assert.throws(() => buildShortLivePrepare([], []));
  assert.throws(() => buildShortLivePrepare([instance(), instance()], [instance().selectionKey]));
  assert.throws(() => buildShortLivePrepare([instance()], ["missing"]));
  assert.throws(() => buildShortLivePrepare([instance()], [instance().selectionKey, instance().selectionKey]));
});
test("missing source pins, unsupported routes and timeframes cannot prepare", () => {
  assert.throws(() => exactShortLiveTarget(instance({ instanceFileSha256: "" })));
  assert.throws(() => exactShortLiveTarget(portfolio({ artifactFileSha256: "invalid" })));
  assert.throws(() => exactShortLiveTarget(instance({ routes: ["KIS_US_CASH"] })));
  assert.throws(() => exactShortLiveTarget(instance({ timeframes: ["1m"] })));
});
test("only 5-7 hours and fixed authorized ceilings are accepted", () => {
  for (const durationHours of [5, 6, 7]) assert.equal(buildShortLivePrepare([instance()], [instance().selectionKey],
    { durationHours, budgetKrwPerBroker: 100000, lossKrwPerBroker: 50000 }).durationHours, durationHours);
  for (const durationHours of [0, 4, 8, 6.5, NaN]) assert.throws(() => buildShortLivePrepare([instance()], [instance().selectionKey],
    { durationHours, budgetKrwPerBroker: 100000, lossKrwPerBroker: 50000 }));
  assert.throws(() => buildShortLivePrepare([instance()], [instance().selectionKey], { durationHours: 6, budgetKrwPerBroker: 200000, lossKrwPerBroker: 50000 }));
});
test("start requires fresh exact prepared authority, phrase and manual acknowledgement", () => {
  assert.equal(shortLiveCanStart(ready(), options), true);
  for (const changes of [{ fresh: false }, { acknowledged: false }, { selectionUnchanged: false },
    { confirmationText: "yes" }, { preparedAuthority: "old-preparation" }, { preparedScopeMatches: false }]) {
    assert.equal(shortLiveCanStart(ready(), { ...options, ...changes }), false);
  }
});
test("server blockers, missing hash and active runtime block start", () => {
  for (const mutate of [
    (state) => { state.blockers = ["insufficient-balance"]; },
    (state) => { state.prepared.blockers = ["market-closed"]; },
    (state) => { state.prepared.scopeHash = "invalid"; },
    (state) => { state.prepared.readyToStart = false; },
    (state) => { state.runtime = { status: "RUNNING" }; },
  ]) { const state = ready(); mutate(state); assert.equal(shortLiveCanStart(state, options), false); }
});
test("catalog pin change invalidates the local preparation selection", () => {
  const before = shortLiveSelectionSignature([instance()], [instance().selectionKey], 6);
  assert.notEqual(before, shortLiveSelectionSignature([instance({ instanceFileSha256: "e".repeat(64) })], [instance().selectionKey], 6));
  assert.notEqual(before, shortLiveSelectionSignature([instance()], [instance().selectionKey], 7));
});
test("prepared scope matches exact selected originals, amount and time", () => {
  const state = ready(); const key = instance().selectionKey;
  assert.equal(shortLivePreparationMatches(state.prepared, state.catalog, [key], 6), true);
  state.prepared.targets[0].instanceFileSha256 = "e".repeat(64);
  assert.equal(shortLivePreparationMatches(state.prepared, state.catalog, [key], 6), false);
  assert.equal(shortLivePreparationMatches(ready().prepared, ready().catalog, [key], 7), false);
  const missing = ready(); missing.prepared.targets = [];
  assert.equal(shortLivePreparationMatches(missing.prepared, missing.catalog, [key], 6), false);
});
test("unknown values and boolean claims are not silently promoted", () => {
  const state = normalizeShortLiveSnapshot({ runtime: { terminalVerified: "true", ownerAlive: "true", unknownOrderCount: "0" }, prepared: { readyToStart: "true" } });
  assert.equal(state.runtime.terminalVerified, false); assert.equal(state.runtime.ownerAlive, null);
  assert.equal(state.runtime.unknownOrderCount, null); assert.equal(state.prepared.readyToStart, false);
});
test("backend runtime aliases retain only public metrics without inventing fills", () => {
  const state = normalizeShortLiveSnapshot({ runtime: {
    brokerRows: [{ route: "BINANCE_SPOT", ready: true, exposureKrw: 1000, realizedLossKrw: 20,
      unrealizedLossKrw: 30, routeBudgetKrw: 50000, dailyLossStopKrw: 50000,
      ownedOrders: [{ accountId: "private" }], ownedPositions: [{ private: true }], positionsBySymbol: { private: true } }],
    strategyRows: [{ instanceId: "exact-fixture-instance", timeframe: "1h", lastSignal: "HOLD", decisionCount: 5 }],
  } });
  assert.equal(state.runtime.brokers[0].dailyRealizedLossKrw, 20);
  assert.equal(state.runtime.brokers[0].unrealizedLossKrw, 30);
  assert.equal(state.runtime.strategies[0].decisionCount, 5);
  assert.equal(state.runtime.strategies[0].signalCount, null);
  assert.equal(state.runtime.strategies[0].orderCount, null);
  assert.equal(state.runtime.strategies[0].fillCount, null);
  assert.doesNotMatch(JSON.stringify(state.runtime), /private|ownedPositions|ownedOrders|positionsBySymbol/);
});
test("daily absence is a normal missing observation rather than PASS", () => {
  assert.match(shortLiveRuntimeMeaning({ timeframe: "1d", signalCount: 0 }), /미관측/);
  assert.match(shortLiveRuntimeMeaning({ timeframe: "1d", signalCount: 0 }), /통과·실패를 뜻하지/);
});
test("private account and token text is masked before display", () => {
  const result = sanitizeShortLiveText("accountNo=12345678-01 api_key=private-value Bearer private-token");
  assert.doesNotMatch(result, /12345678|private-value|private-token/);
});

test("actual backend blocker codes describe account funding, KIS session/cost and futures settings in Korean", () => {
  for (const [code,meaning] of [
    ["KIS_KR_CASH:KRX_SESSION_CLOSED",/국내 정규장.*닫혀/],
    ["KIS_KR_CASH:KIS_CONFIRMED_COST_POLICY_REQUIRED",/수수료·매도세율/],
    ["BINANCE_USDM_FUTURES:FUTURES_SETTINGS_REQUIRE_1X_ISOLATED",/1배.*격리/],
    ["UPBIT_KRW_SPOT:NO_AVAILABLE_BALANCE",/잔고 없음/],
    ["START_BLOCKED:KIS_KR_CASH:KRX_SESSION_CLOSED,UPBIT_KRW_SPOT:AVAILABLE_BELOW_NATIVE_MINIMUM",/국내 정규장.*최소 주문/],
  ]) {
    const label=shortLiveBlockerLabel(code); assert.match(label,meaning); assert.doesNotMatch(label,/[A-Z_]{5,}/);
  }
  assert.match(shortLiveBlockerLabel("FUTURE_UNKNOWN_SAFE_CODE"),/실행 조건을 확인하지 못함/);
  assert.equal(shortLiveBlockerLabel("선택한 범위를 다시 확인하세요."),"선택한 범위를 다시 확인하세요.");
});
test("panel mutates only from explicit event handlers and never retries unknown starts", () => {
  const panel = readFileSync(new URL("../src/ShortLiveTestPanel.jsx", import.meta.url), "utf8");
  assert.match(panel, /onClick=\{prepare\}/); assert.match(panel, /onClick=\{start\}/); assert.match(panel, /onClick=\{stop\}/);
  assert.match(panel, /setUnknownMutation\("start"\)/); assert.match(panel, /setUnknownMutation\("stop"\)/);
  const effect = panel.match(/useEffect\(\(\) => \{\n    mounted\.current = true;([\s\S]*?)\n  \}, \[refresh, pollMs\]\);/)?.[1];
  assert.ok(effect); assert.doesNotMatch(effect, /api\.(start|stop|prepare)/);
  assert.doesNotMatch(panel, /window\.confirm|submitTestIntent|setFlag|applyBinanceFuturesSettings/);
});

test("source position percentages remain explicit, including old 5 percent and new 100 percent", () => {
  assert.deepEqual(normalizeShortLiveCandidate(instance({ positionSizePercents: [5] })).positionSizePercents, [5]);
  assert.deepEqual(normalizeShortLiveCandidate(instance({ positionSizePercents: [100, 100] })).positionSizePercents, [100]);
  assert.deepEqual(normalizeShortLiveCandidate(instance({ positionSizePercents: [false, "100", null, -1, 101, NaN] })).positionSizePercents, []);
  assert.deepEqual(normalizeShortLiveCandidate(instance()).positionSizePercents, []);
  const panel = readFileSync(new URL("../src/ShortLiveTestPanel.jsx", import.meta.url), "utf8");
  assert.match(panel, /원래 5% 후보는 5% 그대로/);
});

test("hourly shortcut filters by exact declared profile while retaining every candidate", () => {
  const bounded = normalizeShortLiveCandidate(instance({ timeframe: "1h", timeframes: ["1h"], functionalSizingProfiles: ["BOUNDED_TEST_SLEEVE_100_PERCENT_V1"] }));
  const old = normalizeShortLiveCandidate(instance({ selectionKey: "old", label: "소액 배정금 100% 기능시험", timeframe: "1h", timeframes: ["1h"], positionSizePercents: [100] }));
  const daily = normalizeShortLiveCandidate(instance({ selectionKey: "daily", timeframe: "1d", timeframes: ["1d"], functionalSizingProfiles: ["BOUNDED_TEST_SLEEVE_100_PERCENT_V1"] }));
  const catalog = [bounded, old, daily];
  assert.equal(shortLiveIsBoundedCandidate(old), false);
  assert.deepEqual(shortLiveFilterCandidates(catalog, { timeframe: "1h", profile: "BOUNDED" }), [bounded]);
  assert.deepEqual(shortLiveFilterCandidates(catalog, { profile: "ORIGINAL" }), [old]);
  assert.deepEqual(shortLiveFilterCandidates(catalog), catalog); assert.equal(catalog.length, 3);
});
test("preparation expires at the exact boundary and missing expiry never enables start", () => {
  const state = ready(); state.prepared.expiresAt = "2026-10-24T12:00:00Z";
  const expiry = Date.parse(state.prepared.expiresAt);
  assert.deepEqual(shortLivePreparationWindow(state.prepared, expiry - 1), { status: "VALID", remainingSeconds: 1 });
  assert.deepEqual(shortLivePreparationWindow(state.prepared, expiry), { status: "EXPIRED", remainingSeconds: 0 });
  assert.equal(shortLiveCanStart(state, { ...options, now: expiry - 1 }), true);
  assert.equal(shortLiveCanStart(state, { ...options, now: expiry }), false);
  state.prepared.expiresAt = ""; assert.equal(shortLiveCanStart(state, options), false);
});
test("actual route budget, attempts and fill observations stay distinct from inferred trade counts", () => {
  const state = normalizeShortLiveSnapshot({ runtime: { brokerRows: [{ route: "BINANCE_SPOT", effectiveRouteBudgetKrw: 23000, brokerBudgetKrw: 100000, routeBudgetKrw: 50000, lossBasis: "ACCOUNT_NATIVE_EQUITY_DECLINE_AT_FRESH_FX", actualFeeKnown: false }], strategyRows: [{ instanceId: "actual-instance", route: "BINANCE_SPOT", pluginId: "rsi_revert", symbol: "BTCUSDT", timeframe: "1h", decisionCount: 4, orderAttemptCount: 2, fillObservationCount: 3, internalAllocationTransferCount: 1 }] } });
  assert.equal(state.runtime.brokers[0].budgetKrw, 23000); assert.equal(state.runtime.brokers[0].brokerBudgetKrw, 100000);
  assert.equal(state.runtime.brokers[0].actualFeeKnown, false);
  const row = state.runtime.strategies[0];
  assert.equal(row.orderAttemptCount, 2); assert.equal(row.fillObservationCount, 3);
  assert.equal(row.orderCount, null); assert.equal(row.fillCount, null); assert.equal(row.signalCount, null);
  assert.equal(row.internalAllocationTransferCount, 1); assert.match(shortLiveRuntimeMeaning(row), /고유 주문 수와 다릅니다/);
});
test("pending, blocked, drained and unknown sessions retain the preparation lock", () => {
  for (const status of ["PENDING_FILL", "BLOCKED", "DRAINED", "UNKNOWN", "TERMINATED"]) assert.equal(shortLiveRunning({ runtime: { status } }), true);
  assert.equal(shortLiveRunning({ runtime: null }), false);
  assert.equal(shortLiveRunning({ runtime: { status: "TERMINATED", terminalVerified: true, naturalExitObserved: true } }), false);
  assert.equal(shortLiveRunning({ runtime: { status: "TERMINATED", terminalVerified: true, naturalExitObserved: false, workerRetirementRequired: false, workerLaunchState: "NOT_ATTEMPTED", workerAlive: false } }), false);
  assert.equal(shortLiveRunning({ runtime: { status: "TERMINATED", terminalVerified: true, naturalExitObserved: false, workerRetirementRequired: false, workerLaunchState: "NOT_ATTEMPTED" } }), true);
  assert.equal(shortLiveRunning({ runtime: { status: "TERMINATED", terminalVerified: true, naturalExitObserved: false, workerRetirementRequired: false, workerLaunchState: "START_FAILED_UNVERIFIED" } }), true);
});
test("hourly waiting and HOLD convey observed evaluation without inventing signal counts", () => {
  assert.match(shortLiveRuntimeMeaning({ timeframe: "1h", decisionCount: 0 }), /1시간봉 평가 대기/);
  assert.match(shortLiveRuntimeMeaning({ timeframe: "1h", decisionCount: 3, signal: "HOLD" }), /완성봉 평가됨/);
  assert.doesNotMatch(shortLiveRuntimeMeaning({ timeframe: "1h", decisionCount: null }), /평가됨/);
});
test("daily actual signals and fills take precedence over unavailable signal counters", () => {
  assert.match(shortLiveRuntimeMeaning({ timeframe: "1d", signalCount: null, signal: "BUY" }), /신호가 관측/);
  assert.doesNotMatch(shortLiveRuntimeMeaning({ timeframe: "1d", signalCount: null, signal: "BUY" }), /신호 미관측/);
  assert.match(shortLiveRuntimeMeaning({ timeframe: "1d", signalCount: null, fillObservationCount: 2 }), /체결 관측 기록/);
  assert.match(shortLiveRuntimeMeaning({ timeframe: "1d", signalCount: null }), /여부 미확인/);
});
