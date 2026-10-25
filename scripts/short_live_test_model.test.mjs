// Pure fixture tests. No account, native bridge, app server, or broker API is opened.
import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import {
  buildShortLivePrepare, exactShortLiveTarget, normalizeShortLiveCandidate,
  normalizeShortLiveSnapshot, mergeShortLiveSnapshot, sanitizeShortLiveText, shortLiveReadyForConsent, shortLiveCoverage,
  shortLiveRuntimeMeaning, shortLiveSelectionSignature, shortLivePreparationMatches, shortLiveBlockerLabel,
  shortLivePreparationWindow, shortLiveFilterCandidates, shortLiveIsBoundedCandidate, shortLiveRunning,
  normalizeShortLiveFuturesPolicy, shortLiveProtectionLabel, shortLiveFuturesIncidentLabel,
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
const options = { fresh: true,
  selectionUnchanged: true, preparedAuthority: `fixture-preparation:${"d".repeat(64)}`, preparedScopeMatches: true };

test("fast status preserves only display candidates and replaces all preparation authority", () => {
  const before = ready();
  const status = { status: "PREPARED", catalogIncluded: false, catalog: [], observedAt: "2099-01-01T00:00:01Z",
    prepared: { ...before.prepared, readyToStart: false, blockers: ["PREPARATION_EXPIRED"] },
    runtime: { sessionId: "new-runtime", status: "BLOCKED", ownerAlive: false }, blockers: ["FRESH_CLOCK_GUARD_UNKNOWN"] };
  const merged = mergeShortLiveSnapshot(status, before.catalog);
  assert.equal(merged.catalog, before.catalog);
  assert.equal(merged.prepared.readyToStart, false);
  assert.deepEqual(merged.prepared.blockers, ["PREPARATION_EXPIRED"]);
  assert.equal(merged.runtime.ownerAlive, false);
  assert.deepEqual(merged.blockers, ["FRESH_CLOCK_GUARD_UNKNOWN"]);
  assert.equal(shortLiveReadyForConsent(merged, options), false);
  assert.deepEqual(mergeShortLiveSnapshot({ catalog: [], prepared: null, runtime: null }, before.catalog).catalog, []);
});

test("prepared funding observations expose numeric quote funds without account identities", () => {
  const view = normalizeShortLiveSnapshot({ prepared: { brokerReadiness: [{ route: "UPBIT_KRW_SPOT", ready: false,
    fundingBasis: "AVAILABLE_QUOTE_ONLY", availableQuoteNative: "12345.6", effectiveRouteBudgetKrw: "12345.6",
    quoteCurrency: "KRW", preservedAssetSymbols: ["BTC"], accountId: "private-fixture", rawAccount: "private-fixture" }] } });
  assert.equal(view.prepared.brokerReadiness[0].availableQuoteNative, 12345.6);
  assert.equal(view.prepared.brokerReadiness[0].ready, false);
  assert.deepEqual(view.prepared.brokerReadiness[0].preservedAssetSymbols, ["BTC"]);
  assert.equal(JSON.stringify(view).includes("private-fixture"), false);
});

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
test("spot preparation pins selected originals without a futures configuration mutation", () => {
  const catalog = [normalizeShortLiveCandidate(instance()), normalizeShortLiveCandidate(portfolio())];
  const body = buildShortLivePrepare(catalog, catalog.map((row) => row.selectionKey));
  assert.equal(body.durationHours, 6); assert.equal(body.capitalKrwPerBroker, 100000);
  assert.equal(body.dailyLossStopKrwPerBroker, 50000); assert.equal(body.selections.length, 2);
  assert.equal(body.selections[0].instanceFileSha256, "b".repeat(64));
  assert.equal(body.futuresPolicy, undefined);
  assert.equal(body.futuresPreparation, undefined);
});

test("futures preparation requires explicit valid protection policy and preserves real capital", () => {
  const row = instance({ route: "BINANCE_USDM_FUTURES", routes: ["BINANCE_USDM_FUTURES"] });
  const limits = { durationHours: 6, budgetKrwPerBroker: 100000, lossKrwPerBroker: 50000 };
  const policy = { leverage: 5, marginMode: "ISOLATED", stopLossPercent: 2 };
  assert.throws(() => buildShortLivePrepare([row], [row.selectionKey], limits), /손절/);
  for (const bad of [null, { ...policy, stopLossPercent: "" }, { ...policy, stopLossPercent: 0 },
    { ...policy, stopLossPercent: 20 }, { ...policy, leverage: true }, { ...policy, leverage: 5.5 },
    { ...policy, leverage: 126 }, { ...policy, marginMode: "CROSSED" }]) {
    assert.equal(normalizeShortLiveFuturesPolicy(bad), null);
    assert.throws(() => buildShortLivePrepare([row], [row.selectionKey], { ...limits, futuresPolicy: bad }));
  }
  const body = buildShortLivePrepare([row], [row.selectionKey], { ...limits, futuresPolicy: policy });
  assert.deepEqual(body.futuresPolicy, policy);
  assert.equal(body.capitalKrwPerBroker, 100000);
  assert.equal(body.dailyLossStopKrwPerBroker, 50000);
  assert.equal(body.futuresPreparation, undefined);
  assert.equal(JSON.stringify(body).includes("applySettings"), false);
});

test("futures policy changes invalidate preparation binding before a new consent click", () => {
  const row = instance({ route: "BINANCE_USDM_FUTURES", routes: ["BINANCE_USDM_FUTURES"] });
  const policy = { leverage: 5, marginMode: "ISOLATED", stopLossPercent: 2 };
  const prepared = { durationHours: 6, budgetKrwPerBroker: 100000, lossKrwPerBroker: 50000,
    targets: [row], futuresPolicy: policy };
  assert.equal(shortLivePreparationMatches(prepared, [row], [row.selectionKey], 6, policy), true);
  assert.equal(shortLivePreparationMatches(prepared, [row], [row.selectionKey], 6), false);
  for (const changed of [{ ...policy, leverage: 3 }, { ...policy, stopLossPercent: 1 }, { ...policy, stopLossPercent: "" }]) {
    assert.equal(shortLivePreparationMatches(prepared, [row], [row.selectionKey], 6, changed), false);
    assert.notEqual(shortLiveSelectionSignature([row], [row.selectionKey], 6, policy),
      shortLiveSelectionSignature([row], [row.selectionKey], 6, changed));
  }
  assert.equal(shortLivePreparationMatches({ ...prepared, futuresPolicy: null }, [row], [row.selectionKey], 6, policy), false);
});

test("futures observations separate margin, notional and actual exchange liquidation without leaking raw accounts", () => {
  const view = normalizeShortLiveSnapshot({ prepared: { futuresPolicy: { leverage: 5, marginMode: "ISOLATED", stopLossPercent: 2 },
    brokerReadiness: [{ route: "BINANCE_USDM_FUTURES", observedFuturesSettingsBySymbol: {
      SOLUSDT: { marginMode: "ISOLATED", leverage: "5", positionMode: "HEDGE", accountId: "private" } } }] },
    runtime: { brokerRows: [{ route: "BINANCE_USDM_FUTURES", committedCapitalKrw: "1000", notionalExposureKrw: "5000",
      protections: [{ symbol: "SOLUSDT", positionSide: "LONG", status: "NEW", triggerPrice: "98", verified: true, raw: "private" }],
      futuresPositions: [{ symbol: "SOLUSDT", liquidationPrice: "80.5", markPrice: "100", leverage: "5", accountId: "private" }],
      futuresIncidents: [{ type: "LIQUIDATION", symbol: "SOLUSDT", quantity: "0.5", verified: true, raw: "private" }] }] } });
  assert.equal(view.prepared.futuresPolicy.leverage, 5);
  assert.equal(view.prepared.brokerReadiness[0].observedFuturesSettingsBySymbol.SOLUSDT.leverage, 5);
  const broker = view.runtime.brokers[0];
  assert.equal(broker.committedCapitalKrw, 1000); assert.equal(broker.notionalExposureKrw, 5000);
  assert.equal(broker.protections[0].verified, true); assert.equal(broker.protections[0].triggerPrice, 98);
  assert.equal(broker.futuresPositions[0].actualLiquidationPrice, 80.5);
  assert.equal(broker.futuresIncidents[0].type, "LIQUIDATION");
  assert.doesNotMatch(JSON.stringify(view), /private|accountId|raw/);
  assert.match(shortLiveProtectionLabel("UNKNOWN"), /미확인/);
  assert.match(shortLiveFuturesIncidentLabel("ADL"), /디레버리징/);
  const unknown = normalizeShortLiveSnapshot({ runtime: { brokerRows: [{ route: "BINANCE_USDM_FUTURES",
    protections: [{ status: "NEW", verified: "true" }], futuresIncidents: [{ type: "ADL", verified: "true" }] }] } });
  assert.equal(unknown.runtime.brokers[0].protections[0].verified, false);
  assert.equal(unknown.runtime.brokers[0].futuresIncidents[0].verified, false);
  const codeOnly = normalizeShortLiveSnapshot({ runtime: { brokerRows: [{ route: "BINANCE_USDM_FUTURES",
    futuresIncidents: [{ code: "FUTURES_ACTUAL_LIQUIDATION_BUFFER_BREACHED", symbol: "SOLUSDT" }] }] } });
  const incident = codeOnly.runtime.brokers[0].futuresIncidents[0];
  assert.match(shortLiveFuturesIncidentLabel(incident.type), /실제 청산 가격.*여유 부족/);
  assert.equal(incident.verified, false); assert.equal(incident.quantity, null);
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
test("consent button requires fresh exact prepared authority and no prior checkbox or typed phrase", () => {
  assert.equal(shortLiveReadyForConsent(ready(), options), true);
  for (const changes of [{ fresh: false }, { selectionUnchanged: false }, { preparedAuthority: "old-preparation" }, { preparedScopeMatches: false }]) {
    assert.equal(shortLiveReadyForConsent(ready(), { ...options, ...changes }), false);
  }
});
test("server blockers, missing hash and active runtime block start", () => {
  for (const mutate of [
    (state) => { state.blockers = ["insufficient-balance"]; },
    (state) => { state.prepared.blockers = ["market-closed"]; },
    (state) => { state.prepared.scopeHash = "invalid"; },
    (state) => { state.prepared.readyToStart = false; },
    (state) => { state.runtime = { status: "RUNNING" }; },
  ]) { const state = ready(); mutate(state); assert.equal(shortLiveReadyForConsent(state, options), false); }
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
    ["BINANCE_USDM_FUTURES:FUTURES_SETTINGS_DIFFER_FROM_PREPARED_POLICY",/레버리지.*격리.*다름/],
    ["FUTURES_ACTUAL_LIQUIDATION_BUFFER_BREACHED",/실제 청산 가격.*여유 부족/],
    ["FUTURES_PROTECTION_ACK_UNKNOWN",/접수 결과 미확정.*진입 차단/],
    ["FUTURES_PROTECTIVE_STOP_TRIGGERED",/보호 손절 발동.*체결 대조/],
    ["FUTURES_AUTO_ADD_MARGIN_MUST_BE_DISABLED",/자동 증거금 추가.*꺼야/],
    ["FUTURES_PROTECTION_RETIREMENT_UNKNOWN",/실제 주문.*종료.*못함/],
    ["BINANCE:DAILY_LOSS_STOP_REACHED",/Binance 현물·선물 합산.*손실 중단/],
    ["IMMUTABLE_FUNCTIONAL_CONTRACT_MISMATCH",/버전·범위.*다시 무주문 준비/],
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
  const effect = panel.match(/useEffect\(\(\) => \{\n    mounted\.current = true;([\s\S]*?)\n  \}, \[refresh, refreshCatalog, pollMs\]\);/)?.[1];
  assert.ok(effect); assert.doesNotMatch(effect, /api\.(start|stop|prepare)/);
  assert.doesNotMatch(panel, /window\.confirm|submitTestIntent|setFlag|applyBinanceFuturesSettings|releaseDryRun|short-live-ack|setAcknowledged/);
  assert.match(panel, /동의하고 실거래 시작/);
  assert.match(panel, /confirmed: true, confirmationText: SHORT_LIVE_CONFIRMATION/);
  assert.match(panel, /실제 자금으로 주문하며 손실이 발생할 수 있습니다/);
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
  assert.equal(shortLiveReadyForConsent(state, { ...options, now: expiry - 1 }), true);
  assert.equal(shortLiveReadyForConsent(state, { ...options, now: expiry }), false);
  state.prepared.expiresAt = ""; assert.equal(shortLiveReadyForConsent(state, options), false);
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
