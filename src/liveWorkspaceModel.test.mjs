import assert from "node:assert/strict";

import {
  buildLiveEnvironmentModel,
  buildRetryMatrix,
  buildRuntimeComponentModel,
  classifyRetryRequest,
  normalizeKoreanStatus,
  projectAccountReconciliation,
  projectAccountValue,
  projectExecutionQuality,
  projectIncidents,
  projectOrderTimeline,
  projectThreeWayReconciliation,
  selectCurrentDeploymentContext,
} from "./liveWorkspaceModel.js";

const naStatus = normalizeKoreanStatus("N/A");
assert.equal(naStatus.code, "not_applicable");
assert.equal(naStatus.label, "해당 없음");
assert.equal(naStatus.tone, "neutral");
assert.equal(normalizeKoreanStatus("unknown").label, "확인 불가");
assert.equal(normalizeKoreanStatus("unknown").known, false);
assert.equal(normalizeKoreanStatus("risk_blocked").label, "차단");
assert.equal(normalizeKoreanStatus("RUNNING").label, "실행 중");

const deploymentSnapshot = {
  api_connected: true,
  mode: "SMALL_LIVE",
  dry_run: false,
  new_entries_blocked: false,
  kill_switch: false,
  deployments: [
    {
      deploymentId: "deploy-one",
      name: "한국 주식 소액 배포",
      portfolioId: "portfolio-one",
      strategyIds: ["strategy-one"],
      brokerId: "kis",
      accountId: "account-one",
      revision: 3,
      contentHash: "hash-one",
    },
    {
      deploymentId: "deploy-two",
      name: "코인 소액 배포",
      portfolioId: "portfolio-two",
      strategyIds: ["strategy-two"],
      brokerId: "binance-futures",
      accountId: "account-two",
      revision: 7,
      contentHash: "hash-two",
    },
  ],
  continuous_runtime: {
    profiles: {
      crypto: {
        running: true,
        deploymentId: "deploy-two",
        portfolioId: "portfolio-two",
        strategyId: "strategy-two",
        sessionId: "session-22",
      },
    },
  },
  brokers: [
    { broker_id: "kis", name: "한국투자증권", order_ready: true },
    { broker_id: "binance-futures", name: "Binance Futures", order_ready: true },
  ],
};

const runtimeDeployment = selectCurrentDeploymentContext(deploymentSnapshot);
assert.equal(runtimeDeployment.deploymentId, "deploy-two");
assert.equal(runtimeDeployment.source, "runtime-deployment");
assert.equal(runtimeDeployment.requiresConfirmation, false);
assert.equal(runtimeDeployment.contextKey, "deploy-two:7:hash-two");

const explicitDeployment = selectCurrentDeploymentContext(deploymentSnapshot, {
  deploymentId: "deploy-one",
});
assert.equal(explicitDeployment.deploymentId, "deploy-one");
assert.equal(explicitDeployment.strategyId, "strategy-one");
assert.equal(explicitDeployment.source, "explicit-deployment");

const missingDeployment = selectCurrentDeploymentContext(deploymentSnapshot, {
  deploymentId: "does-not-exist",
});
assert.equal(missingDeployment.deploymentId, "deploy-two");
assert.deepEqual(missingDeployment.mismatches, ["deployment-id-mismatch"]);

const backendShapedDeployment = selectCurrentDeploymentContext({
  strategies: [{
    strategy_id: "STRATEGY-ETH-1H",
    deployment_id: "DEPLOY-ETH-1H",
    name: "ETH 추세 전략",
    symbol: "ETHUSDT",
    timeframe: "1h",
    broker_id: "binance-futures",
    account_id: "FUTURES-01",
    portfolio_gate: { portfolioId: "PORTFOLIO-CRYPTO-01" },
  }],
}, { deployment_id: "DEPLOY-ETH-1H", strategy_id: "STRATEGY-ETH-1H" });
assert.equal(backendShapedDeployment.deploymentId, "DEPLOY-ETH-1H");
assert.equal(backendShapedDeployment.portfolioId, "PORTFOLIO-CRYPTO-01");
assert.equal(backendShapedDeployment.strategyId, "STRATEGY-ETH-1H");
assert.equal(backendShapedDeployment.brokerId, "binance-futures");
assert.equal(backendShapedDeployment.accountId, "FUTURES-01");
assert.equal(backendShapedDeployment.symbol, "ETHUSDT");
assert.deepEqual(backendShapedDeployment.mismatches, []);

const emergencyEnvironment = buildLiveEnvironmentModel({
  ...deploymentSnapshot,
  kill_switch: true,
}, explicitDeployment);
assert.equal(emergencyEnvironment.titlePrefix, "[LIVE]");
assert.equal(emergencyEnvironment.watermark, "LIVE");
assert.equal(emergencyEnvironment.safety.level, "emergency");
assert.equal(emergencyEnvironment.safety.riskIncreasingOrdersAllowed, false);
assert.equal(emergencyEnvironment.safety.cancelAllowed, true);
assert.equal(emergencyEnvironment.safety.riskReductionAllowed, true);
assert.equal(emergencyEnvironment.barItems.find((item) => item.id === "deployment").value, "한국 주식 소액 배포");

const failClosedEnvironment = buildLiveEnvironmentModel({
  ...deploymentSnapshot,
  api_connected: false,
  kill_switch: false,
}, explicitDeployment);
assert.equal(failClosedEnvironment.safety.level, "fail_closed");
assert.equal(failClosedEnvironment.safety.label, "상태 확인 불가 · 안전 차단");

const runtimeModel = buildRuntimeComponentModel({
  ...deploymentSnapshot,
  new_entries_blocked: true,
  continuous_runtime: {
    running: true,
    phase: "RUNNING",
    deploymentId: "deploy-two",
    engine: { lastCycleAt: "2026-08-01T10:00:00+09:00" },
  },
  execution_streams: { running: false, errors: [] },
  watchdog: { status: "warn", last_action: "heartbeat 지연", last_run: "2026-08-01T10:00:01+09:00" },
  brokers: [{ broker_id: "binance-futures", name: "Binance Futures", order_ready: false, status: "blocked" }],
}, runtimeDeployment);
const runtimeById = Object.fromEntries(runtimeModel.components.map((item) => [item.id, item]));
assert.equal(runtimeById["market-data"].status.label, "실행 중");
assert.equal(runtimeById["strategy-scheduler"].status.label, "실행 중");
assert.equal(runtimeById["risk-gateway"].status.label, "차단");
assert.equal(runtimeById["order-router"].status.label, "확인 불가");
assert.equal(runtimeById.watchdog.status.label, "주의");
assert.equal(runtimeById.broker.status.label, "차단");

const retryMatrix = buildRetryMatrix();
assert.equal(retryMatrix.length, 9);
assert.equal(classifyRetryRequest({ operation: "market quote", method: "GET", outcome: "timeout" }).autoRetry, true);
const unknownSubmitRetry = classifyRetryRequest({ operation: "order submit", outcome: "UNKNOWN_SUBMIT_RESULT" });
assert.equal(unknownSubmitRetry.id, "order-submit-unknown");
assert.equal(unknownSubmitRetry.directRetryAllowed, false);
assert.match(unknownSubmitRetry.nextAction, /브로커 조회/);
assert.equal(classifyRetryRequest({ operation: "order submit", outcome: "rejected" }).id, "order-rejected");
assert.equal(classifyRetryRequest({ operation: "order cancel", outcome: "timeout" }).id, "order-cancel-unknown");
assert.equal(classifyRetryRequest({ operation: "order submit", outcome: "rate_limited", phase: "pre_submit" }).id, "order-rate-limit-pre-acceptance");
assert.equal(classifyRetryRequest({ operation: "order submit", outcome: "rate_limited", phase: "broker_submit" }).id, "order-submit-unknown");

const unknownOrder = {
  order_id: "order-1",
  oms_order_id: "oms-1",
  broker_order_id: "broker-1",
  trace_id: "trace-1",
  created_at: "2026-08-01T10:00:00+09:00",
  updated_at: "2026-08-01T10:00:03+09:00",
  state: "unknown",
  queue_state: "reconcile_required",
  reason: "network timeout after submit",
  retryable: true,
  risk_report: { status: "pass" },
};
const timeline = projectOrderTimeline(unknownOrder, [
  {
    event_id: "event-signal",
    order_id: "order-1",
    stage: "SIGNAL_DECIDED",
    occurred_at: "2026-08-01T10:00:00.100+09:00",
    decision: "BUY",
  },
  {
    event_id: "event-other",
    order_id: "another-order",
    stage: "FILLED",
    occurred_at: "2026-08-01T10:00:02+09:00",
  },
]);
assert.equal(timeline.unknownSubmitResult, true);
assert.equal(timeline.directRetryAllowed, false);
assert.equal(timeline.status.label, "확인 불가");
assert.ok(timeline.timeline.some((item) => item.stage === "SIGNAL_DECIDED"));
assert.ok(timeline.timeline.some((item) => item.stage === "UNKNOWN_SUBMIT_RESULT"));
assert.equal(timeline.timeline.some((item) => item.id === "event-other"), false);

const quality = projectExecutionQuality([
  {
    order_id: "filled-order",
    state: "filled",
    broker_order_id: "broker-filled",
    side: "BUY",
    reference_price: 100,
    fill_price: 101,
    latency_ms: 120,
    fee: 0.15,
    fee_currency: "USDT",
  },
  { order_id: "blocked-order", state: "risk_blocked" },
  { order_id: "unknown-order", state: "unknown", broker_order_id: "broker-unknown" },
], [], { status: "pass", sampleCount: 1, meanAbsoluteModelErrorBps: 4.2 });
assert.equal(quality.submitted, 2);
assert.equal(quality.filled, 1);
assert.equal(quality.rejected, 1);
assert.equal(quality.unknownSubmitResult, 1);
assert.equal(quality.fillRatePct, 50);
assert.ok(Math.abs(quality.averageSlippageBps - 100) < 1e-9);
assert.equal(quality.averageLatencyMs, 120);
assert.equal(quality.feesByCurrency.USDT, 0.15);
assert.equal(projectExecutionQuality([], []).fillRatePct, null);

const unknownAccountValue = projectAccountValue("조회 정보 없음", "KRW");
assert.equal(unknownAccountValue.known, false);
assert.equal(unknownAccountValue.value, null);
assert.equal(unknownAccountValue.label, "확인 불가");

const threeWay = projectThreeWayReconciliation({
  brokerPositions: [
    { broker_id: "kis", symbol: "005930", broker_qty_value: 2 },
    { broker_id: "kis", symbol: "000660", broker_qty_value: 1 },
    { broker_id: "kis", symbol: "035420", broker_qty_value: 3 },
  ],
  streamPositions: [
    { broker_id: "kis", symbol: "005930", position_qty: 2 },
    { broker_id: "kis", symbol: "000660", position_qty: 1 },
  ],
  ledgerPositions: [
    { broker_id: "kis", symbol: "005930", quantity: 2 },
    { broker_id: "kis", symbol: "000660", quantity: 2 },
    { broker_id: "kis", symbol: "035420", quantity: 3 },
  ],
});
assert.equal(threeWay.summary.matchedCount, 1);
assert.equal(threeWay.summary.mismatchCount, 1);
assert.equal(threeWay.summary.unknownCount, 1);
assert.equal(threeWay.blocking, true);
assert.equal(threeWay.rows.find((row) => row.symbol === "035420").stream.label, "확인 불가");

const accountProjection = projectAccountReconciliation({
  accounts: [
    { broker_id: "kis", broker_name: "한국투자증권", account: "1234", currency: "KRW", broker_cash_value: "unknown", broker_equity_value: "조회 정보 없음", status: "unknown" },
    { broker_id: "binance", broker_name: "Binance", account: "spot", currency: "USDT", broker_cash_value: 10, status: "pass" },
  ],
});
assert.equal(accountProjection.unknownAccountCount, 1);
assert.equal(accountProjection.accounts[0].equity.value, null);
assert.equal(accountProjection.accounts[0].equity.label, "확인 불가");
assert.equal(accountProjection.accounts[1].cash.value, 10);

const fallbackIncidents = projectIncidents({
  api_connected: false,
  kill_switch: true,
  watchdog: { critical_count: 1, last_action: "heartbeat timeout" },
  reconciliation: { summary: { mismatch_count: 2, api_required_count: 1 } },
  orders: [{ order_id: "unknown-order", state: "unknown", queue_state: "reconcile_required" }],
});
const fallbackIncidentIds = new Set(fallbackIncidents.map((item) => item.id));
assert.ok(fallbackIncidentIds.has("api-disconnected"));
assert.ok(fallbackIncidentIds.has("kill-switch-active"));
assert.ok(fallbackIncidentIds.has("watchdog-critical"));
assert.ok(fallbackIncidentIds.has("reconciliation-blocked"));
assert.ok(fallbackIncidentIds.has("unknown-submit-result"));
assert.ok(fallbackIncidents.every((item) => item.derived));

const explicitIncidents = projectIncidents({
  incidents: [{ incidentId: "incident-real", title: "브로커 연결 사고", severity: "critical", status: "acknowledged" }],
  api_connected: false,
});
assert.equal(explicitIncidents.length, 1);
assert.equal(explicitIncidents[0].id, "incident-real");
assert.equal(explicitIncidents[0].state.label, "확인됨");
assert.equal(explicitIncidents[0].derived, false);

console.log("live workspace model contracts passed");
