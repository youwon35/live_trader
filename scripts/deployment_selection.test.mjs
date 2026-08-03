import assert from "node:assert/strict";
import test from "node:test";

import {
  buildCurrentDeploymentOptions,
  deploymentContextMatchesPreflight,
  deploymentRuntimeProfile,
  governedDeploymentIdentity,
} from "../src/deploymentSelection.js";
import { buildOrderCsvRows, ORDER_CSV_COLUMNS } from "../src/orderCsv.js";

function strategy(id, lifecycle, extra = {}) {
  return {
    strategy_id: id,
    deployment_id: `dep:${id}:standalone:live`,
    name: "동일 전략명",
    symbol: "BTCUSDT",
    timeframe: "1h",
    lifecycle_status: lifecycle,
    ...extra,
  };
}

test("현재 Deployment 선택기는 실행 후보만 남기고 중복·종료 기록을 숨긴다", () => {
  const papered = strategy("papered", "papered");
  const duplicate = { ...papered, artifact_source_path: "another-root/papered.json" };
  const ready = strategy("ready", "before-live-small", { live_allowed: true });
  const retired = strategy("retired", "retired", { live_allowed: true });
  const archived = strategy("archived", "papered", { archived: true });
  const draft = strategy("draft", "draft");

  const options = buildCurrentDeploymentOptions([
    retired,
    papered,
    duplicate,
    archived,
    draft,
    ready,
  ]);

  assert.deepEqual(options.map((option) => option.id), [ready.deployment_id, papered.deployment_id]);
  assert.equal(new Set(options.map((option) => option.label)).size, options.length);
});

test("현재 실행에 고정된 종료 Deployment는 안전한 Stop을 위해 표시한다", () => {
  const retired = strategy("retired", "retired");
  const [option] = buildCurrentDeploymentOptions([retired], {
    pinnedDeploymentIds: [retired.deployment_id],
  });

  assert.equal(option.id, retired.deployment_id);
  assert.match(option.label, /^\[현재 세션\]/);
});

test("Deployment broker와 runtime profile을 명확히 매핑한다", () => {
  assert.equal(deploymentRuntimeProfile({ brokerId: "kis" }), "stock");
  assert.equal(deploymentRuntimeProfile({ brokerId: "binance-futures" }), "crypto");
  assert.equal(deploymentRuntimeProfile({ brokerId: "upbit" }), "crypto");
  assert.equal(deploymentRuntimeProfile({ brokerId: "unknown" }), "");
});

test("현재 선택 Deployment와 governed Preflight 컨텍스트를 exact match한다", () => {
  const governance = {
    deploymentId: "dep:current:1",
    preflightValidity: { valid: true },
  };

  assert.equal(governedDeploymentIdentity(governance), "dep:current:1");
  assert.equal(
    deploymentContextMatchesPreflight("dep:current:1", governance),
    true,
  );
  assert.equal(
    deploymentContextMatchesPreflight("dep:other:1", governance),
    false,
  );
  assert.equal(deploymentContextMatchesPreflight("", governance), false);
  assert.equal(
    deploymentContextMatchesPreflight("dep:current:1", {}),
    false,
  );
});

test("manifest/latest Preflight의 governed Deployment도 정확히 해석한다", () => {
  assert.equal(
    governedDeploymentIdentity({ manifest: { deploymentId: "dep:manifest" } }),
    "dep:manifest",
  );
  assert.equal(
    governedDeploymentIdentity({ latest_preflight: { deployment_id: "dep:preflight" } }),
    "dep:preflight",
  );
});

test("주문 CSV는 현재 필터 결과와 Deployment 식별자를 보존한다", () => {
  const rows = buildOrderCsvRows([
    {
      order: {
        timestamp: "2026-08-03T01:02:03Z",
        broker_id: "binance",
        order_id: "order-1",
        client_order_id: "client-1",
        symbol: "BTCUSDT",
        side: "BUY",
        quantity: 0.01,
        executed_quantity: 0.01,
        state: "filled",
      },
    },
  ], "dep-selected", (order) => order.timestamp);

  assert.equal(ORDER_CSV_COLUMNS.length, 11);
  assert.deepEqual(rows, [{
    time: "2026-08-03T01:02:03Z",
    broker: "binance",
    order_id: "order-1",
    client_order_id: "client-1",
    deployment: "dep-selected",
    symbol: "BTCUSDT",
    side: "BUY",
    quantity: 0.01,
    executed_quantity: 0.01,
    state: "filled",
    reason: "",
  }]);
});
