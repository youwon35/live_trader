import assert from "node:assert/strict";
import test from "node:test";

import {
  buildCurrentDeploymentOptions,
  deploymentContextMatchesPreflight,
  deploymentRuntimeProfile,
  governedDeploymentIdentity,
  strategyLifecycleStage,
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
    artifactLifecycle: { status: lifecycle || "unknown", source: lifecycle ? "lifecycle.status" : "missing", conflicts: [] },
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
  assert.match(options[0].label, /제한 실거래 대기/);
  assert.match(options[1].label, /모의 검증 완료/);
});

test("현재 실행에 고정된 종료 Deployment는 안전한 Stop을 위해 표시한다", () => {
  const retired = strategy("retired", "retired");
  const [option] = buildCurrentDeploymentOptions([retired], {
    pinnedDeploymentIds: [retired.deployment_id],
  });

  assert.equal(option.id, retired.deployment_id);
  assert.match(option.label, /^\[현재 세션\]/);
});

test("승인 상태는 검증 단계를 대신하지 않으며 오래된 Live 승인보다 중지 상태를 우선한다", () => {
  for (const terminal of ["paused", "retired"]) {
    const stopped = strategy(terminal, terminal, {
      lifecycle: { status: "live" }, promotion: { stage: "LIVE" }, live_allowed: true,
    });
    assert.equal(strategyLifecycleStage(stopped), terminal);
    assert.deepEqual(buildCurrentDeploymentOptions([stopped]), []);
    const [pinned] = buildCurrentDeploymentOptions([stopped], { pinnedDeploymentIds: [stopped.deployment_id] });
    assert.equal(pinned.id, stopped.deployment_id);
    assert.match(pinned.label, terminal === "paused" ? /일시중지/ : /보관·종료/);
  }
  const approvalOnly = strategy("approval-only", undefined, { promotion: { stage: "LIVE" } });
  assert.equal(strategyLifecycleStage(approvalOnly), "unknown");
  assert.deepEqual(buildCurrentDeploymentOptions([approvalOnly]), []);
  const qualified = strategy("qualified", "before-live-small", { promotion: { stage: "PAPER" } });
  assert.equal(strategyLifecycleStage(qualified), "before-live-small");
  assert.equal(buildCurrentDeploymentOptions([qualified])[0].id, qualified.deployment_id);
});

test("같은 저장본의 검증 라벨과 배포 선택 기준은 독립적이다", () => {
  const ready = strategy("ready", "before-live-small", { artifactLifecycle: { status: "backtested", source: "lifecycle.status", conflicts: [] } });
  const [option] = buildCurrentDeploymentOptions([ready]);
  assert.equal(option.id, ready.deployment_id);
  assert.match(option.label, /저장본: 백테스트 완료/);
  assert.match(option.label, /배포: 제한 실거래 대기/);
  assert.deepEqual(buildCurrentDeploymentOptions([{ ...ready, lifecycle_status: "paused" }]), []);
  const [legacy] = buildCurrentDeploymentOptions([{ ...ready, artifactLifecycle: undefined }]);
  assert.match(legacy.label, /저장본: 검증 상태 미확인/);
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


test("봉인된 Paper 근거를 가져온 검토 대기 배포는 선택만 가능하며 주문 권한은 생기지 않는다", () => {
  const candidate = strategy("imported", "draft", { deployment_source: "deployment-registry", paper_live_qualification: { ready: true }, permissions: { live_allowed: false, live_small_eligible: false, live_eligible: false } });
  const options = buildCurrentDeploymentOptions([candidate, strategy("unverified", "draft")]);
  assert.equal(options.length, 1);
  assert.equal(options[0].id, candidate.deployment_id);
  assert.equal(options[0].strategy.permissions.live_allowed, false);
  assert.equal(options[0].strategy.permissions.live_small_eligible, false);
});
