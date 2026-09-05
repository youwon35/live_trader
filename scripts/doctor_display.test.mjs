import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { parse } from "@babel/parser";
import { recordedReconciliation } from "../src/executionAvailability.js";
import { deploymentContextMatchesPreflight } from "../src/deploymentSelection.js";

// Read actual App function bodies by AST boundaries. Do not import App,
// initialize its settings, mount effects or contact a server.
const source = readFileSync(new URL("../src/App.jsx", import.meta.url), "utf8");
const program = parse(source, { sourceType: "module", plugins: ["jsx"] }).program;
const declaration = (name) => {
  const node = program.body.find((item) => item.type === "FunctionDeclaration" && item.id?.name === name);
  assert.ok(node, `missing App function: ${name}`);
  return node;
};
const helpers = ["statusTone", "detailTone", "makeDetail", "buildDoctorItems"]
  .map((name) => { const node = declaration(name); return source.slice(node.start, node.end); }).join("\n");
const buildDoctorItems = new Function(
  "recordedReconciliation", "deploymentContextMatchesPreflight",
  `${helpers}\nreturn buildDoctorItems;`,
)(recordedReconciliation, deploymentContextMatchesPreflight);
const NOW = Date.parse("2026-09-06T03:00:00.000Z");
const future = new Date(NOW + 60_000).toISOString();
const check = (label, status = "pass") => ({ label, status, detail: "기록된 검사", value: 0 });
const validSnapshot = () => ({
  api_connected: true,
  brokers: [{ name: "KIS", missing_env: [] }],
  broker_diagnostics: [],
  checklist: [{ key: "risk_limits_reviewed", label: "한도 검토", required: true, checked: true }],
  risk_checks: [check("위험 한도")],
  strategies: [{ name: "전략 A", symbol: "TEST", live_allowed: true, lifecycle_status: "live" }],
  reconciliation: {
    summary: {
      last_run: "2026-09-06T02:59:00.000Z", status: "pass",
      account_count: 1, position_count: 0, mismatch_count: 0, error_count: 0, api_required_count: 0,
    },
    errors: [],
  },
  watchdog: {
    last_run: "2026-09-06T02:59:00.000Z", status: "pass",
    critical_count: 0, warning_count: 0, trip_count: 0,
    checks: [check("heartbeat")],
  },
  final_preflight: [check("현재 Deployment")],
  live_governance: {
    deploymentId: "deployment-a",
    preflightValidity: { valid: true },
    latestPreflight: { deploymentId: "deployment-a", status: "PASS", expiresAt: future },
  },
});
const inspect = (snapshot, selected = "deployment-a") =>
  buildDoctorItems(snapshot, selected, NOW);
const item = (items, id) => {
  const found = items.find((row) => row.id === id);
  assert.ok(found, `missing Doctor item: ${id}`);
  return found;
};
const unverified = (row) => {
  assert.notEqual(row.tone, "success");
  assert.notEqual(row.status, "통과");
};
function walk(node, visit) {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) { node.forEach((child) => walk(child, visit)); return; }
  visit(node);
  Object.values(node).forEach((child) => { if (child && typeof child === "object") walk(child, visit); });
}

test("empty connected snapshot never marks a Doctor section or fallback detail passed", () => {
  const items = inspect({ api_connected: true });
  assert.equal(items.length, 7);
  for (const row of items) {
    unverified(row);
    assert.ok(row.details.length > 0);
    for (const detail of row.details) assert.notEqual(detail.tone, "success", row.id);
  }
});

test("unknown or disconnected API invalidates even otherwise complete checks", () => {
  for (const connected of [false, undefined, null, "true"]) {
    const items = inspect({ ...validSnapshot(), api_connected: connected });
    assert.ok(items.every((row) => row.tone !== "success"));
    assert.ok(items.every((row) => row.details.every((detail) => detail.tone !== "success")));
  }
});

test("complete current Deployment evidence can display passed checks", () => {
  const rows = inspect(validSnapshot());
  for (const id of ["doctor-checklist", "doctor-risk", "doctor-strategy", "doctor-watchdog", "doctor-reconciliation", "doctor-final"]) {
    assert.equal(item(rows, id).tone, "success", id);
  }
});

test("switching selection keeps another Deployment's Preflight unverified, including its details", () => {
  for (const selected of ["deployment-b", "", undefined]) {
    const rows = buildDoctorItems(validSnapshot(), selected, NOW);
    const final = item(rows, "doctor-final");
    unverified(final);
    assert.ok(final.details.every((detail) => detail.tone !== "success"));
  }
});

test("a current manifest cannot relabel an older Preflight as belonging to the selected Deployment", () => {
  const snapshot = validSnapshot();
  snapshot.live_governance.deploymentId = "deployment-b";
  assert.equal(deploymentContextMatchesPreflight("deployment-b", snapshot.live_governance), true);
  unverified(item(inspect(snapshot, "deployment-b"), "doctor-final"));
});

test("expired, undated or non-PASS Preflight is invalid despite cached server validity", () => {
  for (const expiresAt of [undefined, "", "not-a-date", new Date(NOW).toISOString(), new Date(NOW - 1).toISOString()]) {
    const snapshot = validSnapshot();
    snapshot.live_governance.latestPreflight.expiresAt = expiresAt;
    unverified(item(inspect(snapshot), "doctor-final"));
  }
  for (const status of [undefined, "FAIL", "UNKNOWN", "PASS_WITH_WARNING"]) {
    const snapshot = validSnapshot();
    snapshot.live_governance.latestPreflight.status = status;
    unverified(item(inspect(snapshot), "doctor-final"));
  }
  const snapshot = validSnapshot();
  snapshot.live_governance.preflightValidity.valid = false;
  unverified(item(inspect(snapshot), "doctor-final"));
});

test("supported snake-case Preflight metadata keeps the same exact identity and expiry contract", () => {
  const snapshot = validSnapshot();
  snapshot.live_governance = {
    deployment_id: "deployment-a",
    preflight_validity: { valid: true },
    latest_preflight: { deployment_id: "deployment-a", result: "PASS", expires_at: future },
  };
  assert.equal(item(inspect(snapshot), "doctor-final").tone, "success");
  unverified(item(inspect(snapshot, "deployment-b"), "doctor-final"));
});

test("empty or unknown risk and strategy records cannot count as completed checks", () => {
  for (const risk_checks of [[], [check("위험 한도", "unknown")]]) {
    unverified(item(inspect({ ...validSnapshot(), risk_checks }), "doctor-risk"));
  }
  for (const strategies of [[], [{ name: "전략", live_allowed: undefined }], [{ name: "전략", live_allowed: "true" }]]) {
    unverified(item(inspect({ ...validSnapshot(), strategies }), "doctor-strategy"));
  }
});

test("Watchdog needs recorded checks and valid counters; inactive monitoring is not PASS", () => {
  const original = validSnapshot().watchdog;
  for (const watchdog of [
    undefined,
    {},
    { ...original, checks: [] },
    { ...original, last_run: "미실행" },
    { ...original, status: "unknown" },
    { ...original, critical_count: NaN },
    { ...original, warning_count: undefined },
    { ...original, trip_count: -1 },
    { ...original, status: "na" },
  ]) {
    unverified(item(inspect({ ...validSnapshot(), watchdog }), "doctor-watchdog"));
  }
});

test("known failures remain blocking even if the rest of their evidence is incomplete", () => {
  const snapshot = validSnapshot();
  snapshot.risk_checks = [check("한도 초과", "fail")];
  snapshot.watchdog = { status: "fail", checks: [check("heartbeat", "fail")] };
  snapshot.final_preflight = [check("Preflight 실패", "fail")];
  snapshot.live_governance = {};
  for (const id of ["doctor-risk", "doctor-watchdog", "doctor-final"]) {
    assert.equal(item(inspect(snapshot), id).tone, "danger", id);
  }
});

test("selected Deployment is passed through each actual Doctor caller", () => {
  const propsByParent = [
    ["WorkspaceContent", "OperationsOverviewPage"],
    ["OperationsOverviewPage", "PreTradeDoctorPanel"],
  ];
  for (const [parent, child] of propsByParent) {
    const calls = [];
    walk(declaration(parent), (node) => {
      if (node.type === "JSXOpeningElement" && node.name?.type === "JSXIdentifier" && node.name.name === child) calls.push(node);
    });
    assert.equal(calls.length, 1, child);
    const prop = calls[0].attributes.find((attribute) => attribute.type === "JSXAttribute" && attribute.name.name === "selectedDeploymentId");
    assert.equal(prop?.value?.type, "JSXExpressionContainer", `${child} selected context`);
  }
  const calls = [];
  walk(declaration("PreTradeDoctorPanel"), (node) => {
    if (node.type === "CallExpression" && node.callee?.name === "buildDoctorItems") calls.push(node);
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].arguments[1]?.name, "selectedDeploymentId");
});
