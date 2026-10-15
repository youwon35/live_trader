import test from "node:test";
import assert from "node:assert/strict";
import {runtimeRows, runtimeForDeployment, runtimeModeForDeployment, runningDeploymentIds, artifactIsRunning} from "../src/runtimeSelection.js";

const binance = {running: true, profileId: "crypto", brokerId: "binance", deploymentId: "binance-equal", portfolioId: "portfolio-b", strategyIds: ["strategy-b"], mode: "SMALL_LIVE"};
const upbit = {running: true, profileId: "crypto", brokerId: "upbit", deploymentId: "upbit-equal", portfolioId: "portfolio-u", strategyIds: ["strategy-u"], mode: "MONITOR"};
const runtime = {running: true, runtimes: {b: binance, u: upbit}, profiles: {crypto: {running: true, ambiguous: true}}};

test("Binance and Upbit retain independent exact selection and stop identities", () => {
  assert.equal(runtimeRows(runtime).length, 2);
  assert.equal(runtimeForDeployment(runtime, {id: "upbit-equal", brokerId: "upbit"}).runtimeId, "u");
  assert.equal(runtimeForDeployment(runtime, {id: "binance-equal", brokerId: "binance"}).mode, "SMALL_LIVE");
  assert.deepEqual(runningDeploymentIds(runtime), ["binance-equal", "upbit-equal"]);
});
test("unknown context and mismatched broker cannot select another running crypto runtime", () => {
  assert.equal(runtimeForDeployment(runtime, {id: "missing", brokerId: "upbit"}), null);
  assert.equal(runtimeForDeployment(runtime, {id: "binance-equal", brokerId: "upbit"}), null);
  assert.equal(runtimeForDeployment(runtime, {}), null);
});
test("ambiguous exact deployment refuses selection", () => {
  assert.equal(runtimeForDeployment({runtimes: {b: binance, another: binance}}, {id: "binance-equal"}), null);
});
test("stopped or substring artifact identities are never marked running", () => {
  assert.equal(artifactIsRunning(runtime, "strategy-b"), true);
  assert.equal(artifactIsRunning(runtime, "strategy"), false);
  assert.equal(artifactIsRunning({runtimes: {b: {...binance, running: false}}}, "strategy-b"), false);
  assert.equal(artifactIsRunning(runtime, "binance"), false);
});
test("legacy snapshots remain exact and ambiguous aggregates are ignored", () => {
  assert.equal(runtimeForDeployment({profiles: {crypto: binance}}, {id: "binance-equal"}).brokerId, "binance");
  assert.deepEqual(runtimeRows({profiles: {crypto: {running: true, ambiguous: true}}}), []);
});

test("environment header uses selected account mode/session and global MONITOR wins", async () => {
  const {buildLiveEnvironmentModel} = await import("../src/liveWorkspaceModel.js");
  const snapshot={mode:"FULL_LIVE",continuous_runtime:{...runtime,runtimes:{
    b:{...binance,sessionId:"session-b"},u:{...upbit,mode:"FULL_LIVE",sessionId:"session-u"},
  }}};
  const context={known:true,contextKey:"selected-b",deploymentId:"binance-equal",brokerId:"binance"};
  const model=buildLiveEnvironmentModel(snapshot,context);
  assert.equal(model.mode.code,"SMALL_LIVE");
  assert.equal(model.barItems.find((row)=>row.id==="session").value,"session-b");
  const missing=buildLiveEnvironmentModel(snapshot,{...context,deploymentId:"not-running"});
  assert.equal(missing.mode.code,"MONITOR");
  assert.equal(missing.barItems.find((row)=>row.id==="session").value,"미실행");
  assert.equal(buildLiveEnvironmentModel({...snapshot,mode:"MONITOR"},context).mode.code,"MONITOR");
});

test("actual application mode pill follows the selected runtime", () => {
  const snapshot = {mode:"FULL_LIVE",continuous_runtime:{runtimes:{b:binance,u:{...upbit,mode:"FULL_LIVE"}}}};
  assert.equal(runtimeModeForDeployment(snapshot,{id:"binance-equal",brokerId:"binance"}),"SMALL_LIVE");
  assert.equal(runtimeModeForDeployment(snapshot,{id:"upbit-equal",brokerId:"upbit"}),"FULL_LIVE");
  assert.equal(runtimeModeForDeployment(snapshot,{id:"unknown"}),"MONITOR");
  assert.equal(runtimeModeForDeployment({...snapshot,mode:"MONITOR"},{id:"upbit-equal"}),"MONITOR");
});
