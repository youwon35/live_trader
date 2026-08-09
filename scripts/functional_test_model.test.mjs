import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  formatFunctionalTestRemaining,
  functionalTestDurationBounds,
  functionalTestProgress,
  normalizeFunctionalTestWorkspace,
  preferredFunctionalTestCandidate,
} from "../src/functionalTestModel.js";


const workspace = normalizeFunctionalTestWorkspace({
  status: "PERMIT_READY",
  brokerSubmissionAllowed: true,
  promotionEligible: true,
  durationLimits: { maxDays: 30 },
  candidates: [
    { key: "blocked", available: false, label: "Blocked" },
    { key: "ready", available: true, label: "Ready" },
  ],
  current: {
    selectedTargetKey: "ready",
    blockers: ["one"],
  },
});

assert.equal(workspace.environment, "KIS_LIVE");
assert.equal(workspace.readinessOnly, true);
assert.equal(workspace.brokerSubmissionAllowed, false);
assert.equal(workspace.promotionEligible, false);
assert.equal(workspace.durationLimits.maxDays, 30);
assert.equal(preferredFunctionalTestCandidate(workspace)?.key, "ready");
assert.equal(preferredFunctionalTestCandidate(workspace, "blocked")?.key, "blocked");

assert.deepEqual(functionalTestDurationBounds("DAYS", 90), { min: 1, max: 90 });
assert.deepEqual(functionalTestDurationBounds("HOURS", 90), { min: 1, max: 2160 });

const progress = functionalTestProgress(
  {
    startsAt: "2026-08-05T00:00:00.000000Z",
    endsAt: "2026-08-05T06:00:00.000000Z",
  },
  new Date("2026-08-05T01:30:00.000000Z"),
);
assert.equal(progress.ratio, 0.25);
assert.equal(formatFunctionalTestRemaining(progress.remainingMs), "4시간 30분");
assert.equal(formatFunctionalTestRemaining(2 * 24 * 60 * 60 * 1000), "2일 0시간");

// A malformed or expired pointer is intentionally omitted from current.permit,
// but authorityReferencePresent remains true until safe final reconciliation.
// Keep all scope/duration controls and the replacement-permit action locked in
// that recovery state so the UI matches the server's fail-closed contract.
const workspaceSource = readFileSync(
  new URL("../src/FunctionalTestWorkspace.jsx", import.meta.url),
  "utf8",
);
assert.match(
  workspaceSource,
  /const authorityConfigurationLocked = Boolean\(permit\)\s*\|\| workspace\.current\.authorityReferencePresent === true;/,
);
assert.equal(
  (workspaceSource.match(/\|\| authorityConfigurationLocked/g) || []).length,
  4,
  "target, duration, unit, and permit preparation must share the authority-reference lock",
);

const startExecutionSource = workspaceSource.match(
  /function startExecution\(\) \{([\s\S]*?)\n  \}\n\n  function pauseToday/,
)?.[1] || "";
assert.match(startExecutionSource, /startFunctionalTest\(/);
assert.doesNotMatch(
  startExecutionSource,
  /window\.confirm/,
  "FUNCTIONAL_TEST_START must use the shared identity-bound confirmation instead of a second browser prompt",
);

console.log("functional test model tests passed");
