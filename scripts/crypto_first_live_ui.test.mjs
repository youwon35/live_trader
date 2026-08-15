import assert from "node:assert/strict";

import {
  cryptoFirstCombinedStatus,
  cryptoFirstLaneActive,
  cryptoFirstLaneControls,
} from "../src/cryptoFirstLiveModel.js";


const releaseOpen = {
  rootCompositionReleased: true,
  coordinatorActivationReleased: true,
  coordinatorRollbackProtectionReleased: true,
  externalWormAuthorityReleased: true,
};

const upbitReady = {
  prepared: true,
  available: true,
  networkOrderPostAllowed: true,
  firstLiveBootstrapEligible: true,
  liveEnableGate: true,
  terminalState: "IDLE",
  sessionId: "",
  cryptoFirstLive: { release: releaseOpen, coordinator: { phase: "IDLE" } },
};

const binanceReady = {
  prepared: true,
  available: false,
  candidateIssuanceAvailable: true,
  supervisedNonPromotionAvailable: true,
  supervisedPromotionEligible: false,
  supervisedRealE2EEligible: false,
  networkOrderPostAllowed: false,
  terminalState: "IDLE",
  sessionId: "",
  lifecycle: { phase: "IDLE" },
  cryptoFirstLive: { release: releaseOpen, coordinator: { phase: "IDLE" } },
};

assert.equal(
  cryptoFirstLaneControls("UPBIT", {
    ...upbitReady,
    cryptoFirstLive: {
      release: { ...releaseOpen, coordinatorActivationReleased: false },
      coordinator: { phase: "IDLE" },
    },
  }).startEnabled,
  false,
  "release HOLD must disable Upbit start",
);

assert.equal(
  cryptoFirstLaneControls("BINANCE_SPOT", {
    ...binanceReady,
    supervisedNonPromotionAvailable: false,
  }).startEnabled,
  false,
  "unreleased supervised mode must disable Binance start",
);

assert.equal(cryptoFirstCombinedStatus(upbitReady, binanceReady).upbit.startEnabled, true);
assert.equal(cryptoFirstCombinedStatus(upbitReady, binanceReady).binance.startEnabled, true);

const activeUpbit = {
  ...upbitReady,
  schedulerRunning: true,
  sessionId: "upbit-session-1",
  terminalState: "RUNNING",
};
const blockedParallel = cryptoFirstCombinedStatus(activeUpbit, binanceReady);
assert.equal(blockedParallel.binance.startEnabled, false);
assert.equal(blockedParallel.binance.otherLaneActive, true);
assert.equal(cryptoFirstLaneActive(activeUpbit), true);

const cleanupUpbit = {
  ...upbitReady,
  available: false,
  networkOrderPostAllowed: false,
  cryptoFirstLive: {
    release: { ...releaseOpen, rootCompositionReleased: false },
    coordinator: { phase: "CLEANUP_ONLY" },
  },
  sessionId: "upbit-session-cleanup",
  terminalState: "RECONCILIATION_REQUIRED",
};
const cleanupControls = cryptoFirstLaneControls("UPBIT", cleanupUpbit, {});
assert.equal(cleanupControls.startEnabled, false);
assert.equal(cleanupControls.recoverEnabled, true, "cleanup must remain available under release HOLD");

console.log("crypto-first-live native UI control model checks passed");
