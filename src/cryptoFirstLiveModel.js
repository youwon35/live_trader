const ACTIVE_STATES = new Set([
  "PREPARING",
  "APPROVED_INERT",
  "ACTIVATION_PREPARING",
  "ACTIVE",
  "RUNNING",
  "STOPPING",
  "CLEANUP",
  "CLEANUP_ONLY",
  "FINAL_RESET",
  "FINAL_RESET_PENDING",
]);

const RECOVERY_STATES = new Set([
  "CLEANUP",
  "CLEANUP_ONLY",
  "CLEANUP_RETRY_REQUIRED",
  "RECONCILIATION_REQUIRED",
  "RECOVERY_REQUIRED",
  "FAILED",
  "FINAL_RESET_PENDING",
]);

const IDLE_STATES = new Set(["", "IDLE", "FINALIZED", "UNAVAILABLE"]);

function stateValues(status = {}) {
  const lifecycle = status.lifecycle && typeof status.lifecycle === "object"
    ? status.lifecycle
    : {};
  const coordinator = status.cryptoFirstLive?.coordinator
    && typeof status.cryptoFirstLive.coordinator === "object"
    ? status.cryptoFirstLive.coordinator
    : {};
  return [
    status.terminalState,
    status.phase,
    lifecycle.phase,
    lifecycle.state,
    coordinator.phase,
  ].map((value) => String(value || "").trim().toUpperCase());
}

export function cryptoFirstLaneActive(status = {}) {
  return status.schedulerRunning === true
    || stateValues(status).some((value) => ACTIVE_STATES.has(value));
}

export function cryptoFirstLaneControls(lane, status = {}, otherStatus = {}) {
  const exactLane = String(lane || "").trim().toUpperCase();
  const states = stateValues(status);
  const terminalState = String(status.terminalState || "").trim().toUpperCase();
  const sessionId = String(status.sessionId || "").trim();
  const active = cryptoFirstLaneActive(status);
  const otherLaneActive = cryptoFirstLaneActive(otherStatus);
  const prepared = status.prepared === true;
  const release = status.cryptoFirstLive?.release;
  const globalReleaseOpen = release && typeof release === "object"
    && [
      "rootCompositionReleased",
      "coordinatorActivationReleased",
      "coordinatorRollbackProtectionReleased",
      "externalWormAuthorityReleased",
    ].every((key) => release[key] === true);

  const laneReady = exactLane === "UPBIT"
    ? status.available === true
      && status.networkOrderPostAllowed === true
      && status.firstLiveBootstrapEligible === true
      && status.liveEnableGate === true
    : status.candidateIssuanceAvailable === true
      && status.supervisedNonPromotionAvailable === true
      && status.networkOrderPostAllowed === false;
  const idle = states.every((value) => IDLE_STATES.has(value));
  const startEnabled = Boolean(
    prepared
    && globalReleaseOpen
    && laneReady
    && idle
    && !active
    && !otherLaneActive
  );
  const stopEnabled = Boolean(prepared && sessionId && active);
  const recoverEnabled = Boolean(
    prepared
    && sessionId
    && states.some((value) => RECOVERY_STATES.has(value))
  );

  let startBlockReason = "";
  if (!prepared) startBlockReason = "백엔드 준비가 완료되지 않았습니다.";
  else if (!globalReleaseOpen) startBlockReason = "공용 실거래 release가 HOLD 상태입니다.";
  else if (otherLaneActive) startBlockReason = "다른 코인 기능시험 lane이 실행 중입니다.";
  else if (active || !idle) startBlockReason = "현재 lane이 종료 상태가 아닙니다.";
  else if (!laneReady) startBlockReason = "독립 관측·승인·정적 release 조건이 미완료입니다.";

  return {
    active,
    otherLaneActive,
    prepared,
    globalReleaseOpen: globalReleaseOpen === true,
    laneReady,
    startEnabled,
    stopEnabled,
    recoverEnabled,
    startBlockReason,
    sessionId,
    terminalState: terminalState || "UNAVAILABLE",
  };
}

export function cryptoFirstCombinedStatus(upbit = {}, binance = {}) {
  return {
    upbit: cryptoFirstLaneControls("UPBIT", upbit, binance),
    binance: cryptoFirstLaneControls("BINANCE_SPOT", binance, upbit),
    parallelActivationBlocked: (
      cryptoFirstLaneActive(upbit) || cryptoFirstLaneActive(binance)
    ),
  };
}
