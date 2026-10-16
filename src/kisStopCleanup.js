const SCHEMA = "kis-scoped-owned-stop-cleanup/v1";

function exactId(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 240
    && !/[\u0000-\u001f\u007f]/u.test(value);
}

export function kisStopCleanupView(snapshot = {}) {
  const body = snapshot?.kis_scoped_cleanup;
  if (!body || body.schemaVersion !== SCHEMA || !Array.isArray(body.scopes)
      || body.scopes.length > 128) {
    return { verified: false, blocked: true, scopes: [],
      detail: "KIS 미체결 정리 상태를 확인한 뒤 실거래를 시작할 수 있습니다." };
  }
  const scopes = [];
  let malformed = false;
  for (const row of body.scopes) {
    if (!row || !exactId(row.runtimeScopeId) || !exactId(row.sessionId)
        || !exactId(row.deploymentId) || typeof row.status !== "string"
        || typeof row.requiresReconciliation !== "boolean"
        || typeof row.placementBlocked !== "boolean") {
      malformed = true;
      continue;
    }
    const clean = row.status === "CLEAN" && row.requiresReconciliation === false
      && row.placementBlocked === false;
    if (clean) continue;
    const count = Number.isSafeInteger(row.remainingOrderCount) && row.remainingOrderCount > 0
      ? row.remainingOrderCount : null;
    scopes.push({ runtimeScopeId: row.runtimeScopeId, sessionId: row.sessionId,
      deploymentId: row.deploymentId, remainingOrderCount: count,
      detail: `${count ? `확인 대상 ${count}건. ` : ""}미체결 취소와 최종 대조가 끝날 때까지 KIS 실거래 재시작을 차단합니다.`,
      reason: typeof row.reason === "string" ? row.reason.slice(0, 240) : "" });
  }
  const blocked = malformed || body.status !== "CLEAN" || scopes.length > 0;
  return { verified: !malformed, blocked, scopes,
    detail: malformed ? "일부 KIS 주문 정리 상태를 확인할 수 없습니다. 새 주문을 시작하지 않습니다."
      : blocked ? "KIS 주문 정리와 최종 대조를 확인해야 합니다." : "KIS 주문 정리 대기 없음" };
}

export function kisCleanupBlocksStart(view, profileId, mode) {
  return profileId === "stock" && mode !== "MONITOR" && view.blocked === true;
}

export function kisManualStartPathSupported(snapshot, profileId, brokerId, mode) {
  const support = snapshot?.kis_manual_start_support;
  return profileId === "stock" && String(brokerId || "").toLowerCase() === "kis"
    && ["SMALL_LIVE", "FULL_LIVE"].includes(mode)
    && support?.schemaVersion === "kis-scoped-manual-start-support/v1"
    && support.supported === true && support.requiresNativeConfirmation === true
    && support.ordinaryRoutesRemainClosed === true && support.orderAuthorityGranted === false;
}
