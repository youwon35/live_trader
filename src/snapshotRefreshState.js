import { reconciliationReferenceView } from "./reconciliationReferenceView.js";
// Presentation ordering only; this module never grants execution permission.
export function createSnapshotResponseGate() {
  let issued = 0;
  let accepted = 0;
  let observedAt = null;
  return {
    begin() { return ++issued; },
    accept(token, snapshot) {
      const stamp = Date.parse(snapshot?.generated_at || "");
      const hasStamp = Number.isFinite(stamp);
      if (observedAt !== null && hasStamp && stamp < observedAt) return false;
      if (token < accepted && !(hasStamp && observedAt !== null && stamp > observedAt)) return false;
      accepted = Math.max(accepted, token);
      if (hasStamp) observedAt = stamp;
      return true;
    },
    acceptFailure(token) {
      if (token < accepted) return false;
      accepted = token;
      return true;
    },
  };
}

// Snapshot creation is CPU-heavy on the local server. One read owns it at a
// time; account reads wait behind a current snapshot and suppress periodic polls.
// Native emergency actions deliberately do not pass through this coordinator.
export function createSnapshotReadCoordinator() {
  let snapshotFlight = null;
  let accountFlight = null;
  return {
    isSnapshotRunning() { return snapshotFlight !== null; },
    runSnapshot(work) {
      if (accountFlight) return Promise.resolve(undefined);
      if (snapshotFlight) return snapshotFlight;
      snapshotFlight = Promise.resolve().then(work).finally(() => { snapshotFlight = null; });
      return snapshotFlight;
    },
    runAccount(work) {
      if (accountFlight) return accountFlight;
      accountFlight = Promise.resolve(snapshotFlight).catch(() => {}).then(work)
        .finally(() => { accountFlight = null; });
      return accountFlight;
    },
  };
}

export const initialAccountRefreshState = { pending: false, waitingForSnapshot: false, error: "", warning: "", lastSuccessAt: "" };
export function accountRefreshResultState(previous, result) {
  const summary = result?.snapshot?.reconciliation?.summary || {};
  // A valid HTTP read can report an unapproved historical baseline difference.
  // Explicit read errors remain failures; this does not alter raw reconciliation.
  const referenceOnly = result?.ok === false && !result.reason && reconciliationReferenceView({ ...result.snapshot, api_connected: true }).referenceOnly;
  const complete = (result?.ok === true || referenceOnly) && Number(summary.account_count) > 0
    && summary.error_count === 0 && summary.api_required_count === 0
    && Number.isFinite(Date.parse(summary.last_run || ""));
  return { ...previous, pending: false,
    error: result?.ok === false && !referenceOnly ? result.reason || "계좌·포지션 대조를 완료하지 못했습니다." : "",
    warning: result?.syncWarning || (referenceOnly ? "계좌 조회는 완료했습니다. 과거 조회 기준과의 잔고 변화는 참고 영역에서 확인하세요. 운용 대조 통과를 뜻하지 않습니다." : "") || (!complete && result?.ok === true ? "일부 계좌 조회가 미확인입니다. 계좌별 결과를 확인하세요." : ""),
    lastSuccessAt: complete ? summary.last_run : previous.lastSuccessAt,
  };
}

export function accountObservationView(snapshot, state = initialAccountRefreshState, now = Date.now()) {
  const connected = snapshot?.api_connected === true;
  const observedAt = snapshot?.reconciliation?.summary?.last_run;
  const stamp = Date.parse(observedAt || "");
  const fresh = connected && !state.error && Number.isFinite(stamp) && stamp <= now + 5000 && now - stamp <= 300000;
  return { connected, fresh, observedAt: Number.isFinite(stamp) ? observedAt : "",
    tone: state.pending ? "info" : state.error ? "fail" : fresh ? "pass" : "warn",
    label: state.pending ? state.waitingForSnapshot ? "전체 상태 조회 완료 대기" : "계좌·체결 읽는 중" : state.error ? "갱신 실패" : !connected ? "서버 연결 미확인" : fresh ? "최근 관측값 표시 중" : Number.isFinite(stamp) ? "마지막 조회값 표시 중" : "계좌 조회 필요",
    detail: !connected ? "전체 응답을 받지 못했습니다. 실제 잔고나 실행 중인 거래의 종료를 뜻하지 않습니다."
      : fresh ? "표시된 관측 시각의 계좌 자료입니다. 주문 허용과 시작 점검은 별도로 확인합니다."
        : "자동 반복 조회는 하지 않습니다. 최신 잔고·평가는 계좌 다시 읽기로 확인하세요. 미확인을 0으로 처리하지 않습니다.",
  };
}
