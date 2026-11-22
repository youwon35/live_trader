export const STARTUP_STEPS = Object.freeze([
  { id: "connection", label: "프로그램 서버 연결", route: "settings" },
  { id: "preferences", label: "저장된 화면 설정 읽기", route: "settings" },
  { id: "strategies", label: "저장된 운용 전략 읽기", route: "gate" },
  { id: "accounts", label: "초기 계좌·잔고 조회", route: "accounts" },
]);

const CHECKED = new Set(["done", "review"]);

export const startupInitial = () => ({
  steps: Object.fromEntries(STARTUP_STEPS.map(row => [row.id, { status: "pending", detail: "" }])),
  completedAt: null,
});

export function startupTransition(state, event) {
  if (!Number.isFinite(event?.at) || event.at < 0 || !STARTUP_STEPS.some(row => row.id === event.id)) return state;
  const previous = state.steps[event.id];
  if (previous.status === "done" || (state.completedAt !== null && previous.status !== "review")) return state;
  const status = { begin: "running", done: "done", fail: "failed", review: "review" }[event.type];
  if (!status) return state;
  const detail = event.detail || "";
  const route = event.route;
  if (previous.status === status && previous.detail === detail && previous.route === route) return state;
  const steps = { ...state.steps, [event.id]: route ? { status, detail, route } : { status, detail } };
  const ready = STARTUP_STEPS.every(row => CHECKED.has(steps[row.id].status));
  return { steps, completedAt: ready ? state.completedAt ?? event.at : null };
}

export function startupView(state) {
  const rows = STARTUP_STEPS.map(row => ({ ...row, ...state.steps[row.id] }));
  const completed = rows.filter(row => CHECKED.has(row.status)).length;
  return {
    rows,
    completed,
    total: rows.length,
    percent: Math.floor(completed / rows.length * 100),
    ready: state.completedAt !== null,
    failed: rows.some(row => row.status === "failed"),
    review: rows.some(row => row.status === "review"),
  };
}

export function startupStrategyObservation(snapshot) {
  // getSnapshot validates this array before the Doctor sees it.
  const count = snapshot.strategies.length;
  return count > 0
    ? { ok: true, detail: `저장된 전략 목록 ${count}개를 불러왔습니다.` }
    : { ok: true, review: true, detail: "등록된 운용 전략이 없습니다." };
}

export function startupAccountObservation(result) {
  const summary = result?.snapshot?.reconciliation?.summary;
  const accounts = result?.snapshot?.reconciliation?.accounts;
  if (typeof result?.ok !== "boolean" || !Array.isArray(accounts) || !summary
      || accounts.length !== summary.account_count
      || !accounts.every(row => row && typeof row === "object" && !Array.isArray(row))
      || !Number.isInteger(summary.account_count) || summary.account_count < 0
      || !Number.isFinite(Date.parse(summary.last_run || ""))) {
    return { ok: false, detail: "계좌 조회 결과를 확인하지 못했습니다." };
  }
  if (summary.api_required_count > 0 || summary.account_count === 0) {
    return { ok: false, route: "settings", detail: "연결 정보가 필요한 계좌가 있습니다." };
  }
  if (summary.error_count !== 0 || summary.api_required_count !== 0) {
    return { ok: false, detail: "일부 계좌의 잔고 조회를 완료하지 못했습니다." };
  }
  if (result.syncWarning) {
    return { ok: true, review: true, detail: "잔고를 읽었습니다. 체결 동기화 상태를 확인하세요." };
  }
  // Startup reports reads. Reconciliation remains part of the trading preflight.
  return { ok: true, detail: "계좌·잔고 자료를 읽었습니다. 대조 결과는 계좌·잔고에서 확인할 수 있습니다." };
}
