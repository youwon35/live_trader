// One user Start intent owns one bounded, cancellable public-data preparation.
// Poll results contain no authority: native confirmation and normal Start follow.
const PENDING = new Set(["PREPARING_PUBLIC", "CHECKING_ACCOUNT"]);
const TERMINAL = new Set(["CANCELLED", "FAILED", "EXPIRED"]);

export const KIS_PREPARATION_LABELS = Object.freeze({
  PREPARING_PUBLIC: "시세 준비 중",
  CHECKING_ACCOUNT: "계좌 확인 중",
  READY: "시작 확인 준비 완료",
  NATIVE_CONFIRMATION: "Windows 시작 확인 대기",
  STARTING: "실행 조건 최종 확인 중",
  CANCELLED: "준비 취소됨",
  FAILED: "준비를 완료하지 못했습니다",
  EXPIRED: "준비 시간이 만료되었습니다",
  COMPLETE: "시작 요청 완료",
});

export function kisPreparationFailureMessage(result = {}) {
  const code = [result.reasonCode, result.code, result.errorCode, result.reason].filter((v) => typeof v === "string").join(" ");
  if (/KIS_LABEL_SCOPE_UNPROVEN|LIVE_NATIVE_REVIEWED_LABEL_/.test(code)) {
    return "국내 시세의 분봉 시각 검증이 완료되지 않아 시작을 보류했습니다.";
  }
  if (/SCOPE|BINDING|CONTEXT|source-account-policy|source.*changed/.test(code)) {
    return "선택한 전략 또는 계좌의 검증 정보가 변경되어 시작을 보류했습니다. 현재 설정으로 다시 점검해 주세요.";
  }
  if (/CLOCK/.test(code)) return "시세 수신 시각을 확인하지 못해 시작을 보류했습니다.";
  if (/EXPIRED/.test(code)) return "KIS 시작 준비 시간이 만료되었습니다. 현재 상태로 다시 준비해 주세요.";
  if (/CANCELLED/.test(code)) return "KIS 시작 준비가 취소되었습니다.";
  return "시세 준비 또는 계좌 확인을 완료하지 못해 시작을 보류했습니다. 점검 결과를 확인해 주세요.";
}

export function createKisPreparationCoordinator({ request, now = Date.now,
  sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)),
  pollMilliseconds = 1000, onProgress = () => {} }) {
  const active = new Map();
  let sequence = 0;
  const error = (message, code) => Object.assign(new Error(message), { code, uiSafe: true });
  const isCurrent = (flow) => active.get(flow.scopeKey) === flow && !flow.cancelled;
  function assertCurrent(flow) {
    if (!isCurrent(flow)) throw error("KIS 시세 준비가 취소되었거나 변경되었습니다.", "KIS_PREPARATION_CANCELLED");
  }
  function progress(flow, status, detail = "") {
    if (active.get(flow.scopeKey) !== flow) return;
    onProgress({ scopeKey: flow.scopeKey, flowId: flow.id, preparationId: flow.preparationId,
      label: flow.label, status, detail, canCancel: !TERMINAL.has(status) && !["COMPLETE", "STARTING"].includes(status) });
  }
  async function cancelRemote(flow) {
    if (!flow.preparationId || flow.cancelSent) return;
    flow.cancelSent = true;
    try {
      await request("/api/runtime/kis-native-prepare-cancel", {
        method: "POST", body: { preparationId: flow.preparationId }, timeoutMs: 5000,
      });
    } catch {
      // The local intent remains cancelled even if this request is unavailable.
      // The server's own deadline/revocation remains authoritative.
    }
  }
  async function cancel(scopeKey, reason = "준비가 취소되었습니다.") {
    const flow = active.get(scopeKey);
    if (!flow) return false;
    flow.cancelled = true;
    progress(flow, "CANCELLED", reason);
    active.delete(scopeKey);
    await cancelRemote(flow);
    return true;
  }
  function cancelAll(reason) {
    return Promise.all([...active.keys()].map((key) => cancel(key, reason)));
  }
  async function run({ body, scopeKey, label = "한국투자증권", confirmAndStart }) {
    if (active.has(scopeKey)) throw error("이 계좌의 KIS 시작 준비가 이미 진행 중입니다.", "KIS_START_IN_PROGRESS");
    const flow = { scopeKey, label, id: ++sequence, preparationId: "", cancelled: false, cancelSent: false };
    active.set(scopeKey, flow);
    progress(flow, "PREPARING_PUBLIC");
    try {
      const startedAt = now();
      const begin = await request("/api/runtime/kis-manual-start-challenge", {
        method: "POST", body, timeoutMs: 15000,
      });
      if (typeof begin?.preparationId === "string" && begin.preparationId.length > 0) flow.preparationId = begin.preparationId;
      if (!isCurrent(flow)) {
        await cancelRemote(flow);
        assertCurrent(flow);
      }
      if (begin?.ok !== true || !flow.preparationId || begin.status !== "PREPARING_PUBLIC"
          || !Number.isFinite(begin.maximumPreparationSeconds) || begin.maximumPreparationSeconds <= 0
          || begin.maximumPreparationSeconds > 660 || begin.runtimeStarted !== false) {
        throw error("KIS 시세 준비 응답을 확인할 수 없습니다.", "KIS_PREPARATION_INVALID");
      }
      const deadline = startedAt + begin.maximumPreparationSeconds * 1000;
      let challenge;
      while (true) {
        assertCurrent(flow);
        if (now() >= deadline) throw error("KIS 시세 준비 시간이 만료되었습니다.", "KIS_PREPARATION_EXPIRED");
        const result = await request(`/api/runtime/kis-native-prepare-status?preparationId=${encodeURIComponent(flow.preparationId)}`, { timeoutMs: 5000 });
        assertCurrent(flow);
        if (now() >= deadline) throw error("KIS 시세 준비 시간이 만료되었습니다.", "KIS_PREPARATION_EXPIRED");
        if (result?.ok !== true || result.preparationId !== flow.preparationId
            || (!PENDING.has(result.status) && !TERMINAL.has(result.status) && result.status !== "READY")) {
          throw error("KIS 시세 준비 대상이 변경되었습니다.", "KIS_PREPARATION_INVALID");
        }
        if (TERMINAL.has(result.status)) {
          throw error(result.status === "CANCELLED" ? "KIS 시세 준비가 취소되었습니다."
            : result.status === "EXPIRED" ? "KIS 시세 준비 시간이 만료되었습니다."
              : kisPreparationFailureMessage(result), result.reasonCode || `KIS_PREPARATION_${result.status}`);
        }
        progress(flow, result.status);
        if (result.status === "READY") { challenge = result; break; }
        await sleep(pollMilliseconds);
      }
      assertCurrent(flow);
      progress(flow, "NATIVE_CONFIRMATION");
      const result = await confirmAndStart(challenge, () => assertCurrent(flow), () => progress(flow, "STARTING"));
      assertCurrent(flow);
      if (result?.ok !== true) throw error(kisPreparationFailureMessage(result), result?.reasonCode || "KIS_PREPARATION_START_BLOCKED");
      progress(flow, "COMPLETE");
      return result;
    } catch (cause) {
      const finalCause = !isCurrent(flow)
        ? error("KIS 시세 준비가 취소되었거나 변경되었습니다.", "KIS_PREPARATION_CANCELLED")
        : cause?.uiSafe === true ? cause : error(kisPreparationFailureMessage(cause), cause?.code || "KIS_PREPARATION_FAILED");
      if (isCurrent(flow)) {
        progress(flow, finalCause?.code === "KIS_PREPARATION_EXPIRED" ? "EXPIRED"
          : finalCause?.code === "KIS_PREPARATION_CANCELLED" || finalCause?.code === "SAFETY_CONFIRMATION_CANCELLED" ? "CANCELLED" : "FAILED",
        finalCause instanceof Error ? finalCause.message : "KIS 시세 준비를 완료하지 못했습니다.");
      }
      await cancelRemote(flow);
      throw finalCause;
    } finally {
      if (active.get(scopeKey) === flow) active.delete(scopeKey);
    }
  }
  return { run, cancel, cancelAll, activeCount: () => active.size };
}
