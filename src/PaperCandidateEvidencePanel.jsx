import { useRef, useState } from "react";
import { getPaperCandidateEvidence, importPaperCandidate, runLocalMonitorTrial } from "./api";

const SCOPE_LABELS = {
  evidenceId: "Evidence ID", evidenceHash: "봉인 Evidence hash",
  evidenceBundleHash: "Evidence Bundle hash", publicationId: "발행 ID",
  publicationHash: "발행 hash", bindingHash: "최종 연결 hash",
  strategyArtifactHash: "전략 hash", strategyInstanceId: "전략 Instance",
  portfolioArtifactHash: "Portfolio hash", portfolioInstanceId: "Portfolio Instance",
  deploymentManifestHash: "Manifest hash", sessionId: "검증 세션",
};

export default function PaperCandidateEvidencePanel({ strategyId = "", onRegistered }) {
  const [inbox, setInbox] = useState(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const pending = useRef(false);
  const [trialSelection, setTrialSelection] = useState("");
  const [trialResult, setTrialResult] = useState(null);

  async function refresh() {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    try {
      const result = await getPaperCandidateEvidence();
      const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
      const invalid = !record(result) || result.schemaVersion !== "live-paper-evidence-inbox-v1"
        || result.readOnly !== true || typeof result.canImport !== "boolean" || typeof result.ok !== "boolean"
        || !Array.isArray(result.candidates) || !Array.isArray(result.errors)
        || (result.monitorTrials !== undefined && (!Array.isArray(result.monitorTrials) || result.monitorTrials.some((trial) => !record(trial)
          || typeof trial.name !== "string" || typeof trial.detail !== "string" || typeof trial.portfolioId !== "string" || typeof trial.canRun !== "boolean"
          || (trial.canRun && (!record(trial.request) || ["rootKey", "portfolioId", "portfolioHash", "identityHash"].some((key) => typeof trial.request[key] !== "string" || !trial.request[key]))))))
        || (Array.isArray(result.candidates) && result.canImport !== result.candidates.some((candidate) => candidate?.canImport === true))
        || result.errors.some((error) => typeof error !== "string")
        || (result.requiredNextStep !== undefined && typeof result.requiredNextStep !== "string")
        || result.candidates.some((candidate) => !record(candidate)
          || typeof candidate.canImport !== "boolean"
          || (candidate.canImport === true && (!record(candidate.importRequest)
            || !Number.isInteger(candidate.importRequest.expectedRevision)
            || candidate.importRequest.expectedRevision < 0
            || ["rootKey", "evidenceId", "identityHash", "registryHash"].some((key) => typeof candidate.importRequest[key] !== "string" || !candidate.importRequest[key])))
          || !["VERIFIED_READ_ONLY", "BLOCKED"].includes(candidate.status)
          || ["evidenceId", "detail"].some((key) => typeof candidate[key] !== "string")
          || (candidate.blockedReasons !== undefined && (!Array.isArray(candidate.blockedReasons)
            || candidate.blockedReasons.some((reason) => !record(reason) || typeof reason.code !== "string" || typeof reason.detail !== "string")
            || (candidate.blockedReasons.length > 0 && (candidate.canImport || candidate.registered === true))))
          || ["strategyId", "strategyName", "portfolioId", "instanceHash", "rootKey"].some((key) => candidate[key] !== undefined && typeof candidate[key] !== "string")
          || (candidate.identity !== undefined && (!record(candidate.identity)
            || Object.keys(SCOPE_LABELS).some((key) => candidate.identity[key] !== undefined && typeof candidate.identity[key] !== "string")))
          || (candidate.deployment !== undefined && (!record(candidate.deployment)
            || ["deploymentId", "mode", "lifecycle", "definitionHash"].some((key) => typeof candidate.deployment[key] !== "string")
            || !Number.isInteger(candidate.deployment.revision))));
      if (invalid) throw new Error("검증 근거 응답 형식을 확인하지 못했습니다. 새로고침하세요.");
      setInbox(result.ok ? result : null);
      setTrialSelection("");
      setTrialResult(null);
      setMessage(result.ok ? "" : result.errors.join(" · ") || "검증 근거를 불러오지 못했습니다.");
    } catch (error) {
      setInbox(null);
      setMessage(typeof error?.message === "string" ? error.message : "검증 근거를 불러오지 못했습니다.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }

  async function register(candidate) {
    if (pending.current || !candidate.canImport || !candidate.importRequest) return;
    pending.current = true;
    setBusy(true);
    try {
      const result = await importPaperCandidate(candidate.importRequest);
      if (result?.ok !== true || result.authorizationGranted !== false || typeof result.deploymentId !== "string" || !result.deploymentId) {
        throw new Error(result?.reason || "등록 결과를 확인하지 못했습니다. 새로고침하여 확인하세요.");
      }
      pending.current = false;
      await refresh();
      pending.current = true;
      setBusy(true);
      setMessage(result.detail || "검토 대기 후보로 등록했습니다.");
      if (onRegistered) await onRegistered(result.deploymentId);
    } catch (error) {
      setInbox(null);
      setMessage(error?.message || "후보를 등록하지 못했습니다. 새로고침하세요.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }

  async function openRegistered(deploymentId) {
    if (pending.current || !onRegistered) return;
    pending.current = true;
    setBusy(true);
    try { await onRegistered(deploymentId); }
    catch (error) { setMessage(error?.message || "등록한 배포를 다시 조회해 주세요."); }
    finally { pending.current = false; setBusy(false); }
  }

  async function runTrial() {
    const selected = (inbox?.monitorTrials || []).find((item) => `${item.request?.rootKey}:${item.portfolioId}` === trialSelection);
    if (pending.current || !selected?.canRun) return;
    pending.current = true;
    setBusy(true);
    setTrialResult(null);
    try {
      const result = await runLocalMonitorTrial(selected.request);
      if (result?.ok !== true || result.schemaVersion !== "live-local-monitor-trial-v1" || result.mode !== "MONITOR"
        || result.authorizationGranted !== false || result.promotionEligible !== false || result.ordersSubmitted !== 0
        || result.currentDeploymentChanged !== false || result.accountCalls !== 0 || result.tradingEnabled !== false
        || result.historicalOnly !== true || result.useAsPromotionEvidence !== false
        || !result.summary || !Array.isArray(result.limitations) || result.limitations.some((item) => typeof item !== "string")
        || result.source?.portfolioId !== selected.request.portfolioId || result.source?.portfolioHash !== selected.request.portfolioHash
        || !Array.isArray(result.source?.bindings) || result.source.bindings.length !== result.summary.symbols
        || result.source.bindings.some((item) => !item || ["instanceId", "symbol", "sampleHash", "sourceFinalBarEnd"].some((key) => typeof item[key] !== "string") || !Number.isInteger(item.sampleCount))
        || ["symbols", "sampleCount", "decisionCount"].some((key) => !Number.isInteger(result.summary[key]) || result.summary[key] < 1)
        || ["BUY", "SELL", "HOLD"].some((key) => !Number.isInteger(result.summary.signals?.[key]) || result.summary.signals[key] < 0)
        || result.summary.sampleCount !== result.summary.decisionCount || typeof result.reportHash !== "string" || typeof result.reportPath !== "string") {
        throw new Error(result?.reason || "연결 시험 결과를 확인하지 못했습니다.");
      }
      setTrialResult(result);
      setMessage("연결 시험을 마쳤습니다. 현재 운영 배포와 실거래 자격은 그대로입니다.");
    } catch (error) { setMessage(error?.message || "연결 시험에 실패했습니다."); }
    finally { pending.current = false; setBusy(false); }
  }

  const rows = (inbox?.candidates || []).filter(
    (candidate) => !strategyId || !candidate.strategyId || candidate.strategyId === strategyId,
  );
  return (
    <details className="compact-disclosure">
      <summary>모의거래 검증 근거 확인</summary>

      <p>{inbox?.requiredNextStep || "검증된 저장본을 검토 대기 후보로 등록합니다. 등록으로 계좌 연결이나 주문 실행이 승인되지는 않습니다."}</p>
      <button className="secondary-button" disabled={busy} onClick={refresh} type="button">
        {busy ? "확인 중…" : "Paper 검증 근거 새로고침"}
      </button>
      {message && <p role="status">{message}</p>}
      {inbox?.ok && rows.length === 0 && <p>확인할 근거가 없습니다. 모의거래에서 검증 근거를 발행한 뒤 다시 확인하세요.</p>}
      {inbox?.monitorTrialError && <p role="status">연결 시험 목록: {inbox.monitorTrialError}</p>}
      {(inbox?.monitorTrials || []).length > 0 && <section aria-label="연결 시험 · 주문 없음" style={{ margin: "16px 0" }}>
        <h3>연결 시험 · 주문 없음</h3>
        <p>비승급 연구용 저장본의 과거 종가를 재생합니다. 현재 운영 배포·계좌·실거래 자격은 바뀌지 않습니다.</p>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          <label>시험용 구성 선택 <select value={trialSelection} disabled={busy} onChange={(event) => { setTrialSelection(event.target.value); setTrialResult(null); }}>
            <option value="">구성을 선택하세요</option>
            {inbox.monitorTrials.filter((item) => item.canRun).map((item) => <option key={`${item.request.rootKey}:${item.portfolioId}`} value={`${item.request.rootKey}:${item.portfolioId}`}>{item.name} · {item.detail}</option>)}
          </select></label>
          <button type="button" className="secondary-button" disabled={busy || !trialSelection} onClick={runTrial}>주문 없이 연결 시험</button>
        </div>
        {inbox.monitorTrials.filter((item) => !item.canRun).map((item, index) => <p key={index}>{item.name}: {item.detail}</p>)}
        {trialResult && <div role="status">
          <p>{trialResult.summary.symbols}종목 · 종가 {trialResult.summary.sampleCount.toLocaleString()}개 · 판단 {trialResult.summary.decisionCount.toLocaleString()}회 · 주문 0건</p>
          <p>매수 판단 {trialResult.summary.signals.BUY}회 · 매도 판단 {trialResult.summary.signals.SELL}회 · 관망 {trialResult.summary.signals.HOLD}회</p>
          <p>개별 시각·OHLC·거래량이 없는 종가 표본입니다. 가상 연속 시각과 보유 상태로 연결만 시험합니다.</p>
          <details><summary>입력 근거와 한계 보기</summary>
            {trialResult.limitations.map((text, index) => <p key={index}>{text}</p>)}
            <p>실제 전체 기간: 표본 시각이 없어 미확인</p>
            {trialResult.source.bindings.map((item) => <p key={item.instanceId}>{item.symbol} · 원본 기준 마지막 시각 {item.sourceFinalBarEnd} · 종가 {item.sampleCount}개 · 입력 hash <code>{item.sampleHash}</code></p>)}
            <p>보고서 hash <code>{trialResult.reportHash}</code></p><p>저장 위치 {trialResult.reportPath}</p>
          </details>
        </div>}
      </section>}
      {rows.length > 0 && (
        <div className="table-scroll">
          <table>
            <thead><tr><th>전략 · 검증 근거</th><th>확인 결과</th></tr></thead>
            <tbody>{rows.map((candidate, index) => (
              <tr key={`${candidate.rootKey || "blocked"}:${candidate.evidenceId}:${index}`}>
                <td style={{ verticalAlign: "top" }}>{candidate.strategyName || candidate.strategyId || candidate.evidenceId}<br /><small>{candidate.evidenceId}</small></td>
                <td style={{ verticalAlign: "top" }}>{candidate.detail}
                  {candidate.status === "VERIFIED_READ_ONLY" && <div aria-label="후보 준비 단계">
                    <p>Backtester 저장본 · 현재 전략과 실행 단위 일치</p>
                    <p>Paper 검증 근거 · 봉인 연결 확인</p>
                    <p>Live 후보 등록 · {candidate.canImport ? "검토 대기 등록 가능" : candidate.registered ? "등록됨" : candidate.blockedReasons?.length ? "기존 배포 조건으로 차단" : "상태 추가 확인 필요"}</p>
                    {candidate.blockedReasons?.length > 0 && <ul aria-label="후보 등록 차단 사유">
                      {candidate.blockedReasons.map((reason, reasonIndex) => <li key={`${reason.code}:${reasonIndex}`}>{reason.detail}</li>)}
                    </ul>}
                    <small>실거래 승인과 현재 계좌 상태는 이 조회에서 확인하지 않습니다.</small>
                  </div>}
                  <div style={{ display: "flex", gap: 8, margin: "8px 0", flexWrap: "wrap" }}>
                  {candidate.canImport && <button type="button" className="primary-button" disabled={busy} onClick={() => register(candidate)}>검토 대기 후보 등록</button>}
                  {candidate.registered && onRegistered && <button type="button" className="secondary-button" disabled={busy} onClick={() => openRegistered(candidate.deployment.deploymentId)}>등록한 배포 보기</button>}
                  </div>
                  {candidate.identity && <details>
                    <summary>봉인 정보 보기</summary>
                    <p>현재 배포: {candidate.deployment?.deploymentId || "미등록"} · 상태: {candidate.deployment?.mode || "미확인"} · revision: {candidate.deployment?.revision ?? "-"}</p>
                    <dl>{Object.entries(SCOPE_LABELS).map(([key, label]) => candidate.identity[key] && (
                      <div key={key}><dt>{label}</dt><dd><code>{candidate.identity[key]}</code></dd></div>
                    ))}<div><dt>현재 실행 단위 hash</dt><dd><code>{candidate.instanceHash}</code></dd></div></dl>
                  </details>}
                </td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}
    </details>
  );
}
