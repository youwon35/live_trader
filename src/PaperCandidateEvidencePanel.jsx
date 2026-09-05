import { useRef, useState } from "react";
import { getPaperCandidateEvidence } from "./api";

const SCOPE_LABELS = {
  evidenceId: "Evidence ID", evidenceHash: "봉인 Evidence hash",
  evidenceBundleHash: "Evidence Bundle hash", publicationId: "발행 ID",
  publicationHash: "발행 hash", bindingHash: "최종 연결 hash",
  strategyArtifactHash: "전략 hash", strategyInstanceId: "전략 Instance",
  portfolioArtifactHash: "Portfolio hash", portfolioInstanceId: "Portfolio Instance",
  deploymentManifestHash: "Manifest hash", sessionId: "검증 세션",
};

export default function PaperCandidateEvidencePanel({ strategyId = "" }) {
  const [inbox, setInbox] = useState(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const pending = useRef(false);

  async function refresh() {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    try {
      const result = await getPaperCandidateEvidence();
      const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
      const invalid = !record(result) || result.schemaVersion !== "live-paper-evidence-inbox-v1"
        || result.readOnly !== true || result.canImport !== false || typeof result.ok !== "boolean"
        || !Array.isArray(result.candidates) || !Array.isArray(result.errors)
        || result.errors.some((error) => typeof error !== "string")
        || (result.requiredNextStep !== undefined && typeof result.requiredNextStep !== "string")
        || result.candidates.some((candidate) => !record(candidate)
          || candidate.canImport !== false
          || !["VERIFIED_READ_ONLY", "BLOCKED"].includes(candidate.status)
          || ["evidenceId", "detail"].some((key) => typeof candidate[key] !== "string")
          || ["strategyId", "strategyName", "portfolioId", "instanceHash", "rootKey"].some((key) => candidate[key] !== undefined && typeof candidate[key] !== "string")
          || (candidate.identity !== undefined && (!record(candidate.identity)
            || Object.keys(SCOPE_LABELS).some((key) => candidate.identity[key] !== undefined && typeof candidate.identity[key] !== "string")))
          || (candidate.deployment !== undefined && (!record(candidate.deployment)
            || ["deploymentId", "mode", "lifecycle", "definitionHash"].some((key) => typeof candidate.deployment[key] !== "string")
            || !Number.isInteger(candidate.deployment.revision))));
      if (invalid) throw new Error("검증 근거 응답 형식을 확인하지 못했습니다. 새로고침하세요.");
      setInbox(result.ok ? result : null);
      setMessage(result.ok ? "" : result.errors.join(" · ") || "검증 근거를 불러오지 못했습니다.");
    } catch (error) {
      setInbox(null);
      setMessage(typeof error?.message === "string" ? error.message : "검증 근거를 불러오지 못했습니다.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }

  const rows = (inbox?.candidates || []).filter(
    (candidate) => !strategyId || !candidate.strategyId || candidate.strategyId === strategyId,
  );
  return (
    <details className="compact-disclosure">
      <summary>Paper 검증 근거 확인</summary>
      <p>Paper에서 발행한 검증 근거와 현재 전략·Instance 저장본을 대조합니다. 확인 전용이며 배포 생성, 승인, 주문 설정을 변경하지 않습니다.</p>
      <p>{inbox?.requiredNextStep || "현재는 검증 근거 확인만 가능합니다. Live 후보 등록과 최초 제한 실거래 승인 기능은 준비 중입니다."}</p>
      <button className="secondary-button" disabled={busy} onClick={refresh} type="button">
        {busy ? "확인 중…" : "Paper 검증 근거 새로고침"}
      </button>
      {message && <p role="status">{message}</p>}
      {inbox?.ok && rows.length === 0 && <p>확인할 근거가 없습니다. Paper에서 검증 근거를 발행한 뒤 다시 확인하세요.</p>}
      {rows.length > 0 && (
        <div className="table-scroll">
          <table>
            <thead><tr><th>전략 · 검증 근거</th><th>확인 결과</th></tr></thead>
            <tbody>{rows.map((candidate, index) => (
              <tr key={`${candidate.rootKey || "blocked"}:${candidate.evidenceId}:${index}`}>
                <td>{candidate.strategyName || candidate.strategyId || candidate.evidenceId}<br /><small>{candidate.evidenceId}</small></td>
                <td>{candidate.detail}
                  {candidate.identity && <details>
                    <summary>봉인 정보 보기</summary>
                    <p>현재 배포: {candidate.deployment?.deploymentId || "미등록"} · 상태: {candidate.deployment?.mode || "미확인"} · revision: {candidate.deployment?.revision ?? "-"}</p>
                    <dl>{Object.entries(SCOPE_LABELS).map(([key, label]) => candidate.identity[key] && (
                      <div key={key}><dt>{label}</dt><dd><code>{candidate.identity[key]}</code></dd></div>
                    ))}<div><dt>현재 Instance hash</dt><dd><code>{candidate.instanceHash}</code></dd></div></dl>
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
