import { useRef, useState } from "react";
import { CheckCircle2, LockKeyhole } from "lucide-react";

// Server-owned automatic checks can never be acknowledged by this UI.
// Unknown future keys stay read-only until their operator contract is explicit.
const MANUAL_CHECKLIST_KEYS = new Set([
  "risk_limits_reviewed",
  "notification_channel_reviewed",
  "operator_takeover_ready",
]);
const AUTOMATIC_CHECKLIST_KEYS = new Set([
  "api_keys_reviewed",
  "position_reconcile_reviewed",
]);

function manualItem(item) {
  return MANUAL_CHECKLIST_KEYS.has(item.key)
    && ["manual", "pending"].includes(item.source);
}

export default function OperationalChecklistPanel({
  items = [],
  apiConnected = false,
  onChange,
}) {
  const [pendingKey, setPendingKey] = useState("");
  const [feedback, setFeedback] = useState(null);
  const pending = useRef(false);
  const rows = (Array.isArray(items) ? items : []).filter((item) => (
    item && typeof item === "object"
    && typeof item.key === "string" && item.key
    && typeof item.label === "string"
    && typeof item.checked === "boolean"
    && typeof item.required === "boolean"
  ));
  const remaining = rows.filter((item) => item.required && !item.checked).length;

  async function changeItem(item, checked) {
    if (pending.current || !apiConnected || !manualItem(item)
      || typeof onChange !== "function" || typeof checked !== "boolean"
      || checked === item.checked) return;
    pending.current = true;
    setPendingKey(item.key);
    setFeedback(null);
    try {
      const result = await onChange(item.key, checked);
      if (result?.ok !== true) {
        throw new Error(typeof result?.reason === "string"
          ? result.reason : "운영 확인을 저장하지 못했습니다. 다시 확인하세요.");
      }
      const savedItem = Array.isArray(result?.snapshot?.checklist)
        ? result.snapshot.checklist.find((row) => row?.key === item.key)
        : null;
      if (savedItem?.checked !== checked) {
        throw new Error("저장 결과를 확인하지 못했습니다. 최신 점검 상태를 다시 확인하세요.");
      }
      setFeedback({ error: false, text: `${item.label} ${checked ? "확인을" : "해제를"} 저장했습니다. 실행 전에 새 Preflight를 진행하세요.` });
    } catch (error) {
      setFeedback({ error: true, text: typeof error?.message === "string"
        ? error.message : "운영 확인을 저장하지 못했습니다. 다시 확인하세요." });
    } finally {
      pending.current = false;
      setPendingKey("");
    }
  }

  return (
    <section className="panel operational-checklist-panel" aria-label="운영 체크리스트" aria-busy={Boolean(pendingKey)}>
      <div className="panel-header">
        <div>
          <h2>운영 체크리스트</h2>
          <p>운용자가 실제 절차를 확인한 항목만 개별 저장합니다. 계좌·대조 항목은 서버 검사 결과로 표시합니다.</p>
        </div>
      </div>
      <p className="panel-action-line">
        {!apiConnected ? "API 연결 후 최신 확인 상태를 읽을 수 있습니다."
          : !rows.length ? "운영 체크리스트를 확인할 수 없습니다."
            : remaining ? `필수 확인 ${remaining}개가 남아 있습니다.`
              : "필수 운영 확인이 완료되었습니다. 주문 승인은 별도 Preflight에서 확인합니다."}
      </p>
      <div className="checklist-list">
        {rows.map((item) => {
          const editable = manualItem(item);
          const automatic = AUTOMATIC_CHECKLIST_KEYS.has(item.key);
          const checked = apiConnected && item.checked === true;
          const detail = typeof item.detail === "string" ? item.detail : "";
          return (
            <label className={`checklist-row${checked ? " checked" : ""}`} key={item.key}>
              <input
                type="checkbox"
                aria-label={item.label}
                checked={checked}
                disabled={!apiConnected || !editable || Boolean(pendingKey) || typeof onChange !== "function"}
                onChange={(event) => changeItem(item, event.currentTarget.checked)}
              />
              {editable ? <CheckCircle2 size={16} aria-hidden="true" /> : <LockKeyhole size={16} aria-hidden="true" />}
              <div>
                <strong>{item.label} · {item.required ? "필수" : "선택"}</strong>
                <span style={{ whiteSpace: "normal", overflow: "visible" }}>{detail}</span>
              </div>
              <small>
                {pendingKey === item.key ? "저장 중"
                  : !apiConnected ? "확인 불가"
                    : editable ? (checked ? "운용자 확인" : "운용자 확인 필요")
                      : automatic ? (checked ? "자동 확인" : "자동 확인 대기")
                        : "읽기 전용"}
              </small>
            </label>
          );
        })}
      </div>
      {rows.some((item) => AUTOMATIC_CHECKLIST_KEYS.has(item.key)) && (
        <p className="panel-action-line">자동 확인 대기 항목은 계좌·잔고에서 계좌를 갱신하고 시작 점검을 다시 실행하세요.</p>
      )}
      {feedback && <p className={`inline-state ${feedback.error ? "danger" : "success"}`} role={feedback.error ? "alert" : "status"}>{feedback.text}</p>}
    </section>
  );
}
