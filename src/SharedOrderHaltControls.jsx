import { useEffect, useState } from "react";
import { getSharedOrderHalt, reconcileSharedLiveOrders, setSharedOrderHalt } from "./api.js";

const ACTORS = { live_trader: "Live Trader", gpt_trader: "GPT Trader", local: "로컬 제어" };

export default function SharedOrderHaltControls() {
  const [status, setStatus] = useState(null);
  const [pending, setPending] = useState({});
  const [message, setMessage] = useState("");

  function receiveStatus(next) {
    if (typeof next?.halted !== "boolean" || !Number.isInteger(next.revision)) return;
    setStatus((current) => !current || next.revision >= current.revision ? next : current);
  }

  useEffect(() => {
    let active = true;
    getSharedOrderHalt().then((result) => {
      if (active && result.ok === true) receiveStatus(result.status);
    }).catch(() => {});
    return () => { active = false; };
  }, []);

  async function control(action) {
    if (action === "reset" && !window.confirm(
      "Live Trader와 GPT Trader의 주문 운용을 모두 멈췄나요? 차단을 해제하면 아직 운용 중인 전략은 다음 주문을 보낼 수 있습니다.",
    )) return;
    setPending((current) => ({ ...current, [action]: true }));
    setMessage("");
    try {
      const result = action === "refresh" ? await getSharedOrderHalt()
        : action === "reconcile" ? await reconcileSharedLiveOrders()
        : await setSharedOrderHalt(action === "stop" ? { halted: true }
          : { halted: false, expectedRevision: status.revision, bothEnginesPaused: true });
      if (result.ok !== true) {
        setMessage(result.reasonCode === "COORDINATION_ORDER_HALT_REVISION_CHANGED"
          ? "다른 앱에서 상태가 바뀌었습니다. 상태를 다시 확인하세요."
          : "공통 주문 제어 요청을 확인하지 못했습니다. 상태를 다시 확인하세요.");

      } else if (action === "reconcile") {
        const { checked, resolved, unresolved } = result;
        const affected = [...new Set(result.blockers.map(({ broker, symbol }) => `${broker} ${symbol}`))];
        setMessage(checked
          ? `미확인 주문 ${checked}건 중 ${resolved}건 확인 · ${unresolved}건 확인 대기${affected.length ? ` (${affected.join(", ")})` : ""}`
          : "대조할 Live Trader 일반 운용 주문이 없습니다.");
      } else {
        receiveStatus(result.status);
      }
    } catch {

      setMessage("응답을 확인하지 못했습니다. 다시 확인해 주세요.");
    } finally {
      setPending((current) => ({ ...current, [action]: false }));
    }
  }

  const actor = ACTORS[status?.updatedBy];
  const lastChange = status?.updatedAt
    ? new Date(status.updatedAt * 1000).toLocaleString("ko-KR") : null;
  return (
    <section className="panel operational-safeguards-panel" aria-label="공통 신규 주문 차단">
      <header className="status-list">
        <strong>공통 신규 주문 차단</strong>
        <span className={`inline-state ${status?.halted ? "danger" : status ? "success" : "warning"}`}>
          {status?.halted ? "차단 중" : status ? "차단 해제" : "상태 확인 필요"}
        </span>
      </header>
      <p className="diagnostic-explanation">
        Live Trader와 GPT Trader의 신규 실주문 전송을 차단합니다. 이미 전송에 들어간 주문은 처리될 수 있고, 기존 주문 취소와 체결 대조는 계속 가능합니다.
      </p>
      {actor && lastChange && <small>마지막 확인 상태 변경 · {actor} · {lastChange}</small>}
      <div className="operator-actions">
        <button type="button" className="danger-button" disabled={pending.stop} onClick={() => control("stop")}>
          {pending.stop ? "차단 중…" : "공통 신규 주문 차단"}
        </button>
        {status?.halted && <button type="button" className="secondary-button" disabled={pending.stop || pending.reset} onClick={() => control("reset")}>
          차단 해제
        </button>}
        <button type="button" className="secondary-button" disabled={pending.refresh} onClick={() => control("refresh")}>
          상태 다시 확인
        </button>
        <button type="button" className="secondary-button" disabled={pending.reconcile} onClick={() => control("reconcile")}>
          {pending.reconcile ? "주문 대조 중…" : "미확인 일반 운용 주문 대조"}
        </button>
      </div>
      <p role="status" className="diagnostic-explanation">{message}</p>
    </section>
  );
}
