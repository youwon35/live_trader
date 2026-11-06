import React, { useEffect, useRef, useState } from "react";
import { request } from "./api.js";
import "./TossCashOperationPanel.css";

const base = "/api/toss/operation/";
const labels = { IDLE: "준비 전", PREPARING: "주문 없이 준비 중", PREPARED: "준비 완료", PREPARATION_FAILED: "준비 확인 필요", RUNNING: "토스 현금 운용 중", STOPPING: "소유 주문 정리 중", STOPPED: "종료·대조 완료", STOPPED_WITH_ERROR: "오류를 남기고 종료", RECOVERY_REQUIRED: "이전 운용 대조 필요", RECONCILIATION_REQUIRED: "소유 주문·보유분 확인 필요", CLOSING: "이번 운용 보유분 매도 중" };
const reasons = {
  TOSS_CREDENTIALS_REQUIRED: "연결·설정에서 토스 Client ID와 Secret을 입력해 주세요.",
  TOSS_ACCOUNT_SEQ_REQUIRED: "연결·설정 → 토스증권 연결 확인에서 공식 계좌 목록을 조회하고 사용할 계좌를 선택해 주세요.",
  TOSS_ACCOUNT_SELECTION_REQUIRED: "Client ID와 Secret을 저장한 뒤 연결·설정 → 토스증권 연결 확인에서 계좌를 선택해 주세요. Account Seq는 일반 계좌번호와 다릅니다.",
  TOSS_CONFIGURED_ACCOUNT_NOT_VERIFIED: "저장한 API 계좌 식별값을 확인하지 못했습니다. 연결·설정에서 계좌 목록을 다시 조회하고 선택해 주세요.",
  TOSS_CASH_BUYING_POWER_BELOW_BUDGET: "해당 통화의 현금 주문 가능 금액이 설정한 예산보다 적습니다. 예산을 조정해 주세요. 자동 환전은 하지 않습니다.",
  TOSS_BUDGET_RESERVED_BY_ANOTHER_RUN: "같은 토스 계좌·통화의 예산을 다른 Live/GPT 운용이 사용 중입니다.",
  TOSS_PREPARATION_EXPIRED: "준비 결과가 만료되었습니다. 주문 없는 준비 검사를 다시 진행해 주세요.",
  TOSS_EXISTING_ORDERS_CONFLICT: "선택한 종목에 기존 주문 또는 조건 주문이 있습니다. 기존 주문을 확인해 주세요.",
  TOSS_ORDER_ACK_UNKNOWN_NO_RETRY: "전송 결과를 확정할 수 없는 주문이 있습니다. 중복 주문을 막기 위해 재전송하지 않습니다. 토스 주문 내역을 확인해 주세요.",
  TOSS_OWNED_CLOSE_REGULAR_SESSION_REQUIRED: "이번 운용 보유분 정리는 해당 시장의 정규장에서 가능합니다.",
};
function message(value) { return reasons[value] || value || ""; }
function money(value, currency) { return `${Number(value).toLocaleString("ko-KR", { maximumFractionDigits: currency === "USD" ? 2 : 0 })} ${currency}`; }

export default function TossCashOperationPanel({ monitorOnly = false }) {
  const [items, setItems] = useState([]);
  const [selected, setSelected] = useState([]);
  const [budgets, setBudgets] = useState({ KR: 300000, US: 300000 });
  const [hours, setHours] = useState(6);
  const [binding, setBinding] = useState(false);
  const [consent, setConsent] = useState(false);
  const [closeConsent, setCloseConsent] = useState(false);
  const [snapshot, setSnapshot] = useState(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const submitted = useRef(false);
  const scopeRevision = useRef(0);
  const preparedRevision = useRef(-1);
  const mounted = useRef(true);
  const latestMutation = useRef(0);
  const [now, setNow] = useState(Date.now());
  const state = snapshot?.state || "IDLE";
  const active = snapshot?.running || ["RUNNING", "STOPPING", "CLOSING"].includes(state);
  const preparing = snapshot?.preparing || state === "PREPARING";
  const unresolved = snapshot?.unfinishedOwners || [];
  const locked = busy || active || preparing || unresolved.length > 0;
  const preparation = snapshot?.preparation;
  const ready = state === "PREPARED" && preparation && preparation.expiresAt * 1000 > now && preparedRevision.current === scopeRevision.current;
  const invalidate = () => { scopeRevision.current += 1; preparedRevision.current = -1; setConsent(false); setNotice(""); };

  useEffect(() => {
    mounted.current = true;
    let stopped = false, timer;
    async function poll() {
      const mutation = latestMutation.current;
      try {
        const value = await request(base + "status", { timeoutMs: 10000 });
        if (!stopped && mutation === latestMutation.current) {
          if (!value.ok) throw new Error(value.reason);
          setSnapshot(value); setNow(Date.now());
        }
      } catch (err) { if (!stopped && mutation === latestMutation.current) setError(err.message); }
      if (!stopped) timer = window.setTimeout(poll, 5000);
    }
    poll();
    return () => { stopped = true; mounted.current = false; window.clearTimeout(timer); };
  }, []);

  useEffect(() => {
    if (monitorOnly) return undefined;
    let canceled = false;
    Promise.allSettled([request(base + "catalog"), request(base + "preferences")]).then(results => {
      if (canceled) return;
      const [catalog, preferences] = results;
      if (catalog.status === "fulfilled" && catalog.value.ok) setItems(catalog.value.items || []);
      else setError(catalog.status === "rejected" ? catalog.reason.message : catalog.value.reason);
      if (preferences.status === "fulfilled" && preferences.value.ok) { setBudgets(preferences.value.budgetKrwByMarket); setHours(preferences.value.durationHours); }
    });
    return () => { canceled = true; };
  }, [monitorOnly]);

  async function act(action, body) {
    if (submitted.current) return;
    submitted.current = true; setBusy(true); setError(""); setNotice("");
    latestMutation.current += 1;
    try {
      const result = await request(base + action, { method: "POST", body, timeoutMs: action === "start" || action === "recover" ? 180000 : 15000 });
      if (!result.ok) throw new Error(result.reason || "요청 결과를 확인하지 못했습니다.");
      if (!mounted.current) return;
      if (action === "preferences") setNotice(result.detail);
      else setSnapshot(result);
      if (action === "prepare") preparedRevision.current = scopeRevision.current;
      if (["start", "close_owned"].includes(action)) { setConsent(false); setCloseConsent(false); }
    } catch (err) { if (mounted.current) setError(err.message); }
    finally { submitted.current = false; if (mounted.current) setBusy(false); }
  }

  const accountCurrencyCash = snapshot?.ledger?.cash || {};
  return <section className="panel toss-cash-panel live-grid-full" aria-label="토스 현금 운용">
    <div className="toss-cash-heading"><div><span className="eyebrow">TOSS · 한국주식 / 미국주식</span><h2>토스 현금 운용</h2></div><strong className={active ? "toss-cash-good" : "muted"}>{labels[state] || state}</strong></div>
    <p className="muted">선택한 주식 전략을 토스 계좌에서 실행합니다. 완성봉 판단 후 새 호가의 지정가로 주문하며, 기존 보유 주식은 매도하지 않습니다.</p>
    {snapshot?.reason && <p role="status">{message(snapshot.reason)}</p>}
    {(active || unresolved.length > 0 || snapshot?.ledger) && <div className="toss-cash-stats">
      <span>봉 평가 <b>{snapshot?.evaluations || 0}</b></span><span>주문 접수 <b>{snapshot?.ordersSubmitted || 0}</b></span>
      <span>체결 주문 <b>{snapshot?.ledger?.fillCount ?? "—"}</b></span><span>장·주문 대기 신호 <b>{snapshot?.waitingSignals || 0}</b></span>
      {Object.entries(accountCurrencyCash).map(([c, v]) => <span key={c}>운용 잔여 예산 <b>{money(v, c)}</b></span>)}
    </div>}
    {!monitorOnly && <>
      <details className="ts-disclosure toss-cash-options" open={!active && !preparing && !preparation}>
        <summary>전략·예산 설정 <span className="muted"> · {selected.length}개 선택 · {hours}시간</span></summary>
        <div className="toss-cash-fields">
          <label>국내 운용 상한 (원)<input type="number" min="1" max="1000000000" value={budgets.KR} disabled={locked} onChange={e => { invalidate(); setBudgets({ ...budgets, KR: e.target.value }); }} /></label>
          <label>미국 운용 상한 (원 상당)<input type="number" min="1" max="1000000000" value={budgets.US} disabled={locked} onChange={e => { invalidate(); setBudgets({ ...budgets, US: e.target.value }); }} /></label>
          <label>운용 시간<input type="number" min="0.1" max="24" step="0.5" value={hours} disabled={locked} onChange={e => { invalidate(); setHours(e.target.value); }} /></label>
        </div>
        <p className="muted">미국 예산은 준비할 때 실제 환율로 달러 상한을 고정합니다. 자동 환전은 하지 않습니다. Live·GPT가 같은 계좌·통화 예산을 중복 사용하지 않습니다.</p>
        <button disabled={busy} onClick={() => act("preferences", { budgetKrwByMarket: budgets, durationHours: hours })}>다음 운용 예산 저장</button>
        <div className="toss-cash-targets" aria-label="주식 전략 선택">
          {!items.length && <p className="muted">연결 가능한 주식 전략 인스턴스가 없습니다. Backtester에서 저장한 주식 전략 또는 주식 전용 포트폴리오를 확인하세요.</p>}
          {items.map(item => <label className="toss-cash-check" key={item.key}><input type="checkbox" checked={selected.includes(item.key)} disabled={locked} onChange={e => { invalidate(); setSelected(e.target.checked ? [...selected, item.key] : selected.filter(k => k !== item.key)); }} /><span>{item.label}</span></label>)}
        </div>
        <label className="toss-cash-check"><input type="checkbox" checked={binding} disabled={locked} onChange={e => { invalidate(); setBinding(e.target.checked); }} /><span>선택한 주식 전략을 이번 운용에서는 토스 계좌에 연결합니다.</span></label>
      </details>
      <div className="toss-cash-actions">
        <button disabled={locked || !selected.length || !binding} onClick={() => act("prepare", { targets: items.filter(i => selected.includes(i.key)).map(i => i.target), tossBindingConfirmed: binding, budgetKrwByMarket: budgets, durationHours: hours })}>{preparing ? "자료·계좌 준비 중…" : "자료·계좌 준비 검사 (주문 없음)"}</button>
        {active && <button disabled={busy} onClick={() => act("stop", { ownerId: snapshot.ownerId })}>운용 중단·소유 미체결 정리</button>}
      </div>
      {preparation && !active && !unresolved.length && <div className="toss-cash-start">
        <p>{Object.entries(preparation.budgets).map(([c, v]) => money(v, c)).join(" · ")} · {preparation.durationHours}시간 · {ready ? `준비 유효 시간 약 ${Math.max(0, Math.ceil((preparation.expiresAt * 1000 - now) / 1000))}초` : "범위 변경 또는 만료 · 다시 준비하세요"}</p>
        <label className="toss-cash-check"><input type="checkbox" checked={consent} disabled={!ready || busy} onChange={e => setConsent(e.target.checked)} /><span>실제 자금으로 주문하며 손실이 발생할 수 있음을 확인했습니다.</span></label>
        <button className="primary" disabled={!ready || !consent || busy} onClick={() => act("start", { preparationId: preparation.preparationId, sourceHash: preparation.sourceHash, liveConsent: true })}>토스 실거래 시작</button>
      </div>}
      {!active && unresolved.length > 0 && <details className="ts-disclosure toss-cash-options"><summary>이전 운용 정리</summary>
        <p>종료 시 보유 주식은 자동 매도하지 않습니다. 미체결·체결을 먼저 대조하고, 보유분이 남으면 직접 동의한 경우에만 이번 운용분을 매도합니다. 확인되지 않은 주문은 재전송하지 않습니다.</p>
        {unresolved.map(owner => <div key={owner}><button disabled={busy} onClick={() => act("recover", { ownerId: owner, cancelOwnedOrders: true })}>소유 주문 대조·미체결 취소</button>
          <label className="toss-cash-check"><input type="checkbox" checked={closeConsent} onChange={e => setCloseConsent(e.target.checked)} /><span>이번 운용의 확인된 보유 주식만 현재 호가의 지정가로 매도하는 데 동의합니다.</span></label>
          <button disabled={busy || !closeConsent} onClick={() => act("close_owned", { ownerId: owner, liveConsent: true })}>이번 운용 보유분 매도·정리</button></div>)}
      </details>}
    </>}
    {notice && <p className="toss-cash-good" role="status">{notice}</p>}
    {error && <p className="toss-cash-error" role="alert">{message(error)}</p>}
    <details className="ts-disclosure toss-cash-options"><summary>진행 상세</summary><p className="muted">기존 장기 검증의 통과 판정과 별개인 현금 운용입니다. 총자산이나 수익률 대신 이번 운용의 주문·체결·소유 수량을 표시합니다.</p>
      {!!snapshot?.ledger && <ul>{Object.entries(snapshot.ledger.positions || {}).map(([id, qty]) => <li key={id}>{snapshot.ledger.scope?.specs?.find(s => s.strategyInstanceId === id)?.symbol || id} · 이번 운용 보유 {qty}주</li>)}</ul>}
      <ol>{(snapshot?.activity || []).map((a, i) => <li key={`${a.at}-${i}`}>{new Date(a.at * 1000).toLocaleTimeString()} · {a.detail}</li>)}</ol>
      {snapshot?.firstFailure && <p className="toss-cash-error">최초 오류 · {snapshot.firstFailure}</p>}
    </details>
  </section>;
}
