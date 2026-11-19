import React, { useEffect, useRef, useState } from "react";
import { request } from "./api.js";
import { signalDiagnostics } from "./signalDiagnostics.js";
import "./TossCashOperationPanel.css";

const base = "/api/toss/operation/";
const labels = { IDLE: "준비 전", PREPARING: "주문 없이 준비 중", PREPARED: "준비 완료", PREPARATION_FAILED: "준비 확인 필요", RUNNING: "토스 현금 운용 중", STOPPING: "소유 주문 정리 중", STOPPED: "종료·대조 완료", STOPPED_WITH_ERROR: "오류를 남기고 종료", RECOVERY_REQUIRED: "이전 운용 대조 필요", RECONCILIATION_REQUIRED: "소유 주문·보유분 확인 필요", CLOSING: "이번 운용 보유분 매도 중" };
const reasons = {
  INDICATOR_PRICE_BASIS_REVALIDATION_REQUIRED: "백테스트의 수정주가와 현재 시세의 가격 기준을 맞췄는지 확인하지 못했습니다. 근거가 준비될 때까지 운용을 시작하지 않습니다.",
  TOSS_EXIT_QUANTITY_CONTRACT_UNSUPPORTED: "이 전략의 수량·청산 설정을 현재 토스 운용에서 해석할 수 없습니다. 현재 토스 운용은 이번 전략이 보유한 수량 전체를 매도하는 방식만 지원합니다. 지원되는 전략을 선택하세요.",
  TOSS_DAILY_CANDLE_SESSION_SCOPE_UNAVAILABLE: "토스 일봉은 정규·시간외 범위를 분리할 수 없습니다. 분봉 전략을 선택하세요.",
  TOSS_EXTENDED_SOURCE_REEXPORT_REQUIRED: "시간외 전략은 토스 데이터로 다시 백테스트한 원본을 선택하세요.",
  TOSS_EXTENDED_NXT_ELIGIBILITY_REQUIRED: "이 종목의 NXT 시간외 거래 가능 여부를 확인하지 못했습니다.",
  TOSS_PROFILE_HISTORY_INSUFFICIENT: "선택한 거래시간 범위의 과거 봉이 부족합니다.",
  STOCK_SESSION_OFFICIAL_WINDOWS_REQUIRED: "해당 날짜의 공식 거래시간 자료가 필요합니다.",
  STOCK_SESSION_NEXT_BOUNDARY_UNOBSERVED: "선택한 거래시간의 다음 봉 경계를 아직 확인하지 못했습니다.",
  TOSS_CREDENTIALS_REQUIRED: "연결·설정에서 토스 Client ID와 Secret을 입력해 주세요.",
  TOSS_ACCOUNT_SEQ_REQUIRED: "연결·설정 → 토스증권 연결 확인에서 공식 계좌 목록을 조회하고 사용할 계좌를 선택해 주세요.",
  TOSS_ACCOUNT_SELECTION_REQUIRED: "Client ID와 Secret을 저장한 뒤 연결·설정 → 토스증권 연결 확인에서 계좌를 선택해 주세요. Account Seq는 일반 계좌번호와 다릅니다.",
  TOSS_CONFIGURED_ACCOUNT_NOT_VERIFIED: "저장한 API 계좌 식별값을 확인하지 못했습니다. 연결·설정에서 계좌 목록을 다시 조회하고 선택해 주세요.",
  TOSS_CASH_BUYING_POWER_BELOW_BUDGET: "해당 통화의 현금 주문 가능 금액이 설정한 예산보다 적습니다. 예산을 조정해 주세요. 자동 환전은 하지 않습니다.",
  TOSS_BUDGET_RESERVED_BY_ANOTHER_RUN: "같은 토스 계좌·통화의 예산을 다른 Live/GPT 운용이 사용 중입니다.",
  TOSS_PREPARATION_EXPIRED: "준비 결과가 만료되었습니다. 주문 없는 준비 검사를 다시 진행해 주세요.",
  TOSS_EXISTING_ORDERS_CONFLICT: "선택한 종목에 기존 주문 또는 조건 주문이 있습니다. 기존 주문을 확인해 주세요.",
  TOSS_ORDER_ACK_UNKNOWN_NO_RETRY: "전송 결과를 확정할 수 없는 주문이 있습니다. 중복 주문을 막기 위해 재전송하지 않습니다. 토스 주문 내역을 확인해 주세요.",
  TOSS_OWNED_CLOSE_REGULAR_SESSION_REQUIRED: "이번 운용 보유분 정리는 원래 선택한 거래시간 범위에서 가능합니다.",
};
function message(value) {
  if (!value) return "";
  const code = Object.keys(reasons).find(code => value === code || value.startsWith(`${code}:`));
  return code ? reasons[code] : value;
}
function money(value, currency) { return `${Number(value).toLocaleString("ko-KR", { maximumFractionDigits: currency === "USD" ? 2 : 0 })} ${currency}`; }

const wholeCount = value => Number.isSafeInteger(value) && value >= 0 ? value : null;
const displayQuantity = value => typeof value === "string" && /^\d+(?:\.\d+)?$/.test(value) && Number.isFinite(Number(value))
  ? Number(value).toLocaleString("ko-KR", { maximumFractionDigits: 8 }) : null;
const tossTime = at => typeof at === "number" && Number.isFinite(new Date(at * 1000).getTime())
  ? new Date(at * 1000).toLocaleString("ko-KR", { timeZone: "Asia/Seoul", month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" })
  : "";
const decisionLabels = { BUY: "매수 신호", SELL: "매도 신호", HOLD: "유지" };

export function projectTossCashMonitor(snapshot, observedAt = Date.now() / 1000) {
  const { ledger, ownerId, startedAt, state } = snapshot || {};
  const scopes = ledger?.scope?.scopes;
  // The status DTO can still contain the preceding ledger immediately after
  // a new run starts. Its exact coordination owner is the scope authority.
  const ownsLedger = typeof ownerId === "string" && ownerId.length > 0 && Array.isArray(scopes) && scopes.length > 0
    && scopes.every(row => row?.ownerId === ownerId && row.broker === "toss" && row.market === "CASH"
      && typeof row.symbol === "string" && row.symbol.length > 0);
  const symbols = new Set(ownsLedger ? scopes.map(row => row.symbol) : []);
  const unresolved = ownsLedger ? wholeCount(ledger.unresolvedOrders) : null;
  const specs = ownsLedger && Array.isArray(ledger.scope.specs) ? ledger.scope.specs : [];
  const positions = ledger?.positions;
  const instruments = specs.filter(row => row?.strategyInstanceId && symbols.has(row.symbol)).map(row => {
    let position = null;
    if (positions && typeof positions === "object" && !Array.isArray(positions)) {
      position = Object.hasOwn(positions, row.strategyInstanceId)
        ? displayQuantity(positions[row.strategyInstanceId])
        : unresolved === 0 ? "0" : null;
    }
    const profile = row.parameters?.stockSessionProfile;
    const session = profile?.mode === "REGULAR" ? "정규장" : profile?.mode === "EXTENDED" ? "시간외" : "";
    return { id: row.strategyInstanceId, symbol: row.symbol, timeframe: row.timeframe, session, position, plugin: row.pluginId };
  });
  const notes = Array.isArray(snapshot?.activity) ? snapshot.activity : [];
  const hasRunScope = ownsLedger && typeof startedAt === "number" && Number.isFinite(startedAt);
  const current = hasRunScope && state === "RUNNING";
  const activities = hasRunScope ? notes.filter(row => typeof row?.at === "number" && row.at >= startedAt && row.at <= observedAt + 5)
    .slice().sort((left, right) => right.at - left.at) : [];
  const newest = (kind, pattern) => {
    for (const row of activities) {
      if (row.kind !== kind || typeof row.detail !== "string") continue;
      const found = pattern.exec(row.detail);
      if (found && symbols.has(found[1])) return { symbol: found[1], side: found[2], at: row.at };
    }
    return null;
  };
  return {
    instruments,
    evaluations: current ? wholeCount(snapshot.evaluations) : null,
    acknowledged: current ? wholeCount(snapshot.ordersSubmitted) : null,
    filled: ownsLedger ? wholeCount(ledger.fillCount) : null,
    unresolved,
    latestDecision: current ? newest("EVALUATION", /^([A-Z0-9_.:/-]+) (BUY|SELL|HOLD)$/) : null,
    latestOrder: current ? newest("ORDER", /^([A-Z0-9_.:/-]+) (BUY|SELL) [1-9]\d*(?:\.\d+)?$/) : null,
    calculations: activities.flatMap(row => {
      const entry = row.calculation;
      if (row.kind !== "EVALUATION" || !entry || !Object.hasOwn(decisionLabels, entry.signal)
          || row.detail !== `${entry.symbol} ${entry.signal}`
          || !instruments.some(item => item.symbol === entry.symbol && item.timeframe === entry.timeframe)) return [];
      return [{ at: row.at, symbol: entry.symbol, timeframe: entry.timeframe,
        reason: typeof entry.reason === "string" ? entry.reason : "",
        values: signalDiagnostics(entry.metadata) }];
    }),
  };
}

function TossCashMonitor({ snapshot, state, active, error, now }) {
  const view = projectTossCashMonitor(snapshot, now / 1000);
  const showScope = view.instruments.length > 0;
  const showStats = [view.evaluations, view.acknowledged, view.filled, view.unresolved].some(value => value !== null);
  const activity = Array.isArray(snapshot?.activity) ? snapshot.activity : [];
  const stateLabel = error ? "상태 갱신 미확인" : !snapshot ? "상태 확인 중"
    : state === "RUNNING" && snapshot.running !== true ? "실행 여부 미확인"
    : state === "FAILED" ? "운용 오류" : labels[state] || state;
  return <section className="panel toss-cash-panel live-grid-full" aria-label="토스 현금 운용 감시">
    <div className="toss-cash-heading"><h2>토스 현금 운용 감시</h2>
      <strong className={active && !error && snapshot?.running === true ? "toss-cash-good" : "muted"}>{stateLabel}</strong></div>
    {snapshot?.reason && <p role="status">{message(snapshot.reason)}</p>}
    {error && <p className="toss-cash-error" role="alert">{message(error)}{snapshot && " · 아래는 마지막 수신 기록입니다."}</p>}
    {showStats && <div className="toss-cash-stats" aria-label="운용 관측 횟수">
      <span>봉 평가 <b>{view.evaluations ?? "—"}</b></span>
      <span>주문 접수 <b>{view.acknowledged ?? "—"}</b></span>
      <span>체결 확인 주문 <b>{view.filled ?? "—"}</b></span>
      <span>종료 미확인 주문 <b>{view.unresolved ?? "—"}</b></span>
    </div>}
    {showScope ? <div className="toss-cash-watch">
      <table aria-label="토스 운용 감시 종목">
        <thead><tr><th scope="col">감시 종목</th><th scope="col">시간봉 · 거래 시간</th><th scope="col">이번 운용 보유 수량</th></tr></thead>
        <tbody>{view.instruments.map((item, index) => <tr key={`${item.id}:${index}`}>
          <th scope="row">{item.symbol}{item.plugin && <small>{item.plugin}</small>}</th>
          <td>{[item.timeframe, item.session].filter(Boolean).join(" · ") || "미확인"}</td>
          <td>{item.position === null ? "미확인" : `${item.position}주`}</td>
        </tr>)}</tbody>
      </table>
      <p className="muted">보유 수량은 종료 주문 대조 기준입니다. 이번 운용의 수익률은 미기록입니다.</p>
    </div> : <p className="muted">확인된 토스 운용 범위가 없습니다.</p>}
    {view.latestDecision && <p className="toss-cash-latest"><b>최근 전략 판단</b> {view.latestDecision.symbol} · {decisionLabels[view.latestDecision.side]} <small>{tossTime(view.latestDecision.at)}</small></p>}
    {showScope && state === "RUNNING" && !view.latestDecision && <p className="muted">최근 전략 판단 미관측</p>}
    {view.latestOrder && <p className="toss-cash-latest"><b>최근 주문 접수 기록</b> {view.latestOrder.symbol} · {view.latestOrder.side === "BUY" ? "매수" : "매도"} <small>{tossTime(view.latestOrder.at)}</small></p>}
    <details className="ts-disclosure toss-cash-options"><summary>진행 상세</summary>
      <p className="muted">최근 활동 기록입니다. 주문 접수 후 체결 수량은 대조 결과에 반영됩니다.</p>
      <ol>{activity.filter(row => typeof row?.detail === "string" && tossTime(row.at)).map((row, index) =>
        <li key={`${row.at}:${index}`}>{tossTime(row.at)} · {row.detail}</li>)}</ol>
      {view.calculations.length > 0 && <ol className="tower-event-list tower-event-details" aria-label="전략 계산 기록">
        {view.calculations.map((entry, index) => <li key={`${entry.at}:${index}`}>
          <time>{tossTime(entry.at)}</time>
          <div><strong>{entry.symbol} · {entry.timeframe}</strong>
            {entry.reason && <p>{entry.reason}</p>}
            {entry.values.length > 0 && <dl className="tower-signal-values">
              {entry.values.map(({label,value}) => <div key={label}><dt>{label}</dt><dd>
                {typeof value === "number" ? value.toLocaleString("ko-KR", { maximumFractionDigits: 8 }) : value}
              </dd></div>)}
            </dl>}
          </div>
        </li>)}
      </ol>}
      {snapshot?.firstFailure && <p className="toss-cash-error">최초 오류 · {message(snapshot.firstFailure)}</p>}
    </details>
  </section>;
}

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
          if (monitorOnly) setError("");
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

  if (monitorOnly) return <TossCashMonitor snapshot={snapshot} state={state} active={active} error={error} now={now} />;
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
      {snapshot?.firstFailure && <p className="toss-cash-error">최초 오류 · {message(snapshot.firstFailure)}</p>}
    </details>
  </section>;
}
