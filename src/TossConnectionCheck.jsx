import { useState } from "react";
import { request } from "./api.js";
import "./TossConnectionCheck.css";

export default function TossConnectionCheck({ onSave }) {
  const [symbol, setSymbol] = useState("005930");
  const [marketCountry, setMarket] = useState("KR");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState(null);
  const [accounts, setAccounts] = useState([]);
  const [accountSeq, setAccountSeq] = useState("");
  const [accountNotice, setAccountNotice] = useState("");
  async function discoverAccounts() {
    if (busy) return;
    setBusy(true); setResult(null); setAccounts([]); setAccountSeq(""); setAccountNotice("");
    try {
      const payload = await request("/api/toss/accounts", { method: "POST", body: {}, timeoutMs: 60000 });
      if (!payload.ok) throw new Error(payload.reason || "계좌 목록 조회를 확인하지 못했습니다.");
      setAccounts(payload.accounts || []);
      setAccountSeq((payload.accounts || []).find(row => row.selected)?.accountSeq || "");
      setAccountNotice(payload.accounts?.length ? payload.detail : "선택 가능한 일반 증권 계좌가 없습니다.");
    } catch (err) { setAccountNotice(err.message); }
    finally { setBusy(false); }
  }
  async function selectAccount() {
    if (busy || !accountSeq || typeof onSave !== "function") return;
    setBusy(true); setResult(null);
    try {
      const saved = await onSave({ TOSS_ACCOUNT_SEQ: accountSeq }, false);
      if (saved?.ok !== true) throw new Error(saved?.reason || "계좌 선택 저장을 확인하지 못했습니다.");
      setAccountNotice("선택한 계좌를 저장했습니다. 계좌·종목 확인을 진행하세요. 기존 준비 결과는 다시 검사해야 합니다.");
    } catch (err) { setAccountNotice(err.message); }
    finally { setBusy(false); }
  }
  async function inspect() {
    setBusy(true); setResult(null);
    try { setResult(await request("/api/toss/prepare", { method: "POST", body: { symbol: symbol.trim().toUpperCase(), marketCountry }, timeoutMs: 60000 })); }
    catch (err) { setResult({ ok: false, reason: err.message }); }
    finally { setBusy(false); }
  }
  return <details className="panel ts-disclosure live-toss-connection-check">
    <summary><strong>토스증권 연결 확인</strong><span className="muted"> · 주문 없이 계좌·종목 조회</span></summary>
    <p className="muted">‘설정’에서 토스증권 Client ID와 Secret을 저장한 뒤 계좌를 조회하세요.</p>
    <div className="button-row"><button disabled={busy} onClick={discoverAccounts}>계좌 목록 조회 (주문 없음)</button>
      {accounts.length > 0 && <><select aria-label="토스에서 사용할 계좌" disabled={busy} value={accountSeq} onChange={e => { setAccountSeq(e.target.value); setResult(null); }}><option value="">사용할 계좌 선택</option>{accounts.map(row => <option key={row.accountSeq} value={row.accountSeq}>{row.label}</option>)}</select><button disabled={busy || !accountSeq} onClick={selectAccount}>선택한 계좌 저장</button></>}
    </div>
    {accountNotice && <p role="status">{accountNotice}</p>}
    <div className="toss-connection-fields"><label><span>시장</span><select value={marketCountry} onChange={e => { setMarket(e.target.value); setSymbol(e.target.value === "KR" ? "005930" : "AAPL"); setResult(null); }}><option value="KR">한국주식 · KRW</option><option value="US">미국주식 · USD</option></select></label><label><span>종목</span><input value={symbol} onChange={e => { setSymbol(e.target.value); setResult(null); }} /></label></div>
    <button onClick={inspect} disabled={busy || !symbol.trim()}>{busy ? "조회 중…" : "계좌·종목 확인 (주문 없음)"}</button>
    {result && <div aria-live="polite"><p className={result.ok ? "toss-connection-success" : "toss-connection-error"}>{result.ok ? `${result.name} · ${result.market} · 미체결 ${result.openOrderCount}건` : result.reason}</p>{result.ok && <><p>{result.detail}</p>{result.accounts?.map(row => <p key={row.currency}>주문 가능 금액 {Number(row.available_cash).toLocaleString()} {row.currency} · 총자산과 다릅니다.</p>)}</>}</div>}
  </details>;
}
