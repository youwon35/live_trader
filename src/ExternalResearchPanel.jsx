import React, { useState } from "react";
import { request } from "./api.js";
import "./ExternalResearchPanel.css";

function officialLink(value, pathPrefix = "/") {
  try { const url = new URL(value); return url.protocol === "https:" && (url.hostname === "www.tradingview.com" || url.hostname === "tradingview.com") && url.pathname.startsWith(pathPrefix) ? url.href : null; }
  catch { return null; }
}

export default function ExternalResearchPanel() {
  const [symbol, setSymbol] = useState("KRX:005930");
  const [interval, setInterval] = useState("1h");
  const [status, setStatus] = useState(null);
  const [result, setResult] = useState(null);
  const [url, setUrl] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  async function run(action) {
    if (busy) return;
    setBusy(true); setError(""); setUrl("");
    try {
      const payload = await request(`/api/tradingview/${action}`, action === "status" ? {} : {
        method: "POST", body: { symbol: symbol.trim(), interval }, timeoutMs: 45000,
      });
      if (!payload.ok) throw new Error(payload.reason || "분석 요청을 확인하지 못했습니다.");
      if (action === "analyze") setResult(payload);
      else if (action === "chart" || action === "connect") {
        const link = officialLink(payload.chartUrl || payload.authorizationUrl);
        if (!link) throw new Error("TradingView 공식 링크를 확인하지 못했습니다.");
        setUrl(link);
      } else setStatus(payload);
    } catch (err) { setError(err.message); }
    finally { setBusy(false); }
  }
  return <details className="ts-disclosure panel external-research-panel">
    <summary><strong>TradingView 추가 분석·차트</strong><span className="muted"> · 필요할 때만 열기</span></summary>
    <p className="muted">추가 분석 전용입니다. 결과는 전략 신호나 자동주문에 전달하지 않습니다. 연결은 TradingView 로그인 화면에서 직접 승인합니다.</p>
    <div className="research-fields">
      <label><span>TradingView 종목</span><input value={symbol} onChange={e => { setSymbol(e.target.value); setResult(null); setUrl(""); }} placeholder="KRX:005930 · NASDAQ:AAPL · BINANCE:BTCUSDT" /></label>
      <label><span>주기</span><select value={interval} onChange={e => { setInterval(e.target.value); setResult(null); }}>{["1m", "5m", "15m", "1h", "4h", "1d"].map(value => <option key={value} value={value}>{value}</option>)}</select></label>
    </div>
    <div className="button-row">
      <button disabled={busy || !symbol.trim()} onClick={() => run("chart")}>차트 링크 만들기</button>
      <button disabled={busy} onClick={() => run("connect")}>TradingView 연결</button>
      <button disabled={busy || !symbol.includes(":")} onClick={() => run("analyze")}>{busy ? "확인 중…" : "추가 분석"}</button>
      <button disabled={busy} onClick={() => run("status")}>연결 확인</button>
      <button disabled={busy} onClick={() => run("disconnect")}>연결 해제</button>
    </div>
    {url && <p><a href={url} target="_blank" rel="noopener noreferrer">TradingView에서 열기 ↗</a></p>}
    {status && <p className="muted">{status.connected ? "TradingView 연결됨" : "TradingView 연결 전"}</p>}
    {error && <p role="alert" className="research-error">{error}</p>}
    {result && <div aria-live="polite"><strong>추가 분석 · 주문과 분리</strong><ul>{(result.summary || []).map((line, i) => <li key={i}>{line}</li>)}</ul><p className="muted">관측 시각 {result.observedAt || "미확인"}</p></div>}
  </details>;
}

export function TossConnectionCheck({ onSave }) {
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
  return <details className="ts-disclosure panel external-research-panel">
    <summary><strong>토스증권 연결 확인</strong><span className="muted"> · 주문 없이 계좌·종목 조회</span></summary>
    <p className="muted">위에서 Client ID와 Secret 두 항목을 먼저 저장한 뒤 계좌 목록을 조회하세요. Account Seq는 공식 API의 계좌 식별값으로 일반 계좌번호와 다릅니다. 아래 목록에서 선택하면 직접 입력할 필요가 없습니다.</p>
    <div className="button-row"><button disabled={busy} onClick={discoverAccounts}>계좌 목록 조회 (주문 없음)</button>
      {accounts.length > 0 && <><select aria-label="토스에서 사용할 계좌" disabled={busy} value={accountSeq} onChange={e => { setAccountSeq(e.target.value); setResult(null); }}><option value="">사용할 계좌 선택</option>{accounts.map(row => <option key={row.accountSeq} value={row.accountSeq}>{row.label}</option>)}</select><button disabled={busy || !accountSeq} onClick={selectAccount}>선택한 계좌 저장</button></>}
    </div>
    {accountNotice && <p role="status">{accountNotice}</p>}
    <p className="muted">토스 현금 운용은 실거래 운용 탭에서 전략·예산을 준비한 뒤 직접 시작합니다. 연결 확인만으로 거래가 시작되지는 않습니다.</p>
    <div className="research-fields"><label><span>시장</span><select value={marketCountry} onChange={e => { setMarket(e.target.value); setSymbol(e.target.value === "KR" ? "005930" : "AAPL"); setResult(null); }}><option value="KR">한국주식 · KRW</option><option value="US">미국주식 · USD</option></select></label><label><span>종목</span><input value={symbol} onChange={e => { setSymbol(e.target.value); setResult(null); }} /></label></div>
    <button onClick={inspect} disabled={busy || !symbol.trim()}>{busy ? "조회 중…" : "계좌·종목 확인 (주문 없음)"}</button>
    {result && <div aria-live="polite"><p className={result.ok ? "research-success" : "research-error"}>{result.ok ? `${result.name} · ${result.market} · 미체결 ${result.openOrderCount}건` : result.reason}</p>{result.ok && <><p>{result.detail}</p>{result.accounts?.map(row => <p key={row.currency}>주문 가능 금액 {Number(row.available_cash).toLocaleString()} {row.currency} · 총자산과 다릅니다.</p>)}</>}</div>}
  </details>;
}
