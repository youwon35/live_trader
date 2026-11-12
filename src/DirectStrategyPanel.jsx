import { useEffect, useState } from "react";
import { getDirectStrategies, registerDirectStrategy } from "./api";

export default function DirectStrategyPanel({ onRegistered }) {
  const [data, setData] = useState({ candidates: [], accounts: [] });
  const [key, setKey] = useState("");
  const [profile, setProfile] = useState("");
  const [percent, setPercent] = useState(10);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const candidates = data.candidates ?? [];
  const selected = candidates.find((row) => `${row.rootKey}:${row.candidateId}` === key);
  const accounts = (data.accounts ?? []).filter((row) => row.brokerId === selected?.brokerId);
  async function refresh() {
    setBusy(true); setError("");
    try { setData(await getDirectStrategies()); }
    catch (failure) { setError(failure.message || "전략 목록을 읽지 못했습니다."); }
    finally { setBusy(false); }
  }
  useEffect(() => { refresh(); }, []);
  async function connect() {
    if (!selected) return;
    setBusy(true); setError(""); setMessage("");
    try {
      const result = await registerDirectStrategy({ rootKey: selected.rootKey, candidateId: selected.candidateId,
        identityHash: selected.identityHash, registryHash: selected.registryHash,
        accountProfileId: profile, capitalPercent: Number(percent) });
      if (!result.ok) throw new Error(result.reason || result.detail || "연결하지 못했습니다.");
      setMessage(result.detail); await onRegistered?.(result); await refresh();
    } catch (failure) { setError(failure.message || "연결하지 못했습니다."); }
    finally { setBusy(false); }
  }
  return <div className="ts-layout-stack">
    <p>Backtester 전략을 계좌에 연결합니다. Paper 실행 결과 없이 사용할 수 있으며, 실제 시작 전 자료·계좌·주문 보호를 점검합니다.</p>
    <div className="live-strategy-selector-grid">
      <label><span>전략 또는 포트폴리오</span><select disabled={busy} value={key} onChange={(event) => { setKey(event.target.value); setProfile(""); }}>
        <option value="">선택하세요</option>{candidates.map((row) => <option key={`${row.rootKey}:${row.candidateId}`} value={`${row.rootKey}:${row.candidateId}`}>{row.name} · {row.brokerId} · {row.members.length}개 실행 단위</option>)}
      </select></label>
      <label><span>연결 계좌</span><select value={profile} disabled={busy || !selected} onChange={(event) => setProfile(event.target.value)}><option value="">선택하세요</option>{accounts.map((row) => <option key={row.profileId} value={row.profileId}>{row.alias} {row.maskedAccount}</option>)}</select></label>
      <label><span>가용 현금 사용 비율 (%)</span><input type="number" min="0.01" max="100" step="0.01" value={percent} disabled={busy} onChange={(event) => setPercent(event.target.value)} /></label>
    </div>
    <div className="live-strategy-control-line"><button className="secondary-button" disabled={busy} onClick={refresh}>목록 새로고침</button><button disabled={busy || !selected || !profile || !(Number(percent) > 0 && Number(percent) <= 100)} onClick={connect}>{busy ? "확인 중…" : "전략 연결 (주문 없음)"}</button></div>
    {message && <p role="status">{message}</p>}{error && <p role="alert" style={{ color: "var(--danger)" }}>{error}</p>}
    {!busy && !candidates.length && <p>현재 연결 가능한 실제 데이터 전략이 없습니다. Backtester에서 실행 계약을 저장한 뒤 새로고침하세요. 샘플 데이터 전략은 Paper에서 사용할 수 있습니다.</p>}
    {selected && !accounts.length && <p>이 전략의 거래소 계좌를 연결·설정에서 먼저 저장하세요.</p>}
    <details><summary>연결과 실행의 차이</summary><p>연결은 원본 전략·실행 단위·계좌·사용 비율만 저장합니다. 잔고를 조회하거나 주문을 보내지 않습니다. 원본이나 계좌 설정이 바뀌면 다시 연결해야 합니다. 현재 일반 운용은 포트폴리오별 한 거래소·계좌를 사용합니다. 거래소가 다르면 각각 연결하여 함께 실행할 수 있습니다.</p></details>
  </div>;
}
