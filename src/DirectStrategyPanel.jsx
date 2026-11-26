import { useEffect, useRef, useState } from "react";
import { getDirectStrategies, registerDirectStrategy } from "./api";

const candidateKey = (row) => row.rootKey + ":" + row.candidateId;

export default function DirectStrategyPanel({ onConnected }) {
  const [data, setData] = useState(null);
  const [key, setKey] = useState("");
  const [profile, setProfile] = useState("");
  const [percent, setPercent] = useState(10);
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const pending = useRef(null);
  const candidates = data?.candidates ?? [];
  const selected = candidates.find((row) => candidateKey(row) === key);
  const accounts = (data?.accounts ?? []).filter((row) => row.brokerId === selected?.brokerId);
  const cashPercent = Number(percent);
  const canConnect = selected && accounts.some((row) => row.profileId === profile) && cashPercent > 0 && cashPercent <= 100;

  function run(kind, work) {
    if (pending.current) return pending.current;
    setBusy(kind);
    setError("");
    const flight = Promise.resolve().then(work)
      .catch((failure) => setError(failure.message || "요청을 완료하지 못했습니다."))
      .finally(() => {
        pending.current = null;
        setBusy("");
      });
    pending.current = flight;
    return flight;
  }

  async function readCatalog() {
    // Clear the last hash before reading so an unavailable store cannot submit stale choices.
    setData(null);
    const next = await getDirectStrategies();
    if (next?.ok !== true || !Array.isArray(next.candidates) || !Array.isArray(next.accounts)) {
      throw new Error(next?.reason || "Backtester 전략 목록을 읽지 못했습니다.");
    }
    setData(next);
  }

  const refresh = () => run("read", readCatalog);
  useEffect(() => { void refresh(); }, []);

  function connect() {
    if (!canConnect) return;
    return run("connect", async () => {
      setMessage("");
      const result = await registerDirectStrategy({ rootKey: selected.rootKey, candidateId: selected.candidateId,
        identityHash: selected.identityHash, registryHash: selected.registryHash,
        accountProfileId: profile, capitalPercent: cashPercent });
      if (result?.ok !== true) throw new Error(result?.reason || result?.detail || "전략과 계좌를 연결하지 못했습니다.");
      setMessage(result.detail || "전략과 계좌를 연결했습니다.");
      const deploymentId = typeof result.deploymentId === "string" ? result.deploymentId.trim() : "";
      try {
        if (!deploymentId || !(await onConnected?.(deploymentId))) {
          setMessage("전략과 계좌를 연결했습니다. 운용 전략 목록을 다시 읽어 선택하세요.");
        }
      } catch {
        setMessage("전략과 계좌를 연결했습니다. 운용 전략 목록을 다시 읽어 선택하세요.");
      }
      // Registration changes the registry hash. Read the source catalog once for any next connection.
      await readCatalog();
    });
  }

  return <div className="ts-layout-stack" aria-busy={Boolean(busy)}>
    <p>Backtester 전략과 사용할 계좌를 선택하세요. 연결하면 설정만 저장합니다. 주문은 보내지 않습니다.</p>
    <div className="live-strategy-selector-grid">
      <label><span>전략 또는 포트폴리오</span><select disabled={Boolean(busy)} value={key} onChange={(event) => { setKey(event.target.value); setProfile(""); setMessage(""); }}>
        <option value="">선택하세요</option>{candidates.map((row) => <option key={candidateKey(row)} value={candidateKey(row)}>{row.name} · {row.brokerId} · {row.members.length}개 실행 단위</option>)}
      </select></label>
      <label><span>사용할 계좌</span><select value={profile} disabled={Boolean(busy) || !selected} onChange={(event) => setProfile(event.target.value)}><option value="">선택하세요</option>{accounts.map((row) => <option key={row.profileId} value={row.profileId}>{row.alias} {row.maskedAccount}</option>)}</select></label>
      <label><span>가용 현금 중 사용할 비율 (%)</span><input type="number" min="0.01" max="100" step="0.01" value={percent} disabled={Boolean(busy)} onChange={(event) => setPercent(event.target.value)} /></label>
    </div>
    <div className="live-strategy-control-line">
      <button type="button" className="secondary-button" disabled={Boolean(busy)} onClick={refresh}>전략 목록 새로고침</button>
      <button type="button" className="primary-button" disabled={Boolean(busy) || !canConnect} onClick={connect}>{busy === "connect" ? "연결 중…" : "전략과 계좌 연결"}</button>
    </div>
    {busy === "read" && <p role="status">Backtester 전략 목록을 읽는 중입니다.</p>}
    {message && <p role="status">{message}</p>}{error && <p role="alert" style={{ color: "var(--danger)" }}>{error}</p>}
    {data && !busy && !candidates.length && <p>연결할 전략이 없습니다. Backtester에서 실제 데이터로 검증한 전략을 저장한 뒤 ‘전략 목록 새로고침’을 누르세요.</p>}
    {selected && !accounts.length && <p>이 전략에 맞는 거래소 계좌를 ‘설정’에서 먼저 저장하세요.</p>}
    <details><summary>계좌 연결 기준</summary><p>포트폴리오 하나에는 같은 거래소의 계좌 하나를 연결합니다. 거래소가 여러 곳이면 각각 연결하세요. 전략 원본이나 계좌 설정이 바뀌면 다시 연결해야 합니다.</p></details>
  </div>;
}
