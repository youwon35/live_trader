import * as React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Clock3, Play, RefreshCw, ShieldAlert, Square } from "lucide-react";
import { getShortLiveTestSnapshot, prepareShortLiveTest, startShortLiveTest, stopShortLiveTest } from "./shortLiveTestApi.js";
import {
  SHORT_LIVE_CONFIRMATION, SHORT_LIVE_DEFAULTS, SHORT_LIVE_ROUTES, SHORT_LIVE_TIMEFRAMES,
  buildShortLivePrepare, normalizeShortLiveSnapshot, sanitizeShortLiveText, shortLiveBlockerLabel,
  shortLiveCanStart, shortLiveCoverage, shortLiveRunning, shortLiveRuntimeMeaning, shortLiveSelectionSignature, shortLivePreparationMatches,
} from "./shortLiveTestModel.js";
import "./ShortLiveTestPanel.css";

const defaultApi = { snapshot: getShortLiveTestSnapshot, prepare: prepareShortLiveTest, start: startShortLiveTest, stop: stopShortLiveTest };
const money = (value) => value === null || value === undefined ? "미확인" : `${new Intl.NumberFormat("ko-KR").format(value)}원`;
const date = (value) => { const parsed = new Date(value); return value && !Number.isNaN(parsed.getTime()) ? parsed.toLocaleString("ko-KR", { timeZone: "Asia/Seoul" }) : "미기록"; };
const kindLabel = (kind) => kind === "portfolio" ? "포트폴리오" : "인스턴스";
const compactHash = (value) => value ? `${value.slice(0, 10)}…${value.slice(-6)}` : "원본 해시 없음";

function Blockers({ values = [], label = "진행을 막는 사유" }) {
  const rows = [...new Set(values.map(shortLiveBlockerLabel))];
  return rows.length ? <div className="short-live-blockers" role="status" aria-label={label}><strong>{label}</strong><ul>{rows.map((row) => <li key={row}>{row}</li>)}</ul></div> : null;
}

export default function ShortLiveTestPanel({ api = defaultApi, initialSnapshot, pollMs = 5000, legacyWorkspace = null }) {
  const [snapshot, setSnapshot] = useState(() => normalizeShortLiveSnapshot(initialSnapshot));
  const [fresh, setFresh] = useState(false);
  const [selection, setSelection] = useState([]);
  const [durationHours, setDurationHours] = useState(6);
  const [routeFilter, setRouteFilter] = useState("ALL");
  const [timeframeFilter, setTimeframeFilter] = useState("ALL");
  const [kindFilter, setKindFilter] = useState("ALL");
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [acknowledged, setAcknowledged] = useState(false);
  const [confirmationText, setConfirmationText] = useState("");
  const [preparedSelection, setPreparedSelection] = useState("");
  const [preparedAuthority, setPreparedAuthority] = useState("");
  const [unknownMutation, setUnknownMutation] = useState("");
  const [showLegacy, setShowLegacy] = useState(false);
  const mounted = useRef(false);
  const pollFlight = useRef(false);
  const selectionSignature = shortLiveSelectionSignature(snapshot.catalog, selection, durationHours);
  const active = shortLiveRunning(snapshot);
  const locked = Boolean(busy) || active || unknownMutation;
  const coverage = useMemo(() => shortLiveCoverage(snapshot.catalog), [snapshot.catalog]);
  const visibleCandidates = snapshot.catalog.filter((row) => (routeFilter === "ALL" || row.routes.includes(routeFilter))
    && (timeframeFilter === "ALL" || row.timeframes.includes(timeframeFilter)) && (kindFilter === "ALL" || row.kind === kindFilter));
  const selectedCandidates = snapshot.catalog.filter((row) => selection.includes(row.selectionKey));
  const canStart = !busy && !unknownMutation && shortLiveCanStart(snapshot, {
    fresh, acknowledged, confirmationText, selectionUnchanged: preparedSelection === selectionSignature, preparedAuthority,
    preparedScopeMatches: shortLivePreparationMatches(snapshot.prepared, snapshot.catalog, selection, durationHours),
  });

  const refresh = useCallback(async () => {
    if (pollFlight.current) return;
    pollFlight.current = true;
    try {
      const next = normalizeShortLiveSnapshot(await api.snapshot());
      if (!mounted.current) return;
      setSnapshot(next); setFresh(true);
      return next;
    } catch (failure) {
      if (mounted.current) { setFresh(false); setError(shortLiveBlockerLabel(failure?.message)); }
    } finally { pollFlight.current = false; }
  }, [api]);

  useEffect(() => {
    mounted.current = true; refresh();
    const timer = setInterval(refresh, Math.max(1000, pollMs));
    return () => { mounted.current = false; clearInterval(timer); };
  }, [refresh, pollMs]);

  useEffect(() => { setAcknowledged(false); setConfirmationText(""); }, [selectionSignature, snapshot.prepared?.scopeHash]);

  function changeSelection(key) {
    if (locked) return;
    setSelection((rows) => rows.includes(key) ? rows.filter((item) => item !== key) : [...rows, key]);
    setPreparedSelection(""); setMessage("");
  }

  async function prepare() {
    if (locked || !fresh) return;
    let body;
    try { body = buildShortLivePrepare(snapshot.catalog, selection, { ...SHORT_LIVE_DEFAULTS, durationHours }); }
    catch (failure) { setError(failure.message); return; }
    setBusy("prepare"); setError(""); setMessage("");
    try {
      const result = await api.prepare(body);
      if (result?.snapshot || result?.catalog) setSnapshot(normalizeShortLiveSnapshot(result));
      const observed = await refresh();
      if (observed?.prepared) {
        setPreparedSelection(selectionSignature);
        setPreparedAuthority(`${observed.prepared.preparationId}:${observed.prepared.scopeHash}`);
      }
      setMessage("무주문 준비 결과를 확인했습니다. 차단 사유와 실제 시험 범위를 확인한 뒤 직접 시작해 주세요.");
    } catch (failure) {
      setPreparedSelection(""); setPreparedAuthority(""); setFresh(false);
      setError(shortLiveBlockerLabel(failure?.message));
      await refresh();
    } finally { setBusy(""); }
  }

  async function start() {
    if (!canStart) return;
    const prepared = snapshot.prepared;
    setBusy("start"); setError(""); setMessage("");
    try {
      const result = await api.start({ preparationId: prepared.preparationId, scopeHash: prepared.scopeHash,
        confirmed: true, confirmationText: SHORT_LIVE_CONFIRMATION });
      if (result?.snapshot || result?.catalog) setSnapshot(normalizeShortLiveSnapshot(result));
      setAcknowledged(false); setConfirmationText("");
      setMessage("시작 요청을 보냈습니다. 실제 실행·주문·체결 상태는 아래 관측 결과로 확인합니다.");
      await refresh();
    } catch (failure) {
      setUnknownMutation("start"); setFresh(false);
      setError(`시작 결과 미확정 · 자동 재시도하지 않습니다. ${shortLiveBlockerLabel(failure?.message)}`);
      await refresh();
    } finally { setBusy(""); }
  }

  async function stop() {
    if (busy || unknownMutation === "stop" || !snapshot.runtime?.sessionId) return;
    setBusy("stop"); setError(""); setMessage("");
    try {
      const result = await api.stop({ sessionId: snapshot.runtime.sessionId, reason: "사용자가 짧은 실거래 시험의 정상 중지를 요청함" });
      if (result?.snapshot || result?.catalog) setSnapshot(normalizeShortLiveSnapshot(result));
      setMessage("정상 중지를 요청했습니다. 신규 진입 차단·미체결 대조·실제 프로세스 종료가 확인될 때까지 상태를 확인합니다.");
      await refresh();
    } catch (failure) {
      setUnknownMutation("stop"); setFresh(false);
      setError(`중지 결과 미확정 · 반복 호출하지 않습니다. ${shortLiveBlockerLabel(failure?.message)}`);
      await refresh();
    } finally { setBusy(""); }
  }

  return <div className="short-live-workspace" aria-label="짧은 실거래 기능시험">
    <section className="panel short-live-intro">
      <div className="short-live-heading"><div><p className="short-live-eyebrow">5~7시간 · 실제 주문 경로 확인</p><h2>짧은 실거래 기능시험</h2></div>
        <button type="button" className="secondary-button" onClick={refresh} disabled={Boolean(busy)}><RefreshCw size={16} /> 상태 새로 확인</button></div>
      <p>단일·복수 전략을 인스턴스와 포트폴리오로 연결하고 실제 신호 → 주문 → 체결 → 정상 종료를 확인합니다. 수익성이나 30일 검증을 통과했다는 의미는 아닙니다.</p>
      <div className="short-live-notice"><ShieldAlert size={19} /><span>준비 버튼은 주문을 보내지 않습니다. 실제 API 주문은 준비 결과를 확인하고 아래 시작 버튼을 직접 누른 뒤에만 요청합니다.</span></div>
      {snapshot.fixture && <p className="short-live-fixture" role="note">오프라인 화면 검사 · 표시 자료는 fixture이며 실계좌 조회·실주문 결과가 아닙니다.</p>}
      <p className="short-live-meta">상태 {snapshot.status} · 관측 {date(snapshot.observedAt)} · {fresh ? "최근 조회 성공" : "현재 상태 미확인 · 시작 차단"}</p>
      {error && <p className="short-live-error" role="alert">{error}</p>}
      {message && <p className="short-live-message" role="status">{message}</p>}
      {unknownMutation && <p role="alert">요청 결과가 미확정입니다. 새 시작을 막고 기존 실행 상태를 읽기로 확인합니다. 필요하면 실행 중 시험의 정상 중지를 요청하세요.</p>}
      <Blockers values={snapshot.blockers} />
    </section>

    <section className="panel"><h3>1. 거래소·시간봉·시험 대상 선택</h3>
      <div className="short-live-filters">
        <label>주문 경로<select aria-label="짧은 시험 주문 경로" value={routeFilter} onChange={(event) => setRouteFilter(event.target.value)}><option value="ALL">모든 경로</option>{SHORT_LIVE_ROUTES.map((row) => <option key={row.id} value={row.id}>{row.label}</option>)}</select></label>
        <label>시간봉<select aria-label="짧은 시험 시간봉" value={timeframeFilter} onChange={(event) => setTimeframeFilter(event.target.value)}><option value="ALL">모든 시간봉</option>{SHORT_LIVE_TIMEFRAMES.map((row) => <option key={row} value={row}>{row}</option>)}</select></label>
        <label>유형<select aria-label="짧은 시험 전략 유형" value={kindFilter} onChange={(event) => setKindFilter(event.target.value)}><option value="ALL">모든 유형</option><option value="instance">인스턴스</option><option value="portfolio">포트폴리오</option></select></label>
      </div>
      <p>여러 대상을 선택하면 복수 전략 시험입니다. 포트폴리오의 종목 수와 서로 다른 전략 플러그인 수는 구분합니다.</p>
      <p>신규 기능시험 후보는 배정된 소액 시험 자금의 100%를 사용합니다. 원래 5% 후보는 5% 그대로 적용합니다. 복수 전략은 배정 자금을 나누며 거래소별 총 10만원 한도는 유지합니다.</p>
      <div className="short-live-catalog" aria-label="짧은 시험 후보 목록">
        {visibleCandidates.length === 0 && <p>이 조건에 정확히 연결된 후보가 없습니다. 다른 시간봉을 통과한 결과로 대체하지 않습니다.</p>}
        {visibleCandidates.map((row) => <label key={row.selectionKey} className={`short-live-candidate ${selection.includes(row.selectionKey) ? "is-selected" : ""}`}>
          <input type="checkbox" checked={selection.includes(row.selectionKey)} disabled={locked} onChange={() => changeSelection(row.selectionKey)} aria-label={`시험 대상 ${row.label}`} />
          <span><strong>{row.label}</strong><span className="short-live-meta">{SHORT_LIVE_ROUTES.find((route) => route.id === row.route)?.label || row.route} · {row.timeframe} · {kindLabel(row.kind)} · 전략 {row.strategyCount}개{row.distinctPluginCount !== null ? ` · 플러그인 ${row.distinctPluginCount}종` : ""}</span>
            <span className="short-live-meta">{row.symbols.join(" · ") || "종목 미확인"} · {compactHash(row.kind === "portfolio" ? row.portfolioFileSha256 : row.artifactFileSha256)}</span>
            <span className="short-live-meta">진입 비중: {row.positionSizePercents.length ? `배정 자금 ${row.positionSizePercents.map((percent) => `${percent}%`).join(" / ")}` : "원본 비중 미확인"}{row.sourcePositionSizeWasDefaulted ? " · 원래 모델의 기본 비중" : ""}</span>
            <span>{row.evidenceMeaning}</span><span className="short-live-candidate-state">{row.supported && row.available ? "준비 검사 대상" : "현재 시작 차단 · 준비 검사에서 사유 확인"}</span>
            {row.blockers.length > 0 && <span className="short-live-meta">{row.blockers.map(shortLiveBlockerLabel).join(" · ")}</span>}
          </span>
        </label>)}
      </div>
      <details className="short-live-details"><summary>전체 24조합의 후보·연결 현황</summary><div className="short-live-table-wrap"><table><thead><tr><th>경로</th><th>시간봉</th><th>유형</th><th>후보 / 준비 가능</th><th>현재 차단 사유</th></tr></thead><tbody>{coverage.map((row) => <tr key={`${row.route}:${row.timeframe}:${row.kind}`}><td>{row.routeLabel}</td><td>{row.timeframe}</td><td>{kindLabel(row.kind)}</td><td>{row.count} / {row.availableCount}</td><td>{row.blockers.length ? [...new Set(row.blockers.map(shortLiveBlockerLabel))].join(" · ") : "선택 후 현재 잔고·주문 조건 재검사"}</td></tr>)}</tbody></table></div></details>
    </section>

    <section className="panel"><h3>2. 금액·시간 확인 후 무주문 준비</h3>
      <div className="short-live-limits"><label>시험 시간<select value={durationHours} disabled={locked} aria-label="실거래 시험 시간" onChange={(event) => { setDurationHours(Number(event.target.value)); setPreparedSelection(""); }}>{[5, 6, 7].map((hours) => <option key={hours} value={hours}>{hours}시간</option>)}</select></label><div><strong>거래소별 자본 최대 100,000원</strong><p>일손실 중지선 50,000원 · Binance 현물·선물은 같은 거래소 한도 안에서 합산합니다. 실제 잔고·최소 주문 조건이 우선합니다.</p></div></div>
      {selectedCandidates.some((row) => row.routes.includes("BINANCE_USDM_FUTURES")) && <p className="short-live-notice">선물 준비안: 1x · ISOLATED. 이 화면은 레버리지나 증거금 방식을 실제 변경하지 않습니다. 계좌의 실제 설정을 확인하지 못하면 시작을 막습니다.</p>}
      <p>선택 {selection.length}개 · {durationHours}시간 · 자동 재시도 없음</p>
      <button type="button" className="primary-button" onClick={prepare} disabled={locked || !fresh || selection.length === 0}>{busy === "prepare" ? "무주문 준비 확인 중…" : "무주문 준비 검사"}</button>
      <Blockers values={snapshot.prepared?.blockers} label="선택한 시험의 준비 차단 사유" />
    </section>

    <section className="panel short-live-start"><h3>3. 준비된 범위로 실제 시험 시작</h3>
      {snapshot.prepared ? <><p>준비 ID {snapshot.prepared.preparationId} · 범위 해시 {compactHash(snapshot.prepared.scopeHash)}</p><p>{snapshot.prepared.readyToStart ? "서버의 현재 준비 조건 충족" : "준비 조건 미충족"} · 확인한 범위가 바뀌면 다시 준비해야 합니다.</p></> : <p>아직 준비 결과가 없습니다. 먼저 시험 대상과 시간·금액을 확인해 주세요.</p>}
      <label className="short-live-ack"><input type="checkbox" checked={acknowledged} onChange={(event) => setAcknowledged(event.target.checked)} disabled={locked || !fresh || !snapshot.prepared?.readyToStart} /> 실제 자금으로 주문될 수 있으며 손실이 발생할 수 있음을 확인했습니다. 선택한 거래소·전략·한도로만 시험합니다.</label>
      <label className="short-live-confirm-label">확인 문구: {SHORT_LIVE_CONFIRMATION}<input type="text" value={confirmationText} aria-label="실제 주문 시험 시작 확인 문구" autoComplete="off" placeholder={SHORT_LIVE_CONFIRMATION} onChange={(event) => setConfirmationText(event.target.value)} disabled={locked || !fresh || !snapshot.prepared?.readyToStart} /></label>
      <button type="button" className="primary-button short-live-real-start" onClick={start} disabled={!canStart}><Play size={16} /> {busy === "start" ? "실제 시작 결과 확인 중…" : "선택한 범위로 실제 주문 시험 시작"}</button>
      <p className="short-live-meta">준비 결과·범위·원본·실제 계좌 조건은 서버가 다시 대조합니다. 이 확인은 성능 승급이나 전 계좌 실거래 승인이 아닙니다.</p>
    </section>

    <section className="panel" aria-label="짧은 실거래 시험 실행 관측"><div className="short-live-heading"><h3>실제 실행·손실·신호·주문 관측</h3><button type="button" className="secondary-button" onClick={stop} disabled={Boolean(busy) || unknownMutation === "stop" || !snapshot.runtime?.sessionId || snapshot.runtime?.terminalVerified === true}><Square size={16} /> 정상 중지 요청</button></div>
      {!snapshot.runtime ? <p>아직 실제 실행 기록이 없습니다.</p> : <><div className="short-live-run-status"><Clock3 size={18} /><strong>{snapshot.runtime.status}</strong><span>시작 {date(snapshot.runtime.startedAt)} · 예정 종료 {date(snapshot.runtime.endsAt)}</span></div>
        <p>소유 프로세스 {snapshot.runtime.ownerAlive === true ? "생존 확인" : snapshot.runtime.ownerAlive === false ? "현재 생존 아님" : "미확인"} · 신규 진입 {snapshot.runtime.newEntriesBlocked === true ? "차단" : snapshot.runtime.newEntriesBlocked === false ? "서버 범위 내 허용" : "미확인"} · 미확정 주문 {snapshot.runtime.unknownOrderCount ?? "미확인"}개 · 자연 종료 {snapshot.runtime.naturalExitObserved ? "관측됨" : "아직 미확인"}</p>
        <p>{snapshot.runtime.terminalVerified ? "소유 프로세스와 주문 대조를 포함한 정상 종료가 확인됐습니다." : "실제 정상 종료 확인 전입니다. 예정 시간이 지났다는 이유로 종료를 인정하지 않습니다."}</p>
        <Blockers values={snapshot.runtime.blockers} label="실행 중 확인할 사유" />
        <div className="short-live-table-wrap"><table><thead><tr><th>주문 경로</th><th>자본 한도</th><th>현재 노출</th><th>실현 / 평가손실 / 중지선</th><th>상태</th></tr></thead><tbody>{snapshot.runtime.brokers.map((row, index) => <tr key={`${row.broker}:${index}`}><td>{SHORT_LIVE_ROUTES.find((route) => route.id === row.broker)?.label || row.broker}</td><td>{money(row.budgetKrw)}</td><td>{money(row.exposureKrw)}</td><td>{money(row.dailyRealizedLossKrw)} / {money(row.unrealizedLossKrw)} / {money(row.lossLimitKrw)}</td><td>{row.status || "미확인"}</td></tr>)}</tbody></table></div>
        <div className="short-live-table-wrap"><table><thead><tr><th>전략</th><th>시간봉</th><th>최근 신호</th><th>평가 / 신호 / 주문 / 체결</th><th>관측 의미</th></tr></thead><tbody>{snapshot.runtime.strategies.map((row, index) => <tr key={`${row.selectionKey}:${index}`}><td>{row.label || row.selectionKey}</td><td>{row.timeframe}</td><td>{row.signal || "미관측"}</td><td>{row.decisionCount ?? "미확인"} / {row.signalCount ?? "미확인"} / {row.orderCount ?? "미확인"} / {row.fillCount ?? "미확인"}</td><td>{shortLiveRuntimeMeaning(row)}</td></tr>)}</tbody></table></div>
      </>}
    </section>
    {legacyWorkspace && <section className="panel"><button type="button" className="secondary-button" onClick={() => setShowLegacy((value) => !value)} aria-expanded={showLegacy}>{showLegacy ? "기존 개별 연결 시험 닫기" : "기존 개별 연결 시험 열기"}</button>{showLegacy && legacyWorkspace}</section>}
  </div>;
}
