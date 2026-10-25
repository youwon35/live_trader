import * as React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Clock3, Play, RefreshCw, ShieldAlert, Square } from "lucide-react";
import { getShortLiveTestSnapshot, getShortLiveTestCatalog, prepareShortLiveTest, startShortLiveTest, stopShortLiveTest, releaseShortLiveDryRun } from "./shortLiveTestApi.js";
import {
  SHORT_LIVE_CONFIRMATION, SHORT_LIVE_DEFAULTS, SHORT_LIVE_ROUTES, SHORT_LIVE_TIMEFRAMES,
  buildShortLivePrepare, normalizeShortLiveSnapshot, mergeShortLiveSnapshot, sanitizeShortLiveText, shortLiveBlockerLabel,
  shortLiveCanStart, shortLiveCoverage, shortLiveRunning, shortLiveRuntimeMeaning, shortLiveSelectionSignature, shortLivePreparationMatches,
  shortLiveFilterCandidates, shortLiveIsBoundedCandidate, shortLivePreparationWindow,
  normalizeShortLiveFuturesPolicy, shortLiveProtectionLabel, shortLiveFuturesIncidentLabel,
} from "./shortLiveTestModel.js";
import "./ShortLiveTestPanel.css";

const defaultApi = { snapshot: getShortLiveTestSnapshot, catalog: getShortLiveTestCatalog, prepare: prepareShortLiveTest, start: startShortLiveTest, stop: stopShortLiveTest, releaseDryRun: releaseShortLiveDryRun };
const money = (value) => value === null || value === undefined ? "미확인" : `${new Intl.NumberFormat("ko-KR").format(value)}원`;
const date = (value) => { const parsed = new Date(value); return value && !Number.isNaN(parsed.getTime()) ? parsed.toLocaleString("ko-KR", { timeZone: "Asia/Seoul" }) : "미기록"; };
const kindLabel = (kind) => kind === "portfolio" ? "포트폴리오" : "인스턴스";
const compactHash = (value) => value ? `${value.slice(0, 10)}…${value.slice(-6)}` : "원본 해시 없음";
const remainingTime = (seconds) => `${Math.floor(seconds / 60)}분 ${seconds % 60}초`;
const futuresPrice = (value) => value === null || value === undefined || value <= 0 ? "미확인" : `${new Intl.NumberFormat("ko-KR", { maximumFractionDigits: 8 }).format(value)} USDT`;

function Blockers({ values = [], label = "진행을 막는 사유" }) {
  const rows = [...new Set(values.map(shortLiveBlockerLabel))];
  return rows.length ? <div className="short-live-blockers" role="status" aria-label={label}><strong>{label}</strong><ul>{rows.map((row) => <li key={row}>{row}</li>)}</ul></div> : null;
}

export default function ShortLiveTestPanel({ api = defaultApi, initialSnapshot, pollMs = 5000, legacyWorkspace = null }) {
  const [snapshot, setSnapshot] = useState(() => normalizeShortLiveSnapshot(initialSnapshot));
  const [fresh, setFresh] = useState(false);
  const [selection, setSelection] = useState([]);
  const [durationHours, setDurationHours] = useState(6);
  const [futuresLeverage, setFuturesLeverage] = useState("1");
  const [futuresStopLossPercent, setFuturesStopLossPercent] = useState("");
  const [routeFilter, setRouteFilter] = useState("ALL");
  const [timeframeFilter, setTimeframeFilter] = useState("ALL");
  const [kindFilter, setKindFilter] = useState("ALL");
  const [profileFilter, setProfileFilter] = useState("ALL");
  const [now, setNow] = useState(Date.now);
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [pollError, setPollError] = useState("");
  const [catalogError, setCatalogError] = useState("");
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogReady, setCatalogReady] = useState(false);
  const [acknowledged, setAcknowledged] = useState(false);
  const [confirmationText, setConfirmationText] = useState("");
  const [preparedSelection, setPreparedSelection] = useState("");
  const [preparedAuthority, setPreparedAuthority] = useState("");
  const [unknownMutation, setUnknownMutation] = useState("");
  const [showLegacy, setShowLegacy] = useState(false);
  const [dryRunReleased, setDryRunReleased] = useState(false);
  const mounted = useRef(false);
  const pollFlight = useRef(null);
  const catalogFlight = useRef(null);
  const displayCatalog = useRef(snapshot.catalog);
  const snapshotVersion = useRef(0);
  const mutationPending = useRef(false);
  const futuresPolicy = { leverage: futuresLeverage, marginMode: "ISOLATED", stopLossPercent: futuresStopLossPercent };
  const selectionSignature = shortLiveSelectionSignature(snapshot.catalog, selection, durationHours, futuresPolicy);
  const active = shortLiveRunning(snapshot);
  const locked = Boolean(busy) || catalogLoading || active || unknownMutation;
  const coverage = useMemo(() => shortLiveCoverage(snapshot.catalog), [snapshot.catalog]);
  const visibleCandidates = useMemo(() => shortLiveFilterCandidates(snapshot.catalog, {
    route: routeFilter, timeframe: timeframeFilter, kind: kindFilter, profile: profileFilter,
  }), [snapshot.catalog, routeFilter, timeframeFilter, kindFilter, profileFilter]);
  const selectedCandidates = snapshot.catalog.filter((row) => selection.includes(row.selectionKey));
  const hasFutures = selectedCandidates.some((row) => row.routes.includes("BINANCE_USDM_FUTURES"));
  const futuresPolicyValid = !hasFutures || normalizeShortLiveFuturesPolicy(futuresPolicy) !== null;
  const hiddenSelectionCount = selection.filter((key) => !visibleCandidates.some((row) => row.selectionKey === key)).length;
  const preparationWindow = shortLivePreparationWindow(snapshot.prepared, now);
  const preparationValid = preparationWindow.status === "VALID";
  const canStart = !busy && !catalogLoading && catalogReady && !unknownMutation && shortLiveCanStart(snapshot, {
    fresh, acknowledged, confirmationText, selectionUnchanged: preparedSelection === selectionSignature, preparedAuthority,
    preparedScopeMatches: shortLivePreparationMatches(snapshot.prepared, snapshot.catalog, selection, durationHours, futuresPolicy),
    now,
  });

  const acceptStatus = useCallback((value) => {
    const next = mergeShortLiveSnapshot(value, displayCatalog.current);
    displayCatalog.current = next.catalog;
    setSnapshot(next);
    return next;
  }, []);

  const refreshCatalog = useCallback(async () => {
    if (catalogFlight.current) return catalogFlight.current;
    setCatalogLoading(true); setCatalogReady(false); setCatalogError("");
    const flight = (async () => {
      try {
        const value = await api.catalog();
        if (!mounted.current) return;
        const catalog = normalizeShortLiveSnapshot(value).catalog;
        displayCatalog.current = catalog;
        setSnapshot((current) => ({ ...current, catalog }));
        setCatalogReady(true);
      } catch (failure) {
        if (mounted.current) setCatalogError(shortLiveBlockerLabel(failure?.message));
      }
    })();
    catalogFlight.current = flight;
    try { await flight; }
    finally {
      if (catalogFlight.current === flight) catalogFlight.current = null;
      if (mounted.current) setCatalogLoading(false);
    }
  }, [api]);

  const refresh = useCallback(async ({ forceFresh = false } = {}) => {
    if (pollFlight.current) {
      if (!forceFresh) return pollFlight.current;
      await pollFlight.current;
    }
    if (!forceFresh && mutationPending.current) return;
    const version = snapshotVersion.current;
    const flight = (async () => {
      try {
        const value = await api.snapshot();
        if (!mounted.current || version !== snapshotVersion.current) return;
        const next = acceptStatus(value);
        setFresh(true); setPollError("");
        return next;
      } catch (failure) {
        if (mounted.current && version === snapshotVersion.current) { setFresh(false); setPollError(shortLiveBlockerLabel(failure?.message)); }
      }
    })();
    pollFlight.current = flight;
    try { return await flight; }
    finally { if (pollFlight.current === flight) pollFlight.current = null; }
  }, [api, acceptStatus]);

  useEffect(() => {
    mounted.current = true; refresh(); refreshCatalog();
    const timer = setInterval(refresh, Math.max(1000, pollMs));
    return () => { mounted.current = false; clearInterval(timer); };
  }, [refresh, refreshCatalog, pollMs]);

  useEffect(() => { setAcknowledged(false); setConfirmationText(""); }, [selectionSignature, snapshot.prepared?.scopeHash]);
  useEffect(() => {
    if (!snapshot.prepared || active) return;
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [snapshot.prepared?.expiresAt, active]);
  useEffect(() => {
    if (!preparationValid) { setAcknowledged(false); setConfirmationText(""); }
  }, [preparationValid]);

  function changeSelection(key) {
    if (locked) return;
    setSelection((rows) => rows.includes(key) ? rows.filter((item) => item !== key) : [...rows, key]);
    setPreparedSelection(""); setMessage("");
  }

  async function prepare() {
    if (locked || !fresh || !catalogReady || mutationPending.current) return;
    let body;
    try { body = buildShortLivePrepare(snapshot.catalog, selection, { ...SHORT_LIVE_DEFAULTS, durationHours, futuresPolicy }); }
    catch (failure) { setError(failure.message); return; }
    mutationPending.current = true; snapshotVersion.current += 1;
    setBusy("prepare"); setError(""); setMessage(""); setFresh(false); setDryRunReleased(false);
    setPreparedSelection(""); setPreparedAuthority("");
    try {
      const result = await api.prepare(body);
      const returned = acceptStatus(result);
      const observed = await refresh({ forceFresh: true });
      if (returned.prepared && observed?.prepared
        && returned.prepared.preparationId === observed.prepared.preparationId
        && returned.prepared.scopeHash === observed.prepared.scopeHash
        && shortLivePreparationMatches(observed.prepared, observed.catalog, selection, durationHours, futuresPolicy)) {
        setPreparedSelection(selectionSignature);
        setPreparedAuthority(`${observed.prepared.preparationId}:${observed.prepared.scopeHash}`);
        setMessage("무주문 준비 결과를 확인했습니다. 차단 사유와 실제 시험 범위를 확인한 뒤 직접 시작해 주세요.");
      } else {
        setError("준비 응답과 현재 선택 범위를 정확히 연결하지 못했습니다. 상태를 확인하고 무주문 준비 검사를 다시 실행해 주세요.");
      }
    } catch (failure) {
      setPreparedSelection(""); setPreparedAuthority(""); setFresh(false);
      setError(shortLiveBlockerLabel(failure?.message));
      await refresh({ forceFresh: true });
    } finally { mutationPending.current = false; setBusy(""); }
  }

  async function releaseDryRun() {
    if (locked || !fresh || mutationPending.current || typeof api.releaseDryRun !== "function") return;
    mutationPending.current = true; snapshotVersion.current += 1;
    setBusy("release-dry-run"); setError(""); setMessage(""); setFresh(false);
    setPreparedSelection(""); setPreparedAuthority("");
    setAcknowledged(false); setConfirmationText("");
    try {
      await api.releaseDryRun();
      setDryRunReleased(true);
      setMessage("모의 실행 해제를 확인했습니다. 선택한 전략은 유지됩니다. 무주문 준비 검사를 다시 실행한 뒤 마지막 시작 화면에서 확인해 주세요.");
    } catch (failure) {
      setError(shortLiveBlockerLabel(failure?.message));
    } finally {
      await refresh({ forceFresh: true });
      mutationPending.current = false; setBusy("");
    }
  }

  async function start() {
    if (!canStart || mutationPending.current || shortLivePreparationWindow(snapshot.prepared).status !== "VALID") return;
    const prepared = snapshot.prepared;
    mutationPending.current = true; snapshotVersion.current += 1;
    setBusy("start"); setError(""); setMessage(""); setFresh(false);
    try {
      const result = await api.start({ preparationId: prepared.preparationId, scopeHash: prepared.scopeHash,
        confirmed: true, confirmationText: SHORT_LIVE_CONFIRMATION });
      acceptStatus(result);
      setAcknowledged(false); setConfirmationText("");
      setMessage("시작 요청을 보냈습니다. 실제 실행·주문·체결 상태는 아래 관측 결과로 확인합니다.");
      await refresh({ forceFresh: true });
    } catch (failure) {
      setFresh(false); setPreparedSelection(""); setPreparedAuthority("");
      setAcknowledged(false); setConfirmationText("");
      if (failure?.knownPreMutationRejection === true) {
        setError(`시작 요청이 실행 전에 거절됐습니다. ${shortLiveBlockerLabel(failure?.message)}`);
      } else {
        setUnknownMutation("start");
        setError(`시작 결과 미확정 · 자동 재시도하지 않습니다. ${shortLiveBlockerLabel(failure?.message)}`);
      }
      await refresh({ forceFresh: true });
    } finally { mutationPending.current = false; setBusy(""); }
  }

  async function stop() {
    if (busy || mutationPending.current || unknownMutation === "stop" || !snapshot.runtime?.sessionId) return;
    mutationPending.current = true; snapshotVersion.current += 1;
    setBusy("stop"); setError(""); setMessage(""); setFresh(false);
    try {
      const result = await api.stop({ sessionId: snapshot.runtime.sessionId, reason: "사용자가 짧은 실거래 시험의 정상 중지를 요청함" });
      acceptStatus(result);
      setMessage("정상 중지를 요청했습니다. 신규 진입 차단·미체결 대조·실제 프로세스 종료가 확인될 때까지 상태를 확인합니다.");
      await refresh({ forceFresh: true });
    } catch (failure) {
      setFresh(false);
      if (failure?.knownPreMutationRejection === true) {
        setError(`중지 요청의 대상이 일치하지 않아 실행 전에 거절됐습니다. ${shortLiveBlockerLabel(failure?.message)}`);
      } else {
        setUnknownMutation("stop");
        setError(`중지 결과 미확정 · 반복 호출하지 않습니다. ${shortLiveBlockerLabel(failure?.message)}`);
      }
      await refresh({ forceFresh: true });
    } finally { mutationPending.current = false; setBusy(""); }
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
      {pollError && <p className="short-live-error" role="alert">상태 조회: {pollError}</p>}
      {message && <p className="short-live-message" role="status">{message}</p>}
      {unknownMutation && <p role="alert">요청 결과가 미확정입니다. 새 시작을 막고 기존 실행 상태를 읽기로 확인합니다. 필요하면 실행 중 시험의 정상 중지를 요청하세요.</p>}
      <Blockers values={snapshot.blockers} />
    </section>

    <section className="panel"><h3>1. 거래소·시간봉·시험 대상 선택</h3>
      <button type="button" className="secondary-button" onClick={refreshCatalog} disabled={locked}><RefreshCw size={16} /> {catalogLoading ? "전략 원본 목록 확인 중…" : "전략 목록 새로 확인"}</button>
      <p className="short-live-meta">전략 목록은 선택용입니다. 무주문 준비와 실제 시작에서는 원본과 현재 계좌 조건을 다시 확인합니다.</p>
      {catalogError && <p className="short-live-error" role="alert">전략 목록 조회: {catalogError}</p>}
      <div className="short-live-quick-filter">
        <button type="button" className="secondary-button" aria-pressed={timeframeFilter === "1h" && profileFilter === "BOUNDED" && routeFilter === "ALL" && kindFilter === "ALL"} onClick={() => { setTimeframeFilter("1h"); setProfileFilter("BOUNDED"); setRouteFilter("ALL"); setKindFilter("ALL"); }}><Clock3 size={16} /> 1시간봉 소액 후보 보기</button>
        <span className="short-live-meta">목록만 좁힙니다. 전략 선택이나 준비·시작은 자동으로 하지 않습니다.</span>
      </div>
      <div className="short-live-filters">
        <label>주문 경로<select aria-label="짧은 시험 주문 경로" value={routeFilter} onChange={(event) => setRouteFilter(event.target.value)}><option value="ALL">모든 경로</option>{SHORT_LIVE_ROUTES.map((row) => <option key={row.id} value={row.id}>{row.label}</option>)}</select></label>
        <label>시간봉<select aria-label="짧은 시험 시간봉" value={timeframeFilter} onChange={(event) => setTimeframeFilter(event.target.value)}><option value="ALL">모든 시간봉</option>{SHORT_LIVE_TIMEFRAMES.map((row) => <option key={row} value={row}>{row}</option>)}</select></label>
        <label>유형<select aria-label="짧은 시험 전략 유형" value={kindFilter} onChange={(event) => setKindFilter(event.target.value)}><option value="ALL">모든 유형</option><option value="instance">인스턴스</option><option value="portfolio">포트폴리오</option></select></label>
        <label>후보 구분<select aria-label="짧은 시험 후보 구분" value={profileFilter} onChange={(event) => setProfileFilter(event.target.value)}><option value="ALL">모든 후보</option><option value="BOUNDED">소액 배정금 100% 기능시험</option><option value="ORIGINAL">기존 원본·그 외 비중</option></select></label>
      </div>
      {(timeframeFilter === "1h" || selectedCandidates.some((row) => row.timeframes.includes("1h"))) && <div className="short-live-hourly-guide"><strong>1시간봉은 완성된 봉으로 평가합니다</strong><p>시작 직후 주문이 나오지 않아도 정상일 수 있습니다. 공급자·시작 시점에 따라 과거 봉 연결과 첫 완성봉 수신을 기다린 뒤, 원래 전략 조건이 충족될 때 주문합니다.</p><p>Upbit은 시작 시 이미 진행 중인 일부 봉을 이력 보강에 사용합니다. 연결 뒤 온전히 관측한 봉부터 평가하므로, 첫 평가까지 시작 시점에 따라 약 1~2시간 걸릴 수 있습니다.</p><p className="short-live-meta">5~7시간 시험은 여러 번의 봉 평가를 관찰하기 위한 시간입니다. 주문·체결 횟수를 보장하거나, 신호가 없다는 이유로 통과 처리하지 않습니다.</p></div>}
      <p>여러 대상을 선택하면 복수 전략 시험입니다. 포트폴리오의 종목 수와 서로 다른 전략 플러그인 수는 구분합니다.</p>
      <p>신규 기능시험 후보는 배정된 소액 시험 자금의 100%를 사용합니다. 원래 5% 후보는 5% 그대로 적용합니다. 복수 전략은 배정 자금을 나누며 거래소별 총 10만원 한도는 유지합니다.</p>
      <p className="short-live-catalog-count">{catalogReady ? `현재 목록 ${visibleCandidates.length}개 / 전체 ${snapshot.catalog.length}개` : "전략 목록 미확인"} · 선택 {selection.length}개{hiddenSelectionCount > 0 ? ` · 필터 밖 선택 ${hiddenSelectionCount}개` : ""}</p>
      <div className="short-live-catalog" aria-label="짧은 시험 후보 목록">
        {!catalogReady && <p>{catalogLoading ? "전략 원본 목록을 읽고 있습니다. 실행 상태는 별도로 계속 확인합니다." : "전략 목록을 확인하지 못했습니다. 목록을 다시 확인해야 준비할 수 있습니다."}</p>}
        {catalogReady && visibleCandidates.length === 0 && <p>이 조건에 정확히 연결된 후보가 없습니다. 다른 시간봉을 통과한 결과로 대체하지 않습니다.</p>}
        {visibleCandidates.map((row) => <label key={row.selectionKey} className={`short-live-candidate ${selection.includes(row.selectionKey) ? "is-selected" : ""}`}>
          <input type="checkbox" checked={selection.includes(row.selectionKey)} disabled={locked} onChange={() => changeSelection(row.selectionKey)} aria-label={`시험 대상 ${row.label}`} />
          <span><strong>{row.label}</strong><span className="short-live-meta">{SHORT_LIVE_ROUTES.find((route) => route.id === row.route)?.label || row.route} · {row.timeframe} · {kindLabel(row.kind)} · 전략 {row.strategyCount}개{row.distinctPluginCount !== null ? ` · 플러그인 ${row.distinctPluginCount}종` : ""}</span>
            {shortLiveIsBoundedCandidate(row) && <span className="short-live-profile-badge">소액 배정금 100% 후보</span>}
            <span className="short-live-meta">{row.symbols.join(" · ") || "종목 미확인"} · {compactHash(row.kind === "portfolio" ? row.portfolioFileSha256 : row.artifactFileSha256)}</span>
            <span className="short-live-meta">진입 비중: {row.positionSizePercents.length ? `배정 자금 ${row.positionSizePercents.map((percent) => `${percent}%`).join(" / ")}` : "원본 비중 미확인"}{row.sourcePositionSizeWasDefaulted ? " · 원래 모델의 기본 비중" : ""}</span>
            <span>{row.evidenceMeaning}</span><span className="short-live-candidate-state">{row.supported && row.available ? "준비 검사 대상" : "현재 시작 차단 · 준비 검사에서 사유 확인"}</span>
            {row.blockers.length > 0 && <span className="short-live-meta">{row.blockers.map(shortLiveBlockerLabel).join(" · ")}</span>}
          </span>
        </label>)}
      </div>
      <div className="short-live-selection" aria-label="선택한 시험 범위">
        <div className="short-live-heading"><strong>선택한 시험 범위 · {selection.length}개</strong>{selection.length > 0 && <button type="button" className="secondary-button" disabled={locked} onClick={() => { setSelection([]); setPreparedSelection(""); setMessage(""); }}>선택 모두 해제</button>}</div>
        {selection.length === 0 ? <p className="short-live-meta">위 목록에서 시험할 대상을 선택해 주세요.</p> : <ul>{selection.map((key) => {
          const matches = snapshot.catalog.filter((row) => row.selectionKey === key);
          const row = matches.length === 1 ? matches[0] : null;
          return <li key={key}><span><strong>{row?.label || "선택 원본 미확인"}</strong><span className="short-live-meta">{row ? `${row.timeframes.join(" · ")} · ${kindLabel(row.kind)} · ${row.symbols.join(" · ")}` : "원본을 정확히 하나로 확인하지 못했습니다. 해제 후 다시 선택해 주세요."}</span></span><button type="button" className="secondary-button" disabled={locked} onClick={() => changeSelection(key)} aria-label={`선택 해제 ${row?.label || "미확인 원본"}`}>해제</button></li>;
        })}</ul>}
        {hiddenSelectionCount > 0 && <p className="short-live-meta">필터 밖에 있는 선택도 준비 검사에 포함됩니다. 목록을 바꿔도 선택은 유지됩니다.</p>}
      </div>
      {catalogReady && <details className="short-live-details"><summary>전체 24조합의 후보·연결 현황</summary><div className="short-live-table-wrap"><table><thead><tr><th>경로</th><th>시간봉</th><th>유형</th><th>후보 / 준비 가능</th><th>현재 차단 사유</th></tr></thead><tbody>{coverage.map((row) => <tr key={`${row.route}:${row.timeframe}:${row.kind}`}><td>{row.routeLabel}</td><td>{row.timeframe}</td><td>{kindLabel(row.kind)}</td><td>{row.count} / {row.availableCount}</td><td>{row.blockers.length ? [...new Set(row.blockers.map(shortLiveBlockerLabel))].join(" · ") : "선택 후 현재 잔고·주문 조건 재검사"}</td></tr>)}</tbody></table></div></details>}
    </section>

    <section className="panel"><h3>2. 금액·시간 확인 후 무주문 준비</h3>
      <div className="short-live-limits"><label>시험 시간<select value={durationHours} disabled={locked} aria-label="실거래 시험 시간" onChange={(event) => { setDurationHours(Number(event.target.value)); setPreparedSelection(""); }}>{[5, 6, 7].map((hours) => <option key={hours} value={hours}>{hours}시간</option>)}</select></label><div className="short-live-limit-summary"><div><span>거래소별 자본 상한</span><strong>100,000원</strong></div><div><span>이번 시험 손실 중단 기준</span><strong>50,000원</strong></div><p>Binance 현물·선물은 같은 거래소 한도 안에서 합산합니다. 실제 잔고·최소 주문 조건이 우선하며, 여러 대상을 선택하면 배정 자금이 나뉩니다.</p></div></div>
      <p className="short-live-meta">손실은 시험 범위 손익 또는 계좌 평가 감소를 주기적으로 확인합니다. 수수료·펀딩비와 가격 급변·응답 지연 때문에 실제 손실이 중단 기준을 넘을 수 있습니다.</p>
      {hasFutures && <fieldset className="short-live-futures-policy" disabled={locked}>
        <legend>선물 레버리지·거래소 보호 손절</legend>
        <div className="short-live-futures-fields">
          <label>Binance에서 설정한 레버리지<input type="number" min="1" max="125" step="1" value={futuresLeverage} aria-label="선물 기대 레버리지" onChange={(event) => { setFuturesLeverage(event.target.value); setPreparedSelection(""); }} /><small>선택한 모든 선물 종목의 실제 설정과 같아야 합니다.</small></label>
          <label>보호 손절 가격 거리 (%)<input type="number" min="0" step="any" value={futuresStopLossPercent} placeholder="직접 입력" aria-label="선물 보호 손절 가격 거리 퍼센트" onChange={(event) => { setFuturesStopLossPercent(event.target.value); setPreparedSelection(""); }} /><small>진입 주문 기준 가격에서 불리한 방향으로 움직이는 비율입니다.</small></label>
        </div>
        <p>격리(ISOLATED) 방식만 지원합니다. 이 화면은 거래소의 레버리지·증거금 설정을 변경하지 않습니다. 실제 설정이 준비 결과와 달라지면 신규 진입을 막습니다.</p>
        <p>자본 한도 안에서 포지션 투입 증거금을 배정하고, 명목 포지션 규모는 레버리지를 반영해 계산합니다. 비용 예비금은 주문 가능액 계산 시 별도로 차감합니다. 예를 들어 5배여도 실제 잔고가 5배로 늘어나지는 않습니다.</p>
        <p className="short-live-meta">이 선물 설정과 보호는 이 화면의 5~7시간 기능시험에 적용됩니다.</p>
        <p className="short-live-meta">보호 손절은 Mark Price 기준의 거래소 STOP_MARKET 주문입니다. 보호 주문을 확인한 뒤 진입하며, 정상 종료 중에도 포지션이 남아 있으면 보호를 유지합니다. 손절 가격과 실제 체결 가격은 다를 수 있습니다.</p>
        {!futuresPolicyValid && <p className="short-live-error" role="status">레버리지와 보호 손절 가격 거리를 입력해 주세요. 손절 거리는 0%보다 크고 100÷레버리지%보다 작아야 하며, 실제 청산가·증거금 조건은 서버가 추가 검사합니다.</p>}
      </fieldset>}
      <p>선택 {selection.length}개 · {durationHours}시간 · 자동 재시도 없음</p>
      <button type="button" className="primary-button" onClick={prepare} disabled={locked || !fresh || !catalogReady || selection.length === 0 || !futuresPolicyValid}>{busy === "prepare" ? "무주문 준비 확인 중…" : "무주문 준비 검사"}</button>
      <Blockers values={snapshot.prepared?.blockers} label="선택한 시험의 준비 차단 사유" />
      <div className="short-live-control-help" aria-label="시작 설정 안내">
        <strong>시작 설정은 이렇게 확인하세요</strong>
        <p>운영자 확인은 아래 3번의 체크박스·확인 문구로 한 번만 받습니다. 이 시험을 위해 다른 메뉴에서 운용자 확인을 켤 필요는 없습니다.</p>
        {snapshot.prepared?.blockers?.includes("DRY_RUN_ACTIVE") && <div className="short-live-control-action">
          <p><strong>모의 실행 켜짐</strong> · 앱 시작 때 켜지는 주문 전송 방지 설정입니다. 아래 버튼의 정상 확인 절차로 해제한 뒤 새 준비 검사를 해 주세요.</p>
          <button type="button" className="secondary-button" onClick={releaseDryRun} disabled={locked || !fresh || dryRunReleased || typeof api.releaseDryRun !== "function"}>{busy === "release-dry-run" ? "설정 확인 중…" : dryRunReleased ? "해제 확인됨 · 다시 준비해 주세요" : "모의 실행 해제"}</button>
          <small>앱 전체의 모의 실행 설정을 바꿉니다. 이 버튼으로 시험이 시작되지는 않습니다.</small>
        </div>}
        {(snapshot.prepared?.blockers || []).some((code) => code === "NEW_ENTRIES_BLOCKED" || code === "MANUAL_NEW_ENTRIES_BLOCKED") && <div className="short-live-control-action">
          <p><strong>신규 진입 차단</strong> · 새 매수나 포지션을 늘리는 주문을 멈춥니다. 앱 시작 대기·수동 차단·계좌와 원장 불일치·조회 오류·위험 감시 이상에서 켜질 수 있습니다.</p>
          <p><strong>실거래 운용 → 한도·안전장치 → 운영 차단 설정 → 신규 진입 차단</strong>에서 상태를 확인하고 정상 확인 절차로 해제합니다.</p>
          <small>해제 후 다시 켜지면 계좌·포지션 대조와 감시 오류를 먼저 해결해야 합니다. 보호 손절과 위험 감소 주문은 별도 안전 조건에 따라 처리하며, 이 스위치는 이미 접수된 주문을 취소하지 않습니다.</small>
        </div>}
      </div>
      {snapshot.prepared?.brokerReadiness?.length > 0 && <div className="short-live-readiness"><h4>거래소별 무주문 계좌 검사</h4>
        <p className="short-live-meta">준비 검사 시 관측한 결과입니다. 계좌 검사와 최종 시작 조건은 따로 표시하며, 시작할 때 다시 검사합니다.</p>
        {snapshot.prepared.brokerReadiness.map((row) => <div key={row.route} className="short-live-notice"><div>
          <strong>{SHORT_LIVE_ROUTES.find((route) => route.id === row.route)?.label || row.route} · {row.ready ? "계좌 검사 통과" : "계좌 검사 확인 필요"}</strong>
          {row.fundingBasis === "AVAILABLE_QUOTE_ONLY" && <p>주문 가능 잔고 {row.availableQuoteNative === null ? "미확인" : new Intl.NumberFormat("ko-KR", { maximumFractionDigits: 8 }).format(row.availableQuoteNative)} {row.quoteCurrency} · 이번 경로 배정 가능 {money(row.effectiveRouteBudgetKrw)}</p>}
          {row.preservedAssetSymbols.length > 0 && <p>기존 보유 보호: {row.preservedAssetSymbols.join(", ")} · 시험 주문 자금으로 포함하지 않습니다.</p>}
          {row.observedFuturesSettings && <p>현재 선물 설정: {row.observedFuturesSettings.marginMode || "미확인"} · {row.observedFuturesSettings.leverage ?? "미확인"}배 · {row.observedFuturesSettings.positionMode || "포지션 모드 미확인"}</p>}
          {Object.entries(row.observedFuturesSettingsBySymbol).map(([symbol, setting]) => <p key={symbol}>{symbol}: {setting.marginMode || "미확인"} · {setting.leverage ?? "미확인"}배 · {setting.positionMode || "포지션 모드 미확인"}</p>)}
          <Blockers values={row.blockers} label="계좌 검사 차단 사유" />
        </div></div>)}
      </div>}
    </section>

    <section className="panel short-live-start"><h3>3. 준비된 범위로 실제 시험 시작</h3>
      {snapshot.prepared ? <><p className="short-live-meta">준비 ID {snapshot.prepared.preparationId} · 범위 해시 {compactHash(snapshot.prepared.scopeHash)}</p><p>{snapshot.prepared.readyToStart && preparationValid ? "서버의 현재 준비 조건 충족" : "준비 조건 미충족"} · 확인한 범위가 바뀌면 다시 준비해야 합니다.</p>
        {snapshot.prepared.futuresPolicy && <p>확정 선물 범위: {snapshot.prepared.futuresPolicy.leverage}배 · 격리 · 보호 손절 가격 거리 {snapshot.prepared.futuresPolicy.stopLossPercent}%</p>}
        {!active && <div className={`short-live-expiry ${preparationValid ? "" : "is-expired"}`}><Clock3 size={17} /><span>{preparationValid ? `준비 유효시간 ${remainingTime(preparationWindow.remainingSeconds)} 남음` : preparationWindow.status === "EXPIRED" ? "준비 유효시간 만료 · 무주문 준비 검사를 다시 실행해 주세요." : "준비 유효시간 미확인 · 무주문 준비 검사가 필요합니다."}<small>유효 기한 {date(snapshot.prepared.expiresAt)} · 시작 시 서버에서 다시 확인합니다.</small></span></div>}
        {preparedSelection !== selectionSignature && <p className="short-live-meta">현재 선택·시간·선물 보호 정책으로 무주문 준비 검사를 다시 실행해야 시작할 수 있습니다.</p>}
      </> : <p>아직 준비 결과가 없습니다. 먼저 시험 대상과 시간·금액을 확인해 주세요.</p>}
      <label className="short-live-ack"><input type="checkbox" checked={acknowledged} onChange={(event) => setAcknowledged(event.target.checked)} disabled={locked || !fresh || !preparationValid || !snapshot.prepared?.readyToStart} /> 실제 자금으로 주문될 수 있으며 손실이 발생할 수 있음을 확인했습니다. 선택한 거래소·전략·한도로만 시험합니다.</label>
      <label className="short-live-confirm-label">확인 문구: {SHORT_LIVE_CONFIRMATION}<input type="text" value={confirmationText} aria-label="실제 주문 시험 시작 확인 문구" autoComplete="off" placeholder={SHORT_LIVE_CONFIRMATION} onChange={(event) => setConfirmationText(event.target.value)} disabled={locked || !fresh || !preparationValid || !snapshot.prepared?.readyToStart} /></label>
      <button type="button" className="primary-button short-live-real-start" onClick={start} disabled={!canStart}><Play size={16} /> {busy === "start" ? "실제 시작 결과 확인 중…" : "선택한 범위로 실제 주문 시험 시작"}</button>
      <p className="short-live-meta">준비 결과·범위·원본·실제 계좌 조건은 서버가 다시 대조합니다. 이 확인은 성능 승급이나 전 계좌 실거래 승인이 아닙니다.</p>
    </section>

    <section className="panel" aria-label="짧은 실거래 시험 실행 관측"><div className="short-live-heading"><h3>실제 실행·손실·신호·주문 관측</h3><button type="button" className="secondary-button" onClick={stop} disabled={Boolean(busy) || unknownMutation === "stop" || !snapshot.runtime?.sessionId || snapshot.runtime?.terminalVerified === true}><Square size={16} /> 정상 중지 요청</button></div>
      {!snapshot.runtime ? <p>아직 실제 실행 기록이 없습니다.</p> : <><div className="short-live-run-status"><Clock3 size={18} /><strong>{snapshot.runtime.status}</strong><span>시작 {date(snapshot.runtime.startedAt)} · 예정 종료 {date(snapshot.runtime.endsAt)}</span></div>
        <p>소유 프로세스 {snapshot.runtime.ownerAlive === true ? "생존 확인" : snapshot.runtime.ownerAlive === false ? "현재 생존 아님" : "미확인"} · 신규 진입 {snapshot.runtime.newEntriesBlocked === true ? "차단" : snapshot.runtime.newEntriesBlocked === false ? "서버 범위 내 허용" : "미확인"} · 미확정 주문 {snapshot.runtime.unknownOrderCount ?? "미확인"}개 · 자연 종료 {snapshot.runtime.naturalExitObserved ? "관측됨" : "아직 미확인"}</p>
        <p>{snapshot.runtime.terminalVerified && snapshot.runtime.workerRetirementRequired === false && snapshot.runtime.workerLaunchState === "NOT_ATTEMPTED" && snapshot.runtime.workerAlive === false ? "작업자 시작 전 종료 확인 · 시세 연결 정리와 잔여 주문·포지션 대조가 확인됐습니다. 실행·거래 성공을 뜻하지 않습니다." : snapshot.runtime.terminalVerified && snapshot.runtime.naturalExitObserved ? "소유 프로세스와 주문 대조를 포함한 정상 종료가 확인됐습니다." : "실제 정상 종료 확인 전입니다. 예정 시간이 지났다는 이유로 종료를 인정하지 않습니다."}</p>
        <Blockers values={snapshot.runtime.blockers} label="실행 중 확인할 사유" />
        <div className="short-live-table-wrap"><table><thead><tr><th>주문 경로</th><th>실제 배정 한도</th><th>포지션 금액</th><th>기준 손실 / 평가손실 / 중단선</th><th>상태</th></tr></thead><tbody>{snapshot.runtime.brokers.map((row, index) => <tr key={`${row.broker}:${index}`}><td>{SHORT_LIVE_ROUTES.find((route) => route.id === row.broker)?.label || row.broker}</td><td>{money(row.budgetKrw)}<small className="short-live-meta">거래소 합산 상한 {money(row.brokerBudgetKrw)}</small></td><td>{row.broker === "BINANCE_USDM_FUTURES" ? <>{money(row.committedCapitalKrw)} / {money(row.notionalExposureKrw)}<small className="short-live-meta">포지션 투입 증거금 / 명목 노출</small><small className="short-live-meta">비용 예비금은 주문 가능액 계산 시 별도 차감합니다.</small></> : <>{money(row.exposureKrw)}<small className="short-live-meta">현물 보유 금액</small></>}</td><td>{money(row.dailyRealizedLossKrw)} / {money(row.unrealizedLossKrw)} / {money(row.lossLimitKrw)}<small className="short-live-meta">{row.lossBasis === "ACCOUNT_NATIVE_EQUITY_DECLINE_AT_FRESH_FX" ? "시험 시작 대비 계좌 평가 감소 기준" : row.lossBasis === "EXACT_SCOPE_PNL" ? "시험 범위의 실제 손익 기준" : row.lossBasis === "SESSION_NATIVE_EQUITY_EXCLUDING_PRESERVED_SPOT_WITH_VERIFIED_FEES" ? "기존 보유 자산을 제외한 시험 평가 감소 기준" : "손실 산정 근거 미확인"}</small></td><td>{row.status || "미확인"}<small className="short-live-meta">수수료 {row.actualFeeKnown === true ? "확인됨" : "미확정"}</small></td></tr>)}</tbody></table></div>
        {snapshot.runtime.brokers.filter((row) => row.broker === "BINANCE_USDM_FUTURES").map((row) => <div key={`${row.broker}:protection`} className="short-live-futures-observation">
          <h4>선물 보호 주문·청산 관측</h4>
          {row.protections.length === 0 ? <p className="short-live-meta">아직 거래소 보호 주문 관측 기록이 없습니다. 보호가 확인됐다는 뜻이 아닙니다.</p> : <div className="short-live-table-wrap"><table><thead><tr><th>종목·방향</th><th>보호 상태</th><th>손절 발동 가격</th><th>관측 시각</th></tr></thead><tbody>{row.protections.map((protection, index) => <tr key={`${protection.symbol}:${protection.positionSide}:${index}`}><td>{protection.symbol} · {protection.positionSide}</td><td>{shortLiveProtectionLabel(protection.status)}<small className="short-live-meta">{protection.verified ? "거래소 대조 확인" : "현재 보호 확인 필요"}</small></td><td>{futuresPrice(protection.triggerPrice)}</td><td>{date(protection.observedAt)}</td></tr>)}</tbody></table></div>}
          {row.futuresPositions.map((position, index) => <p key={`${position.symbol}:${position.positionSide}:${index}`}>{position.symbol} · {position.positionSide} · Mark Price {futuresPrice(position.markPrice)} · 거래소 청산가 {futuresPrice(position.actualLiquidationPrice)} · {position.leverage ?? "미확인"}배</p>)}
          {row.futuresIncidents.map((incident, index) => <div key={`${incident.symbol}:${incident.type}:${index}`} className="short-live-blockers" role="status"><strong>{shortLiveFuturesIncidentLabel(incident.type)} · {incident.symbol}</strong><p>{incident.positionSide} · 수량 {incident.quantity ?? "미확인"} · {date(incident.observedAt)}</p><p>{incident.verified ? "거래소 사건 확인 · 신규 진입 중단 및 잔여 주문·포지션 대조" : "사건 근거 미확정 · 자동 재시작하지 않음"}</p></div>)}
        </div>)}
        <div className="short-live-table-wrap"><table><thead><tr><th>전략</th><th>시간봉</th><th>최근 신호</th><th>봉 평가 / 주문 시도 / 체결 관측</th><th>관측 의미</th></tr></thead><tbody>{snapshot.runtime.strategies.map((row, index) => <tr key={`${row.selectionKey}:${index}`}><td>{row.pluginId && row.symbol ? `${row.pluginId} · ${row.symbol}` : row.label || row.selectionKey}<small className="short-live-meta">{SHORT_LIVE_ROUTES.find((route) => route.id === row.route)?.label || row.route}</small></td><td>{row.timeframe}</td><td>{row.signal === "NOT_OBSERVED" ? "미관측" : row.signal || "미관측"}</td><td>{row.decisionCount ?? "미확인"} / {row.orderAttemptCount ?? "미확인"} / {row.fillObservationCount ?? "미확인"}</td><td>{shortLiveRuntimeMeaning(row)}{row.internalAllocationTransferCount > 0 && <small className="short-live-meta">전략 간 내부 배분 {row.internalAllocationTransferCount}회 · 거래소 체결과 구분</small>}</td></tr>)}</tbody></table></div>
      </>}
    </section>
    {legacyWorkspace && <section className="panel"><button type="button" className="secondary-button" onClick={() => setShowLegacy((value) => !value)} aria-expanded={showLegacy}>{showLegacy ? "기존 개별 연결 시험 닫기" : "기존 개별 연결 시험 열기"}</button>{showLegacy && legacyWorkspace}</section>}
  </div>;
}
