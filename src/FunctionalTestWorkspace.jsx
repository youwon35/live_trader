import * as React from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Clock3,
  FileKey2,
  Play,
  RefreshCw,
  ShieldCheck,
  Square,
} from "lucide-react";

import {
  activateFunctionalTestToday,
  createFunctionalTestPermit,
  getFunctionalTestWorkspace,
  pauseFunctionalTestToday,
  startFunctionalTest,
  stopFunctionalTest,
} from "./api";
import {
  EMPTY_FUNCTIONAL_TEST_WORKSPACE,
  formatFunctionalTestRemaining,
  functionalTestDurationBounds,
  functionalTestProgress,
  functionalTestStatusTone,
  normalizeFunctionalTestWorkspace,
  preferredFunctionalTestCandidate,
} from "./functionalTestModel";
const CryptoFirstLivePanel = React.lazy(() => import("./CryptoFirstLivePanel"));


const BLOCKER_LABELS = {
  "functional-test-api-unavailable": "기능시험 API 상태를 확인할 수 없음",
  "artifact-integrity-invalid": "아티팩트 무결성 검증 실패",
  "artifact-sha256-required": "아티팩트 SHA-256 없음",
  "portfolio-non-domestic-sleeve-present": "국내주식/ETF 이외의 sleeve가 포함됨",
  "kis-account-binding-required": "KIS 실계좌 바인딩 없음",
  "kis-live-credentials-not-ready": "KIS 실전 키·계좌 설정 미완료",
  "live-order-adapter-global-lock-enabled": "실주문 어댑터 전역 잠금 유지 중",
  "functional-test-current-artifact-not-found": "선택한 현재 아티팩트를 다시 찾을 수 없음",
  "functional-test-current-binding-changed": "아티팩트·인스턴스·계좌·종목 바인딩 변경 감지",
  "functional-test-permit-required": "먼저 기능시험 허가서를 준비해야 함",
  "functional-test-live-activation-required": "오늘의 운영자 활성화가 필요함",
  "functional-test-permit-expired": "전체 기능시험 기간 종료",
  "functional-test-live-activation-expired": "오늘의 활성화 만료",
  "functional-test-stop-failed": "안전 중지 미완료 · 신규 주문 권한 차단 유지",
  "functional-test-permit-replacement-requires-stop": "기존 시험을 먼저 안전 중지해야 함",
  "functional-test-authority-runtime-active": "기능시험 runtime이 실행 또는 정지 처리 중",
  "functional-test-authority-session-active": "기능시험 거버넌스 Session이 아직 활성 상태",
  "functional-test-working-orders-unresolved": "미체결·상태 미확정 기능시험 주문 존재",
  "functional-test-durable-order-truth-unavailable": "내구 주문 원장을 확인할 수 없음",
  "functional-test-kis-reconciliation-unresolved": "KIS 계좌·주문·포지션 대조 미완료",
};

function compactHash(value = "") {
  const text = String(value || "");
  return text.length > 18 ? `${text.slice(0, 10)}…${text.slice(-6)}` : text || "-";
}

function formatDateTime(value = "") {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat("ko-KR", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function blockerLabel(value) {
  return BLOCKER_LABELS[value] || String(value || "확인 필요");
}

export default function FunctionalTestWorkspace({ snapshot = {} }) {
  const [testRoute, setTestRoute] = useState("kis");
  const [cryptoSafety, setCryptoSafety] = useState({
    statusKnown: false,
    safeToLeave: false,
    anyLaneActive: false,
    upbit: {},
    binance: {},
  });
  const [showBinding, setShowBinding] = useState(false);
  const [workspace, setWorkspace] = useState(EMPTY_FUNCTIONAL_TEST_WORKSPACE);
  const [selectedKey, setSelectedKey] = useState("");
  const [durationValue, setDurationValue] = useState(6);
  const [durationUnit, setDurationUnit] = useState("HOURS");
  const [authorizedBy, setAuthorizedBy] = useState("desktop-operator");
  const [busyAction, setBusyAction] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [startAcknowledged, setStartAcknowledged] = useState(false);
  const [now, setNow] = useState(() => new Date());
  const routeTabRefs = useRef({});

  async function refresh({ quiet = false } = {}) {
    if (!quiet) setBusyAction("refresh");
    try {
      const result = normalizeFunctionalTestWorkspace(await getFunctionalTestWorkspace());
      setWorkspace(result);
      setError("");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "기능시험 상태를 불러오지 못했습니다.");
    } finally {
      if (!quiet) setBusyAction("");
    }
  }

  useEffect(() => {
    if (testRoute !== "kis") return undefined;
    let cancelled = false;
    getFunctionalTestWorkspace()
      .then((result) => {
        if (!cancelled) setWorkspace(normalizeFunctionalTestWorkspace(result));
      })
      .catch((requestError) => {
        if (!cancelled) {
          setError(requestError instanceof Error ? requestError.message : "기능시험 상태를 불러오지 못했습니다.");
        }
      });
    const timer = window.setInterval(() => {
      setNow(new Date());
      void refresh({ quiet: true });
    }, 60_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [testRoute]);

  const selectedCandidate = useMemo(
    () => preferredFunctionalTestCandidate(workspace, selectedKey),
    [selectedKey, workspace],
  );

  useEffect(() => {
    if (selectedCandidate?.key && selectedCandidate.key !== selectedKey) {
      setSelectedKey(selectedCandidate.key);
    }
  }, [selectedCandidate?.key, selectedKey]);

  const durationBounds = functionalTestDurationBounds(
    durationUnit,
    workspace.durationLimits.maxDays,
  );
  const permit = workspace.current.permit;
  const activation = workspace.current.activation;
  const authorityConfigurationLocked = Boolean(permit)
    || workspace.current.authorityReferencePresent === true;
  const progress = functionalTestProgress(permit, now);
  const blockers = workspace.current.blockers;
  const authorityMutation = workspace.authorityMutation;
  const statusTone = functionalTestStatusTone(workspace.status);
  const effectiveCaps = workspace.effectiveCaps;
  const appliedCaps = effectiveCaps.available ? effectiveCaps.values : null;
  const apiFailClosed = snapshot.api_connected !== true;
  const globalKilled = snapshot.kill_switch === true;
  const routeTarget = testRoute === "crypto"
    ? "Upbit KRW-BTC / Binance Spot BTCUSDT"
    : selectedCandidate?.label || "KIS 대상 미선택";
  const accountScope = testRoute === "crypto"
    ? cryptoSafety.statusKnown
      ? `UPBIT ${compactHash(cryptoSafety.upbit.accountFingerprint)} · BINANCE ${compactHash(cryptoSafety.binance.accountFingerprint)}`
      : "계정 fingerprint 확인 중 · 전송 차단"
    : workspace.account.label;
  const sessionScope = testRoute === "crypto"
    ? cryptoSafety.statusKnown
      ? `UPBIT ${compactHash(cryptoSafety.upbit.sessionId)} · BINANCE ${compactHash(cryptoSafety.binance.sessionId)}`
      : "exact session 확인 중"
    : permit?.permitId || activation?.activationId || "허가 세션 없음";
  const authorityContract = testRoute === "crypto"
    ? "시작마다 네이티브 45초 1회"
    : "허가 + 당일 승인 + 시작 확인";
  const routeStatus = apiFailClosed
    ? "FAIL-CLOSED"
    : globalKilled
      ? "KILLED · 전송 차단"
      : testRoute === "crypto"
        ? !cryptoSafety.statusKnown
          ? "FAIL-CLOSED · lane 상태 확인 중"
          : cryptoSafety.anyLaneActive
            ? "CRYPTO FUNCTIONAL_TEST 활성"
            : "LOCKED · 종료 상태"
        : workspace.runtime.functionalTestRunning
          ? "KIS FUNCTIONAL_TEST 활성"
          : "LOCKED";

  async function runCommand(actionName, command) {
    setBusyAction(actionName);
    setMessage("");
    setError("");
    try {
      const result = await command();
      let nextWorkspace = result.workspace;
      try {
        nextWorkspace = await getFunctionalTestWorkspace();
      } catch {
        // Keep the safe command snapshot; the periodic refresh retries.
      }
      if (nextWorkspace) {
        setWorkspace(normalizeFunctionalTestWorkspace(nextWorkspace));
      }
      if (result.ok === false) {
        setError(result.reason || "기능시험 요청이 차단되었습니다.");
      } else {
        setMessage(result.reason || "처리했습니다.");
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "기능시험 요청에 실패했습니다.");
    } finally {
      setBusyAction("");
      setNow(new Date());
    }
  }

  function changeDurationUnit(nextUnit) {
    const bounds = functionalTestDurationBounds(nextUnit, workspace.durationLimits.maxDays);
    setDurationUnit(nextUnit);
    setDurationValue((current) => Math.max(bounds.min, Math.min(bounds.max, Number(current) || bounds.min)));
  }

  function changeTestRoute(nextRoute) {
    if (nextRoute === testRoute) return true;
    if (testRoute === "kis" && workspace.runtime.functionalTestRunning) {
      setError("실행 중인 KIS 기능시험을 먼저 ‘오늘 실행 정지’로 정리한 뒤 경로를 전환하세요.");
      return false;
    }
    if (testRoute === "crypto" && !cryptoSafety.safeToLeave) {
      setError("Upbit와 Binance Spot의 exact terminal 상태가 모두 IDLE 또는 FINALIZED로 확인될 때까지 코인 제어 화면을 닫을 수 없습니다.");
      return false;
    }
    setError("");
    setTestRoute(nextRoute);
    return true;
  }

  function handleRouteTabKeyDown(event, currentRoute) {
    const routes = ["kis", "crypto"];
    let nextRoute = "";
    if (["ArrowLeft", "ArrowUp"].includes(event.key)) {
      nextRoute = routes[(routes.indexOf(currentRoute) - 1 + routes.length) % routes.length];
    } else if (["ArrowRight", "ArrowDown"].includes(event.key)) {
      nextRoute = routes[(routes.indexOf(currentRoute) + 1) % routes.length];
    } else if (event.key === "Home") {
      nextRoute = routes[0];
    } else if (event.key === "End") {
      nextRoute = routes[routes.length - 1];
    }
    if (!nextRoute) return;
    event.preventDefault();
    if (changeTestRoute(nextRoute)) routeTabRefs.current[nextRoute]?.focus();
  }

  function activateToday() {
    const confirmed = window.confirm(
      "오늘의 KIS 실전 기능시험 준비 토큰을 활성화하시겠습니까? 이 동작은 주문을 보내거나 런타임을 시작하지 않습니다.",
    );
    if (!confirmed) return;
    void runCommand("activate", () => activateFunctionalTestToday(authorizedBy.trim(), true));
  }

  function startExecution() {
    if (!startAcknowledged) return;
    setStartAcknowledged(false);
    void runCommand("start", () => startFunctionalTest(
      workspace.current.selectedTargetKey || "",
      true,
    ));
  }

  function pauseToday() {
    const confirmed = window.confirm(
      "오늘 실행만 정지하시겠습니까? 당일 활성화를 먼저 해제한 뒤 runtime을 멈추고 KIS 주문·잔고를 다시 대조합니다. 전체 permit은 유지되어 다음 거래일에 이어갈 수 있습니다.",
    );
    if (!confirmed) return;
    setStartAcknowledged(false);
    void runCommand("pause", () => pauseFunctionalTestToday(true));
  }

  function endPlan() {
    const confirmed = window.confirm(
      "기능시험 계획을 완전히 종료하시겠습니까? 이 작업은 permit 주문 권한을 되돌릴 수 없게 닫고 KIS 대조 후 현재 포인터를 제거합니다. 이미 접수된 주문 취소나 보유 포지션 청산은 자동으로 수행하지 않습니다.",
    );
    if (!confirmed) return;
    setStartAcknowledged(false);
    void runCommand("end", () => stopFunctionalTest(true));
  }

  return (
    <section className="functional-test-workspace" aria-labelledby="functional-test-heading">
      <header className="functional-test-route-header">
        <div>
          <span className="functional-test-eyebrow">CONTROLLED LIVE TEST ROUTES</span>
          <h2 id="functional-test-heading">주문 기능 검증</h2>
        </div>
        <div className="functional-test-route-tabs" role="tablist" aria-label="기능시험 경로">
          <button
            id="functional-test-kis-tab"
            ref={(node) => { routeTabRefs.current.kis = node; }}
            type="button"
            role="tab"
            aria-selected={testRoute === "kis"}
            aria-controls="functional-test-kis-panel"
            tabIndex={testRoute === "kis" ? 0 : -1}
            className={testRoute === "kis" ? "is-active" : ""}
            onClick={() => changeTestRoute("kis")}
            onKeyDown={(event) => handleRouteTabKeyDown(event, "kis")}
          >
            KIS 기간형
          </button>
          <button
            id="functional-test-crypto-tab"
            ref={(node) => { routeTabRefs.current.crypto = node; }}
            type="button"
            role="tab"
            aria-selected={testRoute === "crypto"}
            aria-controls="functional-test-crypto-panel"
            tabIndex={testRoute === "crypto" ? 0 : -1}
            className={testRoute === "crypto" ? "is-active" : ""}
            onClick={() => changeTestRoute("crypto")}
            onKeyDown={(event) => handleRouteTabKeyDown(event, "crypto")}
          >
            코인 2시간
          </button>
        </div>
      </header>

      <div className="functional-test-safety-strip" aria-label="기능시험 고정 안전 상태">
        <div><span>환경</span><strong>LIVE · 실계좌</strong></div>
        <div><span>시험 경계</span><strong>비승급 · promotionEligible=false</strong></div>
        <div><span>대상</span><strong>{routeTarget}</strong></div>
        <div><span>계정 범위</span><strong>{accountScope}</strong></div>
        <div><span>세션</span><strong>{sessionScope}</strong></div>
        <div><span>승인 계약</span><strong>{authorityContract}</strong></div>
        <div><span>API·전송</span><strong>{routeStatus}</strong></div>
        <div><span>전역 Kill</span><strong>{snapshot.kill_switch ? "KILLED" : snapshot.api_connected ? "NORMAL" : "확인 불가"}</strong></div>
      </div>

      <div className="functional-test-route-notice" role="note">
        <AlertTriangle size={17} aria-hidden="true" />
        <div>
          <strong>{testRoute === "kis" ? "허가서 준비 즉시 전체 시험 기간이 시작됩니다." : "활성 코인 lane은 탭 전환만으로 중지되지 않습니다."}</strong>
          <span>{testRoute === "kis" ? workspace.notice : "다른 경로로 이동하기 전에 중지·정리 또는 복구 완료 상태를 확인하세요."}</span>
        </div>
      </div>

      {testRoute === "crypto" ? (
        <div
          className="functional-test-route-panel"
          id="functional-test-crypto-panel"
          role="tabpanel"
          aria-labelledby="functional-test-crypto-tab"
        >
          <React.Suspense
            fallback={<div className="functional-test-empty" role="status">코인 기능시험 화면을 불러오는 중입니다.</div>}
          >
            <CryptoFirstLivePanel onSafetyStateChange={setCryptoSafety} />
          </React.Suspense>
        </div>
      ) : (
        <div
          className="functional-test-route-panel"
          id="functional-test-kis-panel"
          role="tabpanel"
          aria-labelledby="functional-test-kis-tab"
        >
      <header className="functional-test-hero">
        <div>
          <span className="functional-test-eyebrow">KIS LIVE · TIME BOXED</span>
          <h3>KIS 기간형 기능시험</h3>
          <p>대상·기간·당일 승인·실제 적용 한도를 한 흐름에서 관리합니다.</p>
        </div>
        <div className="functional-test-hero-status">
          <span className={`functional-test-status functional-test-status--${statusTone}`}>
            {workspace.runtime.functionalTestRunning
              ? "기능시험 실행 중"
              : workspace.status === "ACTIVE"
                ? "오늘 활성화"
                : workspace.status === "PERMIT_READY"
                  ? "허가서 준비"
                  : workspace.status === "PAUSED"
                    ? "오늘 정지 · 재개 가능"
                    : workspace.status === "PAUSING"
                      ? "오늘 정지 처리 중"
                      : workspace.status === "PAUSE_REQUIRED"
                        ? "활성화 만료 · 오늘 정지 필요"
                  : workspace.status === "STOP_FAILED"
                    ? "중지 실패 · 주문 차단"
                    : "중지"}
          </span>
          <button
            className="mini-button"
            type="button"
            disabled={Boolean(busyAction)}
            onClick={() => void refresh()}
          >
            <RefreshCw size={14} aria-hidden="true" />
            새로고침
          </button>
        </div>
      </header>

      {(message || error) ? (
        <div className={`functional-test-feedback ${error ? "is-error" : "is-success"}`} role={error ? "alert" : "status"}>
          {error || message}
        </div>
      ) : null}

      <div className="functional-test-grid">
        <section className="panel functional-test-setup">
          <div className="panel-header">
            <div>
              <span>EXACT TEST SCOPE</span>
              <h3>시험 대상과 기간</h3>
            </div>
            <FileKey2 size={19} aria-hidden="true" />
          </div>
          <div className="panel-body functional-test-form">
            <label className="functional-test-field functional-test-field--wide">
              <span>전략 또는 포트폴리오</span>
              <select
                value={selectedCandidate?.key || ""}
                onChange={(event) => setSelectedKey(event.target.value)}
                disabled={Boolean(busyAction) || authorityConfigurationLocked || workspace.candidates.length === 0}
              >
                {workspace.candidates.map((candidate) => (
                  <option key={candidate.key} value={candidate.key} disabled={candidate.available !== true}>
                    {candidate.kind === "PORTFOLIO" ? "포트폴리오" : "전략"} · {candidate.label} · {candidate.symbols.join(", ")}
                    {candidate.available === true ? "" : " · 사용 불가"}
                  </option>
                ))}
                {workspace.candidates.length === 0 ? <option value="">국내주식/ETF 아티팩트 없음</option> : null}
              </select>
            </label>

            <label className="functional-test-field">
              <span>시험 기간</span>
              <input
                type="number"
                min={durationBounds.min}
                max={durationBounds.max}
                step="1"
                value={durationValue}
                onChange={(event) => setDurationValue(event.target.value)}
                disabled={Boolean(busyAction) || authorityConfigurationLocked}
              />
            </label>
            <label className="functional-test-field">
              <span>단위</span>
              <select
                value={durationUnit}
                onChange={(event) => changeDurationUnit(event.target.value)}
                disabled={Boolean(busyAction) || authorityConfigurationLocked}
              >
                <option value="HOURS">시간</option>
                <option value="DAYS">달력일</option>
              </select>
            </label>

            <label className="functional-test-field functional-test-field--wide">
              <span>당일 활성화 담당자</span>
              <input
                type="text"
                maxLength="64"
                value={authorizedBy}
                onChange={(event) => setAuthorizedBy(event.target.value)}
                placeholder="예: operator-name"
                disabled={Boolean(busyAction)}
              />
              <small>실전 활성화는 매 거래일 다시 확인하며 최대 {workspace.durationLimits.dailyActivationMaxHours}시간 또는 KRX 장 마감까지만 유지됩니다.</small>
            </label>

            <label className="functional-test-start-confirm functional-test-field--wide">
              <input
                type="checkbox"
                checked={startAcknowledged}
                onChange={(event) => setStartAcknowledged(event.target.checked)}
                disabled={Boolean(busyAction) || workspace.current.ready !== true || workspace.runtime.functionalTestRunning}
              />
              <span>
                신호 발생 시 실제 KIS 지정가 주문이 전송될 수 있으며, 중지는 기존 주문 취소·포지션 청산을 대신하지 않음을 확인했습니다.
              </span>
            </label>

            <div className="functional-test-actions functional-test-field--wide">
              {authorityMutation.blockers.length > 0 ? (
                <div className="functional-test-feedback is-error" role="status">
                  권한 변경 잠금: {authorityMutation.blockers.map(blockerLabel).join(" · ")}
                </div>
              ) : null}
              <button
                className="secondary-button"
                type="button"
                disabled={
                  Boolean(busyAction)
                  || selectedCandidate?.available !== true
                  || authorityConfigurationLocked
                  || authorityMutation.allowed !== true
                }
                onClick={() => void runCommand(
                  "permit",
                  () => createFunctionalTestPermit(
                    selectedCandidate?.key || "",
                    Number(durationValue),
                    durationUnit,
                  ),
                )}
              >
                <FileKey2 size={15} aria-hidden="true" />
                허가서 준비
              </button>
              <button
                className="secondary-button functional-test-activate-button"
                type="button"
                disabled={
                  Boolean(busyAction)
                  || !permit
                  || Boolean(activation)
                  || !authorizedBy.trim()
                  || workspace.account.credentialsReady !== true
                  || authorityMutation.allowed !== true
                  || ["PAUSING", "STOP_FAILED"].includes(workspace.status)
                }
                onClick={activateToday}
              >
                <Play size={15} aria-hidden="true" />
                오늘 활성화
              </button>
              <button
                className="primary-button functional-test-start-button"
                type="button"
                disabled={
                  Boolean(busyAction)
                  || workspace.current.ready !== true
                  || !startAcknowledged
                  || workspace.runtime.functionalTestRunning
                }
                onClick={startExecution}
              >
                <Play size={15} aria-hidden="true" />
                기능시험 시작
              </button>
              <button
                className="secondary-button"
                type="button"
                disabled={
                  Boolean(busyAction)
                  || (!permit && !activation && !workspace.runtime.functionalTestRunning)
                  || workspace.status === "PAUSING"
                }
                onClick={pauseToday}
              >
                <Square size={14} aria-hidden="true" />
                오늘 실행 정지
              </button>
              <button
                className="danger-button"
                type="button"
                disabled={
                  Boolean(busyAction)
                  || (
                    !workspace.current.authorityReferencePresent
                    && !permit
                    && !workspace.runtime.functionalTestRunning
                  )
                  || workspace.status === "PAUSING"
                }
                onClick={endPlan}
              >
                <Square size={14} aria-hidden="true" />
                계획 완전 종료
              </button>
            </div>
          </div>
        </section>

        <section className="functional-test-binding-disclosure">
          <button
            type="button"
            aria-expanded={showBinding}
            aria-controls="functional-test-binding-details"
            onClick={() => setShowBinding((current) => !current)}
          >
            <span><strong>바인딩 세부정보</strong><small>Artifact·Instance·계좌 hash</small></span>
            <span>{showBinding ? "접기" : "열기"}</span>
          </button>
          <section className="panel functional-test-binding" id="functional-test-binding-details" hidden={!showBinding}>
            <div className="panel-header">
              <div>
                <span>IMMUTABLE BINDING</span>
                <h3>현재 선택 범위</h3>
              </div>
              <BadgeCheck size={19} aria-hidden="true" />
            </div>
            <div className="panel-body">
              {selectedCandidate ? (
                <dl className="functional-test-details">
                  <div><dt>대상</dt><dd>{selectedCandidate.kind === "PORTFOLIO" ? "포트폴리오" : "전략"} · {selectedCandidate.label}</dd></div>
                  <div><dt>Artifact</dt><dd>{selectedCandidate.artifactId}<small>{compactHash(selectedCandidate.artifactHash)}</small></dd></div>
                  <div><dt>Instance</dt><dd>{selectedCandidate.instanceId}</dd></div>
                  <div><dt>계좌</dt><dd>{workspace.account.label}<small>{compactHash(workspace.account.bindingId)}</small></dd></div>
                  <div><dt>종목</dt><dd>{selectedCandidate.symbols.join(", ")}</dd></div>
                  <div><dt>주기</dt><dd>{selectedCandidate.timeframe || "-"}</dd></div>
                </dl>
              ) : (
                <div className="functional-test-empty">무결성이 확인된 국내주식/ETF 대상이 없습니다.</div>
              )}
            </div>
          </section>
        </section>
      </div>

      <section className="panel functional-test-timeline">
        <div className="panel-header">
          <div>
            <span>TIME BOX & DAILY AUTHORIZATION</span>
            <h3>기간과 활성화 상태</h3>
          </div>
          <Clock3 size={19} aria-hidden="true" />
        </div>
        <div className="panel-body">
          <div className="functional-test-time-grid">
            <div><span>전체 시작</span><strong>{formatDateTime(permit?.startsAt)}</strong></div>
            <div><span>전체 종료</span><strong>{formatDateTime(permit?.endsAt)}</strong></div>
            <div><span>남은 시간</span><strong>{permit ? formatFunctionalTestRemaining(progress.remainingMs) : "허가서 없음"}</strong></div>
            <div><span>오늘 활성화 종료</span><strong>{formatDateTime(activation?.expiresAt)}</strong></div>
            <div><span>runtime</span><strong>{workspace.runtime.functionalTestRunning ? "FUNCTIONAL_TEST 실행 중" : "중지"}</strong></div>
          </div>
          <div className="functional-test-progress" aria-label={`기능시험 진행률 ${Math.round(progress.ratio * 100)}%`}>
            <span style={{ width: `${Math.round(progress.ratio * 100)}%` }} />
          </div>
          <p>당일 활성화가 만료되면 주문은 차단됩니다. ‘오늘 실행 정지’로 Runtime과 KIS 대조를 마친 뒤 다음 거래일에 다시 활성화하세요.</p>
        </div>
      </section>

      <div className="functional-test-grid functional-test-grid--bottom">
        <section className="panel">
          <div className="panel-header">
            <div>
              <span>NON-NEGOTIABLE LIMITS</span>
              <h3>현재 실제 적용 한도</h3>
            </div>
            <ShieldCheck size={19} aria-hidden="true" />
          </div>
          <div className="panel-body functional-test-cap-grid">
            <div><span>주문당 수량</span><strong>{appliedCaps ? `${appliedCaps.maxOrderQuantity}주` : "계좌 대조 필요"}</strong><small>허가 상한 {workspace.caps.maxOrderQuantity}주</small></div>
            <div><span>주문당 금액</span><strong>{appliedCaps ? `${Number(appliedCaps.maxOrderNotional).toLocaleString("ko-KR")}원` : "계좌 대조 필요"}</strong><small>허가 상한 {Number(workspace.caps.maxOrderNotional).toLocaleString("ko-KR")}원</small></div>
            <div><span>총 익스포저</span><strong>{appliedCaps ? `${Number(appliedCaps.maxGrossExposure).toLocaleString("ko-KR")}원` : "계좌 대조 필요"}</strong><small>허가 상한 {Number(workspace.caps.maxGrossExposure).toLocaleString("ko-KR")}원</small></div>
            <div><span>누적 최대 주문</span><strong>{appliedCaps ? `${appliedCaps.maxOrders}건` : "계좌 대조 필요"}</strong><small>허가 상한 {workspace.caps.maxOrders}건</small></div>
            <div><span>최대 포지션</span><strong>{appliedCaps ? `${appliedCaps.maxOpenPositions}개` : "계좌 대조 필요"}</strong><small>허가 상한 {workspace.caps.maxOpenPositions}개</small></div>
            <div><span>손실 차단</span><strong>{appliedCaps ? `${Number(appliedCaps.maxLoss).toLocaleString("ko-KR")}원` : "계좌 대조 필요"}</strong><small>허가 상한 {Number(workspace.caps.maxLoss).toLocaleString("ko-KR")}원</small></div>
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <span>READINESS BLOCKERS</span>
              <h3>현재 차단 항목</h3>
            </div>
            <AlertTriangle size={19} aria-hidden="true" />
          </div>
          <div className="panel-body">
            {blockers.length ? (
              <ul className="functional-test-blockers">
                {blockers.map((blocker) => <li key={blocker}>{blockerLabel(blocker)}</li>)}
              </ul>
            ) : (
              <div className="functional-test-ready">
                <BadgeCheck size={18} aria-hidden="true" />
                <span>허가서·당일 활성화·현재 exact binding이 일치합니다. 확인 체크 후 ‘기능시험 시작’을 눌러야 runtime이 시작됩니다.</span>
              </div>
            )}
          </div>
        </section>
      </div>
        </div>
      )}
    </section>
  );
}
