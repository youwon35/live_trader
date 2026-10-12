import { useRef, useState } from "react";
import { getPaperCandidateEvidence, importPaperCandidate, runLocalMonitorTrial, previewReadOnlyPreparation, getReadOnlyPreparationSources, getPaperCandidateAccounts, reviewPaperCandidate } from "./api";

const SCOPE_LABELS = {
  evidenceId: "Evidence ID", evidenceHash: "봉인 Evidence hash",
  evidenceBundleHash: "Evidence Bundle hash", publicationId: "발행 ID",
  publicationHash: "발행 hash", bindingHash: "최종 연결 hash",
  strategyArtifactHash: "전략 hash", strategyInstanceId: "전략 Instance",
  portfolioArtifactHash: "Portfolio hash", portfolioInstanceId: "Portfolio Instance",
  deploymentManifestHash: "Manifest hash", sessionId: "검증 세션",
  promotionPolicyHash: "승급 기준 정책 hash", validationProfile: "검증 기준 분류",
};

const POLICY_CRITERION_LABELS = {
  minimumShadowDecisions: "신호 관찰 판단 횟수", minimumShadowIntents: "신호 관찰 주문 의도",
  minimumOrders: "최소 모의 주문", minimumObservationSeconds: "최소 실제 관찰 시간(초)",
  minimumRegimes: "최소 시장국면", minimumCompletedTrades: "최소 완료 거래",
  returnTolerancePct: "수익률 허용차(%)", mddMultiplier: "낙폭 허용 배수",
  minimumDataAvailabilityPct: "최소 데이터 수신율(%)", minimumSignalMatchPct: "최소 신호 일치율(%)",
  continuousMinimumCoverage: "코인 봉 수신 비율", sessionMinimumCoverage: "주식 장중 봉 수신 비율",
  maximumConsecutiveMissedBoundaries: "최대 연속 누락 봉",
};

const PREPARATION_INPUT_STYLE = { minHeight: 32, padding: "0 8px", color: "var(--text)", background: "var(--input)", border: "1px solid var(--border)", borderRadius: "var(--radius-md)", font: "inherit", width: 160 };

export default function PaperCandidateEvidencePanel({ strategyId = "", onRegistered }) {
  const [inbox, setInbox] = useState(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const pending = useRef(false);
  const [trialSelection, setTrialSelection] = useState("");
  const [trialResult, setTrialResult] = useState(null);
  const [accountProfiles, setAccountProfiles] = useState([]);
  const [reviewSelection, setReviewSelection] = useState({});
  const [capitalPercent, setCapitalPercent] = useState("100");

  const [preparationSources, setPreparationSources] = useState([]);
  const [preparationBusy, setPreparationBusy] = useState(false);
  const [preparationMessage, setPreparationMessage] = useState("");
  const preparationPending = useRef(false);
  const [preparationSelection, setPreparationSelection] = useState("");
  const [preparationSource, setPreparationSource] = useState(null);
  const [preparationResult, setPreparationResult] = useState(null);
  const [preparationExpired, setPreparationExpired] = useState(false);
  const [orderDraft, setOrderDraft] = useState({ instanceId: "", side: "", quantity: "", limitPrice: "" });
  const preparationGeneration = useRef(0);
  const preparationStrategy = useRef(strategyId);
  if (preparationStrategy.current !== strategyId) {
    preparationStrategy.current = strategyId;
    clearPreparation(true);
    setPreparationSelection("");
  }

  function clearPreparation(clearSource = false) {
    preparationGeneration.current += 1;
    setPreparationResult(null);
    setPreparationExpired(false);
    if (clearSource) {
      setPreparationSource(null);
      setOrderDraft({ instanceId: "", side: "", quantity: "", limitPrice: "" });
    }
  }

  const preparationChoices = [
    ...preparationSources.map((item) => ({
      key: item.request.kind === "SEALED_SOURCE" ? "sealed:" + item.request.rootKey + ":" + item.request.targetType + ":" + item.request.targetId : "trial:" + item.request.rootKey + ":" + item.request.portfolioId,
      label: item.name + (item.request.kind === "SEALED_SOURCE" ? " · 실제 Paper 근거 대기" : " · 기능시험 자료"),
      request: item.request,
    })),
    ...(inbox?.candidates || []).filter((item) => item.status === "VERIFIED_READ_ONLY" && item.rootKey && item.identity?.evidenceHash && item.instanceHash && (!strategyId || item.strategyId === strategyId)).map((item) => ({
      key: "paper:" + item.rootKey + ":" + item.evidenceId,
      label: (item.strategyName || item.strategyId) + " · 봉인 Paper 근거",
      request: { kind: "PAPER", rootKey: item.rootKey, evidenceId: item.evidenceId,
        evidenceHash: item.identity?.evidenceHash, instanceHash: item.instanceHash },
    })),
  ];

  async function refreshPreparation() {
    if (preparationPending.current) return;
    preparationPending.current = true;
    setPreparationBusy(true);
    clearPreparation(true);
    setPreparationSelection("");
    try {
      const result = await getReadOnlyPreparationSources();
      if (result?.ok !== true || result.schemaVersion !== "live-readonly-preparation-sources-v1"
        || result.readOnly !== true || result.executable !== false || result.authorityGranted !== false
        || !Array.isArray(result.sources) || result.sources.some((item) => !item || typeof item.name !== "string"
          || (item.request?.kind === "SEALED_SOURCE" ? (item.source?.evidenceClass !== "PAPER_APPROVED_EVIDENCE_PENDING"
            || item.source?.proofPending !== true || item.source?.qualification !== "PAPER_EVIDENCE_PENDING"
            || !["INSTANCE", "PORTFOLIO"].includes(item.request.targetType)
            || ["liveAuthorized", "authorizationGranted", "promotionEligible", "useAsPromotionEvidence", "orderSubmissionEnabled", "armingEnabled"].some((key) => item.source[key] !== false)
            || ["rootKey", "targetId", "targetHash", "identityHash"].some((key) => typeof item.request[key] !== "string" || !item.request[key]))
            : (item.source?.evidenceClass !== "FUNCTIONAL_TEST_NON_PROMOTION" || item.request?.kind !== "NON_PROMOTION"
              || ["rootKey", "portfolioId", "portfolioHash", "identityHash"].some((key) => typeof item.request?.[key] !== "string" || !item.request[key]))))) {
        throw new Error(result?.reason || "준비 자료 목록을 확인하지 못했습니다.");
      }
      setPreparationSources(result.sources);
      setPreparationMessage(result.sources.length ? "" : "현재 원본·승인과 일치하는 준비 자료가 없습니다. 변경된 봉인본은 다시 등록한 뒤 새로고침하세요.");
    } catch (error) {
      setPreparationSources([]);
      setPreparationMessage(error?.message || "준비 자료를 읽지 못했습니다.");
    } finally {
      preparationPending.current = false;
      setPreparationBusy(false);
    }
  }

  async function prepare(readAccount) {
    const selected = preparationChoices.find((item) => item.key === preparationSelection);
    if (preparationPending.current || !selected) return;
    clearPreparation();
    const generation = preparationGeneration.current;
    preparationPending.current = true;
    setPreparationBusy(true);
    try {
      const result = await previewReadOnlyPreparation({ source: selected.request, draft: orderDraft, readAccount });
      if (result?.ok !== true || result.schemaVersion !== "live-read-only-preparation-v1"
        || result.reportPurpose !== "READ_ONLY_PREPARATION" || result.readOnly !== true
        || ["authorityGranted", "authorizationGranted", "executable", "promotionEligible", "useAsPromotionEvidence", "tradingEnabled", "currentDeploymentChanged"].some((key) => result[key] !== false)
        || ["ordersSubmitted", "confirmationTokensCreated", "permitsCreated", "runtimeSessionsCreated"].some((key) => result[key] !== 0)
        || result.source?.kind !== selected.request.kind
        || (selected.request.kind === "NON_PROMOTION" && (result.source?.portfolioHash !== selected.request.portfolioHash || result.source?.evidenceClass !== "FUNCTIONAL_TEST_NON_PROMOTION"))
        || (selected.request.kind === "PAPER" && result.source?.evidenceHash !== selected.request.evidenceHash)
        || (selected.request.kind === "SEALED_SOURCE" && (result.source?.targetHash !== selected.request.targetHash || result.source?.identityHash !== selected.request.identityHash
          || result.source?.proofPending !== true || result.source?.qualification !== "PAPER_EVIDENCE_PENDING"
          || ["liveAuthorized", "authorizationGranted", "promotionEligible", "useAsPromotionEvidence", "orderSubmissionEnabled", "armingEnabled"].some((key) => result.source[key] !== false)))
        || !Array.isArray(result.source?.instruments) || result.source.instruments.length === 0
        || result.source.instruments.some((item) => !item || ["instanceId", "symbol", "broker"].some((key) => typeof item[key] !== "string"))
        || !Array.isArray(result.checks) || result.checks.some((item) => !item || !["PASS", "BLOCKED", "UNKNOWN"].includes(item.status) || typeof item.detail !== "string")
        || (result.observation?.cashPreview !== undefined && (!result.observation.cashPreview || result.observation.cashPreview.executable !== false
          || result.observation.cashPreview.orderSubmissionEnabled !== false || result.observation.cashPreview.armingEnabled !== false
          || result.observation.cashPreview.capitalBasis !== "ACCOUNT_AVAILABLE_CASH" || result.observation.cashPreview.capitalPercent !== 100
          || !Number.isFinite(result.observation.cashPreview.sourceMaxOrderPercent) || result.observation.cashPreview.sourceMaxOrderPercent <= 0
          || !Number.isFinite(result.observation.cashPreview.allocationWeight) || result.observation.cashPreview.allocationWeight < 0 || result.observation.cashPreview.allocationWeight > 1
          || !Array.isArray(result.observation.cashPreview.reasons) || result.observation.cashPreview.reasons.some((item) => typeof item !== "string")))
        || !Array.isArray(result.draft?.missingInputs) || !Array.isArray(result.limitations)
        || !Number.isFinite(Date.parse(result.asOf)) || !Number.isFinite(Date.parse(result.expiresAt))
        || Date.parse(result.expiresAt) <= Date.now() || Date.parse(result.expiresAt) - Date.parse(result.asOf) > 60000) {
        throw new Error(result?.reason || "읽기 전용 준비 결과의 범위와 유효시간을 확인하지 못했습니다.");
      }
      if (preparationGeneration.current !== generation) return;
      setPreparationSource(result.source);
      setPreparationResult(result);
      setPreparationExpired(false);
      window.setTimeout(() => {
        if (preparationGeneration.current === generation) setPreparationExpired(true);
      }, Math.max(0, Date.parse(result.expiresAt) - Date.now()));
      setPreparationMessage(readAccount ? "계좌 읽기 결과를 확인했습니다. 주문 권한과 실행 상태는 변경되지 않았습니다." : "원본을 확인했습니다. 종목과 주문 초안은 직접 입력하세요.");
    } catch (error) {
      clearPreparation();
      setPreparationMessage(error?.message || "읽기 전용 준비 조회에 실패했습니다.");
    } finally {
      preparationPending.current = false;
      setPreparationBusy(false);
    }
  }

  async function refresh() {
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    try {
      const result = await getPaperCandidateEvidence();
      const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
      const invalid = !record(result) || result.schemaVersion !== "live-paper-evidence-inbox-v1"
        || result.readOnly !== true || typeof result.canImport !== "boolean" || typeof result.ok !== "boolean"

        || !Array.isArray(result.candidates) || !Array.isArray(result.errors)
        || (result.monitorTrials !== undefined && (!Array.isArray(result.monitorTrials) || result.monitorTrials.some((trial) => !record(trial)
          || typeof trial.name !== "string" || typeof trial.detail !== "string" || typeof trial.portfolioId !== "string" || typeof trial.canRun !== "boolean"
          || (trial.canRun && (!record(trial.request) || ["rootKey", "portfolioId", "portfolioHash", "identityHash"].some((key) => typeof trial.request[key] !== "string" || !trial.request[key]))))))
        || (Array.isArray(result.candidates) && result.canImport !== result.candidates.some((candidate) => candidate?.canImport === true))
        || result.errors.some((error) => typeof error !== "string")
        || (result.requiredNextStep !== undefined && typeof result.requiredNextStep !== "string")
        || result.candidates.some((candidate) => !record(candidate)
          || typeof candidate.canImport !== "boolean"
          || (candidate.canReview !== undefined && typeof candidate.canReview !== "boolean")
          || (candidate.canReview === true && (!record(candidate.reviewRequest)
            || !Number.isInteger(candidate.reviewRequest.expectedRevision) || candidate.reviewRequest.expectedRevision < 1
            || ["rootKey", "evidenceId", "deploymentId", "identityHash", "registryHash"].some((key) => typeof candidate.reviewRequest[key] !== "string" || !candidate.reviewRequest[key])
            || typeof candidate.brokerId !== "string" || candidate.registered !== true))
          || (candidate.canImport === true && (!record(candidate.importRequest)
            || !Number.isInteger(candidate.importRequest.expectedRevision)
            || candidate.importRequest.expectedRevision < 0
            || ["rootKey", "evidenceId", "identityHash", "registryHash"].some((key) => typeof candidate.importRequest[key] !== "string" || !candidate.importRequest[key])))
          || !["VERIFIED_READ_ONLY", "BLOCKED"].includes(candidate.status)
          || ["evidenceId", "detail"].some((key) => typeof candidate[key] !== "string")
          || (candidate.blockedReasons !== undefined && (!Array.isArray(candidate.blockedReasons)
            || candidate.blockedReasons.some((reason) => !record(reason) || typeof reason.code !== "string" || typeof reason.detail !== "string")
            || (candidate.blockedReasons.length > 0 && (candidate.canImport || candidate.registered === true))))
          || ["strategyId", "strategyName", "portfolioId", "instanceHash", "rootKey"].some((key) => candidate[key] !== undefined && typeof candidate[key] !== "string")
          || (candidate.validationSummary !== undefined && (!record(candidate.validationSummary)
            || !["STANDARD", "CUSTOM_THRESHOLD"].includes(candidate.validationSummary.validationProfile)
            || typeof candidate.validationSummary.defaultForwardCriteriaMet !== "boolean"
            || ["actualObservationSeconds", "actualObservationDays", "actualOrderCount", "actualRegimeCount"].some((key) => !Number.isFinite(candidate.validationSummary[key]) || candidate.validationSummary[key] < 0)))
          || (candidate.promotionPolicy !== undefined && candidate.promotionPolicy !== null && (!record(candidate.promotionPolicy)
            || candidate.promotionPolicy.schemaVersion !== "paper-promotion-policy-v1"
            || candidate.promotionPolicy.promotionEligible !== true
            || !["STANDARD", "CUSTOM_THRESHOLD"].includes(candidate.promotionPolicy.validationProfile)
            || typeof candidate.promotionPolicy.policyHash !== "string"
            || !record(candidate.promotionPolicy.thresholds)
            || !Array.isArray(candidate.promotionPolicy.baselineDifferences)))
          || (candidate.identity !== undefined && (!record(candidate.identity)
            || Object.keys(SCOPE_LABELS).some((key) => candidate.identity[key] !== undefined && typeof candidate.identity[key] !== "string")))
          || (candidate.deployment !== undefined && (!record(candidate.deployment)
            || ["deploymentId", "mode", "lifecycle", "definitionHash"].some((key) => typeof candidate.deployment[key] !== "string")
            || !Number.isInteger(candidate.deployment.revision))));
      if (invalid) throw new Error("검증 근거 응답 형식을 확인하지 못했습니다. 새로고침하세요.");
      setInbox(result.ok ? result : null);
      setTrialSelection("");
      setTrialResult(null);
      setMessage(result.ok ? "" : result.errors.join(" · ") || "검증 근거를 불러오지 못했습니다.");
    } catch (error) {
      setInbox(null);
      setMessage(typeof error?.message === "string" ? error.message : "검증 근거를 불러오지 못했습니다.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }

  async function register(candidate) {
    if (pending.current || !candidate.canImport || !candidate.importRequest) return;
    pending.current = true;
    setBusy(true);
    try {
      const result = await importPaperCandidate(candidate.importRequest);
      if (result?.ok !== true || result.authorizationGranted !== false || typeof result.deploymentId !== "string" || !result.deploymentId) {
        throw new Error(result?.reason || "등록 결과를 확인하지 못했습니다. 새로고침하여 확인하세요.");
      }
      pending.current = false;
      await refresh();
      pending.current = true;
      setBusy(true);
      setMessage(result.detail || "검토 대기 후보로 등록했습니다.");
      if (onRegistered) await onRegistered(result.deploymentId);
    } catch (error) {
      setInbox(null);
      setMessage(error?.message || "후보를 등록하지 못했습니다. 새로고침하세요.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }

  async function refreshAccounts() {
    if (pending.current) return;
    pending.current = true; setBusy(true); setReviewSelection({});
    try {
      const result = await getPaperCandidateAccounts();
      if (result?.ok !== true || result.schemaVersion !== "live-saved-account-profiles-v1"
        || result.readOnly !== true || result.brokerCalls !== 0 || result.ordersSubmitted !== 0
        || !Array.isArray(result.accounts) || result.accounts.some((item) => !item
          || !["kis", "binance", "upbit"].includes(item.brokerId)
          || ["profileId", "alias", "maskedAccount"].some((key) => typeof item[key] !== "string" || !item[key])
          || item.balanceQueried !== false)) throw new Error("저장 계좌 목록의 범위를 확인하지 못했습니다.");
      setAccountProfiles(result.accounts);
      setMessage("저장된 계좌 설정만 읽었습니다. 잔액·주문가능액은 시작 점검에서 확인합니다.");
    } catch (error) { setAccountProfiles([]); setMessage(error?.message || "저장 계좌를 확인하지 못했습니다."); }
    finally { pending.current = false; setBusy(false); }
  }

  async function review(candidate) {
    const key = `${candidate.rootKey}:${candidate.evidenceId}`;
    const accountProfileId = reviewSelection[key];
    const percent = Number(capitalPercent);
    if (pending.current || !candidate.canReview || !candidate.reviewRequest || !accountProfileId
      || !Number.isFinite(percent) || percent <= 0 || percent > 100) return;
    pending.current = true; setBusy(true);
    try {
      const result = await reviewPaperCandidate({ ...candidate.reviewRequest, accountProfileId,
        capitalBasis: "ACCOUNT_AVAILABLE_CASH", capitalPercent: percent });
      if (result?.ok !== true || result.schemaVersion !== "live-paper-candidate-review-v1"
        || result.lifecycle !== "before-live-small" || result.mode !== "MONITOR"
        || result.authorizationGranted !== false || result.realOrdersArmed !== false
        || result.brokerCalls !== 0 || result.ordersSubmitted !== 0
        || typeof result.deploymentId !== "string" || !result.deploymentId)
        throw new Error(result?.reason || "계좌 연결·제한 실거래 준비 결과를 확인하지 못했습니다.");
      pending.current = false; await refresh(); pending.current = true;
      setMessage(result.detail);
      if (onRegistered) await onRegistered(result.deploymentId);
    } catch (error) { setMessage(error?.message || "후보 심사가 완료되지 않았습니다. 새로고침하세요."); }
    finally { pending.current = false; setBusy(false); }
  }

  async function openRegistered(deploymentId) {
    if (pending.current || !onRegistered) return;
    pending.current = true;
    setBusy(true);
    try { await onRegistered(deploymentId); }
    catch (error) { setMessage(error?.message || "등록한 배포를 다시 조회해 주세요."); }
    finally { pending.current = false; setBusy(false); }
  }

  async function runTrial() {
    const selected = (inbox?.monitorTrials || []).find((item) => `${item.request?.rootKey}:${item.portfolioId}` === trialSelection);
    if (pending.current || !selected?.canRun) return;
    pending.current = true;
    setBusy(true);
    setTrialResult(null);
    try {
      const result = await runLocalMonitorTrial(selected.request);
      if (result?.ok !== true || result.schemaVersion !== "live-local-monitor-trial-v1" || result.mode !== "MONITOR"
        || result.authorizationGranted !== false || result.promotionEligible !== false || result.ordersSubmitted !== 0
        || result.currentDeploymentChanged !== false || result.accountCalls !== 0 || result.tradingEnabled !== false
        || result.historicalOnly !== true || result.useAsPromotionEvidence !== false
        || !result.summary || !Array.isArray(result.limitations) || result.limitations.some((item) => typeof item !== "string")
        || result.source?.portfolioId !== selected.request.portfolioId || result.source?.portfolioHash !== selected.request.portfolioHash
        || !Array.isArray(result.source?.bindings) || result.source.bindings.length !== result.summary.symbols
        || result.source.bindings.some((item) => !item || ["instanceId", "symbol", "sampleHash", "sourceFinalBarEnd"].some((key) => typeof item[key] !== "string") || !Number.isInteger(item.sampleCount))
        || ["symbols", "sampleCount", "decisionCount"].some((key) => !Number.isInteger(result.summary[key]) || result.summary[key] < 1)
        || ["BUY", "SELL", "HOLD"].some((key) => !Number.isInteger(result.summary.signals?.[key]) || result.summary.signals[key] < 0)
        || result.summary.sampleCount !== result.summary.decisionCount || typeof result.reportHash !== "string" || typeof result.reportPath !== "string") {
        throw new Error(result?.reason || "연결 시험 결과를 확인하지 못했습니다.");
      }
      setTrialResult(result);
      setMessage("연결 시험을 마쳤습니다. 현재 운영 배포와 실거래 자격은 그대로입니다.");
    } catch (error) { setMessage(error?.message || "연결 시험에 실패했습니다."); }
    finally { pending.current = false; setBusy(false); }
  }

  const visibleMonitorTrials = (inbox?.monitorTrials || []).filter((item) => item.canRun
    || !preparationSources.some((source) => source.request.portfolioId === item.portfolioId));
  const rows = (inbox?.candidates || []).filter(
    (candidate) => !strategyId || !candidate.strategyId || candidate.strategyId === strategyId,
  );
  return (
    <details className="compact-disclosure">
      <summary>모의거래 검증 근거 확인</summary>

      <p>{inbox?.requiredNextStep || "검증된 저장본을 검토 대기 후보로 등록합니다. 등록으로 계좌 연결이나 주문 실행이 승인되지는 않습니다."}</p>
      <button className="secondary-button" disabled={busy} onClick={refresh} type="button">
        {busy ? "확인 중…" : "Paper 검증 근거 새로고침"}
      </button>
      {rows.some((candidate) => candidate.canReview) && <section aria-label="저장 계좌와 총사용한도">
        <button type="button" className="secondary-button" disabled={busy} onClick={refreshAccounts}>저장 계좌 새로고침</button>
        <label>총사용한도 · 현재 계좌 주문가능액의 <input aria-label="계좌 주문가능액 사용 비율" type="number" min="0.01" max="100" step="0.01" value={capitalPercent} disabled={busy} onChange={(event) => setCapitalPercent(event.target.value)} style={{ ...PREPARATION_INPUT_STYLE, width: 90 }} />%</label>
        <p>100%는 계좌의 주문가능액 전체를 총사용한도로 준비합니다. 실제 금액과 주문당 수량·위험 한도는 선택 배포의 시작 점검에서 확인합니다.</p>
      </section>}
      {message && <p role="status">{message}</p>}
      {inbox?.ok && rows.length === 0 && <p>{preparationChoices.length ? "정규 Paper 승급 근거는 없습니다. 아래 기능시험 자료로 읽기 전용 준비를 확인할 수 있습니다." : "확인할 근거가 없습니다. 모의거래에서 검증 근거를 발행한 뒤 다시 확인하세요."}</p>}
      {inbox?.monitorTrialError && <p role="status">연결 시험 목록: {inbox.monitorTrialError}</p>}
      {visibleMonitorTrials.length > 0 && <section aria-label="연결 시험 · 주문 없음" style={{ margin: "16px 0" }}>
        <h3>연결 시험 · 주문 없음</h3>
        <p>비승급 연구용 저장본의 과거 종가를 재생합니다. 현재 운영 배포·계좌·실거래 자격은 바뀌지 않습니다.</p>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          <label>시험용 구성 선택 <select value={trialSelection} disabled={busy} onChange={(event) => { setTrialSelection(event.target.value); setTrialResult(null); }}>
            <option value="">구성을 선택하세요</option>
            {visibleMonitorTrials.filter((item) => item.canRun).map((item) => <option key={`${item.request.rootKey}:${item.portfolioId}`} value={`${item.request.rootKey}:${item.portfolioId}`}>{item.name} · {item.detail}</option>)}
          </select></label>
          <button type="button" className="secondary-button" disabled={busy || !trialSelection} onClick={runTrial}>주문 없이 연결 시험</button>
        </div>
        {visibleMonitorTrials.filter((item) => !item.canRun).map((item, index) => <p key={index}>{item.name}: {item.detail}</p>)}
        {trialResult && <div role="status">
          <p>{trialResult.summary.symbols}종목 · 종가 {trialResult.summary.sampleCount.toLocaleString()}개 · 판단 {trialResult.summary.decisionCount.toLocaleString()}회 · 주문 0건</p>
          <p>매수 판단 {trialResult.summary.signals.BUY}회 · 매도 판단 {trialResult.summary.signals.SELL}회 · 관망 {trialResult.summary.signals.HOLD}회</p>
          <p>개별 시각·OHLC·거래량이 없는 종가 표본입니다. 가상 연속 시각과 보유 상태로 연결만 시험합니다.</p>
          <details><summary>입력 근거와 한계 보기</summary>
            {trialResult.limitations.map((text, index) => <p key={index}>{text}</p>)}
            <p>실제 전체 기간: 표본 시각이 없어 미확인</p>
            {trialResult.source.bindings.map((item) => <p key={item.instanceId}>{item.symbol} · 원본 기준 마지막 시각 {item.sourceFinalBarEnd} · 종가 {item.sampleCount}개 · 입력 hash <code>{item.sampleHash}</code></p>)}
            <p>보고서 hash <code>{trialResult.reportHash}</code></p><p>저장 위치 {trialResult.reportPath}</p>
          </details>
        </div>}
      </section>}
      {<section aria-label="주문 준비 자료 확인" style={{ margin: "16px 0" }}>
        <h3>주문 준비 자료 확인 · 읽기 전용</h3>
        <p>Live 설정 계좌와 선택 종목을 조회합니다. 조회만 진행하며 주문은 전송하지 않습니다.</p>
        <button type="button" className="secondary-button" disabled={preparationBusy} onClick={refreshPreparation}>{preparationBusy ? "준비 자료 확인 중…" : "준비 자료 새로고침"}</button>
        {preparationMessage && <p role="status">{preparationMessage}</p>}
        <label>준비할 원본 <select aria-label="준비할 원본" value={preparationSelection} disabled={preparationBusy} onChange={(event) => {
          setPreparationSelection(event.target.value); clearPreparation(true);
        }}><option value="">원본을 선택하세요</option>
          {preparationChoices.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}
        </select></label>{" "}
        <button type="button" className="secondary-button" disabled={preparationBusy || !preparationSelection} onClick={() => prepare(false)}>원본 확인 · 계좌 조회 없음</button>
        {preparationSource && <>
          <p>{preparationSource.kind === "SEALED_SOURCE" ? "Paper 승인 Source · 실제 Paper 승급 근거 대기" : preparationSource.kind === "NON_PROMOTION" ? "기능시험 자료 · 정규 실거래 승급 전" : "봉인 Paper 자료 · 현재 실거래 자격은 별도 확인"}</p>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", margin: "12px 0" }}>
            {preparationSource.kind === "SEALED_SOURCE" && <p>현재 원본과 현재 PAPER 승인 기록을 대조했습니다. 실제 Paper 관찰·종결·독립 검증 근거가 생기기 전에는 Live 후보 등록·주문·arming이 불가합니다. {preparationSource.approvalBindingStatus === "HISTORICAL_APPROVAL_SOURCE_HASH_NOT_RECORDED" ? "역사 승인 기록에 Source hash 결합이 기록되지 않았습니다." : "승인 기록의 Source hash가 일치합니다."}</p>}
          <label>종목 <select aria-label="종목" value={orderDraft.instanceId} disabled={preparationBusy} onChange={(event) => { setOrderDraft({ ...orderDraft, instanceId: event.target.value }); clearPreparation(); }}>
              <option value="">직접 선택하세요</option>
              {preparationSource.instruments.map((item) => <option key={item.instanceId} value={item.instanceId}>{item.symbol} · {item.broker}</option>)}
            </select></label>
            <label>매수/매도 <select aria-label="매수/매도" value={orderDraft.side} disabled={preparationBusy} onChange={(event) => { setOrderDraft({ ...orderDraft, side: event.target.value }); clearPreparation(); }}>
              <option value="">직접 선택하세요</option><option value="BUY">매수</option><option value="SELL">매도</option>
            </select></label>
            <label>수량 <input style={PREPARATION_INPUT_STYLE} aria-label="수량" inputMode="decimal" placeholder="직접 입력" value={orderDraft.quantity} disabled={preparationBusy} onChange={(event) => { setOrderDraft({ ...orderDraft, quantity: event.target.value }); clearPreparation(); }} /></label>
            <label>지정가 <input style={PREPARATION_INPUT_STYLE} aria-label="지정가" inputMode="decimal" placeholder="직접 입력" value={orderDraft.limitPrice} disabled={preparationBusy} onChange={(event) => { setOrderDraft({ ...orderDraft, limitPrice: event.target.value }); clearPreparation(); }} /></label>
          </div>
          <button type="button" className="secondary-button" disabled={preparationBusy || !orderDraft.instanceId} onClick={() => prepare(true)}>계좌·미체결 읽기 및 입력 금액 계산</button>
          <small>수량을 잔액에서 자동 선택하지 않습니다. 빈 입력은 미입력입니다.</small>
        </>}
        {preparationResult && <div role="status">
          <p>조회 시각 {preparationResult.asOf} · 만료 {preparationResult.expiresAt}</p>
          {preparationExpired ? <p>조회가 만료됐습니다. 현재 상태를 다시 조회하세요.</p> : <>
            <p>입력 금액: {preparationResult.draft.notional ?? "미입력"} {preparationResult.observation?.currency || ""} · 미입력 항목 {preparationResult.draft.missingInputs.length}개</p>
            {preparationResult.observation?.marketSession && <p>시장 시간: {preparationResult.observation.marketSession.detail} · {preparationResult.observation.marketSession.localTime || "연속 시장"} · 거래 허가 없음</p>}
            {preparationResult.observation?.cashPreview && <div aria-label="주문가능현금 기준 준비">
              <p>{preparationResult.observation.accountAlias} {preparationResult.observation.maskedAccount} · 저장 credential 설정 관측 · Native 계좌 심사 전</p>
              <p>계산 범위: 매수 현금 한도 전용 · 매도/방향 미선택 수량은 확정하지 않습니다.</p>
              <p>총 사용 기준: 주문가능현금 100% · Source 주문 비율 {preparationResult.observation.cashPreview.sourceMaxOrderPercent}% · Portfolio 배분 {(100 * preparationResult.observation.cashPreview.allocationWeight).toFixed(4)}%</p>
              <p>현재 주문가능현금 {preparationResult.observation.cashPreview.availableCash ?? "미확인"} · 배분 후 Source 한도 {preparationResult.observation.cashPreview.sourceOrderBudget ?? "미확인"} {preparationResult.observation.currency}</p>
              <p>수수료/슬리피지 여유 후 {preparationResult.observation.cashPreview.notionalAfterCostAllowance ?? "미확인"} · 정적 단위 검사 수량 {preparationResult.observation.cashPreview.maximumQuantity ?? "차단·미확정"}</p>
              <p>수량 단위 {preparationResult.observation.constraints?.LOT_SIZE?.stepSize ?? preparationResult.observation.constraints?.quantityStep ?? "미확인"} · 최소 주문 금액 {preparationResult.observation.constraints?.NOTIONAL?.minNotional ?? preparationResult.observation.constraints?.MIN_NOTIONAL?.minNotional ?? preparationResult.observation.constraints?.minNotional ?? "미확인"} · 호가 단위 {preparationResult.observation.constraints?.PRICE_FILTER?.tickSize ?? preparationResult.observation.constraints?.priceTick ?? "미확인"}</p>
              <p>실제 적용 수수료 {preparationResult.observation.cashPreview.actualBrokerFeeObserved ? "관측됨" : "미확인 · 원본 가정만 참고"} · 조회 범위 {preparationResult.observation.cashScope || preparationResult.observation.openOrdersScope || "미확인"}</p>
              <p>호가·최소 단위·거래소 바인딩·실제 수수료가 미확인되면 수량을 확정하지 않습니다. 직접 시작 전 재조회가 필요합니다.</p>
              {(preparationResult.observation.cashPreview.reasons || []).map((reason, index) => <p key={index}>{reason}</p>)}
            </div>}
            <p>계좌 조회 {preparationResult.observation?.status === "NOT_REQUESTED" ? "전" : preparationResult.observation?.account === "AVAILABLE" ? "완료" : "미확인"} · 실거래 승급 및 계좌 연결은 별도 확인</p>
          </>}
          <details><summary>검사 세부정보</summary>
            <ul>{preparationResult.checks.map((item) => <li key={item.code}>{({ PASS: "확인", BLOCKED: "차단", UNKNOWN: "미확인" })[item.status]} · {item.detail}</li>)}</ul>
          {preparationResult.limitations.map((item, index) => <p key={index}>{item}</p>)}

            <p>권한 없음 · 실행 불가 · 확인 토큰 0 · 주문 허가 0 · 실행 세션 0</p>
            <p>목적 READ_ONLY_PREPARATION · 원본 {preparationResult.source.evidenceClass || "미기재"} · {preparationResult.source.qualification}</p>
            <p>계좌 상태 {preparationResult.observation?.status} · hash <code>{preparationResult.reportHash}</code></p>
          </details>
        </div>}
      </section>}
      {rows.length > 0 && (
        <div className="table-scroll">
          <table>
            <thead><tr><th>전략 · 검증 근거</th><th>확인 결과</th></tr></thead>
            <tbody>{rows.map((candidate, index) => (
              <tr key={`${candidate.rootKey || "blocked"}:${candidate.evidenceId}:${index}`}>
                <td style={{ verticalAlign: "top" }}>{candidate.strategyName || candidate.strategyId || candidate.evidenceId}<br /><small>{candidate.evidenceId}</small></td>
                <td style={{ verticalAlign: "top" }}>{candidate.detail}
                  {candidate.status === "VERIFIED_READ_ONLY" && <div aria-label="후보 준비 단계">
                    <p>Backtester 저장본 · 현재 전략과 실행 단위 일치</p>
                    <p>Paper 검증 근거 · 봉인 연결 확인</p>
                    {candidate.validationSummary && <div aria-label="Paper 검증 기준과 관찰 범위">
                      <p>{candidate.validationSummary.validationProfile === "CUSTOM_THRESHOLD" ? "사용자 설정 기준 통과 · CUSTOM_THRESHOLD" : "기본 기준 통과 · STANDARD"}</p>
                      <p>실제 관찰 {candidate.validationSummary.actualObservationSeconds.toLocaleString("ko-KR")}초 · {candidate.validationSummary.actualObservationDays}일 · 주문 {candidate.validationSummary.actualOrderCount}건 · 시장국면 {candidate.validationSummary.actualRegimeCount}개</p>
                      <p>기본 forward 기준(30일·5주문·2국면·봉 수신): {candidate.validationSummary.defaultForwardCriteriaMet ? "충족" : "미충족"}</p>
                      {candidate.promotionPolicy && <details><summary>이 후보의 봉인 승급 기준</summary>
                        <p>정책 hash <code>{candidate.promotionPolicy.policyHash}</code></p>
                        <p>설정 기준은 이 검증 세션에 고정되어 있습니다. 기능시험 근거와 별도로 검증했습니다.</p>
                        <dl>{Object.entries(candidate.promotionPolicy.thresholds).map(([key, value]) => <div key={key}><dt>{POLICY_CRITERION_LABELS[key] || key}</dt><dd>{String(value)}</dd></div>)}</dl>
                      </details>}
                    </div>}
                    <p>Live 후보 등록 · {candidate.canImport ? "검토 대기 등록 가능" : candidate.registered ? "등록됨" : candidate.blockedReasons?.length ? "기존 배포 조건으로 차단" : "상태 추가 확인 필요"}</p>
                    {candidate.blockedReasons?.length > 0 && <ul aria-label="후보 등록 차단 사유">
                      {candidate.blockedReasons.map((reason, reasonIndex) => <li key={`${reason.code}:${reasonIndex}`}>{reason.detail}</li>)}
                    </ul>}
                    <small>실거래 승인과 현재 계좌 상태는 이 조회에서 확인하지 않습니다.</small>
                  </div>}
                  <div style={{ display: "flex", gap: 8, margin: "8px 0", flexWrap: "wrap" }}>
                  {candidate.canImport && <button type="button" className="primary-button" disabled={busy} onClick={() => register(candidate)}>검토 대기 후보 등록</button>}
                  {candidate.canReview && <>
                    <label>저장 계좌 선택 <select aria-label={`계좌 선택 ${candidate.evidenceId}`} disabled={busy} value={reviewSelection[`${candidate.rootKey}:${candidate.evidenceId}`] || ""} onChange={(event) => setReviewSelection({ ...reviewSelection, [`${candidate.rootKey}:${candidate.evidenceId}`]: event.target.value })}>
                      <option value="">직접 선택하세요</option>
                      {accountProfiles.filter((account) => account.brokerId === candidate.brokerId).map((account) => <option key={account.profileId} value={account.profileId}>{account.alias} · {account.maskedAccount}</option>)}
                    </select></label>
                    <button type="button" className="primary-button" disabled={busy || !reviewSelection[`${candidate.rootKey}:${candidate.evidenceId}`] || !(Number(capitalPercent) > 0 && Number(capitalPercent) <= 100)} onClick={() => review(candidate)}>계좌 연결·제한 실거래 준비 확인</button>
                  </>}
                  {candidate.registered && onRegistered && <button type="button" className="secondary-button" disabled={busy} onClick={() => openRegistered(candidate.deployment.deploymentId)}>등록한 배포 보기</button>}
                  </div>
                  {candidate.identity && <details>
                    <summary>봉인 정보 보기</summary>
                    <p>현재 배포: {candidate.deployment?.deploymentId || "미등록"} · 상태: {candidate.deployment?.mode || "미확인"} · revision: {candidate.deployment?.revision ?? "-"}</p>
                    <dl>{Object.entries(SCOPE_LABELS).map(([key, label]) => candidate.identity[key] && (
                      <div key={key}><dt>{label}</dt><dd><code>{candidate.identity[key]}</code></dd></div>
                    ))}<div><dt>현재 실행 단위 hash</dt><dd><code>{candidate.instanceHash}</code></dd></div></dl>
                  </details>}
                </td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}
    </details>
  );
}
