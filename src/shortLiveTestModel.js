export const SHORT_LIVE_ROUTES = Object.freeze([
  { id: "KIS_KR_CASH", label: "한국투자증권 국내 현금" },
  { id: "BINANCE_SPOT", label: "Binance 현물" },
  { id: "BINANCE_USDM_FUTURES", label: "Binance USDT 선물" },
  { id: "UPBIT_KRW_SPOT", label: "Upbit 원화 현물" },
]);
export const SHORT_LIVE_TIMEFRAMES = Object.freeze(["5m", "1h", "1d"]);
export const SHORT_LIVE_CONFIRMATION = "실제 주문 시험 시작";
export const SHORT_LIVE_DEFAULTS = Object.freeze({
  durationHours: 6, budgetKrwPerBroker: 100000, lossKrwPerBroker: 50000,
});
const hashes = /^[0-9a-f]{64}$/i;
const active = new Set(["RUNNING", "STARTING", "STOPPING", "RECONCILING"]);
const routeIds = new Set(SHORT_LIVE_ROUTES.map((route) => route.id));
const strings = (value) => Array.isArray(value) ? value.filter((item) => typeof item === "string") : [];
const text = (value) => typeof value === "string" ? value : "";
const numeric = (value) => typeof value === "number" && Number.isFinite(value) ? value : null;

export function sanitizeShortLiveText(value) {
  return text(value)
    .replace(/(?:api[_ -]?key|secret|access[_ -]?token|authorization|account(?:number|no)?|계좌번호)\s*[:=]\s*[^\s,;]+/gi, "[비공개 정보 숨김]")
    .replace(/\bBearer\s+\S+/gi, "[인증정보 숨김]")
    .replace(/\b\d{8,14}(?:-\d{2,4})?\b/g, "[번호 숨김]")
    .slice(0, 260);
}

export function shortLiveBlockerLabel(value) {
  const code = text(typeof value === "string" ? value : value?.code);
  const route = SHORT_LIVE_ROUTES.find((item) => code.startsWith(`${item.id}:`));
  if (route) return `${route.label} · ${shortLiveBlockerLabel(code.slice(route.id.length + 1))}`;
  for (const prefix of ["START_BLOCKED:", "NATIVE_ORDER_SIZE_BLOCKED:"]) {
    if (code.startsWith(prefix)) return code.slice(prefix.length).split(",").map(shortLiveBlockerLabel).join(" · ");
  }
  const labels = {
    "insufficient-balance": "사용 가능한 잔고 부족",
    "minimum-order-not-met": "거래소 최소 주문 금액·수량 미달",
    "market-closed": "현재 시장이 닫혀 있음",
    "route-not-connected": "해당 주문 경로가 아직 연결되지 않음",
    "candidate-missing": "이 조합에 정확히 연결된 전략 후보 없음",
    "artifact-integrity-invalid": "전략 원본 또는 인스턴스 해시 불일치",
    "scope-changed": "준비 이후 시험 범위 변경 · 다시 준비 필요",
    "runtime-active": "기존 시험이 아직 실행 또는 종료 대조 중",
    "futures-configuration-not-confirmed": "선물 레버리지·격리 방식 확인 필요",
    "stale-snapshot": "현재 상태를 새로 확인하지 못함",
    NO_AVAILABLE_BALANCE: "현재 주문에 사용할 수 있는 잔고 없음",
    AVAILABLE_BELOW_NATIVE_MINIMUM: "현재 잔고가 거래소 최소 주문 금액·수량보다 부족함",
    AVAILABLE_BUDGET_EXHAUSTED: "선택한 시험에 배정된 주문 예산을 모두 사용함",
    FUNDING_OR_EXISTING_EXPOSURE_BLOCKED: "주문 가능 잔고 부족 또는 기존 보유·미체결과 시험 범위 충돌",
    FUNDING_OBSERVATION_UNKNOWN: "주문 가능 잔고와 기존 노출을 현재 자료로 확인하지 못함",
    ORDER_BELOW_EXCHANGE_MINIMUM: "계산된 주문 금액·수량이 거래소 최소 기준보다 작음",
    ORDER_ABOVE_EXCHANGE_MAXIMUM: "계산된 주문이 거래소 최대 기준을 초과함",
    KRX_SESSION_CLOSED: "현재 국내 정규장이 닫혀 있어 주문 시험 시작 불가",
    KRX_TEST_DURATION_EXCEEDS_SESSION: "선택한 5~7시간 시험을 국내 정규장 안에 마칠 수 없음 · 장 초반에 준비 필요",
    KIS_CONFIRMED_COST_POLICY_REQUIRED: "한국투자증권 실제 수수료·매도세율의 확인된 설정 필요",
    FEE_RATE_UNKNOWN: "실제 주문 수수료를 확인하지 못함",
    SELL_TAX_UNKNOWN: "국내주식 매도세율을 확인하지 못함",
    MIN_NOTIONAL_UNKNOWN: "거래소 최소 주문 기준을 확인하지 못함",
    NATIVE_ORDERABLE_QUANTITY_UNKNOWN: "증권사가 확인한 실제 주문 가능 수량을 읽지 못함",
    NATIVE_ORDERABLE_QUANTITY_BELOW_MINIMUM: "증권사의 실제 주문 가능 수량이 최소 주문 단위보다 부족함",
    FUTURES_SETTINGS_REQUIRE_1X_ISOLATED: "선물 계좌의 1배 레버리지·격리 방식 확인 필요 · 자동 변경하지 않음",
    FUTURES_POSITION_MODE_UNKNOWN: "선물 포지션 모드를 확인하지 못함",
    FUTURES_SINGLE_ASSET_MODE_REQUIRED: "현재 선물 계좌를 단일 자산 증거금 모드로 확인해야 함 · 자동 변경하지 않음",
    FUTURES_CONFIG_UNKNOWN: "선물 계좌의 현재 레버리지·격리 설정을 확인하지 못함",
    FUTURES_CONDITIONAL_ORDER_PRESENT_OR_UNKNOWN: "선물 조건주문이 남아 있거나 조건주문 조회가 미확정임",
    BINANCE_SHARED_BUDGET_ACCOUNT_MISMATCH: "Binance 현물·선물을 같은 거래소 예산으로 묶을 계좌 정보를 확인하지 못함",
    EXCHANGE_SHARED_BUDGET_EXCEEDED: "거래소별 합산 자본 10만원 한도를 초과함",
    SHARED_BROKER_BUDGET_EXCEEDED: "거래소별 합산 자본 10만원 한도를 초과함",
    EXCHANGE_BUDGET_TOO_LARGE: "거래소별 자본 한도가 승인된 10만원을 초과함",
    ROUTE_BUDGET_TOO_LARGE: "해당 주문 경로의 배정 자본이 허용 한도를 초과함",
    ROUTE_EXPOSURE_EXCEEDED: "기존 노출과 새 주문을 더하면 경로별 자본 한도를 초과함",
    REAL_ORDERS_DISABLED: "실주문 허용 설정이 꺼져 있음",
    DRY_RUN_ACTIVE: "현재 모의 실행 설정이 켜져 있음",
    OPERATOR_CONFIRMATION_REQUIRED: "운영자의 실주문 확인 설정 필요",
    EXPLICIT_LIVE_MODE_REQUIRED: "실거래 모드가 명시적으로 선택되지 않음",
    EMERGENCY_OR_RISK_HALT_ACTIVE: "긴급 중지 또는 위험 중지가 적용 중",
    MANUAL_NEW_ENTRIES_BLOCKED: "사용자가 신규 진입을 차단한 상태",
    NEW_ENTRIES_BLOCKED: "현재 신규 진입이 차단된 상태",
    OTHER_RUNTIME_ACTIVE: "다른 실행이 진행 중이어서 동시 시험을 시작할 수 없음",
    OTHER_FUNCTIONAL_AUTHORITY_ACTIVE: "다른 주문 기능시험이 같은 주문 경로를 사용 중",
    GENUINE_UI_OWNER_UNAVAILABLE: "정상 Live Trader 앱의 실행 소유자를 확인하지 못함",
    PREPARATION_EXPIRED: "준비 결과의 유효 시간이 지남 · 다시 무주문 준비 필요",
    PREPARATION_REQUIRED: "시작 전에 선택한 범위로 무주문 준비 필요",
    PREPARATION_SCOPE_MISMATCH: "준비 때 확인한 범위와 시작 요청이 다름",
    ACTIVE_SCOPE_CANNOT_BE_REPLACED: "기존 시험의 종료 대조가 끝나기 전에는 시험 범위 변경 불가",
    DUPLICATE_START_FORBIDDEN: "같은 시작 요청을 반복할 수 없음 · 기존 실행 상태 확인 필요",
    EXPLICIT_MANUAL_UI_CONFIRMATION_REQUIRED: "선택 범위·손실 가능성 확인과 시작 문구 직접 입력 필요",
    SOURCE_FILE_UNAVAILABLE: "선택한 전략 원본 파일을 읽지 못함",
    SOURCE_RAW_HASH_CHANGED: "준비 이후 전략 원문이 변경됨 · 원본 재확인 필요",
    OFFICIAL_ACCOUNT_READ_UNKNOWN: "공식 API의 현재 잔고·보유·미체결 조회가 미확정임",
    OFFICIAL_OWNED_ORDER_UNKNOWN: "이 시험이 보낸 주문의 실제 상태가 미확정임 · 재주문 차단",
    OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN: "공식 시세·최소 주문·수수료·환율 중 확인되지 않은 항목이 있음",
    OFFICIAL_PRIVATE_READ_REJECTED: "거래소가 인증된 읽기 요청을 거절함 · 연결·조회 권한 확인 필요",
    OFFICIAL_READ_REJECTED: "거래소가 현재 자료 조회를 거절함",
    SAVED_ACCOUNT_IDENTITY_UNKNOWN: "저장된 계좌 연결을 정확히 확인하지 못함",
    ACCOUNT_TRADING_DISABLED: "거래소 계좌의 거래 가능 상태를 확인하지 못함",
    COMPLETE_ACCOUNT_TRUTH_REQUIRED: "잔고·보유·미체결 전체 조회가 완료되지 않음",
    ACCOUNT_SCOPE_OWNERSHIP_UNVERIFIED: "시험 대상 계좌와 실행 소유 관계를 확인하지 못함",
    EXACT_ACCOUNT_BINDING_MISMATCH: "준비한 계좌와 현재 조회 계좌가 일치하지 않음",
    PREPARED_ACCOUNT_CHANGED: "준비 이후 연결 계좌가 변경됨",
    UNSCOPED_SPOT_ASSET_PRESENT: "시험 범위 밖의 기존 보유 자산이 있어 분리 확인 필요",
    UNOWNED_ACCOUNT_POSITION: "이 시험의 소유로 확인되지 않은 기존 포지션이 있음",
    UNOWNED_ACCOUNT_ORDER: "이 시험의 소유로 확인되지 않은 기존 미체결 주문이 있음",
    ACCOUNT_ROWS_INCOMPLETE: "계좌 잔고 목록의 전체 조회가 완료되지 않음",
    OPEN_ORDERS_INCOMPLETE: "현재 미체결 주문의 전체 조회가 완료되지 않음",
    POSITIONS_INCOMPLETE: "현재 포지션의 전체 조회가 완료되지 않음",
    KIS_ACCOUNT_TRUTH_INCOMPLETE: "한국투자증권 잔고·보유 전체 조회가 완료되지 않음",
    KIS_BALANCE_TOTAL_UNKNOWN: "한국투자증권 계좌 잔고 합계를 확인하지 못함",
    KIS_EQUITY_UNKNOWN: "한국투자증권 계좌 평가금액을 확인하지 못함",
    FRESH_FX_REQUIRED: "원화 예산 환산에 사용할 최신 환율 확인 필요",
    FX_OBSERVATION_CONFLICT: "계좌와 주문 시세의 환율 관측이 일치하지 않음",
    OBSERVATION_STALE: "시세 또는 계좌 조회가 오래되어 현재 주문 조건 재확인 필요",
    FRESH_CLOCK_GUARD_UNKNOWN: "현재 시각의 정확성을 확인하지 못함",
    FRESH_CLOCK_GUARD_NOT_POSITIVE: "현재 시각 검사 조건을 충족하지 못함",
    CLOCK_OFFSET_EXCEEDED: "현재 시각 오차가 허용 범위를 벗어남",
    NATIVE_FEED_CLOCK_INTEGRITY_FAILED: "실제 시세 수신 시각의 정확성 조건을 충족하지 못함",
    ORIGINAL_ALGORITHM_WARMUP_INSUFFICIENT: "선택한 원래 전략을 계산할 실제 과거 봉이 아직 부족함",
    ORIGINAL_CLOSED_BAR_WARMUP_REQUIRED: "전략 준비에는 실제 완성된 과거 봉이 필요함",
    WARMUP_CONTAINS_UNCLOSED_BAR: "과거 자료에 아직 완성되지 않은 봉이 포함됨",
    SPOT_SHORT_FORBIDDEN: "현물 시험에서는 보유 없는 매도 진입을 지원하지 않음",
    ORIGINAL_PLUGIN_HAS_NO_SIGNED_SHORT_EVALUATOR: "선택한 원래 플러그인에 선물 숏 진입 평가기가 없음",
    EXACT_ORDER_OBSERVER_UNAVAILABLE: "이 시험의 주문을 정확히 대조할 수신 경로를 확인하지 못함",
    RECONCILIATION_UNKNOWN: "잔고·주문·체결 대조가 미확정임 · 신규 진입 차단",
    CLEANUP_UNKNOWN: "미체결·잔여 포지션 종료 대조가 미확정임",
    DURABLE_CLEANUP_OBSERVER_REQUIRED: "실제 종료 주문을 지속해서 대조할 관측 연결 확인 필요",
    "functional-backtest-review-required": "원래 전략의 백테스트 기능 검토 근거가 아직 없음",
    "unsupported-short-live-market-route": "이 전략의 거래소·주문 시장을 지원 경로에 연결하지 못함",
    "unsupported-timeframe": "이 시험이 지원하는 5분·1시간·1일봉과 다른 시간봉임",
    "native-provider-route-mismatch": "전략의 시세 공급자와 주문 경로가 일치하지 않음",
    "original-execution-timing-missing-conflicting-or-unsupported": "원래 전략의 주문 시점이 누락되거나 서로 다르게 기록됨",
    "same-strategy-instance-selected-through-multiple-parents": "동일 인스턴스를 개별 대상과 포트폴리오에서 중복 선택함",
    "same-strategy-instance-selected-through-multiple-roots": "동일 인스턴스를 서로 다른 원본 저장소에서 중복 선택함",
    "exact-selection-not-found-or-source-changed": "선택한 원본이 변경되었거나 정확한 대상을 다시 찾지 못함",
    "selection-file-hash-required-or-changed": "선택 원문 해시가 누락되거나 준비 이후 변경됨",
    "selection-instance-file-hash-required-or-changed": "인스턴스 원문 해시가 누락되거나 준비 이후 변경됨",
  };
  if (labels[code]) return labels[code];
  const message = sanitizeShortLiveText(typeof value === "string" ? value : value?.message || code);
  return /[가-힣]/.test(message) ? message : "현재 실행 조건을 확인하지 못함 · 준비 결과와 연결 상태 재확인 필요";
}

export function normalizeShortLiveCandidate(value = {}) {
  const kind = ["portfolio", "PORTFOLIO_INSTANCE"].includes(value.kind) ? "portfolio"
    : ["instance", "STRATEGY_INSTANCE"].includes(value.kind) ? "instance" : "";
  const eligible = value.eligibleForFunctionalPreparation === true;
  return {
    selectionKey: text(value.selectionKey), route: text(value.route), timeframe: text(value.timeframe), kind,
    routes: strings(value.routes).length ? strings(value.routes) : [text(value.route)],
    timeframes: strings(value.timeframes).length ? strings(value.timeframes) : [text(value.timeframe)],
    label: sanitizeShortLiveText(value.label || value.name || value.selectionKey),
    instanceId: text(value.instanceId || (kind === "instance" ? value.id : "")), artifactId: text(value.artifactId),
    portfolioId: text(value.portfolioId || (kind === "portfolio" ? value.id : "")),
    artifactFileSha256: text(value.artifactFileSha256), instanceFileSha256: text(value.instanceFileSha256),
    portfolioFileSha256: text(value.portfolioFileSha256 || (kind === "portfolio" ? value.artifactFileSha256 : "")),
    strategyCount: numeric(value.strategyCount ?? value.memberCount) ?? 1,
    distinctPluginCount: numeric(value.distinctPluginCount) ?? (Array.isArray(value.pluginIds) ? new Set(value.pluginIds).size : null),
    positionSizePercents: [...new Set((Array.isArray(value.positionSizePercents) ? value.positionSizePercents : [])
      .filter((percent) => typeof percent === "number" && Number.isFinite(percent) && percent >= 0 && percent <= 100))].sort((a, b) => a - b),
    sourcePositionSizeWasDefaulted: value.sourcePositionSizeWasDefaulted === true,
    functionalSizingProfiles: strings(value.functionalSizingProfiles),
    symbols: strings(value.symbols).map(sanitizeShortLiveText),
    supported: value.supported === true || eligible, available: value.available === true || eligible,
    blockers: Array.isArray(value.blockers) ? value.blockers : [],
    evidenceMeaning: sanitizeShortLiveText(value.evidenceMeaning || "기능시험 후보 · 수익성·장기 운용 자격을 의미하지 않음"),
  };
}

export function exactShortLiveTarget(candidate) {
  const row = normalizeShortLiveCandidate(candidate);
  if (!row.selectionKey || !row.routes.length || row.routes.some((route) => !routeIds.has(route))
    || !row.timeframes.length || row.timeframes.some((timeframe) => !SHORT_LIVE_TIMEFRAMES.includes(timeframe)) || !row.kind) {
    throw new Error("선택한 시험의 경로·시간봉·유형 정보가 완전하지 않습니다.");
  }
  if (row.kind === "instance" && (!row.instanceId
    || !hashes.test(row.artifactFileSha256) || !hashes.test(row.instanceFileSha256))) {
    throw new Error("전략·인스턴스 원본 SHA-256을 확인해야 합니다.");
  }
  if (row.kind === "portfolio" && (!row.portfolioId || !hashes.test(row.portfolioFileSha256))) {
    throw new Error("포트폴리오 원본 SHA-256을 확인해야 합니다.");
  }
  return {
    selectionKey: row.selectionKey, route: row.route, timeframe: row.timeframe, kind: row.kind,
    instanceId: row.instanceId, artifactId: row.artifactId, portfolioId: row.portfolioId,
    artifactFileSha256: row.artifactFileSha256, instanceFileSha256: row.instanceFileSha256,
    portfolioFileSha256: row.portfolioFileSha256,
  };
}

export function normalizeShortLiveSnapshot(value = {}) {
  const source = value.snapshot && typeof value.snapshot === "object" ? value.snapshot : value;
  const preparation = source.prepared && typeof source.prepared === "object" ? source.prepared : null;
  const run = source.runtime && typeof source.runtime === "object" ? source.runtime : null;
  return {
    schemaVersion: text(source.schemaVersion), status: text(source.status || source.mode) || "UNKNOWN",
    observedAt: text(source.observedAt || source.observedAtUtc), fixture: source.fixture === true,
    catalog: (Array.isArray(source.catalog) ? source.catalog : Array.isArray(source.catalog?.candidates)
      ? source.catalog.candidates : Array.isArray(source.candidates) ? source.candidates : []).map(normalizeShortLiveCandidate),
    blockers: Array.isArray(source.blockers) ? source.blockers : [],
    prepared: preparation ? {
      preparationId: text(preparation.preparationId), scopeHash: text(preparation.scopeHash),
      readyToStart: preparation.readyToStart === true,
      blockers: Array.isArray(preparation.blockers) ? preparation.blockers : [],
      targets: Array.isArray(preparation.targets) ? preparation.targets : Array.isArray(preparation.selections) ? preparation.selections : [],
      durationHours: numeric(preparation.durationHours),
      budgetKrwPerBroker: numeric(preparation.budgetKrwPerBroker ?? preparation.capitalKrwPerBroker),
      lossKrwPerBroker: numeric(preparation.lossKrwPerBroker ?? preparation.dailyLossStopKrwPerBroker),
      expiresAt: text(preparation.expiresAt),
    } : null,
    runtime: run ? {
      sessionId: text(run.sessionId), status: text(run.status) || "UNKNOWN", startedAt: text(run.startedAt), endsAt: text(run.endsAt),
      terminalVerified: run.terminalVerified === true,
      ownerAlive: typeof run.ownerAlive === "boolean" ? run.ownerAlive : null,
      naturalExitObserved: run.naturalExitObserved === true,
      newEntriesBlocked: typeof run.newEntriesBlocked === "boolean" ? run.newEntriesBlocked : null,
      unknownOrderCount: numeric(run.unknownOrderCount),
      blockers: Array.isArray(run.blockers) ? run.blockers : [],
      brokers: (Array.isArray(run.brokers) ? run.brokers : Array.isArray(run.brokerRows) ? run.brokerRows : []).map((row) => ({
        broker: sanitizeShortLiveText(row.broker || row.route), budgetKrw: numeric(row.budgetKrw ?? row.routeBudgetKrw),
        exposureKrw: numeric(row.exposureKrw), dailyRealizedLossKrw: numeric(row.dailyRealizedLossKrw ?? row.realizedLossKrw),
        unrealizedLossKrw: numeric(row.unrealizedLossKrw),
        lossLimitKrw: numeric(row.lossLimitKrw ?? row.dailyLossStopKrw),
        status: sanitizeShortLiveText(row.status) || (row.ready === true ? "대조 확인" : "미확인"),
      })),
      strategies: (Array.isArray(run.strategies) ? run.strategies : Array.isArray(run.strategyRows) ? run.strategyRows : []).map((row) => ({
        selectionKey: text(row.selectionKey || row.instanceId), label: sanitizeShortLiveText(row.label || row.instanceId), timeframe: text(row.timeframe),
        signal: sanitizeShortLiveText(row.signal || row.lastSignal), signalCount: numeric(row.signalCount),
        decisionCount: numeric(row.decisionCount), orderCount: numeric(row.orderCount),
        fillCount: numeric(row.fillCount), status: sanitizeShortLiveText(row.status),
        lastObservedAt: text(row.lastObservedAt),
      })),
    } : null,
  };
}

export function buildShortLivePrepare(catalog, selectedKeys, limits = SHORT_LIVE_DEFAULTS) {
  if (!Array.isArray(selectedKeys) || !selectedKeys.length || new Set(selectedKeys).size !== selectedKeys.length) {
    throw new Error("시험 대상을 한 개 이상 중복 없이 선택해 주세요.");
  }
  const durationHours = Number(limits.durationHours);
  if (!Number.isInteger(durationHours) || durationHours < 5 || durationHours > 7) throw new Error("시험 시간은 5~7시간이어야 합니다.");
  if (Number(limits.budgetKrwPerBroker) !== 100000 || Number(limits.lossKrwPerBroker) !== 50000) {
    throw new Error("거래소별 자본 10만원·일손실 5만원 상한을 유지해야 합니다.");
  }
  const targets = selectedKeys.map((key) => {
    const matches = catalog.filter((item) => item.selectionKey === key);
    if (matches.length !== 1) throw new Error("선택한 전략 원본을 정확히 하나로 식별하지 못했습니다.");
    return exactShortLiveTarget(matches[0]);
  });
  return { selections: targets, durationHours, capitalKrwPerBroker: 100000, dailyLossStopKrwPerBroker: 50000,
    futuresPreparation: { leverage: 1, marginMode: "ISOLATED", applySettings: false } };
}

export function shortLiveCoverage(catalog = []) {
  return SHORT_LIVE_ROUTES.flatMap((route) => SHORT_LIVE_TIMEFRAMES.flatMap((timeframe) => ["instance", "portfolio"].map((kind) => {
    const candidates = catalog.filter((item) => item.routes.includes(route.id) && item.timeframes.includes(timeframe) && item.kind === kind);
    return { route: route.id, routeLabel: route.label, timeframe, kind, count: candidates.length,
      availableCount: candidates.filter((item) => item.supported && item.available).length,
      blockers: candidates.length ? candidates.flatMap((item) => item.blockers) : ["candidate-missing"] };
  })));
}

export function shortLiveRunning(snapshot) { return active.has(snapshot?.runtime?.status); }
export function shortLivePreparationMatches(prepared, catalog, selectedKeys, durationHours) {
  if (!prepared || prepared.durationHours !== durationHours || prepared.budgetKrwPerBroker !== 100000
    || prepared.lossKrwPerBroker !== 50000 || !Array.isArray(prepared.targets)
    || prepared.targets.length !== selectedKeys.length || !selectedKeys.length) return false;
  const targets = prepared.targets.map(normalizeShortLiveCandidate);
  return selectedKeys.every((key) => {
    const current = catalog.filter((row) => row.selectionKey === key);
    const bound = targets.filter((row) => row.selectionKey === key);
    if (current.length !== 1 || bound.length !== 1) return false;
    try { return JSON.stringify(exactShortLiveTarget(current[0])) === JSON.stringify(exactShortLiveTarget(bound[0])); }
    catch { return false; }
  });
}
export function shortLiveSelectionSignature(catalog, selectedKeys, durationHours) {
  return JSON.stringify({ durationHours, selections: [...selectedKeys].sort().map((key) => {
    const matches = catalog.filter((row) => row.selectionKey === key);
    if (matches.length !== 1) return { selectionKey: key, invalid: true };
    const row = normalizeShortLiveCandidate(matches[0]);
    return { selectionKey: key, route: row.route, timeframe: row.timeframe, kind: row.kind,
      instanceId: row.instanceId, artifactId: row.artifactId, portfolioId: row.portfolioId,
      artifactFileSha256: row.artifactFileSha256, instanceFileSha256: row.instanceFileSha256,
      portfolioFileSha256: row.portfolioFileSha256 };
  }) });
}
export function shortLiveCanStart(snapshot, { fresh = false, acknowledged = false, confirmationText = "", selectionUnchanged = false, preparedAuthority = "", preparedScopeMatches = false } = {}) {
  const ready = snapshot?.prepared;
  return fresh === true && acknowledged === true && selectionUnchanged === true
    && confirmationText === SHORT_LIVE_CONFIRMATION && ready?.readyToStart === true
    && Boolean(ready.preparationId) && hashes.test(ready.scopeHash)
    && preparedAuthority === `${ready.preparationId}:${ready.scopeHash}`
    && preparedScopeMatches === true
    && ready.blockers.length === 0 && snapshot.blockers.length === 0 && !shortLiveRunning(snapshot);
}

export function shortLiveRuntimeMeaning(row) {
  if (row.timeframe === "1d" && (row.signalCount === null || row.signalCount === 0)) {
    return "일봉 신호 미관측 · 짧은 시험 중 신호가 없을 수 있으며 통과·실패를 뜻하지 않습니다.";
  }
  if (!row.orderCount && !row.fillCount) return "주문·체결 미관측 · 신호와 주문 조건을 확인합니다.";
  return "관측된 신호·주문·체결이며 수익성 검증 결과가 아닙니다.";
}
