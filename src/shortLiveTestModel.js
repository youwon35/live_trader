import { normalizeUpbitQuoteObservation } from './quoteObservation.js';
export const SHORT_LIVE_ROUTES = Object.freeze([
  { id: "KIS_KR_CASH", label: "한국투자증권 국내 현금" },
  { id: "BINANCE_SPOT", label: "Binance 현물" },
  { id: "BINANCE_USDM_FUTURES", label: "Binance USDT 선물" },
  { id: "UPBIT_KRW_SPOT", label: "Upbit 원화 현물" },
]);
export const SHORT_LIVE_TIMEFRAMES = Object.freeze(["5m", "1h", "1d"]);
export const SHORT_LIVE_CONFIRMATION = "실제 주문 시험 시작";
export const SHORT_LIVE_BOUNDED_PROFILE = "BOUNDED_TEST_SLEEVE_100_PERCENT_V1";
export const SHORT_LIVE_DEFAULTS = Object.freeze({
  durationHours: 6, budgetKrwPerBroker: 100000, lossKrwPerBroker: 50000,
});
const hashes = /^[0-9a-f]{64}$/i;
const routeIds = new Set(SHORT_LIVE_ROUTES.map((route) => route.id));
const strings = (value) => Array.isArray(value) ? value.filter((item) => typeof item === "string") : [];
const text = (value) => typeof value === "string" ? value : "";
const numeric = (value) => typeof value === "number" && Number.isFinite(value) ? value : null;
const amount = (value) => (typeof value === "number" || (typeof value === "string" && /^\d+(?:\.\d+)?$/.test(value))) ? numeric(Number(value)) : null;

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
  const brokerGroup = Object.entries({ BINANCE: "Binance 현물·선물 합산", UPBIT: "Upbit", KIS: "한국투자증권" })
    .find(([group]) => code.startsWith(`${group}:`));
  if (brokerGroup) return `${brokerGroup[1]} · ${shortLiveBlockerLabel(code.slice(brokerGroup[0].length + 1))}`;
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
    FUTURES_PROTECTION_POLICY_REQUIRED: "선물 기대 레버리지와 보호 손절 가격 거리를 직접 입력해야 함",
    FUTURES_PROTECTION_POLICY_INVALID: "선물 보호 손절 정책을 확인하지 못함",
    FUTURES_LEVERAGE_INTEGER_REQUIRED: "선물 레버리지는 양의 정수로 입력해야 하며 실제 종목·계좌 허용 범위는 준비 시 확인함",
    FUTURES_LEVERAGE_INVALID: "선물 레버리지는 양의 정수로 입력해야 하며 실제 종목·계좌 허용 범위는 준비 시 확인함",
    FUTURES_SHARED_POLICY_LEVERAGE_EXCEEDED: "레버리지 정수 표현 범위를 초과함 · 실제 종목·계좌·명목 구간의 허용 배율은 서버가 별도 확인함",
    FUTURES_LEVERAGE_EXCEEDS_LIMIT: "선물 레버리지가 지원 상한을 초과함",
    FUTURES_ISOLATED_MARGIN_REQUIRED: "선물 계좌의 격리 방식을 확인해야 함 · 자동 변경하지 않음",
    FUTURES_STOP_DISTANCE_EXCEEDS_MARGIN: "보호 손절 가격 거리가 레버리지에 따른 증거금 범위를 벗어남",
    FUTURES_SETTINGS_CHANGED: "준비 이후 선물 레버리지·증거금·포지션 설정 변경 · 신규 진입 차단",
    FUTURES_SETTINGS_POLICY_MISMATCH: "실제 선물 계좌 설정과 준비한 레버리지·격리 정책이 다름",
    FUTURES_PROTECTION_UNVERIFIED: "거래소 보호 손절 주문을 확인하지 못함 · 신규 진입 차단",
    FUTURES_PROTECTION_REJECTED: "거래소가 보호 손절 주문을 거절함 · 신규 진입 차단",
    FUTURES_PROTECTION_TRIGGERED: "거래소 보호 손절이 발동함 · 잔여 주문과 실제 포지션 대조 중",
    FUTURES_LIQUIDATION_DETECTED: "거래소 강제청산이 확인됨 · 신규 진입 중단 및 실제 체결 대조 필요",
    FUTURES_ADL_DETECTED: "거래소 자동 디레버리징이 확인됨 · 신규 진입 중단 및 실제 체결 대조 필요",
    FUTURES_FORCE_ORDER_UNKNOWN: "강제청산·자동 디레버리징 여부를 확인하지 못함 · 신규 진입 차단",
    FUTURES_POSITION_MODE_UNKNOWN: "선물 포지션 모드를 확인하지 못함",
    FUTURES_SINGLE_ASSET_MODE_REQUIRED: "현재 선물 계좌를 단일 자산 증거금 모드로 확인해야 함 · 자동 변경하지 않음",
    FUTURES_CONFIG_UNKNOWN: "선물 계좌의 현재 레버리지·격리 설정을 확인하지 못함",
    FUTURES_CONDITIONAL_ORDER_PRESENT_OR_UNKNOWN: "선물 조건주문이 남아 있거나 조건주문 조회가 미확정임",
    FUTURES_SETTINGS_CHANGED_SINCE_PREPARATION: "준비 이후 선물 계좌 설정 변경 · 다시 준비 필요",
    FUTURES_SETTINGS_DIFFER_FROM_PREPARED_POLICY: "실제 레버리지·격리 설정이 준비한 정책과 다름 · 신규 진입 차단",
    FUTURES_SETTINGS_CHANGED_DURING_SESSION: "실행 중 선물 계좌 설정 변경 · 신규 진입 중단 및 기존 포지션 대조",
    FUTURES_INCIDENT_NEW_ENTRIES_BLOCKED: "선물 보호·청산 사건 발생 · 신규 진입 중단",
    FUTURES_PROTECTION_TRANSPORT_UNAVAILABLE: "거래소 보호 손절 주문 연결을 확인하지 못함",
    FUTURES_PROTECTION_NUMBER_UNKNOWN: "보호 주문 가격·수량 계산에 필요한 실제 값을 확인하지 못함",
    FUTURES_STOP_ALREADY_TRIGGERED_OR_INVALID: "현재 가격이 보호 손절 범위를 벗어났거나 발동 가격이 유효하지 않음",
    FUTURES_SYMBOL_NOTIONAL_LIMIT_EXCEEDED: "레버리지를 반영한 명목 노출이 거래소 종목 한도를 초과함",
    FUTURES_MAINTENANCE_BRACKETS_UNKNOWN: "거래소 유지증거금 구간을 확인하지 못함",
    FUTURES_NOTIONAL_BRACKET_UNKNOWN: "계산된 명목 노출에 해당하는 증거금 구간을 확인하지 못함",
    FUTURES_LEVERAGE_BRACKET_EXCEEDED: "설정한 레버리지가 해당 명목 노출 구간의 거래소 상한을 초과함",
    FUTURES_STOP_AT_OR_BEYOND_MAINTENANCE_BUFFER: "보호 손절이 유지증거금 안전 여유 안에서 발동하도록 설정되어야 함",
    FUTURES_PROTECTION_ACK_UNKNOWN: "보호 손절 주문의 거래소 접수 결과 미확정 · 신규 진입 차단",
    FUTURES_PROTECTION_ID_UNKNOWN: "접수된 보호 손절 주문을 정확히 식별하지 못함",
    FUTURES_PROTECTION_OBSERVATION_STALE: "보호 손절 주문의 최신 상태를 확인하지 못함",
    FUTURES_PROTECTION_NOT_ACTIVE: "거래소 보호 손절 주문이 유효한 대기 상태가 아님 · 신규 진입 차단",
    FUTURES_EXISTING_STOP_ALREADY_TRIGGERED: "기존 보호 손절의 발동 가격에 도달함 · 신규 진입 차단",
    FUTURES_PROTECTION_RESERVATION_REQUIRED: "보호 손절 주문의 실행 기록을 확정하지 못함",
    FUTURES_PROTECTION_CANCEL_RESERVATION_REQUIRED: "보호 손절 취소의 실행 기록을 확정하지 못함",
    FUTURES_PROTECTION_CANCEL_UNKNOWN: "보호 손절 취소 결과 미확정 · 종료 대조 계속 필요",
    FUTURES_PROTECTION_RETIREMENT_UNKNOWN: "보호 손절 주문과 발동한 실제 주문의 종료를 확인하지 못함",
    FUTURES_AUTO_ADD_MARGIN_MUST_BE_DISABLED: "선물 자동 증거금 추가를 꺼야 함 · 계좌 자금 추가 투입 방지",
    FUTURES_AUTO_ADD_MARGIN_ENABLED: "실행 중 선물 자동 증거금 추가가 켜짐 · 신규 진입 중단",
    FUTURES_AUTO_ADD_MARGIN_UNKNOWN: "선물 자동 증거금 추가 설정을 확인할 수 없음 · 신규 진입 차단",
    FUTURES_PROTECTIVE_EXECUTION_UNKNOWN: "발동한 보호 손절의 실제 주문·체결을 확인하지 못함",
    FUTURES_PROTECTIVE_EXECUTION_IDENTITY_CHANGED: "보호 손절의 실제 체결 주문 식별 정보가 일치하지 않음",
    FUTURES_PROTECTIVE_STOP_TRIGGERED: "거래소 보호 손절 발동 · 신규 진입 중단 및 실제 체결 대조",
    FUTURES_POSITION_UNPROTECTED: "실제 선물 포지션의 유효한 보호 손절 주문을 확인하지 못함",
    FUTURES_LIQUIDATION_PRICE_UNKNOWN: "거래소가 보고한 실제 청산 가격을 확인하지 못함",
    FUTURES_ACTUAL_LIQUIDATION_BUFFER_BREACHED: "실제 청산 가격까지의 안전 여유 부족 · 신규 진입 중단",
    FUTURES_STOP_TRIGGER_PENDING: "보호 손절 가격 도달 · 거래소 발동·체결 확인 대기",
    FUTURES_FORCE_ORDER_HISTORY_UNKNOWN: "거래소 강제청산·자동 디레버리징 주문 이력을 확인하지 못함",
    FUTURES_FORCE_EXECUTION_IDENTITY_CHANGED: "강제청산·자동 디레버리징의 체결 식별 정보가 일치하지 않음",
    FUTURES_EXTERNAL_EXECUTION_OR_FEE_UNKNOWN: "보호·청산에 따른 실제 체결 또는 수수료 미확정",
    FUTURES_EXTERNAL_EXECUTION_IDENTITY_UNKNOWN: "보호·청산에 따른 체결을 정확한 주문과 연결하지 못함",
    FUTURES_EXTERNAL_CUMULATIVE_FILL_REGRESSED: "거래소 누적 체결 수량이 이전 확인보다 감소함 · 대조 필요",
    FUTURES_EXTERNAL_SLEEVE_ATTRIBUTION_UNKNOWN: "보호·청산 체결을 해당 전략의 보유 수량에 배분하지 못함",
    FUTURES_ALGO_ORDERS_UNKNOWN: "거래소 선물 조건주문 전체 조회를 확인하지 못함",
    UNOWNED_FUTURES_CONDITIONAL_ORDER: "이 시험의 소유로 확인되지 않은 선물 조건주문이 있음",
    UNOWNED_FUTURES_POSITION_SIDE: "이 시험이 관리하는 방향과 다른 선물 포지션이 있음",
    BINANCE_SHARED_BUDGET_ACCOUNT_MISMATCH: "Binance 현물·선물을 같은 거래소 예산으로 묶을 계좌 정보를 확인하지 못함",
    EXCHANGE_SHARED_BUDGET_EXCEEDED: "거래소별 합산 자본 10만원 한도를 초과함",
    DAILY_LOSS_STOP_REACHED: "이번 시험의 손실 중단 기준에 도달함",
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
    GLOBAL_MONITOR_STOP_REQUESTED: "운영자가 중지를 요청함 · 새 준비 후 직접 재시작 필요",
    OTHER_RUNTIME_ACTIVE: "다른 실행이 진행 중이어서 동시 시험을 시작할 수 없음",
    OTHER_FUNCTIONAL_AUTHORITY_ACTIVE: "다른 주문 기능시험이 같은 주문 경로를 사용 중",
    GENUINE_UI_OWNER_UNAVAILABLE: "정상 Live Trader 앱의 실행 소유자를 확인하지 못함",
    PREPARATION_EXPIRED: "준비 결과의 유효 시간이 지남 · 다시 무주문 준비 필요",
    PREPARATION_REQUIRED: "시작 전에 선택한 범위로 무주문 준비 필요",
    PREPARATION_SCOPE_MISMATCH: "준비 때 확인한 범위와 시작 요청이 다름",
    IMMUTABLE_FUNCTIONAL_CONTRACT_MISMATCH: "준비 결과의 버전·범위가 현재 실행 계약과 다름 · 다시 무주문 준비 필요",
    IMMUTABLE_FUTURES_POLICY_MISMATCH: "확정한 선물 보호 정책이 준비 결과와 다름 · 다시 무주문 준비 필요",
    PROTECTION_SESSION_UNAVAILABLE: "보호 주문을 연결할 현재 시험 세션을 확인하지 못함",
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
    PRESERVED_ASSET_BASELINE_CHANGED: "준비 이후 보호할 기존 보유 자산이 변경되거나 확인되지 않음 · 다시 무주문 준비 필요",
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
    FIRST_FAILURE_JOURNAL_WRITE_FAILED: "최초 오류 기록을 파일에 저장하지 못함 · 신규 진입 차단 유지",
    NATIVE_FX_TIME_INVALID: "환율 자료의 시각 형식을 확인하지 못함",
    NATIVE_FX_EXACT_USDT_MARKET_REQUIRED: "환율 환산에 필요한 정확한 KRW-USDT 응답이 아님",
    NATIVE_FX_EXACT_USDT_TRADE_TIME_REQUIRED: "환율 원본의 실제 체결 시각을 확인하지 못함",
    NATIVE_FX_CACHED_HTTP_RESPONSE_REJECTED: "저장된 과거 환율 응답이 반환됨 · 최신 응답 필요",
    NATIVE_FX_HTTP_CLOCK_RTT_INVALID: "환율 응답의 왕복 조회 시간을 확인하지 못함",
    NATIVE_FX_HTTP_CLOCK_SKEW_OR_RTT_EXCEEDED: "환율 응답의 시각 오차 또는 조회 지연이 허용 범위를 넘음",
    NATIVE_FX_OFFICIAL_HTTP_DATE_MISSING_OR_INVALID: "환율 응답의 공식 서버 시각이 없거나 유효하지 않음",
    NATIVE_FX_OFFICIAL_CURRENCY_ROUTE_UNAVAILABLE: "이 통화의 공식 원화 환산 경로를 확인하지 못함",
    NATIVE_FX_OBSERVATION_MISSING: "원화 환산에 필요한 공식 환율 관측이 없음",
    NATIVE_FX_OBSERVATION_STALE_OR_FUTURE: "환율 관측이 오래됐거나 미래 시각임 · 최신 관측 필요",
    NATIVE_FX_HTTP_CLOCK_PROOF_MISSING: "환율 응답과 서버 시각 검증 자료가 일치하지 않음",
    NATIVE_FX_HTTP_CLOCK_HEADER_BINDING_CHANGED: "환율 응답과 서버 시각 검증 자료가 일치하지 않음",
    NATIVE_FX_HTTP_CLOCK_LOCAL_BINDING_CHANGED: "환율 응답과 서버 시각 검증 자료가 일치하지 않음",
    NATIVE_FX_OBSERVATION_SEAL_INVALID: "환율 관측과 공식 원본의 통화·값·시각이 일치하지 않음",
    NATIVE_FX_OBSERVATION_CURRENCY_OR_ROUTE_MISMATCH: "환율 관측과 공식 원본의 통화·값·시각이 일치하지 않음",
    NATIVE_FX_OFFICIAL_RESPONSE_BINDING_CHANGED: "환율 관측과 공식 원본의 통화·값·시각이 일치하지 않음",
    NATIVE_FX_OFFICIAL_RESPONSE_SCOPE_CHANGED: "환율 관측과 공식 원본의 통화·값·시각이 일치하지 않음",
    NATIVE_FX_RATE_RESPONSE_CHANGED: "환율 관측과 공식 원본의 통화·값·시각이 일치하지 않음",
    NATIVE_FX_TIME_RESPONSE_CHANGED: "환율 관측과 공식 원본의 통화·값·시각이 일치하지 않음",
    FX_OBSERVATION_CONFLICT: "계좌와 주문 시세의 환율 관측이 일치하지 않음",
    OBSERVATION_STALE: "시세 또는 계좌 조회가 오래되어 현재 주문 조건 재확인 필요",
    ACCOUNT_OBSERVATION_STALE: "계좌 조회가 오래됐습니다 · 계좌·시세 새로 확인을 눌러 다시 검사해 주세요",
    MARKET_OBSERVATION_STALE: "주문 시세 조회가 오래됐습니다 · 계좌·시세 새로 확인이 필요합니다",
    SOURCE_QUOTE_OBSERVATION_STALE: "거래소가 제공한 시세 시각이 오래됐습니다 · 최신 시세 수신 후 다시 검사해 주세요",
    FX_OBSERVATION_STALE: "원화 환산 시세가 오래됐습니다 · 계좌·시세 새로 확인이 필요합니다",
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

export function normalizeShortLiveFuturesPolicy(value) {
  if (!value || typeof value !== "object" || value.marginMode !== "ISOLATED") return null;
  const constants = { orderType: "STOP_MARKET", workingType: "MARK_PRICE", closePosition: true, triggerBasis: "ENTRY_LIMIT_PRICE" };
  if (Object.entries(constants).some(([key, expected]) => key in value && value[key] !== expected)
    || Object.keys(value).some((key) => !["leverage", "marginMode", "stopLossPercent", ...Object.keys(constants)].includes(key))) return null;
  const leverage = amount(value.leverage), stopLossPercent = amount(value.stopLossPercent);
  if (!Number.isSafeInteger(leverage) || leverage < 1
    || stopLossPercent === null || stopLossPercent <= 0 || stopLossPercent >= 100 / leverage) return null;
  return { leverage, marginMode: "ISOLATED", stopLossPercent };
}

const futuresSelected = (catalog, keys) => catalog.some((row) => keys.includes(row.selectionKey)
  && normalizeShortLiveCandidate(row).routes.includes("BINANCE_USDM_FUTURES"));

const normalizeFuturesSettings = (row) => ({
  marginMode: sanitizeShortLiveText(row?.marginMode), leverage: amount(row?.leverage),
  positionMode: sanitizeShortLiveText(row?.positionMode),
});
const normalizeFuturesSettingsBySymbol = (value) => Object.fromEntries(
  Object.entries(value && typeof value === "object" && !Array.isArray(value) ? value : {})
    .filter(([symbol, row]) => /^[A-Z0-9_.-]{2,40}$/.test(symbol) && row && typeof row === "object")
    .map(([symbol, row]) => [symbol, normalizeFuturesSettings(row)]));

const normalizeFuturesProtection = (row = {}) => ({
  symbol: sanitizeShortLiveText(row.symbol), positionSide: sanitizeShortLiveText(row.positionSide),
  status: sanitizeShortLiveText(row.status || row.algoStatus) || "UNKNOWN",
  triggerPrice: amount(row.triggerPrice), actualLiquidationPrice: amount(row.actualLiquidationPrice),
  markPrice: amount(row.markPrice), leverage: amount(row.leverage),
  verified: row.verified === true, observedAt: text(row.observedAt),
});
const normalizeFuturesIncident = (row = {}) => ({
  symbol: sanitizeShortLiveText(row.symbol), positionSide: sanitizeShortLiveText(row.positionSide),
  type: sanitizeShortLiveText(row.type || row.autoCloseType || row.kind || row.code) || "UNKNOWN",
  observedAt: text(row.observedAt), quantity: amount(row.quantity ?? row.executedQty),
  verified: row.verified === true,
});

export function shortLiveProtectionLabel(status) {
  return ({ NEW: "보호 주문 대기", WORKING: "보호 주문 대기", VERIFIED: "보호 확인",
    TRIGGERED: "보호 발동", FINISHED: "보호 주문 종료", FILLED: "보호 체결 확인",
    CANCELED: "보호 취소 확인", REJECTED: "보호 주문 거절", EXPIRED: "보호 주문 만료",
    UNKNOWN: "보호 상태 미확인" })[status] || "보호 상태 확인 필요";
}

export function shortLiveFuturesIncidentLabel(type) {
  if (typeof type === "string" && type.startsWith("FUTURES_")) return shortLiveBlockerLabel(type);
  return ({ LIQUIDATION: "거래소 강제청산", ADL: "자동 디레버리징",
    PROTECTIVE_STOP: "보호 손절 발동", PROTECTION_TRIGGERED: "보호 손절 발동" })[type] || "선물 상태 불일치";
}

export function normalizeShortLiveFailureDiagnostics(value) {
  // Keep this allowlist aligned with short_live_read_diagnostics.py. No URLs, queries or raw responses.
  const paths = {
    "binance": [
        "/api/v3/account",
        "/api/v3/account/commission",
        "/api/v3/exchangeInfo",
        "/api/v3/myTrades",
        "/api/v3/openOrders",
        "/api/v3/order",
        "/api/v3/ticker/price",
        "/api/v3/time"
    ],
    "binance-futures": [
        "/fapi/v1/accountConfig",
        "/fapi/v1/algoOrder",
        "/fapi/v1/commissionRate",
        "/fapi/v1/exchangeInfo",
        "/fapi/v1/forceOrders",
        "/fapi/v1/leverageBracket",
        "/fapi/v1/openAlgoOrders",
        "/fapi/v1/openOrders",
        "/fapi/v1/order",
        "/fapi/v1/positionSide/dual",
        "/fapi/v1/premiumIndex",
        "/fapi/v1/symbolConfig",
        "/fapi/v1/time",
        "/fapi/v1/userTrades",
        "/fapi/v3/account",
        "/fapi/v3/positionRisk"
    ],
    "upbit": [
        "/v1/accounts",
        "/v1/order",
        "/v1/orderbook/instruments",
        "/v1/orders/chance",
        "/v1/orders/open",
        "/v1/ticker"
    ],
    "kis": [
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"
    ]
};
  const statuses = new Set(["AVAILABLE", "BLOCKED", "REJECTED", "UNAVAILABLE"]);
  const kinds = new Set(["CONNECTION_ERROR", "HTTP_ERROR", "INVALID_RESPONSE", "READ_BOUNDARY", "READ_REJECTED", "READ_UNAVAILABLE", "TIMEOUT", "UNEXPECTED_ERROR"]);
  return (Array.isArray(value) ? value : []).slice(-32).filter(row => row && typeof row === "object"
    && Object.hasOwn(paths, row.provider) && paths[row.provider].includes(row.path) && statuses.has(row.status)
    && typeof row.elapsedMs === "number" && Number.isFinite(row.elapsedMs) && row.elapsedMs >= 0).map(row => ({
    provider: row.provider, path: row.path, elapsedMs: row.elapsedMs, status: row.status,
    quoteObservation: row.provider === 'upbit' && row.path === '/v1/ticker' ? normalizeUpbitQuoteObservation(row.quoteObservation) : null,
    failureKind: kinds.has(row.failureKind) ? row.failureKind : "",
    httpStatus: Number.isInteger(row.httpStatus) && row.httpStatus >= 100 && row.httpStatus <= 599 ? row.httpStatus : null,
    brokerCode: Number.isInteger(row.brokerCode) && row.brokerCode >= -2147483648 && row.brokerCode < 0 ? row.brokerCode : null,
  }));
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
      futuresPolicy: normalizeShortLiveFuturesPolicy(preparation.futuresPolicy),
      executionModelBinding: preparation.executionModelBinding && typeof preparation.executionModelBinding === "object" ? preparation.executionModelBinding : null,
      expiresAt: text(preparation.expiresAt),
      brokerReadiness: (Array.isArray(preparation.brokerReadiness) ? preparation.brokerReadiness : []).map((row) => ({
        route: text(row.route), ready: row.ready === true, blockers: strings(row.blockers),
        checkedAt: text(row.checkedAt),
        fundingBasis: text(row.fundingBasis), quoteCurrency: text(row.quoteCurrency),
        availableQuoteNative: amount(row.availableQuoteNative), availableKrw: amount(row.availableKrw),
        effectiveRouteBudgetKrw: amount(row.effectiveRouteBudgetKrw),
        executionAccounting: row.executionAccounting && typeof row.executionAccounting === "object" ? row.executionAccounting : null,
        preservedAssetSymbols: strings(row.preservedAssetSymbols).map(sanitizeShortLiveText),
        observedFuturesSettings: row.observedFuturesSettings ? normalizeFuturesSettings(row.observedFuturesSettings) : null,
        observedFuturesSettingsBySymbol: normalizeFuturesSettingsBySymbol(row.observedFuturesSettingsBySymbol),
      })),
    } : null,
    runtime: run ? {
      sessionId: text(run.sessionId), status: text(run.status) || "UNKNOWN", startedAt: text(run.startedAt), endsAt: text(run.endsAt), terminatedAt: text(run.terminatedAt),
      terminalVerified: run.terminalVerified === true,
      ownerAlive: typeof run.ownerAlive === "boolean" ? run.ownerAlive : null,
      workerAlive: typeof run.workerAlive === "boolean" ? run.workerAlive : null,
      naturalExitObserved: run.naturalExitObserved === true,
      workerRetirementRequired: typeof run.workerRetirementRequired === "boolean" ? run.workerRetirementRequired : null,
      workerLaunchState: text(run.workerLaunchState),
      newEntriesBlocked: typeof run.newEntriesBlocked === "boolean" ? run.newEntriesBlocked : null,
      unknownOrderCount: numeric(run.unknownOrderCount),
      orderCount: numeric(run.orderCount), fillCount: numeric(run.fillCount),
      equityHistory: (Array.isArray(run.equityHistory) ? run.equityHistory : []).slice(-2880).map(row => ({
        observedAt:text(row.observedAt), route:sanitizeShortLiveText(row.route), basis:text(row.basis),
        baselineNative:amount(row.baselineNative), currentNative:amount(row.currentNative),
        krwPerQuote:amount(row.krwPerQuote), quoteCurrency:sanitizeShortLiveText(row.quoteCurrency),
      })),
      feedObservations: (Array.isArray(run.feedObservations) ? run.feedObservations : []).map(row => ({
        route: sanitizeShortLiveText(row.route),
        streams: (Array.isArray(row.streams) ? row.streams : []).map(stream => ({
          socketPresent: typeof stream.socketPresent === "boolean" ? stream.socketPresent : null,
          lastFrameAt: text(stream.lastFrameAt),
        })),
      })),
      firstFailure: sanitizeShortLiveText(run.firstFailure), stopReason: sanitizeShortLiveText(run.stopReason),
      firstFailureDiagnostics: normalizeShortLiveFailureDiagnostics(run.firstFailureDiagnostics),
      recentActivity: (Array.isArray(run.recentActivity) ? run.recentActivity : []).slice(-60).map((event) => ({
        sequence: numeric(event.sequence), kind: sanitizeShortLiveText(event.kind), observedAt: text(event.observedAt),
        route: sanitizeShortLiveText(event.route), symbol: sanitizeShortLiveText(event.symbol), side: sanitizeShortLiveText(event.side),
        status: sanitizeShortLiveText(event.status), quantity: amount(event.quantity),
        incrementalQuantity: amount(event.incrementalQuantity), cumulativeQuantity: amount(event.cumulativeQuantity),
        cumulativeAveragePrice: amount(event.cumulativeAveragePrice),
      })),
      blockers: Array.isArray(run.blockers) ? run.blockers : [],
      brokers: (Array.isArray(run.brokers) ? run.brokers : Array.isArray(run.brokerRows) ? run.brokerRows : []).map((row) => ({
        broker: sanitizeShortLiveText(row.broker || row.route), budgetKrw: numeric(row.effectiveRouteBudgetKrw ?? row.budgetKrw ?? row.routeBudgetKrw),
        brokerBudgetKrw: numeric(row.brokerBudgetKrw),
        accountObservedAt: text(row.accountObservedAt),
        monitorEquity: row.monitorEquity && typeof row.monitorEquity === "object" ? {
          basis: text(row.monitorEquity.basis), baselineNative: amount(row.monitorEquity.baselineNative),
          currentNative: amount(row.monitorEquity.currentNative), krwPerQuote: amount(row.monitorEquity.krwPerQuote),
          quoteCurrency: sanitizeShortLiveText(row.monitorEquity.quoteCurrency),
          preservedSpotExcluded: row.monitorEquity.preservedSpotExcluded === true,
          cashFlowAdjusted: row.monitorEquity.cashFlowAdjusted === true,
        } : null,
        exposureKrw: numeric(row.exposureKrw), dailyRealizedLossKrw: numeric(row.dailyRealizedLossKrw ?? row.realizedLossKrw),
        committedCapitalKrw: amount(row.committedCapitalKrw),
        executionAccounting: row.executionAccounting && typeof row.executionAccounting === "object" ? row.executionAccounting : null,
        notionalExposureKrw: amount(row.notionalExposureKrw ?? row.exposureKrw),
        protections: (Array.isArray(row.protections) ? row.protections : []).map(normalizeFuturesProtection),
        futuresIncidents: (Array.isArray(row.futuresIncidents) ? row.futuresIncidents : []).map(normalizeFuturesIncident),
        futuresPositions: (Array.isArray(row.futuresPositions) ? row.futuresPositions : []).map((position) => ({
          symbol: sanitizeShortLiveText(position.symbol), positionSide: sanitizeShortLiveText(position.positionSide),
          actualLiquidationPrice: amount(position.actualLiquidationPrice ?? position.liquidationPrice),
          markPrice: amount(position.markPrice), leverage: amount(position.leverage),
        })),
        unrealizedLossKrw: numeric(row.unrealizedLossKrw),
        lossLimitKrw: numeric(row.lossLimitKrw ?? row.dailyLossStopKrw),
        lossBasis: text(row.lossBasis), actualFeeKnown: typeof row.actualFeeKnown === "boolean" ? row.actualFeeKnown : null,
        status: sanitizeShortLiveText(row.status) || (row.ready === true ? "대조 확인" : "미확인"),
      })),
      strategies: (Array.isArray(run.strategies) ? run.strategies : Array.isArray(run.strategyRows) ? run.strategyRows : []).map((row) => ({
        selectionKey: text(row.selectionKey || row.instanceId), label: sanitizeShortLiveText(row.label || row.instanceId), timeframe: text(row.timeframe),
        signal: sanitizeShortLiveText(row.signal || row.lastSignal), signalCount: numeric(row.signalCount),
        decisionCount: numeric(row.decisionCount), orderCount: numeric(row.orderCount),
        fillCount: numeric(row.fillCount), status: sanitizeShortLiveText(row.status),
        orderAttemptCount: numeric(row.orderAttemptCount), fillObservationCount: numeric(row.fillObservationCount),
        internalAllocationTransferCount: numeric(row.internalAllocationTransferCount),
        pluginId: sanitizeShortLiveText(row.pluginId), symbol: sanitizeShortLiveText(row.symbol), route: text(row.route),
        lastObservedAt: text(row.lastObservedAt), portfolioId: sanitizeShortLiveText(row.portfolioId),
        lastPrice: amount(row.lastPrice), lastBarAt: text(row.lastBarAt), lastDecisionAt: text(row.lastDecisionAt),
        averageEntryPrice: amount(row.averageEntryPrice), pendingOrderCount: numeric(row.pendingOrderCount),
        positionQuantity: (typeof row.positionQuantity === "number" || (typeof row.positionQuantity === "string" && /^-?\d+(?:\.\d+)?$/.test(row.positionQuantity))) ? numeric(Number(row.positionQuantity)) : null,
      })),
    } : null,
  };
}

export function mergeShortLiveSnapshot(value, displayCatalog = []) {
  const source = value?.snapshot || value || {};
  const next = normalizeShortLiveSnapshot(value);
  // Only discovery rows persist. Expiry, readiness, blockers and runtime are
  // always replaced by the new status response, never by a catalogue reply.
  return source.catalogIncluded === false ? { ...next, catalog: displayCatalog } : next;
}

export function buildShortLivePrepare(catalog, selectedKeys, limits = SHORT_LIVE_DEFAULTS) {
  if (!Array.isArray(selectedKeys) || !selectedKeys.length || new Set(selectedKeys).size !== selectedKeys.length) {
    throw new Error("시험 대상을 한 개 이상 중복 없이 선택해 주세요.");
  }
  const durationHours = Number(limits.durationHours);
  if (!Number.isInteger(durationHours) || durationHours < 5 || durationHours > 7) throw new Error("시험 시간은 5~7시간이어야 합니다.");
  if (Number(limits.budgetKrwPerBroker) !== 100000 || Number(limits.lossKrwPerBroker) !== 50000) {
    throw new Error("거래소별 자본 10만원·이번 시험 손실 중단 기준 5만원을 유지해야 합니다.");
  }
  const targets = selectedKeys.map((key) => {
    const matches = catalog.filter((item) => item.selectionKey === key);
    if (matches.length !== 1) throw new Error("선택한 전략 원본을 정확히 하나로 식별하지 못했습니다.");
    return exactShortLiveTarget(matches[0]);
  });
  const payload = { selections: targets, durationHours, capitalKrwPerBroker: 100000, dailyLossStopKrwPerBroker: 50000 };
  if (futuresSelected(catalog, selectedKeys)) {
    const policy = normalizeShortLiveFuturesPolicy(limits.futuresPolicy);
    if (!policy) throw new Error("선물 계좌의 실제 레버리지(양의 정수; 종목·계좌 한도 별도 확인)와 보호 손절 가격 거리(0% 초과, 100÷레버리지% 미만)를 입력해 주세요.");
    payload.futuresPolicy = policy;
  }
  return payload;
}

export function shortLiveCoverage(catalog = []) {
  return SHORT_LIVE_ROUTES.flatMap((route) => SHORT_LIVE_TIMEFRAMES.flatMap((timeframe) => ["instance", "portfolio"].map((kind) => {
    const candidates = catalog.filter((item) => item.routes.includes(route.id) && item.timeframes.includes(timeframe) && item.kind === kind);
    return { route: route.id, routeLabel: route.label, timeframe, kind, count: candidates.length,
      availableCount: candidates.filter((item) => item.supported && item.available).length,
      blockers: candidates.length ? candidates.flatMap((item) => item.blockers) : ["candidate-missing"] };
  })));
}

export function shortLiveRunning(snapshot) {
  const runtime = snapshot?.runtime;
  if (!runtime) return false;
  return !(runtime.status === "TERMINATED" && runtime.terminalVerified === true
    && (runtime.naturalExitObserved === true || (runtime.workerRetirementRequired === false && runtime.workerLaunchState === "NOT_ATTEMPTED" && runtime.workerAlive === false)));
}
export function shortLiveRetirementMeaning(runtime) {
  if (!runtime || shortLiveRunning({ runtime })) return "실제 종료 확인 전입니다. 예정 시간이 지났다는 이유로 종료를 인정하지 않습니다.";
  if (runtime.workerRetirementRequired === false && runtime.workerLaunchState === "NOT_ATTEMPTED" && runtime.workerAlive === false) {
    return "작업자 시작 전 종료 확인 · 시세 연결 정리와 잔여 주문·포지션 대조가 확인됐습니다. 실행·거래 성공을 뜻하지 않습니다.";
  }
  if (runtime.unknownOrderCount > 0) return "종료 기록이 있지만 미확정 주문이 남아 있습니다. 주문·체결 대조를 확인하세요. 시험 성공을 뜻하지 않습니다.";
  if (runtime.firstFailure || runtime.blockers?.length) {
    return "오류로 시험이 중단됐으며 작업자 종료와 잔여 주문·포지션 정리가 확인됐습니다. 시험 성공을 뜻하지 않습니다. 최초 오류와 관측 기록을 확인하세요.";
  }
  return "작업자 종료와 잔여 주문·포지션 정리가 확인됐습니다. 실제 주문·체결 여부와 시험 성공은 별도로 확인해야 합니다.";
}

export function shortLivePreparationMatches(prepared, catalog, selectedKeys, durationHours, futuresPolicy = null) {
  if (!prepared || prepared.durationHours !== durationHours || prepared.budgetKrwPerBroker !== 100000
    || prepared.lossKrwPerBroker !== 50000 || !Array.isArray(prepared.targets)
    || prepared.targets.length !== selectedKeys.length || !selectedKeys.length) return false;
  if (futuresSelected(catalog, selectedKeys)) {
    const expected = normalizeShortLiveFuturesPolicy(futuresPolicy);
    const observed = normalizeShortLiveFuturesPolicy(prepared.futuresPolicy);
    if (!expected || !observed || JSON.stringify(expected) !== JSON.stringify(observed)) return false;
  }
  const targets = prepared.targets.map(normalizeShortLiveCandidate);
  return selectedKeys.every((key) => {
    const current = catalog.filter((row) => row.selectionKey === key);
    const bound = targets.filter((row) => row.selectionKey === key);
    if (current.length !== 1 || bound.length !== 1) return false;
    try { return JSON.stringify(exactShortLiveTarget(current[0])) === JSON.stringify(exactShortLiveTarget(bound[0])); }
    catch { return false; }
  });
}
// Restores editable selections only. A new prepare call must bind start authority.
export function shortLivePreviousSelection(prepared, catalog = []) {
  if (!prepared || ![5, 6, 7].includes(prepared.durationHours)) return null;
  const keys = (prepared.targets || []).map((row) => row.selectionKey);
  if (!keys.length || new Set(keys).size !== keys.length || keys.some((key) => !key)) return null;
  if (!shortLivePreparationMatches(prepared, catalog, keys, prepared.durationHours, prepared.futuresPolicy)) return null;
  return { selection: keys, durationHours: prepared.durationHours, futuresPolicy: normalizeShortLiveFuturesPolicy(prepared.futuresPolicy) };
}

export function shortLiveSelectionSignature(catalog, selectedKeys, durationHours, futuresPolicy = null) {
  return JSON.stringify({ durationHours,
    futuresPolicy: futuresSelected(catalog, selectedKeys) ? {
      leverage: String(futuresPolicy?.leverage ?? ""), stopLossPercent: String(futuresPolicy?.stopLossPercent ?? ""),
      marginMode: "ISOLATED",
    } : null,
    selections: [...selectedKeys].sort().map((key) => {
    const matches = catalog.filter((row) => row.selectionKey === key);
    if (matches.length !== 1) return { selectionKey: key, invalid: true };
    const row = normalizeShortLiveCandidate(matches[0]);
    return { selectionKey: key, route: row.route, timeframe: row.timeframe, kind: row.kind,
      instanceId: row.instanceId, artifactId: row.artifactId, portfolioId: row.portfolioId,
      artifactFileSha256: row.artifactFileSha256, instanceFileSha256: row.instanceFileSha256,
      portfolioFileSha256: row.portfolioFileSha256 };
  }) });
}
export function shortLiveIsBoundedCandidate(row) {
  return row.functionalSizingProfiles?.includes(SHORT_LIVE_BOUNDED_PROFILE) === true;
}

export function shortLiveFilterCandidates(catalog, { route = "ALL", timeframe = "ALL", kind = "ALL", profile = "ALL" } = {}) {
  return catalog.filter((row) => (route === "ALL" || row.routes.includes(route))
    && (timeframe === "ALL" || row.timeframes.includes(timeframe)) && (kind === "ALL" || row.kind === kind)
    && (profile === "ALL" || (profile === "BOUNDED" ? shortLiveIsBoundedCandidate(row) : !shortLiveIsBoundedCandidate(row))));
}

export function shortLivePreparationWindow(prepared, now = Date.now()) {
  if (!prepared) return { status: "NONE", remainingSeconds: null };
  const expiry = Date.parse(prepared.expiresAt);
  if (!Number.isFinite(expiry) || !Number.isFinite(now)) return { status: "UNKNOWN", remainingSeconds: null };
  return { status: expiry > now ? "VALID" : "EXPIRED", remainingSeconds: Math.max(0, Math.ceil((expiry - now) / 1000)) };
}

export function shortLiveReadyForConsent(snapshot, { fresh = false, selectionUnchanged = false, preparedAuthority = "", preparedScopeMatches = false, now = Date.now() } = {}) {
  const ready = snapshot?.prepared;
  return fresh === true && selectionUnchanged === true && ready?.readyToStart === true
    && Boolean(ready.preparationId) && hashes.test(ready.scopeHash)
    && preparedAuthority === `${ready.preparationId}:${ready.scopeHash}`
    && preparedScopeMatches === true
    && shortLivePreparationWindow(ready, now).status === "VALID"
    && ready.blockers.length === 0 && snapshot.blockers.length === 0 && !shortLiveRunning(snapshot);
}

export function shortLiveRuntimeMeaning(row, runtime = null) {
  if (runtime?.status === "TERMINATED" && !shortLiveRunning({ runtime }) && row.decisionCount === 0 && !(row.orderAttemptCount > 0) && !(row.orderCount > 0) && !(row.fillObservationCount > 0) && !(row.fillCount > 0)) {
    return "종료 전 완성봉 평가·주문·체결 기록이 없습니다. 현재 신호를 기다리는 실행 상태가 아닙니다.";
  }
  if (row.fillObservationCount > 0) return "체결 관측 기록이 있습니다. 부분 체결·반복 관측 횟수는 고유 주문 수와 다릅니다.";
  if (row.fillCount > 0) return "실제 체결 기록이 있습니다. 수익성 검증 결과를 뜻하지 않습니다.";
  if (row.orderAttemptCount > 0) return "주문 시도가 관측됨 · 실제 체결은 아직 확인되지 않았습니다.";
  if (row.orderCount > 0) return "주문 기록이 있습니다. 실제 체결 여부를 별도로 확인합니다.";
  if (row.timeframe === "1h" && row.decisionCount === 0) {
    return "1시간봉 평가 대기 · 시작 뒤 실제 완성봉 수신과 과거 봉 연결을 확인합니다. 대기는 통과·실패를 뜻하지 않습니다.";
  }
  if (row.timeframe === "1h" && row.decisionCount > 0 && row.signal === "HOLD") {
    return "완성봉 평가됨 · 최근 신호 HOLD. 전략 조건이 충족되지 않으면 주문하지 않습니다.";
  }
  if (row.timeframe === "1d" && (row.signalCount === 0 || row.signal === "NOT_OBSERVED")
      && (!row.signal || row.signal === "NOT_OBSERVED")) {
    return "일봉 신호 미관측 · 짧은 시험 중 신호가 없을 수 있으며 통과·실패를 뜻하지 않습니다.";
  }
  if (row.signal && row.signal !== "NOT_OBSERVED") return "최근 전략 신호가 관측됐습니다. 신호와 실제 주문·체결은 별도로 확인합니다.";
  if (row.timeframe === "1d" && row.signalCount === null) return "일봉 신호 관측 여부 미확인 · 신호 수를 추정하지 않습니다.";
  if (!row.orderCount && !row.fillCount) return "주문·체결 미관측 · 신호와 주문 조건을 확인합니다.";
  return "관측된 신호·주문·체결이며 수익성 검증 결과가 아닙니다.";
}
