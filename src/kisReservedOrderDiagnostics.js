// Public records only. Backend validates its canonical parsed-JSON checksum;
// the browser validates this exact public shape and never treats a hash as authority.
export const RESERVED_FALSE_FLAGS = ["fullAccountReady", "accountWideAbsenceAuthoritative", "completeAccountWorkingOrdersVerified",
  "accountCashReconciled", "sharedCashBaselineEstablished", "sharedCashReservationVerified", "actualFeeObserved",
  "actualFxExecutionObserved", "settlementFinalityVerified", "financialReady", "productionReady", "orderAuthorityGranted",
  "ledgerInitialized", "authenticatedProducerAttested", "reservedOrdersReconciled", "completeConditionalOrdersVerified",
  "orderLifecycleInterpreted", "crossVenueRowsMerged"];
const ENVELOPE_FLAGS = ["fullAccountReady", "reservedOrdersReconciled", "completeConditionalOrdersVerified", "financialReady", "orderAuthorityGranted"];
const STATUS = "REPORTED_FILTERED_RESERVATIONS_NOT_ALL_WORKING_ORDERS";
export const RESERVED_SCOPE_DEFINITIONS = [
  { id: "KR_RESERVED_CNCL_Y_PRCS_0", path: "/uapi/domestic-stock/v1/trading/order-resv-ccnl", trId: "CTSC0004R",
    query: { TMNL_MDIA_KIND_CD: "00", PRCS_DVSN_CD: "0", CNCL_YN: "Y", RSVN_ORD_SEQ: "", PDNO: "", SLL_BUY_DVSN_CD: "" },
    meaning: "KR_EXAMPLE_FILTER_PROCESSING_HISTORY_NOT_ALL_ACTIVE_RESERVATIONS", dates: "KR",
    label: "국내 예약 조회 · 예제 필터 범위" },
  ...["NASD", "NYSE", "AMEX"].map((venue) => ({ id: `US_RESERVED_${venue}`,
    path: "/uapi/overseas-stock/v1/trading/order-resv-list", trId: "TTTT3039R",
    query: { INQR_DVSN_CD: "00", OVRS_EXCG_CD: venue, PRDT_TYPE_CD: "" },
    meaning: "US_EXPLICIT_VENUE_RESERVATION_QUERY_NOT_DISJOINT_OR_ALL_CONDITIONS", dates: "US",
    label: `미국 예약 조회 · ${venue} 범위` })),
  { id: "KR_REGULAR_CONDITION_FIELDS", path: "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl", trId: "TTTC0084R",
    query: { INQR_DVSN_1: "0", INQR_DVSN_2: "0" },
    meaning: "KR_REVOCABLE_ORDER_REPORTED_FIELDS_NOT_ALL_CONDITIONAL_INSTRUCTIONS", dates: null,
    label: "국내 정정·취소 가능 응답의 조건 필드" },
];
export const RESERVED_UNCOVERED_SCOPES = [
  ["DOMESTIC_CNCL_Y_PRCS_0_ALL_ACTIVE_FILTER_MEANING_UNVERIFIED", "국내 예제 필터가 모든 활성 예약을 포함하는지 미확인"],
  ["ORDER_LIFECYCLE_STATE_CODES_UNREVIEWED", "예약 상태 코드의 실제 생명주기 해석 미확인"],
  ["DATES_BEFORE_WINDOW_AND_AFTER_WINDOW", "선택 기간 전·후의 예약"],
  ["UNTRIGGERED_HTS_MTS_OR_OTHER_CONDITIONAL_INSTRUCTIONS", "미발동 HTS·MTS 및 다른 조건부 지시"],
  ["OVERSEAS_NON_US_MARKETS_AND_OTHER_SERVICES", "미국 이외 해외시장과 다른 서비스"],
  ["CROSS_VENUE_OVERLAP_UNRECONCILED", "거래소별 중복 미대조 · 행 수 합산 금지"],
  ["CROSS_REQUEST_ATOMIC_ACCOUNT_SNAPSHOT", "여러 응답이 동일 시점의 전체 계좌 상태인지 미확인"],
  ["ACTUAL_CASH_FEES_FX_AND_SETTLEMENT", "실제 현금·수수료·자동환전·확정 정산"],
];
function exact(value, keys) {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}
function same(a, b) {
  if (a === b) return true;
  if (Array.isArray(a) || Array.isArray(b)) return Array.isArray(a) && Array.isArray(b)
    && a.length === b.length && a.every((v, i) => same(v, b[i]));
  return a !== null && b !== null && typeof a === "object" && typeof b === "object"
    && exact(a, Object.keys(b)) && Object.keys(b).every((k) => same(a[k], b[k]));
}
function micros(value) {
  if (typeof value !== "string") return null;
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(?:Z|\+00:00)$/.exec(value);
  if (!match || match[1].startsWith("0000-")) return null;
  const base = Date.parse(`${match[1]}Z`);
  if (!Number.isFinite(base) || new Date(base).toISOString().slice(0, 19) !== match[1]) return null;
  return BigInt(base) * 1000n + BigInt((match[2] || "").padEnd(6, "0"));
}
function dateMs(value) {
  if (typeof value !== "string" || !/^\d{8}$/.test(value) || value.startsWith("0000")) return NaN;
  const iso = `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6)}T00:00:00.000Z`;
  const result = Date.parse(iso);
  return Number.isFinite(result) && new Date(result).toISOString() === iso ? result : NaN;
}
function count(value) { return Number.isInteger(value) && value >= 0 && value <= 10000; }
function validConditionFields(fields, rowCount, applicable) {
  if (!Array.isArray(fields) || fields.length !== (applicable ? 2 : 0)) return false;
  return fields.every((r, i) => exact(r, ["field", "absentCount", "emptyCount", "reportedTextCount", "invalidTypeCount", "reportedDecimalTextCount"])
    && r.field === ["stpm_cndt_pric", "stpm_efct_occr_yn"][i]
    && ["absentCount", "emptyCount", "reportedTextCount", "invalidTypeCount", "reportedDecimalTextCount"].every((k) => count(r[k]))
    && r.absentCount + r.emptyCount + r.reportedTextCount + r.invalidTypeCount === rowCount
    && r.reportedDecimalTextCount <= r.reportedTextCount);
}
function validate(diagnostic, result, draft, readAccount, readReservedOrders) {
  if (readAccount !== true || readReservedOrders !== true
    || !exact(diagnostic, ["schemaVersion", "status", "reservations", ...ENVELOPE_FLAGS])
    || diagnostic.schemaVersion !== "kis-reserved-order-preparation/v1"
    || ENVELOPE_FLAGS.some((k) => diagnostic[k] !== false)) return false;
  const instruments = result?.source?.instruments;
  if (!Array.isArray(instruments)) return false;
  const selected = instruments.filter((item) => item?.instanceId === draft?.instanceId);
  if (selected.length !== 1 || !["kis-kr", "kis-us"].includes(selected[0].broker)) return false;
  if (diagnostic.status === "UNAVAILABLE") return diagnostic.reservations === null;
  if (diagnostic.status !== "OBSERVED_LIMITED_SCOPES") return false;
  const r = diagnostic.reservations;
  const keys = ["schemaVersion", "status", "provider", "requestWindow", "scopes", "startedAt", "completedAt", "assessedAt",
    "freshnessBoundarySeconds", "collectionMonotonicDurationSeconds", "freshnessExpiresAt", "freshnessStatus", "uncoveredScopes",
    "reasons", "sourceCaptureHash", "hashMeaning", "retrievalTimeMeaning", "contentHash", ...RESERVED_FALSE_FLAGS];
  if (!exact(r, keys) || r.schemaVersion !== "kis-reserved-order-summary/v1" || r.status !== STATUS || r.provider !== "kis"
    || RESERVED_FALSE_FLAGS.some((k) => r[k] !== false) || r.freshnessBoundarySeconds !== 15
    || r.hashMeaning !== "CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_OR_AUTHENTICATION"
    || r.retrievalTimeMeaning !== "LOCAL_GET_TIMES_NOT_ATOMIC_ACCOUNT_OR_SETTLEMENT_TIME"
    || ["sourceCaptureHash", "contentHash"].some((k) => typeof r[k] !== "string" || !/^[a-f0-9]{64}$/.test(r[k]))
    || typeof r.collectionMonotonicDurationSeconds !== "number" || !Number.isFinite(r.collectionMonotonicDurationSeconds)
    || r.collectionMonotonicDurationSeconds < 0 || !Array.isArray(r.scopes) || r.scopes.length !== 5
    || !same(r.uncoveredScopes, RESERVED_UNCOVERED_SCOPES.map(([key]) => key))) return false;
  const start = micros(r.startedAt), end = micros(r.completedAt), assessed = micros(r.assessedAt), expires = micros(r.freshnessExpiresAt);
  if ([start, end, assessed, expires].some((v) => v === null) || end < start || expires !== start + 15000000n) return false;
  const fresh = start <= end && end <= assessed && assessed - start <= 15000000n && r.collectionMonotonicDurationSeconds <= 15;
  if (r.freshnessStatus !== (fresh ? "WITHIN_LOCAL_15_SECOND_BOUNDARY" : "STALE_OR_CLOCK_INVALID")) return false;
  const w = r.requestWindow;
  if (!exact(w, ["startDate", "endDate", "calendarDayCount", "localMaximumCalendarDays", "dateMeaning"])) return false;
  const ws = dateMs(w.startDate), we = dateMs(w.endDate), days = (we - ws) / 86400000 + 1;
  const startedKst = new Date(Number(start / 1000n) + 9 * 3600000).toISOString().slice(0, 10).replaceAll("-", "");
  if (!Number.isInteger(days) || days < 1 || days > 31 || w.calendarDayCount !== days || w.localMaximumCalendarDays !== 31
    || w.endDate > startedKst || w.dateMeaning !== "API_NAMED_DATE_FILTERS_NOT_ALL_LIFETIME_OR_FUTURE_ORDERS") return false;
  const reasons = ["KIS_RESERVED_FILTERED_SCOPES_NOT_FULL_ACCOUNT", "KIS_RESERVED_FILTER_AND_LIFECYCLE_MEANING_UNVERIFIED",
    ...(!fresh ? ["KIS_RESERVED_LOCAL_RETRIEVAL_STALE_OR_CLOCK_INVALID"] : [])];
  let previousEnd = null;
  const counts = ["pageCount", "rowCount", "distinctIdentityCount", "identicalDuplicateCount", "missingIdentityCount", "reportedStateFieldRows", "missingStateFieldRows"];
  for (const [index, row] of r.scopes.entries()) {
    const spec = RESERVED_SCOPE_DEFINITIONS[index];
    const query = { ...spec.query, ...(spec.dates === "KR" ? { RSVN_ORD_ORD_DT: w.startDate, RSVN_ORD_END_DT: w.endDate }
      : spec.dates === "US" ? { INQR_STRT_DT: w.startDate, INQR_END_DT: w.endDate } : {}) };
    if (!exact(row, ["scopeId", "path", "trId", "method", "queryScope", "scopeMeaning", ...counts,
      "conditionFieldStates", "stateInterpretation", "paginationStatus", "startedAt", "completedAt"])
      || row.scopeId !== spec.id || row.path !== spec.path || row.trId !== spec.trId || row.method !== "GET"
      || !same(row.queryScope, query) || row.scopeMeaning !== spec.meaning || row.stateInterpretation !== "UNKNOWN_UNREVIEWED_CODES"
      || !counts.every((k) => count(row[k])) || row.pageCount < 1 || row.pageCount > 2
      || row.rowCount !== row.distinctIdentityCount + row.identicalDuplicateCount + row.missingIdentityCount
      || row.rowCount !== row.reportedStateFieldRows + row.missingStateFieldRows
      || !["COMPLETE", "PARTIAL", "UNKNOWN"].includes(row.paginationStatus)
      || !validConditionFields(row.conditionFieldStates, row.rowCount, index === 4)) return false;
    const rs = micros(row.startedAt), re = micros(row.completedAt);
    if (rs === null || re === null || re < rs || previousEnd !== null && rs < previousEnd) return false;
    previousEnd = re;
    if (row.missingIdentityCount) reasons.push(`KIS_RESERVED_${row.scopeId}_ROW_IDENTITIES_UNVERIFIED`);
    if (row.identicalDuplicateCount) reasons.push(`KIS_RESERVED_${row.scopeId}_IDENTICAL_PAGE_OVERLAP`);
    if (row.paginationStatus !== "COMPLETE") reasons.push(`KIS_RESERVED_${row.scopeId}_PAGINATION_${row.paginationStatus}`);
  }
  return r.startedAt === r.scopes[0].startedAt && r.completedAt === r.scopes[4].completedAt && same(r.reasons, reasons);
}
export function validReservedOrderDiagnostics(diagnostic, result, draft, readAccount, readReservedOrders) {
  try { return validate(diagnostic, result, draft, readAccount, readReservedOrders); } catch { return false; }
}
