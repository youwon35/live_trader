// Display-only projections. No function here grants authority or dispatches an order.
export const numberOrNull = value => value == null || value === '' || typeof value === 'boolean' || !Number.isFinite(Number(value)) ? null : Number(value);
export const amount = value => numberOrNull(value) == null ? '미확인' : Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 10 });
export const stamp = value => !value || !Number.isFinite(Date.parse(value)) ? (value || '미확인') : new Date(value).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' });
const array = value => Array.isArray(value) ? value : [];
const status = value => String(value || '').toLowerCase();

export function preflightDelta(history = []) {
  const [latest, previous] = history;
  if (!latest) return { known: false, first: true, newIssues: [], resolved: [], unchanged: [], removed: [] };
  const old = new Map(array(previous?.checks).map(row => [row.label, row]));
  const now = new Map(array(latest.checks).map(row => [row.label, row]));
  const issue = row => ['fail', 'warn'].includes(status(row?.status));
  const newIssues = [], resolved = [], unchanged = [], removed = [];
  for (const [label, row] of now) {
    const before = old.get(label);
    if (issue(row)) (issue(before) && status(before.status) === status(row.status) ? unchanged : newIssues).push(row);
    if (issue(before) && status(row.status) === 'pass') resolved.push(row);
  }
  for (const [label, row] of old) if (issue(row) && !now.has(label)) removed.push(row);
  return { known: true, first: !previous, latest, previous, newIssues, resolved, unchanged, removed };
}

export function noOrderReasons(snapshot = {}, context = {}) {
  const runtime = snapshot.continuous_runtime || {};
  const profiles = Object.values(runtime.profiles || {}).filter(row => row.running === true);
  if (!profiles.length && runtime.running === true) profiles.push(runtime);
  const running = profiles.find(row => row.deploymentId === context.id);
  const result = [];
  if (snapshot.api_connected !== true) return [{ category: '상태 미확인', detail: '앱 연결이 없어 최근 실행·주문 상태를 확인할 수 없습니다.' }];
  if (!running) result.push({ category: '조건 대기', detail: profiles.length ? '조회 중인 배포는 실행 중이 아닙니다. 다른 배포의 실행 정보와 구분하세요.' : '감시가 중지되어 전략의 새 주문 평가를 하지 않습니다.' });
  if (snapshot.kill_switch === true || snapshot.new_entries_blocked === true) result.push({ category: '위험 제한', detail: snapshot.kill_switch === true ? '긴급 정지로 주문 경로가 차단되었습니다.' : '신규 진입 차단이 켜져 있습니다. 위험 감소 주문도 별도 검사를 거칩니다.' });
  const ordinary = snapshot.execution_availability?.ordinaryContinuous;
  if (snapshot.dry_run === true || snapshot.operator_confirmed === false || ordinary?.liveDispatchAvailable === false || (running && running.mode === 'MONITOR')) result.push({ category: '권한 없음', detail: '관찰 모드·가상 전송 보호 또는 실주문 권한 제한이 있습니다.' });
  const runner = snapshot.strategy_runner || {};
  const matches = running && runner.last_profile === running.profileId && runner.last_strategy === context.strategyId;
  const rejected = array(snapshot.orders).find(row => (row.deployment_id === context.id || (row.strategy_id === context.strategyId && row.broker_id === context.brokerId)) && row.risk_report?.can_submit === false && Number.isFinite(Date.parse(row.time || row.created_at)) && Number.isFinite(Date.parse(runner.last_run)) && Date.parse(row.time || row.created_at) >= Date.parse(runner.last_run) - 2000);
  if (matches && rejected) result.push({ category: '위험 제한', detail: `최근 평가의 주문이 위험 검사에서 차단됨 · ${rejected.reason || '상세 사유 미기록'}. 새 주문은 다시 검사합니다.` });
  if (matches && runner.last_run && runner.last_market_verified === true) {
    const signal = String(runner.last_signal || '').toUpperCase();
    if (['HOLD', 'NONE', 'NO_SIGNAL'].includes(signal)) result.push({ category: '신호 없음', detail: `${stamp(runner.last_run)} 평가에서 매매 신호가 없었습니다. ${runner.last_action || ''}` });
    else result.push({ category: '조건 대기', detail: `${stamp(runner.last_run)} 최근 평가: ${runner.last_action || '상세 사유 미기록'}. 현재 무주문의 단일 원인으로 확정하지 않습니다.` });
  } else if (running) result.push({ category: '조건 대기', detail: '이 배포에 연결된 검증된 최근 평가가 없습니다. 다음 완료 봉 또는 시세·실행 기록을 확인하세요.' });
  return result;
}

export const stopEffects = [
  ['신규 진입 차단', '새 진입·위험 증가 차단', '자동 취소하지 않음', '유지 · 위험 감소도 별도 검사'],
  ['감시 중지', '선택 자산군 평가 중지 · 관찰 모드 전환', '자동 취소하지 않음', '유지 · 자동 청산하지 않음'],
  ['전체 긴급 정지', '영구 차단 기록 후 전송 차단', '소유가 확인된 주문 취소 요청·대조, 실패/미확정은 남김', '유지 · 자동 청산하지 않음'],
];

export function reconciliationEvidence(snapshot = {}) {
  const positions = array(snapshot.reconciliation?.positions || snapshot.positions).map(row => {
    const program = numberOrNull(row.program_qty_value), broker = numberOrNull(row.broker_qty_value);
    return { ...row, program, broker, difference: program == null || broker == null ? null : broker - program };
  });
  const events = array(snapshot.program_ledger?.execution_events || snapshot.execution_events?.recent);
  return { positions, events, at: snapshot.reconciliation?.summary?.last_run, baseline: snapshot.program_ledger?.state?.last_baseline };
}
export function feeEvidence(event) {
  const raw = event.raw || {};
  const fee = numberOrNull(raw.fee ?? event.fee);
  const currency = raw.fee_currency || raw.feeCurrency || raw.commissionAsset || event.fee_currency;
  return { fee, currency: currency || '통화 미기록', mode: raw.fee_mode === 'cumulative' ? '누계' : raw.fee_mode === 'delta' ? '개별' : '기준 미기록', verified: fee != null && Boolean(currency) };
}
export function capitalAllowance(snapshot, context, draftLimit) {
  const limit = numberOrNull(draftLimit);
  if (limit == null || limit < 0) return { limit, used: null, remaining: null, reason: '한도를 올바른 금액으로 입력하세요.' };
  if (snapshot.api_connected !== true) return { limit, used: null, remaining: null, reason: '계좌 연결 상태가 미확인입니다.' };
  const rows = array(snapshot.positions).filter(row => row.broker_id === context.brokerId && row.symbol === context.symbol);
  const account = array(snapshot.accounts).find(row => row.broker_id === context.brokerId && row.currency === 'KRW');
  if (!context.symbol || !rows.length || !account || rows.some(row => row.currency !== 'KRW') || /futures/i.test(context.brokerId || '')) return { limit, used: null, remaining: null, reason: '원화 현물의 확인된 보유 경로가 필요합니다. 외화·선물은 원화로 추정하지 않습니다.' };
  const values = rows.map(row => {
    const qty = numberOrNull(row.broker_qty_value), price = numberOrNull(row.current_price);
    return qty == null || (qty !== 0 && (price == null || price <= 0)) ? null : Math.abs(qty) * (price || 0);
  });
  if (values.some(value => value == null)) return { limit, used: null, remaining: null, reason: '현재 보유 수량 또는 시세가 미확인입니다.' };
  const used = values.reduce((sum, value) => sum + value, 0);
  const pending = array(snapshot.orders).filter(row => row.broker_id === context.brokerId && row.symbol === context.symbol && !['filled', 'canceled', 'cancelled', 'rejected', 'risk_blocked', 'risk_rejected', 'dry_run', 'expired'].includes(status(row.state)));
  return { limit, used, remaining: Math.max(0, limit - used), over: Math.max(0, used-limit), pending: pending.length, reason: '현재 보유 종목의 명목금액 기준입니다. 미체결 추가 체결·현금·다른 위험 검사는 별도이며 주문 허가가 아닙니다.' };
}

const readable = value => value == null ? '미확인' : typeof value === 'object' ? JSON.stringify(value) : typeof value === 'number' ? amount(value) : String(value);
export function deploymentChanges(document = {}) {
  const before = document.before, after = document.after;
  if (!before || !after) return { known: false, changes: [], unknown: ['비교할 두 고정 구성의 근거가 부족합니다.'] };
  const changes = [], unknown = [];
  const compare = (label, old, next) => {
    if (old == null || next == null) { unknown.push(`${label}: ${readable(old)} → ${readable(next)}`); return; }
    if (JSON.stringify(old) !== JSON.stringify(next)) changes.push({ label, before: readable(old), after: readable(next) });
  };
  compare('대상 종목', Array.isArray(before.metadata?.allowedSymbols) ? before.metadata.allowedSymbols.slice().sort().join(', ') : null, Array.isArray(after.metadata?.allowedSymbols) ? after.metadata.allowedSymbols.slice().sort().join(', ') : null);
  compare('전략 저장본', before.strategyArtifactHash, after.strategyArtifactHash);
  compare('거래 경로', before.brokerRoute, after.brokerRoute);
  compare('계좌 식별값', before.accountFingerprint, after.accountFingerprint);
  const weights = new Set([...Object.keys(before.reviewWeights || {}), ...Object.keys(after.reviewWeights || {})]);
  for (const key of weights) {
    const old = before.reviewWeights || {}, next = after.reviewWeights || {};
    if (!(key in old)) changes.push({ label: `목표 비중 ${key}`, before: '구성에 없음', after: next[key] == null ? '미확인' : `${amount(next[key]*100)}%` });
    else if (!(key in next)) changes.push({ label: `목표 비중 ${key}`, before: old[key] == null ? '미확인' : `${amount(old[key]*100)}%`, after: '구성에서 제외' });
    else compare(`목표 비중 ${key}`, old[key] == null ? null : `${amount(old[key]*100)}%`, next[key] == null ? null : `${amount(next[key]*100)}%`);
  }
  if (before.reviewRisk && after.reviewRisk) for (const key of new Set([...Object.keys(before.reviewRisk), ...Object.keys(after.reviewRisk)])) compare(`위험 설정 ${after.reviewRiskLabels?.[key] || before.reviewRiskLabels?.[key] || key}`, before.reviewRisk[key], after.reviewRisk[key]);
  else unknown.push(before.riskPolicyHash === after.riskPolicyHash ? '위험 설정 검증값은 같습니다. 과거 세부값은 저장되지 않았습니다.' : '위험 설정 검증값이 다릅니다. 과거 세부값은 저장되지 않아 항목별 변경은 미확인입니다.');
  return { known: true, changes, unknown };
}
export function orderInvestigation(snapshot, order) {
  const matches = row => Boolean(order.order_id && (row.order_id === order.order_id || row.orderId === order.order_id)) && (!row.broker_id || row.broker_id === order.broker_id);
  const audit = array(snapshot.durable_audit).filter(matches);
  const events = array(snapshot.execution_events?.recent).filter(matches);
  return { audit, events, lastExecution: events.map(row => row.occurred_at).filter(Boolean).sort().at(-1), lastAccountReconciliation: snapshot.reconciliation?.summary?.last_run, reason: order.reconciliation_warning || order.cancel_reconciliation || order.reason || '조사 사유 미기록', nextRetry: order.next_retry_at, lastUpdated: order.updated_at };
}
