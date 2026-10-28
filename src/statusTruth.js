// Read-only presentation: these values never grant execution authority.
import { deploymentContextMatchesPreflight } from './deploymentSelection.js';
import { runtimeRows } from './runtimeSelection.js';
export function currentOrderPermission(snapshot = {}) {
  // Exact execution consent is deliberately not a persistent app switch.
  return snapshot.api_connected === true ? 'confirm-at-start' : null;
}
export function ordinaryRuntimeActive(snapshot = {}) {
  return snapshot.api_connected === true && (snapshot.continuous_runtime?.running === true || runtimeRows(snapshot.continuous_runtime || {}).some(row => row.running === true));
}
export function environmentStatus(snapshot = {}, selectedId = '', now = Date.now()) {
  const known = snapshot.api_connected === true;
  const permission = currentOrderPermission(snapshot);
  const governance = snapshot.live_governance || {};
  const preflight = governance.latestPreflight || governance.latest_preflight || {};
  const expires = Date.parse(preflight.expiresAt || preflight.expires_at || '');
  const valid = known && deploymentContextMatchesPreflight(selectedId, governance)
    && String(preflight.deploymentId || preflight.deployment_id || '') === selectedId
    && governance.preflightValidity?.valid === true
    && String(preflight.status || preflight.result || '').toUpperCase() === 'PASS'
    && Number.isFinite(expires) && expires > now;
  const items = [
    { label: '실거래 확인', value: permission === null ? '미확인' : '시작 시 한 번', tone: permission === null ? 'warning' : 'info', detail: '별도 잠금 설정은 없습니다. 실제 주문은 선택한 실행의 시작 확인과 준비 조건을 통과해야 전송됩니다.' },
    { label: '선택 배포 점검', value: !known ? '미확인' : !selectedId ? '배포 선택 필요' : valid ? '유효' : '점검 필요', tone: valid ? 'success' : 'warning', detail: '일반 운용 배포의 시작 점검입니다. 주문 연결 시험은 해당 탭의 준비 결과를 사용합니다.' },
  ];
  if (ordinaryRuntimeActive(snapshot)) items.push({ label: '일반 운용 신규 진입', value: typeof snapshot.new_entries_blocked !== 'boolean' ? '미확인' : snapshot.new_entries_blocked ? '차단' : '허용', tone: snapshot.new_entries_blocked === false ? 'success' : 'warning' });
  items.push({ label: '전체 긴급 정지', value: snapshot.kill_switch === true ? '작동 중' : known && snapshot.kill_switch === false ? '정상' : '미확인', tone: snapshot.kill_switch === true ? 'danger' : known && snapshot.kill_switch === false ? 'success' : 'warning' });
  return { items, valid };
}
export function brokerAccountObservation(snapshot, brokerId, now = Date.now()) {
  if (!brokerId) return { status: 'warn', detail: '거래소 식별자를 확인하지 못했습니다.' };
  const errors = (snapshot.reconciliation?.errors || []).filter(row => row.broker_id === brokerId);
  if (errors.length) return { status: 'fail', detail: '계좌 조회 실패 · 계좌·잔고에서 오류 원인을 확인하세요.' };
  const accounts = (snapshot.accounts || []).filter(row => row.broker_id === brokerId);
  const number = value => typeof value === 'number' && Number.isFinite(value);
  const found = accounts.length > 0 && accounts.every(row => number(row.broker_cash_value) || number(row.broker_equity_value));
  const stamp = Date.parse(snapshot.reconciliation?.summary?.last_run || '');
  const fresh = snapshot.api_connected === true && Number.isFinite(stamp) && now >= stamp && now - stamp <= 300000;
  if (!found) return { status: 'warn', detail: '인증정보 저장됨 · 실제 계좌 조회는 미확인입니다.' };
  return fresh ? { status: 'pass', detail: '최근 계좌 조회 성공 · 주문 권한과 원장 대조는 별도 점검합니다.' }
    : { status: 'warn', detail: '과거 계좌 조회값 · 현재 연결은 계좌 다시 읽기로 확인하세요.' };
}

export function ordinaryRuntimeKnown(snapshot = {}) {
  if (snapshot.api_connected !== true) return false;
  if (typeof snapshot.continuous_runtime?.running === 'boolean') return true;
  const rows = runtimeRows(snapshot.continuous_runtime || {});
  return rows.length > 0 && rows.every(row => typeof row.running === 'boolean');
}
export function activeOrderCountLabel(snapshot = {}) {
  const count=snapshot.order_queue?.active;
  return snapshot.api_connected === true && Number.isSafeInteger(count) && count >= 0 ? `미결 주문 ${count}` : '미결 주문 미확인';
}

export function ordinaryEntryStatus(snapshot = {}) {
  const known = snapshot.api_connected === true && typeof snapshot.new_entries_blocked === 'boolean';
  return { label: !known ? '일반 운용 신규 진입 미확인' : snapshot.new_entries_blocked ? '일반 운용 신규 진입 차단' : '일반 운용 신규 진입 허용', tone: known && snapshot.new_entries_blocked === false ? 'success' : 'warning' };
}
