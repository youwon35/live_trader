import { ordinaryRuntimeActive, ordinaryRuntimeKnown } from './statusTruth.js';
// Presentation only. None of these labels grants order or execution authority.
export function doctorCategorySummary(items = []) {
  const blocked = items.filter(row => row.tone === 'danger').length;
  const attention = items.filter(row => row.tone === 'warning').length;
  const unknown = items.filter(row => !['success', 'danger', 'warning'].includes(row.tone)).length;
  return { blocked, attention, unknown,
    label: blocked ? '해결할 분야 있음' : attention || unknown ? '확인할 분야 있음' : items.length ? '분야 점검 충족' : '미확인',
    detail: `차단 분야 ${blocked}곳 · 확인·주의 분야 ${attention + unknown}곳 · 아래 세부 시작 조건 수와는 다른 단위입니다.` };
}

export function doctorHistoryPresentation(diagnostics = {}) {
  const latest = diagnostics.latest || {};
  const exists = typeof latest.run_id === 'string' && latest.run_id.length > 0;
  const issues = Array.isArray(latest.issues) ? latest.issues : [];
  const blocked = issues.filter(row => row.severity === 'hard_stop').length;
  const warnings = issues.length - blocked;
  return { exists, blocked, warnings, issues,
    label: !exists ? '저장 기록 없음' : `기록 당시 미충족 ${blocked}건 · 주의 ${warnings}건`,
    detail: '저장된 일반 운용 검사 기록입니다. 현재 API 연결이나 주문 연결 시험의 준비 상태를 뜻하지 않습니다. 같은 원인이 여러 검사에 반복될 수 있습니다.',
    timestamp: latest.generated_at || '미실행' };
}

export function launchConditionPresentation(report = {}, observed = false, context = {}) {
  const failures = Array.isArray(report.hard_stops) ? report.hard_stops : null;
  const warnings = Array.isArray(report.warnings) ? report.warnings : null;
  const known = observed && failures !== null && warnings !== null
    && Number.isSafeInteger(report.hard_stop_count) && report.hard_stop_count >= 0
    && Number.isSafeInteger(report.warning_count) && report.warning_count >= 0
    && failures.length === report.hard_stop_count && warnings.length === report.warning_count;
  if (!known) return { known: false, tone: 'warning', label: '진단 자료 확인 필요',
    detail: '현재 진단 조건과 집계를 확인하지 못했습니다. 새 시작 점검을 실행하세요.',
    blocked: null, warnings: null, conditions: [], standbyInformation: [], informationCount: 0 };
  const blocked = failures.length;
  const presentedWarnings = warnings.map(row => preflightCheckPresentation(row, failures.length ? {...context, safetyHold: true} : context));
  const standbyInformation = presentedWarnings.filter(row => row.standbyOnly);
  const actionableWarnings = presentedWarnings.filter(row => !row.standbyOnly);
  const warningCount = actionableWarnings.length;
  return { known: true, tone: blocked ? 'danger' : warningCount ? 'warning' : 'success',
    label: blocked ? `미충족 시작 조건 ${blocked}건` : warningCount ? `검토할 조건 ${warningCount}건` : '현재 자료의 기본 조건 충족',
    detail: '일반 운용의 조건별 진단입니다. 실행 승인이나 주문 연결 시험의 준비 결과가 아닙니다. 시작 시 실제 계좌와 조건을 다시 확인합니다.',
    blocked, warnings: warningCount, standbyInformation, informationCount: standbyInformation.length,
    conditions: [...failures.map(row => ({...preflightCheckPresentation(row, context), tone: 'danger', statusLabel: '미충족'})),
      ...actionableWarnings.map(row => ({...row, tone: 'warning', statusLabel: '검토'}))] };
}

export function deploymentMetadataLabel(context = {}) {
  const names = { kis: 'KIS 국내', binance: 'Binance 현물', 'binance-futures': 'Binance 선물', upbit: 'Upbit 현물' };
  const broker = names[context.brokerId];
  const account = String(context.accountId || '').trim();
  const accountKnown = account && !/^\*+$/.test(account) && !['미확인','unknown','-'].includes(account);
  return `${broker || '배포 기록에 거래소 미지정'} · ${accountKnown ? `계정 ${account}` : '계좌는 실행 준비에서 확인'}`;
}

const START_DEFAULTS = {
  '운용자 확인': { label: '시작 시 실제 주문 동의', detail: '주문 연결 시험은 시작 화면에서 한 번 동의합니다. 일반 운용의 단계 승인·호환 경로는 해당 고급 제어에서 별도로 확인합니다.' },
  'Dry Run 보호': { label: '일반 운용 시작 전 전송 대기', detail: '실행 전 보수적인 대기 값입니다. 주문 연결 시험의 모의 실행 설정이나 준비 결과가 아닙니다.' },
  '신규 진입 차단': { label: '일반 운용 시작 전 주문 대기', detail: '일반 운용 엔진은 시작 검사와 위험 조건이 확인되기 전까지 새 주문을 보류합니다. 주문 연결 시험의 준비 상태와 별개이며, 일반 운용 호환 제어는 고급 영역에 있습니다.' },
};
const CHECK_GUIDANCE = {
  '포지션·계좌 대조': { meaning: '거래소 잔고와 앱 원장 사이에 차이가 있거나 일치를 확인하지 못했습니다.', action: '계좌·잔고에서 거래소·통화별 차이와 조회 시각을 확인하세요. 직접 거래·입출금이 있었다면 그 사실을 정상 검토 절차로 확인해야 하며, 숫자를 임의로 맞추지 않습니다.', destination: 'accounts' },
  '대조 증거 신선도': { meaning: '일반 운용에 필요한 같은 범위의 계좌·체결·원장 관측이 없거나 오래됐습니다. API 키 오류와는 다릅니다.', action: '일반 운용은 새 시작 점검에서 확인합니다. 계좌 snapshot만으로 전체 체결 관측을 대신할 수 없습니다. 짧은 시험은 주문 연결 시험 탭의 준비 검사를 사용하세요.', destination: 'functional-test' },
  '전략 승인': { meaning: '위에서 선택한 일반 운용 배포에 해당하는 실거래 자격 근거가 없습니다.', action: '운용 전략에서 선택한 배포와 원본 검증 단계를 확인하세요. 주문 연결 시험 후보와 일반 운용 배포의 승인은 별개이며 자동 승인하지 않습니다.', destination: 'gate' },
  '필수 운영 체크리스트': { meaning: '선택한 일반 운용의 필수 확인 항목이 남아 있습니다.', action: '한도·안전장치에서 남은 항목을 확인하세요. 계좌·대조처럼 자동 확인할 항목은 정상 조회 결과가 있어야 하며, 검토 항목을 임의로 체크하지 않습니다.', destination: 'risk' },
};

export function diagnosticSafetyContext(snapshot = {}) {
  const critical = snapshot.watchdog?.critical_count;
  const checks = snapshot.final_preflight;
  const known = snapshot.api_connected === true;
  const active = ordinaryRuntimeActive(snapshot);
  const hasHardStop = Array.isArray(checks) && checks.some(row => row.status === 'fail');
  const safetyHold = snapshot.kill_switch === true || snapshot.central_control?.halted === true
    || typeof critical === 'number' && Number.isFinite(critical) && critical > 0 || hasHardStop ? true
    : known && snapshot.kill_switch === false && snapshot.central_control?.halted === false
      && typeof critical === 'number' && Number.isFinite(critical) && critical === 0 && Array.isArray(checks) ? false : null;
  return { ordinaryActive: active, runtimeKnown: ordinaryRuntimeKnown(snapshot), safetyHold };
}

export function preflightCheckPresentation(check = {}, context = {}) {
  const guidance = CHECK_GUIDANCE[check.label] || {};
  const row = { ...check, ...guidance, sourceLabel: check.label, sourceStatus: check.status };
  const baseline = START_DEFAULTS[check.label];
  if (!baseline || check.status !== 'warn') return row;
  const standbyOnly = context.runtimeKnown === true && context.ordinaryActive === false
    && (check.label !== '신규 진입 차단' || context.safetyHold === false);
  return { ...row, label: standbyOnly ? baseline.label : check.label === '신규 진입 차단' ? '일반 운용 엔진의 신규 주문 보류' : baseline.label,
    detail: standbyOnly ? baseline.detail : check.label === '신규 진입 차단'
      ? '일반 운용 엔진의 안전 보류 상태입니다. 장애·원장·위험 조건을 확인해야 합니다. 주문 연결 시험에서 삭제한 수동 설정과 별개입니다. 일반 운용의 호환 제어는 고급 영역에 유지되며, 먼저 실제 차단 원인을 확인해야 합니다.'
      : baseline.detail,
    status: standbyOnly ? 'na' : check.status, standbyOnly };
}

export function ordinarySafetyPresentation(snapshot = {}) {
  const context = diagnosticSafetyContext(snapshot);
  const known = snapshot.api_connected === true;
  const blocked = snapshot.new_entries_blocked;
  return { context, rows: [
    { label: '실제 주문 동의', status: 'na', detail: '주문 연결 시험은 시작 화면에서 한 번 확인합니다. 일반 운용의 단계 승인과 호환 제어는 아래 고급 영역에서 별도로 확인합니다.' },
    { label: '일반 운용 엔진', status: context.ordinaryActive ? 'pass' : context.runtimeKnown ? 'na' : 'warn', detail: context.ordinaryActive ? '실행 중입니다. 아래 내부 안전 상태와 실제 주문·체결을 별도로 확인하세요.' : context.runtimeKnown ? '현재 일반 운용은 대기 중입니다. 주문 연결 시험은 해당 탭의 실행 범위를 사용합니다.' : '현재 실행 상태를 확인하지 못했습니다.' },
    { label: '엔진의 새 주문 보류', status: !known || typeof blocked !== 'boolean' ? 'warn' : blocked && (context.ordinaryActive || context.safetyHold !== false) ? 'warn' : 'na', detail: !known || typeof blocked !== 'boolean' ? '보류 상태 미확인' : blocked ? '내부 상태는 보류입니다. 일반 운용의 실행 전 대기와 장애·위험 보호에 사용합니다. 주문 연결 시험과 별개이며, 실제 차단 원인과 고급 제어를 확인한 뒤 해당 범위의 시작 검사를 다시 거칩니다.' : '보류 신호는 꺼져 있습니다. 이것만으로 주문 권한을 부여하지 않습니다.' },
    { label: '전체 긴급 정지', status: !known || typeof snapshot.kill_switch !== 'boolean' ? 'warn' : snapshot.kill_switch ? 'fail' : 'pass', detail: !known || typeof snapshot.kill_switch !== 'boolean' ? '긴급 정지 상태 미확인' : snapshot.kill_switch ? '작동 중입니다. 원인과 진행 중 주문을 확인해야 합니다.' : '긴급 정지 신호가 없습니다.' },
  ] };
}
