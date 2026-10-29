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

export function launchConditionPresentation(report = {}, observed = false) {
  const failures = Array.isArray(report.hard_stops) ? report.hard_stops : null;
  const warnings = Array.isArray(report.warnings) ? report.warnings : null;
  const known = observed && failures !== null && warnings !== null
    && Number.isSafeInteger(report.hard_stop_count) && report.hard_stop_count >= 0
    && Number.isSafeInteger(report.warning_count) && report.warning_count >= 0
    && failures.length === report.hard_stop_count && warnings.length === report.warning_count;
  if (!known) return { known: false, tone: 'warning', label: '진단 자료 확인 필요',
    detail: '현재 진단 조건과 집계를 확인하지 못했습니다. 새 시작 점검을 실행하세요.',
    blocked: null, warnings: null, conditions: [] };
  const blocked = failures.length;
  const warningCount = warnings.length;
  return { known: true, tone: blocked ? 'danger' : warningCount ? 'warning' : 'success',
    label: blocked ? `미충족 시작 조건 ${blocked}건` : warningCount ? `검토할 조건 ${warningCount}건` : '현재 자료의 기본 조건 충족',
    detail: '일반 운용의 조건별 진단입니다. 실행 승인이나 주문 연결 시험의 준비 결과가 아닙니다. 시작 시 실제 계좌와 조건을 다시 확인합니다.',
    blocked, warnings: warningCount,
    conditions: [...failures.map(row => ({...row, tone: 'danger', statusLabel: '미충족'})),
      ...warnings.map(row => ({...row, tone: 'warning', statusLabel: '검토'}))] };
}

export function deploymentMetadataLabel(context = {}) {
  const names = { kis: 'KIS 국내', binance: 'Binance 현물', 'binance-futures': 'Binance 선물', upbit: 'Upbit 현물' };
  const broker = names[context.brokerId];
  const account = String(context.accountId || '').trim();
  const accountKnown = account && !/^\*+$/.test(account) && !['미확인','unknown','-'].includes(account);
  return `${broker || '배포 기록에 거래소 미지정'} · ${accountKnown ? `계정 ${account}` : '계좌는 실행 준비에서 확인'}`;
}
