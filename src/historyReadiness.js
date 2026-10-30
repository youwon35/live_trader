export function historyReadinessLabel(observations, strategy, now = Date.now()) {
  const routes = observations?.[strategy.route];
  if (!Array.isArray(routes)) return '';
  const rows = routes.flatMap(route => Array.isArray(route?.streams) ? route.streams : []);
  const row = rows.find(row => row.symbol === strategy.symbol && row.timeframe === strategy.timeframe);
  if (!row) return '';
  const count = row.history?.observedBars;
  const history = Number.isSafeInteger(count) && count >= 0 ? `과거 ${count}봉 준비` : '과거 자료 확인 중';
  if (strategy.decisionCount > 0) return `${history} · 새 완성봉마다 평가`;
  const first = new Date(row.firstEligibleCloseAt).getTime();
  if (!row.firstEligibleCloseAt || !Number.isFinite(first)) return `${history} · 다음 완성봉 대기`;
  const time = new Date(first).toLocaleTimeString('ko-KR', {timeZone: 'Asia/Seoul', hour: '2-digit', minute: '2-digit'});
  return `${history} · ${now > first + 15000 ? '예정 시각 경과 · 수신 확인 필요' : `첫 평가 가능 ${time}`}${row.initialPartialExcluded ? ' · 시작 시 진행 중이던 봉 제외' : ''}`;
}
