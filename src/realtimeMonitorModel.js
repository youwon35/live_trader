import { failureTiming } from './quoteObservation.js';
import { SHORT_LIVE_ROUTES, shortLiveBlockerLabel, normalizeShortLiveFailureDiagnostics } from './shortLiveTestModel.js';

const known = value => typeof value === 'number' && Number.isFinite(value);
const stamp = value => typeof value === 'string' && Number.isFinite(Date.parse(value)) ? Date.parse(value) : null;
const sumKnown = values => values.length > 0 && values.every(known) ? values.reduce((a, b) => a + b, 0) : null;
export const monitorRoute = route => SHORT_LIVE_ROUTES.find(row => row.id === route)?.label || route || '경로 미확인';
export const monitorEventLabels = {
  FIRST_FAILURE: '최초 중단 원인 기록', STARTED: '시험 시작 기록', BAR_EVALUATED: '완성봉 평가', NEXT_OPEN_OBSERVED: '다음 봉 시작 관측',
  ORDER_DEFERRED_PENDING_FILL: '앞선 주문 체결 대기', ORDER_INTENT_BEFORE_SUBMIT: '주문 제출 시도',
  ORDER_REPLY: '거래소 주문 응답', ACTUAL_FILL_INCREMENT: '추가 체결 관측',
  INTERNAL_ALLOCATION_TRANSFER: '전략 간 내부 배분', CLEANUP_INTENT_ONCE: '종료 대조 시작',
  CLEANUP_ORDER_OBSERVED: '종료 주문 관측', WORKER_EXIT_INTENT: '작업자 종료 의사 기록',
  WORKER_DEATH_OBSERVED: '작업자 종료 관측', STARTUP_RETIRED_BEFORE_WORKER: '작업자 시작 전 종료',
};

export function monitorFailureDiagnostic(row) {
  const providerLabel = ({ binance: 'Binance 현물', 'binance-futures': 'Binance USDT 선물', upbit: 'Upbit', kis: '한국투자증권' })[row.provider] || '거래소 미확인';
  const kind = row.quoteObservation?.status === 'BLOCKED' ? 'QUOTE_VALIDATION' : row.failureKind || row.status;
  const kindLabel = ({ TIMEOUT: '응답 시간 초과', HTTP_ERROR: 'HTTP 오류', CONNECTION_ERROR: '연결 오류',
    INVALID_RESPONSE: '응답 형식 오류', READ_BOUNDARY: '조회 조건 확인 실패', READ_REJECTED: '조회 요청 거절',
    READ_UNAVAILABLE: '자료 조회 불가', UNEXPECTED_ERROR: '예상하지 못한 조회 오류',
    QUOTE_VALIDATION: '시세 시각 검증 차단', AVAILABLE: 'HTTP 조회 완료', BLOCKED: '조회 차단', UNAVAILABLE: '자료 조회 불가', REJECTED: '요청 거절' })[kind];
  const resultLabel = [row.httpStatus !== null ? `HTTP ${row.httpStatus}` : '', kindLabel ? `${kindLabel} (${kind})` : kind || '원인 미확인'].filter(Boolean).join(' · ');
  return { ...row, providerLabel, resultLabel, elapsedLabel: known(row.elapsedMs) ? row.elapsedMs < 1000 ? `${row.elapsedMs.toFixed(1)}ms` : `${(row.elapsedMs / 1000).toFixed(1)}초` : '소요 시간 미기록' };
}

export function projectRealtimeMonitor(snapshot, { fresh = false, pollError = '', receivedAt = null, now = Date.now(), staleMs = 20000 } = {}) {
  const run = snapshot?.runtime;
  const received = known(receivedAt) ? receivedAt : null;
  const serverAt = stamp(snapshot?.observedAt);
  const ageMs = received === null ? null : Math.max(0, now - received);
  const responseCurrent = fresh === true && received !== null && received - now <= 5000 && ageMs <= staleMs && serverAt !== null
    && now - serverAt <= staleMs && serverAt - now <= 5000;
  const brokers = run?.brokers || [];
  const strategies = run?.strategies || [];
  const feeds = (run?.feedObservations || []).map(row => {
    const streams = row.streams || [];
    const allRecent = streams.length > 0 && streams.every(stream => stream.socketPresent === true
      && stamp(stream.lastFrameAt) !== null && now - stamp(stream.lastFrameAt) <= 60000 && stamp(stream.lastFrameAt) - now <= 5000);
    const disconnected = streams.some(stream => stream.socketPresent === false);
    const delayed = streams.some(stream => stamp(stream.lastFrameAt) !== null && now - stamp(stream.lastFrameAt) > 60000);
    const times = streams.map(stream => stamp(stream.lastFrameAt)).filter(value => value !== null);
    return { route: row.route, allRecent, disconnected, delayed,
      lastFrameAt: times.length ? new Date(Math.min(...times)).toISOString() : null };
  });
  const expectedRoutes = [...new Set(strategies.map(row => row.route).filter(Boolean))];
  const coversExpectedRoutes = rows => expectedRoutes.length > 0 && expectedRoutes.every(route => rows.filter(row => row === route).length === 1)
    && rows.every(route => expectedRoutes.includes(route));
  const feedsRecent = coversExpectedRoutes(feeds.map(row => row.route)) && feeds.every(row => row.allRecent);
  const counts = { decisions: sumKnown(strategies.map(row => row.decisionCount)),
    attempts: sumKnown(strategies.map(row => row.orderAttemptCount)),
    fills: known(run?.fillCount) ? run.fillCount : null };
  const terminal = run?.status === 'TERMINATED' && run.terminalVerified === true
    && (run.naturalExitObserved === true || (run.workerRetirementRequired === false && run.workerLaunchState === 'NOT_ATTEMPTED' && run.workerAlive === false));
  const blockers = [...new Set([...(snapshot?.blockers || []), ...(run?.blockers || [])])].map(shortLiveBlockerLabel);
  const resultHasIssue = Boolean(run?.firstFailure || blockers.length || run?.unknownOrderCount > 0);
  let state = { tone: 'neutral', title: '아직 시작 전입니다', detail: '시험을 시작하면 봉 평가, 주문 시도와 체결 관측이 여기에 표시됩니다.' };
  if (terminal && !responseCurrent) state = { tone: 'neutral', title: '마지막 종료 기록을 표시합니다', detail: '저장된 종료 관측입니다. 현재 조회 연결은 미확인이며, 과거 종료 결과를 새 실행 상태로 간주하지 않습니다.' };
  else if (!responseCurrent) state = { tone: 'danger', title: received === null ? '실행 상태를 확인하고 있습니다' : '화면 갱신이 지연되고 있습니다', detail: pollError || '마지막 응답을 표시하고 있습니다. 현재 거래 진행 여부는 미확인입니다.' };
  else if (run) {
    if (terminal) state = { tone: resultHasIssue ? 'warning' : 'neutral', title: resultHasIssue ? '문제를 남기고 종료했습니다' : '종료 대조가 확인됐습니다', detail: run.firstFailure ? shortLiveBlockerLabel(run.firstFailure) : '실제 주문·체결 결과는 아래 관측 기록으로 확인하세요.' };
    else if (run.unknownOrderCount > 0) state = { tone: 'danger', title: '주문 결과 확인이 필요합니다', detail: '미확정 주문이 있습니다. 새 시험을 시작하지 말고 주문·체결 기록을 확인하세요.' };
    else if (blockers.length || run.firstFailure) state = { tone: 'danger', title: '진행을 막는 문제가 있습니다', detail: run.firstFailure ? shortLiveBlockerLabel(run.firstFailure) : blockers[0] };
    else if (run.newEntriesBlocked || ['STOPPING', 'DRAINED', 'TERMINATED'].includes(run.status)) state = { tone: 'warning', title: '신규 진입을 멈추고 종료를 확인합니다', detail: '예정 시간이 지나도 실제 주문 대조와 프로세스 종료가 확인될 때까지 완료가 아닙니다.' };
    else if (run.ownerAlive !== true || run.workerAlive !== true) state = { tone: 'warning', title: '실행 프로세스를 확인해야 합니다', detail: '화면 응답은 받았지만 작업자의 현재 생존 상태를 확인하지 못했습니다.' };
    else if (feeds.some(row => row.disconnected || row.delayed)) state = { tone: 'warning', title: '시세 수신 확인이 필요합니다', detail: '연결 소켓이 없거나 마지막 프레임 수신이 지연됐습니다. 화면 응답과 거래소 시세 연결은 별개입니다.' };
    else if (run.status === 'PENDING_FILL') state = { tone: 'warning', title: '주문 체결 결과를 기다립니다', detail: '주문 시도와 실제 체결은 다릅니다. 최근 활동과 체결 관측을 확인하세요.' };
    else if (run.status !== 'RUNNING') state = { tone: 'warning', title: '실행 상태 확인이 필요합니다', detail: `서버 상태: ${run.status || '미확인'}` };
    else if (counts.decisions === 0) state = { tone: feedsRecent ? 'active' : 'neutral', title: '첫 완성봉 평가를 기다립니다', detail: `${strategies.some(row => row.timeframe === '1h') ? '1시간봉은 첫 평가까지 1~2시간 걸릴 수 있습니다.' : '선택한 시간봉의 완성과 과거 봉 연결을 기다립니다.'} ${feedsRecent ? '최근 시세 프레임 수신이 관측됐습니다.' : '최근 시세 프레임 수신 여부는 아직 미확인입니다.'}` };
    else state = { tone: feedsRecent ? 'active' : 'neutral', title: feedsRecent ? '전략을 평가하며 다음 신호를 기다립니다' : '최근 전략 평가 기록을 표시합니다', detail: feedsRecent ? '최근 시세 프레임이 수신됐습니다. 신호와 주문 조건이 충족될 때만 거래합니다.' : '과거 평가 횟수만으로 현재 시세 연결을 확인할 수 없습니다. 아래 마지막 수신 시각을 확인하세요.' };
  }
  const started = stamp(run?.startedAt), ends = stamp(run?.endsAt);
  const terminalReported = run?.status === 'TERMINATED';
  const validEndpoint = value => value !== null && started !== null && value >= started && value <= now + 5000;
  let ended = null;
  if (terminal) {
    if (run.terminatedAt) {
      const declared = stamp(run.terminatedAt);
      if (validEndpoint(declared)) ended = declared;
    } else {
      // Older snapshots omit terminatedAt; only the session's real retirement event can anchor elapsed time.
      const kind = run.naturalExitObserved ? 'WORKER_DEATH_OBSERVED' : 'STARTUP_RETIRED_BEFORE_WORKER';
      const observed = (run.recentActivity || []).filter(event => event.kind === kind)
        .map(event => stamp(event.observedAt)).filter(validEndpoint);
      if (observed.length) ended = Math.min(...observed);
    }
  }
  const progressAt = terminalReported ? ended : now;
  const elapsedSeconds = validEndpoint(progressAt) ? Math.floor((progressAt - started) / 1000) : null;
  const progress = elapsedSeconds !== null && ends !== null && ends > started
    ? Math.min(100, Math.max(0, (progressAt - started) / (ends - started) * 100)) : null;
  const routes = brokers.map(row => {
    const equity = row.monitorEquity;
    const observed = stamp(row.accountObservedAt);
    const accountCurrent = !terminalReported && responseCurrent && observed !== null && now - observed <= staleMs && observed - now <= 5000;
    const equityKnown = equity?.basis === 'OBSERVED_NATIVE_EQUITY_CHANGE' && known(equity.baselineNative)
      && known(equity.currentNative) && known(equity.krwPerQuote) && equity.krwPerQuote > 0;
    const changeKrw = equityKnown ? (equity.currentNative - equity.baselineNative) * equity.krwPerQuote : null;
    const changePercent = equityKnown && equity.baselineNative > 0 ? (equity.currentNative / equity.baselineNative - 1) * 100 : null;
    return { ...row, equityKnown, changeKrw, changePercent, accountCurrent,
      basisLabel: equity?.preservedSpotExcluded ? '기존 보호 현물 제외 · 기준 계좌 평가 대비' : '기준 계좌 평가 대비 · 계좌 범위',
      observedAt: row.accountObservedAt, routeLabel: monitorRoute(row.broker) };
  });
  const activity = [...(run?.recentActivity || [])].filter(event => monitorEventLabels[event.kind] && stamp(event.observedAt) !== null && stamp(event.observedAt) - now <= 5000)
    .sort((a,b) => stamp(b.observedAt)-stamp(a.observedAt) || (b.sequence || 0)-(a.sequence || 0)).slice(0, 30);
  const timing = failureTiming(run || {}, ended === null ? null : new Date(ended).toISOString(), now);
  return { timing, state, resultHasIssue, responseCurrent, receivedAt, ageMs, terminal, blockers, counts, progress, elapsedSeconds, terminalReported, terminatedAt: ended === null ? null : new Date(ended).toISOString(), feeds, feedsRecent,
    allAccountsCurrent: coversExpectedRoutes(routes.map(row => row.broker)) && routes.every(row => row.accountCurrent),
    routes, strategies, activity, startedAt: run?.startedAt, endsAt: run?.endsAt,
    runStatus: run?.status, sessionId: run?.sessionId,
    totalChangeKrw: coversExpectedRoutes(routes.map(row => row.broker)) ? sumKnown(routes.map(row => row.changeKrw)) : null,
    failureDiagnostics: run?.firstFailure ? normalizeShortLiveFailureDiagnostics(run.firstFailureDiagnostics).map(monitorFailureDiagnostic) : [],
    firstFailure: run?.firstFailure ? shortLiveBlockerLabel(run.firstFailure) : null };
}
