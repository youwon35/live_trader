import React, { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { startupInitial, startupTransition, startupView } from './startupProgressModel.js';
import './StartupProgress.css';

const statusLabels = {
  pending: '대기',
  running: '확인 중',
  done: '완료',
  failed: '읽기 실패',
  review: '확인 필요',
};

const actionLabels = {
  settings: '설정 열기',
  gate: '운용 전략 열기',
  accounts: '계좌·잔고 열기',
};

export function useStartupProgress() {
  const [state, dispatch] = useReducer(startupTransition, undefined, startupInitial);
  const started = useRef(performance.now());
  const [now, setNow] = useState(() => performance.now());

  useEffect(() => {
    if (state.completedAt !== null) return;
    const timer = setInterval(() => setNow(performance.now()), 1000);
    return () => clearInterval(timer);
  }, [state.completedAt]);

  const begin = useCallback(id => dispatch({ type: 'begin', id, at: performance.now() }), []);
  const finish = useCallback((id, ok, detail, { review = false, route } = {}) => {
    dispatch({ type: ok ? review ? 'review' : 'done' : 'fail', id, detail, route, at: performance.now() });
  }, []);

  return {
    ...startupView(state),
    begin,
    finish,
    elapsedSeconds: Math.max(0, Math.floor(((state.completedAt ?? now) - started.current) / 1000)),
  };
}

export default function StartupProgress({ view, onRetryConnection, onRetryPreferences, onRetryAccounts, onNavigate }) {
  const [expanded, setExpanded] = useState(false);
  const retry = { connection: onRetryConnection, preferences: onRetryPreferences, accounts: onRetryAccounts };
  const hasIssues = view.failed || view.review;
  const showRows = !view.ready || hasIssues || expanded;
  const title = hasIssues ? '초기 점검에 확인이 필요합니다' : view.ready ? '초기 점검 완료' : 'Live Trader 초기 점검 중';

  return <section className={`startup-progress ${view.failed ? 'is-failed' : view.review ? 'is-review' : view.ready ? 'is-ready' : 'is-loading'}`}
    aria-label="Live Trader 시작 Doctor">
    <div className="startup-progress-heading">
      <div>
        <strong role="status">{title}</strong>
        <p>{view.completed}/{view.total}단계 완료 · {view.elapsedSeconds}초 경과</p>
      </div>
      <div className="startup-progress-actions">
        <b>{view.percent}%</b>
        {view.ready && !hasIssues && <button type="button" className="secondary-button"
          aria-expanded={expanded} onClick={() => setExpanded(value => !value)}>{expanded ? '접기' : '점검 내역'}</button>}
      </div>
    </div>
    <progress max="100" value={view.percent} aria-label="초기 점검 단계 완료율" />
    {showRows && <ol>{view.rows.map(row => {
      const needsAction = row.status === 'failed' || row.status === 'review';
      const retryStep = retry[row.id];
      return <li key={row.id} data-status={row.status}>
        <div>
          <span>{row.label}</span>
          <strong>{statusLabels[row.status]}</strong>
          {needsAction && typeof retryStep === 'function' && <button type="button" className="secondary-button"
            aria-label={`${row.label} 다시 조회`} onClick={retryStep}>다시 조회</button>}
          {needsAction && typeof onNavigate === 'function' && <button type="button" className="secondary-button"
            onClick={() => onNavigate(row.route)}>{actionLabels[row.route]}</button>}
        </div>
        {row.detail && <p>{row.detail}</p>}
      </li>;
    })}</ol>}
    <small>실거래를 시작하기 전에는 아래 실거래 시작 점검을 실행하세요.</small>
  </section>;
}
