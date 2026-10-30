import React from 'react';
import {sanitizeShortLiveText, SHORT_LIVE_ROUTES} from './shortLiveTestModel.js';

const outcomes = {NOT_EVALUATED: '평가 봉 없음', NO_ORDER_ATTEMPT: '평가 완료 · 주문 시도 없음',
  NO_FILL_OBSERVED: '주문 시도 · 체결 미관측', FILLS_OBSERVED: '실제 체결 관측'};

export default function OperatingSummary({report}) {
  if (!report || report.retirementVerified !== true) return null;
  const rows = Array.isArray(report.strategies) ? report.strategies : [];
  return <details className="ts-disclosure">
    <summary className="ts-disclosure-summary"><strong>이번 시험 결과</strong><span>{report.firstFailure ? '오류 기록 있음' : report.orderAttempts ? '주문 관측 기록 있음' : '주문 없이 종료'}</span></summary>
    <div className="ts-disclosure-body">
      <p>종료 대조 완료 · 주문 시도 {report.orderAttempts}회 · 체결 관측 {report.fillObservations}회</p>
      {report.firstFailure && <p className="short-live-error">최초 문제: {sanitizeShortLiveText(report.firstFailure)}</p>}
      <ul>{rows.map(row => <li key={row.instanceId}><strong>{sanitizeShortLiveText(row.symbol)} · {SHORT_LIVE_ROUTES.find(route => route.id === row.route)?.label}</strong> · {outcomes[row.outcome] || '결과 미확인'} · 평가 {row.evaluations}회</li>)}</ul>
      <p className="short-live-meta">{report.fillObservations ? `수수료 ${report.feesKnown ? '기록된 체결 비용 확인됨' : '미확인 비용 있음'}` : '체결 관측 없음'} · 주문이 없거나 체결이 있었다는 사실만으로 전략의 수익성·운용 자격을 판정하지 않습니다.</p>
    </div>
  </details>;
}
