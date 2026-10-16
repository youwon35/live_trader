import * as React from 'react';

const historicalLabel = (row) => row.status === 'WARNING' ? row.currentIdleWarning && !row.recentDeadlineMisses ? '최근 체결 공백이 시가 기한보다 김' : '기한 안 체결이 없는 구간 관측'
  : row.status === 'OBSERVED' ? '최근 관측 구간 내 체결 확인' : '관측 자료 부족';
export default function MarketGapPreparationPanel({ views = {}, onCancel, onContinue }) {
  return Object.values(views).filter((view) => !['COMPLETE', 'CANCELLED'].includes(view.phase)).map((view) => (
    <section key={view.scopeKey} className="panel" aria-label="종목별 최근 체결 공백" data-testid="market-gap-preparation">
      <strong>{view.label || '선택한 구성'} · 최근 체결 공백</strong>
      {view.phase === 'PREPARING' ? <p role="status" aria-live="polite">공개 체결 자료 확인 중… {view.completedMembers || 0}/{view.memberCount || '선택 종목'}</p>
        : view.phase === 'REVIEW' ? <>
          <p>최근 네 번의 5분봉 시작 구간만 살펴본 경고입니다. 앞으로의 검증 통과를 보장하지 않습니다.</p>
          {view.result?.members?.length ? <ul>{view.result.members.map((row) => <li key={row.strategyInstanceId}>
            <strong>{row.symbol}</strong> · {historicalLabel(row)}
            {row.sampleBoundaries?.[0]?.boundaryUtc && <p>관측한 봉 시작: {row.sampleBoundaries[0].boundaryUtc} ~ {row.sampleBoundaries.at(-1).boundaryUtc}</p>}
            {Number.isFinite(row.openingLimitSeconds) && <> · 원래 시가 기한 {row.openingLimitSeconds}초</>}
            {Number.isFinite(row.latestTrade?.idleSeconds) && <> · 최근 체결 후 {row.latestTrade.idleSeconds.toFixed(1)}초</>}
            {row.sixHourScenario && (row.recentDeadlineMisses > 0 ? <p>같은 누락이 6시간에 발생한다면 최대 관측률 {(row.sixHourScenario.upperBoundIfTheseMissesOccurred * 100).toFixed(2)}%입니다. 원래 기준은 {(row.sixHourScenario.coverageThreshold * 100).toFixed(0)}%입니다. 시가 기한을 놓친 판단은 관측률과 별개로 최종 검증을 막을 수 있습니다.</p> : <p>최근 일부 구간만으로 6시간 검증의 충족 여부를 판단할 수 없습니다. 원래 관측률 기준 {(row.sixHourScenario.coverageThreshold * 100).toFixed(0)}%를 유지합니다.</p>)}
          </li>)}</ul> : <p>최근 체결 자료를 확인하지 못했습니다. 유동성을 확인하거나 나중에 다시 준비해 주세요.</p>}
          <p>체결이 드문 종목은 구성을 검토하거나 더 활발한 시간대를 선택할 수 있습니다. 기존 기준은 변경되지 않습니다.</p>
          <button type="button" className="primary-button" onClick={() => onContinue(view.scopeKey)}>기존 시작 절차 계속</button>
        </> : <p role="status">{view.phase === 'CONTINUING' ? '기존 시작 조건을 확인하고 있습니다.' : '준비가 변경되거나 만료되었습니다. 현재 구성으로 다시 확인해 주세요.'}</p>}
      {['PREPARING', 'REVIEW'].includes(view.phase) && <button type="button" className="secondary-button" onClick={() => onCancel(view.scopeKey)}>준비 취소</button>}
    </section>
  ));
}
