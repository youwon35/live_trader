import * as React from 'react';

function ChangeRows({values}) {
  return <div className="ts-disclosure-body">{values.map((issue,index)=><div className="compact-row" key={`${issue.brokerId}:${issue.kind}:${issue.symbol}:${index}`}>
    <strong>{({kis:'한국투자증권',binance:'Binance 현물','binance-futures':'Binance 선물',upbit:'Upbit'})[issue.brokerId] || issue.brokerId} · {issue.symbol || issue.currency} · {issue.reason}</strong>
    <span>{issue.comparison}</span><span>{issue.detail}</span>
  </div>)}</div>;
}

export default function ReconciliationChanges({issues = []}) {
  const values=Array.isArray(issues) ? issues : [];
  const history=values.filter(row=>row?.presentationOnlyReference === true);
  const actionable=values.filter(row=>row?.presentationOnlyReference !== true);
  return <>
    {actionable.length > 0 && <details className="ts-disclosure" open>
      <summary className="ts-disclosure-summary">확인할 운용 차이 · {actionable.length}개</summary>
      <ChangeRows values={actionable}/>
    </details>}
    {history.length > 0 && <details className="ts-disclosure reconciliation-reference-changes">
      <summary className="ts-disclosure-summary" style={{color:'var(--moa-status-info, #3975dc)'}}>과거 조회·기존 보유 · 참고 {history.length}개</summary>
      <p className="workflow-copy">과거 조회와 현재 잔고의 변화는 현재 주문·체결 오류와 구분합니다. 입출금 내역을 확인한 결과는 아니며, 앱 운용 손익으로 계산하거나 원장을 자동 수정하지 않습니다.</p>
      <ChangeRows values={history}/>
    </details>}
  </>;
}
