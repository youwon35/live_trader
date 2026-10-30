import * as React from 'react';

const brokerLabel = id => ({kis:'한국투자증권',binance:'Binance 현물','binance-futures':'Binance 선물',upbit:'Upbit'})[id] || id;
const reasons = [
  ['DEPOSIT','입금 내역을 직접 확인함'], ['WITHDRAWAL','출금 내역을 직접 확인함'],
  ['TRANSFER','계좌·지갑 간 이체를 직접 확인함'], ['EXTERNAL_TRADE','직접 거래·수수료 내역을 확인함'],
  ['EXISTING_HOLDING','기존 보유 자산임을 확인함'], ['OTHER_REVIEWED','외부 변동을 확인했고 이 차이에 문제가 없음'],
];

function ReviewAction({issue,onReview}) {
  const [reason,setReason] = React.useState('');
  const [pending,setPending] = React.useState(false);
  const [error,setError] = React.useState('');
  if (!issue.reviewFingerprint || typeof onReview !== 'function') return null;
  const label = brokerLabel(issue.brokerId)+' '+(issue.symbol || issue.currency);
  const submit = async () => {
    if (pending || (!issue.reviewed && !reason)) return;
    setPending(true);setError('');
    try {
      const result=await onReview({fingerprint:issue.reviewFingerprint,
        reason:issue.reviewed ? issue.reviewReason : reason,
        action:issue.reviewed ? 'REOPEN' : 'ACKNOWLEDGE',confirmed:true});
      if (result?.ok === false) setError(result.reason || '확인 결과를 다시 읽어 주세요.');
    } catch {
      setError('저장 결과를 확인하지 못했습니다. 계좌 화면을 다시 확인하세요. 자동으로 재시도하지 않습니다.');
    } finally {setPending(false);}
  };
  return <div className="reconciliation-review-action">
    {issue.reviewed ? <span className="workflow-copy">사용자 확인 · {issue.reviewReasonLabel} · {issue.reviewedAt}</span> :
      <label>확인 사유 <select aria-label={label+' 확인 사유'} value={reason} disabled={pending} onChange={event=>setReason(event.target.value)}>
        <option value="">직접 확인한 사유 선택</option>
        {reasons.map(([value,text])=><option key={value} value={value}>{text}</option>)}
      </select></label>}
    <button type="button" className="secondary-button" disabled={pending || (!issue.reviewed && !reason)} onClick={submit}>
      {pending ? '기록 중…' : issue.reviewed ? '다시 표시' : '이 차이 확인 완료'}
    </button>
    {error && <span role="alert" className="workflow-copy">{error}</span>}
  </div>;
}

function ChangeRows({values,onReview}) {
  return <div className="ts-disclosure-body">{values.map((issue,index)=><div className="compact-row" key={issue.reviewFingerprint || issue.brokerId+':'+issue.kind+':'+issue.symbol+':'+index}>
    <strong>{brokerLabel(issue.brokerId)} · {issue.symbol || issue.currency} · {issue.reason}</strong>
    <span>{issue.comparison}</span><span>{issue.detail}</span>
    <ReviewAction issue={issue} onReview={onReview}/>
  </div>)}</div>;
}

export default function ReconciliationChanges({issues = [],onReview}) {
  const values=Array.isArray(issues) ? issues : [];
  const reviewed=values.filter(row=>row?.reviewed === true);
  const history=values.filter(row=>!row?.reviewed && (row?.presentationOnlyReference === true || row?.reviewFingerprint));
  const actionable=values.filter(row=>!row?.reviewed && row?.presentationOnlyReference !== true && !row?.reviewFingerprint);
  return <>
    {actionable.length > 0 && <details className="ts-disclosure" open>
      <summary className="ts-disclosure-summary">확인할 운용 차이 · {actionable.length}개</summary>
      <ChangeRows values={actionable}/>
    </details>}
    {history.length > 0 && <details className="ts-disclosure reconciliation-reference-changes">
      <summary className="ts-disclosure-summary" style={{color:'var(--moa-status-info, #3975dc)'}}>과거 조회·기존 보유 · 확인할 항목 {history.length}개</summary>
      <p className="workflow-copy">입출금·이체·직접 거래 내역을 확인했다면 각 항목을 확인 완료로 옮길 수 있습니다. API 입출금 증빙이나 운용 대조 통과로 처리하지 않으며, 계좌·금액·기준이 바뀌면 다시 표시합니다.</p>
      <ChangeRows values={history} onReview={onReview}/>
    </details>}
    {reviewed.length > 0 && <details className="ts-disclosure reconciliation-reviewed-changes">
      <summary className="ts-disclosure-summary" style={{color:'var(--moa-status-info, #3975dc)'}}>사용자가 확인한 과거 차이 · {reviewed.length}개</summary>
      <p className="workflow-copy">원본 차이와 확인 이력을 보존합니다. 원장 금액·실행 권한·현재 주문 및 포지션 검사는 바꾸지 않습니다.</p>
      <ChangeRows values={reviewed} onReview={onReview}/>
    </details>}
  </>;
}

