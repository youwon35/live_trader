import React from 'react';
import {coordinationRetirementView} from './coordinationRetirement.js';

export default function CoordinationRetirementNote({snapshot}) {
  const view = coordinationRetirementView(snapshot);
  if (!view) return null;
  return <div aria-label="종료 운용 주문 권한 대조">
    <p style={{color:view.pending ? 'var(--warning-text)' : 'var(--success-text)'}}><strong>{view.label}</strong></p>
    {view.details.map(detail => <p key={detail}>{detail}</p>)}
    {view.pending && <p>계좌 다시 읽기로 재확인합니다. 확인 전에는 같은 계좌·종목을 다른 운용에 넘기지 않습니다.</p>}
  </div>;
}
