export function coordinationRetirementView(snapshot) {
  const source = snapshot?.order_coordination_retirement;
  if (!source || typeof source !== 'object' || Array.isArray(source)) return null;
  const rows = Object.values(source).filter(row => row && typeof row === 'object');
  const retained = rows.filter(row => row.retained === true || row.reconciliationRequired === true);
  const released = rows.reduce((count, row) => count + (
    row.retained === false && row.reconciliationRequired === false &&
    Number.isInteger(row.releasedScopes) && row.releasedScopes > 0 ? row.releasedScopes : 0), 0);
  if (!retained.length && !released) return null;
  if (snapshot.api_connected !== true) return {pending:true,label:'종료 운용 주문 권한 · 현재 상태 확인 필요',details:[]};
  const details = retained.flatMap(row => Array.isArray(row.blockers) ? row.blockers : []).map(item => {
    const code = String(item?.reason || '');
    const reason = /NOT_FLAT/.test(code) ? '이 운용의 보유 수량이 남아 있습니다.'
      : /PENDING|NOT_TERMINAL|PROTECTION/.test(code) ? '주문·보호 주문의 종료를 더 확인해야 합니다.'
      : /IDENTITY/.test(code) ? '원래 주문과 조회 결과의 식별 정보 확인이 필요합니다.'
      : /STALE/.test(code) ? '최신 주문·계좌 조회가 필요합니다.'
      : '주문·체결·계좌 확인 자료가 부족합니다.';
    return `${item?.symbol || '종목 미확인'} · ${reason}`;
  });
  return {pending:retained.length > 0,label:retained.length
    ? `종료 운용 주문 권한 · ${retained.length}건 확인 필요`
    : `종료 운용 주문 권한 · ${released}개 경로 해제 확인`,details:[...new Set(details)]};
}
