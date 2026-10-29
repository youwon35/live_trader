export const STARTUP_STEPS = Object.freeze([
  {id:'connection',label:'프로그램 서버 연결'},
  {id:'preferences',label:'저장된 화면 설정 읽기'},
  {id:'accounts',label:'초기 계좌 자료 읽기'},
]);
export const startupInitial = () => ({steps:Object.fromEntries(STARTUP_STEPS.map(row=>[row.id,{status:'pending',detail:''}])),completedAt:null});
export function startupTransition(state,event) {
  if(state.completedAt!==null || !Number.isFinite(event?.at) || event.at<0 || !STARTUP_STEPS.some(row=>row.id===event.id))return state;
  const previous=state.steps[event.id];
  if(previous.status==='done')return state;
  const status={begin:'running',done:'done',fail:'failed'}[event.type];
  if(!status)return state;
  const steps={...state.steps,[event.id]:{status,detail:event.detail||''}};
  const ready=STARTUP_STEPS.every(row=>steps[row.id].status==='done');
  return {steps,completedAt:ready?event.at:null};
}
export function startupView(state) {
  const rows=STARTUP_STEPS.map(row=>({...row,...state.steps[row.id]}));
  const completed=rows.filter(row=>row.status==='done').length;
  return {rows,completed,total:rows.length,percent:Math.floor(completed/rows.length*100),ready:state.completedAt!==null,failed:rows.some(row=>row.status==='failed')};
}
export function startupAccountObservation(result) {
  const summary=result?.snapshot?.reconciliation?.summary;
  const accounts=result?.snapshot?.reconciliation?.accounts;
  if(typeof result?.ok !== 'boolean' || !Array.isArray(accounts) || !summary || accounts.length !== summary.account_count || !accounts.every(row=>row && typeof row === 'object' && !Array.isArray(row)) || !Number.isInteger(summary.account_count) || summary.account_count<1 || !Number.isFinite(Date.parse(summary.last_run||'')))return {ok:false,detail:'계좌 조회 결과를 확인하지 못했습니다. 연결·설정에서 등록 상태를 확인하세요.'};
  if(summary.error_count!==0 || summary.api_required_count!==0)return {ok:false,detail:'일부 계좌 조회가 미완료입니다. 계좌·잔고의 개별 결과를 확인하세요.'};
  // Startup reports completed reads, never reconciliation approval or trading authority.
  return {ok:true,detail:'계좌 자료를 읽었습니다. 대조 결과와 주문 허용 조건은 해당 화면에서 별도로 확인합니다.'};
}
