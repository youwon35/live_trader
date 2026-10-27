import React, {useEffect,useRef,useState} from 'react';
import {ArrowUpRight,Layers,Play,RefreshCw} from 'lucide-react';
import {prepareRuntimeBatch,getRuntimeBatchStatus} from './api.js';
import {routeName} from './liveDashboardModel.js';
import './MonitorWorkspace.css';

const label = state => ({PREPARED:'시작 전',STARTED:'시작 요청 완료',RUNNING:'실행 중',BLOCKED:'차단',UNKNOWN:'결과 미확인',STARTING:'시작 확인 중',NOT_STARTED:'미시작',EXPIRED:'유효시간 만료'})[state] || state || '미확인';
export default function RuntimeBatchLauncher({options=[],onStartMember,onOpenMonitor}) {
  const [ids,setIds]=useState([]), [mode,setMode]=useState('MONITOR');
  const [batch,setBatch]=useState(null), [busy,setBusy]=useState(false), [consent,setConsent]=useState(false);
  const [attempted,setAttempted]=useState(false),[error,setError]=useState(''),[now,setNow]=useState(Date.now);
  const flight=useRef(false);
  const [uncertainMember,setUncertainMember]=useState(null);
  useEffect(()=>{const timer=setInterval(()=>setNow(Date.now()),1000);return()=>clearInterval(timer);},[]);
  const chosen=options.filter(option=>ids.includes(option.id));
  const expired=!batch || !Number.isFinite(Date.parse(batch.expiresAt)) || Date.parse(batch.expiresAt)<=now;
  const uncertain=Boolean(uncertainMember) || batch?.members?.some(member=>['UNKNOWN','STARTING'].includes(member.status));
  const invalidate=()=>{setUncertainMember(null);setBatch(null);setConsent(false);setAttempted(false);setError('');};
  function observeBatch(next) {
    if (!next) return;
    setBatch(next);
    if (uncertainMember && next.members?.some(member=>member.memberId===uncertainMember && member.attempted===true && ['RUNNING','STARTED','BLOCKED','FAILED'].includes(member.status))) setUncertainMember(null);
  }
  async function prepare() {
    if(flight.current || !chosen.length || uncertain) return;
    flight.current=true;setBusy(true);invalidate();
    try {
      const result=await prepareRuntimeBatch(chosen.map(({context,profileId})=>({profile_id:profileId,mode,
        portfolio_id:context.portfolioId,deployment_id:context.id,strategy_id:context.strategyId})));
      if(result.batch)setBatch(result.batch);
      if(result.ok!==true)setError(result.reason || '선택한 구성의 연결을 확인하지 못했습니다.');
    } catch(cause){setError(cause.message || '구성 확인 실패');}
    finally{flight.current=false;setBusy(false);}
  }
  async function start() {
    if(flight.current || expired || attempted || (mode!=='MONITOR'&&!consent))return;
    flight.current=true;setBusy(true);setAttempted(true);setError('');
    let current=batch,awaitingMember=null;
    try {
      for(const member of batch.members || []) {
        const option=chosen.find(item=>item.context.id===member.selection?.deployment_id);
        if(!option) {setError('선택한 배포 구성이 변경되었습니다. 시작하지 않았습니다.');break;}
        awaitingMember=member.memberId;
        const result=await onStartMember(option.profileId,mode,option.context,
          {batchId:batch.batchId,scopeHash:batch.scopeHash,memberId:member.memberId});
        if(result?.batch){current=result.batch;setBatch(current);awaitingMember=null;}
        if(result?.outcomeUnknown===true) {
          setUncertainMember(member.memberId);
          setError('시작 응답이 확인되지 않았습니다. 동일 실행 결과를 조회하기 전에는 새 구성을 시작하지 않습니다.');
          break;
        }
        if(result?.ok!==true){setError(result?.reason || '시작 조건을 확인하지 못해 나머지 경로는 시작하지 않았습니다.');break;}
      }
      const status=await getRuntimeBatchStatus(batch.batchId);
      if(status.batch)setBatch(status.batch);
    } catch(cause){
      // A lost response never authorizes a second start or a different member.
      if(awaitingMember)setUncertainMember(awaitingMember);
      setBatch({...current,members:(current.members||[]).map(member=>member.memberId===awaitingMember?{...member,status:'UNKNOWN'}:member)});
      setError(`결과를 확인할 수 없습니다. 자동 재시도하지 않습니다. ${cause.message || ''}`);
    } finally{flight.current=false;setBusy(false);}
  }
  async function refresh() {
    if(!batch || flight.current)return;
    flight.current=true;setBusy(true);
    try{const result=await getRuntimeBatchStatus(batch.batchId);if(result.batch)observeBatch(result.batch);}
    catch(cause){setError(cause.message || '현재 상태 조회 실패');}
    finally{flight.current=false;setBusy(false);}
  }
  return <section className="panel runtime-batch-launcher" aria-label="여러 거래소 함께 실행">
    <header className="monitor-workspace-header"><div><span className="live-monitor-eyebrow">전략 선택 → 거래소 자동 연결</span><h2><Layers size={21}/> 여러 거래소 함께 실행</h2></div>
      <button type="button" className="secondary-button" onClick={onOpenMonitor}>실시간 감시 <ArrowUpRight size={15}/></button></header>
    <p className="monitor-chart-note">전략에 연결된 거래소와 계좌를 사용합니다. 경로별 자금·손익·운용은 분리하고 감시 화면에 함께 표시합니다. 이 기능은 일반 운용이며 6시간 주문 연결 시험의 시간·금액 조건을 대신하지 않습니다.</p>
    <div className="runtime-batch-options">{options.map(option=><label key={option.id} className="runtime-batch-option">
      <input type="checkbox" checked={ids.includes(option.id)} disabled={busy||uncertain||option.running||!option.profileId} onChange={event=>{invalidate();setIds(current=>event.target.checked?[...current,option.id]:current.filter(id=>id!==option.id));}}/>
      <span><strong>{option.context.name}</strong><small>{routeName(option.context.brokerId)} · {option.context.symbol} · {option.context.timeframe}{option.running?' · 이미 실행 중':''}</small></span>
    </label>)}{!options.length&&<p className="live-monitor-empty">운용 가능한 전략을 먼저 연결해 주세요.</p>}</div>
    <label className="runtime-batch-mode">실행 방식 <select value={mode} disabled={busy||uncertain} onChange={event=>{invalidate();setMode(event.target.value);}}>
      <option value="MONITOR">관찰 · 주문 없음</option><option value="SMALL_LIVE">제한 실거래</option><option value="FULL_LIVE">실전 운용</option></select></label>
    <div className="monitor-workspace-actions"><button type="button" disabled={!chosen.length||busy||uncertain} onClick={prepare}>선택 {chosen.length}개 구성 확인 · 주문 없음</button></div>
    {batch&&<div className="runtime-batch-review"><p>구성 연결을 확인했습니다. 계좌·시장·운용 한도는 시작할 때 각 경로에서 다시 검사합니다.</p>
      <div className="runtime-batch-member-list">{batch.members?.map(member=><div key={member.memberId}><strong>{routeName(member.brokerId)} · {member.capitalCurrency || '통화 미확인'}</strong><span className={`monitor-tone-${['BLOCKED','UNKNOWN'].includes(member.status)?'danger':member.status==='RUNNING'?'success':'neutral'}`}>{label(member.status)}{member.reasonCode?` · ${member.reasonCode}`:''}</span></div>)}</div>
      {mode!=='MONITOR'&&<label className="runtime-batch-consent"><input type="checkbox" checked={consent} disabled={busy||attempted} onChange={event=>setConsent(event.target.checked)}/>선택한 각 계좌에서 실제 주문과 손실이 발생할 수 있음을 확인했습니다.</label>}
      <p className="monitor-chart-note">선택한 순서대로 시작하며, 한 경로가 차단되거나 응답이 불명확하면 이후 경로는 시작하지 않습니다. 이미 시작한 운용은 자동 취소되지 않습니다. 전역 신규진입 차단과 각 운용 상태를 확인하고, ‘운용 설정’에서 해당 전략을 선택해 중지할 수 있습니다. 한국투자증권은 별도 계좌 확인창이 표시될 수 있습니다.</p>
      <div className="monitor-workspace-actions"><button type="button" disabled={busy||expired||attempted||(mode!=='MONITOR'&&!consent)} onClick={start}><Play size={15}/>{busy?'확인 중…':mode==='MONITOR'?'선택한 전략 함께 관찰':'선택한 전략 실거래 시작'}</button>
        <button type="button" className="secondary-button" disabled={busy} onClick={refresh}><RefreshCw size={15}/> 실행 결과 조회</button></div>
      {!attempted&&expired&&<p className="monitor-tone-warning">구성 확인 시간이 만료되었습니다. 다시 구성 확인을 해 주세요.</p>}
    </div>}
    {uncertainMember&&<p className="monitor-tone-warning">시작 결과 미확인 · 새 구성 준비와 재시작이 잠겨 있습니다. 실행 결과 조회로 동일 요청을 확인하세요.</p>}
    {error&&<p role="alert" className="monitor-tone-danger">{error}</p>}
  </section>;
}
