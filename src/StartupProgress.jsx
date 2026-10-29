import React,{useCallback,useEffect,useReducer,useRef,useState} from 'react';
import {STARTUP_STEPS,startupInitial,startupTransition,startupView} from './startupProgressModel.js';
import './StartupProgress.css';
export function useStartupProgress() {
  const [state,dispatch]=useReducer(startupTransition,undefined,startupInitial);
  const started=useRef(performance.now());
  const [now,setNow]=useState(()=>performance.now());
  useEffect(()=>{if(state.completedAt!==null)return;const timer=setInterval(()=>setNow(performance.now()),1000);return()=>clearInterval(timer);},[state.completedAt]);
  const begin=useCallback(id=>dispatch({type:'begin',id,at:performance.now()}),[]);
  const finish=useCallback((id,ok,detail)=>dispatch({type:ok?'done':'fail',id,detail,at:performance.now()}),[]);
  const view=startupView(state);
  return {...view,begin,finish,elapsedSeconds:Math.max(0,Math.floor(((state.completedAt??now)-started.current)/1000))};
}
export default function StartupProgress({view,onRetryConnection,onRetryPreferences,onRetryAccounts}) {
  const [expanded,setExpanded]=useState(false);
  const retry={connection:onRetryConnection,preferences:onRetryPreferences,accounts:onRetryAccounts};
  return <section className={`startup-progress ${view.ready?'is-ready':view.failed?'is-failed':'is-loading'}`} aria-label="시작 준비 진행">
    <div className="startup-progress-heading"><div><strong>{view.ready?'초기 조회 완료 · 화면 사용 가능':view.failed?'초기 조회에 확인이 필요합니다':'Live Trader 시작 준비 중'}</strong><p>{view.completed}/{view.total}단계 완료 · {view.elapsedSeconds}초 경과</p></div><div className="startup-progress-actions"><b>{view.percent}%</b>{view.ready&&<button type="button" className="secondary-button" aria-expanded={expanded} onClick={()=>setExpanded(value=>!value)}>{expanded?'접기':'준비 내역'}</button>}</div></div>
    <progress max="100" value={view.percent} aria-label="초기 조회 단계 완료율"/>
    {(!view.ready||expanded)&&<ol>{view.rows.map(row=><li key={row.id} data-status={row.status}><div><span>{row.label}</span><strong>{({pending:'대기',running:'진행 중',done:'완료',failed:'확인 필요'})[row.status]}</strong>{row.status==='failed'&&typeof retry[row.id]==='function'&&<button type="button" className="secondary-button" onClick={retry[row.id]}>다시 읽기</button>}</div>{row.detail&&<p>{row.detail}</p>}</li>)}</ol>}
    <small>진행률은 실제 완료한 조회 단계의 비율입니다. 주문 시작·계좌 대조 승인·시장 데이터 준비를 뜻하지 않습니다. 시작 전 필요한 검사는 해당 실행 화면에서 확인합니다.</small>
  </section>;
}
