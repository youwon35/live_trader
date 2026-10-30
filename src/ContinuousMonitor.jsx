import React, {useEffect,useMemo,useState} from 'react';
import {Activity,ArrowUpRight,Radio} from 'lucide-react';
import {projectContinuousDashboard} from './liveDashboardModel.js';
const n = value => value===null || value===undefined?'—':new Intl.NumberFormat('ko-KR').format(value);
const at = value => value && Number.isFinite(Date.parse(value))?new Date(value).toLocaleTimeString('ko-KR',{timeZone:'Asia/Seoul'}):'미관측';
export default function ContinuousMonitor({snapshot={},onOpenExecution}) {
  const [now,setNow]=useState(Date.now);
  useEffect(()=>{const id=setInterval(()=>setNow(Date.now()),1000);return()=>clearInterval(id);},[]);
  const view=useMemo(()=>projectContinuousDashboard(snapshot,now),[snapshot,now]);
  return <section className="panel continuous-monitor" aria-label="일반 운용 실시간 감시">
    <header className="monitor-workspace-header"><div><span className="live-monitor-eyebrow">LIVE TRADER · 일반 운용</span><h2><Activity size={22}/> 실행 중인 전략과 거래소</h2></div><button className="secondary-button" onClick={onOpenExecution}>운용 설정 <ArrowUpRight size={15}/></button></header>
    <div className="monitor-runtime-summary"><strong>{view.connected ? view.active ? `${view.active}개 운용 실행 중` : '실행 중인 일반 운용이 없습니다' : '현재 실행 상태를 확인할 수 없습니다'}</strong><span>{view.issueCount?`${view.issueCount}개 경로 확인 필요`:'경로별 엔진 응답과 봉 수신 시각을 확인하세요'}</span></div>
    {!view.rows.length?<p className="live-monitor-empty">운용 전략을 시작하면 거래소와 종목이 자동으로 이 화면에 모입니다. 주문 연결 시험은 위의 ‘주문 연결 시험’ 탭에서 확인할 수 있습니다.</p>:<div className="monitor-runtime-grid">{view.rows.map((row,i)=><article key={row.runtimeId||i}>
      <div className="monitor-runtime-title"><strong>{row.brokerLabel}</strong><span className={`monitor-tone-${row.tone}`}><Radio size={13}/> {row.state}</span></div>
      <p>{row.portfolioId || row.requestedStrategyId || row.deploymentId || '전략 식별 정보 미확인'}</p><small>{row.mode==='MONITOR'?'관찰 · 주문 없음':row.mode==='SMALL_LIVE'?'제한 실거래':row.mode==='FULL_LIVE'?'실전 운용':'모드 미확인'}</small>
      <dl><div><dt>완성봉</dt><dd>{n(row.bars)}</dd></div><div><dt>전략 평가</dt><dd>{n(row.decisions)}</dd></div><div><dt>관측 종목</dt><dd>{row.symbols.length || '—'}</dd></div></dl>
      <p>{row.symbols.join(' · ') || '종목 목록 미확인'}</p><div className="monitor-runtime-times"><span>엔진 응답 {at(row.lastHeartbeat)}</span><span>봉 경계 수신 {at(row.lastDataAt)}</span><span>마지막 평가 {at(row.lastCycleAt)}</span></div>
      {row.strategies.length>0&&<div className="continuous-strategy-observations">{row.strategies.map((strategy,index)=><section key={strategy.id||index}>
        <div className="monitor-runtime-title"><strong>{strategy.symbol} · {strategy.timeframe}</strong><span className={strategy.signal==='BUY'?'monitor-tone-success':strategy.signal==='SELL'?'monitor-tone-danger':'monitor-tone-neutral'}>{({BUY:'매수 신호',SELL:'매도 신호',HOLD:'신호 대기'})[strategy.signal] || strategy.signal || '아직 판단 미관측'}</span></div>
        <p>{strategy.plugin || '전략 식별 미확인'} · 완성봉 종가 {n(strategy.lastPrice)}</p><small>봉 마감 {at(strategy.barAt)} · 판단 {at(strategy.decisionAt)}</small>
        {strategy.reason&&<p>{strategy.reason}</p>}
      </section>)}</div>}
      <p className="monitor-chart-note">엔진 응답은 시세 연결의 실시간 정상 여부를 보장하지 않습니다. 봉 경계 수신은 완성봉 또는 새 봉 시작 관측 시각입니다. 전략 신호는 주문·체결이 아닙니다. 실제 계좌 자산과 체결은 계좌·잔고 및 주문·체결 탭에서 확인하세요.</p>
      {row.issue&&<p role="status" className="monitor-tone-danger">{String(row.issue)}</p>}
    </article>)}</div>}
  </section>;
}
