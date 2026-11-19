import React, {useEffect,useMemo,useState} from 'react';
import {Activity, ArrowUpRight, ClipboardList, Radio, Signal, Wallet} from 'lucide-react';
import {appendAccountObservations, projectContinuousDashboard, routeName, watchSignal} from './liveDashboardModel.js';

const numeric = new Intl.NumberFormat('ko-KR',{maximumFractionDigits:8});
const percent = new Intl.NumberFormat('ko-KR',{maximumFractionDigits:4, signDisplay:'exceptZero'});
const time = new Intl.DateTimeFormat('ko-KR',{timeZone:'Asia/Seoul',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});
const fmt = value => value==null?'—':numeric.format(value);
const at = value => value && Number.isFinite(Date.parse(value)) ? time.format(new Date(value)) : '미관측';
const tone = value => value==='BUY'?'success':value==='SELL'?'danger':'neutral';

function MonitorMetric({icon:Icon,label,value,note}) {
  return <div className="tower-metric"><span><Icon size={16} aria-hidden="true"/>{label}</span><strong>{value}</strong><small>{note}</small></div>;
}

function EventList({events, details=false}) {
  return <ol className={details?'tower-event-list tower-event-details':'tower-event-list'}>
    {events.map((event,index)=><li key={[event.kind,event.id,event.instance,index].join(':')}>
      <time dateTime={event.at}>{at(event.at)}</time>
      <div>
        <strong className={event.issue?'monitor-tone-danger':'monitor-tone-'+tone(event.signal)}>{event.label}</strong>
        <small>{[event.symbol,routeName(event.broker),event.timeframe,event.kind==='order'&&({BUY:'매수',SELL:'매도'})[String(event.side||'').toUpperCase()]].filter(Boolean).join(' · ')}</small>
        {details && <>
          {event.barAt && <small>평가에 쓴 봉 {at(event.barAt)} · 종가 {fmt(event.price)}</small>}
          {event.reason && <p>{event.reason}</p>}
          {event.diagnostics?.length>0 && <dl className="tower-signal-values">
            {event.diagnostics.map(({label,value})=><div key={label}><dt>{label}</dt><dd>{typeof value==='number'?fmt(value):value}</dd></div>)}
          </dl>}
          {event.action && <small>처리 결과 {event.action}</small>}
          {event.outcome && event.outcome!==event.reason && <p>{event.outcome}</p>}
          {!event.reason && !event.action && !event.outcome && <small>별도 사유 기록 없음</small>}
        </>}
      </div>
    </li>)}
  </ol>;
}

function AccountTrend({points, account}) {
  if (!points || points.length<2) return null;
  const values = points.map(point=>point.percent);
  const from = Date.parse(points[0].at);
  const to = Date.parse(points.at(-1).at);
  const low = Math.min(...values), spread = Math.max(...values)-low || 1;
  const groups = [];
  for (const point of points) {
    const tail = groups.at(-1);
    if (tail && Date.parse(point.at)-Date.parse(tail.at(-1).at)<=120000) tail.push(point);
    else groups.push([point]);
  }
  const coordinates = point => [4+(Date.parse(point.at)-from)/(to-from)*192, 36-(point.percent-low)/spread*31];
  return <svg className="tower-trend" viewBox="0 0 200 40" role="img" aria-label={routeName(account.broker)+' 계좌 변동률 관측 추이'}>
    {groups.map((group,index)=>group.length>1
      ? <polyline key={index} points={group.map(point=>coordinates(point).join(',')).join(' ')} />
      : <circle key={index} cx={coordinates(group[0])[0]} cy={coordinates(group[0])[1]} r="2.2" />)}
  </svg>;
}

export default function ContinuousMonitor({snapshot={}, onOpenExecution, visible=true}) {
  const [now,setNow]=useState(Date.now);
  const [detailsOpen,setDetailsOpen]=useState(false);
  const [observations,setObservations]=useState({});
  useEffect(()=>{
    if (!visible) return undefined;
    setNow(Date.now());
    const id=setInterval(()=>setNow(Date.now()),5000);
    return()=>clearInterval(id);
  },[visible]);
  const view=useMemo(()=>projectContinuousDashboard(snapshot,now),[snapshot,now]);
  useEffect(()=>{
    if (visible) setObservations(previous=>appendAccountObservations(previous,view.accounts));
  },[visible,view.accounts]);
  const title = !view.connected ? '현재 실행 상태를 확인할 수 없습니다'
    : !view.active ? '실행 중인 일반 운용이 없습니다'
    : view.issueCount ? '확인이 필요한 운용이 있습니다' : '전략 감시 중';
  const statusTone = !view.connected || view.issueCount ? 'warning' : view.active ? 'success' : 'neutral';
  const liveAccounts = view.accounts.filter(row=>row.current && row.percent!==null);
  const accountValue = liveAccounts.length===1 ? percent.format(liveAccounts[0].percent)+'%'
    : liveAccounts.length>1 ? '경로별 보기' : '—';
  return <section className="panel continuous-monitor control-tower" aria-label="일반 운용 실시간 감시">
    <header className="monitor-workspace-header">
      <h2><Activity size={22} aria-hidden="true"/>실행 중인 전략과 거래소</h2>
      <button className="secondary-button" type="button" onClick={onOpenExecution}>운용 설정 <ArrowUpRight size={15} aria-hidden="true"/></button>
    </header>
    <div className={'tower-state monitor-tone-'+statusTone} role="status">
      <Radio size={21} aria-hidden="true"/>
      <strong>{title}</strong>
      <span>{view.connected ? (view.active ? view.active+'개 경로 실행 중' : view.watch.length ? '최근 종료 기록' : '실행 대기') : '마지막 수신 자료'}
        {view.issueCount ? ' · '+view.issueCount+'개 경로 확인 필요' : ''}</span>
    </div>
    <div className="tower-metrics">
      <MonitorMetric icon={Radio} label="감시 종목" value={view.connected && view.active ? fmt(view.symbols) : view.connected ? '0' : '—'} note="종목·거래 경로 기준"/>
      <MonitorMetric icon={Wallet} label="계좌 평가 변동률" value={accountValue} note={liveAccounts.length===1?routeName(liveAccounts[0].broker)+' · 일일 기준 대비':'경로·통화별 관측'}/>
      <MonitorMetric icon={Signal} label="완성봉 평가" value={fmt(view.evaluations)} note={view.active ? '실행 경로 합계' : '최근 운용 기록'}/>
      <MonitorMetric icon={ClipboardList} label="미결 주문" value={fmt(view.pending)} note="일반 주문 원장 기준"/>
    </div>
    <div className="tower-grid">
      <section className="tower-panel" aria-label="감시 종목">
        <div className="tower-heading"><h3>감시 종목</h3>{view.watch.length>0&&<span>{view.watch.length}개 전략</span>}</div>
        {view.watch.length ? <div className="monitor-table-scroll"><table className="monitor-market-table tower-watch">
          <thead><tr><th scope="col">종목 · 경로</th><th scope="col">최근 완성봉 종가</th><th scope="col">최근 신호</th><th scope="col">전략 상태</th></tr></thead>
          <tbody>{view.watch.map(row=><tr key={row.key}>
            <td><strong>{row.symbol||'종목 미확인'}</strong><small>{[row.brokerLabel,row.timeframe].filter(Boolean).join(' · ')}</small></td>
            <td>{fmt(row.lastPrice)}{row.barAt&&<small>{at(row.barAt)} 마감</small>}</td>
            <td><span className={'tower-chip monitor-tone-'+tone(row.signal)}>{watchSignal(row.signal)}</span>{row.decisionAt&&<small>{at(row.decisionAt)}</small>}</td>
            <td className={'monitor-tone-'+(row.issue?'warning':row.tone)}>{row.issue?'최근 처리 확인 필요':row.runtimeState}</td>
          </tr>)}</tbody>
        </table></div> : <p className="tower-empty">일반 운용을 시작하면 전략이 감시하는 종목과 최근 신호가 표시됩니다.</p>}
      </section>
      <section className="tower-panel" aria-label="계좌 변동">
        <div className="tower-heading"><h3>계좌 변동</h3><span>일일 기준 대비</span></div>
        {view.accounts.length ? <ul className="tower-account-list">{view.accounts.map(row=><li key={row.broker+':'+row.currency}>
          <div><strong>{routeName(row.broker)} · {row.currency}</strong><small>{row.current?'계좌 관측':'마지막 관측'} {at(row.at)}</small></div>
          <div><strong className={'monitor-tone-'+(row.change>0?'success':row.change<0?'danger':'neutral')}>{row.percent===null?'—':percent.format(row.percent)+'%'}</strong><small>{fmt(row.change)} {row.currency}</small></div>
          <AccountTrend points={observations[row.broker+':'+row.currency]} account={row}/>
        </li>)}</ul> : <p className="tower-empty">감시 경로의 계좌 기준값과 관측값이 확인되면 표시됩니다.</p>}
        <p className="monitor-chart-note">입출금이 반영될 수 있는 계좌 기준입니다.</p>
      </section>
    </div>
    <div className="tower-grid">
      <section className="tower-panel" aria-label="최근 신호와 주문">
        <div className="tower-heading"><h3>최근 신호 · 주문</h3>{view.recent.length>0&&<span>최근 {view.recent.length}건</span>}</div>
        {view.recent.length ? <EventList events={view.recent}/> : <p className="tower-empty">최근 매수·매도 신호나 주문 기록이 없습니다.</p>}
      </section>
      <section className="tower-panel" aria-label="경로별 수신">
        <div className="tower-heading"><h3>경로별 수신</h3></div>
        {view.visibleRows.length ? <ul className="tower-account-list">{view.visibleRows.map((row,index)=><li key={row.runtimeId||row.deploymentId||index}>
          <div><strong>{row.brokerLabel}</strong><small>{({MONITOR:'관찰 · 주문 없음',SMALL_LIVE:'제한 실거래',FULL_LIVE:'실전 운용'})[row.mode]||'모드 미확인'}</small></div>
          <div><strong className={'monitor-tone-'+row.tone}>{row.state}</strong><small>엔진 {at(row.lastHeartbeat)} · 봉 {at(row.lastDataAt)}</small></div>
        </li>)}</ul> : <p className="tower-empty">표시할 수신 경로가 없습니다.</p>}
      </section>
    </div>
    <details className="tower-diagnostics" onToggle={event=>setDetailsOpen(event.currentTarget.open)}>
      <summary>판단 · 주문 상세 로그 <span>평가에 사용한 봉, 판단 근거와 차단 사유</span></summary>
      {detailsOpen && <>
        {view.visibleRows.some(row=>row.issue) && <ul className="tower-issues">{view.visibleRows.filter(row=>row.issue).map((row,index)=><li key={row.runtimeId||index}>{row.brokerLabel} · {String(row.issue)}</li>)}</ul>}
        {view.details.length ? <EventList events={view.details} details/> : <p className="tower-empty">아직 기록된 상세 근거가 없습니다.</p>}
      </>}
    </details>
  </section>;
}
