import React, {useMemo, useState} from 'react';
import {TrendingUp} from 'lucide-react';
import {equitySeries} from './liveDashboardModel.js';
import {monitorRoute} from './realtimeMonitorModel.js';

const fmt = value => new Intl.NumberFormat('ko-KR',{maximumFractionDigits:4}).format(value);
const time = value => new Date(value).toLocaleTimeString('ko-KR',{timeZone:'Asia/Seoul',hour:'2-digit',minute:'2-digit'});
export default function MonitorEquityChart({history=[], routes=[]}) {
  const [selected, setSelected] = useState('');
  const route = routes.includes(selected) ? selected : routes[0] || '';
  const points = useMemo(()=>equitySeries(history,route),[history,route]);
  const last = points.at(-1);
  const min = Math.min(0,...points.map(p=>p.value)), max = Math.max(0,...points.map(p=>p.value));
  const span = max-min || 1, start = points[0]?.at || 0, end = last?.at || start+1;
  const xy = p => [56+(p.at-start)/Math.max(1,end-start)*638, 220-(p.value-min)/span*172];
  const segments = [];
  for (const p of points) {
    const tail = segments.at(-1);
    // Long observation gaps stay visible rather than interpolating a false live curve.
    if (!tail || p.at-tail.at(-1).at>120000) segments.push([p]); else tail.push(p);
  }
  return <section className="monitor-equity-chart" aria-label="계좌 평가 변화 그래프">
    <div className="monitor-chart-header"><div><span className="live-monitor-eyebrow">실제 계좌 관측</span><h3><TrendingUp size={18}/> 계좌 평가 변화</h3></div>
      <label>거래 경로<select aria-label="그래프 거래 경로" value={route} onChange={event=>setSelected(event.target.value)} disabled={!routes.length}>
        {!routes.length && <option value="">관측 대기</option>}{routes.map(id=><option key={id} value={id}>{monitorRoute(id)}</option>)}
      </select></label></div>
    <div className="monitor-chart-total"><strong className={last?.value>0?'is-positive':last?.value<0?'is-negative':''}>{last?`${last.value>0?'+':''}${fmt(last.value)}`:'—'}</strong><span>{last?.currency || '평가 자료 대기'}</span></div>
    {points.length<2 ? <div className="monitor-chart-empty"><TrendingUp size={32}/><strong>{points.length?'첫 계좌 관측을 받았습니다':'아직 계좌 평가 이력이 없습니다'}</strong><p>실제 관측이 두 번 이상 쌓이면 그래프가 표시됩니다.</p></div> : <svg viewBox="0 0 730 260" role="img" aria-label={`${monitorRoute(route)} 계좌 평가 변화, ${points.length}개 실제 관측, 마지막 ${fmt(last.value)} ${last.currency}`}>
      {[0,.5,1].map(t=><g key={t}><line x1="56" x2="694" y1={48+t*172} y2={48+t*172}/><text x="48" y={52+t*172} textAnchor="end">{fmt(max-t*(max-min))}</text></g>)}
      {segments.map((part,i)=><polyline key={i} points={part.map(p=>xy(p).join(',')).join(' ')} className={last.value<0?'is-negative':'is-positive'}/>)}
      {segments.filter(part=>part.length===1).map((part,i)=><circle key={i} cx={xy(part[0])[0]} cy={xy(part[0])[1]} r="3"/>)}
      <circle cx={xy(last)[0]} cy={xy(last)[1]} r="4"/>
      <text x="56" y="248">{time(start)}</text><text x="694" y="248" textAnchor="end">{time(end)}</text>
    </svg>}
    <p className="monitor-chart-note">경로별 원래 통화로 표시합니다. 계좌 평가 변화에는 외부 자금 변동이 포함될 수 있으며 전략 확정 수익과 다릅니다. 관측 공백은 선을 끊어 표시합니다.</p>
  </section>;
}
