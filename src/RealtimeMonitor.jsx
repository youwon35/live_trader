import React, { useEffect, useMemo, useState } from 'react';
import { Activity, AlertTriangle, ArrowDownLeft, ArrowUpRight, CheckCircle2, Clock3, Radio, TrendingUp, WifiOff } from 'lucide-react';
import { projectRealtimeMonitor, monitorEventLabels, monitorRoute } from './realtimeMonitorModel.js';
import './RealtimeMonitor.css';

const number = value => typeof value === 'number' && Number.isFinite(value) ? new Intl.NumberFormat('ko-KR', { maximumFractionDigits: 2 }).format(value) : '—';
const quantity = value => typeof value === 'number' && Number.isFinite(value) ? new Intl.NumberFormat('ko-KR', { maximumFractionDigits: 8 }).format(value) : '—';
const amount = value => value === null || value === undefined ? '—' : `${value > 0 ? '+' : ''}${number(value)}원`;
const date = value => value && Number.isFinite(Date.parse(value)) ? new Date(value).toLocaleString('ko-KR', {timeZone:'Asia/Seoul', month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '미기록';
const percent = value => value === null ? '변화율 미확인' : `${value > 0 ? '+' : ''}${number(value)}%`;
const signalLabel = value => ({ BUY:'매수 신호', SELL:'매도 신호', HOLD:'유지 · 신호 대기', NOT_OBSERVED:'아직 신호 미관측' }[value] || value || '신호 미확인');

export default function RealtimeMonitor({ snapshot, fresh, pollError, receivedAt, staleMs = 20000 }) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => { const timer = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(timer); }, []);
  const view = useMemo(() => projectRealtimeMonitor(snapshot, {fresh, pollError, receivedAt, now, staleMs}), [snapshot, fresh, pollError, receivedAt, now, staleMs]);
  const StateIcon = view.resultHasIssue || ['warning', 'danger'].includes(view.state.tone) ? AlertTriangle
    : view.terminal ? CheckCircle2 : view.responseCurrent ? Activity : WifiOff;
  return <section className="panel live-monitor" aria-label="실시간 모니터">
    <header className="live-monitor-heading"><div><span className="live-monitor-eyebrow">LIVE TRADER · 5~7시간 실제 주문 시험</span><h2>실시간 모니터</h2></div><span className={`live-monitor-connection ${view.responseCurrent ? 'is-current' : 'is-stale'}`}><Radio size={14}/>{view.responseCurrent ? '화면 응답 수신' : '화면 상태 미확인'}</span></header>
    <div className={`live-monitor-hero tone-${view.state.tone}`} role="status"><StateIcon size={27}/><div><span>현재 상황</span><h3>{view.state.title}</h3><p>{view.state.detail}</p></div></div>
    <div className="live-monitor-freshness"><span>마지막 갱신 <strong>{date(receivedAt ? new Date(receivedAt).toISOString() : null)}</strong></span><span>서버 관측 {date(snapshot?.observedAt)}</span><span>시세 수신: {view.feedsRecent && view.responseCurrent ? "최근 프레임 관측" : view.terminal ? "종료 기록" : "확인 필요"}</span></div>
    <div className="live-monitor-metrics">
      <article><span>완성봉 평가</span><strong>{number(view.counts.decisions)}<small>회</small></strong><p>전략별 평가 횟수 합계</p></article>
      <article><span>주문 시도</span><strong>{number(view.counts.attempts)}<small>회</small></strong><p>전략별 시도 · 체결과 별도</p></article>
      <article><span>체결 관측</span><strong>{number(view.counts.fills)}<small>회</small></strong><p>부분 체결 포함 · 고유 주문 수 아님</p></article>
      <article><span>관측 계좌 평가 변화</span><strong className={view.totalChangeKrw > 0 ? 'is-positive' : view.totalChangeKrw < 0 ? 'is-negative' : ''}>{amount(view.totalChangeKrw)}</strong><p>{view.totalChangeKrw === null ? '모든 경로의 기준값이 있어야 합산합니다' : view.allAccountsCurrent ? '최근 계좌 관측 합산 · 전략 확정 수익 아님' : '마지막 관측값 합산 · 현재 평가 변화는 미확인'}</p></article>
    </div>
    {view.sessionId && <div className="live-monitor-duration"><div><span><Clock3 size={15}/> 시작 {date(view.startedAt)}</span><span>예정 종료 {date(view.endsAt)}</span></div>{view.progress !== null && <progress max="100" value={view.progress} aria-label="예정 시험 시간 경과율"/>}<small>시간 경과 {view.progress === null ? '미확인' : `${Math.floor(view.progress)}%`} · 시간 막대는 거래 성공이나 정상 종료를 뜻하지 않습니다.</small></div>}
    {view.blockers.length > 0 && <div className="live-monitor-alert"><strong>확인이 필요한 사항</strong><ul>{view.blockers.map((row,index)=><li key={index}>{row}</li>)}</ul></div>}
    {view.feeds.length > 0 && <div className="live-monitor-feed-strip" aria-label="시세 수신 관측">{view.feeds.map((row,index)=><div key={`${row.route}:${index}`}><strong>{monitorRoute(row.route)}</strong><span>{view.terminal ? '종료 당시 기록' : !view.responseCurrent ? '현재 미확인' : row.disconnected ? '연결 소켓 없음' : row.delayed ? '프레임 수신 지연' : row.allRecent ? '최근 프레임 수신' : '수신 시각 미확인'}</span><small>마지막 프레임 {date(row.lastFrameAt)}</small></div>)}<p>서버가 처리한 공개 시세 프레임의 원래 수신 시각입니다. 프레임 수신은 봉 평가나 체결 성공을 뜻하지 않습니다.</p></div>}
    <div className="live-monitor-columns"><div className="live-monitor-primary">
      <div className="live-monitor-section-heading"><h3>전략별 진행</h3><span>{view.strategies.length}개 전략</span></div>
      {view.strategies.length === 0 ? <p className="live-monitor-empty">실행을 시작하면 선택한 전략의 진행이 표시됩니다.</p> : <div className="live-monitor-strategies">{view.strategies.map((row,index)=><article key={row.selectionKey || index}>
        <div><strong>{row.symbol || '종목 미확인'} <small>{row.timeframe}</small></strong><span>{monitorRoute(row.route)} · {row.pluginId || row.label || '전략 미확인'}</span></div>
        <b className={`live-monitor-signal signal-${row.signal}`}>{signalLabel(row.signal)}</b>
        <ol aria-label={`${row.symbol} 진행 횟수`}><li><span>봉 평가</span><strong>{number(row.decisionCount)}</strong></li><li><span>주문 시도</span><strong>{number(row.orderAttemptCount)}</strong></li><li><span>체결 관측</span><strong>{number(row.fillObservationCount)}</strong></li></ol>
        <small>{row.decisionCount === 0 ? '첫 완성봉 평가를 기다립니다.' : row.signal === 'HOLD' ? '최근 평가는 유지 신호입니다. 조건이 충족돼야 주문합니다.' : '최근 신호입니다. 주문과 체결은 위 횟수로 따로 확인하세요.'}{!view.responseCurrent && ' · 마지막 수신값'}</small>
      </article>)}</div>}
      <div className="live-monitor-section-heading"><h3>경로별 자금 변화</h3><TrendingUp size={18}/></div>
      {view.routes.length === 0 ? <p className="live-monitor-empty">계좌 평가 기준과 현재 관측값이 아직 없습니다. 0원 수익으로 표시하지 않습니다.</p> : <div className="live-monitor-equity">{view.routes.map((row,index)=><article key={`${row.broker}:${index}`}>
        <div className="live-monitor-equity-head"><strong>{row.routeLabel}</strong><span>{row.accountCurrent ? '최근 계좌 대조' : view.terminal ? '종료 당시 관측값' : '마지막 계좌 관측값'}</span></div>
        <div className="live-monitor-equity-value"><strong className={row.changeKrw > 0 ? 'is-positive' : row.changeKrw < 0 ? 'is-negative' : ''}>{amount(row.changeKrw)}</strong><span>{percent(row.changePercent)}</span></div>
        <p>{row.basisLabel}</p><small>기준 {quantity(row.monitorEquity?.baselineNative)} → 현재 {quantity(row.monitorEquity?.currentNative)} {row.monitorEquity?.quoteCurrency || ''}</small>
        <small>실제 수수료 {row.actualFeeKnown === true ? '대조 확인' : '미확정'} · 계좌 관측 {date(row.observedAt)}</small>
        <small>입출금 등 외부 자금 변동이 포함될 수 있습니다. 전략의 확정 수익률과 다릅니다.</small>
      </article>)}</div>}
    </div><aside className="live-monitor-activity"><div className="live-monitor-section-heading"><h3>최근 활동</h3><span>최신 30개</span></div>
      {view.activity.length === 0 ? <p className="live-monitor-empty">아직 공개 활동 기록이 없습니다. 거래가 없었다는 확정 결과는 아닙니다.</p> : <ol>{view.activity.map((event,index)=><li key={`${event.sequence}:${index}`}><span className={`live-monitor-event-icon ${event.kind === 'ACTUAL_FILL_INCREMENT' ? 'is-fill' : ''}`}>{event.side === 'BUY' ? <ArrowDownLeft size={15}/> : event.side === 'SELL' ? <ArrowUpRight size={15}/> : <Activity size={14}/>}</span><div><strong>{monitorEventLabels[event.kind]}</strong><time>{date(event.observedAt)}</time><p>{[event.symbol, event.route && monitorRoute(event.route), event.side === 'BUY' ? '매수' : event.side === 'SELL' ? '매도' : '', event.status].filter(Boolean).join(' · ')}</p>{event.incrementalQuantity !== null && event.incrementalQuantity !== undefined && <small>추가 체결 수량 {quantity(event.incrementalQuantity)}</small>}{event.kind === 'INTERNAL_ALLOCATION_TRANSFER' && <small>거래소 체결이 아닌 전략 간 배분입니다.</small>}</div></li>)}</ol>}
      <p className="live-monitor-footnote">현재 시험에서 실제 기록된 활동만 표시합니다. 창을 다시 조회해도 새 거래를 만들지 않습니다.</p>
    </aside></div>
  </section>;
}
