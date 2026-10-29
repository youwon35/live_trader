import React, { useEffect, useMemo, useState } from 'react';
import { Activity, AlertTriangle, ArrowDownLeft, ArrowUpRight, CheckCircle2, Clock3, Radio, TrendingUp, WifiOff } from 'lucide-react';
import { projectRealtimeMonitor, monitorEventLabels, monitorRoute } from './realtimeMonitorModel.js';
import './RealtimeMonitor.css';
import MonitorEquityChart from './MonitorEquityChart.jsx';
import './MonitorWorkspace.css';

const number = value => typeof value === 'number' && Number.isFinite(value) ? new Intl.NumberFormat('ko-KR', { maximumFractionDigits: 2 }).format(value) : '—';
const quantity = value => typeof value === 'number' && Number.isFinite(value) ? new Intl.NumberFormat('ko-KR', { maximumFractionDigits: 8 }).format(value) : '—';
const amount = value => value === null || value === undefined ? '—' : `${value > 0 ? '+' : ''}${number(value)}원`;
const date = value => value && Number.isFinite(Date.parse(value)) ? new Date(value).toLocaleString('ko-KR', {timeZone:'Asia/Seoul', month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}) : '미기록';
const percent = value => value === null ? '변화율 미확인' : `${value > 0 ? '+' : ''}${number(value)}%`;
const duration = seconds => seconds === null ? '미확인' : `${seconds >= 3600 ? `${Math.floor(seconds / 3600)}시간 ` : ''}${Math.floor(seconds % 3600 / 60)}분 ${seconds % 60}초`;
const signalLabel = value => ({ BUY:'매수 신호', SELL:'매도 신호', HOLD:'유지 · 신호 대기', NOT_OBSERVED:'아직 신호 미관측' }[value] || value || '신호 미확인');

export default function RealtimeMonitor({ snapshot, fresh, pollError, receivedAt, staleMs = 20000 }) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => { const timer = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(timer); }, []);
  const view = useMemo(() => projectRealtimeMonitor(snapshot, {fresh, pollError, receivedAt, now, staleMs}), [snapshot, fresh, pollError, receivedAt, now, staleMs]);
  const StateIcon = view.resultHasIssue || ['warning', 'danger'].includes(view.state.tone) ? AlertTriangle
    : view.terminal ? CheckCircle2 : view.responseCurrent ? Activity : WifiOff;
  return <section className="panel live-monitor" aria-label="실시간 모니터">
    <header className="live-monitor-heading"><div><span className="live-monitor-eyebrow">LIVE TRADER · 5~7시간 실제 주문 시험</span><h2>주문 연결 시험 · 실시간 감시</h2></div><span className={`live-monitor-connection ${view.responseCurrent ? 'is-current' : 'is-stale'}`}><Radio size={14}/>{view.responseCurrent ? '화면 응답 수신' : '화면 상태 미확인'}</span></header>
    <div className={`live-monitor-hero tone-${view.state.tone}`} role="status"><StateIcon size={27}/><div><span>현재 상황</span><h3>{view.state.title}</h3><p>{view.state.detail}</p></div></div>
    {view.firstFailure && <div className="live-monitor-alert live-monitor-failure-timing">
      <strong>중단까지의 실제 진행</strong>
      <p>{view.timing.firstFailureSeconds === null ? '최초 실패 시각이 기록되지 않아 중단까지 걸린 시간을 확인할 수 없습니다.' : `시작 후 ${number(view.timing.firstFailureSeconds)}초에 최초 중단 조건이 발생했습니다.`}{view.timing.cleanupSeconds !== null && ` 이후 종료·대조에 ${number(view.timing.cleanupSeconds)}초가 걸렸습니다.`}</p>
      <small>6시간은 예정 운용 시간입니다. 필수 조건이 깨지면 새 주문을 보류하고 먼저 종료·대조를 진행합니다. 시작 동의가 6시간 동안의 무조건 실행을 뜻하지 않습니다.</small>
    </div>}
    {view.firstFailure && <details className="live-monitor-alert">
      <summary>최초 오류 상세{view.failureDiagnostics[0] && ` · ${view.failureDiagnostics[0].providerLabel} · ${view.failureDiagnostics[0].resultLabel}`}</summary>
      <p>{view.firstFailure}</p>
      {view.failureDiagnostics.length > 0 ? <ul>{view.failureDiagnostics.map((row, index) => <li key={index}>
        <strong>{row.providerLabel}</strong> · {row.resultLabel} · {row.elapsedLabel}
        {row.path && <><br/><code style={{overflowWrap:'anywhere'}}>{row.path}</code></>}
        {row.brokerCode && <small> · 거래소 코드 {row.brokerCode}</small>}
        {row.fxObservation && <dl className="live-monitor-quote-observation">
          <div><dt>환산 기준</dt><dd>Upbit KRW-USDT 현재 최우선 매도 호가 · 원화 예산 보수적 환산</dd></div>
          <div><dt>거래소 호가 조회 시각</dt><dd>{date(row.fxObservation.sourceObservedAt)} · 로컬 경과 {row.fxObservation.sourceAgeSeconds === null ? '미기록' : `${number(row.fxObservation.sourceAgeSeconds)}초`}</dd></div>
          <div><dt>HTTP 시각 보수 상한</dt><dd>{row.fxObservation.sourceAgeUpperSeconds === null ? '미기록' : `${number(row.fxObservation.sourceAgeUpperSeconds)}초`} · 허용 15초</dd></div>
          <div><dt>수신·실제 검사 시각</dt><dd>{date(row.fxObservation.receivedAt)} / {date(row.fxObservation.checkedAt)}</dd></div>
          <div><dt>응답·검증</dt><dd>요청 {row.fxObservation.readElapsedSeconds === null ? '미기록' : `${number(row.fxObservation.readElapsedSeconds)}초`} · {row.fxObservation.validationCode}</dd></div>
        </dl>}
        {row.quoteObservation && <dl className="live-monitor-quote-observation">
          <div><dt>검사 대상</dt><dd>{row.quoteObservation.symbol} · {row.quoteObservation.purpose === 'INVENTORY_VALUATION' ? '기존 보유 자산 평가' : '주문 시세 검사'}</dd></div>
          <div><dt>현재가 정보 반영 시각</dt><dd>{date(row.quoteObservation.sourceObservedAt)} · 경과 {row.quoteObservation.sourceAgeSeconds === null ? '미기록' : `${number(row.quoteObservation.sourceAgeSeconds)}초`}</dd></div>
          <div><dt>마지막 실제 체결 시각</dt><dd>{date(row.quoteObservation.lastTradeObservedAt)} · 현재가 정보 시각과 구분</dd></div>
          <div><dt>실제 수신 시각</dt><dd>{date(row.quoteObservation.receivedAt)} · 요청 소요 {row.quoteObservation.readElapsedSeconds === null ? '미기록' : `${number(row.quoteObservation.readElapsedSeconds)}초`}</dd></div>
          <div><dt>적용 기준</dt><dd>정보·수신 10초 이내 · 미래 2초 이내 · {row.quoteObservation.issue}</dd></div>
        </dl>}

      </li>)}</ul> : <p>이 기록에는 세부 API 오류가 남아 있지 않습니다. 정확한 요청 경로나 통신 원인을 추정하지 않습니다.</p>}
      {!view.failureDiagnostics.some(row=>row.quoteObservation || row.fxObservation) && <p>이 기록에는 시세 원본 시각이 저장되지 않았습니다. 당시 어떤 시각이 몇 초 오래됐는지는 추측하지 않습니다. HTTP 조회가 성공해도 응답 내용의 시세 검사는 실패할 수 있습니다.</p>}
      <small>최초 실패 당시의 관측입니다. 이후 종료 정리가 완료되어도 이 오류는 보존됩니다.</small>
    </details>}
    <div className="live-monitor-freshness"><span>마지막 갱신 <strong>{date(receivedAt ? new Date(receivedAt).toISOString() : null)}</strong></span><span>서버 관측 {date(snapshot?.observedAt)}</span><span>시세 수신: {view.terminalReported ? "종료 기록" : view.feedsRecent && view.responseCurrent ? "최근 프레임 관측" : "확인 필요"}</span></div>
    <div className="live-monitor-metrics">
      <article><span>완성봉 평가</span><strong>{number(view.counts.decisions)}<small>회</small></strong><p>전략별 평가 횟수 합계</p></article>
      <article><span>주문 시도</span><strong>{number(view.counts.attempts)}<small>회</small></strong><p>전략별 시도 · 체결과 별도</p></article>
      <article><span>체결 관측</span><strong>{number(view.counts.fills)}<small>회</small></strong><p>부분 체결 포함 · 고유 주문 수 아님</p></article>
      <article><span>관측 계좌 평가 변화</span><strong className={view.totalChangeKrw > 0 ? 'is-positive' : view.totalChangeKrw < 0 ? 'is-negative' : ''}>{amount(view.totalChangeKrw)}</strong><p>{view.totalChangeKrw === null ? '모든 경로의 기준값이 있어야 합산합니다' : view.allAccountsCurrent ? '최근 계좌 관측 합산 · 전략 확정 수익 아님' : '마지막 관측값 합산 · 현재 평가 변화는 미확인'}</p></article>
    </div>
    {view.sessionId && <div className="live-monitor-duration"><div><span><Clock3 size={15}/> 시작 {date(view.startedAt)}</span><span>예정 종료 {date(view.endsAt)}{view.terminalReported && ` · 종료 관측 ${date(view.terminatedAt)}`}</span></div>{view.progress !== null && <progress max="100" value={view.progress} aria-label="예정 시험 시간 경과율"/>}<small>{view.terminalReported ? '종료까지 관측 경과' : '시간 경과'} {duration(view.elapsedSeconds)} · 예정 시간 대비 {view.progress === null ? '미확인' : `${number(view.progress)}%`} · 시간 막대는 거래 성공이나 시험 통과를 뜻하지 않습니다.</small></div>}
    {view.blockers.length > 0 && <div className="live-monitor-alert"><strong>확인이 필요한 사항</strong><ul>{view.blockers.map((row,index)=><li key={index}>{row}</li>)}</ul></div>}
    {view.feeds.length > 0 && <div className="live-monitor-feed-strip" aria-label="시세 수신 관측">{view.feeds.map((row,index)=><div key={`${row.route}:${index}`}><strong>{monitorRoute(row.route)}</strong><span>{view.terminal ? '종료 당시 기록' : !view.responseCurrent ? '현재 미확인' : row.disconnected ? '연결 소켓 없음' : row.delayed ? '프레임 수신 지연' : row.allRecent ? '최근 프레임 수신' : '수신 시각 미확인'}</span><small>마지막 프레임 {date(row.lastFrameAt)}</small></div>)}<p>서버가 처리한 공개 시세 프레임의 원래 수신 시각입니다. 프레임 수신은 봉 평가나 체결 성공을 뜻하지 않습니다.</p></div>}
    <div className="monitor-main-grid"><div><MonitorEquityChart history={snapshot?.runtime?.equityHistory || []} routes={[...new Set(view.strategies.map(row=>row.route).filter(Boolean))]}/></div><section className="monitor-strategy-column">      <div className="live-monitor-section-heading"><h3>전략별 진행</h3><span>{view.strategies.length}개 전략</span></div>
      {view.strategies.length === 0 ? <p className="live-monitor-empty">실행을 시작하면 선택한 전략의 진행이 표시됩니다.</p> : <div className="live-monitor-strategies">{view.strategies.map((row,index)=><article key={row.selectionKey || index}>
        <div><strong>{row.symbol || '종목 미확인'} <small>{row.timeframe}</small></strong><span>{monitorRoute(row.route)} · {row.pluginId || row.label || '전략 미확인'}</span></div>
        <b className={`live-monitor-signal signal-${row.signal}`}>{signalLabel(row.signal)}</b>
        <ol aria-label={`${row.symbol} 진행 횟수`}><li><span>봉 평가</span><strong>{number(row.decisionCount)}</strong></li><li><span>주문 시도</span><strong>{number(row.orderAttemptCount)}</strong></li><li><span>체결 관측</span><strong>{number(row.fillObservationCount)}</strong></li></ol>
        <small>{view.terminal ? row.decisionCount === 0 ? '종료 전 완성봉 평가 기록이 없습니다.' : '종료 당시 마지막 평가 기록입니다.' : row.decisionCount === 0 ? '첫 완성봉 평가를 기다립니다.' : row.signal === 'HOLD' ? '최근 평가는 유지 신호입니다. 조건이 충족돼야 주문합니다.' : '최근 신호입니다. 주문과 체결은 위 횟수로 따로 확인하세요.'}{!view.responseCurrent && ' · 마지막 수신값'}</small>
      </article>)}</div>}
</section></div>
    <div className="live-monitor-columns"><div className="live-monitor-primary">
      <div className="live-monitor-section-heading"><h3>감시 종목과 포지션</h3><span>이번 시험 원장 기준</span></div>
      <div className="monitor-table-scroll"><table className="monitor-market-table"><thead><tr><th>종목 · 경로</th><th>최근 완성봉 가격</th><th>시험 보유 수량</th><th>미결 주문</th></tr></thead><tbody>
        {view.strategies.map((row,index)=><tr key={row.selectionKey || index}><td><strong>{row.symbol || '미확인'}</strong><small>{monitorRoute(row.route)} · {row.timeframe}</small></td><td>{quantity(row.lastPrice)}<small>{date(row.lastBarAt)}</small></td><td>{quantity(row.positionQuantity)}<small>평균 진입 {quantity(row.averageEntryPrice)}</small></td><td>{number(row.pendingOrderCount)}<small>마지막 판단 {date(row.lastDecisionAt)}</small></td></tr>)}
        {!view.strategies.length&&<tr><td colSpan="4">아직 전략 관측이 없습니다.</td></tr>}
      </tbody></table></div><p className="monitor-chart-note">가격은 실제 평가에 사용한 완성봉 종가입니다. 현재 호가가 아니며, 기존 계좌 전체 자산은 계좌·잔고 탭에서 확인할 수 있습니다.</p>
      <details className="monitor-account-details"><summary>경로별 자금 변화 · 기준과 수수료 상세</summary>
      <div className="live-monitor-section-heading"><h3>경로별 자금 변화</h3><TrendingUp size={18}/></div>
      {view.routes.length === 0 ? <p className="live-monitor-empty">계좌 평가 기준과 현재 관측값이 아직 없습니다. 0원 수익으로 표시하지 않습니다.</p> : <div className="live-monitor-equity">{view.routes.map((row,index)=><article key={`${row.broker}:${index}`}>
        <div className="live-monitor-equity-head"><strong>{row.routeLabel}</strong><span>{row.accountCurrent ? '최근 계좌 대조' : view.terminal ? '종료 당시 관측값' : '마지막 계좌 관측값'}</span></div>
        <div className="live-monitor-equity-value"><strong className={row.changeKrw > 0 ? 'is-positive' : row.changeKrw < 0 ? 'is-negative' : ''}>{amount(row.changeKrw)}</strong><span>{percent(row.changePercent)}</span></div>
        <p>{row.basisLabel}</p><small>기준 {quantity(row.monitorEquity?.baselineNative)} → 현재 {quantity(row.monitorEquity?.currentNative)} {row.monitorEquity?.quoteCurrency || ''}</small>
        <small>실제 수수료 {row.actualFeeKnown === true ? '대조 확인' : '미확정'} · 계좌 관측 {date(row.observedAt)}</small>
        <small>입출금 등 외부 자금 변동이 포함될 수 있습니다. 전략의 확정 수익률과 다릅니다.</small>
      </article>)}</div>}
      </details>
    </div><aside className="live-monitor-activity"><div className="live-monitor-section-heading"><h3>최근 활동</h3><span>최신 30개</span></div>
      {view.activity.length === 0 ? <p className="live-monitor-empty">아직 공개 활동 기록이 없습니다. 거래가 없었다는 확정 결과는 아닙니다.</p> : <ol>{view.activity.map((event,index)=><li key={`${event.sequence}:${index}`}><span className={`live-monitor-event-icon ${event.kind === 'ACTUAL_FILL_INCREMENT' ? 'is-fill' : ''}`}>{event.side === 'BUY' ? <ArrowDownLeft size={15}/> : event.side === 'SELL' ? <ArrowUpRight size={15}/> : <Activity size={14}/>}</span><div><strong>{monitorEventLabels[event.kind] || event.kind}</strong><time>{date(event.observedAt)}</time><p>{[event.symbol, event.route && monitorRoute(event.route), event.side === 'BUY' ? '매수' : event.side === 'SELL' ? '매도' : '', event.status].filter(Boolean).join(' · ')}</p>{event.incrementalQuantity !== null && event.incrementalQuantity !== undefined && <small>추가 체결 수량 {quantity(event.incrementalQuantity)}</small>}{event.kind === 'INTERNAL_ALLOCATION_TRANSFER' && <small>거래소 체결이 아닌 전략 간 배분입니다.</small>}</div></li>)}</ol>}
      <p className="live-monitor-footnote">현재 시험에서 실제 기록된 활동만 표시합니다. 창을 다시 조회해도 새 거래를 만들지 않습니다.</p>
    </aside></div>
  </section>;
}
