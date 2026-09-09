import React, { useEffect, useId, useState } from 'react';
import { request } from './api';
import { amount, stamp, preflightDelta, noOrderReasons, stopEffects, reconciliationEvidence, feeEvidence, capitalAllowance, deploymentChanges, orderInvestigation } from './operatorInsights';
import './OperatorInsights.css';
function InlineDisclosure({ children, open = false, onToggle, ...props }) {
  const [expanded, setExpanded] = useState(Boolean(open));
  const id = useId();
  const nodes = React.Children.toArray(children);
  const summary = nodes.find(node => React.isValidElement(node) && node.type === 'summary');
  return <section {...props} className={`operator-inline ${props.className || ''}`}>
    <button type="button" className="operator-inline-toggle" aria-expanded={expanded} aria-controls={id} onClick={() => { setExpanded(!expanded); onToggle?.({ currentTarget: { open: !expanded } }); }}>{summary?.props.children}</button>
    <div id={id} hidden={!expanded}>{nodes.filter(node => node !== summary)}</div>
  </section>;
}
const stateName = value => ({ MONITOR: '관찰', SMALL_LIVE: '제한 실거래', FULL_LIVE: '실전 운용', STARTING: '시작 중', RUNNING: '실행 중', DEGRADED: '확인 필요', DRAINING: '정리 중', STOPPING: '중지 중', STOPPED: '중지', FILLED: '체결 완료', PARTIALLY_FILLED: '부분 체결', ACKNOWLEDGED: '접수됨', CANCELED: '취소됨', UNKNOWN: '미확정' })[String(value || '').toUpperCase()] || value || '미확인';
const hash = value => value ? String(value).slice(0, 12) : '미확인';
function useReview(scope, token, enabled=true) {
  const [state, setState] = useState({ loading: true });
  useEffect(() => {
    if (!enabled) return;
    let active = true;
    setState({ loading: true });
    request(`/api/operator-review?deployment_id=${encodeURIComponent(scope || '')}`)
      .then(value => { if (active) setState(value.ok === true ? value : { error: value.reason || '검토 기록 조회 실패' }); })
      .catch(error => { if (active) setState({ error: error.message }); });
    return () => { active = false; };
  }, [scope, token, enabled]);
  return state;
}
export function RunningSelection({ snapshot, context }) {
  const bindings = snapshot.operator_runtime_bindings;
  const values = Array.isArray(bindings) ? bindings : [];
  return <div className="operator-insights operator-running" aria-label="실제 실행 고정 정보">
    <strong>실제 실행</strong>
    {!values.length ? <span>{Array.isArray(bindings) ? '실행 중인 감시 없음' : '실행 정보 미확인'}</span> : values.map((row,index) => <InlineDisclosure key={row.sessionId || index}>
      <summary>{row.deploymentId || '배포 미확인'} · {stateName(row.mode)} {row.deploymentId !== context.id ? '· 조회 선택과 다름' : '· 조회 선택과 같음'}</summary>
      <dl><div><dt>실행 전략</dt><dd>{row.manifest?.metadata?.strategyIds?.join(', ') || row.manifest?.metadata?.strategyId || '미확인'}</dd></div><div><dt>고정 버전</dt><dd title={row.manifest?.manifestHash}>{row.manifest ? `배포 ${row.manifest.revision} · ${hash(row.manifest.strategyArtifactHash)}` : '고정 구성 미확인'}</dd></div><div><dt>계좌</dt><dd title={row.manifest?.accountFingerprint}>{row.manifest?.brokerRoute || '거래 경로 미확인'} · 식별값 {hash(row.manifest?.accountFingerprint)}</dd></div><div><dt>실행 회차</dt><dd>{row.sessionId || '미확인'} · {stateName(row.lifecycle)}</dd></div></dl>
    </InlineDisclosure>)}
  </div>;
}
export function PreflightChanges({ snapshot, scope }) {
  const data = useReview(scope, snapshot.operator_review_revision || snapshot.live_governance?.latestPreflight?.snapshotId);
  const delta = preflightDelta(data.history);
  return <InlineDisclosure className="operator-insights" aria-label="직전 시작 점검과 비교"><summary>직전 시작 점검과 비교{delta.known ? ` · ${delta.first ? "현재 문제" : "새 문제"} ${delta.newIssues.length} · 해결 ${delta.resolved.length}` : ""}</summary>
    {data.loading ? <p>저장된 점검 읽는 중</p> : data.error ? <p>{data.error}</p> : !delta.known ? <p>이 배포에서 실행한 시작 점검이 아직 없습니다.</p> : <>
      <p>{delta.first ? '첫 저장 점검 · 비교 기준은 다음 점검부터 생깁니다.' : `${stamp(delta.previous.recorded_at)} → ${stamp(delta.latest.recorded_at)}`}</p>
      <div className="operator-summary-grid"><div><strong>{delta.first ? '현재 문제' : '새 문제·심화'} {delta.newIssues.length}</strong>{delta.newIssues.map(row => <p key={row.label}>{row.label} · {row.detail}</p>)}</div><div><strong>해결 {delta.resolved.length}</strong>{delta.resolved.map(row => <p key={row.label}>{row.label} · 이번 점검 통과</p>)}</div></div>
      <InlineDisclosure><summary>계속 확인할 문제 {delta.unchanged.length} · 비교에서 빠진 항목 {delta.removed.length}</summary>{delta.unchanged.map(row => <p key={row.label}>{row.label} · {row.detail}</p>)}{delta.removed.map(row => <p key={row.label}>{row.label} · 이번 검사 대상에서 빠짐, 해결 여부 미확인</p>)}</InlineDisclosure>
      <small>저장된 두 점검의 비교입니다. 현재 실행 허가는 별도 시작 점검 유효성에 따릅니다.</small>
    </>}{snapshot.operator_review_error && <p>{snapshot.operator_review_error}</p>}
  </InlineDisclosure>;
}
export function NoOrderExplanation({ snapshot, context }) {
  const reasons = noOrderReasons(snapshot, context);
  return <InlineDisclosure className="operator-insights" open><summary>새 주문이 없는 이유 · {context.symbol || '선택 배포'}</summary><ul>{reasons.map((row,index) => <li key={index}><strong>{row.category}</strong> · {row.detail}</li>)}</ul><small>확인된 제한과 최근 평가를 나눠 보여줍니다. 이미 접수된 주문 상태는 주문 화면에서 확인하세요.</small></InlineDisclosure>;
}
export function StopEffects() {
  return <InlineDisclosure className="operator-insights"><summary>중지 방법별 영향 비교</summary><div className="operator-table-scroll"><table><thead><tr>{['동작','새 주문','미체결','보유 자산'].map(value => <th key={value}>{value}</th>)}</tr></thead><tbody>{stopEffects.map(row => <tr key={row[0]}>{row.map((value,index) => <td key={index}>{value}</td>)}</tr>)}</tbody></table></div><small>취소 요청은 취소 완료와 다릅니다. 기능 검사 세션은 전용 안전 종료 절차를 사용합니다.</small></InlineDisclosure>;
}
export function ReconciliationEvidence({ snapshot }) {
  const evidence = reconciliationEvidence(snapshot);
  return <InlineDisclosure className="operator-insights"><summary>종목·체결·수수료 대조 근거</summary><p>계좌 대조 {stamp(evidence.at)} · 원장 기준 {stamp(evidence.baseline)}</p>
    <div className="operator-table-scroll"><table><thead><tr><th>거래 경로 · 종목</th><th>앱 수량</th><th>거래소 수량</th><th>거래소−앱 차이</th><th>근거</th></tr></thead><tbody>{evidence.positions.map((row,index) => <tr key={index}><td>{row.broker_id} · {row.symbol} · {row.position_side || '현물'}</td><td>{amount(row.program)}</td><td>{amount(row.broker)}</td><td>{amount(row.difference)}</td><td>{row.detail || row.program_source || '원인 미확인'}</td></tr>)}</tbody></table></div>
    {!evidence.positions.length && <p>종목별 대조 기록이 없습니다.</p>}
    <h4>최근 저장 체결 이벤트 {evidence.events.length}건</h4><div className="operator-table-scroll"><table><thead><tr><th>시각 · 주문</th><th>종목 · 상태</th><th>수량 · 가격</th><th>수수료 근거</th></tr></thead><tbody>{evidence.events.map((row,index) => { const fee = feeEvidence(row); return <tr key={row.event_id || index}><td>{stamp(row.occurred_at)}<br/>{row.order_id || row.broker_order_id || '주문 미확인'}</td><td>{row.symbol} · {stateName(row.state)}</td><td>{amount(row.raw?.quantity ?? row.quantity)} @ {amount(row.raw?.price ?? row.price)}<br/>{row.raw?.quantity_mode === 'cumulative' ? '누계 수량' : row.raw?.quantity_mode === 'delta' ? '개별 체결' : '수량 기준 미기록'}</td><td>{amount(fee.fee)} · {fee.currency} · {fee.mode}{!fee.verified && ' · 확정 비용 미확인'}</td></tr>; })}</tbody></table></div>
    <small>최근 이벤트만 표시하므로 기간 전체 체결·수수료 합계가 아닙니다. 수수료의 통화·개별/누계 기준이 없으면 합산하지 않으며, 잔고 차이의 원인이라고 단정하지 않습니다.</small>
  </InlineDisclosure>;
}
export function CapitalAllowance({ snapshot, context, limit }) {
  const view = capitalAllowance(snapshot, context, limit);
  return <div className="operator-insights" aria-label="금액 한도 미리보기"><strong>입력 한도와 보유 사용량 · {context.symbol || '종목 미선택'}</strong><p>입력 {amount(view.limit)}원 · 현재 사용 {amount(view.used)}{view.used != null ? '원' : ''} · 보유 기준 잔여 {amount(view.remaining)}{view.remaining != null ? '원' : ''}</p>{view.over > 0 && <p>현재 보유가 입력 한도를 {amount(view.over)}원 초과합니다.</p>}{view.pending > 0 && <p>관련 미체결·미확정 {view.pending}건 · 추가 주문 가능액은 대조 전 미확인입니다.</p>}<small>{view.reason} 입력값은 기존 방식대로 입력칸을 벗어나면 저장 요청됩니다.</small></div>;
}
export function DeploymentChanges({ snapshot, scope }) {
  const [open,setOpen] = useState(false);
  const data = useReview(scope, snapshot.operator_review_revision, open);
  const view = deploymentChanges(data);
  return <InlineDisclosure className="operator-insights" onToggle={event => setOpen(event.currentTarget.open)}><summary>기존 배포와 달라진 설정</summary>{data.loading ? <p>고정 구성 비교 중</p> : data.error ? <p>{data.error}</p> : <><p>{data.comparisonBasis || '비교 기준 미확인'} → 조회 배포의 현재 설정</p>{data.comparisonError && <p>{data.comparisonError}</p>}{view.changes.length ? <div className="operator-table-scroll"><table><thead><tr><th>변경 항목</th><th>이전</th><th>현재</th></tr></thead><tbody>{view.changes.map(row => <tr key={row.label}><th>{row.label}</th><td>{row.before}</td><td>{row.after}</td></tr>)}</tbody></table></div> : <p>{view.known ? '확인 가능한 항목에서 변경 없음' : '비교 기준 없음'}</p>}{view.unknown.length > 0 && <InlineDisclosure><summary>비교 근거가 부족한 항목 {view.unknown.length}</summary>{view.unknown.map(row => <p key={row}>{row}</p>)}</InlineDisclosure>}<small>종목·목표 비중·위험 설정의 확인된 변경만 표시합니다. 배포 교체나 실행 승인은 하지 않습니다.</small></>}</InlineDisclosure>;
}
export function OrderInvestigation({ snapshot, order }) {
  const view = orderInvestigation(snapshot, order);
  const [history, setHistory] = useState({ events: [], nextOffset: null, total: null });
  const [historyError, setHistoryError] = useState('');
  const [historyBusy, setHistoryBusy] = useState(false);
  useEffect(() => {
    let active = true;
    setHistory({ events: [], nextOffset: null, total: null }); setHistoryError('');
    if (!order.order_id) return;
    request(`/api/operator-order-history?order_id=${encodeURIComponent(order.order_id)}`).then(value => {
      if (active) { if (value.ok === true && Array.isArray(value.events)) setHistory(value); else setHistoryError(value.reason || '조사 기록 응답 미확인'); }
    }).catch(error => { if(active) setHistoryError(error.message); });
    return () => { active = false; };
  }, [order.order_id, order.updated_at]);
  async function loadOlder() {
    if (history.nextOffset == null || historyBusy) return;
    setHistoryBusy(true); setHistoryError('');
    try {
      const value = await request(`/api/operator-order-history?order_id=${encodeURIComponent(order.order_id)}&offset=${history.nextOffset}`);
      if (value.ok !== true || !Array.isArray(value.events)) throw new Error(value.reason || '조사 기록 응답 미확인');
      setHistory(current => ({ ...value, events: [...current.events, ...value.events] }));
    } catch(error) { setHistoryError(error.message); } finally { setHistoryBusy(false); }
  }
  const audit = history.total == null ? view.audit : history.events;
  return <InlineDisclosure className="operator-insights" open><summary>주문 조사 내역 · 마지막 대조</summary><p>{view.reason}</p><dl><div><dt>주문 기록 갱신</dt><dd>{stamp(view.lastUpdated)}</dd></div><div><dt>이 주문의 마지막 체결 이벤트</dt><dd>{stamp(view.lastExecution)}</dd></div><div><dt>전체 계좌 마지막 대조</dt><dd>{stamp(view.lastAccountReconciliation)} · 이 주문의 접수·취소 확정과는 별개</dd></div><div><dt>다음 재시도 기록</dt><dd>{view.nextRetry && view.nextRetry !== '-' ? stamp(view.nextRetry) : '예약 근거 없음'}</dd></div></dl>{audit.map((row,index) => <p key={row.event_id || index}>{stamp(row.timestamp || row.occurred_at)} · {row.message || row.detail || row.reason}</p>)}{!audit.length && <p>이 주문 번호에 직접 연결된 저장 조사 기록이 없습니다.</p>}{history.total != null && <small>저장 조사 기록 {audit.length}/{history.total}건</small>}{history.nextOffset != null && <button type="button" className="secondary-button" disabled={historyBusy} onClick={loadOlder}>이전 조사 기록 더 보기</button>}{historyError && <p>{historyError}</p>}<small>미확정 결과는 새 주문으로 재전송하지 말고 기존 상태 대조 절차로 확인하세요.</small></InlineDisclosure>;
}
export function OperatorNoteEditor({ recordKey }) {
  const [note,setNote] = useState(null), [text,setText] = useState(''), [message,setMessage] = useState(''), [busy,setBusy] = useState(false);
  useEffect(() => { let active=true; setNote(null); setText(''); setMessage(''); if (recordKey) request(`/api/operator-note?record_key=${encodeURIComponent(recordKey)}`).then(value => { if(active) { if(value.ok) {setNote(value.note);setText(value.note.text);} else setMessage(value.reason); } }).catch(error => {if(active)setMessage(error.message);}); return () => { active=false; }; }, [recordKey]);
  async function reload() { setBusy(true); setMessage(''); try { const value = await request(`/api/operator-note?record_key=${encodeURIComponent(recordKey)}`); if (!value.ok) throw new Error(value.reason); setNote(value.note); setText(value.note.text); } catch(error) { setMessage(error.message); } finally { setBusy(false); } }
  async function save() { setBusy(true);setMessage('');try { const value=await request('/api/operator-note', {method:'POST',body:{record_key:recordKey,text,revision:note.revision}});if(!value.ok)throw new Error(value.reason);setNote(value.note);setText(value.note.text);setMessage('메모 저장됨'); }catch(error){setMessage(error.message);}finally{setBusy(false);} }
  return <section className="operator-insights" aria-label="실행 기록 메모">
    <h4>내 실행 기록 메모</h4>
    {!recordKey ? <p>저장된 실행 기록 식별값이 없어 메모를 연결할 수 없습니다.</p> : <>
      <textarea aria-label="실행 기록 메모 입력" maxLength={500} rows={3} value={text} onChange={event=>setText(event.target.value)} disabled={!note || busy}/>
      <div className="operator-note-actions"><small>{text.length}/500 · 개인 메모, 원본 실행 증거와 별도</small><button className="primary-button" type="button" onClick={save} disabled={!note || busy || note.text===text}>{busy ? '저장 중' : '메모 저장'}</button></div>
      {note?.updated_at && <small>저장 {stamp(note.updated_at)}</small>}
      {message && <p role="status" className="ts-text-region">{message}</p>}
      <button type="button" className="secondary-button" disabled={busy} onClick={reload}>저장 메모 다시 읽기</button>
      {note?.history?.length > 1 && <InlineDisclosure><summary>메모 수정 이력 {note.history.length}건</summary>{note.history.map(row=><p key={row.revision}>{stamp(row.updated_at)} · {row.text || '(내용 삭제)'}</p>)}</InlineDisclosure>}
    </>}
  </section>;
}
