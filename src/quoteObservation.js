// Display metadata only; this module never grants trading permission.
const finite = value => typeof value === 'number' && Number.isFinite(value);
const stamp = value => typeof value === 'string' && value.length <= 40 && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value)) ? Date.parse(value) : null;
const utcStamp = value => { const parsed=stamp(value); return parsed !== null && /(?:Z|\+00:00)$/.test(value) && new Date(parsed).toISOString().slice(0,19) === value.slice(0,19) ? parsed : null; };
const issues = new Set(['NONE','READ_CLOCK_INVALID','SOURCE_TIMESTAMP_MISSING_OR_INVALID','LAST_TRADE_TIMESTAMP_MISSING_OR_INVALID','SOURCE_TIMESTAMP_FUTURE','SOURCE_TIMESTAMP_STALE','LAST_TRADE_TIMESTAMP_FUTURE','LAST_TRADE_AFTER_SOURCE_TIMESTAMP','QUOTE_READ_TOO_SLOW','RECEIVE_TIMESTAMP_STALE']);
const numberOrNull = value => value === null || finite(value);
const close = (left,right) => left === null ? right === null : finite(right) && Math.abs(left-right) <= 0.002;

export function normalizeUpbitQuoteObservation(value) {
  if (!value || typeof value !== 'object' || value.schemaVersion !== 'upbit-quote-observation-v1'
      || value.boundary !== 'QUOTE_READ_VALIDATION' || value.observationOnly !== true
      || !/^[A-Z0-9]{1,16}-[A-Z0-9]{1,48}$/.test(value.symbol || '')
      || !['INVENTORY_VALUATION','ORDER_MARKET'].includes(value.purpose)
      || !['FRESH','BLOCKED'].includes(value.status) || !issues.has(value.issue)
      || value.maxSourceAgeSeconds !== 10 || value.maxReceiveAgeSeconds !== 10 || value.maxFutureSeconds !== 2) return null;
  const timeFields=['sourceObservedAt','lastTradeObservedAt','requestStartedAt','receivedAt','checkedAt'];
  if (timeFields.some(key => value[key] !== null && utcStamp(value[key]) === null)) return null;
  for (const [field,iso] of [['sourceTimestampMs','sourceObservedAt'],['lastTradeTimestampMs','lastTradeObservedAt']]) {
    if (value[field] === null ? value[iso] !== null : !Number.isSafeInteger(value[field]) || value[field] <= 0 || utcStamp(value[iso]) !== value[field]) return null;
  }
  const source=stamp(value.sourceObservedAt),trade=stamp(value.lastTradeObservedAt),start=stamp(value.requestStartedAt),received=stamp(value.receivedAt),checked=stamp(value.checkedAt);
  const ages={sourceAgeSeconds:checked === null || source === null ? null : (checked-source)/1000,
    lastTradeAgeSeconds:checked === null || trade === null ? null : (checked-trade)/1000,
    receiveAgeSeconds:checked === null || received === null ? null : (checked-received)/1000,
    readElapsedSeconds:received === null || start === null ? null : (received-start)/1000};
  if (Object.entries(ages).some(([key,expected]) => !numberOrNull(value[key]) || !close(expected,value[key]))) return null;
  const precise=value;
  let issue='NONE';
  if ([start,received,checked].includes(null)) issue='READ_CLOCK_INVALID';
  else if (source === null) issue='SOURCE_TIMESTAMP_MISSING_OR_INVALID';
  else if (trade === null) issue='LAST_TRADE_TIMESTAMP_MISSING_OR_INVALID';
  else if (precise.sourceAgeSeconds < -2) issue='SOURCE_TIMESTAMP_FUTURE';
  else if (precise.sourceAgeSeconds > 10) issue='SOURCE_TIMESTAMP_STALE';
  else if (precise.lastTradeAgeSeconds < -2) issue='LAST_TRADE_TIMESTAMP_FUTURE';
  else if (trade > source) issue='LAST_TRADE_AFTER_SOURCE_TIMESTAMP';
  else if (precise.readElapsedSeconds < -2 || precise.receiveAgeSeconds < -2) issue='READ_CLOCK_INVALID';
  else if (precise.readElapsedSeconds > 10) issue='QUOTE_READ_TOO_SLOW';
  else if (precise.receiveAgeSeconds > 10) issue='RECEIVE_TIMESTAMP_STALE';
  // Ages retain the producer microsecond precision after ISO/source binding.
  // The backend alone enforces the actual trading guard.
  if (value.issue !== issue || value.status !== (issue === 'NONE' ? 'FRESH' : 'BLOCKED')) return null;
  return Object.fromEntries(['schemaVersion','symbol','purpose','boundary','observationOnly','status','issue','sourceTimestampMs','lastTradeTimestampMs',...timeFields,...Object.keys(ages),'maxSourceAgeSeconds','maxReceiveAgeSeconds','maxFutureSeconds'].map(key=>[key,value[key]]));
}

export function failureTiming(run = {}, endedAt = null, now = Date.now()) {
  const started=stamp(run.startedAt),ended=stamp(endedAt);
  const rows=(Array.isArray(run.recentActivity) ? run.recentActivity : []).filter(row=>row.kind === 'FIRST_FAILURE');
  const direct = typeof run.firstFailureAt === 'string' && run.firstFailureAt.trim() ? run.firstFailureAt : null;
  const recorded = rows.length === 1 ? stamp(rows[0].observedAt) : null;
  const first = direct !== null ? stamp(direct) : recorded;
  // firstFailureAt is the decision boundary; observedAt is later journal publication.
  const coherent = rows.length <= 1 && (rows.length === 0 || (recorded !== null && recorded <= now+5000 && (ended === null || recorded <= ended) && (direct === null || (first !== null && first <= recorded))));
  const valid=coherent && started !== null && first !== null && first >= started && first <= now+5000 && (ended === null || first <= ended);
  return { firstFailureAt: valid ? (direct || rows[0].observedAt) : null,
    firstFailureSeconds:valid ? (first-started)/1000 : null,
    cleanupSeconds:valid && ended !== null && ended >= first ? (ended-first)/1000 : null };
}
