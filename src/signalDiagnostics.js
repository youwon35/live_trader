const fields = [
  ['historyCount','평가에 사용한 봉 수'],
  ['shortPeriod','단기 이동평균 기간'],['longPeriod','장기 이동평균 기간'],
  ['previousShort','직전 단기 이동평균'],['previousLong','직전 장기 이동평균'],
  ['currentShort','이번 단기 이동평균'],['currentLong','이번 장기 이동평균'],
  ['period','RSI 기간'],['rsi','이번 RSI'],['oversold','RSI 진입 기준'],['exit','RSI 청산 기준'],
  ['regimeMa','추세 필터 이동평균'],['regimePass','추세 필터'],
  ['window','돌파 관측 기간'],['averageRange','평균 변동폭'],['k','돌파 계산 계수'],
  ['trigger','돌파 기준가'],['barHigh','평가 봉 고가'],['exitMa','청산 이동평균'],
  ['entryBelow','진입 기준가'],['exitAbove','청산 기준가'],
  ['warmupBars','준비에 필요한 봉 수'],['warmupCount','준비된 봉 수'],['entryCadencePassed','진입 시간 조건'],
];

// The engine owns these values. Ignore raw objects, identifiers and unrecorded inputs.
export function signalDiagnostics(metadata) {
  if (!metadata || typeof metadata!=='object') return [];
  return fields.flatMap(([key,label])=>{
    const value = Object.hasOwn(metadata,key) ? metadata[key] : null;
    if (typeof value==='number' && Number.isFinite(value)) return [{label,value}];
    if (typeof value==='boolean' && ['regimePass','entryCadencePassed'].includes(key)) return [{label,value:value?'충족':'미충족'}];
    return [];
  });
}
