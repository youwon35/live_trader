// Diagnostic projection only; this module cannot authorize a trade or FX rate.
const codes=new Set(['NONE','NATIVE_FX_ORDERBOOK_SCOPE_INVALID','NATIVE_FX_ORDERBOOK_PRICE_INVALID','NATIVE_FX_ORDERBOOK_SIZE_INVALID','NATIVE_FX_ORDERBOOK_TIMESTAMP_INVALID','NATIVE_FX_CURRENT_OBSERVATION_REQUIRED','NATIVE_FX_HTTP_CLOCK_PROOF_MISSING','NATIVE_FX_HTTP_CLOCK_HEADER_BINDING_CHANGED','NATIVE_FX_CACHED_HTTP_RESPONSE_REJECTED','NATIVE_FX_OFFICIAL_HTTP_DATE_MISSING_OR_INVALID','NATIVE_FX_HTTP_CLOCK_LOCAL_BINDING_CHANGED','NATIVE_FX_HTTP_CLOCK_RTT_INVALID','NATIVE_FX_HTTP_CLOCK_SKEW_OR_RTT_EXCEEDED','NATIVE_FX_OBSERVATION_STALE_OR_FUTURE','NATIVE_FX_OBSERVATION_SEAL_INVALID','NATIVE_FX_OBSERVATION_CURRENCY_OR_ROUTE_MISMATCH','NATIVE_FX_OFFICIAL_RESPONSE_BINDING_CHANGED','NATIVE_FX_RATE_RESPONSE_CHANGED','NATIVE_FX_TIME_RESPONSE_CHANGED']);
const finite=value=>typeof value==='number'&&Number.isFinite(value);
const micros=value=>{if(typeof value!=='string'||value.length>40)return null;const match=value.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(?:Z|\+00:00)$/);if(!match)return null;const ms=Date.parse(match[1]+'Z');if(!Number.isFinite(ms)||new Date(ms).toISOString().slice(0,19)!==match[1])return null;return BigInt(ms)*1000n+BigInt((match[2]||'').padEnd(6,'0'));};
const seconds=(a,b)=>a===null||b===null?null:Number(a-b)/1e6;
const close=(expected,actual)=>expected===null?actual===null:finite(actual)&&Math.abs(expected-actual)<=.000002;
export function normalizeFxObservation(value){
 if(!value||value.schemaVersion!=='native-fx-clock-observation-v2'||value.provider!=='upbit'||value.symbol!=='KRW-USDT'||value.endpoint!=='/v1/orderbook'||value.observationOnly!==true||value.orderAuthorityGranted!==false||value.timeBasis!=='UPBIT_ORDERBOOK_REQUEST_TIMESTAMP'||value.valueBasis!=='BEST_ASK_CONSERVATIVE_KRW_BUDGET'||value.maxAgeSeconds!==15||value.maxFutureSeconds!==2||!codes.has(value.validationCode)||!codes.has(value.clockIssue))return null;
 const fields=['sourceObservedAt','requestStartedAt','receivedAt','checkedAt'];
 if(fields.some(key=>value[key]!==null&&micros(value[key])===null))return null;
 const [source,started,received,checked]=fields.map(key=>micros(value[key]));
 if(value.sourceTimestampMs===null?source!==null:!Number.isSafeInteger(value.sourceTimestampMs)||value.sourceTimestampMs<=0||value.sourceTimestampMs>253402300799999||source===null||Math.abs(Number(source-BigInt(value.sourceTimestampMs)*1000n))>1)return null;
 let remote=null;
 if(value.dateHeader!==null){if(typeof value.dateHeader!=='string'||value.dateHeader.length>40)return null;const parsed=Date.parse(value.dateHeader);if(!Number.isFinite(parsed)||new Date(parsed).toUTCString()!==value.dateHeader)return null;remote=BigInt(parsed)*1000n;}
 if(![null,'0','INVALID'].includes(value.ageHeader))return null;
 const ns=value.roundTripNanoseconds;
 if(ns!==null&&(!Number.isSafeInteger(ns)||ns<0||ns>86400000000000))return null;
 const localAge=seconds(checked,source),receiveAge=seconds(checked,received),wallRtt=seconds(received,started);
 const rtt=wallRtt!==null&&ns!==null?Math.max(wallRtt,ns/1e9):null;
 const upper=remote===null?null:remote+1000000n;
 const conservative=upper!==null&&source!==null&&rtt!==null&&receiveAge!==null?seconds(upper,source)+rtt+receiveAge:null;
 const skew=remote!==null&&started!==null&&received!==null?Math.max(...[remote,upper].flatMap(a=>[started,received].map(b=>Math.abs(seconds(a,b))))):null;
 let issue='NONE';
 if([started,received,checked].includes(null))issue='NATIVE_FX_HTTP_CLOCK_LOCAL_BINDING_CHANGED';
 else if(remote===null)issue='NATIVE_FX_OFFICIAL_HTTP_DATE_MISSING_OR_INVALID';
 else if(value.ageHeader==='INVALID')issue='NATIVE_FX_CACHED_HTTP_RESPONSE_REJECTED';
 else if(ns===null)issue='NATIVE_FX_HTTP_CLOCK_RTT_INVALID';
 else if(!(started<=received&&received<=checked)||!(rtt>=0&&rtt<=15)||skew>15)issue='NATIVE_FX_HTTP_CLOCK_SKEW_OR_RTT_EXCEEDED';
 else if(source===null)issue='NATIVE_FX_ORDERBOOK_TIMESTAMP_INVALID';
 else if(receiveAge>15||localAge< -2||source>upper||!(conservative>=0&&conservative<=15))issue='NATIVE_FX_OBSERVATION_STALE_OR_FUTURE';
 if(value.clockIssue!==issue||value.status!==(value.validationCode==='NONE'?'FRESH':'BLOCKED')||(issue!=='NONE'&&value.validationCode==='NONE'))return null;
 if(!close(localAge,value.sourceAgeSeconds)||!close(conservative,value.sourceAgeUpperSeconds)||!close(receiveAge,value.receiveAgeSeconds)||!close(rtt,value.readElapsedSeconds))return null;
 return Object.fromEntries(['schemaVersion','provider','symbol','endpoint','observationOnly','orderAuthorityGranted','timeBasis','valueBasis','status','validationCode','clockIssue','sourceTimestampMs',...fields,'dateHeader','ageHeader','roundTripNanoseconds','sourceAgeSeconds','sourceAgeUpperSeconds','receiveAgeSeconds','readElapsedSeconds','maxAgeSeconds','maxFutureSeconds'].map(key=>[key,value[key]]));
}
