export function StockSessionGuide() {
  return <section aria-label="주식 정규장·시간외 지원 범위">
    <h3>주식 정규장·시간외 지원 범위</h3>
    <p>전략은 연결한 시세와 거래 세션을 유지합니다. 정규장과 시간외의 봉을 구분하며, 코인은 24시간 시세를 사용합니다.</p>
    <p>KIS는 KRX·NXT와 미국 일반·주간 경로를 구분하고, 토스증권은 통합 시세를 사용합니다. KIS 미국 주간거래는 연속 봉 운용을 지원하지 않습니다.</p>
    <p>시간외 운용에는 지원 종목, 거래 가능 시간과 지정가 조건이 적용됩니다. 실제 체결이 없는 봉에서 모의 체결을 만들지 않으며, 확인할 수 없는 과거 구간은 비워둡니다.</p>
    <p><a href="https://apiportal.koreainvestment.com/intro" target="_blank" rel="noopener noreferrer">KIS 공식 API 안내</a> · <a href="https://developers.tossinvest.com/docs/market-data" target="_blank" rel="noopener noreferrer">토스증권 시세 안내</a></p>
  </section>;
}
