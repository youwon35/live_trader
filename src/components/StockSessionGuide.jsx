// Static capability guidance; opening this disclosure performs no requests.
export function StockSessionGuide() {
  return <details className="ts-disclosure panel settings-card">
    <summary><strong>주식 정규장·시간외 지원 범위</strong><span className="muted"> · 필요할 때만 확인</span></summary>
    <p>주식 전략은 정규장과 시간외를 따로 저장합니다. 전략의 시간대·시세 소스는 과거 데이터부터 모의 체결과 실제 주문까지 함께 유지하며, 기존 정규장 전략을 자동으로 시간외로 바꾸지 않습니다. 코인 감시는 24시간 경로를 사용합니다.</p>
    <p><strong>봉 종가</strong>는 1분·5분·1시간 등 각 봉에서 마지막으로 체결된 가격입니다. 프리·애프터마켓도 실제 거래가 있으면 새 봉과 가격이 생깁니다. <strong>정규장 일일 종가</strong>는 장이 끝나 확정된 가격이며, 시간외종가 거래는 그 고정 가격으로 거래합니다. 토스가 제공하는 거래량 0인 원본 분봉은 보존하되, 실제 체결이 없는 봉으로 가상 체결을 만들지 않습니다.</p>
    <p>KIS는 KRX·NXT 및 미국 일반·주간 경로를 구분하고, 토스는 통합시장 시세·주문을 사용합니다. 시간외는 원래 시세시각, 거래 가능 종목, 공식 일정과 지정가를 확인합니다. KIS 미국 주간거래는 공식 실시간 체결 구독을 확인하지 못해 연속 봉 운용을 지원하지 않습니다.</p>
    <p className="muted">과거 시간외 자료는 증권사가 제공하거나 당시 저장한 범위만 사용합니다. 확인하지 못한 구간은 누락으로 남깁니다. 일정이 열려 있다는 것만으로 주문 권한이 승인되지는 않습니다.</p>
    <p><a href="https://apiportal.koreainvestment.com/intro" target="_blank" rel="noopener noreferrer">KIS 공식 API 안내 ↗</a> · <a href="https://openapi.tossinvest.com/openapi-docs/latest/openapi.json" target="_blank" rel="noopener noreferrer">토스 공식 API 지원 범위 ↗</a></p>
  </details>;
}
