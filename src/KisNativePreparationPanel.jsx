import * as React from "react";
import { KIS_PREPARATION_LABELS } from "./kisNativePreparation.js";

export default function KisNativePreparationPanel({ views = {}, onCancel }) {
  return Object.values(views).filter((value) => value.status !== "COMPLETE").map((value) => (
    <section key={value.scopeKey} className="panel kis-native-preparation" aria-label="한국투자증권 시작 준비" data-testid="kis-native-preparation">
      <div role="status" aria-live="polite">
        <strong>{value.label} · {KIS_PREPARATION_LABELS[value.status] || "준비 상태 확인 중"}</strong>
        <p>{value.detail || "시세 준비와 계좌 확인이 끝나면 Windows 확인창에서 시작 여부를 직접 확인합니다."}</p>
      </div>
      {value.publicProgress && <p role="status">{value.publicProgress.phase === "REPLAYING_NATIVE_TICKS" ? "모은 시세를 확인하는 중" : "장중 체결 자료를 모으는 중"} · {Math.floor(value.publicProgress.elapsedSeconds / 60)}분 경과 · 수신 {value.publicProgress.frameCount}건 · 종료 {value.publicProgress.cutoff}</p>}
      {value.canCancel && <button className="secondary-button" type="button" onClick={() => onCancel(value.scopeKey)}>준비 취소</button>}
    </section>
  ));
}
