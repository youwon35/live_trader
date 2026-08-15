import * as React from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, RefreshCw, RotateCcw, ShieldCheck, Square, Play } from "lucide-react";

import {
  getBinanceSpotFunctionalStatus,
  getUpbitFunctionalStatus,
  recoverBinanceSpotFunctional,
  recoverUpbitFunctional,
  reprepareCryptoFirstLive,
  startBinanceSpotFunctional,
  startUpbitFunctional,
  stopBinanceSpotFunctional,
  stopUpbitFunctional,
} from "./api";
import { cryptoFirstCombinedStatus } from "./cryptoFirstLiveModel";


const EMPTY_STATUS = Object.freeze({
  prepared: false,
  available: false,
  networkOrderPostAllowed: false,
  terminalState: "UNAVAILABLE",
  reason: "상태를 아직 읽지 않았습니다.",
});

function compact(value = "") {
  const text = String(value || "");
  return text.length > 22 ? `${text.slice(0, 12)}…${text.slice(-7)}` : text || "-";
}

function statusReason(status, controls) {
  return String(
    status.reason
    || status.firstLiveBootstrapBlockedReason
    || status.terminalDetail
    || controls.startBlockReason
    || "준비 상태를 확인하세요.",
  );
}

function LaneCard({ lane, status, controls, busy, onStart, onStop, onRecover }) {
  const upbit = lane === "UPBIT";
  const title = upbit ? "Upbit · KRW-BTC" : "Binance Spot · BTCUSDT";
  const amount = upbit ? "최대 10,000 KRW" : "최대 10 USDT";
  const assurance = upbit
    ? "독립 계좌 관측 authority"
    : String(status.assuranceMode || "SUPERVISED_NON_PROMOTION");
  return (
    <article className="crypto-first-lane-card">
      <div className="crypto-first-lane-heading">
        <div>
          <span>{lane}</span>
          <h3>{title}</h3>
        </div>
        <span className={`functional-test-status functional-test-status--${controls.active ? "running" : controls.prepared ? "ready" : "blocked"}`}>
          {controls.active ? "실행/정리 중" : controls.prepared ? controls.terminalState : "준비 안 됨"}
        </span>
      </div>

      <dl className="crypto-first-lane-facts">
        <div><dt>시험 계약</dt><dd>2시간 · BUY 1회 · SELL 1회 · 재진입 없음</dd></div>
        <div><dt>금액</dt><dd>{amount}</dd></div>
        <div><dt>보증 모드</dt><dd>{assurance}</dd></div>
        <div><dt>세션</dt><dd>{compact(controls.sessionId)}</dd></div>
        <div>
          <dt>Owner SHA256</dt>
          <dd><code>{String(status.serverOwnerIdentitySha256 || "-")}</code></dd>
        </div>
      </dl>

      <div className="crypto-first-lane-hold" role="status">
        {controls.startEnabled ? <ShieldCheck size={17} /> : <AlertTriangle size={17} />}
        <span>{controls.startEnabled ? "시작 조건 확인 완료" : statusReason(status, controls)}</span>
      </div>

      <div className="crypto-first-lane-actions">
        <button
          className="primary-button"
          type="button"
          disabled={Boolean(busy) || !controls.startEnabled}
          onClick={onStart}
          title={controls.startEnabled ? "감독형 1회 기능시험 시작" : controls.startBlockReason}
        >
          <Play size={15} aria-hidden="true" />
          시작
        </button>
        <button
          className="mini-button"
          type="button"
          disabled={Boolean(busy) || !controls.stopEnabled}
          onClick={onStop}
        >
          <Square size={14} aria-hidden="true" />
          중지·정리
        </button>
        <button
          className="mini-button"
          type="button"
          disabled={Boolean(busy) || !controls.recoverEnabled}
          onClick={onRecover}
        >
          <RotateCcw size={14} aria-hidden="true" />
          복구
        </button>
      </div>
    </article>
  );
}

export default function CryptoFirstLivePanel() {
  const [upbit, setUpbit] = useState(EMPTY_STATUS);
  const [binance, setBinance] = useState(EMPTY_STATUS);
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const controls = useMemo(
    () => cryptoFirstCombinedStatus(upbit, binance),
    [upbit, binance],
  );

  const refresh = useCallback(async ({ quiet = false } = {}) => {
    if (!quiet) setBusy("refresh");
    try {
      const [upbitStatus, binanceStatus] = await Promise.all([
        getUpbitFunctionalStatus(),
        getBinanceSpotFunctionalStatus(),
      ]);
      setUpbit(upbitStatus && typeof upbitStatus === "object" ? upbitStatus : EMPTY_STATUS);
      setBinance(binanceStatus && typeof binanceStatus === "object" ? binanceStatus : EMPTY_STATUS);
      setError("");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "코인 기능시험 상태를 읽지 못했습니다.");
    } finally {
      if (!quiet) setBusy("");
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const initial = async () => {
      if (!cancelled) await refresh({ quiet: true });
    };
    void initial();
    const timer = window.setInterval(() => void refresh({ quiet: true }), 5_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [refresh]);

  async function runAction(name, action) {
    setBusy(name);
    setMessage("");
    setError("");
    try {
      const result = await action();
      if (result?.ok === false) setError(result.reason || "요청이 안전 차단되었습니다.");
      else setMessage(result?.reason || "요청을 처리했습니다.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "요청에 실패했습니다.");
    } finally {
      await refresh({ quiet: true });
      setBusy("");
    }
  }

  const anyLaneActive = controls.upbit.active || controls.binance.active;

  return (
    <div className="crypto-first-live-panel" aria-labelledby="crypto-first-live-heading">
      <header className="crypto-first-live-header">
        <div>
          <span className="functional-test-eyebrow">CRYPTO FIRST LIVE · NATIVE TRUSTED CONTROL</span>
          <h2 id="crypto-first-live-heading">코인 2시간 실거래 기능시험</h2>
          <p>감독형 1회 기능시험이며 승급 근거가 아닙니다. 두 거래소를 동시에 활성화하지 않습니다.</p>
        </div>
        <div className="crypto-first-live-toolbar">
          <button
            className="mini-button"
            type="button"
            disabled={Boolean(busy) || anyLaneActive}
            onClick={() => void runAction("reprepare", reprepareCryptoFirstLive)}
            title={anyLaneActive ? "실행·정리 중에는 다시 준비할 수 없습니다." : "외부 authority 설정과 pin을 다시 읽습니다."}
          >
            <RotateCcw size={14} aria-hidden="true" />
            설정·핀 다시 준비
          </button>
          <button className="mini-button" type="button" disabled={Boolean(busy)} onClick={() => void refresh()}>
            <RefreshCw size={14} aria-hidden="true" />
            새로고침
          </button>
        </div>
      </header>

      <div className="crypto-first-live-warning" role="note">
        <AlertTriangle size={18} aria-hidden="true" />
        <span>출금·이체·마진·선물 금지 · 다른 봇/수동거래 금지 · release HOLD에서는 시작 버튼이 잠깁니다.</span>
      </div>

      <div className="crypto-first-live-grid">
        <LaneCard
          lane="UPBIT"
          status={upbit}
          controls={controls.upbit}
          busy={busy}
          onStart={() => void runAction("upbit-start", () => startUpbitFunctional(upbit.pendingApprovalId || ""))}
          onStop={() => void runAction("upbit-stop", () => stopUpbitFunctional(controls.upbit.sessionId))}
          onRecover={() => void runAction("upbit-recover", () => recoverUpbitFunctional("", controls.upbit.sessionId))}
        />
        <LaneCard
          lane="BINANCE_SPOT"
          status={binance}
          controls={controls.binance}
          busy={busy}
          onStart={() => void runAction("binance-start", () => startBinanceSpotFunctional(binance.pendingApprovalId || ""))}
          onStop={() => void runAction("binance-stop", () => stopBinanceSpotFunctional(controls.binance.sessionId))}
          onRecover={() => void runAction("binance-recover", () => recoverBinanceSpotFunctional(controls.binance.sessionId))}
        />
      </div>

      {busy && <p className="crypto-first-live-feedback">처리 중: {busy}</p>}
      {message && <p className="crypto-first-live-feedback crypto-first-live-feedback--ok">{message}</p>}
      {error && <p className="crypto-first-live-feedback crypto-first-live-feedback--error">{error}</p>}
    </div>
  );
}
