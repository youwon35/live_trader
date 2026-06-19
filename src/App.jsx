import { useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Bell,
  CircleStop,
  DatabaseZap,
  FileClock,
  KeyRound,
  LayoutDashboard,
  ListChecks,
  LockKeyhole,
  Network,
  Play,
  Power,
  Radio,
  RefreshCcw,
  Route,
  Search,
  ShieldAlert,
  ShieldCheck,
  Siren,
  TerminalSquare,
  WalletCards,
} from "lucide-react";
import { getSnapshot, setFlag, setMode, submitTestIntent } from "./api";

const navItems = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "gate", label: "Live Gate", icon: ListChecks },
  { id: "orders", label: "Orders", icon: Route },
  { id: "brokers", label: "Brokers", icon: Network },
  { id: "strategies", label: "Strategies", icon: DatabaseZap },
  { id: "audit", label: "Audit", icon: FileClock },
];

const fallbackSnapshot = {
  generated_at: "-",
  mode: "MONITOR",
  kill_switch: false,
  new_entries_blocked: true,
  operator_confirmed: false,
  summary: { status: "blocked", blocker_count: 1, warning_count: 0, live_strategy_count: 0, broker_ready_count: 0 },
  sessions: [],
  readiness: [{ label: "Python API", status: "fail", detail: "Python server connection is required." }],
  risk_checks: [],
  brokers: [],
  strategies: [],
  orders: [],
  positions: [],
  audit: [],
};

function App() {
  const [snapshot, setSnapshot] = useState(fallbackSnapshot);
  const [selectedNav, setSelectedNav] = useState("overview");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function refresh() {
    try {
      const next = await getSnapshot();
      setSnapshot(next);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "API 연결 실패");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => window.clearInterval(timer);
  }, []);

  async function runAction(action) {
    setLoading(true);
    try {
      const result = await action();
      setSnapshot(result.snapshot ?? result);
      setError(result.ok === false ? result.reason : "");
    } catch (err) {
      setError(err instanceof Error ? err.message : "요청 실패");
    } finally {
      setLoading(false);
    }
  }

  const title = navItems.find((item) => item.id === selectedNav)?.label ?? "Overview";
  const canLive = snapshot.summary.blocker_count === 0;
  const canFullLive = canLive && snapshot.summary.warning_count === 0;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block" aria-label="Live Trader">
          <div className="brand-mark">
            <Radio size={19} />
          </div>
          <div>
            <strong>Live Trader</strong>
            <span>Real Order Desk</span>
          </div>
        </div>
        <nav className="nav-list" aria-label="주요 메뉴">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                className={`nav-item ${selectedNav === item.id ? "active" : ""}`}
                type="button"
                key={item.id}
                onClick={() => setSelectedNav(item.id)}
              >
                <Icon size={17} />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
        <div className="sidebar-footer">
          <span>운용 모드</span>
          <StatusPill tone={snapshot.mode === "MONITOR" ? "info" : "danger"}>{snapshot.mode}</StatusPill>
        </div>
      </aside>

      <main className="workspace">
        <header className="topbar">
          <div>
            <p>{title}</p>
            <h1>Live Trader 실거래 콘솔</h1>
          </div>
          <div className="topbar-actions">
            <div className="search-box">
              <Search size={15} />
              <input aria-label="심볼 또는 주문 검색" placeholder="심볼, 전략, 주문 검색" />
            </div>
            <button className="icon-button" type="button" aria-label="새로고침" onClick={refresh}>
              <RefreshCcw size={17} className={loading ? "spin" : ""} />
            </button>
            <button className="icon-button" type="button" aria-label="알림">
              <Bell size={17} />
            </button>
            <button
              className={`danger-button ${snapshot.kill_switch ? "active" : ""}`}
              type="button"
              onClick={() => runAction(() => setFlag("kill_switch", !snapshot.kill_switch))}
            >
              <CircleStop size={17} />
              Kill Switch
            </button>
          </div>
        </header>

        <MarketStrip sessions={snapshot.sessions} />

        {(error || snapshot.summary.blocker_count > 0) && (
          <section className="alert-band" aria-live="polite">
            <ShieldAlert size={20} />
            <div>
              <strong>실거래 주문 차단 상태</strong>
              <span>{error || `readiness blocker ${snapshot.summary.blocker_count}개가 남아 있습니다. API 키, 주문 어댑터, live_allowed 권한을 확인하세요.`}</span>
            </div>
          </section>
        )}

        <section className="command-grid">
          <ModeConsole
            mode={snapshot.mode}
            canLive={canLive}
            canFullLive={canFullLive}
            onMode={(mode) => runAction(() => setMode(mode))}
            onConfirm={() => runAction(() => setFlag("operator_confirmed", !snapshot.operator_confirmed))}
            operatorConfirmed={snapshot.operator_confirmed}
            newEntriesBlocked={snapshot.new_entries_blocked}
            onEntryBlock={() => runAction(() => setFlag("new_entries_blocked", !snapshot.new_entries_blocked))}
            onTestIntent={() => runAction(submitTestIntent)}
          />
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
        </section>

        <section className="content-grid">
          <div className="content-column">
            <ReadinessPanel checks={snapshot.readiness} />
            <RiskPanel checks={snapshot.risk_checks} />
            <StrategyPanel strategies={snapshot.strategies} />
            <OrderPanel orders={snapshot.orders} />
            <AuditPanel audit={snapshot.audit} />
          </div>
          <div className="content-column">
            <BrokerPanel brokers={snapshot.brokers} />
            <PositionPanel positions={snapshot.positions} />
          </div>
        </section>
      </main>
    </div>
  );
}

function StatusPill({ children, tone = "neutral" }) {
  return <span className={`status-pill ${tone}`}>{children}</span>;
}

function statusTone(status) {
  if (status === "pass" || status === "connected" || status === "open") return "success";
  if (status === "warn" || status === "watch" || status === "adapter_required") return "warning";
  if (status === "fail" || status === "blocked" || status === "missing_credentials") return "danger";
  return "neutral";
}

function MarketStrip({ sessions }) {
  return (
    <section className="market-strip">
      {sessions.map((session) => (
        <div className="market-item" key={session.label}>
          <Radio size={16} />
          <div>
            <strong>{session.label}</strong>
            <span>{session.detail}</span>
          </div>
          <StatusPill tone={statusTone(session.state)}>{session.time}</StatusPill>
        </div>
      ))}
    </section>
  );
}

function ModeConsole({
  mode,
  canLive,
  canFullLive,
  onMode,
  operatorConfirmed,
  onConfirm,
  newEntriesBlocked,
  onEntryBlock,
  onTestIntent,
}) {
  const modes = [
    { id: "MONITOR", icon: Power, locked: false },
    { id: "SMALL_LIVE", icon: Play, locked: !canLive },
    { id: "FULL_LIVE", icon: LockKeyhole, locked: !canFullLive },
  ];
  return (
    <section className="panel mode-console">
      <PanelHeader title="실거래 모드" subtitle="실계좌 주문은 모든 게이트 통과 후에만 열립니다." />
      <div className="mode-selector">
        {modes.map((item) => {
          const Icon = item.icon;
          return (
            <button
              type="button"
              key={item.id}
              className={`mode-button ${mode === item.id ? "active" : ""}`}
              onClick={() => onMode(item.id)}
            >
              <Icon size={16} />
              <span>{item.id}</span>
              {item.locked && <LockKeyhole size={13} />}
            </button>
          );
        })}
      </div>
      <div className="operator-actions">
        <button className={`secondary-button ${operatorConfirmed ? "active" : ""}`} type="button" onClick={onConfirm}>
          <BadgeCheck size={16} />
          운용자 확인
        </button>
        <button className={`secondary-button ${newEntriesBlocked ? "active" : ""}`} type="button" onClick={onEntryBlock}>
          <ShieldCheck size={16} />
          신규 진입 차단
        </button>
        <button className="primary-button" type="button" onClick={onTestIntent}>
          <TerminalSquare size={16} />
          테스트 주문 게이트
        </button>
      </div>
    </section>
  );
}

function SummaryPanel({ summary, generatedAt }) {
  const items = [
    { label: "상태", value: summary.status, tone: statusTone(summary.status) },
    { label: "Blocker", value: summary.blocker_count, tone: summary.blocker_count ? "danger" : "success" },
    { label: "Warning", value: summary.warning_count, tone: summary.warning_count ? "warning" : "success" },
    { label: "Live 전략", value: summary.live_strategy_count, tone: summary.live_strategy_count ? "success" : "danger" },
    { label: "브로커 준비", value: summary.broker_ready_count, tone: summary.broker_ready_count ? "success" : "danger" },
  ];
  return (
    <section className="panel summary-panel">
      <PanelHeader title="운용 요약" subtitle={`마지막 갱신 ${generatedAt}`} />
      <div className="summary-grid">
        {items.map((item) => (
          <div key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <StatusPill tone={item.tone}>LIVE GATE</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function ReadinessPanel({ checks }) {
  return (
    <section className="panel readiness-panel">
      <PanelHeader title="Live Readiness" subtitle="API, 계약, 권한, 운용자 확인을 동시에 검사합니다." />
      <div className="check-list">
        {checks.map((check) => (
          <StatusRow key={check.label} label={check.label} status={check.status} detail={check.detail} />
        ))}
      </div>
    </section>
  );
}

function BrokerPanel({ brokers }) {
  return (
    <section className="panel broker-panel">
      <PanelHeader title="브로커/API 연결" subtitle="실거래 API 키와 주문 어댑터 준비 상태입니다." />
      <div className="broker-list">
        {brokers.map((broker) => (
          <div className="broker-row" key={broker.broker_id}>
            <div className="broker-title">
              <KeyRound size={17} />
              <div>
                <strong>{broker.name}</strong>
                <span>{broker.role}</span>
              </div>
              <StatusPill tone={statusTone(broker.status)}>{broker.status}</StatusPill>
            </div>
            <p>{broker.detail}</p>
            <div className="env-list">
              {broker.required_env.map((name) => (
                <span className={broker.missing_env.includes(name) ? "missing" : ""} key={name}>
                  {name}
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function RiskPanel({ checks }) {
  return (
    <section className="panel risk-panel">
      <PanelHeader title="Pre-Trade Risk Gate" subtitle="주문 전 차단 규칙은 항상 브로커 전송보다 먼저 실행됩니다." />
      <div className="risk-grid">
        {checks.map((check) => (
          <div className={`risk-rule ${check.status}`} key={check.label}>
            <AlertTriangle size={16} />
            <div>
              <strong>{check.label}</strong>
              <span>{check.detail}</span>
            </div>
            <em>{check.value}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function StrategyPanel({ strategies }) {
  return (
    <section className="panel strategy-panel">
      <PanelHeader title="전략 Artifact" subtitle="Backtester/Paper 승인 결과와 live_allowed 계약을 확인합니다." />
      <div className="data-table strategy-table">
        <div className="table-head">
          <span>전략</span>
          <span>심볼</span>
          <span>상태</span>
          <span>Score</span>
          <span>권한</span>
          <span>차단 사유</span>
        </div>
        {strategies.map((strategy) => (
          <div className="table-row" key={strategy.strategy_id}>
            <strong>{strategy.name}</strong>
            <span>{strategy.symbol}</span>
            <span>{strategy.lifecycle_status}</span>
            <span>{strategy.score}</span>
            <StatusPill tone={strategy.live_allowed ? "success" : "danger"}>{strategy.permission_label}</StatusPill>
            <em>{strategy.block_reason}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function OrderPanel({ orders }) {
  return (
    <section className="panel order-panel">
      <PanelHeader title="Order Blotter" subtitle="차단된 주문도 감사 추적을 위해 남깁니다." />
      <div className="data-table order-table">
        <div className="table-head">
          <span>시간</span>
          <span>주문ID</span>
          <span>전략</span>
          <span>심볼</span>
          <span>방향</span>
          <span>상태</span>
          <span>사유</span>
        </div>
        {orders.length === 0 ? (
          <EmptyRow text="아직 주문 이벤트가 없습니다. 테스트 주문 게이트를 누르면 차단 이벤트가 생성됩니다." />
        ) : (
          orders.map((order) => (
            <div className="table-row" key={order.order_id}>
              <span>{order.time}</span>
              <strong>{order.order_id}</strong>
              <span>{order.strategy_id}</span>
              <span>{order.symbol}</span>
              <span className="side-buy">{order.side}</span>
              <StatusPill tone="danger">{order.state}</StatusPill>
              <em>{order.reason}</em>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function PositionPanel({ positions }) {
  return (
    <section className="panel position-panel">
      <PanelHeader title="포지션 대조" subtitle="프로그램 포지션과 브로커 계좌 포지션 비교가 필요합니다." />
      <div className="position-list">
        {positions.map((position) => (
          <div className="position-row" key={position.symbol}>
            <WalletCards size={16} />
            <div>
              <strong>{position.symbol}</strong>
              <span>{position.asset}</span>
            </div>
            <em>{position.program_qty} / {position.broker_qty}</em>
            <StatusPill tone="warning">{position.status}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function AuditPanel({ audit }) {
  return (
    <section className="panel audit-panel">
      <PanelHeader title="Audit Stream" subtitle="모드 전환, 주문 차단, 설정 변경을 시간순으로 추적합니다." />
      <div className="audit-list">
        {audit.map((item, index) => (
          <div className={`audit-row ${item.level}`} key={`${item.time}-${index}`}>
            <Siren size={15} />
            <span>{item.time}</span>
            <strong>{item.event}</strong>
            <em>{item.detail}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function StatusRow({ label, status, detail }) {
  return (
    <div className={`status-row ${status}`}>
      {status === "pass" ? <ShieldCheck size={16} /> : <ShieldAlert size={16} />}
      <div>
        <strong>{label}</strong>
        <span>{detail}</span>
      </div>
      <StatusPill tone={statusTone(status)}>{status}</StatusPill>
    </div>
  );
}

function EmptyRow({ text }) {
  return (
    <div className="empty-row">
      <TerminalSquare size={16} />
      <span>{text}</span>
    </div>
  );
}

function PanelHeader({ title, subtitle }) {
  return (
    <div className="panel-header">
      <div>
        <h2>{title}</h2>
        <p>{subtitle}</p>
      </div>
    </div>
  );
}

export default App;
