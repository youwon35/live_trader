import { useEffect, useState } from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Bell,
  CircleStop,
  ClipboardCheck,
  Clock3,
  DatabaseZap,
  Download,
  FileClock,
  KeyRound,
  LayoutDashboard,
  ListChecks,
  LockKeyhole,
  Moon,
  Network,
  Play,
  Power,
  Radio,
  RefreshCcw,
  RotateCcw,
  Route,
  Search,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Siren,
  Sun,
  TerminalSquare,
  WalletCards,
} from "lucide-react";
import {
  cancelOrder,
  exportAudit,
  getSnapshot,
  retryOrder,
  setChecklistItem,
  setFlag,
  setMode,
  setRetryPolicy,
  setRiskSetting,
  submitTestIntent,
} from "./api";

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
  dry_run: true,
  kill_switch: false,
  new_entries_blocked: true,
  operator_confirmed: false,
  summary: { status: "blocked", blocker_count: 1, warning_count: 0, live_strategy_count: 0, broker_ready_count: 0 },
  sessions: [],
  readiness: [{ label: "Python API", status: "fail", detail: "Python server connection is required." }],
  risk_checks: [],
  risk_settings: [],
  checklist: [],
  retry_policy: [],
  order_queue: { total: 0, blocked: 0, dry_run: 0, retryable: 0, canceled: 0 },
  dry_run_ledger: [],
  brokers: [],
  strategies: [],
  orders: [],
  positions: [],
  audit: [],
};

const themeStorageKey = "live-trader.ui-theme.v1";

function getInitialTheme() {
  if (typeof window === "undefined") return "dark";
  const savedTheme = window.localStorage.getItem(themeStorageKey);
  return savedTheme === "light" ? "light" : "dark";
}

function App() {
  const [snapshot, setSnapshot] = useState(fallbackSnapshot);
  const [selectedNav, setSelectedNav] = useState("overview");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [theme, setTheme] = useState(getInitialTheme);
  const [exportResult, setExportResult] = useState(null);

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

  useEffect(() => {
    if (theme === "light") {
      document.documentElement.setAttribute("data-ui-theme", "light");
    } else {
      document.documentElement.removeAttribute("data-ui-theme");
    }
    window.localStorage.setItem(themeStorageKey, theme);
  }, [theme]);

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

  async function runAuditExport(format) {
    setLoading(true);
    try {
      const result = await exportAudit(format);
      if (result.snapshot) setSnapshot(result.snapshot);
      setExportResult(result);
      setError(result.ok === false ? result.reason : "");
      if (result.ok !== false && result.content) {
        downloadExport(result);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "내보내기 실패");
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
            <button
              className="icon-button"
              type="button"
              aria-label={theme === "light" ? "다크 모드로 전환" : "화이트 모드로 전환"}
              title={theme === "light" ? "다크 모드" : "화이트 모드"}
              onClick={() => setTheme((value) => (value === "light" ? "dark" : "light"))}
            >
              {theme === "light" ? <Moon size={17} /> : <Sun size={17} />}
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

        <WorkspaceContent
          selectedNav={selectedNav}
          snapshot={snapshot}
          canLive={canLive}
          canFullLive={canFullLive}
          onMode={(mode) => runAction(() => setMode(mode))}
          onConfirm={() => runAction(() => setFlag("operator_confirmed", !snapshot.operator_confirmed))}
          onDryRun={() => runAction(() => setFlag("dry_run", !snapshot.dry_run))}
          onEntryBlock={() => runAction(() => setFlag("new_entries_blocked", !snapshot.new_entries_blocked))}
          onTestIntent={() => runAction(submitTestIntent)}
          onRiskSetting={(name, value) => runAction(() => setRiskSetting(name, value))}
          onChecklist={(name, value) => runAction(() => setChecklistItem(name, value))}
          onRetryPolicy={(name, value) => runAction(() => setRetryPolicy(name, value))}
          onRetryOrder={(orderId) => runAction(() => retryOrder(orderId))}
          onCancelOrder={(orderId) => runAction(() => cancelOrder(orderId))}
          onAuditExport={runAuditExport}
          exportResult={exportResult}
        />
      </main>
    </div>
  );
}

function downloadExport(result) {
  const blob = new Blob([result.content], { type: result.mime || "text/plain;charset=utf-8" });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = result.filename || `live-trader-audit.${result.format || "txt"}`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
}

function WorkspaceContent({
  selectedNav,
  snapshot,
  canLive,
  canFullLive,
  onMode,
  onConfirm,
  onDryRun,
  onEntryBlock,
  onTestIntent,
  onRiskSetting,
  onChecklist,
  onRetryPolicy,
  onRetryOrder,
  onCancelOrder,
  onAuditExport,
  exportResult,
}) {
  const modeConsole = (
    <ModeConsole
      mode={snapshot.mode}
      canLive={canLive}
      canFullLive={canFullLive}
      onMode={onMode}
      onConfirm={onConfirm}
      dryRun={snapshot.dry_run}
      onDryRun={onDryRun}
      operatorConfirmed={snapshot.operator_confirmed}
      newEntriesBlocked={snapshot.new_entries_blocked}
      onEntryBlock={onEntryBlock}
      onTestIntent={onTestIntent}
    />
  );

  if (selectedNav === "gate") {
    return (
      <section className="content-grid">
        <div className="content-column">
          {modeConsole}
          <RunbookChecklistPanel checklist={snapshot.checklist} onChecklist={onChecklist} />
          <ReadinessPanel checks={snapshot.readiness} />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <GateRunbookPanel />
          <RiskSettingsPanel settings={snapshot.risk_settings} onRiskSetting={onRiskSetting} />
          <RiskPanel checks={snapshot.risk_checks} />
          <PositionPanel positions={snapshot.positions} />
        </div>
      </section>
    );
  }

  if (selectedNav === "orders") {
    return (
      <section className="content-grid">
        <div className="content-column">
          <OrderCommandPanel
            newEntriesBlocked={snapshot.new_entries_blocked}
            dryRun={snapshot.dry_run}
            killSwitch={snapshot.kill_switch}
            onDryRun={onDryRun}
            onEntryBlock={onEntryBlock}
            onTestIntent={onTestIntent}
          />
          <OrderQueueSummaryPanel summary={snapshot.order_queue} />
          <OrderPanel orders={snapshot.orders} onRetryOrder={onRetryOrder} onCancelOrder={onCancelOrder} />
          <RiskPanel checks={snapshot.risk_checks} />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <RetryPolicyPanel policy={snapshot.retry_policy} onRetryPolicy={onRetryPolicy} />
          <DryRunLedgerPanel ledger={snapshot.dry_run_ledger} />
          <PositionPanel positions={snapshot.positions} />
        </div>
      </section>
    );
  }

  if (selectedNav === "brokers") {
    return (
      <section className="content-grid">
        <div className="content-column">
          <BrokerPanel brokers={snapshot.brokers} />
          <BrokerRequirementsPanel brokers={snapshot.brokers} />
        </div>
        <div className="content-column">
          <PositionPanel positions={snapshot.positions} />
          <ReadinessPanel checks={snapshot.readiness} />
        </div>
      </section>
    );
  }

  if (selectedNav === "strategies") {
    return (
      <section className="content-grid">
        <div className="content-column">
          <StrategyPanel strategies={snapshot.strategies} />
          <StrategyWorkflowPanel />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <ReadinessPanel checks={snapshot.readiness} />
        </div>
      </section>
    );
  }

  if (selectedNav === "audit") {
    return (
      <section className="content-grid">
        <div className="content-column">
          <AuditPanel audit={snapshot.audit} />
          <AuditExportPanel onExport={onAuditExport} exportResult={exportResult} />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <RiskPanel checks={snapshot.risk_checks} />
        </div>
      </section>
    );
  }

  return (
    <>
      <section className="command-grid">
        {modeConsole}
        <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
      </section>

      <section className="content-grid">
        <div className="content-column">
          <ReadinessPanel checks={snapshot.readiness} />
          <RunbookChecklistPanel checklist={snapshot.checklist} onChecklist={onChecklist} />
          <RiskPanel checks={snapshot.risk_checks} />
          <StrategyPanel strategies={snapshot.strategies} />
          <OrderPanel orders={snapshot.orders} onRetryOrder={onRetryOrder} onCancelOrder={onCancelOrder} />
          <AuditPanel audit={snapshot.audit} />
        </div>
        <div className="content-column">
          <BrokerPanel brokers={snapshot.brokers} />
          <PositionPanel positions={snapshot.positions} />
        </div>
      </section>
    </>
  );
}

function StatusPill({ children, tone = "neutral" }) {
  return <span className={`status-pill ${tone}`}>{children}</span>;
}

function statusTone(status) {
  if (status === "pass" || status === "connected" || status === "open" || status === "dry_run") return "success";
  if (status === "warn" || status === "watch" || status === "adapter_required" || status === "adapter_blocked") return "warning";
  if (status === "fail" || status === "blocked" || status === "missing_credentials" || status === "risk_blocked" || status === "retry_exhausted") {
    return "danger";
  }
  if (status === "canceled") return "neutral";
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
  dryRun,
  onDryRun,
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
        <button className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`} type="button" onClick={onDryRun}>
          <ShieldCheck size={16} />
          Dry Run
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

function GateRunbookPanel() {
  const items = [
    ["API", "실계좌 키와 주문 어댑터 확인"],
    ["권한", "live_allowed 전략만 통과"],
    ["대조", "브로커 포지션과 프로그램 포지션 비교"],
    ["승인", "운용자 확인 후 SMALL_LIVE부터 시작"],
  ];
  return (
    <section className="panel">
      <PanelHeader title="Live Gate 체크라인" subtitle="실거래 전환 전 필요한 운영 조건입니다." />
      <div className="compact-list">
        {items.map(([label, detail]) => (
          <div className="compact-row" key={label}>
            <strong>{label}</strong>
            <span>{detail}</span>
            <StatusPill tone="neutral">필수</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function RunbookChecklistPanel({ checklist, onChecklist }) {
  return (
    <section className="panel checklist-panel">
      <PanelHeader title="운영 체크리스트" subtitle="필수 항목이 모두 확인되어야 실거래 게이트를 통과할 수 있습니다." />
      <div className="checklist-list">
        {checklist.map((item) => (
          <label className={`checklist-row ${item.checked ? "checked" : ""}`} key={item.key}>
            <input
              type="checkbox"
              checked={item.checked}
              onChange={(event) => onChecklist(item.key, event.currentTarget.checked)}
            />
            <ClipboardCheck size={16} />
            <div>
              <strong>{item.label}</strong>
              <span>{item.detail}</span>
            </div>
            <StatusPill tone={item.checked ? "success" : item.required ? "warning" : "neutral"}>
              {item.checked ? "완료" : item.required ? "필수" : "권장"}
            </StatusPill>
          </label>
        ))}
      </div>
    </section>
  );
}

function RiskSettingsPanel({ settings, onRiskSetting }) {
  function commitChange(event, setting) {
    const nextValue = event.currentTarget.value;
    if (Number(nextValue) !== Number(setting.value)) {
      onRiskSetting(setting.key, nextValue);
    }
  }

  return (
    <section className="panel risk-settings-panel">
      <PanelHeader title="리스크 한도 설정" subtitle="주문 전 게이트에서 사용하는 기본 안전 한도입니다." />
      <div className="settings-list">
        {settings.map((setting) => (
          <div className="setting-row" key={setting.key}>
            <SlidersHorizontal size={16} />
            <div>
              <strong>{setting.label}</strong>
              <span>{setting.detail}</span>
            </div>
            <label>
              <input
                key={`${setting.key}-${setting.value}`}
                type="number"
                defaultValue={setting.value}
                min={setting.min}
                max={setting.max}
                step={setting.step}
                onBlur={(event) => commitChange(event, setting)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") event.currentTarget.blur();
                }}
              />
              <span>{setting.unit}</span>
            </label>
          </div>
        ))}
      </div>
    </section>
  );
}

function OrderCommandPanel({ newEntriesBlocked, dryRun, killSwitch, onDryRun, onEntryBlock, onTestIntent }) {
  return (
    <section className="panel">
      <PanelHeader title="주문 제어" subtitle="실주문 전송 전 차단 상태와 테스트 게이트를 관리합니다." />
      <div className="operator-actions">
        <button className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`} type="button" onClick={onDryRun}>
          <ShieldCheck size={16} />
          Dry Run
        </button>
        <button className={`secondary-button ${newEntriesBlocked ? "active" : ""}`} type="button" onClick={onEntryBlock}>
          <ShieldCheck size={16} />
          신규 진입 차단
        </button>
        <button className="primary-button" type="button" onClick={onTestIntent}>
          <TerminalSquare size={16} />
          테스트 주문 게이트
        </button>
        <StatusPill tone={killSwitch ? "danger" : "success"}>{killSwitch ? "KILL ON" : "KILL OFF"}</StatusPill>
      </div>
    </section>
  );
}

function OrderQueueSummaryPanel({ summary }) {
  const items = [
    { label: "전체 주문", value: summary.total, tone: summary.total ? "info" : "neutral" },
    { label: "차단", value: summary.blocked, tone: summary.blocked ? "danger" : "success" },
    { label: "Dry Run", value: summary.dry_run, tone: summary.dry_run ? "success" : "neutral" },
    { label: "재시도 가능", value: summary.retryable, tone: summary.retryable ? "warning" : "neutral" },
    { label: "취소", value: summary.canceled, tone: summary.canceled ? "neutral" : "success" },
  ];
  return (
    <section className="panel order-queue-panel">
      <PanelHeader title="주문 큐 요약" subtitle="주문 의도의 현재 생명주기 상태입니다." />
      <div className="queue-grid">
        {items.map((item) => (
          <div className="queue-card" key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <StatusPill tone={item.tone}>QUEUE</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function RetryPolicyPanel({ policy, onRetryPolicy }) {
  function commitNumber(event, setting) {
    const nextValue = event.currentTarget.value;
    if (Number(nextValue) !== Number(setting.value)) {
      onRetryPolicy(setting.key, nextValue);
    }
  }

  return (
    <section className="panel retry-policy-panel">
      <PanelHeader title="재시도 정책" subtitle="브로커 전송 전 단계에서 사용할 재시도 기준입니다." />
      <div className="settings-list">
        {policy.map((setting) => (
          <div className="setting-row" key={setting.key}>
            <Clock3 size={16} />
            <div>
              <strong>{setting.label}</strong>
              <span>{setting.detail}</span>
            </div>
            {setting.type === "boolean" ? (
              <label className="switch-label">
                <input
                  type="checkbox"
                  checked={setting.value}
                  onChange={(event) => onRetryPolicy(setting.key, event.currentTarget.checked)}
                />
                <span>{setting.value ? "ON" : "OFF"}</span>
              </label>
            ) : (
              <label>
                <input
                  key={`${setting.key}-${setting.value}`}
                  type="number"
                  defaultValue={setting.value}
                  min={setting.min}
                  max={setting.max}
                  step={setting.step}
                  onBlur={(event) => commitNumber(event, setting)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") event.currentTarget.blur();
                  }}
                />
                <span>{setting.unit}</span>
              </label>
            )}
          </div>
        ))}
      </div>
    </section>
  );
}

function DryRunLedgerPanel({ ledger }) {
  return (
    <section className="panel dry-ledger-panel">
      <PanelHeader title="Dry Run 주문 원장" subtitle="브로커 전송 없이 기록된 주문 의도와 차단 결과입니다." />
      <div className="compact-list">
        {ledger.length === 0 ? (
          <EmptyRow text="아직 Dry Run 주문 의도가 없습니다." />
        ) : (
          ledger.map((order) => (
            <div className="compact-row ledger-row" key={order.order_id}>
              <strong>{order.symbol}</strong>
              <span>{order.order_id} · {order.attempts}/{order.max_attempts}회 · {order.reason}</span>
              <StatusPill tone={statusTone(order.state)}>{order.state}</StatusPill>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function BrokerRequirementsPanel({ brokers }) {
  return (
    <section className="panel">
      <PanelHeader title="브로커 준비 항목" subtitle="실제 주문 연결 전에 비어 있는 환경 값을 확인합니다." />
      <div className="compact-list">
        {brokers.map((broker) => (
          <div className="compact-row" key={broker.broker_id}>
            <strong>{broker.name}</strong>
            <span>{broker.missing_env.length ? `${broker.missing_env.length}개 값 필요` : "환경 값 입력됨"}</span>
            <StatusPill tone={broker.order_ready ? "success" : "danger"}>{broker.order_ready ? "ready" : "blocked"}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function StrategyWorkflowPanel() {
  const steps = [
    ["BACKTEST", "최종 테스트 통과"],
    ["SHADOW", "실시간 신호 기록"],
    ["PAPER", "모의 체결 검증"],
    ["LIVE", "live_allowed 승인"],
  ];
  return (
    <section className="panel">
      <PanelHeader title="전략 승급 흐름" subtitle="실거래 전략은 승인 단계와 계약 권한을 모두 통과해야 합니다." />
      <div className="workflow-strip">
        {steps.map(([label, detail], index) => (
          <div className="workflow-step" key={label}>
            <span>{index + 1}</span>
            <strong>{label}</strong>
            <em>{detail}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function AuditExportPanel({ onExport, exportResult }) {
  return (
    <section className="panel">
      <PanelHeader title="감사 로그 내보내기" subtitle="주문 차단, 모드 변경, 설정 변경을 CSV/HTML로 저장합니다." />
      <div className="compact-list">
        <div className="compact-row">
          <strong>CSV</strong>
          <span>운영 이벤트 원장</span>
          <button className="mini-button" type="button" onClick={() => onExport("csv")}>
            <Download size={14} />
            저장
          </button>
        </div>
        <div className="compact-row">
          <strong>HTML</strong>
          <span>인쇄용 운용 리포트</span>
          <button className="mini-button" type="button" onClick={() => onExport("html")}>
            <Download size={14} />
            저장
          </button>
        </div>
      </div>
      {exportResult?.ok !== false && exportResult?.filename && (
        <div className="export-result">
          <Download size={15} />
          <span>{exportResult.filename} 생성 완료</span>
        </div>
      )}
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

function OrderPanel({ orders, onRetryOrder, onCancelOrder }) {
  return (
    <section className="panel order-panel">
      <PanelHeader title="Order Blotter" subtitle="차단, Dry Run, 재시도, 취소 이벤트를 감사 추적합니다." />
      <div className="order-ledger-list">
        {orders.length === 0 ? (
          <EmptyRow text="아직 주문 이벤트가 없습니다. 테스트 주문 게이트를 누르면 차단 이벤트가 생성됩니다." />
        ) : (
          orders.map((order) => (
            <div className="order-ledger-row" key={order.order_id}>
              <div className="order-ledger-head">
                <div>
                  <strong>{order.order_id}</strong>
                  <span>{order.time} · {order.strategy_id}</span>
                </div>
                <StatusPill tone={statusTone(order.state)}>{order.state}</StatusPill>
              </div>
              <div className="order-ledger-meta">
                <span>{order.symbol}</span>
                <span className="side-buy">{order.side}</span>
                <span>큐 {order.queue_state}</span>
                <span>시도 {order.attempts}/{order.max_attempts}</span>
                <span>다음 {order.next_retry_at}</span>
              </div>
              <div className="order-ledger-foot">
                <em>{order.reason}</em>
                <div className="order-actions">
                  <button
                    className="mini-icon-button"
                    type="button"
                    title="재시도"
                    aria-label={`${order.order_id} 재시도`}
                    disabled={!order.retryable}
                    onClick={() => onRetryOrder(order.order_id)}
                  >
                    <RotateCcw size={13} />
                  </button>
                  <button
                    className="mini-icon-button"
                    type="button"
                    title="취소"
                    aria-label={`${order.order_id} 취소`}
                    disabled={order.state === "canceled"}
                    onClick={() => onCancelOrder(order.order_id)}
                  >
                    <CircleStop size={13} />
                  </button>
                </div>
              </div>
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
