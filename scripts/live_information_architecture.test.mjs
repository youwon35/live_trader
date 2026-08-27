import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const appSource = await readFile(new URL("../src/App.jsx", import.meta.url), "utf8");
const functionalSource = await readFile(new URL("../src/FunctionalTestWorkspace.jsx", import.meta.url), "utf8");
const cryptoSource = await readFile(new URL("../src/CryptoFirstLivePanel.jsx", import.meta.url), "utf8");
const stylesSource = await readFile(new URL("../src/styles.css", import.meta.url), "utf8");

function between(source, start, end) {
  const startIndex = source.indexOf(start);
  const endIndex = source.indexOf(end, startIndex + start.length);
  assert.notEqual(startIndex, -1, `start marker not found: ${start}`);
  assert.notEqual(endIndex, -1, `end marker not found: ${end}`);
  return source.slice(startIndex, endIndex);
}

test("operator navigation follows the live decision sequence", () => {
  const navigation = between(appSource, "const navItems = [", "const pageProfiles =");
  const labels = [...navigation.matchAll(/\{ id: "[^"]+", label: "([^"]+)"/g)].map((match) => match[1]);
  assert.deepEqual(labels, [
    "실행 준비",
    "배포 검증",
    "주문 기능 검증",
    "위험 관리",
    "실거래 실행",
    "계좌 대조",
    "주문 추적",
    "운영 기록",
    "기술 로그",
    "연결 설정",
  ]);

  const groups = between(appSource, "const navGroups = [", "function strategyLifecycleRank");
  assert.match(groups, /label: "실행 전", itemIds: \["overview", "gate", "functional-test", "risk"\]/);
  assert.match(groups, /label: "실행·대조", itemIds: \["automation", "accounts", "orders"\]/);
  assert.match(groups, /label: "기록·설정", itemIds: \["incidents", "audit", "settings"\]/);
  assert.match(appSource, /aria-current=\{selectedNav === item\.id \? "page" : undefined\}/);
  assert.match(appSource, /<h1 id="live-page-title">\{title\}<\/h1>/);
  assert.match(appSource, /aria-labelledby="live-page-title"/);
});

test("persistent live hierarchy keeps every execution-critical state visible", () => {
  const environment = between(appSource, "function LiveEnvironmentBar", "function CompactDisclosure");
  for (const label of ["실거래 잠금", "Preflight", "신규 진입", "위험 증가 주문", "Broker 전송", "전역 Kill"]) {
    assert.match(environment, new RegExp(`label: "${label}"`));
  }
  assert.match(environment, /snapshot\.kill_switch[\s\S]*?snapshot\.api_connected === true[\s\S]*?value: "확인 불가"/);
});

test("default pages prioritize decisions and move technical evidence into disclosures", () => {
  const workspace = between(appSource, "function WorkspaceContent", "function DeploymentContextPanel");
  const automation = between(workspace, 'if (selectedNav === "automation")', 'if (selectedNav === "functional-test")');
  const gate = between(workspace, 'if (selectedNav === "gate")', 'if (selectedNav === "accounts")');
  const risk = between(workspace, 'if (selectedNav === "risk")', 'if (selectedNav === "incidents")');
  const settings = between(workspace, 'if (selectedNav === "settings")', "<OperationsOverviewPage");

  assert.doesNotMatch(automation, /DeploymentContextPanel/);
  assert.match(automation, /RuntimeComponentStatusPanel/);
  assert.match(automation, /WatchdogPanel/);
  assert.match(automation, /title="Binance 선물·자본 확대 도구"/);

  assert.doesNotMatch(gate, /DeploymentContextPanel|LivePromotionReadinessQueue|DataLineagePanel|PortfolioArtifactPanel/);
  assert.match(gate, /LivePreparationPanel/);
  assert.match(gate, /title="Deployment 기술 근거"/);

  assert.doesNotMatch(risk, /DeploymentContextPanel|WatchdogPanel/);
  assert.match(risk, /RiskUsagePanel/);
  assert.match(risk, /OperationalSafeguardsPanel/);
  assert.match(risk, /title="리스크 정책·재시도·선물 계산"/);

  assert.ok(settings.indexOf("BrokerConnectionAssistant") < settings.indexOf("CompactDisclosure"));
  assert.ok(settings.indexOf("DoctorHistoryPanel") < settings.indexOf("CompactDisclosure"));
  assert.match(settings, /title="화면·레이아웃·Telegram"/);

  const overview = between(appSource, "function OperationsOverviewPage", "function RuntimeComponentStatusPanel");
  assert.doesNotMatch(overview, /DeploymentContextPanel/);
  assert.match(overview, /PreTradeDoctorPanel/);
  assert.match(overview, /LaunchReportPanel/);
  assert.match(overview, /title="Preflight·Runtime 기술 근거"/);

  const accounts = between(appSource, "function UnifiedBrokerAccountPanel", "function OperationsReportPanel");
  assert.match(accounts, /현재 계좌를 기준 원장으로 승인/);
  assert.match(accounts, /title="자본 배분·포지션 노출"/);
});

test("functional tests expose explicit KIS and crypto routes without passive crypto mounting", () => {
  assert.match(appSource, /React\.lazy\(\(\) => import\("\.\/FunctionalTestWorkspace"\)\)/);
  assert.match(functionalSource, /React\.lazy\(\(\) => import\("\.\/CryptoFirstLivePanel"\)\)/);
  assert.match(functionalSource, /useState\("kis"\)/);
  assert.match(functionalSource, /if \(testRoute !== "kis"\) return undefined/);
  assert.match(functionalSource, /role="tablist" aria-label="기능시험 경로"/);
  assert.match(functionalSource, />\s*KIS 기간형\s*<\/button>/);
  assert.match(functionalSource, />\s*코인 2시간\s*<\/button>/);
  assert.match(functionalSource, /className="functional-test-safety-strip"/);
  assert.match(functionalSource, /promotionEligible=false/);
  assert.match(functionalSource, /<span>계정 범위<\/span>/);
  assert.match(functionalSource, /<span>세션<\/span>/);
  assert.match(functionalSource, /<span>승인 계약<\/span>/);
  assert.match(functionalSource, /<span>API·전송<\/span>/);
  assert.match(functionalSource, /apiFailClosed[\s\S]*?"FAIL-CLOSED"/);
  assert.match(functionalSource, /시작마다 네이티브 45초 1회/);
  assert.match(functionalSource, /허가서 준비 즉시 전체 시험 기간이 시작됩니다/);
  assert.match(functionalSource, /활성 코인 lane은 탭 전환만으로 중지되지 않습니다/);
  assert.match(functionalSource, /workspace\.runtime\.functionalTestRunning[\s\S]*?먼저 ‘오늘 실행 정지’/);
  assert.match(functionalSource, /testRoute === "crypto" && !cryptoSafety\.safeToLeave/);
  assert.match(functionalSource, /exact terminal 상태가 모두 IDLE 또는 FINALIZED/);
  assert.match(functionalSource, /aria-controls="functional-test-kis-panel"/);
  assert.match(functionalSource, /aria-controls="functional-test-crypto-panel"/);
  assert.match(functionalSource, /role="tabpanel"/);
  assert.match(functionalSource, /handleRouteTabKeyDown/);
  assert.match(functionalSource, /<CryptoFirstLivePanel onSafetyStateChange=\{setCryptoSafety\} \/>/);
  assert.equal((functionalSource.match(/<CryptoFirstLivePanel/g) || []).length, 1);
  assert.match(cryptoSource, /<dt>계정 Fingerprint<\/dt>/);
  assert.match(cryptoSource, /safeTerminalStates = new Set\(\["IDLE", "FINALIZED"\]\)/);
  assert.match(cryptoSource, /onSafetyStateChange\?\.\(safetyState\)/);
  assert.doesNotMatch(functionalSource, /className="functional-test-boundary"/);
  assert.match(functionalSource, /aria-expanded=\{showBinding\}/);
});

test("workspace owns scrolling and disclosures remain keyboard-accessible", () => {
  assert.match(appSource, /aria-expanded=\{open\}/);
  assert.match(appSource, /aria-controls=\{contentId\}/);
  assert.match(appSource, /className="live-compact-disclosure__content"[\s\S]*?\{hasOpened \? children : null\}/);
  assert.match(stylesSource, /Cascade closure for the compact Live workspace/);
  assert.match(stylesSource, /\.page-view,[\s\S]*?overflow: auto !important/);
  assert.match(stylesSource, /\.live-compact-disclosure__trigger:focus-visible/);
});

test("passive panels avoid repeated layout work", () => {
  const editablePanels = between(appSource, "function ensurePanelHandles", "applyAppearance(readAppearance())");
  assert.match(editablePanels, /panel\.dataset\.layoutEnhanced = "true"/);
  assert.match(editablePanels, /panel\.dataset\.layoutEnhanced !== "true"/);
  assert.match(editablePanels, /new MutationObserver\(\(\) => enhancePanels\(\)\)/);
  assert.match(editablePanels, /if \(Math\.abs\(restoredOffset\.x\) > 0 \|\| Math\.abs\(restoredOffset\.y\) > 0\)/);
  assert.match(stylesSource, /:root\[data-program="live-trader"\] \.topbar \{[\s\S]*?backdrop-filter: none;/);
});

test("single-axis resizing freezes its orthogonal axis without persisting peer geometry", () => {
  const editor = between(appSource, "function useEditablePanels", "function App()");
  assert.match(editor, /panel\.style\.width = `\$\{bounds\.width\}px`/);
  assert.match(editor, /panel\.style\.height = `\$\{bounds\.height\}px`/);
  assert.match(editor, /peerInlineDimensions = new Map/);
  assert.match(editor, /slot\.element\.style\.width = original\.width/);
  assert.match(editor, /slot\.element\.style\.height = original\.height/);
  assert.doesNotMatch(editor, /stored\[panelLayoutKey\(slot\.element\)\]/);
});
