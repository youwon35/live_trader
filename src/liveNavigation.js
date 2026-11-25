// Route IDs are durable UI preferences and diagnostic links, not lifecycle stages.
// Keep legacy routes addressable while presenting fewer top-level decisions.
export const LIVE_WORKSPACE_ROUTE_IDS = Object.freeze([
  "overview", "gate", "functional-test", "monitor", "risk", "automation", "accounts",
  "orders", "incidents", "audit", "settings",
]);

export function liveNavigationRoot(route) {
  if (route === "risk" || route === "functional-test") return "automation";
  if (route === "audit") return "incidents";
  return LIVE_WORKSPACE_ROUTE_IDS.includes(route) ? route : "overview";
}

export function liveNavigationRoute(route) {
  return LIVE_WORKSPACE_ROUTE_IDS.includes(route) ? route : "overview";
}

const EXECUTION_TABS = Object.freeze([
  Object.freeze({ id: "automation", label: "실행·중지" }),
  Object.freeze({ id: "risk", label: "한도·안전장치" }),
  Object.freeze({ id: "functional-test", label: "주문 연결 시험" }),
]);
export function liveSectionTabs(route) {
  const root = liveNavigationRoot(route);
  return root === "automation" ? EXECUTION_TABS : [];
}
