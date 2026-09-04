// Route IDs are durable UI preferences and diagnostic links, not lifecycle stages.
// Keep legacy routes addressable while presenting fewer top-level decisions.
export const LIVE_WORKSPACE_ROUTE_IDS = Object.freeze([
  "overview", "gate", "functional-test", "risk", "automation", "accounts",
  "orders", "incidents", "audit", "settings",
]);

export function liveNavigationRoot(route) {
  if (route === "risk") return "automation";
  if (route === "audit") return "incidents";
  return LIVE_WORKSPACE_ROUTE_IDS.includes(route) ? route : "overview";
}

export function liveNavigationRoute(route) {
  return LIVE_WORKSPACE_ROUTE_IDS.includes(route) ? route : "overview";
}

const EXECUTION_TABS = Object.freeze([
  Object.freeze({ id: "automation", label: "실행·중지" }),
  Object.freeze({ id: "risk", label: "한도·안전장치" }),
]);
const RECORD_TABS = Object.freeze([
  Object.freeze({ id: "incidents", label: "운영 이력" }),
  Object.freeze({ id: "audit", label: "상세 로그" }),
]);

export function liveSectionTabs(route) {
  const root = liveNavigationRoot(route);
  return root === "automation" ? EXECUTION_TABS : root === "incidents" ? RECORD_TABS : [];
}
