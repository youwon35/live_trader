import { installTaskNotifications } from "../../../packages/design/task-notifications.js";
import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App.jsx";
import "./styles.css";
import "../../../packages/design/allocation-workspace.css";
import "../../../packages/design/action-feedback.css";
import "../../../packages/design/ui-patterns.css";
import "../../../packages/design/program-console.css";
import "../../../packages/design/layout-editing.css";
import "./stock-console-parity.css";
import "../../../packages/design/readable-notices.css";
import "../../../packages/design/font-unification.css";
import "../../../packages/design/interface-polish.css";
import "./surface-polish.css";
import "../../../packages/design/moa-theme.css";
import "./moa-surfaces.css";
import { installLayoutEditingSupport } from "../../../packages/design/layout-editing.js";

installLayoutEditingSupport({ program: "live-trader" });

installTaskNotifications({ id: "live_trader", title: "Live Trader", location: "작업 상태·실행 기록에서 상세 결과를 확인하세요." });

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
