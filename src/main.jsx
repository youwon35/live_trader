import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App.jsx";
import "./styles.css";
import "../../../packages/design/action-feedback.css";
import "../../../packages/design/ui-patterns.css";
import "../../../packages/design/program-console.css";
import "../../../packages/design/layout-editing.css";
import { installLayoutEditingSupport } from "../../../packages/design/layout-editing.js";

installLayoutEditingSupport({ program: "live-trader" });

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
