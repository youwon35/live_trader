import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import PaperCandidateEvidencePanel from "../../src/PaperCandidateEvidencePanel.jsx";
import "../../src/styles.css";
import "../../../../packages/design/program-console.css";
import "../../../../packages/design/readable-notices.css";
import "../../../../packages/design/font-unification.css";
import "../../../../packages/design/interface-polish.css";
import "../../src/surface-polish.css";
document.documentElement.dataset.uiTheme = window.reviewTheme || "light";
function Fixture() {
 const [selected,setSelected]=useState("");
 return <main style={{padding:24,maxWidth:1400,margin:"auto"}}><section className="panel"><h1>운용 전략 · 검토 대기 후보</h1><p>실제 계좌 연결 없는 오프라인 화면 검사</p><PaperCandidateEvidencePanel onRegistered={(id)=>{window.reviewSelected=id;setSelected(id);}}/><output data-selected-deployment>{selected}</output></section></main>;
}
createRoot(document.getElementById("root")).render(<Fixture/>);
