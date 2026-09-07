import { probeSurfacePolish } from './surface_polish_probe.mjs';
// Presentation-only probe for restoring the pre-unification UI while retaining one font.
export async function probeRestoredUi(page,label) {
 const presentation = await page.evaluate(label=>{
  const visible=e=>!e.closest('[hidden],[aria-hidden="true"]')&&e.getBoundingClientRect().width>0&&e.getBoundingClientRect().height>0&&getComputedStyle(e).visibility!=='hidden';
  const text=[...document.querySelectorAll('*')].filter(e=>visible(e)&&[...e.childNodes].some(n=>n.nodeType===Node.TEXT_NODE&&n.textContent.trim()));
  const fontFailures=text.filter(e=>!getComputedStyle(e).fontFamily.toLowerCase().startsWith('"malgun gothic"')).map(e=>({tag:e.tagName,class:e.className,font:getComputedStyle(e).fontFamily}));
  const logs=[...document.querySelectorAll('.audit-panel .level-pill,.ptw-event-ledger-panel .status-pill')].filter(visible).map(e=>({text:e.textContent.trim(),tone:e.dataset.tone||'',color:getComputedStyle(e).color,background:getComputedStyle(e).backgroundColor}));
  const tabs=[...document.querySelectorAll('[role="tablist"]')].filter(visible).map(e=>({label:e.getAttribute('aria-label'),display:getComputedStyle(e).display,height:Math.round(e.getBoundingClientRect().height),labels:[...e.querySelectorAll('[role="tab"]')].map(t=>t.textContent.trim())}));
  return {label,width:innerWidth,theme:document.documentElement.dataset.uiTheme,textNodes:text.length,fontFailures,logs,tabs};
 },label);
 if(process.env.SURFACE_POLISH_REVIEW==='1') presentation.surface = await probeSurfacePolish(page,label);
 return presentation;
}
export async function inspectRestoredTabs(page,label,results,theme,seen=new Set()) {
 const groups=await page.getByRole('tablist').evaluateAll(nodes=>nodes.map((e,index)=>({index,visible:e.getBoundingClientRect().width>0,label:e.getAttribute('aria-label'),tabs:[...e.querySelectorAll('[role="tab"]')].map(t=>t.textContent.trim())})).filter(e=>e.visible));
 for(const group of groups){
  const key=group.label||group.tabs.join("|");if(seen.has(key))continue;seen.add(key);
  const list=group.label?page.getByRole('tablist',{name:group.label,exact:true}):page.getByRole('tablist').nth(group.index);
  for(const [index,name] of group.tabs.entries()){
   const tab=list.getByRole('tab').nth(index);
   if(await tab.count()!==1||!await tab.isVisible()||await tab.isDisabled())continue;
   await tab.click();await page.evaluate(theme=>{document.documentElement.dataset.uiTheme=theme;},theme);
   results.push(await probeRestoredUi(page,`${label} / ${group.label} / ${name}`));
   await inspectRestoredTabs(page,`${label} / ${name}`,results,theme,seen);
  }
 }
}
