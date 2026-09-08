import assert from 'node:assert/strict';

const textRegionCases = ["api-connection-banner", "emergency-stop-feedback active", "functional-test-route-notice", "functional-test-feedback is-error", "functional-test-feedback is-success", "crypto-first-live-warning", "crypto-first-live-feedback crypto-first-live-feedback--error", "crypto-first-live-feedback crypto-first-live-feedback--ok", "inline-state danger", "validation-evaluation-error ts-semantic-surface"];
const checkedTextRegions = new WeakMap();
async function verifyTextRegionVariants(page, label) {
 const key = await page.evaluate(() => `${innerWidth}:${document.documentElement.dataset.uiTheme}`);
 const seen = checkedTextRegions.get(page) || new Set();
 if (seen.has(key)) return [];
 const variants = await page.evaluate(cases => {
  const host=document.createElement('section');host.setAttribute('aria-label','오프라인 오류 문장 스타일 검사');document.body.append(host);
  try { return cases.map(name => {
   const node=document.createElement('div');node.className=name+' ts-text-region';node.textContent='연결 또는 검증 결과를 확인하세요.';host.append(node);
   const style=getComputedStyle(node);
   return {className:node.className,background:style.backgroundColor,borderWidth:style.borderTopWidth,shadow:style.boxShadow,color:style.color};
  }); } finally {host.remove();}
 },textRegionCases);
 for(const item of variants) {
  assert.equal(item.background,'rgba(0, 0, 0, 0)',`${label}: rare message background ${item.className}`);
  assert.equal(item.borderWidth,'0px',`${label}: rare message border ${item.className}`);
  assert.equal(item.shadow,'none',`${label}: rare message shadow ${item.className}`);
 }
 seen.add(key);checkedTextRegions.set(page,seen);return variants;
}


// Read computed presentation only. No broker action, rejection or retirement is clicked.
export async function probeSurfacePolish(page, label) {
 await page.evaluate(async () => { await new Promise(resolve => requestAnimationFrame(resolve)); await Promise.all(document.getAnimations().filter(animation => animation instanceof CSSTransition).map(animation => animation.finished.catch(() => undefined))); });
 const result = await page.evaluate(label => {
  const visible = e => !e.closest('[hidden],[aria-hidden="true"]') && e.getBoundingClientRect().width > 0 && e.getBoundingClientRect().height > 0 && getComputedStyle(e).visibility !== 'hidden';
  const style = e => { const s = getComputedStyle(e); return {text:e.textContent.trim().slice(0,100),className:e.className,background:s.backgroundColor,border:s.borderTopColor,borderWidth:s.borderTopWidth,shadow:s.boxShadow,color:s.color}; };
  const rgb = value => { const node=document.createElement('i');node.style.color=value;document.body.append(node);const result=getComputedStyle(node).color;node.remove();return result; };
  const root = getComputedStyle(document.documentElement);
  const expectedAccent = rgb(root.getPropertyValue('--ts-selection-accent').trim() || '#2f80ed');
  const selected = [...document.querySelectorAll('button[aria-pressed="true"],button[role="tab"][aria-selected="true"],button[data-ts-selected="true"]')].filter(visible).filter(e=>!e.matches('.danger-button,.danger-action,.ts-danger-button,.trash-icon-button')).map(style);
  const expectedForeground = rgb(root.getPropertyValue('--ts-selection-contrast').trim() || '#ffffff');
  const primary = [...document.querySelectorAll('.primary-button,.primary-action,.run-button,.doctor-run-button,.ts-ui-button--primary,.ts-action-button--primary,[data-ts-action-component="true"][data-ts-variant="primary"]')].filter(visible).filter(e=>!e.matches('.danger-button,.danger-action,.ts-danger-button')).map(style);
  const textRegions = [...document.querySelectorAll('.ts-text-region')].filter(visible).map(style);
  const descriptions = [...document.querySelectorAll('.ts-static-description')].filter(visible).map(style);
  const danger = [...document.querySelectorAll('button.danger-button,button.ts-danger-button,button.danger-action,button.trash-icon-button')].filter(visible).map(style);
  const gaps = [...document.querySelectorAll('.artifact-detail-disclosure + .shared-strategy-actions,.live-compact-disclosure + .live-strategy-control-line')].filter(visible).filter(e=>getComputedStyle(e.previousElementSibling).position!=="fixed").map(e=>({gap:e.getBoundingClientRect().top-e.previousElementSibling.getBoundingClientRect().bottom,text:e.textContent.trim().slice(0,70)}));
  const grids = [...document.querySelectorAll('.ts-appearance-settings__grid')].filter(visible).map(e=>({columns:getComputedStyle(e).gridTemplateColumns.split(' ').length,groups:e.children.length,minGroupWidth:Math.min(...[...e.children].map(child=>child.getBoundingClientRect().width))}));
  return {label,width:innerWidth,theme:document.documentElement.dataset.uiTheme,expectedAccent,expectedForeground,primary,selected,descriptions,textRegions,danger,gaps,grids};
 }, label);
 for (const item of [...result.descriptions,...result.textRegions]) {
  assert.equal(item.borderWidth,"0px",`${label}: text region border width ${item.className}`);
  assert.equal(item.shadow,"none",`${label}: text region shadow ${item.className}`);
  assert.equal(item.background,'rgba(0, 0, 0, 0)',`${label}: static description background ${item.className}`);
  assert.equal(item.border,'rgba(0, 0, 0, 0)',`${label}: static description border ${item.className}`);
 }
 for (const item of result.danger) {
  assert.equal(item.background,'rgb(239, 68, 68)',`${label}: danger background ${item.text}`);
  assert.equal(item.border,'rgb(156, 163, 175)',`${label}: danger border ${item.text}`);
 }
 for (const item of result.primary) { assert.equal(item.background,result.expectedAccent,`${label}: exact primary action accent ${item.text}`); assert.equal(item.color,result.expectedForeground,`${label}: exact accent action contrast ${item.text}`); }
 for (const item of result.selected) assert.equal(item.background,result.expectedAccent,`${label}: exact selected accent ${item.text}`);
 for (const item of result.gaps) assert.ok(item.gap >= 11,`${label}: evidence and actions gap ${item.gap}`);
 for (const item of result.grids) { assert.equal(item.groups,3);assert.equal(item.columns,result.width>=840?3:1);assert.ok(item.minGroupWidth>=180,`${label}: theme group too narrow: ${item.minGroupWidth}`); }
 result.textRegionVariants=await verifyTextRegionVariants(page,label);
 return result;
}

export async function verifyAppearancePolish(page, label, theme, openPanel) {
 await openPanel();
 const panel=page.locator('.ts-appearance-settings');
 const picker=panel.getByLabel('사용자 강조 색상',{exact:true});
 const checks=[];
 for (const hex of ['#000080','#d0ef20']) {
  await picker.fill(hex);
  await page.waitForFunction(hex=>getComputedStyle(document.documentElement).getPropertyValue('--ts-selection-accent').trim()===hex,hex);
  const probe=await probeSurfacePolish(page,`${label} / ${hex}`);checks.push(probe);
  assert.equal(await panel.locator('.ts-appearance-settings__option[aria-pressed="true"]').evaluate(e=>getComputedStyle(e).color),hex==='#000080'?'rgb(255, 255, 255)':'rgb(0, 0, 0)');
 }
 await panel.getByRole('button',{name:theme==='light'?'화이트':'다크',exact:true}).click();
 const edit=panel.locator('[data-layout-control="editor"]');
 await edit.click();assert.equal(await edit.getAttribute('aria-pressed'),'true');
 await edit.click();assert.equal(await edit.getAttribute('aria-pressed'),'false');
 page.once('dialog',dialog=>dialog.accept());
 await panel.getByRole('button',{name:'초기화',exact:true}).click();
 await page.reload();await openPanel();
 assert.equal(await page.locator('.ts-appearance-settings').getByLabel('사용자 강조 색상',{exact:true}).inputValue(),'#d0ef20');
 assert.equal(await page.evaluate(()=>document.documentElement.dataset.uiTheme),theme);
 checks.push(await probeSurfacePolish(page,`${label} / reload persistence`));
 return checks;
}


// Select real detail cards, comparing each embedded status badge before/after.
export async function verifySelectedCards(page, selector, label) {
 const styles = await page.evaluate(() => ['--ts-selection-accent','--ts-selection-contrast'].map(name=>[name,document.documentElement.style.getPropertyValue(name)]));
 const result=[];
 try {
  for(const [background,foreground] of [['#000080','#ffffff'],['#ffdd00','#000000']]) {
   await page.evaluate(({background,foreground})=>{document.documentElement.style.setProperty('--ts-selection-accent',background);document.documentElement.style.setProperty('--ts-selection-contrast',foreground);},{background,foreground});
   const cards=page.locator(selector);
   assert.ok(await cards.count()>=2,`${label}: requires two real selectable cards`);
   const badges=await cards.evaluateAll(nodes=>nodes.map(e=>[...e.querySelectorAll('.status-pill')].map(b=>({text:b.textContent,color:getComputedStyle(b).color,background:getComputedStyle(b).backgroundColor}))));
   for(let index=0;index<2;index++) {
    await cards.nth(index).click();
    await page.evaluate(async()=>{await new Promise(resolve=>requestAnimationFrame(resolve));await Promise.all(document.getAnimations().filter(a=>a instanceof CSSTransition).map(a=>a.finished.catch(()=>undefined)));});
    assert.equal(await cards.nth(index).getAttribute('data-ts-selected'),'true');
    const state=await cards.nth(index).evaluate(e=>({background:getComputedStyle(e).backgroundColor,titleColor:getComputedStyle(e.querySelector('strong')).color,badges:[...e.querySelectorAll('.status-pill')].map(b=>({text:b.textContent,color:getComputedStyle(b).color,background:getComputedStyle(b).backgroundColor}))}));
    assert.equal(state.background,background==='#000080'?'rgb(0, 0, 128)':'rgb(255, 221, 0)');
    assert.equal(state.titleColor,foreground==='#ffffff'?'rgb(255, 255, 255)':'rgb(0, 0, 0)');
    assert.deepEqual(state.badges,badges[index],`${label}: badge semantics survive card selection`);
    assert.ok(state.badges.length>0,`${label}: a semantic badge must remain visible`);
    result.push({label,width:await page.evaluate(()=>innerWidth),theme:await page.evaluate(()=>document.documentElement.dataset.uiTheme),index,accent:background,...state});
   }
  }
 } finally {await page.evaluate(styles=>styles.forEach(([name,value])=>document.documentElement.style.setProperty(name,value)),styles);}
 return result;
}

export async function verifyPrimaryAccents(page, label) {
 const saved=await page.evaluate(()=>['--ts-selection-accent','--ts-selection-contrast'].map(key=>[key,document.documentElement.style.getPropertyValue(key)]));
 const checks=[];
 try {
  for(const [accent,contrast] of [['#000080','#ffffff'],['#ffdd00','#000000']]) {
   await page.evaluate(({accent,contrast})=>{document.documentElement.style.setProperty('--ts-selection-accent',accent);document.documentElement.style.setProperty('--ts-selection-contrast',contrast);},{accent,contrast});
   const result=await probeSurfacePolish(page,`${label} / primary ${accent}`);
   checks.push({accent,expectedForeground:result.expectedForeground,primary:result.primary});
  }
 } finally {
  await page.evaluate(values=>values.forEach(([key,value])=>document.documentElement.style.setProperty(key,value)),saved);
  await page.evaluate(async()=>{await new Promise(resolve=>requestAnimationFrame(resolve));await Promise.all(document.getAnimations().filter(a=>a instanceof CSSTransition).map(a=>a.finished.catch(()=>undefined)));});
 }
 return checks;
}
