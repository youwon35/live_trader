// Read computed presentation only; this helper never invokes an app command.
export async function probeUnifiedControls(page, label) {
 return page.evaluate(label => {
  const isVisible = element => !element.closest('[hidden],[aria-hidden="true"]') && element.getBoundingClientRect().width > 0 && element.getBoundingClientRect().height > 0 && getComputedStyle(element).visibility !== 'hidden';
  const canvas = document.createElement('canvas'); canvas.width = canvas.height = 1;
  const paint = canvas.getContext('2d', {willReadFrequently:true});
  const rgba = value => { paint.clearRect(0,0,1,1); paint.fillStyle=value; paint.fillRect(0,0,1,1); return [...paint.getImageData(0,0,1,1).data]; };
  const selector = 'button,[role="button"],.status-pill,.ts-ui-pill,.ptw-status-badge,.ptw-metric-card,.ts-semantic-surface,.ts-status-card,.ts-status-row,[role="alert"],[role="status"],[role="note"],.alert-band,.inline-state,.notification-badge,.bridge-status-pill,.live-environment-badge,.functional-test-status,.functional-test-state,.functional-test-feedback,.crypto-first-live-feedback,.ptw-inline-note,.ptw-paper-pill,.functional-test-caps>span,.strategy-reason-chip';
  const controls=[...document.querySelectorAll(selector)].filter(isVisible);
  const failures=[];
  for(const element of controls){
   const style=getComputedStyle(element), bg=rgba(style.backgroundColor), border=rgba(style.borderTopColor);
   const checks={text:style.color==='rgb(0, 0, 0)',background:bg[3]===255,border:parseFloat(style.borderTopWidth)>=1&&border[3]===255&&Math.max(...border.slice(0,3))-Math.min(...border.slice(0,3))<=20,opacity:style.opacity==='1'};
   for(let parent=element.parentElement;parent;parent=parent.parentElement) if(Number(getComputedStyle(parent).opacity)<1) checks.opacity=false;
   const badText=[element,...element.querySelectorAll('*')].filter(node=>isVisible(node)&&[...node.childNodes].some(child=>child.nodeType===Node.TEXT_NODE&&child.textContent.trim())&&getComputedStyle(node).color!=='rgb(0, 0, 0)');
   checks.descendantText=badText.length===0;
   if(Object.values(checks).some(ok=>!ok)) failures.push({class:element.className?.baseVal??element.className,text:element.textContent.trim().slice(0,60),checks,bg:style.backgroundColor,border:style.borderTopColor,color:style.color,opacity:style.opacity,child:badText.slice(0,2).map(node=>({class:node.className,color:getComputedStyle(node).color}))});
  }
  const fontFailures=[...document.querySelectorAll('*')].filter(element=>isVisible(element)&&[...element.childNodes].some(child=>child.nodeType===Node.TEXT_NODE&&child.textContent.trim())&&!getComputedStyle(element).fontFamily.toLowerCase().startsWith('"malgun gothic"')).slice(0,12).map(element=>({class:element.className?.baseVal??element.className,font:getComputedStyle(element).fontFamily}));
  const tabs=[...document.querySelectorAll('[role="tablist"]')].filter(isVisible).map(element=>({label:element.getAttribute('aria-label'),display:getComputedStyle(element).display,wrap:getComputedStyle(element).flexWrap,height:Math.round(element.getBoundingClientRect().height),children:[...element.querySelectorAll('[role="tab"]')].filter(isVisible).map(tab=>({text:tab.textContent.trim(),height:Math.round(tab.getBoundingClientRect().height),whiteSpace:getComputedStyle(tab).whiteSpace}))}));
  return {label,theme:document.documentElement.dataset.uiTheme,width:innerWidth,controls:controls.length,disabled:controls.filter(element=>element.matches(':disabled,[aria-disabled="true"]')).length,failures,fontFailures,tabs};
 },label);
}

export async function inspectVisibleInternalTabs(page, label, results) {
 const groups = await page.getByRole('tablist').evaluateAll(nodes=>nodes.filter(node=>node.getBoundingClientRect().width>0).map(node=>({label:node.getAttribute('aria-label'),tabs:[...node.querySelectorAll('[role="tab"]')].map(tab=>tab.textContent.trim())})));
 for(const group of groups){
  if(!group.label) continue;
  for(const text of group.tabs){
   const tab=page.getByRole('tablist',{name:group.label,exact:true}).getByRole('tab',{name:text,exact:true});
   if(await tab.count()!==1 || !await tab.isVisible() || await tab.isDisabled()) continue;
   await tab.click(); await page.evaluate(()=>new Promise(done=>requestAnimationFrame(()=>requestAnimationFrame(done))));
   results.push(await probeUnifiedControls(page,`${label} / ${group.label} / ${text}`));
  }
 }
}
export async function exerciseAppearance(page, {navLabel, theme, app, screenshotPath}) {
 await page.locator('.nav-list').getByRole('button',{name:navLabel,exact:true}).click();
 const panel=page.getByRole('region',{name:'화면 테마 설정',exact:true});
 await panel.getByRole('button',{name:theme==='dark'?'다크':'화이트',exact:true}).click();
 await panel.getByLabel('사용자 강조 색상',{exact:true}).fill('#000080');
 await page.waitForFunction(expected=>document.documentElement.dataset.uiTheme===expected,theme);
 await panel.getByRole('button',{name:'레이아웃 편집',exact:true}).click();
 await panel.locator('[data-layout-control="editor"][aria-pressed="true"]').waitFor();
 const editorActive=await panel.locator('[data-layout-control="editor"]').getAttribute('aria-pressed');
 const handle=panel.locator('[data-resize-direction="e"],[data-layout-resize-direction="e"]').first();
 await handle.waitFor({state:'visible'});
 const before=await panel.boundingBox(), grip=await handle.boundingBox();
 await page.mouse.move(grip.x+grip.width/2,grip.y+grip.height/2);
 await page.mouse.down(); await page.mouse.move(grip.x+grip.width/2-80,grip.y+grip.height/2,{steps:8}); await page.mouse.up();
 const after=await panel.boundingBox();
 const resized=Boolean(before&&after&&Math.abs(before.width-after.width)>10);
 await panel.getByRole('button',{name:'편집 종료',exact:true}).click();
 await panel.locator('[data-layout-control="editor" ][aria-pressed="false" ]').waitFor();
 await page.evaluate(app=>{
  const key=app==='live'?'live-trader.panelSizes.v1':'paper_trader.layoutSizes.v3';
  localStorage.setItem(key,JSON.stringify({'offline-fixture-panel':{width:333,height:222}}));
 },app);
 page.once('dialog',dialog=>dialog.accept());
 await panel.getByRole('button',{name:'초기화',exact:true}).click();
 const resetRemoved=await page.evaluate(app=>!String(localStorage.getItem(app==='live'?'live-trader.panelSizes.v1':'paper_trader.layoutSizes.v3')).includes('offline-fixture-panel'),app);
 await page.reload();
 await page.locator('.nav-list').getByRole('button',{name:navLabel,exact:true}).click();
 await panel.getByLabel('사용자 강조 색상',{exact:true}).waitFor();
 const saved=await panel.getByLabel('사용자 강조 색상',{exact:true}).inputValue();
 const appliedTheme=await page.evaluate(()=>document.documentElement.dataset.uiTheme);
 const accent=await page.evaluate(()=>getComputedStyle(document.documentElement).getPropertyValue('--ts-control-accent').trim());
 const rgb=accent.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i)?.slice(1).map(hex=>parseInt(hex,16)/255)||[];
 const contrast=rgb.reduce((sum,value,index)=>sum+[.2126,.7152,.0722][index]*(value<=.04045?value/12.92:((value+.055)/1.055)**2.4),0)/.05+1;
 if(screenshotPath) await page.screenshot({path:screenshotPath});
 return {app,theme,editorActive,resized,resizeWidths:[before?.width,after?.width],resetRemoved,persisted:saved==='#000080'&&appliedTheme===theme,displayAccent:accent,contrast};
}