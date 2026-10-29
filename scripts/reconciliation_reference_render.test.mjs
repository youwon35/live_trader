import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {runInNewContext} from 'node:vm';
const require=createRequire(import.meta.url),React=require('react'),{renderToStaticMarkup}=require('react-dom/server'),{transformSync}=require('esbuild');
const source=readFileSync(new URL('../src/ReconciliationChanges.jsx',import.meta.url),'utf8');
const compiled=transformSync(source,{loader:'jsx',format:'cjs',jsx:'automatic'}).code;
const module={exports:{}};runInNewContext(compiled,{module,exports:module.exports,require,console});const Component=module.exports.default;
const issue=changes=>({brokerId:'upbit',currency:'KRW',kind:'cash',reason:'과거 조회와 잔고 변화 · 참고',comparison:'과거 100 → 현재 110',detail:'원인 미분류',...changes});
test('the actual component puts reference-only changes in a closed informational disclosure',()=>{
 const html=renderToStaticMarkup(React.createElement(Component,{issues:[issue({presentationOnlyReference:true})]}));
 assert.match(html,/과거 조회·기존 보유 · 참고 1개/);assert.match(html,/입출금 내역을 확인한 결과는 아니며/);assert.match(html,/var\(--moa-status-info/);
 assert.doesNotMatch(html,/<details[^>]*open/);assert.doesNotMatch(html,/확인할 운용 차이/);assert.match(html,/과거 100/);
});
test('a genuine managed-position failure remains open beside a closed historical reference',()=>{
 const html=renderToStaticMarkup(React.createElement(Component,{issues:[issue({presentationOnlyReference:true}),issue({kind:'position',symbol:'SOLUSDT',presentationOnlyReference:false,reason:'실제 수량 불일치'})]}));
 assert.match(html,/확인할 운용 차이 · 1개/);assert.match(html,/실제 수량 불일치/);assert.equal((html.match(/<details[^>]*open/g)||[]).length,1);assert.equal((html.match(/<details/g)||[]).length,2);
});
test('unknown issue classification cannot hide a difference as a reference',()=>{
 const html=renderToStaticMarkup(React.createElement(Component,{issues:[issue({})]}));assert.match(html,/확인할 운용 차이 · 1개/);assert.match(html,/<details[^>]*open/);assert.doesNotMatch(html,/참고 1개/);
});
test('an empty issue list produces no fabricated status or disclosure',()=>{
 assert.equal(renderToStaticMarkup(React.createElement(Component,{issues:[]})),'');
});
