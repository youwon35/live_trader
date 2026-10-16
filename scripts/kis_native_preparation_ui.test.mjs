import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import Module, { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { transformSync } from "esbuild";
import { KIS_PREPARATION_LABELS } from "../src/kisNativePreparation.js";

const sourcePath = fileURLToPath(new URL("../src/KisNativePreparationPanel.jsx", import.meta.url));
const code = transformSync(readFileSync(sourcePath, "utf8"), { loader: "jsx", jsx: "automatic", format: "cjs", target: "es2022" }).code;
const require = createRequire(import.meta.url);
const module = new Module(sourcePath); module.filename = sourcePath;
module.paths = Module._nodeModulePaths(fileURLToPath(new URL("../src", import.meta.url)));
module.require = (specifier) => specifier === "./kisNativePreparation.js" ? { KIS_PREPARATION_LABELS } : require(specifier);
module._compile(code, sourcePath);
const Panel = module.exports.default;
function elements(node, type) {
  if (!node || typeof node !== "object") return [];
  if (Array.isArray(node)) return node.flatMap((child) => elements(child, type));
  return [...(node.type === type ? [node] : []), ...elements(node.props?.children, type)];
}
const row = (status = "PREPARING_PUBLIC", scopeKey = "account-a") => ({ status, scopeKey, flowId: 1, label: "선택한 국내 포트폴리오", canCancel: true });

test("empty mount is passive and contains no execution controls", () => {
  let calls = 0;
  const tree = Panel({ views: {}, onCancel: () => { calls += 1; } });
  assert.equal(renderToStaticMarkup(tree), ""); assert.equal(calls, 0);
});
test("actual preparation and account-check views expose only direct exact-account cancel", () => {
  for (const status of ["PREPARING_PUBLIC", "CHECKING_ACCOUNT", "NATIVE_CONFIRMATION"]) {
    const calls = []; const tree = Panel({ views: { a: row(status) }, onCancel: (key) => calls.push(key) });
    const html = renderToStaticMarkup(tree); assert.match(html, new RegExp(KIS_PREPARATION_LABELS[status]));
    assert.match(html, /aria-live="polite"/); assert.equal(calls.length, 0);
    const buttons = elements(tree, "button"); assert.equal(buttons.length, 1);
    assert.equal(buttons[0].props.children, "준비 취소"); buttons[0].props.onClick(); assert.deepEqual(calls, ["account-a"]);
    assert.doesNotMatch(html, /scopeHash|nativeNonce|preparationId|주문 권한 발급|실행 중/);
  }
});
test("final Start request and terminal preparation statuses never offer a misleading preparation cancel", () => {
  for (const status of ["STARTING", "FAILED", "EXPIRED", "CANCELLED"]) {
    const tree = Panel({ views: { a: { ...row(status), canCancel: false } }, onCancel: () => assert.fail("No cancel control") });
    assert.equal(elements(tree, "button").length, 0);
    assert.match(renderToStaticMarkup(tree), new RegExp(KIS_PREPARATION_LABELS[status]));
  }
});
test("separate account cards preserve their exact cancel target and hide completed preparation", () => {
  const calls = []; const tree = Panel({ views: { a: row("PREPARING_PUBLIC", "a"), b: row("CHECKING_ACCOUNT", "b"), c: row("COMPLETE", "c") }, onCancel: (key) => calls.push(key) });
  assert.equal(elements(tree, "section").length, 2);
  elements(tree, "button")[1].props.onClick(); assert.deepEqual(calls, ["b"]);
});
test("App uses a dedicated preparation state rather than application-wide loading and retains STOP", () => {
  const source = readFileSync(fileURLToPath(new URL("../src/App.jsx", import.meta.url)), "utf8");
  const body = source.slice(source.indexOf("async function runRuntimeStart("), source.indexOf("function updateAppearance("));
  assert.doesNotMatch(body, /setLoading\(/);
  assert.match(body, /profileId !== "stock"/); assert.match(source, /registerKisPreparationPresenter/);
  assert.match(source, /onRuntimeStop=.*runAction.*stopContinuousRuntime/);
});
