import assert from "node:assert/strict";

import {
  API_DEFAULT_TIMEOUT_MS,
  BROKER_REFRESH_TIMEOUT_MS,
  ApiRequestError,
  isApiConnectionFailure,
  isApiRequestTimeout,
  request,
} from "../src/api.js";

assert.ok(API_DEFAULT_TIMEOUT_MS > 10_000);
assert.ok(BROKER_REFRESH_TIMEOUT_MS > API_DEFAULT_TIMEOUT_MS);

const timeoutError = new ApiRequestError("slow read", "TIMEOUT");
assert.equal(isApiRequestTimeout(timeoutError), true);
assert.equal(isApiConnectionFailure(timeoutError), false);

const networkError = new ApiRequestError("offline", "NETWORK");
assert.equal(isApiConnectionFailure(networkError), true);
assert.equal(isApiRequestTimeout(networkError), false);

const previousWindow = globalThis.window;
const previousFetch = globalThis.fetch;
globalThis.window = globalThis;

try {
  globalThis.fetch = async (_path, { signal }) =>
    new Promise((_resolve, reject) => {
      signal.addEventListener("abort", () => {
        const error = new Error("aborted");
        error.name = "AbortError";
        reject(error);
      }, { once: true });
    });

  await assert.rejects(
    request("/api/slow", { timeoutMs: 5 }),
    (error) => isApiRequestTimeout(error) && /1초/.test(error.message),
  );

  globalThis.fetch = async () => {
    throw new TypeError("fetch failed");
  };
  await assert.rejects(
    request("/api/offline"),
    (error) => isApiConnectionFailure(error) && /연결할 수 없습니다/.test(error.message),
  );
} finally {
  globalThis.fetch = previousFetch;
  if (previousWindow === undefined) {
    delete globalThis.window;
  } else {
    globalThis.window = previousWindow;
  }
}

console.log("live API timeout/connection classification regression checks passed");
