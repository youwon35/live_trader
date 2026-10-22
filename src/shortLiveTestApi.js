import { request } from "./api.js";

// The existing native app-session/CSRF request path remains authoritative.
export const getShortLiveTestSnapshot = () => request("/api/short-live-tests/snapshot", { timeoutMs: 10000 });
export const prepareShortLiveTest = (body) => request("/api/short-live-tests/prepare", { method: "POST", body, timeoutMs: 45000 });
export const startShortLiveTest = (body) => request("/api/short-live-tests/start", { method: "POST", body, timeoutMs: 45000 });
export const stopShortLiveTest = (body) => request("/api/short-live-tests/stop", { method: "POST", body, timeoutMs: 45000 });
