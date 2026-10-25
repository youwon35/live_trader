import { request } from "./api.js";

const PRE_START_REJECTIONS = new Set([
  "EXPLICIT_MANUAL_UI_CONFIRMATION_REQUIRED", "PREPARATION_REQUIRED", "DUPLICATE_START_FORBIDDEN",
  "PREPARATION_SCOPE_MISMATCH", "PREPARATION_EXPIRED",
]);

export function verifyShortLiveResponse(value, action) {
  if (value?.ok === false) {
    const code = typeof value.reasonCode === "string" ? value.reasonCode : "SHORT_LIVE_SERVER_REJECTED";
    const error = new Error(code);
    // These exact server branches precede session creation / stop mutation.
    // Other exceptions may follow a partial action and remain non-repeatable.
    error.knownPreMutationRejection = (action === "start" && (PRE_START_REJECTIONS.has(code) || code.startsWith("START_BLOCKED:")))
      || (action === "stop" && code === "STOP_SCOPE_MISMATCH");
    throw error;
  }
  const snapshot = value?.snapshot || value;
  if (value?.ok !== true || typeof snapshot?.status !== "string"
      || !(Array.isArray(snapshot.catalog) || Array.isArray(snapshot.catalog?.candidates))
      || !Object.hasOwn(snapshot, "prepared") || !Object.hasOwn(snapshot, "runtime")) {
    throw new Error("SHORT_LIVE_RESPONSE_UNVERIFIED");
  }
  return value;
}

// The existing native app-session/CSRF request path remains authoritative.
export const getShortLiveTestSnapshot = async () => verifyShortLiveResponse(await request("/api/short-live-tests/snapshot", { timeoutMs: 10000 }), "snapshot");
export async function getShortLiveTestCatalog() {
  const value = await request("/api/short-live-tests/catalog", { timeoutMs: 45000 });
  if (value?.ok === false) throw new Error(value.reasonCode || "SHORT_LIVE_CATALOG_UNAVAILABLE");
  if (value?.ok !== true || value?.schemaVersion !== "short-live-test-catalog-v1"
      || value.readOnly !== true || value.authorityGranted !== false || !Array.isArray(value.catalog)) {
    throw new Error("SHORT_LIVE_CATALOG_UNVERIFIED");
  }
  return value;
}
export const prepareShortLiveTest = async (body) => verifyShortLiveResponse(await request("/api/short-live-tests/prepare", { method: "POST", body, timeoutMs: 45000 }), "prepare");
export const startShortLiveTest = async (body) => verifyShortLiveResponse(await request("/api/short-live-tests/start", { method: "POST", body, timeoutMs: 45000 }), "start");
export const stopShortLiveTest = async (body) => verifyShortLiveResponse(await request("/api/short-live-tests/stop", { method: "POST", body, timeoutMs: 45000 }), "stop");
