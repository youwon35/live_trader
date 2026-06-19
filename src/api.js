export async function getSnapshot() {
  return request("/api/snapshot");
}

export async function setMode(mode) {
  return request("/api/mode", { method: "POST", body: { mode } });
}

export async function setFlag(name, value) {
  return request("/api/flag", { method: "POST", body: { name, value } });
}

export async function setRiskSetting(name, value) {
  return request("/api/risk-setting", { method: "POST", body: { name, value } });
}

export async function setChecklistItem(name, value) {
  return request("/api/checklist", { method: "POST", body: { name, value } });
}

export async function setRetryPolicy(name, value) {
  return request("/api/retry-policy", { method: "POST", body: { name, value } });
}

export async function retryOrder(orderId) {
  return request("/api/order-retry", { method: "POST", body: { order_id: orderId } });
}

export async function cancelOrder(orderId) {
  return request("/api/order-cancel", { method: "POST", body: { order_id: orderId } });
}

export async function runBrokerCheck(brokerId) {
  return request("/api/broker-check", { method: "POST", body: { broker_id: brokerId } });
}

export async function exportAudit(format) {
  return request("/api/audit-export", { method: "POST", body: { format } });
}

export async function submitTestIntent() {
  return request("/api/test-intent", { method: "POST", body: {} });
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    method: options.method ?? "GET",
    headers: { "Content-Type": "application/json" },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}
