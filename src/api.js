export async function getSnapshot() {
  return request("/api/snapshot");
}

export async function setMode(mode) {
  return request("/api/mode", { method: "POST", body: { mode } });
}

export async function setFlag(name, value) {
  return request("/api/flag", { method: "POST", body: { name, value } });
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
