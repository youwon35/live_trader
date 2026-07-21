from __future__ import annotations

import argparse
import json
import math
import mimetypes
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from . import env_settings, state


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8795
SETTINGS_DIR = Path(os.getenv("APPDATA") or Path.home()) / "LiveTrader"
UI_SETTINGS_FILE = SETTINGS_DIR / "ui-settings.json"


def json_safe_value(value: object) -> object:
    """Return strict-JSON data so one non-finite metric cannot break the UI."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe_value(item) for item in value]
    return value


def requests_real_order_enable(payload: dict[str, object]) -> bool:
    values = payload.get("values")
    if not isinstance(values, dict):
        return False
    requested = str(values.get("LIVE_TRADER_ENABLE_REAL_ORDERS", "")).strip().lower()
    return requested in {"true", "1", "yes", "on"}


def read_ui_settings() -> dict[str, object]:
    try:
        if UI_SETTINGS_FILE.exists():
            return json.loads(UI_SETTINGS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {}


def write_ui_settings(payload: dict[str, object]) -> dict[str, object]:
    current = read_ui_settings()
    current.update(payload)
    SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    UI_SETTINGS_FILE.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    return current


class LiveTraderHandler(BaseHTTPRequestHandler):
    server_version = "LiveTraderHTTP/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/snapshot":
            self.send_json(state.snapshot())
            return
        if parsed.path == "/api/ui-settings":
            self.send_json({"ok": True, "settings": read_ui_settings()})
            return
        if parsed.path == "/api/env-settings":
            self.send_json({"ok": True, "settings": env_settings.env_settings_snapshot()})
            return
        self.serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        payload = self.read_json()
        if parsed.path == "/api/mode":
            self.send_json(state.set_mode(str(payload.get("mode", "MONITOR"))))
            return
        if parsed.path == "/api/flag":
            self.send_json(
                state.set_flag(
                    str(payload.get("name", "")),
                    bool(payload.get("value")),
                    confirmed=payload.get("confirmed") is True,
                )
            )
            return
        if parsed.path == "/api/automation":
            self.send_json(
                state.set_automation_profile(
                    str(payload.get("profile_id", "")),
                    bool(payload.get("enabled")),
                    str(payload.get("provider", "")) if payload.get("provider") is not None else None,
                    str(payload.get("mode", "")) if payload.get("mode") is not None else None,
                )
            )
            return
        if parsed.path == "/api/risk-setting":
            self.send_json(state.set_risk_setting(str(payload.get("name", "")), payload.get("value")))
            return
        if parsed.path == "/api/checklist":
            self.send_json(state.set_checklist_item(str(payload.get("name", "")), bool(payload.get("value"))))
            return
        if parsed.path == "/api/retry-policy":
            self.send_json(state.set_retry_policy(str(payload.get("name", "")), payload.get("value")))
            return
        if parsed.path == "/api/order-retry":
            self.send_json(state.retry_order(str(payload.get("order_id", ""))))
            return
        if parsed.path == "/api/order-cancel":
            self.send_json(state.cancel_order(str(payload.get("order_id", ""))))
            return
        if parsed.path == "/api/broker-check":
            self.send_json(state.run_broker_check(str(payload.get("broker_id", ""))))
            return
        if parsed.path == "/api/reconcile":
            self.send_json(state.run_reconciliation())
            return
        if parsed.path == "/api/program-ledger-baseline":
            if payload.get("confirmed") is not True:
                self.send_json(
                    {
                        "ok": False,
                        "reason": "프로그램 원장 기준 저장은 명시 확인이 필요합니다.",
                        "snapshot": state.snapshot(),
                    }
                )
                return
            self.send_json(state.seed_program_ledger_from_broker_snapshot())
            return
        if parsed.path == "/api/execution-events":
            self.send_json(state.poll_execution_events(str(payload.get("broker_id", "all"))))
            return
        if parsed.path == "/api/preflight":
            self.send_json(state.run_final_preflight())
            return
        if parsed.path == "/api/upbit-smoke-preview":
            self.send_json(state.preview_upbit_smoke_order(payload.get("notional_krw", 5000)))
            return
        if parsed.path == "/api/upbit-smoke-submit":
            self.send_json(
                state.submit_upbit_smoke_order(
                    payload.get("confirmation_token", ""),
                    confirmed=payload.get("confirmed") is True,
                )
            )
            return
        if parsed.path == "/api/upbit-smoke-refresh":
            self.send_json(state.refresh_upbit_smoke_order())
            return
        if parsed.path == "/api/audit-export":
            self.send_json(state.export_audit(str(payload.get("format", "csv"))))
            return
        if parsed.path == "/api/test-intent":
            self.send_json(state.submit_test_intent())
            return
        if parsed.path == "/api/policy-replay":
            self.send_json(state.run_policy_replay(payload))
            return
        if parsed.path == "/api/shadow-live":
            self.send_json(state.run_shadow_live(payload))
            return
        if parsed.path == "/api/recovery-drill":
            self.send_json(state.run_recovery_drill())
            return
        if parsed.path == "/api/strategy-cycle":
            self.send_json(state.run_strategy_cycle(str(payload.get("profile_id", "stock"))))
            return
        if parsed.path == "/api/runtime/start":
            self.send_json(state.start_continuous_runtime(
                str(payload.get("profile_id", "stock")),
                str(payload.get("mode", "MONITOR")),
                str(payload.get("portfolio_id", "")),
            ))
            return
        if parsed.path == "/api/runtime/stop":
            self.send_json(state.stop_continuous_runtime(str(payload.get("profile_id", ""))))
            return
        if parsed.path == "/api/strategy-live-promotion":
            self.send_json(state.promote_strategy_to_live(str(payload.get("strategy_id", ""))))
            return
        if parsed.path == "/api/strategy-lifecycle":
            self.send_json(state.set_strategy_lifecycle_status(str(payload.get("strategy_id", "")), str(payload.get("action", ""))))
            return
        if parsed.path == "/api/watchdog":
            self.send_json(state.run_watchdog())
            return
        if parsed.path == "/api/ui-settings":
            self.send_json({"ok": True, "settings": write_ui_settings(payload)})
            return
        if parsed.path == "/api/env-settings":
            if requests_real_order_enable(payload) and payload.get("confirmed") is not True:
                self.send_json(
                    {
                        "ok": False,
                        "reason": "실전 주문 라우트 활성화는 명시 확인이 필요합니다.",
                        "settings": env_settings.env_settings_snapshot(),
                        "snapshot": state.snapshot(),
                    }
                )
                return
            settings = env_settings.save_env_settings(payload.get("values", {}) if isinstance(payload.get("values"), dict) else payload)
            self.send_json({"ok": True, "settings": settings, "snapshot": state.snapshot()})
            return
        self.send_error(404, "Unknown API endpoint")

    def log_message(self, format: str, *args: object) -> None:
        return

    def read_json(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def send_json(self, payload: dict[str, object]) -> None:
        body = json.dumps(json_safe_value(payload), ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def serve_static(self, path: str) -> None:
        if path in {"", "/"}:
            path = "/index.html"
        target = (DIST / path.lstrip("/")).resolve()
        if not str(target).startswith(str(DIST.resolve())):
            self.send_error(403)
            return
        if not target.exists() or target.is_dir():
            target = DIST / "index.html"
        if not target.exists():
            self.send_setup_page()
            return
        content = target.read_bytes()
        content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def send_setup_page(self) -> None:
        body = (
            "<!doctype html><meta charset='utf-8'><title>Live Trader</title>"
            "<body style='font-family:Segoe UI,sans-serif;background:#0b0d10;color:#f3f4f6;padding:32px'>"
            "<h1>Live Trader build is missing</h1>"
            "<p>Run <code>npm install</code> and <code>npm run build</code>, then start the desktop app again.</p>"
            "</body>"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def create_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    state.restore_runtime_from_checkpoint()
    return ThreadingHTTPServer((host, port), LiveTraderHandler)


def watchdog_worker(interval_seconds: float = 15.0) -> None:
    while True:
        try:
            state.run_watchdog(include_snapshot=False)
        except Exception as exc:  # pragma: no cover - defensive desktop loop
            state.append_audit("danger", "Watchdog 오류", f"백그라운드 Watchdog 실행 실패: {exc}")
        time.sleep(max(5.0, interval_seconds))


def start_watchdog_thread() -> threading.Thread:
    thread = threading.Thread(target=watchdog_worker, daemon=True)
    thread.start()
    return thread


def start_in_thread(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> tuple[ThreadingHTTPServer, str]:
    server = create_server(host, port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    start_watchdog_thread()
    return server, f"http://{host}:{server.server_port}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", default=DEFAULT_PORT, type=int)
    args = parser.parse_args()
    server = create_server(args.host, args.port)
    start_watchdog_thread()
    print(f"Live Trader server listening on http://{args.host}:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
