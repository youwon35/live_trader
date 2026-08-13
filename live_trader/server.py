from __future__ import annotations

import argparse
import errno
import json
import math
import mimetypes
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from . import env_settings, state
from .functional_test_workspace import FUNCTIONAL_TEST_WORKSPACE
from .functional_http_session import (
    FunctionalHttpSessionAuthority,
    FunctionalHttpSessionError,
    normalize_loopback_bind_host,
)
from trading_runtime.artifact_metadata import ArtifactMetadataStore
from trading_runtime.telegram_notifications import save_shared_telegram_settings, verify_telegram_connection


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 18795
SETTINGS_DIR = Path(os.getenv("APPDATA") or Path.home()) / "LiveTrader"
UI_SETTINGS_FILE = SETTINGS_DIR / "ui-settings.json"
SHARED_SETTINGS_DIR = Path(os.getenv("APPDATA") or Path.home()) / "trading_programs"
STRATEGY_SEARCH_PRESETS_FILE = SHARED_SETTINGS_DIR / "strategy-search-presets.json"
STRATEGY_SEARCH_PRESET_SCHEMA = "strategy-search-presets-v1"
STRATEGY_SEARCH_PRESET_LIMIT = 30
_FUNCTIONAL_STATUS_PATHS = frozenset(
    {
        "/api/upbit-functional/status",
        "/api/binance-spot-functional/status",
    }
)
_FUNCTIONAL_MUTATION_PATHS = frozenset(
    {
        "/api/safety-confirmation/challenge",
        "/api/upbit-functional/start",
        "/api/upbit-functional/stop",
        "/api/upbit-functional/recover",
        "/api/binance-spot-functional/start",
        "/api/binance-spot-functional/stop",
        "/api/binance-spot-functional/recover",
    }
)
_FUNCTIONAL_BOOTSTRAP_PATH = "/__lt_native_bootstrap"


def functional_test_control_scope() -> dict[str, object]:
    """Resolve exact pause/final-close authority after expiry or restart."""

    workspace_scope = FUNCTIONAL_TEST_WORKSPACE.authority_scope()
    runtime_scope = state.functional_test_runtime_authority_scope()
    workspace_resolved = workspace_scope.get("resolved") is True
    runtime_resolved = bool(
        str(runtime_scope.get("permitId") or "").strip()
        and str(runtime_scope.get("accountFingerprint") or "").strip()
    )
    if workspace_resolved:
        workspace_result = {
            "present": True,
            "resolved": True,
            "permitId": str(workspace_scope.get("permitId") or ""),
            "accountFingerprint": state.functional_test_account_fingerprint(
                workspace_scope.get("accountId")
            ),
            "source": str(workspace_scope.get("source") or "workspace"),
            "reason": "",
        }
        if runtime_resolved and (
            str(runtime_scope.get("permitId") or "")
            != workspace_result["permitId"]
            or str(runtime_scope.get("accountFingerprint") or "").lower()
            != str(workspace_result["accountFingerprint"]).lower()
        ):
            return {
                "present": True,
                "resolved": False,
                "permitId": "",
                "accountFingerprint": "",
                "source": "workspace+runtime",
                "reason": "functional-test-authority-scope-conflict",
            }
        return workspace_result
    if runtime_resolved:
        return {
            "present": True,
            "resolved": True,
            "permitId": str(runtime_scope.get("permitId") or ""),
            "accountFingerprint": str(
                runtime_scope.get("accountFingerprint") or ""
            ).lower(),
            "source": str(runtime_scope.get("source") or "runtime"),
            "reason": "",
        }
    return {
        "present": workspace_scope.get("present") is True,
        "resolved": False,
        "permitId": "",
        "accountFingerprint": "",
        "source": "",
        "reason": str(
            workspace_scope.get("reason")
            or "functional-test-authority-scope-unresolved"
        ),
    }


def create_functional_test_permit(payload: dict[str, object]) -> dict[str, object]:
    with state.FUNCTIONAL_TEST_LIFECYCLE_LOCK:
        workspace = FUNCTIONAL_TEST_WORKSPACE.snapshot()
        authority_scope = FUNCTIONAL_TEST_WORKSPACE.authority_scope()
        mutation = state.functional_test_authority_mutation_assessment()
        current = (
            workspace.get("current")
            if isinstance(workspace.get("current"), dict)
            else {}
        )
        blockers = list(mutation.get("blockers") or [])
        authority_reference_present = bool(
            current.get("permit") is not None
            or current.get("authorityReferencePresent") is True
            or authority_scope.get("present") is True
        )
        if authority_reference_present:
            blockers.insert(
                0,
                "functional-test-permit-replacement-requires-stop",
            )
        if (
            authority_scope.get("present") is True
            and authority_scope.get("resolved") is not True
        ):
            blockers.insert(
                1,
                str(
                    authority_scope.get("reason")
                    or "functional-test-authority-scope-unresolved"
                ),
            )
        if blockers:
            workspace["authorityMutation"] = mutation
            workspace["authorityScope"] = authority_scope
            return {
                "ok": False,
                "reason": "기존 기능시험을 안전 중지·대조한 뒤 새 허가서를 준비하세요: "
                + ", ".join(dict.fromkeys(blockers)),
                "brokerSubmissionPerformed": False,
                "workspace": workspace,
            }
        with state.FUNCTIONAL_TEST_AUTHORITY_DISPATCH_LOCK:
            return FUNCTIONAL_TEST_WORKSPACE.create_permit(payload)


def activate_functional_test_today(
    payload: dict[str, object],
) -> dict[str, object]:
    with state.FUNCTIONAL_TEST_LIFECYCLE_LOCK:
        try:
            activation_poll = state.poll_execution_events(
                "kis",
                force_snapshot=True,
                include_snapshot=False,
            )
        except Exception as exc:
            activation_poll = {
                "ok": False,
                "errors": [
                    {
                        "broker_id": "kis",
                        "detail": type(exc).__name__,
                    }
                ],
            }
        mutation = state.functional_test_authority_mutation_assessment(
            require_kis_reconciliation=True,
        )
        if (
            not isinstance(activation_poll, dict)
            or activation_poll.get("ok") is not True
            or activation_poll.get("coalesced") is True
            or activation_poll.get("errors")
            or mutation.get("allowed") is not True
        ):
            workspace = FUNCTIONAL_TEST_WORKSPACE.snapshot()
            workspace["authorityMutation"] = mutation
            return {
                "ok": False,
                "reason": "당일 활성화 전 runtime·주문·KIS 대조를 정리하세요: "
                + ", ".join(
                    str(item) for item in mutation.get("blockers", [])
                ),
                "brokerSubmissionPerformed": False,
                "reconciliationPoll": activation_poll,
                "workspace": workspace,
            }
        # Token replacement is serialized with the final KIS POST edge.
        with state.FUNCTIONAL_TEST_AUTHORITY_DISPATCH_LOCK:
            return FUNCTIONAL_TEST_WORKSPACE.activate_today(payload)


def start_functional_test_execution(
    payload: dict[str, object],
) -> dict[str, object]:
    with state.FUNCTIONAL_TEST_LIFECYCLE_LOCK:
        return state.start_functional_test_runtime(
            FUNCTIONAL_TEST_WORKSPACE.snapshot(),
            confirmed=payload.get("confirmed") is True,
            target_key=str(payload.get("targetKey") or ""),
            safety_confirmation=(
                dict(payload.get("safety_confirmation"))
                if isinstance(payload.get("safety_confirmation"), dict)
                else None
            ),
        )


def pause_functional_test_today(
    payload: dict[str, object],
) -> dict[str, object]:
    with state.FUNCTIONAL_TEST_LIFECYCLE_LOCK:
        if payload.get("confirmed") is not True:
            return FUNCTIONAL_TEST_WORKSPACE.begin_pause_today(payload)
        pause_scope = functional_test_control_scope()
        # Revoke activation at the same serialized edge used by KIS POST.
        # Runtime joins and network polling happen after this inner lock is
        # released, while the lifecycle lock prevents another control action.
        with state.FUNCTIONAL_TEST_AUTHORITY_DISPATCH_LOCK:
            revoke_result = FUNCTIONAL_TEST_WORKSPACE.begin_pause_today(
                payload
            )
        if revoke_result.get("ok") is not True:
            return revoke_result
        if pause_scope.get("resolved") is not True:
            result = FUNCTIONAL_TEST_WORKSPACE.record_pause_failed(
                pause_scope.get("reason")
            )
            result["safePause"] = {
                "ok": False,
                "status": "STOP_FAILED",
                "reason": pause_scope.get("reason"),
            }
            result["runtimeStopped"] = False
            return result
        safe_pause = state.pause_functional_test_runtime_safely(
            permit_id=str(pause_scope.get("permitId") or ""),
            account_fingerprint=str(
                pause_scope.get("accountFingerprint") or ""
            ),
        )
        if safe_pause.get("ok") is not True:
            result = FUNCTIONAL_TEST_WORKSPACE.record_pause_failed(
                safe_pause.get("reason")
            )
            result["safePause"] = safe_pause
            result["runtimeStopped"] = False
            return result
        result = FUNCTIONAL_TEST_WORKSPACE.complete_pause_today()
        result["safePause"] = safe_pause
        result["runtimeStopped"] = bool(
            isinstance(safe_pause.get("runtime"), dict)
            and safe_pause["runtime"].get("ok") is True
        )
        return result


def end_functional_test_plan(
    payload: dict[str, object],
) -> dict[str, object]:
    with state.FUNCTIONAL_TEST_LIFECYCLE_LOCK:
        if payload.get("confirmed") is not True:
            return FUNCTIONAL_TEST_WORKSPACE.stop(payload)
        stop_scope = functional_test_control_scope()
        if (
            stop_scope.get("present") is True
            and stop_scope.get("resolved") is not True
        ):
            result = FUNCTIONAL_TEST_WORKSPACE.record_stop_failed(
                stop_scope.get("reason")
            )
            result["safeStop"] = {
                "ok": False,
                "status": "STOP_FAILED",
                "reason": stop_scope.get("reason"),
            }
            result["runtimeStopped"] = False
            return result
        if stop_scope.get("resolved") is True:
            safe_stop = state.stop_functional_test_runtime_safely(
                permit_id=str(stop_scope.get("permitId") or ""),
                account_fingerprint=str(
                    stop_scope.get("accountFingerprint") or ""
                ),
            )
            if safe_stop.get("ok") is not True:
                result = FUNCTIONAL_TEST_WORKSPACE.record_stop_failed(
                    safe_stop.get("reason")
                )
                result["safeStop"] = safe_stop
                result["runtimeStopped"] = False
                return result
        else:
            safe_stop = {
                "ok": True,
                "reason": "functional-test-authority-not-present",
            }
        result = FUNCTIONAL_TEST_WORKSPACE.stop(payload)
        result["safeStop"] = safe_stop
        result["runtimeStopped"] = bool(
            isinstance(safe_stop.get("runtime"), dict)
            and safe_stop["runtime"].get("ok") is True
        )
        return result


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


def read_strategy_search_presets() -> dict[str, object]:
    try:
        payload = json.loads(STRATEGY_SEARCH_PRESETS_FILE.read_text(encoding="utf-8")) if STRATEGY_SEARCH_PRESETS_FILE.exists() else {}
    except (OSError, json.JSONDecodeError):
        payload = {}
    presets = payload.get("presets") if isinstance(payload, dict) else []
    if not isinstance(presets, list):
        presets = []
    return {
        "schemaVersion": STRATEGY_SEARCH_PRESET_SCHEMA,
        "updatedAt": str(payload.get("updatedAt") or "") if isinstance(payload, dict) else "",
        "presets": [item for item in presets if isinstance(item, dict)][-STRATEGY_SEARCH_PRESET_LIMIT:],
        "path": str(STRATEGY_SEARCH_PRESETS_FILE),
    }


def write_strategy_search_presets(payload: dict[str, object]) -> dict[str, object]:
    presets = payload.get("presets")
    if not isinstance(presets, list):
        raise ValueError("presets 배열이 필요합니다.")
    document = {
        "schemaVersion": STRATEGY_SEARCH_PRESET_SCHEMA,
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "presets": [item for item in presets if isinstance(item, dict)][-STRATEGY_SEARCH_PRESET_LIMIT:],
    }
    STRATEGY_SEARCH_PRESETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_path = STRATEGY_SEARCH_PRESETS_FILE.with_suffix(".tmp")
    temp_path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(STRATEGY_SEARCH_PRESETS_FILE)
    return {**document, "path": str(STRATEGY_SEARCH_PRESETS_FILE)}


class LiveTraderHandler(BaseHTTPRequestHandler):
    server_version = "LiveTraderHTTP/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if self.path.startswith("/") is False or parsed.scheme or parsed.netloc:
            if parsed.path in (
                _FUNCTIONAL_STATUS_PATHS
                | _FUNCTIONAL_MUTATION_PATHS
                | {_FUNCTIONAL_BOOTSTRAP_PATH}
            ):
                self._send_functional_http_denial(
                    "absolute-form functional request targets are forbidden"
                )
                return
        if parsed.path == _FUNCTIONAL_BOOTSTRAP_PATH:
            self._handle_native_bootstrap(parsed)
            return
        if parsed.path in _FUNCTIONAL_STATUS_PATHS and not self._authorize_functional_http(
            require_origin=False
        ):
            return
        if parsed.path == "/api/snapshot":
            from .soak_monitor import latest_live_soak_report

            # PyWebView can engage the durable latch while this API thread is
            # unreachable. The first reconnect/snapshot becomes an idempotent
            # recovery safe point (MONITOR/cancel/reconcile/evidence once).
            state.recover_durable_emergency_stop()
            self.send_json(
                {
                    **state.snapshot(),
                    "soak_report": latest_live_soak_report(),
                }
            )
            return
        if parsed.path == "/api/ui-settings":
            self.send_json({"ok": True, "settings": read_ui_settings()})
            return
        if parsed.path == "/api/search-presets":
            self.send_json(read_strategy_search_presets())
            return
        if parsed.path == "/api/artifact-metadata":
            self.send_json(ArtifactMetadataStore().read())
            return
        if parsed.path == "/api/env-settings":
            self.send_json({"ok": True, "settings": env_settings.env_settings_snapshot()})
            return
        if parsed.path == "/api/telegram":
            self.send_json({"ok": True, "telegram": state.TELEGRAM_DISPATCHER.status()})
            return
        if parsed.path == "/api/telegram/connection":
            self.send_json(
                {
                    "ok": True,
                    "connection": verify_telegram_connection(
                        state.TELEGRAM_DISPATCHER.settings,
                        api_client=state.TELEGRAM_DISPATCHER.api_client,
                    ),
                }
            )
            return
        if parsed.path == "/api/binance-futures-canary/status":
            self.send_json(
                {
                    "ok": True,
                    "canary": state.binance_futures_canary_status(),
                }
            )
            return
        if parsed.path == "/api/binance-futures-settings/status":
            self.send_json(
                {
                    "ok": True,
                    "settings": state.binance_futures_settings_status(),
                }
            )
            return
        if parsed.path == "/api/binance-futures-fill-soak/status":
            self.send_json(
                {
                    "ok": True,
                    "fill_soak": state.binance_futures_fill_soak_status(),
                }
            )
            return
        if parsed.path == "/api/doctor-diagnostics":
            self.send_json(
                {
                    "ok": True,
                    "doctor_diagnostics": state.doctor_diagnostics_document(),
                }
            )
            return
        if parsed.path == "/api/validation-small-live":
            self.send_json(
                {
                    "ok": True,
                    "validation": state.validation_small_live_snapshot(),
                }
            )
            return
        if parsed.path == "/api/functional-test":
            workspace = FUNCTIONAL_TEST_WORKSPACE.snapshot()
            workspace["effectiveCaps"] = (
                state.functional_test_effective_caps_snapshot()
            )
            workspace["authorityMutation"] = (
                state.functional_test_authority_mutation_assessment()
            )
            runtime_snapshot = state.LIVE_CONTINUOUS_CONTROLLER.snapshot()
            stock_runtime = (
                (runtime_snapshot.get("profiles") or {}).get("stock")
                if isinstance(runtime_snapshot.get("profiles"), dict)
                else {}
            )
            if not isinstance(stock_runtime, dict):
                stock_runtime = {}
            workspace["runtime"] = {
                "running": bool(stock_runtime.get("running")),
                "mode": str(stock_runtime.get("mode") or "MONITOR"),
                "executionPurpose": str(
                    stock_runtime.get("executionPurpose") or ""
                ),
                "functionalTestRunning": bool(stock_runtime.get("running"))
                and str(stock_runtime.get("executionPurpose") or "").upper()
                == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE,
                "strategyIds": list(stock_runtime.get("strategyIds") or []),
                "allowedSymbols": list(stock_runtime.get("allowedSymbols") or []),
            }
            self.send_json(workspace)
            return
        if parsed.path == "/api/upbit-functional/status":
            self.send_json(state.upbit_functional_backend_state_status())
            return
        if parsed.path == "/api/binance-spot-functional/status":
            self.send_json(
                state.binance_spot_functional_backend_state_status()
            )
            return
        if parsed.path == "/api/soak-report/latest":
            from .soak_monitor import latest_live_soak_report

            self.send_json(
                {
                    "ok": True,
                    "soak_report": latest_live_soak_report(),
                }
            )
            return
        self.serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if self.path.startswith("/") is False or parsed.scheme or parsed.netloc:
            if parsed.path in _FUNCTIONAL_STATUS_PATHS | _FUNCTIONAL_MUTATION_PATHS:
                self._send_functional_http_denial(
                    "absolute-form functional request targets are forbidden"
                )
                return
        # Authenticate exact high-risk paths before reading any caller body.
        # There is no CORS/preflight/bootstrap endpoint for these secrets.
        if parsed.path in _FUNCTIONAL_MUTATION_PATHS and not self._authorize_functional_http(
            require_origin=True
        ):
            return
        payload = self.read_json()
        if parsed.path == "/api/safety-confirmation/challenge":
            self.send_json(
                state.issue_safety_confirmation(
                    payload.get("action"),
                    dict(payload.get("context"))
                    if isinstance(payload.get("context"), dict)
                    else {},
                )
            )
            return
        if parsed.path == "/api/telegram":
            values = payload.get("telegram") if isinstance(payload.get("telegram"), dict) else payload
            save_shared_telegram_settings(values)
            self.send_json({"ok": True, "telegram": state.TELEGRAM_DISPATCHER.status()})
            return
        if parsed.path == "/api/telegram/test":
            state.TELEGRAM_DISPATCHER.send_test()
            self.send_json({"ok": True, "telegram": state.TELEGRAM_DISPATCHER.status()})
            return
        if parsed.path == "/api/mode":
            self.send_json(state.set_mode(str(payload.get("mode", "MONITOR"))))
            return
        if parsed.path == "/api/flag":
            self.send_json(
                state.set_flag(
                    str(payload.get("name", "")),
                    bool(payload.get("value")),
                    confirmed=payload.get("confirmed") is True,
                    safety_confirmation=(
                        dict(payload.get("safety_confirmation"))
                        if isinstance(payload.get("safety_confirmation"), dict)
                        else None
                    ),
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
            self.send_json(
                state.run_reconciliation(
                    refresh_brokers=payload.get("refresh_brokers") is not False,
                    include_snapshot=payload.get("include_snapshot") is not False,
                )
            )
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
            force_snapshot = payload.get("force_snapshot")
            self.send_json(
                state.poll_execution_events(
                    str(payload.get("broker_id", "all")),
                    force_snapshot=force_snapshot if isinstance(force_snapshot, bool) else None,
                    include_snapshot=payload.get("include_snapshot") is not False,
                )
            )
            return
        if parsed.path == "/api/execution-streams/start":
            self.send_json(state.start_execution_streams(str(payload.get("broker_id", "all"))))
            return
        if parsed.path == "/api/execution-streams/stop":
            self.send_json(state.stop_execution_streams())
            return
        if parsed.path == "/api/preflight":
            self.send_json(state.run_final_preflight(
                str(payload.get("deployment_id", "")),
                str(payload.get("strategy_id", "")),
            ))
            return
        if parsed.path == "/api/upbit-functional/start":
            self.send_json(
                state.start_upbit_functional_backend_state(dict(payload))
            )
            return
        if parsed.path == "/api/upbit-functional/stop":
            self.send_json(
                state.stop_upbit_functional_backend_state(dict(payload))
            )
            return
        if parsed.path == "/api/upbit-functional/recover":
            self.send_json(
                state.recover_upbit_functional_backend_state(dict(payload))
            )
            return
        if parsed.path == "/api/binance-spot-functional/start":
            self.send_json(
                state.start_binance_spot_functional_backend_state(
                    dict(payload)
                )
            )
            return
        if parsed.path == "/api/binance-spot-functional/stop":
            self.send_json(
                state.stop_binance_spot_functional_backend_state(
                    dict(payload)
                )
            )
            return
        if parsed.path == "/api/binance-spot-functional/recover":
            self.send_json(
                state.recover_binance_spot_functional_backend_state(
                    dict(payload)
                )
            )
            return
        if parsed.path == "/api/upbit-smoke-preview":
            self.send_json(
                state.preview_upbit_smoke_order(
                    str(payload.get("strategy_id", "")),
                    payload.get("notional_krw", 5000),
                )
            )
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
        if parsed.path == "/api/binance-smoke-preview":
            self.send_json(state.preview_binance_smoke_order(str(payload.get("strategy_id", ""))))
            return
        if parsed.path == "/api/binance-smoke-submit":
            self.send_json(
                state.submit_binance_smoke_order(
                    payload.get("confirmation_token", ""),
                    confirmed=payload.get("confirmed") is True,
                )
            )
            return
        if parsed.path == "/api/binance-futures-canary/preview":
            self.send_json(
                state.preview_binance_futures_canary(
                    payload.get("strategy_id", ""),
                    payload.get("notional_usdt", 6),
                )
            )
            return
        if parsed.path == "/api/binance-futures-canary/test":
            self.send_json(
                state.test_binance_futures_canary_order(
                    payload.get("confirmation_token", ""),
                    confirmed=payload.get("confirmed") is True,
                )
            )
            return
        if parsed.path == "/api/binance-futures-settings/preview":
            self.send_json(
                state.preview_binance_futures_settings(
                    payload.get("symbol", "ETHUSDT"),
                    payload.get("margin_type", "ISOLATED"),
                    payload.get("leverage", 1),
                )
            )
            return
        if parsed.path == "/api/binance-futures-risk/preview":
            self.send_json(
                state.preview_binance_futures_order_risk(payload)
            )
            return
        if parsed.path == "/api/binance-futures-settings/apply":
            self.send_json(
                state.apply_binance_futures_settings(
                    payload.get("confirmation_token", ""),
                    confirmed=payload.get("confirmed") is True,
                )
            )
            return
        if parsed.path == "/api/binance-futures-fill-soak/preview":
            self.send_json(
                state.preview_binance_futures_fill_soak(
                    payload.get("symbol", "ETHUSDT"),
                )
            )
            return
        if parsed.path == "/api/binance-futures-fill-soak/start":
            self.send_json(
                state.start_binance_futures_fill_soak(
                    payload.get("confirmation_token", ""),
                    confirmed=payload.get("confirmed") is True,
                    safety_confirmation=(
                        dict(payload.get("safety_confirmation"))
                        if isinstance(payload.get("safety_confirmation"), dict)
                        else None
                    ),
                )
            )
            return
        if parsed.path == "/api/binance-futures-fill-soak/stop":
            self.send_json(state.stop_binance_futures_fill_soak())
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
        if parsed.path == "/api/validation-small-live/evaluate":
            self.send_json(
                state.run_validation_small_live_once(
                    str(
                        payload.get(
                            "validation_strategy_instance_id",
                            "",
                        )
                    )
                )
            )
            return
        if parsed.path == "/api/functional-test/permit":
            self.send_json(create_functional_test_permit(payload))
            return
        if parsed.path == "/api/functional-test/activate":
            self.send_json(activate_functional_test_today(payload))
            return
        if parsed.path == "/api/functional-test/start":
            self.send_json(start_functional_test_execution(payload))
            return
        if parsed.path == "/api/functional-test/pause":
            self.send_json(pause_functional_test_today(payload))
            return
        if parsed.path == "/api/functional-test/stop":
            self.send_json(end_functional_test_plan(payload))
            return
        if parsed.path == "/api/strategy-cycle":
            self.send_json(state.run_strategy_cycle(str(payload.get("profile_id", "stock"))))
            return
        if parsed.path == "/api/runtime/start":
            requested_purpose = str(
                payload.get("execution_purpose")
                or payload.get("executionPurpose")
                or ""
            ).strip().upper()
            if requested_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE:
                self.send_json(
                    {
                        "ok": False,
                        "reason": (
                            "FUNCTIONAL_TEST runtime은 /api/functional-test/start "
                            "전용 2단계 확인 경로만 사용할 수 있습니다."
                        ),
                        "runtimeStarted": False,
                        "brokerSubmissionPerformed": False,
                    }
                )
                return
            self.send_json(state.start_continuous_runtime(
                str(payload.get("profile_id", "stock")),
                str(payload.get("mode", "MONITOR")),
                str(payload.get("portfolio_id", "")),
                str(payload.get("deployment_id", "")),
                str(payload.get("strategy_id", "")),
                requested_purpose,
                (
                    dict(payload.get("functional_test_context"))
                    if isinstance(payload.get("functional_test_context"), dict)
                    else dict(payload.get("functionalTestContext"))
                    if isinstance(payload.get("functionalTestContext"), dict)
                    else None
                ),
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
        if parsed.path == "/api/incidents/transition":
            self.send_json(
                state.transition_operational_incident(
                    payload.get("incident_id", ""),
                    payload.get("action", ""),
                    payload.get("note", ""),
                )
            )
            return
        if parsed.path == "/api/ui-settings":
            self.send_json({"ok": True, "settings": write_ui_settings(payload)})
            return
        if parsed.path == "/api/search-presets":
            try:
                self.send_json(write_strategy_search_presets(payload))
            except ValueError as exc:
                self.send_json({"ok": False, "error": str(exc)})
            return
        if parsed.path == "/api/artifact-metadata":
            artifact_id = str(payload.get("artifactId") or "").strip()
            changes = payload.get("changes") if isinstance(payload.get("changes"), dict) else {}
            if not artifact_id:
                self.send_json({"ok": False, "error": "artifactId가 필요합니다."})
                return
            entry = ArtifactMetadataStore().update(
                artifact_id,
                payload.get("artifactType") or "strategy",
                favorite=changes.get("favorite") if "favorite" in changes else None,
                tags=changes.get("tags") if "tags" in changes else None,
                note=changes.get("note") if "note" in changes else None,
                mark_used=changes.get("markUsed") is True,
                mark_promoted=changes.get("markPromoted") is True,
            )
            self.send_json({"ok": True, "entry": entry, "document": ArtifactMetadataStore().read()})
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
            self.send_json(
                state.save_environment_settings(
                    (
                        payload.get("values", {})
                        if isinstance(payload.get("values"), dict)
                        else payload
                    ),
                    confirmed=payload.get("confirmed") is True,
                    safety_confirmation=(
                        dict(payload.get("safety_confirmation"))
                        if isinstance(payload.get("safety_confirmation"), dict)
                        else None
                    ),
                )
            )
            return
        self.send_error(404, "Unknown API endpoint")

    def do_OPTIONS(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in _FUNCTIONAL_STATUS_PATHS | _FUNCTIONAL_MUTATION_PATHS:
            self._send_functional_http_denial(
                "functional HTTP CORS/preflight is forbidden"
            )
            return
        self.send_error(404, "Unknown API endpoint")

    def _functional_http_authority(self) -> FunctionalHttpSessionAuthority:
        authority = getattr(
            getattr(self, "server", None),
            "functional_http_session_authority",
            None,
        )
        if not isinstance(authority, FunctionalHttpSessionAuthority):
            raise FunctionalHttpSessionError(
                "trusted functional HTTP session is unavailable"
            )
        return authority

    def _handle_native_bootstrap(self, parsed: object) -> None:
        authority = self._functional_http_authority()
        query = str(getattr(parsed, "query", "") or "")
        expected_query = f"nonce={authority.bootstrap_nonce}"
        host_headers = list(self.headers.get_all("Host") or [])
        accepted = (
            query == expected_query
            and len(host_headers) == 1
            and authority.consume_native_bootstrap(
                nonce=query[len("nonce="):],
                host_header=host_headers[0],
                peer_host=(self.client_address or ("", 0))[0],
            )
        )
        if not accepted:
            self._send_functional_http_denial(
                "native functional HTTP bootstrap is invalid or consumed"
            )
            return
        self.send_response(303)
        self.send_header("Location", "/")
        self.send_header("Set-Cookie", authority.set_cookie_header)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Pragma", "no-cache")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _authorize_functional_http(self, *, require_origin: bool) -> bool:
        try:
            authority = self._functional_http_authority()
            authority.assert_request(
                headers=self.headers,
                peer_host=(self.client_address or ("", 0))[0],
                require_origin=require_origin,
            )
        except (FunctionalHttpSessionError, AttributeError, TypeError) as exc:
            self._send_functional_http_denial(str(exc))
            return False
        return True

    def _send_functional_http_denial(self, reason: str) -> None:
        body = json.dumps(
            {
                "ok": False,
                "reason": "trusted-app-session-required",
                "detail": str(reason or "functional HTTP request denied")[:240],
                "brokerSubmissionPerformed": False,
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(403)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

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
        self.send_header("Cache-Control", "no-store")
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
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def _assert_application_instance_lease_held() -> None:
    """Reject embedded/standalone production servers without the app lease."""

    from .process_safety import (  # pylint: disable=import-outside-toplevel
        live_trader_instance_lease_status,
    )

    status = live_trader_instance_lease_status()
    if status.get("acquired") is not True:
        raise RuntimeError(
            "Live Trader application-instance lease is not held by this process"
        )


def prepare_server_state() -> None:
    # A hard-killed or crashed background monitor cannot execute its finally
    # block. Reconcile the persisted lease before the desktop/API reports any
    # previous RUNNING state.
    from .daemon import read_daemon_status  # pylint: disable=import-outside-toplevel

    _assert_application_instance_lease_held()

    state.disarm_real_orders_for_process_start(persist=True)
    read_daemon_status(persist=True)
    state.restore_runtime_from_checkpoint()
    state.recover_durable_emergency_stop()
    state.prepare_upbit_functional_backend_state()
    state.prepare_binance_spot_functional_backend_state()


def bind_server(host: str, port: int) -> ThreadingHTTPServer:
    normalized_host = normalize_loopback_bind_host(host)
    _assert_application_instance_lease_held()
    server_type: type[ThreadingHTTPServer] = ThreadingHTTPServer
    if normalized_host == "::1":
        class _IPv6LoopbackThreadingHTTPServer(ThreadingHTTPServer):
            address_family = socket.AF_INET6

        server_type = _IPv6LoopbackThreadingHTTPServer
    server = server_type((normalized_host, port), LiveTraderHandler)
    bound_host, bound_port = server.server_address[:2]
    server.functional_http_session_authority = (  # type: ignore[attr-defined]
        FunctionalHttpSessionAuthority.mint(
            host=bound_host,
            port=bound_port,
        )
    )
    return server


def create_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    prepare_server_state()
    return bind_server(host, port)


def is_recoverable_bind_error(exc: OSError) -> bool:
    """Return whether the desktop can safely retry on an OS-assigned port."""
    return getattr(exc, "winerror", None) in {10013, 10048} or exc.errno in {
        errno.EACCES,
        errno.EADDRINUSE,
    }


def create_desktop_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    """Bind the preferred desktop port, falling back when Windows blocks it."""
    prepare_server_state()
    try:
        return bind_server(host, port)
    except OSError as exc:
        if port == 0 or not is_recoverable_bind_error(exc):
            raise
        return bind_server(host, 0)


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
    server = create_desktop_server(host, port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    start_watchdog_thread()
    authority = getattr(server, "functional_http_session_authority", None)
    if not isinstance(authority, FunctionalHttpSessionAuthority):
        raise RuntimeError("trusted functional HTTP session is unavailable")
    return server, authority.expected_origin


def main() -> None:
    from .process_safety import hold_live_trader_instance_lease

    instance = hold_live_trader_instance_lease()
    if instance.get("acquired") is not True:
        raise SystemExit(
            "Live Trader server 단일 인스턴스 잠금 실패: "
            + str(instance.get("reason") or "unknown")
        )
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", default=DEFAULT_PORT, type=int)
    args = parser.parse_args()
    server = create_server(args.host, args.port)
    start_watchdog_thread()
    authority = getattr(server, "functional_http_session_authority", None)
    origin = (
        authority.expected_origin
        if isinstance(authority, FunctionalHttpSessionAuthority)
        else "unavailable"
    )
    print(f"Live Trader server listening on {origin}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
