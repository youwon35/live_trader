from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .live_adapters import BINANCE_BASE_URL, KIS_LIVE_BASE_URL, UPBIT_BASE_URL


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
SettingKind = Literal["text", "secret", "bool"]


@dataclass(frozen=True)
class EnvSettingField:
    key: str
    label: str
    detail: str
    group: str
    kind: SettingKind
    default: str = ""
    required: bool = False


ENV_SETTING_FIELDS = (
    EnvSettingField("KIS_APP_KEY", "KIS app key", "KIS 실전투자 app key입니다.", "kis", "secret", required=True),
    EnvSettingField("KIS_APP_SECRET", "KIS app secret", "KIS 실전투자 app secret입니다.", "kis", "secret", required=True),
    EnvSettingField("KIS_ACCOUNT_NO", "KIS 계좌번호", "계좌번호 앞 8자리입니다.", "kis", "text", required=True),
    EnvSettingField("KIS_ACCOUNT_PRODUCT_CODE", "KIS 상품 코드", "계좌번호 뒤 2자리입니다. 보통 01을 사용합니다.", "kis", "text", "01", True),
    EnvSettingField("KIS_BASE_URL", "KIS 실전 URL", "실전 서버 URL입니다. 모의투자 URL과 섞지 마세요.", "kis", "text", KIS_LIVE_BASE_URL),
    EnvSettingField("KIS_HTS_ID", "KIS HTS ID", "체결/시세 경로에서 필요한 HTS ID입니다.", "kis", "text"),
    EnvSettingField("BINANCE_API_KEY", "Binance API key", "Binance Spot API key입니다.", "binance", "secret", required=True),
    EnvSettingField("BINANCE_API_SECRET", "Binance API secret", "Binance Spot API secret입니다.", "binance", "secret", required=True),
    EnvSettingField("BINANCE_BASE_URL", "Binance URL", "Binance REST base URL입니다.", "binance", "text", BINANCE_BASE_URL),
    EnvSettingField("UPBIT_ACCESS_KEY", "Upbit access key", "Upbit Open API access key입니다.", "upbit", "secret", required=True),
    EnvSettingField("UPBIT_SECRET_KEY", "Upbit secret key", "Upbit Open API secret key입니다.", "upbit", "secret", required=True),
    EnvSettingField("UPBIT_BASE_URL", "Upbit URL", "Upbit REST base URL입니다.", "upbit", "text", UPBIT_BASE_URL),
    EnvSettingField(
        "LIVE_TRADER_ENABLE_REAL_ORDERS",
        "실전 주문 라우트",
        "true일 때만 실주문 어댑터가 활성화됩니다. 최종 점검 후에만 켜세요.",
        "live-lock",
        "bool",
        "false",
        True,
    ),
)


def read_env_file(path: Path = ENV_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = unquote_env_value(value.strip())
    return values


def env_settings_snapshot() -> dict[str, Any]:
    file_values = read_env_file()
    fields: list[dict[str, Any]] = []
    for field in ENV_SETTING_FIELDS:
        value = file_values.get(field.key, os.getenv(field.key, field.default))
        configured = bool(str(value).strip())
        fields.append(
            {
                "key": field.key,
                "label": field.label,
                "detail": field.detail,
                "group": field.group,
                "kind": field.kind,
                "required": field.required,
                "configured": configured,
                "value": "" if field.kind == "secret" else value,
                "masked": mask_value(value) if field.kind == "secret" else "",
                "default": field.default,
            }
        )
    return {
        "envPath": str(ENV_PATH),
        "fields": fields,
        "groups": [
            {"id": "kis", "label": "주식/ETF", "detail": "한국투자증권 Open API"},
            {"id": "binance", "label": "Binance", "detail": "코인 현물"},
            {"id": "upbit", "label": "Upbit", "detail": "KRW 코인"},
            {"id": "live-lock", "label": "실거래 잠금", "detail": "실전 주문 라우트"},
        ],
    }


def save_env_settings(raw_values: dict[str, Any]) -> dict[str, Any]:
    existing = read_env_file()
    field_map = {field.key: field for field in ENV_SETTING_FIELDS}
    next_values = {key: existing.get(key, os.getenv(key, field.default)) for key, field in field_map.items()}

    for key, raw_value in raw_values.items():
        field = field_map.get(str(key))
        if not field:
            continue
        value = normalize_value(raw_value, field)
        if field.kind == "secret" and not value:
            continue
        next_values[field.key] = value

    write_env_file(next_values, field_map)
    for key, value in next_values.items():
        os.environ[key] = value
    return env_settings_snapshot()


def write_env_file(values: dict[str, str], field_map: dict[str, EnvSettingField], path: Path = ENV_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    managed_keys = set(field_map)
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    output: list[str] = []

    for raw_line in existing_lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            output.append(raw_line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in managed_keys:
            output.append(f"{key}={quote_env_value(values.get(key, field_map[key].default))}")
            seen.add(key)
        else:
            output.append(raw_line)

    missing = [field for field in ENV_SETTING_FIELDS if field.key not in seen]
    if missing:
        if output and output[-1].strip():
            output.append("")
        output.append("# Managed by Live Trader settings UI")
        for field in missing:
            output.append(f"{field.key}={quote_env_value(values.get(field.key, field.default))}")

    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")
    tmp_path.replace(path)


def normalize_value(value: Any, field: EnvSettingField) -> str:
    if field.kind == "bool":
        if isinstance(value, bool):
            return "true" if value else "false"
        return "true" if str(value).strip().lower() in {"1", "true", "yes", "on"} else "false"
    text = str(value or "").strip()
    return text or field.default


def quote_env_value(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    if any(char.isspace() for char in text) or any(char in text for char in "#\"'"):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def unquote_env_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if len(text) >= 2 and text[0] == text[-1] == "'":
        return text[1:-1]
    return text


def mask_value(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 6:
        return "*" * len(text)
    return f"{text[:3]}...{text[-3:]}"
