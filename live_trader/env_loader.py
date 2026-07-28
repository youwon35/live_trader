from __future__ import annotations

import os
import sys
from pathlib import Path

from trading_runtime.secret_store import SecretStore, default_secret_store_path


ROOT = Path(__file__).resolve().parents[1]
LIVE_TRADER_SECRET_KEYS = {
    "KIS_APP_KEY",
    "KIS_APP_SECRET",
    "KIS_ACCOUNT_NO",
    "KIS_HTS_ID",
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "UPBIT_ACCESS_KEY",
    "UPBIT_SECRET_KEY",
}


def live_secret_store() -> SecretStore:
    configured = str(os.getenv("LIVE_TRADER_SECRET_STORE_PATH") or "").strip()
    return SecretStore(Path(configured).expanduser() if configured else default_secret_store_path())


def live_secret_name(key: str) -> str:
    return f"live_trader.{str(key).strip()}"


def default_runtime_data_root() -> Path:
    configured = str(os.getenv("LIVE_TRADER_DATA_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    if getattr(sys, "frozen", False):
        base = Path(os.getenv("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
        return base / "live_trader"
    return ROOT


def default_env_path() -> Path:
    configured = str(os.getenv("LIVE_TRADER_ENV_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return default_runtime_data_root() / ".env"


def _strip_inline_comment(value: str) -> str:
    quote: str | None = None
    escaped = False
    for index, char in enumerate(value):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char in {'"', "'"}:
            quote = None if quote == char else char if quote is None else quote
            continue
        if char == "#" and quote is None:
            return value[:index].rstrip()
    return value.strip()


def _unquote(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def load_local_env(path: Path | None = None) -> None:
    env_path = path or default_env_path()
    raw_lines = env_path.read_text(encoding="utf-8-sig").splitlines() if env_path.exists() else []
    store = live_secret_store()
    migrated: set[str] = set()
    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        name = key.strip()
        if not name or name.startswith("export "):
            name = name.removeprefix("export ").strip()
        value = _unquote(_strip_inline_comment(raw_value))
        if not name or not value:
            continue
        if name in LIVE_TRADER_SECRET_KEYS:
            try:
                store.set(live_secret_name(name), value)
                migrated.add(name)
            except OSError:
                pass
        if not os.environ.get(name):
            os.environ[name] = value

    if migrated:
        sanitized_lines: list[str] = []
        for raw_line in raw_lines:
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                sanitized_lines.append(raw_line)
                continue
            key = stripped.split("=", 1)[0].strip().removeprefix("export ").strip()
            sanitized_lines.append(f"{key}=" if key in migrated else raw_line)
        temporary = env_path.with_suffix(env_path.suffix + f".{os.getpid()}.tmp")
        temporary.write_text("\n".join(sanitized_lines).rstrip() + "\n", encoding="utf-8")
        temporary.replace(env_path)

    for key in LIVE_TRADER_SECRET_KEYS:
        if os.environ.get(key):
            continue
        value = store.get(live_secret_name(key))
        if value:
            os.environ[key] = value
