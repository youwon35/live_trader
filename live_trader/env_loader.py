from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


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
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        name = key.strip()
        if not name or name.startswith("export "):
            name = name.removeprefix("export ").strip()
        value = _unquote(_strip_inline_comment(raw_value))
        if not name or not value or name in os.environ:
            continue
        os.environ[name] = value
