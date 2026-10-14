"""Explicit next-start latency observation, outside runtime/evidence stores."""
from __future__ import annotations
import os
import re
from pathlib import Path

ENVIRONMENT_FLAG = "LIVE_TRADER_LATENCY_DIAGNOSTICS"


def runtime_latency_options(root: Path, profile: str, runtime_hash: str,
                            *, enabled: bool | None = None):
    """Return observer/path without I/O; disabled performs no observer import.

    The setting never enables a runtime, changes its mode or modifies a
    currently running observer. Only an explicit new start consumes it.
    Diagnostic configuration failures cannot change business authorization.
    """
    chosen = os.getenv(ENVIRONMENT_FLAG, "") == "1" if enabled is None else enabled is True
    if not chosen:
        return None, None
    if profile not in {"stock", "crypto"} or not isinstance(runtime_hash, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", runtime_hash):
        return None, None
    try:
        from trading_runtime.runtime_latency_diagnostics import RuntimeLatencyDiagnostics
        observer = RuntimeLatencyDiagnostics()
        suffix = observer.process_key
        if not isinstance(suffix, str) or not re.fullmatch(r"[a-f0-9]{32}", suffix):
            return None, None
        path = Path(root) / "logs" / "diagnostics" / f"continuous_{profile}_{runtime_hash[:16]}_{suffix}_latency.json"
        return observer, path
    except Exception:
        return None, None
