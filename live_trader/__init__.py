import sys
from pathlib import Path

for parent in Path(__file__).resolve().parents:
    shared_runtime = parent / "packages" / "trading_runtime"
    if shared_runtime.exists():
        runtime_path = str(shared_runtime)
        if runtime_path not in sys.path:
            sys.path.insert(0, runtime_path)
        break

from .env_loader import load_local_env  # noqa: E402

load_local_env()

__all__ = ["__version__"]

__version__ = "0.1.0"
