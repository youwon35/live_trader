"""One durable integrated budget database across source and packaged clients."""
from __future__ import annotations

import os
from pathlib import Path
from .kis_overseas_funding import resolve_funding_mode


def shared_capital_path():
    # Unlike the ordinary source data root, this is independent of checkout,
    # deployment and quote currency. No directory/database is created here.
    base = Path(os.environ.get('LOCALAPPDATA') or Path.home()/'AppData'/'Local')
    if not base.is_absolute():raise ValueError('KIS_SHARED_KRW_ABSOLUTE_COMMON_DATA_ROOT_REQUIRED')
    return base/'live_trader'/'logs'/'kis_owned_capital_v2.sqlite3'


def selected_capital_path(root, settings):
    legacy = Path(root)/'logs'/'kis_owned_capital_v2.sqlite3'
    if resolve_funding_mode(settings) != 'INTEGRATED_MARGIN':
        return legacy
    target = shared_capital_path()
    if legacy.resolve() != target.resolve() and legacy.exists():
        raise ValueError('KIS_SHARED_KRW_SEPARATE_LEGACY_DATABASE_REVIEWED_MIGRATION_REQUIRED')
    return target
