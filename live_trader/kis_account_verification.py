"""Public-only preparation envelopes around bounded private account reads."""
from __future__ import annotations

from .kis_account_inventory_observation import validate_inventory_summary

SCHEMA = 'kis-account-inventory-preparation/v1'
FLAGS = {'fullAccountReady': False, 'financialReady': False, 'orderAuthorityGranted': False}


def inventory_preparation(summary=None):
    checked = validate_inventory_summary(summary) if summary is not None else None
    return {'schemaVersion': SCHEMA,
        'status': 'OBSERVED_LIMITED_SCOPES' if checked is not None else 'UNAVAILABLE',
        'inventory': checked, **FLAGS}


def validate_inventory_preparation(value):
    if (type(value) is not dict or set(value) != {'schemaVersion', 'status', 'inventory', *FLAGS}
            or any(value.get(key) is not False for key in FLAGS)):
        raise ValueError('KIS_ACCOUNT_INVENTORY_PUBLIC_PREPARATION_INVALID')
    expected = inventory_preparation(value['inventory'])
    if value != expected:
        raise ValueError('KIS_ACCOUNT_INVENTORY_PUBLIC_PREPARATION_INVALID')
    return expected
