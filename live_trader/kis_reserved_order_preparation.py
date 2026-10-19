"""Public-only envelope for explicitly requested bounded reservation reads."""
from datetime import datetime, timedelta, timezone
from .kis_reserved_order_observation import validate_reserved_order_summary

SCHEMA = 'kis-reserved-order-preparation/v1'
FLAGS = {'fullAccountReady': False, 'reservedOrdersReconciled': False,
    'completeConditionalOrdersVerified': False, 'financialReady': False,
    'orderAuthorityGranted': False}


def recent_reserved_window(now=None):
    actual = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(actual, datetime) or actual.tzinfo is None:
        raise ValueError('KIS_RESERVED_CURRENT_KST_DATE_REQUIRED')
    end = actual.astimezone(timezone(timedelta(hours=9))).date()
    return {'start_date': (end - timedelta(days=29)).strftime('%Y%m%d'),
        'end_date': end.strftime('%Y%m%d')}


def reserved_preparation(summary=None):
    checked = validate_reserved_order_summary(summary) if summary is not None else None
    return {'schemaVersion': SCHEMA,
        'status': 'OBSERVED_LIMITED_SCOPES' if checked is not None else 'UNAVAILABLE',
        'reservations': checked, **FLAGS}


def validate_reserved_preparation(value):
    if (type(value) is not dict or set(value) != {'schemaVersion', 'status', 'reservations', *FLAGS}
            or any(value.get(key) is not False for key in FLAGS)):
        raise ValueError('KIS_RESERVED_PUBLIC_PREPARATION_INVALID')
    expected = reserved_preparation(value['reservations'])
    if value != expected:
        raise ValueError('KIS_RESERVED_PUBLIC_PREPARATION_INVALID')
    return expected
