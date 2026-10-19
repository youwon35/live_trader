"""Local fee alternatives and outstanding preparation; never financial evidence.

Older Source envelopes do not identify stock versus ETF. Show both fee models
instead of guessing an instrument type from a ticker or using an FX rate.
"""
from __future__ import annotations

from .kis_provisional_fees import build_provisional_fee_profile, estimate_fee

SCHEMA = 'kis-provisional-fee-preview-v1'
STATUS = 'PROVISIONAL_CONFIRMATION_REQUIRED'
FOLLOWUP_STEPS = (
    ('PERSONAL_FEE_TERMS', 'CONFIRMATION_REQUIRED', 'USER_OR_BROKER'),
    ('ACCOUNT_SHARED_KRW_RECONCILIATION', 'VERIFICATION_REQUIRED', 'PROGRAM'),
    ('ACTUAL_FX_AND_SETTLEMENT', 'VERIFICATION_REQUIRED', 'PROGRAM_AND_BROKER'),
    ('PAPER_SOURCE_REVALIDATION', 'VERIFICATION_REQUIRED', 'PROGRAM'),
    ('MANUAL_LIVE_START', 'BLOCKED', 'USER'),
)


def build_provisional_fee_preview(lane, notional, settings=None):
    if lane not in {'kis-kr', 'kis-us'}:
        raise ValueError('KIS_PROVISIONAL_FEE_PREVIEW_MARKET_UNSUPPORTED')
    profile = build_provisional_fee_profile(settings)
    market = 'KRX' if lane == 'kis-kr' else 'US'
    return {
        'schemaVersion': SCHEMA,
        'status': STATUS,
        'profile': profile,
        'estimates': [estimate_fee(profile, market, product, notional)
                      for product in ('STOCK', 'ETF')],
        'calculationBasis': 'USER_ENTERED_NOTIONAL_ONLY',
        'productClassification': 'UNKNOWN_SHOW_SEPARATE_STOCK_ETF_ALTERNATIVES',
        'actualFeeObserved': False,
        'orderAuthorityGranted': False,
        'estimatedNotional': notional,
        'accountReadRequired': False,
        'followupSteps': [dict(zip(('code', 'status', 'owner'), step))
                          for step in FOLLOWUP_STEPS],
    }
