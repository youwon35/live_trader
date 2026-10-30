"""Terminal summary of actual observations. No promotion or inferred fills."""
from __future__ import annotations


def terminal_report(session):
    rows = []
    for spec in session['contract']['specs']:
        sleeve = session['sleeves'][spec['strategyInstanceId']]
        evaluations = sleeve.get('decisionCount', 0)
        attempts = sleeve.get('orderAttemptCount', 0)
        fills = sleeve.get('fillCount', 0)
        meaning = ('NOT_EVALUATED' if not evaluations else 'NO_ORDER_ATTEMPT' if not attempts
                   else 'NO_FILL_OBSERVED' if not fills else 'FILLS_OBSERVED')
        rows.append({'instanceId': spec['strategyInstanceId'], 'route': spec['route'],
            'symbol': spec['symbol'], 'timeframe': spec['timeframe'],
            'evaluations': evaluations, 'orderAttempts': attempts, 'fillObservations': fills,
            'lastBarAt': sleeve.get('lastBarAt'), 'outcome': meaning})
    return {'schemaVersion': 'short-live-report-v1', 'sessionId': session['sessionId'],
        'startedAt': session['startedAt'], 'terminatedAt': session.get('terminatedAt'),
        'retirementVerified': session.get('terminalVerified') is True,
        'firstFailure': session.get('firstFailure'), 'firstFailureAt': session.get('firstFailureAt'),
        'outcome': 'ERROR_RECORDED' if session.get('firstFailure') else
                   'NO_EVALUATIONS' if not any(r['evaluations'] for r in rows) else
                   'NO_ORDERS' if not session['orderCount'] else 'ORDERS_OBSERVED',
        'orderAttempts': session['orderCount'], 'fillObservations': session['fillCount'],
        'feesKnown': session.get('feesKnown') is True, 'strategies': rows,
        'promotionEligible': False, 'profitabilityVerified': False,
        'note': 'Counts describe this session only; no-signal and observed fills are not strategy qualification.'}
