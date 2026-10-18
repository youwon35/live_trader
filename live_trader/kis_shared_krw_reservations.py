"""Transactional local KRW budget reservations, never broker cash or authority.

The caller owns the existing owned-capital SQLite connection and transaction.
Mutations require its BEGIN IMMEDIATE transaction; this module never opens a
database, commits, rolls back, exchanges currency, initializes capital, or sends
orders. An opaque basis hash binds a local reviewed budget, not financial proof.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3

POLICY_SCHEMA = 'kis-shared-krw-cap-policy-v1'
REQUEST_SCHEMA = 'kis-shared-krw-reservation-request-v1'
RECEIPT_SCHEMA = 'kis-shared-krw-reservation-receipt-v1'
STATE_SCHEMA = 'kis-shared-krw-budget-state-v1'
RESULT_SCHEMA = 'kis-shared-krw-reservation-result-v1'
POOL_TABLE = 'kis_shared_krw_cap_pools_v1'
RESERVATION_TABLE = 'kis_shared_krw_reservations_v1'
MAX_RESERVATIONS = 10000
MAX_RECORD_CHARS = 16384
MAX_RECORD_BYTES = 8 * 1024 * 1024
_SHA = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)
_KRW = re.compile(r'(?:0|[1-9][0-9]{0,23})\Z', re.ASCII)
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:\-]{0,127}\Z', re.ASCII)
_POLICY_KEYS = {'schemaVersion', 'accountFingerprint', 'accountProfileId',
    'credentialConfigurationHash', 'capKrw', 'reviewBindingHash', 'policyHash'}
_REQUEST_KEYS = {'schemaVersion', 'reservationId', 'deploymentId', 'scopeHash',
    'accountFingerprint', 'accountProfileId', 'credentialConfigurationHash',
    'requestedKrw', 'basisHash', 'nativeCurrency', 'policyHash', 'requestHash'}
_FALSE = {'financialReady': False, 'productionReady': False,
    'orderAuthorityGranted': False, 'financialQualificationGranted': False,
    'accountCashReconciled': False, 'actualFeesReconciled': False,
    'fxSettlementVerified': False, 'sharedCashReservationVerified': False}
_RECEIPT_KEYS = {'schemaVersion', 'request', 'policyHash', 'accountFingerprint',
    'sequence', 'previousReceiptHash', 'reservedKrwBefore', 'reservedKrwAfter',
    'capKrw', 'localBudgetOnly', 'localReservationRecorded', 'contentHash'} | set(_FALSE)
_DDL = {
    POOL_TABLE: f'CREATE TABLE IF NOT EXISTS {POOL_TABLE} ('
        'account_fingerprint TEXT PRIMARY KEY NOT NULL, payload TEXT NOT NULL, '
        'content_hash TEXT NOT NULL)',
    RESERVATION_TABLE: f'CREATE TABLE IF NOT EXISTS {RESERVATION_TABLE} ('
        'reservation_id TEXT PRIMARY KEY NOT NULL, account_fingerprint TEXT NOT NULL, '
        'deployment_id TEXT NOT NULL, sequence INTEGER NOT NULL, '
        'payload TEXT NOT NULL, content_hash TEXT NOT NULL, '
        'UNIQUE(account_fingerprint, sequence), UNIQUE(account_fingerprint, deployment_id))',
}
_TRIGGERS = {}
for _table in _DDL:
    for _operation in ('UPDATE', 'DELETE'):
        _name = _table + '_no_' + _operation.lower()
        _TRIGGERS[_name] = (f'CREATE TRIGGER IF NOT EXISTS {_name} '
            f'BEFORE {_operation} ON {_table} BEGIN '
            "SELECT RAISE(ABORT,'KIS_SHARED_KRW_APPEND_ONLY'); END")
del _table, _operation, _name
_TRIGGERS[POOL_TABLE + '_no_replacement'] = (
    f'CREATE TRIGGER IF NOT EXISTS {POOL_TABLE}_no_replacement '
    f'BEFORE INSERT ON {POOL_TABLE} WHEN EXISTS '
    f'(SELECT 1 FROM {POOL_TABLE} WHERE account_fingerprint=NEW.account_fingerprint) '
    "BEGIN SELECT RAISE(ABORT,'KIS_SHARED_KRW_APPEND_ONLY'); END")
_TRIGGERS[RESERVATION_TABLE + '_no_replacement'] = (
    f'CREATE TRIGGER IF NOT EXISTS {RESERVATION_TABLE}_no_replacement '
    f'BEFORE INSERT ON {RESERVATION_TABLE} WHEN EXISTS '
    f'(SELECT 1 FROM {RESERVATION_TABLE} WHERE reservation_id=NEW.reservation_id '
    'OR (account_fingerprint=NEW.account_fingerprint AND '
    '(sequence=NEW.sequence OR deployment_id=NEW.deployment_id))) '
    "BEGIN SELECT RAISE(ABORT,'KIS_SHARED_KRW_APPEND_ONLY'); END")


class KisSharedKrwReservationError(ValueError):
    """Fixed status codes only; no database path, credential or private value."""


def _require(condition, code):
    if not condition:
        raise KisSharedKrwReservationError(code)


def canonical_hash(value):
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, OverflowError):
        raise KisSharedKrwReservationError('KIS_SHARED_KRW_CANONICAL_INPUT_INVALID') from None
    return hashlib.sha256(raw).hexdigest()


def _hash(value):
    return type(value) is str and _SHA.fullmatch(value) is not None


def _money(value):
    _require(type(value) is str and _KRW.fullmatch(value) is not None,
        'KIS_SHARED_KRW_CANONICAL_WHOLE_WON_REQUIRED')
    return int(value)


def _tx(db):
    _require(isinstance(db, sqlite3.Connection) and db.in_transaction,
        'KIS_SHARED_KRW_CALLER_TRANSACTION_REQUIRED')


def validate_policy(policy):
    _require(type(policy) is dict and set(policy) == _POLICY_KEYS,
        'KIS_SHARED_KRW_POLICY_SHAPE_INVALID')
    result = dict(policy)
    _require(result['schemaVersion'] == POLICY_SCHEMA and all(_hash(result[key])
        for key in ('accountFingerprint', 'accountProfileId', 'credentialConfigurationHash',
                    'reviewBindingHash', 'policyHash')),
        'KIS_SHARED_KRW_POLICY_BINDING_INVALID')
    _require(_money(result['capKrw']) > 0, 'KIS_SHARED_KRW_POSITIVE_REVIEWED_CAP_REQUIRED')
    _require(canonical_hash({k:v for k,v in result.items() if k != 'policyHash'}) == result['policyHash'],
        'KIS_SHARED_KRW_POLICY_HASH_INVALID')
    return result


def validate_request(request):
    _require(type(request) is dict and set(request) == _REQUEST_KEYS,
        'KIS_SHARED_KRW_REQUEST_SHAPE_INVALID')
    result = dict(request)
    _require(result['schemaVersion'] == REQUEST_SCHEMA and all(_hash(result[key])
        for key in ('scopeHash', 'accountFingerprint', 'accountProfileId',
                    'credentialConfigurationHash', 'basisHash', 'policyHash', 'requestHash')),
        'KIS_SHARED_KRW_REQUEST_BINDING_INVALID')
    _require(all(type(result[key]) is str and _ID.fullmatch(result[key]) is not None
        for key in ('reservationId', 'deploymentId')), 'KIS_SHARED_KRW_BOUNDED_ID_REQUIRED')
    _require(type(result['nativeCurrency']) is str and result['nativeCurrency'] in
        {'KRW', 'USD', 'JPY', 'CNY', 'HKD', 'GBP'}, 'KIS_SHARED_KRW_NATIVE_CURRENCY_UNSUPPORTED')
    _require(_money(result['requestedKrw']) > 0, 'KIS_SHARED_KRW_POSITIVE_RESERVATION_REQUIRED')
    _require(canonical_hash({k:v for k,v in result.items() if k != 'requestHash'}) == result['requestHash'],
        'KIS_SHARED_KRW_REQUEST_HASH_INVALID')
    return result


def _sql_normal(value):
    return re.sub(r'\s+', ' ', value.replace('IF NOT EXISTS ', '').strip().rstrip(';')).upper()


def _schema_present(db):
    rows = db.execute('SELECT name,type,sql FROM sqlite_master WHERE name IN (?,?)',
        (POOL_TABLE, RESERVATION_TABLE)).fetchall()
    if not rows:
        return False
    _require(len(rows) == 2, 'KIS_SHARED_KRW_SCHEMA_PARTIAL_OR_CHANGED')
    for name, kind, sql in rows:
        _require(kind == 'table' and type(sql) is str and _sql_normal(sql) == _sql_normal(_DDL[name]),
            'KIS_SHARED_KRW_SCHEMA_PARTIAL_OR_CHANGED')
    triggers = db.execute('SELECT name,sql FROM sqlite_master WHERE type=? AND tbl_name IN (?,?)',
        ('trigger', POOL_TABLE, RESERVATION_TABLE)).fetchall()
    _require({row[0] for row in triggers} == set(_TRIGGERS), 'KIS_SHARED_KRW_APPEND_ONLY_GUARD_CHANGED')
    for name, sql in triggers:
        _require(type(sql) is str and _sql_normal(sql) == _sql_normal(_TRIGGERS[name]),
            'KIS_SHARED_KRW_APPEND_ONLY_GUARD_CHANGED')
    return True


def ensure_schema(db):
    """DDL only inside the caller's transaction; no executescript/auto-commit."""
    _tx(db)
    try:
        existing = db.execute('SELECT name FROM sqlite_master WHERE name IN (?,?)',
            (POOL_TABLE, RESERVATION_TABLE)).fetchall()
        if existing:
            _require(_schema_present(db), 'KIS_SHARED_KRW_SCHEMA_PARTIAL_OR_CHANGED')
            return
        for statement in _DDL.values():
            db.execute(statement)
        for statement in _TRIGGERS.values():
            db.execute(statement)
        _require(_schema_present(db), 'KIS_SHARED_KRW_SCHEMA_PARTIAL_OR_CHANGED')
    except sqlite3.Error:
        raise KisSharedKrwReservationError('KIS_SHARED_KRW_DATABASE_BUSY_OR_UNAVAILABLE') from None


def _decode(raw):
    _require(type(raw) is str and 0 < len(raw) <= MAX_RECORD_CHARS,
        'KIS_SHARED_KRW_STORED_RECORD_BOUND_INVALID')
    def pairs(items):
        value = {}
        for key, item in items:
            _require(key not in value, 'KIS_SHARED_KRW_STORED_DUPLICATE_KEY')
            value[key] = item
        return value
    def constant(_value):
        raise KisSharedKrwReservationError('KIS_SHARED_KRW_STORED_NONFINITE')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, TypeError, OverflowError) as error:
        if isinstance(error, KisSharedKrwReservationError):
            raise
        raise KisSharedKrwReservationError('KIS_SHARED_KRW_STORED_RECORD_INVALID') from None


def _unknown(account):
    return {'schemaVersion': STATE_SCHEMA, 'status': 'UNKNOWN_NO_POOL',
        'accountFingerprint': account, 'policy': None, 'capKrw': None,
        'reservedKrw': None, 'remainingKrw': None, 'reservationCount': None,
        'headReceiptHash': None, 'receipts': None, 'localBudgetOnly': True, **_FALSE}


def _inspect(db, account):
    if not _schema_present(db):
        return _unknown(account)
    row = db.execute(f'SELECT CASE WHEN length(payload)<=? THEN payload ELSE NULL END, '
        f'content_hash FROM {POOL_TABLE} WHERE account_fingerprint=?',
        (MAX_RECORD_CHARS, account)).fetchone()
    if row is None:
        _require(db.execute(f'SELECT 1 FROM {RESERVATION_TABLE} WHERE account_fingerprint=? LIMIT 1',
            (account,)).fetchone() is None, 'KIS_SHARED_KRW_ORPHAN_RESERVATION')
        return _unknown(account)
    policy = validate_policy(_decode(row[0]))
    _require(policy['accountFingerprint'] == account and row[1] == policy['policyHash'],
        'KIS_SHARED_KRW_STORED_POLICY_CHANGED')
    cap = _money(policy['capKrw'])
    reserved, previous, records, byte_count = 0, None, [], 0
    ids, deployments = set(), set()
    rows = db.execute(f'SELECT reservation_id,account_fingerprint,deployment_id,sequence, '
        f'CASE WHEN length(payload)<=? THEN payload ELSE NULL END,content_hash '
        f'FROM {RESERVATION_TABLE} WHERE account_fingerprint=? ORDER BY sequence LIMIT ?',
        (MAX_RECORD_CHARS, account, MAX_RESERVATIONS + 1))
    for reservation_id, stored_account, deployment_id, sequence, raw, digest in rows:
        _require(len(records) < MAX_RESERVATIONS, 'KIS_SHARED_KRW_RESERVATION_LIMIT_EXCEEDED')
        receipt = _decode(raw)
        byte_count += len(raw.encode('utf-8'))
        _require(byte_count <= MAX_RECORD_BYTES, 'KIS_SHARED_KRW_RESERVATION_LIMIT_EXCEEDED')
        _require(type(receipt) is dict and set(receipt) == _RECEIPT_KEYS
            and receipt['schemaVersion'] == RECEIPT_SCHEMA
            and receipt['localBudgetOnly'] is True and receipt['localReservationRecorded'] is True
            and all(receipt[key] is False for key in _FALSE),
            'KIS_SHARED_KRW_STORED_RECEIPT_INVALID')
        request = validate_request(receipt['request'])
        _require(all(request[key] == policy[key] for key in
            ('accountFingerprint', 'accountProfileId', 'credentialConfigurationHash', 'policyHash')),
            'KIS_SHARED_KRW_STORED_REQUEST_BINDING_CHANGED')
        _require(reservation_id == request['reservationId'] and deployment_id == request['deploymentId']
            and stored_account == account and type(sequence) is int and sequence == len(records) + 1
            and type(receipt['sequence']) is int and receipt['sequence'] == sequence
            and receipt['accountFingerprint'] == account and receipt['policyHash'] == policy['policyHash']
            and receipt['capKrw'] == policy['capKrw'] and receipt['previousReceiptHash'] == previous
            and receipt['reservedKrwBefore'] == str(reserved)
            and _hash(receipt['contentHash']) and digest == receipt['contentHash']
            and canonical_hash({k:v for k,v in receipt.items() if k != 'contentHash'}) == digest,
            'KIS_SHARED_KRW_RECEIPT_CHAIN_OR_SEAL_INVALID')
        _require(reservation_id not in ids and deployment_id not in deployments,
            'KIS_SHARED_KRW_DUPLICATE_RESERVATION_OR_DEPLOYMENT')
        reserved += _money(request['requestedKrw'])
        _require(reserved <= cap and receipt['reservedKrwAfter'] == str(reserved),
            'KIS_SHARED_KRW_CAP_OR_RUNNING_TOTAL_INVALID')
        ids.add(reservation_id)
        deployments.add(deployment_id)
        previous = digest
        records.append(receipt)
    return {'schemaVersion': STATE_SCHEMA, 'status': 'LOCAL_BUDGET_STATE_ONLY',
        'accountFingerprint': account, 'policy': policy, 'capKrw': str(cap),
        'reservedKrw': str(reserved), 'remainingKrw': str(cap - reserved),
        'reservationCount': len(records), 'headReceiptHash': previous,
        'receipts': records, 'localBudgetOnly': True, **_FALSE}


def inspect_in_transaction(db, account_fingerprint):
    """No CREATE on reads. Missing pool/schema is UNKNOWN, never a zero cap."""
    _tx(db)
    _require(_hash(account_fingerprint), 'KIS_SHARED_KRW_ACCOUNT_FINGERPRINT_INVALID')
    try:
        return _inspect(db, account_fingerprint)
    except sqlite3.Error:
        raise KisSharedKrwReservationError('KIS_SHARED_KRW_DATABASE_BUSY_OR_UNAVAILABLE') from None


def reserve_in_transaction(db, *, policy, request):
    """Append one idempotent allocation under the same transaction as principal.

    Caller must roll back its whole transaction on any exception. Status means
    local budget reserved only: no fee, FX, account cash or order proof exists.
    """
    _tx(db)
    policy, request = validate_policy(policy), validate_request(request)
    _require(all(request[key] == policy[key] for key in
        ('accountFingerprint', 'accountProfileId', 'credentialConfigurationHash', 'policyHash')),
        'KIS_SHARED_KRW_CURRENT_POLICY_REQUEST_BINDING_MISMATCH')
    _require(_money(request['requestedKrw']) <= _money(policy['capKrw']),
        'KIS_SHARED_KRW_CAP_EXCEEDED')
    try:
        _require(_schema_present(db), 'KIS_SHARED_KRW_SCHEMA_REQUIRED')
        account = policy['accountFingerprint']
        pool = _inspect(db, account)
        if pool['policy'] is not None:
            _require(pool['policy'] == policy, 'KIS_SHARED_KRW_EXISTING_POOL_POLICY_OR_BINDING_CHANGED')
        old = db.execute(f'SELECT account_fingerprint FROM {RESERVATION_TABLE} WHERE reservation_id=?',
            (request['reservationId'],)).fetchone()
        if old is not None:
            matches = [item for item in (pool['receipts'] or [])
                if item['request']['reservationId'] == request['reservationId']]
            _require(tuple(old) == (account,) and len(matches) == 1
                and matches[0]['request'] == request, 'KIS_SHARED_KRW_IDEMPOTENCY_CONFLICT')
            return {'schemaVersion': RESULT_SCHEMA, 'status': 'LOCAL_BUDGET_RESERVED',
                'idempotent': True, 'receipt': matches[0], 'pool': pool,
                'localBudgetOnly': True, **_FALSE}
        _require(not any(item['request']['deploymentId'] == request['deploymentId']
            for item in (pool['receipts'] or [])), 'KIS_SHARED_KRW_DEPLOYMENT_ALREADY_RESERVED')
        before = int(pool['reservedKrw']) if pool['reservedKrw'] is not None else 0
        after = before + _money(request['requestedKrw'])
        _require(after <= _money(policy['capKrw']), 'KIS_SHARED_KRW_CAP_EXCEEDED')
        if pool['policy'] is None:
            changed = db.execute(f'INSERT INTO {POOL_TABLE} (account_fingerprint,payload,content_hash) '
                'SELECT ?,?,? WHERE NOT EXISTS '
                f'(SELECT 1 FROM {POOL_TABLE} WHERE account_fingerprint=?)',
                (account, json.dumps(policy, ensure_ascii=False, sort_keys=True), policy['policyHash'], account))
            _require(changed.rowcount == 1, 'KIS_SHARED_KRW_POOL_CAS_CHANGED')
        sequence = (pool['reservationCount'] or 0) + 1
        receipt = {'schemaVersion': RECEIPT_SCHEMA, 'request': request,
            'policyHash': policy['policyHash'], 'accountFingerprint': account,
            'sequence': sequence, 'previousReceiptHash': pool['headReceiptHash'],
            'reservedKrwBefore': str(before), 'reservedKrwAfter': str(after),
            'capKrw': policy['capKrw'], 'localBudgetOnly': True,
            'localReservationRecorded': True, **_FALSE}
        receipt['contentHash'] = canonical_hash(receipt)
        changed = db.execute(f'INSERT INTO {RESERVATION_TABLE} '
            '(reservation_id,account_fingerprint,deployment_id,sequence,payload,content_hash) '
            'SELECT ?,?,?,?,?,? WHERE EXISTS '
            f'(SELECT 1 FROM {POOL_TABLE} WHERE account_fingerprint=? AND content_hash=?) '
            f'AND COALESCE((SELECT MAX(sequence) FROM {RESERVATION_TABLE} WHERE account_fingerprint=?),0)=? '
            f'AND COALESCE((SELECT content_hash FROM {RESERVATION_TABLE} WHERE account_fingerprint=? '
            'ORDER BY sequence DESC LIMIT 1),\'\')=? '
            f'AND NOT EXISTS (SELECT 1 FROM {RESERVATION_TABLE} WHERE reservation_id=? '
            'OR (account_fingerprint=? AND deployment_id=?))',
            (request['reservationId'], account, request['deploymentId'], sequence,
             json.dumps(receipt, ensure_ascii=False, sort_keys=True), receipt['contentHash'],
             account, policy['policyHash'], account, sequence - 1,
             account, pool['headReceiptHash'] or '', request['reservationId'], account, request['deploymentId']))
        _require(changed.rowcount == 1, 'KIS_SHARED_KRW_RESERVATION_CAS_CHANGED')
        current = _inspect(db, account)
        _require(current['headReceiptHash'] == receipt['contentHash']
            and current['reservedKrw'] == str(after), 'KIS_SHARED_KRW_RESERVATION_CAS_CHANGED')
        return {'schemaVersion': RESULT_SCHEMA, 'status': 'LOCAL_BUDGET_RESERVED',
            'idempotent': False, 'receipt': receipt, 'pool': current,
            'localBudgetOnly': True, **_FALSE}
    except sqlite3.Error:
        raise KisSharedKrwReservationError('KIS_SHARED_KRW_DATABASE_BUSY_OR_UNAVAILABLE') from None


def release_in_transaction(db, *, request):
    """No positive release producer exists. Labels, timeouts and booleans cannot release."""
    _tx(db)
    raise KisSharedKrwReservationError('KIS_SHARED_KRW_RELEASE_PRODUCER_UNAVAILABLE')
