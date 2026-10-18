"""Read an existing KIS owned book without initialization or financial authority.

Only a normal application's fixed runtime path may be injected by its caller.
No request payload path, schema creation, write transaction or capital setup is
provided. Missing/corrupt/changing data is unknown, never zero account funds.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime
from pathlib import Path
import json
import re
import sqlite3

from .kis_owned_capital import KisOwnedCapitalStore
from .kis_shared_krw_readiness import _amount, _book, _hash

SCHEMA = 'kis-existing-owned-inventory-read-v1'
MAX_BOOKS = 1000
MAX_RECEIPTS = 10000
MAX_PAYLOAD_CHARS = 65536
MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
_SHA = re.compile(r'[a-f0-9]{64}\Z', re.ASCII)
_FLAGS = {'productionReady': False, 'orderAuthorityGranted': False,
    'accountCashReconciled': False, 'sharedCashReservationVerified': False,
    'completeAccountWorkingOrdersVerified': False, 'ledgerInitialized': False,
    'reservationWritten': False, 'actualCapitalInitialized': False}


class _Unknown(ValueError):
    pass


def _require(condition, code):
    if not condition:
        raise _Unknown(code)


def _result(reason, *, books=None, generation=None):
    return {'schemaVersion': SCHEMA,
        'status': 'OBSERVED_LOCAL_INVENTORY_ONLY' if books is not None else 'UNKNOWN',
        'ownedBooks': books, 'reasons': [reason], 'localGeneration': generation,
        'localGenerationMeaning': 'CONNECTION_LOCAL_DATA_VERSION_NOT_DURABLE_REVISION',
        'dependencyHash': _hash(books) if books is not None else None,
        'sourceMeaning': 'EXISTING_LOCAL_OWNED_BOOKS_NOT_ACCOUNT_WIDE_CASH',
        'privateValuesLocalOnly': True, **_FLAGS}


def _identity(path):
    item = path.stat()
    return (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)


def _ordinary_rollback_file(path):
    _require(path.is_file() and not path.is_symlink(), 'KIS_LOCAL_OWNED_PATH_NOT_REGULAR')
    # mode=ro can create a WAL index. Do not open WAL/hot-journal data in this
    # no-create helper, and never use immutable=1 to ignore possible live WAL.
    _require(not any(Path(str(path) + suffix).exists() for suffix in ('-wal', '-shm', '-journal')),
        'KIS_LOCAL_OWNED_SIDECAR_OR_ACTIVE_JOURNAL_UNSUPPORTED')
    with path.open('rb') as stream:
        header = stream.read(100)
    _require(len(header) == 100 and header[:16] == b'SQLite format 3\0',
        'KIS_LOCAL_OWNED_DATABASE_HEADER_INVALID')
    _require(header[18:20] == b'\x01\x01', 'KIS_LOCAL_OWNED_WAL_MODE_UNSUPPORTED')


def _schema(db, table, expected):
    row = db.execute('SELECT type, sql FROM sqlite_master WHERE name=?', (table,)).fetchone()
    _require(row is not None and row[0] == 'table' and type(row[1]) is str
        and 'VIRTUAL' not in row[1].upper(), 'KIS_LOCAL_OWNED_SCHEMA_INVALID')
    # table_xinfo also detects hidden/generated columns; no extra SQL feature
    # or view may masquerade as the exact existing append-only store table.
    columns = db.execute('PRAGMA table_xinfo(' + table + ')').fetchall()
    _require(columns == expected, 'KIS_LOCAL_OWNED_SCHEMA_INVALID')


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'KIS_LOCAL_OWNED_DUPLICATE_JSON_KEY')
            result[key] = value
        return result
    def constant(_value):
        raise _Unknown('KIS_LOCAL_OWNED_NONFINITE_JSON')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def read_existing_owned_books(path, accountBinding):
    """Return private existing scopes/projections or UNKNOWN, with no DB writes.

    Old configuration scopes for the same account remain visible so the shared
    diagnostic can reject their current profile/configuration binding. The
    read-only observer connection's data_version surrounds separate read-only
    KisOwnedCapitalStore.project calls; any intervening commit discards all.
    Local inventory completeness is not external account/order completeness.
    """
    try:
        _require(type(accountBinding) is dict and set(accountBinding) ==
            {'profileId', 'accountFingerprint', 'credentialConfigurationHash'}
            and all(type(value) is str and _SHA.fullmatch(value) for value in accountBinding.values()),
            'KIS_LOCAL_OWNED_ACCOUNT_BINDING_INVALID')
        _require(isinstance(path, (str, Path)), 'KIS_LOCAL_OWNED_FIXED_PATH_REQUIRED')
        path = Path(path)
        _require(path.is_absolute(), 'KIS_LOCAL_OWNED_FIXED_PATH_REQUIRED')
        if not path.exists():
            return _result('KIS_LOCAL_OWNED_DATABASE_MISSING_NOT_ZERO_FUNDS')
        _ordinary_rollback_file(path)
        path = path.resolve(strict=True)
        original = _identity(path)
        account = accountBinding['accountFingerprint']
        keys = (account + ':KRW', account + ':USD')
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1.0)) as db:
            db.execute('PRAGMA query_only=ON')
            _require(db.execute('PRAGMA query_only').fetchone() == (1,),
                'KIS_LOCAL_OWNED_QUERY_ONLY_REQUIRED')
            first_version = db.execute('PRAGMA data_version').fetchone()[0]
            _schema(db, 'principals', [
                (0, 'deployment_id', 'TEXT', 0, None, 1, 0),
                (1, 'account_currency', 'TEXT', 1, None, 0, 0),
                (2, 'payload', 'TEXT', 1, None, 0, 0),
                (3, 'content_hash', 'TEXT', 1, None, 0, 0)])
            _schema(db, 'receipts', [
                (0, 'event_key', 'TEXT', 0, None, 1, 0),
                (1, 'deployment_id', 'TEXT', 1, None, 0, 0),
                (2, 'payload', 'TEXT', 1, None, 0, 0),
                (3, 'content_hash', 'TEXT', 1, None, 0, 0)])
            rows = db.execute('SELECT deployment_id, account_currency, '
                'CASE WHEN length(payload)<=? THEN payload ELSE NULL END, content_hash '
                'FROM principals WHERE account_currency IN (?,?) '
                'ORDER BY deployment_id LIMIT ?', (MAX_PAYLOAD_CHARS, *keys, MAX_BOOKS + 1))
            scopes = []
            payload_bytes = 0
            for deployment, account_currency, raw, digest in rows:
                _require(len(scopes) < MAX_BOOKS, 'KIS_LOCAL_OWNED_BOOK_LIMIT_EXCEEDED')
                _require(type(raw) is str and type(digest) is str and _SHA.fullmatch(digest),
                    'KIS_LOCAL_OWNED_PRINCIPAL_RECORD_INVALID')
                payload_bytes += len(raw.encode('utf-8'))
                _require(payload_bytes <= MAX_PAYLOAD_BYTES, 'KIS_LOCAL_OWNED_PAYLOAD_LIMIT_EXCEEDED')
                body = _decode(raw)
                _require(type(body) is dict and set(body) == {'scope', 'principal', 'occurredAt'}
                    and _hash(body) == digest, 'KIS_LOCAL_OWNED_PRINCIPAL_SEAL_INVALID')
                scope = body['scope']
                _require(type(scope) is dict and scope.get('deploymentId') == deployment
                    and scope.get('accountFingerprint') == account
                    and scope.get('quoteCurrency') in {'KRW', 'USD'}
                    and account_currency == account + ':' + scope['quoteCurrency']
                    and scope.get('scopeHash') == _hash({k:v for k,v in scope.items() if k != 'scopeHash'}),
                    'KIS_LOCAL_OWNED_PRINCIPAL_SCOPE_INVALID')
                _require(_amount(body['principal']) > 0, 'KIS_LOCAL_OWNED_PRINCIPAL_AMOUNT_INVALID')
                when = datetime.fromisoformat(body['occurredAt'].replace('Z', '+00:00'))
                _require(when.tzinfo is not None, 'KIS_LOCAL_OWNED_PRINCIPAL_TIME_INVALID')
                scopes.append(scope)
            receipt_count = 0
            receipt_bytes_upper_bound = 0
            for scope in scopes:
                lengths = db.execute('SELECT length(payload) FROM receipts WHERE deployment_id=? LIMIT ?',
                    (scope['deploymentId'], MAX_RECEIPTS + 1))
                for (length,) in lengths:
                    receipt_count += 1
                    _require(receipt_count <= MAX_RECEIPTS and type(length) is int
                        and 0 <= length <= MAX_PAYLOAD_CHARS,
                        'KIS_LOCAL_OWNED_RECEIPT_LIMIT_EXCEEDED')
                    # Four UTF-8 bytes per SQLite text character is a bounded
                    # upper estimate; no unbounded raw receipt fetch happens
                    # through the existing project() until this budget passes.
                    receipt_bytes_upper_bound += length * 4
                    _require(receipt_bytes_upper_bound <= MAX_PAYLOAD_BYTES,
                        'KIS_LOCAL_OWNED_RECEIPT_LIMIT_EXCEEDED')
            _require(db.execute('PRAGMA data_version').fetchone()[0] == first_version,
                'KIS_LOCAL_OWNED_CHANGED_DURING_READ')
            store = KisOwnedCapitalStore(path)
            books = []
            for scope in scopes:
                projection = store.project(scope, {})  # Existing method is URI mode=ro only.
                book = {'scope': scope, 'projection': projection}
                _book(book)  # Existing diagnostic validates structural roles, not cash authority.
                books.append(book)
            _require(db.execute('PRAGMA data_version').fetchone()[0] == first_version
                and _identity(path) == original, 'KIS_LOCAL_OWNED_CHANGED_DURING_READ')
            _ordinary_rollback_file(path)
        return _result('KIS_LOCAL_OWNED_INVENTORY_NOT_EXTERNAL_ACCOUNT_PROOF',
            books=books, generation=first_version)
    except _Unknown as error:
        return _result(str(error))
    except (ValueError, TypeError, KeyError, AttributeError, OSError, ArithmeticError, sqlite3.Error):
        return _result('KIS_LOCAL_OWNED_DATABASE_UNVERIFIED')
