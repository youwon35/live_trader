"""Bounded asynchronous PUBLIC initialization jobs, never order authority."""
from copy import deepcopy
import re
import secrets
import threading
import time

from .kis_native_start_registry import KIS_NATIVE_START_REGISTRY


class KisNativePreparationJobs:
    def __init__(self, *, registry=KIS_NATIVE_START_REGISTRY, clock=time.monotonic,
                 thread_factory=threading.Thread, timer_factory=threading.Timer):
        self._registry, self._clock = registry, clock
        self._threads, self._timers = thread_factory, timer_factory
        self._lock = threading.RLock()
        self._jobs = {}

    def begin(self, *, entry, run):
        with self._lock:
            for previous in self._jobs.values():
                if previous['status'] in {'PREPARING_PUBLIC','CHECKING_ACCOUNT','READY'}:
                    try:
                        self._registry.binding(previous['entry'])
                    except RuntimeError:
                        previous['status'] = 'EXPIRED' if previous['entry'].deadline and self._clock() >= previous['entry'].deadline else 'CANCELLED'
            # Keep only a bounded recent set. Eviction never retains live data.
            if len(self._jobs) >= 16:
                terminal = [key for key, job in self._jobs.items() if job['status'] in {'FAILED','EXPIRED','CANCELLED','CONSUMED'}]
                if not terminal:
                    raise ValueError('KIS_NATIVE_PREPARATION_JOB_CAPACITY')
                del self._jobs[terminal[0]]
            key = secrets.token_urlsafe(32)
            job = {'entry': entry, 'status': 'PREPARING_PUBLIC', 'started': self._clock(), 'result': {}, 'deadline': self._clock()+660}
            self._jobs[key] = job
            def expire():
                with self._lock:
                    if job['status'] in {'PREPARING_PUBLIC','CHECKING_ACCOUNT'}:
                        job['status'] = 'EXPIRED'
                        self._registry.invalidate(entry)
            job['timer'] = self._timers(660, expire)
            job['timer'].daemon = True
            job['timer'].start()
            def publish(status):
                with self._lock:
                    if job['status'] not in {'PREPARING_PUBLIC','CHECKING_ACCOUNT'} or not 0<=self._clock()-job['started']<=660:
                        raise RuntimeError('KIS_NATIVE_PREPARATION_JOB_CANCELLED_OR_EXPIRED')
                    self._registry.binding(entry)
                    if status != 'CHECKING_ACCOUNT':
                        raise ValueError('KIS_NATIVE_PREPARATION_JOB_PHASE_INVALID')
                    job['status'] = status
            def worker():
                try:
                    self._registry.binding(entry)
                    result = run(publish)
                    with self._lock:
                        if job['status'] not in {'PREPARING_PUBLIC','CHECKING_ACCOUNT'}:
                            self._registry.invalidate(entry)
                            return
                        if not 0<=self._clock()-job['started']<=660:
                            job['status'] = 'EXPIRED'
                            self._registry.invalidate(entry)
                            return
                        if result.get('ok') is not True or not result.get('nativeNonce'):
                            code = str(result.get('reason') or '')
                            if code.startswith(('LIVE_NATIVE_REVIEWED_LABEL_', 'LIVE_NATIVE_INDEPENDENT_LABEL_')):
                                job['reasonCode'] = 'KIS_LABEL_SCOPE_UNPROVEN'
                                job['userMessage'] = '국내 시세의 분봉 시각 검증이 완료되지 않아 시작을 보류했습니다.'
                            else:
                                job['reasonCode'] = code if re.fullmatch(r'[A-Za-z0-9_-]{1,120}',code) else 'KIS_NATIVE_PREPARATION_FAILED'
                            job['status'] = 'FAILED'
                            self._registry.invalidate(entry)
                        else:
                            self._registry.binding(entry)
                            job['result'] = deepcopy(result)
                            job['status'] = 'READY'
                except Exception:
                    with self._lock:
                        if job['status'] not in {'CANCELLED','EXPIRED'}:
                            job['status'] = 'FAILED'
                            job['reasonCode'] = 'KIS_NATIVE_PREPARATION_FAILED'
                        self._registry.invalidate(entry)
                finally:
                    job['timer'].cancel()
            thread = self._threads(target=worker, name='kis-native-public-preparation', daemon=True)
            thread.start()
            return {'ok': True, 'preparationId': key, 'status': 'PREPARING_PUBLIC',
                    'maximumPreparationSeconds': 660, 'runtimeStarted': False, 'brokerSubmissionPerformed': False}

    def status(self, preparation_id):
        if type(preparation_id) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}', preparation_id):
            return {'ok': False, 'status': 'FAILED', 'reasonCode': 'KIS_NATIVE_PREPARATION_UNKNOWN'}
        with self._lock:
            job = self._jobs.get(preparation_id)
            if job is None:
                return {'ok': False, 'status': 'FAILED', 'reasonCode': 'KIS_NATIVE_PREPARATION_UNKNOWN'}
            if job['status'] in {'PREPARING_PUBLIC','CHECKING_ACCOUNT'} and not 0<=self._clock()-job['started']<=660:
                job['status'] = 'EXPIRED'
                self._registry.invalidate(job['entry'])
            if job['status'] in {'PREPARING_PUBLIC','CHECKING_ACCOUNT','READY'}:
                try:
                    self._registry.binding(job['entry'])
                except RuntimeError:
                    job['status'] = 'EXPIRED' if job['entry'].deadline and self._clock() >= job['entry'].deadline else 'CANCELLED'
            return {'ok': True, 'preparationId': preparation_id, 'status': job['status'],
                    'runtimeStarted': False, 'brokerSubmissionPerformed': False,
                    **(deepcopy(job['result']) if job['status']=='READY' else {}),
                    **({'reasonCode':job['reasonCode']} if 'reasonCode' in job else {}),
                    **({'userMessage':job['userMessage']} if 'userMessage' in job else {})}

    def consume(self, nonce):
        with self._lock:
            rows = [job for job in self._jobs.values() if job['entry'].nonce == nonce]
            for job in rows:
                if job['status'] != 'READY':
                    raise RuntimeError('KIS_NATIVE_PREPARATION_JOB_NOT_READY')
                job['status'] = 'CONSUMED'
                job['result'] = {}

    def cancel(self, preparation_id):
        with self._lock:
            value = self.status(preparation_id)
            if value.get('ok') is not True:
                return value
            job = self._jobs[preparation_id]
            if job['status'] == 'CONSUMED':
                return {'ok':False, 'preparationId':preparation_id, 'status':'CONSUMED',
                        'reasonCode':'KIS_NATIVE_PREPARATION_ALREADY_USED_USE_RUNTIME_STOP'}
            job['status'] = 'CANCELLED'
            job['result'] = {}
            job['timer'].cancel()
            self._registry.invalidate(job['entry'])
            return {'ok':True, 'preparationId':preparation_id, 'status':'CANCELLED',
                    'runtimeStarted':False, 'brokerSubmissionPerformed':False}


KIS_NATIVE_PREPARATION_JOBS = KisNativePreparationJobs()
