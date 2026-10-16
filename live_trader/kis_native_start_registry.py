"""Process-private native-start data ownership; this module grants no orders.

Revocation sets an Event synchronously. Socket close/join is delegated to a
separate thread, which owns none of the state or route authority locks.
"""
from copy import deepcopy
from dataclasses import dataclass, field
import threading
import time


@dataclass(eq=False)
class NativePreparation:
    binding: dict
    revoked: threading.Event = field(default_factory=threading.Event)
    prepared: object = None
    nonce: str = ""
    phase: str = "PREPARING"
    deadline: float = 0
    timer: object = None
    activation: dict = field(default_factory=dict)


class KisNativeStartRegistry:
    def __init__(self, *, monotonic_clock=time.monotonic, close_dispatch=None, timer_factory=threading.Timer):
        self._clock = monotonic_clock
        self._lock = threading.RLock()
        self._entries = []
        self._timers = timer_factory
        self._dispatch = close_dispatch or self._background_close

    @staticmethod
    def _background_close(prepared):
        def close():
            try:
                prepared.close()
            except Exception:
                # The synchronous revoke Event remains set even if transport
                # shutdown fails. Such an object can never be reused.
                pass
        threading.Thread(target=close, name="kis-native-public-close", daemon=True).start()

    def _require(self, entry):
        if not any(current is entry for current in self._entries) or entry.revoked.is_set():
            raise RuntimeError("KIS_NATIVE_PREPARATION_REVOKED")
        if entry.phase in {"ARMED", "CONSUMED"} and not 0 < entry.deadline - self._clock() <= 45:
            self._invalidate(entry)
            raise RuntimeError("KIS_NATIVE_PREPARATION_EXPIRED")

    def _invalidate(self, entry):
        entry.revoked.set()
        entry.phase = "REVOKED"
        if entry.timer is not None:
            entry.timer.cancel()
        self._entries = [current for current in self._entries if current is not entry]
        if entry.prepared is not None:
            entry.prepared.revoke("STATE_OR_NATIVE_CONFIRMATION_REVOKED")
            self._dispatch(entry.prepared)

    def reserve(self, binding):
        binding = deepcopy(binding)
        scope = binding["context"]["runtimeScopeId"]
        with self._lock:
            for entry in list(self._entries):
                if entry.binding["context"]["runtimeScopeId"] == scope:
                    self._invalidate(entry)
            if len(self._entries) >= 16:
                raise RuntimeError("KIS_NATIVE_PREPARATION_CAPACITY")
            entry = NativePreparation(binding)
            self._entries.append(entry)
            return entry

    def binding(self, entry):
        with self._lock:
            self._require(entry)
            return deepcopy(entry.binding)

    def attach(self, entry, prepared):
        with self._lock:
            self._require(entry)
            if entry.prepared is not None or entry.phase != "PREPARING":
                raise RuntimeError("KIS_NATIVE_PREPARATION_ALREADY_ATTACHED")
            entry.prepared = prepared

    def arm(self, entry, nonce):
        with self._lock:
            self._require(entry)
            if type(nonce) is not str or not nonce or entry.prepared is None or entry.phase != "PREPARING":
                raise RuntimeError("KIS_NATIVE_PREPARATION_ARM_INVALID")
            entry.nonce, entry.phase, entry.deadline = nonce, "ARMED", self._clock() + 45
            entry.timer = self._timers(45, lambda: self.expire(entry))
            entry.timer.daemon = True
            entry.timer.start()

    def expire(self, entry):
        with self._lock:
            if any(current is entry for current in self._entries) and entry.phase != "ACTIVE":
                self._invalidate(entry)

    def consume(self, nonce, binding):
        with self._lock:
            rows = [entry for entry in self._entries if entry.nonce == nonce]
            if len(rows) != 1:
                raise RuntimeError("KIS_NATIVE_PREPARATION_EXACT_PRIVATE_OBJECT_REQUIRED")
            entry = rows[0]
            self._require(entry)
            if entry.phase != "ARMED" or entry.binding != binding:
                self._invalidate(entry)
                raise RuntimeError("KIS_NATIVE_PREPARATION_SCOPE_OR_REPLAY_INVALID")
            entry.phase = "CONSUMED"
            return entry

    def activated(self, entry, session):
        with self._lock:
            self._require(entry)
            if entry.phase != "CONSUMED":
                raise RuntimeError("KIS_NATIVE_PREPARATION_ACTIVATION_INVALID")
            entry.activation = deepcopy(session)
            entry.phase = "ACTIVE"
            if entry.timer is not None:
                entry.timer.cancel()

    def activation(self, entry):
        with self._lock:
            self._require(entry)
            if entry.phase != "ACTIVE":
                raise RuntimeError("KIS_NATIVE_PREPARATION_NOT_ACTIVE")
            return deepcopy(entry.activation)

    def activated_running(self, entry, session):
        with self._lock:
            self._require(entry)
            expected = {**entry.activation, "lifecycle": "RUNNING"}
            if entry.phase != "ACTIVE" or entry.activation.get("lifecycle") != "STARTING" or session != expected:
                raise RuntimeError("KIS_NATIVE_PREPARATION_RUNNING_SESSION_CHANGED")
            entry.activation = deepcopy(session)

    def entry_for_initialization(self, content_hash):
        with self._lock:
            matches = [entry for entry in self._entries if entry.phase == "ACTIVE"
                       and getattr(entry.prepared, "receipt", {}).get("contentHash") == content_hash]
            if len(matches) != 1:
                raise RuntimeError("KIS_NATIVE_RESTORE_EXACT_PRIVATE_INITIALIZATION_REQUIRED")
            self._require(matches[0])
            return matches[0]

    def invalidate(self, entry):
        with self._lock:
            if any(current is entry for current in self._entries):
                self._invalidate(entry)

    def invalidate_nonce(self, nonce):
        with self._lock:
            for entry in list(self._entries):
                if entry.nonce == nonce:
                    self._invalidate(entry)

    def invalidate_all(self, *, keep=None):
        with self._lock:
            if keep is not None:
                self._require(keep)
                if keep.phase != "CONSUMED":
                    raise RuntimeError("KIS_NATIVE_PREPARATION_KEEP_NOT_CONSUMED")
            for entry in list(self._entries):
                if entry is not keep:
                    self._invalidate(entry)

    def invalidate_stop_selector(self, profile_id="", *, runtime_id="", deployment_id=""):
        """Revoke data early, before STOP can wait for a manager preparing IO.

        Only an exact known pair or an explicit empty global selector acts.
        An invalid/partial/foreign selector cannot revoke a different account.
        """
        if any(type(value) is not str for value in (profile_id, runtime_id, deployment_id)):
            return False
        global_stop = not any((profile_id, runtime_id, deployment_id))
        if not global_stop and (profile_id not in {"", "stock"} or not runtime_id or not deployment_id):
            return False
        with self._lock:
            rows = [entry for entry in self._entries if global_stop or
                    (entry.binding["context"]["runtimeScopeId"] == runtime_id
                     and entry.binding["context"]["deploymentId"] == deployment_id)]
            for entry in rows:
                self._invalidate(entry)
            return bool(rows)


KIS_NATIVE_START_REGISTRY = KisNativeStartRegistry()
