from __future__ import annotations

import hashlib
import json
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping


DEFAULT_TTL_SECONDS = 90


def _digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _secret_digest(value: object) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _utc_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True)
class _ChallengeRecord:
    action: str
    context_digest: str
    token_digest: str
    phrase_digest: str
    issued_epoch: float
    expires_epoch: float


class SafetyConfirmationStore:
    """Process-local, one-shot confirmation challenges.

    Only SHA-256 digests are retained. A process restart therefore invalidates
    every outstanding challenge, and neither snapshots nor audit payloads have
    access to the raw token or typed phrase.
    """

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._ttl_seconds = max(60, min(120, int(ttl_seconds)))
        self._clock = clock
        self._lock = threading.RLock()
        self._records: dict[str, _ChallengeRecord] = {}

    def issue(
        self,
        *,
        action: str,
        context: Mapping[str, Any],
        expected_phrase: str,
        display_context: Mapping[str, Any],
    ) -> dict[str, Any]:
        normalized_action = str(action or "").strip().upper()
        phrase = str(expected_phrase or "").strip()
        if not normalized_action or not phrase:
            return {"ok": False, "reason": "safety-confirmation-context-invalid"}
        now = float(self._clock())
        expires = now + self._ttl_seconds
        challenge_id = secrets.token_urlsafe(18)
        token = secrets.token_urlsafe(32)
        record = _ChallengeRecord(
            action=normalized_action,
            context_digest=_digest(dict(context)),
            token_digest=_secret_digest(token),
            phrase_digest=_secret_digest(phrase),
            issued_epoch=now,
            expires_epoch=expires,
        )
        with self._lock:
            self._purge_expired_unlocked(now)
            self._records[challenge_id] = record
        challenge = {
            "challengeId": challenge_id,
            "token": token,
            "expectedPhrase": phrase,
            "expiresAt": _utc_iso(expires),
            "displayContext": dict(display_context),
        }
        return {"ok": True, **challenge, "challenge": dict(challenge)}

    def consume(
        self,
        *,
        action: str,
        context: Mapping[str, Any],
        confirmation: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        values = confirmation if isinstance(confirmation, Mapping) else {}
        challenge_id = str(values.get("challengeId") or "").strip()
        token = str(values.get("token") or "")
        typed_phrase = str(values.get("typedPhrase") or "").strip()
        if not challenge_id or not token or not typed_phrase:
            return {"ok": False, "reason": "safety-confirmation-required"}

        # Pop before any validation. Wrong, expired, raced, or context-stale
        # submissions are deliberately one-shot and cannot be retried.
        with self._lock:
            record = self._records.pop(challenge_id, None)
        if record is None:
            return {"ok": False, "reason": "safety-confirmation-missing-or-used"}
        now = float(self._clock())
        if now > record.expires_epoch:
            return {"ok": False, "reason": "safety-confirmation-expired"}
        if not secrets.compare_digest(
            record.action,
            str(action or "").strip().upper(),
        ):
            return {"ok": False, "reason": "safety-confirmation-action-changed"}
        if not secrets.compare_digest(record.context_digest, _digest(dict(context))):
            return {"ok": False, "reason": "safety-confirmation-context-changed"}
        if not secrets.compare_digest(record.token_digest, _secret_digest(token)):
            return {"ok": False, "reason": "safety-confirmation-token-invalid"}
        if not secrets.compare_digest(
            record.phrase_digest,
            _secret_digest(typed_phrase),
        ):
            return {"ok": False, "reason": "safety-confirmation-phrase-invalid"}
        return {"ok": True, "reason": "safety-confirmation-consumed"}

    def _purge_expired_unlocked(self, now: float) -> None:
        expired = [
            challenge_id
            for challenge_id, record in self._records.items()
            if now > record.expires_epoch
        ]
        for challenge_id in expired:
            self._records.pop(challenge_id, None)

    def pending_count_for_tests(self) -> int:
        with self._lock:
            return len(self._records)


SAFETY_CONFIRMATIONS = SafetyConfirmationStore()
