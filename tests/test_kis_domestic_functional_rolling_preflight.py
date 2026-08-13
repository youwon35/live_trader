from __future__ import annotations

import copy
import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_contract import (
    ACTIVE_SECONDS,
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    LIVE_ORIGIN,
    MAX_GROSS_KRW,
    MAX_ORDER_KRW,
    ORDER_QUANTITY,
    OWNER_LOSS_LIMIT_KRW,
    PDNO,
    ROUTE,
)
from live_trader.kis_domestic_functional_rolling_preflight import (
    DIAGNOSTIC_SCHEMA,
    KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ACCOUNT_AUTHORITY_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ORDER_AUTHORITY_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_RELEASE_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_TOKEN_AUTHORITY_AVAILABLE,
    DurableKisDomesticFunctionalRollingPreflight,
    KisDomesticFunctionalRollingPreflightBlocked,
    rolling_preflight_component_status,
)


KEY = b"rolling-preflight-test-server-authority-key-48bytes!"
KEY_ID = "kis-rolling-test-authority-v1"
ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
CONTRACT = "c" * 64
CODE = "d" * 64
PREAPPROVAL = "e" * 64
BASELINE = "f" * 64
PROJECTION = "1" * 64
CAPTURE_1 = "2" * 64
CAPTURE_2 = "3" * 64
ARM = "kis-public-arm-0123456789abcdef0123456789abcdef"
PROCESS_1 = "kis-rolling-generation-11111111111111111111111111111111"
PROCESS_2 = "kis-rolling-generation-22222222222222222222222222222222"
SOCKET = "kis-ws-generation-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BOUNDARY = datetime(2026, 8, 14, 3, 0, tzinfo=timezone.utc)
_TEMPS: list[tempfile.TemporaryDirectory] = []


def tearDownModule() -> None:
    while _TEMPS:
        _TEMPS.pop().cleanup()


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sign(domain: str, body) -> str:
    return hmac.new(
        KEY,
        domain.encode("ascii") + b"\n" + _canonical(body),
        hashlib.sha256,
    ).hexdigest()


def _verify(domain: str, body, signature: str) -> bool:
    return type(signature) is str and hmac.compare_digest(
        _sign(domain, body), signature
    )


class _Clock:
    def __init__(self, value: datetime | None = None) -> None:
        self.value = value or (BOUNDARY - timedelta(seconds=5))

    def now(self) -> datetime:
        return self.value


def _route_pages() -> list[dict]:
    routes = (
        ("/uapi/domestic-stock/v1/trading/inquire-balance", "TTTC8434R"),
        ("/uapi/domestic-stock/v1/trading/inquire-daily-ccld", "TTTC0081R"),
        ("/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl", "TTTC0084R"),
        ("/uapi/domestic-stock/v1/trading/inquire-period-trade-profit", "TTTC8715R"),
        ("/uapi/domestic-stock/v1/trading/inquire-period-profit", "TTTC8708R"),
        ("/uapi/domestic-stock/v1/quotations/chk-holiday", "CTCA0903R"),
    )
    return [
        {
            "endpoint": endpoint,
            "trId": tr_id,
            "pageCountAcrossTwoCaptures": 2,
            "terminalContinuationObserved": True,
            "allPagesSigned": True,
        }
        for index, (endpoint, tr_id) in enumerate(routes)
    ]


def _diagnostic_body(**changes) -> dict:
    route_pages = _route_pages()
    gets = sum(page["pageCountAcrossTwoCaptures"] for page in route_pages)
    completed = BOUNDARY - timedelta(seconds=5)
    started = completed - timedelta(seconds=25.2)
    bundle = _hash(
        {
            "rawCaptureHashes": [CAPTURE_1, CAPTURE_2],
            "causalProjectionHash": PROJECTION,
            "preactivationBaselineHash": BASELINE,
        }
    )
    body = {
        "schemaVersion": DIAGNOSTIC_SCHEMA,
        "route": ROUTE,
        "origin": LIVE_ORIGIN,
        "pdno": PDNO,
        "accountFingerprint": ACCOUNT,
        "credentialConfigurationHash": CREDENTIAL,
        "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
        "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
        "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
        "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
        "contractEnvelopeHash": CONTRACT,
        "codeManifestHash": CODE,
        "publicArmId": ARM,
        "preapprovalHash": PREAPPROVAL,
        "tradingDate": "2026-08-14",
        "intendedNextOpenAt": BOUNDARY.isoformat().replace("+00:00", "Z"),
        "startedAt": started.isoformat().replace("+00:00", "Z"),
        "completedAt": completed.isoformat().replace("+00:00", "Z"),
        "captureBundleHash": bundle,
        "preactivationBaselineHash": BASELINE,
        "causalProjectionHash": PROJECTION,
        "rawCaptureHashes": [CAPTURE_1, CAPTURE_2],
        "captureCount": 2,
        "routePages": route_pages,
        "officialGetRequestCount": gets,
        "physicalGetAttemptCount": gets,
        "physicalGetAttemptCountComplete": True,
        "minimumRequestIntervalSeconds": "2.1",
        "physicalPacingElapsedSeconds": "25.2",
        "stableReadElapsedSeconds": "25.2",
        "allGetPaginationComplete": True,
        "allGetPagesSigned": True,
        "officialTradingDayOpen": True,
        "stableRepeatedReads": True,
        "stableComparison": "PARSED_CAUSAL_PROJECTION",
        "accountWideWorkingOrdersZero": True,
        "balanceBaselineComplete": True,
        "costBaselineComplete": True,
        "hiddenGetRetryCount": 0,
        "redirectFollowCount": 0,
        "tradingPostDeleteDispatchCount": 0,
        "caps": {
            "quantity": ORDER_QUANTITY,
            "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
            "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
            "ownerLossMustRemainBelowKrw": format(OWNER_LOSS_LIMIT_KRW, "f"),
            "activeSeconds": ACTIVE_SECONDS,
        },
        "serverAuthorityKeyIdHash": hashlib.sha256(KEY_ID.encode()).hexdigest(),
        "serverAuthorityRestartVerifiable": True,
        "trustedDiagnosticResult": True,
        "durableDiagnosticPersisted": True,
        "rollingWatcherPrivateAccountAuthorityAvailable": False,
        "rollingWatcherTokenAuthorityAvailable": False,
        "rollingWatcherOrderAuthorityAvailable": False,
        "finalQuoteIncluded": False,
        "finalQuoteAvailable": False,
        "finalQuoteAuthoritative": False,
        "promotionEligible": False,
        "releaseEvidenceEligible": False,
    }
    body.update(changes)
    return body


def _diagnostic(**changes) -> dict:
    body = _diagnostic_body(**changes)
    digest = _hash(body)
    return {
        "body": body,
        "diagnosticHash": digest,
        "serverAuthoritySignature": _sign(
            "ROLLING_PREFLIGHT_DIAGNOSTIC",
            {**body, "diagnosticHash": digest},
        ),
    }


def _fixture(
    *,
    clock: _Clock | None = None,
    path: Path | None = None,
    process: str = PROCESS_1,
):
    clock = clock or _Clock()
    if path is None:
        temporary = tempfile.TemporaryDirectory()
        _TEMPS.append(temporary)
        path = Path(temporary.name) / "rolling.sqlite3"
    journal = DurableKisDomesticFunctionalRollingPreflight(
        path,
        capture_signer=_sign,
        capture_verifier=_verify,
        server_authority_key_id=KEY_ID,
        process_generation=process,
        account_fingerprint=ACCOUNT,
        credential_configuration_hash=CREDENTIAL,
        contract_envelope_hash=CONTRACT,
        code_manifest_hash=CODE,
        public_arm_id=ARM,
        preapproval_hash=PREAPPROVAL,
        wall_clock=clock.now,
        owner_token_factory=lambda: b"rolling-owner-token-value-that-is-long-enough",
        id_factory=lambda: "9" * 32,
    )
    return journal, clock, path


def _trigger(snapshot_id: str, *, observed_at: datetime | None = None, **changes):
    observed = observed_at or (BOUNDARY + timedelta(seconds=1))
    body = {
        "schemaVersion": "kis-domestic-rolling-preflight-trigger/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "snapshotId": snapshot_id,
        "publicArmId": ARM,
        "preapprovalHash": PREAPPROVAL,
        "evaluationId": "kis-evaluation-1",
        "evaluationHash": "4" * 64,
        "triggerId": "kis-trigger-1",
        "triggerHash": "5" * 64,
        "sourceGeneration": SOCKET,
        "barOpenAt": BOUNDARY.isoformat().replace("+00:00", "Z"),
        "observedAt": observed.isoformat().replace("+00:00", "Z"),
        "accountFingerprint": ACCOUNT,
        "credentialConfigurationHash": CREDENTIAL,
        "contractEnvelopeHash": CONTRACT,
        "codeManifestHash": CODE,
        "sessionId": "kis-session-1",
        "sessionNonceHash": "6" * 64,
        "accountAuthorityAvailable": False,
        "tokenAuthorityAvailable": False,
        "orderAuthorityAvailable": False,
    }
    body.update(changes)
    digest = _hash(body)
    return {
        "body": body,
        "triggerEnvelopeHash": digest,
        "serverAuthoritySignature": _sign(
            "ROLLING_PREFLIGHT_TRIGGER",
            {**body, "triggerEnvelopeHash": digest},
        ),
    }


class KisDomesticFunctionalRollingPreflightTest(unittest.TestCase):
    def test_flags_and_final_quote_remain_false(self) -> None:
        status = rolling_preflight_component_status()
        for value in (
            KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_PRODUCTION_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_NETWORK_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ACCOUNT_AUTHORITY_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_TOKEN_AUTHORITY_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ORDER_AUTHORITY_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_MUTATION_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_RELEASE_AVAILABLE,
            status["finalQuoteAvailable"],
            status["networkOrderPostAllowed"],
        ):
            self.assertFalse(value)

    def test_happy_ready_to_single_consume(self) -> None:
        journal, clock, _path = _fixture()
        ready = journal.accept_snapshot(_diagnostic())
        snapshot_id = ready["body"]["snapshotId"]
        self.assertEqual("READY", journal.snapshot(snapshot_id)["state"])
        clock.value = BOUNDARY + timedelta(seconds=1)
        receipt = journal.consume_for_trigger(_trigger(snapshot_id))
        self.assertTrue(receipt["body"]["singleUseConsumed"])
        self.assertFalse(receipt["body"]["networkOrderPostAllowed"])
        self.assertEqual("CONSUMED", journal.snapshot(snapshot_id)["state"])
        with self.assertRaisesRegex(
            KisDomesticFunctionalRollingPreflightBlocked,
            "already-consumed",
        ):
            journal.consume_for_trigger(_trigger(snapshot_id))

    def test_stale_future_and_trigger_after_two_seconds_are_post_zero(self) -> None:
        stale_completed = BOUNDARY - timedelta(seconds=61)
        with self.subTest("stale snapshot"):
            journal, _clock, _path = _fixture()
            with self.assertRaisesRegex(
                KisDomesticFunctionalRollingPreflightBlocked, "not-fresh"
            ):
                journal.accept_snapshot(
                    _diagnostic(
                        completedAt=stale_completed.isoformat().replace("+00:00", "Z"),
                        startedAt=(stale_completed - timedelta(seconds=25.2)).isoformat().replace("+00:00", "Z"),
                    )
                )
        with self.subTest("future completed"):
            journal, _clock, _path = _fixture()
            future = BOUNDARY + timedelta(seconds=1)
            with self.assertRaises(KisDomesticFunctionalRollingPreflightBlocked):
                journal.accept_snapshot(
                    _diagnostic(
                        completedAt=future.isoformat().replace("+00:00", "Z"),
                        startedAt=(future - timedelta(seconds=25.2)).isoformat().replace("+00:00", "Z"),
                    )
                )
        with self.subTest("late trigger"):
            journal, clock, _path = _fixture()
            ready = journal.accept_snapshot(_diagnostic())
            sid = ready["body"]["snapshotId"]
            clock.value = BOUNDARY + timedelta(seconds=3)
            with self.assertRaisesRegex(
                KisDomesticFunctionalRollingPreflightBlocked, "post-zero"
            ):
                journal.consume_for_trigger(
                    _trigger(sid, observed_at=BOUNDARY + timedelta(seconds=3))
                )
            self.assertEqual("REJECTED_STALE", journal.snapshot(sid)["state"])

    def test_missing_route_page_count_working_or_external_change_rejected(self) -> None:
        cases = []
        pages = _route_pages()[:-1]
        cases.append(("missing route", {"routePages": pages}))
        pages = _route_pages()
        pages[0]["pageCountAcrossTwoCaptures"] = 1
        cases.append(("missing page", {"routePages": pages}))
        cases.append(("physical count", {"physicalGetAttemptCount": 13}))
        cases.append(("working order", {"accountWideWorkingOrdersZero": False}))
        cases.append(("external delta", {"stableRepeatedReads": False}))
        cases.append(("weak pacing", {"minimumRequestIntervalSeconds": "2"}))
        cases.append(("numeric pacing", {"minimumRequestIntervalSeconds": 2.1}))
        for label, changes in cases:
            with self.subTest(label):
                journal, _clock, _path = _fixture()
                with self.assertRaises(KisDomesticFunctionalRollingPreflightBlocked):
                    journal.accept_snapshot(_diagnostic(**changes))

    def test_account_artifact_and_code_mismatch_rejected(self) -> None:
        cases = (
            {"accountFingerprint": "7" * 64},
            {"artifactContentHash": "8" * 64},
            {"codeManifestHash": "9" * 64},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                journal, _clock, _path = _fixture()
                with self.assertRaisesRegex(
                    KisDomesticFunctionalRollingPreflightBlocked,
                    "binding-or-truth",
                ):
                    journal.accept_snapshot(_diagnostic(**changes))

    def test_restart_invalidates_ready_snapshot(self) -> None:
        journal, clock, path = _fixture()
        ready = journal.accept_snapshot(_diagnostic())
        sid = ready["body"]["snapshotId"]
        successor, _clock, _path = _fixture(
            clock=clock, path=path, process=PROCESS_2
        )
        self.assertEqual((sid,), successor.startup_invalidated_snapshot_ids)
        self.assertEqual("INVALIDATED_RESTART", successor.snapshot(sid)["state"])

    def test_trigger_binding_change_is_rejected_and_burned(self) -> None:
        journal, clock, _path = _fixture()
        ready = journal.accept_snapshot(_diagnostic())
        sid = ready["body"]["snapshotId"]
        clock.value = BOUNDARY + timedelta(seconds=1)
        with self.assertRaisesRegex(
            KisDomesticFunctionalRollingPreflightBlocked, "post-zero"
        ):
            journal.consume_for_trigger(_trigger(sid, codeManifestHash="7" * 64))
        self.assertEqual("REJECTED_TRIGGER_MISMATCH", journal.snapshot(sid)["state"])

    def test_strict_booleans_and_non_session_boundary_are_rejected(self) -> None:
        journal, _clock, _path = _fixture()
        with self.assertRaisesRegex(
            KisDomesticFunctionalRollingPreflightBlocked,
            "binding-or-truth",
        ):
            journal.accept_snapshot(_diagnostic(accountWideWorkingOrdersZero=1))

        saturday = datetime(2026, 8, 15, 3, 0, tzinfo=timezone.utc)
        completed = saturday - timedelta(seconds=5)
        with self.assertRaisesRegex(
            KisDomesticFunctionalRollingPreflightBlocked,
            "official-xkrx-session",
        ):
            journal.accept_snapshot(
                _diagnostic(
                    tradingDate="2026-08-15",
                    intendedNextOpenAt=saturday.isoformat().replace("+00:00", "Z"),
                    startedAt=(completed - timedelta(seconds=25.2)).isoformat().replace("+00:00", "Z"),
                    completedAt=completed.isoformat().replace("+00:00", "Z"),
                )
            )

    def test_consumption_reverifies_stored_diagnostic_snapshot_and_row(self) -> None:
        journal, clock, path = _fixture()
        ready = journal.accept_snapshot(_diagnostic())
        sid = ready["body"]["snapshotId"]
        conn = sqlite3.connect(path)
        try:
            body = json.loads(
                conn.execute(
                    "SELECT snapshot_json FROM kis_functional_rolling_preflight_snapshot "
                    "WHERE snapshot_id=?", (sid,)
                ).fetchone()[0]
            )
            body["finalQuoteAvailable"] = True
            conn.execute(
                "UPDATE kis_functional_rolling_preflight_snapshot SET "
                "snapshot_json=?,snapshot_hash=?,snapshot_signature=? "
                "WHERE snapshot_id=?",
                (
                    _canonical(body).decode("utf-8"),
                    _hash(body),
                    _sign("ROLLING_PREFLIGHT_SNAPSHOT", body),
                    sid,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        clock.value = BOUNDARY + timedelta(seconds=1)
        with self.assertRaisesRegex(
            KisDomesticFunctionalRollingPreflightBlocked,
            "stored-snapshot-row-mismatch",
        ):
            journal.consume_for_trigger(_trigger(sid))
        self.assertEqual("READY", journal.snapshot(sid)["state"])

    def test_dirty_schema_fails_closed(self) -> None:
        journal, _clock, path = _fixture()
        del journal
        conn = sqlite3.connect(path)
        try:
            conn.execute("ALTER TABLE kis_functional_rolling_preflight_snapshot ADD COLUMN hostile TEXT")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            KisDomesticFunctionalRollingPreflightBlocked, "schema-dirty"
        ):
            _fixture(path=path)


if __name__ == "__main__":
    unittest.main()
