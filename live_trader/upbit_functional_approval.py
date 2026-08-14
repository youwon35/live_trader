from __future__ import annotations

"""Durable server-owned approval store for the Upbit functional lane."""

from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import threading
from typing import Any, Callable, Mapping

from .upbit_continuous_functional import (
    AccountExclusivityProofVerifier,
    EVIDENCE_CLASS,
    EXECUTION_ROUTE,
    SCHEMA_VERSION,
    SYMBOL,
    UpbitFunctionalBlocked,
    _account_exclusivity_evidence_complete,
    account_exclusivity_verifier_wiring_status,
)
from trading_runtime.functional_test import (
    issue_functional_test_permit,
    parse_functional_test_permit,
)


_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
UPBIT_FUNCTIONAL_OWNER_LEASE_SCHEMA_VERSION = (
    "upbit-functional-owner-lease/v1"
)
UPBIT_FUNCTIONAL_OWNER_LEASE_SECONDS = 30


def _text(value: object) -> str:
    return str(value or "").strip()


def _stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _exact_lower_hash(value: object) -> str | None:
    if type(value) is not str or _HASH_RE.fullmatch(value) is None:
        return None
    return value


def _utc(value: object, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise UpbitFunctionalBlocked(
                f"upbit-functional-approval-{label}-invalid"
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UpbitFunctionalBlocked(
            f"upbit-functional-approval-{label}-timezone-missing"
        )
    return parsed.astimezone(timezone.utc)


def _functional_wiring_evidence_complete(
    value: Mapping[str, Any],
    *,
    account_exclusivity_verifier: (
        AccountExclusivityProofVerifier | None
    ) = None,
    account_exclusivity_verifier_pin: Mapping[str, Any] | None = None,
) -> bool:
    """Recompute the nonpromotion wiring seal from primitive evidence.

    The producer's ``functionalWiringPassed`` boolean is only a claim.  This
    independent terminal verifier requires exact fills/fees, no working order,
    the fixed KRW caps, the activation-relative two-hour clock, and final
    authority reset before a bootstrap may be consumed as a wiring success.
    """

    try:
        activated_at = _utc(value.get("activatedAt"), "wiring-activated-at")
        permit_ends_at = _utc(
            value.get("permitEndsAt"), "wiring-permit-ends-at"
        )
        final_observed_at = _utc(
            value.get("finalObservedAt"), "wiring-final-observed-at"
        )
        actual_duration = Decimal(str(value.get("actualDurationSeconds")))
        monotonic_duration = Decimal(
            str(value.get("processMonotonicElapsedSeconds"))
        )
        buy_notional = Decimal(str(value.get("strategyBuyExecutedNotional")))
        max_order_notional = Decimal(str(value.get("maxOrderNotionalKRW")))
        max_observed_gross = Decimal(
            str(value.get("maxObservedOwnerGrossExposure"))
        )
        max_gross = Decimal(str(value.get("maxGrossExposureKRW")))
        owner_loss = Decimal(str(value.get("ownerLoss")))
        max_owner_loss = Decimal(str(value.get("maxOwnerLoss")))
        fees = Decimal(str(value.get("fees")))
        account_open_count = int(value.get("accountOpenOrderCount"))
        owned_working_count = int(value.get("ownedWorkingOrderCount"))
        claim_count = int(value.get("claimCount"))
    except (InvalidOperation, TypeError, ValueError, UpbitFunctionalBlocked):
        return False
    terminal_seal = value.get("terminalPrivateStreamSeal")
    if not isinstance(terminal_seal, Mapping):
        return False
    seal_hash = _exact_lower_hash(terminal_seal.get("sealHash"))
    seal_body = {
        key: item for key, item in terminal_seal.items() if key != "sealHash"
    }
    duration_from_timestamps = Decimal(
        str((final_observed_at - activated_at).total_seconds())
    )
    return bool(
        value.get("schemaVersion") == SCHEMA_VERSION
        and value.get("evidenceClass") == EVIDENCE_CLASS
        and value.get("promotionEligible") is False
        and value.get("functionalWiringPassed") is True
        and value.get("strategyBuyTerminalFilled") is True
        and value.get("strategySellTerminalFilled") is True
        and value.get("strategyBuyReconciled") is True
        and value.get("strategySellReconciled") is True
        and value.get("fillAndFeeTruthComplete") is True
        and value.get("strategyOrderCountExact") is True
        and value.get("noReentryVerified") is True
        and claim_count == 2
        and value.get("cleanupFlattenUsed") is False
        and value.get("strategyNotionalCapSatisfied") is True
        and value.get("strategyGrossExposureCapSatisfied") is True
        and max_order_notional == Decimal("10000")
        and Decimal("0") < buy_notional <= max_order_notional
        and max_gross == Decimal("10000")
        and Decimal("0") < max_observed_gross <= max_gross
        and value.get("ownerLossLimitSatisfied") is True
        and max_owner_loss == Decimal("1000")
        and owner_loss < max_owner_loss
        and fees >= 0
        and value.get("preexistingBaselinePreserved") is True
        and value.get("baselineRestoredWithinExchangePrecision") is True
        and value.get("orderableResidual") is False
        and account_open_count == 0
        and owned_working_count == 0
        and value.get("privateStreamContinuous") is True
        and value.get("accountExternalActivityAbsent") is True
        and value.get("accountExclusivityProofVerified") is True
        and value.get("accountExclusivityAuthorityPinned") is True
        and value.get("accountExclusivityContinuouslyVerified") is True
        and value.get("otherApiKeysAbsent") is True
        and value.get("manualTradingAbsent") is True
        and value.get("otherBotsAbsent") is True
        and _account_exclusivity_evidence_complete(
            value,
            verifier=account_exclusivity_verifier,
            verifier_pin=account_exclusivity_verifier_pin,
        )
        and terminal_seal.get("streamContinuous") is True
        and terminal_seal.get("gapDetected") is False
        and terminal_seal.get("externalActivityAbsent") is True
        and seal_hash is not None
        and secrets.compare_digest(seal_hash, _stable_hash(seal_body))
        and value.get("activationRelativePermitExact") is True
        and value.get("exactTwoHourRuntimeComplete") is True
        and value.get("processMonotonicContinuity") is True
        and value.get("clockDiscontinuityAbsent") is True
        and str(value.get("requiredActiveDurationSeconds")) == "7200"
        and permit_ends_at - activated_at == timedelta(seconds=7200)
        and actual_duration == duration_from_timestamps
        and actual_duration >= Decimal("7200")
        and monotonic_duration >= Decimal("7200")
        and abs(actual_duration - monotonic_duration) <= Decimal("15")
        and value.get("functionalCapabilityCleared") is True
        and value.get("functionalMutationEnabled") is False
        and value.get("realOrdersEnabled") is False
        and value.get("newEntriesBlocked") is True
        and _exact_lower_hash(
            value.get("officialRestRawSnapshotHash")
        )
        is not None
    )


def _raw_decimal(value: object) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite() or parsed < 0:
        raise ValueError("invalid nonnegative decimal")
    return parsed


def _decimal_text(value: Decimal) -> str:
    rendered = format(value.normalize(), "f")
    return "0" if rendered in {"", "-0"} else rendered


def _official_rest_raw_matches_terminal(
    snapshot: Mapping[str, Any], terminal: Mapping[str, Any]
) -> bool:
    """Independently reduce persisted raw GET payloads to final truth."""

    try:
        def unwrap(
            key: str, endpoint: str, query: list[list[str]]
        ) -> Any:
            wrapper = snapshot.get(key)
            if (
                not isinstance(wrapper, Mapping)
                or wrapper.get("endpoint") != endpoint
                or wrapper.get("query") != query
            ):
                raise ValueError("raw request boundary mismatch")
            return wrapper.get("payload")

        def normalize_order(raw: Mapping[str, Any]) -> dict[str, Any]:
            normalized = dict(raw)
            normalized.update(
                {
                    "uuid": _text(raw.get("uuid")),
                    "identifier": _text(raw.get("identifier")),
                    "market": _text(raw.get("market")).upper(),
                    "side": _text(raw.get("side")).upper(),
                    "state": _text(raw.get("state")).lower(),
                }
            )
            if (
                not normalized["uuid"]
                or normalized["side"] not in {"BID", "ASK"}
                or normalized["state"]
                not in {"wait", "watch", "done", "cancel", "reject"}
            ):
                raise ValueError("invalid raw order")
            return normalized

        if (
            snapshot.get("schemaVersion")
            != "upbit-functional-official-rest-raw/v2"
            or snapshot.get("sessionId") != terminal.get("sessionId")
            or snapshot.get("accountFingerprint")
            != terminal.get("accountFingerprint")
            or _utc(snapshot.get("sessionStartedAt"), "raw-session-start")
            != _utc(terminal.get("sessionStartedAt"), "terminal-session-start")
            or _utc(snapshot.get("observationCutoff"), "raw-cutoff")
            != _utc(
                terminal.get("observationStartedAt"),
                "terminal-observation-start",
            )
        ):
            return False
        raw_accounts = unwrap("accounts", "/v1/accounts", [])
        if not isinstance(raw_accounts, list):
            return False
        accounts: dict[str, tuple[Decimal, Decimal]] = {}
        for raw in raw_accounts:
            if not isinstance(raw, Mapping):
                return False
            currency = _text(raw.get("currency")).upper()
            if not currency or currency in accounts:
                return False
            accounts[currency] = (
                _raw_decimal(raw.get("balance")),
                _raw_decimal(raw.get("locked")),
            )
        if "KRW" not in accounts or "BTC" not in accounts:
            return False
        account_rows = sorted(
            (
                {
                    "currency": currency,
                    "available": _decimal_text(values[0]),
                    "locked": _decimal_text(values[1]),
                }
                for currency, values in accounts.items()
            ),
            key=lambda row: row["currency"],
        )
        if account_rows != terminal.get("accountRows"):
            return False
        chance = unwrap(
            "orderChance", "/v1/orders/chance", [["market", "KRW-BTC"]]
        )
        if not isinstance(chance, Mapping):
            return False
        market = chance.get("market")
        bid_account = chance.get("bid_account")
        ask_account = chance.get("ask_account")
        if not all(
            isinstance(row, Mapping)
            for row in (market, bid_account, ask_account)
        ):
            return False
        bid_rule = market.get("bid")
        ask_rule = market.get("ask")
        if (
            _text(market.get("id") or market.get("market")).upper()
            != "KRW-BTC"
            or not isinstance(bid_rule, Mapping)
            or not isinstance(ask_rule, Mapping)
            or _text(bid_account.get("currency")).upper() != "KRW"
            or _text(ask_account.get("currency")).upper() != "BTC"
            or _raw_decimal(bid_account.get("balance"))
            != accounts["KRW"][0]
            or _raw_decimal(ask_account.get("balance"))
            != accounts["BTC"][0]
        ):
            return False
        ticker = unwrap(
            "ticker", "/v1/ticker", [["markets", "KRW-BTC"]]
        )
        if (
            not isinstance(ticker, list)
            or len(ticker) != 1
            or not isinstance(ticker[0], Mapping)
            or _text(ticker[0].get("market")).upper() != "KRW-BTC"
        ):
            return False
        rules = terminal.get("orderRules")
        if not isinstance(rules, Mapping) or any(
            Decimal(str(rules.get(field))) != expected
            for field, expected in (
                ("bidMinTotal", _raw_decimal(bid_rule.get("min_total"))),
                ("askMinTotal", _raw_decimal(ask_rule.get("min_total"))),
                ("bidFeeRate", _raw_decimal(chance.get("bid_fee"))),
                ("askFeeRate", _raw_decimal(chance.get("ask_fee"))),
            )
        ):
            return False
        if (
            Decimal(str(terminal.get("quoteAvailable")))
            != accounts["KRW"][0]
            or Decimal(str(terminal.get("baseAvailable")))
            != accounts["BTC"][0]
            or Decimal(str(terminal.get("baseTotal")))
            != accounts["BTC"][0] + accounts["BTC"][1]
            or Decimal(str(terminal.get("markPrice")))
            != _raw_decimal(ticker[0].get("trade_price"))
        ):
            return False
        pages = snapshot.get("openOrderPages")
        if not isinstance(pages, list) or not pages:
            return False
        open_orders: list[dict[str, Any]] = []
        for index, page in enumerate(pages, 1):
            expected_query = [
                ["states[]", "wait"],
                ["states[]", "watch"],
                ["page", str(index)],
                ["limit", "100"],
                ["order_by", "asc"],
            ]
            if (
                not isinstance(page, Mapping)
                or int(page.get("page") or 0) != index
                or page.get("endpoint") != "/v1/orders/open"
                or page.get("query") != expected_query
                or not isinstance(page.get("payload"), list)
            ):
                return False
            if index < len(pages) and len(page["payload"]) != 100:
                return False
            if index == len(pages) and len(page["payload"]) >= 100:
                return False
            open_orders.extend(normalize_order(row) for row in page["payload"])
        closed_query = [
            ["states[]", "done"],
            ["states[]", "cancel"],
            ["start_time", _text(snapshot.get("sessionStartedAt"))],
            ["end_time", _text(snapshot.get("observationCutoff"))],
            ["limit", "1000"],
            ["order_by", "asc"],
        ]
        closed_raw = unwrap(
            "closedOrders", "/v1/orders/closed", closed_query
        )
        if not isinstance(closed_raw, list):
            return False
        if len(closed_raw) >= 1000:
            return False
        closed_orders = [normalize_order(row) for row in closed_raw]
        if open_orders != terminal.get("openOrders") or closed_orders != terminal.get("closedOrders"):
            return False
        all_orders = [*open_orders, *closed_orders]
        detail_rows = snapshot.get("detailsByUuid")
        identifier_rows = snapshot.get("detailsByIdentifier")
        if not isinstance(detail_rows, list) or not isinstance(identifier_rows, list):
            return False
        details: dict[str, Mapping[str, Any]] = {}
        for wrapper in detail_rows:
            if not isinstance(wrapper, Mapping) or not isinstance(wrapper.get("payload"), Mapping):
                return False
            uuid = _text(wrapper.get("uuid"))
            if (
                wrapper.get("endpoint") != "/v1/order"
                or wrapper.get("query") != [["uuid", uuid]]
            ):
                return False
            payload = normalize_order(wrapper["payload"])
            if not uuid or uuid in details or _text(payload.get("uuid")) != uuid:
                return False
            details[uuid] = payload
        if set(details) != {_text(row.get("uuid")) for row in all_orders if _text(row.get("market")).upper() == "KRW-BTC"}:
            return False
        identifiers = {
            _text(row.get("identifier")): row
            for row in all_orders
            if _text(row.get("identifier"))
        }
        raw_identifier_truth: dict[str, Any] = {}
        for wrapper in identifier_rows:
            if not isinstance(wrapper, Mapping):
                return False
            identifier = _text(wrapper.get("identifier"))
            if not identifier or identifier in raw_identifier_truth:
                return False
            if (
                wrapper.get("endpoint") != "/v1/order"
                or wrapper.get("query") != [["identifier", identifier]]
            ):
                return False
            payload = wrapper.get("payload")
            if isinstance(payload, Mapping) and payload.get("_notFound") is True:
                raw_identifier_truth[identifier] = None
            elif isinstance(payload, Mapping) and _text(payload.get("identifier")) == identifier:
                raw_identifier_truth[identifier] = normalize_order(payload)
            else:
                return False
        if raw_identifier_truth != terminal.get("identifierTruth"):
            return False
        fills: list[dict[str, str]] = []
        total_fees = Decimal("0")
        for listed in all_orders:
            if _text(listed.get("market")).upper() != "KRW-BTC":
                continue
            detail = details[_text(listed.get("uuid"))]
            for field in ("uuid", "identifier", "market", "side", "state"):
                if _text(detail.get(field)).lower() != _text(listed.get(field)).lower():
                    return False
            trades = detail.get("trades")
            if not isinstance(trades, list) or int(detail.get("trades_count") or -1) != len(trades):
                return False
            paid_fee = _raw_decimal(detail.get("paid_fee"))
            total_fees += paid_fee
            funds_total = sum((_raw_decimal(row.get("funds")) for row in trades), Decimal("0"))
            volume_total = sum((_raw_decimal(row.get("volume")) for row in trades), Decimal("0"))
            if volume_total != _raw_decimal(detail.get("executed_volume")):
                return False
            if detail.get("executed_funds") is not None and funds_total != _raw_decimal(detail.get("executed_funds")):
                return False
            if trades and funds_total <= 0:
                return False
            allocated = Decimal("0")
            raw_trade_ids: set[str] = set()
            for index, trade in enumerate(trades):
                if not isinstance(trade, Mapping):
                    return False
                volume = _raw_decimal(trade.get("volume"))
                funds = _raw_decimal(trade.get("funds"))
                price = _raw_decimal(trade.get("price"))
                if volume <= 0 or funds <= 0 or price * volume != funds:
                    return False
                trade_id = _text(trade.get("uuid") or trade.get("trade_uuid"))
                if not trade_id or trade_id in raw_trade_ids:
                    return False
                raw_trade_ids.add(trade_id)
                fee = paid_fee - allocated if index == len(trades) - 1 else paid_fee * funds / funds_total
                allocated += fee
                fills.append(
                    {
                        "market": _text(detail.get("market")).upper(),
                        "tradeUuid": trade_id,
                        "orderUuid": _text(detail.get("uuid")),
                        "identifier": _text(detail.get("identifier")),
                        "side": _text(detail.get("side")).upper(),
                        "volume": _decimal_text(volume),
                        "funds": _decimal_text(funds),
                        "fee": _decimal_text(fee),
                    }
                )
        terminal_fills_raw = terminal.get("fills")
        if not isinstance(terminal_fills_raw, list):
            return False
        terminal_fills = [
            {
                **dict(row),
                "volume": _decimal_text(_raw_decimal(row.get("volume"))),
                "funds": _decimal_text(_raw_decimal(row.get("funds"))),
                "fee": _decimal_text(_raw_decimal(row.get("fee"))),
            }
            for row in terminal_fills_raw
            if isinstance(row, Mapping)
        ]
        return (
            len(terminal_fills) == len(terminal_fills_raw)
            and fills == terminal_fills
            and total_fees == Decimal(str(terminal.get("totalFees")))
        )
    except (
        AttributeError,
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        UpbitFunctionalBlocked,
    ):
        return False


def _natural_claim_lifecycle_complete(
    claim: Mapping[str, Any] | sqlite3.Row,
    *,
    starts_at: datetime,
    ends_at: datetime,
    fill_occurred_at: datetime | None = None,
) -> bool:
    try:
        claimed_at = _utc(claim["claimed_at"], "natural-claim-time")
        post_boundary_at = _utc(
            claim["post_boundary_at"], "natural-post-boundary-time"
        )
        resolved_at = _utc(
            claim["resolved_at"], "natural-resolved-time"
        )
        broker_boundary_complete = bool(
            starts_at
            <= claimed_at
            <= post_boundary_at
            <= resolved_at
            < ends_at
        )
        if fill_occurred_at is None:
            return broker_boundary_complete
        occurred_at = _utc(fill_occurred_at, "natural-fill-time")
        return bool(
            broker_boundary_complete
            and post_boundary_at <= occurred_at < ends_at
        )
    except (KeyError, TypeError, ValueError, UpbitFunctionalBlocked):
        return False


def _natural_strategy_claims_complete(
    *,
    bars: list[sqlite3.Row],
    claims: list[sqlite3.Row],
    candidate: Mapping[str, Any],
    permit: Any,
    session_started_at: datetime,
    session_expires_at: datetime,
    terminal_cutoff: datetime,
) -> bool:
    """Independently replay the sealed MA(3/10) source for both claims."""

    try:
        selection = candidate.get("selection")
        if not isinstance(selection, Mapping):
            return False
        identity = {
            "strategyArtifactId": permit.binding.strategy_artifact_id,
            "strategyArtifactHash": permit.binding.strategy_artifact_hash,
            "strategyArtifactFileSha256": _text(
                selection.get("strategyArtifactFileSha256")
            ).lower(),
            "strategyInstanceId": permit.binding.strategy_instance_id,
            "strategyInstanceHash": _text(
                selection.get("strategyInstanceHash")
            ).lower(),
            "strategyInstanceFileSha256": _text(
                selection.get("strategyInstanceFileSha256")
            ).lower(),
            "publicationProofHash": _text(
                selection.get("publicationProofHash")
            ).lower(),
            "publicationProofFileSha256": _text(
                selection.get("publicationProofFileSha256")
            ).lower(),
        }
        if (
            any(
                _HASH_RE.fullmatch(value) is None
                for field, value in identity.items()
                if field not in {"strategyArtifactId", "strategyInstanceId"}
            )
            or any(
                not secrets.compare_digest(
                    _text(selection.get(field)).lower()
                    if field not in {"strategyArtifactId", "strategyInstanceId"}
                    else _text(selection.get(field)),
                    expected,
                )
                for field, expected in identity.items()
            )
            or _text(selection.get("strategyPluginId"))
            != "moving_average_cross"
            or int(selection.get("strategyShortMa") or 0) != 3
            or int(selection.get("strategyLongMa") or 0) != 10
        ):
            return False
        expected_evaluation_fields = {
            "schemaVersion",
            "symbol",
            "interval",
            "finalized",
            "closed",
            "source",
            "barId",
            "barHash",
            "closedAt",
            "signal",
            "evaluationId",
            "strategyEvaluationComplete",
            "naturalSignal",
            "forcedSignal",
            "signalOverrideUsed",
            "manualSignal",
            "strategyArtifactId",
            "strategyArtifactHash",
            "strategyArtifactFileSha256",
            "strategyInstanceId",
            "strategyInstanceHash",
            "strategyInstanceFileSha256",
            "publicationProofHash",
            "publicationProofFileSha256",
            "strategyPluginId",
            "strategyShortMa",
            "strategyLongMa",
            "rawFinalizedWindow",
        }
        expected_window_fields = {
            "schemaVersion",
            "symbol",
            "interval",
            "source",
            "finalized",
            "closed",
            "barId",
            "closedAt",
            "bars",
            "officialCandleEvidence",
        }
        expected_bar_fields = {
            "barId",
            "closedAt",
            "close",
            "finalized",
            "closed",
        }
        by_evaluation: dict[str, sqlite3.Row] = {}
        for durable in bars:
            evaluation = json.loads(durable["evaluation_json"])
            if (
                not isinstance(evaluation, Mapping)
                or set(evaluation) != expected_evaluation_fields
                or _text(evaluation.get("schemaVersion"))
                != "upbit-natural-ma-evaluation/v1"
                or _text(evaluation.get("symbol")).upper() != SYMBOL
                or _text(evaluation.get("interval")).lower() != "5m"
                or _text(evaluation.get("source")).upper()
                not in {"UPBIT_REST", "UPBIT_WEBSOCKET"}
                or evaluation.get("finalized") is not True
                or evaluation.get("closed") is not True
                or evaluation.get("strategyEvaluationComplete") is not True
                or evaluation.get("naturalSignal") is not True
                or evaluation.get("forcedSignal") is not False
                or evaluation.get("signalOverrideUsed") is not False
                or evaluation.get("manualSignal") is not False
                or _text(evaluation.get("strategyPluginId"))
                != "moving_average_cross"
                or int(evaluation.get("strategyShortMa") or 0) != 3
                or int(evaluation.get("strategyLongMa") or 0) != 10
                or any(
                    not secrets.compare_digest(
                        _text(evaluation.get(field)).lower()
                        if field not in {"strategyArtifactId", "strategyInstanceId"}
                        else _text(evaluation.get(field)),
                        expected,
                    )
                    for field, expected in identity.items()
                )
            ):
                return False
            window = evaluation.get("rawFinalizedWindow")
            rows = window.get("bars") if isinstance(window, Mapping) else None
            if (
                not isinstance(window, Mapping)
                or set(window) != expected_window_fields
                or _text(window.get("schemaVersion"))
                != "upbit-official-finalized-5m-window-v1"
                or _text(window.get("symbol")).upper() != SYMBOL
                or _text(window.get("interval")).lower() != "5m"
                or _text(window.get("source")).upper()
                != _text(evaluation.get("source")).upper()
                or window.get("finalized") is not True
                or window.get("closed") is not True
                or not isinstance(rows, list)
                or len(rows) != 11
                or any(not isinstance(row, Mapping) for row in rows)
            ):
                return False
            official = window.get("officialCandleEvidence")
            raw_response = (
                official.get("rawResponse")
                if isinstance(official, Mapping)
                else None
            )
            if (
                not isinstance(official, Mapping)
                or set(official)
                != {
                    "schemaVersion",
                    "origin",
                    "endpoint",
                    "orderedQuery",
                    "observedAt",
                    "maxResponseTimestampMs",
                    "rawResponse",
                    "rawResponseHash",
                }
                or _text(official.get("schemaVersion"))
                != "upbit-official-candle-rest-evidence/v1"
                or _text(official.get("origin")) != "https://api.upbit.com"
                or _text(official.get("endpoint"))
                != "/v1/candles/minutes/5"
                or official.get("orderedQuery")
                != [["market", SYMBOL], ["count", "20"]]
                or not isinstance(raw_response, list)
                or not 11 <= len(raw_response) <= 20
                or any(not isinstance(row, Mapping) for row in raw_response)
                or _text(official.get("rawResponseHash")).lower()
                != _stable_hash(raw_response)
            ):
                return False
            official_observed_at = _utc(
                official.get("observedAt"), "natural-official-observed-at"
            )
            raw_by_start: dict[datetime, dict[str, Any]] = {}
            response_timestamps: list[int] = []
            for raw_row in raw_response:
                if _text(raw_row.get("market")).upper() != SYMBOL:
                    return False
                opened_at = datetime.fromisoformat(
                    _text(raw_row.get("candle_date_time_utc"))
                ).replace(tzinfo=timezone.utc)
                response_timestamp = int(raw_row.get("timestamp"))
                trade_price = Decimal(str(raw_row.get("trade_price")))
                closed_at = opened_at + timedelta(minutes=5)
                if (
                    opened_at in raw_by_start
                    or opened_at.second != 0
                    or opened_at.microsecond != 0
                    or opened_at.minute % 5 != 0
                    or not trade_price.is_finite()
                    or trade_price <= 0
                    or response_timestamp <= 0
                    or datetime.fromtimestamp(
                        response_timestamp / 1000, tz=timezone.utc
                    )
                    > official_observed_at + timedelta(seconds=15)
                ):
                    return False
                response_timestamps.append(response_timestamp)
                if closed_at <= official_observed_at:
                    raw_by_start[opened_at] = {
                        "barId": "upbit-rest-five-minute-"
                        + opened_at.strftime("%Y%m%dT%H%M%SZ"),
                        "closedAt": closed_at.isoformat(
                            timespec="microseconds"
                        ).replace(
                            "+00:00", "Z"
                        ),
                        "close": _decimal_text(trade_price),
                        "finalized": True,
                        "closed": True,
                    }
            independent_rows = [
                raw_by_start[key] for key in sorted(raw_by_start)
            ][-11:]
            if (
                len(independent_rows) != 11
                or independent_rows != [dict(row) for row in rows]
                or int(official.get("maxResponseTimestampMs") or 0)
                != max(response_timestamps)
            ):
                return False
            parsed: list[tuple[str, datetime, Decimal]] = []
            for raw_bar in rows:
                if (
                    set(raw_bar) != expected_bar_fields
                    or raw_bar.get("finalized") is not True
                    or raw_bar.get("closed") is not True
                ):
                    return False
                bar_id = _text(raw_bar.get("barId"))
                closed_at = _utc(
                    raw_bar.get("closedAt"), "natural-bar-closed-at"
                )
                close = Decimal(str(raw_bar.get("close")))
                if (
                    _SAFE_ID_RE.fullmatch(bar_id) is None
                    or not close.is_finite()
                    or close <= 0
                    or closed_at.second != 0
                    or closed_at.microsecond != 0
                    or closed_at.minute % 5 != 0
                ):
                    return False
                parsed.append((bar_id, closed_at, close))
            if any(
                current[1] - previous[1] != timedelta(minutes=5)
                for previous, current in zip(parsed, parsed[1:])
            ):
                return False
            final_id, final_closed_at, _ = parsed[-1]
            window_hash = _stable_hash(window)
            closes = [entry[2] for entry in parsed]
            previous_short = sum(closes[-4:-1], Decimal("0")) / Decimal("3")
            previous_long = sum(closes[-11:-1], Decimal("0")) / Decimal("10")
            current_short = sum(closes[-3:], Decimal("0")) / Decimal("3")
            current_long = sum(closes[-10:], Decimal("0")) / Decimal("10")
            derived = (
                "BUY"
                if previous_short <= previous_long
                and current_short > current_long
                else "SELL"
                if previous_short >= previous_long
                and current_short < current_long
                else "HOLD"
            )
            expected_evaluation_id = "upbit-ma-eval-" + _stable_hash(
                {
                    "windowHash": window_hash,
                    "strategyArtifactHash": identity["strategyArtifactHash"],
                    "strategyInstanceHash": identity["strategyInstanceHash"],
                }
            )[:32]
            evaluation_id = _text(evaluation.get("evaluationId"))
            if (
                _text(window.get("barId")) != final_id
                or _utc(
                    window.get("closedAt"), "natural-window-closed-at"
                )
                != final_closed_at
                or _text(evaluation.get("barId")) != final_id
                or _utc(
                    evaluation.get("closedAt"), "natural-evaluation-closed-at"
                )
                != final_closed_at
                or _text(evaluation.get("barHash")).lower() != window_hash
                or _text(evaluation.get("signal")).upper() != derived
                or evaluation_id != expected_evaluation_id
                or final_closed_at < session_started_at
                or final_closed_at >= session_expires_at
                or official_observed_at < session_started_at
                or official_observed_at >= session_expires_at
                or final_closed_at > official_observed_at
                or official_observed_at - final_closed_at
                > timedelta(minutes=10)
                or durable["bar_id"] != final_id
                or durable["closed_at"]
                != final_closed_at.isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z")
                or durable["bar_hash"] != window_hash
                or durable["signal"] != derived
                or durable["evaluation_id"] != evaluation_id
                or durable["evaluation_hash"] != _stable_hash(evaluation)
                or not secrets.compare_digest(
                    durable["evaluation_hash"],
                    _stable_hash(json.loads(durable["evaluation_json"])),
                )
                or evaluation_id in by_evaluation
            ):
                return False
            by_evaluation[evaluation_id] = durable
        if not bars:
            return False
        strategy_claims = {
            row["slot"]: row
            for row in claims
            if row["slot"] in {"STRATEGY_BUY", "STRATEGY_SELL"}
        }
        if set(strategy_claims) != {"STRATEGY_BUY", "STRATEGY_SELL"}:
            return False
        used_evaluations: set[str] = set()
        for slot, expected_signal, expected_side in (
            ("STRATEGY_BUY", "BUY", "BID"),
            ("STRATEGY_SELL", "SELL", "ASK"),
        ):
            claim = strategy_claims[slot]
            evaluation_id = _text(claim["evaluation_id"])
            durable = by_evaluation.get(evaluation_id)
            if (
                durable is None
                or evaluation_id in used_evaluations
                or durable["signal"] != expected_signal
                or claim["side"] != expected_side
                or claim["bar_id"] != durable["bar_id"]
                or claim["evaluation_closed_at"] != durable["closed_at"]
                or claim["evaluation_hash"] != durable["evaluation_hash"]
                or not _natural_claim_lifecycle_complete(
                    claim,
                    starts_at=session_started_at,
                    ends_at=session_expires_at,
                )
            ):
                return False
            used_evaluations.add(evaluation_id)
        return True
    except (
        AttributeError,
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        UpbitFunctionalBlocked,
    ):
        return False


def _durable_functional_wiring_complete(
    connection: sqlite3.Connection,
    *,
    approval: sqlite3.Row,
    session_id: str,
    evidence: Mapping[str, Any],
    trusted_now: datetime | None = None,
    current_selection: Mapping[str, Any] | None = None,
    account_exclusivity_verifier: (
        AccountExclusivityProofVerifier | None
    ) = None,
    account_exclusivity_verifier_pin: Mapping[str, Any] | None = None,
) -> bool:
    """Join immutable claims, official terminal rows and private journal."""

    if not _functional_wiring_evidence_complete(
        evidence,
        account_exclusivity_verifier=account_exclusivity_verifier,
        account_exclusivity_verifier_pin=(
            account_exclusivity_verifier_pin
        ),
    ):
        return False
    try:
        terminal_row = connection.execute(
            """SELECT truth_json,truth_hash FROM
            upbit_functional_terminal_truth WHERE session_id=?""",
            (session_id,),
        ).fetchone()
        terminal_raw_row = connection.execute(
            """SELECT * FROM upbit_functional_terminal_raw_truth
            WHERE session_id=?""",
            (session_id,),
        ).fetchone()
        session = connection.execute(
            "SELECT * FROM upbit_functional_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        claims = connection.execute(
            """SELECT * FROM upbit_functional_claims WHERE session_id=?
            ORDER BY rowid""",
            (session_id,),
        ).fetchall()
        bars = connection.execute(
            """SELECT * FROM upbit_functional_bars WHERE session_id=?
            ORDER BY closed_at,bar_id""",
            (session_id,),
        ).fetchall()
        journal = connection.execute(
            "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        journal_events = connection.execute(
            """SELECT event_id,occurred_at,order_uuid,trade_uuid,identifier,
            market,raw_hash,payload FROM upbit_myorder_events
            WHERE session_id=? ORDER BY occurred_at,event_id""",
            (session_id,),
        ).fetchall()
        if (
            terminal_row is None
            or terminal_raw_row is None
            or session is None
            or journal is None
        ):
            return False
        terminal_truth = json.loads(terminal_row["truth_json"])
        terminal_hash = _exact_lower_hash(terminal_row["truth_hash"])
        evidence_terminal_hash = _exact_lower_hash(
            evidence.get("terminalOfficialTruthHash")
        )
        if (
            not isinstance(terminal_truth, Mapping)
            or terminal_hash is None
            or evidence_terminal_hash is None
            or not secrets.compare_digest(
                terminal_hash, _stable_hash(terminal_truth)
            )
            or not secrets.compare_digest(
                terminal_hash,
                evidence_terminal_hash,
            )
            or terminal_truth.get("sessionId") != session_id
            or terminal_truth.get("accountFingerprint")
            != approval["account_fingerprint"]
            or terminal_truth.get("observedAt")
            != evidence.get("finalObservedAt")
        ):
            return False
        raw_rest = json.loads(terminal_raw_row["raw_json"])
        raw_rest_hash = _exact_lower_hash(terminal_raw_row["raw_hash"])
        evidence_raw_rest_hash = _exact_lower_hash(
            evidence.get("officialRestRawSnapshotHash")
        )
        truth_raw_rest_hash = _exact_lower_hash(
            terminal_truth.get("officialRestRawSnapshotHash")
        )
        if (
            not isinstance(raw_rest, Mapping)
            or raw_rest_hash is None
            or evidence_raw_rest_hash is None
            or truth_raw_rest_hash is None
            or not secrets.compare_digest(raw_rest_hash, _stable_hash(raw_rest))
            or not secrets.compare_digest(
                raw_rest_hash,
                evidence_raw_rest_hash,
            )
            or not secrets.compare_digest(
                raw_rest_hash,
                truth_raw_rest_hash,
            )
            or terminal_raw_row["account_fingerprint"]
            != approval["account_fingerprint"]
            or terminal_raw_row["permit_hash"] != session["permit_hash"]
            or terminal_raw_row["route_scope_hash"] != session["scope_hash"]
            or _utc(terminal_raw_row["cutoff"], "raw-row-cutoff")
            != _utc(
                terminal_truth.get("observationStartedAt"),
                "terminal-observation-start",
            )
            or not _official_rest_raw_matches_terminal(
                raw_rest, terminal_truth
            )
        ):
            return False
        permit = parse_functional_test_permit(json.loads(approval["permit_json"]))
        candidate = json.loads(approval["candidate_json"])
        if (
            not isinstance(candidate, Mapping)
            or not isinstance(current_selection, Mapping)
            or not isinstance(candidate.get("selection"), Mapping)
            or any(
                candidate["selection"].get(field)
                != current_selection.get(field)
                for field in candidate["selection"]
            )
            or current_selection.get("verified") is not True
            or current_selection.get("publicationProofVerified") is not True
        ):
            return False
        durable_starts_at = _utc(
            session["starts_at"], "durable-session-start"
        )
        durable_expires_at = _utc(
            session["expires_at"], "durable-session-expiry"
        )
        evidence_activated_at = _utc(
            evidence.get("activatedAt"), "evidence-activation"
        )
        evidence_permit_ends_at = _utc(
            evidence.get("permitEndsAt"), "evidence-permit-expiry"
        )
        evidence_final_at = _utc(
            evidence.get("finalObservedAt"), "evidence-final-observed"
        )
        terminal_started_at = _utc(
            terminal_truth.get("sessionStartedAt"),
            "terminal-session-start",
        )
        terminal_observation_started = _utc(
            terminal_truth.get("observationStartedAt"),
            "terminal-observation-start",
        )
        terminal_observed_at = _utc(
            terminal_truth.get("observedAt"),
            "terminal-observed-at",
        )
        raw_started_at = _utc(
            raw_rest.get("sessionStartedAt"), "raw-session-start"
        )
        raw_cutoff = _utc(
            raw_rest.get("observationCutoff"), "raw-observation-cutoff"
        )
        if (
            permit.permit_id != session["permit_id"]
            or permit.content_hash != session["permit_hash"]
            or Decimal(str(permit.caps.max_order_notional))
            != Decimal("10000")
            or Decimal(str(permit.caps.max_gross_exposure))
            != Decimal("10000")
            or Decimal(str(permit.caps.max_loss)) != Decimal("1000")
            or session["state"] != "FINALIZED"
            or session["capability_hash"]
            or int(session["new_entries_blocked"]) != 1
            or int(session["real_orders_enabled"]) != 0
            or permit.starts_at != durable_starts_at
            or permit.ends_at != durable_expires_at
            or evidence_activated_at != durable_starts_at
            or evidence_permit_ends_at != durable_expires_at
            or terminal_started_at != durable_starts_at
            or raw_started_at != durable_starts_at
            or raw_cutoff != terminal_observation_started
            or raw_cutoff < durable_expires_at
            or terminal_observed_at != evidence_final_at
            or terminal_observed_at < raw_cutoff
            or terminal_observed_at - raw_cutoff > timedelta(seconds=15)
            or Decimal(str(evidence.get("actualDurationSeconds")))
            != Decimal(
                str((terminal_observed_at - durable_starts_at).total_seconds())
            )
            or (
                trusted_now is not None
                and (
                    _utc(trusted_now, "trusted-current-time")
                    < durable_expires_at
                    or terminal_observed_at
                    > _utc(trusted_now, "trusted-current-time")
                    + timedelta(seconds=15)
                )
            )
        ):
            return False
        if not _natural_strategy_claims_complete(
            bars=list(bars),
            claims=list(claims),
            candidate=candidate,
            permit=permit,
            session_started_at=durable_starts_at,
            session_expires_at=durable_expires_at,
            terminal_cutoff=raw_cutoff,
        ):
            return False
        if len(claims) != 2:
            return False
        by_slot = {row["slot"]: row for row in claims}
        buy_claim = by_slot.get("STRATEGY_BUY")
        sell_claim = by_slot.get("STRATEGY_SELL")
        if (
            buy_claim is None
            or sell_claim is None
            or len(by_slot) != 2
            or buy_claim["side"] != "BID"
            or sell_claim["side"] != "ASK"
            or any(row["state"] != "RECONCILED" for row in claims)
            or any(not row["broker_order_id"] for row in claims)
            or any(_HASH_RE.fullmatch(row["request_hash"]) is None for row in claims)
        ):
            return False
        open_orders = terminal_truth.get("openOrders")
        closed_orders = terminal_truth.get("closedOrders")
        fills = terminal_truth.get("fills")
        account_rows = terminal_truth.get("accountRows")
        account_exclusivity_proof = terminal_truth.get(
            "accountExclusivityProof"
        )
        terminal_proof_hash = _exact_lower_hash(
            terminal_truth.get("accountExclusivityProofHash")
        )
        evidence_proof_hash = _exact_lower_hash(
            evidence.get("accountExclusivityProofHash")
        )
        if (
            open_orders != []
            or not isinstance(closed_orders, list)
            or not isinstance(fills, list)
            or not isinstance(account_rows, list)
            or terminal_truth.get("accountExternalActivityAbsent") is not True
            or not isinstance(account_exclusivity_proof, Mapping)
            or dict(account_exclusivity_proof)
            != evidence.get("accountExclusivityProof")
            or terminal_truth.get("accountExclusivityProofVerified")
            is not True
            or terminal_truth.get("accountExclusivityAuthorityPinned")
            is not True
            or terminal_truth.get("otherApiKeysAbsent") is not True
            or terminal_truth.get("manualTradingAbsent") is not True
            or terminal_truth.get("otherBotsAbsent") is not True
            or terminal_proof_hash is None
            or evidence_proof_hash is None
            or not secrets.compare_digest(
                terminal_proof_hash,
                evidence_proof_hash,
            )
            or not secrets.compare_digest(
                terminal_proof_hash,
                _stable_hash(account_exclusivity_proof),
            )
            or _stable_hash(account_rows)
            != terminal_truth.get("accountRowsHash")
        ):
            return False
        closed_by_identifier = {
            _text(row.get("identifier")): row
            for row in closed_orders
            if isinstance(row, Mapping) and _text(row.get("identifier"))
        }
        owned_identifiers = {
            buy_claim["identifier"],
            sell_claim["identifier"],
        }
        if (
            set(closed_by_identifier) != owned_identifiers
            or len(closed_orders) != len(owned_identifiers)
            or any(
                _text(row.get("identifier")) not in owned_identifiers
                for row in closed_orders
                if isinstance(row, Mapping)
            )
        ):
            return False
        for claim, expected_side in ((buy_claim, "BID"), (sell_claim, "ASK")):
            order = closed_by_identifier.get(claim["identifier"])
            if (
                not isinstance(order, Mapping)
                or _text(order.get("state")).lower() != "done"
                or _text(order.get("uuid")) != claim["broker_order_id"]
                or _text(order.get("side")).upper() != expected_side
            ):
                return False
        normalized_fills: list[dict[str, Any]] = []
        seen_trade_ids: set[str] = set()
        for fill in fills:
            if not isinstance(fill, Mapping):
                return False
            row = dict(fill)
            identifier = _text(row.get("identifier"))
            trade_id = _text(row.get("tradeUuid"))
            order_id = _text(row.get("orderUuid"))
            if (
                identifier not in owned_identifiers
                or not trade_id
                or trade_id in seen_trade_ids
                or order_id
                != closed_by_identifier[identifier].get("uuid")
            ):
                return False
            row["volume"] = Decimal(str(row.get("volume")))
            row["funds"] = Decimal(str(row.get("funds")))
            row["fee"] = Decimal(str(row.get("fee")))
            if row["volume"] <= 0 or row["funds"] <= 0 or row["fee"] < 0:
                return False
            seen_trade_ids.add(trade_id)
            normalized_fills.append(row)
        buy_fills = [
            row
            for row in normalized_fills
            if row["identifier"] == buy_claim["identifier"]
            and _text(row.get("side")).upper() == "BID"
        ]
        sell_fills = [
            row
            for row in normalized_fills
            if row["identifier"] == sell_claim["identifier"]
            and _text(row.get("side")).upper() == "ASK"
        ]
        if not buy_fills or not sell_fills or len(buy_fills) + len(sell_fills) != len(normalized_fills):
            return False
        buy_volume = sum((row["volume"] for row in buy_fills), Decimal("0"))
        sell_volume = sum((row["volume"] for row in sell_fills), Decimal("0"))
        buy_funds = sum((row["funds"] for row in buy_fills), Decimal("0"))
        sell_funds = sum((row["funds"] for row in sell_fills), Decimal("0"))
        fees = sum((row["fee"] for row in normalized_fills), Decimal("0"))
        residual = buy_volume - sell_volume
        # Match the runtime risk formula exactly.  At an exact-flat terminal
        # this is realized loss clamped at zero; exchange-precision residual
        # is valued from the independently sealed terminal ticker.
        owner_loss = max(
            Decimal("0"),
            buy_funds
            + sum((row["fee"] for row in normalized_fills), Decimal("0"))
            - sell_funds
            - residual * Decimal(str(terminal_truth.get("markPrice"))),
        )
        if (
            residual < 0
            or residual != Decimal(str(evidence.get("residualQuantity")))
            or buy_funds != Decimal(
                str(evidence.get("strategyBuyExecutedNotional"))
            )
            or fees != Decimal(str(terminal_truth.get("totalFees")))
            or fees != Decimal(str(evidence.get("fees")))
            or owner_loss != Decimal(str(session["owner_loss"]))
            or owner_loss != Decimal(str(evidence.get("ownerLoss")))
            or Decimal(str(session["max_owner_gross"]))
            != Decimal(str(evidence.get("maxObservedOwnerGrossExposure")))
            or Decimal(str(session["max_owner_gross"])) > Decimal("10000")
            or owner_loss >= Decimal("1000")
            or Decimal(str(terminal_truth.get("baseTotal")))
            != Decimal(str(session["baseline_base"])) + residual
            or Decimal(str(terminal_truth.get("quoteAvailable")))
            != Decimal(str(session["baseline_quote"]))
            - buy_funds
            - sum((row["fee"] for row in buy_fills), Decimal("0"))
            + sell_funds
            - sum((row["fee"] for row in sell_fills), Decimal("0"))
        ):
            return False
        seal = terminal_truth.get("terminalPrivateStreamSeal")
        if not isinstance(seal, Mapping):
            return False
        journal_seal = json.loads(journal["terminal_seal_json"])
        journal_seal_hash = _exact_lower_hash(journal["terminal_seal_hash"])
        terminal_seal_hash = _exact_lower_hash(seal.get("sealHash"))
        if (
            int(journal["completed"]) != 1
            or int(journal["gap_detected"]) != 0
            or int(journal["cleanup_recovery"]) != 0
            or seal != journal_seal
            or seal != evidence.get("terminalPrivateStreamSeal")
            or journal_seal_hash is None
            or terminal_seal_hash is None
            or not secrets.compare_digest(
                journal_seal_hash,
                terminal_seal_hash,
            )
        ):
            return False
        event_chain = [
            {
                "eventId": _text(row["event_id"]),
                "occurredAt": _text(row["occurred_at"]),
                "identifier": _text(row["identifier"]),
                "market": _text(row["market"]).upper(),
                "rawHash": row["raw_hash"],
            }
            for row in journal_events
        ]
        journal_trade_ids = {
            _text(row["trade_uuid"])
            for row in journal_events
            if _text(row["trade_uuid"])
        }
        fills_by_trade = {
            _text(row["tradeUuid"]): row for row in normalized_fills
        }
        claim_by_identifier = {
            _text(row["identifier"]): row for row in (buy_claim, sell_claim)
        }
        from .upbit_functional_transport import (
            normalize_upbit_myorder_event,
        )

        for journal_event in journal_events:
            stored = json.loads(journal_event["payload"])
            if (
                not isinstance(stored, Mapping)
                or stored.get("schemaVersion")
                != "upbit-functional-myorder-raw-envelope/v1"
                or not isinstance(stored.get("rawPayload"), Mapping)
                or not isinstance(stored.get("normalized"), Mapping)
            ):
                return False
            raw_payload = stored["rawPayload"]
            normalized = stored["normalized"]
            replayed = normalize_upbit_myorder_event(raw_payload)
            journal_event_hash = _exact_lower_hash(journal_event["raw_hash"])
            if (
                journal_event_hash is None
                or
                not secrets.compare_digest(
                    _stable_hash(raw_payload),
                    journal_event_hash,
                )
                or _text(raw_payload.get("uuid"))
                != _text(journal_event["order_uuid"])
                or _text(
                    raw_payload.get("trade_uuid")
                    or raw_payload.get("tradeUuid")
                )
                != _text(journal_event["trade_uuid"])
                or _text(raw_payload.get("identifier"))
                != _text(journal_event["identifier"])
                or _text(
                    raw_payload.get("code") or raw_payload.get("market")
                ).upper()
                != _text(journal_event["market"]).upper()
                or _text(normalized.get("eventId"))
                != _text(journal_event["event_id"])
                or _text(normalized.get("occurredAt"))
                != _text(journal_event["occurred_at"])
                or _text(normalized.get("identifier"))
                not in owned_identifiers
                or _text(normalized.get("market")).upper() != "KRW-BTC"
                or _text(normalized.get("side")).upper()
                not in {"BID", "ASK"}
                or _text(normalized.get("state")).lower()
                not in {
                    "wait",
                    "watch",
                    "trade",
                    "done",
                    "cancel",
                    "reject",
                }
                or dict(normalized) != replayed
                or _utc(
                    replayed.get("occurredAt"),
                    "journal-event-occurred-at",
                )
                < durable_starts_at
                or _utc(
                    replayed.get("occurredAt"),
                    "journal-event-occurred-at",
                )
                > raw_cutoff
            ):
                return False
            trade_id = _text(journal_event["trade_uuid"])
            if not trade_id:
                continue
            fill = fills_by_trade.get(trade_id)
            fill_claim = claim_by_identifier.get(
                _text(journal_event["identifier"])
            )
            occurred_at = _utc(
                replayed.get("occurredAt"), "natural-fill-occurred-at"
            )
            if (
                fill is None
                or fill_claim is None
                or _text(raw_payload.get("state")).lower() != "trade"
                or not _natural_claim_lifecycle_complete(
                    fill_claim,
                    starts_at=durable_starts_at,
                    ends_at=durable_expires_at,
                    fill_occurred_at=occurred_at,
                )
            ):
                return False
            volume = _raw_decimal(
                raw_payload.get("trade_volume")
                or raw_payload.get("volume")
            )
            price = _raw_decimal(
                raw_payload.get("trade_price")
                or raw_payload.get("price")
            )
            raw_fee = (
                raw_payload.get("trade_fee")
                if raw_payload.get("trade_fee") is not None
                else raw_payload.get("paid_fee")
            )
            fee = _raw_decimal(raw_fee) if raw_fee is not None else None
            if (
                volume != fill["volume"]
                or volume * price != fill["funds"]
                or (fee is not None and fee != fill["fee"])
                or _text(
                    raw_payload.get("ask_bid") or raw_payload.get("side")
                ).upper()
                != _text(fill.get("side")).upper()
            ):
                return False
        return bool(
            int(seal.get("eventCursor") or -1) == len(event_chain)
            and seal.get("eventHeadHash") == _stable_hash(event_chain)
            and seen_trade_ids == journal_trade_ids
            and set(seal.get("ownedIdentifiers") or ()) == owned_identifiers
        )
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        sqlite3.Error,
        UpbitFunctionalBlocked,
    ):
        return False
def _permit_immutable_lineage(permit: Any) -> dict[str, Any]:
    """Return the operator-approved portion that activation may not change."""

    return {
        "schemaVersion": "upbit-functional-activation-lineage/v1",
        "environment": permit.environment.value,
        "binding": permit.binding.snapshot(),
        "caps": permit.caps.snapshot(),
        "duration": {
            "value": permit.duration_value,
            "unit": permit.duration_unit.value,
        },
        "promotionEligible": False,
    }


def _candidate_binding(
    permit: Any,
    record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    supplied = (record or {}).get("candidateBinding")
    if supplied is None:
        return {
            "schemaVersion": "upbit-functional-server-candidate-binding/v1",
            "immutablePermit": _permit_immutable_lineage(permit),
        }
    if not isinstance(supplied, Mapping):
        raise UpbitFunctionalBlocked(
            "upbit-functional-permit-candidate-binding-invalid"
        )
    candidate = dict(supplied)
    if candidate.get("immutablePermit") != _permit_immutable_lineage(permit):
        raise UpbitFunctionalBlocked(
            "upbit-functional-permit-candidate-lineage-mismatch"
        )
    return candidate


class DurableUpbitFunctionalApprovalStore:
    """CAS state machine for permits and owner-loss recovery approvals.

    Approval JSON is accepted only after a backend-supplied authenticated
    verifier returns true.  A client can later name only the server record id;
    raw permits and attestation fields never cross the start/recovery command
    boundary.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime],
        operator_verifier: Callable[[Mapping[str, Any]], bool],
        code_manifest_reader: Callable[[], Mapping[str, Any]] | None = None,
        immutable_selection_reader: Callable[[], Mapping[str, Any]] | None = None,
        account_exclusivity_verifier: (
            AccountExclusivityProofVerifier | None
        ) = None,
        account_exclusivity_verifier_pin: Mapping[str, Any] | None = None,
        durable_owner_lease_required: bool = False,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.operator_verifier = operator_verifier
        self.code_manifest_reader = code_manifest_reader
        self.immutable_selection_reader = immutable_selection_reader
        self.account_exclusivity_verifier = account_exclusivity_verifier
        self.account_exclusivity_verifier_pin = (
            dict(account_exclusivity_verifier_pin)
            if isinstance(account_exclusivity_verifier_pin, Mapping)
            else None
        )
        self.durable_owner_lease_required = bool(
            durable_owner_lease_required
        )
        self._lock = threading.RLock()
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS upbit_functional_approvals (
                    approval_id TEXT PRIMARY KEY,
                    permit_id TEXT NOT NULL UNIQUE,
                    permit_hash TEXT NOT NULL UNIQUE,
                    permit_json TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    state TEXT NOT NULL,
                    claimed_session_id TEXT NOT NULL DEFAULT '',
                    approval_hash TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_e2e_bootstrap (
                    bootstrap_id TEXT PRIMARY KEY,
                    approval_id TEXT NOT NULL UNIQUE,
                    candidate_hash TEXT NOT NULL UNIQUE,
                    candidate_json TEXT NOT NULL,
                    session_nonce TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    claimed_session_id TEXT NOT NULL DEFAULT '',
                    operator_approval_hash TEXT NOT NULL DEFAULT '',
                    terminal_evidence_hash TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_e2e_state (
                    state_key TEXT PRIMARY KEY,
                    validated INTEGER NOT NULL DEFAULT 0,
                    evidence_hash TEXT NOT NULL DEFAULT '',
                    session_id TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_owner_lease (
                    lease_id TEXT PRIMARY KEY,
                    approval_id TEXT NOT NULL UNIQUE,
                    candidate_hash TEXT NOT NULL,
                    session_id TEXT NOT NULL DEFAULT '',
                    owner_id_hash TEXT NOT NULL,
                    owner_token_hash TEXT NOT NULL,
                    process_identity_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    cleanup_only INTEGER NOT NULL DEFAULT 0,
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    record_hash TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_recovery_approvals (
                    recovery_id TEXT PRIMARY KEY,
                    recovery_hash TEXT NOT NULL UNIQUE,
                    recovery_json TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                """
            )
            approval_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(upbit_functional_approvals)"
                )
            }
            for name, definition in (
                ("candidate_permit_id", "TEXT NOT NULL DEFAULT ''"),
                ("candidate_permit_hash", "TEXT NOT NULL DEFAULT ''"),
                ("candidate_hash", "TEXT NOT NULL DEFAULT ''"),
                ("candidate_json", "TEXT NOT NULL DEFAULT ''"),
                ("activated_at", "TEXT NOT NULL DEFAULT ''"),
                ("activation_lineage_hash", "TEXT NOT NULL DEFAULT ''"),
                ("bootstrap_id", "TEXT NOT NULL DEFAULT ''"),
                ("cleanup_only", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if name not in approval_columns:
                    connection.execute(
                        f"ALTER TABLE upbit_functional_approvals "
                        f"ADD COLUMN {name} {definition}"
                    )
            connection.execute(
                """INSERT OR IGNORE INTO upbit_functional_e2e_state
                (state_key,validated,evidence_hash,session_id,updated_at)
                VALUES ('REAL_E2E',0,'','',?)""",
                (_utc(self.clock(), "current-time").isoformat(),),
            )
            connection.commit()

    def _assert_current_code_manifest(
        self, candidate: Mapping[str, Any]
    ) -> None:
        """Reject approval/claim/finalization after any sealed code drift."""

        if self.code_manifest_reader is None:
            return
        sealed = candidate.get("codeManifest")
        current = self.code_manifest_reader()
        if (
            not isinstance(sealed, Mapping)
            or not isinstance(current, Mapping)
            or dict(sealed) != dict(current)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-code-manifest-drift"
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    @staticmethod
    def _owner_lease_public(row: Mapping[str, Any]) -> dict[str, Any]:
        """Return restart-verifiable lease identity without bearer material."""

        return {
            "schemaVersion": UPBIT_FUNCTIONAL_OWNER_LEASE_SCHEMA_VERSION,
            "leaseId": _text(row.get("lease_id")),
            "approvalId": _text(row.get("approval_id")),
            "candidateHash": _text(row.get("candidate_hash")).lower(),
            "sessionId": _text(row.get("session_id")),
            "ownerIdHash": _text(row.get("owner_id_hash")).lower(),
            "processIdentityHash": _text(
                row.get("process_identity_hash")
            ).lower(),
            "state": _text(row.get("state")).upper(),
            "cleanupOnly": bool(int(row.get("cleanup_only") or 0)),
            "acquiredAt": _text(row.get("acquired_at")),
            "heartbeatAt": _text(row.get("heartbeat_at")),
            "expiresAt": _text(row.get("expires_at")),
            "revision": int(row.get("revision") or 0),
            "detail": _text(row.get("detail")),
            "recordHash": _text(row.get("record_hash")).lower(),
            "updatedAt": _text(row.get("updated_at")),
        }

    @classmethod
    def _owner_lease_row_verified(cls, row: Mapping[str, Any]) -> bool:
        public = cls._owner_lease_public(row)
        projection = {
            key: item for key, item in public.items() if key != "recordHash"
        }
        return bool(
            _HASH_RE.fullmatch(public["recordHash"])
            and secrets.compare_digest(
                public["recordHash"], _stable_hash(projection)
            )
        )

    def acquire_owner_lease(
        self,
        *,
        approval_id: str,
        owner_id: str,
        owner_token: str,
        process_identity_hash: str,
        cleanup_only: bool = False,
    ) -> dict[str, Any]:
        """Acquire one durable owner before the permit claim can cross.

        Only a hash of the bearer token is persisted.  An ordinary first-live
        approval gets exactly one owner generation; cleanup recovery may rotate
        only a lease already durably marked ``LOST``.
        """

        normalized_approval = _text(approval_id)
        normalized_owner = _text(owner_id)
        normalized_token = _text(owner_token)
        normalized_process = _text(process_identity_hash).lower()
        if (
            _SAFE_ID_RE.fullmatch(normalized_approval) is None
            or _SAFE_ID_RE.fullmatch(normalized_owner) is None
            or len(normalized_token) < 32
            or _HASH_RE.fullmatch(normalized_process) is None
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-owner-lease-input-invalid"
            )
        now = _utc(self.clock(), "current-time")
        expires = now + timedelta(
            seconds=UPBIT_FUNCTIONAL_OWNER_LEASE_SECONDS
        )
        lease_id = "upbit-owner-lease-" + secrets.token_hex(16)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            approval = connection.execute(
                """SELECT approval_id,candidate_hash,state,claimed_session_id
                FROM upbit_functional_approvals WHERE approval_id=?""",
                (normalized_approval,),
            ).fetchone()
            existing = connection.execute(
                """SELECT * FROM upbit_functional_owner_lease
                WHERE approval_id=?""",
                (normalized_approval,),
            ).fetchone()
            if approval is None:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-owner-lease-approval-missing"
                )
            candidate_hash = _text(approval["candidate_hash"]).lower()
            if _HASH_RE.fullmatch(candidate_hash) is None:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-owner-lease-candidate-hash-invalid"
                )
            if cleanup_only:
                if (
                    approval["state"] != "ACTIVE"
                    or not _text(approval["claimed_session_id"])
                    or existing is None
                    or _text(existing["state"]).upper() != "LOST"
                    or not self._owner_lease_row_verified(dict(existing))
                ):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-cleanup-owner-rotation-invalid"
                    )
                session_id = _text(approval["claimed_session_id"])
                revision = int(existing["revision"] or 0) + 1
            else:
                if approval["state"] != "APPROVED" or existing is not None:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-owner-lease-not-acquirable"
                    )
                session_id = ""
                revision = 1
            record = {
                "schemaVersion": UPBIT_FUNCTIONAL_OWNER_LEASE_SCHEMA_VERSION,
                "leaseId": lease_id,
                "approvalId": normalized_approval,
                "candidateHash": candidate_hash,
                "sessionId": session_id,
                "ownerIdHash": _secret_hash(normalized_owner),
                "processIdentityHash": normalized_process,
                "state": "ACTIVE" if cleanup_only else "ACQUIRED",
                "cleanupOnly": bool(cleanup_only),
                "acquiredAt": now.isoformat(),
                "heartbeatAt": now.isoformat(),
                "expiresAt": expires.isoformat(),
                "revision": revision,
                "detail": (
                    "cleanup owner rotated after proven process loss"
                    if cleanup_only
                    else "owner acquired before permit claim"
                ),
                "updatedAt": now.isoformat(),
            }
            record_hash = _stable_hash(record)
            values = (
                lease_id,
                normalized_approval,
                candidate_hash,
                session_id,
                record["ownerIdHash"],
                _secret_hash(normalized_token),
                normalized_process,
                record["state"],
                1 if cleanup_only else 0,
                record["acquiredAt"],
                record["heartbeatAt"],
                record["expiresAt"],
                revision,
                record["detail"],
                record_hash,
                record["updatedAt"],
            )
            if cleanup_only:
                cursor = connection.execute(
                    """UPDATE upbit_functional_owner_lease SET
                    lease_id=?,candidate_hash=?,session_id=?,owner_id_hash=?,
                    owner_token_hash=?,process_identity_hash=?,state=?,
                    cleanup_only=?,acquired_at=?,heartbeat_at=?,expires_at=?,
                    revision=?,detail=?,record_hash=?,updated_at=?
                    WHERE approval_id=? AND state='LOST'""",
                    (
                        lease_id,
                        candidate_hash,
                        session_id,
                        record["ownerIdHash"],
                        _secret_hash(normalized_token),
                        normalized_process,
                        record["state"],
                        1,
                        record["acquiredAt"],
                        record["heartbeatAt"],
                        record["expiresAt"],
                        revision,
                        record["detail"],
                        record_hash,
                        record["updatedAt"],
                        normalized_approval,
                    ),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-cleanup-owner-rotation-raced"
                    )
            else:
                try:
                    connection.execute(
                        """INSERT INTO upbit_functional_owner_lease
                        (lease_id,approval_id,candidate_hash,session_id,
                         owner_id_hash,owner_token_hash,process_identity_hash,
                         state,cleanup_only,acquired_at,heartbeat_at,expires_at,
                         revision,detail,record_hash,updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        values,
                    )
                except sqlite3.IntegrityError as exc:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-owner-lease-replay"
                    ) from exc
            connection.commit()
        return self.owner_lease_status(approval_id=normalized_approval) or {}

    def owner_lease_status(
        self, *, approval_id: str = "", session_id: str = ""
    ) -> dict[str, Any] | None:
        if bool(_text(approval_id)) == bool(_text(session_id)):
            raise UpbitFunctionalBlocked(
                "upbit-functional-owner-lease-lookup-invalid"
            )
        field = "approval_id" if _text(approval_id) else "session_id"
        value = _text(approval_id) or _text(session_id)
        with closing(self._connect()) as connection:
            row = connection.execute(
                f"SELECT * FROM upbit_functional_owner_lease WHERE {field}=?",
                (value,),
            ).fetchone()
        if row is None:
            return None
        public = self._owner_lease_public(dict(row))
        public["recordHashVerified"] = self._owner_lease_row_verified(
            dict(row)
        )
        return public

    def owner_lease_active(
        self,
        *,
        approval_id: str,
        owner_id: str,
        owner_token: str,
        session_id: str = "",
    ) -> bool:
        now = _utc(self.clock(), "current-time")
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT * FROM upbit_functional_owner_lease
                WHERE approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
        if row is None:
            return False
        try:
            expires = _utc(row["expires_at"], "owner-lease-expires-at")
        except UpbitFunctionalBlocked:
            return False
        return bool(
            row["state"] in {"ACQUIRED", "ACTIVE"}
            and self._owner_lease_row_verified(dict(row))
            and now < expires
            and secrets.compare_digest(
                _text(row["owner_id_hash"]), _secret_hash(_text(owner_id))
            )
            and secrets.compare_digest(
                _text(row["owner_token_hash"]),
                _secret_hash(_text(owner_token)),
            )
            and (
                not _text(session_id)
                or secrets.compare_digest(
                    _text(row["session_id"]), _text(session_id)
                )
            )
        )

    def heartbeat_owner_lease(
        self,
        *,
        approval_id: str,
        owner_id: str,
        owner_token: str,
        session_id: str,
    ) -> dict[str, Any]:
        now = _utc(self.clock(), "current-time")
        expires = now + timedelta(
            seconds=UPBIT_FUNCTIONAL_OWNER_LEASE_SECONDS
        )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT * FROM upbit_functional_owner_lease
                WHERE approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
            if (
                row is None
                or row["state"] != "ACTIVE"
                or not self._owner_lease_row_verified(dict(row))
                or _utc(row["expires_at"], "owner-lease-expires-at") <= now
                or not secrets.compare_digest(
                    _text(row["session_id"]), _text(session_id)
                )
                or not secrets.compare_digest(
                    _text(row["owner_id_hash"]),
                    _secret_hash(_text(owner_id)),
                )
                or not secrets.compare_digest(
                    _text(row["owner_token_hash"]),
                    _secret_hash(_text(owner_token)),
                )
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-owner-lease-heartbeat-invalid"
                )
            revision = int(row["revision"] or 0) + 1
            public = self._owner_lease_public(dict(row))
            record = {
                **{
                    key: value
                    for key, value in public.items()
                    if key not in {"recordHash", "heartbeatAt", "expiresAt", "revision", "detail", "updatedAt"}
                },
                "heartbeatAt": now.isoformat(),
                "expiresAt": expires.isoformat(),
                "revision": revision,
                "detail": "owner heartbeat renewed",
                "updatedAt": now.isoformat(),
            }
            cursor = connection.execute(
                """UPDATE upbit_functional_owner_lease SET
                heartbeat_at=?,expires_at=?,revision=?,detail=?,record_hash=?,
                updated_at=? WHERE approval_id=? AND state='ACTIVE'
                AND revision=?""",
                (
                    record["heartbeatAt"],
                    record["expiresAt"],
                    revision,
                    record["detail"],
                    _stable_hash(record),
                    record["updatedAt"],
                    _text(approval_id),
                    int(row["revision"] or 0),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-owner-lease-heartbeat-raced"
                )
            connection.commit()
        return self.owner_lease_status(approval_id=_text(approval_id)) or {}

    def finish_owner_lease(
        self,
        *,
        approval_id: str,
        owner_id: str,
        owner_token: str,
        state: str,
        detail: str,
    ) -> dict[str, Any]:
        terminal = _text(state).upper()
        if terminal not in {"RELEASED", "FAILED", "LOST"}:
            raise UpbitFunctionalBlocked(
                "upbit-functional-owner-lease-terminal-invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            # A heartbeat may be running in another process/store instance.
            # Take the SQLite writer lease before reading so the terminal hash
            # is derived from the exact revision that this transaction owns.
            connection.execute("BEGIN IMMEDIATE")
            now = _utc(self.clock(), "current-time")
            row = connection.execute(
                """SELECT * FROM upbit_functional_owner_lease
                WHERE approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
            if row is None or row["state"] not in {"ACQUIRED", "ACTIVE"}:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-owner-lease-terminal-raced"
                )
            if not self._owner_lease_row_verified(dict(row)):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-owner-lease-record-hash-invalid"
                )
            expected_revision = int(row["revision"] or 0)
            expected_session = _text(row["session_id"])
            expected_record_hash = _text(row["record_hash"]).lower()
            public = self._owner_lease_public(dict(row))
            terminal_record = {
                **{
                    key: value
                    for key, value in public.items()
                    if key
                    not in {
                        "recordHash",
                        "state",
                        "heartbeatAt",
                        "expiresAt",
                        "revision",
                        "detail",
                        "updatedAt",
                    }
                },
                "state": terminal,
                "heartbeatAt": now.isoformat(),
                "expiresAt": now.isoformat(),
                "revision": expected_revision + 1,
                "detail": _text(detail)[:500],
                "updatedAt": now.isoformat(),
            }
            cursor = connection.execute(
                """UPDATE upbit_functional_owner_lease SET state=?,
                owner_token_hash='',heartbeat_at=?,expires_at=?,revision=?,
                detail=?,record_hash=?,updated_at=? WHERE approval_id=?
                AND lease_id=? AND candidate_hash=? AND session_id=?
                AND state=? AND revision=? AND record_hash=?
                AND owner_id_hash=? AND owner_token_hash=?""",
                (
                    terminal,
                    now.isoformat(),
                    now.isoformat(),
                    terminal_record["revision"],
                    _text(detail)[:500],
                    _stable_hash(terminal_record),
                    now.isoformat(),
                    _text(approval_id),
                    _text(row["lease_id"]),
                    _text(row["candidate_hash"]).lower(),
                    expected_session,
                    _text(row["state"]),
                    expected_revision,
                    expected_record_hash,
                    _secret_hash(_text(owner_id)),
                    _secret_hash(_text(owner_token)),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-owner-lease-terminal-raced"
                )
            connection.commit()
        return self.owner_lease_status(approval_id=_text(approval_id)) or {}

    def attest_owner_process_absent(
        self, *, process_absence_attested: bool, detail: str
    ) -> tuple[dict[str, Any], ...]:
        """Revoke every surviving bearer only after process absence is proven."""

        if process_absence_attested is not True:
            raise UpbitFunctionalBlocked(
                "upbit-functional-owner-process-absence-proof-required"
            )
        now = _utc(self.clock(), "current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """SELECT * FROM upbit_functional_owner_lease
                WHERE state IN ('ACQUIRED','ACTIVE')"""
            ).fetchall()
            for row in rows:
                if not self._owner_lease_row_verified(dict(row)):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-owner-lease-record-hash-invalid"
                    )
                public = self._owner_lease_public(dict(row))
                lost_record = {
                    **{
                        key: value
                        for key, value in public.items()
                        if key
                        not in {
                            "recordHash",
                            "state",
                            "heartbeatAt",
                            "expiresAt",
                            "revision",
                            "detail",
                            "updatedAt",
                        }
                    },
                    "state": "LOST",
                    "heartbeatAt": now.isoformat(),
                    "expiresAt": now.isoformat(),
                    "revision": int(row["revision"] or 0) + 1,
                    "detail": _text(detail)[:500],
                    "updatedAt": now.isoformat(),
                }
                connection.execute(
                    """UPDATE upbit_functional_owner_lease SET state='LOST',
                    owner_token_hash='',heartbeat_at=?,expires_at=?,
                    revision=?,detail=?,record_hash=?,updated_at=?
                    WHERE approval_id=? AND state IN ('ACQUIRED','ACTIVE')
                    AND revision=?""",
                    (
                        now.isoformat(),
                        now.isoformat(),
                        lost_record["revision"],
                        lost_record["detail"],
                        _stable_hash(lost_record),
                        now.isoformat(),
                        row["approval_id"],
                        int(row["revision"] or 0),
                    ),
                )
            connection.commit()
        return tuple(
            self.owner_lease_status(approval_id=_text(row["approval_id"]))
            or {}
            for row in rows
        )

    def owner_lease_preparation_status(self) -> dict[str, Any]:
        return {
            "schemaVersion": UPBIT_FUNCTIONAL_OWNER_LEASE_SCHEMA_VERSION,
            "required": self.durable_owner_lease_required,
            "durable": True,
            "singleOwner": True,
            "bearerTokenPersisted": False,
            "heartbeatSeconds": UPBIT_FUNCTIONAL_OWNER_LEASE_SECONDS,
        }

    def first_live_preparation_status(self) -> dict[str, Any]:
        verifier = account_exclusivity_verifier_wiring_status(
            self.account_exclusivity_verifier,
            self.account_exclusivity_verifier_pin,
        )
        owner = self.owner_lease_preparation_status()
        return {
            "prepared": bool(
                verifier.get("ready") is True
                and owner.get("required") is True
                and owner.get("durable") is True
            ),
            "accountExclusivityVerifier": verifier,
            "ownerLease": owner,
        }

    def approve_permit(
        self,
        permit_payload: Mapping[str, Any],
        approval: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = _utc(self.clock(), "current-time")
        permit = parse_functional_test_permit(permit_payload)
        approval_id = _text(approval.get("approvalId"))
        if (
            _SAFE_ID_RE.fullmatch(approval_id) is None
            or approval.get("operatorAuthenticated") is not True
            or approval.get("operatorApproved") is not True
            or not _text(approval.get("operatorId"))
            or not _text(approval.get("nonce"))
            or not self.operator_verifier(dict(approval))
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-operator-approval-invalid"
            )
        exact = {
            "permitId": permit.permit_id,
            "permitHash": permit.content_hash,
            "accountFingerprint": permit.binding.account_id,
            "executionRoute": EXECUTION_ROUTE,
            "symbol": SYMBOL,
        }
        for field, expected in exact.items():
            if not secrets.compare_digest(
                _text(approval.get(field)).lower()
                if field in {"permitHash", "accountFingerprint"}
                else _text(approval.get(field)),
                _text(expected).lower()
                if field in {"permitHash", "accountFingerprint"}
                else _text(expected),
            ):
                raise UpbitFunctionalBlocked(
                    f"upbit-functional-permit-approval-{field}-mismatch"
                )
        approved_at = _utc(approval.get("approvedAt"), "approved-at")
        if abs((now - approved_at).total_seconds()) > 300:
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-approval-stale"
            )
        raw = json.dumps(
            dict(permit_payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        candidate = _candidate_binding(permit, approval)
        candidate_hash = _stable_hash(candidate)
        candidate_raw = json.dumps(
            candidate, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                replay = connection.execute(
                    """SELECT approval_id FROM upbit_functional_approvals
                    WHERE candidate_permit_id=? OR candidate_permit_hash=?""",
                    (permit.permit_id, permit.content_hash),
                ).fetchone()
                if replay is not None:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-permit-approval-replay"
                    )
                connection.execute(
                    """INSERT INTO upbit_functional_approvals
                    (approval_id,permit_id,permit_hash,permit_json,
                     account_fingerprint,state,approval_hash,detail,updated_at,
                     candidate_permit_id,candidate_permit_hash,candidate_hash,
                     candidate_json)
                    VALUES (?,?,?,?,?,'APPROVED',?,'server-owned approval',?,?,?,?,?)""",
                    (
                        approval_id,
                        permit.permit_id,
                        permit.content_hash,
                        raw,
                        permit.binding.account_id,
                        _stable_hash(dict(approval)),
                        now.isoformat(),
                        permit.permit_id,
                        permit.content_hash,
                        candidate_hash,
                        candidate_raw,
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-approval-replay"
                ) from exc
        return self.permit_status(approval_id)

    def issue_permit_candidate(
        self,
        permit_payload: Mapping[str, Any],
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one server-selected permit without granting start authority.

        ``ISSUED`` is deliberately not claimable.  The existing authenticated
        safety-confirmation flow must later perform the only ``ISSUED`` ->
        ``APPROVED`` CAS before ``claim_permit`` can bind a session.  This
        gives the HTTP surface a server-owned id while keeping the raw permit
        and all binding/cap fields off the client boundary.
        """

        now = _utc(self.clock(), "current-time")
        permit = parse_functional_test_permit(permit_payload)
        approval_id = _text(record.get("approvalId"))
        schema_version = _text(record.get("schemaVersion"))
        exact = {
            "permitId": permit.permit_id,
            "permitHash": permit.content_hash,
            "accountFingerprint": permit.binding.account_id,
            "executionRoute": EXECUTION_ROUTE,
            "symbol": SYMBOL,
        }
        if (
            _SAFE_ID_RE.fullmatch(approval_id) is None
            or schema_version
            not in {
                "upbit-functional-server-permit-candidate/v1",
                "upbit-functional-server-permit-candidate/v2",
            }
            or record.get("serverManaged") is not True
            or record.get("singleUse") is not True
            or _text(record.get("issuer")) != "LIVE_TRADER_SERVER"
            or not _text(record.get("nonce"))
            or not self.operator_verifier(dict(record))
            or any(
                not secrets.compare_digest(
                    _text(record.get(field)).lower()
                    if field in {"permitHash", "accountFingerprint"}
                    else _text(record.get(field)),
                    _text(expected).lower()
                    if field in {"permitHash", "accountFingerprint"}
                    else _text(expected),
                )
                for field, expected in exact.items()
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-candidate-invalid"
            )
        issued_at = _utc(record.get("issuedAt"), "candidate-issued-at")
        if abs((now - issued_at).total_seconds()) > 15:
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-candidate-stale"
            )
        candidate = _candidate_binding(permit, record)
        self._assert_current_code_manifest(candidate)
        candidate_hash = _stable_hash(candidate)
        if record.get("candidateHash") is not None and not secrets.compare_digest(
            _text(record.get("candidateHash")).lower(), candidate_hash
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-candidate-hash-mismatch"
            )
        if schema_version.endswith("/v2") and (
            _HASH_RE.fullmatch(_text(record.get("candidateHash")).lower()) is None
            or not isinstance(record.get("candidateBinding"), Mapping)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-candidate-v2-lineage-required"
            )
        candidate_raw = json.dumps(
            candidate, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        bootstrap = record.get("firstLiveBootstrap")
        bootstrap_id = ""
        if bootstrap is not None:
            if not isinstance(bootstrap, Mapping):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-bootstrap-invalid"
                )
            sealed_bootstrap = candidate.get("firstLiveBootstrap")
            expected_bootstrap = {
                key: bootstrap.get(key)
                for key in (
                    "bootstrapId",
                    "sessionNonce",
                    "singleUse",
                    "realE2EValidatedBeforeStart",
                )
            }
            if not isinstance(sealed_bootstrap, Mapping) or dict(
                sealed_bootstrap
            ) != expected_bootstrap:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-bootstrap-lineage-mismatch"
                )
            bootstrap_id = _text(bootstrap.get("bootstrapId"))
            if (
                _SAFE_ID_RE.fullmatch(bootstrap_id) is None
                or _text(bootstrap.get("sessionNonce")) == ""
                or bootstrap.get("singleUse") is not True
                or bootstrap.get("realE2EValidatedBeforeStart") is not False
                or _text(bootstrap.get("candidateHash")).lower()
                != candidate_hash
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-bootstrap-invalid"
                )
        raw = json.dumps(
            dict(permit_payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                active = connection.execute(
                    """SELECT approval_id FROM upbit_functional_approvals
                    WHERE state IN ('ISSUED','APPROVED','CLAIMED','ACTIVE')"""
                ).fetchall()
                if active:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-permit-candidate-already-present"
                    )
                connection.execute(
                    """INSERT INTO upbit_functional_approvals
                    (approval_id,permit_id,permit_hash,permit_json,
                     account_fingerprint,state,approval_hash,detail,updated_at,
                     candidate_permit_id,candidate_permit_hash,candidate_hash,
                     candidate_json,bootstrap_id)
                    VALUES (?,?,?,?,?,'ISSUED',?,'server-selected candidate',?,?,?,?,?,?)""",
                    (
                        approval_id,
                        permit.permit_id,
                        permit.content_hash,
                        raw,
                        permit.binding.account_id,
                        _stable_hash(dict(record)),
                        now.isoformat(),
                        permit.permit_id,
                        permit.content_hash,
                        candidate_hash,
                        candidate_raw,
                        bootstrap_id,
                    ),
                )
                if bootstrap_id:
                    existing_bootstrap = connection.execute(
                        """SELECT bootstrap_id FROM upbit_functional_e2e_bootstrap
                        WHERE state IN ('ISSUED','CLAIMED')"""
                    ).fetchone()
                    if existing_bootstrap is not None:
                        raise UpbitFunctionalBlocked(
                            "upbit-functional-first-live-bootstrap-already-present"
                        )
                    connection.execute(
                        """INSERT INTO upbit_functional_e2e_bootstrap
                        (bootstrap_id,approval_id,candidate_hash,candidate_json,
                         session_nonce,state,detail,updated_at)
                        VALUES (?,?,?,?,?,'ISSUED','operator approval pending',?)""",
                        (
                            bootstrap_id,
                            approval_id,
                            candidate_hash,
                            candidate_raw,
                            _text(bootstrap.get("sessionNonce")),
                            now.isoformat(),
                        ),
                    )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-candidate-replay"
                ) from exc
            except Exception:
                connection.rollback()
                raise
        return self.permit_status(approval_id)

    def approve_issued_permit(
        self,
        *,
        approval_id: str,
        approval: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Authorize an exact candidate after the typed operator challenge."""

        now = _utc(self.clock(), "current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT * FROM upbit_functional_approvals
                WHERE approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
            if row is None or row["state"] != "ISSUED":
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-candidate-not-issuable"
                )
            permit = parse_functional_test_permit(json.loads(row["permit_json"]))
            exact = {
                "approvalId": row["approval_id"],
                "permitId": row["permit_id"],
                "permitHash": row["permit_hash"],
                "candidateHash": row["candidate_hash"],
                "accountFingerprint": row["account_fingerprint"],
                "executionRoute": EXECUTION_ROUTE,
                "symbol": SYMBOL,
            }
            stored_candidate = (
                json.loads(row["candidate_json"])
                if _text(row["candidate_json"])
                else {}
            )
            self._assert_current_code_manifest(stored_candidate)
            candidate_hash_required = bool(
                stored_candidate.get("operatorConfirmationBindsCandidateHash")
            )
            if (
                approval.get("operatorAuthenticated") is not True
                or approval.get("operatorApproved") is not True
                or not _text(approval.get("operatorId"))
                or not _text(approval.get("nonce"))
                or not self.operator_verifier(dict(approval))
                or any(
                    not secrets.compare_digest(
                        _text(approval.get(field)).lower()
                        if field in {"permitHash", "accountFingerprint"}
                        else _text(approval.get(field)),
                        _text(expected).lower()
                        if field in {"permitHash", "accountFingerprint"}
                        else _text(expected),
                    )
                    for field, expected in exact.items()
                    if field != "candidateHash"
                    or candidate_hash_required
                )
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-candidate-approval-invalid"
                )
            approved_at = _utc(approval.get("approvedAt"), "approved-at")
            if (
                abs((now - approved_at).total_seconds()) > 15
                or now < permit.starts_at
                or (now - permit.starts_at).total_seconds() > 300
                or now >= permit.ends_at
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-candidate-approval-stale"
                )
            cursor = connection.execute(
                """UPDATE upbit_functional_approvals
                SET state='APPROVED',approval_hash=?,
                    detail='typed operator confirmation approved',updated_at=?
                WHERE approval_id=? AND state='ISSUED'""",
                (
                    _stable_hash(dict(approval)),
                    now.isoformat(),
                    row["approval_id"],
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-candidate-approval-raced"
                )
            if _text(row["bootstrap_id"]):
                bootstrap = connection.execute(
                    """SELECT * FROM upbit_functional_e2e_bootstrap
                    WHERE bootstrap_id=? AND approval_id=?""",
                    (row["bootstrap_id"], row["approval_id"]),
                ).fetchone()
                sealed_bootstrap = stored_candidate.get("firstLiveBootstrap")
                if (
                    bootstrap is None
                    or not isinstance(sealed_bootstrap, Mapping)
                    or _text(sealed_bootstrap.get("bootstrapId"))
                    != _text(bootstrap["bootstrap_id"])
                    or _text(sealed_bootstrap.get("sessionNonce"))
                    != _text(bootstrap["session_nonce"])
                    or sealed_bootstrap.get("singleUse") is not True
                    or sealed_bootstrap.get(
                        "realE2EValidatedBeforeStart"
                    )
                    is not False
                ):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-first-live-bootstrap-lineage-invalid"
                    )
                bootstrap_cursor = connection.execute(
                    """UPDATE upbit_functional_e2e_bootstrap
                    SET operator_approval_hash=?,detail='operator approved',updated_at=?
                    WHERE bootstrap_id=? AND approval_id=? AND state='ISSUED'
                    AND candidate_hash=?""",
                    (
                        _stable_hash(dict(approval)),
                        now.isoformat(),
                        row["bootstrap_id"],
                        row["approval_id"],
                        row["candidate_hash"],
                    ),
                )
                if bootstrap_cursor.rowcount != 1:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-first-live-bootstrap-approval-invalid"
                    )
            connection.commit()
        return self.permit_status(_text(approval_id))

    def issued_pointer(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT approval_id,permit_id,permit_hash,state,
                claimed_session_id,permit_json,updated_at,candidate_hash,
                bootstrap_id
                FROM upbit_functional_approvals
                WHERE state='ISSUED' ORDER BY updated_at"""
            ).fetchall()
        if len(rows) > 1:
            raise UpbitFunctionalBlocked(
                "upbit-functional-multiple-issued-permit-candidates"
            )
        return dict(rows[0]) if rows else None

    def retire_issued_permit(self, *, approval_id: str, detail: str) -> None:
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """UPDATE upbit_functional_approvals
                SET state='FAILED',detail=?,updated_at=?
                WHERE approval_id=? AND state='ISSUED'""",
                (
                    _text(detail)[:500],
                    _utc(self.clock(), "current-time").isoformat(),
                    _text(approval_id),
                ),
            )
            if cursor.rowcount != 1:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-candidate-retire-invalid"
                )
            connection.execute(
                """UPDATE upbit_functional_e2e_bootstrap
                SET state='FAILED',detail=?,updated_at=?
                WHERE approval_id=? AND state='ISSUED'""",
                (
                    _text(detail)[:500],
                    _utc(self.clock(), "current-time").isoformat(),
                    _text(approval_id),
                ),
            )
            connection.commit()

    def retire_unclaimed_permit(
        self,
        *,
        approval_id: str,
        detail: str,
        expired: bool = False,
    ) -> bool:
        """Retire exactly one APPROVED pointer that never acquired a session.

        This CAS is intentionally a no-op after ``claim_permit`` crosses its
        durable boundary.  Callers may therefore use it from broad owner /
        pre-claim exception handlers without accidentally revoking a live or
        cleanup-capable session.
        """

        terminal = "EXPIRED" if expired else "FAILED"
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """UPDATE upbit_functional_approvals
                SET state=?,detail=?,updated_at=?
                WHERE approval_id=? AND state='APPROVED'
                AND claimed_session_id=''""",
                (
                    terminal,
                    _text(detail)[:500],
                    _utc(self.clock(), "current-time").isoformat(),
                    _text(approval_id),
                ),
            )
            if cursor.rowcount == 1:
                connection.execute(
                    """UPDATE upbit_functional_e2e_bootstrap
                    SET state='FAILED',detail=?,updated_at=?
                    WHERE approval_id=? AND state='ISSUED'""",
                    (
                        _text(detail)[:500],
                        _utc(self.clock(), "current-time").isoformat(),
                        _text(approval_id),
                    ),
                )
            connection.commit()
        return cursor.rowcount == 1

    def claim_permit(
        self,
        *,
        approval_id: str,
        session_id: str,
        owner_lease_id: str = "",
        owner_id: str = "",
        owner_token: str = "",
    ) -> dict[str, Any]:
        now = _utc(self.clock(), "current-time")
        if _SAFE_ID_RE.fullmatch(_text(session_id)) is None:
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-session-invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT * FROM upbit_functional_approvals
                WHERE approval_id=? AND state='APPROVED'
                AND claimed_session_id=''""",
                (_text(approval_id),),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-approval-not-claimable"
                )
            owner_lease = connection.execute(
                """SELECT * FROM upbit_functional_owner_lease
                WHERE approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
            owner_inputs_present = bool(
                _text(owner_lease_id)
                or _text(owner_id)
                or _text(owner_token)
            )
            if self.durable_owner_lease_required or owner_inputs_present:
                if (
                    owner_lease is None
                    or owner_lease["state"] != "ACQUIRED"
                    or not self._owner_lease_row_verified(
                        dict(owner_lease)
                    )
                    or not secrets.compare_digest(
                        _text(owner_lease["lease_id"]),
                        _text(owner_lease_id),
                    )
                    or not secrets.compare_digest(
                        _text(owner_lease["candidate_hash"]).lower(),
                        _text(row["candidate_hash"]).lower(),
                    )
                    or not secrets.compare_digest(
                        _text(owner_lease["owner_id_hash"]),
                        _secret_hash(_text(owner_id)),
                    )
                    or not secrets.compare_digest(
                        _text(owner_lease["owner_token_hash"]),
                        _secret_hash(_text(owner_token)),
                    )
                    or _text(owner_lease["session_id"])
                    or _utc(
                        owner_lease["expires_at"],
                        "owner-lease-expires-at",
                    )
                    <= now
                ):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-permit-owner-lease-required"
                    )
            candidate_permit = parse_functional_test_permit(
                json.loads(row["permit_json"])
            )
            approved_at = _utc(row["updated_at"], "approval-updated-at")
            approval_age_seconds = (now - approved_at).total_seconds()
            if (
                approval_age_seconds < 0
                or approval_age_seconds > 300
                or now >= candidate_permit.ends_at
            ):
                connection.execute(
                    """UPDATE upbit_functional_approvals
                    SET state='EXPIRED',
                        detail='approved candidate expired before claim',
                        updated_at=?
                    WHERE approval_id=? AND state='APPROVED'
                    AND claimed_session_id=''""",
                    (now.isoformat(), row["approval_id"]),
                )
                if _text(row["bootstrap_id"]):
                    connection.execute(
                        """UPDATE upbit_functional_e2e_bootstrap
                        SET state='FAILED',
                            detail='approved candidate expired before claim',
                            updated_at=?
                        WHERE bootstrap_id=? AND approval_id=?
                        AND state='ISSUED'""",
                        (
                            now.isoformat(),
                            row["bootstrap_id"],
                            row["approval_id"],
                        ),
                    )
                connection.commit()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-approval-expired"
                )
            stored_candidate = (
                json.loads(row["candidate_json"])
                if _text(row["candidate_json"])
                else _candidate_binding(candidate_permit)
            )
            candidate = _candidate_binding(
                candidate_permit,
                {"candidateBinding": stored_candidate},
            )
            self._assert_current_code_manifest(candidate)
            candidate_hash = _stable_hash(stored_candidate)
            if (
                candidate != stored_candidate
                or not secrets.compare_digest(
                    candidate_hash, _text(row["candidate_hash"]).lower()
                )
                or not secrets.compare_digest(
                    candidate_permit.permit_id,
                    _text(row["candidate_permit_id"]),
                )
                or not secrets.compare_digest(
                    candidate_permit.content_hash,
                    _text(row["candidate_permit_hash"]).lower(),
                )
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-activation-candidate-lineage-invalid"
                )
            activated_permit = issue_functional_test_permit(
                binding=candidate_permit.binding,
                environment=candidate_permit.environment,
                duration_value=candidate_permit.duration_value,
                duration_unit=candidate_permit.duration_unit,
                caps=candidate_permit.caps,
                now=now,
            )
            if (
                _permit_immutable_lineage(activated_permit)
                != _permit_immutable_lineage(candidate_permit)
                or int(
                    (activated_permit.ends_at - activated_permit.starts_at)
                    .total_seconds()
                )
                != 7200
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-activation-reseal-lineage-mismatch"
                )
            activated_raw = json.dumps(
                activated_permit.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            activation_lineage = {
                "schemaVersion": "upbit-functional-activation-reseal/v1",
                "approvalId": row["approval_id"],
                "candidatePermitId": row["candidate_permit_id"],
                "candidatePermitHash": row["candidate_permit_hash"],
                "candidateHash": candidate_hash,
                "activatedPermitId": activated_permit.permit_id,
                "activatedPermitHash": activated_permit.content_hash,
                "sessionId": _text(session_id),
                "activatedAt": now.isoformat(),
                "activeDurationSeconds": 7200,
            }
            activation_lineage_hash = _stable_hash(activation_lineage)
            cursor = connection.execute(
                """UPDATE upbit_functional_approvals
                SET state='CLAIMED',claimed_session_id=?,
                    permit_id=?,permit_hash=?,permit_json=?,activated_at=?,
                    activation_lineage_hash=?,
                    detail='start claimed with activation-relative two-hour reseal',
                    updated_at=?
                WHERE approval_id=? AND state='APPROVED'
                AND claimed_session_id=''""",
                (
                    session_id,
                    activated_permit.permit_id,
                    activated_permit.content_hash,
                    activated_raw,
                    now.isoformat(),
                    activation_lineage_hash,
                    now.isoformat(),
                    _text(approval_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-approval-not-claimable"
                )
            if self.durable_owner_lease_required or owner_inputs_present:
                lease_revision = int(owner_lease["revision"] or 0) + 1
                lease_public = self._owner_lease_public(dict(owner_lease))
                lease_record = {
                    **{
                        key: value
                        for key, value in lease_public.items()
                        if key
                        not in {
                            "recordHash",
                            "sessionId",
                            "state",
                            "revision",
                            "detail",
                            "updatedAt",
                        }
                    },
                    "sessionId": _text(session_id),
                    "state": "ACTIVE",
                    "revision": lease_revision,
                    "detail": "owner atomically bound to permit claim",
                    "updatedAt": now.isoformat(),
                }
                lease_cursor = connection.execute(
                    """UPDATE upbit_functional_owner_lease SET
                    session_id=?,state='ACTIVE',revision=?,detail=?,
                    record_hash=?,updated_at=? WHERE lease_id=?
                    AND approval_id=? AND state='ACQUIRED' AND revision=?""",
                    (
                        _text(session_id),
                        lease_revision,
                        lease_record["detail"],
                        _stable_hash(lease_record),
                        lease_record["updatedAt"],
                        _text(owner_lease_id),
                        _text(approval_id),
                        int(owner_lease["revision"] or 0),
                    ),
                )
                if lease_cursor.rowcount != 1:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-owner-lease-claim-bind-raced"
                    )
            if _text(row["bootstrap_id"]):
                bootstrap_row = connection.execute(
                    """SELECT * FROM upbit_functional_e2e_bootstrap
                    WHERE bootstrap_id=? AND approval_id=?""",
                    (row["bootstrap_id"], row["approval_id"]),
                ).fetchone()
                sealed_bootstrap = stored_candidate.get("firstLiveBootstrap")
                if (
                    bootstrap_row is None
                    or not isinstance(sealed_bootstrap, Mapping)
                    or _text(sealed_bootstrap.get("bootstrapId"))
                    != _text(bootstrap_row["bootstrap_id"])
                    or _text(sealed_bootstrap.get("sessionNonce"))
                    != _text(bootstrap_row["session_nonce"])
                    or sealed_bootstrap.get("singleUse") is not True
                    or sealed_bootstrap.get(
                        "realE2EValidatedBeforeStart"
                    )
                    is not False
                ):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-first-live-bootstrap-claim-lineage-invalid"
                    )
                bootstrap_cursor = connection.execute(
                    """UPDATE upbit_functional_e2e_bootstrap
                    SET state='CLAIMED',claimed_session_id=?,
                        detail='first-live session claimed',updated_at=?
                    WHERE bootstrap_id=? AND approval_id=? AND state='ISSUED'
                    AND candidate_hash=? AND operator_approval_hash<>''""",
                    (
                        session_id,
                        now.isoformat(),
                        row["bootstrap_id"],
                        row["approval_id"],
                        candidate_hash,
                    ),
                )
                if bootstrap_cursor.rowcount != 1:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-first-live-bootstrap-not-claimable"
                    )
            connection.commit()
        return self.permit_reader(approval_id, session_id=session_id)

    def first_live_bootstrap_claimable(self, *, approval_id: str) -> bool:
        """Read-only preclaim proof for bypassing only the REAL_E2E bit."""

        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT a.state AS approval_state,a.candidate_hash,
                    a.candidate_json,a.bootstrap_id,b.state AS bootstrap_state,
                    b.candidate_hash AS bootstrap_candidate_hash,
                    b.session_nonce,b.operator_approval_hash,e.validated
                FROM upbit_functional_approvals a
                JOIN upbit_functional_e2e_bootstrap b
                  ON b.bootstrap_id=a.bootstrap_id
                 AND b.approval_id=a.approval_id
                JOIN upbit_functional_e2e_state e
                  ON e.state_key='REAL_E2E'
                WHERE a.approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
        if row is None:
            return False
        try:
            candidate = json.loads(row["candidate_json"])
            bootstrap = candidate["firstLiveBootstrap"]
        except (KeyError, TypeError, ValueError):
            return False
        return bool(
            row["approval_state"] == "APPROVED"
            and row["bootstrap_state"] == "ISSUED"
            and int(row["validated"]) == 0
            and _text(row["operator_approval_hash"])
            and secrets.compare_digest(
                _text(row["candidate_hash"]).lower(),
                _text(row["bootstrap_candidate_hash"]).lower(),
            )
            and secrets.compare_digest(
                _stable_hash(candidate), _text(row["candidate_hash"]).lower()
            )
            and _text(bootstrap.get("bootstrapId"))
            == _text(row["bootstrap_id"])
            and _text(bootstrap.get("sessionNonce"))
            == _text(row["session_nonce"])
            and bootstrap.get("singleUse") is True
            and bootstrap.get("realE2EValidatedBeforeStart") is False
        )

    def bind_permit(
        self,
        *,
        approval_id: str,
        session_id: str,
        owner_id: str = "",
        owner_token: str = "",
    ) -> None:
        if self.durable_owner_lease_required and not self.owner_lease_active(
            approval_id=_text(approval_id),
            owner_id=_text(owner_id),
            owner_token=_text(owner_token),
            session_id=_text(session_id),
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-bind-owner-lease-required"
            )
        self._permit_transition(
            approval_id=approval_id,
            session_id=session_id,
            expected="CLAIMED",
            state="ACTIVE",
            detail="bound to durable active session",
        )

    def fail_permit(self, *, approval_id: str, session_id: str, detail: str) -> None:
        self._permit_transition(
            approval_id=approval_id,
            session_id=session_id,
            expected="CLAIMED",
            state="FAILED",
            detail=detail,
        )

    def consume_permit(self, *, approval_id: str, session_id: str) -> None:
        try:
            self._permit_transition(
                approval_id=approval_id,
                session_id=session_id,
                expected="ACTIVE",
                state="CONSUMED",
                detail="session finalized",
            )
        except UpbitFunctionalBlocked:
            current = self.permit_status(approval_id)
            if (
                current["state"] == "CONSUMED"
                and current["claimed_session_id"] == _text(session_id)
            ):
                return
            raise

    def finish_first_live_bootstrap(
        self,
        *,
        approval_id: str,
        session_id: str,
        passed: bool,
        evidence_hash: str,
        detail: str,
    ) -> dict[str, Any] | None:
        """Atomically consume the permit and terminalize a first-live canary.

        The trade ledger shares this SQLite database, so the durable FINALIZED
        row/evidence hash and one-use operator approval are verified in one
        transaction.  This lane is explicitly nonpromotion: it records wiring
        evidence but never turns on the permanent REAL_E2E release bit.
        """

        normalized_hash = _exact_lower_hash(evidence_hash)
        if normalized_hash is None:
            raise UpbitFunctionalBlocked(
                "upbit-functional-first-live-evidence-hash-invalid"
            )
        now = _utc(self.clock(), "current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            approval = connection.execute(
                """SELECT * FROM upbit_functional_approvals
                WHERE approval_id=? AND claimed_session_id=?""",
                (_text(approval_id), _text(session_id)),
            ).fetchone()
            durable = connection.execute(
                """SELECT state,final_evidence_hash,final_evidence_json
                FROM upbit_functional_sessions WHERE session_id=?""",
                (_text(session_id),),
            ).fetchone()
            durable_hash = (
                _exact_lower_hash(durable["final_evidence_hash"])
                if durable is not None
                else None
            )
            if (
                approval is None
                or approval["state"] not in {"ACTIVE", "CONSUMED"}
                or durable is None
                or durable["state"] != "FINALIZED"
                or durable_hash is None
                or not secrets.compare_digest(
                    durable_hash,
                    normalized_hash,
                )
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-durable-final-mismatch"
                )
            try:
                durable_evidence = json.loads(durable["final_evidence_json"])
            except (TypeError, ValueError) as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-evidence-invalid"
                ) from exc
            stored_candidate = (
                json.loads(approval["candidate_json"])
                if _text(approval["candidate_json"])
                else {}
            )
            self._assert_current_code_manifest(stored_candidate)
            account_exclusivity_verified = (
                _account_exclusivity_evidence_complete(
                    durable_evidence,
                    verifier=self.account_exclusivity_verifier,
                    verifier_pin=self.account_exclusivity_verifier_pin,
                )
            )
            verified_pass = bool(
                passed
                and durable_evidence.get("functionalTestPassed") is True
                and durable_evidence.get("exactTwoHourRuntimeComplete") is True
                and durable_evidence.get("activationRelativePermitExact") is True
                and durable_evidence.get("processMonotonicContinuity") is True
                and durable_evidence.get("clockDiscontinuityAbsent") is True
                and Decimal(
                    str(durable_evidence.get("actualDurationSeconds") or 0)
                )
                >= Decimal("7200")
                and Decimal(
                    str(
                        durable_evidence.get(
                            "processMonotonicElapsedSeconds"
                        )
                        or 0
                    )
                )
                >= Decimal("7200")
                and durable_evidence.get(
                    "exclusiveAccountCausalProofComplete"
                )
                is True
                and durable_evidence.get(
                    "accountExclusivityProofVerified"
                )
                is True
                and durable_evidence.get(
                    "accountExclusivityAuthorityPinned"
                )
                is True
                and durable_evidence.get(
                    "accountExclusivityContinuouslyVerified"
                )
                is True
                and durable_evidence.get("otherApiKeysAbsent") is True
                and durable_evidence.get("manualTradingAbsent") is True
                and durable_evidence.get("otherBotsAbsent") is True
                and account_exclusivity_verified
                and durable_evidence.get("functionalCapabilityCleared") is True
                and durable_evidence.get("realOrdersEnabled") is False
                and durable_evidence.get("newEntriesBlocked") is True
            )
            verified_wiring = _durable_functional_wiring_complete(
                connection,
                approval=approval,
                session_id=_text(session_id),
                evidence=durable_evidence,
                trusted_now=now,
                current_selection=(
                    dict(self.immutable_selection_reader())
                    if self.immutable_selection_reader is not None
                    else None
                ),
                account_exclusivity_verifier=(
                    self.account_exclusivity_verifier
                ),
                account_exclusivity_verifier_pin=(
                    self.account_exclusivity_verifier_pin
                ),
            )
            if passed and not verified_pass:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-pass-proof-incomplete"
                )
            # This authorized run is explicitly nonpromotion.  A complete
            # wiring proof burns/consumes the one-use bootstrap, but never
            # turns on the permanent REAL_E2E release bit.  Account-wide
            # causal uncertainty remains an honest SAFE_INCOMPLETE outcome.
            terminal = "CONSUMED" if verified_wiring else "FAILED"
            row = connection.execute(
                """SELECT * FROM upbit_functional_e2e_bootstrap
                WHERE approval_id=? AND claimed_session_id=?""",
                (_text(approval_id), _text(session_id)),
            ).fetchone()
            if row is None:
                cursor = connection.execute(
                    """UPDATE upbit_functional_approvals
                    SET state='CONSUMED',detail='session finalized',updated_at=?
                    WHERE approval_id=? AND claimed_session_id=?
                    AND state='ACTIVE'""",
                    (now.isoformat(), _text(approval_id), _text(session_id)),
                )
                if cursor.rowcount == 0 and approval["state"] != "CONSUMED":
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-permit-final-consume-invalid"
                    )
                connection.commit()
                return None
            if row["state"] in {"CONSUMED", "FAILED"}:
                replay_hash = _exact_lower_hash(row["terminal_evidence_hash"])
                if (
                    row["state"] == terminal
                    and replay_hash is not None
                    and secrets.compare_digest(
                        replay_hash,
                        normalized_hash,
                    )
                ):
                    connection.rollback()
                    return dict(row)
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-bootstrap-terminal-replay"
                )
            cursor = connection.execute(
                """UPDATE upbit_functional_e2e_bootstrap
                SET state=?,terminal_evidence_hash=?,detail=?,updated_at=?
                WHERE bootstrap_id=? AND state='CLAIMED'
                AND approval_id=? AND claimed_session_id=?""",
                (
                    terminal,
                    normalized_hash,
                    _text(detail)[:500],
                    now.isoformat(),
                    row["bootstrap_id"],
                    row["approval_id"],
                    row["claimed_session_id"],
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-first-live-bootstrap-terminal-raced"
                )
            approval_cursor = connection.execute(
                """UPDATE upbit_functional_approvals
                SET state='CONSUMED',detail='session finalized',updated_at=?
                WHERE approval_id=? AND claimed_session_id=?
                AND state='ACTIVE'""",
                (now.isoformat(), _text(approval_id), _text(session_id)),
            )
            if approval_cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-final-consume-invalid"
                )
            connection.commit()
        return self.first_live_bootstrap_status(
            approval_id=_text(approval_id)
        )

    def durable_wiring_verified(
        self,
        *,
        approval_id: str,
        session_id: str,
    ) -> bool:
        """Read-only independent verification for audit/tests and backend."""

        with self._lock, closing(self._connect()) as connection:
            approval = connection.execute(
                """SELECT * FROM upbit_functional_approvals
                WHERE approval_id=? AND claimed_session_id=?""",
                (_text(approval_id), _text(session_id)),
            ).fetchone()
            session = connection.execute(
                """SELECT state,final_evidence_json,final_evidence_hash FROM
                upbit_functional_sessions WHERE session_id=?""",
                (_text(session_id),),
            ).fetchone()
            if approval is None or session is None or session["state"] != "FINALIZED":
                return False
            try:
                evidence = json.loads(session["final_evidence_json"])
            except (TypeError, ValueError):
                return False
            durable_hash = _exact_lower_hash(session["final_evidence_hash"])
            if (
                not isinstance(evidence, Mapping)
                or durable_hash is None
                or not secrets.compare_digest(
                    _stable_hash(evidence),
                    durable_hash,
                )
            ):
                return False
            return _durable_functional_wiring_complete(
                connection,
                approval=approval,
                session_id=_text(session_id),
                evidence=evidence,
                trusted_now=_utc(self.clock(), "current-time"),
                current_selection=(
                    dict(self.immutable_selection_reader())
                    if self.immutable_selection_reader is not None
                    else None
                ),
                account_exclusivity_verifier=(
                    self.account_exclusivity_verifier
                ),
                account_exclusivity_verifier_pin=(
                    self.account_exclusivity_verifier_pin
                ),
            )

    def first_live_bootstrap_status(
        self, *, approval_id: str = ""
    ) -> dict[str, Any] | None:
        query = """SELECT bootstrap_id,approval_id,candidate_hash,state,
            claimed_session_id,terminal_evidence_hash,detail,updated_at
            FROM upbit_functional_e2e_bootstrap"""
        parameters: tuple[object, ...] = ()
        if _text(approval_id):
            query += " WHERE approval_id=?"
            parameters = (_text(approval_id),)
        else:
            query += " WHERE state IN ('ISSUED','CLAIMED')"
        query += " ORDER BY updated_at"
        with closing(self._connect()) as connection:
            rows = connection.execute(query, parameters).fetchall()
        if len(rows) > 1:
            raise UpbitFunctionalBlocked(
                "upbit-functional-multiple-first-live-bootstrap-pointers"
            )
        return dict(rows[0]) if rows else None

    def real_e2e_status(self) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT validated,evidence_hash,session_id,updated_at
                FROM upbit_functional_e2e_state WHERE state_key='REAL_E2E'"""
            ).fetchone()
        if row is None:
            raise UpbitFunctionalBlocked(
                "upbit-functional-real-e2e-state-missing"
            )
        return dict(row)

    def cleanup_claim_authority(
        self, *, session_id: str, claim_id: str
    ) -> bool:
        """Return exact durable authority for risk-reducing Kill cleanup."""

        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT c.slot,c.side,c.state AS claim_state,
                    s.state AS session_state,s.new_entries_blocked
                FROM upbit_functional_claims c
                JOIN upbit_functional_sessions s
                  ON s.session_id=c.session_id
                WHERE c.session_id=? AND c.claim_id=?""",
                (_text(session_id), _text(claim_id)),
            ).fetchone()
        return bool(
            row is not None
            and row["session_state"] == "CLEANUP"
            and int(row["new_entries_blocked"]) == 1
            and row["slot"] in {"CLEANUP_CANCEL", "CLEANUP_SELL"}
            and row["side"] in {"CANCEL", "ASK"}
            and row["claim_state"]
            in {"CLAIMED_PRE_POST", "POST_MAY_HAVE_CROSSED"}
        )

    def request_cleanup_only(
        self, *, approval_id: str, session_id: str, reason: str
    ) -> dict[str, Any]:
        """Durably revoke entry while preserving owned cleanup authority."""

        now = _utc(self.clock(), "current-time")
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """UPDATE upbit_functional_approvals
                SET cleanup_only=1,detail=?,updated_at=?
                WHERE approval_id=? AND claimed_session_id=?
                AND state IN ('CLAIMED','ACTIVE')""",
                (
                    "cleanup-only:" + _text(reason)[:450],
                    now.isoformat(),
                    _text(approval_id),
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-cleanup-only-request-invalid"
                )
            connection.commit()
        return self.permit_status(_text(approval_id))

    def _permit_transition(
        self,
        *,
        approval_id: str,
        session_id: str,
        expected: str,
        state: str,
        detail: str,
    ) -> None:
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """UPDATE upbit_functional_approvals
                SET state=?,detail=?,updated_at=?
                WHERE approval_id=? AND state=? AND claimed_session_id=?""",
                (
                    state,
                    _text(detail)[:500],
                    _utc(self.clock(), "current-time").isoformat(),
                    _text(approval_id),
                    expected,
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-permit-approval-transition-invalid"
                )
            if state == "FAILED":
                connection.execute(
                    """UPDATE upbit_functional_e2e_bootstrap
                    SET state='FAILED',detail=?,updated_at=?
                    WHERE approval_id=? AND claimed_session_id=?
                    AND state='CLAIMED'""",
                    (
                        _text(detail)[:500],
                        _utc(self.clock(), "current-time").isoformat(),
                        _text(approval_id),
                        _text(session_id),
                    ),
                )
            connection.commit()

    def permit_reader(
        self, approval_id: str, *, session_id: str
    ) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT * FROM upbit_functional_approvals
                WHERE approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
        if (
            row is None
            or row["state"] not in {"CLAIMED", "ACTIVE"}
            or row["claimed_session_id"] != _text(session_id)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-active-pointer-missing"
            )
        return {
            "schemaVersion": "upbit-functional-approved-permit/v1",
            "approvalId": row["approval_id"],
            "permitId": row["permit_id"],
            "permitHash": row["permit_hash"],
            "activeSessionId": row["claimed_session_id"],
            "accountFingerprint": row["account_fingerprint"],
            "executionRoute": EXECUTION_ROUTE,
            "symbol": SYMBOL,
            "approvalState": "ACTIVE",
            "serverManaged": True,
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "singleUse": True,
            "permit": json.loads(row["permit_json"]),
        }

    def permit_status(self, approval_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT approval_id,permit_id,permit_hash,state,
                claimed_session_id,detail,updated_at,candidate_permit_id,
                candidate_permit_hash,candidate_hash,activated_at,
                activation_lineage_hash,bootstrap_id,cleanup_only
                FROM upbit_functional_approvals WHERE approval_id=?""",
                (_text(approval_id),),
            ).fetchone()
        if row is None:
            raise UpbitFunctionalBlocked(
                "upbit-functional-permit-approval-missing"
            )
        return dict(row)

    def active_pointer(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT approval_id,permit_id,permit_hash,state,
                claimed_session_id,cleanup_only FROM upbit_functional_approvals
                WHERE state IN ('CLAIMED','ACTIVE') ORDER BY updated_at"""
            ).fetchall()
        if len(rows) > 1:
            raise UpbitFunctionalBlocked(
                "upbit-functional-multiple-active-approval-pointers"
            )
        return dict(rows[0]) if rows else None

    def order_authority_pointer(self) -> dict[str, Any] | None:
        """Return any operator-approved or session-bound order authority."""

        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT approval_id,permit_id,permit_hash,state,
                claimed_session_id,cleanup_only FROM upbit_functional_approvals
                WHERE state IN ('APPROVED','CLAIMED','ACTIVE')
                ORDER BY updated_at"""
            ).fetchall()
        if len(rows) > 1:
            raise UpbitFunctionalBlocked(
                "upbit-functional-multiple-order-authority-pointers"
            )
        return dict(rows[0]) if rows else None

    def approve_recovery(self, record: Mapping[str, Any]) -> dict[str, Any]:
        now = _utc(self.clock(), "current-time")
        recovery_id = _text(record.get("recoveryId"))
        content_hash = _text(record.get("contentHash")).lower()
        if (
            _SAFE_ID_RE.fullmatch(recovery_id) is None
            or _HASH_RE.fullmatch(content_hash) is None
            or content_hash
            != _stable_hash(
                {key: value for key, value in record.items() if key != "contentHash"}
            )
            or record.get("serverManaged") is not True
            or record.get("operatorAuthenticated") is not True
            or record.get("operatorApproved") is not True
            or record.get("singleUse") is not True
            or record.get("previousOwnerLost") is not True
            or record.get("previousOwnerLeaseExpired") is not True
            or record.get("officialRestReconciled") is not True
            or _text(record.get("schemaVersion"))
            != "upbit-functional-recovery-approval/v1"
            or _text(record.get("mode")) != "CLEANUP_ONLY"
            or _text(record.get("approvalState")) != "ACTIVE"
            or _HASH_RE.fullmatch(
                _text(record.get("accountFingerprint")).lower()
            )
            is None
            or _HASH_RE.fullmatch(
                _text(record.get("permitHash")).lower()
            )
            is None
            or _HASH_RE.fullmatch(
                _text(record.get("officialRestTruthHash")).lower()
            )
            is None
            or _HASH_RE.fullmatch(
                _text(record.get("previousOwnerLeaseEvidenceHash")).lower()
            )
            is None
            or not self.operator_verifier(dict(record))
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-approval-invalid"
            )
        observed_at = _utc(record.get("observedAt"), "recovery-observed-at")
        if (
            (now - observed_at).total_seconds() < 0
            or (now - observed_at).total_seconds() > 15
            or int(record.get("previousWriterGeneration") or 0) <= 0
            or int(record.get("nextWriterGeneration") or 0)
            != int(record.get("previousWriterGeneration") or 0) + 1
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-approval-evidence-invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                active = connection.execute(
                    """SELECT * FROM upbit_functional_approvals
                    WHERE permit_id=? AND permit_hash=? AND claimed_session_id=?
                    AND account_fingerprint=? AND state='ACTIVE'""",
                    (
                        _text(record.get("permitId")),
                        _text(record.get("permitHash")).lower(),
                        _text(record.get("sessionId")),
                        _text(record.get("accountFingerprint")).lower(),
                    ),
                ).fetchone()
                if active is None:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-recovery-active-session-approval-missing"
                    )
                connection.execute(
                    """INSERT INTO upbit_functional_recovery_approvals
                    (recovery_id,recovery_hash,recovery_json,session_id,
                     permit_id,permit_hash,state,detail,updated_at)
                    VALUES (?,?,?,?,?,?,'APPROVED','server-owned recovery',?)""",
                    (
                        recovery_id,
                        content_hash,
                        json.dumps(
                            dict(record),
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        ),
                        _text(record.get("sessionId")),
                        _text(record.get("permitId")),
                        _text(record.get("permitHash")).lower(),
                        now.isoformat(),
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-approval-replay"
                ) from exc
            except Exception:
                connection.rollback()
                raise
        return {"recoveryId": recovery_id, "recoveryHash": content_hash}

    def issue_recovery_candidate(
        self, record: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Persist inert owner-loss identity before typed confirmation."""

        now = _utc(self.clock(), "current-time")
        recovery_id = _text(record.get("recoveryId"))
        candidate_hash = _text(record.get("candidateHash")).lower()
        expected_hash = _stable_hash(
            {
                key: value
                for key, value in record.items()
                if key != "candidateHash"
            }
        )
        if (
            _SAFE_ID_RE.fullmatch(recovery_id) is None
            or _HASH_RE.fullmatch(candidate_hash) is None
            or not secrets.compare_digest(candidate_hash, expected_hash)
            or _text(record.get("schemaVersion"))
            != "upbit-functional-recovery-candidate/v1"
            or _text(record.get("mode")) != "CLEANUP_ONLY"
            or _text(record.get("candidateState")) != "ISSUED"
            or record.get("serverManaged") is not True
            or record.get("singleUse") is not True
            or record.get("previousOwnerLost") is not True
            or record.get("previousOwnerLeaseExpired") is not True
            or record.get("operatorAuthenticated") is not False
            or record.get("operatorApproved") is not False
            or _HASH_RE.fullmatch(
                _text(record.get("accountFingerprint")).lower()
            )
            is None
            or _HASH_RE.fullmatch(
                _text(record.get("permitHash")).lower()
            )
            is None
            or int(record.get("previousWriterGeneration") or 0) <= 0
            or int(record.get("nextWriterGeneration") or 0)
            != int(record.get("previousWriterGeneration") or 0) + 1
            or not self.operator_verifier(dict(record))
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-candidate-invalid"
            )
        issued_at = _utc(record.get("issuedAt"), "recovery-candidate-issued-at")
        if abs((now - issued_at).total_seconds()) > 15:
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-candidate-stale"
            )
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                active = connection.execute(
                    """SELECT recovery_id FROM
                    upbit_functional_recovery_approvals
                    WHERE state IN ('ISSUED','APPROVED','CLAIMED')"""
                ).fetchall()
                if active:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-recovery-candidate-already-present"
                    )
                main = connection.execute(
                    """SELECT * FROM upbit_functional_approvals
                    WHERE permit_id=? AND permit_hash=?
                    AND claimed_session_id=? AND account_fingerprint=?
                    AND state='ACTIVE'""",
                    (
                        _text(record.get("permitId")),
                        _text(record.get("permitHash")).lower(),
                        _text(record.get("sessionId")),
                        _text(record.get("accountFingerprint")).lower(),
                    ),
                ).fetchone()
                if main is None:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-recovery-active-session-approval-missing"
                    )
                connection.execute(
                    """INSERT INTO upbit_functional_recovery_approvals
                    (recovery_id,recovery_hash,recovery_json,session_id,
                     permit_id,permit_hash,state,detail,updated_at)
                    VALUES (?,?,?,?,?,?,'ISSUED',
                    'server-owned inert recovery candidate',?)""",
                    (
                        recovery_id,
                        candidate_hash,
                        json.dumps(
                            dict(record),
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        ),
                        _text(record.get("sessionId")),
                        _text(record.get("permitId")),
                        _text(record.get("permitHash")).lower(),
                        now.isoformat(),
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-candidate-replay"
                ) from exc
            except Exception:
                connection.rollback()
                raise
        return {
            "recoveryId": recovery_id,
            "candidateHash": candidate_hash,
            "state": "ISSUED",
        }

    def issued_recovery_pointer(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM upbit_functional_recovery_approvals
                WHERE state='ISSUED' ORDER BY updated_at"""
            ).fetchall()
        if len(rows) > 1:
            raise UpbitFunctionalBlocked(
                "upbit-functional-multiple-recovery-candidates"
            )
        return dict(rows[0]) if rows else None

    def recovery_authority_pointer(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM upbit_functional_recovery_approvals
                WHERE state IN ('ISSUED','APPROVED','CLAIMED')
                ORDER BY updated_at"""
            ).fetchall()
        if len(rows) > 1:
            raise UpbitFunctionalBlocked(
                "upbit-functional-multiple-recovery-authorities"
            )
        return dict(rows[0]) if rows else None

    def retire_recovery_authority(
        self,
        *,
        recovery_id: str,
        expected_states: tuple[str, ...],
        detail: str,
    ) -> None:
        normalized = tuple(_text(state).upper() for state in expected_states)
        if not normalized or any(
            state not in {"ISSUED", "APPROVED", "CLAIMED"}
            for state in normalized
        ):
            raise ValueError("invalid recovery retirement states")
        placeholders = ",".join("?" for _ in normalized)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                f"""UPDATE upbit_functional_recovery_approvals
                SET state='EXPIRED',detail=?,updated_at=?
                WHERE recovery_id=? AND state IN ({placeholders})""",
                (
                    _text(detail)[:500],
                    _utc(self.clock(), "current-time").isoformat(),
                    _text(recovery_id),
                    *normalized,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-retirement-raced"
                )
            connection.commit()

    def approve_issued_recovery(
        self,
        *,
        recovery_id: str,
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Replace an inert candidate with one fresh REST-sealed approval."""

        now = _utc(self.clock(), "current-time")
        content_hash = _text(record.get("contentHash")).lower()
        if (
            _text(record.get("recoveryId")) != _text(recovery_id)
            or _HASH_RE.fullmatch(content_hash) is None
            or not secrets.compare_digest(
                content_hash,
                _stable_hash(
                    {
                        key: value
                        for key, value in record.items()
                        if key != "contentHash"
                    }
                ),
            )
            or _text(record.get("schemaVersion"))
            != "upbit-functional-recovery-approval/v1"
            or _text(record.get("mode")) != "CLEANUP_ONLY"
            or _text(record.get("approvalState")) != "ACTIVE"
            or record.get("serverManaged") is not True
            or record.get("operatorAuthenticated") is not True
            or record.get("operatorApproved") is not True
            or record.get("singleUse") is not True
            or record.get("previousOwnerLost") is not True
            or record.get("previousOwnerLeaseExpired") is not True
            or record.get("officialRestReconciled") is not True
            or _HASH_RE.fullmatch(
                _text(record.get("officialRestTruthHash")).lower()
            )
            is None
            or _HASH_RE.fullmatch(
                _text(record.get("previousOwnerLeaseEvidenceHash")).lower()
            )
            is None
            or not self.operator_verifier(dict(record))
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-candidate-approval-invalid"
            )
        observed_at = _utc(record.get("observedAt"), "recovery-observed-at")
        if (
            (now - observed_at).total_seconds() < 0
            or (now - observed_at).total_seconds() > 15
            or int(record.get("previousWriterGeneration") or 0) <= 0
            or int(record.get("nextWriterGeneration") or 0)
            != int(record.get("previousWriterGeneration") or 0) + 1
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-approval-evidence-invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            candidate = connection.execute(
                """SELECT * FROM upbit_functional_recovery_approvals
                WHERE recovery_id=? AND state='ISSUED'""",
                (_text(recovery_id),),
            ).fetchone()
            if candidate is None:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-candidate-not-issuable"
                )
            candidate_record = json.loads(candidate["recovery_json"])
            exact = {
                "sessionId": candidate["session_id"],
                "permitId": candidate["permit_id"],
                "permitHash": candidate["permit_hash"],
                "accountFingerprint": candidate_record.get(
                    "accountFingerprint"
                ),
                "previousWriterGeneration": candidate_record.get(
                    "previousWriterGeneration"
                ),
                "nextWriterGeneration": candidate_record.get(
                    "nextWriterGeneration"
                ),
                "previousOwnerLeaseEvidenceHash": candidate_record.get(
                    "previousOwnerLeaseEvidenceHash"
                ),
            }
            if any(
                str(record.get(field)) != str(expected)
                for field, expected in exact.items()
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-candidate-binding-mismatch"
                )
            main = connection.execute(
                """SELECT approval_id FROM upbit_functional_approvals
                WHERE permit_id=? AND permit_hash=? AND claimed_session_id=?
                AND account_fingerprint=? AND state='ACTIVE'""",
                (
                    exact["permitId"],
                    exact["permitHash"],
                    exact["sessionId"],
                    exact["accountFingerprint"],
                ),
            ).fetchone()
            if main is None:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-active-session-approval-missing"
                )
            cursor = connection.execute(
                """UPDATE upbit_functional_recovery_approvals
                SET recovery_hash=?,recovery_json=?,state='APPROVED',
                    detail='typed operator + fresh REST approved',updated_at=?
                WHERE recovery_id=? AND state='ISSUED'""",
                (
                    content_hash,
                    json.dumps(
                        dict(record),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                    now.isoformat(),
                    _text(recovery_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-candidate-approval-raced"
                )
            connection.commit()
        return {"recoveryId": _text(recovery_id), "recoveryHash": content_hash}

    def claim_recovery(
        self, *, recovery_id: str, session_id: str
    ) -> dict[str, Any]:
        now = _utc(self.clock(), "current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            candidate = connection.execute(
                """SELECT * FROM upbit_functional_recovery_approvals
                WHERE recovery_id=? AND session_id=? AND state='APPROVED'""",
                (_text(recovery_id), _text(session_id)),
            ).fetchone()
            if candidate is None:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-approval-not-claimable"
                )
            record = json.loads(candidate["recovery_json"])
            observed_at = _utc(
                record.get("observedAt"), "recovery-claim-observed-at"
            )
            age = (now - observed_at).total_seconds()
            if age < 0 or age > 15:
                connection.execute(
                    """UPDATE upbit_functional_recovery_approvals
                    SET state='EXPIRED',detail='fresh REST approval expired',
                        updated_at=? WHERE recovery_id=? AND state='APPROVED'""",
                    (now.isoformat(), _text(recovery_id)),
                )
                connection.commit()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-approval-stale"
                )
            cursor = connection.execute(
                """UPDATE upbit_functional_recovery_approvals
                SET state='CLAIMED',detail='recovery claimed',updated_at=?
                WHERE recovery_id=? AND session_id=? AND state='APPROVED'""",
                (
                    now.isoformat(),
                    _text(recovery_id),
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-approval-not-claimable"
                )
            row = connection.execute(
                """SELECT * FROM upbit_functional_recovery_approvals
                WHERE recovery_id=?""",
                (_text(recovery_id),),
            ).fetchone()
            connection.commit()
        return json.loads(row["recovery_json"])

    def recovery_reader(
        self, recovery_id: str, recovery_hash: str
    ) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT * FROM upbit_functional_recovery_approvals
                WHERE recovery_id=?""",
                (_text(recovery_id),),
            ).fetchone()
        if (
            row is None
            or row["state"] != "CLAIMED"
            or not secrets.compare_digest(
                row["recovery_hash"], _text(recovery_hash).lower()
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-recovery-active-pointer-missing"
            )
        return json.loads(row["recovery_json"])

    def finish_recovery(
        self, *, recovery_id: str, state: str, detail: str
    ) -> None:
        normalized = _text(state).upper()
        if normalized not in {"CONSUMED", "FAILED"}:
            raise ValueError("invalid recovery terminal state")
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """UPDATE upbit_functional_recovery_approvals
                SET state=?,detail=?,updated_at=?
                WHERE recovery_id=? AND state='CLAIMED'""",
                (
                    normalized,
                    _text(detail)[:500],
                    _utc(self.clock(), "current-time").isoformat(),
                    _text(recovery_id),
                ),
            )
            if cursor.rowcount != 1:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-recovery-approval-transition-invalid"
                )
            connection.commit()

    def audit_startup(
        self,
        *,
        ledger_sessions: Mapping[str, str | Mapping[str, Any]],
        ledger_claims: Mapping[str, tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]]] | None = None,
        journal_sessions: Mapping[str, Mapping[str, Any]] | None = None,
        owner_session_ids: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Audit every unconsumed approval against exact durable authority.

        An APPROVED pointer has no legitimate broker authority yet.  On a
        process restart it is retired only when there is exact proof that no
        session, claim/post marker, private-stream owner, or process owner was
        ever attached.  Any contradictory residue is reported for manual
        reconciliation instead of silently reopening ordinary order routes.
        """

        actions: list[dict[str, str]] = []
        claims_by_session = {
            _text(session_id): tuple(dict(row) for row in rows)
            for session_id, rows in dict(ledger_claims or {}).items()
        }
        journals_by_session = {
            _text(session_id): dict(row)
            for session_id, row in dict(journal_sessions or {}).items()
        }
        owner_ids = {_text(value) for value in owner_session_ids if _text(value)}
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """SELECT * FROM upbit_functional_approvals
                WHERE state IN ('APPROVED','CLAIMED','ACTIVE')"""
            ).fetchall()
            for row in rows:
                if row["state"] == "APPROVED":
                    matching_sessions: list[tuple[str, Mapping[str, Any]]] = []
                    for candidate_session_id, candidate_value in ledger_sessions.items():
                        candidate = (
                            dict(candidate_value)
                            if isinstance(candidate_value, Mapping)
                            else {}
                        )
                        if candidate and (
                            _text(candidate.get("permit_id"))
                            == _text(row["permit_id"])
                            or secrets.compare_digest(
                                _text(candidate.get("permit_hash")).lower(),
                                _text(row["permit_hash"]).lower(),
                            )
                        ):
                            matching_sessions.append(
                                (_text(candidate_session_id), candidate)
                            )
                    matching_session_ids = {
                        session_id for session_id, _candidate in matching_sessions
                    }
                    matching_claims = any(
                        claims_by_session.get(session_id)
                        for session_id in matching_session_ids
                    )
                    matching_or_account_journal = any(
                        session_id in matching_session_ids
                        or secrets.compare_digest(
                            _text(journal.get("account_fingerprint")).lower(),
                            _text(row["account_fingerprint"]).lower(),
                        )
                        for session_id, journal in journals_by_session.items()
                    )
                    side_effect_present = bool(
                        _text(row["claimed_session_id"])
                        or matching_sessions
                        or owner_ids
                        or matching_claims
                        or matching_or_account_journal
                    )
                    if side_effect_present:
                        actions.append(
                            {
                                "approvalId": row["approval_id"],
                                "action": "APPROVED_SIDE_EFFECT_PROOF_BLOCKED",
                            }
                        )
                        continue
                    try:
                        approved_permit = parse_functional_test_permit(
                            json.loads(row["permit_json"])
                        )
                        exact_permit = bool(
                            approved_permit.permit_id == row["permit_id"]
                            and secrets.compare_digest(
                                approved_permit.content_hash,
                                _text(row["permit_hash"]).lower(),
                            )
                            and secrets.compare_digest(
                                approved_permit.binding.account_id,
                                _text(row["account_fingerprint"]).lower(),
                            )
                        )
                    except Exception:
                        exact_permit = False
                        approved_permit = None
                    if not exact_permit or approved_permit is None:
                        actions.append(
                            {
                                "approvalId": row["approval_id"],
                                "action": "APPROVED_PERMIT_IDENTITY_BLOCKED",
                            }
                        )
                        continue
                    now = _utc(self.clock(), "current-time")
                    expired = now >= approved_permit.ends_at
                    terminal = "EXPIRED" if expired else "FAILED"
                    cursor = connection.execute(
                        """UPDATE upbit_functional_approvals
                        SET state=?,detail=?,updated_at=?
                        WHERE approval_id=? AND state='APPROVED'
                        AND claimed_session_id=''""",
                        (
                            terminal,
                            (
                                "startup retired expired unclaimed approval"
                                if expired
                                else "startup retired unclaimed approval before owner"
                            ),
                            now.isoformat(),
                            row["approval_id"],
                        ),
                    )
                    if cursor.rowcount != 1:
                        actions.append(
                            {
                                "approvalId": row["approval_id"],
                                "action": "APPROVED_RETIRE_RACED_BLOCKED",
                            }
                        )
                    else:
                        actions.append(
                            {
                                "approvalId": row["approval_id"],
                                "action": (
                                    "APPROVED_EXPIRED_PRECLAIM"
                                    if expired
                                    else "APPROVED_FAILED_PRECLAIM"
                                ),
                            }
                        )
                    continue
                session_id = _text(row["claimed_session_id"])
                durable_value = ledger_sessions.get(session_id)
                durable_row = (
                    dict(durable_value)
                    if isinstance(durable_value, Mapping)
                    else {}
                )
                durable = _text(
                    durable_row.get("state")
                    if durable_row
                    else durable_value
                ).upper()
                if row["state"] == "CLAIMED" and not durable:
                    connection.execute(
                        """UPDATE upbit_functional_approvals
                        SET state='FAILED',detail='startup claimed without durable session',
                            updated_at=? WHERE approval_id=? AND state='CLAIMED'""",
                        (
                            _utc(self.clock(), "current-time").isoformat(),
                            row["approval_id"],
                        ),
                    )
                    actions.append(
                        {
                            "approvalId": row["approval_id"],
                            "action": "CLAIMED_FAILED_CLOSED",
                        }
                    )
                elif row["state"] == "CLAIMED" and durable in {
                    "ACTIVE",
                    "CLEANUP",
                    "FINAL_RESET_PENDING",
                }:
                    exact_durable = bool(
                        durable_row
                        and _text(durable_row.get("permit_id"))
                        == _text(row["permit_id"])
                        and secrets.compare_digest(
                            _text(durable_row.get("permit_hash")).lower(),
                            _text(row["permit_hash"]).lower(),
                        )
                        and secrets.compare_digest(
                            _text(
                                durable_row.get("account_fingerprint")
                            ).lower(),
                            _text(row["account_fingerprint"]).lower(),
                        )
                    )
                    if not exact_durable:
                        actions.append(
                            {
                                "approvalId": row["approval_id"],
                                "action": "CLAIMED_DURABLE_MISMATCH_BLOCKED",
                            }
                        )
                    else:
                        connection.execute(
                            """UPDATE upbit_functional_approvals
                            SET state='ACTIVE',
                                detail='startup bound claimed durable cleanup',
                                updated_at=?
                            WHERE approval_id=? AND state='CLAIMED'
                            AND claimed_session_id=?""",
                            (
                                _utc(self.clock(), "current-time").isoformat(),
                                row["approval_id"],
                                session_id,
                            ),
                        )
                        actions.append(
                            {
                                "approvalId": row["approval_id"],
                                "action": "CLAIMED_BOUND_CLEANUP_ONLY",
                            }
                        )
                elif row["state"] == "ACTIVE" and durable == "FINALIZED":
                    connection.execute(
                        """UPDATE upbit_functional_approvals
                        SET state='CONSUMED',detail='startup observed finalized session',
                            updated_at=? WHERE approval_id=? AND state='ACTIVE'""",
                        (
                            _utc(self.clock(), "current-time").isoformat(),
                            row["approval_id"],
                        ),
                    )
                    actions.append(
                        {
                            "approvalId": row["approval_id"],
                            "action": "ACTIVE_CONSUMED",
                        }
                    )
                elif row["state"] == "ACTIVE" and durable not in {
                    "ACTIVE",
                    "CLEANUP",
                    "FINAL_RESET_PENDING",
                }:
                    actions.append(
                        {
                            "approvalId": row["approval_id"],
                            "action": "ACTIVE_MISMATCH_BLOCKED",
                        }
                    )
            # Recovery authority never survives a process-owner loss.  Its
            # REST observation and writer generation were freshness-bound to
            # the dead owner, so startup retires every nonterminal recovery
            # record and requires a new server-minted candidate/REST proof.
            recovery_rows = connection.execute(
                """SELECT * FROM upbit_functional_recovery_approvals
                WHERE state IN ('ISSUED','APPROVED','CLAIMED')"""
            ).fetchall()
            for recovery in recovery_rows:
                session_id = _text(recovery["session_id"])
                durable_value = ledger_sessions.get(session_id)
                durable_row = (
                    dict(durable_value)
                    if isinstance(durable_value, Mapping)
                    else {}
                )
                durable_state = _text(
                    durable_row.get("state")
                    if durable_row
                    else durable_value
                ).upper()
                exact = bool(
                    durable_row
                    and durable_state in {"CLEANUP", "FINAL_RESET_PENDING"}
                    and _text(durable_row.get("permit_id"))
                    == _text(recovery["permit_id"])
                    and secrets.compare_digest(
                        _text(durable_row.get("permit_hash")).lower(),
                        _text(recovery["permit_hash"]).lower(),
                    )
                )
                terminal = "EXPIRED" if exact else "FAILED"
                detail = (
                    "startup retired owner-bound recovery; fresh proof required"
                    if exact
                    else "startup recovery durable identity mismatch"
                )
                connection.execute(
                    """UPDATE upbit_functional_recovery_approvals
                    SET state=?,detail=?,updated_at=?
                    WHERE recovery_id=? AND state=?""",
                    (
                        terminal,
                        detail,
                        _utc(self.clock(), "current-time").isoformat(),
                        recovery["recovery_id"],
                        recovery["state"],
                    ),
                )
                actions.append(
                    {
                        "recoveryId": recovery["recovery_id"],
                        "action": (
                            "RECOVERY_OWNER_PROOF_RETIRED"
                            if exact
                            else "RECOVERY_DURABLE_MISMATCH_BLOCKED"
                        ),
                    }
                )
            connection.commit()
        return {
            "complete": not any(
                row["action"]
                in {
                    "ACTIVE_MISMATCH_BLOCKED",
                    "CLAIMED_DURABLE_MISMATCH_BLOCKED",
                    "APPROVED_SIDE_EFFECT_PROOF_BLOCKED",
                    "APPROVED_PERMIT_IDENTITY_BLOCKED",
                    "APPROVED_RETIRE_RACED_BLOCKED",
                    "RECOVERY_DURABLE_MISMATCH_BLOCKED",
                }
                for row in actions
            ),
            "actions": actions,
        }


__all__ = [
    "DurableUpbitFunctionalApprovalStore",
    "UPBIT_FUNCTIONAL_OWNER_LEASE_SCHEMA_VERSION",
    "UPBIT_FUNCTIONAL_OWNER_LEASE_SECONDS",
    "_durable_functional_wiring_complete",
    "_functional_wiring_evidence_complete",
]
