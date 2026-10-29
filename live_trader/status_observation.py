"""Pure cached status projections. No network, stores, authority or mutations."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

DEFAULT_OBSERVATION_MAX_AGE_SECONDS = 60
BROKER_IDS = ("kis", "binance", "binance-futures", "upbit")


class DeploymentContextError(ValueError):
    """Source/route/approval context failed before a governance store access."""


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _rows(value: object) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, (list, tuple)) else []


def _ids(value: object) -> set[str]:
    return {str(item).lower() for item in value} if isinstance(value, (list, tuple, set)) else set()


def observation_age_seconds(value: object, now: datetime | None = None) -> float | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
        current = now or datetime.now().astimezone()
        if parsed.tzinfo is None:
            current = current.astimezone().replace(tzinfo=None) if current.tzinfo else current
        else:
            current = current.astimezone(parsed.tzinfo)
        age = (current - parsed).total_seconds()
        return age if age >= 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def broker_account_observation(cache: Mapping[str, Any], broker_id: str, *,
                               now: datetime | None = None,
                               maximum_age_seconds: int = DEFAULT_OBSERVATION_MAX_AGE_SECONDS) -> dict[str, Any]:
    broker_id = str(broker_id or "").strip().lower()
    record = _mapping(_mapping(cache.get("account_observations")).get(broker_id))
    observed = str(record.get("observedAt") or "")
    last_success = str(record.get("lastSuccessAt") or observed)
    last_attempt = str(record.get("lastAttemptAt") or observed)
    age = observation_age_seconds(observed, now)
    errors = [row for row in _rows(cache.get("errors")) if str(row.get("broker_id") or "").lower() == broker_id
              and str(row.get("scope") or "snapshot") in {"snapshot", "account"}]
    success = broker_id in _ids(cache.get("successful_account_brokers"))
    has_account_row = any(str(row.get("broker_id") or "").lower() == broker_id for row in _rows(cache.get("accounts")))
    known = bool(success and has_account_row and observed and age is not None and not errors)
    fresh = bool(known and age <= maximum_age_seconds)
    status = "error" if errors else "success" if fresh else "stale" if known else "unknown"
    return {"observed_at": observed, "last_success_at": last_success, "last_attempt_at": last_attempt,
            "lookup_status": status, "observation_age_seconds": age,
            "observation_max_age_seconds": maximum_age_seconds, "account_observation_fresh": fresh,
            "observation_source": "BROKER_ACCOUNT_SNAPSHOT" if observed else "UNVERIFIED",
            "authority_granted": False}


def explicit_strategy_broker_id(strategy: Mapping[str, Any]) -> str:
    """Presentation-only route identity: never infer from symbol/asset/market."""
    contract = _mapping(strategy.get("traderContract") or strategy.get("trader_contract"))
    scope = _mapping(contract.get("scope"))
    explicit = [str(value).strip().lower() for value in (
        strategy.get("brokerId"), strategy.get("broker_id"), strategy.get("route"), scope.get("brokerId")) if value]
    if explicit:
        return explicit[0] if len(set(explicit)) == 1 and explicit[0] in BROKER_IDS else ""
    dataset = _mapping(strategy.get("dataset"))
    artifact = _mapping(strategy.get("dataArtifact") or strategy.get("data_artifact"))
    providers = [str(value).strip().lower() for value in (
        strategy.get("provider"), dataset.get("provider"), strategy.get("dataset_provider"),
        strategy.get("marketDataProvider"), strategy.get("market_data_provider"),
        artifact.get("marketDataProvider"), artifact.get("provider")) if value]
    if not providers or len(set(providers)) != 1 or providers[0] not in BROKER_IDS:
        return ""
    market = str(strategy.get("marketType") or strategy.get("market_type") or dataset.get("marketType")
                 or dataset.get("market_type") or artifact.get("marketType") or artifact.get("market_type") or "").lower()
    if providers[0] == "binance" and market in {"future", "futures", "perpetual"}:
        return ""  # Binance data provider alone does not identify the execution adapter.
    return providers[0]


def strategy_execution_observation_scope(strategy: Mapping[str, Any]) -> str:
    """Only explicit domestic venue/region metadata can request KIS coverage."""
    if explicit_strategy_broker_id(strategy) != "kis":
        return "UNVERIFIED"
    portfolio = _mapping(strategy.get("portfolio_gate"))
    if any(portfolio.get(key) for key in ("portfolioId", "portfolio_id", "members", "strategyInstances")):
        return "UNVERIFIED"  # A single row cannot certify every portfolio member's market.
    contract = _mapping(strategy.get("traderContract") or strategy.get("trader_contract"))
    records = [strategy, _mapping(strategy.get("dataset")), _mapping(strategy.get("dataArtifact") or strategy.get("data_artifact")),
               contract, _mapping(contract.get("scope")), _mapping(strategy.get("parameters")), _mapping(strategy.get("execution"))]
    venues = [str(row.get(key)).strip().upper() for row in records for key in ("exchange", "venue", "market", "region", "country") if row.get(key)]
    currencies = [str(row.get(key)).strip().upper() for row in records for key in ("currency", "quoteCurrency", "capitalCurrency") if row.get(key)]
    if not venues or any(value not in {"KRX", "XKRX", "KR", "KOREA", "DOMESTIC"} for value in venues):
        return "UNVERIFIED"
    if any(value != "KRW" for value in currencies):
        return "UNVERIFIED"
    return "KIS_DOMESTIC_ONLY"


def broker_execution_read_coverage(broker_id: str, result: Mapping[str, Any], *,
                                   validated_kis_order_truth: bool = False) -> dict[str, str]:
    """Classify the actual existing adapter boundary; no new read is attempted."""
    source = str(result.get("source") or "")
    complete = bool(broker_id == "kis" and result.get("broker_id") == "kis"
                    and source == "kis_balance_and_domestic_order_truth_poll" and validated_kis_order_truth)
    snapshot_only = source in {"binance_account_poll", "binance_futures_account_poll", "upbit_accounts_poll"}
    return {"status": "success" if complete else "unverified", "source": source,
            "coverage": "COMPLETE_ORDER_EXECUTION_READ" if complete else "ACCOUNT_POSITION_SNAPSHOT_ONLY" if snapshot_only else "UNVERIFIED",
            "scope": "KIS_DOMESTIC_ONLY" if complete else "ACCOUNT_POSITION_ONLY" if snapshot_only else "UNVERIFIED",
            "detail": ("기존 KIS 국내 주문·체결 조회와 페이지 완료를 확인했습니다. 해외 체결 조회 근거는 아닙니다." if complete else
                       "이 읽기는 계좌·포지션 조회입니다. 체결 전체 조회나 빈 체결 결과의 확인 근거가 아닙니다." if snapshot_only else
                       "기존 어댑터의 완전한 주문·체결 조회 근거가 확인되지 않았습니다.")}


def completed_broker_poll_observations(cache: Mapping[str, Any], completed_brokers: set[str],
                                      previous: Mapping[str, Any], errors: list[Mapping[str, Any]], *,
                                      observed_at: str, execution_reads: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Called only after a full broker poll and actual ledger event processing.

    These records describe a read and processing boundary, not fills, websocket
    connectivity, a ledger baseline, a human review or execution permission.
    """
    result = {str(key): dict(_mapping(value)) for key, value in _mapping(previous).items()}
    failed = {str(row.get("broker_id") or "").lower() for row in errors}
    for broker_id in completed_brokers - failed:
        read = _mapping(_mapping(execution_reads).get(broker_id))
        if read.get("status") != "success" or read.get("coverage") != "COMPLETE_ORDER_EXECUTION_READ" or not read.get("source"):
            continue
        account = _mapping(_mapping(cache.get("account_observations")).get(broker_id))
        position = _mapping(_mapping(cache.get("position_observations")).get(broker_id))
        if not account.get("observedAt") or not position.get("observedAt"):
            continue
        result[broker_id] = {"status": "success", "observedAt": observed_at,
                             "accountObservedAt": account["observedAt"], "positionObservedAt": position["observedAt"],
                             "source": "BROKER_FULL_POLL_AND_LEDGER_EVENT_PROCESSING",
                             "executionReadCoverage": read["coverage"], "executionReadSource": read["source"],
                             "executionReadScope": str(read.get("scope") or "UNVERIFIED")}
    for broker_id in failed:
        result[broker_id] = {**result.get(broker_id, {}), "status": "error", "lastAttemptAt": observed_at}
    return result


def reconciliation_for_execution_scope(reconciliation: Mapping[str, Any], broker_id: str,
                                       required_execution_scope: str) -> dict[str, Any]:
    """Select by explicit reconciliation currency only after source scope is known.

    USD facts stay in the global view. Missing/other currencies stay in this
    selected view and must remain unverified; no symbol or balance inference.
    """
    result = dict(reconciliation)
    for collection in ("accounts", "positions"):
        rows = [row for row in _rows(reconciliation.get(collection)) if str(row.get("broker_id") or "").lower() == broker_id]
        if broker_id == "kis" and required_execution_scope == "KIS_DOMESTIC_ONLY":
            rows = [row for row in rows if str(row.get("currency") or "").upper() != "USD"]
        result[collection] = rows
    return result


def cached_preflight_reconciliation(broker_id: str, reconciliation: Mapping[str, Any],
                                    cache: Mapping[str, Any], execution: Mapping[str, Any], ledger: Mapping[str, Any], *,
                                    now: datetime | None = None,
                                    maximum_age_seconds: int = DEFAULT_OBSERVATION_MAX_AGE_SECONDS,
                                    required_execution_scope: str = "UNVERIFIED") -> dict[str, Any]:
    """Project the exact broker's cached facts; real admission still rereads."""
    broker_id = str(broker_id or "").strip().lower()
    reconciliation = reconciliation_for_execution_scope(reconciliation, broker_id, required_execution_scope)
    accounts = [row for row in _rows(reconciliation.get("accounts")) if str(row.get("broker_id") or "").lower() == broker_id]
    positions = [row for row in _rows(reconciliation.get("positions")) if str(row.get("broker_id") or "").lower() == broker_id]
    errors = [row for row in _rows(reconciliation.get("errors")) if str(row.get("broker_id") or "").lower() == broker_id]
    execution_errors = [row for row in _rows(execution.get("errors")) if str(row.get("broker_id") or "").lower() == broker_id]
    rows = accounts + positions
    known_statuses = {"pass", "api_required", "mismatch", "capability_unavailable"}
    unknown = sum(row.get("status") not in known_statuses for row in rows)
    missing = sum(row.get("status") == "api_required" for row in rows) + (1 if errors or not accounts else 0)
    mismatch = sum(row.get("status") == "mismatch" for row in rows)
    unsupported = sum(row.get("status") == "capability_unavailable" for row in rows)
    account = broker_account_observation(cache, broker_id, now=now, maximum_age_seconds=maximum_age_seconds)
    position = _mapping(_mapping(cache.get("position_observations")).get(broker_id))
    event_observation = _mapping(_mapping(execution.get("broker_observations")).get(broker_id))
    ledger_observation = _mapping(_mapping(ledger.get("broker_observations")).get(broker_id))
    # Global last_poll/last_event_sync are attempts and can belong to another
    # broker or a skipped poll. Never use them as exact-broker success evidence.
    times = {"account_snapshot": account["observed_at"], "broker_snapshot": position.get("observedAt"),
             "execution_event": event_observation.get("observedAt"), "local_ledger": ledger_observation.get("observedAt")}
    ages = {key: observation_age_seconds(value, now) for key, value in times.items()}
    issues = []
    if broker_id not in BROKER_IDS:
        issues.append("deployment-broker-route-unverified")
    if not accounts:
        issues.append("broker-account-reconciliation-row-missing")
    if unknown:
        issues.append("reconciliation-row-status-unverified")
    if account["lookup_status"] != "success":
        issues.append("broker-account-observation-" + account["lookup_status"])
    if broker_id not in _ids(cache.get("successful_position_brokers")):
        issues.append("broker-position-snapshot-unverified")
    if errors or execution_errors:
        issues.append("broker-or-execution-poll-error")
    read_coverage = _mapping(_mapping(execution.get("read_coverage")).get(broker_id))
    if read_coverage.get("coverage") == "ACCOUNT_POSITION_SNAPSHOT_ONLY":
        issues.append("account-position-snapshot-is-not-execution-read")
    if not accounts or any(not str(row.get("program_source") or "").strip()
                           or str(row.get("program_source") or "").lower() in {"missing", "unknown", "unverified", "none"}
                           or observation_age_seconds(row.get("program_updated_at"), now) is None for row in accounts):
        issues.append("program-ledger-account-source-unverified")
    for key, record in (("execution_event", event_observation), ("local_ledger", ledger_observation)):
        if (record.get("status") != "success" or record.get("source") != "BROKER_FULL_POLL_AND_LEDGER_EVENT_PROCESSING"
                or record.get("executionReadCoverage") != "COMPLETE_ORDER_EXECUTION_READ" or not record.get("executionReadSource")):
            issues.append(key + "-broker-success-unverified")
        if (record.get("accountObservedAt") != times["account_snapshot"]
                or record.get("positionObservedAt") != times["broker_snapshot"]):
            issues.append(key + "-snapshot-binding-unverified")
    if broker_id == "kis" and required_execution_scope != "KIS_DOMESTIC_ONLY":
        issues.append("kis-selected-execution-scope-unverified-or-unsupported")
    if broker_id == "kis" and any(str(row.get("currency") or "").upper() != "KRW" for row in rows):
        issues.append("kis-domestic-execution-read-currency-scope-unverified")
    if event_observation.get("observedAt") != ledger_observation.get("observedAt"):
        issues.append("broker-event-ledger-processing-boundary-mismatch")
    for key, age in ages.items():
        if age is None:
            issues.append(key + "-timestamp-missing-or-future")
        elif age > maximum_age_seconds:
            issues.append(key + "-stale")
    fresh = not issues
    status = "fail" if mismatch else "warn" if missing or unsupported or unknown or issues else "pass"
    verified = bool(fresh and accounts and all(row.get("status") == "pass" for row in rows))
    summary = {"broker_id": broker_id, "status": status,
               "status_label": "불일치" if mismatch else "API 필요" if missing else "일부 미지원" if unsupported else "미확인" if unknown or issues else "정상",
               "api_required_count": missing, "mismatch_count": mismatch, "capability_gap_count": unsupported,
               "unverified_count": unknown, "blocking_count": missing + mismatch + unknown, "row_count": len(rows),
               "pass_count": sum(row.get("status") == "pass" for row in rows), "error_count": len(errors) + len(execution_errors),
               "last_run": account["observed_at"] or "미실행", "fresh": fresh, "three_way_verified": verified,
               "freshness_max_age_seconds": maximum_age_seconds, "evidence_ages_seconds": ages,
               "required_execution_scope": required_execution_scope,
               "freshness_detail": ("현재 선택한 거래소의 계좌·포지션·체결·원장 캐시 관측이 유효 시간 안에 있습니다. 실제 시작 전에는 다시 조회합니다."
                                    if verified else "현재 캐시 관측의 3자 대조 미확인: " + ", ".join(issues or ["broker-event-ledger-mismatch-or-unverified"]))}
    return {"summary": summary, "positions": positions, "accounts": accounts, "errors": errors,
            "observation_only": True, "authority_granted": False, "requires_fresh_read_for_admission": True}
