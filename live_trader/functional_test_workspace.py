from __future__ import annotations

"""Readiness-only workspace for KIS live functional tests.

This module intentionally owns no broker client and exposes no order-submit
operation.  It creates the immutable shared permit and the short-lived daily
operator activation consumed by the real pre-trade/dispatch gates elsewhere.
"""

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
from typing import Any
from zoneinfo import ZoneInfo

from trading_runtime.functional_test import (
    FUNCTIONAL_TEST_MAX_DURATION_DAYS,
    FUNCTIONAL_TEST_MAX_LIVE_ACTIVATION_HOURS,
    FUNCTIONAL_TEST_US_MAX_GROSS_EXPOSURE_USD,
    FUNCTIONAL_TEST_US_MAX_LOSS_USD,
    FUNCTIONAL_TEST_US_MAX_ORDER_NOTIONAL_USD,
    FunctionalTestBinding,
    FunctionalTestCaps,
    FunctionalTestContractError,
    FunctionalTestDurationUnit,
    FunctionalTestEnvironment,
    assert_functional_test_permit_active,
    default_functional_test_root,
    issue_functional_test_permit,
    issue_live_activation_token,
    parse_functional_test_permit,
    parse_live_activation_token,
    read_functional_test_document,
    write_functional_test_document,
)
from trading_runtime.market_calendar import session_bounds_utc


FUNCTIONAL_TEST_WORKSPACE_SCHEMA_VERSION = "live-functional-test-workspace-v1"
FUNCTIONAL_TEST_CONTROL_SCHEMA_VERSION = "live-functional-test-control-v1"
FUNCTIONAL_TEST_ACCOUNT_SCHEMA_VERSION = "kis-account-binding-v1"
FUNCTIONAL_TEST_ENVIRONMENT = "KIS_LIVE"
KST = ZoneInfo("Asia/Seoul")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DOMESTIC_SYMBOL = re.compile(r"^\d{6}$")
_US_SYMBOL = re.compile(r"^[A-Z]{1,8}$")
_SAFE_PERMIT_ID = re.compile(r"^[A-Za-z0-9._:-]{1,160}$")
# This flips only after the live account-wide order truth, owned cancel/SELL,
# and durable final-flat coordinator pass their integration suite.  Keeping
# the candidate visible while blocked lets operators see the exact missing
# release contract without granting real-money authority.
FUNCTIONAL_TEST_US_LIVE_AVAILABLE = False
LIVE_FUNCTIONAL_TEST_CAPS = FunctionalTestCaps(
    max_order_quantity=1,
    max_order_notional=100_000.0,
    max_gross_exposure=300_000.0,
    max_orders=20,
    max_open_positions=3,
    max_loss=20_000.0,
)
US_LIVE_FUNCTIONAL_TEST_CAPS = FunctionalTestCaps(
    max_order_quantity=1,
    max_order_notional=FUNCTIONAL_TEST_US_MAX_ORDER_NOTIONAL_USD,
    max_gross_exposure=FUNCTIONAL_TEST_US_MAX_GROSS_EXPOSURE_USD,
    max_orders=2,
    max_open_positions=1,
    max_loss=FUNCTIONAL_TEST_US_MAX_LOSS_USD,
)


def canonical_kis_domestic_symbol(value: object) -> str:
    """Return the six-digit KIS cash symbol or an empty string."""

    symbol = str(value or "").strip().upper()
    if symbol.startswith("KRX:"):
        symbol = symbol[4:]
    if symbol.endswith((".KS", ".KQ")):
        symbol = symbol[:-3]
    return symbol if _DOMESTIC_SYMBOL.fullmatch(symbol) else ""


def canonical_kis_us_symbol(value: object) -> str:
    """Return an unqualified US cash-equity ticker or an empty string."""

    symbol = str(value or "").strip().upper()
    for prefix in ("NYSE:", "NASDAQ:", "AMEX:"):
        if symbol.startswith(prefix):
            symbol = symbol[len(prefix):]
            break
    return symbol if _US_SYMBOL.fullmatch(symbol) else ""


def _us_exchange(value: Mapping[str, Any]) -> str:
    parameters = value.get("parameters") if isinstance(value.get("parameters"), Mapping) else {}
    trader_contract = (
        value.get("traderContract")
        if isinstance(value.get("traderContract"), Mapping)
        else value.get("trader_contract")
        if isinstance(value.get("trader_contract"), Mapping)
        else {}
    )
    raw = str(
        value.get("exchange")
        or value.get("exchangeCode")
        or trader_contract.get("exchange")
        or parameters.get("exchange")
        or ""
    ).strip().upper()
    return {"NYS": "NYSE", "NAS": "NASD", "AMS": "AMEX"}.get(raw, raw)


def kis_account_binding_id(account_no: object, product_code: object) -> str:
    """Create a stable non-secret identity used at every safety gate."""

    account = str(account_no or "").strip().replace("-", "")
    product = str(product_code or "").strip()
    if not account or not product:
        return ""
    encoded = json.dumps(
        {
            "schemaVersion": FUNCTIONAL_TEST_ACCOUNT_SCHEMA_VERSION,
            "broker": "kis",
            "accountNo": account,
            "productCode": product,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "kis-account:" + hashlib.sha256(encoded).hexdigest()[:24]


def _masked_account(account_no: object, product_code: object) -> str:
    account = str(account_no or "").strip().replace("-", "")
    product = str(product_code or "").strip()
    if not account:
        return "KIS 계좌 미설정"
    visible = account[-2:] if len(account) >= 2 else account
    return f"KIS ******{visible}-{product}" if product else f"KIS ******{visible}"


def _default_catalog() -> dict[str, Any]:
    # Import lazily so server initialization cannot create a state/workspace
    # import cycle and tests can inject a deterministic catalog.
    from . import state  # pylint: disable=import-outside-toplevel

    return {
        "strategies": state.strategy_rows(),
        "portfolios": state.portfolio_rows(),
    }


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _reference(value: object) -> tuple[str, str]:
    source = value if isinstance(value, Mapping) else {}
    artifact_id = str(source.get("artifactId") or "").strip()
    artifact_hash = str(source.get("artifactHash") or "").strip().lower()
    return artifact_id, artifact_hash


def _integrity_blockers(value: object, artifact_hash: str) -> list[str]:
    blockers: list[str] = []
    integrity = value if isinstance(value, Mapping) else {}
    if integrity.get("valid") is not True:
        blockers.append("artifact-integrity-invalid")
    if not artifact_hash or _SHA256.fullmatch(artifact_hash) is None:
        blockers.append("artifact-sha256-required")
    return blockers


def _route_sealed_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(binding)
    route_scope = {
        "marketGroup": str(result.get("marketGroup") or ""),
        "executionRoute": str(result.get("executionRoute") or ""),
        "settlementCurrency": str(result.get("settlementCurrency") or ""),
        "symbolRoutes": list(result.get("symbolRoutes") or []),
    }
    result["routeScopeHash"] = hashlib.sha256(
        json.dumps(
            route_scope,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return result


def _strategy_candidate(strategy: Mapping[str, Any], account_id: str) -> dict[str, Any] | None:
    raw_symbol = (
        strategy.get("execution_instrument")
        or strategy.get("instrument_id")
        or strategy.get("symbol")
    )
    domestic_symbol = canonical_kis_domestic_symbol(raw_symbol)
    us_symbol = canonical_kis_us_symbol(raw_symbol) if not domestic_symbol else ""
    symbol = domestic_symbol or us_symbol
    broker = str(strategy.get("broker_id") or "").strip().lower()
    if broker and "kis" not in broker:
        return None
    if not symbol:
        return None

    artifact_id, artifact_hash = _reference(strategy.get("artifact_reference"))
    strategy_id = str(strategy.get("strategy_id") or artifact_id or "").strip()
    instance_id = str(
        strategy.get("strategy_instance_id")
        or strategy.get("instance_id")
        or (f"standalone:{strategy_id}" if strategy_id else "")
    ).strip()
    blockers = _integrity_blockers(strategy.get("artifact_integrity"), artifact_hash)
    if not artifact_id:
        blockers.append("strategy-artifact-id-required")
    if not instance_id:
        blockers.append("strategy-instance-id-required")
    if not account_id:
        blockers.append("kis-account-binding-required")
    market_group = "US_STOCK" if us_symbol else "KR_STOCK"
    exchange = _us_exchange(strategy) if us_symbol else "KRX"
    timeframe = str(strategy.get("timeframe") or "-").strip().lower()
    if us_symbol:
        if symbol != "F":
            blockers.append("functional-test-us-target-must-be-F")
        if exchange != "NYSE":
            blockers.append("functional-test-us-exchange-must-be-NYSE")
        if timeframe != "5m":
            blockers.append("functional-test-us-timeframe-must-be-5m")
        if not FUNCTIONAL_TEST_US_LIVE_AVAILABLE:
            blockers.append("functional-test-us-live-final-flat-not-released")
    key = f"strategy:{artifact_id}:{instance_id}:{artifact_hash}"
    binding = {
        "strategyArtifactId": artifact_id,
        "strategyArtifactHash": artifact_hash,
        "strategyInstanceId": instance_id,
        "portfolioRequired": False,
        "portfolioArtifactId": "",
        "portfolioArtifactHash": "",
        "portfolioInstanceId": "",
        "accountId": account_id,
        "symbols": [symbol],
        "marketGroup": "KR_STOCK",
        "executionRoute": "KIS_KR_DEMO_CONTINUOUS",
        "settlementCurrency": "KRW",
        "exchanges": ["KRX"],
        "symbolRoutes": [{"symbol": symbol, "exchange": "KRX"}],
    }
    if us_symbol:
        binding.update(
            {
                "marketGroup": "US_STOCK",
                "executionRoute": "KIS_US_LIVE_CONTINUOUS",
                "settlementCurrency": "USD",
                "exchanges": [exchange] if exchange else [],
                "symbolRoutes": (
                    [{"symbol": symbol, "exchange": exchange}]
                    if exchange
                    else []
                ),
            }
        )
    binding = _route_sealed_binding(binding)
    return {
        "key": key,
        "kind": "STRATEGY",
        "label": str(strategy.get("name") or strategy_id or "Strategy"),
        "strategyId": strategy_id,
        "runtimeStrategyId": strategy_id,
        "portfolioId": "",
        "timeframe": timeframe,
        "symbols": [symbol],
        "marketGroup": market_group,
        "executionRoute": (
            "KIS_US_LIVE_CONTINUOUS" if us_symbol else "KIS_KR_LIVE"
        ),
        "settlementCurrency": "USD" if us_symbol else "KRW",
        "exchanges": [exchange],
        "functionalTestCaps": (
            US_LIVE_FUNCTIONAL_TEST_CAPS.snapshot()
            if us_symbol
            else LIVE_FUNCTIONAL_TEST_CAPS.snapshot()
        ),
        "artifactId": artifact_id,
        "artifactHash": artifact_hash,
        "instanceId": instance_id,
        "binding": binding,
        "available": not blockers,
        "blockers": list(dict.fromkeys(blockers)),
    }


def _portfolio_candidate(portfolio: Mapping[str, Any], account_id: str) -> dict[str, Any] | None:
    reference_id, reference_hash = _reference(portfolio.get("artifact_reference"))
    portfolio_id = str(portfolio.get("id") or reference_id or "").strip()
    artifact_id = reference_id or portfolio_id
    raw_instances = portfolio.get("strategy_instances")
    instances = raw_instances if isinstance(raw_instances, list) else []
    raw_targets = portfolio.get("target_portfolio")
    targets = raw_targets if isinstance(raw_targets, list) else []
    raw_symbols = [
        str(
            item.get("executionInstrument")
            or item.get("instrumentId")
            or item.get("qualifiedSymbol")
            or item.get("symbol")
            or ""
        ).strip()
        for item in [*instances, *targets]
        if isinstance(item, Mapping)
    ]
    symbols = sorted(
        {
            normalized
            for symbol in raw_symbols
            if (normalized := canonical_kis_domestic_symbol(symbol))
        }
    )
    if not symbols:
        return None
    instance_id = f"functional-portfolio:{artifact_id}:{reference_hash[:12]}"
    runtime_strategy_id = next(
        (
            str(
                item.get("sourceStrategyId")
                or item.get("strategyId")
                or item.get("strategy_id")
                or ""
            ).strip()
            for item in instances
            if isinstance(item, Mapping)
            and str(
                item.get("sourceStrategyId")
                or item.get("strategyId")
                or item.get("strategy_id")
                or ""
            ).strip()
        ),
        "",
    )
    blockers = _integrity_blockers(portfolio.get("artifact_integrity"), reference_hash)
    if any(not canonical_kis_domestic_symbol(symbol) for symbol in raw_symbols):
        blockers.append("portfolio-non-domestic-sleeve-present")
    if not artifact_id:
        blockers.append("portfolio-artifact-id-required")
    if not account_id:
        blockers.append("kis-account-binding-required")
    key = f"portfolio:{artifact_id}:{instance_id}:{reference_hash}"
    binding = {
        "strategyArtifactId": "",
        "strategyArtifactHash": "",
        "strategyInstanceId": "",
        "portfolioRequired": True,
        "portfolioArtifactId": artifact_id,
        "portfolioArtifactHash": reference_hash,
        "portfolioInstanceId": instance_id,
        "accountId": account_id,
        "symbols": symbols,
        "marketGroup": "KR_STOCK",
        "executionRoute": "KIS_KR_DEMO_CONTINUOUS",
        "settlementCurrency": "KRW",
        "exchanges": ["KRX"],
        "symbolRoutes": [
            {"symbol": symbol, "exchange": "KRX"}
            for symbol in symbols
        ],
    }
    binding = _route_sealed_binding(binding)
    return {
        "key": key,
        "kind": "PORTFOLIO",
        "label": str(portfolio.get("name") or portfolio_id or "Portfolio"),
        "strategyId": "",
        "runtimeStrategyId": runtime_strategy_id,
        "portfolioId": portfolio_id,
        "timeframe": "여러 주기",
        "symbols": symbols,
        "artifactId": artifact_id,
        "artifactHash": reference_hash,
        "instanceId": instance_id,
        "binding": binding,
        "available": not blockers,
        "blockers": list(dict.fromkeys(blockers)),
    }


class FunctionalTestWorkspace:
    """Persist and expose readiness documents without submitting orders."""

    def __init__(
        self,
        *,
        root: str | Path | None = None,
        now_provider: Callable[[], datetime] | None = None,
        catalog_provider: Callable[[], Mapping[str, Any]] | None = None,
        environment_provider: Callable[[], Mapping[str, str]] | None = None,
    ) -> None:
        base = Path(root) if root is not None else default_functional_test_root()
        self.root = base / "live"
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self.catalog_provider = catalog_provider or _default_catalog
        self.environment_provider = environment_provider or (lambda: os.environ)
        self._lock = threading.RLock()

    @property
    def current_permit_path(self) -> Path:
        return self.root / "current-permit.json"

    @property
    def current_activation_path(self) -> Path:
        return self.root / "current-activation.json"

    @property
    def control_path(self) -> Path:
        return self.root / "control.json"

    def _now(self) -> datetime:
        value = self.now_provider()
        if value.tzinfo is None or value.utcoffset() is None:
            raise FunctionalTestContractError("functional-test-current-time-timezone-missing")
        return value.astimezone(timezone.utc)

    def _environment(self) -> Mapping[str, str]:
        return self.environment_provider()

    def _account(self) -> dict[str, Any]:
        env = self._environment()
        account_no = env.get("KIS_ACCOUNT_NO", "")
        product_code = env.get("KIS_ACCOUNT_PRODUCT_CODE", "")
        binding_id = kis_account_binding_id(account_no, product_code)
        missing = [
            name
            for name in (
                "KIS_APP_KEY",
                "KIS_APP_SECRET",
                "KIS_ACCOUNT_NO",
                "KIS_ACCOUNT_PRODUCT_CODE",
                "KIS_HTS_ID",
            )
            if not str(env.get(name, "")).strip()
        ]
        return {
            "broker": "kis",
            "environment": FUNCTIONAL_TEST_ENVIRONMENT,
            "label": _masked_account(account_no, product_code),
            "bindingId": binding_id,
            "credentialsReady": not missing,
            "realOrderAdapterEnabled": _truthy(
                env.get("LIVE_TRADER_ENABLE_REAL_ORDERS", "")
            ),
            "missingSettings": missing,
        }

    def _candidates(self) -> list[dict[str, Any]]:
        catalog = self.catalog_provider()
        account_id = str(self._account().get("bindingId") or "")
        strategies = catalog.get("strategies") if isinstance(catalog, Mapping) else []
        portfolios = catalog.get("portfolios") if isinstance(catalog, Mapping) else []
        candidates: list[dict[str, Any]] = []
        for strategy in strategies if isinstance(strategies, list) else []:
            if isinstance(strategy, Mapping):
                candidate = _strategy_candidate(strategy, account_id)
                if candidate is not None:
                    candidates.append(candidate)
        for portfolio in portfolios if isinstance(portfolios, list) else []:
            if isinstance(portfolio, Mapping):
                candidate = _portfolio_candidate(portfolio, account_id)
                if candidate is not None:
                    candidates.append(candidate)
        return sorted(
            candidates,
            key=lambda item: (
                item.get("kind") != "PORTFOLIO",
                str(item.get("label") or ""),
                str(item.get("key") or ""),
            ),
        )

    def _read_control(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.control_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def authority_scope(self) -> dict[str, Any]:
        """Recover the exact permit scope without requiring it to be active.

        Pause and final-end controls must keep working after a daily token or
        the permit itself expires.  The ordinary snapshot intentionally
        validates time bounds, so destructive authority changes use the raw
        revocable pointer and then the immutable permit history named by the
        control document.  A conflict is fail-closed instead of guessing.
        """

        with self._lock:
            control = self._read_control()
            control_permit_id = str(control.get("permitId") or "").strip()
            present = bool(
                self.current_permit_path.exists()
                or self.current_activation_path.exists()
                or control_permit_id
            )
            sources: list[tuple[str, Path]] = []
            if self.current_permit_path.exists():
                sources.append(("current-permit-pointer", self.current_permit_path))
            if control_permit_id and _SAFE_PERMIT_ID.fullmatch(control_permit_id):
                sources.append(
                    (
                        "immutable-permit-history",
                        self.root / "permits" / f"{control_permit_id}.json",
                    )
                )
            errors: list[str] = []
            for source, path in sources:
                if not path.exists():
                    errors.append(f"{source}-missing")
                    continue
                try:
                    permit = parse_functional_test_permit(
                        read_functional_test_document(path)
                    )
                except (FunctionalTestContractError, OSError, ValueError):
                    errors.append(f"{source}-invalid")
                    continue
                permit_id = str(permit.permit_id or "").strip()
                account_id = str(permit.binding.account_id or "").strip()
                if control_permit_id and permit_id != control_permit_id:
                    return {
                        "present": True,
                        "resolved": False,
                        "permitId": "",
                        "accountId": "",
                        "source": source,
                        "reason": "functional-test-authority-scope-conflict",
                    }
                if permit_id and account_id:
                    return {
                        "present": True,
                        "resolved": True,
                        "permitId": permit_id,
                        "accountId": account_id,
                        "source": source,
                        "reason": "",
                    }
                errors.append(f"{source}-scope-incomplete")
            return {
                "present": present,
                "resolved": False,
                "permitId": "",
                "accountId": "",
                "source": "",
                "reason": (
                    "functional-test-authority-scope-unresolved:"
                    + ",".join(errors or ["no-authority-reference"])
                ),
            }

    def _write_control(self, payload: Mapping[str, Any]) -> None:
        document = {
            "schemaVersion": FUNCTIONAL_TEST_CONTROL_SCHEMA_VERSION,
            **dict(payload),
        }
        self.control_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.control_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(
                document,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, self.control_path)

    def _read_current(self, now: datetime) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
        blockers: list[str] = []
        permit = None
        activation = None
        if self.current_permit_path.exists():
            try:
                parsed_permit = parse_functional_test_permit(
                    read_functional_test_document(self.current_permit_path)
                )
                assert_functional_test_permit_active(parsed_permit, now=now)
                permit = parsed_permit.to_dict()
            except FunctionalTestContractError as exc:
                blockers.append(exc.code)
        if self.current_activation_path.exists():
            if permit is None:
                blockers.append("functional-test-live-activation-without-permit")
            else:
                try:
                    activation = parse_live_activation_token(
                        read_functional_test_document(self.current_activation_path),
                        permit=permit,
                        now=now,
                    ).to_dict()
                except FunctionalTestContractError as exc:
                    blockers.append(exc.code)
        return permit, activation, list(dict.fromkeys(blockers))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = self._now()
            account = self._account()
            candidates = self._candidates()
            permit, activation, blockers = self._read_current(now)
            control = self._read_control()
            selected_key = str(control.get("selectedTargetKey") or "")
            current_candidate = next(
                (item for item in candidates if item.get("key") == selected_key),
                None,
            )
            if permit is not None and current_candidate is None:
                blockers.append("functional-test-current-artifact-not-found")
            elif permit is not None and current_candidate is not None:
                if current_candidate.get("binding") != permit.get("binding"):
                    blockers.append("functional-test-current-binding-changed")
            if permit is None:
                blockers.append("functional-test-permit-required")
            elif activation is None:
                blockers.append("functional-test-live-activation-required")
            if account.get("credentialsReady") is not True:
                blockers.append("kis-live-credentials-not-ready")
            if account.get("realOrderAdapterEnabled") is not True:
                blockers.append("live-order-adapter-global-lock-enabled")
            control_status = str(control.get("status") or "").upper()
            status = "STOPPED"
            if control_status == "STOP_FAILED":
                status = "STOP_FAILED"
                blockers.append("functional-test-stop-failed")
            elif control_status == "PAUSING":
                status = "PAUSING"
            elif control_status == "PAUSED":
                status = "PAUSED"
            elif (
                control_status == "ACTIVE"
                and permit is not None
                and activation is None
            ):
                status = "PAUSE_REQUIRED"
            elif permit is not None:
                status = "ACTIVE" if activation is not None else "PERMIT_READY"
            elif control_status == "STOPPED":
                status = "STOPPED"
            return {
                "ok": True,
                "schemaVersion": FUNCTIONAL_TEST_WORKSPACE_SCHEMA_VERSION,
                "environment": FUNCTIONAL_TEST_ENVIRONMENT,
                "status": status,
                "readinessOnly": True,
                "brokerSubmissionAllowed": False,
                "promotionEligible": False,
                "fullLiveAllowed": False,
                "generatedAt": now.isoformat(),
                "durationLimits": {
                    "units": ["HOURS", "DAYS"],
                    "maxDays": FUNCTIONAL_TEST_MAX_DURATION_DAYS,
                    "dailyActivationMaxHours": FUNCTIONAL_TEST_MAX_LIVE_ACTIVATION_HOURS,
                },
                "caps": LIVE_FUNCTIONAL_TEST_CAPS.snapshot(),
                "account": account,
                "candidates": candidates,
                "current": {
                    "permit": permit,
                    "activation": activation,
                    "selectedTargetKey": selected_key,
                    "authorityReferencePresent": bool(
                        self.current_permit_path.exists()
                        or self.current_activation_path.exists()
                        or str(control.get("permitId") or "").strip()
                    ),
                    "blockers": list(dict.fromkeys(blockers)),
                    "ready": bool(permit) and bool(activation) and current_candidate is not None and not blockers,
                    "pausedAt": str(control.get("pausedAt") or ""),
                    "pauseRequestedAt": str(
                        control.get("pauseRequestedAt") or ""
                    ),
                    "stoppedAt": str(control.get("stoppedAt") or ""),
                    "stopFailedAt": str(control.get("stopFailedAt") or ""),
                    "stopFailureReason": str(
                        control.get("stopFailureReason") or ""
                    ),
                },
                "notice": (
                    "허가서 생성 즉시 선택한 전체 시험 기간이 시작됩니다. "
                    "오늘 실행 정지는 permit을 유지하므로 다음 XKRX 거래일에 다시 활성화할 수 있습니다. "
                    "실제 실행은 "
                    "별도의 '기능시험 시작' 확인을 눌러야 합니다. 승급 증거로는 사용되지 않습니다."
                ),
            }

    def create_permit(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            candidates = self._candidates()
            target_key = str(payload.get("targetKey") or "").strip()
            candidate = next(
                (item for item in candidates if item.get("key") == target_key),
                None,
            )
            if candidate is None:
                return self._failure("선택한 국내주식/ETF 아티팩트를 찾을 수 없습니다.")
            if candidate.get("available") is not True:
                return self._failure(
                    "선택 아티팩트의 exact binding을 만들 수 없습니다: "
                    + ", ".join(candidate.get("blockers") or [])
                )
            is_us_live = candidate.get("marketGroup") == "US_STOCK"
            raw_duration_value = payload.get("durationValue", 6)
            if isinstance(raw_duration_value, bool) or not isinstance(
                raw_duration_value, int
            ):
                return self._failure("시험 시간은 정수여야 합니다.")
            duration_value = raw_duration_value
            duration_unit = str(payload.get("durationUnit") or "HOURS").upper()
            if is_us_live and (duration_value != 2 or duration_unit != "HOURS"):
                return self._failure(
                    "미국주식 실전 기능시험은 안전 계약상 정확히 2시간만 허용합니다."
                )
            binding_payload = candidate["binding"]
            symbol_routes = binding_payload.get("symbolRoutes")
            symbol_routes = symbol_routes if isinstance(symbol_routes, list) else []
            binding = FunctionalTestBinding(
                strategy_artifact_id=str(binding_payload["strategyArtifactId"]),
                strategy_artifact_hash=str(binding_payload["strategyArtifactHash"]),
                strategy_instance_id=str(binding_payload["strategyInstanceId"]),
                portfolio_required=binding_payload["portfolioRequired"] is True,
                portfolio_artifact_id=str(binding_payload["portfolioArtifactId"]),
                portfolio_artifact_hash=str(binding_payload["portfolioArtifactHash"]),
                portfolio_instance_id=str(binding_payload["portfolioInstanceId"]),
                account_id=str(binding_payload["accountId"]),
                symbols=tuple(str(item) for item in binding_payload["symbols"]),
                market_group=str(binding_payload.get("marketGroup") or ""),
                execution_route=str(binding_payload.get("executionRoute") or ""),
                settlement_currency=str(
                    binding_payload.get("settlementCurrency") or ""
                ),
                exchanges=tuple(
                    str(item) for item in binding_payload.get("exchanges") or []
                ),
                symbol_routes=tuple(
                    (
                        str(item.get("symbol") or ""),
                        str(item.get("exchange") or ""),
                    )
                    for item in symbol_routes
                    if isinstance(item, Mapping)
                ),
            )
            try:
                permit = issue_functional_test_permit(
                    binding=binding,
                    environment=FunctionalTestEnvironment.KIS_LIVE,
                    duration_value=duration_value,
                    duration_unit=FunctionalTestDurationUnit(duration_unit),
                    caps=(
                        US_LIVE_FUNCTIONAL_TEST_CAPS
                        if is_us_live
                        else LIVE_FUNCTIONAL_TEST_CAPS
                    ),
                    now=self._now(),
                )
            except (FunctionalTestContractError, ValueError) as exc:
                code = exc.code if isinstance(exc, FunctionalTestContractError) else str(exc)
                return self._failure(f"기능시험 허가서 생성 실패: {code}")
            permit_path = self.root / "permits" / f"{permit.permit_id}.json"
            write_functional_test_document(permit_path, permit)
            write_functional_test_document(self.current_permit_path, permit)
            self.current_activation_path.unlink(missing_ok=True)
            self._write_control(
                {
                    "status": "PERMIT_READY",
                    "permitId": permit.permit_id,
                    "selectedTargetKey": target_key,
                    "createdAt": permit.issued_at.isoformat(),
                    "stoppedAt": "",
                    "stopFailedAt": "",
                    "stopFailureReason": "",
                }
            )
            return {
                "ok": True,
                "reason": "실전 기능시험 허가서를 생성했습니다. 아직 주문이나 런타임은 시작되지 않았습니다.",
                "brokerSubmissionPerformed": False,
                "workspace": self.snapshot(),
            }

    def activate_today(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if payload.get("confirmed") is not True:
                return self._failure("당일 실전 기능시험 활성화는 명시 확인이 필요합니다.")
            operator = str(payload.get("authorizedBy") or "").strip()
            if not operator:
                return self._failure("활성화 담당자 이름을 입력하세요.")
            account = self._account()
            if account.get("credentialsReady") is not True:
                return self._failure("KIS 실전 앱 키·시크릿·계좌 설정이 모두 필요합니다.")
            if not self.current_permit_path.exists():
                return self._failure("먼저 기능시험 허가서를 생성하세요.")
            control = self._read_control()
            control_status = str(control.get("status") or "").upper()
            if control_status == "PAUSING":
                return self._failure("오늘 실행 정지가 끝난 뒤 다시 활성화하세요.")
            if control_status == "STOP_FAILED":
                return self._failure(
                    "이전 안전 정지가 실패했습니다. KIS 주문·잔고 대조를 먼저 복구하세요."
                )
            now = self._now()
            _permit, active_activation, _blockers = self._read_current(now)
            if active_activation is not None:
                return self._failure(
                    "오늘의 활성화가 이미 유효합니다. 기존 토큰을 교체할 수 없습니다."
                )
            try:
                permit = parse_functional_test_permit(
                    read_functional_test_document(self.current_permit_path)
                )
                us_live = permit.binding.market_group == "US_STOCK"
                calendar_id = "XNYS" if us_live else "XKRX"
                market_label = "미국" if us_live else "KRX"
                market_zone = (
                    ZoneInfo("America/New_York") if us_live else KST
                )
                market_open, market_close = session_bounds_utc(
                    calendar_id,
                    now.astimezone(market_zone).date(),
                )
            except (OSError, ValueError) as exc:
                return self._failure(
                    f"오늘은 {calendar_id if 'calendar_id' in locals() else ''} "
                    "공식 캘린더의 거래 세션이 아니거나 "
                    "캘린더 범위 밖이어서 당일 활성화를 차단했습니다: "
                    f"{type(exc).__name__}"
                )
            if now < market_open:
                return self._failure(
                    f"오늘 {market_label} 정규장이 아직 시작되지 않아 당일 활성화할 수 없습니다."
                )
            if market_close <= now:
                return self._failure(
                    f"오늘 {market_label} 정규장이 종료되어 당일 활성화할 수 없습니다."
                )
            try:
                target_key = str(control.get("selectedTargetKey") or "")
                current_candidate = next(
                    (
                        item
                        for item in self._candidates()
                        if item.get("key") == target_key
                    ),
                    None,
                )
                if current_candidate is None:
                    return self._failure(
                        "선택한 현재 아티팩트를 다시 확인할 수 없어 활성화를 차단했습니다."
                    )
                if current_candidate.get("binding") != permit.binding.snapshot():
                    return self._failure(
                        "아티팩트·인스턴스·계좌·종목 바인딩이 허가서 생성 후 변경되었습니다."
                    )
                activation = issue_live_activation_token(
                    permit=permit,
                    market_day_close=market_close,
                    authorized_by=operator,
                    now=now,
                )
            except FunctionalTestContractError as exc:
                return self._failure(f"당일 활성화 실패: {exc.code}")
            activation_path = self.root / "activations" / f"{activation.token_id}.json"
            write_functional_test_document(activation_path, activation)
            write_functional_test_document(self.current_activation_path, activation)
            control = self._read_control()
            self._write_control(
                {
                    **control,
                    "status": "ACTIVE",
                    "activatedAt": activation.issued_at.isoformat(),
                    "activationExpiresAt": activation.expires_at.isoformat(),
                    "authorizedBy": operator,
                    "pausedAt": "",
                    "pauseRequestedAt": "",
                    "stopFailedAt": "",
                    "stopFailureReason": "",
                }
            )
            return {
                "ok": True,
                "reason": (
                    "오늘의 실전 기능시험 준비 토큰을 활성화했습니다. "
                    "주문·런타임은 시작하지 않았습니다."
                ),
                "brokerSubmissionPerformed": False,
                "workspace": self.snapshot(),
            }

    def begin_pause_today(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Revoke today's activation before any runtime drain begins."""

        with self._lock:
            if payload.get("confirmed") is not True:
                return self._failure("오늘 실행 정지는 명시 확인이 필요합니다.")
            control = self._read_control()
            if not (
                self.current_permit_path.exists()
                or self.current_activation_path.exists()
                or str(control.get("permitId") or "").strip()
            ):
                return self._failure("일시정지할 기능시험 계획이 없습니다.")
            now = self._now()
            # This pointer is the durable authority input read again at the
            # final KIS POST boundary.  The server performs this method while
            # holding FUNCTIONAL_TEST_AUTHORITY_DISPATCH_LOCK.
            self.current_activation_path.unlink(missing_ok=True)
            self._write_control(
                {
                    **control,
                    "status": "PAUSING",
                    "pauseRequestedAt": now.isoformat(),
                    "activationExpiresAt": "",
                    "stopFailedAt": "",
                    "stopFailureReason": "",
                }
            )
            return {
                "ok": True,
                "reason": "오늘의 활성화를 먼저 해제했습니다. runtime 정지와 KIS 대조를 진행합니다.",
                "brokerSubmissionPerformed": False,
                "workspace": self.snapshot(),
            }

    def complete_pause_today(self) -> dict[str, Any]:
        """Mark a successfully drained day while retaining the permit."""

        with self._lock:
            control = self._read_control()
            now = self._now()
            self.current_activation_path.unlink(missing_ok=True)
            self._write_control(
                {
                    **control,
                    "status": "PAUSED",
                    "pausedAt": now.isoformat(),
                    "activationExpiresAt": "",
                    "stopFailedAt": "",
                    "stopFailureReason": "",
                }
            )
            return {
                "ok": True,
                "reason": (
                    "오늘 실행을 안전하게 정지했습니다. 전체 permit은 유지되어 "
                    "다음 거래일에 '오늘 활성화'로 이어갈 수 있습니다."
                ),
                "brokerSubmissionPerformed": False,
                "workspace": self.snapshot(),
            }

    def stop(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if payload.get("confirmed") is not True:
                return self._failure("기능시험 준비 중지는 명시 확인이 필요합니다.")
            now = self._now()
            control = self._read_control()
            self._write_control(
                {
                    **control,
                    "status": "STOPPED",
                    "stoppedAt": now.isoformat(),
                    "activationExpiresAt": "",
                    "stopFailedAt": "",
                    "stopFailureReason": "",
                }
            )
            # Immutable history is retained; only the active pointers are
            # revoked so downstream order gates fail closed immediately.
            self.current_activation_path.unlink(missing_ok=True)
            self.current_permit_path.unlink(missing_ok=True)
            return {
                "ok": True,
                "reason": (
                    "기능시험 준비 허가를 중지했습니다. 이 화면은 주문 취소나 포지션 청산을 수행하지 않으므로 "
                    "실제 실행이 있었다면 주문·포지션 탭에서 별도로 대조해야 합니다."
                ),
                "brokerSubmissionPerformed": False,
                "workspace": self.snapshot(),
            }

    def record_stop_failed(self, reason: object) -> dict[str, Any]:
        """Keep authority pointers for recovery while exposing fail-closed state."""

        with self._lock:
            control = self._read_control()
            self._write_control(
                {
                    **control,
                    "status": "STOP_FAILED",
                    "stopFailedAt": self._now().isoformat(),
                    "stopFailureReason": str(reason or "unknown")[:500],
                }
            )
            return self._failure(
                "기능시험 안전 중지가 완료되지 않았습니다. 허가 문서는 복구를 위해 "
                "유지되고 신규 주문 권한은 차단되었습니다: "
                + str(reason or "unknown")
            )

    def record_pause_failed(self, reason: object) -> dict[str, Any]:
        """Expose a failed daily drain while keeping activation revoked."""

        with self._lock:
            control = self._read_control()
            self.current_activation_path.unlink(missing_ok=True)
            self._write_control(
                {
                    **control,
                    "status": "STOP_FAILED",
                    "stopFailedAt": self._now().isoformat(),
                    "stopFailureReason": str(reason or "unknown")[:500],
                    "stopFailureAction": "PAUSE_TODAY",
                    "activationExpiresAt": "",
                }
            )
            return self._failure(
                "오늘 실행 정지는 실패했지만 당일 활성화는 이미 해제되어 신규 주문은 차단됩니다. "
                "permit은 복구를 위해 유지됩니다: "
                + str(reason or "unknown")
            )

    def _failure(self, reason: str) -> dict[str, Any]:
        return {
            "ok": False,
            "reason": reason,
            "brokerSubmissionPerformed": False,
            "workspace": self.snapshot(),
        }


FUNCTIONAL_TEST_WORKSPACE = FunctionalTestWorkspace()


__all__ = [
    "FUNCTIONAL_TEST_WORKSPACE",
    "FUNCTIONAL_TEST_WORKSPACE_SCHEMA_VERSION",
    "FUNCTIONAL_TEST_US_LIVE_AVAILABLE",
    "LIVE_FUNCTIONAL_TEST_CAPS",
    "US_LIVE_FUNCTIONAL_TEST_CAPS",
    "FunctionalTestWorkspace",
    "canonical_kis_domestic_symbol",
    "canonical_kis_us_symbol",
    "kis_account_binding_id",
]
