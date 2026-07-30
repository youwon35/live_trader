from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
import urllib.parse
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Protocol

from .brokers import LiveBrokerRouter, real_orders_enabled
from .futures_canary import (
    HARD_MAX_NOTIONAL_USDT,
    HARD_MIN_NOTIONAL_USDT,
    derive_canary_quantity,
    normalize_usdm_symbol,
)
from .live_adapters import (
    BINANCE_FUTURES_BASE_URL,
    binance_symbol_rules,
    env_value,
    http_json,
)


SCHEMA_VERSION = "binance-usdm-fill-soak-v1"
BROKER_ID = "binance-futures"
PASS = "PASS"
FAIL = "FAIL"
ACTIVE_ORDER_STATES = {"NEW", "PARTIALLY_FILLED", "PENDING_NEW"}
FAILED_ORDER_STATES = {
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "REJECTED",
}


def _decimal(value: object) -> Decimal | None:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return number if number.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def _safe_id(value: object, *, maximum: int = 20) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]", "", str(value or ""))
    return normalized[:maximum]


def _utc_text(clock: "Clock") -> str:
    return clock.utcnow().astimezone(timezone.utc).isoformat()


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def time(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...

    def utcnow(self) -> datetime: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def time(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(max(0.0, seconds))

    def utcnow(self) -> datetime:
        return datetime.now(timezone.utc)


class Router(Protocol):
    def get_binance_futures_canary_observation(
        self,
        symbol: str,
    ) -> dict[str, object]: ...

    def get_account_snapshot(self, broker_id: str) -> dict[str, object]: ...

    def list_positions(self, broker_id: str) -> list[dict[str, object]]: ...

    def list_open_orders(
        self,
        broker_id: str,
        *,
        symbol: str = "",
    ) -> list[dict[str, object]]: ...

    def place_order(self, intent: dict[str, object]) -> dict[str, object]: ...

    def get_order_status(
        self,
        broker_id: str,
        *,
        symbol: str,
        broker_order_id: str,
        client_order_id: bool = False,
    ) -> dict[str, object]: ...

    def cancel_order(
        self,
        broker_id: str,
        broker_order_id: str,
        **context: object,
    ) -> dict[str, object]: ...


class ReportWriter(Protocol):
    def ensure_available(self, session_id: str) -> None: ...

    def write(self, report: dict[str, object]) -> tuple[dict[str, object], str]: ...


def default_report_directory() -> Path:
    local_app_data = os.getenv("LOCALAPPDATA", "").strip()
    root = Path(local_app_data) if local_app_data else Path.home() / "AppData" / "Local"
    return root / "live_trader" / "logs" / "futures-fill-soak"


class ImmutableJsonReportWriter:
    """Write one canonical, content-hashed report without overwriting."""

    def __init__(self, directory: str | Path | None = None) -> None:
        self.directory = Path(directory or default_report_directory())

    def path_for(self, session_id: str) -> Path:
        safe_session_id = _safe_id(session_id, maximum=64)
        if not safe_session_id:
            raise ValueError("session_id is required")
        return self.directory / f"{safe_session_id}.json"

    def ensure_available(self, session_id: str) -> None:
        if self.path_for(session_id).exists():
            raise FileExistsError(
                f"fill soak report already exists: {self.path_for(session_id)}"
            )

    def write(
        self,
        report: dict[str, object],
    ) -> tuple[dict[str, object], str]:
        session_id = str(report.get("session_id") or "")
        final_path = self.path_for(session_id)
        self.directory.mkdir(parents=True, exist_ok=True)
        canonical = json.dumps(
            report,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hashlib.sha256(canonical).hexdigest()
        document = {**report, "report_sha256": digest}
        payload = (
            json.dumps(
                document,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n"
        ).encode("utf-8")
        temporary = final_path.with_name(
            f".{final_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            descriptor = os.open(
                temporary,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
            except Exception:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                raise
            os.link(temporary, final_path)
            temporary.unlink()
        finally:
            if temporary.exists():
                temporary.unlink()
        return document, str(final_path)


@dataclass(frozen=True)
class LiveOrderAuthorization:
    confirmed: bool
    token_fingerprint: str
    issued_at_epoch: float
    expires_at_epoch: float

    def validate(self, clock: Clock) -> tuple[bool, str]:
        if self.confirmed is not True:
            return False, "operator-confirmation-required"
        fingerprint = _safe_id(self.token_fingerprint, maximum=64)
        if len(fingerprint) < 8:
            return False, "operator-token-fingerprint-invalid"
        now = clock.time()
        if self.issued_at_epoch > now + 5:
            return False, "operator-token-issued-in-future"
        if self.expires_at_epoch <= now:
            return False, "operator-token-expired"
        if self.expires_at_epoch <= self.issued_at_epoch:
            return False, "operator-token-window-invalid"
        return True, ""


@dataclass(frozen=True)
class FillSoakConfig:
    session_id: str = field(
        default_factory=lambda: f"bfsoak-{uuid.uuid4().hex[:16]}"
    )
    symbol: str = "BTCUSDT"
    duration_seconds: float = 5 * 60 * 60
    target_round_trips: int = 3
    target_notional_usdt: Decimal = Decimal("5")
    daily_drawdown_limit_pct: Decimal = Decimal("10")
    fill_timeout_seconds: float = 45.0
    poll_interval_seconds: float = 1.0
    monitor_interval_seconds: float = 10.0
    strategy_id: str = "binance-futures-fill-soak"

    def __post_init__(self) -> None:
        normalized_session_id = _safe_id(self.session_id, maximum=64)
        normalized_symbol = normalize_usdm_symbol(self.symbol)
        target = _decimal(self.target_notional_usdt)
        drawdown = _decimal(self.daily_drawdown_limit_pct)
        if len(normalized_session_id) < 8:
            raise ValueError("session_id must contain at least 8 safe characters")
        if not normalized_symbol:
            raise ValueError("symbol is required")
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        if self.target_round_trips != 3:
            raise ValueError("fill soak requires exactly 3 complete round trips")
        if (
            target is None
            or target < HARD_MIN_NOTIONAL_USDT
            or target > HARD_MAX_NOTIONAL_USDT
        ):
            raise ValueError("target_notional_usdt must be between 5 and 10")
        if drawdown is None or drawdown != Decimal("10"):
            raise ValueError("daily_drawdown_limit_pct must be exactly 10")
        if self.fill_timeout_seconds <= 0:
            raise ValueError("fill_timeout_seconds must be positive")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if self.monitor_interval_seconds <= 0:
            raise ValueError("monitor_interval_seconds must be positive")
        object.__setattr__(self, "session_id", normalized_session_id)
        object.__setattr__(self, "symbol", normalized_symbol)
        object.__setattr__(self, "target_notional_usdt", target)
        object.__setattr__(self, "daily_drawdown_limit_pct", drawdown)

    def to_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["target_notional_usdt"] = _decimal_text(
            self.target_notional_usdt
        )
        result["daily_drawdown_limit_pct"] = _decimal_text(
            self.daily_drawdown_limit_pct
        )
        result["broker_id"] = BROKER_ID
        result["required_margin_type"] = "ISOLATED"
        result["required_leverage"] = 1
        result["required_position_mode"] = "HEDGE"
        result["max_open_orders"] = 1
        result["max_open_positions"] = 1
        result["order_cap_mode"] = "initial_available_usdt_100_percent"
        return result


@dataclass(frozen=True)
class SoakObservation:
    can_trade: bool | None
    available_usdt: Decimal | None
    equity_usdt: Decimal | None
    wallet_balance_usdt: Decimal | None
    margin_balance_usdt: Decimal | None
    hedge_mode: bool | None
    margin_type: str
    leverage: Decimal | None
    positions: tuple[dict[str, object], ...]
    open_orders: tuple[dict[str, object], ...]

    def summary(self) -> dict[str, object]:
        return {
            "can_trade": self.can_trade,
            "available_usdt": _decimal_text(self.available_usdt),
            "equity_usdt": _decimal_text(self.equity_usdt),
            "wallet_balance_usdt": _decimal_text(
                self.wallet_balance_usdt
            ),
            "margin_balance_usdt": _decimal_text(
                self.margin_balance_usdt
            ),
            "hedge_mode": self.hedge_mode,
            "margin_type": self.margin_type,
            "leverage": _decimal_text(self.leverage),
            "position_count": len(self.positions),
            "open_order_count": len(self.open_orders),
        }


class FillSoakHalt(RuntimeError):
    def __init__(
        self,
        reason_id: str,
        detail: str = "",
        *,
        ambiguous: bool = False,
    ) -> None:
        super().__init__(detail or reason_id)
        self.reason_id = reason_id
        self.detail = detail or reason_id
        self.ambiguous = ambiguous


def _default_price_provider(symbol: str) -> Decimal:
    base_url = (
        env_value("BINANCE_FUTURES_BASE_URL")
        or BINANCE_FUTURES_BASE_URL
    )
    response = http_json(
        "GET",
        (
            f"{base_url.rstrip('/')}/fapi/v1/ticker/price?"
            + urllib.parse.urlencode({"symbol": normalize_usdm_symbol(symbol)})
        ),
        body=None,
        headers={},
        timeout_seconds=10.0,
    )
    payload = (
        response.get("json")
        if isinstance(response.get("json"), dict)
        else {}
    )
    price = _decimal(payload.get("price"))
    if response.get("ok") is not True or price is None or price <= 0:
        raise FillSoakHalt(
            "ticker-observation-failed",
            str(response.get("text") or "invalid futures ticker response"),
        )
    return price


def _default_rules_provider(symbol: str) -> dict[str, object]:
    return dict(binance_symbol_rules(symbol, futures=True))


def _account_number(
    row: dict[str, object],
    *names: str,
) -> Decimal | None:
    for name in names:
        value = _decimal(row.get(name))
        if value is not None:
            return value
    return None


def _payload(response: object) -> dict[str, object]:
    if not isinstance(response, dict):
        return {}
    nested = response.get("json")
    if isinstance(nested, dict):
        return dict(nested)
    return dict(response)


def _order_state(row: dict[str, object]) -> str:
    return str(row.get("status") or row.get("state") or "").strip().upper()


def _order_id(row: dict[str, object]) -> str:
    return str(
        row.get("orderId")
        or row.get("order_id")
        or row.get("broker_order_id")
        or ""
    ).strip()


def _client_order_id(row: dict[str, object]) -> str:
    return str(
        row.get("clientOrderId")
        or row.get("client_order_id")
        or row.get("origClientOrderId")
        or ""
    ).strip()


def _executed_quantity(row: dict[str, object]) -> Decimal:
    return (
        _decimal(
            row.get("executedQty")
            or row.get("executed_quantity")
            or row.get("cumQty")
            or 0
        )
        or Decimal("0")
    )


class BinanceFuturesFillSoakSession:
    """A fail-closed, minimum-size USD-M short fill validation session.

    The class performs no network or broker action during construction. A
    current, explicit authorization is required by ``run`` before the first
    observation. Strategy promotion is deliberately outside this component.
    """

    def __init__(
        self,
        config: FillSoakConfig,
        *,
        router: Router | None = None,
        clock: Clock | None = None,
        price_provider: Callable[[str], object] | None = None,
        rules_provider: Callable[[str], dict[str, object]] | None = None,
        live_orders_enabled: Callable[[], bool] | None = None,
        report_writer: ReportWriter | None = None,
    ) -> None:
        self.config = config
        self.router: Router = router or LiveBrokerRouter()
        self.clock = clock or SystemClock()
        self.price_provider = price_provider or _default_price_provider
        self.rules_provider = rules_provider or _default_rules_provider
        self.live_orders_enabled = (
            live_orders_enabled or real_orders_enabled
        )
        self.report_writer = report_writer or ImmutableJsonReportWriter()
        self._stop_requested = threading.Event()
        self._run_lock = threading.Lock()
        self._status_lock = threading.Lock()
        self._phase = "CREATED"
        self._started_at = ""
        self._started_monotonic = 0.0
        self._deadline = 0.0
        self._round_trips = 0
        self._fill_count = 0
        self._order_sequence = 0
        session_fingerprint = hashlib.sha256(
            config.session_id.encode("utf-8")
        ).hexdigest()[:12]
        self._client_prefix = (
            f"ltfs-{session_fingerprint}-"
        )
        self._orders: list[dict[str, object]] = []
        self._events: list[dict[str, object]] = []
        self._reason_ids: list[str] = []
        self._baseline: dict[str, Decimal] = {}
        self._last_observation: SoakObservation | None = None
        self._max_drawdown_pct = Decimal("0")
        self._planned_quantity = Decimal("0")
        self._step_size = Decimal("0")
        self._exchange_min_notional = Decimal("0")
        self._estimated_notional = Decimal("0")
        self._active_client_order_id = ""
        self._attempted_entry_quantity = Decimal("0")
        self._final_report: dict[str, object] | None = None
        self._report_path = ""

    def request_stop(self) -> None:
        self._stop_requested.set()

    def status(self) -> dict[str, object]:
        with self._status_lock:
            elapsed = (
                max(
                    0.0,
                    self.clock.monotonic() - self._started_monotonic,
                )
                if self._started_monotonic
                else 0.0
            )
            return {
                "schema_version": SCHEMA_VERSION,
                "session_id": self.config.session_id,
                "phase": self._phase,
                "started_at": self._started_at,
                "duration_seconds": self.config.duration_seconds,
                "elapsed_seconds": elapsed,
                "remaining_seconds": max(
                    0.0,
                    self.config.duration_seconds - elapsed,
                ),
                "round_trips_completed": self._round_trips,
                "target_round_trips": self.config.target_round_trips,
                "fill_count": self._fill_count,
                "target_fill_count": self.config.target_round_trips * 2,
                "max_drawdown_pct": _decimal_text(
                    self._max_drawdown_pct
                ),
                "stop_requested": self._stop_requested.is_set(),
                "active_client_order_id": self._active_client_order_id,
                "report_path": self._report_path,
            }

    def preview(self) -> dict[str, object]:
        """Return a fresh, read-only start assessment.

        Preview never calls ``place_order`` or ``cancel_order``. ``run`` does
        not trust this snapshot; it independently repeats every observation
        and quantity/cap check immediately before sending an entry.
        """

        if not self._run_lock.acquire(blocking=False):
            return {
                "schema_version": SCHEMA_VERSION,
                "session_id": self.config.session_id,
                "ready": False,
                "blockers": ["session-already-running"],
            }
        try:
            blockers: list[str] = []
            observation: SoakObservation | None = None
            quantity: Decimal | None = None
            estimated: Decimal | None = None
            try:
                observation = self._observe()
            except FillSoakHalt as exc:
                blockers.append(exc.reason_id)
            if observation is not None:
                if observation.can_trade is not True:
                    blockers.append("account-cannot-trade")
                if observation.hedge_mode is not True:
                    blockers.append("position-mode-not-hedge")
                if observation.margin_type != "ISOLATED":
                    blockers.append("margin-type-not-isolated")
                if observation.leverage != Decimal("1"):
                    blockers.append("leverage-not-1x")
                if observation.positions:
                    blockers.append("preflight-position-not-flat")
                if observation.open_orders:
                    blockers.append("preflight-open-orders-present")
                if (
                    observation.available_usdt is None
                    or observation.available_usdt <= 0
                ):
                    blockers.append("available-usdt-invalid")
                if (
                    observation.equity_usdt is None
                    or observation.equity_usdt <= 0
                ):
                    blockers.append("initial-equity-invalid")
            try:
                _, quantity, estimated, _, _ = self._derive_order_plan()
            except FillSoakHalt as exc:
                blockers.append(exc.reason_id)
            if (
                observation is not None
                and observation.available_usdt is not None
                and estimated is not None
                and estimated > observation.available_usdt
            ):
                blockers.append("minimum-order-exceeds-available-usdt")
            if self.live_orders_enabled() is not True:
                blockers.append("real-orders-disabled")
            try:
                self.report_writer.ensure_available(
                    self.config.session_id
                )
            except Exception:
                blockers.append("immutable-report-path-unavailable")
            positions = []
            open_orders = []
            if observation is not None:
                positions = [
                    {
                        "symbol": normalize_usdm_symbol(
                            item.get("symbol")
                        ),
                        "position_side": str(
                            item.get("position_side")
                            or item.get("positionSide")
                            or ""
                        ).upper(),
                        "quantity": _decimal_text(
                            abs(
                                _decimal(
                                    item.get("broker_qty")
                                    or item.get("positionAmt")
                                    or 0
                                )
                                or Decimal("0")
                            )
                        ),
                    }
                    for item in observation.positions
                ]
                open_orders = [
                    {
                        "symbol": normalize_usdm_symbol(
                            item.get("symbol")
                        ),
                        "status": _order_state(item),
                        "client_order_id": _client_order_id(item),
                    }
                    for item in observation.open_orders
                ]
            unique_blockers = list(dict.fromkeys(blockers))
            return {
                "schema_version": SCHEMA_VERSION,
                "previewed_at": _utc_text(self.clock),
                "session_id": self.config.session_id,
                "symbol": self.config.symbol,
                "can_trade": (
                    observation.can_trade
                    if observation is not None
                    else None
                ),
                "available_usdt": (
                    _decimal_text(observation.available_usdt)
                    if observation is not None
                    else None
                ),
                "equity_usdt": (
                    _decimal_text(observation.equity_usdt)
                    if observation is not None
                    else None
                ),
                "hedge_mode": (
                    observation.hedge_mode
                    if observation is not None
                    else None
                ),
                "margin_type": (
                    observation.margin_type
                    if observation is not None
                    else ""
                ),
                "leverage": (
                    _decimal_text(observation.leverage)
                    if observation is not None
                    else None
                ),
                "positions": positions,
                "open_orders": open_orders,
                "minimum_order": {
                    "quantity": _decimal_text(quantity),
                    "estimated_notional_usdt": _decimal_text(estimated),
                    "hard_min_notional_usdt": _decimal_text(
                        HARD_MIN_NOTIONAL_USDT
                    ),
                    "hard_max_notional_usdt": _decimal_text(
                        HARD_MAX_NOTIONAL_USDT
                    ),
                },
                "initial_available_cap_usdt": (
                    _decimal_text(observation.available_usdt)
                    if observation is not None
                    else None
                ),
                "ready": not unique_blockers,
                "blockers": unique_blockers,
                "run_revalidates_preview": True,
            }
        finally:
            self._run_lock.release()

    def run(
        self,
        authorization: LiveOrderAuthorization,
    ) -> dict[str, object]:
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("fill soak session is already running")
        try:
            return self._run_locked(authorization)
        finally:
            self._run_lock.release()

    def _run_locked(
        self,
        authorization: LiveOrderAuthorization,
    ) -> dict[str, object]:
        self._started_monotonic = self.clock.monotonic()
        self._started_at = _utc_text(self.clock)
        self._deadline = (
            self._started_monotonic + self.config.duration_seconds
        )
        self._set_phase("AUTHORIZING")
        failure: FillSoakHalt | None = None
        try:
            valid, reason = authorization.validate(self.clock)
            if not valid:
                raise FillSoakHalt(reason)
            if self.live_orders_enabled() is not True:
                raise FillSoakHalt("real-orders-disabled")
            self.report_writer.ensure_available(self.config.session_id)
            self._record_event(
                "operator-authorized",
                token_fingerprint=_safe_id(
                    authorization.token_fingerprint,
                    maximum=64,
                ),
            )
            self._preflight()
            for round_trip in range(1, self.config.target_round_trips + 1):
                self._execute_round_trip(round_trip)
            self._set_phase("SOAKING_FLAT")
            self._soak_flat_until_deadline()
        except FillSoakHalt as exc:
            failure = exc
            self._add_reason(exc.reason_id)
            self._record_event(
                "session-halted",
                reason_id=exc.reason_id,
                detail=exc.detail,
                ambiguous=exc.ambiguous,
            )
        except Exception as exc:  # defensive broker/process boundary
            failure = FillSoakHalt(
                "unexpected-session-error",
                f"{type(exc).__name__}: {exc}",
                ambiguous=True,
            )
            self._add_reason(failure.reason_id)
            self._record_event(
                "session-halted",
                reason_id=failure.reason_id,
                detail=failure.detail,
                ambiguous=True,
            )

        if failure is not None:
            self._set_phase("RECOVERING")
        recovery = (
            self._recover_best_effort()
            if self._orders
            else {
                "attempted": False,
                "cancelled_client_order_ids": [],
                "flatten_orders": [],
                "errors": [],
                "final_observation": None,
            }
        )
        final_observation = recovery.get("final_observation")
        if not isinstance(final_observation, SoakObservation):
            final_observation = self._last_observation
        final_flat = (
            isinstance(final_observation, SoakObservation)
            and len(final_observation.positions) == 0
        )
        final_orders_clear = (
            isinstance(final_observation, SoakObservation)
            and len(final_observation.open_orders) == 0
        )
        duration = max(
            0.0,
            self.clock.monotonic() - self._started_monotonic,
        )
        passed = (
            failure is None
            and self._round_trips == self.config.target_round_trips
            and self._fill_count == self.config.target_round_trips * 2
            and duration >= self.config.duration_seconds
            and self._max_drawdown_pct
            < self.config.daily_drawdown_limit_pct
            and final_flat
            and final_orders_clear
            and not recovery.get("errors")
        )
        if not final_flat:
            self._add_reason("final-position-not-flat")
        if not final_orders_clear:
            self._add_reason("final-open-orders-remain")
        if duration < self.config.duration_seconds:
            self._add_reason("soak-duration-incomplete")
        if self._round_trips != self.config.target_round_trips:
            self._add_reason("round-trip-target-incomplete")
        self._set_phase("COMPLETED" if passed else "FAILED")
        report = self._build_report(
            passed=passed,
            duration_seconds=duration,
            final_observation=final_observation,
            recovery=recovery,
        )
        try:
            document, path = self.report_writer.write(report)
            self._report_path = path
            document = {**document, "report_path": path}
        except Exception as exc:
            self._add_reason("immutable-report-write-failed")
            document = {
                **report,
                "status": FAIL,
                "reason_ids": list(self._reason_ids),
                "report_write_error": f"{type(exc).__name__}: {exc}",
            }
        self._final_report = document
        return document

    def _set_phase(self, phase: str) -> None:
        with self._status_lock:
            self._phase = phase
        self._record_event("phase", phase=phase)

    def _record_event(self, event: str, **fields: object) -> None:
        self._events.append(
            {
                "at": _utc_text(self.clock),
                "elapsed_seconds": max(
                    0.0,
                    self.clock.monotonic() - self._started_monotonic,
                ),
                "event": event,
                **fields,
            }
        )

    def _add_reason(self, reason_id: str) -> None:
        if reason_id and reason_id not in self._reason_ids:
            self._reason_ids.append(reason_id)

    def _preflight(self) -> None:
        self._set_phase("PREFLIGHT")
        observation = self._observe()
        self._require_account_policy(observation)
        if observation.positions:
            raise FillSoakHalt("preflight-position-not-flat")
        if observation.open_orders:
            raise FillSoakHalt("preflight-open-orders-present")
        if (
            observation.available_usdt is None
            or observation.available_usdt <= 0
        ):
            raise FillSoakHalt("available-usdt-invalid")
        if observation.equity_usdt is None or observation.equity_usdt <= 0:
            raise FillSoakHalt("initial-equity-invalid")
        self._baseline = {
            "available_usdt": observation.available_usdt,
            "equity_usdt": observation.equity_usdt,
            "wallet_balance_usdt": (
                observation.wallet_balance_usdt
                if observation.wallet_balance_usdt is not None
                else observation.equity_usdt
            ),
        }
        price, quantity, estimated, step, exchange_minimum = (
            self._derive_order_plan()
        )
        if estimated > observation.available_usdt:
            raise FillSoakHalt("minimum-order-exceeds-available-usdt")
        self._planned_quantity = quantity
        self._step_size = step
        self._exchange_min_notional = exchange_minimum
        self._estimated_notional = estimated
        self._check_drawdown(observation)
        self._record_event(
            "preflight-passed",
            baseline_available_usdt=_decimal_text(
                observation.available_usdt
            ),
            baseline_equity_usdt=_decimal_text(
                observation.equity_usdt
            ),
            planned_quantity=_decimal_text(quantity),
            estimated_notional_usdt=_decimal_text(estimated),
        )

    def _require_account_policy(
        self,
        observation: SoakObservation,
    ) -> None:
        if observation.can_trade is not True:
            raise FillSoakHalt("account-cannot-trade")
        if observation.hedge_mode is not True:
            raise FillSoakHalt("position-mode-not-hedge")
        if observation.margin_type != "ISOLATED":
            raise FillSoakHalt("margin-type-not-isolated")
        if observation.leverage != Decimal("1"):
            raise FillSoakHalt("leverage-not-1x")

    def _observe(self) -> SoakObservation:
        if self._stop_requested.is_set():
            raise FillSoakHalt("operator-stop-requested")
        try:
            canary = self.router.get_binance_futures_canary_observation(
                self.config.symbol
            )
            snapshot = self.router.get_account_snapshot(BROKER_ID)
            positions = self.router.list_positions(BROKER_ID)
            open_orders = self.router.list_open_orders(
                BROKER_ID,
            )
        except FillSoakHalt:
            raise
        except Exception as exc:
            raise FillSoakHalt(
                "broker-observation-failed",
                f"{type(exc).__name__}: {exc}",
                ambiguous=True,
            ) from exc
        account = (
            canary.get("account")
            if isinstance(canary.get("account"), dict)
            else {}
        )
        position_mode = (
            canary.get("position_mode")
            if isinstance(canary.get("position_mode"), dict)
            else {}
        )
        symbol_config = (
            canary.get("symbol_config")
            if isinstance(canary.get("symbol_config"), dict)
            else {}
        )
        accounts = (
            snapshot.get("accounts")
            if isinstance(snapshot.get("accounts"), list)
            else []
        )
        account_row = next(
            (
                item
                for item in accounts
                if isinstance(item, dict)
                and str(item.get("currency") or "").upper() == "USDT"
            ),
            {},
        )
        observed_positions = tuple(
            dict(item)
            for item in positions
            if isinstance(item, dict)
            and abs(
                _decimal(
                    item.get("broker_qty")
                    or item.get("positionAmt")
                    or 0
                )
                or Decimal("0")
            )
            > Decimal("0")
        )
        observed_orders = tuple(
            dict(item)
            for item in open_orders
            if isinstance(item, dict)
            and _order_state(item) in ACTIVE_ORDER_STATES
        )
        reported_position_count = canary.get("position_count")
        reported_order_count = canary.get("open_order_count")
        if (
            not isinstance(reported_position_count, int)
            or reported_position_count != len(observed_positions)
        ):
            raise FillSoakHalt(
                "position-observation-drift",
                (
                    f"canary={reported_position_count}, "
                    f"detail={len(observed_positions)}"
                ),
                ambiguous=True,
            )
        if (
            not isinstance(reported_order_count, int)
            or reported_order_count != len(observed_orders)
        ):
            raise FillSoakHalt(
                "open-order-observation-drift",
                (
                    f"canary={reported_order_count}, "
                    f"detail={len(observed_orders)}"
                ),
                ambiguous=True,
            )
        available = _decimal(account.get("available_usdt"))
        snapshot_available = _account_number(
            account_row,
            "broker_cash",
            "available_usdt",
        )
        if (
            available is None
            or snapshot_available is None
            or abs(available - snapshot_available)
            > max(
                Decimal("0.10"),
                abs(available) * Decimal("0.001"),
            )
        ):
            raise FillSoakHalt(
                "available-balance-observation-drift",
                (
                    f"canary={_decimal_text(available)}, "
                    f"snapshot={_decimal_text(snapshot_available)}"
                ),
                ambiguous=True,
            )
        margin_balance = _account_number(
            account_row,
            "margin_balance",
            "broker_equity",
        )
        wallet_balance = _account_number(
            account_row,
            "wallet_balance",
        )
        equity = margin_balance or _account_number(
            account_row,
            "broker_equity",
        )
        observation = SoakObservation(
            can_trade=(
                account.get("can_trade")
                if isinstance(account.get("can_trade"), bool)
                else None
            ),
            available_usdt=available,
            equity_usdt=equity,
            wallet_balance_usdt=wallet_balance,
            margin_balance_usdt=margin_balance,
            hedge_mode=(
                position_mode.get("dual_side_position")
                if isinstance(
                    position_mode.get("dual_side_position"),
                    bool,
                )
                else None
            ),
            margin_type=str(
                symbol_config.get("margin_type") or ""
            ).strip().upper(),
            leverage=_decimal(symbol_config.get("leverage")),
            positions=observed_positions,
            open_orders=observed_orders,
        )
        self._last_observation = observation
        return observation

    def _check_drawdown(self, observation: SoakObservation) -> None:
        if not self._baseline:
            return
        initial = self._baseline["equity_usdt"]
        current = observation.equity_usdt
        if current is None or current <= 0:
            raise FillSoakHalt("current-equity-invalid")
        drawdown = max(
            Decimal("0"),
            (initial - current) / initial * Decimal("100"),
        )
        self._max_drawdown_pct = max(
            self._max_drawdown_pct,
            drawdown,
        )
        if drawdown >= self.config.daily_drawdown_limit_pct:
            raise FillSoakHalt(
                "daily-drawdown-limit-reached",
                (
                    f"drawdown={_decimal_text(drawdown)}%, "
                    f"limit={_decimal_text(self.config.daily_drawdown_limit_pct)}%"
                ),
            )

    def _check_deadline(self) -> None:
        if self.clock.monotonic() >= self._deadline:
            raise FillSoakHalt("session-deadline-reached")
        if self._stop_requested.is_set():
            raise FillSoakHalt("operator-stop-requested")

    def _execute_round_trip(self, round_trip: int) -> None:
        self._check_deadline()
        self._set_phase(f"ROUND_{round_trip}_ENTRY_PREFLIGHT")
        observation = self._observe()
        self._require_account_policy(observation)
        self._check_drawdown(observation)
        if observation.positions:
            raise FillSoakHalt("entry-precondition-position-not-flat")
        if observation.open_orders:
            raise FillSoakHalt("entry-precondition-open-orders-present")
        price, quantity, live_notional, step, exchange_minimum = (
            self._derive_order_plan()
        )
        self._planned_quantity = quantity
        self._step_size = step
        self._exchange_min_notional = exchange_minimum
        self._estimated_notional = live_notional
        initial_cap = self._baseline["available_usdt"]
        current_available = observation.available_usdt
        if current_available is None:
            raise FillSoakHalt("available-usdt-invalid")
        if (
            live_notional > HARD_MAX_NOTIONAL_USDT
            or live_notional
            < max(HARD_MIN_NOTIONAL_USDT, exchange_minimum)
            or live_notional > initial_cap
            or live_notional > current_available
        ):
            raise FillSoakHalt(
                "entry-notional-exceeds-cap",
                (
                    f"notional={_decimal_text(live_notional)}, "
                    f"initial_cap={_decimal_text(initial_cap)}, "
                    f"available={_decimal_text(current_available)}"
                ),
            )
        self._set_phase(f"ROUND_{round_trip}_ENTRY")
        self._attempted_entry_quantity = quantity
        entry = self._submit_and_wait(
            side="SELL",
            quantity=quantity,
            leg="entry",
            round_trip=round_trip,
            price=price,
        )
        self._set_phase(f"ROUND_{round_trip}_ENTRY_RECONCILE")
        observation = self._observe()
        self._check_drawdown(observation)
        entry_quantity = _executed_quantity(entry)
        if entry_quantity <= 0:
            entry_quantity = quantity
        self._require_exact_short_position(
            observation,
            entry_quantity,
        )
        if observation.open_orders:
            raise FillSoakHalt("entry-reconcile-open-orders-present")

        self._set_phase(f"ROUND_{round_trip}_COVER")
        cover = self._submit_and_wait(
            side="BUY",
            quantity=entry_quantity,
            leg="cover",
            round_trip=round_trip,
            price=price,
        )
        if _executed_quantity(cover) <= 0:
            raise FillSoakHalt("cover-filled-quantity-missing")
        self._set_phase(f"ROUND_{round_trip}_FLAT_RECONCILE")
        observation = self._observe()
        self._check_drawdown(observation)
        if observation.positions:
            raise FillSoakHalt("cover-reconcile-position-not-flat")
        if observation.open_orders:
            raise FillSoakHalt("cover-reconcile-open-orders-present")
        self._round_trips += 1
        self._attempted_entry_quantity = Decimal("0")
        self._record_event(
            "round-trip-completed",
            round_trip=round_trip,
        )

    def _derive_order_plan(
        self,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
        try:
            price = _decimal(self.price_provider(self.config.symbol))
        except FillSoakHalt:
            raise
        except Exception as exc:
            raise FillSoakHalt(
                "ticker-observation-failed",
                f"{type(exc).__name__}: {exc}",
            ) from exc
        if price is None or price <= 0:
            raise FillSoakHalt("ticker-observation-failed")
        try:
            rules = self.rules_provider(self.config.symbol)
        except Exception as exc:
            raise FillSoakHalt(
                "exchange-rules-observation-failed",
                f"{type(exc).__name__}: {exc}",
            ) from exc
        exchange_minimum = _decimal(rules.get("minNotional"))
        minimum_quantity = _decimal(rules.get("minQty"))
        effective_target = self.config.target_notional_usdt
        if (
            minimum_quantity is not None
            and minimum_quantity > 0
        ):
            effective_target = max(
                effective_target,
                minimum_quantity * price,
            )
        if exchange_minimum is not None:
            effective_target = max(
                effective_target,
                exchange_minimum,
            )
        quantity_result = derive_canary_quantity(
            target_notional_usdt=effective_target,
            price=price,
            min_qty=rules.get("minQty"),
            max_qty=rules.get("maxQty"),
            step_size=rules.get("stepSize"),
            exchange_min_notional=rules.get("minNotional"),
        )
        if quantity_result.get("ok") is not True:
            blockers = ",".join(
                str(item)
                for item in quantity_result.get("blockers") or []
            )
            raise FillSoakHalt(
                "minimum-order-derivation-failed",
                blockers,
            )
        quantity = _decimal(quantity_result.get("quantity"))
        estimated = _decimal(
            quantity_result.get("estimated_notional_usdt")
        )
        step = _decimal(rules.get("stepSize"))
        if (
            quantity is None
            or quantity <= 0
            or estimated is None
            or estimated < HARD_MIN_NOTIONAL_USDT
            or estimated > HARD_MAX_NOTIONAL_USDT
            or step is None
            or step <= 0
            or exchange_minimum is None
            or exchange_minimum < 0
        ):
            raise FillSoakHalt("minimum-order-plan-invalid")
        return price, quantity, estimated, step, exchange_minimum

    def _require_exact_short_position(
        self,
        observation: SoakObservation,
        expected_quantity: Decimal,
    ) -> None:
        if len(observation.positions) != 1:
            raise FillSoakHalt(
                "entry-position-count-invalid",
                f"count={len(observation.positions)}",
            )
        position = observation.positions[0]
        if normalize_usdm_symbol(position.get("symbol")) != self.config.symbol:
            raise FillSoakHalt("entry-position-symbol-mismatch")
        side = str(
            position.get("position_side")
            or position.get("positionSide")
            or ""
        ).upper()
        quantity = abs(
            _decimal(
                position.get("broker_qty")
                or position.get("positionAmt")
                or 0
            )
            or Decimal("0")
        )
        if side != "SHORT":
            raise FillSoakHalt("entry-position-side-not-short")
        if abs(quantity - expected_quantity) >= self._step_size:
            raise FillSoakHalt(
                "entry-position-quantity-mismatch",
                (
                    f"expected={_decimal_text(expected_quantity)}, "
                    f"actual={_decimal_text(quantity)}"
                ),
                ambiguous=True,
            )

    def _next_client_order_id(self, leg: str, round_trip: int) -> str:
        self._order_sequence += 1
        suffix = "e" if leg == "entry" else "c"
        candidate = (
            f"{self._client_prefix}{round_trip}{suffix}"
            f"{self._order_sequence:02d}"
        )[:36]
        if any(
            item.get("client_order_id") == candidate
            for item in self._orders
        ):
            raise FillSoakHalt("client-order-id-collision")
        return candidate

    def _submit_and_wait(
        self,
        *,
        side: str,
        quantity: Decimal,
        leg: str,
        round_trip: int,
        price: Decimal,
    ) -> dict[str, object]:
        self._check_deadline()
        if leg == "entry" and self.live_orders_enabled() is not True:
            raise FillSoakHalt("real-orders-disabled-before-entry")
        client_order_id = self._next_client_order_id(leg, round_trip)
        self._active_client_order_id = client_order_id
        intent = {
            "broker_id": BROKER_ID,
            "strategy_id": self.config.strategy_id,
            "symbol": self.config.symbol,
            "side": side,
            "quantity": _decimal_text(quantity),
            "qty": _decimal_text(quantity),
            "price": _decimal_text(price),
            "order_type": "MARKET",
            "position_direction": "short",
            "market_type": "futures",
            "max_leverage": 1,
            "required_margin_type": "ISOLATED",
            "risk_reducing": leg == "cover",
            "reduce_only": leg == "cover",
            "identifier": client_order_id,
        }
        record: dict[str, object] = {
            "round_trip": round_trip,
            "leg": leg,
            "client_order_id": client_order_id,
            "side": side,
            "quantity": _decimal_text(quantity),
            "estimated_notional_usdt": _decimal_text(price * quantity),
            "submitted_at": _utc_text(self.clock),
            "status": "SUBMITTING",
        }
        self._orders.append(record)
        try:
            response = self.router.place_order(intent)
        except Exception as exc:
            resolved = self._resolve_ambiguous_submission(
                client_order_id,
                record,
            )
            if resolved is not None:
                record["status"] = _order_state(resolved) or "UNKNOWN"
                record["broker_order_id"] = _order_id(resolved)
                record["executed_quantity"] = _decimal_text(
                    _executed_quantity(resolved)
                )
                if _order_state(resolved) == "FILLED":
                    self._fill_count += 1
            raise FillSoakHalt(
                "order-submit-ambiguous",
                f"{type(exc).__name__}: {exc}",
                ambiguous=True,
            ) from exc
        payload = _payload(response)
        response_ok = not (
            isinstance(response, dict) and response.get("ok") is False
        )
        if not response_ok:
            resolved = self._resolve_ambiguous_submission(
                client_order_id,
                record,
            )
            if resolved is not None:
                record["status"] = _order_state(resolved) or "UNKNOWN"
                record["broker_order_id"] = _order_id(resolved)
                record["executed_quantity"] = _decimal_text(
                    _executed_quantity(resolved)
                )
                if _order_state(resolved) == "FILLED":
                    self._fill_count += 1
            raise FillSoakHalt(
                "order-submit-ambiguous",
                str(
                    response.get("text")
                    or response.get("status")
                    or "broker response not ok"
                ),
                ambiguous=True,
            )
        broker_order_id = _order_id(payload)
        acknowledged_client_id = _client_order_id(payload)
        if acknowledged_client_id and acknowledged_client_id != client_order_id:
            raise FillSoakHalt(
                "client-order-id-mismatch",
                (
                    f"sent={client_order_id}, "
                    f"ack={acknowledged_client_id}"
                ),
                ambiguous=True,
            )
        record["broker_order_id"] = broker_order_id
        record["status"] = _order_state(payload) or "ACKNOWLEDGED"
        terminal = self._wait_for_terminal(
            broker_order_id=broker_order_id or client_order_id,
            client_order_id=not bool(broker_order_id),
            record=record,
        )
        self._active_client_order_id = ""
        return terminal

    def _resolve_ambiguous_submission(
        self,
        client_order_id: str,
        record: dict[str, object],
    ) -> dict[str, object] | None:
        record["ambiguity_resolution_attempted"] = True
        try:
            status = self.router.get_order_status(
                BROKER_ID,
                symbol=self.config.symbol,
                broker_order_id=client_order_id,
                client_order_id=True,
            )
        except Exception as exc:
            record["ambiguity_resolution_error"] = (
                f"{type(exc).__name__}: {exc}"
            )
            return None
        record["ambiguity_resolution_status"] = _order_state(status)
        return status

    def _wait_for_terminal(
        self,
        *,
        broker_order_id: str,
        client_order_id: bool,
        record: dict[str, object],
    ) -> dict[str, object]:
        wait_started = self.clock.monotonic()
        next_risk_check = wait_started + self.config.monitor_interval_seconds
        while True:
            self._check_deadline()
            if (
                self.clock.monotonic() - wait_started
                >= self.config.fill_timeout_seconds
            ):
                raise FillSoakHalt("order-fill-timeout", ambiguous=True)
            try:
                status = self.router.get_order_status(
                    BROKER_ID,
                    symbol=self.config.symbol,
                    broker_order_id=broker_order_id,
                    client_order_id=client_order_id,
                )
            except Exception as exc:
                raise FillSoakHalt(
                    "order-status-ambiguous",
                    f"{type(exc).__name__}: {exc}",
                    ambiguous=True,
                ) from exc
            state = _order_state(status)
            record["status"] = state or "UNKNOWN"
            record["broker_order_id"] = (
                _order_id(status)
                or str(record.get("broker_order_id") or "")
            )
            record["executed_quantity"] = _decimal_text(
                _executed_quantity(status)
            )
            if state == "FILLED":
                if _executed_quantity(status) <= 0:
                    raise FillSoakHalt(
                        "filled-order-quantity-missing",
                        ambiguous=True,
                    )
                record["filled_at"] = _utc_text(self.clock)
                record["fill_latency_seconds"] = max(
                    0.0,
                    self.clock.monotonic() - wait_started,
                )
                record["average_price"] = _decimal_text(
                    _decimal(
                        status.get("avgPrice")
                        or status.get("average_price")
                        or 0
                    )
                )
                self._fill_count += 1
                return status
            if state in FAILED_ORDER_STATES:
                raise FillSoakHalt(
                    "order-terminal-without-fill",
                    state,
                )
            if state not in ACTIVE_ORDER_STATES:
                raise FillSoakHalt(
                    "order-status-unknown",
                    state or "EMPTY",
                    ambiguous=True,
                )
            if self.clock.monotonic() >= next_risk_check:
                observation = self._observe()
                self._check_drawdown(observation)
                next_risk_check = (
                    self.clock.monotonic()
                    + self.config.monitor_interval_seconds
                )
            self.clock.sleep(self.config.poll_interval_seconds)

    def _soak_flat_until_deadline(self) -> None:
        while self.clock.monotonic() < self._deadline:
            if self._stop_requested.is_set():
                raise FillSoakHalt("operator-stop-requested")
            observation = self._observe()
            self._require_account_policy(observation)
            self._check_drawdown(observation)
            if observation.positions:
                raise FillSoakHalt(
                    "unexpected-position-during-flat-soak",
                    ambiguous=True,
                )
            if observation.open_orders:
                raise FillSoakHalt(
                    "unexpected-open-order-during-flat-soak",
                    ambiguous=True,
                )
            remaining = self._deadline - self.clock.monotonic()
            if remaining <= 0:
                break
            self.clock.sleep(
                min(self.config.monitor_interval_seconds, remaining)
            )

    def _recover_best_effort(self) -> dict[str, object]:
        result: dict[str, object] = {
            "attempted": True,
            "cancelled_client_order_ids": [],
            "flatten_orders": [],
            "errors": [],
            "final_observation": None,
        }
        observation: SoakObservation | None = None
        try:
            observation = self._observe_ignoring_stop()
        except FillSoakHalt as exc:
            result["errors"].append(
                {
                    "reason_id": exc.reason_id,
                    "detail": exc.detail,
                }
            )
        if observation is not None:
            for order in observation.open_orders:
                client_id = _client_order_id(order)
                if not client_id.startswith(self._client_prefix):
                    result["errors"].append(
                        {
                            "reason_id": "foreign-open-order-not-cancelled",
                            "client_order_id": client_id,
                        }
                    )
                    continue
                identifier = _order_id(order) or client_id
                try:
                    cancel_response = self.router.cancel_order(
                        BROKER_ID,
                        identifier,
                        symbol=self.config.symbol,
                        client_order_id=not bool(_order_id(order)),
                    )
                    if (
                        isinstance(cancel_response, dict)
                        and cancel_response.get("ok") is False
                    ):
                        raise RuntimeError(
                            str(
                                cancel_response.get("text")
                                or cancel_response.get("status")
                                or "broker cancel response not ok"
                            )
                        )
                    result["cancelled_client_order_ids"].append(client_id)
                except Exception as exc:
                    result["errors"].append(
                        {
                            "reason_id": "session-order-cancel-failed",
                            "client_order_id": client_id,
                            "detail": f"{type(exc).__name__}: {exc}",
                        }
                    )
            try:
                observation = self._observe_ignoring_stop()
            except FillSoakHalt as exc:
                result["errors"].append(
                    {
                        "reason_id": exc.reason_id,
                        "detail": exc.detail,
                    }
                )

        if observation is not None and observation.positions:
            owned_quantity = self._recoverable_short_quantity(observation)
            if owned_quantity > 0:
                flatten_result = self._submit_recovery_cover(owned_quantity)
                result["flatten_orders"].append(flatten_result)
                if flatten_result.get("ok") is not True:
                    result["errors"].append(
                        {
                            "reason_id": str(
                                flatten_result.get("reason_id")
                                or "recovery-cover-failed"
                            ),
                            "detail": str(
                                flatten_result.get("detail") or ""
                            ),
                        }
                    )
            else:
                result["errors"].append(
                    {
                        "reason_id": "position-ownership-ambiguous",
                    }
                )
        try:
            result["final_observation"] = self._observe_ignoring_stop()
        except FillSoakHalt as exc:
            result["errors"].append(
                {
                    "reason_id": exc.reason_id,
                    "detail": exc.detail,
                }
            )
        for error in result["errors"]:
            if isinstance(error, dict):
                self._add_reason(str(error.get("reason_id") or ""))
        return result

    def _observe_ignoring_stop(self) -> SoakObservation:
        was_set = self._stop_requested.is_set()
        if was_set:
            self._stop_requested.clear()
        try:
            return self._observe()
        finally:
            if was_set:
                self._stop_requested.set()

    def _recoverable_short_quantity(
        self,
        observation: SoakObservation,
    ) -> Decimal:
        if self._attempted_entry_quantity <= 0:
            return Decimal("0")
        matching = [
            item
            for item in observation.positions
            if normalize_usdm_symbol(item.get("symbol"))
            == self.config.symbol
            and str(
                item.get("position_side")
                or item.get("positionSide")
                or ""
            ).upper()
            == "SHORT"
        ]
        if len(matching) != 1:
            return Decimal("0")
        actual = abs(
            _decimal(
                matching[0].get("broker_qty")
                or matching[0].get("positionAmt")
                or 0
            )
            or Decimal("0")
        )
        return min(actual, self._attempted_entry_quantity)

    def _submit_recovery_cover(
        self,
        quantity: Decimal,
    ) -> dict[str, object]:
        client_order_id = self._next_client_order_id(
            "cover",
            max(1, self._round_trips + 1),
        )
        intent = {
            "broker_id": BROKER_ID,
            "strategy_id": self.config.strategy_id,
            "symbol": self.config.symbol,
            "side": "BUY",
            "quantity": _decimal_text(quantity),
            "qty": _decimal_text(quantity),
            "order_type": "MARKET",
            "position_direction": "short",
            "market_type": "futures",
            "max_leverage": 1,
            "required_margin_type": "ISOLATED",
            "risk_reducing": True,
            "reduce_only": True,
            "identifier": client_order_id,
        }
        record: dict[str, object] = {
            "leg": "recovery-cover",
            "client_order_id": client_order_id,
            "quantity": _decimal_text(quantity),
            "submitted_at": _utc_text(self.clock),
            "status": "SUBMITTING",
        }
        self._orders.append(record)
        try:
            response = self.router.place_order(intent)
        except Exception as exc:
            status = self._resolve_ambiguous_submission(
                client_order_id,
                record,
            )
            if status is None:
                return {
                    "ok": False,
                    "reason_id": "recovery-cover-submit-ambiguous",
                    "detail": f"{type(exc).__name__}: {exc}",
                    "client_order_id": client_order_id,
                }
            state = _order_state(status)
            record["status"] = state
            record["executed_quantity"] = _decimal_text(
                _executed_quantity(status)
            )
            return {
                "ok": state == "FILLED",
                "reason_id": (
                    "" if state == "FILLED"
                    else "recovery-cover-not-filled"
                ),
                "client_order_id": client_order_id,
                "status": state,
            }
        payload = _payload(response)
        broker_order_id = _order_id(payload)
        try:
            status = self._wait_for_recovery_terminal(
                broker_order_id or client_order_id,
                client_order_id=not bool(broker_order_id),
                record=record,
            )
            return {
                "ok": True,
                "client_order_id": client_order_id,
                "status": _order_state(status),
            }
        except FillSoakHalt as exc:
            return {
                "ok": False,
                "reason_id": exc.reason_id,
                "detail": exc.detail,
                "client_order_id": client_order_id,
            }

    def _wait_for_recovery_terminal(
        self,
        broker_order_id: str,
        *,
        client_order_id: bool,
        record: dict[str, object],
    ) -> dict[str, object]:
        started = self.clock.monotonic()
        while (
            self.clock.monotonic() - started
            < self.config.fill_timeout_seconds
        ):
            try:
                status = self.router.get_order_status(
                    BROKER_ID,
                    symbol=self.config.symbol,
                    broker_order_id=broker_order_id,
                    client_order_id=client_order_id,
                )
            except Exception as exc:
                raise FillSoakHalt(
                    "recovery-cover-status-ambiguous",
                    f"{type(exc).__name__}: {exc}",
                    ambiguous=True,
                ) from exc
            state = _order_state(status)
            record["status"] = state or "UNKNOWN"
            record["executed_quantity"] = _decimal_text(
                _executed_quantity(status)
            )
            if state == "FILLED":
                self._fill_count += 1
                return status
            if state in FAILED_ORDER_STATES:
                raise FillSoakHalt(
                    "recovery-cover-terminal-without-fill",
                    state,
                )
            if state not in ACTIVE_ORDER_STATES:
                raise FillSoakHalt(
                    "recovery-cover-status-unknown",
                    state or "EMPTY",
                    ambiguous=True,
                )
            self.clock.sleep(self.config.poll_interval_seconds)
        raise FillSoakHalt(
            "recovery-cover-timeout",
            ambiguous=True,
        )

    def _build_report(
        self,
        *,
        passed: bool,
        duration_seconds: float,
        final_observation: SoakObservation | None,
        recovery: dict[str, object],
    ) -> dict[str, object]:
        sanitized_recovery = {
            key: (
                value.summary()
                if isinstance(value, SoakObservation)
                else value
            )
            for key, value in recovery.items()
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "session_id": self.config.session_id,
            "status": PASS if passed else FAIL,
            "reason_ids": list(self._reason_ids),
            "started_at": self._started_at,
            "ended_at": _utc_text(self.clock),
            "duration_seconds": duration_seconds,
            "configuration": self.config.to_dict(),
            "baseline": {
                key: _decimal_text(value)
                for key, value in self._baseline.items()
            },
            "final_observation": (
                final_observation.summary()
                if isinstance(final_observation, SoakObservation)
                else None
            ),
            "risk": {
                "initial_available_cap_usdt": _decimal_text(
                    self._baseline.get("available_usdt")
                ),
                "initial_equity_usdt": _decimal_text(
                    self._baseline.get("equity_usdt")
                ),
                "daily_drawdown_limit_pct": _decimal_text(
                    self.config.daily_drawdown_limit_pct
                ),
                "max_drawdown_pct": _decimal_text(
                    self._max_drawdown_pct
                ),
                "required_margin_type": "ISOLATED",
                "required_leverage": "1",
                "required_position_mode": "HEDGE",
                "max_open_orders": 1,
                "max_open_positions": 1,
            },
            "order_plan": {
                "quantity": _decimal_text(self._planned_quantity),
                "estimated_notional_usdt": _decimal_text(
                    self._estimated_notional
                ),
                "hard_min_notional_usdt": _decimal_text(
                    HARD_MIN_NOTIONAL_USDT
                ),
                "hard_max_notional_usdt": _decimal_text(
                    HARD_MAX_NOTIONAL_USDT
                ),
            },
            "progress": {
                "round_trips_completed": self._round_trips,
                "target_round_trips": self.config.target_round_trips,
                "fill_count": self._fill_count,
                "target_fill_count": self.config.target_round_trips * 2,
            },
            "final_checks": {
                "duration_complete": (
                    duration_seconds >= self.config.duration_seconds
                ),
                "round_trips_complete": (
                    self._round_trips == self.config.target_round_trips
                ),
                "fill_target_complete": (
                    self._fill_count
                    == self.config.target_round_trips * 2
                ),
                "drawdown_within_limit": (
                    self._max_drawdown_pct
                    < self.config.daily_drawdown_limit_pct
                ),
                "flat": (
                    isinstance(final_observation, SoakObservation)
                    and not final_observation.positions
                ),
                "open_orders_clear": (
                    isinstance(final_observation, SoakObservation)
                    and not final_observation.open_orders
                ),
                "recovery_errors_clear": not bool(
                    sanitized_recovery.get("errors")
                ),
            },
            "orders": list(self._orders),
            "events": list(self._events),
            "recovery": sanitized_recovery,
            "strategy_promotion_authorized": False,
        }


__all__ = [
    "BinanceFuturesFillSoakSession",
    "FillSoakConfig",
    "FillSoakHalt",
    "ImmutableJsonReportWriter",
    "LiveOrderAuthorization",
    "PASS",
    "FAIL",
    "SCHEMA_VERSION",
    "SoakObservation",
]
