from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
import os
import sqlite3
import threading
import time
from typing import Any

from trading_runtime import (
    BuiltinBarSignalEvaluator,
    ContinuousRuntimeSupervisor,
    DurableRuntimeState,
    FeedSubscription,
    PortfolioRuntimeEngine,
    RuntimeStrategySpec,
    artifact_content_hash,
    feeds_for_specs,
    futures_execution_policy_from_artifact,
    infer_market_route,
    load_portfolio_runtime_path,
    required_warmup_bars,
)

from .order_management import OrderIntent
from .portfolio_execution import (
    LIVE_PORTFOLIO_PLAN_SCHEMA,
    ExecutionSyncReport,
    LivePortfolioLedger,
    SleeveTarget,
    SymbolNetPlan,
    build_symbol_net_plan,
    canonical_kis_symbol,
    validate_symbol_net_plan_metadata,
)


class LiveContinuousController:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._lock = threading.RLock()
        self.supervisor: ContinuousRuntimeSupervisor | None = None
        self.profile_id = ""
        self.mode = "MONITOR"
        self.portfolio_path = ""
        self.deployment_id = ""
        self.portfolio_id = ""
        self.requested_strategy_id = ""
        self.strategy_ids: tuple[str, ...] = ()
        self.allowed_symbols: tuple[str, ...] = ()
        self.execution_purpose = ""
        self.functional_test_context: dict[str, Any] = {}
        self.portfolio_instance_id = ""
        self.portfolio_ledger: LivePortfolioLedger | None = None
        self.portfolio_execution_scope_id = ""
        self.portfolio_execution_account_id = ""
        self.portfolio_execution_symbols: tuple[str, ...] = ()
        self._portfolio_sync_lock = threading.RLock()
        self._portfolio_last_sync_monotonic = 0.0

    def start(
        self,
        profile_id: str,
        mode: str,
        portfolio_id: str = "",
        strategy_id: str = "",
        deployment_id: str = "",
        execution_purpose: str = "",
        functional_test_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from . import state

        normalized_profile = "stock" if profile_id == "stock" else "crypto"
        normalized_mode = str(mode or "MONITOR").upper()
        requested_portfolio = str(portfolio_id or "").strip()
        requested_strategy = str(strategy_id or "").strip()
        requested_deployment = str(deployment_id or "").strip()
        normalized_purpose = str(execution_purpose or "").strip().upper()
        functional_context = (
            dict(functional_test_context)
            if isinstance(functional_test_context, dict)
            else {}
        )
        if normalized_mode not in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"}:
            return {"ok": False, "reason": f"지원하지 않는 runtime mode입니다: {normalized_mode}", "snapshot": state.snapshot()}
        if normalized_purpose not in {"", state.FUNCTIONAL_TEST_EXECUTION_PURPOSE}:
            return {
                "ok": False,
                "reason": f"지원하지 않는 execution purpose입니다: {normalized_purpose}",
                "snapshot": state.snapshot(),
            }
        if normalized_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE and (
            normalized_profile != "stock" or normalized_mode != "SMALL_LIVE"
        ):
            return {
                "ok": False,
                "reason": "FUNCTIONAL_TEST는 KIS 주식 SMALL_LIVE 전송 모드만 허용합니다.",
                "snapshot": state.snapshot(),
            }
        functional_start_authorization: dict[str, Any] = {}
        if normalized_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE:
            functional_allowed, functional_reason, functional_start_authorization = (
                state.functional_test_runtime_start_allowed(
                    str(functional_context.get("functional_test_session_id") or ""),
                    portfolio_id=requested_portfolio,
                    strategy_id=requested_strategy,
                )
            )
            if not functional_allowed:
                return {
                    "ok": False,
                    "reason": functional_reason,
                    "snapshot": state.snapshot(),
                }
        with self._lock:
            if self.supervisor and self.supervisor.running:
                raw_supervisor_phase = self.supervisor.snapshot().get("phase")
                supervisor_phase = (
                    raw_supervisor_phase.upper()
                    if isinstance(raw_supervisor_phase, str)
                    else ""
                )
                if supervisor_phase and supervisor_phase not in {
                    "STARTING",
                    "RUNNING",
                    "DEGRADED",
                }:
                    return {
                        "ok": False,
                        "reason": (
                            "기존 runtime thread가 "
                            f"{supervisor_phase or 'UNKNOWN'} 상태로 아직 "
                            "종료되지 않아 새 시작/전환을 차단했습니다."
                        ),
                        "runtime": self.snapshot(),
                        "snapshot": state.snapshot(),
                    }
                context_blocker = self._running_context_blocker(
                    portfolio_id=requested_portfolio,
                    strategy_id=requested_strategy,
                    deployment_id=requested_deployment,
                    execution_purpose=normalized_purpose,
                )
                if context_blocker:
                    return {
                        "ok": False,
                        "reason": context_blocker,
                        "runtime": self.snapshot(),
                        "snapshot": state.snapshot(),
                    }
                specs = tuple(self.supervisor.engine.specs)
                broker_context_blocker = self._single_broker_context_blocker(specs)
                if broker_context_blocker:
                    return {
                        "ok": False,
                        "reason": broker_context_blocker,
                        "runtime": self.snapshot(),
                        "snapshot": state.snapshot(),
                    }
                if normalized_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE:
                    functional_blocker = self._functional_test_spec_blocker(specs)
                    if functional_blocker:
                        return {
                            "ok": False,
                            "reason": functional_blocker,
                            "runtime": self.snapshot(),
                            "snapshot": state.snapshot(),
                        }
                if (
                    normalized_mode != "MONITOR"
                    and normalized_purpose
                    != state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                    and not all(
                        self._spec_mode_allowed(spec, normalized_mode)
                        for spec in specs
                    )
                ):
                    return {
                        "ok": False,
                        "reason": f"실행 중인 Artifact가 {normalized_mode} 권한을 통과하지 못했습니다.",
                        "runtime": self.snapshot(),
                        "snapshot": state.snapshot(),
                    }
                if (
                    normalized_mode != "MONITOR"
                    and normalized_purpose
                    != state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                ):
                    binding_blocker = self._paper_final_binding_blocker(specs)
                    if binding_blocker:
                        return {
                            "ok": False,
                            "reason": binding_blocker,
                            "runtime": self.snapshot(),
                            "snapshot": state.snapshot(),
                        }
                with state.RUNTIME_MODE_LOCK:
                    restore_context = (
                        self._restore_context_assessment(
                            specs,
                            self.supervisor.engine,
                        )
                        if normalized_mode != "MONITOR"
                        else None
                    )
                    portfolio_reconciliation_blocker = (
                        self._portfolio_reconciliation_blocker(specs)
                        if normalized_mode != "MONITOR"
                        else ""
                    )
                    if portfolio_reconciliation_blocker:
                        return {
                            "ok": False,
                            "reason": portfolio_reconciliation_blocker,
                            "runtime": self.snapshot(),
                            "snapshot": state.snapshot(),
                        }
                    restore_blocker = self.supervisor.engine.transition_mode(
                        normalized_mode,  # type: ignore[arg-type]
                        restore_context=restore_context,
                    )
                    if restore_blocker:
                        return {
                            "ok": False,
                            "reason": restore_blocker,
                            "runtime": self.snapshot(),
                            "snapshot": state.snapshot(),
                        }
                    previous_mode = self.mode
                    self.mode = normalized_mode
                    self.execution_purpose = normalized_purpose
                    self.functional_test_context = (
                        functional_context
                        if normalized_purpose
                        == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                        else {}
                    )
                state.append_audit(
                    "info" if normalized_mode == "MONITOR" else "warn",
                    "Continuous Runtime",
                    f"{normalized_profile} runtime {previous_mode} → {normalized_mode} 무중단 전환",
                )
                return {"ok": True, "reason": "continuous runtime mode transitioned", "runtime": self.snapshot(), "snapshot": state.snapshot()}
            functional_scope = (
                functional_start_authorization.get("scope")
                if isinstance(
                    functional_start_authorization.get("scope"), dict
                )
                else {}
            )
            if normalized_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE:
                portfolio = (
                    dict(functional_scope.get("portfolio"))
                    if requested_portfolio
                    and isinstance(functional_scope.get("portfolio"), dict)
                    else None
                )
            else:
                portfolio = (
                    self._select_runtime_portfolio(
                        normalized_profile,
                        requested_portfolio,
                        normalized_mode,
                    )
                    if requested_portfolio or not requested_strategy
                    else None
                )
            loaded = None
            if portfolio is not None:
                source_path = str(portfolio.get("source_path") or "")
                if not source_path or not Path(source_path).exists():
                    return {"ok": False, "reason": "Portfolio Artifact 원본 경로가 없습니다.", "snapshot": state.snapshot()}
                loaded = load_portfolio_runtime_path(source_path)
                if requested_portfolio and loaded.portfolio_id != requested_portfolio:
                    return {
                        "ok": False,
                        "reason": (
                            "요청 Portfolio ID와 로드된 Artifact ID가 일치하지 않습니다: "
                            f"{requested_portfolio} != {loaded.portfolio_id}"
                        ),
                        "snapshot": state.snapshot(),
                    }
                portfolio_broker_blocker = self._single_broker_context_blocker(
                    tuple(loaded.specs)
                )
                if portfolio_broker_blocker:
                    return {
                        "ok": False,
                        "reason": portfolio_broker_blocker,
                        "snapshot": state.snapshot(),
                    }
                specs = tuple(spec for spec in loaded.specs if self._matches_profile(spec, normalized_profile))
                loaded_strategy_ids = {
                    str(spec.strategy_id or "").strip() for spec in specs
                }
                if requested_strategy and requested_strategy not in loaded_strategy_ids:
                    return {
                        "ok": False,
                        "reason": (
                            f"요청 Strategy({requested_strategy})가 선택 Portfolio "
                            f"Artifact({loaded.portfolio_id}) 구성에 없습니다."
                        ),
                        "snapshot": state.snapshot(),
                    }
                runtime_id = loaded.portfolio_id
                runtime_hash = loaded.portfolio_hash
                runtime_permissions = loaded.payload.get("permissions") if isinstance(loaded.payload.get("permissions"), dict) else {}
            else:
                if requested_portfolio:
                    return {
                        "ok": False,
                        "reason": f"요청한 Portfolio Artifact({requested_portfolio})가 없거나 현재 구성 전략 상태로는 실행할 수 없습니다.",
                        "snapshot": state.snapshot(),
                    }
                standalone = (
                    dict(functional_scope.get("leadStrategy"))
                    if normalized_purpose
                    == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                    and isinstance(functional_scope.get("leadStrategy"), dict)
                    else self._select_standalone_strategy(
                        normalized_profile,
                        normalized_mode,
                        requested_strategy,
                        requested_deployment,
                    )
                )
                if standalone is None:
                    return {"ok": False, "reason": f"{normalized_profile}용 Portfolio/Strategy Artifact를 찾을 수 없습니다.", "snapshot": state.snapshot()}
                source_path = str(standalone.get("source_path") or "")
                specs = (self._standalone_spec(standalone),)
                runtime_id = specs[0].portfolio_id
                runtime_hash = specs[0].portfolio_hash
                runtime_permissions = dict(standalone.get("permissions") or {})
            if not specs:
                return {"ok": False, "reason": f"선택 Artifact에 {normalized_profile} Strategy Instance가 없습니다.", "snapshot": state.snapshot()}
            broker_context_blocker = self._single_broker_context_blocker(specs)
            if broker_context_blocker:
                return {
                    "ok": False,
                    "reason": broker_context_blocker,
                    "snapshot": state.snapshot(),
                }
            if normalized_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE:
                functional_blocker = self._functional_test_spec_blocker(specs)
                if functional_blocker:
                    return {
                        "ok": False,
                        "reason": functional_blocker,
                        "snapshot": state.snapshot(),
                    }
            if (
                normalized_mode != "MONITOR"
                and normalized_purpose
                != state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
            ):
                allowed = runtime_permissions.get("live_eligible") is True or runtime_permissions.get("live_allowed") is True
                if normalized_mode == "SMALL_LIVE":
                    allowed = allowed or runtime_permissions.get("live_small_eligible") is True or runtime_permissions.get("live_small_allowed") is True
                if not allowed:
                    return {"ok": False, "reason": f"{runtime_id}는 {normalized_mode} 권한을 통과하지 못했습니다. MONITOR만 가능합니다.", "snapshot": state.snapshot()}
                binding_blocker = self._paper_final_binding_blocker(specs)
                if binding_blocker:
                    return {
                        "ok": False,
                        "reason": binding_blocker,
                        "snapshot": state.snapshot(),
                    }
            lineage_blocker = self._loaded_portfolio_lineage_blocker(
                loaded,
                specs,
                require_complete=normalized_mode != "MONITOR",
            )
            if lineage_blocker:
                return {
                    "ok": False,
                    "reason": lineage_blocker,
                    "snapshot": state.snapshot(),
                }
            portfolio_execution_blocker = self._configure_portfolio_execution(
                specs,
                portfolio_id=runtime_id,
                portfolio_hash=runtime_hash,
                require_account=normalized_mode != "MONITOR",
            )
            if portfolio_execution_blocker:
                return {
                    "ok": False,
                    "reason": portfolio_execution_blocker,
                    "snapshot": state.snapshot(),
                }
            engine = PortfolioRuntimeEngine(
                specs,
                mode="MONITOR",
                evaluator=BuiltinBarSignalEvaluator(
                    self._runtime_position_quantity
                ),
                cycle_handler=self._handle_cycle,
                state_store=DurableRuntimeState(self.root / "logs" / f"continuous_{normalized_profile}_{runtime_hash[:16]}_engine.json"),
            )
            with state.RUNTIME_MODE_LOCK:
                restore_context = (
                    self._restore_context_assessment(specs, engine)
                    if normalized_mode != "MONITOR"
                    else None
                )
                portfolio_restore_blocker = (
                    self._portfolio_reconciliation_blocker(specs)
                    if normalized_mode != "MONITOR"
                    else ""
                )
                restore_blocker = portfolio_restore_blocker or (
                    engine.transition_mode(
                        normalized_mode,  # type: ignore[arg-type]
                        restore_context=restore_context,
                    )
                )
            if restore_blocker:
                return {
                    "ok": False,
                    "reason": restore_blocker,
                    "runtime": {
                        **engine.snapshot(),
                        "profileId": normalized_profile,
                        "mode": "MONITOR",
                        "portfolioPath": source_path,
                    },
                    "snapshot": state.snapshot(),
                }
            self.profile_id = normalized_profile
            self.mode = normalized_mode
            self.portfolio_path = source_path
            self.deployment_id = requested_deployment
            self.portfolio_id = runtime_id if loaded is not None else ""
            self.requested_strategy_id = requested_strategy or (
                specs[0].strategy_id if len(specs) == 1 else ""
            )
            self.strategy_ids = tuple(
                sorted({str(spec.strategy_id or "").strip() for spec in specs})
            )
            self.allowed_symbols = tuple(
                sorted({str(spec.symbol or "").strip().upper() for spec in specs})
            )
            self.execution_purpose = normalized_purpose
            self.functional_test_context = (
                functional_context
                if normalized_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                else {}
            )
            self.portfolio_instance_id = (
                (
                    f"functional-portfolio:"
                    f"{str(((loaded.payload.get('artifact_reference') or {}).get('artifactId') if isinstance(loaded.payload.get('artifact_reference'), dict) else '') or runtime_id).strip()}:"
                    f"{str(((loaded.payload.get('artifact_reference') or {}).get('artifactHash') if isinstance(loaded.payload.get('artifact_reference'), dict) else '') or runtime_hash).strip().lower()[:12]}"
                    if normalized_purpose
                    == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                    else str(
                        loaded.payload.get("portfolioInstanceId")
                        or loaded.payload.get("portfolio_instance_id")
                        or f"portfolio:{runtime_id}"
                    ).strip()
                )
                if loaded is not None
                else ""
            )
            feeds = feeds_for_specs(
                specs,
                prefer_kis=True,
                kis_demo=False,
                kis_app_key=os.getenv("KIS_APP_KEY", ""),
                kis_app_secret=os.getenv("KIS_APP_SECRET", ""),
                kis_account_id=(
                    f"{os.getenv('KIS_ACCOUNT_NO', '').strip()}-"
                    f"{os.getenv('KIS_ACCOUNT_PRODUCT_CODE', '').strip()}"
                ),
                kis_websocket_owner_id=(
                    "live_trader:"
                    + (
                        str(
                            functional_context.get(
                                "functional_test_session_id"
                            )
                            or ""
                        ).strip()
                        if normalized_purpose
                        == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                        else (
                            f"{normalized_profile}:{requested_deployment or requested_portfolio or requested_strategy or os.getpid()}"
                        )
                    )
                ),
                kis_private_tr_key=(
                    os.getenv("KIS_HTS_ID", "").strip()
                    if normalized_purpose
                    == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                    else ""
                ),
                kis_private_execution_sink=(
                    state.ingest_functional_test_kis_private_execution
                    if normalized_purpose
                    == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
                    else None
                ),
            )
            for feed in feeds:
                feed.connect()
            warmup: dict[str, int] = {}
            for spec in specs:
                feed = next(item for item in feeds if item.provider_id == spec.provider)
                bars = feed.warmup(FeedSubscription(spec.instrument_id, spec.symbol, spec.timeframe), required_warmup_bars(spec))
                warmup[spec.strategy_instance_id] = engine.seed_history(spec.strategy_instance_id, bars)
            for feed in feeds:
                feed.disconnect()
            self.supervisor = ContinuousRuntimeSupervisor(
                engine=engine,
                feeds=feeds,
                status_store=DurableRuntimeState(self.root / "logs" / f"continuous_{normalized_profile}_status.json"),
                poll_seconds=2.0,
                heartbeat_seconds=5.0,
                operation_lock=state.RUNTIME_MODE_LOCK,
            )
            self.supervisor.start()
            source_kind = "Portfolio" if loaded is not None else "Standalone Strategy"
            state.append_audit("info", "Continuous Runtime", f"{runtime_id} · {source_kind} · {normalized_profile} · {normalized_mode} 시작, {len(specs)}개 전략")
            return {"ok": True, "reason": "continuous runtime started", "warmup": warmup, "runtime": self.snapshot(), "snapshot": state.snapshot()}

    def stop(self) -> dict[str, Any]:
        from . import state

        with self._lock:
            if self.supervisor is None:
                self.mode = "MONITOR"
                return {"ok": True, "reason": "continuous runtime already stopped", "runtime": self.snapshot(), "snapshot": state.snapshot()}
            supervisor = self.supervisor
            # A due-bar cycle and a mode transition share this lock, so the
            # worker observes one complete mode.  Release it before stop()
            # joins: the worker may currently be waiting to flush a due bar.
            with state.RUNTIME_MODE_LOCK:
                transition_blocker = supervisor.engine.transition_mode(
                    "MONITOR"
                )
                self.mode = "MONITOR"
        result = supervisor.stop()
        phase = str(result.get("phase") or "").upper()
        running = bool(result.get("running"))
        last_error = str(result.get("lastError") or "").strip()
        stopped = not transition_blocker and phase == "STOPPED" and not running
        if transition_blocker:
            reason = f"MONITOR 전환 실패: {transition_blocker}"
        elif phase == "FAILED" or running:
            reason = last_error or "continuous runtime stop failed"
        elif not stopped:
            reason = (
                f"continuous runtime stop 상태가 확정되지 않았습니다: "
                f"{phase or 'UNKNOWN'}"
            )
        else:
            reason = "continuous runtime stopped"
        with self._lock:
            state.append_audit(
                "info" if stopped else "danger",
                "Continuous Runtime",
                (
                    f"{self.profile_id or '-'} runtime 정지"
                    if stopped
                    else f"{self.profile_id or '-'} runtime 정지 실패: {reason}"
                ),
            )
            return {
                "ok": stopped,
                "reason": reason,
                "runtime": result,
                "snapshot": state.snapshot(),
            }

    def snapshot(self) -> dict[str, Any]:
        base = self.supervisor.snapshot() if self.supervisor is not None else {
            "schemaVersion": "continuous-runtime-supervisor-v1", "phase": "STOPPED", "running": False,
            "startedAt": "", "stoppedAt": "", "lastHeartbeat": "", "lastDataAt": "", "lastError": "",
            "reconnectCount": 0, "feedErrors": {}, "feeds": [], "engine": {},
        }
        return {
            **base,
            "profileId": self.profile_id,
            "mode": self.mode,
            "portfolioPath": self.portfolio_path,
            "deploymentId": self.deployment_id,
            "portfolioId": self.portfolio_id,
            "requestedStrategyId": self.requested_strategy_id,
            "strategyIds": list(self.strategy_ids),
            "allowedSymbols": list(self.allowed_symbols),
            "executionPurpose": self.execution_purpose,
            "promotionEligible": (
                False
                if self.execution_purpose == "FUNCTIONAL_TEST"
                else None
            ),
        }

    def _running_context_blocker(
        self,
        *,
        portfolio_id: str,
        strategy_id: str,
        deployment_id: str,
        execution_purpose: str,
    ) -> str:
        """Refuse a mode transition when it targets another deployment."""

        explicit = bool(
            portfolio_id
            or strategy_id
            or deployment_id
            or execution_purpose
            or self.execution_purpose
        )
        if not explicit:
            return ""
        mismatches: list[str] = []
        if portfolio_id != self.portfolio_id:
            mismatches.append(
                f"portfolio={portfolio_id or 'standalone'} (running={self.portfolio_id or 'standalone'})"
            )
        if deployment_id and deployment_id != self.deployment_id:
            mismatches.append(
                f"deployment={deployment_id} (running={self.deployment_id or '-'})"
            )
        if strategy_id:
            if self.requested_strategy_id:
                if strategy_id != self.requested_strategy_id:
                    mismatches.append(
                        f"strategy={strategy_id} (running={self.requested_strategy_id})"
                    )
            elif strategy_id not in self.strategy_ids:
                mismatches.append(
                    f"strategy={strategy_id} (running={','.join(self.strategy_ids) or '-'})"
                )
        if execution_purpose != self.execution_purpose:
            mismatches.append(
                "executionPurpose="
                f"{execution_purpose or 'STANDARD'} "
                f"(running={self.execution_purpose or 'STANDARD'})"
            )
        return (
            "실행 중인 runtime과 요청한 Deployment 컨텍스트가 일치하지 않아 전환을 차단했습니다: "
            + "; ".join(mismatches)
            if mismatches
            else ""
        )

    @staticmethod
    def _functional_test_spec_blocker(specs: tuple[Any, ...]) -> str:
        for spec in specs:
            broker_id = str(spec.broker_id or "").strip().lower()
            symbol = str(spec.symbol or "").strip().upper()
            local_code = symbol.removesuffix(".KS").removesuffix(".KQ")
            if broker_id != "kis":
                return "FUNCTIONAL_TEST는 KIS broker route만 허용합니다."
            if not (
                local_code.isdigit() and len(local_code) == 6
            ):
                return (
                    "첫 FUNCTIONAL_TEST는 국내주식·ETF 6자리 종목만 "
                    f"허용합니다: {symbol or '-'}"
                )
        return ""

    @staticmethod
    def _single_broker_context_blocker(specs: tuple[Any, ...]) -> str:
        broker_ids = {
            str(spec.broker_id or "").strip().lower() for spec in specs
        }
        if "" in broker_ids or len(broker_ids) != 1:
            routes = ", ".join(sorted(item or "unresolved" for item in broker_ids))
            return (
                "현재 Preflight는 단일 Broker 계좌만 봉인하므로 cross-broker "
                f"Portfolio runtime을 차단했습니다: {routes or 'unresolved'}"
            )
        return ""

    @classmethod
    def _portfolio_execution_spec_blocker(
        cls, specs: tuple[Any, ...]
    ) -> str:
        """Limit the first durable sleeve ledger to KIS domestic cash longs."""

        if len(specs) <= 1:
            return ""
        portfolio_ids = {str(spec.portfolio_id or "").strip() for spec in specs}
        portfolio_hashes = {
            str(spec.portfolio_hash or "").strip().lower() for spec in specs
        }
        sleeve_ids = [
            str(spec.strategy_instance_id or "").strip() for spec in specs
        ]
        if "" in portfolio_ids or len(portfolio_ids) != 1:
            return "multi-sleeve runtime의 Portfolio ID가 하나로 고정되지 않았습니다."
        if "" in portfolio_hashes or len(portfolio_hashes) != 1:
            return "multi-sleeve runtime의 Portfolio hash가 하나로 고정되지 않았습니다."
        if any(not item for item in sleeve_ids) or len(set(sleeve_ids)) != len(
            sleeve_ids
        ):
            return "multi-sleeve runtime의 Strategy Instance ID가 비었거나 중복되었습니다."
        for spec in specs:
            if str(spec.broker_id or "").strip().lower() != "kis":
                return (
                    "다중 Sleeve 실주문은 현재 KIS 국내주식 단일 계좌만 "
                    "지원합니다."
                )
            if cls._position_direction(spec) != "long":
                return "KIS 다중 Sleeve 원장은 현물 long-only 전략만 지원합니다."
            try:
                canonical_kis_symbol(spec.symbol)
            except ValueError as exc:
                return str(exc)
        return ""

    @staticmethod
    def _loaded_portfolio_lineage_blocker(
        loaded: Any,
        selected_specs: tuple[Any, ...],
        *,
        require_complete: bool,
    ) -> str:
        """Bind every runtime sleeve to the exact sealed Portfolio member."""

        if loaded is None:
            return ""
        payload = loaded.payload if isinstance(loaded.payload, dict) else {}
        raw_instances = payload.get("strategyInstances")
        if not isinstance(raw_instances, list) or not raw_instances:
            return "Portfolio Artifact의 strategyInstances가 비어 있습니다."
        expected: dict[str, tuple[str, str, str]] = {}
        for index, raw in enumerate(raw_instances):
            if not isinstance(raw, dict):
                return "Portfolio Artifact의 sleeve 형식이 올바르지 않습니다."
            instance_id = str(
                raw.get("instanceId")
                or raw.get("templateInstanceId")
                or f"instance-{index + 1}"
            ).strip()
            strategy_id = str(
                raw.get("strategyId")
                or raw.get("sourceStrategyId")
                or instance_id
            ).strip()
            artifact_hash = str(raw.get("sourceArtifactHash") or "").strip().lower()
            source_instance_hash = str(
                raw.get("sourceInstanceHash") or ""
            ).strip().lower()
            if (
                not instance_id
                or not strategy_id
                or not artifact_hash
                or not source_instance_hash
            ):
                return (
                    "Portfolio sleeve의 sourceStrategyId/sourceArtifactHash/"
                    "instanceId/sourceInstanceHash가 완전하지 않습니다."
                )
            if instance_id in expected:
                return f"Portfolio sleeve instance가 중복되었습니다: {instance_id}"
            expected[instance_id] = (
                strategy_id,
                artifact_hash,
                source_instance_hash,
            )
        actual: dict[str, Any] = {}
        for spec in selected_specs:
            instance_id = str(spec.strategy_instance_id or "").strip()
            if instance_id in actual:
                return f"로드된 runtime sleeve가 중복되었습니다: {instance_id}"
            actual[instance_id] = spec
        if require_complete and set(actual) != set(expected):
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            return (
                "Portfolio permit은 모든 sleeve를 함께 봉인해야 합니다: "
                f"missing={','.join(missing) or '-'}, extra={','.join(extra) or '-'}"
            )
        for instance_id, spec in actual.items():
            lineage = expected.get(instance_id)
            if lineage is None:
                return f"로드된 sleeve가 Portfolio payload에 없습니다: {instance_id}"
            strategy_id, artifact_hash, source_instance_hash = lineage
            spec_artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
            if str(spec.strategy_id or "").strip() != strategy_id:
                return f"Portfolio sleeve strategy ID 불일치: {instance_id}"
            if str(spec.artifact_hash or "").strip().lower() != artifact_hash:
                return f"Portfolio sleeve strategy artifact hash 불일치: {instance_id}"
            if str(spec_artifact.get("sourceStrategyId") or spec_artifact.get("strategyId") or "").strip() != strategy_id:
                return f"로드된 sleeve sourceStrategyId 불일치: {instance_id}"
            if str(spec_artifact.get("sourceArtifactHash") or "").strip().lower() != artifact_hash:
                return f"로드된 sleeve sourceArtifactHash 불일치: {instance_id}"
            if str(spec_artifact.get("sourceInstanceHash") or "").strip().lower() != source_instance_hash:
                return f"로드된 sleeve sourceInstanceHash 불일치: {instance_id}"
        return ""

    def _configure_portfolio_execution(
        self,
        specs: tuple[Any, ...],
        *,
        portfolio_id: str,
        portfolio_hash: str,
        require_account: bool,
    ) -> str:
        from . import state

        if len(specs) <= 1:
            self.portfolio_ledger = None
            self.portfolio_execution_scope_id = ""
            self.portfolio_execution_account_id = ""
            self.portfolio_execution_symbols = ()
            return ""
        blocker = self._portfolio_execution_spec_blocker(specs)
        if blocker:
            self.portfolio_ledger = None
            return blocker if require_account else ""
        account_id = str(state.kis_functional_test_account_id() or "").strip()
        if not account_id:
            self.portfolio_ledger = None
            self.portfolio_execution_scope_id = ""
            self.portfolio_execution_account_id = ""
            self.portfolio_execution_symbols = ()
            return (
                "KIS 다중 Sleeve 실주문 원장을 계좌에 봉인할 수 없습니다. "
                "KIS_ACCOUNT_NO와 KIS_ACCOUNT_PRODUCT_CODE를 확인하세요."
                if require_account
                else ""
            )
        normalized_portfolio = str(portfolio_id or "").strip()
        normalized_hash = str(portfolio_hash or "").strip().lower()
        if not normalized_portfolio or not normalized_hash:
            return "KIS 다중 Sleeve 원장에 필요한 Portfolio identity가 없습니다."
        scope_id = f"live:{normalized_portfolio}:{normalized_hash}"
        try:
            ledger = LivePortfolioLedger(
                self.root / "logs" / "live_portfolio_sleeves.sqlite3"
            )
            ledger.register_scope(
                scope_id=scope_id,
                portfolio_id=normalized_portfolio,
                portfolio_hash=normalized_hash,
                account_id=account_id,
            )
        except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self.portfolio_ledger = None
            return (
                "KIS 다중 Sleeve 원장을 열거나 계좌에 봉인하지 못했습니다: "
                f"{type(exc).__name__}: {str(exc)[:240]}"
            )
        self.portfolio_ledger = ledger
        self.portfolio_execution_scope_id = scope_id
        self.portfolio_execution_account_id = account_id
        self.portfolio_execution_symbols = tuple(
            sorted({canonical_kis_symbol(spec.symbol) for spec in specs})
        )
        self._portfolio_last_sync_monotonic = 0.0
        return ""

    @staticmethod
    def _normalize_kis_broker_holdings(
        rows: Any,
        *,
        require_kis_only: bool = False,
    ) -> dict[str, Decimal]:
        if not isinstance(rows, list):
            raise ValueError(
                "KIS 전체 잔고 응답이 목록이 아니어서 전용 계좌 대조를 "
                "차단했습니다."
            )
        result: dict[str, Decimal] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError(
                    "KIS 전체 잔고에 잘못된 행이 있어 전용 계좌 대조를 "
                    "차단했습니다."
                )
            broker_id = str(row.get("broker_id") or "").strip().lower()
            if not broker_id:
                raise ValueError(
                    "전체 잔고 행의 broker_id가 없어 KIS 전용 계좌 대조를 "
                    "차단했습니다."
                )
            if broker_id != "kis":
                if require_kis_only:
                    raise ValueError(
                        "KIS 직접 잔고 조회에 다른 broker 행이 섞여 있어 "
                        "전용 계좌 대조를 차단했습니다."
                    )
                continue
            try:
                raw_quantity = row.get("quantity")
                if raw_quantity in {None, ""}:
                    raw_quantity = row.get("qty")
                if raw_quantity in {None, ""}:
                    raw_quantity = row.get("broker_qty")
                if raw_quantity in {None, ""}:
                    raise ValueError("KIS position quantity is missing")
                quantity = Decimal(str(raw_quantity))
                if not quantity.is_finite():
                    raise ValueError("non-finite KIS position")
            except (ArithmeticError, TypeError, ValueError) as exc:
                raise ValueError(
                    "KIS 전체 잔고의 수량을 검증할 수 없어 전용 계좌 대조를 "
                    f"차단했습니다: {str(exc)[:160]}"
                ) from exc
            if quantity == 0:
                continue
            if quantity < 0 or quantity != quantity.to_integral_value():
                raise ValueError(
                    "KIS 국내 현물 전용 계좌에 음수 또는 소수 보유수량이 있어 "
                    "Sleeve 귀속을 증명할 수 없습니다."
                )
            try:
                symbol = canonical_kis_symbol(
                    row.get("symbol") or row.get("instrument_id")
                )
            except ValueError as exc:
                raise ValueError(
                    "KIS 다중 Sleeve 전용 계좌에 해외/미지원 보유종목이 있어 "
                    f"실주문을 차단했습니다: {str(exc)[:160]}"
                ) from exc
            result[symbol] = result.get(symbol, Decimal("0")) + quantity
        return result

    @classmethod
    def _kis_broker_holdings(cls) -> dict[str, Decimal]:
        from . import state

        rows = state.STATE.get("broker_reconciliation", {}).get("positions", [])
        return cls._normalize_kis_broker_holdings(rows)

    def _sync_portfolio_execution(
        self, *, force: bool = False
    ) -> ExecutionSyncReport:
        from . import state

        ledger = self.portfolio_ledger
        scope_id = self.portfolio_execution_scope_id
        if ledger is None or not scope_id:
            return ExecutionSyncReport(0, 0, 0)
        with self._portfolio_sync_lock:
            now = time.monotonic()
            if not force and now - self._portfolio_last_sync_monotonic < 0.25:
                return ExecutionSyncReport(0, 0, 0)
            dispatch_rows = state.PROGRAM_LEDGER.order_dispatch_rows(1_000_000)
            ledger.recover_accepted_orders(scope_id, dispatch_rows)
            execution_rows = state.PROGRAM_LEDGER.execution_event_rows(1_000_000)
            report = ledger.apply_execution_events(scope_id, execution_rows)
            self._portfolio_last_sync_monotonic = now
            return report

    def _runtime_position_quantity(self, spec: Any) -> float:
        from . import state

        if self.portfolio_ledger is not None and self.portfolio_execution_scope_id:
            self._sync_portfolio_execution()
            return float(
                self.portfolio_ledger.sleeve_quantity(
                    self.portfolio_execution_scope_id,
                    str(spec.strategy_instance_id or ""),
                    str(spec.symbol or ""),
                )
            )
        return state.broker_position_quantity(
            spec.symbol,
            spec.broker_id,
            "SHORT" if self._position_direction(spec) == "short" else "LONG",
        )

    def _portfolio_reconciliation_blocker(
        self, specs: tuple[Any, ...]
    ) -> str:
        """Fail closed before live whenever sleeve ownership is not exact."""

        if len(specs) <= 1:
            return ""
        blocker = self._portfolio_execution_spec_blocker(specs)
        if blocker:
            return blocker
        if self.portfolio_ledger is None or not self.portfolio_execution_scope_id:
            blocker = self._configure_portfolio_execution(
                specs,
                portfolio_id=str(specs[0].portfolio_id or ""),
                portfolio_hash=str(specs[0].portfolio_hash or ""),
                require_account=True,
            )
            if blocker:
                return blocker
        assert self.portfolio_ledger is not None
        try:
            self._sync_portfolio_execution(force=True)
            reconciliation = self.portfolio_ledger.reconcile_restart(
                scope_id=self.portfolio_execution_scope_id,
                broker_holdings=self._kis_broker_holdings(),
                managed_symbols=self.portfolio_execution_symbols,
                persist=True,
            )
        except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
            return (
                "KIS 다중 Sleeve 재시작 대조를 완료하지 못했습니다: "
                f"{type(exc).__name__}: {str(exc)[:240]}"
            )
        if reconciliation.mismatches:
            detail = ", ".join(
                (
                    f"{item['symbol']} ledger={item['ledgerQuantity']} "
                    f"broker={item['brokerQuantity']}"
                )
                for item in reconciliation.mismatches[:5]
            )
            return f"KIS Sleeve 합계와 실제 계좌 보유량이 달라 실주문을 차단했습니다: {detail}"
        if reconciliation.pending_orders:
            return (
                "KIS 다중 Sleeve의 미종결 broker 주문을 먼저 체결·취소 대조해야 합니다: "
                + ", ".join(reconciliation.pending_orders[:5])
            )
        if reconciliation.external_holdings:
            detail = ", ".join(
                f"{symbol}={quantity}"
                for symbol, quantity in sorted(
                    reconciliation.external_holdings.items()
                )[:5]
            )
            return (
                "KIS 다중 Sleeve는 전용 계좌 정책을 사용합니다. Portfolio 밖의 "
                f"보유종목을 다른 계좌로 분리해야 합니다: {detail}"
            )
        return ""

    def validate_portfolio_execution_dispatch(
        self, intent: OrderIntent
    ) -> tuple[bool, str, dict[str, Any]]:
        """Re-read complete KIS truth immediately before the broker POST."""

        from . import state

        metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
        raw_plan = metadata.get("portfolio_execution")
        report: dict[str, Any] = {
            "schemaVersion": "live-portfolio-pre-post-validation-v1",
            "scopeId": self.portfolio_execution_scope_id,
            "allowed": False,
            "reason": "portfolio-execution-validation-incomplete",
        }
        try:
            plan = validate_symbol_net_plan_metadata(raw_plan)
            if self.portfolio_ledger is None:
                raise ValueError("portfolio sleeve ledger is not configured")
            if plan.scope_id != self.portfolio_execution_scope_id:
                raise ValueError("portfolio execution scope does not match runtime")
            if plan.portfolio_id != self.portfolio_id:
                raise ValueError("portfolio execution artifact id does not match runtime")
            if plan.side != str(intent.side or "").strip().upper():
                raise ValueError("portfolio execution side does not match intent")
            if plan.quantity != Decimal(str(intent.quantity)):
                raise ValueError("portfolio execution quantity does not match intent")
            if plan.symbol != canonical_kis_symbol(intent.symbol):
                raise ValueError("portfolio execution symbol does not match intent")
            specs = tuple(self.supervisor.engine.specs) if self.supervisor else ()
            symbol_specs = tuple(
                spec
                for spec in specs
                if canonical_kis_symbol(spec.symbol) == plan.symbol
            )
            expected_sleeves = {
                str(spec.strategy_instance_id or "") for spec in symbol_specs
            }
            plan_sleeves = {item.sleeve_id for item in plan.deltas}
            if not expected_sleeves or plan_sleeves != expected_sleeves:
                raise ValueError(
                    "portfolio execution does not bind every runtime sleeve for symbol"
                )
            if any(
                str(spec.portfolio_hash or "").strip().lower()
                != plan.portfolio_hash
                or str(spec.portfolio_id or "").strip() != plan.portfolio_id
                for spec in symbol_specs
            ):
                raise ValueError("portfolio execution lineage does not match runtime")

            # Consume all already-durable ACK/fill/cost evidence first.  The
            # current dispatch row has no broker order id yet and is ignored.
            self._sync_portfolio_execution(force=True)
            pending = self.portfolio_ledger.pending_orders(plan.scope_id)
            if pending:
                raise ValueError(
                    "portfolio has unresolved broker orders: "
                    + ",".join(pending[:5])
                )
            fresh_positions = state.LiveBrokerRouter().list_positions("kis")
            fresh_holdings = self._normalize_kis_broker_holdings(
                fresh_positions,
                require_kis_only=True,
            )
            reconciliation = self.portfolio_ledger.reconcile_restart(
                scope_id=plan.scope_id,
                broker_holdings=fresh_holdings,
                managed_symbols=self.portfolio_execution_symbols,
                persist=False,
            )
            if not reconciliation.ready:
                if reconciliation.external_holdings:
                    raise ValueError("dedicated KIS account has external holdings")
                if reconciliation.mismatches:
                    raise ValueError("fresh KIS holdings do not match sleeve ledger")
                raise ValueError("portfolio reconciliation is not ready")
            sleeve_holdings = self.portfolio_ledger.sleeve_holdings(plan.scope_id)
            for delta in plan.deltas:
                latest = sleeve_holdings.get(delta.sleeve_id, {}).get(
                    plan.symbol, Decimal("0")
                )
                if latest != delta.current_quantity:
                    raise ValueError(
                        "portfolio plan base sleeve position changed before POST: "
                        f"{delta.sleeve_id} {delta.current_quantity}->{latest}"
                    )
            if plan.side == "SELL" and fresh_holdings.get(
                plan.symbol, Decimal("0")
            ) < plan.quantity:
                raise ValueError("fresh KIS holding is insufficient for portfolio sell")
        except (
            OSError,
            sqlite3.Error,
            ArithmeticError,
            TypeError,
            ValueError,
            RuntimeError,
        ) as exc:
            reason = f"portfolio-pre-post-validation-blocked:{type(exc).__name__}:{str(exc)[:240]}"
            report.update({"allowed": False, "reason": reason})
            return False, reason, report
        report.update(
            {
                "allowed": True,
                "reason": "portfolio-pre-post-validation-passed",
                "planId": plan.plan_id,
                "symbol": plan.symbol,
                "side": plan.side,
                "quantity": str(plan.quantity),
                "brokerHolding": str(
                    fresh_holdings.get(plan.symbol, Decimal("0"))
                ),
                "sleeveCount": len(plan.deltas),
            }
        )
        return True, str(report["reason"]), report

    def _select_portfolio(
        self,
        profile_id: str,
        portfolio_id: str,
        mode: str = "MONITOR",
    ) -> dict[str, Any] | None:
        candidates = self._portfolio_candidates(
            profile_id,
            portfolio_id,
            mode,
        )
        return candidates[0] if candidates else None

    def _select_runtime_portfolio(
        self,
        profile_id: str,
        portfolio_id: str,
        mode: str = "MONITOR",
    ) -> dict[str, Any] | None:
        """Choose the newest loadable artifact for an automatic start.

        An explicit ID remains fail-closed so the caller sees its integrity
        error. Automatic MONITOR startup can skip legacy portfolio locks and
        fall back to another trusted portfolio or a verified standalone
        strategy.
        """

        candidates = self._portfolio_candidates(
            profile_id,
            portfolio_id,
            mode,
        )
        if portfolio_id:
            return candidates[0] if candidates else None
        for candidate in candidates:
            source_path = str(candidate.get("source_path") or "")
            if not source_path or not Path(source_path).is_file():
                continue
            try:
                load_portfolio_runtime_path(source_path)
            except (OSError, ValueError):
                continue
            return candidate
        return None

    def _portfolio_candidates(
        self,
        profile_id: str,
        portfolio_id: str,
        mode: str = "MONITOR",
    ) -> list[dict[str, Any]]:
        from . import state

        strategy_rows = state.strategy_rows([])
        strategies_by_id: dict[str, list[dict[str, Any]]] = {}
        for strategy in strategy_rows:
            strategy_id = str(strategy.get("strategy_id") or "")
            if strategy_id:
                strategies_by_id.setdefault(strategy_id, []).append(strategy)

        candidates: list[dict[str, Any]] = []
        for portfolio in state.portfolio_rows():
            if portfolio_id and str(portfolio.get("id") or "") != portfolio_id:
                continue
            instances = portfolio.get("strategy_instances") if isinstance(portfolio.get("strategy_instances"), list) else []
            symbols = [str(item.get("symbol") or item.get("qualifiedSymbol") or "") for item in instances if isinstance(item, dict)]
            if (
                any(self._symbol_matches_profile(symbol, profile_id) for symbol in symbols)
                and self._portfolio_components_eligible(portfolio, strategies_by_id, mode)
            ):
                candidates.append(portfolio)
        # contracts.load_portfolio_artifacts() returns newest artifacts first.
        return candidates

    @staticmethod
    def _portfolio_components_eligible(
        portfolio: dict[str, Any],
        strategies_by_id: dict[str, list[dict[str, Any]]],
        mode: str,
    ) -> bool:
        from . import state

        portfolio_lifecycle = state.normalize_lifecycle_status(portfolio.get("lifecycle_status"))
        if portfolio_lifecycle in {"paused", "retired"}:
            return False
        instances = portfolio.get("strategy_instances") if isinstance(portfolio.get("strategy_instances"), list) else []
        if not instances:
            return False
        for instance in instances:
            if not isinstance(instance, dict):
                return False
            strategy_id = str(instance.get("sourceStrategyId") or instance.get("strategyId") or "")
            source_hash = str(instance.get("sourceArtifactHash") or "")
            matches = strategies_by_id.get(strategy_id, [])
            if source_hash:
                matches = [
                    strategy
                    for strategy in matches
                    if str(strategy.get("artifact_hash") or "") in {"", source_hash}
                ]
            strategy = matches[0] if matches else None
            if strategy is None:
                return False
            lifecycle = state.normalize_lifecycle_status(strategy.get("lifecycle_status"))
            if lifecycle in {"paused", "retired"} or state.lifecycle_rank(lifecycle) < state.lifecycle_rank("backtested"):
                return False
            if strategy.get("backtester_verified") is not True:
                return False
            if mode == "SMALL_LIVE" and strategy.get("live_small_eligible") is not True:
                return False
            if mode == "FULL_LIVE" and strategy.get("live_eligible") is not True:
                return False
        return True

    def _select_standalone_strategy(
        self,
        profile_id: str,
        mode: str,
        strategy_id: str = "",
        deployment_id: str = "",
    ) -> dict[str, Any] | None:
        from . import state

        requested_strategy = str(strategy_id or "").strip()
        requested_deployment = str(deployment_id or "").strip()
        for strategy in state.strategy_rows():
            if requested_strategy and str(strategy.get("strategy_id") or "") != requested_strategy:
                continue
            if requested_deployment and str(strategy.get("deployment_id") or "") != requested_deployment:
                continue
            lifecycle = state.normalize_lifecycle_status(
                strategy.get("lifecycle_status")
            )
            if mode == "MONITOR":
                if (
                    lifecycle in {"paused", "retired"}
                    or state.lifecycle_rank(lifecycle)
                    < state.lifecycle_rank("backtested")
                    or strategy.get("backtester_verified") is not True
                ):
                    continue
            else:
                required = (
                    "live_eligible"
                    if mode == "FULL_LIVE"
                    else "live_small_eligible"
                )
                if strategy.get(required) is not True:
                    continue
            symbol = str(strategy.get("symbol") or "")
            if self._symbol_matches_profile(symbol, profile_id):
                return strategy
        return None

    @staticmethod
    def _standalone_spec(strategy: dict[str, Any]) -> RuntimeStrategySpec:
        symbol = str(strategy.get("symbol") or "").strip().upper()
        market_type = str(
            strategy.get("market_type")
            or strategy.get("marketType")
            or ""
        ).strip().lower()
        provider, broker_id, instrument_id = infer_market_route(
            symbol,
            market_type,
        )
        provider = str(strategy.get("provider") or provider).strip().lower()
        broker_id = str(strategy.get("broker_id") or broker_id).strip().lower()
        strategy_id = str(strategy.get("strategy_id") or strategy.get("id") or "").strip()
        instance_id = str(
            strategy.get("strategy_instance_id")
            or strategy.get("instance_id")
            or f"standalone:{strategy_id}"
        )
        artifact_hash = str(
            strategy.get("artifact_hash")
            or ((strategy.get("artifactLock") or {}).get("artifactHash") if isinstance(strategy.get("artifactLock"), dict) else "")
            or artifact_content_hash(strategy)
        )
        return RuntimeStrategySpec(
            portfolio_id=f"standalone:{strategy_id}",
            portfolio_hash=artifact_hash,
            strategy_instance_id=instance_id,
            strategy_id=strategy_id,
            artifact_hash=artifact_hash,
            plugin_id=str(strategy.get("plugin") or strategy.get("pluginId") or "").strip(),
            instrument_id=instrument_id,
            symbol=symbol,
            timeframe=str(strategy.get("timeframe") or "1d").strip().lower(),
            provider=provider,
            broker_id=broker_id,
            target_weight=max(0.0, float(strategy.get("parameters", {}).get("positionSize", 100.0))) / 100.0,
            parameters=dict(strategy.get("parameters") or {}),
            artifact={**strategy, "standalone": True},
        )

    @staticmethod
    def _symbol_matches_profile(symbol: str, profile_id: str) -> bool:
        text = str(symbol).upper()
        crypto = text.startswith("KRW-") or text.endswith("USDT") or "BTC" in text or "ETH" in text
        return crypto if profile_id == "crypto" else not crypto

    def _matches_profile(self, spec: Any, profile_id: str) -> bool:
        return self._symbol_matches_profile(spec.symbol, profile_id)

    @staticmethod
    def _spec_mode_allowed(spec: Any, mode: str) -> bool:
        if mode == "MONITOR":
            return True
        artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
        market_type = str(
            artifact.get("market_type")
            or artifact.get("marketType")
            or ""
        ).strip().lower()
        if market_type in {"future", "futures", "perpetual"}:
            policy = futures_execution_policy_from_artifact(artifact)
            if policy.get("valid") is not True:
                return False
        permissions = artifact.get("portfolioPermissions") if isinstance(artifact.get("portfolioPermissions"), dict) else {}
        if not permissions:
            permissions = artifact.get("permissions") if isinstance(artifact.get("permissions"), dict) else {}
        if mode == "FULL_LIVE":
            return permissions.get("live_eligible") is True or permissions.get("live_allowed") is True
        return (
            permissions.get("live_small_eligible") is True
            or permissions.get("live_small_allowed") is True
            or permissions.get("live_eligible") is True
            or permissions.get("live_allowed") is True
        )

    @staticmethod
    def _paper_final_binding_blocker(specs: tuple[Any, ...]) -> str:
        """Re-read the pinned qualification before start or hot transition."""

        from . import state

        rows = state.strategy_rows(state.portfolio_rows())
        for spec in specs:
            strategy_id = str(spec.strategy_id or "").strip()
            instance_id = str(spec.strategy_instance_id or "").strip()
            portfolio_id = str(spec.portfolio_id or "").strip()
            standalone = bool(
                isinstance(spec.artifact, dict)
                and spec.artifact.get("standalone") is True
            )
            candidate = next(
                (
                    item
                    for item in rows
                    if str(item.get("strategy_id") or "") == strategy_id
                    and (
                        standalone
                        or str(
                            (item.get("portfolio_gate") or {}).get("portfolioId")
                            if isinstance(item.get("portfolio_gate"), dict)
                            else ""
                        )
                        == portfolio_id
                    )
                ),
                None,
            )
            if candidate is None:
                return (
                    "exact-paper-final-binding-current-strategy-missing:"
                    + strategy_id
                )
            gate = state.paper_live_qualification_gate_for_strategy(candidate)
            if gate.get("required") is not True or gate.get("ready") is not True:
                return (
                    "exact-paper-final-binding-invalid:"
                    + strategy_id
                    + ":"
                    + ",".join(str(item) for item in gate.get("issues") or [])
                )
            if str(gate.get("strategyInstanceId") or "") != instance_id:
                return "exact-paper-final-binding-instance-mismatch:" + strategy_id
            if not standalone and str(gate.get("portfolioArtifactId") or "") != portfolio_id:
                return "exact-paper-final-binding-portfolio-mismatch:" + strategy_id
            if not standalone:
                current_portfolio_gate = (
                    candidate.get("portfolio_gate")
                    if isinstance(candidate.get("portfolio_gate"), dict)
                    else {}
                )
                expected_portfolio_hash = str(
                    gate.get("portfolioArtifactHash") or ""
                ).strip().lower()
                current_portfolio_hash = str(
                    current_portfolio_gate.get("portfolioArtifactHash") or ""
                ).strip().lower()
                if (
                    not expected_portfolio_hash
                    or not current_portfolio_hash
                    or expected_portfolio_hash != current_portfolio_hash
                ):
                    return (
                        "exact-paper-final-binding-portfolio-hash-mismatch:"
                        + strategy_id
                    )
        return ""

    @staticmethod
    def _state_context_reconciled(specs: tuple[Any, ...]) -> bool:
        """Legacy cache-only probe retained for diagnostics, never unlocking live."""

        from . import state

        broker_ids = {
            str(spec.broker_id or "").strip().lower()
            for spec in specs
            if str(spec.broker_id or "").strip()
        }
        if not broker_ids:
            return False
        successful = state.successful_position_brokers()
        if not broker_ids.issubset(successful):
            return False
        for broker_id in broker_ids:
            summary = state.reconciliation_summary_for_broker(broker_id)
            if (
                state.reconciliation_blocker_count(summary) > 0
                or int(summary.get("capability_gap_count") or 0) > 0
            ):
                return False
        return True

    @staticmethod
    def _restore_context_assessment(
        specs: tuple[Any, ...],
        engine: PortfolioRuntimeEngine,
    ) -> dict[str, Any]:
        from . import state

        export_state = getattr(engine.evaluator, "export_state", None)
        evaluator_state = (
            export_state(engine.specs)
            if callable(export_state)
            else {}
        )
        return state.forced_restore_context_assessment(
            specs,
            portfolio_id=engine.portfolio_id,
            portfolio_hash=engine.portfolio_hash,
            strategy_identity_hash=engine.strategy_identity_hash,
            checkpoint_seal=engine.restore_context_seal,
            evaluator_state=evaluator_state,
        )

    def _handle_cycle(self, cycle: Any) -> dict[str, Any]:
        # The supervisor acquires this lock before engine evaluation and keeps
        # it through this handler.  Re-entering it here also protects direct
        # unit/integration calls without introducing an inverse controller
        # lock order.
        from . import state

        with state.RUNTIME_MODE_LOCK:
            return self._handle_cycle_locked(cycle)

    def _handle_cycle_locked(self, cycle: Any) -> dict[str, Any]:
        from . import state

        specs = tuple(self.supervisor.engine.specs) if self.supervisor else ()
        if len(specs) > 1:
            return self._handle_portfolio_cycle_locked(cycle, specs)
        results: list[dict[str, Any]] = []
        for decision in cycle.decisions:
            state.STATE["strategy_runner"].update({
                "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_profile": self.profile_id,
                "last_strategy": decision.strategy_id,
                "last_signal": decision.signal,
                "last_action": decision.reason,
            })
            if decision.signal == "HOLD" or self.mode == "MONITOR":
                state.append_audit("info", "Continuous Runtime", f"{decision.strategy_id} {decision.signal}: {decision.reason}")
                results.append({"strategyId": decision.strategy_id, "signal": decision.signal, "action": "MONITOR", "reason": decision.reason})
                continue
            spec = next(
                (
                    item
                    for item in self.supervisor.engine.specs  # type: ignore[union-attr]
                    if item.strategy_instance_id == decision.strategy_instance_id
                ),
                None,
            )
            context_blocker = self._decision_context_blocker(decision, spec)
            if context_blocker:
                state.append_audit(
                    "danger",
                    "Continuous Runtime",
                    context_blocker,
                )
                results.append(
                    {
                        "strategyId": str(decision.strategy_id or ""),
                        "signal": decision.signal,
                        "action": "BLOCKED",
                        "reason": context_blocker,
                        "ok": False,
                    }
                )
                continue
            position_direction = self._position_direction(spec)
            current_position_quantity = state.broker_position_quantity(
                spec.symbol,
                spec.broker_id,
                "SHORT" if position_direction == "short" else "LONG",
            )
            is_short_entry = (
                position_direction == "short"
                and decision.signal == "SELL"
                and current_position_quantity >= 0
            )
            is_short_cover = (
                position_direction == "short"
                and decision.signal == "BUY"
                and current_position_quantity < 0
            )
            is_long_exit = (
                position_direction == "long"
                and decision.signal == "SELL"
                and current_position_quantity > 0
            )
            quantity = (
                abs(current_position_quantity)
                if is_short_cover or is_long_exit
                else self._order_quantity(spec, decision.bar.close)
            )
            market_type = self._market_type(spec)
            futures_policy = self._futures_execution_policy(spec)
            functional_test = (
                self.execution_purpose
                == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
            )
            functional_permit_binding: dict[str, Any] = {}
            if functional_test:
                try:
                    functional_permit_binding = (
                        state.functional_test_active_permit_binding()
                    )
                except Exception:
                    # The actual order edge reports the strict parser error;
                    # this cycle must not infer a portfolio-only scope when
                    # the active pointer is missing or malformed.
                    functional_permit_binding = {}
            functional_portfolio_only = bool(
                functional_permit_binding.get("portfolioRequired") is True
                and not any(
                    str(functional_permit_binding.get(key) or "").strip()
                    for key in (
                        "strategyArtifactId",
                        "strategyArtifactHash",
                        "strategyInstanceId",
                    )
                )
            )
            standalone = bool(
                isinstance(spec.artifact, dict)
                and spec.artifact.get("standalone") is True
            )
            strategy_reference = (
                spec.artifact.get("artifact_reference")
                if isinstance(spec.artifact, dict)
                and isinstance(spec.artifact.get("artifact_reference"), dict)
                else {}
            )
            functional_metadata: dict[str, Any] = {}
            if functional_test:
                functional_metadata = {
                    **self.functional_test_context,
                    "execution_purpose": state.FUNCTIONAL_TEST_EXECUTION_PURPOSE,
                    "functional_test_environment": state.FUNCTIONAL_TEST_ENVIRONMENT,
                    "environment": state.FUNCTIONAL_TEST_ENVIRONMENT,
                    "functional_test_portfolio_only": functional_portfolio_only,
                    "functional_test_strategy_artifact_id": (
                        ""
                        if functional_portfolio_only
                        else str(
                            strategy_reference.get("artifactId")
                            or spec.strategy_id
                        )
                    ),
                    "functional_test_strategy_artifact_hash": (
                        ""
                        if functional_portfolio_only
                        else str(
                            strategy_reference.get("artifactHash")
                            or spec.artifact_hash
                        ).lower()
                    ),
                    "functional_test_strategy_instance_id": (
                        ""
                        if functional_portfolio_only
                        else spec.strategy_instance_id
                    ),
                    "portfolio_artifact_id": "" if standalone else spec.portfolio_id,
                    "portfolio_artifact_hash": "" if standalone else spec.portfolio_hash,
                    "portfolio_instance_id": "" if standalone else self.portfolio_instance_id,
                    "account_id": state.kis_functional_test_account_id(),
                    "promotion_eligible": False,
                    "use_as_promotion_evidence": False,
                    "full_live_requested": False,
                }
            intent = OrderIntent(
                strategy_id=decision.strategy_id,
                asset=state.asset_from_symbol(spec.symbol),
                symbol=spec.symbol,
                side=decision.signal,
                quantity=quantity,
                reference_price=decision.bar.close,
                mode=self.mode,  # type: ignore[arg-type]
                reason=decision.reason,
                metadata={
                    **functional_metadata,
                    "broker_id": spec.broker_id,
                    "portfolio_id": "" if spec.artifact.get("standalone") else spec.portfolio_id,
                    "portfolio_hash": spec.portfolio_hash,
                    "deployment_id": self.deployment_id,
                    "runtime_strategy_ids": list(self.strategy_ids),
                    "runtime_allowed_symbols": list(self.allowed_symbols),
                    "strategy_instance_id": spec.strategy_instance_id,
                    "instrument_id": spec.instrument_id,
                    "target_revision": max(1, int(datetime.fromisoformat(decision.bar.end_time.replace("Z", "+00:00")).timestamp())),
                    "order_purpose": "SIGNAL",
                    "current_weight": (
                        (-spec.target_weight if current_position_quantity < 0 else spec.target_weight)
                        if abs(current_position_quantity) > 1e-12
                        else 0.0
                    ),
                    "portfolio_equity": max(1.0, float(state.STATE["risk_settings"]["strategy_capital_limit_krw"])),
                    "expected_alpha_bps": self._numeric_parameter(spec, "expectedAlphaBps", 0.0),
                    "expected_cost_bps": self._numeric_parameter(spec, "expectedCostBps", 5.0),
                    "position_direction": position_direction,
                    "market_type": market_type,
                    "short_entries_requested": (
                        is_short_entry
                        and self._artifact_short_requested(spec)
                    ),
                    "broker_short_adapter_verified": (
                        spec.broker_id == "binance-futures"
                        and market_type in {
                            "future",
                            "futures",
                            "perpetual",
                        }
                    ),
                    "risk_reducing": is_short_cover or is_long_exit,
                    "max_leverage": futures_policy["max_leverage"],
                    "required_margin_type": futures_policy[
                        "required_margin_type"
                    ],
                    "futures_execution_policy": futures_policy["canonical"],
                    "futures_policy_valid": futures_policy["valid"],
                    "futures_policy_blockers": futures_policy["blockers"],
                    "per_trade_risk_pct": futures_policy[
                        "per_trade_risk_pct"
                    ],
                    "max_notional_pct": futures_policy[
                        "max_notional_pct"
                    ],
                    "runtime_evaluation_key": decision.evaluation_key,
                    "confirmed_bar_end": decision.bar.end_time,
                    "order_type": self._order_type_for_broker(
                        spec.broker_id,
                        decision.signal,
                        spec.symbol,
                        functional_test=functional_test,
                    ),
                    "execution_timing": "next-open-boundary",
                    "decision_price_role": "reference-and-sizing-only",
                },
            )
            checks = state.snapshot()
            result = state.submit_order_intent(checks, intent, dry_run=bool(state.STATE["dry_run"]), audit_event="Continuous Runtime")
            results.append({"strategyId": decision.strategy_id, "signal": decision.signal, "action": result.get("reason"), "ok": result.get("ok")})
        return {"mode": self.mode, "profileId": self.profile_id, "results": results}

    def _portfolio_functional_metadata(self, spec: Any) -> dict[str, Any]:
        from . import state

        if self.execution_purpose != state.FUNCTIONAL_TEST_EXECUTION_PURPOSE:
            return {}
        try:
            permit_binding = state.functional_test_active_permit_binding()
        except Exception:
            permit_binding = {}
        portfolio_only = bool(
            permit_binding.get("portfolioRequired") is True
            and not any(
                str(permit_binding.get(key) or "").strip()
                for key in (
                    "strategyArtifactId",
                    "strategyArtifactHash",
                    "strategyInstanceId",
                )
            )
        )
        reference = (
            spec.artifact.get("artifact_reference")
            if isinstance(spec.artifact, dict)
            and isinstance(spec.artifact.get("artifact_reference"), dict)
            else {}
        )
        return {
            **self.functional_test_context,
            "execution_purpose": state.FUNCTIONAL_TEST_EXECUTION_PURPOSE,
            "functional_test_environment": state.FUNCTIONAL_TEST_ENVIRONMENT,
            "environment": state.FUNCTIONAL_TEST_ENVIRONMENT,
            "functional_test_portfolio_only": portfolio_only,
            "functional_test_strategy_artifact_id": (
                ""
                if portfolio_only
                else str(reference.get("artifactId") or spec.strategy_id)
            ),
            "functional_test_strategy_artifact_hash": (
                ""
                if portfolio_only
                else str(
                    reference.get("artifactHash") or spec.artifact_hash
                ).lower()
            ),
            "functional_test_strategy_instance_id": (
                "" if portfolio_only else spec.strategy_instance_id
            ),
            "portfolio_artifact_id": spec.portfolio_id,
            "portfolio_artifact_hash": spec.portfolio_hash,
            "portfolio_instance_id": self.portfolio_instance_id,
            "account_id": state.kis_functional_test_account_id(),
            "promotion_eligible": False,
            "use_as_promotion_evidence": False,
            "full_live_requested": False,
        }

    def _portfolio_order_intent(
        self,
        plan: SymbolNetPlan,
        decision: Any,
        spec: Any,
        contributing_specs: tuple[Any, ...],
        current_quantity: Decimal,
    ) -> OrderIntent:
        from . import state

        functional_test = (
            self.execution_purpose == state.FUNCTIONAL_TEST_EXECUTION_PURPOSE
        )
        futures_policy = self._futures_execution_policy(spec)
        confirmed_bar_end = str(decision.bar.end_time)
        target_revision = max(
            1,
            int(
                datetime.fromisoformat(
                    confirmed_bar_end.replace("Z", "+00:00")
                ).timestamp()
            ),
        )
        instance_ids = tuple(
            sorted(
                {
                    item.sleeve_id
                    for item in plan.deltas
                    if item.signed_quantity != 0
                }
            )
        )
        contributing_ids = tuple(
            sorted(
                {
                    str(item.strategy_id or "")
                    for item in contributing_specs
                    if str(item.strategy_id or "")
                }
            )
        )
        sleeve_targets = {
            item.sleeve_id: float(item.target_quantity) for item in plan.deltas
        }
        return OrderIntent(
            strategy_id=str(decision.strategy_id),
            asset=state.asset_from_symbol(spec.symbol),
            symbol=spec.symbol,
            side=plan.side,
            quantity=float(plan.quantity),
            reference_price=float(decision.bar.close),
            mode=self.mode,  # type: ignore[arg-type]
            reason=(
                f"portfolio net {plan.side}: {len(plan.allocations)} sleeve allocations"
            ),
            metadata={
                **self._portfolio_functional_metadata(spec),
                "broker_id": "kis",
                "portfolio_id": spec.portfolio_id,
                "portfolio_hash": spec.portfolio_hash,
                "deployment_id": self.deployment_id,
                "runtime_strategy_ids": list(self.strategy_ids),
                "runtime_allowed_symbols": list(self.allowed_symbols),
                "strategy_instance_id": spec.strategy_instance_id,
                "strategy_instance_ids": list(instance_ids),
                "contributing_strategy_ids": list(contributing_ids),
                "sleeve_targets": sleeve_targets,
                "multi_strategy": True,
                "instrument_id": spec.instrument_id,
                "target_revision": target_revision,
                "order_purpose": "PORTFOLIO_NET",
                "current_weight": (
                    spec.target_weight if current_quantity > 0 else 0.0
                ),
                "portfolio_equity": max(
                    1.0,
                    float(
                        state.STATE["risk_settings"][
                            "strategy_capital_limit_krw"
                        ]
                    ),
                ),
                "expected_alpha_bps": self._numeric_parameter(
                    spec, "expectedAlphaBps", 0.0
                ),
                "expected_cost_bps": self._numeric_parameter(
                    spec, "expectedCostBps", 5.0
                ),
                "position_direction": "long",
                "market_type": "spot",
                "short_entries_requested": False,
                "broker_short_adapter_verified": False,
                "risk_reducing": plan.side == "SELL",
                "max_leverage": futures_policy["max_leverage"],
                "required_margin_type": futures_policy[
                    "required_margin_type"
                ],
                "futures_execution_policy": futures_policy["canonical"],
                "futures_policy_valid": futures_policy["valid"],
                "futures_policy_blockers": futures_policy["blockers"],
                "per_trade_risk_pct": futures_policy["per_trade_risk_pct"],
                "max_notional_pct": futures_policy["max_notional_pct"],
                "runtime_evaluation_key": plan.plan_id,
                "confirmed_bar_end": confirmed_bar_end,
                "order_type": self._order_type_for_broker(
                    "kis",
                    plan.side,
                    spec.symbol,
                    functional_test=functional_test,
                ),
                "execution_timing": "next-open-boundary",
                "decision_price_role": "reference-and-sizing-only",
                "portfolio_execution": plan.metadata(),
            },
        )

    @staticmethod
    def _portfolio_result(
        decision: Any,
        *,
        action: str,
        ok: bool,
        reason: str,
        plan_id: str = "",
        broker_order_id: str = "",
    ) -> dict[str, Any]:
        return {
            "strategyId": str(decision.strategy_id or ""),
            "strategyInstanceId": str(decision.strategy_instance_id or ""),
            "signal": str(decision.signal or ""),
            "action": action,
            "ok": ok,
            "reason": reason,
            "planId": plan_id,
            "brokerOrderId": broker_order_id,
        }

    def _handle_portfolio_cycle_locked(
        self, cycle: Any, specs: tuple[Any, ...]
    ) -> dict[str, Any]:
        from . import state

        decisions = tuple(cycle.decisions)
        results: list[dict[str, Any]] = []
        for decision in decisions:
            state.STATE["strategy_runner"].update({
                "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_profile": self.profile_id,
                "last_strategy": decision.strategy_id,
                "last_signal": decision.signal,
                "last_action": decision.reason,
            })
        if self.mode == "MONITOR":
            for decision in decisions:
                state.append_audit(
                    "info",
                    "Continuous Runtime",
                    f"{decision.strategy_id} {decision.signal}: {decision.reason}",
                )
                results.append(
                    self._portfolio_result(
                        decision,
                        action="MONITOR",
                        ok=True,
                        reason=str(decision.reason),
                    )
                )
            return {
                "mode": self.mode,
                "profileId": self.profile_id,
                "portfolioExecution": "MONITOR",
                "results": results,
            }

        if self.portfolio_ledger is None or not self.portfolio_execution_scope_id:
            reason = (
                "다중 Sleeve 실주문 원장이 계좌에 봉인되지 않아 주문을 차단했습니다."
            )
            state.append_audit("danger", "Continuous Runtime", reason)
            return {
                "mode": self.mode,
                "profileId": self.profile_id,
                "portfolioExecution": "BLOCKED",
                "results": [
                    self._portfolio_result(
                        decision, action="BLOCKED", ok=False, reason=reason
                    )
                    for decision in decisions
                ],
            }
        ledger = self.portfolio_ledger
        try:
            sync_report = self._sync_portfolio_execution(force=True)
            pending_orders = ledger.pending_orders(
                self.portfolio_execution_scope_id
            )
            reconciliation = ledger.reconcile_restart(
                scope_id=self.portfolio_execution_scope_id,
                broker_holdings=self._kis_broker_holdings(),
                managed_symbols=self.portfolio_execution_symbols,
                persist=False,
            )
        except (OSError, sqlite3.Error, TypeError, ValueError, RuntimeError) as exc:
            reason = (
                "다중 Sleeve 체결 원장을 동기화하지 못해 신규 주문을 차단했습니다: "
                f"{type(exc).__name__}: {str(exc)[:240]}"
            )
            state.STATE["new_entries_blocked"] = True
            state.append_audit("danger", "Continuous Runtime", reason)
            return {
                "mode": self.mode,
                "profileId": self.profile_id,
                "portfolioExecution": "BLOCKED",
                "results": [
                    self._portfolio_result(
                        decision, action="BLOCKED", ok=False, reason=reason
                    )
                    for decision in decisions
                ],
            }
        if pending_orders or not reconciliation.ready:
            reason = (
                "기존 KIS 순주문의 체결·취소 대조가 끝나지 않았습니다: "
                + ", ".join(pending_orders[:5])
                if pending_orders
                else (
                    "KIS 다중 Sleeve 전용 계좌에 Portfolio 밖 보유종목이 있어 "
                    "신규 주문을 차단했습니다."
                    if reconciliation.external_holdings
                    else "KIS 실제 보유량과 Sleeve 합계가 달라 신규 주문을 차단했습니다."
                )
            )
            state.append_audit("warn", "Continuous Runtime", reason)
            return {
                "mode": self.mode,
                "profileId": self.profile_id,
                "portfolioExecution": "WAIT_RECONCILIATION",
                "results": [
                    self._portfolio_result(
                        decision,
                        action=("MONITOR" if decision.signal == "HOLD" else "BLOCKED"),
                        ok=decision.signal == "HOLD",
                        reason=reason,
                    )
                    for decision in decisions
                ],
            }

        specs_by_instance = {
            str(spec.strategy_instance_id or ""): spec for spec in specs
        }
        decisions_by_symbol: dict[str, list[Any]] = {}
        instance_counts: dict[str, int] = {}
        for item in decisions:
            instance_id = str(item.strategy_instance_id or "")
            instance_counts[instance_id] = instance_counts.get(instance_id, 0) + 1
        for decision in decisions:
            instance_id = str(decision.strategy_instance_id or "")
            spec = specs_by_instance.get(instance_id)
            blocker = self._decision_context_blocker(decision, spec)
            if instance_counts.get(instance_id, 0) > 1:
                blocker = f"동일 cycle에 Strategy Instance {instance_id} 결정이 중복되었습니다."
            if blocker:
                state.append_audit("danger", "Continuous Runtime", blocker)
                results.append(
                    self._portfolio_result(
                        decision, action="BLOCKED", ok=False, reason=blocker
                    )
                )
                continue
            if decision.signal == "HOLD":
                results.append(
                    self._portfolio_result(
                        decision,
                        action="MONITOR",
                        ok=True,
                        reason=str(decision.reason),
                    )
                )
                continue
            symbol = canonical_kis_symbol(spec.symbol)
            decisions_by_symbol.setdefault(symbol, []).append(decision)

        holdings = self._kis_broker_holdings()
        sleeve_holdings = ledger.sleeve_holdings(
            self.portfolio_execution_scope_id
        )
        for symbol in sorted(decisions_by_symbol):
            active_decisions = decisions_by_symbol[symbol]
            active_instances = {
                str(item.strategy_instance_id or "") for item in active_decisions
            }
            symbol_specs = tuple(
                sorted(
                    (
                        spec
                        for spec in specs
                        if canonical_kis_symbol(spec.symbol) == symbol
                    ),
                    key=lambda item: str(item.strategy_instance_id or ""),
                )
            )
            decision_by_instance = {
                str(item.strategy_instance_id or ""): item
                for item in active_decisions
            }
            reference_prices = [float(item.bar.close) for item in active_decisions]
            if any(price <= 0 for price in reference_prices) or (
                max(reference_prices) - min(reference_prices)
                > max(reference_prices) * 1e-9
            ):
                reason = (
                    f"{symbol} Sleeve 결정 가격이 서로 달라 하나의 KIS 주문 가격으로 "
                    "봉인할 수 없습니다."
                )
                for decision in active_decisions:
                    results.append(
                        self._portfolio_result(
                            decision, action="BLOCKED", ok=False, reason=reason
                        )
                    )
                continue
            targets: list[SleeveTarget] = []
            current_positions: dict[tuple[str, str], Decimal] = {}
            for spec in symbol_specs:
                sleeve_id = str(spec.strategy_instance_id or "")
                current = sleeve_holdings.get(sleeve_id, {}).get(
                    symbol, Decimal("0")
                )
                current_positions[(sleeve_id, symbol)] = current
                decision = decision_by_instance.get(sleeve_id)
                if decision is None:
                    target = current
                    intent_id = f"carry:{sleeve_id}:{symbol}"
                elif decision.signal == "BUY":
                    quantity = Decimal(
                        str(self._order_quantity(spec, float(decision.bar.close)))
                    )
                    if quantity != quantity.to_integral_value() or quantity <= 0:
                        reason = (
                            f"{sleeve_id}/{symbol} KIS 주문 수량은 1주 이상의 "
                            "정수여야 합니다."
                        )
                        results.append(
                            self._portfolio_result(
                                decision,
                                action="BLOCKED",
                                ok=False,
                                reason=reason,
                            )
                        )
                        target = current
                        active_instances.discard(sleeve_id)
                    else:
                        target = current + quantity
                    intent_id = str(
                        decision.evaluation_key
                        or f"{sleeve_id}:{decision.bar.end_time}:BUY"
                    )
                else:
                    target = Decimal("0")
                    intent_id = str(
                        decision.evaluation_key
                        or f"{sleeve_id}:{decision.bar.end_time}:SELL"
                    )
                targets.append(
                    SleeveTarget(
                        intent_id=intent_id,
                        sleeve_id=sleeve_id,
                        symbol=symbol,
                        target_quantity=target,
                    )
                )
            active_decisions = [
                item
                for item in active_decisions
                if str(item.strategy_instance_id or "") in active_instances
            ]
            if not active_decisions:
                continue
            try:
                plan = build_symbol_net_plan(
                    scope_id=self.portfolio_execution_scope_id,
                    portfolio_id=str(symbol_specs[0].portfolio_id or ""),
                    portfolio_hash=str(symbol_specs[0].portfolio_hash or ""),
                    targets=targets,
                    current_positions=current_positions,
                    broker_quantity=holdings.get(symbol, Decimal("0")),
                    reference_price=reference_prices[0],
                )
            except (TypeError, ValueError) as exc:
                reason = f"{symbol} Sleeve 순주문 계획을 만들지 못했습니다: {exc}"
                for decision in active_decisions:
                    results.append(
                        self._portfolio_result(
                            decision, action="BLOCKED", ok=False, reason=reason
                        )
                    )
                continue
            if not plan.allocations and not plan.internal_allocations:
                for decision in active_decisions:
                    results.append(
                        self._portfolio_result(
                            decision,
                            action="NO_CHANGE",
                            ok=True,
                            reason="Sleeve 목표와 현재 보유량이 같습니다.",
                            plan_id=plan.plan_id,
                        )
                    )
                continue
            lead_allocations = (
                plan.allocations
                if plan.allocations
                else plan.internal_allocations
            )
            lead_candidates = [
                allocation.sleeve_id
                for allocation in lead_allocations
                if (
                    allocation.signed_quantity > 0
                    if plan.side == "BUY"
                    else allocation.signed_quantity < 0
                )
                and allocation.sleeve_id in active_instances
            ]
            lead_instance = sorted(lead_candidates or active_instances)[0]
            lead_decision = decision_by_instance[lead_instance]
            lead_spec = specs_by_instance[lead_instance]
            if plan.internal_only:
                try:
                    ledger.record_internal_cross(
                        plan,
                        price=lead_decision.bar.close,
                        occurred_at=lead_decision.bar.end_time,
                    )
                except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                    reason = (
                        "Sleeve 내부 상계를 원장에 기록하지 못해 차단했습니다: "
                        f"{type(exc).__name__}: {str(exc)[:240]}"
                    )
                    state.STATE["new_entries_blocked"] = True
                    for decision in active_decisions:
                        results.append(
                            self._portfolio_result(
                                decision,
                                action="BLOCKED",
                                ok=False,
                                reason=reason,
                                plan_id=plan.plan_id,
                            )
                        )
                    continue
                for decision in active_decisions:
                    results.append(
                        self._portfolio_result(
                            decision,
                            action="INTERNAL_CROSS",
                            ok=True,
                            reason="동일 종목 Sleeve 간 수량을 내부 상계했습니다.",
                            plan_id=plan.plan_id,
                        )
                    )
                continue
            contributing_specs = tuple(
                specs_by_instance[item.sleeve_id]
                for item in plan.deltas
                if item.signed_quantity != 0
            )
            intent = self._portfolio_order_intent(
                plan,
                lead_decision,
                lead_spec,
                contributing_specs,
                sum(current_positions.values(), Decimal("0")),
            )
            checks = state.snapshot()
            result = state.submit_order_intent(
                checks,
                intent,
                dry_run=bool(state.STATE["dry_run"]),
                audit_event="Continuous Runtime Portfolio Net",
            )
            order = result.get("order") if isinstance(result.get("order"), dict) else {}
            broker_order_id = str(order.get("broker_order_id") or "").strip()
            accepted = (
                result.get("ok") is True
                and not bool(state.STATE["dry_run"])
                and broker_order_id not in {"", "-"}
            )
            if accepted:
                try:
                    ledger.record_accepted_order(
                        plan,
                        broker_order_id=broker_order_id,
                        local_order_id=str(order.get("order_id") or ""),
                        # State display timestamps are historically local and
                        # timezone-naive.  The sleeve hash chain stamps this
                        # ACK checkpoint with its own canonical UTC clock.
                        occurred_at="",
                    )
                except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
                    state.STATE["new_entries_blocked"] = True
                    result = {
                        **result,
                        "ok": False,
                        "reason": (
                            "broker ACK 이후 Sleeve 원장 checkpoint에 실패했습니다; "
                            "재시작 대조 전 신규 주문을 차단합니다: "
                            f"{type(exc).__name__}: {str(exc)[:200]}"
                        ),
                    }
                    state.append_audit(
                        "danger",
                        "Continuous Runtime Portfolio Net",
                        str(result["reason"]),
                    )
            for decision in active_decisions:
                results.append(
                    self._portfolio_result(
                        decision,
                        action=str(result.get("reason") or "ORDER"),
                        ok=result.get("ok") is True,
                        reason=str(result.get("reason") or ""),
                        plan_id=plan.plan_id,
                        broker_order_id=broker_order_id,
                    )
                )
        state.STATE["strategy_runner"].update({
            "last_strategy": f"{len(specs)} sleeves",
            "last_signal": "NET",
            "last_action": (
                f"{len(decisions_by_symbol)} symbols · "
                f"{sum(1 for item in results if item.get('brokerOrderId'))} broker orders"
            ),
        })
        return {
            "mode": self.mode,
            "profileId": self.profile_id,
            "portfolioExecution": {
                "schemaVersion": LIVE_PORTFOLIO_PLAN_SCHEMA,
                "scopeId": self.portfolio_execution_scope_id,
                "syncedFills": sync_report.applied_fills,
                "syncedStatuses": sync_report.applied_statuses,
            },
            "results": results,
        }

    def _decision_context_blocker(self, decision: Any, spec: Any | None) -> str:
        if spec is None:
            return (
                "평가 결과의 Strategy Instance가 현재 runtime 구성에 없어 주문 생성을 차단했습니다: "
                f"{getattr(decision, 'strategy_instance_id', '') or '-'}"
            )
        decision_strategy = str(getattr(decision, "strategy_id", "") or "").strip()
        spec_strategy = str(spec.strategy_id or "").strip()
        if decision_strategy != spec_strategy:
            return (
                "평가 결과 Strategy와 runtime spec이 일치하지 않아 주문 생성을 차단했습니다: "
                f"{decision_strategy or '-'} != {spec_strategy or '-'}"
            )
        symbol = str(spec.symbol or "").strip().upper()
        if self.strategy_ids and spec_strategy not in self.strategy_ids:
            return f"Strategy {spec_strategy}가 승인된 runtime 전략 목록에 없습니다."
        if self.allowed_symbols and symbol not in self.allowed_symbols:
            return f"Symbol {symbol}이 승인된 runtime 상품 목록에 없습니다."
        if self.portfolio_id:
            if spec.artifact.get("standalone") is True or str(spec.portfolio_id or "") != self.portfolio_id:
                return (
                    "평가 결과의 Portfolio가 승인된 runtime Portfolio와 일치하지 않아 주문 생성을 차단했습니다."
                )
        elif self.requested_strategy_id and spec_strategy != self.requested_strategy_id:
            return (
                f"Standalone runtime은 요청 Strategy {self.requested_strategy_id} 외 주문을 생성할 수 없습니다."
            )
        return ""

    @staticmethod
    def _custom_definition(spec: Any) -> dict[str, Any]:
        artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
        settings = artifact.get("settings") if isinstance(artifact.get("settings"), dict) else {}
        for candidate in (
            spec.parameters.get("customStrategyDefinition"),
            spec.parameters.get("custom_strategy_definition"),
            artifact.get("customStrategyDefinition"),
            artifact.get("custom_strategy_definition"),
            settings.get("customStrategyDefinition"),
        ):
            if isinstance(candidate, dict):
                return candidate
        return {}

    @classmethod
    def _position_direction(cls, spec: Any) -> str:
        artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
        definition = cls._custom_definition(spec)
        value = (
            definition.get("positionDirection")
            or artifact.get("position_direction")
            or artifact.get("positionDirection")
            or "long"
        )
        return "short" if str(value).strip().lower() == "short" else "long"

    @staticmethod
    def _market_type(spec: Any) -> str:
        artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
        return str(
            artifact.get("market_type")
            or artifact.get("marketType")
            or "spot"
        ).strip().lower()

    @classmethod
    def _artifact_short_requested(cls, spec: Any) -> bool:
        artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
        execution_policy = (
            artifact.get("executionPolicy")
            if isinstance(artifact.get("executionPolicy"), dict)
            else {}
        )
        return bool(
            cls._position_direction(spec) == "short"
            or artifact.get("allow_short_requested") is True
            or artifact.get("allowShort") is True
            or execution_policy.get("allowShort") is True
        )

    @classmethod
    def _futures_execution_policy(
        cls,
        spec: Any,
    ) -> dict[str, object]:
        artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
        policy = futures_execution_policy_from_artifact(artifact)
        return {
            "schema_version": policy.get("schemaVersion"),
            "valid": policy.get("valid") is True,
            "blockers": list(policy.get("blockers") or []),
            "max_leverage": float(policy.get("maxLeverageMultiplier") or 1.0),
            "required_margin_type": str(policy.get("marginMode") or "ISOLATED"),
            "per_trade_risk_pct": float(policy.get("perTradeRiskPercent") or 0.5),
            "max_notional_pct": float(policy.get("maxNotionalPercent") or 10.0),
            "canonical": policy,
        }

    @staticmethod
    def _order_quantity(spec: Any, price: float) -> float:
        override = spec.parameters.get("liveOrderQuantity")
        if override not in (None, ""):
            try:
                return max(0.0, float(override))
            except (TypeError, ValueError):
                pass
        if spec.broker_id == "binance-futures":
            artifact = spec.artifact if isinstance(spec.artifact, dict) else {}
            execution_sizing = (
                artifact.get("executionSizing")
                if isinstance(artifact.get("executionSizing"), dict)
                else artifact.get("execution_sizing")
                if isinstance(artifact.get("execution_sizing"), dict)
                else {}
            )
            raw_fixed_quantity = (
                execution_sizing.get("paperOrderQuantity")
                or spec.parameters.get("paperOrderQuantity")
            )
            try:
                fixed_quantity = max(0.0, float(raw_fixed_quantity or 0.0))
            except (TypeError, ValueError):
                fixed_quantity = 0.0
            try:
                notional_cap = max(
                    0.0,
                    float(spec.parameters.get("liveOrderNotionalUsdt", 100.0)),
                )
            except (TypeError, ValueError):
                notional_cap = 100.0
            capped_quantity = notional_cap / max(price, 1e-12)
            return min(fixed_quantity, capped_quantity) if fixed_quantity > 0 else capped_quantity
        if spec.broker_id in {"binance", "binance-futures"}:
            try:
                notional = float(spec.parameters.get("liveOrderNotionalUsdt", 5.5))
            except (TypeError, ValueError):
                notional = 5.5
            return max(0.0, notional / max(price, 1e-12))
        if spec.broker_id == "upbit":
            return max(0.0, 5000.0 / max(price, 1e-12))
        return 1.0

    @staticmethod
    def _numeric_parameter(spec: Any, key: str, default: float) -> float:
        try:
            return float(spec.parameters.get(key, default))
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _order_type_for_broker(
        broker_id: str,
        side: str = "BUY",
        symbol: str = "",
        *,
        functional_test: bool = False,
    ) -> str:
        normalized = str(broker_id or "").strip().lower()
        normalized_side = str(side or "BUY").strip().upper()
        if normalized in {"binance", "binance-futures"}:
            return "MARKET"
        if normalized == "kis":
            text = str(symbol or "").strip().upper()
            local_code = text.removesuffix(".KS").removesuffix(".KQ")
            if functional_test:
                # SMALL_LIVE's safety profile forbids market orders. The
                # confirmed bar price remains a priced KIS limit order and is
                # rechecked against the exact permit immediately before POST.
                return "00"
            # Domestic cash equities support ordinary market orders as
            # ORD_DVSN=01 / ORD_UNPR=0.  Overseas KIS remains a priced limit
            # route and is separately blocked unless a fresh quote lifecycle
            # is attested.
            return "01" if local_code.isdigit() and len(local_code) == 6 else "00"
        if normalized == "upbit":
            # Upbit native market buy spends quote notional (`price`), while
            # native market sell submits base quantity (`market`).
            return "price" if normalized_side == "BUY" else "market"
        return "LIMIT"


class LiveContinuousRuntimeManager:
    """Runs stock and crypto portfolio loops independently and concurrently."""

    def __init__(self, root: Path) -> None:
        self._lock = threading.RLock()
        self.controllers = {
            "stock": LiveContinuousController(root),
            "crypto": LiveContinuousController(root),
        }

    def start(
        self,
        profile_id: str,
        mode: str,
        portfolio_id: str = "",
        strategy_id: str = "",
        deployment_id: str = "",
        execution_purpose: str = "",
        functional_test_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized = "stock" if profile_id == "stock" else "crypto"
        with self._lock:
            return self.controllers[normalized].start(
                normalized,
                mode,
                portfolio_id,
                strategy_id,
                deployment_id,
                execution_purpose,
                functional_test_context,
            )

    def validate_portfolio_execution_dispatch(
        self, intent: OrderIntent
    ) -> tuple[bool, str, dict[str, Any]]:
        metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
        payload = metadata.get("portfolio_execution")
        scope_id = (
            str(payload.get("scopeId") or "").strip()
            if isinstance(payload, dict)
            else ""
        )
        if not scope_id:
            reason = "portfolio-pre-post-validation-blocked:scope-missing"
            return False, reason, {
                "schemaVersion": "live-portfolio-pre-post-validation-v1",
                "allowed": False,
                "reason": reason,
            }
        matches = [
            controller
            for controller in self.controllers.values()
            if controller.portfolio_execution_scope_id == scope_id
            and controller.portfolio_ledger is not None
        ]
        if len(matches) != 1:
            reason = (
                "portfolio-pre-post-validation-blocked:"
                f"runtime-scope-match-count:{len(matches)}"
            )
            return False, reason, {
                "schemaVersion": "live-portfolio-pre-post-validation-v1",
                "scopeId": scope_id,
                "allowed": False,
                "reason": reason,
            }
        return matches[0].validate_portfolio_execution_dispatch(intent)

    def stop(self, profile_id: str = "") -> dict[str, Any]:
        with self._lock:
            if profile_id in self.controllers:
                return self.controllers[profile_id].stop()
            results = {
                key: controller.stop()
                for key, controller in self.controllers.items()
            }
            failed_profiles = [
                key
                for key, result in results.items()
                if result.get("ok") is not True
            ]
            return {
                "ok": not failed_profiles,
                "reason": (
                    "all continuous runtimes stopped"
                    if not failed_profiles
                    else "continuous runtime stop failed: "
                    + ", ".join(failed_profiles)
                ),
                "results": results,
                "runtime": self.snapshot(),
            }

    def transition_running(self, mode: str) -> dict[str, Any]:
        """Transition all running profiles together, rolling back on failure."""

        with self._lock:
            running = [
                (profile_id, controller)
                for profile_id, controller in self.controllers.items()
                if controller.supervisor is not None
                and controller.supervisor.running
            ]
            acquired: list[threading.RLock] = []
            previous_modes = {
                profile_id: controller.mode
                for profile_id, controller in running
            }
            try:
                for _profile_id, controller in running:
                    controller._lock.acquire()  # noqa: SLF001 - coordinated owner
                    acquired.append(controller._lock)  # noqa: SLF001
                results: dict[str, dict[str, Any]] = {}
                transitioned: list[tuple[str, LiveContinuousController]] = []
                for profile_id, controller in running:
                    result = controller.start(profile_id, mode)
                    results[profile_id] = result
                    if not result.get("ok"):
                        for rollback_id, rollback_controller in reversed(
                            transitioned
                        ):
                            rollback_controller.start(
                                rollback_id,
                                previous_modes[rollback_id],
                            )
                        return {
                            "ok": False,
                            "reason": str(
                                result.get("reason")
                                or f"{profile_id} runtime mode transition failed"
                            ),
                            "results": results,
                            "runtime": self.snapshot(),
                        }
                    transitioned.append((profile_id, controller))
                return {
                    "ok": True,
                    "reason": "running continuous runtimes transitioned",
                    "results": results,
                    "runtime": self.snapshot(),
                }
            finally:
                for lock in reversed(acquired):
                    lock.release()

    def snapshot(self) -> dict[str, Any]:
        profiles = {key: controller.snapshot() for key, controller in self.controllers.items()}
        running_profiles = [key for key, value in profiles.items() if value.get("running")]
        return {
            "schemaVersion": "multi-profile-continuous-runtime-v1",
            "running": bool(running_profiles),
            "runningProfiles": running_profiles,
            "profiles": profiles,
        }


def validate_portfolio_execution_dispatch(
    intent: OrderIntent,
) -> tuple[bool, str, dict[str, Any]]:
    """Validate a multi-sleeve KIS plan at the final broker POST edge.

    The state layer imports this function lazily so the callback can reuse the
    active runtime/ledger without introducing an import cycle during startup.
    """

    from . import state

    return state.LIVE_CONTINUOUS_CONTROLLER.validate_portfolio_execution_dispatch(
        intent
    )
