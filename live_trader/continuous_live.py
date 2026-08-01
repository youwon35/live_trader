from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os
import threading
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

    def start(
        self,
        profile_id: str,
        mode: str,
        portfolio_id: str = "",
        strategy_id: str = "",
        deployment_id: str = "",
    ) -> dict[str, Any]:
        from . import state

        normalized_profile = "stock" if profile_id == "stock" else "crypto"
        normalized_mode = str(mode or "MONITOR").upper()
        requested_portfolio = str(portfolio_id or "").strip()
        requested_strategy = str(strategy_id or "").strip()
        requested_deployment = str(deployment_id or "").strip()
        if normalized_mode not in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"}:
            return {"ok": False, "reason": f"지원하지 않는 runtime mode입니다: {normalized_mode}", "snapshot": state.snapshot()}
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
                if normalized_mode != "MONITOR" and not all(
                    self._spec_mode_allowed(spec, normalized_mode) for spec in specs
                ):
                    return {
                        "ok": False,
                        "reason": f"실행 중인 Artifact가 {normalized_mode} 권한을 통과하지 못했습니다.",
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
                state.append_audit(
                    "info" if normalized_mode == "MONITOR" else "warn",
                    "Continuous Runtime",
                    f"{normalized_profile} runtime {previous_mode} → {normalized_mode} 무중단 전환",
                )
                return {"ok": True, "reason": "continuous runtime mode transitioned", "runtime": self.snapshot(), "snapshot": state.snapshot()}
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
                standalone = self._select_standalone_strategy(
                    normalized_profile,
                    normalized_mode,
                    requested_strategy,
                    requested_deployment,
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
            if normalized_mode != "MONITOR":
                allowed = runtime_permissions.get("live_eligible") is True or runtime_permissions.get("live_allowed") is True
                if normalized_mode == "SMALL_LIVE":
                    allowed = allowed or runtime_permissions.get("live_small_eligible") is True or runtime_permissions.get("live_small_allowed") is True
                if not allowed:
                    return {"ok": False, "reason": f"{runtime_id}는 {normalized_mode} 권한을 통과하지 못했습니다. MONITOR만 가능합니다.", "snapshot": state.snapshot()}
            engine = PortfolioRuntimeEngine(
                specs,
                mode="MONITOR",
                evaluator=BuiltinBarSignalEvaluator(
                    lambda spec: state.broker_position_quantity(
                        spec.symbol,
                        spec.broker_id,
                        (
                            "SHORT"
                            if self._position_direction(spec) == "short"
                            else "LONG"
                        ),
                    )
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
                restore_blocker = engine.transition_mode(
                    normalized_mode,  # type: ignore[arg-type]
                    restore_context=restore_context,
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
            feeds = feeds_for_specs(
                specs,
                prefer_kis=True,
                kis_demo=False,
                kis_app_key=os.getenv("KIS_APP_KEY", ""),
                kis_app_secret=os.getenv("KIS_APP_SECRET", ""),
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
        }

    def _running_context_blocker(
        self,
        *,
        portfolio_id: str,
        strategy_id: str,
        deployment_id: str,
    ) -> str:
        """Refuse a mode transition when it targets another deployment."""

        explicit = bool(portfolio_id or strategy_id or deployment_id)
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
        return (
            "실행 중인 runtime과 요청한 Deployment 컨텍스트가 일치하지 않아 전환을 차단했습니다: "
            + "; ".join(mismatches)
            if mismatches
            else ""
        )

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
        instance_id = str(strategy.get("instance_id") or f"standalone:{strategy_id}")
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
                    ),
                    "execution_timing": "next-open-boundary",
                    "decision_price_role": "reference-and-sizing-only",
                },
            )
            checks = state.snapshot()
            result = state.submit_order_intent(checks, intent, dry_run=bool(state.STATE["dry_run"]), audit_event="Continuous Runtime")
            results.append({"strategyId": decision.strategy_id, "signal": decision.signal, "action": result.get("reason"), "ok": result.get("ok")})
        return {"mode": self.mode, "profileId": self.profile_id, "results": results}

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
    ) -> str:
        normalized = str(broker_id or "").strip().lower()
        normalized_side = str(side or "BUY").strip().upper()
        if normalized in {"binance", "binance-futures"}:
            return "MARKET"
        if normalized == "kis":
            text = str(symbol or "").strip().upper()
            local_code = text.removesuffix(".KS").removesuffix(".KQ")
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
    ) -> dict[str, Any]:
        normalized = "stock" if profile_id == "stock" else "crypto"
        with self._lock:
            return self.controllers[normalized].start(
                normalized,
                mode,
                portfolio_id,
                strategy_id,
                deployment_id,
            )

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
