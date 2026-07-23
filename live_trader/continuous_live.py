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

    def start(self, profile_id: str, mode: str, portfolio_id: str = "") -> dict[str, Any]:
        from . import state

        normalized_profile = "stock" if profile_id == "stock" else "crypto"
        normalized_mode = str(mode or "MONITOR").upper()
        if normalized_mode not in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"}:
            return {"ok": False, "reason": f"지원하지 않는 runtime mode입니다: {normalized_mode}", "snapshot": state.snapshot()}
        with self._lock:
            if self.supervisor and self.supervisor.running:
                specs = tuple(self.supervisor.engine.specs)
                if normalized_mode != "MONITOR" and not all(
                    self._spec_mode_allowed(spec, normalized_mode) for spec in specs
                ):
                    return {
                        "ok": False,
                        "reason": f"실행 중인 Artifact가 {normalized_mode} 권한을 통과하지 못했습니다.",
                        "runtime": self.snapshot(),
                        "snapshot": state.snapshot(),
                    }
                previous_mode = self.mode
                self.mode = normalized_mode
                self.supervisor.engine.mode = normalized_mode
                state.append_audit(
                    "info" if normalized_mode == "MONITOR" else "warn",
                    "Continuous Runtime",
                    f"{normalized_profile} runtime {previous_mode} → {normalized_mode} 무중단 전환",
                )
                return {"ok": True, "reason": "continuous runtime mode transitioned", "runtime": self.snapshot(), "snapshot": state.snapshot()}
            portfolio = self._select_portfolio(normalized_profile, portfolio_id)
            loaded = None
            if portfolio is not None:
                source_path = str(portfolio.get("source_path") or "")
                if not source_path or not Path(source_path).exists():
                    return {"ok": False, "reason": "Portfolio Artifact 원본 경로가 없습니다.", "snapshot": state.snapshot()}
                loaded = load_portfolio_runtime_path(source_path)
                specs = tuple(spec for spec in loaded.specs if self._matches_profile(spec, normalized_profile))
                runtime_id = loaded.portfolio_id
                runtime_hash = loaded.portfolio_hash
                runtime_permissions = loaded.payload.get("permissions") if isinstance(loaded.payload.get("permissions"), dict) else {}
            else:
                standalone = self._select_standalone_strategy(normalized_profile, normalized_mode)
                if standalone is None:
                    return {"ok": False, "reason": f"{normalized_profile}용 Portfolio/Strategy Artifact를 찾을 수 없습니다.", "snapshot": state.snapshot()}
                source_path = str(standalone.get("source_path") or "")
                specs = (self._standalone_spec(standalone),)
                runtime_id = specs[0].portfolio_id
                runtime_hash = specs[0].portfolio_hash
                runtime_permissions = dict(standalone.get("permissions") or {})
            if not specs:
                return {"ok": False, "reason": f"선택 Artifact에 {normalized_profile} Strategy Instance가 없습니다.", "snapshot": state.snapshot()}
            if normalized_mode != "MONITOR":
                allowed = runtime_permissions.get("live_eligible") is True or runtime_permissions.get("live_allowed") is True
                if normalized_mode == "SMALL_LIVE":
                    allowed = allowed or runtime_permissions.get("live_small_eligible") is True or runtime_permissions.get("live_small_allowed") is True
                if not allowed:
                    return {"ok": False, "reason": f"{runtime_id}는 {normalized_mode} 권한을 통과하지 못했습니다. MONITOR만 가능합니다.", "snapshot": state.snapshot()}
            self.profile_id = normalized_profile
            self.mode = normalized_mode
            self.portfolio_path = source_path
            engine = PortfolioRuntimeEngine(
                specs,
                mode=normalized_mode,
                evaluator=BuiltinBarSignalEvaluator(lambda spec: state.broker_position_quantity(spec.symbol)),
                cycle_handler=self._handle_cycle,
                state_store=DurableRuntimeState(self.root / "logs" / f"continuous_{normalized_profile}_{runtime_hash[:16]}_engine.json"),
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
            )
            self.supervisor.start()
            source_kind = "Portfolio" if loaded is not None else "Standalone Strategy"
            state.append_audit("info", "Continuous Runtime", f"{runtime_id} · {source_kind} · {normalized_profile} · {normalized_mode} 시작, {len(specs)}개 전략")
            return {"ok": True, "reason": "continuous runtime started", "warmup": warmup, "runtime": self.snapshot(), "snapshot": state.snapshot()}

    def stop(self) -> dict[str, Any]:
        from . import state

        with self._lock:
            if self.supervisor is None:
                return {"ok": True, "reason": "continuous runtime already stopped", "runtime": self.snapshot(), "snapshot": state.snapshot()}
            result = self.supervisor.stop()
            state.append_audit("info", "Continuous Runtime", f"{self.profile_id or '-'} runtime 정지")
            return {"ok": True, "reason": "continuous runtime stopped", "runtime": result, "snapshot": state.snapshot()}

    def snapshot(self) -> dict[str, Any]:
        base = self.supervisor.snapshot() if self.supervisor is not None else {
            "schemaVersion": "continuous-runtime-supervisor-v1", "phase": "STOPPED", "running": False,
            "startedAt": "", "stoppedAt": "", "lastHeartbeat": "", "lastDataAt": "", "lastError": "",
            "reconnectCount": 0, "feedErrors": {}, "feeds": [], "engine": {},
        }
        return {**base, "profileId": self.profile_id, "mode": self.mode, "portfolioPath": self.portfolio_path}

    def _select_portfolio(self, profile_id: str, portfolio_id: str) -> dict[str, Any] | None:
        from . import state

        candidates: list[dict[str, Any]] = []
        for portfolio in state.portfolio_rows():
            if portfolio_id and str(portfolio.get("id") or "") != portfolio_id:
                continue
            instances = portfolio.get("strategy_instances") if isinstance(portfolio.get("strategy_instances"), list) else []
            symbols = [str(item.get("symbol") or item.get("qualifiedSymbol") or "") for item in instances if isinstance(item, dict)]
            if any(self._symbol_matches_profile(symbol, profile_id) for symbol in symbols):
                candidates.append(portfolio)
        # contracts.load_portfolio_artifacts() returns newest artifacts first.
        return candidates[0] if candidates else None

    def _select_standalone_strategy(self, profile_id: str, mode: str) -> dict[str, Any] | None:
        from . import state

        required = "live_eligible" if mode == "FULL_LIVE" else "live_small_eligible"
        for strategy in state.strategy_rows():
            if strategy.get(required) is not True:
                continue
            symbol = str(strategy.get("symbol") or "")
            if self._symbol_matches_profile(symbol, profile_id):
                return strategy
        return None

    @staticmethod
    def _standalone_spec(strategy: dict[str, Any]) -> RuntimeStrategySpec:
        symbol = str(strategy.get("symbol") or "").strip().upper()
        provider, broker_id, instrument_id = infer_market_route(symbol)
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

    def _handle_cycle(self, cycle: Any) -> dict[str, Any]:
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
            spec = next(item for item in self.supervisor.engine.specs if item.strategy_instance_id == decision.strategy_instance_id)  # type: ignore[union-attr]
            quantity = self._order_quantity(spec, decision.bar.close)
            intent = OrderIntent(
                strategy_id=decision.strategy_id,
                asset=state.asset_from_symbol(spec.symbol),
                symbol=spec.symbol,
                side=decision.signal,
                quantity=quantity,
                reference_price=decision.bar.close,
                mode=state.current_mode(),
                reason=decision.reason,
                metadata={
                    "broker_id": spec.broker_id,
                    "portfolio_id": "" if spec.artifact.get("standalone") else spec.portfolio_id,
                    "portfolio_hash": spec.portfolio_hash,
                    "strategy_instance_id": spec.strategy_instance_id,
                    "instrument_id": spec.instrument_id,
                    "target_revision": max(1, int(datetime.fromisoformat(decision.bar.end_time.replace("Z", "+00:00")).timestamp())),
                    "order_purpose": "SIGNAL",
                    "current_weight": spec.target_weight if state.broker_position_quantity(spec.symbol) > 0 else 0.0,
                    "portfolio_equity": max(1.0, float(state.STATE["risk_settings"]["strategy_capital_limit_krw"])),
                    "expected_alpha_bps": self._numeric_parameter(spec, "expectedAlphaBps", 0.0),
                    "expected_cost_bps": self._numeric_parameter(spec, "expectedCostBps", 5.0),
                    "runtime_evaluation_key": decision.evaluation_key,
                    "confirmed_bar_end": decision.bar.end_time,
                    "order_type": "LIMIT",
                },
            )
            checks = state.snapshot()
            result = state.submit_order_intent(checks, intent, dry_run=bool(state.STATE["dry_run"]), audit_event="Continuous Runtime")
            results.append({"strategyId": decision.strategy_id, "signal": decision.signal, "action": result.get("reason"), "ok": result.get("ok")})
        return {"mode": self.mode, "profileId": self.profile_id, "results": results}

    @staticmethod
    def _order_quantity(spec: Any, price: float) -> float:
        override = spec.parameters.get("liveOrderQuantity") or spec.parameters.get("paperOrderQuantity")
        if override not in (None, ""):
            try:
                return max(0.0, float(override))
            except (TypeError, ValueError):
                pass
        if spec.broker_id == "binance":
            return max(0.0, 5.0 / max(price, 1e-12))
        if spec.broker_id == "upbit":
            return max(0.0, 5000.0 / max(price, 1e-12))
        return 1.0

    @staticmethod
    def _numeric_parameter(spec: Any, key: str, default: float) -> float:
        try:
            return float(spec.parameters.get(key, default))
        except (TypeError, ValueError):
            return float(default)


class LiveContinuousRuntimeManager:
    """Runs stock and crypto portfolio loops independently and concurrently."""

    def __init__(self, root: Path) -> None:
        self.controllers = {
            "stock": LiveContinuousController(root),
            "crypto": LiveContinuousController(root),
        }

    def start(self, profile_id: str, mode: str, portfolio_id: str = "") -> dict[str, Any]:
        normalized = "stock" if profile_id == "stock" else "crypto"
        return self.controllers[normalized].start(normalized, mode, portfolio_id)

    def stop(self, profile_id: str = "") -> dict[str, Any]:
        if profile_id in self.controllers:
            return self.controllers[profile_id].stop()
        results = {key: controller.stop() for key, controller in self.controllers.items()}
        return {"ok": True, "reason": "all continuous runtimes stopped", "results": results, "runtime": self.snapshot()}

    def snapshot(self) -> dict[str, Any]:
        profiles = {key: controller.snapshot() for key, controller in self.controllers.items()}
        running_profiles = [key for key, value in profiles.items() if value.get("running")]
        return {
            "schemaVersion": "multi-profile-continuous-runtime-v1",
            "running": bool(running_profiles),
            "runningProfiles": running_profiles,
            "profiles": profiles,
        }
