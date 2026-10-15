r"""Read-only prelaunch inventory. Never grants permission or contacts a broker.

Run this file directly, not ``python -m live_trader`` (the app initializer may
migrate settings). Report output is stdout; callers may explicitly save it.

From the Live Trader folder, explicitly select the canonical store and the
packaged runtime (use --runtime-root . for the separate source-mode settings):

  .\.venv\Scripts\python.exe -B tools\prelaunch_readiness.py --artifact-root
  C:\Users\youwo\AppData\Local\trading-system\artifacts\strategy-core
  --runtime-root C:\Users\youwo\AppData\Local\live_trader

Pass the command on one line. Exit 0 means the local inventory was readable,
not that trading is ready. Every report remains NOT_READY because this command
does not perform a fresh broker preflight or the operator's manual launch.
"""
from __future__ import annotations

import argparse
import importlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import types
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

APP_ROOT = Path(__file__).resolve().parents[1]
_PACKAGE = "_live_prelaunch_read_only"


def dependencies(app_root=APP_ROOT):
    """Bypass the application initializer and its dotenv/secret migration."""
    runtime = app_root.parents[1] / "packages" / "trading_runtime"
    if str(runtime) not in sys.path:
        sys.path.insert(0, str(runtime))
    if _PACKAGE not in sys.modules:
        package = types.ModuleType(_PACKAGE)
        package.__path__ = [str(app_root / "live_trader")]
        sys.modules[_PACKAGE] = package
    inbox = importlib.import_module(_PACKAGE + ".paper_candidate_inbox")
    profiles = importlib.import_module(_PACKAGE + ".account_profiles")
    secrets = importlib.import_module("trading_runtime.secret_store")
    return inbox, profiles, secrets


def read_settings(env_path, *, environ=None, secret_store=None):
    """Same precedence as the app, with no migration, env mutation, or writes."""
    env = dict(os.environ if environ is None else environ)
    status = "READ"
    try:
        lines = Path(env_path).read_text(encoding="utf-8-sig").splitlines()
    except FileNotFoundError:
        lines, status = [], "MISSING"
    except (OSError, UnicodeError):
        lines, status = [], "UNREADABLE"
    for line in lines:
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        key = key.removeprefix("export ").strip()
        # Match dotenv's unquoted inline comments without evaluating any text.
        quote, escaped, end = None, False, len(value)
        for index, char in enumerate(value):
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char in {"'", '"'}:
                quote = None if quote == char else char if quote is None else quote
            elif char == "#" and quote is None:
                end = index
                break
        value = value[:end].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key and value and not env.get(key):
            env[key] = value
    if secret_store is not None:
        for key in ("KIS_APP_KEY", "KIS_APP_SECRET", "KIS_ACCOUNT_NO", "KIS_HTS_ID",
                    "BINANCE_API_KEY", "BINANCE_API_SECRET", "UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY"):
            if not env.get(key):
                env[key] = secret_store.get("live_trader." + key)
    return env, status


def _capital_status(path, deployment, account, digest):
    """Read metadata using SQLite mode=ro. No schema creation or cash export."""
    if not path.is_file():
        return "NOT_INITIALIZED"
    if path.is_symlink():
        return "UNREADABLE_OR_INVALID"
    wal = path.with_name(path.name + "-wal")
    try:
        # Immutable SQLite never creates a journal/SHM. A live WAL could contain
        # newer committed data, so refusing it is essential before ignoring it.
        if wal.exists() and wal.stat().st_size:
            return "UNVERIFIED_ACTIVE_WAL"
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            row = db.execute("SELECT scope_json,principal,occurred_at,content_hash FROM principals WHERE deployment_id=?",
                             (deployment["deploymentId"],)).fetchone()
        if wal.exists() and wal.stat().st_size:
            return "UNVERIFIED_ACTIVE_WAL"
        if row is None:
            return "NOT_INITIALIZED"
        scope = json.loads(row[0])
        principal = Decimal(row[1])
        if (not account or not principal.is_finite() or principal <= 0
                or scope.get("schemaVersion") != "live-owned-capital-v1"
                or scope.get("scopeHash") != digest({k: v for k, v in scope.items() if k != "scopeHash"})
                or scope.get("deploymentId") != deployment["deploymentId"]
                or scope.get("accountProfileId") != account["profileId"]
                or scope.get("accountFingerprint") != account["accountFingerprint"]
                or scope.get("credentialConfigurationHash") != account["credentialConfigurationHash"]
                or scope.get("reviewBindingHash") != deployment.get("permissions", {}).get("liveReviewBindingHash")
                or digest({"scope": scope, "principal": row[1], "occurredAt": row[2]}) != row[3]):
            return "INVALID_OR_CHANGED_SCOPE"
        return "STORED_ACCOUNT_BINDING_VERIFIED_CURRENT_EQUITY_UNOBSERVED"
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, InvalidOperation):
        return "UNREADABLE_OR_INVALID"


def _limits(path):
    keys = ("daily_loss_limit_pct", "strategy_capital_limit_krw", "duplicate_order_cooldown_sec",
            "max_slippage_bps", "max_symbol_exposure_pct", "max_open_orders")
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        values = value["values"]
        if not isinstance(values, dict) or any(type(values.get(key)) not in (int, float)
                                               or not math.isfinite(values[key]) for key in keys):
            raise ValueError("invalid limits")
        return {"status": "SAVED_VALUES_READ_NOT_LIVE_VALIDATED", "values": {k: values[k] for k in keys}}
    except FileNotFoundError:
        return {"status": "NO_SAVED_OVERRIDE_RUNTIME_DEFAULTS_NOT_EVALUATED", "values": {}}
    except (OSError, ValueError, KeyError, TypeError):
        return {"status": "UNREADABLE_OR_INVALID", "values": {}}


def build_report(root, runtime_root, settings, *, env_status="NOT_LOADED", inbox=None, profiles=None):
    if inbox is None or profiles is None:
        inbox, profiles, _ = dependencies()
    root, runtime_root = Path(root).resolve(), Path(runtime_root).resolve()
    accounts = profiles.saved_account_profiles(settings)
    account_map = {a["brokerId"]: a for a in accounts}
    registry_path = root / "deployments" / "deployment-registry.json"
    errors, candidates, deployments = [], [], []
    before = None
    try:
        if not registry_path.is_file():
            errors.append("CANONICAL_REGISTRY_MISSING")
        before = inbox._registry(root)
        if any(item.get("deploymentId") != key or type(item.get("revision")) is not int
               or item["revision"] < 1 for key, item in before["entries"].items()):
            raise ValueError("invalid registry revision")
        catalogs = {kind: inbox._catalog(root, kind) for kind in ("strategy", "portfolio")}
        catalogs["instance"] = inbox._instance_catalog(root)
        for path in inbox._files(root / "evidence" / "paper"):
            try:
                row = inbox._candidate(root, inbox._read(path, root), before, catalogs)
                candidates.append({"evidenceId": row["evidenceId"], "strategyId": row["strategyId"],
                    "portfolioId": row["portfolioId"], "brokerId": row["brokerId"],
                    "status": "SEALED_CURRENT_SOURCE_VERIFIED", "registered": row["registered"],
                    "canReview": row["canReview"], "deploymentId": row["deployment"]["deploymentId"],
                    "validationProfile": row.get("validationSummary", {}).get("validationProfile", "STANDARD"),
                    "defaultForwardCriteriaMet": row.get("validationSummary", {}).get("defaultForwardCriteriaMet")})
            except (OSError, ValueError, UnicodeError, TypeError, KeyError, RuntimeError):
                candidates.append({"evidenceId": path.stem, "status": "BLOCKED_INVALID_OR_STALE_EVIDENCE"})
        for key, entry in before["entries"].items():
            if not (entry.get("environment") in {"LIVE", "SMALL_LIVE", "FULL_LIVE"}
                    or (entry.get("permissions") or {}).get("paper_candidate_imported") is True):
                continue
            permissions = entry.get("permissions") or {}
            try:
                from trading_runtime.artifact_governance import safe_file_token
                definition = inbox._read(root / "deployments" / "definitions" / (safe_file_token(key, "deployment") + ".json"), root)
                digest = inbox.stable_sha256({k: v for k, v in definition.items() if k != "definitionHash"})
                scope_fields = ("deploymentId", "accountId", "environment", "symbol", "instrumentId", "route", "strategyArtifact", "portfolioArtifact")
                valid = (definition.get("schemaVersion") == inbox.DEPLOYMENT_DEFINITION_SCHEMA_VERSION
                         and definition.get("definitionHash") == digest == entry.get("definitionHash")
                         and all(definition.get(k) == entry.get(k) for k in scope_fields))
                for kind, field in (("strategy", "strategyArtifact"), ("portfolio", "portfolioArtifact")):
                    if entry.get(field):
                        ref = entry[field]
                        item = inbox._exact_artifact(catalogs[kind], ref["artifactId"], ref["artifactHash"])
                        valid = valid and inbox.artifact_reference(item) == ref
            except (OSError, ValueError, TypeError, KeyError):
                valid = False
            candidate = next((c for c in candidates if c.get("deploymentId") == key), {})
            broker = candidate.get("brokerId") or (entry.get("route") if entry.get("route") in account_map else "unknown")
            account = account_map.get(broker)
            reviewed = bool(permissions.get("liveReviewBindingHash") and account
                and entry.get("accountId") == account["accountBindingId"]
                and permissions.get("liveAccountProfileId") == account["profileId"]
                and permissions.get("liveAccountFingerprint") == account["accountFingerprint"]
                and permissions.get("liveAccountCredentialConfigurationHash") == account["credentialConfigurationHash"])
            policy = permissions.get("liveCapitalPolicy") or {}
            percent = policy.get("percent")
            capital_policy_present = (policy.get("basis") == "ACCOUNT_AVAILABLE_CASH"
                and type(percent) in (int, float) and math.isfinite(percent) and 0 < percent <= 100)
            deployments.append({"deploymentId": key, "strategyId": (entry.get("strategyArtifact") or {}).get("artifactId"),
                "symbol": entry.get("symbol"), "brokerId": broker, "definitionAndArtifactValid": valid,
                "lifecycle": entry.get("lifecycle"), "mode": entry.get("mode"), "revision": entry.get("revision"),
                "accountReviewedAgainstSavedConfiguration": reviewed,
                "capitalPolicyPresent": capital_policy_present,
                "ownedCapital": _capital_status(runtime_root / "logs" / "native_owned_capital.sqlite3", entry, account, inbox.stable_sha256),
                "liveSmallEligible": permissions.get("live_small_eligible") is True,
                "liveAllowed": permissions.get("live_allowed") is True})
        if inbox._registry(root) != before:
            errors.append("REGISTRY_CHANGED_DURING_READ")
    except (OSError, ValueError, UnicodeError, TypeError, KeyError, RuntimeError):
        errors.append("CANONICAL_REGISTRY_OR_CATALOG_INVALID")
    if errors:
        # Never present an incomplete/racing read as verified current state.
        candidates, deployments = [], []
    markets = []
    for market, broker, capability in (
            ("BINANCE_SPOT", "binance", "NATIVE_SPOT_IMPLEMENTED_PER_ORDER_EVIDENCE_REQUIRED"),
            ("UPBIT_SPOT", "upbit", "NATIVE_SPOT_IMPLEMENTED_PER_ORDER_EVIDENCE_REQUIRED"),
            ("KIS_KR_STOCK_ETF", "kis", "NATIVE_EQUITY_REFERENCE_ONLY_PRODUCER_AND_OWNED_CAPITAL_UNWIRED"),
            ("KIS_US_STOCK_ETF", "kis", "NATIVE_EQUITY_US_CALCULATION_PRICE_ECHO_UNSUPPORTED")):
        items = [d for d in deployments if d["brokerId"] == broker
                 and (broker != "kis" or (str(d.get("symbol", "")).isdigit() == (market == "KIS_KR_STOCK_ETF")))]
        blockers = ["FRESH_BROKER_PREFLIGHT_NOT_OBSERVED", "FINAL_OPERATOR_START_NOT_PERFORMED"]
        if errors: blockers.append("CANONICAL_INVENTORY_UNVERIFIED")
        if broker not in account_map: blockers.append("SAVED_CREDENTIAL_PROFILE_MISSING_OR_UNREADABLE")
        if not items: blockers.append("NO_IMPORTED_CURRENT_DEPLOYMENT")
        if any(not d["definitionAndArtifactValid"] for d in items): blockers.append("DEPLOYMENT_OR_ARTIFACT_INVALID")
        if any(not d["accountReviewedAgainstSavedConfiguration"] for d in items): blockers.append("EXACT_ACCOUNT_REVIEW_REQUIRED")
        if any(not d["capitalPolicyPresent"] for d in items): blockers.append("DEPLOYMENT_CAPITAL_POLICY_MISSING")
        if any(d["ownedCapital"] == "NOT_INITIALIZED" for d in items): blockers.append("OWNED_CAPITAL_NOT_INITIALIZED")
        if any(d["ownedCapital"] not in {"NOT_INITIALIZED", "STORED_ACCOUNT_BINDING_VERIFIED_CURRENT_EQUITY_UNOBSERVED"} for d in items):
            blockers.append("OWNED_CAPITAL_METADATA_UNVERIFIED")
        if broker == "kis": blockers.append("NATIVE_EQUITY_PRODUCTION_CONTRACT_INCOMPLETE")
        markets.append({"market": market, "brokerId": broker, "status": "NOT_READY",
            "savedAccountProfilePresent": broker in account_map, "importedDeployments": len(items),
            "staticCodeCapability": capability, "currentBrokerEvidence": "NOT_QUERIED",
            "personalFeeObservation": "NOT_QUERIED", "blockers": blockers})
    return {"schemaVersion": "live-read-only-prelaunch-inventory-v1", "capturedAtUtc": datetime.now(timezone.utc).isoformat(),
        "status": "NOT_READY", "readOnly": True, "authorizationGranted": False, "liveReady": False,
        "canonicalArtifactRoot": str(root), "runtimeRoot": str(runtime_root), "envFileStatus": env_status,
        "registryHash": inbox.stable_sha256(before) if before is not None and not errors else None,
        "errors": errors, "candidates": candidates, "deployments": deployments, "markets": markets,
        "savedRiskLimits": _limits(Path(settings.get("LIVE_TRADER_RISK_SETTINGS") or runtime_root / "logs" / "risk-settings.json")),
        "sideEffects": {"brokerCalls": 0, "ordersSubmitted": 0, "armingCalls": 0, "runtimeStarts": 0,
                        "accountReviews": 0, "capitalAllocations": 0, "settingsWrites": 0},
        "limitations": ["Static local inventory only; never a fresh broker preflight or start authorization.",
                        "Stored settings presence does not establish broker authentication or trading permission.",
                        "Ordinary automation currently has one stock and one crypto manager; Binance and Upbit cannot run concurrently through that manager.",
                        "Capital initialization remains part of the existing reviewed manual start workflow."]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--runtime-root", type=Path)
    args = parser.parse_args(argv)
    inbox, profiles, secrets = dependencies()
    from trading_runtime.artifact_paths import shared_artifact_root
    runtime_root = args.runtime_root or Path(os.environ.get("LIVE_TRADER_DATA_DIR") or
                                             str(Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local") / "live_trader"))
    env_path = Path(os.environ.get("LIVE_TRADER_ENV_PATH") or runtime_root / ".env")
    preliminary, _ = read_settings(env_path)
    store_path = preliminary.get("LIVE_TRADER_SECRET_STORE_PATH") or preliminary.get("TRADING_SYSTEM_SECRET_STORE_PATH")
    store = secrets.SecretStore(Path(os.path.expandvars(store_path)).expanduser() if store_path else None)
    settings, status = read_settings(env_path, secret_store=store)
    configured_root = (settings.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR") or settings.get("TRADER_STRATEGY_ARTIFACT_DIR")
                       or settings.get("TRADING_SYSTEM_ARTIFACT_ROOT") or "").split(os.pathsep)[0].strip()
    root = args.artifact_root or (Path(os.path.expandvars(configured_root)).expanduser() if configured_root else shared_artifact_root())
    report = build_report(root, runtime_root, settings, env_status=status, inbox=inbox, profiles=profiles)
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
    return 0 if not report["errors"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
