from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


APP_ROOT = Path(__file__).resolve().parents[1]
TRADING_SYSTEM_ROOT = APP_ROOT.parents[1]
for candidate in (
    APP_ROOT,
    TRADING_SYSTEM_ROOT / "packages" / "trading_runtime",
):
    text = str(candidate)
    if text not in sys.path:
        sys.path.insert(0, text)

from live_trader.validation_small_live import (  # noqa: E402
    build_validation_plan,
    default_plan_path,
    load_and_validate_plan,
    research_short_bundle_snapshot,
    validate_monitor_only_plan,
    write_validation_plan,
)
from trading_runtime.artifact_paths import shared_artifact_root  # noqa: E402


def _csv(value: str) -> tuple[str, ...]:
    return tuple(
        item.strip()
        for item in str(value or "").split(",")
        if item.strip()
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backtest PASS 후보를 exact ID/hash 완전쌍으로 바인딩해 "
            "실제 주문 권한 없는 MONITOR/Dry-run 검증 plan으로 고정합니다. "
            "Portfolio 경로는 Strategy+Portfolio 두 완전쌍이 필요하고, "
            "--strategy-only는 Strategy 완전쌍만 허용합니다."
        )
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=shared_artifact_root(),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_plan_path(),
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="쉼표로 구분한 symbol 필터",
    )
    parser.add_argument(
        "--timeframes",
        default="",
        help="쉼표로 구분한 timeframe 필터",
    )
    parser.add_argument(
        "--max-per-bucket",
        type=int,
        default=1,
        help="broker/symbol/timeframe별 최대 후보 수",
    )
    parser.add_argument(
        "--strategy-id",
        default="",
        help="정확히 바인딩할 Strategy Artifact ID",
    )
    parser.add_argument(
        "--strategy-artifact-hash",
        default="",
        help="정확히 바인딩할 Strategy canonical artifact hash",
    )
    parser.add_argument(
        "--portfolio-id",
        default="",
        help="정확히 바인딩할 Portfolio Artifact ID",
    )
    parser.add_argument(
        "--portfolio-artifact-hash",
        default="",
        help="정확히 바인딩할 Portfolio canonical artifact hash",
    )
    parser.add_argument(
        "--strategy-only",
        action="store_true",
        help=(
            "Portfolio를 합성하거나 대체하지 않고 canonical Strategy "
            "Artifact만 MONITOR 후보로 고정합니다."
        ),
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="plan을 검사만 하고 파일을 쓰지 않습니다.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="기존 --output plan과 원본 artifact hash를 다시 검사합니다.",
    )
    return parser


def _summary(
    plan: dict,
    *,
    output: Path,
    written: bool,
    verification: dict,
) -> dict:
    return {
        "ok": verification.get("ok") is True,
        "mode": "MONITOR",
        "dryRunRequired": True,
        "brokerSubmitAllowed": False,
        "productionLifecycleMutation": False,
        "planId": plan.get("planId"),
        "candidateCount": plan.get("candidateCount"),
        "blockedCount": plan.get("blockedCount"),
        "coverage": plan.get("coverage"),
        "strategyOnly": bool(
            (plan.get("selectionPolicy") or {}).get("strategyOnly")
        ),
        "artifactBinding": plan.get("artifactBinding") or {},
        "candidates": [
            {
                "validationStrategyInstanceId": item.get(
                    "validationStrategyInstanceId"
                ),
                "strategyId": item.get("strategyId"),
                "strategyArtifactHash": item.get(
                    "strategyArtifactHash"
                ),
                "portfolioId": item.get("portfolioId"),
                "portfolioArtifactHash": item.get(
                    "portfolioArtifactHash"
                ),
                "symbol": item.get("symbol"),
                "timeframe": item.get("timeframe"),
            }
            for item in plan.get("candidates") or []
            if isinstance(item, dict)
        ],
        "output": str(output),
        "written": written,
        "verificationIssues": verification.get("issues") or [],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output = args.output.expanduser().resolve(strict=False)
    if args.verify:
        result = load_and_validate_plan(output)
        print(
            json.dumps(
                _summary(
                    result["plan"],
                    output=output,
                    written=False,
                    verification=result["verification"],
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if result["verification"]["ok"] else 3

    try:
        plan = build_validation_plan(
            args.artifact_root,
            symbols=_csv(args.symbols),
            timeframes=_csv(args.timeframes),
            max_per_bucket=args.max_per_bucket,
            strategy_id=args.strategy_id,
            strategy_artifact_hash=args.strategy_artifact_hash,
            portfolio_id=args.portfolio_id,
            portfolio_artifact_hash=args.portfolio_artifact_hash,
            strategy_only=args.strategy_only,
            research_short_bundle=research_short_bundle_snapshot(),
        )
    except ValueError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "mode": "MONITOR",
                    "dryRunRequired": True,
                    "brokerSubmitAllowed": False,
                    "productionLifecycleMutation": False,
                    "reason": str(exc),
                    "written": False,
                    "output": str(output),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 3
    verification = validate_monitor_only_plan(
        plan,
        verify_files=True,
    )
    if verification["ok"] is not True:
        print(
            json.dumps(
                _summary(
                    plan,
                    output=output,
                    written=False,
                    verification=verification,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 3
    if int(plan.get("candidateCount") or 0) <= 0:
        print(
            json.dumps(
                _summary(
                    plan,
                    output=output,
                    written=False,
                    verification=verification,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2

    written = False
    if not args.preview:
        write_validation_plan(output, plan)
        written = True
    print(
        json.dumps(
            _summary(
                plan,
                output=output,
                written=written,
                verification=verification,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
