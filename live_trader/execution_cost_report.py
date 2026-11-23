"""Export spot execution assumptions from local, already reconciled broker receipts."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import sqlite3

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.continuous_runtime import REALTIME_OPEN_BOUNDARY_ATTESTATIONS
from trading_runtime.observed_execution_costs import TerminalSpotCost, build_observed_spot_cost_report

from .native_spot_quantity import decimal_value
from .program_ledger import ProgramLedger
from .verified_terminal_cache import SPOT, VerifiedTerminalCache


def _read_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve(strict=True).as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _observed_cost(record: dict, dispatch: dict, interval: str) -> TerminalSpotCost | None:
    identity, proof, fills = record["identity"], record["proof"], record["receipts"]
    if proof["kind"] != SPOT or not fills:
        return None
    metadata = dispatch.get("metadata") or {}
    boundary = metadata.get("native_open_boundary") or {}
    if (metadata.get("order_purpose") != "SIGNAL" or metadata.get("market_type") not in ("spot", "SPOT")
            or boundary.get("provider") != identity["brokerId"]
            or boundary.get("sourceProvider") != identity["brokerId"]
            or boundary.get("symbol") != identity["symbol"]
            or boundary.get("sourceArtifactHash") != identity["sourceArtifactHash"]
            or boundary.get("attestation") not in REALTIME_OPEN_BOUNDARY_ATTESTATIONS
            or boundary.get("recoveryOnly") is not False or boundary.get("promotionEligible") is not True
            or boundary.get("timeframe") != interval):
        return None
    reference_price = decimal_value(boundary.get("open"), "verified provider opening price", positive=True)
    quantity = notional = fee = Decimal(0)
    for fill in fills:
        price = decimal_value(fill["price"], "actual fill price", positive=True)
        units = decimal_value(fill["quantity"], "actual fill quantity", positive=True)
        quantity += units
        notional += units * price
        payment = decimal_value(fill["fee"], "actual fill fee")
        fee += payment * price if fill["feeCurrency"] == "BASE" else payment
    last_fill = max(datetime.fromisoformat(fill["occurredAt"].replace("Z", "+00:00")) for fill in fills)
    observed_at = datetime.fromisoformat(boundary["observedTime"].replace("Z", "+00:00"))
    return TerminalSpotCost(
        evidence_hash=proof["identityHash"], side=identity["side"], reference_price=reference_price,
        filled_quantity=quantity, fill_notional=notional, fee_in_quote=fee,
        last_fill_at=last_fill, reference_observed_at=observed_at,
        partial_fill=proof["terminalStatus"] in {"CANCELED", "EXPIRED", "cancel"},
    )


def read_execution_cost_report(
    cache_path: Path, ledger_path: Path, *, deployment_id: str, artifact_hash: str,
    provider: str, symbol: str, interval: str,
) -> dict:
    scope = {"artifact_hash": artifact_hash, "provider": provider, "symbol": symbol, "interval": interval}
    # A deployment is intentionally required to avoid pooling accounts or experiments.
    if not deployment_id.strip():
        raise ValueError("A Live Trader deployment ID is required.")
    with closing(_read_database(cache_path)) as cache, closing(_read_database(ledger_path)) as ledger:
        def costs():
            rows = cache.execute(
                """
                SELECT slot_key, broker_key, payload, content_hash FROM terminal_receipts
                WHERE json_extract(payload, '$.identity.deploymentId') = ?
                  AND json_extract(payload, '$.identity.sourceArtifactHash') = ?
                  AND json_extract(payload, '$.identity.brokerId') = ?
                  AND json_extract(payload, '$.identity.symbol') = ?
                """,
                (deployment_id, artifact_hash, provider, symbol),
            )
            for row in rows:
                identity = json.loads(row["payload"])["identity"]
                record = VerifiedTerminalCache._decode(row, identity)
                dispatch_row = ledger.execute(
                    """
                    SELECT order_id, idempotency_key, broker_id, broker_order_id, state,
                           created_at, updated_at, order_json
                    FROM order_dispatch_journal WHERE order_id = ? AND idempotency_key = ?
                    """,
                    (identity["dispatchOrderId"], identity["idempotencyKey"]),
                ).fetchone()
                if dispatch_row is None:
                    raise ValueError("A verified terminal order has no matching local submission record.")
                dispatch = ProgramLedger._dispatch_order_from_row(dispatch_row)
                immutable = {key: dispatch[key] for key in (
                    "order_id", "idempotency_key", "broker_id", "broker_order_id", "symbol", "side",
                    "quantity", "reference_price", "price", "order_type", "time_in_force",
                    "order_date", "organization_no", "broker_order_key",
                ) if key in dispatch}
                immutable["metadata"] = dispatch.get("metadata") or {}
                if stable_sha256(immutable) != identity["immutableDispatchHash"]:
                    raise ValueError("A terminal order failed the exact local submission check.")
                sample = _observed_cost(record, dispatch, interval)
                if sample is not None:
                    yield sample
        return build_observed_spot_cost_report(costs(), **scope)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--terminal-cache", type=Path, required=True)
    parser.add_argument("--order-ledger", type=Path, required=True)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--artifact-hash", required=True)
    parser.add_argument("--provider", choices=("binance", "upbit"), required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--interval", required=True)
    args = parser.parse_args()
    try:
        report = read_execution_cost_report(
            args.terminal_cache, args.order_ledger, deployment_id=args.deployment_id,
            artifact_hash=args.artifact_hash, provider=args.provider, symbol=args.symbol, interval=args.interval,
        )
    except (OSError, sqlite3.Error, ValueError) as error:
        parser.exit(2, f"{error}\n")
    print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
