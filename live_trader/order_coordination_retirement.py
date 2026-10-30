"""Read-only broker reconciliation after an ordinary Live worker has stopped.

The local journal preserves exact client/order identity, not credentials or
account balances. It never cancels or places orders and never treats a missing
order response, PID disappearance, elapsed time or zero UI balance as flat.
"""
from datetime import datetime, timezone
from contextlib import contextmanager
from decimal import Decimal
import json
import sqlite3
import threading
import time
from zoneinfo import ZoneInfo

from trading_runtime.order_coordination import CoordinationError, canonical_request_hash

_ROUTES = {("binance", "SPOT"): "BINANCE_SPOT",
           ("binance", "USDM_FUTURES"): "BINANCE_USDM_FUTURES",
           ("upbit", "SPOT"): "UPBIT_KRW_SPOT", ("kis", "CASH"): "KIS_KR_CASH"}
_TERMINAL = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


def capture_ordinary_worker(manager, runtime_key):
    """Hold the actual generation, not an aggregate status or a PID lookup."""
    runtimes = vars(manager).get("runtimes")
    controllers = vars(manager).get("controllers")
    controller = runtimes.get(runtime_key) if isinstance(runtimes, dict) else None
    if controller is None and runtime_key in {"stock", "crypto"} and isinstance(controllers, dict):
        controller = controllers.get(runtime_key)
    if controller is None:
        return None
    supervisor = vars(controller).get("supervisor")
    thread = vars(supervisor).get("_thread") if supervisor is not None else None
    if not isinstance(thread, threading.Thread) or thread.ident is None:
        return None
    return controller, supervisor, thread


def ordinary_worker_stopped(captured, exact_result):
    if captured is None or not isinstance(exact_result, dict) or exact_result.get("ok") is not True:
        return False
    controller, supervisor, thread = captured
    runtime = exact_result.get("runtime")
    return (vars(controller).get("supervisor") is supervisor
            and vars(supervisor).get("_thread") is thread and not thread.is_alive()
            and isinstance(runtime, dict) and runtime.get("phase") == "STOPPED" and runtime.get("running") is False)


class LiveRetirementJournal:
    def __init__(self, path):
        self.path = path

    @contextmanager
    def _db(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        db.execute("CREATE TABLE IF NOT EXISTS owners(owner TEXT PRIMARY KEY, stopped INTEGER NOT NULL DEFAULT 0)")
        db.execute("""CREATE TABLE IF NOT EXISTS proofs(intent_id TEXT PRIMARY KEY,owner TEXT NOT NULL,
            request_hash TEXT NOT NULL, request_json TEXT NOT NULL, response_json TEXT)""")
        try:
            with db:
                yield db
        finally:
            db.close()

    def register(self, owner, receipt, intent, route, now):
        if not owner.startswith("live-runtime:"):
            return
        # Narrow identity fields only; never persist tokens or arbitrary broker payloads.
        row = {"route": route, "symbol": str(intent.get("symbol") or "").upper(),
               "clientOrderId": receipt["clientOrderId"],
               "side": str(intent.get("side") or "").upper(),
               "positionSide": str(intent.get("position_side") or intent.get("positionSide") or "BOTH").upper(),
               "quantity": str(intent.get("quantity") or intent.get("qty") or ""),
               "protection": bool(intent.get("client_algo_id")),
               "queryDate": datetime.fromtimestamp(now, ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")}
        request_hash = canonical_request_hash(dict(intent))
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT OR IGNORE INTO owners(owner) VALUES (?)", (owner,))
            if db.execute("SELECT stopped FROM owners WHERE owner=?", (owner,)).fetchone()[0]:
                raise CoordinationError("COORDINATION_STOPPED_OWNER_CANNOT_SUBMIT")
            prior = db.execute("SELECT * FROM proofs WHERE intent_id=?", (receipt["intentId"],)).fetchone()
            if prior and (prior["owner"] != owner or prior["request_hash"] != request_hash):
                raise CoordinationError("COORDINATION_RETIREMENT_IDENTITY_CONFLICT")
            db.execute("INSERT OR IGNORE INTO proofs VALUES (?,?,?,?,NULL)",
                       (receipt["intentId"], owner, request_hash, json.dumps(row, sort_keys=True)))

    def response(self, owner, receipt, raw, status, broker_id):
        if not owner.startswith("live-runtime:"):
            return
        body = raw.get("json") if isinstance(raw, dict) and "ok" in raw else raw
        body = body if isinstance(body, dict) else {}
        output = body.get("output") if isinstance(body.get("output"), dict) else {}
        row = {"status": status, "brokerOrderId": broker_id,
               "organizationNo": str(output.get("KRX_FWDG_ORD_ORGNO") or output.get("krx_fwdg_ord_orgno") or ""),
               "orderDate": str(output.get("ORD_DT") or output.get("ORD_DATE") or ""),
               "responseHash": canonical_request_hash(raw) if isinstance(raw, dict) else ""}
        with self._db() as db:
            db.execute("UPDATE proofs SET response_json=? WHERE intent_id=? AND owner=?",
                       (json.dumps(row, sort_keys=True), receipt["intentId"], owner))

    def mark_stopped(self, owner):
        if not owner.startswith("live-runtime:"):
            raise CoordinationError("COORDINATION_ORDINARY_OWNER_REQUIRED")
        with self._db() as db:
            db.execute("INSERT INTO owners VALUES (?,1) ON CONFLICT(owner) DO UPDATE SET stopped=1", (owner,))

    def stopped_owners(self):
        if not self.path.exists():
            return []
        with self._db() as db:
            return [r[0] for r in db.execute("SELECT owner FROM owners WHERE stopped=1")]

    def proof(self, owner, intent_id):
        if not self.path.exists():
            raise CoordinationError("COORDINATION_ORDER_PROOF_MISSING")
        with self._db() as db:
            row = db.execute("SELECT request_json,response_json FROM proofs WHERE owner=? AND intent_id=?",
                             (owner, intent_id)).fetchone()
        if row is None:
            raise CoordinationError("COORDINATION_ORDER_PROOF_MISSING")
        return {**json.loads(row[0]), "response": json.loads(row[1]) if row[1] else {}}


def _decimal(value):
    try:
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError
        return result
    except (ValueError, ArithmeticError):
        raise CoordinationError("COORDINATION_RECONCILIATION_QUANTITY_UNKNOWN") from None


def _timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if isinstance(value, str) else float(value)
        if not Decimal(str(result)).is_finite():
            raise ValueError
        return result
    except (ValueError, TypeError, OverflowError):
        raise CoordinationError("COORDINATION_RECONCILIATION_TIME_UNKNOWN") from None


class OrdinaryRetirementReader:
    """Existing official GET readers; the only POST they may use is KIS OAuth."""
    def __init__(self, settings=None, readers=None):
        if readers is None:
            from .short_live_broker_readers import ProductionShortLiveReaders
            from .read_only_account import runtime_settings
            readers = ProductionShortLiveReaders(settings=runtime_settings() if settings is None else settings)
        self.readers = readers

    def snapshot(self, route, symbols):
        return self.readers.snapshot(route, tuple(symbols))

    def order(self, route, proof, core_intent):
        response = proof["response"]
        client, symbol = proof["clientOrderId"], proof["symbol"]
        if core_intent["state"] == "RESERVED":
            return {"known": True, "status": "CANCELLED_BEFORE_SUBMIT", "symbol": symbol,
                    "clientOrderId": client, "filledQuantity": "0", "baseCommissionQuantity": "0",
                    "baseCommissionKnown": True, "side": proof["side"], "positionSide": proof["positionSide"], "observedAt": time.time()}
        if response.get("status") == "REJECTED" and core_intent["state"] == "REJECTED" and not response.get("brokerOrderId"):
            return {"known": True, "status": "REJECTED", "symbol": symbol, "clientOrderId": client,
                    "filledQuantity": "0", "baseCommissionQuantity": "0", "baseCommissionKnown": True,
                    "side": proof["side"], "positionSide": proof["positionSide"], "observedAt": time.time(), "rejectionHash": response.get("responseHash")}
        if proof["protection"]:
            result = self.readers.protection_order(route, {"symbol": symbol, "clientAlgoId": client,
                "algoId": response.get("brokerOrderId"), "side": proof["side"], "positionSide": proof["positionSide"]})
            if result.get("status") == "FINISHED":
                if result.get("executionKnown") is not True or (result.get("actualOrder") or {}).get("status") not in _TERMINAL:
                    raise CoordinationError("COORDINATION_PROTECTION_EXECUTION_UNKNOWN")
                result = {**result, "status": result["actualOrder"]["status"]}
            execution = result.get("actualOrder") if result.get("executionKnown") is True else None
            return {**result, "clientOrderId": result.get("clientAlgoId"),
                    "filledQuantity": execution.get("filledQuantity") if isinstance(execution, dict) else "0",
                    "baseCommissionQuantity": "0", "baseCommissionKnown": True}
        broker_id = str(response.get("brokerOrderId") or "")
        if route == "KIS_KR_CASH":
            return self._kis(proof)
        # Query by the original client ID even when a broker ID was received.
        # This prevents a stale or mismatched local broker ID from releasing a scope.
        if route == "UPBIT_KRW_SPOT":
            raw = self.readers._upbit("/v1/order", {"identifier": client})
            returned_id = str(raw.get("uuid") or "")
            matched = raw.get("identifier") == client and raw.get("market") == symbol
        else:
            provider = "binance-futures" if route == "BINANCE_USDM_FUTURES" else "binance"
            path = "/fapi/v1/order" if provider == "binance-futures" else "/api/v3/order"
            raw = self.readers._binance(provider, path, {"symbol": symbol, "origClientOrderId": client})
            returned_id = str(raw.get("orderId") or "")
            matched = raw.get("clientOrderId") == client and raw.get("symbol") == symbol
        if not matched or not returned_id or (broker_id and broker_id != returned_id):
            raise CoordinationError("COORDINATION_BROKER_ORDER_IDENTITY_MISMATCH")
        claim = {"symbol": symbol, "brokerOrderId": returned_id, "clientOrderId": client, "side": proof["side"]}
        if route == "BINANCE_USDM_FUTURES":
            claim.update(positionSide=proof["positionSide"])
            if proof["quantity"]:
                claim["quantity"] = proof["quantity"]
        return self.readers.get_order(route, claim)

    def _kis(self, proof):
        response = proof["response"]
        order_id, org = response.get("brokerOrderId"), response.get("organizationNo")
        date = response.get("orderDate") or proof.get("queryDate")
        if not order_id or not org or not isinstance(date, str) or len(date) != 8 or not date.isdigit():
            raise CoordinationError("COORDINATION_KIS_EXACT_ORDER_IDENTITY_MISSING")
        from .brokers import fetch_kis_domestic_order_truth, normalize_kis_domestic_order_truth
        from .kis_order_authority import kis_read_diagnostic_boundary
        started = time.time()
        with kis_read_diagnostic_boundary():
            raw = normalize_kis_domestic_order_truth(fetch_kis_domestic_order_truth(
                self.readers.reader._kis_token(), start_date=date, end_date=date))
        if raw.get("complete") is not True or raw.get("pagination_complete") is not True:
            raise CoordinationError("COORDINATION_KIS_EXACT_ORDER_TRUTH_UNKNOWN")
        matches = [row for row in raw["orders"] if str(row.get("broker_order_id")) == order_id
                   and str(row.get("organization_no")) == org and str(row.get("order_date")) == date
                   and str(row.get("symbol")).removesuffix(".KS").removesuffix(".KQ") == proof["symbol"]]
        if len(matches) != 1 or order_id in raw.get("ambiguous_broker_order_ids", []):
            raise CoordinationError("COORDINATION_KIS_EXACT_ORDER_TRUTH_UNKNOWN")
        row = matches[0]
        if str(row.get("side")).upper() != proof["side"]:
            raise CoordinationError("COORDINATION_BROKER_ORDER_IDENTITY_MISMATCH")
        return {"known": True, "symbol": proof["symbol"], "clientOrderId": proof["clientOrderId"],
                "brokerOrderId": order_id, "status": str(row["state"]).upper(), "side": proof["side"],
                "filledQuantity": str(row["filled_quantity"]), "baseCommissionQuantity": "0",
                "baseCommissionKnown": True, "observedAt": started, "orderDate": date, "organizationNo": org}


def reconcile_stopped_owner(helper, owner_id, reader=None):
    """Release only exact stopped owner scopes with current complete proof."""
    if owner_id not in helper.retirement_journal.stopped_owners():
        raise CoordinationError("COORDINATION_WORKER_STOP_NOT_VERIFIED")
    result = helper.retirement_status(owner_id)
    result.update(releasedScopes=0, blockers=[])
    if not result["retained"]:
        return result
    reader = reader or OrdinaryRetirementReader()
    owned = helper.coordinator.list_owned(owner_id=owner_id)
    for scope in owned["scopes"]:
        if scope["status"] != "CLAIMED":
            continue
        try:
            route = _ROUTES[(scope["broker"], scope["market"])]
            helper._owned_scope(owner_id, route, scope["symbol"])
            intents = [i for i in owned["intents"] if i["scopeKey"] == scope["scope_key"]]
            times, terminal, observations, net = [], {}, [], Decimal(0)
            side_nets = {}
            for item in intents:
                proof = helper.retirement_journal.proof(owner_id, item["intentId"])
                if proof["symbol"] != scope["symbol"] or proof["clientOrderId"] != item["clientOrderId"]:
                    raise CoordinationError("COORDINATION_ORDER_PROOF_IDENTITY_MISMATCH")
                observation = reader.order(route, proof, item)
                if (observation.get("known") is not True or observation.get("symbol") != scope["symbol"]
                        or observation.get("clientOrderId") != item["clientOrderId"]):
                    raise CoordinationError("COORDINATION_OWNED_ORDER_UNKNOWN")
                if observation.get("side") != proof["side"]:
                    raise CoordinationError("COORDINATION_OWNED_ORDER_SIDE_MISMATCH")
                status = str(observation.get("status") or "").upper().replace("CANCELLED", "CANCELED")
                if status == "CANCELED_BEFORE_SUBMIT":
                    status = "CANCELLED_BEFORE_SUBMIT"
                if status not in _TERMINAL | {"CANCELLED_BEFORE_SUBMIT"}:
                    raise CoordinationError("COORDINATION_OWNED_ORDER_NOT_TERMINAL")
                if status == "CANCELLED_BEFORE_SUBMIT" and item["state"] != "RESERVED":
                    raise CoordinationError("COORDINATION_OWNED_ORDER_UNKNOWN")
                filled = _decimal(observation.get("filledQuantity"))
                if filled < 0 or observation.get("baseCommissionKnown") is not True:
                    raise CoordinationError("COORDINATION_OWNED_FILL_UNKNOWN")
                fee = _decimal(observation.get("baseCommissionQuantity"))
                if fee < 0 or observation.get("side") not in {"BUY", "SELL"}:
                    raise CoordinationError("COORDINATION_OWNED_FILL_UNKNOWN")
                net += filled * (1 if observation["side"] == "BUY" else -1) - fee
                if scope["market"] == "USDM_FUTURES":
                    position_side = observation.get("positionSide")
                    if position_side not in {"BOTH", "LONG", "SHORT"} or position_side != proof["positionSide"]:
                        raise CoordinationError("COORDINATION_OWNED_POSITION_SIDE_MISMATCH")
                    side_nets[position_side] = side_nets.get(position_side, Decimal(0)) + filled * (1 if observation["side"] == "BUY" else -1)
                terminal[item["clientOrderId"]] = status
                times.append(_timestamp(observation.get("observedAt")))
                observations.append(observation)
            snapshot = reader.snapshot(route, [scope["symbol"]])
            if (snapshot.get("known") is not True or snapshot.get("ordersComplete") is not True
                    or snapshot.get("positionsComplete") is not True):
                raise CoordinationError("COORDINATION_ACCOUNT_TRUTH_UNKNOWN")
            for field in ("openOrders", "positions"):
                if not isinstance(snapshot.get(field), list):
                    raise CoordinationError("COORDINATION_ACCOUNT_TRUTH_UNKNOWN")
                if any(not isinstance(row, dict) or not isinstance(row.get("symbol"), str) or not row["symbol"]
                       for row in snapshot[field]):
                    raise CoordinationError("COORDINATION_ACCOUNT_TRUTH_UNKNOWN")
            pending = [r for r in snapshot["openOrders"] if r.get("symbol") == scope["symbol"]]
            if scope["market"] == "USDM_FUTURES":
                if snapshot.get("algoOrdersComplete") is not True or not isinstance(snapshot.get("openAlgoOrders"), list):
                    raise CoordinationError("COORDINATION_PROTECTION_TRUTH_UNKNOWN")
                if any(not isinstance(row, dict) or not isinstance(row.get("symbol"), str) or not row["symbol"]
                       for row in snapshot["openAlgoOrders"]):
                    raise CoordinationError("COORDINATION_PROTECTION_TRUTH_UNKNOWN")
                pending += [r for r in snapshot["openAlgoOrders"] if r.get("symbol") == scope["symbol"]]
                if any(r.get("positionSide") not in {"BOTH", "LONG", "SHORT"}
                       for r in snapshot["positions"] if r.get("symbol") == scope["symbol"]):
                    raise CoordinationError("COORDINATION_FUTURES_POSITION_SIDE_UNKNOWN")
                if any(_decimal(r.get("quantity")) != 0 for r in snapshot["positions"] if r.get("symbol") == scope["symbol"]):
                    raise CoordinationError("COORDINATION_FUTURES_POSITION_NOT_FLAT")
                if any(value != 0 for value in side_nets.values()):
                    raise CoordinationError("COORDINATION_OWNED_FUTURES_FILL_NOT_FLAT")
            elif net != 0:
                # Existing assets remain external; do not subtract them, sell
                # them, or declare an unmatched owned fill an external deposit.
                raise CoordinationError("COORDINATION_OWNED_SPOT_QUANTITY_NOT_FLAT")
            if pending:
                raise CoordinationError("COORDINATION_PENDING_ORDERS_REMAIN")
            times.append(_timestamp(snapshot.get("observedAt")))
            now = helper.coordinator.clock()
            if any(not -2 <= now - observed <= 30 for observed in times):
                raise CoordinationError("COORDINATION_RECONCILIATION_STALE")
            truth = {"observedAt": min(times), "ownedPositionFlat": True, "pendingOrderCount": 0,
                     "terminalOrders": terminal, "evidenceHash": canonical_request_hash({"orders": observations, "account": snapshot})}
            helper.reconcile_release(owner_id, route, [scope["symbol"]], truth)
            result["releasedScopes"] += 1
        except Exception as exc:
            result["blockers"].append({"broker": scope["broker"], "market": scope["market"], "symbol": scope["symbol"],
                "reason": exc.code if isinstance(exc, CoordinationError) else "COORDINATION_BROKER_RECONCILIATION_UNAVAILABLE"})
    result.update(helper.retirement_status(owner_id))
    result["reason"] = "OWNED_ORDERS_AND_POSITIONS_RECONCILED" if not result["retained"] else "OWNED_POSITION_AND_ORDER_RECONCILIATION_REQUIRED"
    return result
