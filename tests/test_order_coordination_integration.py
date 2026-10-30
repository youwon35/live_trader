from contextlib import nullcontext, ExitStack
from pathlib import Path
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from trading_runtime.order_coordination import CoordinationError, OrderCoordinator, canonical_request_hash
from live_trader.order_coordination import LiveOrderCoordination, live_order_coordination_context, read_dispatch_funds


class LiveCoordinationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = patch.dict("os.environ", {"LOCALAPPDATA": self.temp.name, "BINANCE_API_KEY": "synthetic-key",
            "UPBIT_ACCESS_KEY": "synthetic-upbit-key", "KIS_ACCOUNT_NO": "12345678", "KIS_ACCOUNT_PRODUCT_CODE": "01"})
        self.env.start()
        self.addCleanup(self.env.stop)
        block_network = patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden in offline test"))
        block_network.start()
        self.addCleanup(block_network.stop)
        self.c = OrderCoordinator(Path(self.temp.name) / "injected.sqlite3")
        self.live = LiveOrderCoordination(self.c)
        self.intent = {"broker_id": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "1",
                       "price": "10", "notional": "10", "identifier": "stable-order-id"}

    def funds(self, **values):
        return {"currency": "USDT", "required": "10.1", "available": "20", "observedAt": time.time(), "unitScale": 8, **values}

    def test_one_callback_only_on_repeat_and_unknown(self):
        sender = Mock(side_effect=TimeoutError("synthetic transport failure"))
        with self.assertRaises(TimeoutError):
            self.live.dispatch(owner_id="live-short:1", route="binance", intent=self.intent,
                submit=sender, budget_evidence=self.funds())
        self.assertEqual(1, sender.call_count)
        with self.assertRaisesRegex(CoordinationError, "ALREADY_SUBMITTED_RECONCILE_ONLY"):
            self.live.dispatch(owner_id="live-short:1", route="binance", intent=self.intent,
                submit=sender, budget_evidence=self.funds())
        self.assertEqual(1, sender.call_count)
        self.assertEqual("UNKNOWN", self.c.find_intent(owner_id="live-short:1", intent_key="stable-order-id")["state"])

    def test_other_program_claim_blocks_before_callback_or_balance_read(self):
        self.live.claim("gpt-controller", "BINANCE_SPOT", ["BTCUSDT"])
        sender, reader = Mock(), Mock()
        with self.assertRaisesRegex(CoordinationError, "SCOPE_OWNED_BY_ANOTHER_RUN"):
            self.live.dispatch(owner_id="live-short:1", route="binance", intent=self.intent, submit=sender, funds_reader=reader)
        sender.assert_not_called()
        reader.assert_not_called()

    def test_partial_multi_symbol_claim_is_rolled_back_without_touching_other_owner(self):
        self.live.claim("gpt-controller", "BINANCE_SPOT", ["ETHUSDT"])
        with self.assertRaisesRegex(CoordinationError, "SCOPE_OWNED_BY_ANOTHER_RUN"):
            self.live.claim("live-short:1", "BINANCE_SPOT", ["BTCUSDT", "ETHUSDT"])
        self.live.claim("other", "BINANCE_SPOT", ["BTCUSDT"])
        self.assertEqual("CLAIMED", self.c.list_owned(owner_id="gpt-controller")["scopes"][0]["status"])

    def test_fresh_native_evidence_skips_additional_read(self):
        reader = Mock(side_effect=AssertionError("extra read forbidden"))
        sender = Mock(return_value={"orderId": 123, "status": "FILLED"})
        self.live.dispatch(owner_id="live-short:1", route="binance", intent=self.intent,
            submit=sender, funds_reader=reader, budget_evidence=self.funds())
        reader.assert_not_called()
        receipt = self.c.find_intent(owner_id="live-short:1", intent_key="stable-order-id")
        self.assertEqual("FILLED", receipt["state"])
        self.assertFalse(receipt["reconciled"])
        self.assertEqual(1010000000, receipt["reservationUnits"])

    def test_reducing_protection_needs_owned_scope_but_no_new_capital(self):
        protection = {"symbol": "SOLUSDT", "client_algo_id": "owned-stop", "close_position": True}
        reader = Mock(side_effect=AssertionError("no balance read for protection"))
        sender = Mock(return_value={"algoId": 42})
        with self.assertRaisesRegex(CoordinationError, "REDUCE_SCOPE_NOT_OWNED"):
            self.live.dispatch(owner_id="live-short:1", route="binance-futures", intent=protection,
                submit=sender, funds_reader=reader, reducing_only=True)
        sender.assert_not_called()
        self.live.claim("live-short:1", "BINANCE_USDM_FUTURES", ["SOLUSDT"])
        self.live.dispatch(owner_id="live-short:1", route="binance-futures", intent=protection,
            submit=sender, funds_reader=reader, reducing_only=True)
        reader.assert_not_called()
        self.assertEqual(0, self.c.find_intent(owner_id="live-short:1", intent_key="owned-stop")["reservationUnits"])

    def test_release_requires_actual_flat_pending_zero_and_per_order_terminal(self):
        self.live.dispatch(owner_id="live-short:1", route="binance", intent=self.intent,
            submit=lambda: {"orderId": 123, "status": "FILLED"}, budget_evidence=self.funds())
        truth = {"observedAt": time.time(), "evidenceHash": canonical_request_hash({"observed": "flat"}),
                 "ownedPositionFlat": True, "pendingOrderCount": 0, "terminalOrders": {"stable-order-id": "FILLED"}}
        with self.assertRaisesRegex(CoordinationError, "EXPOSURE_NOT_RECONCILED_FLAT"):
            self.live.reconcile_release("live-short:1", "binance", ["BTCUSDT"], {**truth, "ownedPositionFlat": False})
        with self.assertRaisesRegex(CoordinationError, "TERMINAL_ORDER_EVIDENCE_MISSING"):
            self.live.reconcile_release("live-short:1", "binance", ["BTCUSDT"], {**truth, "terminalOrders": {}})
        self.assertTrue(self.live.reconcile_release("live-short:1", "binance", ["BTCUSDT"], truth))
        self.live.claim("gpt-controller", "binance", ["BTCUSDT"])

    def test_rollback_refuses_even_reserved_intent(self):
        claims = self.live.claim("live-short:1", "binance", ["BTCUSDT"])
        self.c.reserve_intent(scope_key=claims[0]["scopeKey"], owner_id="live-short:1", intent_key="x",
            request_hash=canonical_request_hash(self.intent), currency="USDT", amount_units=1,
            available_units=2, unit_scale=8, observed_at=time.time())
        with self.assertRaisesRegex(CoordinationError, "INTENT_EXISTS_RECONCILIATION_REQUIRED"):
            self.live.rollback_unsubmitted("live-short:1", claims)

    def test_http_timeout_is_not_a_known_rejection(self):
        self.assertEqual(("UNKNOWN", ""), self.live._result({"ok": False, "json": {"code": -1007}}))
        self.assertEqual(("FILLED", "123"), self.live._result({"ok": True, "json": {"orderId": 123, "status": "FILLED"}}))
        for flags in ({"outcomeAmbiguous": True}, {"redirectBlocked": True}, {"statusCode": 0}, {"statusCode": 500}):
            self.assertEqual(("UNKNOWN", ""), self.live._result({"ok": True,
                "json": {"orderId": 123, "status": "FILLED"}, **flags}))
            self.assertEqual(("UNKNOWN", ""), self.live._result({"ok": False,
                "json": {"code": -2010}, **flags}))

    def test_invalid_pre_dispatch_releases_only_new_empty_scope(self):
        with self.assertRaisesRegex(CoordinationError, "FRESH_FUNDS_REQUIRED"):
            self.live.dispatch(owner_id="live-runtime:1", route="binance", intent=self.intent, submit=Mock())
        self.live.claim("gpt-controller", "binance", ["BTCUSDT"])
        self.live.claim("live-runtime:1", "binance", ["ETHUSDT"])
        with self.assertRaisesRegex(CoordinationError, "FRESH_FUNDS_REQUIRED"):
            self.live.dispatch(owner_id="live-runtime:1", route="binance",
                intent={**self.intent, "symbol": "ETHUSDT"}, submit=Mock())
        with self.assertRaisesRegex(CoordinationError, "SCOPE_OWNED_BY_ANOTHER_RUN"):
            self.live.claim("gpt-controller", "binance", ["ETHUSDT"])

    def test_upbit_explicit_rejection_without_order_id_releases_after_fresh_flat(self):
        from test_short_live_brokers import Fixture, NOW
        from live_trader.order_coordination import current_coordination_context
        f = Fixture("UPBIT_KRW_SPOT")
        coordinator = OrderCoordinator(Path(self.temp.name) / "upbit-rejected.sqlite3", clock=lambda: NOW)
        helper = LiveOrderCoordination(coordinator)
        f.adapter._coordination, f.adapter._coordination_settings = helper, dict(__import__("os").environ)
        response = {"ok": False, "statusCode": 400, "json": {"error": {"name": "insufficient_funds_bid"}}}
        def dispatch(route, intent):
            c = current_coordination_context()
            return helper.dispatch(owner_id=c["ownerId"], route=route, intent=intent,
                submit=lambda: response, budget_evidence=c["budgetEvidence"])
        f.adapter.transport.submit = dispatch
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        f.adapter.claim_coordination(f.manifest)
        result = f.adapter.submit(f.adapter.size_order(f.proposal()))
        self.assertEqual("REJECTED", result["status"])
        self.assertTrue(f.adapter.release_coordination(f.manifest))
        self.assertEqual(("UNKNOWN", ""), helper._result({"ok": False, "statusCode": 400,
            "json": {"error": {"name": "duplicated_identifier"}}}))
        self.assertEqual(("UNKNOWN", ""), helper._result({**response, "statusCode": 500}))

    def test_roundtrip_settles_funds_without_releasing_scope(self):
        self.live.dispatch(owner_id="live-short:1", route="binance", intent=self.intent,
            submit=lambda: {"orderId": 123, "status": "FILLED"}, budget_evidence=self.funds())
        truth = {"observedAt": time.time(), "evidenceHash": canonical_request_hash({"flat": True}),
                 "ownedPositionFlat": True, "pendingOrderCount": 0, "terminalOrders": {"stable-order-id": "FILLED"}}
        self.live.reconcile_release("live-short:1", "binance", ["BTCUSDT"], truth, release_scope=False)
        self.assertTrue(self.c.find_intent(owner_id="live-short:1", intent_key="stable-order-id")["reconciled"])
        with self.assertRaisesRegex(CoordinationError, "SCOPE_OWNED_BY_ANOTHER_RUN"):
            self.live.claim("gpt-controller", "binance", ["BTCUSDT"])
        self.live.dispatch(owner_id="live-short:1", route="binance", intent={**self.intent, "identifier": "next-cycle"},
            submit=lambda: {"orderId": 124, "status": "NEW"}, budget_evidence=self.funds())

    def test_short_adapter_prepare_does_not_claim_start_context_reaches_sender_and_flat_release(self):
        from test_short_live_brokers import Fixture, NOW
        from live_trader.order_coordination import current_coordination_context
        f = Fixture("BINANCE_SPOT")
        coordinator = OrderCoordinator(Path(self.temp.name) / "short.sqlite3", clock=lambda: NOW)
        helper = LiveOrderCoordination(coordinator)
        f.adapter._coordination, f.adapter._coordination_settings = helper, dict(__import__("os").environ)
        original = f.adapter.transport.submit
        failures = []
        def coordinated(route, intent):
            context = current_coordination_context()
            self.assertEqual("live-short:session-exact", context["ownerId"])
            self.assertEqual(8, context["budgetEvidence"]["unitScale"])
            try:
                return helper.dispatch(owner_id=context["ownerId"], route=route, intent=intent,
                    submit=lambda: original(route, intent), budget_evidence=context["budgetEvidence"],
                    reducing_only=intent["risk_reducing"])
            except Exception as exc:
                failures.append(repr(exc))
                raise
        f.adapter.transport.submit = coordinated
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        self.assertFalse(coordinator.path.exists())
        f.adapter.claim_coordination(f.manifest)
        buy = f.adapter.size_order(f.proposal())
        self.assertEqual("FILLED", f.adapter.submit(buy)["status"], failures)
        sell = f.adapter.size_order(f.proposal(side="SELL", reducing=True, cid="short-client-0002"))
        self.assertEqual("FILLED", f.adapter.submit(sell)["status"])
        self.assertTrue(f.adapter.reconcile(f.manifest)["known"])
        self.assertTrue(all(i["reconciled"] for i in coordinator.list_owned(owner_id="live-short:session-exact")["intents"]))
        with self.assertRaisesRegex(CoordinationError, "SCOPE_OWNED_BY_ANOTHER_RUN"):
            helper.claim("gpt-controller", "BINANCE_SPOT", [f.symbol])
        self.assertTrue(f.adapter.release_coordination(f.manifest))
        helper.claim("gpt-controller", "BINANCE_SPOT", [f.symbol])

    def test_short_service_claims_after_durable_start_before_feed_and_releases_after_stop(self):
        from test_short_live_session import ShortLiveSessionTests
        fixture = ShortLiveSessionTests("runTest")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        service, brokers, feeds = fixture.service()
        calls = []
        broker = brokers["BINANCE_SPOT"]
        def claim(manifest):
            self.assertIsNotNone(service._session)
            self.assertFalse(feeds["BINANCE_SPOT"].connected)
            self.assertTrue(list(fixture.root.rglob("session-start-*.json")))
            calls.append("claim")
        def release(manifest):
            self.assertTrue(service._session["naturalExitObserved"])
            self.assertTrue(feeds["BINANCE_SPOT"].closed)
            calls.append("release")
            return True
        broker.claim_coordination, broker.release_coordination = claim, release
        fixture.prepare(service)
        self.assertEqual([], calls)
        fixture.start(service)
        self.assertEqual(["claim"], calls)
        service.stop({})
        self.assertEqual(["claim", "release"], calls)
        self.assertTrue(service._session["terminalVerified"])

    def test_short_service_second_route_claim_failure_rolls_back_empty_first_before_feeds(self):
        from test_short_live_session import ShortLiveSessionTests, runtime_spec
        fixture = ShortLiveSessionTests("runTest")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        service, brokers, feeds = fixture.service([runtime_spec(), runtime_spec("UPBIT_KRW_SPOT", "upbit")])
        calls = []
        brokers["BINANCE_SPOT"].claim_coordination = lambda _: calls.append("claimed")
        brokers["BINANCE_SPOT"].rollback_unsubmitted_coordination = lambda: calls.append("rollback")
        def conflict(_):
            raise CoordinationError("SCOPE_OWNED_BY_ANOTHER_RUN")
        brokers["UPBIT_KRW_SPOT"].claim_coordination = conflict
        with self.assertRaises(CoordinationError):
            fixture.start(service)
        self.assertEqual(["claimed", "rollback"], calls)
        self.assertFalse(any(feed.connected for feed in feeds.values()))

    def test_short_terminal_rejects_failed_shared_retirement(self):
        from test_short_live_session import ShortLiveSessionTests
        fixture = ShortLiveSessionTests("runTest")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        service, brokers, _ = fixture.service()
        brokers["BINANCE_SPOT"].release_coordination = lambda _: False
        fixture.start(service)
        service.stop({})
        self.assertFalse(service._session["terminalVerified"])
        self.assertEqual("BLOCKED", service._session["status"])

    def test_normal_monitor_stop_status_does_not_clear_filled_exposure(self):
        self.assertFalse(self.live.retirement_status("live-runtime:1")["retained"])
        self.assertFalse(self.c.path.exists())
        self.live.dispatch(owner_id="live-runtime:1", route="binance", intent=self.intent,
            submit=lambda: {"orderId": 123, "status": "FILLED"}, budget_evidence=self.funds())
        view = self.live.retirement_status("live-runtime:1")
        self.assertTrue(view["retained"])
        self.assertEqual(1, view["unreconciledIntentCount"])
        self.assertFalse(self.c.find_intent(owner_id="live-runtime:1", intent_key="stable-order-id")["reconciled"])

    def test_short_futures_protection_and_cleanup_share_owner_and_release_only_after_stop_retired(self):
        from test_short_live_brokers import Fixture, NOW
        from live_trader.order_coordination import current_coordination_context
        f = Fixture("BINANCE_USDM_FUTURES")
        coordinator = OrderCoordinator(Path(self.temp.name) / "futures.sqlite3", clock=lambda: NOW)
        helper = LiveOrderCoordination(coordinator)
        f.adapter._coordination, f.adapter._coordination_settings = helper, dict(__import__("os").environ)
        submit = f.adapter.transport.submit
        protect = f.adapter._protection.transport.protect
        def dispatch(route, intent):
            c = current_coordination_context()
            return helper.dispatch(owner_id=c["ownerId"], route=route, intent=intent,
                submit=lambda: submit(route, intent), budget_evidence=c["budgetEvidence"], reducing_only=intent["risk_reducing"])
        def protection(order, guard):
            c = current_coordination_context()
            self.assertEqual("live-short:session-exact", c["ownerId"])
            native = {"symbol": order["symbol"], "client_algo_id": order["clientAlgoId"]}
            return helper.dispatch(owner_id=c["ownerId"], route=f.route, intent=native,
                submit=lambda: protect(order, guard), reducing_only=True)
        f.adapter.transport.submit, f.adapter._protection.transport.protect = dispatch, protection
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        f.adapter.claim_coordination(f.manifest)
        self.assertEqual("FILLED", f.adapter.submit(f.adapter.size_order(f.proposal()))["status"])
        self.assertEqual("FILLED", f.adapter.submit(f.adapter.size_order(
            f.proposal(side="SELL", reducing=True, cid="short-client-0002")))["status"])
        # Flat entry/exit is insufficient while the close-all stop remains.
        with self.assertRaisesRegex(Exception, "EXPOSURE_NOT_FLAT"):
            f.adapter.release_coordination(f.manifest)
        self.assertTrue(f.adapter.retire_flat_protection(f.manifest))
        self.assertTrue(f.adapter.release_coordination(f.manifest))
        rows = coordinator.list_owned(owner_id="live-short:session-exact")["intents"]
        self.assertEqual(3, len(rows))
        self.assertTrue(all(row["reconciled"] for row in rows))
        helper.claim("gpt-controller", f.route, [f.symbol])

    def test_spot_dispatch_funds_uses_free_not_locked_or_equity(self):
        with patch("live_trader.brokers.send_binance_signed_request", return_value={"ok": True, "json": {"balances": [{"asset": "USDT", "free": "20", "locked": "90"}]}}):
            funds = read_dispatch_funds(self.intent)
        self.assertEqual("20", funds["available"])
        self.assertEqual("10.10", funds["required"])

    def test_futures_reserves_margin_plus_cost_not_leveraged_notional(self):
        intent = {**self.intent, "broker_id": "binance-futures", "max_leverage": 10}
        with patch("live_trader.brokers.send_binance_signed_request", return_value={"ok": True, "json": {"assets": [{"asset": "USDT", "availableBalance": "2", "walletBalance": "90"}]}}):
            funds = read_dispatch_funds(intent)
        self.assertEqual("2", funds["available"])
        self.assertEqual("1.10", funds["required"])

    def test_futures_missing_available_must_not_fallback_to_wallet(self):
        intent = {**self.intent, "broker_id": "binance-futures"}
        with patch("live_trader.brokers.send_binance_signed_request", return_value={"ok": True, "json": {"assets": [{"asset": "USDT", "walletBalance": "90"}]}}):
            with self.assertRaisesRegex(CoordinationError, "AVAILABLE_BALANCE_UNKNOWN"):
                read_dispatch_funds(intent)

    def test_router_final_binance_spot_boundary_uses_coordination(self):
        from live_trader.brokers import LiveBrokerRouter, BrokerNotReadyError
        router = LiveBrokerRouter(coordination_owner_id="live-runtime:1", order_coordinator=self.c,
                                  coordination_funds_reader=lambda _: self.funds())
        sender = Mock(return_value={"orderId": 123, "status": "NEW"})
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), \
             patch("live_trader.brokers.normalize_binance_spot_intent", side_effect=lambda value: value), \
             patch("live_trader.brokers.ordinary_binance_final_mutation_boundary", return_value=nullcontext()), \
             patch("live_trader.brokers.send_binance_signed_request", sender), \
             patch("live_trader.short_live_order_authority.validate_short_live_native_payload"), \
             patch("live_trader.short_live_order_authority.validate_short_live_normalized_payload"):
            router.place_order(self.intent)
            with self.assertRaisesRegex(BrokerNotReadyError, "ALREADY_SUBMITTED_RECONCILE_ONLY"):
                router.place_order(self.intent)
        self.assertEqual(1, sender.call_count)

    def test_context_owner_preserves_native_payload(self):
        from live_trader.brokers import LiveBrokerRouter
        router = LiveBrokerRouter(order_coordinator=self.c,
            coordination_funds_reader=Mock(side_effect=AssertionError("fresh context already provided")))
        original = dict(self.intent)
        with live_order_coordination_context("live-short:1", budget_evidence=self.funds()):
            router._coordinated_send(self.intent, lambda: {"orderId": 123, "status": "NEW"})
        self.assertEqual(original, self.intent)

    def test_router_spot_journal_uses_normalized_wire_symbol_and_quantity(self):
        from live_trader.brokers import LiveBrokerRouter
        owner = "live-runtime:normalized-spot"
        original = {**self.intent, "symbol": "btc-usdt", "side": "SELL", "quantity": "1.234567"}
        normalized = {**original, "symbol": "BTCUSDT", "quantity": "1.234", "qty": "1.234"}
        before = dict(original)
        wires = []
        def send(builder, **_):
            wires.append(builder().query)
            return {"orderId": 123, "status": "NEW"}
        router = LiveBrokerRouter(coordination_owner_id=owner, order_coordinator=self.c,
            coordination_funds_reader=lambda _: self.funds())
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), \
             patch("live_trader.brokers.normalize_binance_spot_intent", return_value=normalized), \
             patch("live_trader.brokers.ordinary_binance_final_mutation_boundary", return_value=nullcontext()), \
             patch("live_trader.brokers.send_binance_signed_request", side_effect=send), \
             patch("live_trader.short_live_order_authority.validate_short_live_native_payload"), \
             patch("live_trader.short_live_order_authority.validate_short_live_normalized_payload"):
            router.place_order(original)
        receipt = self.c.list_owned(owner_id=owner)["intents"][0]
        proof = router._order_coordination.retirement_journal.proof(owner, receipt["intentId"])
        self.assertEqual(wires[0]["symbol"], proof["symbol"])
        self.assertEqual(wires[0]["quantity"], proof["quantity"])
        self.assertEqual(wires[0]["newClientOrderId"], proof["clientOrderId"])
        self.assertEqual(before, original)

    def test_router_futures_journal_matches_actual_hedge_and_one_way_wire(self):
        from live_trader.brokers import LiveBrokerRouter
        for hedge, direction, expected in ((True, "short", "SHORT"), (False, "long", "BOTH")):
            with self.subTest(hedge=hedge):
                owner = "live-runtime:normalized-futures-" + str(hedge)
                original = {**self.intent, "broker_id": "binance-futures", "symbol": "sol-usdt.PERP" if hedge else "eth-usdt.PERP",
                    "quantity": "1.234567", "position_direction": direction, "identifier": "wire-id-" + str(hedge)}
                normalized = {**original, "symbol": "SOLUSDT" if hedge else "ETHUSDT", "quantity": "1.234", "qty": "1.234"}
                before = dict(original)
                wires, queries = [], []
                def send(builder, **_):
                    queries.append(builder)
                    if len(queries) == 1:
                        return {"ok": True, "json": {"dualSidePosition": hedge}}
                    if len(queries) == 2:
                        return {"ok": True, "json": []}
                    wires.append(builder().query)
                    return {"orderId": 123, "status": "NEW"}
                router = LiveBrokerRouter(coordination_owner_id=owner, order_coordinator=self.c,
                    coordination_funds_reader=lambda _: self.funds(available="100"))
                with ExitStack() as stack:
                    stack.enter_context(patch("live_trader.brokers.real_orders_enabled", return_value=True))
                    stack.enter_context(patch("live_trader.brokers.normalize_binance_futures_intent", return_value=normalized))
                    stack.enter_context(patch("live_trader.brokers.ordinary_binance_final_mutation_boundary", return_value=nullcontext()))
                    stack.enter_context(patch("live_trader.brokers.send_binance_signed_request", side_effect=send))
                    stack.enter_context(patch("live_trader.brokers.normalize_binance_futures_symbol_config", return_value={}))
                    stack.enter_context(patch("live_trader.brokers.validate_binance_futures_execution_policy"))
                    stack.enter_context(patch.object(router, "_verify_futures_entry_bracket"))
                    stack.enter_context(patch("live_trader.short_live_order_authority.validate_short_live_native_payload"))
                    stack.enter_context(patch("live_trader.short_live_order_authority.validate_short_live_normalized_payload"))
                    router.place_order(original)
                receipt = self.c.list_owned(owner_id=owner)["intents"][0]
                proof = router._order_coordination.retirement_journal.proof(owner, receipt["intentId"])
                self.assertEqual(expected, wires[0]["positionSide"])
                for wire, journal in (("symbol", "symbol"), ("quantity", "quantity"), ("newClientOrderId", "clientOrderId"), ("positionSide", "positionSide")):
                    self.assertEqual(wires[0][wire], proof[journal])
                self.assertEqual(before, original)

    def test_router_rejects_client_id_truncation_before_reserving_or_sending(self):
        from live_trader.brokers import LiveBrokerRouter, BrokerNotReadyError
        for identifier in ("a" * 37, "contains space", ""):
            with self.subTest(identifier=identifier):
                with self.assertRaisesRegex(BrokerNotReadyError, "EXACT_CLIENT_ORDER_ID_REQUIRED"):
                    LiveBrokerRouter._binance_coordination_intent({**self.intent, "identifier": identifier}, hedge_mode=True)
        self.assertFalse(self.c.path.exists())

    def test_prepared_kis_identity_preserves_authority_input_but_uses_wire_code_and_integer_quantity(self):
        from live_trader.brokers import LiveBrokerRouter
        original = {**self.intent, "broker_id": "kis", "symbol": "005930.KS", "quantity": "1.9"}
        before = dict(original)
        prepared = SimpleNamespace(provider="kis", body={"PDNO": "005930", "ORD_QTY": "1"})
        exact = LiveBrokerRouter._prepared_coordination_intent(original, prepared)
        self.assertEqual("005930", exact["symbol"])
        self.assertEqual("1", exact["quantity"])
        self.assertEqual(before, original)

    def test_router_upbit_identity_uses_actual_market_override_and_native_identifier(self):
        from live_trader.brokers import LiveBrokerRouter
        owner = "live-runtime:upbit-wire"
        original = {**self.intent, "broker_id": "upbit", "symbol": "KRW-BTC", "market": "KRW-ETH", "side": "SELL", "order_type": "market"}
        before = dict(original)
        sent = []
        def send(prepared):
            sent.append(prepared.body)
            return {"uuid": "native-order", "state": "wait"}
        router = LiveBrokerRouter(coordination_owner_id=owner, order_coordinator=self.c,
            coordination_funds_reader=lambda _: self.funds(currency="KRW"))
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), \
             patch("live_trader.brokers.ordinary_upbit_final_mutation_boundary", return_value=nullcontext()), \
             patch("live_trader.brokers.send_prepared_request", side_effect=send), \
             patch("live_trader.short_live_order_authority.validate_short_live_native_payload"), \
             patch("live_trader.short_live_order_authority.validate_short_live_prepared_request"):
            router.place_order(original)
        receipt = self.c.list_owned(owner_id=owner)["intents"][0]
        proof = router._order_coordination.retirement_journal.proof(owner, receipt["intentId"])
        self.assertEqual(sent[0]["market"], proof["symbol"])
        self.assertEqual("KRW-ETH", proof["symbol"])
        self.assertEqual(sent[0]["volume"], proof["quantity"])
        self.assertEqual(sent[0]["identifier"], proof["clientOrderId"])
        self.assertEqual("SELL", proof["side"])
        self.assertEqual(before, original)


if __name__ == "__main__":
    unittest.main()
