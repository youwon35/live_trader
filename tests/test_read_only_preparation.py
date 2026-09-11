from __future__ import annotations
import ast
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import types
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlparse
import test_paper_candidate_inbox as base
import test_monitor_trial as trial

service = importlib.import_module(base.PACKAGE + ".read_only_preparation")
account = importlib.import_module(base.PACKAGE + ".read_only_account")


class PreparationTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        trial.fixture(self.root)
        self.request = {"source": service.list_preparation_sources(roots=[self.root])[0]["request"],
                        "draft": {}, "readAccount": False}

    def run_preview(self, reader=None):
        return service.prepare_read_only(self.request, roots=[self.root], reader=reader)

    def test_blank_draft_stays_missing_no_reader_no_writes_no_promotion(self):
        before = base.file_bytes(self.root)
        reader = Mock()
        result = self.run_preview(reader)
        reader.read.assert_not_called()
        self.assertEqual(before, base.file_bytes(self.root))
        self.assertEqual("FUNCTIONAL_TEST_NON_PROMOTION", result["source"]["evidenceClass"])
        self.assertEqual(4, len(result["draft"]["missingInputs"]))
        self.assertIsNone(result["draft"]["notional"])
        for name in ("authorityGranted", "authorizationGranted", "executable", "promotionEligible", "useAsPromotionEvidence"):
            self.assertIs(result[name], False)
        for name in ("confirmationTokensCreated", "permitsCreated", "runtimeSessionsCreated", "ordersSubmitted"):
            self.assertEqual(0, result[name])
        self.assertFalse(base.validate_paper_live_evidence(result).valid)

    def test_user_decimal_draft_exact_and_broker_facts_do_not_grant_authority(self):
        self.request["draft"] = {"instanceId": "trial-instance", "side": "BUY", "quantity": "0.1", "limitPrice": "0.2"}
        self.request["readAccount"] = True
        reader = Mock()
        reader.read.return_value = {"status": "OBSERVED", "account": "AVAILABLE", "openOrders": "SYMBOL_NONE_OBSERVED",
            "orderability": "MARKET_AND_ACCOUNT_ENABLED", "fundsCheck": "WITHIN_OBSERVED_BALANCE"}
        result = self.run_preview(reader)
        self.assertEqual("0.02", result["draft"]["notional"])
        self.assertFalse(result["executable"])
        self.assertEqual("BLOCKED", next(c for c in result["checks"] if c["code"] == "LIVE_AUTHORITY")["status"])
        self.assertEqual(60, (datetime.fromisoformat(result["expiresAt"]) - datetime.fromisoformat(result["asOf"])).total_seconds())

    def test_long_decimal_input_keeps_all_significant_digits(self):
        from decimal import Decimal, localcontext
        value = "999999999999999999.999999999999999999"
        self.request["draft"] = {"quantity": value, "limitPrice": value}
        result = self.run_preview()
        with localcontext() as context:
            context.prec = 80
            self.assertEqual(format(Decimal(value) * Decimal(value), "f"), result["draft"]["notional"])

    def test_source_change_during_account_read_discards_report(self):
        self.request.update(draft={"instanceId": "trial-instance"}, readAccount=True)
        reader = Mock()
        def mutate(*args, **kwargs):
            p=self.root/"strategy.json";b=json.loads(p.read_text());b["sample_prices"][0]+=1;base.write(p,b)
            return {"account":"AVAILABLE","openOrders":"UNKNOWN","orderability":"UNKNOWN","fundsCheck":"UNKNOWN"}
        reader.read.side_effect=mutate
        with self.assertRaises(ValueError): self.run_preview(reader)

    def test_unselected_or_wrong_symbol_never_reads_account(self):
        for value in ("", "other-instance"):
            self.request.update(draft={"instanceId": value}, readAccount=True)
            reader=Mock()
            with self.assertRaises(ValueError): self.run_preview(reader)
            reader.read.assert_not_called()

    def test_arbitrary_paths_modes_extra_fields_and_invalid_amounts_rejected(self):
        for field,value in (("quantity","NaN"),("quantity","1e20"),("limitPrice","-1"),("quantity","0"),("side","AUTO")):
            self.request["draft"]={field:value}
            self.assertFalse(service.preparation_response(self.request,roots=[self.root])["ok"])
        self.request["draft"]={}
        self.request["source"]["path"]="C:/private"
        self.assertFalse(service.preparation_response(self.request,roots=[self.root])["ok"])

    def test_normal_paper_source_revalidates_exact_evidence_without_promoting_report(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);base.fixture(root)
            row=base.service.list_paper_candidates(roots=[root])["candidates"][0]
            request={"source":{"kind":"PAPER","rootKey":row["rootKey"],"evidenceId":row["evidenceId"],
                     "evidenceHash":row["identity"]["evidenceHash"],"instanceHash":row["instanceHash"]},"draft":{},"readAccount":False}
            report=service.prepare_read_only(request,roots=[root])
            self.assertEqual("SEALED_PAPER_EVIDENCE_ONLY",report["source"]["qualification"])
            self.assertFalse(report["executable"])
            request["source"]["evidenceHash"]="0"*64
            self.assertFalse(service.preparation_response(request,roots=[root])["ok"])

    def test_real_post_route_authenticates_before_body_and_calls_only_preparation(self):
        tree=ast.parse((base.APP_ROOT/"live_trader/server.py").read_text(encoding="utf-8-sig"))
        method=next(node for cls in tree.body if isinstance(cls,ast.ClassDef) for node in cls.body if isinstance(node,ast.FunctionDef) and node.name=="do_POST")
        paths_node=next(node.value for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="_FUNCTIONAL_MUTATION_PATHS" for t in node.targets))
        paths=eval(compile(ast.Expression(paths_node),"<paths>","eval"),{"frozenset":frozenset})
        run=Mock(side_effect=lambda payload: service.preparation_response(payload,roots=[self.root]))
        namespace={"urlparse":urlparse,"_FUNCTIONAL_STATUS_PATHS":set(),"_FUNCTIONAL_MUTATION_PATHS":paths,"state":types.SimpleNamespace(read_only_order_preparation=run)}
        cls=ast.ClassDef(name="Handler",bases=[],keywords=[],body=[method],decorator_list=[]);ast.fix_missing_locations(cls)
        exec(compile(ast.Module(body=[cls],type_ignores=[]),"<isolated-http>","exec"),namespace)
        handler=namespace["Handler"]();handler.path="/api/preparation/preview";handler.read_json=Mock(return_value=self.request);handler.send_json=Mock();handler._authorize_functional_http=Mock(return_value=False)
        handler.do_POST();handler.read_json.assert_not_called();run.assert_not_called()
        handler._authorize_functional_http.return_value=True;handler.do_POST()
        response=handler.send_json.call_args.args[0]
        self.assertFalse(response["executable"]);self.assertEqual(0,response["permitsCreated"])
        self.assertEqual(0,response["runtimeSessionsCreated"])

    def test_dedicated_get_route_does_not_scan_legacy_candidates_and_requires_session(self):
        tree=ast.parse((base.APP_ROOT/"live_trader/server.py").read_text(encoding="utf-8-sig"))
        method=next(node for cls in tree.body if isinstance(cls,ast.ClassDef) for node in cls.body if isinstance(node,ast.FunctionDef) and node.name=="do_GET")
        paths_node=next(node.value for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="_FUNCTIONAL_STATUS_PATHS" for t in node.targets))
        paths=eval(compile(ast.Expression(paths_node),"<paths>","eval"),{"frozenset":frozenset})
        with patch.object(service,"dedicated_preparation_roots",return_value=[self.root]), patch.object(base.service,"list_paper_candidates",side_effect=AssertionError("legacy scan forbidden")):
            run=Mock(side_effect=service.preparation_sources_response)
            namespace={"urlparse":urlparse,"_FUNCTIONAL_STATUS_PATHS":paths,"_FUNCTIONAL_MUTATION_PATHS":set(),"_FUNCTIONAL_BOOTSTRAP_PATH":"/bootstrap","state":types.SimpleNamespace(read_only_preparation_sources=run)}
            cls=ast.ClassDef(name="Handler",bases=[],keywords=[],body=[method],decorator_list=[]);ast.fix_missing_locations(cls)
            exec(compile(ast.Module(body=[cls],type_ignores=[]),"<isolated-get>","exec"),namespace)
            handler=namespace["Handler"]();handler.path="/api/preparation/sources";handler.send_json=Mock();handler._authorize_functional_http=Mock(return_value=False)
            handler.do_GET();run.assert_not_called()
            handler._authorize_functional_http.return_value=True;handler.do_GET()
            result=handler.send_json.call_args.args[0];self.assertTrue(result["ok"]);self.assertEqual(1,len(result["sources"]));self.assertFalse(result["executable"])


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.transport=account.ReadOnlyTransport()
        self.transport.opener=Mock()

    def test_all_mutation_methods_order_test_cancel_and_arbitrary_hosts_denied_before_transport(self):
        attempts=[("binance",m,p) for m in ("POST","PUT","PATCH","DELETE","GET")
                  for p in ("/api/v3/order","/api/v3/order/test","https://evil.test/api/v3/account","/api/v3/account/../order")]
        attempts += [("upbit","GET","/v1/orders"),("kis","POST","/uapi/domestic-stock/v1/trading/order-cash")]
        for provider,method,path in attempts:
            with self.assertRaises(account.ReadBoundaryError):self.transport.request(provider,method,path)
        self.transport.opener.open.assert_not_called()

    def test_get_body_and_kis_wrong_transaction_id_are_denied(self):
        with self.assertRaises(account.ReadBoundaryError): self.transport.request("upbit","GET","/v1/accounts",body={})
        with self.assertRaises(account.ReadBoundaryError): self.transport.request("kis","GET","/uapi/domestic-stock/v1/trading/inquire-balance",headers={"tr_id":"TTTC0012U"})
        self.transport.opener.open.assert_not_called()

    def test_redirect_denied_before_follow_and_after_untrusted_opener(self):
        with self.assertRaisesRegex(account.ReadFailure,"REDIRECT_BLOCKED"):
            account.NoRedirect().redirect_request(None,None,302,"",{},"https://evil.test")
        response=Mock();response.status=200;response.geturl.return_value="https://evil.test";response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        self.transport.opener.open.return_value=response
        with self.assertRaisesRegex(account.ReadFailure,"REDIRECT_BLOCKED"):self.transport.request("upbit","GET","/v1/accounts")
        response.read.assert_not_called()

    def test_only_well_formed_kis_authentication_post_allowed(self):
        for body in ({},{"grant_type":"client_credentials","appkey":"a","appsecret":"b","order":"BUY"},{"grant_type":"order","appkey":"a","appsecret":"b"}):
            with self.assertRaises(account.ReadBoundaryError):self.transport.request("kis","POST","/oauth2/tokenP",body=body)
        response=Mock();response.status=200;response.geturl.return_value=account.ORIGINS["kis"]+"/oauth2/tokenP";response.read.return_value=b'{"access_token":"fixture-only"}';response.headers={};response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        self.transport.opener.open.return_value=response
        self.transport.request("kis","POST","/oauth2/tokenP",body={"grant_type":"client_credentials","appkey":"a","appsecret":"b"})
        self.assertEqual("POST",self.transport.opener.open.call_args.args[0].method)
        self.assertNotIn("fixture-only",json.dumps(self.transport.events))

    def test_missing_credentials_and_configured_nonofficial_origin_never_send(self):
        for settings in ({},{"UPBIT_ACCESS_KEY":"a","UPBIT_SECRET_KEY":"b","UPBIT_BASE_URL":"https://evil.test"}):
            transport=Mock();reader=account.ReadOnlyBrokerReader(settings,transport=transport)
            report=reader.read("upbit","KRW-BTC")
            transport.request.assert_not_called();self.assertEqual("UNKNOWN",report["account"])
            self.assertNotEqual("OBSERVED",report["status"])

    def test_upbit_wait_and_watch_and_only_statuses_escape(self):
        transport=Mock()
        def request(provider,method,path,**kwargs):
            if path=="/v1/accounts":return ([{"currency":"KRW","balance":"99887766","uuid":"private-account"}],{})
            if path=="/v1/orders/open":
                self.assertIn(kwargs["query"]["state"], ("wait","watch"))
                return ([],{})
            return ({"market":{"state":"active"},"bid_account":{"balance":"99887766"},"ask_account":{"balance":"99"}},{})
        transport.request.side_effect=request
        reader=account.ReadOnlyBrokerReader({"UPBIT_ACCESS_KEY":"fixture-key","UPBIT_SECRET_KEY":"fixture-secret"},transport=transport)
        result=reader.read("upbit","KRW-BTC",side="BUY",quantity=account.Decimal("1"),limit_price=account.Decimal("2"))
        self.assertEqual({"wait","watch"}, {call.kwargs["query"]["state"] for call in transport.request.call_args_list if call.args[2] == "/v1/orders/open"})
        self.assertEqual("SYMBOL_NONE_OBSERVED",result["openOrders"]);self.assertEqual("WITHIN_OBSERVED_BALANCE",result["fundsCheck"])
        for secret in ("99887766","private-account","fixture-key","fixture-secret"):self.assertNotIn(secret,json.dumps(result))

    def test_binance_futures_empty_regular_orders_does_not_claim_no_algo_orders(self):
        transport=Mock()
        def request(provider,method,path,**kwargs):
            if path.endswith("/time"):return ({"serverTime":123}, {})
            if path.endswith("/account"):return ({"assets":[]}, {})
            if path.endswith("/openOrders"):return ([],{})
            return ({"symbols":[{"symbol":"BTCUSDT","status":"TRADING","quoteAsset":"USDT"}]}, {})
        transport.request.side_effect=request
        reader=account.ReadOnlyBrokerReader({"BINANCE_API_KEY":"a","BINANCE_API_SECRET":"b"},transport=transport)
        result=reader.read("binance-futures","BTCUSDT")
        self.assertEqual("REGULAR_SYMBOL_NONE_ALGO_UNKNOWN",result["openOrders"])
        self.assertEqual("UNKNOWN",result["orderability"]);self.assertEqual("UNKNOWN",result["fundsCheck"])

    def test_kis_authentication_cache_is_memory_only_and_reused(self):
        transport=Mock();transport.request.return_value=({"access_token":"fixture-bearer","expires_in":3600},{})
        reader=account.ReadOnlyBrokerReader({"KIS_APP_KEY":"test-distinct-app","KIS_APP_SECRET":"b"},transport=transport)
        with patch.object(reader,"_pace_kis"), patch.object(Path,"write_text",side_effect=AssertionError("no disk writes")),patch.object(Path,"write_bytes",side_effect=AssertionError("no disk writes")):
            self.assertEqual("fixture-bearer",reader._kis_token());self.assertEqual("fixture-bearer",reader._kis_token())
        transport.request.assert_called_once()
        account._TOKEN_CACHE.clear()


if __name__=="__main__":unittest.main()
