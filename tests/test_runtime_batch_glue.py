"""Execute only extracted glue functions; no real app init, account or network."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[1] / "live_trader"


def load_function(path, name, namespace, class_name=None):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    body = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name).body if class_name else tree.body
    node = next(node for node in body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
    assert not node.decorator_list
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


def http_sets():
    tree = ast.parse((ROOT / "server.py").read_text(encoding="utf-8"))
    return {node.targets[0].id: ast.literal_eval(node.value.args[0])
            for node in tree.body if isinstance(node, ast.Assign)
            and isinstance(node.targets[0], ast.Name) and node.targets[0].id in {
                "_FUNCTIONAL_STATUS_PATHS", "_FUNCTIONAL_MUTATION_PATHS"}}


class Handler:
    def __init__(self, path, payload=None, authorized=True):
        self.path = path
        self.payload = payload
        self.authorized = authorized
        self.reads = 0
        self.result = None
        self.denied = None
        self.auth_requirements = []

    def _authorize_functional_http(self, *, require_origin):
        self.auth_requirements.append(require_origin)
        return self.authorized

    def _send_functional_http_denial(self, reason):
        self.denied = reason

    def read_json(self):
        self.reads += 1
        return self.payload

    def send_json(self, result):
        self.result = result


class RuntimeBatchGlueTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.ns = {**http_sets(), "urlparse": urlparse, "parse_qs": parse_qs,
                   "_FUNCTIONAL_BOOTSTRAP_PATH": "/__lt_native_bootstrap",
                   "state": SimpleNamespace(runtime_batch_request=lambda *args: self.calls.append(args) or {"ok": True})}
        self.post = load_function(ROOT / "server.py", "do_POST", self.ns, "LiveTraderHandler")
        self.get = load_function(ROOT / "server.py", "do_GET", self.ns, "LiveTraderHandler")

    def test_http_native_auth_precedes_body_read_for_both_mutations(self):
        for action in ("prepare", "start-member"):
            denied = Handler("/api/runtime/batches/" + action, {"confirmed": True}, authorized=False)
            self.post(denied)
            self.assertEqual(denied.reads, 0)
            self.assertEqual(denied.auth_requirements, [True])
            self.assertFalse(self.calls)

    def test_http_authenticated_exact_post_shape_routes_once(self):
        for action in ("prepare", "start-member"):
            payload = {"fixture": action}
            handler = Handler("/api/runtime/batches/" + action, payload)
            self.post(handler)
            self.assertEqual(self.calls[-1], (action, payload))
            self.assertEqual(handler.auth_requirements, [True])
        for suffix, payload in (("?unexpected=1", {}), ("", [])):
            handler = Handler("/api/runtime/batches/prepare" + suffix, payload)
            self.post(handler)
            self.assertEqual(handler.result["reasonCode"], "EXACT_BATCH_REQUEST_REQUIRED")
        self.assertEqual(len(self.calls), 2)

    def test_http_status_is_authenticated_exact_id_and_readonly(self):
        handler = Handler("/api/runtime/batches/status?batchId=batch-test")
        self.get(handler)
        self.assertEqual(self.calls, [("status", "batch-test")])
        self.assertEqual(handler.auth_requirements, [False])
        self.assertEqual(handler.reads, 0)
        for query in ("", "?batchId=a&batchId=b", "?batchId=a&other=b"):
            bad = Handler("/api/runtime/batches/status" + query)
            self.get(bad)
            self.assertEqual(bad.result["reasonCode"], "EXACT_BATCH_ID_REQUIRED")
        self.assertEqual(len(self.calls), 1)

    def test_absolute_http_target_is_denied_without_start(self):
        for method, path in ((self.post, "http://evil/api/runtime/batches/start-member"),
                             (self.get, "http://evil/api/runtime/batches/status?batchId=x")):
            handler = Handler(path, {})
            method(handler)
            self.assertIsNotNone(handler.denied)
            self.assertFalse(self.calls)

    def test_start_glue_calls_existing_decorated_admission_with_exact_kis_confirmation(self):
        calls = []
        namespace = {
            "start_continuous_runtime": lambda *args, **kwargs: calls.append((args, kwargs)) or {"ok": True},
            "public_market_gap_start_note_current": lambda preparation, selection: preparation == "fresh",
        }
        start = load_function(ROOT / "state.py", "_start_runtime_batch_member", namespace)
        selection = {"profile_id": "stock", "mode": "SMALL_LIVE", "portfolio_id": "p",
                     "deployment_id": "d", "strategy_id": "s"}
        native = {"nativeAttestation": {"opaque": "exact-native"}}
        result = start(selection, {"safety_confirmation": native, "marketGapPreparationId": "fresh"})
        self.assertTrue(result["ok"])
        self.assertEqual(calls, [(("stock", "SMALL_LIVE", "p", "d", "s"), {"safety_confirmation": native})])
        blocked = start(selection, {"safety_confirmation": native, "marketGapPreparationId": "old"})
        self.assertFalse(blocked["ok"])
        self.assertEqual(len(calls), 1)

    def test_local_manifest_and_scope_bound_without_invoking_start(self):
        calls = []
        source = {"strategy_id": "s"}
        scope = {"runtimeScopeId": "r"}
        namespace = {
            "resolve_continuous_runtime_scope": lambda *args: calls.append(args) or scope,
            "_operational_strategy": lambda *args, **kwargs: source,
            "strategy_rows": lambda rows: [],
            "portfolio_rows": lambda: [],
            "_operational_manifest_inputs": lambda row: {"sourceHash": "a" * 64},
            "governance_sha256": lambda row: "manifest-hash",
        }
        resolve = load_function(ROOT / "state.py", "_resolve_runtime_batch_member", namespace)
        selection = {"profile_id": "crypto", "mode": "MONITOR", "portfolio_id": "p",
                     "deployment_id": "d", "strategy_id": "s"}
        self.assertEqual(resolve(selection), {"scope": scope, "manifestInputsHash": "manifest-hash"})
        self.assertEqual(calls, [("crypto", "MONITOR", "p", "s", "d")])

if __name__ == "__main__":
    unittest.main()
