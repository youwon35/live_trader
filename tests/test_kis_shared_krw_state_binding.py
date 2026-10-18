"""Execute the two narrow state bindings without importing the running app.

Only actual state function ASTs are compiled. Their provider, store, preflight
and authority dependencies are controlled synthetic functions; no state module,
account transport, database, process lease, UI or grant is initialized.
"""
from copy import deepcopy
import ast
import importlib
import json
from pathlib import Path
import sys
import types
from typing import Any
import unittest
from unittest.mock import patch

from trading_runtime.artifact_governance import stable_sha256

APP = Path(__file__).resolve().parents[1]
PACKAGE = '_kis_shared_krw_state_binding_fixture'
package = types.ModuleType(PACKAGE)
package.__path__ = [str(APP / 'live_trader')]
sys.modules[PACKAGE] = package
profiles = importlib.import_module(PACKAGE + '.account_profiles')
STATE_PATH = APP / 'live_trader' / 'state.py'
STATE_AST = ast.parse(STATE_PATH.read_text(encoding='utf-8-sig'))
FUNCTION_NAMES = {
    '_normalized_execution_permission_digest', '_current_live_deployment_binding',
    '_kis_scoped_start_material',
}
FUNCTIONS = [node for node in STATE_AST.body
             if isinstance(node, ast.FunctionDef) and node.name in FUNCTION_NAMES]
if {node.name for node in FUNCTIONS} != FUNCTION_NAMES:
    raise AssertionError('Exact production state binding functions are missing')

SETTINGS = {
    'KIS_APP_KEY': 'SYNTHETIC_APP_KEY', 'KIS_APP_SECRET': 'SYNTHETIC_APP_SECRET',
    'KIS_ACCOUNT_NO': '12345678', 'KIS_ACCOUNT_PRODUCT_CODE': '01',
    'KIS_BASE_URL': 'https://openapi.koreainvestment.com:9443',
}
MODE_KEY = 'KIS_OVERSEAS_FUNDING_MODE'


def reviewed_input(account, amount='10000'):
    policy = {
        'schemaVersion': 'kis-shared-krw-cap-policy-v1',
        'accountFingerprint': account['accountFingerprint'],
        'accountProfileId': account['profileId'],
        'credentialConfigurationHash': account['credentialConfigurationHash'],
        'capKrw': '1000000', 'reviewBindingHash': 'a' * 64,
    }
    policy['policyHash'] = stable_sha256(policy)
    value = {'schemaVersion': 'kis-shared-krw-reviewed-allocation-input-v1',
             'policy': policy, 'allocationKrw': amount}
    return {**value, 'inputHash': stable_sha256(value)}


class StateFixture:
    def __init__(self, *, mode='FOREIGN_CASH', broker='kis', permissions=None):
        self.settings = dict(SETTINGS)
        if mode is not None:
            self.settings[MODE_KEY] = mode
        self.account = next(profile for profile in profiles.saved_account_profiles(self.settings)
                            if profile['brokerId'] == 'kis')
        self.settings_seen = []
        self.preflight = types.SimpleNamespace(snapshot_id='synthetic-preflight')
        self.strategy = {
            'strategy_id': 'synthetic-strategy', 'deployment_id': 'synthetic-deployment',
            'deployment_source': 'deployment-registry',
            'artifact_source_path': '/synthetic-only/strategy.json',
            'symbol': '005930', 'broker': broker,
        }
        self.deployment = {
            'deploymentId': 'synthetic-deployment', 'revision': 3,
            'lifecycle': 'before-live-small', 'environment': 'SMALL_LIVE',
            'mode': 'SMALL_LIVE', 'permissions': deepcopy(permissions or {}),
            'strategyArtifact': {'artifactId': 'synthetic-strategy',
                                 'artifactHash': 'b' * 64, 'contentHash': 'c' * 64},
            'portfolioArtifact': {'artifactId': 'synthetic-portfolio',
                                  'artifactHash': 'd' * 64, 'contentHash': 'e' * 64},
        }
        outer = self
        class FakeDeploymentStore:
            def __init__(self, _path):
                pass
            def get(self, deployment_id):
                if deployment_id != 'synthetic-deployment':
                    raise AssertionError('Unexpected synthetic deployment')
                return deepcopy(outer.deployment)
        self.namespace = {
            '__package__': PACKAGE, 'Any': Any, 'Path': Path, 'json': json,
            'governance_sha256': stable_sha256, 'DeploymentStore': FakeDeploymentStore,
            'safe_float': lambda value, default=0: float(default if value is None else value),
            'normalize_lifecycle_status': lambda value: str(value).lower(),
            '_operational_strategy': lambda _rows, **_kwargs: self.strategy,
            'strategy_rows': lambda _portfolios: [self.strategy], 'portfolio_rows': lambda: [],
            'strategy_broker_id': lambda strategy: strategy['broker'],
            'resolve_continuous_runtime_scope': lambda *_args: {
                'brokerId': 'kis', 'accountFingerprint': self.account['accountFingerprint'],
                'credentialConfigurationHash': self.account['credentialConfigurationHash'],
                'runtimeScopeId': 'synthetic-runtime', 'capitalCurrency': 'KRW'},
            'env_value': lambda key: self.settings.get(key, ''),
            '_kis_cleanup_store': lambda: types.SimpleNamespace(pending_for_account=lambda _account: False),
            '_operational_manifest_inputs': lambda _strategy: self.manifest_inputs(),
            '_live_deployment_binding_is_executable': lambda _binding, **_kwargs: True,
            'OPERATIONAL_GOVERNANCE': types.SimpleNamespace(
                get_deployment_manifest=lambda _deployment: types.SimpleNamespace(),
                latest_preflight_for_deployment=lambda _deployment: self.preflight,
                preflight_validity=lambda _snapshot: {'valid': True}),
            '_operational_manifest_matches_inputs': lambda _manifest, _inputs: True,
            '_reconciliation_summary_for_broker_snapshot': lambda *_args: {
                'status': 'pass', 'error_count': 0, 'blocking_count': 0},
            'reconciliation_snapshot': lambda: {},
            '_kis_environment_order_authority_binding': lambda: ('f' * 64, '1' * 64),
            '_kis_application_owner_epoch': lambda: ('synthetic-owner', '2' * 64, True),
            'load_strategy_artifacts': lambda: [{'artifactHash': 'b' * 64, 'symbol': '005930'}],
            '_operational_artifact_hash': lambda artifact: artifact['artifactHash'],
        }
        # Execute the previous two-field operational policy contract as well,
        # changing only the newly owned dictionary unpack in a copied AST.
        legacy_binding = deepcopy(next(node for node in FUNCTIONS
            if node.name == '_current_live_deployment_binding'))
        legacy_binding.name = '_legacy_live_deployment_binding'
        changed = 0
        for node in ast.walk(legacy_binding):
            if isinstance(node, ast.Dict) and {
                    key.value for key in node.keys if isinstance(key, ast.Constant)
                    }.issuperset({'kisEquityExecutionPolicies', 'kisEquityFeeReservePolicy'}):
                old = [(key, value) for key, value in zip(node.keys, node.values) if key is not None]
                node.keys, node.values = [key for key, _ in old], [value for _, value in old]
                changed += 1
        if changed != 1:
            raise AssertionError('The narrow operational policy digest fixture changed')
        compiled = ast.Module(body=deepcopy(FUNCTIONS) + [legacy_binding], type_ignores=[])
        exec(compile(compiled, '<actual-state-binding-functions>', 'exec'), self.namespace)

    def binding(self):
        return self.namespace['_current_live_deployment_binding'](self.strategy, require_store=True)

    def manifest_inputs(self):
        return {'metadata': {'deploymentBinding': self.binding(),
            'paperFinalBindings': [{'ready': True}], 'portfolioId': 'synthetic-portfolio',
            'strategyMembers': [{'artifactHash': 'b' * 64, 'symbol': '005930'}]}}

    def start_context(self):
        def saved(settings):
            self.settings_seen.append(dict(settings))
            return profiles.saved_account_profiles(settings)
        stubs = {}
        for name, members in {
            'kis_scoped_operational_authority': {'exact_context': lambda value: dict(value)},
            'kis_scoped_cleanup_authority': {'CLEANUP_POLICY': 'SYNTHETIC_FIXED_CLEANUP_POLICY'},
            'native_spot_quantity': {'sizing_from_artifact': lambda _artifact: {
                'quantityContract': {'schemaVersion': 'native-equity-quantity-v1',
                                     'quoteCurrency': 'KRW', 'exchange': 'KRX'}}},
            'process_safety': {'held_process_lease_status': lambda _scope: {'acquired': False}},
            'account_profiles': {'saved_account_profiles': saved},
        }.items():
            module = types.ModuleType(PACKAGE + '.' + name)
            for key, value in members.items():
                setattr(module, key, value)
            stubs[module.__name__] = module
        with patch.dict(sys.modules, stubs):
            return self.namespace['_kis_scoped_start_material'](
                'stock', 'SMALL_LIVE', 'synthetic-portfolio', 'synthetic-deployment', 'synthetic-strategy')


class SharedKrwStateBindingTests(unittest.TestCase):
    def test_explicit_foreign_cash_reaches_actual_wire_profile_recalculation_unchanged(self):
        fixture = StateFixture(mode='FOREIGN_CASH')
        context, preflight = fixture.start_context()
        self.assertEqual(fixture.settings_seen[-1][MODE_KEY], 'FOREIGN_CASH')
        self.assertEqual(context['credentialConfigurationHash'], fixture.account['credentialConfigurationHash'])
        self.assertIs(preflight, fixture.preflight)

    def test_missing_and_blank_public_mode_retain_requested_integrated_default(self):
        for mode in (None, ''):
            fixture = StateFixture(mode=mode)
            context, _preflight = fixture.start_context()
            passed = fixture.settings_seen[-1]
            self.assertIn(MODE_KEY, passed)
            self.assertEqual(passed[MODE_KEY], '')
            account = profiles.saved_account_profiles(passed)[0]
            self.assertEqual(account['overseasFunding']['fundingMode'], 'INTEGRATED_MARGIN')
            self.assertEqual(context['credentialConfigurationHash'], account['credentialConfigurationHash'])

    def test_wire_mode_change_cannot_match_a_previously_bound_cash_only_account(self):
        fixture = StateFixture(mode='FOREIGN_CASH')
        fixture.settings[MODE_KEY] = 'INTEGRATED_MARGIN'
        with self.assertRaisesRegex(ValueError, 'saved-profile-differs-from-actual-wire-account'):
            fixture.start_context()

    def test_reviewed_reservation_change_alters_actual_operational_binding_and_context_cas_hash(self):
        fixture = StateFixture()
        first_plan = reviewed_input(fixture.account, '10000')
        fixture.deployment['permissions']['kisSharedKrwReservationPlan'] = first_plan
        first_binding = fixture.binding(); first_context, _ = fixture.start_context()
        fixture.deployment['permissions']['kisSharedKrwReservationPlan'] = reviewed_input(fixture.account, '20000')
        second_binding = fixture.binding(); second_context, _ = fixture.start_context()
        self.assertNotEqual(first_binding['operationalExecutionPolicyDigest'], second_binding['operationalExecutionPolicyDigest'])
        self.assertNotEqual(first_binding['bindingHash'], second_binding['bindingHash'])
        self.assertNotEqual(first_context['scopeHash'], second_context['scopeHash'])
        self.assertEqual(first_context['accountFingerprint'], second_context['accountFingerprint'])

    def test_missing_plan_and_non_kis_legacy_policy_digest_preserve_previous_contract(self):
        for broker in ('kis', 'binance', 'upbit'):
            for permissions in ({}, {'kisSharedKrwReservationPlan': None}):
                fixture = StateFixture(broker=broker, permissions=permissions)
                binding = fixture.binding()
                expected = stable_sha256({'kisEquityExecutionPolicies': None, 'kisEquityFeeReservePolicy': None})
                self.assertEqual(binding['operationalExecutionPolicyDigest'], expected)
                previous = fixture.namespace['_legacy_live_deployment_binding'](fixture.strategy, require_store=True)
                self.assertEqual(binding, previous)
        permissions = {'kisEquityExecutionPolicies': {'synthetic-artifact': {'contentHash': '3' * 64}},
                       'kisEquityFeeReservePolicy': {'contentHash': '4' * 64}}
        fixture = StateFixture(permissions=permissions)
        self.assertEqual(fixture.binding()['operationalExecutionPolicyDigest'], stable_sha256(permissions))
        self.assertEqual(fixture.binding(), fixture.namespace['_legacy_live_deployment_binding'](
            fixture.strategy, require_store=True))

    def test_persisted_permissions_not_a_stale_strategy_copy_define_actual_digest(self):
        fixture = StateFixture()
        fixture.strategy['permissions'] = {'kisSharedKrwReservationPlan': reviewed_input(fixture.account, '90000')}
        persisted = reviewed_input(fixture.account, '10000')
        fixture.deployment['permissions']['kisSharedKrwReservationPlan'] = persisted
        self.assertEqual(fixture.binding()['operationalExecutionPolicyDigest'], stable_sha256({
            'kisEquityExecutionPolicies': None, 'kisEquityFeeReservePolicy': None,
            'kisSharedKrwReservationPlan': persisted}))

    def test_running_app_state_and_order_modules_are_never_imported(self):
        for name in ('state', 'continuous_live', 'kis_scoped_operational_authority',
                     'kis_scoped_cleanup_authority', 'process_safety'):
            self.assertNotIn(PACKAGE + '.' + name, sys.modules)


if __name__ == '__main__':
    unittest.main()
