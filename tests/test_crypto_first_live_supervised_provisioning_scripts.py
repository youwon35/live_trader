from __future__ import annotations

from pathlib import Path
import json
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
PROVISION = (
    ROOT
    / "scripts"
    / "provision_crypto_first_live_supervised_git_authority.ps1"
)
UNINSTALL = (
    ROOT
    / "scripts"
    / "uninstall_crypto_first_live_supervised_git_authority.ps1"
)
BROKER_TEMPLATE = (
    ROOT
    / "scripts"
    / "crypto_first_live_supervised_broker_bundle.template.json"
)


class SupervisedAuthorityProvisioningScriptTests(unittest.TestCase):
    def test_plan_gate_precedes_every_mutating_boundary(self) -> None:
        source = PROVISION.read_text(encoding="utf-8")
        gate = source.index("if (-not $Apply)")
        for boundary in (
            "Set-ProtectedDirectoryAcl $AuthorityRoot",
            'Invoke-GitHubApi "POST"',
            "Register-SystemTask $ServeTaskName",
            'Invoke-TransientSystemMode $launcherPath "Provision"',
        ):
            self.assertGreater(source.index(boundary), gate)
        self.assertIn(
            "I ACCEPT SUPERVISED GIT IS NOT FORMAL WORM", source
        )
        self.assertIn("mutationPerformed = $false", source)
        self.assertIn("orderAllowed = $false", source)
        self.assertIn("$ProtectedBundleProvisioningApplyReleased = $false", source)
        self.assertIn("$BrokerNetworkReleaseAllowed = $false", source)
        self.assertIn("protected-bundle-provisioning-release-held", source)

    def test_system_bundle_is_hash_sealed_and_personal_keys_are_forbidden(self) -> None:
        source = PROVISION.read_text(encoding="utf-8")
        self.assertIn('$SystemSid = "S-1-5-18"', source)
        self.assertIn('-UserId "SYSTEM"', source)
        self.assertIn('taskAutoStart = $false', source)
        self.assertNotIn("New-ScheduledTaskTrigger", source)
        self.assertIn("bundle-manifest.json", source)
        self.assertIn("manifest-extra-file", source)
        self.assertIn("ExpectedAuthorityToolSha256", source)
        self.assertIn("ExpectedAnchorModuleSha256", source)
        self.assertIn("ExpectedCredentialRewrapToolSha256", source)
        self.assertIn("code-origin-cannot-be-supervised-authority-remote", source)
        self.assertIn("independent-github-administrator-required", source)
        self.assertIn('"credential.helper", ""', source)
        self.assertIn("github-deploy-ed25519", source)
        self.assertIn("machineProtectedCredentials", source)
        self.assertIn("pinnedFiles", source)

    def test_remote_ref_rules_separate_writer_from_integrity(self) -> None:
        source = PROVISION.read_text(encoding="utf-8")
        writer = source.index("crypto-first-live-anchor-writer")
        integrity = source.index("crypto-first-live-anchor-integrity")
        block_other = source.index("crypto-first-live-block-other-branches")
        block_tags = source.index("crypto-first-live-block-all-tags")
        self.assertLess(writer, integrity)
        self.assertLess(integrity, block_other)
        self.assertLess(block_other, block_tags)
        self.assertRegex(source, r'type = "non_fast_forward"')
        self.assertIn('actor_type = "DeployKey"', source)
        self.assertIn('requiresGitHubActionsDisabled = $true', source)
        self.assertIn('/actions/permissions', source)
        self.assertIn('enabled = $false', source)
        self.assertIn('githubActionsEnabled = $false', source)
        self.assertIn('bypass_mode = "always"', source)
        self.assertIn("private-unarchived-dedicated-github-repository-required", source)

    def test_rollback_revokes_capability_but_never_deletes_anchor(self) -> None:
        source = UNINSTALL.read_text(encoding="utf-8")
        self.assertIn(
            "REVOKE SUPERVISED AUTHORITY WITHOUT DELETING REMOTE ANCHOR",
            source,
        )
        self.assertIn("deployKeyRevoked = $true", source)
        self.assertIn("remoteAnchorRefPreserved = $true", source)
        self.assertIn("remoteRulesetsPreserved = $true", source)
        self.assertNotIn("git push", source.lower())
        self.assertNotRegex(source, re.compile(r"Remove-Item[^\n]+-Recurse", re.I))
        self.assertIn('/keys/$($receipt.deployKeyId)', source)

    def test_broker_bundle_contract_is_exact_and_system_prearmed(self) -> None:
        source = PROVISION.read_text(encoding="utf-8")
        template = json.loads(BROKER_TEMPLATE.read_text(encoding="utf-8"))
        self.assertEqual(
            {
                "schemaVersion",
                "readyForApply",
                "pythonWheels",
                "modes",
            },
            set(template),
        )
        self.assertFalse(template["readyForApply"])
        self.assertEqual(
            {"UPBIT_AUTHORITY", "BINANCE_OBSERVER"},
            {mode["mode"] for mode in template["modes"]},
        )
        for mode in template["modes"]:
            self.assertEqual(
                {
                    "mode",
                    "pipeAddress",
                    "entryPointDestinationRelativePath",
                    "importRootDestinationRelativePath",
                    "arguments",
                    "environment",
                    "files",
                },
                set(mode),
            )
        self.assertIn("ExpectedBrokerBundleDescriptorSha256", source)
        self.assertIn("protected-broker-bundle-copy", source)
        self.assertIn("manifest-broker-mode-invalid", source)
        self.assertIn('Register-SystemTask $mode.taskName', source)
        self.assertNotIn("New-ScheduledTaskTrigger", source)
        uninstall = UNINSTALL.read_text(encoding="utf-8")
        self.assertIn("CryptoFirstLive-UpbitAuthority", uninstall)
        self.assertIn("CryptoFirstLive-BinanceObserver", uninstall)


if __name__ == "__main__":
    unittest.main()
