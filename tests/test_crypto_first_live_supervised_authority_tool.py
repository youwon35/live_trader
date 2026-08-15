from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "crypto_first_live_supervised_git_authority.py"
SPEC = importlib.util.spec_from_file_location(
    "crypto_first_live_supervised_git_authority_tool", MODULE_PATH
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("supervised authority tool is not importable")
tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tool)


class SupervisedAuthorityToolTests(unittest.TestCase):
    def test_only_external_https_ssh_git_remotes_are_accepted(self) -> None:
        self.assertTrue(tool._external_remote_url("https://example.com/a.git"))
        self.assertTrue(tool._external_remote_url("git@example.com:a.git"))
        self.assertFalse(tool._external_remote_url(r"D:\anchor.git"))
        self.assertFalse(tool._external_remote_url("file:///D:/anchor.git"))
        self.assertFalse(tool._external_remote_url("ssh://localhost/a.git"))

    def test_exact_config_rejects_authority_trader_sid_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "authority-repo"
            trader = root / "trader-data"
            secrets = root / "secrets"
            repo.mkdir()
            trader.mkdir()
            secrets.mkdir()
            private_key = secrets / "private.pem"
            pipe_key = secrets / "pipe.key"
            private_key.write_bytes(b"private-placeholder")
            pipe_key.write_bytes(b"x" * 32)
            config = {
                "schemaVersion": tool.CONFIG_SCHEMA,
                "authorityId": "supervised-authority-0001",
                "namespaceId": "supervised-namespace-0001",
                "keyId": "supervised-key-0001",
                "authorityOsSid": "S-1-5-21-1-2-3-1001",
                "traderOsSid": "S-1-5-21-1-2-3-1001",
                "authorityRepoPath": str(repo.resolve()),
                "traderDataRoot": str(trader.resolve()),
                "privateKeyPath": str(private_key.resolve()),
                "pipeAuthKeyPath": str(pipe_key.resolve()),
                "pipeAddress": r"\\.\pipe\crypto-first-live-supervised",
                "remoteName": "origin",
                "remoteRef": "refs/heads/crypto-first-live-anchor",
                "remoteUrlSha256": "a" * 64,
                "statePath": "audit/state.json",
            }
            path = root / "authority.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(
                tool.SupervisedGitAuthorityToolError, "os-sid-invalid"
            ):
                tool.load_config(path)

    def test_append_uses_non_force_exact_ref_and_returns_verified_head(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = object.__new__(tool.GitFastForwardRemoteStore)
            store.repo = Path(temporary)
            store.remote_name = "origin"
            store.remote_ref = "refs/heads/crypto-first-live-anchor"
            store.state_path = "audit/state.json"
            store._observed_head = "a" * 40
            calls: list[tuple[str, ...]] = []

            def fake_git(*args, input_text=None, check=True):
                del input_text, check
                calls.append(tuple(args))
                if args[:2] == ("status", "--porcelain"):
                    return ""
                if args[:2] == ("rev-parse", "HEAD"):
                    return "b" * 40 + "\n"
                if args[:2] == ("ls-remote", "origin"):
                    return "b" * 40 + "\t" + store.remote_ref + "\n"
                if args[:3] == ("ls-tree", "-r", "--full-tree"):
                    if args[3] == "b" * 40:
                        return (
                            "100644 blob "
                            + "c" * 40
                            + "\t"
                            + store.state_path
                            + "\n"
                        )
                    return ""
                return ""

            store._git = fake_git
            store._fetch_head = lambda: "a" * 40
            result = store.fast_forward_append(
                {"schemaVersion": "test-state/v1", "sequence": 1}
            )
            self.assertEqual("b" * 40, result)
            flattened = [item for call in calls for item in call]
            self.assertNotIn("--force", flattened)
            self.assertIn(
                ("push", "--porcelain", "origin", "HEAD:" + store.remote_ref),
                calls,
            )

    def test_remote_tree_rejects_extra_content_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = object.__new__(tool.GitFastForwardRemoteStore)
            store.repo = Path(temporary)
            store.remote_name = "origin"
            store.remote_ref = "refs/heads/crypto-first-live-anchor"
            store.state_path = "audit/state.json"
            store._observed_head = ""
            store._fetch_head = lambda: "a" * 40

            def fake_git(*args, input_text=None, check=True):
                del input_text, check
                if args[:3] == ("ls-tree", "-r", "--full-tree"):
                    return (
                        "100644 blob "
                        + "b" * 40
                        + "\taudit/state.json\n"
                        + "100644 blob "
                        + "c" * 40
                        + "\t.github/workflows/hostile.yml\n"
                    )
                return ""

            store._git = fake_git
            with self.assertRaisesRegex(
                tool.SupervisedGitAuthorityToolError,
                "remote-tree-invalid",
            ):
                store.read_head()

    def test_command_parser_rejects_extra_fields(self) -> None:
        command = {
            "schemaVersion": (
                "crypto-first-live-supervised-authority-command/v1"
            ),
            "requestId": "supervised-command-0001",
            "request": {},
            "extra": True,
        }
        with self.assertRaisesRegex(
            tool.SupervisedGitAuthorityToolError, "command-invalid"
        ):
            tool._parse_command(json.dumps(command).encode("utf-8"))


if __name__ == "__main__":
    unittest.main()
