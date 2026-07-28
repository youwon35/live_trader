from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from live_trader import env_loader, env_settings


class EnvSettingsSafetyTests(unittest.TestCase):
    def test_packaged_env_path_uses_persistent_local_app_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {"LOCALAPPDATA": tmp, "LIVE_TRADER_DATA_DIR": ""},
            clear=False,
        ), patch.object(env_loader.sys, "frozen", True, create=True):
            self.assertEqual(env_loader.default_env_path(), Path(tmp) / "live_trader" / ".env")

    def test_packaged_runtime_data_uses_persistent_local_app_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {"LOCALAPPDATA": tmp, "LIVE_TRADER_DATA_DIR": ""},
            clear=False,
        ), patch.object(env_loader.sys, "frozen", True, create=True):
            self.assertEqual(env_loader.default_runtime_data_root(), Path(tmp) / "live_trader")

    def test_explicit_env_path_override_is_respected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {"LIVE_TRADER_ENV_PATH": str(Path(tmp) / "operator.env")},
            clear=False,
        ):
            self.assertEqual(env_loader.default_env_path(), Path(tmp) / "operator.env")

    def test_kis_account_and_hts_identifiers_are_masked(self) -> None:
        with patch.object(
            env_settings,
            "read_env_file",
            return_value={"KIS_ACCOUNT_NO": "12345678", "KIS_HTS_ID": "operator-login"},
        ):
            snapshot = env_settings.env_settings_snapshot()

        fields = {field["key"]: field for field in snapshot["fields"]}
        for key in ("KIS_ACCOUNT_NO", "KIS_HTS_ID"):
            self.assertEqual(fields[key]["kind"], "secret")
            self.assertEqual(fields[key]["value"], "")
            self.assertTrue(fields[key]["configured"])
            self.assertTrue(fields[key]["masked"])

    def test_kis_account_help_distinguishes_account_number_from_login_id(self) -> None:
        fields = {field.key: field for field in env_settings.ENV_SETTING_FIELDS}
        self.assertIn("로그인 ID가 아니라", fields["KIS_ACCOUNT_NO"].detail)
        self.assertIn("로그인/HTS ID", fields["KIS_HTS_ID"].detail)

    def test_plaintext_env_secrets_are_migrated_and_hydrated_from_protected_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_path = root / "live.env"
            secret_path = root / "secrets.json"
            env_path.write_text(
                "BINANCE_API_KEY=test-api-key\n"
                "BINANCE_API_SECRET=test-super-secret\n"
                "BINANCE_BASE_URL=https://example.test\n",
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "LIVE_TRADER_SECRET_STORE_PATH": str(secret_path),
                    "BINANCE_API_KEY": "",
                    "BINANCE_API_SECRET": "",
                },
                clear=False,
            ):
                env_loader.load_local_env(env_path)
                self.assertEqual("test-api-key", os.environ["BINANCE_API_KEY"])
                self.assertEqual("test-super-secret", os.environ["BINANCE_API_SECRET"])
                sanitized = env_path.read_text(encoding="utf-8")
                self.assertNotIn("test-api-key", sanitized)
                self.assertNotIn("test-super-secret", sanitized)
                self.assertNotIn("test-super-secret", secret_path.read_text(encoding="utf-8"))

                os.environ.pop("BINANCE_API_KEY", None)
                os.environ.pop("BINANCE_API_SECRET", None)
                env_loader.load_local_env(env_path)
                self.assertEqual("test-api-key", os.environ["BINANCE_API_KEY"])
                self.assertEqual("test-super-secret", os.environ["BINANCE_API_SECRET"])

    def test_settings_writer_never_persists_secret_fields_in_plaintext(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_path = root / "live.env"
            secret_path = root / "secrets.json"
            with patch.dict(
                os.environ,
                {"LIVE_TRADER_SECRET_STORE_PATH": str(secret_path)},
                clear=False,
            ), patch.object(env_settings, "ENV_PATH", env_path):
                env_settings.save_env_settings(
                    {
                        "KIS_APP_KEY": "protected-kis-key",
                        "KIS_APP_SECRET": "protected-kis-secret",
                        "KIS_ACCOUNT_PRODUCT_CODE": "01",
                    }
                )

            raw_env = env_path.read_text(encoding="utf-8")
            raw_store = secret_path.read_text(encoding="utf-8")
            self.assertNotIn("protected-kis-key", raw_env)
            self.assertNotIn("protected-kis-secret", raw_env)
            self.assertNotIn("protected-kis-secret", raw_store)


if __name__ == "__main__":
    unittest.main()
