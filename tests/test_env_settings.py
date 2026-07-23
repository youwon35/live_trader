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


if __name__ == "__main__":
    unittest.main()
