from __future__ import annotations

import unittest
from unittest.mock import patch

from live_trader import env_settings


class EnvSettingsSafetyTests(unittest.TestCase):
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
