# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Replacement parsing contracts shared by the command-line tools."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kiwi_scan.yaml_loader import (
    get_env_replacements,
    parse_replacements,
    yaml_loader,
)


class TestParseReplacements(unittest.TestCase):
    def test_default_preserves_legacy_handling_of_malformed_entries(self):
        self.assertEqual(
            parse_replacements(["IOC", "PV=signal=raw", "ignored"]),
            {"PV": "signal=raw"},
        )

    def test_strict_rejects_entries_missing_equals(self):
        for entry in ("IOC", "", "IOC:value"):
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError) as caught:
                    parse_replacements(["PV=valid", entry], strict=True)
                self.assertIn(repr(entry), str(caught.exception))
                self.assertIn("KEY=VALUE", str(caught.exception))

    def test_values_keep_equals_empty_values_and_last_duplicate(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                self.assertEqual(
                    parse_replacements(
                        ["IOC=old", "PV=signal=raw", "EMPTY=", "IOC=new"],
                        strict=strict,
                    ),
                    {"IOC": "new", "PV": "signal=raw", "EMPTY": ""},
                )

    def test_empty_replacements_are_valid(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                self.assertEqual(parse_replacements([], strict=strict), {})

    def test_environment_replacements_keep_command_precedence(self):
        environment = {
            "KIWI_SCAN_REPLACE_IOC": "environment-ioc",
            "KIWI_SCAN_REPLACE_EMPTY": "",
            "OTHER_REPLACE_IOC": "other-ioc",
            "KIWI_SCAN_DATA_DIR": "/data",
        }
        replacements = parse_replacements(
            ["IOC=cli-ioc", "PV=signal=raw"], strict=True
        )
        with patch.dict(os.environ, environment, clear=True):
            replacements.update(get_env_replacements("KIWI_SCAN"))
        self.assertEqual(
            replacements,
            {"IOC": "environment-ioc", "PV": "signal=raw", "EMPTY": ""},
        )


class TestYamlLoader(unittest.TestCase):
    def test_valid_yaml_expands_replacements_and_keeps_unknown_placeholders(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scan.yaml"
            path.write_text(
                "pv: '${IOC}:${PV}'\nunknown: '${UNSET}'\n", encoding="utf-8"
            )
            data = yaml_loader(
                str(path),
                parse_replacements(["IOC=TEST", "PV=signal=raw"], strict=True),
            )
        self.assertEqual(data, {"pv": "TEST:signal=raw", "unknown": "${UNSET}"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
