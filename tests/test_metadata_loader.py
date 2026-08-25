# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import tempfile
import unittest
from pathlib import Path

from kiwi_scan.data.metadata_loader import parse_metadata_file


class TestMetadataLoader(unittest.TestCase):
    def test_parse_file_without_metadata_constants(self):
        content = (
            "TS-ISO8601\tPV\tVALUE\tPV-TS-ISO8601\tSEVR\tSTAT\n"
            "2026-07-11T11:12:02.870310+02:00\tTEST:PV\t2.25\t"
            "2026-07-11T10:55:01.781726+02:00\t\t\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "scan_metadata.txt"
            path.write_text(content, encoding="utf-8")

            result = parse_metadata_file(str(path))

        self.assertIsNotNone(result)
        self.assertEqual(result.constants, {})
        self.assertEqual(len(result.df_raw), 1)
        self.assertEqual(result.df_raw.iloc[0]["PV"], "TEST:PV")
        self.assertAlmostEqual(result.df_raw.iloc[0]["VALUE"], 2.25)
        self.assertIn("TEST:PV", result.df_pivot.columns)

    def test_parse_file_with_metadata_constants_and_separator(self):
        content = (
            "# metadata_constants\n"
            "# beamline\ttest\n"
            "# --- metadata above; monitor data below ---\n"
            "TS-ISO8601\tPV\tVALUE\tPV-TS-ISO8601\tSEVR\tSTAT\n"
            "2026-07-11T11:12:02.870310+02:00\tTEST:PV\t2.25\t"
            "2026-07-11T10:55:01.781726+02:00\t0\t0\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "scan_metadata.txt"
            path.write_text(content, encoding="utf-8")

            result = parse_metadata_file(str(path))

        self.assertIsNotNone(result)
        self.assertEqual(result.constants, {"beamline": "test"})
        self.assertEqual(len(result.df_raw), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
