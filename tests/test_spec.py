# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from kiwi_scan.export.model import ExportBundle, ExportScan
from kiwi_scan.export.spec import SpecWriter
from kiwi_scan.metadata_loader import MetadataFile


class TestSpecWriterMetadata(unittest.TestCase):
    @staticmethod
    def _bundle():
        recv_ts = pd.Timestamp("2026-07-11T11:12:02.870310+02:00")
        pv_ts = pd.Timestamp("2026-07-11T10:55:01.781726+02:00")
        raw = pd.DataFrame(
            {
                "TS-ISO8601": [recv_ts],
                "PV": ["TESTU171PGM1:cff"],
                "VALUE": [2.25],
                "PV-TS-ISO8601": [pv_ts],
                "SEVR": [None],
                "STAT": [None],
            }
        )
        metadata = MetadataFile(
            path="scan_metadata.txt",
            constants={},
            df_raw=raw,
            df_pivot=raw.pivot(
                index="TS-ISO8601",
                columns="PV",
                values="VALUE",
            ),
        )
        scan = ExportScan(
            scan_id="scan_1",
            scan_type="linear",
            data_file=None,
            metadata_file=Path("scan_metadata.txt"),
            data=pd.DataFrame({"Position": [1.0], "signal": [2.0]}),
            metadata=metadata,
        )
        return ExportBundle(scans=[scan])

    def _write(self, writer):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "out.spec"
            writer.write(self._bundle(), output)
            return output.read_text(encoding="utf-8")

    def test_monitor_rows_are_omitted_by_default(self):
        text = self._write(SpecWriter())

        self.assertNotIn("#C metadata_monitor ", text)

    def test_writes_monitor_rows_when_explicitly_enabled(self):
        text = self._write(SpecWriter(include_metadata_monitor=True))

        self.assertIn(
            "#C metadata_monitor "
            "TS-ISO8601=2026-07-11T11:12:02.870310+02:00 "
            "PV=TESTU171PGM1:cff VALUE=2.25 "
            "PV-TS-ISO8601=2026-07-11T10:55:01.781726+02:00",
            text,
        )
        self.assertNotIn("metadata_constant", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
