# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import unittest
from pathlib import Path
from unittest.mock import Mock, patch, sentinel

import numpy as np
import pandas as pd

from kiwi_scan.ioc.pva_table import (
    DataFrameToNTTableConverter,
    PvaKiwiDataAdapter,
    ScanReader,
    serve,
)


class TestScanReader(unittest.TestCase):
    @patch("kiwi_scan.ioc.pva_table.ManifestResolver")
    def test_init_passes_data_directory_to_resolver(self, resolver_class):
        reader = ScanReader("/tmp/scan-data")

        resolver_class.assert_called_once_with("/tmp/scan-data")
        self.assertIs(reader.resolver, resolver_class.return_value)

    @patch("kiwi_scan.ioc.pva_table.DataLoader")
    @patch("kiwi_scan.ioc.pva_table.ManifestResolver")
    def test_read_table_loads_selected_scan_and_records_source_file(
        self,
        resolver_class,
        loader_class,
    ):
        scan_path = Path("/data/scans/scan-004.dat")
        resolver_class.return_value.select_file.return_value = scan_path
        loaded_table = pd.DataFrame({"position": [1.0, 2.0]})
        loader_class.return_value.load_data.return_value = loaded_table
        reader = ScanReader("/data/scans")

        result = reader.read_table("2")

        resolver_class.return_value.select_file.assert_called_once_with(
            source_type="scan",
            manifest_index=0,
            scan_index=2,
        )
        loader_class.assert_called_once_with(
            str(scan_path),
            data_dir=str(scan_path.parent),
        )
        loader_class.return_value.load_data.assert_called_once_with()
        self.assertIs(result, loaded_table)
        self.assertEqual(result.attrs["source_file"], "scan-004.dat")

    @patch("kiwi_scan.ioc.pva_table.DataLoader")
    @patch("kiwi_scan.ioc.pva_table.ManifestResolver")
    def test_read_table_raises_when_loader_returns_none(
        self,
        resolver_class,
        loader_class,
    ):
        scan_path = Path("/data/scans/missing.dat")
        resolver_class.return_value.select_file.return_value = scan_path
        loader_class.return_value.load_data.return_value = None
        reader = ScanReader()

        with self.assertLogs(
            "kiwi_scan.ioc.pva_table",
            level="ERROR",
        ), self.assertRaisesRegex(
            FileNotFoundError,
            r"Could not load scan data file: /data/scans/missing\.dat",
        ):
            reader.read_table(0)

    @patch("kiwi_scan.ioc.pva_table.ManifestResolver")
    def test_read_table_rejects_non_integer_scan_index(self, resolver_class):
        reader = ScanReader()

        with self.assertRaises(ValueError):
            reader.read_table("latest")

        resolver_class.return_value.select_file.assert_not_called()


class TestDataFrameToNTTableConverter(unittest.TestCase):
    @patch("kiwi_scan.ioc.pva_table.Value")
    @patch("kiwi_scan.ioc.pva_table.NTTable")
    def test_convert_maps_numeric_and_text_columns(self, nt_table_class, value_class):
        table = pd.DataFrame(
            {
                "position": [1, 2],
                "reading": [1.5, np.nan],
                "status": ["ready", None],
                "mixed": [1, "two"],
            }
        )
        table.attrs["source_file"] = "scan-004.dat"
        nt_table_class.return_value.type = sentinel.nt_type
        value_class.return_value = sentinel.pva_value

        result = DataFrameToNTTableConverter.convert(table)

        self.assertIs(result, sentinel.pva_value)
        nt_table_class.assert_called_once_with(
            [("c0", "d"), ("c1", "d"), ("c2", "s"), ("c3", "s")]
        )
        value_class.assert_called_once()
        value_type, payload = value_class.call_args.args
        self.assertIs(value_type, sentinel.nt_type)
        self.assertEqual(
            payload["labels"],
            ["position", "reading", "status", "mixed"],
        )
        self.assertEqual(payload["descriptor"], "scan-004.dat")
        np.testing.assert_array_equal(payload["value"]["c0"], [1.0, 2.0])
        np.testing.assert_allclose(
            payload["value"]["c1"],
            [1.5, np.nan],
            equal_nan=True,
        )
        self.assertEqual(payload["value"]["c2"], ["ready", ""])
        self.assertEqual(payload["value"]["c3"], ["1", "two"])

    @patch("kiwi_scan.ioc.pva_table.Value")
    @patch("kiwi_scan.ioc.pva_table.NTTable")
    def test_convert_uses_string_labels_and_empty_default_descriptor(
        self,
        nt_table_class,
        value_class,
    ):
        table = pd.DataFrame({7: [True, False]})
        nt_table_class.return_value.type = sentinel.nt_type

        DataFrameToNTTableConverter.convert(table)

        _value_type, payload = value_class.call_args.args
        self.assertEqual(payload["labels"], ["7"])
        self.assertEqual(payload["descriptor"], "")
        np.testing.assert_array_equal(payload["value"]["c0"], [1.0, 0.0])


class TestPvaKiwiDataAdapter(unittest.TestCase):
    @patch("kiwi_scan.ioc.pva_table.DataFrameToNTTableConverter.convert")
    def test_table_reads_requested_scan_and_converts_it(self, convert):
        reader = Mock()
        table = pd.DataFrame({"position": [1.0]})
        table.attrs["source_file"] = "scan-001.dat"
        reader.read_table.return_value = table
        convert.return_value = sentinel.pva_value
        adapter = PvaKiwiDataAdapter(reader)

        result = adapter.TABLE("3")

        reader.read_table.assert_called_once_with(3)
        convert.assert_called_once_with(table)
        self.assertIs(result, sentinel.pva_value)

    @patch("kiwi_scan.ioc.pva_table.DataFrameToNTTableConverter.convert")
    def test_table_propagates_reader_errors(self, convert):
        reader = Mock()
        reader.read_table.side_effect = FileNotFoundError("missing scan")
        adapter = PvaKiwiDataAdapter(reader)

        with self.assertLogs(
            "kiwi_scan.ioc.pva_table",
            level="ERROR",
        ), self.assertRaisesRegex(FileNotFoundError, "missing scan"):
            adapter.TABLE(1)

        convert.assert_not_called()


class TestServe(unittest.TestCase):
    @patch("kiwi_scan.ioc.pva_table.quickRPCServer")
    @patch("kiwi_scan.ioc.pva_table.PvaKiwiDataAdapter")
    @patch("kiwi_scan.ioc.pva_table.ScanReader")
    def test_serve_constructs_expected_rpc_endpoint(
        self,
        reader_class,
        adapter_class,
        quick_rpc_server,
    ):
        serve(data_dir="/data/scans", prefix="TEST:DATA:")

        reader_class.assert_called_once_with("/data/scans")
        adapter_class.assert_called_once_with(reader_class.return_value)
        quick_rpc_server.assert_called_once_with(
            provider="KiwiPVAServer",
            prefix="TEST:DATA:SCAN:",
            target=adapter_class.return_value,
        )

    @patch(
        "kiwi_scan.ioc.pva_table.quickRPCServer",
        side_effect=RuntimeError("server failed"),
    )
    @patch("kiwi_scan.ioc.pva_table.PvaKiwiDataAdapter")
    @patch("kiwi_scan.ioc.pva_table.ScanReader")
    def test_serve_propagates_server_errors(
        self,
        _reader_class,
        _adapter_class,
        _quick_rpc_server,
    ):
        with self.assertLogs(
            "kiwi_scan.ioc.pva_table",
            level="ERROR",
        ), self.assertRaisesRegex(RuntimeError, "server failed"):
            serve()


if __name__ == "__main__":
    unittest.main()
