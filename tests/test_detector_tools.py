# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Exercise one-shot reads through the real detector reader and formatter."""

import csv
import io
import json
import unittest
from unittest.mock import patch

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.test_support import FakePV
from kiwi_scan.tools.detectors import print_detector_values


class DetectorPV(FakePV):
    """Extend the shared dummy with direct metadata reads and owned cleanup."""

    def __init__(self, pvname, **kwargs):
        super().__init__(pvname, **kwargs)
        self.read_error = None
        self.close_calls = 0

    def get_with_metadata(self, *, use_monitor, timeout=None):
        self.get_calls.append({"use_monitor": use_monitor, "timeout": timeout})
        if self.read_error is not None:
            raise self.read_error
        if self._value is None:
            return None
        return {"value": self._value, "timestamp": self.timestamp}

    def close(self):
        self.close_calls += 1
        self.disconnect()


class TestDetectorTools(unittest.TestCase):
    def setUp(self):
        self.instances = []
        self.attempted_names = []

    def factory(self, *, values=None, initialization_failures=(), read_errors=None):
        values = {} if values is None else values
        read_errors = {} if read_errors is None else read_errors

        def create(pvname, **kwargs):
            self.attempted_names.append(pvname)
            if pvname in initialization_failures:
                raise ConnectionError("unavailable " + pvname)
            pv = DetectorPV(pvname, **kwargs)
            pv._value = values.get(pvname, 1.25)
            pv.timestamp = 0
            pv.read_error = read_errors.get(pvname)
            self.instances.append(pv)
            return pv

        return create

    def config(self, names=None, **kwargs):
        return ScanConfig(actuators={}, detector_pvs=["DET:A", "DET:B"] if names is None else names, **kwargs)

    def assert_closed(self):
        self.assertTrue(all(pv.disconnected and pv.close_calls == 1 for pv in self.instances))

    def test_initialization_failures_keep_first_middle_and_all_column_positions(self):
        names = ["DET:A", "DET:B", "DET:C"]
        for failures in (("DET:A",), ("DET:B",), tuple(names)):
            with self.subTest(failures=failures):
                self.instances.clear()
                self.attempted_names.clear()
                out = io.StringIO()
                with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory(initialization_failures=failures)):
                    result = print_detector_values(self.config(names), out=out)
                rows = list(csv.reader(io.StringIO(out.getvalue()), delimiter="\t"))
                self.assertEqual(result, 1)
                self.assertEqual(rows[0], names)
                self.assertEqual(rows[1], ["" if name in failures else "1.250000000000e+00" for name in names])
                self.assertEqual(self.attempted_names, names)
                self.assertEqual([pv.pvname for pv in self.instances], [name for name in names if name not in failures])
                self.assertTrue(all(len(pv.get_calls) == 1 for pv in self.instances))
                self.assert_closed()

    def test_read_failures_and_missing_data_keep_alignment_and_read_later_pvs(self):
        names = ["DET:A", "DET:B", "DET:C", "DET:D"]
        out = io.StringIO()
        with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory(
            values={"DET:A": 2, "DET:B": None, "DET:D": 4},
            read_errors={"DET:C": RuntimeError("read failed")},
        )):
            result = print_detector_values(self.config(names), out=out, output_format="json")
        self.assertEqual(result, 1)
        self.assertEqual(json.loads(out.getvalue()), {"DET:A": 2, "DET:B": None, "DET:C": None, "DET:D": 4})
        self.assertTrue(all(len(pv.get_calls) == 1 for pv in self.instances))
        self.assert_closed()

    def test_timeout_precedence_is_explicit_then_yaml_then_tool_default(self):
        cases = [
            ({"detector_read_timeout": 0.35}, None, 0.35),
            ({"detector_read_timeout": "0.45"}, None, 0.45),
            ({"detector_read_timeout": 0.35}, 0.75, 0.75),
            ({}, None, 1.0),
        ]
        for yaml_options, override, expected in cases:
            with self.subTest(yaml_options=yaml_options, override=override):
                self.instances.clear()
                config = ScanConfig.from_dict(dict({"detector_pvs": ["DET:A", "DET:B"]}, **yaml_options))
                with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory()):
                    result = print_detector_values(config, read_timeout=override, out=io.StringIO())
                self.assertEqual(result, 0)
                self.assertEqual([pv.get_calls for pv in self.instances], [
                    [{"use_monitor": False, "timeout": expected}],
                    [{"use_monitor": False, "timeout": expected}],
                ])
                self.assert_closed()

    def test_snapshot_scan_configuration_still_uses_direct_gets_without_monitors(self):
        config = self.config(detector_reader_strategy="snapshot", detector_pvs_monitor=True)
        with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory()):
            result = print_detector_values(config, connection_timeout=2.5, read_timeout=0.5, out=io.StringIO())
        self.assertEqual(result, 0)
        for pv in self.instances:
            self.assertFalse(pv.auto_monitor)
            self.assertEqual(pv.kwargs["connection_timeout"], 2.5)
            self.assertEqual(pv.get_calls, [{"use_monitor": False, "timeout": 0.5}])
            self.assertEqual(pv.callbacks, [])
            self.assertEqual(pv.put_calls, [])
        self.assertEqual(config.detector_reader_strategy, "snapshot")
        self.assert_closed()

    def test_tsv_and_csv_use_original_monitor_value_and_timestamp_formatting(self):
        names = ["DET:A", "DET:TEXT"]
        for output_format, delimiter in (("tsv", "\t"), ("csv", ",")):
            with self.subTest(output_format=output_format):
                self.instances.clear()
                out = io.StringIO()
                with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory(
                    values={"DET:A": 2.5, "DET:TEXT": 'one,two\n"three"'},
                )):
                    result = print_detector_values(
                        self.config(names), output_format=output_format, include_timestamps=True, out=out,
                    )
                self.assertEqual(result, 0)
                self.assertEqual(list(csv.reader(io.StringIO(out.getvalue()), delimiter=delimiter)), [
                    ["DET:A", "TS-ISO8601-DET:A", "DET:TEXT", "TS-ISO8601-DET:TEXT"],
                    ["2.500000000000e+00", "1970-01-01T00:00:00+00:00", 'one,two\n"three"', "1970-01-01T00:00:00+00:00"],
                ])
                self.assertFalse(out.closed)
                self.assert_closed()

    def test_json_uses_original_monitor_object_layout_and_native_values(self):
        out = io.StringIO()
        with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory(values={"DET:A": 2.5, "DET:B": None})):
            result = print_detector_values(self.config(), output_format="json", include_timestamps=True, out=out)
        self.assertEqual(result, 1)
        self.assertEqual(json.loads(out.getvalue()), {
            "DET:A": 2.5, "TS-ISO8601-DET:A": "1970-01-01T00:00:00+00:00",
            "DET:B": None, "TS-ISO8601-DET:B": "",
        })
        self.assert_closed()

    def test_invalid_timeouts_fail_before_any_pv_initialization(self):
        for keyword in ("read_timeout", "connection_timeout"):
            for value in (0, -1, float("nan"), float("inf"), True, "invalid"):
                with self.subTest(keyword=keyword, value=value), \
                        patch("kiwi_scan.tools.detectors.EpicsPV") as factory, \
                        self.assertRaises((TypeError, ValueError)):
                    print_detector_values(self.config(), out=io.StringIO(), **{keyword: value})
                factory.assert_not_called()

    def test_invalid_yaml_timeout_on_direct_config_fails_before_pv_creation(self):
        config = self.config(detector_read_timeout=-1)
        with patch("kiwi_scan.tools.detectors.EpicsPV") as factory, self.assertRaises(ValueError):
            print_detector_values(config, out=io.StringIO())
        factory.assert_not_called()

    def test_invalid_format_fails_before_any_pv_initialization(self):
        with patch("kiwi_scan.tools.detectors.EpicsPV") as factory, self.assertRaises(ValueError):
            print_detector_values(self.config(), output_format="invalid", out=io.StringIO())
        factory.assert_not_called()

    def test_read_interrupt_closes_every_created_pv_and_propagates(self):
        out = io.StringIO()
        with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory(read_errors={"DET:A": KeyboardInterrupt()})), \
                self.assertRaises(KeyboardInterrupt):
            print_detector_values(self.config(), out=out)
        self.assertEqual(out.getvalue(), "")
        self.assertEqual(len(self.instances), 2)
        self.assert_closed()

    def test_initialization_interrupt_closes_previously_created_pvs(self):
        good_factory = self.factory()

        def factory(name, **kwargs):
            if name == "DET:B":
                raise KeyboardInterrupt()
            return good_factory(name, **kwargs)

        with patch("kiwi_scan.tools.detectors.EpicsPV", factory), self.assertRaises(KeyboardInterrupt):
            print_detector_values(self.config(), out=io.StringIO())
        self.assertEqual(len(self.instances), 1)
        self.assert_closed()

    def test_output_failure_still_closes_every_created_pv(self):
        class BrokenOutput(io.StringIO):
            def write(self, text):
                raise OSError("output unavailable")

        with patch("kiwi_scan.tools.detectors.EpicsPV", self.factory()), self.assertRaises(OSError):
            print_detector_values(self.config(), out=BrokenOutput())
        self.assert_closed()

    def test_empty_detector_list_succeeds_and_owns_no_connections(self):
        out = io.StringIO()
        with patch("kiwi_scan.tools.detectors.EpicsPV") as factory:
            result = print_detector_values(self.config([]), output_format="json", out=out)
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(out.getvalue()), {})
        factory.assert_not_called()

    def test_invalid_detector_names_fail_before_opening_connections(self):
        for names in ("DET:A", None, [""], ["   "], [7]):
            with self.subTest(names=names), patch("kiwi_scan.tools.detectors.EpicsPV") as factory, \
                    self.assertRaises((TypeError, ValueError)):
                config = ScanConfig(actuators={}, detector_pvs=names)
                print_detector_values(config, out=io.StringIO())
            factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
