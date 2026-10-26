# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Validate optional detector read timing and its scan-reader wiring."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import call, patch

from kiwi_scan import test_support
from kiwi_scan.datamodels import ScanConfig

# BaseScan imports the EPICS wrapper; use the project fake for hardware-free tests.
if "epics" not in sys.modules:
    sys.modules["epics"] = test_support.make_fake_epics_module()

from kiwi_scan.scan.common import BaseScan


class TestDetectorReadTimeoutConfig(unittest.TestCase):
    def test_missing_timeout_preserves_legacy_default(self):
        parsed = ScanConfig.from_dict({"detector_pvs": ["DET:A"]})
        programmatic = ScanConfig(actuators={}, detector_pvs=["DET:A"])
        programmatic.validate()

        self.assertIsNone(parsed.detector_read_timeout)
        self.assertIsNone(programmatic.detector_read_timeout)

    def test_explicit_null_preserves_legacy_default(self):
        config = ScanConfig.from_dict({"detector_read_timeout": None})
        config.validate()
        self.assertIsNone(config.detector_read_timeout)

    def test_from_dict_normalizes_positive_numbers_and_numeric_strings(self):
        for value in (1, 0.25, "2.5", " 0.125 ", "1e-3"):
            with self.subTest(value=value):
                config = ScanConfig.from_dict({"detector_read_timeout": value})
                self.assertIsInstance(config.detector_read_timeout, float)
                self.assertEqual(config.detector_read_timeout, float(value))

    def test_validate_normalizes_programmatic_configuration(self):
        config = ScanConfig(
            actuators={}, detector_pvs=[], detector_read_timeout="0.75",
        )
        config.validate()
        self.assertEqual(config.detector_read_timeout, 0.75)
        self.assertIsInstance(config.detector_read_timeout, float)

    def test_from_dict_rejects_invalid_timeout_without_later_validation(self):
        invalid = (
            True, False, 0, -0.1, "0", "-1", "nan", "inf", "-inf",
            float("nan"), float("inf"), float("-inf"), "bad", "", [], {},
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaisesRegex(
                (TypeError, ValueError), "detector_read_timeout",
            ):
                ScanConfig.from_dict({"detector_read_timeout": value})

    def test_validate_rejects_invalid_programmatic_timeout(self):
        for value in (True, False, 0, -1, "nan", "inf", float("nan"), []):
            config = ScanConfig(
                actuators={}, detector_pvs=[], detector_read_timeout=value,
            )
            with self.subTest(value=value), self.assertRaisesRegex(
                (TypeError, ValueError), "detector_read_timeout",
            ):
                config.validate()


class TestDetectorReadTimeoutScanWiring(unittest.TestCase):
    def test_connect_detectors_forwards_timeout_and_keeps_connection_settings(self):
        cases = [
            ("direct", False, None),
            ("direct", True, 0.75),
            ("snapshot", True, 0.75),
        ]
        for strategy, use_monitor, timeout in cases:
            with self.subTest(strategy=strategy, use_monitor=use_monitor, timeout=timeout):
                config = ScanConfig(
                    actuators={},
                    detector_pvs=["DET:A", "DET:B"],
                    detector_pvs_monitor=use_monitor,
                    detector_reader_strategy=strategy,
                    detector_read_timeout=timeout,
                )
                scan = SimpleNamespace(cfg=config)
                reader = object()
                with patch(
                    "kiwi_scan.scan.detector_reader.EpicsPV", side_effect=test_support.FakePV,
                ) as create_pv, patch(
                    "kiwi_scan.scan.common.create_detector_reader", return_value=reader,
                ) as create_reader:
                    BaseScan._connect_detectors(scan)

                expected_connections = [
                    call(
                        name,
                        timeout=1.0,
                        connection_timeout=1.0,
                        queueing_delay=0.0,
                        auto_monitor=True,
                    )
                    for name in config.detector_pvs
                ]
                self.assertEqual(create_pv.call_args_list, expected_connections)
                self.assertEqual([pv.pvname for pv in scan.detector_pvs], config.detector_pvs)
                self.assertTrue(all(isinstance(pv, test_support.FakePV) for pv in scan.detector_pvs))
                create_reader.assert_called_once_with(
                    strategy,
                    scan.detector_pvs,
                    use_monitor=use_monitor,
                    read_timeout=timeout,
                )
                self.assertIs(scan._detector_reader, reader)


if __name__ == "__main__":
    unittest.main()
