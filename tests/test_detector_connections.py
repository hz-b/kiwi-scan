# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Protect shared detector startup and the callers' existing failure policies."""

import io
import json
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.scan import detector_reader
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.test_support import FakePV
from kiwi_scan.tools.detectors import print_detector_values


class ConnectionPV(FakePV):
    """Add owned cleanup and metadata reads to the project's shared PV dummy."""

    def __init__(self, pvname, **kwargs):
        super().__init__(pvname, **kwargs)
        self.close_calls = 0
        self.close_error = None
        self.close_order = None

    def get_with_metadata(self, *, use_monitor, timeout=None):
        self.get_calls.append({"use_monitor": use_monitor, "timeout": timeout})
        return {"value": 3.5, "timestamp": 0}

    def close(self):
        self.close_calls += 1
        self.disconnect()
        if self.close_order is not None:
            self.close_order.append(self.pvname)
        if self.close_error is not None:
            raise self.close_error


class TestSharedDetectorConnections(unittest.TestCase):
    def setUp(self):
        self.attempts = []
        self.instances = []

    def factory(self, errors=None):
        errors = {} if errors is None else errors

        def create(name, **kwargs):
            self.attempts.append((name, kwargs))
            if name in errors:
                raise errors[name]
            pv = ConnectionPV(name, **kwargs)
            self.instances.append(pv)
            return pv

        return create

    def test_generator_creates_connections_only_when_consumed(self):
        with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory()):
            connections = detector_reader.connect_detector_pvs(
                ["DET:A", "DET:B"], 2.0, auto_monitor=False, abort_on_error=False,
            )
            self.assertEqual(self.attempts, [])
            index, pv = next(connections)
            self.assertEqual((index, pv.pvname), (0, "DET:A"))
            self.assertEqual([name for name, _kwargs in self.attempts], ["DET:A"])
            self.assertEqual([(index, pv.pvname) for index, pv in connections], [(1, "DET:B")])
        self.assertTrue(all(pv.close_calls == 0 for pv in self.instances))

    def test_tolerant_connections_preserve_original_indices_and_cleanup(self):
        names = ["DET:A", "DET:B", "DET:C"]
        for failures in (("DET:A",), ("DET:B",), tuple(names)):
            with self.subTest(failures=failures):
                self.attempts.clear()
                self.instances.clear()
                errors = {name: ConnectionError("unavailable " + name) for name in failures}
                with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory(errors)), \
                        self.assertLogs("kiwi_scan.scan.detector_reader", level="WARNING") as log, \
                        ExitStack() as cleanup:
                    connected = list(detector_reader.connect_detector_pvs(
                        names, 2.5, cleanup, auto_monitor=False, abort_on_error=False,
                    ))
                    self.assertTrue(all(pv.close_calls == 0 for pv in self.instances))
                self.assertEqual([(index, pv.pvname) for index, pv in connected], [
                    (index, name) for index, name in enumerate(names) if name not in failures
                ])
                self.assertEqual([name for name, _kwargs in self.attempts], names)
                for name in failures:
                    self.assertTrue(any(name in message for message in log.output))
                self.assertTrue(all(pv.close_calls == 1 and pv.disconnected for pv in self.instances))
                for _name, kwargs in self.attempts:
                    self.assertEqual(kwargs, {
                        "timeout": 1.0, "connection_timeout": 2.5,
                        "queueing_delay": 0.01, "auto_monitor": False,
                    })

    def test_strict_connections_reraise_same_error_and_stop_immediately(self):
        failure = ConnectionError("second detector unavailable")
        with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory({"DET:B": failure})):
            connections = detector_reader.connect_detector_pvs(
                ["DET:A", "DET:B", "DET:C"], 1.0, auto_monitor=True,
                abort_on_error=True, timeout=1.0, queueing_delay=0.0,
            )
            self.assertEqual(next(connections)[0], 0)
            with self.assertRaises(ConnectionError) as error:
                next(connections)
        self.assertIs(error.exception, failure)
        self.assertEqual([name for name, _kwargs in self.attempts], ["DET:A", "DET:B"])
        self.assertEqual(self.instances[0].close_calls, 0)

    def test_strict_failure_with_an_exit_stack_closes_preceding_connections(self):
        failure = ConnectionError("second detector unavailable")
        with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory({"DET:B": failure})), \
                self.assertRaises(ConnectionError) as error, ExitStack() as cleanup:
            list(detector_reader.connect_detector_pvs(
                ["DET:A", "DET:B", "DET:C"], 1.0, cleanup,
                auto_monitor=True, abort_on_error=True,
            ))
        self.assertIs(error.exception, failure)
        self.assertEqual(self.instances[0].close_calls, 1)

    def test_interruption_is_not_swallowed_and_exit_stack_cleans_up(self):
        with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory({"DET:B": KeyboardInterrupt()})), \
                self.assertRaises(KeyboardInterrupt), ExitStack() as cleanup:
            list(detector_reader.connect_detector_pvs(
                ["DET:A", "DET:B", "DET:C"], 1.0, cleanup,
                auto_monitor=False, abort_on_error=False,
            ))
        self.assertEqual([name for name, _kwargs in self.attempts], ["DET:A", "DET:B"])
        self.assertEqual(self.instances[0].close_calls, 1)

    def test_cleanup_failure_still_runs_other_registered_callbacks(self):
        close_order = []
        failure = OSError("disconnect failed")
        with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory()), \
                self.assertRaises(OSError) as error, ExitStack() as cleanup:
            connections = list(detector_reader.connect_detector_pvs(
                ["DET:A", "DET:B"], 1.0, cleanup,
                auto_monitor=False, abort_on_error=False,
            ))
            for _index, pv in connections:
                pv.close_order = close_order
            connections[1][1].close_error = failure
        self.assertIs(error.exception, failure)
        self.assertEqual(close_order, ["DET:B", "DET:A"])
        self.assertTrue(all(pv.close_calls == 1 for pv in self.instances))

    def test_base_scan_preserves_connection_settings_and_retains_pvs(self):
        config = ScanConfig(
            actuators={}, detector_pvs=["DET:A", "DET:B"], detector_pvs_monitor=False,
            detector_read_timeout=0.4,
        )
        scan = SimpleNamespace(cfg=config, _resources=ExitStack())
        with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory()):
            BaseScan._connect_detectors(scan)
        self.assertEqual(scan.detector_pvs, self.instances)
        self.assertFalse(scan.detector_pvs_monitor)
        self.assertTrue(all(pv.close_calls == 0 for pv in self.instances))
        self.assertEqual(len(self.attempts), 2)
        for _name, kwargs in self.attempts:
            self.assertEqual(kwargs, {
                "timeout": 1.0, "connection_timeout": 1.0,
                "queueing_delay": 0.0, "auto_monitor": True,
            })
        self.assertIsInstance(scan._detector_reader.strategy, detector_reader.DirectDetectorReadStrategy)

    def test_base_scan_failed_startup_keeps_partial_detector_list(self):
        config = ScanConfig(actuators={}, detector_pvs=["DET:A", "DET:B", "DET:C"])
        scan = SimpleNamespace(cfg=config, _resources=ExitStack())
        failure = ConnectionError("second detector unavailable")
        with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory({"DET:B": failure})), \
                patch("kiwi_scan.scan.common.create_detector_reader") as create_reader, \
                self.assertRaises(ConnectionError) as error:
            BaseScan._connect_detectors(scan)
        self.assertIs(error.exception, failure)
        self.assertEqual([pv.pvname for pv in scan.detector_pvs], ["DET:A"])
        self.assertEqual(scan.detector_pvs, self.instances)
        self.assertEqual(self.instances[0].close_calls, 0)
        self.assertEqual([name for name, _kwargs in self.attempts], ["DET:A", "DET:B"])
        create_reader.assert_not_called()
        # BaseScan.__init__ closes _resources on failure, which releases the partial list.
        scan._resources.close()
        self.assertEqual(self.instances[0].close_calls, 1)

    def test_one_shot_tool_preserves_columns_and_closes_successful_connections(self):
        names = ["DET:A", "DET:B", "DET:C"]
        config = ScanConfig(actuators={}, detector_pvs=names)
        for failures in (("DET:A",), ("DET:B",), tuple(names)):
            with self.subTest(failures=failures):
                self.instances.clear()
                self.attempts.clear()
                out = io.StringIO()
                errors = {name: ConnectionError("unavailable") for name in failures}
                with patch("kiwi_scan.scan.detector_reader.EpicsPV", self.factory(errors)):
                    result = print_detector_values(config, connection_timeout=2.5, output_format="json", out=out)
                self.assertEqual(result, 1)
                values = json.loads(out.getvalue())
                self.assertEqual(list(values), names)
                self.assertEqual(values, {name: None if name in failures else 3.5 for name in names})
                self.assertEqual([name for name, _kwargs in self.attempts], names)
                self.assertTrue(all(pv.close_calls == 1 for pv in self.instances))
                for _name, kwargs in self.attempts:
                    self.assertEqual(kwargs, {
                        "timeout": 1.0, "connection_timeout": 2.5,
                        "queueing_delay": 0.01, "auto_monitor": False,
                    })


if __name__ == "__main__":
    unittest.main()
