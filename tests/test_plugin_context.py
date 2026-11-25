# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import math
import sys
import unittest
from types import SimpleNamespace
from typing import Any, List, Optional, get_type_hints
from unittest.mock import call, patch
from weakref import ProxyType

from kiwi_scan import test_support

if "epics" not in sys.modules:
    sys.modules["epics"] = test_support.make_fake_epics_module()

from kiwi_scan.datamodels import PluginConfig
from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.context import ScanPluginContext
from kiwi_scan.plugin.registry import create_plugin
from kiwi_scan.plugin_concrete.emc_getter import EmcGetterPlugin
from kiwi_scan.plugin_concrete.jog_pid import JogPIDPlugin
from kiwi_scan.plugin_concrete.logging import LoggingPlugin
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.output_manager import OutputManager
from kiwi_scan.scan.performance_tracker import PerformanceTracker


class _ContextScan(BaseScan):
    """Lightweight BaseScan proving the structural plugin contract."""

    def __init__(self) -> None:
        self.cfg = SimpleNamespace(detector_pvs=["DET"])
        self.detector_pvs = [SimpleNamespace(pvname="DET")]
        self.plugins: List[Any] = []
        self.actuators = {"energy": object()}
        self.include_timestamps = False
        self.timestamp_output_format = "iso8601"
        self.performance = PerformanceTracker(enabled=False)
        self.output_manager = OutputManager(
            data_dir=".",
            requested_output_file="unused.txt",
            data_writing_enabled=False,
        )
        self._position = None
        self._initialize_point_pipeline()

    def execute(self) -> None:
        pass


class _ContextPlugin(ScanPlugin):
    def get_headers(self, timestamps: bool) -> List[str]:
        del timestamps
        return []

    def get_values(self, idx: int, pos: Any) -> List[Any]:
        del idx, pos
        return []


class TestScanPluginContext(unittest.TestCase):
    def test_base_scan_satisfies_plugin_context(self) -> None:
        scan = _ContextScan()
        scan._point_pipeline.replace_last_point({"DET": {"value": 3.0}})
        scan._point_pipeline.replace_current_row_cache({"DET": 4.0})

        self.assertIsInstance(scan, ScanPluginContext)
        self.assertEqual(scan.get_value("DET"), 3.0)
        self.assertEqual(scan.get_current_row_value("DET"), 4.0)
        self.assertIs(scan.get_actuator("energy"), scan.actuators["energy"])
        self.assertEqual(scan.get_actuators(), scan.actuators)
        self.assertEqual(scan.cfg.detector_pvs, ["DET"])

    def test_scan_plugin_keeps_weak_proxy_runtime_behavior(self) -> None:
        scan = _ContextScan()
        plugin = _ContextPlugin("context", scan=scan)

        self.assertIsNot(plugin.scan, scan)
        self.assertIs(plugin.scan.cfg, scan.cfg)
        self.assertEqual(plugin.scan.get_actuators(), scan.get_actuators())

    def test_scan_plugin_constructor_is_typed_against_context(self) -> None:
        for plugin in (ScanPlugin, LoggingPlugin, JogPIDPlugin, EmcGetterPlugin):
            with self.subTest(plugin=plugin.__name__):
                hints = get_type_hints(plugin.__init__)
                self.assertEqual(hints["scan"], Optional[ScanPluginContext])

    @patch("kiwi_scan.plugin_concrete.emc_getter.socket.create_connection", autospec=True)
    def test_registered_emc_getter_prepares_snapshot_without_connecting(self, connect) -> None:
        scan = _ContextScan()
        config = PluginConfig(
            type="hzb.emc_getter",
            parameters={"getters": [{"command": "GPE", "column": "energy"}]},
        )
        plugin = create_plugin(config, scan=scan)
        self.addCleanup(plugin.close)

        self.assertIsInstance(plugin, EmcGetterPlugin)
        self.assertIsInstance(plugin.scan, ProxyType)
        self.assertIs(plugin.scan.cfg, scan.cfg)
        self.assertIsNotNone(plugin.build_data_snapshot(0, {"energy": 12.5}))
        connect.assert_not_called()


class TestEmcGetterPlugin(unittest.TestCase):
    @patch("kiwi_scan.plugin_concrete.emc_getter.socket.create_connection", autospec=True)
    @patch("kiwi_scan.plugin_concrete.emc_getter.time")
    @patch("kiwi_scan.plugin_concrete.emc_getter.monotonic")
    def test_scan_points_match_cached_headers(self, clock, wall_time, connect) -> None:
        for record_times, headers, expected in (
            (False, ["energy", "status"], [12.5, 7]),
            (
                True,
                ["energy", "energy_before", "energy_after", "status", "status_before", "status_after"],
                [12.5, 100, 101, 7, 102, 103],
            ),
        ):
            with self.subTest(record_times=record_times):
                parameters = {
                    "getters": [
                        {"command": "GPE", "column": "energy"},
                        {"command": "GST", "column": "status", "value_type": "int"},
                    ],
                    "record_times": record_times,
                    "min_interval_s": 1,
                }
                plugin = EmcGetterPlugin("emc", parameters=parameters)
                self.addCleanup(plugin.close)
                sock = connect.return_value
                sock.reset_mock()
                sock.recv.side_effect = [b"t 12.5", b"t 7", b"t 99.0", b"f"]
                wall_time.side_effect = range(100, 108)
                clock.return_value = 0.0

                cached = plugin.get_headers(False)
                self.assertEqual(cached, headers)
                pending = plugin.on_scan_point(0, {})
                plugin._async_job.result(timeout=5)

                # The next point publishes the first group and starts the failing group.
                clock.return_value = 1.0
                complete = plugin.on_scan_point(1, {})
                self.assertEqual([field["value"] for field in complete], expected)
                with self.assertRaises(ValueError):
                    plugin._async_job.result(timeout=5)
                with self.assertLogs(plugin.logger, level="ERROR"):
                    failed = plugin.on_scan_point(2, {})

                for row in (pending, failed):
                    self.assertEqual(len(row), len(headers))
                    self.assertTrue(all(math.isnan(field["value"]) for field in row))
                self.assertIs(plugin.get_headers(False), cached)
                self.assertIs(plugin.get_headers(True), cached)
                self.assertEqual(cached, headers)
                self.assertEqual(sock.sendall.call_args_list, [call(b"GPE\n"), call(b"GST\n")] * 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
