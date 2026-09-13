# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import sys
import unittest
from types import SimpleNamespace
from typing import Any, List, get_type_hints

from kiwi_scan import test_support

if "epics" not in sys.modules:
    sys.modules["epics"] = test_support.make_fake_epics_module()

from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.context import ScanPluginContext
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
        hints = get_type_hints(ScanPlugin.__init__)
        self.assertIn("ScanPluginContext", str(hints["scan"]))
        self.assertNotIn("BaseScan", str(hints["scan"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
