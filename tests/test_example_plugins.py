# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT
"""
Tests for the example plugins in examples/plugins/.

The example files are not part of the kiwi_scan package. They are loaded by
path with importlib, the same way KIWI_SCAN_PLUGIN_PATH loads them.

Unit tests use a small fake ScanPluginContext (no EPICS, no scan). The end to
end test runs a LinearScan with a ``type: sim`` actuator and
``detector_pvs: []``, so it needs no IOC either.
"""

import importlib.util
import math
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import patch

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.plugin.base import ScanPlugin
from kiwi_scan.plugin.registry import PLUGIN_REGISTRY, register_plugin
from kiwi_scan.scan_concrete.linear import LinearScan

EXAMPLES = Path(__file__).resolve().parents[1] / "examples" / "plugins"
SIGNAL = "SIM:ENERGY:RBV"
TS_KEY = "TS-" + SIGNAL
T0 = 1.7e9  # realistic POSIX seconds, to exercise float precision


def _load_example(stem: str, plugin_type: str):
    """
    Import one example file and return its module.

    A plugin name can be registered only once per process. If the plugin is
    already registered (e.g. KIWI_SCAN_PLUGIN_PATH points to examples/plugins),
    reuse the module that registered it.
    """
    cls = PLUGIN_REGISTRY.get(plugin_type)
    if cls is not None:
        return sys.modules[cls.__module__]
    module_name = f"kiwi_example_plugin_{stem}"
    spec = importlib.util.spec_from_file_location(module_name, EXAMPLES / f"{stem}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)  # registers the plugin
    return module


motion_rate = _load_example("motion_rate", "MotionRatePlugin")
MotionRatePlugin = motion_rate.MotionRatePlugin


class FakeContext:
    """Implements the ScanPluginContext methods with a dict as current row."""

    def __init__(self) -> None:
        self.cfg = SimpleNamespace(detector_pvs=[])
        self.row: Dict[str, Any] = {}

    def get_current_row_value(self, key: str, default: Any = None) -> Any:
        return self.row.get(key, default)

    def get_current_row_cache(self) -> Dict[str, Any]:
        return dict(self.row)

    def get_value(self, name: str, *, default: Any = None, with_metadata: bool = False) -> Any:
        del name, with_metadata
        return default

    def get_actuator(self, name: str):
        raise KeyError(name)

    def get_actuators(self):
        return {}


def _wait_ready(plugin, ctx: FakeContext, idx: int, timeout: float = 2.0) -> List[Any]:
    """Repeat get_values() for the current row until a new fit is collected."""
    deadline = time.monotonic() + timeout
    values = plugin.get_values(idx, 0.0)
    while time.monotonic() < deadline:
        if values[1] == idx:
            return values
        time.sleep(0.005)
        # Same row again: the duplicate timestamp is ignored, so only the
        # pending job result is collected.
        values = plugin.get_values(idx, 0.0)
    return values


class TestMotionRateRegistration(unittest.TestCase):
    def test_registered_name(self) -> None:
        self.assertIs(PLUGIN_REGISTRY["MotionRatePlugin"], MotionRatePlugin)


class TestMotionRateParameters(unittest.TestCase):
    def _make(self, parameters):
        plugin = MotionRatePlugin("rate", parameters, None)
        self.addCleanup(plugin.close)
        return plugin

    def test_defaults(self) -> None:
        plugin = self._make({"signal": SIGNAL})
        self.assertEqual(plugin.signal, SIGNAL)
        self.assertEqual(plugin.window, 50)

    def test_valid_window(self) -> None:
        self.assertEqual(self._make({"signal": SIGNAL, "window": 2}).window, 2)
        self.assertEqual(self._make({"signal": SIGNAL, "window": 7.0}).window, 7)

    def test_signal_is_required(self) -> None:
        for parameters in ({}, None, {"signal": ""}, {"signal": "  "}, {"signal": 3}):
            with self.subTest(parameters=parameters), self.assertRaisesRegex(ValueError, "'signal'"):
                MotionRatePlugin("rate", parameters, None)

    def test_window_range(self) -> None:
        for window in (1, 0, -5, 2.5, True, "50", None, float("nan")):
            with self.subTest(window=window), self.assertRaisesRegex(ValueError, "'window'.*>= 2"):
                MotionRatePlugin("rate", {"signal": SIGNAL, "window": window}, None)

    def test_invalid_config_starts_no_worker(self) -> None:
        with patch("kiwi_scan.plugin.async_base.ThreadPoolExecutor") as executor, \
                self.assertRaises(ValueError):
            MotionRatePlugin("rate", {"signal": SIGNAL, "window": 1}, None)
        executor.assert_not_called()


class TestFitLine(unittest.TestCase):
    def test_known_slope_from_synthetic_timestamps(self) -> None:
        # 1 kHz sampling at realistic POSIX time, 0.25 units/s.
        times = tuple(T0 + 0.001 * i for i in range(50))
        values = tuple(100.0 + 0.25 * (t - T0) for t in times)
        slope, offset = motion_rate.fit_line(times, values)
        self.assertAlmostEqual(slope, 0.25, places=6)
        self.assertAlmostEqual(offset, 100.0, places=9)

    def test_offset_is_value_at_first_timestamp(self) -> None:
        times = (T0 + 10.0, T0 + 11.0, T0 + 12.0)
        values = (5.0, 3.0, 1.0)
        slope, offset = motion_rate.fit_line(times, values)
        self.assertAlmostEqual(slope, -2.0)
        self.assertAlmostEqual(offset, 5.0)

    def test_all_timestamps_equal(self) -> None:
        slope, offset = motion_rate.fit_line((T0, T0, T0), (1.0, 2.0, 3.0))
        self.assertTrue(math.isnan(slope))
        self.assertTrue(math.isnan(offset))

    def test_some_repeated_timestamps(self) -> None:
        slope, offset = motion_rate.fit_line((T0, T0, T0 + 1.0, T0 + 1.0), (0.0, 0.0, 2.0, 2.0))
        self.assertAlmostEqual(slope, 2.0)
        self.assertAlmostEqual(offset, 0.0)

    def test_too_few_samples(self) -> None:
        self.assertTrue(math.isnan(motion_rate.fit_line((T0,), (1.0,))[0]))
        self.assertTrue(math.isnan(motion_rate.fit_line((), ())[0]))


class TestMotionRatePlugin(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = FakeContext()
        self.plugin = MotionRatePlugin("rate", {"signal": SIGNAL, "window": 4}, self.ctx)
        self.addCleanup(self.plugin.close)
        self.plugin.on_start()
        self.headers = self.plugin.get_headers(False)

    def _set_row(self, ts: Any, value: Any) -> None:
        self.ctx.row = {SIGNAL: value, TS_KEY: ts}

    def _assert_shape(self, values: List[Any]) -> None:
        self.assertEqual(len(values), len(self.headers))

    def test_headers(self) -> None:
        self.assertEqual(
            self.headers,
            ["MotionRateStatus", "MotionRateIndex", "MotionRateSlope", "MotionRateOffset"],
        )
        self.assertIs(self.plugin.get_headers(True), self.headers)
        self.assertEqual(len(self.plugin.get_async_default_values()), len(self.headers))

    def test_pending_before_first_fit(self) -> None:
        self._set_row(T0, 1.0)
        values = self.plugin.get_values(0, 0.0)
        self._assert_shape(values)
        self.assertEqual(values[:2], ["pending", -1])
        self.assertTrue(math.isnan(values[2]) and math.isnan(values[3]))

    def test_value_count_on_every_path(self) -> None:
        rows = [
            {},                                   # signal and timestamp missing
            {SIGNAL: 1.0},                        # timestamp missing
            {TS_KEY: T0},                         # value missing
            {SIGNAL: "busy", TS_KEY: T0},         # not numeric
            {SIGNAL: [1.0, 2.0], TS_KEY: T0},     # waveform
            {SIGNAL: float("nan"), TS_KEY: T0},   # not finite
            {SIGNAL: 1.0, TS_KEY: float("inf")},  # not finite
            {SIGNAL: 1.0, TS_KEY: T0},            # first valid sample
            {SIGNAL: 1.0, TS_KEY: T0},            # repeated timestamp
            {SIGNAL: 2.0, TS_KEY: T0 + 1.0},      # first fit
        ]
        for idx, row in enumerate(rows):
            self.ctx.row = row
            with self.subTest(idx=idx, row=row):
                self._assert_shape(self.plugin.get_values(idx, 0.0))
        values = _wait_ready(self.plugin, self.ctx, len(rows) - 1)
        self._assert_shape(values)
        self.assertEqual(values[0], "ready")

    def test_value_count_without_scan_context(self) -> None:
        plugin = MotionRatePlugin("rate", {"signal": SIGNAL}, None)
        self.addCleanup(plugin.close)
        self.assertEqual(len(plugin.get_values(0, 0.0)), len(plugin.get_headers(False)))

    def test_value_count_after_worker_failure(self) -> None:
        with patch.object(motion_rate, "fit_line", side_effect=RuntimeError("boom")), \
                self.assertLogs(self.plugin.logger, "ERROR"):
            self._set_row(T0, 1.0)
            self.plugin.get_values(0, 0.0)
            self._set_row(T0 + 1.0, 2.0)
            self.plugin.get_values(1, 0.0)
            deadline = time.monotonic() + 2.0
            while self.plugin.get_async_status()["completed_jobs"] == 0 and time.monotonic() < deadline:
                time.sleep(0.005)
                values = self.plugin.get_values(1, 0.0)
        self._assert_shape(values)
        self.assertEqual(values[0], "pending")

    def test_value_count_after_close(self) -> None:
        self.plugin.close()
        self._set_row(T0, 1.0)
        self._assert_shape(self.plugin.get_values(0, 0.0))
        self._set_row(T0 + 1.0, 2.0)
        self._assert_shape(self.plugin.get_values(1, 0.0))
        self.plugin.close()  # repeated close is safe

    def test_degenerate_window_reports_error(self) -> None:
        values = self.plugin.process_data_snapshot((7, (T0, T0), (1.0, 2.0)))
        self._assert_shape(values)
        self.assertEqual(values[:2], ["error", 7])
        self.assertTrue(math.isnan(values[2]) and math.isnan(values[3]))

    def test_known_slope_through_plugin(self) -> None:
        # 3 units/s; sampled at 10 Hz.
        values: List[Any] = []
        for idx in range(6):
            t = 0.1 * idx
            self._set_row(T0 + t, 7.0 + 3.0 * t)
            values = _wait_ready(self.plugin, self.ctx, idx) if idx else self.plugin.get_values(idx, 0.0)
        self.assertEqual(values[:2], ["ready", 5])
        self.assertAlmostEqual(values[2], 3.0, places=6)
        # window=4: the newest window starts at idx 2 (t=0.2s).
        self.assertAlmostEqual(values[3], 7.0 + 3.0 * 0.2, places=6)

    def test_repeated_timestamps_are_not_new_samples(self) -> None:
        self._set_row(T0, 1.0)
        self.assertIsNone(self.plugin.build_data_snapshot(0, 0.0))
        self.assertIsNone(self.plugin.build_data_snapshot(1, 0.0))  # PV not updated
        self._set_row(T0 + 1.0, 3.0)
        snapshot = self.plugin.build_data_snapshot(2, 0.0)
        self.assertEqual(snapshot, (2, (T0, T0 + 1.0), (1.0, 3.0)))
        self.assertIsNone(self.plugin.build_data_snapshot(3, 0.0))  # PV not updated

    def test_window_slides(self) -> None:
        snapshot = None
        for idx in range(10):
            self._set_row(T0 + idx, float(idx))
            snapshot = self.plugin.build_data_snapshot(idx, 0.0)
        self.assertEqual(snapshot, (9, tuple(T0 + i for i in range(6, 10)), (6.0, 7.0, 8.0, 9.0)))

    def test_invalid_sample_is_reported_once(self) -> None:
        with self.assertLogs(self.plugin.logger, "WARNING") as logs:
            for idx in range(5):
                self.plugin.build_data_snapshot(idx, 0.0)
        self.assertEqual(len(logs.records), 1)

    def test_on_start_resets_window(self) -> None:
        self._set_row(T0, 1.0)
        self.plugin.build_data_snapshot(0, 0.0)
        self.plugin.on_start()
        self._set_row(T0 + 1.0, 2.0)
        self.assertIsNone(self.plugin.build_data_snapshot(1, 0.0))


@register_plugin("MotionRateTestSource")
class MotionRateTestSource(ScanPlugin):
    """
    Test-only stand-in for an energy readback detector PV.

    Emits ``SimEnergy`` with a synthetic timestamp so that the scan row gets
    ``SimEnergy`` and ``TS-SimEnergy``, as for a detector PV:
    10 Hz sampling, 3 units/s, 7.0 at the first point.
    """

    def get_headers(self, timestamps: bool) -> List[str]:
        del timestamps
        return ["SimEnergy"]

    def get_values(self, idx: int, pos: Any) -> List[Any]:
        del pos
        return [7.0 + 0.3 * idx]

    def on_scan_point(self, idx: int, pos: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [{"value": self.get_values(idx, pos)[0], "timestamp": T0 + 0.1 * idx}]


class TestMotionRateScan(unittest.TestCase):
    STEPS = 40
    WINDOW = 5

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        manifest = os.path.join(self.tmp, "manifest.txt")
        Path(manifest).touch()
        # Keep the manifest writer away from the user's manifest files.
        env = patch.dict(os.environ, {
            "KIWI_SCAN_DATA_DIR": self.tmp,
            "KIWI_SCAN_MANIFEST_FILE": manifest,
            "KIWI_SCAN_MANIFEST_STATE_FILE": os.path.join(self.tmp, "state"),
        })
        env.start()
        self.addCleanup(env.stop)

    def _config(self) -> ScanConfig:
        return ScanConfig.from_dict({
            "actuators": {"energy": {"type": "sim", "dwell_time": 0.005}},
            "detector_pvs": [],
            "scan_dimensions": [{"actuator": "energy", "start": 0, "stop": 1, "steps": self.STEPS}],
            "data_dir": self.tmp,
            "performance_report": True,
            "plugin_configs": [
                {"type": "MotionRateTestSource", "name": "source"},
                {"type": "MotionRatePlugin", "name": "rate",
                 "parameters": {"signal": "SimEnergy", "window": self.WINDOW}},
            ],
        })

    def test_scan_writes_motion_rate_columns(self) -> None:
        with LinearScan(self._config(), self.tmp) as scan:
            scan.execute()
            output = scan.get_output_file()
            plugin_samples = scan.performance.samples.get("plugins", [])
        self.assertIsNotNone(output)
        self.assertEqual(len(plugin_samples), self.STEPS)

        lines = Path(str(output)).read_text(encoding="utf-8").splitlines()
        header = lines[0].split("\t")
        rows = [dict(zip(header, line.split("\t"))) for line in lines[1:]]
        self.assertEqual(len(rows), self.STEPS)
        for name in MotionRatePlugin.HEADERS:
            self.assertIn(name, header)
        for line in lines[1:]:
            self.assertEqual(len(line.split("\t")), len(header))

        ready = [row for row in rows if row["MotionRateStatus"] == "ready"]
        self.assertTrue(ready, "no fit result reached the scan file")
        self.assertTrue(all(row["MotionRateStatus"] in ("pending", "ready") for row in rows))
        for row in ready:
            source_idx = int(float(row["MotionRateIndex"]))  # written as float
            first_idx = max(0, source_idx - self.WINDOW + 1)
            # POSIX timestamps near 1.7e9 carry ~2e-7 s rounding, so the
            # fit is exact only to about 1e-6 relative.
            self.assertAlmostEqual(float(row["MotionRateSlope"]), 3.0, delta=1e-4)
            self.assertAlmostEqual(float(row["MotionRateOffset"]), 7.0 + 0.3 * first_idx, delta=1e-4)


if __name__ == "__main__":
    unittest.main()
