# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import sys
import tempfile
import threading
import unittest
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kiwi_scan import test_support

# common.py imports the EPICS wrapper, but these unit tests only exercise
# BaseScan helpers.  Use the project's fake module on hosts without pyepics.
if "epics" not in sys.modules:
    sys.modules["epics"] = test_support.make_fake_epics_module()

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import SubscriptionConfig
from kiwi_scan.scan.common import BaseScan


class DummyScan(BaseScan):
    """Concrete BaseScan without the hardware-heavy BaseScan constructor."""

    def __init__(self):
        pass

    def execute(self):
        pass


class DummyDetector:
    def __init__(self, pvname, reading=None, error=None):
        self.pvname = pvname
        self.reading = reading
        self.error = error
        self.use_monitor = None

    def get_with_metadata(self, *, use_monitor):
        self.use_monitor = use_monitor
        if self.error is not None:
            raise self.error
        return self.reading


class TestBaseScanConfigurationHelpers(unittest.TestCase):
    def test_validate_and_filter_actuators_removes_unused_actuators(self):
        scan = DummyScan()
        scan.cfg = SimpleNamespace(
            scan_dimensions=[SimpleNamespace(actuator="energy")],
            actuators={"energy": object(), "unused": object()},
        )

        with self.assertLogs("kiwi_scan.scan.common", level="WARNING"):
            scan._validate_and_filter_actuators()

        self.assertEqual(list(scan.cfg.actuators), ["energy"])

    def test_validate_and_filter_actuators_requires_a_dimension(self):
        scan = DummyScan()
        scan.cfg = SimpleNamespace(scan_dimensions=[], actuators={})

        with self.assertRaisesRegex(ValueError, "at least one ScanDimension"):
            scan._validate_and_filter_actuators()

    def test_validate_and_filter_actuators_rejects_unknown_actuator(self):
        scan = DummyScan()
        scan.cfg = SimpleNamespace(
            scan_dimensions=[SimpleNamespace(actuator="missing")],
            actuators={"energy": object()},
        )

        with self.assertLogs(
            "kiwi_scan.scan.common", level="WARNING"
        ), self.assertRaisesRegex(ValueError, "unknown actuator: 'missing'"):
            scan._validate_and_filter_actuators()

    def test_position_sync_uses_primary_actuator_readback(self):
        scan = DummyScan()
        scan.scan_dimensions = [SimpleNamespace(actuator="energy")]

        self.assertTrue(
            scan._is_position_sync_subscription(
                SubscriptionConfig(
                    name="energy_sync",
                    role="sync",
                    actuator="energy",
                    source="RBV",
                )
            )
        )
        self.assertFalse(
            scan._is_position_sync_subscription(
                SubscriptionConfig(
                    name="energy_setpoint",
                    role="sync",
                    actuator="energy",
                    source="setpoint",
                )
            )
        )
        self.assertFalse(
            scan._is_position_sync_subscription(
                SubscriptionConfig(
                    name="other_sync",
                    role="sync",
                    actuator="other",
                    source="rbv",
                )
            )
        )

    def test_apply_sample_rate_sets_rate_and_period(self):
        scan = DummyScan()

        scan._apply_sample_rate("4")

        self.assertEqual(scan.sample_rate_hz, 4.0)
        self.assertEqual(scan.sampletime, 0.25)

    def test_apply_sample_rate_uses_default_and_normalizes_negative_rate(self):
        scan = DummyScan()

        scan._apply_sample_rate(None)
        self.assertEqual((scan.sample_rate_hz, scan.sampletime), (1.0, 1.0))

        with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
            scan._apply_sample_rate(-5)
        self.assertEqual((scan.sample_rate_hz, scan.sampletime), (5.0, 0.2))

    def test_set_samplerate_updates_sync_timer_period(self):
        scan = DummyScan()
        scan.cfg = SimpleNamespace(sample_rate_hz=2.0)
        scan.sync_controller = MagicMock()

        scan.set_samplerate()

        scan.sync_controller.set_timer_period.assert_called_once_with(0.5)


class TestBaseScanOutputHelpers(unittest.TestCase):
    def setUp(self):
        self.scan = DummyScan()
        self.scan.detector_pvs = [
            SimpleNamespace(pvname="DET:A"),
            SimpleNamespace(pvname="DET:B"),
        ]
        self.scan.plugins = []
        self.scan.include_timestamps = True
        self.scan._data_column_providers = []
        self.scan._current_row_cache = {}
        self.scan._last_point = {}

    def test_build_output_headers_preserves_file_column_order(self):
        provider = MagicMock()
        provider.get_headers.return_value = ["mean", "std"]
        plugin = MagicMock()
        plugin.get_headers.return_value = ["plugin_value", "TS-plugin_value"]
        self.scan._data_column_providers = [provider]
        self.scan.plugins = [plugin]

        headers = self.scan.build_output_headers(include_timestamps=True)

        self.assertEqual(
            headers,
            [
                "Position",
                "mean",
                "std",
                "TS-ISO8601",
                "DET:A",
                "TS-ISO8601-DET:A",
                "DET:B",
                "TS-ISO8601-DET:B",
                "plugin_value",
                "TS-plugin_value",
            ],
        )
        provider.get_headers.assert_called_once_with(True)
        plugin.get_headers.assert_called_once_with(True)

    def test_build_output_row_values_handles_metadata_and_scalars(self):
        with patch.object(
            self.scan,
            "_timestamp_to_iso",
            side_effect=lambda value: "" if value is None else "detector-time",
        ):
            row = self.scan.build_output_row_values(
                10.0,
                [
                    {"value": 1.5, "timestamp": 123.0},
                    2.5,
                ],
                include_timestamps=True,
                line_ts_iso="line-time",
                provider_values=[9.0],
            )

        self.assertEqual(
            row,
            [10.0, 9.0, "line-time", 1.5, "detector-time", 2.5, ""],
        )

    def test_build_output_row_values_can_omit_detector_timestamps(self):
        row = self.scan.build_output_row_values(
            10.0,
            [{"value": 1.5, "timestamp": 123.0}, 2.5],
            include_timestamps=False,
            line_ts_iso="line-time",
            provider_values=[],
        )

        self.assertEqual(row, [10.0, "line-time", 1.5, 2.5])

    def test_timestamp_to_iso_handles_missing_invalid_and_epoch_values(self):
        self.assertEqual(self.scan._timestamp_to_iso(None), "")
        self.assertEqual(self.scan._timestamp_to_iso("invalid"), "invalid")
        converted = self.scan._timestamp_to_iso(0)
        self.assertEqual(datetime.fromisoformat(converted).timestamp(), 0)

    def test_format_scan_value_handles_numbers_text_and_none(self):
        self.assertEqual(self.scan._format_scan_value(None), "")
        self.assertEqual(self.scan._format_scan_value(1.25), "1.250000000000e+00")
        self.assertEqual(self.scan._format_scan_value("ready"), "ready")

    def test_update_current_row_cache_flattens_value_mappings(self):
        row = self.scan.update_current_row_cache(
            idx=3,
            pos="4.5",
            values=[{"value": 11.0, "timestamp": 0}, 12.0],
            headers=["DET:A", "DET:B"],
            provider_values=[],
            line_ts_iso="line-time",
        )

        self.assertEqual(row["idx"], 3)
        self.assertEqual(row["pos"], "4.5")
        self.assertEqual(row["Position"], 4.5)
        self.assertEqual(row["TS-ISO8601"], "line-time")
        self.assertEqual(row["DET:A"], 11.0)
        self.assertEqual(row["DET:B"], 12.0)
        self.assertIn("TS-ISO8601-DET:A", row)

    def test_update_current_row_cache_can_keep_existing_values(self):
        self.scan._current_row_cache = {"old": 1}

        row = self.scan.update_current_row_cache(
            idx=2,
            pos=None,
            values=[],
            provider_values=[],
            clear=False,
        )

        self.assertEqual(row["old"], 1)
        self.assertIsNone(row["Position"])

    def test_extend_and_get_current_row_cache_use_defensive_copies(self):
        self.scan._current_row_cache = {"Position": 1.0}

        row = self.scan.extend_current_row_cache(
            ["plugin"],
            [{"value": 8.0, "timestamp": 0}],
        )
        returned = self.scan.get_current_row_cache()
        returned["Position"] = 99.0

        self.assertEqual(row["plugin"], 8.0)
        self.assertIn("TS-plugin", row)
        self.assertEqual(self.scan.get_current_row_value("plugin"), 8.0)
        self.assertEqual(
            self.scan.get_current_row_value("missing", "fallback"), "fallback"
        )
        self.assertEqual(self.scan.get_current_row_value("Position"), 1.0)

    def test_get_value_supports_scalar_metadata_and_defaults(self):
        metadata = {"value": 12.5, "timestamp": 100.0}
        self.scan._last_point = {"DET:A": metadata, "state": "ready"}

        self.assertEqual(self.scan.get_value("DET:A"), 12.5)
        self.assertIs(self.scan.get_value("DET:A", with_metadata=True), metadata)
        self.assertEqual(self.scan.get_value("state"), "ready")
        self.assertEqual(self.scan.get_value("missing", default=-1), -1)
        self.assertEqual(self.scan.get_last_point_keys(), ["DET:A", "state"])

    def test_column_provider_failures_do_not_hide_other_providers(self):
        broken = MagicMock()
        broken.get_headers.side_effect = RuntimeError("broken headers")
        broken.get_values.side_effect = RuntimeError("broken values")
        working = MagicMock()
        working.get_headers.return_value = ["mean"]
        working.get_values.return_value = [3.0]
        self.scan._data_column_providers = [broken, working]

        with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
            self.assertEqual(self.scan._get_data_column_headers(False), ["mean"])
            self.assertEqual(self.scan._get_data_column_values(), [3.0])


class TestBaseScanRuntimeHelpers(unittest.TestCase):
    def test_time_block_only_records_when_performance_reporting_is_enabled(self):
        scan = DummyScan()
        scan._perf = defaultdict(list)
        scan._perf_enabled = False

        with scan._time_block("read"):
            pass
        self.assertEqual(scan._perf["read"], [])

        scan._perf_enabled = True
        with patch(
            "kiwi_scan.scan.common.time.perf_counter",
            side_effect=[10.0, 10.25],
        ), scan._time_block("read", idx=2):
            pass
        self.assertEqual(scan._perf["read"], [0.25])

    def test_manager_helpers_delegate_to_their_managers(self):
        scan = DummyScan()
        scan.subscription_manager = MagicMock()
        scan.trigger_manager = MagicMock()
        scan.sync_controller = MagicMock()
        scan.sync_controller.wait.return_value = True
        handler = object()
        stop_event = threading.Event()

        scan.register_subscription_role("sync", handler)
        scan._start_subscriptions()
        scan._clear_subscriptions()
        scan._fire_triggers("before")
        scan._arm_sync_controller()

        self.assertTrue(scan._wait_for_sync(stop_event))
        scan.subscription_manager.register_role.assert_called_once_with("sync", handler)
        scan.subscription_manager.start.assert_called_once_with()
        scan.subscription_manager.stop.assert_called_once_with()
        scan.trigger_manager.fire.assert_called_once_with("before")
        scan.sync_controller.arm.assert_called_once_with()
        scan.sync_controller.wait.assert_called_once_with(stop_event=stop_event)

    def test_read_detectors_keeps_column_alignment_on_failures(self):
        good = DummyDetector("DET:GOOD", {"value": 5.0})
        missing = DummyDetector("DET:NONE", None)
        broken = DummyDetector("DET:BAD", error=RuntimeError("read failed"))
        scan = DummyScan()
        scan.detector_pvs = [good, missing, broken]
        scan.detector_pvs_monitor = True

        with self.assertLogs("kiwi_scan.scan.common", level="WARNING"):
            readings = scan.read_detectors()

        self.assertEqual(readings, [{"value": 5.0}, None, None])
        self.assertTrue(good.use_monitor)
        self.assertTrue(missing.use_monitor)
        self.assertTrue(broken.use_monitor)

    def test_move_scan_step_moves_configured_actuators(self):
        scan = DummyScan()
        scan._stop_requested = threading.Event()
        scan._daq_is_on = True
        energy = MagicMock()
        gap = MagicMock()
        unused = MagicMock()
        scan.actuators = {
            "energy": energy,
            "gap": gap,
            "unused": unused,
        }

        completed = scan._move_scan_step(
            {
                "energy": [1.0, 2.0],
                "gap": [10.0, 20.0],
            },
            1,
        )

        self.assertTrue(completed)
        self.assertFalse(scan._daq_is_on)
        energy.move.assert_called_once_with(2.0)
        gap.move.assert_called_once_with(20.0)
        unused.move.assert_not_called()

    def test_move_scan_step_stops_before_remaining_moves(self):
        scan = DummyScan()
        scan._stop_requested = threading.Event()
        scan._daq_is_on = True
        first = MagicMock()
        second = MagicMock()
        first.move.side_effect = lambda _target: scan._stop_requested.set()
        scan.actuators = {"first": first, "second": second}

        completed = scan._move_scan_step(
            {"first": [1.0], "second": [2.0]},
            0,
        )

        self.assertFalse(completed)
        first.move.assert_called_once_with(1.0)
        second.move.assert_not_called()

    def test_acquire_scan_point_processes_and_publishes_values(self):
        scan = DummyScan()
        scan._perf_enabled = False
        scan._stop_requested = threading.Event()
        scan._reset_data_column_provider_windows = MagicMock()
        scan._fire_triggers = MagicMock()
        scan.integration_time = 0.0
        detector_values = [{"value": 3.0}]
        scan.read_detectors = MagicMock(return_value=detector_values)
        scan.update_current_row_cache = MagicMock()
        plugin = MagicMock()
        plugin.on_scan_point.return_value = [{"value": 4.0}]
        plugin.get_headers.return_value = ["PluginValue"]
        scan.plugins = [plugin]
        scan.extend_current_row_cache = MagicMock()
        scan.include_timestamps = False
        scan.save_to_file = MagicMock(return_value=[2.0, 3.0, 4.0])
        monitor = MagicMock()

        completed = scan._acquire_scan_point(5, 2.0, monitor)

        self.assertTrue(completed)
        self.assertTrue(scan._daq_is_on)
        self.assertEqual(scan._position, 2.0)
        scan._reset_data_column_provider_windows.assert_called_once_with()
        scan._fire_triggers.assert_called_once_with("on_point")
        scan.update_current_row_cache.assert_called_once_with(
            idx=5,
            pos=2.0,
            values=detector_values,
        )
        plugin.on_scan_point.assert_called_once_with(5, 2.0)
        scan.extend_current_row_cache.assert_called_once_with(
            ["PluginValue"],
            [{"value": 4.0}],
        )
        scan.save_to_file.assert_called_once_with(
            2.0,
            [{"value": 3.0}, {"value": 4.0}],
            False,
        )
        monitor.update.assert_called_once_with([2.0, 3.0, 4.0])

    def test_acquire_scan_point_stops_during_integration(self):
        scan = DummyScan()
        scan._perf_enabled = False
        scan._stop_requested = threading.Event()
        scan._stop_requested.set()
        scan._reset_data_column_provider_windows = MagicMock()
        scan._fire_triggers = MagicMock()
        scan.integration_time = 1.0
        scan.read_detectors = MagicMock()

        completed = scan._acquire_scan_point(0, 1.0, None)

        self.assertFalse(completed)
        scan.read_detectors.assert_not_called()

    def test_scan_cleanup_continues_and_preserves_original_error(self):
        scan = DummyScan()
        scan._perf_enabled = False
        scan._stop_requested = threading.Event()
        scan.write_header_to_output_file = MagicMock(
            side_effect=RuntimeError("scan failed")
        )
        scan._end_plugins = MagicMock(
            side_effect=RuntimeError("plugin stop failed")
        )
        scan._close_plugins = MagicMock()
        scan._stop_metadata_monitor = MagicMock(
            side_effect=RuntimeError("metadata stop failed")
        )
        scan._stop_subscriptions = MagicMock(
            side_effect=RuntimeError("subscription stop failed")
        )
        scan._perf_report = MagicMock()
        monitor = MagicMock()
        monitor.close.side_effect = RuntimeError("monitor close failed")

        with patch(
            "kiwi_scan.scan.common.ensure_ca_context"
        ), self.assertLogs(
            "kiwi_scan.scan.common",
            level="ERROR",
        ), self.assertRaisesRegex(RuntimeError, "scan failed"):
            scan.scan({}, monitor)

        scan._end_plugins.assert_called_once_with()
        scan._close_plugins.assert_called_once_with()
        scan._stop_metadata_monitor.assert_called_once_with()
        scan._stop_subscriptions.assert_called_once_with()
        monitor.close.assert_called_once_with()
        scan._perf_report.assert_called_once_with()
        self.assertFalse(scan.busyflag)

    def test_metadata_drop_count_supports_method_attribute_and_bad_values(self):
        scan = DummyScan()

        scan._meta_mon = None
        self.assertEqual(scan.get_metadata_queue_drop_count(), 0)

        scan._meta_mon = SimpleNamespace(get_drop_count=lambda: "7")
        self.assertEqual(scan.get_metadata_queue_drop_count(), 7)

        scan._meta_mon = SimpleNamespace(dropped_events=4)
        self.assertEqual(scan.get_metadata_queue_drop_count(), 4)

        scan._meta_mon = SimpleNamespace(dropped_events="bad")
        self.assertEqual(scan.get_metadata_queue_drop_count(), 0)

    def test_metadata_monitor_start_and_stop_are_idempotent(self):
        scan = DummyScan()
        scan._data_writer_lock = threading.RLock()
        scan._data_writing_enabled = True
        scan._meta_mon_started = False
        scan._meta_mon = MagicMock()

        scan._start_metadata_monitor()
        scan._start_metadata_monitor()
        self.assertTrue(scan._meta_mon_started)
        scan._meta_mon.start.assert_called_once_with()

        scan._stop_metadata_monitor()
        scan._stop_metadata_monitor()
        self.assertFalse(scan._meta_mon_started)
        scan._meta_mon.stop.assert_called_once_with()

    def test_metadata_monitor_failures_leave_consistent_started_state(self):
        scan = DummyScan()
        scan._data_writer_lock = threading.RLock()
        scan._data_writing_enabled = True
        scan._meta_mon_started = False
        scan._meta_mon = MagicMock()
        scan._meta_mon.start.side_effect = RuntimeError("start failed")

        with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
            scan._start_metadata_monitor()
        self.assertFalse(scan._meta_mon_started)

        scan._meta_mon_started = True
        scan._meta_mon.stop.side_effect = RuntimeError("stop failed")
        with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
            scan._stop_metadata_monitor()
        self.assertFalse(scan._meta_mon_started)

    def test_prepare_positions_filters_pads_and_does_not_modify_input(self):
        scan = DummyScan()
        scan.actuators = {
            "energy": SimpleNamespace(backlash=0.0),
            "gap": SimpleNamespace(backlash=0.0),
        }
        positions = {"energy": [1.0, 2.0, 3.0], "gap": [10.0], "empty": []}

        prepared, overshoot = scan._prepare_positions(positions)

        self.assertEqual(
            prepared,
            {"energy": [1.0, 2.0, 3.0], "gap": [10.0, 10.0, 10.0]},
        )
        self.assertFalse(overshoot)
        self.assertEqual(positions["gap"], [10.0])

    def test_prepare_positions_adds_direction_dependent_backlash_point(self):
        scan = DummyScan()
        scan.actuators = {
            "up": SimpleNamespace(backlash=0.5),
            "down": SimpleNamespace(backlash=0.25),
            "plain": SimpleNamespace(backlash=0.0),
        }

        prepared, overshoot = scan._prepare_positions(
            {
                "up": [10.0, 20.0],
                "down": [20.0, 10.0],
                "plain": [3.0, 4.0],
            }
        )

        self.assertTrue(overshoot)
        self.assertEqual(prepared["up"], [9.5, 10.0, 20.0])
        self.assertEqual(prepared["down"], [20.25, 20.0, 10.0])
        self.assertEqual(prepared["plain"], [3.0, 3.0, 4.0])

    def test_generate_and_create_file_uses_suffix_after_collision(self):
        scan = DummyScan()
        scan._output_timestamp = "20260825160000"

        with tempfile.TemporaryDirectory() as tmp:
            scan.data_dir = tmp
            first = Path(tmp) / "scan-20260825160000.txt"
            first.touch()
            with patch(
                "kiwi_scan.scan.common.random.choices",
                return_value=list("ABC123"),
            ):
                result = scan.generate_and_create_file("scan.txt")

            self.assertEqual(result, str(Path(tmp) / "scan-20260825160000_ABC123.txt"))
            self.assertTrue(Path(result).is_file())

    def test_data_writing_toggle_starts_and_stops_metadata_monitor(self):
        scan = DummyScan()
        scan._data_writer_lock = threading.RLock()
        scan._data_writing_enabled = True
        scan.busyflag = True
        scan._start_metadata_monitor = MagicMock()
        scan._stop_metadata_monitor = MagicMock()

        scan.set_data_writing_enabled(False)
        scan._stop_metadata_monitor.assert_called_once_with()

        scan.set_data_writing_enabled(True)
        scan._start_metadata_monitor.assert_called_once_with()

    def test_ensure_output_file_exists_is_lazy_and_idempotent(self):
        scan = DummyScan()
        scan._data_writer_lock = threading.RLock()
        scan._data_writing_enabled = True
        scan._requested_output_file = "scan.txt"
        scan.output_file = None
        scan.generate_and_create_file = MagicMock(return_value="generated.txt")

        self.assertEqual(scan._ensure_output_file_exists(), "generated.txt")
        self.assertEqual(scan._ensure_output_file_exists(), "generated.txt")
        scan.generate_and_create_file.assert_called_once_with("scan.txt")

        scan._data_writing_enabled = False
        self.assertIsNone(scan._ensure_output_file_exists())

    def test_write_header_to_output_file_writes_header_once(self):
        scan = DummyScan()
        scan._data_writer_lock = threading.RLock()
        scan._data_writing_enabled = True
        scan._data_header_written = False
        scan.include_timestamps = False
        scan.build_output_headers = MagicMock(return_value=["Position", "TS-ISO8601"])

        with tempfile.TemporaryDirectory() as tmp:
            scan.output_file = str(Path(tmp) / "scan.txt")
            scan._ensure_output_file_exists = MagicMock(return_value=scan.output_file)

            scan.write_header_to_output_file()
            scan.write_header_to_output_file()

            content = Path(scan.output_file).read_text(encoding="utf-8")

        self.assertEqual(content, "Position\tTS-ISO8601\n")
        self.assertTrue(scan._data_header_written)
        scan.build_output_headers.assert_called_once_with(False)

    def test_save_to_file_updates_cache_when_file_writing_is_disabled(self):
        scan = DummyScan()
        scan._data_writer_lock = threading.RLock()
        scan._data_writing_enabled = False
        scan._data_header_written = False
        scan._data_column_providers = []
        scan.detector_pvs = [SimpleNamespace(pvname="DET:A")]
        scan.plugins = []
        scan._last_point = {}

        row = scan.save_to_file(
            2.0,
            [{"value": 8.0, "timestamp": 0}],
            include_timestamps=False,
        )

        self.assertEqual(row[0], 2.0)
        self.assertEqual(row[-1], 8.0)
        self.assertEqual(scan.get_value("Position"), 2.0)
        self.assertEqual(scan.get_value("DET:A"), 8.0)

    def test_column_provider_window_helpers_are_best_effort(self):
        scan = DummyScan()
        working = MagicMock()
        broken = MagicMock()
        broken.update_last_point.side_effect = RuntimeError("update failed")
        broken.reset_window.side_effect = RuntimeError("reset failed")
        without_reset = SimpleNamespace(update_last_point=MagicMock())
        scan._data_column_providers = [working, broken, without_reset]
        last = {"Position": 1.0}

        with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
            scan._update_data_column_provider_cache(last, True)
            scan._reset_data_column_provider_windows()

        working.update_last_point.assert_called_once_with(last, True)
        broken.update_last_point.assert_called_once_with(last, True)
        without_reset.update_last_point.assert_called_once_with(last, True)
        working.reset_window.assert_called_once_with()
        broken.reset_window.assert_called_once_with()

    def test_get_actuator_reports_errors_and_get_actuators_returns_copy(self):
        scan = DummyScan()
        energy = object()
        scan.actuators = {"energy": energy, "gap": object()}

        self.assertIs(scan.get_actuator("energy"), energy)
        copied = scan.get_actuators()
        copied.clear()
        self.assertEqual(set(scan.actuators), {"energy", "gap"})

        with self.assertRaisesRegex(KeyError, "Available actuators: energy, gap"):
            scan.get_actuator("missing")

        scan.actuators["none"] = None
        with self.assertRaisesRegex(KeyError, "exists but is None"):
            scan.get_actuator("none")

    def test_stop_is_best_effort_and_wakes_waiters(self):
        scan = DummyScan()
        scan._stop_requested = threading.Event()
        scan._daq_is_on = True
        scan._tick_cond = threading.Condition()
        scan._tick_seq = 2
        working = MagicMock()
        broken = MagicMock()
        broken.stop.side_effect = RuntimeError("cannot stop")
        scan.actuators = {"working": working, "broken": broken}
        scan.sync_controller = MagicMock()

        with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
            scan.stop()

        self.assertTrue(scan._stop_requested.is_set())
        self.assertFalse(scan._daq_is_on)
        self.assertEqual(scan._tick_seq, 3)
        working.stop.assert_called_once_with()
        broken.stop.assert_called_once_with()
        scan.sync_controller.wake.assert_called_once_with()

    def test_parallel_wait_supports_current_and_legacy_actuator_signatures(self):
        scan = DummyScan()
        scan._stop_requested = threading.Event()
        calls = []

        class CurrentActuator:
            def wait_until_done(self, target, stop_event):
                calls.append(("current", target, stop_event))

        class LegacyActuator:
            def wait_until_done(self, target):
                calls.append(("legacy", target))

        scan._parallel_wait(
            {"current": CurrentActuator(), "legacy": LegacyActuator()},
            {"current": 1.0, "legacy": 2.0},
        )

        self.assertIn(("current", 1.0, scan._stop_requested), calls)
        self.assertIn(("legacy", 2.0), calls)

    def test_plugin_lifecycle_is_best_effort(self):
        scan = DummyScan()
        working = MagicMock(name="working")
        broken = MagicMock(name="broken")
        broken.on_start.side_effect = RuntimeError("start failed")
        broken.on_end.side_effect = RuntimeError("end failed")
        broken.close.side_effect = RuntimeError("close failed")
        no_close = SimpleNamespace(
            name="no-close",
            on_start=MagicMock(),
            on_end=MagicMock(),
        )
        scan.plugins = [broken, working, no_close]

        with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
            scan._start_plugins()
            scan._end_plugins()
            scan._close_plugins()

        working.on_start.assert_called_once_with()
        working.on_end.assert_called_once_with()
        working.close.assert_called_once_with()
        no_close.on_start.assert_called_once_with()
        no_close.on_end.assert_called_once_with()

    def test_stop_pv_is_read_and_reset(self):
        scan = DummyScan()
        scan.stop_pv = MagicMock()
        scan.stop_pv.get.return_value = 1

        self.assertEqual(scan.get_stop_pv(), 1)
        scan.stop_pv.put.assert_called_once_with(0)

        scan.stop_pv = None
        self.assertIsNone(scan.get_stop_pv())

    def test_event_handlers_store_and_queue_events(self):
        scan = DummyScan()
        scan._last_status = None
        scan._last_heartbeat = None
        scan._tick_cond = threading.Condition()
        scan._tick_seq = 0
        scan._trigger_q = MagicMock()
        scan._plugin_q = MagicMock()
        event = PvEvent("TEST:PV", 5.0)
        subscription = SubscriptionConfig(
            name="test_subscription",
            role="status",
            pv="TEST:PV",
        )

        scan._on_status_event(event, subscription)
        scan._on_heartbeat_event(event, subscription)
        scan._on_trigger_event(event, subscription)
        scan._on_plugin_event(event, subscription)

        self.assertIs(scan._last_status, event)
        self.assertIs(scan._last_heartbeat, event)
        self.assertEqual(scan._tick_seq, 1)
        scan._trigger_q.put.assert_called_once_with(event)
        scan._plugin_q.put.assert_called_once_with(event)

    def test_sync_event_updates_controller_and_position(self):
        scan = DummyScan()
        scan.scan_dimensions = [SimpleNamespace(actuator="energy")]
        scan.sync_controller = MagicMock()
        scan._last_sync = None
        scan._position = None
        scan._position_sync_subscription_set = False

        event = PvEvent("ENERGY:RBV", "12.5")
        subscription = SubscriptionConfig(
            name="energy_sync",
            role="sync",
            actuator="energy",
            source="rbv",
        )

        scan._on_sync_event(event, subscription)

        self.assertIs(scan._last_sync, event)
        scan.sync_controller.note_event.assert_called_once_with("energy_sync")
        self.assertEqual(scan._position, 12.5)
        self.assertTrue(scan._position_sync_subscription_set)

    def test_trigger_worker_fires_monitor_triggers(self):
        scan = DummyScan()
        scan._trigger_worker_stop = threading.Event()
        event = PvEvent("TEST:TRIGGER", 1)

        def get_event():
            scan._trigger_worker_stop.set()
            return event

        scan._trigger_q = SimpleNamespace(get=get_event)
        scan._fire_triggers = MagicMock()

        scan._trigger_worker_loop()

        scan._fire_triggers.assert_called_once_with("monitor")

    def test_plugin_worker_isolates_plugin_failures(self):
        scan = DummyScan()
        scan._plugin_worker_stop = threading.Event()
        event = PvEvent("TEST:PLUGIN", 2)

        def get_event():
            scan._plugin_worker_stop.set()
            return event

        broken_hook = MagicMock(side_effect=RuntimeError("plugin failed"))
        working_hook = MagicMock()
        scan._plugin_q = SimpleNamespace(get=get_event)
        scan.plugins = [
            SimpleNamespace(name="broken", on_monitor=broken_hook),
            SimpleNamespace(name="working", on_monitor=working_hook),
        ]

        with self.assertLogs("kiwi_scan.scan.common", level="ERROR") as logs:
            scan._plugin_worker_loop()

        broken_hook.assert_called_once_with(event)
        working_hook.assert_called_once_with(event)
        self.assertIn("Plugin 'broken' failed", "\n".join(logs.output))

    def test_stop_event_only_stops_an_active_scan(self):
        scan = DummyScan()
        scan._stop_requested = threading.Event()
        scan._tick_cond = threading.Condition()
        scan.sync_controller = MagicMock()
        actuator = MagicMock()
        scan.actuators = {"energy": actuator}
        event = PvEvent("TEST:STOP", 1)
        subscription = SubscriptionConfig(
            name="stop_subscription",
            role="stop",
            pv="TEST:STOP",
        )

        scan.busyflag = False
        scan._on_stop_event(event, subscription)
        self.assertFalse(scan._stop_requested.is_set())
        actuator.stop.assert_not_called()

        scan.busyflag = True
        scan._on_stop_event(event, subscription)
        self.assertTrue(scan._stop_requested.is_set())
        scan.sync_controller.wake.assert_called_once_with()
        actuator.stop.assert_called_once_with()

    def test_wait_for_tick_returns_false_when_stopped_or_timed_out(self):
        scan = DummyScan()
        scan._tick_cond = threading.Condition()
        scan._tick_seq = 0
        scan._stop_requested = threading.Event()

        self.assertFalse(scan._wait_for_tick_or_timeout(0))

        scan._stop_requested.set()
        self.assertFalse(scan._wait_for_tick_or_timeout(1.0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
