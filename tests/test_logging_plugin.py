# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import math
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from kiwi_scan import test_support

# Install a small pyepics replacement before importing the logging plugin.
if "epics" not in sys.modules:
    sys.modules["epics"] = test_support.make_fake_epics_module()

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.plugin_concrete.logging import LoggingPlugin


class FakeActuator:
    def __init__(
        self,
        config=None,
        *,
        rbv=10.0,
        cmdv=12.0,
        moving=False,
        ready=True,
    ):
        self.config = config or ActuatorConfig()
        self.rbv = rbv
        self.cmdv = cmdv
        self.moving = moving
        self.ready = ready

    def is_moving(self):
        return self.moving

    def is_ready(self):
        return self.ready


class FakeScan:
    def __init__(self, actuators=None, actuator_configs=None):
        self.actuators = actuators or {}
        self.cfg = SimpleNamespace(actuators=actuator_configs or {})

    def get_actuators(self):
        return self.actuators

    def get_actuator(self, name):
        return self.actuators[name]


class LoggingPluginTestCase(unittest.TestCase):
    def setUp(self):
        # Unit tests must not create plugin log files or logging handlers.
        self.logging_patch = patch.object(LoggingPlugin, "_init_logging")
        self.logging_patch.start()
        self.addCleanup(self.logging_patch.stop)

    def make_plugin(self, scan=None, **parameters):
        defaults = {
            "enable_alarm_trace": False,
            "enable_point_timing": False,
            "enable_actuator_trace": False,
        }
        defaults.update(parameters)
        plugin = LoggingPlugin("test-logging", defaults, scan)
        # ScanPlugin intentionally stores only a weak proxy. Keep the test
        # double alive for tests that construct it inline.
        plugin._test_scan_owner = scan
        return plugin

    @staticmethod
    def make_scan(*, config=None, **actuator_values):
        config = config or ActuatorConfig(
            pv="MOTOR:SET",
            rb_pv="MOTOR:RBV",
            cmd_pv="MOTOR:CMD",
            status_pv="MOTOR:STATUS",
            ready_value=0,
        )
        actuator = FakeActuator(config, **actuator_values)
        return FakeScan({"motor": actuator}, {"motor": config})


class TestLoggingPluginInitialization(LoggingPluginTestCase):
    def test_defaults_enable_all_features_and_discover_actuators(self):
        scan = self.make_scan()

        plugin = LoggingPlugin("diagnostics", {}, scan)

        self.assertTrue(plugin.enable_alarm_trace)
        self.assertTrue(plugin.enable_point_timing)
        self.assertTrue(plugin.enable_actuator_trace)
        self.assertEqual(plugin.actuator_trace_names, ["motor"])
        self.assertEqual(plugin.DEFAULT_LOG_FILE, "logging_plugin.log")

    def test_configured_actuator_names_accept_string_and_iterables(self):
        plugin = self.make_plugin()

        self.assertEqual(plugin._resolve_actuator_trace_names("motor"), ["motor"])
        self.assertEqual(
            plugin._resolve_actuator_trace_names(["motor", 2]),
            ["motor", "2"],
        )
        self.assertEqual(
            sorted(plugin._resolve_actuator_trace_names(("motor", "slit"))),
            ["motor", "slit"],
        )

    def test_invalid_actuator_configuration_falls_back_to_scan_actuators(self):
        scan = self.make_scan()
        plugin = self.make_plugin(scan)

        names = plugin._resolve_actuator_trace_names({"invalid": "mapping"})

        self.assertEqual(names, ["motor"])

    def test_failure_to_get_actuators_is_non_fatal(self):
        scan = Mock()
        scan.get_actuators.side_effect = RuntimeError("unavailable")
        scan.cfg = SimpleNamespace(actuators={})

        plugin = self.make_plugin(scan)

        self.assertEqual(plugin.actuator_trace_names, [])

    @patch("kiwi_scan.plugin_concrete.logging.EpicsPV")
    def test_alarm_pv_creation_failure_is_non_fatal(self, epics_pv_class):
        good_pv = Mock()
        epics_pv_class.side_effect = [good_pv, RuntimeError("disconnected")]

        plugin = self.make_plugin(alarm_log=["GOOD:PV", "BAD:PV"])

        self.assertEqual(plugin.monitored_pvs, {"GOOD:PV": good_pv})
        self.assertEqual(epics_pv_class.call_count, 2)

    def test_actuator_pv_map_contains_all_supported_sources(self):
        scan = self.make_scan()

        plugin = self.make_plugin(
            scan,
            enable_actuator_trace=True,
            actuator_trace="motor",
        )

        self.assertEqual(
            plugin._actuator_pv_map,
            {
                "MOTOR:RBV": ("motor", "rbv"),
                "MOTOR:CMD": ("motor", "cmd"),
                "MOTOR:SET": ("motor", "cmd"),
                "MOTOR:STATUS": ("motor", "status"),
            },
        )


class TestLoggingPluginAlarmTrace(LoggingPluginTestCase):
    def test_empty_alarm_list_reports_no_alarm(self):
        plugin = self.make_plugin(enable_alarm_trace=True)

        self.assertEqual(
            plugin._collect_alarm_trace(3),
            ["NO_ALARM", "", None, None],
        )

    def test_worst_alarm_is_returned(self):
        no_alarm = Mock()
        no_alarm.get_with_metadata.return_value = {
            "value": 1.0,
            "severity": 0,
            "status": 0,
        }
        major_alarm = Mock()
        major_alarm.get_with_metadata.return_value = {
            "value": 2.0,
            "severity": 2,
            "status": 7,
        }
        plugin = self.make_plugin(enable_alarm_trace=True)
        plugin.monitored_pvs = {
            "NO:ALARM": no_alarm,
            "MAJOR:ALARM": major_alarm,
        }

        result = plugin._collect_alarm_trace(4)

        self.assertEqual(result, ["MAJOR", "MAJOR:ALARM", 2, 7])
        no_alarm.get_with_metadata.assert_called_once_with(
            use_monitor=True,
            full=True,
        )

    def test_missing_metadata_is_reported_without_raising(self):
        pv = Mock()
        pv.get_with_metadata.return_value = None
        plugin = self.make_plugin(enable_alarm_trace=True)
        plugin.monitored_pvs = {"MISSING:PV": pv}

        result = plugin._collect_alarm_trace(5)

        # The current AlarmState spelling is intentionally not fixed here.
        self.assertEqual(result[1:], ["MISSING:PV", None, "NO_METADATA"])

    def test_alarm_read_failure_is_reported_without_raising(self):
        pv = Mock()
        pv.get_with_metadata.side_effect = RuntimeError("read failed")
        plugin = self.make_plugin(enable_alarm_trace=True)
        plugin.monitored_pvs = {"BROKEN:PV": pv}

        result = plugin._collect_alarm_trace(6)

        # The current AlarmState spelling is intentionally not fixed here.
        self.assertEqual(result[1:], ["BROKEN:PV", None, "read failed"])


class TestLoggingPluginPointTiming(LoggingPluginTestCase):
    @patch(
        "kiwi_scan.plugin_concrete.logging.time.time",
        side_effect=[100.0, 100.75],
    )
    def test_first_point_has_no_delta_and_second_has_elapsed_time(self, _time):
        plugin = self.make_plugin(enable_point_timing=True)

        self.assertEqual(plugin._collect_point_timing(), [None])
        self.assertEqual(plugin._collect_point_timing(), [0.75])

    @patch(
        "kiwi_scan.plugin_concrete.logging.time.time",
        side_effect=[100.0, 99.0],
    )
    def test_negative_point_delta_is_clamped_to_zero(self, _time):
        plugin = self.make_plugin(enable_point_timing=True)

        plugin._collect_point_timing()

        self.assertEqual(plugin._collect_point_timing(), [0.0])


class TestLoggingPluginActuatorHelpers(LoggingPluginTestCase):
    def test_event_timestamp_prefers_timestamp(self):
        event = PvEvent(
            pvname="MOTOR:RBV",
            value=1,
            timestamp=12.5,
            posixseconds=99,
            nanoseconds=500,
        )

        self.assertEqual(LoggingPlugin._event_timestamp(event), 12.5)

    def test_event_timestamp_falls_back_to_posix_fields(self):
        event = PvEvent(
            pvname="MOTOR:RBV",
            value=1,
            timestamp="invalid",
            posixseconds=12,
            nanoseconds=500_000_000,
        )

        self.assertEqual(LoggingPlugin._event_timestamp(event), 12.5)

    def test_event_timestamp_returns_none_for_invalid_fields(self):
        event = PvEvent(
            pvname="MOTOR:RBV",
            value=1,
            timestamp="invalid",
            posixseconds="also invalid",
        )

        self.assertIsNone(LoggingPlugin._event_timestamp(event))

    def test_ready_state_uses_bitmask(self):
        config = ActuatorConfig(ready_bitmask=0x0F, ready_value="0x02")
        plugin = self.make_plugin(self.make_scan(config=config))

        self.assertTrue(plugin._decode_ready_from_status("motor", 0x12))
        self.assertFalse(plugin._decode_ready_from_status("motor", 0x13))

    def test_invalid_bitmask_values_fall_back_to_direct_comparison(self):
        config = ActuatorConfig(ready_bitmask=0x01, ready_value="READY")
        plugin = self.make_plugin(self.make_scan(config=config))

        # This is the important regression test: EpicsActuator falls back to
        # numeric/string comparison when bitmask conversion is impossible.
        self.assertTrue(plugin._decode_ready_from_status("motor", " READY "))
        self.assertFalse(plugin._decode_ready_from_status("motor", "MOVING"))

    def test_ready_state_uses_numeric_then_string_comparison(self):
        numeric = ActuatorConfig(ready_value=2)
        numeric_plugin = self.make_plugin(self.make_scan(config=numeric))
        text = ActuatorConfig(ready_value="READY")
        text_plugin = self.make_plugin(self.make_scan(config=text))

        self.assertTrue(numeric_plugin._decode_ready_from_status("motor", "2.0"))
        self.assertTrue(text_plugin._decode_ready_from_status("motor", " READY "))
        self.assertFalse(text_plugin._decode_ready_from_status("motor", "MOVING"))

    def test_ready_state_is_unknown_without_value_config_or_actuator(self):
        plugin = self.make_plugin(self.make_scan())
        no_config_scan = FakeScan({"motor": SimpleNamespace(config=None)})
        no_config_plugin = self.make_plugin(no_config_scan)

        self.assertIsNone(plugin._decode_ready_from_status("motor", None))
        self.assertIsNone(plugin._decode_ready_from_status("missing", 0))
        self.assertIsNone(no_config_plugin._decode_ready_from_status("motor", 0))

    def test_cached_value_is_preferred_to_actuator_read(self):
        scan = self.make_scan(rbv=15.0)
        plugin = self.make_plugin(scan)
        plugin._last_events["motor:rbv"] = {"value": 42.0}

        self.assertEqual(plugin._get_or_read_actuator_value("motor", "rbv"), 42.0)

    @patch("kiwi_scan.plugin_concrete.logging.time.time", return_value=20.0)
    def test_actuator_value_is_read_and_cached(self, _time):
        scan = self.make_scan(rbv=15.0, cmdv=16.0, moving=True)
        plugin = self.make_plugin(scan)

        self.assertEqual(plugin._get_or_read_actuator_value("motor", "rbv"), 15.0)
        self.assertEqual(plugin._get_or_read_actuator_value("motor", "cmd"), 16.0)
        self.assertTrue(plugin._get_or_read_actuator_value("motor", "status"))
        self.assertEqual(plugin._last_events["motor:rbv"]["arrival_time"], 20.0)

    def test_actuator_read_failure_returns_none(self):
        plugin = self.make_plugin(FakeScan())

        self.assertIsNone(plugin._get_or_read_actuator_value("missing", "rbv"))

    @patch("kiwi_scan.plugin_concrete.logging.time.time", return_value=10.0)
    def test_event_age_uses_timestamp_or_arrival_time_and_clamps(self, _time):
        plugin = self.make_plugin()
        plugin._last_events = {
            "motor:rbv": {"timestamp": 8.5, "arrival_time": 9.0},
            "motor:cmd": {"timestamp": None, "arrival_time": 8.0},
            "motor:status": {"timestamp": 11.0, "arrival_time": 11.0},
            "motor:none": {"timestamp": None, "arrival_time": None},
        }

        self.assertEqual(plugin._event_age("motor", "rbv"), 1.5)
        self.assertEqual(plugin._event_age("motor", "cmd"), 2.0)
        self.assertEqual(plugin._event_age("motor", "status"), 0.0)
        self.assertIsNone(plugin._event_age("motor", "none"))
        self.assertIsNone(plugin._event_age("missing", "rbv"))

    def test_safe_float_rejects_invalid_and_nan_values(self):
        self.assertEqual(LoggingPlugin._safe_float("1.25"), 1.25)
        self.assertIsNone(LoggingPlugin._safe_float("invalid"))
        self.assertIsNone(LoggingPlugin._safe_float(None))
        self.assertIsNone(LoggingPlugin._safe_float(math.nan))


class TestLoggingPluginActuatorTrace(LoggingPluginTestCase):
    @patch("kiwi_scan.plugin_concrete.logging.time.time", return_value=100.0)
    def test_collect_actuator_trace_reports_values_ages_and_following_error(self, _time):
        scan = self.make_scan()
        plugin = self.make_plugin(
            scan,
            enable_actuator_trace=True,
            actuator_trace="motor",
        )
        plugin._last_events = {
            "motor:rbv": {
                "value": 10.0,
                "timestamp": 99.0,
                "arrival_time": 99.0,
            },
            "motor:cmd": {
                "value": 12.0,
                "timestamp": 98.0,
                "arrival_time": 98.0,
            },
            "motor:status": {
                "value": 0,
                "timestamp": 97.0,
                "arrival_time": 97.0,
            },
        }

        values = plugin._collect_actuator_trace()

        self.assertEqual(
            values,
            ["ready", 2.0, 10.0, 12.0, 0, 1.0, 2.0, 3.0, 0],
        )

    def test_collect_actuator_trace_falls_back_to_is_ready(self):
        config = ActuatorConfig(ready_bitmask=1, ready_value=0)
        scan = self.make_scan(config=config, moving="invalid", ready=False)
        plugin = self.make_plugin(
            scan,
            enable_actuator_trace=True,
            actuator_trace="motor",
        )

        values = plugin._collect_actuator_trace()

        self.assertEqual(values[0], "not_ready")

    def test_collect_actuator_trace_reports_unknown_when_actuator_fails(self):
        plugin = self.make_plugin(
            FakeScan(),
            enable_actuator_trace=True,
            actuator_trace="missing",
        )

        values = plugin._collect_actuator_trace()

        self.assertEqual(values[0], "unknown")
        self.assertIsNone(values[1])

    def test_collect_actuator_trace_counts_ready_state_changes(self):
        scan = self.make_scan()
        plugin = self.make_plugin(
            scan,
            enable_actuator_trace=True,
            actuator_trace="motor",
        )
        plugin._last_ready_state["motor"] = False
        plugin._last_events["motor:status"] = {"value": 0}

        values = plugin._collect_actuator_trace()

        self.assertEqual(values[0], "ready")
        self.assertEqual(values[-1], 1)


class TestLoggingPluginMonitorEvents(LoggingPluginTestCase):
    def test_monitor_event_is_ignored_when_actuator_trace_is_disabled(self):
        plugin = self.make_plugin(self.make_scan())

        plugin.on_monitor(PvEvent("MOTOR:RBV", 5.0))

        self.assertEqual(plugin._last_events, {})

    def test_unmapped_monitor_event_is_ignored(self):
        plugin = self.make_plugin(
            self.make_scan(),
            enable_actuator_trace=True,
            actuator_trace="motor",
        )

        plugin.on_monitor(PvEvent("OTHER:PV", 5.0))

        self.assertEqual(plugin._last_events, {})

    @patch("kiwi_scan.plugin_concrete.logging.time.time", return_value=20.0)
    def test_mapped_monitor_event_is_recorded(self, _time):
        plugin = self.make_plugin(
            self.make_scan(),
            enable_actuator_trace=True,
            actuator_trace="motor",
        )

        plugin.on_monitor(
            PvEvent(
                "MOTOR:RBV",
                7.5,
                timestamp=19.0,
            )
        )

        self.assertEqual(
            plugin._last_events["motor:rbv"],
            {
                "value": 7.5,
                "timestamp": 19.0,
                "arrival_time": 20.0,
                "pvname": "MOTOR:RBV",
            },
        )

    def test_status_monitor_event_counts_transitions_once(self):
        plugin = self.make_plugin(
            self.make_scan(),
            enable_actuator_trace=True,
            actuator_trace="motor",
        )

        plugin.on_monitor(PvEvent("MOTOR:STATUS", 1))
        plugin.on_monitor(PvEvent("MOTOR:STATUS", 0))
        plugin.on_monitor(PvEvent("MOTOR:STATUS", 0))

        self.assertTrue(plugin._last_ready_state["motor"])
        self.assertEqual(plugin._transition_counts["motor"], 1)


class TestLoggingPluginPublicHooks(LoggingPluginTestCase):
    def test_headers_follow_enabled_features(self):
        plugin = self.make_plugin(
            self.make_scan(),
            enable_alarm_trace=True,
            enable_point_timing=True,
            enable_actuator_trace=True,
            actuator_trace="motor",
        )

        headers = plugin.get_headers(False)

        self.assertEqual(
            headers[:5],
            ["AlarmState", "AlarmPV", "AlarmSeverity", "AlarmStatus", "PointDtS"],
        )
        self.assertEqual(len(headers), 14)
        self.assertEqual(headers[-1], "ActuatormotorTransitions")

    def test_timestamp_headers_are_expanded_in_value_timestamp_order(self):
        plugin = self.make_plugin(enable_point_timing=True)

        self.assertEqual(plugin.get_headers(True), ["PointDtS", "TS-PointDtS"])

    def test_all_features_disabled_return_empty_headers_and_values(self):
        plugin = self.make_plugin()

        self.assertEqual(plugin.get_headers(False), [])
        self.assertEqual(plugin.get_values(0, {}), [])

    def test_get_values_combines_enabled_collectors_in_header_order(self):
        plugin = self.make_plugin(
            enable_alarm_trace=True,
            enable_point_timing=True,
            enable_actuator_trace=True,
        )
        plugin._collect_alarm_trace = Mock(return_value=["alarm"])
        plugin._collect_point_timing = Mock(return_value=["timing"])
        plugin._collect_actuator_trace = Mock(return_value=["actuator"])

        values = plugin.get_values(9, {"motor": 2.0})

        self.assertEqual(values, ["alarm", "timing", "actuator"])
        plugin._collect_alarm_trace.assert_called_once_with(9)
        plugin._collect_point_timing.assert_called_once_with()
        plugin._collect_actuator_trace.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
