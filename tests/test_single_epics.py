# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import threading
import unittest
from unittest.mock import Mock, patch

from kiwi_scan.actuator_concrete.single_epics import EpicsActuator
from kiwi_scan.datamodels import ActuatorConfig, JogConfig
from kiwi_scan.test_support import make_fake_actuator_pv_class

MODULE = "kiwi_scan.actuator_concrete.single_epics"


class EpicsActuatorTestCase(unittest.TestCase):
    def setUp(self):
        self.fake_pv_class = make_fake_actuator_pv_class()
        patcher = patch(f"{MODULE}.EpicsPV", self.fake_pv_class)
        patcher.start()
        self.addCleanup(patcher.stop)

    def make_actuator(self, **overrides):
        values = {
            "pv": "MOTOR:SET",
            "dwell_time": 0.0,
            "queueing_delay": 0.0,
        }
        values.update(overrides)
        return EpicsActuator(ActuatorConfig(**values))

    def pv(self, pvname):
        return next(pv for pv in self.fake_pv_class.instances if pv.pvname == pvname)


class TestEpicsActuatorConfiguration(EpicsActuatorTestCase):
    def test_initializes_all_configured_pvs_and_checks_writable_ones(self):
        actuator = self.make_actuator(
            rel_pv="MOTOR:REL",
            rb_pv="MOTOR:RBV",
            cmd_pv="MOTOR:CMD",
            cmdvel_pv="MOTOR:CMDVEL",
            start_pv="MOTOR:START",
            stop_pv="MOTOR:STOP",
            status_pv="MOTOR:STATUS",
            velocity_pv="MOTOR:VELO",
            get_velocity_pv="MOTOR:VELO:RBV",
            jog=JogConfig(
                velocity_pv="MOTOR:JOGVELO",
                command_pv="MOTOR:JOG",
            ),
        )

        self.assertEqual(actuator.pvname, "MOTOR:SET")
        self.assertEqual(
            {pv.pvname for pv in self.fake_pv_class.instances},
            {
                "MOTOR:SET",
                "MOTOR:REL",
                "MOTOR:RBV",
                "MOTOR:CMD",
                "MOTOR:CMDVEL",
                "MOTOR:START",
                "MOTOR:STOP",
                "MOTOR:STATUS",
                "MOTOR:VELO",
                "MOTOR:VELO:RBV",
                "MOTOR:JOGVELO",
                "MOTOR:JOG",
            },
        )
        for pvname in ("MOTOR:SET", "MOTOR:START", "MOTOR:STOP", "MOTOR:VELO"):
            self.assertEqual(self.pv(pvname).check_pv_calls, 1)
        self.assertTrue(self.pv("MOTOR:RBV").auto_monitor)
        self.assertTrue(self.pv("MOTOR:STATUS").auto_monitor)
        self.assertEqual(self.pv("MOTOR:RBV").queueing_delay, 0.0)

    def test_uses_default_ca_timeout_when_configured_value_is_zero(self):
        actuator = self.make_actuator(ca_timeout=0.0)

        self.assertEqual(actuator.ca_timeout, 1.0)

    def test_missing_setter_has_safe_name_and_logs_warning(self):
        with self.assertLogs(MODULE, level="WARNING") as captured:
            actuator = self.make_actuator(pv=None)

        self.assertEqual(actuator.pvname, "<setter PV not configured>")
        self.assertIn("without setter PV", captured.output[0])

    def test_supports_monitors(self):
        self.assertTrue(self.make_actuator().supports_monitors())


class TestEpicsActuatorMonitors(EpicsActuatorTestCase):
    def test_create_monitor_path_dispatches_event_and_preserves_source(self):
        actuator = self.make_actuator(queueing_delay=0.25)
        received = []

        handle = actuator.add_monitor(
            "MOTOR:EXTRA",
            received.append,
            timeout=2,
        )
        callback_index = next(iter(handle.callbacks))
        handle.trigger(callback_index, value=4.2, source="test")

        self.assertTrue(handle.created_via_create_monitor)
        self.assertEqual(handle.timeout, 2.0)
        self.assertEqual(handle.queueing_delay, 0.25)
        self.assertEqual(
            handle.add_callback_kwargs,
            [{"run_now": False, "with_ctrlvars": False}],
        )
        self.assertEqual(received[0].value, 4.2)
        self.assertEqual(received[0].source, "test")

    def test_add_monitor_reuses_existing_handle(self):
        actuator = self.make_actuator()

        first = actuator.add_monitor("MOTOR:EXTRA")
        second = actuator.add_monitor("MOTOR:EXTRA")

        self.assertIs(first, second)
        self.assertEqual(len(first.callbacks), 1)

    def test_dispatch_ignores_unmonitored_pv(self):
        actuator = self.make_actuator()

        self.assertIsNone(actuator._dispatch_pv_update("UNKNOWN", 1))
        self.assertIsNone(actuator.get_last_event("UNKNOWN"))

    def test_dispatches_when_listener_exists_without_monitor_handle(self):
        actuator = self.make_actuator()
        received = []
        actuator.on_pv_event("MOTOR:LISTEN", received.append)

        event = actuator._dispatch_pv_update("MOTOR:LISTEN", 3)

        self.assertEqual(event.value, 3)
        self.assertEqual(received, [event])

    def test_remove_monitor_logs_backend_cleanup_failures(self):
        actuator = self.make_actuator()
        handle = actuator.add_monitor("MOTOR:EXTRA")
        handle.remove_callback = Mock(side_effect=RuntimeError("remove failed"))
        handle.disconnect = Mock(side_effect=RuntimeError("disconnect failed"))

        with self.assertLogs(MODULE, level="DEBUG") as captured:
            actuator.remove_monitor("MOTOR:EXTRA")

        self.assertNotIn("MOTOR:EXTRA", actuator._monitors)
        self.assertTrue(any("remove_callback failed" in line for line in captured.output))
        self.assertTrue(any("disconnect failed" in line for line in captured.output))

    def test_remove_unknown_monitor_is_a_no_op(self):
        actuator = self.make_actuator()

        actuator.remove_monitor("UNKNOWN")

        self.assertEqual(actuator._monitors, {})


class TestEpicsActuatorValues(EpicsActuatorTestCase):
    def test_readback_uses_monitor_then_fallback_poll(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV", ca_timeout=2.5)
        actuator.rb_pv.get_responses = [None, 7.5]

        self.assertEqual(actuator.rbv, 7.5)
        self.assertEqual(
            actuator.rb_pv.get_calls,
            [
                {"timeout": None, "use_monitor": True},
                {"timeout": 2.5, "use_monitor": False},
            ],
        )

    def test_readback_getter_and_setter_without_readback_pv(self):
        actuator = self.make_actuator()

        self.assertIsNone(actuator.rbv)
        with self.assertRaisesRegex(AttributeError, "Read-back PV not configured"):
            actuator.rbv = 1

    def test_readback_setter_writes_value(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV")

        actuator.rbv = 8.0

        self.assertEqual(actuator.rb_pv.put_calls, [8.0])

    def test_command_value_uses_cache_fallback_and_setter(self):
        actuator = self.make_actuator(cmd_pv="MOTOR:CMD", ca_timeout=3.0)
        actuator.cmd_pv.get_responses = [None, 9.0]

        self.assertEqual(actuator.cmdv, 9.0)
        actuator.cmdv = 10.0

        self.assertEqual(actuator.cmd_pv.put_calls, [10.0])
        self.assertEqual(actuator.cmd_pv.get_calls[-1]["timeout"], 3.0)

    def test_command_value_without_pv(self):
        actuator = self.make_actuator()

        self.assertIsNone(actuator.cmdv)
        with self.assertRaisesRegex(AttributeError, "Command PV not configured"):
            actuator.cmdv = 1

    def test_command_velocity_uses_cache_fallback_and_setter(self):
        actuator = self.make_actuator(cmdvel_pv="MOTOR:CMDVEL")
        actuator.cmdvel_pv.get_responses = [None, 4.0]

        self.assertEqual(actuator.cmdvelv, 4.0)
        actuator.cmdvelv = 5.0

        self.assertEqual(actuator.cmdvel_pv.put_calls, [5.0])

    def test_command_velocity_without_pv(self):
        actuator = self.make_actuator()

        self.assertIsNone(actuator.cmdvelv)
        with self.assertRaisesRegex(AttributeError, "Command PV not configured"):
            actuator.cmdvelv = 1

    def test_get_velocity_uses_cache_fallback(self):
        actuator = self.make_actuator(get_velocity_pv="MOTOR:VELO:RBV")
        actuator.get_velocity_pv.get_responses = [None, 6.5]

        self.assertEqual(actuator.get_velocity(), 6.5)

    def test_get_velocity_without_pv(self):
        self.assertIsNone(self.make_actuator().get_velocity())


class TestEpicsActuatorCommands(EpicsActuatorTestCase):
    def test_wait_for_condition_returns_immediately_when_true(self):
        actuator = self.make_actuator()

        self.assertTrue(actuator._wait_for_condition(lambda: True))

    def test_wait_for_condition_polls_until_true(self):
        actuator = self.make_actuator()
        condition = Mock(side_effect=[False, True])

        with patch(f"{MODULE}.time.sleep") as sleep:
            self.assertTrue(actuator._wait_for_condition(condition, interval=0.2))

        sleep.assert_called_once_with(0.2)

    def test_wait_for_condition_times_out(self):
        actuator = self.make_actuator()

        with patch(
            f"{MODULE}.time.time", side_effect=[0.0, 2.0]
        ), patch(f"{MODULE}.logger.warning") as warning:
            result = actuator._wait_for_condition(
                lambda: False,
                timeout=1.0,
                msg="custom timeout",
            )

        self.assertFalse(result)
        warning.assert_called_once_with("custom timeout")

    def test_wait_for_condition_observes_preexisting_stop(self):
        actuator = self.make_actuator()
        stop_event = threading.Event()
        stop_event.set()

        self.assertFalse(
            actuator._wait_for_condition(lambda: False, stop_event=stop_event)
        )

    def test_wait_for_condition_observes_stop_during_wait(self):
        actuator = self.make_actuator()
        stop_event = Mock(spec=threading.Event)
        stop_event.is_set.return_value = False
        stop_event.wait.return_value = True

        self.assertFalse(
            actuator._wait_for_condition(
                lambda: False,
                interval=0.3,
                stop_event=stop_event,
            )
        )
        stop_event.wait.assert_called_once_with(0.3)

    def test_start_actuator_and_set_velocity(self):
        actuator = self.make_actuator(
            start_pv="MOTOR:START",
            start_command=2,
            velocity_pv="MOTOR:VELO",
        )

        actuator.start_actuator()
        actuator.set_velocity(12.5)

        self.assertEqual(actuator.start_pv.put_calls, [2])
        self.assertEqual(actuator.velocity_pv.put_calls, [12.5])
        self.assertEqual(actuator.velocity, 12.5)

    def test_start_and_velocity_failures_are_logged(self):
        actuator = self.make_actuator(
            start_pv="MOTOR:START",
            velocity_pv="MOTOR:VELO",
        )
        actuator.start_pv.put_result = False
        actuator.velocity_pv.put_result = False

        with self.assertLogs(MODULE, level="ERROR") as captured:
            actuator.start_actuator()
            actuator.set_velocity(1.5)

        self.assertTrue(any("Failed to start" in line for line in captured.output))
        self.assertTrue(any("Failed to set velocity" in line for line in captured.output))

    def test_start_and_velocity_without_control_pvs_are_safe(self):
        actuator = self.make_actuator()

        actuator.start_actuator()
        actuator.set_velocity(2.0)

        self.assertEqual(actuator.velocity, 2.0)

    def test_issue_move_writes_starts_and_applies_queue_delay(self):
        actuator = self.make_actuator(
            start_pv="MOTOR:START",
            start_command=1,
            queueing_delay=0.4,
        )

        with patch(f"{MODULE}.time.sleep") as sleep:
            actuator.move(5.5)

        self.assertEqual(actuator.pv.put_calls, [5.5])
        self.assertEqual(actuator.start_pv.put_calls, [1])
        sleep.assert_called_once_with(0.4)

    def test_issue_move_logs_failed_write_but_still_starts(self):
        actuator = self.make_actuator(start_pv="MOTOR:START")
        actuator.pv.put_result = False

        with self.assertLogs(MODULE, level="ERROR") as captured, patch(
            f"{MODULE}.time.sleep"
        ):
            actuator.move(5.5)

        self.assertIn("Failed to write position", captured.output[0])
        self.assertEqual(actuator.start_pv.put_calls, [0.0])

    def test_issue_move_without_setter_logs_once_and_returns(self):
        actuator = self.make_actuator(pv=None, start_pv="MOTOR:START")

        with self.assertLogs(MODULE, level="ERROR") as captured, patch.object(
            actuator, "start_actuator"
        ) as start, patch(f"{MODULE}.time.sleep") as sleep:
            actuator._issue_move(5.5)

        self.assertEqual(len(captured.output), 1)
        self.assertIn("setter PV is not configured", captured.output[0])
        start.assert_not_called()
        sleep.assert_not_called()

    def test_run_move_selects_completion_or_startup_wait(self):
        actuator = self.make_actuator()

        with patch.object(actuator, "move") as move, patch.object(
            actuator, "wait_until_done"
        ) as done, patch.object(actuator, "wait_for_startup") as startup:
            actuator.run_move(2.0)
            actuator.run_move(3.0, sync=False, wait_startup=True)
            actuator.run_move(4.0, sync=False)

        self.assertEqual(move.call_count, 3)
        done.assert_called_once_with(2.0)
        startup.assert_called_once_with()


class TestEpicsActuatorRelativeMoves(EpicsActuatorTestCase):
    def test_relative_pv_move_writes_starts_and_delays(self):
        actuator = self.make_actuator(
            rel_pv="MOTOR:REL",
            start_pv="MOTOR:START",
            queueing_delay=0.2,
        )

        with patch(f"{MODULE}.time.sleep") as sleep:
            actuator.rel_move(-1.5)

        self.assertEqual(actuator.rel_pv.put_calls, [-1.5])
        self.assertEqual(actuator.start_pv.put_calls, [0.0])
        sleep.assert_called_once_with(0.2)

    def test_relative_pv_failed_write_is_logged(self):
        actuator = self.make_actuator(rel_pv="MOTOR:REL")
        actuator.rel_pv.put_result = False

        with self.assertLogs(MODULE, level="ERROR") as captured, patch(
            f"{MODULE}.time.sleep"
        ):
            actuator.rel_move(1)

        self.assertIn("Failed to write relative move", captured.output[0])

    def test_relative_move_falls_back_to_absolute_target(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV")
        actuator.rb_pv._value = 10.0

        with patch.object(actuator, "_issue_move") as issue:
            actuator.rel_move(2.5)

        issue.assert_called_once_with(12.5)

    def test_relative_fallback_rejects_missing_or_invalid_readback(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV")

        with self.assertLogs(MODULE, level="ERROR") as captured:
            actuator.rb_pv._value = None
            actuator.rel_move(1)
            actuator.rb_pv._value = "not-a-number"
            actuator.rel_move(1)

        self.assertTrue(any("rbv is None" in line for line in captured.output))
        self.assertTrue(any("Failed to compute" in line for line in captured.output))

    def test_run_relative_with_relative_pv_waits_for_computed_target(self):
        actuator = self.make_actuator(
            rel_pv="MOTOR:REL",
            rb_pv="MOTOR:RBV",
            in_position_band=0.1,
        )
        actuator.rb_pv._value = 4.0

        with patch.object(actuator, "rel_move") as move, patch.object(
            actuator, "wait_until_done"
        ) as done:
            actuator.run_rel_move(1.5)

        move.assert_called_once_with(1.5)
        done.assert_called_once_with(5.5)

    def test_run_relative_with_no_target_uses_status_wait_and_dwell(self):
        actuator = self.make_actuator(rel_pv="MOTOR:REL")

        with patch.object(actuator, "rel_move"), patch.object(
            actuator, "wait_for_startup_and_done"
        ) as wait, patch.object(actuator, "dwell") as dwell:
            actuator.run_rel_move(1.0)

        wait.assert_called_once_with()
        dwell.assert_called_once_with()

    def test_run_relative_conversion_failure_uses_status_wait_and_dwell(self):
        actuator = self.make_actuator(
            rel_pv="MOTOR:REL",
            rb_pv="MOTOR:RBV",
            in_position_band=0.1,
        )
        actuator.rb_pv._value = "invalid"

        with patch.object(actuator, "rel_move"), patch.object(
            actuator, "wait_for_startup_and_done"
        ) as wait, patch.object(actuator, "dwell") as dwell:
            actuator.run_rel_move(1.0)

        wait.assert_called_once_with()
        dwell.assert_called_once_with()

    def test_run_relative_can_only_wait_for_startup(self):
        actuator = self.make_actuator(rel_pv="MOTOR:REL")

        with patch.object(actuator, "rel_move"), patch.object(
            actuator, "wait_for_startup"
        ) as startup:
            actuator.run_rel_move(1.0, sync=False, wait_startup=True)

        startup.assert_called_once_with()

    def test_run_relative_absolute_fallback_sync_and_startup_paths(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV")
        actuator.rb_pv._value = 2.0

        with patch.object(actuator, "_issue_move") as issue, patch.object(
            actuator, "wait_until_done"
        ) as done, patch.object(actuator, "wait_for_startup") as startup:
            actuator.run_rel_move(3.0)
            actuator.run_rel_move(4.0, sync=False, wait_startup=True)

        self.assertEqual(issue.call_count, 2)
        done.assert_called_once_with(5.0)
        startup.assert_called_once_with()

    def test_run_relative_absolute_fallback_stops_without_readback(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV")
        actuator.rb_pv._value = None

        with self.assertLogs(MODULE, level="ERROR") as captured, patch.object(
            actuator, "_issue_move"
        ) as issue:
            actuator.run_rel_move(3.0)

        self.assertIn("rbv is None", captured.output[0])
        issue.assert_not_called()


class TestEpicsActuatorJog(EpicsActuatorTestCase):
    def test_jog_requires_configuration(self):
        with self.assertRaisesRegex(ValueError, "Jog feature is not configured"):
            self.make_actuator().jog(1.0)

    def test_jog_writes_absolute_velocity_and_direction_command(self):
        actuator = self.make_actuator(
            jog=JogConfig(
                velocity_pv="MOTOR:JOGVELO",
                abs_velocity=True,
                command_pv="MOTOR:JOG",
                command_pos=1,
                command_neg=-1,
            )
        )

        actuator.jog(-3.5, sync=False)
        actuator.jog(2.0, sync=False)

        self.assertEqual(actuator.jog_velocity_pv.put_calls, [3.5, 2.0])
        self.assertEqual(actuator.jog_command_pv.put_calls, [-1, 1])

    def test_jog_supports_signed_velocity_and_single_command(self):
        actuator = self.make_actuator(
            jog=JogConfig(
                velocity_pv="MOTOR:JOGVELO",
                command_pv="MOTOR:JOG",
                command_pos=0,
            )
        )

        actuator.jog(-2.0, sync=False)

        self.assertEqual(actuator.jog_velocity_pv.put_calls, [-2.0])
        self.assertEqual(actuator.jog_command_pv.put_calls, [1.0])

    def test_jog_failures_are_logged_and_sync_waits(self):
        actuator = self.make_actuator(
            jog=JogConfig(
                velocity_pv="MOTOR:JOGVELO",
                command_pv="MOTOR:JOG",
                command_pos=1,
            )
        )
        actuator.jog_velocity_pv.put_result = False
        actuator.jog_command_pv.put_result = False

        with self.assertLogs(MODULE, level="ERROR") as captured, patch.object(
            actuator, "wait_for_startup_and_done"
        ) as wait:
            actuator.jog(1.0)

        self.assertEqual(len(captured.output), 2)
        wait.assert_called_once_with()


class TestEpicsActuatorStatus(EpicsActuatorTestCase):
    def test_read_status_uses_cache_then_fallback(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS", ca_timeout=4.0)
        actuator.status_pv.get_responses = [None, 7]

        self.assertEqual(actuator._read_status_value(), 7)
        self.assertEqual(actuator.status_pv.get_calls[-1]["timeout"], 4.0)

    def test_read_status_without_pv_returns_none(self):
        self.assertIsNone(self.make_actuator()._read_status_value())

    def test_status_decoder_supports_bitmask_and_hex_ready_value(self):
        actuator = self.make_actuator(ready_bitmask=0x0F, ready_value="0x02")

        self.assertTrue(actuator._status_value_is_ready(0x12))
        self.assertFalse(actuator._status_value_is_ready(0x11))

    def test_status_decoder_falls_back_from_invalid_bitmask(self):
        actuator = self.make_actuator(ready_bitmask="invalid", ready_value=5)

        self.assertTrue(actuator._status_value_is_ready(5))

    def test_status_decoder_supports_non_numeric_values(self):
        actuator = self.make_actuator(ready_value=" READY ")

        self.assertTrue(actuator._status_value_is_ready("READY"))
        self.assertFalse(actuator._status_value_is_ready("MOVING"))

    def test_ready_state_without_status_pv_is_ready(self):
        actuator = self.make_actuator()

        self.assertTrue(actuator.is_ready())
        self.assertFalse(actuator.is_moving())

    def test_unknown_status_is_neither_ready_nor_moving(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS")
        actuator.status_pv._value = None

        self.assertFalse(actuator.is_ready())
        self.assertFalse(actuator.is_moving())

    def test_concrete_status_reports_ready_and_moving(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS", ready_value=0)

        actuator.status_pv._value = 0
        self.assertTrue(actuator.is_ready())
        self.assertFalse(actuator.is_moving())

        actuator.status_pv._value = 1
        self.assertFalse(actuator.is_ready())
        self.assertTrue(actuator.is_moving())


class TestEpicsActuatorWaiting(EpicsActuatorTestCase):
    def test_in_position_is_immediate_when_disabled_or_unconfigured(self):
        self.assertTrue(self.make_actuator(in_position_band=-1).in_position_check(10))
        self.assertTrue(self.make_actuator(in_position_band=1).in_position_check(10))

    def test_in_position_handles_match_and_missing_readback(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV", in_position_band=0.5)
        actuator.rb_pv._value = 10.4
        self.assertTrue(actuator.in_position_check(10.0))

        actuator.rb_pv._value = None
        with self.assertLogs(MODULE, level="WARNING"):
            self.assertTrue(actuator.in_position_check(10.0))

    def test_in_position_times_out(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV", in_position_band=0.1)
        actuator.rb_pv._value = 20.0

        with patch(f"{MODULE}.time.time", side_effect=[0.0, 1.0]):
            self.assertFalse(actuator.in_position_check(10.0, timeout=0.5))

    def test_in_position_observes_stop_before_and_during_wait(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV", in_position_band=0.1)
        actuator.rb_pv._value = 20.0
        already_stopped = threading.Event()
        already_stopped.set()
        self.assertFalse(actuator.in_position_check(10.0, stop_event=already_stopped))

        stop_event = Mock(spec=threading.Event)
        stop_event.is_set.return_value = False
        stop_event.wait.return_value = True
        self.assertFalse(actuator.in_position_check(10.0, stop_event=stop_event))

    def test_in_position_polls_until_match(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV", in_position_band=0.1)
        actuator.rb_pv.get_responses = [20.0, 10.0]

        with patch(f"{MODULE}.time.sleep") as sleep:
            self.assertTrue(actuator.in_position_check(10.0))

        sleep.assert_called_once_with(0.1)

    def test_is_in_position_reads_with_ca_timeout(self):
        actuator = self.make_actuator(rb_pv="MOTOR:RBV", ca_timeout=2.0)
        actuator.rb_pv._value = 5.1

        self.assertTrue(actuator.is_in_position(5.0, 0.2))
        self.assertEqual(actuator.rb_pv.get_calls[-1]["timeout"], 2.0)

    def test_dwell_sleeps_for_configured_time(self):
        actuator = self.make_actuator(dwell_time=0.6)

        with patch(f"{MODULE}.time.sleep") as sleep:
            actuator.dwell()

        sleep.assert_called_once_with(0.6)

    def test_wait_for_startup_shortcuts_for_stop_or_missing_status(self):
        actuator = self.make_actuator()
        stop_event = threading.Event()
        stop_event.set()

        self.assertFalse(actuator.wait_for_startup(stop_event))
        self.assertTrue(actuator.wait_for_startup())

    def test_wait_for_startup_reports_success_and_timeout(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS", startup_timeout=2.0)

        with patch.object(actuator, "_wait_for_condition", return_value=True) as wait:
            self.assertTrue(actuator.wait_for_startup())
        wait.assert_called_once_with(
            actuator.is_moving,
            2.0,
            msg="[MOTOR:SET] move start was not observed within 2.0s",
            stop_event=None,
        )

        with patch.object(
            actuator, "_wait_for_condition", return_value=False
        ), self.assertLogs(MODULE, level="WARNING"):
            self.assertFalse(actuator.wait_for_startup())

    def test_wait_for_startup_detects_stop_after_poll(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS")
        stop_event = threading.Event()

        def request_stop(*args, **kwargs):
            stop_event.set()
            return True

        with patch.object(actuator, "_wait_for_condition", side_effect=request_stop):
            self.assertFalse(actuator.wait_for_startup(stop_event))

    def test_wait_for_startup_and_done_honors_stop(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS")
        stop_event = threading.Event()

        def request_stop(*args, **kwargs):
            stop_event.set()
            return False

        with patch.object(
            actuator, "wait_for_startup", side_effect=request_stop
        ), patch.object(actuator, "_wait_for_condition") as wait:
            actuator.wait_for_startup_and_done(stop_event)

        wait.assert_not_called()

    def test_wait_for_startup_and_done_waits_for_ready(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS")

        with patch.object(actuator, "wait_for_startup") as startup, patch.object(
            actuator, "_wait_for_condition"
        ) as wait:
            actuator.wait_for_startup_and_done()

        startup.assert_called_once_with(stop_event=None)
        wait.assert_called_once_with(actuator.is_ready, stop_event=None)

    def test_interruptible_dwell_paths(self):
        self.assertTrue(self.make_actuator(dwell_time=0)._dwell_interruptible())

        actuator = self.make_actuator(dwell_time=0.5)
        stop_event = Mock(spec=threading.Event)
        stop_event.wait.side_effect = [False, True]
        self.assertTrue(actuator._dwell_interruptible(stop_event))
        self.assertFalse(actuator._dwell_interruptible(stop_event))

        with patch.object(actuator, "dwell") as dwell:
            self.assertTrue(actuator._dwell_interruptible())
        dwell.assert_called_once_with()

    def test_wait_until_done_runs_status_band_and_dwell(self):
        actuator = self.make_actuator(
            status_pv="MOTOR:STATUS",
            rb_pv="MOTOR:RBV",
            in_position_band=0.1,
            dwell_time=0.2,
        )

        with patch.object(
            actuator, "wait_for_startup_and_done"
        ) as status_wait, patch.object(
            actuator, "in_position_check", return_value=True
        ) as in_band, patch.object(
            actuator, "_dwell_interruptible", return_value=True
        ) as dwell, patch(f"{MODULE}.time.time", side_effect=[10.0, 10.5]):
            actuator.wait_until_done(4.0)

        status_wait.assert_called_once_with(stop_event=None)
        in_band.assert_called_once_with(4.0, stop_event=None)
        dwell.assert_called_once_with(None)
        self.assertEqual(actuator._last_move_time, 0.5)

    def test_wait_until_done_without_conditions_only_records_elapsed_time(self):
        actuator = self.make_actuator()

        with patch(f"{MODULE}.logger.info") as info, patch(
            f"{MODULE}.time.time", side_effect=[2.0, 2.25]
        ):
            actuator.wait_until_done(1.0)

        self.assertTrue(
            any("no wait conditions" in call.args[0] for call in info.call_args_list)
        )
        self.assertEqual(actuator._last_move_time, 0.25)

    def test_wait_until_done_honors_stop_before_start(self):
        actuator = self.make_actuator()
        stop_event = threading.Event()
        stop_event.set()

        with patch(f"{MODULE}.time.time", side_effect=[1.0, 1.1]):
            actuator.wait_until_done(1.0, stop_event)

        self.assertAlmostEqual(actuator._last_move_time, 0.1)

    def test_wait_until_done_honors_stop_after_status_wait(self):
        actuator = self.make_actuator(status_pv="MOTOR:STATUS")
        stop_event = threading.Event()

        def request_stop(*args, **kwargs):
            stop_event.set()

        with patch.object(
            actuator,
            "wait_for_startup_and_done",
            side_effect=request_stop,
        ), patch.object(actuator, "_dwell_interruptible") as dwell:
            actuator.wait_until_done(1.0, stop_event)

        dwell.assert_not_called()

    def test_wait_until_done_honors_stop_during_in_position_check(self):
        actuator = self.make_actuator(
            rb_pv="MOTOR:RBV",
            in_position_band=0.1,
        )

        for in_position_result in (False, True):
            with self.subTest(in_position_result=in_position_result):
                stop_event = threading.Event()

                def request_stop(
                    *args,
                    event=stop_event,
                    result=in_position_result,
                    **kwargs,
                ):
                    event.set()
                    return result

                with patch.object(
                    actuator,
                    "in_position_check",
                    side_effect=request_stop,
                ), patch.object(actuator, "_dwell_interruptible") as dwell:
                    actuator.wait_until_done(1.0, stop_event)

                dwell.assert_not_called()

    def test_wait_until_done_logs_failed_in_position_and_interruptible_dwell(self):
        actuator = self.make_actuator(
            rb_pv="MOTOR:RBV",
            in_position_band=0.1,
            dwell_time=0.2,
        )

        with patch.object(
            actuator, "in_position_check", return_value=False
        ), patch.object(
            actuator, "_dwell_interruptible", return_value=False
        ), self.assertLogs(MODULE, level="INFO") as captured:
            actuator.wait_until_done(1.0)

        self.assertTrue(any("never reached" in line for line in captured.output))
        self.assertTrue(any("dwell interrupted" in line for line in captured.output))


class TestEpicsActuatorStop(EpicsActuatorTestCase):
    def test_stop_writes_configured_command(self):
        actuator = self.make_actuator(stop_pv="MOTOR:STOP", stop_command=9)

        actuator.stop()

        self.assertEqual(actuator.stop_pv.put_calls, [9])

    def test_stop_failure_is_logged(self):
        actuator = self.make_actuator(stop_pv="MOTOR:STOP")
        actuator.stop_pv.put_result = False

        with self.assertLogs(MODULE, level="ERROR") as captured:
            actuator.stop()

        self.assertIn("Failed to stop actuator", captured.output[0])

    def test_stop_without_pv_is_safe(self):
        self.make_actuator().stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
