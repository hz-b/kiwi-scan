# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""CLI dispatch tests without EPICS connections or real hardware commands."""

import argparse
import io
import signal
import threading
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from kiwi_scan.cli import actuator_runner as cli
from kiwi_scan.datamodels import ActuatorConfig


class _ImmediateExecutor:
    """Execute deterministically, preserving Future's exception semantics."""

    def __init__(self, max_workers):
        self.max_workers = max_workers
        self.futures = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def submit(self, function, *args, **kwargs):
        future = Future()
        try:
            future.set_result(function(*args, **kwargs))
        except Exception as exc:  # noqa: BLE001 - preserve executor Future exception semantics
            future.set_exception(exc)
        self.futures.append(future)
        return future


class _QueuedExecutor(_ImmediateExecutor):
    """Keep submitted commands pending until the caller cancels them."""

    def submit(self, _function, *_args, **_kwargs):
        future = Future()
        self.futures.append(future)
        return future


class TestActuatorRunnerParsing(unittest.TestCase):
    def test_numeric_assignment_trims_whitespace(self):
        self.assertEqual(cli._parse_name_value(" energy = -1.25e2 "), ("energy", -125.0))

    def test_invalid_numeric_assignments_name_the_bad_input(self):
        for spec in ("energy", "=2", "energy=", "energy=abc", "energy=1=2"):
            with self.subTest(spec=spec), \
                    self.assertRaisesRegex(ValueError, "Expected|Empty"):
                cli._parse_name_value(spec)

    def test_relative_move_accepts_scalar_and_axis_list(self):
        self.assertEqual(cli._parse_name_value_any("energy=2.5"), ("energy", 2.5))
        self.assertEqual(
            cli._parse_name_value_any(" undulator = [0.1, -0.2] "),
            ("undulator", [0.1, -0.2]),
        )

    def test_relative_move_rejects_malformed_assignment(self):
        for spec in ("energy", "=2", "energy=", "energy=[1,", "energy=word"):
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                cli._parse_name_value_any(spec)

    def test_resolve_explicit_config_and_merge_replacements(self):
        args = argparse.Namespace(config_file="custom.yaml", config=None, replace=["IOC=CLI"])
        with patch.object(cli, "get_env_replacements", return_value={"SITE": "RING"}):
            path, replacements = cli._resolve_config_input(args, "/presets")
        self.assertEqual(path, "custom.yaml")
        self.assertEqual(replacements, {"IOC": "CLI", "SITE": "RING"})

    def test_resolve_preset_config(self):
        args = argparse.Namespace(config_file=None, config="mono", replace=[])
        with patch.object(cli, "get_env_replacements", return_value={}):
            self.assertEqual(cli._resolve_config_input(args, "/presets"), ("/presets/mono.yaml", {}))


class TestActuatorRunnerCLI(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.output = io.StringIO()
        self.errors = io.StringIO()
        self.stack.enter_context(redirect_stdout(self.output))
        self.stack.enter_context(redirect_stderr(self.errors))
        self.stack.enter_context(patch.dict(cli.os.environ, {}, clear=True))
        self.stack.enter_context(patch.object(cli, "get_scan_config_dir", return_value="/presets"))
        self.configs = self.stack.enter_context(
            patch.object(cli, "load_scan_configs", return_value={"mono": "mono.yaml"})
        )
        self.stack.enter_context(patch.object(cli, "get_env_replacements", return_value={}))
        self.required = self.stack.enter_context(
            patch.object(cli, "get_replacements_help_and_required", return_value=("IOC help", False))
        )
        self.energy = Mock(name="energy", config=ActuatorConfig(pv="TEST:SP", rb_pv="TEST:RBV"))
        self.slit = Mock(name="slit", config=ActuatorConfig(pv="TEST:SLIT"))
        self.actuators = {"energy": self.energy, "slit": self.slit}
        self.load = self.stack.enter_context(patch.object(cli, "load_actuators", return_value=self.actuators))
        self.monitors = self.stack.enter_context(patch.object(cli, "run_monitors", return_value=(3, 0)))
        self.install_signal = self.stack.enter_context(patch.object(cli.signal, "signal"))
        self.stack.enter_context(
            patch.object(cli.os, "_exit", side_effect=AssertionError("unexpected forced exit"))
        )
        self.logging_level = self.stack.enter_context(patch.object(cli, "set_valid_logging_level"))
        self.executor = self.stack.enter_context(
            patch.object(cli, "ThreadPoolExecutor", side_effect=_ImmediateExecutor)
        )

    def run_cli(self, *args):
        with patch.object(cli.sys, "argv", ["actuator_runner"] + list(args)):
            return cli.main()

    def run_file(self, *args):
        return self.run_cli("--config-file", "custom.yaml", *args)

    def test_explicit_config_replacements_and_logging(self):
        self.run_file("--replace", "IOC=TEST", "--log-level", "2", "--stop", "energy")
        self.load.assert_called_once_with("custom.yaml", {"IOC": "TEST"})
        self.logging_level.assert_called_once_with(2)
        self.required.assert_not_called()

    def test_preset_uses_environment_directory_and_shows_required_replacements(self):
        self.required.return_value = ("Required IOC replacement", True)
        with patch.dict(cli.os.environ, {"KIWI_SCAN_CONFIG_DIR": "/site/presets"}):
            self.run_cli("--config", "mono", "--stop", "energy")
        self.configs.assert_called_once_with("/site/presets", None)
        self.load.assert_called_once_with("/site/presets/mono.yaml", {})
        self.assertIn("Required IOC replacement", self.output.getvalue())

    def test_missing_and_conflicting_config_are_parser_errors(self):
        for args in ((), ("--config", "mono", "--config-file", "other.yaml")):
            with self.subTest(args=args), \
                    self.assertRaises(SystemExit) as caught:
                self.run_cli(*args)
            self.assertEqual(caught.exception.code, 2)
        self.load.assert_not_called()

    def test_load_failures_are_parser_errors_without_actions(self):
        for failure in (FileNotFoundError("missing"), ValueError("bad config"),
                        TypeError("bad type"), ConnectionError("disconnected")):
            with self.subTest(failure=failure):
                self.load.side_effect = failure
                with self.assertRaises(SystemExit) as caught:
                    self.run_file("--move", "energy=1")
                self.assertEqual(caught.exception.code, 2)
        self.assertIn("failed to load actuators", self.errors.getvalue())
        self.energy.run_move.assert_not_called()
        self.executor.assert_not_called()

    def test_every_invalid_action_is_rejected_before_any_action(self):
        invalid = (("--move", "unknown=1"), ("--rel-move", "unknown=[1, 2]"),
                   ("--jog", "energy=bad"), ("--stop", "unknown"),
                   ("--set-velocity", "energy="), ("--monitor", "unknown:rbv"))
        for option, spec in invalid:
            with self.subTest(option=option, spec=spec), \
                    self.assertRaises(SystemExit) as caught:
                self.run_file("--stop", "energy", option, spec)
            self.assertEqual(caught.exception.code, 2)
        self.energy.stop.assert_not_called()
        self.energy.run_move.assert_not_called()
        self.monitors.assert_not_called()
        self.install_signal.assert_not_called()

    def test_monitor_source_without_configured_pv_is_rejected(self):
        with self.assertRaises(SystemExit) as caught:
            self.run_file("--monitor", "slit:status", "--monitor-count", "1")
        self.assertEqual(caught.exception.code, 2)
        self.monitors.assert_not_called()

    def test_monitors_only_requires_exit_condition(self):
        with self.assertRaises(SystemExit) as caught:
            self.run_file("--monitor", "energy:rbv")
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("no exit condition", self.errors.getvalue())
        self.monitors.assert_not_called()

    def test_stop_and_velocity_run_before_async_motion(self):
        actions = Mock()
        actions.attach_mock(self.energy.stop, "stop")
        actions.attach_mock(self.energy.set_velocity, "velocity")
        actions.attach_mock(self.energy.run_move, "move")
        self.run_file("--stop", "energy", "--set-velocity", "energy=2", "--move", "energy=10")
        self.assertEqual(actions.mock_calls, [call.stop(), call.velocity(2.0), call.move(10.0, True)])

    def test_immediate_actions_do_not_start_executor_or_monitors(self):
        self.run_file("--stop", "energy", "--set-velocity", "slit=0.5")
        self.energy.stop.assert_called_once_with()
        self.slit.set_velocity.assert_called_once_with(0.5)
        self.executor.assert_not_called()
        self.monitors.assert_not_called()

    def test_move_relative_move_and_jog_dispatch_synchronously_in_workers(self):
        self.run_file("--move", "energy=10", "--rel-move", "slit=[0.1, -0.2]", "--jog", "energy=-2")
        self.energy.run_move.assert_called_once_with(10.0, True)
        self.slit.run_rel_move.assert_called_once_with([0.1, -0.2], True)
        self.energy.jog.assert_called_once_with(-2.0, True)
        self.executor.assert_called_once_with(max_workers=2)
        self.assertIn("Done. events_seen=0 dropped=0", self.output.getvalue())

    def test_scalar_relative_move_dispatch(self):
        self.run_file("--rel-move", "energy=-0.25")
        self.energy.run_rel_move.assert_called_once_with(-0.25, True)

    def test_monitor_options_and_motion_completion_are_forwarded(self):
        def monitor(*args, **kwargs):
            self.assertIs(args[0], self.actuators)
            self.assertEqual(args[1], ["energy:rbv"])
            self.assertEqual(kwargs["duration"], 1.5)
            self.assertEqual(kwargs["count"], 4)
            self.assertEqual(kwargs["out_path"], "output.txt")
            self.assertFalse(kwargs["stop_event"].is_set())
            self.assertTrue(kwargs["completion_check"]())
            return 4, 2

        self.monitors.side_effect = monitor
        with self.assertLogs(cli.logger, level="WARNING") as captured:
            self.run_file("--monitor", "energy:rbv", "--monitor-duration", "1.5",
                          "--monitor-count", "4", "--out", "output.txt", "--move", "energy=1")
        self.assertIn("Dropped 2 monitor events", captured.output[0])
        self.assertIn("Done. events_seen=4 dropped=2", self.output.getvalue())

    def test_monitors_only_has_no_motion_completion_check(self):
        self.run_file("--monitor", "energy:rbv", "--monitor-count", "1")
        self.assertIsNone(self.monitors.call_args.kwargs["completion_check"])

    def test_keep_alive_disables_motion_completion_check(self):
        self.run_file("--monitor", "energy:rbv", "--move", "energy=1", "--keep-alive")
        self.assertIsNone(self.monitors.call_args.kwargs["completion_check"])

    def test_sigint_stops_all_actuators_and_requests_monitor_shutdown(self):
        def monitor(*_args, **kwargs):
            handler = self.install_signal.call_args.args[1]
            handler(signal.SIGINT, None)
            self.assertTrue(kwargs["stop_event"].is_set())
            return 0, 0

        self.monitors.side_effect = monitor
        self.run_file("--monitor", "energy:rbv", "--monitor-count", "1", "--move", "energy=1")
        self.assertGreaterEqual(self.energy.stop.call_count, 1)
        self.slit.stop.assert_called_once_with()
        self.assertIn("stopping actuators", self.errors.getvalue())

    def test_second_sigint_requests_forced_exit(self):
        with patch.object(cli.os, "_exit", side_effect=SystemExit(130)) as force_exit:
            def monitor(*_args, **_kwargs):
                handler = self.install_signal.call_args.args[1]
                handler(signal.SIGINT, None)
                handler(signal.SIGINT, None)

            self.monitors.side_effect = monitor
            with self.assertRaises(SystemExit) as caught:
                self.run_file("--monitor", "energy:rbv", "--monitor-count", "1")
        self.assertEqual(caught.exception.code, 130)
        force_exit.assert_called_once_with(130)

    def test_failed_motion_must_not_be_reported_as_success(self):
        self.energy.run_move.side_effect = RuntimeError("motion failed")
        with self.assertLogs(cli.logger, level="ERROR") as captured, \
                self.assertRaises(SystemExit) as caught:
            self.run_file("--move", "energy=1")
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("motion failed", "\n".join(captured.output))
        self.assertNotIn("Done.", self.output.getvalue())
        self.energy.stop.assert_called_once_with()

    def test_sigint_cancels_commands_that_have_not_started(self):
        def first_move(position, _sync):
            if position == 1.0:
                handler = self.install_signal.call_args.args[1]
                handler(signal.SIGINT, None)

        self.energy.run_move.side_effect = first_move
        self.run_file("--move", "energy=1", "--move", "energy=2")
        self.energy.run_move.assert_called_once_with(1.0, True)

    def test_loaded_actuators_are_closed_after_use(self):
        self.run_file("--stop", "energy")
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()

    def test_original_sigint_handler_is_restored_after_return(self):
        previous_handler = Mock(name="previous_sigint_handler")
        self.install_signal.return_value = previous_handler
        with patch.object(cli.signal, "getsignal", return_value=previous_handler):
            self.run_file("--stop", "energy")
        self.install_signal.assert_called_with(signal.SIGINT, previous_handler)

    def test_monitor_failure_stops_started_motion_before_propagating(self):
        self.monitors.side_effect = OSError("monitor output failed")
        with self.assertRaisesRegex(OSError, "monitor output failed"):
            self.run_file("--monitor", "energy:rbv", "--move", "energy=1")
        self.energy.stop.assert_called_once_with()

    def test_stop_is_checked_after_acquiring_actuator_lock(self):
        """A command may already have started waiting for its device lock."""
        install_signal = self.install_signal

        class StopOnAcquisition:
            def __enter__(self):
                install_signal.call_args.args[1](signal.SIGINT, None)

            def __exit__(self, *_args):
                return False

        threading_api = SimpleNamespace(Event=threading.Event, Lock=StopOnAcquisition)
        with patch.object(cli, "threading", threading_api):
            self.run_file("--move", "energy=1")
        self.energy.run_move.assert_not_called()

    def test_sigint_cancels_pending_futures(self):
        queued = _QueuedExecutor(max_workers=2)
        self.executor.side_effect = None
        self.executor.return_value = queued

        def monitor(*_args, **kwargs):
            self.install_signal.call_args.args[1](signal.SIGINT, None)
            self.assertTrue(kwargs["stop_event"].is_set())
            return 0, 0

        self.monitors.side_effect = monitor
        self.run_file("--monitor", "energy:rbv", "--move", "energy=1", "--move", "slit=2")
        self.assertEqual(len(queued.futures), 2)
        self.assertTrue(all(future.cancelled() for future in queued.futures))
        self.energy.run_move.assert_not_called()
        self.slit.run_move.assert_not_called()

    def test_failed_worker_cancels_later_commands_and_stops_keep_alive_monitors(self):
        self.energy.run_move.side_effect = RuntimeError("motion failed")

        def monitor(*_args, **kwargs):
            self.assertTrue(kwargs["stop_event"].is_set())
            self.assertIsNone(kwargs["completion_check"])
            return 0, 0

        self.monitors.side_effect = monitor
        with self.assertLogs(cli.logger, level="ERROR") as captured, \
                self.assertRaises(SystemExit) as caught:
            self.run_file("--monitor", "energy:rbv", "--keep-alive",
                          "--move", "energy=1", "--move", "slit=2")
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("motion failed", "\n".join(captured.output))
        self.slit.run_move.assert_not_called()
        self.assertTrue(self.energy.stop.called)
        self.assertNotIn("Done.", self.output.getvalue())

    def test_monitor_error_stops_motion_before_executor_teardown(self):
        monitor_kwargs = {}
        test_case = self

        class CheckedExecutor(_ImmediateExecutor):
            def __exit__(self, *_args):
                test_case.assertTrue(monitor_kwargs["stop_event"].is_set())
                test_case.energy.stop.assert_called_once_with()
                test_case.energy.close.assert_not_called()
                return False

        def monitor(*_args, **kwargs):
            monitor_kwargs.update(kwargs)
            raise OSError("monitor output failed")

        self.executor.side_effect = CheckedExecutor
        self.monitors.side_effect = monitor
        with self.assertRaisesRegex(OSError, "monitor output failed"):
            self.run_file("--monitor", "energy:rbv", "--move", "energy=1")
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()

    def test_monitor_error_is_preserved_when_stop_and_close_also_fail(self):
        failure = OSError("monitor output failed")
        self.monitors.side_effect = failure
        self.energy.stop.side_effect = RuntimeError("stop failed")
        self.energy.close.side_effect = RuntimeError("close failed")
        with self.assertLogs(cli.logger, level="ERROR") as captured, \
                self.assertRaises(OSError) as caught:
            self.run_file("--monitor", "energy:rbv", "--move", "energy=1")
        self.assertIs(caught.exception, failure)
        self.assertIn("stop failed", "\n".join(captured.output))
        self.assertIn("close failed", "\n".join(captured.output))
        self.slit.stop.assert_called_once_with()
        self.slit.close.assert_called_once_with()

    def test_worker_failure_remains_visible_when_stopping_fails(self):
        self.energy.run_move.side_effect = RuntimeError("motion failed")
        self.energy.stop.side_effect = RuntimeError("stop failed")
        with self.assertLogs(cli.logger, level="ERROR") as captured, \
                self.assertRaises(SystemExit) as caught:
            self.run_file("--move", "energy=1")
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("motion failed", "\n".join(captured.output))
        self.assertIn("stop failed", "\n".join(captured.output))
        self.slit.stop.assert_called_once_with()
        self.slit.close.assert_called_once_with()

    def test_rejected_jog_is_an_unsuccessful_command(self):
        self.energy.jog.return_value = False
        with self.assertLogs(cli.logger, level="ERROR") as captured, \
                self.assertRaises(SystemExit) as caught:
            self.run_file("--jog", "energy=1")
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("energy", "\n".join(captured.output))
        self.assertNotIn("Done.", self.output.getvalue())

    def test_submission_failure_stops_motion_before_executor_teardown(self):
        test_case = self

        class FailingExecutor(_ImmediateExecutor):
            def submit(self, function, *args, **kwargs):
                if self.futures:
                    raise RuntimeError("submission failed")
                return super().submit(function, *args, **kwargs)

            def __exit__(self, *_args):
                test_case.energy.stop.assert_called_once_with()
                test_case.energy.close.assert_not_called()
                return False

        self.executor.side_effect = FailingExecutor
        with self.assertRaisesRegex(RuntimeError, "submission failed"):
            self.run_file("--move", "energy=1", "--move", "slit=2")
        self.energy.run_move.assert_called_once_with(1.0, True)
        self.slit.run_move.assert_not_called()
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()

    def test_worker_failure_after_monitors_finish_stops_peer_before_teardown(self):
        """Duration/count may finish keep-alive monitoring before workers finish."""
        stop_event = threading.Event()
        commands = []
        test_case = self

        class DeferredExecutor(_ImmediateExecutor):
            def submit(self, function, *args, **kwargs):
                future = Future()
                future.set_running_or_notify_cancel()
                self.futures.append(future)
                commands.append((function, args, kwargs))
                return future

            def __exit__(self, *_args):
                test_case.assertTrue(stop_event.is_set())
                test_case.slit.stop.assert_called_once_with()
                test_case.assertTrue(self.futures[1].done())
                test_case.energy.close.assert_not_called()
                return False

        deferred = DeferredExecutor(max_workers=2)
        self.executor.side_effect = None
        self.executor.return_value = deferred
        self.energy.run_move.side_effect = RuntimeError("late worker failure")
        self.slit.stop.side_effect = lambda: deferred.futures[1].set_result(None)

        def finish_failed_worker(_timeout):
            function, args, kwargs = commands[0]
            try:
                function(*args, **kwargs)
            except RuntimeError as exc:
                deferred.futures[0].set_exception(exc)

        with patch.object(cli.threading, "Event", return_value=stop_event), \
                patch.object(stop_event, "wait", side_effect=finish_failed_worker), \
                self.assertLogs(cli.logger, level="ERROR") as captured, \
                self.assertRaises(SystemExit) as caught:
            self.run_file("--monitor", "energy:rbv", "--monitor-count", "1", "--keep-alive",
                          "--move", "energy=1", "--move", "slit=2")
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("late worker failure", "\n".join(captured.output))
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()

    def test_cleanup_continues_after_failure_and_cli_is_unsuccessful(self):
        self.energy.close.side_effect = RuntimeError("close failed")
        with self.assertLogs(cli.logger, level="ERROR") as captured, \
                self.assertRaises(SystemExit) as caught:
            self.run_file("--move", "energy=1")
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("close failed", "\n".join(captured.output))
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()
        self.assertNotIn("Done.", self.output.getvalue())

    def test_validation_failure_closes_loaded_actuators(self):
        with self.assertRaises(SystemExit) as caught:
            self.run_file("--move", "unknown=1")
        self.assertEqual(caught.exception.code, 2)
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()
        self.install_signal.assert_not_called()

    def test_cleanup_failure_does_not_replace_validation_error(self):
        self.energy.close.side_effect = RuntimeError("close failed")
        with self.assertLogs(cli.logger, level="ERROR"), \
                self.assertRaises(SystemExit) as caught:
            self.run_file("--move", "unknown=1")
        self.assertEqual(caught.exception.code, 2)
        self.slit.close.assert_called_once_with()

    def test_immediate_action_failure_closes_owned_actuators(self):
        failure = RuntimeError("velocity failed")
        self.energy.set_velocity.side_effect = failure
        with self.assertRaises(RuntimeError) as caught:
            self.run_file("--set-velocity", "energy=1")
        self.assertIs(caught.exception, failure)
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()
        self.executor.assert_not_called()

    def test_owned_actuator_with_multiple_names_is_closed_once(self):
        self.actuators["energy_alias"] = self.energy
        self.run_file("--stop", "energy")
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()

    def test_sigint_handler_is_restored_on_exceptional_exits(self):
        previous_handler = Mock(name="previous_sigint_handler")
        scenarios = ("worker", "monitor", "immediate", "close")
        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                self.energy.reset_mock(side_effect=True)
                self.slit.reset_mock(side_effect=True)
                self.monitors.reset_mock(side_effect=True)
                self.monitors.return_value = (0, 0)
                self.install_signal.reset_mock()
                self.install_signal.return_value = previous_handler
                if scenario == "worker":
                    self.energy.run_move.side_effect = RuntimeError("worker failed")
                    options = ("--move", "energy=1")
                    expected = SystemExit
                elif scenario == "monitor":
                    self.monitors.side_effect = OSError("monitor failed")
                    options = ("--monitor", "energy:rbv", "--monitor-count", "1")
                    expected = OSError
                elif scenario == "immediate":
                    self.energy.set_velocity.side_effect = RuntimeError("action failed")
                    options = ("--set-velocity", "energy=1")
                    expected = RuntimeError
                else:
                    self.energy.close.side_effect = RuntimeError("close failed")
                    options = ("--stop", "energy")
                    expected = SystemExit
                with patch.object(cli.logger, "error"), patch.object(cli.logger, "exception"), \
                        self.assertRaises(expected):
                    self.run_file(*options)
                self.assertEqual(self.install_signal.call_count, 2)
                self.install_signal.assert_called_with(signal.SIGINT, previous_handler)
                self.energy.close.assert_called_once_with()
                self.slit.close.assert_called_once_with()

    def test_sigint_installation_failure_still_closes_loaded_actuators(self):
        self.install_signal.side_effect = ValueError("signal only works in main thread")
        with self.assertRaisesRegex(ValueError, "signal only works in main thread"):
            self.run_file("--stop", "energy")
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()
        self.energy.stop.assert_not_called()

    def test_monitor_interruption_stops_closes_and_restores_handler(self):
        previous_handler = Mock(name="previous_sigint_handler")
        self.install_signal.return_value = previous_handler
        interruption = KeyboardInterrupt()
        self.monitors.side_effect = interruption
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.run_file("--monitor", "energy:rbv", "--move", "energy=1")
        self.assertIs(caught.exception, interruption)
        self.energy.stop.assert_called_once_with()
        self.slit.stop.assert_called_once_with()
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()
        self.install_signal.assert_called_with(signal.SIGINT, previous_handler)

    def test_commands_serialize_per_actuator_and_different_actuators_overlap(self):
        energy_started = threading.Event()
        slit_started = threading.Event()
        release_energy = threading.Event()
        first_energy_done = threading.Event()

        def move_energy(position, _sync):
            if position == 1:
                energy_started.set()
                if not release_energy.wait(3):
                    raise RuntimeError("test did not release first energy command")
                first_energy_done.set()
            elif not first_energy_done.is_set():
                raise RuntimeError("commands on one actuator overlapped")

        def move_slit(_position, _sync):
            if not energy_started.wait(3):
                raise RuntimeError("first energy command did not start")
            if first_energy_done.is_set():
                raise RuntimeError("different actuators did not run concurrently")
            slit_started.set()

        def monitor(*_args, **_kwargs):
            try:
                self.assertTrue(slit_started.wait(3), "slit command did not overlap energy")
                self.assertFalse(first_energy_done.is_set())
            finally:
                release_energy.set()
            return 0, 0

        self.energy.run_move.side_effect = move_energy
        self.slit.run_move.side_effect = move_slit
        self.monitors.side_effect = monitor
        self.executor.side_effect = ThreadPoolExecutor
        self.addCleanup(release_energy.set)
        self.run_file("--monitor", "energy:rbv", "--move", "energy=1",
                      "--move", "slit=1", "--move", "energy=2")
        self.assertEqual(self.energy.run_move.call_args_list, [call(1.0, True), call(2.0, True)])
        self.energy.close.assert_called_once_with()
        self.slit.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
