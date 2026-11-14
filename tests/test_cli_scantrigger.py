# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Exercise standalone trigger dispatch, configuration, and resource cleanup."""

import argparse
import io
import os
import unittest
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from unittest.mock import Mock, call, patch

from kiwi_scan.cli import scantrigger_cli as cli


class TestScanTriggerCLI(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {"KIWI_SCAN_CONFIG_DIR": "/presets"}))
        self.index = self.stack.enter_context(patch.object(cli, "load_scan_configs", return_value={"mono": "mono.yaml"}))
        self.load = self.stack.enter_context(patch.object(cli, "_load_raw_config", return_value=({}, "/tmp/scan.yaml")))
        self.help = self.stack.enter_context(patch.object(cli, "print_required_replacements"))
        self.manager = Mock(spec=cli.TriggerManager)
        self.real_create = cli.TriggerManager.from_config
        self.create = self.stack.enter_context(patch.object(cli.TriggerManager, "from_config", return_value=self.manager))

    def run_cli(self, arguments):
        with patch("sys.argv", ["scantrigger_cli"] + arguments):
            return cli.main()

    def test_dispatches_selected_phase_from_loaded_config(self):
        self.assertIsNone(self.run_cli(["--config-file", "scan.yaml", "--phase", "after"]))
        self.create.assert_called_once()
        self.manager.fire.assert_called_once_with("after")
        self.help.assert_called_once_with(self.load.call_args[0][0], "/presets", "/tmp/scan.yaml")

    def test_custom_phase_and_preset_are_forwarded(self):
        self.run_cli(["--config", "mono", "--phase", "custom", "--replace", "IOC=TEST"])
        args, config_dir = self.load.call_args[0]
        self.assertEqual(config_dir, "/presets")
        self.assertEqual((args.config, args.replace), ("mono", ["IOC=TEST"]))
        self.manager.fire.assert_called_once_with("custom")

    def test_logging_level_is_applied(self):
        with patch.object(cli, "set_valid_logging_level") as set_level:
            self.run_cli(["--config", "mono", "--phase", "after", "--log-level", "1"])
        set_level.assert_called_once_with(1)

    def test_invalid_arguments_never_load_or_fire(self):
        for args in ([], ["--config", "mono"], ["--config", "unknown", "--phase", "after"],
                     ["--config", "mono", "--config-file", "x", "--phase", "after"]):
            with self.subTest(args=args):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    self.run_cli(args)
                self.assertEqual(error.exception.code, 2)
        self.load.assert_not_called()
        self.create.assert_not_called()
        self.manager.close.assert_not_called()

    def test_missing_preset_directory_still_allows_explicit_file(self):
        self.index.side_effect = FileNotFoundError("missing presets")
        self.run_cli(["--config-file", "scan.yaml", "--phase", "before"])
        self.manager.fire.assert_called_once_with("before")

    def test_config_errors_use_exit_two_and_prevent_trigger_creation(self):
        for failure in (FileNotFoundError("missing config"), ValueError("bad YAML"), TypeError("not a mapping"), KeyError("pv")):
            with self.subTest(failure=failure):
                self.load.side_effect = failure
                output = io.StringIO()
                with redirect_stderr(output), self.assertRaises(SystemExit) as error:
                    self.run_cli(["--config-file", "scan.yaml", "--phase", "before"])
                self.assertEqual(error.exception.code, 2)
                self.assertNotIn("Traceback", output.getvalue())
        self.create.assert_not_called()
        self.manager.close.assert_not_called()

    def test_trigger_resources_are_closed_after_success(self):
        self.assertIsNone(self.run_cli(["--config-file", "scan.yaml", "--phase", "after"]))
        self.assertEqual(self.manager.method_calls, [call.fire("after"), call.close()])

    def test_trigger_resources_are_closed_when_firing_fails(self):
        self.manager.fire.side_effect = ValueError("bad trigger value")
        output = io.StringIO()
        with redirect_stderr(output), self.assertRaises(SystemExit) as error:
            self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
        self.assertEqual(error.exception.code, 2)
        self.assertIn("bad trigger value", output.getvalue())
        self.assertEqual(self.manager.method_calls, [call.fire("after"), call.close()])

    def test_manager_construction_failure_reports_usage_error_without_cleanup(self):
        self.create.side_effect = ValueError("invalid trigger configuration")
        output = io.StringIO()
        with redirect_stderr(output), self.assertRaises(SystemExit) as error:
            self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
        self.assertEqual(error.exception.code, 2)
        self.assertIn("invalid trigger configuration", output.getvalue())
        self.create.assert_called_once()
        self.manager.fire.assert_not_called()
        self.manager.close.assert_not_called()

    def test_trigger_resources_are_closed_when_firing_is_interrupted(self):
        failure = KeyboardInterrupt()
        self.manager.fire.side_effect = failure
        with self.assertRaises(KeyboardInterrupt) as error:
            self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
        self.assertIs(error.exception, failure)
        self.assertEqual(self.manager.method_calls, [call.fire("after"), call.close()])

    def test_trigger_resources_are_closed_for_unhandled_firing_error(self):
        failure = RuntimeError("trigger firing failed")
        self.manager.fire.side_effect = failure
        with self.assertRaises(RuntimeError) as error:
            self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
        self.assertIs(error.exception, failure)
        self.assertEqual(self.manager.method_calls, [call.fire("after"), call.close()])

    def test_cleanup_failure_is_logged_after_success(self):
        failure = RuntimeError("trigger cleanup failed")
        self.manager.close.side_effect = failure
        with self.assertLogs(level="ERROR") as logs:
            self.assertIsNone(self.run_cli(["--config-file", "scan.yaml", "--phase", "after"]))
        self.assertIn("trigger cleanup failed", "\n".join(logs.output))
        self.assertIs(logs.records[0].exc_info[1], failure)
        self.assertEqual(self.manager.method_calls, [call.fire("after"), call.close()])

    def test_cleanup_failure_preserves_firing_usage_error(self):
        self.manager.fire.side_effect = ValueError("bad trigger value")
        self.manager.close.side_effect = RuntimeError("trigger cleanup failed")
        output = io.StringIO()
        with self.assertLogs(level="ERROR") as logs, redirect_stderr(output), \
                self.assertRaises(SystemExit) as error:
            self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
        self.assertEqual(error.exception.code, 2)
        self.assertIn("bad trigger value", output.getvalue())
        self.assertIn("trigger cleanup failed", "\n".join(logs.output))
        self.assertEqual(self.manager.method_calls, [call.fire("after"), call.close()])

    def test_cleanup_failure_preserves_unhandled_error_and_interruption(self):
        for failure in (RuntimeError("trigger firing failed"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__):
                self.manager.reset_mock()
                self.manager.fire.side_effect = failure
                self.manager.close.side_effect = RuntimeError("trigger cleanup failed")
                with self.assertLogs(level="ERROR") as logs, self.assertRaises(type(failure)) as error:
                    self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
                self.assertIs(error.exception, failure)
                self.assertIn("trigger cleanup failed", "\n".join(logs.output))
                self.assertEqual(self.manager.method_calls, [call.fire("after"), call.close()])

    def test_real_manager_preserves_write_order_and_waits_before_cleanup(self):
        self.load.return_value = ({"triggers": {"after": [
            {"pv": "TEST:FIRST", "value": 1, "delay": 0.1},
            {"pv": "TEST:SECOND", "value": 2, "put_timeout": 0.5},
            {"pv": "TEST:THIRD", "value": 3},
        ]}}, "/tmp/scan.yaml")
        first, second, third = Mock(), Mock(), Mock()
        first.put.return_value = third.put.return_value = True
        completion = second.put_with_completion.return_value
        completion.wait.return_value = True
        events = Mock()
        for name, action in (("first_put", first.put), ("second_put", second.put_with_completion),
                             ("wait", completion.wait), ("third_put", third.put),
                             ("first_close", first.close), ("second_close", second.close),
                             ("third_close", third.close)):
            events.attach_mock(action, name)
        with ExitStack() as stack:
            stack.enter_context(patch.object(cli.TriggerManager, "from_config", side_effect=self.real_create))
            create_pv = stack.enter_context(patch("kiwi_scan.scan.trigger_manager.EpicsPV",
                                                  side_effect=[first, second, third]))
            delay = stack.enter_context(patch("kiwi_scan.scan.trigger_manager.time.sleep"))
            events.attach_mock(delay, "delay")
            self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
        self.assertEqual([args[0][0] for args in create_pv.call_args_list],
                         ["TEST:FIRST", "TEST:SECOND", "TEST:THIRD"])
        self.assertEqual(events.mock_calls, [
            call.first_put(1), call.delay(0.1),
            call.second_put(2, timeout=0.5), call.wait(None), call.third_put(3),
            call.first_close(), call.second_close(), call.third_close(),
        ])

    def test_permission_error_is_reported_as_an_argument_error(self):
        self.load.side_effect = PermissionError("cannot read scan.yaml")
        output = io.StringIO()
        with redirect_stderr(output), self.assertRaises(SystemExit) as error:
            self.run_cli(["--config-file", "scan.yaml", "--phase", "after"])
        self.assertEqual(error.exception.code, 2)
        self.assertIn("cannot read scan.yaml", output.getvalue())
        self.assertNotIn("Traceback", output.getvalue())
        self.create.assert_not_called()
        self.manager.close.assert_not_called()


class TestScanTriggerConfigLoading(unittest.TestCase):
    def test_explicit_path_is_expanded_and_environment_replacements_win(self):
        args = argparse.Namespace(config_file="scan.yaml", config=None, replace=["IOC=cli", "AXIS=x"])
        with patch.object(cli.os.path, "isfile", return_value=True), \
                patch.object(cli, "get_env_replacements", return_value={"IOC": "env"}), \
                patch.object(cli, "yaml_loader", return_value={"triggers": {}}) as load:
            raw, origin = cli._load_raw_config(args, "/presets")
        self.assertEqual(origin, os.path.abspath("scan.yaml"))
        self.assertEqual(raw, {"triggers": {}})
        load.assert_called_once_with(origin, {"IOC": "env", "AXIS": "x"})

    def test_missing_explicit_file_is_rejected_before_yaml_load(self):
        args = argparse.Namespace(config_file="missing.yaml", config=None, replace=[])
        with patch.object(cli.os.path, "isfile", return_value=False), \
                patch.object(cli, "yaml_loader") as load, \
                self.assertRaisesRegex(FileNotFoundError, "--config-file not found"):
            cli._load_raw_config(args, "/presets")
        load.assert_not_called()

    def test_replacement_help_uses_the_selected_file_directory(self):
        args = argparse.Namespace(config_file="/tmp/scan.yaml", config=None)
        with patch.object(cli, "get_replacements_help_and_required", return_value=("Required: IOC", True)) as help_fn, \
                redirect_stdout(io.StringIO()) as output:
            cli.print_required_replacements(args, "/presets", "/tmp/scan.yaml")
        help_fn.assert_called_once_with("/tmp", ["scan.yaml"])
        self.assertIn("Required: IOC", output.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
