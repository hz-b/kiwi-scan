# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Test CLI subscriptions, finite loops, formatting, and cleanup without EPICS."""

import argparse
import io
import os
import unittest
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from kiwi_scan.cli import pollstats_cli as cli


class TestPollStatsCLI(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stat = SimpleNamespace(name="DET:A", role="stat")
        self.sync = SimpleNamespace(name="DET:B", role="sync")
        self.config = SimpleNamespace(subscriptions=[self.stat, self.sync], actuators={"x": object()})
        self.load = self.stack.enter_context(patch.object(cli, "_load_config", return_value=self.config))
        self.collector = Mock()
        self.collector.get_headers.return_value = ["mean", "std"]
        self.collector.get_values.return_value = [2.5, 0.1]
        self.make_collector = self.stack.enter_context(patch.object(cli, "StatsCollector", return_value=self.collector))
        self.manager = Mock()
        self.make_manager = self.stack.enter_context(patch.object(cli, "SubscriptionManager", return_value=self.manager))
        self.clock = self.stack.enter_context(patch.object(cli.time, "monotonic", side_effect=[0.0, 0.0, 1.0]))
        self.sleep = self.stack.enter_context(patch.object(cli.time, "sleep"))
        self.row = self.stack.enter_context(patch.object(cli, "_print_row"))
        self.real_multiline = cli._print_row_multiline
        self.multiline = self.stack.enter_context(patch.object(cli, "_print_row_multiline"))
        self.output = self.stack.enter_context(redirect_stdout(io.StringIO()))

    def run_cli(self, *arguments):
        return cli.main(["--config-file", "scan.yaml", "--duration", "1"] + list(arguments))

    def test_default_stat_role_filters_subscriptions_and_cleans_up(self):
        self.assertEqual(self.run_cli(), 0)
        self.make_collector.assert_called_once_with([self.stat], role="stat")
        self.make_manager.assert_called_once_with([self.stat], actuator_configs=self.config.actuators, actuators=None)
        self.manager.start.assert_called_once_with()
        self.manager.stop.assert_called_once_with()
        self.assertEqual(self.manager.register_role.call_args[0][0], "stat")
        callback = self.manager.register_role.call_args[0][1]
        event = object()
        callback(event, self.stat)
        self.collector.update.assert_called_once_with(event, self.stat, collect=True)
        self.assertEqual(self.output.getvalue(), "TS-ISO8601\tmean\tstd\n")
        self.assertEqual(self.row.call_args[0][0][1:], [2.5, 0.1])
        self.multiline.assert_not_called()

    def test_selected_role_and_no_timestamp_are_forwarded(self):
        self.assertEqual(self.run_cli("--role", "sync", "--no-timestamp"), 0)
        self.make_collector.assert_called_once_with([self.sync], role="sync")
        self.row.assert_called_once_with([2.5, 0.1], sep="\t")
        self.assertEqual(self.output.getvalue(), "mean\tstd\n")

    def test_print_interval_and_window_reset(self):
        self.assertEqual(self.run_cli("--interval", "0.25", "--reset-each-print"), 0)
        self.sleep.assert_called_once_with(0.25)
        self.collector.reset_window.assert_called_once_with()

    def test_more_than_two_subscriptions_select_multiline(self):
        self.config.subscriptions = [self.stat, self.stat, self.stat]
        self.assertEqual(self.run_cli(), 0)
        self.multiline.assert_called_once()
        self.assertEqual(self.multiline.call_args[0][0], ["TS-ISO8601", "mean", "std"])
        self.row.assert_not_called()

    def test_multiline_without_timestamp_labels_the_first_statistic(self):
        self.config.subscriptions = [self.stat, self.stat, self.stat]
        self.collector.get_headers.return_value = ["mean-A", "mean-B", "mean-C"]
        self.collector.get_values.return_value = [11, 22, 33]
        self.multiline.side_effect = self.real_multiline
        self.assertEqual(self.run_cli("--no-timestamp"), 0)
        header, blank, *rows = self.output.getvalue().splitlines()
        self.assertEqual(header, "mean-A\tmean-B\tmean-C")
        self.assertEqual(blank, "")
        self.assertEqual(
            [row.split() for row in rows],
            [["mean-A", "11"], ["mean-B", "22"], ["mean-C", "33"]],
        )

    def test_zero_duration_starts_and_stops_without_reading_or_sleeping(self):
        self.assertEqual(self.run_cli("--duration", "0"), 0)
        self.sleep.assert_not_called()
        self.collector.get_values.assert_not_called()
        self.manager.start.assert_called_once_with()
        self.manager.stop.assert_called_once_with()

    def test_keyboard_interrupt_returns_130_and_stops_subscriptions(self):
        self.sleep.side_effect = KeyboardInterrupt
        self.assertEqual(self.run_cli(), 130)
        self.manager.stop.assert_called_once_with()

    def test_read_error_propagates_after_stopping_subscriptions(self):
        self.collector.get_values.side_effect = RuntimeError("statistics unavailable")
        with self.assertRaisesRegex(RuntimeError, "statistics unavailable"):
            self.run_cli()
        self.manager.stop.assert_called_once_with()

    def test_connection_error_is_logged_and_returns_one(self):
        self.manager.start.side_effect = ConnectionError("IOC unavailable")
        with self.assertLogs(cli.logger, level="ERROR") as logs:
            self.assertEqual(self.run_cli(), 1)
        self.assertIn("IOC unavailable", " ".join(logs.output))
        self.collector.get_values.assert_not_called()

    def test_stop_error_is_logged_without_replacing_original_failure(self):
        self.collector.get_values.side_effect = RuntimeError("read error")
        self.manager.stop.side_effect = OSError("stop error")
        with self.assertLogs(cli.logger, level="ERROR") as logs, \
                self.assertRaisesRegex(RuntimeError, "read error"):
            self.run_cli()
        self.assertIn("Failed to stop subscriptions", " ".join(logs.output))

    def test_missing_selected_role_is_rejected_before_starting(self):
        output = io.StringIO()
        with redirect_stderr(output), self.assertRaises(SystemExit) as error:
            self.run_cli("--role", "plugin")
        self.assertEqual(error.exception.code, 2)
        self.assertIn("role='plugin'", output.getvalue())
        self.make_manager.assert_not_called()

    def test_nonpositive_intervals_are_rejected_before_loading(self):
        for interval in ("0", "-1"):
            with self.subTest(interval=interval):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    self.run_cli("--interval", interval)
                self.assertEqual(error.exception.code, 2)
        self.load.assert_not_called()
        self.make_manager.assert_not_called()

    def test_config_errors_are_reported_without_starting_subscriptions(self):
        self.load.side_effect = ValueError("bad subscription configuration")
        output = io.StringIO()
        with redirect_stderr(output), self.assertRaises(SystemExit) as error:
            self.run_cli()
        self.assertEqual(error.exception.code, 2)
        self.assertIn("bad subscription configuration", output.getvalue())
        self.make_manager.assert_not_called()


class TestPollStatsConfigAndFormatting(unittest.TestCase):
    def test_explicit_config_path_and_environment_replacement_precedence(self):
        args = argparse.Namespace(config_file="scan.yaml", config=None, replace=["IOC=cli", "AXIS=x"])
        with patch.object(cli.os.path, "isfile", return_value=True), \
                patch.object(cli, "get_env_replacements", return_value={"IOC": "env"}), \
                patch.object(cli, "yaml_loader", return_value={"detector_pvs": []}) as load:
            config = cli._load_config(args)
        load.assert_called_once_with(os.path.abspath("scan.yaml"), {"IOC": "env", "AXIS": "x"})
        self.assertEqual(config.detector_pvs, [])

    def test_preset_uses_environment_config_directory(self):
        args = argparse.Namespace(config_file=None, config="mono", replace=[])
        with patch.dict(os.environ, {"KIWI_SCAN_CONFIG_DIR": "/presets"}), \
                patch.object(cli, "get_env_replacements", return_value={}), \
                patch.object(cli, "yaml_loader", return_value={"detector_pvs": []}) as load:
            cli._load_config(args)
        load.assert_called_once_with("/presets/mono.yaml", {})

    def test_missing_explicit_file_does_not_load_yaml(self):
        args = argparse.Namespace(config_file="missing.yaml", config=None, replace=[])
        with patch.object(cli.os.path, "isfile", return_value=False), \
                patch.object(cli, "yaml_loader") as load, \
                self.assertRaises(FileNotFoundError):
            cli._load_config(args)
        load.assert_not_called()

    def test_values_are_formatted_without_losing_strings_or_missing_values(self):
        self.assertEqual(cli._format_value(None), "")
        self.assertEqual(cli._format_value(1.23456789), "1.23457")
        self.assertEqual(cli._format_value("offline"), "offline")

    def test_compact_row_obeys_terminal_width(self):
        with patch.object(cli.shutil, "get_terminal_size", return_value=os.terminal_size((12, 20))), \
                patch.object(cli, "_LAST_LINE", ""), redirect_stdout(io.StringIO()) as output:
            cli._print_row(["0123456789abcdef"])
            self.assertEqual(cli._LAST_LINE, "01234567 ...")
        self.assertEqual(output.getvalue(), "\r\033[2K01234567 ...")

    def test_multiline_row_labels_each_statistic_after_timestamp(self):
        with redirect_stdout(io.StringIO()) as output:
            cli._print_row_multiline(["TS-ISO8601", "mean", "std"], ["2026-11-10", 2.5, None])
        self.assertIn("2026-11-10", output.getvalue())
        self.assertIn("mean", output.getvalue())
        self.assertIn("2.5", output.getvalue())
        self.assertIn("std", output.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
