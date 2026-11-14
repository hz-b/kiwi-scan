# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""CLI input and lifecycle contracts without PV connections or scan workers."""

import logging
import os
import signal
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, call, patch

from kiwi_scan.cli import scan_runner as cli
from kiwi_scan.datamodels import ActuatorConfig, ScanConfig, ScanDimension

DIM = "actuator=x,start=0,stop=1,steps=2"


class TestScanRunner(unittest.TestCase):
    """Keep argument parsing and model conversion real; mock hardware entry points."""

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "scan.yaml"
        self.path.write_text(
            "actuators:\n  x:\n    type: sim\ndetector_pvs: []\n", encoding="utf-8"
        )
        self.config = ScanConfig(
            actuators={"x": ActuatorConfig(type="sim")},
            detector_pvs=[],
            scan_dimensions=[ScanDimension("yaml-axis", 10, 20, 3)],
        )
        self.args = ["--config-file", str(self.path), "--dim", DIM]
        self.scan = Mock(spec=["execute", "stop", "close"])
        self.factory = Mock(return_value=self.scan)
        self.previous_handler = object()
        self.signals = Mock()
        self.manifest = Mock(return_value=self.path.with_name("manifest.yaml"))
        self.index = Mock(return_value={"preset": self.config})
        self.original_loader = cli._load_config_from_path
        self.loader = Mock(return_value=self.config)
        self.plugins = Mock()
        self.logging_level = Mock()
        root_logger = logging.getLogger()
        self.addCleanup(root_logger.setLevel, root_logger.level)

        environment = patch.dict(os.environ, {}, clear=True)
        self.addCleanup(environment.stop)
        environment.start()
        cli_patches = patch.multiple(
            cli,
            SCAN_REGISTRY={"linear": object(), "cm": object()},
            load_all_scan_types=Mock(),
            get_scan_config_dir=Mock(return_value="/presets"),
            load_scan_configs=self.index,
            _load_config_from_path=self.loader,
            create_scan_with_config=self.factory,
            set_valid_logging_level=self.logging_level,
        )
        self.addCleanup(cli_patches.stop)
        cli_patches.start()
        self._patch(cli.kiwi_scan, "load_all_plugins", self.plugins)
        self._patch(cli.ManifestWriter, "newmanifest", self.manifest)
        self._patch(cli.signal, "getsignal", Mock(return_value=self.previous_handler))
        self._patch(cli.signal, "signal", self.signals)
        self._patch(cli.os, "_exit", Mock(side_effect=AssertionError("unexpected forced exit")))

    def _patch(self, target, name, value):
        patcher = patch.object(target, name, value)
        self.addCleanup(patcher.stop)
        patcher.start()

    def assert_no_scan_side_effects(self):
        self.manifest.assert_not_called()
        self.factory.assert_not_called()
        self.plugins.assert_not_called()
        self.scan.execute.assert_not_called()
        self.scan.stop.assert_not_called()
        self.scan.close.assert_not_called()
        self.signals.assert_not_called()

    def assert_usage_error(self, args, *diagnostics):
        stderr = StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
            cli.main(args)
        self.assertEqual(caught.exception.code, 2)
        for diagnostic in diagnostics:
            self.assertIn(diagnostic, stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assert_no_scan_side_effects()

    def test_explicit_config_dispatches_and_cli_dimensions_replace_yaml(self):
        self.assertEqual(cli.main(self.args), 0)
        self.loader.assert_called_once_with(str(self.path), {})
        self.factory.assert_called_once_with(
            scantype="linear", config=self.config, data_dir=None
        )
        self.assertEqual(self.config.scan_dimensions, [ScanDimension("x", 0, 1, 2)])
        self.plugins.assert_called_once_with()
        self.scan.execute.assert_called_once_with()
        self.scan.stop.assert_not_called()
        self.scan.close.assert_called_once_with()
        self.manifest.assert_not_called()
        self.assertEqual(
            self.signals.call_args_list[-1], call(signal.SIGINT, self.previous_handler)
        )

    def test_multiple_dimensions_and_selected_scan_type_are_forwarded(self):
        args = self.args + [
            "--scan_type", "cm", "--dim", "actuator=y,start=5,stop=2,steps=0,velocity=0.5"
        ]
        self.assertEqual(cli.main(args), 0)
        self.assertEqual(self.factory.call_args.kwargs["scantype"], "cm")
        self.assertEqual(self.config.scan_dimensions, [
            ScanDimension("x", 0, 1, 2), ScanDimension("y", 5, 2, 0, 0.5)
        ])

    def test_data_directory_environment_and_replacement_precedence(self):
        os.environ["KIWI_SCAN_DATA_DIR"] = "/chosen-output"
        os.environ["KIWI_SCAN_REPLACE_IOC"] = "environment-ioc"
        self.assertEqual(
            cli.main(self.args + ["--replace", "IOC=cli-ioc", "PV=signal=raw"]), 0
        )
        # Preserve the existing environment-over-CLI precedence.
        self.loader.assert_called_once_with(
            str(self.path), {"IOC": "environment-ioc", "PV": "signal=raw"}
        )
        self.assertEqual(self.factory.call_args.kwargs["data_dir"], "/chosen-output")

    def test_preset_config_uses_config_directory_environment(self):
        os.environ["KIWI_SCAN_CONFIG_DIR"] = "/chosen-presets"
        self._patch(cli, "get_replacements_help_and_required", Mock(return_value=("", False)))
        self.assertEqual(
            cli.main(["--config", "preset", "--dim", DIM, "--replace", "IOC=test"]), 0
        )
        self.assertEqual(self.index.call_args_list, [
            call("/chosen-presets", None), call("/chosen-presets", {"IOC": "test"})
        ])
        self.loader.assert_not_called()

    def test_missing_preset_directory_does_not_block_explicit_file(self):
        self.index.side_effect = FileNotFoundError("no presets")
        self.assertEqual(cli.main(self.args), 0)
        self.scan.execute.assert_called_once_with()

    def test_required_replacements_are_displayed_for_explicit_file(self):
        self._patch(
            cli, "get_replacements_help_and_required", Mock(return_value=("Required: IOC", True))
        )
        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(cli.main(self.args), 0)
        self.assertIn("Required: IOC", stdout.getvalue())

    def test_explicit_yaml_is_parsed_with_placeholder_replacements(self):
        self.path.write_text(
            "actuators:\n  x:\n    type: sim\n    pv: '${IOC}:SET'\ndetector_pvs: []\n",
            encoding="utf-8",
        )
        self._patch(cli, "_load_config_from_path", self.original_loader)
        with redirect_stdout(StringIO()):
            self.assertEqual(cli.main(self.args + ["--replace", "IOC=TEST"]), 0)
        config = self.factory.call_args.kwargs["config"]
        self.assertEqual(config.actuators["x"].pv, "TEST:SET")
        self.assertEqual(config.scan_dimensions, [ScanDimension("x", 0, 1, 2)])

    def test_standalone_manifest_needs_no_config_or_plugins(self):
        os.environ["KIWI_SCAN_DATA_DIR"] = "/output"
        for option in ("--new-manifest", "--newmanifest"):
            with self.subTest(option=option):
                self.manifest.reset_mock()
                self.logging_level.reset_mock()
                self.assertEqual(cli.main([option, "--log-level", "1"]), 0)
                self.manifest.assert_called_once_with(directory="/output")
                self.logging_level.assert_called_once_with(1)
                self.index.assert_not_called()
                self.loader.assert_not_called()
                self.plugins.assert_not_called()
                self.factory.assert_not_called()

    def test_manifest_then_scan_dispatch_order(self):
        events = Mock()
        validate = self.config.validate

        def validate_resolved_config():
            self.assertEqual(self.config.scan_dimensions, [ScanDimension("x", 0, 1, 2)])
            validate()

        validator = Mock(side_effect=validate_resolved_config)
        self._patch(self.config, "validate", validator)
        events.attach_mock(validator, "validate")
        events.attach_mock(self.manifest, "manifest")
        events.attach_mock(self.factory, "factory")
        events.attach_mock(self.scan.execute, "execute")
        self.assertEqual(cli.main(["--new-manifest"] + self.args), 0)
        self.assertEqual(
            [entry[0] for entry in events.mock_calls], ["validate", "manifest", "factory", "execute"]
        )

    def test_invalid_scan_config_does_not_change_active_manifest(self):
        self.config.detector_reader_strategy = "unsupported"
        with self.assertLogs(cli.logger, level="ERROR") as captured:
            self.assertEqual(cli.main(self.args + ["--new-manifest"]), 1)
        self.assertIn("detector_reader_strategy", "\n".join(captured.output))
        self.assertIn("Failed to initialize scan", "\n".join(captured.output))
        self.assert_no_scan_side_effects()

    def test_manifest_creation_error_is_usage_error(self):
        for exception in (OSError("read only"), ValueError("bad directory")):
            with self.subTest(exception=exception):
                self.manifest.side_effect = exception
                stderr = StringIO()
                with redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
                    cli.main(["--new-manifest"])
                self.assertEqual(caught.exception.code, 2)
                self.assertIn("failed to create manifest", stderr.getvalue())
                self.factory.assert_not_called()
                self.plugins.assert_not_called()
                self.index.assert_not_called()

    def test_bad_dimensions_fail_before_manifest_or_scan(self):
        cases = [
            ("", "must not be empty"),
            (DIM + ",", "empty comma-separated field"),
            (DIM + ",velocity", "not KEY=VALUE"),
            (DIM + ",=bad", "empty key"),
            (DIM + ",velocity=", "empty value"),
            (DIM + ",steps=3", "duplicate key"),
            (DIM.replace("steps=2", "step=2"), "did you mean 'steps'"),
            ("actuator=x,start=0,stop=1", "missing required key"),
            (DIM.replace("start=0", "start=bad"), "start must be a number"),
            (DIM.replace("stop=1", "stop=bad"), "stop must be a number"),
            (DIM.replace("steps=2", "steps=1.5"), "steps must be an integer"),
            (DIM + ",velocity=bad", "velocity must be a number"),
            (DIM.replace("steps=2", "steps=-1"), "steps must be >= 0"),
            (DIM + ",velocity=-1", "velocity must be >= 0"),
        ]
        for dimension, diagnostic in cases:
            with self.subTest(dimension=dimension):
                self.assert_usage_error(
                    ["--config-file", str(self.path), "--new-manifest", "--dim", dimension],
                    diagnostic,
                )

    def test_argparse_errors_do_not_initialize_scan(self):
        for extra in (["--scan_type", "unknown"], ["--log-level", "6"], ["--unknown"]):
            with self.subTest(extra=extra):
                self.assert_usage_error(self.args + ["--new-manifest"] + extra)

    def test_missing_config_file_is_usage_error(self):
        self.assert_usage_error(
            ["--config-file", str(self.path.with_name("missing.yaml")), "--dim", DIM, "--new-manifest"],
            "--config-file not found",
        )

    def test_config_loading_errors_do_not_create_manifest_or_scan(self):
        for exception in (
            OSError("denied"), ValueError("Invalid subscriptions[0]"), TypeError("invalid model type")
        ):
            with self.subTest(exception=exception):
                self.loader.side_effect = exception
                self.assert_usage_error(
                    self.args + ["--new-manifest"], "failed to load --config-file", str(exception)
                )

    def test_factory_failure_returns_one_without_executing_scan(self):
        for failure in (RuntimeError("init failed"), None):
            with self.subTest(failure=failure):
                self.factory.return_value = None
                self.factory.side_effect = failure
                with self.assertLogs(cli.logger, level="ERROR") as captured:
                    self.assertEqual(cli.main(self.args), 1)
                self.assertIn("Failed to initialize scan", "\n".join(captured.output))
                self.scan.execute.assert_not_called()
                self.scan.close.assert_not_called()
                self.signals.assert_not_called()

    def test_plugin_discovery_failure_prevents_factory(self):
        self.plugins.side_effect = RuntimeError("broken plugin")
        with self.assertLogs(cli.logger, level="ERROR") as captured:
            self.assertEqual(cli.main(self.args), 1)
        self.assertIn("broken plugin", "\n".join(captured.output))
        self.factory.assert_not_called()
        self.scan.execute.assert_not_called()

    def test_execute_failure_stops_scan_and_restores_signal(self):
        for stop_error in (None, RuntimeError("stop failed")):
            with self.subTest(stop_error=stop_error):
                self.scan.reset_mock()
                self.scan.execute.side_effect = RuntimeError("acquisition failed")
                self.scan.stop.side_effect = stop_error
                with self.assertLogs(cli.logger, level="ERROR") as captured:
                    self.assertEqual(cli.main(self.args), 1)
                self.assertIn("acquisition failed", "\n".join(captured.output))
                self.scan.stop.assert_called_once_with()
                self.scan.close.assert_called_once_with()
                self.assertEqual(
                    self.signals.call_args_list[-1], call(signal.SIGINT, self.previous_handler)
                )

    def test_first_ctrl_c_requests_stop_without_terminating(self):
        def interrupt():
            handler = self.signals.call_args_list[0].args[1]
            handler(signal.SIGINT, None)

        self.scan.execute.side_effect = interrupt
        stderr = StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(cli.main(self.args), 0)
        self.scan.stop.assert_called_once_with()
        self.assertIn("stopping scan actuators", stderr.getvalue())
        cli.os._exit.assert_not_called()

    def test_second_ctrl_c_forces_exit_130_and_restores_signal(self):
        forced_exit = Mock(side_effect=SystemExit(130))
        self._patch(cli.os, "_exit", forced_exit)

        def interrupt_twice():
            handler = self.signals.call_args_list[0].args[1]
            handler(signal.SIGINT, None)
            handler(signal.SIGINT, None)

        self.scan.execute.side_effect = interrupt_twice
        stderr = StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
            cli.main(self.args)
        self.assertEqual(caught.exception.code, 130)
        forced_exit.assert_called_once_with(130)
        self.assertIn("Second Ctrl-C", stderr.getvalue())
        self.scan.close.assert_called_once_with()
        self.assertEqual(
            self.signals.call_args_list[-1], call(signal.SIGINT, self.previous_handler)
        )

    def test_owned_scan_is_closed_after_execution(self):
        for execution_error in (None, RuntimeError("failed")):
            with self.subTest(execution_error=execution_error):
                self.scan.reset_mock()
                self.scan.execute.side_effect = execution_error
                with patch.object(cli.logger, "error"):
                    self.assertEqual(cli.main(self.args), 1 if execution_error else 0)
                self.scan.close.assert_called_once_with()

    def test_invalid_yaml_shape_is_reported_as_usage_error(self):
        self._patch(cli, "_load_config_from_path", self.original_loader)
        cases = [
            ("", "mapping"),
            ("- not-a-mapping\n", "mapping"),
            ("plugin_configs: {}\n", "plugin_configs"),
            ("detector_read_timeout: true\n", "detector_read_timeout"),
            ("42\n", "mapping"),
            ("null\n", "mapping"),
            ("[unclosed\n", "Invalid YAML"),
        ]
        for content, diagnostic in cases:
            with self.subTest(content=content):
                self.path.write_text(content, encoding="utf-8")
                self.assert_usage_error(
                    self.args + ["--new-manifest"], "failed to load --config-file", diagnostic
                )

    def test_nonfinite_dimensions_are_rejected_before_dispatch(self):
        for parameter in ("start", "stop", "velocity"):
            for value in ("nan", "inf", "-inf"):
                with self.subTest(parameter=parameter, value=value):
                    fields = {"actuator": "x", "start": "0", "stop": "1", "steps": "2"}
                    fields[parameter] = value
                    dimension = ",".join(f"{key}={item}" for key, item in fields.items())
                    self.assert_usage_error(
                        ["--config-file", str(self.path), "--new-manifest", "--dim", dimension],
                        parameter, "finite",
                    )

    def test_valid_negative_positions_descending_scans_and_zero_values(self):
        cases = [
            ("actuator=x,start=-2,stop=-1,steps=2", ScanDimension("x", -2, -1, 2)),
            ("actuator=x,start=2,stop=-2,steps=2", ScanDimension("x", 2, -2, 2)),
            ("actuator=x,start=-2,stop=-2,steps=0,velocity=0", ScanDimension("x", -2, -2, 0, 0)),
        ]
        for dimension, expected in cases:
            with self.subTest(dimension=dimension):
                self.factory.reset_mock()
                self.assertEqual(
                    cli.main(["--config-file", str(self.path), "--dim", dimension]), 0
                )
                self.assertEqual(self.config.scan_dimensions, [expected])
                self.factory.assert_called_once()

    def test_malformed_replacement_is_usage_error(self):
        for replacements in (["IOC"], ["PV=signal=raw", "IOC"]):
            with self.subTest(replacements=replacements):
                self.assert_usage_error(
                    self.args + ["--new-manifest", "--replace"] + replacements,
                    "invalid --replace", "IOC", "KEY=VALUE",
                )
                self.loader.assert_not_called()


class TestScanRunnerCleanup(unittest.TestCase):
    """Exercise CLI ownership and signal restoration without hardware access."""

    def setUp(self):
        self.scan = Mock(spec=["execute", "stop", "close"])
        self.config = ScanConfig(
            actuators={"x": ActuatorConfig(type="sim")}, detector_pvs=[]
        )
        self.factory = Mock(return_value=self.scan)
        self.args = ["--config", "preset", "--dim", DIM]
        self.previous_handler = signal.getsignal(signal.SIGINT)
        self.addCleanup(signal.signal, signal.SIGINT, self.previous_handler)

        environment = patch.dict(os.environ, {}, clear=True)
        self.addCleanup(environment.stop)
        environment.start()
        cli_patches = patch.multiple(
            cli,
            SCAN_REGISTRY={"linear": object()},
            load_all_scan_types=Mock(),
            get_scan_config_dir=Mock(return_value="/presets"),
            load_scan_configs=Mock(return_value={"preset": self.config}),
            get_replacements_help_and_required=Mock(return_value=("", False)),
            create_scan_with_config=self.factory,
        )
        self.addCleanup(cli_patches.stop)
        cli_patches.start()
        plugins = patch.object(cli.kiwi_scan, "load_all_plugins")
        self.addCleanup(plugins.stop)
        plugins.start()

    def test_cleanup_failure_keeps_execution_result_and_diagnostics(self):
        for execution_error in (None, RuntimeError("acquisition failed")):
            with self.subTest(execution_error=execution_error):
                self.scan.reset_mock()
                self.scan.execute.side_effect = execution_error
                cleanup_error = RuntimeError("cleanup failed")
                self.scan.close.side_effect = cleanup_error
                with self.assertLogs(cli.logger, level="ERROR") as captured:
                    result = cli.main(self.args)

                self.assertEqual(result, 1 if execution_error else 0)
                self.scan.close.assert_called_once_with()
                self.assertIn("cleanup failed", "\n".join(captured.output))
                self.assertIs(captured.records[-1].exc_info[1], cleanup_error)
                if execution_error:
                    self.assertIn("acquisition failed", "\n".join(captured.output))
                    self.scan.stop.assert_called_once_with()
                else:
                    self.scan.stop.assert_not_called()
                self.assertIs(signal.getsignal(signal.SIGINT), self.previous_handler)

    def test_sigint_handler_is_restored_when_cleanup_is_interrupted(self):
        interruption = KeyboardInterrupt("cleanup interrupted")
        self.scan.close.side_effect = interruption

        with self.assertRaises(KeyboardInterrupt) as caught:
            cli.main(self.args)

        self.assertIs(caught.exception, interruption)
        self.scan.close.assert_called_once_with()
        self.assertIs(signal.getsignal(signal.SIGINT), self.previous_handler)

    def test_execution_interruption_is_preserved_when_cleanup_fails(self):
        interruption = KeyboardInterrupt("execution interrupted")
        self.scan.execute.side_effect = interruption
        self.scan.close.side_effect = RuntimeError("cleanup failed")

        with self.assertRaises(KeyboardInterrupt) as caught, self.assertLogs(cli.logger, "ERROR") as captured:
            cli.main(self.args)

        self.assertIs(caught.exception, interruption)
        self.assertIn("cleanup failed", "\n".join(captured.output))
        self.scan.close.assert_called_once_with()
        self.scan.stop.assert_not_called()
        self.assertIs(signal.getsignal(signal.SIGINT), self.previous_handler)

    def test_execution_and_stop_failures_still_close_before_restoring_sigint(self):
        self.scan.execute.side_effect = RuntimeError("acquisition failed")
        self.scan.stop.side_effect = RuntimeError("stop failed")
        handlers_during_cleanup = []
        self.scan.close.side_effect = lambda: handlers_during_cleanup.append(
            signal.getsignal(signal.SIGINT)
        )

        with self.assertLogs(cli.logger, level="ERROR") as captured:
            self.assertEqual(cli.main(self.args), 1)

        self.assertEqual(self.scan.mock_calls, [call.execute(), call.stop(), call.close()])
        self.assertIn("acquisition failed", "\n".join(captured.output))
        self.assertIn("stop failed", "\n".join(captured.output))
        self.assertEqual(len(handlers_during_cleanup), 1)
        self.assertIsNot(handlers_during_cleanup[0], self.previous_handler)
        self.assertIs(signal.getsignal(signal.SIGINT), self.previous_handler)

    def test_first_ctrl_c_waits_for_execution_to_finish_before_cleanup(self):
        events = []

        def execute():
            events.append("executing")
            handler = signal.getsignal(signal.SIGINT)
            try:
                handler(signal.SIGINT, None)
                self.scan.stop.assert_called_once_with()
                self.scan.close.assert_not_called()
            finally:
                events.append("finished")

        self.scan.execute.side_effect = execute
        self.scan.close.side_effect = lambda: events.append("closed")
        with patch.object(cli.sys, "stderr"):
            self.assertEqual(cli.main(self.args), 0)

        self.assertEqual(events, ["executing", "finished", "closed"])
        self.scan.close.assert_called_once_with()
        self.assertIs(signal.getsignal(signal.SIGINT), self.previous_handler)

    def test_cleanup_runs_after_execution_error_unwinds(self):
        events = []

        def execute():
            try:
                raise RuntimeError("acquisition failed")
            finally:
                events.append("execution unwound")

        self.scan.execute.side_effect = execute
        self.scan.close.side_effect = lambda: events.append("closed")
        with self.assertLogs(cli.logger, level="ERROR"):
            self.assertEqual(cli.main(self.args), 1)

        self.assertEqual(events, ["execution unwound", "closed"])
        self.scan.close.assert_called_once_with()

    def test_factory_failure_does_not_attempt_cleanup(self):
        for initialization_error in (None, RuntimeError("initialization failed")):
            with self.subTest(initialization_error=initialization_error):
                self.factory.return_value = None
                self.factory.side_effect = initialization_error
                with self.assertLogs(cli.logger, level="ERROR"):
                    self.assertEqual(cli.main(self.args), 1)

                self.scan.execute.assert_not_called()
                self.scan.stop.assert_not_called()
                self.scan.close.assert_not_called()
                self.assertIs(signal.getsignal(signal.SIGINT), self.previous_handler)


if __name__ == "__main__":
    unittest.main(verbosity=2)
