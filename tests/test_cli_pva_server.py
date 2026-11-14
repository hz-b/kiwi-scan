# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Test PVA command dispatch and errors without opening a PVA server."""

import io
import logging
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from kiwi_scan.cli import pva_server as cli


class TestPvaServerCLI(unittest.TestCase):
    """Exercise CLI behavior with the server entry point mocked."""

    def setUp(self):
        self.root_logger = logging.getLogger()
        self.addCleanup(self.root_logger.setLevel, self.root_logger.level)

        server_patcher = patch.object(cli, "serve")
        self.addCleanup(server_patcher.stop)
        self.serve = server_patcher.start()

        logging_patcher = patch.object(cli, "set_valid_logging_level")
        self.addCleanup(logging_patcher.stop)
        self.set_logging_level = logging_patcher.start()

    def test_defaults_delegate_directory_resolution_to_server(self):
        self.assertEqual(cli.main([]), 0)
        self.serve.assert_called_once_with(data_dir=None, prefix="KIWI:DATA")
        self.set_logging_level.assert_not_called()

    def test_explicit_directory_prefix_and_log_level(self):
        self.assertEqual(cli.main([
            "--data-dir", "/scan/data", "--prefix", "TEST:DATA", "--log-level", "2",
        ]), 0)
        self.serve.assert_called_once_with(data_dir="/scan/data", prefix="TEST:DATA")
        self.set_logging_level.assert_called_once_with(2)

    def test_failure_exit_codes_and_diagnostics(self):
        cases = [
            (KeyboardInterrupt(), 0), (FileNotFoundError("missing manifest"), 2),
            (ValueError("invalid directory"), 2), (RuntimeError("server failed"), 1),
            (PermissionError("denied"), 1),
        ]
        for error, expected in cases:
            with self.subTest(error=type(error).__name__):
                self.serve.reset_mock()
                self.serve.side_effect = error
                with self.assertLogs(cli.__name__, level=logging.INFO) as captured:
                    self.assertEqual(cli.main([]), expected)
                self.serve.assert_called_once()
                messages = "\n".join(captured.output)
                if isinstance(error, KeyboardInterrupt):
                    self.assertIn("stopped by user", messages)
                elif expected == 2:
                    self.assertIn("Cannot start PVA server", messages)
                    self.assertIn(str(error), messages)
                else:
                    self.assertIn("PVA server failed", messages)
                    self.assertTrue(any(record.exc_info for record in captured.records))

    def test_invalid_options_never_start_server(self):
        cases = [
            ["--log-level", "-1"], ["--log-level", "6"],
            ["--log-level", "invalid"], ["--unknown"],
        ]
        for argv in cases:
            with self.subTest(argv=argv):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                    cli.main(argv)
                self.assertEqual(caught.exception.code, 2)
                self.serve.assert_not_called()

    def test_help_does_not_start_server(self):
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as caught:
            cli.main(["--help"])
        self.assertEqual(caught.exception.code, 0)
        self.assertIn("--prefix", output.getvalue())
        self.serve.assert_not_called()

    def test_zero_log_level_sets_notset(self):
        # Exercise the real helper: mocking it would hide a broken level mapping.
        from kiwi_scan.scan.tools import set_valid_logging_level

        self.root_logger.setLevel(logging.WARNING)
        with patch.object(cli, "set_valid_logging_level", set_valid_logging_level):
            self.assertEqual(cli.main(["--log-level", "0"]), 0)
        self.assertEqual(self.root_logger.level, logging.NOTSET)


if __name__ == "__main__":
    unittest.main(verbosity=2)
