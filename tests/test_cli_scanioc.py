# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Exercise IOC CLI parsing and dispatch without starting a soft IOC."""

import logging
from unittest.mock import Mock

import pytest

from kiwi_scan.cli import scanioc as cli
from kiwi_scan.ioc.datamodels import DataPVSpec
from kiwi_scan.scan.tools import set_valid_logging_level


@pytest.fixture(autouse=True)
def isolate_startup(monkeypatch):
    root = logging.getLogger()
    previous_level = root.level
    for key in tuple(cli.os.environ):
        if key.startswith("KIWI_SCAN_"):
            monkeypatch.delenv(key)
    monkeypatch.setattr(cli, "load_all_scan_types", Mock())
    monkeypatch.setattr(cli.kiwi_scan, "load_all_plugins", Mock())
    monkeypatch.setattr(cli, "get_available_scan_types", lambda: ["linear", "cm"])
    monkeypatch.setattr(cli, "default_config_dir", lambda: "/test/configs")
    monkeypatch.setattr(cli, "load_scan_configs", lambda *_: {"mono": Mock()})
    monkeypatch.setattr(cli, "set_valid_logging_level", Mock())
    monkeypatch.setattr(cli, "run_ioc", Mock())
    monkeypatch.setattr(cli.logging, "basicConfig", Mock())
    yield
    root.setLevel(previous_level)


def test_default_options_reach_ioc():
    assert cli.main(["--config", "mono"]) == 0
    options = cli.run_ioc.call_args.args[0]
    assert options.prefix == "KIWI:SCAN"
    assert options.scan_type == "linear"
    assert options.config_name == "mono"
    assert options.config_file is None
    assert options.config_dir == "/test/configs"
    assert options.data_dir is None
    assert options.publish_period == 1.0
    assert options.data_pvs == []
    cli.set_valid_logging_level.assert_called_once_with(3)


def test_explicit_options_override_environment(monkeypatch):
    monkeypatch.setenv("KIWI_SCAN_IOC_PREFIX", "ENV:SCAN")
    monkeypatch.setenv("KIWI_SCAN_IOC_SCAN_TYPE", "linear")
    monkeypatch.setenv("KIWI_SCAN_DATA_DIR", "/env/data")
    assert cli.main([
        "--config-file", "mono.yaml", "--prefix", "TEST:SCAN",
        "--scan_type", "cm", "--config-dir", "/explicit/configs",
        "--data-dir", "/explicit/data", "--publish-period", "0.25",
        "--data-pv", "Energy=MONO:RBV:float", "--data-pv", "State=State:string",
        "--log-level", "2",
    ]) == 0
    options = cli.run_ioc.call_args.args[0]
    assert options.prefix == "TEST:SCAN"
    assert options.scan_type == "cm"
    assert options.config_file == "mono.yaml"
    assert options.config_name is None
    assert options.config_dir == "/explicit/configs"
    assert options.data_dir == "/explicit/data"
    assert options.publish_period == 0.25
    assert options.data_pvs == [
        DataPVSpec("Energy", "MONO:RBV", "float"),
        DataPVSpec("State", "State", "str"),
    ]
    cli.set_valid_logging_level.assert_called_once_with(2)


def test_environment_defaults(monkeypatch):
    monkeypatch.setenv("KIWI_SCAN_IOC_PREFIX", "ENV:SCAN")
    monkeypatch.setenv("KIWI_SCAN_IOC_SCAN_TYPE", "cm")
    monkeypatch.setenv("KIWI_SCAN_DATA_DIR", "/env/data")
    cli.main(["--config", "mono"])
    options = cli.run_ioc.call_args.args[0]
    assert (options.prefix, options.scan_type, options.data_dir) == (
        "ENV:SCAN", "cm", "/env/data",
    )


def test_replacement_values_preserve_equals_and_merge_environment(monkeypatch):
    # Preserve the existing environment-wins convention used by this CLI.
    monkeypatch.setenv("KIWI_SCAN_REPLACE_IOC", "ENV:MONO")
    monkeypatch.setenv("KIWI_SCAN_REPLACE_SITE", "BESSY")
    cli.main([
        "--config", "mono", "--replace", "IOC=CLI:MONO",
        "--replace", "EXPRESSION=a=b",
    ])
    assert cli.run_ioc.call_args.args[0].replacements == {
        "IOC": "ENV:MONO", "SITE": "BESSY", "EXPRESSION": "a=b",
    }


@pytest.mark.parametrize("argv", [
    [], ["--config", "mono", "--config-file", "mono.yaml"],
    ["--config", "mono", "--scan-type", "unknown"],
    ["--config", "mono", "--log-level", "6"],
    ["--config", "mono", "--publish-period", "invalid"],
    ["--config", "mono", "--data-pv", "invalid"],
    ["--config", "mono", "--data-pv", "=RBV"],
])
def test_invalid_arguments_never_start_ioc(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2
    assert "error:" in capsys.readouterr().err
    cli.run_ioc.assert_not_called()


@pytest.mark.parametrize("error", [
    FileNotFoundError("missing.yaml"), KeyError("missing actuator"),
    RuntimeError("IOC startup failed"), TypeError("not a mapping"),
    ValueError("invalid config"),
])
def test_startup_errors_become_cli_errors(error, capsys):
    cli.run_ioc.side_effect = error
    with pytest.raises(SystemExit) as exc:
        cli.main(["--config", "mono"])
    assert exc.value.code == 2
    stderr = capsys.readouterr().err
    assert str(error) in stderr
    assert "Traceback" not in stderr


def test_zero_log_level_selects_notset(monkeypatch):
    monkeypatch.setattr(cli, "set_valid_logging_level", set_valid_logging_level)
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    assert cli.main(["--config", "mono", "--log-level", "0"]) == 0
    assert root.level == logging.NOTSET


def test_unreadable_preset_directory_does_not_break_explicit_file(monkeypatch):
    monkeypatch.setattr(cli, "load_scan_configs", Mock(side_effect=OSError("unreadable")))
    assert cli.main(["--config-file", "mono.yaml"]) == 0
    cli.run_ioc.assert_called_once()


def test_help_lists_sorted_presets_and_does_not_start_ioc(monkeypatch, capsys):
    monkeypatch.setattr(cli, "load_scan_configs", lambda *_: {"zeta": Mock(), "alpha": Mock()})
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    assert "alpha, zeta" in capsys.readouterr().out
    cli.run_ioc.assert_not_called()


def test_safe_config_names_handles_no_directory():
    assert cli._safe_config_names(None) == []
