# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import logging
import os
from typing import Any, Dict, Tuple

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.scan.tools import (
    get_scan_config_dir,
    load_scan_configs,
    set_valid_logging_level,
)
from kiwi_scan.scan.trigger_manager import TriggerManager
from kiwi_scan.yaml_loader import (
    get_env_replacements,
    get_replacements_help_and_required,
    parse_replacements,
    yaml_loader,
)


logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
)


def _safe_load_config_index(config_dir: str) -> Dict[str, Any]:
    """Load config-name index for argparse choices without blocking --config-file."""
    try:
        return load_scan_configs(config_dir, None)
    except FileNotFoundError:
        return {}


def _load_raw_config(args: argparse.Namespace, config_dir: str) -> Tuple[Dict[str, Any], str]:
    """Load one YAML config with CLI and environment replacements applied."""
    replacements = parse_replacements(args.replace or [])
    replacements.update(get_env_replacements("KIWI_SCAN"))

    if args.config_file:
        config_path = os.path.abspath(os.path.expanduser(args.config_file))
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"--config-file not found: {config_path}")
        return yaml_loader(config_path, replacements), config_path

    config_path = os.path.join(config_dir, f"{args.config}.yaml")
    return yaml_loader(config_path, replacements), config_path


def print_required_replacements(args: argparse.Namespace, config_dir: str, origin: str) -> None:
    """Print replacement help in the same spirit as scan_runner."""
    if args.config_file:
        cfg_dir = os.path.dirname(origin)
        cfg_files = [os.path.basename(origin)]
    else:
        cfg_dir = config_dir
        cfg_files = [f"{args.config}.yaml"]

    help_text, required = get_replacements_help_and_required(cfg_dir, cfg_files)
    if required:
        print(help_text)


def main() -> None:
    config_dir = os.environ.get("KIWI_SCAN_CONFIG_DIR", get_scan_config_dir())
    scan_configs_index = _safe_load_config_index(config_dir)

    parser = argparse.ArgumentParser(
        prog="scan_trigger_cli",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Execute one configured kiwi-scan trigger phase without running a scan.\n\n"
            "Examples:\n"
            "  scan_trigger_cli --config-file ./mono.yaml --phase after\n"
            "  scan_trigger_cli --config mono --replace IOC_MONO=ue521sgm1:monoGetEnergy --phase on_point\n"
        ),
    )

    cfg_group = parser.add_mutually_exclusive_group(required=True)
    cfg_group.add_argument(
        "--config",
        choices=scan_configs_index.keys(),
        help="Preset configuration name (loaded from KIWI_SCAN_CONFIG_DIR)",
    )
    cfg_group.add_argument(
        "--config-file",
        help="Path to a YAML config file",
    )

    parser.add_argument(
        "--phase",
        required=True,
        help="Trigger phase: before, on_point, after, monitor, or a custom phase",
    )
    parser.add_argument(
        "--replace",
        nargs="*",
        default=[],
        help="List of replacements: KEY=VALUE",
    )
    parser.add_argument(
        "--log-level",
        type=int,
        choices=range(0, 6),
        metavar="0-5",
        help="MBBO record level (0..5) mapped to Python logging",
    )

    args = parser.parse_args()

    if args.log_level is not None:
        set_valid_logging_level(args.log_level)

    try:
        raw_config, origin = _load_raw_config(args, config_dir)
        print_required_replacements(args, config_dir, origin)
        config = ScanConfig.from_dict(raw_config)
        trigger_manager = TriggerManager.from_config(config.triggers)
        trigger_manager.fire(args.phase)
    except (FileNotFoundError, KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))

    logging.info("Executed trigger phase %r from %s", args.phase, origin)

if __name__ == "__main__":
    main()
