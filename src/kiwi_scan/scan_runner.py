# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import os
import argparse

import kiwi_scan
from kiwi_scan.yaml_loader import (
    parse_replacements,
    get_replacements_help_and_required,
    get_env_replacements,
    yaml_loader,
)
from kiwi_scan.scan.registry import SCAN_REGISTRY, load_all_scan_types
from kiwi_scan.scan.tools import (
    load_scan_configs,
    scan_with_config,
    get_scan_config_dir,
    set_valid_logging_level,
)
from kiwi_scan.datamodels import ScanConfig, ScanDimension

import logging 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s"
)

def _load_config_from_path(config_path: str, replacements: dict) -> ScanConfig:
    """Load a single YAML scan config from an explicit path."""
    data = yaml_loader(config_path, replacements)
    return ScanConfig.from_dict(data)


def _safe_load_config_index(config_dir: str) -> dict:
    """Load config-name index for argparse choices.

    This should never block using --config-file if config_dir is missing.
    """
    try:
        return load_scan_configs(config_dir, None)
    except FileNotFoundError:
        return {}

def main():
    config_dir = os.environ.get("KIWI_SCAN_CONFIG_DIR", get_scan_config_dir())
    data_dir = os.environ.get("KIWI_SCAN_DATA_DIR")

    # IMPORTANT: load scan types before argparse builds choices
    load_all_scan_types()

    scan_configs_index = _safe_load_config_index(config_dir)

    parser = argparse.ArgumentParser(
        description="Perform different types of scans on EPICS PVs.\n"
                    "Examples:\n"
                    "./scan_runner.py --scan_type linear --config mono \\\n"
                    "--dim actuator=energy,start=100,stop=200,steps=5 \\\n"
                    "--dim actuator=gap,start=1.0,stop=2.0,steps=3\n\n"
                    "./scan_runner.py --scan_type linear --config-file /path/to/mono.yaml \\\n"
                    "--dim actuator=energy,start=100,stop=200,steps=5",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--scan_type",
        choices=sorted(SCAN_REGISTRY.keys()),
        default="linear",
        help="Type of scan to perform",
    )

    cfg_group = parser.add_mutually_exclusive_group(required=True)
    cfg_group.add_argument(
        "--config",
        choices=scan_configs_index.keys(),
        help="Preset configuration name (loaded from config_dir)",
    )
    cfg_group.add_argument(
        "--config-file",
        help="Path to a YAML config file (bypasses config_dir search)",
    )

    parser.add_argument(
        "--dim",
        action="append",
        required=True,
        help="Scan dimension in the form: actuator=NAME,start=VAL,stop=VAL,steps=N,velocity=VAL. "
             "Repeat for each parallel/nested dimension."
    )
    parser.add_argument(
        "--replace",
        nargs="*",
        help="List of replacements in the form KEY=NEW_FIELD_VALUE",
        default=[]
    )
    parser.add_argument(
        "--log-level",
        type=int,
        choices=range(0, 6),
        metavar="0-5",
        help="MBBO record level (0..5) to set log verbosity via scanlib helper"
    )

    args = parser.parse_args()

    if args.log_level is not None:
        set_valid_logging_level(args.log_level)

    # Parse structured input
    scan_dimensions = ScanDimension.from_dim_args(args.dim)
    replacements = parse_replacements(args.replace)
    replacements.update(get_env_replacements("KIWI_SCAN"))

    actuators = ScanDimension.get_actuators(scan_dimensions)

    # Resolve config (preset name OR explicit file)
    if args.config_file:
        config_path = os.path.abspath(os.path.expanduser(args.config_file))
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"--config-file not found: {config_path}")

        # Show required replacements (if any) for this specific file
        replacements_help, replace_required = get_replacements_help_and_required(
            os.path.dirname(config_path),
            [os.path.basename(config_path)],
        )
        if replace_required:
            print(replacements_help)

        config = _load_config_from_path(config_path, replacements)
        config_label = config_path
    else:
        # Preset: load from config_dir
        scan_configs = load_scan_configs(config_dir, replacements)
        config = scan_configs[args.config]

        replacements_help, replace_required = get_replacements_help_and_required(
            config_dir,
            [args.config + ".yaml"],
        )
        if replace_required:
            print(replacements_help)

        config_label = args.config

    # Debug output
    print("Scan Type:", args.scan_type)
    if args.config_file:
        print("Config File:", config_label)
    else:
        print("Config:", config_label)
    print("Replacements:", replacements)

    print("Actuators:", actuators)
    print("Scan Dimensions:")
    for dim in scan_dimensions:
        print(
            f"  Actuator: {dim.actuator}, "
            f"Start: {dim.start}, "
            f"Stop: {dim.stop}, "
            f"Steps: {dim.steps}, "
            f"Velocity: {dim.velocity}"
        )

    # Override YAML scan_dimensions with CLI scan dimensions (existing behavior)
    config.scan_dimensions = scan_dimensions
    kiwi_scan.load_all_plugins()
    # Execute the scan
    scan_with_config(
        scantype=args.scan_type,
        config=config,
        data_dir=data_dir
    )


if __name__ == "__main__":
    main()
