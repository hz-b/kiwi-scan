# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import os
import sys
import argparse
import difflib
import signal
from typing import List, Dict

import kiwi_scan
from kiwi_scan.yaml_loader import (
    parse_replacements,
    get_replacements_help_and_required,
    get_env_replacements,
    yaml_loader,
)
from kiwi_scan.scan.registry import SCAN_REGISTRY, load_all_scan_types
from kiwi_scan.data.manifestwriter import ManifestWriter
from kiwi_scan.scan.tools import (
    load_scan_configs,
    create_scan_with_config,
    get_scan_config_dir,
    set_valid_logging_level,
)
from kiwi_scan.datamodels import ScanConfig, ScanDimension

import logging 

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s"
)


_DIM_REQUIRED_KEYS = {"actuator", "start", "stop", "steps"}
_DIM_OPTIONAL_KEYS = {"velocity"}
_DIM_ALLOWED_KEYS = _DIM_REQUIRED_KEYS | _DIM_OPTIONAL_KEYS


def _validate_dim_args(dim_args: List[str]) -> None:
    """
    Validate --dim syntax/keys before constructing ScanDimension objects.
    ScanDimension.from_dim_args raises low-level exceptions such as
    KeyError for misspelled keys. 
    """
    for spec in dim_args:
        if not spec or not spec.strip():
            raise ValueError("--dim must not be empty")

        kv: Dict[str, str] = {}
        for part in spec.split(","):
            part = part.strip()
            if not part:
                raise ValueError(f"Invalid --dim {spec!r}: empty comma-separated field")
            if "=" not in part:
                raise ValueError(
                    f"Invalid --dim {spec!r}: field {part!r} is not KEY=VALUE"
                )
            key, value = part.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                raise ValueError(f"Invalid --dim {spec!r}: empty key")
            if not value:
                raise ValueError(f"Invalid --dim {spec!r}: empty value for {key!r}")
            if key in kv:
                raise ValueError(f"Invalid --dim {spec!r}: duplicate key {key!r}")
            kv[key] = value

        unknown = sorted(set(kv) - _DIM_ALLOWED_KEYS)
        if unknown:
            details = []
            for key in unknown:
                suggestion = difflib.get_close_matches(key, _DIM_ALLOWED_KEYS, n=1)
                if suggestion:
                    details.append(f"{key!r} (did you mean {suggestion[0]!r}?)")
                else:
                    details.append(repr(key))
            raise ValueError(
                f"Invalid --dim {spec!r}: unknown key(s): {', '.join(details)}. "
                f"Allowed keys are: {', '.join(sorted(_DIM_ALLOWED_KEYS))}"
            )

        missing = sorted(_DIM_REQUIRED_KEYS - set(kv))
        if missing:
            raise ValueError(
                f"Invalid --dim {spec!r}: missing required key(s): {', '.join(missing)}"
            )

        for key in ("start", "stop"):
            try:
                float(kv[key])
            except ValueError as exc:
                raise ValueError(
                    f"Invalid --dim {spec!r}: {key} must be a number, got {kv[key]!r}"
                ) from exc

        try:
            int(kv["steps"])
        except ValueError as exc:
            raise ValueError(
                f"Invalid --dim {spec!r}: steps must be an integer, got {kv['steps']!r}"
            ) from exc

        if "velocity" in kv:
            try:
                float(kv["velocity"])
            except ValueError as exc:
                raise ValueError(
                    f"Invalid --dim {spec!r}: velocity must be a number, got {kv['velocity']!r}"
                ) from exc


def _validate_scan_dimensions(scan_dimensions: List[ScanDimension]) -> None:
    """Validate parsed scan dimensions before executing a scan."""
    for dim in scan_dimensions:
        if dim.steps < 0:
            raise ValueError(
                f"Invalid --dim for actuator {dim.actuator!r}: steps must be >= 0, got {dim.steps}"
            )

        velocity = getattr(dim, "velocity", None)
        if velocity is not None and velocity < 0:
            raise ValueError(
                f"Invalid --dim for actuator {dim.actuator!r}: velocity must be >= 0, got {velocity}"
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

    # manifest first (optional single argument)
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--newmanifest", nargs="?", const=True, default=None)

    pre_args, remaining_argv = pre_parser.parse_known_args()

    if pre_args.newmanifest is not None:
        if pre_args.newmanifest is True:
            path = ManifestWriter.newmanifest(directory=data_dir)
        else:
            path = ManifestWriter.newmanifest(pre_args.newmanifest)

        logging.info("New manifest: %s", path)

        # If ONLY --newmanifest → exit immediately
        if len(sys.argv) <= 2:
            return

    # IMPORTANT: load scan types before argparse builds choices
    load_all_scan_types()

    scan_configs_index = _safe_load_config_index(config_dir)

    parser = argparse.ArgumentParser(
        description="Perform different types of scans on EPICS PVs.\n"
                    "Examples:\n"
                    "scan_runner --scan_type linear --config mono \\\n"
                    "--dim actuator=energy,start=100,stop=200,steps=5 \\\n"
                    "--dim actuator=gap,start=1.0,stop=2.0,steps=3\n\n"
                    "scan_runner --scan_type linear --config-file /path/to/mono.yaml \\\n"
                    "--dim actuator=energy,start=100,stop=200,steps=5 \n\n"
                    "scan_runner --newmanifest",
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
    try:
        _validate_dim_args(args.dim)
        scan_dimensions = ScanDimension.from_dim_args(args.dim)
    except (KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))

    try:
        replacements = parse_replacements(args.replace)
    except (TypeError, ValueError) as exc:
        parser.error(f"invalid --replace: {exc}")
    replacements.update(get_env_replacements("KIWI_SCAN"))

    actuators = ScanDimension.get_actuators(scan_dimensions)

    # Resolve config (preset name OR explicit file)
    if args.config_file:
        config_path = os.path.abspath(os.path.expanduser(args.config_file))
        if not os.path.isfile(config_path):
            parser.error(f"--config-file not found: {config_path}")

        # Show required replacements (if any) for this specific file
        replacements_help, replace_required = get_replacements_help_and_required(
            os.path.dirname(config_path),
            [os.path.basename(config_path)],
        )
        if replace_required:
            print(replacements_help)

        try:
            config = _load_config_from_path(config_path, replacements)
        except (FileNotFoundError, OSError, ValueError) as exc:
            parser.error(f"failed to load --config-file {config_path!r}: {exc}")
        config_label = config_path
    else:
        # Preset: load from config_dir
        try:
            scan_configs = load_scan_configs(config_dir, replacements)
            config = scan_configs[args.config]
        except (FileNotFoundError, OSError, KeyError, ValueError) as exc:
            parser.error(f"failed to load --config {args.config!r}: {exc}")

        replacements_help, replace_required = get_replacements_help_and_required(
            config_dir,
            [args.config + ".yaml"],
        )
        if replace_required:
            print(replacements_help)

        config_label = args.config

    try:
        _validate_scan_dimensions(scan_dimensions)
    except ValueError as exc:
        parser.error(str(exc))

    # Debug output
    logging.info(f"Scan Type: {args.scan_type}")
    if args.config_file:
        logging.info(f"Config File: {config_label}")
    else:
        logging.info(f"Config: {config_label}")
    logging.info(f"Replacements: {replacements}")

    logging.info(f"Actuators: {actuators}")
    logging.info(f"Scan Dimensions:")
    for dim in scan_dimensions:
        logging.info(
            f"  Actuator: {dim.actuator}, "
            f"Start: {dim.start}, "
            f"Stop: {dim.stop}, "
            f"Steps: {dim.steps}, "
            f"Velocity: {dim.velocity}"
        )

    # Override YAML scan_dimensions with CLI scan dimensions (existing behavior)
    config.scan_dimensions = scan_dimensions

    try:
        kiwi_scan.load_all_plugins()
        scan = create_scan_with_config(
            scantype=args.scan_type,
            config=config,
            data_dir=data_dir,
        )
    except Exception as exc:
        logging.error("Failed to initialize scan: %s", exc)
        logging.debug("Scan initialization traceback", exc_info=True)
        return 1

    if scan is None:
        logging.error("Failed to initialize scan: scan factory returned None")
        return 1

    sigint_count = 0

    def _sigint(_signum, _frame):
        nonlocal sigint_count
        sigint_count += 1

        if sigint_count >= 2:
            print(
                "Second Ctrl-C received: interrupting scan_runner.",
                file=sys.stderr,
                flush=True,
            )
            os._exit(130)

        print(
            "Ctrl-C received: stopping scan actuators. "
            "Press Ctrl-C again to interrupt.",
            file=sys.stderr,
            flush=True,
        )
        try:
            scan.stop()
        except Exception:
            logging.exception("Failed to stop scan during Ctrl-C handling")

    previous_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _sigint)
    try:
        scan.execute()
    except Exception as exc:
        logging.error("Scan failed: %s", exc)
        logging.debug("Scan failure traceback", exc_info=True)
        try:
            scan.stop()
        except Exception:
            logging.exception("Failed to stop scan after error")
        return 1
    finally:
        signal.signal(signal.SIGINT, previous_sigint_handler)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
