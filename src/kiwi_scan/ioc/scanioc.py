# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Command-line entry point for the generic kiwi-scan IOC."""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any, List, Optional, Sequence

from kiwi_scan.yaml_loader import get_env_replacements, parse_replacements

from .controller import default_config_dir
from .datamodels import parse_data_pv_specs
from .factory import GenericScanIOCOptions, run_ioc
from kiwi_scan.scan.tools import load_scan_configs
from kiwi_scan.scan.registry import get_available_scan_types, load_all_scan_types
from kiwi_scan.scan.tools import set_valid_logging_level
import kiwi_scan

logger = logging.getLogger(__name__)


def _safe_config_names(config_dir: Optional[str]) -> List[str]:
    if not config_dir:
        return []
    try:
        return sorted(load_scan_configs(config_dir, None).keys())
    except Exception:
        logger.debug("Could not load config names for CLI help", exc_info=True)
        return []


def build_arg_parser() -> argparse.ArgumentParser:
    load_all_scan_types()
    kiwi_scan.load_all_plugins()

    default_dir = default_config_dir()
    available_scan_types = get_available_scan_types()

    parser = argparse.ArgumentParser(
        prog="scanioc",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Generic kiwi-scan soft IOC.\n\n"
            "Examples:\n"
            "  scanioc --prefix KIWI:SCAN --config mono \\\n"
            "    --scan-type linear --data-pv Energy=ue521sgm1:monoGetEnergy:float\n\n"
            "  scanioc --prefix TEST:SCAN --config-file ./mono.yaml \\\n"
            "    --data-pv Position=Position:float --data-pv Time=TS-ISO8601:string\n"
        ),
    )

    parser.add_argument("--prefix", default=os.environ.get("KIWI_SCAN_IOC_PREFIX", "KIWI:SCAN"))
    scan_type_kwargs: dict[str, Any] = {"default": os.environ.get("KIWI_SCAN_IOC_SCAN_TYPE", "linear")}
    if available_scan_types:
        scan_type_kwargs["choices"] = available_scan_types
    parser.add_argument("--scan-type", "--scan_type", dest="scan_type", **scan_type_kwargs)

    cfg_group = parser.add_mutually_exclusive_group(required=True)
    cfg_names = _safe_config_names(default_dir)
    cfg_help = "Preset config name from KIWI_SCAN_CONFIG_DIR"
    if cfg_names:
        cfg_help += " (available: %s)" % ", ".join(cfg_names)
    cfg_group.add_argument("--config", dest="config_name", help=cfg_help)
    cfg_group.add_argument("--config-file", help="Explicit YAML config file")

    parser.add_argument("--config-dir", default=default_dir)
    parser.add_argument("--data-dir", default=os.environ.get("KIWI_SCAN_DATA_DIR"))
    parser.add_argument(
        "--replace",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Macro replacement for YAML templates. Repeatable.",
    )
    parser.add_argument(
        "--data-pv",
        action="append",
        default=[],
        metavar="LOCAL=KEY[:TYPE]",
        help=(
            "Publish scan.get_value(KEY) as PREFIX:DATA:LOCAL. Repeatable. "
            "TYPE is float, int, str, or bool. Colons inside KEY are allowed."
        ),
    )
    parser.add_argument("--publish-period", type=float, default=1.0, help="State/data publish period in seconds")
    parser.add_argument(
        "--log-level",
        type=int,
        choices=range(0, 6),
        metavar="0-5",
        help="MBBO-style logging level, mapped through kiwi-scan set_valid_logging_level",
    )
    return parser


def options_from_args(args: argparse.Namespace) -> GenericScanIOCOptions:
    replacements = parse_replacements(args.replace or [])
    replacements.update(get_env_replacements("KIWI_SCAN"))
    data_pvs = parse_data_pv_specs(args.data_pv)

    logger.info(
        "Parsed GenericScanIOC CLI options prefix=%s scan_type=%s config_name=%s config_file=%s data_pvs=%d replacements=%s",
        args.prefix,
        args.scan_type,
        args.config_name,
        args.config_file,
        len(data_pvs),
        sorted(replacements.keys()),
    )
    return GenericScanIOCOptions(
        prefix=args.prefix,
        scan_type=args.scan_type,
        config_name=args.config_name,
        config_file=args.config_file,
        config_dir=args.config_dir,
        data_dir=args.data_dir,
        replacements=replacements,
        data_pvs=data_pvs,
        publish_period=args.publish_period,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
    )
    set_valid_logging_level(args.log_level)

    try:
        options = options_from_args(args)
        run_ioc(options)
    except (FileNotFoundError, KeyError, RuntimeError, TypeError, ValueError) as exc:
        parser.error(str(exc))
    return 0
