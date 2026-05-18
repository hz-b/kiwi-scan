# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import shutil
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, List, Optional

from kiwi_scan.datamodels import ScanConfig, SubscriptionConfig
from kiwi_scan.scan.stats_collector import StatsCollector
from kiwi_scan.scan.subscription_manager import SubscriptionManager
from kiwi_scan.scan.tools import get_scan_config_dir, set_valid_logging_level
from kiwi_scan.yaml_loader import (
    get_env_replacements,
    parse_replacements,
    yaml_loader,
)


def _load_config(args: argparse.Namespace) -> ScanConfig:
    replacements = parse_replacements(args.replace or [])
    replacements.update(get_env_replacements("KIWI_SCAN"))

    if args.config_file:
        path = os.path.abspath(os.path.expanduser(args.config_file))
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
        return ScanConfig.from_dict(yaml_loader(path, replacements))

    config_dir = os.environ.get("KIWI_SCAN_CONFIG_DIR", get_scan_config_dir())
    path = os.path.join(config_dir, f"{args.config}.yaml")
    return ScanConfig.from_dict(yaml_loader(path, replacements))


def _subscriptions_for_role(
    config: ScanConfig,
    role: str,
) -> List[SubscriptionConfig]:
    return [
        sub
        for sub in (getattr(config, "subscriptions", None) or [])
        if getattr(sub, "role", None) == role
    ]


def _format_value(value):
    if value is None:
        return ""

    try:
        return f"{float(value):.6g}"
    except Exception:
        return str(value)

_LAST_LINE = ""
def _print_row(values, *, sep="\t"):
    global _LAST_LINE

    line = sep.join(_format_value(v) for v in values)

    # terminal width
    cols = shutil.get_terminal_size((120, 20)).columns

    # avoid terminal auto-wrap corruption
    if len(line) >= cols:
        line = line[: cols - 4] + " ..."

    # clear current line + carriage return
    print("\r\033[2K" + line, end="", flush=True)

    _LAST_LINE = line


def _print_row_multiline(headers, values):
    ts = values[0]
    print()
    print(ts)

    for h, v in zip(headers[1:], values[1:]):
        print(f"  {h:20s} { _format_value(v) }")

    sys.stdout.flush()

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="stats_collector_cli",
        description=(
            "Monitor configured kiwi-scan subscriptions and print live "
            "StatsCollector columns."
        ),
    )

    cfg_group = parser.add_mutually_exclusive_group(required=True)
    cfg_group.add_argument(
        "--config",
        help="Preset config name loaded from KIWI_SCAN_CONFIG_DIR",
    )
    cfg_group.add_argument(
        "--config-file",
        help="Path to a YAML config file",
    )

    parser.add_argument(
        "--role",
        default="sync",
        help="Subscription role to collect statistics from. Default: sync",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Print interval in seconds. Default: 1.0",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Optional total runtime in seconds. Default: run until Ctrl+C",
    )
    parser.add_argument(
        "--reset-each-print",
        action="store_true",
        help="Reset the stats window after each printed row.",
    )
    parser.add_argument(
        "--no-timestamp",
        action="store_true",
        help="Do not prepend TS-ISO8601 to each output row.",
    )
    parser.add_argument(
        "--replace",
        nargs="*",
        default=[],
        help="YAML replacements in KEY=VALUE form.",
    )
    parser.add_argument(
        "--log-level",
        type=int,
        choices=range(0, 6),
        metavar="0-5",
        help="MBBO record level mapped to Python logging.",
    )

    args = parser.parse_args(argv)

    if args.log_level is not None:
        set_valid_logging_level(args.log_level)

    if args.interval <= 0:
        parser.error("--interval must be > 0")

    try:
        config = _load_config(args)
        subscriptions = _subscriptions_for_role(config, args.role)
    except (FileNotFoundError, OSError, TypeError, ValueError) as exc:
        parser.error(str(exc))

    if not subscriptions:
        parser.error(f"No subscriptions with role={args.role!r} found in config")

    collector = StatsCollector(subscriptions, role=args.role)
    compact_mode = len(subscriptions) <= 2

    manager = SubscriptionManager(
        subscriptions,
        actuator_configs=getattr(config, "actuators", {}) or {},
        actuators=None,
    )

    def _on_stats_event(event, subscription=None) -> None:
        collector.update(event, subscription, collect=True)

    manager.register_role(args.role, _on_stats_event)

    sep = "\t"
    headers = collector.get_headers(False)
    if not args.no_timestamp:
        headers = ["TS-ISO8601"] + headers

    print(sep.join(headers), flush=True)

    t_start = time.monotonic()

    try:
        manager.start()
    except ConnectionError as exc:
        logging.error("Failed to start manager: %s", exc)
        return 1
    try:
        while True:
            if args.duration is not None:
                elapsed = time.monotonic() - t_start
                if elapsed >= args.duration:
                    break

            time.sleep(args.interval)

            values = collector.get_values()
            if not args.no_timestamp:
                values = [datetime.now(timezone.utc).isoformat()] + values

            if compact_mode:
                _print_row(values, sep=sep)
            else:
                _print_row_multiline(headers, values)
            if args.reset_each_print:
                collector.reset_window()
    except KeyboardInterrupt:
        return 130
    finally:
        try:
            manager.stop()
        except Exception:
            logging.exception("Failed to stop subscriptions")

    return 0


if __name__ == "__main__":
    sys.exit(main())
