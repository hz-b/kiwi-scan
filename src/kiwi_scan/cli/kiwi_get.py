# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Command-line adapter for the reusable detector printing function."""

import argparse
from typing import List, Optional

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.tools.detectors import print_detector_values
from kiwi_scan.yaml_loader import get_env_replacements, parse_replacements, yaml_loader


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="kiwi-get", description="caget configured detector PVs once")
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--replace", nargs="*", default=[], help="KEY=VALUE")
    parser.add_argument("--format", choices=("tsv", "csv", "json"), default="tsv")
    parser.add_argument("--timestamps", action="store_true")
    parser.add_argument("--timeout", type=float, help="Read timeout per PV; default: YAML value or 1 second")
    parser.add_argument("--connection-timeout", type=float, default=1.0, help="Connection timeout per PV")
    args = parser.parse_args(argv)

    try:
        replacements = parse_replacements(args.replace)
        replacements.update(get_env_replacements("KIWI_SCAN"))
        data = yaml_loader(args.config_file, replacements)
        if not isinstance(data, dict):
            raise TypeError("Configuration must be a YAML mapping")
        if not isinstance(data.get("actuators", {}), dict):
            raise TypeError("actuators must be a mapping")
        config = ScanConfig.from_dict(data)
        return print_detector_values(
            config, read_timeout=args.timeout, connection_timeout=args.connection_timeout,
            output_format=args.format, include_timestamps=args.timestamps,
        )
    except (OSError, TypeError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
