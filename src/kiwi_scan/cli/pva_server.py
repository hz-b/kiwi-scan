# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional, Sequence

from kiwi_scan.ioc.pva_table import serve
from kiwi_scan.scan.tools import set_valid_logging_level

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
    # force=True,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the kiwi-scan PV Access service."
    )
    parser.add_argument(
        "--data-dir",
        help="Manifest directory; defaults to KIWI_SCAN_DATA_DIR.",
    )
    parser.add_argument(
        "--prefix",
        default="KIWI:DATA",
        help="PVA endpoint prefix. Default: KIWI:DATA",
    )
    parser.add_argument(
        "--log-level",
        type=int,
        choices=range(0, 6),
        metavar="0-5",
        help="MBBO record level (0..5) to set log verbosity via scanlib helper"
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if args.log_level is not None:
        logging.info("######################## set loglevel")
        set_valid_logging_level(args.log_level)

    try:
        serve(data_dir=args.data_dir, prefix=args.prefix)
    except KeyboardInterrupt:
        logging.info("PVA server stopped by user")
        return 0
    except (FileNotFoundError, ValueError) as exc:
        logging.error("Cannot start PVA server: %s", exc)
        return 2
    except Exception:
        logging.exception("PVA server failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
