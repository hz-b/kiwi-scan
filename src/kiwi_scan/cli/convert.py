# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

""" kiwi-scan data conversion CLI. """

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Optional

from kiwi_scan.export import (
    available_writers,
    get_writer,
    load_export_bundle_from_latest_manifest,
    load_export_bundle_from_manifest,
    load_export_bundle_from_scan_file,
)
from kiwi_scan.scan.tools import set_valid_logging_level

logger = logging.getLogger(__name__)

def _build_parser(prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Convert kiwi-scan data to external formats.\n\n"
            "Examples:\n"
            "  kiwi-convert --manifest-file manifest.yaml --format spec --out export.spec\n"
            "  kiwi-convert --data-file scan_results.txt --format spec --out export.spec\n"
            "  kiwi-convert --latest-manifest --format spec --out export.spec"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--manifest-file", help="Explicit kiwi-scan manifest YAML file to convert")
    source.add_argument("--data-file", help="Single kiwi-scan scan data file to convert")
    parser.add_argument("--metadata-file", help="Optional metadata sidecar for --data-file")
    parser.add_argument("--scan-id", help="Optional scan id for --data-file input")
    parser.add_argument("--scan-type", help="Optional scan type for --data-file input")
    source.add_argument(
        "--latest-manifest",
        action="store_true",
        help="Convert a manifest selected from KIWI_SCAN_DATA_DIR or --data-dir",
    )
    parser.add_argument("--data-dir", help="Directory used when selecting --latest-manifest")
    parser.add_argument(
        "--manifest-index",
        type=int,
        default=0,
        help="N-th newest manifest for --latest-manifest (0=newest, default: 0)",
    )
    parser.add_argument(
        "--format",
        required=True,
        choices=available_writers(),
        help="Output format",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output file",
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Do not parse metadata sidecar files",
    )
    parser.add_argument(
        "--skip-missing-data",
        action="store_true",
        help="For manifest input, skip scan entries whose data file is missing",
    )
    parser.add_argument(
        "--include-non-numeric",
        action="store_true",
        help="SPEC only: include non-numeric columns instead of dropping them",
    )
    parser.add_argument(
        "--include-metadata-monitor",
        action="store_true",
        help=(
            "SPEC only: include metadata monitor rows as #C comments. "
            "Disabled by default because monitor files can be very large."
        ),
    )
    parser.add_argument(
        "--log-level",
        type=int,
        choices=range(6),
        metavar="0-5",
        help="MBBO record level (0..5) to set log verbosity via scanlib helper"
    )
    return parser

def _load_bundle(args: argparse.Namespace):
    include_metadata = not args.no_metadata
    if args.manifest_file:
        return load_export_bundle_from_manifest(
            args.manifest_file,
            include_metadata=include_metadata,
            skip_missing_data=args.skip_missing_data,
        )
    if args.data_file:
        return load_export_bundle_from_scan_file(
            args.data_file,
            metadata_file=args.metadata_file,
            scan_id=args.scan_id,
            scan_type=args.scan_type,
            include_metadata=include_metadata,
        )
    return load_export_bundle_from_latest_manifest(
        data_dir=args.data_dir,
        manifest_index=args.manifest_index,
        include_metadata=include_metadata,
        skip_missing_data=args.skip_missing_data,
    )


def _writer_kwargs(args: argparse.Namespace) -> dict:
    if args.format == "spec":
        return {
            "include_non_numeric": args.include_non_numeric,
            "include_metadata_monitor": args.include_metadata_monitor,
        }
    return {}


def _error_text(exc: BaseException) -> str:
    """ single-line error description """
    message = str(exc).replace("\n", " ").replace("\r", " ").strip()
    return message or type(exc).__name__

def _exit_on_error(
    parser: argparse.ArgumentParser,
    prog: str,
    exc: BaseException,
    *,
    status: int,
    unexpected: bool = False,
) -> None:
    """ Exit gracefully with debug output. """
    if unexpected:
        logger.debug("Unexpected conversion failure")
        description = f"{type(exc).__name__}: {_error_text(exc)}"
    else:
        description = _error_text(exc)

    parser.exit(status, f"{prog}: error: {description}\n")

def main(argv: Optional[List[str]] = None, *, prog: str = "kiwi_convert") -> int:
    parser = _build_parser(prog)
    args = parser.parse_args(argv)

    try:
        if args.log_level is not None:
            set_valid_logging_level(args.log_level)

        bundle = _load_bundle(args)
        writer = get_writer(args.format, **_writer_kwargs(args))
        out_path = writer.write(bundle, Path(args.out))
    except (FileNotFoundError, ValueError, OSError) as exc:
        _exit_on_error(parser, prog, exc, status=2)
    except Exception as exc:  # noqa BLE001
        _exit_on_error(parser, prog, exc, status=1, unexpected=True)


    logger.debug(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
