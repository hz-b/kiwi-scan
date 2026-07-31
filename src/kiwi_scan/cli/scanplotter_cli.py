# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import argparse
import logging
from pathlib import Path
from typing import List

from kiwi_scan.data.manifestwriter import ManifestResolver
from kiwi_scan.postmortem import PostMortemConfig, SeriesSpec, plot_postmortem

_ALLOWED_SERIES_KEYS = {"file", "column", "axis", "label", "type"}
_REQUIRED_SERIES_KEYS = {"column"}
_AUTO_FILE_VALUES = {"", "auto", "manifest"}


def _series_usage() -> str:
    return "expected column=NAME[,file=PATH|auto,axis=N,label=TEXT,type=scan|meta]"


def parse_series_args(args: List[str]) -> List[SeriesSpec]:
    """
    Parse specs of form:
        column=NAME[,file=PATH|auto,axis=N,label=TEXT,type=scan|meta]

    If file is omitted or file=auto/manifest, scanplotter_cli resolves it
    from the selected manifest before calling the post-mortem plotter.

    Raises:
        ValueError: if a spec is malformed. The CLI catches this and turns it
        into an argparse-style error without a traceback.
    """
    result: List[SeriesSpec] = []
    for spec in args:
        kv = {}
        for part in spec.split(","):
            if "=" not in part:
                raise ValueError(f"Invalid --series {spec!r}: {part!r} is not KEY=VALUE; {_series_usage()}")
            key, value = part.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                raise ValueError(f"Invalid --series {spec!r}: empty key; {_series_usage()}")
            if key not in _ALLOWED_SERIES_KEYS:
                allowed = ", ".join(sorted(_ALLOWED_SERIES_KEYS))
                raise ValueError(f"Invalid --series {spec!r}: unknown key {key!r}. Allowed keys are: {allowed}")
            if key != "file" and not value:
                raise ValueError(f"Invalid --series {spec!r}: empty value for {key!r}")
            if key in kv:
                raise ValueError(f"Invalid --series {spec!r}: duplicate key {key!r}")
            kv[key] = value

        missing = sorted(_REQUIRED_SERIES_KEYS - kv.keys())
        if missing:
            raise ValueError(f"Invalid --series {spec!r}: missing required key(s): {', '.join(missing)}; {_series_usage()}")

        try:
            axis = int(kv.pop("axis", "0"))
        except ValueError as exc:
            raise ValueError(f"Invalid --series {spec!r}: axis must be an integer") from exc

        source_type = kv.pop("type", "scan").lower()
        if source_type not in {"scan", "meta"}:
            raise ValueError(f"Invalid --series {spec!r}: type must be 'scan' or 'meta'")

        file = kv.pop("file", "")
        column = kv.pop("column")
        label = kv.pop("label", None)
        result.append(SeriesSpec(file=file, column=column, axis=axis, label=label, source_type=source_type))
    return result


def _resolve_manifest_series(series: List[SeriesSpec], args: argparse.Namespace) -> None:
    unresolved = [s for s in series if s.file.strip().lower() in _AUTO_FILE_VALUES]
    if not unresolved:
        return

    if args.manifest_file:
        manifest_path = Path(args.manifest_file).expanduser()
        resolver = ManifestResolver.from_manifest_file(str(manifest_path))
        manifest_for_log = manifest_path
    else:
        resolver = ManifestResolver.from_env()
        manifest_for_log = resolver.select_manifest(args.manifest_index)

    for item in unresolved:
        resolved = resolver.select_file(
            source_type=item.source_type,
            manifest_file=str(manifest_for_log),
            manifest_index=args.manifest_index,
            scan_index=args.scan_index,
        )
        item.file = str(resolved)
        logging.info(
            "Resolved --series type=%s column=%s from manifest=%s scan-index=%s -> %s",
            item.source_type,
            item.column,
            manifest_for_log,
            args.scan_index,
            resolved,
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Post-mortem multi-axis plotting tool for scan + metadata files.\n\n"
            "Examples:\n"
            "  scanplotter_cli \\\n"
            "    --x_column TS-ISO8601 \\\n"
            "    --series file=scan_results-20251106.txt,column=ue521sgm1:monoGetEnergy,axis=0,label=eV\n\n"
            "  scanplotter_cli \\\n"
            "    --manifest-index 0 --scan-index 0 --x_column Position \\\n"
            "    --series column=ue521sgm1:liIDcics,type=scan,axis=0,label=cts\n\n"
            "    --series column=ue521sgm1:Status,type=meta,axis=1,label=State"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--x_column", required=True, help="Column to use as X axis (time or position)")
    parser.add_argument(
        "--series",
        action="append",
        required=True,
        help=(
            "Series spec: column=NAME[,file=PATH|auto,axis=N,label=TEXT,type=scan|meta].\n"
            "If file is omitted or file=auto, the file is resolved from the selected manifest.\n"
            "Repeat for multiple plotted series. Example: "
            "--series column=intensity,type=scan,axis=0,label=I0"
        ),
    )
    parser.add_argument(
        "--manifest-index",
        type=int,
        default=0,
        help=(
            "N-th newest manifest in KIWI_SCAN_DATA_DIR for auto-resolved series "
            "(0=newest, default: 0). Ignored when --manifest-file is used."
        ),
    )
    parser.add_argument(
        "--manifest-file",
        help="Explicit manifest YAML file for auto-resolved series; overrides --manifest-index.",
    )
    parser.add_argument(
        "--scan-index",
        type=int,
        default=0,
        help="M-th newest scan entry inside the selected manifest for auto-resolved series (0=newest, default: 0).",
    )
    parser.add_argument(
        "--join-tol",
        type=float,
        default=0.1,
        help="Time join tolerance in seconds (for scan/meta alignment)",
    )
    parser.add_argument(
        "--log-level",
        type=int,
        default=logging.INFO,
        help="Python logging level (10=DEBUG,20=INFO,...)",
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)

    try:
        series = parse_series_args(args.series)
        _resolve_manifest_series(series, args)
        cfg = PostMortemConfig(
            x_column=args.x_column,
            series=series,
            # join_on_time=None,   # TODO: using TS columns directly in build_combined_dataframe
            join_tolerance=args.join_tol,
        )
        print(f"PostMortemConfig {cfg}")
        plot_postmortem(cfg=cfg)
    except (ValueError, FileNotFoundError, IndexError, OSError) as exc:
        parser.error(str(exc))
    except Exception as exc:
        parser.error(f"Plotting failed: {exc}")


if __name__ == "__main__":
    main()
