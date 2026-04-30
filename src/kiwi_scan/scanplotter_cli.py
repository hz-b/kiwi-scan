# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import argparse
import logging
from typing import List

from kiwi_scan.postmortem import PostMortemConfig, SeriesSpec, plot_postmortem

_ALLOWED_SERIES_KEYS = {"file", "column", "axis", "label", "type"}
_REQUIRED_SERIES_KEYS = {"file", "column"}


def _series_usage() -> str:
    return "expected file=PATH,column=NAME[,axis=N,label=TEXT,type=scan|meta]"


def parse_series_args(args: List[str]) -> List[SeriesSpec]:
    """
    Parse specs of form:
        file=PATH,column=NAME,axis=N,label=TEXT,type=scan|meta

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
            if not value:
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

        source_type = kv.pop("type", "scan")
        if source_type not in {"scan", "meta"}:
            raise ValueError(f"Invalid --series {spec!r}: type must be 'scan' or 'meta'")

        file = kv.pop("file")
        column = kv.pop("column")
        label = kv.pop("label", None)
        result.append(SeriesSpec(file=file, column=column, axis=axis, label=label, source_type=source_type))
    return result


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Post-mortem multi-axis plotting tool for scan + metadata files.\n\n"
            "Examples:\n"
            "  scanplotter_cli \\\n"
            "    --x_column TS-ISO8601-ue521sgm1:monoGetEnergy \\\n"
            "    --series file=scan_results-20251106.txt,column=ue521sgm1:liIDcics,axis=0,label=RingCurrent \\\n"
            "    --series file=meta_ue52_pid.txt,column=VALUE,axis=1,label=PID_OUT,type=meta\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--x_column", required=True, help="Column to use as X axis (time or position)")
    parser.add_argument(
        "--series",
        action="append",
        required=True,
        help=(
            "Series spec: file=PATH,column=NAME[,axis=N,label=TEXT,type=scan|meta].\n"
            "Repeat for multiple plotted series. Example: "
            "--series file=scan.txt,column=intensity,axis=0,label=I0"
        ),
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
    except ValueError as exc:
        parser.error(str(exc))

    cfg = PostMortemConfig(
        x_column=args.x_column,
        series=series,
        # join_on_time=None,   # TODO: using TS columns directly in build_combined_dataframe
        join_tolerance=args.join_tol,
    )
    print(f"PostMortemConfig {cfg}")
    plot_postmortem(
        cfg=cfg,
    )


if __name__ == "__main__":
    main()
