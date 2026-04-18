# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import argparse
import logging
from typing import List

from kiwi_scan.postmortem import PostMortemConfig, SeriesSpec, plot_postmortem

# TODO: FIXME: small typo will crash parsing! 
def parse_series_args(args: List[str]) -> List[SeriesSpec]:
    """
    Parse specs of form:
        file=PATH,column=NAME,axis=N,label=TEXT
    """
    result: List[SeriesSpec] = []
    for spec in args:
        kv = dict(part.split("=", 1) for part in spec.split(","))
        file = kv.pop("file")
        column = kv.pop("column")
        axis = int(kv.pop("axis", 0))
        label = kv.pop("label", None)
        source_type = kv.pop("type", "scan")
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
        help="Series spec: file=PATH,column=NAME[,axis=N,label=TEXT,type=scan|meta]",
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

    series = parse_series_args(args.series)
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
