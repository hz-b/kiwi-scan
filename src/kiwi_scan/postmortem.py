# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import logging

from kiwi_scan.dataloader import DataLoader
from kiwi_scan.metadata_loader import parse_metadata_file, MetadataFile
from kiwi_scan.plotter import Plotter


@dataclass
class SeriesSpec:
    file: str
    column: str        # for meta: PV name (after pivot)
    axis: int = 0
    label: Optional[str] = None
    source_type: str = "scan"  # "scan" or "meta"
    # TODO: for waveforms add something like: element_index, use case: multi actuator velocity readback

@dataclass
class PostMortemConfig:
    x_column: str
    series: List[SeriesSpec]
    join_tolerance: float = 0.1


def _load_scan_dataframe(path: str) -> Optional[pd.DataFrame]:
    loader = DataLoader(path)
    return loader.load_data()


def plot_postmortem(cfg: PostMortemConfig) -> None:
    """
    Generic multi-axis post-mortem plotter.
    It assumes:
      - scan sources: one row per position/time, columns already shaped
      - meta sources: use metadata_loader to pivot PVs to columns
    """
    plotter = Plotter(title="Post Mortem Plot")

    for s in cfg.series:
        if s.source_type == "meta":
            meta = parse_metadata_file(s.file)
            if not meta:
                continue
            df = meta.df_pivot

            if cfg.x_column not in df.index.names:
                raise ValueError(f"For meta series, x_column '{cfg.x_column}' must be the index (e.g. TS-ISO8601)")

            if s.column not in df.columns:
                logging.warning("PV '%s' not found in metadata file %s", s.column, s.file)
                continue

            x = df.index
            y = df[s.column]
            label = s.label or s.column

        else:
            df = _load_scan_dataframe(s.file)
            if df is None:
                continue
            if cfg.x_column not in df.columns or s.column not in df.columns:
                logging.warning("Columns '%s' or '%s' not found in scan file %s", cfg.x_column, s.column, s.file)
                continue
            x = df[cfg.x_column]
            y = df[s.column]
            label = s.label or s.column

        plotter.add_series(x=x, y=y, label=label, axis=s.axis)

    plotter.plot(subplot=False, multi_axis=True)

