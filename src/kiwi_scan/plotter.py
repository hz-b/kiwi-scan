# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import matplotlib
matplotlib.use('TkAgg')  # Must come BEFORE importing pyplot, temporary workaround TODO: should not be hard coded here
import matplotlib.pyplot as plt
import numpy as np
from typing import List, Optional, Union
from pathlib import Path
import pandas as pd

class PlotData:
    """
    Lightweight container for 1D plot series data.
    Stores x/y values as NumPy arrays plus optional metadata (label and axis index).
    """
    def __init__(self, x, y, label=None, axis: int = 0):
        self.x = np.array(x)
        self.y = np.array(y)
        self.label = label
        self.axis = axis    

    def derivative(self):
        dy = np.gradient(self.y, self.x)
        return PlotData(self.x, dy, label=f"Derivative({self.label})")

    def multiply(self, other):
        assert np.array_equal(self.x, other.x), "X data must match!"
        return PlotData(self.x, self.y * other.y, label=f"({self.label})*({other.label})")

class Plotter:
    """
    Handles plotting of multiple data sets using matplotlib.
    """
    def __init__(
        self,
        title: str = None,
        xlabel: str = "X",
        ylabel: str = "Y",
        title_fontsize: int = 12,
        label_fontsize: int = 10,
    ):
        self.plots: List[PlotData] = []
        self.title = title
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.title_fontsize = title_fontsize
        self.label_fontsize = label_fontsize

    def add_series(self, x, y, label=None, axis: int = 0):
        self.plots.append(PlotData(x, y, label=label, axis=axis))

    def add_dataframe_columns_as_plotdata(
        self,
        df: pd.DataFrame,
        y_columns: List[str],
        position_column: str,
        labels: List[str] = None
    ):
        if df is None or position_column not in df.columns:
            logging.error("Invalid DataFrame or position column provided.")
            return

        for i, y_col in enumerate(y_columns):
            if y_col not in df.columns:
                logging.warning(f"Column '{y_col}' not found in DataFrame.")
                continue
            label = labels[i] if labels and i < len(labels) else y_col
            plot_data = PlotData(df[position_column], df[y_col], label=label)
            self.add_plot(plot_data)

    def add_plot(self, plot_data: 'PlotData'):
        self.plots.append(plot_data)

    def export_space_delimited(
        self,
        path: Union[str, Path],
        include_x: bool = True,
        x_label: Optional[str] = None,
        float_fmt: str = "%.10g"
    ) -> Path:
        """
        Export all series to a single space-delimited text file with a header.
        Requires all series share the exact same X values.

        Header format: "# X <label1> <label2> ..."
        Data rows:     "<x> <y1> <y2> ..."

        Returns the output path.
        """
        if not self.plots:
            raise ValueError("No data to export.")

        # Use first plot's x as reference
        ref_x = self.plots[0].x
        for i, p in enumerate(self.plots[1:], start=1):
            if not (len(p.x) == len(ref_x) and np.array_equal(p.x, ref_x)):
                raise ValueError(
                    f"Series '{p.label or i}' has X that does not match the first series. "
                    "Use export_each_series() or resample/align first."
                )

        cols = []
        header_labels = []
        if include_x:
            cols.append(ref_x)
            header_labels.append(x_label or self.xlabel or "X")

        for p in self.plots:
            cols.append(p.y)
            header_labels.append(p.label or "series")

        arr = np.column_stack(cols)
        out_path = Path(path)
        header = "# " + " ".join(header_labels)
        np.savetxt(out_path, arr, fmt=float_fmt, delimiter=" ", header=header, comments="")
        logging.info(f"Exported space-delimited data to {out_path}")
        return out_path

    def export_each_series(
        self,
        directory: Union[str, Path],
        include_x: bool = True,
        x_label: Optional[str] = None,
        float_fmt: str = "%.10g",
        basename: str = "series"
    ) -> List[Path]:
        """
        Export each series to its own file, safe when X differ.
        File names: <basename>_<i>_<label>.txt
        Header: "# X <label>" (or just "# <label>" if include_x=False)
        """
        if not self.plots:
            raise ValueError("No data to export.")

        out_dir = Path(directory)
        out_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for i, p in enumerate(self.plots):
            label_safe = (p.label or f"s{i}").replace(" ", "_")
            path = out_dir / f"{basename}_{i}_{label_safe}.txt"
            if include_x:
                arr = np.column_stack((p.x, p.y))
                header = "# " + " ".join([x_label or self.xlabel or "X", p.label or "series"])
            else:
                arr = p.y.reshape(-1, 1)
                header = "# " + (p.label or "series")
            np.savetxt(path, arr, fmt=float_fmt, delimiter=" ", header=header, comments="")
            paths.append(path)
        logging.info(f"Exported {len(paths)} files to {out_dir}")
        return paths

    # --- plotting ---------------------------------------------------------

    def plot(self, subplot: bool = False, multi_axis: bool = False):
        """ Render the stored PlotData series using matplotlib.
        Modes:
          - subplot=True: one PlotData per row (stacked subplots).
          - multi_axis=True: shared X, multiple Y axes via twinx() selected by PlotData.axis.
          - default: all series on a single axis.
          TODO: Better color scheme for multi axis plots

        If no plot data is present, logs a warning and returns.
        """
        if not self.plots:
            logging.warning("No data available for plotting.")
            return

        if subplot:
            logging.info("subplot")
            fig, axes = plt.subplots(len(self.plots), 1, figsize=(8, 3 * len(self.plots)))
            if len(self.plots) == 1:
                axes = [axes]
            for ax, data in zip(axes, self.plots):
                ax.plot(data.x, data.y, marker='o', linestyle='-', label=data.label)
                ax.set_xlabel(self.xlabel, fontsize=self.label_fontsize)
                ax.set_ylabel(data.label or self.ylabel, fontsize=self.label_fontsize)
                ax.set_title(data.label or "", fontsize=self.title_fontsize)
                ax.legend()
                ax.grid(True)
                plt.tight_layout()
                plt.show()
                return
        
        # --- multi-axis mode (shared X, several Y scales) ---
        if multi_axis:
            fig, ax0 = plt.subplots(figsize=(8, 5))
            axes = {0: ax0}

            # At the moment: up to 3 axes (0: left, 1/2: right with offset)
            for data in self.plots:
                ax = axes.get(data.axis)
                if ax is None:
                    # create new y-axis on the right
                    ax = ax0.twinx()
                    axes[data.axis] = ax
                ax.plot(data.x, data.y, marker='o', linestyle='-', label=data.label)

            # Label axes
            for idx, ax in axes.items():
                ax.set_xlabel('X')
                ax.grid(True, axis='y', alpha=0.3)
                # Put legend on each axis
                ax.legend(loc='best')

            fig.suptitle(self.title or "Post Mortem Multi-Axis Plot")
            plt.tight_layout()
            plt.show()
            return


        plt.figure(figsize=(8, 5))
        for data in self.plots:
            plt.plot(data.x, data.y, marker='o', linestyle='-', label=data.label)
        plt.xlabel(self.xlabel, fontsize=self.label_fontsize)
        plt.ylabel(self.ylabel, fontsize=self.label_fontsize)
        plt.title(self.title or "Combined Plot", fontsize=self.title_fontsize)
        plt.legend()
        plt.grid(True)

        plt.tight_layout()
        plt.show()

def plot_scan_data(
    data: pd.DataFrame,
    y_columns: List[str],
    position_column: str,
    data_file: str,
    export_path: Optional[Union[str, Path]] = None
):
    """
    Plot and (optionally) export space-delimited text for debugging.
    If export_path is a file, attempts combined export (requires same X).
    If export_path is a directory, exports one file per series.
    """
    plotter = Plotter()
    plotter.add_dataframe_columns_as_plotdata(data, y_columns, position_column)
    plotter.plot(subplot=True)

    if export_path:
        export_path = Path(export_path)
        try:
            if export_path.suffix:  # looks like a file path
                plotter.export_space_delimited(export_path, include_x=True)
            else:  # directory
                plotter.export_each_series(export_path, include_x=True)
        except ValueError as e:
            # If X don't match, fall back to per-series export in same directory as the file path
            logging.warning(f"{e} Falling back to per-series export.")
            if export_path.suffix:
                plotter.export_each_series(export_path.parent, include_x=True)
            else:
                plotter.export_each_series(export_path, include_x=True)

