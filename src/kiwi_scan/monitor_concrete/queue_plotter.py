# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import collections
import logging
import math
import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.monitor.row_format import MonitorRowFormatter

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class PlotSpec:
    """One live plot panel.

    ``y`` may contain one or more channel names. All Y channels are drawn into
    the same matplotlib axes against the same X channel.
    """

    x: str
    y: List[str]
    title: str = ""
    xlabel: Optional[str] = None
    ylabel: Optional[str] = None


class QueuePlotterMonitor(BaseMonitor):
    """Queue-based live plotting monitor with optional PrintMonitor-compatible output.

    Hot-path design: ``update()`` runs in the scan thread and only appends the raw row. 
    Buffering and drawing happen in the GUI thread. 
    The plot uses points of a fixed-size NumPy ring buffer.  
    Plot lines are created once and are reused.
    YAML example::

        monitor_type: plot
        monitor:
          print:
            enabled: true
            format: tsv
            include_header: true
          max_points: 10000
          max_draw_points: 2000
          refresh_interval: 0.1
          plots:
            - x: t
              y: [detector1, detector2]
              title: Detectors
            - x: detector1
              y: detector2
              title: Correlation
    """

    DEFAULT_MAX_POINTS = 10000
    DEFAULT_MAX_DRAW_POINTS = 2000
    DEFAULT_REFRESH_INTERVAL = 0.1

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        self.parameters = parameters or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.max_points = self._positive_int("max_points", self.DEFAULT_MAX_POINTS)
        self.max_draw_points = self._positive_int("max_draw_points", self.DEFAULT_MAX_DRAW_POINTS)
        self.refresh_interval = self._positive_float("refresh_interval", self.DEFAULT_REFRESH_INTERVAL)
        self.marker: Optional[str] = self.parameters.get("marker") or None
        # Scan thread -> GUI thread hand-off: collections.deque.append/popleft are fast and atomic with GIL (lock) 
        # update() never blocks. When the GUI falls behind by more than max_points rows, the oldest are lost.
        self.pending: Deque[Tuple[float, List[Any]]] = collections.deque(maxlen=self.max_points)
        self._points_received = 0  # written by the scan thread only
        self._points_drained = 0   # written by the GUI thread only
        self.root: Optional[Any] = None  # Will be set in start() when GUI is available.
        self._plt: Optional[Any] = None
        self.running = False
        self._headless = False
        self._closed_by_user = False
        self._start_time: Optional[float] = None
        self.signal_names: List[str] = []
        # Plot history (ring buffer), 2-D NumPy array of shape (number of signals, max_points)
        # slot:    0    1    2    3    4
        # t     [ 5.0  6.0  2.0  3.0  4.0 ]   <- points 5,6 overwrote 0,1
        # det1  [ ...                      ]
        # det2  [ ...                      ]
        self._buffer: Optional[np.ndarray] = None
        self._buffer_count = 0  # total points written into the ring buffer
        self._lines: List[Tuple[Any, List[Tuple[Any, int, int]]]] = []
        self.plot_specs: List[PlotSpec] = []
        self.x_signal = None  # backward-compatible single-plot setter support
        self.y_signal = None

        print_cfg = self._print_config(self.parameters)
        self._print_enabled = bool(print_cfg.pop("enabled", True))
        self._print_parameters = dict(print_cfg)
        self._row_formatter = MonitorRowFormatter(self._print_parameters, logger=self.logger)
        self._value_formatter = self._row_formatter.value_formatter

    def _positive_int(self, key: str, default: int) -> int:
        value = int(self.parameters.get(key, default))
        if value <= 0:
            raise ValueError(f"queue_plotter: monitor.{key} must be > 0")
        return value

    def _positive_float(self, key: str, default: float) -> float:
        value = float(self.parameters.get(key, default))
        if value <= 0.0:
            raise ValueError(f"queue_plotter: monitor.{key} must be > 0")
        return value

    @staticmethod
    def _print_config(parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Return the nested monitor.print configuration block."""
        raw = parameters.get("print", {})
        if raw is False:
            return {"enabled": False}
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise TypeError("queue_plotter: monitor.print must be a mapping or false")
        return dict(raw)

    @staticmethod
    def _display_is_available() -> bool:
        """Figure out and return False when plotter cannot create displays in this environment."""
        if os.name != "posix" or sys.platform == "darwin":
            return True
        return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))

    @staticmethod
    def _import_tk():
        import tkinter as tk
        from tkinter import ttk
        return tk, ttk

    @staticmethod
    def _import_pyplot():
        import matplotlib
        try:
            matplotlib.use('TkAgg')
        except ImportError:
            logger.warning("TkAgg backend is unavailable; keeping matplotlib's current backend")
        import matplotlib.pyplot as plt
        return plt

    def start(self, signal_names: Iterable[str], headers: Optional[Iterable[str]] = None) -> None:
        # Create all Tk objects in the same thread that will later run loop().
        self._start_time = time.time()
        raw_signal_names = list(headers) if headers is not None else list(signal_names)
        self.signal_names = ["t"] + raw_signal_names
        self._buffer = None  # allocated by the GUI thread on first drain
        self._buffer_count = 0
        self.pending.clear()
        self._points_received = 0
        self._points_drained = 0
        self.plot_specs = self._parse_plot_specs(self.parameters, self.signal_names)

        self._headless = not self._display_is_available()
        if self._headless and not self._print_enabled:
            self.logger.warning(
                "QueuePlotterMonitor detected a headless environment and monitor.print is disabled; "
                "enabling print fallback automatically"
            )
            self._print_enabled = True
            self._print_parameters = {}

        if self._print_enabled:
            # Print the exact same concrete stream as PrintMonitor
            print_parameters = dict(self._print_parameters)
            if headers is not None:
                print_parameters["include_timestamps"] = False
            self._row_formatter = MonitorRowFormatter(print_parameters, logger=self.logger)
            self._value_formatter = self._row_formatter.value_formatter
            self._row_formatter.start(raw_signal_names)

        if self._headless:
            self.logger.warning(
                "QueuePlotterMonitor detected no graphical display; "
                "live plotting is disabled and print output is used instead"
            )
            self.running = False
            self.logger.debug(
                "QueuePlotterMonitor headless fallback signals=%r plots=%r",
                self.signal_names,
                self.plot_specs,
            )
            return

        try:
            tk, ttk = self._import_tk()
            self._tk = tk
            self._ttk = ttk
            self.root = tk.Tk()
        except Exception as exc: # noqa BLE001
            self._headless = True
            self.running = False
            self.root = None
            if not self._print_enabled:
                self.logger.warning("QueuePlotterMonitor could not start Tk GUI and monitor.print is disabled; enabling print fallback automatically")
                self._print_enabled = True
                self._print_parameters = {}
                self._row_formatter = MonitorRowFormatter({}, logger=self.logger)
                self._value_formatter = self._row_formatter.value_formatter
                self._row_formatter.start(raw_signal_names)
            self.logger.warning( "QueuePlotterMonitor could not start Tk GUI (%s); "
                "live plotting is disabled and print output is used instead", exc)
            return

        # Importing the matplotlib must happen before the scan thread runs, no locking.
        self._plt = self._import_pyplot()
        self.running = True
        self._start_gui()
        self.logger.debug("QueuePlotterMonitor signals=%r plots=%r", self.signal_names, self.plot_specs)

    def update(self, vals: List[Any]) -> None:
        if self._print_enabled:
            self._row_formatter.write(vals)

        if self._start_time is None:
            self.logger.error("QueuePlotterMonitor.update() called before start()")
            return

        if self._headless or not self.running:
            # GUI not started, closed by the user, or close() called.
            return

        # Contract:
        # - optimized "hot path" processing.
        # - the caller must pass a new list per point and not mutate it afterwards. 
        # - the GUI thread reads it later from `pending`. 
        # - Scan engines must do _commit_point_async and return a fresh list. 
        self.pending.append((time.time(), vals))
        self._points_received += 1

    def set_signals(self, x: str, y: str) -> None:
        """Backward-compatible single-panel override might be used for interactive features later."""
        self.plot_specs = [PlotSpec(x=x, y=[y], title=y)]
        if self.x_signal is not None:
            self.x_signal.set(x)
        if self.y_signal is not None:
            self.y_signal.set(y)

    def _plot_value(self, item: Any) -> float:
        value = self._value_formatter.extract_value(item)
        if value is None:
            return math.nan
        try:
            return float(value)
        except (TypeError, ValueError):
            return math.nan

    @classmethod
    def _parse_plot_specs(
        cls,
        parameters: Dict[str, Any],
        available_channels: Sequence[str],
    ) -> List[PlotSpec]:
        available = list(available_channels)
        raw_specs = parameters.get("plots") or []

        if not raw_specs:
            if len(available) > 1:
                return [PlotSpec(x="t", y=[available[1]], title=available[1])]
            return []

        if not isinstance(raw_specs, list):
            raise TypeError("monitor.plots must be a list of plot specifications")

        specs: List[PlotSpec] = []
        for idx, item in enumerate(raw_specs, start=1):
            if not isinstance(item, dict):
                raise TypeError(f"monitor.plots[{idx}] must be a mapping")

            x_name = str(item.get("x", "t"))
            if "y" not in item:
                raise ValueError(f"monitor.plots[{idx}] is missing required key 'y'")

            y_names = cls._normalize_y_names(item["y"])
            title = str(item.get("title") or ", ".join(y_names))
            xlabel = item.get("xlabel")
            ylabel = item.get("ylabel")

            missing = [name for name in [x_name] + y_names if name not in available]
            if missing:
                unknown_channels = ", ".join(repr(name) for name in missing)
                available_channels = ", ".join(available)
                raise ValueError(f"Unknown queue plot channel(s): {unknown_channels}. Available channels are: {available_channels}")

            specs.append(
                PlotSpec(
                    x=x_name,
                    y=y_names,
                    title=title,
                    xlabel=str(xlabel) if xlabel is not None else None,
                    ylabel=str(ylabel) if ylabel is not None else None,
                )
            )

        return specs

    @staticmethod
    def _normalize_y_names(raw_y: Any) -> List[str]:
        if isinstance(raw_y, str):
            names = [raw_y]
        elif isinstance(raw_y, (list, tuple)):
            names = [str(item) for item in raw_y]
        else:
            raise TypeError("plot 'y' must be a channel name or a list of channel names")

        names = [name for name in names if name]
        if not names:
            raise ValueError("plot 'y' must contain at least one channel")
        return names

    def _start_gui(self) -> None:
        def on_close() -> None:
            # Do not destroy the Tk root from here. The loop() method owns all
            # Tk lifecycle calls and will clean up from the GUI/main thread.
            self._closed_by_user = True
            self.running = False

        if self.root is None:
            return
        tk = getattr(self, '_tk', None)
        ttk = getattr(self, '_ttk', None)
        if tk is None or ttk is None:
            tk, ttk = self._import_tk()
            self._tk = tk
            self._ttk = ttk

        self.root.title("Live Queue Plotter")
        self.root.protocol("WM_DELETE_WINDOW", on_close)

        ttk.Label(self.root, text="Configured live plots").grid(
            row=0,
            column=0,
            sticky="w",
            padx=4,
            pady=4,
        )

        if not self.plot_specs:
            ttk.Label(self.root, text="No plots configured").grid(row=1, column=0, sticky="w")
            return

        for row, spec in enumerate(self.plot_specs, start=1):
            label = f"{spec.title}: {spec.x} -> {', '.join(spec.y)}"
            ttk.Label(self.root, text=label).grid(row=row, column=0, sticky="w", padx=4)

        # Compatibility vars for old set_signals() users. They are no longer
        # connected to comboboxes because YAML is now the plot source of truth.
        first = self.plot_specs[0]
        self.x_signal = tk.StringVar(self.root, value=first.x)
        self.y_signal = tk.StringVar(self.root, value=first.y[0])

    def loop(self) -> None:
        if self._headless:
            self.logger.info("QueuePlotterMonitor loop skipped because no graphical display is available")
            return

        self.logger.info("[%s] In monitor.loop(), driving Tkinter from this thread", threading.current_thread().name)
        plt = self._plt or self._import_pyplot()
        self._ensure_tk()
        plt.ion()

        fig = None
        try:
            fig, axes = self._create_plot_canvas(plt)
            self._run_plot_loop(plt, fig, axes)
        finally:
            self._cleanup_gui(plt, fig)

    def _ensure_tk(self) -> None:
        """Ensure the Tk module needed for GUI error handling is available."""
        if getattr(self, "_tk", None) is not None:
            return

        tk, _ttk = self._import_tk()
        self._tk = tk

    def _create_plot_canvas(self, plt: Any) -> Tuple[Any, List[Any]]:
        """Create the figure and axes required by the configured plots."""
        if self.plot_specs:
            fig, axes = plt.subplots(
                len(self.plot_specs),
                1,
                figsize=(8, max(4, 3 * len(self.plot_specs))),
                squeeze=False,
            )
            return fig, [row[0] for row in axes]

        fig, ax = plt.subplots(figsize=(8, 4))
        return fig, [ax]

    def _run_plot_loop(self, plt: Any, fig: Any, axes: Sequence[Any]) -> None:
        """Process pending points and refresh the live plots until stopped."""
        if self.plot_specs:
            self._setup_plots(axes, fig)
        else:
            axes[0].set_title("No live plots configured")
            fig.canvas.draw_idle()

        while self.running or self.pending:
            has_new_points = self._drain_queue()
            self._update_tk()

            if has_new_points and self.plot_specs:
                self._draw_plots(axes, fig)

            plt.pause(self.refresh_interval)

        dropped = self._points_received - self._points_drained
        if dropped > 0:
            self.logger.warning(
                "QueuePlotterMonitor dropped %d of %d points because the GUI fell behind (max_points=%d)",
                dropped,
                self._points_received,
                self.max_points,
            )

    def _cleanup_gui(self, plt: Any, fig: Any) -> None:
        """Release Matplotlib and Tk resources without aborting the scan."""
        plt.ioff()

        if fig is not None:
            try:
                plt.close(fig)
            except Exception as exc:  # noqa: BLE001 - cleanup must not abort the scan
                self.logger.debug("Failed to close plot figure: %s", exc)

        if self.root is not None:
            try:
                self.root.destroy()
            except self._tk.TclError as exc:
                self.logger.debug("Failed to destroy Tk root window: %s", exc)
            finally:
                self.root = None

    def _drain_queue(self) -> bool:
        """Move pending rows into the ring buffer. Return True if any arrived."""
        pending = self.pending
        count = len(pending)
        if count == 0:
            return False
        if self._buffer is None:
            self._buffer = np.full((len(self.signal_names), self.max_points), np.nan)

        buffer = self._buffer
        capacity = self.max_points
        start_time = self._start_time or 0.0
        n_values = len(self.signal_names) - 1
        plot_value = self._plot_value
        for _ in range(count):
            timestamp, vals = pending.popleft()
            column = self._buffer_count % capacity
            buffer[0, column] = timestamp - start_time
            n_present = min(len(vals), n_values)
            for idx in range(n_present):
                buffer[idx + 1, column] = plot_value(vals[idx])
            # Short rows: remaining channels are NaN.
            buffer[n_present + 1:, column] = np.nan
            self._buffer_count += 1
        self._points_drained += count
        return True

    def _ordered_data(self) -> np.ndarray:
        """Return buffered points oldest first, decimated to max_draw_points."""
        if self._buffer is None:
            return np.empty((len(self.signal_names), 0))
        capacity = self.max_points
        count = self._buffer_count
        if count <= capacity:
            data = self._buffer[:, :count]
        else:
            split = count % capacity
            data = np.concatenate((self._buffer[:, split:], self._buffer[:, :split]), axis=1)
        step = max(1, -(-data.shape[1] // self.max_draw_points))
        return data[:, ::step]

    def _update_tk(self) -> None:
        if self.root is None:
            return
        try:
            self.root.update_idletasks()
            self.root.update()
        except self._tk.TclError:
            self.running = False
            self.root = None

    def _setup_plots(self, axes: Sequence[Any], fig: Any) -> None:
        """Create lines, labels, legends and layout once; frames only update data."""
        index = {name: idx for idx, name in enumerate(self.signal_names)}
        self._lines = []
        for ax, spec in zip(axes, self.plot_specs):
            lines = []
            for y_name in spec.y:
                (line,) = ax.plot([], [], marker=self.marker, linestyle="-", label=y_name)
                lines.append((line, index[spec.x], index[y_name]))
            ax.set_xlabel(spec.xlabel or spec.x)
            ax.set_ylabel(spec.ylabel or ", ".join(spec.y))
            ax.set_title(spec.title)
            if len(spec.y) > 1:
                # Optimized fixed location: loc="best" would scans all points on every draw.
                ax.legend(loc="upper left")
            ax.grid(True)
            self._lines.append((ax, lines))

        fig.tight_layout()
        fig.canvas.draw_idle()

    def _draw_plots(self, axes: Sequence[Any], fig: Any) -> None:
        del axes  # lines were bound to their axes in _setup_plots()
        data = self._ordered_data()
        for ax, lines in self._lines:
            for line, x_idx, y_idx in lines:
                line.set_data(data[x_idx], data[y_idx])
            ax.relim()
            ax.autoscale_view()

        fig.canvas.draw_idle()

    def close(self) -> None:
        # close() may be called by the scan worker thread. Keep it free of Tk calls
        # ---- > it tells loop() to finish and perform GUI cleanup.
        self.running = False
        self._row_formatter.close()
