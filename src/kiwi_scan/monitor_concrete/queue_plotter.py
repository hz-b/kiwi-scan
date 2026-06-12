# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.monitor.row_format import MonitorRowFormatter


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

    YAML example::

        monitor_type: queueplotter
        monitor:
          print:
            enabled: true
            format: tsv
            include_header: true
          plots:
            - x: t
              y: [detector1, detector2]
              title: Detectors
            - x: detector1
              y: detector2
              title: Correlation
    """

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        self.parameters = parameters or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.root: Optional[Any] = None  # Will be set in start().
        self.running = False
        self._closed_by_user = False
        self._start_time: Optional[float] = None
        self.signal_names: List[str] = []
        self.data: Dict[str, List[Any]] = {}
        self.plot_specs: List[PlotSpec] = []
        self.x_signal = None  # backward-compatible single-plot setter support
        self.y_signal = None

        print_cfg = self._print_config(self.parameters)
        self._print_enabled = bool(print_cfg.pop("enabled", True))
        self._print_parameters = dict(print_cfg)
        self._row_formatter = MonitorRowFormatter(self._print_parameters, logger=self.logger)
        self._value_formatter = self._row_formatter.value_formatter

    @staticmethod
    def _print_config(parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Return the nested monitor.print configuration block."""
        raw = parameters.get("print", {})
        if raw is False:
            return {"enabled": False}
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ValueError("monitor.print must be a mapping or false")
        return dict(raw)

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
            logging.warning(
                "TkAgg backend is unavailable; keeping matplotlib's current backend"
            )
        import matplotlib.pyplot as plt
        return plt

    def start(self, signal_names: Iterable[str], headers: Optional[Iterable[str]] = None) -> None:
        # Create all Tk objects in the same thread that will later run loop().
        self._start_time = time.time()
        raw_signal_names = list(headers) if headers is not None else list(signal_names)
        self.signal_names = ["t"] + raw_signal_names
        self.data = {name: [] for name in self.signal_names}
        self.plot_specs = self._parse_plot_specs(self.parameters, self.signal_names)

        if self._print_enabled:
            # Print the exact same concrete stream as PrintMonitor
            print_parameters = dict(self._print_parameters)
            if headers is not None:
                print_parameters["include_timestamps"] = False
                self._row_formatter = MonitorRowFormatter(print_parameters, logger=self.logger)
                self._value_formatter = self._row_formatter.value_formatter
            self._row_formatter.start(raw_signal_names)

        tk, ttk = self._import_tk()
        self._tk = tk
        self._ttk = ttk
        self.root = tk.Tk()
        self.running = True
        self._start_gui()
        self.logger.debug("QueuePlotterMonitor signals=%r plots=%r", self.signal_names, self.plot_specs)

    def update(self, vals: List[Any]) -> None:
        if self._print_enabled:
            self._row_formatter.write(vals)

        if self._start_time is None:
            self.logger.error("QueuePlotterMonitor.update() called before start()")
            return

        values_only = [self._plot_value(item) for item in vals]
        now = time.time() - self._start_time
        values_dict = dict(zip(self.signal_names, [now] + values_only))
        point = {k: values_dict.get(k, float("nan")) for k in self.signal_names}
        self.queue.put(point)

    def set_signals(self, x: str, y: str) -> None:
        """Backward-compatible single-panel override might be used for interactive features later."""
        self.plot_specs = [PlotSpec(x=x, y=[y], title=y)]
        if self.x_signal is not None:
            self.x_signal.set(x)
        if self.y_signal is not None:
            self.y_signal.set(y)

    def _plot_value(self, item: Any) -> Any:
        value = self._value_formatter.extract_value(item)
        if value is None:
            return float("nan")
        return value

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
            raise ValueError("monitor.plots must be a list of plot specifications")

        specs: List[PlotSpec] = []
        for idx, item in enumerate(raw_specs, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"monitor.plots[{idx}] must be a mapping")

            x_name = str(item.get("x", "t"))
            if "y" not in item:
                raise ValueError(f"monitor.plots[{idx}] is missing required key 'y'")

            y_names = cls._normalize_y_names(item["y"])
            title = str(item.get("title") or ", ".join(y_names))
            xlabel = item.get("xlabel")
            ylabel = item.get("ylabel")

            missing = [name for name in [x_name] + y_names if name not in available]
            if missing:
                raise ValueError(
                    "Unknown queue plot channel(s) %s. Available channels are: %s"
                    % (", ".join(repr(name) for name in missing), ", ".join(available))
                )

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
            raise ValueError("plot 'y' must be a channel name or a list of channel names")

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

        # Compatibility vars for old set_signals(). They are no longer connected to comboboxes at the moment
        # YAML is now the plot config source.
        first = self.plot_specs[0]
        self.x_signal = tk.StringVar(self.root, value=first.x)
        self.y_signal = tk.StringVar(self.root, value=first.y[0])

    def loop(self) -> None:
        logging.info(
            "[%s] In monitor.loop(), driving Tkinter from this thread",
            threading.current_thread().name,
        )
        plt = self._import_pyplot()
        tk = getattr(self, '_tk', None)
        if tk is None:
            tk, _ttk = self._import_tk()
            self._tk = tk
        plt.ion()
        fig = None
        try:
            if self.plot_specs:
                fig, axes = plt.subplots(
                    len(self.plot_specs),
                    1,
                    figsize=(8, max(4, 3 * len(self.plot_specs))),
                    squeeze=False,
                )
                axes_list = [row[0] for row in axes]
            else:
                fig, ax = plt.subplots(figsize=(8, 4))
                axes_list = [ax]

            while self.running or not self.queue.empty():
                self._drain_queue()
                self._update_tk()

                if self.plot_specs:
                    self._draw_plots(axes_list, fig)
                else:
                    axes_list[0].set_title("No live plots configured")
                    fig.canvas.draw_idle()

                plt.pause(0.05)
        finally:
            plt.ioff()
            if fig is not None:
                try:
                    plt.close(fig)
                except Exception:
                    pass
            if self.root is not None:
                try:
                    self.root.destroy()
                except self._tk.TclError:
                    pass
                finally:
                    self.root = None

    def _drain_queue(self) -> None:
        try:
            while True:
                point = self.queue.get_nowait()
                self.logger.debug("point: %r", point)
                for name in self.signal_names:
                    self.data[name].append(point[name])
        except queue.Empty:
            pass

    def _update_tk(self) -> None:
        if self.root is None:
            return
        try:
            self.root.update_idletasks()
            self.root.update()
        except self._tk.TclError:
            self.running = False
            self.root = None

    def _draw_plots(self, axes: Sequence[Any], fig: Any) -> None:
        for ax, spec in zip(axes, self.plot_specs):
            ax.clear()
            x_values = self.data.get(spec.x, [])
            if not x_values:
                ax.set_title(spec.title)
                continue

            for y_name in spec.y:
                y_values = self.data.get(y_name, [])
                n = min(len(x_values), len(y_values))
                if n <= 0:
                    continue
                ax.plot(x_values[:n], y_values[:n], marker='o', linestyle='-', label=y_name)

            ax.set_xlabel(spec.xlabel or spec.x)
            ax.set_ylabel(spec.ylabel or ", ".join(spec.y))
            ax.set_title(spec.title)
            if len(spec.y) > 1:
                ax.legend(loc="best")
            ax.grid(True)

        fig.tight_layout()
        fig.canvas.draw_idle()

    def close(self) -> None:
        # close() may be called by the scan worker thread. Keep it free of Tk calls
        # ---- > it tells loop() to finish and perform GUI cleanup.
        self.running = False
        self._row_formatter.close()
