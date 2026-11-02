# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import io
import math
import unittest
from unittest.mock import MagicMock, patch

import numpy as np

from kiwi_scan.monitor_concrete.queue_plotter import PlotSpec, QueuePlotterMonitor


class TestQueuePlotterConfiguration(unittest.TestCase):
    def test_print_config_defaults_to_empty_mapping(self):
        self.assertEqual(QueuePlotterMonitor._print_config({}), {})
        self.assertEqual(QueuePlotterMonitor._print_config({"print": None}), {})

    def test_print_config_false_disables_printing(self):
        self.assertEqual(
            QueuePlotterMonitor._print_config({"print": False}),
            {"enabled": False},
        )

    def test_print_config_returns_copy(self):
        raw_print_config = {"enabled": True, "format": "csv"}

        result = QueuePlotterMonitor._print_config({"print": raw_print_config})
        result["format"] = "json"

        self.assertEqual(raw_print_config["format"], "csv")

    def test_print_config_rejects_non_mapping(self):
        with self.assertRaisesRegex(
            TypeError,
            "monitor.print must be a mapping or false",
        ):
            QueuePlotterMonitor._print_config({"print": "csv"})

    def test_default_plot_uses_time_and_first_signal(self):
        specs = QueuePlotterMonitor._parse_plot_specs({}, ["t", "detector"])

        self.assertEqual(
            specs,
            [PlotSpec(x="t", y=["detector"], title="detector")],
        )

    def test_no_default_plot_without_data_signal(self):
        specs = QueuePlotterMonitor._parse_plot_specs({}, ["t"])

        self.assertEqual(specs, [])

    def test_parse_multiple_plot_specs(self):
        parameters = {
            "plots": [
                {
                    "x": "t",
                    "y": ["detector1", "detector2"],
                    "title": "Detectors",
                    "xlabel": "Elapsed time",
                    "ylabel": "Intensity",
                },
                {"x": "detector1", "y": "detector2"},
            ]
        }

        specs = QueuePlotterMonitor._parse_plot_specs(
            parameters,
            ["t", "detector1", "detector2"],
        )

        self.assertEqual(
            specs,
            [
                PlotSpec(
                    x="t",
                    y=["detector1", "detector2"],
                    title="Detectors",
                    xlabel="Elapsed time",
                    ylabel="Intensity",
                ),
                PlotSpec(
                    x="detector1",
                    y=["detector2"],
                    title="detector2",
                ),
            ],
        )

    def test_parse_plot_specs_rejects_non_list(self):
        with self.assertRaisesRegex(TypeError, "monitor.plots must be a list"):
            QueuePlotterMonitor._parse_plot_specs(
                {"plots": {"x": "t", "y": "detector"}},
                ["t", "detector"],
            )

    def test_parse_plot_specs_rejects_non_mapping_item(self):
        with self.assertRaisesRegex(
            TypeError,
            r"monitor\.plots\[1\] must be a mapping",
        ):
            QueuePlotterMonitor._parse_plot_specs(
                {"plots": ["detector"]},
                ["t", "detector"],
            )

    def test_parse_plot_specs_requires_y(self):
        with self.assertRaisesRegex(
            ValueError,
            r"monitor\.plots\[1\] is missing required key 'y'",
        ):
            QueuePlotterMonitor._parse_plot_specs(
                {"plots": [{"x": "t"}]},
                ["t", "detector"],
            )

    def test_parse_plot_specs_reports_unknown_channels(self):
        with self.assertRaisesRegex(
            ValueError,
            r"Unknown queue plot channel\(s\): 'missing_x', 'missing_y'",
        ):
            QueuePlotterMonitor._parse_plot_specs(
                {"plots": [{"x": "missing_x", "y": "missing_y"}]},
                ["t", "detector"],
            )

    def test_normalize_y_names_accepts_string_list_and_tuple(self):
        self.assertEqual(
            QueuePlotterMonitor._normalize_y_names("detector"),
            ["detector"],
        )
        self.assertEqual(
            QueuePlotterMonitor._normalize_y_names(["a", "b"]),
            ["a", "b"],
        )
        self.assertEqual(
            QueuePlotterMonitor._normalize_y_names(("a", 2)),
            ["a", "2"],
        )

    def test_normalize_y_names_rejects_invalid_type(self):
        with self.assertRaisesRegex(TypeError, "plot 'y' must be a channel name"):
            QueuePlotterMonitor._normalize_y_names(123)

    def test_performance_options_have_defaults(self):
        monitor = QueuePlotterMonitor({"print": False})

        self.assertEqual(monitor.max_points, QueuePlotterMonitor.DEFAULT_MAX_POINTS)
        self.assertEqual(monitor.max_draw_points, QueuePlotterMonitor.DEFAULT_MAX_DRAW_POINTS)
        self.assertEqual(monitor.refresh_interval, QueuePlotterMonitor.DEFAULT_REFRESH_INTERVAL)
        self.assertIsNone(monitor.marker)
        self.assertEqual(monitor.pending.maxlen, monitor.max_points)

    def test_performance_options_are_read_from_parameters(self):
        monitor = QueuePlotterMonitor(
            {
                "print": False,
                "max_points": 50,
                "max_draw_points": 10,
                "refresh_interval": 0.25,
                "marker": "o",
            }
        )

        self.assertEqual(monitor.max_points, 50)
        self.assertEqual(monitor.max_draw_points, 10)
        self.assertEqual(monitor.refresh_interval, 0.25)
        self.assertEqual(monitor.marker, "o")
        self.assertEqual(monitor.pending.maxlen, 50)

    def test_performance_options_reject_non_positive_values(self):
        for key in ("max_points", "max_draw_points", "refresh_interval"):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, f"monitor.{key} must be > 0"):
                QueuePlotterMonitor({"print": False, key: 0})

    def test_normalize_y_names_rejects_empty_names(self):
        with self.assertRaisesRegex(
            ValueError,
            "plot 'y' must contain at least one channel",
        ):
            QueuePlotterMonitor._normalize_y_names([""])


class TestQueuePlotterLifecycle(unittest.TestCase):
    def test_start_headless_initializes_signals_data_and_default_plot(self):
        output = io.StringIO()
        with patch("sys.stdout", output):
            monitor = QueuePlotterMonitor()
            with patch.object(monitor, "_display_is_available", return_value=False):
                monitor.start(["detector1", "detector2"])

        self.assertEqual(monitor.signal_names, ["t", "detector1", "detector2"])
        self.assertEqual(monitor._ordered_data().shape, (3, 0))
        self.assertEqual(
            monitor.plot_specs,
            [PlotSpec(x="t", y=["detector1"], title="detector1")],
        )
        self.assertTrue(monitor._headless)
        self.assertFalse(monitor.running)
        self.assertEqual(output.getvalue(), "detector1\tdetector2\n")

    def test_start_uses_headers_as_signal_names(self):
        output = io.StringIO()
        parameters = {
            "print": {
                "include_header": True,
                "include_timestamps": True,
            }
        }
        with patch("sys.stdout", output):
            monitor = QueuePlotterMonitor(parameters)
            with patch.object(monitor, "_display_is_available", return_value=False):
                monitor.start(["PV:ONE", "PV:TWO"], headers=["one", "two"])

        self.assertEqual(monitor.signal_names, ["t", "one", "two"])
        self.assertFalse(monitor._row_formatter.include_timestamps)
        self.assertEqual(output.getvalue(), "one\ttwo\n")

    def test_start_headless_enables_print_fallback(self):
        output = io.StringIO()
        with patch("sys.stdout", output):
            monitor = QueuePlotterMonitor({"print": False})
            with patch.object(
                monitor,
                "_display_is_available",
                return_value=False,
            ), self.assertLogs(
                monitor.logger,
                level="WARNING",
            ) as captured:
                monitor.start(["detector"])

        self.assertTrue(monitor._print_enabled)
        self.assertIn("detector\n", output.getvalue())
        self.assertTrue(
            any("enabling print fallback" in message for message in captured.output)
        )

    def test_start_creates_tk_root_when_display_is_available(self):
        monitor = QueuePlotterMonitor({"print": False})
        tk = MagicMock()
        ttk = MagicMock()
        root = MagicMock()
        tk.Tk.return_value = root

        with patch.object(
            monitor,
            "_display_is_available",
            return_value=True,
        ), patch.object(
            monitor,
            "_import_tk",
            return_value=(tk, ttk),
        ), patch.object(
            monitor,
            "_import_pyplot",
        ) as import_pyplot, patch.object(
            monitor,
            "_start_gui",
        ) as start_gui:
            monitor.start(["detector"])

        self.assertIs(monitor.root, root)
        import_pyplot.assert_called_once_with()
        self.assertIs(monitor._plt, import_pyplot.return_value)
        self.assertTrue(monitor.running)
        start_gui.assert_called_once_with()

    def test_start_falls_back_when_tk_creation_fails(self):
        output = io.StringIO()
        with patch("sys.stdout", output):
            monitor = QueuePlotterMonitor({"print": False})
            with patch.object(
                monitor,
                "_display_is_available",
                return_value=True,
            ), patch.object(
                monitor,
                "_import_tk",
                side_effect=RuntimeError("Tk unavailable"),
            ), self.assertLogs(
                monitor.logger,
                level="WARNING",
            ) as captured:
                monitor.start(["detector"])

        self.assertTrue(monitor._headless)
        self.assertTrue(monitor._print_enabled)
        self.assertFalse(monitor.running)
        self.assertIsNone(monitor.root)
        self.assertEqual(output.getvalue(), "detector\n")
        self.assertTrue(
            any("could not start Tk GUI" in message for message in captured.output)
        )

    def test_update_before_start_logs_error_and_does_not_queue_point(self):
        monitor = QueuePlotterMonitor({"print": False})

        with self.assertLogs(monitor.logger, level="ERROR") as captured:
            monitor.update([1.0])

        self.assertEqual(len(monitor.pending), 0)
        self.assertTrue(
            any("called before start" in message for message in captured.output)
        )

    def test_update_headless_prints_row_without_queueing_point(self):
        output = io.StringIO()
        parameters = {"print": {"include_header": True, "format": "tsv"}}
        with patch("sys.stdout", output):
            monitor = QueuePlotterMonitor(parameters)
            with patch.object(monitor, "_display_is_available", return_value=False):
                monitor.start(["detector"])
            monitor.update([{"value": 1.25, "timestamp": 100.0}])

        self.assertEqual(output.getvalue(), "detector\n1.250000000000e+00\n")
        self.assertEqual(len(monitor.pending), 0)

    def _started_gui_monitor(self, parameters=None, signals=("detector", "missing")):
        monitor = QueuePlotterMonitor(parameters or {"print": False})
        tk = MagicMock()
        ttk = MagicMock()
        tk.Tk.return_value = MagicMock()
        with patch.object(
            monitor,
            "_display_is_available",
            return_value=True,
        ), patch.object(
            monitor,
            "_import_tk",
            return_value=(tk, ttk),
        ), patch.object(
            monitor,
            "_import_pyplot",
        ), patch.object(
            monitor,
            "_start_gui",
        ), patch(
            "kiwi_scan.monitor_concrete.queue_plotter.time.time",
            return_value=100.0,
        ):
            monitor.start(list(signals))
        return monitor

    def test_update_gui_only_appends_raw_row(self):
        monitor = self._started_gui_monitor()
        row = [{"value": 4.5}, None]

        with patch(
            "kiwi_scan.monitor_concrete.queue_plotter.time.time",
            return_value=102.5,
        ):
            monitor.update(row)

        self.assertEqual(len(monitor.pending), 1)
        timestamp, vals = monitor.pending[0]
        self.assertEqual(timestamp, 102.5)
        self.assertIs(vals, row)

    def test_drain_converts_elapsed_time_and_values(self):
        monitor = self._started_gui_monitor()
        with patch(
            "kiwi_scan.monitor_concrete.queue_plotter.time.time",
            return_value=102.5,
        ):
            monitor.update([{"value": 4.5}, None])

        self.assertTrue(monitor._drain_queue())

        data = monitor._ordered_data()
        self.assertEqual(data.shape, (3, 1))
        self.assertEqual(data[0, 0], 2.5)
        self.assertEqual(data[1, 0], 4.5)
        self.assertTrue(math.isnan(data[2, 0]))
        self.assertEqual(len(monitor.pending), 0)

    def test_drain_uses_nan_for_short_rows_and_non_numeric_values(self):
        monitor = self._started_gui_monitor(signals=("first", "second", "third"))
        with patch(
            "kiwi_scan.monitor_concrete.queue_plotter.time.time",
            return_value=101.0,
        ):
            monitor.update([1.0, "not-a-number"])

        monitor._drain_queue()

        data = monitor._ordered_data()
        self.assertEqual(data[0, 0], 1.0)
        self.assertEqual(data[1, 0], 1.0)
        self.assertTrue(math.isnan(data[2, 0]))
        self.assertTrue(math.isnan(data[3, 0]))

    def test_drain_returns_false_without_new_points(self):
        monitor = self._started_gui_monitor()

        self.assertFalse(monitor._drain_queue())

    def test_update_is_ignored_after_window_closed(self):
        monitor = self._started_gui_monitor()
        monitor.running = False  # what the WM_DELETE_WINDOW handler does

        monitor.update([1.0, 2.0])

        self.assertEqual(len(monitor.pending), 0)

    def test_pending_is_bounded_and_counts_dropped_points(self):
        monitor = self._started_gui_monitor({"print": False, "max_points": 3})

        for idx in range(5):
            monitor.update([float(idx), float(idx)])
        monitor._drain_queue()

        self.assertEqual(monitor._points_received, 5)
        self.assertEqual(monitor._points_drained, 3)
        np.testing.assert_array_equal(monitor._ordered_data()[1], [2.0, 3.0, 4.0])

    def test_ring_buffer_keeps_newest_points_in_order(self):
        monitor = self._started_gui_monitor({"print": False, "max_points": 4})

        for idx in range(6):
            monitor.update([float(idx), 0.0])
            monitor._drain_queue()

        np.testing.assert_array_equal(monitor._ordered_data()[1], [2.0, 3.0, 4.0, 5.0])

    def test_ordered_data_is_decimated_to_max_draw_points(self):
        monitor = self._started_gui_monitor({"print": False, "max_points": 100, "max_draw_points": 10})

        for idx in range(100):
            monitor.update([float(idx), 0.0])
        monitor._drain_queue()

        data = monitor._ordered_data()
        self.assertLessEqual(data.shape[1], 10)
        self.assertEqual(data[1, 0], 0.0)

    def test_set_signals_replaces_plot_and_updates_compatibility_variables(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor.x_signal = MagicMock()
        monitor.y_signal = MagicMock()

        monitor.set_signals("position", "detector")

        self.assertEqual(
            monitor.plot_specs,
            [PlotSpec(x="position", y=["detector"], title="detector")],
        )
        monitor.x_signal.set.assert_called_once_with("position")
        monitor.y_signal.set.assert_called_once_with("detector")

    def test_loop_returns_immediately_in_headless_mode(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor._headless = True

        with patch.object(
            monitor,
            "_import_pyplot",
        ) as import_pyplot, self.assertLogs(
            monitor.logger,
            level="INFO",
        ):
            monitor.loop()

        import_pyplot.assert_not_called()

    def test_loop_closes_figure_and_destroys_root(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor._headless = False
        monitor.running = False
        monitor.plot_specs = []
        root = MagicMock()
        monitor.root = root
        monitor._tk = MagicMock()

        plt = MagicMock()
        fig = MagicMock()
        ax = MagicMock()
        plt.subplots.return_value = (fig, ax)

        with patch.object(monitor, "_import_pyplot", return_value=plt):
            monitor.loop()

        plt.ion.assert_called_once_with()
        plt.ioff.assert_called_once_with()
        plt.close.assert_called_once_with(fig)
        root.destroy.assert_called_once_with()
        self.assertIsNone(monitor.root)

    def test_close_stops_monitor_and_closes_row_formatter(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor.running = True
        monitor._row_formatter.close = MagicMock()

        monitor.close()

        self.assertFalse(monitor.running)
        monitor._row_formatter.close.assert_called_once_with()


class TestQueuePlotterDrawing(unittest.TestCase):
    @staticmethod
    def _monitor_with_lines(spec):
        monitor = QueuePlotterMonitor({"print": False})
        monitor.signal_names = ["t", "detector1", "detector2"]
        monitor.plot_specs = [spec]
        ax = MagicMock()
        ax.plot.side_effect = lambda *args, **kwargs: [MagicMock(name=kwargs["label"])]
        fig = MagicMock()
        monitor._setup_plots([ax], fig)
        return monitor, ax, fig

    def test_setup_plots_creates_lines_labels_and_layout_once(self):
        spec = PlotSpec(
            x="t",
            y=["detector1", "detector2"],
            title="Detectors",
            xlabel="Time",
            ylabel="Signal",
        )
        monitor, ax, fig = self._monitor_with_lines(spec)

        self.assertEqual(ax.plot.call_count, 2)
        ax.plot.assert_any_call([], [], marker=None, linestyle="-", label="detector1")
        ax.plot.assert_any_call([], [], marker=None, linestyle="-", label="detector2")
        ax.set_xlabel.assert_called_once_with("Time")
        ax.set_ylabel.assert_called_once_with("Signal")
        ax.set_title.assert_called_once_with("Detectors")
        ax.legend.assert_called_once_with(loc="upper left")
        ax.grid.assert_called_once_with(True)
        fig.tight_layout.assert_called_once_with()
        fig.canvas.draw_idle.assert_called_once_with()
        self.assertEqual([(x, y) for _line, x, y in monitor._lines[0][1]], [(0, 1), (0, 2)])

    def test_setup_plots_single_y_has_no_legend(self):
        _monitor, ax, _fig = self._monitor_with_lines(PlotSpec(x="t", y=["detector1"], title="Detector"))

        ax.legend.assert_not_called()

    def test_draw_plots_updates_line_data_without_clearing(self):
        spec = PlotSpec(x="t", y=["detector1", "detector2"], title="Detectors")
        monitor, ax, fig = self._monitor_with_lines(spec)
        monitor._start_time = 0.0
        monitor.running = True
        with patch(
            "kiwi_scan.monitor_concrete.queue_plotter.time.time",
            side_effect=[1.0, 2.0],
        ):
            monitor.update([10.0, 20.0])
            monitor.update([11.0])
        monitor._drain_queue()
        fig.reset_mock()
        ax.reset_mock()

        monitor._draw_plots([ax], fig)

        ax.clear.assert_not_called()
        ax.plot.assert_not_called()
        fig.tight_layout.assert_not_called()
        line1, _x, _y = monitor._lines[0][1][0]
        line2, _x, _y = monitor._lines[0][1][1]
        x_values, y_values = line1.set_data.call_args.args
        np.testing.assert_array_equal(x_values, [1.0, 2.0])
        np.testing.assert_array_equal(y_values, [10.0, 11.0])
        _x_values, y2_values = line2.set_data.call_args.args
        self.assertEqual(y2_values[0], 20.0)
        self.assertTrue(math.isnan(y2_values[1]))
        ax.relim.assert_called_once_with()
        ax.autoscale_view.assert_called_once_with()
        fig.canvas.draw_idle.assert_called_once_with()

    def test_run_plot_loop_redraws_only_when_new_points_arrive(self):
        monitor = QueuePlotterMonitor({"print": False, "refresh_interval": 0.2})
        monitor.signal_names = ["t", "detector1"]
        monitor.plot_specs = [PlotSpec(x="t", y=["detector1"], title="Detector")]
        monitor._start_time = 0.0
        monitor.running = True
        plt = MagicMock()

        def stop_after_three(_interval):
            if plt.pause.call_count >= 3:
                monitor.running = False

        plt.pause.side_effect = stop_after_three
        with patch.object(monitor, "_setup_plots") as setup, patch.object(
            monitor, "_drain_queue", side_effect=[False, True, False]
        ), patch.object(monitor, "_update_tk"), patch.object(monitor, "_draw_plots") as draw:
            monitor._run_plot_loop(plt, MagicMock(), [MagicMock()])

        setup.assert_called_once()
        draw.assert_called_once()
        plt.pause.assert_called_with(0.2)
        self.assertEqual(plt.pause.call_count, 3)

    def test_run_plot_loop_warns_about_dropped_points(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor.running = False
        monitor._points_received = 5
        monitor._points_drained = 3

        with self.assertLogs(monitor.logger, level="WARNING") as captured:
            monitor._run_plot_loop(MagicMock(), MagicMock(), [MagicMock()])

        self.assertTrue(any("dropped 2 of 5 points" in message for message in captured.output))

if __name__ == "__main__":
    unittest.main(verbosity=2)
