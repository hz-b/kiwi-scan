# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import io
import math
import queue
import unittest
from unittest.mock import MagicMock, patch

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
        self.assertEqual(
            monitor.data,
            {"t": [], "detector1": [], "detector2": []},
        )
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
            "_start_gui",
        ) as start_gui:
            monitor.start(["detector"])

        self.assertIs(monitor.root, root)
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

        self.assertTrue(monitor.queue.empty())
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
        self.assertTrue(monitor.queue.empty())

    def test_update_gui_queues_elapsed_time_and_values(self):
        monitor = QueuePlotterMonitor({"print": False})
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
            "_start_gui",
        ), patch(
            "kiwi_scan.monitor_concrete.queue_plotter.time.time",
            side_effect=[100.0, 102.5],
        ):
            monitor.start(["detector", "missing"])
            monitor.update([{"value": 4.5}, None])

        point = monitor.queue.get_nowait()
        self.assertEqual(point["t"], 2.5)
        self.assertEqual(point["detector"], 4.5)
        self.assertTrue(math.isnan(point["missing"]))

    def test_update_uses_nan_for_value_missing_from_short_row(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor._start_time = 10.0
        monitor.signal_names = ["t", "first", "second"]

        with patch(
            "kiwi_scan.monitor_concrete.queue_plotter.time.time",
            return_value=11.0,
        ):
            monitor.update([1.0])

        point = monitor.queue.get_nowait()
        self.assertEqual(point["t"], 1.0)
        self.assertEqual(point["first"], 1.0)
        self.assertTrue(math.isnan(point["second"]))

    def test_drain_queue_appends_all_points(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor.signal_names = ["t", "detector"]
        monitor.data = {"t": [], "detector": []}
        monitor.queue.put({"t": 0.1, "detector": 1.0})
        monitor.queue.put({"t": 0.2, "detector": 2.0})

        monitor._drain_queue()

        self.assertEqual(monitor.data["t"], [0.1, 0.2])
        self.assertEqual(monitor.data["detector"], [1.0, 2.0])
        with self.assertRaises(queue.Empty):
            monitor.queue.get_nowait()

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
    def test_draw_plots_draws_each_y_channel_with_shortest_length(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor.plot_specs = [
            PlotSpec(
                x="t",
                y=["detector1", "detector2"],
                title="Detectors",
                xlabel="Time",
                ylabel="Signal",
            )
        ]
        monitor.data = {
            "t": [0.0, 1.0, 2.0],
            "detector1": [10.0, 11.0],
            "detector2": [20.0, 21.0, 22.0],
        }
        ax = MagicMock()
        fig = MagicMock()

        monitor._draw_plots([ax], fig)

        ax.clear.assert_called_once_with()
        self.assertEqual(ax.plot.call_count, 2)
        ax.plot.assert_any_call(
            [0.0, 1.0],
            [10.0, 11.0],
            marker="o",
            linestyle="-",
            label="detector1",
        )
        ax.plot.assert_any_call(
            [0.0, 1.0, 2.0],
            [20.0, 21.0, 22.0],
            marker="o",
            linestyle="-",
            label="detector2",
        )
        ax.set_xlabel.assert_called_once_with("Time")
        ax.set_ylabel.assert_called_once_with("Signal")
        ax.set_title.assert_called_once_with("Detectors")
        ax.legend.assert_called_once_with(loc="best")
        ax.grid.assert_called_once_with(True)
        fig.tight_layout.assert_called_once_with()
        fig.canvas.draw_idle.assert_called_once_with()

    def test_draw_plots_with_no_x_data_sets_title_only(self):
        monitor = QueuePlotterMonitor({"print": False})
        monitor.plot_specs = [
            PlotSpec(x="t", y=["detector"], title="Detector")
        ]
        monitor.data = {"t": [], "detector": []}
        ax = MagicMock()
        fig = MagicMock()

        monitor._draw_plots([ax], fig)

        ax.clear.assert_called_once_with()
        ax.set_title.assert_called_once_with("Detector")
        ax.plot.assert_not_called()
        ax.set_xlabel.assert_not_called()
        fig.tight_layout.assert_called_once_with()
        fig.canvas.draw_idle.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
