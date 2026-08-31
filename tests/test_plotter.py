# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from kiwi_scan.plotter import PlotData, Plotter, plot_scan_data


class TestPlotData(unittest.TestCase):
    def test_constructor_converts_values_to_numpy_arrays(self):
        plot_data = PlotData([0, 1], [2, 3], label="signal", axis=2)

        np.testing.assert_array_equal(plot_data.x, np.array([0, 1]))
        np.testing.assert_array_equal(plot_data.y, np.array([2, 3]))
        self.assertEqual(plot_data.label, "signal")
        self.assertEqual(plot_data.axis, 2)

    def test_derivative_returns_new_plot_data(self):
        plot_data = PlotData([0.0, 1.0, 2.0], [0.0, 1.0, 4.0], label="y")

        result = plot_data.derivative()

        self.assertIsInstance(result, PlotData)
        np.testing.assert_array_equal(result.x, plot_data.x)
        np.testing.assert_allclose(result.y, [1.0, 2.0, 3.0])
        self.assertEqual(result.label, "Derivative(y)")

    def test_multiply_combines_y_values_and_labels(self):
        left = PlotData([0, 1], [2, 3], label="left")
        right = PlotData([0, 1], [4, 5], label="right")

        result = left.multiply(right)

        np.testing.assert_array_equal(result.x, [0, 1])
        np.testing.assert_array_equal(result.y, [8, 15])
        self.assertEqual(result.label, "(left)*(right)")

    def test_multiply_rejects_different_x_values(self):
        left = PlotData([0, 1], [2, 3])
        right = PlotData([0, 2], [4, 5])

        with self.assertRaisesRegex(AssertionError, "X data must match"):
            left.multiply(right)


class TestPlotterDataManagement(unittest.TestCase):
    def test_constructor_and_add_methods(self):
        plotter = Plotter(
            title="Test",
            xlabel="position",
            ylabel="value",
            title_fontsize=14,
            label_fontsize=11,
        )
        existing = PlotData([0], [1], label="existing")

        plotter.add_series([0, 1], [2, 3], label="added", axis=1)
        plotter.add_plot(existing)

        self.assertEqual(plotter.title, "Test")
        self.assertEqual(plotter.xlabel, "position")
        self.assertEqual(plotter.ylabel, "value")
        self.assertEqual(plotter.title_fontsize, 14)
        self.assertEqual(plotter.label_fontsize, 11)
        self.assertEqual(len(plotter.plots), 2)
        self.assertEqual(plotter.plots[0].label, "added")
        self.assertEqual(plotter.plots[0].axis, 1)
        self.assertIs(plotter.plots[1], existing)

    def test_add_dataframe_columns_uses_custom_and_default_labels(self):
        data = pd.DataFrame(
            {
                "position": [0.0, 1.0],
                "first": [2.0, 3.0],
                "second": [4.0, 5.0],
            }
        )
        plotter = Plotter()

        plotter.add_dataframe_columns_as_plotdata(
            data,
            ["first", "second"],
            "position",
            labels=["custom"],
        )

        self.assertEqual([plot.label for plot in plotter.plots], ["custom", "second"])
        np.testing.assert_array_equal(plotter.plots[0].x, [0.0, 1.0])
        np.testing.assert_array_equal(plotter.plots[1].y, [4.0, 5.0])

    def test_add_dataframe_columns_skips_unknown_y_column(self):
        data = pd.DataFrame({"position": [0], "signal": [1]})
        plotter = Plotter()

        with self.assertLogs("kiwi_scan.plotter", level="WARNING") as logs:
            plotter.add_dataframe_columns_as_plotdata(
                data,
                ["missing", "signal"],
                "position",
            )

        self.assertEqual(len(plotter.plots), 1)
        self.assertIn("Column 'missing' not found", logs.output[0])

    def test_add_dataframe_columns_rejects_invalid_input(self):
        plotter = Plotter()

        with self.assertLogs("kiwi_scan.plotter", level="ERROR") as logs:
            plotter.add_dataframe_columns_as_plotdata(None, ["signal"], "position")

        self.assertEqual(plotter.plots, [])
        self.assertIn("Invalid DataFrame or position column", logs.output[0])


class TestPlotterExport(unittest.TestCase):
    def test_export_space_delimited_writes_header_and_data(self):
        plotter = Plotter(xlabel="position")
        plotter.add_series([0.0, 1.0], [2.0, 3.0], label="first")
        plotter.add_series([0.0, 1.0], [4.0, 5.0], label="second")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "combined.txt"
            result = plotter.export_space_delimited(output, float_fmt="%.1f")
            content = output.read_text(encoding="utf-8").splitlines()

        self.assertEqual(result, output)
        self.assertEqual(content[0], "# position first second")
        self.assertEqual(content[1:], ["0.0 2.0 4.0", "1.0 3.0 5.0"])

    def test_export_space_delimited_can_omit_x(self):
        plotter = Plotter()
        plotter.add_series([10, 20], [1, 2])

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "without_x.txt"
            plotter.export_space_delimited(output, include_x=False, float_fmt="%.0f")
            content = output.read_text(encoding="utf-8").splitlines()

        self.assertEqual(content, ["# series", "1", "2"])

    def test_export_space_delimited_requires_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "empty.txt"
            with self.assertRaisesRegex(ValueError, "No data to export"):
                Plotter().export_space_delimited(output)

    def test_export_space_delimited_rejects_different_x_values(self):
        plotter = Plotter()
        plotter.add_series([0, 1], [2, 3], label="first")
        plotter.add_series([0, 2], [4, 5], label="second")

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "combined.txt"
            with self.assertRaisesRegex(ValueError, "Series 'second'.*does not match"):
                plotter.export_space_delimited(output)

    def test_export_each_series_creates_safe_filenames(self):
        plotter = Plotter(xlabel="position")
        plotter.add_series([0, 1], [2, 3], label="first signal")
        plotter.add_series([10, 20], [4, 5])

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "nested" / "exports"
            paths = plotter.export_each_series(
                output_dir,
                basename="scan",
                float_fmt="%.0f",
            )
            first_content = paths[0].read_text(encoding="utf-8").splitlines()
            second_content = paths[1].read_text(encoding="utf-8").splitlines()

        self.assertEqual(
            [path.name for path in paths],
            ["scan_0_first_signal.txt", "scan_1_s1.txt"],
        )
        self.assertEqual(first_content, ["# position first signal", "0 2", "1 3"])
        self.assertEqual(second_content, ["# position series", "10 4", "20 5"])

    def test_export_each_series_can_omit_x(self):
        plotter = Plotter()
        plotter.add_series([10, 20], [1, 2], label="signal")

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = plotter.export_each_series(
                tmpdir,
                include_x=False,
                float_fmt="%.0f",
            )
            content = paths[0].read_text(encoding="utf-8").splitlines()

        self.assertEqual(content, ["# signal", "1", "2"])


class TestPlotterRendering(unittest.TestCase):
    def test_plot_without_data_logs_warning(self):
        with self.assertLogs("kiwi_scan.plotter", level="WARNING") as logs:
            Plotter().plot()

        self.assertIn("No data available for plotting", logs.output[0])

    @patch("kiwi_scan.plotter.plt.show")
    @patch("kiwi_scan.plotter.plt.tight_layout")
    @patch("kiwi_scan.plotter.plt.grid")
    @patch("kiwi_scan.plotter.plt.legend")
    @patch("kiwi_scan.plotter.plt.title")
    @patch("kiwi_scan.plotter.plt.ylabel")
    @patch("kiwi_scan.plotter.plt.xlabel")
    @patch("kiwi_scan.plotter.plt.plot")
    @patch("kiwi_scan.plotter.plt.figure")
    def test_plot_combined_mode(
        self,
        figure,
        plot,
        xlabel,
        ylabel,
        title,
        legend,
        grid,
        tight_layout,
        show,
    ):
        plotter = Plotter(title="Combined", xlabel="position", ylabel="value")
        plotter.add_series([0, 1], [2, 3], label="signal")

        plotter.plot()

        figure.assert_called_once_with(figsize=(8, 5))
        plot.assert_called_once()
        xlabel.assert_called_once_with("position", fontsize=10)
        ylabel.assert_called_once_with("value", fontsize=10)
        title.assert_called_once_with("Combined", fontsize=12)
        legend.assert_called_once_with()
        grid.assert_called_once_with(True)
        tight_layout.assert_called_once_with()
        show.assert_called_once_with()

    @patch("kiwi_scan.plotter.plt.show")
    @patch("kiwi_scan.plotter.plt.tight_layout")
    @patch("kiwi_scan.plotter.plt.subplots")
    def test_plot_subplot_mode(self, subplots, tight_layout, show):
        axis = MagicMock()
        axes = np.empty((1, 1), dtype=object)
        axes[0, 0] = axis
        subplots.return_value = (MagicMock(), axes)
        plotter = Plotter(xlabel="position")
        plotter.add_series([0], [1], label="first")

        plotter.plot(subplot=True)

        subplots.assert_called_once_with(
            1,
            1,
            figsize=(8, 3),
            squeeze=False,
        )
        axis.plot.assert_called_once()
        axis.set_xlabel.assert_called_once_with("position", fontsize=10)
        axis.set_ylabel.assert_called_once_with("first", fontsize=10)
        axis.set_title.assert_called_once_with("first", fontsize=12)
        axis.legend.assert_called_once_with()
        axis.grid.assert_called_once_with(True)
        tight_layout.assert_called_once_with()
        show.assert_called_once_with()

    @patch("kiwi_scan.plotter.plt.show")
    @patch("kiwi_scan.plotter.plt.tight_layout")
    @patch("kiwi_scan.plotter.plt.subplots")
    def test_plot_multi_axis_mode(self, subplots, tight_layout, show):
        figure = MagicMock()
        first_axis = MagicMock()
        second_axis = MagicMock()
        first_axis.twinx.return_value = second_axis
        subplots.return_value = (figure, first_axis)
        plotter = Plotter(title="Multi")
        plotter.add_series([0, 1], [2, 3], label="first", axis=0)
        plotter.add_series([0, 1], [4, 5], label="second", axis=1)

        plotter.plot(multi_axis=True)

        subplots.assert_called_once_with(figsize=(8, 5))
        first_axis.twinx.assert_called_once_with()
        first_axis.plot.assert_called_once()
        second_axis.plot.assert_called_once()
        first_axis.set_xlabel.assert_called_once_with("X")
        second_axis.set_xlabel.assert_called_once_with("X")
        figure.suptitle.assert_called_once_with("Multi")
        tight_layout.assert_called_once_with()
        show.assert_called_once_with()


class TestPlotScanData(unittest.TestCase):
    @patch("kiwi_scan.plotter.Plotter")
    def test_plot_scan_data_exports_combined_file(self, plotter_class):
        plotter = plotter_class.return_value
        data = pd.DataFrame({"position": [0], "signal": [1]})

        plot_scan_data(data, ["signal"], "position", "unused", "output.txt")

        plotter.add_dataframe_columns_as_plotdata.assert_called_once_with(
            data,
            ["signal"],
            "position",
        )
        plotter.plot.assert_called_once_with(subplot=True)
        plotter.export_space_delimited.assert_called_once_with(
            Path("output.txt"),
            include_x=True,
        )
        plotter.export_each_series.assert_not_called()

    @patch("kiwi_scan.plotter.Plotter")
    def test_plot_scan_data_exports_directory(self, plotter_class):
        plotter = plotter_class.return_value
        data = pd.DataFrame({"position": [0], "signal": [1]})

        plot_scan_data(data, ["signal"], "position", "unused", "exports")

        plotter.export_each_series.assert_called_once_with(
            Path("exports"),
            include_x=True,
        )

    @patch("kiwi_scan.plotter.Plotter")
    def test_plot_scan_data_falls_back_to_individual_files(self, plotter_class):
        plotter = plotter_class.return_value
        plotter.export_space_delimited.side_effect = ValueError("different X")
        data = pd.DataFrame({"position": [0], "signal": [1]})

        with self.assertLogs("kiwi_scan.plotter", level="WARNING") as logs:
            plot_scan_data(
                data,
                ["signal"],
                "position",
                "unused",
                "exports/combined.txt",
            )

        plotter.export_each_series.assert_called_once_with(
            Path("exports"),
            include_x=True,
        )
        self.assertIn("Falling back to per-series export", logs.output[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
