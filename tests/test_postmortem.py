# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from kiwi_scan.postmortem import (
    PostMortemConfig,
    SeriesSpec,
    _load_scan_dataframe,
    plot_postmortem,
)


class TestPostMortemConfiguration(unittest.TestCase):
    def test_series_spec_defaults(self):
        spec = SeriesSpec(file="scan.txt", column="signal")

        self.assertEqual(spec.axis, 0)
        self.assertIsNone(spec.label)
        self.assertEqual(spec.source_type, "scan")

    def test_postmortem_config_default_tolerance(self):
        config = PostMortemConfig(x_column="position", series=[])

        self.assertEqual(config.join_tolerance, 0.1)


class TestLoadScanDataframe(unittest.TestCase):
    @patch("kiwi_scan.postmortem.DataLoader")
    def test_load_scan_dataframe_uses_data_loader(self, loader_class):
        expected = pd.DataFrame({"position": [1.0]})
        loader_class.return_value.load_data.return_value = expected

        result = _load_scan_dataframe("scan.txt")

        loader_class.assert_called_once_with("scan.txt")
        loader_class.return_value.load_data.assert_called_once_with()
        self.assertIs(result, expected)


class TestPlotPostmortem(unittest.TestCase):
    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem._load_scan_dataframe")
    def test_scan_series_is_added_to_requested_axis(self, load_scan, plotter_class):
        dataframe = pd.DataFrame(
            {"position": [0.0, 1.0], "signal": [2.0, 3.0]}
        )
        load_scan.return_value = dataframe
        plotter = plotter_class.return_value
        config = PostMortemConfig(
            x_column="position",
            series=[
                SeriesSpec(
                    file="scan.txt",
                    column="signal",
                    axis=2,
                    label="custom signal",
                )
            ],
        )

        plot_postmortem(config)

        plotter_class.assert_called_once_with(title="Post Mortem Plot")
        load_scan.assert_called_once_with("scan.txt")
        call = plotter.add_series.call_args
        pd.testing.assert_series_equal(call.kwargs["x"], dataframe["position"])
        pd.testing.assert_series_equal(call.kwargs["y"], dataframe["signal"])
        self.assertEqual(call.kwargs["label"], "custom signal")
        self.assertEqual(call.kwargs["axis"], 2)
        plotter.plot.assert_called_once_with(subplot=False, multi_axis=True)

    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem._load_scan_dataframe", return_value=None)
    def test_missing_scan_file_is_skipped(self, _load_scan, plotter_class):
        config = PostMortemConfig(
            x_column="position",
            series=[SeriesSpec(file="missing.txt", column="signal")],
        )

        plot_postmortem(config)

        plotter = plotter_class.return_value
        plotter.add_series.assert_not_called()
        plotter.plot.assert_called_once_with(subplot=False, multi_axis=True)

    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem._load_scan_dataframe")
    def test_scan_series_with_missing_column_is_skipped(
        self,
        load_scan,
        plotter_class,
    ):
        load_scan.return_value = pd.DataFrame({"position": [0.0]})
        config = PostMortemConfig(
            x_column="position",
            series=[SeriesSpec(file="scan.txt", column="missing")],
        )

        with self.assertLogs("kiwi_scan.postmortem", level="WARNING") as logs:
            plot_postmortem(config)

        plotter_class.return_value.add_series.assert_not_called()
        self.assertIn("Columns 'position' or 'missing' not found", logs.output[0])

    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem.parse_metadata_file")
    def test_metadata_series_uses_dataframe_index(self, parse_metadata, plotter_class):
        index = pd.Index(
            pd.to_datetime(["2026-08-25T10:00:00Z", "2026-08-25T10:00:01Z"]),
            name="TS-ISO8601",
        )
        dataframe = pd.DataFrame({"TEST:PV": [1.0, 2.0]}, index=index)
        parse_metadata.return_value = SimpleNamespace(df_pivot=dataframe)
        plotter = plotter_class.return_value
        config = PostMortemConfig(
            x_column="TS-ISO8601",
            series=[
                SeriesSpec(
                    file="metadata.txt",
                    column="TEST:PV",
                    source_type="meta",
                    axis=1,
                )
            ],
        )

        plot_postmortem(config)

        parse_metadata.assert_called_once_with("metadata.txt")
        call = plotter.add_series.call_args
        self.assertIs(call.kwargs["x"], dataframe.index)
        pd.testing.assert_series_equal(call.kwargs["y"], dataframe["TEST:PV"])
        self.assertEqual(call.kwargs["label"], "TEST:PV")
        self.assertEqual(call.kwargs["axis"], 1)
        plotter.plot.assert_called_once_with(subplot=False, multi_axis=True)

    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem.parse_metadata_file", return_value=None)
    def test_missing_metadata_file_is_skipped(self, _parse_metadata, plotter_class):
        config = PostMortemConfig(
            x_column="TS-ISO8601",
            series=[
                SeriesSpec(
                    file="missing.txt",
                    column="TEST:PV",
                    source_type="meta",
                )
            ],
        )

        plot_postmortem(config)

        plotter_class.return_value.add_series.assert_not_called()

    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem.parse_metadata_file")
    def test_metadata_series_requires_x_column_as_index(
        self,
        parse_metadata,
        plotter_class,
    ):
        dataframe = pd.DataFrame(
            {"TEST:PV": [1.0]},
            index=pd.Index([0], name="other"),
        )
        parse_metadata.return_value = SimpleNamespace(df_pivot=dataframe)
        config = PostMortemConfig(
            x_column="TS-ISO8601",
            series=[
                SeriesSpec(
                    file="metadata.txt",
                    column="TEST:PV",
                    source_type="meta",
                )
            ],
        )

        with self.assertRaisesRegex(
            ValueError,
            "x_column 'TS-ISO8601' must be the index",
        ):
            plot_postmortem(config)

        plotter_class.return_value.add_series.assert_not_called()
        plotter_class.return_value.plot.assert_not_called()

    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem.parse_metadata_file")
    def test_metadata_series_with_missing_pv_is_skipped(
        self,
        parse_metadata,
        plotter_class,
    ):
        dataframe = pd.DataFrame(
            {"OTHER:PV": [1.0]},
            index=pd.Index([0], name="TS-ISO8601"),
        )
        parse_metadata.return_value = SimpleNamespace(df_pivot=dataframe)
        config = PostMortemConfig(
            x_column="TS-ISO8601",
            series=[
                SeriesSpec(
                    file="metadata.txt",
                    column="TEST:PV",
                    source_type="meta",
                )
            ],
        )

        with self.assertLogs("kiwi_scan.postmortem", level="WARNING") as logs:
            plot_postmortem(config)

        plotter_class.return_value.add_series.assert_not_called()
        self.assertIn("PV 'TEST:PV' not found", logs.output[0])

    @patch("kiwi_scan.postmortem.Plotter")
    @patch("kiwi_scan.postmortem._load_scan_dataframe")
    def test_processes_multiple_scan_series(self, load_scan, plotter_class):
        first = pd.DataFrame({"position": [0], "first": [1]})
        second = pd.DataFrame({"position": [0], "second": [2]})
        load_scan.side_effect = [first, second]
        config = PostMortemConfig(
            x_column="position",
            series=[
                SeriesSpec(file="first.txt", column="first"),
                SeriesSpec(file="second.txt", column="second"),
            ],
        )

        plot_postmortem(config)

        self.assertEqual(plotter_class.return_value.add_series.call_count, 2)
        self.assertEqual(
            [call.kwargs["label"] for call in plotter_class.return_value.add_series.call_args_list],
            ["first", "second"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
