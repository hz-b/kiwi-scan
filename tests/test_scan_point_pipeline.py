# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import tempfile
import threading
import unittest
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, ClassVar, Dict, List
from unittest.mock import MagicMock, patch

from kiwi_scan.scan._point_frame import _DetectorLayout
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.output_manager import OutputManager
from kiwi_scan.scan.performance_tracker import PerformanceTracker


class _PipelineScan(BaseScan):
    """BaseScan test double without hardware initialization."""

    def __init__(
        self,
        *,
        include_timestamps: bool = True,
        timestamp_output_format: str = "iso8601",
    ) -> None:
        self.detector_pvs = [SimpleNamespace(pvname="DET")]
        self.plugins: List[Any] = []
        self.include_timestamps = include_timestamps
        self.timestamp_output_format = timestamp_output_format
        self.performance = PerformanceTracker(enabled=False)
        self.output_manager = OutputManager(
            data_dir=".",
            requested_output_file="scan.txt",
            data_writing_enabled=False,
            output_timestamp="20260901120000",
        )
        self._initialize_point_pipeline()
        self.busyflag = False
        self._meta_mon_started = False
        self._meta_mon = MagicMock()

    def execute(self) -> None:
        pass


class _Plugin:
    def __init__(
        self,
        scan: _PipelineScan,
        header: str,
        timestamp: float,
        calculate: Callable[[Dict[str, Any]], float],
    ) -> None:
        self.scan = scan
        self.header = header
        self.timestamp = timestamp
        self.calculate = calculate
        self.seen_rows: List[Dict[str, Any]] = []
        self.seen_completed_values: List[Any] = []

    def get_headers(self, timestamps: bool) -> List[str]:
        if timestamps:
            return [self.header, "TS-" + self.header]
        return [self.header]

    def on_scan_point(self, _index: int, _position: Any) -> List[Dict[str, Any]]:
        row = self.scan.get_current_row_cache()
        self.seen_rows.append(row)
        self.seen_completed_values.append(self.scan.get_value("DET"))
        return [
            {
                "value": self.calculate(row),
                "timestamp": self.timestamp,
            }
        ]


class _IsoValue:
    def __init__(self, value: str) -> None:
        self.value = value

    def astimezone(self) -> _IsoValue:
        return self

    def isoformat(self) -> str:
        return self.value


class _Clock:
    """Clock replacement covering the datetime operations used by BaseScan."""

    TIMESTAMPS: ClassVar[Dict[float, str]] = {
        100.25: "line-time",
        101.25: "detector-time",
        102.25: "plugin-one-time",
        103.25: "plugin-two-time",
    }
    converted: ClassVar[List[float]] = []

    @classmethod
    def reset(cls) -> None:
        cls.converted = []

    @classmethod
    def now(cls) -> _IsoValue:
        return _IsoValue("line-time")

    @classmethod
    def fromtimestamp(cls, timestamp: float) -> _IsoValue:
        value = float(timestamp)
        cls.converted.append(value)
        return _IsoValue(cls.TIMESTAMPS[value])


class _CountingReading(dict):
    def __init__(self, value: Any, timestamp: float) -> None:
        super().__init__(value=value, timestamp=timestamp)
        self.get_calls: Dict[str, int] = defaultdict(int)

    def get(self, key: str, default: Any = None) -> Any:
        self.get_calls[key] += 1
        return super().get(key, default)


def _process_point(
    scan: _PipelineScan,
    index: int,
    position: float,
    detector_values: List[Any],
) -> List[Any]:
    scan._begin_point_frame(
        idx=index,
        pos=position,
        values=detector_values,
    )
    plugin_values = scan._collect_plugin_point_data(index, position)
    return scan.save_to_file(
        position,
        detector_values + plugin_values,
        scan.include_timestamps,
    )


class TestScanPointPipeline(unittest.TestCase):
    def setUp(self) -> None:
        _Clock.reset()

    def test_plugins_see_incremental_point_but_get_value_stays_completed(self) -> None:
        scan = _PipelineScan()
        scan._point_pipeline.replace_last_point({"DET": {"value": -1.0}})
        plugin_one = _Plugin(
            scan,
            "P1",
            102.25,
            lambda row: row["DET"] + 1.0,
        )
        plugin_two = _Plugin(
            scan,
            "P2",
            103.25,
            lambda row: row["DET"] + row["P1"],
        )
        scan.plugins = [plugin_one, plugin_two]

        _process_point(
            scan,
            index=4,
            position=5.0,
            detector_values=[{"value": 10.0, "timestamp": 101.25}],
        )

        self.assertEqual(plugin_one.seen_rows[0]["DET"], 10.0)
        self.assertNotIn("P1", plugin_one.seen_rows[0])
        self.assertEqual(plugin_two.seen_rows[0]["DET"], 10.0)
        self.assertEqual(plugin_two.seen_rows[0]["P1"], 11.0)
        self.assertEqual(plugin_one.seen_completed_values, [-1.0])
        self.assertEqual(plugin_two.seen_completed_values, [-1.0])
        self.assertEqual(scan.get_value("DET"), 10.0)
        self.assertEqual(scan.get_value("P1"), 11.0)
        self.assertEqual(scan.get_value("P2"), 21.0)

    def test_each_metadata_item_is_normalized_once(self) -> None:
        scan = _PipelineScan()
        reading = _CountingReading(10.0, 101.25)

        _process_point(scan, 0, 5.0, [reading])

        self.assertEqual(reading.get_calls["value"], 1)
        self.assertEqual(reading.get_calls["timestamp"], 1)

    def test_scan_thread_keeps_timestamps_raw(self) -> None:
        scan = _PipelineScan()
        scan.plugins = [
            _Plugin(scan, "P1", 102.25, lambda row: row["DET"] + 1.0),
            _Plugin(scan, "P2", 103.25, lambda row: row["DET"] + row["P1"]),
        ]

        with patch("kiwi_scan.scan.point_pipeline.time.time", return_value=100.25):
            row = _process_point(
                scan,
                index=0,
                position=5.0,
                detector_values=[{"value": 10.0, "timestamp": 101.25}],
            )

        self.assertEqual(
            row,
            [
                5.0,
                100.25,
                10.0,
                101.25,
                11.0,
                21.0,
            ],
        )
        self.assertEqual(scan.get_value("TS"), 100.25)
        self.assertEqual(scan.get_value("TS-DET"), 101.25)
        self.assertEqual(scan.get_value("TS-P1"), 102.25)
        self.assertEqual(_Clock.converted, [])

    def test_equal_timestamps_remain_raw_without_conversion(self) -> None:
        scan = _PipelineScan()
        scan.detector_pvs = [
            SimpleNamespace(pvname="DET:A"),
            SimpleNamespace(pvname="DET:B"),
        ]
        scan._point_pipeline.set_detector_layout(
            _DetectorLayout.from_headers(
                pv.pvname for pv in scan.detector_pvs
            )
        )

        with patch("kiwi_scan.scan.point_pipeline.time.time", return_value=100.25):
            row = _process_point(
                scan,
                index=0,
                position=5.0,
                detector_values=[
                    {"value": 10.0, "timestamp": 101.25},
                    {"value": 20.0, "timestamp": 101.25},
                ],
            )

        self.assertEqual(
            row,
            [5.0, 100.25, 10.0, 101.25, 20.0, 101.25],
        )
        self.assertEqual(_Clock.converted, [])

    def test_async_snapshot_retains_point_n_after_point_n_plus_one_starts(self) -> None:
        scan = _PipelineScan(include_timestamps=False)
        captured_snapshots: List[Dict[str, Any]] = []
        observed_snapshots: List[Dict[str, Any]] = []
        snapshot_ready = threading.Event()
        inspect_snapshot = threading.Event()

        def inspect_in_background(snapshot: Dict[str, Any]) -> None:
            snapshot_ready.set()
            inspect_snapshot.wait(timeout=1.0)
            observed_snapshots.append(snapshot)

        class SnapshotPlugin:
            @staticmethod
            def get_headers(timestamps: bool) -> List[str]:
                del timestamps
                return ["ASYNC"]

            @staticmethod
            def on_scan_point(_index: int, _position: Any) -> List[Dict[str, Any]]:
                snapshot = scan.get_current_row_cache()
                captured_snapshots.append(snapshot)
                worker = threading.Thread(
                    target=inspect_in_background,
                    args=(snapshot,),
                )
                worker.start()
                snapshot_workers.append(worker)
                return [{"value": len(captured_snapshots), "timestamp": 102.25}]

        scan.plugins = [SnapshotPlugin()]
        snapshot_workers: List[threading.Thread] = []
        _process_point(scan, 0, 1.0, [{"value": 10.0, "timestamp": 101.25}])
        self.assertTrue(snapshot_ready.wait(timeout=1.0))
        scan._begin_point_frame(
            idx=1,
            pos=2.0,
            values=[{"value": 20.0, "timestamp": 101.25}],
        )
        inspect_snapshot.set()

        for worker in snapshot_workers:
            worker.join(timeout=1.0)

        self.assertEqual(observed_snapshots[0]["idx"], 0)
        self.assertEqual(observed_snapshots[0]["DET"], 10.0)
        self.assertEqual(scan.get_current_row_value("idx"), 1)
        self.assertEqual(scan.get_current_row_value("DET"), 20.0)

    def test_completed_frame_rejects_late_mutation(self) -> None:
        scan = _PipelineScan(include_timestamps=False)
        detector_values = [{"value": 10.0, "timestamp": 101.25}]
        scan._begin_point_frame(idx=0, pos=1.0, values=detector_values)
        frame = scan._point_pipeline.active_point_frame

        scan.save_to_file(1.0, detector_values, include_timestamps=False)

        with self.assertRaisesRegex(RuntimeError, "completed point frame"):
            frame.append_values(
                ["late"],
                [{"value": 99.0, "timestamp": 103.25}],
            )
        self.assertIsNone(scan._point_pipeline.active_point_frame)

    def test_plugin_failure_aborts_and_detaches_incomplete_frame(self) -> None:
        scan = _PipelineScan(include_timestamps=False)

        class BrokenPlugin:
            @staticmethod
            def get_headers(timestamps: bool) -> List[str]:
                del timestamps
                return ["BROKEN"]

            @staticmethod
            def on_scan_point(_index: int, _position: Any) -> List[Any]:
                raise RuntimeError("plugin failed")

        scan.plugins = [BrokenPlugin()]
        scan._begin_point_frame(
            idx=0,
            pos=1.0,
            values=[{"value": 10.0, "timestamp": 101.25}],
        )
        frame = scan._point_pipeline.active_point_frame

        with self.assertRaisesRegex(RuntimeError, "plugin failed"):
            scan._collect_plugin_point_data(0, 1.0)

        self.assertIsNone(scan._point_pipeline.active_point_frame)
        with self.assertRaisesRegex(RuntimeError, "aborted point frame"):
            frame.append_values(
                ["late"],
                [{"value": 99.0, "timestamp": 103.25}],
            )

    def test_starting_next_point_aborts_unfinished_frame(self) -> None:
        scan = _PipelineScan(include_timestamps=False)
        scan._begin_point_frame(
            idx=0,
            pos=1.0,
            values=[{"value": 10.0, "timestamp": 101.25}],
        )
        unfinished = scan._point_pipeline.active_point_frame

        scan._begin_point_frame(
            idx=1,
            pos=2.0,
            values=[{"value": 20.0, "timestamp": 101.25}],
        )

        with self.assertRaisesRegex(RuntimeError, "aborted point frame"):
            unfinished.append_values(
                ["late"],
                [{"value": 99.0, "timestamp": 103.25}],
            )
        self.assertIsNot(scan._point_pipeline.active_point_frame, unfinished)
        self.assertEqual(scan.get_current_row_value("idx"), 1)

    def test_enabling_writing_lazily_creates_file_and_writes_exact_columns(self) -> None:
        scan = _PipelineScan()
        scan.plugins = [
            _Plugin(scan, "P1", 102.25, lambda row: row["DET"] + 1.0),
            _Plugin(scan, "P2", 103.25, lambda row: row["DET"] + row["P1"]),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            scan.data_dir = tmp
            scan.output_manager.data_dir = tmp
            scan.set_data_writing_enabled(True)
            with patch(
                "kiwi_scan.scan.point_pipeline.time.time",
                return_value=100.25,
            ), patch(
                "kiwi_scan.scan._parallel_writer.datetime",
                _Clock,
            ):
                _process_point(
                    scan,
                    index=0,
                    position=5.0,
                    detector_values=[{"value": 10.0, "timestamp": 101.25}],
                )

            output_file = Path(scan.output_file or "")
            contents = output_file.read_text(encoding="utf-8")

        self.assertEqual(
            contents,
            "Position\tTS-ISO8601\tDET\tTS-ISO8601-DET\tP1\tP2\n"
            "5.000000000000e+00\tline-time\t1.000000000000e+01\tdetector-time\t"
            "1.100000000000e+01\t2.100000000000e+01\n",
        )
        self.assertEqual(
            _Clock.converted,
            [100.25, 101.25],
        )

    def test_unix_timestamp_file_output_skips_iso_conversion(self) -> None:
        scan = _PipelineScan(timestamp_output_format="unix")

        with tempfile.TemporaryDirectory() as tmp:
            scan.data_dir = tmp
            scan.output_manager.data_dir = tmp
            scan.set_data_writing_enabled(True)
            with patch(
                "kiwi_scan.scan.point_pipeline.time.time",
                return_value=100.25,
            ):
                _process_point(
                    scan,
                    index=0,
                    position=5.0,
                    detector_values=[{"value": 10.0, "timestamp": 101.25}],
                )

            output_file = Path(scan.output_file or "")
            contents = output_file.read_text(encoding="utf-8")

        self.assertEqual(
            contents,
            "Position\tTS-UNIX\tDET\tTS-UNIX-DET\n"
            "5.000000000000e+00\t1.002500000000e+02\t"
            "1.000000000000e+01\t1.012500000000e+02\n",
        )
        self.assertEqual(_Clock.converted, [])

    def test_write_failure_keeps_existing_acquired_point_semantics(self) -> None:
        """Persistence errors occur after the acquired point is published."""
        scan = _PipelineScan(include_timestamps=False)
        scan.output_manager.set_data_writing_enabled(True)
        scan.output_file = "scan.txt"
        scan.write_header_to_output_file = MagicMock()
        detector_values = [{"value": 10.0, "timestamp": 101.25}]
        scan._begin_point_frame(idx=0, pos=1.0, values=detector_values)
        frame = scan._point_pipeline.active_point_frame
        with patch(
            "builtins.open",
            side_effect=OSError("disk full"),
        ), self.assertRaisesRegex(OSError, "disk full"):
            scan.save_to_file(1.0, detector_values, include_timestamps=False)
        self.assertEqual(scan.get_value("DET"), 10.0)
        self.assertIsNone(scan._point_pipeline.active_point_frame)
        with self.assertRaisesRegex(RuntimeError, "completed point frame"):
            frame.append_values(
                ["late"],
                [{"value": 99.0, "timestamp": 103.25}],
            )


if __name__ == "__main__":
    unittest.main()
