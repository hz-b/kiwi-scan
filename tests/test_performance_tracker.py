# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from kiwi_scan.scan.performance_tracker import PerformanceTracker


class TestPerformanceTracker(unittest.TestCase):
    def test_time_block_only_records_when_enabled(self):
        tracker = PerformanceTracker(enabled=False)

        with tracker.time_block("read"):
            pass
        self.assertEqual(tracker.samples["read"], [])

        tracker.enabled = True
        with patch(
            "kiwi_scan.scan.performance_tracker.time.perf_counter",
            side_effect=[10.0, 10.25],
        ), tracker.time_block("read", idx=2):
            pass

        self.assertEqual(tracker.samples["read"], [0.25])

    def test_detector_profile_api_is_removed(self):
        tracker = PerformanceTracker(enabled=True)

        self.assertFalse(hasattr(tracker, "record_detector_profile"))

    def test_report_includes_external_runtime_metrics(self):
        tracker = PerformanceTracker(
            enabled=True,
            metadata_queue_drop_count=lambda: 3,
            writer_metrics=lambda: (17, 0.125),
        )
        tracker.samples["read_detectors"].extend([0.1, 0.2])

        output = io.StringIO()
        with redirect_stdout(output):
            tracker.report()

        text = output.getvalue()
        self.assertIn("PERFORMANCE SUMMARY", text)
        self.assertIn("read_detectors", text)
        self.assertIn("metadata_queue_drops", text)
        self.assertIn("count=3", text)
        self.assertIn("point_writer_queue_high_water", text)
        self.assertIn("count=17", text)
        self.assertIn("seconds=0.125000", text)

    def test_report_is_silent_when_disabled(self):
        tracker = PerformanceTracker(enabled=False)
        output = io.StringIO()

        with redirect_stdout(output):
            tracker.report()

        self.assertEqual(output.getvalue(), "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
