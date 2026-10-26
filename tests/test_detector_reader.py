# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import unittest

from kiwi_scan.scan.detector_reader import (
    DetectorReader,
    DirectDetectorReadStrategy,
    MonitorSnapshotDetectorReadStrategy,
    create_detector_reader,
)


class DummyDetector:
    def __init__(self, pvname, reading=None, error=None):
        self.pvname = pvname
        self.reading = reading
        self.error = error
        self.use_monitor = None
        self._callbacks = {}
        self._next_callback_id = 1

    def get_with_metadata(self, *, use_monitor, timeout=None):
        self.use_monitor = use_monitor
        if self.error is not None:
            raise self.error
        return self.reading

    def add_callback(self, callback, *, run_now=False):
        callback_id = self._next_callback_id
        self._next_callback_id += 1
        self._callbacks[callback_id] = callback
        if run_now and self.reading is not None:
            reading = dict(self.reading)
            value = reading.pop("value", None)
            pvname = reading.pop("pvname", self.pvname)
            callback(pvname=pvname, value=value, **reading)
        return callback_id

    def remove_callback(self, callback_id):
        del self._callbacks[callback_id]

    def emit(self, value, **metadata):
        for callback in list(self._callbacks.values()):
            callback(pvname=self.pvname, value=value, **metadata)


class TimeoutDetector(DummyDetector):
    """PV recording the configured CA timeout."""

    def __init__(self, pvname, reading=None, error=None):
        super().__init__(pvname, reading, error)
        self.read_calls = []

    def get_with_metadata(self, *, use_monitor, timeout=None):
        self.read_calls.append((use_monitor, timeout))
        return super().get_with_metadata(use_monitor=use_monitor)


class TestDirectDetectorReadStrategy(unittest.TestCase):
    def test_read_keeps_column_alignment_on_failures(self):
        good = DummyDetector("DET:GOOD", {"value": 5.0})
        missing = DummyDetector("DET:NONE", None)
        broken = DummyDetector(
            "DET:BAD",
            error=RuntimeError("read failed"),
        )
        reader = create_detector_reader(
            "direct",
            [good, missing, broken],
            use_monitor=True,
        )

        with self.assertLogs(
            "kiwi_scan.scan.detector_reader",
            level="WARNING",
        ):
            readings = reader.read()

        self.assertEqual(readings, [{"value": 5.0}, None, None])
        self.assertTrue(good.use_monitor)
        self.assertTrue(missing.use_monitor)
        self.assertTrue(broken.use_monitor)

    def test_monitored_read_uses_monitor_cache(self):
        detector = DummyDetector("DET:MON", {"value": 1.0})
        reader = create_detector_reader(
            "direct",
            [detector],
            use_monitor=True,
        )

        self.assertEqual(reader.read(), [{"value": 1.0}])
        self.assertTrue(detector.use_monitor)

    def test_unmonitored_read_does_not_use_monitor_cache(self):
        detector = DummyDetector("DET:DIRECT", {"value": 2.0})
        reader = create_detector_reader(
            "direct",
            [detector],
            use_monitor=False,
        )

        self.assertEqual(reader.read(), [{"value": 2.0}])
        self.assertFalse(detector.use_monitor)

    def test_start_and_stop_are_noops(self):
        strategy = DirectDetectorReadStrategy([], use_monitor=True)

        strategy.start()
        strategy.stop()

    def test_unconfigured_timeout_passes_none_for_wrapper_defaults(self):
        detector = TimeoutDetector("DET:DEFAULT", {"value": 4.0})
        reader = create_detector_reader(
            "direct", [detector], use_monitor=False, read_timeout=None,
        )

        self.assertEqual(reader.read(), [{"value": 4.0}])
        self.assertFalse(detector.use_monitor)
        self.assertEqual(detector.read_calls, [(False, None)])

    def test_timeout_applies_to_every_pv_without_changing_monitor_selection(self):
        for use_monitor in (False, True):
            with self.subTest(use_monitor=use_monitor):
                detectors = [
                    TimeoutDetector("DET:A", {"value": 1.0}),
                    TimeoutDetector("DET:B", {"value": 2.0}),
                ]
                reader = create_detector_reader(
                    "direct", detectors, use_monitor=use_monitor, read_timeout="0.75",
                )

                self.assertEqual(reader.read(), [{"value": 1.0}, {"value": 2.0}])
                for detector in detectors:
                    self.assertEqual(detector.read_calls, [(use_monitor, 0.75)])

    def test_timeout_does_not_break_failure_alignment_or_later_reads(self):
        good = TimeoutDetector("DET:GOOD", {"value": 1})
        timeout = TimeoutDetector("DET:TIMEOUT", error=TimeoutError("CA get timed out"))
        missing = TimeoutDetector("DET:NONE")
        later = TimeoutDetector("DET:LATER", {"value": 2})
        detectors = [good, timeout, missing, later]
        strategy = DirectDetectorReadStrategy(
            detectors, use_monitor=False, read_timeout=0.5,
        )

        with self.assertLogs("kiwi_scan.scan.detector_reader", level="ERROR"):
            self.assertEqual(strategy.read(), [{"value": 1}, None, None, {"value": 2}])
        for detector in detectors:
            self.assertEqual(detector.read_calls, [(False, 0.5)])

    def test_invalid_timeout_is_rejected_before_any_read(self):
        detector = TimeoutDetector("DET:A", {"value": 1})
        for timeout in (0, -1, float("inf"), float("-inf"), float("nan"), "invalid", "", 10 ** 400):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(ValueError, "read_timeout"):
                DirectDetectorReadStrategy(
                    [detector], use_monitor=False, read_timeout=timeout,
                )
        for timeout in (True, False):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(TypeError, "read_timeout"):
                DirectDetectorReadStrategy(
                    [detector], use_monitor=False, read_timeout=timeout,
                )
        self.assertEqual(detector.read_calls, [])


class TestMonitorSnapshotDetectorReadStrategy(unittest.TestCase):
    def test_read_requires_start(self):
        strategy = MonitorSnapshotDetectorReadStrategy([])

        with self.assertRaisesRegex(RuntimeError, "not started"):
            strategy.read()

    def test_start_takes_initial_snapshot_and_updates_from_callbacks(self):
        first = DummyDetector(
            "DET:A",
            {"value": 1.0, "timestamp": 10.0},
        )
        second = DummyDetector(
            "DET:B",
            {"value": 2.0, "timestamp": 20.0},
        )
        strategy = MonitorSnapshotDetectorReadStrategy([first, second])

        strategy.start()
        self.assertEqual(
            strategy.read(),
            [
                {"timestamp": 10.0, "value": 1.0, "pvname": "DET:A"},
                {"timestamp": 20.0, "value": 2.0, "pvname": "DET:B"},
            ],
        )

        first.emit(3.0, timestamp=30.0)
        self.assertEqual(
            strategy.read(),
            [
                {"timestamp": 30.0, "value": 3.0, "pvname": "DET:A"},
                {"timestamp": 20.0, "value": 2.0, "pvname": "DET:B"},
            ],
        )

        strategy.stop()
        self.assertEqual(first._callbacks, {})
        self.assertEqual(second._callbacks, {})

    def test_start_and_stop_are_idempotent(self):
        detector = DummyDetector("DET:A", {"value": 1.0})
        strategy = MonitorSnapshotDetectorReadStrategy([detector])

        strategy.start()
        strategy.start()
        self.assertEqual(len(detector._callbacks), 1)

        strategy.stop()
        strategy.stop()
        self.assertEqual(detector._callbacks, {})


class TestDetectorReaderFactory(unittest.TestCase):
    def test_facade_delegates_lifecycle_and_read(self):
        detector = DummyDetector("DET:A", {"value": 4.0})
        strategy = DirectDetectorReadStrategy(
            [detector],
            use_monitor=False,
        )
        reader = DetectorReader(strategy)

        reader.start()
        self.assertEqual(reader.read(), [{"value": 4.0}])
        reader.stop()
        self.assertIs(reader.strategy, strategy)

    def test_monitor_snapshot_alias_is_supported(self):
        reader = create_detector_reader(
            "monitor_snapshot",
            [],
            use_monitor=True,
        )

        self.assertIsInstance(
            reader.strategy,
            MonitorSnapshotDetectorReadStrategy,
        )

    def test_snapshot_timeout_does_not_add_synchronous_gets(self):
        detector = DummyDetector("DET:A", {"value": 1, "timestamp": 10})
        reader = create_detector_reader(
            "snapshot", [detector], use_monitor=True, read_timeout=0.75,
        )

        reader.start()
        self.addCleanup(reader.stop)
        self.assertEqual(reader.read()[0]["value"], 1)
        detector.emit(2, timestamp=11)
        self.assertEqual(reader.read()[0]["value"], 2)
        self.assertIsNone(detector.use_monitor)

    def test_snapshot_factory_rejects_invalid_timeout_at_configuration(self):
        with self.assertRaisesRegex(ValueError, "read_timeout"):
            create_detector_reader("snapshot", [], use_monitor=True, read_timeout=-1)

    def test_unknown_strategy_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unknown detector reader strategy"):
            create_detector_reader(
                "unknown",
                [],
                use_monitor=True,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
