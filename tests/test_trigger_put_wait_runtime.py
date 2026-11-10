# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import threading
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock

from test_common import DummyScan

from kiwi_scan.epics_wrapper import PutCompletion
from kiwi_scan.scan.performance_tracker import PerformanceTracker
from kiwi_scan.scan.trigger_manager import PreparedTriggerAction, TriggerManager
from kiwi_scan.scan_concrete.para import ParaScan


class TestTriggerPutWaitAcquisition(unittest.TestCase):
    def make_scan(self, mode):
        scan = object.__new__(ParaScan) if mode == "para" else DummyScan()
        scan._stop_requested = threading.Event()
        scan._point_pipeline = Mock()
        scan.performance = PerformanceTracker(enabled=False)
        scan.integration_time = 0.0
        scan._stat_window_lock = threading.Lock()
        scan.scan_dimensions = [SimpleNamespace(actuator="motor")]
        scan._integrate_point = Mock(return_value=False)
        scan.read_detectors = Mock(return_value=[2.0])
        scan._begin_point_frame = Mock()
        scan._collect_plugin_point_data = Mock()
        scan._commit_point_async = Mock(return_value=[])
        return scan

    @staticmethod
    def acquire(scan, mode, monitor):
        if mode == "step":
            return scan._acquire_daq_point_step(0, 1.0, monitor)
        if mode == "continuous":
            return scan._acquire_daq_point_continuous(0, 1.0, monitor)
        return scan._acquire_point(0, {"motor": 1.0}, monitor)

    @staticmethod
    def set_trigger(scan, phase, outcome):
        pv = Mock()
        pv.pvname = "DET:ACQUIRE"

        def submit(_value, *, timeout):
            completion = PutCompletion(pv.pvname, timeout)
            if outcome == "stop":
                scan._stop_requested.set()
            elif outcome == "complete":
                completion.callback()
            return completion

        pv.put_with_completion.side_effect = submit
        # A positive prepared timeout selects completion handling. For a
        # deterministic timeout test, expire the completion before waiting.
        if outcome == "timeout":
            pv.put_with_completion.side_effect = (
                lambda _value, **_kwargs: PutCompletion(pv.pvname, 0.0)
            )
        action = PreparedTriggerAction(pv=pv, value=1, put_timeout=60.0)
        scan.trigger_manager = TriggerManager({phase: [action]})

    def test_stop_during_on_point_wait_drops_point_without_detector_read(self):
        for mode in ("step", "continuous", "para"):
            with self.subTest(mode=mode):
                scan = self.make_scan(mode)
                self.set_trigger(scan, "on_point", "stop")
                monitor = Mock()

                result = self.acquire(scan, mode, monitor)

                self.assertFalse(result)
                scan.read_detectors.assert_not_called()
                scan._begin_point_frame.assert_not_called()
                scan._collect_plugin_point_data.assert_not_called()
                scan._commit_point_async.assert_not_called()
                scan._integrate_point.assert_not_called()
                monitor.update.assert_not_called()
                if mode == "para":
                    self.assertFalse(scan._daq_is_on)

    def test_stop_during_after_point_wait_discards_prepared_frame(self):
        for mode in ("continuous", "para"):
            with self.subTest(mode=mode):
                scan = self.make_scan(mode)
                self.set_trigger(scan, "after_point", "stop")
                monitor = Mock()

                self.acquire(scan, mode, monitor)

                scan.read_detectors.assert_called_once_with()
                scan._begin_point_frame.assert_called_once()
                scan._point_pipeline.abort_active_point_frame.assert_called_once_with()
                scan._collect_plugin_point_data.assert_not_called()
                scan._commit_point_async.assert_not_called()
                monitor.update.assert_not_called()

    def test_successful_completion_preserves_acquisition(self):
        for mode in ("step", "continuous", "para"):
            with self.subTest(mode=mode):
                scan = self.make_scan(mode)
                self.set_trigger(scan, "on_point", "complete")
                monitor = Mock()

                self.acquire(scan, mode, monitor)

                scan.read_detectors.assert_called_once_with()
                scan._collect_plugin_point_data.assert_called_once_with(0, 1.0)
                scan._commit_point_async.assert_called_once_with(1.0, [2.0])
                monitor.update.assert_called_once_with([])

    def test_timeout_without_stop_keeps_warning_and_continue_behavior(self):
        for mode in ("step", "continuous", "para"):
            with self.subTest(mode=mode):
                scan = self.make_scan(mode)
                self.set_trigger(scan, "on_point", "timeout")
                with self.assertLogs("kiwi_scan.epics_wrapper", level="WARNING") as logs:
                    self.acquire(scan, mode, None)

                self.assertEqual(len(logs.records), 1)
                self.assertFalse(scan._stop_requested.is_set())
                scan.read_detectors.assert_called_once_with()
                scan._commit_point_async.assert_called_once_with(1.0, [2.0])


class TestTriggerPutWaitShutdown(unittest.TestCase):
    def test_close_cancels_monitor_wait_before_closing_trigger_pv(self):
        scan = DummyScan()
        scan._resources = ExitStack()
        scan._initialize_event_state()
        completion = PutCompletion("MONITOR:ACQUIRE", 60.0)
        waiting = threading.Event()
        original_wait = completion.wait

        def wait(stop_event=None):
            waiting.set()
            return original_wait(stop_event)

        completion.wait = wait
        pv = Mock()
        pv.pvname = completion.pvname
        pv.put_with_completion.return_value = completion
        closed_after_worker_exit = []
        pv.close.side_effect = lambda: closed_after_worker_exit.append(
            not scan._trigger_worker.is_alive()
        )
        scan.trigger_manager = TriggerManager({
            "monitor": [PreparedTriggerAction(pv=pv, value=1, put_timeout=60.0)]
        })
        scan._resources.callback(scan.trigger_manager.close)
        scan._start_event_workers()
        try:
            scan._trigger_q.put(object())
            self.assertTrue(waiting.wait(1.0))

            scan.close()
            scan.close()

            self.assertFalse(scan._trigger_worker.is_alive())
            self.assertFalse(scan._plugin_worker.is_alive())
            self.assertTrue(completion.cancelled)
            self.assertEqual(closed_after_worker_exit, [True])
            pv.close.assert_called_once_with()
        finally:
            completion.cancel()
            scan._stop_requested.set()
            scan._resources.close()
            scan._trigger_worker.join(1.0)
            scan._plugin_worker.join(1.0)

    def test_close_before_event_state_initialization_remains_safe(self):
        scan = DummyScan()
        scan._resources = ExitStack()
        resource = Mock()
        scan._resources.callback(resource.close)

        scan.close()
        scan.close()

        resource.close.assert_called_once_with()

    def test_after_cleanup_keeps_waiting_despite_scan_stop(self):
        scan = DummyScan()
        scan._initialize_event_state()
        scan._stop_requested.set()
        completion = PutCompletion("DET:RESET", 0.0)
        pv = Mock()
        pv.pvname = completion.pvname
        pv.put_with_completion.return_value = completion
        scan.trigger_manager = TriggerManager({
            "after": [PreparedTriggerAction(pv=pv, value=0, put_timeout=1.0)]
        })

        # A warning proves that cleanup reached its completion deadline rather
        # than returning immediately because the scan stop event was already set.
        with self.assertLogs("kiwi_scan.epics_wrapper", level="WARNING"):
            scan._fire_triggers("after")

        self.assertFalse(completion.cancelled)
        pv.put_with_completion.assert_called_once_with(0, timeout=1.0)


if __name__ == "__main__":
    unittest.main()
