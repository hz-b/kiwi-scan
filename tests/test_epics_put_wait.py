# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import threading
import unittest
from unittest.mock import Mock, patch

import numpy as np

from kiwi_scan.epics_wrapper import _CA_LOCK, EpicsPV


class TestEpicsPutWait(unittest.TestCase):
    def make_pv(self, result=1):
        wrapper = EpicsPV.__new__(EpicsPV)
        wrapper.pvname = "TRIGGER"
        wrapper.timeout = 1.5
        wrapper.queueing_delay = 0.0
        wrapper.last_written = None
        wrapper._pv = Mock()
        wrapper._pv.put.return_value = result
        return wrapper

    def test_blocking_put_forwards_wait_and_explicit_timeout_without_callback(self):
        pv = self.make_pv()

        self.assertTrue(pv.put(2.0, wait=True, timeout=4.0))

        pv._pv.put.assert_called_once_with(2.0, wait=True, timeout=4.0)
        self.assertEqual(pv.last_written, 2.0)

    def test_blocking_put_uses_wrapper_timeout_when_not_overridden(self):
        pv = self.make_pv()

        self.assertTrue(pv.put(2.0, wait=True))

        pv._pv.put.assert_called_once_with(2.0, wait=True, timeout=1.5)

    def test_timeout_is_raised_instead_of_misreading_negative_status_as_success(self):
        pv = self.make_pv(result=-1)

        with self.assertLogs("kiwi_scan.epics_wrapper", level="DEBUG") as logs, \
                self.assertRaisesRegex(TimeoutError, r"TRIGGER.*4\.000s"):
            pv.put(2.0, wait=True, timeout=4.0)

        # The put was accepted, but IOC processing may still be in progress.
        self.assertEqual(pv.last_written, 2.0)
        # The caller owns the warning; avoid a duplicate warning or failure log.
        self.assertTrue(all(record.levelno < 30 for record in logs.records))

    def test_rejected_blocking_writes_log_failure_and_return_false(self):
        for result in (None, False, 0, -2):
            with self.subTest(result=result):
                pv = self.make_pv(result=result)
                with self.assertLogs("kiwi_scan.epics_wrapper", level="ERROR") as logs:
                    self.assertFalse(pv.put(2.0, wait=True, timeout=4.0))
                self.assertIn("TRIGGER", logs.output[0])
                self.assertIn("status", logs.output[0])
                self.assertIsNone(pv.last_written)

    def test_submission_exception_returns_false_with_useful_error(self):
        pv = self.make_pv()
        pv._pv.put.side_effect = RuntimeError("disconnected")

        with self.assertLogs("kiwi_scan.epics_wrapper", level="ERROR") as logs:
            self.assertFalse(pv.put(2.0, wait=True, timeout=4.0))

        self.assertIn("TRIGGER", logs.output[0])
        self.assertIn("disconnected", logs.output[0])

    def test_invalid_blocking_timeout_prevents_write(self):
        for timeout in (-1, float("inf"), float("nan")):
            with self.subTest(timeout=timeout):
                pv = self.make_pv()
                with self.assertRaises(ValueError):
                    pv.put(2.0, wait=True, timeout=timeout)
                pv._pv.put.assert_not_called()

    def test_waveform_completion_precedes_wrapper_delay_and_logs_elapsed_time(self):
        pv = self.make_pv()
        pv.queueing_delay = 0.25
        trace = []
        pv._pv.put.side_effect = lambda *_args, **_kwargs: trace.append("completed") or 1

        with self.assertLogs("kiwi_scan.epics_wrapper", level="DEBUG") as logs, patch(
            "kiwi_scan.epics_wrapper.time.sleep", side_effect=lambda delay: trace.append(delay)
        ):
            self.assertTrue(pv.put(np.array([1, 2]), wait=True, timeout=4.0))

        self.assertEqual(trace, ["completed", 0.25])
        pv._pv.put.assert_called_once_with([1, 2], wait=True, timeout=4.0)
        self.assertTrue(any("put completed in" in line for line in logs.output))

    def test_blocking_put_does_not_hold_global_ca_lock(self):
        pv = self.make_pv()
        finished = threading.Event()
        results = []

        def write():
            results.append(pv.put(2.0, wait=True))
            finished.set()

        worker = threading.Thread(target=write)
        with _CA_LOCK:
            worker.start()
            progressed_while_locked = finished.wait(1.0)
        worker.join(1.0)

        self.assertTrue(progressed_while_locked)
        self.assertEqual(results, [True])


if __name__ == "__main__":
    unittest.main()
