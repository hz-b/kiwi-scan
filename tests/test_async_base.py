# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import sys
import threading
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from kiwi_scan.plugin.async_base import AsyncScanPlugin


class TestAsyncPlugin(AsyncScanPlugin):
    """Small concrete plugin used by the unit tests."""

    __test__ = False

    def __init__(self, *args, **kwargs):
        self.default_values = ["pending"]
        self.sync_values = ["sync"]
        self.snapshots = []
        self.processed_snapshots = []
        self.result_values = ["complete"]
        super().__init__(*args, **kwargs)

    def get_headers(self, timestamps):
        return ["sync", "async"]

    def get_sync_values(self, idx, pos):
        return list(self.sync_values)

    def get_async_default_values(self):
        return list(self.default_values)

    def build_data_snapshot(self, idx, pos):
        snapshot = {"scanIndex": idx, "position": pos}
        self.snapshots.append(snapshot)
        return snapshot

    def process_data_snapshot(self, data_snapshot):
        self.processed_snapshots.append(data_snapshot)
        return list(self.result_values)


class BlockingAsyncPlugin(TestAsyncPlugin):
    __test__ = False

    def __init__(self, *args, **kwargs):
        self.processing_started = threading.Event()
        self.release_processing = threading.Event()
        super().__init__(*args, **kwargs)

    def process_data_snapshot(self, data_snapshot):
        self.processed_snapshots.append(data_snapshot)
        self.processing_started.set()
        if not self.release_processing.wait(timeout=2.0):
            raise RuntimeError("test worker was not released")
        return list(self.result_values)


class FailingAsyncPlugin(TestAsyncPlugin):
    __test__ = False

    def process_data_snapshot(self, data_snapshot):
        self.processed_snapshots.append(data_snapshot)
        raise RuntimeError("processing failed")


class SkippingAsyncPlugin(TestAsyncPlugin):
    __test__ = False

    def build_data_snapshot(self, idx, pos):
        return None


class NoneResultAsyncPlugin(TestAsyncPlugin):
    __test__ = False

    def process_data_snapshot(self, data_snapshot):
        return None


class TestAsyncScanPluginInitialization(unittest.TestCase):
    def test_initial_status_and_default_values(self):
        plugin = TestAsyncPlugin("test")
        self.addCleanup(plugin.close)

        self.assertEqual(plugin._latest_async_result_values, ["pending"])
        self.assertEqual(
            plugin.get_async_status(),
            {
                "running": False,
                "submitted_jobs": 0,
                "completed_jobs": 0,
                "dropped_snapshots": 0,
                "latest_result_timestamp": None,
                "latest_error": None,
                "max_workers": 1,
            },
        )

    @patch("kiwi_scan.plugin.async_base.ThreadPoolExecutor")
    def test_max_workers_is_passed_to_executor(self, executor_class):
        plugin = TestAsyncPlugin("test", parameters={"max_workers": "3"})

        executor_class.assert_called_once_with(
            max_workers=3,
            thread_name_prefix="kiwi-plugin-test",
        )
        self.assertEqual(plugin.get_async_status()["max_workers"], 3)
        plugin.close()

    @patch("kiwi_scan.plugin.async_base.ThreadPoolExecutor")
    def test_max_workers_is_at_least_one(self, executor_class):
        plugin = TestAsyncPlugin("test", parameters={"max_workers": 0})

        executor_class.assert_called_once_with(
            max_workers=1,
            thread_name_prefix="kiwi-plugin-test",
        )
        plugin.close()

    def test_default_hook_implementations(self):
        class MinimalAsyncPlugin(AsyncScanPlugin):
            def get_headers(self, timestamps):
                return []

        plugin = MinimalAsyncPlugin("minimal")
        self.addCleanup(plugin.close)

        self.assertEqual(plugin.get_sync_values(1, 2.0), [])
        self.assertEqual(plugin.get_async_default_values(), [])
        self.assertEqual(
            plugin.build_data_snapshot(1, {"motor": 2.0}),
            {"scanIndex": 1, "position": {"motor": 2.0}},
        )
        self.assertEqual(plugin.process_data_snapshot({}), [])


class TestAsyncScanPluginProcessing(unittest.TestCase):
    def test_get_values_returns_defaults_without_waiting_for_worker(self):
        plugin = BlockingAsyncPlugin("test")
        self.addCleanup(plugin.close)

        values = plugin.get_values(4, {"motor": 1.5})

        self.assertEqual(values, ["sync", "pending"])
        self.assertTrue(plugin.processing_started.wait(timeout=1.0))
        self.assertEqual(
            plugin.snapshots,
            [{"scanIndex": 4, "position": {"motor": 1.5}}],
        )
        self.assertEqual(plugin.get_async_status()["submitted_jobs"], 1)

        plugin.release_processing.set()
        plugin._async_job.result(timeout=1.0)

    def test_finished_result_is_returned_on_next_scan_point(self):
        plugin = TestAsyncPlugin("test")
        self.addCleanup(plugin.close)

        first_values = plugin.get_values(0, 1.0)
        first_job = plugin._async_job
        first_job.result(timeout=1.0)
        second_values = plugin.get_values(1, 2.0)

        self.assertEqual(first_values, ["sync", "pending"])
        self.assertEqual(second_values, ["sync", "complete"])
        status = plugin.get_async_status()
        self.assertEqual(status["submitted_jobs"], 2)
        self.assertEqual(status["completed_jobs"], 1)
        self.assertIsNotNone(status["latest_result_timestamp"])
        self.assertIsNone(status["latest_error"])

        plugin._async_job.result(timeout=1.0)

    def test_busy_plugin_drops_new_snapshot(self):
        plugin = BlockingAsyncPlugin("test", parameters={"max_workers": 2})
        self.addCleanup(plugin.close)

        plugin.get_values(0, 1.0)
        self.assertTrue(plugin.processing_started.wait(timeout=1.0))
        values = plugin.get_values(1, 2.0)

        self.assertEqual(values, ["sync", "pending"])
        self.assertEqual(plugin.get_async_status()["submitted_jobs"], 1)
        self.assertEqual(plugin.get_async_status()["dropped_snapshots"], 1)
        self.assertEqual(len(plugin.processed_snapshots), 1)

        plugin.release_processing.set()
        plugin._async_job.result(timeout=1.0)

    def test_none_snapshot_skips_submission(self):
        plugin = SkippingAsyncPlugin("test")
        self.addCleanup(plugin.close)

        values = plugin.get_values(0, 1.0)

        self.assertEqual(values, ["sync", "pending"])
        self.assertEqual(plugin.get_async_status()["submitted_jobs"], 0)
        self.assertFalse(plugin.get_async_status()["running"])

    def test_none_result_is_converted_to_empty_list(self):
        plugin = NoneResultAsyncPlugin("test")
        self.addCleanup(plugin.close)

        plugin.get_values(0, 1.0)
        plugin._async_job.result(timeout=1.0)
        plugin._update_async_data()

        self.assertEqual(plugin._latest_async_result_values, [])
        self.assertEqual(plugin.get_async_status()["completed_jobs"], 1)
        self.assertIsNone(plugin.get_async_status()["latest_error"])

    def test_processing_error_restores_defaults_and_records_error(self):
        plugin = FailingAsyncPlugin("test")
        self.addCleanup(plugin.close)

        plugin.get_values(0, 1.0)
        with self.assertRaisesRegex(RuntimeError, "processing failed"):
            plugin._async_job.result(timeout=1.0)

        with self.assertLogs("ScanPlugin.test", level="ERROR"):
            plugin._update_async_data()

        status = plugin.get_async_status()
        self.assertEqual(plugin._latest_async_result_values, ["pending"])
        self.assertEqual(status["completed_jobs"], 1)
        self.assertEqual(status["latest_error"], "processing failed")
        self.assertIsNotNone(status["latest_result_timestamp"])

    def test_unfinished_job_is_not_consumed(self):
        plugin = TestAsyncPlugin("test")
        self.addCleanup(plugin.close)
        future = MagicMock(spec=Future)
        future.done.return_value = False
        plugin._async_job = future

        plugin._update_async_data()

        self.assertIs(plugin._async_job, future)
        self.assertEqual(plugin.get_async_status()["completed_jobs"], 0)
        future.result.assert_not_called()

    def test_result_cache_is_copied_before_return(self):
        plugin = SkippingAsyncPlugin("test")
        self.addCleanup(plugin.close)
        plugin._latest_async_result_values = ["cached"]

        result = plugin.get_values(0, 1.0)
        result[-1] = "changed"

        self.assertEqual(plugin._latest_async_result_values, ["cached"])


class TestAsyncScanPluginLifecycle(unittest.TestCase):
    @patch("kiwi_scan.plugin.async_base.ThreadPoolExecutor")
    def test_closed_plugin_ignores_new_submission(self, executor_class):
        executor = executor_class.return_value
        plugin = TestAsyncPlugin("test")
        plugin.close()

        plugin._submit_async_background_job(0, 1.0)

        executor.submit.assert_not_called()
        self.assertEqual(plugin.get_async_status()["submitted_jobs"], 0)

    @patch("kiwi_scan.plugin.async_base.ThreadPoolExecutor")
    def test_close_cancels_job_and_shuts_down_executor(self, executor_class):
        executor = executor_class.return_value
        future = MagicMock(spec=Future)
        plugin = TestAsyncPlugin("test")
        plugin._async_job = future

        plugin.close()

        future.cancel.assert_called_once_with()
        expected_kwargs = {"wait": False}
        if sys.version_info >= (3, 9):
            expected_kwargs["cancel_futures"] = True
        executor.shutdown.assert_called_once_with(**expected_kwargs)
        self.assertTrue(plugin._closed)

    @patch("kiwi_scan.plugin.async_base.ThreadPoolExecutor")
    def test_close_is_idempotent(self, executor_class):
        executor = executor_class.return_value
        plugin = TestAsyncPlugin("test")

        plugin.close()
        plugin.close()

        executor.shutdown.assert_called_once()

    def test_on_end_closes_plugin(self):
        plugin = TestAsyncPlugin("test")

        with patch.object(plugin, "close") as close:
            plugin.on_end()

        close.assert_called_once_with()
        plugin.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
