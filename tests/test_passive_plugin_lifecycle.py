# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Plugin lifecycle regressions using the existing hardware-free scan fixtures."""

import threading
import unittest
from unittest.mock import Mock

import test_async_base
import test_para
import test_poll


class TestPassiveScanPluginLifecycle(unittest.TestCase):
    @staticmethod
    def _make_scan(engine):
        if engine == "poll":
            scan, _actuator = test_poll.PollScanTestCase().make_scan()
            scan.run_daq = Mock()
        else:
            scan = test_para.ParaScanTestCase().make_scan()
            scan._wait_cycle = Mock()
            scan._read_position_snapshot = Mock(return_value={"motor": 2.0})
            scan._acquire_point = Mock(return_value=False)
        return scan

    def test_hooks_surround_acquisition_and_precede_resource_teardown(self):
        for engine in ("poll", "para"):
            with self.subTest(engine=engine):
                scan = self._make_scan(engine)
                plugin = Mock(name="plugin")
                scan.plugins = [plugin]
                events = Mock()
                events.attach_mock(plugin.on_start, "start")
                events.attach_mock(scan._start_subscriptions, "subscribe")
                acquisition = scan.run_daq if engine == "poll" else scan._acquire_point
                events.attach_mock(acquisition, "acquire")
                events.attach_mock(plugin.on_end, "end")
                events.attach_mock(plugin.close, "close")
                events.attach_mock(scan._stop_metadata_monitor, "metadata_stop")

                scan.scan(None)

                # Poll subscriptions are active during the motion wait, before
                # plugins start. Para starts plugins before subscribing.
                startup = ["subscribe", "start"] if engine == "poll" else ["start", "subscribe"]
                self.assertEqual(
                    [event[0] for event in events.mock_calls],
                    startup + ["acquire", "end", "close", "metadata_stop"],
                )

    def test_startup_failures_still_end_and_close_plugins(self):
        stages = {
            "poll": (
                "_start_metadata_monitor", "_fire_triggers", "_wait_for_motion_start",
                "_start_detector_reader", "_start_subscriptions",
            ),
            "para": (
                "write_header_to_output_file", "_position_tolerances",
                "_start_detector_reader", "_start_subscriptions",
                "_start_metadata_monitor", "_fire_triggers",
            ),
        }
        for engine, methods in stages.items():
            for method in methods:
                with self.subTest(engine=engine, method=method):
                    scan = self._make_scan(engine)
                    plugin = Mock(name="plugin")
                    scan.plugins = [plugin]
                    error = RuntimeError("startup failed")
                    # The after trigger must remain callable during cleanup.
                    failing_step = Mock(side_effect=[error, None])
                    setattr(scan, method, failing_step)

                    with self.assertRaises(RuntimeError) as raised:
                        scan.scan(None)

                    self.assertIs(raised.exception, error)
                    plugin.on_end.assert_called_once_with()
                    plugin.close.assert_called_once_with()
                    self.assertFalse(scan.busyflag)

    def test_acquisition_failures_and_interrupts_still_close_plugins(self):
        for engine in ("poll", "para"):
            for error_type in (RuntimeError, KeyboardInterrupt):
                with self.subTest(engine=engine, error=error_type.__name__):
                    scan = self._make_scan(engine)
                    plugin = Mock(name="plugin")
                    scan.plugins = [plugin]
                    acquisition = scan.run_daq if engine == "poll" else scan._acquire_point
                    acquisition.side_effect = error_type("acquisition interrupted")

                    with self.assertRaises(error_type):
                        scan.scan(None)

                    plugin.on_start.assert_called_once_with()
                    plugin.on_end.assert_called_once_with()
                    plugin.close.assert_called_once_with()
                    self.assertFalse(scan.busyflag)

    def test_stop_request_still_ends_and_closes_plugins(self):
        for engine in ("poll", "para"):
            with self.subTest(engine=engine):
                scan = self._make_scan(engine)
                acquisition = scan.run_daq if engine == "poll" else scan._acquire_point
                scan._start_subscriptions.side_effect = scan._stop_requested.set
                plugin = Mock(name="plugin")
                scan.plugins = [plugin]

                scan.scan(None)

                if engine == "poll":
                    # Startup was cancelled before plugins or DAQ could start.
                    plugin.on_start.assert_not_called()
                else:
                    plugin.on_start.assert_called_once_with()
                plugin.on_end.assert_called_once_with()
                plugin.close.assert_called_once_with()
                acquisition.assert_not_called()
                scan.read_detectors.assert_not_called()
                self.assertTrue(scan._stop_requested.is_set())
                self.assertFalse(scan.busyflag)

    def test_poll_stop_during_motion_wait_still_ends_and_closes_plugins(self):
        scan = self._make_scan("poll")
        plugin = Mock(name="plugin")
        scan.plugins = [plugin]

        def stop_during_wait(_actuator):
            self.assertTrue(scan.busyflag)
            scan._start_subscriptions.assert_called_once_with()
            scan._stop_requested.set()
            return False

        scan._wait_for_motion_start = Mock(side_effect=stop_during_wait)

        scan.scan(None)

        plugin.on_start.assert_not_called()
        plugin.on_end.assert_called_once_with()
        plugin.close.assert_called_once_with()
        scan._detector_reader.start.assert_not_called()
        scan.run_daq.assert_not_called()
        scan._stop_metadata_monitor.assert_called_once_with()
        self.assertTrue(scan._stop_requested.is_set())
        self.assertFalse(scan.busyflag)

    def test_poll_stop_after_plugin_start_still_ends_and_closes_plugins(self):
        scan = self._make_scan("poll")
        # Retain coverage of the real DAQ loop's early stop path.
        del scan.run_daq
        plugin = Mock(name="plugin")
        plugin.on_start.side_effect = scan._stop_requested.set
        scan.plugins = [plugin]

        scan.scan(None)

        scan._start_subscriptions.assert_called_once_with()
        plugin.on_start.assert_called_once_with()
        plugin.on_end.assert_called_once_with()
        plugin.close.assert_called_once_with()
        scan.read_detectors.assert_not_called()
        scan._arm_sync_controller.assert_not_called()
        self.assertTrue(scan._stop_requested.is_set())
        self.assertFalse(scan.busyflag)

    def test_failing_hook_does_not_block_other_plugins_or_resource_cleanup(self):
        for engine in ("poll", "para"):
            for hook in ("on_start", "on_end", "close"):
                with self.subTest(engine=engine, hook=hook):
                    scan = self._make_scan(engine)
                    broken = Mock(name="broken")
                    healthy = Mock(name="healthy")
                    getattr(broken, hook).side_effect = RuntimeError("hook failed")
                    scan.plugins = [broken, healthy]

                    with self.assertLogs("kiwi_scan.scan.common", level="ERROR"):
                        scan.scan(None)

                    for plugin in (broken, healthy):
                        plugin.on_start.assert_called_once_with()
                        plugin.on_end.assert_called_once_with()
                        plugin.close.assert_called_once_with()
                    scan._stop_metadata_monitor.assert_called_once_with()
                    self.assertFalse(scan.busyflag)

    def test_async_worker_exits_while_plugin_is_still_referenced(self):
        for engine in ("poll", "para"):
            with self.subTest(engine=engine):
                scan = self._make_scan(engine)
                plugin = test_async_base.TestAsyncPlugin("passive-lifecycle-" + engine)
                self.addCleanup(plugin.close)
                scan.plugins = [plugin]
                workers = []

                def acquire(*_args, current_plugin=plugin, current_workers=workers):
                    current_plugin.get_values(0, 2.0)
                    # Wait for a real job to finish, leaving its worker idle.
                    current_plugin._async_job.result(timeout=2.0)
                    prefix = "kiwi-plugin-" + current_plugin.name
                    current_workers.extend(
                        thread for thread in threading.enumerate()
                        if thread.name.startswith(prefix)
                    )

                acquisition = scan.run_daq if engine == "poll" else scan._acquire_point
                acquisition.side_effect = acquire
                scan.scan(None)

                self.assertTrue(workers, "The regression requires a real executor worker")
                for worker in workers:
                    worker.join(timeout=2.0)
                    self.assertFalse(worker.is_alive())
                self.assertTrue(plugin._closed)
                with self.assertRaises(RuntimeError):
                    plugin._async_executor.submit(lambda: None)


if __name__ == "__main__":
    unittest.main()
