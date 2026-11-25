# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import socket
import unittest
from threading import Event
from unittest.mock import Mock, call, patch

from kiwi_scan.plugin_concrete import emc_getter


class TestEmcGetter(unittest.TestCase):
    """ 
    Test target: 
     - TCP reply
     - Configuration
     - Reconnection and shutdown
    """
    def setUp(self):
        patcher = patch.object(emc_getter.socket, "create_connection", autospec=True)
        self.connect = patcher.start()
        self.addCleanup(patcher.stop)
        self.sock = Mock(spec=socket.socket)
        self.connect.return_value = self.sock

    def make_plugin(self, **settings):
        parameters = {"getters": [{"command": "GPE", "column": "energy"}]}
        parameters.update(settings)
        plugin = emc_getter.EmcGetterPlugin("emc", parameters)
        self.addCleanup(plugin.close)
        return plugin

    def test_fragmented_replies(self):
        for terminator, chunks in (
            ("lf", [b"t 12", b".5\n"]),
            ("cr", [b"t 12", b".5\r"]),
            ("crlf", [b"t 12.5\r", b"\n"]),
        ):
            with self.subTest(terminator=terminator):
                plugin = self.make_plugin(reply_terminator=terminator)
                self.sock.recv.side_effect = chunks
                self.sock.sendall.reset_mock()
                self.sock.recv.reset_mock()
                self.assertEqual(plugin.process_data_snapshot(0), [12.5])
                self.sock.sendall.assert_called_once_with(b"GPE\n")
                self.assertEqual(self.sock.recv.call_count, len(chunks))
                plugin.close()

    def test_invalid_frames_discard_the_connection(self):
        for chunks, message in (
            ([b"t 12", b""], "closed the connection"),
            ([b"t 12\nextra"], "unexpected data after EMC reply"),
            ([b"x" * 4096], "exceeds 4096 bytes"),
        ):
            with self.subTest(message=message):
                plugin = self.make_plugin(reply_terminator="lf")
                self.sock.recv.side_effect = chunks
                self.sock.close.reset_mock()
                with self.assertRaisesRegex(RuntimeError, "GPE.*" + message):
                    plugin.process_data_snapshot(0)
                self.sock.close.assert_called_once_with()

    def test_partial_reply_does_not_restart_exchange_deadline(self):
        plugin = self.make_plugin(reply_terminator="lf", timeout_s=1.0)
        self.sock.recv.side_effect = [b"t 12", b".5\n"]
        clock = patch.object(emc_getter, "monotonic", side_effect=[10.0, 10.25, 11.0])
        with clock, self.assertRaisesRegex(RuntimeError, "GPE.*deadline exceeded"):
            plugin.process_data_snapshot(0)
        self.assertEqual(self.sock.recv.call_count, 1)
        self.assertEqual(self.sock.settimeout.call_args_list, [call(1.0), call(0.75)])
        self.sock.close.assert_called_once_with()

    def test_invalid_settings_are_rejected_before_connecting(self):
        for settings, error, message in (
            ({"host": " "}, ValueError, "host"),
            ({"port": 65536}, ValueError, "port"),
            ({"timeout_s": 0}, ValueError, "timeout_s"),
            ({"reply_terminator": "tab"}, ValueError, "reply_terminator"),
            ({"min_interval_s": -1}, ValueError, "min_interval_s"),
            ({"max_workers": 2}, ValueError, "max_workers"),
            ({"record_times": "yes"}, TypeError, "record_times"),
        ):
            with self.subTest(settings=settings), self.assertRaisesRegex(error, message):
                self.make_plugin(**settings)
        self.connect.assert_not_called()

    def test_invalid_getter_definitions_are_rejected(self):
        energy = {"command": "GPE", "column": "energy"}
        for getters, record_times, message in (
            ([], False, "non-empty list"),
            ([dict(energy, command="")], False, "command"),
            ([dict(energy, command="GPE\nGST")], False, "ASCII line"),
            ([dict(energy, column="bad name")], False, "column"),
            ([dict(energy, value_type="str")], False, "value_type"),
            ([energy, energy], False, "unique"),
            ([energy, {"command": "GST", "column": "energy_before"}], True, "unique"),
        ):
            with self.subTest(getters=getters), self.assertRaisesRegex(ValueError, message):
                self.make_plugin(getters=getters, record_times=record_times)
        self.connect.assert_not_called()

    def test_reconnects_after_failed_read_and_reuses_new_socket(self):
        replacement = Mock(spec=socket.socket)
        self.connect.side_effect = [self.sock, replacement]
        self.sock.recv.side_effect = ConnectionResetError("connection reset")
        replacement.recv.side_effect = [b"t 12.5", b"t 14.0"]
        plugin = self.make_plugin()

        with self.assertRaisesRegex(RuntimeError, "GPE.*connection reset"):
            plugin.process_data_snapshot(0)
        self.sock.close.assert_called_once_with()
        self.assertEqual(plugin.process_data_snapshot(1), [12.5])
        self.assertEqual(plugin.process_data_snapshot(2), [14.0])
        self.assertEqual(self.connect.call_count, 2)
        self.assertEqual(replacement.sendall.call_args_list, [call(b"GPE\n")] * 2)
        plugin.close()
        replacement.close.assert_called_once_with()

    def test_close_defers_socket_cleanup_until_active_read_finishes(self):
        entered_recv = Event()
        release_recv = Event()
        socket_closed = Event()

        def blocked_recv(_size):
            entered_recv.set()
            if not release_recv.wait(timeout=5):
                raise AssertionError("test did not release recv")
            return b"t 12.5"

        self.sock.recv.side_effect = blocked_recv
        self.sock.close.side_effect = socket_closed.set
        plugin = self.make_plugin()
        try:
            plugin.get_values(0, {})
            self.assertTrue(entered_recv.wait(timeout=5), "worker did not reach recv")
            plugin.close()
            self.sock.close.assert_not_called()
        finally:
            release_recv.set()

        self.assertTrue(socket_closed.wait(timeout=5), "socket remained open after recv")
        plugin.get_values(1, {})
        self.assertEqual(plugin.get_async_status()["submitted_jobs"], 1)
        plugin.close()
        self.sock.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
