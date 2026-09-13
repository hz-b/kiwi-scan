# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import queue
import threading
import unittest
from unittest.mock import MagicMock

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import SubscriptionConfig
from kiwi_scan.scan.event_handler import ScanEventHandler


class _Context:
    def __init__(self) -> None:
        self.busyflag = False
        self.actuators = {}
        self.sync_controller = MagicMock()
        self._tick_cond = threading.Condition()
        self._tick_seq = 0
        self._stop_requested = threading.Event()
        self._last_heartbeat = None
        self._last_status = None
        self._position = None
        self._position_sync_subscription_set = False
        self._trigger_q = queue.SimpleQueue()
        self._plugin_q = queue.SimpleQueue()
        self.position_sync = True

    def _is_position_sync_subscription(self, _subscription) -> bool:
        return self.position_sync


class TestScanEventHandler(unittest.TestCase):
    def setUp(self) -> None:
        self.context = _Context()
        self.handler = ScanEventHandler(self.context)
        self.subscription = SubscriptionConfig(
            name="event",
            role="status",
            pv="TEST:PV",
        )

    def test_status_and_heartbeat_update_shared_scan_state(self) -> None:
        status = PvEvent("TEST:STATUS", 1)
        heartbeat = PvEvent("TEST:HEARTBEAT", 2)

        self.handler.on_status_event(status, self.subscription)
        self.handler.on_heartbeat_event(heartbeat, self.subscription)

        self.assertIs(self.context._last_status, status)
        self.assertIs(self.context._last_heartbeat, heartbeat)
        self.assertEqual(self.context._tick_seq, 1)

    def test_trigger_and_plugin_events_are_queued(self) -> None:
        trigger = PvEvent("TEST:TRIGGER", 3)
        plugin = PvEvent("TEST:PLUGIN", 4)

        self.handler.on_trigger_event(trigger, self.subscription)
        self.handler.on_plugin_event(plugin, self.subscription)

        self.assertIs(self.context._trigger_q.get(), trigger)
        self.assertIs(self.context._plugin_q.get(), plugin)

    def test_sync_event_updates_controller_and_position(self) -> None:
        subscription = SubscriptionConfig(
            name="energy_sync",
            role="sync",
            actuator="energy",
            source="rbv",
        )
        event = PvEvent("ENERGY:RBV", "12.5")

        self.handler.on_sync_event(event, subscription)

        self.context.sync_controller.note_event.assert_called_once_with("energy_sync")
        self.assertEqual(self.context._position, 12.5)
        self.assertTrue(self.context._position_sync_subscription_set)

    def test_sync_event_preserves_raw_value_when_not_numeric(self) -> None:
        subscription = SubscriptionConfig(
            name="energy_sync",
            role="sync",
            actuator="energy",
            source="rbv",
        )
        event = PvEvent("ENERGY:RBV", "moving")

        self.handler.on_sync_event(event, subscription)

        self.assertEqual(self.context._position, "moving")

    def test_sync_event_does_not_change_position_for_unrelated_source(self) -> None:
        subscription = SubscriptionConfig(
            name="other_sync",
            role="sync",
            pv="OTHER:SYNC",
        )
        event = PvEvent("OTHER:SYNC", 9.0)
        self.context.position_sync = False
        self.context._position = 3.0

        self.handler.on_sync_event(event, subscription)

        self.assertEqual(self.context._position, 3.0)
        self.assertFalse(self.context._position_sync_subscription_set)

    def test_stop_event_only_stops_active_scan(self) -> None:
        event = PvEvent("TEST:STOP", 1)
        first = MagicMock()
        second = MagicMock()
        self.context.actuators = {"first": first, "second": second}

        self.handler.on_stop_event(event, self.subscription)
        self.assertFalse(self.context._stop_requested.is_set())
        first.stop.assert_not_called()
        second.stop.assert_not_called()

        self.context.busyflag = True
        self.handler.on_stop_event(event, self.subscription)

        self.assertTrue(self.context._stop_requested.is_set())
        self.context.sync_controller.wake.assert_called_once_with()
        first.stop.assert_called_once_with()
        second.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
