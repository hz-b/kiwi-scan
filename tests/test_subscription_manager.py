import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


from kiwi_scan.datamodels import ActuatorConfig, SubscriptionConfig
from kiwi_scan.actuator.single import PvEvent
import kiwi_scan.scan.subscription_manager as subscription_manager_module
from kiwi_scan.scan.subscription_manager import SubscriptionManager
from kiwi_scan.test_support import (
    FakeMonitorProvider,
    FakeNoMonitorBackend,
    make_fake_epics_pv_class,
)


FakeEpicsPV = make_fake_epics_pv_class()


class TestSubscriptionManager(unittest.TestCase):
    def setUp(self):
        FakeEpicsPV.instances.clear()

    def test_resolve_velocity_pv_prefers_get_velocity_pv(self):
        manager = SubscriptionManager(
            actuator_configs={
                "motor": {
                    "pv": "MOTOR:VAL",
                    "cmdvel_pv": "MOTOR:CMDVEL",
                    "velocity_pv": "MOTOR:VELO",
                    "get_velocity_pv": "MOTOR:GETVELO",
                }
            }
        )

        sub = SubscriptionConfig(
            name="velocity_sub",
            role="status",
            actuator="motor",
            source="velocity",
        )

        resolved = manager.resolve_pv(sub)

        self.assertEqual(resolved, "MOTOR:GETVELO")
        self.assertIsInstance(manager._actuator_configs["motor"], ActuatorConfig)

    def test_start_dispatch_and_stop_with_monitor_provider(self):
        provider = FakeMonitorProvider()
        received = []

        manager = SubscriptionManager(
            subscriptions=[
                SubscriptionConfig(
                    name="heartbeat_sub",
                    role="heartbeat",
                    actuator="motor",
                    source="rbv",
                )
            ],
            actuator_configs={
                "motor": {
                    "pv": "MOTOR:SET",
                    "rb_pv": "MOTOR:RBV",
                }
            },
            actuators={"motor": provider},
        )

        def on_heartbeat(event, subscription):
            received.append((event, subscription.name))

        manager.register_role("heartbeat", on_heartbeat)
        manager.start()

        self.assertTrue(manager.started)
        self.assertIn("MOTOR:RBV", provider.callbacks_by_pv)

        provider.callbacks_by_pv["MOTOR:RBV"](
            PvEvent(pvname="MOTOR:RBV", value=12.5, source="simulated")
        )

        self.assertEqual(len(received), 1)
        event, sub_name = received[0]
        self.assertEqual(sub_name, "heartbeat_sub")
        self.assertEqual(event.pvname, "MOTOR:RBV")
        self.assertEqual(event.value, 12.5)
        self.assertEqual(event.source, "simulated")

        manager.stop()

        self.assertFalse(manager.started)
        self.assertEqual(provider.removed_pvs, ["MOTOR:RBV"])

    def test_start_falls_back_to_direct_epics_monitor(self):
        received = []

        manager = SubscriptionManager(
            subscriptions=[
                SubscriptionConfig(
                    name="stop_sub",
                    role="stop",
                    pv="SYS:STOP",
                )
            ],
            actuators={"motor": FakeNoMonitorBackend()},
        )

        def on_stop(event):
            received.append(event)

        manager.register_role("stop", on_stop)

        with patch.object(subscription_manager_module, "EpicsPV", FakeEpicsPV):
            manager.start()

            self.assertTrue(manager.started)
            self.assertEqual(len(FakeEpicsPV.instances), 1)

            handle = FakeEpicsPV.instances[0]
            self.assertEqual(handle.pvname, "SYS:STOP")
            self.assertTrue(handle.auto_monitor)
            self.assertEqual(len(handle.callbacks), 1)

            handle.callbacks[0](value=1, timestamp=123.0)

            self.assertEqual(len(received), 1)
            self.assertEqual(received[0].pvname, "SYS:STOP")
            self.assertEqual(received[0].value, 1)
            self.assertEqual(received[0].timestamp, 123.0)
            self.assertEqual(received[0].source, "epics_monitor")

            manager.stop()

            self.assertFalse(manager.started)
            self.assertTrue(handle.clear_callbacks_called)
            self.assertTrue(handle.disconnected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
