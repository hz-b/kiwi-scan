import os
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from kiwi_scan.actuator.factory import create_actuator
from kiwi_scan.actuator_concrete.single_epics import EpicsActuator
from kiwi_scan.actuator_concrete.single_simulation import SimulatedActuator
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.yaml_loader import yaml_loader
# import kiwi_scan.test_support as test_support
from kiwi_scan.test_support import make_fake_monitor_pv_class


class TestSimulatedActuatorMonitor(unittest.TestCase):

    def setUp(self):
        self.act = SimulatedActuator(
            ActuatorConfig(
                pv="SIM:ACT",
                dwell_time=0.0,
            )
        )

    def test_simulated_actuator_does_not_support_monitors(self):
        self.assertFalse(self.act.supports_monitors())

    def test_simulated_actuator_rejects_monitor_registration(self):
        with self.assertRaisesRegex(
            NotImplementedError,
            "does not support PV monitors",
        ):
            self.act.add_monitor("PV:HEARTBEAT")

    def test_create_simulated_actuator_from_yaml(self):
        yaml_text = textwrap.dedent(
            """
            actuators:
              theta:
                type: sim
                pv: "SIM:THETA"
                rb_pv: "${IOC}:mono:ThetaRBV"
                status_pv: "${IOC}:mono:ThetaStatus"
            """
        ).strip()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = os.path.join(td, "scan.yaml")
            with open(cfg_path, "w", encoding="utf-8") as stream:
                stream.write(yaml_text)

            data = yaml_loader(cfg_path, replacements={"IOC": "TESTIOC"})

        cfg = ActuatorConfig.from_dict(data["actuators"]["theta"])

        self.assertEqual(cfg.rb_pv, "TESTIOC:mono:ThetaRBV")
        self.assertEqual(cfg.status_pv, "TESTIOC:mono:ThetaStatus")
        self.assertEqual(cfg.pv, "SIM:THETA")
        self.assertEqual(cfg.type, "sim")

        act = create_actuator(cfg)
        self.assertIsInstance(act, SimulatedActuator)
        self.assertFalse(act.supports_monitors())


class TestEpicsActuatorMonitor(unittest.TestCase):
    """ Test monitor backend (PV) """

    def setUp(self):
        self.fake_pv_class = make_fake_monitor_pv_class()
        patcher = patch(
            "kiwi_scan.actuator_concrete.single_epics.EpicsPV",
            self.fake_pv_class,
        )
        self.addCleanup(patcher.stop)
        patcher.start()

        self.act = EpicsActuator(
            ActuatorConfig(
                pv="PV:ACT",
                dwell_time=0.0,
            )
        )

    @staticmethod
    def _callback_index(handle):
        return next(iter(handle.callbacks))

    def test_monitor_dispatch_stores_last_event_and_calls_listener(self):
        pvname = "PV:HEARTBEAT"
        received = []

        handle = self.act.add_monitor(pvname, user_callback=received.append)
        callback_index = self._callback_index(handle)

        handle.trigger(
            callback_index,
            value=123,
            timestamp=12.5,
            posixseconds=12,
            nanoseconds=500_000_000,
            severity=1,
            status=0,
        )

        self.assertEqual(len(received), 1)
        event = received[0]
        self.assertEqual(event.pvname, pvname)
        self.assertEqual(event.value, 123)
        self.assertEqual(event.timestamp, 12.5)
        self.assertEqual(event.posixseconds, 12)
        self.assertEqual(event.nanoseconds, 500_000_000)
        self.assertEqual(event.severity, 1)
        self.assertEqual(event.status, 0)
        self.assertEqual(event.source, "epics_monitor")
        self.assertIs(self.act.get_last_event(pvname), event)

    def test_multiple_callbacks_are_all_called(self):
        pvname = "PV:RBV"
        calls = []

        handle = self.act.add_monitor(pvname)
        self.act.on_pv_event(pvname, lambda event: calls.append("cb1"))
        self.act.on_pv_event(pvname, lambda event: calls.append("cb2"))

        handle.trigger(self._callback_index(handle), value=1.0)

        self.assertEqual(calls, ["cb1", "cb2"])

    def test_remove_monitor_clears_bookkeeping_and_detaches_callback(self):
        pvname = "PV:CMD"
        received = []

        handle = self.act.add_monitor(pvname, user_callback=received.append)
        callback_index = self._callback_index(handle)
        raw_pv = handle._pv

        handle.trigger(callback_index, value=7)
        self.assertEqual(len(received), 1)
        self.assertIsNotNone(self.act.get_last_event(pvname))

        self.act.remove_monitor(pvname)

        self.assertIsNone(self.act.get_last_event(pvname))
        self.assertNotIn(pvname, self.act._monitors)
        self.assertNotIn(pvname, self.act._monitor_callbacks)
        self.assertNotIn(pvname, self.act._epics_cb_indices)
        self.assertEqual(raw_pv.removed, [callback_index])
        self.assertTrue(raw_pv.disconnected)

    def test_clear_monitors_removes_all(self):
        handles = {}
        for pvname in ("PV:A", "PV:B", "PV:C"):
            handle = self.act.add_monitor(pvname)
            handle.trigger(self._callback_index(handle), value=pvname)
            handles[pvname] = handle

        self.act.clear_monitors()

        self.assertEqual(self.act._monitors, {})
        self.assertEqual(self.act._monitor_callbacks, {})
        self.assertEqual(self.act._epics_cb_indices, {})

        for pvname, handle in handles.items():
            self.assertIsNone(self.act.get_last_event(pvname))
            self.assertTrue(handle._pv.disconnected)
            self.assertEqual(len(handle._pv.removed), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
