import unittest
import os
import tempfile
import textwrap
from dataclasses import is_dataclass
import time
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.actuator_concrete.single_simulation import SimulatedActuator
from kiwi_scan.yaml_loader import yaml_loader
from kiwi_scan.actuator.factory import create_actuator


class TestActuatorMonitor(unittest.TestCase):
    def setUp(self):
        self.act = SimulatedActuator(ActuatorConfig(pv="SIM:ACT"))

    def tearDown(self):
        # simulated actuator has:
        try:
            self.act.stop_all_pv_generators()
        except Exception:
            pass

    def _event_value(self, ev):
        return ev.value if is_dataclass(ev) else ev.get("value")

    def _event_timestamp(self, ev):
        if is_dataclass(ev):
            return getattr(ev, "timestamp", None)
        return ev.get("timestamp")

    def _wait_for(self, predicate, timeout: float = 1.0, step: float = 0.01) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(step)
        return False

    def _assert_event(self, ev, pvname, value):
        if is_dataclass(ev):
            self.assertEqual(ev.pvname, pvname)
            self.assertEqual(ev.value, value)
        else:
            self.assertEqual(ev.get("pvname") or ev.get("pv"), pvname)
            self.assertEqual(ev.get("value"), value)

    def test_monitor_dispatch_stores_last_event_and_calls_listener(self):
        pvname = "PV:HEARTBEAT"

        received = []

        def cb(ev):
            received.append(ev)

        self.act.add_monitor(pvname, user_callback=cb)

        self.act._dispatch_pv_update(
            pvname,
            123,
            timestamp=12.5,
            posixseconds=12,
            nanoseconds=500_000_000,
            severity=1,
            status=0,
        )

        self.assertEqual(len(received), 1)
        self._assert_event(received[0], pvname, 123)

        last = self.act.get_last_event(pvname)
        self.assertIsNotNone(last)
        self._assert_event(last, pvname, 123)

        if is_dataclass(last):
            self.assertEqual(last.timestamp, 12.5)
            self.assertEqual(last.posixseconds, 12)
            self.assertEqual(last.nanoseconds, 500_000_000)
            self.assertEqual(last.severity, 1)
            self.assertEqual(last.status, 0)

    def test_multiple_callbacks_are_all_called(self):
        pvname = "PV:RBV"
        calls = []

        def cb1(ev):
            calls.append("cb1")

        def cb2(ev):
            calls.append("cb2")

        self.act.add_monitor(pvname)
        self.act.on_pv_event(pvname, cb1)
        self.act.on_pv_event(pvname, cb2)

        self.act._dispatch_pv_update(pvname, 1.0)

        self.assertEqual(calls.count("cb1"), 1)
        self.assertEqual(calls.count("cb2"), 1)

    def test_remove_monitor_clears_last_event_and_callbacks(self):
        pvname = "PV:CMD"
        calls = []

        def cb(ev):
            calls.append(ev)

        self.act.add_monitor(pvname, user_callback=cb)

        self.act._dispatch_pv_update(pvname, 7)
        self.assertIsNotNone(self.act.get_last_event(pvname))
        self.assertEqual(len(calls), 1)

        self.act.remove_monitor(pvname)
        # last event should be cleared
        self.assertIsNone(self.act.get_last_event(pvname))
        # and callback should no longer be called
        self.act._dispatch_pv_update(pvname, 8)
        self.assertEqual(len(calls), 1)

    def test_clear_monitors_removes_all(self):
        pvs = ["PV:A", "PV:B", "PV:C"]
        for pv in pvs:
            self.act.add_monitor(pv)

        self.act.clear_monitors()

        for pv in pvs:
            self.assertIsNone(self.act.get_last_event(pv))

        # listeners should be gone:
        called = []

        def cb(ev):
            called.append(ev)

        self.act.add_monitor("PV:A", user_callback=cb)
        self.act.clear_monitors()
        self.act._dispatch_pv_update("PV:A", 1)
        self.assertEqual(called, [])

    def test_pv_generator_emits_periodic_events(self):
        pvname = "PV:HEARTBEAT"
        received = []

        def cb(ev):
            received.append(ev)

        self.act.add_monitor(pvname, user_callback=cb)

        rate_hz = 20.0
        self.act.start_pv_generator(pvname, rate_hz=rate_hz, include_counter=True)

        ok = self._wait_for(lambda: len(received) >= 5, timeout=1.0)
        self.assertTrue(ok, f"Expected >=5 events within timeout, got {len(received)}")

        # Values should be monotonic counters starting at 0 (default behavior)
        vals = [self._event_value(ev) for ev in received[:5]]
        self.assertEqual(vals, list(range(5)))

        # Rough check of rate using timestamps 
        ts0 = self._event_timestamp(received[0])
        tsN = self._event_timestamp(received[4])
        if ts0 is not None and tsN is not None and tsN > ts0:
            measured = 4.0 / (tsN - ts0)
            self.assertGreater(measured, rate_hz * 0.25)
            self.assertLess(measured, rate_hz * 4.0)

    def test_stop_pv_generator_stops_new_events(self):
        pvname = "PV:STOPTEST"
        received = []

        def cb(ev):
            received.append(ev)

        self.act.add_monitor(pvname, user_callback=cb)
        self.act.start_pv_generator(pvname, rate_hz=30.0, include_counter=True)

        self.assertTrue(self._wait_for(lambda: len(received) >= 3, timeout=1.0))

        self.act.stop_pv_generator(pvname)

        time.sleep(0.05)
        n1 = len(received)
        time.sleep(0.25)
        n2 = len(received)

        self.assertEqual(n2, n1, "No new events should arrive after stop_pv_generator")

    def test_remove_monitor_stops_generator_for_that_pv(self):
        pvname = "PV:REMOVETEST"
        received = []

        def cb(ev):
            received.append(ev)

        self.act.add_monitor(pvname, user_callback=cb)
        self.act.start_pv_generator(pvname, rate_hz=25.0, include_counter=True)

        self.assertTrue(self._wait_for(lambda: len(received) >= 3, timeout=1.0))

        self.act.remove_monitor(pvname)

        time.sleep(0.05)
        n1 = len(received)
        time.sleep(0.25)
        n2 = len(received)
        self.assertEqual(n2, n1, "remove_monitor() should stop generator and callbacks")

    # TODO: Move functionality into actuator class, EpicsActuator will have PV objects here
    def _resolve_monitor_pv(self, spec, cfg: ActuatorConfig) -> str:
        """
        Resolve a monitor spec (either {pv: ...} or {source: rbv/status/...})
        to a concrete pvname string.
        """
        if "pv" in spec and spec["pv"]:
            return spec["pv"]

        source = spec.get("source")
        if source == "rbv":
            if not cfg.rb_pv:
                raise AssertionError("monitor source=rbv requires cfg.rb_pv")
            return cfg.rb_pv
        if source == "status":
            if not cfg.status_pv:
                raise AssertionError("monitor source=status requires cfg.status_pv")
            return cfg.status_pv
        if source == "cmd":
            if not cfg.cmd_pv:
                raise AssertionError("monitor source=cmd requires cfg.cmd_pv")
            return cfg.cmd_pv
        if source == "velocity":
            if not cfg.velocity_pv:
                raise AssertionError("monitor source=velocity requires cfg.velocity_pv")
            return cfg.velocity_pv

        raise AssertionError(f"Unsupported monitor spec: {spec!r}")

    def _apply_monitors_from_yaml(self, act: SimulatedActuator, cfg: ActuatorConfig, monitors):
        """
        Minimal (BaseScan):
        iterate monitor specs and call act.add_monitor(pvname, user_callback=...).
        Returns a dict pvname -> list[events] for assertions.
        """
        received = {}

        for spec in monitors:
            pvname = self._resolve_monitor_pv(spec, cfg)

            def _cb(ev, _pv=pvname):
                received.setdefault(_pv, []).append(ev)

            act.add_monitor(pvname, user_callback=_cb)

        return received

    def test_create_actuator_from_yaml_and_configure_monitors(self):
        yaml_text = textwrap.dedent(
            """
            actuators:
              gap:
                type: sim
                pv: "SIM:GAP"
                rb_pv: "${IOC}:ID:GapRBV"
                status_pv: "${IOC}:ID:GapMsta"

                monitors:
                  - source: rbv
                    name: gap_rbv
                  - source: status
                    name: gap_msta
                  - pv: "${IOC}:SYS:Heartbeat"
                    name: ioc_heartbeat
            """
        ).strip()

        with tempfile.TemporaryDirectory() as td:
            cfg_path = os.path.join(td, "scan.yaml")
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(yaml_text)

            data = yaml_loader(cfg_path, replacements={"IOC": "TESTIOC"})

        gap_raw = data["actuators"]["gap"]
        self.assertIn("monitors", gap_raw)
        self.assertIsInstance(gap_raw["monitors"], list)
        self.assertEqual(len(gap_raw["monitors"]), 3)

        # --- Create ActuatorConfig + actuator instance ----------------------
        cfg = ActuatorConfig.from_dict(gap_raw)

        # Check replacements
        self.assertEqual(cfg.rb_pv, "TESTIOC:ID:GapRBV")
        self.assertEqual(cfg.status_pv, "TESTIOC:ID:GapMsta")
        self.assertEqual(cfg.pv, "SIM:GAP")
        self.assertEqual(cfg.type, "sim")

        act = create_actuator(cfg)
        self.assertIsInstance(act, SimulatedActuator)
        self.assertTrue(act.supports_monitors())

        received = self._apply_monitors_from_yaml(act, cfg, gap_raw["monitors"])

        hb_pv = "TESTIOC:SYS:Heartbeat"

        act.publish_pv(cfg.rb_pv, 1.23, timestamp=1.0)
        act.publish_pv(cfg.status_pv, 42, timestamp=2.0)
        act.publish_pv(hb_pv, 7, timestamp=3.0)

        # callbacks called exactly once per PV
        self.assertEqual(len(received[cfg.rb_pv]), 1)
        self.assertEqual(len(received[cfg.status_pv]), 1)
        self.assertEqual(len(received[hb_pv]), 1)

        # last-event storage updated
        ev_rbv = act.get_last_event(cfg.rb_pv)
        ev_msta = act.get_last_event(cfg.status_pv)
        ev_hb = act.get_last_event(hb_pv)

        self.assertIsNotNone(ev_rbv)
        self.assertIsNotNone(ev_msta)
        self.assertIsNotNone(ev_hb)

        self.assertEqual(ev_rbv.value, 1.23)
        self.assertEqual(ev_msta.value, 42)
        self.assertEqual(ev_hb.value, 7)



if __name__ == "__main__":
    unittest.main(verbosity=2)

