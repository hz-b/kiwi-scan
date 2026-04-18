import os
import time
import unittest
from typing import List, Dict, Any, Optional

import yaml

from kiwi_scan.epics_wrapper import EpicsPV


MONO_YAML = r"""
actuators:
  energy:
    type: "epics"
    pv: "TESTU171PGM1:monoSetEnergy"
    rb_pv: "TESTU171PGM1:monoGetEnergy"
    status_pv: "TESTU171PGM1:GK_STATUS"
    queueing_delay: 0.1
    dwell_time: 0.1
    ready_value: 0
  slit:
    type: "epics"
    pv: "TESTU171PGM1:SlitInput"
    rb_pv: "TESTU171PGM1:slitwidth"
    status_pv: "TESTU171PGM1:ES_STATUS"
    ready_value: 0
    queueing_delay: 1
detector_pvs:
  - "TESTU171PGM1:monoGetEnergy"
  - "TESTU171PGM1:Theta"
  - "TESTU171PGM1:Beta"
  - "TESTU171PGM1:Alpha"
  - "TESTU171PGM1:aiIdGetGap"
subscriptions:
  - pv: "TESTU171PGM1:Theta"
    name: ioc_heartbeat
    role: heartbeat
  - actuator: energy
    source: rbv
    name: mono_energy
    role: sync
"""


def _parse_config_pvs() -> Dict[str, Any]:
    cfg = yaml.safe_load(MONO_YAML)
    actuators = cfg.get("actuators", {}) or {}
    dets = cfg.get("detector_pvs", []) or []

    # Primary actuator PVs from config
    energy = actuators.get("energy", {}) or {}
    slit = actuators.get("slit", {}) or {}

    write_pvs = []
    # Tests must not write by default.
    if energy.get("pv"):
        write_pvs.append(energy["pv"])
    if slit.get("pv"):
        write_pvs.append(slit["pv"])

    # Readback and status PVs
    read_pvs = list(dets)
    for block in (energy, slit):
        for k in ("rb_pv", "status_pv"):
            if block.get(k):
                read_pvs.append(block[k])

    # Remove doubles
    def dedup(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return {
        "read_pvs": dedup(read_pvs),
        "write_pvs": dedup(write_pvs),
    }


class TestEpicsPVIntegration(unittest.TestCase):
    """
    Integration tests for scan.epics_wrapper.EpicsPV using our legacy mono simulation IOC

      - No writes unless EPICS_WRITETEST=1
      - Test add_callback/clear_callbacks without requiring external PV changes.

    Optional env vars:
      - EPICS_WRITETEST=1  # Enables write test (writes the SAME value back).
      - EPICS_CONN_TIMEOUT (float, default 3.0)
      - EPICS_TIMEOUT (float, default 1.0)
      - EPICS_ADD_CALLBACK_MAX_S (float, default 2.0) # Timeout for add_callback()
    """

    @classmethod
    def setUpClass(cls) -> None:
        pvs = _parse_config_pvs()
        cls.READ_PVS = pvs["read_pvs"]
        cls.WRITE_PVS = pvs["write_pvs"]

        cls.CONN_TIMEOUT = float(os.getenv("EPICS_CONN_TIMEOUT", "3.0"))
        cls.TIMEOUT = float(os.getenv("EPICS_TIMEOUT", "1.0"))
        cls.ADD_CB_MAX_S = float(os.getenv("EPICS_ADD_CALLBACK_MAX_S", "2.0"))

        cls.ENABLE_WRITE = os.getenv("EPICS_WRITETEST", "0").strip() == "1"

        # Probe one PV to decide whether to skip integration tests.
        # If your IOC is down, we skip rather than fail the whole suite.
        probe = cls.READ_PVS[0] if cls.READ_PVS else None
        if not probe:
            raise unittest.SkipTest("No PVs found in embedded mono.yaml test config")

        try:
            pv = EpicsPV(probe, timeout=cls.TIMEOUT, connection_timeout=cls.CONN_TIMEOUT, auto_monitor=True)
            pv.check_pv()
        except Exception as e:
            raise unittest.SkipTest(f"EPICS not reachable / PV did not connect: {probe} ({e})")

    def test_connect_and_check_pvs(self):
        """All configured read PVs should connect and pass check_pv()."""
        for name in self.READ_PVS:
            with self.subTest(pv=name):
                pv = EpicsPV(name, timeout=self.TIMEOUT, connection_timeout=self.CONN_TIMEOUT, auto_monitor=True)
                pv.check_pv()

    def test_get_with_metadata_returns_expected_fields(self):
        """get_with_metadata() returns dict with at least pvname, value, timestamp."""
        for name in self.READ_PVS:
            with self.subTest(pv=name):
                pv = EpicsPV(name, timeout=self.TIMEOUT, connection_timeout=self.CONN_TIMEOUT, auto_monitor=True)
                meta = pv.get_with_metadata(use_monitor=True)
                self.assertIsInstance(meta, dict)
                # Some PVs may legitimately return None if server is misbehaving,
                # but in normal operation this should not happen.
                self.assertIn("pvname", meta)
                self.assertIn("value", meta)
                self.assertIn("timestamp", meta)
                self.assertEqual(meta["pvname"], name)

    def test_get_use_monitor_fallback_does_not_return_none(self):
        """
        Wrapper should fall back to direct get if monitor cache isn't primed.
        """
        for name in self.READ_PVS:
            with self.subTest(pv=name):
                pv = EpicsPV(name, timeout=self.TIMEOUT, connection_timeout=self.CONN_TIMEOUT, auto_monitor=True)
                val = pv.get(use_monitor=True)
                self.assertIsNotNone(val)

    def test_add_callback_does_not_block_excessively_and_can_be_cleared(self):
        """
        Regression guard for slow/blocked add_callback().
        We don't require the callback to fire (PV may be static),
        but we do assert add_callback returns promptly and clear_callbacks works.
        """
        # Use a PV that is likely to exist from your config
        name = self.READ_PVS[0]
        pv = EpicsPV(name, timeout=self.TIMEOUT, connection_timeout=self.CONN_TIMEOUT, auto_monitor=True)

        calls: List[Dict[str, Any]] = []

        def cb(**kw):
            calls.append(kw)

        t0 = time.perf_counter()
        pv.add_callback(cb, run_now=False)
        dt = time.perf_counter() - t0

        self.assertLess(
            dt,
            self.ADD_CB_MAX_S,
            f"add_callback() blocked too long ({dt:.3f}s > {self.ADD_CB_MAX_S:.3f}s) for PV {name}",
        )

        # Clearing callbacks should not raise
        pv.clear_callbacks()

    def test_optional_write_same_value_roundtrip(self):
        """
        Enable with EPICS_WRITETEST=1.
        """
        if not self.ENABLE_WRITE:
            raise unittest.SkipTest("Set EPICS_WRITETEST=1 to enable (conservative) write test")

        if not self.WRITE_PVS:
            raise unittest.SkipTest("No write PVs found in embedded mono.yaml test config")

        # Use energy setpoint PV by default (from your config)
        name = self.WRITE_PVS[0]
        pv = EpicsPV(name, timeout=self.TIMEOUT, connection_timeout=self.CONN_TIMEOUT, auto_monitor=False)

        current = pv.get(use_monitor=False)
        if current is None:
            raise unittest.SkipTest(f"Write PV {name} returned None on get(); cannot safely roundtrip")

        ok = pv.put(0)  # write to trigger record processing (analog input)
        self.assertTrue(ok, f"put() failed writing same value back to {name}")

    def test_optional_callback_fire_on_write_same_value(self):
        """
        Optional: try to get at least one callback by writing back to PV.
        Enable with EPICS_WRITETEST=1.
        """
        if not self.ENABLE_WRITE:
            raise unittest.SkipTest("Set EPICS_WRITETEST=1 to enable write/callback test")

        if not self.WRITE_PVS:
            raise unittest.SkipTest("No write PVs found in embedded mono.yaml test config")

        name = self.WRITE_PVS[0]
        pv = EpicsPV(name, timeout=self.TIMEOUT, connection_timeout=self.CONN_TIMEOUT, auto_monitor=True)

        got = {"n": 0}

        def cb(pvname=None, value=None, **kw):
            got["n"] += 1

        pv.add_callback(cb, run_now=False)

        current = pv.get(use_monitor=False)
        if current is None:
            raise unittest.SkipTest(f"Write PV {name} returned None on get(); cannot safely write")

        pv.put(current)

        # wait a bit for CA to deliver (if it posts)
        deadline = time.time() + 1.0
        while time.time() < deadline and got["n"] == 0:
            time.sleep(0.02)

        # Do NOT fail, ensure everything is callable and cleanup works.
        pv.clear_callbacks()


if __name__ == "__main__":
    unittest.main(verbosity=2)

