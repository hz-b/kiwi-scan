"""
Leak test for scan objects that are created and destroyed inside one process.

A long-running host (e.g. the generic scan IOC) creates many scan objects
without exiting. Every scan must release what it owns once it is closed and
dropped: worker threads, EpicsPV/pyepics PV objects, pyepics CA cache
callbacks and file descriptors. pyepics keeps channels in ``epics.ca._cache`` by name even
after a PV is disconnected, so the test compares growth across cycles after a
warm-up cycle instead of expecting absolute zero.

The test needs working PVs and runs against the internal U17/1 PGM1 digital
twin (prefix TESTU171PGM1). It is skipped when the IOC is not reachable.

Optional env vars:
  - KIWI_LEAKTEST_PREFIX (default TESTU171PGM1)  # IOC prefix without trailing colon
  - KIWI_LEAKTEST_CYCLES (int, default 3)       # measured create/destroy cycles
  - EPICS_WRITETEST=1                           # enables the run-scan variant (moves to current position)
  - EPICS_CONN_TIMEOUT (float, default 3.0)
"""

# TODO: Add a self-contained caproto IOC fixture (tests/ioc/mono_twin_ioc.py) that serves the
#       prefix below, so this test runs without the internal twin (e.g. in CI).

import gc
import logging
import os
import tempfile
import threading
import time
import unittest
import weakref
from typing import Any, Dict, List, Optional
from unittest import mock

import epics
import yaml

from kiwi_scan.data.manifestwriter import ManifestWriter
from kiwi_scan.datamodels import ScanConfig, ScanDimension
from kiwi_scan.epics_wrapper import EpicsPV
from kiwi_scan.scan.tools import create_scan_with_config

PREFIX = os.getenv("KIWI_LEAKTEST_PREFIX", "TESTU171PGM1").rstrip(":")

MONO_TWIN_YAML = r"""
actuators:
  energy:
    type: "epics"
    pv: "${PREFIX}:monoSetEnergy"
    rb_pv: "${PREFIX}:monoGetEnergy"
    status_pv: "${PREFIX}:GK_STATUS"
    velocity_pv: "${PREFIX}:SetCombinedVelocity"
    get_velocity_pv: "${PREFIX}:SetCombinedVelocity"
    queueing_delay: 0.01
    stop_pv: "${PREFIX}:MonoAktion.PROC"
    stop_command: 0
    dwell_time: 0.1
    ready_value: 0
  slit:
    type: "epics"
    pv: "${PREFIX}:SlitInput"
    rb_pv: "${PREFIX}:slitwidth"
    status_pv: "${PREFIX}:ES_STATUS"
    queueing_delay: 0.01
    stop_pv: "${PREFIX}:ES_0_AKTION"
    stop_command: 1
    ready_value: 0
    dwell_time: 0.1
    jog:
      velocity_pv: "${PREFIX}:ES_0_SETJOGSPEED"
      abs_velocity: True
      command_pv: "${PREFIX}:ES_0_AKTION"
      command_pos: 5
      command_neg: 6
detector_pvs:
  - "${PREFIX}:monoGetEnergy"
  - "${PREFIX}:Theta"
  - "${PREFIX}:Beta"
  - "${PREFIX}:Alpha"
metadata_pvs:
  - "${PREFIX}:Alpha"
subscriptions:
  - pv: "${PREFIX}:Theta"
    name: ioc_heartbeat
    role: heartbeat
"""

SETTLE_TIMEOUT_S = 3.0

logger = logging.getLogger(__name__)


def _make_config(positions: Dict[str, float], data_dir: str) -> ScanConfig:
    """Build a fresh ScanConfig (BaseScan mutates its config)."""
    cfg = ScanConfig.from_dict(yaml.safe_load(MONO_TWIN_YAML.replace("${PREFIX}", PREFIX)))
    cfg.scan_dimensions = [
        ScanDimension(actuator=name, start=pos, stop=pos, steps=2)
        for name, pos in positions.items()
    ]
    cfg.data_dir = data_dir
    return cfg


def _live(cls: type) -> List[Any]:
    return [o for o in gc.get_objects() if isinstance(o, cls)]


def _ca_cache_callbacks() -> int:
    """Connection + access-rights callbacks pyepics keeps per channel; each pins a PV object."""
    total = 0
    for ctx_cache in list(epics.ca._cache.values()):
        for item in list(ctx_cache.values()):
            total += len(getattr(item, "callbacks", []) or [])
            total += len(getattr(item, "access_event_callback", []) or [])
    return total


def _open_fds() -> Optional[int]:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return None


def _snapshot() -> Dict[str, Any]:
    gc.collect()
    pvs = _live(epics.PV)
    return {
        "threads": threading.active_count(),
        "EpicsPV objects": len(_live(EpicsPV)),
        "epics.PV objects": len(pvs),
        "epics.PV user callbacks": sum(len(pv.callbacks) for pv in pvs),
        "ca cache callbacks": _ca_cache_callbacks(),
        "open fds": _open_fds(),
    }


def _growth(baseline: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, int]:
    return {
        key: current[key] - baseline[key]
        for key in baseline
        if baseline[key] is not None and current[key] is not None and current[key] != baseline[key]
    }


def _stable_snapshot() -> Dict[str, Any]:
    """Snapshot once two consecutive snapshots agree (or after SETTLE_TIMEOUT_S)."""
    deadline = time.monotonic() + SETTLE_TIMEOUT_S
    previous = _snapshot()
    while time.monotonic() < deadline:
        time.sleep(0.1)
        current = _snapshot()
        if current == previous:
            return current
        previous = current
    return previous


def _wait_settled(baseline: Dict[str, Any]) -> Dict[str, int]:
    """Give threads time to exit; return what still grew after SETTLE_TIMEOUT_S."""
    deadline = time.monotonic() + SETTLE_TIMEOUT_S
    while True:
        growth = _growth(baseline, _snapshot())
        if not growth or time.monotonic() >= deadline:
            return growth
        time.sleep(0.1)


class TestScanLeakIntegration(unittest.TestCase):
    """Create and destroy scans repeatedly in one process and check that nothing accumulates."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.CYCLES = int(os.getenv("KIWI_LEAKTEST_CYCLES", "3"))
        cls.CONN_TIMEOUT = float(os.getenv("EPICS_CONN_TIMEOUT", "3.0"))
        cls.ENABLE_WRITE = os.getenv("EPICS_WRITETEST", "0").strip() == "1"

        # Probe the IOC and read the current positions; moves in the run variant go there.
        cls.POSITIONS: Dict[str, float] = {}
        for name, pvname in (("energy", f"{PREFIX}:monoGetEnergy"), ("slit", f"{PREFIX}:slitwidth")):
            try:
                pv = EpicsPV(pvname, connection_timeout=cls.CONN_TIMEOUT)
            except ConnectionError as exc:
                raise unittest.SkipTest(f"Mono twin IOC not reachable: {pvname} ({exc})") from exc
            try:
                value = pv.get(timeout=cls.CONN_TIMEOUT)
            finally:
                pv.close()
            if value is None:
                raise unittest.SkipTest(f"Could not read current position from {pvname}")
            cls.POSITIONS[name] = float(value)

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="kiwi-leaktest-")
        self.addCleanup(self._tmp.cleanup)
        # Keep manifest appends and the active-manifest state file away from the user's setup.
        env = mock.patch.dict(
            os.environ,
            {
                ManifestWriter.ENV_MANIFEST_FILE: os.path.join(self._tmp.name, "manifest.yaml"),
                ManifestWriter.ENV_MANIFEST_STATE_FILE: os.path.join(self._tmp.name, "active_manifest"),
                "KIWI_SCAN_DATA_DIR": self._tmp.name,
            },
        )
        env.start()
        self.addCleanup(env.stop)
        ManifestWriter.newmanifest(filename=os.environ[ManifestWriter.ENV_MANIFEST_FILE])

    def _cycle(self, run_scan: bool) -> "weakref.ref[Any]":
        """Create one scan, optionally run it, close and drop it. Returns a weakref to the dropped scan."""
        data_dir = tempfile.mkdtemp(dir=self._tmp.name)
        scan = create_scan_with_config("linear", _make_config(self.POSITIONS, data_dir), data_dir)
        if scan is None:
            # Not an AssertionError on purpose: setup problems must not count as the expected failure.
            raise RuntimeError("create_scan_with_config() returned None; see log for the cause")
        with scan:
            if run_scan:
                scan.execute()
        ref = weakref.ref(scan)
        del scan
        return ref

    def _assert_no_leak(self, run_scan: bool) -> None:
        self._cycle(run_scan)  # warm-up: lazy imports, CA context, channel cache entries
        baseline = _stable_snapshot()
        baseline_threads = {t.ident for t in threading.enumerate()}

        refs = [self._cycle(run_scan) for _ in range(self.CYCLES)]
        growth = _wait_settled(baseline)
        alive = sum(ref() is not None for ref in refs)

        problems = []
        if alive:
            problems.append(f"scan objects still alive: {alive}/{self.CYCLES}")
        for key, delta in growth.items():
            problems.append(f"{key}: +{delta} after {self.CYCLES} cycles (+{delta / self.CYCLES:g} per scan)")
        if growth.get("threads"):
            names = sorted(t.name for t in threading.enumerate() if t.ident not in baseline_threads)
            problems.append(f"new threads: {names}")

        if problems:
            self.fail("Scan create/destroy leaks resources:\n  " + "\n  ".join(problems))

    def test_create_destroy_does_not_leak(self):
        """Create scans and drop them without running. No writes to the IOC."""
        self._assert_no_leak(run_scan=False)

    def test_create_run_destroy_does_not_leak(self):
        """
        Create scans, run a 2-point scan at the current positions and drop them.
        Enable with EPICS_WRITETEST=1 (the scan writes the current positions back).
        """
        if not self.ENABLE_WRITE:
            raise unittest.SkipTest("Set EPICS_WRITETEST=1 to enable the run-scan leak test")
        self._assert_no_leak(run_scan=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
