# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import threading
import queue
import math
from typing import List, Dict, Any, Optional
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.monitor.factory import create_monitor
from kiwi_scan.actuator.single import PvEvent
from kiwi_scan import stats
import epics

class LinearScan(BaseScan):
    """
    Perform a simple linear scan for each configured actuator over its ScanDimension.
    """
    def __init__(self, config: ScanConfig, data_dir=None):
        super().__init__(config, data_dir)

        if not self.scan_dimensions:
            raise ValueError("LinearScan requires at least one ScanDimension")
        logging.debug(f"Creating scan data points from scan dimensions: {self.scan_dimensions}")
        # build one linear positions array per actuator
        self.positions: Dict[str, List[float]] = {}
        for name in self.cfg.actuators:
            dim = next((d for d in self.scan_dimensions if d.actuator == name), None)
            if dim is None:
                raise ValueError(f"No ScanDimension for actuator '{name}'")

            # if only one step, just [start]
            if dim.steps < 2:
                series = [dim.start]
            else:
                step = (dim.stop - dim.start) / (dim.steps - 1)
                series = [dim.start + i * step for i in range(dim.steps)]

            self.positions[name] = series

        if self.debug:
            for nm, pts in self.positions.items():
                print(f"[DEBUG] LinearScan {nm}: {len(pts)} points "
                      f"from {pts[0]} to {pts[-1]}")
            print(f"{self.positions}")
        self.ROLE_CALLBACKS = {
            "heartbeat": self._on_heartbeat_event,
            "sync": self._on_sync_event,
            "status": self._on_status_event,
            "stop": self._on_stop_event,
            "trigger": self._on_trigger_event,
            "plugin": self._on_plugin_event,
        }
        self._last_heartbeat: Optional[PvEvent] = None
        self._last_sync: Optional[PvEvent] = None
        self._last_status: Optional[PvEvent] = None
        # Online stats for sync position samples (updated only while DAQ is on)
        self._mean_stat = stats.Mean()
        self._var_stat = stats.Var()
        self._daq_was_on = False

        # NEW: min/max/count for the current DAQ-on window
        self._sync_n = 0
        self._sync_min = None
        self._sync_max = None

        # Keep a default stats tuple (now 5 fields)
        self._stats = (0.0, 0.0, 0.0, 0.0, 0)
        # Creating trigger worker thread to avoid caput from callback context.
        self._trigger_q = queue.SimpleQueue()
        self._trigger_worker_stop = threading.Event()
        self._trigger_worker = threading.Thread(
            target=self._trigger_worker_loop,
            daemon=True,
        )
        self._trigger_worker.start()
        # Creating plugin worker thread to avoid caput from callback context.
        self._plugin_q = queue.SimpleQueue()
        self._plugin_worker_stop = threading.Event()
        self._plugin_worker = threading.Thread(
            target=self._plugin_worker_loop,
            daemon=True,
        )
        self._plugin_worker.start()

    # -------------------- callbacks --------------------
    def _on_trigger_event(self, ev: PvEvent, subscription=None) -> None:
        # Return immediately; do not call put() here
        self._trigger_q.put(ev)

    def _trigger_worker_loop(self) -> None:
        while not self._trigger_worker_stop.is_set():
            ev = self._trigger_q.get()
            try:
                self._fire_triggers("monitor")
            except Exception:
                logging.exception("WORKER: Failed to fire monitor triggers")

    def _on_plugin_event(self, ev: PvEvent, subscription=None) -> None:
        """
        If the PV emits a value, plugins are triggered.
        PvEvent data provided for the plugin hook.
        """
        self._plugin_q.put(ev)
    
    def _plugin_worker_loop(self) -> None:
        while not self._plugin_worker_stop.is_set():
            ev = self._plugin_q.get()
            # logging.debug("PLUGIN_WORKER: pv=%s", ev.pvname) 
            try:
                for plugin in self.plugins:
                    plugin.on_monitor(ev)
            except Exception:
                logging.exception("WORKER: Failed to run plugin")

    def _on_heartbeat_event(self, ev: PvEvent, subscription=None) -> None:
        self._last_heartbeat = ev
        logging.debug("[heartbeat] %s = %r (ts=%r)", ev.pvname, ev.value, ev.timestamp)

    def _on_sync_event(self, ev: PvEvent, subscription=None) -> None:
        """
        Sync callback: keep online mean/std/min/max/n of sync position samples.
        """
        self._last_sync = ev

        try:
            pos = float(ev.value)
        except Exception:
            logging.debug("[sync] %s value=%r not float-convertible", ev.pvname, ev.value)
            return

        # DAQ off: reset once on falling edge; expose mean=pos, std=0, min=max=pos, n=0
        if not self._daq_is_on:
            if self._daq_was_on:
                self._mean_stat = stats.Mean()
                self._var_stat = stats.Var()
                self._sync_n = 0
                self._sync_min = None
                self._sync_max = None

            self._stats = (pos, 0.0, pos, pos, 0)
            self._daq_was_on = False
            return

        # DAQ on: update online stats + min/max/count
        self._daq_was_on = True

        self._mean_stat.update(pos)
        self._var_stat.update(pos)

        self._sync_n += 1
        if self._sync_min is None or pos < self._sync_min:
            self._sync_min = pos
        if self._sync_max is None or pos > self._sync_max:
            self._sync_max = pos

        mean = float(self._mean_stat.get())
        var = float(self._var_stat.get() or 0.0)
        std = math.sqrt(var) if var > 0.0 else 0.0

        self._stats = (mean, std, float(self._sync_min), float(self._sync_max), int(self._sync_n))
        logging.info(
            "[sync] %s: value=%r, mean=%f, std=%f, min=%f, max=%f, n=%d, source=%s",
            ev.pvname, ev.value, mean, std, self._sync_min, self._sync_max, self._sync_n, ev.source
        )

    def _on_status_event(self, ev: PvEvent, subscription=None) -> None:
        self._last_status = ev
        logging.info("[status] %s = %r", ev.pvname, ev.value)

    def _on_stop_event(self, ev: PvEvent, subscription=None) -> None:
        """
        If the stop PV emits a value that should stop the scan, stop immediately.
        """
        logging.info("[stop] %s = %r -> stopping scan", ev.pvname, ev.value)
        try:
            for act in self.actuators.values():
                act.stop()
        except Exception:
            logging.exception("Error while stopping actuators on stop event")

    def execute(self):
        """
        Execute the linear scan over the pre-defined positions.
        """
        monitor = create_monitor(self.cfg)
        if monitor is not None: 
            monitor.start(self.cfg.detector_pvs)

        def _run_scan():
            try:
                epics.ca.use_initial_context()
            except Exception:
                pass
            self.scan(self.positions, monitor)

        scan_thread = threading.Thread(target=_run_scan)
        scan_thread.start()
        if monitor is not None:
            monitor.loop()
        scan_thread.join()
