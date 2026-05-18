# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from typing import Dict, List

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.stats_collector import StatsCollector


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

        # Generic provider columns: one stats group per sync subscription.
        self.stats_collector = StatsCollector(
            getattr(self.cfg, "subscriptions", None) or [],
            role="sync",
        )
        self.add_column_provider(self.stats_collector)

    def _on_sync_event(self, ev: PvEvent, subscription=None) -> None:
        """Record sync events and feed the per-subscription StatsCollector."""
        self._last_sync = ev

        self.sync_controller.note_event(getattr(subscription, "name", None))

        if self._is_position_sync_subscription(subscription):
            try:
                self._position = float(ev.value)
            except Exception:
                self._position = ev.value

        self.stats_collector.update(
            ev,
            subscription,
            collect=bool(getattr(self, "_daq_is_on", False)),
        )

        logging.debug(
            "[sync] %s=%r daq=%s sub=%s",
            ev.pvname,
            ev.value,
            getattr(self, "_daq_is_on", False),
            getattr(subscription, "name", None),
        )

    def execute(self):
        self._execute_standard(self.positions)
