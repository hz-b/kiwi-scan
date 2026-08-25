# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from typing import Dict, List

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import ScanConfig, SubscriptionConfig
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.stats_collector import StatsCollector

logger = logging.getLogger(__name__)

class LinearScan(BaseScan):
    """
    Perform a simple linear scan for each configured actuator over its ScanDimension.
    """
    def __init__(self, config: ScanConfig, data_dir=None):
        super().__init__(config, data_dir)

        if not self.scan_dimensions:
            raise ValueError("LinearScan requires at least one ScanDimension")
        logger.debug(f"Creating scan data points from scan dimensions: {self.scan_dimensions}")
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
        
        self.register_subscription_role("heartbeat", self._on_heartbeat_event)
        self.register_subscription_role("stat", self._on_stat_event)
        self.register_subscription_role("status", self._on_status_event)
        self.register_subscription_role("stop", self._on_stop_event)
        self.register_subscription_role("trigger", self._on_trigger_event)
        self.register_subscription_role("plugin", self._on_plugin_event)

        # Generic provider columns: one stats group per stat subscription.
        self.stats_collector = StatsCollector(
            getattr(self.cfg, "subscriptions", None) or [],
            role="stat",
        )
        self.add_column_provider(self.stats_collector)

    def _on_stat_event(self, ev: PvEvent, subscription: SubscriptionConfig=None) -> None:
        """Record stat events and feed the per-subscription StatsCollector."""

        self.stats_collector.update(ev, subscription, collect=self._daq_is_on)
        logger.debug("[stat] {ev.pvname}={ev.value}, daq on: {self._daq_is_on}, {subscription.name}")

    def execute(self):
        self._execute_standard(self.positions)
