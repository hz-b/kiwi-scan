# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import time

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.range_exit_detector import RangeExitDetector

logger = logging.getLogger(__name__)

class PollScan(BaseScan):
    """
    Poll detector values while the primary actuator is within [start, stop].
    TODO: range check for all actuators.

    Event Roles::
      - TODO: this heardbeat can be fully replaced by sync subscripts 
        If a heartbeat subscription (role="heartbeat") is configured, each heartbeat
        wakes the loop immediately (event-driven scan).
      - The configured poll interval (self.sampletime) becomes a timeout, so the loop
        still runs periodically even if heartbeat stalls or isn't configured.
    """

    def __init__(self, config: ScanConfig, data_dir=None):
        super().__init__(config, data_dir)

        if not self.scan_dimensions:
            raise ValueError("PollScan requires at least one ScanDimension")

        logger.info("Creating samplerate from scan dimensions: %s", self.scan_dimensions)
        self.set_samplerate()
        self._start = self.scan_dimensions[0].start
        self._stop = self.scan_dimensions[0].stop

        self.register_subscription_role("heartbeat", self._on_heartbeat_event)
        self.register_subscription_role("sync", self._on_sync_event)
        self.register_subscription_role("status", self._on_status_event)
        self.register_subscription_role("stop", self._on_stop_event)
        
        self._maxindex = 0
        if self.scan_dimensions:
            self._maxindex = self.scan_dimensions[0].steps

    def _on_sync_event(self, ev: PvEvent, subscription=None) -> None:
        """
        Record sync events for the SyncController. Only the primary actuator
        RBV-style sync source updates self._position.
        """
        self._last_sync = ev
        self.sync_controller.note_event(getattr(subscription, "name", None))

        if self._is_position_sync_subscription(subscription):
            try:
                self._position = float(ev.value)
            except (TypeError, ValueError):
                self._position = ev.value
            self._position_sync_subscription_set = True

        logger.debug(
            "[sync] %s=%r -> _position=%r (source=%r, sub=%s)",
            ev.pvname,
            ev.value,
            self._position,
            ev.source,
            getattr(subscription, "name", None),
        )

    def scan(self, positions, monitor: BaseMonitor = None):
        """
        Poll detector values.
        Now synchronized by heartbeat events when available, with poll timeout fallback.
        """

        self.write_header_to_output_file()
        index = 0

        first_actuator = self.actuators[self.scan_dimensions[0].actuator]
        self._start_metadata_monitor()
        self._fire_triggers("before")
        range_exit = RangeExitDetector(
            self._start,
            self._stop,
            eps=0.001,
            out_threshold=6,
        )
        while not first_actuator.is_moving():
            logger.debug("Wait for actuator to start")
            time.sleep(0.05)

        self._stop_requested.clear()
        try:
            self._start_subscriptions()
            self.busyflag = True
            self._position_sync_subscription_set = False
            while True:
                if self._stop_requested.is_set():
                    logger.debug("Stop event set")
                    break
                if self.get_stop_pv() == 1:
                    logger.debug("Stop PV set")
                    break

                # Start a new sync cycle, then wait for heartbeat and all
                # configured sync-role subscriptions.
                self._arm_sync_controller()

                if self._stop_requested.is_set():
                    logger.debug("Stop event set")
                    break

                self._wait_for_sync(stop_event=self._stop_requested)

                if self._stop_requested.is_set():
                    logger.debug("Stop event set")
                    break

                # Prefer subscribed position if sync role is configured, else read rbv
                pos = self._position
                if not self._position_sync_subscription_set:
                    pos = first_actuator.rbv
                    self._position = pos
                    self._position_sync_subscription_set = False
                if pos == None:
                    continue

                current_position = pos
                
                range_exit_detected = range_exit.update(pos) 
                first_actuator_ready = first_actuator.is_ready() 
                if range_exit_detected and first_actuator_ready and self._start != self._stop:
                    logger.info("Scan termination detected at pos=%s", pos)
                    break
                if first_actuator_ready:
                    continue

                self._fire_triggers("on_point")
                # logger.debug("Read detectors")
                vals = self.read_detectors()
                self.update_current_row_cache(
                    idx=index,
                    pos=current_position,
                    values=vals,
                )
                self._fire_triggers("after_point")
                # plugin data
                plugin_data = []
                for plugin in self.plugins:
                    data = plugin.on_scan_point(index, current_position)
                    plugin_data += data
                    self.extend_current_row_cache(plugin.get_headers(False), data)

                vals = vals + plugin_data
                self.save_to_file(current_position, vals, self.include_timestamps)

                # >>> Notify monitor/plotter
                if monitor is not None:
                    monitor.update(vals)

                index += 1
                logger.debug("Poll %d @ pos=%r", index, current_position)

                # refresh from actuator rbv if no sync subscription is used
                # (keeps range check honest for non-subscribed setups)
                if self._last_sync is None:
                    current_position = first_actuator.rbv
                    self._position = current_position
                
                if self._maxindex > 0 and index >= self._maxindex:
                    super().stop()
                    break

                
        finally:
            self._stop_metadata_monitor()
            if monitor is not None:
                monitor.close()
            # IMPORTANT: PollScan doesn't use BaseScan.scan(), so we must clean up subscriptions here.
            try:
                self._clear_subscriptions()
            except Exception:
                logger.exception("Error clearing scan subscriptions")

            self._fire_triggers("after")
            self.busyflag = False

    def execute(self) -> None:
        self._execute_standard(None) 
