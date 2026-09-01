# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import time
from typing import Optional

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

    @staticmethod
    def _wait_for_motion_start(actuator) -> None:
        """Wait until the externally commanded actuator begins moving."""
        while not actuator.is_moving():
            logger.debug("Wait for actuator to start")
            time.sleep(0.05)

    def _scan_stop_requested(self) -> bool:
        """Return whether an event or the configured stop PV ended the scan."""
        if self._stop_requested.is_set():
            logger.debug("Stop event set")
            return True
        if self.get_stop_pv() != 1:
            return False
        logger.debug("Stop PV set")
        return True

    def _wait_for_scan_cycle(self) -> bool:
        """Arm and wait for one synchronized acquisition cycle."""
        self._arm_sync_controller()
        if self._stop_requested.is_set():
            logger.debug("Stop event set")
            return False

        self._wait_for_sync(stop_event=self._stop_requested)
        if self._stop_requested.is_set():
            logger.debug("Stop event set")
            return False
        return True

    def _read_daq_position(self, actuator):
        """Return the synchronized position, polling RBV as a fallback."""
        if self._position_sync_subscription_set:
            return self._position

        position = actuator.rbv
        self._position = position
        return position

    def _acquire_poll_point(
        self,
        index: int,
        position,
        monitor: Optional[BaseMonitor] = None,
    ) -> None:
        """Acquire and publish one polling scan point."""
        self._fire_triggers("on_point")
        values = self.read_detectors()
        self.update_current_row_cache(
            idx=index,
            pos=position,
            values=values,
        )
        self._fire_triggers("after_point")

        plugin_values = self._collect_plugin_point_data(index, position)
        values = values + plugin_values
        self.save_to_file(position, values, self.include_timestamps)

        if monitor is not None:
            monitor.update(values)

    def _cleanup_scan(self, monitor: Optional[BaseMonitor] = None) -> None:
        """Release polling-scan resources using its established semantics."""
        self._stop_metadata_monitor()
        if monitor is not None:
            monitor.close()
        try:
            self._clear_subscriptions()
        except Exception:
            logger.exception("Error clearing scan subscriptions")
        self._fire_triggers("after")
        self.busyflag = False

    def scan(self, positions, monitor: Optional[BaseMonitor] = None):
        """
        Poll detector values.
        Now synchronized by heartbeat events when available, with poll timeout fallback.
        """

        del positions

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
        self._wait_for_motion_start(first_actuator)

        self._stop_requested.clear()
        try:
            self._start_subscriptions()
            self.busyflag = True
            self._position_sync_subscription_set = False
            while True:
                if self._scan_stop_requested():
                    break

                if not self._wait_for_scan_cycle():
                    break

                position = self._read_daq_position(first_actuator)
                if position is None:
                    continue
                
                range_exit_detected = range_exit.update(position)
                first_actuator_ready = first_actuator.is_ready() 
                if range_exit_detected and first_actuator_ready and self._start != self._stop:
                    logger.info(
                        "Scan termination detected at pos=%s",
                        position,
                    )
                    break
                if first_actuator_ready:
                    continue

                self._acquire_poll_point(index, position, monitor)
                index += 1
                logger.debug("Poll %d @ pos=%r", index, position)

                # refresh from actuator rbv if no sync subscription is used
                # (keeps range check honest for non-subscribed setups)
                if self._last_sync is None:
                    self._position = first_actuator.rbv
                
                if self._maxindex > 0 and index >= self._maxindex:
                    super().stop()
                    break
        finally:
            self._cleanup_scan(monitor)

    def execute(self) -> None:
        self._execute_standard(None) 
