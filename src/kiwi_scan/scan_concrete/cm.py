# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from typing import Optional

from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.range_exit_detector import RangeExitDetector

logger = logging.getLogger(__name__)

# TODO: refactor with poll,monocm, ...
# TODO: offsets for backlash and end of range
class CMScan(BaseScan):
    def __init__(self, config: ScanConfig, data_dir=None):
        super().__init__(config, data_dir)
        
        scan_dimensions = self.scan_dimensions
        if not scan_dimensions:
            raise ValueError("CMScan requires at least one ScanDimension")
        logger.info("Creating samplerate from scan dimensions: %s", scan_dimensions)
        dim = scan_dimensions[0]
        if dim.start == dim.stop:
            raise ArithmeticError(f"Start equals stop == {dim.start!r}")
        self._start = dim.start
        self._stop = dim.stop
        self._maxindex = dim.steps
        self.set_samplerate()
        self.first_actuator = self.actuators[self.scan_dimensions[0].actuator]

        self.register_subscription_role("heartbeat", self._on_heartbeat_event)
        self.register_subscription_role("sync", self._on_sync_event)
        self.register_subscription_role("status", self._on_status_event)
        self.register_subscription_role("stop", self._on_stop_event)

        self._original_velocities = {}

    def _restore_original_velocities(self) -> None:
        """Restore actuator velocities saved before the continuous move."""
        for name, orig_vel in self._original_velocities.items():
            if orig_vel is None:
                logger.debug("Skipping velocity restore for actuator '%s': original velocity is None", name)
                continue

            actuator = self.actuators.get(name)
            if actuator is None:
                logger.warning("Cannot restore velocity for unknown actuator '%s'", name)
                continue

            try:
                actuator.set_velocity(orig_vel)
                logger.info("Restored velocity for actuator %s to %s", name, orig_vel)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to restore velocity for actuator %s: %s", name, exc)

    def stop(self) -> None:
        """Request CM scan stop and restore original actuator velocities."""
        super().stop()
        self._restore_original_velocities()

    def _daq_stop_requested(self, index: int) -> bool:
        """Return whether an event or the configured stop PV ended DAQ."""
        if self._stop_requested.is_set():
            return True

        with self._time_block("stop:poll", idx=index):
            stop_pv_value = self.get_stop_pv()
        if stop_pv_value != 1:
            return False

        super().stop()
        return True

    def _read_daq_position(self):
        """Return the synchronized position, polling RBV as a fallback."""
        if self._position_sync_subscription_set:
            return self._position

        position = self.first_actuator.rbv
        self._position = position
        return position

    def _acquire_daq_point(self, index: int, position, monitor: Optional[BaseMonitor] = None) -> None:
        """Acquire, process, persist, and publish one continuous-motion point."""
        with self._time_block("daq:point", idx=index):
            with self._time_block("triggers:on_point", idx=index):
                self._fire_triggers("on_point")

            with self._time_block("read_detectors", idx=index):
                values = self.read_detectors()
            self.update_current_row_cache(idx=index, pos=position, values=values)

            with self._time_block("plugins", idx=index):
                plugin_values = self._collect_plugin_point_data(index, position)
            values = values + plugin_values

            with self._time_block("write:data", idx=index):
                monitor_values = self.save_to_file(position, values, self.include_timestamps)

            with self._time_block("monitor:update", idx=index):
                if monitor is not None:
                    logger.debug("Monitor values: %s", monitor_values)
                    monitor.update(monitor_values)

    def run_daq(self, monitor: Optional[BaseMonitor] = None):
        """
        DAQ loop driven by heartbeat subscription when available.
        sampletime acts as timeout fallback (so it still works without heartbeat).
        Position is taken from sync subscription when available; otherwise RBV is polled.
        """
        with self._time_block("write:header"):
            self.write_header_to_output_file()
        index = 0

        # initial snapshot; may quickly be overwritten by sync subscription indicated by flag
        self._position_sync_subscription_set = False
        self._position = self.first_actuator.rbv
        self._stop_requested.clear()
        range_exit = RangeExitDetector(
            self._start,
            self._stop,
            eps=0.001,   # TODO: parameter to overwrite default 
            out_threshold=2,
        )
        while True:
            logger.debug("run_daq: Entered cm scan loop")
            if self._daq_stop_requested(index):
                break

            # heartbeat-driven tick plus all configured sync-role updates
            self._arm_sync_controller()
            if self._stop_requested.is_set():
                break

            with self._time_block("triggers:after_point", idx=index):
                self._fire_triggers("after_point")
            with self._time_block("sync:wait", idx=index):
                self._wait_for_sync(stop_event=self._stop_requested)
            if self._stop_requested.is_set():
                break

            with self._time_block("actuator:ready", idx=index):
                actuator_ready = self.first_actuator.is_ready()
            if actuator_ready:
                logger.info("run_daq: First actuator is ready.")
                break
            # Prefer the position delivered by the primary actuator's sync
            # subscription. Poll the actuator RBV only when no such event has
            # been received.
            with self._time_block("position:read", idx=index):
                pos = self._read_daq_position()

            with self._time_block("range:update", idx=index):
                scan_finished = range_exit.update(pos)
            if scan_finished:
                logger.info("Scan termination detected at pos=%s", pos)
                break
            if not range_exit.entered:
                continue

            self._acquire_daq_point(index, pos, monitor)
            index += 1
            if self._maxindex > 0 and index >= self._maxindex:
                super().stop()
                break

    def _move_to_start_positions(self) -> None:
        """Move each scan actuator to its backlash-adjusted start position."""
        with self._time_block("move:to_start"):
            for dim in self.scan_dimensions:
                name = dim.actuator
                actuator = self.actuators[name]
                backlash = (
                    -actuator.backlash
                    if dim.stop > dim.start
                    else actuator.backlash
                )
                overshoot = dim.start + backlash
                logger.info("overshoot=%s, bdist=%s, backlash=%s", overshoot, backlash, actuator.backlash)
                try:
                    actuator.run_move(overshoot, sync=True)
                    logger.info("Started actuator '%s' moving to %s", name, dim.start)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Failed to move actuator '%s': %s", name, exc)

    def _store_original_velocities(self) -> None:
        """Read actuator velocities for best-effort restoration after the scan."""
        with self._time_block("velocity:read"):
            for name, actuator in self.actuators.items():
                try:
                    velocity = actuator.get_velocity()
                    if velocity is None:
                        logger.warning( "Could not read original velocity for actuator '%s'; velocity restore will be skipped", name)
                        continue
                    self._original_velocities[name] = velocity
                    logger.info("Stored velocity for actuator '%s': %s", name, velocity)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Could not read velocity for actuator '%s': %s", name, exc)

    def _start_continuous_motion(self) -> None:
        """Apply configured velocities and start all continuous moves."""
        with self._time_block("move:start"):
            for dim in self.scan_dimensions:
                name = dim.actuator
                actuator = self.actuators[name]
                try:
                    actuator.set_velocity(dim.velocity)
                    logger.info("Set velocity of actuator '%s' to %s", name, dim.velocity)
                    actuator.run_move(dim.stop, sync=False, wait_startup=True)
                    logger.info("Started actuator '%s' moving to %s", name, dim.stop)
                except Exception as exc:  # noqa: BLE001
                    logger.error("Failed to configure/startup actuator '%s': %s", name, exc)

    def _cleanup_scan(self, monitor: Optional[BaseMonitor] = None) -> None:
        """Release scan resources without allowing one failure to block others."""
        self._run_cleanup_step("velocity:restore", self._restore_original_velocities)
        self._run_cleanup_step("plugins:stop", self._end_plugins)
        self._run_cleanup_step("plugins:close", self._close_plugins)
        self._run_cleanup_step("metadata:stop", self._stop_metadata_monitor)
        self._run_cleanup_step("subscriptions:stop", self._stop_subscriptions)
        if monitor is not None:
            self._run_cleanup_step("monitor:close", monitor.close)
        self._run_cleanup_step("triggers:after", lambda: self._fire_triggers("after"))
        self.busyflag = False
        self._run_cleanup_step("performance:report", self._perf_report)

    # ---------------- cm scan logic --------------------
    def scan(self, positions, monitor: Optional[BaseMonitor] = None):
        """
        1) Move to start position
        2) Store current velocities
        3) Apply configured velocities and start moves
        4) Run DAQ while 1st actuator is within range
        5) Restore original velocities
        """
        del positions

        self.busyflag = True
        try:
            with self._time_block("plugins:start"):
                self._start_plugins()
            self._move_to_start_positions()
            self._store_original_velocities()

            with self._time_block("metadata:start"):
                self._start_metadata_monitor()
            with self._time_block("triggers:before"):
                self._fire_triggers("before")
            self._start_continuous_motion()

            with self._time_block("subscriptions:start"):
                self._start_subscriptions()
            with self._time_block("daq:run"):
                self.run_daq(monitor)
        finally:
            self._cleanup_scan(monitor)

    def execute(self):
        self._execute_standard(None)
