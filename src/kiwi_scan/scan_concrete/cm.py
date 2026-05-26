# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import threading
import epics
from typing import Optional
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.scan.range_exit_detector import RangeExitDetector

# TODO: refactor with poll,monocm, ...
# TODO: offsets for backlash and end of range
class CMScan(BaseScan):
    def __init__(self, config: ScanConfig, data_dir=None):
        super().__init__(config, data_dir)
        
        if not self.scan_dimensions:
            raise ValueError("CMScan requires at least one ScanDimension")
        
        logging.info("Creating samplerate from scan dimensions: %s", self.scan_dimensions)
        dim = config.scan_dimensions[0]
        if dim.start == dim.stop:
            raise ArithmeticError(f"Start equals stop == {dim.start!r}")
        self._start = dim.start
        self._stop = dim.stop
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
                logging.debug("Skipping velocity restore for actuator '%s': original velocity is None", name)
                continue

            actuator = self.actuators.get(name)
            if actuator is None:
                logging.warning("Cannot restore velocity for unknown actuator '%s'", name)
                continue

            try:
                actuator.set_velocity(orig_vel)
                logging.info("Restored velocity for actuator '%s' to %s", name, orig_vel)
            except Exception as e:
                logging.warning("Failed to restore velocity for actuator '%s': %s", name, e)

    def stop(self) -> None:
        """Request CM scan stop and restore original actuator velocities."""
        super().stop()
        self._restore_original_velocities()

    """ ----------- sync event handler -----------------------
        Example config yaml:
            subscriptions:
              - name: energy_sync
                role: sync
                actuator: energy
                source: rbv
    """
    def _on_sync_event(self, ev: PvEvent, subscription=None) -> None:
        """
        Record sync events for the SyncController. Only the primary actuator
        RBV-style sync source updates self._position.
        """
        self._last_sync = ev
        self.sync_controller.note_event(getattr(subscription, "name", None))

        # Only the primary actuator rbv-style sync should update scan position.
        if self._is_position_sync_subscription(subscription):
            try:
                self._position = float(ev.value)
            except Exception:
                self._position = ev.value
            self._position_sync_subscription_set = True

        logging.debug(
            "[sync] %s=%r -> _position=%r (source=%r, sub=%s)",
            ev.pvname,
            ev.value,
            self._position,
            ev.source,
            getattr(subscription, "name", None),
        )

    def run_daq(self, monitor: BaseMonitor = None):
        """
        DAQ loop driven by heartbeat subscription when available.
        sampletime acts as timeout fallback (so it still works without heartbeat).
        Position is taken from sync subscription when available; otherwise RBV is polled.
        """
        self.write_header_to_output_file()
        idx = 0

        # initial snapshot; may quickly be overwritten by sync subscription indicated by flag
        self._position_sync_subscription_set = False
        self._position = self.first_actuator.rbv
        self._stop_requested.clear()
        primary_dim = self.scan_dimensions[0]
        primary = self.actuators[primary_dim.actuator]
        range_exit = RangeExitDetector(
            self._start,
            self._stop,
            eps=0.001,   # TODO: could be taken from actuator backlash?
            out_threshold=2,
        )
        while True:
            if self._stop_requested.is_set():
                break
            if self.get_stop_pv() == 1:
                break

            # heartbeat-driven tick plus all configured sync-role updates
            self._arm_sync_controller()
            # self._wait_for_tick_or_timeout(self.sampletime)
            if self._stop_requested.is_set():
                break
            self._fire_triggers("after_point")
            # --------------------------------------- block scan task
            if self.sync_controller.is_enabled():
                self._wait_for_sync(
                    timeout_s=self.sampletime,
                    stop_event=self._stop_requested,
                )
            else:
                if self._stop_requested.wait(self.sampletime):
                    break
            # ---------------------------------------
            if self._stop_requested.is_set():
                break

            if self.first_actuator.is_ready():
                break
            # Prefer sync-subscription position; fall back to RBV
            pos = self.first_actuator.rbv
            if range_exit.update(pos):
                logging.info("Scan termination detected at pos=%s", pos)
                break
            if not range_exit.entered:
                continue
            if self._position_sync_subscription_set:
                self._position = pos
            self._fire_triggers("on_point")
            vals = self.read_detectors()

            plugin_data = []
            for plugin in self.plugins:
                plugin_data += plugin.on_scan_point(idx, pos)
            vals = vals + plugin_data

            self.save_to_file(pos, vals, self.include_timestamps)
            # >>> Notify monitor/plotter
            if monitor is not None:
                logging.debug(f"{vals}")
                monitor.update(vals)
            idx += 1

    # ---------------- cm scan logic --------------------
    def scan(self, positions, monitor: BaseMonitor = None):
        """
        1) Move to start position
        2) Store current velocities
        3) Apply configured velocities and start moves
        4) Run DAQ while primary actuator is within range
        5) Restore original velocities
        """
        self.busyflag = True
        try:
            # 1) Move each actuator to start position
            for dim in self.scan_dimensions:
                name = dim.actuator
                actuator = self.actuators[name]
                bdist = -actuator.backlash if dim.stop > dim.start else actuator.backlash
                overshoot = dim.start + bdist
                logging.info(f"overshoot={overshoot}, bdist={bdist}, backlash={actuator.backlash}")
                try:
                    actuator.run_move(overshoot, sync=True)
                    logging.info(f"Started actuator '{name}' moving to {dim.start}")
                except Exception as e:
                    logging.warning(f"Failed to move actuator '{name}': {e}")
            # 2) Store all original velocities
            for name, actuator in self.actuators.items():
                try:
                    vel = actuator.get_velocity()
                    if vel is None:
                        logging.warning("Could not read original velocity for actuator '%s'; velocity restore will be skipped", name)
                        continue
                    self._original_velocities[name] = vel
                    logging.info(f"Stored velocity for actuator '{name}': {vel}")
                except Exception as e:
                    logging.warning(f"Could not read velocity for actuator '{name}': {e}")

            # 3) Set target velocities and start each actuator
            #  start CA monitors BEFORE motion begins 
            self._start_metadata_monitor()
            self._fire_triggers("before")
            for dim in self.scan_dimensions:
                name = dim.actuator
                actuator = self.actuators[name]
                try:
                    actuator.set_velocity(dim.velocity)
                    logging.info(f"Set velocity of actuator '{name}' to {dim.velocity}")
                    actuator.move(dim.stop)
                    logging.info(f"Started actuator '{name}' moving to {dim.stop}")
                except Exception as e:
                    logging.warning(f"Failed to configure/startup actuator '{name}': {e}")
            self._start_subscriptions()

            # 4) DAQ loop on primary actuator
            self.run_daq(monitor)
        finally:
            self._restore_original_velocities()
            self._stop_metadata_monitor()
            # MonoCMScan overrides BaseScan.scan(), so it must clear subscriptions itself
            try:
                self._stop_subscriptions()
            except Exception:
                logging.exception("Error stopping scan subscriptions")
            
            if monitor is not None:
                monitor.close()

            self._fire_triggers("after")
            self.busyflag = False

    def execute(self):
        self._execute_standard(None)

