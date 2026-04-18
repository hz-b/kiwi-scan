# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import threading
from typing import Optional
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.actuator.single import PvEvent

# TODO: refactor with poll,monocm, ...
# TODO: offsets for backlash and end of range
class CMScan(BaseScan):
    def __init__(self, config: ScanConfig, data_dir=None):
        super().__init__(config, data_dir)
        
        if not self.scan_dimensions:
            raise ValueError("CMScan requires at least one ScanDimension")
        
        logging.info("Creating samplerate from scan dimensions: %s", self.scan_dimensions)
        dim = config.scan_dimensions[0]
        self.set_samplerate(dim)
        self.first_actuator = self.actuators[self.scan_dimensions[0].actuator]
        # ---- event-driven wakeup state (heartbeat-driven, timeout fallback) ----
        self._tick_cond = threading.Condition()
        self._tick_seq = 0
        self._stop_requested = threading.Event()

        # optional: last-seen events for debugging
        self._last_heartbeat: Optional[PvEvent] = None
        self._last_sync: Optional[PvEvent] = None
        self._last_status: Optional[PvEvent] = None

        self.register_subscription_role("heartbeat", self._on_heartbeat_event)
        self.register_subscription_role("sync", self._on_sync_event)
        self.register_subscription_role("status", self._on_status_event)
        self.register_subscription_role("stop", self._on_stop_event)


        self._original_velocities = {}
    
    # -------------------- subscription role handlers --------------------

    def _on_heartbeat_event(self, ev: PvEvent, subscription=None) -> None:
        self._last_heartbeat = ev
        with self._tick_cond:
            self._tick_seq += 1
            self._tick_cond.notify_all()
        logging.debug("[heartbeat] %s=%r (seq=%d)", ev.pvname, ev.value, self._tick_seq)
    # TODO: common handler
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

    def _on_status_event(self, ev: PvEvent, subscription=None) -> None:
        self._last_status = ev
        logging.debug("[status] %s=%r", ev.pvname, ev.value)

    def _on_stop_event(self, ev: PvEvent, subscription=None) -> None:
        logging.info("[stop] %s=%r -> stopping scan", ev.pvname, ev.value)
        if not self.busyflag:
            return
        self._stop_requested.set()
        with self._tick_cond:
            self._tick_cond.notify_all()
        try:
            for act in self.actuators.values():
                act.stop()
        except Exception:
            logging.exception("Error while stopping actuators on stop event")

    # -------------------- helpers --------------------

    def _wait_for_tick_or_timeout(self, timeout_s: float) -> bool:
        """
        Wait until a heartbeat arrives, or until timeout.
        Returns True if heartbeat tick arrived, False if timed out / stopping.
        """
        if timeout_s is None or timeout_s < 0:
            timeout_s = 0.0
        with self._tick_cond:
            start_seq = self._tick_seq
            if self._stop_requested.is_set():
                return False
            self._tick_cond.wait(timeout=timeout_s)
            if self._stop_requested.is_set():
                return False
            return self._tick_seq != start_seq

    def run_daq(self):
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

        entered_range = False
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
            self._wait_for_sync(timeout_s=self.sampletime, stop_event=self._stop_requested)
            if self._stop_requested.is_set():
                break

            # Prefer sync-subscription position; fall back to RBV
            pos = self.first_actuator.rbv

            in_range = self.is_within_range(pos, primary_dim.start, primary_dim.stop)

            # break only if out of range and the range is entered once
            if in_range:
                logging.debug(f"in range: {primary_dim.start}|{pos}|{primary_dim.stop}")
                entered_range = True
            elif entered_range:
                logging.debug(f"out of range: {primary_dim.start}|{pos}|{primary_dim.stop}")
                break

            if self._position_sync_subscription_set:
                self._position = pos
            self._fire_triggers("on_point")
            dets = self.read_detectors()

            plugin_data = []
            for plugin in self.plugins:
                plugin_data += plugin.on_scan_point(idx, pos)
            dets = dets + plugin_data

            self.save_to_file(pos, dets, self.include_timestamps)
            idx += 1

    # -------------------- scan logic --------------------

    def scan(self):
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
                try:
                    actuator.run_move(dim.start, sync=True )
                    logging.info(f"Started actuator '{name}' moving to {dim.start}")
                except Exception as e:
                    logging.warning(f"Failed to move actuator '{name}': {e}")
            # 2) Store all original velocities
            for name, actuator in self.actuators.items():
                try:
                    vel = actuator.get_velocity()
                    self._original_velocities[name] = vel
                    logging.info(f"Stored velocity for actuator '{name}': {vel}")
                except Exception as e:
                    logging.warning(f"Could not read velocity for actuator '{name}': {e}")

            # 3) Set target velocities and start each actuator
            #  start CA monitors BEFORE motion begins 
            try:
                self._meta_mon.start()
            except Exception as e:
                # Non-fatal: keep the scan running even if metadata fails
                logging.error("Failed to start metadata monitor: %s", e, exc_info=True)

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
            self.run_daq()

            # 5) Restore original velocities
            for name, orig_vel in self._original_velocities.items():
                actuator = self.actuators[name]
                try:
                    actuator.set_velocity(orig_vel)
                    logging.info(f"Restored velocity for actuator '{name}' to {orig_vel}")
                except Exception as e:
                    logging.warning(f"Failed to restore velocity for actuator '{name}': {e}")
        finally:
            try:
                self._meta_mon.stop()
            except Exception:
                logging.exception("Error stopping metadata monitor")

            # MonoCMScan overrides BaseScan.scan(), so it must clear subscriptions itself
            try:
                self._clear_subscriptions()
            except Exception:
                logging.exception("Error clearing scan subscriptions")

            self._fire_triggers("after")
            self.busyflag = False

    def execute(self):
        logging.info(f"Starting CM scan.")
        self.scan()
        logging.info("CM complete.")

