# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import time
import threading
from typing import Optional

from kiwi_scan.scan.common import BaseScan
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.monitor.factory import create_monitor
from kiwi_scan.actuator.single import PvEvent


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

        logging.info("Creating samplerate from scan dimensions: %s", self.scan_dimensions)
        self.set_samplerate(self.scan_dimensions[0])
        self._start = self.scan_dimensions[0].start
        self._stop = self.scan_dimensions[0].stop

        # --- event-driven wakeup state ---
        self._tick_cond = threading.Condition()
        self._tick_seq = 0  # increments on each heartbeat event
        self._stop_requested = threading.Event()

        # --- optional: store last events for debugging/logic ---
        self._last_heartbeat: Optional[PvEvent] = None
        self._last_sync: Optional[PvEvent] = None
        self._last_status: Optional[PvEvent] = None

        self.register_subscription_role("heartbeat", self._on_heartbeat_event)
        self.register_subscription_role("sync", self._on_sync_event)
        self.register_subscription_role("status", self._on_status_event)
        self.register_subscription_role("stop", self._on_stop_event)

    # -------------------- role callbacks --------------------

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
        """
        Immediate stop trigger. Stops actuators best-effort and wakes the loop.
        """
        logging.info("[stop] %s=%r -> stopping scan", ev.pvname, ev.value)
        if self.busyflag == True:
            self._stop_requested.set()
            with self._tick_cond:
                self._tick_cond.notify_all()
            try:
                for act in self.actuators.values():
                    act.stop()
            except Exception:
                logging.exception("Error while stopping actuators on stop event")

    # -------------------- internal helpers --------------------

    def _wait_for_tick_or_timeout(self, timeout_s: float) -> bool:
        """
        Wait until:
          - a heartbeat tick arrives (returns True), or
          - timeout occurs (returns False), or
          - stop is requested (returns False).
        """
        if timeout_s is None or timeout_s < 0:
            timeout_s = 0.0

        with self._tick_cond:
            start_seq = self._tick_seq
            if self._stop_requested.is_set():
                return False

            # Wait until seq changes or timeout
            self._tick_cond.wait(timeout=timeout_s)
            if self._stop_requested.is_set():
                return False

            return self._tick_seq != start_seq

    # -------------------- main scan --------------------

    def scan(self, monitor: BaseMonitor = None) -> None:
        """
        Poll detector values.
        Now synchronized by heartbeat events when available, with poll timeout fallback.
        """

        self.write_header_to_output_file()
        index = 0

        first_actuator = self.actuators[self.scan_dimensions[0].actuator]
        self._start_metadata_monitor()
        self._fire_triggers("before")
        
        while not first_actuator.is_moving():
            logging.debug("Wait for actuator to start")
            time.sleep(0.05)

        self._stop_requested.clear()
        try:
            self._start_subscriptions()
            self.busyflag = True
            entered_range = False
            self._position_sync_subscription_set = False
            while True:
                if self._stop_requested.is_set():
                    logging.debug("Stop event set")
                    break
                if self.get_stop_pv() == 1:
                    logging.debug("Stop PV set")
                    break

                # Start a new sync cycle, then wait for heartbeat and all
                # configured sync-role subscriptions.
                self._arm_sync_controller()
                self._wait_for_tick_or_timeout(self.sampletime)

                if self._stop_requested.is_set():
                    logging.debug("Stop event set")
                    break

                self._wait_for_sync(timeout_s=self.sampletime, stop_event=self._stop_requested)

                if self._stop_requested.is_set():
                    logging.debug("Stop event set")
                    break

                # Prefer subscribed position if sync role is configured, else read rbv
                pos = self._position
                if self._position_sync_subscription_set:
                    pos = first_actuator.rbv
                    self._position = pos
                current_position = pos
                self._fire_triggers("on_point")
                # logging.debug("Read detectors")
                vals = self.read_detectors()
                self._fire_triggers("after_point")
                in_range = self.is_within_range(current_position, self._start, self._stop)

                # break only if out of range and the range is entered once
                if in_range:
                    entered_range = True
                elif entered_range and first_actuator.is_ready():
                    logging.debug(f"out of range: {self._start}|{pos}|{self._stop}")
                    break

                # plugin data
                plugin_data = []
                for plugin in self.plugins:
                    plugin_data += plugin.on_scan_point(index, current_position)

                vals = vals + plugin_data
                self.save_to_file(current_position, vals, self.include_timestamps)

                # >>> Notify monitor/plotter
                if monitor is not None:
                    monitor.update(vals)

                index += 1
                logging.debug("Poll %d @ pos=%r", index, current_position)

                # refresh from actuator rbv if no sync subscription is used
                # (keeps range check honest for non-subscribed setups)
                if self._last_sync is None:
                    current_position = first_actuator.rbv
                    self._position = current_position
                
        finally:
            self._stop_metadata_monitor()
            if monitor is not None:
                monitor.close()
            # IMPORTANT: PollScan doesn't use BaseScan.scan(), so we must clean up subscriptions here.
            try:
                self._clear_subscriptions()
            except Exception:
                logging.exception("Error clearing scan subscriptions")

            self._fire_triggers("after")
            self.busyflag = False

    def execute(self) -> None:
        """
        Execute polling within range.
        """
        logging.info(f"Starting polling from {self._start} to {self._stop}")

        monitor = create_monitor(self.cfg)

        # Monitor columns: detector PVs + plugin headers
        plugin_headers = []
        for plugin in self.plugins:
            plugin_headers += plugin.get_headers(self.include_timestamps)

        if monitor is not None:
            monitor.start(self.cfg.detector_pvs + plugin_headers)

        scan_thread = threading.Thread(target=self.scan, args=(monitor,), daemon=True)
        scan_thread.start()

        if monitor is not None:
            monitor.loop()

        scan_thread.join()
        logging.info("Polling complete.")

