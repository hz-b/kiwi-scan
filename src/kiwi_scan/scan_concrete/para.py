# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""
Parasitical step-scan engine.

This scan engine does not command actuator motion. It observes externally
moved actuators and records one scan point whenever all configured actuators
are inside their scan ranges and all actuators report ready.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import ScanConfig
from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.scan.common import BaseScan
from kiwi_scan.scan.range_exit_detector import RangeExitDetector
from kiwi_scan.scan.stats_collector import StatsCollector
from kiwi_scan.scan.registry import register_scan


@register_scan("para")
class ParaScan(BaseScan):
    """Passive, externally driven step scan.

    This scan never commands actuator motion. Instead, it waits for an
    external process or operator to move one or more configured actuators,
    then waits until all actuators report ready before acquiring a scan point.

    Once a point is stable, the scan runs the same per-point acquisition
    pipeline used by LinearScan: triggers, detector reads, plugin processing,
    monitor updates, statistics, and file writing.

    Each scan point is recorded from the current actuator readbacks. The first
    configured scan dimension remains the canonical ``Position`` column for
    compatibility with existing writers, while the full actuator readback state
    remains available on the scan object for plugins and runtime consumers.

    In practice, the scan cycles as follows:
      1. wait for external motion
      2. wait until all configured actuators are ready
      3. execute the normal DAQ point pipeline
      4. return to the wait state for the next externally initiated step
    """

    def __init__(self, config: ScanConfig, data_dir: Optional[str] = None):
        super().__init__(config, data_dir)

        if not self.scan_dimensions:
            raise ValueError("ParaScan requires at least one ScanDimension")

        logging.info("Creating parasitical samplerate from scan dimensions: %s", self.scan_dimensions)
        self.set_samplerate(sample_rate_hz=20)

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

        self._last_position_snapshot: Dict[str, Any] = {}

    def _on_stat_event(self, ev: PvEvent, subscription=None) -> None:
        """Record stat events and feed the per-subscription StatsCollector."""

        self.stats_collector.update(
            ev,
            subscription,
            collect=bool(getattr(self, "_daq_is_on", False)),
        )

        logging.debug(
            "[stat] %s=%r daq=%s sub=%s",
            ev.pvname,
            ev.value,
            getattr(self, "_daq_is_on", False),
            getattr(subscription, "name", None),
        )

    @staticmethod
    def _dim_in_range(dim, value: Any) -> bool:
        if value is None:
            return False
        try:
            pos = float(value)
        except (TypeError, ValueError):
            logging.debug("Cannot range-check non-numeric readback %r for actuator %s", value, dim.actuator)
            return False

        start = float(dim.start)
        stop = float(dim.stop)
        return start <= pos <= stop if start <= stop else stop <= pos <= start

    @staticmethod
    def _position_changed(
        current: Dict[str, Any],
        previous: Dict[str, Any],
        tolerances: Dict[str, float],
    ) -> bool:
        if not previous:
            return True

        for name, value in current.items():
            old = previous.get(name)
            if value is None or old is None:
                if value != old:
                    return True
                continue
            try:
                if abs(float(value) - float(old)) > tolerances.get(name, 0.0):
                    return True
            except (TypeError, ValueError):
                if value != old:
                    return True
        return False

    def _position_tolerances(self) -> Dict[str, float]:
        """Return a small per-axis tolerance used to suppress duplicate points."""
        tolerances: Dict[str, float] = {}
        for dim in self.scan_dimensions:
            span = abs(float(dim.stop) - float(dim.start))
            # Prefer a tiny fraction of the configured range.  For degenerate
            # ranges fall back to a conservative absolute tolerance.
            tolerances[dim.actuator] = max(span * 1e-9, 1e-12)
        return tolerances

    def _read_position_snapshot(self) -> Dict[str, Any]:
        """Read the current RBV for all configured scan actuators."""
        snapshot: Dict[str, Any] = {}
        for dim in self.scan_dimensions:
            actuator = self.actuators[dim.actuator]
            try:
                value = actuator.rbv
            except Exception:
                logging.exception("Failed to read RBV for actuator '%s'", dim.actuator)
                value = None
            snapshot[dim.actuator] = value
        return snapshot

    def _all_positions_in_range(self, snapshot: Dict[str, Any]) -> bool:
        return all(
            self._dim_in_range(dim, snapshot.get(dim.actuator))
            for dim in self.scan_dimensions
        )

    def _all_actuators_ready(self) -> bool:
        for dim in self.scan_dimensions:
            actuator = self.actuators[dim.actuator]
            try:
                if not actuator.is_ready():
                    return False
            except Exception:
                logging.exception("Failed to read ready state for actuator '%s'", dim.actuator)
                return False
        return True

    def _any_actuator_moving(self) -> bool:
        for dim in self.scan_dimensions:
            actuator = self.actuators[dim.actuator]
            try:
                if actuator.is_moving():
                    return True
            except Exception:
                logging.exception("Failed to read moving state for actuator '%s'", dim.actuator)
        return False

    def _termination_detected(
        self,
        snapshot: Dict[str, Any],
        range_exits: Dict[str, RangeExitDetector],
    ) -> bool:
        """Return True once an already-entered actuator range is left past its stop."""
        for dim in self.scan_dimensions:
            value = snapshot.get(dim.actuator)
            try:
                pos = float(value)
            except (TypeError, ValueError):
                continue
            if range_exits[dim.actuator].update(pos):
                logging.info("Scan termination detected for actuator '%s' at pos=%s", dim.actuator, pos)
                return True
        return False

    def _wait_cycle(self) -> None:
        """Wait for heartbeat/sync events or fall back to the configured sample time."""
        self._arm_sync_controller()
        if self._stop_requested.is_set():
            return

        if self.sync_controller.is_enabled():
            self._wait_for_sync(timeout_s=self.sampletime, stop_event=self._stop_requested)
        else:
            # Heartbeat-driven wakeup with timeout fallback for setups that do
            # not configure sync-role subscriptions.
            self._wait_for_tick_or_timeout(self.sampletime)

    def _acquire_point(
        self,
        index: int,
        snapshot: Dict[str, Any],
        monitor: Optional[BaseMonitor] = None,
    ) -> None:
        """Run the per-point DAQ pipeline without commanding any actuator."""
        primary_name = self.scan_dimensions[0].actuator
        current_position = snapshot.get(primary_name)
        self._position = current_position
        self._last_position_snapshot = dict(snapshot)

        self._reset_data_column_provider_windows()
        self._daq_is_on = True
        try:
            with self._time_block("triggers:on_point", idx=index):
                self._fire_triggers("on_point")

            if self.integration_time > 0.0:
                logging.info("DAQ for integration_time = %s", self.integration_time)
                time.sleep(self.integration_time)
            else:
                logging.info("integration_time = %s", self.integration_time)

            # Take the actuator snapshot after the integration window.  This is
            # the position that is written to the file and passed to plugins.
            snapshot = self._read_position_snapshot()
            current_position = snapshot.get(primary_name)
            self._position = current_position
            self._last_position_snapshot = dict(snapshot)

            with self._time_block("read_detectors", idx=index):
                vals = self.read_detectors()

            with self._time_block("triggers:after_point", idx=index):
                self._fire_triggers("after_point")

            plugin_data: List[Any] = []
            with self._time_block("plugins", idx=index):
                for plugin in self.plugins:
                    plugin_data += plugin.on_scan_point(index, current_position)
            vals = vals + plugin_data

            with self._time_block("write:data", idx=index):
                self.save_to_file(current_position, vals, self.include_timestamps)

            with self._time_block("monitor:update", idx=index):
                if monitor is not None:
                    logging.debug("%s", vals)
                    monitor.update(vals)

            logging.info("ParaScan point %d recorded at %s=%r", index, primary_name, current_position)
        finally:
            self._daq_is_on = False

    def scan(self, positions, monitor: Optional[BaseMonitor] = None):
        """Run the passive parasitical step-scan loop.

        ``positions`` is intentionally ignored.  The scan positions come from
        current actuator readbacks because motion is performed externally.
        """
        del positions

        try:
            import epics
            epics.ca.use_initial_context()
        except Exception:
            pass

        self.busyflag = True
        self._stop_requested.clear()
        self.write_header_to_output_file()

        range_exits = {
            dim.actuator: RangeExitDetector(dim.start, dim.stop, eps=0.001, out_threshold=3)
            for dim in self.scan_dimensions
        }
        tolerances = self._position_tolerances()
        index = 0
        have_recorded_inside_range = False

        try:
            self._start_subscriptions()
            self._start_metadata_monitor()
            self._fire_triggers("before")

            while True:
                if self._stop_requested.is_set():
                    logging.debug("Stop event set")
                    break
                if self.get_stop_pv() == 1:
                    logging.info("Stop PV triggered—aborting para scan.")
                    break

                self._wait_cycle()
                if self._stop_requested.is_set():
                    break

                snapshot = self._read_position_snapshot()
                in_range = self._all_positions_in_range(snapshot)

                if have_recorded_inside_range and self._termination_detected(snapshot, range_exits):
                    break

                if not in_range:
                    logging.debug("Waiting for all actuators to enter range: %s", snapshot)
                    continue

                have_recorded_inside_range = True

                if not self._all_actuators_ready():
                    logging.debug("Actuators in range but not ready yet: %s", snapshot)
                    continue

                if not self._position_changed(snapshot, self._last_position_snapshot, tolerances):
                    # Stable, ready, and unchanged since the previous point.  This
                    # is the passive scan's wait-for-external-step state.
                    logging.debug("Skipping duplicate ready position snapshot: %s", snapshot)
                    continue

                self._acquire_point(index, snapshot, monitor)
                index += 1

                # Idle state after DAQ:
                # Do not allow another point only because the readback changed
                # slightly after the integration window.  A new parasitical step
                # starts only after at least one configured actuator reports
                # moving/running.  The outer loop will then wait until all
                # actuators are ready again before recording the next point.
                while not self._stop_requested.is_set():
                    if self.get_stop_pv() == 1:
                        logging.info("Stop PV triggered—aborting para scan.")
                        self._stop_requested.set()
                        break
                    if self._any_actuator_moving():
                        logging.debug("External para step startup detected")
                        break
                    self._wait_cycle()

        finally:
            self._daq_is_on = False
            self._stop_metadata_monitor()
            if monitor is not None:
                monitor.close()
            try:
                self._stop_subscriptions()
            except Exception:
                logging.exception("Error stopping scan subscriptions")
            try:
                self._fire_triggers("after")
            finally:
                self.busyflag = False
                self._perf_report()

    def execute(self):
        self._execute_standard(None)
