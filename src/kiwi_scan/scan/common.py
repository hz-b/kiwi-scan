# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations
from contextlib import contextmanager
from collections import defaultdict
import logging
from typing import List, Any, Dict, Optional, Tuple, Iterator
from datetime import timezone
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import random
import string
import epics
# import pdb
from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.actuator.factory import create_actuator
from kiwi_scan.datamodels import ActuatorConfig, ScanDimension, ScanConfig
from kiwi_scan.plugin.registry import create_plugin
from kiwi_scan.dataloader import DataLoader, resolve_data_dir
from kiwi_scan.monitor.base import BaseMonitor
from kiwi_scan.scan.scan_abs import ScanABC
from kiwi_scan.epics_wrapper import EpicsPV
from .metadata_monitor import MetadataCAMonitor
from .trigger_manager import TriggerManager
from .subscription_manager import SubscriptionManager
from .sync_controller import SyncController

class BaseScan(ScanABC):
    """
    A base class for performing scans using EPICS Process Variables (PVs).

    * Event-Driven Scan Architecture *

            [Triggers]        [Plugins]
             ↑               ↓↑
         ┌────────────────────────┐
         │      Scan Engine       │
         └────────────────────────┘
           ↑          ↑↓         ↓
   [Subscriptions]←[Actuators] [Data Writer]
           ↑                      ↓
       [Monitors]        [Metadata Sidecar]
    """
    def __init__(self, config: ScanConfig, data_dir=None):
        epics.ca.use_initial_context()
        #pv = EpicsPV("TESTU171PGM1:Psi")
        #print(pv.get())
        super().__init__(config, data_dir)
        self.busyflag = False
        logging.debug("Init BaseScan")
        self.cfg = config
        # Perform config cleanup
        self._validate_and_filter_actuators()

        self.plugins = [create_plugin(plugin_config, self) for plugin_config in self.cfg.plugin_configs]
        logging.debug(f"Plugin Configs: {self.cfg.plugin_configs}")

        # Perform config cleanup
        self._validate_and_filter_actuators()
        # normalize optionals
        self.scan_dimensions = config.scan_dimensions or []
        self.parallel_scans  = config.parallel_scans  or []
        self.nested_scans    = config.nested_scans    or []
        self.trigger_manager = TriggerManager.from_config(self.cfg.triggers)
        # Prepare I/O
        self.data_dir = resolve_data_dir(data_dir, config.data_dir)
        logging.info(f"Data directory: {self.data_dir}")

        # copy runtime flags
        self.include_timestamps = config.include_timestamps
        self.samplerate = 1.0
        self.sampletime = 1.0
        self.debug = config.debug
        if self.debug:
            logging.basicConfig(level=logging.INFO)
            # pdb.set_trace()
        self.output_file = self.generate_and_create_file(config.output_file)
        # setup
        logging.debug("_connect_detectors")
        self._connect_detectors()
        logging.debug("_connect_actuators")
        self._connect_actuators()
        self.actuators = getattr(self, "actuators", {})
        logging.debug("init subscription manager")
        self.subscription_manager = SubscriptionManager(
            getattr(self.cfg, "subscriptions", None) or [],
            actuator_configs=getattr(self.cfg, "actuators", {}) or {},
            actuators=self.actuators,
        )
        self.sync_controller = SyncController(
            getattr(self.cfg, "subscriptions", None) or []
        )
        self._sync_legacy_role_callbacks()
        logging.debug("_validate_config")
        
        self._validate_config()
        if config.stop_pv:
            self.stop_pv = epics.PV(config.stop_pv)
            self.prefix = config.stop_pv.split(':')[0]
        else:
            self.stop_pv = None
        # Build a time-stamped sibling file next to main scan file
        base_name, ext = os.path.splitext(self.cfg.metadata_file or "scan_metadata.txt")
        # Reuse the same timestamp as main file (nice correlation)
        main_name, main_ext = os.path.splitext(os.path.basename(self.output_file))
        # main_name looks like "<base>-YYYYMMDDHHMMSS"
        self._metadata_out = os.path.join(self.data_dir, f"{base_name}-{main_name.split('-', 1)[-1]}{ext}")
        # Create the monitor (but don't start yet)
        self._meta_mon = MetadataCAMonitor(
            pvs=list(self.cfg.metadata_pvs or []),
            constants=dict(self.cfg.metadata_constants or {}),
            outfile=self._metadata_out,
            queue_maxsize=20000, 
        )
        self._position: Any = None
        self._last_point: Dict[str, Any] = {}
        self._stats: Optional[Tuple[float, float]] = None
        self._daq_is_on = False   # safe to take data for stats
        self.integration_time = config.integration_time
        self._perf_enabled: bool = bool(
            getattr(self.cfg, "debug", False)
            or getattr(self.cfg, "performance_report", False)
        )
        if self._perf_enabled:
            logging.info("Performance reporting enabled")
        else:
            logging.debug("Performance report disabled")
        self._perf: Dict[str, List[float]] = defaultdict(list)
        
        # TODO: cleanup, use  _start_subscriptions ouside
        if getattr(self, "ROLE_CALLBACKS", None):
            logging.debug("Detected legacy ROLE_CALLBACKS on %s; auto-starting subscriptions for compatibility", type(self).__name__)
            self._start_subscriptions()
    
    # -------------------- performance testing --------------------

    @contextmanager
    def _time_block(self, name: str, *, idx: Optional[int] = None) -> Iterator[None]:
        """
        Measure wall time for a scan sub-block.
        Stores seconds in self._perf[name].
        """
        if not self._perf_enabled:
            yield
            return

        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self._perf[name].append(dt)
            # Debug per-point; summary is printed at end.
            if idx is not None:
                logging.debug("[PERF] idx=%d %-20s %.6f s", idx, name, dt)
            else:
                logging.debug("[PERF] %-20s %.6f s", name, dt)

    def _perf_report(self) -> None:
        """Print a compact summary once at the end of a scan."""
        if not self._perf_enabled or not self._perf:
            return

        def p95(values: List[float]) -> float:
            if not values:
                return 0.0
            vs = sorted(values)
            k = int(0.95 * (len(vs) - 1))
            return vs[k]

        logging.info("========== PERF SUMMARY ==========")
        for name, values in sorted(self._perf.items()):
            n = len(values)
            if n == 0:
                continue
            total = sum(values)
            mean = total / n
            mx = max(values)
            logging.info(
                "[PERF] %-20s n=%d total=%.3fs mean=%.6fs p95=%.6fs max=%.6fs",
                name, n, total, mean, p95(values), mx
            )
        logging.info("==================================")

    # -------------------- subscription/callback integration --------------------

    def _sync_legacy_role_callbacks(self) -> None:
        """Mirror legacy ROLE_CALLBACKS into SubscriptionManager.

        This keeps external scan classes that still populate ROLE_CALLBACKS
        working while the preferred API is register_subscription_role(...).
        """
        role_callbacks = getattr(self, "ROLE_CALLBACKS", None) or {}
        if not isinstance(role_callbacks, dict):
            return

        for role, handler in role_callbacks.items():
            if callable(handler):
                self.subscription_manager.register_role(role, handler)

    def register_subscription_role(self, role: str, handler) -> None:
        self.subscription_manager.register_role(role, handler)

    def _start_subscriptions(self) -> None:
        # Pick up any late-bound legacy ROLE_CALLBACKS before starting.
        self._sync_legacy_role_callbacks()
        self.subscription_manager.start()

    def _stop_subscriptions(self) -> None:
        self.subscription_manager.stop()

    def _clear_subscriptions(self) -> None:
        self._stop_subscriptions()

    def _fire_triggers(self, phase: str) -> None:
        """Compatibility wrapper delegating trigger execution to TriggerManager."""
        self.trigger_manager.fire(phase)

    def _primary_actuator_name(self):
        if not self.scan_dimensions:
            return None
        return self.scan_dimensions[0].actuator

    def _is_position_sync_subscription(self, subscription) -> bool:
        """
        When multiple sync subscriptions exist, only one should update
        self._position. For now, use the primary actuator RBV sync source.
        """
        if subscription is None:
            return True

        primary_name = self._primary_actuator_name()
        if primary_name is None:
            return True

        if getattr(subscription, "actuator", None) == primary_name:
            source = (getattr(subscription, "source", None) or "rbv").lower()
            return source == "rbv"

        return False

    def _arm_sync_controller(self) -> None:
        self.sync_controller.arm()

    def _wait_for_sync(self, timeout_s=None, stop_event=None) -> bool:
        if not self.sync_controller.is_enabled():
            return True

        ok = self.sync_controller.wait(timeout=timeout_s, stop_event=stop_event)
        if not ok:
            logging.debug(
                "SyncController wait ended without full sync (required=%s)",
                list(self.sync_controller.required_names),
            )
        return ok

    def _validate_and_filter_actuators(self):
        if not self.cfg.scan_dimensions:
            raise ValueError("ScanConfig must contain at least one ScanDimension")

        # Extract actuator names from scan_dimensions
        dim_actuators = {dim.actuator for dim in self.cfg.scan_dimensions}

        # Filter out actuators not referenced in dimensions
        all_actuators = set(self.cfg.actuators.keys())
        unused_actuators = all_actuators - dim_actuators

        if unused_actuators:
            logging.warning(f"Removing unused actuators not referenced in scan_dimensions: {unused_actuators}")
            for name in unused_actuators:
                del self.cfg.actuators[name]

        # Ensure every dimension references a valid actuator
        for dim in self.cfg.scan_dimensions:
            if dim.actuator not in self.cfg.actuators:
                raise ValueError(f"ScanDimension refers to unknown actuator: '{dim.actuator}'")

    def _connect_detectors(self):
        logging.debug(f"Detector PVs: {self.cfg.detector_pvs}")
        logging.debug("Init Detectors")

        self.detector_pvs = []
        for i, pvname in enumerate(self.cfg.detector_pvs):
            logging.debug("Creating detector PV %d/%d: %s", i+1, len(self.cfg.detector_pvs), pvname)
            pv = EpicsPV(
                pvname,
                timeout=1.0,
                connection_timeout=1.0,
                queueing_delay=0.0,
                auto_monitor=True,
            )
            logging.debug("Created detector PV: %s", pvname)
            self.detector_pvs.append(pv)

    def _connect_actuators(self):
        logging.debug("Init Actuators")
        # 1. Check for at least one actuator
        if not getattr(self.cfg, "actuators", None):
            logging.info("No actuators have been configured!")
            self.actuators = {}
            return

        actuators: Dict[str, AbstractActuator] = {}
        for name, raw_cfg in self.cfg.actuators.items():
            # support both dicts and ActuatorConfig instances
            if isinstance(raw_cfg, dict):
                cfg = ActuatorConfig.from_dict(raw_cfg)
            elif isinstance(raw_cfg, ActuatorConfig):
                cfg = raw_cfg
            else:
                raise TypeError(f"Actuator config for '{name}' must be dict or ActuatorConfig, got {type(raw_cfg)}")

            logging.info(f"Creating actuator '{name}' → PV='{cfg.pv}', RB_PV='{cfg.rb_pv}'")
            # instantiate the Actuator
            act = create_actuator(cfg)
            logging.debug("Actuator created")
            actuators[name] = act

        # assign the full dict back onto self
        self.actuators = actuators
        logging.info(f"Number of actuators: {len(self.actuators)}")

    def _validate_config(self):
        if not (self.scan_dimensions or self.parallel_scans or self.nested_scans):
            raise ValueError("No scan dimensions provided in ScanConfig.")

    def set_samplerate(self, dim: ScanDimension):
        """
        Safely sets the samplerate and sampletime based on dim.steps.
        Args:
            dim: Object with a 'steps' attribute representing the number of steps.
        Raises:
            ValueError: If steps is zero or negative.
        """
        steps = dim.steps
        if steps <= 0:
            raise ValueError(f"Steps must be positive, got {steps}")
        self.samplerate = steps
        self.sampletime = 1.0 / self.samplerate
    
    def task_delay(self, start_time, sampletime, index):
        """
        Time between sample points
        A soft RT fixed sample rate design
        """
        scheduled_sample_time = index * sampletime + start_time
        now =  time.time()
        delay = scheduled_sample_time - now
        if delay > 0:
            time.sleep(delay)
        logging.debug(
                f"delay = {delay:.6f}, "
                f"start time = {datetime.fromtimestamp(start_time).strftime('%H:%M:%S.%f')}, "
                f"scheduled time = {datetime.fromtimestamp(scheduled_sample_time).strftime('%H:%M:%S.%f')}")

    def generate_and_create_file(self, base_filename):
        """
        Generates a new filename by appending the current date and time to the base filename.
        Parameters:
            base_filename (str): The original filename (e.g., 'monotest.txt').
        Returns:
            str: The new filename with the timestamp appended (e.g., 'monotest-202411061655.txt').
        """
        while True:
            now = datetime.now()
            timestamp = now.strftime('%Y%m%d%H%M%S')
            name, ext = os.path.splitext(base_filename)
            new_filename = os.path.join(self.data_dir, f"{name}-{timestamp}{ext}")
            if not os.path.exists(new_filename):
                with open(new_filename, 'w') as f:
                    pass  # Create an empty file
                return new_filename
            # If the file exists, add a random 6-character suffix and retry
            random_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
            new_filename = os.path.join(self.data_dir, f"{name}-{timestamp}_{random_suffix}{ext}")
            
            if not os.path.exists(new_filename):
                with open(new_filename, 'w') as f:
                    pass  # Create an empty file
                return new_filename

    def get_output_file(self):
        return self.output_file

    def is_within_range(self, current_position, start, stop):
        """
        Check if the actuator's current position is within the scan range.

        Parameters:
            current_position (float): The current position of the actuator.
            start (float): The starting point of the scan range.
            stop (float): The ending point of the scan range.

        Returns:
            bool: True if within range, False if out of range.
        """
        if start == stop:
            return True

        lower = min(start, stop)
        upper = max(start, stop)

        return lower <= current_position <= upper

    def read_detectors(self) -> List[Any]:
        """
        Read values (with metadata) from all configured detector PVs.

        This method loops over each PV in `self.detector_pvs` and calls
        `pv.get_with_metadata()`.  If a PV returns `None`, a WARNING is logged
        and that PV is skipped.  Any exception during the get is caught, logged
        as an ERROR (including traceback), and the loop continues.

        Returns:
            List[Any]:
                A list of “readings” where each element is whatever
                `pv.get_with_metadata()` returned.   
        """
        readings: List[Any] = []
        for pv in self.detector_pvs:
            try:
                reading = pv.get_with_metadata(use_monitor=True) # read from cache 
                if reading is None:
                    logging.warning("Received None for PV %s", pv.pvname)
                    readings.append(None)
                else:
                    readings.append(reading)
            except Exception as e:
                logging.error("Failed to read metadata for PV %s: %s", pv.pvname, e, exc_info=True)
                readings.append(None)
            # logging.debug("PV %s → %r", pv.pvname, reading)
        return readings

    def save_to_file(self, position, detector_values, include_timestamps=True):
        """
        Writes one line:
          line_timestamp_utc  position  [mean stddev]  det_value [det_timestamp_utc] ...
        Args:
            position (float): The current position of the actuator.
            detector_values (list): The list of dictionaries containing detector readings and metadata.
            include_timestamps (bool): Whether to include timestamps in the output. Defaults to True.
        """
        logging.debug(f"Detector values to be written: {detector_values}")

        # Independent per-line timestamp (UTC ISO 8601)
        line_ts_iso = datetime.now(timezone.utc).isoformat()

        # Update in-memory last-point cache (used by get_value)
        try:
            self._update_last_point_cache(
                position=position,
                line_ts_iso=line_ts_iso,
                values=detector_values,
                include_timestamps=include_timestamps,
            )
        except Exception:
            logging.debug("Failed to update last-point cache", exc_info=True)

        with open(self.output_file, "a", encoding="utf-8") as file:
            parts = []

            # Line timestamp + position
            parts.append(f"{float(position):.12e}")

            # Optional stats stored as a tuple (support (mean,std) and (mean,std,min,max,n))
            st = getattr(self, "_stats", None)
            if st is not None:
                try:
                    n_fields = len(st)
                except Exception:
                    n_fields = 0

                if n_fields >= 5:
                    mean, stddev, vmin, vmax, ns = st
                    parts.append(f"{float(mean):.12e}")
                    parts.append(f"{float(stddev):.12e}")
                    parts.append(f"{float(vmin):.12e}")
                    parts.append(f"{float(vmax):.12e}")
                    parts.append(f"{int(ns)}")
                else:
                    mean, stddev = st
                    parts.append(f"{float(mean):.12e}")
                    parts.append(f"{float(stddev):.12e}")

            parts.append(line_ts_iso)

            # Detector values (+ optional detector timestamps)
            for det in detector_values:
                value = det.get("value")
                try:
                    parts.append(f"{float(value):.12e}")
                except (ValueError, TypeError):
                    parts.append(str(value))

                if include_timestamps:
                    ts = det.get("timestamp")
                    if ts is None:
                        parts.append("")
                    else:
                        parts.append(datetime.fromtimestamp(ts, tz=timezone.utc).isoformat())

            line = "\t".join(parts) + "\n"
            logging.debug(f"Save line to file:{line}")
            file.write(line)

    def _update_last_point_cache(
        self,
        *,
        position: Any,
        line_ts_iso: str,
        values: List[Any],
        include_timestamps: bool,
    ) -> None:
        """Update in-memory cache of the last acquired scan point.

        This is used by get_value() to provide fast access to the latest row
        across all scan types. Keys match the written column headers:
          - base: Position, PositionMean/Std/..., TS-ISO8601
          - detectors: <PV>, TS-ISO8601-<PV>
          - plugins: <PluginHeader>, TS-<PluginHeader>
        """
        last: Dict[str, Any] = {}

        # Base columns
        last["Position"] = float(position) if position is not None else position

        st = getattr(self, "_stats", None)
        if st is not None:
            try:
                n_fields = len(st)
            except Exception:
                n_fields = 0

            if n_fields >= 5:
                mean, stddev, vmin, vmax, ns = st
                last["PositionMean"] = float(mean)
                last["PositionStd"] = float(stddev)
                last["PositionMin"] = float(vmin)
                last["PositionMax"] = float(vmax)
                last["PositionNSamples"] = int(ns)
            else:
                mean, stddev = st
                last["PositionMean"] = float(mean)
                last["PositionStd"] = float(stddev)

        last["TS-ISO8601"] = line_ts_iso

        # Build ordered data headers (one per dict in values)
        data_headers: List[str] = []
        detector_names: List[str] = []
        try:
            detector_names = [pv.pvname for pv in getattr(self, "detector_pvs", [])]
            data_headers += detector_names
        except Exception:
            detector_names = []

        try:
            for plugin in getattr(self, "plugins", []) or []:
                # only the data headers (timestamps are derived from wrapped dict timestamp)
                data_headers += plugin.get_headers(False)
        except Exception:
            pass

        # Map each acquired dict to its header; fallback to pvname inside dict if present
        for i, item in enumerate(values or []):
            header: Optional[str] = data_headers[i] if i < len(data_headers) else None

            if isinstance(item, dict):
                header = header or item.get("pvname") or item.get("name")
                if header:
                    last[header] = item
                    if include_timestamps:
                        ts = item.get("timestamp")
                        if ts is None:
                            if header in detector_names:
                                last["TS-ISO8601-" + header] = ""
                            else:
                                last["TS-" + header] = ""
                        else:
                            iso = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
                            if header in detector_names:
                                last["TS-ISO8601-" + header] = iso
                            else:
                                last["TS-" + header] = iso
                else:
                    last[f"col{i}"] = item
            else:
                if header:
                    last[header] = item
                else:
                    last[f"col{i}"] = item

        self._last_point = last

    def get_value(
        self,
        name: str,
        *,
        default: Any = None,
        with_metadata: bool = False,
    ) -> Any:
        """Return the last-acquired datapoint by column name.

        If with_metadata=False (default), returns the scalar value if the stored
        entry is a dict containing a 'value' key; otherwise returns the stored
        object itself.
        """
        if not getattr(self, "_last_point", None):
            return default
        if name not in self._last_point:
            return default
        v = self._last_point.get(name, default)
        if with_metadata:
            return v
        if isinstance(v, dict) and "value" in v:
            return v.get("value", default)
        return v

    def get_last_point_keys(self) -> List[str]:
        """Return the currently available keys for get_value()."""
        if not getattr(self, "_last_point", None):
            return []
        return list(self._last_point.keys())


    def load_data(self):
        """
        Load recent data file
        """
        data_loader = DataLoader(self.output_file, data_dir=self.data_dir)
        return data_loader.load_data()

    def write_header_to_output_file(self):
        """
        Open the output file and write the headers.

        Returns:
            file: The opened file object.
        """
        file = open(self.output_file, "w")

        detector_headers = [pv.pvname for pv in self.detector_pvs]

        # Base columns always present
        base_headers = ["Position"]

        # Add online stats columns if available/used
        st = getattr(self, "_stats", None)
        if st is not None:
            try:
                n_fields = len(st)
            except Exception:
                n_fields = 0

            if n_fields >= 5:
                base_headers += ["PositionMean", "PositionStd", "PositionMin", "PositionMax", "PositionNSamples"]
            else:
                base_headers += ["PositionMean", "PositionStd"]

        # Scan-point timestamp (one per row)
        base_headers += ["TS-ISO8601"]  # scan_point_timestamp in ISO8601

        # Detector columns
        if self.include_timestamps:
            det_cols = [
                item
                for pv in self.detector_pvs
                for item in (pv.pvname, "TS-ISO8601-" + pv.pvname)
            ]
            headers = base_headers + det_cols
        else:
            headers = base_headers + detector_headers

        logging.debug(f"headers {headers}")

        # Plugin headers
        plugin_headers = []
        for plugin in self.plugins:
            plugin_headers += plugin.get_headers(self.include_timestamps)

        headers += plugin_headers

        file.write("\t".join(headers) + "\n")
        return file

    def get_stop_pv(self):
        """  Read stop PV and reset it if triggered (value == 1).
             Returns the current PV value or None on failure. """
        value = None
        if self.stop_pv:
            try:
                value = self.stop_pv.get()
                logging.info(f"scan stop PV value received: {value}")
            except Exception as e:
                logging.error(
                    f"Failed to get stop PV {self.stop_pv.pvname}: {e}",
                    exc_info=True,
                )
            if value == 1:
                try:
                    self.stop_pv.put(0)
                except Exception as e:
                    logging.error(
                        f"Failed to reset stop PV {self.stop_pv.pvname}: {e}",
                        exc_info=True,
                    )
        return value

    def scan(self, positions, monitor: BaseMonitor = None):
        """
        Parallel multi-actuator scan:
         1. pad all position lists to equal length
         2. optionally prepend an overshoot point (if any backlash>0)
         3. broadcast moves, wait in parallel, then read & save (skipping the overshoot)
        """
        try:
            epics.ca.use_initial_context()
        except Exception:
            pass
        self.busyflag = True
        self.write_header_to_output_file()
        try:
            self._start_subscriptions()
            logging.debug(f"Actuators: {list(self.actuators)}")
            logging.debug(f"Requested positions: {positions}")
            # --- start CA monitors BEFORE motion begins ---
            try:
                self._meta_mon.start()
                logging.info("Started meta data task")
            except Exception as e:
                # Non-fatal: keep the scan running even if metadata fails
                logging.error("Failed to start metadata monitor: %s", e, exc_info=True)

            # prepare new_positions and tell us if we added an overshoot step
            new_positions, overshoot_applied = self._prepare_positions(positions)
            if not new_positions:
                logging.warning("No valid actuators with positions—nothing to scan.")
                self.busyflag = False
                return

            # how many total steps (includes overshoot if applied)
            n_steps = len(next(iter(new_positions.values())))

            self._fire_triggers("before")
            for idx in range(n_steps):
                self._daq_is_on = False
                # 1) broadcast every move
                for name, act in self.actuators.items():
                    if name not in new_positions:
                        continue
                    tgt = new_positions[name][idx]
                    logging.info(f"[{name}] moving to {tgt}")
                    act.move(tgt)

                # 2) wait for all in parallel
                self._parallel_wait(
                    {name: self.actuators[name] for name in new_positions},
                    {name: new_positions[name][idx] for name in new_positions}
                )

                # 3) skip detector‐read on the overshoot step
                if overshoot_applied and idx == 0:
                    continue

                # 4) read detectors & save & monitor
                self._daq_is_on = True
                with self._time_block("triggers:on_point", idx=idx):
                    self._fire_triggers("on_point")
                if self.integration_time > 0.0:
                    logging.info(f"DAQ for integration_time = {self.integration_time}")
                    time.sleep(self.integration_time)
                else:
                    logging.info(f"integration_time = {self.integration_time}")
                ## pos_snapshot = {n: new_positions[n][idx] for n in new_positions}
                first_actuator = next(iter(new_positions))
                pos_snapshot = new_positions[first_actuator][idx]
                with self._time_block("read_detectors", idx=idx):
                    vals = self.read_detectors()
                # Collect plugin data and append to detector values
                plugin_data = []
                with self._time_block("plugins", idx=idx):
                    for plugin in self.plugins:
                        data = plugin.on_scan_point(idx, pos_snapshot)
                        plugin_data = plugin_data + data
                vals = vals + plugin_data
                with self._time_block("write:data", idx=idx):
                    self.save_to_file(pos_snapshot, vals, self.include_timestamps)
                self._position = pos_snapshot 
                # >>> Notify monitor/plotter
                with self._time_block("monitor:update", idx=idx):
                    if monitor is not None:
                        logging.debug(f"{vals}")
                        monitor.update(vals)

                # 5) abort if needed
                if self.get_stop_pv() == 1:
                    logging.info("Stop PV triggered—aborting scan.")
                    break

            self._fire_triggers("after")
            logging.info("Scan complete for all actuators.")
        
        finally:
            self._daq_is_on = False
            try:
                self._meta_mon.stop()
            except Exception:
                logging.exception("Error stopping metadata monitor")

            if monitor is not None:
                monitor.close()

            try:
                self._stop_subscriptions()
            except Exception:
                logging.exception("Error stopping scan subscriptions")
            self.busyflag = False
            self._perf_report()

    def _prepare_positions(self, positions):
        """
        1) Pad each actuator’s position list by repeating its last element
           so all have the same length.
        2) If any actuator has non-zero backlash *inject* an initial overshoot
           for each axis (otherwise leave lists as is).

        Returns:
          - new_positions: dict[name → list of targets]
          - overshoot_applied: bool
        """
        # filter out actuators with no positions
        filtered = {
            name: pts[:]  # copy
            for name, pts in positions.items()
            if pts
        }

        # nothing to do?
        if not filtered:
            return {}, False

        # pad to max length
        max_len = max(len(pts) for pts in filtered.values())
        for name, pts in filtered.items():
            if len(pts) < max_len:
                pts.extend([pts[-1]] * (max_len - len(pts)))

        # check if any backlash
        any_backlash = any(
            self.actuators[name].backlash != 0.0
            for name in filtered
            if name in self.actuators
        )

        if not any_backlash:
            # no overshoot step needed
            return filtered, False

        # build overshoot + real sequences
        prepared = {}
        for name, pts in filtered.items():
            act = self.actuators.get(name)
            if not act:
                continue
            first, second = pts[0], pts[1] if len(pts) > 1 else pts[0]
            if act.backlash != 0.0 and len(pts) > 1:
                bdist = -act.backlash if second > first else act.backlash
                overshoot = first + bdist
            else:
                # duplicate first point
                overshoot = first
            prepared[name] = [overshoot] + pts

        return prepared, True

    def _parallel_wait(self, acts: dict, targets: dict):
        """
        Wait for multiple actuators to reach their target positions in parallel.

        This method starts a separate thread for each actuator's `wait_until_done(target)`
        method, allowing all actuators to be monitored for completion concurrently.
        This prevents blocking on one actuator's wait and allows for more responsive
        overall execution, especially when actuators finish at different times.

        Parameters
        ----------
        acts : dict
            Dictionary mapping actuator names (str) to actuator objects.
            Each actuator must implement a `wait_until_done(target)` method.
        targets : dict
            Dictionary mapping actuator names (str) to target positions/values.

        Notes
        -----
        If any actuator's `wait_until_done` method raises an exception, it will be
        caught and logged with the actuator's name. All waits are attempted,
        even if some fail.

        Example
        -------
        >>> self._parallel_wait(
                acts={'motor1': m1, 'motor2': m2},
                targets={'motor1': 10.0, 'motor2': 5.0}
            )
        """
        with ThreadPoolExecutor(max_workers=len(acts)) as exe:
            futures = {
                exe.submit(act.wait_until_done, targets[name]): name
                for name, act in acts.items()
            }
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    logging.error(f"[{name}] wait failed: {e}")
    def stop(self):
        """ To be implemented in concrete classes """
        print("> Stop not implemented <")

    @property
    def busy(self) -> bool:
        """True while scan"""
        return self.busyflag
    
    @property
    def position(self) ->  Any:
        return self._position

    def get_actuator(self, name: str) -> AbstractActuator:
        """
        Return the actuator object by name.

        This is intended for sharing actuators between scans/plugins or for
        composition in higher-level scan engines.
        """
        if not hasattr(self, "actuators") or self.actuators is None:
            raise RuntimeError("Actuators are not initialized on this scan instance.")

        try:
            act = self.actuators[name]
        except KeyError as exc:
            available = ", ".join(sorted(self.actuators.keys()))
            raise KeyError(
                f"Unknown actuator '{name}'. Available actuators: {available}"
            ) from exc

        if act is None:
            raise KeyError(f"Actuator '{name}' exists but is None.")

        return act

    def get_actuators(self) -> Dict[str, AbstractActuator]:
        """
        Return the full actuator mapping (name -> actuator object).

        Returns a shallow copy to prevent accidental mutation of internal state.
        """
        if not hasattr(self, "actuators") or self.actuators is None:
            return {}
        return dict(self.actuators)
